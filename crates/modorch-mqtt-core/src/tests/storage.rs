use super::*;
use modorch_mqtt_storage::{SqliteStorageConfig, SqliteStorageFactory};
use std::sync::atomic::{AtomicU8, Ordering};

fn database_path() -> std::path::PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "mqtt-core-storage-{}-{}.sqlite",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}
fn persistent_config() -> MqttBrokerConfig {
    MqttBrokerConfig {
        storage: Some(StorageProviderConfig {
            component: "store-proxy".into(),
            access: StoreAccessKey([19; 32]),
            max_snapshot_bytes: 1024 * 1024,
            recovery_page_bytes: 73,
            timeout: Duration::from_millis(150),
            recovery_timeout: Duration::from_secs(10),
        }),
        ..config()
    }
}
#[derive(Clone, Default)]
struct Faults {
    mode: Arc<AtomicU8>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}
struct Proxy(Faults);
fn proxy_descriptor() -> ComponentDescriptor {
    let mut d = ComponentDescriptor::new(ComponentRef::module("store-proxy"));
    d.requires.push(ComponentRef::module("sqlite-store"));
    d
}
impl ModuleFactory for Proxy {
    fn descriptor(&self) -> ComponentDescriptor {
        proxy_descriptor()
    }
    fn create(&self, _: &ComponentConfig) -> FrameworkResult<Box<dyn Module>> {
        Ok(Box::new(Proxy(self.0.clone())))
    }
}
#[async_trait]
impl Lifecycle for Proxy {}
impl Module for Proxy {
    fn descriptor(&self) -> ComponentDescriptor {
        proxy_descriptor()
    }
    fn handlers(&mut self) -> FrameworkResult<Option<HandlerRegistration>> {
        Ok(Some(HandlerRegistration::Serial {
            handler: Box::new(Proxy(self.0.clone())),
            subscriptions: vec![SubscriptionSpec::new(
                5001,
                ComponentRef::module("store-proxy"),
                STORAGE_TOPIC,
            )],
        }))
    }
}
#[async_trait]
impl SerialHandler for Proxy {
    async fn handle(
        &mut self,
        ctx: &HandlerContext,
        message: MessageEnvelope,
    ) -> FrameworkResult<HandlerReply> {
        let request = message.payload::<StoreEnvelope>()?;
        let mode = self.0.mode.load(Ordering::Acquire);
        if mode == 3 && matches!(request.request, StoreRequest::Commit { .. }) {
            return Ok(Arc::new(Err::<StoreReply, _>(StoreError::Unavailable)));
        }
        let response = ctx
            .event_hub
            .request::<_, StoreResult>(
                STORAGE_TOPIC,
                Destination::Component(ComponentRef::module("sqlite-store")),
                request.as_ref().clone(),
                MessageOptions {
                    payload_bytes: Some(2 * 1024 * 1024),
                    ..Default::default()
                },
            )?
            .wait(Duration::from_secs(2))
            .await?;
        if matches!(request.request, StoreRequest::Commit { .. }) && (mode == 1 || mode == 2) {
            self.0.mode.store(0, Ordering::Release);
            self.0.entered.notify_one();
            if mode == 1 {
                self.0.release.notified().await;
            } else {
                tokio::time::sleep(Duration::from_millis(210)).await;
            }
        }
        if mode == 4 {
            if let Ok(StoreReply::Page {
                epoch,
                version,
                offset,
                data,
                done,
            }) = response.as_ref()
            {
                return Ok(Arc::new(Ok::<_, StoreError>(StoreReply::Page {
                    epoch: *epoch,
                    version: *version,
                    offset: offset + 1,
                    data: data.clone(),
                    done: *done,
                })));
            }
        }
        Ok(response)
    }
}
async fn attempt(
    path: &std::path::Path,
    c: MqttBrokerConfig,
    faults: Faults,
) -> (Result<ApplicationHandle, StartupError>, BrokerHandle) {
    let mut builder = ApplicationBuilder::new("durable-test");
    let auth = c.auth.is_some();
    let factory = MqttBrokerFactory::new(c)
        .unwrap()
        .with_failure_policy(FailurePolicy::StopDependents);
    let broker = factory.handle();
    builder
        .register_module(
            SqliteStorageFactory::new(SqliteStorageConfig {
                name: "sqlite-store".into(),
                broker: "mqtt-broker".into(),
                subscription_id: 5000,
                access: StoreAccessKey([19; 32]),
                database: path.into(),
                max_snapshot_bytes: 1024 * 1024,
                disk_budget_bytes: 12 * 1024 * 1024,
            })
            .unwrap(),
        )
        .unwrap();
    if auth {
        builder.register_module(TestAuth).unwrap();
    }
    builder.register_module(InternalPublisher).unwrap();
    builder.register_module(Proxy(faults)).unwrap();
    builder.register_module(factory).unwrap();
    let mut select = ComponentSelection::new();
    if auth {
        select.enable_module("test-auth");
    }
    select.enable_module("internal-business");
    select
        .enable_module("mqtt-broker")
        .enable_module("sqlite-store")
        .enable_module("store-proxy");
    (
        builder.selection(select).build().unwrap().start().await,
        broker,
    )
}
async fn launch(path: &std::path::Path, faults: Faults) -> (ApplicationHandle, BrokerHandle) {
    let (app, broker) = attempt(path, persistent_config(), faults).await;
    (app.unwrap(), broker)
}

#[tokio::test]
async fn sqlite_restart_restores_offline_routes_retained_and_inflight_before_subscriber_reconnect()
{
    let path = database_path();
    let (app, broker) = launch(&path, Faults::default()).await;
    let addr = broker.local_addr().unwrap();
    let (mut subscriber, _) = Client::connect(addr, "durable-subscriber", false).await;
    subscriber.subscribe(&[("persist/#", 1)]).await;
    let (mut publisher, _) = Client::connect(addr, "publisher", true).await;
    publisher
        .publish("persist/first", b"unacked", 1, true)
        .await;
    let (header, first) = subscriber.recv().await;
    assert_eq!(header, 0x32);
    let n = u16::from_be_bytes([first[0], first[1]]) as usize;
    let id = first[2 + n..4 + n].to_vec();
    stop(&app).await;
    drop(subscriber);
    drop(publisher);
    drop(app);
    let (app, broker) = launch(&path, Faults::default()).await;
    let addr = broker.local_addr().unwrap();
    assert_eq!(broker.status().subscriptions, 1);
    assert_eq!(broker.status().pending_messages, 1);
    assert_eq!(broker.status().retained, 1);
    // This publication happens before the durable subscriber has reconnected.
    let (mut publisher, _) = Client::connect(addr, "second-publisher", true).await;
    publisher
        .publish("persist/offline", b"before-reconnect", 1, false)
        .await;
    let (mut subscriber, present) = Client::connect(addr, "durable-subscriber", false).await;
    assert!(present);
    let (header, replayed) = subscriber.recv().await;
    assert_eq!(header, 0x3a);
    assert_eq!(first, replayed);
    subscriber.send(codec::frame(0x40, &id)).await;
    let (_, message) = subscriber.recv().await;
    assert!(message.ends_with(b"before-reconnect"));
    let (mut retained, _) = Client::connect(addr, "retained-reader", true).await;
    retained.subscribe(&[("persist/first", 1)]).await;
    let (header, data) = retained.recv().await;
    assert_eq!(header & 1, 1);
    assert!(data.ends_with(b"unacked"));
    stop(&app).await;
    drop((subscriber, publisher, retained, app));
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn durable_commit_barrier_blocks_puback_and_downstream_delivery_and_resolves_lost_reply() {
    let path = database_path();
    let faults = Faults::default();
    let (app, broker) = launch(&path, faults.clone()).await;
    let addr = broker.local_addr().unwrap();
    let (mut subscriber, _) = Client::connect(addr, "subscriber", false).await;
    subscriber.subscribe(&[("durable/#", 1)]).await;
    let (mut publisher, _) = Client::connect(addr, "publisher", true).await;
    faults.mode.store(1, Ordering::Release);
    let mut body = string("durable/barrier");
    body.extend([0, 9]);
    body.extend(b"commit-before-ack");
    publisher.send(codec::frame(0x32, &body)).await;
    tokio::time::timeout(Duration::from_secs(2), faults.entered.notified())
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(30), publisher.0.read_u8())
            .await
            .is_err()
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(30), subscriber.0.read_u8())
            .await
            .is_err()
    );
    faults.release.notify_one();
    assert_eq!(publisher.recv().await, (0x40, vec![0, 9]));
    assert!(subscriber.recv().await.1.ends_with(b"commit-before-ack"));
    faults.mode.store(2, Ordering::Release);
    publisher
        .publish("durable/uncertain", b"same-operation-query", 1, false)
        .await;
    assert!(subscriber.recv().await.1.ends_with(b"same-operation-query"));
    assert_eq!(broker.status().received_publications, 2);
    let status = broker.status();
    assert!(status.storage_commits > 0);
    assert_eq!(status.storage_commit_failures, 0);
    assert!(status.storage_commit_max_micros > 0);
    assert!(status.storage_commit_duration_micros >= status.storage_commit_max_micros);
    stop(&app).await;
    drop((subscriber, publisher, app));
    let (app, broker) = launch(&path, Faults::default()).await;
    assert_eq!(broker.status().pending_messages, 2);
    stop(&app).await;
    drop(app);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn corrupt_recovery_page_and_reduced_budget_never_open_listener_or_discard_committed_data() {
    let path = database_path();
    let (app, broker) = launch(&path, Faults::default()).await;
    let (mut client, _) = Client::connect(broker.local_addr().unwrap(), "saved", false).await;
    let long_filter = format!("saved/{}", "x".repeat(1200));
    client.subscribe(&[("saved/+", 1), (&long_filter, 1)]).await;
    stop(&app).await;
    drop((client, app));
    let faults = Faults::default();
    faults.mode.store(4, Ordering::Release);
    let (app, broker) = attempt(&path, persistent_config(), faults).await;
    assert!(app.is_err());
    assert!(!broker.status().active);
    assert!(
        TcpStream::connect(broker.local_addr().unwrap())
            .await
            .is_err()
    );
    let mut c = persistent_config();
    c.memory_budget_bytes = 4 * c.storage.as_ref().unwrap().max_snapshot_bytes + 128;
    let (app, broker) = attempt(&path, c, Faults::default()).await;
    assert!(app.is_err());
    assert!(!broker.status().active);
    let mut c = persistent_config();
    c.storage.as_mut().unwrap().max_snapshot_bytes = 1024;
    let (app, broker) = attempt(&path, c, Faults::default()).await;
    assert!(app.is_err());
    assert!(!broker.status().active);
    let (app, broker) = launch(&path, Faults::default()).await;
    assert_eq!(broker.status().subscriptions, 2);
    stop(&app).await;
    drop(app);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn failed_store_commit_sends_no_ack_or_delivery_and_restart_uses_last_committed_image() {
    let path = database_path();
    let faults = Faults::default();
    let (app, broker) = launch(&path, faults.clone()).await;
    let addr = broker.local_addr().unwrap();
    let (mut subscriber, _) = Client::connect(addr, "protected", false).await;
    subscriber.subscribe(&[("failure/#", 1)]).await;
    let (mut publisher, _) = Client::connect(addr, "publisher", true).await;
    faults.mode.store(3, Ordering::Release);
    let mut body = string("failure/not-committed");
    body.extend([0, 7]);
    body.extend(b"must-not-deliver");
    publisher.send(codec::frame(0x33, &body)).await;
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), publisher.0.read_u8()).await,
        Ok(Err(_))
    ));
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), subscriber.0.read_u8()).await,
        Ok(Err(_))
    ));
    tokio::time::timeout(Duration::from_secs(2), async {
        while broker.status().active {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(broker.status().storage_commit_failures > 0);
    stop(&app).await;
    drop((subscriber, publisher, app));
    let (app, broker) = launch(&path, Faults::default()).await;
    assert_eq!(broker.status().pending_messages, 0);
    assert_eq!(broker.status().retained, 0);
    assert_eq!(broker.status().subscriptions, 1);
    stop(&app).await;
    drop(app);
    std::fs::remove_file(path).unwrap();
}

struct InternalPublisher;
fn business_descriptor() -> ComponentDescriptor {
    ComponentDescriptor::new(ComponentRef::module("internal-business"))
}
impl ModuleFactory for InternalPublisher {
    fn descriptor(&self) -> ComponentDescriptor {
        business_descriptor()
    }
    fn create(&self, _: &ComponentConfig) -> FrameworkResult<Box<dyn Module>> {
        Ok(Box::new(Self))
    }
}
#[async_trait]
impl Lifecycle for InternalPublisher {}
impl Module for InternalPublisher {
    fn descriptor(&self) -> ComponentDescriptor {
        business_descriptor()
    }
    fn handlers(&mut self) -> FrameworkResult<Option<HandlerRegistration>> {
        Ok(Some(HandlerRegistration::Serial {
            handler: Box::new(Self),
            subscriptions: vec![SubscriptionSpec::new(
                5010,
                business_descriptor().component(),
                "test/internal",
            )],
        }))
    }
}
#[async_trait]
impl SerialHandler for InternalPublisher {
    async fn handle(
        &mut self,
        ctx: &HandlerContext,
        message: MessageEnvelope,
    ) -> FrameworkResult<HandlerReply> {
        let request = message.payload::<MqttPublishRequest>()?;
        Ok(ctx
            .event_hub
            .request::<_, MqttPublishResult>(
                PUBLISH_TOPIC,
                Destination::Component(ComponentRef::module("mqtt-broker")),
                request.as_ref().clone(),
                MessageOptions {
                    payload_bytes: Some(4096),
                    ..Default::default()
                },
            )?
            .wait(Duration::from_secs(2))
            .await?)
    }
}
async fn internal_publish(
    app: &ApplicationHandle,
    request: MqttPublishRequest,
) -> MqttPublishResult {
    app.event_hub()
        .external()
        .request::<_, MqttPublishResult>(
            "test/internal",
            Destination::Component(ComponentRef::module("internal-business")),
            request,
            MessageOptions {
                payload_bytes: Some(4096),
                ..Default::default()
            },
        )
        .unwrap()
        .wait(Duration::from_secs(3))
        .await
        .unwrap()
        .as_ref()
        .clone()
}
#[tokio::test]
async fn durable_internal_idempotency_and_original_receipt_survive_a_new_application() {
    let path = database_path();
    let (app, broker) = launch(&path, Faults::default()).await;
    let request = MqttPublishRequest {
        request_id: "stable-business-id".into(),
        topic: "durable/internal".into(),
        payload: b"one-commit".as_slice().into(),
        qos: Qos::AtLeastOnce,
        retain: true,
        properties: Default::default(),
    };
    assert_eq!(
        internal_publish(&app, request.clone())
            .await
            .unwrap()
            .durability,
        AcceptanceDurability::DurableCommitted
    );
    let (mut publisher, _) =
        Client::connect(broker.local_addr().unwrap(), "delete-retained", true).await;
    publisher.publish("durable/internal", b"", 1, true).await;
    stop(&app).await;
    drop((publisher, app));
    let (app, broker) = launch(&path, Faults::default()).await;
    assert_eq!(broker.status().retained, 0);
    assert_eq!(broker.status().publish_records, 1);
    assert_eq!(
        internal_publish(&app, request.clone())
            .await
            .unwrap()
            .durability,
        AcceptanceDurability::DurableCommitted
    );
    assert_eq!(broker.status().retained, 0);
    let mut conflict = request;
    conflict.payload = b"different".as_slice().into();
    assert_eq!(
        internal_publish(&app, conflict).await,
        Err(MqttPublishRejection::IdConflict)
    );
    let denied = app
        .event_hub()
        .external()
        .request::<_, StoreResult>(
            STORAGE_TOPIC,
            Destination::Component(ComponentRef::module("sqlite-store")),
            StoreEnvelope {
                access: StoreAccessKey([20; 32]),
                request: StoreRequest::Begin {
                    recovery_id: "forged-root".into(),
                },
            },
            MessageOptions::default(),
        )
        .unwrap()
        .wait(Duration::from_secs(1))
        .await
        .unwrap();
    assert!(matches!(*denied, Err(StoreError::NotAuthorized)));
    stop(&app).await;
    drop(app);
    std::fs::remove_file(path).unwrap();
}
struct TestAuth;
fn auth_descriptor() -> ComponentDescriptor {
    ComponentDescriptor::new(ComponentRef::module("test-auth"))
}
impl ModuleFactory for TestAuth {
    fn descriptor(&self) -> ComponentDescriptor {
        auth_descriptor()
    }
    fn create(&self, _: &ComponentConfig) -> FrameworkResult<Box<dyn Module>> {
        Ok(Box::new(Self))
    }
}
#[async_trait]
impl Lifecycle for TestAuth {}
impl Module for TestAuth {
    fn descriptor(&self) -> ComponentDescriptor {
        auth_descriptor()
    }
    fn handlers(&mut self) -> FrameworkResult<Option<HandlerRegistration>> {
        Ok(Some(HandlerRegistration::Serial {
            handler: Box::new(Self),
            subscriptions: vec![SubscriptionSpec::new(
                5020,
                auth_descriptor().component(),
                AUTH_TOPIC,
            )],
        }))
    }
}
#[async_trait]
impl SerialHandler for TestAuth {
    async fn handle(
        &mut self,
        _: &HandlerContext,
        message: MessageEnvelope,
    ) -> FrameworkResult<HandlerReply> {
        let request = message.payload::<AuthRequest>()?;
        let decision = match request.as_ref() {
            AuthRequest::Authenticate(a) => AuthDecision::Principal(Principal {
                key: PrincipalKey {
                    domain: "stable-users".into(),
                    subject: a.username.clone().unwrap(),
                },
                policy_version: 1,
                expires_at_ms: unix_time_ms() + 3600000,
            }),
            AuthRequest::Publish(p) => AuthDecision::Publish(PublishGrant {
                context: p.clone(),
                policy_version: 1,
            }),
            _ => AuthDecision::Allowed,
        };
        Ok(Arc::new(Ok::<_, AuthError>(decision)))
    }
}
async fn authenticated(
    address: SocketAddr,
    id: &str,
    user: &str,
    clean: bool,
    will: bool,
) -> (Client, u8) {
    let mut c = Client(TcpStream::connect(address).await.unwrap());
    let mut body = string("MQTT");
    body.extend([
        4,
        0x80 + u8::from(clean) * 2 + if will { 12 } else { 0 },
        0,
        0,
    ]);
    body.extend(string(id));
    if will {
        body.extend(string("secure/will"));
        body.extend(string("persisted-grant"));
    }
    body.extend(string(user));
    c.send(codec::frame(0x10, &body)).await;
    let (_, reply) = c.recv().await;
    (c, reply[1])
}
#[tokio::test]
async fn durable_owner_and_authorized_will_survive_restart_without_credential_storage() {
    let path = database_path();
    let mut config = persistent_config();
    config.anonymous_access = false;
    config.auth = Some(AuthProviderConfig {
        component: "test-auth".into(),
        timeout: Duration::from_secs(1),
    });
    let (app, broker) = attempt(&path, config.clone(), Faults::default()).await;
    let app = app.unwrap();
    let address = broker.local_addr().unwrap();
    let (mut alice, code) = authenticated(address, "shared", "alice", false, false).await;
    assert_eq!(code, 0);
    alice.subscribe(&[("secure/#", 1)]).await;
    let (will, code) = authenticated(address, "will-owner", "bob", true, true).await;
    assert_eq!(code, 0);
    stop(&app).await;
    drop((alice, will, app));
    let (app, broker) = attempt(&path, config, Faults::default()).await;
    let app = app.unwrap();
    let address = broker.local_addr().unwrap();
    for clean in [false, true] {
        let (_bob, code) = authenticated(address, "shared", "bob", clean, false).await;
        assert_eq!(code, 5);
    }
    let (mut alice, code) = authenticated(address, "shared", "alice", false, false).await;
    assert_eq!(code, 0);
    assert!(alice.recv().await.1.ends_with(b"persisted-grant"));
    stop(&app).await;
    drop((alice, app));
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn qos2_snapshot_restores_both_directions_without_rerouting_and_resumes_pubrel() {
    use super::qos2::*;
    let path = database_path();
    let (app, broker) = launch_qos2(&path, Faults::default()).await;
    let addr = broker.local_addr().unwrap();
    let (mut sub, _) = Client::connect(addr, "q2-reader", false).await;
    sub.subscribe(&[("q2", 2)]).await;
    let (mut p, _) = Client::connect(addr, "q2-writer", false).await;
    publish_start(&mut p, 9, "q2", b"saved", false, true).await;
    let (_, payload, _, id) = sub.message().await;
    let id = id.unwrap();
    stop(&app).await;
    drop((app, p, sub));
    let (app, broker) = launch_qos2(&path, Faults::default()).await;
    let addr = broker.local_addr().unwrap();
    assert_eq!(broker.status().incoming_qos2, 1);
    let (mut p, present) = Client::connect(addr, "q2-writer", false).await;
    assert!(present);
    publish_start(&mut p, 9, "q2", b"saved", true, true).await;
    assert_eq!(broker.status().pending_messages, 1); // duplicate did not reroute
    let (mut sub, _) = Client::connect(addr, "q2-reader", false).await;
    let (_, replayed, header, replay_id) = sub.message().await;
    assert_eq!((replayed, header, replay_id), (payload, 0x3c, Some(id)));
    received(&mut sub, id).await;
    stop(&app).await;
    drop((app, p, sub));
    let (app, broker) = launch_qos2(&path, Faults::default()).await;
    let addr = broker.local_addr().unwrap();
    let (mut p, _) = Client::connect(addr, "q2-writer", false).await;
    release(&mut p, 9).await;
    release(&mut p, 9).await;
    let (mut sub, _) = Client::connect(addr, "q2-reader", false).await;
    assert_eq!(sub.recv().await, (0x62, id.to_be_bytes().to_vec()));
    complete(&mut sub, id).await;
    assert_eq!(broker.status().incoming_qos2, 0);
    assert_eq!(broker.status().pending_messages, 0);
    assert_eq!(broker.status().retained, 1);
    stop(&app).await;
    drop((app, p, sub));
    std::fs::remove_file(path).unwrap();
}

async fn storage_latch_enter(faults: &Faults, client: &mut Client) {
    tokio::time::timeout(Duration::from_secs(2), faults.entered.notified())
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(30), client.0.read_u8())
            .await
            .is_err()
    );
    faults.release.notify_one();
}

#[tokio::test]
async fn qos2_each_ack_and_phase_transition_waits_for_durable_confirmation() {
    use super::qos2::*;
    let path = database_path();
    let faults = Faults::default();
    let (app, broker) = launch_qos2(&path, faults.clone()).await;
    let addr = broker.local_addr().unwrap();
    let (mut p, _) = Client::connect(addr, "writer", false).await;
    faults.mode.store(1, Ordering::Release);
    let mut body = string("q2");
    body.extend([0, 9]);
    body.extend(b"barrier");
    p.send(codec::frame(0x34, &body)).await;
    storage_latch_enter(&faults, &mut p).await;
    assert_eq!(p.recv().await, (0x50, vec![0, 9]));
    faults.mode.store(1, Ordering::Release);
    p.send(codec::frame(0x62, &[0, 9])).await;
    storage_latch_enter(&faults, &mut p).await;
    assert_eq!(p.recv().await, (0x70, vec![0, 9]));
    let (mut sub, _) = Client::connect(addr, "reader", false).await;
    sub.subscribe(&[("q2", 2)]).await;
    p.publish("q2", b"outbound", 2, false).await;
    let (_, _, _, id) = sub.message().await;
    let id = id.unwrap();
    faults.mode.store(1, Ordering::Release);
    sub.send(codec::frame(0x50, &id.to_be_bytes())).await;
    storage_latch_enter(&faults, &mut sub).await;
    assert_eq!(sub.recv().await, (0x62, id.to_be_bytes().to_vec()));
    faults.mode.store(1, Ordering::Release);
    sub.send(codec::frame(0x70, &id.to_be_bytes())).await;
    sub.send(vec![0xc0, 0]).await;
    storage_latch_enter(&faults, &mut sub).await;
    assert_eq!(sub.recv().await, (0xd0, vec![]));
    assert_eq!(broker.status().pending_messages, 0);
    ping(&mut p).await;
    stop(&app).await;
    drop((app, p, sub));
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn qos2_failed_phase_commits_never_advance_the_wire_or_discard_committed_exchanges() {
    use super::qos2::*;
    for phase in 0..4 {
        let path = database_path();
        let faults = Faults::default();
        let (app, broker) = launch_qos2(&path, faults.clone()).await;
        let addr = broker.local_addr().unwrap();
        let (mut p, _) = Client::connect(addr, "writer", false).await;
        let (mut sub, _) = Client::connect(addr, "reader", false).await;
        sub.subscribe(&[("q2", 2)]).await;
        let mut outbound = 0;
        if phase != 0 {
            publish_start(&mut p, 9, "q2", b"committed", false, false).await;
            let (_, _, _, id) = sub.message().await;
            outbound = id.unwrap();
            if phase == 3 {
                received(&mut sub, outbound).await;
            }
        }
        faults.mode.store(3, Ordering::Release);
        let victim = match phase {
            0 => {
                let mut b = string("q2");
                b.extend([0, 9]);
                b.extend(b"not-committed");
                p.send(codec::frame(0x34, &b)).await;
                &mut p
            }
            1 => {
                p.send(codec::frame(0x62, &[0, 9])).await;
                &mut p
            }
            2 => {
                sub.send(codec::frame(0x50, &outbound.to_be_bytes())).await;
                &mut sub
            }
            _ => {
                sub.send(codec::frame(0x70, &outbound.to_be_bytes())).await;
                sub.send(vec![0xc0, 0]).await;
                &mut sub
            }
        };
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), victim.0.read_u8()).await,
            Ok(Err(_))
        ));
        stop(&app).await;
        drop((app, p, sub));
        let (app, broker) = launch_qos2(&path, Faults::default()).await;
        let addr = broker.local_addr().unwrap();
        assert_eq!(broker.status().incoming_qos2, usize::from(phase != 0));
        assert_eq!(broker.status().pending_messages, usize::from(phase != 0));
        let (mut p, _) = Client::connect(addr, "writer", false).await;
        if phase != 0 {
            release(&mut p, 9).await;
        }
        let (mut sub, _) = Client::connect(addr, "reader", false).await;
        if phase == 3 {
            assert_eq!(sub.recv().await, (0x62, outbound.to_be_bytes().to_vec()));
            complete(&mut sub, outbound).await;
        } else if phase != 0 {
            let (_, payload, header, id) = sub.message().await;
            assert_eq!(payload, b"committed");
            assert_eq!(header, 0x3c);
            received(&mut sub, id.unwrap()).await;
            complete(&mut sub, id.unwrap()).await;
        } else {
            ping(&mut sub).await;
        }
        stop(&app).await;
        drop((app, p, sub));
        std::fs::remove_file(path).unwrap();
    }
}

// These tests exercise phase correctness, not short Request deadlines. Keep the
// 150ms fixture above for the explicit lost-reply/uncertainty tests.
async fn launch_qos2(path: &std::path::Path, faults: Faults) -> (ApplicationHandle, BrokerHandle) {
    let mut c = persistent_config();
    c.storage.as_mut().unwrap().timeout = Duration::from_secs(2);
    let (app, broker) = attempt(path, c, faults).await;
    (app.unwrap(), broker)
}

#[tokio::test]
async fn reversed_pubrec_order_survives_sqlite_restart() {
    use super::qos2::*;
    let path = database_path();
    let (app, broker) = launch_qos2(&path, Faults::default()).await;
    let addr = broker.local_addr().unwrap();
    let (mut s, _) = Client::connect(addr, "ordered-reader", false).await;
    s.subscribe(&[("ordered", 2)]).await;
    let (mut p, _) = Client::connect(addr, "writer", true).await;
    let mut ids = Vec::new();
    for payload in [b"one", b"two", b"tri"] {
        p.publish("ordered", payload, 2, false).await;
        ids.push(s.message().await.3.unwrap());
    }
    for id in [ids[1], ids[0]] {
        received(&mut s, id).await;
    }
    stop(&app).await;
    drop((app, s, p));
    let (app, broker) = launch_qos2(&path, Faults::default()).await;
    let (mut s, present) =
        Client::connect(broker.local_addr().unwrap(), "ordered-reader", false).await;
    assert!(present);
    for id in [ids[1], ids[0]] {
        assert_eq!(s.recv().await, (0x62, id.to_be_bytes().to_vec()));
    }
    let (_, payload, header, id) = s.message().await;
    assert_eq!((payload, header, id), (b"tri".to_vec(), 0x3c, Some(ids[2])));
    for id in [ids[1], ids[0]] {
        complete(&mut s, id).await;
    }
    received(&mut s, ids[2]).await;
    complete(&mut s, ids[2]).await;
    assert_eq!(broker.status().pending_messages, 0);
    stop(&app).await;
    drop((app, s));
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn keepalive_closes_socket_during_commit_then_drains_without_poison_or_qos2_reroute() {
    use super::qos2::*;
    let path = database_path();
    let faults = Faults::default();
    let mut c = persistent_config();
    c.storage.as_mut().unwrap().timeout = Duration::from_secs(4);
    let (app, broker) = attempt(&path, c, faults.clone()).await;
    let app = app.unwrap();
    let addr = broker.local_addr().unwrap();
    let (mut sub, _) = Client::connect(addr, "deadline-reader", false).await;
    sub.subscribe(&[("deadline/#", 1)]).await;
    let (mut p, _) = Client::connect_will(
        addr,
        "deadline-writer",
        false,
        Some(("deadline/will", b"expired", false)),
        1,
    )
    .await;
    let generation = broker.status().generation;
    faults.mode.store(1, Ordering::Release);
    let mut body = string("deadline/message");
    body.extend([0, 9]);
    body.extend(b"committed-once");
    p.send(codec::frame(0x34, &body)).await;
    tokio::time::timeout(Duration::from_secs(1), faults.entered.notified())
        .await
        .unwrap();
    assert!(
        matches!(
            tokio::time::timeout(Duration::from_secs(2), p.0.read_u8()).await,
            Ok(Err(_))
        ),
        "KeepAlive must close the network while a committed receipt is pending"
    );
    assert!(broker.status().active);
    assert_eq!(broker.status().generation, generation);
    assert_eq!(broker.status().connections, 2); // task/permit retained until commit resolves
    faults.release.notify_one();
    let (_, payload, _, id) = sub.message().await;
    assert_eq!(payload, b"committed-once");
    sub.send(codec::frame(0x40, &id.unwrap().to_be_bytes()))
        .await;
    let (_, payload, _, id) = sub.message().await;
    assert_eq!(payload, b"expired");
    sub.send(codec::frame(0x40, &id.unwrap().to_be_bytes()))
        .await;
    let (mut p, present) = Client::connect(addr, "deadline-writer", false).await;
    assert!(present);
    publish_start(
        &mut p,
        9,
        "deadline/message",
        b"committed-once",
        true,
        false,
    )
    .await;
    release(&mut p, 9).await;
    ping(&mut sub).await; // no duplicate delivery before the PINGRESP
    assert!(broker.status().active);
    assert_eq!(broker.status().generation, generation);
    assert_eq!(broker.status().storage_commit_failures, 0);
    assert_eq!(broker.status().timeouts, 1);
    stop(&app).await;
    drop((app, sub, p));
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn handshake_deadline_during_attach_commit_drains_and_preserves_the_session() {
    let path = database_path();
    let faults = Faults::default();
    let mut c = persistent_config();
    c.handshake_timeout = Duration::from_millis(500);
    c.storage.as_mut().unwrap().timeout = Duration::from_secs(4);
    let (app, broker) = attempt(&path, c, faults.clone()).await;
    let app = app.unwrap();
    let addr = broker.local_addr().unwrap();
    faults.mode.store(1, Ordering::Release);
    let mut p = Client(TcpStream::connect(addr).await.unwrap());
    let mut body = string("MQTT");
    body.extend([4, 0, 0, 0]);
    body.extend(string("late-attach"));
    p.send(codec::frame(0x10, &body)).await;
    tokio::time::timeout(Duration::from_secs(1), faults.entered.notified())
        .await
        .unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), p.0.read_u8()).await,
        Ok(Err(_))
    ));
    assert!(broker.status().active);
    assert_eq!(broker.status().connections, 1);
    faults.release.notify_one();
    let (mut p, present) = Client::connect(addr, "late-attach", false).await;
    assert!(present);
    super::qos2::ping(&mut p).await;
    assert_eq!(broker.status().generation, 1);
    assert_eq!(broker.status().storage_commit_failures, 0);
    stop(&app).await;
    drop((app, p));
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn handshake_deadline_while_waiting_for_store_gate_never_attaches_late() {
    let path = database_path();
    let faults = Faults::default();
    let mut c = persistent_config();
    c.handshake_timeout = Duration::from_millis(500);
    c.storage.as_mut().unwrap().timeout = Duration::from_secs(4);
    let (app, broker) = attempt(&path, c, faults.clone()).await;
    let app = app.unwrap();
    let addr = broker.local_addr().unwrap();
    let (mut p, _) = Client::connect(addr, "gate-holder", false).await;
    faults.mode.store(1, Ordering::Release);
    let mut body = string("gate/message");
    body.extend([0, 9]);
    body.extend(b"held");
    p.send(codec::frame(0x33, &body)).await;
    tokio::time::timeout(Duration::from_secs(1), faults.entered.notified())
        .await
        .unwrap();
    let mut waiting = Client(TcpStream::connect(addr).await.unwrap());
    let mut body = string("MQTT");
    body.extend([4, 0, 0, 0]);
    body.extend(string("must-not-attach"));
    waiting.send(codec::frame(0x10, &body)).await;
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), waiting.0.read_u8()).await,
        Ok(Err(_))
    ));
    faults.release.notify_one();
    assert_eq!(p.recv().await, (0x40, vec![0, 9]));
    super::qos2::ping(&mut p).await;
    assert_eq!(broker.status().sessions, 1);
    assert_eq!(broker.status().generation, 1);
    assert_eq!(broker.status().storage_commit_failures, 0);
    stop(&app).await;
    drop((app, p, waiting));
    std::fs::remove_file(path).unwrap();
}

#[cfg(feature = "tls")]
#[tokio::test]
async fn tls_keepalive_closes_during_commit_without_canceling_the_durable_promise() {
    use super::tls::{connector, tls_config};
    use tokio_rustls::rustls::pki_types::ServerName;
    let path = database_path();
    let faults = Faults::default();
    let mut c = persistent_config();
    c.tls = tls_config().tls;
    c.storage.as_mut().unwrap().timeout = Duration::from_secs(4);
    let (app, broker) = attempt(&path, c, faults.clone()).await;
    let app = app.unwrap();
    let addr = broker.local_addr().unwrap();
    let mut tls = connector(true)
        .connect(
            ServerName::try_from("localhost").unwrap(),
            TcpStream::connect(addr).await.unwrap(),
        )
        .await
        .unwrap();
    let mut body = string("MQTT");
    body.extend([4, 0, 0, 1]);
    body.extend(string("tls-deadline"));
    tls.write_all(&codec::frame(0x10, &body)).await.unwrap();
    let mut reply = [0; 4];
    tls.read_exact(&mut reply).await.unwrap();
    assert_eq!(reply, [0x20, 2, 0, 0]);
    faults.mode.store(1, Ordering::Release);
    let mut body = string("tls/retained");
    body.extend([0, 9]);
    body.extend(b"durable");
    tls.write_all(&codec::frame(0x33, &body)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), faults.entered.notified())
        .await
        .unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), tls.read_u8()).await,
        Ok(Err(_))
    ));
    assert!(broker.status().active);
    assert_eq!(broker.status().connections, 1);
    faults.release.notify_one();
    let mut tls = connector(true)
        .connect(
            ServerName::try_from("localhost").unwrap(),
            TcpStream::connect(addr).await.unwrap(),
        )
        .await
        .unwrap();
    let mut body = string("MQTT");
    body.extend([4, 0, 0, 0]);
    body.extend(string("tls-deadline"));
    tls.write_all(&codec::frame(0x10, &body)).await.unwrap();
    tls.read_exact(&mut reply).await.unwrap();
    assert_eq!(reply, [0x20, 2, 1, 0]);
    assert_eq!(broker.status().retained, 1);
    assert_eq!(broker.status().storage_commit_failures, 0);
    assert_eq!(broker.status().generation, 1);
    stop(&app).await;
    drop((app, tls));
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn shutdown_closes_socket_and_waits_for_committed_receipt_before_releasing_resources() {
    let path = database_path();
    let faults = Faults::default();
    let mut cfg = persistent_config();
    cfg.storage.as_mut().unwrap().timeout = Duration::from_secs(4);
    let (app, broker) = attempt(&path, cfg, faults.clone()).await;
    let app = app.unwrap();
    let (mut p, _) = Client::connect(broker.local_addr().unwrap(), "shutdown-writer", false).await;
    faults.mode.store(1, Ordering::Release);
    let mut body = string("shutdown/retained");
    body.extend([0, 9]);
    body.extend(b"committed");
    p.send(codec::frame(0x33, &body)).await;
    tokio::time::timeout(Duration::from_secs(1), faults.entered.notified())
        .await
        .unwrap();
    // Force maintenance to wait behind the held commit gate before cancellation.
    tokio::time::sleep(Duration::from_millis(150)).await;
    tokio::join!(stop(&app), async {
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), p.0.read_u8()).await,
            Ok(Err(_))
        ));
        assert_eq!(broker.status().connections, 1);
        faults.release.notify_one();
    });
    assert_eq!(broker.status().connections, 0);
    assert_eq!(broker.status().storage_commit_failures, 0);
    drop((app, p));
    let (app, broker) = launch(&path, Faults::default()).await;
    let (mut p, present) =
        Client::connect(broker.local_addr().unwrap(), "shutdown-writer", false).await;
    assert!(present);
    super::qos2::ping(&mut p).await;
    assert_eq!(broker.status().retained, 1);
    stop(&app).await;
    drop((app, p));
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn keepalive_while_waiting_for_store_gate_rejects_publish_after_network_close() {
    let path = database_path();
    let faults = Faults::default();
    let mut cfg = persistent_config();
    cfg.storage.as_mut().unwrap().timeout = Duration::from_secs(4);
    let (app, broker) = attempt(&path, cfg, faults.clone()).await;
    let app = app.unwrap();
    let addr = broker.local_addr().unwrap();
    let (mut blocker, _) = Client::connect(addr, "gate-holder", false).await;
    let (mut expiring, _) = Client::connect_will(addr, "gate-waiter", false, None, 1).await;
    faults.mode.store(1, Ordering::Release);
    let mut body = string("gate/retained");
    body.extend([0, 9]);
    body.extend(b"committed");
    blocker.send(codec::frame(0x33, &body)).await;
    tokio::time::timeout(Duration::from_secs(1), faults.entered.notified())
        .await
        .unwrap();
    let mut body = string("late/retained");
    body.extend([0, 9]);
    body.extend(b"must-not-admit");
    expiring.send(codec::frame(0x33, &body)).await;
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), expiring.0.read_u8()).await,
        Ok(Err(_))
    ));
    faults.release.notify_one();
    assert_eq!(blocker.recv().await, (0x40, vec![0, 9]));
    let (mut recovered, present) = Client::connect(addr, "gate-waiter", false).await;
    assert!(present);
    super::qos2::ping(&mut recovered).await;
    assert_eq!(broker.status().retained, 1);
    assert_eq!(broker.status().generation, 1);
    assert_eq!(broker.status().storage_commit_failures, 0);
    stop(&app).await;
    drop((app, blocker, expiring, recovered));
    std::fs::remove_file(path).unwrap();
}

#[cfg(feature = "mqtt5")]
#[tokio::test]
async fn mqtt5_sqlite_restart_preserves_options_metadata_and_qos2_ownership() {
    use super::mqtt5::{connect, decoded, publish, sub};
    let path = database_path();
    let cfg = MqttBrokerConfig {
        mqtt5: true,
        ..persistent_config()
    };
    let (app, broker) = attempt(&path, cfg.clone(), Faults::default()).await;
    let app = app.unwrap();
    let addr = broker.local_addr().unwrap();
    let (mut subscriber, _) = connect(addr, "v5-durable", true, 60, &[], None).await;
    sub(&mut subscriber, &[("saved/#", 0x0a)], Some(12)).await;
    let (mut publisher, _) = connect(addr, "writer", true, 0, &[], None).await;
    publish(
        &mut publisher,
        "saved/value",
        b"recover",
        2,
        &[0x26, 0, 1, b'k', 0, 1, b'v'],
        true,
    )
    .await;
    let (h, b) = subscriber.recv().await;
    let p = decoded(h, &b);
    assert_eq!(
        p.properties.as_ref().unwrap().subscription_identifiers,
        [12]
    );
    let id = p.pkid;
    subscriber.send(codec::frame(0x50, &id.to_be_bytes())).await;
    assert_eq!(subscriber.recv().await, (0x62, id.to_be_bytes().to_vec()));
    drop((subscriber, publisher));
    stop(&app).await;
    drop(app);
    let (app, broker) = attempt(&path, cfg, Faults::default()).await;
    let app = app.unwrap();
    let (mut subscriber, present) = connect(
        broker.local_addr().unwrap(),
        "v5-durable",
        false,
        60,
        &[],
        None,
    )
    .await;
    assert!(present);
    // Restore the release phase directly, without delivering the publication again.
    assert_eq!(subscriber.recv().await, (0x62, id.to_be_bytes().to_vec()));
    subscriber.send(codec::frame(0x70, &id.to_be_bytes())).await;
    let (mut late, _) = connect(broker.local_addr().unwrap(), "late", true, 0, &[], None).await;
    sub(&mut late, &[("saved/#", 2)], Some(13)).await;
    let (h, b) = late.recv().await;
    let p = decoded(h, &b);
    assert_eq!(p.payload, b"recover"[..]);
    assert!(p.retain);
    let props = p.properties.unwrap();
    assert_eq!(props.subscription_identifiers, [13]);
    assert_eq!(props.user_properties, [("k".into(), "v".into())]);
    stop(&app).await;
    drop((app, subscriber, late));
    std::fs::remove_file(path).unwrap();
}

#[cfg(feature = "mqtt5")]
#[tokio::test]
async fn mqtt5_sqlite_idempotency_properties_keep_their_budget_and_identity_after_restart() {
    let path = database_path();
    let cfg = MqttBrokerConfig {
        mqtt5: true,
        ..persistent_config()
    };
    let (app, broker) = attempt(&path, cfg.clone(), Faults::default()).await;
    let app = app.unwrap();
    let mut requests = vec![];
    for n in 0..3 {
        let request = MqttPublishRequest {
            request_id: format!("metadata-{n}"),
            topic: "idempotency/value".into(),
            payload: b"payload".as_slice().into(),
            qos: Qos::AtLeastOnce,
            retain: false,
            properties: MqttProperties {
                content_type: Some("x".repeat(2048)),
                ..Default::default()
            },
        };
        assert!(internal_publish(&app, request.clone()).await.is_ok());
        requests.push(request);
    }
    // Maintenance temporarily charges a full image workspace; observe the
    // persistent ledger only after that managed job releases its reservation.
    tokio::time::timeout(Duration::from_secs(2), async {
        while broker.status().memory_used_bytes
            >= 4 * cfg.storage.as_ref().unwrap().max_snapshot_bytes
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let before = broker.status();
    assert_eq!(before.publish_records, 3);
    stop(&app).await;
    drop(app);
    let (app, broker) = attempt(&path, cfg, Faults::default()).await;
    let app = app.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while broker.status().memory_used_bytes != before.memory_used_bytes {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let after = broker.status();
    assert_eq!(after.memory_used_bytes, before.memory_used_bytes);
    assert_eq!(after.publish_records, 3);
    for request in requests {
        assert!(internal_publish(&app, request.clone()).await.is_ok());
        let mut changed = request;
        changed.properties.content_type = Some("changed".into());
        assert!(internal_publish(&app, changed).await.is_err());
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        while broker.status().memory_used_bytes != before.memory_used_bytes {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    stop(&app).await;
    drop(app);
    std::fs::remove_file(path).unwrap();
}

#[cfg(feature = "mqtt5")]
#[tokio::test]
async fn mixed_protocol_session_resume_is_rejected_before_fencing_and_clean_switch_survives_restart()
 {
    use super::mqtt5::{connect, sub};
    for identifier in [None, Some(7)] {
        let path = database_path();
        let cfg = MqttBrokerConfig {
            mqtt5: true,
            ..persistent_config()
        };
        let (app, broker) = attempt(&path, cfg.clone(), Faults::default()).await;
        let app = app.unwrap();
        let addr = broker.local_addr().unwrap();
        let (mut original, _) = connect(addr, "mixed", true, 60, &[], None).await;
        sub(
            &mut original,
            &[("mixed/#", if identifier.is_some() { 5 } else { 1 })],
            identifier,
        )
        .await;
        let mut rejected = Client(TcpStream::connect(addr).await.unwrap());
        let mut body = string("MQTT");
        body.extend([4, 0, 0, 0]);
        body.extend(string("mixed"));
        rejected.send(codec::frame(0x10, &body)).await;
        assert_eq!(rejected.recv().await, (0x20, vec![0, 3]));
        super::qos2::ping(&mut original).await; // rejection did not fence the owner
        assert_eq!(broker.status().subscriptions, 1);
        stop(&app).await;
        drop((app, original, rejected));

        let (app, broker) = attempt(&path, cfg.clone(), Faults::default()).await;
        let app = app.unwrap();
        let addr = broker.local_addr().unwrap();
        let (mut original, present) = connect(addr, "mixed", false, 60, &[], None).await;
        assert!(present);
        super::qos2::ping(&mut original).await;
        let (clean, present) = Client::connect(addr, "mixed", true).await;
        assert!(!present);
        assert_eq!(broker.status().subscriptions, 0);
        // Establish a persistent 3.1.1 Session, then reject the reverse switch.
        let (mut legacy, present) = Client::connect(addr, "mixed", false).await;
        assert!(!present);
        legacy.subscribe(&[("legacy/#", 1)]).await;
        let mut rejected = Client(TcpStream::connect(addr).await.unwrap());
        let mut body = string("MQTT");
        body.extend([5, 0, 0, 0, 5, 0x11, 0, 0, 0, 60]);
        body.extend(string("mixed"));
        rejected.send(codec::frame(0x10, &body)).await;
        let (header, ack) = rejected.recv().await;
        assert_eq!(header, 0x20);
        assert_eq!(ack[0], 0);
        assert_ne!(ack[1], 0);
        super::qos2::ping(&mut legacy).await;
        stop(&app).await;
        drop((app, original, clean, legacy, rejected));

        let (app, broker) = attempt(&path, cfg.clone(), Faults::default()).await;
        let app = app.unwrap();
        let addr = broker.local_addr().unwrap();
        let (mut legacy, present) = Client::connect(addr, "mixed", false).await;
        assert!(present);
        assert_eq!(broker.status().subscriptions, 1);
        super::qos2::ping(&mut legacy).await;
        let (mut fresh, present) = connect(addr, "mixed", true, 60, &[], None).await;
        assert!(!present);
        assert_eq!(broker.status().subscriptions, 0);
        super::qos2::ping(&mut fresh).await;
        stop(&app).await;
        drop((app, legacy, fresh));
        let (app, broker) = attempt(&path, cfg, Faults::default()).await;
        let app = app.unwrap();
        let (_fresh, present) =
            connect(broker.local_addr().unwrap(), "mixed", false, 60, &[], None).await;
        assert!(present);
        stop(&app).await;
        drop(app);
        std::fs::remove_file(path).unwrap();
    }
}

#[cfg(feature = "mqtt5")]
#[tokio::test]
async fn mixed_listener_sqlite_restores_packet_boundary_retained_pending_qos2_and_will() {
    for (v5, payload_len) in [(false, 8183), (false, 8184), (true, 8182), (true, 8183)] {
        let path = database_path();
        let cfg = MqttBrokerConfig {
            mqtt5: true,
            ..persistent_config()
        };
        let (app, broker) = attempt(&path, cfg.clone(), Faults::default()).await;
        let app = app.unwrap();
        let addr = broker.local_addr().unwrap();
        let (mut sink, _) = Client::connect(addr, "boundary-sink", false).await;
        sink.subscribe(&[("a", 2)]).await;
        let (mut source, _) = if v5 {
            super::mqtt5::connect(addr, "boundary-source", false, 60, &[], None).await
        } else {
            Client::connect(addr, "boundary-source", false).await
        };
        let payload = vec![b'x'; payload_len];
        let mut body = string("a");
        body.extend([0, 7]);
        if v5 {
            body.push(0);
        }
        body.extend(&payload);
        let frame = codec::frame(0x35, &body); // Retained QoS2: leave incoming ownership armed
        assert_eq!(frame.len(), payload_len + 8 + usize::from(v5));
        source.send(frame).await;
        assert_eq!(source.recv().await, (0x50, vec![0, 7]));
        let (_, got, _, _) = sink.message().await;
        assert_eq!(got, payload);
        // Will PUBLISH representation also fits the same boundary. Graceful
        // shutdown recovery exercises its validation before triggered routing.
        let (will, _) = Client::connect_will(
            addr,
            "boundary-will",
            false,
            Some(("w", &payload[..payload_len - 64], true)),
            0,
        )
        .await;
        assert_eq!(broker.status().retained, 1);
        assert_eq!(broker.status().incoming_qos2, 1);
        stop(&app).await;
        drop((app, source, sink, will));
        let (app, broker) = attempt(&path, cfg, Faults::default()).await;
        let app = app.unwrap();
        let addr = broker.local_addr().unwrap();
        let (mut source, present) = if v5 {
            super::mqtt5::connect(addr, "boundary-source", false, 60, &[], None).await
        } else {
            Client::connect(addr, "boundary-source", false).await
        };
        assert!(present);
        source.send(codec::frame(0x62, &[0, 7])).await;
        assert_eq!(source.recv().await, (0x70, vec![0, 7]));
        let (mut sink, present) = Client::connect(addr, "boundary-sink", false).await;
        assert!(present);
        let (_, got, header, _) = sink.message().await;
        assert_eq!(got, payload);
        assert_ne!(header & 8, 0); // original QoS2 reliable delivery survived
        let (mut late, _) = Client::connect(addr, "boundary-late", true).await;
        late.subscribe(&[("a", 1)]).await;
        let (_, got, header, _) = late.message().await;
        assert_eq!(got, payload);
        assert_eq!(header & 1, 1);
        stop(&app).await;
        drop((app, source, sink, late));
        std::fs::remove_file(path).unwrap();
    }
}

#[cfg(feature = "mqtt5")]
#[tokio::test]
async fn mqtt5_no_local_retained_origin_survives_clean_reconnect_and_sqlite_restart() {
    use super::mqtt5::{connect, decoded, publish, sub};
    let path = database_path();
    let cfg = MqttBrokerConfig {
        mqtt5: true,
        ..persistent_config()
    };
    let (app, broker) = attempt(&path, cfg.clone(), Faults::default()).await;
    let app = app.unwrap();
    let addr = broker.local_addr().unwrap();
    let (mut source, _) = connect(addr, "origin", true, 60, &[], None).await;
    for qos in 0..=2 {
        publish(
            &mut source,
            &format!("origin/{qos}"),
            b"own",
            qos,
            &[],
            true,
        )
        .await;
    }
    sub(&mut source, &[("origin/#", 5)], Some(7)).await;
    super::qos2::ping(&mut source).await;
    // Disable No Local: all three messages are visible, including QoS0.
    sub(&mut source, &[("origin/#", 1)], None).await;
    for _ in 0..3 {
        let (h, b) = source.recv().await;
        let p = decoded(h, &b);
        assert_eq!(p.payload, b"own"[..]);
        if p.pkid != 0 {
            source.send(codec::frame(0x40, &p.pkid.to_be_bytes())).await;
        }
    }
    super::qos2::ping(&mut source).await;
    // A 3.1.1 publisher and its armed retained Will carry the same ClientID.
    // The Will is finalized during recovery, before the listener is opened.
    let (mut will, _) =
        Client::connect_will(addr, "origin", true, Some(("origin/will", b"own", true)), 0).await;
    will.publish("origin/legacy", b"own", 1, true).await;
    internal_publish(
        &app,
        MqttPublishRequest {
            request_id: "retained-internal".into(),
            topic: "business/value".into(),
            payload: b"internal".as_slice().into(),
            qos: Qos::AtLeastOnce,
            retain: true,
            properties: Default::default(),
        },
    )
    .await
    .unwrap();
    stop(&app).await;
    drop((app, source, will));
    let (app, broker) = attempt(&path, cfg, Faults::default()).await;
    let app = app.unwrap();
    let addr = broker.local_addr().unwrap();
    // Identity persists independently of Session/Clean Start and connection ID.
    let (mut source, present) = connect(addr, "origin", true, 0, &[], None).await;
    assert!(!present);
    sub(&mut source, &[("origin/#", 5)], None).await;
    super::qos2::ping(&mut source).await;
    sub(&mut source, &[("business/#", 5)], None).await;
    let (h, b) = source.recv().await;
    let p = decoded(h, &b);
    assert_eq!(p.payload, b"internal"[..]);
    source.send(codec::frame(0x40, &p.pkid.to_be_bytes())).await;
    super::qos2::ping(&mut source).await;
    let (mut other, _) = connect(addr, "other", true, 0, &[], None).await;
    sub(&mut other, &[("origin/#", 5)], None).await;
    for _ in 0..5 {
        let (h, b) = other.recv().await;
        let p = decoded(h, &b);
        assert_eq!(p.payload, b"own"[..]);
        if p.pkid != 0 {
            other.send(codec::frame(0x40, &p.pkid.to_be_bytes())).await;
        }
    }
    super::qos2::ping(&mut other).await;
    stop(&app).await;
    drop((app, source, other));
    std::fs::remove_file(path).unwrap();
}

#[cfg(feature = "mqtt5")]
#[tokio::test]
async fn mqtt5_alias_expansion_sqlite_restores_retained_both_qos2_directions_and_real_properties() {
    use super::mqtt5::{connect, sub};
    // None: original 8191-byte counterexample. Some: 8192 bytes with a
    // two-byte Property Length, actual Content Type and an expanded Topic.
    for content_type in [None, Some("c".repeat(123))] {
        let path = database_path();
        let cfg = MqttBrokerConfig {
            mqtt5: true,
            ..persistent_config()
        };
        let (app, broker) = attempt(&path, cfg.clone(), Faults::default()).await;
        let app = app.unwrap();
        let addr = broker.local_addr().unwrap();
        let topic = "t".repeat(1000);
        let (sink, _) = Client::connect(addr, "alias-sink", false).await;
        let mut sink = sink;
        sink.subscribe(&[(&topic, 2)]).await;
        let (mut v5_sink, _) = connect(addr, "v5-too-large", true, 60, &[], None).await;
        sub(&mut v5_sink, &[(&topic, 1)], None).await;
        let (mut source, _) = connect(addr, "alias-source", true, 60, &[], None).await;
        let mut bind = string(&topic);
        bind.extend([0, 8, 3, 0x23, 0, 1, b'b']);
        source.send(codec::frame(0x32, &bind)).await;
        assert_eq!(source.recv().await, (0x40, vec![0, 8]));
        // Drain the small binding publication; leave only the oversized value
        // owned by the reliable receiver and offline destination.
        let (_, _, _, id) = sink.message().await;
        sink.ack(id.unwrap()).await;
        let (h, b) = v5_sink.recv().await;
        let p = super::mqtt5::decoded(h, &b);
        v5_sink
            .send(codec::frame(0x40, &p.pkid.to_be_bytes()))
            .await;
        super::qos2::ping(&mut v5_sink).await;
        // The legacy sink is deliberately offline before the large publish.
        sink.send(vec![0xe0, 0]).await;
        assert_eq!(sink.0.read(&mut [0]).await.unwrap(), 0);
        drop(sink);
        let mut body = vec![0, 0, 0, 9];
        let payload_len = if let Some(ct) = &content_type {
            body.extend([0x81, 1, 3]);
            body.extend(string(ct));
            body.extend([0x23, 0, 1]);
            8054
        } else {
            body.extend([3, 0x23, 0, 1]);
            8180
        };
        let payload = vec![b'x'; payload_len];
        body.extend(&payload);
        let frame = codec::frame(0x35, &body);
        assert_eq!(
            frame.len(),
            if content_type.is_some() { 8192 } else { 8191 }
        );
        source.send(frame).await;
        assert_eq!(source.recv().await, (0x50, vec![0, 9]));
        // MQTT5 outbound still uses the complete Topic: discard this target's
        // over-limit frame, then serve PING rather than blocking its Session.
        super::qos2::ping(&mut v5_sink).await;
        assert_eq!(broker.status().dropped_oversized, 1);
        assert_eq!(broker.status().incoming_qos2, 1);
        assert!(broker.status().pending_bytes > 8192);
        stop(&app).await;
        drop((app, source, v5_sink));

        let (app, broker) = attempt(&path, cfg.clone(), Faults::default()).await;
        let app = app.unwrap();
        let addr = broker.local_addr().unwrap();
        assert_eq!(broker.status().incoming_qos2, 1);
        assert_eq!(broker.status().retained, 1);
        // Recovery succeeds with the original limit, without promising delivery
        // of a full-Topic frame that exceeds the destination's listener limit.
        let (mut source, present) = connect(addr, "alias-source", false, 60, &[], None).await;
        assert!(present);
        source.send(codec::frame(0x62, &[0, 9])).await;
        assert_eq!(source.recv().await, (0x70, vec![0, 9]));
        stop(&app).await;
        drop((app, source));

        // Explicitly increase the listener limit to drain the expanded output.
        let cfg = MqttBrokerConfig {
            max_packet_bytes: 10000,
            ..cfg
        };
        let (app, broker) = attempt(&path, cfg, Faults::default()).await;
        let app = app.unwrap();
        let addr = broker.local_addr().unwrap();
        let (mut sink, present) = Client::connect(addr, "alias-sink", false).await;
        assert!(present);
        let (got_topic, got, header, id) = sink.message().await;
        assert_eq!(got_topic, topic);
        assert_eq!(got, payload);
        assert_eq!(header & 6, 4);
        sink.send(codec::frame(0x50, &id.unwrap().to_be_bytes()))
            .await;
        assert_eq!(
            sink.recv().await,
            (0x62, id.unwrap().to_be_bytes().to_vec())
        );
        sink.send(codec::frame(0x70, &id.unwrap().to_be_bytes()))
            .await;
        super::qos2::ping(&mut sink).await;
        assert_eq!(broker.status().inflight, 0);
        let (mut late, _) = Client::connect(addr, "alias-late", true).await;
        late.subscribe(&[(&topic, 1)]).await;
        let (got_topic, got, header, _) = late.message().await;
        assert_eq!(got_topic, topic);
        assert_eq!(got, payload);
        assert_eq!(header & 1, 1);
        // The same restored Retained value preserves real MQTT5 properties.

        let (mut large, _) =
            connect(broker.local_addr().unwrap(), "large", true, 0, &[], None).await;
        sub(&mut large, &[(&topic, 1)], None).await;
        let (h, b) = large.recv().await;
        let wire = codec::frame(h, &b);
        let decoded = rumqttc::v5::mqttbytes::v5::Packet::read(
            &mut bytes::BytesMut::from(wire.as_slice()),
            Some(10000),
        )
        .unwrap();
        match decoded {
            rumqttc::v5::mqttbytes::v5::Packet::Publish(p) => {
                assert!(p.retain);
                assert_eq!(p.payload, payload);
                assert_eq!(p.properties.unwrap_or_default().content_type, content_type);
                large.send(codec::frame(0x40, &p.pkid.to_be_bytes())).await;
            }
            _ => panic!("expected retained MQTT5 PUBLISH"),
        }
        stop(&app).await;
        drop((app, sink, late, large));
        std::fs::remove_file(path).unwrap();
    }
}
