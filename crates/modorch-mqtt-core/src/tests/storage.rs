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
            // Keep boundary-sized images paginated without thousands of Hub
            // round trips. The tiny-page/deadline probe below uses 73 bytes.
            recovery_page_bytes: 4096,
            // Healthy integration scenarios are not sub-150ms storage benchmarks.
            // Explicit timeout probes below retain the 150ms boundary.
            timeout: Duration::from_secs(2),
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
    pages: Arc<std::sync::atomic::AtomicUsize>,
    queries: Arc<std::sync::atomic::AtomicUsize>,
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
        if matches!(request.request, StoreRequest::Page { .. }) {
            self.0.pages.fetch_add(1, Ordering::Relaxed);
            if mode == 6 {
                self.0.entered.notify_one();
                self.0.release.notified().await;
            }
        }
        if matches!(request.request, StoreRequest::Query { .. }) {
            self.0.queries.fetch_add(1, Ordering::Relaxed);
            // Commit reached SQLite, but neither receipt nor operation query
            // can be confirmed in this generation. Restart must recover it.
            if mode == 7 {
                return Ok(Arc::new(Err::<StoreReply, _>(StoreError::Unavailable)));
            }
        }
        if mode == 3 && matches!(request.request, StoreRequest::Commit { .. }) {
            return Ok(Arc::new(Err::<StoreReply, _>(StoreError::Unavailable)));
        }
        // Deterministic queue/run timeout probes; no SQLite request has been
        // forwarded yet, so releasing this latch cannot mutate durable state.
        if mode == 5 && matches!(request.request, StoreRequest::Query { .. }) {
            self.0.entered.notify_one();
            self.0.release.notified().await;
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
        if mode == 7 && matches!(request.request, StoreRequest::Commit { .. }) {
            self.0.entered.notify_one();
            return Ok(Arc::new(Err::<StoreReply, _>(StoreError::Unavailable)));
        }
        if matches!(request.request, StoreRequest::Commit { .. }) && (mode == 1 || mode == 2) {
            self.0.mode.store(0, Ordering::Release);
            self.0.entered.notify_one();
            if mode == 1 {
                self.0.release.notified().await;
            } else {
                // The real SQLite commit has completed. Lose its receipt
                // deterministically; do not couple recovery correctness to a
                // 60ms scheduling gap between two short request deadlines.
                return Ok(Arc::new(Err::<StoreReply, _>(StoreError::Unavailable)));
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
    c.storage.as_mut().unwrap().recovery_page_bytes = 1024;
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
    for (v5, phase) in [false, cfg!(feature = "mqtt5")]
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .flat_map(|v5| (0..4).map(move |phase| (v5, phase)))
    {
        let path = database_path();
        let faults = Faults::default();
        let (app, broker) = launch_qos2(&path, faults.clone()).await;
        let addr = broker.local_addr().unwrap();
        let (mut p, _) = fault_connect(addr, "writer", v5).await;
        let (mut sub, _) = fault_connect(addr, "reader", v5).await;
        fault_subscribe(&mut sub, v5).await;
        let mut outbound = 0;
        if phase != 0 {
            fault_publish(&mut p, v5, b"committed").await;
            assert_eq!(p.recv().await, (0x50, vec![0, 9]));
            let (_, b) = sub.recv().await;
            let n = usize::from(u16::from_be_bytes([b[0], b[1]]));
            outbound = u16::from_be_bytes([b[n + 2], b[n + 3]]);
            if phase == 3 {
                received(&mut sub, outbound).await;
            }
        }
        faults.mode.store(3, Ordering::Release);
        let victim = match phase {
            0 => {
                let mut b = string("q2");
                b.extend([0, 9]);
                if v5 {
                    b.push(0);
                }
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
        let (mut p, _) = fault_connect(addr, "writer", v5).await;
        if phase != 0 {
            release(&mut p, 9).await;
        }
        let (mut sub, _) = fault_connect(addr, "reader", v5).await;
        if phase == 3 {
            assert_eq!(sub.recv().await, (0x62, outbound.to_be_bytes().to_vec()));
            complete(&mut sub, outbound).await;
        } else if phase != 0 {
            let (h, b) = sub.recv().await;
            let (payload, header, id) = if v5 {
                #[cfg(feature = "mqtt5")]
                {
                    let m = super::mqtt5::decoded(h, &b);
                    (m.payload.to_vec(), h, Some(m.pkid))
                }
                #[cfg(not(feature = "mqtt5"))]
                {
                    unreachable!()
                }
            } else {
                let n = usize::from(u16::from_be_bytes([b[0], b[1]]));
                (
                    b[n + 4..].to_vec(),
                    h,
                    Some(u16::from_be_bytes([b[n + 2], b[n + 3]])),
                )
            };
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

// These tests exercise phase correctness. Short uncertainty deadlines are
// installed only at their explicit fault injection points.
async fn launch_qos2(path: &std::path::Path, faults: Faults) -> (ApplicationHandle, BrokerHandle) {
    let mut c = persistent_config();
    c.mqtt5 = cfg!(feature = "mqtt5");
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
    manual_clock(async {
        faults.mode.store(1, Ordering::Release);
        let mut p = Client(TcpStream::connect(addr).await.unwrap());
        let mut body = string("MQTT");
        body.extend([4, 0, 0, 0]);
        body.extend(string("late-attach"));
        p.send(codec::frame(0x10, &body)).await;
        tokio::time::timeout(Duration::from_secs(1), faults.entered.notified())
            .await
            .unwrap();
        tokio::time::advance(Duration::from_millis(499)).await;
        assert_eq!(
            p.0.try_read(&mut [0]).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        tokio::time::advance(Duration::from_millis(2)).await;
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
    })
    .await;
    stop(&app).await;
    drop(app);
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
    manual_clock(async {
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
        while broker.status().connections != 2 {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_millis(499)).await;
        assert_eq!(
            waiting.0.try_read(&mut [0]).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        tokio::time::advance(Duration::from_millis(2)).await;
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
    })
    .await;
    stop(&app).await;
    drop(app);
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
        let started = std::time::Instant::now();
        let trace = |phase: &str, broker: &BrokerHandle| {
            if std::env::var_os("MQTT_TEST_TIMING").is_some() {
                eprintln!(
                    "alias recovery: properties={} phase={phase} elapsed={:?} status={:?}",
                    content_type.is_some(),
                    started.elapsed(),
                    broker.status()
                );
            }
        };
        let path = database_path();
        let cfg = MqttBrokerConfig {
            mqtt5: true,
            ..persistent_config()
        };
        let (app, broker) = attempt(&path, cfg.clone(), Faults::default()).await;
        let app = app.unwrap();
        let addr = broker.local_addr().unwrap();
        let topic = "t".repeat(1000);
        trace("initial legacy CONNECT/SUBACK", &broker);
        let (sink, _) = Client::connect(addr, "alias-sink", false).await;
        let mut sink = sink;
        sink.subscribe(&[(&topic, 2)]).await;
        trace("initial v5 CONNECT/SUBACK", &broker);
        let (mut v5_sink, _) = connect(addr, "v5-too-large", true, 60, &[], None).await;
        sub(&mut v5_sink, &[(&topic, 1)], None).await;
        trace("source CONNECT/alias binding PUBACK", &broker);
        let (mut source, _) = connect(addr, "alias-source", true, 60, &[], None).await;
        let mut bind = string(&topic);
        bind.extend([0, 8, 3, 0x23, 0, 1, b'b']);
        source.send(codec::frame(0x32, &bind)).await;
        assert_eq!(source.recv().await, (0x40, vec![0, 8]));
        // Drain the small binding publication; leave only the oversized value
        // owned by the reliable receiver and offline destination.
        trace("binding legacy PUBLISH", &broker);
        let (_, _, _, id) = sink.message().await;
        sink.ack(id.unwrap()).await;
        trace("binding v5 PUBLISH/PINGRESP", &broker);
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
        trace("large incoming PUBREC", &broker);
        assert_eq!(source.recv().await, (0x50, vec![0, 9]));
        // MQTT5 outbound still uses the complete Topic: discard this target's
        // over-limit frame, then serve PING rather than blocking its Session.
        super::qos2::ping(&mut v5_sink).await;
        trace("first shutdown", &broker);
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
        trace("original-limit source resume/PUBCOMP", &broker);
        // Recovery succeeds with the original limit, without promising delivery
        // of a full-Topic frame that exceeds the destination's listener limit.
        let (mut source, present) = connect(addr, "alias-source", false, 60, &[], None).await;
        assert!(present);
        source.send(codec::frame(0x62, &[0, 9])).await;
        assert_eq!(source.recv().await, (0x70, vec![0, 9]));
        trace("second shutdown", &broker);
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
        trace("raised-limit sink resume/PUBLISH", &broker);
        let (mut sink, present) = Client::connect(addr, "alias-sink", false).await;
        assert!(present);
        let (got_topic, got, header, id) = sink.message().await;
        assert_eq!(got_topic, topic);
        assert_eq!(got, payload);
        assert_eq!(header & 6, 4);
        sink.send(codec::frame(0x50, &id.unwrap().to_be_bytes()))
            .await;
        trace("raised-limit sink PUBREL", &broker);
        assert_eq!(
            sink.recv().await,
            (0x62, id.unwrap().to_be_bytes().to_vec())
        );
        sink.send(codec::frame(0x70, &id.unwrap().to_be_bytes()))
            .await;
        super::qos2::ping(&mut sink).await;
        trace("legacy retained CONNECT/SUBACK/PUBLISH", &broker);
        assert_eq!(broker.status().inflight, 0);
        let (mut late, _) = Client::connect(addr, "alias-late", true).await;
        late.subscribe(&[(&topic, 1)]).await;
        let (got_topic, got, header, _) = late.message().await;
        assert_eq!(got_topic, topic);
        assert_eq!(got, payload);
        assert_eq!(header & 1, 1);
        // The same restored Retained value preserves real MQTT5 properties.
        trace("v5 retained CONNECT/SUBACK/PUBLISH", &broker);

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
        trace("complete", &broker);
        drop((app, sink, late, large));
        std::fs::remove_file(path).unwrap();
    }
}

#[cfg(feature = "mqtt5")]
#[tokio::test]
async fn mqtt5_expired_delay_will_survives_sqlite_restart_and_repeated_session_resume() {
    use super::mqtt5::{connect, decoded, publish, sub, wait_will_status};
    // Cover both a saved delay that expires after recovery and an already
    // triggered, frozen-target Will saved while its reliable target is full.
    for triggered_before_restart in [false, true] {
        let path = database_path();
        let cfg = MqttBrokerConfig {
            mqtt5: true,
            max_offline_messages_per_client: 1,
            max_inflight_per_client: 1,
            ..persistent_config()
        };
        let (app, broker) = attempt(&path, cfg.clone(), Faults::default()).await;
        let app = app.unwrap();
        let address = broker.local_addr().unwrap();
        let (mut sink, _) = connect(address, "sink", true, 60, &[], None).await;
        sub(&mut sink, &[("v5/#", 1)], Some(17)).await;
        let delay = if triggered_before_restart { 1 } else { 2 };
        let (mut source, _) = connect(address, "source", true, 60, &[], Some((delay, 60))).await;
        publish(&mut source, "v5/blocker", b"blocker", 1, &[], false).await;
        let (header, body) = sink.recv().await;
        let blocker = decoded(header, &body);
        drop(source);
        wait_will_status(&broker, |s| s.connections == 1).await;
        if triggered_before_restart {
            wait_will_status(&broker, |s| s.pending_wills == 1).await;
        } else {
            assert_eq!(broker.status().pending_wills, 0);
        }
        stop(&app).await;
        drop((app, sink));

        let (app, broker) = attempt(&path, cfg.clone(), Faults::default()).await;
        let app = app.unwrap();
        let address = broker.local_addr().unwrap();
        wait_will_status(&broker, |s| s.pending_wills == 1).await;
        let (mut source, present) = connect(address, "source", false, 60, &[], None).await;
        assert!(present);
        assert_eq!(broker.status().pending_wills, 1);
        source.send(vec![0xe0, 0]).await;
        assert_eq!(source.0.read(&mut [0]).await.unwrap(), 0);
        assert_eq!(broker.status().pending_wills, 1);
        // Save again after the resume, then recover the same pending Will.
        stop(&app).await;
        drop((app, source));

        let (app, broker) = attempt(&path, cfg, Faults::default()).await;
        let app = app.unwrap();
        let address = broker.local_addr().unwrap();
        assert_eq!(broker.status().pending_wills, 1);
        let (mut source, present) = connect(address, "source", false, 60, &[], None).await;
        assert!(present);
        let (mut sink, present) = connect(address, "sink", false, 60, &[], None).await;
        assert!(present);
        let (header, body) = sink.recv().await;
        let replay = decoded(header, &body);
        assert!(replay.dup);
        assert_eq!(replay.pkid, blocker.pkid);
        assert_eq!(replay.payload, b"blocker"[..]);
        sink.send(codec::frame(0x40, &replay.pkid.to_be_bytes()))
            .await;
        let (header, body) = sink.recv().await;
        let will = decoded(header, &body);
        assert_eq!(will.topic, b"v5/will"[..]);
        assert_eq!(will.payload, b"w"[..]);
        assert_eq!(will.properties.unwrap().subscription_identifiers, [17]);
        sink.send(codec::frame(0x40, &will.pkid.to_be_bytes()))
            .await;
        sink.send(vec![0xc0, 0]).await;
        assert_eq!(sink.recv().await, (0xd0, vec![]));
        publish(&mut source, "v5/barrier", b"after-restart", 1, &[], false).await;
        let (header, body) = sink.recv().await;
        let barrier = decoded(header, &body);
        assert_eq!(barrier.payload, b"after-restart"[..]);
        sink.send(codec::frame(0x40, &barrier.pkid.to_be_bytes()))
            .await;
        sink.send(vec![0xc0, 0]).await;
        assert_eq!(sink.recv().await, (0xd0, vec![]));
        assert_eq!(broker.status().pending_wills, 0);
        assert_eq!(broker.status().will_records, 0);
        assert_eq!(broker.status().pending_messages, 0);
        stop(&app).await;
        drop((app, source, sink));
        std::fs::remove_file(path).unwrap();
    }
}

#[cfg(feature = "mqtt5")]
#[tokio::test]
async fn mqtt5_disconnect_reason_will_state_survives_sqlite_restart() {
    use super::mqtt5::{connect, decoded, publish, sub};
    for (reason, empty) in [(0, true), (0, false), (0x80, false)] {
        let path = database_path();
        let cfg = MqttBrokerConfig {
            mqtt5: true,
            ..persistent_config()
        };
        let (app, broker) = attempt(&path, cfg.clone(), Faults::default()).await;
        let app = app.unwrap();
        let address = broker.local_addr().unwrap();
        let (mut sink, _) = connect(address, "sink", false, 60, &[], None).await;
        sub(&mut sink, &[("v5/#", 1)], Some(17)).await;
        sink.disconnect().await;
        let (mut owner, _) = connect(address, "owner", true, 60, &[], Some((1, 30))).await;
        // Both RL=0 and RL=1 omit Property Length in the actual input.
        let reason_bytes = [reason];
        owner
            .send(codec::frame(0xe0, if empty { &[] } else { &reason_bytes }))
            .await;
        assert_eq!(owner.0.read(&mut [0]).await.unwrap(), 0);
        assert_eq!(broker.status().will_records, usize::from(reason != 0));
        stop(&app).await;
        drop((app, owner));

        let (app, broker) = attempt(&path, cfg, Faults::default()).await;
        let app = app.unwrap();
        let address = broker.local_addr().unwrap();
        let (mut sink, present) = connect(address, "sink", false, 60, &[], None).await;
        assert!(present);
        if reason != 0 {
            let (h, b) = sink.recv().await;
            let will = decoded(h, &b);
            assert_eq!(will.topic, b"v5/will"[..]);
            assert_eq!(will.properties.unwrap().subscription_identifiers, [17]);
            sink.send(codec::frame(0x40, &will.pkid.to_be_bytes()))
                .await;
        }
        let (mut source, _) = connect(address, "source", true, 0, &[], None).await;
        publish(&mut source, "v5/barrier", b"after-restart", 1, &[], false).await;
        let (h, b) = sink.recv().await;
        let barrier = decoded(h, &b);
        assert_eq!(barrier.topic, b"v5/barrier"[..]);
        sink.send(codec::frame(0x40, &barrier.pkid.to_be_bytes()))
            .await;
        sink.send(vec![0xc0, 0]).await;
        assert_eq!(sink.recv().await, (0xd0, vec![]));
        assert_eq!(broker.status().will_records, 0);
        stop(&app).await;
        drop((app, sink, source));
        std::fs::remove_file(path).unwrap();
    }
}

#[tokio::test]
async fn paginated_recovery_deadline_closes_ingress_and_retry_preserves_committed_image() {
    let path = database_path();
    let (app, broker) = launch(&path, Faults::default()).await;
    let (mut source, _) = Client::connect(broker.local_addr().unwrap(), "writer", true).await;
    source.publish("recover/value", b"committed", 1, true).await;
    stop(&app).await;
    drop((app, source));

    let faults = Faults::default();
    faults.mode.store(6, Ordering::Release);
    let mut cfg = persistent_config();
    let store = cfg.storage.as_mut().unwrap();
    store.recovery_page_bytes = 73;
    store.recovery_timeout = Duration::from_millis(150);
    let recovery = attempt(&path, cfg, faults.clone());
    tokio::pin!(recovery);
    let (result, broker) = tokio::select! {
        // On an overloaded host the absolute deadline may expire during Begin
        // itself, before Page reaches the latch. That is the same fail-closed
        // outcome, and must not strand the test waiting for an impossible Page.
        result = &mut recovery => result,
        _ = faults.entered.notified() => {
            // Hold Page across the whole recovery deadline; the individual
            // request still has 2s. Abort drains after this release.
            tokio::time::sleep(Duration::from_millis(300)).await;
            faults.mode.store(0, Ordering::Release);
            faults.release.notify_one();
            recovery.await
        }
    };
    let error = result.err().expect("recovery must fail closed");
    assert!(
        format!("{error:?}").contains("RecoveryDeadline"),
        "{error:?}"
    );
    assert!(
        TcpStream::connect(broker.local_addr().unwrap())
            .await
            .is_err()
    );

    let faults = Faults::default();
    let mut cfg = persistent_config();
    cfg.storage.as_mut().unwrap().recovery_page_bytes = 73;
    let (app, broker) = attempt(&path, cfg, faults.clone()).await;
    let app = app.unwrap();
    assert!(faults.pages.load(Ordering::Relaxed) > 1);
    let (mut sink, _) = Client::connect(broker.local_addr().unwrap(), "reader", true).await;
    sink.subscribe(&[("recover/#", 1)]).await;
    let (topic, payload, header, _) = sink.message().await;
    assert_eq!(topic, "recover/value");
    assert_eq!(payload, b"committed");
    assert_eq!(header & 1, 1);
    stop(&app).await;
    drop((app, sink));
    std::fs::remove_file(&path).unwrap();
}

#[tokio::test]
async fn store_diagnostics_distinguish_admission_queued_running_and_provider_failures() {
    use crate::persistence::{Hub, call};
    let path = database_path();
    let faults = Faults::default();
    let (app, _) = launch(&path, faults.clone()).await;
    let hub = app.event_hub().external();
    let mut cfg = persistent_config().storage.unwrap();
    cfg.timeout = Duration::from_secs(2);
    let query = || StoreRequest::Query {
        epoch: 0,
        operation_id: "probe".into(),
        digest: [0; 32],
    };
    let check = |err: MqttError, expected| {
        assert!(
            matches!(err, MqttError::Storage { operation: "query", failure } if failure == expected),
            "{err:?}"
        );
    };
    let mut bad = cfg.clone();
    bad.component = "missing-store".into();
    check(
        call(Hub::External(&hub), &bad, query()).await.unwrap_err(),
        StoreCallFailure::Admission,
    );
    bad = cfg.clone();
    bad.access = StoreAccessKey([42; 32]);
    // Proxy itself does not authenticate; the real SQLite Provider checks it.
    check(
        call(Hub::External(&hub), &bad, query()).await.unwrap_err(),
        StoreCallFailure::Provider(StoreError::NotAuthorized),
    );

    manual_clock(async {
        let mut short = cfg.clone();
        short.timeout = Duration::from_millis(150);
        faults.mode.store(5, Ordering::Release);
        let running = call(Hub::External(&hub), &short, query());
        tokio::pin!(running);
        tokio::select! {
            biased;
            result = &mut running => panic!("query did not reach latch: {result:?}"),
            _ = faults.entered.notified() => {},
        }
        tokio::time::advance(Duration::from_millis(151)).await;
        check(running.await.unwrap_err(), StoreCallFailure::Indeterminate);
        // Occupied serial handler: expire a second request while still Queued.
        let queued = call(Hub::External(&hub), &short, query());
        tokio::pin!(queued);
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(
                queued.as_mut().poll(cx).is_pending()
            ))
            .await
        );
        tokio::time::advance(Duration::from_millis(151)).await;
        check(
            queued.await.unwrap_err(),
            StoreCallFailure::CanceledBeforeStart,
        );
    })
    .await;
    faults.mode.store(0, Ordering::Release);
    faults.release.notify_one();
    // A subsequent typed request verifies the serial queue drains after release.
    check(
        call(Hub::External(&hub), &cfg, query()).await.unwrap_err(),
        StoreCallFailure::Provider(StoreError::Fenced),
    );
    stop(&app).await;
    drop(app);
    std::fs::remove_file(path).unwrap();
}

async fn fault_connect(addr: SocketAddr, id: &str, v5: bool) -> (Client, bool) {
    #[cfg(feature = "mqtt5")]
    if v5 {
        return super::mqtt5::connect(addr, id, false, 60, &[], None).await;
    }
    let _ = v5;
    Client::connect(addr, id, false).await
}

#[tokio::test]
async fn ping_on_idle_connection_does_not_wait_for_another_connections_store_commit() {
    for v5 in [false, cfg!(feature = "mqtt5")]
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>()
    {
        let path = database_path();
        let faults = Faults::default();
        let mut cfg = persistent_config();
        cfg.mqtt5 = cfg!(feature = "mqtt5");
        cfg.storage.as_mut().unwrap().timeout = Duration::from_secs(4);
        let (app, broker) = attempt(&path, cfg, faults.clone()).await;
        let app = app.unwrap();
        let addr = broker.local_addr().unwrap();
        let (mut idle, _) = fault_connect(addr, "ping-only", v5).await;
        let (mut writer, _) = fault_connect(addr, "writer", v5).await;
        super::qos2::ping(&mut idle).await;
        faults.mode.store(1, Ordering::Release);
        fault_publish(&mut writer, v5, b"held-durable-promise").await;
        writer.send(vec![0xc0, 0]).await;
        tokio::time::timeout(Duration::from_secs(1), faults.entered.notified())
            .await
            .unwrap();
        idle.send(vec![0xc0, 0]).await;
        let response = tokio::time::timeout(Duration::from_secs(1), idle.recv()).await;
        assert_eq!(
            writer.0.try_read(&mut [0]).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock,
            "same-connection PING overtook durable PUBREC"
        );
        // Always release the actual SQLite commit, even on the pre-fix failure.
        faults.release.notify_one();
        assert_eq!(writer.recv().await, (0x50, vec![0, 9]));
        assert_eq!(writer.recv().await, (0xd0, vec![]));
        assert_eq!(
            response.expect("PINGRESP blocked by unrelated Store commit"),
            (0xd0, vec![])
        );
        assert_eq!(broker.status().incoming_qos2, 1);
        assert_eq!(broker.status().storage_commit_failures, 0);
        stop(&app).await;
        drop((app, idle, writer));
        std::fs::remove_file(path).unwrap();
    }
}
async fn fault_subscribe(c: &mut Client, v5: bool) {
    let mut b = vec![0, 1];
    if v5 {
        b.push(0);
    }
    b.extend(string("q2"));
    b.push(2);
    c.send(codec::frame(0x82, &b)).await;
    let (h, b) = c.recv().await;
    assert_eq!(h, 0x90);
    assert_eq!(b, if v5 { vec![0, 1, 0, 2] } else { vec![0, 1, 2] });
}
async fn fault_publish(c: &mut Client, v5: bool, payload: &[u8]) {
    let mut b = string("q2");
    b.extend([0, 9]);
    if v5 {
        b.push(0);
    }
    b.extend(payload);
    c.send(codec::frame(0x34, &b)).await;
}

#[tokio::test]
async fn qos2_receiver_duplicate_content_retained_and_id_reuse_survive_sqlite_restart() {
    async fn send(c: &mut Client, v5: bool, dup: bool, payload: &[u8]) {
        let mut body = string("q2");
        body.extend([0, 9]);
        if v5 {
            // Real Content Type property differs between original/duplicate.
            body.extend([4, 3, 0, 1, payload[0]]);
        }
        body.extend(payload);
        c.send(codec::frame(if dup { 0x3d } else { 0x35 }, &body))
            .await;
        assert_eq!(c.recv().await, (0x50, vec![0, 9]));
    }
    async fn delivery(c: &mut Client, v5: bool, payload: &[u8], retained: bool) {
        let (h, body) = c.recv().await;
        assert_eq!((h & 7, h & 8), (4 | u8::from(retained), 0));
        let n = usize::from(u16::from_be_bytes([body[0], body[1]]));
        let id = [body[n + 2], body[n + 3]];
        if v5 {
            #[cfg(feature = "mqtt5")]
            {
                let p = super::mqtt5::decoded(h, &body);
                assert_eq!(p.payload, payload);
                assert_eq!(
                    p.properties.unwrap().content_type.as_deref(),
                    Some(std::str::from_utf8(&payload[..1]).unwrap())
                );
            }
        } else {
            assert_eq!(&body[n + 4..], payload);
        }
        c.send(codec::frame(0x50, &id)).await;
        assert_eq!(c.recv().await, (0x62, id.to_vec()));
        c.send(codec::frame(0x70, &id)).await;
        // FIFO barrier makes an accidentally rerouted duplicate observable.
        super::qos2::ping(c).await;
    }
    for v5 in [false, cfg!(feature = "mqtt5")]
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>()
    {
        for restart in [false, true] {
            for dup in [false, true] {
                eprintln!(
                    "receiver wire matrix: mqtt5={v5} sqlite_restart={restart} duplicate_flag={dup}"
                );
                let path = database_path();
                let (mut app, mut broker) = launch_qos2(&path, Faults::default()).await;
                let (mut sink, _) = fault_connect(broker.local_addr().unwrap(), "reader", v5).await;
                fault_subscribe(&mut sink, v5).await;
                sink.send(vec![0xe0, 0]).await;
                assert_eq!(sink.0.read(&mut [0]).await.unwrap(), 0);
                drop(sink);
                let (mut source, _) =
                    fault_connect(broker.local_addr().unwrap(), "writer", v5).await;
                send(&mut source, v5, false, b"original").await;
                source.send(vec![0xe0, 0]).await;
                assert_eq!(source.0.read(&mut [0]).await.unwrap(), 0);
                drop(source);
                if restart {
                    stop(&app).await;
                    drop(app);
                    (app, broker) = launch_qos2(&path, Faults::default()).await;
                }
                let addr = broker.local_addr().unwrap();
                let (mut source, present) = fault_connect(addr, "writer", v5).await;
                assert!(present);
                // Method B identifies the receiver-owned exchange by Packet ID,
                // even if a peer changes DUP, payload and application properties.
                send(&mut source, v5, dup, b"changed-must-not-replace").await;
                assert_eq!(broker.status().incoming_qos2, 1);
                let (mut sink, present) = fault_connect(addr, "reader", v5).await;
                assert!(present);
                delivery(&mut sink, v5, b"original", false).await;
                let (mut late, _) = fault_connect(addr, "late", v5).await;
                fault_subscribe(&mut late, v5).await;
                delivery(&mut late, v5, b"original", true).await;
                late.send(vec![0xe0, 0]).await;
                assert_eq!(late.0.read(&mut [0]).await.unwrap(), 0);
                for _ in 0..2 {
                    source.send(codec::frame(0x62, &[0, 9])).await;
                    assert_eq!(source.recv().await, (0x70, vec![0, 9]));
                }
                assert_eq!(broker.status().incoming_qos2, 0);
                // PUBCOMP releases the ID; the next PUBLISH is new ownership.
                send(&mut source, v5, false, b"reused").await;
                delivery(&mut sink, v5, b"reused", false).await;
                assert_eq!(broker.status().incoming_qos2, 1);
                assert_eq!(broker.status().storage_commit_failures, 0);
                stop(&app).await;
                drop((app, sink, source, late));
                std::fs::remove_file(path).unwrap();
            }
        }
    }
}

#[cfg(feature = "mqtt5")]
#[tokio::test]
async fn mqtt5_sqlite_recovered_pubrel_has_fresh_connection_quota() {
    use super::mqtt5::{connect, decoded, publish, sub};
    let path = database_path();
    let (app, broker) = launch_qos2(&path, Faults::default()).await;
    let addr = broker.local_addr().unwrap();
    let (mut sink, _) = connect(addr, "quota", false, 60, &[0x21, 0, 1], None).await;
    sub(&mut sink, &[("q2", 2)], None).await;
    let (mut source, _) = connect(addr, "source", true, 0, &[], None).await;
    publish(&mut source, "q2", b"old", 2, &[], false).await;
    let (h, b) = sink.recv().await;
    let old = decoded(h, &b);
    sink.send(codec::frame(0x50, &old.pkid.to_be_bytes())).await;
    assert_eq!(sink.recv().await, (0x62, old.pkid.to_be_bytes().to_vec()));
    stop(&app).await;
    drop((app, sink, source));
    let (app, broker) = launch_qos2(&path, Faults::default()).await;
    let addr = broker.local_addr().unwrap();
    let (mut sink, present) = connect(addr, "quota", false, 60, &[0x21, 0, 1], None).await;
    assert!(present);
    assert_eq!(sink.recv().await, (0x62, old.pkid.to_be_bytes().to_vec()));
    let (mut source, _) = connect(addr, "source", true, 0, &[], None).await;
    publish(&mut source, "q2", b"new", 1, &[], false).await;
    let (h, b) = sink.recv().await;
    let new = decoded(h, &b);
    assert_eq!(new.payload, b"new"[..]);
    sink.send(codec::frame(0x70, &old.pkid.to_be_bytes())).await;
    sink.send(codec::frame(0x40, &new.pkid.to_be_bytes())).await;
    sink.send(vec![0xc0, 0]).await;
    assert_eq!(sink.recv().await, (0xd0, vec![]));
    stop(&app).await;
    drop(app);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn actual_committed_store_timeout_is_resolved_by_original_operation_before_next_commit() {
    use crate::persistence::{Hub, call};
    use sha2::{Digest, Sha256};
    let path = database_path();
    let cfg = persistent_config().storage.unwrap();
    let faults = Faults::default();
    let mut builder = ApplicationBuilder::new("commit-timeout-probe");
    builder
        .register_module(
            SqliteStorageFactory::new(SqliteStorageConfig {
                name: "sqlite-store".into(),
                broker: "mqtt-broker".into(),
                subscription_id: 5000,
                access: cfg.access.clone(),
                database: path.clone(),
                max_snapshot_bytes: 1024 * 1024,
                disk_budget_bytes: 12 * 1024 * 1024,
            })
            .unwrap(),
        )
        .unwrap();
    builder.register_module(Proxy(faults.clone())).unwrap();
    let mut selection = ComponentSelection::new();
    selection
        .enable_module("sqlite-store")
        .enable_module("store-proxy");
    let app = builder
        .selection(selection)
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();
    let hub = app.event_hub().external();
    let StoreReply::Recovery { epoch, version, .. } = call(
        Hub::External(&hub),
        &cfg,
        StoreRequest::Begin {
            recovery_id: "timeout-probe".into(),
        },
    )
    .await
    .unwrap() else {
        panic!()
    };
    call(Hub::External(&hub), &cfg, StoreRequest::Complete { epoch })
        .await
        .unwrap();
    let image = bytes::Bytes::from_static(b"opaque-provider-test-image");
    let digest: [u8; 32] = Sha256::digest(&image).into();
    let operation_id = format!("{epoch}:{}", version + 1);
    faults.mode.store(1, Ordering::Release);
    let handle = hub
        .request::<_, StoreResult>(
            STORAGE_TOPIC,
            Destination::Component(ComponentRef::module("store-proxy")),
            StoreEnvelope {
                access: cfg.access.clone(),
                request: StoreRequest::Commit {
                    epoch,
                    expected_version: version,
                    operation_id: operation_id.clone(),
                    digest,
                    image: image.clone(),
                },
            },
            MessageOptions {
                payload_bytes: Some(4096),
                ..Default::default()
            },
        )
        .unwrap();
    // Observe a real SQLite commit before starting the short wait. This avoids
    // assuming disk work finishes within a fixed sleep/scheduling gap.
    tokio::time::timeout(Duration::from_secs(3), faults.entered.notified())
        .await
        .unwrap();
    assert!(matches!(
        handle.wait(Duration::from_millis(150)).await,
        Err(FrameworkError::Indeterminate(_))
    ));
    faults.release.notify_one();
    assert!(
        matches!(call(Hub::External(&hub),&cfg,StoreRequest::Query{epoch,operation_id,digest}).await.unwrap(),StoreReply::Committed{version:v} if v==version+1)
    );
    assert!(
        matches!(call(Hub::External(&hub),&cfg,StoreRequest::Commit{epoch,expected_version:version+1,operation_id:format!("{epoch}:{}",version+2),digest,image}).await.unwrap(),StoreReply::Committed{version:v} if v==version+2)
    );
    stop(&app).await;
    drop(app);
    std::fs::remove_file(path).unwrap();
}

#[cfg(feature = "mqtt5")]
#[tokio::test]
async fn mqtt5_shared_offline_sqlite_restart_preserves_selected_owners_and_expiry() {
    use super::mqtt5::{connect, publish, sub};
    use super::mqtt5_protocol::receive_reliable;
    let path = database_path();
    let cfg = MqttBrokerConfig {
        mqtt5: true,
        ..persistent_config()
    };
    let (app, broker) = attempt(&path, cfg.clone(), Faults::default()).await;
    let app = app.unwrap();
    let addr = broker.local_addr().unwrap();
    for id in ["a", "b"] {
        let (mut c, _) = connect(addr, id, true, 60, &[], None).await;
        sub(&mut c, &[("$share/g/jobs", 2)], Some(7)).await;
        c.send(vec![0xe0, 0]).await;
        assert_eq!(c.0.read(&mut [0]).await.unwrap(), 0);
    }
    let (mut source, _) = connect(addr, "source", true, 0, &[], None).await;
    publish(&mut source, "jobs", b"one", 1, &[], false).await;
    publish(&mut source, "jobs", b"two", 2, &[], false).await;
    // Expiry=0 must not survive disconnection or receive shared work.
    let (mut gone, _) = connect(addr, "gone", true, 0, &[], None).await;
    sub(&mut gone, &[("$share/g/jobs", 2)], None).await;
    gone.send(vec![0xe0, 0]).await;
    assert_eq!(gone.0.read(&mut [0]).await.unwrap(), 0);
    // Expired publications do not enter any member's reliable queue.
    publish(&mut source, "jobs", b"expired", 1, &[2, 0, 0, 0, 0], false).await;
    stop(&app).await;
    drop((app, source, gone));
    let (app, broker) = attempt(&path, cfg, Faults::default()).await;
    let app = app.unwrap();
    let addr = broker.local_addr().unwrap();
    let (mut a, present) = connect(addr, "a", false, 60, &[], None).await;
    assert!(present);
    let (mut b, present) = connect(addr, "b", false, 60, &[], None).await;
    assert!(present);
    receive_reliable(&mut a, b"one", 1).await;
    receive_reliable(&mut b, b"two", 2).await;
    for c in [&mut a, &mut b] {
        c.send(vec![0xc0, 0]).await;
        assert_eq!(c.recv().await, (0xd0, vec![]));
    }
    let (_gone, present) = connect(addr, "gone", false, 60, &[], None).await;
    assert!(!present);
    stop(&app).await;
    drop(app);
    std::fs::remove_file(path).unwrap();
}

#[cfg(feature = "mqtt5")]
async fn sender_matrix_connect(addr: SocketAddr, v5: bool, resume: bool) -> Client {
    let (mut sink, present) = if v5 {
        super::mqtt5::connect(addr, "sender-matrix", !resume, 60, &[], None).await
    } else {
        Client::connect(addr, "sender-matrix", false).await
    };
    assert_eq!(present, resume);
    if !resume {
        if v5 {
            super::mqtt5::sub(&mut sink, &[("matrix/q2", 2)], Some(17)).await;
        } else {
            assert_eq!(sink.subscribe(&[("matrix/q2", 2)]).await, [2]);
        }
    }
    sink
}
#[cfg(feature = "mqtt5")]
async fn sender_matrix_delivery(sink: &mut Client, v5: bool, dup: bool, payload: &[u8]) -> u16 {
    sender_matrix_delivery_qos(sink, v5, 2, dup, payload).await
}
#[cfg(feature = "mqtt5")]
async fn sender_matrix_delivery_qos(
    sink: &mut Client,
    v5: bool,
    qos: u8,
    dup: bool,
    payload: &[u8],
) -> u16 {
    let (head, body) = sink.recv().await;
    assert_eq!(head, 0x30 | (qos << 1) | if dup { 8 } else { 0 });
    assert_eq!(
        &body[..11],
        &[0, 9, b'm', b'a', b't', b'r', b'i', b'x', b'/', b'q', b'2']
    );
    let id = u16::from_be_bytes([body[11], body[12]]);
    assert_ne!(id, 0);
    let at = if v5 {
        assert_eq!(&body[13..16], &[2, 0x0b, 17]);
        16
    } else {
        13
    };
    assert_eq!(&body[at..], payload);
    id
}
#[cfg(feature = "mqtt5")]
async fn sender_matrix_finish(sink: &mut Client, release: bool, id: u16) {
    if !release {
        sink.send(codec::frame(0x50, &id.to_be_bytes())).await;
        assert_eq!(sink.recv().await, (0x62, id.to_be_bytes().to_vec()));
    }
    sink.send(codec::frame(0x70, &id.to_be_bytes())).await;
    super::qos2::ping(sink).await;
}

#[cfg(feature = "mqtt5")]
async fn duplex_publish_start(client: &mut Client, v5: bool, id: u16, payload: &[u8], dup: bool) {
    let mut body = string("matrix/in");
    body.extend(id.to_be_bytes());
    if v5 {
        body.push(0);
    }
    body.extend(payload);
    client
        .send(codec::frame(if dup { 0x3c } else { 0x34 }, &body))
        .await;
    assert_eq!(client.recv().await, (0x50, id.to_be_bytes().to_vec()));
}

#[cfg(feature = "mqtt5")]
#[tokio::test]
async fn qos2_same_identifier_bidirectional_matrix_keeps_sqlite_ownership_independent() {
    // Both directions own exactly the same Packet ID. Complete each direction
    // first in turn, then restart again to check persisted independence.
    for v5 in [false, true] {
        for release in [false, true] {
            for incoming_first in [false, true] {
                eprintln!(
                    "duplex ID matrix: mqtt5={v5} release={release} incoming_first={incoming_first}"
                );
                let path = database_path();
                let (app, broker) = launch_qos2(&path, Faults::default()).await;
                let addr = broker.local_addr().unwrap();
                let mut owner = sender_matrix_connect(addr, v5, false).await;
                let (mut source, _) = Client::connect(addr, "duplex-source", true).await;
                let (mut observer, _) = Client::connect(addr, "duplex-observer", true).await;
                assert_eq!(observer.subscribe(&[("matrix/in", 0)]).await, [0]);
                source.publish("matrix/q2", b"outgoing", 2, false).await;
                let id = sender_matrix_delivery(&mut owner, v5, false, b"outgoing").await;
                if release {
                    super::qos2::received(&mut owner, id).await;
                }
                duplex_publish_start(&mut owner, v5, id, b"incoming", false).await;
                let mut expected = string("matrix/in");
                expected.extend(b"incoming");
                assert_eq!(observer.recv().await, (0x30, expected));
                assert_eq!(broker.status().incoming_qos2, 1);
                assert_eq!(broker.status().pending_messages, 1);
                stop(&app).await;
                drop((app, owner, observer, source));

                for after_completion in [false, true] {
                    let (app, broker) = launch_qos2(&path, Faults::default()).await;
                    let addr = broker.local_addr().unwrap();
                    let mut owner = sender_matrix_connect(addr, v5, true).await;
                    if !after_completion || incoming_first {
                        if release {
                            assert_eq!(owner.recv().await, (0x62, id.to_be_bytes().to_vec()));
                        } else {
                            assert_eq!(
                                sender_matrix_delivery(&mut owner, v5, true, b"outgoing").await,
                                id
                            );
                        }
                    } else {
                        super::qos2::ping(&mut owner).await;
                    }
                    let (mut observer, _) = Client::connect(addr, "duplex-observer", true).await;
                    assert_eq!(observer.subscribe(&[("matrix/in", 0)]).await, [0]);
                    if !after_completion || !incoming_first {
                        duplex_publish_start(&mut owner, v5, id, b"changed-duplicate", true).await;
                        super::qos2::ping(&mut observer).await;
                    }
                    assert_eq!(
                        broker.status().incoming_qos2,
                        usize::from(!after_completion || !incoming_first)
                    );
                    assert_eq!(
                        broker.status().pending_messages,
                        usize::from(!after_completion || incoming_first)
                    );
                    if incoming_first != after_completion {
                        super::qos2::release(&mut owner, id).await;
                    } else {
                        sender_matrix_finish(&mut owner, release, id).await;
                    }
                    assert_eq!(
                        broker.status().incoming_qos2,
                        usize::from(!after_completion && !incoming_first)
                    );
                    assert_eq!(
                        broker.status().pending_messages,
                        usize::from(!after_completion && incoming_first)
                    );
                    if after_completion {
                        // Both namespaces are now free. The old inbound ID
                        // admits a new message, and outbound allocation proceeds.
                        duplex_publish_start(&mut owner, v5, id, b"new-incoming", false).await;
                        let mut expected = string("matrix/in");
                        expected.extend(b"new-incoming");
                        assert_eq!(observer.recv().await, (0x30, expected));
                        super::qos2::release(&mut owner, id).await;
                        let (mut source, _) = Client::connect(addr, "duplex-source", true).await;
                        source.publish("matrix/q2", b"next", 2, false).await;
                        let next = sender_matrix_delivery(&mut owner, v5, false, b"next").await;
                        sender_matrix_finish(&mut owner, false, next).await;
                        super::qos2::ping(&mut observer).await;
                        assert_eq!(broker.status().incoming_qos2, 0);
                        assert_eq!(broker.status().pending_messages, 0);
                    }
                    stop(&app).await;
                    drop((app, owner, observer));
                }
                std::fs::remove_file(path).unwrap();
            }
        }
    }
}

#[cfg(feature = "mqtt5")]
#[tokio::test]
async fn mqtt5_zero_expiry_retained_qos_matrix_preserves_handshake_across_sqlite_restarts() {
    use super::mqtt5::{connect, publish};
    for qos in [0, 1, 2] {
        for retain in [false, true] {
            for payload in [b"".as_slice(), b"expired".as_slice()] {
                eprintln!("zero expiry matrix: qos={qos} retain={retain} payload={payload:?}");
                let path = database_path();
                let (app, broker) = launch_qos2(&path, Faults::default()).await;
                let addr = broker.local_addr().unwrap();
                let (mut source, present) =
                    connect(addr, "expiry-source", true, 60, &[], None).await;
                assert!(!present);
                publish(&mut source, "expiry/value", b"old-retained", 1, &[], true).await;
                let (mut observer, _) = Client::connect(addr, "expiry-observer", true).await;
                assert_eq!(observer.subscribe(&[("expiry/value", 0)]).await, [0]);
                let mut old_retained = string("expiry/value");
                old_retained.extend(b"old-retained");
                assert_eq!(observer.recv().await, (0x31, old_retained.clone()));

                // Expiry=0 avoids clock races. Stop QoS2 after PUBREC so the
                // expired Application Message still has a live receiver record.
                let mut body = string("expiry/value");
                if qos != 0 {
                    body.extend([0, 23]);
                }
                body.extend([5, 2, 0, 0, 0, 0]);
                body.extend(payload);
                source
                    .send(codec::frame(0x30 | (qos << 1) | u8::from(retain), &body))
                    .await;
                if qos != 0 {
                    assert_eq!(
                        source.recv().await,
                        (if qos == 1 { 0x40 } else { 0x50 }, vec![0, 23])
                    );
                }
                super::qos2::ping(&mut source).await;
                super::qos2::ping(&mut observer).await;
                assert_eq!(broker.status().retained, usize::from(!retain));
                assert_eq!(broker.status().incoming_qos2, usize::from(qos == 2));
                stop(&app).await;
                drop((app, source, observer));

                for completed in [false, true] {
                    let (app, broker) = launch_qos2(&path, Faults::default()).await;
                    let addr = broker.local_addr().unwrap();
                    let (mut source, present) =
                        connect(addr, "expiry-source", false, 60, &[], None).await;
                    assert!(present);
                    // PING also rejects any unexpected retransmitted message.
                    super::qos2::ping(&mut source).await;
                    let (mut observer, _) = Client::connect(addr, "expiry-observer", true).await;
                    assert_eq!(observer.subscribe(&[("expiry/value", 0)]).await, [0]);
                    if !retain {
                        assert_eq!(observer.recv().await, (0x31, old_retained.clone()));
                    }
                    super::qos2::ping(&mut observer).await;
                    assert_eq!(
                        broker.status().incoming_qos2,
                        usize::from(qos == 2 && !completed)
                    );
                    if qos == 2 {
                        if !completed {
                            // Changing a duplicate to an unexpired retained
                            // message must not resurrect or replace its content.
                            let mut duplicate = string("expiry/value");
                            duplicate.extend([0, 23, 0]);
                            duplicate.extend(b"must-not-resurrect");
                            source.send(codec::frame(0x3d, &duplicate)).await;
                            assert_eq!(source.recv().await, (0x50, vec![0, 23]));
                            super::qos2::ping(&mut observer).await;
                        }
                        // Repeated PUBREL after the second restart still
                        // completes, without replaying expired application data.
                        super::qos2::release(&mut source, 23).await;
                    }
                    super::qos2::ping(&mut observer).await;
                    assert_eq!(broker.status().retained, usize::from(!retain));
                    assert_eq!(broker.status().incoming_qos2, 0);
                    assert_eq!(broker.status().pending_messages, 0);
                    if completed {
                        // Once PUBCOMP has completed, the same ID is a new
                        // exchange; a normal retained publication works again.
                        let mut fresh = string("expiry/value");
                        fresh.extend([0, 23, 0]);
                        fresh.extend(b"fresh");
                        source.send(codec::frame(0x33, &fresh)).await;
                        assert_eq!(source.recv().await, (0x40, vec![0, 23]));
                        let mut expected = string("expiry/value");
                        expected.extend(b"fresh");
                        assert_eq!(observer.recv().await, (0x30, expected));
                        assert_eq!(broker.status().retained, 1);
                    }
                    stop(&app).await;
                    drop((app, source, observer));
                }
                std::fs::remove_file(path).unwrap();
            }
        }
    }
}

#[cfg(feature = "mqtt5")]
#[tokio::test]
async fn qos2_sender_wrong_ack_phase_matrix_preserves_ownership_across_sqlite_restart() {
    for v5 in [false, true] {
        for release in [false, true] {
            for event in ["wrong-kind", "wrong-id", "premature-or-duplicate"] {
                eprintln!("sender restart matrix: mqtt5={v5} release={release} event={event}");
                let path = database_path();
                let (app, broker) = launch_qos2(&path, Faults::default()).await;
                let addr = broker.local_addr().unwrap();
                let mut sink = sender_matrix_connect(addr, v5, false).await;
                let (mut source, _) = Client::connect(addr, "sender-source", true).await;
                source.publish("matrix/q2", b"original", 2, false).await;
                let id = sender_matrix_delivery(&mut sink, v5, false, b"original").await;
                if release {
                    sink.send(codec::frame(0x50, &id.to_be_bytes())).await;
                    assert_eq!(sink.recv().await, (0x62, id.to_be_bytes().to_vec()));
                }
                stop(&app).await;
                drop((app, sink, source));
                // Two real restarts: the second must preserve the state after
                // the wrong ACK, not just the initial pre-fault image.
                for after_fault in [false, true] {
                    let (app, broker) = launch_qos2(&path, Faults::default()).await;
                    let addr = broker.local_addr().unwrap();
                    let mut sink = sender_matrix_connect(addr, v5, true).await;
                    if release {
                        assert_eq!(sink.recv().await, (0x62, id.to_be_bytes().to_vec()));
                    } else {
                        assert_eq!(
                            sender_matrix_delivery(&mut sink, v5, true, b"original").await,
                            id
                        );
                    }
                    assert_eq!(broker.status().pending_messages, 1);
                    if !after_fault {
                        let (head, sent_id) = match event {
                            "wrong-kind" => (0x40, id),
                            "wrong-id" => (if release { 0x70 } else { 0x50 }, id ^ 0x8000),
                            _ => (if release { 0x50 } else { 0x70 }, id),
                        };
                        sink.send(codec::frame(head, &sent_id.to_be_bytes())).await;
                        if release && event == "premature-or-duplicate" {
                            assert_eq!(sink.recv().await, (0x62, id.to_be_bytes().to_vec()));
                        }
                        super::qos2::ping(&mut sink).await;
                        assert_eq!(broker.status().pending_messages, 1);
                    } else {
                        sender_matrix_finish(&mut sink, release, id).await;
                        assert_eq!(broker.status().pending_messages, 0);
                        let (mut source, _) = Client::connect(addr, "sender-source", true).await;
                        source.publish("matrix/q2", b"next", 2, false).await;
                        let next = sender_matrix_delivery(&mut sink, v5, false, b"next").await;
                        assert_ne!(next, id);
                        sender_matrix_finish(&mut sink, false, next).await;
                        assert_eq!(broker.status().pending_messages, 0);
                        assert_eq!(broker.status().incoming_qos2, 0);
                    }
                    assert_eq!(broker.status().generation, 1);
                    stop(&app).await;
                    drop((app, sink));
                }
                std::fs::remove_file(path).unwrap();
            }
        }
    }
}

#[cfg(feature = "mqtt5")]
#[tokio::test]
async fn mqtt5_negative_pubrec_reason_matrix_after_sqlite_restart_only_releases_publish_phase() {
    // Seven legal Client failure reasons x Publish/Release phase. A late
    // negative PUBREC must not discard a transfer already advanced to PUBREL.
    for release in [false, true] {
        for reason in [0x80, 0x83, 0x87, 0x90, 0x91, 0x97, 0x99] {
            eprintln!("negative PUBREC restart matrix: release={release} reason={reason:02x}");
            let path = database_path();
            let (app, broker) = launch_qos2(&path, Faults::default()).await;
            let addr = broker.local_addr().unwrap();
            let mut sink = sender_matrix_connect(addr, true, false).await;
            let (mut source, _) = Client::connect(addr, "sender-source", true).await;
            source.publish("matrix/q2", b"original", 2, false).await;
            let id = sender_matrix_delivery(&mut sink, true, false, b"original").await;
            if release {
                sink.send(codec::frame(0x50, &id.to_be_bytes())).await;
                assert_eq!(sink.recv().await, (0x62, id.to_be_bytes().to_vec()));
            }
            stop(&app).await;
            drop((app, sink, source));
            let (app, broker) = launch_qos2(&path, Faults::default()).await;
            let mut sink = sender_matrix_connect(broker.local_addr().unwrap(), true, true).await;
            if release {
                assert_eq!(sink.recv().await, (0x62, id.to_be_bytes().to_vec()));
            } else {
                assert_eq!(
                    sender_matrix_delivery(&mut sink, true, true, b"original").await,
                    id
                );
            }
            let mut body = id.to_be_bytes().to_vec();
            body.push(reason);
            sink.send(codec::frame(0x50, &body)).await;
            super::qos2::ping(&mut sink).await;
            assert_eq!(broker.status().pending_messages, usize::from(release));
            stop(&app).await;
            drop((app, sink));
            let (app, broker) = launch_qos2(&path, Faults::default()).await;
            let addr = broker.local_addr().unwrap();
            let mut sink = sender_matrix_connect(addr, true, true).await;
            if release {
                assert_eq!(sink.recv().await, (0x62, id.to_be_bytes().to_vec()));
                sender_matrix_finish(&mut sink, true, id).await;
            } else {
                super::qos2::ping(&mut sink).await;
            }
            assert_eq!(broker.status().pending_messages, 0);
            let (mut source, _) = Client::connect(addr, "sender-source", true).await;
            source.publish("matrix/q2", b"next", 2, false).await;
            let next = sender_matrix_delivery(&mut sink, true, false, b"next").await;
            assert_ne!(next, id);
            sender_matrix_finish(&mut sink, false, next).await;
            assert_eq!(broker.status().pending_messages, 0);
            stop(&app).await;
            drop((app, sink, source));
            std::fs::remove_file(path).unwrap();
        }
    }
}

#[cfg(feature = "mqtt5")]
#[tokio::test]
async fn client_ack_server_only_reason_matrix_closes_without_mutating_sqlite_sender_ownership() {
    // MQTT5 Server-only0x10 is not a valid Client ACK. In3.1.1 the
    // extra Reason Code/Property Length bytes are themselves malformed.
    for v5 in [false, true] {
        for phase in ["qos1", "qos2-publish", "qos2-release"] {
            for explicit in [false, true] {
                eprintln!("ACK role matrix: mqtt5={v5} phase={phase} explicit={explicit}");
                let qos = if phase == "qos1" { 1 } else { 2 };
                let release = phase == "qos2-release";
                let path = database_path();
                let (app, broker) = launch_qos2(&path, Faults::default()).await;
                let addr = broker.local_addr().unwrap();
                let mut sink = sender_matrix_connect(addr, v5, false).await;
                let (mut source, _) = Client::connect(addr, "role-source", true).await;
                source.publish("matrix/q2", b"original", qos, false).await;
                let id = sender_matrix_delivery_qos(&mut sink, v5, qos, false, b"original").await;
                if release {
                    sink.send(codec::frame(0x50, &id.to_be_bytes())).await;
                    assert_eq!(sink.recv().await, (0x62, id.to_be_bytes().to_vec()));
                }
                let mut body = id.to_be_bytes().to_vec();
                body.push(0x10);
                if explicit {
                    body.push(0);
                }
                sink.send(codec::frame(if qos == 1 { 0x40 } else { 0x50 }, &body))
                    .await;
                assert_eq!(
                    tokio::time::timeout(Duration::from_secs(2), sink.0.read(&mut [0]))
                        .await
                        .unwrap()
                        .unwrap(),
                    0,
                    "illegal ACK must close before another success response"
                );
                // An unrelated connection can still exchange control and data;
                // rejection neither completes nor advances the owned message.
                super::qos2::ping(&mut source).await;
                source.publish("matrix/health", b"healthy", 1, false).await;
                assert_eq!(broker.status().pending_messages, 1);
                stop(&app).await;
                drop((app, sink, source));
                let (app, broker) = launch_qos2(&path, Faults::default()).await;
                let addr = broker.local_addr().unwrap();
                let mut sink = sender_matrix_connect(addr, v5, true).await;
                if release {
                    assert_eq!(sink.recv().await, (0x62, id.to_be_bytes().to_vec()));
                } else {
                    assert_eq!(
                        sender_matrix_delivery_qos(&mut sink, v5, qos, true, b"original").await,
                        id
                    );
                }
                if qos == 1 {
                    sink.send(codec::frame(0x40, &id.to_be_bytes())).await;
                    super::qos2::ping(&mut sink).await;
                } else {
                    sender_matrix_finish(&mut sink, release, id).await;
                }
                assert_eq!(broker.status().pending_messages, 0);
                let (mut source, _) = Client::connect(addr, "role-source", true).await;
                source.publish("matrix/q2", b"next", 2, false).await;
                let next = sender_matrix_delivery(&mut sink, v5, false, b"next").await;
                assert_ne!(next, id);
                sender_matrix_finish(&mut sink, false, next).await;
                assert_eq!(broker.status().pending_messages, 0);
                assert_eq!(broker.status().incoming_qos2, 0);
                stop(&app).await;
                drop((app, sink, source));
                std::fs::remove_file(path).unwrap();
            }
        }
    }
}

#[cfg(feature = "mqtt5")]
#[tokio::test]
async fn mqtt5_client_ack_legal_reason_matrix_completes_once_across_sqlite_restarts() {
    // Both ACK kinds x eight legal Client reasons x shortened/explicit
    // Properties. A negative PUBREC ends Publish ownership without PUBREL.
    for qos in [1, 2] {
        for reason in [0, 0x80, 0x83, 0x87, 0x90, 0x91, 0x97, 0x99] {
            for explicit in [false, true] {
                eprintln!("legal ACK matrix: qos={qos} reason={reason:02x} explicit={explicit}");
                let path = database_path();
                let (app, broker) = launch_qos2(&path, Faults::default()).await;
                let addr = broker.local_addr().unwrap();
                let mut sink = sender_matrix_connect(addr, true, false).await;
                let (mut source, _) = Client::connect(addr, "legal-source", true).await;
                source.publish("matrix/q2", b"original", qos, false).await;
                let id = sender_matrix_delivery_qos(&mut sink, true, qos, false, b"original").await;
                stop(&app).await;
                drop((app, sink, source));
                let (app, broker) = launch_qos2(&path, Faults::default()).await;
                let mut sink =
                    sender_matrix_connect(broker.local_addr().unwrap(), true, true).await;
                assert_eq!(
                    sender_matrix_delivery_qos(&mut sink, true, qos, true, b"original").await,
                    id
                );
                let mut body = id.to_be_bytes().to_vec();
                body.push(reason);
                if explicit {
                    body.push(0);
                }
                sink.send(codec::frame(if qos == 1 { 0x40 } else { 0x50 }, &body))
                    .await;
                if qos == 2 && reason == 0 {
                    assert_eq!(sink.recv().await, (0x62, id.to_be_bytes().to_vec()));
                    sink.send(codec::frame(0x70, &id.to_be_bytes())).await;
                }
                // A surprising PUBREL, extra PUBLISH or connection closure
                // cannot pass this FIFO barrier.
                super::qos2::ping(&mut sink).await;
                assert_eq!(broker.status().pending_messages, 0);
                stop(&app).await;
                drop((app, sink));
                let (app, broker) = launch_qos2(&path, Faults::default()).await;
                let addr = broker.local_addr().unwrap();
                let mut sink = sender_matrix_connect(addr, true, true).await;
                super::qos2::ping(&mut sink).await;
                let (mut source, _) = Client::connect(addr, "legal-source", true).await;
                source.publish("matrix/q2", b"next", 2, false).await;
                let next = sender_matrix_delivery(&mut sink, true, false, b"next").await;
                assert_ne!(next, id);
                sender_matrix_finish(&mut sink, false, next).await;
                assert_eq!(broker.status().pending_messages, 0);
                stop(&app).await;
                drop((app, sink, source));
                std::fs::remove_file(path).unwrap();
            }
        }
    }
}

#[cfg(feature = "mqtt5")]
#[tokio::test]
async fn qos2_duplex_same_id_commit_fault_matrix_recovers_each_direction_independently() {
    for v5 in [false, true] {
        for release in [false, true] {
            for incoming in [false, true] {
                // Before commit, lost receipt resolved by Query, and committed
                // but unconfirmable until recovery are different durable states.
                for mode in [3, 2, 7] {
                    eprintln!(
                        "duplex commit matrix: mqtt5={v5} release={release} incoming={incoming} mode={mode}"
                    );
                    let path = database_path();
                    let faults = Faults::default();
                    let (app, broker) = launch_qos2(&path, faults.clone()).await;
                    let addr = broker.local_addr().unwrap();
                    let mut owner = sender_matrix_connect(addr, v5, false).await;
                    let (mut source, _) = Client::connect(addr, "fault-source", true).await;
                    source.publish("matrix/q2", b"original", 2, false).await;
                    let id = sender_matrix_delivery(&mut owner, v5, false, b"original").await;
                    if release {
                        super::qos2::received(&mut owner, id).await;
                    }
                    duplex_publish_start(&mut owner, v5, id, b"inbound", false).await;
                    assert_eq!(broker.status().incoming_qos2, 1);
                    assert_eq!(broker.status().pending_messages, 1);
                    faults.mode.store(mode, Ordering::Release);
                    let head = if incoming {
                        0x62
                    } else if release {
                        0x70
                    } else {
                        0x50
                    };
                    owner.send(codec::frame(head, &id.to_be_bytes())).await;
                    if mode == 2 {
                        if incoming || !release {
                            assert_eq!(
                                owner.recv().await,
                                (
                                    if incoming { 0x70 } else { 0x62 },
                                    id.to_be_bytes().to_vec()
                                )
                            );
                        }
                        super::qos2::ping(&mut owner).await;
                        assert_eq!(broker.status().storage_commit_failures, 0);
                        assert_eq!(faults.mode.load(Ordering::Acquire), 0);
                    } else {
                        assert!(matches!(
                            tokio::time::timeout(Duration::from_secs(2), owner.0.read_u8()).await,
                            Ok(Err(_))
                        ));
                        assert!(broker.status().storage_commit_failures > 0);
                    }
                    assert!(faults.queries.load(Ordering::Relaxed) > 0);
                    stop(&app).await;
                    drop((app, owner, source));

                    let (app, broker) = launch_qos2(&path, Faults::default()).await;
                    let addr = broker.local_addr().unwrap();
                    let committed = mode != 3;
                    let inbound_owned = !(committed && incoming);
                    let outbound_owned = !(committed && !incoming && release);
                    let resumed_release = release || (committed && !incoming);
                    assert_eq!(broker.status().incoming_qos2, usize::from(inbound_owned));
                    assert_eq!(
                        broker.status().pending_messages,
                        usize::from(outbound_owned)
                    );
                    let mut owner = sender_matrix_connect(addr, v5, true).await;
                    if outbound_owned {
                        if resumed_release {
                            assert_eq!(owner.recv().await, (0x62, id.to_be_bytes().to_vec()));
                        } else {
                            assert_eq!(
                                sender_matrix_delivery(&mut owner, v5, true, b"original").await,
                                id
                            );
                        }
                    } else {
                        super::qos2::ping(&mut owner).await;
                    }
                    let (mut observer, _) = Client::connect(addr, "fault-observer", true).await;
                    assert_eq!(observer.subscribe(&[("matrix/in", 0)]).await, [0]);
                    if inbound_owned {
                        duplex_publish_start(&mut owner, v5, id, b"changed", true).await;
                        super::qos2::ping(&mut observer).await;
                        super::qos2::release(&mut owner, id).await;
                    }
                    assert_eq!(
                        broker.status().pending_messages,
                        usize::from(outbound_owned)
                    );
                    if outbound_owned {
                        sender_matrix_finish(&mut owner, resumed_release, id).await;
                    }
                    duplex_publish_start(&mut owner, v5, id, b"reused", false).await;
                    let mut expected = string("matrix/in");
                    expected.extend(b"reused");
                    assert_eq!(observer.recv().await, (0x30, expected));
                    super::qos2::release(&mut owner, id).await;
                    super::qos2::ping(&mut observer).await;
                    assert_eq!(broker.status().incoming_qos2, 0);
                    assert_eq!(broker.status().pending_messages, 0);
                    stop(&app).await;
                    drop((app, owner, observer));
                    let (app, broker) = launch_qos2(&path, Faults::default()).await;
                    let mut owner =
                        sender_matrix_connect(broker.local_addr().unwrap(), v5, true).await;
                    super::qos2::ping(&mut owner).await;
                    assert_eq!(broker.status().incoming_qos2, 0);
                    assert_eq!(broker.status().pending_messages, 0);
                    stop(&app).await;
                    drop((app, owner));
                    std::fs::remove_file(path).unwrap();
                }
            }
        }
    }
}

#[cfg(feature = "mqtt5")]
fn retained_expiry(broker: &BrokerHandle) -> u64 {
    let snapshot = serde_json::to_value(broker.state.lock().unwrap().snapshot()).unwrap();
    snapshot["retained"][0]["properties"]["expires_at_ms"]
        .as_u64()
        .unwrap()
}

#[cfg(feature = "mqtt5")]
async fn wait_unix(deadline: u64) {
    while unix_time_ms() < deadline {
        tokio::time::sleep(Duration::from_millis(
            deadline - unix_time_ms().min(deadline),
        ))
        .await;
    }
}

#[cfg(feature = "mqtt5")]
async fn ttl_delivery(client: &mut Client, dup: bool, expiry_zero: bool) -> u16 {
    let (head, body) = client.recv().await;
    assert_eq!(head, if dup { 0x3c } else { 0x34 });
    assert_eq!(&body[..11], string("matrix/q2"));
    let id = u16::from_be_bytes([body[11], body[12]]);
    assert_ne!(id, 0);
    assert_eq!(&body[13..15], &[7, 2]);
    let remaining = u32::from_be_bytes(body[15..19].try_into().unwrap());
    if expiry_zero {
        assert_eq!(remaining, 0);
    } else {
        assert!((1..=4).contains(&remaining));
    }
    assert_eq!(&body[19..22], &[0x0b, 17, b't']);
    assert_eq!(&body[21..], b"ttl-payload");
    id
}

#[cfg(feature = "mqtt5")]
#[tokio::test]
async fn mqtt5_positive_ttl_phase_restart_matrix_expires_only_unsent_messages_and_retained() {
    use super::mqtt5::{connect, publish, sub};
    for phase in ["queued", "publish", "release"] {
        for expire_before_restart in [false, true] {
            eprintln!(
                "positive TTL matrix: phase={phase} expire_before_restart={expire_before_restart}"
            );
            let path = database_path();
            let (app, broker) = launch_qos2(&path, Faults::default()).await;
            let addr = broker.local_addr().unwrap();
            let mut sink = sender_matrix_connect(addr, true, false).await;
            if phase == "queued" {
                sink.send(vec![0xe0, 0]).await;
                assert_eq!(sink.0.read(&mut [0]).await.unwrap(), 0);
            }
            let (mut source, _) = connect(addr, "ttl-source", true, 60, &[], None).await;
            publish(
                &mut source,
                "matrix/q2",
                b"ttl-payload",
                2,
                &[2, 0, 0, 0, 4],
                true,
            )
            .await;
            let deadline = retained_expiry(&broker);
            assert!(deadline > unix_time_ms());
            let id = if phase == "queued" {
                0
            } else {
                ttl_delivery(&mut sink, false, false).await
            };
            if phase == "release" {
                super::qos2::received(&mut sink, id).await;
            }
            // A real wall-clock checkpoint checks decremented positive TTL on
            // retained replay before crossing the absolute persisted deadline.
            wait_unix(deadline.saturating_sub(1500)).await;
            let (mut early, _) = connect(addr, "ttl-early", true, 0, &[], None).await;
            sub(&mut early, &[("matrix/q2", 0)], None).await;
            let (head, body) = early.recv().await;
            assert_eq!(head, 0x31);
            assert_eq!(
                &body[..13],
                &[
                    0, 9, b'm', b'a', b't', b'r', b'i', b'x', b'/', b'q', b'2', 5, 2
                ]
            );
            let remaining = u32::from_be_bytes(body[13..17].try_into().unwrap());
            assert!((1..=2).contains(&remaining));
            assert_eq!(&body[17..], b"ttl-payload");
            if expire_before_restart {
                wait_unix(deadline + 1).await;
            }
            stop(&app).await;
            drop((app, source, sink, early));
            let (app, broker) = launch_qos2(&path, Faults::default()).await;
            let addr = broker.local_addr().unwrap();
            if !expire_before_restart {
                assert!(
                    unix_time_ms() < deadline,
                    "restart must finish before expiry in this branch"
                );
                assert_eq!(retained_expiry(&broker), deadline);
                wait_unix(deadline + 1).await;
            }
            let mut sink = sender_matrix_connect(addr, true, true).await;
            if phase == "queued" {
                super::qos2::ping(&mut sink).await;
            } else if phase == "publish" {
                assert_eq!(ttl_delivery(&mut sink, true, true).await, id);
                sender_matrix_finish(&mut sink, false, id).await;
            } else {
                assert_eq!(sink.recv().await, (0x62, id.to_be_bytes().to_vec()));
                sender_matrix_finish(&mut sink, true, id).await;
            }
            let (mut late, _) = Client::connect(addr, "ttl-late", true).await;
            assert_eq!(late.subscribe(&[("matrix/q2", 0)]).await, [0]);
            super::qos2::ping(&mut late).await;
            assert_eq!(broker.status().pending_messages, 0);
            let (mut source, _) = Client::connect(addr, "ttl-next", true).await;
            source.publish("matrix/q2", b"fresh", 2, false).await;
            let next = sender_matrix_delivery(&mut sink, true, false, b"fresh").await;
            sender_matrix_finish(&mut sink, false, next).await;
            let mut expected = string("matrix/q2");
            expected.extend(b"fresh");
            assert_eq!(late.recv().await, (0x30, expected));
            stop(&app).await;
            drop((app, sink, late, source));
            let (app, broker) = launch_qos2(&path, Faults::default()).await;
            let mut sink = sender_matrix_connect(broker.local_addr().unwrap(), true, true).await;
            super::qos2::ping(&mut sink).await;
            assert_eq!(broker.status().pending_messages, 0);
            assert_eq!(broker.status().retained, 0);
            stop(&app).await;
            drop((app, sink));
            std::fs::remove_file(path).unwrap();
        }
    }
}

#[cfg(all(feature = "mqtt5", unix))]
struct CrashChild {
    process: std::process::Child,
    port_file: std::path::PathBuf,
}
#[cfg(all(feature = "mqtt5", unix))]
impl CrashChild {
    async fn start(database: &std::path::Path) -> (Self, SocketAddr) {
        let port_file = database.with_extension("port");
        let _ = std::fs::remove_file(&port_file);
        let process = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::storage::qos2_duplex_same_id_sigkill_matrix_restores_both_directions_and_completion",
                "--nocapture",
            ])
            .env("MQTT_DUPLEX_CHILD_DATABASE", database)
            .env("MQTT_DUPLEX_CHILD_PORT", &port_file)
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let mut child = Self { process, port_file };
        let addr = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Ok(text) = std::fs::read_to_string(&child.port_file) {
                    if let Ok(addr) = text.parse() {
                        break addr;
                    }
                }
                assert!(
                    child.process.try_wait().unwrap().is_none(),
                    "crash fixture exited before listening"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        (child, addr)
    }
    fn crash(&mut self) {
        use std::os::unix::process::ExitStatusExt;
        self.process.kill().unwrap();
        assert_eq!(self.process.wait().unwrap().signal(), Some(9));
    }
}
#[cfg(all(feature = "mqtt5", unix))]
impl Drop for CrashChild {
    fn drop(&mut self) {
        if self.process.try_wait().ok().flatten().is_none() {
            let _ = self.process.kill();
            let _ = self.process.wait();
        }
        let _ = std::fs::remove_file(&self.port_file);
    }
}

#[cfg(all(feature = "mqtt5", unix))]
#[tokio::test]
async fn qos2_duplex_same_id_sigkill_matrix_restores_both_directions_and_completion() {
    // The same exact test is the explicit child entry point. Its child branch
    // never reports a test pass: the parent kills it and checks signal9. Keeping
    // this inside the parent avoids ignored/no-op fixtures in strict evidence.
    if let Some(database) = std::env::var_os("MQTT_DUPLEX_CHILD_DATABASE") {
        let port_file = std::env::var_os("MQTT_DUPLEX_CHILD_PORT").unwrap();
        let (_app, broker) = launch_qos2(std::path::Path::new(&database), Faults::default()).await;
        std::fs::write(port_file, broker.local_addr().unwrap().to_string()).unwrap();
        std::future::pending::<()>().await;
    }
    for v5 in [false, true] {
        for release in [false, true] {
            eprintln!("duplex SIGKILL matrix: mqtt5={v5} release={release}");
            let path = database_path();
            let (mut child, addr) = CrashChild::start(&path).await;
            let mut owner = sender_matrix_connect(addr, v5, false).await;
            let (mut source, _) = Client::connect(addr, "crash-source", true).await;
            source
                .publish("matrix/q2", b"crash-outbound", 2, false)
                .await;
            let id = sender_matrix_delivery(&mut owner, v5, false, b"crash-outbound").await;
            if release {
                super::qos2::received(&mut owner, id).await;
            }
            duplex_publish_start(&mut owner, v5, id, b"crash-inbound", false).await;
            child.crash();
            drop((child, owner, source));
            let (mut child, addr) = CrashChild::start(&path).await;
            let mut owner = sender_matrix_connect(addr, v5, true).await;
            if release {
                assert_eq!(owner.recv().await, (0x62, id.to_be_bytes().to_vec()));
            } else {
                assert_eq!(
                    sender_matrix_delivery(&mut owner, v5, true, b"crash-outbound").await,
                    id
                );
            }
            let (mut observer, _) = Client::connect(addr, "crash-observer", true).await;
            assert_eq!(observer.subscribe(&[("matrix/in", 0)]).await, [0]);
            duplex_publish_start(&mut owner, v5, id, b"changed", true).await;
            super::qos2::ping(&mut observer).await;
            super::qos2::release(&mut owner, id).await;
            sender_matrix_finish(&mut owner, release, id).await;
            duplex_publish_start(&mut owner, v5, id, b"reused", false).await;
            let mut expected = string("matrix/in");
            expected.extend(b"reused");
            assert_eq!(observer.recv().await, (0x30, expected));
            super::qos2::release(&mut owner, id).await;
            // The second real SIGKILL also verifies durable completion; normal
            // Application shutdown must not be the event that saves these ACKs.
            child.crash();
            drop((child, owner, observer));
            let (app, broker) = launch_qos2(&path, Faults::default()).await;
            let addr = broker.local_addr().unwrap();
            let mut owner = sender_matrix_connect(addr, v5, true).await;
            super::qos2::ping(&mut owner).await;
            assert_eq!(broker.status().incoming_qos2, 0);
            assert_eq!(broker.status().pending_messages, 0);
            let (mut source, _) = Client::connect(addr, "crash-next", true).await;
            source.publish("matrix/q2", b"next", 2, false).await;
            let next = sender_matrix_delivery(&mut owner, v5, false, b"next").await;
            sender_matrix_finish(&mut owner, false, next).await;
            stop(&app).await;
            drop((app, owner, source));
            std::fs::remove_file(path).unwrap();
        }
    }
}
