//! Fault-injected replaceable Auth contract tests, independent from password provider.
use super::*;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

#[derive(Clone)]
struct Policy {
    mode: Arc<AtomicU8>,
    pause_attach: Arc<AtomicBool>,
    entered: Arc<Semaphore>,
    gate: Arc<Semaphore>,
}
impl Default for Policy {
    fn default() -> Self {
        Self {
            mode: Arc::new(AtomicU8::new(0)),
            pause_attach: Arc::new(AtomicBool::new(false)),
            entered: Arc::new(Semaphore::new(0)),
            gate: Arc::new(Semaphore::new(0)),
        }
    }
}
impl ModuleFactory for Policy {
    fn descriptor(&self) -> ComponentDescriptor {
        ComponentDescriptor::new(ComponentRef::module("auth"))
    }
    fn create(&self, _: &ComponentConfig) -> FrameworkResult<Box<dyn Module>> {
        Ok(Box::new(self.clone()))
    }
}
#[async_trait]
impl Lifecycle for Policy {}
impl Module for Policy {
    fn descriptor(&self) -> ComponentDescriptor {
        ModuleFactory::descriptor(self)
    }
    fn handlers(&mut self) -> FrameworkResult<Option<HandlerRegistration>> {
        Ok(Some(HandlerRegistration::Serial {
            handler: Box::new(self.clone()),
            subscriptions: vec![SubscriptionSpec::new(
                1000,
                ComponentRef::module("auth"),
                AUTH_TOPIC,
            )],
        }))
    }
}
#[async_trait]
impl SerialHandler for Policy {
    async fn handle(
        &mut self,
        _: &HandlerContext,
        message: MessageEnvelope,
    ) -> FrameworkResult<HandlerReply> {
        let request = message.payload::<AuthRequest>()?;
        let mode = self.mode.load(Ordering::SeqCst);
        let result: AuthResult = match request.as_ref() {
            AuthRequest::Authenticate(a) => {
                if mode == 2 {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                Ok(AuthDecision::Principal(Principal {
                    key: PrincipalKey {
                        domain: "fixture".into(),
                        subject: a.username.clone().unwrap_or_default(),
                    },
                    policy_version: 1,
                    expires_at_ms: unix_time_ms() + if mode == 7 { 500 } else { 60000 },
                }))
            }
            AuthRequest::SessionAttach(_) => {
                if self.pause_attach.swap(false, Ordering::SeqCst) {
                    self.entered.add_permits(1);
                    self.gate.acquire().await.unwrap().forget();
                }
                // Deliberately allow cross-owner requests: Core must still prevent takeover.
                Ok(AuthDecision::Allowed)
            }
            AuthRequest::Subscribe(_) if mode == 1 => Err(AuthError::Denied),
            AuthRequest::Subscribe(_) => {
                if mode == 5 {
                    self.entered.add_permits(1);
                    self.gate.acquire().await.unwrap().forget();
                }
                Ok(AuthDecision::Allowed)
            }
            AuthRequest::Publish(context) => {
                if mode == 4 || mode == 7 {
                    self.entered.add_permits(1);
                    self.gate.acquire().await.unwrap().forget();
                }
                let mut context = context.clone();
                if mode == 3 {
                    context.topic = "forged/grant".into();
                }
                Ok(AuthDecision::Publish(PublishGrant {
                    context,
                    policy_version: 1,
                }))
            }
        };
        Ok(Arc::new(result))
    }
}
async fn setup(policy: Policy) -> (ApplicationHandle, BrokerHandle) {
    let mut config = config();
    config.anonymous_access = false;
    config.auth = Some(AuthProviderConfig {
        component: "auth".into(),
        timeout: Duration::from_millis(100),
    });
    setup_config(policy, config).await
}
async fn setup_config(
    policy: Policy,
    config: MqttBrokerConfig,
) -> (ApplicationHandle, BrokerHandle) {
    let mut builder = ApplicationBuilder::new("auth-contract");
    let factory = MqttBrokerFactory::new(config).unwrap();
    let handle = factory.handle();
    builder.register_module(policy).unwrap();
    builder.register_module(factory).unwrap();
    let mut selected = ComponentSelection::new();
    selected.enable_module("mqtt-broker").enable_module("auth");
    (
        builder
            .selection(selected)
            .build()
            .unwrap()
            .start()
            .await
            .unwrap(),
        handle,
    )
}
async fn connect(address: SocketAddr, id: &str, user: &str, clean: bool) -> (Client, u8, bool) {
    let mut client = Client(TcpStream::connect(address).await.unwrap());
    let mut body = string("MQTT");
    body.extend([4, 128 | (u8::from(clean) * 2), 0, 0]);
    body.extend(string(id));
    body.extend(string(user));
    client.send(codec::frame(0x10, &body)).await;
    let (_, response) = client.recv().await;
    (client, response[1], response[0] != 0)
}
#[tokio::test]
async fn core_enforces_owner_even_when_provider_allows_and_rechecks_recovered_subscriptions() {
    let policy = Policy::default();
    let (app, handle) = setup(policy.clone()).await;
    let address = handle.local_addr().unwrap();
    let (mut original, code, _) = connect(address, "persistent", "alice", false).await;
    assert_eq!(code, 0);
    original.subscribe(&[("data/#", 1)]).await;
    for clean in [true, false] {
        let (_, code, _) = connect(address, "persistent", "bob", clean).await;
        assert_eq!(code, 5);
        original.send(vec![0xc0, 0]).await;
        assert_eq!(original.recv().await.0, 0xd0);
    }
    policy.mode.store(1, Ordering::SeqCst);
    let (_, code, _) = connect(address, "persistent", "alice", false).await;
    assert_eq!(code, 5);
    original.send(vec![0xc0, 0]).await;
    assert_eq!(original.recv().await.0, 0xd0);
    policy.mode.store(0, Ordering::SeqCst);
    let (mut replacement, code, present) = connect(address, "persistent", "alice", false).await;
    assert_eq!(code, 0);
    assert!(present);
    replacement
        .publish("data/after", b"restored", 1, false)
        .await;
    assert_eq!(replacement.message().await.1, b"restored");
    stop(&app).await;
}
#[tokio::test]
async fn session_change_during_external_authorization_is_rejected_before_takeover() {
    let policy = Policy::default();
    let (app, handle) = setup(policy.clone()).await;
    let address = handle.local_addr().unwrap();
    let (mut original, code, _) = connect(address, "race", "alice", false).await;
    assert_eq!(code, 0);
    original.subscribe(&[("data/#", 1)]).await;
    policy.pause_attach.store(true, Ordering::SeqCst);
    let (replacement, ()) = tokio::join!(connect(address, "race", "alice", false), async {
        policy.entered.acquire().await.unwrap().forget();
        let mut body = vec![0, 3];
        body.extend(string("data/#"));
        original.send(codec::frame(0xa2, &body)).await;
        assert_eq!(original.recv().await.0, 0xb0);
        policy.gate.add_permits(1);
    });
    assert_eq!(replacement.1, 3);
    original.send(vec![0xc0, 0]).await;
    assert_eq!(original.recv().await.0, 0xd0);
    stop(&app).await;
}
#[tokio::test]
async fn authorization_timeout_and_mismatched_grant_never_commit_or_restart_core() {
    let policy = Policy::default();
    let (app, handle) = setup(policy.clone()).await;
    let address = handle.local_addr().unwrap();
    policy.mode.store(2, Ordering::SeqCst);
    let (_, code, _) = connect(address, "timeout", "alice", false).await;
    assert_eq!(code, 5);
    assert_eq!(handle.status().sessions, 0);
    tokio::time::sleep(Duration::from_millis(110)).await;
    policy.mode.store(0, Ordering::SeqCst);
    let (mut original, code, _) = connect(address, "grant", "alice", false).await;
    assert_eq!(code, 0);
    policy.mode.store(3, Ordering::SeqCst);
    let mut body = string("data/topic");
    body.extend([0, 1]);
    body.extend(b"must-not-commit");
    original.send(codec::frame(0x33, &body)).await;
    assert_eq!(original.0.read(&mut [0]).await.unwrap(), 0);
    assert_eq!(handle.status().retained, 0);
    assert_eq!(handle.status().generation, 1);
    stop(&app).await;
}

#[tokio::test]
async fn peer_ip_handshake_admission_and_backoff_are_bounded_before_authentication() {
    let mut config = config();
    config.max_connections_per_ip = 1;
    config.max_connection_attempts_per_ip = 1;
    config.connection_attempt_window = Duration::from_secs(1);
    config.max_tracked_peer_ips = 1;
    let (app, handle) = start(config).await;
    let address = handle.local_addr().unwrap();
    let first = TcpStream::connect(address).await.unwrap();
    let mut rejected = TcpStream::connect(address).await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), rejected.read(&mut [0]))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    drop(first);
    tokio::time::timeout(Duration::from_secs(1), async {
        while handle.status().connections != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut rejected = TcpStream::connect(address).await.unwrap();
    assert_eq!(rejected.read(&mut [0]).await.unwrap(), 0);
    assert_eq!(handle.status().rate_limited_connections, 2);
    assert_eq!(handle.status().peer_buckets, 1);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let (mut admitted, _) = Client::connect(address, "after-window", true).await;
    admitted.send(vec![0xc0, 0]).await;
    assert_eq!(admitted.recv().await.0, 0xd0);
    assert_eq!(handle.status().generation, 1);
    stop(&app).await;
}

#[tokio::test]
async fn keepalive_and_principal_expiry_close_during_authorization_and_ignore_late_grants() {
    for mode in [4, 5, 7] {
        let policy = Policy::default();
        let mut cfg = config();
        cfg.anonymous_access = false;
        cfg.auth = Some(AuthProviderConfig {
            component: "auth".into(),
            timeout: Duration::from_secs(4),
        });
        let (app, broker) = setup_config(policy.clone(), cfg).await;
        let addr = broker.local_addr().unwrap();
        if mode == 7 {
            policy.mode.store(7, Ordering::SeqCst);
        }
        let mut client = Client(TcpStream::connect(addr).await.unwrap());
        let mut body = string("MQTT");
        body.extend([4, 128, 0, if mode == 7 { 0 } else { 1 }]);
        body.extend(string("auth-deadline"));
        body.extend(string("alice"));
        client.send(codec::frame(0x10, &body)).await;
        assert_eq!(client.recv().await, (0x20, vec![0, 0]));
        policy.mode.store(mode, Ordering::SeqCst);
        if mode == 5 {
            let mut body = vec![0, 9];
            body.extend(string("late/#"));
            body.push(1);
            client.send(codec::frame(0x82, &body)).await;
        } else {
            let mut body = string("late/retained");
            body.extend([0, 9]);
            body.extend(b"must-not-admit");
            client.send(codec::frame(0x33, &body)).await;
        }
        tokio::time::timeout(Duration::from_secs(1), policy.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), client.0.read_u8()).await,
            Ok(Err(_))
        ));
        policy.mode.store(0, Ordering::SeqCst);
        policy.gate.add_permits(1);
        let (mut healthy, code, _) = connect(addr, "healthy", "alice", true).await;
        assert_eq!(code, 0);
        super::qos2::ping(&mut healthy).await;
        assert_eq!(broker.status().retained, 0);
        assert_eq!(broker.status().subscriptions, 0);
        assert_eq!(broker.status().generation, 1);
        stop(&app).await;
    }
}

#[test]
fn principal_deadline_preserves_long_lifetimes_and_unlimited_expiry() {
    let mut principal = Principal {
        key: PrincipalKey {
            domain: "fixture".into(),
            subject: "alice".into(),
        },
        policy_version: 1,
        expires_at_ms: unix_time_ms() + 172_800_000,
    };
    let deadline = crate::auth::deadline(&principal).unwrap();
    assert!(deadline.duration_since(tokio::time::Instant::now()) > Duration::from_secs(172_799));
    principal.expires_at_ms = u64::MAX;
    assert!(crate::auth::deadline(&principal).is_none());
    principal.expires_at_ms = unix_time_ms() - 1;
    assert!(crate::auth::deadline(&principal).unwrap() <= tokio::time::Instant::now());
}
