use argon2::{Argon2, PasswordHasher, password_hash::SaltString};
use modorch::{prelude::*, *};
use modorch_mqtt_api::*;
use modorch_mqtt_auth::*;
use modorch_mqtt_core::{AuthProviderConfig, BrokerHandle, MqttBrokerConfig, MqttBrokerFactory};
use std::{net::SocketAddr, sync::OnceLock, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

fn password_hash() -> String {
    static HASH: OnceLock<String> = OnceLock::new();
    HASH.get_or_init(|| {
        Argon2::default()
            .hash_password(
                b"test-password",
                &SaltString::encode_b64(b"fixture-salt-1234").unwrap(),
            )
            .unwrap()
            .to_string()
    })
    .clone()
}
fn policy() -> PasswordAuthConfig {
    PasswordAuthConfig {
        users: ["alice", "bob"]
            .into_iter()
            .map(|name| UserPolicy {
                username: name.into(),
                subject: name.into(),
                password_hash: password_hash(),
                client_id_prefixes: vec!["client-".into()],
                publish: vec![
                    PublishRule {
                        filter: "secure/data".into(),
                        max_qos: Qos::AtLeastOnce,
                        allow_retain: true,
                    },
                    PublishRule {
                        filter: "secure/will".into(),
                        max_qos: Qos::AtLeastOnce,
                        allow_retain: false,
                    },
                ],
                subscribe: vec!["secure/#".into()],
            })
            .collect(),
        ..Default::default()
    }
}
async fn start(policy: PasswordAuthConfig) -> (ApplicationHandle, BrokerHandle) {
    let mut builder = ApplicationBuilder::new("auth-integration");
    let config = MqttBrokerConfig {
        listener: "127.0.0.1:0".parse().unwrap(),
        // Debug Argon2 runs concurrently in this suite; timeout behavior has
        // separate short-deadline Core tests. Do not depend on host CPU speed.
        max_packet_bytes: 8192,
        handshake_timeout: Duration::from_secs(30),
        auth: Some(AuthProviderConfig {
            component: policy.name.clone(),
            timeout: Duration::from_secs(30),
        }),
        ..Default::default()
    };
    let factory = MqttBrokerFactory::new(config).unwrap();
    let handle = factory.handle();
    let mut selection = ComponentSelection::new();
    selection
        .enable_module("mqtt-broker")
        .enable_module(&policy.name);
    builder
        .register_module(PasswordAuthFactory::new(policy).unwrap())
        .unwrap();
    builder.register_module(factory).unwrap();
    builder.register_module(Publisher).unwrap();
    selection.enable_module("business");
    (
        builder
            .selection(selection)
            .build()
            .unwrap()
            .start()
            .await
            .unwrap(),
        handle,
    )
}
fn string(value: &str) -> Vec<u8> {
    let mut v = (value.len() as u16).to_be_bytes().to_vec();
    v.extend_from_slice(value.as_bytes());
    v
}
fn frame(header: u8, body: &[u8]) -> Vec<u8> {
    let mut output = vec![header];
    let mut len = body.len();
    loop {
        let digit = (len % 128) as u8;
        len /= 128;
        output.push(digit | if len > 0 { 128 } else { 0 });
        if len == 0 {
            break;
        }
    }
    output.extend_from_slice(body);
    output
}
struct Client(TcpStream);
impl Client {
    async fn connect(
        address: SocketAddr,
        id: &str,
        username: &str,
        password: &str,
        clean: bool,
        will: Option<(&str, bool)>,
    ) -> (Self, u8, bool) {
        let mut client = Self(TcpStream::connect(address).await.unwrap());
        let mut body = string("MQTT");
        body.extend([
            4,
            0xc0 | (u8::from(clean) * 2)
                | will.map_or(0, |(_, retain)| 12 | (u8::from(retain) * 32)),
            0,
            0,
        ]);
        body.extend(string(id));
        if let Some((topic, _)) = will {
            body.extend(string(topic));
            body.extend(string("will-message"));
        }
        body.extend(string(username));
        body.extend(string(password));
        client.send(0x10, &body).await;
        let (header, response) = client.recv().await;
        assert_eq!(header, 0x20);
        (client, response[1], response[0] != 0)
    }
    async fn send(&mut self, header: u8, body: &[u8]) {
        self.0.write_all(&frame(header, body)).await.unwrap();
    }
    async fn recv(&mut self) -> (u8, Vec<u8>) {
        tokio::time::timeout(Duration::from_secs(35), async {
            let header = self.0.read_u8().await.unwrap();
            let mut len = 0;
            for shift in [0, 7, 14, 21] {
                let value = self.0.read_u8().await.unwrap();
                len |= usize::from(value & 127) << shift;
                if value & 128 == 0 {
                    break;
                }
            }
            let mut body = vec![0; len];
            self.0.read_exact(&mut body).await.unwrap();
            (header, body)
        })
        .await
        .unwrap()
    }
    async fn subscribe(&mut self, filter: &str) -> u8 {
        let mut body = vec![0, 1];
        body.extend(string(filter));
        body.push(1);
        self.send(0x82, &body).await;
        self.recv().await.1[2]
    }
    async fn publish(&mut self, topic: &str, payload: &[u8]) {
        let mut body = string(topic);
        body.extend([0, 1]);
        body.extend_from_slice(payload);
        self.send(0x32, &body).await;
    }
    async fn ping(&mut self) {
        self.send(0xc0, &[]).await;
        assert_eq!(self.recv().await, (0xd0, vec![]));
    }
}
async fn stop(app: &ApplicationHandle) {
    app.shutdown_checked(ShutdownOptions::graceful())
        .await
        .unwrap();
}

#[tokio::test]
async fn authenticated_owner_cannot_be_taken_over_or_cleared_by_another_identity() {
    let (app, handle) = start(policy()).await;
    let address = handle.local_addr().unwrap();
    let (mut alice, code, _) = Client::connect(
        address,
        "client-shared",
        "alice",
        "test-password",
        false,
        None,
    )
    .await;
    assert_eq!(code, 0);
    assert_eq!(alice.subscribe("secure/#").await, 1);
    for clean in [false, true] {
        let (_bob, code, _) = Client::connect(
            address,
            "client-shared",
            "bob",
            "test-password",
            clean,
            None,
        )
        .await;
        assert_eq!(code, 5);
        alice.ping().await;
    }
    let (_bad, code, _) = Client::connect(
        address,
        "client-shared",
        "alice",
        "wrong-password",
        true,
        None,
    )
    .await;
    assert_eq!(code, 5);
    alice.ping().await;
    let (_bad, code, _) = Client::connect(
        address,
        "unassigned-id",
        "alice",
        "test-password",
        true,
        None,
    )
    .await;
    assert_eq!(code, 5);
    alice.send(0xe0, &[]).await;
    drop(alice);
    let (mut restored, code, present) = Client::connect(
        address,
        "client-shared",
        "alice",
        "test-password",
        false,
        None,
    )
    .await;
    assert_eq!(code, 0);
    assert!(present);
    restored.publish("secure/data", b"still-subscribed").await;
    assert_eq!(restored.recv().await.0, 0x40);
    let (_, body) = restored.recv().await;
    assert!(body.ends_with(b"still-subscribed"));
    stop(&app).await;
}

#[tokio::test]
async fn will_authorization_precedes_takeover_and_subscription_acl_cannot_be_widened() {
    let (app, handle) = start(policy()).await;
    let address = handle.local_addr().unwrap();
    let (mut original, code, _) = Client::connect(
        address,
        "client-original",
        "alice",
        "test-password",
        false,
        None,
    )
    .await;
    assert_eq!(code, 0);
    assert_eq!(original.subscribe("secure/#").await, 1);
    assert_eq!(original.subscribe("#").await, 128);
    assert_eq!(original.subscribe("secure/+/nested").await, 1);
    for will in [
        ("forbidden/will", false),
        ("secure/will", true),
        ("$modorch/mqtt/control/publish", false),
    ] {
        let (_denied, code, _) = Client::connect(
            address,
            "client-original",
            "alice",
            "test-password",
            true,
            Some(will),
        )
        .await;
        assert_eq!(code, 5);
        original.ping().await;
        assert_eq!(handle.status().pending_wills, 0);
    }
    let (will_client, code, _) = Client::connect(
        address,
        "client-will",
        "bob",
        "test-password",
        true,
        Some(("secure/will", false)),
    )
    .await;
    assert_eq!(code, 0);
    drop(will_client);
    let (_, body) = original.recv().await;
    assert!(body.ends_with(b"will-message"));
    original.publish("forbidden/data", b"denied").await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), original.0.read(&mut [0]))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    stop(&app).await;
}

#[tokio::test]
async fn expiry_closes_idle_connections_and_auth_configuration_has_no_implicit_anonymous_fallback()
{
    let mut config = policy();
    config.principal_ttl = Duration::from_secs(1);
    let (app, handle) = start(config).await;
    let (mut client, code, _) = Client::connect(
        handle.local_addr().unwrap(),
        "client-expire",
        "alice",
        "test-password",
        true,
        None,
    )
    .await;
    assert_eq!(code, 0);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), client.0.read(&mut [0]))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    stop(&app).await;
    let config = MqttBrokerConfig {
        auth: Some(AuthProviderConfig {
            component: "mqtt-auth".into(),
            timeout: Duration::from_secs(1),
        }),
        ..MqttBrokerConfig::loopback_development()
    };
    assert!(config.validate().is_err());
    let debug = format!(
        "{:?}",
        Authenticate {
            client_id: "a".into(),
            username: Some("private-name".into()),
            password: Some(b"very-secret".as_slice().into()),
            peer_ip: "127.0.0.1".parse().unwrap()
        }
    );
    assert!(!debug.contains("private-name") && !debug.contains("very-secret"));
    assert!(!format!("{:?}", policy()).contains("$argon2id$"));
}

#[test]
fn filter_containment_preserves_root_dollar_and_empty_level_semantics() {
    for (allowed, requested, expected) in [
        ("secure/#", "secure", true),
        ("secure/+", "secure/", true),
        ("secure/+", "secure/#", false),
        ("secure/+", "secure/+/nested", false),
        ("#", "$SYS/#", false),
        ("+/status", "$SYS/status", false),
        ("$SYS/#", "$SYS/+", true),
        ("a/#", "a/+/b", true),
        ("a/+", "a/+", true),
        ("a/b", "a/+", false),
        ("#", "#", true),
        ("a/#", "a", true),
        ("a", "a/#", false),
        ("a/+", "a", false),
        ("a/+", "a/+#", false),
    ] {
        assert_eq!(
            filter_covers(allowed, requested),
            expected,
            "{allowed} covers {requested}"
        );
    }
    let mut config = policy();
    config.users.push(config.users[0].clone());
    assert!(config.validate().is_err());
    let mut config = policy();
    config.users[0].password_hash = "plaintext".into();
    assert!(config.validate().is_err());
}

struct Publisher;
impl ModuleFactory for Publisher {
    fn descriptor(&self) -> ComponentDescriptor {
        ComponentDescriptor::new(ComponentRef::module("business"))
    }
    fn create(&self, _: &ComponentConfig) -> FrameworkResult<Box<dyn Module>> {
        Ok(Box::new(Publisher))
    }
}
#[async_trait]
impl Lifecycle for Publisher {}
impl Module for Publisher {
    fn descriptor(&self) -> ComponentDescriptor {
        ModuleFactory::descriptor(self)
    }
    fn handlers(&mut self) -> FrameworkResult<Option<HandlerRegistration>> {
        Ok(Some(HandlerRegistration::Serial {
            handler: Box::new(Publisher),
            subscriptions: vec![SubscriptionSpec::new(
                1001,
                ComponentRef::module("business"),
                "test/publish",
            )],
        }))
    }
}
#[async_trait]
impl SerialHandler for Publisher {
    async fn handle(
        &mut self,
        ctx: &HandlerContext,
        message: MessageEnvelope,
    ) -> FrameworkResult<HandlerReply> {
        let request = message.payload::<MqttPublishRequest>()?;
        let result = ctx
            .event_hub
            .request::<_, MqttPublishResult>(
                modorch_mqtt_core::PUBLISH_TOPIC,
                Destination::Component(ComponentRef::module("mqtt-broker")),
                request.as_ref().clone(),
                MessageOptions {
                    payload_bytes: Some(1024),
                    ..Default::default()
                },
            )?
            .wait(Duration::from_secs(2))
            .await?;
        Ok(result)
    }
}
#[tokio::test]
async fn internal_publications_require_component_policy_and_provider_refuses_forged_origin() {
    let mut config = policy();
    config.internal.push(InternalPolicy {
        source: "Module:business".into(),
        publish: vec![PublishRule {
            filter: "secure/data".into(),
            max_qos: Qos::AtLeastOnce,
            allow_retain: true,
        }],
    });
    let (app, handle) = start(config).await;
    for (id, topic, allowed) in [
        ("allowed", "secure/data", true),
        ("forbidden", "secret/topic", false),
    ] {
        let result = app
            .event_hub()
            .external()
            .request::<_, MqttPublishResult>(
                "test/publish",
                Destination::Component(ComponentRef::module("business")),
                MqttPublishRequest {
                    request_id: id.into(),
                    topic: topic.into(),
                    payload: b"business".as_slice().into(),
                    qos: Qos::AtLeastOnce,
                    retain: true,
                    properties: Default::default(),
                },
                MessageOptions {
                    payload_bytes: Some(1024),
                    ..Default::default()
                },
            )
            .unwrap()
            .wait(Duration::from_secs(3))
            .await
            .unwrap();
        if allowed {
            assert!(result.as_ref().as_ref().unwrap().accepted);
        } else {
            assert_eq!(*result, Err(MqttPublishRejection::NotAuthorized));
        }
    }
    assert_eq!(handle.status().retained, 1);
    assert_eq!(handle.status().publish_records, 1);
    let forged = app
        .event_hub()
        .external()
        .request::<_, AuthResult>(
            AUTH_TOPIC,
            Destination::Component(ComponentRef::module("mqtt-auth")),
            AuthRequest::Publish(PublishAuthContext {
                principal: None,
                internal_source: Some("Module:business".into()),
                topic: "secure/data".into(),
                qos: Qos::AtLeastOnce,
                retain: true,
                kind: PublishKind::InternalPublish,
            }),
            MessageOptions {
                payload_bytes: Some(1024),
                ..Default::default()
            },
        )
        .unwrap()
        .wait(Duration::from_secs(1))
        .await
        .unwrap();
    assert!(matches!(*forged, Err(AuthError::Denied)));
    stop(&app).await;
}

#[tokio::test]
async fn qos2_publish_acl_uses_original_qos_and_retries_do_not_create_another_exchange() {
    let mut config = policy();
    config.users[0].publish[0].max_qos = Qos::ExactlyOnce;
    let (app, broker) = start(config).await;
    let addr = broker.local_addr().unwrap();
    let (mut alice, code, _) =
        Client::connect(addr, "client-alice", "alice", "test-password", false, None).await;
    assert_eq!(code, 0);
    let mut body = string("secure/data");
    body.extend([0, 9]);
    body.extend(b"qos2");
    alice.send(0x34, &body).await;
    assert_eq!(alice.recv().await, (0x50, vec![0, 9]));
    alice.send(0x3c, &body).await;
    assert_eq!(alice.recv().await, (0x50, vec![0, 9]));
    assert_eq!(broker.status().received_publications, 1);
    let (mut bob, code, _) =
        Client::connect(addr, "client-bob", "bob", "test-password", false, None).await;
    assert_eq!(code, 0);
    bob.send(0x34, &body).await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), bob.0.read(&mut [0]))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    assert_eq!(broker.status().incoming_qos2, 1);
    alice.send(0x62, &[0, 9]).await;
    assert_eq!(alice.recv().await, (0x70, vec![0, 9]));
    assert_eq!(broker.status().incoming_qos2, 0);
    stop(&app).await;
}

#[tokio::test]
async fn policy_replacement_uses_new_revision_and_rejects_stale_or_changed_identity() {
    let config = policy();
    let factory = PasswordAuthFactory::new(config.clone()).unwrap();
    let policy_handle = factory.handle();
    let mut builder = ApplicationBuilder::new("policy-reload");
    let mut selection = ComponentSelection::new();
    selection.enable_module("mqtt-auth");
    builder.register_module(factory).unwrap();
    let app = builder
        .selection(selection)
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();
    let request = |revision| {
        AuthRequest::Subscribe(AuthorizeSubscribe {
            principal: Principal {
                key: PrincipalKey {
                    domain: config.domain.clone(),
                    subject: "alice".into(),
                },
                policy_version: revision,
                expires_at_ms: unix_time_ms() + 60000,
            },
            filter: "secure/data".into(),
        })
    };
    let decide = |request| async {
        app.event_hub()
            .external()
            .request::<_, AuthResult>(
                AUTH_TOPIC,
                Destination::Component(ComponentRef::module("mqtt-auth")),
                request,
                Default::default(),
            )
            .unwrap()
            .wait(Duration::from_secs(1))
            .await
            .unwrap()
    };
    assert!(decide(request(1)).await.is_ok());
    let mut newer = config.clone();
    newer.policy_version = 2;
    newer.users[0].subscribe = vec!["new/#".into()];
    policy_handle.replace(newer.clone()).unwrap();
    assert_eq!(policy_handle.revision(), 2);
    assert!(decide(request(1)).await.is_err());
    assert!(decide(request(2)).await.is_err());
    assert!(policy_handle.replace(config).is_err());
    newer.domain = "other".into();
    newer.policy_version = 3;
    assert!(policy_handle.replace(newer).is_err());
    assert_eq!(policy_handle.revision(), 2);
    stop(&app).await;
}
