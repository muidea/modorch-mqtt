use modorch::{prelude::*, *};
use modorch_mqtt_api::*;
use modorch_mqtt_core::{AdminControlConfig, BrokerHandle, MqttBrokerConfig, MqttBrokerFactory};
use modorch_mqtt_extensions::*;
use std::{net::SocketAddr, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
#[derive(Clone)]
struct Operator(&'static str);
impl ModuleFactory for Operator {
    fn descriptor(&self) -> ComponentDescriptor {
        ComponentDescriptor::new(ComponentRef::module(self.0))
    }
    fn create(&self, _: &ComponentConfig) -> FrameworkResult<Box<dyn Module>> {
        Ok(Box::new(self.clone()))
    }
}
#[async_trait]
impl Lifecycle for Operator {}
impl Module for Operator {
    fn descriptor(&self) -> ComponentDescriptor {
        ModuleFactory::descriptor(self)
    }
    fn handlers(&mut self) -> FrameworkResult<Option<HandlerRegistration>> {
        Ok(Some(HandlerRegistration::Serial {
            handler: Box::new(self.clone()),
            subscriptions: vec![SubscriptionSpec::new(
                if self.0 == "operator" { 3000 } else { 3001 },
                ComponentRef::module(self.0),
                "test/admin",
            )],
        }))
    }
}
#[async_trait]
impl SerialHandler for Operator {
    async fn handle(
        &mut self,
        ctx: &HandlerContext,
        message: MessageEnvelope,
    ) -> FrameworkResult<HandlerReply> {
        let request = message.payload::<AdminRequest>()?;
        Ok(ctx
            .event_hub
            .request::<_, AdminResult>(
                ADMIN_TOPIC,
                Destination::Component(ComponentRef::module("mqtt-admin")),
                request.as_ref().clone(),
                MessageOptions {
                    payload_bytes: Some(65536),
                    ..Default::default()
                },
            )?
            .wait(Duration::from_secs(2))
            .await?)
    }
}
async fn setup() -> (ApplicationHandle, BrokerHandle, AdminHandle, MetricsHandle) {
    let mut builder = ApplicationBuilder::new("optional-extensions");
    let factory = MqttBrokerFactory::new(MqttBrokerConfig {
        listener: "127.0.0.1:0".parse().unwrap(),
        max_packet_bytes: 8192,
        admin: Some(AdminControlConfig {
            subscription_id: 4,
            allowed_sources: vec![ComponentRef::module("mqtt-admin")],
        }),
        ..MqttBrokerConfig::loopback_development()
    })
    .unwrap();
    let broker = factory.handle();
    builder.register_module(factory).unwrap();
    let admin = AdminFactory::new(AdminConfig {
        allowed_callers: vec![ComponentRef::module("operator")],
        audit_capacity: 4,
        ..Default::default()
    })
    .unwrap();
    let audit = admin.handle();
    builder.register_module(admin).unwrap();
    let metrics = MetricsFactory::new(MetricsConfig {
        interval: Duration::from_millis(100),
        timeout: Duration::from_millis(80),
        ..Default::default()
    })
    .unwrap();
    let observations = metrics.handle();
    builder.register_module(metrics).unwrap();
    builder.register_module(Operator("operator")).unwrap();
    builder.register_module(Operator("outsider")).unwrap();
    let mut selected = ComponentSelection::new();
    for name in [
        "mqtt-broker",
        "mqtt-admin",
        "mqtt-metrics",
        "operator",
        "outsider",
    ] {
        selected.enable_module(name);
    }
    (
        builder
            .selection(selected)
            .build()
            .unwrap()
            .start()
            .await
            .unwrap(),
        broker,
        audit,
        observations,
    )
}
async fn call(app: &ApplicationHandle, source: &str, request: AdminRequest) -> AdminResult {
    app.event_hub()
        .external()
        .request::<_, AdminResult>(
            "test/admin",
            Destination::Component(ComponentRef::module(source)),
            request,
            MessageOptions {
                payload_bytes: Some(65536),
                ..Default::default()
            },
        )
        .unwrap()
        .wait(Duration::from_secs(2))
        .await
        .unwrap()
        .as_ref()
        .clone()
}
fn string(s: &str) -> Vec<u8> {
    let mut v = (s.len() as u16).to_be_bytes().to_vec();
    v.extend_from_slice(s.as_bytes());
    v
}
async fn connect(address: SocketAddr, id: &str) -> TcpStream {
    let mut stream = TcpStream::connect(address).await.unwrap();
    let mut body = string("MQTT");
    body.extend([4, 0, 0, 0]);
    body.extend(string(id));
    let mut frame = vec![0x10, body.len() as u8];
    frame.extend(body);
    stream.write_all(&frame).await.unwrap();
    let mut response = [0; 4];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!([response[0], response[1], response[3]], [0x20, 2, 0]);
    stream
}
#[tokio::test]
async fn administration_is_source_protected_paged_audited_and_disconnect_is_connection_fenced() {
    let (app, broker, audit, _metrics) = setup().await;
    let address = broker.local_addr().unwrap();
    let mut first = connect(address, "a-client").await;
    let _second = connect(address, "b-client").await;
    let mut body = vec![0, 7];
    body.extend(string("a/+"));
    body.push(1);
    body.extend(string("$SYS/#"));
    body.push(0);
    let mut packet = vec![0x82, body.len() as u8];
    packet.extend(body);
    first.write_all(&packet).await.unwrap();
    let mut suback = [0; 6];
    first.read_exact(&mut suback).await.unwrap();
    assert_eq!(suback, [0x90, 4, 0, 7, 1, 0]);
    let cursor = match call(
        &app,
        "operator",
        AdminRequest::ListSubscriptions {
            client_id: "a-client".into(),
            after: None,
            limit: 1,
        },
    )
    .await
    .unwrap()
    {
        AdminReply::Subscriptions { items, next } => {
            assert_eq!(items[0].filter, "$SYS/#");
            assert_eq!(items[0].qos, Qos::AtMostOnce);
            next.unwrap()
        }
        _ => panic!("missing subscriptions"),
    };
    match call(
        &app,
        "operator",
        AdminRequest::ListSubscriptions {
            client_id: "a-client".into(),
            after: Some(cursor),
            limit: 1,
        },
    )
    .await
    .unwrap()
    {
        AdminReply::Subscriptions { items, next } => {
            assert_eq!(items[0].filter, "a/+");
            assert!(next.is_none());
        }
        _ => panic!("missing subscriptions"),
    };
    let page = call(
        &app,
        "operator",
        AdminRequest::ListClients {
            after: None,
            limit: 1,
        },
    )
    .await
    .unwrap();
    let cursor = match page {
        AdminReply::Clients { items, next } => {
            assert_eq!(items.len(), 1);
            assert_eq!(items[0].client_id, "a-client");
            next.unwrap()
        }
        _ => panic!("wrong reply"),
    };
    match call(
        &app,
        "operator",
        AdminRequest::ListClients {
            after: Some(cursor),
            limit: 1,
        },
    )
    .await
    .unwrap()
    {
        AdminReply::Clients { items, next } => {
            assert_eq!(items[0].client_id, "b-client");
            assert!(next.is_none());
        }
        _ => panic!("wrong reply"),
    }
    assert!(matches!(
        call(&app, "outsider", AdminRequest::BrokerStats).await,
        Err(AdminError::NotAuthorized)
    ));
    let mut options = MessageOptions {
        payload_bytes: Some(256),
        ..Default::default()
    };
    options
        .headers
        .insert("source".into(), "Module:mqtt-admin".into());
    let denied = app
        .event_hub()
        .external()
        .request::<_, AdminResult>(
            ADMIN_CONTROL_TOPIC,
            Destination::Component(ComponentRef::module("mqtt-broker")),
            AdminRequest::BrokerStats,
            options,
        )
        .unwrap()
        .wait(Duration::from_secs(1))
        .await
        .unwrap();
    assert!(matches!(*denied, Err(AdminError::NotAuthorized)));
    let old_token = match call(
        &app,
        "operator",
        AdminRequest::GetSession {
            client_id: "a-client".into(),
        },
    )
    .await
    .unwrap()
    {
        AdminReply::Session(Some(session)) => session.connection_id.unwrap(),
        _ => panic!("missing session"),
    };
    let mut replacement = connect(address, "a-client").await;
    assert_eq!(first.read(&mut [0]).await.unwrap(), 0);
    assert!(matches!(
        call(
            &app,
            "operator",
            AdminRequest::DisconnectClient {
                client_id: "a-client".into(),
                expected_connection_id: old_token
            }
        )
        .await
        .unwrap(),
        AdminReply::Disconnected(false)
    ));
    replacement.write_all(&[0xc0, 0]).await.unwrap();
    let mut pong = [0; 2];
    replacement.read_exact(&mut pong).await.unwrap();
    assert_eq!(pong, [0xd0, 0]);
    let token = match call(
        &app,
        "operator",
        AdminRequest::GetSession {
            client_id: "a-client".into(),
        },
    )
    .await
    .unwrap()
    {
        AdminReply::Session(Some(session)) => session.connection_id.unwrap(),
        _ => panic!("missing session"),
    };
    assert!(matches!(
        call(
            &app,
            "operator",
            AdminRequest::DisconnectClient {
                client_id: "a-client".into(),
                expected_connection_id: token
            }
        )
        .await
        .unwrap(),
        AdminReply::Disconnected(true)
    ));
    assert_eq!(replacement.read(&mut [0]).await.unwrap(), 0);
    assert!(matches!(
        call(
            &app,
            "operator",
            AdminRequest::ListClients {
                after: None,
                limit: 257
            }
        )
        .await,
        Err(AdminError::InvalidRequest)
    ));
    assert_eq!(audit.audit().len(), 4);
    assert!(audit.audit().iter().all(|a| a.timestamp_ms > 0));
    assert!(audit.audit().iter().any(|a| !a.success));
    app.shutdown_checked(ShutdownOptions::graceful())
        .await
        .unwrap();
}
#[tokio::test]
async fn metrics_pull_real_counters_without_client_or_topic_labels_and_stop_cleanly() {
    let (app, broker, _audit, metrics) = setup().await;
    let mut socket = connect(broker.local_addr().unwrap(), "sensitive-client-id").await;
    let mut body = string("secret/topic");
    body.extend([0, 1]);
    body.extend(b"payload");
    let mut frame = vec![0x33, body.len() as u8];
    frame.extend(body);
    socket.write_all(&frame).await.unwrap();
    let mut ack = [0; 4];
    socket.read_exact(&mut ack).await.unwrap();
    assert_eq!(ack, [0x40, 2, 0, 1]);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if metrics
                .snapshot()
                .is_some_and(|s| s.broker.received_publications == 1)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let sample = metrics.snapshot().unwrap();
    assert!(sample.broker.bytes_received > 0 && sample.broker.bytes_sent > 0);
    assert_eq!(sample.broker.accepted_publications, 1);
    assert_eq!(sample.broker.connections, 1);
    let text = metrics.prometheus();
    assert!(text.contains("modorch_mqtt_received_publications_total 1\n"));
    assert!(!text.contains("sensitive-client-id") && !text.contains("secret/topic"));
    assert!(text.contains("modorch_mqtt_dropped_oversized_total 0\n"));
    assert_eq!(text.lines().count(), 37);
    app.shutdown_checked(ShutdownOptions::graceful())
        .await
        .unwrap();
    assert_eq!(broker.status().connections, 0);
}
#[tokio::test]
async fn optional_factories_do_not_enable_themselves_or_export_listeners() {
    let mut builder = ApplicationBuilder::new("disabled-extensions");
    let admin = AdminFactory::new(Default::default()).unwrap();
    let audit = admin.handle();
    let metrics = MetricsFactory::new(Default::default()).unwrap();
    let observation = metrics.handle();
    builder.register_module(admin).unwrap();
    builder.register_module(metrics).unwrap();
    let app = builder.build().unwrap().start().await.unwrap();
    assert!(audit.audit().is_empty());
    assert!(observation.snapshot().is_none());
    assert!(observation.prometheus().is_empty());
    app.shutdown_checked(ShutdownOptions::graceful())
        .await
        .unwrap();
}

#[tokio::test]
async fn metrics_task_quota_failure_compensates_the_already_active_broker() {
    let mut builder = ApplicationBuilder::new("metrics-quota").task_limits(2, 1);
    let core = MqttBrokerFactory::new(MqttBrokerConfig {
        listener: "127.0.0.1:0".parse().unwrap(),
        ..MqttBrokerConfig::loopback_development()
    })
    .unwrap();
    let handle = core.handle();
    builder.register_module(core).unwrap();
    builder
        .register_module(MetricsFactory::new(Default::default()).unwrap())
        .unwrap();
    let mut selected = ComponentSelection::new();
    selected
        .enable_module("mqtt-broker")
        .enable_module("mqtt-metrics");
    assert!(
        builder
            .selection(selected)
            .build()
            .unwrap()
            .start()
            .await
            .is_err()
    );
    assert!(!handle.status().active);
    assert_eq!(handle.status().connections, 0);
    let _listener = tokio::net::TcpListener::bind(handle.local_addr().unwrap())
        .await
        .unwrap();
}

#[cfg(feature = "audit")]
#[tokio::test]
async fn persistent_audit_survives_application_restart_and_rejects_unrelated_database() {
    let path = std::env::temp_dir().join(format!(
        "mqtt-admin-audit-{}-{}.sqlite",
        std::process::id(),
        unix_time_ms()
    ));
    for _ in 0..2 {
        let mut builder = ApplicationBuilder::new("durable-admin-audit");
        let core = MqttBrokerFactory::new(MqttBrokerConfig {
            listener: "127.0.0.1:0".parse().unwrap(),
            max_packet_bytes: 8192,
            admin: Some(AdminControlConfig {
                subscription_id: 4,
                allowed_sources: vec![ComponentRef::module("mqtt-admin")],
            }),
            ..MqttBrokerConfig::loopback_development()
        })
        .unwrap();
        let admin = AdminFactory::new(AdminConfig {
            allowed_callers: vec![ComponentRef::module("operator")],
            ..Default::default()
        })
        .unwrap()
        .with_audit_log(SqliteAuditConfig {
            path: path.clone(),
            max_records: 16,
            max_database_bytes: 65536,
        })
        .unwrap();
        builder.register_module(core).unwrap();
        builder.register_module(admin).unwrap();
        builder.register_module(Operator("operator")).unwrap();
        let mut selection = ComponentSelection::new();
        for id in ["mqtt-broker", "mqtt-admin", "operator"] {
            selection.enable_module(id);
        }
        let app = builder
            .selection(selection)
            .build()
            .unwrap()
            .start()
            .await
            .unwrap();
        for _ in 0..10 {
            let result = app
                .event_hub()
                .external()
                .request::<_, AdminResult>(
                    "test/admin",
                    Destination::Component(ComponentRef::module("operator")),
                    AdminRequest::BrokerStats,
                    Default::default(),
                )
                .unwrap()
                .wait(Duration::from_secs(2))
                .await
                .unwrap();
            assert!(result.is_ok());
        }
        app.shutdown_checked(ShutdownOptions::graceful())
            .await
            .unwrap();
        let db = rusqlite::Connection::open(&path).unwrap();
        let rows: i64 = db
            .query_row("SELECT count(*) FROM audit", [], |r| r.get(0))
            .unwrap();
        assert_eq!(rows, 16);
        let intents: i64 = db
            .query_row("SELECT count(*) FROM audit WHERE phase='intent'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(intents, 8);
        let outcomes: i64 = db
            .query_row(
                "SELECT count(*) FROM audit WHERE phase='outcome' AND success=1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(outcomes, 8);
    }
    std::fs::remove_file(&path).unwrap();
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute_batch("CREATE TABLE unrelated(value TEXT); INSERT INTO unrelated VALUES('keep');")
        .unwrap();
    drop(db);
    let mut builder = ApplicationBuilder::new("unrelated-audit");
    let core = MqttBrokerFactory::new(MqttBrokerConfig {
        listener: "127.0.0.1:0".parse().unwrap(),
        ..MqttBrokerConfig::loopback_development()
    })
    .unwrap();
    let admin = AdminFactory::new(AdminConfig::default())
        .unwrap()
        .with_audit_log(SqliteAuditConfig {
            path: path.clone(),
            max_records: 16,
            max_database_bytes: 65536,
        })
        .unwrap();
    builder.register_module(core).unwrap();
    builder.register_module(admin).unwrap();
    let mut selection = ComponentSelection::new();
    selection
        .enable_module("mqtt-broker")
        .enable_module("mqtt-admin");
    let failed = builder
        .selection(selection)
        .build()
        .unwrap()
        .start()
        .await
        .err()
        .expect("unrelated audit must reject");
    assert!(failed.cleanup.is_ok());
    let db = rusqlite::Connection::open(&path).unwrap();
    let value: String = db
        .query_row("SELECT value FROM unrelated", [], |r| r.get(0))
        .unwrap();
    assert_eq!(value, "keep");
    drop(db);
    std::fs::remove_file(path).unwrap();
}
