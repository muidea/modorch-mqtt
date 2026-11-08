//! Compare admitted and recovered accounting of one MQTT5 idempotency record.
use modorch::*;
use modorch_mqtt_api::*;
use modorch_mqtt_core::{
    BrokerHandle, MqttBrokerConfig, MqttBrokerFactory, PUBLISH_TOPIC, StorageProviderConfig,
};
use modorch_mqtt_storage::{SqliteStorageConfig, SqliteStorageFactory};
use std::{path::Path, time::Duration};

#[derive(Clone)]
struct Publisher;
impl ModuleFactory for Publisher {
    fn descriptor(&self) -> ComponentDescriptor {
        ComponentDescriptor::new(ComponentRef::module("business"))
    }
    fn create(&self, _: &ComponentConfig) -> FrameworkResult<Box<dyn Module>> {
        Ok(Box::new(Self))
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
            handler: Box::new(Self),
            subscriptions: vec![SubscriptionSpec::new(
                9000,
                ComponentRef::module("business"),
                "audit/publish",
            )],
        }))
    }
}
#[async_trait]
impl SerialHandler for Publisher {
    async fn handle(
        &mut self,
        ctx: &HandlerContext,
        msg: MessageEnvelope,
    ) -> FrameworkResult<HandlerReply> {
        let request = msg.payload::<MqttPublishRequest>()?;
        Ok(ctx
            .event_hub
            .request::<_, MqttPublishResult>(
                PUBLISH_TOPIC,
                Destination::Component(ComponentRef::module("mqtt-broker")),
                request.as_ref().clone(),
                MessageOptions {
                    payload_bytes: Some(8192),
                    ..Default::default()
                },
            )?
            .wait(Duration::from_secs(3))
            .await?)
    }
}
async fn start(path: &Path) -> (ApplicationHandle, BrokerHandle) {
    let access = StoreAccessKey([7; 32]);
    let core = MqttBrokerFactory::new(MqttBrokerConfig {
        mqtt5: true,
        max_packet_bytes: 8192,
        storage: Some(StorageProviderConfig {
            component: "store".into(),
            access: access.clone(),
            max_snapshot_bytes: 1024 * 1024,
            recovery_page_bytes: 4096,
            timeout: Duration::from_secs(2),
            recovery_timeout: Duration::from_secs(10),
        }),
        ..MqttBrokerConfig::loopback_development()
    })
    .unwrap();
    let broker = core.handle();
    let mut builder = ApplicationBuilder::new("storage-audit");
    builder
        .register_module(
            SqliteStorageFactory::new(SqliteStorageConfig {
                name: "store".into(),
                broker: "mqtt-broker".into(),
                subscription_id: 8000,
                access,
                database: path.into(),
                max_snapshot_bytes: 1024 * 1024,
                disk_budget_bytes: 12 * 1024 * 1024,
            })
            .unwrap(),
        )
        .unwrap();
    builder.register_module(core).unwrap();
    builder.register_module(Publisher).unwrap();
    let mut selection = ComponentSelection::new();
    selection
        .enable_module("store")
        .enable_module("mqtt-broker")
        .enable_module("business");
    (
        builder
            .selection(selection)
            .build()
            .unwrap()
            .start()
            .await
            .unwrap(),
        broker,
    )
}
#[tokio::main]
async fn main() {
    let path = std::env::temp_dir().join(format!(
        "mqtt-remaining-storage-{}.sqlite",
        std::process::id()
    ));
    assert!(!path.exists());
    let (app, broker) = start(&path).await;
    let baseline = broker.status().memory_used_bytes;
    let request = MqttPublishRequest {
        request_id: "audit-id".into(),
        topic: "audit/output".into(),
        payload: b"payload".as_slice().into(),
        qos: Qos::AtLeastOnce,
        retain: false,
        properties: MqttProperties {
            content_type: Some("x".repeat(4096)),
            ..Default::default()
        },
    };
    let property_cost = request.properties.byte_cost();
    let result = app
        .event_hub()
        .external()
        .request::<_, MqttPublishResult>(
            "audit/publish",
            Destination::Component(ComponentRef::module("business")),
            request.clone(),
            MessageOptions {
                payload_bytes: Some(8192),
                ..Default::default()
            },
        )
        .unwrap()
        .wait(Duration::from_secs(3))
        .await
        .unwrap();
    assert!(result.is_ok(), "{result:?}");
    assert_eq!(broker.status().publish_records, 1);
    let before = broker.status().memory_used_bytes;
    app.shutdown_checked(ShutdownOptions::graceful())
        .await
        .unwrap();
    let (app, broker) = start(&path).await;
    let after = broker.status().memory_used_bytes;
    assert_eq!(broker.status().publish_records, 1);
    assert_eq!(before - after, property_cost);
    let result = app
        .event_hub()
        .external()
        .request::<_, MqttPublishResult>(
            "audit/publish",
            Destination::Component(ComponentRef::module("business")),
            request,
            MessageOptions {
                payload_bytes: Some(8192),
                ..Default::default()
            },
        )
        .unwrap()
        .wait(Duration::from_secs(3))
        .await
        .unwrap();
    assert!(
        result.is_ok(),
        "metadata lost from idempotency request: {result:?}"
    );
    println!(
        "REPRODUCED R-04: baseline={baseline}, admitted={before}, recovered={after}; metadata remains in idempotency request but {property_cost} bytes disappear from budget"
    );
    app.shutdown_checked(ShutdownOptions::graceful())
        .await
        .unwrap();
    std::fs::remove_file(path).unwrap();
    println!("Both Applications checked shutdown: success");
}
