//! A business module and the MQTT module share the host's sole EventHub.
use modorch::{prelude::*, *};
use modorch_mqtt::{
    BrokerStatus, MqttBrokerBundle, MqttBrokerConfig, MqttInbound, MqttPublishRequest,
    MqttPublishResult, PUBLISH_TOPIC, Qos, STATUS_TOPIC,
};
use std::{sync::Arc, time::Duration};

const BUSINESS_PUBLISH_TOPIC: &str = "$demo/business/publish";

struct Business;
impl ModuleFactory for Business {
    fn descriptor(&self) -> ComponentDescriptor {
        let mut descriptor = ComponentDescriptor::new(ComponentRef::module("business"));
        descriptor
            .requires
            .push(ComponentRef::module("mqtt-broker"));
        descriptor
    }
    fn create(&self, _: &ComponentConfig) -> FrameworkResult<Box<dyn Module>> {
        Ok(Box::new(Business))
    }
}
#[async_trait]
impl Lifecycle for Business {}
#[async_trait]
impl Module for Business {
    fn descriptor(&self) -> ComponentDescriptor {
        ModuleFactory::descriptor(self)
    }
    fn handlers(&mut self) -> FrameworkResult<Option<HandlerRegistration>> {
        Ok(Some(HandlerRegistration::Serial {
            handler: Box::new(Business),
            subscriptions: vec![
                SubscriptionSpec::new(
                    100,
                    ComponentRef::module("business"),
                    BUSINESS_PUBLISH_TOPIC,
                ),
                SubscriptionSpec::new(101, ComponentRef::module("business"), "demo/device/+"),
            ],
        }))
    }
    async fn activate_ingress(&mut self, ctx: &RuntimeContext) -> FrameworkResult<()> {
        let status = ctx
            .event_hub
            .request::<_, BrokerStatus>(
                STATUS_TOPIC,
                Destination::Component(ComponentRef::module("mqtt-broker")),
                (),
                Default::default(),
            )?
            .wait(Duration::from_secs(1))
            .await?;
        assert!(status.active);
        println!(
            "business -> MQTT Core request through host EventHub: generation {}",
            status.generation
        );
        // Lifecycle hooks have no EventHub Handler source identity. Enter our
        // business Handler, which explicitly requests a broker publication.
        let result = ctx
            .event_hub
            .request::<_, MqttPublishResult>(
                BUSINESS_PUBLISH_TOPIC,
                Destination::Component(ComponentRef::module("business")),
                MqttPublishRequest {
                    request_id: "startup-ready-v1".into(),
                    topic: "demo/business/ready".into(),
                    payload: b"business ready".as_slice().into(),
                    qos: Qos::AtLeastOnce,
                    retain: true,
                    properties: Default::default(),
                },
                MessageOptions {
                    payload_bytes: Some(512),
                    ..Default::default()
                },
            )?
            .wait(Duration::from_secs(1))
            .await?;
        let receipt = result
            .as_ref()
            .as_ref()
            .map_err(|error| FrameworkError::InvalidConfig(error.to_string()))?;
        println!(
            "business publication accepted: {} ({:?})",
            receipt.request_id, receipt.durability
        );
        Ok(())
    }
}

#[async_trait]
impl SerialHandler for Business {
    async fn handle(
        &mut self,
        ctx: &HandlerContext,
        message: MessageEnvelope,
    ) -> FrameworkResult<HandlerReply> {
        if message.kind == MessageKind::Event {
            if let Ok(event) = message.payload::<MqttInbound>() {
                println!(
                    "MQTT inbound: client={} topic={} bytes={}",
                    event.client_id,
                    event.topic,
                    event.payload.len()
                );
            }
            return Ok(Arc::new(()));
        }
        let request = message.payload::<MqttPublishRequest>()?;
        let result = ctx
            .event_hub
            .request::<_, MqttPublishResult>(
                PUBLISH_TOPIC,
                Destination::Component(ComponentRef::module("mqtt-broker")),
                request.as_ref().clone(),
                MessageOptions {
                    payload_bytes: Some(
                        256 + request.topic.len()
                            + request.request_id.len()
                            + request.payload.len(),
                    ),
                    ..Default::default()
                },
            )?
            .wait(Duration::from_secs(1))
            .await?;
        Ok(result)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let once = std::env::args().any(|arg| arg == "--once");
    let mut builder = ApplicationBuilder::new("existing-business-server");
    builder.register_module(Business)?;
    let config = MqttBrokerConfig {
        listener: if once {
            "127.0.0.1:0"
        } else {
            "127.0.0.1:1883"
        }
        .parse()?,
        ..MqttBrokerConfig::loopback_development()
    };
    let mqtt = MqttBrokerBundle::new(config).register(&mut builder)?;
    let mut selected = ComponentSelection::new();
    selected.enable_module("business");
    for id in mqtt.module_ids() {
        selected.enable_module(id);
    }
    let app = builder.selection(selected).build()?.start().await?;
    println!(
        "MQTT subset listening on {}",
        mqtt.handle().local_addr().unwrap()
    );
    assert_eq!(mqtt.handle().status().publish_records, 1);
    assert_eq!(mqtt.handle().status().retained, 1);
    if !once {
        tokio::signal::ctrl_c().await?;
    }
    app.shutdown_checked(ShutdownOptions::graceful()).await?;
    Ok(())
}
