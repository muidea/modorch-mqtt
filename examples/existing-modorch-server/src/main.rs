//! A business module and the MQTT module share the host's sole EventHub.
use modorch::{prelude::*, *};
use modorch_mqtt::{
    BrokerStatus, MqttBrokerBundle, MqttBrokerConfig, MqttInbound, MqttPublishRequest,
    MqttPublishResult, PUBLISH_TOPIC, Qos, STATUS_TOPIC,
};
use std::{io::Write, sync::Arc, time::Duration};

mod host;

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
async fn main() -> host::Result<()> {
    let mut once = false;
    let mut listener = None;
    let mut arguments = std::env::args().skip(1);
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--once" => once = true,
            "--listen" => {
                listener = Some(
                    arguments
                        .next()
                        .ok_or("--listen requires an address")?
                        .parse()?,
                );
            }
            _ => return Err(format!("unknown argument: {argument}").into()),
        }
    }
    let mut signals = host::Signals::install()?;
    let budget = host::DrainBudget::default();
    let mut builder = ApplicationBuilder::new("existing-business-server");
    builder.register_module(Business)?;
    let config = MqttBrokerConfig {
        listener: listener.unwrap_or(
            if once {
                "127.0.0.1:0"
            } else {
                "127.0.0.1:1883"
            }
            .parse()?,
        ),
        ..MqttBrokerConfig::loopback_development()
    };
    let mqtt = MqttBrokerBundle::new(config)
        .with_failure_policy(FailurePolicy::FailFast)
        .register(&mut builder)?;
    let mut selected = ComponentSelection::new();
    selected.enable_module("business");
    for id in mqtt.module_ids() {
        selected.enable_module(id);
    }
    let app = host::start(builder.selection(selected).build()?, budget).await?;
    let observed: host::Result<()> = (|| {
        let address = mqtt
            .handle()
            .local_addr()
            .ok_or("MQTT listener missing after Ready")?;
        let status = mqtt.handle().status();
        if status.publish_records != 1 || status.retained != 1 {
            return Err("startup business publication was not retained".into());
        }
        writeln!(
            std::io::stdout().lock(),
            "MQTT subset listening on {address}"
        )?;
        Ok(())
    })();
    if let Err(cause) = observed {
        if let Err(cleanup) = host::drain(&app, budget).await {
            eprintln!("Ready observation cleanup failed: {cleanup}; preserving observation cause");
        }
        return Err(cause);
    }
    host::wait_and_drain(
        &app,
        async { if once { Ok(()) } else { signals.wait().await } },
        budget,
    )
    .await?;
    writeln!(
        std::io::stdout().lock(),
        "checked shutdown: GracefullyStopped"
    )?;
    Ok(())
}
