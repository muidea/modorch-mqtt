//! A business module and the MQTT module share the host's sole EventHub.
use modorch::{prelude::*, *};
use modorch_mqtt::{BrokerStatus, MqttBrokerBundle, MqttBrokerConfig, STATUS_TOPIC};
use std::time::Duration;

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
        Ok(())
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
    if !once {
        tokio::signal::ctrl_c().await?;
    }
    app.shutdown_checked(ShutdownOptions::graceful()).await?;
    Ok(())
}
