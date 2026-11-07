use modorch::{ApplicationBuilder, ComponentSelection, ShutdownOptions};
use modorch_mqtt::{MqttBrokerBundle, MqttBrokerConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = MqttBrokerConfig::loopback_development();
    if let Some(address) = std::env::args().nth(1) {
        config.listener = address.parse()?;
    }
    let mut builder = ApplicationBuilder::new("standalone-mqtt");
    let mqtt = MqttBrokerBundle::new(config).register(&mut builder)?;
    let mut selection = ComponentSelection::new();
    for id in mqtt.module_ids() {
        selection.enable_module(id);
    }
    let app = builder.selection(selection).build()?.start().await?;
    println!(
        "Experimental MQTT 3.1.1 QoS0/1 listening on {}",
        mqtt.handle().local_addr().unwrap()
    );
    tokio::signal::ctrl_c().await?;
    app.shutdown_checked(ShutdownOptions::graceful()).await?;
    Ok(())
}
