//! Explicit anonymous loopback composition for testing SQLite restart recovery.
use modorch::{ApplicationBuilder, ComponentSelection, ShutdownOptions};
use modorch_mqtt::*;
use std::time::Duration;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1).collect::<Vec<_>>();
    let once = args.iter().any(|a| a == "--once");
    args.retain(|a| a != "--once");
    if args.is_empty() || args.len() > 2 {
        return Err("usage: durable-broker DATABASE_PATH [LOOPBACK_ADDR] [--once]".into());
    }
    let database = args[0].clone().into();
    let listener = args
        .get(1)
        .map(String::as_str)
        .unwrap_or("127.0.0.1:1883")
        .parse()?;
    let mut key = [0; 32];
    getrandom::fill(&mut key).map_err(|_| "storage capability generation failed")?;
    let access = StoreAccessKey(key);
    let mut builder = ApplicationBuilder::new("durable-mqtt-host");
    builder.register_module(SqliteStorageFactory::new(SqliteStorageConfig {
        name: "mqtt-storage".into(),
        broker: "mqtt-broker".into(),
        subscription_id: 3000,
        access: access.clone(),
        database,
        max_snapshot_bytes: 4 * 1024 * 1024,
        disk_budget_bytes: 64 * 1024 * 1024,
    })?)?;
    let mqtt = MqttBrokerBundle::new(MqttBrokerConfig {
        listener,
        max_packet_bytes: 65536,
        max_connections: 32,
        storage: Some(StorageProviderConfig {
            component: "mqtt-storage".into(),
            access,
            max_snapshot_bytes: 4 * 1024 * 1024,
            recovery_page_bytes: 4096,
            timeout: Duration::from_secs(2),
            recovery_timeout: Duration::from_secs(30),
        }),
        ..MqttBrokerConfig::loopback_development()
    })
    .register(&mut builder)?;
    let mut selected = ComponentSelection::new();
    selected
        .enable_module("mqtt-broker")
        .enable_module("mqtt-storage");
    let app = builder.selection(selected).build()?.start().await?;
    println!(
        "Durable MQTT QoS0/1/2 listening on {}",
        mqtt.handle().local_addr().unwrap()
    );
    if !once {
        tokio::signal::ctrl_c().await?;
    }
    app.shutdown_checked(ShutdownOptions::graceful()).await?;
    println!("SQLite checked shutdown complete");
    Ok(())
}
