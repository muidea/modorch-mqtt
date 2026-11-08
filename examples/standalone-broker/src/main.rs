use modorch::{ApplicationBuilder, ComponentSelection, ExecutionResources, ShutdownOptions};
use modorch_mqtt::{MAX_MQTT_TOML_BYTES, MqttBrokerBundle, MqttBrokerConfig, MqttTomlConfig};
use std::io::Read;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let (mut address, mut file) = (None, None);
    let (mut benchmark, mut once) = (false, false);
    let usage =
        "usage: standalone-broker [LISTEN_ADDR] [--benchmark] [--once] | --config FILE [--once]";
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" if file.is_none() => file = Some(args.next().ok_or(usage)?),
            "--benchmark" if !benchmark => benchmark = true,
            "--once" if !once => once = true,
            _ if !arg.starts_with('-') && address.is_none() => address = Some(arg),
            _ => return Err(usage.into()),
        }
    }
    if file.is_some() && (address.is_some() || benchmark) {
        return Err("--config cannot be combined with LISTEN_ADDR or --benchmark".into());
    }
    let (config, module_ids) = if let Some(file) = file {
        // Before building the Application: bounded startup IO owned by this host.
        let path = std::path::Path::new(&file);
        let mut input = String::new();
        std::fs::File::open(path)?
            .take(MAX_MQTT_TOML_BYTES as u64 + 1)
            .read_to_string(&mut input)
            .map_err(|_| "unable to read bounded UTF-8 MQTT configuration")?;
        let plan = MqttTomlConfig::parse(
            &input,
            path.parent().unwrap_or_else(|| std::path::Path::new(".")),
            None,
        )?;
        let ids = plan.module_ids().map(str::to_owned).collect::<Vec<_>>();
        (plan.into_core_config(), ids)
    } else {
        let mut config = MqttBrokerConfig::loopback_development();
        if let Some(address) = address {
            config.listener = address.parse()?;
        }
        if benchmark {
            config.max_connections = 512;
            config.max_connections_per_ip = 512;
            config.max_connection_attempts_per_ip = 1024;
            config.max_packet_bytes = 4096;
        }
        let name = config.name.clone();
        (Some(config), vec![name])
    };
    let Some(config) = config else {
        println!("MQTT disabled by configuration");
        return Ok(());
    };
    let config_summary = format!("{config:?}");
    let mut builder = ApplicationBuilder::new("standalone-mqtt");
    if benchmark {
        builder = builder.execution_resources(ExecutionResources {
            max_tasks_per_owner: 1024,
            ..Default::default()
        });
    }
    let mqtt = MqttBrokerBundle::new(config).register(&mut builder)?;
    let mut selection = ComponentSelection::new();
    for id in module_ids {
        selection.enable_module(id);
    }
    let app = builder.selection(selection).build()?.start().await?;
    println!(
        "Experimental MQTT 3.1.1 QoS0/1/2 listening on {}",
        mqtt.handle().local_addr().unwrap()
    );
    if benchmark {
        println!(
            "Benchmark configuration: {config_summary}; host max_tasks=1024, max_tasks_per_owner=1024"
        );
    }
    let signal_result = if once {
        Ok(())
    } else {
        tokio::signal::ctrl_c().await
    };
    let shutdown_result = app.shutdown_checked(ShutdownOptions::graceful()).await;
    shutdown_result?;
    signal_result?;
    println!("MQTT checked shutdown complete");
    Ok(())
}
