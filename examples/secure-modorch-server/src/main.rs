//! Explicit TLS/password broker composition with optional Admin and Metrics Modules.
use argon2::{Argon2, PasswordHasher, password_hash::SaltString};
use modorch::{ApplicationBuilder, ComponentRef, ComponentSelection, ShutdownOptions};
use modorch_mqtt::*;
use std::{io::Read, path::PathBuf, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1).collect::<Vec<_>>();
    let demo = args.iter().any(|s| s == "--demo");
    let once = args.iter().any(|s| s == "--once");
    args.retain(|s| s != "--demo" && s != "--once");
    let database = if let Some(index) = args.iter().position(|s| s == "--database") {
        if !demo || index + 1 >= args.len() {
            return Err("--database PATH requires --demo".into());
        }
        args.remove(index);
        Some(PathBuf::from(args.remove(index)))
    } else {
        None
    };
    let (tls, password_hash, listener) = if demo {
        if !args.is_empty() {
            return Err(
                "--demo accepts --once and --database PATH, and always binds loopback port 0"
                    .into(),
            );
        }
        let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../crates/modorch-mqtt-core/tests/fixtures");
        // Explicit demo credentials only, generated before starting any application components.
        let salt = SaltString::encode_b64(b"demo-fixture-salt").map_err(|_| "invalid demo salt")?;
        let hash = Argon2::default()
            .hash_password(b"demo-password", &salt)
            .map_err(|_| "invalid demo hash")?
            .to_string();
        (
            TlsConfig {
                certificate_chain: fixtures.join("localhost-cert.pem"),
                private_key: fixtures.join("localhost-key.pem"),
            },
            hash,
            "127.0.0.1:0".parse()?,
        )
    } else {
        if args.len() != 4 {
            return Err("usage: secure-modorch-server CERT_CHAIN_PEM PRIVATE_KEY_PEM PASSWORD_PHC_FILE LISTEN_ADDR | --demo [--database PATH] [--once]".into());
        }
        let mut hash = String::new();
        std::fs::File::open(&args[2])?
            .take(513)
            .read_to_string(&mut hash)?;
        if hash.len() > 512 {
            return Err("password PHC file exceeds 512 bytes".into());
        }
        (
            TlsConfig {
                certificate_chain: args[0].clone().into(),
                private_key: args[1].clone().into(),
            },
            hash.trim().to_string(),
            args[3].parse()?,
        )
    };
    let mut builder = ApplicationBuilder::new("secure-mqtt-host");
    builder.register_module(PasswordAuthFactory::new(PasswordAuthConfig {
        users: vec![UserPolicy {
            username: "device".into(),
            subject: "device-account".into(),
            password_hash,
            client_id_prefixes: vec!["device-".into()],
            publish: vec![PublishRule {
                filter: "devices/#".into(),
                max_qos: Qos::ExactlyOnce,
                allow_retain: true,
            }],
            subscribe: vec!["devices/#".into()],
        }],
        ..Default::default()
    })?)?;
    // The host generates and shares the storage capability, registers the Provider,
    // and explicitly selects it. Core still knows only the API contract.
    let storage = if let Some(database) = database {
        let mut secret = [0; 32];
        getrandom::fill(&mut secret).map_err(|_| "storage capability generation failed")?;
        let access = StoreAccessKey(secret);
        builder.register_module(SqliteStorageFactory::new(SqliteStorageConfig {
            name: "mqtt-storage".into(),
            broker: "mqtt-broker".into(),
            subscription_id: 3000,
            access: access.clone(),
            database,
            max_snapshot_bytes: 4 * 1024 * 1024,
            disk_budget_bytes: 64 * 1024 * 1024,
        })?)?;
        Some(StorageProviderConfig {
            component: "mqtt-storage".into(),
            access,
            max_snapshot_bytes: 4 * 1024 * 1024,
            recovery_page_bytes: 4096,
            timeout: Duration::from_secs(2),
            recovery_timeout: Duration::from_secs(30),
        })
    } else {
        None
    };
    let config = MqttBrokerConfig {
        listener,
        storage,
        tls: Some(tls),
        auth: Some(AuthProviderConfig {
            component: "mqtt-auth".into(),
            timeout: Duration::from_secs(5),
        }),
        admin: Some(AdminControlConfig {
            subscription_id: 4,
            allowed_sources: vec![ComponentRef::module("mqtt-admin")],
        }),
        ..Default::default()
    };
    let durable = config.storage.is_some();
    let summary = format!("{config:?}");
    let mqtt = MqttBrokerBundle::new(config).register(&mut builder)?;
    // This example exposes no operator ingress. A host must explicitly allowlist
    // an authenticated business Handler before it can use administrative operations.
    builder.register_module(AdminFactory::new(Default::default())?)?;
    let metrics = MetricsFactory::new(MetricsConfig {
        interval: Duration::from_millis(100),
        timeout: Duration::from_millis(80),
        ..Default::default()
    })?;
    let observations = metrics.handle();
    builder.register_module(metrics)?;
    let mut selected = ComponentSelection::new();
    for name in ["mqtt-broker", "mqtt-auth", "mqtt-admin", "mqtt-metrics"] {
        selected.enable_module(name);
    }
    if durable {
        selected.enable_module("mqtt-storage");
    }
    let app = builder.selection(selected).build()?.start().await?;
    println!("MQTTS listening on {}", mqtt.handle().local_addr().unwrap());
    if demo {
        println!(
            "Demo configuration: {summary}; SQLite FULL/DELETE, image_limit=4MiB, disk_limit=64MiB when enabled"
        );
    }
    let run_result: Result<(), Box<dyn std::error::Error>> = async {
        tokio::time::timeout(Duration::from_secs(2), async {
            while observations.snapshot().is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        if !observations
            .prometheus()
            .contains("modorch_mqtt_connections ")
        {
            return Err("metrics snapshot missing expected connection counter".into());
        }
        if !once {
            tokio::signal::ctrl_c().await?;
        }
        Ok(())
    }
    .await;
    if let Err(error) = &run_result {
        // Emit the host failure before checked cleanup can close client sockets;
        // preserve the cause even if a diagnostic client aborts the process.
        eprintln!(
            "secure host run failed before checked shutdown: {error}; broker_status={:?}",
            mqtt.handle().status()
        );
    }
    // Once started, always complete checked cleanup, including a metrics or
    // signal error. Keep the shutdown failure visible before returning run errors.
    let shutdown_result = app.shutdown_checked(ShutdownOptions::graceful()).await;
    shutdown_result?;
    run_result?;
    println!("TLS/Auth/Admin/Metrics checked shutdown complete");
    Ok(())
}
