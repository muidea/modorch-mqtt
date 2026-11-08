//! Public-API regressions for cross-version rejection and valid packet recovery.
use modorch::*;
use modorch_mqtt_api::StoreAccessKey;
use modorch_mqtt_core::{BrokerHandle, MqttBrokerConfig, MqttBrokerFactory, StorageProviderConfig};
use modorch_mqtt_storage::{SqliteStorageConfig, SqliteStorageFactory};
use std::{path::Path, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

fn string(s: &str) -> Vec<u8> {
    let mut b = (s.len() as u16).to_be_bytes().to_vec();
    b.extend(s.as_bytes());
    b
}
async fn send(s: &mut TcpStream, h: u8, b: &[u8]) {
    let mut header = vec![h];
    let mut remaining = b.len();
    loop {
        let digit = (remaining % 128) as u8;
        remaining /= 128;
        header.push(digit | if remaining != 0 { 128 } else { 0 });
        if remaining == 0 {
            break;
        }
    }
    s.write_all(&header).await.unwrap();
    s.write_all(b).await.unwrap();
}
async fn recv(s: &mut TcpStream) -> (u8, Vec<u8>) {
    let h = s.read_u8().await.unwrap();
    let mut n = 0;
    for shift in [0, 7, 14, 21] {
        let b = s.read_u8().await.unwrap();
        n |= usize::from(b & 127) << shift;
        if b & 128 == 0 {
            break;
        }
    }
    let mut body = vec![0; n];
    s.read_exact(&mut body).await.unwrap();
    (h, body)
}
async fn start(path: &Path) -> (Result<ApplicationHandle, StartupError>, BrokerHandle) {
    let access = StoreAccessKey([7; 32]);
    let core = MqttBrokerFactory::new(MqttBrokerConfig {
        mqtt5: true,
        listener: "127.0.0.1:0".parse().unwrap(),
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
    let mut builder = ApplicationBuilder::new("mixed-version-audit");
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
    let mut selection = ComponentSelection::new();
    selection
        .enable_module("store")
        .enable_module("mqtt-broker");
    (
        builder.selection(selection).build().unwrap().start().await,
        broker,
    )
}

#[tokio::main]
async fn main() {
    tokio::time::timeout(Duration::from_secs(30), async {
        for aliased in [false, true] {
            audit(aliased).await;
        }
    })
    .await
    .unwrap();
}
async fn audit(aliased: bool) {
    let path = std::env::temp_dir().join(format!(
        "mqtt-alias-audit-{}-{aliased}.sqlite",
        std::process::id()
    ));
    assert!(!path.exists());
    let (app, broker) = start(&path).await;
    let app = app.unwrap();
    let mut c = TcpStream::connect(broker.local_addr().unwrap())
        .await
        .unwrap();
    let mut connect = string("MQTT");
    connect.extend([5, 2, 0, 0, 0]);
    connect.extend(string("alias-source"));
    send(&mut c, 0x10, &connect).await;
    let (h, b) = recv(&mut c).await;
    assert_eq!(h, 0x20);
    assert_eq!(b[1], 0);
    let topic = "t".repeat(1000);
    if aliased {
        let mut bind = string(&topic);
        bind.extend([0, 8, 3, 0x23, 0, 1, b'b']);
        send(&mut c, 0x32, &bind).await;
        assert_eq!(recv(&mut c).await, (0x40, vec![0, 8]));
    }
    let payload = if aliased { 8180 } else { 7180 };
    let mut publish = string(if aliased { "" } else { &topic });
    publish.extend([0, 9]);
    if aliased {
        publish.extend([3, 0x23, 0, 1]);
    } else {
        publish.push(0);
    }
    publish.extend(vec![b'x'; payload]);
    assert!(publish.len() + 3 <= 8192);
    send(&mut c, 0x33, &publish).await;
    assert_eq!(recv(&mut c).await, (0x40, vec![0, 9]));
    assert_eq!(broker.status().retained, 1);
    println!(
        "Accepted MQTT5 retained PUBLISH: alias={aliased}, wire={}, expanded_topic={}, payload={payload}",
        publish.len() + 3,
        topic.len()
    );
    send(&mut c, 0xe0, &[]).await;
    assert_eq!(c.read(&mut [0]).await.unwrap(), 0);
    drop(c);
    app.shutdown_checked(ShutdownOptions::graceful())
        .await
        .unwrap();
    drop(app);
    let (restarted, handle) = start(&path).await;
    match restarted {
        Ok(app) => {
            app.shutdown_checked(ShutdownOptions::graceful())
                .await
                .unwrap();
            assert!(!aliased, "expected expanded-alias snapshot rejection");
            println!("CONTROL: full-topic message restores successfully");
        }
        Err(error) => {
            assert!(aliased, "{}", error.source);
            assert!(
                error
                    .source
                    .to_string()
                    .contains("invalid persistent snapshot")
            );
            assert!(error.cleanup.is_ok());
            if let Some(address) = handle.local_addr() {
                assert!(TcpStream::connect(address).await.is_err());
            }
            println!(
                "REPRODUCED R-08: confirmed alias publication prevents SQLite restart: {}",
                error.source
            );
        }
    }
    std::fs::remove_file(path).unwrap();
}
