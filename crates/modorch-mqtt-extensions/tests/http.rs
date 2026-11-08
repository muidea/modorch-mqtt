#![cfg(feature = "http")]
use modorch::prelude::*;
use modorch_mqtt_core::{AdminControlConfig, MqttBrokerConfig, MqttBrokerFactory};
use modorch_mqtt_extensions::*;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
const SECRET: &str = "local-test-token-at-least-32-bytes";
#[tokio::test]
async fn http_authenticates_forwards_admin_exports_metrics_and_drains_slow_headers() {
    let mut builder = ApplicationBuilder::new("http-test");
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
    let metrics = MetricsFactory::new(MetricsConfig {
        interval: Duration::from_millis(100),
        timeout: Duration::from_millis(100),
        ..Default::default()
    })
    .unwrap();
    let admin = AdminConfig {
        allowed_callers: vec![ComponentRef::module("http")],
        ..Default::default()
    };
    let http = HttpFactory::new(
        HttpConfig {
            name: "http".into(),
            admin: "mqtt-admin".into(),
            listener: "127.0.0.1:0".parse().unwrap(),
            subscription_id: 4000,
            bearer_token: SECRET.into(),
            max_connections: 4,
            timeout: Duration::from_secs(2),
        },
        Some(metrics.handle()),
    )
    .unwrap();
    let handle = http.handle();
    assert!(handle.local_addr().is_none());
    builder.register_module(core).unwrap();
    builder
        .register_module(AdminFactory::new(admin).unwrap())
        .unwrap();
    builder.register_module(metrics).unwrap();
    builder.register_module(http).unwrap();
    let mut selection = ComponentSelection::new();
    for id in ["mqtt-broker", "mqtt-admin", "mqtt-metrics", "http"] {
        selection.enable_module(id);
    }
    let app = builder
        .selection(selection)
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();
    let addr = handle.local_addr().unwrap();
    for (path, token, status) in [
        ("/v1/stats", "wrong", 401),
        ("/v1/stats", SECRET, 200),
        ("/v1/clients?limit=2", SECRET, 200),
        ("/v1/clients?limit=100000", SECRET, 400),
        ("/v1/session?client_id=%00", SECRET, 400),
        ("/metrics", SECRET, 200),
    ] {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {token}\r\n\r\n").as_bytes()).await.unwrap();
        let mut bytes = Vec::new();
        tokio::time::timeout(Duration::from_secs(3), stream.read_to_end(&mut bytes))
            .await
            .unwrap()
            .unwrap();
        assert!(
            String::from_utf8(bytes)
                .unwrap()
                .starts_with(&format!("HTTP/1.1 {status} "))
        );
    }
    let mut slow = TcpStream::connect(addr).await.unwrap();
    slow.write_all(b"GET /v1/stats HTTP/1.1\r\n").await.unwrap();
    app.shutdown_checked(ShutdownOptions::graceful())
        .await
        .unwrap();
    assert!(TcpStream::connect(addr).await.is_err());
}
