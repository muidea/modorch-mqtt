//! Fake destination drops its session before acknowledging the first publication.
use modorch::*;
use modorch_mqtt_bridge::*;
use modorch_mqtt_core::{MqttBrokerConfig, MqttBrokerFactory};
use std::{net::SocketAddr, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

fn string(s: &str) -> Vec<u8> {
    let mut b = (s.len() as u16).to_be_bytes().to_vec();
    b.extend(s.as_bytes());
    b
}
async fn send(s: &mut TcpStream, h: u8, b: &[u8]) {
    assert!(b.len() < 128);
    s.write_all(&[h, b.len() as u8]).await.unwrap();
    s.write_all(b).await.unwrap();
}
async fn recv(s: &mut TcpStream) -> (u8, Vec<u8>) {
    let h = s.read_u8().await.unwrap();
    let mut n = 0usize;
    for shift in [0, 7, 14, 21] {
        let b = s.read_u8().await.unwrap();
        n |= usize::from(b & 127) << shift;
        if b & 128 == 0 {
            break;
        }
    }
    let mut b = vec![0; n];
    s.read_exact(&mut b).await.unwrap();
    (h, b)
}
fn endpoint(a: SocketAddr) -> BridgeEndpoint {
    BridgeEndpoint {
        host: a.ip().to_string(),
        port: a.port(),
        credentials: None,
    }
}
#[tokio::main]
async fn main() {
    tokio::time::timeout(Duration::from_secs(15), probe())
        .await
        .unwrap();
}
async fn probe() {
    let destination = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote = destination.local_addr().unwrap();
    let source = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = source.local_addr().unwrap();
    drop(source);
    let core = MqttBrokerFactory::new(MqttBrokerConfig {
        name: "audit-core".into(),
        listener: address,
        mqtt5: true,
        ..MqttBrokerConfig::loopback_development()
    })
    .unwrap();
    let broker = core.handle();
    let bridge = BridgeFactory::new(BridgeConfig {
        name: "audit-bridge".into(),
        broker: "audit-core".into(),
        links: vec![BridgeLink {
            source: endpoint(address),
            destination: endpoint(remote),
            source_prefix: "src/".into(),
            destination_prefix: "dst/".into(),
        }],
        max_packet_bytes: 8192,
        session_expiry_seconds: 30,
        max_hops: 4,
        reconnect_delay: Duration::from_millis(50),
    })
    .unwrap();
    let handle = bridge.handle();
    let mut builder = ApplicationBuilder::new("remaining-audit");
    builder.register_module(core).unwrap();
    builder.register_module(bridge).unwrap();
    let mut selection = ComponentSelection::new();
    selection.enable_module("audit-core");
    selection.enable_module("audit-bridge");
    let app = builder
        .selection(selection)
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();
    let (mut first, _) = destination.accept().await.unwrap();
    assert_eq!(recv(&mut first).await.0, 0x10);
    send(&mut first, 0x20, &[0, 0, 0]).await;
    while broker.status().subscriptions != 1 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let mut publisher = TcpStream::connect(address).await.unwrap();
    let mut c = string("MQTT");
    c.extend([5, 2, 0, 0, 0]);
    c.extend(string("writer"));
    send(&mut publisher, 0x10, &c).await;
    assert_eq!(recv(&mut publisher).await.0, 0x20);
    let mut p = string("src/work");
    p.extend([0, 9, 0]);
    p.extend(b"owned");
    send(&mut publisher, 0x32, &p).await;
    assert_eq!(recv(&mut publisher).await, (0x40, vec![0, 9]));
    let (h, body) = recv(&mut first).await;
    assert_eq!(h & 0xf0, 0x30);
    assert!(body.ends_with(b"owned"));
    println!("Destination received initial PUBLISH; closing without PUBACK");
    drop(first);
    let (mut second, _) = destination.accept().await.unwrap();
    assert_eq!(recv(&mut second).await.0, 0x10);
    send(&mut second, 0x20, &[0, 0, 0]).await;
    println!("Destination reconnected with SessionPresent=false");
    let next = tokio::time::timeout(Duration::from_secs(1), recv(&mut second)).await;
    assert!(next.is_err(), "unexpected replay: {next:?}");
    assert_eq!(handle.status().pending, 1);
    assert_eq!(handle.status().forwarded, 0);
    assert_eq!(broker.status().pending_messages, 1);
    println!(
        "REPRODUCED R-02: no replay after session loss; Bridge pending=1, forwarded=0, source pending=1"
    );
    drop(publisher);
    app.shutdown_checked(ShutdownOptions::graceful())
        .await
        .unwrap();
    println!("Application checked shutdown: success");
}
