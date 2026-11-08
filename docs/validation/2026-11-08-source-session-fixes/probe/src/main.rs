//! Public-API regression: source Session loss never ACKs a different publication.
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
fn frame(h: u8, b: &[u8]) -> Vec<u8> {
    let mut f = vec![h];
    let mut n = b.len();
    loop {
        let d = (n % 128) as u8;
        n /= 128;
        f.push(d | if n == 0 { 0 } else { 128 });
        if n == 0 {
            break;
        }
    }
    f.extend(b);
    f
}
async fn send(s: &mut TcpStream, h: u8, b: &[u8]) {
    s.write_all(&frame(h, b)).await.unwrap();
}
async fn recv(s: &mut TcpStream) -> (u8, Vec<u8>) {
    tokio::time::timeout(Duration::from_secs(8), async {
        let h = s.read_u8().await.unwrap();
        let mut n = 0;
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
    })
    .await
    .unwrap()
}
async fn application(s: &mut TcpStream) -> (u8, Vec<u8>) {
    loop {
        let p = recv(s).await;
        if p.0 == 0xc0 {
            send(s, 0xd0, &[]).await;
        } else {
            return p;
        }
    }
}
async fn publish(s: &mut TcpStream, id: u16, payload: &[u8], dup: bool) {
    let mut b = string("data/work");
    b.extend(id.to_be_bytes());
    b.push(0);
    b.extend(payload);
    send(s, if dup { 0x3a } else { 0x32 }, &b).await;
}
fn publication(p: (u8, Vec<u8>)) -> (u16, Vec<u8>) {
    assert_eq!(p.0 >> 4, 3);
    let b = p.1;
    let n = usize::from(u16::from_be_bytes([b[0], b[1]]));
    let id = u16::from_be_bytes([b[2 + n], b[3 + n]]);
    let mut at = 4 + n;
    let mut props = 0;
    for shift in [0, 7, 14, 21] {
        let d = b[at];
        at += 1;
        props |= usize::from(d & 127) << shift;
        if d & 128 == 0 {
            break;
        }
    }
    (id, b[at + props..].to_vec())
}
fn endpoint(a: SocketAddr) -> BridgeEndpoint {
    BridgeEndpoint {
        host: a.ip().to_string(),
        port: a.port(),
        credentials: None,
    }
}
async fn subscribe(s: &mut TcpStream, present: bool) {
    assert_eq!(recv(s).await.0, 0x10);
    send(s, 0x20, &[u8::from(present), 0, 0]).await;
    let (h, b) = recv(s).await;
    assert_eq!(h, 0x82);
    send(s, 0x90, &[b[0], b[1], 0, 1]).await;
}
#[tokio::main]
async fn main() {
    tokio::time::timeout(Duration::from_secs(30), audit())
        .await
        .unwrap();
}
async fn audit() {
    let resumed = std::env::args().nth(1).as_deref() == Some("resumed");
    let source = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let destination = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let core = MqttBrokerFactory::new(MqttBrokerConfig {
        listener: "127.0.0.1:0".parse().unwrap(),
        ..MqttBrokerConfig::loopback_development()
    })
    .unwrap();
    let bridge = BridgeFactory::new(BridgeConfig {
        name: "source-audit".into(),
        broker: "mqtt-broker".into(),
        links: vec![BridgeLink {
            source: endpoint(source.local_addr().unwrap()),
            destination: endpoint(destination.local_addr().unwrap()),
            source_prefix: "data/".into(),
            destination_prefix: "data/".into(),
        }],
        max_packet_bytes: 8192,
        session_expiry_seconds: 30,
        max_hops: 4,
        reconnect_delay: Duration::from_millis(50),
    })
    .unwrap();
    let handle = bridge.handle();
    let mut b = ApplicationBuilder::new("source-loss-audit");
    b.register_module(core).unwrap();
    b.register_module(bridge).unwrap();
    let mut sel = ComponentSelection::new();
    sel.enable_module("mqtt-broker")
        .enable_module("source-audit");
    let app = b.selection(sel).build().unwrap().start().await.unwrap();
    let (s, d) = tokio::join!(source.accept(), destination.accept());
    let mut s = s.unwrap().0;
    let mut d = d.unwrap().0;
    subscribe(&mut s, false).await;
    assert_eq!(recv(&mut d).await.0, 0x10);
    send(&mut d, 0x20, &[0, 0, 0]).await;
    publish(&mut s, 1, b"old-owned", false).await;
    let (old_id, payload) = publication(application(&mut d).await);
    assert_eq!(payload, b"old-owned");
    assert_eq!(handle.status().forwarded, 0);
    println!("old source Session publication id1 is owned; destination has not ACKed");
    drop(s);
    let mut s = source.accept().await.unwrap().0;
    if resumed {
        subscribe(&mut s, true).await;
        publish(&mut s, 1, b"old-owned", true).await;
    } else {
        assert_eq!(recv(&mut s).await.0, 0x10);
        send(&mut s, 0x20, &[0, 0, 0]).await;
    }
    // Resumed Session replays old-owned; new Session waits to subscribe.
    // Neither may ACK the held publication before the destination accepts it.
    assert_eq!(recv(&mut s).await, (0xc0, vec![]));
    send(&mut s, 0xd0, &[]).await;
    send(&mut d, 0x40, &old_id.to_be_bytes()).await;
    if resumed {
        assert_eq!(application(&mut s).await, (0x40, vec![0, 1]));
        println!("PASS: resumed Session duplicate id1 ACKed for the original publication");
    } else {
        let (h, b) = application(&mut s).await;
        assert_eq!(
            h, 0x82,
            "old Session PUBACK must not precede the new subscription"
        );
        send(&mut s, 0x90, &[b[0], b[1], 0, 1]).await;
        publish(&mut s, 1, b"new-session-value", false).await;
        let (id, payload) = publication(application(&mut d).await);
        assert_eq!(payload, b"new-session-value");
        assert_eq!(recv(&mut s).await, (0xc0, vec![]));
        send(&mut s, 0xd0, &[]).await;
        send(&mut d, 0x40, &id.to_be_bytes()).await;
        assert_eq!(application(&mut s).await, (0x40, vec![0, 1]));
        println!("PASS: new Session id1 reaches destination before receiving its own PUBACK");
    }
    publish(&mut s, 2, b"next-barrier", false).await;
    let (id, payload) = publication(application(&mut d).await);
    assert_eq!(payload, b"next-barrier");
    send(&mut d, 0x40, &id.to_be_bytes()).await;
    assert_eq!(application(&mut s).await, (0x40, vec![0, 2]));
    assert_eq!(handle.status().forwarded, if resumed { 2 } else { 3 });
    println!(
        "destination order preserved (new-session-value included when Session lost); resumed={resumed}; {:#?}",
        handle.status()
    );
    app.shutdown_checked(ShutdownOptions::graceful())
        .await
        .unwrap();
    println!("checked shutdown: Passed");
}
