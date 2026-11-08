use modorch::*;
use modorch_mqtt_bridge::*;
use modorch_mqtt_core::{MqttBrokerConfig, MqttBrokerFactory};
use std::{net::SocketAddr, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
struct Client(TcpStream);
fn string(s: &str) -> Vec<u8> {
    let mut b = (s.len() as u16).to_be_bytes().to_vec();
    b.extend(s.as_bytes());
    b
}
fn frame(h: u8, b: &[u8]) -> Vec<u8> {
    let mut f = vec![h];
    f.extend(varint(b.len()));
    f.extend(b);
    f
}
fn varint(mut n: usize) -> Vec<u8> {
    let mut out = vec![];
    loop {
        let b = (n % 128) as u8;
        n /= 128;
        out.push(b | if n == 0 { 0 } else { 128 });
        if n == 0 {
            return out;
        }
    }
}
fn publication(packet: (u8, Vec<u8>)) -> rumqttc::v5::mqttbytes::v5::Publish {
    let bytes = frame(packet.0, &packet.1);
    match rumqttc::v5::mqttbytes::v5::Packet::read(
        &mut bytes::BytesMut::from(bytes.as_slice()),
        Some(8192),
    )
    .unwrap()
    {
        rumqttc::v5::mqttbytes::v5::Packet::Publish(p) => p,
        _ => panic!("expected publication"),
    }
}
impl Client {
    async fn connect(a: SocketAddr, id: &str) -> Self {
        let mut c = Self(TcpStream::connect(a).await.unwrap());
        let mut b = string("MQTT");
        b.extend([5, 2, 0, 0, 0]);
        b.extend(string(id));
        c.0.write_all(&frame(0x10, &b)).await.unwrap();
        assert_eq!(c.recv().await.0, 0x20);
        c
    }
    async fn recv(&mut self) -> (u8, Vec<u8>) {
        tokio::time::timeout(Duration::from_secs(6), async {
            let h = self.0.read_u8().await.unwrap();
            let mut n = 0;
            for shift in [0, 7, 14, 21] {
                let b = self.0.read_u8().await.unwrap();
                n |= usize::from(b & 127) << shift;
                if b & 128 == 0 {
                    break;
                }
            }
            let mut body = vec![0; n];
            self.0.read_exact(&mut body).await.unwrap();
            (h, body)
        })
        .await
        .unwrap()
    }
    async fn subscribe(&mut self) {
        let mut b = vec![0, 1, 0];
        b.extend(string("data/#"));
        b.push(1);
        self.0.write_all(&frame(0x82, &b)).await.unwrap();
        assert_eq!(self.recv().await, (0x90, vec![0, 1, 0, 1]));
    }
    async fn publish(&mut self, payload: &[u8]) {
        let mut b = string("data/work");
        b.extend([0, 9, 0]);
        b.extend(payload);
        self.0.write_all(&frame(0x32, &b)).await.unwrap();
        assert_eq!(self.recv().await, (0x40, vec![0, 9]));
    }
}
fn endpoint(a: SocketAddr) -> BridgeEndpoint {
    BridgeEndpoint {
        host: a.ip().to_string(),
        port: a.port(),
        credentials: None,
    }
}
fn unused() -> SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}
async fn wait(mut f: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(7), async {
        while !f() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn bridge_retains_source_ownership_until_remote_recovers_and_filters_bidirectional_loop() {
    let a = unused();
    let b = unused();
    let bridge = BridgeFactory::new(BridgeConfig {
        name: "bridge".into(),
        broker: "a".into(),
        links: vec![
            BridgeLink {
                source: endpoint(a),
                destination: endpoint(b),
                source_prefix: "data/".into(),
                destination_prefix: "data/".into(),
            },
            BridgeLink {
                source: endpoint(b),
                destination: endpoint(a),
                source_prefix: "data/".into(),
                destination_prefix: "data/".into(),
            },
        ],
        max_packet_bytes: 8192,
        session_expiry_seconds: 30,
        max_hops: 4,
        reconnect_delay: Duration::from_millis(50),
    })
    .unwrap();
    let handle = bridge.handle();
    let core = MqttBrokerFactory::new(MqttBrokerConfig {
        name: "a".into(),
        listener: a,
        mqtt5: true,
        ..MqttBrokerConfig::loopback_development()
    })
    .unwrap();
    let core_handle = core.handle();
    let mut builder = ApplicationBuilder::new("bridge-a");
    builder.register_module(core).unwrap();
    builder.register_module(bridge).unwrap();
    let mut selection = ComponentSelection::new();
    selection.enable_module("a");
    selection.enable_module("bridge");
    let app = builder
        .selection(selection)
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();
    // The remote is offline. A source publication remains in its persistent session.
    wait(|| core_handle.status().subscriptions == 1).await;
    let mut publisher = Client::connect(a, "writer").await;
    publisher.publish(b"queued").await;
    wait(|| handle.status().pending == 1).await;
    assert_eq!(core_handle.status().pending_messages, 1);
    let remote = MqttBrokerFactory::new(MqttBrokerConfig {
        name: "b".into(),
        listener: b,
        mqtt5: true,
        ..MqttBrokerConfig::loopback_development()
    })
    .unwrap();
    let remote_handle = remote.handle();
    let mut builder = ApplicationBuilder::new("bridge-b");
    builder.register_module(remote).unwrap();
    let mut selection = ComponentSelection::new();
    selection.enable_module("b");
    let other = builder
        .selection(selection)
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();
    wait(|| handle.status().forwarded >= 1 && handle.status().pending == 0).await;
    wait(|| remote_handle.status().subscriptions == 1).await;
    let mut reader = Client::connect(b, "reader").await;
    reader.subscribe().await;
    publisher.publish(b"live").await;
    let (h, body) = reader.recv().await;
    assert_eq!(h, 0x32);
    assert!(body.ends_with(b"live"));
    assert!(
        body.windows("modorch.bridge.origin".len())
            .any(|w| w == b"modorch.bridge.origin")
    );
    wait(|| {
        handle.status().loop_filtered >= 1
            && handle.status().forwarded == 2
            && handle.status().pending == 0
    })
    .await;
    let count = handle.status().forwarded;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(handle.status().forwarded, count);
    tokio::time::timeout(
        Duration::from_secs(3),
        app.shutdown_checked(ShutdownOptions::graceful()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(handle.status().pending, 0);
    other
        .shutdown_checked(ShutdownOptions::graceful())
        .await
        .unwrap();
    assert!(TcpStream::connect(a).await.is_err());
}

#[tokio::test]
async fn bridge_rejected_subscription_reconnects_with_backoff_and_recovers() {
    let source = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let source_address = source.local_addr().unwrap();
    let destination_address = unused();
    let fake = tokio::spawn(async move {
        let (socket, _) = source.accept().await.unwrap();
        let mut first = Client(socket);
        assert_eq!(first.recv().await.0, 0x10);
        first.0.write_all(&[0x20, 3, 0, 0, 0]).await.unwrap();
        let (h, b) = first.recv().await;
        assert_eq!(h, 0x82);
        first
            .0
            .write_all(&[0x90, 4, b[0], b[1], 0, 0x87])
            .await
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), first.0.read(&mut [0]))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        let (socket, _) = source.accept().await.unwrap();
        let mut second = Client(socket);
        assert_eq!(second.recv().await.0, 0x10);
        second.0.write_all(&[0x20, 3, 0, 0, 0]).await.unwrap();
        let (h, b) = second.recv().await;
        assert_eq!(h, 0x82);
        second
            .0
            .write_all(&[0x90, 4, b[0], b[1], 0, 1])
            .await
            .unwrap();
        let mut b = string("data/work");
        b.extend([0, 77, 0]);
        b.extend(b"acl-recovered");
        second.0.write_all(&frame(0x32, &b)).await.unwrap();
        assert_eq!(second.recv().await, (0x40, vec![0, 77]));
    });
    let bridge = BridgeFactory::new(BridgeConfig {
        name: "acl-bridge".into(),
        broker: "destination".into(),
        links: vec![BridgeLink {
            source: endpoint(source_address),
            destination: endpoint(destination_address),
            source_prefix: "data/".into(),
            destination_prefix: "mapped/".into(),
        }],
        max_packet_bytes: 8192,
        session_expiry_seconds: 30,
        max_hops: 4,
        reconnect_delay: Duration::from_millis(50),
    })
    .unwrap();
    let handle = bridge.handle();
    let core = MqttBrokerFactory::new(MqttBrokerConfig {
        name: "destination".into(),
        listener: destination_address,
        mqtt5: true,
        ..MqttBrokerConfig::loopback_development()
    })
    .unwrap();
    let mut builder = ApplicationBuilder::new("acl-recovery");
    builder.register_module(core).unwrap();
    builder.register_module(bridge).unwrap();
    let mut selected = ComponentSelection::new();
    selected
        .enable_module("destination")
        .enable_module("acl-bridge");
    let app = builder
        .selection(selected)
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), fake)
        .await
        .unwrap()
        .unwrap();
    wait(|| handle.status().pending == 0 && handle.status().forwarded == 1).await;
    assert!(handle.status().failures >= 1);
    app.shutdown_checked(ShutdownOptions::graceful())
        .await
        .unwrap();
}

async fn start_link(
    remote: SocketAddr,
) -> (
    ApplicationHandle,
    modorch_mqtt_core::BrokerHandle,
    BridgeHandle,
    SocketAddr,
) {
    let address = unused();
    // The listener address must be explicit before constructing the Bridge endpoints.
    let core = MqttBrokerFactory::new(MqttBrokerConfig {
        name: "source".into(),
        listener: address,
        mqtt5: true,
        max_packet_bytes: 8192,
        ..MqttBrokerConfig::loopback_development()
    })
    .unwrap();
    let broker = core.handle();
    let bridge = BridgeFactory::new(BridgeConfig {
        name: "audit-bridge".into(),
        broker: "source".into(),
        links: vec![BridgeLink {
            source: endpoint(address),
            destination: endpoint(remote),
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
    let mut builder = ApplicationBuilder::new("bridge-regression");
    builder.register_module(core).unwrap();
    builder.register_module(bridge).unwrap();
    let mut selection = ComponentSelection::new();
    selection
        .enable_module("source")
        .enable_module("audit-bridge");
    let app = builder
        .selection(selection)
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();
    wait(|| broker.status().subscriptions == 1).await;
    (app, broker, handle, address)
}

#[tokio::test]
async fn bridge_target_session_loss_resubmits_and_resumed_session_replays_one_exchange() {
    for (present, invalid_ack) in [(false, false), (true, false), (false, true)] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let remote = listener.local_addr().unwrap();
        let (replayed, replay) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel();
        let fake = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut first = Client(socket);
            assert_eq!(first.recv().await.0, 0x10);
            first.0.write_all(&[0x20, 3, 0, 0, 0]).await.unwrap();
            let original = publication(first.recv().await);
            assert!(!original.dup);
            if invalid_ack {
                first
                    .0
                    .write_all(&frame(0x40, &(original.pkid + 1).to_be_bytes()))
                    .await
                    .unwrap();
                assert_eq!(first.0.read(&mut [0]).await.unwrap(), 0);
            }
            drop(first); // Lose the PUBACK, with or without retaining the remote Session.
            let (socket, _) = listener.accept().await.unwrap();
            let mut second = Client(socket);
            assert_eq!(second.recv().await.0, 0x10);
            second
                .0
                .write_all(&[0x20, 3, u8::from(present), 0, 0])
                .await
                .unwrap();
            let retry = publication(second.recv().await);
            assert_eq!(retry.payload, original.payload);
            assert_eq!(retry.properties, original.properties);
            assert_eq!(retry.dup, present);
            if present {
                assert_eq!(retry.pkid, original.pkid);
            }
            replayed.send(()).unwrap();
            released.await.unwrap();
            second
                .0
                .write_all(&frame(0x40, &retry.pkid.to_be_bytes()))
                .await
                .unwrap();
            // Exactly one owned retry: the next publication must be the next source message.
            let next = publication(second.recv().await);
            assert_eq!(next.payload, b"next"[..]);
            second
                .0
                .write_all(&frame(0x40, &next.pkid.to_be_bytes()))
                .await
                .unwrap();
            // ACK immediately followed by EOF must still release source ownership.
        });
        let (app, broker, handle, address) = start_link(remote).await;
        let mut publisher = Client::connect(address, "writer").await;
        publisher.publish(b"owned").await;
        tokio::time::timeout(Duration::from_secs(5), replay)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(handle.status().pending, 1);
        assert_eq!(handle.status().forwarded, 0);
        assert_eq!(broker.status().pending_messages, 1);
        release.send(()).unwrap();
        wait(|| {
            handle.status().forwarded == 1
                && handle.status().pending == 0
                && broker.status().pending_messages == 0
        })
        .await;
        publisher.publish(b"next").await;
        wait(|| {
            handle.status().forwarded == 2
                && handle.status().pending == 0
                && broker.status().pending_messages == 0
        })
        .await;
        fake.await.unwrap();
        app.shutdown_checked(ShutdownOptions::graceful())
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn bridge_preserves_128_ordinary_properties_plus_reserved_metadata_end_to_end() {
    let core = MqttBrokerFactory::new(MqttBrokerConfig {
        name: "remote".into(),
        listener: "127.0.0.1:0".parse().unwrap(),
        mqtt5: true,
        max_packet_bytes: 8192,
        ..MqttBrokerConfig::loopback_development()
    })
    .unwrap();
    let remote = core.handle();
    let mut builder = ApplicationBuilder::new("property-destination");
    builder.register_module(core).unwrap();
    let mut selection = ComponentSelection::new();
    selection.enable_module("remote");
    let destination = builder
        .selection(selection)
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();
    let address = remote.local_addr().unwrap();
    let mut reader = Client::connect(address, "reader").await;
    reader.subscribe().await;
    let (app, broker, handle, address) = start_link(address).await;
    let mut writer = Client::connect(address, "writer").await;
    let mut forwarded = 0;
    for count in [126, 127, 128] {
        for existing in [false, true] {
            let mut props = vec![];
            for _ in 0..count {
                props.push(0x26);
                props.extend(string("k"));
                props.extend(string("v"));
            }
            if existing {
                for (k, v) in [
                    ("modorch.bridge.origin", "upstream"),
                    ("modorch.bridge.hops", "1"),
                ] {
                    props.push(0x26);
                    props.extend(string(k));
                    props.extend(string(v));
                }
            }
            let mut body = string("data/work");
            body.extend([0, 9]);
            body.extend(varint(props.len()));
            body.extend(props);
            body.extend(b"properties");
            writer.0.write_all(&frame(0x32, &body)).await.unwrap();
            assert_eq!(writer.recv().await, (0x40, vec![0, 9]));
            let received = publication(reader.recv().await);
            let props = received.properties.unwrap().user_properties;
            assert_eq!(props.len(), count + 2);
            assert!(props[..count].iter().all(|(k, v)| k == "k" && v == "v"));
            assert_eq!(
                props[count],
                (
                    "modorch.bridge.origin".into(),
                    if existing { "upstream" } else { "audit-bridge" }.into()
                )
            );
            assert_eq!(
                props[count + 1],
                (
                    "modorch.bridge.hops".into(),
                    if existing { "2" } else { "1" }.into()
                )
            );
            reader
                .0
                .write_all(&frame(0x40, &received.pkid.to_be_bytes()))
                .await
                .unwrap();
            forwarded += 1;
            wait(|| {
                handle.status().forwarded == forwarded
                    && handle.status().pending == 0
                    && broker.status().pending_messages == 0
            })
            .await;
        }
    }
    assert_eq!(handle.status().failures, 0);
    app.shutdown_checked(ShutdownOptions::graceful())
        .await
        .unwrap();
    destination
        .shutdown_checked(ShutdownOptions::graceful())
        .await
        .unwrap();
}

// Both endpoints are controlled, but the link is the actual managed Bridge.
async fn remote_link(
    source: SocketAddr,
    destination: SocketAddr,
) -> (ApplicationHandle, BridgeHandle) {
    let core = MqttBrokerFactory::new(MqttBrokerConfig {
        name: "dependency".into(),
        listener: "127.0.0.1:0".parse().unwrap(),
        ..MqttBrokerConfig::loopback_development()
    })
    .unwrap();
    let bridge = BridgeFactory::new(BridgeConfig {
        name: "source-fencing".into(),
        broker: "dependency".into(),
        links: vec![BridgeLink {
            source: endpoint(source),
            destination: endpoint(destination),
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
    let mut builder = ApplicationBuilder::new("source-session-regression");
    builder.register_module(core).unwrap();
    builder.register_module(bridge).unwrap();
    let mut selection = ComponentSelection::new();
    selection
        .enable_module("dependency")
        .enable_module("source-fencing");
    let app = builder
        .selection(selection)
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();
    (app, handle)
}
impl Client {
    async fn application(&mut self) -> (u8, Vec<u8>) {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let packet = self.recv().await;
                if packet.0 != 0xc0 {
                    return packet;
                }
                self.0.write_all(&[0xd0, 0]).await.unwrap();
            }
        })
        .await
        .unwrap()
    }
    async fn server_connack(&mut self, present: bool) {
        assert_eq!(self.recv().await.0, 0x10);
        self.0
            .write_all(&[0x20, 3, u8::from(present), 0, 0])
            .await
            .unwrap();
    }
    async fn server_suback(&mut self) {
        let (h, b) = self.application().await;
        // In particular, this must not be the old Session's queued PUBACK.
        assert_eq!(h, 0x82);
        self.0
            .write_all(&frame(0x90, &[b[0], b[1], 0, 1]))
            .await
            .unwrap();
    }
    async fn server_publish(&mut self, id: u16, payload: &[u8], dup: bool) {
        let mut body = string("data/work");
        body.extend(id.to_be_bytes());
        body.push(0);
        body.extend(payload);
        self.0
            .write_all(&frame(if dup { 0x3a } else { 0x32 }, &body))
            .await
            .unwrap();
    }
    async fn only_ping(&mut self) {
        // A real outgoing Keep Alive proves polling progressed. Any early ACK
        // or subscription is an error, not something hidden by a short timeout.
        assert_eq!(self.recv().await, (0xc0, vec![]));
        self.0.write_all(&[0xd0, 0]).await.unwrap();
    }
}

#[tokio::test]
async fn bridge_source_session_loss_fences_old_ack_and_preserves_reused_and_distinct_ids() {
    tokio::time::timeout(Duration::from_secs(60), async {
        for new_id in [1u16, 7] {
            let source = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let destination = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let (app, handle) = remote_link(
                source.local_addr().unwrap(),
                destination.local_addr().unwrap(),
            )
            .await;
            let (s, d) = tokio::join!(source.accept(), destination.accept());
            let mut s = Client(s.unwrap().0);
            let mut d = Client(d.unwrap().0);
            s.server_connack(false).await;
            s.server_suback().await;
            d.server_connack(false).await;
            s.server_publish(1, b"old-owned", false).await;
            let mut old = publication(d.application().await);
            assert_eq!(old.payload, b"old-owned"[..]);
            drop(s);

            let mut s = Client(source.accept().await.unwrap().0);
            s.server_connack(false).await;
            // New Session has no old subscription. Keep it alive without creating
            // a second owned transfer or sending an ACK for the abandoned Session.
            s.only_ping().await;
            assert_eq!(handle.status().pending, 1);
            assert_eq!(handle.status().forwarded, 0);
            if new_id == 1 {
                // Source and destination can both lose their Sessions. Keep the
                // original owned work until the destination accepts its retry.
                drop(d);
                d = Client(destination.accept().await.unwrap().0);
                d.server_connack(false).await;
                let retry = publication(d.application().await);
                assert_eq!(retry.payload, old.payload);
                old = retry;
            }
            if new_id == 7 {
                // Service the destination's concurrent Keep Alive while the source
                // remains deliberately paused across two connections.
                d.only_ping().await;
                // A second loss while the same target work is pending must fence
                // every previous source Session, not just the immediately prior ID.
                drop(s);
                s = Client(source.accept().await.unwrap().0);
                s.server_connack(false).await;
                s.only_ping().await;
            }
            d.0.write_all(&frame(0x40, &old.pkid.to_be_bytes()))
                .await
                .unwrap();
            s.server_suback().await;
            s.server_publish(new_id, b"new-session-value", false).await;
            let new = publication(d.application().await);
            assert_eq!(new.payload, b"new-session-value"[..]);
            if new_id == 1 {
                // The old ID is reused, but no PUBACK may arrive before this new
                // publication's destination ACK, even after the old work completed.
                s.only_ping().await;
            }
            d.0.write_all(&frame(0x40, &new.pkid.to_be_bytes()))
                .await
                .unwrap();
            assert_eq!(s.application().await, (0x40, new_id.to_be_bytes().to_vec()));
            s.server_publish(8, b"next-barrier", false).await;
            let next = publication(d.application().await);
            assert_eq!(next.payload, b"next-barrier"[..]);
            d.0.write_all(&frame(0x40, &next.pkid.to_be_bytes()))
                .await
                .unwrap();
            assert_eq!(s.application().await, (0x40, vec![0, 8]));
            wait(|| handle.status().pending == 0).await;
            assert_eq!(handle.status().forwarded, 3);
            assert_eq!(handle.status().dropped_qos0, 0);
            app.shutdown_checked(ShutdownOptions::graceful())
                .await
                .unwrap();
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn bridge_source_resume_and_destination_ack_during_source_reconnect_keep_ack_identity() {
    tokio::time::timeout(Duration::from_secs(60), async {
        for present in [true, false] {
            let source = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let destination = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let (app, handle) = remote_link(
                source.local_addr().unwrap(),
                destination.local_addr().unwrap(),
            )
            .await;
            let (s, d) = tokio::join!(source.accept(), destination.accept());
            let mut s = Client(s.unwrap().0);
            let mut d = Client(d.unwrap().0);
            s.server_connack(false).await;
            s.server_suback().await;
            d.server_connack(false).await;
            s.server_publish(1, b"old-owned", false).await;
            let old = publication(d.application().await);
            drop(s);
            let mut s = Client(source.accept().await.unwrap().0);
            if present {
                s.server_connack(true).await;
                s.server_suback().await;
                s.server_publish(1, b"old-owned", true).await;
                s.only_ping().await;
                assert_eq!(handle.status().forwarded, 0);
                d.0.write_all(&frame(0x40, &old.pkid.to_be_bytes()))
                    .await
                    .unwrap();
                assert_eq!(s.application().await, (0x40, vec![0, 1]));
            } else {
                // Destination completes while source CONNECT awaits its CONNACK.
                // Do not queue an ACK into the client's reconnect request channel.
                d.0.write_all(&frame(0x40, &old.pkid.to_be_bytes()))
                    .await
                    .unwrap();
                wait(|| handle.status().forwarded == 1).await;
                assert_eq!(handle.status().pending, 1);
                s.server_connack(false).await;
                s.server_suback().await;
            }
            let id = if present { 2 } else { 1 };
            s.server_publish(id, b"next-barrier", false).await;
            let next = publication(d.application().await);
            assert_eq!(next.payload, b"next-barrier"[..]);
            d.0.write_all(&frame(0x40, &next.pkid.to_be_bytes()))
                .await
                .unwrap();
            assert_eq!(s.application().await, (0x40, id.to_be_bytes().to_vec()));
            wait(|| handle.status().pending == 0).await;
            assert_eq!(handle.status().forwarded, 2);
            app.shutdown_checked(ShutdownOptions::graceful())
                .await
                .unwrap();
        }
    })
    .await
    .unwrap();
}
