//! Literal MQTT5 wire fixtures independent of the new codec's encoder.
use super::*;

#[tokio::test]
async fn mqtt5_online_zero_expiry_takeover_preserves_subscriptions_and_both_qos2_directions() {
    let (app, broker) = start(MqttBrokerConfig {
        mqtt5: true,
        ..config()
    })
    .await;
    let addr = broker.local_addr().unwrap();
    let (mut old, _) = connect(addr, "zero", true, 0, &[], None).await;
    sub(&mut old, &[("work/#", 2)], Some(17)).await;
    let (mut source, _) = connect(addr, "writer", true, 0, &[], None).await;
    publish(&mut source, "work/one", b"qos1", 1, &[], false).await;
    let (h, b) = old.recv().await;
    let first = decoded(h, &b);
    publish(&mut source, "work/two", b"qos2", 2, &[], false).await;
    let (h, b) = old.recv().await;
    let second = decoded(h, &b);
    old.send(codec::frame(0x50, &second.pkid.to_be_bytes()))
        .await;
    assert_eq!(old.recv().await, (0x62, second.pkid.to_be_bytes().to_vec()));
    // A separate inbound QoS2 ownership record must survive the takeover too.
    let mut inbound = string("incoming/value");
    inbound.extend([0, 42, 0, b'i']);
    old.send(codec::frame(0x34, &inbound)).await;
    assert_eq!(old.recv().await, (0x50, vec![0, 42]));
    let (mut resumed, present) = connect(addr, "zero", false, 0, &[], None).await;
    assert!(present);
    assert_eq!(
        resumed.recv().await,
        (0x62, second.pkid.to_be_bytes().to_vec())
    );
    let (h, b) = resumed.recv().await;
    let replay = decoded(h, &b);
    assert!(replay.dup);
    assert_eq!(replay.pkid, first.pkid);
    assert_eq!(replay.payload, first.payload);
    assert_eq!(replay.properties.unwrap().subscription_identifiers, [17]);
    assert_eq!(broker.status().incoming_qos2, 1);
    resumed
        .send(codec::frame(0x40, &first.pkid.to_be_bytes()))
        .await;
    resumed
        .send(codec::frame(0x70, &second.pkid.to_be_bytes()))
        .await;
    resumed.send(codec::frame(0x62, &[0, 42])).await;
    assert_eq!(resumed.recv().await, (0x70, vec![0, 42]));
    publish(&mut source, "work/next", b"next", 1, &[], false).await;
    let (h, b) = resumed.recv().await;
    let p = decoded(h, &b);
    assert_eq!(p.payload, b"next"[..]);
    resumed
        .send(codec::frame(0x40, &p.pkid.to_be_bytes()))
        .await;
    resumed.disconnect().await;
    // Zero expiry is still enforced after disconnection.
    tokio::time::timeout(Duration::from_secs(2), async {
        while broker.status().subscriptions != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let (mut fresh, present) = connect(addr, "zero", false, 30, &[], None).await;
    assert!(!present);
    sub(&mut fresh, &[("work/#", 1)], None).await;
    // Clean Start explicitly discards even an online Session.
    let (mut clean, present) = connect(addr, "zero", true, 0, &[], None).await;
    assert!(!present);
    publish(&mut source, "work/gone", b"gone", 1, &[], false).await;
    clean.send(vec![0xc0, 0]).await;
    assert_eq!(clean.recv().await, (0xd0, vec![]));
    stop(&app).await;
}

#[tokio::test]
async fn mqtt5_user_property_limits_reserve_two_unique_bridge_slots_and_preserve_byte_limits() {
    let (app, broker) = start(MqttBrokerConfig {
        mqtt5: true,
        ..config()
    })
    .await;
    let addr = broker.local_addr().unwrap();
    let ordinary = || {
        (0..128)
            .map(|_| ("k".to_owned(), "v".to_owned()))
            .collect::<Vec<_>>()
    };
    let mut valid = MqttProperties {
        user_properties: ordinary(),
        ..Default::default()
    };
    valid.user_properties.extend([
        (BRIDGE_ORIGIN_PROPERTY.into(), "other".into()),
        (BRIDGE_HOPS_PROPERTY.into(), "1".into()),
    ]);
    let (mut writer, _) = connect(addr, "properties", true, 0, &[], None).await;
    let message = MqttMessage {
        topic: "props/work".into(),
        payload: b"ok".as_slice().into(),
        qos: Qos::AtLeastOnce,
        retain: false,
        properties: valid.clone(),
    };
    writer
        .send(crate::mqtt5::publish(
            &message,
            message.qos,
            false,
            Some(7),
            false,
        ))
        .await;
    assert_eq!(writer.recv().await, (0x40, vec![0, 7]));
    for (id, bad) in [
        ("ordinary-overflow", {
            let mut p = valid.clone();
            p.user_properties.push(("extra".into(), "x".into()));
            p
        }),
        ("duplicate-origin", {
            let mut p = valid.clone();
            p.user_properties
                .push((BRIDGE_ORIGIN_PROPERTY.into(), "again".into()));
            p
        }),
        ("duplicate-hops", {
            let mut p = valid.clone();
            p.user_properties
                .push((BRIDGE_HOPS_PROPERTY.into(), "2".into()));
            p
        }),
        ("byte-overflow", {
            let mut p = valid.clone();
            p.content_type = Some("x".repeat(8192));
            p
        }),
    ] {
        let (mut c, _) = connect(addr, id, true, 0, &[], None).await;
        let m = MqttMessage {
            properties: bad,
            ..message.clone()
        };
        c.send(crate::mqtt5::publish(&m, m.qos, false, Some(8), false))
            .await;
        assert!(matches!(c.0.read(&mut [0]).await, Ok(0) | Err(_)));
    }
    writer.send(vec![0xc0, 0]).await;
    assert_eq!(writer.recv().await, (0xd0, vec![]));
    stop(&app).await;
}
pub(super) async fn connect(
    addr: SocketAddr,
    id: &str,
    clean: bool,
    expiry: u32,
    extra: &[u8],
    will: Option<(u32, u32)>,
) -> (Client, bool) {
    let mut c = Client(TcpStream::connect(addr).await.unwrap());
    let mut b = string("MQTT");
    b.extend([
        5,
        if will.is_some() { 0x0c } else { 0 } | (u8::from(clean) * 2),
        0,
        0,
    ]);
    let mut p = vec![0x11];
    p.extend(expiry.to_be_bytes());
    p.extend(extra);
    b.push(p.len() as u8);
    b.extend(p);
    b.extend(string(id));
    if let Some((delay, expiry)) = will {
        b.push(10);
        b.push(0x18);
        b.extend(delay.to_be_bytes());
        b.push(2);
        b.extend(expiry.to_be_bytes());
        b.extend(string("v5/will"));
        b.extend([0, 1, b'w']);
    }
    c.send(codec::frame(0x10, &b)).await;
    let (h, b) = c.recv().await;
    assert_eq!(h, 0x20);
    assert_eq!(b[1], 0);
    (c, b[0] == 1)
}
pub(super) async fn sub(c: &mut Client, filters: &[(&str, u8)], id: Option<u8>) {
    let mut b = vec![0, 1];
    if let Some(id) = id {
        b.extend([2, 0x0b, id]);
    } else {
        b.push(0);
    }
    for (f, o) in filters {
        b.extend(string(f));
        b.push(*o);
    }
    c.send(codec::frame(0x82, &b)).await;
    let (h, b) = c.recv().await;
    assert_eq!(h, 0x90);
    assert_eq!(&b[..3], &[0, 1, 0]);
    assert_eq!(b.len(), 3 + filters.len());
    assert!(b[3..].iter().all(|c| *c <= 2));
}
pub(super) async fn publish(
    c: &mut Client,
    topic: &str,
    payload: &[u8],
    qos: u8,
    props: &[u8],
    retain: bool,
) {
    let mut b = string(topic);
    if qos != 0 {
        b.extend([0, 9]);
    }
    b.push(props.len() as u8);
    b.extend(props);
    b.extend(payload);
    c.send(codec::frame(0x30 | (qos << 1) | u8::from(retain), &b))
        .await;
    if qos != 0 {
        assert_eq!(
            c.recv().await,
            (if qos == 2 { 0x50 } else { 0x40 }, vec![0, 9])
        );
        if qos == 2 {
            c.send(vec![0x62, 2, 0, 9]).await;
            assert_eq!(c.recv().await, (0x70, vec![0, 9]));
        }
    }
}
pub(super) fn decoded(h: u8, b: &[u8]) -> rumqttc::v5::mqttbytes::v5::Publish {
    let f = codec::frame(h, b);
    match rumqttc::v5::mqttbytes::v5::Packet::read(
        &mut bytes::BytesMut::from(f.as_slice()),
        Some(8192),
    )
    .unwrap()
    {
        rumqttc::v5::mqttbytes::v5::Packet::Publish(p) => p,
        _ => panic!("PUBLISH expected"),
    }
}
#[tokio::test]
async fn mqtt5_alias_metadata_subscription_options_window_unsubscribe_and_mixed311() {
    let (app, broker) = start(MqttBrokerConfig {
        mqtt5: true,
        ..config()
    })
    .await;
    let addr = broker.local_addr().unwrap();
    let (mut sink, _) = connect(addr, "sink", true, 30, &[0x21, 0, 1], None).await;
    sub(&mut sink, &[("v5/#", 0x09)], Some(7)).await; // QoS1 + Retain As Published
    let (mut source, _) = connect(addr, "source", true, 0, &[], None).await;
    let props = [0x23, 0, 1, 1, 1, 0x26, 0, 1, b'k', 0, 1, b'v'];
    publish(&mut source, "v5/a", b"one", 2, &props, true).await;
    let (h, b) = sink.recv().await;
    assert_eq!(h & 1, 1);
    let p = decoded(h, &b);
    assert_eq!(p.payload, b"one"[..]);
    let metadata = p.properties.unwrap();
    assert_eq!(metadata.subscription_identifiers, [7]);
    assert_eq!(metadata.user_properties, [("k".into(), "v".into())]);
    publish(&mut source, "", b"two", 1, &[0x23, 0, 1], false).await;
    sink.send(vec![0xc0, 0]).await;
    assert_eq!(sink.recv().await, (0xd0, vec![])); // window blocks second delivery
    sink.send(codec::frame(0x40, &p.pkid.to_be_bytes())).await;
    let (h, b) = sink.recv().await;
    let p = decoded(h, &b);
    assert_eq!(p.payload, b"two"[..]);
    sink.send(codec::frame(0x40, &p.pkid.to_be_bytes())).await;
    // No Local changes the existing subscription without re-sending retained values.
    sub(&mut sink, &[("v5/#", 0x25)], Some(8)).await;
    publish(&mut sink, "v5/self", b"self", 1, &[], false).await;
    sink.send(vec![0xc0, 0]).await;
    assert_eq!(sink.recv().await, (0xd0, vec![]));
    let mut u = vec![0, 2, 0];
    u.extend(string("v5/#"));
    u.extend(string("missing"));
    sink.send(codec::frame(0xa2, &u)).await;
    assert_eq!(sink.recv().await, (0xb0, vec![0, 2, 0, 0, 0x11]));
    let (mut old, _) = Client::connect(addr, "legacy", true).await;
    assert_eq!(old.subscribe(&[("v5/#", 1)]).await, [1]);
    publish(&mut source, "v5/legacy", b"ok", 1, &[], false).await;
    // Retained replay then live publication both use MQTT3.1.1 layout.
    assert_eq!(old.recv().await.0, 0x33);
    let (_, body) = old.recv().await;
    assert!(body.ends_with(b"ok"));
    stop(&app).await;
}
#[tokio::test]
async fn mqtt5_delayed_will_cancel_resume_session_expiry_and_message_expiry() {
    let (app, broker) = start(MqttBrokerConfig {
        mqtt5: true,
        ..config()
    })
    .await;
    let addr = broker.local_addr().unwrap();
    let (mut observer, _) = connect(addr, "watch", true, 0, &[], None).await;
    sub(&mut observer, &[("v5/#", 0)], None).await;
    let (c, _) = connect(addr, "delayed", true, 3, &[], Some((1, 1))).await;
    drop(c);
    tokio::time::sleep(Duration::from_millis(150)).await;
    let (c, present) = connect(addr, "delayed", false, 3, &[], None).await;
    assert!(present);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    observer.send(vec![0xc0, 0]).await;
    assert_eq!(observer.recv().await, (0xd0, vec![]));
    drop(c);
    let (c, _) = connect(addr, "will", true, 1, &[], Some((5, 1))).await;
    drop(c);
    let (h, b) = observer.recv().await;
    assert_eq!(decoded(h, &b).payload, b"w"[..]);
    let (mut c, _) = connect(addr, "expiry", true, 1, &[], None).await;
    sub(&mut c, &[("offline/#", 1)], None).await;
    c.send(vec![0xe0, 0]).await;
    drop(c);
    tokio::time::sleep(Duration::from_millis(1150)).await;
    let (_c, present) = connect(addr, "expiry", false, 1, &[], None).await;
    assert!(!present);
    let (mut p, _) = connect(addr, "publisher", true, 0, &[], None).await;
    publish(&mut p, "v5/expired", b"gone", 1, &[2, 0, 0, 0, 0], true).await;
    observer.send(vec![0xc0, 0]).await;
    assert_eq!(observer.recv().await, (0xd0, vec![]));
    assert_eq!(broker.status().retained, 0);
    stop(&app).await;
}
#[tokio::test]
async fn mqtt5_shared_groups_distribute_and_invalid_properties_close_only_offender() {
    let (app, broker) = start(MqttBrokerConfig {
        mqtt5: true,
        ..config()
    })
    .await;
    let addr = broker.local_addr().unwrap();
    let (mut a, _) = connect(addr, "a", true, 0, &[], None).await;
    let (mut b, _) = connect(addr, "b", true, 0, &[], None).await;
    sub(&mut a, &[("$share/g/jobs/#", 1)], None).await;
    sub(&mut b, &[("$share/g/jobs/#", 1)], None).await;
    let (mut p, _) = connect(addr, "p", true, 0, &[], None).await;
    publish(&mut p, "jobs/1", b"first", 1, &[], false).await;
    publish(&mut p, "jobs/2", b"second", 1, &[], false).await;
    let (h, x) = a.recv().await;
    let (h2, y) = b.recv().await;
    let x = decoded(h, &x);
    let y = decoded(h2, &y);
    assert_ne!(x.payload, y.payload);
    let mut invalid = string("jobs/bad");
    invalid.extend([0, 1, 6, 0x23, 0, 1, 0x23, 0, 2, b'x']);
    p.send(codec::frame(0x32, &invalid)).await;
    assert_eq!(p.0.read(&mut [0]).await.unwrap(), 0);
    a.send(vec![0xc0, 0]).await;
    assert_eq!(a.recv().await, (0xd0, vec![]));
    stop(&app).await;
}

#[tokio::test]
async fn mqtt5_negotiated_packet_limit_discards_oversized_delivery_without_blocking_session() {
    let (app, broker) = start(MqttBrokerConfig {
        mqtt5: true,
        ..config()
    })
    .await;
    let addr = broker.local_addr().unwrap();
    let (mut sink, _) = connect(addr, "small", true, 0, &[0x27, 0, 0, 0, 40], None).await;
    sub(&mut sink, &[("size/#", 1)], None).await;
    let (mut source, _) = connect(addr, "large", true, 0, &[], None).await;
    publish(&mut source, "size/value", &[b'x'; 128], 1, &[], false).await;
    sink.send(vec![0xc0, 0]).await;
    assert_eq!(sink.recv().await, (0xd0, vec![]));
    assert_eq!(broker.status().dropped_oversized, 1);
    publish(&mut source, "size/value", b"ok", 1, &[], false).await;
    let (h, b) = sink.recv().await;
    assert_eq!(decoded(h, &b).payload, b"ok"[..]);
    stop(&app).await;
}

#[tokio::test]
async fn mqtt5_negative_pubrec_releases_ownership_and_disconnect_four_publishes_will() {
    let (app, broker) = start(MqttBrokerConfig {
        mqtt5: true,
        ..config()
    })
    .await;
    let addr = broker.local_addr().unwrap();
    let (mut sink, _) = connect(addr, "negative", true, 0, &[], None).await;
    sub(&mut sink, &[("v5/#", 2)], None).await;
    let (mut source, _) = connect(addr, "source", true, 0, &[], None).await;
    publish(&mut source, "v5/value", b"reject", 2, &[], false).await;
    let (h, b) = sink.recv().await;
    let p = decoded(h, &b);
    let mut ack = p.pkid.to_be_bytes().to_vec();
    ack.push(0x80);
    sink.send(codec::frame(0x50, &ack)).await;
    sink.send(vec![0xc0, 0]).await;
    assert_eq!(sink.recv().await, (0xd0, vec![]));
    assert_eq!(broker.status().pending_messages, 0);
    let (mut will, _) = connect(addr, "will", true, 0, &[], Some((10, 30))).await;
    will.send(vec![0xe0, 1, 4]).await;
    let (h, b) = sink.recv().await;
    assert_eq!(decoded(h, &b).payload, b"w"[..]);
    stop(&app).await;
}

#[tokio::test]
async fn mqtt5_alias_rejects_invalid_unbound_reconnected_oversize_and_unfunded_messages_atomically()
{
    let cfg = MqttBrokerConfig {
        mqtt5: true,
        ..config()
    };
    let topic = "t".repeat(1000);
    // The wire limit is still enforced; aliases neither survive reconnect nor
    // permit zero/unbound values. Each rejection leaves the previous Retained.
    let (app, broker) = start(cfg.clone()).await;
    let addr = broker.local_addr().unwrap();
    let (mut c, _) = connect(addr, "alias-negative", true, 60, &[], None).await;
    publish(&mut c, &topic, b"old", 1, &[0x23, 0, 1], true).await;
    c.disconnect().await;
    for (alias, payload_len) in [(1u16, 1usize), (0, 1), (2, 1), (1, 8182)] {
        let (mut c, present) = connect(addr, "alias-negative", false, 60, &[], None).await;
        assert!(present);
        if payload_len > 1 {
            // Establish a valid mapping so only the 8193-byte wire limit fails.
            publish(&mut c, &topic, b"old", 1, &[0x23, 0, 1], true).await;
        }
        let mut body = vec![0, 0, 0, 10, 3, 0x23];
        body.extend(alias.to_be_bytes());
        body.extend(vec![b'x'; payload_len]);
        let frame = codec::frame(0x35, &body);
        if payload_len > 1 {
            assert_eq!(frame.len(), 8193);
        }
        c.send(frame).await;
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(3), c.0.read(&mut [0]))
                .await
                .unwrap(),
            Ok(0) | Err(_)
        ));
        assert_eq!(broker.status().incoming_qos2, 0);
        assert_eq!(broker.status().retained, 1);
    }
    let (mut late, _) = connect(addr, "alias-negative-reader", true, 0, &[], None).await;
    sub(&mut late, &[(&topic, 1)], None).await;
    let (h, b) = late.recv().await;
    assert_eq!(decoded(h, &b).payload, b"old"[..]);
    stop(&app).await;

    // Reserve enough for one connection and small state, but not the expanded
    // Topic/payload retained plus inbound QoS2 records of a legal alias frame.
    let mut cfg = cfg;
    cfg.memory_budget_bytes = cfg.connection_budget().unwrap() + 8192;
    let (app, broker) = start(cfg).await;
    let addr = broker.local_addr().unwrap();
    let (mut c, _) = connect(addr, "alias-budget", true, 60, &[], None).await;
    publish(&mut c, &topic, b"old", 1, &[0x23, 0, 1], true).await;
    let mut body = vec![0, 0, 0, 10, 3, 0x23, 0, 1];
    body.extend(vec![b'x'; 8180]);
    assert_eq!(codec::frame(0x35, &body).len(), 8191);
    c.send(codec::frame(0x35, &body)).await;
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(3), c.0.read(&mut [0]))
            .await
            .unwrap(),
        Ok(0) | Err(_)
    ));
    // EOF can precede detach accounting; wait for the connection reservation.
    tokio::time::timeout(Duration::from_secs(3), async {
        while broker.status().connections != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(broker.status().incoming_qos2, 0);
    assert_eq!(broker.status().retained, 1);
    let (mut late, _) = connect(addr, "alias-budget-reader", true, 0, &[], None).await;
    sub(&mut late, &[(&topic, 1)], None).await;
    let (h, b) = late.recv().await;
    assert_eq!(decoded(h, &b).payload, b"old"[..]);
    stop(&app).await;
}

pub(super) async fn wait_will_status(
    broker: &BrokerHandle,
    condition: impl Fn(BrokerStatus) -> bool,
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !condition(broker.status()) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Will state did not reach the expected barrier");
}

#[tokio::test]
async fn mqtt5_expired_delay_will_survives_backpressure_resume_clean_start_and_takeover() {
    // Observe actual Will ownership before reconnecting, rather than relying on
    // a sleep to guess whether the maintenance task has triggered the Will.
    for (name, delay, live, clean, repeat, expected_will) in [
        ("zero-delay", 0, false, false, false, true),
        ("before-delay", 30, false, false, false, false),
        ("live-takeover", 30, true, false, false, false),
        ("expired-resume", 1, false, false, false, true),
        ("expired-clean", 1, false, true, false, true),
        ("expired-repeat", 1, false, false, true, true),
    ] {
        let (app, broker) = start(MqttBrokerConfig {
            mqtt5: true,
            max_offline_messages_per_client: 1,
            max_inflight_per_client: 1,
            ..config()
        })
        .await;
        let address = broker.local_addr().unwrap();
        let (mut sink, _) = connect(address, "sink", true, 60, &[], None).await;
        sub(&mut sink, &[("v5/#", 1)], Some(17)).await;
        let (mut source, _) = connect(address, name, true, 60, &[], Some((delay, 60))).await;
        publish(&mut source, "v5/blocker", b"blocker", 1, &[], false).await;
        let (header, body) = sink.recv().await;
        let blocker = decoded(header, &body);
        assert_eq!(blocker.payload, b"blocker"[..]);
        let mut old = Some(source);
        if !live {
            drop(old.take());
            wait_will_status(&broker, |s| s.connections == 1).await;
            if expected_will {
                wait_will_status(&broker, |s| s.pending_wills == 1).await;
            }
        }
        let (mut resumed, present) =
            connect(address, name, clean, 60, &[], repeat.then_some((30, 60))).await;
        assert_eq!(present, !clean, "{name}");
        if let Some(mut old) = old {
            assert_eq!(old.0.read(&mut [0]).await.unwrap(), 0);
        }
        if repeat {
            let (next, present) = connect(address, name, false, 60, &[], None).await;
            assert!(present);
            assert_eq!(resumed.0.read(&mut [0]).await.unwrap(), 0);
            resumed = next;
        }
        assert_eq!(
            broker.status().pending_wills,
            usize::from(expected_will),
            "{name}"
        );
        assert_eq!(
            broker.status().will_records,
            usize::from(expected_will),
            "{name}"
        );
        assert_eq!(broker.status().pending_messages, 1);
        sink.send(codec::frame(0x40, &blocker.pkid.to_be_bytes()))
            .await;
        if expected_will {
            let (header, body) = sink.recv().await;
            let will = decoded(header, &body);
            assert_eq!(will.topic, b"v5/will"[..], "{name}");
            assert_eq!(will.payload, b"w"[..], "{name}");
            assert_eq!(will.properties.unwrap().subscription_identifiers, [17]);
            sink.send(codec::frame(0x40, &will.pkid.to_be_bytes()))
                .await;
        }
        sink.send(vec![0xc0, 0]).await;
        assert_eq!(sink.recv().await, (0xd0, vec![]));
        publish(&mut resumed, "v5/barrier", b"after-will", 1, &[], false).await;
        let (header, body) = sink.recv().await;
        let barrier = decoded(header, &body);
        assert_eq!(barrier.payload, b"after-will"[..], "{name}");
        sink.send(codec::frame(0x40, &barrier.pkid.to_be_bytes()))
            .await;
        sink.send(vec![0xc0, 0]).await;
        assert_eq!(sink.recv().await, (0xd0, vec![]));
        assert_eq!(broker.status().pending_wills, 0);
        assert_eq!(broker.status().pending_messages, 0);
        stop(&app).await;
    }
}
