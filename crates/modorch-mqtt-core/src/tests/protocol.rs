use super::*;

async fn settle(c: &mut Client, header: u8, id: Option<u16>) {
    match (header >> 1) & 3 {
        0 => assert!(id.is_none()),
        1 => c.ack(id.unwrap()).await,
        2 => {
            qos2::received(c, id.unwrap()).await;
            qos2::complete(c, id.unwrap()).await;
        }
        _ => panic!("invalid delivery QoS"),
    }
}

pub(super) async fn disconnect(c: &mut Client) {
    c.send(vec![0xe0, 0]).await;
    closed(c).await;
}

pub(super) async fn closed(c: &mut Client) {
    let result = tokio::time::timeout(Duration::from_secs(2), c.0.read_u8())
        .await
        .expect("connection must close");
    assert_eq!(
        result.unwrap_err().kind(),
        std::io::ErrorKind::UnexpectedEof
    );
}

#[tokio::test]
async fn mixed_subscribe_codes_follow_filter_order_and_duplicate_filters_replace_in_sequence() {
    let mut cfg = config();
    cfg.max_subscriptions_per_client = 5;
    let (app, broker) = start(cfg).await;
    let addr = broker.local_addr().unwrap();
    let (mut p, _) = Client::connect(addr, "writer", true).await;
    p.publish("mix/topic", b"saved", 2, true).await;
    let (mut s, _) = Client::connect(addr, "reader", false).await;
    let mut body = vec![0x12, 0x34];
    for (filter, qos) in [("mix/#", 0), ("$modorch/#", 1), ("mix/#", 2), ("extra", 1)] {
        body.extend(string(filter));
        body.push(qos);
    }
    s.send(codec::frame(0x82, &body)).await;
    assert_eq!(s.recv().await, (0x90, vec![0x12, 0x34, 0, 0x80, 2, 1]));
    let mut replay_qos = Vec::new();
    let mut exchanges = Vec::new();
    for _ in 0..2 {
        let (topic, payload, header, id) = s.message().await;
        assert_eq!((topic.as_str(), payload), ("mix/topic", b"saved".to_vec()));
        assert_eq!(header & 1, 1);
        replay_qos.push((header >> 1) & 3);
        exchanges.push((header, id));
    }
    for (header, id) in exchanges {
        settle(&mut s, header, id).await;
    }
    replay_qos.sort();
    assert_eq!(replay_qos, [0, 2]);
    p.publish("mix/topic", b"live", 2, false).await;
    let (_, payload, header, id) = s.message().await;
    assert_eq!((payload, header), (b"live".to_vec(), 0x34));
    settle(&mut s, header, id).await;
    p.publish("extra", b"qos1", 2, false).await;
    let (_, payload, header, id) = s.message().await;
    assert_eq!((payload, header), (b"qos1".to_vec(), 0x32));
    settle(&mut s, header, id).await;
    qos2::ping(&mut s).await; // no duplicate live delivery for replaced filter
    assert_eq!(broker.status().subscriptions, 2);
    stop(&app).await;
}

#[tokio::test]
async fn publish_qos_matrix_keeps_payload_and_sets_outgoing_dup_independently() {
    let (app, broker) = start(config()).await;
    let addr = broker.local_addr().unwrap();
    let (mut p, _) = Client::connect(addr, "writer", false).await;
    for requested in 0..=2 {
        let (mut s, _) = Client::connect(addr, "reader", true).await;
        assert_eq!(s.subscribe(&[("matrix", requested)]).await, [requested]);
        for original in 0..=2 {
            let id = 0x1200 + u16::from(requested * 3 + original);
            let payload = [0x12, 0x34, 0, 0xff, requested, original];
            let mut body = string("matrix");
            if original != 0 {
                body.extend(id.to_be_bytes());
            }
            body.extend(payload);
            // A first observed incoming retransmission must not mark a new
            // subscriber delivery DUP. QoS0 cannot carry DUP.
            p.send(codec::frame(
                0x30 | (original << 1) | if original == 0 { 0 } else { 8 },
                &body,
            ))
            .await;
            if original != 0 {
                assert_eq!(
                    p.recv().await,
                    (
                        if original == 1 { 0x40 } else { 0x50 },
                        id.to_be_bytes().to_vec()
                    )
                );
                if original == 2 {
                    qos2::release(&mut p, id).await;
                }
            }
            qos2::ping(&mut p).await; // QoS0 produced no ACK; acceptance barrier
            let (topic, actual, header, delivery_id) = s.message().await;
            assert_eq!(topic, "matrix");
            assert_eq!(actual, payload);
            assert_eq!(header, 0x30 | (original.min(requested) << 1));
            assert_eq!(delivery_id.is_some(), original.min(requested) != 0);
            assert_ne!(delivery_id, Some(0));
            settle(&mut s, header, delivery_id).await;
        }
        disconnect(&mut s).await;
    }
    assert_eq!(broker.status().pending_messages, 0);
    assert_eq!(broker.status().incoming_qos2, 0);
    stop(&app).await;
}

#[tokio::test]
async fn retained_replacement_qos0_delete_and_nonretained_publish_obey_wire_flags() {
    let (app, broker) = start(config()).await;
    let addr = broker.local_addr().unwrap();
    let (mut p, _) = Client::connect(addr, "writer", true).await;
    let (mut observer, _) = Client::connect(addr, "observer", true).await;
    observer.subscribe(&[("retain", 2)]).await;
    for qos in [2, 0, 1] {
        let payload = [qos, 0, 0xff];
        p.publish("retain", &payload, qos, true).await;
        qos2::ping(&mut p).await;
        let (_, actual, header, id) = observer.message().await;
        assert_eq!((actual, header), (payload.to_vec(), 0x30 | (qos << 1)));
        settle(&mut observer, header, id).await;
        let (mut late, _) = Client::connect(addr, "late", true).await;
        late.subscribe(&[("retain", 2)]).await;
        let (_, actual, header, id) = late.message().await;
        assert_eq!((actual, header), (payload.to_vec(), 0x31 | (qos << 1)));
        settle(&mut late, header, id).await;
        disconnect(&mut late).await;
    }
    // RETAIN=0 neither replaces nor deletes the last retained value, including
    // an empty payload. Existing subscribers see the ordinary publication.
    for payload in [b"transient".as_slice(), b""] {
        p.publish("retain", payload, 1, false).await;
        let (_, actual, header, id) = observer.message().await;
        assert_eq!((actual, header), (payload.to_vec(), 0x32));
        settle(&mut observer, header, id).await;
    }
    let (mut late, _) = Client::connect(addr, "late", true).await;
    late.subscribe(&[("retain", 2)]).await;
    let (_, actual, header, id) = late.message().await;
    assert_eq!((actual, header), (vec![1, 0, 0xff], 0x33));
    settle(&mut late, header, id).await;
    disconnect(&mut late).await;
    p.publish("retain", b"", 2, true).await;
    let (_, actual, header, id) = observer.message().await;
    assert_eq!((actual, header), (vec![], 0x34));
    settle(&mut observer, header, id).await;
    assert_eq!(broker.status().retained, 0);
    let (mut late, _) = Client::connect(addr, "late", true).await;
    late.subscribe(&[("retain", 2)]).await;
    qos2::ping(&mut late).await;
    stop(&app).await;
}

#[tokio::test]
async fn unsubscribe_keeps_started_qos1_and_qos2_deliveries_but_stops_new_routing() {
    let (app, broker) = start(config()).await;
    let addr = broker.local_addr().unwrap();
    let (mut s, _) = Client::connect(addr, "reader", false).await;
    s.subscribe(&[("flight", 2)]).await;
    let (mut p, _) = Client::connect(addr, "writer", true).await;
    let mut outstanding = Vec::new();
    for qos in [1, 2] {
        p.publish("flight", &[qos], qos, false).await;
        let (_, payload, header, id) = s.message().await;
        assert_eq!(payload, [qos]);
        outstanding.push((header, id));
    }
    let mut body = vec![0xab, 0xcd];
    body.extend(string("flight"));
    s.send(codec::frame(0xa2, &body)).await;
    assert_eq!(s.recv().await, (0xb0, vec![0xab, 0xcd]));
    assert_eq!(broker.status().pending_messages, 2);
    p.publish("flight", b"after", 2, false).await;
    for (header, id) in outstanding {
        settle(&mut s, header, id).await;
    }
    qos2::ping(&mut s).await;
    assert_eq!(broker.status().pending_messages, 0);
    assert_eq!(broker.status().subscriptions, 0);
    stop(&app).await;
}

#[tokio::test]
async fn clean_session_discards_both_qos2_directions_and_subscriptions_but_keeps_retained() {
    let (app, broker) = start(config()).await;
    let addr = broker.local_addr().unwrap();
    let (mut s, present) = Client::connect(addr, "reader", false).await;
    assert!(!present);
    s.subscribe(&[("clean", 2)]).await;
    qos2::publish_start(&mut s, 77, "clean", b"saved", false, true).await;
    let (_, _, _, delivery_id) = s.message().await;
    assert!(delivery_id.is_some());
    assert_eq!(broker.status().pending_messages, 1);
    assert_eq!(broker.status().incoming_qos2, 1);
    // Take over with CleanSession=1 while the old socket is still alive.
    let (mut fresh, present) = Client::connect(addr, "reader", true).await;
    assert!(!present);
    closed(&mut s).await;
    qos2::ping(&mut fresh).await;
    assert_eq!(broker.status().pending_messages, 0);
    assert_eq!(broker.status().incoming_qos2, 0);
    assert_eq!(broker.status().subscriptions, 0);
    assert_eq!(broker.status().retained, 1);
    fresh.subscribe(&[("clean", 2)]).await;
    let (_, payload, header, id) = fresh.message().await;
    assert_eq!((payload, header), (b"saved".to_vec(), 0x35));
    settle(&mut fresh, header, id).await;
    disconnect(&mut fresh).await;
    let (mut empty, present) = Client::connect(addr, "reader", false).await;
    assert!(!present);
    qos2::ping(&mut empty).await;
    assert_eq!(broker.status().subscriptions, 0);
    let (mut resumed, present) = Client::connect(addr, "reader", false).await;
    assert!(present);
    closed(&mut empty).await;
    qos2::ping(&mut resumed).await;
    stop(&app).await;
}

#[tokio::test]
async fn network_topic_matching_preserves_empty_levels_dollar_isolation_and_literal_colons() {
    let (app, broker) = start(config()).await;
    let addr = broker.local_addr().unwrap();
    let (mut p, _) = Client::connect(addr, "writer", true).await;
    for (filter, topic, expected) in [
        ("a/#", "a", true),
        ("a/#", "a/", true),
        ("a/+", "a/", true),
        ("a/+", "a", false),
        ("a/+", "a//b", false),
        ("/+/", "//", true),
        ("#", "$SYS/a", false),
        ("+/#", "$SYS/a", false),
        ("$SYS/#", "$SYS/a", true),
        ("$SYS/#", "$SYS", true),
        ("a/:id", "a/7", false),
        ("a/:id", "a/:id", true),
        ("A", "a", false),
        (" ", " ", true),
        ("/", "/", true),
        ("a/#", "ab", false),
    ] {
        let (mut s, _) = Client::connect(addr, "reader", true).await;
        assert_eq!(s.subscribe(&[(filter, 0)]).await, [0]);
        p.publish(topic, b"match", 1, false).await;
        if expected {
            let (actual, payload, header, id) = s.message().await;
            assert_eq!(
                (actual.as_str(), payload, header, id),
                (topic, b"match".to_vec(), 0x30, None),
                "{filter} vs {topic}"
            );
        }
        qos2::ping(&mut s).await;
        disconnect(&mut s).await;
    }
    stop(&app).await;
}

#[tokio::test]
async fn maximum_utf8_topic_and_filter_are_supported_as_65535_encoded_bytes() {
    let mut cfg = config();
    cfg.max_packet_bytes = 131072;
    let (app, broker) = start(cfg).await;
    let addr = broker.local_addr().unwrap();
    let topic = "界".repeat(21845);
    assert_eq!(topic.len(), 65535);
    let (mut p, _) = Client::connect(addr, "writer", true).await;
    p.publish(&topic, &[0, 0xff], 1, true).await;
    let (mut s, _) = Client::connect(addr, "reader", true).await;
    assert_eq!(s.subscribe(&[(&topic, 1)]).await, [1]);
    let (actual, payload, header, id) = s.message().await;
    assert_eq!((actual, payload, header), (topic, vec![0, 0xff], 0x33));
    s.ack(id.unwrap()).await;
    qos2::ping(&mut s).await;
    stop(&app).await;
}

#[tokio::test]
async fn malformed_utf8_in_each_connect_and_filter_field_is_rejected_but_password_is_binary() {
    let (app, broker) = start(config()).await;
    let addr = broker.local_addr().unwrap();
    for invalid in [b"\0".as_slice(), b"\xff", b"\xed\xa0\x80"] {
        let encoded = || {
            let mut bytes = (invalid.len() as u16).to_be_bytes().to_vec();
            bytes.extend(invalid);
            bytes
        };
        for field in 0..3 {
            let mut body = string("MQTT");
            body.extend([
                4,
                if field == 1 {
                    6
                } else if field == 2 {
                    0x82
                } else {
                    2
                },
                0,
                0,
            ]);
            body.extend(if field == 0 {
                encoded()
            } else {
                string("reader")
            });
            if field == 1 {
                body.extend(encoded());
                body.extend([0, 0]);
            }
            if field == 2 {
                body.extend(encoded());
            }
            let mut c = Client(TcpStream::connect(addr).await.unwrap());
            c.send(codec::frame(0x10, &body)).await;
            closed(&mut c).await;
        }
        for header in [0x82, 0xa2] {
            let (mut c, _) = Client::connect(addr, "reader", true).await;
            let mut body = vec![0, 9];
            body.extend(encoded());
            if header == 0x82 {
                body.push(0);
            }
            c.send(codec::frame(header, &body)).await;
            closed(&mut c).await;
        }
    }
    let mut body = string("MQTT");
    body.extend([4, 0xc2, 0, 0]);
    body.extend(string("reader"));
    body.extend(string("\u{feff}用户"));
    body.extend([0, 3, 0, 0xff, 0x80]); // password is binary, not MQTT UTF8
    let mut c = Client(TcpStream::connect(addr).await.unwrap());
    c.send(codec::frame(0x10, &body)).await;
    // Anonymous mode rejects credentials after parsing; a malformed string
    // would close with no CONNACK. Code5 demonstrates binary parsing succeeded.
    assert_eq!(c.recv().await, (0x20, vec![0, 5]));
    closed(&mut c).await;
    stop(&app).await;
}

#[tokio::test]
async fn will_qos_and_retain_matrix_preserves_binary_payload_and_no_will_stays_silent() {
    let (app, broker) = start(config()).await;
    let addr = broker.local_addr().unwrap();
    let (mut observer, _) = Client::connect(addr, "observer", true).await;
    observer.subscribe(&[("will/#", 2)]).await;
    for qos in 0..=2 {
        for retain in [false, true] {
            let topic = format!("will/{qos}/{retain}");
            let payload = [0, 0xff, qos];
            let mut body = string("MQTT");
            body.extend([4, 6 | (qos << 3) | (u8::from(retain) << 5), 0, 0]);
            body.extend(string("writer"));
            body.extend(string(&topic));
            body.extend([0, 3]);
            body.extend(payload);
            let mut c = Client(TcpStream::connect(addr).await.unwrap());
            c.send(codec::frame(0x10, &body)).await;
            assert_eq!(c.recv().await, (0x20, vec![0, 0]));
            drop(c);
            let (actual_topic, actual_payload, header, id) = observer.message().await;
            assert_eq!(
                (actual_topic.as_str(), actual_payload, header),
                (topic.as_str(), payload.to_vec(), 0x30 | (qos << 1))
            );
            settle(&mut observer, header, id).await;
            let (mut late, _) = Client::connect(addr, "late", true).await;
            late.subscribe(&[(&topic, 2)]).await;
            if retain {
                let (_, actual, header, id) = late.message().await;
                assert_eq!((actual, header), (payload.to_vec(), 0x31 | (qos << 1)));
                settle(&mut late, header, id).await;
            }
            qos2::ping(&mut late).await;
            disconnect(&mut late).await;
        }
    }
    let (mut c, _) = Client::connect(addr, "no-will", true).await;
    disconnect(&mut c).await;
    qos2::ping(&mut observer).await;
    assert_eq!(broker.status().triggered_wills, 6);
    stop(&app).await;
}

#[tokio::test]
async fn persistent_offline_session_queues_both_reliable_qos_levels_until_reconnect() {
    let (app, broker) = start(config()).await;
    let addr = broker.local_addr().unwrap();
    let (mut s, _) = Client::connect(addr, "offline", false).await;
    s.subscribe(&[("offline", 2)]).await;
    disconnect(&mut s).await;
    let (mut p, _) = Client::connect(addr, "writer", true).await;
    for qos in [1, 2] {
        p.publish("offline", &[qos], qos, false).await;
    }
    assert_eq!(broker.status().pending_messages, 2);
    let (mut s, present) = Client::connect(addr, "offline", false).await;
    assert!(present);
    let one = s.message().await;
    let two = s.message().await;
    assert_eq!((one.1, one.2), (vec![1], 0x32));
    assert_eq!((two.1, two.2), (vec![2], 0x34));
    assert_ne!(one.3, two.3);
    assert_eq!(one.3, Some(1));
    assert_eq!(two.3, Some(2));
    s.ack(one.3.unwrap()).await;
    qos2::received(&mut s, two.3.unwrap()).await;
    qos2::complete(&mut s, two.3.unwrap()).await;
    assert_eq!(broker.status().pending_messages, 0);
    stop(&app).await;
}

#[tokio::test]
async fn connect_failure_codes_distinguish_rejected_id_capacity_and_authorization() {
    let mut cfg = config();
    cfg.max_client_id_bytes = 23;
    cfg.max_connections = 2;
    cfg.max_sessions = 2;
    let (app, broker) = start(cfg).await;
    let addr = broker.local_addr().unwrap();
    let connect = |name: &str, id: &str| {
        let mut body = string(name);
        body.extend([4, 2, 0, 0]);
        body.extend(string(id));
        codec::frame(0x10, &body)
    };
    let mut malformed = Client(TcpStream::connect(addr).await.unwrap());
    malformed.send(connect("mqtt", "reader")).await;
    closed(&mut malformed).await; // incorrect protocol name: no CONNACK
    let mut rejected = Client(TcpStream::connect(addr).await.unwrap());
    rejected
        .send(connect("MQTT", "012345678901234567890123"))
        .await;
    assert_eq!(rejected.recv().await, (0x20, vec![0, 2]));
    closed(&mut rejected).await;
    let mut denied = Client(TcpStream::connect(addr).await.unwrap());
    let mut body = string("MQTT");
    body.extend([4, 6, 0, 0]);
    body.extend(string("denied"));
    body.extend(string("$modorch/private"));
    body.extend([0, 0]);
    denied.send(codec::frame(0x10, &body)).await;
    assert_eq!(denied.recv().await, (0x20, vec![0, 5]));
    closed(&mut denied).await;
    // Exhaust Session capacity using disconnected persistent sessions, leaving
    // TCP admission available so a CONNACK3 can be sent.
    for id in ["one", "two"] {
        let (mut c, present) = Client::connect(addr, id, false).await;
        assert!(!present);
        disconnect(&mut c).await;
    }
    let mut full = Client(TcpStream::connect(addr).await.unwrap());
    full.send(connect("MQTT", "three")).await;
    assert_eq!(full.recv().await, (0x20, vec![0, 3]));
    closed(&mut full).await;
    assert_eq!(broker.status().sessions, 2);
    stop(&app).await;
}

#[tokio::test]
async fn packet_identifier_namespaces_are_independent_in_simultaneous_qos2_directions() {
    let (app, broker) = start(config()).await;
    let (mut c, _) = Client::connect(broker.local_addr().unwrap(), "both-directions", false).await;
    c.subscribe(&[("duplex", 2)]).await;
    qos2::publish_start(&mut c, 1, "duplex", b"same-id", false, false).await;
    let (_, payload, header, id) = c.message().await;
    assert_eq!((payload, header, id), (b"same-id".to_vec(), 0x34, Some(1)));
    qos2::received(&mut c, 1).await; // ack the server's PUBLISH1
    assert_eq!(broker.status().incoming_qos2, 1);
    assert_eq!(broker.status().pending_messages, 1);
    qos2::release(&mut c, 1).await; // complete the client's PUBLISH1
    assert_eq!(broker.status().incoming_qos2, 0);
    assert_eq!(broker.status().pending_messages, 1);
    qos2::complete(&mut c, 1).await;
    assert_eq!(broker.status().pending_messages, 0);
    stop(&app).await;
}

#[tokio::test]
async fn malformed_filter_packets_do_not_partially_install_or_remove_prior_filters() {
    let (app, broker) = start(config()).await;
    let addr = broker.local_addr().unwrap();
    let (mut p, _) = Client::connect(addr, "writer", true).await;
    for header in [0x82, 0xa2] {
        for invalid in ["", "a+", "a/#/b", "a/+b"] {
            let (mut c, _) = Client::connect(addr, "reader", true).await;
            // Start a new persistent session with no previous filters.
            disconnect(&mut c).await;
            let (mut c, present) = Client::connect(addr, "reader", false).await;
            assert!(!present);
            if header == 0xa2 {
                c.subscribe(&[("valid", 1)]).await;
            }
            let mut body = vec![0, 9];
            for filter in ["valid", invalid] {
                body.extend(string(filter));
                if header == 0x82 {
                    body.push(1);
                }
            }
            c.send(codec::frame(header, &body)).await;
            closed(&mut c).await;
            let (mut c, present) = Client::connect(addr, "reader", false).await;
            assert!(present);
            p.publish("valid", b"unchanged", 1, false).await;
            if header == 0xa2 {
                let (_, payload, h, id) = c.message().await;
                assert_eq!((payload, h), (b"unchanged".to_vec(), 0x32));
                c.ack(id.unwrap()).await;
            }
            qos2::ping(&mut c).await;
            disconnect(&mut c).await;
        }
    }
    stop(&app).await;
}
