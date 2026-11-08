use super::*;

fn connect_packet(level: u8, flags: u8, id: &str) -> Vec<u8> {
    let mut body = string("MQTT");
    body.extend([level, flags, 0, 0]);
    body.extend(string(id));
    codec::frame(0x10, &body)
}

async fn closed(client: &mut Client) {
    let result = tokio::time::timeout(Duration::from_secs(2), client.0.read_u8())
        .await
        .expect("broker must close the connection");
    assert_eq!(
        result.unwrap_err().kind(),
        std::io::ErrorKind::UnexpectedEof
    );
}

#[tokio::test]
async fn rejected_connect_does_not_process_pipelined_publication() {
    let (app, broker) = start(config()).await;
    let addr = broker.local_addr().unwrap();
    let (mut sub, _) = Client::connect(addr, "reader", true).await;
    sub.subscribe(&[("pipeline", 1)]).await;
    for (level, flags, id, reason) in [(5, 2, "writer", 1), (4, 0, "", 2)] {
        let mut c = Client(TcpStream::connect(addr).await.unwrap());
        let mut bytes = connect_packet(level, flags, id);
        let mut publish = string("pipeline");
        publish.extend([0, 7]);
        publish.extend(b"must-not-route");
        bytes.extend(codec::frame(0x32, &publish));
        c.send(bytes).await;
        assert_eq!(c.recv().await, (0x20, vec![0, reason]));
        closed(&mut c).await;
        qos2::ping(&mut sub).await;
        assert_eq!(broker.status().received_publications, 0);
    }
    stop(&app).await;
}

#[tokio::test]
async fn malformed_connect_closes_without_connack() {
    let (app, broker) = start(config()).await;
    let addr = broker.local_addr().unwrap();
    // Reserved bit, absent Will with QoS/retain, invalid Will QoS, missing
    // flagged Will/user/password fields, and password without user flag.
    for flags in [3, 10, 34, 30, 6, 130, 194, 66] {
        let mut c = Client(TcpStream::connect(addr).await.unwrap());
        c.send(connect_packet(4, flags, "invalid")).await;
        closed(&mut c).await;
    }
    let mut c = Client(TcpStream::connect(addr).await.unwrap());
    let mut body = string("MQTT");
    body.extend([4, 2, 0, 0]);
    body.extend(string("extra"));
    body.extend(string("unflagged-username"));
    c.send(codec::frame(0x10, &body)).await;
    closed(&mut c).await;
    stop(&app).await;
}

#[tokio::test]
async fn malformed_packets_and_second_connect_close_only_the_offending_connection() {
    let (app, broker) = start(config()).await;
    let addr = broker.local_addr().unwrap();
    let (mut healthy, _) = Client::connect(addr, "healthy", true).await;
    let mut bad = vec![
        vec![0x80, 0],       // SUBSCRIBE flags
        vec![0xa0, 0],       // UNSUBSCRIBE flags
        vec![0xe1, 0],       // DISCONNECT flags
        vec![0x82, 2, 0, 1], // empty SUBSCRIBE
        vec![0xa2, 2, 0, 1], // empty UNSUBSCRIBE
        vec![0xc0, 1, 0],    // PINGREQ payload
        vec![0x40, 2, 0, 0], // zero Packet ID
        connect_packet(4, 2, "second"),
    ];
    for qos in [3, 4, 0x80] {
        let mut b = vec![0, 1];
        b.extend(string("valid"));
        b.push(qos);
        bad.push(codec::frame(0x82, &b));
    }
    for topic in [
        b"bad\0".as_slice(),
        b"bad\xff",
        b"\xed\xa0\x80",
        b"+",
        b"#",
        b"",
    ] {
        let mut b = (topic.len() as u16).to_be_bytes().to_vec();
        b.extend(topic);
        bad.push(codec::frame(0x30, &b));
    }
    let mut first = Client(TcpStream::connect(addr).await.unwrap());
    first.send(vec![0xc0, 0]).await;
    closed(&mut first).await;
    for frame in bad {
        let (mut c, _) = Client::connect(addr, "offender", true).await;
        c.send(frame).await;
        closed(&mut c).await;
        qos2::ping(&mut healthy).await;
    }
    stop(&app).await;
}

#[tokio::test]
async fn replacement_subscription_replays_retained_and_unsubscribe_is_exact() {
    let (app, broker) = start(config()).await;
    let addr = broker.local_addr().unwrap();
    let (mut p, _) = Client::connect(addr, "writer", true).await;
    p.publish("a/", b"retained", 1, true).await;
    let (mut s, _) = Client::connect(addr, "reader", true).await;
    assert_eq!(s.subscribe(&[("a/+", 1)]).await, [1]);
    let (_, _, header, id) = s.message().await;
    assert_eq!(header, 0x33);
    s.ack(id.unwrap()).await;
    assert_eq!(s.subscribe(&[("a/+", 0)]).await, [0]);
    let (_, payload, header, id) = s.message().await;
    assert_eq!((payload, header, id), (b"retained".to_vec(), 0x31, None));
    // Matching a wildcard's expansion is not an exact filter deletion.
    let mut body = vec![0x12, 0x34];
    body.extend(string("a/"));
    body.extend(string("unknown"));
    s.send(codec::frame(0xa2, &body)).await;
    assert_eq!(s.recv().await, (0xb0, vec![0x12, 0x34]));
    p.publish("a/", b"live", 1, false).await;
    let (_, payload, header, _) = s.message().await;
    assert_eq!((payload, header), (b"live".to_vec(), 0x30));
    let mut body = vec![0x12, 0x35];
    body.extend(string("a/+"));
    s.send(codec::frame(0xa2, &body)).await;
    assert_eq!(s.recv().await, (0xb0, vec![0x12, 0x35]));
    p.publish("a/", b"after-unsubscribe", 1, false).await;
    qos2::ping(&mut s).await;
    stop(&app).await;
}

#[tokio::test]
async fn ack_fifos_preserve_received_order_including_duplicate_pubrec() {
    let mut c = config();
    c.per_client_outbound_queue = 1; // exercise bounded ingress backpressure
    let (app, broker) = start(c).await;
    let addr = broker.local_addr().unwrap();
    let (mut p, _) = Client::connect(addr, "writer", true).await;
    for qos in [1u8, 2] {
        let mut frames = Vec::new();
        for id in [9u16, 2, 7] {
            let mut b = string("fifo");
            b.extend(id.to_be_bytes());
            b.push(id as u8);
            frames.extend(codec::frame(0x30 | (qos << 1), &b));
        }
        p.send(frames).await;
        for id in [9u16, 2, 7] {
            assert_eq!(
                p.recv().await,
                (
                    if qos == 1 { 0x40 } else { 0x50 },
                    id.to_be_bytes().to_vec()
                )
            );
        }
        if qos == 2 {
            for id in [9, 2, 7] {
                qos2::release(&mut p, id).await;
            }
        }
    }
    let (mut s, _) = Client::connect(addr, "reader", false).await;
    s.subscribe(&[("fifo", 2)]).await;
    let mut ids = Vec::new();
    for payload in [b"one", b"two", b"tri"] {
        p.publish("fifo", payload, 2, false).await;
        ids.push(s.message().await.3.unwrap());
    }
    let order = [ids[1], ids[0], ids[1], ids[2]];
    let mut frames = Vec::new();
    for id in order {
        frames.extend(codec::frame(0x50, &id.to_be_bytes()));
    }
    s.send(frames).await;
    for id in order {
        assert_eq!(s.recv().await, (0x62, id.to_be_bytes().to_vec()));
    }
    for id in ids {
        qos2::complete(&mut s, id).await;
    }
    assert_eq!(broker.status().pending_messages, 0);
    stop(&app).await;
}

#[tokio::test]
async fn required_client_identifiers_and_opaque_unicode_topics_are_preserved() {
    let mut cfg = config();
    cfg.max_client_id_bytes = 23;
    let (app, broker) = start(cfg).await;
    let addr = broker.local_addr().unwrap();
    let alphabet = "0123456789abcXYZklmnopqr";
    for len in 1..=23 {
        let (mut c, present) = Client::connect(addr, &alphabet[..len], true).await;
        assert!(!present);
        c.send(vec![0xe0, 0]).await;
        closed(&mut c).await;
    }
    let (mut p, _) = Client::connect(addr, "writer", true).await;
    let (mut s, _) = Client::connect(addr, "reader", true).await;
    s.subscribe(&[("#", 0)]).await;
    // BOM is U+FEFF, not a marker to strip; composed/decomposed forms,
    // case, spaces, slash and empty levels must remain distinct and intact.
    for topic in ["\u{feff}topic", "é", "e\u{301}", "A", "a", " ", "/", "a//b"] {
        p.publish(topic, b"opaque", 1, false).await;
        let (actual, payload, header, _) = s.message().await;
        assert_eq!(
            (actual.as_str(), payload, header),
            (topic, b"opaque".to_vec(), 0x30)
        );
    }
    s.subscribe(&[("é", 0)]).await;
    let mut body = vec![0, 7];
    body.extend(string("#"));
    s.send(codec::frame(0xa2, &body)).await;
    assert_eq!(s.recv().await, (0xb0, vec![0, 7]));
    p.publish("e\u{301}", b"must-not-normalize", 1, false).await;
    qos2::ping(&mut s).await;
    stop(&app).await;
}

#[test]
fn client_id_budget_cannot_exclude_mandatory_23_byte_identifiers() {
    let mut c = config();
    for len in [0, 1, 22, 65536] {
        c.max_client_id_bytes = len;
        assert!(c.validate().is_err());
    }
    for len in [23, 128, 65535] {
        c.max_client_id_bytes = len;
        assert!(c.validate().is_ok());
    }
}
