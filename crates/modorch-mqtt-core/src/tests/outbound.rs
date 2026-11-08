//! Literal wire expectations are owned by the specification fixture, not Codec.
use super::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Vectors {
    version: u8,
    connect: ConnectVectors,
    controls: Vec<ControlVector>,
    publishes: Vec<PublishVector>,
    lengths: Vec<LengthVector>,
}
#[derive(Deserialize)]
struct ConnectVectors {
    persistent_hex: String,
    clean_hex: String,
    new_hex: String,
    resumed_hex: String,
    rejections: Vec<ControlVector>,
}
#[derive(Deserialize)]
struct ControlVector {
    kind: String,
    request_hex: String,
    response_hex: String,
}
#[derive(Deserialize)]
struct PublishVector {
    qos: u8,
    retain: bool,
    dup: bool,
    packet_id: Option<u16>,
    topic: String,
    payload_hex: String,
    wire_hex: String,
}
#[derive(Deserialize)]
struct LengthVector {
    qos: u8,
    remaining: usize,
    payload_bytes: usize,
    prefix_hex: String,
}
fn vectors() -> Vectors {
    let vectors: Vectors =
        serde_json::from_str(include_str!("../../../../contracts/mqtt311-outbound.json")).unwrap();
    assert_eq!(vectors.version, 1);
    assert_eq!(vectors.controls.len(), 16);
    assert_eq!(vectors.publishes.len(), 14);
    assert_eq!(vectors.lengths.len(), 12);
    vectors
}
fn hex(input: &str) -> Vec<u8> {
    assert_eq!(input.len() % 2, 0);
    input
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}
async fn expect_wire(client: &mut Client, expected: &[u8]) {
    // Read exactly the independently known length; never decode through Codec.
    let mut actual = vec![0; expected.len()];
    tokio::time::timeout(Duration::from_secs(3), client.0.read_exact(&mut actual))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(actual, expected);
}
async fn ping_wire(client: &mut Client) {
    client.send(vec![0xc0, 0]).await;
    // Also detects an extra packet, trailing bytes or an incorrectly declared RL.
    expect_wire(client, &[0xd0, 0]).await;
}
fn publish_vector(vectors: &Vectors, qos: u8, retain: bool, dup: bool) -> &PublishVector {
    vectors
        .publishes
        .iter()
        .find(|v| {
            v.qos == qos
                && v.retain == retain
                && v.dup == dup
                && v.topic == "w"
                && v.payload_hex == "00ff"
                && v.packet_id == (qos != 0).then_some(1)
        })
        .unwrap()
}

#[test]
fn outgoing_publish_flags_binary_utf8_and_length_boundaries_match_literal_vectors() {
    let vectors = vectors();
    for vector in &vectors.publishes {
        let qos = Qos::try_from(vector.qos).unwrap();
        let message = MqttMessage {
            topic: vector.topic.clone(),
            payload: hex(&vector.payload_hex).into(),
            qos,
            retain: vector.retain,
            properties: Default::default(),
        };
        assert_eq!(
            codec::publish(&message, qos, vector.retain, vector.packet_id, vector.dup),
            hex(&vector.wire_hex),
        );
        assert_eq!(
            codec::publish_size(&message, vector.packet_id),
            hex(&vector.wire_hex).len()
        );
    }
    for vector in &vectors.lengths {
        let qos = Qos::try_from(vector.qos).unwrap();
        let message = MqttMessage {
            topic: "w".into(),
            payload: vec![0xa5; vector.payload_bytes].into(),
            qos,
            retain: false,
            properties: Default::default(),
        };
        let expected_prefix = hex(&vector.prefix_hex);
        let actual = codec::publish(&message, qos, false, (vector.qos != 0).then_some(1), false);
        assert_eq!(actual[..expected_prefix.len()], expected_prefix);
        assert_eq!(
            codec::publish_size(&message, (vector.qos != 0).then_some(1)),
            actual.len()
        );
        assert_eq!(actual[expected_prefix.len()..], message.payload);
        assert_eq!(
            actual.len(),
            vector.remaining + expected_prefix.len() - (if vector.qos == 0 { 3 } else { 5 }),
        );
    }
}

#[tokio::test]
async fn outgoing_control_responses_have_exact_flags_lengths_identifiers_and_codes() {
    let vectors = vectors();
    let (app, broker) = start(config()).await;
    let mut peer = Client(
        TcpStream::connect(broker.local_addr().unwrap())
            .await
            .unwrap(),
    );
    peer.send(hex(&vectors.connect.persistent_hex)).await;
    expect_wire(&mut peer, &hex(&vectors.connect.new_hex)).await;
    for vector in &vectors.controls {
        peer.send(hex(&vector.request_hex)).await;
        expect_wire(&mut peer, &hex(&vector.response_hex)).await;
        ping_wire(&mut peer).await;
    }
    // PUBREL is idempotent after the original receiver exchange has gone away.
    for vector in vectors.controls.iter().filter(|v| v.kind == "PUBCOMP") {
        peer.send(hex(&vector.request_hex)).await;
        expect_wire(&mut peer, &hex(&vector.response_hex)).await;
    }
    ping_wire(&mut peer).await;
    protocol::disconnect(&mut peer).await;
    stop(&app).await;
}

#[tokio::test]
async fn outgoing_connack_new_resumed_clean_and_rejected_frames_preserve_existing_session() {
    let vectors = vectors();
    let mut cfg = config();
    cfg.max_sessions = 8;
    let (app, broker) = start(cfg).await;
    let addr = broker.local_addr().unwrap();
    let mut peer = Client(TcpStream::connect(addr).await.unwrap());
    peer.send(hex(&vectors.connect.persistent_hex)).await;
    expect_wire(&mut peer, &hex(&vectors.connect.new_hex)).await;
    protocol::disconnect(&mut peer).await;
    // Fill the remaining persistent Session capacity without retaining sockets.
    for index in 1..8 {
        let (mut filler, _) = Client::connect(addr, &format!("fill-{index}"), false).await;
        protocol::disconnect(&mut filler).await;
    }
    let mut healthy = Client(TcpStream::connect(addr).await.unwrap());
    healthy.send(hex(&vectors.connect.persistent_hex)).await;
    expect_wire(&mut healthy, &hex(&vectors.connect.resumed_hex)).await;
    for vector in &vectors.connect.rejections {
        let mut rejected = Client(TcpStream::connect(addr).await.unwrap());
        rejected.send(hex(&vector.request_hex)).await;
        expect_wire(&mut rejected, &hex(&vector.response_hex)).await;
        protocol::closed(&mut rejected).await;
        ping_wire(&mut healthy).await;
        assert_eq!(
            broker.status().sessions,
            8,
            "{} rejection changed Sessions",
            vector.kind
        );
    }
    let mut clean = Client(TcpStream::connect(addr).await.unwrap());
    clean.send(hex(&vectors.connect.clean_hex)).await;
    expect_wire(&mut clean, &hex(&vectors.connect.new_hex)).await;
    protocol::closed(&mut healthy).await;
    ping_wire(&mut clean).await;
    protocol::disconnect(&mut clean).await;
    stop(&app).await;
}

async fn finish_exchange(
    mut peer: Client,
    address: SocketAddr,
    name: &str,
    qos: u8,
    retain: bool,
    vectors: &Vectors,
) -> Client {
    if qos == 0 {
        drop(peer);
        let (mut peer, present) = Client::connect(address, name, false).await;
        assert!(present);
        ping_wire(&mut peer).await; // QoS0 must never be retried.
        return peer;
    }
    // Wrong-phase ACKs must leave the original reliable exchange intact.
    peer.send(vec![if qos == 1 { 0x50 } else { 0x40 }, 2, 0, 1])
        .await;
    peer.send(vec![0x70, 2, 0, 1]).await;
    ping_wire(&mut peer).await;
    drop(peer);
    let (mut peer, present) = Client::connect(address, name, false).await;
    assert!(present);
    expect_wire(
        &mut peer,
        &hex(&publish_vector(vectors, qos, retain, true).wire_hex),
    )
    .await;
    if qos == 1 {
        peer.send(vec![0x40, 2, 0, 1]).await;
    } else {
        for _ in 0..2 {
            peer.send(vec![0x50, 2, 0, 1]).await;
            expect_wire(&mut peer, &[0x62, 2, 0, 1]).await;
        }
        drop(peer);
        let (mut resumed, present) = Client::connect(address, name, false).await;
        assert!(present);
        expect_wire(&mut resumed, &[0x62, 2, 0, 1]).await;
        resumed.send(vec![0x40, 2, 0, 1]).await;
        ping_wire(&mut resumed).await;
        resumed.send(vec![0x70, 2, 0, 1]).await;
        peer = resumed;
    }
    ping_wire(&mut peer).await;
    peer
}

#[tokio::test]
async fn outgoing_live_retained_and_reconnected_qos_flows_match_literal_frames() {
    let vectors = vectors();
    for qos in 0..=2 {
        for retain in [false, true] {
            let (app, broker) = start(config()).await;
            let address = broker.local_addr().unwrap();
            let (mut reader, _) = Client::connect(address, "reader", false).await;
            reader.subscribe(&[("w", qos)]).await;
            let (mut writer, _) = Client::connect(address, "writer", true).await;
            writer
                .send(hex(&publish_vector(&vectors, qos, retain, false).wire_hex))
                .await;
            match qos {
                1 => expect_wire(&mut writer, &[0x40, 2, 0, 1]).await,
                2 => {
                    expect_wire(&mut writer, &[0x50, 2, 0, 1]).await;
                    writer.send(vec![0x62, 2, 0, 1]).await;
                    expect_wire(&mut writer, &[0x70, 2, 0, 1]).await;
                }
                _ => (),
            }
            expect_wire(
                &mut reader,
                &hex(&publish_vector(&vectors, qos, false, false).wire_hex),
            )
            .await;
            let mut reader = finish_exchange(reader, address, "reader", qos, false, &vectors).await;
            assert_eq!(broker.status().pending_messages, 0);
            if retain {
                let (mut late, _) = Client::connect(address, "late", false).await;
                late.subscribe(&[("w", qos)]).await;
                expect_wire(
                    &mut late,
                    &hex(&publish_vector(&vectors, qos, true, false).wire_hex),
                )
                .await;
                let mut late = finish_exchange(late, address, "late", qos, true, &vectors).await;
                protocol::disconnect(&mut late).await;
                assert_eq!(broker.status().pending_messages, 0);
            }
            protocol::disconnect(&mut reader).await;
            protocol::disconnect(&mut writer).await;
            stop(&app).await;
        }
    }
}

#[tokio::test]
async fn outgoing_publish_remaining_lengths_cross_boundaries_on_real_tcp() {
    let vectors = vectors();
    let mut cfg = config();
    cfg.max_packet_bytes = 32768;
    let (app, broker) = start(cfg).await;
    let address = broker.local_addr().unwrap();
    let (mut writer, _) = Client::connect(address, "writer", true).await;
    for (index, vector) in vectors.lengths.iter().enumerate() {
        // A new Session makes the first outbound Packet Identifier deterministic.
        let (mut reader, _) = Client::connect(address, &format!("reader-{index}"), true).await;
        reader.subscribe(&[("w", vector.qos)]).await;
        let mut expected = hex(&vector.prefix_hex);
        expected.extend(vec![0xa5; vector.payload_bytes]);
        writer.send(expected.clone()).await;
        match vector.qos {
            1 => expect_wire(&mut writer, &[0x40, 2, 0, 1]).await,
            2 => {
                expect_wire(&mut writer, &[0x50, 2, 0, 1]).await;
                writer.send(vec![0x62, 2, 0, 1]).await;
                expect_wire(&mut writer, &[0x70, 2, 0, 1]).await;
            }
            _ => (),
        }
        expect_wire(&mut reader, &expected).await;
        match vector.qos {
            1 => reader.send(vec![0x40, 2, 0, 1]).await,
            2 => {
                reader.send(vec![0x50, 2, 0, 1]).await;
                expect_wire(&mut reader, &[0x62, 2, 0, 1]).await;
                reader.send(vec![0x70, 2, 0, 1]).await;
            }
            _ => (),
        }
        ping_wire(&mut reader).await;
        protocol::disconnect(&mut reader).await;
    }
    protocol::disconnect(&mut writer).await;
    stop(&app).await;
}
