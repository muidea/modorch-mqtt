use super::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Tables {
    version: u8,
    packets: Vec<Row>,
    valid_connect_flags: Vec<u8>,
    remaining_length_cases: Vec<LengthCase>,
}

#[derive(Deserialize)]
struct LengthCase {
    value: usize,
    encoding: Vec<u8>,
}

#[derive(Deserialize)]
struct Row {
    #[serde(rename = "type")]
    kind: u8,
    name: String,
    client_to_server: bool,
    valid_flags: Vec<u8>,
    min_remaining: Option<usize>,
    exact_remaining: Option<usize>,
}

fn tables() -> Vec<Row> {
    let tables: Tables =
        serde_json::from_str(include_str!("../../../../contracts/mqtt311-packets.json")).unwrap();
    assert_eq!(tables.version, 1);
    assert_eq!(tables.packets.len(), 16);
    for (kind, row) in tables.packets.iter().enumerate() {
        assert_eq!(row.kind as usize, kind);
        assert_eq!(row.client_to_server, !row.valid_flags.is_empty());
    }
    tables.packets
}

fn body(header: u8) -> Vec<u8> {
    match header >> 4 {
        1 => {
            let mut bytes = string("MQTT");
            bytes.extend([4, 2, 0, 0]);
            bytes.extend(string("table-client"));
            bytes
        }
        3 => {
            let mut bytes = string("table/topic");
            if header & 6 != 0 {
                bytes.extend([0, 9]);
            }
            bytes
        }
        4..=7 => vec![0, 9],
        8 | 10 => {
            let mut bytes = vec![0, 9];
            bytes.extend(string("table/+"));
            if header >> 4 == 8 {
                bytes.push(1);
            }
            bytes
        }
        _ => vec![],
    }
}

#[test]
fn packet_type_direction_and_fixed_flags_cover_all_256_first_bytes() {
    for row in tables() {
        for flags in 0..16 {
            let header = row.kind << 4 | flags;
            let valid = row.valid_flags.contains(&flags);
            let bytes = codec::frame(header, &body(header));
            assert_eq!(
                codec::decode(&bytes, 8).is_ok(),
                valid,
                "decode: {} {flags}",
                row.name
            );
            if valid {
                assert_eq!(codec::frame_len(&[header], 4096).unwrap(), None);
                assert_eq!(codec::frame_len(&bytes, 4096).unwrap(), Some(bytes.len()));
            } else {
                // Fixed-header violations are known before any Remaining Length
                // byte or payload, so a peer cannot postpone rejection forever.
                assert!(
                    codec::frame_len(&[header], 4096).is_err(),
                    "header: {} {flags}",
                    row.name
                );
            }
        }
    }
}

#[test]
fn impossible_packet_lengths_and_non_exact_decoder_inputs_fail_before_body() {
    for row in tables().into_iter().filter(|r| r.client_to_server) {
        let header = row.kind << 4 | row.valid_flags[0];
        let minimum = row.min_remaining.unwrap();
        for remaining in 0..minimum {
            assert!(
                codec::frame_len(&[header, remaining as u8], 4096).is_err(),
                "short: {} {remaining}",
                row.name
            );
        }
        if let Some(exact) = row.exact_remaining {
            for remaining in exact + 1..=127 {
                assert!(
                    codec::frame_len(&[header, remaining as u8], 4096).is_err(),
                    "long: {} {remaining}",
                    row.name
                );
            }
        }
        let bytes = codec::frame(header, &body(header));
        assert!(
            codec::decode(&bytes[..bytes.len() - 1], 8).is_err(),
            "truncated: {}",
            row.name
        );
        let mut trailing = bytes;
        trailing.push(0);
        assert!(
            codec::decode(&trailing, 8).is_err(),
            "trailing: {}",
            row.name
        );
    }
    for header in [0x32, 0x34, 0x3a, 0x3c] {
        assert!(codec::frame_len(&[header, 4], 4096).is_err());
    }
}

#[test]
fn remaining_length_table_boundaries_are_incremental_without_large_allocations() {
    let fixture: Tables =
        serde_json::from_str(include_str!("../../../../contracts/mqtt311-packets.json")).unwrap();
    assert_eq!(fixture.remaining_length_cases.len(), 8);
    for case in fixture.remaining_length_cases {
        let mut prefix = vec![if case.value == 0 { 0xc0 } else { 0x30 }];
        prefix.extend(case.encoding);
        for end in 0..prefix.len() {
            assert_eq!(codec::frame_len(&prefix[..end], usize::MAX).unwrap(), None);
        }
        let total = case.value + prefix.len();
        assert_eq!(
            codec::frame_len(&prefix, total).unwrap(),
            (case.value == 0).then_some(total)
        );
        assert!(matches!(
            codec::frame_len(&prefix, total - 1),
            Err(MqttError::Capacity(_))
        ));
    }
    for bytes in [
        vec![0x30, 0xff, 0xff, 0xff, 0x80],
        vec![0x30, 0x80, 0x80, 0x80, 0x80, 0],
        vec![0x30, 0x83, 0],
    ] {
        assert!(codec::frame_len(&bytes, usize::MAX).is_err());
    }
}

#[test]
fn packet_identifier_table_enforces_nonzero_ids_and_qos0_payload_remains_binary() {
    for header in [0x32, 0x34, 0x40, 0x50, 0x62, 0x70, 0x82, 0xa2] {
        let original = body(header);
        let offset = if header >> 4 == 3 {
            2 + "table/topic".len()
        } else {
            0
        };
        for id in [0u16, 1, u16::MAX] {
            let mut bytes = original.clone();
            bytes[offset..offset + 2].copy_from_slice(&id.to_be_bytes());
            assert_eq!(
                codec::decode(&codec::frame(header, &bytes), 8).is_ok(),
                id != 0
            );
        }
    }
    let mut bytes = body(0x30);
    bytes.extend([0, 0, 0xff]);
    let codec::Packet::Publish { packet_id, message } =
        codec::decode(&codec::frame(0x30, &bytes), 8).unwrap()
    else {
        panic!("PUBLISH expected");
    };
    assert!(packet_id.is_none());
    assert_eq!(message.payload.as_ref(), &[0, 0, 0xff]);
}

#[tokio::test]
async fn all_256_fixed_headers_preserve_healthy_connections_and_expected_wire_responses() {
    let mut cfg = config();
    cfg.max_connection_attempts_per_ip = 512;
    let (app, broker) = start(cfg).await;
    let generation = broker.status().generation;
    let addr = broker.local_addr().unwrap();
    let (mut healthy, _) = Client::connect(addr, "healthy", true).await;
    for row in tables() {
        for flags in 0..16 {
            let header = row.kind << 4 | flags;
            let (mut peer, _) = Client::connect(addr, "table-peer", true).await;
            peer.send(codec::frame(header, &body(header))).await;
            if !row.valid_flags.contains(&flags) || matches!(row.kind, 1 | 14) {
                // A second CONNECT closes even though its syntax is legal;
                // legal DISCONNECT closes without a response.
                protocol::closed(&mut peer).await;
            } else {
                match row.kind {
                    3 => match (flags >> 1) & 3 {
                        0 => (),
                        1 => assert_eq!(peer.recv().await, (0x40, vec![0, 9])),
                        2 => {
                            assert_eq!(peer.recv().await, (0x50, vec![0, 9]));
                            qos2::release(&mut peer, 9).await;
                        }
                        _ => unreachable!(),
                    },
                    6 => assert_eq!(peer.recv().await, (0x70, vec![0, 9])),
                    8 => assert_eq!(peer.recv().await, (0x90, vec![0, 9, 1])),
                    10 => assert_eq!(peer.recv().await, (0xb0, vec![0, 9])),
                    12 => assert_eq!(peer.recv().await, (0xd0, vec![])),
                    _ => (), // Unknown acknowledgements have no exchange to advance.
                }
                qos2::ping(&mut peer).await;
                protocol::disconnect(&mut peer).await;
            }
            qos2::ping(&mut healthy).await;
            assert_eq!(
                broker.status().generation,
                generation,
                "{} {flags}",
                row.name
            );
        }
    }
    stop(&app).await;
}

#[tokio::test]
async fn illegal_header_or_impossible_length_closes_without_waiting_for_advertised_body() {
    let (app, broker) = start(config()).await;
    let addr = broker.local_addr().unwrap();
    let (mut healthy, _) = Client::connect(addr, "healthy", true).await;
    for bytes in [
        vec![0x00],
        vec![0xf0],
        vec![0x20],
        vec![0x81],
        vec![0x38],
        vec![0x36],
        vec![0xc0, 1],
        vec![0xe0, 1],
        vec![0x40, 3],
        vec![0x50, 3],
        vec![0x62, 3],
        vec![0x70, 3],
        vec![0x82, 5],
        vec![0xa2, 4],
        vec![0x32, 4],
        vec![0x34, 4],
        vec![0x10, 11],
    ] {
        let (mut peer, _) = Client::connect(addr, "incomplete", true).await;
        peer.send(bytes).await;
        protocol::closed(&mut peer).await;
        qos2::ping(&mut healthy).await;
    }
    stop(&app).await;
}

#[tokio::test]
async fn all_256_connect_flags_validate_payload_presence_and_wire_rejection() {
    let fixture: Tables =
        serde_json::from_str(include_str!("../../../../contracts/mqtt311-packets.json")).unwrap();
    assert_eq!(fixture.valid_connect_flags.len(), 42);
    let mut cfg = config();
    cfg.max_connection_attempts_per_ip = 512;
    let (app, broker) = start(cfg).await;
    let addr = broker.local_addr().unwrap();
    let (mut healthy, _) = Client::connect(addr, "healthy", true).await;
    for flags in 0..=255u8 {
        let mut bytes = string("MQTT");
        bytes.extend([4, flags, 0, 0]);
        bytes.extend(string("connect-flags"));
        if flags & 4 != 0 {
            bytes.extend(string("flags/will"));
            bytes.extend([0, 2, 0, 0xff]); // Will payload is binary.
        }
        if flags & 128 != 0 {
            bytes.extend(string("user"));
        }
        if flags & 64 != 0 {
            bytes.extend([0, 2, 0, 0xff]); // Password is binary too.
        }
        let packet = codec::frame(0x10, &bytes);
        let valid = fixture.valid_connect_flags.contains(&flags);
        assert_eq!(codec::decode(&packet, 8).is_ok(), valid, "flags {flags}");
        let mut peer = Client(TcpStream::connect(addr).await.unwrap());
        peer.send(packet).await;
        if !valid {
            protocol::closed(&mut peer).await;
        } else if flags & 128 != 0 {
            // Anonymous mode refuses supplied credentials; syntax remains valid.
            assert_eq!(peer.recv().await, (0x20, vec![0, 5]), "flags {flags}");
            protocol::closed(&mut peer).await;
        } else {
            let (header, reply) = peer.recv().await;
            assert_eq!(header, 0x20);
            assert_eq!(reply.len(), 2);
            assert_eq!(reply[0] & 0xfe, 0, "reserved CONNACK flags");
            assert_eq!(reply[1], 0);
            qos2::ping(&mut peer).await;
            protocol::disconnect(&mut peer).await;
        }
        qos2::ping(&mut healthy).await;
        assert_eq!(broker.status().will_records, 0);
    }
    stop(&app).await;
}

#[tokio::test]
async fn connack_reserved_flags_are_zero_for_new_resumed_clean_and_rejected_sessions() {
    let (app, broker) = start(config()).await;
    let addr = broker.local_addr().unwrap();
    for (clean, expected) in [(false, 0), (false, 1), (true, 0)] {
        let mut bytes = body(0x10);
        bytes[7] = u8::from(clean) * 2;
        let mut peer = Client(TcpStream::connect(addr).await.unwrap());
        peer.send(codec::frame(0x10, &bytes)).await;
        assert_eq!(peer.recv().await, (0x20, vec![expected, 0]));
        protocol::disconnect(&mut peer).await;
    }
    let mut bytes = body(0x10);
    bytes[6] = 5;
    let mut peer = Client(TcpStream::connect(addr).await.unwrap());
    peer.send(codec::frame(0x10, &bytes)).await;
    assert_eq!(peer.recv().await, (0x20, vec![0, 1]));
    protocol::closed(&mut peer).await;
    stop(&app).await;
}

#[tokio::test]
async fn invalid_partial_header_triggers_will_and_preserves_persistent_qos2_session() {
    let (app, broker) = start(config()).await;
    let addr = broker.local_addr().unwrap();
    let (mut healthy, _) = Client::connect(addr, "healthy", true).await;
    healthy.subscribe(&[("violation/will", 1)]).await;
    let (mut peer, _) = Client::connect_will(
        addr,
        "persistent",
        false,
        Some(("violation/will", b"closed", false)),
        0,
    )
    .await;
    peer.subscribe(&[("persistent/+", 1)]).await;
    qos2::publish_start(&mut peer, 17, "source", b"once", false, false).await;
    peer.send(vec![0x81]).await;
    protocol::closed(&mut peer).await;
    let (topic, payload, header, id) = healthy.message().await;
    assert_eq!(
        (topic.as_str(), payload.as_slice(), header),
        ("violation/will", b"closed".as_slice(), 0x32)
    );
    healthy.ack(id.unwrap()).await;
    let (mut peer, present) = Client::connect(addr, "persistent", false).await;
    assert!(present);
    assert_eq!(broker.status().incoming_qos2, 1);
    qos2::publish_start(&mut peer, 17, "source", b"once", true, false).await;
    assert_eq!(broker.status().received_publications, 1);
    qos2::release(&mut peer, 17).await;
    healthy
        .publish("persistent/alive", b"resumed", 1, false)
        .await;
    let (_, payload, _, id) = peer.message().await;
    assert_eq!(payload, b"resumed");
    peer.ack(id.unwrap()).await;
    protocol::disconnect(&mut peer).await;
    stop(&app).await;
}
