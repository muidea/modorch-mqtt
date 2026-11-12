//! Protocol expectations come from OASIS MQTT5 tables, not rumqttc's encoder.
use super::{mqtt5::*, *};

const CLIENT_DISCONNECT_REASONS: &[u8] = &[
    0, 4, 0x80, 0x81, 0x82, 0x83, 0x90, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99,
];

pub(super) async fn receive_reliable(c: &mut Client, payload: &[u8], qos: u8) {
    let (h, b) = c.recv().await;
    let p = decoded(h, &b);
    assert_eq!(p.payload, payload);
    assert_eq!((h >> 1) & 3, qos);
    c.send(codec::frame(
        if qos == 2 { 0x50 } else { 0x40 },
        &p.pkid.to_be_bytes(),
    ))
    .await;
    if qos == 2 {
        assert_eq!(c.recv().await, (0x62, p.pkid.to_be_bytes().to_vec()));
        c.send(codec::frame(0x70, &p.pkid.to_be_bytes())).await;
    }
}

#[tokio::test]
async fn mqtt5_shared_offline_single_member_preserves_reliable_messages_and_options() {
    let (app, broker) = start(MqttBrokerConfig {
        mqtt5: true,
        ..config()
    })
    .await;
    let addr = broker.local_addr().unwrap();
    let (mut sink, _) = connect(addr, "offline", true, 60, &[], None).await;
    sub(&mut sink, &[("$share/g/jobs", 0x0a)], Some(17)).await;
    sink.send(vec![0xe0, 0]).await;
    assert_eq!(sink.0.read(&mut [0]).await.unwrap(), 0);
    let (mut source, _) = connect(addr, "source", true, 0, &[], None).await;
    publish(&mut source, "jobs", b"best-effort", 0, &[], false).await;
    publish(&mut source, "jobs", b"one", 1, &[], true).await;
    publish(&mut source, "jobs", b"two", 2, &[], false).await;
    let (mut sink, present) = connect(addr, "offline", false, 60, &[], None).await;
    assert!(present);
    let (h, b) = sink.recv().await;
    let p = decoded(h, &b);
    assert_eq!(p.payload, b"one"[..]);
    assert!(p.retain);
    assert_eq!(p.properties.unwrap().subscription_identifiers, vec![17]);
    sink.send(codec::frame(0x40, &p.pkid.to_be_bytes())).await;
    receive_reliable(&mut sink, b"two", 2).await;
    // This reliable FIFO barrier also proves QoS0 wasn't queued or Retained replayed.
    publish(&mut source, "jobs", b"barrier", 1, &[], false).await;
    receive_reliable(&mut sink, b"barrier", 1).await;
    stop(&app).await;
}

#[tokio::test]
async fn mqtt5_shared_all_offline_selects_one_member_and_prefers_online_members() {
    let (app, broker) = start(MqttBrokerConfig {
        mqtt5: true,
        ..config()
    })
    .await;
    let addr = broker.local_addr().unwrap();
    for id in ["a", "b"] {
        let (mut c, _) = connect(addr, id, true, 60, &[], None).await;
        sub(&mut c, &[("$share/g/jobs", 2)], None).await;
        c.send(vec![0xe0, 0]).await;
        assert_eq!(c.0.read(&mut [0]).await.unwrap(), 0);
    }
    let (mut source, _) = connect(addr, "source", true, 0, &[], None).await;
    publish(&mut source, "jobs", b"a-only", 1, &[], false).await;
    publish(&mut source, "jobs", b"b-only", 2, &[], false).await;
    let (mut b, present) = connect(addr, "b", false, 60, &[], None).await;
    assert!(present);
    receive_reliable(&mut b, b"b-only", 2).await;
    publish(&mut source, "jobs", b"online", 1, &[], false).await;
    receive_reliable(&mut b, b"online", 1).await;
    let (mut a, present) = connect(addr, "a", false, 60, &[], None).await;
    assert!(present);
    receive_reliable(&mut a, b"a-only", 1).await;
    // An ordinary subscription also receives a publication assigned to b.
    sub(&mut a, &[("jobs", 2)], None).await;
    publish(&mut source, "jobs", b"merged", 1, &[], false).await;
    receive_reliable(&mut a, b"merged", 1).await;
    receive_reliable(&mut b, b"merged", 1).await;
    // Next rotation selects a: its ordinary and shared matches coalesce.
    publish(&mut source, "jobs", b"coalesced", 1, &[], false).await;
    receive_reliable(&mut a, b"coalesced", 1).await;
    for c in [&mut a, &mut b] {
        c.send(vec![0xc0, 0]).await;
        assert_eq!(c.recv().await, (0xd0, vec![]));
    }
    stop(&app).await;
}

#[tokio::test]
async fn mqtt5_shared_offline_full_backlog_rejects_atomically_and_clean_start_discards() {
    let (app, broker) = start(MqttBrokerConfig {
        mqtt5: true,
        max_offline_messages_per_client: 1,
        max_inflight_per_client: 1,
        ..config()
    })
    .await;
    let addr = broker.local_addr().unwrap();
    let (mut sink, _) = connect(addr, "offline", true, 60, &[], None).await;
    sub(&mut sink, &[("$share/g/jobs", 2)], None).await;
    sink.send(vec![0xe0, 0]).await;
    assert_eq!(sink.0.read(&mut [0]).await.unwrap(), 0);
    let (mut source, _) = connect(addr, "source", true, 0, &[], None).await;
    publish(&mut source, "jobs", b"accepted", 1, &[], true).await;
    let mut body = string("jobs");
    body.extend([0, 9, 0]);
    body.extend(b"rejected");
    source.send(codec::frame(0x33, &body)).await;
    assert_eq!(source.0.read(&mut [0]).await.unwrap(), 0);
    let (mut late, _) = connect(addr, "late", true, 0, &[], None).await;
    sub(&mut late, &[("jobs", 1)], None).await;
    receive_reliable(&mut late, b"accepted", 1).await;
    let (mut sink, present) = connect(addr, "offline", true, 60, &[], None).await;
    assert!(!present);
    sub(&mut sink, &[("$share/g/jobs", 2)], None).await;
    let (mut source, _) = connect(addr, "source", true, 0, &[], None).await;
    publish(&mut source, "jobs", b"barrier", 1, &[], false).await;
    receive_reliable(&mut sink, b"barrier", 1).await;
    stop(&app).await;
}

#[test]
fn mqtt5_ack_and_disconnect_reason_tables_enforce_packet_kind_and_sender_role() {
    for header in [0x40, 0x50, 0x62, 0x70, 0xe0] {
        for reason in 0..=255 {
            let allowed = match header {
                0x40 | 0x50 => {
                    // Tables3-4/3-5:0x10 is sent only by the Server.
                    [0, 0x80, 0x83, 0x87, 0x90, 0x91, 0x97, 0x99].contains(&reason)
                }
                0x62 | 0x70 => [0, 0x92].contains(&reason),
                _ => CLIENT_DISCONNECT_REASONS.contains(&reason),
            };
            // Exercise both legal shortened form and explicit zero Properties.
            for explicit in [false, true] {
                let mut body = if header == 0xe0 { vec![] } else { vec![0, 7] };
                body.push(reason);
                if explicit {
                    body.push(0);
                }
                let mut wire = crate::mqtt5::Codec::default();
                let result = wire.decode(&codec::frame(header, &body), 8);
                assert_eq!(
                    result.is_ok(),
                    allowed,
                    "{header:02x}/{reason:02x}/{explicit}"
                );
                if allowed && header == 0xe0 {
                    assert_eq!(wire.disconnect_will, reason != 0);
                }
            }
        }
    }
}

#[tokio::test]
async fn mqtt5_disconnect_nonzero_reasons_publish_will_and_normal_disconnect_cancels() {
    let (app, broker) = start(MqttBrokerConfig {
        mqtt5: true,
        ..config()
    })
    .await;
    let address = broker.local_addr().unwrap();
    let (mut sink, _) = connect(address, "sink", true, 0, &[], None).await;
    sub(&mut sink, &[("v5/#", 1)], None).await;
    let (mut source, _) = connect(address, "source", true, 0, &[], None).await;
    for (reason, explicit_properties) in CLIENT_DISCONNECT_REASONS
        .iter()
        .flat_map(|reason| [(reason, false), (reason, true)])
    {
        let (mut owner, _) = connect(address, "will-owner", true, 0, &[], Some((0, 30))).await;
        let body = if explicit_properties {
            vec![*reason, 0]
        } else {
            vec![*reason]
        };
        owner.send(codec::frame(0xe0, &body)).await;
        assert_eq!(owner.0.read(&mut [0]).await.unwrap(), 0);
        // A matching barrier verifies absence for normal DISCONNECT without a
        // timing-only negative assertion; EOF follows the detach transaction.
        publish(&mut source, "v5/barrier", &[*reason], 1, &[], false).await;
        if *reason != 0 {
            let (h, b) = sink.recv().await;
            let will = decoded(h, &b);
            assert_eq!(will.topic, b"v5/will"[..], "reason {reason:02x}");
            assert_eq!(will.payload, b"w"[..]);
            sink.send(codec::frame(0x40, &will.pkid.to_be_bytes()))
                .await;
        }
        let (h, b) = sink.recv().await;
        let barrier = decoded(h, &b);
        assert_eq!(barrier.topic, b"v5/barrier"[..]);
        assert_eq!(barrier.payload.as_ref(), &[*reason]);
        sink.send(codec::frame(0x40, &barrier.pkid.to_be_bytes()))
            .await;
        sink.send(vec![0xc0, 0]).await;
        assert_eq!(sink.recv().await, (0xd0, vec![]));
        assert_eq!(broker.status().will_records, 0);
    }
    stop(&app).await;
}

#[tokio::test]
async fn mqtt5_server_only_disconnect_is_abnormal_and_does_not_suppress_will() {
    let (app, broker) = start(MqttBrokerConfig {
        mqtt5: true,
        ..config()
    })
    .await;
    let address = broker.local_addr().unwrap();
    let (mut sink, _) = connect(address, "sink", true, 0, &[], None).await;
    sub(&mut sink, &[("v5/will", 1)], None).await;
    for reason in [
        0x87, 0x89, 0x8b, 0x8d, 0x8e, 0x8f, 0x9a, 0x9b, 0x9c, 0x9d, 0x9e, 0x9f, 0xa0, 0xa1, 0xa2,
    ] {
        let (mut owner, _) = connect(address, "invalid-owner", true, 0, &[], Some((0, 30))).await;
        owner.send(codec::frame(0xe0, &[reason, 0])).await;
        assert_eq!(owner.0.read(&mut [0]).await.unwrap(), 0);
        let (h, b) = sink.recv().await;
        let will = decoded(h, &b);
        assert_eq!(will.payload, b"w"[..]);
        sink.send(codec::frame(0x40, &will.pkid.to_be_bytes()))
            .await;
    }
    stop(&app).await;
}

#[tokio::test]
async fn mqtt5_unsubscribe_duplicate_filters_report_sequential_results_and_keep_inflight() {
    let (app, broker) = start(MqttBrokerConfig {
        mqtt5: true,
        ..config()
    })
    .await;
    let address = broker.local_addr().unwrap();
    let (mut sink, _) = connect(address, "sink", true, 0, &[], None).await;
    sub(
        &mut sink,
        &[("v5/#", 2), ("v5/+", 1), ("barrier", 1)],
        Some(17),
    )
    .await;
    let (mut source, _) = connect(address, "source", true, 0, &[], None).await;
    publish(&mut source, "v5/owned", b"owned", 2, &[], false).await;
    let (h, b) = sink.recv().await;
    let owned = decoded(h, &b);
    let mut body = vec![0xff, 0xff, 0];
    for filter in ["v5/#", "missing", "v5/#", "v5/+", "v5/+"] {
        body.extend(string(filter));
    }
    sink.send(codec::frame(0xa2, &body)).await;
    assert_eq!(
        sink.recv().await,
        (0xb0, vec![0xff, 0xff, 0, 0, 0x11, 0x11, 0, 0x11])
    );
    // Removing subscriptions must preserve already started QoS2 ownership.
    sink.send(codec::frame(0x50, &owned.pkid.to_be_bytes()))
        .await;
    assert_eq!(sink.recv().await, (0x62, owned.pkid.to_be_bytes().to_vec()));
    sink.send(codec::frame(0x70, &owned.pkid.to_be_bytes()))
        .await;
    publish(&mut source, "v5/new", b"filtered", 1, &[], false).await;
    publish(&mut source, "barrier", b"next", 1, &[], false).await;
    let (h, b) = sink.recv().await;
    let barrier = decoded(h, &b);
    assert_eq!(barrier.topic, b"barrier"[..]);
    sink.send(codec::frame(0x40, &barrier.pkid.to_be_bytes()))
        .await;
    stop(&app).await;
}

#[tokio::test]
async fn mqtt5_reconnected_pubrel_does_not_consume_fresh_receive_quota_or_replenish_it_twice() {
    let (app, broker) = start(MqttBrokerConfig {
        mqtt5: true,
        ..config()
    })
    .await;
    let addr = broker.local_addr().unwrap();
    let (mut sink, _) = connect(addr, "quota-sink", false, 60, &[0x21, 0, 1], None).await;
    sub(&mut sink, &[("quota/#", 2)], None).await;
    let (mut source, _) = connect(addr, "quota-source", true, 0, &[], None).await;
    publish(&mut source, "quota/old", b"old", 2, &[], false).await;
    let (h, b) = sink.recv().await;
    let old = decoded(h, &b);
    sink.send(codec::frame(0x50, &old.pkid.to_be_bytes())).await;
    assert_eq!(sink.recv().await, (0x62, old.pkid.to_be_bytes().to_vec()));
    drop(sink);
    let (mut sink, present) = connect(addr, "quota-sink", false, 60, &[0x21, 0, 1], None).await;
    assert!(present);
    assert_eq!(sink.recv().await, (0x62, old.pkid.to_be_bytes().to_vec()));
    publish(&mut source, "quota/new", b"new", 1, &[], false).await;
    // A retransmitted PUBREL is not a new PUBLISH and consumes no new quota.
    let (h, b) = sink.recv().await;
    let new = decoded(h, &b);
    assert_eq!(new.payload, b"new"[..]);
    assert_ne!(new.pkid, old.pkid);
    sink.send(codec::frame(0x70, &old.pkid.to_be_bytes())).await;
    // Completion of old PUBREL must not grant a second slot on this connection.
    publish(&mut source, "quota/third", b"third", 1, &[], false).await;
    sink.send(vec![0xc0, 0]).await;
    assert_eq!(sink.recv().await, (0xd0, vec![]));
    sink.send(codec::frame(0x40, &new.pkid.to_be_bytes())).await;
    let (h, b) = sink.recv().await;
    let third = decoded(h, &b);
    assert_eq!(third.payload, b"third"[..]);
    sink.send(codec::frame(0x40, &third.pkid.to_be_bytes()))
        .await;
    stop(&app).await;
}

#[test]
fn mqtt5_all_256_connect_flags_preserve_field_order_and_binary_password() {
    for flags in 0..=255u8 {
        let mut b = string("MQTT");
        b.extend([5, flags, 0, 0, 0]);
        b.extend(string("0123456789abcdefghijklm"));
        if flags & 4 != 0 {
            b.push(0);
            b.extend(string("will"));
            b.extend([0, 1, 0xff]);
        }
        if flags & 128 != 0 {
            b.extend(string("user"));
        }
        if flags & 64 != 0 {
            b.extend([0, 2, 0xff, 0]);
        }
        let legal = flags & 1 == 0 && (flags & 4 != 0 || flags & 0x38 == 0) && flags & 0x18 != 0x18;
        let mut wire = crate::mqtt5::Codec::default();
        let decoded = wire.decode(&codec::frame(0x10, &b), 8);
        assert_eq!(decoded.is_ok(), legal, "flags {flags:02x}");
        if let Ok(codec::Packet::Connect(c)) = decoded {
            assert_eq!(c.username.as_deref(), (flags & 128 != 0).then_some("user"));
            assert_eq!(
                c.password.as_deref(),
                (flags & 64 != 0).then_some(&[0xff, 0][..])
            );
            assert_eq!(c.will.is_some(), flags & 4 != 0);
        }
    }
}

#[test]
fn mqtt5_all_256_fixed_headers_follow_direction_flags_and_exact_frame_boundaries() {
    for first in 0..=255u8 {
        let kind = first >> 4;
        let low = first & 15;
        let mut b = match kind {
            1 => {
                let mut b = string("MQTT");
                b.extend([5, 2, 0, 0, 0]);
                b.extend(string("c"));
                b
            }
            3 => {
                let mut b = string("t");
                if first & 6 != 0 {
                    b.extend([0, 1]);
                }
                b.extend([0, 0xff]);
                b
            }
            4..=7 => vec![0, 1],
            8 => vec![0, 1, 0, 0, 1, b't', 0],
            10 => vec![0, 1, 0, 0, 1, b't'],
            _ => vec![],
        };
        let allowed = match kind {
            1 | 4 | 5 | 7 | 12 | 14 => low == 0,
            6 | 8 | 10 => low == 2,
            3 => low & 6 != 6 && (low & 6 != 0 || low & 8 == 0),
            _ => false,
        };
        let frame = codec::frame(first, &b);
        let framed = codec::frame_len_version(&frame, 8192, true);
        // AUTH has a structurally valid header even though this Broker does
        // not implement Enhanced Authentication and rejects its body.
        assert_eq!(
            framed.is_ok(),
            allowed || first == 0xf0,
            "header {first:02x}"
        );
        if !allowed {
            continue;
        }
        assert_eq!(framed.unwrap(), Some(frame.len()));
        assert!(
            crate::mqtt5::Codec::default().decode(&frame, 8).is_ok(),
            "header {first:02x}"
        );
        b.push(0);
        // ACK and PUBLISH can legally grow; PING/CONNECT/filter lists cannot.
        if [1, 8, 10, 12, 14].contains(&kind) && kind != 14 {
            assert!(
                crate::mqtt5::Codec::default()
                    .decode(&codec::frame(first, &b), 8)
                    .is_err()
            );
        }
        assert!(
            crate::mqtt5::Codec::default()
                .decode(&frame[..frame.len() - 1], 8)
                .is_err()
        );
    }
}

#[test]
fn mqtt5_outbound_control_frames_match_independent_literal_layouts() {
    let wire = crate::mqtt5::Codec::default();
    for (input, expected) in [
        (
            vec![0x20, 2, 0, 0],
            vec![
                0x20, 14, 0, 0, 11, 0x21, 0, 32, 0x22, 0, 32, 0x27, 0, 0, 0x20, 0,
            ],
        ),
        (vec![0x20, 2, 0, 2], vec![0x20, 3, 0, 0x85, 0]),
        (
            vec![0x90, 5, 0, 9, 0, 2, 0x80],
            vec![0x90, 6, 0, 9, 0, 0, 2, 0x87],
        ),
        (
            vec![0xb0, 4, 0, 9, 0, 0x11],
            vec![0xb0, 5, 0, 9, 0, 0, 0x11],
        ),
        (vec![0x40, 2, 0xff, 0xff], vec![0x40, 2, 0xff, 0xff]),
        (vec![0x50, 2, 0, 1], vec![0x50, 2, 0, 1]),
        (vec![0x62, 2, 0, 1], vec![0x62, 2, 0, 1]),
        (vec![0x70, 2, 0, 1], vec![0x70, 2, 0, 1]),
        (vec![0xd0, 0], vec![0xd0, 0]),
    ] {
        assert_eq!(wire.response(input, None, 8192, 32), expected);
    }
}

#[tokio::test]
async fn mqtt5_invalid_subscribe_payload_and_options_close_only_offender() {
    let (app, broker) = start(MqttBrokerConfig {
        mqtt5: true,
        ..config()
    })
    .await;
    let addr = broker.local_addr().unwrap();
    let (mut sink, _) = connect(addr, "valid-sink", true, 0, &[], None).await;
    let cases: Vec<Vec<u8>> = vec![
        vec![0, 1, 0],
        [&[0, 1, 0, 0, 1, 0xff, 0][..]].concat(),
        [&[0, 1, 0, 0, 5, b'/', b'#', b'/', b'b', 0][..]].concat(),
        [&[0, 1, 0, 0, 2, b'o', b'k', 3][..]].concat(),
        [&[0, 1, 0, 0, 2, b'o', b'k', 0x40][..]].concat(),
        [&[0, 1, 0, 0, 2, b'o', b'k', 0x30][..]].concat(),
        [&[
            0, 1, 0, 0, 11, b'$', b's', b'h', b'a', b'r', b'e', b'/', b'g', b'/', b'a', 0x04,
        ][..]]
        .concat(),
    ];
    for (index, body) in cases.iter().enumerate() {
        let (mut offender, _) =
            connect(addr, &format!("invalid-{index}"), true, 0, &[], None).await;
        offender.send(codec::frame(0x82, body)).await;
        let first = offender.0.read(&mut [0]).await.unwrap();
        assert!(
            first == 0 || first == 0xe0,
            "invalid SUBSCRIBE {index} was accepted"
        );
        sink.send(vec![0xc0, 0]).await;
        assert_eq!(sink.recv().await, (0xd0, vec![]));
    }
    stop(&app).await;
}

#[tokio::test]
async fn mqtt5_multi_filter_subscribe_reports_codes_and_replaces_duplicate() {
    let (app, broker) = start(MqttBrokerConfig {
        mqtt5: true,
        ..config()
    })
    .await;
    let addr = broker.local_addr().unwrap();
    let (mut sink, _) = connect(addr, "multi-sink", true, 0, &[], None).await;
    let mut body = vec![0, 1, 2, 0x0b, 7];
    for (filter, option) in [("multi/#", 0), ("multi/#", 2), ("$modorch/blocked", 1)] {
        body.extend(string(filter));
        body.push(option);
    }
    sink.send(codec::frame(0x82, &body)).await;
    assert_eq!(sink.recv().await, (0x90, vec![0, 1, 0, 0, 2, 0x87]));
    let (mut source, _) = connect(addr, "multi-source", true, 0, &[], None).await;
    publish(&mut source, "multi/item", b"replaced", 2, &[], false).await;
    let (h, b) = sink.recv().await;
    let p = decoded(h, &b);
    assert_eq!(p.qos, rumqttc::v5::mqttbytes::QoS::ExactlyOnce);
    assert_eq!(p.payload, b"replaced"[..]);
    assert_eq!(p.properties.unwrap().subscription_identifiers, vec![7]);
    let id = p.pkid.to_be_bytes();
    sink.send(codec::frame(0x50, &id)).await;
    assert_eq!(sink.recv().await, (0x62, id.to_vec()));
    sink.send(codec::frame(0x70, &id)).await;
    stop(&app).await;
}

#[tokio::test]
async fn mqtt5_overlapping_subscriptions_use_max_qos_and_report_matching_identifiers() {
    let (app, broker) = start(MqttBrokerConfig {
        mqtt5: true,
        ..config()
    })
    .await;
    let addr = broker.local_addr().unwrap();
    let (mut max2, _) = connect(addr, "ov-max2", true, 0, &[], None).await;
    sub(&mut max2, &[("ov2/#", 0)], Some(7)).await;
    sub(&mut max2, &[("ov2/item", 2)], Some(9)).await;
    let (mut max1, _) = connect(addr, "ov-max1", true, 0, &[], None).await;
    sub(&mut max1, &[("ov1/#", 0)], Some(10)).await;
    sub(&mut max1, &[("ov1/+", 1)], Some(11)).await;
    let (mut max0, _) = connect(addr, "ov-max0", true, 0, &[], None).await;
    sub(&mut max0, &[("ov0/#", 0)], Some(12)).await;
    let (mut source, _) = connect(addr, "ov-source", true, 0, &[], None).await;

    publish(&mut source, "ov2/item", b"two", 2, &[], false).await;
    let (h, b) = max2.recv().await;
    let p = decoded(h, &b);
    assert_eq!(p.qos, rumqttc::v5::mqttbytes::QoS::ExactlyOnce);
    assert_eq!(p.payload, b"two"[..]);
    let mut ids = p.properties.unwrap().subscription_identifiers;
    ids.sort();
    assert_eq!(ids, vec![7, 9]);
    let id = p.pkid.to_be_bytes();
    max2.send(codec::frame(0x50, &id)).await;
    assert_eq!(max2.recv().await, (0x62, id.to_vec()));
    max2.send(codec::frame(0x70, &id)).await;

    publish(&mut source, "ov1/item", b"one", 2, &[], false).await;
    let (h, b) = max1.recv().await;
    let p = decoded(h, &b);
    assert_eq!(p.qos, rumqttc::v5::mqttbytes::QoS::AtLeastOnce);
    assert_eq!(p.payload, b"one"[..]);
    let mut ids = p.properties.unwrap().subscription_identifiers;
    ids.sort();
    assert_eq!(ids, vec![10, 11]);
    max1.send(codec::frame(0x40, &p.pkid.to_be_bytes())).await;

    publish(&mut source, "ov0/item", b"zero", 2, &[], false).await;
    let (h, b) = max0.recv().await;
    let p = decoded(h, &b);
    assert_eq!(p.qos, rumqttc::v5::mqttbytes::QoS::AtMostOnce);
    assert_eq!(p.payload, b"zero"[..]);
    assert_eq!(p.properties.unwrap().subscription_identifiers, vec![12]);

    stop(&app).await;
}

#[tokio::test]
async fn mqtt5_retained_handling_replacement_overlap_identifiers_and_shared_suppression() {
    let (app, broker) = start(MqttBrokerConfig {
        mqtt5: true,
        ..config()
    })
    .await;
    let addr = broker.local_addr().unwrap();
    let (mut source, _) = connect(addr, "rh-source", true, 0, &[], None).await;
    publish(&mut source, "rh/t", b"retained", 1, &[], true).await;
    let (mut sink, _) = connect(addr, "rh-sink", true, 0, &[], None).await;
    for (options, replay) in [(0x11, true), (0x11, false), (0x21, false), (1, true)] {
        sub(&mut sink, &[("rh/#", options)], Some(7)).await;
        if replay {
            let (h, b) = sink.recv().await;
            let m = decoded(h, &b);
            assert_eq!(m.payload, b"retained"[..]);
            assert!(m.retain);
            assert_eq!(m.properties.unwrap().subscription_identifiers, vec![7]);
            sink.send(codec::frame(0x40, &m.pkid.to_be_bytes())).await;
        }
        sink.send(vec![0xc0, 0]).await;
        assert_eq!(sink.recv().await, (0xd0, vec![]));
    }
    // The second matching filter has its own ID; suppress retained replay.
    sub(&mut sink, &[("rh/t", 0x21)], Some(9)).await;
    publish(&mut source, "rh/t", b"live", 1, &[], false).await;
    let (h, b) = sink.recv().await;
    let m = decoded(h, &b);
    let mut ids = m.properties.unwrap().subscription_identifiers;
    ids.sort();
    assert_eq!(ids, vec![7, 9]);
    assert!(!m.retain);
    sink.send(codec::frame(0x40, &m.pkid.to_be_bytes())).await;
    let (mut shared, _) = connect(addr, "rh-shared", true, 0, &[], None).await;
    sub(&mut shared, &[("$share/g/rh/#", 1)], None).await;
    shared.send(vec![0xc0, 0]).await;
    assert_eq!(shared.recv().await, (0xd0, vec![]));
    stop(&app).await;
}

#[tokio::test]
async fn mqtt5_empty_disconnect_cancels_will_and_preserves_the_session() {
    let (app, broker) = start(MqttBrokerConfig {
        mqtt5: true,
        ..config()
    })
    .await;
    let addr = broker.local_addr().unwrap();
    let (mut sink, _) = connect(addr, "empty-sink", true, 0, &[], None).await;
    sub(&mut sink, &[("v5/#", 1)], None).await;
    let (mut owner, _) = connect(addr, "empty-owner", false, 60, &[], Some((0, 30))).await;
    owner.send(vec![0xe0, 0]).await;
    assert_eq!(owner.0.read(&mut [0]).await.unwrap(), 0);
    assert_eq!(broker.status().will_records, 0);
    let (_owner, present) = connect(addr, "empty-owner", false, 60, &[], None).await;
    assert!(present);
    let (mut source, _) = connect(addr, "empty-source", true, 0, &[], None).await;
    publish(&mut source, "v5/barrier", b"normal", 1, &[], false).await;
    let (h, b) = sink.recv().await;
    let m = decoded(h, &b);
    assert_eq!(m.topic, b"v5/barrier"[..]);
    sink.send(codec::frame(0x40, &m.pkid.to_be_bytes())).await;
    stop(&app).await;
}

#[tokio::test]
async fn mqtt5_shared_qos2_ownership_is_not_reassigned_on_reconnect_rejection_or_session_loss() {
    let (app, broker) = start(MqttBrokerConfig {
        mqtt5: true,
        ..config()
    })
    .await;
    let addr = broker.local_addr().unwrap();
    let (mut a, _) = connect(addr, "share-a", false, 60, &[], None).await;
    let (mut b, _) = connect(addr, "share-b", false, 60, &[], None).await;
    sub(&mut a, &[("$share/g/jobs", 2)], None).await;
    sub(&mut b, &[("$share/g/jobs", 2)], None).await;
    let (mut source, _) = connect(addr, "share-source", true, 0, &[], None).await;
    publish(&mut source, "jobs", b"owned", 2, &[], false).await;
    let (h, body) = a.recv().await;
    let first = decoded(h, &body);
    assert_eq!(first.qos, rumqttc::v5::mqttbytes::QoS::ExactlyOnce);
    drop(a);
    let (mut a, present) = connect(addr, "share-a", false, 60, &[], None).await;
    assert!(present);
    let (h, body) = a.recv().await;
    let resent = decoded(h, &body);
    assert_eq!(resent.pkid, first.pkid);
    assert!(resent.dup);
    assert_eq!(resent.payload, b"owned"[..]);
    b.send(vec![0xc0, 0]).await;
    assert_eq!(b.recv().await, (0xd0, vec![]));
    a.send(codec::frame(
        0x50,
        &[
            first.pkid.to_be_bytes()[0],
            first.pkid.to_be_bytes()[1],
            0x80,
        ],
    ))
    .await;
    a.send(vec![0xc0, 0]).await;
    assert_eq!(a.recv().await, (0xd0, vec![]));
    b.send(vec![0xc0, 0]).await;
    assert_eq!(b.recv().await, (0xd0, vec![]));
    publish(&mut source, "jobs", b"session-lost", 2, &[], false).await;
    let (h, body) = b.recv().await;
    assert_eq!(decoded(h, &body).payload, b"session-lost"[..]);
    drop(b);
    let (mut b, present) = connect(addr, "share-b", true, 60, &[], None).await;
    assert!(!present);
    sub(&mut b, &[("$share/g/jobs", 2)], None).await;
    for c in [&mut a, &mut b] {
        c.send(vec![0xc0, 0]).await;
        assert_eq!(c.recv().await, (0xd0, vec![]));
    }
    stop(&app).await;
}

#[tokio::test]
async fn mqtt5_keepalive_uses_connect_value_and_publishes_will_on_timeout() {
    let (app, broker) = start(MqttBrokerConfig {
        mqtt5: true,
        ..config()
    })
    .await;
    let addr = broker.local_addr().unwrap();
    let (mut sink, _) = connect(addr, "keep-sink", true, 0, &[], None).await;
    sub(&mut sink, &[("keep/will", 1)], None).await;
    let mut owner = Client(TcpStream::connect(addr).await.unwrap());
    let mut body = string("MQTT");
    body.extend([5, 0x0e, 0, 1, 0]);
    body.extend(string("keep-owner"));
    body.push(0);
    body.extend(string("keep/will"));
    body.extend([0, 1, b'w']);
    owner.send(codec::frame(0x10, &body)).await;
    let (h, b) = owner.recv().await;
    assert_eq!(h, 0x20);
    assert_eq!(b[1], 0);
    // No Server Keep Alive is sent, so the original one-second value applies.
    assert_eq!(
        b,
        vec![0, 0, 11, 0x21, 0, 128, 0x22, 0, 32, 0x27, 0, 0, 32, 0]
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), owner.0.read(&mut [0]))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    let (h, b) = sink.recv().await;
    assert_eq!(decoded(h, &b).payload, b"w"[..]);
    stop(&app).await;
}

#[tokio::test]
async fn mqtt5_publish_all_qos_preserves_binary_metadata_user_order_and_outgoing_flags() {
    let (app, broker) = start(MqttBrokerConfig {
        mqtt5: true,
        ..config()
    })
    .await;
    let addr = broker.local_addr().unwrap();
    let (mut sink, _) = connect(addr, "meta-sink", true, 0, &[], None).await;
    sub(&mut sink, &[("meta", 0x0a)], Some(7)).await; // RAP=1, QoS2
    let (mut source, _) = connect(addr, "meta-source", true, 0, &[], None).await;
    let props = [
        1, 0, 3, 0, 1, b't', 8, 0, 5, b'r', b'e', b'p', b'l', b'y', 9, 0, 2, 0xff, 0, 0x26, 0, 1,
        b'k', 0, 1, b'a', 0x26, 0, 1, b'k', 0, 1, b'b',
    ];
    for qos in 0..=2u8 {
        for retain in [false, true] {
            publish(&mut source, "meta", &[0, 0xff, qos], qos, &props, retain).await;
            let (h, b) = sink.recv().await;
            let m = decoded(h, &b);
            assert_eq!((h >> 1) & 3, qos);
            assert!(!m.dup);
            assert_eq!(m.retain, retain);
            assert_eq!(m.payload[..], [0, 0xff, qos]);
            let p = m.properties.unwrap();
            assert_eq!(p.content_type.as_deref(), Some("t"));
            assert_eq!(p.response_topic.as_deref(), Some("reply"));
            assert_eq!(p.correlation_data.as_deref(), Some(&[0xff, 0][..]));
            assert_eq!(
                p.user_properties,
                vec![("k".into(), "a".into()), ("k".into(), "b".into())]
            );
            assert_eq!(p.subscription_identifiers, vec![7]);
            if qos == 1 {
                sink.send(codec::frame(0x40, &m.pkid.to_be_bytes())).await;
            }
            if qos == 2 {
                sink.send(codec::frame(0x50, &m.pkid.to_be_bytes())).await;
                assert_eq!(sink.recv().await, (0x62, m.pkid.to_be_bytes().to_vec()));
                sink.send(codec::frame(0x70, &m.pkid.to_be_bytes())).await;
            }
        }
    }
    stop(&app).await;
}

#[test]
fn mqtt5_payload_format_indicator_enforces_utf8_for_publish_and_will() {
    for (payload, valid) in [
        (&b"valid\0"[..], true),
        (&[0xef, 0xbb, 0xbf][..], true),
        (&[0xff][..], false),
        (&[0xc0, 0x80][..], false),
        (&[0xed, 0xa0, 0x80][..], false),
    ] {
        let mut publish = string("t");
        publish.extend([2, 1, 1]);
        publish.extend(payload);
        assert_eq!(
            crate::mqtt5::Codec::default()
                .decode(&codec::frame(0x30, &publish), 8)
                .is_ok(),
            valid
        );
        let mut connect = string("MQTT");
        connect.extend([5, 6, 0, 0, 0]);
        connect.extend(string("c"));
        connect.extend([2, 1, 1]);
        connect.extend(string("w"));
        connect.extend((payload.len() as u16).to_be_bytes());
        connect.extend(payload);
        assert_eq!(
            crate::mqtt5::Codec::default()
                .decode(&codec::frame(0x10, &connect), 8)
                .is_ok(),
            valid
        );
    }
}

#[tokio::test]
async fn mqtt5_will_preserves_user_property_order_response_correlation_and_content_type() {
    let (app, broker) = start(MqttBrokerConfig {
        mqtt5: true,
        ..config()
    })
    .await;
    let addr = broker.local_addr().unwrap();
    let (mut sink, _) = connect(addr, "will-meta-sink", true, 0, &[], None).await;
    sub(&mut sink, &[("meta/will", 1)], None).await;
    let mut owner = Client(TcpStream::connect(addr).await.unwrap());
    let mut body = string("MQTT");
    body.extend([5, 0x0e, 0, 0, 0]);
    body.extend(string("will-meta-owner"));
    let props = [
        3, 0, 1, b't', 8, 0, 5, b'r', b'e', b'p', b'l', b'y', 9, 0, 1, 0xff, 0x26, 0, 1, b'k', 0,
        1, b'a', 0x26, 0, 1, b'k', 0, 1, b'b',
    ];
    body.push(props.len() as u8);
    body.extend(props);
    body.extend(string("meta/will"));
    body.extend([0, 1, 0xff]);
    owner.send(codec::frame(0x10, &body)).await;
    assert_eq!(owner.recv().await.0, 0x20);
    owner.send(vec![0xe0, 1, 4]).await;
    assert_eq!(owner.0.read(&mut [0]).await.unwrap(), 0);
    let (h, b) = sink.recv().await;
    let m = decoded(h, &b);
    let p = m.properties.unwrap();
    assert_eq!(m.payload, b"\xff"[..]);
    assert_eq!(p.content_type.as_deref(), Some("t"));
    assert_eq!(p.response_topic.as_deref(), Some("reply"));
    assert_eq!(p.correlation_data.as_deref(), Some(&[0xff][..]));
    assert_eq!(
        p.user_properties,
        vec![("k".into(), "a".into()), ("k".into(), "b".into())]
    );
    sink.send(codec::frame(0x40, &m.pkid.to_be_bytes())).await;
    stop(&app).await;
}

#[tokio::test]
async fn mqtt5_receive_maximum_rejects_before_retained_routing_and_isolates_the_connection() {
    let (app, broker) = start(MqttBrokerConfig {
        mqtt5: true,
        max_inbound_qos2_per_client: 1,
        ..config()
    })
    .await;
    let addr = broker.local_addr().unwrap();
    let (mut source, _) = connect(addr, "full-window", true, 0, &[], None).await;
    let (mut sink, _) = connect(addr, "healthy", true, 0, &[], None).await;
    sub(&mut sink, &[("quota/value", 1)], None).await;
    let mut original = string("quota/value");
    original.extend([0, 1, 0]);
    original.extend(b"accepted");
    source.send(codec::frame(0x35, &original)).await;
    assert_eq!(source.recv().await, (0x50, vec![0, 1]));
    receive_reliable(&mut sink, b"accepted", 1).await;
    let mut violating = string("quota/value");
    violating.extend([0, 2, 0]);
    violating.extend(b"must-not-route-or-replace-retained");
    source.send(codec::frame(0x33, &violating)).await;
    assert_eq!(source.recv().await, (0xe0, vec![0x93, 0]));
    assert_eq!(source.0.read(&mut [0]).await.unwrap(), 0);
    // A new subscriber must see the original Retained value. A reliable FIFO
    // barrier on the existing subscriber proves the violation was not queued.
    let (mut late, _) = connect(addr, "late", true, 0, &[], None).await;
    sub(&mut late, &[("quota/value", 1)], None).await;
    receive_reliable(&mut late, b"accepted", 1).await;
    let (mut healthy_source, _) = connect(addr, "other-source", true, 0, &[], None).await;
    publish(
        &mut healthy_source,
        "quota/value",
        b"barrier",
        1,
        &[],
        false,
    )
    .await;
    receive_reliable(&mut sink, b"barrier", 1).await;
    receive_reliable(&mut late, b"barrier", 1).await;
    sink.send(vec![0xc0, 0]).await;
    assert_eq!(sink.recv().await, (0xd0, vec![]));
    assert_eq!(broker.status().received_publications, 2);
    assert_eq!(broker.status().generation, 1);
    stop(&app).await;
}

#[tokio::test]
async fn mqtt5_connack_omits_optional_diagnostics_and_defaults_allow_qos2_retained_with_exact_packet_limit()
 {
    let (app, broker) = start(MqttBrokerConfig {
        mqtt5: true,
        max_packet_bytes: 8192,
        ..config()
    })
    .await;
    let addr = broker.local_addr().unwrap();
    for limit in [15u32, 16, 17] {
        let mut c = Client(TcpStream::connect(addr).await.unwrap());
        let mut body = string("MQTT");
        body.extend([5, 2, 0, 0, 5, 0x27]);
        body.extend(limit.to_be_bytes());
        body.extend(string(&format!("packet-limit-{limit}")));
        c.send(codec::frame(0x10, &body)).await;
        if limit == 15 {
            assert_eq!(c.0.read(&mut [0]).await.unwrap(), 0);
        } else {
            // Complete literal properties prove absence of optional diagnostics
            // and disabled-capability declarations, without using the encoder.
            assert_eq!(
                c.recv().await,
                (
                    0x20,
                    vec![0, 0, 11, 0x21, 0, 128, 0x22, 0, 32, 0x27, 0, 0, 32, 0]
                )
            );
            publish(&mut c, "caps", b"ok", 2, &[], true).await;
            c.send(vec![0xc0, 0]).await;
            assert_eq!(c.recv().await, (0xd0, vec![]));
            c.send(vec![0xe0, 0]).await;
            assert_eq!(c.0.read(&mut [0]).await.unwrap(), 0);
        }
    }
    assert_eq!(broker.status().retained, 1);
    let (mut oversized, _) = connect(addr, "oversized", true, 0, &[], None).await;
    let mut body = string("caps");
    body.push(0); // Properties, QoS0 has no Packet Identifier.
    body.extend(vec![b'x'; 8192]);
    oversized.send(codec::frame(0x31, &body)).await;
    // Closing with unread oversized bytes may produce TCP RST rather than FIN.
    match oversized.0.read(&mut [0]).await {
        Ok(0) => (),
        Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => (),
        other => panic!("oversized publication was not closed: {other:?}"),
    }
    let (mut sink, _) = connect(addr, "read-retained", true, 0, &[], None).await;
    sub(&mut sink, &[("caps", 1)], None).await;
    receive_reliable(&mut sink, b"ok", 1).await;
    assert_eq!(broker.status().generation, 1);
    stop(&app).await;
}
