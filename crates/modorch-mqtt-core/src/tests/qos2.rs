use super::*;

pub(super) async fn publish_start(
    client: &mut Client,
    id: u16,
    topic: &str,
    payload: &[u8],
    dup: bool,
    retain: bool,
) {
    let mut body = string(topic);
    body.extend(id.to_be_bytes());
    body.extend(payload);
    client
        .send(codec::frame(
            0x34 | (u8::from(dup) << 3) | u8::from(retain),
            &body,
        ))
        .await;
    assert_eq!(client.recv().await, (0x50, id.to_be_bytes().to_vec()));
}
pub(super) async fn release(client: &mut Client, id: u16) {
    client.send(codec::frame(0x62, &id.to_be_bytes())).await;
    assert_eq!(client.recv().await, (0x70, id.to_be_bytes().to_vec()));
}
pub(super) async fn received(client: &mut Client, id: u16) {
    client.send(codec::frame(0x50, &id.to_be_bytes())).await;
    assert_eq!(client.recv().await, (0x62, id.to_be_bytes().to_vec()));
}
pub(super) async fn complete(client: &mut Client, id: u16) {
    client.send(codec::frame(0x70, &id.to_be_bytes())).await;
    ping(client).await;
}
pub(super) async fn ping(client: &mut Client) {
    client.send(vec![0xc0, 0]).await;
    assert_eq!(client.recv().await, (0xd0, vec![]));
}

#[tokio::test]
async fn qos2_incoming_duplicate_release_and_identifier_reuse_route_exactly_once() {
    let (app, broker) = start(config()).await;
    let addr = broker.local_addr().unwrap();
    let (mut sub, _) = Client::connect(addr, "reader", true).await;
    sub.subscribe(&[("q2/#", 1)]).await;
    let (mut publisher, _) = Client::connect(addr, "writer", false).await;
    publish_start(&mut publisher, 17, "q2/a", b"first", false, true).await;
    let (_, payload, _, id) = sub.message().await;
    assert_eq!(payload, b"first");
    sub.ack(id.unwrap()).await;
    publish_start(&mut publisher, 17, "q2/a", b"first", true, true).await;
    ping(&mut sub).await; // a duplicate delivery would precede PINGRESP
    assert_eq!(broker.status().incoming_qos2, 1);
    assert_eq!(broker.status().received_publications, 1);
    release(&mut publisher, 17).await;
    release(&mut publisher, 17).await;
    assert_eq!(broker.status().incoming_qos2, 0);
    publish_start(&mut publisher, 17, "q2/a", b"second", false, false).await;
    release(&mut publisher, 17).await;
    let (_, payload, _, id) = sub.message().await;
    assert_eq!(payload, b"second");
    sub.ack(id.unwrap()).await;
    ping(&mut sub).await;
    assert_eq!(broker.status().received_publications, 2);
    stop(&app).await;
}

#[tokio::test]
async fn qos2_outgoing_reconnect_resumes_publish_or_pubrel_and_wrong_acks_cannot_free_id() {
    let (app, broker) = start(config()).await;
    let addr = broker.local_addr().unwrap();
    let (mut sub, _) = Client::connect(addr, "reader", false).await;
    assert_eq!(sub.subscribe(&[("q2", 2)]).await, [2]);
    let (mut publisher, _) = Client::connect(addr, "writer", true).await;
    publisher.publish("q2", b"payload", 2, false).await;
    let (_, payload, header, id) = sub.message().await;
    let id = id.unwrap();
    assert_eq!(header, 0x34);
    sub.ack(id).await; // PUBACK is not PUBCOMP
    sub.send(codec::frame(0x70, &id.to_be_bytes())).await; // premature PUBCOMP
    ping(&mut sub).await;
    assert_eq!(broker.status().pending_messages, 1);
    drop(sub);
    let (mut sub, present) = Client::connect(addr, "reader", false).await;
    assert!(present);
    let (_, replayed, header, replay_id) = sub.message().await;
    assert_eq!((header, replay_id, replayed), (0x3c, Some(id), payload));
    received(&mut sub, id).await;
    received(&mut sub, id).await;
    drop(sub);
    let (mut sub, present) = Client::connect(addr, "reader", false).await;
    assert!(present);
    assert_eq!(sub.recv().await, (0x62, id.to_be_bytes().to_vec()));
    complete(&mut sub, id).await;
    complete(&mut sub, id).await;
    assert_eq!(broker.status().pending_messages, 0);
    stop(&app).await;
}

#[tokio::test]
async fn qos2_receiver_count_and_byte_limits_reject_before_pubrec_and_resume_after_release() {
    for bytes in [false, true] {
        let mut c = config();
        c.max_inbound_qos2_per_client = if bytes { 10 } else { 1 };
        c.max_inbound_qos2_bytes_per_client = if bytes { 280 } else { 8192 };
        let (app, broker) = start(c).await;
        let addr = broker.local_addr().unwrap();
        let (mut p, _) = Client::connect(addr, "writer", false).await;
        publish_start(&mut p, 1, "q2", b"one", false, false).await;
        publish_start(&mut p, 1, "q2", b"one", true, false).await; // allowed at quota
        let mut body = string("q2");
        body.extend([0, 2]);
        body.extend(b"two");
        p.send(codec::frame(0x34, &body)).await;
        assert_eq!(
            p.0.read_u8().await.unwrap_err().kind(),
            std::io::ErrorKind::UnexpectedEof
        );
        assert_eq!(broker.status().incoming_qos2, 1);
        let (mut p, present) = Client::connect(addr, "writer", false).await;
        assert!(present);
        release(&mut p, 1).await;
        publish_start(&mut p, 2, "q2", b"two", false, false).await;
        release(&mut p, 2).await;
        assert_eq!(broker.status().incoming_qos2_bytes, 0);
        stop(&app).await;
    }
}

#[tokio::test]
async fn qos2_reliable_window_preserves_publish_order_and_releases_capacity_only_on_pubcomp() {
    let mut c = config();
    c.max_inflight_per_client = 2;
    let (app, broker) = start(c).await;
    let addr = broker.local_addr().unwrap();
    let (mut sub, _) = Client::connect(addr, "reader", false).await;
    sub.subscribe(&[("q2", 2)]).await;
    let (mut p, _) = Client::connect(addr, "writer", true).await;
    for payload in [b"one", b"two", b"tri"] {
        p.publish("q2", payload, 2, false).await;
    }
    let (_, a, _, one) = sub.message().await;
    let (_, b, _, two) = sub.message().await;
    assert_eq!((a, b), (b"one".to_vec(), b"two".to_vec()));
    received(&mut sub, two.unwrap()).await;
    ping(&mut sub).await; // no third PUBLISH until PUBCOMP
    sub.send(codec::frame(0x70, &two.unwrap().to_be_bytes()))
        .await;
    let (_, c, header, three) = sub.message().await;
    assert_eq!(c, b"tri");
    assert_eq!(header, 0x34);
    assert_ne!(three, one);
    assert_ne!(three, two);
    received(&mut sub, one.unwrap()).await;
    complete(&mut sub, one.unwrap()).await;
    received(&mut sub, three.unwrap()).await;
    complete(&mut sub, three.unwrap()).await;
    assert_eq!(broker.status().pending_messages, 0);
    stop(&app).await;
}

#[tokio::test]
async fn qos2_packet_id_dedup_ignores_changed_payload_and_clean_session_resets_exchange() {
    let (app, broker) = start(config()).await;
    let addr = broker.local_addr().unwrap();
    let (mut p, _) = Client::connect(addr, "writer", false).await;
    publish_start(&mut p, 5, "q2", b"first", false, false).await;
    let mut body = string("q2");
    body.extend([0, 5]);
    body.extend(b"conflict");
    p.send(codec::frame(0x3c, &body)).await;
    assert_eq!(p.recv().await, (0x50, vec![0, 5]));
    assert_eq!(broker.status().received_publications, 1);
    // Reusing an outstanding QoS2 identifier for QoS1 is a protocol violation.
    p.send(codec::frame(0x32, &body)).await;
    assert_eq!(p.0.read(&mut [0]).await.unwrap(), 0);
    assert_eq!(broker.status().incoming_qos2, 1);
    let (mut p, present) = Client::connect(addr, "writer", true).await;
    assert!(!present);
    assert_eq!(broker.status().incoming_qos2, 0);
    publish_start(&mut p, 5, "q2", b"new-session", false, false).await;
    p.disconnect().await;
    let (mut p, present) = Client::connect(addr, "writer", false).await;
    assert!(!present);
    ping(&mut p).await;
    assert_eq!(broker.status().incoming_qos2, 0);
    assert_eq!(broker.status().generation, 1);
    stop(&app).await;
}

#[tokio::test]
async fn qos2_will_and_retained_replay_use_full_receiver_handshake() {
    let (app, broker) = start(config()).await;
    let addr = broker.local_addr().unwrap();
    let (mut sub, _) = Client::connect(addr, "reader", false).await;
    sub.subscribe(&[("q2/will", 2)]).await;
    let mut will = Client(TcpStream::connect(addr).await.unwrap());
    let mut body = string("MQTT");
    body.extend([4, 0x36, 0, 0]);
    body.extend(string("will-writer"));
    body.extend(string("q2/will"));
    body.extend(string("goodbye"));
    will.send(codec::frame(0x10, &body)).await;
    assert_eq!(will.recv().await, (0x20, vec![0, 0]));
    drop(will);
    let (_, payload, header, id) = sub.message().await;
    assert_eq!(payload, b"goodbye");
    assert_eq!(header, 0x34);
    received(&mut sub, id.unwrap()).await;
    complete(&mut sub, id.unwrap()).await;
    let (mut late, _) = Client::connect(addr, "late", true).await;
    late.subscribe(&[("q2/will", 2)]).await;
    let (_, payload, header, id) = late.message().await;
    assert_eq!(payload, b"goodbye");
    assert_eq!(header, 0x35);
    received(&mut late, id.unwrap()).await;
    complete(&mut late, id.unwrap()).await;
    stop(&app).await;
}

#[tokio::test]
async fn qos2_memory_generation_recovery_preserves_receiver_and_sender_phase() {
    let (app, broker) = start(config()).await;
    let addr = broker.local_addr().unwrap();
    let (mut sub, _) = Client::connect(addr, "reader", false).await;
    sub.subscribe(&[("q2", 2)]).await;
    let (mut p, _) = Client::connect(addr, "writer", false).await;
    publish_start(&mut p, 9, "q2", b"memory", false, false).await;
    let (_, _, _, id) = sub.message().await;
    let id = id.unwrap();
    received(&mut sub, id).await;
    {
        let mut s = broker.state.lock().unwrap();
        s.fail_activations = 1;
        s.fail_listener = true;
    }
    tokio::time::timeout(Duration::from_secs(3), async {
        while broker.status().generation < 3 || !broker.status().active {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop((p, sub));
    let (mut p, present) = Client::connect(addr, "writer", false).await;
    assert!(present);
    publish_start(&mut p, 9, "q2", b"memory", true, false).await;
    assert_eq!(broker.status().pending_messages, 1);
    release(&mut p, 9).await;
    let (mut sub, present) = Client::connect(addr, "reader", false).await;
    assert!(present);
    assert_eq!(sub.recv().await, (0x62, id.to_be_bytes().to_vec()));
    complete(&mut sub, id).await;
    assert!(broker.state.lock().unwrap().check(1, "writer", 1).is_err());
    stop(&app).await;
}

#[tokio::test]
async fn qos2_slow_consumer_pubcomp_backlog_rejects_atomically_and_same_id_retry_can_commit() {
    let mut cfg = config();
    cfg.max_inflight_per_client = 1;
    cfg.max_offline_messages_per_client = 2;
    let (app, broker) = start(cfg).await;
    let addr = broker.local_addr().unwrap();
    let (mut healthy, _) = Client::connect(addr, "a-healthy", true).await;
    healthy.subscribe(&[("flow", 1)]).await;
    let (mut slow, _) = Client::connect(addr, "z-slow", false).await;
    slow.subscribe(&[("flow", 2)]).await;
    let (mut publisher, _) = Client::connect(addr, "writer", false).await;
    publish_start(&mut publisher, 1, "flow", b"old", false, true).await;
    release(&mut publisher, 1).await;
    let (_, body, _, id) = healthy.message().await;
    assert_eq!(body, b"old");
    healthy.ack(id.unwrap()).await;
    ping(&mut healthy).await;
    let (_, body, _, first) = slow.message().await;
    let first = first.unwrap();
    assert_eq!(body, b"old");
    received(&mut slow, first).await; // Hold PUBCOMP, not just PUBREC.
    publish_start(&mut publisher, 2, "flow", b"second", false, false).await;
    release(&mut publisher, 2).await;
    let (_, body, _, id) = healthy.message().await;
    assert_eq!(body, b"second");
    healthy.ack(id.unwrap()).await;
    ping(&mut healthy).await;
    ping(&mut slow).await; // Window remains occupied by the Release phase.
    assert_eq!(broker.status().pending_messages, 2);
    assert_eq!(broker.status().incoming_qos2, 0);
    // The healthy target sorts before the full target. Its tentative reservation
    // and the Retained replacement must both roll back before PUBREC is possible.
    let mut body = string("flow");
    body.extend([0, 3]);
    body.extend(b"third");
    publisher.send(codec::frame(0x35, &body)).await;
    protocol::closed(&mut publisher).await;
    ping(&mut healthy).await;
    ping(&mut slow).await;
    assert_eq!(broker.status().pending_messages, 2);
    assert_eq!(broker.status().incoming_qos2, 0);
    assert_eq!(broker.status().received_publications, 2);
    let (mut late, _) = Client::connect(addr, "retained-check", true).await;
    late.subscribe(&[("flow", 0)]).await;
    let (_, payload, header, _) = late.message().await;
    assert_eq!((payload.as_slice(), header), (b"old".as_slice(), 0x31));
    protocol::disconnect(&mut late).await;
    slow.send(codec::frame(0x70, &first.to_be_bytes())).await;
    let (_, body, _, second) = slow.message().await;
    let second = second.unwrap();
    assert_eq!(body, b"second");
    received(&mut slow, second).await;
    complete(&mut slow, second).await;
    assert_eq!(broker.status().pending_messages, 0);
    let (mut publisher, present) = Client::connect(addr, "writer", false).await;
    assert!(present);
    publish_start(&mut publisher, 3, "flow", b"third", true, true).await;
    // The failed attempt did not create a ghost receiver record. A successful
    // retry routes once, and its next duplicate must not route a second time.
    publish_start(&mut publisher, 3, "flow", b"third", true, true).await;
    release(&mut publisher, 3).await;
    let (_, body, _, id) = healthy.message().await;
    assert_eq!(body, b"third");
    healthy.ack(id.unwrap()).await;
    let (_, body, _, third) = slow.message().await;
    assert_eq!(body, b"third");
    received(&mut slow, third.unwrap()).await;
    complete(&mut slow, third.unwrap()).await;
    ping(&mut healthy).await;
    assert_eq!(broker.status().received_publications, 3);
    assert_eq!(broker.status().pending_messages, 0);
    protocol::disconnect(&mut publisher).await;
    stop(&app).await;
}
