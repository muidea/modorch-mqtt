use super::*;

impl State {
    /// Method B: route and record receiver ownership atomically before PUBREC.
    /// PUBREL subsequently frees the receiver record without routing again.
    pub fn incoming_duplicate(
        &self,
        generation: u64,
        client: &str,
        connection: u64,
        id: u16,
        message: &MqttMessage,
    ) -> Result<bool> {
        self.check(generation, client, connection)?;
        match self.sessions[client].incoming_qos2.get(&id) {
            // MQTT-4.3.3-2 identifies retransmission by Packet Identifier.
            // Ignore a repeated QoS2 payload even when its contents differ;
            // acknowledging it must never replace the original owned message.
            Some(_) if message.qos == Qos::ExactlyOnce => Ok(true),
            Some(_) => Err(MqttError::Protocol("QoS2 Packet Identifier still in use")),
            None => Ok(false),
        }
    }

    pub fn receive_qos2(
        &mut self,
        generation: u64,
        client: &str,
        connection: u64,
        id: u16,
        message: MqttMessage,
    ) -> Result<bool> {
        if self.incoming_duplicate(generation, client, connection, id, &message)? {
            return Ok(false);
        }
        allowed_topic(&message.topic)?;
        let session = &self.sessions[client];
        let cost = message_cost(&message) + 64;
        let used: usize = session
            .incoming_qos2
            .values()
            .map(|p| message_cost(&p.message) + 64)
            .sum();
        if session.incoming_qos2.len() >= self.config.max_inbound_qos2_per_client
            || used.saturating_add(cost) > self.config.max_inbound_qos2_bytes_per_client
        {
            return Err(MqttError::Capacity("incoming QoS2 exchanges"));
        }
        let charge = self.budget.reserve(cost)?;
        // Reserve receiver state before any routing, including self-subscription.
        // Failed routing drops this reservation without creating an exchange.
        self.route_from(&message, Some(client))?;
        self.sessions.get_mut(client).unwrap().incoming_qos2.insert(
            id,
            IncomingQos2 {
                message,
                _charge: charge,
            },
        );
        self.counters.received_publications += 1;
        Ok(true)
    }

    pub fn incoming_release(
        &mut self,
        generation: u64,
        client: &str,
        connection: u64,
        id: u16,
    ) -> Result<()> {
        self.check(generation, client, connection)?;
        // Repeated or unknown PUBREL must still receive PUBCOMP. Never route here.
        self.sessions
            .get_mut(client)
            .unwrap()
            .incoming_qos2
            .remove(&id);
        self.process_wills();
        Ok(())
    }

    pub fn outgoing_received(
        &mut self,
        generation: u64,
        client: &str,
        connection: u64,
        id: u16,
    ) -> Result<Option<Vec<u8>>> {
        self.check(generation, client, connection)?;
        let session = self.sessions.get_mut(client).unwrap();
        if let Some(index) = session
            .pending
            .iter()
            .position(|d| d.id == Some(id) && d.qos == Qos::ExactlyOnce && d.ever_sent)
        {
            let mut d = session.pending.remove(index).unwrap();
            // Queue each response in the connection's bounded control FIFO,
            // including duplicates (MQTT-4.6.0-4). Selecting Release from the
            // PUBLISH queue can reorder acknowledgements under writer pressure.
            d.phase = DeliveryPhase::Release { ever_sent: true };
            d.sent = true;
            // The remaining Publish entries keep their original relative order.
            // Persist the most recent PUBREL order for reconnect/recovery.
            session.pending.push_back(d);
            return Ok(Some(codec::frame(0x62, &id.to_be_bytes())));
        }
        Ok(None)
    }

    pub fn outgoing_completed(
        &mut self,
        generation: u64,
        client: &str,
        connection: u64,
        id: u16,
    ) -> Result<()> {
        self.check(generation, client, connection)?;
        let session = self.sessions.get_mut(client).unwrap();
        if let Some(index) = session.pending.iter().position(|d| {
            d.id == Some(id)
                && d.qos == Qos::ExactlyOnce
                && d.phase == (DeliveryPhase::Release { ever_sent: true })
        }) {
            session.pending.remove(index);
            session.notify.notify_one();
        }
        self.process_wills();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attach(s: &mut State, g: u64) -> u64 {
        let c = Connect {
            v5: None,
            client_id: "ordered".into(),
            clean: false,
            keep_alive: 0,
            will: None,
            username: None,
            password: None,
        };
        let p = Principal {
            key: PrincipalKey {
                domain: "test".into(),
                subject: "owner".into(),
            },
            policy_version: 0,
            expires_at_ms: u64::MAX,
        };
        let expected = s.sessions.get("ordered").map(|s| s.revision);
        s.attach(g, &c, CancellationToken::new(), &p, expected, None)
            .unwrap()
            .1
    }

    #[test]
    fn receiver_transition_matrix_keeps_dedup_retained_and_reuse_across_snapshot_recovery() {
        // Two receiver ownership states x four input events x two recovery
        // paths. Expectations come from 4.3.3 receiver Method B, not a model
        // that invokes the implementation to compute its expected result.
        for occupied in [false, true] {
            for event in ["publish", "changed-publish", "pubrel", "conflicting-qos1"] {
                for recover in [false, true] {
                    let mut s = State::new(Arc::new(MqttBrokerConfig::loopback_development()));
                    let g = s.begin_generation().unwrap();
                    s.active = true;
                    let mut connection = attach(&mut s, g);
                    s.subscribe(
                        g,
                        "ordered",
                        connection,
                        vec![("ordered".into(), Some(Qos::ExactlyOnce))],
                    )
                    .unwrap();
                    let message = |payload: &'static [u8]| MqttMessage {
                        topic: "ordered".into(),
                        payload: bytes::Bytes::from_static(payload),
                        qos: Qos::ExactlyOnce,
                        retain: true,
                        properties: Default::default(),
                    };
                    if occupied {
                        assert!(
                            s.receive_qos2(g, "ordered", connection, 7, message(b"original"))
                                .unwrap()
                        );
                    }
                    if recover {
                        let snapshot = s.snapshot();
                        s.restore(snapshot).unwrap();
                        connection = attach(&mut s, g);
                    }
                    match event {
                        "publish" | "changed-publish" => {
                            let next = message(if event == "publish" {
                                b"original"
                            } else {
                                b"changed"
                            });
                            assert_eq!(
                                s.receive_qos2(g, "ordered", connection, 7, next).unwrap(),
                                !occupied
                            );
                            assert_eq!(s.sessions["ordered"].incoming_qos2.len(), 1);
                            assert_eq!(s.sessions["ordered"].pending.len(), 1);
                            let expected = if occupied || event == "publish" {
                                b"original".as_slice()
                            } else {
                                b"changed"
                            };
                            assert_eq!(s.retained["ordered"].message.payload.as_ref(), expected);
                        }
                        "pubrel" => {
                            for _ in 0..2 {
                                s.incoming_release(g, "ordered", connection, 7).unwrap();
                                assert!(s.sessions["ordered"].incoming_qos2.is_empty());
                                assert_eq!(
                                    s.sessions["ordered"].pending.len(),
                                    usize::from(occupied)
                                );
                            }
                            // PUBCOMP ends dedup: a fresh PUBLISH with the
                            // same ID must own and route the new application message.
                            assert!(
                                s.receive_qos2(g, "ordered", connection, 7, message(b"reused"))
                                    .unwrap()
                            );
                            assert_eq!(
                                s.sessions["ordered"].pending.len(),
                                usize::from(occupied) + 1
                            );
                            assert_eq!(s.retained["ordered"].message.payload.as_ref(), b"reused");
                        }
                        _ => {
                            let mut wrong = message(b"wrong-qos");
                            wrong.qos = Qos::AtLeastOnce;
                            let result = s.incoming_duplicate(g, "ordered", connection, 7, &wrong);
                            if occupied {
                                assert!(result.is_err());
                            } else {
                                assert!(!result.unwrap());
                            }
                            assert_eq!(s.sessions["ordered"].pending.len(), usize::from(occupied));
                            assert_eq!(
                                s.sessions["ordered"].incoming_qos2.len(),
                                usize::from(occupied)
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn sender_ack_transition_matrix_preserves_phase_and_identifier_ownership() {
        #[derive(serde::Deserialize)]
        struct Matrix {
            qos2_sender_ack_transitions: Vec<Row>,
        }
        #[derive(serde::Deserialize)]
        struct Row {
            state: String,
            event: String,
            next_state: String,
            pubrel: bool,
        }
        let matrix: Matrix =
            serde_json::from_str(include_str!("../../../../contracts/mqtt311-packets.json"))
                .unwrap();
        assert_eq!(matrix.qos2_sender_ack_transitions.len(), 12);
        let mut seen = std::collections::BTreeSet::new();
        for row in matrix.qos2_sender_ack_transitions {
            assert!(seen.insert((row.state.clone(), row.event.clone())));
            let mut s = State::new(Arc::new(MqttBrokerConfig::loopback_development()));
            let g = s.begin_generation().unwrap();
            s.active = true;
            let connection = attach(&mut s, g);
            s.subscribe(
                g,
                "ordered",
                connection,
                vec![("ordered".into(), Some(Qos::ExactlyOnce))],
            )
            .unwrap();
            if row.state != "Empty" {
                s.publish(
                    g,
                    "ordered",
                    connection,
                    MqttMessage {
                        topic: "ordered".into(),
                        payload: bytes::Bytes::from_static(b"owned"),
                        qos: Qos::ExactlyOnce,
                        retain: false,
                        properties: Default::default(),
                    },
                )
                .unwrap();
            }
            match row.state.as_str() {
                "Empty" | "Queued" => (),
                "AwaitPubRec" | "AwaitPubComp" => {
                    assert_eq!(
                        s.next_outbound(g, "ordered", connection).unwrap().unwrap()[0],
                        0x34
                    );
                    if row.state == "AwaitPubComp" {
                        assert_eq!(
                            s.outgoing_received(g, "ordered", connection, 1).unwrap(),
                            Some(vec![0x62, 2, 0, 1])
                        );
                    }
                }
                _ => panic!("unknown sender state"),
            }
            let reply = match row.event.as_str() {
                "PubAck" => {
                    s.ack(g, "ordered", connection, 1).unwrap();
                    None
                }
                "PubRec" => s.outgoing_received(g, "ordered", connection, 1).unwrap(),
                "PubComp" => {
                    s.outgoing_completed(g, "ordered", connection, 1).unwrap();
                    None
                }
                _ => panic!("unknown ACK event"),
            };
            assert_eq!(reply, row.pubrel.then_some(vec![0x62, 2, 0, 1]));
            let session = &s.sessions["ordered"];
            let actual = match session.pending.front() {
                None => "Empty",
                Some(d) => {
                    assert_eq!(d.message.payload.as_ref(), b"owned");
                    if !d.ever_sent {
                        assert_eq!(d.id, None);
                        "Queued"
                    } else {
                        assert_eq!(d.id, Some(1));
                        if matches!(d.phase, DeliveryPhase::Release { .. }) {
                            "AwaitPubComp"
                        } else {
                            "AwaitPubRec"
                        }
                    }
                }
            };
            assert_eq!(actual, row.next_state, "{} + {}", row.state, row.event);
            assert_eq!(session.pending.len(), usize::from(actual != "Empty"));
        }
    }

    #[test]
    fn queued_pubrec_responses_and_snapshot_keep_release_order_without_reordering_publish() {
        let mut s = State::new(Arc::new(MqttBrokerConfig::loopback_development()));
        let g = s.begin_generation().unwrap();
        s.active = true;
        let connection = attach(&mut s, g);
        s.subscribe(
            g,
            "ordered",
            connection,
            vec![("ordered".into(), Some(Qos::ExactlyOnce))],
        )
        .unwrap();
        for payload in [b"one", b"two", b"tri", b"for"] {
            s.publish(
                g,
                "ordered",
                connection,
                MqttMessage {
                    topic: "ordered".into(),
                    payload: bytes::Bytes::from_static(payload),
                    qos: Qos::ExactlyOnce,
                    retain: false,
                    properties: Default::default(),
                },
            )
            .unwrap();
        }
        let mut ids = Vec::new();
        for _ in 0..4 {
            let frame = s.next_outbound(g, "ordered", connection).unwrap().unwrap();
            match codec::decode(&frame, 1).unwrap() {
                codec::Packet::Publish { packet_id, .. } => ids.push(packet_id.unwrap()),
                _ => panic!("expected PUBLISH"),
            }
        }
        // No socket progress between inputs: deterministically model a blocked
        // writer. Every response, including the duplicate, must remain FIFO.
        let order = [ids[1], ids[0], ids[1]];
        let replies: Vec<_> = order
            .iter()
            .map(|id| {
                s.outgoing_received(g, "ordered", connection, *id)
                    .unwrap()
                    .unwrap()
            })
            .collect();
        for (id, frame) in order.into_iter().zip(replies) {
            assert_eq!(frame, codec::frame(0x62, &id.to_be_bytes()));
        }
        assert!(s.next_outbound(g, "ordered", connection).unwrap().is_none());
        let image = serde_json::to_value(s.snapshot()).unwrap();
        let mut legacy = image.clone();
        legacy["format"] = 2.into();
        assert!(matches!(
            s.restore(serde_json::from_value(legacy).unwrap()),
            Err(MqttError::Unsupported("legacy QoS2 release order"))
        ));
        assert_eq!(s.status().pending_messages, 4); // failed restore is atomic
        s.restore(serde_json::from_value(image).unwrap()).unwrap();
        let connection = attach(&mut s, g);
        for id in [ids[0], ids[1]] {
            assert_eq!(
                s.next_outbound(g, "ordered", connection).unwrap().unwrap(),
                codec::frame(0x62, &id.to_be_bytes())
            );
        }
        // Removing acknowledged PUBLISH entries cannot reorder the remaining
        // unacknowledged PUBLISH retransmissions (MQTT-4.6.0-1).
        for (id, payload) in [(ids[2], b"tri"), (ids[3], b"for")] {
            let frame = s.next_outbound(g, "ordered", connection).unwrap().unwrap();
            assert_eq!(frame[0], 0x3c);
            match codec::decode(&frame, 1).unwrap() {
                codec::Packet::Publish { packet_id, message } => {
                    assert_eq!(packet_id, Some(id));
                    assert_eq!(&message.payload[..], payload);
                }
                _ => panic!("expected PUBLISH"),
            }
        }
        s.outgoing_completed(g, "ordered", connection, ids[0])
            .unwrap();
        let mut legacy = serde_json::to_value(s.snapshot()).unwrap();
        legacy["format"] = 2.into();
        s.restore(serde_json::from_value(legacy).unwrap()).unwrap(); // one Release is unambiguous
        assert_eq!(s.status().pending_messages, 3);
    }

    #[test]
    fn packet_identifier_wrap_skips_live_qos1_and_qos2_ids_and_reuses_only_completed_ids() {
        let mut s = State::new(Arc::new(MqttBrokerConfig::loopback_development()));
        let g = s.begin_generation().unwrap();
        s.active = true;
        let c = attach(&mut s, g);
        s.subscribe(
            g,
            "ordered",
            c,
            vec![("ordered".into(), Some(Qos::ExactlyOnce))],
        )
        .unwrap();
        for qos in [
            Qos::AtLeastOnce,
            Qos::ExactlyOnce,
            Qos::AtLeastOnce,
            Qos::ExactlyOnce,
            Qos::ExactlyOnce,
        ] {
            s.publish(
                g,
                "ordered",
                c,
                MqttMessage {
                    topic: "ordered".into(),
                    payload: bytes::Bytes::from_static(b"wrap"),
                    qos,
                    retain: false,
                    properties: Default::default(),
                },
            )
            .unwrap();
        }
        fn next_id(s: &mut State, g: u64, c: u64) -> u16 {
            let frame = s.next_outbound(g, "ordered", c).unwrap().unwrap();
            match codec::decode(&frame, 1).unwrap() {
                codec::Packet::Publish {
                    packet_id: Some(id),
                    ..
                } => id,
                _ => panic!("expected PUBLISH with Packet ID"),
            }
        }
        s.sessions.get_mut("ordered").unwrap().next_packet_id = u16::MAX;
        assert_eq!(next_id(&mut s, g, c), u16::MAX);
        assert_eq!(next_id(&mut s, g, c), 1);
        s.sessions.get_mut("ordered").unwrap().next_packet_id = u16::MAX;
        assert_eq!(next_id(&mut s, g, c), 2); // skip both QoS1 and QoS2 IDs
        s.outgoing_received(g, "ordered", c, 1).unwrap().unwrap();
        s.ack(g, "ordered", c, 1).unwrap(); // wrong ACK must not free QoS2 ID
        s.outgoing_completed(g, "ordered", c, u16::MAX).unwrap(); // nor QoS1
        assert_eq!(s.status().pending_messages, 5);
        s.ack(g, "ordered", c, u16::MAX).unwrap();
        s.sessions.get_mut("ordered").unwrap().next_packet_id = u16::MAX;
        assert_eq!(next_id(&mut s, g, c), u16::MAX); // completed ID can be reused
        s.sessions.get_mut("ordered").unwrap().next_packet_id = 1;
        assert_eq!(next_id(&mut s, g, c), 3); // Release1 and Publish2 remain in use
        let ids: std::collections::BTreeSet<_> = s.sessions["ordered"]
            .pending
            .iter()
            .filter_map(|d| d.id)
            .collect();
        assert_eq!(ids.len(), 4);
        assert!(!ids.contains(&0));
    }
}
