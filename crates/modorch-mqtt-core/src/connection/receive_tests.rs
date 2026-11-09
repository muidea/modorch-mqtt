use super::*;

async fn frame(peer: &mut tokio::io::DuplexStream) -> Vec<u8> {
    let head = peer.read_u8().await.unwrap();
    let len = peer.read_u8().await.unwrap();
    assert!(len < 128);
    let mut bytes = vec![head, len];
    bytes.resize(2 + usize::from(len), 0);
    peer.read_exact(&mut bytes[2..]).await.unwrap();
    bytes
}

fn publish(v5: bool, qos: u8, id: u8, payload: u8) -> Vec<u8> {
    let mut body = vec![0, 1, b't', 0, id];
    if v5 {
        body.push(0);
    }
    body.push(payload);
    codec::frame(0x30 | qos << 1, &body)
}

#[tokio::test]
async fn partial_ack_write_enforces_mqtt5_receive_maximum_without_limiting_mqtt311() {
    // Two protocols x three acknowledgement phases x QoS1/QoS2 second. A one-byte
    // transport makes the advertised window observable before any complete ACK.
    for v5 in [false, true] {
        for (first, release) in [(1, false), (2, false), (2, true)] {
            for second in [1, 2] {
                let cfg = crate::MqttBrokerConfig {
                    mqtt5: true,
                    max_inbound_qos2_per_client: if v5 { 1 } else { 2 },
                    ..crate::MqttBrokerConfig::loopback_development()
                };
                let mut initial = State::new(Arc::new(cfg));
                initial.active = true;
                initial.generation = 1;
                initial.connections = 1;
                let state = Arc::new(Mutex::new(initial));
                let mut life = ConnectionLifetime {
                    diagnostics: None,
                    state: state.clone(),
                    drained: Arc::new(Notify::new()),
                    generation: 1,
                    peer_ip: "127.0.0.1".parse().unwrap(),
                    attached: None,
                    graceful: false,
                    _permit: None,
                    _memory: None,
                    persistence: Default::default(),
                };
                let app = modorch::ApplicationBuilder::new("receive-window")
                    .build()
                    .unwrap()
                    .start()
                    .await
                    .unwrap();
                let hub = app.event_hub().external();
                let (server, mut peer) = tokio::io::duplex(1);
                let protocol = run(
                    Box::new(server),
                    &mut life,
                    CancellationToken::new(),
                    &hub,
                    "127.0.0.1".parse().unwrap(),
                    Instant::now() + Duration::from_secs(5),
                );
                let traffic = async {
                    let mut body = vec![
                        0,
                        4,
                        b'M',
                        b'Q',
                        b'T',
                        b'T',
                        if v5 { 5 } else { 4 },
                        2,
                        0,
                        0,
                    ];
                    if v5 {
                        body.push(0);
                    }
                    body.extend([0, 1, b'x']);
                    peer.write_all(&codec::frame(0x10, &body)).await.unwrap();
                    let connack = frame(&mut peer).await;
                    assert_eq!(connack[0], 0x20);
                    if v5 {
                        assert_eq!(&connack[5..8], &[0x21, 0, 1]);
                    }
                    peer.write_all(&publish(v5, first, 1, b'a')).await.unwrap();
                    if release {
                        assert_eq!(frame(&mut peer).await, [0x50, 2, 0, 1]);
                        peer.write_all(&[0x62, 2, 0, 1]).await.unwrap();
                    }
                    let ack = if release {
                        0x70
                    } else if first == 1 {
                        0x40
                    } else {
                        0x50
                    };
                    assert_eq!(peer.read_u8().await.unwrap(), ack);
                    // Finish sending a sentinel after the second PUBLISH while
                    // the first ACK is blocked. Its decoding must precede reads
                    // of this sentinel; no sleeps or socket buffer assumptions.
                    peer.write_all(&publish(v5, second, 2, b'b')).await.unwrap();
                    peer.write_all(&[0xc0]).await.unwrap();
                    let mut rest = [0; 3];
                    peer.read_exact(&mut rest).await.unwrap();
                    assert_eq!(rest, [2, 0, 1]);
                    if v5 {
                        assert_eq!(frame(&mut peer).await, [0xe0, 2, 0x93, 0]);
                        assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
                        assert_eq!(state.lock().unwrap().status().received_publications, 1);
                    } else {
                        peer.write_all(&[0]).await.unwrap();
                        assert_eq!(
                            frame(&mut peer).await,
                            [if second == 1 { 0x40 } else { 0x50 }, 2, 0, 2]
                        );
                        assert_eq!(frame(&mut peer).await, [0xd0, 0]);
                        peer.write_all(&[0xe0, 0]).await.unwrap();
                    }
                };
                let (result, ()) = tokio::time::timeout(Duration::from_secs(3), async {
                    tokio::join!(protocol, traffic)
                })
                .await
                .unwrap();
                result.unwrap();
                drop(life);
                app.shutdown_checked(modorch::ShutdownOptions::graceful())
                    .await
                    .unwrap();
            }
        }
    }
}

fn lifetime(state: SharedState) -> ConnectionLifetime {
    ConnectionLifetime {
        diagnostics: None,
        state,
        drained: Arc::new(Notify::new()),
        generation: 1,
        peer_ip: "127.0.0.1".parse().unwrap(),
        attached: None,
        graceful: false,
        _permit: None,
        _memory: None,
        persistence: Default::default(),
    }
}

async fn connect5(peer: &mut tokio::io::DuplexStream, clean: bool) {
    let mut body = vec![
        0,
        4,
        b'M',
        b'Q',
        b'T',
        b'T',
        5,
        if clean { 2 } else { 0 },
        0,
        0,
    ];
    // One-minute Session Expiry; Receive Maximum remains a connection property.
    body.extend([5, 0x11, 0, 0, 0, 60, 0, 1, b'x']);
    peer.write_all(&codec::frame(0x10, &body)).await.unwrap();
    let reply = frame(peer).await;
    assert_eq!(&reply[..5], &[0x20, 14, u8::from(!clean), 0, 11]);
    assert_eq!(&reply[5..8], &[0x21, 0, 2]);
}

#[tokio::test]
async fn receive_quota_requires_pubcomp_and_resets_on_reconnect_with_old_receiver_records() {
    let cfg = crate::MqttBrokerConfig {
        mqtt5: true,
        max_inbound_qos2_per_client: 2,
        ..crate::MqttBrokerConfig::loopback_development()
    };
    let mut initial = State::new(Arc::new(cfg));
    initial.active = true;
    initial.generation = 1;
    initial.connections = 1;
    let state = Arc::new(Mutex::new(initial));
    let app = modorch::ApplicationBuilder::new("receive-reset")
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();
    let hub = app.event_hub().external();
    for reconnect in [false, true] {
        if reconnect {
            state.lock().unwrap().connections += 1;
        }
        let mut life = lifetime(state.clone());
        let (server, mut peer) = tokio::io::duplex(128);
        let protocol = run(
            Box::new(server),
            &mut life,
            CancellationToken::new(),
            &hub,
            "127.0.0.1".parse().unwrap(),
            Instant::now() + Duration::from_secs(5),
        );
        let traffic = async {
            connect5(&mut peer, !reconnect).await;
            if !reconnect {
                peer.write_all(&publish(true, 2, 1, b'a')).await.unwrap();
                assert_eq!(frame(&mut peer).await, [0x50, 2, 0, 1]);
                // Receiver-owned duplicate does not consume a second slot.
                peer.write_all(&publish(true, 2, 1, b'b')).await.unwrap();
                assert_eq!(frame(&mut peer).await, [0x50, 2, 0, 1]);
                peer.write_all(&[0x30, 5, 0, 1, b't', 0, b'z'])
                    .await
                    .unwrap();
                for id in [2, 3] {
                    peer.write_all(&publish(true, 1, id, b'c')).await.unwrap();
                    assert_eq!(frame(&mut peer).await, [0x40, 2, 0, id]);
                }
                peer.write_all(&[0x62, 2, 0, 9]).await.unwrap();
                assert_eq!(frame(&mut peer).await, [0x70, 2, 0, 9]);
                peer.write_all(&publish(true, 2, 4, b'd')).await.unwrap();
                assert_eq!(frame(&mut peer).await, [0x50, 2, 0, 4]);
                for _ in 0..2 {
                    peer.write_all(&[0x62, 2, 0, 1]).await.unwrap();
                    assert_eq!(frame(&mut peer).await, [0x70, 2, 0, 1]);
                }
                peer.write_all(&publish(true, 2, 5, b'e')).await.unwrap();
                assert_eq!(frame(&mut peer).await, [0x50, 2, 0, 5]);
            } else {
                // Persistent records 4/5 do not fill the new network window.
                peer.write_all(&publish(true, 1, 6, b'f')).await.unwrap();
                assert_eq!(frame(&mut peer).await, [0x40, 2, 0, 6]);
                peer.write_all(&publish(true, 2, 5, b'g')).await.unwrap();
                assert_eq!(frame(&mut peer).await, [0x50, 2, 0, 5]);
                // An old PUBREL without PUBLISH on this connection adds no credit.
                for _ in 0..2 {
                    peer.write_all(&[0x62, 2, 0, 4]).await.unwrap();
                    assert_eq!(frame(&mut peer).await, [0x70, 2, 0, 4]);
                }
                peer.write_all(&publish(true, 2, 7, b'h')).await.unwrap();
                assert_eq!(frame(&mut peer).await, [0x50, 2, 0, 7]);
            }
            // QoS0 remains usable when the reliable receive window is full.
            peer.write_all(&[0x30, 5, 0, 1, b't', 0, b'z', 0xc0, 0])
                .await
                .unwrap();
            assert_eq!(frame(&mut peer).await, [0xd0, 0]);
            peer.write_all(&publish(true, 1, 8, b'i')).await.unwrap();
            assert_eq!(frame(&mut peer).await, [0xe0, 2, 0x93, 0]);
            assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
        };
        let (result, ()) = tokio::time::timeout(Duration::from_secs(3), async {
            tokio::join!(protocol, traffic)
        })
        .await
        .unwrap();
        result.unwrap();
        drop(life);
    }
    assert_eq!(state.lock().unwrap().status().received_publications, 10);
    app.shutdown_checked(modorch::ShutdownOptions::graceful())
        .await
        .unwrap();
}

#[tokio::test]
async fn queued_duplicate_pubcomp_cannot_release_reused_identifier_receive_quota() {
    let cfg = crate::MqttBrokerConfig {
        mqtt5: true,
        max_inbound_qos2_per_client: 2,
        ..crate::MqttBrokerConfig::loopback_development()
    };
    let mut initial = State::new(Arc::new(cfg));
    initial.active = true;
    initial.generation = 1;
    initial.connections = 1;
    let state = Arc::new(Mutex::new(initial));
    let mut life = lifetime(state.clone());
    let app = modorch::ApplicationBuilder::new("duplicate-completion")
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();
    let hub = app.event_hub().external();
    let (server, mut peer) = tokio::io::duplex(1);
    let protocol = run(
        Box::new(server),
        &mut life,
        CancellationToken::new(),
        &hub,
        "127.0.0.1".parse().unwrap(),
        Instant::now() + Duration::from_secs(5),
    );
    let traffic = async {
        connect5(&mut peer, true).await;
        peer.write_all(&publish(true, 2, 1, b'a')).await.unwrap();
        assert_eq!(frame(&mut peer).await, [0x50, 2, 0, 1]);
        peer.write_all(&[0x62, 2, 0, 1]).await.unwrap();
        assert_eq!(peer.read_u8().await.unwrap(), 0x70);
        // Queue a second PUBCOMP before the first is fully written.
        peer.write_all(&[0x62, 2, 0, 1, 0xc0]).await.unwrap();
        let mut tail = [0; 3];
        peer.read_exact(&mut tail).await.unwrap();
        assert_eq!(tail, [2, 0, 1]);
        peer.write_all(&[0]).await.unwrap();
        // Complete the first exchange, then reuse its ID while the duplicate
        // PUBCOMP still has only one byte of buffer space and cannot finish.
        peer.write_all(&publish(true, 2, 1, b'b')).await.unwrap();
        peer.write_all(&[0xc0, 0]).await.unwrap();
        assert_eq!(frame(&mut peer).await, [0x70, 2, 0, 1]);
        assert_eq!(frame(&mut peer).await, [0xd0, 0]);
        assert_eq!(frame(&mut peer).await, [0x50, 2, 0, 1]);
        assert_eq!(frame(&mut peer).await, [0xd0, 0]);
        peer.write_all(&publish(true, 2, 2, b'c')).await.unwrap();
        assert_eq!(frame(&mut peer).await, [0x50, 2, 0, 2]);
        peer.write_all(&publish(true, 1, 3, b'd')).await.unwrap();
        assert_eq!(frame(&mut peer).await, [0xe0, 2, 0x93, 0]);
        assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(protocol, traffic)
    })
    .await
    .unwrap();
    result.unwrap();
    assert_eq!(state.lock().unwrap().status().received_publications, 3);
    drop(life);
    app.shutdown_checked(modorch::ShutdownOptions::graceful())
        .await
        .unwrap();
}

#[tokio::test]
async fn interrupted_partial_ack_reconnect_preserves_receiver_ownership_and_reclaims_connection_budget()
 {
    // Protocol x ACK phase x cancellation/peer EOF, using a one-byte transport.
    // Persistent receiver state survives; network window and queued bytes do not.
    for v5 in [false, true] {
        for (qos, release) in [(1, false), (2, false), (2, true)] {
            for canceled in [false, true] {
                let cfg = crate::MqttBrokerConfig {
                    mqtt5: true,
                    max_inbound_qos2_per_client: 2,
                    ..crate::MqttBrokerConfig::loopback_development()
                };
                let mut initial = State::new(Arc::new(cfg));
                initial.active = true;
                initial.generation = 1;
                let state = Arc::new(Mutex::new(initial));
                let app = modorch::ApplicationBuilder::new("partial-ack-reconnect")
                    .build()
                    .unwrap()
                    .start()
                    .await
                    .unwrap();
                let hub = app.event_hub().external();
                // First connection is interrupted, second resumes, third clears
                // the Session explicitly so every owned byte must be returned.
                for phase in 0..3 {
                    state.lock().unwrap().connections += 1;
                    let mut life = lifetime(state.clone());
                    let (server, mut peer) = tokio::io::duplex(1);
                    let cancel = CancellationToken::new();
                    let protocol = run(
                        Box::new(server),
                        &mut life,
                        cancel.clone(),
                        &hub,
                        "127.0.0.1".parse().unwrap(),
                        Instant::now() + Duration::from_secs(5),
                    );
                    let traffic = async {
                        let clean = phase == 2 || (v5 && phase == 0);
                        let mut body = vec![
                            0,
                            4,
                            b'M',
                            b'Q',
                            b'T',
                            b'T',
                            if v5 { 5 } else { 4 },
                            if clean { 2 } else { 0 },
                            0,
                            0,
                        ];
                        if v5 {
                            if phase == 2 {
                                body.push(0);
                            } else {
                                body.extend([5, 0x11, 0, 0, 0, 60]);
                            }
                        }
                        body.extend([0, 1, b'x']);
                        peer.write_all(&codec::frame(0x10, &body)).await.unwrap();
                        let reply = frame(&mut peer).await;
                        assert_eq!(reply[0], 0x20);
                        assert_eq!(reply[2], u8::from(phase == 1));
                        if phase == 0 {
                            peer.write_all(&publish(v5, qos, 1, b'a')).await.unwrap();
                            if release {
                                assert_eq!(frame(&mut peer).await, [0x50, 2, 0, 1]);
                                peer.write_all(&[0x62, 2, 0, 1]).await.unwrap();
                            }
                            let head = if release {
                                0x70
                            } else if qos == 1 {
                                0x40
                            } else {
                                0x50
                            };
                            assert_eq!(peer.read_u8().await.unwrap(), head);
                            if canceled {
                                cancel.cancel();
                                // Keep peer open: owner cancellation alone must
                                // stop the blocked write and close its endpoint.
                                let mut remaining = Vec::new();
                                peer.read_to_end(&mut remaining).await.unwrap();
                                assert!(remaining.len() < 3);
                            }
                            // Drop the peer with ACK still partly buffered. EOF
                            // and owner cancellation must both stop blocked I/O.
                        } else if phase == 1 {
                            assert_eq!(
                                state.lock().unwrap().status().incoming_qos2,
                                usize::from(qos == 2 && !release)
                            );
                            peer.write_all(&[0x62, 2, 0, 1]).await.unwrap();
                            assert_eq!(frame(&mut peer).await, [0x70, 2, 0, 1]);
                            peer.write_all(&publish(v5, 2, 1, b'b')).await.unwrap();
                            assert_eq!(frame(&mut peer).await, [0x50, 2, 0, 1]);
                            peer.write_all(&[0x62, 2, 0, 1]).await.unwrap();
                            assert_eq!(frame(&mut peer).await, [0x70, 2, 0, 1]);
                            peer.write_all(&[0xc0, 0]).await.unwrap();
                            assert_eq!(frame(&mut peer).await, [0xd0, 0]);
                            peer.write_all(&[0xe0, 0]).await.unwrap();
                        } else {
                            peer.write_all(&[0xe0, 0]).await.unwrap();
                        }
                        drop(peer);
                    };
                    let (result, ()) = tokio::time::timeout(Duration::from_secs(3), async {
                        tokio::join!(protocol, traffic)
                    })
                    .await
                    .unwrap();
                    if phase == 0 && !canceled {
                        assert!(matches!(result, Ok(()) | Err(MqttError::Io(_))));
                    } else {
                        result.unwrap();
                    }
                    drop(life);
                    assert_eq!(state.lock().unwrap().connections, 0);
                }
                let status = state.lock().unwrap().status();
                assert_eq!(status.sessions, 0);
                assert_eq!(status.incoming_qos2, 0);
                assert_eq!(status.memory_used_bytes, 0);
                assert_eq!(status.generation, 1);
                app.shutdown_checked(modorch::ShutdownOptions::graceful())
                    .await
                    .unwrap();
            }
        }
    }
}

#[tokio::test]
async fn duplex_same_id_partial_ack_interrupt_matrix_preserves_outbound_and_receiver_state() {
    for v5 in [false, true] {
        for outbound_release in [false, true] {
            for partial_pubcomp in [false, true] {
                for canceled in [false, true] {
                    eprintln!(
                        "duplex partial ACK matrix: mqtt5={v5} outbound_release={outbound_release} pubcomp={partial_pubcomp} canceled={canceled}"
                    );
                    let cfg = Arc::new(crate::MqttBrokerConfig {
                        mqtt5: true,
                        ..crate::MqttBrokerConfig::loopback_development()
                    });
                    let mut initial = State::new(cfg);
                    initial.active = true;
                    initial.generation = 1;
                    let state = Arc::new(Mutex::new(initial));
                    let app = modorch::ApplicationBuilder::new("duplex-partial-ack")
                        .build()
                        .unwrap()
                        .start()
                        .await
                        .unwrap();
                    let hub = app.event_hub().external();
                    for phase in 0..3 {
                        state.lock().unwrap().connections += 1;
                        let mut life = lifetime(state.clone());
                        let (server, mut peer) = tokio::io::duplex(1);
                        let cancel = CancellationToken::new();
                        let protocol = run(
                            Box::new(server),
                            &mut life,
                            cancel.clone(),
                            &hub,
                            "127.0.0.1".parse().unwrap(),
                            Instant::now() + Duration::from_secs(5),
                        );
                        let traffic = async {
                            let clean = phase == 2 || (v5 && phase == 0);
                            let mut body = vec![
                                0,
                                4,
                                b'M',
                                b'Q',
                                b'T',
                                b'T',
                                if v5 { 5 } else { 4 },
                                if clean { 2 } else { 0 },
                                0,
                                0,
                            ];
                            if v5 {
                                if phase == 2 {
                                    body.push(0); // Clean Start and zero expiry release the new Session too.
                                } else {
                                    body.extend([5, 0x11, 0, 0, 0, 60]);
                                }
                            }
                            body.extend([0, 1, b'x']);
                            peer.write_all(&codec::frame(0x10, &body)).await.unwrap();
                            let connack = frame(&mut peer).await;
                            assert_eq!(connack[0], 0x20);
                            assert_eq!(connack[2], u8::from(phase == 1));
                            if phase == 0 {
                                let mut sub = vec![0, 8];
                                if v5 {
                                    sub.push(0);
                                }
                                sub.extend([0, 3, b'o', b'u', b't', 2]);
                                peer.write_all(&codec::frame(0x82, &sub)).await.unwrap();
                                assert_eq!(
                                    frame(&mut peer).await,
                                    if v5 {
                                        vec![0x90, 4, 0, 8, 0, 2]
                                    } else {
                                        vec![0x90, 3, 0, 8, 2]
                                    }
                                );
                                let mut seed = vec![0, 3, b'o', b'u', b't', 0, 7];
                                if v5 {
                                    seed.push(0);
                                }
                                seed.push(b'o');
                                peer.write_all(&codec::frame(0x34, &seed)).await.unwrap();
                                assert_eq!(frame(&mut peer).await, [0x50, 2, 0, 7]);
                                let mut expected = vec![0, 3, b'o', b'u', b't', 0, 1];
                                if v5 {
                                    expected.push(0);
                                }
                                expected.push(b'o');
                                assert_eq!(frame(&mut peer).await, codec::frame(0x34, &expected));
                                peer.write_all(&[0x62, 2, 0, 7]).await.unwrap();
                                assert_eq!(frame(&mut peer).await, [0x70, 2, 0, 7]);
                                if outbound_release {
                                    peer.write_all(&[0x50, 2, 0, 1]).await.unwrap();
                                    assert_eq!(frame(&mut peer).await, [0x62, 2, 0, 1]);
                                }
                                peer.write_all(&publish(v5, 2, 1, b'a')).await.unwrap();
                                if partial_pubcomp {
                                    assert_eq!(frame(&mut peer).await, [0x50, 2, 0, 1]);
                                    peer.write_all(&[0x62, 2, 0, 1]).await.unwrap();
                                }
                                assert_eq!(
                                    peer.read_u8().await.unwrap(),
                                    if partial_pubcomp { 0x70 } else { 0x50 }
                                );
                                if canceled {
                                    cancel.cancel();
                                    let mut tail = Vec::new();
                                    peer.read_to_end(&mut tail).await.unwrap();
                                    assert!(tail.len() < 3);
                                }
                            } else if phase == 1 {
                                assert_eq!(
                                    state.lock().unwrap().status().incoming_qos2,
                                    usize::from(!partial_pubcomp)
                                );
                                assert_eq!(state.lock().unwrap().status().pending_messages, 1);
                                if outbound_release {
                                    assert_eq!(frame(&mut peer).await, [0x62, 2, 0, 1]);
                                } else {
                                    let mut expected = vec![0, 3, b'o', b'u', b't', 0, 1];
                                    if v5 {
                                        expected.push(0);
                                    }
                                    expected.push(b'o');
                                    assert_eq!(
                                        frame(&mut peer).await,
                                        codec::frame(0x3c, &expected)
                                    );
                                }
                                if !partial_pubcomp {
                                    peer.write_all(&publish(v5, 2, 1, b'b')).await.unwrap();
                                    assert_eq!(frame(&mut peer).await, [0x50, 2, 0, 1]);
                                }
                                peer.write_all(&[0x62, 2, 0, 1]).await.unwrap();
                                assert_eq!(frame(&mut peer).await, [0x70, 2, 0, 1]);
                                assert_eq!(state.lock().unwrap().status().pending_messages, 1);
                                if !outbound_release {
                                    peer.write_all(&[0x50, 2, 0, 1]).await.unwrap();
                                    assert_eq!(frame(&mut peer).await, [0x62, 2, 0, 1]);
                                }
                                peer.write_all(&[0x70, 2, 0, 1, 0xc0, 0]).await.unwrap();
                                assert_eq!(frame(&mut peer).await, [0xd0, 0]);
                                assert_eq!(state.lock().unwrap().status().pending_messages, 0);
                                peer.write_all(&publish(v5, 2, 1, b'c')).await.unwrap();
                                assert_eq!(frame(&mut peer).await, [0x50, 2, 0, 1]);
                                peer.write_all(&[0x62, 2, 0, 1]).await.unwrap();
                                assert_eq!(frame(&mut peer).await, [0x70, 2, 0, 1]);
                                peer.write_all(&[0xe0, 0]).await.unwrap();
                            } else {
                                peer.write_all(&[0xe0, 0]).await.unwrap();
                            }
                            drop(peer);
                        };
                        let (result, ()) = tokio::time::timeout(Duration::from_secs(3), async {
                            tokio::join!(protocol, traffic)
                        })
                        .await
                        .unwrap();
                        if phase == 0 && !canceled {
                            assert!(matches!(result, Ok(()) | Err(MqttError::Io(_))));
                        } else {
                            result.unwrap();
                        }
                        drop(life);
                        assert_eq!(state.lock().unwrap().connections, 0);
                    }
                    let status = state.lock().unwrap().status();
                    assert_eq!(status.sessions, 0);
                    assert_eq!(status.pending_messages, 0);
                    assert_eq!(status.incoming_qos2, 0);
                    assert_eq!(status.memory_used_bytes, 0);
                    app.shutdown_checked(modorch::ShutdownOptions::graceful())
                        .await
                        .unwrap();
                }
            }
        }
    }
}
