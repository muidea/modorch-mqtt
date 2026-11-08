use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};

use modorch::{
    CancellationToken, Destination, ExternalEventHub, MessageOptions, PublishPolicy, TaskScheduler,
};
use modorch_mqtt_api::{MqttError, MqttInbound, Principal, PublishKind, Qos};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{Notify, OwnedSemaphorePermit, watch},
    time::Instant,
};

use crate::{
    budget::Charge,
    codec::{self, Packet},
    state::State,
};

pub(crate) type SharedState = Arc<Mutex<State>>;

/// RAII owns the socket task's session, quota and memory until actual task exit,
/// including panic/abort and executor rejection before its first poll.
pub(crate) struct ConnectionLifetime {
    pub state: SharedState,
    pub drained: Arc<Notify>,
    pub generation: u64,
    pub peer_ip: std::net::IpAddr,
    pub attached: Option<(String, u64)>,
    pub graceful: bool,
    pub _permit: Option<OwnedSemaphorePermit>,
    pub _memory: Option<Charge>,
    pub persistence: crate::persistence::Persistence,
}

pub(crate) struct DeferredDetach {
    pub client: String,
    pub connection: u64,
    pub graceful: bool,
    pub _permit: Option<OwnedSemaphorePermit>,
    pub _memory: Option<Charge>,
}

impl Drop for ConnectionLifetime {
    fn drop(&mut self) {
        let mut state = self.state.lock().unwrap();
        if let Some((client, connection)) = &self.attached {
            if state.config.storage.is_some() {
                state.deferred.push_back(DeferredDetach {
                    client: client.clone(),
                    connection: *connection,
                    graceful: self.graceful,
                    _permit: self._permit.take(),
                    _memory: self._memory.take(),
                });
            } else {
                state.detach(client, *connection, self.graceful);
            }
        }
        state.connections -= 1;
        state.release_peer(self.peer_ip);
        self.drained.notify_one();
    }
}

pub(crate) fn spawn(
    tasks: &TaskScheduler,
    stream: TcpStream,
    mut lifetime: ConnectionLifetime,
    hub: ExternalEventHub,
    transport: crate::transport::Transport,
) -> modorch::FrameworkResult<()> {
    // Include scheduler queue time and TLS negotiation in the CONNECT budget.
    let deadline = Instant::now() + lifetime.state.lock().unwrap().config.handshake_timeout;
    tasks.spawn(move |cancel| async move {
        // A malformed client, IO failure or quota rejection closes only this
        // connection. It must never restart/fail the whole component.
        let result = async {
            stream.set_nodelay(true)?;
            let peer_ip = stream.peer_addr()?.ip();
            let stream = transport.accept(stream, &cancel, deadline).await?;
            run(stream, &mut lifetime, cancel, &hub, peer_ip, deadline).await
        }
        .await;
        if let Err(error) = result {
            lifetime.state.lock().unwrap().record_client_error(&error);
        }
        Ok(())
    })?;
    Ok(())
}

#[derive(Clone, Copy)]
struct Deadlines {
    network: Option<Instant>,
    identity: Option<Instant>,
    write: Option<Instant>,
}
impl Deadlines {
    fn earliest(self) -> Option<Instant> {
        [self.network, self.identity, self.write]
            .into_iter()
            .flatten()
            .min()
    }
}

async fn run(
    stream: crate::transport::Stream,
    life: &mut ConnectionLifetime,
    cancel: CancellationToken,
    hub: &ExternalEventHub,
    peer_ip: std::net::IpAddr,
    handshake_deadline: Instant,
) -> Result<(), MqttError> {
    let socket = crate::transport::Interruptible::new(stream);
    let local_cancel = cancel.child_token();
    let (deadlines, mut updates) = watch::channel(Deadlines {
        network: Some(handshake_deadline),
        identity: None,
        write: None,
    });
    let protocol = run_protocol(
        Box::new(socket.clone()),
        life,
        local_cancel.clone(),
        hub,
        peer_ip,
        handshake_deadline,
        deadlines,
    );
    tokio::pin!(protocol);
    loop {
        let limits = *updates.borrow_and_update();
        let deadline = limits.earliest();
        tokio::select! { biased;
            _ = local_cancel.cancelled() => {
                socket.close();
                // Keep lifetime, permit and memory in this managed task until
                // any durable promise has a known outcome.
                return protocol.await;
            }
            _ = async {
                match deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            } => {
                socket.close();
                local_cancel.cancel();
                let reason = if limits.write == deadline {
                    MqttError::Protocol("slow writer timeout")
                } else if limits.identity == deadline {
                    MqttError::NotAuthorized
                } else {
                    MqttError::Protocol("handshake/keepalive timeout")
                };
                // Canceled-state fencing is expected after a deadline; preserve
                // the timeout reason for metrics rather than reporting Closed.
                return match protocol.await {
                    Err(MqttError::Closed) | Ok(()) => Err(reason),
                    Err(error) => Err(error),
                };
            }
            result = &mut protocol => return result,
            _ = updates.changed() => (),
        }
    }
}

async fn run_protocol(
    stream: crate::transport::Stream,
    life: &mut ConnectionLifetime,
    cancel: CancellationToken,
    hub: &ExternalEventHub,
    peer_ip: std::net::IpAddr,
    handshake_deadline: Instant,
    deadlines: watch::Sender<Deadlines>,
) -> Result<(), MqttError> {
    let (mut reader, mut writer) = tokio::io::split(stream);
    let mut principal: Option<Principal> = None;
    let config = life.state.lock().unwrap().config.clone();
    let mut wire = crate::wire::Protocol::new(config.mqtt5);
    // Prevent geometric Vec growth from exceeding the charged receive-buffer
    // bound. The connection reservation is acquired before allocating this.
    let mut input = Vec::with_capacity(config.max_packet_bytes);
    let mut read = [0u8; 4096];
    let mut control = VecDeque::<Vec<u8>>::new();
    let mut current: Option<(Vec<u8>, usize)> = None;
    let mut notify = Arc::new(Notify::new());
    let mut keep_alive = None;
    let mut close_after_write = false;
    loop {
        if principal.as_ref().is_some_and(|p| !p.is_valid()) {
            return Err(MqttError::NotAuthorized);
        }
        if cancel.is_cancelled() {
            return Ok(());
        }
        if let Some((client, connection)) = &life.attached {
            life.state
                .lock()
                .unwrap()
                .check(life.generation, client, *connection)?;
        } else if !life.state.lock().unwrap().active {
            return Err(MqttError::Closed);
        }

        // Process at most one packet per turn; input persists across every select
        // cancellation. Bound control replies before consuming another packet.
        if !close_after_write && control.len() < config.per_client_outbound_queue {
            if let Some(len) = wire.frame_len(&input, config.max_packet_bytes)? {
                let decoded =
                    wire.decode(&input[..len], config.max_subscriptions_per_client.min(1000));
                input.drain(..len);
                let last_packet = Instant::now();
                if life.attached.is_some() {
                    deadlines.send_modify(|d| d.network = keep_alive.map(|k| last_packet + k));
                }
                match life.attached.as_ref() {
                    None => match decoded {
                        Ok(Packet::Connect(connect)) => {
                            keep_alive = (connect.keep_alive != 0).then(|| {
                                Duration::from_millis(u64::from(connect.keep_alive) * 1500)
                            });
                            let attached = crate::auth::attach(
                                life,
                                hub,
                                connect,
                                &cancel,
                                handshake_deadline,
                                peer_ip,
                            )
                            .await;
                            match attached {
                                Ok((client, id, present, session_notify, identity)) => {
                                    deadlines.send_modify(|d| {
                                        d.network = keep_alive.map(|k| last_packet + k);
                                        d.identity = crate::auth::deadline(&identity);
                                    });
                                    principal = Some(identity);
                                    life.attached = Some((client, id));
                                    notify = session_notify;
                                    control.push_back(vec![0x20, 2, u8::from(present), 0]);
                                }
                                Err(error) => {
                                    let Some(frame) = connack_failure(&error) else {
                                        return Err(error);
                                    };
                                    life.state.lock().unwrap().record_client_error(&error);
                                    control.push_back(frame);
                                    close_after_write = true;
                                }
                            }
                        }
                        Err(MqttError::Unsupported("MQTT protocol level")) => {
                            control.push_back(vec![0x20, 2, 0, 1]);
                            close_after_write = true;
                        }
                        _ => {
                            return Err(MqttError::Protocol(
                                "first packet must be a valid CONNECT",
                            ));
                        }
                    },
                    Some((client, connection)) => {
                        let mut packet = decoded?;
                        let duplicate = if let Packet::Publish {
                            message,
                            packet_id: Some(id),
                        } = &packet
                        {
                            life.state.lock().unwrap().incoming_duplicate(
                                life.generation,
                                client,
                                *connection,
                                *id,
                                message,
                            )?
                        } else {
                            false
                        };
                        if let (Some(identity), Some(provider)) = (&principal, &config.auth) {
                            let authorize = async {
                                match &mut packet {
                                    Packet::Publish { message, .. } if !duplicate => {
                                        crate::auth::publish(
                                            hub,
                                            &config,
                                            identity,
                                            message,
                                            PublishKind::ClientPublish,
                                        )
                                        .await?;
                                    }
                                    Packet::Subscribe { filters, .. } => {
                                        for (filter, qos) in filters {
                                            if crate::auth::subscribe(
                                                hub,
                                                &config,
                                                identity,
                                                crate::auth::acl_filter(filter, wire.is_v5()),
                                            )
                                            .await
                                            .is_err()
                                            {
                                                *qos = None;
                                            }
                                        }
                                    }
                                    _ => (),
                                }
                                Ok::<(), MqttError>(())
                            };
                            let deadline = Instant::now()
                                + provider.timeout.min(crate::auth::expiry(identity));
                            tokio::select! { biased;
                                _ = cancel.cancelled() => return Ok(()),
                                _ = tokio::time::sleep_until(deadline) => return Err(MqttError::NotAuthorized),
                                result = authorize => result?,
                            }
                        }
                        let mut observation = None;
                        let transaction = tokio::select! { biased;
                            _ = cancel.cancelled() => return Ok(()),
                            result = life.persistence.begin(&life.state) => result?,
                        };
                        let mut disconnected = false;
                        let result = (|| -> Result<(), MqttError> {
                            let mut state = life.state.lock().unwrap();
                            state.check(life.generation, client, *connection)?;
                            match packet {
                                Packet::Publish { message, packet_id } => {
                                    let first = if message.qos == Qos::ExactlyOnce {
                                        state.receive_qos2(
                                            life.generation,
                                            client,
                                            *connection,
                                            packet_id.unwrap(),
                                            message.clone(),
                                        )?
                                    } else {
                                        state.publish(
                                            life.generation,
                                            client,
                                            *connection,
                                            message.clone(),
                                        )?;
                                        true
                                    };
                                    let qos = message.qos;
                                    if first {
                                        observation = Some(MqttInbound {
                                            properties: message.properties.clone(),
                                            broker: config.name.clone(),
                                            generation: life.generation,
                                            connection_id: *connection,
                                            client_id: client.clone(),
                                            topic: message.topic,
                                            payload: message.payload,
                                            qos: message.qos,
                                            retain: message.retain,
                                            packet_id,
                                            received_at_ms: std::time::SystemTime::now()
                                                .duration_since(std::time::UNIX_EPOCH)
                                                .unwrap_or_default()
                                                .as_millis()
                                                .min(u128::from(u64::MAX))
                                                as u64,
                                        });
                                    }
                                    if let Some(id) = packet_id {
                                        control.push_back(codec::frame(
                                            if qos == Qos::ExactlyOnce { 0x50 } else { 0x40 },
                                            &id.to_be_bytes(),
                                        ));
                                    }
                                }
                                Packet::PubAck(id) => {
                                    state.ack(life.generation, client, *connection, id)?
                                }
                                Packet::PubRec(id) => {
                                    if wire.negative_rec(id) {
                                        state.reject_received(
                                            life.generation,
                                            client,
                                            *connection,
                                            id,
                                        )?;
                                    } else if let Some(frame) = state.outgoing_received(
                                        life.generation,
                                        client,
                                        *connection,
                                        id,
                                    )? {
                                        control.push_back(frame);
                                    }
                                }
                                Packet::PubRel(id) => {
                                    state.incoming_release(
                                        life.generation,
                                        client,
                                        *connection,
                                        id,
                                    )?;
                                    control.push_back(codec::frame(0x70, &id.to_be_bytes()));
                                }
                                Packet::PubComp(id) => state.outgoing_completed(
                                    life.generation,
                                    client,
                                    *connection,
                                    id,
                                )?,
                                Packet::Subscribe { id, filters } => {
                                    let subscription_options = wire.subscribe_options();
                                    let codes = state.subscribe_with_options(
                                        life.generation,
                                        client,
                                        *connection,
                                        filters,
                                        subscription_options,
                                    )?;
                                    let mut reply = id.to_be_bytes().to_vec();
                                    reply.extend(codes);
                                    control.push_back(codec::frame(0x90, &reply));
                                }
                                Packet::Unsubscribe { id, filters } => {
                                    let reasons = filters
                                        .iter()
                                        .map(|f| {
                                            if state.has_subscription(client, f) {
                                                0
                                            } else {
                                                0x11
                                            }
                                        })
                                        .collect::<Vec<u8>>();
                                    state.unsubscribe(
                                        life.generation,
                                        client,
                                        *connection,
                                        filters,
                                    )?;
                                    let mut body = id.to_be_bytes().to_vec();
                                    if wire.is_v5() {
                                        body.extend(reasons);
                                    }
                                    control.push_back(codec::frame(0xb0, &body));
                                }
                                Packet::Ping => control.push_back(vec![0xd0, 0]),
                                Packet::Disconnect => {
                                    if let Some(seconds) = wire.disconnect_expiry() {
                                        state.set_disconnect_expiry(client, seconds)?;
                                    }
                                    state.detach(client, *connection, !wire.disconnect_will());
                                    disconnected = true;
                                }
                                Packet::Connect(_) => {
                                    return Err(MqttError::Protocol("second CONNECT"));
                                }
                            }
                            Ok(())
                        })();
                        transaction
                            .commit(crate::persistence::Hub::External(hub))
                            .await?;
                        result?;
                        if disconnected {
                            life.graceful = true;
                            life.attached = None;
                            return Ok(());
                        }
                        if let Some(event) = observation {
                            observe(life, hub, event);
                        }
                    }
                }
            }
        }

        if current.is_none() {
            let next = if let Some(frame) = control.pop_front() {
                Some(wire.response(
                    frame,
                    life.attached.as_ref().map(|(c, _)| c.as_str()),
                    config.max_packet_bytes,
                    config.max_inbound_qos2_per_client,
                ))
            } else if !close_after_write {
                if let Some((client, id)) = &life.attached {
                    let transaction = tokio::select! { biased;
                        _ = cancel.cancelled() => return Ok(()),
                        result = life.persistence.begin(&life.state) => result?,
                    };
                    let next =
                        life.state
                            .lock()
                            .unwrap()
                            .next_outbound(life.generation, client, *id);
                    transaction
                        .commit(crate::persistence::Hub::External(hub))
                        .await?;
                    next?
                } else {
                    None
                }
            } else {
                None
            };
            if let Some(frame) = next {
                if frame.len() > config.max_packet_bytes || frame.len() > wire.max_packet() {
                    return Err(MqttError::Capacity("outbound packet"));
                }
                let write_deadline = Instant::now() + config.write_timeout;
                current = Some((frame, 0));
                deadlines.send_modify(|d| d.write = Some(write_deadline));
            } else if close_after_write {
                return Ok(());
            }
        }

        // Buffered complete frames should make progress without waiting for a new
        // TCP read; still poll IO each turn so a flood cannot starve ACK writes.
        let buffered = !close_after_write
            && control.len() < config.per_client_outbound_queue
            && wire.frame_len(&input, config.max_packet_bytes)?.is_some();
        let capacity = (config.max_packet_bytes - input.len()).min(read.len());
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(()),
            result = async {
                let (bytes, offset) = current.as_ref().unwrap();
                writer.write(&bytes[*offset..]).await
            }, if current.is_some() => {
                let written = result?;
                life.state.lock().unwrap().record_io(0, written);
                if written == 0 { return Err(MqttError::Protocol("socket write closed")); }
                let (bytes, offset) = current.as_mut().unwrap();
                *offset += written;
                if *offset == bytes.len() {
                    current = None;
                    deadlines.send_modify(|d| d.write = None);
                }
            }
            _ = std::future::ready(()), if buffered => {},
            result = reader.read(&mut read[..capacity]), if !close_after_write && capacity > 0 => {
                let len = result?;
                life.state.lock().unwrap().record_io(len, 0);
                if len == 0 { return Ok(()); }
                input.extend_from_slice(&read[..len]);
            }
            _ = notify.notified(), if life.attached.is_some() => {},
        }
    }
}

// Table3.1: only identifier rejection may use code2. In particular, storage
// failure is server-unavailable; an unclassified error must close without a
// CONNACK rather than invent an identifier failure (MQTT-3.2.2-6).
fn connack_failure(error: &MqttError) -> Option<Vec<u8>> {
    let reason = match error {
        MqttError::Protocol(
            "invalid ClientID" | "ClientID exceeds configured limit" | "empty persistent ClientID",
        ) => 2,
        MqttError::Capacity(_)
        | MqttError::Closed
        | MqttError::SessionConflict
        | MqttError::Protocol("persistent storage unavailable") => 3,
        MqttError::Protocol("reserved internal namespace") | MqttError::NotAuthorized => 5,
        _ => return None,
    };
    Some(vec![0x20, 2, 0, reason])
}

// Nonblocking fanout outside the broker mutex. Epochs identify acceptance even
// if shutdown races with the independent observation after this fencing check.
fn observe(life: &ConnectionLifetime, hub: &ExternalEventHub, event: MqttInbound) {
    if life
        .state
        .lock()
        .unwrap()
        .check(event.generation, &event.client_id, event.connection_id)
        .is_err()
    {
        life.state.lock().unwrap().inbound_unobserved += 1;
        return;
    }
    let cost = 256
        + event.broker.len()
        + event.client_id.len()
        + event.topic.len()
        + event.payload.len()
        + event.properties.byte_cost();
    let topic = event.topic.clone();
    let report = hub.publish(
        &topic,
        Destination::Broadcast,
        event,
        MessageOptions {
            payload_bytes: Some(cost),
            ..Default::default()
        },
        PublishPolicy::Reject,
    );
    let mut state = life.state.lock().unwrap();
    match report {
        Ok(report) if report.accepted() > 0 => {
            state.inbound_observed += 1;
            if report.is_partial() {
                state.inbound_partial += 1;
            }
        }
        _ => state.inbound_unobserved += 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_failures_use_table31_reasons_and_unknown_failures_have_no_connack() {
        // Compare complete wire frames against the protocol return-code table.
        assert_eq!(
            connack_failure(&MqttError::Protocol("invalid ClientID")).unwrap(),
            [0x20, 2, 0, 2]
        );
        assert_eq!(
            connack_failure(&MqttError::Protocol("persistent storage unavailable")).unwrap(),
            [0x20, 2, 0, 3]
        );
        assert_eq!(
            connack_failure(&MqttError::NotAuthorized).unwrap(),
            [0x20, 2, 0, 5]
        );
        for error in [
            MqttError::Protocol("unclassified failure"),
            MqttError::Unsupported("other capability"),
            MqttError::Io(std::io::ErrorKind::BrokenPipe.into()),
        ] {
            assert!(connack_failure(&error).is_none());
        }
    }
    #[tokio::test]
    async fn stalled_frame_write_expires_despite_incoming_ping_traffic() {
        // A one-byte duplex models a receiver that stops draining mid-CONNACK,
        // without relying on kernel socket buffer sizes or host load.
        let mut cfg = crate::MqttBrokerConfig::loopback_development();
        cfg.write_timeout = Duration::from_millis(100);
        let mut state = State::new(Arc::new(cfg));
        state.active = true;
        state.generation = 1;
        state.connections = 1;
        let mut life = ConnectionLifetime {
            state: Arc::new(Mutex::new(state)),
            drained: Arc::new(Notify::new()),
            generation: 1,
            peer_ip: "127.0.0.1".parse().unwrap(),
            attached: None,
            graceful: false,
            _permit: None,
            _memory: None,
            persistence: Default::default(),
        };
        let app = modorch::ApplicationBuilder::new("write-deadline")
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
            Instant::now() + Duration::from_secs(2),
        );
        let traffic = async {
            let mut body = vec![0, 4, b'M', b'Q', b'T', b'T', 4, 2, 0, 0, 0, 1, b'x'];
            peer.write_all(&codec::frame(0x10, &body)).await.unwrap();
            assert_eq!(peer.read_u8().await.unwrap(), 0x20);
            // Retain the unread rest of CONNACK while sending complete packets.
            while peer.write_all(&[0xc0, 0]).await.is_ok() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            body.clear();
            peer.read_to_end(&mut body).await.unwrap();
            assert!(body.len() < 3, "the stalled CONNACK must never finish");
        };
        let (result, ()) = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::join!(protocol, traffic)
        })
        .await
        .unwrap();
        assert!(matches!(
            result,
            Err(MqttError::Protocol("slow writer timeout"))
        ));
        drop(life);
        app.shutdown_checked(modorch::ShutdownOptions::graceful())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn partial_publish_write_timeout_preserves_qos1_and_qos2_for_dup_reconnect() {
        for qos in [Qos::AtLeastOnce, Qos::ExactlyOnce] {
            let cfg = crate::MqttBrokerConfig {
                write_timeout: Duration::from_millis(120),
                ..crate::MqttBrokerConfig::loopback_development()
            };
            let mut initial = State::new(Arc::new(cfg));
            initial.active = true;
            initial.generation = 1;
            initial.connections = 1;
            let state = Arc::new(Mutex::new(initial));
            let make_life = || ConnectionLifetime {
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
            let app = modorch::ApplicationBuilder::new("partial-publish")
                .build()
                .unwrap()
                .start()
                .await
                .unwrap();
            let hub = app.event_hub().external();
            let connect = [
                0x10, 13, 0, 4, b'M', b'Q', b'T', b'T', 4, 0, 0, 0, 0, 1, b'x',
            ];
            let mut life = make_life();
            let (server, mut peer) = tokio::io::duplex(1);
            let protocol = run(
                Box::new(server),
                &mut life,
                CancellationToken::new(),
                &hub,
                "127.0.0.1".parse().unwrap(),
                Instant::now() + Duration::from_secs(3),
            );
            let traffic = async {
                peer.write_all(&connect).await.unwrap();
                let mut connack = [0; 4];
                peer.read_exact(&mut connack).await.unwrap();
                assert_eq!(connack, [0x20, 2, 0, 0]);
                peer.write_all(&[0x82, 6, 0, 1, 0, 1, b'w', qos as u8])
                    .await
                    .unwrap();
                let mut suback = [0; 5];
                peer.read_exact(&mut suback).await.unwrap();
                assert_eq!(suback, [0x90, 3, 0, 1, qos as u8]);
                state
                    .lock()
                    .unwrap()
                    .publish(
                        1,
                        "x",
                        1,
                        modorch_mqtt_api::MqttMessage {
                            topic: "w".into(),
                            payload: bytes::Bytes::from_static(b"hello"),
                            qos,
                            retain: false,
                            properties: Default::default(),
                        },
                    )
                    .unwrap();
                assert_eq!(peer.read_u8().await.unwrap(), 0x30 | (qos as u8) << 1);
                // Leave the rest unread until the socket supervisor interrupts it.
                tokio::time::sleep(Duration::from_millis(240)).await;
                let mut remainder = Vec::new();
                peer.read_to_end(&mut remainder).await.unwrap();
                assert!(remainder.len() < 11, "PUBLISH must remain incomplete");
            };
            let (result, ()) = tokio::time::timeout(Duration::from_secs(3), async {
                tokio::join!(protocol, traffic)
            })
            .await
            .unwrap();
            assert!(matches!(
                result,
                Err(MqttError::Protocol("slow writer timeout"))
            ));
            drop(life);
            assert_eq!(state.lock().unwrap().status().pending_messages, 1);
            assert_eq!(state.lock().unwrap().connections, 0);
            state.lock().unwrap().connections = 1;
            let mut life = make_life();
            let (server, mut peer) = tokio::io::duplex(128);
            let protocol = run(
                Box::new(server),
                &mut life,
                CancellationToken::new(),
                &hub,
                "127.0.0.1".parse().unwrap(),
                Instant::now() + Duration::from_secs(3),
            );
            let traffic = async {
                peer.write_all(&connect).await.unwrap();
                let mut actual = [0; 16];
                peer.read_exact(&mut actual).await.unwrap();
                assert_eq!(
                    actual,
                    [
                        0x20,
                        2,
                        1,
                        0,
                        0x38 | (qos as u8) << 1,
                        10,
                        0,
                        1,
                        b'w',
                        0,
                        1,
                        b'h',
                        b'e',
                        b'l',
                        b'l',
                        b'o'
                    ]
                );
                if qos == Qos::ExactlyOnce {
                    peer.write_all(&[0x50, 2, 0, 1]).await.unwrap();
                    let mut release = [0; 4];
                    peer.read_exact(&mut release).await.unwrap();
                    assert_eq!(release, [0x62, 2, 0, 1]);
                }
                peer.write_all(&[
                    if qos == Qos::ExactlyOnce { 0x70 } else { 0x40 },
                    2,
                    0,
                    1,
                    0xc0,
                    0,
                ])
                .await
                .unwrap();
                let mut pong = [0; 2];
                peer.read_exact(&mut pong).await.unwrap();
                assert_eq!(pong, [0xd0, 0]);
                peer.write_all(&[0xe0, 0]).await.unwrap();
                assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
            };
            let (result, ()) = tokio::time::timeout(Duration::from_secs(3), async {
                tokio::join!(protocol, traffic)
            })
            .await
            .unwrap();
            result.unwrap();
            drop(life);
            assert_eq!(state.lock().unwrap().status().pending_messages, 0);
            assert_eq!(state.lock().unwrap().connections, 0);
            app.shutdown_checked(modorch::ShutdownOptions::graceful())
                .await
                .unwrap();
        }
    }
}
