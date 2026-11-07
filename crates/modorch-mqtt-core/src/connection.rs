use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};

use modorch::{CancellationToken, TaskScheduler};
use modorch_mqtt_api::MqttError;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{Notify, OwnedSemaphorePermit},
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
    pub attached: Option<(String, u64)>,
    pub graceful: bool,
    pub _permit: OwnedSemaphorePermit,
    pub _memory: Charge,
}

impl Drop for ConnectionLifetime {
    fn drop(&mut self) {
        let mut state = self.state.lock().unwrap();
        if let Some((client, connection)) = &self.attached {
            state.detach(client, *connection, self.graceful);
        }
        state.connections -= 1;
        self.drained.notify_one();
    }
}

pub(crate) fn spawn(
    tasks: &TaskScheduler,
    stream: TcpStream,
    mut lifetime: ConnectionLifetime,
) -> modorch::FrameworkResult<()> {
    tasks.spawn(move |cancel| async move {
        // A malformed client, IO failure or quota rejection closes only this
        // connection. It must never restart/fail the whole component.
        let _ = run(stream, &mut lifetime, cancel).await;
        Ok(())
    })?;
    Ok(())
}

async fn run(
    mut stream: TcpStream,
    life: &mut ConnectionLifetime,
    cancel: CancellationToken,
) -> Result<(), MqttError> {
    stream.set_nodelay(true)?;
    let config = life.state.lock().unwrap().config.clone();
    // Prevent geometric Vec growth from exceeding the charged receive-buffer
    // bound. The connection reservation is acquired before allocating this.
    let mut input = Vec::with_capacity(config.max_packet_bytes);
    let mut read = [0u8; 4096];
    let mut control = VecDeque::<Vec<u8>>::new();
    let mut current: Option<(Vec<u8>, usize)> = None;
    let mut notify = Arc::new(Notify::new());
    let mut keep_alive = None;
    let mut last_packet = Instant::now();
    let mut write_deadline = Instant::now() + config.write_timeout;
    let handshake_deadline = Instant::now() + config.handshake_timeout;
    let mut close_after_write = false;
    loop {
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
            if let Some(len) = codec::frame_len(&input, config.max_packet_bytes)? {
                let decoded =
                    codec::decode(&input[..len], config.max_subscriptions_per_client.min(1000));
                input.drain(..len);
                last_packet = Instant::now();
                match life.attached.as_ref() {
                    None => match decoded {
                        Ok(Packet::Connect(connect)) => {
                            keep_alive = (connect.keep_alive != 0).then(|| {
                                Duration::from_millis(u64::from(connect.keep_alive) * 1500)
                            });
                            let attached = life.state.lock().unwrap().attach(
                                life.generation,
                                &connect,
                                cancel.clone(),
                            );
                            match attached {
                                Ok((client, id, present, session_notify)) => {
                                    life.attached = Some((client, id));
                                    notify = session_notify;
                                    control.push_back(vec![0x20, 2, u8::from(present), 0]);
                                }
                                Err(error) => {
                                    let reason = match error {
                                        MqttError::Capacity(_) | MqttError::Closed => 3,
                                        MqttError::Protocol("reserved internal namespace") => 5,
                                        _ => 2,
                                    };
                                    control.push_back(vec![0x20, 2, 0, reason]);
                                    close_after_write = true;
                                }
                            }
                        }
                        Err(MqttError::Unsupported("MQTT protocol level")) => {
                            control.push_back(vec![0x20, 2, 0, 1]);
                            close_after_write = true;
                        }
                        Err(MqttError::Unsupported("authentication")) => {
                            control.push_back(vec![0x20, 2, 0, 5]);
                            close_after_write = true;
                        }
                        _ => {
                            return Err(MqttError::Protocol(
                                "first packet must be a valid CONNECT",
                            ));
                        }
                    },
                    Some((client, connection)) => {
                        let mut state = life.state.lock().unwrap();
                        state.check(life.generation, client, *connection)?;
                        match decoded? {
                            Packet::Publish { message, packet_id } => {
                                state.publish(life.generation, client, *connection, message)?;
                                if let Some(id) = packet_id {
                                    control.push_back(codec::frame(0x40, &id.to_be_bytes()));
                                }
                            }
                            Packet::PubAck(id) => {
                                state.ack(life.generation, client, *connection, id)?
                            }
                            Packet::Subscribe { id, filters } => {
                                let codes = state.subscribe(
                                    life.generation,
                                    client,
                                    *connection,
                                    filters,
                                )?;
                                let mut reply = id.to_be_bytes().to_vec();
                                reply.extend(codes);
                                control.push_back(codec::frame(0x90, &reply));
                            }
                            Packet::Unsubscribe { id, filters } => {
                                state.unsubscribe(life.generation, client, *connection, filters)?;
                                control.push_back(codec::frame(0xb0, &id.to_be_bytes()));
                            }
                            Packet::Ping => control.push_back(vec![0xd0, 0]),
                            Packet::Disconnect => {
                                life.graceful = true;
                                return Ok(());
                            }
                            Packet::Connect(_) => {
                                return Err(MqttError::Protocol("second CONNECT"));
                            }
                        }
                    }
                }
            }
        }

        if current.is_none() {
            let next = if let Some(frame) = control.pop_front() {
                Some(frame)
            } else if !close_after_write {
                if let Some((client, id)) = &life.attached {
                    life.state
                        .lock()
                        .unwrap()
                        .next_outbound(life.generation, client, *id)?
                } else {
                    None
                }
            } else {
                None
            };
            if let Some(frame) = next {
                if frame.len() > config.max_packet_bytes {
                    return Err(MqttError::Capacity("outbound packet"));
                }
                write_deadline = Instant::now() + config.write_timeout;
                current = Some((frame, 0));
            } else if close_after_write {
                return Ok(());
            }
        }

        // Buffered complete frames should make progress without waiting for a new
        // TCP read; still poll IO each turn so a flood cannot starve ACK writes.
        let buffered = !close_after_write
            && control.len() < config.per_client_outbound_queue
            && codec::frame_len(&input, config.max_packet_bytes)?.is_some();
        let deadline = if life.attached.is_none() {
            handshake_deadline
        } else {
            keep_alive.map_or_else(
                || Instant::now() + Duration::from_secs(86400),
                |k| last_packet + k,
            )
        };
        let write_timer = if current.is_some() {
            write_deadline
        } else {
            Instant::now() + Duration::from_secs(86400)
        };
        let (reader, writer) = stream.split();
        let mut reader = reader;
        let mut writer = writer;
        let capacity = (config.max_packet_bytes - input.len()).min(read.len());
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(()),
            _ = tokio::time::sleep_until(deadline) => return Err(MqttError::Protocol("handshake/keepalive timeout")),
            _ = tokio::time::sleep_until(write_timer) => return Err(MqttError::Protocol("slow writer timeout")),
            result = async {
                let (bytes, offset) = current.as_ref().unwrap();
                writer.write(&bytes[*offset..]).await
            }, if current.is_some() => {
                let written = result?;
                if written == 0 { return Err(MqttError::Protocol("socket write closed")); }
                let (bytes, offset) = current.as_mut().unwrap();
                *offset += written;
                if *offset == bytes.len() { current = None; }
            }
            _ = std::future::ready(()), if buffered => {},
            result = reader.read(&mut read[..capacity]), if !close_after_write && capacity > 0 => {
                let len = result?;
                if len == 0 { return Ok(()); }
                input.extend_from_slice(&read[..len]);
            }
            _ = notify.notified(), if life.attached.is_some() => {},
        }
    }
}
