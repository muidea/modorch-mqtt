use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};

use modorch::CancellationToken;
use modorch_mqtt_api::{BrokerStatus, MqttError, MqttMessage, Qos};
use tokio::sync::Notify;

use crate::{
    budget::{Budget, Charge},
    codec::{self, Connect},
    config::MqttBrokerConfig,
};

pub(crate) const RESERVED: &str = "$modorch/";
type Result<T> = std::result::Result<T, MqttError>;

pub(crate) struct Subscription {
    qos: Qos,
    _charge: Charge,
}
pub(crate) struct Delivery {
    message: MqttMessage,
    qos: Qos,
    retain: bool,
    id: Option<u16>,
    sent: bool,
    ever_sent: bool,
    _charge: Charge,
}
impl Delivery {
    fn bytes(&self) -> usize {
        message_cost(&self.message)
    }
}
struct Retained {
    message: MqttMessage,
    _charge: Charge,
}
struct Connection {
    id: u64,
    cancel: CancellationToken,
}
pub(crate) struct Session {
    epoch: u64,
    clean: bool,
    online: Option<Connection>,
    subscriptions: BTreeMap<String, Subscription>,
    pending: VecDeque<Delivery>,
    qos0: VecDeque<Delivery>,
    next_packet_id: u16,
    notify: Arc<Notify>,
    _charge: Charge,
}

#[derive(Clone)]
struct Target {
    client: String,
    epoch: u64,
    qos: Qos,
}
struct Will {
    message: MqttMessage,
    targets: Option<Vec<Target>>,
    _charge: Charge,
}

/// All state mutations are short, synchronous critical sections. No network or
/// provider I/O occurs while holding the broker mutex in this memory milestone.
pub(crate) struct State {
    pub config: Arc<MqttBrokerConfig>,
    pub budget: Arc<Budget>,
    pub generation: u64,
    pub active: bool,
    next_id: u64,
    sessions: BTreeMap<String, Session>,
    retained: BTreeMap<String, Retained>,
    wills: BTreeMap<u64, Will>,
    pub address: Option<std::net::SocketAddr>,
    pub connections: usize,
    pub rejected_connections: u64,
    rejected_publications: u64,
    dropped_qos0: u64,
    #[cfg(test)]
    pub fail_listener: bool,
    #[cfg(test)]
    pub fail_activations: usize,
}

pub(crate) fn message_cost(message: &MqttMessage) -> usize {
    192 + message.topic.len() + message.payload.len()
}

fn allowed_topic(topic: &str) -> Result<()> {
    codec::validate_topic(topic)?;
    if topic.starts_with(RESERVED) || topic == "$modorch" {
        return Err(MqttError::Protocol("reserved internal namespace"));
    }
    Ok(())
}

impl State {
    pub fn new(config: Arc<MqttBrokerConfig>) -> Self {
        Self {
            budget: Budget::new(config.memory_budget_bytes),
            config,
            generation: 0,
            active: false,
            next_id: 0,
            sessions: BTreeMap::new(),
            retained: BTreeMap::new(),
            wills: BTreeMap::new(),
            address: None,
            connections: 0,
            rejected_connections: 0,
            rejected_publications: 0,
            dropped_qos0: 0,
            #[cfg(test)]
            fail_listener: false,
            #[cfg(test)]
            fail_activations: 0,
        }
    }

    pub fn status(&self) -> BrokerStatus {
        BrokerStatus {
            generation: self.generation,
            active: self.active,
            connections: self.connections,
            sessions: self.sessions.len(),
            retained: self.retained.len(),
            pending_wills: self
                .wills
                .values()
                .filter(|will| will.targets.is_some())
                .count(),
            memory_used_bytes: self.budget.used(),
            rejected_connections: self.rejected_connections,
            rejected_publications: self.rejected_publications,
            dropped_qos0: self.dropped_qos0,
        }
    }

    pub fn begin_generation(&mut self) -> Result<u64> {
        if self.connections != 0 {
            return Err(MqttError::Closed);
        }
        self.active = false;
        self.generation = self.generation.checked_add(1).ok_or(MqttError::Closed)?;
        // Rebuilds reuse the whole authoritative in-memory session map. A future
        // secondary trie must be reconstructed from every session, including offline.
        self.process_wills();
        Ok(self.generation)
    }

    fn next_id(&mut self) -> Result<u64> {
        self.next_id = self.next_id.checked_add(1).ok_or(MqttError::Closed)?;
        Ok(self.next_id)
    }

    pub fn check(&self, generation: u64, client: &str, connection: u64) -> Result<()> {
        if !self.active || self.generation != generation {
            return Err(MqttError::Closed);
        }
        self.check_connection(client, connection)
    }

    fn check_connection(&self, client: &str, connection: u64) -> Result<()> {
        if self
            .sessions
            .get(client)
            .and_then(|s| s.online.as_ref())
            .is_some_and(|c| c.id == connection)
        {
            Ok(())
        } else {
            Err(MqttError::Closed)
        }
    }

    pub fn attach(
        &mut self,
        generation: u64,
        connect: &Connect,
        cancel: CancellationToken,
    ) -> Result<(String, u64, bool, Arc<Notify>)> {
        if !self.active || self.generation != generation {
            return Err(MqttError::Closed);
        }
        if connect.client_id.len() > self.config.max_client_id_bytes {
            return Err(MqttError::Protocol("ClientID exceeds configured limit"));
        }
        if connect.client_id.is_empty() && !connect.clean {
            return Err(MqttError::Protocol("empty persistent ClientID"));
        }
        // This milestone has one explicitly anonymous identity domain only. A
        // future Auth adapter must add owner/ClientID/Will checks BEFORE this point.
        if let Some(will) = &connect.will {
            allowed_topic(&will.topic)?;
        }
        let mut connection = self.next_id()?;
        let client = if connect.client_id.is_empty() {
            loop {
                let candidate = format!("m{connection:x}");
                if candidate.len() > self.config.max_client_id_bytes {
                    return Err(MqttError::Capacity("assigned ClientID space"));
                }
                if !self.sessions.contains_key(&candidate) {
                    break candidate;
                }
                connection = self.next_id()?;
            }
        } else {
            connect.client_id.clone()
        };
        let existed = self.sessions.contains_key(&client);
        if !existed && self.sessions.len() >= self.config.max_sessions {
            return Err(MqttError::Capacity("sessions"));
        }
        let present = !connect.clean && self.sessions.get(&client).is_some_and(|s| !s.clean);
        // Allocate all resources before disturbing the old connection or session.
        let new_session = if !present {
            Some(Session {
                epoch: connection,
                clean: connect.clean,
                online: None,
                subscriptions: BTreeMap::new(),
                pending: VecDeque::new(),
                qos0: VecDeque::new(),
                next_packet_id: 1,
                notify: Arc::new(Notify::new()),
                _charge: self.budget.reserve(512 + client.len())?,
            })
        } else {
            None
        };
        let will = if let Some(message) = &connect.will {
            if self.wills.len() >= self.config.max_will_records {
                return Err(MqttError::Capacity("Will records"));
            }
            Some(Will {
                message: message.clone(),
                targets: None,
                _charge: self.budget.reserve(
                    message_cost(message)
                        + self.config.max_sessions * (96 + self.config.max_client_id_bytes),
                )?,
            })
        } else {
            None
        };
        if let Some(old) = self.sessions.get_mut(&client).and_then(|s| s.online.take()) {
            old.cancel.cancel();
            self.trigger_will(old.id);
        }
        if let Some(session) = new_session {
            self.sessions.insert(client.clone(), session);
        }
        let session = self.sessions.get_mut(&client).unwrap();
        session.online = Some(Connection {
            id: connection,
            cancel,
        });
        session.qos0.clear();
        for delivery in &mut session.pending {
            delivery.sent = false;
        }
        let notify = session.notify.clone();
        if let Some(will) = will {
            self.wills.insert(connection, will);
        }
        Ok((client, connection, present, notify))
    }

    pub fn detach(&mut self, client: &str, connection: u64, graceful: bool) {
        if graceful {
            self.wills.remove(&connection);
        } else {
            self.trigger_will(connection);
        }
        if self.check_connection(client, connection).is_ok() {
            if self.sessions[client].clean {
                self.sessions.remove(client);
            } else if let Some(session) = self.sessions.get_mut(client) {
                session.online = None;
                session.qos0.clear();
            }
        }
        self.process_wills();
    }

    fn targets(&self, message: &MqttMessage) -> Vec<Target> {
        self.sessions
            .iter()
            .filter_map(|(client, session)| {
                let qos = session
                    .subscriptions
                    .iter()
                    .filter(|(filter, _)| codec::matches(filter, &message.topic))
                    .map(|(_, sub)| sub.qos.min(message.qos))
                    .max()?;
                if qos == Qos::AtMostOnce && session.online.is_none() {
                    return None;
                }
                Some(Target {
                    client: client.clone(),
                    epoch: session.epoch,
                    qos,
                })
            })
            .collect()
    }

    fn trigger_will(&mut self, connection: u64) {
        let targets = self
            .wills
            .get(&connection)
            .filter(|w| w.targets.is_none())
            .map(|will| self.targets(&will.message));
        if let Some(targets) = targets {
            self.wills.get_mut(&connection).unwrap().targets = Some(targets);
        }
    }

    pub fn process_wills(&mut self) {
        let ids: Vec<_> = self
            .wills
            .iter()
            .filter_map(|(id, w)| w.targets.is_some().then_some(*id))
            .collect();
        for id in ids {
            let will = self.wills.remove(&id).unwrap();
            if self
                .commit_publication(&will.message, will.targets.as_ref().unwrap())
                .is_err()
            {
                self.wills.insert(id, will);
            }
        }
    }

    pub fn publish(
        &mut self,
        generation: u64,
        client: &str,
        connection: u64,
        message: MqttMessage,
    ) -> Result<()> {
        self.check(generation, client, connection)?;
        allowed_topic(&message.topic)?;
        let _routing = self
            .budget
            .reserve(self.sessions.len() * (96 + self.config.max_client_id_bytes))?;
        let targets = self.targets(&message);
        let result = self.commit_publication(&message, &targets);
        if result.is_err() {
            self.rejected_publications += 1;
        }
        result
    }

    fn commit_publication(&mut self, message: &MqttMessage, targets: &[Target]) -> Result<()> {
        let cost = message_cost(message);
        let mut planned = Vec::new();
        for target in targets {
            let Some(session) = self
                .sessions
                .get(&target.client)
                .filter(|s| s.epoch == target.epoch)
            else {
                continue;
            };
            if target.qos == Qos::AtMostOnce {
                continue;
            }
            if session.pending.len() >= self.config.max_offline_messages_per_client
                || session
                    .pending
                    .iter()
                    .map(Delivery::bytes)
                    .sum::<usize>()
                    .saturating_add(cost)
                    > self.config.max_pending_bytes_per_client
            {
                return Err(MqttError::Capacity("reliable session backlog"));
            }
            let delivery = Delivery {
                message: message.clone(),
                qos: target.qos,
                retain: false,
                id: None,
                sent: false,
                ever_sent: false,
                _charge: self.budget.reserve(cost)?,
            };
            planned.push((target.client.clone(), delivery));
        }
        let retained = if message.retain && !message.payload.is_empty() {
            if !self.retained.contains_key(&message.topic)
                && self.retained.len() >= self.config.max_retained_topics
            {
                return Err(MqttError::Capacity("retained topics"));
            }
            Some(Retained {
                message: message.clone(),
                _charge: self.budget.reserve(cost)?,
            })
        } else {
            None
        };
        // All fallible reliable/retained preparation has completed. Mutex ownership
        // makes the following writes one atomic acceptance transaction.
        if message.retain {
            if let Some(retained) = retained {
                self.retained.insert(message.topic.clone(), retained);
            } else {
                self.retained.remove(&message.topic);
            }
        }
        for (client, delivery) in planned {
            let session = self.sessions.get_mut(&client).unwrap();
            session.pending.push_back(delivery);
            session.notify.notify_one();
        }
        for target in targets.iter().filter(|t| t.qos == Qos::AtMostOnce) {
            let Some(session) = self
                .sessions
                .get_mut(&target.client)
                .filter(|s| s.epoch == target.epoch && s.online.is_some())
            else {
                continue;
            };
            if session.qos0.len() >= self.config.per_client_outbound_queue {
                self.dropped_qos0 += 1;
                continue;
            }
            let Ok(charge) = self.budget.reserve(cost) else {
                self.dropped_qos0 += 1;
                continue;
            };
            session.qos0.push_back(Delivery {
                message: message.clone(),
                qos: Qos::AtMostOnce,
                retain: false,
                id: None,
                sent: false,
                ever_sent: false,
                _charge: charge,
            });
            session.notify.notify_one();
        }
        Ok(())
    }

    pub fn subscribe(
        &mut self,
        generation: u64,
        client: &str,
        connection: u64,
        filters: Vec<(String, Qos)>,
    ) -> Result<Vec<u8>> {
        self.check(generation, client, connection)?;
        let mut codes = Vec::with_capacity(filters.len());
        for (filter, qos) in filters {
            if filter.starts_with("$modorch") || qos == Qos::ExactlyOnce {
                codes.push(0x80);
                continue;
            }
            let replay_bytes = self
                .retained
                .values()
                .filter(|r| codec::matches(&filter, &r.message.topic))
                .map(|r| 96 + r.message.topic.len())
                .sum();
            let Ok(_replay_memory) = self.budget.reserve(replay_bytes) else {
                codes.push(0x80);
                continue;
            };
            let replay: Vec<_> = self
                .retained
                .values()
                .filter(|r| codec::matches(&filter, &r.message.topic))
                .map(|r| r.message.clone())
                .collect();
            match self.subscribe_one(client, filter, qos, replay) {
                Ok(()) => codes.push(qos as u8),
                Err(_) => codes.push(0x80),
            }
        }
        Ok(codes)
    }

    fn subscribe_one(
        &mut self,
        client: &str,
        filter: String,
        qos: Qos,
        replay: Vec<MqttMessage>,
    ) -> Result<()> {
        let session = self.sessions.get_mut(client).unwrap();
        if !session.subscriptions.contains_key(&filter)
            && session.subscriptions.len() >= self.config.max_subscriptions_per_client
        {
            return Err(MqttError::Capacity("subscriptions"));
        }
        let subscription = Subscription {
            qos,
            _charge: self.budget.reserve(128 + filter.len())?,
        };
        let mut reliable = Vec::new();
        let mut qos0 = Vec::new();
        let mut pending_bytes = session.pending.iter().map(Delivery::bytes).sum::<usize>();
        for message in replay {
            let granted = qos.min(message.qos);
            let cost = message_cost(&message);
            if granted != Qos::AtMostOnce {
                pending_bytes = pending_bytes.saturating_add(cost);
                if session.pending.len() + reliable.len()
                    >= self.config.max_offline_messages_per_client
                    || pending_bytes > self.config.max_pending_bytes_per_client
                {
                    return Err(MqttError::Capacity("retained replay backlog"));
                }
            } else if session.qos0.len() + qos0.len() >= self.config.per_client_outbound_queue {
                return Err(MqttError::Capacity("retained replay QoS0 queue"));
            }
            let delivery = Delivery {
                message,
                qos: granted,
                retain: true,
                id: None,
                sent: false,
                ever_sent: false,
                _charge: self.budget.reserve(cost)?,
            };
            if granted == Qos::AtMostOnce {
                qos0.push(delivery);
            } else {
                reliable.push(delivery);
            }
        }
        session.subscriptions.insert(filter, subscription);
        session.pending.extend(reliable);
        session.qos0.extend(qos0);
        session.notify.notify_one();
        Ok(())
    }

    pub fn unsubscribe(
        &mut self,
        generation: u64,
        client: &str,
        connection: u64,
        filters: Vec<String>,
    ) -> Result<()> {
        self.check(generation, client, connection)?;
        let session = self.sessions.get_mut(client).unwrap();
        for filter in filters {
            session.subscriptions.remove(&filter);
        }
        Ok(())
    }

    pub fn ack(&mut self, generation: u64, client: &str, connection: u64, id: u16) -> Result<()> {
        self.check(generation, client, connection)?;
        let session = self.sessions.get_mut(client).unwrap();
        if let Some(index) = session
            .pending
            .iter()
            .position(|d| d.id == Some(id) && d.ever_sent)
        {
            session.pending.remove(index);
        }
        session.notify.notify_one();
        self.process_wills();
        Ok(())
    }

    pub fn next_outbound(
        &mut self,
        generation: u64,
        client: &str,
        connection: u64,
    ) -> Result<Option<Vec<u8>>> {
        self.check(generation, client, connection)?;
        let session = self.sessions.get_mut(client).unwrap();
        let inflight = session.pending.iter().filter(|d| d.sent).count();
        if inflight < self.config.max_inflight_per_client {
            if let Some(index) = session.pending.iter().position(|d| !d.sent) {
                if session.pending[index].id.is_none() {
                    let id = loop {
                        let candidate = session.next_packet_id;
                        session.next_packet_id = if candidate == u16::MAX {
                            1
                        } else {
                            candidate + 1
                        };
                        if !session.pending.iter().any(|d| d.id == Some(candidate)) {
                            break candidate;
                        }
                    };
                    session.pending[index].id = Some(id);
                }
                let delivery = &mut session.pending[index];
                let bytes = codec::publish(
                    &delivery.message,
                    delivery.qos,
                    delivery.retain,
                    delivery.id,
                    delivery.ever_sent,
                );
                delivery.sent = true;
                delivery.ever_sent = true;
                return Ok(Some(bytes));
            }
        }
        Ok(session
            .qos0
            .pop_front()
            .map(|d| codec::publish(&d.message, d.qos, d.retain, None, false)))
    }

    pub fn stop(&mut self, generation: u64) {
        if self.generation == generation {
            self.active = false;
            for session in self.sessions.values() {
                if let Some(connection) = &session.online {
                    connection.cancel.cancel();
                }
            }
        }
    }
}
