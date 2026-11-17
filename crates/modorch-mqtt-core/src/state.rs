use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};

use modorch::{CancellationToken, ComponentRef};
use modorch_mqtt_api::*;
use tokio::sync::Notify;

mod qos2;
pub(crate) mod snapshot;

use crate::{
    budget::{Budget, Charge},
    codec::{self, Connect},
    config::MqttBrokerConfig,
};

pub(crate) const RESERVED: &str = "$modorch/";
type Result<T> = std::result::Result<T, MqttError>;

pub(crate) struct Subscription {
    qos: Qos,
    options: crate::wire::SubscribeOptions,
    _charge: Charge,
}
pub(crate) struct Delivery {
    message: MqttMessage,
    qos: Qos,
    retain: bool,
    id: Option<u16>,
    sent: bool,
    // Connection-local PUBLISH quota; retransmitted PUBREL consumes none.
    quota_used: bool,
    ever_sent: bool,
    phase: DeliveryPhase,
    _charge: Charge,
}
#[derive(Clone, Copy, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
enum DeliveryPhase {
    #[default]
    Publish,
    Release {
        ever_sent: bool,
    },
}
struct IncomingQos2 {
    message: MqttMessage,
    _charge: Charge,
}
impl Delivery {
    fn bytes(&self) -> usize {
        message_cost(&self.message)
    }
}
struct Retained {
    message: MqttMessage,
    origin: RetainedOrigin,
    _charge: Charge,
}
#[derive(Clone, serde::Serialize, serde::Deserialize)]
enum RetainedOrigin {
    Client(String),
    Internal,
    // Formats 1–4 did not preserve the publisher. Never guess its identity.
    LegacyUnknown,
}
impl RetainedOrigin {
    fn byte_cost(&self) -> usize {
        32 + match self {
            Self::Client(client) => client.len(),
            _ => 0,
        }
    }
    fn permits(&self, client: &str, no_local: bool) -> bool {
        !no_local
            || match self {
                Self::Client(origin) => origin != client,
                Self::Internal => true,
                Self::LegacyUnknown => false,
            }
    }
}
const MAX_EVENT_HISTORY: usize = 256;
const MAX_ACTIVE_TOPICS: usize = 512;

struct ActiveTopicRecord {
    publish_count: u64,
    last_qos: Qos,
    last_publisher: Option<String>,
    last_published_ms: u64,
    seq: u64,
}

struct Connection {
    id: u64,
    peer_addr: Option<String>,
    keep_alive: u16,
    connected_at_ms: u64,
    cancel: CancellationToken,
    taken_over: Arc<std::sync::atomic::AtomicBool>,
}
pub(crate) struct Session {
    protocol5: bool,
    expiry_interval: Option<u32>,
    expires_at_ms: Option<u64>,
    receive_maximum: usize,
    maximum_packet_size: usize,
    epoch: u64,
    revision: u64,
    owner: PrincipalKey,
    clean: bool,
    online: Option<Connection>,
    subscriptions: BTreeMap<String, Subscription>,
    pending: VecDeque<Delivery>,
    incoming_qos2: BTreeMap<u16, IncomingQos2>,
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
    retain: bool,
    identifiers: Vec<u32>,
}
struct Will {
    client: String,
    delay: u32,
    due: Option<u64>,
    expiry: Option<u32>,
    message: MqttMessage,
    // Authorization is established before CONNECT; expiry later may itself trigger this Will.
    _authorization: Option<PublishGrant>,
    targets: Option<Vec<Target>>,
    _charge: Charge,
}

struct Peer {
    active: usize,
    attempts: usize,
    window: std::time::Instant,
    _charge: Charge,
}

struct PublishRecord {
    request: MqttPublishRequest,
    result: MqttPublishResult,
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
    publications: BTreeMap<(ComponentRef, String), PublishRecord>,
    peers: BTreeMap<std::net::IpAddr, Peer>,
    pub inbound_observed: u64,
    pub inbound_unobserved: u64,
    pub inbound_partial: u64,
    pub address: Option<std::net::SocketAddr>,
    pub connections: usize,
    pub deferred: VecDeque<crate::connection::DeferredDetach>,
    pub storage_poisoned: bool,
    counters: BrokerStatus,
    events: VecDeque<ConnectionEvent>,
    next_event_seq: u64,
    active_topics: BTreeMap<String, ActiveTopicRecord>,
    next_topic_seq: u64,
    pub rejected_connections: u64,
    rejected_publications: u64,
    dropped_qos0: u64,
    #[cfg(test)]
    pub fail_listener: bool,
    #[cfg(test)]
    pub fail_activations: usize,
}

pub(crate) fn message_cost(message: &MqttMessage) -> usize {
    192 + message.topic.len() + message.payload.len() + message.properties.byte_cost()
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
            publications: BTreeMap::new(),
            peers: BTreeMap::new(),
            inbound_observed: 0,
            inbound_unobserved: 0,
            inbound_partial: 0,
            address: None,
            connections: 0,
            deferred: VecDeque::new(),
            storage_poisoned: false,
            counters: BrokerStatus::default(),
            events: VecDeque::with_capacity(MAX_EVENT_HISTORY),
            next_event_seq: 0,
            active_topics: BTreeMap::new(),
            next_topic_seq: 0,
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
            peer_buckets: self.peers.len(),
            subscriptions: self.sessions.values().map(|s| s.subscriptions.len()).sum(),
            pending_messages: self.sessions.values().map(|s| s.pending.len()).sum(),
            pending_bytes: self
                .sessions
                .values()
                .flat_map(|s| &s.pending)
                .map(Delivery::bytes)
                .sum(),
            incoming_qos2: self.sessions.values().map(|s| s.incoming_qos2.len()).sum(),
            incoming_qos2_bytes: self
                .sessions
                .values()
                .flat_map(|s| s.incoming_qos2.values())
                .map(|p| message_cost(&p.message) + 64)
                .sum(),
            inflight: self
                .sessions
                .values()
                .flat_map(|s| &s.pending)
                .filter(|d| d.sent)
                .count(),
            queued_qos0: self.sessions.values().map(|s| s.qos0.len()).sum(),
            will_records: self.wills.len(),
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
            publish_records: self.publications.len(),
            inbound_observed: self.inbound_observed,
            inbound_unobserved: self.inbound_unobserved,
            inbound_partial: self.inbound_partial,
            ..self.counters.clone()
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
        if self.config.storage.is_none() {
            self.process_wills();
        }
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
        self.check_connection(client, connection)?;
        if self.sessions[client]
            .online
            .as_ref()
            .unwrap()
            .cancel
            .is_cancelled()
        {
            return Err(MqttError::Closed);
        }
        Ok(())
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

    pub fn assign_client_id(&mut self) -> Result<String> {
        loop {
            let id = self.next_id()?;
            let candidate = format!("m{id:x}");
            if candidate.len() > self.config.max_client_id_bytes {
                return Err(MqttError::Capacity("assigned ClientID space"));
            }
            if !self.sessions.contains_key(&candidate) {
                return Ok(candidate);
            }
        }
    }

    pub fn auth_view(
        &self,
        principal: &Principal,
        client: &str,
        clean: bool,
    ) -> Result<(AuthorizeSessionAttach, Vec<String>, Charge)> {
        let session = self
            .sessions
            .get(client)
            .filter(|s| s.online.is_some() || s.expires_at_ms.is_none_or(|t| t > unix_time_ms()));
        let bytes = session
            .filter(|_| !clean)
            .map_or(0, |s| s.subscriptions.keys().map(|f| f.len() + 64).sum());
        let charge = self.budget.reserve(bytes)?;
        Ok((
            AuthorizeSessionAttach {
                principal: principal.clone(),
                client_id: client.into(),
                existing_owner: session.map(|s| s.owner.clone()),
                expected_version: session.map(|s| s.revision),
                clean_session: clean,
                takeover: session.is_some_and(|s| s.online.is_some()),
            },
            session
                .filter(|_| !clean)
                .map(|s| s.subscriptions.keys().cloned().collect())
                .unwrap_or_default(),
            charge,
        ))
    }

    #[cfg(test)]
    pub fn attach(
        &mut self,
        generation: u64,
        connect: &Connect,
        cancel: CancellationToken,
        principal: &Principal,
        expected_version: Option<u64>,
        will_grant: Option<PublishGrant>,
    ) -> Result<(String, u64, bool, Arc<Notify>)> {
        self.attach_with_peer(
            generation,
            connect,
            cancel,
            principal,
            expected_version,
            will_grant,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn attach_with_peer(
        &mut self,
        generation: u64,
        connect: &Connect,
        cancel: CancellationToken,
        principal: &Principal,
        expected_version: Option<u64>,
        will_grant: Option<PublishGrant>,
        peer_addr: Option<std::net::SocketAddr>,
    ) -> Result<(String, u64, bool, Arc<Notify>)> {
        if !self.active || self.generation != generation || cancel.is_cancelled() {
            return Err(MqttError::Closed);
        }
        if connect.client_id.len() > self.config.max_client_id_bytes {
            return Err(MqttError::Protocol("ClientID exceeds configured limit"));
        }
        if connect.client_id.is_empty() && !connect.clean && connect.v5.is_none() {
            return Err(MqttError::Protocol("empty persistent ClientID"));
        }
        if !principal.is_valid() {
            return Err(MqttError::NotAuthorized);
        }
        self.process_wills();
        if self.sessions.get(&connect.client_id).map(|s| s.revision) != expected_version {
            return Err(MqttError::SessionConflict);
        }
        if self
            .sessions
            .get(&connect.client_id)
            .is_some_and(|s| s.owner != principal.key)
        {
            return Err(MqttError::NotAuthorized);
        }
        // A Session's subscriptions and QoS ownership belong to one protocol.
        // Reject before allocating IDs, fencing an owner, or replacing state.
        // An explicit Clean Start/Clean Session may create a new Session.
        if !connect.clean
            && self.sessions.get(&connect.client_id).is_some_and(|s| {
                (!s.clean || (s.protocol5 && s.online.is_some()))
                    && s.protocol5 != connect.v5.is_some()
            })
        {
            return Err(MqttError::SessionConflict);
        }
        if let Some(will) = &connect.will {
            allowed_topic(&will.topic)?;
            if self.config.auth.is_some() {
                let expected = PublishAuthContext {
                    principal: Some(principal.clone()),
                    internal_source: None,
                    topic: will.topic.clone(),
                    qos: will.qos,
                    retain: will.retain,
                    kind: PublishKind::Will,
                };
                if !will_grant.as_ref().is_some_and(|g| {
                    g.context == expected && g.policy_version == principal.policy_version
                }) {
                    return Err(MqttError::NotAuthorized);
                }
            }
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
        // MQTT5 zero expiry ends the Session after disconnection, not while
        // its old connection is still online and being taken over.
        let present = !connect.clean
            && self
                .sessions
                .get(&client)
                .is_some_and(|s| !s.clean || (s.protocol5 && s.online.is_some()));
        // Allocate all resources before disturbing the old connection or session.
        let new_session = if !present {
            Some(Session {
                epoch: connection,
                revision: connection,
                owner: principal.key.clone(),
                clean: connect
                    .v5
                    .as_ref()
                    .map_or(connect.clean, |o| o.session_expiry == 0),
                protocol5: connect.v5.is_some(),
                expiry_interval: connect.v5.as_ref().map(|o| o.session_expiry),
                expires_at_ms: None,
                receive_maximum: connect
                    .v5
                    .as_ref()
                    .map_or(usize::MAX, |o| usize::from(o.receive_maximum)),
                maximum_packet_size: connect
                    .v5
                    .as_ref()
                    .map_or(usize::MAX, |o| o.maximum_packet_size as usize),
                online: None,
                subscriptions: BTreeMap::new(),
                pending: VecDeque::new(),
                incoming_qos2: BTreeMap::new(),
                qos0: VecDeque::new(),
                next_packet_id: 1,
                notify: Arc::new(Notify::new()),
                _charge: self.budget.reserve(
                    512 + client.len() + principal.key.domain.len() + principal.key.subject.len(),
                )?,
            })
        } else {
            None
        };
        let will = if let Some(message) = &connect.will {
            if self.wills.len() >= self.config.max_will_records {
                return Err(MqttError::Capacity("Will records"));
            }
            Some(Will {
                client: client.clone(),
                delay: connect.v5.as_ref().map_or(0, |o| o.will_delay),
                expiry: connect.v5.as_ref().and_then(|o| o.will_expiry),
                due: None,
                message: message.clone(),
                _authorization: will_grant,
                targets: None,
                _charge: self.budget.reserve(
                    message_cost(message)
                        + if self.config.auth.is_some() {
                            1024 + message.topic.len()
                        } else {
                            0
                        }
                        + self.config.max_sessions
                            * (96
                                + self.config.max_client_id_bytes
                                + if self.config.mqtt5 {
                                    self.config.max_subscriptions_per_client * 4
                                } else {
                                    0
                                }),
                )?,
            })
        } else {
            None
        };
        if existed {
            self.counters.reconnects += 1;
        }
        // Only a delay that has not elapsed can be canceled by resuming the
        // Session (MQTT5-3.1.2-8). Preserve an overdue Will or frozen targets
        // waiting for reliable capacity, even across repeated reconnects.
        if present {
            let now = unix_time_ms();
            self.wills.retain(|_, w| {
                w.client != client
                    || w.delay == 0
                    || w.targets.is_some()
                    || w.due.is_some_and(|due| due <= now)
            });
        }
        let peer_text = peer_addr.map(|addr| addr.to_string());
        if let Some(old) = self.sessions.get_mut(&client).and_then(|s| s.online.take()) {
            old.taken_over
                .store(true, std::sync::atomic::Ordering::Release);
            old.cancel.cancel();
            self.trigger_will(old.id);
            let replacement = peer_text.as_deref().unwrap_or("unknown");
            self.push_event(
                "taken_over",
                Some(client.clone()),
                old.peer_addr.clone(),
                Some(old.id),
                format!("replaced by new connection from {replacement}"),
            );
        }
        if !present {
            for w in self
                .wills
                .values_mut()
                .filter(|w| w.client == client && w.due.is_some())
            {
                w.due = Some(unix_time_ms());
            }
        }
        if let Some(session) = new_session {
            self.sessions.insert(client.clone(), session);
        }
        let session = self.sessions.get_mut(&client).unwrap();
        let protocol5 = connect.v5.is_some();
        session.protocol5 = protocol5;
        session.expiry_interval = connect.v5.as_ref().map(|o| o.session_expiry);
        session.clean = connect
            .v5
            .as_ref()
            .map_or(connect.clean, |o| o.session_expiry == 0);
        let clean = session.clean;
        session.expires_at_ms = None;
        session.receive_maximum = connect
            .v5
            .as_ref()
            .map_or(usize::MAX, |o| usize::from(o.receive_maximum));
        session.maximum_packet_size = connect
            .v5
            .as_ref()
            .map_or(usize::MAX, |o| o.maximum_packet_size as usize);
        session.revision = connection;
        let now_ms = unix_time_ms();
        session.online = Some(Connection {
            id: connection,
            peer_addr: peer_text.clone(),
            keep_alive: connect.keep_alive,
            connected_at_ms: now_ms,
            cancel,
            taken_over: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        });
        session.qos0.clear();
        for delivery in &mut session.pending {
            delivery.sent = false;
            delivery.quota_used = false;
        }
        let notify = session.notify.clone();
        if let Some(will) = will {
            self.wills.insert(connection, will);
        }
        self.push_event(
            "connected",
            Some(client.clone()),
            peer_text,
            Some(connection),
            format!(
                "protocol={} clean={} keep_alive={}s present={}",
                if protocol5 { "5.0" } else { "3.1.1" },
                clean,
                connect.keep_alive,
                present
            ),
        );
        Ok((client, connection, present, notify))
    }

    pub fn takeover_flag(
        &self,
        client: &str,
        connection: u64,
    ) -> Option<Arc<std::sync::atomic::AtomicBool>> {
        self.sessions
            .get(client)
            .and_then(|s| s.online.as_ref())
            .filter(|c| c.id == connection)
            .map(|c| c.taken_over.clone())
    }

    pub fn detach(&mut self, client: &str, connection: u64, graceful: bool) {
        if graceful {
            self.wills.remove(&connection);
        } else {
            self.trigger_will(connection);
        }
        if self.check_connection(client, connection).is_ok() {
            let peer_addr = self.sessions[client]
                .online
                .as_ref()
                .and_then(|c| c.peer_addr.clone());
            self.push_event(
                "disconnected",
                Some(client.to_owned()),
                peer_addr,
                Some(connection),
                if graceful {
                    "client DISCONNECT".into()
                } else {
                    "connection closed without DISCONNECT".into()
                },
            );
            if self.sessions[client].clean {
                self.sessions.remove(client);
            } else {
                let revision = self.next_id().unwrap_or(u64::MAX);
                let session = self.sessions.get_mut(client).unwrap();
                session.revision = revision;
                session.online = None;
                session.expires_at_ms = session
                    .expiry_interval
                    .filter(|n| *n != u32::MAX)
                    .map(|s| unix_time_ms().saturating_add(u64::from(s) * 1000));
                session.qos0.clear();
            }
        }
        self.process_wills();
    }

    fn targets_from(&self, message: &MqttMessage, origin: Option<&str>) -> Vec<Target> {
        let mut targets = vec![];
        #[cfg(feature = "mqtt5")]
        let mut groups: BTreeMap<String, (Vec<Target>, Vec<Target>)> = BTreeMap::new();
        for (client, session) in &self.sessions {
            let mut quality = None;
            for (filter, sub) in &session.subscriptions {
                if session.protocol5
                    && sub.options.no_local
                    && origin.is_some_and(|o| o.is_empty() || o == client)
                {
                    continue;
                }
                #[cfg(feature = "mqtt5")]
                if session.protocol5 {
                    if let Some((_, actual)) = crate::mqtt5::shared_filter(filter) {
                        let qos = sub.qos.min(message.qos);
                        if codec::matches(actual, &message.topic)
                            && (session.online.is_some()
                                || (!session.clean
                                    && qos != Qos::AtMostOnce
                                    && session.expires_at_ms.is_none_or(|t| t > unix_time_ms())))
                        {
                            let (online, offline) = groups.entry(filter.clone()).or_default();
                            let members = if session.online.is_some() {
                                online
                            } else {
                                offline
                            };
                            members.push(Target {
                                client: client.clone(),
                                epoch: session.epoch,
                                qos,
                                retain: sub.options.retain_as_published && message.retain,
                                identifiers: sub.options.identifier.into_iter().collect(),
                            });
                        }
                        continue;
                    }
                }
                if codec::matches(filter, &message.topic) {
                    quality = Some(quality.map_or(sub.qos.min(message.qos), |q: Qos| {
                        q.max(sub.qos.min(message.qos))
                    }));
                }
            }
            if let Some(qos) = quality {
                if qos != Qos::AtMostOnce || session.online.is_some() {
                    let matching: Vec<_> = session
                        .subscriptions
                        .iter()
                        .filter(|(f, s)| {
                            codec::matches(f, &message.topic)
                                && !(session.protocol5
                                    && s.options.no_local
                                    && origin.is_some_and(|o| o.is_empty() || o == client))
                        })
                        .map(|(_, s)| s)
                        .collect();
                    targets.push(Target {
                        client: client.clone(),
                        epoch: session.epoch,
                        qos,
                        retain: session.protocol5
                            && message.retain
                            && matching.iter().any(|s| s.options.retain_as_published),
                        identifiers: matching
                            .iter()
                            .filter_map(|s| s.options.identifier)
                            .collect(),
                    });
                }
            }
        }
        #[cfg(feature = "mqtt5")]
        for (online, offline) in groups.values() {
            // Select exactly one owner at acceptance. Prefer live consumers;
            // otherwise retain QoS1/2 for one unexpired persistent member.
            // commit_publication applies the same atomic budget checks as for
            // ordinary subscriptions; reconnect never reassigns this owner.
            let members = if online.is_empty() { offline } else { online };
            targets.push(
                members[(self.counters.accepted_publications as usize) % members.len()].clone(),
            );
        }
        let mut merged: BTreeMap<String, Target> = BTreeMap::new();
        for t in targets {
            if let Some(old) = merged.get_mut(&t.client) {
                old.qos = old.qos.max(t.qos);
                old.retain |= t.retain;
                old.identifiers.extend(t.identifiers);
            } else {
                merged.insert(t.client.clone(), t);
            }
        }
        merged.into_values().collect()
    }

    fn trigger_will(&mut self, connection: u64) {
        if let Some(w) = self.wills.get_mut(&connection) {
            if w.due.is_none() && w.targets.is_none() {
                let expiry = self
                    .sessions
                    .get(&w.client)
                    .and_then(|s| s.expiry_interval)
                    .unwrap_or(u32::MAX);
                w.due = Some(unix_time_ms().saturating_add(u64::from(w.delay.min(expiry)) * 1000));
            }
        }
    }

    pub fn process_wills(&mut self) {
        let now = unix_time_ms();
        self.sessions
            .retain(|_, s| s.online.is_some() || s.expires_at_ms.is_none_or(|t| t > now));
        self.retained.retain(|_, r| !r.message.properties.expired());
        let ready: Vec<_> = self
            .wills
            .iter()
            .filter_map(|(id, w)| {
                (w.targets.is_none() && w.due.is_some_and(|t| t <= now)).then_some(*id)
            })
            .collect();
        for id in ready {
            let mut w = self.wills.remove(&id).unwrap();
            w.message.properties.expires_at_ms =
                w.expiry.map(|s| now.saturating_add(u64::from(s) * 1000));
            w.targets = Some(self.targets_from(&w.message, Some(&w.client)));
            self.counters.triggered_wills += 1;
            self.wills.insert(id, w);
        }
        let ids: Vec<_> = self
            .wills
            .iter()
            .filter_map(|(id, w)| w.targets.is_some().then_some(*id))
            .collect();
        for id in ids {
            let will = self.wills.remove(&id).unwrap();
            if self
                .commit_publication(
                    &will.message,
                    will.targets.as_ref().unwrap(),
                    Some(&will.client),
                )
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
        self.route_from(&message, Some(client))?;
        self.counters.received_publications += 1;
        Ok(())
    }

    fn route(&mut self, message: &MqttMessage) -> Result<()> {
        self.route_from(message, None)
    }
    fn route_from(&mut self, message: &MqttMessage, origin: Option<&str>) -> Result<()> {
        let _routing = self.budget.reserve(
            self.sessions.len()
                * (96
                    + self.config.max_client_id_bytes
                    + if self.config.mqtt5 {
                        self.config.max_subscriptions_per_client * 4
                    } else {
                        0
                    }),
        )?;
        let targets = self.targets_from(message, origin);
        let result = self.commit_publication(message, &targets, origin);
        if result.is_err() {
            self.rejected_publications += 1;
        }
        result
    }

    pub fn publish_internal(
        &mut self,
        generation: u64,
        source: &ComponentRef,
        request: &MqttPublishRequest,
    ) -> MqttPublishResult {
        use MqttPublishRejection as Rejection;
        if !self.active || generation != self.generation {
            return Err(Rejection::Inactive);
        }
        validate_request_id(&request.request_id)?;
        let key = (source.clone(), request.request_id.clone());
        if let Some(record) = self.publications.get(&key) {
            return if record.request == *request {
                record.result.clone()
            } else {
                Err(Rejection::IdConflict)
            };
        }
        allowed_topic(&request.topic).map_err(publication_rejection)?;
        if !request.properties.subscription_identifiers.is_empty() {
            return Err(Rejection::Invalid(
                "Subscription Identifiers are broker assigned".into(),
            ));
        }
        if !self.config.mqtt5 && request.properties != MqttProperties::default() {
            return Err(Rejection::Invalid(
                "MQTT5 metadata requires an enabled listener".into(),
            ));
        }
        if request.properties.utf8_payload && std::str::from_utf8(&request.payload).is_err() {
            return Err(Rejection::Invalid("invalid UTF-8 payload".into()));
        }
        #[cfg(feature = "mqtt5")]
        crate::mqtt5::validate_properties(&request.properties).map_err(publication_rejection)?;
        // Account for fixed header, variable remaining length, Topic and packet ID
        // without first allocating an unbounded encoding of an internal payload.
        let remaining = 2usize
            .checked_add(request.topic.len())
            .and_then(|n| n.checked_add(request.payload.len()))
            .and_then(|n| {
                n.checked_add(if self.config.mqtt5 {
                    request.properties.byte_cost() + 5
                } else {
                    0
                })
            })
            .and_then(|n| n.checked_add(if request.qos != Qos::AtMostOnce { 2 } else { 0 }))
            .ok_or(Rejection::Invalid("packet size overflow".into()))?;
        let length_bytes = if remaining < 128 {
            1
        } else if remaining < 16384 {
            2
        } else if remaining < 2097152 {
            3
        } else {
            4
        };
        if remaining > 268_435_455 || remaining + 1 + length_bytes > self.config.max_packet_bytes {
            return Err(Rejection::Invalid("packet exceeds configured bound".into()));
        }
        if self.publications.len() >= self.config.max_publish_records {
            return Err(Rejection::LedgerFull);
        }
        let message = MqttMessage {
            topic: request.topic.clone(),
            payload: request.payload.clone(),
            qos: request.qos,
            retain: request.retain,
            properties: request.properties.clone(),
        };
        let charge = self
            .budget
            .reserve(message_cost(&message) + 256 + request.request_id.len() * 2 + source.id.len())
            .map_err(publication_rejection)?;
        // Reservation and all routing commits share one critical section. Store a
        // terminal result even on routing rejection; retries never re-match targets.
        let result = self
            .route(&message)
            .map_err(publication_rejection)
            .map(|()| MqttAcceptReceipt {
                request_id: request.request_id.clone(),
                accepted: true,
                durability: if self.config.storage.is_some() {
                    AcceptanceDurability::DurableCommitted
                } else {
                    AcceptanceDurability::MemoryOnly
                },
            });
        self.publications.insert(
            key,
            PublishRecord {
                request: request.clone(),
                result: result.clone(),
                _charge: charge,
            },
        );
        result
    }

    pub fn recorded_publication(
        &self,
        generation: u64,
        source: &ComponentRef,
        request: &MqttPublishRequest,
    ) -> Option<MqttPublishResult> {
        if !self.active || generation != self.generation {
            return Some(Err(MqttPublishRejection::Inactive));
        }
        self.publications
            .get(&(source.clone(), request.request_id.clone()))
            .map(|p| {
                if p.request == *request {
                    p.result.clone()
                } else {
                    Err(MqttPublishRejection::IdConflict)
                }
            })
    }
    pub fn record_storage_commit(&mut self, elapsed: std::time::Duration, success: bool) {
        if success {
            self.counters.storage_commits = self.counters.storage_commits.saturating_add(1);
        } else {
            self.counters.storage_commit_failures =
                self.counters.storage_commit_failures.saturating_add(1);
        }
        let micros = elapsed.as_micros().min(u64::MAX as u128) as u64;
        self.counters.storage_commit_duration_micros = self
            .counters
            .storage_commit_duration_micros
            .saturating_add(micros);
        self.counters.storage_commit_max_micros =
            self.counters.storage_commit_max_micros.max(micros);
    }

    pub fn query_publication(
        &self,
        generation: u64,
        source: &ComponentRef,
        request_id: &str,
    ) -> MqttPublishQueryResult {
        if !self.active || generation != self.generation {
            return Err(MqttPublishRejection::Inactive);
        }
        validate_request_id(request_id)?;
        Ok(self
            .publications
            .get(&(source.clone(), request_id.into()))
            .map(|r| r.result.clone()))
    }

    fn commit_publication(
        &mut self,
        message: &MqttMessage,
        targets: &[Target],
        origin: Option<&str>,
    ) -> Result<()> {
        #[cfg(feature = "mqtt5")]
        crate::mqtt5::validate_properties(&message.properties)?;
        if message.properties.expired() {
            if message.retain {
                self.retained.remove(&message.topic);
            }
            return Ok(());
        }
        let cost = message_cost(message);
        let mut planned = Vec::new();
        let mut additions: BTreeMap<String, (usize, usize)> = BTreeMap::new();
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
            let mut delivery_message = message.clone();
            delivery_message.properties.subscription_identifiers = target.identifiers.clone();
            let cost = message_cost(&delivery_message);
            let extra = additions.entry(target.client.clone()).or_default();
            extra.0 += 1;
            extra.1 = extra.1.saturating_add(cost);
            if session.pending.len() + extra.0 > self.config.max_offline_messages_per_client
                || session
                    .pending
                    .iter()
                    .map(Delivery::bytes)
                    .sum::<usize>()
                    .saturating_add(extra.1)
                    > self.config.max_pending_bytes_per_client
            {
                return Err(MqttError::Capacity("reliable session backlog"));
            }
            let delivery = Delivery {
                message: delivery_message,
                qos: target.qos,
                retain: target.retain,
                id: None,
                sent: false,
                quota_used: false,
                ever_sent: false,
                phase: DeliveryPhase::Publish,
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
            let origin = match origin {
                // Legacy Will records may not contain a ClientID either.
                Some("") => RetainedOrigin::LegacyUnknown,
                Some(client) => RetainedOrigin::Client(client.into()),
                None => RetainedOrigin::Internal,
            };
            Some(Retained {
                message: message.clone(),
                _charge: self.budget.reserve(cost + origin.byte_cost())?,
                origin,
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
            let mut delivery_message = message.clone();
            delivery_message.properties.subscription_identifiers = target.identifiers.clone();
            let Ok(charge) = self.budget.reserve(message_cost(&delivery_message)) else {
                self.dropped_qos0 += 1;
                continue;
            };
            session.qos0.push_back(Delivery {
                message: delivery_message,
                qos: Qos::AtMostOnce,
                retain: target.retain,
                id: None,
                sent: false,
                quota_used: false,
                ever_sent: false,
                phase: DeliveryPhase::Publish,
                _charge: charge,
            });
            session.notify.notify_one();
        }
        self.counters.accepted_publications += 1;
        self.record_active_topic(message, origin);
        Ok(())
    }

    pub fn set_disconnect_expiry(&mut self, client: &str, seconds: u32) -> Result<()> {
        let session = self.sessions.get_mut(client).ok_or(MqttError::Closed)?;
        if session.expiry_interval == Some(0) && seconds != 0 {
            return Err(MqttError::Protocol("cannot extend zero Session Expiry"));
        }
        session.expiry_interval = Some(seconds);
        session.clean = seconds == 0;
        Ok(())
    }
    pub fn subscribe_with_options(
        &mut self,
        generation: u64,
        client: &str,
        connection: u64,
        filters: Vec<(String, Option<Qos>)>,
        options: Vec<crate::wire::SubscribeOptions>,
    ) -> Result<Vec<u8>> {
        if options.is_empty() {
            return self.subscribe(generation, client, connection, filters);
        }
        self.check(generation, client, connection)?;
        if filters.len() != options.len() {
            return Err(MqttError::Protocol("subscription option count"));
        }
        let mut codes = vec![];
        for ((filter, qos), option) in filters.into_iter().zip(options) {
            let Some(qos) = qos else {
                codes.push(0x80);
                continue;
            };
            let actual = shared_filter_name(&filter).unwrap_or(&filter);
            if actual.starts_with("$modorch") {
                codes.push(0x80);
                continue;
            }
            let existed = self.sessions[client].subscriptions.contains_key(&filter);
            let replay = if filter.starts_with("$share/")
                || option.retain_handling == 2
                || (option.retain_handling == 1 && existed)
            {
                vec![]
            } else {
                self.retained
                    .values()
                    .filter(|r| {
                        !r.message.properties.expired()
                            && codec::matches(actual, &r.message.topic)
                            && r.origin.permits(client, option.no_local)
                    })
                    .map(|r| r.message.clone())
                    .collect()
            };
            if self
                .subscribe_one(client, filter.clone(), qos, replay, option.clone())
                .is_err()
            {
                codes.push(0x80);
                continue;
            }
            self.sessions
                .get_mut(client)
                .unwrap()
                .subscriptions
                .get_mut(&filter)
                .unwrap()
                .options = option;
            codes.push(qos as u8);
        }
        let revision = self.next_id()?;
        self.sessions.get_mut(client).unwrap().revision = revision;
        Ok(codes)
    }
    pub fn subscribe(
        &mut self,
        generation: u64,
        client: &str,
        connection: u64,
        filters: Vec<(String, Option<Qos>)>,
    ) -> Result<Vec<u8>> {
        self.check(generation, client, connection)?;
        let revision = self.next_id()?;
        self.sessions.get_mut(client).unwrap().revision = revision;
        let mut codes = Vec::with_capacity(filters.len());
        for (filter, qos) in filters {
            let Some(qos) = qos.filter(|_| !filter.starts_with("$modorch")) else {
                codes.push(0x80);
                continue;
            };
            let replay_bytes = self
                .retained
                .values()
                .filter(|r| {
                    !r.message.properties.expired() && codec::matches(&filter, &r.message.topic)
                })
                .map(|r| 96 + r.message.topic.len())
                .sum();
            let Ok(_replay_memory) = self.budget.reserve(replay_bytes) else {
                codes.push(0x80);
                continue;
            };
            let replay: Vec<_> = self
                .retained
                .values()
                .filter(|r| {
                    !r.message.properties.expired() && codec::matches(&filter, &r.message.topic)
                })
                .map(|r| r.message.clone())
                .collect();
            match self.subscribe_one(client, filter, qos, replay, Default::default()) {
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
        options: crate::wire::SubscribeOptions,
    ) -> Result<()> {
        let session = self.sessions.get_mut(client).unwrap();
        if !session.subscriptions.contains_key(&filter)
            && session.subscriptions.len() >= self.config.max_subscriptions_per_client
        {
            return Err(MqttError::Capacity("subscriptions"));
        }
        let subscription = Subscription {
            qos,
            options: options.clone(),
            _charge: self.budget.reserve(128 + filter.len())?,
        };
        let mut reliable = Vec::new();
        let mut qos0 = Vec::new();
        let mut pending_bytes = session.pending.iter().map(Delivery::bytes).sum::<usize>();
        for mut message in replay {
            message.properties.subscription_identifiers = options.identifier.into_iter().collect();
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
                quota_used: false,
                ever_sent: false,
                phase: DeliveryPhase::Publish,
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
    ) -> Result<Vec<u8>> {
        self.check(generation, client, connection)?;
        let revision = self.next_id()?;
        let session = self.sessions.get_mut(client).unwrap();
        session.revision = revision;
        // Process entries sequentially, including repeated filters (3.10.4-6).
        // Deriving reasons before removal incorrectly reports repeated success.
        Ok(filters
            .into_iter()
            .map(|filter| {
                if session.subscriptions.remove(&filter).is_some() {
                    0
                } else {
                    0x11
                }
            })
            .collect())
    }

    pub fn reject_received(
        &mut self,
        generation: u64,
        client: &str,
        connection: u64,
        id: u16,
    ) -> Result<()> {
        self.check(generation, client, connection)?;
        let s = self.sessions.get_mut(client).unwrap();
        if let Some(index) = s.pending.iter().position(|d| {
            d.id == Some(id)
                && d.ever_sent
                && d.qos == Qos::ExactlyOnce
                && d.phase == DeliveryPhase::Publish
        }) {
            s.pending.remove(index);
            s.notify.notify_one();
        }
        Ok(())
    }
    pub fn ack(&mut self, generation: u64, client: &str, connection: u64, id: u16) -> Result<()> {
        self.check(generation, client, connection)?;
        let session = self.sessions.get_mut(client).unwrap();
        if let Some(index) = session
            .pending
            .iter()
            .position(|d| d.id == Some(id) && d.ever_sent && d.qos == Qos::AtLeastOnce)
        {
            session.pending.remove(index);
        }
        session.notify.notify_one();
        self.process_wills();
        Ok(())
    }

    pub fn has_outbound(&self, generation: u64, client: &str, connection: u64) -> Result<bool> {
        self.check(generation, client, connection)?;
        let session = &self.sessions[client];
        Ok(session.pending.iter().any(|d| !d.sent) || !session.qos0.is_empty())
    }

    pub fn next_outbound(
        &mut self,
        generation: u64,
        client: &str,
        connection: u64,
    ) -> Result<Option<Vec<u8>>> {
        self.check(generation, client, connection)?;
        let session = self.sessions.get_mut(client).unwrap();
        // Release packets complete existing ownership transfers even when the
        // PUBLISH window is full; reconnect always resumes the saved phase.
        if let Some(d) = session
            .pending
            .iter_mut()
            .find(|d| !d.sent && matches!(d.phase, DeliveryPhase::Release { .. }))
        {
            d.phase = DeliveryPhase::Release { ever_sent: true };
            d.sent = true;
            return Ok(Some(codec::frame(0x62, &d.id.unwrap().to_be_bytes())));
        }
        session
            .pending
            .retain(|d| d.ever_sent || !d.message.properties.expired());
        session.qos0.retain(|d| !d.message.properties.expired());
        let inflight = session
            .pending
            .iter()
            .filter(|d| d.sent && (!session.protocol5 || d.quota_used))
            .count();
        if inflight
            < self
                .config
                .max_inflight_per_client
                .min(session.receive_maximum)
        {
            // Only MQTT5 can skip an oversized publication and continue the loop.
            #[cfg_attr(not(feature = "mqtt5"), allow(clippy::never_loop))]
            while let Some(index) = session
                .pending
                .iter()
                .position(|d| !d.sent && d.phase == DeliveryPhase::Publish)
            {
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
                #[cfg(feature = "mqtt5")]
                if session.protocol5
                    && crate::mqtt5::publish_size(&delivery.message, delivery.qos, delivery.id)
                        > session
                            .maximum_packet_size
                            .min(self.config.max_packet_bytes)
                {
                    session.pending.remove(index);
                    self.counters.dropped_oversized += 1;
                    continue;
                }
                let bytes = encode_publish(
                    session.protocol5,
                    &delivery.message,
                    delivery.qos,
                    delivery.retain,
                    delivery.id,
                    delivery.ever_sent,
                );
                delivery.sent = true;
                delivery.quota_used = true;
                delivery.ever_sent = true;
                self.counters.sent_publications += 1;
                return Ok(Some(bytes));
            }
        }
        // MQTT3.1.1 returns its first queued publication; MQTT5 may skip entries.
        #[cfg_attr(not(feature = "mqtt5"), allow(clippy::never_loop))]
        while let Some(d) = session.qos0.pop_front() {
            #[cfg(feature = "mqtt5")]
            if session.protocol5
                && crate::mqtt5::publish_size(&d.message, d.qos, None)
                    > session
                        .maximum_packet_size
                        .min(self.config.max_packet_bytes)
            {
                self.counters.dropped_oversized += 1;
                continue;
            }
            let frame = encode_publish(session.protocol5, &d.message, d.qos, d.retain, None, false);
            self.counters.sent_publications += 1;
            return Ok(Some(frame));
        }
        Ok(None)
    }

    pub fn admit_peer(&mut self, ip: std::net::IpAddr) -> Result<()> {
        let now = std::time::Instant::now();
        self.prune_peers();
        if !self.peers.contains_key(&ip) {
            if self.peers.len() >= self.config.max_tracked_peer_ips {
                self.counters.rate_limited_connections += 1;
                return Err(MqttError::Capacity("peer IP rate buckets"));
            }
            let charge = self.budget.reserve(192)?;
            self.peers.insert(
                ip,
                Peer {
                    active: 0,
                    attempts: 0,
                    window: now,
                    _charge: charge,
                },
            );
        }
        let peer = self.peers.get_mut(&ip).unwrap();
        if now.duration_since(peer.window) >= self.config.connection_attempt_window {
            peer.window = now;
            peer.attempts = 0;
        }
        if peer.active >= self.config.max_connections_per_ip
            || peer.attempts >= self.config.max_connection_attempts_per_ip
        {
            self.counters.rate_limited_connections += 1;
            return Err(MqttError::Capacity("peer IP concurrency/attempt limit"));
        }
        peer.active += 1;
        peer.attempts += 1;
        Ok(())
    }

    pub fn release_peer(&mut self, ip: std::net::IpAddr) {
        if let Some(peer) = self.peers.get_mut(&ip) {
            peer.active -= 1;
        }
    }

    pub fn prune_peers(&mut self) {
        let window = self.config.connection_attempt_window;
        self.peers
            .retain(|_, p| p.active > 0 || p.window.elapsed() < window);
    }

    pub fn record_io(&mut self, read: usize, written: usize) {
        self.counters.bytes_received = self.counters.bytes_received.saturating_add(read as u64);
        self.counters.bytes_sent = self.counters.bytes_sent.saturating_add(written as u64);
    }

    fn record_active_topic(&mut self, message: &MqttMessage, origin: Option<&str>) {
        self.next_topic_seq = self.next_topic_seq.saturating_add(1);
        let seq = self.next_topic_seq;
        let now_ms = unix_time_ms();
        let publisher = origin.filter(|o| !o.is_empty()).map(str::to_owned);
        if let Some(entry) = self.active_topics.get_mut(&message.topic) {
            entry.publish_count = entry.publish_count.saturating_add(1);
            entry.last_qos = message.qos;
            entry.last_publisher = publisher;
            entry.last_published_ms = now_ms;
            entry.seq = seq;
            return;
        }
        if self.active_topics.len() >= MAX_ACTIVE_TOPICS {
            let oldest_key = self
                .active_topics
                .iter()
                .min_by_key(|(_, rec)| rec.seq)
                .map(|(k, _)| k.clone());
            if let Some(oldest_key) = oldest_key {
                self.active_topics.remove(&oldest_key);
            }
        }
        self.active_topics.insert(
            message.topic.clone(),
            ActiveTopicRecord {
                publish_count: 1,
                last_qos: message.qos,
                last_publisher: publisher,
                last_published_ms: now_ms,
                seq,
            },
        );
    }

    fn retained_info(&self, topic: &str, r: &Retained, preview_limit: usize) -> RetainedInfo {
        let origin = match &r.origin {
            RetainedOrigin::Client(c) => format!("client:{c}"),
            RetainedOrigin::Internal => "internal".into(),
            RetainedOrigin::LegacyUnknown => "legacy".into(),
        };
        let payload = r.message.payload.as_ref();
        let (payload_preview, payload_truncated) = if preview_limit == 0 {
            (None, false)
        } else {
            let truncated = payload.len() > preview_limit;
            let slice = if truncated {
                &payload[..preview_limit]
            } else {
                payload
            };
            let text = match std::str::from_utf8(slice) {
                Ok(s) => s.to_owned(),
                Err(_) => {
                    let hex: String = slice.iter().take(64).map(|b| format!("{b:02x}")).collect();
                    format!("<binary {} bytes: {}>", payload.len(), hex)
                }
            };
            (Some(text), truncated)
        };
        RetainedInfo {
            topic: topic.to_owned(),
            qos: r.message.qos,
            payload_bytes: payload.len(),
            origin,
            payload_preview,
            payload_truncated,
        }
    }

    fn push_event(
        &mut self,
        kind: &str,
        client_id: Option<String>,
        peer_addr: Option<String>,
        connection_id: Option<u64>,
        detail: String,
    ) {
        self.next_event_seq = self.next_event_seq.saturating_add(1);
        if self.events.len() >= MAX_EVENT_HISTORY {
            self.events.pop_front();
        }
        self.events.push_back(ConnectionEvent {
            seq: self.next_event_seq,
            timestamp_ms: unix_time_ms(),
            kind: kind.to_owned(),
            client_id,
            peer_addr,
            connection_id,
            detail,
        });
    }

    pub fn record_connection_failure(
        &mut self,
        peer_addr: Option<std::net::SocketAddr>,
        client_id: Option<String>,
        error: &MqttError,
    ) {
        let kind = match error {
            MqttError::NotAuthorized => {
                self.counters.auth_rejections += 1;
                "rejected"
            }
            MqttError::Protocol(
                "handshake/keepalive timeout" | "slow writer timeout" | "TLS handshake deadline",
            ) => {
                self.counters.timeouts += 1;
                if matches!(error, MqttError::Protocol("slow writer timeout")) {
                    self.counters.slow_writers += 1;
                }
                "timeout"
            }
            MqttError::Closed => return,
            _ => "error",
        };
        self.push_event(
            kind,
            client_id,
            peer_addr.map(|a| a.to_string()),
            None,
            error.to_string(),
        );
    }

    fn session_info(&self, client: &str, session: &Session) -> SessionInfo {
        SessionInfo {
            client_id: client.into(),
            owner: session.owner.clone(),
            clean_session: session.clean,
            online: session.online.is_some(),
            session_epoch: session.epoch,
            connection_id: session.online.as_ref().map(|c| c.id),
            peer_addr: session.online.as_ref().and_then(|c| c.peer_addr.clone()),
            protocol_level: if session.protocol5 { 5 } else { 4 },
            keep_alive: session.online.as_ref().map_or(0, |c| c.keep_alive),
            connected_at_ms: session.online.as_ref().map(|c| c.connected_at_ms),
            subscriptions: session.subscriptions.len(),
            pending_messages: session.pending.len(),
            pending_bytes: session.pending.iter().map(Delivery::bytes).sum(),
            inflight: session.pending.iter().filter(|d| d.sent).count(),
        }
    }

    pub fn admin(&mut self, generation: u64, request: &AdminRequest) -> AdminResult {
        if !self.active || self.generation != generation {
            return Err(AdminError::Unavailable);
        }
        let valid_client = |id: &str| {
            !id.is_empty() && id.len() <= self.config.max_client_id_bytes && !id.contains('\0')
        };
        match request {
            AdminRequest::BrokerStats => Ok(AdminReply::Stats(self.status())),
            AdminRequest::GetSession { client_id } => {
                if !valid_client(client_id) {
                    return Err(AdminError::InvalidRequest);
                }
                Ok(AdminReply::Session(
                    self.sessions
                        .get(client_id)
                        .map(|s| self.session_info(client_id, s)),
                ))
            }
            AdminRequest::DisconnectClient {
                client_id,
                expected_connection_id,
            } => {
                if !valid_client(client_id) {
                    return Err(AdminError::InvalidRequest);
                }
                let connection = self
                    .sessions
                    .get(client_id)
                    .and_then(|s| s.online.as_ref())
                    .filter(|c| c.id == *expected_connection_id)
                    .map(|c| (c.id, c.peer_addr.clone(), c.cancel.clone()));
                if let Some((conn_id, peer_addr, cancel)) = &connection {
                    cancel.cancel();
                    self.push_event(
                        "kicked",
                        Some(client_id.clone()),
                        peer_addr.clone(),
                        Some(*conn_id),
                        "administrative disconnect".into(),
                    );
                }
                Ok(AdminReply::Disconnected(connection.is_some()))
            }
            AdminRequest::ListClients { after, limit } => {
                if !(1..=256).contains(limit) || after.as_ref().is_some_and(|a| !valid_client(a)) {
                    return Err(AdminError::InvalidRequest);
                }
                let mut iter = self
                    .sessions
                    .iter()
                    .filter(|(id, _)| after.as_ref().is_none_or(|a| *id > a));
                let items: Vec<_> = iter
                    .by_ref()
                    .take(*limit)
                    .map(|(id, s)| self.session_info(id, s))
                    .collect();
                let next = iter
                    .next()
                    .and_then(|_| items.last().map(|s| s.client_id.clone()));
                Ok(AdminReply::Clients { items, next })
            }
            AdminRequest::ListSubscriptions {
                client_id,
                after,
                limit,
            } => {
                if !valid_client(client_id)
                    || !(1..=256).contains(limit)
                    || after
                        .as_ref()
                        .is_some_and(|a| codec::validate_filter(a).is_err())
                {
                    return Err(AdminError::InvalidRequest);
                }
                let mut iter = self
                    .sessions
                    .get(client_id)
                    .into_iter()
                    .flat_map(|s| &s.subscriptions)
                    .filter(|(filter, _)| after.as_ref().is_none_or(|a| *filter > a));
                let items: Vec<_> = iter
                    .by_ref()
                    .take(*limit)
                    .map(|(filter, s)| SubscriptionInfo {
                        filter: filter.clone(),
                        qos: s.qos,
                    })
                    .collect();
                let next = iter
                    .next()
                    .and_then(|_| items.last().map(|s| s.filter.clone()));
                Ok(AdminReply::Subscriptions { items, next })
            }
            AdminRequest::ListEvents { limit } => {
                if !(1..=256).contains(limit) {
                    return Err(AdminError::InvalidRequest);
                }
                let items = self.events.iter().rev().take(*limit).cloned().collect();
                Ok(AdminReply::Events(items))
            }
            AdminRequest::ListTopicFilters { after, limit } => {
                if !(1..=256).contains(limit)
                    || after
                        .as_ref()
                        .is_some_and(|a| codec::validate_filter(a).is_err())
                {
                    return Err(AdminError::InvalidRequest);
                }
                let mut grouped: BTreeMap<String, Vec<TopicSubscriberInfo>> = BTreeMap::new();
                for (client_id, session) in &self.sessions {
                    let online = session.online.is_some();
                    for (filter, sub) in &session.subscriptions {
                        grouped
                            .entry(filter.clone())
                            .or_default()
                            .push(TopicSubscriberInfo {
                                client_id: client_id.clone(),
                                online,
                                qos: sub.qos,
                            });
                    }
                }
                let mut iter = grouped
                    .into_iter()
                    .filter(|(filter, _)| after.as_ref().is_none_or(|a| filter > a));
                let items: Vec<_> = iter
                    .by_ref()
                    .take(*limit)
                    .map(|(filter, subscribers)| {
                        let subscriber_count = subscribers.len();
                        let online_count = subscribers.iter().filter(|s| s.online).count();
                        let max_qos = subscribers
                            .iter()
                            .map(|s| s.qos)
                            .max()
                            .unwrap_or(Qos::AtMostOnce);
                        TopicFilterSummary {
                            filter,
                            subscriber_count,
                            online_count,
                            max_qos,
                            subscribers,
                        }
                    })
                    .collect();
                let next = iter
                    .next()
                    .and_then(|_| items.last().map(|s| s.filter.clone()));
                Ok(AdminReply::TopicFilters { items, next })
            }
            AdminRequest::MatchTopic { topic } => {
                if allowed_topic(topic).is_err() {
                    return Err(AdminError::InvalidRequest);
                }
                let mut matches = Vec::new();
                for (client_id, session) in &self.sessions {
                    let mut matched_filters = Vec::new();
                    let mut max_qos: Option<Qos> = None;
                    for (filter, sub) in &session.subscriptions {
                        let actual = shared_filter_name(filter).unwrap_or(filter);
                        if codec::matches(actual, topic) {
                            matched_filters.push(filter.clone());
                            max_qos = Some(max_qos.map_or(sub.qos, |q| q.max(sub.qos)));
                        }
                    }
                    if let Some(granted_max_qos) = max_qos {
                        matches.push(TopicMatchEntry {
                            client_id: client_id.clone(),
                            online: session.online.is_some(),
                            matched_filters,
                            granted_max_qos,
                        });
                    }
                }
                Ok(AdminReply::TopicMatch(TopicMatchResult {
                    topic: topic.clone(),
                    matches,
                }))
            }
            AdminRequest::ListRetained {
                prefix,
                after,
                limit,
            } => {
                if !(1..=256).contains(limit)
                    || after.as_ref().is_some_and(|a| allowed_topic(a).is_err())
                {
                    return Err(AdminError::InvalidRequest);
                }
                let mut iter = self
                    .retained
                    .iter()
                    .filter(|(_, r)| !r.message.properties.expired())
                    .filter(|(t, _)| prefix.as_deref().is_none_or(|p| t.starts_with(p)))
                    .filter(|(t, _)| after.as_ref().is_none_or(|a| *t > a));
                let items: Vec<_> = iter
                    .by_ref()
                    .take(*limit)
                    .map(|(t, r)| self.retained_info(t, r, 256))
                    .collect();
                let next = iter
                    .next()
                    .and_then(|_| items.last().map(|r| r.topic.clone()));
                Ok(AdminReply::RetainedList { items, next })
            }
            AdminRequest::GetRetained { topic } => {
                if allowed_topic(topic).is_err() {
                    return Err(AdminError::InvalidRequest);
                }
                let item = self
                    .retained
                    .get(topic)
                    .filter(|r| !r.message.properties.expired())
                    .map(|r| self.retained_info(topic, r, 4096));
                Ok(AdminReply::RetainedDetail(item))
            }
            AdminRequest::DeleteRetained { topic } => {
                if allowed_topic(topic).is_err() {
                    return Err(AdminError::InvalidRequest);
                }
                let removed = self.retained.remove(topic).is_some();
                if removed {
                    self.push_event(
                        "retained_deleted",
                        None,
                        None,
                        None,
                        format!("topic={topic}"),
                    );
                }
                Ok(AdminReply::RetainedDeleted(removed))
            }
            AdminRequest::ListActiveTopics { search, limit } => {
                if !(1..=256).contains(limit) {
                    return Err(AdminError::InvalidRequest);
                }
                let search_lower = search
                    .as_deref()
                    .map(str::trim)
                    .unwrap_or("")
                    .to_lowercase();
                let mut rows: Vec<_> = self
                    .active_topics
                    .iter()
                    .filter(|(t, _)| {
                        search_lower.is_empty() || t.to_lowercase().contains(&search_lower)
                    })
                    .collect();
                rows.sort_by(|a, b| b.1.seq.cmp(&a.1.seq));
                let items = rows
                    .into_iter()
                    .take(*limit)
                    .map(|(topic, rec)| {
                        let matching_subscriptions = self
                            .sessions
                            .values()
                            .flat_map(|s| s.subscriptions.keys())
                            .filter(|f| {
                                let actual = shared_filter_name(f).unwrap_or(f);
                                codec::matches(actual, topic)
                            })
                            .count();
                        ActiveTopicInfo {
                            topic: topic.clone(),
                            publish_count: rec.publish_count,
                            last_qos: rec.last_qos,
                            last_publisher: rec.last_publisher.clone(),
                            last_published_ms: rec.last_published_ms,
                            matching_subscriptions,
                        }
                    })
                    .collect();
                Ok(AdminReply::ActiveTopics(items))
            }
            AdminRequest::AdminPublish {
                topic,
                payload,
                qos,
                retain,
            } => {
                if allowed_topic(topic).is_err() || payload.len() > self.config.max_packet_bytes {
                    return Err(AdminError::InvalidRequest);
                }
                let message = MqttMessage {
                    topic: topic.clone(),
                    payload: bytes::Bytes::copy_from_slice(payload),
                    qos: *qos,
                    retain: *retain,
                    properties: Default::default(),
                };
                let matched_sessions = self.targets_from(&message, None).len();
                self.route_from(&message, None)
                    .map_err(|_| AdminError::Unavailable)?;
                self.push_event(
                    "admin_publish",
                    None,
                    None,
                    None,
                    format!(
                        "topic={topic} qos={} retain={retain} matched={matched_sessions}",
                        *qos as u8
                    ),
                );
                Ok(AdminReply::Published { matched_sessions })
            }
        }
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

fn validate_request_id(id: &str) -> std::result::Result<(), MqttPublishRejection> {
    if id.is_empty() || id.len() > 128 || id.contains('\0') {
        return Err(MqttPublishRejection::Invalid(
            "request_id must be 1..128 bytes without NUL".into(),
        ));
    }
    Ok(())
}

fn publication_rejection(error: MqttError) -> MqttPublishRejection {
    match error {
        MqttError::Capacity(reason) => MqttPublishRejection::Capacity(reason.into()),
        MqttError::Protocol(reason) => MqttPublishRejection::Invalid(reason.into()),
        _ => MqttPublishRejection::Inactive,
    }
}

fn shared_filter_name(filter: &str) -> Option<&str> {
    #[cfg(feature = "mqtt5")]
    {
        crate::mqtt5::shared_filter(filter).map(|(_, f)| f)
    }
    #[cfg(not(feature = "mqtt5"))]
    {
        let _ = filter;
        None
    }
}
fn encode_publish(
    v5: bool,
    message: &MqttMessage,
    qos: Qos,
    retain: bool,
    id: Option<u16>,
    dup: bool,
) -> Vec<u8> {
    #[cfg(feature = "mqtt5")]
    if v5 {
        return crate::mqtt5::publish(message, qos, retain, id, dup);
    }
    let _ = v5;
    codec::publish(message, qos, retain, id, dup)
}

#[cfg(all(test, feature = "mqtt5"))]
mod shared_regressions {
    use super::*;

    fn member(s: &mut State, client: &str) {
        let c = Connect {
            v5: Some(crate::wire::ConnectOptions {
                session_expiry: 60,
                receive_maximum: 16,
                maximum_packet_size: 8192,
                ..Default::default()
            }),
            client_id: client.into(),
            clean: true,
            keep_alive: 0,
            will: None,
            username: None,
            password: None,
        };
        let p = Principal {
            key: PrincipalKey {
                domain: "test".into(),
                subject: "member".into(),
            },
            policy_version: 0,
            expires_at_ms: u64::MAX,
        };
        let connection = s
            .attach(s.generation, &c, CancellationToken::new(), &p, None, None)
            .unwrap()
            .1;
        assert_eq!(
            s.subscribe_with_options(
                s.generation,
                client,
                connection,
                vec![("$share/g/jobs".into(), Some(Qos::ExactlyOnce))],
                vec![crate::wire::SubscribeOptions::default()]
            )
            .unwrap(),
            vec![2]
        );
        s.detach(client, connection, true);
    }

    #[test]
    fn shared_offline_selection_matrix_excludes_expired_sessions_and_qos0() {
        // 2 live/offline choices x 2 expiry states x 3 QoS values. No sleeps:
        // an expired member is still physically present until maintenance runs.
        for online in [false, true] {
            for expired in [false, true] {
                for qos in [Qos::AtMostOnce, Qos::AtLeastOnce, Qos::ExactlyOnce] {
                    let mut s = State::new(Arc::new(MqttBrokerConfig {
                        mqtt5: true,
                        ..MqttBrokerConfig::loopback_development()
                    }));
                    s.begin_generation().unwrap();
                    s.active = true;
                    member(&mut s, "offline");
                    s.sessions.get_mut("offline").unwrap().expires_at_ms =
                        if expired { Some(0) } else { None };
                    if online {
                        member(&mut s, "online");
                        let session = s.sessions.get_mut("online").unwrap();
                        session.online = Some(Connection {
                            id: 999,
                            peer_addr: None,
                            keep_alive: 0,
                            connected_at_ms: 0,
                            cancel: CancellationToken::new(),
                            taken_over: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                        });
                    }
                    let message = MqttMessage {
                        topic: "jobs".into(),
                        payload: bytes::Bytes::from_static(b"work"),
                        qos,
                        retain: false,
                        properties: Default::default(),
                    };
                    let targets = s.targets_from(&message, None);
                    if online {
                        assert_eq!(targets.len(), 1);
                        assert_eq!(targets[0].client, "online");
                    } else if expired || qos == Qos::AtMostOnce {
                        assert!(targets.is_empty());
                    } else {
                        assert_eq!(targets.len(), 1);
                        assert_eq!(targets[0].client, "offline");
                    }
                }
            }
        }
    }

    #[test]
    fn shared_offline_global_budget_rejection_does_not_accept_or_change_retained() {
        let mut s = State::new(Arc::new(MqttBrokerConfig {
            mqtt5: true,
            ..MqttBrokerConfig::loopback_development()
        }));
        s.begin_generation().unwrap();
        s.active = true;
        member(&mut s, "offline");
        let message = MqttMessage {
            topic: "jobs".into(),
            payload: bytes::Bytes::from_static(b"work"),
            qos: Qos::ExactlyOnce,
            retain: true,
            properties: Default::default(),
        };
        let routing_bytes = s.sessions.len()
            * (96 + s.config.max_client_id_bytes + s.config.max_subscriptions_per_client * 4);
        // Allow the temporary routing reservation, fail when reserving the
        // reliable delivery itself, before Retained or any queue is mutated.
        let remaining = routing_bytes + message_cost(&message) - 1;
        let guard = s
            .budget
            .reserve(s.config.memory_budget_bytes - s.budget.used() - remaining)
            .unwrap();
        assert!(s.route(&message).is_err());
        assert!(s.sessions["offline"].pending.is_empty());
        assert_eq!(s.rejected_publications, 1);
        assert!(s.retained.is_empty());
        assert_eq!(s.counters.accepted_publications, 0);
        drop(guard);
        s.route(&message).unwrap();
        assert_eq!(s.sessions["offline"].pending.len(), 1);
        assert_eq!(s.retained["jobs"].message.payload.as_ref(), b"work");
    }

    #[test]
    fn session_diagnostics_and_connection_events_track_peer_takeover_and_kick() {
        let mut s = State::new(Arc::new(MqttBrokerConfig::loopback_development()));
        s.begin_generation().unwrap();
        s.active = true;
        let connect = Connect {
            client_id: "dup-client".into(),
            v5: None,
            clean: false,
            keep_alive: 45,
            will: None,
            username: None,
            password: None,
        };
        let principal = Principal {
            key: PrincipalKey {
                domain: "test".into(),
                subject: "anon".into(),
            },
            policy_version: 0,
            expires_at_ms: u64::MAX,
        };
        let addr1: std::net::SocketAddr = "192.168.11.10:50001".parse().unwrap();
        let addr2: std::net::SocketAddr = "192.168.11.11:50002".parse().unwrap();
        let (_, conn1, _, _) = s
            .attach_with_peer(
                s.generation,
                &connect,
                CancellationToken::new(),
                &principal,
                None,
                None,
                Some(addr1),
            )
            .unwrap();
        let AdminReply::Session(Some(info1)) = s
            .admin(
                s.generation,
                &AdminRequest::GetSession {
                    client_id: "dup-client".into(),
                },
            )
            .unwrap()
        else {
            panic!("expected session");
        };
        assert_eq!(info1.peer_addr.as_deref(), Some("192.168.11.10:50001"));
        assert_eq!(info1.protocol_level, 4);
        assert_eq!(info1.keep_alive, 45);
        assert!(info1.connected_at_ms.is_some());

        // Second connection with same ClientID takes over the first.
        let (_, conn2, _, _) = s
            .attach_with_peer(
                s.generation,
                &connect,
                CancellationToken::new(),
                &principal,
                Some(conn1),
                None,
                Some(addr2),
            )
            .unwrap();
        s.admin(
            s.generation,
            &AdminRequest::DisconnectClient {
                client_id: "dup-client".into(),
                expected_connection_id: conn2,
            },
        )
        .unwrap();
        s.detach("dup-client", conn2, false);

        let AdminReply::Events(events) = s
            .admin(s.generation, &AdminRequest::ListEvents { limit: 10 })
            .unwrap()
        else {
            panic!("expected events");
        };
        let kinds: Vec<&str> = events.iter().map(|e| e.kind.as_str()).collect();
        assert_eq!(
            kinds,
            vec![
                "disconnected",
                "kicked",
                "connected",
                "taken_over",
                "connected"
            ]
        );
        assert_eq!(events[3].peer_addr.as_deref(), Some("192.168.11.10:50001"));
        assert!(events[3].detail.contains("192.168.11.11:50002"));
    }

    #[test]
    fn topic_filters_match_retained_active_and_admin_publish_work() {
        let mut s = State::new(Arc::new(MqttBrokerConfig::loopback_development()));
        s.begin_generation().unwrap();
        s.active = true;
        let principal = Principal {
            key: PrincipalKey {
                domain: "test".into(),
                subject: "anon".into(),
            },
            policy_version: 0,
            expires_at_ms: u64::MAX,
        };
        let c1 = Connect {
            client_id: "sub-slash".into(),
            v5: None,
            clean: true,
            keep_alive: 30,
            will: None,
            username: None,
            password: None,
        };
        let c2 = Connect {
            client_id: "sub-all".into(),
            v5: None,
            clean: true,
            keep_alive: 30,
            will: None,
            username: None,
            password: None,
        };
        let conn1 = s
            .attach(
                s.generation,
                &c1,
                CancellationToken::new(),
                &principal,
                None,
                None,
            )
            .unwrap()
            .1;
        let conn2 = s
            .attach(
                s.generation,
                &c2,
                CancellationToken::new(),
                &principal,
                None,
                None,
            )
            .unwrap()
            .1;
        s.subscribe(
            s.generation,
            "sub-slash",
            conn1,
            vec![("/#".into(), Some(Qos::AtMostOnce))],
        )
        .unwrap();
        s.subscribe(
            s.generation,
            "sub-all",
            conn2,
            vec![("sensors/+".into(), Some(Qos::ExactlyOnce))],
        )
        .unwrap();

        let AdminReply::TopicFilters { items, .. } = s
            .admin(
                s.generation,
                &AdminRequest::ListTopicFilters {
                    after: None,
                    limit: 50,
                },
            )
            .unwrap()
        else {
            panic!("expected topic filters");
        };
        assert_eq!(items.len(), 2);

        let AdminReply::TopicMatch(matched) = s
            .admin(
                s.generation,
                &AdminRequest::MatchTopic {
                    topic: "sensors/temp".into(),
                },
            )
            .unwrap()
        else {
            panic!("expected match");
        };
        assert_eq!(matched.matches.len(), 1);
        assert_eq!(matched.matches[0].client_id, "sub-all");
        assert_eq!(matched.matches[0].granted_max_qos, Qos::ExactlyOnce);

        let AdminReply::Published { matched_sessions } = s
            .admin(
                s.generation,
                &AdminRequest::AdminPublish {
                    topic: "sensors/temp".into(),
                    payload: b"23.5".to_vec(),
                    qos: Qos::ExactlyOnce,
                    retain: true,
                },
            )
            .unwrap()
        else {
            panic!("expected published");
        };
        assert_eq!(matched_sessions, 1);

        let AdminReply::RetainedList { items, .. } = s
            .admin(
                s.generation,
                &AdminRequest::ListRetained {
                    prefix: Some("sensors/".into()),
                    after: None,
                    limit: 50,
                },
            )
            .unwrap()
        else {
            panic!("expected retained list");
        };
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].payload_preview.as_deref(), Some("23.5"));

        let AdminReply::ActiveTopics(active) = s
            .admin(
                s.generation,
                &AdminRequest::ListActiveTopics {
                    search: None,
                    limit: 50,
                },
            )
            .unwrap()
        else {
            panic!("expected active topics");
        };
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].topic, "sensors/temp");
        assert_eq!(active[0].publish_count, 1);
        assert_eq!(active[0].matching_subscriptions, 1);

        assert!(matches!(
            s.admin(
                s.generation,
                &AdminRequest::DeleteRetained {
                    topic: "sensors/temp".into(),
                },
            )
            .unwrap(),
            AdminReply::RetainedDeleted(true)
        ));
        let AdminReply::RetainedDetail(none) = s
            .admin(
                s.generation,
                &AdminRequest::GetRetained {
                    topic: "sensors/temp".into(),
                },
            )
            .unwrap()
        else {
            panic!("expected detail");
        };
        assert!(none.is_none());
    }
}
