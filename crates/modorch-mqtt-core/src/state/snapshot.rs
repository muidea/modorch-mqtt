use super::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub(crate) struct Snapshot {
    format: u32,
    #[serde(default)]
    captured_at_ms: u64,
    broker: String,
    next_id: u64,
    quota: Quota,
    sessions: Vec<SessionImage>,
    retained: Vec<MqttMessage>,
    #[serde(default)]
    retained_origins: BTreeMap<String, RetainedOrigin>,
    wills: Vec<WillImage>,
    publications: Vec<PublicationImage>,
}
#[derive(Serialize, Deserialize, PartialEq, Eq)]
struct Quota {
    sessions: usize,
    subscriptions: usize,
    reliable_messages: usize,
    reliable_bytes: usize,
    retained: usize,
    wills: usize,
    publications: usize,
    #[serde(default)]
    incoming_qos2: usize,
    #[serde(default)]
    incoming_qos2_bytes: usize,
}
impl Snapshot {
    fn counts(&self) -> Quota {
        Quota {
            sessions: self.sessions.len(),
            subscriptions: self.sessions.iter().map(|s| s.subscriptions.len()).sum(),
            reliable_messages: self.sessions.iter().map(|s| s.pending.len()).sum(),
            reliable_bytes: self
                .sessions
                .iter()
                .flat_map(|s| &s.pending)
                .map(|d| message_cost(&d.message))
                .sum(),
            retained: self.retained.len(),
            wills: self.wills.len(),
            publications: self.publications.len(),
            incoming_qos2: self.sessions.iter().map(|s| s.incoming_qos2.len()).sum(),
            incoming_qos2_bytes: self
                .sessions
                .iter()
                .flat_map(|s| &s.incoming_qos2)
                .map(|(_, m)| message_cost(m) + 64)
                .sum(),
        }
    }
}
#[derive(Serialize, Deserialize)]
struct SessionImage {
    #[serde(default)]
    protocol5: bool,
    #[serde(default)]
    expiry_interval: Option<u32>,
    #[serde(default)]
    expires_at_ms: Option<u64>,
    #[serde(default)]
    subscription_options: BTreeMap<String, crate::wire::SubscribeOptions>,
    client: String,
    epoch: u64,
    revision: u64,
    owner: PrincipalKey,
    clean: bool,
    subscriptions: Vec<(String, Qos)>,
    pending: Vec<DeliveryImage>,
    #[serde(default)]
    incoming_qos2: Vec<(u16, MqttMessage)>,
    next_packet_id: u16,
}
#[derive(Serialize, Deserialize)]
struct DeliveryImage {
    message: MqttMessage,
    qos: Qos,
    retain: bool,
    id: Option<u16>,
    ever_sent: bool,
    #[serde(default)]
    phase: DeliveryPhase,
}
#[derive(Serialize, Deserialize)]
struct TargetImage {
    #[serde(default)]
    retain: bool,
    #[serde(default)]
    identifiers: Vec<u32>,
    client: String,
    epoch: u64,
    qos: Qos,
}
#[derive(Serialize, Deserialize)]
struct WillImage {
    #[serde(default)]
    client: String,
    #[serde(default)]
    delay: u32,
    #[serde(default)]
    due: Option<u64>,
    #[serde(default)]
    expiry: Option<u32>,
    id: u64,
    message: MqttMessage,
    authorization: Option<PublishGrant>,
    targets: Option<Vec<TargetImage>>,
}
#[derive(Serialize, Deserialize)]
struct PublicationImage {
    source_kind: u8,
    source: String,
    request: MqttPublishRequest,
    result: MqttPublishResult,
}
fn corrupt() -> MqttError {
    MqttError::Protocol("invalid persistent snapshot")
}
fn valid_owner(p: &PrincipalKey) -> bool {
    [&p.domain, &p.subject]
        .iter()
        .all(|s| !s.is_empty() && s.len() <= 256 && !s.contains('\0'))
}
fn valid_message(m: &MqttMessage, config: &MqttBrokerConfig) -> Result<()> {
    valid_stored_message(m, config, false)
}
fn valid_network_message(m: &MqttMessage, config: &MqttBrokerConfig) -> Result<()> {
    valid_stored_message(m, config, config.mqtt5)
}
fn valid_stored_message(
    m: &MqttMessage,
    config: &MqttBrokerConfig,
    allow_alias: bool,
) -> Result<()> {
    allowed_topic(&m.topic)?;
    #[cfg(feature = "mqtt5")]
    crate::mqtt5::validate_properties(&m.properties)?;
    if !config.mqtt5 && m.properties != MqttProperties::default() {
        return Err(corrupt());
    }
    if m.properties.utf8_payload && std::str::from_utf8(&m.payload).is_err() {
        return Err(corrupt());
    }
    // Empty metadata may originate from 3.1.1. A network MQTT5 source may
    // instead have used an established Topic Alias: the wire frame contains
    // an empty Topic plus the alias property, while storage owns the full Topic.
    // Include real properties and both variable-length integers in that size.
    let fits = m.properties == MqttProperties::default()
        && codec::publish_size(m, (m.qos != Qos::AtMostOnce).then_some(1))
            <= config.max_packet_bytes;
    #[cfg(feature = "mqtt5")]
    let fits = fits
        || (config.mqtt5
            && crate::mqtt5::minimum_source_size(m, allow_alias) <= config.max_packet_bytes);
    #[cfg(not(feature = "mqtt5"))]
    let _ = allow_alias;
    if !fits {
        return Err(corrupt());
    }
    Ok(())
}
fn valid_delivery_message(m: &MqttMessage, config: &MqttBrokerConfig) -> Result<()> {
    #[cfg(feature = "mqtt5")]
    crate::mqtt5::validate_properties(&m.properties)?;
    if !config.mqtt5 && m.properties != MqttProperties::default() {
        return Err(corrupt());
    }
    // Subscription Identifiers are added after ingress admission. They belong
    // to the target's outbound limit, not the publisher's packet-size bound.
    let mut source = m.clone();
    source.properties.subscription_identifiers.clear();
    valid_network_message(&source, config)
}
impl State {
    pub(crate) fn snapshot(&self) -> Snapshot {
        Snapshot {
            format: 5,
            captured_at_ms: if self.sessions.values().any(|s| s.protocol5) {
                unix_time_ms() / 1000 * 1000
            } else {
                0
            },
            broker: self.config.name.clone(),
            next_id: self.next_id,
            quota: Quota {
                sessions: self.sessions.len(),
                subscriptions: self.sessions.values().map(|s| s.subscriptions.len()).sum(),
                reliable_messages: self.sessions.values().map(|s| s.pending.len()).sum(),
                reliable_bytes: self
                    .sessions
                    .values()
                    .flat_map(|s| &s.pending)
                    .map(Delivery::bytes)
                    .sum(),
                retained: self.retained.len(),
                wills: self.wills.len(),
                publications: self.publications.len(),
                incoming_qos2: self.sessions.values().map(|s| s.incoming_qos2.len()).sum(),
                incoming_qos2_bytes: self
                    .sessions
                    .values()
                    .flat_map(|s| s.incoming_qos2.values())
                    .map(|p| message_cost(&p.message) + 64)
                    .sum(),
            },
            sessions: self
                .sessions
                .iter()
                .map(|(client, s)| SessionImage {
                    client: client.clone(),
                    epoch: s.epoch,
                    revision: s.revision,
                    owner: s.owner.clone(),
                    clean: s.clean,
                    protocol5: s.protocol5,
                    expiry_interval: s.expiry_interval,
                    expires_at_ms: s.expires_at_ms,
                    subscription_options: s
                        .subscriptions
                        .iter()
                        .map(|(f, s)| (f.clone(), s.options.clone()))
                        .collect(),
                    subscriptions: s
                        .subscriptions
                        .iter()
                        .map(|(f, s)| (f.clone(), s.qos))
                        .collect(),
                    pending: s
                        .pending
                        .iter()
                        .map(|d| DeliveryImage {
                            message: d.message.clone(),
                            qos: d.qos,
                            retain: d.retain,
                            id: d.id,
                            ever_sent: d.ever_sent,
                            phase: d.phase,
                        })
                        .collect(),
                    incoming_qos2: s
                        .incoming_qos2
                        .iter()
                        .map(|(id, p)| (*id, p.message.clone()))
                        .collect(),
                    next_packet_id: s.next_packet_id,
                })
                .collect(),
            retained: self.retained.values().map(|r| r.message.clone()).collect(),
            retained_origins: self
                .retained
                .iter()
                .map(|(topic, r)| (topic.clone(), r.origin.clone()))
                .collect(),
            wills: self
                .wills
                .iter()
                .map(|(id, w)| WillImage {
                    id: *id,
                    client: w.client.clone(),
                    delay: w.delay,
                    due: w.due,
                    expiry: w.expiry,
                    message: w.message.clone(),
                    authorization: w._authorization.clone(),
                    targets: w.targets.as_ref().map(|t| {
                        t.iter()
                            .map(|t| TargetImage {
                                client: t.client.clone(),
                                epoch: t.epoch,
                                qos: t.qos,
                                retain: t.retain,
                                identifiers: t.identifiers.clone(),
                            })
                            .collect()
                    }),
                })
                .collect(),
            publications: self
                .publications
                .iter()
                .map(|((source, _), p)| PublicationImage {
                    source_kind: if source.kind == modorch::ComponentKind::Module {
                        1
                    } else {
                        0
                    },
                    source: source.id.clone(),
                    request: p.request.clone(),
                    result: p.result.clone(),
                })
                .collect(),
        }
    }
    pub(crate) fn restore(&mut self, mut snapshot: Snapshot) -> Result<()> {
        if snapshot.quota != snapshot.counts()
            || !matches!(snapshot.format, 1..=5)
            || snapshot.broker != self.config.name
            || snapshot.sessions.len() > self.config.max_sessions
            || snapshot.retained.len() > self.config.max_retained_topics
            || snapshot.wills.len() > self.config.max_will_records
            || snapshot.publications.len() > self.config.max_publish_records
            || (snapshot.format < 5 && !snapshot.retained_origins.is_empty())
            || (snapshot.format == 5
                && (snapshot.retained_origins.len() != snapshot.retained.len()
                    || snapshot
                        .retained
                        .iter()
                        .any(|m| !snapshot.retained_origins.contains_key(&m.topic))))
        {
            return Err(corrupt());
        }
        let mut candidate = State::new(self.config.clone());
        candidate.budget = self.budget.clone();
        candidate.next_id = snapshot.next_id;
        let mut epochs = std::collections::BTreeSet::new();
        for s in snapshot.sessions {
            // Format2 did not record PUBREC receipt order. With more than one
            // outstanding Release it cannot be reconstructed safely. Do not
            // silently invent an order or replace the committed database.
            if snapshot.format == 2
                && !s.clean
                && s.pending
                    .iter()
                    .filter(|d| matches!(d.phase, DeliveryPhase::Release { .. }))
                    .count()
                    > 1
            {
                return Err(MqttError::Unsupported("legacy QoS2 release order"));
            }
            if s.client.is_empty()
                || s.client.len() > self.config.max_client_id_bytes
                || s.client.contains('\0')
                || !valid_owner(&s.owner)
                || s.epoch == 0
                || s.epoch > snapshot.next_id
                || s.revision == 0
                || s.revision > snapshot.next_id
                || !epochs.insert(s.epoch)
                || s.next_packet_id == 0
                || s.subscriptions.len() > self.config.max_subscriptions_per_client
                || s.pending.len() > self.config.max_offline_messages_per_client
                || s.incoming_qos2.len() > self.config.max_inbound_qos2_per_client
                || (snapshot.format == 1
                    && (!s.incoming_qos2.is_empty()
                        || s.pending.iter().any(|d| d.phase != DeliveryPhase::Publish)))
                || candidate.sessions.contains_key(&s.client)
            {
                return Err(corrupt());
            }
            if s.protocol5 && !self.config.mqtt5 {
                return Err(corrupt());
            }
            if s.subscription_options
                .keys()
                .any(|k| !s.subscriptions.iter().any(|(f, _)| k == f))
            {
                return Err(corrupt());
            }
            let mut subscriptions = BTreeMap::new();
            for (f, qos) in s.subscriptions {
                codec::validate_filter(&f)?;
                if f.starts_with("$modorch") || subscriptions.contains_key(&f) {
                    return Err(corrupt());
                }
                if let Some(o) = s.subscription_options.get(&f) {
                    if o.retain_handling > 2
                        || o.identifier.is_some_and(|id| id == 0 || id > 268435455)
                        || (!s.protocol5
                            && (o.no_local
                                || o.retain_as_published
                                || o.retain_handling != 0
                                || o.identifier.is_some()))
                    {
                        return Err(corrupt());
                    }
                    #[cfg(feature = "mqtt5")]
                    if s.protocol5
                        && f.starts_with("$share/")
                        && (crate::mqtt5::shared_filter(&f).is_none() || o.no_local)
                    {
                        return Err(corrupt());
                    }
                }
                let charge = candidate.budget.reserve(128 + f.len())?;
                subscriptions.insert(
                    f.clone(),
                    Subscription {
                        qos,
                        options: s.subscription_options.get(&f).cloned().unwrap_or_default(),
                        _charge: charge,
                    },
                );
            }
            let mut pending = VecDeque::new();
            let mut ids = std::collections::BTreeSet::new();
            let mut pending_bytes = 0usize;
            for d in s.pending {
                valid_delivery_message(&d.message, &self.config)?;
                if d.qos == Qos::AtMostOnce
                    || d.qos > d.message.qos
                    || d.id == Some(0)
                    || (d.ever_sent && d.id.is_none())
                    || d.id.is_some_and(|id| !ids.insert(id))
                    || (d.phase != DeliveryPhase::Publish
                        && (d.qos != Qos::ExactlyOnce || !d.ever_sent || d.id.is_none()))
                {
                    return Err(corrupt());
                }
                pending_bytes = pending_bytes
                    .checked_add(message_cost(&d.message))
                    .ok_or_else(corrupt)?;
                if pending_bytes > self.config.max_pending_bytes_per_client {
                    return Err(MqttError::Capacity("recovered reliable backlog"));
                }
                let charge = candidate.budget.reserve(message_cost(&d.message))?;
                pending.push_back(Delivery {
                    message: d.message,
                    qos: d.qos,
                    retain: d.retain,
                    id: d.id,
                    sent: false,
                    quota_used: false,
                    ever_sent: d.ever_sent,
                    phase: d.phase,
                    _charge: charge,
                });
            }
            let mut incoming_qos2 = BTreeMap::new();
            let mut incoming_bytes = 0usize;
            for (id, message) in s.incoming_qos2 {
                valid_network_message(&message, &self.config)?;
                if id == 0 || message.qos != Qos::ExactlyOnce || incoming_qos2.contains_key(&id) {
                    return Err(corrupt());
                }
                let cost = message_cost(&message) + 64;
                incoming_bytes = incoming_bytes.checked_add(cost).ok_or_else(corrupt)?;
                if incoming_bytes > self.config.max_inbound_qos2_bytes_per_client {
                    return Err(MqttError::Capacity("recovered incoming QoS2 exchanges"));
                }
                let charge = candidate.budget.reserve(cost)?;
                incoming_qos2.insert(
                    id,
                    IncomingQos2 {
                        message,
                        _charge: charge,
                    },
                );
            }
            // Clean sessions lost their TCP connection. Their Will remains independently recoverable.
            if s.clean {
                continue;
            }
            let charge = candidate
                .budget
                .reserve(512 + s.client.len() + s.owner.domain.len() + s.owner.subject.len())?;
            candidate.sessions.insert(
                s.client,
                Session {
                    epoch: s.epoch,
                    revision: s.revision,
                    owner: s.owner,
                    clean: false,
                    protocol5: s.protocol5,
                    expiry_interval: s.expiry_interval,
                    expires_at_ms: s.expires_at_ms.or_else(|| {
                        s.expiry_interval
                            .filter(|n| *n != u32::MAX)
                            .map(|n| snapshot.captured_at_ms.saturating_add(u64::from(n) * 1000))
                    }),
                    receive_maximum: usize::MAX,
                    maximum_packet_size: usize::MAX,
                    online: None,
                    subscriptions,
                    pending,
                    incoming_qos2,
                    qos0: VecDeque::new(),
                    next_packet_id: s.next_packet_id,
                    notify: Arc::new(Notify::new()),
                    _charge: charge,
                },
            );
        }
        for message in snapshot.retained {
            let origin = snapshot
                .retained_origins
                .remove(&message.topic)
                .unwrap_or(RetainedOrigin::LegacyUnknown);
            if matches!(&origin, RetainedOrigin::Internal) {
                valid_message(&message, &self.config)?;
            } else {
                valid_network_message(&message, &self.config)?;
            }
            if !message.retain
                || message.payload.is_empty()
                || candidate.retained.contains_key(&message.topic)
            {
                return Err(corrupt());
            }
            if let RetainedOrigin::Client(client) = &origin {
                if client.is_empty()
                    || client.len() > self.config.max_client_id_bytes
                    || codec::validate_string(client).is_err()
                {
                    return Err(corrupt());
                }
            }
            let charge = candidate
                .budget
                .reserve(message_cost(&message) + origin.byte_cost())?;
            candidate.retained.insert(
                message.topic.clone(),
                Retained {
                    message,
                    origin,
                    _charge: charge,
                },
            );
        }
        for w in snapshot.wills {
            valid_message(&w.message, &self.config)?;
            if w.id == 0 || w.id > snapshot.next_id || candidate.wills.contains_key(&w.id) {
                return Err(corrupt());
            }
            if self.config.auth.is_some()
                && !w.authorization.as_ref().is_some_and(|g| {
                    let c = &g.context;
                    c.kind == PublishKind::Will
                        && c.internal_source.is_none()
                        && c.topic == w.message.topic
                        && c.qos == w.message.qos
                        && c.retain == w.message.retain
                        && c.principal.as_ref().is_some_and(|p| {
                            valid_owner(&p.key) && p.policy_version == g.policy_version
                        })
                })
            {
                return Err(corrupt());
            }
            let targets = if let Some(ts) = w.targets {
                if ts.len() > self.config.max_sessions {
                    return Err(corrupt());
                }
                let mut seen = std::collections::BTreeSet::new();
                let mut targets = Vec::new();
                for t in ts {
                    if t.client.is_empty()
                        || t.client.len() > self.config.max_client_id_bytes
                        || t.epoch == 0
                        || t.epoch > snapshot.next_id
                        || t.qos > w.message.qos
                        || !seen.insert(t.client.clone())
                    {
                        return Err(corrupt());
                    }
                    targets.push(Target {
                        client: t.client,
                        epoch: t.epoch,
                        qos: t.qos,
                        retain: t.retain,
                        identifiers: t.identifiers,
                    });
                }
                Some(targets)
            } else {
                None
            };
            let charge = candidate.budget.reserve(
                message_cost(&w.message)
                    + if self.config.auth.is_some() {
                        1024 + w.message.topic.len()
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
            )?;
            let due = w.due.or_else(|| {
                (w.delay > 0).then(|| {
                    snapshot.captured_at_ms.saturating_add(
                        u64::from(
                            w.delay.min(
                                candidate
                                    .sessions
                                    .get(&w.client)
                                    .and_then(|s| s.expiry_interval)
                                    .unwrap_or(0),
                            ),
                        ) * 1000,
                    )
                })
            });
            candidate.wills.insert(
                w.id,
                Will {
                    client: w.client,
                    delay: w.delay,
                    due,
                    expiry: w.expiry,
                    message: w.message,
                    _authorization: w.authorization,
                    targets,
                    _charge: charge,
                },
            );
        }
        for p in snapshot.publications {
            if p.source.is_empty()
                || p.source.len() > 128
                || p.source.contains('\0')
                || p.source_kind > 1
            {
                return Err(corrupt());
            }
            validate_request_id(&p.request.request_id).map_err(|_| corrupt())?;
            let message = MqttMessage {
                topic: p.request.topic.clone(),
                payload: p.request.payload.clone(),
                qos: p.request.qos,
                retain: p.request.retain,
                properties: p.request.properties.clone(),
            };
            valid_message(&message, &self.config)?;
            if !message.properties.subscription_identifiers.is_empty() {
                return Err(corrupt());
            }
            if p.result.as_ref().is_ok_and(|r| {
                r.request_id != p.request.request_id
                    || !r.accepted
                    || r.durability != AcceptanceDurability::DurableCommitted
            }) {
                return Err(corrupt());
            }
            if matches!(&p.result,Err(MqttPublishRejection::Invalid(reason)|MqttPublishRejection::Capacity(reason)) if reason.len()>256)
            {
                return Err(corrupt());
            }
            let source = if p.source_kind == 1 {
                ComponentRef::module(p.source)
            } else {
                ComponentRef::initiator(p.source)
            };
            let key = (source.clone(), p.request.request_id.clone());
            if candidate.publications.contains_key(&key) {
                return Err(corrupt());
            }
            let charge = candidate.budget.reserve(
                message_cost(&message) + 256 + p.request.request_id.len() * 2 + source.id.len(),
            )?;
            candidate.publications.insert(
                key,
                PublishRecord {
                    request: p.request,
                    result: p.result,
                    _charge: charge,
                },
            );
        }
        // All offline subscription maps exist before recovering armed Wills or opening ingress.
        let armed: Vec<_> = candidate
            .wills
            .iter()
            .filter_map(|(id, w)| w.targets.is_none().then_some(*id))
            .collect();
        for id in armed {
            candidate.trigger_will(id);
        }
        candidate.process_wills();
        self.next_id = candidate.next_id;
        self.sessions = candidate.sessions;
        self.retained = candidate.retained;
        self.wills = candidate.wills;
        self.publications = candidate.publications;
        self.storage_poisoned = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "mqtt5")]
    #[test]
    fn mqtt5_idempotency_restore_validates_real_properties_and_refuses_insufficient_budget() {
        let mut cfg = MqttBrokerConfig::loopback_development();
        cfg.mqtt5 = true;
        let mut source = State::new(Arc::new(cfg.clone()));
        let generation = source.begin_generation().unwrap();
        source.active = true;
        for n in 0..2 {
            source
                .publish_internal(
                    generation,
                    &modorch::ComponentRef::module("business"),
                    &MqttPublishRequest {
                        request_id: format!("record-{n}"),
                        topic: "ledger/value".into(),
                        payload: b"p".as_slice().into(),
                        qos: Qos::AtLeastOnce,
                        retain: false,
                        properties: MqttProperties {
                            content_type: Some("x".repeat(4096)),
                            ..Default::default()
                        },
                    },
                )
                .unwrap();
        }
        let snapshot = || {
            let mut s = source.snapshot();
            for p in &mut s.publications {
                p.result.as_mut().unwrap().durability = AcceptanceDurability::DurableCommitted;
            }
            s
        };
        let mut restored = State::new(Arc::new(cfg.clone()));
        restored.restore(snapshot()).unwrap();
        assert_eq!(
            restored.status().memory_used_bytes,
            source.status().memory_used_bytes
        );
        let mut small = cfg.clone();
        small.memory_budget_bytes = source.status().memory_used_bytes - 1;
        let mut limited = State::new(Arc::new(small));
        assert!(limited.restore(snapshot()).is_err());
        assert_eq!(limited.status().publish_records, 0);
        assert_eq!(limited.status().memory_used_bytes, 0);
        let mut small_packet = cfg;
        small_packet.max_packet_bytes = 1024;
        let mut limited = State::new(Arc::new(small_packet));
        assert!(limited.restore(snapshot()).is_err());
        for bad in [
            MqttProperties {
                content_type: Some("bad\0type".into()),
                ..Default::default()
            },
            MqttProperties {
                subscription_identifiers: vec![1],
                ..Default::default()
            },
            MqttProperties {
                utf8_payload: true,
                ..Default::default()
            },
        ] {
            let mut s = snapshot();
            s.publications[0].request.properties = bad;
            s.publications[0].request.payload = bytes::Bytes::from_static(b"\xff");
            assert!(restored.restore(s).is_err());
            assert_eq!(restored.status().publish_records, 2);
        }
    }

    #[test]
    fn legacy_snapshot_upgrades_and_invalid_qos2_phase_or_quotas_fail_without_replacing_state() {
        let config = Arc::new(MqttBrokerConfig::loopback_development());
        let mut s = State::new(config);
        let g = s.begin_generation().unwrap();
        s.active = true;
        let c = Connect {
            v5: None,
            client_id: "legacy".into(),
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
        let (_, connection, _, _) = s
            .attach(g, &c, CancellationToken::new(), &p, None, None)
            .unwrap();
        s.subscribe(
            g,
            "legacy",
            connection,
            vec![("legacy".into(), Some(Qos::AtLeastOnce))],
        )
        .unwrap();
        s.publish(
            g,
            "legacy",
            connection,
            MqttMessage {
                topic: "legacy".into(),
                payload: bytes::Bytes::from_static(b"saved"),
                qos: Qos::AtLeastOnce,
                retain: false,
                properties: Default::default(),
            },
        )
        .unwrap();
        s.next_outbound(g, "legacy", connection).unwrap();
        let mut legacy = serde_json::to_value(s.snapshot()).unwrap();
        legacy["format"] = 1.into();
        legacy["quota"]
            .as_object_mut()
            .unwrap()
            .remove("incoming_qos2");
        legacy["quota"]
            .as_object_mut()
            .unwrap()
            .remove("incoming_qos2_bytes");
        for session in legacy["sessions"].as_array_mut().unwrap() {
            session.as_object_mut().unwrap().remove("incoming_qos2");
            for d in session["pending"].as_array_mut().unwrap() {
                d.as_object_mut().unwrap().remove("phase");
            }
        }
        s.restore(serde_json::from_value(legacy).unwrap()).unwrap();
        assert_eq!(s.snapshot().format, 5);
        assert_eq!(s.status().pending_messages, 1);
        let mut bad = s.snapshot();
        bad.sessions[0].pending[0].phase = DeliveryPhase::Release { ever_sent: true };
        assert!(s.restore(bad).is_err());
        assert_eq!(s.status().pending_messages, 1);
        let mut bad = s.snapshot();
        bad.quota.incoming_qos2 = 1;
        assert!(s.restore(bad).is_err());
        assert_eq!(s.status().pending_messages, 1);
    }
}

#[cfg(all(test, feature = "mqtt5"))]
mod regressions {
    use super::*;

    fn message() -> MqttMessage {
        MqttMessage {
            topic: "own".into(),
            payload: b"value".as_slice().into(),
            qos: Qos::AtLeastOnce,
            retain: true,
            properties: Default::default(),
        }
    }
    fn attach(s: &mut State, id: &str) -> u64 {
        s.active = true;
        let c = Connect {
            v5: Some(crate::wire::ConnectOptions {
                session_expiry: 60,
                receive_maximum: 10,
                maximum_packet_size: 8192,
                ..Default::default()
            }),
            client_id: id.into(),
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
        s.attach(s.generation, &c, CancellationToken::new(), &p, None, None)
            .unwrap()
            .1
    }

    #[test]
    fn cross_protocol_resume_rejection_keeps_both_qos2_directions_ids_and_owner() {
        let cfg = Arc::new(MqttBrokerConfig {
            mqtt5: true,
            ..MqttBrokerConfig::loopback_development()
        });
        let mut s = State::new(cfg);
        let c = attach(&mut s, "owner");
        s.subscribe_with_options(
            s.generation,
            "owner",
            c,
            vec![("own".into(), Some(Qos::ExactlyOnce))],
            vec![crate::wire::SubscribeOptions {
                identifier: Some(7),
                ..Default::default()
            }],
        )
        .unwrap();
        let mut m = message();
        m.qos = Qos::ExactlyOnce;
        s.receive_qos2(s.generation, "owner", c, 42, m).unwrap();
        assert!(s.next_outbound(s.generation, "owner", c).unwrap().is_some());
        let id = s.sessions["owner"].pending[0].id.unwrap();
        s.outgoing_received(s.generation, "owner", c, id).unwrap();
        let cancel = s.sessions["owner"].online.as_ref().unwrap().cancel.clone();
        let old = s.snapshot();
        let before = serde_json::to_value(&old).unwrap();
        let p = Principal {
            key: s.sessions["owner"].owner.clone(),
            policy_version: 0,
            expires_at_ms: u64::MAX,
        };
        let request = Connect {
            v5: None,
            client_id: "owner".into(),
            clean: false,
            keep_alive: 0,
            will: None,
            username: None,
            password: None,
        };
        let revision = Some(s.sessions["owner"].revision);
        assert!(matches!(
            s.attach(
                s.generation,
                &request,
                CancellationToken::new(),
                &p,
                revision,
                None
            ),
            Err(MqttError::SessionConflict)
        ));
        assert!(!cancel.is_cancelled());
        let mut after = s.snapshot();
        after.captured_at_ms = old.captured_at_ms;
        assert_eq!(serde_json::to_value(after).unwrap(), before);
        assert_eq!(s.sessions["owner"].incoming_qos2.len(), 1);
        s.outgoing_completed(s.generation, "owner", c, id).unwrap();
        s.incoming_release(s.generation, "owner", c, 42).unwrap();
        assert_eq!(s.status().pending_messages, 0);
        assert_eq!(s.status().incoming_qos2, 0);
    }

    #[test]
    fn retained_origin_legacy_migration_filters_no_local_and_rejects_invalid_ledger_atomically() {
        let cfg = Arc::new(MqttBrokerConfig {
            mqtt5: true,
            ..MqttBrokerConfig::loopback_development()
        });
        let mut source = State::new(cfg.clone());
        source.route_from(&message(), Some("self/+")).unwrap(); // valid ClientID, not a Topic
        let snapshot = || source.snapshot();
        let mut restored = State::new(cfg.clone());
        restored.restore(snapshot()).unwrap();
        assert_eq!(
            restored.status().memory_used_bytes,
            source.status().memory_used_bytes
        );
        let before = serde_json::to_value(restored.snapshot()).unwrap();
        let memory = restored.status().memory_used_bytes;
        for bad in 0..5 {
            let mut s = snapshot();
            match bad {
                0 => {
                    s.retained_origins.clear();
                }
                1 => {
                    s.retained_origins
                        .insert("orphan".into(), RetainedOrigin::Internal);
                }
                2 => {
                    s.retained_origins
                        .insert("own".into(), RetainedOrigin::Client("bad\0id".into()));
                }
                3 => {
                    s.retained_origins
                        .insert("own".into(), RetainedOrigin::Client(String::new()));
                }
                4 => {
                    s.retained_origins.insert(
                        "own".into(),
                        RetainedOrigin::Client("x".repeat(cfg.max_client_id_bytes + 1)),
                    );
                }
                _ => unreachable!(),
            }
            assert!(restored.restore(s).is_err());
            assert_eq!(serde_json::to_value(restored.snapshot()).unwrap(), before);
            assert_eq!(restored.status().memory_used_bytes, memory);
        }
        let small = Arc::new(MqttBrokerConfig {
            memory_budget_bytes: memory - 1,
            ..(*cfg).clone()
        });
        let mut limited = State::new(small);
        assert!(limited.restore(snapshot()).is_err());
        assert_eq!(limited.status().memory_used_bytes, 0);
        for format in 1..=4 {
            let mut old = snapshot();
            old.format = format;
            old.retained_origins.clear();
            old.next_id = 1;
            let mut will = message();
            will.topic = "will".into();
            old.wills.push(WillImage {
                client: String::new(),
                delay: 0,
                due: None,
                expiry: None,
                id: 1,
                message: will,
                authorization: None,
                targets: None,
            });
            old.quota.wills = 1;
            let mut s = State::new(cfg.clone());
            s.restore(old).unwrap();
            assert!(matches!(
                s.retained["will"].origin,
                RetainedOrigin::LegacyUnknown
            ));
            // A second restore must preserve Unknown, rather than invent Internal.
            s.restore(s.snapshot()).unwrap();
            let c = attach(&mut s, "reader");
            let subscribe = |s: &mut State, no_local| {
                s.subscribe_with_options(
                    s.generation,
                    "reader",
                    c,
                    vec![("own".into(), Some(Qos::AtLeastOnce))],
                    vec![crate::wire::SubscribeOptions {
                        no_local,
                        ..Default::default()
                    }],
                )
                .unwrap();
            };
            subscribe(&mut s, true);
            assert!(
                s.next_outbound(s.generation, "reader", c)
                    .unwrap()
                    .is_none()
            );
            subscribe(&mut s, false);
            assert!(
                s.next_outbound(s.generation, "reader", c)
                    .unwrap()
                    .is_some()
            );
            // Republishing establishes known provenance and replaces Unknown.
            let mut m = message();
            m.topic = "new".into();
            s.route_from(&m, Some("other")).unwrap();
            s.subscribe_with_options(
                s.generation,
                "reader",
                c,
                vec![("new".into(), Some(Qos::AtLeastOnce))],
                vec![crate::wire::SubscribeOptions {
                    no_local: true,
                    ..Default::default()
                }],
            )
            .unwrap();
            assert!(
                s.next_outbound(s.generation, "reader", c)
                    .unwrap()
                    .is_some()
            );
        }
    }

    #[test]
    fn recovery_packet_bounds_cover_empty_metadata_real_properties_and_broker_identifiers() {
        let cfg = MqttBrokerConfig {
            mqtt5: true,
            max_packet_bytes: 8192,
            ..MqttBrokerConfig::loopback_development()
        };
        let mut m = message();
        m.topic = "a".into();
        m.payload = vec![b'x'; 8184].into();
        assert_eq!(codec::publish(&m, m.qos, true, Some(1), false).len(), 8192);
        valid_message(&m, &cfg).unwrap();
        m.payload = vec![b'x'; 8185].into();
        assert!(valid_message(&m, &cfg).is_err());
        m.payload = vec![b'x'; 8184].into();
        m.properties.subscription_identifiers = vec![7];
        valid_delivery_message(&m, &cfg).unwrap();
        assert!(valid_message(&m, &cfg).is_err());
        m.properties.subscription_identifiers.clear();
        m.properties.utf8_payload = true;
        // Real source properties must still count (3 bytes including length).
        m.payload = vec![b'x'; 8181].into();
        valid_message(&m, &cfg).unwrap();
        m.payload = vec![b'x'; 8182].into();
        assert!(valid_message(&m, &cfg).is_err());
        let mut cfg311 = cfg.clone();
        cfg311.mqtt5 = false;
        assert!(valid_message(&m, &cfg311).is_err());
        m.properties = MqttProperties::default();
        m.payload = vec![b'x'; 8184].into();
        valid_message(&m, &cfg311).unwrap();
    }
}

#[cfg(all(test, feature = "mqtt5"))]
mod alias_regressions {
    use super::*;

    #[test]
    fn alias_recovery_checks_binding_real_properties_budget_and_nonnetwork_limits() {
        let cfg = MqttBrokerConfig {
            mqtt5: true,
            max_packet_bytes: 8192,
            ..MqttBrokerConfig::loopback_development()
        };
        let mut m = MqttMessage {
            topic: "t".repeat(1000),
            payload: vec![b'x'; 8180].into(),
            qos: Qos::AtLeastOnce,
            retain: true,
            properties: Default::default(),
        };
        assert_eq!(crate::mqtt5::minimum_source_size(&m, true), 8191);
        assert!(valid_message(&m, &cfg).is_err());
        valid_network_message(&m, &cfg).unwrap();
        m.payload = vec![b'x'; 8181].into();
        assert_eq!(crate::mqtt5::minimum_source_size(&m, true), 8192);
        valid_network_message(&m, &cfg).unwrap();
        m.payload = vec![b'x'; 8182].into();
        assert!(valid_network_message(&m, &cfg).is_err());
        // Content Type occupies 126 property bytes; adding the three alias
        // bytes crosses 127, requiring a two-byte Property Length integer.
        m.properties.content_type = Some("c".repeat(123));
        m.payload = vec![b'x'; 8054].into();
        assert_eq!(crate::mqtt5::minimum_source_size(&m, true), 8192);
        valid_network_message(&m, &cfg).unwrap();
        m.payload = vec![b'x'; 8055].into();
        assert!(valid_network_message(&m, &cfg).is_err());
        m.properties.content_type = Some("bad\0type".into());
        assert!(valid_network_message(&m, &cfg).is_err());

        let tiny = MqttBrokerConfig {
            max_packet_bytes: 128,
            ..cfg.clone()
        };
        m.properties = Default::default();
        m.topic = "t".repeat(120);
        m.payload = vec![b'x'; 118].into();
        // Binding frame and aliased message each fit exactly in 128 bytes.
        assert_eq!(crate::mqtt5::minimum_source_size(&m, true), 128);
        valid_network_message(&m, &tiny).unwrap();
        m.topic.push('t'); // message still fits, but its binding cannot fit
        assert!(valid_network_message(&m, &tiny).is_err());
        m.topic = "t".repeat(1000);
        m.payload = vec![b'x'; 8180].into();
        assert!(
            valid_network_message(
                &m,
                &MqttBrokerConfig {
                    mqtt5: false,
                    ..cfg.clone()
                }
            )
            .is_err()
        );
        let mut source = State::new(Arc::new(cfg.clone()));
        source.route_from(&m, Some("network")).unwrap();
        let memory = source.status().memory_used_bytes;
        assert!(memory > m.payload.len() + m.topic.len());
        let mut target = State::new(Arc::new(cfg.clone()));
        target.restore(source.snapshot()).unwrap();
        assert_eq!(target.status().memory_used_bytes, memory);
        // A pre-fix format4 image with this already accepted message also
        // recovers, with Unknown provenance, instead of discarding the data.
        let mut legacy = source.snapshot();
        legacy.format = 4;
        legacy.retained_origins.clear();
        let mut old = State::new(Arc::new(cfg.clone()));
        old.restore(legacy).unwrap();
        assert_eq!(old.retained[&m.topic].message, m);
        let mut limited = State::new(Arc::new(MqttBrokerConfig {
            memory_budget_bytes: memory - 1,
            ..cfg.clone()
        }));
        assert!(limited.restore(source.snapshot()).is_err());
        assert_eq!(limited.status().memory_used_bytes, 0);

        let before = serde_json::to_value(target.snapshot()).unwrap();
        let mut internal = source.snapshot();
        internal
            .retained_origins
            .insert(m.topic.clone(), RetainedOrigin::Internal);
        assert!(target.restore(internal).is_err());
        let mut will = State::new(Arc::new(cfg.clone())).snapshot();
        will.next_id = 1;
        will.quota.wills = 1;
        will.wills.push(WillImage {
            client: "network".into(),
            delay: 0,
            due: None,
            expiry: None,
            id: 1,
            message: m.clone(),
            authorization: None,
            targets: None,
        });
        assert!(target.restore(will).is_err());
        let mut ledger = State::new(Arc::new(cfg)).snapshot();
        ledger.quota.publications = 1;
        ledger.publications.push(PublicationImage {
            source_kind: 1,
            source: "business".into(),
            request: MqttPublishRequest {
                request_id: "oversized".into(),
                topic: m.topic.clone(),
                payload: m.payload.clone(),
                qos: m.qos,
                retain: m.retain,
                properties: m.properties.clone(),
            },
            result: Ok(MqttAcceptReceipt {
                request_id: "oversized".into(),
                accepted: true,
                durability: AcceptanceDurability::DurableCommitted,
            }),
        });
        assert!(target.restore(ledger).is_err());
        assert_eq!(serde_json::to_value(target.snapshot()).unwrap(), before);
        assert_eq!(target.status().memory_used_bytes, memory);
    }
}
