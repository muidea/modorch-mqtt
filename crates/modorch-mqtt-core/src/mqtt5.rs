//! MQTT5 wire translation; ownership and persistence stay in the existing Core.
use crate::{
    codec::{self, Packet},
    wire::{ConnectOptions, SubscribeOptions},
};
use bytes::{Bytes, BytesMut};
use modorch_mqtt_api::*;
use rumqttc::v5::mqttbytes::{QoS, v5 as v};
use std::collections::BTreeMap;
mod validation;
#[derive(Default)]
pub(crate) struct Codec {
    aliases: BTreeMap<u16, String>,
    pub subscriptions: Vec<SubscribeOptions>,
    assigned: bool,
    pub disconnect_expiry: Option<u32>,
    pub disconnect_will: bool,
    pub negative_rec: Option<u16>,
    pub maximum_packet: usize,
}
fn invalid() -> MqttError {
    MqttError::Protocol("invalid MQTT5 packet")
}
fn qos(q: QoS) -> Qos {
    match q {
        QoS::AtMostOnce => Qos::AtMostOnce,
        QoS::AtLeastOnce => Qos::AtLeastOnce,
        QoS::ExactlyOnce => Qos::ExactlyOnce,
    }
}
fn properties(p: Option<v::PublishProperties>) -> Result<MqttProperties, MqttError> {
    let p = p.unwrap_or_default();
    if p.payload_format_indicator.is_some_and(|v| v > 1) || !p.subscription_identifiers.is_empty() {
        return Err(invalid());
    }
    let result = MqttProperties {
        expires_at_ms: p
            .message_expiry_interval
            .map(|s| unix_time_ms().saturating_add(u64::from(s) * 1000)),
        utf8_payload: p.payload_format_indicator == Some(1),
        content_type: p.content_type,
        response_topic: p.response_topic,
        correlation_data: p.correlation_data,
        user_properties: p.user_properties,
        subscription_identifiers: vec![],
    };
    validate_properties(&result)?;
    Ok(result)
}
pub(crate) fn validate_properties(p: &MqttProperties) -> Result<(), MqttError> {
    let (mut ordinary, mut origin, mut hops) = (0usize, false, false);
    for (key, _) in &p.user_properties {
        match key.as_str() {
            BRIDGE_ORIGIN_PROPERTY if !origin => origin = true,
            BRIDGE_HOPS_PROPERTY if !hops => hops = true,
            BRIDGE_ORIGIN_PROPERTY | BRIDGE_HOPS_PROPERTY => return Err(invalid()),
            _ => ordinary += 1,
        }
    }
    if ordinary > 128
        || p.subscription_identifiers
            .iter()
            .any(|v| *v == 0 || *v > 268435455)
    {
        return Err(invalid());
    }
    for value in p
        .user_properties
        .iter()
        .flat_map(|(k, v)| [k, v])
        .chain(p.content_type.iter())
        .chain(p.response_topic.iter())
    {
        codec::validate_string(value)?;
        if value.len() > 65535 {
            return Err(invalid());
        }
    }
    if let Some(topic) = &p.response_topic {
        codec::validate_topic(topic)?;
    }
    if p.correlation_data.as_ref().is_some_and(|p| p.len() > 65535) {
        return Err(invalid());
    }
    Ok(())
}
impl Codec {
    pub fn decode(&mut self, bytes: &[u8], max_filters: usize) -> Result<Packet, MqttError> {
        let login_bytes = validation::validate(bytes)?;
        // The client codec models passwords as UTF-8; substitute only its temporary
        // decode input, retaining the original binary field from strict validation.
        let mut sanitized = bytes.to_vec();
        if let Some((_, Some(password))) = &login_bytes {
            let start = sanitized.len() - password.len();
            sanitized[start..].fill(b'x');
        }
        let mut input = BytesMut::from(sanitized.as_slice());
        let packet =
            v::Packet::read(&mut input, Some(bytes.len() as u32)).map_err(|_| invalid())?;
        if !input.is_empty() {
            return Err(invalid());
        }
        Ok(match packet {
            v::Packet::Connect(c, will, _login) => {
                let p = c.properties.unwrap_or_default();
                if p.receive_maximum == Some(0)
                    || p.max_packet_size == Some(0)
                    || p.request_problem_info.is_some_and(|v| v > 1)
                    || p.request_response_info.is_some_and(|v| v > 1)
                    || p.authentication_method.is_some()
                    || p.authentication_data.is_some()
                {
                    return Err(invalid());
                }
                self.maximum_packet = p.max_packet_size.unwrap_or(268435455) as usize;
                let (username, password) = login_bytes.ok_or_else(invalid)?;
                codec::validate_string(&c.client_id)?;
                self.assigned = c.client_id.is_empty();
                let mut will_delay = 0;
                let mut will_expiry = None;
                let will = will
                    .map(|w| {
                        let props = w.properties.unwrap_or(v::LastWillProperties {
                            delay_interval: None,
                            payload_format_indicator: None,
                            message_expiry_interval: None,
                            content_type: None,
                            response_topic: None,
                            correlation_data: None,
                            user_properties: vec![],
                        });
                        will_delay = props.delay_interval.unwrap_or(0);
                        will_expiry = props.message_expiry_interval;
                        let message = MqttMessage {
                            topic: String::from_utf8(w.topic.to_vec()).map_err(|_| invalid())?,
                            payload: w.message,
                            qos: qos(w.qos),
                            retain: w.retain,
                            properties: properties(Some(v::PublishProperties {
                                payload_format_indicator: props.payload_format_indicator,
                                message_expiry_interval: None,
                                response_topic: props.response_topic,
                                correlation_data: props.correlation_data,
                                user_properties: props.user_properties,
                                content_type: props.content_type,
                                ..Default::default()
                            }))?,
                        };
                        if message.properties.utf8_payload
                            && std::str::from_utf8(&message.payload).is_err()
                        {
                            return Err(invalid());
                        }
                        codec::validate_topic(&message.topic)?;
                        Ok::<_, MqttError>(message)
                    })
                    .transpose()?;
                Packet::Connect(codec::Connect {
                    client_id: c.client_id,
                    clean: c.clean_start,
                    keep_alive: c.keep_alive,
                    will,
                    username,
                    password,
                    v5: Some(ConnectOptions {
                        will_delay,
                        will_expiry,
                        session_expiry: p.session_expiry_interval.unwrap_or(0),
                        receive_maximum: p.receive_maximum.unwrap_or(u16::MAX),
                        maximum_packet_size: p.max_packet_size.unwrap_or(268435455),
                    }),
                })
            }
            v::Packet::Publish(p) => {
                let alias = p.properties.as_ref().and_then(|p| p.topic_alias);
                let mut topic = String::from_utf8(p.topic.to_vec()).map_err(|_| invalid())?;
                if let Some(alias) = alias {
                    if alias == 0 || alias > 32 {
                        return Err(invalid());
                    }
                    if topic.is_empty() {
                        topic = self.aliases.get(&alias).cloned().ok_or_else(invalid)?;
                    } else {
                        codec::validate_topic(&topic)?;
                        self.aliases.insert(alias, topic.clone());
                    }
                }
                codec::validate_topic(&topic)?;
                let quality = qos(p.qos);
                if quality != Qos::AtMostOnce && p.pkid == 0 {
                    return Err(invalid());
                }
                let props = properties(p.properties)?;
                if props.utf8_payload && std::str::from_utf8(&p.payload).is_err() {
                    return Err(invalid());
                }
                Packet::Publish {
                    message: MqttMessage {
                        topic,
                        payload: p.payload,
                        qos: quality,
                        retain: p.retain,
                        properties: props,
                    },
                    packet_id: (quality != Qos::AtMostOnce).then_some(p.pkid),
                }
            }
            v::Packet::Subscribe(p) => {
                if p.pkid == 0 || p.filters.is_empty() || p.filters.len() > max_filters {
                    return Err(invalid());
                }
                let identifier = p.properties.and_then(|p| p.id).map(|n| n as u32);
                if identifier == Some(0) {
                    return Err(invalid());
                }
                let mut filters = vec![];
                self.subscriptions.clear();
                for f in p.filters {
                    codec::validate_filter(&f.path)?;
                    if f.path.starts_with("$share/")
                        && (shared_filter(&f.path).is_none() || f.nolocal)
                    {
                        return Err(invalid());
                    }
                    self.subscriptions.push(SubscribeOptions {
                        no_local: f.nolocal,
                        retain_as_published: f.preserve_retain,
                        retain_handling: match f.retain_forward_rule {
                            v::RetainForwardRule::OnEverySubscribe => 0,
                            v::RetainForwardRule::OnNewSubscribe => 1,
                            v::RetainForwardRule::Never => 2,
                        },
                        identifier,
                    });
                    filters.push((f.path, Some(qos(f.qos))));
                }
                Packet::Subscribe {
                    id: p.pkid,
                    filters,
                }
            }
            v::Packet::Unsubscribe(p) => {
                if p.pkid == 0 || p.filters.is_empty() || p.filters.len() > max_filters {
                    return Err(invalid());
                }
                for f in &p.filters {
                    codec::validate_filter(f)?;
                }
                Packet::Unsubscribe {
                    id: p.pkid,
                    filters: p.filters,
                }
            }
            v::Packet::PubAck(p) if p.pkid != 0 => Packet::PubAck(p.pkid),
            v::Packet::PubRec(p) if p.pkid != 0 => {
                if !matches!(
                    p.reason,
                    v::PubRecReason::Success | v::PubRecReason::NoMatchingSubscribers
                ) {
                    self.negative_rec = Some(p.pkid);
                }
                Packet::PubRec(p.pkid)
            }
            v::Packet::PubRel(p) if p.pkid != 0 => Packet::PubRel(p.pkid),
            v::Packet::PubComp(p) if p.pkid != 0 => Packet::PubComp(p.pkid),
            v::Packet::PingReq(_) => Packet::Ping,
            v::Packet::Disconnect(p) => {
                self.disconnect_will =
                    p.reason_code == v::DisconnectReasonCode::DisconnectWithWillMessage;
                self.disconnect_expiry = p.properties.and_then(|p| p.session_expiry_interval);
                Packet::Disconnect
            }
            _ => return Err(invalid()),
        })
    }
    pub fn response(
        &self,
        frame: Vec<u8>,
        client: Option<&str>,
        max_packet: usize,
        max_receive: usize,
    ) -> Vec<u8> {
        let mut offset = 1;
        while frame[offset] & 128 != 0 {
            offset += 1;
        }
        offset += 1;
        let data = &frame[offset..];
        match frame[0] >> 4 {
            2 => {
                let mut props = vec![];
                if data[1] == 0 {
                    props.extend([0x21]);
                    props.extend((max_receive.min(u16::MAX as usize) as u16).to_be_bytes());
                    props.extend([0x22, 0, 32, 0x27]);
                    props.extend((max_packet as u32).to_be_bytes());
                    if self.assigned {
                        if let Some(client) = client {
                            props.push(0x12);
                            props.extend((client.len() as u16).to_be_bytes());
                            props.extend(client.as_bytes());
                        }
                    }
                }
                let reason = match data[1] {
                    0 => 0,
                    1 => 0x84,
                    2 => 0x85,
                    3 => 0x88,
                    4 => 0x86,
                    _ => 0x87,
                };
                let mut body = vec![data[0], reason];
                body.extend(varint(props.len()));
                body.extend(props);
                codec::frame(0x20, &body)
            }
            9 => {
                let mut body = data[..2].to_vec();
                body.push(0);
                body.extend(data[2..].iter().map(|c| if *c == 0x80 { 0x87 } else { *c }));
                codec::frame(0x90, &body)
            }
            11 => {
                let mut body = data[..2].to_vec();
                body.push(0);
                body.extend_from_slice(&data[2..]);
                codec::frame(0xb0, &body)
            }
            _ => frame,
        }
    }
}
pub fn shared_filter(filter: &str) -> Option<(&str, &str)> {
    let rest = filter.strip_prefix("$share/")?;
    let (group, actual) = rest.split_once('/')?;
    (!group.is_empty() && !group.contains(['+', '#']) && !actual.is_empty())
        .then_some((group, actual))
}
fn varint(mut n: usize) -> Vec<u8> {
    let mut bytes = vec![];
    loop {
        let mut byte = (n % 128) as u8;
        n /= 128;
        if n > 0 {
            byte |= 128;
        }
        bytes.push(byte);
        if n == 0 {
            break;
        }
    }
    bytes
}
fn publish_packet(
    message: &MqttMessage,
    qos: Qos,
    retain: bool,
    id: Option<u16>,
    dup: bool,
) -> v::Publish {
    let p = &message.properties;
    let props = v::PublishProperties {
        payload_format_indicator: p.utf8_payload.then_some(1),
        message_expiry_interval: p.expires_at_ms.map(|t| {
            t.saturating_sub(unix_time_ms())
                .div_ceil(1000)
                .min(u32::MAX as u64) as u32
        }),
        topic_alias: None,
        response_topic: p.response_topic.clone(),
        correlation_data: p.correlation_data.clone(),
        user_properties: p.user_properties.clone(),
        subscription_identifiers: p
            .subscription_identifiers
            .iter()
            .map(|n| *n as usize)
            .collect(),
        content_type: p.content_type.clone(),
    };
    let mut packet = v::Publish::new(
        message.topic.clone(),
        match qos {
            Qos::AtMostOnce => QoS::AtMostOnce,
            Qos::AtLeastOnce => QoS::AtLeastOnce,
            Qos::ExactlyOnce => QoS::ExactlyOnce,
        },
        message.payload.clone(),
        Some(props),
    );
    packet.retain = retain;
    packet.dup = dup;
    packet.pkid = id.unwrap_or(0);
    packet
}

/// Minimum source frame limit, including establishment of a connection-local alias.
/// Only recovery of network-owned data uses this bound. No alias is persisted
/// or emitted by this calculation; the full Topic stays in the stored message.
pub(crate) fn minimum_source_size(message: &MqttMessage, allow_alias: bool) -> usize {
    let mut packet = publish_packet(
        message,
        message.qos,
        message.retain,
        (message.qos != Qos::AtMostOnce).then_some(1),
        false,
    );
    let full = packet.size();
    if !allow_alias {
        return full;
    }
    let mut binding = v::Publish::new(
        message.topic.clone(),
        QoS::AtMostOnce,
        Bytes::new(),
        Some(v::PublishProperties {
            topic_alias: Some(1),
            ..Default::default()
        }),
    );
    binding.pkid = 0;
    packet.topic = Bytes::new();
    packet.properties.as_mut().unwrap().topic_alias = Some(1);
    full.min(packet.size().max(binding.size()))
}

pub(crate) fn publish_size(message: &MqttMessage, qos: Qos, id: Option<u16>) -> usize {
    publish_packet(message, qos, message.retain, id, false).size()
}

pub fn publish(
    message: &MqttMessage,
    qos: Qos,
    retain: bool,
    id: Option<u16>,
    dup: bool,
) -> Vec<u8> {
    let packet = publish_packet(message, qos, retain, id, dup);
    let mut bytes = BytesMut::new();
    packet
        .write(&mut bytes)
        .expect("validated bounded MQTT5 publication");
    bytes.to_vec()
}
