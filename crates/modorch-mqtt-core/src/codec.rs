//! Incremental, cancellation-safe frame parsing. Socket reads never own partially
//! decoded protocol state outside the connection's persistent receive buffer.
use bytes::Bytes;
use modorch_mqtt_api::{MqttError, MqttMessage, Qos};

type Result<T> = std::result::Result<T, MqttError>;

#[derive(Debug)]
pub(crate) struct Connect {
    pub client_id: String,
    pub clean: bool,
    pub keep_alive: u16,
    pub will: Option<MqttMessage>,
}

#[derive(Debug)]
pub(crate) enum Packet {
    Connect(Connect),
    Publish {
        message: MqttMessage,
        packet_id: Option<u16>,
    },
    PubAck(u16),
    Subscribe {
        id: u16,
        filters: Vec<(String, Qos)>,
    },
    Unsubscribe {
        id: u16,
        filters: Vec<String>,
    },
    Ping,
    Disconnect,
}

fn error(message: &'static str) -> MqttError {
    MqttError::Protocol(message)
}

pub(crate) fn validate_string(value: &str) -> Result<()> {
    if value.contains('\0') {
        return Err(error("NUL in MQTT UTF-8 string"));
    }
    Ok(())
}

pub(crate) fn validate_topic(topic: &str) -> Result<()> {
    validate_string(topic)?;
    if topic.is_empty() || topic.len() > 65535 || topic.contains(['+', '#']) {
        return Err(error("invalid Topic Name"));
    }
    Ok(())
}

pub(crate) fn validate_filter(filter: &str) -> Result<()> {
    validate_string(filter)?;
    if filter.is_empty() || filter.len() > 65535 {
        return Err(error("invalid Topic Filter length"));
    }
    let mut parts = filter.split('/').peekable();
    while let Some(level) = parts.next() {
        if (level.contains('+') && level != "+")
            || (level.contains('#') && (level != "#" || parts.peek().is_some()))
        {
            return Err(error("invalid Topic Filter wildcard"));
        }
    }
    Ok(())
}

pub(crate) fn matches(filter: &str, topic: &str) -> bool {
    if topic.starts_with('$') && filter.starts_with(['+', '#']) {
        return false;
    }
    let mut actual = topic.split('/');
    for level in filter.split('/') {
        match level {
            "#" => return true,
            "+" => {
                if actual.next().is_none() {
                    return false;
                }
            }
            literal => {
                if actual.next() != Some(literal) {
                    return false;
                }
            }
        }
    }
    actual.next().is_none()
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}
impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }
    fn byte(&mut self) -> Result<u8> {
        let value = *self
            .bytes
            .get(self.offset)
            .ok_or(error("truncated packet"))?;
        self.offset += 1;
        Ok(value)
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes([self.byte()?, self.byte()?]))
    }
    fn id(&mut self) -> Result<u16> {
        let id = self.u16()?;
        if id == 0 {
            return Err(error("zero Packet Identifier"));
        }
        Ok(id)
    }
    fn binary(&mut self) -> Result<&'a [u8]> {
        let len = self.u16()? as usize;
        let end = self
            .offset
            .checked_add(len)
            .ok_or(error("length overflow"))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(error("truncated string"))?;
        self.offset = end;
        Ok(value)
    }
    fn string(&mut self) -> Result<String> {
        let value = std::str::from_utf8(self.binary()?).map_err(|_| error("invalid UTF-8"))?;
        validate_string(value)?;
        Ok(value.to_owned())
    }
    fn done(&self) -> Result<()> {
        if self.offset != self.bytes.len() {
            return Err(error("extra packet bytes"));
        }
        Ok(())
    }
}

/// Return a complete frame size without consuming partial bytes.
pub(crate) fn frame_len(buffer: &[u8], max: usize) -> Result<Option<usize>> {
    if buffer.len() < 2 {
        return Ok(None);
    }
    let mut remaining = 0usize;
    for index in 1..=4 {
        let Some(&value) = buffer.get(index) else {
            return Ok(None);
        };
        remaining |= ((value & 127) as usize) << (7 * (index - 1));
        if value & 128 == 0 {
            let total = remaining + index + 1;
            if total > max {
                return Err(MqttError::Capacity("maximum packet bytes"));
            }
            if index > 1 && value == 0 {
                return Err(error("nonminimal Remaining Length"));
            }
            return Ok((buffer.len() >= total).then_some(total));
        }
    }
    Err(error("Remaining Length exceeds four bytes"))
}

pub(crate) fn decode(frame: &[u8], max_filters: usize) -> Result<Packet> {
    let header = *frame.first().ok_or(error("missing fixed header"))?;
    let mut start = 1;
    while *frame.get(start).ok_or(error("missing Remaining Length"))? & 128 != 0 {
        start += 1;
    }
    start += 1;
    let mut c = Cursor::new(&frame[start..]);
    if header >> 4 != 3 {
        let required = if matches!(header >> 4, 8 | 10) { 2 } else { 0 };
        if header & 15 != required {
            return Err(error("invalid fixed header flags"));
        }
    }
    let packet = match header >> 4 {
        1 => {
            if c.string()? != "MQTT" {
                return Err(error("incorrect protocol name"));
            }
            if c.byte()? != 4 {
                return Err(MqttError::Unsupported("MQTT protocol level"));
            }
            let flags = c.byte()?;
            let keep_alive = c.u16()?;
            if flags & 1 != 0 || (flags & 64 != 0 && flags & 128 == 0) {
                return Err(error("invalid CONNECT flags"));
            }
            let will_qos = (flags >> 3) & 3;
            if will_qos == 3 || (flags & 4 == 0 && flags & 0x38 != 0) {
                return Err(error("invalid Will flags"));
            }
            let client_id = c.string()?;
            let will = if flags & 4 != 0 {
                let topic = c.string()?;
                validate_topic(&topic)?;
                let payload = Bytes::copy_from_slice(c.binary()?);
                let qos = Qos::try_from(will_qos)?;
                if qos == Qos::ExactlyOnce {
                    return Err(MqttError::Unsupported("QoS2 Will"));
                }
                Some(MqttMessage {
                    topic,
                    payload,
                    qos,
                    retain: flags & 32 != 0,
                })
            } else {
                None
            };
            // Consume credentials for framing validation, but do not silently
            // treat credential-bearing connections as authenticated.
            if flags & 128 != 0 {
                c.string()?;
            }
            if flags & 64 != 0 {
                c.binary()?;
            }
            if flags & 192 != 0 {
                return Err(MqttError::Unsupported("authentication"));
            }
            c.done()?;
            Packet::Connect(Connect {
                client_id,
                clean: flags & 2 != 0,
                keep_alive,
                will,
            })
        }
        3 => {
            let qos = Qos::try_from((header >> 1) & 3)?;
            if qos == Qos::ExactlyOnce {
                return Err(MqttError::Unsupported("QoS2 PUBLISH"));
            }
            if qos == Qos::AtMostOnce && header & 8 != 0 {
                return Err(error("QoS0 DUP flag"));
            }
            let topic = c.string()?;
            validate_topic(&topic)?;
            let packet_id = if qos != Qos::AtMostOnce {
                Some(c.id()?)
            } else {
                None
            };
            let payload = Bytes::copy_from_slice(&c.bytes[c.offset..]);
            Packet::Publish {
                message: MqttMessage {
                    topic,
                    payload,
                    qos,
                    retain: header & 1 != 0,
                },
                packet_id,
            }
        }
        4 => {
            let id = c.id()?;
            c.done()?;
            Packet::PubAck(id)
        }
        8 => {
            let id = c.id()?;
            let mut filters = Vec::new();
            while c.offset < c.bytes.len() {
                if filters.len() == max_filters {
                    return Err(MqttError::Capacity("SUBSCRIBE filters per packet"));
                }
                let filter = c.string()?;
                validate_filter(&filter)?;
                let qos = Qos::try_from(c.byte()?)?;
                filters.push((filter, qos));
            }
            if filters.is_empty() {
                return Err(error("empty SUBSCRIBE"));
            }
            Packet::Subscribe { id, filters }
        }
        10 => {
            let id = c.id()?;
            let mut filters = Vec::new();
            while c.offset < c.bytes.len() {
                if filters.len() == max_filters {
                    return Err(MqttError::Capacity("UNSUBSCRIBE filters per packet"));
                }
                let filter = c.string()?;
                validate_filter(&filter)?;
                filters.push(filter);
            }
            if filters.is_empty() {
                return Err(error("empty UNSUBSCRIBE"));
            }
            Packet::Unsubscribe { id, filters }
        }
        12 => {
            c.done()?;
            Packet::Ping
        }
        14 => {
            c.done()?;
            Packet::Disconnect
        }
        _ => return Err(error("unexpected client control packet")),
    };
    Ok(packet)
}

pub(crate) fn frame(header: u8, body: &[u8]) -> Vec<u8> {
    let mut output = vec![header];
    let mut len = body.len();
    loop {
        let mut value = (len % 128) as u8;
        len /= 128;
        if len != 0 {
            value |= 128;
        }
        output.push(value);
        if len == 0 {
            break;
        }
    }
    output.extend_from_slice(body);
    output
}

pub(crate) fn publish(
    message: &MqttMessage,
    qos: Qos,
    retain: bool,
    id: Option<u16>,
    dup: bool,
) -> Vec<u8> {
    let mut body = Vec::with_capacity(4 + message.topic.len() + message.payload.len());
    body.extend_from_slice(&(message.topic.len() as u16).to_be_bytes());
    body.extend_from_slice(message.topic.as_bytes());
    if let Some(id) = id {
        body.extend_from_slice(&id.to_be_bytes());
    }
    body.extend_from_slice(&message.payload);
    frame(
        0x30 | ((qos as u8) << 1) | u8::from(retain) | (u8::from(dup) << 3),
        &body,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fragmented_length_and_malformed_frames_are_bounded() {
        assert_eq!(frame_len(&[0x30, 0x80], 1024).unwrap(), None);
        assert!(frame_len(&[0x30, 0xff, 0xff, 0xff, 0xff], usize::MAX).is_err());
        assert!(frame_len(&[0x30, 0x80, 0], 1024).is_err());
        assert!(frame_len(&[0x30, 0x80, 0x20], 1024).is_err());
        for size in [0, 127, 128, 16383, 16384] {
            let frame = frame(0x30, &vec![0; size]);
            for end in [1, frame.len() - 1] {
                assert_eq!(frame_len(&frame[..end], usize::MAX).unwrap(), None);
            }
            assert_eq!(frame_len(&frame, usize::MAX).unwrap(), Some(frame.len()));
        }
    }

    #[test]
    fn mqtt_topic_edges() {
        for (filter, topic, expected) in [
            ("a/#", "a", true),
            ("a/+", "a/", true),
            ("#", "$SYS/a", false),
            ("+/#", "$SYS/a", false),
            ("$SYS/#", "$SYS/a", true),
            ("a/:id", "a/1", false),
            ("/+/", "//", true),
            ("a/+", "a", false),
        ] {
            assert_eq!(matches(filter, topic), expected, "{filter} -> {topic}");
        }
        for filter in ["a/#/b", "a+", "a/+b", "", "a\0"] {
            assert!(validate_filter(filter).is_err());
        }
        for topic in ["", "a/+", "a/#", "a\0"] {
            assert!(validate_topic(topic).is_err());
        }
    }
}
