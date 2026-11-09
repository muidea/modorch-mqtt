//! Validate property multiplicity, direction and byte boundaries before the codec.
use super::*;
struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], MqttError> {
        if self.0.len() < n {
            return Err(invalid());
        }
        let (a, b) = self.0.split_at(n);
        self.0 = b;
        Ok(a)
    }
    fn byte(&mut self) -> Result<u8, MqttError> {
        Ok(self.take(1)?[0])
    }
    fn word(&mut self) -> Result<u16, MqttError> {
        let b = self.take(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }
    fn int(&mut self) -> Result<usize, MqttError> {
        let mut n = 0;
        for i in 0..4 {
            let b = self.byte()?;
            n |= usize::from(b & 127) << (7 * i);
            if b & 128 == 0 {
                if i > 0 && b == 0 {
                    return Err(invalid());
                }
                return Ok(n);
            }
        }
        Err(invalid())
    }
    fn binary(&mut self) -> Result<&'a [u8], MqttError> {
        let n = usize::from(self.word()?);
        self.take(n)
    }
    fn string(&mut self) -> Result<String, MqttError> {
        let s = std::str::from_utf8(self.binary()?).map_err(|_| invalid())?;
        codec::validate_string(s)?;
        Ok(s.to_owned())
    }
    fn props(&mut self, kind: u8) -> Result<(), MqttError> {
        let n = self.int()?;
        let mut p = Reader(self.take(n)?);
        let mut seen = std::collections::BTreeSet::new();
        while !p.0.is_empty() {
            let id = p.byte()?;
            let allowed = match kind {
                1 => matches!(
                    id,
                    0x11 | 0x15 | 0x16 | 0x17 | 0x19 | 0x21 | 0x22 | 0x26 | 0x27
                ),
                0 => matches!(id, 1 | 2 | 3 | 8 | 9 | 0x18 | 0x26),
                3 => matches!(id, 1 | 2 | 3 | 8 | 9 | 0x23 | 0x26),
                8 => matches!(id, 0x0b | 0x26),
                10 => id == 0x26,
                4..=7 => matches!(id, 0x1f | 0x26),
                14 => matches!(id, 0x11 | 0x1f | 0x26),
                _ => false,
            };
            if !allowed || (id != 0x26 && !seen.insert(id)) {
                return Err(invalid());
            }
            match id {
                1 | 0x17 | 0x19 => {
                    if p.byte()? > 1 {
                        return Err(invalid());
                    }
                }
                2 | 0x11 | 0x18 => {
                    p.take(4)?;
                }
                0x27 => {
                    if p.take(4)? == [0, 0, 0, 0] {
                        return Err(invalid());
                    }
                }
                0x21 | 0x23 => {
                    if p.word()? == 0 {
                        return Err(invalid());
                    }
                }
                0x22 => {
                    p.word()?;
                }
                0x0b => {
                    if p.int()? == 0 {
                        return Err(invalid());
                    }
                }
                3 | 8 | 0x15 | 0x1f => {
                    let s = p.string()?;
                    if id == 8 {
                        codec::validate_topic(&s)?;
                    }
                }
                9 | 0x16 => {
                    p.binary()?;
                }
                0x26 => {
                    p.string()?;
                    p.string()?;
                }
                _ => return Err(invalid()),
            }
        }
        Ok(())
    }
}
type LoginBytes = (Option<String>, Option<Bytes>);
pub(super) fn validate(bytes: &[u8]) -> Result<Option<LoginBytes>, MqttError> {
    let mut r = Reader(&bytes[1..]);
    let n = r.int()?;
    if n != r.0.len() {
        return Err(invalid());
    }
    let kind = bytes[0] >> 4;
    match kind {
        1 => {
            if r.string()? != "MQTT" || r.byte()? != 5 {
                return Err(invalid());
            }
            let flags = r.byte()?;
            if flags & 1 != 0 || flags & 0x04 == 0 && flags & 0x38 != 0 || flags & 0x18 == 0x18 {
                return Err(invalid());
            }
            r.word()?;
            r.props(1)?;
            r.string()?;
            if flags & 4 != 0 {
                r.props(0)?;
                let t = r.string()?;
                codec::validate_topic(&t)?;
                r.binary()?;
            }
            let username = if flags & 128 != 0 {
                Some(r.string()?)
            } else {
                None
            };
            let password = if flags & 64 != 0 {
                Some(Bytes::copy_from_slice(r.binary()?))
            } else {
                None
            };
            if !r.0.is_empty() {
                return Err(invalid());
            }
            return Ok(Some((username, password)));
        }
        3 => {
            r.string()?;
            if bytes[0] & 6 != 0 && r.word()? == 0 {
                return Err(invalid());
            }
            r.props(3)?;
            return Ok(None);
        }
        8 | 10 => {
            if r.word()? == 0 {
                return Err(invalid());
            }
            r.props(kind)?;
            if r.0.is_empty() {
                return Err(invalid());
            }
            while !r.0.is_empty() {
                let f = r.string()?;
                codec::validate_filter(&f)?;
                if kind == 8 {
                    let o = r.byte()?;
                    if o & 0xc0 != 0 || o & 3 == 3 || (o >> 4) & 3 == 3 {
                        return Err(invalid());
                    }
                }
            }
        }
        4..=7 => {
            if r.word()? == 0 {
                return Err(invalid());
            }
            if !r.0.is_empty() {
                let reason = r.byte()?;
                // Tables3-4/3-5 reserve No matching subscribers(0x10)
                // for Server senders. Check the Client direction before a
                // decoded ACK can complete or advance durable ownership.
                let allowed = match kind {
                    4 | 5 => matches!(reason, 0 | 0x80 | 0x83 | 0x87 | 0x90 | 0x91 | 0x97 | 0x99),
                    _ => matches!(reason, 0 | 0x92),
                };
                if !allowed {
                    return Err(invalid());
                }
                if !r.0.is_empty() {
                    r.props(kind)?;
                }
            }
        }
        12 => {}
        14 => {
            if !r.0.is_empty() {
                // Table3-10 distinguishes Client and Server senders. A
                // server-only code is not a valid graceful client disconnect.
                if !matches!(
                    r.byte()?,
                    0 | 4 | 0x80..=0x83 | 0x90 | 0x93..=0x99
                ) {
                    return Err(invalid());
                }
                if !r.0.is_empty() {
                    r.props(14)?;
                }
            }
        }
        _ => return Err(invalid()),
    }
    if !r.0.is_empty() {
        return Err(invalid());
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Table2-4 property identifiers/types, with legal nonzero scalar values.
    const PROPERTIES: &[(u8, &[u8])] = &[
        (1, &[1]),
        (2, &[0, 0, 0, 1]),
        (3, &[0, 1, b'x']),
        (8, &[0, 1, b't']),
        (9, &[0, 1, 0xff]),
        (11, &[1]),
        (17, &[0, 0, 0, 1]),
        (18, &[0, 1, b'c']),
        (19, &[0, 1]),
        (21, &[0, 1, b'm']),
        (22, &[0, 1, 0xff]),
        (23, &[1]),
        (24, &[0, 0, 0, 1]),
        (25, &[1]),
        (26, &[0, 1, b'r']),
        (28, &[0, 1, b's']),
        (31, &[0, 1, b'r']),
        (33, &[0, 1]),
        (34, &[0, 1]),
        (35, &[0, 1]),
        (36, &[1]),
        (37, &[1]),
        (38, &[0, 1, b'k', 0, 1, b'v']),
        (39, &[0, 0, 0, 64]),
        (40, &[1]),
        (41, &[1]),
        (42, &[1]),
    ];

    fn packet(kind: u8, props: &[u8]) -> Vec<u8> {
        let mut body = match kind {
            0 | 1 => vec![
                0,
                4,
                b'M',
                b'Q',
                b'T',
                b'T',
                5,
                if kind == 0 { 6 } else { 2 },
                0,
                0,
            ],
            3 => vec![0, 1, b't'],
            4..=7 | 8 | 10 => vec![0, 1],
            14 => vec![0],
            _ => unreachable!(),
        };
        if kind == 0 {
            body.extend([0, 0, 1, b'c']); // CONNECT props and ClientID
        }
        if matches!(kind, 4..=7) {
            body.push(0); // explicit Success
        }
        body.push(props.len() as u8);
        body.extend(props);
        match kind {
            0 => body.extend([0, 1, b'w', 0, 1, b'p']), // Will topic and binary payload
            1 => body.extend([0, 1, b'c']),
            8 => body.extend([0, 1, b't', 0]),
            10 => body.extend([0, 1, b't']),
            _ => (),
        }
        let header = match kind {
            0 | 1 => 0x10,
            6 | 8 | 10 => (kind << 4) | 2,
            _ => kind << 4,
        };
        codec::frame(header, &body)
    }

    #[test]
    fn mqtt5_all_property_types_follow_client_direction_cardinality_and_field_boundaries() {
        // Independently transcribed Table2-4 plus per-packet direction rules.
        let cases: &[(u8, &[u8])] = &[
            (0, &[1, 2, 3, 8, 9, 24, 38]),
            (1, &[17, 21, 22, 23, 25, 33, 34, 38, 39]),
            (3, &[1, 2, 3, 8, 9, 35, 38]),
            (4, &[31, 38]),
            (5, &[31, 38]),
            (6, &[31, 38]),
            (7, &[31, 38]),
            (8, &[11, 38]),
            (10, &[38]),
            (14, &[17, 31, 38]),
        ];
        for (kind, allowed) in cases {
            assert!(validate(&packet(*kind, &[])).is_ok());
            for (id, value) in PROPERTIES {
                let mut prop = vec![*id];
                prop.extend(*value);
                assert_eq!(
                    validate(&packet(*kind, &prop)).is_ok(),
                    allowed.contains(id),
                    "packet {kind}, property {id}"
                );
                if !allowed.contains(id) {
                    continue;
                }
                let twice = [prop.as_slice(), prop.as_slice()].concat();
                assert_eq!(
                    validate(&packet(*kind, &twice)).is_ok(),
                    *id == 38,
                    "duplicate packet {kind}, property {id}"
                );
                for end in 1..prop.len() {
                    assert!(
                        validate(&packet(*kind, &prop[..end])).is_err(),
                        "truncated packet {kind}, property {id}, end {end}"
                    );
                }
            }
            for id in [0, 4, 0x7f, 0xff] {
                assert!(validate(&packet(*kind, &[id, 0])).is_err());
            }
        }
    }
}
