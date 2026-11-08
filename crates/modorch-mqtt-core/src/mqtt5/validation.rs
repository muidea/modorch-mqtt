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
                r.byte()?;
                if !r.0.is_empty() {
                    r.props(kind)?;
                }
            }
        }
        12 => {}
        14 => {
            if !r.0.is_empty() {
                r.byte()?;
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
