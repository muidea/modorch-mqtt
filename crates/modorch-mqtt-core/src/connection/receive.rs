//! MQTT5 receive quota belongs to a network connection, not a durable Session.
use super::*;
use std::collections::BTreeMap;

#[derive(Clone, Copy)]
pub(super) enum Receipt {
    Qos1,
    Qos2 { id: u16, epoch: u64 },
}

pub(super) struct Frame {
    pub bytes: Vec<u8>,
    pub receipt: Option<Receipt>,
}

impl From<Vec<u8>> for Frame {
    fn from(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            receipt: None,
        }
    }
}

impl Frame {
    pub fn releasing(bytes: Vec<u8>, receipt: Option<Receipt>) -> Self {
        Self { bytes, receipt }
    }
}

#[derive(Default)]
pub(super) struct Window {
    qos1: usize,
    qos2: BTreeMap<u16, (u64, Charge)>,
    epoch: u64,
}

impl Window {
    pub fn admit(
        &mut self,
        qos: Qos,
        id: u16,
        limit: usize,
        duplicate: bool,
        budget: &Arc<crate::budget::Budget>,
    ) -> Result<bool, MqttError> {
        if qos == Qos::AtMostOnce {
            return Ok(true);
        }
        if qos == Qos::ExactlyOnce && self.qos2.contains_key(&id) {
            // PUBREL removed durable receiver ownership, but the peer cannot
            // reuse the ID until its PUBCOMP has actually left this connection.
            return if duplicate {
                Ok(true)
            } else {
                Err(MqttError::Protocol("QoS2 Packet Identifier still in use"))
            };
        }
        if self.qos1 + self.qos2.len() >= limit.min(u16::MAX as usize) {
            return Ok(false);
        }
        if qos == Qos::ExactlyOnce {
            // Charge map nodes as well as payload ownership held by State.
            let charge = budget.reserve(128)?;
            self.epoch = self
                .epoch
                .checked_add(1)
                .ok_or(MqttError::Capacity("receive epoch"))?;
            self.qos2.insert(id, (self.epoch, charge));
        } else {
            self.qos1 += 1;
        }
        Ok(true)
    }

    pub fn completion(&self, id: u16) -> Option<Receipt> {
        self.qos2
            .get(&id)
            .map(|(epoch, _)| Receipt::Qos2 { id, epoch: *epoch })
    }

    pub fn written(&mut self, receipt: Option<Receipt>) {
        match receipt {
            Some(Receipt::Qos1) => self.qos1 -= 1,
            Some(Receipt::Qos2 { id, epoch }) => {
                // A queued duplicate PUBCOMP cannot release a newer exchange
                // that reused this identifier after the first PUBCOMP finished.
                if self
                    .qos2
                    .get(&id)
                    .is_some_and(|(current, _)| *current == epoch)
                {
                    self.qos2.remove(&id);
                }
            }
            None => (),
        }
    }
}
