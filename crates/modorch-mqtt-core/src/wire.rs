use crate::codec::{self, Packet};
use modorch_mqtt_api::MqttError;
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub(crate) struct ConnectOptions {
    pub session_expiry: u32,
    pub will_delay: u32,
    pub will_expiry: Option<u32>,
    pub receive_maximum: u16,
    pub maximum_packet_size: u32,
}
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub(crate) struct SubscribeOptions {
    pub no_local: bool,
    pub retain_as_published: bool,
    pub retain_handling: u8,
    pub identifier: Option<u32>,
}
pub(crate) struct Protocol {
    enabled: bool,
    #[cfg(feature = "mqtt5")]
    pub v5: Option<crate::mqtt5::Codec>,
}
impl Protocol {
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled,
            #[cfg(feature = "mqtt5")]
            v5: None,
        }
    }
    pub fn frame_len(&self, input: &[u8], max: usize) -> Result<Option<usize>, MqttError> {
        #[cfg(feature = "mqtt5")]
        if self.v5.is_some() {
            return codec::frame_len_version(input, max, true);
        }
        codec::frame_len(input, max)
    }
    pub fn decode(&mut self, input: &[u8], max_filters: usize) -> Result<Packet, MqttError> {
        #[cfg(feature = "mqtt5")]
        {
            let mut at = 1;
            while input.get(at).is_some_and(|b| b & 128 != 0) {
                at += 1;
            }
            at += 1;
            if self.v5.is_none()
                && self.enabled
                && input.first() == Some(&0x10)
                && input.get(at..at + 7) == Some(&[0, 4, b'M', b'Q', b'T', b'T', 5])
            {
                self.v5 = Some(crate::mqtt5::Codec::default());
            }
            if let Some(v5) = &mut self.v5 {
                return v5.decode(input, max_filters);
            }
        }
        let _ = self.enabled;
        codec::decode(input, max_filters)
    }
    pub fn response(
        &self,
        frame: Vec<u8>,
        client: Option<&str>,
        max_packet: usize,
        max_receive: usize,
    ) -> Vec<u8> {
        #[cfg(feature = "mqtt5")]
        if let Some(v5) = &self.v5 {
            return v5.response(frame, client, max_packet, max_receive);
        }
        let _ = (client, max_packet, max_receive);
        frame
    }
    pub fn subscribe_options(&mut self) -> Vec<SubscribeOptions> {
        #[cfg(feature = "mqtt5")]
        if let Some(v5) = &mut self.v5 {
            return std::mem::take(&mut v5.subscriptions);
        }
        vec![]
    }
    pub fn is_v5(&self) -> bool {
        #[cfg(feature = "mqtt5")]
        {
            self.v5.is_some()
        }
        #[cfg(not(feature = "mqtt5"))]
        {
            false
        }
    }
    pub fn max_packet(&self) -> usize {
        #[cfg(feature = "mqtt5")]
        if let Some(v) = &self.v5 {
            return v.maximum_packet;
        }
        usize::MAX
    }
    pub fn disconnect_will(&self) -> bool {
        #[cfg(feature = "mqtt5")]
        if let Some(v) = &self.v5 {
            return v.disconnect_will;
        }
        false
    }
    pub fn negative_rec(&mut self, id: u16) -> bool {
        #[cfg(feature = "mqtt5")]
        if let Some(v) = &mut self.v5 {
            return v.negative_rec.take() == Some(id);
        }
        let _ = id;
        false
    }
    pub fn disconnect_expiry(&self) -> Option<u32> {
        #[cfg(feature = "mqtt5")]
        if let Some(v5) = &self.v5 {
            return v5.disconnect_expiry;
        }
        None
    }
}
