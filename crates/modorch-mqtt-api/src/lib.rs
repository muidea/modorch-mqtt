//! Shared MQTT data types. This experimental release implements QoS 0/1 only.
#![forbid(unsafe_code)]
#![deny(missing_docs)]

use bytes::Bytes;

/// MQTT delivery quality. QoS 2 is represented for explicit capability rejection.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum Qos {
    /// Best-effort delivery without acknowledgement.
    AtMostOnce = 0,
    /// Delivery tracked until PUBACK, allowing duplicates.
    AtLeastOnce = 1,
    /// Represented for capability negotiation; not implemented in this release.
    ExactlyOnce = 2,
}

impl TryFrom<u8> for Qos {
    type Error = MqttError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::AtMostOnce),
            1 => Ok(Self::AtLeastOnce),
            2 => Ok(Self::ExactlyOnce),
            _ => Err(MqttError::Protocol("invalid QoS")),
        }
    }
}

/// Owned application-independent publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MqttMessage {
    /// Validated MQTT Topic Name when admitted by Core.
    pub topic: String,
    /// Shared immutable binary payload.
    pub payload: Bytes,
    /// Original publication QoS.
    pub qos: Qos,
    /// Request to replace/delete the retained value.
    pub retain: bool,
}

/// Observable broker status; counts include disconnected persistent sessions.
#[derive(Clone, Debug, Default)]
pub struct BrokerStatus {
    /// Monotonic factory-local Core generation.
    pub generation: u64,
    /// Whether this broker currently admits connections and protocol mutations.
    pub active: bool,
    /// Managed connection tasks, including handshakes and canceled tasks draining.
    pub connections: usize,
    /// Online and offline sessions combined.
    pub sessions: usize,
    /// Current retained Topic Names.
    pub retained: usize,
    /// Triggered Will records waiting for reliable target capacity.
    pub pending_wills: usize,
    /// Accounted memory including reservations; not an OS RSS measurement.
    pub memory_used_bytes: usize,
    /// Admission failures from connection, task, or memory limits.
    pub rejected_connections: u64,
    /// Normal client publications rejected at the ownership boundary.
    pub rejected_publications: u64,
    /// Per-target QoS0 deliveries dropped due to resource limits.
    pub dropped_qos0: u64,
}

/// Domain errors; client-local failures must not escape as component task failures.
#[derive(Debug, thiserror::Error)]
pub enum MqttError {
    /// Invalid or unimplemented runtime configuration.
    #[error("invalid configuration: {0}")]
    Configuration(String),
    /// Invalid control packet, Topic, or state transition.
    #[error("protocol error: {0}")]
    Protocol(&'static str),
    /// Capability intentionally excluded from the declared subset.
    #[error("unsupported experimental capability: {0}")]
    Unsupported(&'static str),
    /// Bounded admission failed before ownership transfer.
    #[error("capacity exceeded: {0}")]
    Capacity(&'static str),
    /// Generation or connection ownership no longer matches.
    #[error("broker or connection generation is inactive")]
    Closed,
    /// Connection-local transport error.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
