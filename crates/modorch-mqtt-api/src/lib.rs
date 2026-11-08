//! Shared MQTT data types for the experimental MQTT 3.1.1 broker.
#![forbid(unsafe_code)]
#![deny(missing_docs)]

use bytes::Bytes;
use serde::{Deserialize, Serialize};

mod storage;
pub use storage::*;

mod admin;
pub use admin::*;
mod auth;
pub use auth::*;

/// Reserved network Bridge origin User Property. At most one is accepted.
/// Core budgets this separately from the 128 ordinary User Properties.
pub const BRIDGE_ORIGIN_PROPERTY: &str = "modorch.bridge.origin";
/// Reserved network Bridge hop User Property. At most one is accepted.
/// These routing hints convey no authentication or authorization authority.
pub const BRIDGE_HOPS_PROPERTY: &str = "modorch.bridge.hops";

/// MQTT delivery quality.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[repr(u8)]
pub enum Qos {
    /// Best-effort delivery without acknowledgement.
    AtMostOnce = 0,
    /// Delivery tracked until PUBACK, allowing duplicates.
    AtLeastOnce = 1,
    /// Exactly-once MQTT exchange, tracked through PUBREC/PUBREL/PUBCOMP.
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

/// Persistable MQTT5 application properties. Topic aliases stay connection-local.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct MqttProperties {
    /// Absolute expiry in Unix milliseconds; absent means no message expiry.
    pub expires_at_ms: Option<u64>,
    /// Whether the payload is UTF-8 character data.
    pub utf8_payload: bool,
    /// Optional media type, without implicit conversion.
    pub content_type: Option<String>,
    /// MQTT response Topic Name.
    pub response_topic: Option<String>,
    /// Binary request correlation data.
    pub correlation_data: Option<Bytes>,
    /// Ordered user properties: up to 128 ordinary entries (duplicate keys allowed),
    /// plus one each of the reserved Bridge origin/hop properties. Packet and
    /// memory byte limits include every entry, including reserved properties.
    pub user_properties: Vec<(String, String)>,
    /// Server-generated matching Subscription Identifiers.
    pub subscription_identifiers: Vec<u32>,
}
impl MqttProperties {
    /// Whether this message's lifetime has ended.
    pub fn expired(&self) -> bool {
        self.expires_at_ms.is_some_and(|t| t <= unix_time_ms())
    }
    /// Additional logical memory cost, keeping legacy empty-property accounting stable.
    pub fn byte_cost(&self) -> usize {
        usize::from(self.expires_at_ms.is_some()) * 16
            + self.content_type.as_ref().map_or(0, |s| 32 + s.len())
            + self.response_topic.as_ref().map_or(0, |s| 32 + s.len())
            + self.correlation_data.as_ref().map_or(0, |s| 32 + s.len())
            + self
                .user_properties
                .iter()
                .map(|(k, v)| 64 + k.len() + v.len())
                .sum::<usize>()
            + self.subscription_identifiers.len() * 4
    }
}

/// Owned application-independent publication.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MqttMessage {
    /// Validated MQTT Topic Name when admitted by Core.
    pub topic: String,
    /// Shared immutable binary payload.
    pub payload: Bytes,
    /// Original publication QoS.
    pub qos: Qos,
    /// Request to replace/delete the retained value.
    pub retain: bool,
    /// MQTT5 application metadata; empty for MQTT3.1.1 publications.
    #[serde(default)]
    pub properties: MqttProperties,
}

/// A network publication observed after the configured MQTT acceptance boundary. Observation is best effort.
/// Local in-process code can construct DTOs; this is not a cryptographic identity.
#[derive(Clone, Debug)]
pub struct MqttInbound {
    /// Broker component name stamped by Core.
    pub broker: String,
    /// Core generation at acceptance.
    pub generation: u64,
    /// Factory-local connection identity, not a global deduplication key.
    pub connection_id: u64,
    /// ClientID from the accepted connection context.
    pub client_id: String,
    /// Original MQTT Topic Name; also the EventHub event topic.
    pub topic: String,
    /// Shared immutable publication bytes.
    pub payload: Bytes,
    /// Original MQTT QoS.
    pub qos: Qos,
    /// Original retain flag.
    pub retain: bool,
    /// MQTT5 application metadata, with connection-local aliases resolved.
    pub properties: MqttProperties,
    /// Connection-local packet ID, absent at QoS0.
    pub packet_id: Option<u16>,
    /// Unix time in milliseconds at observation creation.
    pub received_at_ms: u64,
}

/// Explicit internal publication; send as a Request from a component Handler.
/// Idempotency is scoped by the framework-generated source component.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MqttPublishRequest {
    /// Stable caller ID, 1..128 UTF-8 bytes without NUL. Never reuse for different content.
    pub request_id: String,
    /// Actual MQTT Topic Name, not the EventHub control topic.
    pub topic: String,
    /// Opaque binary payload; callers declare its cost to the EventHub.
    pub payload: Bytes,
    /// Requested MQTT delivery QoS.
    pub qos: Qos,
    /// Replace/delete the retained value on acceptance.
    pub retain: bool,
    /// MQTT5 application metadata; empty for MQTT3.1.1 publications.
    #[serde(default)]
    pub properties: MqttProperties,
}

/// Storage boundary actually reached by an accepted publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum AcceptanceDurability {
    /// Factory-owned memory only; a process restart loses this promise.
    MemoryOnly,
    /// SQLite FULL synchronous transaction committed before the receipt was returned.
    DurableCommitted,
}

/// Broker acceptance, independent from client delivery or PUBACK completion.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MqttAcceptReceipt {
    /// Caller-supplied stable identity.
    pub request_id: String,
    /// Always true for a successful result; rejections use the result's error variant.
    pub accepted: bool,
    /// Actual ownership boundary reached.
    pub durability: AcceptanceDurability,
}

/// Typed rejection before publication ownership transfer.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error, Serialize, Deserialize)]
pub enum MqttPublishRejection {
    /// Only component-originated Requests addressed directly to Core may publish/query.
    #[error("a component-originated, directly addressed Request is required")]
    UntrustedSource,
    /// Inactive or superseded Core generation.
    #[error("broker generation is inactive")]
    Inactive,
    /// Invalid request ID, Topic or complete MQTT packet size.
    #[error("invalid publication: {0}")]
    Invalid(std::borrow::Cow<'static, str>),
    /// Reserved capability rejection for Providers with a narrower QoS range.
    #[error("requested QoS is unsupported")]
    UnsupportedQos,
    /// The configured authorizer denied or could not confirm the internal publication.
    #[error("internal publication is not authorized")]
    NotAuthorized,
    /// Same source and ID were previously used with different content.
    #[error("request ID content conflict")]
    IdConflict,
    /// New ID cannot enter the bounded, non-evicting ledger.
    #[error("idempotency ledger is full")]
    LedgerFull,
    /// Atomic reliable routing, retained or memory capacity could not be reserved.
    #[error("publication capacity exceeded: {0}")]
    Capacity(std::borrow::Cow<'static, str>),
}

/// Business reply type for the publish control Request. Hub admission is not this result.
pub type MqttPublishResult = Result<MqttAcceptReceipt, MqttPublishRejection>;

/// Look up a publication using the same framework source as the original request.
#[derive(Clone, Debug)]
pub struct MqttPublishQuery {
    /// Original stable request ID.
    pub request_id: String,
}

/// Query reply. None means no ledger entry, not a guarantee about a concurrent queued Request.
pub type MqttPublishQueryResult = Result<Option<MqttPublishResult>, MqttPublishRejection>;

/// Observable broker status; counts include disconnected persistent sessions.
#[derive(Clone, Debug, Default, Serialize)]
pub struct BrokerStatus {
    /// Confirmed durable image commits in this Factory.
    pub storage_commits: u64,
    /// Failed or unresolved durable transaction attempts, including local preparation failures.
    pub storage_commit_failures: u64,
    /// Cumulative successful/failed durable transaction time after gate acquisition, in microseconds.
    pub storage_commit_duration_micros: u64,
    /// Longest durable transaction observed by this Factory, in microseconds.
    pub storage_commit_max_micros: u64,
    /// Total installed session filters, including offline sessions.
    pub subscriptions: usize,
    /// Reliable Pending plus Inflight messages across all sessions.
    pub pending_messages: usize,
    /// Accounted bytes of all reliable backlogs.
    pub pending_bytes: usize,
    /// Currently sent PUBLISH packets awaiting acknowledgement.
    pub inflight: usize,
    /// Best-effort PUBLISH frames queued across active sessions.
    pub queued_qos0: usize,
    /// Successfully committed publications, including Will and internal operations.
    pub accepted_publications: u64,
    /// Accepted network PUBLISH packets; duplicate QoS1 packets count separately.
    pub received_publications: u64,
    /// PUBLISH frames selected for network writing, including retries.
    pub sent_publications: u64,
    /// MQTT plaintext bytes read (TLS framing excluded).
    pub bytes_received: u64,
    /// MQTT plaintext bytes written (TLS framing excluded).
    pub bytes_sent: u64,
    /// Client-local authentication/authorization failures.
    pub auth_rejections: u64,
    /// Handshake, KeepAlive and slow-writer deadline failures.
    pub timeouts: u64,
    /// Specifically slow-writer deadline failures.
    pub slow_writers: u64,
    /// Successful attachments to an existing ClientID.
    pub reconnects: u64,
    /// Number of Wills triggered by connection loss/takeover.
    pub triggered_wills: u64,
    /// Combined armed and triggered Will records.
    pub will_records: usize,
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
    /// Received QoS2 exchanges awaiting PUBREL, separate from outbound backlog.
    pub incoming_qos2: usize,
    /// Logical bytes charged to received QoS2 exchanges.
    pub incoming_qos2_bytes: usize,
    /// Accounted memory including reservations; not an OS RSS measurement.
    pub memory_used_bytes: usize,
    /// TCP/TLS connections rejected by per-IP concurrency, attempt or peer-table bounds.
    pub rate_limited_connections: u64,
    /// Bounded peer IP rate buckets retained within this Factory.
    pub peer_buckets: usize,
    /// Admission failures from connection, task, or memory limits.
    pub rejected_connections: u64,
    /// Normal client publications rejected at the ownership boundary.
    pub rejected_publications: u64,
    /// Number of terminal internal publication results retained without eviction.
    pub publish_records: usize,
    /// Inbound observations with at least one observer admitted (not completed).
    pub inbound_observed: u64,
    /// Inbound observations with no observer admitted, including no match/Hub rejection.
    pub inbound_unobserved: u64,
    /// Observations admitted to some observers while other observers rejected admission.
    pub inbound_partial: u64,
    /// Per-target QoS0 deliveries dropped due to resource limits.
    pub dropped_qos0: u64,
    /// MQTT5 per-target deliveries discarded because they exceed the negotiated packet size.
    pub dropped_oversized: u64,
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
    /// Authentication, authorization or expiry failed; does not disclose credentials.
    #[error("not authorized")]
    NotAuthorized,
    /// A session changed while external authorization was in progress.
    #[error("session authorization snapshot changed")]
    SessionConflict,
    /// Connection-local transport error.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
