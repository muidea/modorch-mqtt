//! Bounded internal administration contract. Network MQTT clients cannot call it.
use crate::{BrokerStatus, PrincipalKey, Qos};
/// Core endpoint, protected by a configured framework-source allowlist.
pub const ADMIN_CONTROL_TOPIC: &str = "$modorch/mqtt/control/admin/v1";
/// Optional Admin Module endpoint, separately authorized before forwarding to Core.
pub const ADMIN_TOPIC: &str = "$modorch/mqtt/admin/v1";
/// Bounded administrative operations. Pagination is a live view, not a stable snapshot.
#[derive(Clone, Debug)]
pub enum AdminRequest {
    /// List up to 256 sessions, ordered by ClientID; cursor is exclusive.
    ListClients {
        /// Last ClientID from the preceding page.
        after: Option<String>,
        /// Requested page length, 1..256.
        limit: usize,
    },
    /// Inspect one session without returning publication payloads.
    GetSession {
        /// Exact ClientID.
        client_id: String,
    },
    /// Cancel an active connection; normal abrupt-disconnect Will semantics apply.
    DisconnectClient {
        /// Exact ClientID.
        client_id: String,
        /// Connection token observed by GetSession; retries cannot cancel a newer connection.
        expected_connection_id: u64,
    },
    /// List up to 256 installed filters of one session, ordered lexically.
    ListSubscriptions {
        /// Exact ClientID.
        client_id: String,
        /// Exclusive filter cursor.
        after: Option<String>,
        /// Requested page length, 1..256.
        limit: usize,
    },
    /// Read aggregate bounded counters without ClientID/Topic metric labels.
    BrokerStats,
    /// Read up to 256 recent connection, takeover, disconnect and rejection events, newest first.
    ListEvents {
        /// Requested event count, 1..=256.
        limit: usize,
    },
    /// List up to 256 subscribed Topic Filters aggregated across sessions.
    ListTopicFilters {
        /// Exclusive filter cursor.
        after: Option<String>,
        /// Requested page length, 1..=256.
        limit: usize,
    },
    /// Simulate topic routing against installed session subscriptions without publishing.
    MatchTopic {
        /// Exact MQTT Topic Name to evaluate.
        topic: String,
    },
    /// List up to 256 retained topics, optionally filtered by prefix.
    ListRetained {
        /// Optional Topic Name prefix filter.
        prefix: Option<String>,
        /// Exclusive topic cursor.
        after: Option<String>,
        /// Requested page length, 1..=256.
        limit: usize,
    },
    /// Inspect one retained topic including a bounded payload preview.
    GetRetained {
        /// Exact retained Topic Name.
        topic: String,
    },
    /// Remove a retained message from broker memory.
    DeleteRetained {
        /// Exact retained Topic Name.
        topic: String,
    },
    /// List up to 256 recently active published topics from the bounded in-memory directory.
    ListActiveTopics {
        /// Optional substring search on topic name.
        search: Option<String>,
        /// Requested item count, 1..=256.
        limit: usize,
    },
    /// Publish an administrative test message through broker routing.
    AdminPublish {
        /// Exact MQTT Topic Name.
        topic: String,
        /// Message payload bytes.
        payload: Vec<u8>,
        /// Delivery QoS.
        qos: Qos,
        /// Whether to store as retained message.
        retain: bool,
    },
}
/// Session metadata. Credentials and message payloads never appear here.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct SessionInfo {
    /// Exact client identifier.
    pub client_id: String,
    /// Stable session owner.
    pub owner: PrincipalKey,
    /// Whether the session disappears when disconnected.
    pub clean_session: bool,
    /// Whether a connection currently owns it.
    pub online: bool,
    /// Stable session incarnation token.
    pub session_epoch: u64,
    /// Current connection token; use it to fence administrative disconnects.
    pub connection_id: Option<u64>,
    /// Remote peer socket address of the current active connection, if connected.
    pub peer_addr: Option<String>,
    /// Protocol level (4 for MQTT 3.1.1, 5 for MQTT 5.0).
    pub protocol_level: u8,
    /// Negotiated keep-alive interval in seconds (0 disables keep-alive).
    pub keep_alive: u16,
    /// UTC Unix milliseconds when the current active connection attached, if connected.
    pub connected_at_ms: Option<u64>,
    /// Installed filter count.
    pub subscriptions: usize,
    /// Reliable Pending plus Inflight record count.
    pub pending_messages: usize,
    /// Accounted reliable backlog bytes.
    pub pending_bytes: usize,
    /// PUBLISH packets currently sent awaiting acknowledgement.
    pub inflight: usize,
}
/// One installed filter and granted quality of service.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct SubscriptionInfo {
    /// Exact MQTT Topic Filter.
    pub filter: String,
    /// Maximum requested delivery quality.
    pub qos: Qos,
}
/// Bounded recent connection/session diagnostic event, newest first.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ConnectionEvent {
    /// Monotonic broker-local sequence number.
    pub seq: u64,
    /// UTC Unix milliseconds when the event occurred.
    pub timestamp_ms: u64,
    /// Fixed event classification (connected, disconnected, taken_over, kicked, rejected, timeout, error).
    pub kind: String,
    /// Associated ClientID, if known.
    pub client_id: Option<String>,
    /// Remote peer address, if known.
    pub peer_addr: Option<String>,
    /// Connection token associated with the event, if any.
    pub connection_id: Option<u64>,
    /// Bounded diagnostic detail without credentials or message payloads.
    pub detail: String,
}
/// One client subscribing to a specific Topic Filter.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct TopicSubscriberInfo {
    /// Subscribing ClientID.
    pub client_id: String,
    /// Whether this session currently owns an active connection.
    pub online: bool,
    /// Granted QoS for this filter.
    pub qos: Qos,
}
/// Aggregated subscription statistics for one Topic Filter.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct TopicFilterSummary {
    /// Exact MQTT Topic Filter.
    pub filter: String,
    /// Total subscribing sessions.
    pub subscriber_count: usize,
    /// Currently connected subscribing sessions.
    pub online_count: usize,
    /// Highest QoS among subscribers of this filter.
    pub max_qos: Qos,
    /// Subscriber details (bounded per filter summary).
    pub subscribers: Vec<TopicSubscriberInfo>,
}
/// One session matched by a simulated Topic Name lookup.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct TopicMatchEntry {
    /// Matched ClientID.
    pub client_id: String,
    /// Whether this session is currently online.
    pub online: bool,
    /// Filters installed on this session that match the topic.
    pub matched_filters: Vec<String>,
    /// Highest granted subscription QoS across matching filters on this session.
    pub granted_max_qos: Qos,
}
/// Routing simulation result for one Topic Name.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct TopicMatchResult {
    /// Evaluated Topic Name.
    pub topic: String,
    /// Sessions matched by the topic.
    pub matches: Vec<TopicMatchEntry>,
}
/// Retained message metadata and bounded payload preview.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct RetainedInfo {
    /// Exact retained Topic Name.
    pub topic: String,
    /// Stored message QoS.
    pub qos: Qos,
    /// Original payload size in bytes.
    pub payload_bytes: usize,
    /// Publishing origin description (`client:<id>`, `internal`, or `legacy`).
    pub origin: String,
    /// Bounded UTF-8 preview when requested, truncated to safe display length.
    pub payload_preview: Option<String>,
    /// Whether the preview was truncated from the full payload.
    pub payload_truncated: bool,
}
/// Bounded statistics for one recently published active topic.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct ActiveTopicInfo {
    /// Exact MQTT Topic Name.
    pub topic: String,
    /// Total accepted publications on this topic while tracked.
    pub publish_count: u64,
    /// Most recent publication QoS.
    pub last_qos: Qos,
    /// ClientID of the most recent publisher, if client-originated.
    pub last_publisher: Option<String>,
    /// UTC Unix milliseconds of the most recent publication.
    pub last_published_ms: u64,
    /// Current count of session subscriptions matching this topic.
    pub matching_subscriptions: usize,
}
/// Bounded response variants matching the requested operation.
#[derive(Clone, Debug)]
pub enum AdminReply {
    /// Session page and next exclusive cursor, absent at the end.
    Clients {
        /// Page contents.
        items: Vec<SessionInfo>,
        /// Next cursor.
        next: Option<String>,
    },
    /// One session, absent if it does not exist.
    Session(Option<SessionInfo>),
    /// Whether an active connection was found and canceled.
    Disconnected(bool),
    /// Filter page and next exclusive cursor.
    Subscriptions {
        /// Page contents.
        items: Vec<SubscriptionInfo>,
        /// Next cursor.
        next: Option<String>,
    },
    /// Aggregate broker status and counters.
    Stats(BrokerStatus),
    /// Recent connection/session diagnostic events, newest first.
    Events(Vec<ConnectionEvent>),
    /// Aggregated Topic Filter summaries and next cursor.
    TopicFilters {
        /// Page contents.
        items: Vec<TopicFilterSummary>,
        /// Next cursor.
        next: Option<String>,
    },
    /// Simulated routing result for one Topic Name.
    TopicMatch(TopicMatchResult),
    /// Retained message page and next cursor.
    RetainedList {
        /// Page contents.
        items: Vec<RetainedInfo>,
        /// Next cursor.
        next: Option<String>,
    },
    /// Detailed retained message view, absent if not present.
    RetainedDetail(Option<RetainedInfo>),
    /// Whether a retained topic existed and was removed.
    RetainedDeleted(bool),
    /// Recently active published topics, newest first.
    ActiveTopics(Vec<ActiveTopicInfo>),
    /// Result of an administrative test publication.
    Published {
        /// Number of target sessions matched by routing.
        matched_sessions: usize,
    },
}
/// Sanitized administration failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AdminError {
    /// No trusted, allowlisted Handler source or wrong destination/message kind.
    #[error("administration not authorized")]
    NotAuthorized,
    /// Invalid ClientID, cursor or bounded page request.
    #[error("invalid administration request")]
    InvalidRequest,
    /// Core is inactive or the operation belongs to an old generation.
    #[error("broker unavailable")]
    Unavailable,
}
/// Admin business result, separate from EventHub admission and execution status.
pub type AdminResult = Result<AdminReply, AdminError>;
