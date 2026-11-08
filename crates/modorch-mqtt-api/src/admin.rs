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
