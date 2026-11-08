//! Versioned typed contracts between Core and replaceable authorization Modules.
use crate::Qos;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Internal Auth request topic; external MQTT clients cannot reach this namespace.
pub const AUTH_TOPIC: &str = "$modorch/mqtt/auth/v1";

/// Stable identity; authentication policy revisions never change session ownership.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PrincipalKey {
    /// Authentication realm. Anonymous broker identities use a separate reserved realm.
    pub domain: String,
    /// Stable account identity within that realm.
    pub subject: String,
}
/// A bounded authenticated connection capability, never persisted as a password.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Principal {
    /// Session ownership identity.
    pub key: PrincipalKey,
    /// Authorization policy revision returned by the provider.
    pub policy_version: u64,
    /// Unix millisecond expiry; Core closes expired connections.
    pub expires_at_ms: u64,
}
impl Principal {
    /// Whether bounded identity fields and the current expiry are valid.
    pub fn is_valid(&self) -> bool {
        [&self.key.domain, &self.key.subject]
            .into_iter()
            .all(|s| !s.is_empty() && s.len() <= 256 && !s.contains('\0'))
            && self.expires_at_ms > unix_time_ms()
    }
}
/// Current UTC Unix time, saturating on unsupported clocks.
pub fn unix_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

/// Password bytes deliberately excluded from Debug output.
#[derive(Clone)]
pub struct Authenticate {
    /// ClientID selected before authentication, including broker assignment.
    pub client_id: String,
    /// Optional MQTT UTF-8 username.
    pub username: Option<String>,
    /// Optional MQTT binary password; never written to a session or observation.
    pub password: Option<Bytes>,
    /// Peer socket IP for host-defined rate/identity policy.
    pub peer_ip: std::net::IpAddr,
}
impl std::fmt::Debug for Authenticate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Authenticate")
            .field("client_id", &self.client_id)
            .field("credentials", &"[REDACTED]")
            .finish()
    }
}
/// Session snapshot authorized without mutating the old connection or its state.
#[derive(Clone, Debug)]
pub struct AuthorizeSessionAttach {
    /// Newly authenticated connection capability.
    pub principal: Principal,
    /// Requested ClientID.
    pub client_id: String,
    /// Existing session owner; Core independently enforces equality.
    pub existing_owner: Option<PrincipalKey>,
    /// CAS revision covering owner, subscriptions and connection transitions.
    pub expected_version: Option<u64>,
    /// Whether the request would replace existing persistent state.
    pub clean_session: bool,
    /// Whether an active connection would be taken over.
    pub takeover: bool,
}
/// Publication context. Will is authorized before successful CONNECT.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PublishKind {
    /// Normal network PUBLISH.
    ClientPublish,
    /// Will armed by CONNECT.
    Will,
    /// Explicit business Handler publication.
    InternalPublish,
}
/// Parameters bound by the returned grant, independently from payload contents.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublishAuthContext {
    /// Identity from a connection, absent only for internal Handler publication.
    pub principal: Option<Principal>,
    /// Trusted framework component identity encoded by Core for internal requests.
    pub internal_source: Option<String>,
    /// Actual MQTT topic.
    pub topic: String,
    /// Requested quality of service.
    pub qos: Qos,
    /// Requested retained mutation.
    pub retain: bool,
    /// Normal, Will or internal operation.
    pub kind: PublishKind,
}
/// An explicit permit bound to the checked parameters and policy version.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublishGrant {
    /// Original parameters; Core checks equality before using this result.
    pub context: PublishAuthContext,
    /// Policy revision that issued the grant.
    pub policy_version: u64,
}
/// A subscription (including recovered subscriptions) needing current authorization.
#[derive(Clone, Debug)]
pub struct AuthorizeSubscribe {
    /// Authenticated connection identity.
    pub principal: Principal,
    /// Complete MQTT Topic Filter; provider must check language containment.
    pub filter: String,
}
/// Operations served by the independent authorization Module.
#[derive(Clone, Debug)]
pub enum AuthRequest {
    /// Validate credentials, returning a stable principal.
    Authenticate(Authenticate),
    /// Check ClientID usage and session attachment policy.
    SessionAttach(AuthorizeSessionAttach),
    /// Authorize one normal/Will/internal publication.
    Publish(PublishAuthContext),
    /// Authorize a new or restored subscription.
    Subscribe(AuthorizeSubscribe),
}
/// Successful responses; receiving a different variant is a fail-closed error.
#[derive(Clone, Debug)]
pub enum AuthDecision {
    /// Authenticated identity and expiry.
    Principal(Principal),
    /// Session attachment or subscription permitted.
    Allowed,
    /// Parameter-bound publication permit.
    Publish(PublishGrant),
}
/// Sanitized denial; providers must not include credentials in error messages.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AuthError {
    /// Authentication, policy, expiry or capability binding failed.
    #[error("not authorized")]
    Denied,
    /// Provider cannot currently establish a decision.
    #[error("authorization temporarily unavailable")]
    Unavailable,
}
/// Auth Request business result, distinct from EventHub admission or execution errors.
pub type AuthResult = Result<AuthDecision, AuthError>;
