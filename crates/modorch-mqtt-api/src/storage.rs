//! Versioned storage protocol. Provider state is private; Core snapshots are opaque bytes.
use bytes::Bytes;
/// Directed Request endpoint of an explicitly selected Storage Module.
pub const STORAGE_TOPIC: &str = "$modorch/mqtt/storage/v1";
/// One fenced storage operation, authorized by the host-owned Store capability.
#[derive(Clone, Debug)]
pub enum StoreRequest {
    /// Establish a new writer epoch and freeze the latest committed image.
    Begin {
        /// Stable identity for retries of this recovery attempt.
        recovery_id: String,
    },
    /// Read one bounded byte page from the frozen image.
    Page {
        /// Fenced recovery epoch.
        epoch: u64,
        /// Exact byte offset, beginning with zero.
        offset: usize,
        /// Maximum page payload bytes.
        max_bytes: usize,
    },
    /// Release the recovery barrier; retrying the same epoch is idempotent.
    Complete {
        /// Current recovery epoch.
        epoch: u64,
    },
    /// Keep writes closed after failed recovery. Does not delete committed data.
    Abort {
        /// Recovery epoch, never re-enables an older writer.
        epoch: u64,
    },
    /// Atomically replace the complete image and one current-operation receipt.
    Commit {
        /// Current writer epoch.
        epoch: u64,
        /// CAS revision of the previous committed image.
        expected_version: u64,
        /// Stable epoch/sequence identity; never an EventHub operation ID.
        operation_id: String,
        /// SHA256 of the complete serialized image.
        digest: [u8; 32],
        /// Bounded, versioned Core snapshot.
        image: Bytes,
    },
    /// Resolve uncertainty before submitting any later operation.
    Query {
        /// Current writer epoch.
        epoch: u64,
        /// Original stable operation identity.
        operation_id: String,
        /// Original content digest; mismatch is a conflict.
        digest: [u8; 32],
    },
}
/// Bounded recovery metadata and terminal commit results.
#[derive(Clone, Debug)]
pub enum StoreReply {
    /// Fixed image descriptor; every page belongs to this epoch/version/digest.
    Recovery {
        /// New writer epoch.
        epoch: u64,
        /// Latest committed revision.
        version: u64,
        /// Exact image length; zero denotes a fresh database.
        bytes: usize,
        /// SHA256 of the image (including the empty fresh image).
        digest: [u8; 32],
    },
    /// A single image page with an explicit end marker.
    Page {
        /// Fenced recovery epoch.
        epoch: u64,
        /// Image revision.
        version: u64,
        /// Requested offset.
        offset: usize,
        /// Page contents.
        data: Bytes,
        /// Whether this page ends the image exactly.
        done: bool,
    },
    /// Recovery barrier operation completed.
    Ready,
    /// Transaction durably committed under FULL synchronous SQLite policy.
    Committed {
        /// Revision after the commit.
        version: u64,
    },
    /// Current operation was not found. Older operations cannot bypass CAS.
    Unknown,
}
/// Sanitized storage failures; no SQL, path, message or credential contents.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StoreError {
    /// Wrong capability, destination or message kind.
    #[error("storage access denied")]
    NotAuthorized,
    /// Invalid bounded protocol request.
    #[error("invalid storage request")]
    Invalid,
    /// Old writer/recovery epoch, or writes remain closed.
    #[error("storage writer fenced")]
    Fenced,
    /// CAS or idempotency content mismatch.
    #[error("storage version/content conflict")]
    Conflict,
    /// Image or physical SQLite quota exhausted.
    #[error("storage capacity exceeded")]
    Capacity,
    /// Corrupt persisted metadata or image checksum.
    #[error("storage integrity failure")]
    Corrupt,
    /// I/O, executor or database currently unavailable; caller must resolve uncertainty.
    #[error("storage unavailable")]
    Unavailable,
}
/// Business result; EventHub transport errors remain a separate uncertainty layer.
pub type StoreResult = Result<StoreReply, StoreError>;
/// Capability for lifecycle/network roots, whose framework source is intentionally absent.
/// Generate fresh random bytes in the host, share only with Core and the selected Provider.
#[derive(Clone, Eq, PartialEq)]
pub struct StoreAccessKey(pub [u8; 32]);
impl std::fmt::Debug for StoreAccessKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StoreAccessKey([REDACTED])")
    }
}
/// Private application capability plus the typed request. Never expose this on MQTT/HTTP.
#[derive(Clone, Debug)]
pub struct StoreEnvelope {
    /// Host-owned access capability, including for source-less lifecycle roots.
    pub access: StoreAccessKey,
    /// Requested storage operation.
    pub request: StoreRequest,
}

/// Sanitized distinction between Store admission, execution uncertainty and reply errors.
/// A caller timeout is never proof that a running durable operation rolled back.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoreCallFailure {
    /// The Hub rejected the request before returning a response handle.
    Admission,
    /// Queued work was canceled without starting its handler.
    CanceledBeforeStart,
    /// A running handler did not return in time; its durable outcome is unknown.
    Indeterminate,
    /// The Hub could not supply the typed response for another execution reason.
    Transport,
    /// The whole paginated recovery exceeded its deadline; ingress stays closed.
    RecoveryDeadline,
    /// The Provider returned a sanitized business error.
    Provider(StoreError),
}
