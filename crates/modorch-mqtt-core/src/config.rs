use std::{net::SocketAddr, time::Duration};

use modorch_mqtt_api::MqttError;

/// A separately registered authorization Module used by directed EventHub Requests.
#[derive(Clone, Debug)]
pub struct AuthProviderConfig {
    /// Explicitly enabled provider Module ID; Core declares a required dependency.
    pub component: String,
    /// Maximum time to establish each decision; timeout always denies.
    pub timeout: Duration,
}

/// PEM file configuration loaded through the host's bounded blocking-I/O executor.
#[derive(Clone, Debug)]
pub struct TlsConfig {
    /// PEM certificate chain, at most 1 MiB.
    pub certificate_chain: std::path::PathBuf,
    /// PEM private key, at most 1 MiB. Never included in errors or status.
    pub private_key: std::path::PathBuf,
}

/// MQTT over WebSocket on this listener, optionally wrapped in TLS.
#[derive(Clone, Debug)]
pub struct WebSocketConfig {
    /// Exact HTTP upgrade path, without a query string; default `/mqtt`.
    pub path: String,
}
impl Default for WebSocketConfig {
    fn default() -> Self {
        Self {
            path: "/mqtt".into(),
        }
    }
}

/// Optional Core admin endpoint. The forwarding Admin Module is selected independently.
#[derive(Clone, Debug)]
pub struct AdminControlConfig {
    /// Application-global subscription ID for the additional endpoint.
    pub subscription_id: u64,
    /// Exact trusted Handler sources permitted to use this endpoint.
    pub allowed_sources: Vec<modorch::ComponentRef>,
}

/// Explicit persistent Provider and bounded snapshot/recovery policy.
#[derive(Clone, Debug)]
pub struct StorageProviderConfig {
    /// Selected Provider Module, required by Core's descriptor.
    pub component: String,
    /// Host-owned secret capability shared only with that Provider.
    pub access: modorch_mqtt_api::StoreAccessKey,
    /// Maximum serialized state image, 1KiB..64MiB.
    pub max_snapshot_bytes: usize,
    /// Bounded recovery byte page, 1..1MiB, must fit host response budgets.
    pub recovery_page_bytes: usize,
    /// Individual Request wait; uncertainty is queried before a later operation.
    pub timeout: Duration,
    /// Absolute deadline for the entire pre-listen recovery.
    pub recovery_timeout: Duration,
}

/// Bounds for the experimental MQTT 3.1.1 broker.
/// Auth, TLS, Storage, MQTT5, WebSocket and admin control are explicitly enabled.
#[derive(Clone, Debug)]
pub struct MqttBrokerConfig {
    /// Modorch component identity; unique within the host Application.
    pub name: String,
    /// Listener address; non-loopback requires Auth and TLS. Port 0 is ephemeral.
    pub listener: SocketAddr,
    /// Optional, source-protected administrative control endpoint.
    pub admin: Option<AdminControlConfig>,
    /// Optional durable Provider. Registration/selection remain explicit.
    pub storage: Option<StorageProviderConfig>,
    /// TLS listener configuration. Requires the `tls` Cargo feature.
    pub tls: Option<TlsConfig>,
    /// Use binary MQTT WebSocket messages with the required `mqtt` subprotocol.
    pub websocket: Option<WebSocketConfig>,
    /// Accept MQTT5 in addition to MQTT3.1.1; requires the mqtt5 feature.
    pub mqtt5: bool,
    /// Optional replaceable Auth provider. Cannot be combined with anonymous mode.
    pub auth: Option<AuthProviderConfig>,
    /// Required explicit acknowledgement of anonymous development access.
    pub anonymous_access: bool,
    /// Application-global subscription IDs for status, publish and publish/query.
    /// The host must reserve these IDs uniquely, including across broker instances.
    pub control_subscription_ids: [u64; 3],
    /// Bound on managed socket tasks, including incomplete CONNECT handshakes.
    pub max_connections: usize,
    /// Simultaneously admitted TCP/TLS handshakes and clients per peer IP.
    pub max_connections_per_ip: usize,
    /// Maximum socket admissions per peer IP within one fixed window.
    pub max_connection_attempts_per_ip: usize,
    /// Peer attempt window; expired inactive buckets are reclaimed.
    pub connection_attempt_window: Duration,
    /// Maximum peer rate buckets, including inactive unexpired peers.
    pub max_tracked_peer_ips: usize,
    /// Combined online/offline Session bound.
    pub max_sessions: usize,
    /// Maximum complete control packet size, including fixed header.
    pub max_packet_bytes: usize,
    /// ClientID UTF-8 byte bound, 23..=65535 (MQTT-3.1.3-5).
    pub max_client_id_bytes: usize,
    /// Deadline from TCP admission to CONNECT acceptance.
    pub handshake_timeout: Duration,
    /// Absolute deadline to finish writing one packet.
    pub write_timeout: Duration,
    /// Maximum installed filters per Session and filters per control packet.
    pub max_subscriptions_per_client: usize,
    /// Separate bounded QoS0 and control reply queue lengths per connection.
    pub per_client_outbound_queue: usize,
    /// Maximum outstanding reliable PUBLISH packets on a connection.
    pub max_inflight_per_client: usize,
    /// Combined reliable Pending + Inflight limit, both online and offline.
    pub max_offline_messages_per_client: usize,
    /// Accounted bytes for reliable Pending + Inflight per Session.
    pub max_pending_bytes_per_client: usize,
    /// Maximum received QoS2 exchanges awaiting PUBREL per Session, 1..65535.
    pub max_inbound_qos2_per_client: usize,
    /// Logical bytes retained for received QoS2 exchanges per Session.
    pub max_inbound_qos2_bytes_per_client: usize,
    /// Maximum retained Topic Names.
    pub max_retained_topics: usize,
    /// Combined armed and triggered Will record count.
    pub max_will_records: usize,
    /// Terminal internal publication ledger bound. No eviction within a Factory lifetime.
    pub max_publish_records: usize,
    /// Global accounted data and reservation budget; excludes allocator/RSS overhead.
    pub memory_budget_bytes: usize,
}

impl Default for MqttBrokerConfig {
    fn default() -> Self {
        Self {
            name: "mqtt-broker".into(),
            listener: "127.0.0.1:1883".parse().unwrap(),
            anonymous_access: false,
            auth: None,
            tls: None,
            websocket: None,
            mqtt5: false,
            admin: None,
            storage: None,
            control_subscription_ids: [1, 2, 3],
            max_connections: 500,
            max_connections_per_ip: 32,
            max_connection_attempts_per_ip: 100,
            connection_attempt_window: Duration::from_secs(60),
            max_tracked_peer_ips: 1024,
            max_sessions: 2000,
            max_packet_bytes: 1024 * 1024,
            max_client_id_bytes: 128,
            handshake_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(10),
            max_subscriptions_per_client: 256,
            per_client_outbound_queue: 128,
            max_inflight_per_client: 16,
            max_offline_messages_per_client: 2048,
            max_pending_bytes_per_client: 16 * 1024 * 1024,
            max_inbound_qos2_per_client: 128,
            max_inbound_qos2_bytes_per_client: 16 * 1024 * 1024,
            max_retained_topics: 10_000,
            max_will_records: 500,
            max_publish_records: 10_000,
            memory_budget_bytes: 256 * 1024 * 1024,
        }
    }
}

impl MqttBrokerConfig {
    /// Explicitly enable anonymous development access on loopback.
    pub fn loopback_development() -> Self {
        Self {
            anonymous_access: true,
            ..Self::default()
        }
    }

    /// Validate implemented capabilities and overflow-safe resource bounds.
    pub fn validate(&self) -> Result<(), MqttError> {
        let invalid = |message: &str| MqttError::Configuration(message.into());
        if self.mqtt5 && !cfg!(feature = "mqtt5") {
            return Err(invalid("MQTT5 requires the mqtt5 Cargo feature"));
        }
        if self.name.is_empty() || self.name.len() > 128 || self.name.contains(['/', '\0']) {
            return Err(invalid("broker name must be 1..128 bytes without / or NUL"));
        }
        let [status, publish, query] = self.control_subscription_ids;
        if status == publish || status == query || publish == query {
            return Err(invalid("control subscription IDs must be distinct"));
        }
        if let Some(ws) = &self.websocket {
            if !cfg!(feature = "websocket")
                || !ws.path.starts_with('/')
                || ws.path.len() > 1024
                || ws
                    .path
                    .bytes()
                    .any(|b| b <= 32 || b >= 127 || b == b'?' || b == b'#')
            {
                return Err(invalid(
                    "invalid WebSocket path or disabled websocket feature",
                ));
            }
        }
        if self.tls.is_some() && !cfg!(feature = "tls") {
            return Err(invalid("TLS configuration requires the tls Cargo feature"));
        }
        if let Some(store) = &self.storage {
            if store.component.is_empty()
                || store.component.len() > 128
                || store.component.contains(['/', '\0'])
                || store.component == self.name
                || store.access.0 == [0; 32]
                || self
                    .auth
                    .as_ref()
                    .is_some_and(|a| a.component == store.component)
                || !(1024..=64 * 1024 * 1024).contains(&store.max_snapshot_bytes)
                || !(1..=1024 * 1024).contains(&store.recovery_page_bytes)
                || store.recovery_page_bytes > store.max_snapshot_bytes
                || store
                    .max_snapshot_bytes
                    .checked_mul(4)
                    .is_none_or(|n| n >= self.memory_budget_bytes)
                || store.timeout.is_zero()
                || store.timeout > Duration::from_secs(60)
                || store.recovery_timeout.is_zero()
                || store.recovery_timeout > Duration::from_secs(300)
            {
                return Err(invalid(
                    "invalid Storage Provider, snapshot quota or recovery policy",
                ));
            }
        }
        if let Some(admin) = &self.admin {
            if self
                .control_subscription_ids
                .contains(&admin.subscription_id)
                || admin.allowed_sources.len() > 256
                || admin
                    .allowed_sources
                    .iter()
                    .any(|s| s.id.is_empty() || s.id.len() > 128 || s.id.contains('\0'))
            {
                return Err(invalid(
                    "invalid admin endpoint subscription ID or source bounds",
                ));
            }
        }
        if !self.listener.ip().is_loopback()
            && (self.auth.is_none() || self.tls.is_none() || self.anonymous_access)
        {
            return Err(invalid("non-loopback listeners require both Auth and TLS"));
        }
        if !self.anonymous_access && self.auth.is_none() {
            return Err(invalid(
                "explicit anonymous development mode or an Auth provider is required",
            ));
        }
        if let Some(auth) = &self.auth {
            if self.anonymous_access
                || auth.component.is_empty()
                || auth.component.len() > 128
                || auth.component == self.name
                || auth.component.contains(['/', '\0'])
                || auth.timeout.is_zero()
                || auth.timeout > Duration::from_secs(60)
            {
                return Err(invalid(
                    "invalid Auth provider or ambiguous anonymous/Auth modes",
                ));
            }
        }
        if [
            self.max_connections,
            self.max_connections_per_ip,
            self.max_connection_attempts_per_ip,
            self.max_tracked_peer_ips,
            self.max_sessions,
            self.max_packet_bytes,
            self.max_client_id_bytes,
            self.max_subscriptions_per_client,
            self.per_client_outbound_queue,
            self.max_inflight_per_client,
            self.max_offline_messages_per_client,
            self.max_pending_bytes_per_client,
            self.max_inbound_qos2_per_client,
            self.max_inbound_qos2_bytes_per_client,
            self.max_retained_topics,
            self.max_will_records,
            self.max_publish_records,
            self.memory_budget_bytes,
        ]
        .contains(&0)
            || self.connection_attempt_window.is_zero()
            || self.connection_attempt_window > Duration::from_secs(3600)
            || self.handshake_timeout.is_zero()
            || self.write_timeout.is_zero()
        {
            return Err(invalid("capacities and timeouts must be positive"));
        }
        if !(23..=65535).contains(&self.max_client_id_bytes)
            || self.max_packet_bytes < 128
            || self.max_packet_bytes > 268_435_460
            || self.max_subscriptions_per_client > 65535
            || self.max_inflight_per_client > 65535
            || self.max_connections > tokio::sync::Semaphore::MAX_PERMITS
            || self.max_connections > self.max_sessions
            || self.max_inflight_per_client > self.max_offline_messages_per_client
            || self.max_offline_messages_per_client > 65535
            || self.max_inbound_qos2_per_client > 65535
            || self.handshake_timeout > Duration::from_secs(86400)
            || self.write_timeout > Duration::from_secs(86400)
        {
            return Err(invalid("inconsistent protocol/session/connection limits"));
        }
        if self
            .connection_budget()
            .and_then(|n| n.checked_add(192))
            .is_none_or(|n| n > self.memory_budget_bytes)
            || self
                .max_sessions
                .checked_mul(
                    96 + self.max_client_id_bytes
                        + if self.mqtt5 {
                            self.max_subscriptions_per_client * 4
                        } else {
                            0
                        },
                )
                .and_then(|n| n.checked_add(self.max_packet_bytes + 192))
                .is_none()
        {
            return Err(invalid(
                "memory budget cannot hold one connection or Will target reservation overflows",
            ));
        }
        Ok(())
    }

    // Conservative reservation for receive buffer, frame encoding/decoding
    // scratch, one transmit frame, control frames, and connection metadata.
    // Actual connection count may be
    // lower than max_connections when the shared memory budget is exhausted.
    pub(crate) fn connection_budget(&self) -> Option<usize> {
        self.max_packet_bytes
            // MQTT5 strict validation and the external codec keep bounded scratch copies.
            .checked_mul(if self.mqtt5 { 5 } else { 3 })?
            .checked_add(
                self.per_client_outbound_queue
                    .checked_mul(self.max_packet_bytes.min(1024))?,
            )?
            .checked_add(16 * 1024)?
            .checked_add(if self.mqtt5 {
                32 * (self.max_packet_bytes.min(65535) + 64)
            } else {
                0
            })?
            .checked_add(if self.tls.is_some() { 256 * 1024 } else { 0 })?
            .checked_add(if self.websocket.is_some() {
                self.max_packet_bytes
                    .checked_mul(3)?
                    .checked_add(32 * 1024)?
            } else {
                0
            })
    }
}
