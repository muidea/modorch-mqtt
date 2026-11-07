use std::{net::SocketAddr, time::Duration};

use modorch_mqtt_api::MqttError;

/// Bounds for the experimental anonymous, loopback-only memory broker.
/// No Auth, TLS, persistent store, MQTT5 or QoS2 capability is implied.
#[derive(Clone, Debug)]
pub struct MqttBrokerConfig {
    /// Modorch component identity; unique within the host Application.
    pub name: String,
    /// Loopback address; port 0 requests an ephemeral port.
    pub listener: SocketAddr,
    /// Required explicit acknowledgement of anonymous development access.
    pub anonymous_access: bool,
    /// Bound on managed socket tasks, including incomplete CONNECT handshakes.
    pub max_connections: usize,
    /// Combined online/offline Session bound.
    pub max_sessions: usize,
    /// Maximum complete control packet size, including fixed header.
    pub max_packet_bytes: usize,
    /// ClientID UTF-8 byte bound.
    pub max_client_id_bytes: usize,
    /// Deadline from TCP admission to CONNECT acceptance.
    pub handshake_timeout: Duration,
    /// Absolute deadline to finish writing one packet.
    pub write_timeout: Duration,
    /// Maximum installed filters per Session and filters per control packet.
    pub max_subscriptions_per_client: usize,
    /// Separate bounded QoS0 and control reply queue lengths per connection.
    pub per_client_outbound_queue: usize,
    /// Maximum QoS1 PUBLISH packets awaiting PUBACK on a connection.
    pub max_inflight_per_client: usize,
    /// Combined reliable Pending + Inflight limit, both online and offline.
    pub max_offline_messages_per_client: usize,
    /// Accounted bytes for reliable Pending + Inflight per Session.
    pub max_pending_bytes_per_client: usize,
    /// Maximum retained Topic Names.
    pub max_retained_topics: usize,
    /// Combined armed and triggered Will record count.
    pub max_will_records: usize,
    /// Global accounted data and reservation budget; excludes allocator/RSS overhead.
    pub memory_budget_bytes: usize,
}

impl Default for MqttBrokerConfig {
    fn default() -> Self {
        Self {
            name: "mqtt-broker".into(),
            listener: "127.0.0.1:1883".parse().unwrap(),
            anonymous_access: false,
            max_connections: 500,
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
            max_retained_topics: 10_000,
            max_will_records: 500,
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
        if self.name.is_empty() || self.name.len() > 128 || self.name.contains(['/', '\0']) {
            return Err(invalid("broker name must be 1..128 bytes without / or NUL"));
        }
        if !self.listener.ip().is_loopback() {
            return Err(invalid(
                "this release supports loopback listeners only; TLS/Auth are not implemented",
            ));
        }
        if !self.anonymous_access {
            return Err(invalid(
                "anonymous_access must be explicitly enabled; Auth is not implemented",
            ));
        }
        if [
            self.max_connections,
            self.max_sessions,
            self.max_packet_bytes,
            self.max_client_id_bytes,
            self.max_subscriptions_per_client,
            self.per_client_outbound_queue,
            self.max_inflight_per_client,
            self.max_offline_messages_per_client,
            self.max_pending_bytes_per_client,
            self.max_retained_topics,
            self.max_will_records,
            self.memory_budget_bytes,
        ]
        .contains(&0)
            || self.handshake_timeout.is_zero()
            || self.write_timeout.is_zero()
        {
            return Err(invalid("capacities and timeouts must be positive"));
        }
        if self.max_client_id_bytes > 65535
            || self.max_packet_bytes < 128
            || self.max_packet_bytes > 268_435_460
            || self.max_inflight_per_client > 65535
            || self.max_connections > tokio::sync::Semaphore::MAX_PERMITS
            || self.max_connections > self.max_sessions
            || self.max_inflight_per_client > self.max_offline_messages_per_client
            || self.max_offline_messages_per_client > 65535
            || self.handshake_timeout > Duration::from_secs(86400)
            || self.write_timeout > Duration::from_secs(86400)
        {
            return Err(invalid("inconsistent protocol/session/connection limits"));
        }
        if self
            .connection_budget()
            .is_none_or(|n| n > self.memory_budget_bytes)
            || self
                .max_sessions
                .checked_mul(96 + self.max_client_id_bytes)
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
            .checked_mul(3)?
            .checked_add(
                self.per_client_outbound_queue
                    .checked_mul(self.max_packet_bytes.min(1024))?,
            )?
            .checked_add(16 * 1024)
    }
}
