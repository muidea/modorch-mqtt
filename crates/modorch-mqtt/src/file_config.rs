//! Strict configuration adapter. Provider factories and all IO remain host-owned.
use crate::*;
use serde::Deserialize;
use std::{path::Path, time::Duration};

/// Maximum UTF-8 TOML document size accepted before parsing (64 KiB).
pub const MAX_MQTT_TOML_BYTES: usize = 64 * 1024;

/// Validated Core configuration and explicit module selection from a TOML document.
///
/// Parsing and registration perform no file IO, bind, task spawn or module enabling.
/// Provider sections reference factories the host must register separately. Storage
/// capability is supplied out of band; it is never read from the document.
///
/// ```
/// use modorch_mqtt::MqttTomlConfig;
/// let plan = MqttTomlConfig::parse(r#"
/// [mqtt.core]
/// enabled = true
/// [mqtt.security]
/// anonymous_access = true
/// "#, ".", None).unwrap();
/// assert_eq!(plan.module_ids().collect::<Vec<_>>(), ["mqtt-broker"]);
/// assert!(plan.core_config().unwrap().listener.ip().is_loopback());
/// ```
pub struct MqttTomlConfig {
    core: Option<MqttBrokerConfig>,
    modules: Vec<String>,
}
impl std::fmt::Debug for MqttTomlConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqttTomlConfig")
            .field("enabled", &self.core.is_some())
            .field("module_count", &self.modules.len())
            .finish()
    }
}
impl MqttTomlConfig {
    /// Parse an isolated `[mqtt.*]` document, reject unknown/unsupported settings,
    /// and run Core validation. Relative TLS paths use `base_dir` without IO.
    ///
    /// Core defaults to disabled; an enabled Core requires explicit anonymous
    /// loopback access or an Auth provider. Extension references never register
    /// factories. An enabled external Store requires the host's nonzero access key.
    /// Errors contain static reasons or line/byte-column locations, never source
    /// lines or input key/value contents, passwords, PHC strings or capabilities.
    pub fn parse(
        input: &str,
        base_dir: impl AsRef<Path>,
        storage_access: Option<StoreAccessKey>,
    ) -> Result<Self, MqttError> {
        if input.len() > MAX_MQTT_TOML_BYTES {
            return Err(invalid("MQTT TOML exceeds 64 KiB"));
        }
        let Root { mqtt } = toml::from_str(input).map_err(|error: toml::de::Error| {
            // Do not format the upstream error: it includes the source snippet.
            let offset = error.span().map_or(0, |span| span.start).min(input.len());
            let prefix = &input.as_bytes()[..offset];
            let line = prefix.iter().filter(|&&b| b == b'\n').count() + 1;
            let column = prefix
                .iter()
                .rposition(|&b| b == b'\n')
                .map_or(offset + 1, |i| offset - i);
            invalid(&format!(
                "invalid MQTT TOML at line {line}, byte column {column}"
            ))
        })?;
        mqtt.resolve(base_dir.as_ref(), storage_access)
    }

    /// Validated enabled Core settings; `None` means no MQTT components selected.
    pub fn core_config(&self) -> Option<&MqttBrokerConfig> {
        self.core.as_ref()
    }

    /// Core plus enabled provider/extension IDs, for the host's explicit selection.
    /// The host still must register each referenced provider factory.
    pub fn module_ids(&self) -> impl Iterator<Item = &str> {
        self.modules.iter().map(String::as_str)
    }

    /// Consume the plan for a custom Core Factory/failure-policy assembly.
    pub fn into_core_config(self) -> Option<MqttBrokerConfig> {
        self.core
    }

    /// Register only Core in the existing builder, returning all declared IDs for
    /// explicit selection. Returns `None` without registering anything if disabled.
    pub fn register(
        self,
        builder: &mut modorch::ApplicationBuilder,
    ) -> Result<Option<MqttRegisteredComponents>, MqttError> {
        let Some(config) = self.core else {
            return Ok(None);
        };
        let mut registered = MqttBrokerBundle::new(config).register(builder)?;
        registered.ids = self.modules;
        Ok(Some(registered))
    }
}
fn invalid(reason: &str) -> MqttError {
    MqttError::Configuration(reason.into())
}
fn module_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 128 && !value.contains(['/', '\0'])
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Root {
    mqtt: Mqtt,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Mqtt {
    core: Core,
    #[serde(default)]
    security: Security,
    #[serde(default)]
    eventhub: EventHubSettings,
    #[serde(default)]
    auth: Auth,
    #[serde(default)]
    tls: Tls,
    #[serde(default)]
    websocket: WebSocket,
    #[serde(default)]
    storage: Storage,
    #[serde(default)]
    admin: Admin,
    #[serde(default)]
    metrics: Reference,
}

// Keep every directly mapped Core field together; defaults always come from
// the Rust API rather than a second, independently maintained default table.
macro_rules! core_fields {
    ($($field:ident: $ty:ty),* $(,)?) => {
        #[derive(Deserialize)]
        #[serde(default, deny_unknown_fields)]
        struct Core {
            enabled: bool,
            protocols: Vec<String>,
            connection_idle_ms: u64,
            reliable_overflow_policy: String,
            handshake_timeout_ms: u64,
            write_timeout_ms: u64,
            connection_attempt_window_ms: u64,
            $($field: $ty,)*
        }
        impl Default for Core {
            fn default() -> Self {
                let config = MqttBrokerConfig::default();
                Self {
                    enabled: false,
                    protocols: vec!["3.1.1".into()],
                    connection_idle_ms: 0,
                    reliable_overflow_policy: "reject_publish".into(),
                    handshake_timeout_ms: config.handshake_timeout.as_millis() as u64,
                    write_timeout_ms: config.write_timeout.as_millis() as u64,
                    connection_attempt_window_ms: config.connection_attempt_window.as_millis() as u64,
                    $($field: config.$field,)*
                }
            }
        }
        impl Core {
            fn config(self) -> MqttBrokerConfig {
                MqttBrokerConfig {
                    $($field: self.$field,)*
                    mqtt5:self.protocols.iter().any(|p|p=="5.0"),
                    handshake_timeout: Duration::from_millis(self.handshake_timeout_ms),
                    write_timeout: Duration::from_millis(self.write_timeout_ms),
                    connection_attempt_window: Duration::from_millis(self.connection_attempt_window_ms),
                    ..Default::default()
                }
            }
        }
    };
}
core_fields! {
    name: String,
    listener: std::net::SocketAddr,
    control_subscription_ids: [u64; 3],
    max_connections: usize,
    max_connections_per_ip: usize,
    max_connection_attempts_per_ip: usize,
    max_tracked_peer_ips: usize,
    max_sessions: usize,
    max_packet_bytes: usize,
    max_client_id_bytes: usize,
    max_subscriptions_per_client: usize,
    per_client_outbound_queue: usize,
    max_inflight_per_client: usize,
    max_offline_messages_per_client: usize,
    max_pending_bytes_per_client: usize,
    max_inbound_qos2_per_client: usize,
    max_inbound_qos2_bytes_per_client: usize,
    max_retained_topics: usize,
    max_will_records: usize,
    max_publish_records: usize,
    memory_budget_bytes: usize,
}
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Security {
    anonymous_access: bool,
    allow_public_listener_without_tls: bool,
    reserved_internal_prefix: String,
    session_owner_policy: String,
}
impl Default for Security {
    fn default() -> Self {
        Self {
            anonymous_access: false,
            allow_public_listener_without_tls: false,
            reserved_internal_prefix: "$modorch/".into(),
            session_owner_policy: "same_principal".into(),
        }
    }
}
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct EventHubSettings {
    inbound_mode: String,
    inbound_topic_mode: String,
    control_prefix: String,
    allow_implicit_outbound: bool,
}
impl Default for EventHubSettings {
    fn default() -> Self {
        Self {
            inbound_mode: "best_effort".into(),
            inbound_topic_mode: "original".into(),
            control_prefix: "$modorch/mqtt/control/".into(),
            allow_implicit_outbound: false,
        }
    }
}
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Auth {
    enabled: bool,
    component: Option<String>,
    timeout_ms: Option<u64>,
}
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Tls {
    enabled: bool,
    certificate_chain: Option<std::path::PathBuf>,
    private_key: Option<std::path::PathBuf>,
}
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Storage {
    enabled: bool,
    mode: String,
    component: Option<String>,
    max_snapshot_bytes: Option<usize>,
    recovery_page_bytes: Option<usize>,
    timeout_ms: Option<u64>,
    recovery_timeout_ms: Option<u64>,
}
impl Default for Storage {
    fn default() -> Self {
        Self {
            enabled: false,
            mode: "memory".into(),
            component: None,
            max_snapshot_bytes: None,
            recovery_page_bytes: None,
            timeout_ms: None,
            recovery_timeout_ms: None,
        }
    }
}
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct WebSocket {
    enabled: bool,
    path: Option<String>,
}
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Admin {
    enabled: bool,
    component: Option<String>,
    control_subscription_id: Option<u64>,
}
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Reference {
    enabled: bool,
    component: Option<String>,
}
impl Mqtt {
    fn resolve(
        self,
        base_dir: &Path,
        access: Option<StoreAccessKey>,
    ) -> Result<MqttTomlConfig, MqttError> {
        if self.core.protocols != ["3.1.1"] && self.core.protocols != ["3.1.1", "5.0"] {
            return Err(invalid(
                "mqtt.core.protocols must be [3.1.1] or [3.1.1,5.0]",
            ));
        }
        if self.core.connection_idle_ms != 0
            || self.core.reliable_overflow_policy != "reject_publish"
        {
            return Err(invalid(
                "only MQTT KeepAlive and reject_publish are supported",
            ));
        }
        if self.security.allow_public_listener_without_tls
            || self.security.reserved_internal_prefix != "$modorch/"
            || self.security.session_owner_policy != "same_principal"
        {
            return Err(invalid("unsupported mqtt.security policy"));
        }
        if self.eventhub.inbound_mode != "best_effort"
            || self.eventhub.inbound_topic_mode != "original"
            || self.eventhub.control_prefix != "$modorch/mqtt/control/"
            || self.eventhub.allow_implicit_outbound
        {
            return Err(invalid("unsupported mqtt.eventhub policy"));
        }
        let enabled = self.core.enabled;
        let mut config = self.core.config();
        config.anonymous_access = self.security.anonymous_access;
        let mut modules = vec![config.name.clone()];
        if self.auth.enabled {
            let component = self
                .auth
                .component
                .ok_or_else(|| invalid("enabled Auth requires component"))?;
            modules.push(component.clone());
            config.auth = Some(AuthProviderConfig {
                component,
                timeout: Duration::from_millis(self.auth.timeout_ms.unwrap_or(5000)),
            });
        } else if self.auth.component.is_some() || self.auth.timeout_ms.is_some() {
            return Err(invalid("disabled Auth must not contain provider settings"));
        }
        if self.tls.enabled {
            let certificate_chain = self
                .tls
                .certificate_chain
                .filter(|p| !p.as_os_str().is_empty())
                .ok_or_else(|| invalid("enabled TLS requires certificate_chain"))?;
            let private_key = self
                .tls
                .private_key
                .filter(|p| !p.as_os_str().is_empty())
                .ok_or_else(|| invalid("enabled TLS requires private_key"))?;
            config.tls = Some(TlsConfig {
                certificate_chain: base_dir.join(certificate_chain),
                private_key: base_dir.join(private_key),
            });
        } else if self.tls.certificate_chain.is_some() || self.tls.private_key.is_some() {
            return Err(invalid("disabled TLS must not contain file settings"));
        }
        if self.websocket.enabled {
            config.websocket = Some(crate::WebSocketConfig {
                path: self.websocket.path.unwrap_or_else(|| "/mqtt".into()),
            });
        } else if self.websocket.path.is_some() {
            return Err(invalid("disabled WebSocket must not contain path"));
        }
        if self.storage.enabled {
            if self.storage.mode != "external" {
                return Err(invalid("enabled Storage requires mode = external"));
            }
            let component = self
                .storage
                .component
                .ok_or_else(|| invalid("enabled Storage requires component"))?;
            let access = access
                .ok_or_else(|| invalid("enabled Storage requires host-supplied capability"))?;
            modules.push(component.clone());
            config.storage = Some(StorageProviderConfig {
                component,
                access,
                max_snapshot_bytes: self.storage.max_snapshot_bytes.unwrap_or(4 * 1024 * 1024),
                recovery_page_bytes: self.storage.recovery_page_bytes.unwrap_or(4096),
                timeout: Duration::from_millis(self.storage.timeout_ms.unwrap_or(2000)),
                recovery_timeout: Duration::from_millis(
                    self.storage.recovery_timeout_ms.unwrap_or(30000),
                ),
            });
        } else if self.storage.mode != "memory"
            || self.storage.component.is_some()
            || self.storage.max_snapshot_bytes.is_some()
            || self.storage.recovery_page_bytes.is_some()
            || self.storage.timeout_ms.is_some()
            || self.storage.recovery_timeout_ms.is_some()
        {
            return Err(invalid("disabled Storage permits only mode = memory"));
        }
        if self.admin.enabled {
            let component = self
                .admin
                .component
                .ok_or_else(|| invalid("enabled Admin requires component"))?;
            let subscription_id = self
                .admin
                .control_subscription_id
                .ok_or_else(|| invalid("enabled Admin requires control_subscription_id"))?;
            modules.push(component.clone());
            config.admin = Some(AdminControlConfig {
                subscription_id,
                allowed_sources: vec![modorch::ComponentRef::module(component)],
            });
        } else if self.admin.component.is_some() || self.admin.control_subscription_id.is_some() {
            return Err(invalid("disabled Admin must not contain endpoint settings"));
        }
        if self.metrics.enabled {
            modules.push(
                self.metrics
                    .component
                    .ok_or_else(|| invalid("enabled Metrics requires component"))?,
            );
        } else if self.metrics.component.is_some() {
            return Err(invalid("disabled Metrics must not contain component"));
        }
        if !enabled {
            if modules.len() != 1 || config.tls.is_some() || config.websocket.is_some() {
                return Err(invalid("disabled Core cannot enable extensions"));
            }
            return Ok(MqttTomlConfig {
                core: None,
                modules: vec![],
            });
        }
        let mut unique = std::collections::BTreeSet::new();
        if modules
            .iter()
            .any(|id| !module_id(id) || !unique.insert(id))
        {
            return Err(invalid("configured module IDs must be valid and distinct"));
        }
        config.validate()?;
        Ok(MqttTomlConfig {
            core: Some(config),
            modules,
        })
    }
}
