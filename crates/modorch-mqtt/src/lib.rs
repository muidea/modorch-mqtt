//! Embed the experimental MQTT 3.1.1 QoS0/1/2 broker into an existing Modorch app.
//! Register factories, explicitly enable the returned module IDs, and await the
//! host's checked shutdown. No second Application, EventHub or Runtime is created.
//!
//! ```no_run
//! use modorch::{ApplicationBuilder, ComponentSelection, ShutdownOptions};
//! use modorch_mqtt::{MqttBrokerBundle, MqttBrokerConfig};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut builder = ApplicationBuilder::new("existing-host");
//! let mqtt = MqttBrokerBundle::new(MqttBrokerConfig::loopback_development())
//!     .register(&mut builder)?;
//! let mut selected = ComponentSelection::new();
//! for id in mqtt.module_ids() { selected.enable_module(id); }
//! let app = builder.selection(selected).build()?.start().await?;
//! app.shutdown_checked(ShutdownOptions::graceful()).await?;
//! # Ok(())
//! # }
//! ```
#![forbid(unsafe_code)]
#![deny(missing_docs)]

#[cfg(feature = "bootstrap")]
mod host;
#[cfg(feature = "bootstrap")]
pub use host::{MqttBrokerHost, MqttHostError};

#[cfg(feature = "toml")]
mod file_config;
#[cfg(feature = "toml")]
pub use file_config::{MAX_MQTT_TOML_BYTES, MqttTomlConfig};

pub use modorch_mqtt_api::*;
#[cfg(feature = "auth")]
pub use modorch_mqtt_auth::*;
pub use modorch_mqtt_core::{
    AdminControlConfig, AuthProviderConfig, BrokerHandle, MqttBrokerConfig, MqttBrokerFactory,
    PUBLISH_QUERY_TOPIC, PUBLISH_TOPIC, STATUS_TOPIC, StorageProviderConfig, TlsConfig,
    WebSocketConfig,
};
#[cfg(feature = "admin")]
pub use modorch_mqtt_extensions::{AdminAuditEntry, AdminConfig, AdminFactory, AdminHandle};
#[cfg(feature = "metrics")]
pub use modorch_mqtt_extensions::{MetricsConfig, MetricsFactory, MetricsHandle, MetricsSnapshot};

#[cfg(feature = "storage")]
pub use modorch_mqtt_storage::{SqliteStorageConfig, SqliteStorageFactory};

/// Factory assembly only. Construction/registration does not listen or spawn.
pub struct MqttBrokerBundle {
    config: MqttBrokerConfig,
}

impl MqttBrokerBundle {
    /// Build an unstarted assembly from an explicit configuration.
    pub fn new(config: MqttBrokerConfig) -> Self {
        Self { config }
    }

    /// Register Core in the supplied builder; does not implicitly enable it.
    pub fn register(
        self,
        builder: &mut modorch::ApplicationBuilder,
    ) -> Result<MqttRegisteredComponents, MqttError> {
        let name = self.config.name.clone();
        let factory = MqttBrokerFactory::new(self.config)?;
        let handle = factory.handle();
        builder
            .register_module(factory)
            .map_err(|e| MqttError::Configuration(e.to_string()))?;
        Ok(MqttRegisteredComponents {
            ids: vec![name],
            handle,
        })
    }
}

/// Component selection and read-only observation returned by registration.
pub struct MqttRegisteredComponents {
    ids: Vec<String>,
    handle: BrokerHandle,
}

impl MqttRegisteredComponents {
    /// Module IDs the host must explicitly add to its ComponentSelection.
    pub fn module_ids(&self) -> impl Iterator<Item = &str> {
        self.ids.iter().map(String::as_str)
    }
    /// Read-only status and bound socket observation.
    pub fn handle(&self) -> &BrokerHandle {
        &self.handle
    }
}

#[cfg(feature = "http")]
pub use modorch_mqtt_extensions::{HttpConfig, HttpFactory, HttpHandle};

#[cfg(feature = "audit")]
pub use modorch_mqtt_extensions::SqliteAuditConfig;

/// Optional explicit MQTT5 network Bridge.
#[cfg(feature = "bridge")]
pub use modorch_mqtt_bridge as bridge;
