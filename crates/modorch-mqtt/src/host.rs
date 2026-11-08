use crate::{BrokerHandle, MqttBrokerBundle, MqttBrokerConfig, MqttError};
use modorch::{ApplicationBuilder, ApplicationHandle, ComponentSelection, FrameworkError};

/// Failure preserving the framework startup compensation and cleanup retry handle.
#[derive(Debug)]
pub enum MqttHostError {
    /// Invalid broker assembly; nothing was started.
    Configuration(MqttError),
    /// Application construction or dependency selection failed.
    Build(FrameworkError),
    /// Startup failed; inspect the primary cause, compensation and retained handle.
    Startup(Box<modorch::StartupError>),
}
impl std::fmt::Display for MqttHostError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Configuration(e) => write!(f, "broker configuration: {e}"),
            Self::Build(e) => write!(f, "broker host build: {e}"),
            Self::Startup(e) => write!(f, "broker host startup: {e}"),
        }
    }
}
impl std::error::Error for MqttHostError {}

/// Opt-in adapter for a Tokio host that does not already own a Modorch Application.
///
/// Uses the caller's Runtime. No Runtime, signal handler or background cleanup is
/// created. Call and await `shutdown_checked` before dropping the host; a timeout
/// retains this handle for retry. Existing Modorch applications use the Bundle.
pub struct MqttBrokerHost {
    application: ApplicationHandle,
    broker: BrokerHandle,
}
impl MqttBrokerHost {
    /// Explicitly create one Application and select its Core; extensions stay disabled.
    pub async fn start(config: MqttBrokerConfig) -> Result<Self, MqttHostError> {
        Self::start_with(
            ApplicationBuilder::new("mqtt-host"),
            config,
            ComponentSelection::new(),
        )
        .await
    }

    /// Start an unbuilt builder containing caller-registered extensions.
    ///
    /// The caller explicitly selects those extensions in `selection`. Core is
    /// registered and added here; configured providers are never auto-registered.
    pub async fn start_with(
        mut builder: ApplicationBuilder,
        config: MqttBrokerConfig,
        mut selection: ComponentSelection,
    ) -> Result<Self, MqttHostError> {
        let registered = MqttBrokerBundle::new(config)
            .register(&mut builder)
            .map_err(MqttHostError::Configuration)?;
        for id in registered.module_ids() {
            selection.enable_module(id);
        }
        let application = builder
            .selection(selection)
            .build()
            .map_err(MqttHostError::Build)?
            .start()
            .await
            .map_err(|e| MqttHostError::Startup(Box::new(e)))?;
        Ok(Self {
            application,
            broker: registered.handle().clone(),
        })
    }

    /// Broker address and read-only status; availability still follows lifecycle state.
    pub fn broker(&self) -> &BrokerHandle {
        &self.broker
    }

    /// Application owned by this adapter, for trusted business and lifecycle integration.
    pub fn application(&self) -> &ApplicationHandle {
        &self.application
    }

    /// Await resource release, preserving Forced/StopIncomplete instead of reporting success.
    pub async fn shutdown_checked(
        &self,
        options: modorch::ShutdownOptions,
    ) -> modorch::FrameworkResult<modorch::ShutdownReceipt> {
        self.application.shutdown_checked(options).await
    }
}
