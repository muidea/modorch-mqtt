use modorch::{prelude::*, *};
use modorch_mqtt_api::*;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};

/// Explicitly selected internal administrative proxy. No HTTP listener is opened.
#[derive(Clone, Debug)]
pub struct AdminConfig {
    /// Admin Module name.
    pub name: String,
    /// Target Core Module name. Core must allowlist this Admin component.
    pub broker: String,
    /// Application-global Admin endpoint subscription ID.
    pub subscription_id: u64,
    /// Exact business Handler identities allowed to issue administrative operations.
    pub allowed_callers: Vec<ComponentRef>,
    /// Maximum Core result wait; Indeterminate is preserved as a framework error.
    pub timeout: Duration,
    /// Bounded in-memory audit ring length, 1..10000.
    pub audit_capacity: usize,
}
impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            name: "mqtt-admin".into(),
            broker: "mqtt-broker".into(),
            subscription_id: 2000,
            allowed_callers: vec![],
            timeout: Duration::from_secs(2),
            audit_capacity: 1024,
        }
    }
}
/// One bounded metadata-only administrative audit entry.
#[derive(Clone, Debug)]
pub struct AdminAuditEntry {
    /// UTC Unix milliseconds.
    pub timestamp_ms: u64,
    /// Framework source identity, absent for rejected external root submissions.
    pub source: Option<ComponentRef>,
    /// Fixed operation label; contains no ClientID, Topic, credential or payload.
    pub operation: &'static str,
    /// Whether a successful Core business reply was observed. False includes uncertainty.
    pub success: bool,
}
/// Read-only application-local audit observation.
#[derive(Clone)]
pub struct AdminHandle(Arc<Mutex<VecDeque<AdminAuditEntry>>>);
impl AdminHandle {
    /// Read the current bounded audit window, oldest first. It is not a durable audit log.
    pub fn audit(&self) -> Vec<AdminAuditEntry> {
        self.0.lock().unwrap().iter().cloned().collect()
    }
}
/// Factory retaining the audit ring across Admin Module reconstructions.
pub struct AdminFactory {
    config: Arc<AdminConfig>,
    handle: AdminHandle,
    #[cfg(feature = "audit")]
    audit_log: Option<crate::audit::AuditLog>,
}
fn descriptor(config: &AdminConfig) -> ComponentDescriptor {
    let mut d = ComponentDescriptor::new(ComponentRef::module(&config.name));
    d.requires.push(ComponentRef::module(&config.broker));
    d.queue_capacity = 64;
    d
}
impl AdminFactory {
    /// Validate explicit component names, bounded audit resources and timeout.
    pub fn new(config: AdminConfig) -> Result<Self, MqttError> {
        if [&config.name, &config.broker]
            .into_iter()
            .any(|s| s.is_empty() || s.len() > 128 || s.contains(['/', '\0']))
            || config.name == config.broker
            || !(1..=10000).contains(&config.audit_capacity)
            || config.timeout.is_zero()
            || config.timeout > Duration::from_secs(60)
            || config.allowed_callers.len() > 256
            || config
                .allowed_callers
                .iter()
                .any(|s| s.id.is_empty() || s.id.len() > 128 || s.id.contains('\0'))
        {
            return Err(MqttError::Configuration(
                "invalid Admin configuration".into(),
            ));
        }
        let handle = AdminHandle(Arc::new(Mutex::new(VecDeque::with_capacity(
            config.audit_capacity,
        ))));
        Ok(Self {
            config: Arc::new(config),
            handle,
            #[cfg(feature = "audit")]
            audit_log: None,
        })
    }
    /// Add a dedicated durable audit with intent-before-forward and outcome-after-reply.
    /// A committed intent without an outcome means the administrative result is unknown.
    #[cfg(feature = "audit")]
    pub fn with_audit_log(mut self, config: crate::SqliteAuditConfig) -> Result<Self, MqttError> {
        if !config.validate() {
            return Err(MqttError::Configuration(
                "invalid persistent audit configuration".into(),
            ));
        }
        self.audit_log = Some(crate::audit::AuditLog::new(config));
        Ok(self)
    }
    /// Obtain read-only audit access without enabling or starting this Module.
    pub fn handle(&self) -> AdminHandle {
        self.handle.clone()
    }
}
impl ModuleFactory for AdminFactory {
    fn descriptor(&self) -> ComponentDescriptor {
        descriptor(&self.config)
    }
    fn create(&self, _: &ComponentConfig) -> FrameworkResult<Box<dyn Module>> {
        Ok(Box::new(Admin {
            config: self.config.clone(),
            handle: self.handle.clone(),
            #[cfg(feature = "audit")]
            audit_log: self.audit_log.clone(),
        }))
    }
}
#[derive(Clone)]
struct Admin {
    config: Arc<AdminConfig>,
    handle: AdminHandle,
    #[cfg(feature = "audit")]
    audit_log: Option<crate::audit::AuditLog>,
}
#[async_trait]
impl Lifecycle for Admin {
    async fn setup(&mut self, _ctx: &SetupContext) -> FrameworkResult<()> {
        #[cfg(feature = "audit")]
        if let Some(log) = &self.audit_log {
            log.open(&_ctx.tasks).await?;
        }
        Ok(())
    }
    async fn teardown(&mut self, _: &ShutdownContext) -> FrameworkResult<()> {
        #[cfg(feature = "audit")]
        if let Some(log) = &self.audit_log {
            log.close();
        }
        Ok(())
    }
}
impl Module for Admin {
    fn descriptor(&self) -> ComponentDescriptor {
        descriptor(&self.config)
    }
    fn handlers(&mut self) -> FrameworkResult<Option<HandlerRegistration>> {
        Ok(Some(HandlerRegistration::Serial {
            handler: Box::new(self.clone()),
            subscriptions: vec![SubscriptionSpec::new(
                self.config.subscription_id,
                self.descriptor().component(),
                ADMIN_TOPIC,
            )],
        }))
    }
}
#[async_trait]
impl SerialHandler for Admin {
    async fn handle(
        &mut self,
        ctx: &HandlerContext,
        message: MessageEnvelope,
    ) -> FrameworkResult<HandlerReply> {
        let allowed = message.kind == MessageKind::Request
            && message.destination == Destination::Component(ctx.component.clone())
            && message
                .source
                .as_ref()
                .is_some_and(|s| self.config.allowed_callers.contains(s));
        if !allowed {
            self.audit(&message, "denied", false);
            #[cfg(feature = "audit")]
            if let Some(log) = &self.audit_log {
                log.append(
                    message.source.as_ref().map(ToString::to_string),
                    "denied",
                    "outcome",
                    false,
                )
                .await?;
            }
            return Ok(Arc::new(Err::<AdminReply, _>(AdminError::NotAuthorized)));
        }
        let request = match message.payload::<AdminRequest>() {
            Ok(request) => request,
            Err(error) => {
                self.audit(&message, "invalid", false);
                #[cfg(feature = "audit")]
                if let Some(log) = &self.audit_log {
                    log.append(
                        message.source.as_ref().map(ToString::to_string),
                        "invalid",
                        "outcome",
                        false,
                    )
                    .await?;
                }
                return Err(error);
            }
        };
        let label = match request.as_ref() {
            AdminRequest::BrokerStats => "stats",
            AdminRequest::GetSession { .. } => "session",
            AdminRequest::ListClients { .. } => "clients",
            AdminRequest::DisconnectClient { .. } => "disconnect",
            AdminRequest::ListSubscriptions { .. } => "subscriptions",
        };
        let bytes = match request.as_ref() {
            AdminRequest::GetSession { client_id }
            | AdminRequest::DisconnectClient { client_id, .. } => 256 + client_id.len(),
            AdminRequest::ListClients { after, .. } => 256 + after.as_ref().map_or(0, String::len),
            AdminRequest::ListSubscriptions {
                client_id, after, ..
            } => 256 + client_id.len() + after.as_ref().map_or(0, String::len),
            AdminRequest::BrokerStats => 256,
        };
        #[cfg(feature = "audit")]
        if let Some(log) = &self.audit_log {
            log.append(
                message.source.as_ref().map(ToString::to_string),
                label,
                "intent",
                false,
            )
            .await?;
        }
        let result = async {
            ctx.event_hub
                .request::<_, AdminResult>(
                    ADMIN_CONTROL_TOPIC,
                    Destination::Component(ComponentRef::module(&self.config.broker)),
                    request.as_ref().clone(),
                    MessageOptions {
                        payload_bytes: Some(bytes),
                        ..Default::default()
                    },
                )?
                .wait(self.config.timeout)
                .await
        }
        .await;
        #[cfg(feature = "audit")]
        if let Some(log) = &self.audit_log {
            log.append(
                message.source.as_ref().map(ToString::to_string),
                label,
                "outcome",
                result.as_ref().is_ok_and(|r| r.is_ok()),
            )
            .await?;
        }
        self.audit(&message, label, result.as_ref().is_ok_and(|r| r.is_ok()));
        Ok(result?)
    }
}
impl Admin {
    fn audit(&self, message: &MessageEnvelope, operation: &'static str, success: bool) {
        let mut audit = self.handle.0.lock().unwrap();
        if audit.len() == self.config.audit_capacity {
            audit.pop_front();
        }
        audit.push_back(AdminAuditEntry {
            timestamp_ms: unix_time_ms(),
            source: message.source.clone(),
            operation,
            success,
        });
    }
}
