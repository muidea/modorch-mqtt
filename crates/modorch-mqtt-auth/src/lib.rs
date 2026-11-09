//! Optional password/ACL authorization Module. Passwords are verified by Argon2id
//! on the host's bounded CPU executor; Core depends only on the typed API contract.
#![forbid(unsafe_code)]
#![deny(missing_docs)]
use argon2::{Argon2, PasswordHash, PasswordVerifier};
use modorch::{prelude::*, *};
use modorch_mqtt_api::*;
use std::{
    collections::BTreeSet,
    num::NonZeroUsize,
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};
use tokio::sync::Semaphore;

/// Explicit permission for normal, retained and Will publications.
#[derive(Clone, Debug)]
pub struct PublishRule {
    /// MQTT Topic Filter matching permitted names.
    pub filter: String,
    /// Maximum original publication QoS.
    pub max_qos: Qos,
    /// Whether a retained write/delete is permitted.
    pub allow_retain: bool,
}
/// Fixed user policy; store PHC hashes, never plaintext passwords.
#[derive(Clone)]
pub struct UserPolicy {
    /// MQTT username.
    pub username: String,
    /// Stable identity, independent of future username/password changes.
    pub subject: String,
    /// Argon2id v19 PHC string, validated with bounded memory/iteration costs.
    pub password_hash: String,
    /// Nonempty literal ClientID prefixes allowed for this user.
    pub client_id_prefixes: Vec<String>,
    /// Explicit publication grants; empty denies all publication/Will operations.
    pub publish: Vec<PublishRule>,
    /// Allowed MQTT filter languages; requested filters must be contained by one entry.
    pub subscribe: Vec<String>,
}
impl std::fmt::Debug for UserPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UserPolicy")
            .field("username", &self.username)
            .field("subject", &self.subject)
            .field("password_hash", &"[REDACTED]")
            .finish()
    }
}
/// Permission for a trusted business component's explicit outbound requests.
#[derive(Clone, Debug)]
pub struct InternalPolicy {
    /// Exact framework component Display form, e.g. `Module:business`.
    pub source: String,
    /// Publication permissions; no implicit inheritance from user ACLs.
    pub publish: Vec<PublishRule>,
}
/// Immutable provider configuration. Replace the provider explicitly to change policy.
#[derive(Clone, Debug)]
pub struct PasswordAuthConfig {
    /// Provider Module identity.
    pub name: String,
    /// Stable authentication realm. Cannot use Core's `$anonymous/` namespace.
    pub domain: String,
    /// Application-global subscription ID, distinct from Core and other modules.
    pub subscription_id: u64,
    /// Policy revision bound into principals and grants.
    pub policy_version: u64,
    /// Connection capability validity, at most 24 hours.
    pub principal_ttl: Duration,
    /// Core Module names permitted to attest internal publisher identity.
    pub trusted_brokers: Vec<String>,
    /// Immutable bounded user table, at most 1024 entries.
    pub users: Vec<UserPolicy>,
    /// Explicit business component permissions.
    pub internal: Vec<InternalPolicy>,
}
impl Default for PasswordAuthConfig {
    fn default() -> Self {
        Self {
            name: "mqtt-auth".into(),
            domain: "mqtt-users".into(),
            subscription_id: 1000,
            policy_version: 1,
            principal_ttl: Duration::from_secs(3600),
            trusted_brokers: vec!["mqtt-broker".into()],
            users: vec![],
            internal: vec![],
        }
    }
}
fn invalid(reason: &str) -> MqttError {
    MqttError::Configuration(reason.into())
}
fn valid_name(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256 && !value.contains('\0')
}
fn valid_filter(value: &str) -> bool {
    if value.len() > 65535 || value.is_empty() || value.contains('\0') {
        return false;
    }
    let mut parts = value.split('/').peekable();
    while let Some(part) = parts.next() {
        if (part.contains('+') && part != "+")
            || (part.contains('#') && (part != "#" || parts.peek().is_some()))
        {
            return false;
        }
    }
    true
}
fn rules_valid(rules: &[PublishRule]) -> bool {
    rules.len() <= 256
        && rules
            .iter()
            .all(|r| r.filter.len() <= 1024 && valid_filter(&r.filter))
}
impl PasswordAuthConfig {
    /// Reject ambiguous identities, unbounded ACLs and unsafe password hash work factors.
    pub fn validate(&self) -> Result<(), MqttError> {
        if !valid_name(&self.name)
            || self.name.len() > 128
            || self.name.contains('/')
            || !valid_name(&self.domain)
            || self.domain.starts_with("$anonymous/")
            || self.principal_ttl < Duration::from_secs(1)
            || self.principal_ttl > Duration::from_secs(86400)
            || self.users.len() > 1024
            || self.internal.len() > 1024
            || self.trusted_brokers.len() > 64
            || self.trusted_brokers.iter().any(|s| !valid_name(s))
        {
            return Err(invalid("invalid Auth identity, TTL or table limits"));
        }
        let mut usernames = BTreeSet::new();
        let mut subjects = BTreeSet::new();
        for user in &self.users {
            if !valid_name(&user.username)
                || !valid_name(&user.subject)
                || !usernames.insert(&user.username)
                || !subjects.insert(&user.subject)
                || user.client_id_prefixes.is_empty()
                || user.client_id_prefixes.len() > 256
                || user.client_id_prefixes.iter().any(|s| !valid_name(s))
                || !rules_valid(&user.publish)
                || user.subscribe.len() > 256
                || user
                    .subscribe
                    .iter()
                    .any(|s| s.len() > 1024 || !valid_filter(s))
                || user.password_hash.len() > 512
            {
                return Err(invalid("invalid or duplicate Auth user/ACL"));
            }
            let hash = PasswordHash::new(&user.password_hash)
                .map_err(|_| invalid("invalid Argon2id PHC hash"))?;
            let m = hash.params.get_decimal("m").unwrap_or(0);
            let t = hash.params.get_decimal("t").unwrap_or(0);
            let p = hash.params.get_decimal("p").unwrap_or(0);
            if hash.algorithm.as_str() != "argon2id"
                || hash.version != Some(19)
                || !(19456..=65536).contains(&m)
                || !(2..=4).contains(&t)
                || !(1..=4).contains(&p)
                || hash.salt.is_none()
                || hash.hash.as_ref().is_none_or(|v| v.len() < 32)
            {
                return Err(invalid(
                    "unsupported Argon2id parameters (m=19456..65536,t=2..4,p=1..4)",
                ));
            }
        }
        let mut internal = BTreeSet::new();
        if self.internal.iter().any(|p| {
            !valid_name(&p.source) || !internal.insert(&p.source) || !rules_valid(&p.publish)
        }) {
            return Err(invalid("invalid or duplicate internal publication ACL"));
        }
        Ok(())
    }
}
/// Factory for the optional password/ACL Module. Registration does not enable it.
pub struct PasswordAuthFactory {
    config: Arc<PasswordAuthConfig>,
    policy: PasswordAuthHandle,
    password_slots: Arc<PasswordSlots>,
}
// Four admitted password requests, one physical hash. Independent lightweight
// decisions retain worker capacity even while these four wait for CPU work.
struct PasswordSlots {
    admission: Arc<Semaphore>,
    execution: Arc<Semaphore>,
    #[cfg(test)]
    probe: Mutex<Option<Arc<tests::PasswordProbe>>>,
}
impl Default for PasswordSlots {
    fn default() -> Self {
        Self {
            admission: Arc::new(Semaphore::new(4)),
            execution: Arc::new(Semaphore::new(1)),
            #[cfg(test)]
            probe: Mutex::new(None),
        }
    }
}
/// Host-owned policy replacement capability. Never expose it over MQTT.
#[derive(Clone)]
pub struct PasswordAuthHandle(Arc<RwLock<Arc<PasswordAuthConfig>>>);
impl PasswordAuthHandle {
    /// Atomically install a validated, strictly newer revision of the same provider/realm.
    /// New authorization operations use this revision. Existing reliable exchanges
    /// and previously armed Will grants retain their accepted ownership boundaries.
    pub fn replace(&self, config: PasswordAuthConfig) -> Result<(), MqttError> {
        config.validate()?;
        let mut current = self.0.write().unwrap();
        if config.name != current.name
            || config.domain != current.domain
            || config.subscription_id != current.subscription_id
            || config.policy_version <= current.policy_version
        {
            return Err(invalid(
                "policy identity must remain stable and revision must increase",
            ));
        }
        *current = Arc::new(config);
        Ok(())
    }
    /// Current policy revision, without credentials or ACL contents.
    pub fn revision(&self) -> u64 {
        self.0.read().unwrap().policy_version
    }
}
impl PasswordAuthFactory {
    /// Validate all policies without authenticating or starting an executor.
    pub fn new(config: PasswordAuthConfig) -> Result<Self, MqttError> {
        config.validate()?;
        let config = Arc::new(config);
        Ok(Self {
            policy: PasswordAuthHandle(Arc::new(RwLock::new(config.clone()))),
            config,
            password_slots: Arc::new(PasswordSlots::default()),
        })
    }
}
impl PasswordAuthFactory {
    /// Retain an explicit trusted-host capability for runtime policy updates.
    pub fn handle(&self) -> PasswordAuthHandle {
        self.policy.clone()
    }
}
impl ModuleFactory for PasswordAuthFactory {
    fn descriptor(&self) -> ComponentDescriptor {
        self.password_slots.descriptor(&self.config)
    }
    fn create(&self, _: &ComponentConfig) -> FrameworkResult<Box<dyn Module>> {
        Ok(Box::new(AuthModule {
            config: self.config.clone(),
            policy: self.policy.clone(),
            tasks: Arc::new(Mutex::new(None)),
            password_slots: self.password_slots.clone(),
        }))
    }
}
impl PasswordSlots {
    fn descriptor(&self, config: &PasswordAuthConfig) -> ComponentDescriptor {
        let mut d = ComponentDescriptor::new(ComponentRef::module(&config.name));
        d.queue_capacity = 64;
        d.execution = ExecutionPolicy::Concurrent {
            max_in_flight: NonZeroUsize::new(8).unwrap(),
        };
        #[cfg(test)]
        if self
            .probe
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|p| p.restart)
        {
            d.failure = FailurePolicy::Restart {
                max_attempts: 1,
                backoff: Duration::from_millis(1),
            };
        }
        d
    }
}
struct AuthModule {
    config: Arc<PasswordAuthConfig>,
    policy: PasswordAuthHandle,
    tasks: Arc<Mutex<Option<TaskScheduler>>>,
    password_slots: Arc<PasswordSlots>,
}
#[async_trait]
impl Lifecycle for AuthModule {
    async fn start(&mut self, ctx: &RuntimeContext) -> FrameworkResult<()> {
        *self.tasks.lock().unwrap() = Some(ctx.tasks.clone());
        #[cfg(test)]
        if let Some(probe) = self.password_slots.probe.lock().unwrap().as_ref() {
            probe.schedulers.lock().unwrap().push(ctx.tasks.clone());
        }
        Ok(())
    }
    async fn teardown(&mut self, _: &ShutdownContext) -> FrameworkResult<()> {
        self.tasks.lock().unwrap().take();
        Ok(())
    }
}
impl Module for AuthModule {
    fn descriptor(&self) -> ComponentDescriptor {
        self.password_slots.descriptor(&self.config)
    }
    fn handlers(&mut self) -> FrameworkResult<Option<HandlerRegistration>> {
        Ok(Some(HandlerRegistration::Concurrent {
            max_in_flight: NonZeroUsize::new(8).unwrap(),
            handler: Arc::new(AuthHandler {
                policy: self.policy.clone(),
                tasks: self.tasks.clone(),
                password_slots: self.password_slots.clone(),
            }),
            subscriptions: vec![SubscriptionSpec::new(
                self.config.subscription_id,
                self.descriptor().component(),
                AUTH_TOPIC,
            )],
        }))
    }
}
struct AuthHandler {
    policy: PasswordAuthHandle,
    tasks: Arc<Mutex<Option<TaskScheduler>>>,
    password_slots: Arc<PasswordSlots>,
}
#[async_trait]
impl ConcurrentHandler for AuthHandler {
    async fn handle(
        &self,
        _: &HandlerContext,
        message: MessageEnvelope,
    ) -> FrameworkResult<HandlerReply> {
        self.reply(message).await
    }
}
impl AuthHandler {
    async fn reply(&self, message: MessageEnvelope) -> FrameworkResult<HandlerReply> {
        if message.kind != MessageKind::Request {
            return Ok(Arc::new(()));
        }
        // A per-operation immutable revision, never shared mutable handler state.
        let config = self.policy.0.read().unwrap().clone();
        let request = message.payload::<AuthRequest>()?;
        let result = if let AuthRequest::Authenticate(auth) = request.as_ref() {
            self.authenticate(auth, config).await
        } else {
            PolicyDecision { config }.decide(&message, request.as_ref())
        };
        Ok(Arc::new(result))
    }
    async fn authenticate(
        &self,
        auth: &Authenticate,
        config: Arc<PasswordAuthConfig>,
    ) -> AuthResult {
        if auth.username.as_ref().is_none_or(|s| s.len() > 256)
            || auth.password.as_ref().is_none_or(|p| p.len() > 1024)
        {
            return Err(AuthError::Denied);
        }
        let user = config
            .users
            .iter()
            .find(|u| Some(&u.username) == auth.username.as_ref())
            .cloned()
            .ok_or(AuthError::Denied)?;
        let admission = self
            .password_slots
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| AuthError::Unavailable)?;
        let execution = self
            .password_slots
            .execution
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| AuthError::Unavailable)?;
        let tasks = self
            .tasks
            .lock()
            .unwrap()
            .clone()
            .ok_or(AuthError::Unavailable)?;
        let password = auth.password.clone().unwrap();
        #[cfg(test)]
        let probe = self.password_slots.probe.lock().unwrap().clone();
        let task = tasks
            .spawn_cpu(move |cancel| {
                // The physical job owns both permits until actual exit, including
                // handler abort or a Running request whose caller timed out.
                let _permits = (admission, execution);
                // A test-only physical-job barrier; no production timing hook.
                #[cfg(test)]
                let _probe = probe.as_ref().map(|p| p.enter(cancel.clone()));
                if cancel.is_cancelled() {
                    return Ok(Err(AuthError::Unavailable));
                }
                let verified = PasswordHash::new(&user.password_hash)
                    .is_ok_and(|hash| Argon2::default().verify_password(&password, &hash).is_ok());
                Ok(if verified && !cancel.is_cancelled() {
                    Ok(AuthDecision::Principal(Principal {
                        key: PrincipalKey {
                            domain: config.domain.clone(),
                            subject: user.subject,
                        },
                        policy_version: config.policy_version,
                        expires_at_ms: unix_time_ms()
                            .saturating_add(config.principal_ttl.as_millis() as u64),
                    }))
                } else {
                    Err(AuthError::Denied)
                })
            })
            .map_err(|_| AuthError::Unavailable)?;
        task.wait_finished()
            .await
            .unwrap_or(Err(AuthError::Unavailable))
    }
}
struct PolicyDecision {
    config: Arc<PasswordAuthConfig>,
}
impl PolicyDecision {
    fn user(&self, principal: &Principal) -> Option<&UserPolicy> {
        (principal.is_valid()
            && principal.key.domain == self.config.domain
            && principal.policy_version == self.config.policy_version)
            .then(|| {
                self.config
                    .users
                    .iter()
                    .find(|u| u.subject == principal.key.subject)
            })
            .flatten()
    }
    fn decide(&self, envelope: &MessageEnvelope, request: &AuthRequest) -> AuthResult {
        match request {
            AuthRequest::SessionAttach(a) => {
                let user = self.user(&a.principal).ok_or(AuthError::Denied)?;
                if a.existing_owner
                    .as_ref()
                    .is_some_and(|o| o != &a.principal.key)
                    || !user
                        .client_id_prefixes
                        .iter()
                        .any(|prefix| a.client_id.starts_with(prefix))
                {
                    return Err(AuthError::Denied);
                }
                Ok(AuthDecision::Allowed)
            }
            AuthRequest::Subscribe(a) => {
                let user = self.user(&a.principal).ok_or(AuthError::Denied)?;
                if user
                    .subscribe
                    .iter()
                    .any(|allowed| filter_covers(allowed, &a.filter))
                {
                    Ok(AuthDecision::Allowed)
                } else {
                    Err(AuthError::Denied)
                }
            }
            AuthRequest::Publish(a) => {
                let rules = if a.kind == PublishKind::InternalPublish {
                    if a.principal.is_some()
                        || !envelope.source.as_ref().is_some_and(|source| {
                            source.kind == ComponentKind::Module
                                && self.config.trusted_brokers.contains(&source.id)
                        })
                    {
                        return Err(AuthError::Denied);
                    }
                    &self
                        .config
                        .internal
                        .iter()
                        .find(|p| Some(&p.source) == a.internal_source.as_ref())
                        .ok_or(AuthError::Denied)?
                        .publish
                } else {
                    if a.internal_source.is_some() {
                        return Err(AuthError::Denied);
                    }
                    &self
                        .user(a.principal.as_ref().ok_or(AuthError::Denied)?)
                        .ok_or(AuthError::Denied)?
                        .publish
                };
                if rules.iter().any(|r| {
                    a.qos <= r.max_qos
                        && (!a.retain || r.allow_retain)
                        && modorch::topic_matches(&r.filter, &a.topic)
                }) {
                    Ok(AuthDecision::Publish(PublishGrant {
                        context: a.clone(),
                        policy_version: self.config.policy_version,
                    }))
                } else {
                    Err(AuthError::Denied)
                }
            }
            AuthRequest::Authenticate(_) => Err(AuthError::Denied),
        }
    }
}

/// Conservative MQTT filter containment. Never grants a wider filter than allowed.
/// A requested wildcard language must fit in one configured filter; unions are not inferred.
pub fn filter_covers(allowed: &str, requested: &str) -> bool {
    if !valid_filter(allowed)
        || !valid_filter(requested)
        || (requested.starts_with('$') && allowed.starts_with(['+', '#']))
    {
        return false;
    }
    let mut permitted = allowed.split('/');
    let mut desired = requested.split('/');
    loop {
        match (permitted.next(), desired.next()) {
            (Some("#"), _) => return true,
            (None, None) => return true,
            (Some(_), Some("#")) => return false,
            (Some("+"), Some(_)) => (),
            (Some(a), Some(b)) if a == b => (),
            _ => return false,
        }
    }
}

#[cfg(test)]
mod tests;
