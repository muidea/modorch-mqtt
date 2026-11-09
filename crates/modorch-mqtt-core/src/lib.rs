//! Modorch module for an experimental MQTT 3.1.1 QoS0/1/2 broker with optional durable storage.
//! The host owns Application, EventHub and Tokio. No background task is detached.
#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod auth;
mod budget;
mod codec;
mod config;
mod connection;
mod diagnostics;
#[cfg(feature = "mqtt5")]
mod mqtt5;
mod persistence;
mod state;
mod transport;
#[cfg(feature = "websocket")]
mod websocket;
mod wire;

pub use config::{
    AdminControlConfig, AuthProviderConfig, MqttBrokerConfig, StorageProviderConfig, TlsConfig,
    WebSocketConfig,
};
use connection::{ConnectionLifetime, SharedState};
use modorch::{prelude::*, *};
use modorch_mqtt_api::*;
use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    net::TcpSocket,
    sync::{Notify, Semaphore},
};

/// Minimal framework integration endpoint. Network clients cannot access it.
pub const STATUS_TOPIC: &str = "$modorch/mqtt/control/status";
/// Explicit publication control endpoint; only component-originated Requests mutate Core.
pub const PUBLISH_TOPIC: &str = "$modorch/mqtt/control/publish";
/// Source-scoped terminal publication result lookup endpoint.
pub const PUBLISH_QUERY_TOPIC: &str = "$modorch/mqtt/control/publish/query";

fn framework(error: impl std::fmt::Display) -> FrameworkError {
    FrameworkError::InvalidConfig(error.to_string())
}

/// Read-only local observation handle; cloning it does not start a runtime/Hub.
#[derive(Clone)]
pub struct BrokerHandle {
    state: SharedState,
    #[allow(dead_code)]
    transport: Arc<Mutex<transport::Transport>>,
    #[allow(dead_code)]
    tasks: Arc<Mutex<Option<TaskScheduler>>>,
}

impl BrokerHandle {
    /// Bound local socket address after setup, even when listener is inactive.
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.state.lock().unwrap().address
    }
    /// Read an instantaneous status snapshot without EventHub admission.
    pub fn status(&self) -> BrokerStatus {
        self.state.lock().unwrap().status()
    }
    /// Atomically replace TLS credentials for future handshakes using managed IO.
    /// Existing TLS sessions continue with their negotiated keys. A failed load or
    /// superseded generation leaves the current certificate and paths unchanged.
    #[cfg(feature = "tls")]
    pub async fn reload_tls(&self, tls: TlsConfig) -> Result<(), MqttError> {
        let (generation, mut config) = {
            let state = self.state.lock().unwrap();
            if !state.active || state.config.tls.is_none() {
                return Err(MqttError::Closed);
            }
            (state.generation, state.config.as_ref().clone())
        };
        config.tls = Some(tls);
        let tasks = self
            .tasks
            .lock()
            .unwrap()
            .clone()
            .ok_or(MqttError::Closed)?;
        let loaded = transport::Transport::prepare(&config, &tasks)
            .await
            .map_err(|_| MqttError::Configuration("TLS credential reload failed".into()))?;
        let mut state = self.state.lock().unwrap();
        if !state.active || state.generation != generation {
            return Err(MqttError::Closed);
        }
        self.transport.lock().unwrap().replace_tls(&loaded);
        state.config = Arc::new(config);
        Ok(())
    }
}

/// Factory owns broker memory across component generations, scoped to one
/// Application registration. Construct a new factory for each Application.
pub struct MqttBrokerFactory {
    state: SharedState,
    descriptor: ComponentDescriptor,
    persistence: persistence::Persistence,
    transport: Arc<Mutex<transport::Transport>>,
    tasks: Arc<Mutex<Option<TaskScheduler>>>,
}

impl MqttBrokerFactory {
    /// Validate configuration and construct an unstarted, application-local factory.
    pub fn new(config: MqttBrokerConfig) -> Result<Self, MqttError> {
        config.validate()?;
        let mut descriptor = ComponentDescriptor::new(ComponentRef::module(&config.name));
        if let Some(auth) = &config.auth {
            descriptor
                .requires
                .push(ComponentRef::module(&auth.component));
        }
        if let Some(storage) = &config.storage {
            descriptor
                .requires
                .push(ComponentRef::module(&storage.component));
        }
        descriptor.failure = FailurePolicy::Restart {
            max_attempts: 3,
            backoff: Duration::from_millis(50),
        };
        let state = Arc::new(Mutex::new(state::State::new(Arc::new(config))));
        Ok(Self {
            state,
            descriptor,
            persistence: persistence::Persistence::default(),
            transport: Arc::new(Mutex::new(transport::Transport::default())),
            tasks: Arc::new(Mutex::new(None)),
        })
    }

    /// Obtain a read-only handle without starting or enabling the component.
    pub fn handle(&self) -> BrokerHandle {
        BrokerHandle {
            state: self.state.clone(),
            transport: self.transport.clone(),
            tasks: self.tasks.clone(),
        }
    }

    /// Change supervision before registering; immutable after Application build.
    pub fn with_failure_policy(mut self, policy: FailurePolicy) -> Self {
        self.descriptor.failure = policy;
        self
    }
}

impl ModuleFactory for MqttBrokerFactory {
    fn descriptor(&self) -> ComponentDescriptor {
        self.descriptor.clone()
    }

    fn validate_config(&self, _: &ComponentConfig) -> FrameworkResult<()> {
        self.state
            .lock()
            .unwrap()
            .config
            .validate()
            .map_err(framework)
    }

    fn create(&self, _: &ComponentConfig) -> FrameworkResult<Box<dyn Module>> {
        Ok(Box::new(BrokerModule {
            state: self.state.clone(),
            persistence: self.persistence.clone(),
            descriptor: self.descriptor.clone(),
            socket: None,
            transport: self.transport.clone(),
            tasks: self.tasks.clone(),
            listener: None,
            maintenance: None,
            generation: 0,
            drained: Arc::new(Notify::new()),
        }))
    }
}

struct BrokerModule {
    state: SharedState,
    descriptor: ComponentDescriptor,
    socket: Option<TcpSocket>,
    transport: Arc<Mutex<transport::Transport>>,
    tasks: Arc<Mutex<Option<TaskScheduler>>>,
    listener: Option<TaskHandle>,
    maintenance: Option<TaskHandle>,
    persistence: persistence::Persistence,
    generation: u64,
    drained: Arc<Notify>,
}

#[async_trait]
impl Lifecycle for BrokerModule {
    async fn setup(&mut self, ctx: &SetupContext) -> FrameworkResult<()> {
        self.persistence.set_tasks(ctx.tasks.clone());
        let config = self.state.lock().unwrap().config.clone();
        let transport = transport::Transport::prepare(&config, &ctx.tasks).await?;
        *self.transport.lock().unwrap() = transport;
        *self.tasks.lock().unwrap() = Some(ctx.tasks.clone());
        let mut state = self.state.lock().unwrap();
        state.config.validate().map_err(framework)?;
        self.generation = state.begin_generation().map_err(framework)?;
        let address = state.address.unwrap_or(state.config.listener);
        let socket = if address.is_ipv4() {
            TcpSocket::new_v4()
        } else {
            TcpSocket::new_v6()
        }
        .map_err(framework)?;
        socket.set_reuseaddr(true).map_err(framework)?;
        socket.bind(address).map_err(framework)?;
        state.address = Some(socket.local_addr().map_err(framework)?);
        self.socket = Some(socket);
        Ok(())
    }

    async fn begin_shutdown(&mut self, _: &ShutdownContext) -> FrameworkResult<()> {
        self.state.lock().unwrap().stop(self.generation);
        if let Some(listener) = &self.listener {
            listener.cancel();
        }
        if let Some(task) = &self.maintenance {
            task.cancel();
        }
        Ok(())
    }

    async fn quiesce(&mut self, _: &ShutdownContext) -> FrameworkResult<()> {
        if let Some(listener) = &self.listener {
            // A failure result is already reported to the Supervisor. Waiting for
            // resource release must not re-report it as a cleanup failure.
            let _ = listener.wait_finished().await;
        }
        if let Some(task) = &self.maintenance {
            let _ = task.wait_finished().await;
        }
        loop {
            let notified = self.drained.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.state.lock().unwrap().connections == 0 {
                break;
            }
            notified.await;
        }
        self.persistence.shutdown(&self.state).await;
        Ok(())
    }

    async fn teardown(&mut self, _: &ShutdownContext) -> FrameworkResult<()> {
        self.socket.take();
        self.listener.take();
        self.maintenance.take();
        // The registered Factory retains the same state; never clear it here.
        Ok(())
    }
}

#[async_trait]
impl Module for BrokerModule {
    fn descriptor(&self) -> ComponentDescriptor {
        self.descriptor.clone()
    }

    fn handlers(&mut self) -> FrameworkResult<Option<HandlerRegistration>> {
        let config = self.state.lock().unwrap().config.clone();
        let mut subscriptions: Vec<_> = [STATUS_TOPIC, PUBLISH_TOPIC, PUBLISH_QUERY_TOPIC]
            .into_iter()
            .zip(config.control_subscription_ids)
            .map(|(topic, id)| SubscriptionSpec::new(id, self.descriptor.component(), topic))
            .collect();
        if let Some(admin) = &config.admin {
            subscriptions.push(SubscriptionSpec::new(
                admin.subscription_id,
                self.descriptor.component(),
                ADMIN_CONTROL_TOPIC,
            ));
        }
        Ok(Some(HandlerRegistration::Serial {
            handler: Box::new(ControlHandler {
                state: self.state.clone(),
                persistence: self.persistence.clone(),
                generation: self.generation,
                component: self.descriptor.component(),
            }),
            subscriptions,
        }))
    }

    async fn activate_ingress(&mut self, ctx: &RuntimeContext) -> FrameworkResult<()> {
        #[cfg(test)]
        {
            let mut state = self.state.lock().unwrap();
            if state.fail_activations > 0 {
                state.fail_activations -= 1;
                return Err(framework("injected activation failure"));
            }
        }
        self.persistence
            .recover(&self.state, &ctx.event_hub)
            .await
            .map_err(framework)?;
        // Persist recovery normalization (lost clean sessions/armed Wills) before listen.
        let tx = self
            .persistence
            .begin(&self.state)
            .await
            .map_err(framework)?;
        tx.commit(persistence::Hub::Internal(&ctx.event_hub))
            .await
            .map_err(framework)?;
        let listener = self
            .socket
            .take()
            .ok_or_else(|| framework("socket not prepared"))?
            .listen(128)
            .map_err(framework)?;
        let state = self.state.clone();
        let generation = self.generation;
        let drained = self.drained.clone();
        let tasks = ctx.tasks.clone();
        let hub = ctx.event_hub.external();
        let transport = self.transport.lock().unwrap().clone();
        let persistence = self.persistence.clone();
        let permits = Arc::new(Semaphore::new(state.lock().unwrap().config.max_connections));
        let (started, startup) = tokio::sync::oneshot::channel();
        self.state.lock().unwrap().active = true;
        let maintenance_state = state.clone();
        let maintenance_hub = hub.clone();
        let maintenance_persistence = self.persistence.clone();
        self.maintenance=Some(ctx.tasks.spawn(move|cancel|async move{
            let mut interval=tokio::time::interval(Duration::from_millis(100));interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop{
                tokio::select!{biased;_=cancel.cancelled()=>break,_=interval.tick()=>{}}
                let tx=tokio::select!{biased;_=cancel.cancelled()=>break,result=maintenance_persistence.begin(&maintenance_state)=>result.map_err(framework)?};
                {let mut state=maintenance_state.lock().unwrap();
                    #[cfg(test)] if state.fail_listener{state.fail_listener=false;return Err(framework("injected listener failure"));}
                    state.process_wills();state.prune_peers();}
                tx.commit(persistence::Hub::External(&maintenance_hub)).await.map_err(framework)?;
            }Ok(())
        })?);
        let task = ctx.tasks.spawn(move |cancel| async move {
            let _ = started.send(());
            loop {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => break,
                    result = listener.accept() => {
                        let (stream, peer) = result.map_err(framework)?;
                        let permit = permits.clone().try_acquire_owned();
                        let memory = {
                            let state = state.lock().unwrap();
                            state.budget.reserve(state.config.connection_budget().unwrap())
                        };
                        match (permit, memory) {
                            (Ok(permit), Ok(memory)) => {
                                let admitted = {
                                    let mut state = state.lock().unwrap();
                                    match state.admit_peer(peer.ip()) {
                                        Ok(()) => { state.connections += 1; true }
                                        Err(_) => { state.rejected_connections += 1; false }
                                    }
                                };
                                if !admitted { continue; }
                                let lifetime = ConnectionLifetime { diagnostics: diagnostics::configured(), state: state.clone(), drained: drained.clone(), generation,
                                    attached: None, peer_ip: peer.ip(), graceful: false, _permit: Some(permit), _memory: Some(memory), persistence:persistence.clone() };
                                if connection::spawn(&tasks, stream, lifetime, hub.clone(), transport.clone()).is_err() {
                                    state.lock().unwrap().rejected_connections += 1;
                                }
                            }
                            _ => { state.lock().unwrap().rejected_connections += 1; }
                        }
                    }
                }
            }
            Ok(())
        });
        match task {
            Ok(task) => self.listener = Some(task),
            Err(error) => {
                self.state.lock().unwrap().stop(generation);
                return Err(error);
            }
        }
        startup.await.map_err(framework)?;
        Ok(())
    }
}

struct ControlHandler {
    state: SharedState,
    generation: u64,
    component: ComponentRef,
    persistence: persistence::Persistence,
}

#[async_trait]
impl SerialHandler for ControlHandler {
    async fn handle(
        &mut self,
        ctx: &HandlerContext,
        message: MessageEnvelope,
    ) -> FrameworkResult<HandlerReply> {
        if message.kind != MessageKind::Request {
            // A Command admission never implies broker acceptance. Events and
            // Commands cannot mutate the broker, even with a correctly typed DTO.
            // Non-Request replies are ignored. Reject locally without reporting
            // a component failure to the Supervisor for an invalid caller.
            return Ok(Arc::new(()));
        }
        match message.topic.as_str() {
            STATUS_TOPIC => {
                message.payload::<()>()?;
                Ok(Arc::new(self.state.lock().unwrap().status()))
            }
            ADMIN_CONTROL_TOPIC => {
                let request = message.payload::<AdminRequest>()?;
                let mut state = self.state.lock().unwrap();
                let allowed = message.destination == Destination::Component(self.component.clone())
                    && state.config.admin.as_ref().is_some_and(|a| {
                        message
                            .source
                            .as_ref()
                            .is_some_and(|s| a.allowed_sources.contains(s))
                    });
                let result = if allowed {
                    state.admin(self.generation, &request)
                } else {
                    Err(AdminError::NotAuthorized)
                };
                Ok(Arc::new(result))
            }
            PUBLISH_TOPIC => {
                let request = message.payload::<MqttPublishRequest>()?;
                if self.state.lock().unwrap().generation != self.generation {
                    return Ok(Arc::new(Err::<MqttAcceptReceipt, _>(
                        MqttPublishRejection::Inactive,
                    )));
                }
                let result = match message.source.as_ref() {
                    Some(source)
                        if message.destination
                            == Destination::Component(self.component.clone()) =>
                    {
                        if let Some(result) = self.state.lock().unwrap().recorded_publication(
                            self.generation,
                            source,
                            &request,
                        ) {
                            return Ok(Arc::new(result));
                        }
                        let config = self.state.lock().unwrap().config.clone();
                        let allowed =
                            auth::authorize_internal(&ctx.event_hub, &config, source, &request)
                                .await;
                        match allowed {
                            Ok(()) => {
                                let tx = self
                                    .persistence
                                    .begin(&self.state)
                                    .await
                                    .map_err(framework)?;
                                let result = self.state.lock().unwrap().publish_internal(
                                    self.generation,
                                    source,
                                    &request,
                                );
                                tx.commit(persistence::Hub::Internal(&ctx.event_hub))
                                    .await
                                    .map_err(framework)?;
                                result
                            }
                            Err(_) => Err(MqttPublishRejection::NotAuthorized),
                        }
                    }
                    _ => Err(MqttPublishRejection::UntrustedSource),
                };
                Ok(Arc::new(result))
            }
            PUBLISH_QUERY_TOPIC => {
                let query = message.payload::<MqttPublishQuery>()?;
                if self.state.lock().unwrap().generation != self.generation {
                    return Ok(Arc::new(Err::<Option<MqttPublishResult>, _>(
                        MqttPublishRejection::Inactive,
                    )));
                }
                let result = match message.source.as_ref() {
                    Some(source)
                        if message.destination
                            == Destination::Component(self.component.clone()) =>
                    {
                        let _gate = self
                            .persistence
                            .begin(&self.state)
                            .await
                            .map_err(framework)?;
                        // Read under the transaction gate and expose the result only
                        // after any deferred disconnects have been committed.
                        let result = self.state.lock().unwrap().query_publication(
                            self.generation,
                            source,
                            &query.request_id,
                        );
                        _gate
                            .commit(persistence::Hub::Internal(&ctx.event_hub))
                            .await
                            .map_err(framework)?;
                        result
                    }
                    _ => Err(MqttPublishRejection::UntrustedSource),
                };
                Ok(Arc::new(result))
            }
            _ => Err(framework("unknown MQTT control topic")),
        }
    }
}

#[cfg(test)]
mod tests;
