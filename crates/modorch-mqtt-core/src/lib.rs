//! Modorch module for an experimental MQTT 3.1.1 QoS0/1 memory broker.
//! The host owns Application, EventHub and Tokio. No background task is detached.
#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod budget;
mod codec;
mod config;
mod connection;
mod state;

pub use config::MqttBrokerConfig;
use connection::{ConnectionLifetime, SharedState};
use modorch::{prelude::*, *};
use modorch_mqtt_api::{BrokerStatus, MqttError};
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

fn framework(error: impl std::fmt::Display) -> FrameworkError {
    FrameworkError::InvalidConfig(error.to_string())
}

/// Read-only local observation handle; cloning it does not start a runtime/Hub.
#[derive(Clone)]
pub struct BrokerHandle {
    state: SharedState,
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
}

/// Factory owns broker memory across component generations, scoped to one
/// Application registration. Construct a new factory for each Application.
pub struct MqttBrokerFactory {
    state: SharedState,
    descriptor: ComponentDescriptor,
}

impl MqttBrokerFactory {
    /// Validate configuration and construct an unstarted, application-local factory.
    pub fn new(config: MqttBrokerConfig) -> Result<Self, MqttError> {
        config.validate()?;
        let mut descriptor = ComponentDescriptor::new(ComponentRef::module(&config.name));
        descriptor.failure = FailurePolicy::Restart {
            max_attempts: 3,
            backoff: Duration::from_millis(50),
        };
        let state = Arc::new(Mutex::new(state::State::new(Arc::new(config))));
        Ok(Self { state, descriptor })
    }

    /// Obtain a read-only handle without starting or enabling the component.
    pub fn handle(&self) -> BrokerHandle {
        BrokerHandle {
            state: self.state.clone(),
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
            descriptor: self.descriptor.clone(),
            socket: None,
            listener: None,
            generation: 0,
            drained: Arc::new(Notify::new()),
        }))
    }
}

struct BrokerModule {
    state: SharedState,
    descriptor: ComponentDescriptor,
    socket: Option<TcpSocket>,
    listener: Option<TaskHandle>,
    generation: u64,
    drained: Arc<Notify>,
}

#[async_trait]
impl Lifecycle for BrokerModule {
    async fn setup(&mut self, _: &SetupContext) -> FrameworkResult<()> {
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
        Ok(())
    }

    async fn quiesce(&mut self, _: &ShutdownContext) -> FrameworkResult<()> {
        if let Some(listener) = &self.listener {
            // A failure result is already reported to the Supervisor. Waiting for
            // resource release must not re-report it as a cleanup failure.
            let _ = listener.wait_finished().await;
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
        self.state.lock().unwrap().process_wills();
        Ok(())
    }

    async fn teardown(&mut self, _: &ShutdownContext) -> FrameworkResult<()> {
        self.socket.take();
        self.listener.take();
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
        Ok(Some(HandlerRegistration::Serial {
            handler: Box::new(StatusHandler(self.state.clone())),
            subscriptions: vec![SubscriptionSpec::new(
                1,
                self.descriptor.component(),
                STATUS_TOPIC,
            )],
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
        let permits = Arc::new(Semaphore::new(state.lock().unwrap().config.max_connections));
        let (started, startup) = tokio::sync::oneshot::channel();
        self.state.lock().unwrap().active = true;
        let task = ctx.tasks.spawn(move |cancel| async move {
            let _ = started.send(());
            let mut maintenance = tokio::time::interval(Duration::from_millis(100));
            maintenance.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => break,
                    _ = maintenance.tick() => {
                        let mut state = state.lock().unwrap();
                        #[cfg(test)]
                        if state.fail_listener { state.fail_listener = false; return Err(framework("injected listener failure")); }
                        state.process_wills();
                    }
                    result = listener.accept() => {
                        let (stream, _) = result.map_err(framework)?;
                        let permit = permits.clone().try_acquire_owned();
                        let memory = {
                            let state = state.lock().unwrap();
                            state.budget.reserve(state.config.connection_budget().unwrap())
                        };
                        match (permit, memory) {
                            (Ok(permit), Ok(memory)) => {
                                state.lock().unwrap().connections += 1;
                                let lifetime = ConnectionLifetime { state: state.clone(), drained: drained.clone(), generation,
                                    attached: None, graceful: false, _permit: permit, _memory: memory };
                                if connection::spawn(&tasks, stream, lifetime).is_err() {
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

struct StatusHandler(SharedState);

#[async_trait]
impl SerialHandler for StatusHandler {
    async fn handle(
        &mut self,
        _: &HandlerContext,
        message: MessageEnvelope,
    ) -> FrameworkResult<HandlerReply> {
        if message.kind != MessageKind::Request {
            return Err(framework("status requires a Request"));
        }
        message.payload::<()>()?;
        Ok(Arc::new(self.0.lock().unwrap().status()))
    }
}

#[cfg(test)]
mod tests;
