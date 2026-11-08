//! Opt-in loopback HTTP management; all mutations pass through the Admin Handler.
use crate::MetricsHandle;
use modorch::{prelude::*, *};
use modorch_mqtt_api::*;
use sha2::{Digest, Sha256};
use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpSocket, TcpStream},
    sync::Semaphore,
};

/// Explicit HTTP endpoint. Use a trusted TLS reverse proxy for remote access.
#[derive(Clone)]
pub struct HttpConfig {
    /// Unique Module name, independent from Core/Admin/Metrics.
    pub name: String,
    /// Existing Admin Module; its allowed_callers must include this Module.
    pub admin: String,
    /// Loopback bind address, with port0 for automatic allocation.
    pub listener: SocketAddr,
    /// Application-global private forwarding subscription ID.
    pub subscription_id: u64,
    /// Bearer secret, 32..256 printable ASCII bytes, never returned in diagnostics.
    pub bearer_token: String,
    /// Concurrent accepted requests, 1..128; additional connections are closed.
    pub max_connections: usize,
    /// Absolute per-request read/forward/write deadline, at most30s.
    pub timeout: Duration,
}
impl std::fmt::Debug for HttpConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpConfig")
            .field("name", &self.name)
            .field("listener", &self.listener)
            .field("bearer_token", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}
/// Read-only listener observation across Module generations.
#[derive(Clone)]
pub struct HttpHandle(Arc<Mutex<Option<SocketAddr>>>);
impl HttpHandle {
    /// Bound address after setup; readiness still depends on Application lifecycle.
    pub fn local_addr(&self) -> Option<SocketAddr> {
        *self.0.lock().unwrap()
    }
}
/// Independent optional ingress Factory. No IO occurs in construction/registration.
pub struct HttpFactory {
    config: Arc<HttpConfig>,
    metrics: Option<MetricsHandle>,
    handle: HttpHandle,
    capability: Arc<()>,
}
impl HttpFactory {
    /// Validate explicit loopback exposure, secret, request and connection limits.
    pub fn new(config: HttpConfig, metrics: Option<MetricsHandle>) -> Result<Self, MqttError> {
        if [&config.name, &config.admin]
            .iter()
            .any(|s| s.is_empty() || s.len() > 128 || s.contains(['/', '\0']))
            || config.name == config.admin
            || !config.listener.ip().is_loopback()
            || !(32..=256).contains(&config.bearer_token.len())
            || !config.bearer_token.bytes().all(|b| (33..=126).contains(&b))
            || !(1..=128).contains(&config.max_connections)
            || config.timeout.is_zero()
            || config.timeout > Duration::from_secs(30)
        {
            return Err(MqttError::Configuration(
                "invalid HTTP endpoint or bearer configuration".into(),
            ));
        }
        Ok(Self {
            config: Arc::new(config),
            metrics,
            handle: HttpHandle(Arc::new(Mutex::new(None))),
            capability: Arc::new(()),
        })
    }
    /// Observe the ephemeral bound port without enabling the Module.
    pub fn handle(&self) -> HttpHandle {
        self.handle.clone()
    }
}
fn descriptor(c: &HttpConfig) -> ComponentDescriptor {
    let mut d = ComponentDescriptor::new(ComponentRef::module(&c.name));
    d.requires.push(ComponentRef::module(&c.admin));
    d.queue_capacity = c.max_connections;
    d
}
impl ModuleFactory for HttpFactory {
    fn descriptor(&self) -> ComponentDescriptor {
        descriptor(&self.config)
    }
    fn create(&self, _: &ComponentConfig) -> FrameworkResult<Box<dyn Module>> {
        Ok(Box::new(HttpModule {
            config: self.config.clone(),
            metrics: self.metrics.clone(),
            handle: self.handle.clone(),
            capability: self.capability.clone(),
            socket: None,
            listener: None,
            permits: Arc::new(Semaphore::new(self.config.max_connections)),
        }))
    }
}
struct HttpModule {
    config: Arc<HttpConfig>,
    metrics: Option<MetricsHandle>,
    handle: HttpHandle,
    capability: Arc<()>,
    socket: Option<TcpSocket>,
    listener: Option<TaskHandle>,
    permits: Arc<Semaphore>,
}
#[async_trait]
impl Lifecycle for HttpModule {
    async fn setup(&mut self, _: &SetupContext) -> FrameworkResult<()> {
        let socket = if self.config.listener.is_ipv4() {
            TcpSocket::new_v4()
        } else {
            TcpSocket::new_v6()
        }
        .map_err(failure)?;
        socket.set_reuseaddr(true).map_err(failure)?;
        socket.bind(self.config.listener).map_err(failure)?;
        *self.handle.0.lock().unwrap() = Some(socket.local_addr().map_err(failure)?);
        self.socket = Some(socket);
        Ok(())
    }
    async fn begin_shutdown(&mut self, _: &ShutdownContext) -> FrameworkResult<()> {
        if let Some(task) = &self.listener {
            task.cancel();
        }
        Ok(())
    }
    async fn quiesce(&mut self, _: &ShutdownContext) -> FrameworkResult<()> {
        if let Some(task) = &self.listener {
            let _ = task.wait_finished().await;
        }
        let _all = self
            .permits
            .clone()
            .acquire_many_owned(self.config.max_connections as u32)
            .await
            .map_err(failure)?;
        Ok(())
    }
    async fn teardown(&mut self, _: &ShutdownContext) -> FrameworkResult<()> {
        self.socket.take();
        self.listener.take();
        Ok(())
    }
}
fn failure(e: impl std::fmt::Display) -> FrameworkError {
    FrameworkError::InvalidConfig(e.to_string())
}
struct Forward {
    capability: Arc<()>,
    request: AdminRequest,
}
struct ForwardHandler {
    config: Arc<HttpConfig>,
    capability: Arc<()>,
}
#[async_trait]
impl SerialHandler for ForwardHandler {
    async fn handle(
        &mut self,
        ctx: &HandlerContext,
        message: MessageEnvelope,
    ) -> FrameworkResult<HandlerReply> {
        if message.kind != MessageKind::Request
            || message.destination != Destination::Component(ctx.component.clone())
        {
            return Ok(Arc::new(Err::<AdminReply, _>(AdminError::NotAuthorized)));
        }
        let forward = message.payload::<Forward>()?;
        if !Arc::ptr_eq(&forward.capability, &self.capability) {
            return Ok(Arc::new(Err::<AdminReply, _>(AdminError::NotAuthorized)));
        }
        Ok(ctx
            .event_hub
            .request::<_, AdminResult>(
                ADMIN_TOPIC,
                Destination::Component(ComponentRef::module(&self.config.admin)),
                forward.request.clone(),
                MessageOptions {
                    payload_bytes: Some(2048),
                    ..Default::default()
                },
            )?
            .wait(self.config.timeout)
            .await?)
    }
}
#[async_trait]
impl Module for HttpModule {
    fn descriptor(&self) -> ComponentDescriptor {
        descriptor(&self.config)
    }
    fn handlers(&mut self) -> FrameworkResult<Option<HandlerRegistration>> {
        Ok(Some(HandlerRegistration::Serial {
            handler: Box::new(ForwardHandler {
                config: self.config.clone(),
                capability: self.capability.clone(),
            }),
            subscriptions: vec![SubscriptionSpec::new(
                self.config.subscription_id,
                self.descriptor().component(),
                format!("$modorch/mqtt/http/{}", self.config.name),
            )],
        }))
    }
    async fn activate_ingress(&mut self, ctx: &RuntimeContext) -> FrameworkResult<()> {
        let listener = self
            .socket
            .take()
            .ok_or_else(|| failure("HTTP socket not prepared"))?
            .listen(128)
            .map_err(failure)?;
        let tasks = ctx.tasks.clone();
        let hub = ctx.event_hub.external();
        let config = self.config.clone();
        let metrics = self.metrics.clone();
        let capability = self.capability.clone();
        let permits = self.permits.clone();
        self.listener=Some(ctx.tasks.spawn(move|cancel|async move{
            loop{
                let (socket,_)=tokio::select!{biased;_=cancel.cancelled()=>break,result=listener.accept()=>result.map_err(failure)?};
                let Ok(permit)=permits.clone().try_acquire_owned()else{continue};
                let (hub,config,metrics,capability,parent)=(hub.clone(),config.clone(),metrics.clone(),capability.clone(),cancel.clone());
                let _ = tasks.spawn(move|local|async move{
                    let _permit=permit;
                    tokio::select!{biased;_=parent.cancelled()=>(),_=local.cancelled()=>(),_=tokio::time::timeout(config.timeout,serve(socket,&config,metrics.as_ref(),hub,capability))=>()};Ok(())
                });
            }Ok(())
        })?);
        Ok(())
    }
}
fn decode(value: &str) -> Option<String> {
    let bytes = value.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let pair = std::str::from_utf8(bytes.get(i + 1..i + 3)?).ok()?;
            output.push(u8::from_str_radix(pair, 16).ok()?);
            i += 3;
        } else {
            output.push(bytes[i]);
            i += 1;
        }
    }
    let text = String::from_utf8(output).ok()?;
    (!text.contains('\0') && text.len() <= 1024).then_some(text)
}
fn action(method: &str, uri: &str) -> Option<AdminRequest> {
    let (path, query) = uri.split_once('?').unwrap_or((uri, ""));
    let mut args = std::collections::BTreeMap::new();
    for field in query.split('&').filter(|s| !s.is_empty()) {
        let (key, value) = field.split_once('=')?;
        if args.insert(key, decode(value)?).is_some() {
            return None;
        }
    }
    let after = args.remove("after");
    let limit = match args.remove("limit") {
        Some(v) => v.parse().ok()?,
        None => 100,
    };
    let client = args.remove("client_id");
    let connection = args.remove("connection_id");
    if !args.is_empty() || !(1..=256).contains(&limit) {
        return None;
    }
    match (method, path) {
        ("GET", "/v1/stats") if client.is_none() && connection.is_none() && after.is_none() => {
            Some(AdminRequest::BrokerStats)
        }
        ("GET", "/v1/clients") if client.is_none() && connection.is_none() => {
            Some(AdminRequest::ListClients { after, limit })
        }
        ("GET", "/v1/session") if after.is_none() && connection.is_none() => {
            Some(AdminRequest::GetSession { client_id: client? })
        }
        ("GET", "/v1/subscriptions") if connection.is_none() => {
            Some(AdminRequest::ListSubscriptions {
                client_id: client?,
                after,
                limit,
            })
        }
        ("POST", "/v1/disconnect") if after.is_none() => Some(AdminRequest::DisconnectClient {
            client_id: client?,
            expected_connection_id: connection?.parse().ok()?,
        }),
        _ => None,
    }
}
async fn serve(
    mut socket: TcpStream,
    config: &HttpConfig,
    metrics: Option<&MetricsHandle>,
    hub: ExternalEventHub,
    capability: Arc<()>,
) -> std::io::Result<()> {
    let mut bytes = Vec::with_capacity(8192);
    let mut read = [0; 1024];
    let response = loop {
        if bytes.len() >= 8192 {
            break (
                431,
                "application/json",
                b"{\"error\":\"headers too large\"}".to_vec(),
            );
        }
        let n = socket
            .read(&mut read[..1024.min(8192 - bytes.len())])
            .await?;
        if n == 0 {
            return Ok(());
        }
        bytes.extend_from_slice(&read[..n]);
        let mut headers = [httparse::EMPTY_HEADER; 32];
        let mut request = httparse::Request::new(&mut headers);
        let length = match request.parse(&bytes) {
            Ok(httparse::Status::Partial) => continue,
            Ok(httparse::Status::Complete(n)) => n,
            Err(_) => break (400, "application/json", b"{}".to_vec()),
        };
        let auth: Vec<_> = request
            .headers
            .iter()
            .filter(|h| h.name.eq_ignore_ascii_case("authorization"))
            .collect();
        let expected = Sha256::digest(format!("Bearer {}", config.bearer_token).as_bytes());
        let valid = auth.len() == 1
            && Sha256::digest(auth[0].value)
                .iter()
                .zip(expected.iter())
                .fold(0u8, |v, (a, b)| v | (a ^ b))
                == 0;
        if !valid {
            break (
                401,
                "application/json",
                b"{\"error\":\"unauthorized\"}".to_vec(),
            );
        }
        if length != bytes.len()
            || request.headers.iter().any(|h| {
                h.name.eq_ignore_ascii_case("transfer-encoding")
                    || (h.name.eq_ignore_ascii_case("content-length") && h.value != b"0")
            })
        {
            break (400, "application/json", b"{}".to_vec());
        }
        let (method, path) = (request.method.unwrap_or(""), request.path.unwrap_or(""));
        if method == "GET" && path == "/metrics" {
            break match metrics {
                Some(m) => (
                    200,
                    "text/plain; version=0.0.4",
                    m.prometheus().into_bytes(),
                ),
                None => (404, "application/json", b"{}".to_vec()),
            };
        }
        let Some(request) = action(method, path) else {
            break (400, "application/json", b"{}".to_vec());
        };
        let result = async {
            hub.request::<_, AdminResult>(
                &format!("$modorch/mqtt/http/{}", config.name),
                Destination::Component(ComponentRef::module(&config.name)),
                Forward {
                    capability,
                    request,
                },
                MessageOptions {
                    payload_bytes: Some(2048),
                    ..Default::default()
                },
            )?
            .wait(config.timeout)
            .await
        }
        .await;
        let result = match result {
            Ok(r) => r,
            Err(_) => {
                break (
                    503,
                    "application/json",
                    b"{\"error\":\"unavailable or indeterminate\"}".to_vec(),
                );
            }
        };
        #[derive(serde::Serialize)]
        struct Page<'a, T> {
            items: &'a [T],
            next: &'a Option<String>,
        }
        #[derive(serde::Serialize)]
        struct Session<'a> {
            session: &'a Option<SessionInfo>,
        }
        #[derive(serde::Serialize)]
        struct Disconnected {
            disconnected: bool,
        }
        let mut output = LimitedJson(Vec::with_capacity(4096));
        let encoded = match result.as_ref() {
            Ok(AdminReply::Clients { items, next }) => {
                serde_json::to_writer(&mut output, &Page { items, next })
            }
            Ok(AdminReply::Session(session)) => {
                serde_json::to_writer(&mut output, &Session { session })
            }
            Ok(AdminReply::Subscriptions { items, next }) => {
                serde_json::to_writer(&mut output, &Page { items, next })
            }
            Ok(AdminReply::Disconnected(value)) => serde_json::to_writer(
                &mut output,
                &Disconnected {
                    disconnected: *value,
                },
            ),
            Ok(AdminReply::Stats(value)) => serde_json::to_writer(&mut output, value),
            Err(AdminError::NotAuthorized) => break (403, "application/json", b"{}".to_vec()),
            Err(AdminError::InvalidRequest) => break (400, "application/json", b"{}".to_vec()),
            Err(AdminError::Unavailable) => break (503, "application/json", b"{}".to_vec()),
        };
        if encoded.is_err() {
            break (
                413,
                "application/json",
                b"{\"error\":\"response too large; reduce page size\"}".to_vec(),
            );
        }
        break (200, "application/json", output.0);
    };
    let (status, kind, body) = response;
    socket.write_all(format!("HTTP/1.1 {status} response\r\nContent-Type: {kind}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",body.len()).as_bytes()).await?;
    socket.write_all(&body).await?;
    socket.shutdown().await
}

// No intermediate JSON Value tree or unbounded string allocation.
struct LimitedJson(Vec<u8>);
impl std::io::Write for LimitedJson {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        if self.0.len().saturating_add(b.len()) > 1048576 {
            return Err(std::io::Error::other("HTTP response bound"));
        }
        self.0.extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
