//! Explicit MQTT5 network Bridge. Each link has one reliable transfer in flight.
//! Source PUBACK follows destination PUBACK; broker persistence provides replay.
//! A crash between these ACKs can duplicate delivery. No application Event is exported.
#![forbid(unsafe_code)]
#![deny(missing_docs)]
use modorch::*;
use modorch_mqtt_api::{BRIDGE_HOPS_PROPERTY as HOPS, BRIDGE_ORIGIN_PROPERTY as ORIGIN, MqttError};
use rumqttc::v5::{
    AsyncClient, Event, EventLoop, MqttOptions,
    mqttbytes::{
        QoS,
        v5::{Packet, PubAckReason, Publish, PublishProperties, SubscribeReasonCode},
    },
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

/// MQTT5 endpoint with explicit credentials. Debug never prints credentials.
#[derive(Clone)]
pub struct BridgeEndpoint {
    /// DNS name or IP address; TLS termination may be provided by an explicit local proxy.
    pub host: String,
    /// TCP port.
    pub port: u16,
    /// Optional username/password, never inferred from the process environment.
    pub credentials: Option<(String, String)>,
}
impl std::fmt::Debug for BridgeEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BridgeEndpoint")
            .field("host", &self.host)
            .field("port", &self.port)
            .field(
                "credentials",
                &self.credentials.as_ref().map(|_| "[redacted]"),
            )
            .finish()
    }
}
/// One explicit direction. Use two links for bidirectional routing.
#[derive(Clone, Debug)]
pub struct BridgeLink {
    /// Source MQTT5 broker. Its ACL must grant only the configured subscription.
    pub source: BridgeEndpoint,
    /// Destination MQTT5 broker. Its ACL must grant only the mapped topic prefix.
    pub destination: BridgeEndpoint,
    /// Nonempty literal source prefix ending in `/`; subscribes to `prefix#`.
    pub source_prefix: String,
    /// Nonempty literal destination prefix ending in `/`.
    pub destination_prefix: String,
}
/// Bounded, explicit network bridge configuration.
#[derive(Clone, Debug)]
pub struct BridgeConfig {
    /// Globally stable bridge identity, also the Module name and MQTT client ID prefix.
    pub name: String,
    /// Explicit local Core dependency (both endpoints can otherwise be remote).
    pub broker: String,
    /// One to sixteen directions; client IDs include the stable link index.
    pub links: Vec<BridgeLink>,
    /// MQTT maximum packet size and per-link payload bound, 1KiB..1MiB.
    pub max_packet_bytes: usize,
    /// Source persistent-session retention, 1..604800 seconds.
    pub session_expiry_seconds: u32,
    /// Maximum forwarding hops, 1..32. Origin/hop metadata are reserved to the Bridge.
    pub max_hops: u8,
    /// Reconnect delay, 10ms..60s; one reconnect attempt per link at a time.
    pub reconnect_delay: Duration,
}
/// Low-cardinality aggregate progress; no topics, payloads or credentials.
#[derive(Clone, Debug, Default)]
pub struct BridgeStatus {
    /// Destination PUBACKs received successfully.
    pub forwarded: u64,
    /// Origin/hop guarded messages acknowledged without forwarding.
    pub loop_filtered: u64,
    /// Publications whose expiry elapsed before forwarding began.
    pub expired: u64,
    /// Best-effort messages dropped while a reliable transfer occupies the link.
    pub dropped_qos0: u64,
    /// Failed polls, rejected subscriptions or destination acknowledgements.
    pub failures: u64,
    /// Currently owned source publications, at most the configured number of links.
    pub pending: usize,
}
/// Read-only application-scoped progress handle.
#[derive(Clone)]
pub struct BridgeHandle(Arc<Mutex<BridgeStatus>>);
impl BridgeHandle {
    /// Copy aggregate counters.
    pub fn status(&self) -> BridgeStatus {
        self.0.lock().unwrap().clone()
    }
}
/// Explicit Module factory. Creating/registering it opens no network connection.
pub struct BridgeFactory {
    config: Arc<BridgeConfig>,
    handle: BridgeHandle,
}
fn valid(s: &str) -> bool {
    !s.is_empty() && s.len() <= 128 && !s.contains(['\0', '/', '+', '#'])
}
fn prefix(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 512
        && s.ends_with('/')
        && !s.contains(['\0', '+', '#'])
        && !s.starts_with("$modorch")
}
impl BridgeFactory {
    /// Validate fixed bounded resources and explicit mappings.
    pub fn new(config: BridgeConfig) -> Result<Self, MqttError> {
        if config.name.len() > 120
            || !valid(&config.name)
            || !valid(&config.broker)
            || config.name == config.broker
            || !(1..=16).contains(&config.links.len())
            || !(1024..=1048576).contains(&config.max_packet_bytes)
            || !(1..=604800).contains(&config.session_expiry_seconds)
            || !(1..=32).contains(&config.max_hops)
            || config.reconnect_delay < Duration::from_millis(10)
            || config.reconnect_delay > Duration::from_secs(60)
            || config.links.iter().any(|l| {
                !prefix(&l.source_prefix)
                    || !prefix(&l.destination_prefix)
                    || [&l.source, &l.destination].iter().any(|e| {
                        e.host.is_empty()
                            || e.host.len() > 253
                            || e.host.contains(['\0', '/'])
                            || e.port == 0
                            || e.credentials.as_ref().is_some_and(|(u, p)| {
                                u.len() > 1024 || p.len() > 1024 || u.contains('\0')
                            })
                    })
            })
        {
            return Err(MqttError::Configuration(
                "invalid Bridge configuration".into(),
            ));
        }
        Ok(Self {
            config: Arc::new(config),
            handle: BridgeHandle(Arc::new(Mutex::new(BridgeStatus::default()))),
        })
    }
    /// Obtain progress without enabling the Module.
    pub fn handle(&self) -> BridgeHandle {
        self.handle.clone()
    }
}
fn descriptor(config: &BridgeConfig) -> ComponentDescriptor {
    let mut d = ComponentDescriptor::new(ComponentRef::module(&config.name));
    d.requires.push(ComponentRef::module(&config.broker));
    d
}
impl ModuleFactory for BridgeFactory {
    fn descriptor(&self) -> ComponentDescriptor {
        descriptor(&self.config)
    }
    fn create(&self, _: &ComponentConfig) -> FrameworkResult<Box<dyn Module>> {
        Ok(Box::new(Bridge {
            config: self.config.clone(),
            handle: self.handle.clone(),
            drained: Arc::new(tokio::sync::Notify::new()),
            cancel: CancellationToken::new(),
            active: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }))
    }
}
struct Bridge {
    config: Arc<BridgeConfig>,
    handle: BridgeHandle,
    drained: Arc<tokio::sync::Notify>,
    cancel: CancellationToken,
    active: Arc<std::sync::atomic::AtomicUsize>,
}
#[async_trait]
impl Lifecycle for Bridge {
    async fn quiesce(&mut self, _: &ShutdownContext) -> FrameworkResult<()> {
        self.cancel.cancel();
        while self.active.load(std::sync::atomic::Ordering::SeqCst) != 0 {
            self.drained.notified().await;
        }
        Ok(())
    }
}
#[async_trait]
impl Module for Bridge {
    fn descriptor(&self) -> ComponentDescriptor {
        descriptor(&self.config)
    }
    async fn activate_ingress(&mut self, ctx: &RuntimeContext) -> FrameworkResult<()> {
        for (index, link) in self.config.links.iter().cloned().enumerate() {
            let config = self.config.clone();
            let handle = self.handle.clone();
            let cancel = self.cancel.clone();
            let drained = self.drained.clone();
            let active = self.active.clone();
            active.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            struct Exit(
                Arc<std::sync::atomic::AtomicUsize>,
                Arc<tokio::sync::Notify>,
            );
            impl Drop for Exit {
                fn drop(&mut self) {
                    self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                    self.1.notify_one();
                }
            }
            let exit = Exit(active, drained);
            let result=ctx.tasks.spawn(move|task|async move{
                let _exit=exit;
                tokio::select!{biased;_=task.cancelled()=>{},_=cancel.cancelled()=>{},_=run(config,index,link,handle)=>{}}Ok(())
            });
            result?;
        }
        Ok(())
    }
}
fn client(
    config: &BridgeConfig,
    index: usize,
    endpoint: &BridgeEndpoint,
    source: bool,
) -> (AsyncClient, EventLoop) {
    let id = format!(
        "{}-{index}-{}",
        config.name,
        if source { "src" } else { "dst" }
    );
    let mut o = MqttOptions::new(id, endpoint.host.clone(), endpoint.port);
    o.set_clean_start(false)
        .set_session_expiry_interval(Some(config.session_expiry_seconds))
        .set_keep_alive(Duration::from_secs(5))
        .set_manual_acks(true)
        .set_receive_maximum(Some(1))
        .set_max_packet_size(Some(config.max_packet_bytes as u32))
        .set_connection_timeout(3);
    if let Some((u, p)) = &endpoint.credentials {
        o.set_credentials(u, p);
    }
    AsyncClient::new(o, 2)
}
fn mapped(
    config: &BridgeConfig,
    link: &BridgeLink,
    p: &Publish,
) -> Option<(String, PublishProperties)> {
    let topic = std::str::from_utf8(&p.topic).ok()?;
    let tail = topic.strip_prefix(&link.source_prefix)?;
    let mut props = p.properties.clone().unwrap_or_default();
    let mut origin = None;
    let mut hops = None;
    for (k, v) in &props.user_properties {
        if k == ORIGIN && origin.replace(v.clone()).is_some() {
            return None;
        }
        if k == HOPS && hops.replace(v.parse::<u8>().ok()?).is_some() {
            return None;
        }
    }
    let hop = hops.unwrap_or(0);
    if origin.as_deref() == Some(&config.name) || hop >= config.max_hops {
        return None;
    }
    props
        .user_properties
        .retain(|(k, _)| k != ORIGIN && k != HOPS);
    props
        .user_properties
        .push((ORIGIN.into(), origin.unwrap_or_else(|| config.name.clone())));
    props
        .user_properties
        .push((HOPS.into(), (hop + 1).to_string()));
    props.topic_alias = None;
    props.subscription_identifiers.clear();
    Some((format!("{}{tail}", link.destination_prefix), props))
}
async fn poll_after(
    mut event_loop: EventLoop,
    retry: tokio::time::Instant,
) -> (EventLoop, Result<Event, rumqttc::v5::ConnectionError>) {
    // A read can decode PUBACK and then encounter EOF in the same batch.
    // Deliver already-decoded events before trying to reconnect, otherwise an
    // unavailable remote could hide an acknowledgement we already received.
    if let Some(event) = event_loop.state.events.pop_front() {
        return (event_loop, Ok(event));
    }
    tokio::time::sleep_until(retry).await;
    let event = event_loop.poll().await;
    if matches!(&event, Err(rumqttc::v5::ConnectionError::MqttState(error))
        if !matches!(error, rumqttc::v5::StateError::Io(_) | rumqttc::v5::StateError::ConnectionAborted))
    {
        // The client queues incoming packets before validating their state.
        // Never surface an unsolicited ACK or another rejected packet as a
        // successful application event. Transport EOF alone preserves valid ACKs.
        event_loop.state.events.clear();
    }
    (event_loop, event)
}
async fn run(config: Arc<BridgeConfig>, index: usize, link: BridgeLink, handle: BridgeHandle) {
    let (source, input) = client(&config, index, &link.source, true);
    let (destination, output) = client(&config, index, &link.destination, false);
    struct Transfer {
        publication: Publish,
        // Packet Identifiers are only meaningful within this source Session.
        source_session: u64,
        received: tokio::time::Instant,
        submitted: bool,
        complete: bool,
    }
    struct Owned(BridgeHandle, bool);
    impl Drop for Owned {
        fn drop(&mut self) {
            if self.1 {
                self.0.0.lock().unwrap().pending -= 1;
            }
        }
    }
    let mut owned = Owned(handle.clone(), false);
    let mut pending: Option<Transfer> = None;
    let mut connected = false;
    let mut source_connected = false;
    let mut source_session = 0u64;
    let mut subscribe_pending = false;
    let mut source_retry = tokio::time::Instant::now();
    let mut destination_retry = source_retry;
    let mut publication_retry = source_retry;
    // A poll may own an incomplete CONNECT or network write. Preserve both
    // futures across select iterations; progress on one side must not cancel
    // the other side's handshake or partially processed request.
    let mut input_poll = Box::pin(poll_after(input, source_retry));
    let mut output_poll = Box::pin(poll_after(output, destination_retry));
    loop {
        if pending.as_ref().is_some_and(|p| {
            p.complete
                && source_connected
                && (p.source_session != source_session || source.try_ack(&p.publication).is_ok())
        }) {
            pending = None;
            owned.1 = false;
            handle.0.lock().unwrap().pending -= 1;
        }
        // A new source Session has no subscription yet. Delay its subscription
        // until the old target exchange finishes, keeping one bounded Transfer.
        // A resumed Session can replay its existing subscription immediately.
        if source_connected
            && subscribe_pending
            && pending
                .as_ref()
                .is_none_or(|p| p.source_session == source_session)
            && source
                .try_subscribe(format!("{}#", link.source_prefix), QoS::AtLeastOnce)
                .is_ok()
        {
            subscribe_pending = false;
        }
        if let Some(p) = pending.as_mut().filter(|p| {
            !p.complete
                && !p.submitted
                && connected
                && tokio::time::Instant::now() >= publication_retry
        }) {
            if let Some((topic, mut props)) = mapped(&config, &link, &p.publication) {
                if let Some(seconds) = props.message_expiry_interval {
                    let elapsed = p.received.elapsed().as_secs();
                    if elapsed >= u64::from(seconds) {
                        p.complete = true;
                        handle.0.lock().unwrap().expired += 1;
                        continue;
                    }
                    props.message_expiry_interval = Some(seconds - elapsed as u32);
                }
                p.submitted = destination
                    .try_publish_with_properties(
                        topic,
                        QoS::AtLeastOnce,
                        p.publication.retain,
                        p.publication.payload.clone(),
                        props,
                    )
                    .is_ok();
                if !p.submitted {
                    publication_retry = tokio::time::Instant::now() + config.reconnect_delay;
                }
            } else {
                p.complete = true;
                handle.0.lock().unwrap().loop_filtered += 1;
                continue;
            }
        }
        let retry = pending
            .as_ref()
            .is_some_and(|p| !p.complete && !p.submitted && connected);
        tokio::select! {
            (mut input,event)=&mut input_poll=>{
              match event{
                Ok(Event::Incoming(Packet::ConnAck(ack)))=>{
                    if !ack.session_present {
                        // Fence ACKs for every older Session, including repeated
                        // losses while a destination exchange is still pending.
                        let Some(next)=source_session.checked_add(1) else {return;};
                        source_session=next;
                    }
                    source_connected=true;
                    subscribe_pending=true;
                },
                Ok(Event::Incoming(Packet::SubAck(ack)))=>{
                    if ack.return_codes.len()!=1||!matches!(ack.return_codes[0],SubscribeReasonCode::Success(_)){
                        handle.0.lock().unwrap().failures+=1;
                        source_connected=false;
                        input.clean();source_retry=tokio::time::Instant::now()+config.reconnect_delay;
                    }
                },
                Ok(Event::Incoming(Packet::Publish(p)))=>{
                    if let Some(old)=&pending{
                        if p.qos==QoS::AtMostOnce {
                            handle.0.lock().unwrap().dropped_qos0+=1;
                        } else if old.source_session!=source_session||p.pkid!=old.publication.pkid {
                            // An unexpected reliable delivery must stay owned by
                            // its broker, rather than be silently lost or ACKed.
                            source_connected=false;
                            input.clean();
                            source_retry=tokio::time::Instant::now()+config.reconnect_delay;
                            handle.0.lock().unwrap().failures+=1;
                        }
                    } else {
                        pending=Some(Transfer{publication:p,source_session,received:tokio::time::Instant::now(),submitted:false,complete:false});owned.1=true;handle.0.lock().unwrap().pending+=1;
                        // Loop filtering also runs while the destination is unavailable.
                        if mapped(&config,&link,&pending.as_ref().unwrap().publication).is_none(){pending.as_mut().unwrap().complete=true;handle.0.lock().unwrap().loop_filtered+=1;}
                    }
                },
                Err(_)=>{
                    source_connected=false;
                    // poll() may already have cleaned before the destination
                    // side completed. Drain again: an ACK queued after that
                    // cleanup must never survive into a new source Session.
                    input.clean();
                    source_retry=tokio::time::Instant::now()+config.reconnect_delay;
                    handle.0.lock().unwrap().failures+=1;
                },
                _=>{},
              }
              input_poll=Box::pin(poll_after(input,source_retry));
            },
            (mut output,event)=&mut output_poll=>{
              match event{
                Ok(Event::Incoming(Packet::ConnAck(ack)))=>{
                    // rumqttc discards its replay queue when the remote Session
                    // is gone. Re-submit our owned Transfer only in that case;
                    // a resumed Session already replays the original exchange.
                    if !ack.session_present {
                        if let Some(p)=pending.as_mut().filter(|p|!p.complete){p.submitted=false;}
                    } else {
                        for request in &mut output.pending {
                            if let rumqttc::v5::Request::Publish(p)=request {p.dup=true;}
                        }
                    }
                    connected=true;
                },
                Ok(Event::Incoming(Packet::PubAck(ack)))=>{
                    if let Some(p)=pending.as_mut().filter(|p|p.submitted&&!p.complete){
                        if matches!(ack.reason,PubAckReason::Success|PubAckReason::NoMatchingSubscribers){p.complete=true;handle.0.lock().unwrap().forwarded+=1;}
                        else{p.submitted=false;publication_retry=tokio::time::Instant::now()+config.reconnect_delay;handle.0.lock().unwrap().failures+=1;}
                    }
                },
                Err(_)=>{connected=false;destination_retry=tokio::time::Instant::now()+config.reconnect_delay;handle.0.lock().unwrap().failures+=1;},
                _=>{},
              }
              output_poll=Box::pin(poll_after(output,destination_retry));
            },
            _=tokio::time::sleep_until(publication_retry),if retry=>{},
        }
    }
}
