use super::*;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

fn config() -> MqttBrokerConfig {
    MqttBrokerConfig {
        listener: "127.0.0.1:0".parse().unwrap(),
        max_packet_bytes: 8192,
        max_connections: 8,
        max_sessions: 32,
        max_will_records: 8,
        ..MqttBrokerConfig::loopback_development()
    }
}

async fn start(config: MqttBrokerConfig) -> (ApplicationHandle, BrokerHandle) {
    let mut builder = ApplicationBuilder::new("mqtt-test");
    let factory = MqttBrokerFactory::new(config).unwrap();
    let handle = factory.handle();
    let name = factory.descriptor().id;
    builder.register_module(factory).unwrap();
    let mut selection = ComponentSelection::new();
    selection.enable_module(name);
    let app = builder
        .selection(selection)
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();
    (app, handle)
}

async fn stop(app: &ApplicationHandle) {
    tokio::time::timeout(
        Duration::from_secs(3),
        app.shutdown_checked(ShutdownOptions::graceful()),
    )
    .await
    .unwrap()
    .unwrap();
}

fn string(value: &str) -> Vec<u8> {
    let mut bytes = (value.len() as u16).to_be_bytes().to_vec();
    bytes.extend(value.as_bytes());
    bytes
}

struct Client(TcpStream);
impl Client {
    async fn connect(address: SocketAddr, id: &str, clean: bool) -> (Self, bool) {
        Self::connect_will(address, id, clean, None, 0).await
    }
    async fn connect_will(
        address: SocketAddr,
        id: &str,
        clean: bool,
        will: Option<(&str, &[u8], bool)>,
        keep_alive: u16,
    ) -> (Self, bool) {
        let mut client = Self(TcpStream::connect(address).await.unwrap());
        let mut body = string("MQTT");
        body.push(4);
        let flags =
            u8::from(clean) * 2 + will.map_or(0, |(_, _, retain)| 4 + 8 + u8::from(retain) * 32);
        body.push(flags);
        body.extend(keep_alive.to_be_bytes());
        body.extend(string(id));
        if let Some((topic, payload, _)) = will {
            body.extend(string(topic));
            body.extend((payload.len() as u16).to_be_bytes());
            body.extend(payload);
        }
        client.send(codec::frame(0x10, &body)).await;
        let reply = client.recv().await;
        assert_eq!(reply.0, 0x20);
        assert_eq!(reply.1[1], 0);
        (client, reply.1[0] == 1)
    }
    async fn send(&mut self, frame: Vec<u8>) {
        self.0.write_all(&frame).await.unwrap();
    }
    async fn recv(&mut self) -> (u8, Vec<u8>) {
        tokio::time::timeout(Duration::from_secs(3), async {
            let header = self.0.read_u8().await.unwrap();
            let mut len = 0;
            for shift in [0, 7, 14, 21] {
                let byte = self.0.read_u8().await.unwrap();
                len |= usize::from(byte & 127) << shift;
                if byte & 128 == 0 {
                    break;
                }
            }
            let mut bytes = vec![0; len];
            self.0.read_exact(&mut bytes).await.unwrap();
            (header, bytes)
        })
        .await
        .unwrap()
    }
    async fn subscribe(&mut self, filters: &[(&str, u8)]) -> Vec<u8> {
        let mut body = vec![0, 1];
        for (filter, qos) in filters {
            body.extend(string(filter));
            body.push(*qos);
        }
        self.send(codec::frame(0x82, &body)).await;
        let (header, body) = self.recv().await;
        assert_eq!(header, 0x90);
        body[2..].to_vec()
    }
    async fn publish(&mut self, topic: &str, payload: &[u8], qos: u8, retain: bool) {
        let mut body = string(topic);
        if qos == 1 {
            body.extend([0, 7]);
        }
        body.extend(payload);
        self.send(codec::frame(0x30 | (qos << 1) | u8::from(retain), &body))
            .await;
        if qos == 1 {
            assert_eq!(self.recv().await, (0x40, vec![0, 7]));
        }
    }
    async fn message(&mut self) -> (String, Vec<u8>, u8, Option<u16>) {
        let (header, body) = self.recv().await;
        assert_eq!(header >> 4, 3);
        let len = u16::from_be_bytes([body[0], body[1]]) as usize;
        let topic = String::from_utf8(body[2..2 + len].to_vec()).unwrap();
        let mut offset = 2 + len;
        let id = if header & 6 != 0 {
            let id = u16::from_be_bytes([body[offset], body[offset + 1]]);
            offset += 2;
            Some(id)
        } else {
            None
        };
        (topic, body[offset..].to_vec(), header, id)
    }
    async fn ack(&mut self, id: u16) {
        self.send(codec::frame(0x40, &id.to_be_bytes())).await;
    }
    async fn disconnect(mut self) {
        self.send(vec![0xe0, 0]).await;
    }
}

#[tokio::test]
async fn registration_is_explicit_and_status_uses_the_hosts_hub() {
    let mut builder = ApplicationBuilder::new("disabled-mqtt");
    let factory = MqttBrokerFactory::new(config()).unwrap();
    let handle = factory.handle();
    builder.register_module(factory).unwrap();
    let app = builder.build().unwrap().start().await.unwrap();
    assert!(handle.local_addr().is_none());
    stop(&app).await;

    let (app, handle) = start(config()).await;
    let status = app
        .event_hub()
        .external()
        .request::<_, BrokerStatus>(
            STATUS_TOPIC,
            Destination::Component(ComponentRef::module("mqtt-broker")),
            (),
            Default::default(),
        )
        .unwrap()
        .wait(Duration::from_secs(1))
        .await
        .unwrap();
    assert!(status.active);
    assert_eq!(status.generation, handle.status().generation);
    stop(&app).await;
    assert!(
        TcpStream::connect(handle.local_addr().unwrap())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn qos1_overlap_retained_delete_and_unsubscribe() {
    let (app, handle) = start(config()).await;
    let addr = handle.local_addr().unwrap();
    let (mut publisher, _) = Client::connect(addr, "publisher", true).await;
    let (mut subscriber, _) = Client::connect(addr, "subscriber", false).await;
    assert_eq!(
        subscriber
            .subscribe(&[("sensor/#", 0), ("sensor/+", 1), ("qos2", 2)])
            .await,
        [0, 1, 0x80]
    );
    publisher.publish("sensor/a", b"value", 1, true).await;
    let (topic, payload, header, id) = subscriber.message().await;
    assert_eq!(topic, "sensor/a");
    assert_eq!(payload, b"value");
    assert_eq!(header, 0x32);
    subscriber.ack(id.unwrap()).await;
    subscriber.send(vec![0xc0, 0]).await;
    assert_eq!(subscriber.recv().await, (0xd0, vec![]));
    let (mut late, _) = Client::connect(addr, "late", true).await;
    assert_eq!(late.subscribe(&[("sensor/#", 1)]).await, [1]);
    let (_, payload, header, id) = late.message().await;
    assert_eq!(payload, b"value");
    assert_eq!(header, 0x33);
    late.ack(id.unwrap()).await;
    publisher.publish("sensor/a", b"", 1, true).await;
    assert_eq!(handle.status().retained, 0);
    let mut body = vec![0, 2];
    body.extend(string("sensor/#"));
    body.extend(string("sensor/+"));
    subscriber.send(codec::frame(0xa2, &body)).await;
    // The already routed retained-delete PUBLISH is allowed before UNSUBACK.
    let (_, _, _, id) = subscriber.message().await;
    subscriber.ack(id.unwrap()).await;
    assert_eq!(subscriber.recv().await, (0xb0, vec![0, 2]));
    stop(&app).await;
    assert_eq!(handle.status().connections, 0);
}

#[tokio::test]
async fn offline_qos1_and_inflight_reconnect_keep_packet_id_and_dup() {
    let (app, handle) = start(config()).await;
    let addr = handle.local_addr().unwrap();
    let (mut sub, present) = Client::connect(addr, "durable-memory", false).await;
    assert!(!present);
    sub.subscribe(&[("work", 1)]).await;
    sub.disconnect().await;
    let (mut publisher, _) = Client::connect(addr, "publisher", true).await;
    publisher.publish("work", b"offline", 1, false).await;
    let (mut sub, present) = Client::connect(addr, "durable-memory", false).await;
    assert!(present);
    let (_, payload, header, id) = sub.message().await;
    assert_eq!(payload, b"offline");
    assert_eq!(header, 0x32);
    drop(sub);
    let (mut sub, present) = Client::connect(addr, "durable-memory", false).await;
    assert!(present);
    let (_, payload, header, replay_id) = sub.message().await;
    assert_eq!(payload, b"offline");
    assert_eq!(header, 0x3a);
    assert_eq!(id, replay_id);
    sub.ack(id.unwrap()).await;
    sub.send(vec![0xc0, 0]).await;
    assert_eq!(sub.recv().await, (0xd0, vec![]));
    let (mut reset, present) = Client::connect(addr, "durable-memory", true).await;
    assert!(!present);
    reset.send(vec![0xc0, 0]).await;
    assert_eq!(reset.recv().await, (0xd0, vec![]));
    stop(&app).await;
}

#[tokio::test]
async fn will_on_abrupt_close_and_takeover_but_not_disconnect() {
    let (app, handle) = start(config()).await;
    let addr = handle.local_addr().unwrap();
    let (mut sub, _) = Client::connect(addr, "observer", true).await;
    sub.subscribe(&[("will", 1)]).await;
    let (will, _) =
        Client::connect_will(addr, "will-client", false, Some(("will", b"gone", true)), 0).await;
    drop(will);
    let (_, payload, _, id) = sub.message().await;
    assert_eq!(payload, b"gone");
    sub.ack(id.unwrap()).await;
    let (old, _) = Client::connect_will(
        addr,
        "will-client",
        false,
        Some(("will", b"takeover", false)),
        0,
    )
    .await;
    let (new, present) = Client::connect(addr, "will-client", false).await;
    assert!(present);
    let (_, payload, _, id) = sub.message().await;
    assert_eq!(payload, b"takeover");
    sub.ack(id.unwrap()).await;
    drop(old);
    new.disconnect().await;
    let (clean, _) = Client::connect_will(
        addr,
        "normal",
        true,
        Some(("will", b"must-not-fire", false)),
        0,
    )
    .await;
    clean.disconnect().await;
    sub.send(vec![0xc0, 0]).await;
    assert_eq!(sub.recv().await, (0xd0, vec![]));
    stop(&app).await;
}

#[tokio::test]
async fn malformed_client_and_qos2_close_only_that_connection() {
    let (app, handle) = start(config()).await;
    let addr = handle.local_addr().unwrap();
    for bytes in [
        vec![0xc0, 0],
        vec![0x30, 0xff, 0xff, 0xff, 0xff],
        vec![0x10, 0],
    ] {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(&bytes).await.unwrap();
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), stream.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
    }
    let (mut client, _) = Client::connect(addr, "bad-qos", true).await;
    let mut body = string("topic");
    body.extend([0, 1]);
    client.send(codec::frame(0x34, &body)).await;
    assert_eq!(client.0.read(&mut [0]).await.unwrap(), 0);
    let (mut healthy, _) = Client::connect(addr, "healthy", true).await;
    healthy.send(vec![0xc0, 0]).await;
    assert_eq!(healthy.recv().await, (0xd0, vec![]));
    assert_eq!(handle.status().generation, 1);
    stop(&app).await;
}

#[tokio::test]
async fn saturation_rejects_entire_publish_without_retained_or_partial_delivery() {
    let mut cfg = config();
    cfg.max_offline_messages_per_client = 1;
    cfg.max_inflight_per_client = 1;
    let (app, handle) = start(cfg).await;
    let addr = handle.local_addr().unwrap();
    let (mut slow, _) = Client::connect(addr, "slow", false).await;
    slow.subscribe(&[("fanout", 1)]).await;
    slow.disconnect().await;
    let (mut publisher, _) = Client::connect(addr, "publisher", true).await;
    publisher.publish("fanout", b"first", 1, false).await;
    let (mut other, _) = Client::connect(addr, "other", true).await;
    other.subscribe(&[("fanout", 1)]).await;
    let mut body = string("fanout");
    body.extend([0, 9]);
    body.extend(b"rejected");
    publisher.send(codec::frame(0x33, &body)).await;
    assert_eq!(publisher.0.read(&mut [0]).await.unwrap(), 0);
    assert_eq!(handle.status().retained, 0);
    assert_eq!(handle.status().rejected_publications, 1);
    other.send(vec![0xc0, 0]).await;
    assert_eq!(other.recv().await, (0xd0, vec![]));
    let (mut slow, present) = Client::connect(addr, "slow", false).await;
    assert!(present);
    let (_, payload, _, id) = slow.message().await;
    assert_eq!(payload, b"first");
    slow.ack(id.unwrap()).await;
    stop(&app).await;
}

#[tokio::test]
async fn restart_preserves_memory_sessions_retained_inflight_and_fences_old_generation() {
    let (app, handle) = start(config()).await;
    let addr = handle.local_addr().unwrap();
    let (mut sub, _) = Client::connect(addr, "persist", false).await;
    sub.subscribe(&[("persist", 1)]).await;
    let (mut publisher, _) = Client::connect(addr, "publisher", true).await;
    publisher.publish("persist", b"saved", 1, true).await;
    let (_, _, _, id) = sub.message().await;
    {
        let mut state = handle.state.lock().unwrap();
        state.fail_activations = 1;
        state.fail_listener = true;
    }
    tokio::time::timeout(Duration::from_secs(3), async {
        while handle.status().generation < 3 || !handle.status().active {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(handle.status().retained, 1);
    let (mut restored, present) = Client::connect(addr, "persist", false).await;
    assert!(present);
    let (_, payload, header, restored_id) = restored.message().await;
    assert_eq!(payload, b"saved");
    assert_eq!(header, 0x3a);
    assert_eq!(restored_id, id);
    restored.ack(id.unwrap()).await;
    assert!(handle.state.lock().unwrap().check(1, "persist", 1).is_err());
    stop(&app).await;
}

struct LaterFailure;
#[async_trait]
impl Lifecycle for LaterFailure {}
#[async_trait]
impl Module for LaterFailure {
    fn descriptor(&self) -> ComponentDescriptor {
        let mut d = ComponentDescriptor::new(ComponentRef::module("later"));
        d.requires.push(ComponentRef::module("mqtt-broker"));
        d
    }
    async fn activate_ingress(&mut self, _: &RuntimeContext) -> FrameworkResult<()> {
        Err(framework("later activation failed"))
    }
}
impl ModuleFactory for LaterFailure {
    fn descriptor(&self) -> ComponentDescriptor {
        Module::descriptor(self)
    }
    fn create(&self, _: &ComponentConfig) -> FrameworkResult<Box<dyn Module>> {
        Ok(Box::new(LaterFailure))
    }
}

#[tokio::test]
async fn later_activation_failure_and_listener_quota_release_the_port() {
    for quota in [1, 8] {
        let factory = MqttBrokerFactory::new(config()).unwrap();
        let handle = factory.handle();
        let mut builder = ApplicationBuilder::new("compensation").task_limits(quota, 1);
        builder.register_module(factory).unwrap();
        builder.register_module(LaterFailure).unwrap();
        let mut selection = ComponentSelection::new();
        selection
            .enable_module("mqtt-broker")
            .enable_module("later");
        let result = builder.selection(selection).build().unwrap().start().await;
        assert!(result.is_err());
        assert!(!handle.status().active);
        assert_eq!(handle.status().connections, 0);
        let address = handle.local_addr().unwrap();
        assert!(TcpStream::connect(address).await.is_err());
        let _listener = tokio::net::TcpListener::bind(address).await.unwrap();
    }
    let factory = MqttBrokerFactory::new(config()).unwrap();
    let handle = factory.handle();
    let mut builder = ApplicationBuilder::new("connection-quota").task_limits(1, 1);
    builder.register_module(factory).unwrap();
    let mut selection = ComponentSelection::new();
    selection.enable_module("mqtt-broker");
    let app = builder
        .selection(selection)
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();
    let mut stream = TcpStream::connect(handle.local_addr().unwrap())
        .await
        .unwrap();
    assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
    assert_eq!(handle.status().connections, 0);
    assert_eq!(handle.status().rejected_connections, 1);
    stop(&app).await;
}

struct ReadyProbe {
    broker: BrokerHandle,
    occupy_task: bool,
    gate: Option<Arc<Semaphore>>,
    task: Option<TaskHandle>,
}
#[async_trait]
impl Lifecycle for ReadyProbe {
    async fn start(&mut self, ctx: &RuntimeContext) -> FrameworkResult<()> {
        if self.occupy_task {
            self.task = Some(ctx.tasks.spawn(|cancel| async move {
                cancel.cancelled().await;
                Ok(())
            })?);
        }
        Ok(())
    }
    async fn ready(&mut self, _: &RuntimeContext) -> FrameworkResult<()> {
        assert!(
            TcpStream::connect(self.broker.local_addr().unwrap())
                .await
                .is_err()
        );
        if let Some(gate) = &self.gate {
            gate.acquire().await.unwrap().forget();
        }
        Ok(())
    }
    async fn quiesce(&mut self, _: &ShutdownContext) -> FrameworkResult<()> {
        if let Some(task) = &self.task {
            task.wait_finished().await?;
        }
        Ok(())
    }
}
impl Module for ReadyProbe {
    fn descriptor(&self) -> ComponentDescriptor {
        let mut d = ComponentDescriptor::new(ComponentRef::module("ready-probe"));
        d.requires.push(ComponentRef::module("mqtt-broker"));
        d
    }
}
impl ModuleFactory for ReadyProbe {
    fn descriptor(&self) -> ComponentDescriptor {
        Module::descriptor(self)
    }
    fn create(&self, _: &ComponentConfig) -> FrameworkResult<Box<dyn Module>> {
        Ok(Box::new(Self {
            broker: self.broker.clone(),
            occupy_task: self.occupy_task,
            gate: self.gate.clone(),
            task: None,
        }))
    }
}

#[tokio::test]
async fn all_ready_before_listen_and_listener_spawn_quota_compensates_bound_socket() {
    for occupy_task in [false, true] {
        let factory = MqttBrokerFactory::new(config()).unwrap();
        let handle = factory.handle();
        let mut builder = ApplicationBuilder::new("ready-gate").task_limits(1, 1);
        builder.register_module(factory).unwrap();
        builder
            .register_module(ReadyProbe {
                broker: handle.clone(),
                occupy_task,
                gate: None,
                task: None,
            })
            .unwrap();
        let mut selected = ComponentSelection::new();
        selected
            .enable_module("mqtt-broker")
            .enable_module("ready-probe");
        let result = builder.selection(selected).build().unwrap().start().await;
        if occupy_task {
            assert!(result.is_err());
        } else {
            stop(&result.unwrap()).await;
        }
        assert!(!handle.status().active);
        let _listener = tokio::net::TcpListener::bind(handle.local_addr().unwrap())
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn cancel_start_while_ready_still_completes_owned_compensation() {
    let factory = MqttBrokerFactory::new(config()).unwrap();
    let handle = factory.handle();
    let gate = Arc::new(Semaphore::new(0));
    let mut builder = ApplicationBuilder::new("cancel-start");
    builder.register_module(factory).unwrap();
    builder
        .register_module(ReadyProbe {
            broker: handle.clone(),
            occupy_task: false,
            gate: Some(gate.clone()),
            task: None,
        })
        .unwrap();
    let mut selected = ComponentSelection::new();
    selected
        .enable_module("mqtt-broker")
        .enable_module("ready-probe");
    let application = builder.selection(selected).build().unwrap();
    // Test harness only: the runtime itself retains the owned startup transaction.
    let starter = tokio::spawn(application.start());
    tokio::time::timeout(Duration::from_secs(2), async {
        while handle.local_addr().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    starter.abort();
    let _ = starter.await;
    gate.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Ok(listener) = tokio::net::TcpListener::bind(handle.local_addr().unwrap()).await
            {
                drop(listener);
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!handle.status().active);
}

#[tokio::test]
async fn handshake_timeout_keepalive_will_and_fragmented_packets() {
    let mut cfg = config();
    cfg.handshake_timeout = Duration::from_millis(100);
    let (app, handle) = start(cfg).await;
    let addr = handle.local_addr().unwrap();
    let mut idle = TcpStream::connect(addr).await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), idle.read(&mut [0]))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    let (mut observer, _) = Client::connect(addr, "observer", true).await;
    observer.subscribe(&[("timeout", 1)]).await;
    let (_silent, _) = Client::connect_will(
        addr,
        "silent",
        true,
        Some(("timeout", b"timed-out", false)),
        1,
    )
    .await;
    let (_, payload, _, id) = observer.message().await;
    assert_eq!(payload, b"timed-out");
    observer.ack(id.unwrap()).await;
    observer.0.write_all(&[0xc0]).await.unwrap();
    tokio::task::yield_now().await;
    observer.0.write_all(&[0]).await.unwrap();
    assert_eq!(observer.recv().await, (0xd0, vec![]));
    stop(&app).await;
}

#[tokio::test]
async fn long_topics_qos0_self_echo_and_reserved_namespaces() {
    let (app, handle) = start(config()).await;
    let addr = handle.local_addr().unwrap();
    let (mut client, _) = Client::connect(addr, "self", true).await;
    let topic = "x".repeat(2048); // exceeds EventHub's default local Topic budget
    assert_eq!(
        client.subscribe(&[(&topic, 1), ("$modorch/#", 1)]).await,
        [1, 0x80]
    );
    client.publish(&topic, b"zero", 0, false).await;
    let (actual, payload, header, _) = client.message().await;
    assert_eq!(actual, topic);
    assert_eq!(payload, b"zero");
    assert_eq!(header, 0x30);
    let mut bad = Client(TcpStream::connect(addr).await.unwrap());
    let mut body = string("MQTT");
    body.extend([4, 0x0e, 0, 0]);
    body.extend(string("self"));
    body.extend(string("$modorch/mqtt/control/publish"));
    body.extend([0, 1, b'x']);
    bad.send(codec::frame(0x10, &body)).await;
    assert_eq!(bad.recv().await.1[1], 5); // failed CONNECT did not take over self
    client.send(vec![0xc0, 0]).await;
    assert_eq!(client.recv().await, (0xd0, vec![]));
    let mut body = string("$modorch/mqtt/control/status");
    body.extend([0, 1]);
    client.send(codec::frame(0x32, &body)).await;
    assert_eq!(client.0.read(&mut [0]).await.unwrap(), 0);
    stop(&app).await;
}

#[tokio::test]
async fn application_isolation_and_memory_quota_release() {
    let mut cfg = config();
    cfg.max_connections = 1;
    let (first, handle) = start(cfg).await;
    let (second, other) = start(config()).await;
    let (mut client, _) = Client::connect(handle.local_addr().unwrap(), "same-id", false).await;
    client.subscribe(&[("local", 1)]).await;
    let (mut foreign, present) =
        Client::connect(other.local_addr().unwrap(), "same-id", false).await;
    assert!(!present);
    foreign.publish("local", b"isolated", 1, true).await;
    assert_eq!(handle.status().retained, 0);
    assert_eq!(other.status().retained, 1);
    let mut rejected = TcpStream::connect(handle.local_addr().unwrap())
        .await
        .unwrap();
    assert_eq!(rejected.read(&mut [0]).await.unwrap(), 0);
    assert_eq!(handle.status().rejected_connections, 1);
    client.send(vec![0xc0, 0]).await;
    assert_eq!(client.recv().await, (0xd0, vec![]));
    stop(&first).await;
    stop(&second).await;
    assert_eq!(handle.status().connections, 0);
    assert!(handle.status().memory_used_bytes < 4096);
}

#[tokio::test]
async fn assigned_client_id_never_takes_over_an_existing_session() {
    let (app, handle) = start(config()).await;
    let address = handle.local_addr().unwrap();
    let (mut existing, _) = Client::connect(address, "m2", false).await;
    let (_assigned, present) = Client::connect(address, "", true).await;
    assert!(!present);
    existing.send(vec![0xc0, 0]).await;
    assert_eq!(existing.recv().await, (0xd0, vec![]));
    assert_eq!(handle.status().sessions, 2);
    stop(&app).await;
}

#[test]
fn configuration_rejects_unsafe_and_overflowing_limits() {
    assert!(MqttBrokerFactory::new(MqttBrokerConfig::default()).is_err());
    let mut cfg = config();
    cfg.listener = "0.0.0.0:1883".parse().unwrap();
    assert!(cfg.validate().is_err());
    let mut cfg = config();
    cfg.max_sessions = usize::MAX;
    assert!(cfg.validate().is_err());
    let mut cfg = config();
    cfg.per_client_outbound_queue = usize::MAX;
    assert!(cfg.validate().is_err());
    let mut cfg = config();
    cfg.max_packet_bytes = 0;
    assert!(cfg.validate().is_err());
    let mut cfg = config();
    cfg.memory_budget_bytes = 1024;
    assert!(cfg.validate().is_err());
}
