use super::*;
use tokio::sync::{Semaphore, mpsc};

#[derive(Clone)]
struct Business {
    name: &'static str,
    events: mpsc::UnboundedSender<Arc<MqttInbound>>,
    gate: Arc<Semaphore>,
    entered: Arc<Semaphore>,
}
#[derive(Clone)]
enum Call {
    Publish(MqttPublishRequest),
    PublishAndLoseReply(MqttPublishRequest),
    Query(String),
}
impl ModuleFactory for Business {
    fn descriptor(&self) -> ComponentDescriptor {
        let mut d = ComponentDescriptor::new(ComponentRef::module(self.name));
        d.queue_capacity = 1;
        d
    }
    fn create(&self, _: &ComponentConfig) -> FrameworkResult<Box<dyn Module>> {
        Ok(Box::new(self.clone()))
    }
}
#[async_trait]
impl Lifecycle for Business {}
impl Module for Business {
    fn descriptor(&self) -> ComponentDescriptor {
        ModuleFactory::descriptor(self)
    }
    fn handlers(&mut self) -> FrameworkResult<Option<HandlerRegistration>> {
        let component = ComponentRef::module(self.name);
        Ok(Some(HandlerRegistration::Serial {
            handler: Box::new(self.clone()),
            subscriptions: ["wire/#", "test/call", "test/block"]
                .into_iter()
                .enumerate()
                .map(|(i, topic)| {
                    SubscriptionSpec::new(
                        (if self.name == "first" { 10 } else { 20 }) + i as u64,
                        component.clone(),
                        topic,
                    )
                })
                .collect(),
        }))
    }
}
#[async_trait]
impl SerialHandler for Business {
    async fn handle(
        &mut self,
        ctx: &HandlerContext,
        message: MessageEnvelope,
    ) -> FrameworkResult<HandlerReply> {
        if message.topic == "test/block" {
            self.entered.add_permits(1);
            self.gate.acquire().await.unwrap().forget();
            return Ok(Arc::new(()));
        }
        if message.kind == MessageKind::Event {
            if let Ok(event) = message.payload::<MqttInbound>() {
                assert_eq!(message.topic, event.topic);
                assert!(message.source.is_none()); // external() establishes a new root
                assert!(message.causation_id.is_none());
                let _ = self.events.send(event);
            }
            return Ok(Arc::new(()));
        }
        let call = message.payload::<Call>()?;
        let destination = Destination::Component(ComponentRef::module("mqtt-broker"));
        match call.as_ref() {
            Call::Publish(request) | Call::PublishAndLoseReply(request) => {
                let result = ctx
                    .event_hub
                    .request::<_, MqttPublishResult>(
                        PUBLISH_TOPIC,
                        destination,
                        request.clone(),
                        options(request),
                    )?
                    .wait(Duration::from_secs(1))
                    .await?;
                if matches!(call.as_ref(), Call::PublishAndLoseReply(_)) {
                    self.entered.add_permits(1);
                    self.gate.acquire().await.unwrap().forget();
                }
                Ok(result)
            }
            Call::Query(id) => Ok(ctx
                .event_hub
                .request::<_, MqttPublishQueryResult>(
                    PUBLISH_QUERY_TOPIC,
                    destination,
                    MqttPublishQuery {
                        request_id: id.clone(),
                    },
                    MessageOptions {
                        payload_bytes: Some(id.len() + 64),
                        ..Default::default()
                    },
                )?
                .wait(Duration::from_secs(1))
                .await?),
        }
    }
}
fn publication(id: &str) -> MqttPublishRequest {
    MqttPublishRequest {
        request_id: id.into(),
        topic: "wire/output".into(),
        payload: bytes::Bytes::from_static(b"business"),
        qos: Qos::AtLeastOnce,
        retain: false,
        properties: Default::default(),
    }
}
fn options(request: &MqttPublishRequest) -> MessageOptions {
    MessageOptions {
        payload_bytes: Some(
            256 + request.request_id.len() + request.topic.len() + request.payload.len(),
        ),
        ..Default::default()
    }
}
type Businesses = Vec<(Business, mpsc::UnboundedReceiver<Arc<MqttInbound>>)>;

async fn setup(
    config: MqttBrokerConfig,
    limits: HubLimits,
) -> (ApplicationHandle, BrokerHandle, Businesses) {
    let mut builder = ApplicationBuilder::new("eventhub-mqtt-test").message_limits(limits);
    let factory = MqttBrokerFactory::new(config).unwrap();
    let handle = factory.handle();
    builder.register_module(factory).unwrap();
    let mut businesses = Vec::new();
    let mut selected = ComponentSelection::new();
    selected.enable_module("mqtt-broker");
    for name in ["first", "second"] {
        let (events, receiver) = mpsc::unbounded_channel();
        let business = Business {
            name,
            events,
            gate: Arc::new(Semaphore::new(0)),
            entered: Arc::new(Semaphore::new(0)),
        };
        builder.register_module(business.clone()).unwrap();
        selected.enable_module(name);
        businesses.push((business, receiver));
    }
    let app = builder
        .selection(selected)
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();
    (app, handle, businesses)
}
async fn call<R: Send + Sync + Clone + 'static>(
    app: &ApplicationHandle,
    who: &str,
    call: Call,
) -> R {
    app.event_hub()
        .external()
        .request::<_, R>(
            "test/call",
            Destination::Component(ComponentRef::module(who)),
            call,
            MessageOptions {
                payload_bytes: Some(16384),
                ..Default::default()
            },
        )
        .unwrap()
        .wait(Duration::from_secs(2))
        .await
        .unwrap()
        .as_ref()
        .clone()
}
async fn publish(
    app: &ApplicationHandle,
    who: &str,
    request: MqttPublishRequest,
) -> MqttPublishResult {
    call(app, who, Call::Publish(request)).await
}
async fn query(app: &ApplicationHandle, who: &str, id: &str) -> MqttPublishQueryResult {
    call(app, who, Call::Query(id.into())).await
}
async fn no_message(client: &mut Client) {
    // Protocol barrier first, so the negative assertion is not just a scheduling guess.
    client.send(vec![0xc0, 0]).await;
    assert_eq!(client.recv().await, (0xd0, vec![]));
    assert!(
        tokio::time::timeout(Duration::from_millis(30), client.0.read_u8())
            .await
            .is_err()
    );
}
async fn wait_for(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn inbound_fans_out_typed_events_and_self_subscription_has_no_bridge_loop() {
    let (app, handle, mut businesses) = setup(config(), HubLimits::default()).await;
    let (mut client, _) = Client::connect(handle.local_addr().unwrap(), "sensor", true).await;
    client.subscribe(&[("wire/input", 1)]).await;
    client.publish("wire/input", b"temperature", 1, true).await;
    let (_, payload, _, id) = client.message().await;
    assert_eq!(payload, b"temperature");
    client.ack(id.unwrap()).await;
    for (_, receiver) in &mut businesses {
        let event = tokio::time::timeout(Duration::from_secs(1), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event.client_id, "sensor");
        assert_eq!(event.broker, "mqtt-broker");
        assert_eq!(event.generation, 1);
        assert!(event.connection_id > 0 && event.received_at_ms > 0);
        assert_eq!(event.payload, b"temperature"[..]);
        assert_eq!(event.packet_id, Some(7));
        assert_eq!(event.qos, Qos::AtLeastOnce);
        assert!(event.retain);
    }
    no_message(&mut client).await;
    assert_eq!(handle.status().inbound_observed, 1);
    stop(&app).await;
}

#[tokio::test]
async fn outbound_receipts_idempotency_source_isolation_and_frozen_targets() {
    let (app, handle, _businesses) = setup(config(), HubLimits::default()).await;
    let address = handle.local_addr().unwrap();
    let (mut client, _) = Client::connect(address, "consumer", true).await;
    client.subscribe(&[("wire/output", 1)]).await;
    let request = publication("stable-id");
    let receipt = publish(&app, "first", request.clone()).await.unwrap();
    assert!(receipt.accepted);
    assert_eq!(receipt.durability, AcceptanceDurability::MemoryOnly);
    let (_, payload, _, id) = client.message().await;
    assert_eq!(payload, b"business");
    client.ack(id.unwrap()).await;
    // A later subscriber must not join the committed target set on a retry.
    let (mut later, _) = Client::connect(address, "later", true).await;
    later.subscribe(&[("wire/output", 1)]).await;
    assert_eq!(
        publish(&app, "first", request.clone()).await.unwrap(),
        receipt
    );
    assert_eq!(
        query(&app, "first", "stable-id").await.unwrap(),
        Some(Ok(receipt))
    );
    assert_eq!(query(&app, "second", "stable-id").await.unwrap(), None);
    no_message(&mut client).await;
    no_message(&mut later).await;
    let mut conflict = request.clone();
    conflict.payload = bytes::Bytes::from_static(b"changed");
    assert_eq!(
        publish(&app, "first", conflict).await,
        Err(MqttPublishRejection::IdConflict)
    );
    publish(&app, "second", request).await.unwrap();
    for client in [&mut client, &mut later] {
        let (_, _, _, id) = client.message().await;
        client.ack(id.unwrap()).await;
    }
    assert_eq!(handle.status().publish_records, 2);
    assert_eq!(handle.status().inbound_observed, 0); // internal publishes are not network inbound
    stop(&app).await;
}

#[tokio::test]
async fn control_rejects_spoofed_source_invalid_dtos_and_non_requests_without_restart() {
    let (app, handle, _businesses) = setup(config(), HubLimits::default()).await;
    let request = publication("forged");
    let destination = Destination::Component(ComponentRef::module("mqtt-broker"));
    let mut metadata = options(&request);
    metadata.headers.insert("source".into(), "first".into());
    let result = app
        .event_hub()
        .external()
        .request::<_, MqttPublishResult>(
            PUBLISH_TOPIC,
            destination.clone(),
            request.clone(),
            metadata.clone(),
        )
        .unwrap()
        .wait(Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(*result, Err(MqttPublishRejection::UntrustedSource));
    app.event_hub()
        .external()
        .command(
            PUBLISH_TOPIC,
            destination.clone(),
            request.clone(),
            metadata.clone(),
        )
        .unwrap();
    app.event_hub()
        .external()
        .publish(
            PUBLISH_TOPIC,
            destination.clone(),
            request.clone(),
            metadata.clone(),
            PublishPolicy::Reject,
        )
        .unwrap();
    // Ordinary business events do not match Core's control-only subscriptions.
    app.event_hub()
        .external()
        .publish(
            "wire/output",
            Destination::Broadcast,
            request.clone(),
            metadata,
            PublishPolicy::Reject,
        )
        .unwrap();
    let wrong = app
        .event_hub()
        .external()
        .request::<_, MqttPublishResult>(
            PUBLISH_TOPIC,
            destination,
            (),
            MessageOptions {
                payload_bytes: Some(0),
                ..Default::default()
            },
        )
        .unwrap()
        .wait(Duration::from_secs(1))
        .await;
    assert!(wrong.is_err());
    let (mut client, _) = Client::connect(handle.local_addr().unwrap(), "unchanged", true).await;
    client.subscribe(&[("wire/output", 1)]).await;
    no_message(&mut client).await;
    assert_eq!(handle.status().generation, 1);
    assert_eq!(handle.status().publish_records, 0);
    for (id, mutate, expected) in [
        (
            "reserved",
            1,
            MqttPublishRejection::Invalid("reserved internal namespace".into()),
        ),
        (
            "wildcard",
            2,
            MqttPublishRejection::Invalid("invalid Topic Name".into()),
        ),
        (
            "oversize",
            3,
            MqttPublishRejection::Invalid("packet exceeds configured bound".into()),
        ),
        (
            "",
            4,
            MqttPublishRejection::Invalid("request_id must be 1..128 bytes without NUL".into()),
        ),
    ] {
        let mut invalid = publication(id);
        match mutate {
            1 => invalid.topic = PUBLISH_TOPIC.into(),
            2 => invalid.topic = "wire/+".into(),
            3 => invalid.payload = vec![0; 8192].into(),
            _ => (),
        }
        assert_eq!(publish(&app, "first", invalid).await, Err(expected));
    }
    assert_eq!(handle.status().publish_records, 0);
    stop(&app).await;
}

#[tokio::test]
async fn ledger_is_bounded_and_atomic_routing_rejections_are_queryable() {
    let mut cfg = config();
    cfg.max_publish_records = 2;
    cfg.max_inflight_per_client = 1;
    cfg.max_offline_messages_per_client = 1;
    let (app, handle, _businesses) = setup(cfg, HubLimits::default()).await;
    let address = handle.local_addr().unwrap();
    let (mut client, _) = Client::connect(address, "persistent", false).await;
    client.subscribe(&[("wire/#", 1)]).await;
    publish(&app, "first", publication("one")).await.unwrap();
    let (_, _, _, id) = client.message().await;
    let (mut healthy, _) = Client::connect(address, "healthy", true).await;
    healthy.subscribe(&[("wire/#", 1)]).await;
    let mut rejected = publication("two");
    rejected.retain = true;
    let result = publish(&app, "first", rejected.clone()).await;
    assert_eq!(
        result,
        Err(MqttPublishRejection::Capacity(
            "reliable session backlog".into()
        ))
    );
    assert_eq!(handle.status().retained, 0);
    client.ack(id.unwrap()).await;
    client.send(vec![0xc0, 0]).await;
    assert_eq!(client.recv().await.0, 0xd0); // ACK processed before retry
    assert_eq!(
        query(&app, "first", "two").await.unwrap(),
        Some(result.clone())
    );
    assert_eq!(publish(&app, "first", rejected).await, result);
    assert_eq!(
        publish(&app, "first", publication("three")).await,
        Err(MqttPublishRejection::LedgerFull)
    );
    publish(&app, "first", publication("one")).await.unwrap(); // accepted key still works when full
    no_message(&mut client).await;
    no_message(&mut healthy).await;
    assert_eq!(handle.status().publish_records, 2);
    assert!(handle.status().memory_used_bytes <= config().memory_budget_bytes);
    stop(&app).await;
}

#[tokio::test]
async fn publish_ledger_survives_reconstruction_and_stale_handler_cannot_mutate() {
    let (app, handle, _businesses) = setup(config(), HubLimits::default()).await;
    let request = publication("reconstruct");
    let receipt = publish(&app, "first", request.clone()).await.unwrap();
    let used = handle.status().memory_used_bytes;
    handle.state.lock().unwrap().fail_listener = true;
    wait_for(|| handle.status().generation >= 2 && handle.status().active).await;
    assert_eq!(
        query(&app, "first", "reconstruct").await.unwrap(),
        Some(Ok(receipt))
    );
    let (mut client, _) = Client::connect(handle.local_addr().unwrap(), "new-target", true).await;
    client.subscribe(&[("wire/output", 1)]).await;
    publish(&app, "first", request.clone()).await.unwrap();
    no_message(&mut client).await;
    assert_eq!(
        handle
            .state
            .lock()
            .unwrap()
            .publish_internal(1, &ComponentRef::module("first"), &request),
        Err(MqttPublishRejection::Inactive)
    );
    stop(&app).await;
    assert_eq!(handle.status().publish_records, 1);
    // The ledger remains charged; a bounded IP rate bucket may remain after the transient connection.
    assert!(handle.status().memory_used_bytes >= used);
    assert!(handle.status().memory_used_bytes < used + 4096);
}

async fn block(app: &ApplicationHandle, business: &Business) {
    app.event_hub()
        .external()
        .command(
            "test/block",
            Destination::Component(ComponentRef::module(business.name)),
            (),
            MessageOptions {
                payload_bytes: Some(0),
                ..Default::default()
            },
        )
        .unwrap();
    business.entered.acquire().await.unwrap().forget();
}

#[tokio::test]
async fn observer_backpressure_partial_fanout_long_topics_and_no_match_do_not_change_puback() {
    let (app, handle, mut businesses) = setup(config(), HubLimits::default()).await;
    let (mut client, _) = Client::connect(handle.local_addr().unwrap(), "producer", true).await;
    block(&app, &businesses[0].0).await;
    client.publish("wire/first", b"one", 1, false).await; // first observer has one queued event
    businesses[1].1.recv().await.unwrap();
    client.publish("wire/second", b"two", 1, false).await; // partial: first full, second free
    businesses[1].1.recv().await.unwrap();
    assert_eq!(handle.status().inbound_partial, 1);
    block(&app, &businesses[1].0).await;
    client.publish("wire/third", b"three", 1, false).await; // second queue fills
    client.publish("wire/fourth", b"four", 1, false).await; // both queues full
    client.publish(&"x".repeat(2048), b"long", 1, false).await;
    client.publish("unmatched", b"none", 1, false).await;
    assert_eq!(handle.status().inbound_observed, 3);
    assert_eq!(handle.status().inbound_unobserved, 3);
    assert_eq!(handle.status().inbound_partial, 2);
    assert_eq!(handle.status().generation, 1);
    for (business, _) in &businesses {
        business.gate.add_permits(1);
    }
    stop(&app).await;
}

#[tokio::test]
async fn observation_declares_payload_cost_and_hub_byte_rejection_preserves_mqtt_routing() {
    let limits = HubLimits {
        max_message_bytes: Some(512),
        ..Default::default()
    };
    let (app, handle, _businesses) = setup(config(), limits).await;
    let address = handle.local_addr().unwrap();
    let (mut client, _) = Client::connect(address, "source", true).await;
    let (mut sub, _) = Client::connect(address, "target", true).await;
    sub.subscribe(&[("wire/large", 1)]).await;
    client.publish("wire/large", &[42; 1024], 1, false).await;
    let (_, payload, _, id) = sub.message().await;
    assert_eq!(payload.len(), 1024);
    sub.ack(id.unwrap()).await;
    assert_eq!(handle.status().inbound_unobserved, 1);
    stop(&app).await;
}

#[tokio::test]
async fn caller_indeterminate_after_commit_resolves_with_same_source_query_and_retry() {
    let (app, handle, businesses) = setup(config(), HubLimits::default()).await;
    let request = publication("uncertain");
    let operation = app
        .event_hub()
        .external()
        .request::<_, MqttPublishResult>(
            "test/call",
            Destination::Component(ComponentRef::module("first")),
            Call::PublishAndLoseReply(request.clone()),
            options(&request),
        )
        .unwrap();
    businesses[0].0.entered.acquire().await.unwrap().forget();
    assert_eq!(handle.status().publish_records, 1);
    assert!(matches!(
        operation.wait(Duration::ZERO).await,
        Err(FrameworkError::Indeterminate(_))
    ));
    businesses[0].0.gate.add_permits(1);
    assert!(
        query(&app, "first", "uncertain")
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .accepted
    );
    let (mut client, _) = Client::connect(handle.local_addr().unwrap(), "after-commit", true).await;
    client.subscribe(&[("wire/output", 1)]).await;
    publish(&app, "first", request).await.unwrap();
    no_message(&mut client).await;
    stop(&app).await;
}

#[tokio::test]
async fn ledger_reservations_share_global_budget_and_do_not_mutate_on_exhaustion() {
    let (app, handle, _businesses) = setup(config(), HubLimits::default()).await;
    let charge = {
        let state = handle.state.lock().unwrap();
        state
            .budget
            .reserve(state.config.memory_budget_bytes - state.status().memory_used_bytes)
            .unwrap()
    };
    assert!(matches!(
        publish(&app, "first", publication("budget")).await,
        Err(MqttPublishRejection::Capacity(_))
    ));
    assert_eq!(handle.status().publish_records, 0);
    assert_eq!(query(&app, "first", "budget").await.unwrap(), None);
    drop(charge);
    publish(&app, "first", publication("budget")).await.unwrap();
    assert_eq!(handle.status().publish_records, 1);
    stop(&app).await;
}

#[tokio::test]
async fn host_assigns_global_control_subscription_ids_to_multiple_brokers() {
    let mut builder = ApplicationBuilder::new("two-brokers");
    let mut selected = ComponentSelection::new();
    for (name, ids) in [("mqtt-broker", [1, 2, 3]), ("other-broker", [4, 5, 6])] {
        let mut config = config();
        config.name = name.into();
        config.control_subscription_ids = ids;
        builder
            .register_module(MqttBrokerFactory::new(config).unwrap())
            .unwrap();
        selected.enable_module(name);
    }
    let app = builder
        .selection(selected)
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();
    for name in ["mqtt-broker", "other-broker"] {
        let status = app
            .event_hub()
            .external()
            .request::<_, BrokerStatus>(
                STATUS_TOPIC,
                Destination::Component(ComponentRef::module(name)),
                (),
                Default::default(),
            )
            .unwrap()
            .wait(Duration::from_secs(1))
            .await
            .unwrap();
        assert!(status.active);
    }
    stop(&app).await;
    let mut invalid = config();
    invalid.control_subscription_ids = [1, 1, 2];
    assert!(invalid.validate().is_err());
}

#[tokio::test]
async fn internal_retained_delete_and_retry_do_not_reapply_an_old_value() {
    let (app, handle, _businesses) = setup(config(), HubLimits::default()).await;
    let mut request = publication("retained-original");
    request.retain = true;
    publish(&app, "first", request.clone()).await.unwrap();
    assert_eq!(handle.status().retained, 1);
    let address = handle.local_addr().unwrap();
    let (mut network, _) = Client::connect(address, "network", true).await;
    network
        .publish("wire/output", b"replacement", 1, true)
        .await;
    publish(&app, "first", request).await.unwrap();
    let (mut reader, _) = Client::connect(address, "reader", true).await;
    reader.subscribe(&[("wire/output", 1)]).await;
    let (_, payload, header, id) = reader.message().await;
    assert_eq!(payload, b"replacement");
    assert_eq!(header & 1, 1);
    reader.ack(id.unwrap()).await;
    let mut deletion = publication("retained-delete");
    deletion.retain = true;
    deletion.payload = bytes::Bytes::new();
    publish(&app, "first", deletion).await.unwrap();
    let (_, payload, _, id) = reader.message().await;
    assert!(payload.is_empty());
    reader.ack(id.unwrap()).await;
    assert_eq!(handle.status().retained, 0);
    assert_eq!(handle.status().publish_records, 2);
    stop(&app).await;
}

#[tokio::test]
async fn qos2_internal_publication_and_network_duplicate_preserve_single_business_observation() {
    use super::qos2::*;
    let (app, handle, mut businesses) = setup(config(), HubLimits::default()).await;
    let (_, mut observations) = businesses.remove(0);
    let addr = handle.local_addr().unwrap();
    let (mut subscriber, _) = Client::connect(addr, "q2-reader", false).await;
    subscriber.subscribe(&[("wire/#", 2)]).await;
    let mut request = publication("q2-business");
    request.qos = Qos::ExactlyOnce;
    let receipt = publish(&app, "first", request.clone()).await.unwrap();
    assert_eq!(receipt.durability, AcceptanceDurability::MemoryOnly);
    assert_eq!(publish(&app, "first", request).await.unwrap(), receipt);
    let (_, _, header, id) = subscriber.message().await;
    assert_eq!(header, 0x34);
    received(&mut subscriber, id.unwrap()).await;
    complete(&mut subscriber, id.unwrap()).await;
    let (mut p, _) = Client::connect(addr, "q2-writer", false).await;
    publish_start(&mut p, 9, "wire/q2", b"observe-once", false, false).await;
    publish_start(&mut p, 9, "wire/q2", b"observe-once", true, false).await;
    let event = tokio::time::timeout(Duration::from_secs(2), observations.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event.qos, Qos::ExactlyOnce);
    assert_eq!(event.payload.as_ref(), b"observe-once");
    assert!(
        tokio::time::timeout(Duration::from_millis(30), observations.recv())
            .await
            .is_err()
    );
    release(&mut p, 9).await;
    stop(&app).await;
}
