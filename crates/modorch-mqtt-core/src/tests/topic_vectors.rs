use super::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Fixture {
    version: u32,
    vectors: Vec<Vector>,
}

#[derive(Deserialize)]
struct Vector {
    id: String,
    filter: String,
    topic: String,
    filter_valid: bool,
    topic_valid: bool,
    matches: bool,
}

fn vectors() -> Vec<Vector> {
    let fixture: Fixture =
        serde_json::from_str(include_str!("../../../../contracts/topic-vectors.json")).unwrap();
    assert_eq!(fixture.version, 1);
    assert_eq!(fixture.vectors.len(), 39);
    let ids: std::collections::BTreeSet<_> = fixture.vectors.iter().map(|v| &v.id).collect();
    assert_eq!(ids.len(), fixture.vectors.len());
    fixture.vectors
}

#[test]
fn shared_topic_vectors_agree_with_codec_and_modorch_facade() {
    for v in vectors() {
        assert_eq!(
            codec::validate_filter(&v.filter).is_ok(),
            v.filter_valid,
            "{}",
            v.id
        );
        assert_eq!(
            codec::validate_topic(&v.topic).is_ok(),
            v.topic_valid,
            "{}",
            v.id
        );
        // The private fast matcher only accepts previously validated strings.
        let actual = v.filter_valid && v.topic_valid && codec::matches(&v.filter, &v.topic);
        assert_eq!(actual, v.matches, "codec: {}", v.id);
        assert_eq!(
            modorch::topic_matches(&v.filter, &v.topic),
            v.matches,
            "facade: {}",
            v.id
        );
    }
}

struct Recorder(Arc<Mutex<Vec<String>>>);

#[async_trait]
impl SerialHandler for Recorder {
    async fn handle(
        &mut self,
        _: &HandlerContext,
        message: MessageEnvelope,
    ) -> FrameworkResult<HandlerReply> {
        self.0.lock().unwrap().push(message.topic);
        Ok(Arc::new(()))
    }
}

#[tokio::test]
async fn shared_topic_vectors_drive_real_hub_subscriptions_and_handlers() {
    // Test-only Hub: product Core continues to use the one supplied by its host.
    let tracker = OperationTracker::new();
    tracker.open().unwrap();
    let hub = EventHub::with_limits(
        tracker.clone(),
        FailureReporter::new(),
        HubLimits::default(),
    )
    .unwrap();
    let target = ComponentRef::module("topic-vector-recorder");
    let seen = Arc::new(Mutex::new(Vec::new()));
    hub.install(
        &ComponentDescriptor::new(target.clone()),
        HandlerRegistration::Serial {
            handler: Box::new(Recorder(seen.clone())),
            subscriptions: vec![],
        },
    )
    .unwrap();
    for v in vectors() {
        let result = hub.subscribe(SubscriptionSpec::new(100, target.clone(), &v.filter));
        assert_eq!(result.is_ok(), v.filter_valid, "subscribe: {}", v.id);
        if !v.filter_valid {
            assert!(matches!(result, Err(FrameworkError::InvalidConfig(_))));
        }
        let result = hub.publish(
            &v.topic,
            Destination::Broadcast,
            (),
            MessageOptions::default(),
            PublishPolicy::Reject,
        );
        if !v.topic_valid {
            assert!(
                matches!(result, Err(FrameworkError::InvalidConfig(_))),
                "{}",
                v.id
            );
        } else if v.matches {
            assert_eq!(result.unwrap().accepted(), 1, "{}", v.id);
        } else {
            assert!(
                matches!(result, Err(FrameworkError::Unroutable(_))),
                "{}",
                v.id
            );
        }
        tokio::time::timeout(Duration::from_secs(3), tracker.drain())
            .await
            .unwrap();
        let delivered = std::mem::take(&mut *seen.lock().unwrap());
        assert_eq!(
            delivered,
            if v.matches { vec![v.topic] } else { vec![] },
            "handler: {}",
            v.id
        );
        if v.filter_valid {
            hub.unsubscribe(100).unwrap();
        }
    }
    tracker.close();
    tokio::time::timeout(Duration::from_secs(3), tracker.drain())
        .await
        .unwrap();
    hub.terminate(false).await;
}

#[tokio::test]
async fn shared_topic_vectors_drive_real_mqtt_validation_and_delivery() {
    let (app, broker) = start(config()).await;
    let addr = broker.local_addr().unwrap();
    let marker = "$fixture/marker";
    for v in vectors() {
        let (mut s, _) = Client::connect(addr, "vector-reader", true).await;
        let (mut p, _) = Client::connect(addr, "vector-writer", true).await;
        if v.filter_valid {
            assert_eq!(
                s.subscribe(&[(&v.filter, 1), (marker, 1)]).await,
                [1, 1],
                "{}",
                v.id
            );
        } else {
            let mut body = vec![0, 1];
            body.extend(string(&v.filter));
            body.push(1);
            s.send(codec::frame(0x82, &body)).await;
            protocol::closed(&mut s).await;
        }
        if v.topic_valid {
            p.publish(&v.topic, v.id.as_bytes(), 1, false).await;
        } else {
            let mut body = string(&v.topic);
            body.extend([0, 7]);
            body.extend(v.id.as_bytes());
            p.send(codec::frame(0x32, &body)).await;
            protocol::closed(&mut p).await;
            (p, _) = Client::connect(addr, "vector-writer", true).await;
        }
        if v.filter_valid {
            // Same publisher and reliable FIFO: the marker is a positive barrier
            // for both matching and non-matching cases, without a timing guess.
            p.publish(marker, b"barrier", 1, false).await;
            if v.matches {
                let (topic, payload, header, id) = s.message().await;
                assert_eq!(
                    (topic, payload, header),
                    (v.topic, v.id.as_bytes().to_vec(), 0x32)
                );
                s.ack(id.unwrap()).await;
            }
            let (topic, payload, header, id) = s.message().await;
            assert_eq!(
                (topic.as_str(), payload.as_slice(), header),
                (marker, b"barrier".as_slice(), 0x32),
                "{}",
                v.id
            );
            s.ack(id.unwrap()).await;
            qos2::ping(&mut s).await;
            protocol::disconnect(&mut s).await;
        }
        protocol::disconnect(&mut p).await;
    }
    stop(&app).await;
}
