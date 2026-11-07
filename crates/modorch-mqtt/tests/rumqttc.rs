//! Independent MQTT client implementation compatibility, without using our codec.
use modorch::{ApplicationBuilder, ComponentSelection, ShutdownOptions};
use modorch_mqtt::{MqttBrokerBundle, MqttBrokerConfig};
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS};
use std::time::Duration;

#[tokio::test]
async fn rumqttc_qos0_qos1_retained_self_delivery_and_graceful_stop() {
    let config = MqttBrokerConfig {
        listener: "127.0.0.1:0".parse().unwrap(),
        ..MqttBrokerConfig::loopback_development()
    };
    let mut builder = ApplicationBuilder::new("rumqttc-host");
    let registered = MqttBrokerBundle::new(config)
        .register(&mut builder)
        .unwrap();
    let mut selected = ComponentSelection::new();
    for id in registered.module_ids() {
        selected.enable_module(id);
    }
    let app = builder
        .selection(selected)
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();
    let address = registered.handle().local_addr().unwrap();
    let mut options = MqttOptions::new("rumqttc-client", address.ip().to_string(), address.port());
    options.set_keep_alive(Duration::from_secs(2));
    options.set_clean_session(false);
    let (client, mut events) = AsyncClient::new(options, 16);
    client
        .subscribe("interop/+", QoS::AtLeastOnce)
        .await
        .unwrap();
    let mut received = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match events.poll().await.unwrap() {
                Event::Incoming(Incoming::SubAck(_)) => {
                    client
                        .publish("interop/qos0", QoS::AtMostOnce, false, "zero")
                        .await
                        .unwrap();
                    client
                        .publish("interop/qos1", QoS::AtLeastOnce, true, "one")
                        .await
                        .unwrap();
                }
                Event::Incoming(Incoming::Publish(message)) => {
                    received.push((message.topic, message.payload.to_vec()));
                    if received.len() == 2 {
                        break;
                    }
                }
                _ => {}
            }
        }
    })
    .await
    .unwrap();
    received.sort();
    assert_eq!(
        received,
        [
            ("interop/qos0".into(), b"zero".to_vec()),
            ("interop/qos1".into(), b"one".to_vec())
        ]
    );
    assert_eq!(registered.handle().status().retained, 1);
    client.disconnect().await.unwrap();
    // Keep polling to actually transmit DISCONNECT, rather than just enqueue it.
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if matches!(
                events.poll().await.unwrap(),
                Event::Outgoing(rumqttc::Outgoing::Disconnect)
            ) {
                break;
            }
        }
    })
    .await
    .unwrap();
    app.shutdown_checked(ShutdownOptions::graceful())
        .await
        .unwrap();
    assert_eq!(registered.handle().status().connections, 0);
}
