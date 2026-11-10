//! Public Bundle supervision does not turn malformed clients into Core failures.
use modorch::{
    ApplicationBuilder, ComponentSelection, FailurePolicy, ShutdownOptions, ShutdownReceipt,
};
use modorch_mqtt::{BrokerStatus, MqttBrokerBundle, MqttBrokerConfig, STATUS_TOPIC};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[tokio::test]
async fn failfast_bundle_is_explicit_and_malformed_connections_remain_local() {
    let config = MqttBrokerConfig {
        listener: "127.0.0.1:0".parse().unwrap(),
        ..MqttBrokerConfig::loopback_development()
    };
    let mut builder = ApplicationBuilder::new("bundle-failfast-host");
    let registered = MqttBrokerBundle::new(config)
        .with_failure_policy(FailurePolicy::FailFast)
        .register(&mut builder)
        .unwrap();
    assert!(registered.handle().local_addr().is_none());
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
    let generation = registered.handle().status().generation;
    let mut invalid = TcpStream::connect(address).await.unwrap();
    invalid.write_all(&[0x00, 0]).await.unwrap();
    let read = tokio::time::timeout(Duration::from_secs(2), invalid.read(&mut [0]))
        .await
        .unwrap();
    assert!(matches!(read, Ok(0)) || read.is_err());
    let mut valid = TcpStream::connect(address).await.unwrap();
    valid
        .write_all(&[
            0x10, 13, 0, 4, b'M', b'Q', b'T', b'T', 4, 2, 0, 0, 0, 1, b'x',
        ])
        .await
        .unwrap();
    let mut ack = [0; 4];
    tokio::time::timeout(Duration::from_secs(2), valid.read_exact(&mut ack))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ack, [0x20, 2, 0, 0]);
    let name = registered.module_ids().next().unwrap();
    let status = app
        .event_hub()
        .external()
        .request::<_, BrokerStatus>(
            STATUS_TOPIC,
            modorch::Destination::Component(modorch::ComponentRef::module(name)),
            (),
            Default::default(),
        )
        .unwrap()
        .wait(Duration::from_secs(2))
        .await
        .unwrap();
    assert!(status.active);
    assert_eq!(status.generation, generation);
    assert!(app.health().ready);
    assert!(app.health().failure.is_none());
    valid.write_all(&[0xe0, 0]).await.unwrap();
    assert_eq!(
        app.shutdown_checked(ShutdownOptions::graceful())
            .await
            .unwrap(),
        ShutdownReceipt::GracefullyStopped
    );
    assert_eq!(app.task_metrics().tasks, 0);
    assert_eq!(registered.handle().status().connections, 0);
    drop(TcpListener::bind(address).await.unwrap());
}
