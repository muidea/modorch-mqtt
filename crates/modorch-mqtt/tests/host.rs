#![cfg(feature = "bootstrap")]
use modorch::{ShutdownOptions, ShutdownReceipt};
use modorch_mqtt::{MqttBrokerConfig, MqttBrokerHost, MqttHostError};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[tokio::test]
async fn tokio_adapter_isolates_applications_and_releases_ports_on_checked_shutdown() {
    let cfg = || MqttBrokerConfig {
        listener: "127.0.0.1:0".parse().unwrap(),
        ..MqttBrokerConfig::loopback_development()
    };
    let first = MqttBrokerHost::start(cfg()).await.unwrap();
    let second = MqttBrokerHost::start(cfg()).await.unwrap();
    let address = first.broker().local_addr().unwrap();
    assert_ne!(address, second.broker().local_addr().unwrap());
    let mut socket = TcpStream::connect(address).await.unwrap();
    socket
        .write_all(&[
            0x10, 13, 0, 4, b'M', b'Q', b'T', b'T', 4, 0, 0, 0, 0, 1, b'x',
        ])
        .await
        .unwrap();
    let mut ack = [0; 4];
    socket.read_exact(&mut ack).await.unwrap();
    assert_eq!(ack, [0x20, 2, 0, 0]);
    assert_eq!(first.broker().status().sessions, 1);
    assert_eq!(second.broker().status().sessions, 0);
    assert_eq!(
        first
            .shutdown_checked(ShutdownOptions::graceful())
            .await
            .unwrap(),
        ShutdownReceipt::GracefullyStopped
    );
    let listener = TcpListener::bind(address).await.unwrap();
    drop(listener);
    assert!(second.broker().status().active);
    second
        .shutdown_checked(ShutdownOptions::graceful())
        .await
        .unwrap();
}
#[tokio::test]
async fn tokio_adapter_preserves_failed_startup_cleanup_and_retry_handle() {
    let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = MqttBrokerConfig {
        listener: occupied.local_addr().unwrap(),
        ..MqttBrokerConfig::loopback_development()
    };
    match MqttBrokerHost::start(config).await {
        Err(MqttHostError::Startup(error)) => {
            assert!(error.cleanup.is_ok());
            error
                .handle
                .shutdown_checked(ShutdownOptions::graceful())
                .await
                .unwrap();
        }
        _ => panic!("expected structured startup error"),
    }
}
