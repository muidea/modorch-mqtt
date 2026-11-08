use super::*;
use std::io::Cursor;
use tokio_rustls::{
    TlsConnector,
    rustls::{self, pki_types::ServerName},
};

pub(super) fn tls_config() -> MqttBrokerConfig {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    MqttBrokerConfig {
        tls: Some(TlsConfig {
            certificate_chain: root.join("localhost-cert.pem"),
            private_key: root.join("localhost-key.pem"),
        }),
        ..config()
    }
}
pub(super) fn connector(trusted: bool) -> TlsConnector {
    let mut roots = rustls::RootCertStore::empty();
    if trusted {
        let certs = rustls_pemfile::certs(&mut Cursor::new(include_bytes!(
            "../../tests/fixtures/localhost-cert.pem"
        )))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
        for cert in certs {
            roots.add(cert).unwrap();
        }
    }
    TlsConnector::from(Arc::new(
        rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth(),
    ))
}
#[tokio::test]
async fn real_tls_qos1_roundtrip_and_untrusted_or_plaintext_clients_are_local_failures() {
    let (app, handle) = start(tls_config()).await;
    let address = handle.local_addr().unwrap();
    let socket = TcpStream::connect(address).await.unwrap();
    let mut tls = connector(true)
        .connect(ServerName::try_from("localhost").unwrap(), socket)
        .await
        .unwrap();
    let mut body = string("MQTT");
    body.extend([4, 2, 0, 0]);
    body.extend(string("tls-client"));
    tls.write_all(&codec::frame(0x10, &body)).await.unwrap();
    let mut response = [0; 4];
    tls.read_exact(&mut response).await.unwrap();
    assert_eq!(response, [0x20, 2, 0, 0]);
    let mut body = vec![0, 1];
    body.extend(string("tls/topic"));
    body.push(1);
    tls.write_all(&codec::frame(0x82, &body)).await.unwrap();
    let mut response = [0; 5];
    tls.read_exact(&mut response).await.unwrap();
    assert_eq!(response, [0x90, 3, 0, 1, 1]);
    let mut body = string("tls/topic");
    body.extend([0, 2]);
    body.extend(b"encrypted-payload");
    tls.write_all(&codec::frame(0x32, &body)).await.unwrap();
    let mut response = [0; 4];
    tls.read_exact(&mut response).await.unwrap();
    assert_eq!(response, [0x40, 2, 0, 2]);
    let message = tokio::time::timeout(Duration::from_secs(2), async {
        assert_eq!(tls.read_u8().await.unwrap(), 0x32);
        let len = tls.read_u8().await.unwrap() as usize;
        let mut body = vec![0; len];
        tls.read_exact(&mut body).await.unwrap();
        body
    })
    .await
    .unwrap();
    assert!(message.ends_with(b"encrypted-payload"));
    let socket = TcpStream::connect(address).await.unwrap();
    assert!(
        connector(false)
            .connect(ServerName::try_from("localhost").unwrap(), socket)
            .await
            .is_err()
    );
    let mut plain = TcpStream::connect(address).await.unwrap();
    plain
        .write_all(&[0x10, 12, 0, 4, b'M', b'Q', b'T', b'T', 4, 2, 0, 0, 0, 0])
        .await
        .unwrap();
    let mut alert = Vec::new();
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        plain.take(4096).read_to_end(&mut alert),
    )
    .await
    .unwrap();
    assert!(
        result.is_ok()
            || result
                .as_ref()
                .is_err_and(|e| e.kind() == std::io::ErrorKind::ConnectionReset)
    );
    assert!(alert.is_empty() || alert[0] == 21); // TLS alert, never an MQTT success reply
    assert_eq!(handle.status().generation, 1);
    stop(&app).await;
    assert_eq!(handle.status().connections, 0);
}
#[tokio::test]
async fn tls_handshake_deadline_cancellation_and_invalid_files_release_owned_resources() {
    let mut config = tls_config();
    config.handshake_timeout = Duration::from_millis(80);
    let (app, handle) = start(config).await;
    let mut socket = TcpStream::connect(handle.local_addr().unwrap())
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), socket.read(&mut [0]))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    let _half_open = TcpStream::connect(handle.local_addr().unwrap())
        .await
        .unwrap();
    stop(&app).await;
    assert_eq!(handle.status().connections, 0);
    let mut config = tls_config();
    config.tls.as_mut().unwrap().private_key = "nonexistent-test-private-key.pem".into();
    let factory = MqttBrokerFactory::new(config).unwrap();
    let handle = factory.handle();
    let mut builder = ApplicationBuilder::new("tls-failure");
    builder.register_module(factory).unwrap();
    let mut selected = ComponentSelection::new();
    selected.enable_module("mqtt-broker");
    assert!(
        builder
            .selection(selected)
            .build()
            .unwrap()
            .start()
            .await
            .is_err()
    );
    assert!(!handle.status().active);
    assert_eq!(handle.status().connections, 0);
    assert!(handle.local_addr().is_none());
}
#[test]
fn public_listener_requires_auth_and_tls_together() {
    let mut config = tls_config();
    config.listener = "0.0.0.0:8883".parse().unwrap();
    assert!(config.validate().is_err());
    config.anonymous_access = false;
    config.auth = Some(AuthProviderConfig {
        component: "auth".into(),
        timeout: Duration::from_secs(1),
    });
    assert!(config.validate().is_ok());
    config.tls = None;
    assert!(config.validate().is_err());
}

#[tokio::test]
async fn tls_reload_is_atomic_failed_reload_preserves_certificate_and_existing_sessions() {
    let config = tls_config();
    let tls_files = config.tls.clone().unwrap();
    let (app, handle) = start(config).await;
    let address = handle.local_addr().unwrap();
    let connect = || async {
        let tcp = TcpStream::connect(address).await.unwrap();
        connector(true)
            .connect(ServerName::try_from("localhost").unwrap(), tcp)
            .await
            .unwrap()
    };
    let mut first = connect().await;
    first
        .write_all(&[
            0x10, 13, 0, 4, b'M', b'Q', b'T', b'T', 4, 2, 0, 0, 0, 1, b'r',
        ])
        .await
        .unwrap();
    let mut ack = [0; 4];
    first.read_exact(&mut ack).await.unwrap();
    assert_eq!(ack, [0x20, 2, 0, 0]);
    let mut missing = tls_files.clone();
    missing.private_key = "/nonexistent/mqtt-reload-key".into();
    assert!(handle.reload_tls(missing).await.is_err());
    handle.reload_tls(tls_files.clone()).await.unwrap();
    first.write_all(&[0xc0, 0]).await.unwrap();
    let mut pong = [0; 2];
    first.read_exact(&mut pong).await.unwrap();
    assert_eq!(pong, [0xd0, 0]);
    let _new = connect().await;
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    handle
        .reload_tls(TlsConfig {
            certificate_chain: root.join("localhost-rotated-cert.pem"),
            private_key: root.join("localhost-rotated-key.pem"),
        })
        .await
        .unwrap();
    let tcp = TcpStream::connect(address).await.unwrap();
    assert!(
        connector(true)
            .connect(ServerName::try_from("localhost").unwrap(), tcp)
            .await
            .is_err()
    );
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut Cursor::new(include_bytes!(
        "../../tests/fixtures/localhost-rotated-cert.pem"
    ))) {
        roots.add(cert.unwrap()).unwrap();
    }
    let new_trust = TlsConnector::from(Arc::new(
        rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth(),
    ));
    let _rotated = new_trust
        .connect(
            ServerName::try_from("localhost").unwrap(),
            TcpStream::connect(address).await.unwrap(),
        )
        .await
        .unwrap();
    first.write_all(&[0xc0, 0]).await.unwrap();
    first.read_exact(&mut pong).await.unwrap();
    assert_eq!(pong, [0xd0, 0]);
    stop(&app).await;
    assert!(handle.reload_tls(tls_files).await.is_err());
}
