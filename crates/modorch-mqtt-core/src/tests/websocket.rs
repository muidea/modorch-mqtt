use super::*;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};

async fn connect(
    addr: SocketAddr,
    path: &str,
    protocol: bool,
) -> Result<tokio_tungstenite::WebSocketStream<TcpStream>, tokio_tungstenite::tungstenite::Error> {
    let mut request = format!("ws://{addr}{path}").into_client_request().unwrap();
    if protocol {
        request
            .headers_mut()
            .insert("Sec-WebSocket-Protocol", "mqtt".parse().unwrap());
    }
    let stream = TcpStream::connect(addr).await.unwrap();
    let (ws, response) = tokio_tungstenite::client_async(request, stream).await?;
    assert_eq!(response.headers()["Sec-WebSocket-Protocol"], "mqtt");
    Ok(ws)
}
#[tokio::test]
async fn websocket_binary_fragmented_mqtt_qos2_upgrade_isolation_and_shutdown() {
    let mut cfg = config();
    cfg.websocket = Some(WebSocketConfig::default());
    let (app, broker) = start(cfg).await;
    let address = broker.local_addr().unwrap();
    assert!(connect(address, "/bad", true).await.is_err());
    assert!(connect(address, "/mqtt", false).await.is_err());
    let mut ws = connect(address, "/mqtt", true).await.unwrap();
    // MQTT packet split across binary messages; WebSocket boundaries are not packet boundaries.
    ws.send(Message::Binary(vec![0x10, 13, 0, 4, b'M'].into()))
        .await
        .unwrap();
    ws.send(Message::Binary(
        vec![b'Q', b'T', b'T', 4, 2, 0, 0, 0, 1, b'w'].into(),
    ))
    .await
    .unwrap();
    assert_eq!(
        ws.next().await.unwrap().unwrap().into_data().as_ref(),
        [0x20, 2, 0, 0]
    );
    ws.send(Message::Binary(vec![0x82, 6, 0, 1, 0, 1, b'x', 2].into()))
        .await
        .unwrap();
    assert_eq!(
        ws.next().await.unwrap().unwrap().into_data().as_ref(),
        [0x90, 3, 0, 1, 2]
    );
    ws.send(Message::Binary(
        vec![0x34, 6, 0, 1, b'x', 0, 7, b'a'].into(),
    ))
    .await
    .unwrap();
    assert_eq!(
        ws.next().await.unwrap().unwrap().into_data().as_ref(),
        [0x50, 2, 0, 7]
    );
    assert_eq!(
        ws.next().await.unwrap().unwrap().into_data().as_ref(),
        [0x34, 6, 0, 1, b'x', 0, 1, b'a']
    );
    ws.send(Message::Binary(vec![0x62, 2, 0, 7, 0x50, 2, 0, 1].into()))
        .await
        .unwrap();
    assert_eq!(
        ws.next().await.unwrap().unwrap().into_data().as_ref(),
        [0x70, 2, 0, 7]
    );
    assert_eq!(
        ws.next().await.unwrap().unwrap().into_data().as_ref(),
        [0x62, 2, 0, 1]
    );
    ws.send(Message::Binary(vec![0x70, 2, 0, 1, 0xc0, 0].into()))
        .await
        .unwrap();
    assert_eq!(
        ws.next().await.unwrap().unwrap().into_data().as_ref(),
        [0xd0, 0]
    );
    let mut bad = connect(address, "/mqtt", true).await.unwrap();
    bad.send(Message::Text("not mqtt".into())).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), bad.next())
            .await
            .is_ok()
    );
    ws.send(Message::Ping(vec![1, 2].into())).await.unwrap();
    assert_eq!(
        ws.next().await.unwrap().unwrap(),
        Message::Pong(vec![1, 2].into())
    );
    stop(&app).await;
    assert!(TcpStream::connect(address).await.is_err());
}

#[cfg(all(feature = "tls", feature = "mqtt5"))]
#[tokio::test]
async fn websocket_over_tls_mqtt5_qos2_and_plaintext_isolation() {
    let mut cfg = tls::tls_config();
    cfg.websocket = Some(WebSocketConfig::default());
    cfg.mqtt5 = true;
    let (app, broker) = start(cfg).await;
    let address = broker.local_addr().unwrap();

    // Plaintext TCP/WS attempt receives a TLS alert/close rather than HTTP 101 without impacting wss.
    let mut plain = TcpStream::connect(address).await.unwrap();
    let _ = tokio::io::AsyncWriteExt::write_all(&mut plain, b"GET /mqtt HTTP/1.1\r\n\r\n").await;
    let mut alert = Vec::new();
    let _ = tokio::io::AsyncReadExt::read_to_end(&mut plain, &mut alert).await;
    assert!(!alert.starts_with(b"HTTP/1.1"));
    if let Some(&first) = alert.first() {
        assert_eq!(first, 0x15);
    }

    let socket = TcpStream::connect(address).await.unwrap();
    let tls_stream = tls::connector(true)
        .connect(
            tokio_rustls::rustls::pki_types::ServerName::try_from("localhost").unwrap(),
            socket,
        )
        .await
        .unwrap();
    let mut request = format!("wss://{address}/mqtt")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("Sec-WebSocket-Protocol", "mqtt".parse().unwrap());
    let (mut wss, response) = tokio_tungstenite::client_async(request, tls_stream)
        .await
        .unwrap();
    assert_eq!(response.headers()["Sec-WebSocket-Protocol"], "mqtt");

    // MQTT 5.0 CONNECT -> CONNACK
    let mut connect_body = string("MQTT");
    connect_body.extend([5, 2, 0, 0, 0]);
    connect_body.extend(string("wss-v5"));
    wss.send(Message::Binary(codec::frame(0x10, &connect_body).into()))
        .await
        .unwrap();
    let connack = wss.next().await.unwrap().unwrap().into_data();
    assert_eq!((connack[0], connack[2], connack[3]), (0x20, 0, 0));

    // MQTT 5.0 SUBSCRIBE ("wss/#", QoS 2, Subscription Identifier=5) -> SUBACK
    let mut sub_body = vec![0, 1, 2, 0x0b, 5];
    sub_body.extend(string("wss/#"));
    sub_body.push(2);
    wss.send(Message::Binary(codec::frame(0x82, &sub_body).into()))
        .await
        .unwrap();
    assert_eq!(
        wss.next().await.unwrap().unwrap().into_data().as_ref(),
        [0x90, 4, 0, 1, 0, 2]
    );

    // MQTT 5.0 QoS 2 PUBLISH + User Property -> full handshake + PING
    let mut prop = vec![0x26];
    prop.extend(string("k"));
    prop.extend(string("v"));
    let mut pub_body = string("wss/item");
    pub_body.extend([0, 7]);
    pub_body.push(prop.len() as u8);
    pub_body.extend(&prop);
    pub_body.extend(b"ok");
    wss.send(Message::Binary(codec::frame(0x34, &pub_body).into()))
        .await
        .unwrap();
    assert_eq!(
        wss.next().await.unwrap().unwrap().into_data().as_ref(),
        [0x50, 2, 0, 7]
    );
    let delivered = wss.next().await.unwrap().unwrap().into_data();
    assert_eq!(delivered[0], 0x34);
    let decoded_pub = crate::tests::mqtt5::decoded(delivered[0], &delivered[2..]);
    assert_eq!(
        (decoded_pub.topic.as_ref(), decoded_pub.payload.as_ref()),
        (b"wss/item".as_slice(), b"ok".as_slice())
    );
    let props = decoded_pub.properties.unwrap();
    assert_eq!(props.subscription_identifiers, vec![5]);
    assert_eq!(props.user_properties, vec![("k".into(), "v".into())]);
    wss.send(Message::Binary(
        [
            codec::frame(0x62, &[0, 7]),
            codec::frame(0x50, &decoded_pub.pkid.to_be_bytes()),
        ]
        .concat()
        .into(),
    ))
    .await
    .unwrap();
    assert_eq!(
        wss.next().await.unwrap().unwrap().into_data().as_ref(),
        [0x70, 2, 0, 7]
    );
    assert_eq!(
        wss.next().await.unwrap().unwrap().into_data().as_ref(),
        codec::frame(0x62, &decoded_pub.pkid.to_be_bytes()).as_slice()
    );
    wss.send(Message::Binary(
        [
            codec::frame(0x70, &decoded_pub.pkid.to_be_bytes()),
            vec![0xc0, 0],
        ]
        .concat()
        .into(),
    ))
    .await
    .unwrap();
    assert_eq!(
        wss.next().await.unwrap().unwrap().into_data().as_ref(),
        [0xd0, 0]
    );
    stop(&app).await;
}
