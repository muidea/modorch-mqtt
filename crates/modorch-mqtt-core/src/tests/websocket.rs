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
