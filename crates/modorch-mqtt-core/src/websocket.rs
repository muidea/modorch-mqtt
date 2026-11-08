//! A bounded binary-message adapter; no proxy task or second MQTT parser.
use crate::{WebSocketConfig, transport::Stream};
use futures_util::{Sink, Stream as FuturesStream};
use modorch_mqtt_api::MqttError;
use std::{
    io,
    pin::Pin,
    task::{Context, Poll, ready},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Message, protocol::WebSocketConfig as WireConfig},
};

// Tungstenite fixes the callback error type to an unboxed HTTP response.
#[allow(clippy::result_large_err)]
pub async fn accept(
    stream: Stream,
    config: &WebSocketConfig,
    max: usize,
) -> Result<Stream, MqttError> {
    let path = config.path.clone();
    let wire = WireConfig::default()
        .read_buffer_size(4096)
        .write_buffer_size(0)
        .max_write_buffer_size(max.saturating_add(1024))
        .max_message_size(Some(max))
        .max_frame_size(Some(max));
    let socket = tokio_tungstenite::accept_hdr_async_with_config(
        stream,
        move |request: &tokio_tungstenite::tungstenite::handshake::server::Request,
              mut response: tokio_tungstenite::tungstenite::handshake::server::Response| {
            let protocols = request
                .headers()
                .get("Sec-WebSocket-Protocol")
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v.split(',').any(|p| p.trim() == "mqtt"));
            if request.uri().path_and_query().map(|p| p.as_str()) != Some(path.as_str())
                || !protocols
            {
                let mut rejected = tokio_tungstenite::tungstenite::http::Response::new(Some(
                    "MQTT WebSocket upgrade rejected".into(),
                ));
                *rejected.status_mut() =
                    tokio_tungstenite::tungstenite::http::StatusCode::BAD_REQUEST;
                return Err(rejected);
            }
            response
                .headers_mut()
                .insert("Sec-WebSocket-Protocol", "mqtt".parse().unwrap());
            Ok(response)
        },
        Some(wire),
    )
    .await
    .map_err(|_| MqttError::Protocol("WebSocket upgrade failed"))?;
    Ok(Box::new(BinaryStream {
        socket,
        input: bytes::Bytes::new(),
        offset: 0,
        writing: None,
    }))
}
struct BinaryStream {
    socket: WebSocketStream<Stream>,
    input: bytes::Bytes,
    offset: usize,
    writing: Option<usize>,
}
fn error(error: tokio_tungstenite::tungstenite::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}
impl AsyncRead for BinaryStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        loop {
            if self.offset < self.input.len() {
                let len = buf.remaining().min(self.input.len() - self.offset);
                buf.put_slice(&self.input[self.offset..self.offset + len]);
                self.offset += len;
                return Poll::Ready(Ok(()));
            }
            // Flush automatic Pong/Close responses, including when no MQTT write follows.
            ready!(Pin::new(&mut self.socket).poll_flush(cx)).map_err(error)?;
            match ready!(Pin::new(&mut self.socket).poll_next(cx)) {
                Some(Ok(Message::Binary(bytes))) => {
                    self.input = bytes;
                    self.offset = 0;
                }
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => (),
                Some(Ok(Message::Close(_))) | None => return Poll::Ready(Ok(())),
                Some(Ok(_)) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "MQTT requires binary WebSocket messages",
                    )));
                }
                Some(Err(e)) => return Poll::Ready(Err(error(e))),
            }
        }
    }
}
impl AsyncWrite for BinaryStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.writing.is_none() {
            ready!(Pin::new(&mut self.socket).poll_ready(cx)).map_err(error)?;
            Pin::new(&mut self.socket)
                .start_send(Message::Binary(bytes::Bytes::copy_from_slice(buf)))
                .map_err(error)?;
            self.writing = Some(buf.len());
        }
        // Do not acknowledge a write until it reached the underlying transport;
        // Core's frame deadline therefore includes WebSocket/TLS backpressure.
        ready!(Pin::new(&mut self.socket).poll_flush(cx)).map_err(error)?;
        Poll::Ready(Ok(self.writing.take().unwrap()))
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_flush(cx).map_err(error)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_close(cx).map_err(error)
    }
}
