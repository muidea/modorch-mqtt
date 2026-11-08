use crate::MqttBrokerConfig;
use modorch::{CancellationToken, FrameworkResult, TaskScheduler};
use modorch_mqtt_api::MqttError;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    time::Instant,
};

pub(crate) trait Socket: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Socket for T {}
pub(crate) type Stream = Box<dyn Socket>;

#[derive(Clone, Default)]
pub(crate) struct Transport {
    #[cfg(feature = "tls")]
    acceptor: std::sync::Arc<std::sync::RwLock<Option<tokio_rustls::TlsAcceptor>>>,
    websocket: Option<crate::WebSocketConfig>,
    max_packet_bytes: usize,
}
impl Transport {
    pub async fn prepare(
        config: &MqttBrokerConfig,
        tasks: &TaskScheduler,
    ) -> FrameworkResult<Self> {
        #[cfg(feature = "tls")]
        if let Some(tls) = &config.tls {
            let tls = tls.clone();
            let acceptor = tasks
                .spawn_blocking_io(move |cancel| {
                    Ok((|| {
                        use std::{
                            io::{Cursor, Read},
                            sync::Arc,
                        };
                        use tokio_rustls::rustls;
                        let fail = || crate::framework("invalid or unreadable TLS certificate/key");
                        let read = |path: &std::path::Path| {
                            let mut bytes = Vec::new();
                            std::fs::File::open(path)
                                .map_err(|_| fail())?
                                .take(1_048_577)
                                .read_to_end(&mut bytes)
                                .map_err(|_| fail())?;
                            if bytes.len() > 1_048_576 || cancel.is_cancelled() {
                                return Err(fail());
                            }
                            Ok(bytes)
                        };
                        let cert = read(&tls.certificate_chain)?;
                        let key = read(&tls.private_key)?;
                        let certs = rustls_pemfile::certs(&mut Cursor::new(cert))
                            .collect::<Result<Vec<_>, _>>()
                            .map_err(|_| fail())?;
                        let key = rustls_pemfile::private_key(&mut Cursor::new(key))
                            .map_err(|_| fail())?
                            .ok_or_else(fail)?;
                        let provider = Arc::new(rustls::crypto::ring::default_provider());
                        let mut server = rustls::ServerConfig::builder_with_provider(provider)
                            .with_safe_default_protocol_versions()
                            .map_err(|_| fail())?
                            .with_no_client_auth()
                            .with_single_cert(certs, key)
                            .map_err(|_| fail())?;
                        server.max_early_data_size = 0;
                        Ok(tokio_rustls::TlsAcceptor::from(Arc::new(server)))
                    })())
                })?
                .wait_finished()
                .await??;
            return Ok(Self {
                acceptor: std::sync::Arc::new(std::sync::RwLock::new(Some(acceptor))),
                websocket: config.websocket.clone(),
                max_packet_bytes: config.max_packet_bytes,
            });
        }
        let _ = (config, tasks);
        Ok(Self {
            websocket: config.websocket.clone(),
            max_packet_bytes: config.max_packet_bytes,
            ..Default::default()
        })
    }
    #[cfg(feature = "tls")]
    pub fn replace_tls(&self, loaded: &Self) {
        *self.acceptor.write().unwrap() = loaded.acceptor.read().unwrap().clone();
    }
    pub async fn accept(
        &self,
        stream: TcpStream,
        cancel: &CancellationToken,
        deadline: Instant,
    ) -> Result<Stream, MqttError> {
        let stream: Stream = Box::new(stream);
        #[cfg(feature = "tls")]
        let acceptor = self.acceptor.read().unwrap().clone();
        #[cfg(feature = "tls")]
        let stream: Stream = if let Some(acceptor) = acceptor {
            tokio::select! { biased;
                _ = cancel.cancelled() => return Err(MqttError::Closed),
                _ = tokio::time::sleep_until(deadline) => return Err(MqttError::Protocol("TLS handshake deadline")),
                stream = acceptor.accept(stream) => {
                    let mut stream = stream?;
                    stream.get_mut().1.set_buffer_limit(Some(65536));
                    Box::new(stream)
                }
            }
        } else {
            stream
        };
        #[cfg(feature = "websocket")]
        if let Some(config) = &self.websocket {
            return tokio::select! { biased;
                _ = cancel.cancelled() => Err(MqttError::Closed),
                _ = tokio::time::sleep_until(deadline) => Err(MqttError::Protocol("WebSocket handshake deadline")),
                stream = crate::websocket::accept(stream, config, self.max_packet_bytes) => stream,
            };
        }
        let _ = (cancel, deadline, &self.websocket, self.max_packet_bytes);
        Ok(stream)
    }
}

/// A socket may be closed while the same managed task drains a durable commit.
/// Poll methods hold the mutex only for the immediate, nonblocking IO poll.
#[derive(Clone)]
pub(crate) struct Interruptible(std::sync::Arc<std::sync::Mutex<Option<Stream>>>);
impl Interruptible {
    pub fn new(stream: Stream) -> Self {
        Self(std::sync::Arc::new(std::sync::Mutex::new(Some(stream))))
    }
    pub fn close(&self) {
        let stream = self.0.lock().unwrap().take();
        drop(stream);
    }
}
impl AsyncRead for Interruptible {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.0.lock().unwrap().as_mut() {
            Some(stream) => std::pin::Pin::new(stream).poll_read(cx, buf),
            None => std::task::Poll::Ready(Ok(())),
        }
    }
}
impl AsyncWrite for Interruptible {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.0.lock().unwrap().as_mut() {
            Some(stream) => std::pin::Pin::new(stream).poll_write(cx, buf),
            None => std::task::Poll::Ready(Err(std::io::ErrorKind::BrokenPipe.into())),
        }
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.0.lock().unwrap().as_mut() {
            Some(stream) => std::pin::Pin::new(stream).poll_flush(cx),
            None => std::task::Poll::Ready(Err(std::io::ErrorKind::BrokenPipe.into())),
        }
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.0.lock().unwrap().as_mut() {
            Some(stream) => std::pin::Pin::new(stream).poll_shutdown(cx),
            None => std::task::Poll::Ready(Ok(())),
        }
    }
}
