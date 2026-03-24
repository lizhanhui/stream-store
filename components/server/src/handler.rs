use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc};
use tokio_util::codec::Framed;
use tracing::{Instrument, error, info, info_span};

use rpc::codec::FrameCodec;
use rpc::frame::Frame;

/// Trait implemented by ExtentNode and StreamManager to handle incoming frames.
///
/// Returns `Some(Frame)` for immediate responses, or `None` when the response
/// is deferred (e.g., Primary ExtentNode waiting for replication watermark before ACKing).
///
/// `response_tx` is an optional per-connection channel for sending deferred responses.
/// When the handler returns `None`, it may later send frames through this channel.
pub trait RequestHandler: Send + Sync + 'static {
    fn handle_frame(
        &self,
        frame: Frame,
        response_tx: Option<&mpsc::Sender<Frame>>,
    ) -> impl std::future::Future<Output = Option<Frame>> + Send;
}

/// A TCP server identified by a name (e.g., "ExtentNode", "StreamManager").
///
/// The name is carried as a structured tracing span field so that all log output
/// within this server is automatically annotated with the server's identity.
///
/// Use [`Server::builder`] to construct a `Server` via the fluent builder API.
pub struct Server<H: RequestHandler> {
    name: String,
    listener: TcpListener,
    handler: Arc<H>,
    deferred: bool,
    shutdown_rx: Option<broadcast::Receiver<()>>,
}

impl<H: RequestHandler> Server<H> {
    /// Create a new [`ServerBuilder`] with the given name.
    ///
    /// The name identifies this server's role in log output (e.g., "ExtentNode",
    /// "StreamManager-us-west-1").
    pub fn builder(name: impl Into<String>) -> ServerBuilder<H> {
        ServerBuilder {
            name: name.into(),
            listener: None,
            handler: None,
            deferred: false,
            shutdown_rx: None,
        }
    }

    /// Run the accept loop.
    ///
    /// Returns when the shutdown signal is received (if configured), or runs
    /// forever if no shutdown receiver was provided.
    ///
    /// All log output is wrapped in a `server{name=...}` span.
    pub async fn run(mut self) {
        let span = info_span!("server", name = %self.name);
        async {
            info!(
                addr = %self.listener.local_addr().expect("listener has local addr"),
                deferred = self.deferred,
                "listening",
            );

            loop {
                let accept_result = if let Some(ref mut shutdown) = self.shutdown_rx {
                    tokio::select! {
                        result = self.listener.accept() => Some(result),
                        _ = shutdown.recv() => {
                            info!("shutdown signal received");
                            None
                        }
                    }
                } else {
                    Some(self.listener.accept().await)
                };

                match accept_result {
                    Some(Ok((stream, _addr))) => {
                        let handler = Arc::clone(&self.handler);
                        let deferred = self.deferred;
                        tokio::spawn(async move {
                            if deferred {
                                serve_connection_with_deferred(stream, handler).await;
                            } else {
                                serve_connection(stream, handler).await;
                            }
                        });
                    }
                    Some(Err(e)) => {
                        error!("accept error: {e}");
                    }
                    None => break, // shutdown
                }
            }

            info!("stopped");
        }
        .instrument(span)
        .await
    }
}

/// Builder for constructing a [`Server`].
pub struct ServerBuilder<H: RequestHandler> {
    name: String,
    listener: Option<TcpListener>,
    handler: Option<Arc<H>>,
    deferred: bool,
    shutdown_rx: Option<broadcast::Receiver<()>>,
}

impl<H: RequestHandler> ServerBuilder<H> {
    /// Set the TCP listener for the server.
    pub fn listener(mut self, listener: TcpListener) -> Self {
        self.listener = Some(listener);
        self
    }

    /// Set the request handler.
    pub fn handler(mut self, handler: Arc<H>) -> Self {
        self.handler = Some(handler);
        self
    }

    /// Enable deferred response mode (used by ExtentNode for quorum replication).
    ///
    /// Default is `false` (immediate response mode).
    pub fn deferred(mut self, deferred: bool) -> Self {
        self.deferred = deferred;
        self
    }

    /// Set a shutdown signal receiver for graceful termination.
    ///
    /// When the corresponding `broadcast::Sender` sends a value, the server
    /// stops accepting new connections and returns from `run()`.
    pub fn shutdown(mut self, shutdown_rx: broadcast::Receiver<()>) -> Self {
        self.shutdown_rx = Some(shutdown_rx);
        self
    }

    /// Build the [`Server`]. Panics if `listener` or `handler` is not set.
    pub fn build(self) -> Server<H> {
        Server {
            name: self.name,
            listener: self.listener.expect("Server requires a listener"),
            handler: self.handler.expect("Server requires a handler"),
            deferred: self.deferred,
            shutdown_rx: self.shutdown_rx,
        }
    }
}

/// Serve a single TCP connection: read frames, dispatch to handler, write responses.
async fn serve_connection<H: RequestHandler>(stream: TcpStream, handler: Arc<H>) {
    let peer = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".into());

    let span = info_span!("connection", peer = %peer);
    async {
        info!("accepted");

        let mut framed = Framed::new(stream, FrameCodec);

        while let Some(result) = framed.next().await {
            match result {
                Ok(frame) => {
                    let response = handler.handle_frame(frame, None).await;
                    if let Some(response) = response {
                        if let Err(e) = framed.send(response).await {
                            error!("failed to send response: {e}");
                            return;
                        }
                    }
                }
                Err(e) => {
                    error!("frame decode error: {e}");
                    return;
                }
            }
        }

        info!("closed");
    }
    .instrument(span)
    .await
}

/// Serve a connection with deferred response support.
///
/// Splits the TCP connection into a read task and a write task, connected by
/// a per-connection mpsc channel. This enables deferred responses: the handler
/// can return `None` for an Append, and later the WatermarkHandler sends the
/// AppendAck through the channel.
async fn serve_connection_with_deferred<H: RequestHandler>(stream: TcpStream, handler: Arc<H>) {
    let peer = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".into());

    let span = info_span!("connection", peer = %peer, mode = "deferred");
    async {
        info!("accepted");

        let (read_half, write_half) = stream.into_split();
        let (response_tx, mut response_rx) = mpsc::channel::<Frame>(256);

        // Write task: drain response channel, send frames to client.
        let write_span = info_span!("writer");
        let write_task = tokio::spawn(
            async move {
                let mut framed_write = tokio_util::codec::FramedWrite::new(write_half, FrameCodec);
                while let Some(frame) = response_rx.recv().await {
                    if let Err(e) = framed_write.send(frame).await {
                        error!("failed to send response: {e}");
                        return;
                    }
                }
                info!("done");
            }
            .instrument(write_span),
        );

        // Read task: read client frames, dispatch to handler.
        let mut framed_read = tokio_util::codec::FramedRead::new(read_half, FrameCodec);

        while let Some(result) = framed_read.next().await {
            match result {
                Ok(frame) => {
                    let response = handler.handle_frame(frame, Some(&response_tx)).await;
                    if let Some(response) = response {
                        if response_tx.send(response).await.is_err() {
                            error!("response channel closed");
                            break;
                        }
                    }
                }
                Err(e) => {
                    error!("frame decode error: {e}");
                    break;
                }
            }
        }

        // Drop the sender so the write task finishes.
        drop(response_tx);
        let _ = write_task.await;
        info!("closed");
    }
    .instrument(span)
    .await
}
