use std::sync::Arc;

use futures_util::{FutureExt, SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc};
use tokio_util::codec::Framed;
use tracing::{Instrument, error, info, info_span};

use common::types::Opcode;
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

    /// Handle a batch of Append frames targeting the same stream/extent.
    ///
    /// Default implementation falls back to per-frame `handle_frame()`.
    /// `ExtentNodeStore` overrides this with an optimized path that amortizes
    /// DashMap lookups, leader elections, and ReplicaInfo access across the batch.
    fn handle_append_batch(
        &self,
        frames: &[Frame],
        response_tx: Option<&mpsc::Sender<Frame>>,
    ) -> impl std::future::Future<Output = Vec<Frame>> + Send {
        async move {
            let mut responses = Vec::new();
            for frame in frames {
                if let Some(resp) = self.handle_frame(frame.clone(), response_tx).await {
                    responses.push(resp);
                }
            }
            responses
        }
    }
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
                        // Disable Nagle's algorithm for low-latency RPC.
                        let _ = stream.set_nodelay(true);
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
///
/// The read task uses greedy batching: when it receives an Append frame, it
/// peeks ahead for more Appends targeting the same stream/extent (via
/// `now_or_never()`) and dispatches them as a single batch through
/// `handle_append_batch()`. This amortizes DashMap lookups and leader elections.
///
/// The write task uses feed+flush batching: it feeds all immediately-available
/// response frames into the codec buffer, then flushes once per batch to reduce
/// syscalls.
async fn serve_connection_with_deferred<H: RequestHandler>(stream: TcpStream, handler: Arc<H>) {
    let peer = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".into());

    let span = info_span!("connection", peer = %peer, mode = "deferred");
    async {
        info!("accepted");

        let (read_half, write_half) = stream.into_split();
        let (response_tx, mut response_rx) = mpsc::channel::<Frame>(1024);

        // Write task: drain response channel, batch feed+flush to reduce syscalls.
        let write_span = info_span!("writer");
        let write_task = tokio::spawn(
            async move {
                let mut framed_write = tokio_util::codec::FramedWrite::new(write_half, FrameCodec);
                while let Some(frame) = response_rx.recv().await {
                    // Feed the first frame without flushing.
                    if let Err(e) = framed_write.feed(frame).await {
                        error!("failed to write response: {e}");
                        return;
                    }
                    // Drain all immediately-available frames without blocking.
                    while let Ok(frame) = response_rx.try_recv() {
                        if let Err(e) = framed_write.feed(frame).await {
                            error!("failed to write response: {e}");
                            return;
                        }
                    }
                    // Single flush for the entire batch.
                    if let Err(e) = framed_write.flush().await {
                        error!("failed to flush responses: {e}");
                        return;
                    }
                }
                info!("done");
            }
            .instrument(write_span),
        );

        // Read task: read client frames, batch consecutive same-extent Appends.
        let mut framed_read = tokio_util::codec::FramedRead::new(read_half, FrameCodec);
        let mut look_ahead: Option<Frame> = None;

        'outer: loop {
            // Get the next frame: from look_ahead or from the wire.
            let frame = if let Some(f) = look_ahead.take() {
                f
            } else {
                match framed_read.next().await {
                    Some(Ok(f)) => f,
                    Some(Err(e)) => {
                        error!("frame decode error: {e}");
                        break;
                    }
                    None => break, // connection closed
                }
            };

            if frame.opcode() == Opcode::Append {
                let target_stream = frame.stream_id();
                let target_extent = frame.extent_id();
                let mut batch = vec![frame];

                // Greedily extend: peek next frame, only take if same-extent Append.
                while let Some(result) = framed_read.next().now_or_never() {
                    match result {
                        Some(Ok(next)) => {
                            if next.opcode() == Opcode::Append
                                && next.stream_id() == target_stream
                                && next.extent_id() == target_extent
                            {
                                batch.push(next);
                            } else {
                                // Save for next iteration — don't process inline.
                                look_ahead = Some(next);
                                break;
                            }
                        }
                        Some(Err(e)) => {
                            error!("frame decode error: {e}");
                            break 'outer;
                        }
                        None => break 'outer, // connection closed
                    }
                }

                let responses = handler
                    .handle_append_batch(&batch, Some(&response_tx))
                    .await;
                for response in responses {
                    if response_tx.send(response).await.is_err() {
                        error!("response channel closed");
                        break 'outer;
                    }
                }
            } else {
                // Non-append: process individually.
                if let Some(resp) = handler.handle_frame(frame, Some(&response_tx)).await {
                    if response_tx.send(resp).await.is_err() {
                        error!("response channel closed");
                        break;
                    }
                }
            }
        }

        // Drop the sender so the write task finishes.
        drop(response_tx);
        // Abort the write task in case PendingAck clones keep the channel alive
        // after the client disconnects. Without this, the write task blocks forever
        // on response_rx.recv() because the channel never fully closes.
        write_task.abort();
        let _ = write_task.await;
        info!("closed");
    }
    .instrument(span)
    .await
}
