//! DownstreamPool: direct TCP connection pool for broadcast replication.
//!
//! Replaces the former DownstreamManager + WatermarkHandler tasks with a
//! zero-channel-hop design:
//! - `forward()` / `forward_batch()` write frames directly to the TCP socket
//!   (holding an async Mutex on the writer — no intermediate mpsc channel).
//! - Each connection spawns a reader task that processes Watermark ACKs inline,
//!   calling `ack_queue.drain_quorum()` directly on the store — no WatermarkEvent
//!   channel.
//!
//! Connections are lazily created on first forward. On write failure, a single
//! reconnect-and-retry is attempted before giving up (timeout handles client impact).

use futures_util::StreamExt;
use socket2::{SockRef, TcpKeepalive};
use std::collections::HashMap;
use std::io::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::codec::FramedRead;

use futures_util::SinkExt;
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{Mutex, broadcast};
use tokio_util::codec::FramedWrite;
use tracing::{error, info, warn};

use common::types::Opcode;
use rpc::codec::FrameCodec;
use rpc::frame::Frame;

use crate::store::ExtentNodeStore;

/// Direct TCP connection pool for broadcast replication.
///
/// One writer per secondary address. Writers are behind `tokio::sync::Mutex`
/// so concurrent callers serialize on the same connection (keeps TCP ordering).
pub struct DownstreamPool {
    /// Per-address TCP writers. Outer Mutex for the map, inner Mutex per writer.
    connections: Mutex<HashMap<String, Arc<Mutex<FramedWrite<OwnedWriteHalf, FrameCodec>>>>>,
    /// Shutdown signal for reader tasks. Each spawned reader subscribes to this.
    shutdown_tx: broadcast::Sender<()>,
    /// Back-reference to the store for inline watermark processing.
    store: Arc<ExtentNodeStore>,
}

impl DownstreamPool {
    /// Create a new pool with a back-reference to the store.
    pub fn new(store: Arc<ExtentNodeStore>) -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);
        Self {
            connections: Mutex::new(HashMap::new()),
            shutdown_tx,
            store,
        }
    }

    /// Shut down the pool: signal all reader tasks and clear connections.
    pub async fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
        let mut conns = self.connections.lock().await;
        conns.clear();
    }

    /// Forward a single frame to a secondary address.
    ///
    /// Gets or creates a TCP connection, locks the writer, feeds + flushes.
    /// On failure, reconnects once and retries.
    pub async fn forward(&self, addr: &str, frame: Frame) {
        let writer = self.get_or_create_writer(addr).await;
        let Some(writer) = writer else { return };

        let mut guard = writer.lock().await;
        if let Err(e) = guard.feed(frame.clone()).await {
            warn!("send to secondary {addr} failed: {e}; reconnecting");
            drop(guard);
            // Reconnect once.
            if let Some(new_writer) = self.reconnect(addr).await {
                let mut guard = new_writer.lock().await;
                if let Err(e) = guard.send(frame).await {
                    warn!("retry send to {addr} failed: {e}; giving up on frame");
                }
            }
            return;
        }
        if let Err(e) = guard.flush().await {
            warn!("flush to secondary {addr} failed: {e}; reconnecting");
            drop(guard);
            let _ = self.reconnect(addr).await;
        }
    }

    /// Forward a batch of frames to a secondary address: feed all, flush once.
    pub async fn forward_batch(&self, addr: &str, frames: &[Frame]) {
        if frames.is_empty() {
            return;
        }
        let writer = self.get_or_create_writer(addr).await;
        let Some(writer) = writer else { return };

        let mut guard = writer.lock().await;
        for frame in frames {
            if let Err(e) = guard.feed(frame.clone()).await {
                warn!("send to secondary {addr} failed during batch: {e}; reconnecting");
                drop(guard);
                // Reconnect and retry remaining frames individually.
                if let Some(new_writer) = self.reconnect(addr).await {
                    let mut g = new_writer.lock().await;
                    // Best-effort: try to send the failed frame.
                    let _ = g.send(frame.clone()).await;
                }
                return;
            }
        }
        if let Err(e) = guard.flush().await {
            warn!("flush to secondary {addr} failed: {e}; reconnecting");
            drop(guard);
            let _ = self.reconnect(addr).await;
        }
    }

    /// Get an existing writer or create a new connection.
    ///
    /// Holds the connections lock across `create_connection` to prevent multiple
    /// concurrent TCP connections to the same address. Without this, concurrent
    /// callers could each create a connection, and Forward frames sent on an
    /// orphaned (overwritten) connection would arrive at the secondary before
    /// ForwardInitExtent on the surviving connection — causing permanent gaps.
    pub async fn get_or_create_writer(
        &self,
        addr: &str,
    ) -> Option<Arc<Mutex<FramedWrite<OwnedWriteHalf, FrameCodec>>>> {
        let mut conns = self.connections.lock().await;
        if let Some(writer) = conns.get(addr) {
            return Some(Arc::clone(writer));
        }

        // Create connection while holding the lock — no other caller can race us.
        match self.create_connection(addr).await {
            Ok(writer) => {
                let writer = Arc::new(Mutex::new(writer));
                conns.insert(addr.to_string(), Arc::clone(&writer));
                Some(writer)
            }
            Err(e) => {
                error!("failed to connect to secondary {addr}: {e}; dropping frame");
                None
            }
        }
    }

    /// Reconnect to a secondary: remove old writer, create new connection.
    async fn reconnect(
        &self,
        addr: &str,
    ) -> Option<Arc<Mutex<FramedWrite<OwnedWriteHalf, FrameCodec>>>> {
        // Remove the old (broken) writer.
        {
            let mut conns = self.connections.lock().await;
            conns.remove(addr);
        }

        match self.create_connection(addr).await {
            Ok(writer) => {
                let writer = Arc::new(Mutex::new(writer));
                let mut conns = self.connections.lock().await;
                conns.insert(addr.to_string(), Arc::clone(&writer));
                Some(writer)
            }
            Err(e) => {
                error!("reconnect to secondary {addr} failed: {e}");
                None
            }
        }
    }

    /// Create a new TCP connection to a secondary ExtentNode.
    /// Sets TCP_NODELAY and keepalive. Spawns a reader task for inline Watermark handling.
    async fn create_connection(
        &self,
        addr: &str,
    ) -> Result<FramedWrite<OwnedWriteHalf, FrameCodec>, Error> {
        let stream = TcpStream::connect(addr).await?;

        // Disable Nagle's algorithm for low-latency small frames.
        stream.set_nodelay(true)?;

        // Set TCP keepalive to detect half-open connections quickly.
        let sock_ref = SockRef::from(&stream);
        let keepalive = TcpKeepalive::new()
            .with_time(Duration::from_secs(5))
            .with_interval(Duration::from_secs(2));
        sock_ref.set_tcp_keepalive(&keepalive)?;

        let (read_half, write_half) = stream.into_split();
        let framed_write = FramedWrite::new(write_half, FrameCodec);

        // Spawn reader task that handles Watermarks INLINE (no channel hop).
        // Receives a shutdown signal to exit gracefully instead of being aborted.
        let store = Arc::clone(&self.store);
        let addr_owned = addr.to_string();
        let shutdown_rx = self.shutdown_tx.subscribe();
        tokio::spawn(async move {
            downstream_reader_inline(addr_owned, read_half, store, shutdown_rx).await;
        });

        info!("connected to secondary ExtentNode at {addr}");
        Ok(framed_write)
    }
}

/// Reader task for a single secondary connection.
///
/// Reads cumulative Watermark ACKs and processes them INLINE:
/// directly updates the per-stream AckQueue and drains quorum,
/// eliminating the WatermarkEvent channel hop.
///
/// Exits gracefully when the shutdown signal is received.
async fn downstream_reader_inline(
    addr: String,
    read_half: OwnedReadHalf,
    store: Arc<ExtentNodeStore>,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    let mut framed_read = FramedRead::new(read_half, FrameCodec);

    loop {
        let result = tokio::select! {
            frame = framed_read.next() => frame,
            _ = shutdown_rx.recv() => {
                info!("secondary {addr} reader received shutdown signal");
                break;
            }
        };

        match result {
            Some(Ok(frame)) => {
                if frame.opcode() == Opcode::Watermark {
                    let stream_id = frame.stream_id();
                    let acked_offset = frame.offset().0;

                    // Inline watermark processing — no channel hop.
                    if let Some(mut ack_queue) = store.ack_queues.get_mut(&stream_id) {
                        ack_queue.ack_from_secondary(&addr, acked_offset);
                        ack_queue.drain_quorum();
                    } else {
                        warn!(
                            "received watermark for stream {:?} but no ack_queue exists",
                            stream_id
                        );
                    }
                } else {
                    warn!(
                        "unexpected opcode {:?} from secondary {addr}",
                        frame.opcode()
                    );
                }
            }
            Some(Err(e)) => {
                error!("secondary {addr} read error: {e}");
                break;
            }
            None => break, // connection closed
        }
    }

    // Secondary is gone — expire stale PendingAcks immediately so clients get
    // timely error responses instead of waiting for SM heartbeat-based recovery.
    store.expire_pending_acks();

    info!("secondary {addr} reader closed");
}
