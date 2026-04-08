//! DownstreamPool: channel-based TCP connection pool for broadcast replication.
//!
//! Each secondary address gets a dedicated writer task that owns the TCP
//! `FramedWrite` exclusively — no per-writer Mutex. The leader pushes frames
//! into per-address unbounded mpsc channels and returns immediately (fire-and-
//! forget), completely decoupling append latency from network I/O.
//!
//! Each connection also spawns a reader task that processes Watermark ACKs
//! inline, calling `ack_queue.drain_quorum()` directly on the store.
//!
//! Connections are lazily created on first `send_frames()` call. On write
//! failure the writer task exits; the next `send_frames()` detects the closed
//! channel and triggers a lazy reconnect.

use futures_util::SinkExt;
use futures_util::StreamExt;
use socket2::{SockRef, TcpKeepalive};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::codec::FramedRead;

use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{Mutex, broadcast, mpsc};
use tokio_util::codec::FramedWrite;
use tracing::{error, info, warn};

use common::types::Opcode;
use rpc::codec::FrameCodec;
use rpc::frame::Frame;

use crate::store::ExtentNodeStore;

/// Handle to a dedicated per-address writer task.
struct WriterHandle {
    /// Unbounded sender — leader pushes frames here, never blocks.
    tx: mpsc::UnboundedSender<Frame>,
}

/// Channel-based TCP connection pool for broadcast replication.
///
/// One writer task per secondary address. The leader pushes frames into
/// unbounded mpsc channels; each writer task drains its channel, feeds
/// frames into `FramedWrite`, and flushes independently.
pub struct DownstreamPool {
    /// Per-address writer handles. Outer Mutex protects the map for
    /// get-or-create atomicity (prevents TOCTOU on connection creation).
    writers: Mutex<HashMap<String, WriterHandle>>,
    /// Shutdown signal for all spawned tasks (reader + writer).
    shutdown_tx: broadcast::Sender<()>,
    /// Back-reference to the store for inline watermark processing.
    store: Arc<ExtentNodeStore>,
}

impl DownstreamPool {
    /// Create a new pool with a back-reference to the store.
    pub fn new(store: Arc<ExtentNodeStore>) -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);
        Self {
            writers: Mutex::new(HashMap::new()),
            shutdown_tx,
            store,
        }
    }

    /// Shut down the pool: signal all tasks and clear writer handles.
    pub async fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
        let mut map = self.writers.lock().await;
        map.clear(); // Drops senders, causing writer tasks to exit on next recv().
    }

    /// Send frames to a secondary address. Fire-and-forget: pushes into
    /// the per-address unbounded channel. Creates the connection lazily
    /// if it doesn't exist yet.
    ///
    /// The map Mutex is held only for HashMap lookup + channel sends in
    /// the common case (sub-microsecond). On first use or after reconnect,
    /// it is held across TCP connect to prevent TOCTOU races.
    pub async fn send_frames(&self, addr: &str, frames: Vec<Frame>) {
        let mut map = self.writers.lock().await;

        // Check for existing live handle.
        let need_create = match map.get(addr) {
            Some(h) if !h.tx.is_closed() => false,
            _ => true,
        };

        if need_create {
            // Remove stale entry if present.
            map.remove(addr);
            // Create connection + spawn tasks while holding the lock.
            match self.create_and_spawn(addr).await {
                Some(h) => {
                    map.insert(addr.to_string(), h);
                }
                None => return, // connection failed, drop frames
            }
        }

        let handle = map.get(addr).unwrap();
        for frame in frames {
            if handle.tx.send(frame).is_err() {
                // Writer task died between the check and here — remove stale handle.
                map.remove(addr);
                break;
            }
        }
    }

    /// Create a TCP connection and spawn reader + writer tasks.
    ///
    /// Sets TCP_NODELAY and keepalive. Returns a `WriterHandle` whose
    /// channel feeds the dedicated writer task.
    async fn create_and_spawn(&self, addr: &str) -> Option<WriterHandle> {
        let stream = match TcpStream::connect(addr).await {
            Ok(s) => s,
            Err(e) => {
                error!("failed to connect to secondary {addr}: {e}; dropping frames");
                return None;
            }
        };

        // Disable Nagle's algorithm for low-latency small frames.
        if let Err(e) = stream.set_nodelay(true) {
            warn!("set_nodelay failed for {addr}: {e}");
        }

        // Set TCP keepalive to detect half-open connections quickly.
        let sock_ref = SockRef::from(&stream);
        let keepalive = TcpKeepalive::new()
            .with_time(Duration::from_secs(5))
            .with_interval(Duration::from_secs(2));
        if let Err(e) = sock_ref.set_tcp_keepalive(&keepalive) {
            warn!("set_tcp_keepalive failed for {addr}: {e}");
        }

        let (read_half, write_half) = stream.into_split();
        let framed_write = FramedWrite::new(write_half, FrameCodec);

        let (tx, rx) = mpsc::unbounded_channel::<Frame>();

        // Spawn reader task (handles Watermark ACKs inline — unchanged).
        let store = Arc::clone(&self.store);
        let addr_owned = addr.to_string();
        let shutdown_rx_reader = self.shutdown_tx.subscribe();
        tokio::spawn(async move {
            downstream_reader_inline(addr_owned, read_half, store, shutdown_rx_reader).await;
        });

        // Spawn dedicated writer task.
        let addr_writer = addr.to_string();
        let shutdown_rx_writer = self.shutdown_tx.subscribe();
        tokio::spawn(async move {
            downstream_writer_task(addr_writer, framed_write, rx, shutdown_rx_writer).await;
        });

        info!("connected to secondary ExtentNode at {addr}");
        Some(WriterHandle { tx })
    }
}

/// Dedicated writer task for a single secondary address.
///
/// Owns the `FramedWrite` exclusively — no Mutex needed. Drains frames
/// from the unbounded channel, feeds them into the TCP writer, and flushes.
/// Batches naturally: after the first blocking `recv()`, drains all
/// immediately available frames via `try_recv()` before a single `flush()`.
///
/// On write error, logs and exits. The sender side will detect the closed
/// channel on the next `send_frames()` call and trigger a lazy reconnect.
async fn downstream_writer_task(
    addr: String,
    mut writer: FramedWrite<OwnedWriteHalf, FrameCodec>,
    mut rx: mpsc::UnboundedReceiver<Frame>,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    loop {
        // Wait for the first frame (or shutdown).
        let first = tokio::select! {
            frame = rx.recv() => match frame {
                Some(f) => f,
                None => break, // channel closed
            },
            _ = shutdown_rx.recv() => break,
        };

        // Feed the first frame.
        if let Err(e) = writer.feed(first).await {
            warn!("writer to {addr} feed error: {e}");
            break;
        }

        // Drain all immediately available frames (batch for single flush).
        let mut feed_err = false;
        loop {
            match rx.try_recv() {
                Ok(frame) => {
                    if let Err(e) = writer.feed(frame).await {
                        warn!("writer to {addr} feed error: {e}");
                        feed_err = true;
                        break;
                    }
                }
                Err(_) => break, // channel empty, proceed to flush
            }
        }
        if feed_err {
            break;
        }

        // Single flush for the entire batch.
        if let Err(e) = writer.flush().await {
            warn!("writer to {addr} flush error: {e}");
            break;
        }
    }

    info!("writer task for {addr} exiting");
    rx.close();
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
