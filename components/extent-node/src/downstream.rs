//! DownstreamPool: channel-based TCP connection pool for broadcast replication.
//!
//! Each secondary address gets a dedicated writer task that owns the TCP
//! `FramedWrite` exclusively — no per-writer Mutex. The leader pushes frames
//! into per-address bounded mpsc channels via `try_send` (non-blocking,
//! fire-and-forget), completely decoupling append latency from network I/O.
//!
//! Channels are created once per address and outlive individual TCP connections.
//! On TCP failure, the writer task reconnects in a loop — frames buffered in
//! the channel survive outages and are drained on reconnect.
//!
//! Streams cache cloned `Sender` handles at RegisterExtent time so
//! the hot append path pushes directly into channels with zero lookup overhead.
//!
//! Each connection also spawns a reader task that processes Watermark ACKs
//! inline, calling `ack_queue.drain_quorum()` directly on the store.

use futures_util::SinkExt;
use futures_util::StreamExt;
use socket2::{SockRef, TcpKeepalive};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_util::codec::FramedRead;

use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc};
use tokio_util::codec::FramedWrite;
use tracing::{error, info, warn};

use common::types::Opcode;
use rpc::codec::FrameCodec;
use rpc::frame::Frame;

use crate::store::ExtentNodeStore;

/// Handle to a dedicated per-address writer task.
struct WriterHandle {
    /// Bounded sender — leader pushes frames via try_send (never blocks).
    tx: mpsc::Sender<Frame>,
}

/// Channel-based TCP connection pool for broadcast replication.
///
/// One writer task per secondary address. Channels are created once per address
/// and outlive TCP connections. The writer task reconnects forever on failure.
///
/// Uses `std::sync::Mutex` (not tokio) because the critical section is a
/// HashMap lookup + Sender::clone — sub-microsecond, never awaits.
pub struct DownstreamPool {
    /// Per-address writer handles. Mutex protects the map for
    /// get-or-create atomicity.
    writers: Mutex<HashMap<String, WriterHandle>>,
    /// Shutdown signal for all spawned tasks (reader + writer).
    shutdown_tx: broadcast::Sender<()>,
    /// Back-reference to the store for inline watermark processing.
    store: Arc<ExtentNodeStore>,
}

/// Reconnect backoff: start at 100ms, double each attempt, cap at 5s.
const INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(5);

/// Default capacity for the per-secondary bounded channel.
/// Shared across all streams replicating to the same secondary.
const DEFAULT_DOWNSTREAM_CHANNEL_CAPACITY: usize = 1_048_576;

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
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
        let mut map = self.writers.lock().unwrap();
        map.clear(); // Drops senders, causing writer tasks to exit on next recv().
    }

    /// Get or create a `Sender` for the given secondary address.
    ///
    /// Called at `handle_register_extent` time (cold path). Returns a clone
    /// of the sender that can be cached in the Stream struct for zero-lookup
    /// inline pushes on the hot append path.
    ///
    /// If the address already has a live channel, returns a clone of the
    /// existing sender (multiple streams sharing the same secondary reuse
    /// the same channel). Otherwise creates the channel and spawns the
    /// writer task (which connects to TCP internally in its reconnect loop).
    pub fn get_or_create_sender(&self, addr: &str) -> mpsc::Sender<Frame> {
        let mut map = self.writers.lock().unwrap();

        // Return existing sender if alive.
        if let Some(handle) = map.get(addr) {
            if !handle.tx.is_closed() {
                return handle.tx.clone();
            }
            // Channel closed (shouldn't happen unless shutdown) — recreate.
        }

        // Create bounded channel + spawn writer task.
        let (tx, rx) = mpsc::channel::<Frame>(DEFAULT_DOWNSTREAM_CHANNEL_CAPACITY);

        let addr_owned = addr.to_string();
        let shutdown_rx = self.shutdown_tx.subscribe();
        let store = Arc::clone(&self.store);
        tokio::spawn(async move {
            downstream_writer_task(addr_owned, rx, shutdown_rx, store).await;
        });

        let sender = tx.clone();
        map.insert(addr.to_string(), WriterHandle { tx });
        info!("created downstream channel for secondary at {addr}");
        sender
    }
}

/// Connect to a secondary with TCP_NODELAY and keepalive.
async fn connect_tcp(addr: &str) -> Result<TcpStream, std::io::Error> {
    let stream = TcpStream::connect(addr).await?;

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

    Ok(stream)
}

/// Dedicated writer task for a single secondary address.
///
/// Reconnects forever on TCP failure — the bounded channel outlives any
/// individual TCP connection. Frames buffered during outages are drained
/// on reconnect. Only exits when the channel is closed (all senders dropped)
/// or the shutdown signal is received.
///
/// Within each TCP session, batches naturally: blocking `recv()` for the
/// first frame, then `try_recv()` drains all immediately available frames
/// before a single `flush()`.
async fn downstream_writer_task(
    addr: String,
    mut rx: mpsc::Receiver<Frame>,
    mut shutdown_rx: broadcast::Receiver<()>,
    store: Arc<ExtentNodeStore>,
) {
    let mut backoff = INITIAL_BACKOFF;

    'outer: loop {
        // ── Connect phase ──────────────────────────────────────────────
        let stream = loop {
            // Check shutdown / channel closed before attempting connect.
            if rx.is_closed() {
                break 'outer;
            }

            tokio::select! {
                biased;
                _ = shutdown_rx.recv() => break 'outer,
                result = connect_tcp(&addr) => {
                    match result {
                        Ok(s) => {
                            backoff = INITIAL_BACKOFF; // reset on success
                            info!("connected to secondary ExtentNode at {addr}");
                            break s;
                        }
                        Err(e) => {
                            warn!("failed to connect to secondary {addr}: {e}; retrying in {backoff:?}");
                            tokio::select! {
                                _ = tokio::time::sleep(backoff) => {}
                                _ = shutdown_rx.recv() => break 'outer,
                            }
                            backoff = (backoff * 2).min(MAX_BACKOFF);
                        }
                    }
                }
            }
        };

        let (read_half, write_half) = stream.into_split();
        let mut writer = FramedWrite::new(write_half, FrameCodec);

        // Spawn reader task for this TCP session (handles Watermark ACKs).
        let reader_store = Arc::clone(&store);
        let reader_addr = addr.clone();
        let reader_shutdown = shutdown_rx.resubscribe();
        let reader_handle = tokio::spawn(async move {
            downstream_reader_inline(reader_addr, read_half, reader_store, reader_shutdown).await;
        });

        // ── Drain-and-write phase ──────────────────────────────────────
        let mut should_exit = false;
        loop {
            // Wait for the first frame (or shutdown / channel closed).
            let first = tokio::select! {
                biased;
                _ = shutdown_rx.recv() => { should_exit = true; break; }
                frame = rx.recv() => match frame {
                    Some(f) => f,
                    None => { should_exit = true; break; } // channel closed
                },
            };

            // Feed the first frame.
            if let Err(e) = writer.feed(first).await {
                warn!("writer to {addr} feed error: {e}");
                break; // TCP error → reconnect
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
                break; // TCP error → reconnect
            }

            // Single flush for the entire batch.
            if let Err(e) = writer.flush().await {
                warn!("writer to {addr} flush error: {e}");
                break; // TCP error → reconnect
            }
        }

        // Clean up this TCP session's reader task.
        reader_handle.abort();

        if should_exit {
            break;
        }

        // TCP error — loop back to reconnect.
        warn!("writer to {addr} TCP session ended, reconnecting...");
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
/// Exits gracefully when the shutdown signal is received or the connection closes.
async fn downstream_reader_inline(
    addr: String,
    read_half: tokio::net::tcp::OwnedReadHalf,
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
