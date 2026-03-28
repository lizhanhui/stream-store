//! DownstreamManager: manages TCP connections to secondary ExtentNode nodes for broadcast replication.
//!
//! One TCP connection per unique node_addr, shared across all streams that route through
//! that secondary node. The connection is bidirectional:
//! - Write half: sends forwarded Append frames (FLAG_FORWARDED set)
//! - Read half: receives cumulative Watermark ACKs, forwarded as WatermarkEvents
//!
//! Each secondary gets its own forwarding task with a dedicated mpsc channel,
//! so a stalled secondary cannot block other secondaries (Fix 4).
//! Send failures trigger a single reconnect-and-retry before giving up (Fix 2).
//! TCP keepalive detects half-open connections after power failures (Fix 5).

use std::collections::HashMap;
use std::time::Duration;

use futures_util::SinkExt;
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc};
use tokio_util::codec::FramedWrite;
use tracing::{error, info, warn};

use common::types::{FLAG_FORWARDED, Opcode};
use rpc::codec::FrameCodec;
use rpc::frame::{Frame, VariableHeader};

use crate::store::{ForwardRequest, WatermarkEvent};

/// Capacity for each per-connection forwarding channel.
const PER_CONNECTION_CHANNEL_CAPACITY: usize = 1024;

/// Wrapper for a per-connection forwarding channel.
struct DownstreamConnection {
    tx: mpsc::Sender<Frame>,
}

/// Run the DownstreamManager task.
///
/// Continuously receives `ForwardRequest`s and routes them to per-connection
/// forwarding tasks. Each secondary gets its own task with a dedicated channel,
/// isolating secondaries from each other (a stalled secondary only blocks its
/// own channel, not all streams).
///
/// Returns when the shutdown signal is received or the forward channel is closed.
pub async fn run_downstream_manager(
    mut forward_rx: mpsc::Receiver<ForwardRequest>,
    watermark_tx: mpsc::Sender<WatermarkEvent>,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    info!("DownstreamManager started");

    // Per-connection forwarding tasks, keyed by secondary node address.
    let mut connections: HashMap<String, DownstreamConnection> = HashMap::new();

    loop {
        let req = tokio::select! {
            req = forward_rx.recv() => {
                match req {
                    Some(req) => req,
                    None => break, // channel closed
                }
            }
            _ = shutdown_rx.recv() => {
                info!("DownstreamManager received shutdown signal");
                break;
            }
        };

        let addr = req.downstream_addr.clone();

        // Build the forwarded Append frame.
        let mut frame = Frame::new(
            VariableHeader::Append {
                request_id: 0,
                stream_id: req.stream_id,
                extent_id: req.extent_id,
            },
            Some(req.payload),
        );
        frame.header.flags = FLAG_FORWARDED;

        // Get or create per-connection forwarding task.
        let conn = connections.entry(addr.clone()).or_insert_with(|| {
            let (tx, rx) = mpsc::channel(PER_CONNECTION_CHANNEL_CAPACITY);
            spawn_connection_writer(addr.clone(), rx, watermark_tx.clone());
            DownstreamConnection { tx }
        });

        // Non-blocking send to per-connection channel.
        // If the channel is full, this secondary is backpressured — log and drop.
        // Fix 1's PendingAck timeout ensures the client eventually gets an error.
        if let Err(e) = conn.tx.try_send(frame) {
            warn!(
                "per-connection channel full for secondary {}: {e}; frame dropped",
                req.downstream_addr,
            );
        }
    }

    info!("DownstreamManager shutting down");
}

/// Spawn a per-connection writer task that owns a TCP connection to a secondary.
///
/// Receives frames from a dedicated mpsc channel and sends them sequentially.
/// On send failure, reconnects once and retries (Fix 2). If retry also fails,
/// the frame is dropped and Fix 1's timeout handles the client-facing impact.
fn spawn_connection_writer(
    addr: String,
    mut rx: mpsc::Receiver<Frame>,
    watermark_tx: mpsc::Sender<WatermarkEvent>,
) {
    tokio::spawn(async move {
        let mut writer: Option<FramedWrite<tokio::net::tcp::OwnedWriteHalf, FrameCodec>> = None;

        while let Some(frame) = rx.recv().await {
            // Ensure we have a connection.
            if writer.is_none() {
                match create_downstream_connection(&addr, watermark_tx.clone()).await {
                    Ok(w) => writer = Some(w),
                    Err(e) => {
                        error!("failed to connect to secondary {addr}: {e}; dropping frame");
                        continue;
                    }
                }
            }

            // Try to send the frame.
            let w = writer.as_mut().unwrap();
            if let Err(e) = w.send(frame.clone()).await {
                warn!("send to secondary {addr} failed: {e}; reconnecting");
                writer = None;

                // Retry once with a fresh connection (Fix 2).
                match create_downstream_connection(&addr, watermark_tx.clone()).await {
                    Ok(mut new_writer) => {
                        if let Err(e) = new_writer.send(frame).await {
                            warn!("retry send to {addr} failed: {e}; giving up on frame");
                            // Connection is likely dead, drop it.
                        } else {
                            writer = Some(new_writer);
                        }
                    }
                    Err(e) => {
                        error!("reconnect to secondary {addr} failed: {e}");
                    }
                }
            }
        }

        info!("connection writer for {addr} shutting down");
    });
}

/// Create a new TCP connection to a secondary ExtentNode node.
/// Returns the write half for sending frames.
/// Spawns a background reader task that forwards Watermarks to the watermark channel.
///
/// Sets TCP keepalive with aggressive timers to detect half-open connections
/// after power failures (Fix 5).
async fn create_downstream_connection(
    addr: &str,
    watermark_tx: mpsc::Sender<WatermarkEvent>,
) -> Result<FramedWrite<tokio::net::tcp::OwnedWriteHalf, FrameCodec>, std::io::Error> {
    let stream = TcpStream::connect(addr).await?;

    // Disable Nagle's algorithm: Watermark responses are small frames (~20 bytes)
    // that would otherwise be delayed up to 40ms by Nagle buffering.
    stream.set_nodelay(true)?;

    // Set TCP keepalive to detect half-open connections (Fix 5).
    // After 10s idle, probe every 5s. This detects dead peers within ~25s
    // instead of the default 2+ hours.
    let sock_ref = socket2::SockRef::from(&stream);
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(Duration::from_secs(10))
        .with_interval(Duration::from_secs(5));
    sock_ref.set_tcp_keepalive(&keepalive)?;

    let (read_half, write_half) = stream.into_split();

    let framed_write = FramedWrite::new(write_half, FrameCodec);

    // Spawn reader task for this connection.
    let addr_owned = addr.to_string();
    tokio::spawn(async move {
        downstream_reader(addr_owned, read_half, watermark_tx).await;
    });

    info!("connected to secondary ExtentNode at {addr}");
    Ok(framed_write)
}

/// Reader task for a single secondary connection.
/// Reads cumulative Watermark ACKs and forwards them as WatermarkEvents
/// with the source address so the Primary's AckQueue can track per-secondary offsets.
async fn downstream_reader(
    addr: String,
    read_half: tokio::net::tcp::OwnedReadHalf,
    watermark_tx: mpsc::Sender<WatermarkEvent>,
) {
    use futures_util::StreamExt;
    use tokio_util::codec::FramedRead;

    let mut framed_read = FramedRead::new(read_half, FrameCodec);

    while let Some(result) = framed_read.next().await {
        match result {
            Ok(frame) => {
                if frame.opcode() == Opcode::Watermark {
                    let event = WatermarkEvent {
                        stream_id: frame.stream_id(),
                        acked_offset: frame.offset().0,
                        source_addr: addr.clone(),
                    };
                    if let Err(e) = watermark_tx.send(event).await {
                        error!("failed to send WatermarkEvent: {e}");
                        return;
                    }
                } else {
                    warn!(
                        "unexpected opcode {:?} from secondary {addr}",
                        frame.opcode()
                    );
                }
            }
            Err(e) => {
                error!("secondary {addr} read error: {e}");
                return;
            }
        }
    }

    info!("secondary {addr} reader closed");
}
