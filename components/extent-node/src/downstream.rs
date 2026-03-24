//! DownstreamManager: manages TCP connections to secondary ExtentNode nodes for broadcast replication.
//!
//! One TCP connection per unique node_addr, shared across all streams that route through
//! that secondary node. The connection is bidirectional:
//! - Write half: sends forwarded Append frames (FLAG_FORWARDED set)
//! - Read half: receives cumulative Watermark ACKs, forwarded as WatermarkEvents

use std::collections::HashMap;

use futures_util::SinkExt;
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc};
use tokio_util::codec::FramedWrite;
use tracing::{error, info, warn};

use common::types::{FLAG_FORWARDED, Offset, Opcode};
use rpc::codec::FrameCodec;
use rpc::frame::Frame;

use crate::store::{ForwardRequest, WatermarkEvent};

/// Run the DownstreamManager task.
///
/// Continuously receives `ForwardRequest`s and sends them to the appropriate
/// secondary ExtentNode. Creates connections on demand and caches them by node address.
/// In broadcast mode, the Primary sends one ForwardRequest per secondary per append,
/// so this task naturally fans out to multiple connections.
///
/// Returns when the shutdown signal is received or the forward channel is closed.
pub async fn run_downstream_manager(
    mut forward_rx: mpsc::Receiver<ForwardRequest>,
    watermark_tx: mpsc::Sender<WatermarkEvent>,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    info!("DownstreamManager started");

    // Cache of write halves, keyed by secondary node address.
    let mut connections: HashMap<String, FramedWrite<tokio::net::tcp::OwnedWriteHalf, FrameCodec>> =
        HashMap::new();

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

        let addr = &req.downstream_addr;

        // Get or create connection to this secondary node.
        if !connections.contains_key(addr) {
            match create_downstream_connection(addr, watermark_tx.clone()).await {
                Ok(write_half) => {
                    connections.insert(addr.clone(), write_half);
                }
                Err(e) => {
                    error!("failed to connect to secondary {addr}: {e}");
                    continue;
                }
            }
        }

        // Build the forwarded Append frame.
        let frame = Frame {
            opcode: Opcode::Append,
            flags: FLAG_FORWARDED,
            stream_id: req.stream_id,
            offset: Offset(req.offset),
            payload: req.payload,
            ..Default::default()
        };

        // Send to secondary.
        let write_half = connections.get_mut(addr).unwrap();
        if let Err(e) = write_half.send(frame).await {
            warn!("failed to send to secondary {addr}: {e}; removing connection");
            connections.remove(addr);
        }
    }

    info!("DownstreamManager shutting down");
}

/// Create a new TCP connection to a secondary ExtentNode node.
/// Returns the write half for sending frames.
/// Spawns a background reader task that forwards Watermarks to the watermark channel.
async fn create_downstream_connection(
    addr: &str,
    watermark_tx: mpsc::Sender<WatermarkEvent>,
) -> Result<FramedWrite<tokio::net::tcp::OwnedWriteHalf, FrameCodec>, std::io::Error> {
    let stream = TcpStream::connect(addr).await?;
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
                if frame.opcode == Opcode::Watermark {
                    let event = WatermarkEvent {
                        stream_id: frame.stream_id,
                        acked_offset: frame.offset.0,
                        source_addr: addr.clone(),
                    };
                    if let Err(e) = watermark_tx.send(event).await {
                        error!("failed to send WatermarkEvent: {e}");
                        return;
                    }
                } else {
                    warn!("unexpected opcode {:?} from secondary {addr}", frame.opcode);
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
