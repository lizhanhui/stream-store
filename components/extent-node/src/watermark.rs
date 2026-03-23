//! WatermarkHandler: processes watermark events from secondary ExtentNodes and resolves pending ACKs.
//!
//! When the DownstreamManager receives a Watermark from a secondary ExtentNode, it sends a
//! WatermarkEvent to this handler. The handler accesses the per-stream AckQueue via DashMap
//! (fine-grained per-stream lock, not global) to:
//! 1. Record the cumulative ACK from the secondary via `ack_from_secondary`
//! 2. Compute the quorum offset and drain all pending ACKs that have reached quorum
//! 3. Send AppendAck frames through each pending ACK's per-connection response channel

use std::sync::Arc;

use tokio::sync::{broadcast, mpsc};
use tracing::{info, warn};

use crate::store::{ExtentNodeStore, WatermarkEvent};

/// Run the WatermarkHandler task.
///
/// Receives `WatermarkEvent`s from secondaries and resolves deferred client ACKs
/// by updating cumulative per-secondary offsets and draining the quorum.
///
/// Returns when the shutdown signal is received or the watermark channel is closed.
pub async fn run_watermark_handler(
    mut watermark_rx: mpsc::Receiver<WatermarkEvent>,
    store: Arc<ExtentNodeStore>,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    info!("WatermarkHandler started");

    loop {
        let event = tokio::select! {
            event = watermark_rx.recv() => {
                match event {
                    Some(event) => event,
                    None => break, // channel closed
                }
            }
            _ = shutdown_rx.recv() => {
                info!("WatermarkHandler received shutdown signal");
                break;
            }
        };

        if let Some(mut ack_queue) = store.ack_queues.get_mut(&event.stream_id) {
            // Record cumulative ACK from this secondary.
            ack_queue.ack_from_secondary(&event.source_addr, event.acked_offset);
            // Drain all pending ACKs that have reached quorum.
            ack_queue.drain_quorum();
        } else {
            warn!(
                "received watermark for stream {:?} but no ack_queue exists",
                event.stream_id
            );
        }
    }

    info!("WatermarkHandler shutting down");
}
