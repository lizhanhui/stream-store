//! Async offload worker for non-critical background tasks.
//!
//! Allows the hot append/replication path to delegate work (checksums,
//! monitoring, etc.) to a background worker without blocking critical
//! operations. The hot path sends an [`OffloadTask`] to the channel
//! via `try_send()` (~nanoseconds), and the worker processes it
//! asynchronously.

use std::sync::Arc;

use common::types::{ExtentId, StreamId};
use tokio::sync::{broadcast, mpsc};
use tracing::info;

use crate::store::ExtentNodeStore;

/// Extensible enum of async work offloaded from the critical append path.
#[derive(Debug, Clone)]
pub enum OffloadTask {
    /// Compute CRC32 of a sealed extent and send ForwardChecksum to secondaries.
    Checksum {
        stream_id: StreamId,
        extent_id: ExtentId,
    },
}

/// Background worker that processes [`OffloadTask`]s from the channel.
pub struct OffloadWorker;

impl OffloadWorker {
    /// Process tasks from the offload channel until shutdown is signalled.
    pub async fn run(
        store: Arc<ExtentNodeStore>,
        mut task_rx: mpsc::Receiver<OffloadTask>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) {
        loop {
            tokio::select! {
                task = task_rx.recv() => {
                    let Some(task) = task else { break };
                    Self::handle_task(&store, task).await;
                }
                _ = shutdown_rx.recv() => {
                    info!("OffloadWorker shutting down");
                    break;
                }
            }
        }
    }

    async fn handle_task(store: &ExtentNodeStore, task: OffloadTask) {
        match task {
            OffloadTask::Checksum {
                stream_id,
                extent_id,
            } => {
                if let Some((frame, addrs)) = store.build_checksum_frame(stream_id, extent_id) {
                    store.flush_forward_work(vec![(addrs, frame)]).await;
                }
            }
        }
    }
}
