use std::sync::Arc;
use std::time::Duration;

use crate::metadata::MetadataStore;
use tokio::sync::{Mutex, broadcast};
use tracing::{error, info, warn};

use crate::allocator::Allocator;

/// Background task that checks for expired ExtentNode nodes and handles failover.
///
/// For each expired node:
/// 1. Mark the node as DEAD in metadata.
/// 2. Seal all active extents on that node.
/// 3. Allocate replacement extents on alive nodes.
///
/// Returns when the shutdown signal is received.
pub async fn run_heartbeat_checker(
    store: MetadataStore,
    allocator: Arc<Mutex<Allocator>>,
    check_interval: Duration,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    info!("heartbeat checker started, interval={check_interval:?}");

    loop {
        tokio::select! {
            _ = tokio::time::sleep(check_interval) => {}
            _ = shutdown_rx.recv() => {
                info!("heartbeat checker received shutdown signal");
                break;
            }
        }

        match check_expired_nodes(&store, &allocator).await {
            Ok(dead_count) => {
                if dead_count > 0 {
                    warn!("heartbeat checker: handled {dead_count} dead node(s)");
                }
            }
            Err(e) => {
                error!("heartbeat checker error: {e}");
            }
        }
    }

    info!("heartbeat checker stopped");
}

/// Check for expired nodes and handle failover. Returns the number of dead nodes found.
async fn check_expired_nodes(
    store: &MetadataStore,
    allocator: &Arc<Mutex<Allocator>>,
) -> Result<usize, common::errors::StorageError> {
    let expired = store.get_expired_nodes().await?;
    let dead_count = expired.len();

    for node in expired {
        info!("node {} expired, marking DEAD", node.node_id);

        // 1. Mark node as dead and clean up in-memory metrics.
        store.mark_node_dead(&node.node_id).await?;
        {
            let mut alloc = allocator.lock().await;
            alloc.remove_metrics(&node.node_id);
        }

        // 2. Get all active extents on this dead node and seal them.
        let active_extents = store.get_active_extents_on_node(&node.addr).await?;

        for extent in &active_extents {
            info!(
                "sealing extent {:?} on dead node {} (stream {:?})",
                extent.extent_id, node.node_id, extent.stream_id
            );
            // Seal with current message_count (we can't know the exact count
            // from ExtentNode since it's dead; record what metadata has).
            store
                .seal_extent(extent.stream_id, extent.extent_id, extent.message_count)
                .await?;

            // 3. Allocate a replacement extent on a healthy node.
            let new_base_offset = extent.base_offset + extent.message_count as u64;
            let mut alloc = allocator.lock().await;
            match alloc.pick_node(store).await {
                Ok(target) => {
                    let replicas = vec![(target.addr.clone(), 0u8)];
                    let new_extent = store
                        .allocate_extent(extent.stream_id, new_base_offset, &replicas)
                        .await?;
                    info!(
                        "replacement extent {:?} allocated on {} for stream {:?}",
                        new_extent, target.addr, extent.stream_id
                    );
                }
                Err(e) => {
                    warn!(
                        "cannot allocate replacement extent for stream {:?}: {e}",
                        extent.stream_id
                    );
                }
            }
        }
    }

    Ok(dead_count)
}
