use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;
use tracing::{error, info, warn};

use crate::store::StreamManagerStore;

/// Background task that manages SM leadership lease and checks for expired ExtentNode nodes.
///
/// Only the lease holder (leader) runs failover. All SM instances compete for
/// the lease on each check interval. For each expired node the leader:
/// 1. Marks the node as DEAD in metadata.
/// 2. Resolves the true committed offset from surviving replicas.
/// 3. Seals and allocates replacement extents with proper RF and RegisterExtent.
///
/// Returns when the shutdown signal is received.
pub async fn run_heartbeat_checker(
    sm_store: Arc<StreamManagerStore>,
    node_id: String,
    check_interval: Duration,
    lease_duration_secs: u32,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    info!("heartbeat checker started, interval={check_interval:?}");
    let mut is_leader = false;

    loop {
        // Acquire or renew leadership lease (runs immediately on first iteration
        // so the SM doesn't wait a full check_interval before competing).
        let store = sm_store.store();
        let lease_result = if is_leader {
            store.renew_leadership(&node_id, lease_duration_secs).await
        } else {
            store
                .try_acquire_leadership(&node_id, lease_duration_secs)
                .await
        };

        match lease_result {
            Ok(true) => {
                if !is_leader {
                    info!("acquired SM leadership lease");
                }
                is_leader = true;
            }
            Ok(false) => {
                if is_leader {
                    info!("lost SM leadership lease");
                }
                is_leader = false;
            }
            Err(e) => {
                warn!("leadership lease operation failed: {e}");
                is_leader = false;
            }
        }

        // Only check expired nodes if we hold the leadership lease.
        if is_leader {
            match check_expired_nodes(&sm_store).await {
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

        // Wait for next check interval or shutdown signal.
        tokio::select! {
            _ = tokio::time::sleep(check_interval) => {}
            _ = shutdown_rx.recv() => {
                info!("heartbeat checker received shutdown signal");
                if is_leader {
                    if let Err(e) = sm_store.store().release_leadership(&node_id).await {
                        warn!("failed to release leadership on shutdown: {e}");
                    }
                }
                break;
            }
        }
    }

    info!("heartbeat checker stopped");
}

/// Check for expired nodes and handle failover. Returns the number of dead nodes found.
///
/// For each active extent on the dead node:
/// 1. Resolve the committed offset by querying surviving replicas (Primary if alive,
///    otherwise secondary quorum). Falls back to metadata end_offset if all replicas
///    are unreachable.
/// 2. Bump epoch (replica set is changing due to node failure).
/// 3. Seal-and-allocate with proper replication factor, RegisterExtent to new Primary,
///    and fire-and-forget notify to secondaries.
async fn check_expired_nodes(
    sm_store: &Arc<StreamManagerStore>,
) -> Result<usize, common::errors::StorageError> {
    let store = sm_store.store();
    let expired = store.get_expired_nodes().await?;
    let dead_count = expired.len();

    for node in expired {
        info!("node {} expired, marking DEAD", node.node_id);

        // Mark node as dead.
        store.mark_node_dead(&node.node_id).await?;

        // Get all active extents on this dead node and seal them.
        let active_extents = store.get_active_extents_on_node(&node.addr).await?;

        for extent in &active_extents {
            info!(
                "failover: sealing extent {} on dead node {} (stream {})",
                extent.extent_id, node.node_id, extent.stream_id
            );

            // Resolve the true committed offset from surviving replicas.
            // This contacts the Primary (if alive) or uses secondary quorum.
            let committed_offset = match sm_store
                .resolve_committed_offset(
                    extent.stream_id,
                    extent.extent_id,
                    extent.start_offset,
                    extent.epoch,
                )
                .await
            {
                Ok(offset) => {
                    info!(
                        "failover: resolved committed offset={offset} for extent {} stream {}",
                        extent.extent_id, extent.stream_id
                    );
                    offset
                }
                Err(e) => {
                    // All replicas unreachable — fall back to metadata end_offset.
                    // This may lose data but is the best we can do.
                    warn!(
                        "failover: resolve_committed_offset failed for extent {} stream {}: {e}; \
                         falling back to metadata end_offset={}",
                        extent.extent_id, extent.stream_id, extent.end_offset
                    );
                    extent.end_offset
                }
            };

            // Bump epoch since the replica set is changing due to node failure.
            // CAS guard prevents double-bump if another SM raced us.
            let new_epoch = match store.bump_epoch(extent.stream_id).await {
                Ok(epoch) => epoch,
                Err(e) => {
                    warn!(
                        "failover: bump_epoch failed for stream {} (concurrent failover?): {e}",
                        extent.stream_id
                    );
                    continue;
                }
            };

            // Seal-and-allocate with proper RF, RegisterExtent to new Primary,
            // and fire-and-forget notify to secondaries.
            match sm_store
                .seal_allocate_register(
                    extent.stream_id,
                    extent.extent_id,
                    committed_offset,
                    new_epoch,
                )
                .await
            {
                Ok((new_extent_id, primary_addr)) => {
                    info!(
                        "failover: replacement extent {} allocated on {primary_addr} for stream {} (epoch={})",
                        new_extent_id, extent.stream_id, new_epoch
                    );
                }
                Err(e) => {
                    warn!(
                        "failover: seal_allocate_register failed for stream {}: {e}",
                        extent.stream_id
                    );
                }
            }
        }
    }

    Ok(dead_count)
}
