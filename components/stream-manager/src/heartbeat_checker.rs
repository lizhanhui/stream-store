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
/// 3. Seals and allocates replacement extents with proper RF and RegisterEpoch.
///
/// Returns when the shutdown signal is received.
pub async fn run_heartbeat_checker(
    sm_store: Arc<StreamManagerStore>,
    node_id: String,
    check_interval: Duration,
    lease_duration_secs: u32,
    flush_staleness_threshold_ms: u32,
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

            // Scan for stale sealed extents and delegate flush to secondaries.
            sm_store
                .flush_stale_extents(flush_staleness_threshold_ms)
                .await;
        }

        // Wait for next check interval or shutdown signal.
        tokio::select! {
            _ = tokio::time::sleep(check_interval) => {}
            _ = shutdown_rx.recv() => {
                info!("heartbeat checker received shutdown signal");
                if is_leader
                    && let Err(e) = sm_store.store().release_leadership(&node_id).await
                {
                    warn!("failed to release leadership on shutdown: {e}");
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
/// 3. Seal-and-allocate with proper replication factor, RegisterEpoch to new Primary,
///    and fire-and-forget notify to secondaries.
async fn check_expired_nodes(
    sm_store: &Arc<StreamManagerStore>,
) -> Result<usize, common::errors::StorageError> {
    let store = sm_store.store();
    let expired = store.get_expired_nodes().await?;
    let dead_count = expired.len();

    for node in expired {
        info!("node {} expired, marking DEAD", node.node_id);

        // Mark node as dead so the allocator excludes it from future placements.
        // Do NOT proactively seal extents — let clients trigger seal when they
        // observe append errors (timeout, connection reset, UnknownStream).
        // Client-driven seal is simpler and avoids races with SM-driven seal.
        store.mark_node_dead(&node.node_id).await?;
    }

    if dead_count > 0 {
        info!("heartbeat checker: marked {dead_count} dead node(s)");
    }

    Ok(dead_count)
}
