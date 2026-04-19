use crate::metadata::{MetadataStore, NodeRow};
use common::errors::{InternalSnafu, StorageError};
use common::types::NodeMetrics;

/// Stateless load-aware allocator that picks the least-loaded ExtentNode(s) for extent placement.
///
/// All metrics are read from the database on each allocation decision. Node metrics are
/// persisted by the heartbeat handler via `MetadataStore::persist_node_metrics()`.
/// Nodes with no metrics yet (just registered, first heartbeat not received) get a neutral
/// score of 0.5 so they are neither penalized nor aggressively preferred.
pub struct Allocator {
    store: MetadataStore,
}

impl Allocator {
    pub fn new(store: MetadataStore) -> Self {
        Self { store }
    }

    /// Compute a load score for a node. Lower is better (less loaded).
    ///
    /// Components (all normalized to [0.0, 1.0]):
    /// - Memory pressure: (1 - available/total).  Weight 0.25
    /// - Extent count:    active_extent_count / 100.  Weight 0.35
    /// - Write throughput: bytes_written_per_sec / 100MB.  Weight 0.25
    /// - Append rate:     appends_per_sec / 50000.  Weight 0.15
    ///
    /// If metrics are absent, return 0.5 (neutral) so new nodes are not
    /// penalized but also not aggressively preferred.
    fn score_node(metrics: Option<&NodeMetrics>) -> f64 {
        let metrics = match metrics {
            Some(m) => m,
            None => return 0.5,
        };

        // Memory pressure: fraction of memory used.
        let mem_pressure = if metrics.total_memory_bytes > 0 {
            1.0 - (metrics.available_memory_bytes as f64 / metrics.total_memory_bytes as f64)
        } else {
            0.5
        };

        // Normalize extent count. Cap at 100 for the denominator.
        let extent_load = (metrics.active_extent_count as f64) / 100.0;

        // Normalize bytes/s. Cap at 100 MB/s for the denominator.
        let bw_load = (metrics.bytes_written_per_sec as f64) / (100.0 * 1024.0 * 1024.0);

        // Normalize appends/s. Cap at 50_000 for the denominator.
        let append_load = (metrics.appends_per_sec as f64) / 50_000.0;

        // Weighted sum, clamp components to [0, 1].
        0.25 * mem_pressure.min(1.0)
            + 0.35 * extent_load.min(1.0)
            + 0.25 * bw_load.min(1.0)
            + 0.15 * append_load.min(1.0)
    }

    /// Pick up to `desired` distinct least-loaded alive nodes for replica set placement.
    /// Returns nodes sorted by load (least loaded first): [Primary, Secondary_1, ...].
    ///
    /// If fewer than `desired` nodes are alive but at least `min_count` are,
    /// returns the available nodes (degraded RF). This allows writes to continue
    /// after a node failure as long as quorum is still possible.
    ///
    /// Errors if fewer than `min_count` alive nodes are available.
    pub async fn pick_nodes(
        &self,
        desired: usize,
        min_count: usize,
    ) -> Result<Vec<NodeRow>, StorageError> {
        if desired == 0 || min_count == 0 {
            return Err(InternalSnafu {
                message: "replication_factor must be >= 1",
            }
            .build());
        }
        let alive = self.store.get_alive_nodes().await?;
        if alive.len() < min_count {
            return Err(InternalSnafu { message: format!(
                "need at least {min_count} alive ExtentNode nodes (desired {desired}), but only {} available",
                alive.len()
            ) }.build());
        }
        let count = desired.min(alive.len());

        // Load all node metrics in a single query.
        let all_metrics = self.store.get_all_node_metrics().await?;

        // Score all nodes and sort ascending (least loaded first).
        let mut scored: Vec<(usize, f64)> = alive
            .iter()
            .enumerate()
            .map(|(i, node)| (i, Self::score_node(all_metrics.get(&node.node_id))))
            .collect();
        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        // Pick top-`count` distinct nodes.
        let result: Vec<NodeRow> = scored
            .iter()
            .take(count)
            .map(|(i, _)| alive[*i].clone())
            .collect();

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn score_node_no_metrics() {
        let score = Allocator::score_node(None);
        assert!((score - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn score_node_idle() {
        let metrics = NodeMetrics {
            available_memory_bytes: 16_000_000_000,
            total_memory_bytes: 16_000_000_000,
            appends_per_sec: 0,
            active_extent_count: 0,
            bytes_written_per_sec: 0,
        };
        let score = Allocator::score_node(Some(&metrics));
        // mem_pressure = 0.0, all others = 0.0 => score ~ 0.0
        assert!(
            score < 0.01,
            "idle node score should be near 0, got {score}"
        );
    }

    #[test]
    fn score_node_loaded() {
        let metrics = NodeMetrics {
            available_memory_bytes: 1_000_000_000,
            total_memory_bytes: 16_000_000_000,
            appends_per_sec: 50_000,
            active_extent_count: 100,
            bytes_written_per_sec: 100 * 1024 * 1024,
        };
        let score = Allocator::score_node(Some(&metrics));
        // All components at or near max => score near 1.0
        assert!(
            score > 0.8,
            "loaded node score should be near 1.0, got {score}"
        );
    }

    #[test]
    fn score_prefers_lighter_node() {
        let light = NodeMetrics {
            available_memory_bytes: 14_000_000_000,
            total_memory_bytes: 16_000_000_000,
            appends_per_sec: 100,
            active_extent_count: 5,
            bytes_written_per_sec: 1_000_000,
        };

        let heavy = NodeMetrics {
            available_memory_bytes: 2_000_000_000,
            total_memory_bytes: 16_000_000_000,
            appends_per_sec: 40_000,
            active_extent_count: 80,
            bytes_written_per_sec: 80 * 1024 * 1024,
        };

        let light_score = Allocator::score_node(Some(&light));
        let heavy_score = Allocator::score_node(Some(&heavy));
        assert!(
            light_score < heavy_score,
            "light node ({light_score}) should score lower than heavy node ({heavy_score})"
        );
    }
}
