use std::collections::HashMap;

use crate::metadata::{MetadataStore, NodeRow};
use common::errors::StorageError;
use common::types::NodeMetrics;

/// Load-aware allocator that picks the least-loaded ExtentNode node(s) for extent placement.
///
/// Each heartbeat from an extent-node carries runtime metrics (memory, throughput, extent count).
/// The allocator scores each node based on these metrics and picks the least-loaded nodes.
/// Nodes with no metrics yet (just registered, first heartbeat not received) get a neutral
/// score of 0.5 so they are neither penalized nor aggressively preferred.
pub struct Allocator {
    /// Latest metrics reported by each node (keyed by node_id).
    /// Updated on each heartbeat, removed when a node goes dead or disconnects.
    node_metrics: HashMap<String, NodeMetrics>,
}

impl Allocator {
    pub fn new() -> Self {
        Self {
            node_metrics: HashMap::new(),
        }
    }

    /// Update the cached metrics for a node (called on each heartbeat).
    pub fn update_metrics(&mut self, node_id: &str, metrics: NodeMetrics) {
        self.node_metrics.insert(node_id.to_string(), metrics);
    }

    /// Remove cached metrics for a node (called when a node goes dead or disconnects).
    pub fn remove_metrics(&mut self, node_id: &str) {
        self.node_metrics.remove(node_id);
    }

    /// Get the cached metrics for a node.
    pub fn get_metrics(&self, node_id: &str) -> Option<&NodeMetrics> {
        self.node_metrics.get(node_id)
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
    fn score_node(&self, node_id: &str) -> f64 {
        let metrics = match self.node_metrics.get(node_id) {
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

    /// Pick the least-loaded alive node for extent placement (single node, RF=1).
    /// Returns the selected node, or an error if no alive nodes exist.
    pub async fn pick_node(&mut self, store: &MetadataStore) -> Result<NodeRow, StorageError> {
        let alive = store.get_alive_nodes().await?;
        if alive.is_empty() {
            return Err(StorageError::Internal("no alive ExtentNode nodes".into()));
        }

        // Score each node, pick the one with the lowest score (least loaded).
        let mut best_idx = 0;
        let mut best_score = f64::MAX;
        for (i, node) in alive.iter().enumerate() {
            let score = self.score_node(&node.node_id);
            if score < best_score {
                best_score = score;
                best_idx = i;
            }
        }

        Ok(alive[best_idx].clone())
    }

    /// Pick `count` distinct least-loaded alive nodes for replica set placement.
    /// Returns nodes sorted by load (least loaded first): [Primary, Secondary_1, ...].
    /// Errors if fewer than `count` alive nodes are available.
    pub async fn pick_nodes(
        &mut self,
        store: &MetadataStore,
        count: usize,
    ) -> Result<Vec<NodeRow>, StorageError> {
        if count == 0 {
            return Err(StorageError::Internal("replication_factor must be >= 1".into()));
        }
        let alive = store.get_alive_nodes().await?;
        if alive.len() < count {
            return Err(StorageError::Internal(format!(
                "need {count} alive ExtentNode nodes for replica set, but only {} available",
                alive.len()
            )));
        }

        // Score all nodes and sort ascending (least loaded first).
        let mut scored: Vec<(usize, f64)> = alive
            .iter()
            .enumerate()
            .map(|(i, node)| (i, self.score_node(&node.node_id)))
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

impl Default for Allocator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn score_node_no_metrics() {
        let alloc = Allocator::new();
        let score = alloc.score_node("unknown-node");
        assert!((score - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn score_node_idle() {
        let mut alloc = Allocator::new();
        // Idle node: plenty of memory, no load.
        alloc.update_metrics("node-1", NodeMetrics {
            available_memory_bytes: 16_000_000_000,
            total_memory_bytes: 16_000_000_000,
            appends_per_sec: 0,
            active_extent_count: 0,
            bytes_written_per_sec: 0,
        });
        let score = alloc.score_node("node-1");
        // mem_pressure = 0.0, all others = 0.0 => score ~ 0.0
        assert!(score < 0.01, "idle node score should be near 0, got {score}");
    }

    #[test]
    fn score_node_loaded() {
        let mut alloc = Allocator::new();
        // Heavily loaded node: low memory, high throughput, many extents.
        alloc.update_metrics("node-1", NodeMetrics {
            available_memory_bytes: 1_000_000_000,
            total_memory_bytes: 16_000_000_000,
            appends_per_sec: 50_000,
            active_extent_count: 100,
            bytes_written_per_sec: 100 * 1024 * 1024,
        });
        let score = alloc.score_node("node-1");
        // All components at or near max => score near 1.0
        assert!(score > 0.8, "loaded node score should be near 1.0, got {score}");
    }

    #[test]
    fn update_and_remove_metrics() {
        let mut alloc = Allocator::new();
        assert!(alloc.get_metrics("node-1").is_none());

        alloc.update_metrics("node-1", NodeMetrics::default());
        assert!(alloc.get_metrics("node-1").is_some());

        alloc.remove_metrics("node-1");
        assert!(alloc.get_metrics("node-1").is_none());
    }

    #[test]
    fn score_prefers_lighter_node() {
        let mut alloc = Allocator::new();

        // Light node.
        alloc.update_metrics("light", NodeMetrics {
            available_memory_bytes: 14_000_000_000,
            total_memory_bytes: 16_000_000_000,
            appends_per_sec: 100,
            active_extent_count: 5,
            bytes_written_per_sec: 1_000_000,
        });

        // Heavy node.
        alloc.update_metrics("heavy", NodeMetrics {
            available_memory_bytes: 2_000_000_000,
            total_memory_bytes: 16_000_000_000,
            appends_per_sec: 40_000,
            active_extent_count: 80,
            bytes_written_per_sec: 80 * 1024 * 1024,
        });

        let light_score = alloc.score_node("light");
        let heavy_score = alloc.score_node("heavy");
        assert!(
            light_score < heavy_score,
            "light node ({light_score}) should score lower than heavy node ({heavy_score})"
        );
    }
}
