//! ArenaId generator + node_id hash.
//!
//! `ArenaId` itself lives in `common::types` so that `StorageError::ArenaFull`
//! can reference it without a dep cycle. This module re-exports it plus the
//! EN-local generator and node-prefix hash.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) use common::types::ArenaId;

/// Mints monotonically-increasing ArenaIds for one EN.
pub(crate) struct ArenaIdGenerator {
    node_prefix: u16,
    counter: AtomicU64,
}

impl ArenaIdGenerator {
    pub(crate) fn new(node_prefix: u16) -> Self {
        Self {
            node_prefix,
            counter: AtomicU64::new(0),
        }
    }

    pub(crate) fn next(&self) -> ArenaId {
        let c = self.counter.fetch_add(1, Ordering::Relaxed);
        ArenaId::new(self.node_prefix, c)
    }
}

/// Hash a node_id string to a 16-bit prefix. `+1` avoids the zero
/// prefix so logs make it obvious when a prefix was unset.
pub(crate) fn node_prefix_from_id(node_id: &str) -> u16 {
    let mut h = DefaultHasher::new();
    node_id.hash(&mut h);
    (h.finish() as u16).wrapping_add(1)
}
