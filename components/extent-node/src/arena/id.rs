//! Globally-unique arena identifier.
//!
//! Layout: `(node_prefix << 48) | counter`
//!   - 16-bit node_prefix: stable hash of the EN's node_id string.
//!     65,535 distinct prefixes; collisions are possible in large
//!     clusters but best-effort is acceptable for S3 key uniqueness
//!     (operators can assign explicit node IDs if collisions
//!     matter).
//!   - 48-bit counter: monotonically increasing per EN; ~2.8e14 IDs
//!     before wrap, vastly more than any practical workload.
//!
//! The S3 key `{namespace}/arenas/{arena_id:016x}.dat` therefore does
//! not collide across ENs. The format is defined by the shared-arena
//! spec (§ArenaId); introducing it in P2 so Shape A upload in a later
//! plan has one naming authority.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct ArenaId(pub u64);

impl ArenaId {
    pub(crate) fn new(node_prefix: u16, counter: u64) -> Self {
        debug_assert!(counter < (1u64 << 48), "ArenaId counter overflow");
        Self(((node_prefix as u64) << 48) | (counter & ((1u64 << 48) - 1)))
    }

    #[allow(dead_code)]
    pub(crate) fn node_prefix(&self) -> u16 {
        (self.0 >> 48) as u16
    }

    #[allow(dead_code)]
    pub(crate) fn counter(&self) -> u64 {
        self.0 & ((1u64 << 48) - 1)
    }
}

impl std::fmt::Display for ArenaId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

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
