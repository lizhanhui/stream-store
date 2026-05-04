//! Arena pool factory traits.
//!
//! `ArenaPool::allocate` mints a fresh `Arena` for a `(stream, epoch)`
//! tuple. The returned `Arc<Arena>` is owned by `StreamEpoch`, which
//! holds a `SmallVec<[Arc<Arena>; 4]>` and rotates to a new arena on
//! `ArenaFull` by calling `allocate` again.
//!
//! - `DedicatedArenaPool`: stateless per-stream factory; each Dedicated
//!   stream owns its own instance.
//! - `SharedArenaPool`: EN-wide singleton; `allocate` panics until P3
//!   wires the real multi-stream path.

use std::sync::Arc;

use common::types::{ArenaClass, Epoch, Offset, StreamId};

use crate::arena::{Arena, ArenaIdGenerator};

pub(crate) trait ArenaPool: Send + Sync {
    #[allow(dead_code)]
    fn class(&self) -> ArenaClass;

    /// Mint a fresh arena for the given `(stream, epoch)` placement,
    /// sized to `capacity` bytes. Called by `StreamEpoch` on epoch
    /// registration and on arena-full rotation.
    fn allocate(
        &self,
        stream_id: StreamId,
        epoch: Epoch,
        start_offset: Offset,
        capacity: u32,
    ) -> Arc<Arena>;
}

// ── Dedicated ───────────────────────────────────────────────────────

/// Per-stream factory for Dedicated-class streams. Stateless except
/// for its shared `ArenaIdGenerator`.
pub(crate) struct DedicatedArenaPool {
    ids: Arc<ArenaIdGenerator>,
}

impl DedicatedArenaPool {
    pub(crate) fn new(ids: Arc<ArenaIdGenerator>) -> Self {
        Self { ids }
    }
}

impl ArenaPool for DedicatedArenaPool {
    fn class(&self) -> ArenaClass {
        ArenaClass::Dedicated
    }

    fn allocate(
        &self,
        stream_id: StreamId,
        epoch: Epoch,
        start_offset: Offset,
        capacity: u32,
    ) -> Arc<Arena> {
        Arc::new(Arena::new(
            self.ids.next(),
            stream_id,
            epoch,
            start_offset,
            capacity,
        ))
    }
}

// ── Shared (stub) ───────────────────────────────────────────────────

/// EN-wide singleton for Shared-class streams. `allocate` panics until
/// P3 wires the multi-stream arena pool, the Shape A flush path, and
/// the directory_ref_count bookkeeping.
#[allow(dead_code)]
pub(crate) struct SharedArenaPool {
    ids: Arc<ArenaIdGenerator>,
    arena_size: u32,
}

impl SharedArenaPool {
    #[allow(dead_code)]
    pub(crate) fn new(ids: Arc<ArenaIdGenerator>, arena_size: u32) -> Self {
        Self { ids, arena_size }
    }
}

impl ArenaPool for SharedArenaPool {
    fn class(&self) -> ArenaClass {
        ArenaClass::Shared
    }

    fn allocate(
        &self,
        _stream_id: StreamId,
        _epoch: Epoch,
        _start_offset: Offset,
        _capacity: u32,
    ) -> Arc<Arena> {
        panic!(
            "SharedArenaPool::allocate not wired (ids node_prefix present, arena_size={}); \
             P3 scope",
            self.arena_size
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedicated_pool_allocate_mints_fresh_arena_each_call() {
        let pool = DedicatedArenaPool::new(Arc::new(ArenaIdGenerator::new(1)));
        let a = pool.allocate(StreamId(1), Epoch(1), Offset(0), 4096);
        let b = pool.allocate(StreamId(1), Epoch(1), Offset(100), 4096);
        assert_ne!(a.arena_id, b.arena_id);
        assert_eq!(a.start_offset, Offset(0));
        assert_eq!(b.start_offset, Offset(100));
        assert_eq!(pool.class(), ArenaClass::Dedicated);
    }

    #[test]
    #[should_panic(expected = "SharedArenaPool::allocate not wired")]
    fn shared_pool_allocate_panics_until_p3() {
        let pool = SharedArenaPool::new(Arc::new(ArenaIdGenerator::new(1)), 4096);
        let _ = pool.allocate(StreamId(1), Epoch(1), Offset(0), 4096);
    }
}
