// ── Shared (stub) ─────────────────────────────────────────────────────

use std::sync::{Arc, atomic::AtomicU64};

use bytes::Bytes;
use common::{
    errors::StorageError,
    types::{ArenaClass, Epoch, Offset, StreamId},
};
use crossbeam_channel::{Receiver, Sender, unbounded};
use smallvec::SmallVec;

use crate::arena::{
    Arena, ArenaAppend, ArenaAppendResult, ArenaIdGenerator, ArenaPool, WriteBatch,
};

/// EN-wide singleton for Shared-class streams. All methods panic until
/// P3 wires the multi-stream arena pool, the Shape A flush path, and
/// the directory_ref_count bookkeeping.
#[allow(dead_code)]
pub(crate) struct SharedArenaPool {
    ids: Arc<ArenaIdGenerator>,
    arena_size: u32,

    /// Arena-level leader-election counter for shared arenas.
    /// P3 wires this for the two-layer CAS protocol.
    #[allow(dead_code)]
    pub(crate) in_flight: AtomicU64,

    /// Delegation channel. Followers in a Shared arena (P3) submit
    /// `WriteBatch`es via `tx`; the arena leader drains from
    /// `rx`.
    #[allow(dead_code)]
    pub(crate) tx: Sender<WriteBatch>,

    #[allow(dead_code)]
    pub(crate) rx: Receiver<WriteBatch>,
}

impl SharedArenaPool {
    #[allow(dead_code)]
    pub(crate) fn new(ids: Arc<ArenaIdGenerator>, arena_size: u32) -> Self {
        let (tx, rx) = unbounded();
        Self {
            ids,
            arena_size,
            in_flight: AtomicU64::new(0),
            tx,
            rx,
        }
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

    fn write_batch(
        &self,
        _stream_id: StreamId,
        _epoch: Epoch,
        _jobs: &[ArenaAppend],
    ) -> SmallVec<[Result<ArenaAppendResult, StorageError>; 16]> {
        panic!("SharedArenaPool::write_batch not wired; P3 scope")
    }

    fn read_at_offset(
        &self,
        _stream_id: StreamId,
        _epoch: Epoch,
        _offset: Offset,
        _count: u32,
    ) -> Result<Vec<Bytes>, StorageError> {
        panic!("SharedArenaPool::read_at_offset not wired; P3 scope")
    }

    fn committed_data(&self, _stream_id: StreamId, _epoch: Epoch) -> Bytes {
        panic!("SharedArenaPool::committed_data not wired; P3 scope")
    }

    fn index_lookup(&self, _stream_id: StreamId, _epoch: Epoch, _seq: u64) -> Option<u64> {
        panic!("SharedArenaPool::index_lookup not wired; P3 scope")
    }

    fn bytes_written(&self, _stream_id: StreamId, _epoch: Epoch) -> u64 {
        panic!("SharedArenaPool::bytes_written not wired; P3 scope")
    }

    fn register_arena(
        &self,
        _stream_id: StreamId,
        _epoch: Epoch,
        _arena: Arc<Arena>,
        _arena_capacity: u32,
    ) {
        panic!("SharedArenaPool::register_arena not wired; P3 scope")
    }

    fn current_arena(&self, _stream_id: StreamId, _epoch: Epoch) -> Option<Arc<Arena>> {
        panic!("SharedArenaPool::current_arena not wired; P3 scope")
    }

    fn release_epoch(&self, _stream_id: StreamId, _epoch: Epoch) {
        panic!("SharedArenaPool::release_epoch not wired; P3 scope")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common::types::{Epoch, Offset, StreamId};

    use crate::arena::{ArenaIdGenerator, ArenaPool};

    use super::SharedArenaPool;

    #[test]
    #[should_panic(expected = "SharedArenaPool::allocate not wired")]
    fn shared_pool_allocate_panics_until_p3() {
        let pool = SharedArenaPool::new(Arc::new(ArenaIdGenerator::new(1)), 4096);
        let _ = pool.allocate(StreamId(1), Epoch(1), Offset(0), 4096);
    }
}
