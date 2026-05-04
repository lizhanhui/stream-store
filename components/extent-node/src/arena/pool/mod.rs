//! Arena pool: memory management for stream epochs.
//!
//! `ArenaPool` is the memory layer below `Stream`. It owns the arena
//! ringbuffer and provides write, read, and data-access methods.
//! `StreamEpoch` holds metadata only; all physical arena operations
//! route through the pool.
//!
//! - `DedicatedArenaPool`: per-stream pool with a ringbuffer of arenas.
//!   Active arena = `arenas.last()`, rotation = `arenas.push()`.
//!   No HashMap — ringbuffer pattern, O(1) active-arena lookup.
use bytes::Bytes;
use common::{
    errors::StorageError,
    types::{ArenaClass, Epoch, Offset, StreamId},
};
use smallvec::SmallVec;
use std::sync::Arc;

use crate::arena::{ArenaAppend, ArenaAppendResult, arena::Arena};

pub(crate) mod dedicated;
pub(crate) mod shared;

// ── Trait ─────────────────────────────────────────────────────────────

#[allow(dead_code)] // P3/P4: class, bytes_written, register_arena, current_arena, release_epoch
pub(crate) trait ArenaPool: Send + Sync {
    fn class(&self) -> ArenaClass;

    /// Mint a fresh arena for the given `(stream, epoch)` placement,
    /// sized to `capacity` bytes. On DedicatedArenaPool, also
    /// registers the arena in the internal ringbuffer (dual-store
    /// during transition).
    fn allocate(
        &self,
        stream_id: StreamId,
        epoch: Epoch,
        start_offset: Offset,
        capacity: u32,
    ) -> Arc<Arena>;

    /// Write a batch of jobs to the active arena. On `ArenaFull`,
    /// rotates to a fresh arena and retries. Does NOT check seal —
    /// that is the caller's responsibility. Returns one result per
    /// job in 1:1 order.
    fn write_batch(
        &self,
        stream_id: StreamId,
        epoch: Epoch,
        appends: &[ArenaAppend],
    ) -> SmallVec<[Result<ArenaAppendResult, StorageError>; 16]>;

    /// Read up to `count` records starting at `offset`, spanning
    /// arena boundaries if needed.
    fn read_at_offset(
        &self,
        stream_id: StreamId,
        epoch: Epoch,
        offset: Offset,
        count: u32,
    ) -> Result<Vec<Bytes>, StorageError>;

    /// Concatenation of every resident arena's committed bytes.
    /// Used by `ForwardChecksum` and S3 flush.
    fn committed_data(&self, stream_id: StreamId, epoch: Epoch) -> Bytes;

    /// Lookup the byte position of record `seq` (relative to the
    /// epoch's `start_offset`) across the arena ring.
    fn index_lookup(&self, stream_id: StreamId, epoch: Epoch, seq: u64) -> Option<u64>;

    /// Total bytes written across every arena in the ring.
    fn bytes_written(&self, stream_id: StreamId, epoch: Epoch) -> u64;

    /// Register an existing arena into the pool's ringbuffer.
    /// Used when StreamEpoch already holds an arena that needs to
    /// be mirrored in the pool (dual-store during transition).
    fn register_arena(
        &self,
        stream_id: StreamId,
        epoch: Epoch,
        arena: Arc<Arena>,
        arena_capacity: u32,
    );

    /// Return the current active arena (last in the ring), if any.
    fn current_arena(&self, stream_id: StreamId, epoch: Epoch) -> Option<Arc<Arena>>;

    /// Release the epoch's arena list. Called after S3 flush completes
    /// and the epoch is no longer resident in memory.
    fn release_epoch(&self, stream_id: StreamId, epoch: Epoch);
}
