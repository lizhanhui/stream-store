//! Arena allocation abstraction.
//!
//! P2: introduces the `ArenaPool` trait plus two impls:
//!
//! * `DedicatedArenaPool` — one pool per Stream; the stream owns its
//!   arenas exclusively. Wraps today's fast-path allocation. Used by
//!   every Stream in P2.
//! * `SharedArenaPool` — one pool per ExtentNode (future: many streams
//!   share arenas). Stubbed in P2: every method `panic!`s. A later
//!   plan wires it in once ArenaClass::Shared routing exists.
//!
//! The trait is deliberately narrow in P2: a single `allocate_epoch`
//! method. Future phases will add `write_batch`, `roll`, etc.

use std::sync::Arc;

use common::types::{Epoch, ExtentId, Offset, StreamId};

use crate::stream_epoch::StreamEpoch;

/// Allocates StreamEpoch instances for a given (stream_id, epoch).
pub(crate) trait ArenaPool: Send + Sync {
    /// Allocate a fresh StreamEpoch wrapped in Arc. Returned object is
    /// ready to insert into `Stream::epochs` via `insert_epoch`.
    fn allocate_epoch(
        &self,
        stream_id:    StreamId,
        extent_id:    ExtentId,
        start_offset: Offset,
        epoch:        Epoch,
    ) -> Arc<StreamEpoch>;
}

/// One pool per Stream. Today's fast path: a fresh arena per epoch.
pub(crate) struct DedicatedArenaPool {
    arena_size: u32,
}

impl DedicatedArenaPool {
    pub(crate) fn new(arena_size: u32) -> Self {
        Self { arena_size }
    }
}

impl ArenaPool for DedicatedArenaPool {
    fn allocate_epoch(
        &self,
        _stream_id:   StreamId,
        extent_id:    ExtentId,
        start_offset: Offset,
        epoch:        Epoch,
    ) -> Arc<StreamEpoch> {
        Arc::new(StreamEpoch::with_capacity(
            extent_id,
            start_offset,
            self.arena_size,
            epoch,
        ))
    }
}

/// Stub for the future EN-wide shared-arena pool. Routing is not wired
/// in P2; any caller that lands here signals a bug in stream setup.
#[allow(dead_code)]
pub(crate) struct SharedArenaPool {
    _arena_size: u32,
}

impl SharedArenaPool {
    #[allow(dead_code)]
    pub(crate) fn new(arena_size: u32) -> Self {
        Self { _arena_size: arena_size }
    }
}

impl ArenaPool for SharedArenaPool {
    fn allocate_epoch(
        &self,
        _stream_id:    StreamId,
        _extent_id:    ExtentId,
        _start_offset: Offset,
        _epoch:        Epoch,
    ) -> Arc<StreamEpoch> {
        panic!("SharedArenaPool::allocate_epoch not wired yet; P2 routes every stream through DedicatedArenaPool")
    }
}
