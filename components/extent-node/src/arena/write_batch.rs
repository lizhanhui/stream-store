//! Arena-level batch write types for pipelined group commit.
//!
//! Spec (§ Arena Concurrency Primitive):
//!
//! ```text
//! struct WriteBatch {
//!     stream_id: StreamId,
//!     epoch:     u64,
//!     jobs:      SmallVec<[SharedAppendJob; 16]>,
//!     reply:     oneshot::Sender<WriteBatchAck>,
//! }
//! struct WriteBatchAck { results: SmallVec<[JobResult; 16]> }
//! struct JobResult { arena_id: ArenaId, byte_pos: u32 }
//! ```
//!
//! In P2 we introduce these types and `StreamEpoch::write_batch`; the
//! channel-delegation path is unused on the Dedicated fast path (the
//! stream-level leader is the sole writer), so `reply`-via-oneshot is
//! plumbed but not yet exercised. A later plan wires it hot when
//! `SharedArenaPool` streams contend on a single arena.
//!
//! One deliberate deviation from the spec text: `JobResult` also
//! carries a per-record `Result<(), StorageError>`. The spec assumes
//! every record in a WriteBatchAck succeeded; when a single record
//! exceeds the arena's remaining space (ExtentFull), the caller needs
//! to know which job failed without losing positioning info for the
//! successful ones. The extra field costs 16 bytes per result and
//! keeps the interface honest about partial failures.

#![allow(dead_code)]

use bytes::Bytes;
use smallvec::SmallVec;
use tokio::sync::oneshot;

use common::errors::StorageError;
use common::types::{Epoch, StreamId};

use crate::arena::ArenaId;

// ── SharedAppendJob ──────────────────────────────────────────────────────────

/// One record submitted inside a [`WriteBatch`] from a stream leader.
///
/// Spec: `{ seq: u64, payload: Bytes }`. `seq` is the epoch-relative
/// sequence number assigned by the stream-level leader before the batch
/// is dispatched to the arena. The store-layer correlation id
/// (`request_id`) is tracked on the stream-level `AppendJob` and
/// does not flow into the arena layer.
#[derive(Debug)]
pub(crate) struct SharedAppendJob {
    pub(crate) seq: u64,
    pub(crate) payload: Bytes,
}

impl SharedAppendJob {
    pub(crate) fn new(seq: u64, payload: Bytes) -> Self {
        Self { seq, payload }
    }
}

// ── WriteBatch ───────────────────────────────────────────────────────────────

/// A batch from one stream leader, routed to the arena-level writer.
///
/// Spec shape — every batch belongs to exactly one `(stream_id, epoch)`
/// so the arena leader can drop records into one directory entry. The
/// `reply` oneshot is how followers await the arena leader's result
/// when multiple streams contend on a shared arena. In P2 every stream
/// is Dedicated, so the stream leader is always the arena leader and
/// this oneshot is an API placeholder — the Dedicated path invokes
/// `StreamEpoch::write_batch` directly and drops the reply.
pub(crate) struct WriteBatch {
    pub(crate) stream_id: StreamId,
    pub(crate) epoch: Epoch,
    pub(crate) jobs: SmallVec<[SharedAppendJob; 16]>,
    pub(crate) reply: oneshot::Sender<WriteBatchAck>,
}

impl WriteBatch {
    pub(crate) fn new(
        stream_id: StreamId,
        epoch: Epoch,
        jobs: SmallVec<[SharedAppendJob; 16]>,
        reply: oneshot::Sender<WriteBatchAck>,
    ) -> Self {
        Self {
            stream_id,
            epoch,
            jobs,
            reply,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.jobs.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }
}

// ── JobResult ────────────────────────────────────────────────────────────────

/// Per-job resolved placement within an arena after the arena writer
/// has performed the memcpy.
///
/// Spec: `{ arena_id: ArenaId, byte_pos: u32 }`. Records in a batch
/// may straddle an arena roll (when shared-arena routing lands), so
/// `arena_id` is per-record, not per-batch. `result` is a P2 extension
/// (see module docs) — carries the ExtentFull / StorageError from a
/// single job without poisoning its siblings in the batch.
#[derive(Debug)]
pub(crate) struct JobResult {
    pub(crate) arena_id: ArenaId,
    pub(crate) byte_pos: u32,
    pub(crate) result: Result<(), StorageError>,
}

impl JobResult {
    pub(crate) fn ok(arena_id: ArenaId, byte_pos: u32) -> Self {
        Self {
            arena_id,
            byte_pos,
            result: Ok(()),
        }
    }

    pub(crate) fn err(arena_id: ArenaId, err: StorageError) -> Self {
        Self {
            arena_id,
            byte_pos: 0,
            result: Err(err),
        }
    }

    pub(crate) fn is_ok(&self) -> bool {
        self.result.is_ok()
    }
}

// ── WriteBatchAck ────────────────────────────────────────────────────────────

/// The result of processing a [`WriteBatch`]: one [`JobResult`] per
/// input job, in the same order as `WriteBatch.jobs`.
pub(crate) struct WriteBatchAck {
    pub(crate) results: SmallVec<[JobResult; 16]>,
}

impl WriteBatchAck {
    pub(crate) fn new() -> Self {
        Self {
            results: SmallVec::new(),
        }
    }

    pub(crate) fn push(&mut self, result: JobResult) {
        self.results.push(result);
    }

    pub(crate) fn len(&self) -> usize {
        self.results.len()
    }
}

impl Default for WriteBatchAck {
    fn default() -> Self {
        Self::new()
    }
}
