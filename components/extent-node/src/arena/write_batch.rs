//! Arena-level batch write types for pipelined group commit.

#![allow(dead_code)]

use bytes::Bytes;
use smallvec::SmallVec;
use tokio::sync::oneshot;

use common::errors::StorageError;
use common::types::{Epoch, Offset, StreamId};

use crate::arena::ArenaId;

// ── WriteBatchJob ────────────────────────────────────────────────────────────

/// One record submitted inside a [`WriteBatch`] from a stream leader.
///
/// The logical offset is assigned by `Stream` before the physical arena
/// write. ArenaPool echoes it back in [`ArenaAppendResult`] with local
/// physical placement.
#[derive(Debug)]
pub(crate) struct WriteBatchJob {
    pub(crate) offset: Offset,
    pub(crate) payload: Bytes,
}

impl WriteBatchJob {
    pub(crate) fn new(offset: Offset, payload: Bytes) -> Self {
        Self { offset, payload }
    }
}

// ── WriteBatch ───────────────────────────────────────────────────────────────

/// A batch from one stream leader, routed to the arena-level writer.
pub(crate) struct WriteBatch {
    pub(crate) stream_id: StreamId,
    pub(crate) epoch: Epoch,
    pub(crate) jobs: SmallVec<[WriteBatchJob; 16]>,
    pub(crate) reply: Option<oneshot::Sender<WriteBatchAck>>,
}

impl WriteBatch {
    pub(crate) fn new(
        stream_id: StreamId,
        epoch: Epoch,
        jobs: SmallVec<[WriteBatchJob; 16]>,
    ) -> Self {
        Self {
            stream_id,
            epoch,
            jobs,
            reply: None,
        }
    }

    pub(crate) fn with_reply(
        stream_id: StreamId,
        epoch: Epoch,
        jobs: SmallVec<[WriteBatchJob; 16]>,
        reply: oneshot::Sender<WriteBatchAck>,
    ) -> Self {
        Self {
            stream_id,
            epoch,
            jobs,
            reply: Some(reply),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.jobs.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }
}

// ── ArenaAppendResult ────────────────────────────────────────────────────────

/// Per-job resolved placement within an arena after the arena writer
/// has performed the memcpy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ArenaAppendResult {
    pub(crate) offset: Offset,
    pub(crate) arena_id: ArenaId,
    pub(crate) byte_pos: u32,
}

impl ArenaAppendResult {
    pub(crate) fn new(offset: Offset, arena_id: ArenaId, byte_pos: u32) -> Self {
        Self {
            offset,
            arena_id,
            byte_pos,
        }
    }
}

// ── WriteBatchAck ────────────────────────────────────────────────────────────

/// The result of processing a [`WriteBatch`]: one result per input job,
/// in the same order as `WriteBatch.jobs`.
pub(crate) struct WriteBatchAck {
    pub(crate) results: SmallVec<[Result<ArenaAppendResult, StorageError>; 16]>,
}

impl WriteBatchAck {
    pub(crate) fn new() -> Self {
        Self {
            results: SmallVec::new(),
        }
    }

    pub(crate) fn push(&mut self, result: Result<ArenaAppendResult, StorageError>) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use common::types::Offset;

    #[test]
    fn arena_append_result_carries_logical_and_physical_placement() {
        let result = ArenaAppendResult {
            offset: Offset(42),
            arena_id: ArenaId(0x0001_0000_0000_0007),
            byte_pos: 128,
        };

        assert_eq!(result.offset, Offset(42));
        assert_eq!(result.arena_id.0, 0x0001_0000_0000_0007);
        assert_eq!(result.byte_pos, 128);
    }

    #[test]
    fn write_batch_ack_holds_per_job_results() {
        let mut ack = WriteBatchAck::new();
        ack.push(Ok(ArenaAppendResult {
            offset: Offset(1),
            arena_id: ArenaId(1),
            byte_pos: 0,
        }));

        assert_eq!(ack.len(), 1);
        assert!(ack.results[0].is_ok());
    }
}
