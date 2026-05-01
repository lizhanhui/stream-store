//! Arena-level batch write types for pipelined group commit.
//!
//! P2.7.1: defines the types used by `StreamEpoch::write_batch`.
//! P2.7.2: `StreamEpoch::write_batch` and `arena_in_flight` are wired in.
//! Store-level callers (P2.8+) use `WriteBatch`/`WriteBatchAck` directly.

#![allow(dead_code)]

use bytes::Bytes;
use smallvec::SmallVec;

use common::errors::StorageError;

use crate::stream_epoch::AppendResult;

// ── SharedAppendJob ──────────────────────────────────────────────────────────

/// A single append job at the arena level.
///
/// Arena-level equivalent of the store-level `AppendJob`; no client
/// `response_tx` at this level — the store layer handles responses.
#[derive(Debug)]
pub(crate) struct SharedAppendJob {
    /// Caller-assigned identifier (mirrors `AppendJob.request_id` for
    /// correlation when the store layer maps results back to responses).
    pub(crate) request_id: u32,
    /// The payload bytes to append.
    pub(crate) payload: Bytes,
}

impl SharedAppendJob {
    pub(crate) fn new(request_id: u32, payload: Bytes) -> Self {
        Self { request_id, payload }
    }
}

// ── WriteBatch ───────────────────────────────────────────────────────────────

/// A batch of [`SharedAppendJob`] items to be processed together by
/// [`StreamEpoch::write_batch`].
///
/// Uses `SmallVec<[_; 8]>` for inline storage of small batches (the typical
/// case is 1–8 jobs per group-commit round).
pub(crate) struct WriteBatch(pub(crate) SmallVec<[SharedAppendJob; 8]>);

impl WriteBatch {
    pub(crate) fn new() -> Self {
        Self(SmallVec::new())
    }

    pub(crate) fn push(&mut self, job: SharedAppendJob) {
        self.0.push(job);
    }

    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn as_slice(&self) -> &[SharedAppendJob] {
        &self.0
    }
}

impl Default for WriteBatch {
    fn default() -> Self {
        Self::new()
    }
}

// ── JobResult ────────────────────────────────────────────────────────────────

/// Per-job outcome from a [`WriteBatch`] call.
pub(crate) type JobResult = Result<AppendResult, StorageError>;

// ── WriteBatchAck ────────────────────────────────────────────────────────────

/// The result of processing a [`WriteBatch`]: one [`JobResult`] per input
/// job, in the same order as the original batch.
pub(crate) struct WriteBatchAck(pub(crate) SmallVec<[JobResult; 8]>);

impl WriteBatchAck {
    pub(crate) fn new() -> Self {
        Self(SmallVec::new())
    }

    pub(crate) fn push(&mut self, result: JobResult) {
        self.0.push(result);
    }

    pub(crate) fn into_inner(self) -> SmallVec<[JobResult; 8]> {
        self.0
    }

    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }
}

impl Default for WriteBatchAck {
    fn default() -> Self {
        Self::new()
    }
}
