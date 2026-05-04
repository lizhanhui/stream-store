//! A single byte-pool arena.
//!
//! An `Arena` owns one `ArenaBuffer`, one `ArenaDirectory`, and the
//! single-writer cursors that track physical placement of records
//! within the buffer. It is the sole byte-pool primitive backing
//! `StreamEpoch`: a Dedicated epoch holds a `SmallVec<[Arc<Arena>; 4]>`
//! (length grows on arena-full rotation); a Shared epoch (P3) observes
//! arenas minted by a process-wide `SharedArenaPool`.
//!
//! Identity `(stream_id, epoch, start_offset)` is stamped at construction.
//! Dedicated arenas are 1:1 with a `(stream, epoch)` pair; on rotation
//! the successor arena takes the pre-rotation `committed_offset` as its
//! `start_offset`, so offset-to-arena lookup is a simple range check.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use common::errors::{ArenaFullSnafu, InternalSnafu, StorageError};
use common::types::{ArenaId, Epoch, Offset, StreamId};
use crossbeam_channel::{Receiver, Sender, unbounded};
use smallvec::SmallVec;

use crate::arena::{
    ArenaAppend, ArenaAppendResult, ArenaBuffer, ArenaDirectory, EpochArenaEntry, OwnedArenaSlice,
    WriteBatch,
};

/// Minimum record size (4-byte length prefix + 1-byte payload). Used to
/// size the per-arena directory: `arena_capacity / MIN_RECORD_SIZE`
/// slots, matching the worst-case record count.
pub(crate) const MIN_RECORD_SIZE: u32 = 5;

pub(crate) struct Arena {
    pub(crate) arena_id: ArenaId,
    pub(crate) stream_id: StreamId,
    pub(crate) epoch: Epoch,
    pub(crate) start_offset: Offset,

    /// Reference-counted buffer. `Bytes` slices returned by `read()`
    /// keep the buffer alive after the owning epoch is dropped.
    buffer: Arc<ArenaBuffer>,

    /// Derived write pointer (same address as `buffer.ptr()`).
    /// Single-writer invariant upheld by the caller (Stream leader).
    buf: *mut u8,

    capacity: u32,
    write_cursor: AtomicU64,
    record_count: AtomicU64,
    committed_bytes: AtomicU64,
    directory: ArenaDirectory,

    /// Arena-level leader-election counter. Unused on Dedicated path
    /// (the stream leader is always the arena leader); P3 wires this
    /// for SharedArenaPool.
    #[allow(dead_code)]
    pub(crate) arena_in_flight: AtomicU64,

    /// Delegation channel. Followers in a Shared arena (P3) submit
    /// `WriteBatch`es via `arena_job_tx`; the arena leader drains from
    /// `arena_job_rx`. Unused on Dedicated path.
    #[allow(dead_code)]
    pub(crate) arena_job_tx: Sender<WriteBatch>,
    #[allow(dead_code)]
    pub(crate) arena_job_rx: Receiver<WriteBatch>,
}

// SAFETY: `buf` aliases `buffer`'s allocation; single-writer invariant
// is upheld by the caller. All concurrent access is via atomic cursors.
unsafe impl Send for Arena {}
unsafe impl Sync for Arena {}

impl Arena {
    pub(crate) fn new(
        arena_id: ArenaId,
        stream_id: StreamId,
        epoch: Epoch,
        start_offset: Offset,
        capacity: u32,
    ) -> Self {
        let buffer = ArenaBuffer::new(capacity);
        let buf = buffer.ptr_mut();
        let record_cap = (capacity / MIN_RECORD_SIZE) as usize;
        let entry = EpochArenaEntry::with_capacity(stream_id, epoch, start_offset, record_cap);
        let directory = ArenaDirectory::new(entry);
        let (arena_job_tx, arena_job_rx) = unbounded();
        Self {
            arena_id,
            stream_id,
            epoch,
            start_offset,
            buffer,
            buf,
            capacity,
            write_cursor: AtomicU64::new(0),
            record_count: AtomicU64::new(0),
            committed_bytes: AtomicU64::new(0),
            directory,
            arena_in_flight: AtomicU64::new(0),
            arena_job_tx,
            arena_job_rx,
        }
    }

    // ── Accessors ───────────────────────────────────────────────────

    #[allow(dead_code)]
    pub(crate) fn capacity(&self) -> u32 {
        self.capacity
    }

    pub(crate) fn bytes_written(&self) -> u64 {
        self.committed_bytes.load(Ordering::Acquire)
    }

    pub(crate) fn record_count(&self) -> u64 {
        self.record_count.load(Ordering::Acquire)
    }

    /// Exclusive upper bound on offsets stored in this arena.
    pub(crate) fn next_offset(&self) -> Offset {
        Offset(self.start_offset.0 + self.record_count())
    }

    pub(crate) fn contains_offset(&self, offset: Offset) -> bool {
        offset.0 >= self.start_offset.0 && offset.0 < self.next_offset().0
    }

    pub(crate) fn directory(&self) -> &ArenaDirectory {
        &self.directory
    }

    #[allow(dead_code)]
    pub(crate) fn buffer(&self) -> &Arc<ArenaBuffer> {
        &self.buffer
    }

    // ── Write path ──────────────────────────────────────────────────

    /// Single-writer batch append.
    ///
    /// The caller owns the single-writer invariant (store-level
    /// leader election for Dedicated, arena-level leader election for
    /// Shared — P3).
    ///
    /// On `ArenaFull` the caller is expected to rotate to a fresh
    /// arena and retry the failing job; `Arena::write_batch_inline`
    /// never rotates itself. Per-job results preserve 1:1 ordering
    /// with `jobs`.
    ///
    /// On the Primary, `job.offset` is treated as an echo-back hint
    /// and the returned `ArenaAppendResult.offset` is authoritative
    /// (`start_offset + seq`). On a Secondary, the caller should
    /// verify that the returned offset matches the Forward frame's
    /// offset and treat a mismatch as a protocol-level error.
    pub(crate) fn write_batch_inline(
        &self,
        jobs: &[ArenaAppend],
    ) -> SmallVec<[Result<ArenaAppendResult, StorageError>; 16]> {
        let mut out = SmallVec::with_capacity(jobs.len());
        for job in jobs {
            out.push(self.write_one(job));
        }
        out
    }

    fn write_one(&self, job: &ArenaAppend) -> Result<ArenaAppendResult, StorageError> {
        let payload_len = job.payload.len();
        let record_len = 4 + payload_len as u64;

        let byte_pos = self.write_cursor.load(Ordering::Relaxed);
        if byte_pos + record_len > self.capacity as u64 {
            return Err(ArenaFullSnafu {
                stream_id: self.stream_id,
                epoch: self.epoch,
                arena_id: self.arena_id,
            }
            .build());
        }
        self.write_cursor
            .store(byte_pos + record_len, Ordering::Relaxed);

        let seq = self.record_count.load(Ordering::Relaxed);
        self.record_count.store(seq + 1, Ordering::Relaxed);

        // SAFETY: single-writer invariant; byte_pos..byte_pos+record_len
        // is disjoint from every other writer's slot.
        unsafe {
            let dst = self.buf.add(byte_pos as usize);
            std::ptr::copy_nonoverlapping((payload_len as u32).to_be_bytes().as_ptr(), dst, 4);
            if payload_len > 0 {
                std::ptr::copy_nonoverlapping(job.payload.as_ptr(), dst.add(4), payload_len);
            }
        }

        self.committed_bytes
            .store(byte_pos + record_len, Ordering::Release);
        self.directory.single_entry().record(seq, byte_pos);

        let assigned_offset = Offset(self.start_offset.0 + seq);
        Ok(ArenaAppendResult::new(
            assigned_offset,
            self.arena_id,
            byte_pos as u32,
        ))
    }

    // ── Read path ───────────────────────────────────────────────────

    /// Read up to `count` records starting at the given logical offset.
    /// Returns an empty vec if the offset is past the committed frontier.
    pub(crate) fn read(&self, offset: Offset, count: u32) -> Result<Vec<Bytes>, StorageError> {
        if offset.0 < self.start_offset.0 {
            return Err(InternalSnafu {
                message: format!(
                    "arena {}: offset {} < start_offset {}",
                    self.arena_id, offset.0, self.start_offset.0
                ),
            }
            .build());
        }
        let seq = offset.0 - self.start_offset.0;
        let byte_pos = match self.directory.single_entry().lookup(seq) {
            Some(bp) => bp,
            None => return Ok(Vec::new()),
        };
        self.read_at(byte_pos, count)
    }

    fn read_at(&self, byte_pos: u64, count: u32) -> Result<Vec<Bytes>, StorageError> {
        let committed = self.committed_bytes.load(Ordering::Acquire) as usize;
        let mut pos = byte_pos as usize;
        if pos >= committed {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity(count as usize);
        let view = self.arena_as_bytes();
        for _ in 0..count {
            if pos + 4 > committed {
                break;
            }
            let len = u32::from_be_bytes([view[pos], view[pos + 1], view[pos + 2], view[pos + 3]])
                as usize;
            let payload_start = pos + 4;
            let payload_end = payload_start + len;
            if payload_end > committed {
                break;
            }
            out.push(view.slice(payload_start..payload_end));
            pos = payload_end;
        }
        Ok(out)
    }

    /// Return a `Bytes` view of the entire arena allocation; caller
    /// must bound reads by `committed_bytes`.
    fn arena_as_bytes(&self) -> Bytes {
        let buffer = Arc::clone(&self.buffer);
        let ptr = buffer.ptr();
        let len = buffer.capacity();
        Bytes::from_owner(OwnedArenaSlice {
            _arena: buffer,
            ptr,
            len,
        })
    }

    /// Zero-copy view of the arena's committed prefix. Used by
    /// `StreamEpoch::committed_data()` at seal / flush time.
    pub(crate) fn committed_data(&self) -> Bytes {
        let committed = self.bytes_written() as usize;
        let buffer = Arc::clone(&self.buffer);
        let ptr = buffer.ptr();
        let view = Bytes::from_owner(OwnedArenaSlice {
            _arena: buffer,
            ptr,
            len: committed as u32,
        });
        view
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use smallvec::smallvec;

    fn mk(capacity: u32) -> Arena {
        Arena::new(ArenaId(1), StreamId(7), Epoch(3), Offset(100), capacity)
    }

    #[test]
    fn write_batch_inline_single_record_round_trip() {
        let arena = mk(4096);
        let jobs: SmallVec<[ArenaAppend; 16]> =
            smallvec![ArenaAppend::new(Offset(100), Bytes::from_static(b"hello"))];

        let results = arena.write_batch_inline(&jobs);

        assert_eq!(results.len(), 1);
        let r = results[0].as_ref().unwrap();
        assert_eq!(r.offset, Offset(100));
        assert_eq!(r.byte_pos, 0);
        assert_eq!(arena.record_count(), 1);
        assert_eq!(arena.bytes_written(), 9); // 4 len + 5 payload
    }

    #[test]
    fn write_batch_inline_multiple_records_advance_cursor() {
        let arena = mk(4096);
        let jobs: SmallVec<[ArenaAppend; 16]> = smallvec![
            ArenaAppend::new(Offset(100), Bytes::from_static(b"a")),
            ArenaAppend::new(Offset(101), Bytes::from_static(b"bb")),
            ArenaAppend::new(Offset(102), Bytes::from_static(b"ccc")),
        ];
        let results = arena.write_batch_inline(&jobs);
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].as_ref().unwrap().offset, Offset(100));
        assert_eq!(results[1].as_ref().unwrap().offset, Offset(101));
        assert_eq!(results[2].as_ref().unwrap().offset, Offset(102));
        assert_eq!(results[0].as_ref().unwrap().byte_pos, 0);
        assert_eq!(results[1].as_ref().unwrap().byte_pos, 5);
        assert_eq!(results[2].as_ref().unwrap().byte_pos, 11);
    }

    #[test]
    fn write_batch_inline_returns_arena_full_at_boundary() {
        // Capacity 16 bytes: fits two 4+4-byte records exactly, third returns ArenaFull.
        let arena = mk(16);
        let jobs: SmallVec<[ArenaAppend; 16]> = smallvec![
            ArenaAppend::new(Offset(100), Bytes::from_static(b"xxxx")),
            ArenaAppend::new(Offset(101), Bytes::from_static(b"yyyy")),
        ];
        let results = arena.write_batch_inline(&jobs);
        assert!(results[0].is_ok());
        assert!(results[1].is_ok());
        assert_eq!(arena.bytes_written(), 16);
        // A third record is too large.
        let more: SmallVec<[ArenaAppend; 16]> =
            smallvec![ArenaAppend::new(Offset(102), Bytes::from_static(b"z"))];
        let third = arena.write_batch_inline(&more);
        assert!(matches!(third[0], Err(StorageError::ArenaFull { .. })));
        // Cursor unchanged after the failed attempt.
        assert_eq!(arena.bytes_written(), 16);
        assert_eq!(arena.record_count(), 2);
    }

    #[test]
    fn arena_read_round_trip() {
        let arena = mk(4096);
        let jobs: SmallVec<[ArenaAppend; 16]> = smallvec![
            ArenaAppend::new(Offset(100), Bytes::from_static(b"first")),
            ArenaAppend::new(Offset(101), Bytes::from_static(b"second")),
            ArenaAppend::new(Offset(102), Bytes::from_static(b"third")),
        ];
        let _ = arena.write_batch_inline(&jobs);

        let msgs = arena.read(Offset(100), 3).unwrap();
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0], Bytes::from_static(b"first"));
        assert_eq!(msgs[1], Bytes::from_static(b"second"));
        assert_eq!(msgs[2], Bytes::from_static(b"third"));
    }

    #[test]
    fn contains_offset_checks_range() {
        let arena = mk(4096);
        let jobs: SmallVec<[ArenaAppend; 16]> = smallvec![
            ArenaAppend::new(Offset(100), Bytes::from_static(b"a")),
            ArenaAppend::new(Offset(101), Bytes::from_static(b"b")),
        ];
        let _ = arena.write_batch_inline(&jobs);
        assert!(arena.contains_offset(Offset(100)));
        assert!(arena.contains_offset(Offset(101)));
        assert!(!arena.contains_offset(Offset(102)));
        assert!(!arena.contains_offset(Offset(99)));
    }
}
