use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

use bytes::Bytes;
use common::errors::{EpochFullSnafu, InternalSnafu, StorageError};
use common::types::{Epoch, Offset, StreamId};
use smallvec::SmallVec;

use crate::arena::{ArenaAppendResult, ArenaBuffer, ArenaDirectory, ArenaId, EpochArenaEntry, OwnedArenaSlice, WriteBatchJob};

#[derive(Debug, Clone, Copy)]
struct ArenaRange {
    stream_id: StreamId,
    epoch: Epoch,
    start_offset: Offset,
    end_offset: Offset,
}

pub(crate) struct Arena {
    arena_id: ArenaId,
    arena: Arc<ArenaBuffer>,
    directory: ArenaDirectory,
    buf: *mut u8,
    capacity: u32,
    write_cursor: AtomicU64,
    record_count: AtomicU64,
    committed_bytes: AtomicU64,
    ranges: Mutex<SmallVec<[ArenaRange; 4]>>,
}

unsafe impl Send for Arena {}
unsafe impl Sync for Arena {}

impl Arena {
    pub(crate) fn new(arena_id: ArenaId, capacity: u32) -> Self {
        let arena = ArenaBuffer::new(capacity);
        let buf = arena.ptr_mut();
        let record_cap = (capacity / 5) as usize;
        let entry = EpochArenaEntry::with_capacity(StreamId(0), Epoch(0), Offset(0), record_cap);
        let directory = ArenaDirectory::new(entry);
        Self {
            arena_id,
            arena,
            directory,
            buf,
            capacity,
            write_cursor: AtomicU64::new(0),
            record_count: AtomicU64::new(0),
            committed_bytes: AtomicU64::new(0),
            ranges: Mutex::new(SmallVec::new()),
        }
    }

    pub(crate) fn write_batch(
        &self,
        stream_id: StreamId,
        epoch: Epoch,
        jobs: &[WriteBatchJob],
    ) -> SmallVec<[Result<ArenaAppendResult, StorageError>; 16]> {
        let mut results = SmallVec::with_capacity(jobs.len());
        for job in jobs {
            results.push(self.write_one(stream_id, epoch, job));
        }
        results
    }

    fn write_one(
        &self,
        _stream_id: StreamId,
        _epoch: Epoch,
        job: &WriteBatchJob,
    ) -> Result<ArenaAppendResult, StorageError> {
        let payload_len = job.payload.len();
        let record_len = 4 + payload_len;
        let byte_pos = self.write_cursor.load(Ordering::Relaxed);
        if byte_pos + record_len as u64 > self.capacity as u64 {
            return Err(EpochFullSnafu {
                stream_id: _stream_id,
                epoch: _epoch,
            }
            .build());
        }
        self.write_cursor
            .store(byte_pos + record_len as u64, Ordering::Relaxed);

        let seq = self.record_count.load(Ordering::Relaxed);
        self.record_count.store(seq + 1, Ordering::Relaxed);

        unsafe {
            let dst = self.buf.add(byte_pos as usize);
            std::ptr::copy_nonoverlapping((payload_len as u32).to_be_bytes().as_ptr(), dst, 4);
            if payload_len > 0 {
                std::ptr::copy_nonoverlapping(job.payload.as_ref().as_ptr(), dst.add(4), payload_len);
            }
        }

        let new_committed_bytes = byte_pos + record_len as u64;
        self.committed_bytes
            .store(new_committed_bytes, Ordering::Release);
        self.directory.single_entry().record(seq, byte_pos);
        self.record_range(_stream_id, _epoch, job.offset);

        Ok(ArenaAppendResult::new(
            job.offset,
            self.arena_id,
            byte_pos as u32,
        ))
    }

    fn record_range(&self, stream_id: StreamId, epoch: Epoch, offset: Offset) {
        let mut ranges = self.ranges.lock();
        if let Some(range) = ranges
            .iter_mut()
            .find(|r| r.stream_id == stream_id && r.epoch == epoch)
        {
            if offset.0 < range.start_offset.0 {
                range.start_offset = offset;
            }
            if offset.0 + 1 > range.end_offset.0 {
                range.end_offset = Offset(offset.0 + 1);
            }
        } else {
            ranges.push(ArenaRange {
                stream_id,
                epoch,
                start_offset: offset,
                end_offset: Offset(offset.0 + 1),
            });
        }
    }

    pub(crate) fn start_offset(&self, stream_id: StreamId, epoch: Epoch) -> Option<Offset> {
        self.ranges
            .lock()
            .iter()
            .find(|r| r.stream_id == stream_id && r.epoch == epoch)
            .map(|r| r.start_offset)
    }

    pub(crate) fn end_offset(&self, stream_id: StreamId, epoch: Epoch) -> Option<Offset> {
        self.ranges
            .lock()
            .iter()
            .find(|r| r.stream_id == stream_id && r.epoch == epoch)
            .map(|r| r.end_offset)
    }

    pub(crate) fn contains_offset(&self, stream_id: StreamId, epoch: Epoch, offset: Offset) -> bool {
        match (self.start_offset(stream_id, epoch), self.end_offset(stream_id, epoch)) {
            (Some(start), Some(end)) => offset.0 >= start.0 && offset.0 < end.0,
            _ => false,
        }
    }

    pub(crate) fn read(
        &self,
        stream_id: StreamId,
        epoch: Epoch,
        offset: Offset,
        count: u32,
    ) -> Result<Vec<Bytes>, StorageError> {
        let start = self.start_offset(stream_id, epoch).ok_or_else(|| {
            InternalSnafu {
                message: format!("arena {} has no range for stream {} epoch {}", self.arena_id, stream_id, epoch),
            }
            .build()
        })?;
        if offset.0 < start.0 {
            return Ok(Vec::new());
        }
        let seq = offset.0 - start.0;
        let byte_pos = self.directory.single_entry().lookup(seq).ok_or_else(|| {
            InternalSnafu {
                message: format!("arena {} index lookup failed for offset {}", self.arena_id, offset),
            }
            .build()
        })?;
        self.read_at(byte_pos, count)
    }

    fn read_at(&self, byte_pos: u64, count: u32) -> Result<Vec<Bytes>, StorageError> {
        let committed_byte_pos = self.committed_bytes.load(Ordering::Acquire) as usize;
        let mut pos = byte_pos as usize;
        if pos >= committed_byte_pos {
            return Ok(Vec::new());
        }

        let mut result = Vec::with_capacity(count as usize);
        let arena = self.arena_as_bytes();
        for _ in 0..count {
            if pos + 4 > committed_byte_pos {
                break;
            }
            let len =
                u32::from_be_bytes([arena[pos], arena[pos + 1], arena[pos + 2], arena[pos + 3]])
                    as usize;
            let payload_start = pos + 4;
            let payload_end = payload_start + len;
            if payload_end > committed_byte_pos {
                break;
            }
            result.push(arena.slice(payload_start..payload_end));
            pos = payload_end;
        }
        Ok(result)
    }

    fn arena_as_bytes(&self) -> Bytes {
        let arena = Arc::clone(&self.arena);
        let ptr = arena.ptr();
        let len = arena.capacity();
        Bytes::from_owner(OwnedArenaSlice {
            _arena: arena,
            ptr,
            len,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use smallvec::smallvec;

    #[test]
    fn arena_writes_batch_and_tracks_range_by_stream_epoch() {
        let arena = Arena::new(ArenaId(1), 4096);
        let jobs: SmallVec<[WriteBatchJob; 16]> = smallvec![
            WriteBatchJob::new(Offset(100), Bytes::from_static(b"a")),
            WriteBatchJob::new(Offset(101), Bytes::from_static(b"bb")),
        ];

        let results = arena.write_batch(StreamId(7), Epoch(3), &jobs);

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].as_ref().unwrap().offset, Offset(100));
        assert_eq!(results[0].as_ref().unwrap().byte_pos, 0);
        assert_eq!(results[1].as_ref().unwrap().offset, Offset(101));
        assert_eq!(results[1].as_ref().unwrap().byte_pos, 5);
        assert_eq!(arena.start_offset(StreamId(7), Epoch(3)), Some(Offset(100)));
        assert_eq!(arena.end_offset(StreamId(7), Epoch(3)), Some(Offset(102)));
    }

    #[test]
    fn arena_reports_full_when_record_cannot_fit_empty_arena() {
        let arena = Arena::new(ArenaId(1), 8);
        let jobs: SmallVec<[WriteBatchJob; 16]> =
            smallvec![WriteBatchJob::new(Offset(0), Bytes::from_static(b"0123456789"))];

        let results = arena.write_batch(StreamId(1), Epoch(1), &jobs);

        assert!(results[0].is_err());
    }
}
