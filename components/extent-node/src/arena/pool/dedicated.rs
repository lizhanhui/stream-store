use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use common::errors::{InternalSnafu, StorageError};
use common::types::{ArenaClass, Epoch, Offset, StreamId};
use parking_lot::Mutex;
use smallvec::SmallVec;

use crate::arena::Arena;
use crate::arena::{ArenaAppend, ArenaAppendResult, ArenaIdGenerator, pool::ArenaPool};

// ── PoolState (ringbuffer) ────────────────────────────────────────────

/// Ringbuffer of arenas for a Dedicated stream. Active arena is always
/// the last element; rotation appends a new arena.
struct DedicatedPoolState {
    /// Arenas in allocation order. Active = `arenas.last()`.
    arenas: SmallVec<[Arc<Arena>; 4]>,
    /// Fixed per-arena capacity for this pool.
    arena_capacity: u32,
}

// ── Dedicated ─────────────────────────────────────────────────────────

/// Per-stream pool for Dedicated-class streams. Owns a ringbuffer
/// of arenas; active arena = last element.
pub(crate) struct DedicatedArenaPool {
    ids: Arc<ArenaIdGenerator>,
    state: Mutex<DedicatedPoolState>,
}

impl DedicatedArenaPool {
    pub(crate) fn new(ids: Arc<ArenaIdGenerator>) -> Self {
        Self {
            ids,
            state: Mutex::new(DedicatedPoolState {
                arenas: SmallVec::new(),
                arena_capacity: 0,
            }),
        }
    }
}

/// Clone a StorageError by its Display string. SNAFU errors don't
/// implement Clone; when we need to broadcast the same failure across
/// multiple pending jobs in a batch, re-wrap via `InternalSnafu`.
fn err_clone(e: &StorageError) -> StorageError {
    InternalSnafu {
        message: e.to_string(),
    }
    .build()
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
        let arena = Arc::new(Arena::new(
            self.ids.next(),
            stream_id,
            epoch,
            start_offset,
            capacity,
        ));
        let mut state = self.state.lock();
        // First allocation sets arena_capacity for the pool.
        if state.arena_capacity == 0 {
            state.arena_capacity = capacity;
        }
        state.arenas.push(Arc::clone(&arena));
        arena
    }

    fn write_batch(
        &self,
        _stream_id: StreamId,
        epoch: Epoch,
        jobs: &[ArenaAppend],
    ) -> SmallVec<[Result<ArenaAppendResult, StorageError>; 16]> {
        let mut out: SmallVec<[Result<ArenaAppendResult, StorageError>; 16]> =
            SmallVec::with_capacity(jobs.len());

        let mut idx: usize = 0;
        while idx < jobs.len() {
            let arena = {
                let state = self.state.lock();
                state
                    .arenas
                    .last()
                    .cloned()
                    .expect("DedicatedArenaPool must have at least one arena")
            };
            let was_fresh = arena.record_count() == 0;
            let job = &jobs[idx];
            let one: [ArenaAppend; 1] = [ArenaAppend::new(job.offset, job.payload.clone())];
            let mut r = arena.write_batch_inline(&one);
            match r.pop().expect("one result") {
                Ok(ok) => {
                    out.push(Ok(ok));
                    idx += 1;
                }
                Err(StorageError::ArenaFull { .. }) => {
                    if was_fresh {
                        let err = InternalSnafu {
                            message: format!(
                                "record too large for arena: epoch={} arena_capacity={}",
                                epoch,
                                {
                                    let state = self.state.lock();
                                    state.arena_capacity
                                }
                            ),
                        }
                        .build();
                        for _ in &jobs[idx..] {
                            out.push(Err(err_clone(&err)));
                        }
                        return out;
                    }
                    // Rotate: allocate a new arena with next_start = current committed frontier.
                    // The caller (Stream) will update StreamEpoch.resident_arenas separately.
                    let next_start = arena.next_offset();
                    let _new_arena = self.allocate(_stream_id, epoch, next_start, {
                        let state = self.state.lock();
                        state.arena_capacity
                    });
                    // Retry the same job on the new arena.
                }
                Err(e) => {
                    out.push(Err(e));
                    idx += 1;
                }
            }
        }
        out
    }

    fn read_at_offset(
        &self,
        _stream_id: StreamId,
        _epoch: Epoch,
        offset: Offset,
        count: u32,
    ) -> Result<Vec<Bytes>, StorageError> {
        let arenas: SmallVec<[Arc<Arena>; 4]> = {
            let state = self.state.lock();
            state.arenas.clone()
        };
        if arenas.is_empty() {
            return Ok(Vec::new());
        }
        let mut out: Vec<Bytes> = Vec::with_capacity(count as usize);
        let mut next = offset;
        for arena in arenas.iter() {
            if out.len() as u32 >= count {
                break;
            }
            if !arena.contains_offset(next) {
                continue;
            }
            let want = count - out.len() as u32;
            let mut got = arena.read(next, want)?;
            if got.is_empty() {
                break;
            }
            next = Offset(next.0 + got.len() as u64);
            out.append(&mut got);
        }
        Ok(out)
    }

    fn committed_data(&self, _stream_id: StreamId, _epoch: Epoch) -> Bytes {
        let arenas: SmallVec<[Arc<Arena>; 4]> = {
            let state = self.state.lock();
            state.arenas.clone()
        };
        if arenas.is_empty() {
            return Bytes::new();
        }
        if arenas.len() == 1 {
            return arenas[0].committed_data();
        }
        let total: usize = arenas.iter().map(|a| a.bytes_written() as usize).sum();
        let mut buf = BytesMut::with_capacity(total);
        for arena in arenas.iter() {
            buf.extend_from_slice(&arena.committed_data());
        }
        buf.freeze()
    }

    fn index_lookup(&self, _stream_id: StreamId, _epoch: Epoch, seq: u64) -> Option<u64> {
        let state = self.state.lock();
        let offset = Offset(state.arenas.first()?.start_offset.0 + seq);
        let arena = state.arenas.iter().find(|a| a.contains_offset(offset))?;
        let local_seq = offset.0 - arena.start_offset.0;
        arena.directory().single_entry().lookup(local_seq)
    }

    fn bytes_written(&self, _stream_id: StreamId, _epoch: Epoch) -> u64 {
        let state = self.state.lock();
        state.arenas.iter().map(|a| a.bytes_written()).sum()
    }

    fn register_arena(
        &self,
        _stream_id: StreamId,
        _epoch: Epoch,
        arena: Arc<Arena>,
        arena_capacity: u32,
    ) {
        let mut state = self.state.lock();
        if state.arena_capacity == 0 {
            state.arena_capacity = arena_capacity;
        }
        state.arenas.push(arena);
    }

    fn current_arena(&self, _stream_id: StreamId, _epoch: Epoch) -> Option<Arc<Arena>> {
        let state = self.state.lock();
        state.arenas.last().cloned()
    }

    fn release_epoch(&self, _stream_id: StreamId, _epoch: Epoch) {
        let mut state = self.state.lock();
        state.arenas.clear();
        state.arena_capacity = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn new_pool() -> DedicatedArenaPool {
        DedicatedArenaPool::new(Arc::new(ArenaIdGenerator::new(1)))
    }

    #[test]
    fn dedicated_pool_allocate_mints_fresh_arena_each_call() {
        let pool = new_pool();
        let a = pool.allocate(StreamId(1), Epoch(1), Offset(0), 4096);
        let b = pool.allocate(StreamId(1), Epoch(1), Offset(100), 4096);
        assert_ne!(a.arena_id, b.arena_id);
        assert_eq!(a.start_offset, Offset(0));
        assert_eq!(b.start_offset, Offset(100));
        assert_eq!(pool.class(), ArenaClass::Dedicated);
    }

    #[test]
    fn dedicated_pool_allocate_registers_in_ringbuffer() {
        let pool = new_pool();
        let sid = StreamId(1);
        let ep = Epoch(1);
        let a = pool.allocate(sid, ep, Offset(0), 4096);
        // The arena should be in the pool's ringbuffer now.
        let current = pool.current_arena(sid, ep).unwrap();
        assert_eq!(current.arena_id, a.arena_id);
    }

    #[test]
    fn dedicated_pool_write_batch_single_record() {
        let pool = new_pool();
        let sid = StreamId(1);
        let ep = Epoch(1);
        pool.allocate(sid, ep, Offset(0), 4096);

        let jobs = [ArenaAppend::new(Offset(0), Bytes::from_static(b"hello"))];
        let results = pool.write_batch(sid, ep, &jobs);
        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());
        assert_eq!(results[0].as_ref().unwrap().offset, Offset(0));
    }

    #[test]
    fn dedicated_pool_write_batch_rotates_on_arena_full() {
        let pool = new_pool();
        let sid = StreamId(1);
        let ep = Epoch(1);
        pool.allocate(sid, ep, Offset(0), 16);

        // Two 4-byte payloads = 8+8=16 bytes, fills the arena.
        let r0 = pool
            .write_batch(
                sid,
                ep,
                &[ArenaAppend::new(Offset(0), Bytes::from_static(b"aaaa"))],
            )
            .pop()
            .unwrap();
        assert!(r0.is_ok());
        let r1 = pool
            .write_batch(
                sid,
                ep,
                &[ArenaAppend::new(Offset(1), Bytes::from_static(b"bbbb"))],
            )
            .pop()
            .unwrap();
        assert!(r1.is_ok());

        // Third write triggers rotation.
        let r2 = pool
            .write_batch(
                sid,
                ep,
                &[ArenaAppend::new(Offset(2), Bytes::from_static(b"cccc"))],
            )
            .pop()
            .unwrap();
        assert!(r2.is_ok());

        // Two arenas in the ring now.
        let arenas = {
            let state = pool.state.lock();
            state.arenas.clone()
        };
        assert_eq!(arenas.len(), 2);
        assert_eq!(arenas[0].start_offset, Offset(0));
        assert_eq!(arenas[1].start_offset, Offset(2));
    }

    #[test]
    fn dedicated_pool_read_at_offset_spans_arenas() {
        let pool = new_pool();
        let sid = StreamId(1);
        let ep = Epoch(1);
        pool.allocate(sid, ep, Offset(0), 16);

        // Write 5 records (triggers rotation).
        for i in 0..5u32 {
            let jobs = [ArenaAppend::new(
                Offset(i as u64),
                Bytes::copy_from_slice(&i.to_be_bytes()),
            )];
            let results = pool.write_batch(sid, ep, &jobs);
            assert!(results[0].is_ok());
        }

        let msgs = pool.read_at_offset(sid, ep, Offset(0), 5).unwrap();
        assert_eq!(msgs.len(), 5);
        for (i, msg) in msgs.iter().enumerate() {
            assert_eq!(msg.as_ref(), (i as u32).to_be_bytes());
        }
    }

    #[test]
    fn dedicated_pool_committed_data_and_index_lookup() {
        let pool = new_pool();
        let sid = StreamId(1);
        let ep = Epoch(1);
        pool.allocate(sid, ep, Offset(0), 4096);

        let jobs = [ArenaAppend::new(Offset(0), Bytes::from_static(b"hello"))];
        let _ = pool.write_batch(sid, ep, &jobs);

        let data = pool.committed_data(sid, ep);
        assert!(!data.is_empty());

        let bp = pool.index_lookup(sid, ep, 0);
        assert!(bp.is_some());
    }

    #[test]
    fn dedicated_pool_release_epoch_clears_arenas() {
        let pool = new_pool();
        let sid = StreamId(1);
        let ep = Epoch(1);
        pool.allocate(sid, ep, Offset(0), 4096);
        assert!(pool.current_arena(sid, ep).is_some());

        pool.release_epoch(sid, ep);
        assert!(pool.current_arena(sid, ep).is_none());
    }
}
