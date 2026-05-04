//! Arena pool abstraction.
//!
//! DedicatedArenaPool is one private arena ring per Stream. SharedArenaPool is
//! intentionally a P2 stub; P3 wires the EN-wide shared implementation.

use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use common::errors::StorageError;
use common::types::{ArenaClass, Epoch, Offset, StreamId};
use parking_lot::Mutex;

use crate::arena::{Arena, ArenaIdGenerator, WriteBatch, WriteBatchAck};

pub(crate) trait ArenaPool: Send + Sync {
    fn class(&self) -> ArenaClass;

    fn write_batch(&self, batch: WriteBatch) -> WriteBatchAck;

    fn read(
        &self,
        stream_id: StreamId,
        epoch: Epoch,
        offset: Offset,
        count: u32,
    ) -> Result<Vec<Bytes>, StorageError>;
}

pub(crate) struct DedicatedArenaPool {
    ids: Arc<ArenaIdGenerator>,
    arena_capacity: u32,
    active: Mutex<Option<Arc<Arena>>>,
    arenas: Mutex<VecDeque<Arc<Arena>>>,
}

impl DedicatedArenaPool {
    pub(crate) fn new(ids: Arc<ArenaIdGenerator>, arena_capacity: u32) -> Self {
        Self {
            ids,
            arena_capacity,
            active: Mutex::new(None),
            arenas: Mutex::new(VecDeque::new()),
        }
    }

    pub(crate) fn arena_count(&self) -> usize {
        self.arenas.lock().len()
    }

    fn active_or_create(&self) -> Arc<Arena> {
        if let Some(arena) = self.active.lock().as_ref().cloned() {
            return arena;
        }
        self.rotate_arena()
    }

    fn rotate_arena(&self) -> Arc<Arena> {
        let arena = Arc::new(Arena::new(self.ids.next(), self.arena_capacity));
        self.arenas.lock().push_back(Arc::clone(&arena));
        *self.active.lock() = Some(Arc::clone(&arena));
        arena
    }
}

impl ArenaPool for DedicatedArenaPool {
    fn class(&self) -> ArenaClass {
        ArenaClass::Dedicated
    }

    fn write_batch(&self, batch: WriteBatch) -> WriteBatchAck {
        let mut ack = WriteBatchAck::new();
        for job in &batch.jobs {
            let mut arena = self.active_or_create();
            let mut single = arena.write_batch(batch.stream_id, batch.epoch, std::slice::from_ref(job));
            let result = single.pop().expect("single-job write result");
            match result {
                Ok(result) => ack.push(Ok(result)),
                Err(StorageError::EpochFull { .. }) => {
                    arena = self.rotate_arena();
                    let mut retry =
                        arena.write_batch(batch.stream_id, batch.epoch, std::slice::from_ref(job));
                    let retry_result = retry.pop().expect("single-job retry result");
                    if retry_result.is_err() {
                        ack.push(retry_result);
                        break;
                    }
                    ack.push(retry_result);
                }
                Err(err) => {
                    ack.push(Err(err));
                    break;
                }
            }
        }
        ack
    }

    fn read(
        &self,
        stream_id: StreamId,
        epoch: Epoch,
        offset: Offset,
        count: u32,
    ) -> Result<Vec<Bytes>, StorageError> {
        let mut out = Vec::with_capacity(count as usize);
        let mut next = offset;
        let arenas = self.arenas.lock();
        while out.len() < count as usize {
            let Some(arena) = arenas
                .iter()
                .find(|arena| arena.contains_offset(stream_id, epoch, next))
            else {
                break;
            };
            let remaining = count - out.len() as u32;
            let mut records = arena.read(stream_id, epoch, next, remaining)?;
            if records.is_empty() {
                break;
            }
            next = Offset(next.0 + records.len() as u64);
            out.append(&mut records);
        }
        Ok(out)
    }
}

/// Stub for the future EN-wide shared-arena pool. Routing is not wired
/// in P2; any caller that lands here signals a bug in stream setup.
#[allow(dead_code)]
pub(crate) struct SharedArenaPool {
    _arena_size: u32,
    _generator: Arc<ArenaIdGenerator>,
}

impl SharedArenaPool {
    #[allow(dead_code)]
    pub(crate) fn new(arena_size: u32, generator: Arc<ArenaIdGenerator>) -> Self {
        Self {
            _arena_size: arena_size,
            _generator: generator,
        }
    }
}

impl ArenaPool for SharedArenaPool {
    fn class(&self) -> ArenaClass {
        ArenaClass::Shared
    }

    fn write_batch(&self, _batch: WriteBatch) -> WriteBatchAck {
        panic!("SharedArenaPool not wired in P2; every stream is Dedicated")
    }

    fn read(
        &self,
        _stream_id: StreamId,
        _epoch: Epoch,
        _offset: Offset,
        _count: u32,
    ) -> Result<Vec<Bytes>, StorageError> {
        panic!("SharedArenaPool not wired in P2; every stream is Dedicated")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use smallvec::smallvec;

    use crate::arena::WriteBatchJob;

    #[test]
    fn dedicated_pool_writes_to_private_active_arena() {
        let pool = DedicatedArenaPool::new(Arc::new(ArenaIdGenerator::new(1)), 4096);
        let batch = WriteBatch::new(
            StreamId(1),
            Epoch(1),
            smallvec![WriteBatchJob::new(Offset(0), Bytes::from_static(b"abc"))],
        );

        let ack = pool.write_batch(batch);
        let result = ack.results[0].as_ref().unwrap();
        assert_eq!(result.offset, Offset(0));
        assert_eq!(result.byte_pos, 0);
        assert_eq!(pool.arena_count(), 1);
    }

    #[test]
    fn dedicated_pool_rotates_arena_when_full_within_same_epoch() {
        let pool = DedicatedArenaPool::new(Arc::new(ArenaIdGenerator::new(1)), 8);
        let batch = WriteBatch::new(
            StreamId(1),
            Epoch(1),
            smallvec![
                WriteBatchJob::new(Offset(0), Bytes::from_static(b"abc")),
                WriteBatchJob::new(Offset(1), Bytes::from_static(b"def")),
            ],
        );

        let ack = pool.write_batch(batch);
        assert!(ack.results.iter().all(|r| r.is_ok()));
        assert_ne!(
            ack.results[0].as_ref().unwrap().arena_id,
            ack.results[1].as_ref().unwrap().arena_id,
        );
        assert_eq!(pool.arena_count(), 2);
    }
}
