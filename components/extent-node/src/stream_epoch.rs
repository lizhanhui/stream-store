use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};

use bytes::{Bytes, BytesMut};
use common::errors::{EpochSealedSnafu, InternalSnafu, StorageError};
use common::types::{Epoch, EpochState, Offset, StreamId};
use parking_lot::Mutex;
use smallvec::{SmallVec, smallvec};

use crate::arena::{Arena, ArenaAppendResult, ArenaId, ArenaPool, WriteBatchJob};

/// Sentinel value for `limit`: extent is not sealed.
const LIMIT_OPEN: u64 = u64::MAX;

/// Forward-flags bitmap: checked inline during `send_forward()` to
/// guarantee ordering relative to Forward frames.
pub const FLAG_INIT_FORWARD: u8 = 0x01;

/// ForwardChecksum has been received from primary (secondary side).
/// Used by `try_verify_checksum()` to know when to compare.
const FLAG_CHECKSUM_RECEIVED: u8 = 0x02;

/// StreamEpoch has been flushed to S3 and is eligible for memory eviction.
/// Set by Primary locally after upload, and by Secondaries on ForwardFlushed.
pub const FLAG_FLUSHED: u8 = 0x04;

/// Result of a successful append: the logical offset and the byte position
/// within the arena where the record was written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendResult {
    pub offset: Offset,
    /// Byte position from the start of the arena the record was
    /// written into. With arena rotation this is arena-local, not a
    /// stream-global offset; combined with the `AppendResult.offset`
    /// and `index_lookup`, callers can still resolve it back.
    pub byte_pos: u64,
}

/// An epoch: metadata over one-or-more `Arc<Arena>` byte pools.
///
/// The arena list grows on arena-full rotation. For Dedicated streams
/// (P2), each rotated arena belongs to the same `(stream_id, epoch)`
/// pair; its `start_offset` is the pre-rotation `committed_offset`, so
/// offset→arena lookup is a simple range scan.
///
/// # Concurrency
///
/// - Write-side state (`hasher`, `committed_offset`, the `arenas`
///   vec's tail growth) is mutated only by the stream-level leader.
/// - Read-side state (`committed_offset`, `finalized_crc32`, `flags`)
///   is published with `Release` and observed with `Acquire`.
/// - The `arenas` `Mutex` protects vec-level mutation (rotation);
///   reads clone `Arc<Arena>`s under the lock and drop the guard
///   before doing work.
pub struct StreamEpoch {
    pub stream_id: StreamId,
    pub start_offset: Offset,
    pub epoch: Epoch,

    /// The arenas holding records for this epoch, in the order they
    /// were created. Length grows on arena-full rotation.
    arenas: Mutex<SmallVec<[Arc<Arena>; 4]>>,

    /// Pool that mints arenas for this epoch. Held so that
    /// `write_batch` can request a successor on rotation without
    /// routing back through `Stream`.
    pool: Arc<dyn ArenaPool>,

    /// Fixed per-arena capacity, passed to `pool.allocate(...)` on
    /// both initial allocation and rotation.
    arena_capacity: u32,

    /// Committed logical offset: next offset after the last
    /// fully-written record (exclusive). Starts at `start_offset.0`.
    committed_offset: AtomicU64,

    /// Seal marker. `LIMIT_OPEN` while the epoch is active; count of
    /// accepted records once sealed.
    limit: AtomicU64,

    /// Lifecycle flags: `FLAG_INIT_FORWARD`, `FLAG_CHECKSUM_RECEIVED`,
    /// `FLAG_FLUSHED`.
    flags: AtomicU8,

    /// Per-epoch CRC32 hasher. Covers records in append order across
    /// all arenas in this epoch, so the finalized value matches what
    /// a reader would compute by replaying every record start-to-end.
    /// Single-writer by construction (stream leader owns write side).
    hasher: UnsafeCell<crc32fast::Hasher>,

    /// Finalized CRC32 checksum.
    /// - Primary: set at seal time after all writers drain.
    /// - Secondary: stores the primary's CRC32 from `ForwardChecksum`,
    ///   used as the expected value for verification.
    finalized_crc32: AtomicU32,

    /// Which arenas on this EN currently hold at least one directory
    /// entry for this (stream, epoch). Denormalized tail of
    /// `self.arenas` for fast enumeration; kept in sync with arena
    /// rotation.
    pub(crate) resident_arenas: Mutex<SmallVec<[ArenaId; 4]>>,

    /// Reference count of live directory entries for this epoch
    /// across all resident arenas. Init 1; decremented when Shape A
    /// upload releases an arena (P3).
    pub(crate) directory_ref_count: AtomicU32,
}

unsafe impl Send for StreamEpoch {}
unsafe impl Sync for StreamEpoch {}

impl StreamEpoch {
    /// Create a new epoch and allocate its first arena from `pool`.
    pub(crate) fn new(
        stream_id: StreamId,
        epoch: Epoch,
        start_offset: Offset,
        arena_capacity: u32,
        pool: Arc<dyn ArenaPool>,
    ) -> Self {
        let first = pool.allocate(stream_id, epoch, start_offset, arena_capacity);
        let first_id = first.arena_id;
        Self {
            stream_id,
            start_offset,
            epoch,
            arenas: Mutex::new(smallvec![first]),
            pool,
            arena_capacity,
            committed_offset: AtomicU64::new(start_offset.0),
            limit: AtomicU64::new(LIMIT_OPEN),
            flags: AtomicU8::new(FLAG_INIT_FORWARD),
            hasher: UnsafeCell::new(crc32fast::Hasher::new()),
            finalized_crc32: AtomicU32::new(0),
            resident_arenas: Mutex::new(smallvec![first_id]),
            directory_ref_count: AtomicU32::new(1),
        }
    }

    // ── Flag helpers ────────────────────────────────────────────────

    pub fn take_init_forward(&self) -> bool {
        self.flags.fetch_and(!FLAG_INIT_FORWARD, Ordering::AcqRel) & FLAG_INIT_FORWARD != 0
    }

    pub fn mark_flushed(&self) {
        self.flags.fetch_or(FLAG_FLUSHED, Ordering::Release);
    }

    pub fn is_flushed(&self) -> bool {
        self.flags.load(Ordering::Acquire) & FLAG_FLUSHED != 0
    }

    // ── Arena list helpers ──────────────────────────────────────────

    fn current_arena(&self) -> Arc<Arena> {
        self.arenas
            .lock()
            .last()
            .cloned()
            .expect("StreamEpoch must always have at least one arena")
    }

    fn rotate_arena(&self, next_start: Offset) -> Result<Arc<Arena>, StorageError> {
        let new_arena =
            self.pool
                .allocate(self.stream_id, self.epoch, next_start, self.arena_capacity);
        let new_id = new_arena.arena_id;
        self.arenas.lock().push(Arc::clone(&new_arena));
        self.resident_arenas.lock().push(new_id);
        self.directory_ref_count.fetch_add(1, Ordering::Release);
        Ok(new_arena)
    }

    fn arenas_snapshot(&self) -> SmallVec<[Arc<Arena>; 4]> {
        self.arenas.lock().clone()
    }

    /// Find the arena covering `offset`, or None. Walks
    /// `resident_arenas` in order; arenas are contiguous in offset
    /// space so this is O(number of rotations) which is ≤ ~4 in
    /// practice.
    fn find_arena_for_offset(&self, offset: Offset) -> Option<Arc<Arena>> {
        self.arenas
            .lock()
            .iter()
            .find(|a| a.contains_offset(offset))
            .cloned()
    }

    // ── Write path ──────────────────────────────────────────────────

    /// Arena-level batch append with internal rotation on `ArenaFull`.
    ///
    /// Single-writer: only the stream-level leader calls `write_batch`.
    /// On each job success, advances per-epoch `committed_offset` and
    /// updates the per-epoch CRC32 hasher inline. On `ArenaFull`,
    /// rotates to a fresh arena within the same epoch and retries the
    /// failing job; if the record does not fit in a freshly-rotated
    /// empty arena, returns `InternalSnafu` ("record too large").
    ///
    /// Returns one result per job in 1:1 order.
    pub(crate) fn write_batch(
        &self,
        jobs: &[WriteBatchJob],
    ) -> SmallVec<[Result<ArenaAppendResult, StorageError>; 16]> {
        let mut out: SmallVec<[Result<ArenaAppendResult, StorageError>; 16]> =
            SmallVec::with_capacity(jobs.len());

        // Seal gate.
        let limit = self.limit.load(Ordering::Acquire);
        if limit != LIMIT_OPEN {
            let committed_count =
                self.committed_offset.load(Ordering::Relaxed) - self.start_offset.0;
            if committed_count >= limit {
                for _ in jobs {
                    out.push(Err(EpochSealedSnafu {
                        stream_id: self.stream_id,
                        epoch: self.epoch,
                    }
                    .build()));
                }
                return out;
            }
        }

        self.write_batch_inner(jobs, &mut out);
        out
    }

    fn write_batch_inner(
        &self,
        jobs: &[WriteBatchJob],
        out: &mut SmallVec<[Result<ArenaAppendResult, StorageError>; 16]>,
    ) {
        let mut idx: usize = 0;
        while idx < jobs.len() {
            let arena = self.current_arena();
            let was_fresh = arena.record_count() == 0;
            let job = &jobs[idx];
            let one: [WriteBatchJob; 1] = [WriteBatchJob::new(job.offset, job.payload.clone())];
            let mut r = arena.write_batch_inline(&one);
            match r.pop().expect("one result") {
                Ok(ok) => {
                    let payload = &job.payload;
                    // SAFETY: single-writer invariant upheld by caller.
                    unsafe {
                        let h = &mut *self.hasher.get();
                        h.update(&(payload.len() as u32).to_be_bytes());
                        if !payload.is_empty() {
                            h.update(payload);
                        }
                    }
                    self.committed_offset.fetch_add(1, Ordering::Release);
                    out.push(Ok(ok));
                    idx += 1;
                }
                Err(StorageError::ArenaFull { .. }) => {
                    if was_fresh {
                        let err = InternalSnafu {
                            message: format!(
                                "record too large for arena: stream={} epoch={} arena_capacity={}",
                                self.stream_id, self.epoch, self.arena_capacity
                            ),
                        }
                        .build();
                        for _ in &jobs[idx..] {
                            out.push(Err(err_clone(&err)));
                        }
                        return;
                    }
                    let next_start = Offset(self.committed_offset.load(Ordering::Relaxed));
                    if let Err(e) = self.rotate_arena(next_start) {
                        for _ in &jobs[idx..] {
                            out.push(Err(err_clone(&e)));
                        }
                        return;
                    }
                    // Retry the same job on the new arena.
                }
                Err(e) => {
                    out.push(Err(e));
                    idx += 1;
                }
            }
        }
    }

    /// Convenience single-record append. Used by tests and by the
    /// `append_inner` / `replicate` shims that translate store-layer
    /// calls into a 1-job batch.
    pub fn append(&self, payload: Bytes) -> Result<AppendResult, StorageError> {
        self.append_inner(payload)
    }

    /// Primary append: 1-job batch through `write_batch`.
    pub(crate) fn append_inner(&self, payload: Bytes) -> Result<AppendResult, StorageError> {
        // Offset hint: Arena assigns its own authoritative offset; this
        // is just the anticipated value, ignored on the primary write.
        let hint = Offset(self.committed_offset.load(Ordering::Relaxed));
        let job = WriteBatchJob::new(hint, payload);
        let mut results = self.write_batch(std::slice::from_ref(&job));
        match results.pop().expect("one result per job") {
            Ok(r) => Ok(AppendResult {
                offset: r.offset,
                byte_pos: r.byte_pos as u64,
            }),
            Err(e) => Err(e),
        }
    }

    /// Secondary replicate: caller passes the primary-assigned offset.
    /// The offset must equal `committed_offset` for strict-order FIFO
    /// replay; any mismatch is treated as a protocol error.
    pub fn replicate(&self, offset: Offset, payload: Bytes) -> Result<AppendResult, StorageError> {
        if offset.0 < self.start_offset.0 {
            return Err(InternalSnafu {
                message: format!(
                    "stale forward: offset {} < start_offset {}",
                    offset.0, self.start_offset.0
                ),
            }
            .build());
        }
        let expected = self.committed_offset.load(Ordering::Relaxed);
        if offset.0 != expected {
            return Err(InternalSnafu {
                message: format!(
                    "out-of-order forward: got {} expected {}",
                    offset.0, expected
                ),
            }
            .build());
        }
        let job = WriteBatchJob::new(offset, payload);
        let mut results = self.write_batch(std::slice::from_ref(&job));
        match results.pop().expect("one result") {
            Ok(r) => {
                // Sanity: arena-assigned offset must match the primary's.
                if r.offset != offset {
                    return Err(InternalSnafu {
                        message: format!(
                            "arena assigned offset {} but primary said {}",
                            r.offset.0, offset.0
                        ),
                    }
                    .build());
                }
                Ok(AppendResult {
                    offset: r.offset,
                    byte_pos: r.byte_pos as u64,
                })
            }
            Err(e) => Err(e),
        }
    }

    // ── Read path ───────────────────────────────────────────────────

    /// Read up to `count` records starting at the given logical offset.
    ///
    /// Finds the arena containing `offset` and delegates. If the read
    /// exhausts the arena before `count` records, the tail of the
    /// result is drawn from successor arenas (rotation-aware).
    pub fn read_at_offset(&self, offset: Offset, count: u32) -> Result<Vec<Bytes>, StorageError> {
        let arenas = self.arenas_snapshot();
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

    /// Lookup the byte position of record `seq` (relative to
    /// `self.start_offset`). Walks the arena list; returns the
    /// arena-local byte position on hit, `None` otherwise.
    ///
    /// NOTE: with multi-arena epochs the returned byte position is
    /// arena-local, not epoch-global. Callers that need to route the
    /// read should prefer `read_at_offset`.
    pub fn index_lookup(&self, seq: u64) -> Option<u64> {
        let offset = Offset(self.start_offset.0 + seq);
        let arena = self.find_arena_for_offset(offset)?;
        let local_seq = offset.0 - arena.start_offset.0;
        arena.directory().single_entry().lookup(local_seq)
    }

    // ── Seal / finalize ─────────────────────────────────────────────

    pub fn seal(&self, committed_offset: Option<u64>) -> u64 {
        if let Some(offset) = committed_offset {
            // Secondary path: SM provides the authoritative committed offset.
            let count = offset.saturating_sub(self.start_offset.0);
            match self.limit.compare_exchange(
                LIMIT_OPEN,
                count,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => self.start_offset.0 + count,
                Err(limit) => self.start_offset.0 + limit,
            }
        } else {
            // Primary path: caller guarantees no writers are active.
            let preliminary = self.committed_offset.load(Ordering::Acquire) - self.start_offset.0;
            match self.limit.compare_exchange(
                LIMIT_OPEN,
                preliminary,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => {}
                Err(limit) => {
                    return self.start_offset.0 + limit;
                }
            }
            // Writers are drained; committed_offset is final.
            let final_count = self.committed_offset.load(Ordering::Acquire) - self.start_offset.0;
            if final_count > preliminary {
                self.limit.store(final_count, Ordering::Release);
            }
            self.finalize_crc32();
            self.start_offset.0 + final_count
        }
    }

    pub fn is_sealed(&self) -> bool {
        self.limit.load(Ordering::Acquire) != LIMIT_OPEN
    }

    pub fn correct_seal_offset(&self, end_offset: u64) {
        let count = end_offset.saturating_sub(self.start_offset.0);
        loop {
            let current = self.limit.load(Ordering::Acquire);
            if current == LIMIT_OPEN || current <= count {
                return;
            }
            match self
                .limit
                .compare_exchange(current, count, Ordering::Release, Ordering::Acquire)
            {
                Ok(_) => return,
                Err(_) => continue,
            }
        }
    }

    pub fn finalized_crc32(&self) -> Option<u32> {
        let crc = self.finalized_crc32.load(Ordering::Acquire);
        if crc == 0 && self.limit.load(Ordering::Acquire) == LIMIT_OPEN {
            return None;
        }
        Some(crc)
    }

    fn finalize_crc32(&self) {
        // SAFETY: caller must ensure no concurrent writer. For the
        // primary path, seal drains in_flight before calling. For the
        // secondary path (try_verify_checksum), the per-connection
        // sequential read loop upholds single-writer.
        let crc = unsafe { (*self.hasher.get()).clone().finalize() };
        self.finalized_crc32.store(crc, Ordering::Release);
    }

    /// With arena rotation the write_batch path advances
    /// `committed_offset` inline, so `try_advance_committed` is no
    /// longer needed for forward delivery. It is retained as a no-op
    /// stub so call sites that invoke it after
    /// `store_primary_checksum` stay green; the sequential replicate
    /// path already finalized committed state.
    pub fn try_advance_committed(&self) {
        // Intentional no-op. Forward frames advance committed_offset
        // synchronously via `replicate`.
    }

    pub fn store_primary_checksum(&self, crc32: u32) {
        self.finalized_crc32.store(crc32, Ordering::Release);
        self.flags
            .fetch_or(FLAG_CHECKSUM_RECEIVED, Ordering::Release);
    }

    pub fn try_verify_checksum(&self) -> Option<bool> {
        let flags = self.flags.load(Ordering::Acquire);
        if flags & FLAG_CHECKSUM_RECEIVED == 0 {
            return None;
        }
        let limit = self.limit.load(Ordering::Acquire);
        if limit == LIMIT_OPEN {
            return None;
        }
        let committed = self.committed_offset.load(Ordering::Acquire) - self.start_offset.0;
        if committed < limit {
            return None;
        }
        // Finalize on our side and compare.
        let local_crc = unsafe { (*self.hasher.get()).clone().finalize() };
        let primary_crc = self.finalized_crc32.load(Ordering::Acquire);
        Some(local_crc == primary_crc)
    }

    pub fn accepts_post_seal_writes(&self) -> bool {
        let limit = self.limit.load(Ordering::Acquire);
        if limit == LIMIT_OPEN {
            return false;
        }
        let count = self.committed_offset.load(Ordering::Acquire) - self.start_offset.0;
        count < limit
    }

    // ── Accessors ───────────────────────────────────────────────────

    pub fn state(&self) -> EpochState {
        if self.is_sealed() {
            EpochState::Sealed
        } else {
            EpochState::Active
        }
    }

    pub fn message_count(&self) -> u64 {
        self.committed_offset.load(Ordering::Acquire) - self.start_offset.0
    }

    pub fn next_offset(&self) -> Offset {
        Offset(self.committed_offset.load(Ordering::Acquire))
    }

    pub fn last_offset(&self) -> Option<Offset> {
        let offset = self.committed_offset.load(Ordering::Acquire);
        if offset <= self.start_offset.0 {
            None
        } else {
            Some(Offset(offset - 1))
        }
    }

    pub fn committed_offset(&self) -> u64 {
        self.committed_offset.load(Ordering::Acquire)
    }

    /// Total bytes written across every arena in this epoch.
    pub fn bytes_written(&self) -> u64 {
        self.arenas.lock().iter().map(|a| a.bytes_written()).sum()
    }

    /// Per-arena capacity (same value across every resident arena in
    /// this epoch; not a multiply-by-arenas total).
    pub fn capacity(&self) -> u32 {
        self.arena_capacity
    }

    pub fn limit_hint(&self) -> u64 {
        self.limit.load(Ordering::Acquire)
    }

    /// Concatenation of every resident arena's committed bytes.
    /// Used by `ForwardChecksum` and S3 flush.
    pub fn committed_data(&self) -> Bytes {
        let arenas = self.arenas_snapshot();
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

    #[allow(dead_code)]
    pub(crate) fn resident_arenas(&self) -> SmallVec<[ArenaId; 4]> {
        self.resident_arenas.lock().clone()
    }

    #[allow(dead_code)]
    pub fn incr_directory_ref(&self) -> u32 {
        self.directory_ref_count.fetch_add(1, Ordering::Relaxed) + 1
    }

    #[allow(dead_code)]
    pub fn decr_directory_ref(&self) -> u32 {
        let prev = self.directory_ref_count.fetch_sub(1, Ordering::Release);
        prev.saturating_sub(1)
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

impl std::fmt::Debug for StreamEpoch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamEpoch")
            .field("stream_id", &self.stream_id)
            .field("epoch", &self.epoch)
            .field("start_offset", &self.start_offset)
            .field("arena_count", &self.arenas.lock().len())
            .field(
                "committed_offset",
                &self.committed_offset.load(Ordering::Relaxed),
            )
            .field("limit", &self.limit.load(Ordering::Relaxed))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arena::{ArenaIdGenerator, DedicatedArenaPool};
    use smallvec::smallvec;

    fn new_epoch(start: u64, arena_capacity: u32) -> StreamEpoch {
        let pool: Arc<dyn ArenaPool> =
            Arc::new(DedicatedArenaPool::new(Arc::new(ArenaIdGenerator::new(1))));
        StreamEpoch::new(StreamId(7), Epoch(3), Offset(start), arena_capacity, pool)
    }

    #[test]
    fn append_inner_single_arena_happy_path() {
        let ep = new_epoch(0, 4096);
        let r0 = ep.append(Bytes::from_static(b"msg0")).unwrap();
        let r1 = ep.append(Bytes::from_static(b"msg1")).unwrap();
        let r2 = ep.append(Bytes::from_static(b"msg2")).unwrap();
        assert_eq!(r0.offset, Offset(0));
        assert_eq!(r1.offset, Offset(1));
        assert_eq!(r2.offset, Offset(2));
        assert_eq!(ep.committed_offset(), 3);
        assert_eq!(ep.arenas.lock().len(), 1);
        assert_eq!(ep.resident_arenas().len(), 1);
    }

    #[test]
    fn append_inner_rotates_on_arena_full() {
        // Capacity 16 bytes: fits two 4-byte payloads (4+4 each = 8 bytes).
        // Third record triggers rotation.
        let ep = new_epoch(100, 16);
        let r0 = ep.append(Bytes::from_static(b"aaaa")).unwrap();
        let r1 = ep.append(Bytes::from_static(b"bbbb")).unwrap();
        let r2 = ep.append(Bytes::from_static(b"cccc")).unwrap();

        assert_eq!(r0.offset, Offset(100));
        assert_eq!(r1.offset, Offset(101));
        assert_eq!(r2.offset, Offset(102));
        assert_eq!(ep.committed_offset(), 103);

        let arenas = ep.arenas.lock();
        assert_eq!(arenas.len(), 2);
        // First arena holds r0, r1; second arena holds r2 and starts at offset 102.
        assert_eq!(arenas[0].start_offset, Offset(100));
        assert_eq!(arenas[0].record_count(), 2);
        assert_eq!(arenas[1].start_offset, Offset(102));
        assert_eq!(arenas[1].record_count(), 1);
        drop(arenas);

        assert_eq!(ep.resident_arenas().len(), 2);
        assert_eq!(ep.directory_ref_count.load(Ordering::Acquire), 2);
    }

    #[test]
    fn read_at_offset_crosses_arena_boundary() {
        let ep = new_epoch(0, 16);
        for i in 0..5u32 {
            ep.append(Bytes::copy_from_slice(&i.to_be_bytes())).unwrap();
        }
        // Expect rotation across the boundary.
        assert!(ep.arenas.lock().len() >= 2);

        let msgs = ep.read_at_offset(Offset(0), 5).unwrap();
        assert_eq!(msgs.len(), 5);
        for (i, msg) in msgs.iter().enumerate() {
            assert_eq!(msg.as_ref(), (i as u32).to_be_bytes());
        }

        // Partial read starting mid-stream.
        let tail = ep.read_at_offset(Offset(3), 10).unwrap();
        assert_eq!(tail.len(), 2);
        assert_eq!(tail[0].as_ref(), 3u32.to_be_bytes());
        assert_eq!(tail[1].as_ref(), 4u32.to_be_bytes());
    }

    #[test]
    fn seal_after_rotation_finalizes_crc_over_all_records() {
        // Epoch-level CRC must cover every record in order across every arena.
        let ep = new_epoch(0, 16);
        let payloads: &[&[u8]] = &[b"aaaa", b"bbbb", b"cccc", b"dddd"];
        for p in payloads {
            ep.append(Bytes::copy_from_slice(p)).unwrap();
        }
        assert!(ep.arenas.lock().len() >= 2, "expected rotation");
        ep.seal(None);

        // Independently compute the expected CRC over [len:be u32][payload]*.
        let mut expected = crc32fast::Hasher::new();
        for p in payloads {
            expected.update(&(p.len() as u32).to_be_bytes());
            expected.update(p);
        }
        assert_eq!(ep.finalized_crc32(), Some(expected.finalize()));
    }

    #[test]
    fn write_batch_returns_per_job_results_in_order() {
        let ep = new_epoch(0, 4096);
        let jobs: SmallVec<[WriteBatchJob; 16]> = smallvec![
            WriteBatchJob::new(Offset(0), Bytes::from_static(b"a")),
            WriteBatchJob::new(Offset(1), Bytes::from_static(b"bb")),
            WriteBatchJob::new(Offset(2), Bytes::from_static(b"ccc")),
        ];
        let results = ep.write_batch(&jobs);
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].as_ref().unwrap().offset, Offset(0));
        assert_eq!(results[1].as_ref().unwrap().offset, Offset(1));
        assert_eq!(results[2].as_ref().unwrap().offset, Offset(2));
        assert_eq!(ep.committed_offset(), 3);
    }

    #[test]
    fn write_batch_seal_rejects_remaining_jobs() {
        let ep = new_epoch(0, 4096);
        ep.append(Bytes::from_static(b"a")).unwrap();
        ep.seal(None);

        let jobs: SmallVec<[WriteBatchJob; 16]> = smallvec![
            WriteBatchJob::new(Offset(1), Bytes::from_static(b"rejected1")),
            WriteBatchJob::new(Offset(2), Bytes::from_static(b"rejected2")),
        ];
        let results = ep.write_batch(&jobs);
        assert_eq!(results.len(), 2);
        assert!(matches!(results[0], Err(StorageError::EpochSealed { .. })));
        assert!(matches!(results[1], Err(StorageError::EpochSealed { .. })));
    }

    #[test]
    fn write_batch_record_too_large_for_arena_reports_internal() {
        // Capacity 8: any 5+ byte payload would need > 8 bytes (len 4 + payload).
        let ep = new_epoch(0, 8);
        let jobs: SmallVec<[WriteBatchJob; 16]> = smallvec![
            WriteBatchJob::new(Offset(0), Bytes::from_static(b"toolarge")),
            WriteBatchJob::new(Offset(1), Bytes::from_static(b"next")),
        ];
        let results = ep.write_batch(&jobs);
        assert_eq!(results.len(), 2);
        // First fails as "too large", second inherits the fail (no forward progress).
        for r in &results {
            assert!(matches!(r, Err(StorageError::Internal { .. })));
        }
        // No records committed, no rotation.
        assert_eq!(ep.committed_offset(), 0);
        assert_eq!(ep.arenas.lock().len(), 1);
    }

    #[test]
    fn replicate_out_of_order_fails() {
        let ep = new_epoch(0, 4096);
        // Replicate offset 0 OK.
        ep.replicate(Offset(0), Bytes::from_static(b"a")).unwrap();
        // Offset 2 skipping 1 fails.
        let err = ep
            .replicate(Offset(2), Bytes::from_static(b"b"))
            .unwrap_err();
        assert!(matches!(err, StorageError::Internal { .. }));
    }

    #[test]
    fn replicate_matches_primary_offset_across_rotation() {
        // Simulate a secondary replay with offsets provided by the primary.
        let ep = new_epoch(0, 16);
        let payloads: &[&[u8]] = &[b"aaaa", b"bbbb", b"cccc"];
        for (i, p) in payloads.iter().enumerate() {
            ep.replicate(Offset(i as u64), Bytes::copy_from_slice(p))
                .unwrap();
        }
        assert!(ep.arenas.lock().len() >= 2);
        assert_eq!(ep.committed_offset(), 3);
        let msgs = ep.read_at_offset(Offset(0), 3).unwrap();
        assert_eq!(msgs.len(), 3);
        for (i, msg) in msgs.iter().enumerate() {
            assert_eq!(msg.as_ref(), payloads[i]);
        }
    }
}
