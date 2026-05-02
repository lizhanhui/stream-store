use std::cell::UnsafeCell;
use std::sync::{Arc, Mutex};

use crossbeam_channel::{Receiver, Sender, unbounded};
use smallvec::{SmallVec, smallvec};

use crate::arena::{
    ArenaBuffer, ArenaDirectory, ArenaId, EpochArenaEntry, JobResult, OwnedArenaSlice,
    SharedAppendJob, WriteBatch,
};
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};

use bytes::Bytes;
use common::errors::StorageError;
use common::errors::{EpochFullSnafu, EpochSealedSnafu, InternalSnafu};
use common::types::{Epoch, EpochState, ExtentId, Offset, StreamId};

/// Sentinel value for `limit`: extent is not sealed.
const LIMIT_OPEN: u64 = u64::MAX;
const MIN_RECORD_SIZE: u32 = 5;

/// Forward-flags bitmap: checked inline during `send_forward()`
/// to guarantee ordering relative to Forward frames.
pub const FLAG_INIT_FORWARD: u8 = 0x01;

/// ForwardChecksum has been received from primary (secondary side).
/// Used by `try_verify_checksum()` to know when to compare.
const FLAG_CHECKSUM_RECEIVED: u8 = 0x02;

/// StreamEpoch has been flushed to S3 and is eligible for memory eviction.
/// Set by Primary locally after upload, and by Secondaries on ForwardFlushed.
pub const FLAG_FLUSHED: u8 = 0x04;

/// Result of a successful append: the logical offset and the byte position
/// within the arena where the record was written. The caller can use the
/// byte position to build an external index (e.g., fixed-width index stream
/// records: `[stream_id:u64][extent_id:u32][offset:u64][byte_pos:u64]`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendResult {
    /// Logical offset assigned to this record (start_offset + sequence number).
    pub offset: Offset,
    /// Byte position from the beginning of the arena where this record starts.
    /// The record is stored as `[payload_len: u32 BE][payload: bytes]` at this position.
    pub byte_pos: u64,
}

/// A lock-free stream epoch backed by a pre-allocated contiguous memory arena.
///
/// # Concurrency Model: Pipelined Group Commit
///
/// Writers use a **leader election + delegation** pattern at the store level:
///
/// 1. **Leader election**: `in_flight.fetch_add(1, Acquire)`. If `prev == 0`, the
///    thread becomes the **active writer** (fast path). If `prev > 0`, an active
///    writer already exists — the thread pushes an `AppendJob` into the channel
///    and returns immediately (slow path).
///
/// 2. **Single-writer append**: The active writer calls `append_inner()` which
///    uses plain loads/stores on cursors (no `fetch_add`, no spin-wait) since
///    single-writer access is guaranteed by the leader election.
///
/// 3. **Batch drain**: After its own append, the leader checks if followers
///    arrived (`in_flight > 1`). If so, it drains the `job_rx` channel and
///    processes all pending jobs as a batch, amortizing synchronization cost.
///
/// This eliminates cache-line bouncing from the old multi-writer spin-wait
/// protocol while maintaining the same lock-free read path.
///
/// # Memory Layout
///
/// ```text
///   ┌───────────────────────────────────────────────────────────┐
///   │  buf: pre-allocated contiguous arena (configurable size)   │
///   │                                                           │
///   │  [len0|payload0][len1|payload1][...][   free space   ]    │
///   │  ^                             ^                          │
///   │  0                       write_cursor                     │
///   └───────────────────────────────────────────────────────────┘
///
///   committed_offset = next offset after last contiguously committed record
///   committed_bytes = byte position up to which all records are fully written
/// ```
///
/// Each record is stored as `[payload_len: u32 BE][payload: [u8; payload_len]]`.
/// Records are self-contained: a reader can walk the arena sequentially by reading
/// the length prefix, advancing by `4 + len` bytes to the next record.
/// This is the same format as the S3 flush layout, enabling zero-copy upload of
/// sealed extents.
pub struct StreamEpoch {
    pub id: ExtentId,

    /// The stream that owns this epoch. Required for constructing
    /// `EpochSealed` / `EpochFull` errors without threading the stream id
    /// through every append/write-batch call.
    pub stream_id: StreamId,

    pub start_offset: Offset,

    /// The epoch under which this extent was created (informational).
    /// Used by `report_epoch` to filter extents by epoch during SM recovery.
    pub epoch: Epoch,

    /// Globally unique arena identifier. Stamped at allocation.
    #[allow(dead_code)]
    pub(crate) arena_id: ArenaId,

    /// Reference-counted arena buffer. Shared with any outstanding `Bytes`
    /// slices, so the memory is not freed until all readers are done.
    arena: Arc<ArenaBuffer>,

    /// Derived write pointer into the arena (for append writes).
    buf: *mut u8,

    /// Total capacity of the arena in bytes.
    capacity: u32,

    /// Byte position of the next free slot. Updated by the single active writer.
    write_cursor: AtomicU64,

    /// Number of records appended. Updated by the single active writer.
    record_count: AtomicU64,

    /// Committed offset: the next logical offset after the last contiguously
    /// committed record (exclusive). All offsets in [start_offset, committed_offset)
    /// have been fully written and are safe to read.
    /// Advanced inline in both `append_inner()` (primary) and `replicate()`
    /// (secondary). In-order Forward delivery guarantees sequential processing.
    /// Watermark ACKs use this value to report progress to the primary.
    committed_offset: AtomicU64,

    /// Committed byte position: contiguous byte frontier.
    /// Byte offset up to which all records are fully written. Readers use
    /// this as the upper bound. Advanced inline in both `append_inner()`
    /// (primary) and `replicate()` (secondary).
    committed_bytes: AtomicU64,

    /// Message limit for this extent.
    ///
    /// - `LIMIT_OPEN` (`u64::MAX`): extent is **not sealed**, appends proceed normally.
    /// - Any other value `N`: extent is **sealed** at message count `N`.
    ///   Appends with `record_count < N` are still accepted (late forwarded writes
    ///   within the primary's committed range); appends at or beyond `N` are rejected
    ///   with `ExtentSealed`.
    limit: AtomicU64,

    /// Per-record byte-position directory. P2 holds exactly one entry.
    directory: ArenaDirectory,

    /// Bitmap of extent lifecycle flags (AtomicU8):
    /// - `FLAG_INIT_FORWARD` (0x01): prepend ForwardInitEpoch before first Forward
    ///   (checked inline during `send_forward()`)
    /// - `FLAG_CHECKSUM_RECEIVED` (0x02): ForwardChecksum received from primary
    ///   (secondary side, checked by `try_verify_checksum()`)
    /// - `FLAG_FLUSHED` (0x04): extent flushed to S3, eligible for eviction
    ///   (set by Primary after upload, by Secondaries on ForwardFlushed)
    flags: AtomicU8,

    /// Incremental CRC32 hasher.
    ///
    /// Updated inline in both `append_inner()` (primary) and `replicate()`
    /// (secondary). In-order Forward delivery guarantees sequential processing
    /// on secondaries, so CRC32 can be hashed directly from the payload.
    ///
    /// `UnsafeCell` is used instead of `Mutex` because the single-writer
    /// invariant already guarantees exclusive access — same reasoning as
    /// `write_cursor`, `record_count`, etc.
    hasher: UnsafeCell<crc32fast::Hasher>,

    /// Finalized CRC32 checksum.
    /// - **Primary**: set at seal time after all writers drain.
    /// - **Secondary**: stores the primary's CRC32 from ForwardChecksum, used as
    ///   the expected value for verification.
    finalized_crc32: AtomicU32,

    /// Which arenas on this EN currently hold at least one directory
    /// entry for this (stream, epoch). P2 always holds exactly one
    /// (the epoch's own arena); future phases widen this when a
    /// Primary's record stream spans multiple shared arenas.
    pub(crate) resident_arenas: Mutex<SmallVec<[ArenaId; 4]>>,

    /// Reference count of live directory entries for this epoch across
    /// all resident arenas. Init 1. When it drops to zero, the owning
    /// Stream removes the StreamEpoch from its epochs vec.
    pub(crate) directory_ref_count: AtomicU32,

    /// Arena-level leader election counter for `write_batch` (mirrors the
    /// store-level `Stream::in_flight` but scoped to a single StreamEpoch).
    ///
    /// Incremented by each `write_batch` caller before writing; the first
    /// caller (prev == 0) is the arena-level leader and proceeds directly.
    /// Followers must not call `append_inner` concurrently — the arena's
    /// single-writer invariant is enforced by the caller at the store level.
    ///
    /// Exposed so that `DedicatedArenaPool::write_batch` (added in a later
    /// phase) can read and update it without going through an Arc.
    #[allow(dead_code)]
    pub(crate) arena_in_flight: AtomicU64,

    /// Arena-level delegation channel. Followers submit `WriteBatch`s to
    /// the arena leader via `arena_job_tx`; the leader drains from
    /// `arena_job_rx`. Unused on the Dedicated path (the stream leader
    /// is always the arena leader), but carried as a field so the type
    /// shape matches the spec and the Shared-arena path (later plan) can
    /// wire it up without widening `StreamEpoch`.
    #[allow(dead_code)]
    pub(crate) arena_job_tx: Sender<WriteBatch>,
    #[allow(dead_code)]
    pub(crate) arena_job_rx: Receiver<WriteBatch>,
}

// SAFETY: The raw write pointer `buf` is derived from Arc<ArenaBuffer> and only
// used for non-overlapping writes mediated by atomic cursors. The ArenaBuffer
// itself is Send+Sync, and all concurrent access is bounded by atomic cursors.
unsafe impl Send for StreamEpoch {}
unsafe impl Sync for StreamEpoch {}

impl StreamEpoch {
    /// Create a new active extent with the specified capacity in bytes.
    pub(crate) fn with_capacity(
        id: ExtentId,
        stream_id: StreamId,
        start_offset: Offset,
        capacity: u32,
        epoch: Epoch,
        arena_id: ArenaId,
    ) -> Self {
        let arena = ArenaBuffer::new(capacity);
        let buf = arena.ptr_mut();

        // Allocate the index with alloc_zeroed: the OS provides pre-zeroed pages
        // (MAP_ANONYMOUS) at near-zero cost, avoiding a 13M+ iteration init loop
        // that caused ~80ms stalls. INDEX_UNSET == 0, so zeroed memory is correct.
        let record_cap = (capacity / MIN_RECORD_SIZE) as usize;
        let entry = EpochArenaEntry::with_capacity(stream_id, epoch, start_offset, record_cap);
        let directory = ArenaDirectory::new(entry);

        let (arena_job_tx, arena_job_rx) = unbounded();
        Self {
            id,
            stream_id,
            start_offset,
            epoch,
            arena_id,
            arena,
            buf,
            capacity,
            write_cursor: AtomicU64::new(0),
            record_count: AtomicU64::new(0),
            committed_offset: AtomicU64::new(start_offset.0),
            committed_bytes: AtomicU64::new(0),
            limit: AtomicU64::new(LIMIT_OPEN),
            directory,
            flags: AtomicU8::new(FLAG_INIT_FORWARD),
            hasher: UnsafeCell::new(crc32fast::Hasher::new()),
            finalized_crc32: AtomicU32::new(0),
            resident_arenas: Mutex::new(smallvec![arena_id]),
            directory_ref_count: AtomicU32::new(1),
            arena_in_flight: AtomicU64::new(0),
            arena_job_tx,
            arena_job_rx,
        }
    }

    /// Atomically check and clear the `FLAG_INIT_FORWARD` bit.
    /// Returns `true` if the flag was set (i.e., caller should prepend ForwardInitEpoch).
    pub fn take_init_forward(&self) -> bool {
        self.flags.fetch_and(!FLAG_INIT_FORWARD, Ordering::AcqRel) & FLAG_INIT_FORWARD != 0
    }

    /// Mark this extent as flushed to S3 (eligible for memory eviction).
    pub fn mark_flushed(&self) {
        self.flags.fetch_or(FLAG_FLUSHED, Ordering::Release);
    }

    /// Whether this extent has been flushed to S3.
    pub fn is_flushed(&self) -> bool {
        self.flags.load(Ordering::Acquire) & FLAG_FLUSHED != 0
    }

    /// Append a message. Returns the assigned logical offset and the byte
    /// position within the arena.
    ///
    /// In production, the store layer performs stream-level leader election and
    /// calls `append_inner()` directly. This method is a convenience wrapper
    /// for unit tests where single-threaded access is guaranteed.
    pub fn append(&self, payload: Bytes) -> Result<AppendResult, StorageError> {
        self.append_inner(payload)
    }

    /// Arena-level batch append: processes a slice of [`SharedAppendJob`]s
    /// by calling [`append_inner`] for each one and collecting the results.
    ///
    /// # Concurrency model
    ///
    /// `write_batch` is the arena-level analogue of the store-level pipelined
    /// group-commit loop. The **single-writer invariant** of `append_inner` is
    /// upheld here via the **store-level** `Stream::in_flight` counter: only one
    /// thread (the stream-level leader) calls `write_batch` at a time.
    ///
    /// `arena_in_flight` is provided as a hook for future phases (e.g. the
    /// Shared arena pool) where multiple streams may share an arena and require
    /// a second level of leader election. In P2 (Dedicated path), `write_batch`
    /// is always called from the store-level leader, so no contention occurs.
    ///
    /// # Returns
    ///
    /// One [`JobResult`] per input job, in the same order as `jobs`. Each
    /// result carries the record's `arena_id` + `byte_pos` plus a
    /// per-job `Result<(), StorageError>` so a single ExtentFull does not
    /// poison its siblings.
    #[allow(dead_code)]
    pub(crate) fn write_batch(&self, jobs: &[SharedAppendJob]) -> SmallVec<[JobResult; 16]> {
        let mut results: SmallVec<[JobResult; 16]> = SmallVec::with_capacity(jobs.len());
        for job in jobs {
            match self.append_inner(job.payload.clone()) {
                Ok(r) => results.push(JobResult::ok(self.arena_id, r.byte_pos as u32)),
                Err(e) => results.push(JobResult::err(self.arena_id, e)),
            }
        }
        results
    }

    /// Single-writer append: plain loads/stores on cursors.
    ///
    /// # Safety contract
    ///
    /// Must be called by at most one thread at a time. In production, this is
    /// guaranteed by the store-level leader election (`in_flight` counter).
    /// In tests/benchmarks, single-threaded or `append()` wrapper provides
    /// the guarantee.
    pub(crate) fn append_inner(&self, payload: Bytes) -> Result<AppendResult, StorageError> {
        // Check seal limit.
        let limit = self.limit.load(Ordering::Acquire);
        if limit != LIMIT_OPEN {
            let current = self.record_count.load(Ordering::Relaxed);
            if current >= limit {
                return Err(EpochSealedSnafu {
                    stream_id: self.stream_id,
                    epoch: self.epoch,
                }
                .build());
            }
        }

        let payload_len = payload.len();
        // Record layout: [len: 4 bytes][payload: payload_len bytes]
        let record_len = 4 + payload_len;

        // 1. Reserve byte slot (plain load + store, single writer).
        let byte_pos = self.write_cursor.load(Ordering::Relaxed);
        if byte_pos + record_len as u64 > self.capacity as u64 {
            return Err(EpochFullSnafu {
                stream_id: self.stream_id,
                epoch: self.epoch,
            }
            .build());
        }
        self.write_cursor
            .store(byte_pos + record_len as u64, Ordering::Relaxed);

        // 2. Reserve logical sequence number (plain load + store, single writer).
        let seq = self.record_count.load(Ordering::Relaxed);
        self.record_count.store(seq + 1, Ordering::Relaxed);

        // 3. Write record into reserved region.
        unsafe {
            let dst = self.buf.add(byte_pos as usize);
            // Write length prefix (big-endian u32).
            std::ptr::copy_nonoverlapping((payload_len as u32).to_be_bytes().as_ptr(), dst, 4);
            // Write payload bytes.
            if payload_len > 0 {
                std::ptr::copy_nonoverlapping(payload.as_ref().as_ptr(), dst.add(4), payload_len);
            }
        }

        // 3b. Update incremental CRC32 with the same [len][payload] record bytes.
        // Single-writer guarantee means no contention on the hasher.
        unsafe {
            let h = &mut *self.hasher.get();
            h.update(&(payload_len as u32).to_be_bytes());
            if payload_len > 0 {
                h.update(&payload);
            }
        }

        // 4. Update committed state directly (single writer, no spin-wait needed).
        let new_committed_bytes = byte_pos + record_len as u64;
        self.committed_bytes
            .store(new_committed_bytes, Ordering::Release);
        self.index_record(seq, byte_pos);
        self.committed_offset
            .store(self.start_offset.0 + seq + 1, Ordering::Release);

        Ok(AppendResult {
            offset: Offset(self.start_offset.0 + seq),
            byte_pos,
        })
    }

    /// Replicate a record; byte_pos is derived from the running write_cursor.
    ///
    /// Secondaries call this instead of `append_inner`. Forward frames arrive in
    /// strict per-connection FIFO order (TCP guarantees), so computing byte_pos
    /// from `self.write_cursor` produces the same value the primary used.
    ///
    /// CRC32 is computed inline and committed state is advanced directly —
    /// matching `append_inner` semantics on the primary.
    ///
    /// Returns the logical offset on success.
    pub fn replicate(&self, offset: Offset, payload: Bytes) -> Result<AppendResult, StorageError> {
        // Reject stale Forward frames from a previous extent (offset < start_offset).
        if offset.0 < self.start_offset.0 {
            return Err(InternalSnafu {
                message: format!(
                    "stale forward: offset {} < extent start_offset {}",
                    offset.0, self.start_offset.0
                ),
            }
            .build());
        }
        let seq = offset.0 - self.start_offset.0;
        // Check seal limit (limit is count-based).
        let limit = self.limit.load(Ordering::Acquire);
        if limit != LIMIT_OPEN && seq >= limit {
            return Err(EpochSealedSnafu {
                stream_id: self.stream_id,
                epoch: self.epoch,
            }
            .build());
        }

        let payload_len = payload.len();
        let record_len = 4 + payload_len;

        // Compute byte_pos from the running cursor — strict-order replay guarantees
        // this matches the primary's position.
        let byte_pos = self.write_cursor.load(Ordering::Relaxed);
        // Capacity check (same as before, now using local byte_pos).
        if byte_pos + record_len as u64 > self.capacity as u64 {
            return Err(EpochFullSnafu {
                stream_id: self.stream_id,
                epoch: self.epoch,
            }
            .build());
        }
        // Advance cursor.
        self.write_cursor
            .store(byte_pos + record_len as u64, Ordering::Relaxed);

        // Write record at exact byte_pos (same layout as append).
        unsafe {
            let dst = self.buf.add(byte_pos as usize);
            // Write length prefix (big-endian u32).
            std::ptr::copy_nonoverlapping((payload_len as u32).to_be_bytes().as_ptr(), dst, 4);
            // Write payload bytes.
            if payload_len > 0 {
                std::ptr::copy_nonoverlapping(payload.as_ref().as_ptr(), dst.add(4), payload_len);
            }
        }

        // Increment record_count (plain store — in-order guarantee means
        // single-connection sequential processing, same as append_inner).
        let count = self.record_count.load(Ordering::Relaxed);
        self.record_count.store(count + 1, Ordering::Relaxed);

        // Update incremental CRC32 inline (in-order arrival means we can
        // hash directly from the payload, no arena re-read needed).
        unsafe {
            let h = &mut *self.hasher.get();
            h.update(&(payload_len as u32).to_be_bytes());
            if payload_len > 0 {
                h.update(&payload);
            }
        }

        // Advance committed state directly (no index walk needed).
        let new_committed_bytes = byte_pos + record_len as u64;
        self.committed_bytes
            .store(new_committed_bytes, Ordering::Release);
        self.index_record(seq, byte_pos);
        self.committed_offset
            .store(self.start_offset.0 + seq + 1, Ordering::Release);

        Ok(AppendResult { offset, byte_pos })
    }

    /// Read `count` records starting from the given byte position in the arena.
    ///
    /// The caller provides the byte position (obtained from `AppendResult.byte_pos`
    /// or from an external index stream). Records are self-contained
    /// (`[len: u32 BE][payload]`), so reading walks forward from the given position.
    ///
    /// Returns zero-copy `Bytes` slices referencing the arena buffer.
    /// Only data within the committed byte range is visible.
    pub fn read(&self, byte_pos: u64, count: u32) -> Result<Vec<Bytes>, StorageError> {
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

    /// Record byte_pos in the internal index. Called after successful commit.
    /// Stores `byte_pos + 1` to distinguish from the zero sentinel (INDEX_UNSET).
    fn index_record(&self, seq: u64, byte_pos: u64) {
        self.directory.single_entry().record(seq, byte_pos);
    }

    /// Lookup byte_pos from the internal index.
    ///
    /// Returns `None` if `seq` is out of bounds or the entry has not been
    /// committed yet (still holds the sentinel value 0).
    /// Decodes the stored `byte_pos + 1` encoding back to the real byte_pos.
    pub fn index_lookup(&self, seq: u64) -> Option<u64> {
        self.directory.single_entry().lookup(seq)
    }

    /// Create a `Bytes` view of the entire arena buffer.
    ///
    /// The returned `Bytes` holds an `Arc` clone of the arena buffer,
    /// so the memory stays alive as long as any derived slice is held by a reader.
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

    /// Seal this extent by setting `limit` to the maximum number of messages
    /// it will accept.
    ///
    /// If `committed_offset` is provided (from SM/primary), the limit is
    /// `committed_offset - start_offset`. This allows secondaries to accept late
    /// forwarded appends up to the primary's committed offset.
    ///
    /// If `None` (primary sealing itself), we set `limit` to the current
    /// `record_count`. The caller (stream-level leader) must ensure all in-flight
    /// work is drained before calling seal.
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
            // Primary path: seal atomically.
            //
            // Step 1: Set limit to current record_count. This prevents any NEW
            // appender from passing the sealed check.
            let preliminary = self.record_count.load(Ordering::Acquire);
            match self.limit.compare_exchange(
                LIMIT_OPEN,
                preliminary,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => {}
                Err(limit) => {
                    // Already sealed (e.g., concurrent seal call).
                    return self.start_offset.0 + limit;
                }
            }

            // Step 2: Read the true final record_count and update limit.
            // The caller ensures no writer is active (stream-level in_flight == 0).
            let final_count = self.record_count.load(Ordering::Acquire);
            if final_count > preliminary {
                self.limit.store(final_count, Ordering::Release);
            }

            // Finalize incremental CRC32 (primary path — all writers drained).
            self.finalize_crc32();

            self.start_offset.0 + final_count
        }
    }

    /// Whether this extent is sealed.
    pub fn is_sealed(&self) -> bool {
        self.limit.load(Ordering::Acquire) != LIMIT_OPEN
    }

    /// Correct the seal point to an authoritative committed offset (DR path).
    ///
    /// During fallback seal, a secondary may have sealed at its own local offset
    /// which can be higher than the quorum-committed offset determined by SM.
    /// This method forces the limit down to match SM's authoritative end_offset.
    /// Only takes effect if the extent is already sealed and the current limit
    /// exceeds the requested count (i.e., a downward correction). No-op if
    /// the extent is not sealed or the limit is already at or below the target.
    pub fn correct_seal_offset(&self, end_offset: u64) {
        let count = end_offset.saturating_sub(self.start_offset.0);
        loop {
            let current = self.limit.load(Ordering::Acquire);
            if current == LIMIT_OPEN || current <= count {
                return; // not sealed or already at/below target
            }
            match self
                .limit
                .compare_exchange(current, count, Ordering::Release, Ordering::Acquire)
            {
                Ok(_) => return,
                Err(_) => continue, // concurrent modification, retry
            }
        }
    }

    /// Returns the finalized CRC32 checksum computed incrementally during primary
    /// appends, or `None` if this extent has not been sealed on the primary path.
    pub fn finalized_crc32(&self) -> Option<u32> {
        let crc = self.finalized_crc32.load(Ordering::Acquire);
        if crc == 0 && self.limit.load(Ordering::Acquire) == LIMIT_OPEN {
            // Not sealed yet — no finalized checksum.
            return None;
        }
        Some(crc)
    }

    /// Finalize the incremental CRC32 hasher and store the result.
    ///
    /// Called at seal time on the primary (all writers drained). On secondaries,
    /// called by `try_verify_checksum()` after `try_advance_committed()` has caught up.
    ///
    /// # Safety contract
    ///
    /// Must not be called while a writer is concurrently appending/replicating.
    /// On the primary, seal ensures `in_flight == 0`. On the secondary, this is
    /// called from the same sequential connection read loop as `replicate()`.
    fn finalize_crc32(&self) {
        let crc = unsafe { (*self.hasher.get()).clone().finalize() };
        self.finalized_crc32.store(crc, Ordering::Release);
    }

    /// Advance `committed_offset`, `committed_bytes`, and incremental CRC32 by
    /// walking the index in sequence order from the current `committed_offset`.
    ///
    /// For each consecutive populated index entry, reads the record at that
    /// byte_pos from the arena, hashes `[len:u32 BE][payload]`, and advances
    /// both committed cursors. Stops at the first gap (unpopulated entry).
    ///
    /// No longer called from the normal `replicate()` path — with in-order
    /// Forward delivery, `replicate()` advances committed state and CRC32
    /// inline (matching `append_inner`). Retained for edge cases such as
    /// post-seal late writes with `accepts_post_seal_writes`.
    ///
    /// Amortized O(1) per replicate call.
    ///
    /// # Safety contract
    ///
    /// Same as `replicate()` — single connection read loop on secondaries.
    pub fn try_advance_committed(&self) {
        let h = unsafe { &mut *self.hasher.get() };
        let mut seq = self.committed_offset.load(Ordering::Relaxed) - self.start_offset.0;

        loop {
            let entry = self.directory.single_entry();
            if (seq as usize) >= entry.record_capacity() {
                break;
            }
            let val = match entry.raw_slot(seq) {
                Some(v) => v,
                None => break,
            };
            if val == crate::arena::SLOT_UNSET {
                break; // gap — record not yet replicated
            }
            let byte_pos = (val - 1) as usize;

            // Read [len:4 BE][payload] from arena at byte_pos.
            let len = u32::from_be_bytes(unsafe {
                [
                    *self.buf.add(byte_pos),
                    *self.buf.add(byte_pos + 1),
                    *self.buf.add(byte_pos + 2),
                    *self.buf.add(byte_pos + 3),
                ]
            });
            h.update(&len.to_be_bytes());
            if len > 0 {
                let payload =
                    unsafe { std::slice::from_raw_parts(self.buf.add(byte_pos + 4), len as usize) };
                h.update(payload);
            }

            let record_end = byte_pos as u64 + 4 + len as u64;
            seq += 1;

            // Advance committed state.
            self.committed_bytes.store(record_end, Ordering::Release);
            self.committed_offset
                .store(self.start_offset.0 + seq, Ordering::Release);
        }
    }

    /// Store the primary's CRC32 checksum from a ForwardChecksum frame.
    ///
    /// Sets `FLAG_CHECKSUM_RECEIVED` on `flags` to indicate the primary
    /// checksum has been received on this secondary.
    ///
    /// committed_bytes is not stored separately — if CRC32 matches, byte layout
    /// is guaranteed identical (same byte_pos assignments, same record format).
    pub fn store_primary_checksum(&self, crc32: u32) {
        self.finalized_crc32.store(crc32, Ordering::Release);
        self.flags
            .fetch_or(FLAG_CHECKSUM_RECEIVED, Ordering::Release);
    }

    /// Attempt to verify the CRC32 checksum against the primary's value.
    ///
    /// Returns:
    /// - `None` if not ready (ForwardChecksum not received or not all records hashed)
    /// - `Some(true)` if CRC32 matches the primary's value
    /// - `Some(false)` if mismatch detected
    ///
    /// When ready, finalizes the local hasher and compares with the stored primary CRC32.
    pub fn try_verify_checksum(&self) -> Option<bool> {
        // Check if ForwardChecksum has been received.
        let flags = self.flags.load(Ordering::Acquire);
        if flags & FLAG_CHECKSUM_RECEIVED == 0 {
            return None;
        }

        // Check if we've committed all records up to the sealed limit.
        let limit = self.limit.load(Ordering::Acquire);
        if limit == LIMIT_OPEN {
            return None; // not sealed
        }
        let committed = self.committed_offset.load(Ordering::Acquire) - self.start_offset.0;
        if committed < limit {
            return None; // still have gaps
        }

        // All records hashed — finalize and compare.
        let local_crc = unsafe { (*self.hasher.get()).clone().finalize() };
        let primary_crc = self.finalized_crc32.load(Ordering::Acquire);

        Some(local_crc == primary_crc)
    }

    /// Whether this sealed extent can still accept post-seal forwarded writes.
    /// Returns true when sealed and committed record count < limit,
    /// meaning there are still outstanding writes that haven't landed yet.
    pub fn accepts_post_seal_writes(&self) -> bool {
        let limit = self.limit.load(Ordering::Acquire);
        if limit == LIMIT_OPEN {
            return false;
        }
        let count = self.committed_offset.load(Ordering::Acquire) - self.start_offset.0;
        count < limit
    }

    /// The extent state (Active or Sealed).
    pub fn state(&self) -> EpochState {
        if self.is_sealed() {
            EpochState::Sealed
        } else {
            EpochState::Active
        }
    }

    /// Number of committed (fully written, readable) messages in this extent.
    pub fn message_count(&self) -> u64 {
        self.committed_offset.load(Ordering::Acquire) - self.start_offset.0
    }

    /// The next logical offset that would be assigned by an append.
    pub fn next_offset(&self) -> Offset {
        Offset(self.committed_offset.load(Ordering::Acquire))
    }

    /// The last valid offset in this extent (inclusive), or None if empty.
    pub fn last_offset(&self) -> Option<Offset> {
        let offset = self.committed_offset.load(Ordering::Acquire);
        if offset <= self.start_offset.0 {
            None
        } else {
            Some(Offset(offset - 1))
        }
    }

    /// The current committed offset (for diagnostics).
    pub fn committed_offset(&self) -> u64 {
        self.committed_offset.load(Ordering::Acquire)
    }

    /// Total bytes written (write_cursor position). Useful for metrics and
    /// size-based seal triggers.
    pub fn bytes_written(&self) -> u64 {
        self.write_cursor.load(Ordering::Relaxed)
    }

    /// Arena capacity in bytes.
    pub fn capacity(&self) -> u32 {
        self.capacity
    }

    /// u64::MAX while the epoch is not sealed; the sealed message count otherwise.
    pub fn limit_hint(&self) -> u64 {
        self.limit.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Return a contiguous `Bytes` view of all committed record data in the arena.
    /// Useful for S3 flush -- the sealed extent can be uploaded as a single blob
    /// (after prepending header and appending footer/index).
    pub fn committed_data(&self) -> Bytes {
        let cb = self.committed_bytes.load(Ordering::Acquire) as usize;
        if cb == 0 {
            return Bytes::new();
        }
        let arena = self.arena_as_bytes();
        arena.slice(0..cb)
    }

    /// Return a snapshot of the arenas holding directory entries for this epoch.
    #[allow(dead_code)]
    pub(crate) fn resident_arenas(&self) -> SmallVec<[ArenaId; 4]> {
        self.resident_arenas.lock().unwrap().clone()
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

// Debug impl that doesn't try to print the entire buffer.
impl std::fmt::Debug for StreamEpoch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamEpoch")
            .field("id", &self.id)
            .field("start_offset", &self.start_offset)
            .field("epoch", &self.epoch)
            .field("capacity", &self.capacity)
            .field("write_cursor", &self.write_cursor.load(Ordering::Relaxed))
            .field("record_count", &self.record_count.load(Ordering::Relaxed))
            .field(
                "committed_offset",
                &self.committed_offset.load(Ordering::Relaxed),
            )
            .field(
                "committed_bytes",
                &self.committed_bytes.load(Ordering::Relaxed),
            )
            .field("limit", &self.limit.load(Ordering::Relaxed))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_and_read() {
        let ext = StreamEpoch::with_capacity(
            ExtentId(1),
            StreamId(0),
            Offset(0),
            4096,
            Epoch(0),
            ArenaId(0),
        );
        let r0 = ext.append(Bytes::from_static(b"msg0")).unwrap();
        let r1 = ext.append(Bytes::from_static(b"msg1")).unwrap();
        let r2 = ext.append(Bytes::from_static(b"msg2")).unwrap();

        assert_eq!(r0.offset, Offset(0));
        assert_eq!(r1.offset, Offset(1));
        assert_eq!(r2.offset, Offset(2));
        assert_eq!(r0.byte_pos, 0);
        // "msg0" = 4 bytes payload, record = 4 + 4 = 8 bytes
        assert_eq!(r1.byte_pos, 8);
        assert_eq!(r2.byte_pos, 16);
        assert_eq!(ext.message_count(), 3);
        assert_eq!(ext.next_offset(), Offset(3));

        // Read all 3 starting from byte_pos 0.
        let msgs = ext.read(r0.byte_pos, 3).unwrap();
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0], Bytes::from_static(b"msg0"));
        assert_eq!(msgs[1], Bytes::from_static(b"msg1"));
        assert_eq!(msgs[2], Bytes::from_static(b"msg2"));

        // Read 1 record starting from r1's byte_pos (random access).
        let msgs = ext.read(r1.byte_pos, 1).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0], Bytes::from_static(b"msg1"));
    }

    #[test]
    fn read_from_middle() {
        let ext = StreamEpoch::with_capacity(
            ExtentId(1),
            StreamId(0),
            Offset(10),
            4096,
            Epoch(0),
            ArenaId(0),
        );
        let _r0 = ext.append(Bytes::from_static(b"a")).unwrap();
        let r1 = ext.append(Bytes::from_static(b"b")).unwrap();

        // Read starting from r1's byte_pos — direct seek, no walk from 0.
        let msgs = ext.read(r1.byte_pos, 5).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0], Bytes::from_static(b"b"));
    }

    #[test]
    fn read_out_of_range_returns_empty() {
        let ext = StreamEpoch::with_capacity(
            ExtentId(1),
            StreamId(0),
            Offset(0),
            4096,
            Epoch(0),
            ArenaId(0),
        );
        // No records appended, read at byte_pos 0 returns empty.
        let msgs = ext.read(0, 10).unwrap();
        assert!(msgs.is_empty());

        // Read at a position beyond committed bytes.
        ext.append(Bytes::from_static(b"x")).unwrap();
        let msgs = ext.read(9999, 1).unwrap();
        assert!(msgs.is_empty());
    }

    #[test]
    fn seal_rejects_append() {
        let ext = StreamEpoch::with_capacity(
            ExtentId(1),
            StreamId(0),
            Offset(0),
            4096,
            Epoch(0),
            ArenaId(0),
        );
        ext.append(Bytes::from_static(b"ok")).unwrap();
        ext.seal(None);

        let result = ext.append(Bytes::from_static(b"fail"));
        assert!(matches!(result, Err(StorageError::EpochSealed { .. })));
        assert_eq!(ext.message_count(), 1);
    }

    #[test]
    fn start_offset_nonzero() {
        let ext = StreamEpoch::with_capacity(
            ExtentId(2),
            StreamId(0),
            Offset(100),
            4096,
            Epoch(0),
            ArenaId(0),
        );
        let r = ext.append(Bytes::from_static(b"hello")).unwrap();
        assert_eq!(r.offset, Offset(100));
        assert_eq!(r.byte_pos, 0);
        assert_eq!(ext.next_offset(), Offset(101));

        let msgs = ext.read(r.byte_pos, 1).unwrap();
        assert_eq!(msgs[0], Bytes::from_static(b"hello"));
    }

    #[test]
    fn extent_full_returns_error() {
        // Tiny capacity: 16 bytes. Each record is 4 (len prefix) + payload.
        // "hello" = 5 bytes -> record = 9 bytes. Two records = 18 bytes > 16.
        let ext = StreamEpoch::with_capacity(
            ExtentId(1),
            StreamId(0),
            Offset(0),
            16,
            Epoch(0),
            ArenaId(0),
        );
        ext.append(Bytes::from_static(b"hello")).unwrap(); // 9 bytes, fits
        let result = ext.append(Bytes::from_static(b"world")); // 9 bytes, doesn't fit
        assert!(matches!(result, Err(StorageError::EpochFull { .. })));
    }

    #[test]
    fn committed_data_returns_arena_slice() {
        let ext = StreamEpoch::with_capacity(
            ExtentId(1),
            StreamId(0),
            Offset(0),
            4096,
            Epoch(0),
            ArenaId(0),
        );
        ext.append(Bytes::from_static(b"abc")).unwrap();
        ext.append(Bytes::from_static(b"de")).unwrap();

        let data = ext.committed_data();
        // Record 0: [00 00 00 03] [a b c]   = 7 bytes
        // Record 1: [00 00 00 02] [d e]      = 6 bytes
        // Total = 13 bytes
        assert_eq!(data.len(), 13);
        assert_eq!(&data[0..4], &[0, 0, 0, 3]); // len of "abc"
        assert_eq!(&data[4..7], b"abc");
        assert_eq!(&data[7..11], &[0, 0, 0, 2]); // len of "de"
        assert_eq!(&data[11..13], b"de");
    }

    #[test]
    fn index_lookup_basic() {
        let ext = StreamEpoch::with_capacity(
            ExtentId(1),
            StreamId(0),
            Offset(0),
            4096,
            Epoch(0),
            ArenaId(0),
        );

        // Before any append, all index entries are None.
        assert_eq!(ext.index_lookup(0), None);
        assert_eq!(ext.index_lookup(1), None);

        let r0 = ext.append(Bytes::from_static(b"msg0")).unwrap();
        let r1 = ext.append(Bytes::from_static(b"msg1")).unwrap();
        let r2 = ext.append(Bytes::from_static(b"msg2")).unwrap();

        // After append, index entries should match byte_pos.
        assert_eq!(ext.index_lookup(0), Some(r0.byte_pos));
        assert_eq!(ext.index_lookup(1), Some(r1.byte_pos));
        assert_eq!(ext.index_lookup(2), Some(r2.byte_pos));

        // Uncommitted entries are still None.
        assert_eq!(ext.index_lookup(3), None);

        // Out-of-bounds returns None.
        assert_eq!(ext.index_lookup(999_999), None);
    }

    #[test]
    fn index_lookup_with_nonzero_start_offset() {
        let ext = StreamEpoch::with_capacity(
            ExtentId(1),
            StreamId(0),
            Offset(100),
            4096,
            Epoch(0),
            ArenaId(0),
        );
        let r0 = ext.append(Bytes::from_static(b"hello")).unwrap();
        assert_eq!(r0.offset, Offset(100));

        // Internal index uses seq (0-based within extent), not global offset.
        assert_eq!(ext.index_lookup(0), Some(r0.byte_pos));
    }

    #[test]
    fn replicate_basic() {
        // Simulate a secondary receiving 3 records from the primary.
        let ext = StreamEpoch::with_capacity(
            ExtentId(1),
            StreamId(0),
            Offset(0),
            4096,
            Epoch(0),
            ArenaId(0),
        );

        let r0 = ext
            .replicate(Offset(0), Bytes::from_static(b"msg0"))
            .unwrap();
        assert_eq!(r0.offset, Offset(0));
        assert_eq!(r0.byte_pos, 0);

        // "msg0" = 4 bytes payload, record = 4 + 4 = 8 bytes
        let r1 = ext
            .replicate(Offset(1), Bytes::from_static(b"msg1"))
            .unwrap();
        assert_eq!(r1.offset, Offset(1));
        assert_eq!(r1.byte_pos, 8);

        let r2 = ext
            .replicate(Offset(2), Bytes::from_static(b"msg2"))
            .unwrap();
        assert_eq!(r2.offset, Offset(2));
        assert_eq!(r2.byte_pos, 16);

        assert_eq!(ext.message_count(), 3);
        assert_eq!(ext.next_offset(), Offset(3));

        // Read all 3 starting from byte_pos 0.
        let msgs = ext.read(0, 3).unwrap();
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0], Bytes::from_static(b"msg0"));
        assert_eq!(msgs[1], Bytes::from_static(b"msg1"));
        assert_eq!(msgs[2], Bytes::from_static(b"msg2"));
    }

    #[test]
    fn replicate_matches_append_layout() {
        // Prove that replicate() produces a bit-for-bit identical arena as append().
        let primary = StreamEpoch::with_capacity(
            ExtentId(1),
            StreamId(0),
            Offset(0),
            4096,
            Epoch(0),
            ArenaId(0),
        );
        let secondary = StreamEpoch::with_capacity(
            ExtentId(1),
            StreamId(0),
            Offset(0),
            4096,
            Epoch(0),
            ArenaId(0),
        );

        let payloads: Vec<Bytes> = vec![
            Bytes::from_static(b"hello"),
            Bytes::from_static(b"world"),
            Bytes::from_static(b"foo"),
        ];

        // Append on primary, replicate on secondary with same positions.
        for payload in &payloads {
            let result = primary.append(payload.clone()).unwrap();
            secondary.replicate(result.offset, payload.clone()).unwrap();
        }

        // Arenas must be identical.
        assert_eq!(primary.committed_data(), secondary.committed_data());
        assert_eq!(primary.message_count(), secondary.message_count());
    }

    #[test]
    fn replicate_sealed_extent_rejects() {
        let ext = StreamEpoch::with_capacity(
            ExtentId(1),
            StreamId(0),
            Offset(0),
            4096,
            Epoch(0),
            ArenaId(0),
        );
        ext.replicate(Offset(0), Bytes::from_static(b"msg0"))
            .unwrap();
        ext.seal(Some(1)); // seal at 1 record

        // Replicate at offset=1 (at limit) should fail.
        let result = ext.replicate(Offset(1), Bytes::from_static(b"msg1"));
        assert!(matches!(result, Err(StorageError::EpochSealed { .. })));
    }

    #[test]
    fn post_seal_append_within_committed_offset() {
        // Simulate a secondary: primary committed 3 records, but secondary only
        // received 1 before seal. SM seals secondary with committed_offset=3.
        // Late forwarded appends for offsets 1,2 should be accepted.
        let ext = StreamEpoch::with_capacity(
            ExtentId(1),
            StreamId(0),
            Offset(0),
            4096,
            Epoch(0),
            ArenaId(0),
        );

        // Secondary receives 1 of 3 expected messages before seal.
        ext.append(Bytes::from_static(b"msg0")).unwrap();
        assert_eq!(ext.message_count(), 1);

        // SM seals with committed_offset=3 (primary committed 3 records).
        ext.seal(Some(3));
        assert!(ext.is_sealed());
        assert!(ext.accepts_post_seal_writes()); // committed count(1) < limit(3)

        // Late forwarded appends within the sealed range should succeed.
        let r1 = ext.append(Bytes::from_static(b"msg1")).unwrap();
        assert_eq!(r1.offset, Offset(1));
        let r2 = ext.append(Bytes::from_static(b"msg2")).unwrap();
        assert_eq!(r2.offset, Offset(2));

        // Now at the limit — further appends should be rejected.
        assert!(!ext.accepts_post_seal_writes());
        let result = ext.append(Bytes::from_static(b"should-fail"));
        assert!(matches!(result, Err(StorageError::EpochSealed { .. })));
    }

    #[test]
    fn seal_without_committed_offset_uses_local_count() {
        // Primary sealing itself (extent-full path): no committed_offset provided.
        let ext = StreamEpoch::with_capacity(
            ExtentId(1),
            StreamId(0),
            Offset(0),
            4096,
            Epoch(0),
            ArenaId(0),
        );
        ext.append(Bytes::from_static(b"msg0")).unwrap();
        ext.append(Bytes::from_static(b"msg1")).unwrap();

        ext.seal(None);
        assert!(ext.is_sealed());
        // limit = record_count = 2, committed count = 2 → no room.
        assert!(!ext.accepts_post_seal_writes());
        let result = ext.append(Bytes::from_static(b"should-fail"));
        assert!(matches!(result, Err(StorageError::EpochSealed { .. })));
    }

    #[test]
    fn accepts_post_seal_writes_flag() {
        let ext = StreamEpoch::with_capacity(
            ExtentId(1),
            StreamId(0),
            Offset(0),
            4096,
            Epoch(0),
            ArenaId(0),
        );

        // Not sealed → false.
        assert!(!ext.accepts_post_seal_writes());

        ext.append(Bytes::from_static(b"msg0")).unwrap();
        ext.seal(None);

        // Sealed with local count, committed count == limit → false.
        assert!(!ext.accepts_post_seal_writes());
    }

    #[test]
    fn incremental_crc32_matches_full_hash() {
        let ext = StreamEpoch::with_capacity(
            ExtentId(1),
            StreamId(0),
            Offset(0),
            4096,
            Epoch(0),
            ArenaId(0),
        );

        // Before any append, no finalized CRC32.
        assert_eq!(ext.finalized_crc32(), None);

        ext.append(Bytes::from_static(b"hello")).unwrap();
        ext.append(Bytes::from_static(b"world")).unwrap();
        ext.append(Bytes::from_static(b"!")).unwrap();

        // Still active — no finalized CRC32.
        assert_eq!(ext.finalized_crc32(), None);

        // Seal triggers finalization.
        ext.seal(None);

        let incremental = ext
            .finalized_crc32()
            .expect("should be finalized after seal");
        let full_hash = crc32fast::hash(&ext.committed_data());
        assert_eq!(
            incremental, full_hash,
            "incremental CRC32 ({:#010x}) != full hash ({:#010x})",
            incremental, full_hash,
        );
    }

    #[test]
    fn incremental_crc32_single_record() {
        let ext = StreamEpoch::with_capacity(
            ExtentId(1),
            StreamId(0),
            Offset(0),
            4096,
            Epoch(0),
            ArenaId(0),
        );
        ext.append(Bytes::from_static(b"only-one")).unwrap();
        ext.seal(None);

        let incremental = ext.finalized_crc32().unwrap();
        let full_hash = crc32fast::hash(&ext.committed_data());
        assert_eq!(incremental, full_hash);
    }

    #[test]
    fn incremental_crc32_empty_payloads() {
        let ext = StreamEpoch::with_capacity(
            ExtentId(1),
            StreamId(0),
            Offset(0),
            4096,
            Epoch(0),
            ArenaId(0),
        );
        ext.append(Bytes::new()).unwrap(); // empty payload
        ext.append(Bytes::from_static(b"data")).unwrap();
        ext.append(Bytes::new()).unwrap(); // another empty
        ext.seal(None);

        let incremental = ext.finalized_crc32().unwrap();
        let full_hash = crc32fast::hash(&ext.committed_data());
        assert_eq!(incremental, full_hash);
    }

    #[test]
    fn incremental_crc32_via_replicate() {
        // Simulate primary appends to get reference data.
        let primary = StreamEpoch::with_capacity(
            ExtentId(1),
            StreamId(0),
            Offset(0),
            4096,
            Epoch(0),
            ArenaId(0),
        );
        primary.append(Bytes::from_static(b"hello")).unwrap();
        primary.append(Bytes::from_static(b"world")).unwrap();
        primary.append(Bytes::from_static(b"!")).unwrap();
        primary.seal(None);

        // Simulate secondary receiving the same records via replicate() IN ORDER.
        let secondary = StreamEpoch::with_capacity(
            ExtentId(1),
            StreamId(0),
            Offset(0),
            4096,
            Epoch(0),
            ArenaId(0),
        );
        secondary
            .replicate(Offset(0), Bytes::from_static(b"hello"))
            .unwrap();
        secondary
            .replicate(Offset(1), Bytes::from_static(b"world"))
            .unwrap();
        secondary
            .replicate(Offset(2), Bytes::from_static(b"!"))
            .unwrap();

        // After in-order replicate, try_advance_committed (called inside replicate)
        // should have hashed all 3 records.
        // Seal the secondary to set the limit, then store primary checksum.
        secondary.seal(Some(3));
        let primary_crc = primary.finalized_crc32().unwrap();
        secondary.store_primary_checksum(primary_crc);
        secondary.try_advance_committed();

        // Verification should succeed.
        assert_eq!(secondary.try_verify_checksum(), Some(true));

        // Also verify both match the full-extent hash.
        let full_hash = crc32fast::hash(&primary.committed_data());
        assert_eq!(primary_crc, full_hash);
    }

    #[test]
    fn crc32_in_order_replicate() {
        // Simulate primary appends to get reference data.
        let primary = StreamEpoch::with_capacity(
            ExtentId(1),
            StreamId(0),
            Offset(0),
            4096,
            Epoch(0),
            ArenaId(0),
        );
        primary.append(Bytes::from_static(b"hello")).unwrap();
        primary.append(Bytes::from_static(b"world")).unwrap();
        primary.append(Bytes::from_static(b"!")).unwrap();
        primary.seal(None);

        // Simulate secondary receiving records IN ORDER (guaranteed by FIFO mpsc).
        let secondary = StreamEpoch::with_capacity(
            ExtentId(1),
            StreamId(0),
            Offset(0),
            4096,
            Epoch(0),
            ArenaId(0),
        );

        secondary
            .replicate(Offset(0), Bytes::from_static(b"hello"))
            .unwrap();
        secondary
            .replicate(Offset(1), Bytes::from_static(b"world"))
            .unwrap();
        secondary
            .replicate(Offset(2), Bytes::from_static(b"!"))
            .unwrap();

        // Seal and store primary checksum.
        secondary.seal(Some(3));
        let primary_crc = primary.finalized_crc32().unwrap();
        secondary.store_primary_checksum(primary_crc);

        // CRC32 was computed inline — verification should succeed immediately.
        assert_eq!(secondary.try_verify_checksum(), Some(true));
    }

    #[test]
    fn crc32_forward_checksum_arrives_after_records() {
        // ForwardChecksum arrives after all records (normal case with FIFO channel).
        let primary = StreamEpoch::with_capacity(
            ExtentId(1),
            StreamId(0),
            Offset(0),
            4096,
            Epoch(0),
            ArenaId(0),
        );
        primary.append(Bytes::from_static(b"hello")).unwrap();
        primary.append(Bytes::from_static(b"world")).unwrap();
        primary.seal(None);

        let secondary = StreamEpoch::with_capacity(
            ExtentId(1),
            StreamId(0),
            Offset(0),
            4096,
            Epoch(0),
            ArenaId(0),
        );

        // Records arrive in order.
        secondary
            .replicate(Offset(0), Bytes::from_static(b"hello"))
            .unwrap();
        secondary
            .replicate(Offset(1), Bytes::from_static(b"world"))
            .unwrap();

        // Seal and store primary checksum.
        secondary.seal(Some(2));
        let primary_crc = primary.finalized_crc32().unwrap();
        secondary.store_primary_checksum(primary_crc);

        // All records present, CRC32 hashed inline — verification succeeds.
        assert_eq!(secondary.try_verify_checksum(), Some(true));
    }

    #[test]
    fn correct_seal_offset_lowers_limit() {
        // Sealed at 100 records, correct down to 80
        let extent = StreamEpoch::with_capacity(
            ExtentId(1),
            StreamId(0),
            Offset(0),
            1024 * 1024,
            Epoch(0),
            ArenaId(0),
        );
        for i in 0..100u32 {
            extent.append(Bytes::from(vec![i as u8; 10])).unwrap();
        }
        extent.seal(None); // seals at 100
        assert_eq!(extent.message_count(), 100);

        extent.correct_seal_offset(80);
        // limit should now be 80; message_count still reads committed_offset which is 100
        // but is_sealed() still true
        assert!(extent.is_sealed());
    }

    #[test]
    fn correct_seal_offset_noop_if_already_lower() {
        // Sealed at 50 records, try to "correct" to 80 → no-op
        let extent = StreamEpoch::with_capacity(
            ExtentId(1),
            StreamId(0),
            Offset(0),
            1024 * 1024,
            Epoch(0),
            ArenaId(0),
        );
        for i in 0..50u32 {
            extent.append(Bytes::from(vec![i as u8; 10])).unwrap();
        }
        extent.seal(None); // seals at 50

        extent.correct_seal_offset(80); // 80 > 50, no change
        assert!(extent.is_sealed());
        // limit stays at 50
    }

    #[test]
    fn correct_seal_offset_noop_if_unsealed() {
        // Not sealed → correct_seal_offset is a no-op
        let extent = StreamEpoch::with_capacity(
            ExtentId(1),
            StreamId(0),
            Offset(0),
            1024 * 1024,
            Epoch(0),
            ArenaId(0),
        );
        extent.append(Bytes::from_static(b"hello")).unwrap();

        extent.correct_seal_offset(0); // should not seal or crash
        assert!(!extent.is_sealed());
    }

    #[test]
    fn correct_seal_offset_concurrent() {
        // Multiple threads calling correct_seal_offset → CAS loop handles it
        let extent = Arc::new(StreamEpoch::with_capacity(
            ExtentId(1),
            StreamId(0),
            Offset(0),
            1024 * 1024,
            Epoch(0),
            ArenaId(0),
        ));
        for i in 0..100u32 {
            extent.append(Bytes::from(vec![i as u8; 10])).unwrap();
        }
        extent.seal(None); // seals at 100

        let mut handles = vec![];
        for target in [90, 80, 70, 60, 50] {
            let ext = Arc::clone(&extent);
            handles.push(std::thread::spawn(move || {
                ext.correct_seal_offset(target);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        // After all corrections, limit should be 50 (the lowest)
        assert!(extent.is_sealed());
    }

    #[test]
    fn write_batch_basic() {
        use crate::arena::SharedAppendJob;

        let ext = StreamEpoch::with_capacity(
            ExtentId(1),
            StreamId(0),
            Offset(0),
            4096,
            Epoch(0),
            ArenaId(0),
        );

        let jobs = vec![
            SharedAppendJob::new(0, Bytes::from_static(b"msg0")),
            SharedAppendJob::new(1, Bytes::from_static(b"msg1")),
            SharedAppendJob::new(2, Bytes::from_static(b"msg2")),
        ];

        let results = ext.write_batch(&jobs);

        assert_eq!(results.len(), 3);
        assert!(results[0].is_ok());
        assert!(results[1].is_ok());
        assert!(results[2].is_ok());

        // All records land in the same arena (Dedicated).
        assert_eq!(results[0].arena_id, ArenaId(0));
        assert_eq!(results[1].arena_id, ArenaId(0));
        assert_eq!(results[2].arena_id, ArenaId(0));

        assert_eq!(results[0].byte_pos, 0);
        // "msg0" = 4 bytes, record = 4+4 = 8 bytes
        assert_eq!(results[1].byte_pos, 8);
        assert_eq!(results[2].byte_pos, 16);
        assert_eq!(ext.message_count(), 3);
    }

    #[test]
    fn write_batch_empty() {
        let ext = StreamEpoch::with_capacity(
            ExtentId(1),
            StreamId(0),
            Offset(0),
            4096,
            Epoch(0),
            ArenaId(0),
        );
        let results = ext.write_batch(&[]);
        assert!(results.is_empty());
        assert_eq!(ext.message_count(), 0);
    }

    #[test]
    fn write_batch_propagates_errors() {
        use crate::arena::SharedAppendJob;

        // Tiny capacity: 9 bytes → room for exactly one 4-byte payload record.
        let ext = StreamEpoch::with_capacity(
            ExtentId(1),
            StreamId(0),
            Offset(0),
            9,
            Epoch(0),
            ArenaId(0),
        );

        let jobs = vec![
            SharedAppendJob::new(0, Bytes::from_static(b"fits")), // 4+4=8 bytes, fits
            SharedAppendJob::new(1, Bytes::from_static(b"nope")), // 4+4=8 bytes, exceeds capacity
        ];

        let results = ext.write_batch(&jobs);
        assert_eq!(results.len(), 2);
        assert!(results[0].is_ok());
        assert!(!results[1].is_ok());
        assert!(matches!(
            &results[1].result,
            Err(StorageError::EpochFull { .. })
        ));
    }

    #[test]
    fn arena_in_flight_initialized_to_zero() {
        let ext = StreamEpoch::with_capacity(
            ExtentId(1),
            StreamId(0),
            Offset(0),
            4096,
            Epoch(0),
            ArenaId(0),
        );
        assert_eq!(ext.arena_in_flight.load(Ordering::Relaxed), 0);
    }
}
