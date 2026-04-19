use std::alloc::{Layout, alloc, dealloc};
use std::cell::UnsafeCell;
use std::ops::Deref;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};

use bytes::Bytes;
use common::errors::StorageError;
use common::errors::{ExtentFullSnafu, ExtentSealedSnafu, InternalSnafu};
use common::types::{Epoch, ExtentId, ExtentState, Offset};

/// Sentinel for unwritten index entries.
/// We use 0 so the index can be allocated with `alloc_zeroed` (OS provides
/// pre-zeroed pages via MAP_ANONYMOUS at near-zero cost), avoiding a 13M+
/// iteration init loop that caused ~80ms stalls on the hot append path.
/// Actual byte positions are stored as `byte_pos + 1` to distinguish from
/// the sentinel (since byte_pos=0 is valid for the first record).
const INDEX_UNSET: u32 = 0;

/// Sentinel value for `limit`: extent is not sealed.
const LIMIT_OPEN: u64 = u64::MAX;
const MIN_RECORD_SIZE: u32 = 5;

/// Forward-flags bitmap: checked inline during `send_forward()`
/// to guarantee ordering relative to Forward frames.
pub const FLAG_INIT_FORWARD: u8 = 0x01;

/// ForwardChecksum has been received from primary (secondary side).
/// Used by `try_verify_checksum()` to know when to compare.
const FLAG_CHECKSUM_RECEIVED: u8 = 0x02;

/// Extent has been flushed to S3 and is eligible for memory eviction.
/// Set by Primary locally after upload, and by Secondaries on ForwardFlushed.
pub const FLAG_FLUSHED: u8 = 0x04;

/// Owns the raw heap allocation for an extent's arena buffer.
/// Wrapped in `Arc` so that `Bytes` slices keep the buffer alive
/// even after the `Extent` is dropped.
struct ArenaBuffer {
    ptr: NonNull<u8>,
    capacity: u32,
    layout: Layout,
}

// SAFETY: The raw allocation is exclusively managed by ArenaBuffer via Arc.
// No aliased mutable access is possible once shared.
unsafe impl Send for ArenaBuffer {}
unsafe impl Sync for ArenaBuffer {}

impl ArenaBuffer {
    /// Resize the arena buffer to a new capacity via `realloc`.
    ///
    /// # Safety
    /// Must only be called on buffers with no outstanding `Bytes` references
    /// (i.e., `Arc::strong_count == 1`). The caller must update any derived
    /// pointers (`buf`) after this call since `realloc` may move the allocation.
    fn resize(&mut self, new_capacity: u32) {
        let new_layout = Layout::from_size_align(new_capacity as usize, 8).expect("invalid layout");
        // SAFETY: ptr and layout were produced by alloc() in with_capacity().
        // new_capacity > 0 (enforced by caller).
        let new_ptr =
            unsafe { std::alloc::realloc(self.ptr.as_ptr(), self.layout, new_capacity as usize) };
        if new_ptr.is_null() {
            std::alloc::handle_alloc_error(new_layout);
        }
        self.ptr = NonNull::new(new_ptr).unwrap();
        self.capacity = new_capacity;
        self.layout = new_layout;
    }
}

impl Drop for ArenaBuffer {
    fn drop(&mut self) {
        // SAFETY: ptr and layout were produced by alloc() in ArenaBuffer::new().
        unsafe {
            dealloc(self.ptr.as_ptr(), self.layout);
        }
    }
}

/// A reference-counted slice into an `ArenaBuffer`.
/// Implements `Deref<Target=[u8]>` so it can be passed to `Bytes::from_owner()`.
struct OwnedArenaSlice {
    _arena: Arc<ArenaBuffer>,
    ptr: *const u8,
    len: u32,
}

// SAFETY: The underlying memory is owned by Arc<ArenaBuffer> which is Send+Sync.
// The ptr/len describe an immutable view into that allocation.
unsafe impl Send for OwnedArenaSlice {}
unsafe impl Sync for OwnedArenaSlice {}

impl Deref for OwnedArenaSlice {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        // SAFETY: ptr is valid for len bytes as long as _arena is alive,
        // and _arena is kept alive by the Arc clone in this struct.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len as usize) }
    }
}

impl AsRef<[u8]> for OwnedArenaSlice {
    fn as_ref(&self) -> &[u8] {
        self.deref()
    }
}

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

/// A lock-free extent backed by a pre-allocated contiguous memory arena.
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
pub struct Extent {
    pub id: ExtentId,

    pub start_offset: Offset,

    /// The epoch under which this extent was created (informational).
    /// Used by `report_extents` to filter extents by epoch during SM recovery.
    pub epoch: Epoch,

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

    /// Internal index mapping sequence number → byte position (compressed u32).
    /// Entry i holds the byte_pos for the i-th record appended to this extent.
    /// Capacity = extent_capacity / MIN_RECORD_SIZE.
    index: Box<[AtomicU32]>,

    /// Bitmap of extent lifecycle flags (AtomicU8):
    /// - `FLAG_INIT_FORWARD` (0x01): prepend ForwardInitExtent before first Forward
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
}

// SAFETY: The raw write pointer `buf` is derived from Arc<ArenaBuffer> and only
// used for non-overlapping writes mediated by atomic cursors. The ArenaBuffer
// itself is Send+Sync, and all concurrent access is bounded by atomic cursors.
unsafe impl Send for Extent {}
unsafe impl Sync for Extent {}

impl Extent {
    /// Create a new active extent with the specified capacity in bytes.
    pub fn with_capacity(id: ExtentId, start_offset: Offset, capacity: u32, epoch: Epoch) -> Self {
        let layout = Layout::from_size_align(capacity as usize, 8).expect("invalid layout");
        // SAFETY: layout is valid, nonzero size.
        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }

        let arena = Arc::new(ArenaBuffer {
            ptr: NonNull::new(ptr).unwrap(),
            capacity,
            layout,
        });
        let buf = arena.ptr.as_ptr();

        // Allocate the index with alloc_zeroed: the OS provides pre-zeroed pages
        // (MAP_ANONYMOUS) at near-zero cost, avoiding a 13M+ iteration init loop
        // that caused ~80ms stalls. INDEX_UNSET == 0, so zeroed memory is correct.
        let index_capacity = (capacity / MIN_RECORD_SIZE) as usize;
        let index = {
            let index_layout = Layout::from_size_align(
                index_capacity * std::mem::size_of::<AtomicU32>(),
                std::mem::align_of::<AtomicU32>(),
            )
            .expect("invalid index layout");
            // SAFETY: layout is valid, nonzero size. alloc_zeroed returns zeroed memory.
            // AtomicU32 has the same layout as u32, and 0u32 == INDEX_UNSET.
            let index_ptr = unsafe { std::alloc::alloc_zeroed(index_layout) };
            if index_ptr.is_null() {
                std::alloc::handle_alloc_error(index_layout);
            }
            // SAFETY: alloc_zeroed returned a valid allocation of index_capacity * 4 bytes,
            // all zeroed. AtomicU32 is repr-compatible with u32. We reconstruct the Vec
            // from the raw parts so it can be converted to Box<[AtomicU32]>.
            unsafe {
                Vec::from_raw_parts(index_ptr as *mut AtomicU32, index_capacity, index_capacity)
            }
            .into_boxed_slice()
        };

        Self {
            id,
            start_offset,
            epoch,
            arena,
            buf,
            capacity,
            write_cursor: AtomicU64::new(0),
            record_count: AtomicU64::new(0),
            committed_offset: AtomicU64::new(start_offset.0),
            committed_bytes: AtomicU64::new(0),
            limit: AtomicU64::new(LIMIT_OPEN),
            index,
            flags: AtomicU8::new(FLAG_INIT_FORWARD),
            hasher: UnsafeCell::new(crc32fast::Hasher::new()),
            finalized_crc32: AtomicU32::new(0),
        }
    }

    /// Atomically check and clear the `FLAG_INIT_FORWARD` bit.
    /// Returns `true` if the flag was set (i.e., caller should prepend ForwardInitExtent).
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

    /// Whether this extent's arena buffer has no outstanding reader references
    /// and can be safely recycled. Returns `false` if any `Bytes` slices from
    /// `read()` or `committed_data()` are still alive.
    pub fn can_recycle(&self) -> bool {
        Arc::strong_count(&self.arena) == 1
    }

    /// Reset this extent for reuse with a new identity. O(1) cost — only
    /// resets atomics and the CRC32 hasher; arena and index memory are reused
    /// as-is (stale data is overwritten in order before it could be read).
    ///
    /// Requires `&mut self` — the extent must not be in the active extent list.
    pub fn reset(&mut self, id: ExtentId, start_offset: Offset, epoch: Epoch) {
        self.id = id;
        self.start_offset = start_offset;
        self.epoch = epoch;
        self.write_cursor.store(0, Ordering::Relaxed);
        self.record_count.store(0, Ordering::Relaxed);
        self.committed_offset
            .store(start_offset.0, Ordering::Relaxed);
        self.committed_bytes.store(0, Ordering::Relaxed);
        self.limit.store(LIMIT_OPEN, Ordering::Relaxed);
        self.flags.store(FLAG_INIT_FORWARD, Ordering::Relaxed);
        self.finalized_crc32.store(0, Ordering::Relaxed);
        // SAFETY: exclusive access via &mut self — no concurrent readers/writers.
        unsafe {
            *self.hasher.get() = crc32fast::Hasher::new();
        }
    }

    /// Resize this extent's arena and index for a new capacity.
    ///
    /// Uses `realloc` for the arena (may extend in-place) and reallocates
    /// the index array. Must only be called on pool extents (not in the active
    /// extent list, no outstanding `Bytes` references). Requires `&mut self`.
    ///
    /// After resize, the extent must be `reset()` before use — stale data
    /// from the old capacity is not cleared (overwritten on use).
    pub fn resize(&mut self, new_capacity: u32) {
        debug_assert!(
            Arc::strong_count(&self.arena) == 1,
            "cannot resize arena with outstanding references"
        );
        // Resize the arena buffer via realloc.
        Arc::get_mut(&mut self.arena)
            .expect("arena has outstanding references")
            .resize(new_capacity);
        self.buf = self.arena.ptr.as_ptr();
        self.capacity = new_capacity;

        // Reallocate the index array for the new capacity.
        let index_capacity = (new_capacity / MIN_RECORD_SIZE) as usize;
        let index_layout = Layout::from_size_align(
            index_capacity * std::mem::size_of::<AtomicU32>(),
            std::mem::align_of::<AtomicU32>(),
        )
        .expect("invalid index layout");
        let index_ptr = unsafe { std::alloc::alloc_zeroed(index_layout) };
        if index_ptr.is_null() {
            std::alloc::handle_alloc_error(index_layout);
        }
        self.index = unsafe {
            Vec::from_raw_parts(index_ptr as *mut AtomicU32, index_capacity, index_capacity)
        }
        .into_boxed_slice();
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
                return Err(ExtentSealedSnafu { extent_id: self.id }.build());
            }
        }

        let payload_len = payload.len();
        // Record layout: [len: 4 bytes][payload: payload_len bytes]
        let record_len = 4 + payload_len;

        // 1. Reserve byte slot (plain load + store, single writer).
        let byte_pos = self.write_cursor.load(Ordering::Relaxed);
        if byte_pos + record_len as u64 > self.capacity as u64 {
            return Err(ExtentFullSnafu { extent_id: self.id }.build());
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

    /// Replicate a record at the exact position assigned by the primary.
    ///
    /// This method is used by secondaries to write records at the same
    /// `byte_pos` and logical offset as the primary, ensuring bit-for-bit
    /// identical arena layouts across replicas.
    ///
    /// Forward frames arrive in strict offset order (guaranteed by the
    /// per-address FIFO mpsc channel), so CRC32 is computed inline and
    /// committed state is advanced directly — matching `append_inner`
    /// semantics on the primary.
    ///
    /// Returns the logical offset on success.
    pub fn replicate(
        &self,
        offset: Offset,
        byte_pos: u64,
        payload: Bytes,
    ) -> Result<AppendResult, StorageError> {
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
            return Err(ExtentSealedSnafu { extent_id: self.id }.build());
        }

        let payload_len = payload.len();
        let record_len = 4 + payload_len;

        // Check capacity.
        if byte_pos + record_len as u64 > self.capacity as u64 {
            return Err(ExtentFullSnafu { extent_id: self.id }.build());
        }

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
        let idx = seq as usize;
        if idx < self.index.len() {
            self.index[idx].store(byte_pos as u32 + 1, Ordering::Release);
        }
    }

    /// Lookup byte_pos from the internal index.
    ///
    /// Returns `None` if `seq` is out of bounds or the entry has not been
    /// committed yet (still holds the sentinel value 0).
    /// Decodes the stored `byte_pos + 1` encoding back to the real byte_pos.
    pub fn index_lookup(&self, seq: u64) -> Option<u64> {
        let idx = seq as usize;
        if idx >= self.index.len() {
            return None;
        }
        let val = self.index[idx].load(Ordering::Acquire);
        if val == INDEX_UNSET {
            None
        } else {
            Some((val - 1) as u64)
        }
    }

    /// Create a `Bytes` view of the entire arena buffer.
    ///
    /// The returned `Bytes` holds an `Arc` clone of the arena buffer,
    /// so the memory stays alive as long as any derived slice is held by a reader.
    fn arena_as_bytes(&self) -> Bytes {
        let arena = Arc::clone(&self.arena);
        let ptr = arena.ptr.as_ptr() as *const u8;
        let len = arena.capacity;
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
            match self.limit.compare_exchange(
                current,
                count,
                Ordering::Release,
                Ordering::Acquire,
            ) {
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
            let idx = seq as usize;
            if idx >= self.index.len() {
                break;
            }
            let val = self.index[idx].load(Ordering::Acquire);
            if val == INDEX_UNSET {
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
    pub fn state(&self) -> ExtentState {
        if self.is_sealed() {
            ExtentState::Sealed
        } else {
            ExtentState::Active
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
}

// Debug impl that doesn't try to print the entire buffer.
impl std::fmt::Debug for Extent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Extent")
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
        let ext = Extent::with_capacity(ExtentId(1), Offset(0), 4096, Epoch(0));
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
        let ext = Extent::with_capacity(ExtentId(1), Offset(10), 4096, Epoch(0));
        let _r0 = ext.append(Bytes::from_static(b"a")).unwrap();
        let r1 = ext.append(Bytes::from_static(b"b")).unwrap();

        // Read starting from r1's byte_pos — direct seek, no walk from 0.
        let msgs = ext.read(r1.byte_pos, 5).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0], Bytes::from_static(b"b"));
    }

    #[test]
    fn read_out_of_range_returns_empty() {
        let ext = Extent::with_capacity(ExtentId(1), Offset(0), 4096, Epoch(0));
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
        let ext = Extent::with_capacity(ExtentId(1), Offset(0), 4096, Epoch(0));
        ext.append(Bytes::from_static(b"ok")).unwrap();
        ext.seal(None);

        let result = ext.append(Bytes::from_static(b"fail"));
        assert!(matches!(result, Err(StorageError::ExtentSealed { .. })));
        assert_eq!(ext.message_count(), 1);
    }

    #[test]
    fn start_offset_nonzero() {
        let ext = Extent::with_capacity(ExtentId(2), Offset(100), 4096, Epoch(0));
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
        let ext = Extent::with_capacity(ExtentId(1), Offset(0), 16, Epoch(0));
        ext.append(Bytes::from_static(b"hello")).unwrap(); // 9 bytes, fits
        let result = ext.append(Bytes::from_static(b"world")); // 9 bytes, doesn't fit
        assert!(matches!(result, Err(StorageError::ExtentFull { .. })));
    }

    #[test]
    fn committed_data_returns_arena_slice() {
        let ext = Extent::with_capacity(ExtentId(1), Offset(0), 4096, Epoch(0));
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
        let ext = Extent::with_capacity(ExtentId(1), Offset(0), 4096, Epoch(0));

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
        let ext = Extent::with_capacity(ExtentId(1), Offset(100), 4096, Epoch(0));
        let r0 = ext.append(Bytes::from_static(b"hello")).unwrap();
        assert_eq!(r0.offset, Offset(100));

        // Internal index uses seq (0-based within extent), not global offset.
        assert_eq!(ext.index_lookup(0), Some(r0.byte_pos));
    }

    #[test]
    fn replicate_basic() {
        // Simulate a secondary receiving 3 records from the primary.
        let ext = Extent::with_capacity(ExtentId(1), Offset(0), 4096, Epoch(0));

        let r0 = ext
            .replicate(Offset(0), 0, Bytes::from_static(b"msg0"))
            .unwrap();
        assert_eq!(r0.offset, Offset(0));
        assert_eq!(r0.byte_pos, 0);

        // "msg0" = 4 bytes payload, record = 4 + 4 = 8 bytes
        let r1 = ext
            .replicate(Offset(1), 8, Bytes::from_static(b"msg1"))
            .unwrap();
        assert_eq!(r1.offset, Offset(1));
        assert_eq!(r1.byte_pos, 8);

        let r2 = ext
            .replicate(Offset(2), 16, Bytes::from_static(b"msg2"))
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
        let primary = Extent::with_capacity(ExtentId(1), Offset(0), 4096, Epoch(0));
        let secondary = Extent::with_capacity(ExtentId(1), Offset(0), 4096, Epoch(0));

        let payloads: Vec<Bytes> = vec![
            Bytes::from_static(b"hello"),
            Bytes::from_static(b"world"),
            Bytes::from_static(b"foo"),
        ];

        // Append on primary, replicate on secondary with same positions.
        for payload in &payloads {
            let result = primary.append(payload.clone()).unwrap();
            secondary
                .replicate(result.offset, result.byte_pos, payload.clone())
                .unwrap();
        }

        // Arenas must be identical.
        assert_eq!(primary.committed_data(), secondary.committed_data());
        assert_eq!(primary.message_count(), secondary.message_count());
    }

    #[test]
    fn replicate_sealed_extent_rejects() {
        let ext = Extent::with_capacity(ExtentId(1), Offset(0), 4096, Epoch(0));
        ext.replicate(Offset(0), 0, Bytes::from_static(b"msg0"))
            .unwrap();
        ext.seal(Some(1)); // seal at 1 record

        // Replicate at offset=1 (at limit) should fail.
        let result = ext.replicate(Offset(1), 8, Bytes::from_static(b"msg1"));
        assert!(matches!(result, Err(StorageError::ExtentSealed { .. })));
    }

    #[test]
    fn post_seal_append_within_committed_offset() {
        // Simulate a secondary: primary committed 3 records, but secondary only
        // received 1 before seal. SM seals secondary with committed_offset=3.
        // Late forwarded appends for offsets 1,2 should be accepted.
        let ext = Extent::with_capacity(ExtentId(1), Offset(0), 4096, Epoch(0));

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
        assert!(matches!(result, Err(StorageError::ExtentSealed { .. })));
    }

    #[test]
    fn seal_without_committed_offset_uses_local_count() {
        // Primary sealing itself (extent-full path): no committed_offset provided.
        let ext = Extent::with_capacity(ExtentId(1), Offset(0), 4096, Epoch(0));
        ext.append(Bytes::from_static(b"msg0")).unwrap();
        ext.append(Bytes::from_static(b"msg1")).unwrap();

        ext.seal(None);
        assert!(ext.is_sealed());
        // limit = record_count = 2, committed count = 2 → no room.
        assert!(!ext.accepts_post_seal_writes());
        let result = ext.append(Bytes::from_static(b"should-fail"));
        assert!(matches!(result, Err(StorageError::ExtentSealed { .. })));
    }

    #[test]
    fn accepts_post_seal_writes_flag() {
        let ext = Extent::with_capacity(ExtentId(1), Offset(0), 4096, Epoch(0));

        // Not sealed → false.
        assert!(!ext.accepts_post_seal_writes());

        ext.append(Bytes::from_static(b"msg0")).unwrap();
        ext.seal(None);

        // Sealed with local count, committed count == limit → false.
        assert!(!ext.accepts_post_seal_writes());
    }

    #[test]
    fn incremental_crc32_matches_full_hash() {
        let ext = Extent::with_capacity(ExtentId(1), Offset(0), 4096, Epoch(0));

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
        let ext = Extent::with_capacity(ExtentId(1), Offset(0), 4096, Epoch(0));
        ext.append(Bytes::from_static(b"only-one")).unwrap();
        ext.seal(None);

        let incremental = ext.finalized_crc32().unwrap();
        let full_hash = crc32fast::hash(&ext.committed_data());
        assert_eq!(incremental, full_hash);
    }

    #[test]
    fn incremental_crc32_empty_payloads() {
        let ext = Extent::with_capacity(ExtentId(1), Offset(0), 4096, Epoch(0));
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
        let primary = Extent::with_capacity(ExtentId(1), Offset(0), 4096, Epoch(0));
        let r0 = primary.append(Bytes::from_static(b"hello")).unwrap();
        let r1 = primary.append(Bytes::from_static(b"world")).unwrap();
        let r2 = primary.append(Bytes::from_static(b"!")).unwrap();
        primary.seal(None);

        // Simulate secondary receiving the same records via replicate() IN ORDER.
        let secondary = Extent::with_capacity(ExtentId(1), Offset(0), 4096, Epoch(0));
        secondary
            .replicate(Offset(0), r0.byte_pos, Bytes::from_static(b"hello"))
            .unwrap();
        secondary
            .replicate(Offset(1), r1.byte_pos, Bytes::from_static(b"world"))
            .unwrap();
        secondary
            .replicate(Offset(2), r2.byte_pos, Bytes::from_static(b"!"))
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
        let primary = Extent::with_capacity(ExtentId(1), Offset(0), 4096, Epoch(0));
        let r0 = primary.append(Bytes::from_static(b"hello")).unwrap();
        let r1 = primary.append(Bytes::from_static(b"world")).unwrap();
        let r2 = primary.append(Bytes::from_static(b"!")).unwrap();
        primary.seal(None);

        // Simulate secondary receiving records IN ORDER (guaranteed by FIFO mpsc).
        let secondary = Extent::with_capacity(ExtentId(1), Offset(0), 4096, Epoch(0));

        secondary
            .replicate(Offset(0), r0.byte_pos, Bytes::from_static(b"hello"))
            .unwrap();
        secondary
            .replicate(Offset(1), r1.byte_pos, Bytes::from_static(b"world"))
            .unwrap();
        secondary
            .replicate(Offset(2), r2.byte_pos, Bytes::from_static(b"!"))
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
        let primary = Extent::with_capacity(ExtentId(1), Offset(0), 4096, Epoch(0));
        let r0 = primary.append(Bytes::from_static(b"hello")).unwrap();
        let r1 = primary.append(Bytes::from_static(b"world")).unwrap();
        primary.seal(None);

        let secondary = Extent::with_capacity(ExtentId(1), Offset(0), 4096, Epoch(0));

        // Records arrive in order.
        secondary
            .replicate(Offset(0), r0.byte_pos, Bytes::from_static(b"hello"))
            .unwrap();
        secondary
            .replicate(Offset(1), r1.byte_pos, Bytes::from_static(b"world"))
            .unwrap();

        // Seal and store primary checksum.
        secondary.seal(Some(2));
        let primary_crc = primary.finalized_crc32().unwrap();
        secondary.store_primary_checksum(primary_crc);

        // All records present, CRC32 hashed inline — verification succeeds.
        assert_eq!(secondary.try_verify_checksum(), Some(true));
    }

    #[test]
    fn resize_basic() {
        // Create a small extent and resize it larger.
        let mut ext = Extent::with_capacity(ExtentId(1), Offset(0), 1024, Epoch(0));
        assert_eq!(ext.capacity(), 1024);

        // Resize to 4096.
        ext.resize(4096);
        assert_eq!(ext.capacity(), 4096);
        // Index should be resized too.
        let expected_index_len = (4096 / MIN_RECORD_SIZE) as usize;
        assert_eq!(ext.index.len(), expected_index_len);

        // After resize + reset, the extent should work normally.
        ext.reset(ExtentId(2), Offset(100), Epoch(1));
        let r0 = ext.append(Bytes::from_static(b"after-resize")).unwrap();
        assert_eq!(r0.offset, Offset(100));
        let msgs = ext.read(r0.byte_pos, 1).unwrap();
        assert_eq!(msgs[0], Bytes::from_static(b"after-resize"));
    }

    #[test]
    fn resize_shrink() {
        // Create a larger extent and resize it smaller.
        let mut ext = Extent::with_capacity(ExtentId(1), Offset(0), 8192, Epoch(0));
        assert_eq!(ext.capacity(), 8192);

        ext.resize(2048);
        assert_eq!(ext.capacity(), 2048);
        let expected_index_len = (2048 / MIN_RECORD_SIZE) as usize;
        assert_eq!(ext.index.len(), expected_index_len);

        // After resize + reset, the extent should work with reduced capacity.
        ext.reset(ExtentId(3), Offset(0), Epoch(0));
        let r = ext.append(Bytes::from_static(b"shrunk")).unwrap();
        assert_eq!(r.offset, Offset(0));
        let msgs = ext.read(r.byte_pos, 1).unwrap();
        assert_eq!(msgs[0], Bytes::from_static(b"shrunk"));
    }
}
