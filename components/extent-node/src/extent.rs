use std::alloc::{Layout, alloc, dealloc};
use std::ops::Deref;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use bytes::Bytes;
use common::errors::StorageError;
use common::types::{ExtentId, ExtentState, Offset};

/// Default arena capacity: 64 MB.
pub const DEFAULT_ARENA_CAPACITY: usize = 64 * 1024 * 1024;

/// Sentinel for unwritten index entries.
const INDEX_UNSET: u32 = u32::MAX;

/// Sentinel value for `limit`: extent is not sealed.
const LIMIT_OPEN: u64 = u64::MAX;
const MIN_RECORD_SIZE: usize = 5;

/// Owns the raw heap allocation for an extent's arena buffer.
/// Wrapped in `Arc` so that `Bytes` slices keep the buffer alive
/// even after the `Extent` is dropped.
struct ArenaBuffer {
    ptr: NonNull<u8>,
    capacity: usize,
    layout: Layout,
}

// SAFETY: The raw allocation is exclusively managed by ArenaBuffer via Arc.
// No aliased mutable access is possible once shared.
unsafe impl Send for ArenaBuffer {}
unsafe impl Sync for ArenaBuffer {}

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
    len: usize,
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
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
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
/// # Concurrency Model
///
/// Multiple threads can append concurrently without any mutex:
///
/// 1. **Slot reservation**: `write_cursor` is advanced via `fetch_add` to atomically
///    claim a `(byte_offset, length)` region. Each writer owns its reserved region
///    exclusively -- no overlap is possible.
///
/// 2. **Sequence assignment**: `record_count` is advanced via `fetch_add` to atomically
///    assign a monotonic logical sequence number to each record.
///
/// 3. **Payload copy**: The writer copies its payload into the reserved region.
///    This is safe because regions do not overlap.
///
/// 4. **Commit advancement** (Approach A -- spin-wait): After copying, the writer
///    spins on `committed_seq` until it equals their sequence number, then CAS-advances
///    it to seq+1. This ensures `committed_seq` advances in-order. The spin is brief
///    because it only waits for the immediately preceding writer to finish its memcpy
///    (nanoseconds for typical messages <1 KB). This is the same pattern used by
///    the Linux kernel's io_uring SQ and the LMAX Disruptor.
///
/// 5. **Reads**: Readers observe `committed_bytes` (Acquire load) as the visibility
///    boundary. Records in the arena are self-contained (`[len: u32 BE][payload]`),
///    so readers walk forward from a given byte position by reading each record's
///    length prefix. No separate index structure is needed.
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
///   committed_seq   = number of records fully written (logical offset cursor)
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

    /// Reference-counted arena buffer. Shared with any outstanding `Bytes`
    /// slices, so the memory is not freed until all readers are done.
    arena: Arc<ArenaBuffer>,
    /// Derived write pointer into the arena (for append writes).
    buf: *mut u8,
    /// Total capacity of the arena in bytes.
    capacity: usize,

    /// Byte position of the next free slot. Writers `fetch_add` to reserve space.
    write_cursor: AtomicU64,

    /// Number of records appended. Writers `fetch_add` to assign sequence numbers.
    record_count: AtomicU64,

    /// Committed sequence: all records with seq < committed_seq have been fully
    /// written and are safe to read. Advanced in-order via spin-wait CAS.
    committed_seq: AtomicU64,

    /// Committed byte position: the byte offset up to which all records are fully
    /// written. Readers use this as the upper bound when walking the arena.
    /// Advanced in-order alongside `committed_seq`.
    committed_bytes: AtomicU64,

    /// Message limit for this extent.
    ///
    /// - `LIMIT_OPEN` (`u64::MAX`): extent is **not sealed**, appends proceed normally.
    /// - Any other value `N`: extent is **sealed** at message count `N`.
    ///   Appends with `record_count < N` are still accepted (late forwarded writes
    ///   within the primary's committed range); appends at or beyond `N` are rejected
    ///   with `ExtentSealed`.
    limit: AtomicU64,

    /// Number of in-flight appenders: threads that have passed the `limit` check
    /// but have not yet finished committing. Used by `seal()` to wait for all
    /// in-flight appenders to complete before reading the final `record_count`.
    in_flight: AtomicU64,

    /// Internal index mapping sequence number → byte position (compressed u32).
    /// Entry i holds the byte_pos for the i-th record appended to this extent.
    /// Capacity = arena_capacity / MIN_RECORD_SIZE.
    index: Box<[AtomicU32]>,
}

// SAFETY: The raw write pointer `buf` is derived from Arc<ArenaBuffer> and only
// used for non-overlapping writes mediated by atomic cursors. The ArenaBuffer
// itself is Send+Sync, and all concurrent access is bounded by atomic cursors.
unsafe impl Send for Extent {}
unsafe impl Sync for Extent {}

impl Extent {
    /// Create a new active extent with default capacity (64 MB).
    pub fn new(id: ExtentId, start_offset: Offset) -> Self {
        Self::with_capacity(id, start_offset, DEFAULT_ARENA_CAPACITY)
    }

    /// Create a new active extent with the specified capacity in bytes.
    pub fn with_capacity(id: ExtentId, start_offset: Offset, capacity: usize) -> Self {
        let layout = Layout::from_size_align(capacity, 8).expect("invalid layout");
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

        let index_capacity = capacity / MIN_RECORD_SIZE;
        let mut index_entries = Vec::with_capacity(index_capacity);
        for _ in 0..index_capacity {
            index_entries.push(AtomicU32::new(INDEX_UNSET));
        }
        let index = index_entries.into_boxed_slice();

        Self {
            id,
            start_offset,
            arena,
            buf,
            capacity,
            write_cursor: AtomicU64::new(0),
            record_count: AtomicU64::new(0),
            committed_seq: AtomicU64::new(0),
            committed_bytes: AtomicU64::new(0),
            limit: AtomicU64::new(LIMIT_OPEN),
            in_flight: AtomicU64::new(0),
            index,
        }
    }

    /// Append a message. Returns the assigned logical offset and the byte
    /// position within the arena.
    ///
    /// This method is **lock-free**: multiple threads may call it concurrently.
    /// Slot reservation and sequence assignment use `fetch_add`. The payload copy
    /// writes into a non-overlapping region. Commit advancement uses a brief
    /// spin-wait CAS on `committed_seq`.
    ///
    /// The returned `AppendResult.byte_pos` allows the caller to build an
    /// external offset-to-position index for O(1) random reads.
    pub fn append(&self, payload: Bytes) -> Result<AppendResult, StorageError> {
        // Increment in_flight BEFORE loading limit. This ensures seal() can wait
        // for all appenders who might have seen LIMIT_OPEN.
        self.in_flight.fetch_add(1, Ordering::AcqRel);

        let limit = self.limit.load(Ordering::Acquire);
        if limit != LIMIT_OPEN {
            let current = self.record_count.load(Ordering::Relaxed);
            if current >= limit {
                self.in_flight.fetch_sub(1, Ordering::AcqRel);
                return Err(StorageError::ExtentSealed(self.id));
            }
            // Fall through to normal append path — late forwarded write within sealed range.
        }

        let payload_len = payload.len();
        // Record layout: [len: 4 bytes][payload: payload_len bytes]
        let record_len = 4 + payload_len;

        // 1. Reserve byte slot (atomic fetch_add, lock-free).
        let byte_pos = self
            .write_cursor
            .fetch_add(record_len as u64, Ordering::Relaxed);
        if byte_pos + record_len as u64 > self.capacity as u64 {
            // Extent full. The cursor may overshoot capacity -- that's fine because
            // seal will stop further appends and committed_bytes won't advance past
            // the last successful record.
            self.in_flight.fetch_sub(1, Ordering::AcqRel);
            return Err(StorageError::ExtentFull(self.id));
        }

        // 2. Reserve logical sequence number (atomic fetch_add, lock-free).
        let seq = self.record_count.fetch_add(1, Ordering::Relaxed);

        // 3. Write record into reserved region (no lock -- exclusive ownership).
        unsafe {
            let dst = self.buf.add(byte_pos as usize);
            // Write length prefix (big-endian u32).
            std::ptr::copy_nonoverlapping((payload_len as u32).to_be_bytes().as_ptr(), dst, 4);
            // Write payload bytes.
            if payload_len > 0 {
                std::ptr::copy_nonoverlapping(payload.as_ref().as_ptr(), dst.add(4), payload_len);
            }
        }

        // 4. Advance committed_seq and committed_bytes in-order (spin-wait).
        //    Each writer spins until committed_seq == their seq (preceding writer is done),
        //    then updates committed_bytes and the index, and FINALLY advances committed_seq.
        //    This ordering ensures that when an observer sees committed_seq == N,
        //    ALL records with seq < N have their committed_bytes and index entries
        //    fully visible. This is critical for seal() atomicity.
        //    Uses exponential backoff: up to 63 spin iterations (sub-µs), then yield to OS.
        let new_committed_bytes = byte_pos + record_len as u64;

        // Phase A: Spin until it's our turn (committed_seq == seq).
        for spin_count in 0u32.. {
            if self.committed_seq.load(Ordering::Acquire) == seq {
                break;
            }
            if spin_count < 6 {
                for _ in 0..(1 << spin_count) {
                    std::hint::spin_loop();
                }
            } else {
                std::thread::yield_now();
            }
        }

        // Phase B: Update committed_bytes and index BEFORE advancing committed_seq.
        self.committed_bytes
            .store(new_committed_bytes, Ordering::Release);
        self.index_record(seq, byte_pos);

        // Phase C: Signal completion — advance committed_seq so the next writer
        // (and any seal() spin-wait) can proceed.
        self.committed_seq.store(seq + 1, Ordering::Release);

        // Decrement in-flight counter — seal() may be waiting for this.
        self.in_flight.fetch_sub(1, Ordering::AcqRel);

        Ok(AppendResult {
            offset: Offset(self.start_offset.0 + seq),
            byte_pos,
        })
    }

    /// Replicate a record at the exact position assigned by the primary.
    ///
    /// This method is used by secondaries to write records at the same
    /// `byte_pos` and sequence number as the primary, ensuring bit-for-bit
    /// identical arena layouts across replicas.
    ///
    /// Unlike `append()`, this method is **single-writer** (one secondary
    /// processes forwards sequentially), so it uses plain `store()` instead
    /// of `fetch_add`/CAS. No `in_flight` tracking is needed.
    ///
    /// Returns the logical offset (`start_offset + seq`) on success.
    pub fn replicate(&self, seq: u64, byte_pos: u64, payload: Bytes) -> Result<AppendResult, StorageError> {
        // Check seal limit.
        let limit = self.limit.load(Ordering::Acquire);
        if limit != LIMIT_OPEN && seq >= limit {
            return Err(StorageError::ExtentSealed(self.id));
        }

        let payload_len = payload.len();
        let record_len = 4 + payload_len;

        // Check capacity.
        if byte_pos + record_len as u64 > self.capacity as u64 {
            return Err(StorageError::ExtentFull(self.id));
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

        // Update cursors via plain store (single-writer on secondary).
        let new_write_cursor = byte_pos + record_len as u64;
        let new_count = seq + 1;

        // Advance write_cursor to max(current, new) — records may arrive out of order.
        let current_wc = self.write_cursor.load(Ordering::Relaxed);
        if new_write_cursor > current_wc {
            self.write_cursor.store(new_write_cursor, Ordering::Relaxed);
        }

        // Advance record_count to max(current, new).
        let current_rc = self.record_count.load(Ordering::Relaxed);
        if new_count > current_rc {
            self.record_count.store(new_count, Ordering::Relaxed);
        }

        // Update committed state.
        self.committed_bytes.store(new_write_cursor, Ordering::Release);
        self.index_record(seq, byte_pos);
        self.committed_seq.store(new_count, Ordering::Release);

        Ok(AppendResult {
            offset: Offset(self.start_offset.0 + seq),
            byte_pos,
        })
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
    fn index_record(&self, seq: u64, byte_pos: u64) {
        let idx = seq as usize;
        if idx < self.index.len() {
            self.index[idx].store(byte_pos as u32, Ordering::Release);
        }
    }

    /// Lookup byte_pos from the internal index.
    ///
    /// Returns `None` if `seq` is out of bounds or the entry has not been
    /// committed yet (still holds the sentinel value).
    pub fn index_lookup(&self, seq: u64) -> Option<u64> {
        let idx = seq as usize;
        if idx >= self.index.len() {
            return None;
        }
        let val = self.index[idx].load(Ordering::Acquire);
        if val == INDEX_UNSET {
            None
        } else {
            Some(val as u64)
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
    /// If `None` (primary sealing itself), the seal is **atomic** with respect
    /// to concurrent appenders:
    ///
    /// 1. Set `limit` to the current `record_count` to block new appenders from
    ///    passing the sealed check in `append()`.
    /// 2. Spin-wait until `committed_seq == record_count`, ensuring all in-flight
    ///    writers (who loaded `limit == LIMIT_OPEN` before step 1) have finished
    ///    their commit.
    /// 3. Re-read `record_count` — it may have advanced beyond the initial limit
    ///    due to those in-flight writers. Update `limit` to the final value.
    ///
    /// This guarantees: after `seal()` returns, `limit == committed_seq == record_count`.
    /// No phantom writes, no orphaned data.
    pub fn seal(&self, committed_offset: Option<u64>) -> u64 {
        if let Some(offset) = committed_offset {
            // Secondary path: SM provides the authoritative committed offset.
            let count = offset.saturating_sub(self.start_offset.0);
            match self
                .limit
                .compare_exchange(LIMIT_OPEN, count, Ordering::Release, Ordering::Acquire)
            {
                Ok(_) => self.start_offset.0 + count,
                Err(limit) => self.start_offset.0 + limit,
            }
        } else {
            // Primary path: seal atomically with concurrent appenders.

            // Step 1: Set limit to current record_count. This prevents any NEW
            // appender from passing the sealed check. Appenders already past the
            // check (loaded LIMIT_OPEN) are in-flight and will commit normally.
            let preliminary = self.record_count.load(Ordering::Acquire);
            match self
                .limit
                .compare_exchange(LIMIT_OPEN, preliminary, Ordering::Release, Ordering::Acquire)
            {
                Ok(_) => {}
                Err(limit) => {
                    // Already sealed (e.g., concurrent seal call).
                    return self.start_offset.0 + limit;
                }
            }

            // Step 2: Wait for all in-flight appenders to drain.
            //
            // After step 1 set limit, new appenders see `limit != LIMIT_OPEN` and
            // either return ExtentSealed (if at/past limit) or proceed as a late
            // forwarded write (if within limit). Either way they decrement in_flight.
            //
            // Appenders who loaded LIMIT_OPEN before step 1 are in-flight: they will
            // increment record_count, write, commit, advance committed_seq, and then
            // decrement in_flight. We wait for all of them to finish.
            let mut spin_count = 0u32;
            loop {
                let inflight = self.in_flight.load(Ordering::Acquire);
                if inflight == 0 {
                    break;
                }
                if spin_count < 6 {
                    for _ in 0..(1 << spin_count) {
                        std::hint::spin_loop();
                    }
                } else {
                    std::thread::yield_now();
                }
                spin_count += 1;
            }

            // Step 3: All in-flight appenders have committed. Read the true final
            // record_count and update limit. This is the authoritative sealed count.
            let final_count = self.record_count.load(Ordering::Acquire);
            if final_count > preliminary {
                self.limit.store(final_count, Ordering::Release);
            }

            self.start_offset.0 + final_count
        }
    }

    /// Whether this extent is sealed.
    pub fn is_sealed(&self) -> bool {
        self.limit.load(Ordering::Acquire) != LIMIT_OPEN
    }

    /// Whether this sealed extent can still accept post-seal forwarded writes.
    /// Returns true when sealed and committed_seq < limit,
    /// meaning there are still outstanding writes that haven't landed yet.
    pub fn accepts_post_seal_writes(&self) -> bool {
        let limit = self.limit.load(Ordering::Acquire);
        if limit == LIMIT_OPEN {
            return false;
        }
        let current = self.committed_seq.load(Ordering::Acquire);
        current < limit
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
        self.committed_seq.load(Ordering::Acquire)
    }

    /// The next logical offset that would be assigned by an append.
    pub fn next_offset(&self) -> Offset {
        Offset(self.start_offset.0 + self.committed_seq.load(Ordering::Acquire))
    }

    /// The last valid offset in this extent (inclusive), or None if empty.
    pub fn last_offset(&self) -> Option<Offset> {
        let count = self.committed_seq.load(Ordering::Acquire);
        if count == 0 {
            None
        } else {
            Some(Offset(self.start_offset.0 + count - 1))
        }
    }

    /// Total bytes written (write_cursor position). Useful for metrics and
    /// size-based seal triggers.
    pub fn bytes_written(&self) -> u64 {
        self.write_cursor.load(Ordering::Relaxed)
    }

    /// Arena capacity in bytes.
    pub fn capacity(&self) -> usize {
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
            .field("capacity", &self.capacity)
            .field("write_cursor", &self.write_cursor.load(Ordering::Relaxed))
            .field("record_count", &self.record_count.load(Ordering::Relaxed))
            .field("committed_seq", &self.committed_seq.load(Ordering::Relaxed))
            .field(
                "committed_bytes",
                &self.committed_bytes.load(Ordering::Relaxed),
            )
            .field("limit", &self.limit.load(Ordering::Relaxed))
            .field("in_flight", &self.in_flight.load(Ordering::Relaxed))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_and_read() {
        let ext = Extent::with_capacity(ExtentId(1), Offset(0), 4096);
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
        let ext = Extent::with_capacity(ExtentId(1), Offset(10), 4096);
        let _r0 = ext.append(Bytes::from_static(b"a")).unwrap();
        let r1 = ext.append(Bytes::from_static(b"b")).unwrap();

        // Read starting from r1's byte_pos — direct seek, no walk from 0.
        let msgs = ext.read(r1.byte_pos, 5).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0], Bytes::from_static(b"b"));
    }

    #[test]
    fn read_out_of_range_returns_empty() {
        let ext = Extent::with_capacity(ExtentId(1), Offset(0), 4096);
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
        let ext = Extent::with_capacity(ExtentId(1), Offset(0), 4096);
        ext.append(Bytes::from_static(b"ok")).unwrap();
        ext.seal(None);

        let result = ext.append(Bytes::from_static(b"fail"));
        assert!(matches!(result, Err(StorageError::ExtentSealed(_))));
        assert_eq!(ext.message_count(), 1);
    }

    #[test]
    fn start_offset_nonzero() {
        let ext = Extent::with_capacity(ExtentId(2), Offset(100), 4096);
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
        let ext = Extent::with_capacity(ExtentId(1), Offset(0), 16);
        ext.append(Bytes::from_static(b"hello")).unwrap(); // 9 bytes, fits
        let result = ext.append(Bytes::from_static(b"world")); // 9 bytes, doesn't fit
        assert!(matches!(result, Err(StorageError::ExtentFull(_))));
    }

    #[test]
    fn committed_data_returns_arena_slice() {
        let ext = Extent::with_capacity(ExtentId(1), Offset(0), 4096);
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
    fn concurrent_appends() {
        use std::sync::Arc;
        use std::thread;

        let ext = Arc::new(Extent::with_capacity(ExtentId(1), Offset(0), 1024 * 1024));
        let num_threads = 8;
        let appends_per_thread = 1000;

        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let ext = Arc::clone(&ext);
                thread::spawn(move || {
                    let mut results = Vec::new();
                    for i in 0..appends_per_thread {
                        let msg = format!("t{t}-m{i}");
                        let r = ext.append(Bytes::from(msg)).unwrap();
                        results.push(r);
                    }
                    results
                })
            })
            .collect();

        let mut all_results: Vec<AppendResult> = Vec::new();
        for h in handles {
            all_results.extend(h.join().unwrap());
        }

        // All offsets should be unique and form a contiguous range.
        all_results.sort_by_key(|r| r.offset.0);
        let total = num_threads * appends_per_thread;
        assert_eq!(all_results.len(), total);
        for i in 0..total {
            assert_eq!(all_results[i].offset.0, i as u64, "offset {} is wrong", i);
        }

        // All records should be readable via their byte_pos.
        assert_eq!(ext.message_count(), total as u64);
        for r in &all_results {
            let msgs = ext.read(r.byte_pos, 1).unwrap();
            assert_eq!(msgs.len(), 1);
            assert!(!msgs[0].is_empty());
            let s = std::str::from_utf8(&msgs[0]).unwrap();
            assert!(s.starts_with('t'));
        }

        // Also verify sequential read from byte 0 returns all records.
        let all_msgs = ext.read(0, total as u32).unwrap();
        assert_eq!(all_msgs.len(), total);
    }

    #[test]
    fn index_lookup_basic() {
        let ext = Extent::with_capacity(ExtentId(1), Offset(0), 4096);

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
        let ext = Extent::with_capacity(ExtentId(1), Offset(100), 4096);
        let r0 = ext.append(Bytes::from_static(b"hello")).unwrap();
        assert_eq!(r0.offset, Offset(100));

        // Internal index uses seq (0-based within extent), not global offset.
        assert_eq!(ext.index_lookup(0), Some(r0.byte_pos));
    }

    #[test]
    fn concurrent_appends_index_consistent() {
        use std::sync::Arc;
        use std::thread;

        let ext = Arc::new(Extent::with_capacity(ExtentId(1), Offset(0), 1024 * 1024));
        let num_threads = 8;
        let appends_per_thread = 1000;

        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let ext = Arc::clone(&ext);
                thread::spawn(move || {
                    let mut results = Vec::new();
                    for i in 0..appends_per_thread {
                        let msg = format!("t{t}-m{i}");
                        let r = ext.append(Bytes::from(msg)).unwrap();
                        results.push(r);
                    }
                    results
                })
            })
            .collect();

        let mut all_results: Vec<AppendResult> = Vec::new();
        for h in handles {
            all_results.extend(h.join().unwrap());
        }

        // Verify every appended record has a consistent index entry.
        all_results.sort_by_key(|r| r.offset.0);
        let total = num_threads * appends_per_thread;
        for i in 0..total {
            let seq = all_results[i].offset.0;
            let expected_byte_pos = all_results[i].byte_pos;
            let actual = ext
                .index_lookup(seq)
                .expect(&format!("seq {} should be set", seq));
            assert_eq!(actual, expected_byte_pos, "index mismatch at seq {}", seq);
        }
    }

    #[test]
    fn replicate_basic() {
        // Simulate a secondary receiving 3 records from the primary.
        let ext = Extent::with_capacity(ExtentId(1), Offset(0), 4096);

        let r0 = ext.replicate(0, 0, Bytes::from_static(b"msg0")).unwrap();
        assert_eq!(r0.offset, Offset(0));
        assert_eq!(r0.byte_pos, 0);

        // "msg0" = 4 bytes payload, record = 4 + 4 = 8 bytes
        let r1 = ext.replicate(1, 8, Bytes::from_static(b"msg1")).unwrap();
        assert_eq!(r1.offset, Offset(1));
        assert_eq!(r1.byte_pos, 8);

        let r2 = ext.replicate(2, 16, Bytes::from_static(b"msg2")).unwrap();
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
        let primary = Extent::with_capacity(ExtentId(1), Offset(0), 4096);
        let secondary = Extent::with_capacity(ExtentId(1), Offset(0), 4096);

        let payloads: Vec<Bytes> = vec![
            Bytes::from_static(b"hello"),
            Bytes::from_static(b"world"),
            Bytes::from_static(b"foo"),
        ];

        // Append on primary, replicate on secondary with same positions.
        for payload in &payloads {
            let result = primary.append(payload.clone()).unwrap();
            secondary
                .replicate(result.offset.0, result.byte_pos, payload.clone())
                .unwrap();
        }

        // Arenas must be identical.
        assert_eq!(primary.committed_data(), secondary.committed_data());
        assert_eq!(primary.message_count(), secondary.message_count());
    }

    #[test]
    fn replicate_sealed_extent_rejects() {
        let ext = Extent::with_capacity(ExtentId(1), Offset(0), 4096);
        ext.replicate(0, 0, Bytes::from_static(b"msg0")).unwrap();
        ext.seal(Some(1)); // seal at 1 record

        // Replicate at seq=1 (at limit) should fail.
        let result = ext.replicate(1, 8, Bytes::from_static(b"msg1"));
        assert!(matches!(result, Err(StorageError::ExtentSealed(_))));
    }

    #[test]
    fn post_seal_append_within_committed_offset() {
        // Simulate a secondary: primary committed 3 records, but secondary only
        // received 1 before seal. SM seals secondary with committed_offset=3.
        // Late forwarded appends for offsets 1,2 should be accepted.
        let ext = Extent::with_capacity(ExtentId(1), Offset(0), 4096);

        // Secondary receives 1 of 3 expected messages before seal.
        ext.append(Bytes::from_static(b"msg0")).unwrap();
        assert_eq!(ext.message_count(), 1);

        // SM seals with committed_offset=3 (primary committed 3 records).
        ext.seal(Some(3));
        assert!(ext.is_sealed());
        assert!(ext.accepts_post_seal_writes()); // committed_seq(1) < limit(3)

        // Late forwarded appends within the sealed range should succeed.
        let r1 = ext.append(Bytes::from_static(b"msg1")).unwrap();
        assert_eq!(r1.offset, Offset(1));
        let r2 = ext.append(Bytes::from_static(b"msg2")).unwrap();
        assert_eq!(r2.offset, Offset(2));

        // Now at the limit — further appends should be rejected.
        assert!(!ext.accepts_post_seal_writes());
        let result = ext.append(Bytes::from_static(b"should-fail"));
        assert!(matches!(result, Err(StorageError::ExtentSealed(_))));
    }

    #[test]
    fn seal_without_committed_offset_uses_local_count() {
        // Primary sealing itself (extent-full path): no committed_offset provided.
        let ext = Extent::with_capacity(ExtentId(1), Offset(0), 4096);
        ext.append(Bytes::from_static(b"msg0")).unwrap();
        ext.append(Bytes::from_static(b"msg1")).unwrap();

        ext.seal(None);
        assert!(ext.is_sealed());
        // limit = record_count = 2, committed_seq = 2 → no room.
        assert!(!ext.accepts_post_seal_writes());
        let result = ext.append(Bytes::from_static(b"should-fail"));
        assert!(matches!(result, Err(StorageError::ExtentSealed(_))));
    }

    #[test]
    fn accepts_post_seal_writes_flag() {
        let ext = Extent::with_capacity(ExtentId(1), Offset(0), 4096);

        // Not sealed → false.
        assert!(!ext.accepts_post_seal_writes());

        ext.append(Bytes::from_static(b"msg0")).unwrap();
        ext.seal(None);

        // Sealed with local count, committed_seq == limit → false.
        assert!(!ext.accepts_post_seal_writes());
    }

    /// Proves that seal(None) is atomic with concurrent appenders:
    /// after seal returns, limit == committed_seq == record_count.
    /// No phantom writes, no orphaned data.
    #[test]
    fn seal_is_atomic_with_concurrent_appends() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        // Run multiple iterations to exercise the race window.
        for _ in 0..50 {
            let ext = Arc::new(Extent::with_capacity(ExtentId(1), Offset(0), 4 * 1024 * 1024));
            let num_appenders = 8;
            let appends_per_thread = 500;

            // Barrier so all appenders and the sealer start simultaneously.
            let barrier = Arc::new(Barrier::new(num_appenders + 1));

            // Spawn appender threads — they race with the seal.
            let appender_handles: Vec<_> = (0..num_appenders)
                .map(|t| {
                    let ext = Arc::clone(&ext);
                    let barrier = Arc::clone(&barrier);
                    thread::spawn(move || {
                        barrier.wait();
                        let mut count = 0u64;
                        for i in 0..appends_per_thread {
                            let msg = format!("t{t}-m{i}");
                            match ext.append(Bytes::from(msg)) {
                                Ok(_) => count += 1,
                                Err(StorageError::ExtentSealed(_)) => break,
                                Err(e) => panic!("unexpected error: {e}"),
                            }
                        }
                        count
                    })
                })
                .collect();

            // Seal from the main thread — races with appenders.
            barrier.wait();
            // Let appenders run briefly before sealing.
            std::hint::spin_loop();
            let seal_offset = ext.seal(None);

            // Collect total successful appends from all threads.
            let total_appended: u64 = appender_handles.into_iter().map(|h| h.join().unwrap()).sum();

            // Key invariants — the entire point of atomic seal:
            let final_limit = ext.limit.load(Ordering::Acquire);
            let final_committed = ext.committed_seq.load(Ordering::Acquire);
            let final_record_count = ext.record_count.load(Ordering::Acquire);

            assert_eq!(
                final_limit, final_committed,
                "limit must equal committed_seq (no phantom writes)"
            );
            assert_eq!(
                final_limit, final_record_count,
                "limit must equal record_count (all in-flight writers finished)"
            );
            assert_eq!(
                seal_offset,
                ext.start_offset.0 + final_limit,
                "seal return value must match final limit"
            );
            assert_eq!(
                total_appended, final_limit,
                "total successful appends must match sealed limit"
            );

            // Every committed record's index entry must be set — no orphaned data.
            for seq in 0..final_committed {
                assert!(
                    ext.index_lookup(seq).is_some(),
                    "index entry for seq {seq} must be set"
                );
            }
        }
    }
}
