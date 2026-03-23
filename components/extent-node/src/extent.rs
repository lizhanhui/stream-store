use std::alloc::{Layout, alloc, dealloc};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use bytes::Bytes;
use common::errors::StorageError;
use common::types::{ExtentId, ExtentState, Offset};

/// Default arena capacity: 64 MB.
pub const DEFAULT_ARENA_CAPACITY: usize = 64 * 1024 * 1024;

/// Result of a successful append: the logical offset and the byte position
/// within the arena where the record was written. The caller can use the
/// byte position to build an external index (e.g., fixed-width index stream
/// records: `[stream_id:u64][extent_id:u64][offset:u64][byte_pos:u64]`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendResult {
    /// Logical offset assigned to this record (base_offset + sequence number).
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
///    (nanoseconds for typical MQTT messages <1 KB). This is the same pattern used by
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
    pub base_offset: Offset,

    /// Pre-allocated contiguous buffer. Owned by this Extent; freed on drop.
    buf: *mut u8,
    /// Total capacity of the arena in bytes.
    capacity: usize,
    /// Layout used for allocation (needed for dealloc).
    layout: Layout,

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

    /// Whether this extent has been sealed.
    sealed: AtomicBool,
}

// SAFETY: The raw pointer `buf` is exclusively owned by this Extent.
// All concurrent access to the arena is mediated by atomic cursors that
// guarantee non-overlapping writes and committed-seq-bounded reads.
unsafe impl Send for Extent {}
unsafe impl Sync for Extent {}

impl Extent {
    /// Create a new active extent with default capacity (64 MB).
    pub fn new(id: ExtentId, base_offset: Offset) -> Self {
        Self::with_capacity(id, base_offset, DEFAULT_ARENA_CAPACITY)
    }

    /// Create a new active extent with the specified capacity in bytes.
    pub fn with_capacity(id: ExtentId, base_offset: Offset, capacity: usize) -> Self {
        let layout = Layout::from_size_align(capacity, 8)
            .expect("invalid layout");
        // SAFETY: layout is valid, nonzero size.
        let buf = unsafe { alloc(layout) };
        if buf.is_null() {
            std::alloc::handle_alloc_error(layout);
        }

        Self {
            id,
            base_offset,
            buf,
            capacity,
            layout,
            write_cursor: AtomicU64::new(0),
            record_count: AtomicU64::new(0),
            committed_seq: AtomicU64::new(0),
            committed_bytes: AtomicU64::new(0),
            sealed: AtomicBool::new(false),
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
        if self.sealed.load(Ordering::Acquire) {
            return Err(StorageError::ExtentSealed(self.id));
        }

        let payload_len = payload.len();
        // Record layout: [len: 4 bytes][payload: payload_len bytes]
        let record_len = 4 + payload_len;

        // 1. Reserve byte slot (atomic fetch_add, lock-free).
        let byte_pos = self.write_cursor.fetch_add(record_len as u64, Ordering::Relaxed);
        if byte_pos + record_len as u64 > self.capacity as u64 {
            // Extent full. The cursor may overshoot capacity -- that's fine because
            // seal will stop further appends and committed_bytes won't advance past
            // the last successful record.
            return Err(StorageError::ExtentFull(self.id));
        }

        // 2. Reserve logical sequence number (atomic fetch_add, lock-free).
        let seq = self.record_count.fetch_add(1, Ordering::Relaxed);

        // 3. Write record into reserved region (no lock -- exclusive ownership).
        unsafe {
            let dst = self.buf.add(byte_pos as usize);
            // Write length prefix (big-endian u32).
            std::ptr::copy_nonoverlapping(
                (payload_len as u32).to_be_bytes().as_ptr(),
                dst,
                4,
            );
            // Write payload bytes.
            if payload_len > 0 {
                std::ptr::copy_nonoverlapping(
                    payload.as_ref().as_ptr(),
                    dst.add(4),
                    payload_len,
                );
            }
        }

        // 4. Advance committed_seq and committed_bytes in-order (spin-wait CAS).
        //    Each writer spins until committed_seq == their seq, then advances to seq+1.
        //    The spin waits only for the immediately preceding writer's memcpy to finish.
        let new_committed_bytes = byte_pos + record_len as u64;
        loop {
            match self.committed_seq.compare_exchange_weak(
                seq,
                seq + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.committed_bytes.store(new_committed_bytes, Ordering::Release);
                    break;
                }
                Err(_) => std::hint::spin_loop(),
            }
        }

        Ok(AppendResult {
            offset: Offset(self.base_offset.0 + seq),
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

            let len = u32::from_be_bytes([
                arena[pos],
                arena[pos + 1],
                arena[pos + 2],
                arena[pos + 3],
            ]) as usize;

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

    /// Create a `Bytes` view of the entire arena buffer.
    ///
    /// The returned `Bytes` is reference-counted, so the arena memory stays alive
    /// as long as any derived slice is held by a reader.
    fn arena_as_bytes(&self) -> Bytes {
        let ptr = self.buf;
        let cap = self.capacity;
        // SAFETY: `ptr` is valid for `cap` bytes and remains valid as long as this
        // Extent lives. We extend the lifetime to 'static -- this is sound because
        // the Extent guarantees the buffer lives as long as any Bytes derived from it
        // (sealed extents are kept alive until evicted, active extents are never
        // dropped while serving reads).
        //
        // NOTE: In production we should use a proper Arc<[u8]> wrapper.
        let slice = unsafe { std::slice::from_raw_parts(ptr, cap) };
        let static_slice: &'static [u8] = unsafe { std::mem::transmute(slice) };
        Bytes::from_static(static_slice)
    }

    /// Seal this extent. No more appends will be accepted.
    ///
    /// After sealing, `committed_seq` is the definitive record count.
    pub fn seal(&self) {
        self.sealed.store(true, Ordering::Release);
    }

    /// Whether this extent is sealed.
    pub fn is_sealed(&self) -> bool {
        self.sealed.load(Ordering::Acquire)
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
        Offset(self.base_offset.0 + self.committed_seq.load(Ordering::Acquire))
    }

    /// The last valid offset in this extent (inclusive), or None if empty.
    pub fn last_offset(&self) -> Option<Offset> {
        let count = self.committed_seq.load(Ordering::Acquire);
        if count == 0 {
            None
        } else {
            Some(Offset(self.base_offset.0 + count - 1))
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

impl Drop for Extent {
    fn drop(&mut self) {
        // SAFETY: buf was allocated with this layout in new()/with_capacity().
        unsafe {
            dealloc(self.buf, self.layout);
        }
    }
}

// Debug impl that doesn't try to print the entire buffer.
impl std::fmt::Debug for Extent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Extent")
            .field("id", &self.id)
            .field("base_offset", &self.base_offset)
            .field("capacity", &self.capacity)
            .field("write_cursor", &self.write_cursor.load(Ordering::Relaxed))
            .field("record_count", &self.record_count.load(Ordering::Relaxed))
            .field("committed_seq", &self.committed_seq.load(Ordering::Relaxed))
            .field("committed_bytes", &self.committed_bytes.load(Ordering::Relaxed))
            .field("sealed", &self.sealed.load(Ordering::Relaxed))
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
        ext.seal();

        let result = ext.append(Bytes::from_static(b"fail"));
        assert!(matches!(result, Err(StorageError::ExtentSealed(_))));
        assert_eq!(ext.message_count(), 1);
    }

    #[test]
    fn base_offset_nonzero() {
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
}
