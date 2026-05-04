use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arc_swap::ArcSwapOption;
use common::types::{Epoch, EpochState, Offset, StreamId};
use parking_lot::Mutex;
use smallvec::SmallVec;

use crate::arena::ArenaId;
use crate::store::ReplicaInfo;

/// Sentinel value for `limit`: extent is not sealed.
pub(crate) const LIMIT_OPEN: u64 = u64::MAX;

/// Forward-flags bitmap: checked inline during `send_forward()` to
/// guarantee ordering relative to Forward frames.
pub const FLAG_INIT_FORWARD: u8 = 0x01;

/// ForwardChecksum has been received from primary (secondary side).
/// Used by `try_verify_checksum()` to know when to compare.
const FLAG_CHECKSUM_RECEIVED: u8 = 0x02;

/// StreamEpoch has been flushed to S3 and is eligible for memory eviction.
/// Set by Primary locally after upload, and by Secondaries on ForwardFlushed.
pub const FLAG_FLUSHED: u8 = 0x04;

/// Send a periodic CRC checkpoint after this many records.
const CRC_CHECKSUM_RECORD_INTERVAL: u64 = 4096;

/// Send a periodic CRC checkpoint after this many milliseconds.
const CRC_CHECKSUM_TIME_INTERVAL_MS: u64 = 10_000;

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

/// An epoch: pure metadata. Physical arena operations are delegated
/// to `ArenaPool`; this struct tracks consistency state only.
///
/// # Concurrency
///
/// - Write-side state (`hasher`, `committed_offset`) is mutated only
///   by the stream-level leader via `update_crc` / `advance_committed`.
/// - Read-side state (`committed_offset`, `finalized_crc32`, `flags`)
///   is published with `Release` and observed with `Acquire`.
pub struct StreamEpoch {
    pub stream_id: StreamId,
    pub start_offset: Offset,
    pub epoch: Epoch,

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
    /// entry for this (stream, epoch). Updated by Stream after pool
    /// operations (allocation, rotation).
    pub(crate) resident_arenas: Mutex<SmallVec<[ArenaId; 4]>>,

    /// Reference count of live directory entries for this epoch
    /// across all resident arenas. Init 1; decremented when Shape A
    /// upload releases an arena (P3).
    pub(crate) directory_ref_count: AtomicU32,

    /// Replica info for this epoch. `None` until `RegisterEpoch`
    /// arrives. Epoch-scoped: each epoch can have different replica
    /// configuration (e.g., after failover).
    replica_info: ArcSwapOption<ReplicaInfo>,

    /// Last periodic CRC32 checksum sent by the primary (via
    /// `ForwardCrcChecksum`). Advisory: used by secondaries for
    /// early data-integrity checks while the epoch is still active.
    last_crc_checksum: AtomicU32,

    /// The committed offset covered by `last_crc_checksum`.
    /// Secondary compares its own CRC up to this offset against
    /// `last_crc_checksum` once it has caught up.
    last_crc_checksum_offset: AtomicU64,

    /// When the last ForwardCrcChecksum was sent. Used by the primary
    /// to decide when to send the next periodic checkpoint.
    /// Guarded by single-writer invariant (stream leader).
    last_checksum_sent: UnsafeCell<Instant>,

    /// Number of records written since the last ForwardCrcChecksum.
    /// Reset to 0 after each send.
    records_since_checksum: AtomicU64,
}

unsafe impl Send for StreamEpoch {}
unsafe impl Sync for StreamEpoch {}

impl StreamEpoch {
    /// Create a new metadata-only epoch. The first arena is allocated
    /// and registered by `Stream::register_epoch`, not here.
    pub(crate) fn new(stream_id: StreamId, epoch: Epoch, start_offset: Offset) -> Self {
        Self {
            stream_id,
            start_offset,
            epoch,
            committed_offset: AtomicU64::new(start_offset.0),
            limit: AtomicU64::new(LIMIT_OPEN),
            flags: AtomicU8::new(FLAG_INIT_FORWARD),
            hasher: UnsafeCell::new(crc32fast::Hasher::new()),
            finalized_crc32: AtomicU32::new(0),
            resident_arenas: Mutex::new(SmallVec::new()),
            directory_ref_count: AtomicU32::new(0),
            replica_info: ArcSwapOption::from(None),
            last_crc_checksum: AtomicU32::new(0),
            last_crc_checksum_offset: AtomicU64::new(0),
            last_checksum_sent: UnsafeCell::new(Instant::now()),
            records_since_checksum: AtomicU64::new(0),
        }
    }

    // ── Replica info ────────────────────────────────────────────────

    pub(crate) fn replica_info(&self) -> Option<Arc<ReplicaInfo>> {
        self.replica_info.load_full()
    }

    pub(crate) fn set_replica_info(&self, info: Arc<ReplicaInfo>) {
        self.replica_info.store(Some(info));
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

    pub fn try_advance_committed(&self) {
        // Intentional no-op. Forward frames advance committed_offset
        // synchronously via `Stream::replicate`.
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

    pub fn limit_hint(&self) -> u64 {
        self.limit.load(Ordering::Acquire)
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

    // ── Pool-delegated metadata updates ─────────────────────────────

    /// Update the per-epoch CRC32 hasher with one record's payload.
    /// Called by Stream AFTER `pool.write_batch` returns, preserving
    /// the same ordering as the physical writes.
    ///
    /// # SAFETY
    ///
    /// Single-writer invariant upheld by Stream leader: only one
    /// task at a time calls this method per epoch.
    pub(crate) fn update_crc(&self, payload: &[u8]) {
        // SAFETY: single-writer invariant upheld by Stream leader.
        unsafe {
            let h = &mut *self.hasher.get();
            h.update(&(payload.len() as u32).to_be_bytes());
            if !payload.is_empty() {
                h.update(payload);
            }
        }
    }

    /// Advance `committed_offset` by `count` records. Called by
    /// Stream AFTER `pool.write_batch` returns.
    pub(crate) fn advance_committed(&self, count: u32) {
        self.committed_offset
            .fetch_add(count as u64, Ordering::Release);
    }

    // ── Periodic CRC checksum (ForwardCrcChecksum) ─────────────────

    /// Store a periodic CRC checkpoint received from the primary.
    /// Called by the secondary's forward handler.
    pub(crate) fn store_crc_checksum(&self, checksum: u32, up_to_offset: u64) {
        self.last_crc_checksum.store(checksum, Ordering::Release);
        self.last_crc_checksum_offset
            .store(up_to_offset, Ordering::Release);
    }

    /// Verify the local CRC up to `last_crc_checksum_offset` against
    /// the primary's stored checkpoint. Returns `None` if the
    /// secondary hasn't caught up to the checkpoint offset yet.
    ///
    /// # SAFETY
    ///
    /// Reads the hasher under single-writer invariant: the secondary's
    /// forward handler is the only writer, and this is called after
    /// ForwardCrcChecksum processing.
    pub(crate) fn verify_crc_checksum(&self) -> Option<bool> {
        let up_to = self.last_crc_checksum_offset.load(Ordering::Acquire);
        if up_to == 0 {
            return None;
        }
        let committed = self.committed_offset.load(Ordering::Acquire);
        if committed < up_to {
            // Secondary hasn't caught up yet.
            return None;
        }
        // SAFETY: single-writer invariant — secondary's forward handler
        // is the only task that mutates the hasher, and ForwardCrcChecksum
        // frames arrive after all preceding Forward frames for the same
        // offsets have been processed.
        let local_crc = unsafe { (*self.hasher.get()).clone().finalize() };
        let remote_crc = self.last_crc_checksum.load(Ordering::Acquire);
        Some(local_crc == remote_crc)
    }

    /// Snapshot the current CRC32 value for sending a periodic
    /// `ForwardCrcChecksum`. Clones the hasher (does NOT finalize it)
    /// so that ongoing writes can continue. Resets the tracking
    /// counters after snapshotting.
    ///
    /// # SAFETY
    ///
    /// Single-writer invariant: only the stream leader calls this.
    pub(crate) fn snapshot_crc(&self) -> (u32, Offset) {
        // SAFETY: single-writer invariant upheld by stream leader.
        let crc = unsafe { (*self.hasher.get()).clone().finalize() };
        let offset = Offset(self.committed_offset.load(Ordering::Acquire));
        // SAFETY: single-writer invariant.
        unsafe {
            *self.last_checksum_sent.get() = Instant::now();
        }
        self.records_since_checksum.store(0, Ordering::Release);
        (crc, offset)
    }

    /// Whether it's time to send a periodic ForwardCrcChecksum.
    /// True when 4096 records written OR 10 seconds elapsed.
    pub(crate) fn should_send_crc_checksum(&self) -> bool {
        let records = self.records_since_checksum.load(Ordering::Acquire);
        if records >= CRC_CHECKSUM_RECORD_INTERVAL {
            return true;
        }
        // SAFETY: single-writer invariant — only stream leader reads
        // and writes this field.
        let elapsed = unsafe { (*self.last_checksum_sent.get()).elapsed() };
        elapsed >= Duration::from_millis(CRC_CHECKSUM_TIME_INTERVAL_MS)
    }

    /// Increment the count of records written since last checksum.
    /// Called by Stream after each write.
    pub(crate) fn incr_records_since_checksum(&self, count: u32) {
        self.records_since_checksum
            .fetch_add(count as u64, Ordering::Release);
    }
}

impl std::fmt::Debug for StreamEpoch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamEpoch")
            .field("stream_id", &self.stream_id)
            .field("epoch", &self.epoch)
            .field("start_offset", &self.start_offset)
            .field("resident_arena_count", &self.resident_arenas.lock().len())
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
    use crate::arena::{ArenaIdGenerator, ArenaPool, DedicatedArenaPool, WriteBatchJob};
    use bytes::Bytes;
    use common::errors::StorageError;

    fn new_epoch_with_pool(start: u64, arena_capacity: u32) -> (StreamEpoch, Arc<dyn ArenaPool>) {
        let pool: Arc<dyn ArenaPool> =
            Arc::new(DedicatedArenaPool::new(Arc::new(ArenaIdGenerator::new(1))));
        let ep = StreamEpoch::new(StreamId(7), Epoch(3), Offset(start));
        // Allocate the first arena through the pool (as Stream::register_epoch does).
        let arena = pool.allocate(ep.stream_id, ep.epoch, Offset(start), arena_capacity);
        ep.resident_arenas.lock().push(arena.arena_id);
        ep.directory_ref_count.fetch_add(1, Ordering::Release);
        (ep, pool)
    }

    /// Helper: write one record via pool, then update epoch metadata.
    /// Also syncs resident_arenas when arena rotation occurs.
    fn write_one(
        ep: &StreamEpoch,
        pool: &dyn ArenaPool,
        payload: Bytes,
    ) -> Result<AppendResult, StorageError> {
        let hint = Offset(ep.committed_offset());
        let job = WriteBatchJob::new(hint, payload.clone());
        let mut results = pool.write_batch(ep.stream_id, ep.epoch, std::slice::from_ref(&job));
        match results.pop().expect("one result") {
            Ok(r) => {
                // Sync resident_arenas if the arena was rotated (new arena_id).
                let known: SmallVec<[ArenaId; 4]> = ep.resident_arenas.lock().clone();
                if !known.contains(&r.arena_id) {
                    ep.resident_arenas.lock().push(r.arena_id);
                    ep.directory_ref_count.fetch_add(1, Ordering::Release);
                }
                ep.update_crc(&payload);
                ep.advance_committed(1);
                Ok(AppendResult {
                    offset: r.offset,
                    byte_pos: r.byte_pos as u64,
                })
            }
            Err(e) => Err(e),
        }
    }

    #[test]
    fn append_inner_single_arena_happy_path() {
        let (ep, pool) = new_epoch_with_pool(0, 4096);
        let r0 = write_one(&ep, &*pool, Bytes::from_static(b"msg0")).unwrap();
        let r1 = write_one(&ep, &*pool, Bytes::from_static(b"msg1")).unwrap();
        let r2 = write_one(&ep, &*pool, Bytes::from_static(b"msg2")).unwrap();
        assert_eq!(r0.offset, Offset(0));
        assert_eq!(r1.offset, Offset(1));
        assert_eq!(r2.offset, Offset(2));
        assert_eq!(ep.committed_offset(), 3);
        assert_eq!(ep.resident_arenas().len(), 1);
    }

    #[test]
    fn append_inner_rotates_on_arena_full() {
        let (ep, pool) = new_epoch_with_pool(100, 16);
        let r0 = write_one(&ep, &*pool, Bytes::from_static(b"aaaa")).unwrap();
        let r1 = write_one(&ep, &*pool, Bytes::from_static(b"bbbb")).unwrap();
        let r2 = write_one(&ep, &*pool, Bytes::from_static(b"cccc")).unwrap();

        assert_eq!(r0.offset, Offset(100));
        assert_eq!(r1.offset, Offset(101));
        assert_eq!(r2.offset, Offset(102));
        assert_eq!(ep.committed_offset(), 103);
        // Pool should have 2 arenas after rotation.
        assert_eq!(ep.resident_arenas().len(), 2);
        assert_eq!(ep.directory_ref_count.load(Ordering::Acquire), 2);
    }

    #[test]
    fn read_at_offset_crosses_arena_boundary() {
        let (ep, pool) = new_epoch_with_pool(0, 16);
        for i in 0..5u32 {
            write_one(&ep, &*pool, Bytes::copy_from_slice(&i.to_be_bytes())).unwrap();
        }
        // Pool should have rotated.
        assert!(ep.resident_arenas().len() >= 2);

        let msgs = pool
            .read_at_offset(ep.stream_id, ep.epoch, Offset(0), 5)
            .unwrap();
        assert_eq!(msgs.len(), 5);
        for (i, msg) in msgs.iter().enumerate() {
            assert_eq!(msg.as_ref(), (i as u32).to_be_bytes());
        }

        // Partial read starting mid-stream.
        let tail = pool
            .read_at_offset(ep.stream_id, ep.epoch, Offset(3), 10)
            .unwrap();
        assert_eq!(tail.len(), 2);
        assert_eq!(tail[0].as_ref(), 3u32.to_be_bytes());
        assert_eq!(tail[1].as_ref(), 4u32.to_be_bytes());
    }

    #[test]
    fn seal_after_rotation_finalizes_crc_over_all_records() {
        let (ep, pool) = new_epoch_with_pool(0, 16);
        let payloads: &[&[u8]] = &[b"aaaa", b"bbbb", b"cccc", b"dddd"];
        for p in payloads {
            write_one(&ep, &*pool, Bytes::copy_from_slice(p)).unwrap();
        }
        assert!(ep.resident_arenas().len() >= 2, "expected rotation");
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
    fn update_crc_and_advance_committed() {
        let ep = StreamEpoch::new(StreamId(1), Epoch(1), Offset(0));
        assert_eq!(ep.committed_offset(), 0);

        ep.update_crc(b"hello");
        ep.advance_committed(1);
        assert_eq!(ep.committed_offset(), 1);

        ep.update_crc(b"world");
        ep.advance_committed(1);
        assert_eq!(ep.committed_offset(), 2);
    }

    #[test]
    fn replica_info_set_and_get() {
        let ep = StreamEpoch::new(StreamId(1), Epoch(1), Offset(0));
        assert!(ep.replica_info().is_none());
        // Note: can't easily construct ReplicaInfo in a unit test without
        // the full store infrastructure. The set/get contract is tested
        // via integration tests.
    }
}
