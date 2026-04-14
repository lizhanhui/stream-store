use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bytes::Bytes;
use common::errors::StorageError;
use common::types::{Epoch, ExtentId, ExtentState, Offset, StreamId};
use crossbeam_channel::{Receiver, Sender, unbounded};
use rpc::frame::Frame;
use tokio::sync::mpsc;
use tracing::error;

use crate::extent::{AppendResult, Extent};
use crate::store::AppendJob;

/// Reason for sealing the active extent and creating a new one.
/// Controls the capacity scaling heuristic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SealReason {
    /// Extent arena is full — stream needs more space. Scale up: double capacity.
    ExtentFull,
    /// System tick detected an under-utilized extent (idle > threshold, < 50% full).
    /// Scale down: halve capacity (or jump to min if completely empty).
    IdleShrink,
}

/// A stream: an ordered, append-only sequence of messages backed by a list of extents.
///
/// The active (last) extent is a lock-free arena. Multiple concurrent appenders
/// can write to it without any external mutex -- offset assignment, payload copy,
/// and commit advancement are all handled by the Extent's internal atomics.
///
/// Each extent maintains an internal index mapping sequence numbers to byte
/// positions (compressed u32 pointers). The index is populated atomically during
/// append and used during read to resolve offsets without client-side byte_pos.
///
/// Stream-level mutation (`seal`, adding new extents) still requires `&mut self`
/// because these operations change the extent list. In the ExtentNodeStore, this is
/// handled at a higher level (DashMap per-stream write lock or equivalent).
///
/// Pipelined group commit is coordinated at the stream level via `in_flight`,
/// `job_tx`, and `job_rx`. This ensures extent transitions (seal + create) are
/// handled transparently by the stream-level leader without callers needing to
/// know about individual extent boundaries.
pub struct Stream {
    pub id: StreamId,

    extents: Vec<Extent>,

    /// Current epoch assigned by Stream Manager. Within an epoch, the replica set
    /// is fixed and the Primary can autonomously create extents on extent-full.
    epoch: Epoch,

    /// Next extent ID for autonomous creation within the current epoch.
    /// Initialized to `first_extent_id + 1` when SM sends RegisterExtent.
    next_extent_id: ExtentId,

    /// Extent capacity for autonomously created extents (bytes).
    /// This is the capacity of the current/last extent created.
    extent_capacity: u32,

    /// Minimum extent capacity (floor for adaptive shrink). From SM via RegisterExtent.
    min_extent_capacity: u32,

    /// Maximum extent capacity (ceiling for adaptive growth). From SM via RegisterExtent.
    max_extent_capacity: u32,

    /// Capacity to use for the next autonomously created extent.
    /// Adaptive: doubles on extent-full, halves on idle-shrink.
    /// Clamped to [min_extent_capacity, max_extent_capacity].
    next_extent_capacity: u32,

    /// When the current active extent was created (for the 5-minute idle-shrink rule).
    active_extent_created_at: Option<Instant>,

    /// Maximum number of extents to retain. 0 = no limit.
    /// When exceeded, the oldest sealed extents are dropped to free memory.
    max_extents: usize,

    /// Leader election counter for pipelined group commit (stream-level).
    /// 0 = idle. The leader owns the entire stream, handling extent transitions inline.
    in_flight: AtomicU64,

    /// Channel for followers to submit append jobs to the active writer.
    job_tx: Sender<AppendJob>,
    job_rx: Receiver<AppendJob>,

    /// Cached per-secondary UnboundedSender clones (Primary only).
    /// Populated at RegisterExtent time from DownstreamPool.
    /// Vec since RF is small (1-3); iteration is the hot path.
    downstream_txs: Vec<mpsc::UnboundedSender<Frame>>,

    /// Pool of recycled extents ready for reuse. Avoids ~5ms allocation
    /// on extent-full transitions. Pre-populated at register_extent time;
    /// replenished by evict_oldest_extents.
    extent_pool: VecDeque<Extent>,
}

impl Stream {
    /// Create a new stream with no extents. Extents are added via `register_extent()`.
    pub fn new(id: StreamId) -> Self {
        let (job_tx, job_rx) = unbounded();
        Self {
            id,
            extents: Vec::new(),
            epoch: Epoch(0),
            next_extent_id: ExtentId(0),
            extent_capacity: 0,
            min_extent_capacity: 0,
            max_extent_capacity: 0,
            next_extent_capacity: 0,
            active_extent_created_at: None,
            max_extents: 0,
            in_flight: AtomicU64::new(0),
            job_tx,
            job_rx,
            downstream_txs: Vec::new(),
            extent_pool: VecDeque::new(),
        }
    }

    /// Set the maximum number of extents to retain per stream.
    /// 0 means no limit (default).
    pub fn set_max_extents(&mut self, max: usize) {
        self.max_extents = max;
    }

    /// Register a new extent on this stream (called when SM sends RegisterExtent).
    /// Updates the epoch and sets up the next extent ID for autonomous creation.
    ///
    /// `extent_capacity` is the capacity for this specific extent (SM-decided).
    /// `min_extent_capacity` and `max_extent_capacity` are the stream-level bounds
    /// for adaptive sizing during autonomous extent creation.
    pub fn register_extent(
        &mut self,
        id: ExtentId,
        start_offset: Offset,
        extent_capacity: u32,
        epoch: Epoch,
        min_extent_capacity: u32,
        max_extent_capacity: u32,
    ) {
        self.epoch = epoch;
        self.extent_capacity = extent_capacity;
        self.min_extent_capacity = min_extent_capacity;
        self.max_extent_capacity = max_extent_capacity;
        self.next_extent_capacity = extent_capacity;
        self.next_extent_id = ExtentId(id.0 + 1);
        self.active_extent_created_at = Some(Instant::now());
        self.extents.push(Extent::with_capacity(
            id,
            start_offset,
            extent_capacity,
            epoch,
        ));
        self.evict_oldest_extents();

        // Pre-allocate one spare extent for the pool so the first
        // seal-and-create can recycle instead of allocating fresh.
        if self.max_extents > 0 && self.extent_pool.is_empty() {
            self.extent_pool.push_back(Extent::with_capacity(
                ExtentId(0), // placeholder — reset() overwrites on use
                Offset(0),
                extent_capacity,
                Epoch(0),
            ));
        }
    }

    /// Simplified register_extent for tests and backward compatibility.
    /// Uses extent_capacity as both min and max (no adaptive sizing).
    #[cfg(test)]
    pub fn register_extent_simple(
        &mut self,
        id: ExtentId,
        start_offset: Offset,
        extent_capacity: u32,
        epoch: Epoch,
    ) {
        self.register_extent(
            id,
            start_offset,
            extent_capacity,
            epoch,
            extent_capacity,
            extent_capacity,
        );
    }

    /// Return the extent capacity configured for this stream.
    pub fn extent_capacity(&self) -> u32 {
        self.extent_capacity
    }

    /// Return the maximum number of extents to retain (0 = no limit).
    pub fn max_extents(&self) -> usize {
        self.max_extents
    }

    /// Append a message to the specified extent. Returns the assigned
    /// offset and byte position within the extent arena.
    ///
    /// Only requires `&self` -- the Extent is internally synchronized (lock-free).
    /// The byte_pos is recorded in the extent's internal index automatically.
    ///
    /// Returns an error if the extent doesn't exist.
    pub fn append(
        &self,
        extent_id: ExtentId,
        payload: Bytes,
    ) -> Result<AppendResult, StorageError> {
        let extent = self.find_extent(extent_id).ok_or_else(|| {
            StorageError::Internal(format!(
                "stream {}: extent {} not found",
                self.id, extent_id
            ))
        })?;
        extent.append(payload)
    }

    /// Replicate a record at the exact position assigned by the primary.
    ///
    /// Delegates to `Extent::replicate()` for deterministic replication.
    /// Only requires `&self` — the Extent handles writes internally.
    pub fn replicate(
        &self,
        extent_id: ExtentId,
        offset: Offset,
        byte_pos: u64,
        payload: Bytes,
    ) -> Result<AppendResult, StorageError> {
        let extent = self.find_extent(extent_id).ok_or_else(|| {
            error!("Stream: {}, Extent: {} is not found", self.id, extent_id);
            StorageError::Internal(format!(
                "stream {}: extent {} not found",
                self.id, extent_id
            ))
        })?;
        extent.replicate(offset, byte_pos, payload)
    }

    /// Read `count` messages starting from the given logical `offset` within
    /// the specified extent.
    ///
    /// The server resolves `offset → byte_pos` internally via the index stream,
    /// so callers only need to provide the logical offset. This keeps byte_pos
    /// as an internal implementation detail invisible to clients.
    pub fn read(
        &self,
        extent_id: ExtentId,
        offset: Offset,
        count: u32,
    ) -> Result<Vec<Bytes>, StorageError> {
        let extent = self.find_extent(extent_id).ok_or_else(|| {
            StorageError::Internal(format!(
                "stream {}: extent {} not found",
                self.id, extent_id
            ))
        })?;

        // Check offset is within this extent's range.
        if offset.0 < extent.start_offset.0 || offset.0 >= extent.next_offset().0 {
            return Ok(Vec::new());
        }

        let seq = offset.0 - extent.start_offset.0;
        let byte_pos = extent.index_lookup(seq).ok_or_else(|| {
            StorageError::Internal(format!("index lookup failed for offset {}", offset.0))
        })?;
        extent.read(byte_pos, count)
    }

    /// Whether this stream can accept appends (its last extent is active/unsealed).
    pub fn is_mutable(&self) -> bool {
        self.extents
            .last()
            .map(|e| e.state() == ExtentState::Active)
            .unwrap_or(false)
    }

    /// The extent ID of the active (last) extent, or None if no extents.
    pub fn active_extent_id(&self) -> Option<ExtentId> {
        self.extents.last().map(|e| e.id)
    }

    /// The maximum offset (exclusive): the next offset that would be assigned.
    /// Returns `Offset(0)` if the stream has no extents.
    pub fn max_offset(&self) -> Offset {
        self.extents
            .last()
            .map(|e| e.next_offset())
            .unwrap_or(Offset(0))
    }

    /// Seal the extent identified by `extent_id`.
    /// Returns `(start_offset, end_offset)` of the sealed extent, or `None` if:
    /// - no extents exist
    /// - the active extent doesn't match `extent_id`
    /// - the extent is already sealed
    ///
    /// `end_offset` = `start_offset + message_count` (exclusive upper bound).
    ///
    /// If `committed_offset` is `Some`, it's the primary's committed offset propagated
    /// via SM. The sealed extent will accept late forwarded appends up to that offset.
    /// If `None`, the extent uses its local record_count (primary sealing itself).
    ///
    /// After seal, the stream has no active extent until SM sends a new `RegisterExtent`
    /// or the Primary autonomously creates one via `create_next_extent()`.
    ///
    /// Requires `&mut self` because it modifies the extent list.
    pub fn seal(
        &mut self,
        extent_id: ExtentId,
        committed_offset: Option<u64>,
    ) -> Option<(u64, u64)> {
        let last = self.extents.last()?;
        if last.id != extent_id {
            return None;
        }
        if last.state() == ExtentState::Sealed {
            return None;
        }
        let start_offset = last.start_offset.0;
        let end_offset = last.seal(committed_offset);
        Some((start_offset, end_offset))
    }

    /// Autonomously create the next extent on extent-full (Primary only, within same epoch).
    ///
    /// Uses `next_extent_capacity` for the new extent (adaptive sizing).
    /// Extent ID is incremented locally — no SM round-trip needed.
    ///
    /// Returns `(new_extent_id, start_offset)` of the created extent.
    pub fn create_next_extent(&mut self) -> (ExtentId, Offset) {
        let end_offset = self
            .extents
            .last()
            .map(|e| Offset(e.start_offset.0 + e.message_count()))
            .unwrap_or(Offset(0));
        let new_id = self.next_extent_id;
        self.next_extent_id = ExtentId(new_id.0 + 1);
        let target_capacity = self.next_extent_capacity;

        // Recycle from pool (O(1) reset, resize if needed) or allocate fresh (~5ms).
        if let Some(mut recycled) = self.extent_pool.pop_front() {
            if recycled.capacity() != target_capacity {
                recycled.resize(target_capacity);
            }
            recycled.reset(new_id, end_offset, self.epoch);
            self.extents.push(recycled);
        } else {
            self.extents.push(Extent::with_capacity(
                new_id,
                end_offset,
                target_capacity,
                self.epoch,
            ));
        }

        self.extent_capacity = target_capacity;
        self.active_extent_created_at = Some(Instant::now());
        self.evict_oldest_extents();
        (new_id, end_offset)
    }

    /// Current epoch of this stream.
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// Update the epoch (e.g., when RegisterExtent arrives for an already lazily-created extent).
    pub fn set_epoch(&mut self, epoch: Epoch) {
        self.epoch = epoch;
    }

    /// Report extents for this stream that belong to the specified epoch.
    ///
    /// During recovery, SM only cares about extents created in the specified epoch
    /// (extents from prior epochs are already reconciled in MySQL metadata).
    /// Filters by per-extent epoch, so only extents actually created under the
    /// requested epoch are returned.
    pub fn report_extents(&self, epoch: Epoch) -> Vec<(ExtentId, Offset, u64, ExtentState)> {
        self.extents
            .iter()
            .filter(|e| e.epoch == epoch)
            .map(|e| {
                let end_offset = if e.is_sealed() {
                    e.start_offset.0 + e.message_count()
                } else {
                    0 // active extent, end_offset not yet determined
                };
                (e.id, e.start_offset, end_offset, e.state())
            })
            .collect()
    }

    /// The end_offset of the specified sealed extent.
    /// Used by handle_seal to return committed offset idempotently when the
    /// extent was already sealed (e.g., primary already sealed via extent-full path).
    /// Returns 0 if the extent is not found or not sealed.
    pub fn sealed_end_offset(&self, extent_id: ExtentId) -> u64 {
        if let Some(extent) = self.find_extent(extent_id)
            && extent.is_sealed()
        {
            return extent.start_offset.0 + extent.message_count();
        }
        0
    }

    /// Find an extent by its ID.
    pub fn find_extent(&self, extent_id: ExtentId) -> Option<&Extent> {
        self.extents.iter().find(|e| e.id == extent_id)
    }

    /// Recycle oldest extents into the pool when count exceeds `max_extents`.
    ///
    /// Evicts from the front of the extent list (oldest first). The last extent
    /// (active/current) is never evicted. Recycled extents are pushed to the
    /// pool for O(1) reuse by `create_next_extent`. If an extent has outstanding
    /// reader references (Arc refcount > 1), it is dropped instead of recycled.
    fn evict_oldest_extents(&mut self) {
        if self.max_extents == 0 {
            if self.extents.len() > 4 {
                tracing::warn!(
                    "stream {} has {} extents but max_extents=0 (no eviction); \
                     memory will grow unbounded",
                    self.id,
                    self.extents.len(),
                );
            }
            return;
        }
        while self.extents.len() > self.max_extents && self.extents.len() > 1 {
            let evicted = self.extents.remove(0);
            // Cap the pool at 2 spares to bound memory. Beyond that, just drop.
            if evicted.can_recycle() && self.extent_pool.len() < 2 {
                tracing::info!(
                    "recycled extent {} (sealed={}) from stream {} (retained: {}, pool: {})",
                    evicted.id,
                    evicted.is_sealed(),
                    self.id,
                    self.extents.len(),
                    self.extent_pool.len() + 1,
                );
                self.extent_pool.push_back(evicted);
            } else {
                tracing::info!(
                    "dropping extent {} from stream {} (retained: {}, pool: {})",
                    evicted.id,
                    self.id,
                    self.extents.len(),
                    self.extent_pool.len(),
                );
            }
        }
    }

    /// Return the stream-level in_flight counter (for pipelined group commit).
    pub(crate) fn in_flight(&self) -> &AtomicU64 {
        &self.in_flight
    }

    /// Return a reference to the job sender channel.
    pub(crate) fn job_tx(&self) -> &Sender<AppendJob> {
        &self.job_tx
    }

    /// Return a reference to the job receiver channel.
    pub(crate) fn job_rx(&self) -> &Receiver<AppendJob> {
        &self.job_rx
    }

    /// Set cached downstream senders (Primary only, called at RegisterExtent time).
    pub(crate) fn set_downstream_txs(&mut self, txs: Vec<mpsc::UnboundedSender<Frame>>) {
        self.downstream_txs = txs;
    }

    /// Whether this stream has secondary senders (i.e., is Primary with RF >= 2).
    pub(crate) fn has_secondaries(&self) -> bool {
        !self.downstream_txs.is_empty()
    }

    /// Push a frame to all secondary channels. Fire-and-forget.
    ///
    /// Called inline from the leader's append path while `in_flight > 0`,
    /// guaranteeing FIFO ordering — no frame from a subsequent leader can
    /// appear in the channel before frames from the current leader.
    pub(crate) fn send_forward(&self, frame: Frame) {
        let n = self.downstream_txs.len();
        if n == 0 {
            return;
        }
        // Send clones to all but the last, move the original to the last.
        for tx in &self.downstream_txs[..n - 1] {
            let _ = tx.send(frame.clone());
        }
        let _ = self.downstream_txs[n - 1].send(frame);
    }

    /// Append to the active extent (single-writer, called by stream-level leader).
    ///
    /// Returns `Ok((result, extent_id))` on success, or `Err(ExtentFull)` when the
    /// caller should seal + create + retry.
    pub fn try_append_active(
        &self,
        payload: Bytes,
    ) -> Result<(AppendResult, ExtentId), StorageError> {
        let extent = self.extents.last().ok_or_else(|| {
            StorageError::Internal(format!("stream {}: no active extent", self.id))
        })?;
        let result = extent.append_inner(payload)?;
        Ok((result, extent.id))
    }

    /// Seal the active extent and create a new one with adaptive capacity.
    ///
    /// Must be called under exclusive access (`&mut self`, i.e., DashMap `get_mut`).
    pub fn seal_and_create_next(&mut self, reason: SealReason) -> Option<SealNotification> {
        let active_id = self.active_extent_id()?;
        let active_bytes_written = self.extents.last().map(|e| e.bytes_written()).unwrap_or(0);
        let (_, end_offset) = self.seal(active_id, None)?;

        // Compute next extent capacity based on seal reason.
        match reason {
            SealReason::ExtentFull => {
                // Scale up: double capacity (capped at max).
                self.next_extent_capacity =
                    (self.next_extent_capacity.saturating_mul(2)).min(self.max_extent_capacity);
            }
            SealReason::IdleShrink => {
                if active_bytes_written == 0 {
                    // Completely empty — jump to floor and free pool memory.
                    self.next_extent_capacity = self.min_extent_capacity;
                    self.extent_pool.clear();
                } else {
                    // Partially filled — gradual halve (pool kept, resized on use).
                    self.next_extent_capacity =
                        (self.next_extent_capacity / 2).max(self.min_extent_capacity);
                }
            }
        }

        // create_next_extent already calls evict_oldest_extents
        let (new_id, _) = self.create_next_extent();
        Some(SealNotification {
            sealed_extent_id: active_id,
            end_offset,
            new_extent_id: new_id,
            epoch: self.epoch,
            new_extent_capacity: self.extent_capacity,
        })
    }

    /// Whether this stream's active extent should be idle-shrunk.
    ///
    /// Returns `true` if:
    /// - The active extent has been alive longer than `threshold`
    /// - The active extent is less than 50% full
    /// - NOT already at min capacity with zero bytes written (nothing to reclaim)
    pub fn should_idle_shrink(&self, threshold: std::time::Duration) -> bool {
        let extent = match self.extents.last() {
            Some(e) if e.state() == ExtentState::Active => e,
            _ => return false,
        };

        // Already at min and empty — nothing to reclaim.
        if self.next_extent_capacity <= self.min_extent_capacity && extent.bytes_written() == 0 {
            return false;
        }

        // min_extent_capacity == 0 means adaptive sizing not configured (legacy).
        if self.min_extent_capacity == 0 {
            return false;
        }

        let created_at = match self.active_extent_created_at {
            Some(t) => t,
            None => return false,
        };

        if created_at.elapsed() < threshold {
            return false;
        }

        extent.bytes_written() < (extent.capacity() as u64) / 2
    }
}

/// Information about an extent that was sealed during an append.
#[derive(Debug, Clone)]
pub struct SealNotification {
    pub sealed_extent_id: ExtentId,
    pub end_offset: u64,
    pub new_extent_id: ExtentId,
    pub epoch: Epoch,
    /// The actual capacity of the newly created extent (adaptive sizing).
    pub new_extent_capacity: u32,
}

impl std::fmt::Debug for Stream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Stream")
            .field("id", &self.id)
            .field("extents", &self.extents)
            .field("epoch", &self.epoch)
            .field("next_extent_id", &self.next_extent_id)
            .field("extent_capacity", &self.extent_capacity)
            .field("min_extent_capacity", &self.min_extent_capacity)
            .field("max_extent_capacity", &self.max_extent_capacity)
            .field("next_extent_capacity", &self.next_extent_capacity)
            .field("max_extents", &self.max_extents)
            .field("in_flight", &self.in_flight.load(Ordering::Relaxed))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::config::DEFAULT_EXTENT_CAPACITY;

    /// Helper: create a stream with one active extent (simulating RegisterExtent from SM).
    fn new_stream_with_extent(id: StreamId) -> Stream {
        let mut stream = Stream::new(id);
        stream.register_extent_simple(ExtentId(0), Offset(0), DEFAULT_EXTENT_CAPACITY, Epoch(0));
        stream
    }

    #[test]
    fn basic_append_and_read() {
        let stream = new_stream_with_extent(StreamId(1));
        let extent_id = ExtentId(0);
        let r0 = stream
            .append(extent_id, Bytes::from_static(b"msg0"))
            .unwrap();
        let r1 = stream
            .append(extent_id, Bytes::from_static(b"msg1"))
            .unwrap();
        let r2 = stream
            .append(extent_id, Bytes::from_static(b"msg2"))
            .unwrap();

        assert_eq!(r0.offset, Offset(0));
        assert_eq!(r1.offset, Offset(1));
        assert_eq!(r2.offset, Offset(2));
        assert_eq!(stream.max_offset(), Offset(3));

        // Read all 3 from offset 0.
        let msgs = stream.read(extent_id, Offset(0), 3).unwrap();
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0], Bytes::from_static(b"msg0"));
        assert_eq!(msgs[2], Bytes::from_static(b"msg2"));

        // Random access: read msg1 directly via its offset.
        let msgs = stream.read(extent_id, r1.offset, 1).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0], Bytes::from_static(b"msg1"));
    }

    #[test]
    fn read_from_offset() {
        let stream = new_stream_with_extent(StreamId(1));
        let extent_id = ExtentId(0);
        let mut results = Vec::new();
        for i in 0..10 {
            results.push(
                stream
                    .append(extent_id, Bytes::from(format!("msg{i}")))
                    .unwrap(),
            );
        }

        // Read 3 messages starting at offset 5.
        let r5 = &results[5];
        let msgs = stream.read(extent_id, r5.offset, 3).unwrap();
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0], Bytes::from("msg5"));
        assert_eq!(msgs[1], Bytes::from("msg6"));
        assert_eq!(msgs[2], Bytes::from("msg7"));
    }

    #[test]
    fn read_beyond_end_returns_available() {
        let stream = new_stream_with_extent(StreamId(1));
        let extent_id = ExtentId(0);
        let r = stream
            .append(extent_id, Bytes::from_static(b"only"))
            .unwrap();

        let msgs = stream.read(extent_id, r.offset, 100).unwrap();
        assert_eq!(msgs.len(), 1);
    }

    #[test]
    fn read_empty_stream() {
        let stream = Stream::new(StreamId(1));
        assert_eq!(stream.max_offset(), Offset(0));

        // Stream with no extents: read returns error (extent not found).
        let result = stream.read(ExtentId(0), Offset(0), 10);
        assert!(result.is_err());
    }

    #[test]
    fn empty_stream_properties() {
        let stream = Stream::new(StreamId(1));
        assert_eq!(stream.max_offset(), Offset(0));
        assert!(!stream.is_mutable());
        assert_eq!(stream.active_extent_id(), None);
        assert!(
            stream
                .append(ExtentId(0), Bytes::from_static(b"fail"))
                .is_err()
        );
    }

    #[test]
    fn seal_and_new() {
        let mut stream = new_stream_with_extent(StreamId(1));
        let first_extent_id = ExtentId(0);
        // Append 3 messages to first extent.
        for i in 0..3 {
            stream
                .append(first_extent_id, Bytes::from(format!("msg{i}")))
                .unwrap();
        }
        assert_eq!(stream.max_offset(), Offset(3));

        // Seal active extent.
        let (start_offset, end_offset) = stream.seal(first_extent_id, None).unwrap();
        assert_eq!(start_offset, 0);
        assert_eq!(end_offset, 3);

        // After seal, stream has no active extent until register_extent.
        assert!(!stream.is_mutable());

        // Register a new extent (simulating SM sending RegisterExtent).
        let second_extent_id = ExtentId(1);
        stream.register_extent_simple(
            second_extent_id,
            Offset(3),
            DEFAULT_EXTENT_CAPACITY,
            Epoch(0),
        );
        assert!(stream.is_mutable());
        assert_eq!(stream.max_offset(), Offset(3)); // new extent is empty

        // Append to the new extent.
        let r = stream
            .append(second_extent_id, Bytes::from_static(b"after-seal"))
            .unwrap();
        assert_eq!(r.offset, Offset(3));
        assert_eq!(r.byte_pos, 0); // new extent, byte_pos starts at 0
        assert_eq!(stream.max_offset(), Offset(4));

        // Read from the new extent.
        let msgs = stream.read(second_extent_id, r.offset, 1).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0], Bytes::from_static(b"after-seal"));
    }

    #[test]
    fn seal_already_sealed_returns_none() {
        let mut stream = new_stream_with_extent(StreamId(1));
        let first_extent_id = ExtentId(0);
        let r = stream
            .append(first_extent_id, Bytes::from_static(b"a"))
            .unwrap();
        assert_eq!(r.offset, Offset(0));
        stream.seal(first_extent_id, None); // seals extent with 1 msg
        assert_eq!(stream.seal(first_extent_id, None), None); // already sealed, returns None

        // Register a new extent and append.
        let second_extent_id = ExtentId(1);
        stream.register_extent_simple(
            second_extent_id,
            Offset(1),
            DEFAULT_EXTENT_CAPACITY,
            Epoch(0),
        );
        let r = stream
            .append(second_extent_id, Bytes::from_static(b"b"))
            .unwrap();
        assert_eq!(r.offset, Offset(1));
        assert_eq!(stream.max_offset(), Offset(2));
    }

    #[test]
    fn evict_oldest_sealed_extents() {
        let mut stream = Stream::new(StreamId(1));
        stream.set_max_extents(2);

        // Register extent 0 and append a message.
        stream.register_extent_simple(ExtentId(0), Offset(0), DEFAULT_EXTENT_CAPACITY, Epoch(0));
        stream
            .append(ExtentId(0), Bytes::from_static(b"msg0"))
            .unwrap();

        // Seal extent 0, register extent 1.
        stream.seal(ExtentId(0), None);
        stream.register_extent_simple(ExtentId(1), Offset(1), DEFAULT_EXTENT_CAPACITY, Epoch(0));
        // 2 extents (sealed + active) — at limit, no eviction.
        assert!(stream.find_extent(ExtentId(0)).is_some());
        assert!(stream.find_extent(ExtentId(1)).is_some());

        // Seal extent 1, register extent 2 — now 3 extents, should evict extent 0.
        stream
            .append(ExtentId(1), Bytes::from_static(b"msg1"))
            .unwrap();
        stream.seal(ExtentId(1), None);
        stream.register_extent_simple(ExtentId(2), Offset(2), DEFAULT_EXTENT_CAPACITY, Epoch(0));
        assert!(
            stream.find_extent(ExtentId(0)).is_none(),
            "extent 0 should be evicted"
        );
        assert!(stream.find_extent(ExtentId(1)).is_some());
        assert!(stream.find_extent(ExtentId(2)).is_some());
    }

    #[test]
    fn evict_via_create_next_extent() {
        let mut stream = Stream::new(StreamId(1));
        stream.set_max_extents(2);

        stream.register_extent_simple(ExtentId(0), Offset(0), DEFAULT_EXTENT_CAPACITY, Epoch(0));
        stream
            .append(ExtentId(0), Bytes::from_static(b"a"))
            .unwrap();

        // seal_and_create_next triggers eviction via create_next_extent.
        let notif = stream.seal_and_create_next(SealReason::ExtentFull).unwrap();
        // 2 extents: sealed extent 0 + new active extent 1. At limit.
        assert!(stream.find_extent(ExtentId(0)).is_some());
        assert!(stream.find_extent(notif.new_extent_id).is_some());

        // Append to new extent, then seal_and_create again.
        stream
            .append(notif.new_extent_id, Bytes::from_static(b"b"))
            .unwrap();
        let notif2 = stream.seal_and_create_next(SealReason::ExtentFull).unwrap();
        // 3 would exceed limit — extent 0 should be evicted.
        assert!(
            stream.find_extent(ExtentId(0)).is_none(),
            "extent 0 should be evicted"
        );
        assert!(stream.find_extent(notif.new_extent_id).is_some());
        assert!(stream.find_extent(notif2.new_extent_id).is_some());
    }

    #[test]
    fn no_eviction_when_limit_is_zero() {
        let mut stream = Stream::new(StreamId(1));
        // max_extents = 0 (default) means no limit.

        for i in 0..5u32 {
            stream.register_extent_simple(
                ExtentId(i),
                Offset(i as u64),
                DEFAULT_EXTENT_CAPACITY,
                Epoch(0),
            );
            stream
                .append(ExtentId(i), Bytes::from_static(b"x"))
                .unwrap();
            stream.seal(ExtentId(i), None);
        }
        // Register one more active extent.
        stream.register_extent_simple(ExtentId(5), Offset(5), DEFAULT_EXTENT_CAPACITY, Epoch(0));

        // All 6 extents should still be present.
        for i in 0..=5 {
            assert!(stream.find_extent(ExtentId(i)).is_some());
        }
    }

    #[test]
    fn evict_unsealed_extents_secondary_scenario() {
        // On secondaries, old extents may not be sealed (autonomous extent-full
        // only seals on the Primary). Eviction should still work.
        let mut stream = Stream::new(StreamId(1));
        stream.set_max_extents(2);

        // Register extent 0 (not sealed — simulating secondary).
        stream.register_extent_simple(ExtentId(0), Offset(0), DEFAULT_EXTENT_CAPACITY, Epoch(0));

        // Register extent 1 — 2 extents, at limit.
        stream.register_extent_simple(ExtentId(1), Offset(100), DEFAULT_EXTENT_CAPACITY, Epoch(0));
        assert!(stream.find_extent(ExtentId(0)).is_some());
        assert!(stream.find_extent(ExtentId(1)).is_some());

        // Register extent 2 — 3 extents, exceeds limit.
        // Extent 0 is NOT sealed, but should still be evicted.
        stream.register_extent_simple(ExtentId(2), Offset(200), DEFAULT_EXTENT_CAPACITY, Epoch(0));
        assert!(
            stream.find_extent(ExtentId(0)).is_none(),
            "unsealed extent 0 should be evicted"
        );
        assert!(stream.find_extent(ExtentId(1)).is_some());
        assert!(stream.find_extent(ExtentId(2)).is_some());
    }

    // ── Adaptive capacity tests ─────────────────────────────────────────

    #[test]
    fn adaptive_growth_on_extent_full() {
        let min_cap: u32 = 256; // tiny for testing
        let max_cap: u32 = 2048;
        let mut stream = Stream::new(StreamId(1));
        stream.set_max_extents(4);
        stream.register_extent(ExtentId(0), Offset(0), min_cap, Epoch(0), min_cap, max_cap);

        // Fill extent to trigger extent-full on next append.
        // Each record = 4 bytes header + payload. With 256 bytes, we can fit ~25 records of 6 bytes.
        let mut offset = 0u64;
        loop {
            match stream.append(ExtentId(0), Bytes::from_static(b"xx")) {
                Ok(r) => offset = r.offset.0 + 1,
                Err(StorageError::ExtentFull(_)) => break,
                Err(e) => panic!("unexpected error: {e}"),
            }
        }
        assert!(offset > 0);

        // Seal with ExtentFull reason — should double capacity.
        let notif = stream.seal_and_create_next(SealReason::ExtentFull).unwrap();
        assert_eq!(notif.new_extent_capacity, min_cap * 2);

        // Fill again and seal — should double again.
        loop {
            match stream.append(notif.new_extent_id, Bytes::from_static(b"xx")) {
                Ok(_) => {}
                Err(StorageError::ExtentFull(_)) => break,
                Err(e) => panic!("unexpected error: {e}"),
            }
        }
        let notif2 = stream.seal_and_create_next(SealReason::ExtentFull).unwrap();
        assert_eq!(notif2.new_extent_capacity, min_cap * 4);
    }

    #[test]
    fn adaptive_cap_at_max() {
        let min_cap: u32 = 256;
        let max_cap: u32 = 512;
        let mut stream = Stream::new(StreamId(1));
        stream.set_max_extents(4);
        stream.register_extent(ExtentId(0), Offset(0), min_cap, Epoch(0), min_cap, max_cap);

        // Fill and seal with ExtentFull — doubles to 512.
        loop {
            match stream.append(ExtentId(0), Bytes::from_static(b"xx")) {
                Ok(_) => {}
                Err(StorageError::ExtentFull(_)) => break,
                Err(e) => panic!("unexpected error: {e}"),
            }
        }
        let notif = stream.seal_and_create_next(SealReason::ExtentFull).unwrap();
        assert_eq!(notif.new_extent_capacity, max_cap);

        // Fill and seal again — should stay at max.
        loop {
            match stream.append(notif.new_extent_id, Bytes::from_static(b"xx")) {
                Ok(_) => {}
                Err(StorageError::ExtentFull(_)) => break,
                Err(e) => panic!("unexpected error: {e}"),
            }
        }
        let notif2 = stream.seal_and_create_next(SealReason::ExtentFull).unwrap();
        assert_eq!(notif2.new_extent_capacity, max_cap);
    }

    #[test]
    fn adaptive_shrink_on_idle_partial() {
        let min_cap: u32 = 256;
        let max_cap: u32 = 2048;
        let mut stream = Stream::new(StreamId(1));
        stream.set_max_extents(4);
        // Start at 1024 (mid-range).
        stream.register_extent(ExtentId(0), Offset(0), 1024, Epoch(0), min_cap, max_cap);

        // Write a small amount (less than half).
        stream
            .append(ExtentId(0), Bytes::from_static(b"tiny"))
            .unwrap();

        // IdleShrink with partial fill — should halve.
        let notif = stream.seal_and_create_next(SealReason::IdleShrink).unwrap();
        assert_eq!(notif.new_extent_capacity, 512);
    }

    #[test]
    fn adaptive_shrink_empty_jumps_to_min() {
        let min_cap: u32 = 256;
        let max_cap: u32 = 2048;
        let mut stream = Stream::new(StreamId(1));
        stream.set_max_extents(4);
        stream.register_extent(ExtentId(0), Offset(0), 1024, Epoch(0), min_cap, max_cap);
        // Don't write anything — extent is completely empty.

        let notif = stream.seal_and_create_next(SealReason::IdleShrink).unwrap();
        assert_eq!(notif.new_extent_capacity, min_cap);
    }

    #[test]
    fn adaptive_shrink_noop_at_min_empty() {
        let min_cap: u32 = 256;
        let max_cap: u32 = 2048;
        let mut stream = Stream::new(StreamId(1));
        stream.set_max_extents(4);
        stream.register_extent(ExtentId(0), Offset(0), min_cap, Epoch(0), min_cap, max_cap);
        // Artificially set active_extent_created_at to the past.
        stream.active_extent_created_at =
            Some(Instant::now() - std::time::Duration::from_secs(600));

        // Already at min + empty → should_idle_shrink returns false.
        assert!(!stream.should_idle_shrink(std::time::Duration::from_secs(300)));
    }

    #[test]
    fn adaptive_floor_at_min() {
        let min_cap: u32 = 256;
        let max_cap: u32 = 2048;
        let mut stream = Stream::new(StreamId(1));
        stream.set_max_extents(4);
        stream.register_extent(ExtentId(0), Offset(0), min_cap, Epoch(0), min_cap, max_cap);

        // Write a small amount.
        stream
            .append(ExtentId(0), Bytes::from_static(b"x"))
            .unwrap();

        // IdleShrink — already at min, should stay at min.
        let notif = stream.seal_and_create_next(SealReason::IdleShrink).unwrap();
        assert_eq!(notif.new_extent_capacity, min_cap);
    }

    #[test]
    fn extent_pool_resize_on_growth() {
        let min_cap: u32 = 256;
        let max_cap: u32 = 2048;
        let mut stream = Stream::new(StreamId(1));
        stream.set_max_extents(3);
        stream.register_extent(ExtentId(0), Offset(0), min_cap, Epoch(0), min_cap, max_cap);

        // Fill and seal — creates extent at 512, evicts extent 0 into pool (capacity 256).
        loop {
            match stream.append(ExtentId(0), Bytes::from_static(b"xx")) {
                Ok(_) => {}
                Err(StorageError::ExtentFull(_)) => break,
                Err(e) => panic!("unexpected error: {e}"),
            }
        }
        let notif = stream.seal_and_create_next(SealReason::ExtentFull).unwrap();
        assert_eq!(notif.new_extent_capacity, min_cap * 2);

        // The new extent should have capacity 512 regardless of pool recycling.
        let new_ext = stream.find_extent(notif.new_extent_id).unwrap();
        assert_eq!(new_ext.capacity(), min_cap * 2);
    }
}
