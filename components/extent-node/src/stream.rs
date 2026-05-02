use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use arc_swap::ArcSwap;
use bytes::Bytes;
use common::config::{DEFAULT_CACHE_EPOCHS, DEFAULT_EPOCH_CAPACITY};
use common::errors::{InternalSnafu, StorageError};
use common::types::{Epoch, EpochState, ExtentId, Offset, StorageClass, StreamId};
use crossbeam_channel::{Receiver, Sender, unbounded};
use parking_lot::RwLock;
use rpc::frame::Frame;
use smallvec::SmallVec;
use tokio::sync::mpsc;
use tracing::error;

use crate::ack_queue::AckQueue;
use crate::arena::ArenaIdGenerator;
use crate::store::AppendJob;
use crate::stream_epoch::{AppendResult, StreamEpoch};

/// Reason for sealing the active extent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SealReason {
    /// Epoch arena is full — client must reopen on a new epoch.
    EpochFull,
}

/// Mutable state protected by `RwLock`. Grouped here so that a single
/// lock acquisition covers all fields that need coordinated mutation.
struct StreamInner {
    // epochs: Vec<StreamEpoch>,   // REMOVED — now on Stream as ArcSwap<SmallVec<...>>
    /// Fixed capacity for every epoch on this stream. Set at register_epoch time
    /// from config and never changed.
    epoch_capacity: u32,

    /// Maximum number of epochs to retain. 0 = no limit.
    /// When exceeded, the oldest sealed epochs are dropped to free memory.
    max_epochs: usize,

    /// Cached per-secondary Sender clones (Primary only).
    /// Populated at RegisterEpoch time from DownstreamPool.
    /// Vec since RF is small (1-3); iteration is the hot path.
    downstream_txs: Vec<mpsc::Sender<Frame>>,

    /// Storage class for sealed extents: S3 or Memory.
    storage_class: StorageClass,
}

/// A stream: an ordered, append-only sequence of messages backed by a list of extents.
///
/// Thread-safe (`Send + Sync`). All public methods take `&self`:
/// - Hot-path reads (`epoch`, `in_flight`) use lock-free atomics.
/// - Epoch reads (`append`, `read`, `try_append_active`, ...) use `self.epochs.load()` (lock-free).
/// - Mutations of inner fields (`seal`, `register_epoch`, etc.) use `inner.write()`.
///
/// The active (last) extent is a lock-free arena. Multiple concurrent appenders
/// can write to it without any external mutex -- offset assignment, payload copy,
/// and commit advancement are all handled by the Extent's internal atomics.
///
/// Each extent maintains an internal index mapping sequence numbers to byte
/// positions (compressed u32 pointers). The index is populated atomically during
/// append and used during read to resolve offsets without client-side byte_pos.
///
/// Pipelined group commit is coordinated at the stream level via `in_flight`,
/// `job_tx`, and `job_rx`. This ensures extent transitions (seal + create) are
/// handled transparently by the stream-level leader without callers needing to
/// know about individual extent boundaries.
pub struct Stream {
    pub id: StreamId,

    /// Current epoch assigned by Stream Manager. AtomicU32 for lock-free
    /// hot-path reads; updated via `register_epoch` / `set_epoch`.
    epoch: AtomicU32,

    /// Leader election counter for pipelined group commit (stream-level).
    /// 0 = idle. The leader owns the entire stream, handling extent transitions inline.
    in_flight: AtomicU64,

    /// Channel for followers to submit append jobs to the active writer.
    job_tx: Sender<AppendJob>,
    job_rx: Receiver<AppendJob>,

    /// Per-stream ACK queue for quorum-based replication (Primary only).
    /// `None` on Secondaries. Initialized once at RegisterEpoch time via `OnceLock`.
    /// Has its own internal Mutex — lives outside `inner`'s RwLock to avoid
    /// forcing the append hot path into a write lock.
    ack_queue: OnceLock<AckQueue>,

    /// Tracks epochs with an in-progress S3 flush (both Primary and DR paths).
    /// Used to deduplicate concurrent flush requests for the same epoch.
    /// Lock-free via papaya::HashMap.
    flush_in_progress: papaya::HashMap<ExtentId, ()>,

    /// All StreamEpochs this EN currently tracks for this stream,
    /// sorted by epoch number ascending. Copy-on-write via ArcSwap:
    /// readers take a single Arc load (no lock); writers clone the
    /// SmallVec, mutate, and `store()`. Writes happen only on epoch
    /// register / arena roll / epoch death — rare.
    epochs: ArcSwap<SmallVec<[Arc<StreamEpoch>; 4]>>,

    /// Mints ArenaIds for epochs registered directly (register_epoch path).
    /// Shared with the pool so that all extents for this stream draw from the
    /// same monotonic counter.
    arena_ids: Arc<ArenaIdGenerator>,

    /// Mutable state protected by RwLock.
    inner: RwLock<StreamInner>,
}

// Compile-time assertion that Stream is Send + Sync.
const _: () = {
    fn _assert_send<T: Send>() {}
    fn _assert_sync<T: Sync>() {}
    fn _assert_all() {
        _assert_send::<Stream>();
        _assert_sync::<Stream>();
    }
};

impl Stream {
    // ── Construction ────────────────────────────────────────────────────

    /// Create a new stream with no extents. Extents are added via `register_epoch()`.
    pub(crate) fn new(id: StreamId, arena_ids: Arc<ArenaIdGenerator>) -> Self {
        let (job_tx, job_rx) = unbounded();
        Self {
            id,
            epoch: AtomicU32::new(0),
            in_flight: AtomicU64::new(0),
            job_tx,
            job_rx,
            ack_queue: OnceLock::new(),
            flush_in_progress: papaya::HashMap::new(),
            epochs: ArcSwap::from_pointee(SmallVec::new()),
            arena_ids,
            inner: RwLock::new(StreamInner {
                epoch_capacity: DEFAULT_EPOCH_CAPACITY,
                max_epochs: DEFAULT_CACHE_EPOCHS as usize,
                downstream_txs: Vec::new(),
                storage_class: StorageClass::S3,
            }),
        }
    }

    // ── Epoch vec helpers (lock-free reads, CoW writes) ──────────────

    /// Transitional lookup by legacy extent_id for internal paths that still
    /// carry ExtentId until StreamEpoch.id is removed.
    fn find_epoch_by_extent_id(&self, extent_id: ExtentId) -> Option<Arc<StreamEpoch>> {
        self.epochs
            .load()
            .iter()
            .find(|e| e.id == extent_id)
            .cloned()
    }

    fn find_epoch_by_number(&self, epoch: Epoch) -> Option<Arc<StreamEpoch>> {
        self.epochs
            .load()
            .iter()
            .find(|e| e.epoch == epoch)
            .cloned()
    }

    /// Bridge helper (pre-P3 Phase 4): look up the `StreamEpoch` covering a
    /// given offset. Introduced when the wire protocol dropped `extent_id`
    /// from read/seal/flush handlers; removed once StreamEpoch.id
    /// is rewritten to key on `Epoch`.
    ///
    /// Find the extent whose range covers `offset`. Used by handlers that
    /// receive an offset on the wire without an extent_id.
    pub fn find_extent_for_offset(&self, offset: Offset) -> Option<ExtentId> {
        self.epochs
            .load()
            .iter()
            .find(|e| offset.0 >= e.start_offset.0 && offset.0 < e.next_offset().0)
            .map(|e| e.id)
    }

    /// The currently-active (last, highest-epoch) StreamEpoch. None if none
    /// registered yet.
    fn active_epoch_ref(&self) -> Option<Arc<StreamEpoch>> {
        self.epochs.load().last().cloned()
    }

    /// Insert a new epoch, keeping the vec sorted by epoch number.
    /// RCU: clones the current SmallVec, pushes + sorts, stores.
    fn insert_epoch(&self, new_ep: Arc<StreamEpoch>) {
        self.epochs.rcu(|current| {
            let mut next: SmallVec<[Arc<StreamEpoch>; 4]> = (**current).clone();
            next.push(new_ep.clone());
            next.sort_by_key(|e| e.epoch.0);
            next
        });
    }

    /// Remove the epoch with matching epoch number.
    #[allow(dead_code)]
    fn remove_epoch_by_number(&self, epoch: Epoch) {
        self.epochs.rcu(|current| {
            let mut next: SmallVec<[Arc<StreamEpoch>; 4]> = (**current).clone();
            next.retain(|e| e.epoch != epoch);
            next
        });
    }

    /// Remove the head epoch (the lowest-epoch StreamEpoch). Used by
    /// the eviction loop. No-op if the vec is empty.
    fn remove_head_epoch(&self) {
        self.epochs.rcu(|current| {
            let mut next: SmallVec<[Arc<StreamEpoch>; 4]> = (**current).clone();
            if !next.is_empty() {
                next.remove(0);
            }
            next
        });
    }

    // ── Epoch-list mutation methods (moved from StreamInner) ──────────

    /// Find-by-id helper used by seal. Returns a clone of the last
    /// epoch's Arc if and only if its id matches AND it's still Active.
    fn seal_epoch_by_id(
        &self,
        extent_id: ExtentId,
        committed_offset: Option<u64>,
    ) -> Option<(u64, u64)> {
        let snap = self.epochs.load();
        let last = snap.last()?;
        if last.id != extent_id {
            return None;
        }
        if last.state() == EpochState::Sealed {
            return None;
        }
        let start_offset = last.start_offset.0;
        let end_offset = last.seal(committed_offset);
        Some((start_offset, end_offset))
    }

    /// Evict oldest epochs when count exceeds max_epochs.
    ///
    /// - S3 streams: only flushed epochs are eligible; oldest-first stops at
    ///   the first un-flushed head.
    /// - Memory streams: all but the active (last) epoch are eligible.
    /// - max_epochs == 0: unlimited; log a warn if the count grows past 4.
    fn evict_oldest_epochs(&self) {
        let (max_epochs, is_s3) = {
            let inner = self.inner.read();
            (inner.max_epochs, inner.storage_class == StorageClass::S3)
        };
        if max_epochs == 0 {
            let snap = self.epochs.load();
            if snap.len() > 4 {
                tracing::warn!(
                    "stream {} has {} epochs but max_epochs=0 (no eviction); \
                     memory will grow unbounded",
                    self.id,
                    snap.len(),
                );
            }
            return;
        }
        // CoW loop: check the head, decide whether to drop it, repeat.
        loop {
            let snap = self.epochs.load();
            if snap.len() <= max_epochs.max(1) {
                break;
            }
            // Never evict the active (last) epoch.
            if snap.len() <= 1 {
                break;
            }
            let head = snap.first().expect("len checked");
            if is_s3 && !head.is_flushed() {
                break;
            }
            // Drop the snap before RCU so we don't hold an Arc to the victim
            // across the RCU loop.
            drop(snap);
            self.remove_head_epoch();
        }
    }

    // ── Lock-free accessors (no lock needed) ───────────────────────────

    /// Current epoch of this stream (lock-free atomic load).
    pub fn epoch(&self) -> Epoch {
        Epoch(self.epoch.load(Ordering::Acquire))
    }

    /// Update the epoch (e.g., when RegisterEpoch arrives for an already lazily-created extent).
    pub fn set_epoch(&self, epoch: Epoch) {
        self.epoch.store(epoch.0, Ordering::Release);
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

    /// Get the AckQueue for this stream (Primary only). Returns `None` on Secondaries.
    pub(crate) fn ack_queue(&self) -> Option<&AckQueue> {
        self.ack_queue.get()
    }

    /// Initialize the AckQueue for this stream (Primary only, idempotent).
    /// Returns a reference to the (possibly pre-existing) AckQueue.
    pub(crate) fn init_ack_queue(&self, required_acks: u32, timeout: Duration) -> &AckQueue {
        self.ack_queue
            .get_or_init(|| AckQueue::with_timeout(required_acks, timeout))
    }

    // ── Read-lock methods ──────────────────────────────────────────────

    /// Append a message to the specified extent. Returns the assigned
    /// offset and byte position within the extent arena.
    ///
    /// Lock-free on the hot path: reads the epoch snapshot via ArcSwap.
    /// The byte_pos is recorded in the extent's internal index automatically.
    ///
    /// Returns an error if the extent doesn't exist.
    pub fn append(
        &self,
        extent_id: ExtentId,
        payload: Bytes,
    ) -> Result<AppendResult, StorageError> {
        let extent = self.find_epoch_by_extent_id(extent_id).ok_or_else(|| {
            InternalSnafu {
                message: format!("stream {}: extent {} not found", self.id, extent_id),
            }
            .build()
        })?;
        extent.append(payload)
    }

    /// Append to the active extent (single-writer, called by stream-level leader).
    ///
    /// Returns `Ok((result, extent_id))` on success, or `Err(ExtentFull)` when the
    /// caller should seal + create + retry.
    pub fn try_append_active(
        &self,
        payload: Bytes,
    ) -> Result<(AppendResult, ExtentId), StorageError> {
        let extent = self.active_epoch_ref().ok_or_else(|| {
            InternalSnafu {
                message: format!("stream {}: no active extent", self.id),
            }
            .build()
        })?;
        let result = extent.append_inner(payload)?;
        Ok((result, extent.id))
    }

    /// Replicate a record; the secondary derives byte_pos from its own cursor.
    ///
    /// Delegates to `Extent::replicate()` for deterministic replication.
    /// Lock-free on the hot path via ArcSwap epoch snapshot.
    pub fn replicate(
        &self,
        extent_id: ExtentId,
        offset: Offset,
        payload: Bytes,
    ) -> Result<AppendResult, StorageError> {
        let extent = self.find_epoch_by_extent_id(extent_id).ok_or_else(|| {
            error!("Stream: {}, Extent: {} is not found", self.id, extent_id);
            InternalSnafu {
                message: format!("stream {}: extent {} not found", self.id, extent_id),
            }
            .build()
        })?;
        extent.replicate(offset, payload)
    }

    /// Read `count` messages starting from the given logical `offset` within
    /// the specified extent.
    ///
    /// The server resolves `offset -> byte_pos` internally via the index stream,
    /// so callers only need to provide the logical offset. This keeps byte_pos
    /// as an internal implementation detail invisible to clients.
    pub fn read(
        &self,
        extent_id: ExtentId,
        offset: Offset,
        count: u32,
    ) -> Result<Vec<Bytes>, StorageError> {
        let extent = self.find_epoch_by_extent_id(extent_id).ok_or_else(|| {
            InternalSnafu {
                message: format!("stream {}: extent {} not found", self.id, extent_id),
            }
            .build()
        })?;
        // Check offset is within this extent's range.
        if offset.0 < extent.start_offset.0 || offset.0 >= extent.next_offset().0 {
            return Ok(Vec::new());
        }

        let seq = offset.0 - extent.start_offset.0;
        let byte_pos = extent.index_lookup(seq).ok_or_else(|| {
            InternalSnafu {
                message: format!("index lookup failed for offset {}", offset.0),
            }
            .build()
        })?;
        extent.read(byte_pos, count)
    }

    /// Whether this stream can accept appends (its last epoch is active/unsealed).
    pub fn is_mutable(&self) -> bool {
        self.active_epoch_ref()
            .map(|e| e.state() == EpochState::Active)
            .unwrap_or(false)
    }

    /// The active epoch, or None if no epochs are registered.
    pub fn active_epoch(&self) -> Option<Epoch> {
        self.active_epoch_ref().map(|e| e.epoch)
    }

    /// Transitional lookup for callers that still need the legacy ExtentId while
    /// StreamEpoch.id exists.
    pub(crate) fn extent_id_for_epoch(&self, epoch: Epoch) -> Option<ExtentId> {
        self.find_epoch_by_number(epoch).map(|e| e.id)
    }

    /// The sealed offset range for the given epoch.
    /// Returns `(start_offset, end_offset)` or None if the epoch is not sealed.
    pub fn sealed_epoch(&self, epoch: Epoch) -> Option<(Offset, Offset)> {
        self.find_epoch_by_number(epoch)
            .filter(|e| e.is_sealed())
            .map(|e| (e.start_offset, Offset(e.start_offset.0 + e.message_count())))
    }

    /// The maximum offset (exclusive): the next offset that would be assigned.
    /// Returns `Offset(0)` if the stream has no extents.
    pub fn max_offset(&self) -> Offset {
        self.active_epoch_ref()
            .map(|e| e.next_offset())
            .unwrap_or(Offset(0))
    }

    /// Closure-based access to an epoch. Returns `None` if the epoch does not
    /// exist; otherwise applies `f` to the StreamEpoch and returns `Some(R)`.
    pub fn with_epoch<F, R>(&self, epoch: Epoch, f: F) -> Option<R>
    where
        F: FnOnce(&StreamEpoch) -> R,
    {
        self.find_epoch_by_number(epoch).map(|ep| f(&ep))
    }

    /// Transitional lookup by legacy extent id while StreamEpoch.id still exists.
    pub(crate) fn with_epoch_by_extent_id<F, R>(&self, extent_id: ExtentId, f: F) -> Option<R>
    where
        F: FnOnce(&StreamEpoch) -> R,
    {
        self.find_epoch_by_extent_id(extent_id).map(|ep| f(&ep))
    }

    /// Report this stream's state for the specified epoch.
    pub fn report_epoch(&self, epoch: Epoch) -> Option<(Epoch, Offset, Offset, EpochState)> {
        self.find_epoch_by_number(epoch).map(|e| {
            let end_offset = if e.is_sealed() {
                Offset(e.start_offset.0 + e.message_count())
            } else {
                Offset(0) // active epoch, end_offset not yet determined
            };
            (e.epoch, e.start_offset, end_offset, e.state())
        })
    }

    /// The end_offset of the specified sealed epoch.
    /// Used by handle_seal to return committed offset idempotently when the
    /// epoch was already sealed.
    /// Returns 0 if the epoch is not found or not sealed.
    pub fn sealed_end_offset(&self, epoch: Epoch) -> u64 {
        if let Some(extent) = self.find_epoch_by_number(epoch)
            && extent.is_sealed()
        {
            return extent.start_offset.0 + extent.message_count();
        }
        0
    }

    /// Whether this stream has secondary senders (i.e., is Primary with RF >= 2).
    pub(crate) fn has_secondaries(&self) -> bool {
        let inner = self.inner.read();
        !inner.downstream_txs.is_empty()
    }

    /// Push a frame to all secondary channels. Fire-and-forget.
    ///
    /// Called inline from the leader's append path while `in_flight > 0`,
    /// guaranteeing FIFO ordering — no frame from a subsequent leader can
    /// appear in the channel before frames from the current leader.
    pub(crate) fn send_forward(&self, frame: Frame) {
        let inner = self.inner.read();
        let n = inner.downstream_txs.len();
        if n == 0 {
            return;
        }
        // Send clones to all but the last, move the original to the last.
        for tx in &inner.downstream_txs[..n - 1] {
            if let Err(mpsc::error::TrySendError::Full(_)) = tx.try_send(frame.clone()) {
                tracing::warn!(
                    "downstream channel full for stream {}, dropping forward frame",
                    self.id,
                );
            }
        }
        if let Err(mpsc::error::TrySendError::Full(_)) = inner.downstream_txs[n - 1].try_send(frame)
        {
            tracing::warn!(
                "downstream channel full for stream {}, dropping forward frame",
                self.id,
            );
        }
    }

    /// Return the maximum number of epochs to retain (0 = no limit).
    pub fn max_epochs(&self) -> usize {
        self.inner.read().max_epochs
    }

    /// Return the storage class for this stream.
    pub fn storage_class(&self) -> StorageClass {
        self.inner.read().storage_class
    }

    /// Try to mark an extent as flush-in-progress. Returns `true` if inserted
    /// (caller should proceed with flush), `false` if already in progress (dedup).
    pub fn start_flush(&self, extent_id: ExtentId) -> bool {
        let guard = self.flush_in_progress.pin();
        if guard.contains_key(&extent_id) {
            false
        } else {
            guard.insert(extent_id, ());
            true
        }
    }

    /// Clear the flush-in-progress marker for an extent (flush completed or failed).
    pub fn finish_flush(&self, extent_id: ExtentId) {
        self.flush_in_progress.pin().remove(&extent_id);
    }

    // ── Write-lock methods ─────────────────────────────────────────────

    /// Set the maximum number of epochs to retain per stream.
    /// 0 means no limit (default).
    pub fn set_max_epochs(&self, max: usize) {
        self.inner.write().max_epochs = max;
    }

    /// Set the storage class for this stream.
    pub fn set_storage_class(&self, class: StorageClass) {
        self.inner.write().storage_class = class;
    }

    /// Register a new epoch on this stream (called when SM sends RegisterEpoch
    /// or when a secondary receives ForwardInitEpoch).
    ///
    /// `epoch_capacity` is the arena size for this specific epoch.
    pub fn register_epoch(
        &self,
        id: ExtentId,
        start_offset: Offset,
        epoch: Epoch,
        epoch_capacity: u32,
    ) {
        self.epoch.store(epoch.0, Ordering::Release);
        {
            let mut inner = self.inner.write();
            inner.epoch_capacity = epoch_capacity;
        }
        let arena_id = self.arena_ids.next();
        let ep = Arc::new(StreamEpoch::with_capacity(
            id,
            self.id,
            start_offset,
            epoch_capacity,
            epoch,
            arena_id,
        ));
        self.insert_epoch(ep);
        self.evict_oldest_epochs();
    }

    /// Simplified register_epoch for tests.
    #[cfg(test)]
    pub fn register_epoch_simple(
        &self,
        id: ExtentId,
        start_offset: Offset,
        epoch_capacity: u32,
        epoch: Epoch,
    ) {
        self.register_epoch(id, start_offset, epoch, epoch_capacity);
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
    /// After seal, the stream has no active extent until SM sends a new `RegisterEpoch`.
    pub fn seal(&self, extent_id: ExtentId, committed_offset: Option<u64>) -> Option<(u64, u64)> {
        self.seal_epoch_by_id(extent_id, committed_offset)
    }

    /// Seal the active epoch without creating a successor.
    ///
    /// Returns `(sealed_epoch, end_offset)` if the active epoch was sealed, or `None`
    /// if no active epoch exists or it was already sealed.
    pub fn seal_current_epoch(&self) -> Option<(Epoch, Offset)> {
        let active = self.active_epoch_ref()?;
        let active_id = active.id;
        let epoch = active.epoch;
        drop(active);
        let (_, end_offset) = self.seal_epoch_by_id(active_id, None)?;
        Some((epoch, Offset(end_offset)))
    }

    /// Set cached downstream senders (Primary only, called at RegisterEpoch time).
    pub(crate) fn set_downstream_txs(&self, txs: Vec<mpsc::Sender<Frame>>) {
        self.inner.write().downstream_txs = txs;
    }
}

/// Information about an extent that was sealed during an append.
#[derive(Debug, Clone)]
pub struct SealNotification {
    pub sealed_extent_id: ExtentId,
    pub sealed_epoch: Epoch,
    pub end_offset: u64,
}

impl std::fmt::Debug for Stream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.read();
        let epochs_snap = self.epochs.load();
        f.debug_struct("Stream")
            .field("id", &self.id)
            .field("epochs", &*epochs_snap)
            .field("epoch", &self.epoch.load(Ordering::Relaxed))
            .field("epoch_capacity", &inner.epoch_capacity)
            .field("max_epochs", &inner.max_epochs)
            .field("in_flight", &self.in_flight.load(Ordering::Relaxed))
            .field("has_ack_queue", &self.ack_queue.get().is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::config::DEFAULT_EPOCH_CAPACITY;

    fn test_arena_ids() -> Arc<ArenaIdGenerator> {
        Arc::new(crate::arena::ArenaIdGenerator::new(1))
    }

    /// Helper: create a stream with one active extent (simulating RegisterEpoch from SM).
    fn new_stream_with_epoch(id: StreamId) -> Stream {
        let stream = Stream::new(id, test_arena_ids());
        stream.register_epoch_simple(ExtentId(0), Offset(0), DEFAULT_EPOCH_CAPACITY, Epoch(0));
        stream
    }

    #[test]
    fn basic_append_and_read() {
        let stream = new_stream_with_epoch(StreamId(1));
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
        let stream = new_stream_with_epoch(StreamId(1));
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
        let stream = new_stream_with_epoch(StreamId(1));
        let extent_id = ExtentId(0);
        let r = stream
            .append(extent_id, Bytes::from_static(b"only"))
            .unwrap();

        let msgs = stream.read(extent_id, r.offset, 100).unwrap();
        assert_eq!(msgs.len(), 1);
    }

    #[test]
    fn read_empty_stream() {
        let stream = Stream::new(StreamId(1), test_arena_ids());
        assert_eq!(stream.max_offset(), Offset(0));

        // Stream with no extents: read returns error (extent not found).
        let result = stream.read(ExtentId(0), Offset(0), 10);
        assert!(result.is_err());
    }

    #[test]
    fn empty_stream_properties() {
        let stream = Stream::new(StreamId(1), test_arena_ids());
        assert_eq!(stream.max_offset(), Offset(0));
        assert!(!stream.is_mutable());
        assert_eq!(stream.active_epoch(), None);
        assert!(
            stream
                .append(ExtentId(0), Bytes::from_static(b"fail"))
                .is_err()
        );
    }

    #[test]
    fn seal_and_new() {
        let stream = new_stream_with_epoch(StreamId(1));
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

        // After seal, stream has no active extent until register_epoch.
        assert!(!stream.is_mutable());

        // Register a new extent (simulating SM sending RegisterEpoch).
        let second_extent_id = ExtentId(1);
        stream.register_epoch_simple(
            second_extent_id,
            Offset(3),
            DEFAULT_EPOCH_CAPACITY,
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
    fn seal_current_epoch_returns_epoch_and_end_offset() {
        let stream = new_stream_with_epoch(StreamId(1));
        let first_extent_id = ExtentId(0);
        let r = stream
            .append(first_extent_id, Bytes::from_static(b"a"))
            .unwrap();
        assert_eq!(r.offset, Offset(0));

        assert_eq!(stream.seal_current_epoch(), Some((Epoch(0), Offset(1))));
        assert_eq!(stream.seal_current_epoch(), None); // already sealed, returns None
        assert!(!stream.is_mutable());
    }

    #[test]
    fn evict_oldest_sealed_extents() {
        let stream = Stream::new(StreamId(1), test_arena_ids());
        stream.set_storage_class(StorageClass::Memory);
        stream.set_max_epochs(2);

        // Register extent 0 and append a message.
        stream.register_epoch_simple(ExtentId(0), Offset(0), DEFAULT_EPOCH_CAPACITY, Epoch(0));
        stream
            .append(ExtentId(0), Bytes::from_static(b"msg0"))
            .unwrap();

        // Seal extent 0, register extent 1.
        stream.seal(ExtentId(0), None);
        stream.register_epoch_simple(ExtentId(1), Offset(1), DEFAULT_EPOCH_CAPACITY, Epoch(0));
        // 2 extents (sealed + active) — at limit, no eviction.
        assert!(stream.with_epoch_by_extent_id(ExtentId(0), |_| ()).is_some());
        assert!(stream.with_epoch_by_extent_id(ExtentId(1), |_| ()).is_some());

        // Seal extent 1, register extent 2 — now 3 extents, should evict extent 0.
        stream
            .append(ExtentId(1), Bytes::from_static(b"msg1"))
            .unwrap();
        stream.seal(ExtentId(1), None);
        stream.register_epoch_simple(ExtentId(2), Offset(2), DEFAULT_EPOCH_CAPACITY, Epoch(0));
        assert!(
            stream.with_epoch_by_extent_id(ExtentId(0), |_| ()).is_none(),
            "extent 0 should be evicted"
        );
        assert!(stream.with_epoch_by_extent_id(ExtentId(1), |_| ()).is_some());
        assert!(stream.with_epoch_by_extent_id(ExtentId(2), |_| ()).is_some());
    }

    #[test]
    fn no_eviction_when_limit_is_zero() {
        let stream = Stream::new(StreamId(1), test_arena_ids());
        stream.set_max_epochs(0); // 0 means no limit

        for i in 0..5u32 {
            stream.register_epoch_simple(
                ExtentId(i),
                Offset(i as u64),
                DEFAULT_EPOCH_CAPACITY,
                Epoch(0),
            );
            stream
                .append(ExtentId(i), Bytes::from_static(b"x"))
                .unwrap();
            stream.seal(ExtentId(i), None);
        }
        // Register one more active extent.
        stream.register_epoch_simple(ExtentId(5), Offset(5), DEFAULT_EPOCH_CAPACITY, Epoch(0));

        // All 6 extents should still be present.
        for i in 0..=5 {
            assert!(stream.with_epoch_by_extent_id(ExtentId(i), |_| ()).is_some());
        }
    }

    #[test]
    fn evict_unsealed_extents_secondary_scenario() {
        // On secondaries, old extents may not be sealed (autonomous extent-full
        // only seals on the Primary). Eviction should still work for Memory-class streams.
        let stream = Stream::new(StreamId(1), test_arena_ids());
        stream.set_storage_class(StorageClass::Memory);
        stream.set_max_epochs(2);

        // Register extent 0 (not sealed — simulating secondary).
        stream.register_epoch_simple(ExtentId(0), Offset(0), DEFAULT_EPOCH_CAPACITY, Epoch(0));

        // Register extent 1 — 2 extents, at limit.
        stream.register_epoch_simple(ExtentId(1), Offset(100), DEFAULT_EPOCH_CAPACITY, Epoch(0));
        assert!(stream.with_epoch_by_extent_id(ExtentId(0), |_| ()).is_some());
        assert!(stream.with_epoch_by_extent_id(ExtentId(1), |_| ()).is_some());

        // Register extent 2 — 3 extents, exceeds limit.
        // Extent 0 is NOT sealed, but should still be evicted.
        stream.register_epoch_simple(ExtentId(2), Offset(200), DEFAULT_EPOCH_CAPACITY, Epoch(0));
        assert!(
            stream.with_epoch_by_extent_id(ExtentId(0), |_| ()).is_none(),
            "unsealed extent 0 should be evicted"
        );
        assert!(stream.with_epoch_by_extent_id(ExtentId(1), |_| ()).is_some());
        assert!(stream.with_epoch_by_extent_id(ExtentId(2), |_| ()).is_some());
    }

    #[test]
    fn s3_stream_skips_eviction_until_flushed() {
        // S3-class streams must NOT evict extents that haven't been flushed.
        let stream = Stream::new(StreamId(1), test_arena_ids());
        // Default is StorageClass::S3, verify explicitly.
        assert_eq!(stream.storage_class(), StorageClass::S3);
        stream.set_max_epochs(2);

        // Create 3 extents: extent 0 (sealed), extent 1 (sealed), extent 2 (active).
        stream.register_epoch_simple(ExtentId(0), Offset(0), DEFAULT_EPOCH_CAPACITY, Epoch(0));
        stream
            .append(ExtentId(0), Bytes::from_static(b"a"))
            .unwrap();
        stream.seal(ExtentId(0), None);

        stream.register_epoch_simple(ExtentId(1), Offset(1), DEFAULT_EPOCH_CAPACITY, Epoch(0));
        stream
            .append(ExtentId(1), Bytes::from_static(b"b"))
            .unwrap();
        stream.seal(ExtentId(1), None);

        stream.register_epoch_simple(ExtentId(2), Offset(2), DEFAULT_EPOCH_CAPACITY, Epoch(0));

        // 3 extents exceed limit=2, but extent 0 is not flushed — no eviction.
        assert!(
            stream.with_epoch_by_extent_id(ExtentId(0), |_| ()).is_some(),
            "unflushed S3 extent 0 must NOT be evicted"
        );

        // Mark extent 0 as flushed, then trigger eviction by adding extent 3.
        stream.with_epoch_by_extent_id(ExtentId(0), |ext| ext.mark_flushed());
        stream
            .append(ExtentId(2), Bytes::from_static(b"c"))
            .unwrap();
        stream.seal(ExtentId(2), None);
        stream.register_epoch_simple(ExtentId(3), Offset(3), DEFAULT_EPOCH_CAPACITY, Epoch(0));

        // Now extent 0 is flushed — should be evicted.
        assert!(
            stream.with_epoch_by_extent_id(ExtentId(0), |_| ()).is_none(),
            "flushed S3 extent 0 should be evicted"
        );
        // Extent 1 is still not flushed — should remain even though we're over limit.
        assert!(
            stream.with_epoch_by_extent_id(ExtentId(1), |_| ()).is_some(),
            "unflushed S3 extent 1 must NOT be evicted"
        );
        assert!(stream.with_epoch_by_extent_id(ExtentId(2), |_| ()).is_some());
        assert!(stream.with_epoch_by_extent_id(ExtentId(3), |_| ()).is_some());
    }

}
