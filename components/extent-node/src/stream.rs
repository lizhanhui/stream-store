use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use arc_swap::ArcSwap;
use bytes::Bytes;
use common::config::{DEFAULT_CACHE_EXTENTS, DEFAULT_EXTENT_CAPACITY};
use common::errors::{InternalSnafu, StorageError};
use common::types::{Epoch, EpochState, ExtentId, Offset, StorageClass, StreamId};
use crossbeam_channel::{Receiver, Sender, unbounded};
use parking_lot::RwLock;
use rpc::frame::Frame;
use smallvec::SmallVec;
use tokio::sync::mpsc;
use tracing::error;

use crate::ack_queue::AckQueue;
use crate::arena::{ArenaIdGenerator, ArenaPool};
use crate::store::AppendJob;
use crate::stream_epoch::{AppendResult, StreamEpoch};

/// Reason for sealing the active extent and creating a new one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SealReason {
    /// Extent arena is full — stream needs more space.
    ExtentFull,
}

/// Mutable state protected by `RwLock`. Grouped here so that a single
/// lock acquisition covers all fields that need coordinated mutation.
struct StreamInner {
    // extents: Vec<StreamEpoch>,   // REMOVED — now on Stream as ArcSwap<SmallVec<...>>
    /// Next extent ID for autonomous creation within the current epoch.
    /// Initialized to `first_extent_id + 1` when SM sends RegisterEpoch.
    next_extent_id: ExtentId,

    /// Fixed capacity for every extent on this stream. Set at register_extent time
    /// from config and never changed.
    extent_capacity: u32,

    /// Maximum number of extents to retain. 0 = no limit.
    /// When exceeded, the oldest sealed extents are dropped to free memory.
    max_extents: usize,

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
/// - Mutations of inner fields (`seal`, `register_extent`, etc.) use `inner.write()`.
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
    /// hot-path reads; updated via `register_extent` / `set_epoch`.
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

    /// Tracks extents with an in-progress S3 flush (both Primary and DR paths).
    /// Used to deduplicate concurrent flush requests for the same extent.
    /// Lock-free via papaya::HashMap.
    flush_in_progress: papaya::HashMap<ExtentId, ()>,

    /// All StreamEpochs this EN currently tracks for this stream,
    /// sorted by epoch number ascending. Copy-on-write via ArcSwap:
    /// readers take a single Arc load (no lock); writers clone the
    /// SmallVec, mutate, and `store()`. Writes happen only on epoch
    /// register / arena roll / epoch death — rare.
    epochs: ArcSwap<SmallVec<[Arc<StreamEpoch>; 4]>>,

    /// Allocator for new StreamEpochs. Dedicated in P2; Shared routing
    /// lands in a later plan.
    pool: Arc<dyn ArenaPool>,

    /// Mints ArenaIds for extents registered directly (register_extent path).
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

    /// Create a new stream with no extents. Extents are added via `register_extent()`.
    pub(crate) fn new(
        id: StreamId,
        pool: Arc<dyn ArenaPool>,
        arena_ids: Arc<ArenaIdGenerator>,
    ) -> Self {
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
            pool,
            arena_ids,
            inner: RwLock::new(StreamInner {
                next_extent_id: ExtentId(0),
                extent_capacity: DEFAULT_EXTENT_CAPACITY,
                max_extents: DEFAULT_CACHE_EXTENTS as usize,
                downstream_txs: Vec::new(),
                storage_class: StorageClass::S3,
            }),
        }
    }

    // ── Epoch vec helpers (lock-free reads, CoW writes) ──────────────

    /// Find the epoch by extent_id in the epochs vec. Used by
    /// with_extent and every internal find-by-id path.
    fn find_epoch(&self, extent_id: ExtentId) -> Option<Arc<StreamEpoch>> {
        self.epochs
            .load()
            .iter()
            .find(|e| e.id == extent_id)
            .cloned()
    }

    /// Bridge helper (pre-P3 Phase 4): look up the `StreamEpoch` covering a
    /// given offset. Introduced when the wire protocol dropped `extent_id`
    /// from read/seal/flush handlers; removed once `Stream::with_extent`
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

    /// The currently-active (last, highest-epoch) epoch. None if none
    /// registered yet.
    fn active_epoch(&self) -> Option<Arc<StreamEpoch>> {
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

    /// Allocate a new epoch (autonomous on Primary). Returns
    /// `Some((new_id, start_offset))` on success, `None` on
    /// S3-backpressure block.
    fn try_create_next_epoch(&self, epoch: Epoch) -> Option<(ExtentId, Offset)> {
        // S3 backpressure check: read the current snapshot + inner fields.
        let (max_extents, is_s3) = {
            let inner = self.inner.read();
            (inner.max_extents, inner.storage_class == StorageClass::S3)
        };
        {
            let snap = self.epochs.load();
            if max_extents > 0
                && snap.len() >= max_extents
                && is_s3
                && snap.first().is_some_and(|e| !e.is_flushed())
            {
                return None;
            }
        }

        // Compute start_offset + next id; bump next_extent_id under the write lock.
        let (new_id, start_offset) = {
            let snap = self.epochs.load();
            let start = snap
                .last()
                .map(|e| Offset(e.start_offset.0 + e.message_count()))
                .unwrap_or(Offset(0));
            let mut inner = self.inner.write();
            let new_id = inner.next_extent_id;
            inner.next_extent_id = ExtentId(new_id.0 + 1);
            (new_id, start)
        };

        // Allocate + insert outside the write lock. Insert ordering is by
        // epoch number, which is monotone, so the new entry appends at the
        // tail.
        let ep = self
            .pool
            .allocate_epoch(self.id, new_id, start_offset, epoch);
        self.insert_epoch(ep);

        self.evict_oldest_epochs();
        Some((new_id, start_offset))
    }

    /// Evict oldest epochs when count exceeds max_extents.
    ///
    /// - S3 streams: only flushed epochs are eligible; oldest-first stops at
    ///   the first un-flushed head.
    /// - Memory streams: all but the active (last) epoch are eligible.
    /// - max_extents == 0: unlimited; log a warn if the count grows past 4.
    fn evict_oldest_epochs(&self) {
        let (max_extents, is_s3) = {
            let inner = self.inner.read();
            (inner.max_extents, inner.storage_class == StorageClass::S3)
        };
        if max_extents == 0 {
            let snap = self.epochs.load();
            if snap.len() > 4 {
                tracing::warn!(
                    "stream {} has {} epochs but max_extents=0 (no eviction); \
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
            if snap.len() <= max_extents.max(1) {
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
        let extent = self.find_epoch(extent_id).ok_or_else(|| {
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
        let extent = self.active_epoch().ok_or_else(|| {
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
        let extent = self.find_epoch(extent_id).ok_or_else(|| {
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
        let extent = self.find_epoch(extent_id).ok_or_else(|| {
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

    /// Whether this stream can accept appends (its last extent is active/unsealed).
    pub fn is_mutable(&self) -> bool {
        self.active_epoch()
            .map(|e| e.state() == EpochState::Active)
            .unwrap_or(false)
    }

    /// The extent ID of the active (last) extent, or None if no extents.
    pub fn active_extent_id(&self) -> Option<ExtentId> {
        self.active_epoch().map(|e| e.id)
    }

    /// The extent ID of the last mutable extent at the given epoch.
    /// Returns None if no unsealed extent exists at that epoch.
    pub fn active_extent_at_epoch(&self, epoch: Epoch) -> Option<ExtentId> {
        self.epochs
            .load()
            .iter()
            .rev()
            .find(|e| e.epoch == epoch && e.state() == EpochState::Active)
            .map(|e| e.id)
    }

    /// The last sealed extent at the given epoch.
    /// Returns `(extent_id, start_offset, end_offset)` or None if no sealed extent exists at that epoch.
    pub fn last_sealed_extent_at_epoch(&self, epoch: Epoch) -> Option<(ExtentId, u64, u64)> {
        self.epochs
            .load()
            .iter()
            .rev()
            .find(|e| e.epoch == epoch && e.is_sealed())
            .map(|e| (e.id, e.start_offset.0, e.start_offset.0 + e.message_count()))
    }

    /// The maximum offset (exclusive): the next offset that would be assigned.
    /// Returns `Offset(0)` if the stream has no extents.
    pub fn max_offset(&self) -> Offset {
        self.active_epoch()
            .map(|e| e.next_offset())
            .unwrap_or(Offset(0))
    }

    /// Closure-based access to an extent by ID. Returns `None` if the extent
    /// does not exist; otherwise applies `f` to the extent and returns `Some(R)`.
    ///
    /// Use this instead of the old `find_extent(&self) -> Option<&Extent>` pattern,
    /// which cannot return a reference from behind the RwLock guard.
    pub fn with_extent<F, R>(&self, extent_id: ExtentId, f: F) -> Option<R>
    where
        F: FnOnce(&StreamEpoch) -> R,
    {
        self.find_epoch(extent_id).map(|ep| f(&ep))
    }

    /// Report extents for this stream that belong to the specified epoch.
    ///
    /// During recovery, SM only cares about extents created in the specified epoch
    /// (extents from prior epochs are already reconciled in MySQL metadata).
    /// Filters by per-extent epoch, so only extents actually created under the
    /// requested epoch are returned.
    pub fn report_extents(&self, epoch: Epoch) -> Vec<(ExtentId, Offset, u64, EpochState)> {
        self.epochs
            .load()
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
        if let Some(extent) = self.find_epoch(extent_id)
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

    /// Return the maximum number of extents to retain (0 = no limit).
    pub fn max_extents(&self) -> usize {
        self.inner.read().max_extents
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

    /// Set the maximum number of extents to retain per stream.
    /// 0 means no limit (default).
    pub fn set_max_extents(&self, max: usize) {
        self.inner.write().max_extents = max;
    }

    /// Set the storage class for this stream.
    pub fn set_storage_class(&self, class: StorageClass) {
        self.inner.write().storage_class = class;
    }

    /// Register a new extent on this stream (called when SM sends RegisterEpoch
    /// or when a secondary receives ForwardInitEpoch).
    ///
    /// `extent_capacity` is the arena size for this specific extent.
    pub fn register_extent(
        &self,
        id: ExtentId,
        start_offset: Offset,
        epoch: Epoch,
        extent_capacity: u32,
    ) {
        self.epoch.store(epoch.0, Ordering::Release);
        {
            let mut inner = self.inner.write();
            inner.extent_capacity = extent_capacity;
            inner.next_extent_id = ExtentId(id.0 + 1);
        }
        let arena_id = self.arena_ids.next();
        let ep = Arc::new(StreamEpoch::with_capacity(
            id,
            start_offset,
            extent_capacity,
            epoch,
            arena_id,
        ));
        self.insert_epoch(ep);
        self.evict_oldest_epochs();
    }

    /// Simplified register_extent for tests and backward compatibility.
    #[cfg(test)]
    pub fn register_extent_simple(
        &self,
        id: ExtentId,
        start_offset: Offset,
        extent_capacity: u32,
        epoch: Epoch,
    ) {
        self.register_extent(id, start_offset, epoch, extent_capacity);
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
    /// After seal, the stream has no active extent until SM sends a new `RegisterEpoch`
    /// or the Primary autonomously creates one via `create_next_extent()`.
    pub fn seal(&self, extent_id: ExtentId, committed_offset: Option<u64>) -> Option<(u64, u64)> {
        self.seal_epoch_by_id(extent_id, committed_offset)
    }

    /// Autonomously create the next extent on extent-full (Primary only, within same epoch).
    ///
    /// Uses `next_extent_capacity` for the new extent (adaptive sizing).
    /// Extent ID is incremented locally — no SM round-trip needed.
    ///
    /// Returns `Some((new_extent_id, start_offset))` on success, `None` if
    /// backpressure blocks creation (S3 flush backlog).
    pub fn create_next_extent(&self) -> Option<(ExtentId, Offset)> {
        let epoch = Epoch(self.epoch.load(Ordering::Acquire));
        self.try_create_next_epoch(epoch)
    }

    /// Seal the active extent and create a new one.
    ///
    /// Acquires the write lock internally. Returns the seal notification
    /// if a seal+create occurred, or None if already sealed / no active extent.
    pub fn seal_and_create_next(&self, _reason: SealReason) -> Option<SealNotification> {
        let active_id = self.active_epoch()?.id;
        let (_, end_offset) = self.seal_epoch_by_id(active_id, None)?;
        let epoch = Epoch(self.epoch.load(Ordering::Acquire));
        let (new_id, _) = self.try_create_next_epoch(epoch)?;
        let new_capacity = self.active_epoch().map(|e| e.capacity()).unwrap_or(0);
        Some(SealNotification {
            sealed_extent_id: active_id,
            end_offset,
            new_extent_id: new_id,
            epoch,
            new_extent_capacity: new_capacity,
        })
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
    pub end_offset: u64,
    pub new_extent_id: ExtentId,
    pub epoch: Epoch,
    /// The capacity of the newly created extent.
    pub new_extent_capacity: u32,
}

impl std::fmt::Debug for Stream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.read();
        let epochs_snap = self.epochs.load();
        f.debug_struct("Stream")
            .field("id", &self.id)
            .field("epochs", &*epochs_snap)
            .field("epoch", &self.epoch.load(Ordering::Relaxed))
            .field("next_extent_id", &inner.next_extent_id)
            .field("extent_capacity", &inner.extent_capacity)
            .field("max_extents", &inner.max_extents)
            .field("in_flight", &self.in_flight.load(Ordering::Relaxed))
            .field("has_ack_queue", &self.ack_queue.get().is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::config::DEFAULT_EXTENT_CAPACITY;

    fn test_pool() -> Arc<dyn ArenaPool> {
        use crate::arena::{ArenaIdGenerator, DedicatedArenaPool};
        let ids = Arc::new(ArenaIdGenerator::new(1));
        Arc::new(DedicatedArenaPool::new(DEFAULT_EXTENT_CAPACITY, ids))
    }

    fn test_arena_ids() -> Arc<ArenaIdGenerator> {
        Arc::new(crate::arena::ArenaIdGenerator::new(1))
    }

    /// Helper: create a stream with one active extent (simulating RegisterEpoch from SM).
    fn new_stream_with_extent(id: StreamId) -> Stream {
        let stream = Stream::new(id, test_pool(), test_arena_ids());
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
        let stream = Stream::new(StreamId(1), test_pool(), test_arena_ids());
        assert_eq!(stream.max_offset(), Offset(0));

        // Stream with no extents: read returns error (extent not found).
        let result = stream.read(ExtentId(0), Offset(0), 10);
        assert!(result.is_err());
    }

    #[test]
    fn empty_stream_properties() {
        let stream = Stream::new(StreamId(1), test_pool(), test_arena_ids());
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
        let stream = new_stream_with_extent(StreamId(1));
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

        // Register a new extent (simulating SM sending RegisterEpoch).
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
        let stream = new_stream_with_extent(StreamId(1));
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
        let stream = Stream::new(StreamId(1), test_pool(), test_arena_ids());
        stream.set_storage_class(StorageClass::Memory);
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
        assert!(stream.with_extent(ExtentId(0), |_| ()).is_some());
        assert!(stream.with_extent(ExtentId(1), |_| ()).is_some());

        // Seal extent 1, register extent 2 — now 3 extents, should evict extent 0.
        stream
            .append(ExtentId(1), Bytes::from_static(b"msg1"))
            .unwrap();
        stream.seal(ExtentId(1), None);
        stream.register_extent_simple(ExtentId(2), Offset(2), DEFAULT_EXTENT_CAPACITY, Epoch(0));
        assert!(
            stream.with_extent(ExtentId(0), |_| ()).is_none(),
            "extent 0 should be evicted"
        );
        assert!(stream.with_extent(ExtentId(1), |_| ()).is_some());
        assert!(stream.with_extent(ExtentId(2), |_| ()).is_some());
    }

    #[test]
    fn evict_via_create_next_extent() {
        let stream = Stream::new(StreamId(1), test_pool(), test_arena_ids());
        stream.set_storage_class(StorageClass::Memory);
        stream.set_max_extents(2);

        stream.register_extent_simple(ExtentId(0), Offset(0), DEFAULT_EXTENT_CAPACITY, Epoch(0));
        stream
            .append(ExtentId(0), Bytes::from_static(b"a"))
            .unwrap();

        // seal_and_create_next triggers eviction via create_next_extent.
        let notif = stream.seal_and_create_next(SealReason::ExtentFull).unwrap();
        // 2 extents: sealed extent 0 + new active extent 1. At limit.
        assert!(stream.with_extent(ExtentId(0), |_| ()).is_some());
        assert!(stream.with_extent(notif.new_extent_id, |_| ()).is_some());

        // Append to new extent, then seal_and_create again.
        stream
            .append(notif.new_extent_id, Bytes::from_static(b"b"))
            .unwrap();
        let notif2 = stream.seal_and_create_next(SealReason::ExtentFull).unwrap();
        // 3 would exceed limit — extent 0 should be evicted.
        assert!(
            stream.with_extent(ExtentId(0), |_| ()).is_none(),
            "extent 0 should be evicted"
        );
        assert!(stream.with_extent(notif.new_extent_id, |_| ()).is_some());
        assert!(stream.with_extent(notif2.new_extent_id, |_| ()).is_some());
    }

    #[test]
    fn no_eviction_when_limit_is_zero() {
        let stream = Stream::new(StreamId(1), test_pool(), test_arena_ids());
        stream.set_max_extents(0); // 0 means no limit

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
            assert!(stream.with_extent(ExtentId(i), |_| ()).is_some());
        }
    }

    #[test]
    fn evict_unsealed_extents_secondary_scenario() {
        // On secondaries, old extents may not be sealed (autonomous extent-full
        // only seals on the Primary). Eviction should still work for Memory-class streams.
        let stream = Stream::new(StreamId(1), test_pool(), test_arena_ids());
        stream.set_storage_class(StorageClass::Memory);
        stream.set_max_extents(2);

        // Register extent 0 (not sealed — simulating secondary).
        stream.register_extent_simple(ExtentId(0), Offset(0), DEFAULT_EXTENT_CAPACITY, Epoch(0));

        // Register extent 1 — 2 extents, at limit.
        stream.register_extent_simple(ExtentId(1), Offset(100), DEFAULT_EXTENT_CAPACITY, Epoch(0));
        assert!(stream.with_extent(ExtentId(0), |_| ()).is_some());
        assert!(stream.with_extent(ExtentId(1), |_| ()).is_some());

        // Register extent 2 — 3 extents, exceeds limit.
        // Extent 0 is NOT sealed, but should still be evicted.
        stream.register_extent_simple(ExtentId(2), Offset(200), DEFAULT_EXTENT_CAPACITY, Epoch(0));
        assert!(
            stream.with_extent(ExtentId(0), |_| ()).is_none(),
            "unsealed extent 0 should be evicted"
        );
        assert!(stream.with_extent(ExtentId(1), |_| ()).is_some());
        assert!(stream.with_extent(ExtentId(2), |_| ()).is_some());
    }

    #[test]
    fn s3_stream_skips_eviction_until_flushed() {
        // S3-class streams must NOT evict extents that haven't been flushed.
        let stream = Stream::new(StreamId(1), test_pool(), test_arena_ids());
        // Default is StorageClass::S3, verify explicitly.
        assert_eq!(stream.storage_class(), StorageClass::S3);
        stream.set_max_extents(2);

        // Create 3 extents: extent 0 (sealed), extent 1 (sealed), extent 2 (active).
        stream.register_extent_simple(ExtentId(0), Offset(0), DEFAULT_EXTENT_CAPACITY, Epoch(0));
        stream
            .append(ExtentId(0), Bytes::from_static(b"a"))
            .unwrap();
        stream.seal(ExtentId(0), None);

        stream.register_extent_simple(ExtentId(1), Offset(1), DEFAULT_EXTENT_CAPACITY, Epoch(0));
        stream
            .append(ExtentId(1), Bytes::from_static(b"b"))
            .unwrap();
        stream.seal(ExtentId(1), None);

        stream.register_extent_simple(ExtentId(2), Offset(2), DEFAULT_EXTENT_CAPACITY, Epoch(0));

        // 3 extents exceed limit=2, but extent 0 is not flushed — no eviction.
        assert!(
            stream.with_extent(ExtentId(0), |_| ()).is_some(),
            "unflushed S3 extent 0 must NOT be evicted"
        );

        // Mark extent 0 as flushed, then trigger eviction by adding extent 3.
        stream.with_extent(ExtentId(0), |ext| ext.mark_flushed());
        stream
            .append(ExtentId(2), Bytes::from_static(b"c"))
            .unwrap();
        stream.seal(ExtentId(2), None);
        stream.register_extent_simple(ExtentId(3), Offset(3), DEFAULT_EXTENT_CAPACITY, Epoch(0));

        // Now extent 0 is flushed — should be evicted.
        assert!(
            stream.with_extent(ExtentId(0), |_| ()).is_none(),
            "flushed S3 extent 0 should be evicted"
        );
        // Extent 1 is still not flushed — should remain even though we're over limit.
        assert!(
            stream.with_extent(ExtentId(1), |_| ()).is_some(),
            "unflushed S3 extent 1 must NOT be evicted"
        );
        assert!(stream.with_extent(ExtentId(2), |_| ()).is_some());
        assert!(stream.with_extent(ExtentId(3), |_| ()).is_some());
    }

    #[test]
    fn s3_backpressure_blocks_new_extent_creation() {
        // S3-class stream: when eviction is blocked (unflushed extents),
        // seal_and_create_next should return None (backpressure).
        let stream = Stream::new(StreamId(1), test_pool(), test_arena_ids());
        assert_eq!(stream.storage_class(), StorageClass::S3);
        stream.set_max_extents(2);

        // Create extent 0, append, seal it (but NOT flushed).
        stream.register_extent_simple(ExtentId(0), Offset(0), DEFAULT_EXTENT_CAPACITY, Epoch(0));
        stream
            .append(ExtentId(0), Bytes::from_static(b"a"))
            .unwrap();
        stream.seal(ExtentId(0), None);

        // Create extent 1 (active) — 2 extents, at limit.
        stream.register_extent_simple(ExtentId(1), Offset(1), DEFAULT_EXTENT_CAPACITY, Epoch(0));
        stream
            .append(ExtentId(1), Bytes::from_static(b"b"))
            .unwrap();

        // Try seal_and_create_next — should fail (extent 0 not flushed).
        let result = stream.seal_and_create_next(SealReason::ExtentFull);
        assert!(
            result.is_none(),
            "backpressure: should not create new extent when eviction is blocked"
        );

        // Extent 1 should be sealed (seal happened), but no new extent created.
        assert!(
            stream.with_extent(ExtentId(0), |_| ()).is_some(),
            "extent 0 still present"
        );
        assert!(
            stream.with_extent(ExtentId(1), |_| ()).is_some(),
            "extent 1 still present"
        );

        // Flush both sealed extents (simulating S3 upload completing).
        stream.with_extent(ExtentId(0), |ext| ext.mark_flushed());
        stream.with_extent(ExtentId(1), |ext| ext.mark_flushed());

        // Register a new active extent so we can seal+create again.
        stream.register_extent_simple(ExtentId(2), Offset(2), DEFAULT_EXTENT_CAPACITY, Epoch(0));
        stream
            .append(ExtentId(2), Bytes::from_static(b"c"))
            .unwrap();

        // Now both old extents are flushed — eviction is unblocked.
        let result = stream.seal_and_create_next(SealReason::ExtentFull);
        assert!(
            result.is_some(),
            "after flush, seal_and_create_next should succeed"
        );
        // Both old extents should be evicted (flushed + over limit).
        assert!(
            stream.with_extent(ExtentId(0), |_| ()).is_none(),
            "flushed extent 0 should be evicted"
        );
        assert!(
            stream.with_extent(ExtentId(1), |_| ()).is_none(),
            "flushed extent 1 should be evicted"
        );
    }
}
