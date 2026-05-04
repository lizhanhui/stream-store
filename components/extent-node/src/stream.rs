use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use bytes::Bytes;
use common::config::{DEFAULT_CACHE_EPOCHS, DEFAULT_EPOCH_CAPACITY};
use common::errors::{EpochSealedSnafu, InternalSnafu, StorageError};
use common::types::{ArenaClass, Epoch, EpochState, ErrorCode, Offset, StorageClass, StreamId};
use crossbeam_channel::{Receiver, Sender, unbounded};
use parking_lot::RwLock;
use rpc::frame::Frame;
use rpc::frame::VariableHeader;
use smallvec::SmallVec;
use tokio::sync::mpsc;

use crate::ack_queue::{AckQueue, PendingAck};
use crate::arena::{ArenaAppendResult, ArenaId, ArenaIdGenerator, ArenaPool, WriteBatchJob};
use crate::store::{AppendJob, ReplicaInfo};
use crate::stream_epoch::{AppendResult, StreamEpoch};

/// Entry for a successful append within a batch, used by `dispatch_results`.
struct BatchResultEntry {
    request_id: u32,
    offset: Offset,
    payload_for_forward: Bytes,
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
/// - Epoch reads (`append`, `read`, `write_batch_active`, ...) use `self.epochs.load()` (lock-free).
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
    flush_in_progress: papaya::HashMap<Epoch, ()>,

    /// All StreamEpochs this EN currently tracks for this stream,
    /// sorted by epoch number ascending. Copy-on-write via ArcSwap:
    /// readers take a single Arc load (no lock); writers clone the
    /// SmallVec, mutate, and `store()`. Writes happen only on epoch
    /// register / arena roll / epoch death — rare.
    epochs: ArcSwap<SmallVec<[Arc<StreamEpoch>; 4]>>,

    /// Mints ArenaIds for epochs registered directly (register_epoch path).
    /// Shared with the pool so that all extents for this stream draw from the
    /// same monotonic counter.
    #[allow(dead_code)]
    arena_ids: Arc<ArenaIdGenerator>,

    /// Arena pool. Chosen by `arena_class` at stream-creation
    /// time: Dedicated streams own a per-stream `DedicatedArenaPool`;
    /// Shared streams (P3) reference the EN-wide `SharedArenaPool`
    /// singleton. `Stream::register_epoch` calls `pool.allocate(...)`
    /// to create the first arena for each epoch.
    pool: Arc<dyn ArenaPool>,

    /// EN-wide counters shared by every Stream. Incremented on
    /// successful append/replicate; read and reset by the Store's
    /// heartbeat snapshot.
    metrics: Arc<crate::store::StoreMetrics>,

    /// Replication timeout: how long a `PendingAck` can wait for
    /// quorum before being expired. Set at stream creation from the
    /// Store's config and never changes after.
    replication_timeout: Duration,

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
    pub(crate) fn new(
        id: StreamId,
        arena_ids: Arc<ArenaIdGenerator>,
        pool: Arc<dyn ArenaPool>,
        metrics: Arc<crate::store::StoreMetrics>,
        replication_timeout: Duration,
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
            arena_ids,
            pool,
            metrics,
            replication_timeout,
            inner: RwLock::new(StreamInner {
                epoch_capacity: DEFAULT_EPOCH_CAPACITY,
                max_epochs: DEFAULT_CACHE_EPOCHS as usize,
                downstream_txs: Vec::new(),
                storage_class: StorageClass::S3,
            }),
        }
    }

    // ── Epoch vec helpers (lock-free reads, CoW writes) ──────────────

    fn find_epoch_by_number(&self, epoch: Epoch) -> Option<Arc<StreamEpoch>> {
        self.epochs
            .load()
            .iter()
            .find(|e| e.epoch == epoch)
            .cloned()
    }

    /// Find the epoch whose range covers `offset`.
    pub fn find_epoch_for_offset(&self, offset: Offset) -> Option<Epoch> {
        self.epochs
            .load()
            .iter()
            .find(|e| offset.0 >= e.start_offset.0 && offset.0 < e.next_offset().0)
            .map(|e| e.epoch)
    }

    /// The currently-active (last, highest-epoch) StreamEpoch. None if none
    /// registered yet.
    pub fn active_epoch(&self) -> Option<Arc<StreamEpoch>> {
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

    /// Seal the active epoch if it matches the requested epoch.
    fn seal_epoch_by_number(
        &self,
        epoch: Epoch,
        committed_offset: Option<u64>,
    ) -> Option<(u64, u64)> {
        let snap = self.epochs.load();
        let last = snap.last()?;
        if last.epoch != epoch {
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

    /// Current replica info for this stream's epoch, or `None` if
    /// `RegisterEpoch` has not landed yet. Cheap atomic load.
    pub(crate) fn replica_info(&self) -> Option<Arc<ReplicaInfo>> {
        self.active_epoch().and_then(|ep| ep.replica_info())
    }

    /// Install (or replace) the active epoch's replica info. Called at
    /// `RegisterEpoch` time; sets the replica info on the epoch itself.
    pub(crate) fn set_replica_info(&self, info: Arc<ReplicaInfo>) {
        if let Some(ep) = self.active_epoch() {
            ep.set_replica_info(info);
        }
    }

    /// Timeout for quorum-ACK expiry. Set once at stream construction
    /// from the Store's EN config; never changes thereafter.
    pub(crate) fn replication_timeout(&self) -> Duration {
        self.replication_timeout
    }

    /// Shared EN-wide metrics counters. Exposed for store-layer code
    /// paths that increment on the stream's behalf (e.g. the batch
    /// append handler).
    #[allow(dead_code)] // future: metrics introspection
    pub(crate) fn metrics_handle(&self) -> &Arc<crate::store::StoreMetrics> {
        &self.metrics
    }

    /// Arena pool accessor. Used by s3_codec, send_forward_checksum,
    /// and future write/read path routing through the pool.
    pub(crate) fn pool(&self) -> &Arc<dyn ArenaPool> {
        &self.pool
    }

    // ── Read-lock methods ──────────────────────────────────────────────

    /// Append a message to the specified epoch. Routes through the
    /// ArenaPool for the physical write, then updates CRC and
    /// committed_offset on the StreamEpoch.
    pub fn append(&self, epoch: Epoch, payload: Bytes) -> Result<AppendResult, StorageError> {
        let extent = self.find_epoch_by_number(epoch).ok_or_else(|| {
            InternalSnafu {
                message: format!("stream {}: epoch {} not found", self.id, epoch),
            }
            .build()
        })?;
        let hint_offset = Offset(extent.committed_offset());
        let job = WriteBatchJob::new(hint_offset, payload.clone());
        let mut results = self.pool.write_batch(self.id, epoch, std::slice::from_ref(&job));
        match results.pop().expect("one result per job") {
            Ok(r) => {
                extent.update_crc(&payload);
                extent.advance_committed(1);
                Ok(AppendResult {
                    offset: r.offset,
                    byte_pos: r.byte_pos as u64,
                })
            }
            Err(e) => Err(e),
        }
    }

    /// Batch-append to the active epoch. Routes through the ArenaPool
    /// for the physical write. Does NOT update CRC or committed_offset —
    /// the caller (append_one, append_batch_leader) is responsible for
    /// those metadata updates after processing the results.
    ///
    /// Does NOT check seal — that is the caller's responsibility.
    pub(crate) fn write_batch_active(
        &self,
        jobs: &[WriteBatchJob],
    ) -> SmallVec<[Result<ArenaAppendResult, StorageError>; 16]> {
        match self.active_epoch() {
            Some(ep) => {
                if ep.is_sealed() {
                    let committed = ep.committed_offset() - ep.start_offset.0;
                    let limit = ep.limit_hint();
                    if committed >= limit {
                        return jobs
                            .iter()
                            .map(|_| {
                                Err(EpochSealedSnafu {
                                    stream_id: self.id,
                                    epoch: ep.epoch,
                                }
                                .build())
                            })
                            .collect();
                    }
                }
                self.pool.write_batch(self.id, ep.epoch, jobs)
            }
            None => {
                let err = InternalSnafu {
                    message: format!("stream {}: no active epoch", self.id),
                }
                .build();
                jobs.iter()
                    .map(|_| {
                        Err(InternalSnafu {
                            message: err.to_string(),
                        }
                        .build())
                    })
                    .collect()
            }
        }
    }

    /// Replicate a record into the specified epoch. Routes through
    /// the ArenaPool for the physical write, then updates CRC and
    /// committed_offset on the StreamEpoch.
    pub fn replicate(
        &self,
        epoch: Epoch,
        offset: Offset,
        payload: Bytes,
    ) -> Result<AppendResult, StorageError> {
        let extent = self.find_epoch_by_number(epoch).ok_or_else(|| {
            InternalSnafu {
                message: format!("stream {}: epoch {} not found", self.id, epoch),
            }
            .build()
        })?;
        // Seal check: reject post-seal writes on the secondary too.
        if extent.is_sealed() {
            let committed = extent.committed_offset() - extent.start_offset.0;
            let limit = extent.limit_hint();
            if committed >= limit {
                return Err(EpochSealedSnafu {
                    stream_id: self.id,
                    epoch: extent.epoch,
                }
                .build());
            }
        }
        // Offset validation (was in StreamEpoch::replicate).
        if offset.0 < extent.start_offset.0 {
            return Err(InternalSnafu {
                message: format!(
                    "stale forward: offset {} < start_offset {}",
                    offset.0, extent.start_offset.0
                ),
            }
            .build());
        }
        let expected = extent.committed_offset();
        if offset.0 != expected {
            return Err(InternalSnafu {
                message: format!(
                    "out-of-order forward: got {} expected {}",
                    offset.0, expected
                ),
            }
            .build());
        }
        let job = WriteBatchJob::new(offset, payload.clone());
        let mut results = self.pool.write_batch(self.id, epoch, std::slice::from_ref(&job));
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
                // Sync resident_arenas if arena was rotated.
                let known: SmallVec<[ArenaId; 4]> = extent.resident_arenas.lock().clone();
                if !known.contains(&r.arena_id) {
                    extent.resident_arenas.lock().push(r.arena_id);
                    extent.directory_ref_count.fetch_add(1, Ordering::Release);
                }
                extent.update_crc(&payload);
                extent.advance_committed(1);
                Ok(AppendResult {
                    offset: r.offset,
                    byte_pos: r.byte_pos as u64,
                })
            }
            Err(e) => Err(e),
        }
    }

    /// Read `count` messages starting from the given logical `offset` within the specified epoch.
    /// Routes through the ArenaPool for the physical read.
    pub fn read(
        &self,
        epoch: Epoch,
        offset: Offset,
        count: u32,
    ) -> Result<Vec<Bytes>, StorageError> {
        let extent = self.find_epoch_by_number(epoch).ok_or_else(|| {
            InternalSnafu {
                message: format!("stream {}: epoch {} not found", self.id, epoch),
            }
            .build()
        })?;
        if offset.0 < extent.start_offset.0 || offset.0 >= extent.next_offset().0 {
            return Ok(Vec::new());
        }
        self.pool.read_at_offset(self.id, epoch, offset, count)
    }

    /// Whether this stream can accept appends (its last epoch is active/unsealed).
    pub fn is_mutable(&self) -> bool {
        self.active_epoch()
            .map(|e| e.state() == EpochState::Active)
            .unwrap_or(false)
    }

    /// The active epoch number, or None if no epochs are registered.
    pub fn active_epoch_number(&self) -> Option<Epoch> {
        self.active_epoch().map(|e| e.epoch)
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
        self.active_epoch()
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

    /// Check whether `frame` is a Forward/ForwardChecksum targeting an
    /// epoch on this stream that has not yet emitted its
    /// ForwardInitEpoch. Returns the init frame if so (and clears the
    /// flag atomically so exactly one init frame is emitted).
    ///
    /// Called on the leader side before pushing to the per-secondary
    /// channel. FIFO channel ordering guarantees ForwardInitEpoch
    /// arrives before the Forward frame on the wire.
    pub(crate) fn maybe_build_init_forward(&self, frame: &Frame) -> Option<Frame> {
        let (stream_id, epoch) = match &frame.variable_header {
            VariableHeader::Forward {
                stream_id, epoch, ..
            }
            | VariableHeader::ForwardChecksum {
                stream_id, epoch, ..
            } => (*stream_id, *epoch),
            _ => return None,
        };

        self.with_epoch(epoch, |ext| {
            if !ext.take_init_forward() {
                return None;
            }
            Some(Frame::new(
                VariableHeader::ForwardInitEpoch {
                    stream_id,
                    epoch,
                    start_offset: ext.start_offset,
                    cache_extents: self.max_epochs() as u16,
                    storage_class: self.storage_class(),
                    arena_class: ArenaClass::Dedicated,
                },
                None,
            ))
        })?
    }

    /// Return the maximum number of epochs to retain (0 = no limit).
    pub fn max_epochs(&self) -> usize {
        self.inner.read().max_epochs
    }

    /// Return the storage class for this stream.
    pub fn storage_class(&self) -> StorageClass {
        self.inner.read().storage_class
    }

    /// Try to mark an epoch as flush-in-progress. Returns `true` if inserted
    /// (caller should proceed with flush), `false` if already in progress (dedup).
    pub fn start_flush(&self, epoch: Epoch) -> bool {
        let guard = self.flush_in_progress.pin();
        if guard.contains_key(&epoch) {
            false
        } else {
            guard.insert(epoch, ());
            true
        }
    }

    /// Clear the flush-in-progress marker for an epoch (flush completed or failed).
    pub fn finish_flush(&self, epoch: Epoch) {
        self.flush_in_progress.pin().remove(&epoch);
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
    pub fn register_epoch(&self, start_offset: Offset, epoch: Epoch, epoch_capacity: u32) {
        self.epoch.store(epoch.0, Ordering::Release);
        {
            let mut inner = self.inner.write();
            inner.epoch_capacity = epoch_capacity;
        }
        let ep = Arc::new(StreamEpoch::new(self.id, epoch, start_offset));
        // Allocate the first arena through the pool.
        let first_arena = self.pool.allocate(self.id, epoch, start_offset, epoch_capacity);
        ep.resident_arenas.lock().push(first_arena.arena_id);
        ep.directory_ref_count.fetch_add(1, Ordering::Release);
        self.insert_epoch(ep);
        self.evict_oldest_epochs();
    }

    /// Simplified register_epoch for tests.
    #[cfg(test)]
    pub fn register_epoch_simple(&self, start_offset: Offset, epoch_capacity: u32, epoch: Epoch) {
        self.register_epoch(start_offset, epoch, epoch_capacity);
    }

    /// Seal the active epoch if it matches `epoch`.
    pub fn seal(&self, epoch: Epoch, committed_offset: Option<u64>) -> Option<(u64, u64)> {
        self.seal_epoch_by_number(epoch, committed_offset)
    }

    /// Seal the active epoch without creating a successor.
    ///
    /// Returns `(sealed_epoch, end_offset)` if the active epoch was sealed, or `None`
    /// if no active epoch exists or it was already sealed.
    pub fn seal_current_epoch(&self) -> Option<(Epoch, Offset)> {
        let epoch = self.active_epoch_number()?;
        let (_, end_offset) = self.seal_epoch_by_number(epoch, None)?;
        Some((epoch, Offset(end_offset)))
    }

    /// Set cached downstream senders (Primary only, called at RegisterEpoch time).
    pub(crate) fn set_downstream_txs(&self, txs: Vec<mpsc::Sender<Frame>>) {
        self.inner.write().downstream_txs = txs;
    }

    // ── Hot-path append ────────────────────────────────────────────────

    /// Append a single record to the active epoch and handle replication
    /// / ACK according to this stream's ReplicaInfo. Used by the store's
    /// leader-election fast path and by `drain_follower_jobs`.
    ///
    /// Returns the response frame, or `None` when the response was
    /// already delivered via `response_tx`.
    ///
    /// Arena-full is handled transparently inside
    /// `StreamEpoch::write_batch`; any error surfaced here is a genuine
    /// per-job failure (EpochSealed / internal).
    pub(crate) fn append_one(
        &self,
        request_id: u32,
        epoch: Epoch,
        payload: Bytes,
        response_tx: Option<mpsc::Sender<Frame>>,
    ) -> Option<Frame> {
        let payload_len = payload.len();
        let payload_for_forward = payload.clone();

        let hint_offset = self.max_offset();
        let job = WriteBatchJob::new(hint_offset, payload);
        let mut results = self.write_batch_active(std::slice::from_ref(&job));
        let append_result = match results.pop().expect("one result per job") {
            Ok(r) => r,
            Err(StorageError::EpochSealed { .. }) => {
                let err = Frame::append_ack_error(
                    request_id,
                    self.id,
                    epoch,
                    ErrorCode::ExtentSealed,
                    "extent is sealed",
                );
                if let Some(tx) = response_tx {
                    let _ = tx.try_send(err);
                    return None;
                }
                return Some(err);
            }
            Err(e) => {
                let err = Frame::append_ack_error(
                    request_id,
                    self.id,
                    epoch,
                    ErrorCode::InternalError,
                    &e.to_string(),
                );
                if let Some(tx) = response_tx {
                    let _ = tx.try_send(err);
                    return None;
                }
                return Some(err);
            }
        };

        let offset = append_result.offset;

        // Update epoch metadata: CRC and committed_offset.
        // (Previously done inside StreamEpoch::write_batch; now the
        // pool does the physical write and Stream updates metadata.)
        if let Some(ep) = self.active_epoch() {
            // Sync resident_arenas if arena was rotated.
            let known: SmallVec<[ArenaId; 4]> = ep.resident_arenas.lock().clone();
            if !known.contains(&append_result.arena_id) {
                ep.resident_arenas.lock().push(append_result.arena_id);
                ep.directory_ref_count.fetch_add(1, Ordering::Release);
            }
            ep.update_crc(&payload_for_forward);
            ep.advance_committed(1);
        }

        // Update metrics counters.
        self.metrics.append_count.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .bytes_written
            .fetch_add(payload_len as u64, Ordering::Relaxed);

        // Track records for periodic CRC checkpoint.
        if let Some(ep) = self.active_epoch() {
            ep.incr_records_since_checksum(1);
        }

        // Check replica info (one atomic load; no map lookup).
        let replica = self.replica_info();

        match replica {
            None => {
                // Standalone mode: immediate ACK.
                let ack = Frame::new(
                    VariableHeader::AppendAck {
                        request_id,
                        stream_id: self.id,
                        epoch,
                        offset,
                    },
                    None,
                );
                if let Some(tx) = response_tx {
                    let _ = tx.try_send(ack);
                    None
                } else {
                    Some(ack)
                }
            }
            Some(ref ri) if ri.is_primary() => {
                if ri.is_standalone() {
                    // RF=1: no secondaries, ACK immediately.
                    let ack = Frame::new(
                        VariableHeader::AppendAck {
                            request_id,
                            stream_id: self.id,
                            epoch,
                            offset,
                        },
                        None,
                    );
                    if let Some(tx) = response_tx {
                        let _ = tx.try_send(ack);
                        None
                    } else {
                        Some(ack)
                    }
                } else {
                    // RF≥2: push Forward frames inline into per-stream channels.
                    if self.has_secondaries() {
                        let forward_frame = Frame::new(
                            VariableHeader::Forward {
                                stream_id: self.id,
                                epoch,
                                offset,
                            },
                            Some(payload_for_forward),
                        );
                        // Inject ForwardInitEpoch if this is the first forward for the epoch.
                        if let Some(init) = self.maybe_build_init_forward(&forward_frame) {
                            self.send_forward(init);
                        }
                        self.send_forward(forward_frame);

                        // Send periodic CRC checkpoint if threshold reached.
                        if let Some(ep) = self.active_epoch()
                            && ep.should_send_crc_checksum()
                        {
                            let (checksum, up_to) = ep.snapshot_crc();
                            let crc_frame = Frame::new(
                                VariableHeader::ForwardCrcChecksum {
                                    stream_id: self.id,
                                    epoch,
                                    checksum,
                                    up_to_offset: up_to.0,
                                },
                                None,
                            );
                            if let Some(init) = self.maybe_build_init_forward(&crc_frame) {
                                self.send_forward(init);
                            }
                            self.send_forward(crc_frame);
                        }
                    }

                    // Queue deferred ACK — lock-free, no contention with watermark readers.
                    if let Some(ref resp_tx) = response_tx {
                        let aq = self.init_ack_queue(
                            ri.required_secondary_acks(),
                            self.replication_timeout(),
                        );
                        aq.enqueue(PendingAck {
                            request_id,
                            stream_id: self.id,
                            response_tx: resp_tx.clone(),
                            assigned_offset: offset.0,
                            epoch,
                            created_at: Instant::now(),
                        });
                    }

                    None
                }
            }
            Some(_) => {
                // Secondary received normal Append (not Forward) — shouldn't normally happen.
                let ack = Frame::new(
                    VariableHeader::AppendAck {
                        request_id,
                        stream_id: self.id,
                        epoch,
                        offset,
                    },
                    None,
                );
                if let Some(tx) = response_tx {
                    let _ = tx.try_send(ack);
                    None
                } else {
                    Some(ack)
                }
            }
        }
    }

    /// Shared ACK/forward/replica-info logic used by both `append_one` and
    /// `append_batch_leader`. Processes a batch of successful append entries:
    /// updates metrics, builds Forward frames, enqueues pending ACKs, and
    /// sends immediate ACKs for standalone mode.
    fn dispatch_results(
        &self,
        entries: &[BatchResultEntry],
        epoch: Epoch,
        response_tx: Option<&mpsc::Sender<Frame>>,
    ) -> Vec<Frame> {
        let mut responses = Vec::new();
        if entries.is_empty() {
            return responses;
        }

        let total_bytes: u64 = entries.iter().map(|e| e.payload_for_forward.len() as u64).sum();
        self.metrics.append_count.fetch_add(entries.len() as u64, Ordering::Relaxed);
        self.metrics.bytes_written.fetch_add(total_bytes, Ordering::Relaxed);

        // Track records for periodic CRC checkpoint.
        if let Some(ep) = self.active_epoch() {
            ep.incr_records_since_checksum(entries.len() as u32);
        }

        let replica = self.replica_info();

        match replica.as_ref() {
            None => {
                for entry in entries {
                    let ack = Frame::new(
                        VariableHeader::AppendAck {
                            request_id: entry.request_id,
                            stream_id: self.id,
                            epoch,
                            offset: entry.offset,
                        },
                        None,
                    );
                    if let Some(tx) = response_tx {
                        let _ = tx.try_send(ack);
                    } else {
                        responses.push(ack);
                    }
                }
            }
            Some(ri) if ri.is_primary() => {
                if ri.is_standalone() {
                    for entry in entries {
                        let ack = Frame::new(
                            VariableHeader::AppendAck {
                                request_id: entry.request_id,
                                stream_id: self.id,
                                epoch,
                                offset: entry.offset,
                            },
                            None,
                        );
                        if let Some(tx) = response_tx {
                            let _ = tx.try_send(ack);
                        } else {
                            responses.push(ack);
                        }
                    }
                } else {
                    if self.has_secondaries() {
                        for entry in entries {
                            let forward_frame = Frame::new(
                                VariableHeader::Forward {
                                    stream_id: self.id,
                                    epoch,
                                    offset: entry.offset,
                                },
                                Some(entry.payload_for_forward.clone()),
                            );
                            if let Some(init) = self.maybe_build_init_forward(&forward_frame) {
                                self.send_forward(init);
                            }
                            self.send_forward(forward_frame);
                        }

                        // Send periodic CRC checkpoint if threshold reached.
                        if let Some(ep) = self.active_epoch()
                            && ep.should_send_crc_checksum()
                        {
                            let (checksum, up_to) = ep.snapshot_crc();
                            let crc_frame = Frame::new(
                                VariableHeader::ForwardCrcChecksum {
                                    stream_id: self.id,
                                    epoch,
                                    checksum,
                                    up_to_offset: up_to.0,
                                },
                                None,
                            );
                            if let Some(init) = self.maybe_build_init_forward(&crc_frame) {
                                self.send_forward(init);
                            }
                            self.send_forward(crc_frame);
                        }
                    }
                    if let Some(resp_tx) = response_tx {
                        let aq = self.init_ack_queue(
                            ri.required_secondary_acks(),
                            self.replication_timeout(),
                        );
                        let now = Instant::now();
                        for entry in entries {
                            aq.enqueue(PendingAck {
                                request_id: entry.request_id,
                                stream_id: self.id,
                                response_tx: resp_tx.clone(),
                                assigned_offset: entry.offset.0,
                                epoch,
                                created_at: now,
                            });
                        }
                    }
                }
            }
            Some(_) => {
                for entry in entries {
                    let ack = Frame::new(
                        VariableHeader::AppendAck {
                            request_id: entry.request_id,
                            stream_id: self.id,
                            epoch,
                            offset: entry.offset,
                        },
                        None,
                    );
                    if let Some(tx) = response_tx {
                        let _ = tx.try_send(ack);
                    } else {
                        responses.push(ack);
                    }
                }
            }
        }

        responses
    }

    /// Batch-append leader: builds WriteBatchJob array, calls pool.write_batch,
    /// processes errors, calls epoch.update_crc/advance_committed per success,
    /// then dispatch_results.
    pub(crate) fn append_batch_leader(
        &self,
        epoch: Epoch,
        frames: &[Frame],
        response_tx: Option<&mpsc::Sender<Frame>>,
    ) -> Vec<Frame> {
        let mut responses = Vec::new();

        let mut jobs: SmallVec<[WriteBatchJob; 16]> = SmallVec::with_capacity(frames.len());
        let hint = self.max_offset();
        for (i, frame) in frames.iter().enumerate() {
            let payload = frame.payload.clone().unwrap_or_default();
            jobs.push(WriteBatchJob::new(Offset(hint.0 + i as u64), payload));
        }
        let results = self.write_batch_active(&jobs);

        let mut entries: Vec<BatchResultEntry> = Vec::with_capacity(frames.len());

        for (i, res) in results.into_iter().enumerate() {
            let request_id = frames[i].request_id();
            let payload = frames[i].payload.clone().unwrap_or_default();
            match res {
                Ok(result) => {
                    // Update epoch metadata: CRC, committed_offset, and arena tracking.
                    if let Some(ep) = self.active_epoch() {
                        let known: SmallVec<[ArenaId; 4]> = ep.resident_arenas.lock().clone();
                        if !known.contains(&result.arena_id) {
                            ep.resident_arenas.lock().push(result.arena_id);
                            ep.directory_ref_count.fetch_add(1, Ordering::Release);
                        }
                        ep.update_crc(&payload);
                        ep.advance_committed(1);
                    }
                    entries.push(BatchResultEntry {
                        request_id,
                        offset: result.offset,
                        payload_for_forward: payload,
                    });
                }
                Err(StorageError::EpochSealed { .. }) => {
                    let err = Frame::append_ack_error(
                        request_id,
                        self.id,
                        epoch,
                        ErrorCode::ExtentSealed,
                        "extent is sealed",
                    );
                    if let Some(tx) = response_tx {
                        let _ = tx.try_send(err);
                    } else {
                        responses.push(err);
                    }
                }
                Err(e) => {
                    let err = Frame::append_ack_error(
                        request_id,
                        self.id,
                        epoch,
                        ErrorCode::InternalError,
                        &e.to_string(),
                    );
                    if let Some(tx) = response_tx {
                        let _ = tx.try_send(err);
                    } else {
                        responses.push(err);
                    }
                }
            }
        }

        let dispatch_responses = self.dispatch_results(&entries, epoch, response_tx);
        responses.extend(dispatch_responses);

        responses
    }

    /// Drain follower append jobs from this stream's channel.
    ///
    /// Called by the active writer after its own append when
    /// `in_flight > 1`. Loops until all followers have been processed.
    /// Forward frames are pushed inline by `append_one`; this method
    /// only deals with per-job logical outcomes (seal, err, ok).
    ///
    /// Holds `&self` across every `.await` — no papaya guard dance
    /// required, because the caller already owns an `Arc<Stream>` that
    /// outlives this loop.
    pub(crate) async fn drain_follower_jobs(&self) {
        loop {
            // ── Phase 1: Drain jobs from the channel ──
            let mut batch: Vec<AppendJob> = Vec::new();
            let mut epoch = Epoch(0);
            loop {
                if batch.is_empty() {
                    epoch = self.epoch();
                }
                match self.job_rx().try_recv() {
                    Ok(job) => {
                        batch.push(job);
                        while let Ok(job) = self.job_rx().try_recv() {
                            batch.push(job);
                        }
                        break;
                    }
                    Err(_) if !batch.is_empty() => break,
                    Err(_) => {
                        // Follower incremented in_flight but hasn't pushed yet.
                        let delegated = self.in_flight.load(Ordering::Acquire);
                        if delegated > 0 {
                            tokio::task::yield_now().await;
                        } else {
                            break;
                        }
                    }
                }
            }

            // ── Phase 2: Process the batch ──
            let batch_len = batch.len();
            for job in &batch {
                self.append_one(
                    job.request_id,
                    epoch,
                    job.payload.clone(),
                    job.response_tx.clone(),
                );
            }
            let remaining = self
                .in_flight
                .fetch_sub(batch_len as u64, Ordering::Release);
            if remaining <= batch_len as u64 {
                break;
            }
            // More followers arrived during processing — loop again.
        }
    }
}

/// Information about an extent that was sealed during an append.
#[derive(Debug, Clone)]
pub struct SealNotification {
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

    fn test_pool(ids: &Arc<ArenaIdGenerator>) -> Arc<dyn ArenaPool> {
        Arc::new(crate::arena::DedicatedArenaPool::new(Arc::clone(ids)))
    }

    fn test_metrics() -> Arc<crate::store::StoreMetrics> {
        crate::store::StoreMetrics::new()
    }

    /// Helper: create a stream with one active extent (simulating RegisterEpoch from SM).
    fn new_stream_with_epoch(id: StreamId) -> Stream {
        let ids = test_arena_ids();
        let stream = Stream::new(
            id,
            Arc::clone(&ids),
            test_pool(&ids),
            test_metrics(),
            crate::ack_queue::DEFAULT_REPLICATION_TIMEOUT,
        );
        stream.register_epoch_simple(Offset(0), DEFAULT_EPOCH_CAPACITY, Epoch(0));
        stream
    }

    #[test]
    fn basic_append_and_read() {
        let stream = new_stream_with_epoch(StreamId(1));
        let epoch = Epoch(0);
        let r0 = stream.append(epoch, Bytes::from_static(b"msg0")).unwrap();
        let r1 = stream.append(epoch, Bytes::from_static(b"msg1")).unwrap();
        let r2 = stream.append(epoch, Bytes::from_static(b"msg2")).unwrap();

        assert_eq!(r0.offset, Offset(0));
        assert_eq!(r1.offset, Offset(1));
        assert_eq!(r2.offset, Offset(2));
        assert_eq!(stream.max_offset(), Offset(3));

        // Read all 3 from offset 0.
        let msgs = stream.read(epoch, Offset(0), 3).unwrap();
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0], Bytes::from_static(b"msg0"));
        assert_eq!(msgs[2], Bytes::from_static(b"msg2"));

        // Random access: read msg1 directly via its offset.
        let msgs = stream.read(epoch, r1.offset, 1).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0], Bytes::from_static(b"msg1"));
    }

    #[test]
    fn read_from_offset() {
        let stream = new_stream_with_epoch(StreamId(1));
        let epoch = Epoch(0);
        let mut results = Vec::new();
        for i in 0..10 {
            results.push(
                stream
                    .append(epoch, Bytes::from(format!("msg{i}")))
                    .unwrap(),
            );
        }

        // Read 3 messages starting at offset 5.
        let r5 = &results[5];
        let msgs = stream.read(epoch, r5.offset, 3).unwrap();
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0], Bytes::from("msg5"));
        assert_eq!(msgs[1], Bytes::from("msg6"));
        assert_eq!(msgs[2], Bytes::from("msg7"));
    }

    #[test]
    fn read_beyond_end_returns_available() {
        let stream = new_stream_with_epoch(StreamId(1));
        let epoch = Epoch(0);
        let r = stream.append(epoch, Bytes::from_static(b"only")).unwrap();

        let msgs = stream.read(epoch, r.offset, 100).unwrap();
        assert_eq!(msgs.len(), 1);
    }

    #[test]
    fn read_empty_stream() {
        let stream = {
            let ids = test_arena_ids();
            Stream::new(
                StreamId(1),
                Arc::clone(&ids),
                test_pool(&ids),
                test_metrics(),
                crate::ack_queue::DEFAULT_REPLICATION_TIMEOUT,
            )
        };
        assert_eq!(stream.max_offset(), Offset(0));

        // Stream with no extents: read returns error (extent not found).
        let result = stream.read(Epoch(0), Offset(0), 10);
        assert!(result.is_err());
    }

    #[test]
    fn empty_stream_properties() {
        let stream = {
            let ids = test_arena_ids();
            Stream::new(
                StreamId(1),
                Arc::clone(&ids),
                test_pool(&ids),
                test_metrics(),
                crate::ack_queue::DEFAULT_REPLICATION_TIMEOUT,
            )
        };
        assert_eq!(stream.max_offset(), Offset(0));
        assert!(!stream.is_mutable());
        assert_eq!(stream.active_epoch_number(), None);
        assert!(
            stream
                .append(Epoch(0), Bytes::from_static(b"fail"))
                .is_err()
        );
    }

    #[test]
    fn seal_and_new() {
        let stream = new_stream_with_epoch(StreamId(1));
        let first_epoch = Epoch(0);
        // Append 3 messages to first extent.
        for i in 0..3 {
            stream
                .append(first_epoch, Bytes::from(format!("msg{i}")))
                .unwrap();
        }
        assert_eq!(stream.max_offset(), Offset(3));

        // Seal active extent.
        let (start_offset, end_offset) = stream.seal(first_epoch, None).unwrap();
        assert_eq!(start_offset, 0);
        assert_eq!(end_offset, 3);

        // After seal, stream has no active extent until register_epoch.
        assert!(!stream.is_mutable());

        // Register a new extent (simulating SM sending RegisterEpoch).
        let second_epoch = Epoch(1);
        stream.register_epoch_simple(Offset(3), DEFAULT_EPOCH_CAPACITY, second_epoch);
        assert!(stream.is_mutable());
        assert_eq!(stream.max_offset(), Offset(3)); // new extent is empty

        // Append to the new extent.
        let r = stream
            .append(second_epoch, Bytes::from_static(b"after-seal"))
            .unwrap();
        assert_eq!(r.offset, Offset(3));
        assert_eq!(r.byte_pos, 0); // new extent, byte_pos starts at 0
        assert_eq!(stream.max_offset(), Offset(4));

        // Read from the new extent.
        let msgs = stream.read(second_epoch, r.offset, 1).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0], Bytes::from_static(b"after-seal"));
    }

    #[test]
    fn seal_current_epoch_returns_epoch_and_end_offset() {
        let stream = new_stream_with_epoch(StreamId(1));
        let first_epoch = Epoch(0);
        let r = stream
            .append(first_epoch, Bytes::from_static(b"a"))
            .unwrap();
        assert_eq!(r.offset, Offset(0));

        assert_eq!(stream.seal_current_epoch(), Some((Epoch(0), Offset(1))));
        assert_eq!(stream.seal_current_epoch(), None); // already sealed, returns None
        assert!(!stream.is_mutable());
    }

    #[test]
    fn evict_oldest_sealed_extents() {
        let stream = {
            let ids = test_arena_ids();
            Stream::new(
                StreamId(1),
                Arc::clone(&ids),
                test_pool(&ids),
                test_metrics(),
                crate::ack_queue::DEFAULT_REPLICATION_TIMEOUT,
            )
        };
        stream.set_storage_class(StorageClass::Memory);
        stream.set_max_epochs(2);

        // Register extent 0 and append a message.
        stream.register_epoch_simple(Offset(0), DEFAULT_EPOCH_CAPACITY, Epoch(0));
        stream
            .append(Epoch(0), Bytes::from_static(b"msg0"))
            .unwrap();

        // Seal extent 0, register extent 1.
        stream.seal(Epoch(0), None);
        stream.register_epoch_simple(Offset(1), DEFAULT_EPOCH_CAPACITY, Epoch(1));
        // 2 extents (sealed + active) — at limit, no eviction.
        assert!(stream.with_epoch(Epoch(0), |_| ()).is_some());
        assert!(stream.with_epoch(Epoch(1), |_| ()).is_some());

        // Seal extent 1, register extent 2 — now 3 extents, should evict extent 0.
        stream
            .append(Epoch(1), Bytes::from_static(b"msg1"))
            .unwrap();
        stream.seal(Epoch(1), None);
        stream.register_epoch_simple(Offset(2), DEFAULT_EPOCH_CAPACITY, Epoch(2));
        assert!(
            stream.with_epoch(Epoch(0), |_| ()).is_none(),
            "extent 0 should be evicted"
        );
        assert!(stream.with_epoch(Epoch(1), |_| ()).is_some());
        assert!(stream.with_epoch(Epoch(2), |_| ()).is_some());
    }

    #[test]
    fn no_eviction_when_limit_is_zero() {
        let stream = {
            let ids = test_arena_ids();
            Stream::new(
                StreamId(1),
                Arc::clone(&ids),
                test_pool(&ids),
                test_metrics(),
                crate::ack_queue::DEFAULT_REPLICATION_TIMEOUT,
            )
        };
        stream.set_max_epochs(0); // 0 means no limit

        for i in 0..5u32 {
            stream.register_epoch_simple(Offset(i as u64), DEFAULT_EPOCH_CAPACITY, Epoch(i));
            stream.append(Epoch(i), Bytes::from_static(b"x")).unwrap();
            stream.seal(Epoch(i), None);
        }
        // Register one more active extent.
        stream.register_epoch_simple(Offset(5), DEFAULT_EPOCH_CAPACITY, Epoch(5));

        // All 6 extents should still be present.
        for i in 0..=5 {
            assert!(stream.with_epoch(Epoch(i), |_| ()).is_some());
        }
    }

    #[test]
    fn evict_unsealed_extents_secondary_scenario() {
        // On secondaries, old extents may not be sealed (autonomous extent-full
        // only seals on the Primary). Eviction should still work for Memory-class streams.
        let stream = {
            let ids = test_arena_ids();
            Stream::new(
                StreamId(1),
                Arc::clone(&ids),
                test_pool(&ids),
                test_metrics(),
                crate::ack_queue::DEFAULT_REPLICATION_TIMEOUT,
            )
        };
        stream.set_storage_class(StorageClass::Memory);
        stream.set_max_epochs(2);

        // Register extent 0 (not sealed — simulating secondary).
        stream.register_epoch_simple(Offset(0), DEFAULT_EPOCH_CAPACITY, Epoch(0));

        // Register extent 1 — 2 extents, at limit.
        stream.register_epoch_simple(Offset(100), DEFAULT_EPOCH_CAPACITY, Epoch(1));
        assert!(stream.with_epoch(Epoch(0), |_| ()).is_some());
        assert!(stream.with_epoch(Epoch(1), |_| ()).is_some());

        // Register extent 2 — 3 extents, exceeds limit.
        // Extent 0 is NOT sealed, but should still be evicted.
        stream.register_epoch_simple(Offset(200), DEFAULT_EPOCH_CAPACITY, Epoch(2));
        assert!(
            stream.with_epoch(Epoch(0), |_| ()).is_none(),
            "unsealed extent 0 should be evicted"
        );
        assert!(stream.with_epoch(Epoch(1), |_| ()).is_some());
        assert!(stream.with_epoch(Epoch(2), |_| ()).is_some());
    }

    #[test]
    fn s3_stream_skips_eviction_until_flushed() {
        // S3-class streams must NOT evict extents that haven't been flushed.
        let stream = {
            let ids = test_arena_ids();
            Stream::new(
                StreamId(1),
                Arc::clone(&ids),
                test_pool(&ids),
                test_metrics(),
                crate::ack_queue::DEFAULT_REPLICATION_TIMEOUT,
            )
        };
        // Default is StorageClass::S3, verify explicitly.
        assert_eq!(stream.storage_class(), StorageClass::S3);
        stream.set_max_epochs(2);

        // Create 3 extents: extent 0 (sealed), extent 1 (sealed), extent 2 (active).
        stream.register_epoch_simple(Offset(0), DEFAULT_EPOCH_CAPACITY, Epoch(0));
        stream.append(Epoch(0), Bytes::from_static(b"a")).unwrap();
        stream.seal(Epoch(0), None);

        stream.register_epoch_simple(Offset(1), DEFAULT_EPOCH_CAPACITY, Epoch(1));
        stream.append(Epoch(1), Bytes::from_static(b"b")).unwrap();
        stream.seal(Epoch(1), None);

        stream.register_epoch_simple(Offset(2), DEFAULT_EPOCH_CAPACITY, Epoch(2));

        // 3 extents exceed limit=2, but extent 0 is not flushed — no eviction.
        assert!(
            stream.with_epoch(Epoch(0), |_| ()).is_some(),
            "unflushed S3 extent 0 must NOT be evicted"
        );

        // Mark extent 0 as flushed, then trigger eviction by adding extent 3.
        stream.with_epoch(Epoch(0), |ext| ext.mark_flushed());
        stream.append(Epoch(2), Bytes::from_static(b"c")).unwrap();
        stream.seal(Epoch(2), None);
        stream.register_epoch_simple(Offset(3), DEFAULT_EPOCH_CAPACITY, Epoch(3));

        // Now extent 0 is flushed — should be evicted.
        assert!(
            stream.with_epoch(Epoch(0), |_| ()).is_none(),
            "flushed S3 extent 0 should be evicted"
        );
        // Extent 1 is still not flushed — should remain even though we're over limit.
        assert!(
            stream.with_epoch(Epoch(1), |_| ()).is_some(),
            "unflushed S3 extent 1 must NOT be evicted"
        );
        assert!(stream.with_epoch(Epoch(2), |_| ()).is_some());
        assert!(stream.with_epoch(Epoch(3), |_| ()).is_some());
    }
}
