mod append;
mod forward;
mod read;
mod register;
mod seal;
mod types;

#[cfg(test)]
mod tests;

pub(crate) use types::AppendRequest;
pub use types::{ExtentUpdate, ReplicaInfo};

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use common::hasher::IdentityBuildHasher;
use common::types::{Epoch, EpochPolicy, ErrorCode, Opcode, StorageClass, StreamId};
use rpc::frame::{Frame, VariableHeader};
use server::handler::RequestHandler;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tracing::warn;

use crate::ack_queue::DEFAULT_REPLICATION_TIMEOUT;
use crate::arena::ArenaIdGenerator;
use crate::downstream::DownstreamPool;
use crate::s3::S3Client;
use crate::s3_flusher::FlushRequest;
use crate::stream::Stream;

// ── StoreMetrics ─────────────────────────────────────────────────────────────

/// EN-wide metrics counters, shared by every Stream.
///
/// Streams increment on each successful append/replicate; the Store
/// reads on heartbeat and resets via `swap`. Held behind `Arc` so the
/// hot path can mutate without routing through a Store lookup.
pub(crate) struct StoreMetrics {
    pub(crate) append_count: AtomicU64,
    pub(crate) bytes_written: AtomicU64,
}

impl StoreMetrics {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            append_count: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
        })
    }
}

// ── ExtentNodeStore ──────────────────────────────────────────────────────────

/// The ExtentNode's in-memory store: holds all streams and their extents.
///
/// Uses per-stream fine-grained concurrency via `papaya::HashMap` instead of a single global
/// Mutex. This ensures:
/// - **Different streams are fully concurrent**: requests to Stream A and Stream B
///   never block each other.
/// - **Reads within a stream don't block other streams**: each Stream has
///   its own internal `RwLock` separating reads from lifecycle mutations.
/// - **Writes to the same extent** use the lock-free arena (atomic CAS), so even
///   within a stream, multiple appenders only synchronize on slot reservation.
pub struct ExtentNodeStore {
    /// Per-stream data with fine-grained locking.
    /// Uses `IdentityBuildHasher` — StreamId is a server-assigned u32, so the
    /// identity hash (no mixing) is safe and eliminates ~15 ns of SipHash per lookup.
    pub(crate) streams: papaya::HashMap<StreamId, Arc<Stream>, IdentityBuildHasher>,
    /// ArenaId generator used by register_epoch paths.
    pub(crate) arena_ids: Arc<ArenaIdGenerator>,
    /// EN-wide singleton pool for Shared-class streams. All Shared
    /// streams reference this `Arc` via `Stream.pool`. P2 ships a
    /// panicking stub; P3 wires the real multi-stream path.
    pub(crate) shared_pool: Arc<crate::arena::SharedArenaPool>,
    /// Direct TCP connection pool for broadcast replication (None for standalone/test mode).
    /// Initialized via `set_downstream()` after construction (OnceLock breaks circular dep).
    pub(crate) downstream: OnceLock<Arc<DownstreamPool>>,
    /// S3 client for flushed extent storage (None when s3_bucket is empty).
    /// Initialized via `set_s3_client()` after construction (OnceLock for async init).
    pub(crate) s3_client: OnceLock<Arc<S3Client>>,
    /// Channel to send ExtentUpdate notifications to SM (Primary only).
    /// The SM connection task receives these and sends UPDATE_EXTENT frames.
    pub(crate) update_tx: Option<Sender<ExtentUpdate>>,
    /// Configurable timeout for replication quorum ACK expiry.
    pub(crate) replication_timeout: Duration,
    /// EN-wide counters shared by every Stream. Streams increment on
    /// successful append/replicate; heartbeat reads and resets via
    /// `swap`.
    pub(crate) metrics: Arc<StoreMetrics>,
    /// Channel to send sealed extent flush requests to the S3 flusher task.
    /// None when S3 is not configured.
    pub(crate) flush_tx: Option<Sender<FlushRequest>>,
}

impl ExtentNodeStore {
    /// Create a new store in standalone mode (no replication) with default arena capacity.
    /// Uses a default node prefix (1). For production use, call `new_with_ids` and pass
    /// a node-specific `ArenaIdGenerator`.
    pub fn new() -> Self {
        let arena_ids = Arc::new(ArenaIdGenerator::new(1));
        Self::new_with_ids(arena_ids)
    }

    /// Create a new store with a caller-provided `ArenaIdGenerator`.
    /// Called by `ExtentNode::start` after resolving the node_id.
    pub(crate) fn new_with_ids(arena_ids: Arc<ArenaIdGenerator>) -> Self {
        let shared_pool = Arc::new(crate::arena::SharedArenaPool::new(
            Arc::clone(&arena_ids),
            common::config::DEFAULT_EPOCH_CAPACITY,
        ));
        Self {
            streams: papaya::HashMap::with_hasher(IdentityBuildHasher),
            arena_ids,
            shared_pool,
            downstream: OnceLock::new(),
            s3_client: OnceLock::new(),
            update_tx: None,
            replication_timeout: DEFAULT_REPLICATION_TIMEOUT,
            metrics: StoreMetrics::new(),
            flush_tx: None,
        }
    }

    /// Set the replication timeout (from config). Called once at startup.
    pub fn set_replication_timeout(&mut self, timeout: Duration) {
        self.replication_timeout = timeout;
    }

    /// Set the downstream connection pool for broadcast replication.
    /// Called once during ExtentNode bootstrap after the store is created.
    /// Uses OnceLock to break the circular dependency: store needs pool, pool needs store.
    pub fn set_downstream(&self, pool: Arc<DownstreamPool>) {
        self.downstream.set(pool).ok();
    }

    /// Ensure a stream exists, creating it if needed.
    ///
    /// Always applies all stream-level configs (cache, storage_class) to the
    /// stream, whether existing or new. On first creation the `arena_class`
    /// picks the Stream's `pool`: Dedicated → a fresh per-stream
    /// `DedicatedArenaPool`; Shared → the EN-wide `SharedArenaPool` singleton.
    ///
    /// Returns `true` if the stream was just created.
    pub(crate) fn try_create_stream(
        &self,
        stream_id: StreamId,
        storage_class: StorageClass,
        arena_class: common::types::ArenaClass,
        policy: &EpochPolicy,
    ) -> bool {
        let guard = self.streams.pin();
        if let Some(stream) = guard.get(&stream_id) {
            if policy.cache > 0 {
                stream.set_max_epochs(policy.cache as usize);
            }
            stream.set_storage_class(storage_class);
            false
        } else {
            let pool: Arc<dyn crate::arena::ArenaPool> = match arena_class {
                common::types::ArenaClass::Dedicated => Arc::new(
                    crate::arena::DedicatedArenaPool::new(Arc::clone(&self.arena_ids)),
                ),
                common::types::ArenaClass::Shared => {
                    Arc::clone(&self.shared_pool) as Arc<dyn crate::arena::ArenaPool>
                }
            };
            let stream = Stream::new(
                stream_id,
                Arc::clone(&self.arena_ids),
                pool,
                Arc::clone(&self.metrics),
                self.replication_timeout,
            );
            if policy.cache > 0 {
                stream.set_max_epochs(policy.cache as usize);
            }
            stream.set_storage_class(storage_class);
            guard.insert(stream_id, Arc::new(stream));
            true
        }
    }

    /// Set the S3 client for flushed extent storage.
    /// Called once during ExtentNode bootstrap after async initialization.
    pub fn set_s3_client(&self, client: Arc<S3Client>) {
        self.s3_client.set(client).ok();
    }

    /// Get a reference to the S3 client, if configured.
    pub fn s3_client(&self) -> Option<&Arc<S3Client>> {
        self.s3_client.get()
    }

    /// Set the seal request channel (called during ExtentNode bootstrap).
    pub fn set_update_tx(&mut self, update_tx: Sender<ExtentUpdate>) {
        self.update_tx = Some(update_tx);
    }

    /// Set the S3 flush request channel (called during ExtentNode bootstrap).
    pub fn set_flush_tx(&mut self, flush_tx: Sender<FlushRequest>) {
        self.flush_tx = Some(flush_tx);
    }

    /// Get the replication info for a stream, if registered via RegisterEpoch.
    pub fn get_replica_info(&self, stream_id: StreamId) -> Option<ReplicaInfo> {
        self.streams
            .pin()
            .get(&stream_id)
            .and_then(|s| s.replica_info())
            .map(|arc| (*arc).clone())
    }

    /// Expire stale PendingAcks across all streams by running the timeout sweep.
    ///
    /// Called when a downstream secondary reader exits (secondary died or disconnected),
    /// so clients get timely error responses instead of waiting for SM recovery.
    /// Without this, RF=2 clients would stall for the full SM heartbeat detection
    /// window (~7.5s) because no watermark ACKs arrive to trigger `drain_quorum`.
    pub fn expire_pending_acks(&self) {
        let guard = self.streams.pin();
        for (_k, stream) in guard.iter() {
            if let Some(aq) = stream.ack_queue() {
                let mut inner = aq.lock_inner();
                inner.receive_pending();
                inner.drain_quorum();
            }
        }
    }

    /// Resolve the secondary index for the given addr within a stream's replica set.
    ///
    /// Returns `None` if the stream has no `ReplicaInfo` or the addr is not among
    /// the registered secondaries. The index is the position in
    /// `ReplicaInfo.replica_addrs` (0-based: secondary-1 → 0, secondary-2 → 1).
    ///
    /// Callers should cache the result per `(stream_id, addr)` pair — the mapping
    /// is immutable within an epoch.
    pub fn secondary_index(&self, stream_id: StreamId, addr: &str) -> Option<u8> {
        let ri = self
            .streams
            .pin()
            .get(&stream_id)
            .and_then(|s| s.replica_info())?;
        ri.replica_addrs
            .iter()
            .position(|a| a == addr)
            .map(|i| i as u8)
    }

    /// Snapshot current metrics and reset counters.
    /// Returns (appends_since_last, bytes_written_since_last, active_extent_count).
    pub fn snapshot_metrics(&self) -> (u64, u64, u32) {
        let appends = self.metrics.append_count.swap(0, Ordering::Relaxed);
        let bytes = self.metrics.bytes_written.swap(0, Ordering::Relaxed);

        // Count active extents: streams whose last extent is active (mutable).
        let active_count = self
            .streams
            .pin()
            .iter()
            .filter(|entry| entry.1.is_mutable())
            .count() as u32;

        (appends, bytes, active_count)
    }

    /// Snapshot active extent info for progress reporting to SM.
    /// Returns (stream_id, current_offset, epoch) for each active epoch.
    pub fn snapshot_active_extents(&self) -> Vec<(StreamId, u64, Epoch)> {
        let guard = self.streams.pin();
        guard
            .iter()
            .filter_map(|(k, stream)| {
                if stream.is_mutable() {
                    let offset = stream.max_offset().0;
                    Some((*k, offset, stream.epoch()))
                } else {
                    None
                }
            })
            .collect()
    }
}

impl Default for ExtentNodeStore {
    fn default() -> Self {
        Self::new()
    }
}

impl RequestHandler for ExtentNodeStore {
    async fn handle_frame(
        &self,
        frame: Frame,
        response_tx: Option<&Sender<Frame>>,
    ) -> Option<Frame> {
        match frame.opcode() {
            Opcode::Append => self.handle_append(frame, response_tx).await,
            Opcode::Forward => {
                // Forward, ForwardInitEpoch, and ForwardChecksum share opcode 0x0B.
                match &frame.variable_header {
                    VariableHeader::ForwardInitEpoch { .. } => {
                        self.handle_forward_init_epoch(frame);
                        None // fire-and-forget, no response
                    }
                    VariableHeader::ForwardChecksum { .. } => {
                        self.handle_forward_checksum(frame);
                        None // fire-and-forget, no response
                    }
                    VariableHeader::ForwardFlushed { .. } => {
                        self.handle_forward_flushed(frame);
                        None // fire-and-forget, no response
                    }
                    VariableHeader::ForwardCrcChecksum { .. } => {
                        self.handle_forward_crc_checksum(frame);
                        None // fire-and-forget, no response
                    }
                    _ => self.handle_forward(frame),
                }
            }
            Opcode::Read => Some(self.handle_read(frame)),
            Opcode::QueryOffset => Some(self.handle_query_offset(frame)),
            Opcode::SealEpoch => match &frame.variable_header {
                VariableHeader::SealEpochCommit { .. } => Some(self.handle_seal_commit(frame)),
                _ => Some(self.handle_seal(frame)),
            },
            Opcode::RegisterEpoch => Some(self.handle_register_epoch(frame)),
            Opcode::Connect => Some(Frame::new(
                VariableHeader::ConnectAck {
                    request_id: frame.request_id(),
                },
                None,
            )),
            Opcode::Heartbeat => Some(Frame::new(
                VariableHeader::Heartbeat {
                    request_id: frame.request_id(),
                },
                None,
            )),
            Opcode::ReportEpoch => Some(self.handle_report_epoch(frame)),
            Opcode::FlushEpoch => Some(self.handle_flush_extent(frame)),
            Opcode::UpdateEpoch
            | Opcode::Watermark
            | Opcode::SealStream
            | Opcode::StreamManagerMembershipChange => {
                warn!(
                    opcode = ?frame.opcode(),
                    "EN received unexpected opcode that should not be sent to ExtentNode"
                );
                None
            }
            _ => Some(Frame::error_from_request(
                &frame,
                ErrorCode::InternalError,
                "unsupported opcode",
            )),
        }
    }

    /// Optimized batch append for consecutive same-epoch frames.
    ///
    /// All frames in the batch share the same stream_id/epoch, so we:
    /// - Do a single streams.pin().get() instead of N
    /// - Do a single leader election (fetch_add(batch.len())) instead of N
    /// - Borrow ReplicaInfo once (no clone) instead of N clones
    /// - Enqueue all PendingAcks in a single loop instead of N separate lookups
    async fn handle_append_batch(
        &self,
        frames: &[Frame],
        response_tx: Option<&Sender<Frame>>,
    ) -> Vec<Frame> {
        if frames.is_empty() {
            return Vec::new();
        }
        // Single frame: fall back to normal path (avoids overhead of batch setup).
        if frames.len() == 1 {
            return self
                .handle_append(frames[0].clone(), response_tx)
                .await
                .into_iter()
                .collect();
        }
        self.handle_append_batch_inner(frames, response_tx).await
    }
}
