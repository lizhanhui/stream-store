mod append;
mod forward;
mod read;
mod register;
mod seal;
mod types;

#[cfg(test)]
mod tests;

pub(crate) use types::AppendJob;
pub use types::{ExtentUpdate, ReplicaInfo};

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use common::types::{Epoch, ErrorCode, ExtentId, Opcode, StreamId};
use rpc::frame::{Frame, VariableHeader};
use server::handler::RequestHandler;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tracing::warn;

use crate::ack_queue::{AckQueue, DEFAULT_REPLICATION_TIMEOUT};
use crate::downstream::DownstreamPool;
use crate::s3::S3Client;
use crate::s3_flusher::FlushRequest;
use crate::stream::Stream;

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
    pub(crate) streams: papaya::HashMap<StreamId, Stream>,
    /// Monotonic stream ID generator (atomic, no lock needed).
    pub(crate) next_stream_id: AtomicU64,
    /// Replication info per stream_id (registered via RegisterExtent).
    /// Immutable within an epoch — wrapped in Arc for cheap hot-path cloning.
    pub(crate) replicas: papaya::HashMap<StreamId, Arc<ReplicaInfo>>,
    /// Direct TCP connection pool for broadcast replication (None for standalone/test mode).
    /// Initialized via `set_downstream()` after construction (OnceLock breaks circular dep).
    pub(crate) downstream: OnceLock<Arc<DownstreamPool>>,
    /// S3 client for flushed extent storage (None when s3_bucket is empty).
    /// Initialized via `set_s3_client()` after construction (OnceLock for async init).
    pub(crate) s3_client: OnceLock<Arc<S3Client>>,
    /// Channel to send ExtentUpdate notifications to SM (Primary only).
    /// The SM connection task receives these and sends UPDATE_EXTENT frames.
    pub(crate) update_tx: Option<Sender<ExtentUpdate>>,
    /// Per-stream ACK queues for the Primary (only used when this node is Primary for a stream).
    /// AckQueue has its own internal Mutex — no outer Mutex needed.
    pub ack_queues: papaya::HashMap<StreamId, AckQueue>,
    /// Configurable timeout for replication quorum ACK expiry.
    pub(crate) replication_timeout: Duration,
    // -- Metrics counters (reset on each heartbeat snapshot) --
    /// Total appends since last snapshot (atomic, no lock needed).
    pub(crate) append_count: AtomicU64,
    /// Total bytes written since last snapshot (atomic, no lock needed).
    pub(crate) bytes_written: AtomicU64,
    /// Channel to send sealed extent flush requests to the S3 flusher task.
    /// None when S3 is not configured.
    pub(crate) flush_tx: Option<Sender<FlushRequest>>,
}

impl ExtentNodeStore {
    /// Create a new store in standalone mode (no replication) with default arena capacity.
    pub fn new() -> Self {
        Self {
            streams: papaya::HashMap::new(),
            next_stream_id: AtomicU64::new(1),
            replicas: papaya::HashMap::new(),
            downstream: OnceLock::new(),
            s3_client: OnceLock::new(),
            update_tx: None,
            ack_queues: papaya::HashMap::new(),
            replication_timeout: DEFAULT_REPLICATION_TIMEOUT,
            append_count: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
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

    /// Get the replication info for a stream, if registered via RegisterExtent.
    pub fn get_replica_info(&self, stream_id: StreamId) -> Option<ReplicaInfo> {
        self.replicas
            .pin()
            .get(&stream_id)
            .map(|arc| (**arc).clone())
    }

    /// Expire stale PendingAcks across all streams by running the timeout sweep.
    ///
    /// Called when a downstream secondary reader exits (secondary died or disconnected),
    /// so clients get timely error responses instead of waiting for SM recovery.
    /// Without this, RF=2 clients would stall for the full SM heartbeat detection
    /// window (~7.5s) because no watermark ACKs arrive to trigger `drain_quorum`.
    pub fn expire_pending_acks(&self) {
        let guard = self.ack_queues.pin();
        for (_k, v) in guard.iter() {
            let mut inner = v.lock_inner();
            inner.receive_pending();
            inner.drain_quorum();
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
        let guard = self.replicas.pin();
        let ri = guard.get(&stream_id)?;
        ri.replica_addrs
            .iter()
            .position(|a| a == addr)
            .map(|i| i as u8)
    }

    /// Snapshot current metrics and reset counters.
    /// Returns (appends_since_last, bytes_written_since_last, active_extent_count).
    pub fn snapshot_metrics(&self) -> (u64, u64, u32) {
        let appends = self.append_count.swap(0, Ordering::Relaxed);
        let bytes = self.bytes_written.swap(0, Ordering::Relaxed);

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
    /// Returns (stream_id, extent_id, current_offset, epoch) for each active extent.
    pub fn snapshot_active_extents(&self) -> Vec<(StreamId, ExtentId, u64, Epoch)> {
        let guard = self.streams.pin();
        guard
            .iter()
            .filter_map(|(k, stream)| {
                if stream.is_mutable() {
                    let extent_id = stream.active_extent_id()?;
                    let offset = stream.max_offset().0;
                    Some((*k, extent_id, offset, stream.epoch()))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Lightweight iterator over (StreamId, Epoch) for all active streams.
    ///
    /// Used by the idle-shrink tick task to construct system tick frames.
    /// Pure read-only scan — no write guards, no idle-shrink checks.
    pub fn stream_epochs(&self) -> Vec<(StreamId, Epoch)> {
        let guard = self.streams.pin();
        guard.iter().map(|(k, v)| (*k, v.epoch())).collect()
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
                // Forward, ForwardInitExtent, and ForwardChecksum share opcode 0x0B.
                match &frame.variable_header {
                    VariableHeader::ForwardInitExtent { .. } => {
                        self.handle_forward_init_extent(frame);
                        None // fire-and-forget, no response
                    }
                    VariableHeader::ForwardChecksum { .. } => {
                        self.handle_forward_checksum(frame);
                        None // fire-and-forget, no response
                    }
                    _ => self.handle_forward(frame),
                }
            }
            Opcode::Read => Some(self.handle_read(frame)),
            Opcode::QueryOffset => Some(self.handle_query_offset(frame)),
            Opcode::SealExtentNode => Some(self.handle_seal(frame)),
            Opcode::RegisterExtent => Some(self.handle_register_extent(frame)),
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
            Opcode::ReportExtents => Some(self.handle_report_extents(frame)),
            Opcode::UpdateExtent
            | Opcode::Watermark
            | Opcode::SealStreamManager
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
                ExtentId(0),
            )),
        }
    }

    /// Optimized batch append for consecutive same-epoch frames.
    ///
    /// All frames in the batch share the same stream_id/extent_id, so we:
    /// - Do a single streams.pin().get() instead of N
    /// - Do a single leader election (fetch_add(batch.len())) instead of N
    /// - Borrow ReplicaInfo once (no clone) instead of N clones
    /// - Do a single ack_queues.pin().get() push instead of N
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
