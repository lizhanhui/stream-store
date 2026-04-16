use std::collections::{HashMap, VecDeque};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use std::time::Instant;

use bytes::{BufMut, Bytes, BytesMut};
use common::config::{
    DEFAULT_EXTENT_GROWTH_FACTOR, DEFAULT_IDLE_SHRINK_THRESHOLD_SECS, DEFAULT_MAX_EXTENT_CAPACITY,
    DEFAULT_MIN_EXTENT_CAPACITY,
};
use common::errors::StorageError;
use common::types::{Epoch, ErrorCode, ExtentId, FLAG_SYSTEM_TICK, Offset, Opcode, StreamId};
use std::sync::Mutex;
use rpc::frame::{Frame, VariableHeader};
use rpc::payload::{ROLE_PRIMARY, parse_register_extent_payload};
use server::handler::RequestHandler;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tracing::{debug, info, warn};

use crate::downstream::DownstreamPool;
use crate::stream::{SealNotification, SealReason, Stream};

// ── Broadcast replication types ──────────────────────────────────────────────

/// Default replication timeout used when no config is provided (e.g., in tests).
const DEFAULT_REPLICATION_TIMEOUT: Duration =
    Duration::from_millis(common::config::DEFAULT_REPLICATION_TIMEOUT_MS);

/// A pending client ACK waiting for quorum replication.
#[derive(Debug)]
pub struct PendingAck {
    /// The original request_id from the client's Append frame.
    pub request_id: u32,
    /// The stream the append was written to.
    pub stream_id: StreamId,
    /// Channel back to the client connection's write task.
    pub response_tx: Sender<Frame>,
    /// The offset assigned to this append.
    pub assigned_offset: u64,
    /// The extent the record landed on (for diagnostics in AppendAck).
    pub extent_id: ExtentId,
    pub epoch: Epoch,
    /// When this PendingAck was created, for timeout expiry.
    pub created_at: Instant,
}

/// Per-stream ACK queue on the Primary with cumulative quorum tracking.
///
/// Tracks pending client ACKs and per-secondary highest acked offset.
/// When enough secondaries have confirmed (quorum), drains pending ACKs.
#[derive(Debug)]
pub struct AckQueue {
    /// Pending client ACKs, ordered by offset (front = lowest).
    pub pending: VecDeque<PendingAck>,
    /// Highest acked offset per secondary address (cumulative).
    pub secondary_acked: HashMap<String, u64>,
    /// Number of secondary ACKs needed for quorum.
    pub required_secondary_acks: u32,
    /// Timeout for expiring stale PendingAcks.
    replication_timeout: Duration,
}

impl AckQueue {
    pub fn new(required_secondary_acks: u32) -> Self {
        Self::with_timeout(required_secondary_acks, DEFAULT_REPLICATION_TIMEOUT)
    }

    pub fn with_timeout(required_secondary_acks: u32, replication_timeout: Duration) -> Self {
        Self {
            pending: VecDeque::new(),
            secondary_acked: HashMap::new(),
            required_secondary_acks,
            replication_timeout,
        }
    }

    /// Compute the quorum offset: the highest offset where at least
    /// `required_secondary_acks` secondaries have confirmed.
    ///
    /// Returns None if quorum cannot be met (not enough secondaries have reported).
    ///
    /// Optimized to avoid heap allocation:
    /// - RF=2 (required=1): just take the max of secondary offsets.
    /// - General case: use a fixed-size stack array (RF never exceeds ~4).
    pub fn quorum_offset(&self) -> Option<u64> {
        if self.required_secondary_acks == 0 {
            return None; // RF=1, no quorum needed
        }
        let required = self.required_secondary_acks as usize;
        if self.secondary_acked.len() < required {
            return None; // Not enough secondaries have reported yet
        }
        // Fast path for RF=2 (required=1): just return the max offset.
        if required == 1 {
            return self.secondary_acked.values().copied().max();
        }
        // General case: use a stack-allocated array for secondary offsets.
        // MAX_REPLICATION_FACTOR includes the Primary, so max secondaries = MAX_RF - 1.
        // Collect into a fixed buffer, sort descending, pick the required-th.
        const MAX_SECONDARIES: usize = common::config::MAX_REPLICATION_FACTOR - 1;
        debug_assert!(
            self.secondary_acked.len() <= MAX_SECONDARIES,
            "secondary count {} exceeds MAX_SECONDARIES {}",
            self.secondary_acked.len(),
            MAX_SECONDARIES,
        );
        let mut offsets = [0u64; MAX_SECONDARIES];
        let mut count = 0;
        for &offset in self.secondary_acked.values() {
            if count < offsets.len() {
                offsets[count] = offset;
                count += 1;
            }
        }
        let slice = &mut offsets[..count];
        slice.sort_unstable_by(|a, b| b.cmp(a)); // descending
        slice.get(required - 1).copied()
    }

    /// Record a cumulative ACK from a secondary at a given offset.
    pub fn ack_from_secondary(&mut self, addr: &str, offset: u64) {
        let entry = self.secondary_acked.entry(addr.to_string()).or_insert(0);
        if offset > *entry {
            *entry = offset;
        }
    }

    /// Drain all pending ACKs that have reached quorum, sending AppendAck
    /// frames back to the client connections.
    ///
    /// After the normal quorum drain, sweeps the front of the queue for expired
    /// entries (older than the configured replication timeout) and sends error responses.
    pub fn drain_quorum(&mut self) {
        let qo = self.quorum_offset();
        if let Some(qo) = qo {
            while let Some(front) = self.pending.front() {
                if front.assigned_offset <= qo {
                    let ack = self.pending.pop_front().unwrap();
                    let frame = Frame::new(
                        VariableHeader::AppendAck {
                            request_id: ack.request_id,
                            stream_id: ack.stream_id,
                            epoch: ack.epoch,
                            extent_id: ack.extent_id,
                            offset: Offset(ack.assigned_offset),
                        },
                        None,
                    );
                    // Best-effort send — if the client disconnected, the channel is closed.
                    let _ = ack.response_tx.try_send(frame);
                } else {
                    break;
                }
            }
        }

        // Timeout sweep: expire PendingAcks older than the configured replication timeout.
        // Queue is ordered by creation time, so stop at the first non-expired entry.
        let now = Instant::now();
        while let Some(front) = self.pending.front() {
            if now.duration_since(front.created_at) > self.replication_timeout {
                let ack = self.pending.pop_front().unwrap();
                warn!(
                    request_id = ack.request_id,
                    stream_id = %ack.stream_id,
                    extent_id = %ack.extent_id,
                    offset = ack.assigned_offset,
                    "PendingAck expired after replication timeout",
                );
                let frame = Frame::append_ack_error(
                    ack.request_id,
                    ack.stream_id,
                    ack.epoch,
                    ack.extent_id,
                    ErrorCode::InternalError,
                    "replication timeout",
                );
                let _ = ack.response_tx.try_send(frame);
            } else {
                break;
            }
        }
    }
}

/// Notification emitted by the Primary to update Stream Manager about extent state.
/// Sent to the SM connection task which forwards it as an UPDATE_EXTENT frame.
#[derive(Debug, Clone)]
pub enum ExtentUpdate {
    /// Extent was sealed and a new one created (autonomous extent creation).
    Sealed {
        stream_id: StreamId,
        sealed_extent_id: ExtentId,
        end_offset: u64,
        new_extent_id: ExtentId,
        new_extent_capacity: u32,
        epoch: Epoch,
    },
    /// Periodic progress report for an active extent (observability).
    Progress {
        stream_id: StreamId,
        extent_id: ExtentId,
        current_offset: u64,
        epoch: Epoch,
    },
}

// ── Replica info ─────────────────────────────────────────────────────────────

/// Replication role and topology info for a single extent on this ExtentNode.
#[derive(Debug, Clone)]
pub struct ReplicaInfo {
    /// Stream this extent belongs to.
    pub stream_id: StreamId,
    /// Extent this replica assignment covers.
    pub extent_id: ExtentId,
    /// 0 = Primary, 1+ = Secondary.
    pub role: u8,
    /// Total replication factor (used for quorum calculation).
    pub replication_factor: u16,
    /// All secondary addresses (Primary only). Empty for secondaries.
    pub replica_addrs: Vec<String>,
}

impl ReplicaInfo {
    pub fn is_primary(&self) -> bool {
        self.role == ROLE_PRIMARY
    }

    /// True if RF=1 (no secondaries needed). Immediate ACK.
    pub fn is_standalone(&self) -> bool {
        self.replication_factor <= 1 || self.replica_addrs.is_empty()
    }

    /// Number of secondary ACKs required for quorum.
    /// Formula: rf / 2 (integer division).
    /// RF=1: 0, RF=2: 1, RF=3: 1, RF=4: 2
    pub fn required_secondary_acks(&self) -> u32 {
        (self.replication_factor as u32) / 2
    }
}

// ── Pipelined group commit types ─────────────────────────────────────────────

/// A pending append job delegated from a follower to the active writer.
///
/// When a thread arrives at a stream and finds another writer already active
/// (via `in_flight` counter), it pushes an `AppendJob` into the stream's channel
/// and returns immediately. The active writer drains these jobs as a batch.
pub(crate) struct AppendJob {
    pub request_id: u32,
    pub stream_id: StreamId,
    pub payload: Bytes,
    /// Channel back to the client connection for sending response frames.
    /// `None` in test mode (no client connection).
    pub response_tx: Option<Sender<Frame>>,
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
    streams: papaya::HashMap<StreamId, Stream>,
    /// Monotonic stream ID generator (atomic, no lock needed).
    next_stream_id: AtomicU64,
    /// Replication info per stream_id (registered via RegisterExtent).
    /// Fine-grained per-stream locking.
    replicas: papaya::HashMap<StreamId, Mutex<ReplicaInfo>>,
    /// Direct TCP connection pool for broadcast replication (None for standalone/test mode).
    /// Initialized via `set_downstream()` after construction (OnceLock breaks circular dep).
    downstream: OnceLock<Arc<DownstreamPool>>,
    /// Channel to send ExtentUpdate notifications to SM (Primary only).
    /// The SM connection task receives these and sends UPDATE_EXTENT frames.
    update_tx: Option<Sender<ExtentUpdate>>,
    /// Per-stream ACK queues for the Primary (only used when this node is Primary for a stream).
    /// Fine-grained per-stream locking.
    pub ack_queues: papaya::HashMap<StreamId, Mutex<AckQueue>>,
    /// Configurable timeout for replication quorum ACK expiry.
    replication_timeout: Duration,
    // -- Metrics counters (reset on each heartbeat snapshot) --
    /// Total appends since last snapshot (atomic, no lock needed).
    append_count: AtomicU64,
    /// Total bytes written since last snapshot (atomic, no lock needed).
    bytes_written: AtomicU64,
}

impl ExtentNodeStore {
    /// Create a new store in standalone mode (no replication) with default arena capacity.
    pub fn new() -> Self {
        Self {
            streams: papaya::HashMap::new(),
            next_stream_id: AtomicU64::new(1),
            replicas: papaya::HashMap::new(),
            downstream: OnceLock::new(),
            update_tx: None,
            ack_queues: papaya::HashMap::new(),
            replication_timeout: DEFAULT_REPLICATION_TIMEOUT,
            append_count: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
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

    /// Set the seal request channel (called during ExtentNode bootstrap).
    pub fn set_update_tx(&mut self, update_tx: Sender<ExtentUpdate>) {
        self.update_tx = Some(update_tx);
    }

    /// Get the replication info for a stream, if registered via RegisterExtent.
    pub fn get_replica_info(&self, stream_id: StreamId) -> Option<ReplicaInfo> {
        self.replicas.pin().get(&stream_id).map(|m| m.lock().unwrap().clone())
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
            v.lock().unwrap().drain_quorum();
        }
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
        guard
            .iter()
            .map(|(k, v)| (*k, v.epoch()))
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

impl ExtentNodeStore {
    /// Handle RegisterExtent from StreamManager: assign this ExtentNode a role in broadcast replication.
    ///
    /// Creates the stream locally (with the StreamManager-assigned stream_id) and stores replica info.
    fn handle_register_extent(&self, frame: Frame) -> Frame {
        // Extract stream_id, extent_id, role, replication_factor from the variable header.
        let (
            stream_id,
            extent_id,
            role,
            replication_factor,
            epoch,
            extent_capacity,
            cache_extents,
            extent_growth_factor,
        ) = match &frame.variable_header {
            VariableHeader::RegisterExtent {
                stream_id,
                extent_id,
                role,
                replication_factor,
                epoch,
                extent_capacity,
                cache_extents,
                extent_growth_factor,
                ..
            } => (
                *stream_id,
                *extent_id,
                *role,
                *replication_factor,
                *epoch,
                *extent_capacity,
                *cache_extents,
                *extent_growth_factor,
            ),
            _ => {
                return Frame::error_from_request(
                    &frame,
                    ErrorCode::InternalError,
                    "invalid RegisterExtent frame",
                    ExtentId(0),
                );
            }
        };

        // Parse replica addresses from the payload.
        let replica_addrs =
            match parse_register_extent_payload(frame.payload.as_deref().unwrap_or_default()) {
                Some(addrs) => addrs,
                None => {
                    return Frame::error_from_request(
                        &frame,
                        ErrorCode::InternalError,
                        "invalid RegisterExtent payload",
                        ExtentId(0),
                    );
                }
            };

        // Create the stream locally if it doesn't exist, then register the new extent.
        // Skip extent creation if it already exists (idempotent — extent may have been
        // lazily created by a forwarded append that arrived before this RegisterExtent).
        let _start_offset = {
            let guard = self.streams.pin();
            if let Some(stream_ref) = guard.get(&stream_id) {
                // RegisterExtent is the authoritative source for cache policy.
                // Always apply — the stream may have been lazily created by
                // ForwardInitExtent before this arrives with max_extents=0.
                stream_ref.set_max_extents(cache_extents as usize);
                if stream_ref.with_extent(extent_id, |_| ()).is_none() {
                    let so = stream_ref.max_offset();
                    stream_ref.register_extent(
                        extent_id,
                        so,
                        extent_capacity,
                        epoch,
                        DEFAULT_MIN_EXTENT_CAPACITY,
                        DEFAULT_MAX_EXTENT_CAPACITY,
                        extent_growth_factor,
                    );
                    so
                } else {
                    // Extent already exists (lazy creation from Forward), but update epoch
                    // from authoritative source (RegisterExtent carries the real epoch).
                    stream_ref.set_epoch(epoch);
                    stream_ref.max_offset()
                }
            } else {
                let stream = Stream::new(stream_id);
                stream.set_max_extents(cache_extents as usize);
                stream.register_extent(
                    extent_id,
                    Offset(0),
                    extent_capacity,
                    epoch,
                    DEFAULT_MIN_EXTENT_CAPACITY,
                    DEFAULT_MAX_EXTENT_CAPACITY,
                    extent_growth_factor,
                );
                guard.insert(stream_id, stream);
                Offset(0)
            }
        };

        // Update next_stream_id to avoid collision with StreamManager-assigned IDs.
        // Use fetch_max to atomically ensure we stay above the assigned ID.
        self.next_stream_id
            .fetch_max(stream_id.0 + 1, Ordering::Relaxed);

        let role_name = if role == ROLE_PRIMARY {
            "Primary"
        } else {
            &format!("Secondary-{}", role)
        };
        let addrs_info = if replica_addrs.is_empty() {
            "none".to_string()
        } else {
            replica_addrs.join(", ")
        };
        info!(
            "RegisterExtent: stream={}, extent={}, role={role_name}, rf={}, secondaries=[{addrs_info}]",
            stream_id, extent_id, replication_factor,
        );

        let ri = ReplicaInfo {
            stream_id,
            extent_id,
            role,
            replication_factor,
            replica_addrs,
        };

        // If this node is Primary, initialize an AckQueue.
        if ri.is_primary() {
            {
                let aq_guard = self.ack_queues.pin();
                aq_guard.get_or_insert_with(stream_id, || {
                    Mutex::new(AckQueue::with_timeout(ri.required_secondary_acks(), self.replication_timeout))
                });
            }

            // Cache per-secondary Sender handles in the Stream so the
            // hot append path can push Forward frames with zero lookup overhead.
            if !ri.replica_addrs.is_empty() {
                if let Some(pool) = self.downstream.get() {
                    let txs: Vec<_> = ri
                        .replica_addrs
                        .iter()
                        .map(|addr| pool.get_or_create_sender(addr))
                        .collect();
                    let stream_guard = self.streams.pin();
                    if let Some(stream_ref) = stream_guard.get(&stream_id) {
                        stream_ref.set_downstream_txs(txs);
                    }
                }
            }
        }

        self.replicas.pin().insert(stream_id, Mutex::new(ri));

        Frame::new(
            VariableHeader::RegisterExtentAck {
                request_id: frame.request_id(),
                stream_id,
                extent_id,
            },
            None,
        )
    }

    /// Handle Append — pipelined group commit with stream-level leader election.
    ///
    /// Uses per-stream `in_flight` counter for leader election:
    /// - `prev == 0`: This thread becomes the active writer (fast path).
    ///   Appends its own payload, then drains any follower jobs from the channel.
    /// - `prev > 0`: An active writer exists. Push an `AppendJob` to the channel
    ///   and return `None` immediately (deferred ACK).
    ///
    /// The active writer handles replication (Forward + PendingAck for RF≥2)
    /// or sends immediate AppendAck (RF=1/standalone) for each job.
    /// On ExtentFull, the leader seals the active extent, creates a new one,
    /// and retries — all transparently within the same leader tenure.
    ///
    /// Pin guards are scoped in blocks so they're dropped before `.await` points
    /// (papaya pin guards are non-Send).
    async fn handle_append(
        &self,
        frame: Frame,
        response_tx: Option<&Sender<Frame>>,
    ) -> Option<Frame> {
        let stream_id = frame.stream_id();
        let client_epoch = frame.epoch();

        // ── Validation + leader election + own append (scoped pin guard) ──
        let (epoch, own_result, extent_full, remaining, is_tick, should_shrink, payload, request_id) = {
            let guard = self.streams.pin();
            let stream_ref = match guard.get(&stream_id) {
                Some(s) => s,
                None => {
                    return Some(Frame::error_from_request(
                        &frame,
                        ErrorCode::UnknownStream,
                        &format!("stream {} not found", stream_id),
                        ExtentId(0),
                    ));
                }
            };

            let epoch = stream_ref.epoch();
            if client_epoch != Epoch(0) && client_epoch != epoch {
                return Some(Frame::error_from_request(
                    &frame,
                    ErrorCode::EpochStale,
                    &format!("epoch stale: client={}, current={}", client_epoch, epoch),
                    ExtentId(0),
                ));
            }

            let is_tick = frame.flags() & FLAG_SYSTEM_TICK != 0;
            let prev = stream_ref.in_flight().fetch_add(1, Ordering::Acquire);

            if prev > 0 {
                if is_tick {
                    stream_ref.in_flight().fetch_sub(1, Ordering::Release);
                    return None;
                }
                let job = AppendJob {
                    request_id: frame.request_id(),
                    stream_id,
                    payload: frame.payload.clone().unwrap_or_default(),
                    response_tx: response_tx.cloned(),
                };
                let _ = stream_ref.job_tx().send(job);
                return None;
            }

            // FAST PATH: I'm the active writer (prev == 0).

            if is_tick {
                let should_shrink = stream_ref
                    .should_idle_shrink(Duration::from_secs(DEFAULT_IDLE_SHRINK_THRESHOLD_SECS));
                let remaining = stream_ref.in_flight().fetch_sub(1, Ordering::Release);
                (epoch, None, false, remaining, true, should_shrink, Bytes::new(), 0)
            } else {
                let payload = frame.payload.clone().unwrap_or_default();
                let request_id = frame.request_id();
                let (own_result, extent_full) = self.do_append_and_respond(
                    stream_ref, request_id, stream_id, epoch, payload.clone(), response_tx.cloned(),
                );
                if extent_full {
                    // Don't decrement in_flight — we're still the leader.
                    // Will decrement after seal+create+retry below.
                    (epoch, own_result, true, 0, false, false, payload, request_id)
                } else {
                    let remaining = stream_ref.in_flight().fetch_sub(1, Ordering::Release);
                    (epoch, own_result, false, remaining, false, false, payload, request_id)
                }
            }
        };
        // Pin guard dropped — safe to .await.

        // ── System tick path ──
        if is_tick {
            if remaining > 1 {
                let batch_seals = self.drain_follower_jobs(stream_id).await;
                for notification in &batch_seals {
                    self.send_extent_update(stream_id, notification);
                    self.send_forward_checksum(stream_id, notification.sealed_extent_id);
                }
            }
            if should_shrink {
                if let Some(ref notification) =
                    self.seal_and_create(stream_id, SealReason::IdleShrink)
                {
                    self.send_extent_update(stream_id, notification);
                    self.send_forward_checksum(stream_id, notification.sealed_extent_id);
                    info!(
                        "idle-shrink: stream={}, sealed={}, new={}, capacity={}",
                        stream_id, notification.sealed_extent_id,
                        notification.new_extent_id, notification.new_extent_capacity,
                    );
                }
            }
            return None;
        }

        // ── Extent-full path: seal+create, retry, then drain ──
        if extent_full {
            let seal_notification = self.seal_and_create(stream_id, SealReason::ExtentFull);

            // Re-acquire pin guard for retry on the new extent.
            let (retry_result, remaining) = {
                let guard = self.streams.pin();
                match guard.get(&stream_id) {
                    Some(stream_ref) => {
                        let (retry_result, _) = self.do_append_and_respond(
                            stream_ref, request_id, stream_id, epoch, payload, response_tx.cloned(),
                        );
                        let remaining = stream_ref.in_flight().fetch_sub(1, Ordering::Release);
                        (retry_result, remaining)
                    }
                    None => return None,
                }
            };

            if remaining > 1 {
                let batch_seals = self.drain_follower_jobs(stream_id).await;
                for notification in &batch_seals {
                    self.send_extent_update(stream_id, notification);
                    self.send_forward_checksum(stream_id, notification.sealed_extent_id);
                }
            }
            if let Some(ref notification) = seal_notification {
                self.send_extent_update(stream_id, notification);
                self.send_forward_checksum(stream_id, notification.sealed_extent_id);
            }
            return retry_result;
        }

        // ── Normal path: drain followers if any arrived ──
        if remaining > 1 {
            let batch_seals = self.drain_follower_jobs(stream_id).await;
            for notification in &batch_seals {
                self.send_extent_update(stream_id, notification);
            }
        }

        own_result
    }

    /// Perform a single append via the stream's active extent and handle replication / ACK.
    ///
    /// Returns `(Option<Frame>, bool)`:
    /// - Option<Frame>: response frame (None if deferred or sent via channel)
    /// - bool: whether ExtentFull occurred (caller should trigger proactive seal)
    ///
    /// Forward frames are pushed **inline** into the stream's cached per-secondary
    /// mpsc channels, while the leader still holds `in_flight > 0`. This guarantees
    /// FIFO ordering — no subsequent leader can push frames before us.
    ///
    /// When response_tx is Some, success ACKs are sent via the channel and the
    /// Frame return is None. When response_tx is None, success ACKs are returned
    /// as Some(Frame).
    fn do_append_and_respond(
        &self,
        stream: &Stream,
        request_id: u32,
        stream_id: StreamId,
        epoch: Epoch,
        payload: Bytes,
        response_tx: Option<Sender<Frame>>,
    ) -> (Option<Frame>, bool) {
        let payload_len = payload.len();
        let payload_for_forward = payload.clone();

        // Write locally via single-writer append on the active extent.
        let (append_result, extent_id) = match stream.try_append_active(payload) {
            Ok(r) => r,
            Err(StorageError::ExtentSealed(extent_id)) => {
                let err = Frame::append_ack_error(
                    request_id,
                    stream_id,
                    epoch,
                    extent_id,
                    ErrorCode::ExtentSealed,
                    "extent is sealed",
                );
                if let Some(ref tx) = response_tx {
                    let _ = tx.try_send(err);
                    return (None, false);
                }
                return (Some(err), false);
            }
            Err(StorageError::ExtentFull(_)) => {
                // Don't send error to client — the caller will seal, create a new extent,
                // and retry the append transparently. Return extent_full=true.
                return (None, true);
            }
            Err(e) => {
                let err = Frame::append_ack_error(
                    request_id,
                    stream_id,
                    epoch,
                    ExtentId(0),
                    ErrorCode::InternalError,
                    &e.to_string(),
                );
                if let Some(ref tx) = response_tx {
                    let _ = tx.try_send(err);
                    return (None, false);
                }
                return (Some(err), false);
            }
        };

        let offset = append_result.offset;
        let _extent_start_offset = stream
            .with_extent(extent_id, |e| e.start_offset.0)
            .unwrap_or(0);

        // Update metrics counters.
        self.append_count.fetch_add(1, Ordering::Relaxed);
        self.bytes_written
            .fetch_add(payload_len as u64, Ordering::Relaxed);

        // Check replica info for this stream.
        let replica = self.replicas.pin().get(&stream_id).map(|m| m.lock().unwrap().clone());

        match replica {
            None => {
                // Standalone mode: immediate ACK.
                let ack = Frame::new(
                    VariableHeader::AppendAck {
                        request_id,
                        stream_id,
                        epoch,
                        extent_id,
                        offset,
                    },
                    None,
                );
                if let Some(ref tx) = response_tx {
                    let _ = tx.try_send(ack);
                    (None, false)
                } else {
                    (Some(ack), false)
                }
            }
            Some(ref ri) if ri.is_primary() => {
                if ri.is_standalone() {
                    // RF=1: no secondaries, ACK immediately.
                    let ack = Frame::new(
                        VariableHeader::AppendAck {
                            request_id,
                            stream_id,
                            epoch,
                            extent_id,
                            offset,
                        },
                        None,
                    );
                    if let Some(ref tx) = response_tx {
                        let _ = tx.try_send(ack);
                        (None, false)
                    } else {
                        (Some(ack), false)
                    }
                } else {
                    // RF≥2: push Forward frames inline into per-stream channels.
                    if stream.has_secondaries() {
                        let forward_frame = Frame::new(
                            VariableHeader::Forward {
                                stream_id,
                                extent_id,
                                epoch,
                                offset,
                                byte_pos: append_result.byte_pos,
                            },
                            Some(payload_for_forward),
                        );
                        // Inject ForwardInitExtent if this is the first forward for the extent.
                        if let Some(init) = self.maybe_build_init_forward(stream, &forward_frame) {
                            stream.send_forward(init);
                        }
                        stream.send_forward(forward_frame);
                    }

                    // Queue deferred ACK.
                    if let Some(ref resp_tx) = response_tx {
                        let aq_guard = self.ack_queues.pin();
                        let aq_mutex = aq_guard.get_or_insert_with(stream_id, || {
                            Mutex::new(AckQueue::with_timeout(
                                ri.required_secondary_acks(),
                                self.replication_timeout,
                            ))
                        });
                        let mut ack_queue = aq_mutex.lock().unwrap();
                        ack_queue.pending.push_back(PendingAck {
                            request_id,
                            stream_id,
                            response_tx: resp_tx.clone(),
                            assigned_offset: offset.0,
                            extent_id,
                            epoch,
                            created_at: Instant::now(),
                        });
                    }

                    (None, false)
                }
            }
            Some(_) => {
                // Secondary received normal Append (not Forward) — shouldn't normally happen.
                let ack = Frame::new(
                    VariableHeader::AppendAck {
                        request_id,
                        stream_id,
                        epoch,
                        extent_id,
                        offset,
                    },
                    None,
                );
                if let Some(ref tx) = response_tx {
                    let _ = tx.try_send(ack);
                    (None, false)
                } else {
                    (Some(ack), false)
                }
            }
        }
    }

    /// Drain follower append jobs from the stream's channel and process them.
    ///
    /// Called by the active writer after its own append when `in_flight > 1`.
    /// Loops until all followers have been processed.
    ///
    /// Forward frames are pushed inline by `do_append_and_respond` — this method
    /// only returns seal notifications for the caller to send SM updates.
    ///
    /// On ExtentFull, this method calls `seal_and_create` (which manages its own
    /// pin guard) and retries the remaining jobs on the new extent.
    /// Pin guards are scoped in blocks so they're dropped before `yield_now().await`.
    async fn drain_follower_jobs(&self, stream_id: StreamId) -> Vec<SealNotification> {
        let mut all_seal_notifications = Vec::new();

        loop {
            // ── Phase 1: Drain jobs from the channel ──
            let mut batch: Vec<AppendJob> = Vec::new();
            let mut epoch = Epoch(0);
            loop {
                // Scope pin guard — must be dropped before yield_now().await.
                let need_yield = {
                    let guard = self.streams.pin();
                    let stream_ref = match guard.get(&stream_id) {
                        Some(s) => s,
                        None => return all_seal_notifications,
                    };
                    if batch.is_empty() {
                        epoch = stream_ref.epoch();
                    }
                    match stream_ref.job_rx().try_recv() {
                        Ok(job) => {
                            batch.push(job);
                            while let Ok(job) = stream_ref.job_rx().try_recv() {
                                batch.push(job);
                            }
                            false
                        }
                        Err(_) if !batch.is_empty() => false,
                        Err(_) => {
                            // Follower incremented in_flight but hasn't pushed yet.
                            let delegated = stream_ref.in_flight().load(Ordering::Acquire);
                            delegated > 0
                        }
                    }
                };
                if need_yield {
                    tokio::task::yield_now().await;
                } else {
                    break;
                }
            }

            // ── Phase 2: Process the batch ──
            let batch_len = batch.len();
            let mut extent_full_idx: Option<usize> = None;

            // Process each job (scoped pin guard).
            {
                let guard = self.streams.pin();
                let stream_ref = match guard.get(&stream_id) {
                    Some(s) => s,
                    None => break,
                };
                for (i, job) in batch.iter().enumerate() {
                    let (_, extent_full) = self.do_append_and_respond(
                        stream_ref,
                        job.request_id,
                        job.stream_id,
                        epoch,
                        job.payload.clone(),
                        job.response_tx.clone(),
                    );
                    if extent_full {
                        extent_full_idx = Some(i);
                        break;
                    }
                }
            }
            // Pin guard dropped.

            if let Some(index) = extent_full_idx {
                let seal_notification = self.seal_and_create(stream_id, SealReason::ExtentFull);
                if let Some(ref notification) = seal_notification {
                    all_seal_notifications.push(notification.clone());
                }

                // Retry the failed job and remaining jobs on the new extent.
                let done = {
                    let guard = self.streams.pin();
                    if let Some(stream_ref) = guard.get(&stream_id) {
                        epoch = stream_ref.epoch();
                        for job in &batch[index..] {
                            let (_, _) = self.do_append_and_respond(
                                stream_ref,
                                job.request_id,
                                job.stream_id,
                                epoch,
                                job.payload.clone(),
                                job.response_tx.clone(),
                            );
                        }
                        let remaining = stream_ref
                            .in_flight()
                            .fetch_sub(batch_len as u64, Ordering::Release);
                        remaining <= batch_len as u64
                    } else {
                        true
                    }
                };
                if done {
                    break;
                }
            } else {
                // All jobs processed without ExtentFull.
                let done = {
                    let guard = self.streams.pin();
                    if let Some(stream_ref) = guard.get(&stream_id) {
                        let remaining = stream_ref
                            .in_flight()
                            .fetch_sub(batch_len as u64, Ordering::Release);
                        remaining <= batch_len as u64
                    } else {
                        true
                    }
                };
                if done {
                    break;
                }
            }
            // More followers arrived during processing — loop again.
        }

        all_seal_notifications
    }

    /// Seal the active extent and create a new one.
    ///
    /// Acquires write lock on the stream's inner RwLock. Returns the seal notification
    /// if a seal+create occurred, or None if already sealed / stream not found.
    fn seal_and_create(&self, stream_id: StreamId, reason: SealReason) -> Option<SealNotification> {
        if let Some(stream_ref) = self.streams.pin().get(&stream_id) {
            // For IdleShrink, re-check eligibility under write guard.
            if matches!(reason, SealReason::IdleShrink)
                && !stream_ref
                    .should_idle_shrink(Duration::from_secs(DEFAULT_IDLE_SHRINK_THRESHOLD_SECS))
            {
                return None;
            }
            let t0 = std::time::Instant::now();
            let notification = stream_ref.seal_and_create_next(reason);
            let seal_us = t0.elapsed().as_micros();
            if let Some(ref n) = notification {
                info!(
                    "seal_and_create: stream={}, sealed={}, new={}, capacity={}, reason={:?}, duration={}us",
                    stream_id,
                    n.sealed_extent_id,
                    n.new_extent_id,
                    n.new_extent_capacity,
                    reason,
                    seal_us,
                );
                if let Some(ri) = self.replicas.pin().get(&stream_id) {
                    ri.lock().unwrap().extent_id = n.new_extent_id;
                }
            }
            notification
        } else {
            None
        }
    }

    /// Send an async UPDATE_EXTENT (Sealed) to SM (fire-and-forget).
    fn send_extent_update(&self, stream_id: StreamId, notification: &SealNotification) {
        if let Some(ref tx) = self.update_tx {
            let _ = tx.try_send(ExtentUpdate::Sealed {
                stream_id,
                sealed_extent_id: notification.sealed_extent_id,
                end_offset: notification.end_offset,
                new_extent_id: notification.new_extent_id,
                new_extent_capacity: notification.new_extent_capacity,
                epoch: notification.epoch,
            });
        }
    }

    /// Send a ForwardChecksum for a sealed extent inline via per-stream channels.
    ///
    /// Fire-and-forget: the secondary defers verification via `try_verify_checksum()`.
    fn send_forward_checksum(&self, stream_id: StreamId, sealed_extent_id: ExtentId) {
        let (checksum, committed_bytes) = {
            let guard = self.streams.pin();
            match guard.get(&stream_id) {
                Some(stream_ref) => match stream_ref.with_extent(sealed_extent_id, |ext| {
                    (
                        ext.finalized_crc32().unwrap_or(0),
                        ext.committed_data().len() as u64,
                    )
                }) {
                    Some(pair) => pair,
                    None => return,
                },
                None => return,
            }
        };
        debug!(
            "ForwardChecksum sent: stream={}, extent={}, crc32={:#x}, bytes={}",
            stream_id, sealed_extent_id, checksum, committed_bytes,
        );
        let frame = Frame::new(
            VariableHeader::ForwardChecksum {
                stream_id,
                extent_id: sealed_extent_id,
                checksum,
                committed_bytes,
            },
            None,
        );
        // Push inline via per-stream channels.
        let guard2 = self.streams.pin();
        if let Some(stream_ref) = guard2.get(&stream_id) {
            if let Some(init) = self.maybe_build_init_forward(stream_ref, &frame) {
                stream_ref.send_forward(init);
            }
            stream_ref.send_forward(frame);
        }
    }

    /// Optimized batch append: all frames share the same stream_id/epoch.
    ///
    /// Amortizes map lookups (3N → 3), leader elections (N → 1),
    /// ReplicaInfo access (N clones → 0, borrow within guard), and
    /// atomic operations (2N → 2).
    ///
    /// Pin guards are scoped in blocks so they're dropped before `.await` points
    /// (papaya pin guards are non-Send).
    async fn handle_append_batch_inner(
        &self,
        frames: &[Frame],
        response_tx: Option<&Sender<Frame>>,
    ) -> Vec<Frame> {
        let stream_id = frames[0].stream_id();

        struct BatchEntry {
            request_id: u32,
            payload_for_forward: Bytes,
            offset: Offset,
            byte_pos: u64,
            payload_len: usize,
            extent_id: ExtentId,
        }
        struct FailedFrame {
            request_id: u32,
            payload: Bytes,
        }

        let mut responses = Vec::new();
        let mut entries: Vec<BatchEntry> = Vec::with_capacity(frames.len());
        let mut failed_frames: Vec<FailedFrame> = Vec::new();
        let mut extent_full = false;

        // ── Validation + leader election + batch appends (scoped pin guard) ──
        let (epoch, batch_len) = {
            let guard = self.streams.pin();
            let stream_ref = match guard.get(&stream_id) {
                Some(s) => s,
                None => {
                    for frame in frames {
                        responses.push(Frame::error_from_request(
                            frame,
                            ErrorCode::UnknownStream,
                            &format!("stream {} not found", stream_id),
                            ExtentId(0),
                        ));
                    }
                    return responses;
                }
            };

            let epoch = stream_ref.epoch();
            let client_epoch = frames[0].epoch();
            if client_epoch != Epoch(0) && client_epoch != epoch {
                for frame in frames {
                    responses.push(Frame::error_from_request(
                        frame,
                        ErrorCode::EpochStale,
                        &format!("epoch stale: client={}, current={}", client_epoch, epoch),
                        ExtentId(0),
                    ));
                }
                return responses;
            }

            let batch_len = frames.len() as u64;
            let prev = stream_ref.in_flight().fetch_add(batch_len, Ordering::Acquire);

            if prev > 0 {
                // SLOW PATH: active writer exists. Push all as AppendJobs.
                for frame in frames {
                    let job = AppendJob {
                        request_id: frame.request_id(),
                        stream_id,
                        payload: frame.payload.clone().unwrap_or_default(),
                        response_tx: response_tx.cloned(),
                    };
                    let _ = stream_ref.job_tx().send(job);
                }
                return responses; // All deferred — empty responses.
            }

            // FAST PATH: I'm the active writer (prev == 0).
            for frame in frames {
                let request_id = frame.request_id();
                let payload = frame.payload.clone().unwrap_or_default();
                let payload_len = payload.len();
                let payload_for_forward = payload.clone();

                if extent_full {
                    failed_frames.push(FailedFrame { request_id, payload });
                    continue;
                }

                match stream_ref.try_append_active(payload.clone()) {
                    Ok((result, eid)) => {
                        entries.push(BatchEntry {
                            request_id, payload_for_forward, offset: result.offset,
                            byte_pos: result.byte_pos, payload_len, extent_id: eid,
                        });
                    }
                    Err(StorageError::ExtentSealed(extent_id)) => {
                        let err = Frame::append_ack_error(request_id, stream_id, epoch, extent_id, ErrorCode::ExtentSealed, "extent is sealed");
                        if let Some(tx) = response_tx { let _ = tx.try_send(err); } else { responses.push(err); }
                    }
                    Err(StorageError::ExtentFull(_)) => {
                        extent_full = true;
                        failed_frames.push(FailedFrame { request_id, payload });
                    }
                    Err(e) => {
                        let err = Frame::append_ack_error(request_id, stream_id, epoch, ExtentId(0), ErrorCode::InternalError, &e.to_string());
                        if let Some(tx) = response_tx { let _ = tx.try_send(err); } else { responses.push(err); }
                    }
                }
            }

            // Process successful entries: metrics, replica info, forwards, ACKs.
            if !entries.is_empty() {
                let _extent_start_offset = stream_ref
                    .with_extent(entries[0].extent_id, |e| e.start_offset.0)
                    .unwrap_or(0);

                let total_bytes: u64 = entries.iter().map(|e| e.payload_len as u64).sum();
                self.append_count.fetch_add(entries.len() as u64, Ordering::Relaxed);
                self.bytes_written.fetch_add(total_bytes, Ordering::Relaxed);

                let replica = self.replicas.pin().get(&stream_id).map(|m| m.lock().unwrap().clone());

                match replica.as_ref() {
                    None => {
                        for entry in &entries {
                            let ack = Frame::new(VariableHeader::AppendAck { request_id: entry.request_id, stream_id, epoch, extent_id: entry.extent_id, offset: entry.offset }, None);
                            if let Some(tx) = response_tx { let _ = tx.try_send(ack); } else { responses.push(ack); }
                        }
                    }
                    Some(ri) if ri.is_primary() => {
                        if ri.is_standalone() {
                            for entry in &entries {
                                let ack = Frame::new(VariableHeader::AppendAck { request_id: entry.request_id, stream_id, epoch, extent_id: entry.extent_id, offset: entry.offset }, None);
                                if let Some(tx) = response_tx { let _ = tx.try_send(ack); } else { responses.push(ack); }
                            }
                        } else {
                            if stream_ref.has_secondaries() {
                                for entry in &entries {
                                    let forward_frame = Frame::new(VariableHeader::Forward { stream_id, extent_id: entry.extent_id, epoch, offset: entry.offset, byte_pos: entry.byte_pos }, Some(entry.payload_for_forward.clone()));
                                    if let Some(init) = self.maybe_build_init_forward(stream_ref, &forward_frame) {
                                        stream_ref.send_forward(init);
                                    }
                                    stream_ref.send_forward(forward_frame);
                                }
                            }
                            if let Some(resp_tx) = response_tx {
                                let aq_guard = self.ack_queues.pin();
                                let aq_mutex = aq_guard.get_or_insert_with(stream_id, || {
                                    Mutex::new(AckQueue::with_timeout(ri.required_secondary_acks(), self.replication_timeout))
                                });
                                let mut ack_queue = aq_mutex.lock().unwrap();
                                let now = Instant::now();
                                for entry in &entries {
                                    ack_queue.pending.push_back(PendingAck {
                                        request_id: entry.request_id, stream_id,
                                        response_tx: resp_tx.clone(), assigned_offset: entry.offset.0,
                                        extent_id: entry.extent_id, epoch, created_at: now,
                                    });
                                }
                            }
                        }
                    }
                    Some(_) => {
                        for entry in &entries {
                            let ack = Frame::new(VariableHeader::AppendAck { request_id: entry.request_id, stream_id, epoch, extent_id: entry.extent_id, offset: entry.offset }, None);
                            if let Some(tx) = response_tx { let _ = tx.try_send(ack); } else { responses.push(ack); }
                        }
                    }
                }
            }

            (epoch, batch_len)
        };
        // Pin guard dropped — safe to .await.

        // ── Extent-full: seal+create, retry failed frames, then drain ──
        if extent_full {
            let seal_notification = self.seal_and_create(stream_id, SealReason::ExtentFull);

            // Retry failed frames on the new extent (scoped pin guard).
            let remaining = {
                let guard = self.streams.pin();
                if let Some(stream_ref) = guard.get(&stream_id) {
                    for ff in &failed_frames {
                        let (_, _) = self.do_append_and_respond(
                            stream_ref, ff.request_id, stream_id, epoch,
                            ff.payload.clone(), response_tx.cloned(),
                        );
                    }
                    stream_ref.in_flight().fetch_sub(batch_len, Ordering::Release)
                } else {
                    0
                }
            };

            if remaining > batch_len {
                let batch_seals = self.drain_follower_jobs(stream_id).await;
                for notif in &batch_seals {
                    self.send_extent_update(stream_id, notif);
                    self.send_forward_checksum(stream_id, notif.sealed_extent_id);
                }
            }
            if let Some(ref notif) = seal_notification {
                self.send_extent_update(stream_id, notif);
                self.send_forward_checksum(stream_id, notif.sealed_extent_id);
            }
            return responses;
        }

        // ── Normal path: decrement in_flight and drain followers if any ──
        let remaining = {
            let guard = self.streams.pin();
            if let Some(stream_ref) = guard.get(&stream_id) {
                stream_ref.in_flight().fetch_sub(batch_len, Ordering::Release)
            } else {
                0
            }
        };

        if remaining > batch_len {
            let batch_seals = self.drain_follower_jobs(stream_id).await;
            for notif in &batch_seals {
                self.send_extent_update(stream_id, notif);
                self.send_forward_checksum(stream_id, notif.sealed_extent_id);
            }
        }

        responses
    }

    /// Check if a Forward or ForwardChecksum frame targets an extent that
    /// needs ForwardInitExtent. Returns the init frame if so.
    ///
    /// Called on the leader side before pushing to the channel. FIFO channel
    /// ordering guarantees ForwardInitExtent arrives before the Forward frame
    /// on the wire. The atomic `take_init_forward()` ensures exactly-once
    /// semantics — the init frame is built once and cloned to all secondaries.
    ///
    /// Accepts a `&Stream` reference to avoid re-acquiring the map pin
    /// (the caller already holds a guard).
    fn maybe_build_init_forward(&self, stream: &Stream, frame: &Frame) -> Option<Frame> {
        let (stream_id, extent_id, epoch) = match &frame.variable_header {
            VariableHeader::Forward {
                stream_id,
                extent_id,
                epoch,
                ..
            } => (*stream_id, *extent_id, Some(*epoch)),
            VariableHeader::ForwardChecksum {
                stream_id,
                extent_id,
                ..
            } => (*stream_id, *extent_id, None),
            _ => return None,
        };

        stream.with_extent(extent_id, |ext| {
            if !ext.take_init_forward() {
                return None;
            }

            let epoch = epoch.unwrap_or(ext.epoch);
            Some(Frame::new(
                VariableHeader::ForwardInitExtent {
                    stream_id,
                    extent_id,
                    epoch,
                    start_offset: ext.start_offset,
                    extent_capacity: stream.extent_capacity(),
                    cache_extents: stream.max_extents() as u32,
                },
                None,
            ))
        })?
    }

    /// Handle ForwardInitExtent (0x0B, flag=0x01) — init-extent notification.
    ///
    /// Creates the stream (if needed) and registers the extent with the provided
    /// start_offset and extent_capacity. Fire-and-forget: no response.
    fn handle_forward_init_extent(&self, frame: Frame) {
        let (stream_id, extent_id, epoch, start_offset, extent_capacity, cache_extents) =
            match &frame.variable_header {
                VariableHeader::ForwardInitExtent {
                    stream_id,
                    extent_id,
                    epoch,
                    start_offset,
                    extent_capacity,
                    cache_extents,
                } => (
                    *stream_id,
                    *extent_id,
                    *epoch,
                    *start_offset,
                    *extent_capacity,
                    *cache_extents,
                ),
                _ => return,
            };

        if let Some(stream) = self.streams.pin().get(&stream_id) {
            // Apply cache policy if not yet set (RegisterExtent may arrive later).
            if cache_extents > 0 && stream.max_extents() == 0 {
                stream.set_max_extents(cache_extents as usize);
            }
            if stream.with_extent(extent_id, |_| ()).is_none() {
                stream.register_extent(
                    extent_id,
                    start_offset,
                    extent_capacity,
                    epoch,
                    DEFAULT_MIN_EXTENT_CAPACITY,
                    DEFAULT_MAX_EXTENT_CAPACITY,
                    DEFAULT_EXTENT_GROWTH_FACTOR,
                );
                info!(
                    "ForwardInitExtent: stream={}, extent={}, start_offset={}, capacity={}",
                    stream_id, extent_id, start_offset, extent_capacity,
                );
            }
        } else {
            let stream = Stream::new(stream_id);
            stream.set_max_extents(cache_extents as usize);
            stream.register_extent(
                extent_id,
                start_offset,
                extent_capacity,
                epoch,
                DEFAULT_MIN_EXTENT_CAPACITY,
                DEFAULT_MAX_EXTENT_CAPACITY,
                DEFAULT_EXTENT_GROWTH_FACTOR,
            );
            self.streams.pin().insert(stream_id, stream);
            self.next_stream_id
                .fetch_max(stream_id.0 + 1, Ordering::Relaxed);
            info!(
                "ForwardInitExtent (new stream): stream={}, extent={}, start_offset={}, capacity={}",
                stream_id, extent_id, start_offset, extent_capacity,
            );
        }
    }

    /// Handle Forward (0x0B, flag=0x00) — per-record primary→secondary replication.
    ///
    /// The Forward frame carries (stream_id, extent_id, epoch, offset, byte_pos)
    /// so the secondary writes each record at the exact same arena position as
    /// the primary. The stream/extent must already exist (created by a prior
    /// ForwardInitExtent or RegisterExtent).
    ///
    /// Returns a cumulative Watermark with the contiguous committed offset,
    /// or None if the forward cannot be processed (bad frame, unknown stream, etc.).
    fn handle_forward(&self, frame: Frame) -> Option<Frame> {
        let (stream_id, extent_id, _epoch, offset, byte_pos) = match &frame.variable_header {
            VariableHeader::Forward {
                stream_id,
                extent_id,
                epoch,
                offset,
                byte_pos,
            } => (*stream_id, *extent_id, *epoch, *offset, *byte_pos),
            _ => return None,
        };

        // Look up the stream — must exist (created by ForwardInitExtent or RegisterExtent).
        let streams = self.streams.pin();
        let stream_ref = match streams.get(&stream_id) {
            Some(s) => s,
            None => {
                warn!(
                    "Forward for unknown stream {}, extent {} — missing ForwardInitExtent?",
                    stream_id, extent_id,
                );
                return None;
            }
        };

        let replicate_result = stream_ref.replicate(
            extent_id,
            offset,
            byte_pos,
            frame.payload.clone().unwrap_or_default(),
        );

        self.finish_forward(stream_ref, stream_id, extent_id, replicate_result, &frame)
    }

    /// Shared tail of handle_forward: process replicate result, update metrics, return watermark.
    fn finish_forward(
        &self,
        stream_ref: &Stream,
        stream_id: StreamId,
        extent_id: ExtentId,
        replicate_result: Result<crate::extent::AppendResult, StorageError>,
        frame: &Frame,
    ) -> Option<Frame> {
        match replicate_result {
            Ok(_r) => {}
            Err(e) => {
                warn!(
                    "Forward replicate failed for stream={}, extent={}: {}",
                    stream_id, extent_id, e,
                );
                return None;
            }
        };

        // Update metrics counters.
        self.append_count.fetch_add(1, Ordering::Relaxed);
        self.bytes_written.fetch_add(
            frame.payload.as_ref().map_or(0, |p| p.len()) as u64,
            Ordering::Relaxed,
        );

        // Check if deferred CRC32 verification can now complete.
        // Also read the contiguous watermark for the response.
        let watermark = stream_ref.with_extent(extent_id, |extent| {
            match extent.try_verify_checksum() {
                Some(true) => {
                    info!(
                        "CRC32 checksum verified (deferred): stream={}, extent={}",
                        stream_id, extent_id,
                    );
                }
                Some(false) => {
                    warn!(
                        "CRC32 checksum mismatch (deferred): stream={}, extent={}",
                        stream_id, extent_id,
                    );
                }
                None => {} // not ready yet
            }
            extent.last_offset()
        })??;

        Some(Frame::new(
            VariableHeader::Watermark {
                stream_id,
                extent_id,
                offset: watermark,
            },
            None,
        ))
    }

    /// Handle ForwardChecksum (0x0B, flag=0x02) — CRC32 verification for sealed extent.
    ///
    /// Due to leader Mutex races on the primary, this frame may arrive before all
    /// Forward frames have been processed on the secondary. The primary's checksum
    /// and committed_bytes are stored in the extent for deferred verification.
    /// `try_advance_committed()` is called to advance as far as possible, and
    /// `try_verify_checksum()` checks if all records have been hashed.
    /// If not yet ready, verification will complete on a subsequent `replicate()` call.
    fn handle_forward_checksum(&self, frame: Frame) {
        let (stream_id, extent_id, primary_crc32, primary_committed_bytes) =
            match &frame.variable_header {
                VariableHeader::ForwardChecksum {
                    stream_id,
                    extent_id,
                    checksum,
                    committed_bytes,
                } => (*stream_id, *extent_id, *checksum, *committed_bytes),
                _ => return,
            };

        let streams = self.streams.pin();
        let stream_ref = match streams.get(&stream_id) {
            Some(s) => s,
            None => {
                warn!(
                    "ForwardChecksum for unknown stream {}, extent {}",
                    stream_id, extent_id,
                );
                return;
            }
        };

        let found = stream_ref.with_extent(extent_id, |extent| {
            // Store primary's checksum for deferred comparison.
            extent.store_primary_checksum(primary_crc32);

            // Advance incremental CRC32 as far as possible.
            extent.try_advance_committed();

            // Check if verification can complete now.
            match extent.try_verify_checksum() {
                Some(true) => {
                    info!(
                        "CRC32 checksum verified: stream={}, extent={}, crc32={:#010x}, bytes={}",
                        stream_id, extent_id, primary_crc32, primary_committed_bytes,
                    );
                }
                Some(false) => {
                    warn!(
                        "CRC32 checksum mismatch: stream={}, extent={}, \
                         primary_crc32={:#010x}, primary_bytes={} \
                         (verification will be logged by try_verify_checksum)",
                        stream_id, extent_id, primary_crc32, primary_committed_bytes,
                    );
                }
                None => {
                    info!(
                        "ForwardChecksum stored (deferred): stream={}, extent={}, \
                         crc32={:#010x}, bytes={} — waiting for remaining records",
                        stream_id, extent_id, primary_crc32, primary_committed_bytes,
                    );
                }
            }
        });
        if found.is_none() {
            warn!(
                "ForwardChecksum for unknown extent {} on stream {}",
                extent_id, stream_id,
            );
        }
    }

    fn handle_read(&self, frame: Frame) -> Frame {
        let stream_id = frame.stream_id();
        let extent_id = frame.extent_id();
        let streams = self.streams.pin();
        let stream_ref = match streams.get(&stream_id) {
            Some(s) => s,
            None => {
                return Frame::error_from_request(
                    &frame,
                    ErrorCode::UnknownStream,
                    &format!("stream {} not found", stream_id),
                    ExtentId(0),
                );
            }
        };

        let count = frame.count();

        match stream_ref.read(extent_id, frame.offset(), count) {
            Ok(messages) => {
                let total_size: usize = messages.iter().map(|m| 4 + m.len()).sum();
                let mut payload = BytesMut::with_capacity(total_size);
                for msg in &messages {
                    payload.put_u32(msg.len() as u32);
                    payload.extend_from_slice(msg);
                }
                Frame::new(
                    VariableHeader::ReadResp {
                        request_id: frame.request_id(),
                        stream_id,
                        offset: frame.offset(),
                        count: messages.len() as u32,
                    },
                    Some(payload.freeze()),
                )
            }
            Err(e) => Frame::error_from_request(
                &frame,
                ErrorCode::InternalError,
                &e.to_string(),
                ExtentId(0),
            ),
        }
    }

    fn handle_query_offset(&self, frame: Frame) -> Frame {
        let stream_id = frame.stream_id();
        let streams = self.streams.pin();
        let stream_ref = match streams.get(&stream_id) {
            Some(s) => s,
            None => {
                return Frame::error_from_request(
                    &frame,
                    ErrorCode::UnknownStream,
                    &format!("stream {} not found", stream_id),
                    ExtentId(0),
                );
            }
        };

        Frame::new(
            VariableHeader::QueryOffsetResp {
                request_id: frame.request_id(),
                stream_id,
                offset: stream_ref.max_offset(),
            },
            None,
        )
    }

    /// Handle REPORT_EXTENTS: SM queries this EN for all extents it holds for a stream.
    /// Used during crash recovery so SM can discover extents it doesn't know about.
    fn handle_report_extents(&self, frame: Frame) -> Frame {
        let (stream_id, epoch) = match &frame.variable_header {
            VariableHeader::ReportExtents {
                stream_id, epoch, ..
            } => (*stream_id, *epoch),
            _ => {
                return Frame::error_from_request(
                    &frame,
                    ErrorCode::InternalError,
                    "invalid ReportExtents frame",
                    ExtentId(0),
                );
            }
        };

        let streams = self.streams.pin();
        let stream_ref = match streams.get(&stream_id) {
            Some(s) => s,
            None => {
                // Stream not found — return empty response.
                return Frame::new(
                    VariableHeader::ReportExtentsResp {
                        request_id: frame.request_id(),
                        stream_id,
                        epoch,
                    },
                    Some(Bytes::from(0u32.to_be_bytes().to_vec())), // num_extents = 0
                );
            }
        };

        let report = stream_ref.report_extents(epoch);
        // Encode payload: [num_extents:u32] per extent: [extent_id:u32][start_offset:u64][end_offset:u64][state:u8]
        let mut buf = BytesMut::with_capacity(4 + report.len() * (4 + 8 + 8 + 1));
        buf.put_u32(report.len() as u32);
        for (eid, start, end, state) in &report {
            buf.put_u32(eid.0);
            buf.put_u64(start.0);
            buf.put_u64(*end);
            buf.put_u8(state.as_u8());
        }

        Frame::new(
            VariableHeader::ReportExtentsResp {
                request_id: frame.request_id(),
                stream_id,
                epoch: stream_ref.epoch(),
            },
            Some(buf.freeze()),
        )
    }

    fn handle_seal(&self, frame: Frame) -> Frame {
        // Parse SealExtentNodeRequest fields.
        let (request_id, stream_id, epoch, extent_id_from, req_start_offset) =
            match &frame.variable_header {
                VariableHeader::SealExtentNodeRequest {
                    request_id,
                    stream_id,
                    epoch,
                    extent_id_from,
                    start_offset,
                } => (
                    *request_id,
                    *stream_id,
                    *epoch,
                    *extent_id_from,
                    *start_offset,
                ),
                _ => {
                    return Frame::seal_extent_node_resp_error(
                        frame.request_id(),
                        frame.stream_id(),
                        ErrorCode::InternalError,
                        "invalid SealExtentNodeRequest frame",
                    );
                }
            };

        let streams = self.streams.pin();
        let stream_ref = match streams.get(&stream_id) {
            Some(s) => s,
            None => {
                // Stream not found. The SM is sealing a secondary that never received
                // any Forward frames — respond with start_offset to indicate zero
                // committed records for quorum.
                info!(
                    "seal for absent stream {} epoch {}: responding with start_offset={req_start_offset}",
                    stream_id, epoch
                );
                return Frame::new(
                    VariableHeader::SealExtentNodeResp {
                        request_id,
                        stream_id,
                        epoch,
                        extent_id: extent_id_from,
                        start_offset: req_start_offset,
                        end_offset: req_start_offset,
                    },
                    None,
                );
            }
        };

        // Wait for any active stream-level writer to finish before sealing.
        // With papaya, Stream is internally mutable via RwLock, but an
        // existing writer may have already started (in_flight > 0).
        {
            let mut spin_count = 0u32;
            loop {
                let inflight = stream_ref.in_flight().load(Ordering::Acquire);
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
        }

        // Find the LAST MUTABLE extent for (stream_id, epoch).
        // Only seal extents at the requested epoch — newer epochs are untouched.
        let active_id = match stream_ref.active_extent_at_epoch(epoch) {
            Some(id) => id,
            None => {
                // No active extent — all extents already sealed.
                // Return idempotent response with the actual sealed extent's offsets.
                let (extent_id, start_offset, end_offset) = stream_ref
                    .last_sealed_extent_at_epoch(epoch)
                    .unwrap_or((extent_id_from, req_start_offset, req_start_offset));
                let _ = stream_ref;
                let payload =
                    self.build_seal_predecessor_payload(stream_id, extent_id_from, extent_id);
                return Frame::new(
                    VariableHeader::SealExtentNodeResp {
                        request_id,
                        stream_id,
                        epoch,
                        extent_id,
                        start_offset,
                        end_offset,
                    },
                    payload,
                );
            }
        };

        match stream_ref.seal(active_id, None) {
            Some((start_offset, end_offset)) => {
                let sealed_extent_id = active_id;
                let _ = stream_ref;
                info!(
                    "sealed extent {} for stream {}, start_offset={start_offset}, end_offset={end_offset}",
                    sealed_extent_id, stream_id
                );
                // Primary seals finalize CRC32 — send checksum to secondaries inline.
                self.send_forward_checksum(stream_id, sealed_extent_id);

                // Build payload with predecessor extents (extent_id >= extent_id_from AND < sealed).
                let payload = self.build_seal_predecessor_payload(
                    stream_id,
                    extent_id_from,
                    sealed_extent_id,
                );

                Frame::new(
                    VariableHeader::SealExtentNodeResp {
                        request_id,
                        stream_id,
                        epoch,
                        extent_id: sealed_extent_id,
                        start_offset,
                        end_offset,
                    },
                    payload,
                )
            }
            None => {
                // Already sealed — return the sealed extent's end_offset idempotently.
                let end_offset = stream_ref.sealed_end_offset(active_id);
                let start_offset = stream_ref
                    .with_extent(active_id, |e| e.start_offset.0)
                    .unwrap_or(req_start_offset);
                let _ = stream_ref;
                info!(
                    "extent {} for stream {} already sealed, returning end_offset={end_offset} idempotently",
                    active_id, stream_id
                );

                let payload =
                    self.build_seal_predecessor_payload(stream_id, extent_id_from, active_id);

                Frame::new(
                    VariableHeader::SealExtentNodeResp {
                        request_id,
                        stream_id,
                        epoch,
                        extent_id: active_id,
                        start_offset,
                        end_offset,
                    },
                    payload,
                )
            }
        }
    }

    /// Build a payload containing predecessor extents for a seal response.
    ///
    /// Returns extent info for extents with `extent_id >= extent_id_from AND < sealed_extent_id`.
    /// Payload format: [num_extents:u32] then for each: [extent_id:u32][start_offset:u64][end_offset:u64]
    fn build_seal_predecessor_payload(
        &self,
        stream_id: StreamId,
        extent_id_from: ExtentId,
        sealed_extent_id: ExtentId,
    ) -> Option<Bytes> {
        let streams = self.streams.pin();
        let stream_ref = match streams.get(&stream_id) {
            Some(s) => s,
            None => return None,
        };

        let mut predecessors: Vec<(ExtentId, u64, u64)> = Vec::new();
        // Iterate over all known extents to find predecessors.
        let mut eid = extent_id_from;
        while eid.0 < sealed_extent_id.0 {
            if let Some((start, end)) = stream_ref.with_extent(eid, |ext| {
                let start = ext.start_offset.0;
                let end = start + ext.message_count();
                (start, end)
            }) {
                predecessors.push((eid, start, end));
            }
            eid = ExtentId(eid.0 + 1);
        }

        if predecessors.is_empty() {
            return None;
        }

        let mut buf = BytesMut::with_capacity(4 + predecessors.len() * (4 + 8 + 8));
        buf.put_u32(predecessors.len() as u32);
        for (eid, start, end) in &predecessors {
            buf.put_u32(eid.0);
            buf.put_u64(*start);
            buf.put_u64(*end);
        }
        Some(buf.freeze())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::config::{DEFAULT_CACHE_EXTENTS, DEFAULT_EXTENT_CAPACITY};
    use tokio::sync::mpsc;

    /// Register a stream on the ExtentNode via RegisterExtent (RF=1, Primary, no secondaries).
    /// This is the production path: StreamManager assigns a stream_id and sends RegisterExtent.
    async fn register_stream(store: &ExtentNodeStore, stream_id: u64, req_id: u32) -> StreamId {
        use rpc::payload::build_register_extent_payload;

        let sid = StreamId(stream_id);
        let payload = build_register_extent_payload(&[]);
        let resp = store
            .handle_frame(
                Frame::new(
                    VariableHeader::RegisterExtent {
                        request_id: req_id,
                        stream_id: sid,
                        extent_id: ExtentId(1),
                        role: 0,
                        replication_factor: 1,
                        epoch: Epoch(0),
                        extent_capacity: DEFAULT_EXTENT_CAPACITY,
                        min_extent_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
                        max_extent_capacity: DEFAULT_MAX_EXTENT_CAPACITY,
                        extent_growth_factor: DEFAULT_EXTENT_GROWTH_FACTOR,
                        cache_extents: DEFAULT_CACHE_EXTENTS,
                    },
                    Some(payload),
                ),
                None,
            )
            .await
            .unwrap();
        assert_eq!(resp.opcode(), Opcode::RegisterExtent);
        sid
    }

    #[tokio::test]
    async fn create_and_append() {
        let store = ExtentNodeStore::new();
        let sid = register_stream(&store, 1, 1).await;

        let resp = store
            .handle_frame(
                Frame::new(
                    VariableHeader::Append {
                        request_id: 2,
                        stream_id: sid,
                        epoch: Epoch(0),
                    },
                    Some(Bytes::from_static(b"hello")),
                ),
                None,
            )
            .await
            .unwrap();

        assert_eq!(resp.opcode(), Opcode::Append);
        assert_eq!(resp.offset(), Offset(0));
    }

    #[tokio::test]
    async fn append_to_unknown_stream() {
        let store = ExtentNodeStore::new();
        let resp = store
            .handle_frame(
                Frame::new(
                    VariableHeader::Append {
                        request_id: 1,
                        stream_id: StreamId(999),
                        epoch: Epoch(0),
                    },
                    Some(Bytes::from_static(b"fail")),
                ),
                None,
            )
            .await
            .unwrap();
        assert_eq!(resp.opcode(), Opcode::Append);
        assert!(resp.is_error_response());
    }

    #[tokio::test]
    async fn append_to_sealed_stream_reports_extent_id() {
        let store = ExtentNodeStore::new();
        let sid = register_stream(&store, 1, 1).await;

        {
            let streams = store.streams.pin();
            let stream = streams.get(&sid).unwrap();
            assert_eq!(stream.seal(ExtentId(1), None), Some((0, 0)));
        }

        let resp = store
            .handle_frame(
                Frame::new(
                    VariableHeader::Append {
                        request_id: 2,
                        stream_id: sid,
                        epoch: Epoch(0),
                    },
                    Some(Bytes::from_static(b"sealed")),
                ),
                None,
            )
            .await
            .unwrap();

        assert_eq!(resp.opcode(), Opcode::Append);
        assert!(resp.is_error_response());
        assert_eq!(resp.error_code(), ErrorCode::ExtentSealed as u16);
        assert_eq!(resp.extent_id(), ExtentId(1));
    }

    #[tokio::test]
    async fn append_read_query_offset() {
        let store = ExtentNodeStore::new();
        let sid = register_stream(&store, 1, 1).await;

        for i in 0u32..3 {
            let resp = store
                .handle_frame(
                    Frame::new(
                        VariableHeader::Append {
                            request_id: 10 + i,
                            stream_id: sid,
                            epoch: Epoch(0),
                        },
                        Some(Bytes::from(format!("msg{i}"))),
                    ),
                    None,
                )
                .await
                .unwrap();
            assert_eq!(resp.opcode(), Opcode::Append);
            assert_eq!(resp.offset(), Offset(i as u64));
        }

        let resp = store
            .handle_frame(
                Frame::new(
                    VariableHeader::QueryOffset {
                        request_id: 20,
                        stream_id: sid,
                    },
                    None,
                ),
                None,
            )
            .await
            .unwrap();
        assert_eq!(resp.opcode(), Opcode::QueryOffset);
        assert_eq!(resp.offset(), Offset(3));

        // Read all 3 from offset 0.
        let resp = store
            .handle_frame(
                Frame::new(
                    VariableHeader::Read {
                        request_id: 30,
                        stream_id: sid,
                        extent_id: ExtentId(1),
                        offset: Offset(0),
                        count: 3,
                    },
                    None,
                ),
                None,
            )
            .await
            .unwrap();
        assert_eq!(resp.opcode(), Opcode::Read);
        assert_eq!(resp.count(), 3);

        let resp_payload = resp.payload.as_ref().unwrap();
        let mut payload = &resp_payload[..];
        for i in 0..3 {
            let len = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
            payload = &payload[4..];
            let msg = &payload[..len];
            assert_eq!(msg, format!("msg{i}").as_bytes());
            payload = &payload[len..];
        }
        assert!(payload.is_empty());

        // Read msg1 directly via its offset.
        let resp = store
            .handle_frame(
                Frame::new(
                    VariableHeader::Read {
                        request_id: 31,
                        stream_id: sid,
                        extent_id: ExtentId(1),
                        offset: Offset(1),
                        count: 1,
                    },
                    None,
                ),
                None,
            )
            .await
            .unwrap();
        assert_eq!(resp.opcode(), Opcode::Read);
        assert_eq!(resp.count(), 1);
        let resp_payload = resp.payload.as_ref().unwrap();
        let len = u32::from_be_bytes([
            resp_payload[0],
            resp_payload[1],
            resp_payload[2],
            resp_payload[3],
        ]) as usize;
        assert_eq!(&resp_payload[4..4 + len], b"msg1");
    }

    #[tokio::test]
    async fn register_extent_creates_stream() {
        use rpc::payload::build_register_extent_payload;

        let store = ExtentNodeStore::new();

        // RegisterExtent as Primary with 1 secondary (RF=2).
        let payload = build_register_extent_payload(&["127.0.0.1:9802"]);
        let resp = store
            .handle_frame(
                Frame::new(
                    VariableHeader::RegisterExtent {
                        request_id: 1,
                        stream_id: StreamId(42),
                        extent_id: ExtentId(100),
                        role: 0,
                        replication_factor: 2,
                        epoch: Epoch(0),
                        extent_capacity: DEFAULT_EXTENT_CAPACITY as u32,
                        min_extent_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
                        max_extent_capacity: DEFAULT_MAX_EXTENT_CAPACITY,
                        extent_growth_factor: DEFAULT_EXTENT_GROWTH_FACTOR,
                        cache_extents: DEFAULT_CACHE_EXTENTS,
                    },
                    Some(payload),
                ),
                None,
            )
            .await
            .unwrap();

        assert_eq!(resp.opcode(), Opcode::RegisterExtent);
        assert_eq!(resp.stream_id(), StreamId(42));

        assert!(store.streams.pin().contains_key(&StreamId(42)));

        let ri = store.get_replica_info(StreamId(42)).unwrap();
        assert!(ri.is_primary());
        assert!(!ri.is_standalone());
        assert_eq!(ri.replica_addrs, vec!["127.0.0.1:9802"]);
        assert_eq!(ri.extent_id, ExtentId(100));
        assert_eq!(ri.replication_factor, 2);

        // AckQueue should be initialized for Primary.
        {
            let aq_guard = store.ack_queues.pin();
            let aq = aq_guard.get(&StreamId(42)).unwrap().lock().unwrap();
            assert_eq!(aq.required_secondary_acks, 1);
        }
    }

    #[tokio::test]
    async fn register_extent_secondary() {
        use rpc::payload::build_register_extent_payload;

        let store = ExtentNodeStore::new();

        // RegisterExtent as Secondary (RF=2, no replica addrs).
        let payload = build_register_extent_payload(&[]);
        let resp = store
            .handle_frame(
                Frame::new(
                    VariableHeader::RegisterExtent {
                        request_id: 1,
                        stream_id: StreamId(42),
                        extent_id: ExtentId(100),
                        role: 1,
                        replication_factor: 2,
                        epoch: Epoch(0),
                        extent_capacity: DEFAULT_EXTENT_CAPACITY as u32,
                        min_extent_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
                        max_extent_capacity: DEFAULT_MAX_EXTENT_CAPACITY,
                        extent_growth_factor: DEFAULT_EXTENT_GROWTH_FACTOR,
                        cache_extents: DEFAULT_CACHE_EXTENTS,
                    },
                    Some(payload),
                ),
                None,
            )
            .await
            .unwrap();

        assert_eq!(resp.opcode(), Opcode::RegisterExtent);

        let ri = store.get_replica_info(StreamId(42)).unwrap();
        assert!(!ri.is_primary());
        assert_eq!(ri.role, 1);
        assert!(ri.replica_addrs.is_empty());
        assert_eq!(ri.replication_factor, 2);

        // Secondary should NOT have an AckQueue.
        assert!(!store.ack_queues.pin().contains_key(&StreamId(42)));
    }

    #[tokio::test]
    async fn register_extent_then_append_rf1() {
        use rpc::payload::build_register_extent_payload;

        let store = ExtentNodeStore::new();

        // Register as Primary, RF=1 (standalone).
        let payload = build_register_extent_payload(&[]);
        store
            .handle_frame(
                Frame::new(
                    VariableHeader::RegisterExtent {
                        request_id: 1,
                        stream_id: StreamId(10),
                        extent_id: ExtentId(50),
                        role: 0,
                        replication_factor: 1,
                        epoch: Epoch(0),
                        extent_capacity: DEFAULT_EXTENT_CAPACITY as u32,
                        min_extent_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
                        max_extent_capacity: DEFAULT_MAX_EXTENT_CAPACITY,
                        extent_growth_factor: DEFAULT_EXTENT_GROWTH_FACTOR,
                        cache_extents: DEFAULT_CACHE_EXTENTS,
                    },
                    Some(payload),
                ),
                None,
            )
            .await
            .unwrap();

        // Append — standalone should ACK immediately.
        let resp = store
            .handle_frame(
                Frame::new(
                    VariableHeader::Append {
                        request_id: 2,
                        stream_id: StreamId(10),
                        epoch: Epoch(0),
                    },
                    Some(Bytes::from_static(b"hello standalone")),
                ),
                None,
            )
            .await
            .unwrap();

        assert_eq!(resp.opcode(), Opcode::Append);
        assert_eq!(resp.offset(), Offset(0));
    }

    #[tokio::test]
    async fn primary_append_defers_and_broadcasts() {
        use futures_util::StreamExt;
        use rpc::codec::FrameCodec;
        use rpc::payload::build_register_extent_payload;
        use tokio_util::codec::FramedRead;

        let (resp_tx, mut resp_rx) = mpsc::channel::<Frame>(100);

        // Start two mock TCP listeners (acting as secondaries).
        let listener1 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr1 = listener1.local_addr().unwrap().to_string();
        let listener2 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr2 = listener2.local_addr().unwrap().to_string();

        let store = Arc::new(ExtentNodeStore::new());
        let pool = Arc::new(crate::downstream::DownstreamPool::new(Arc::clone(&store)));
        store.set_downstream(Arc::clone(&pool));

        // Register as Primary with 2 secondaries (RF=3).
        let payload = build_register_extent_payload(&[&addr1, &addr2]);
        store
            .handle_frame(
                Frame::new(
                    VariableHeader::RegisterExtent {
                        request_id: 1,
                        stream_id: StreamId(10),
                        extent_id: ExtentId(50),
                        role: 0,
                        replication_factor: 3,
                        epoch: Epoch(0),
                        extent_capacity: DEFAULT_EXTENT_CAPACITY as u32,
                        min_extent_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
                        max_extent_capacity: DEFAULT_MAX_EXTENT_CAPACITY,
                        extent_growth_factor: DEFAULT_EXTENT_GROWTH_FACTOR,
                        cache_extents: DEFAULT_CACHE_EXTENTS,
                    },
                    Some(payload),
                ),
                None,
            )
            .await
            .unwrap();

        // Append — should return None (deferred), send 2 Forward frames over TCP.
        let result = store
            .handle_frame(
                Frame::new(
                    VariableHeader::Append {
                        request_id: 2,
                        stream_id: StreamId(10),
                        epoch: Epoch(0),
                    },
                    Some(Bytes::from_static(b"broadcast msg")),
                ),
                Some(&resp_tx),
            )
            .await;

        assert!(
            result.is_none(),
            "Primary with secondaries should defer ACK"
        );

        // Accept connections and read Forward frames from mock secondaries.
        let (conn1, _) = listener1.accept().await.unwrap();
        let mut reader1 = FramedRead::new(conn1, FrameCodec);
        let fwd1 = reader1.next().await.unwrap().unwrap();
        assert_eq!(fwd1.opcode(), Opcode::Forward);
        assert_eq!(fwd1.stream_id(), StreamId(10));
        assert_eq!(fwd1.offset(), Offset(0));

        let (conn2, _) = listener2.accept().await.unwrap();
        let mut reader2 = FramedRead::new(conn2, FrameCodec);
        let fwd2 = reader2.next().await.unwrap().unwrap();
        assert_eq!(fwd2.opcode(), Opcode::Forward);
        assert_eq!(fwd2.stream_id(), StreamId(10));
        assert_eq!(fwd2.offset(), Offset(0));

        let ack_queues = store.ack_queues.pin();

        // PendingAck should be in the ack_queue.
        {
            let ack_queue = ack_queues.get(&StreamId(10)).unwrap().lock().unwrap();
            assert_eq!(ack_queue.pending.len(), 1);
            assert_eq!(ack_queue.pending[0].assigned_offset, 0);
            // RF=3 requires 1 secondary ACK.
            assert_eq!(ack_queue.required_secondary_acks, 1);
        }

        // Simulate watermark from first secondary (quorum met with 1 ACK for RF=3).
        {
            let mut ack_queue = ack_queues.get(&StreamId(10)).unwrap().lock().unwrap();
            ack_queue.ack_from_secondary(&addr1, 0);
            ack_queue.drain_quorum();
        }

        // The client response channel should now have the AppendAck.
        let ack = resp_rx.try_recv().unwrap();
        assert_eq!(ack.opcode(), Opcode::Append);
        assert_eq!(ack.offset(), Offset(0));
        assert_eq!(ack.request_id(), 2);
    }

    #[tokio::test]
    async fn secondary_returns_watermark() {
        use rpc::payload::build_register_extent_payload;

        let store = ExtentNodeStore::new();

        // Register as Secondary (RF=2).
        let payload = build_register_extent_payload(&[]);
        store
            .handle_frame(
                Frame::new(
                    VariableHeader::RegisterExtent {
                        request_id: 1,
                        stream_id: StreamId(10),
                        extent_id: ExtentId(50),
                        role: 1,
                        replication_factor: 2,
                        epoch: Epoch(0),
                        extent_capacity: DEFAULT_EXTENT_CAPACITY as u32,
                        min_extent_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
                        max_extent_capacity: DEFAULT_MAX_EXTENT_CAPACITY,
                        extent_growth_factor: DEFAULT_EXTENT_GROWTH_FACTOR,
                        cache_extents: DEFAULT_CACHE_EXTENTS,
                    },
                    Some(payload),
                ),
                None,
            )
            .await
            .unwrap();

        // Forward frame (dedicated opcode for replication).
        let resp = store
            .handle_frame(
                Frame::new(
                    VariableHeader::Forward {
                        stream_id: StreamId(10),
                        extent_id: ExtentId(50),
                        epoch: Epoch(0),
                        offset: Offset(0),
                        byte_pos: 0,
                    },
                    Some(Bytes::from_static(b"forwarded msg")),
                ),
                None,
            )
            .await
            .unwrap();

        assert_eq!(resp.opcode(), Opcode::Watermark);
        assert_eq!(resp.stream_id(), StreamId(10));
        assert_eq!(resp.offset(), Offset(0));
    }

    #[tokio::test]
    async fn cumulative_ack_drains_multiple_pending() {
        // Test that a single watermark can drain multiple pending ACKs.
        let (resp_tx, mut resp_rx) = mpsc::channel::<Frame>(100);

        let mut ack_queue = AckQueue::new(1); // need 1 secondary ACK

        // Queue 3 pending ACKs at offsets 0, 1, 2.
        for i in 0u64..3 {
            ack_queue.pending.push_back(PendingAck {
                request_id: i as u32,
                stream_id: StreamId(10),
                extent_id: ExtentId(0),
                epoch: Epoch(0),
                response_tx: resp_tx.clone(),
                assigned_offset: i,
                created_at: Instant::now(),
            });
        }

        // Single cumulative ACK at offset 2 from one secondary.
        ack_queue.ack_from_secondary("sec-1", 2);
        ack_queue.drain_quorum();

        // All 3 should be drained.
        let ack0 = resp_rx.try_recv().unwrap();
        let ack1 = resp_rx.try_recv().unwrap();
        let ack2 = resp_rx.try_recv().unwrap();
        assert_eq!(ack0.offset(), Offset(0));
        assert_eq!(ack1.offset(), Offset(1));
        assert_eq!(ack2.offset(), Offset(2));
        assert!(resp_rx.try_recv().is_err()); // no more
    }

    #[tokio::test]
    async fn quorum_offset_with_multiple_secondaries() {
        let mut aq = AckQueue::new(2); // RF=4: need 2 secondary ACKs

        // Only 1 secondary has reported — not enough for quorum.
        aq.ack_from_secondary("sec-1", 5);
        assert!(aq.quorum_offset().is_none());

        // Second secondary reports — now we have quorum.
        aq.ack_from_secondary("sec-2", 3);
        // quorum_offset = min of top-2 = 3
        assert_eq!(aq.quorum_offset(), Some(3));

        // Third secondary reports higher.
        aq.ack_from_secondary("sec-3", 10);
        // top-2 descending: [10, 5], so quorum_offset = 5
        assert_eq!(aq.quorum_offset(), Some(5));
    }

    #[tokio::test]
    async fn pending_ack_timeout() {
        // Verify that PendingAcks expire after the configured replication timeout.
        let (resp_tx, mut resp_rx) = mpsc::channel::<Frame>(100);

        let mut ack_queue = AckQueue::new(1); // need 1 secondary ACK

        // Queue a PendingAck with a creation time far in the past (simulates timeout).
        ack_queue.pending.push_back(PendingAck {
            request_id: 42,
            stream_id: StreamId(10),
            extent_id: ExtentId(0),
            epoch: Epoch(0),
            response_tx: resp_tx.clone(),
            assigned_offset: 0,
            created_at: Instant::now() - DEFAULT_REPLICATION_TIMEOUT - Duration::from_secs(1),
        });

        // Queue a second PendingAck that is NOT expired.
        ack_queue.pending.push_back(PendingAck {
            request_id: 43,
            stream_id: StreamId(10),
            extent_id: ExtentId(0),
            epoch: Epoch(0),
            response_tx: resp_tx.clone(),
            assigned_offset: 1,
            created_at: Instant::now(),
        });

        // No quorum (no secondary has acked), but timeout sweep should fire.
        ack_queue.drain_quorum();

        // First PendingAck should have been expired with an error.
        let err_frame = resp_rx.try_recv().unwrap();
        assert_eq!(err_frame.opcode(), Opcode::Append);
        assert!(err_frame.is_error_response());
        assert_eq!(err_frame.request_id(), 42);

        // Second PendingAck should still be pending (not expired).
        assert!(resp_rx.try_recv().is_err());
        assert_eq!(ack_queue.pending.len(), 1);
        assert_eq!(ack_queue.pending[0].request_id, 43);
    }

    // ── Concurrent multi-stream benchmark ────────────────────────────────────

    /// Benchmark: N tokio tasks appending concurrently to N independent streams.
    ///
    /// Verifies that the per-stream concurrent map design allows true parallelism:
    /// - Each stream's offsets are contiguous [0..APPENDS_PER_STREAM)
    /// - All data is readable and correct after concurrent writes
    /// - No cross-stream interference
    ///
    /// With a global Mutex, all N tasks would serialize; with papaya::HashMap,
    /// they run in parallel with lock-free reads.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_multi_stream_appends() {
        use std::sync::Arc;

        const NUM_STREAMS: u64 = 8;
        const APPENDS_PER_STREAM: u64 = 5_000;
        const PAYLOAD_SIZE: usize = 128; // bytes per message

        let store = Arc::new(ExtentNodeStore::new());

        // Pre-create all streams so IDs are deterministic.
        let mut stream_ids = Vec::new();
        for i in 0..NUM_STREAMS {
            let sid = register_stream(&store, i + 1, i as u32).await;
            stream_ids.push(sid);
        }

        let start = Instant::now();

        // Spawn N tasks, each appending to its own stream.
        let mut handles = Vec::new();
        for (task_idx, &sid) in stream_ids.iter().enumerate() {
            let store = Arc::clone(&store);
            handles.push(tokio::spawn(async move {
                let payload_data = vec![b'A' + (task_idx as u8 % 26); PAYLOAD_SIZE];
                let mut offsets = Vec::with_capacity(APPENDS_PER_STREAM as usize);

                for seq in 0..APPENDS_PER_STREAM {
                    let resp = store
                        .handle_frame(
                            Frame::new(
                                VariableHeader::Append {
                                    request_id: seq as u32,
                                    stream_id: sid,
                                    epoch: Epoch(0),
                                },
                                Some(Bytes::from(payload_data.clone())),
                            ),
                            None,
                        )
                        .await
                        .unwrap();

                    assert_eq!(
                        resp.opcode(),
                        Opcode::Append,
                        "task {task_idx} seq {seq}: expected AppendAck"
                    );
                    offsets.push(resp.offset().0);
                }
                offsets
            }));
        }

        // Collect results from all tasks.
        let mut all_offsets: Vec<Vec<u64>> = Vec::new();
        for handle in handles {
            all_offsets.push(handle.await.unwrap());
        }

        let elapsed = start.elapsed();

        // ── Correctness checks ──

        // 1. Each stream's offsets should be a contiguous range [0..APPENDS_PER_STREAM).
        for (task_idx, offsets) in all_offsets.iter().enumerate() {
            assert_eq!(
                offsets.len(),
                APPENDS_PER_STREAM as usize,
                "task {task_idx}: wrong number of offsets"
            );

            let mut sorted = offsets.clone();
            sorted.sort_unstable();
            sorted.dedup();
            assert_eq!(
                sorted.len(),
                APPENDS_PER_STREAM as usize,
                "task {task_idx}: duplicate offsets detected"
            );
            assert_eq!(
                *sorted.first().unwrap(),
                0,
                "task {task_idx}: first offset should be 0"
            );
            assert_eq!(
                *sorted.last().unwrap(),
                APPENDS_PER_STREAM - 1,
                "task {task_idx}: last offset should be {}",
                APPENDS_PER_STREAM - 1
            );
        }

        // 2. Each stream should have correct max_offset.
        for (task_idx, &sid) in stream_ids.iter().enumerate() {
            let resp = store
                .handle_frame(
                    Frame::new(
                        VariableHeader::QueryOffset {
                            request_id: 0,
                            stream_id: sid,
                        },
                        None,
                    ),
                    None,
                )
                .await
                .unwrap();
            assert_eq!(
                resp.offset(),
                Offset(APPENDS_PER_STREAM),
                "task {task_idx}: stream max_offset mismatch"
            );
        }

        // 3. Read all records from each stream and verify payload content.
        for (task_idx, &sid) in stream_ids.iter().enumerate() {
            let expected_byte = b'A' + (task_idx as u8 % 26);
            let resp = store
                .handle_frame(
                    Frame::new(
                        VariableHeader::Read {
                            request_id: 0,
                            stream_id: sid,
                            extent_id: ExtentId(1),
                            offset: Offset(0),
                            count: 100,
                        },
                        None,
                    ),
                    None,
                )
                .await
                .unwrap();
            assert_eq!(resp.opcode(), Opcode::Read);
            let count = resp.count() as usize;
            assert!(count > 0, "task {task_idx}: expected at least 1 message");

            // Verify first record's payload.
            let resp_payload = resp.payload.as_ref().unwrap();
            let len = u32::from_be_bytes([
                resp_payload[0],
                resp_payload[1],
                resp_payload[2],
                resp_payload[3],
            ]) as usize;
            assert_eq!(len, PAYLOAD_SIZE, "task {task_idx}: payload size mismatch");
            assert_eq!(
                resp_payload[4], expected_byte,
                "task {task_idx}: payload content mismatch"
            );
        }

        // 4. Verify metrics counters.
        let total_expected = NUM_STREAMS * APPENDS_PER_STREAM;
        let (appends, bytes, active_count) = store.snapshot_metrics();
        assert_eq!(appends, total_expected, "metrics: append count mismatch");
        assert_eq!(
            bytes,
            total_expected * PAYLOAD_SIZE as u64,
            "metrics: bytes_written mismatch"
        );
        assert_eq!(
            active_count, NUM_STREAMS as u32,
            "metrics: active extent count mismatch"
        );

        // Print throughput info (visible with `cargo test -- --nocapture`).
        let total_ops = total_expected;
        let throughput = total_ops as f64 / elapsed.as_secs_f64();
        let mb_per_sec = (bytes as f64) / elapsed.as_secs_f64() / (1024.0 * 1024.0);
        eprintln!(
            "\n=== Concurrent Multi-Stream Benchmark ===\n\
             Streams: {NUM_STREAMS}, Appends/stream: {APPENDS_PER_STREAM}, \
             Payload: {PAYLOAD_SIZE}B\n\
             Total appends: {total_ops}\n\
             Elapsed: {:.2}ms\n\
             Throughput: {throughput:.0} ops/sec ({mb_per_sec:.1} MiB/sec)\n\
             ==========================================\n",
            elapsed.as_secs_f64() * 1000.0,
        );
    }

    /// Benchmark: concurrent readers and writers to different streams.
    ///
    /// Verifies that reads on stream A don't block writes on stream B.
    /// Writer tasks append to even-numbered streams while reader tasks
    /// read from odd-numbered streams (pre-populated).
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_readers_and_writers_different_streams() {
        use std::sync::Arc;

        const NUM_WRITER_STREAMS: u64 = 4;
        const NUM_READER_STREAMS: u64 = 4;
        const APPENDS_PER_STREAM: u64 = 2_000;
        const READS_PER_STREAM: u64 = 2_000;

        let store = Arc::new(ExtentNodeStore::new());

        // Create writer streams (will be written to concurrently).
        let mut writer_sids = Vec::new();
        for i in 0..NUM_WRITER_STREAMS {
            let sid = register_stream(&store, i + 1, i as u32).await;
            writer_sids.push(sid);
        }

        // Create reader streams and pre-populate them with data.
        let mut reader_sids = Vec::new();
        for i in 0..NUM_READER_STREAMS {
            let sid = register_stream(&store, 100 + i + 1, (100 + i) as u32).await;
            for j in 0..100u32 {
                store
                    .handle_frame(
                        Frame::new(
                            VariableHeader::Append {
                                request_id: j,
                                stream_id: sid,
                                epoch: Epoch(0),
                            },
                            Some(Bytes::from(format!("pre-{j}"))),
                        ),
                        None,
                    )
                    .await
                    .unwrap();
            }
            reader_sids.push(sid);
        }

        // Reset metrics after pre-population.
        store.snapshot_metrics();

        let mut handles = Vec::new();

        // Spawn writer tasks.
        for &sid in &writer_sids {
            let store = Arc::clone(&store);
            handles.push(tokio::spawn(async move {
                for seq in 0..APPENDS_PER_STREAM {
                    let resp = store
                        .handle_frame(
                            Frame::new(
                                VariableHeader::Append {
                                    request_id: seq as u32,
                                    stream_id: sid,
                                    epoch: Epoch(0),
                                },
                                Some(Bytes::from_static(b"write-payload")),
                            ),
                            None,
                        )
                        .await
                        .unwrap();
                    assert_eq!(resp.opcode(), Opcode::Append);
                }
                "writer_done"
            }));
        }

        // Spawn reader tasks.
        for &sid in &reader_sids {
            let store = Arc::clone(&store);
            handles.push(tokio::spawn(async move {
                for _ in 0..READS_PER_STREAM {
                    let resp = store
                        .handle_frame(
                            Frame::new(
                                VariableHeader::Read {
                                    request_id: 0,
                                    stream_id: sid,
                                    extent_id: ExtentId(1),
                                    offset: Offset(0),
                                    count: 10,
                                },
                                None,
                            ),
                            None,
                        )
                        .await
                        .unwrap();
                    assert_eq!(resp.opcode(), Opcode::Read);
                    assert!(resp.count() > 0, "reader should get at least 1 message");
                }
                "reader_done"
            }));
        }

        // Wait for all tasks.
        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result == "writer_done" || result == "reader_done");
        }

        // Verify writer streams have correct data.
        for &sid in &writer_sids {
            let resp = store
                .handle_frame(
                    Frame::new(
                        VariableHeader::QueryOffset {
                            request_id: 0,
                            stream_id: sid,
                        },
                        None,
                    ),
                    None,
                )
                .await
                .unwrap();
            assert_eq!(
                resp.offset(),
                Offset(APPENDS_PER_STREAM),
                "writer stream {:?} should have {APPENDS_PER_STREAM} messages",
                sid
            );
        }

        // Verify reader streams are untouched (still 100 messages each).
        for &sid in &reader_sids {
            let resp = store
                .handle_frame(
                    Frame::new(
                        VariableHeader::QueryOffset {
                            request_id: 0,
                            stream_id: sid,
                        },
                        None,
                    ),
                    None,
                )
                .await
                .unwrap();
            assert_eq!(
                resp.offset(),
                Offset(100),
                "reader stream {:?} should still have 100 messages",
                sid
            );
        }
    }

    /// Benchmark: multiple tasks appending to the SAME stream concurrently.
    ///
    /// Verifies that the pipelined group commit works correctly when multiple
    /// tokio tasks target the same stream. The leader election on the extent's
    /// in_flight counter serializes writes, and followers receive their ACKs
    /// via the response_tx channel.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_appends_same_stream() {
        use std::sync::Arc;

        const NUM_TASKS: u64 = 8;
        const APPENDS_PER_TASK: u64 = 2_000;

        let store = Arc::new(ExtentNodeStore::new());
        let sid = register_stream(&store, 1, 1).await;

        let start = Instant::now();

        let mut handles = Vec::new();
        for task_idx in 0..NUM_TASKS {
            let store = Arc::clone(&store);
            handles.push(tokio::spawn(async move {
                let (resp_tx, mut resp_rx) = mpsc::channel::<Frame>(16);
                let mut offsets = Vec::with_capacity(APPENDS_PER_TASK as usize);
                for seq in 0..APPENDS_PER_TASK {
                    let result = store
                        .handle_frame(
                            Frame::new(
                                VariableHeader::Append {
                                    request_id: seq as u32,
                                    stream_id: sid,
                                    epoch: Epoch(0),
                                },
                                Some(Bytes::from(format!("t{task_idx}-m{seq}"))),
                            ),
                            Some(&resp_tx),
                        )
                        .await;

                    // With group commit, the response comes either:
                    // - directly as Some(Frame) from handle_frame (leader path, no response_tx match)
                    // - via the response_tx channel (both leader and follower paths)
                    let resp = if let Some(frame) = result {
                        frame
                    } else {
                        // ACK was sent via response_tx channel.
                        resp_rx.recv().await.unwrap()
                    };

                    assert_eq!(resp.opcode(), Opcode::Append);
                    offsets.push(resp.offset().0);
                }
                offsets
            }));
        }

        let mut all_offsets: Vec<u64> = Vec::new();
        for handle in handles {
            all_offsets.extend(handle.await.unwrap());
        }

        let elapsed = start.elapsed();

        // All offsets across all tasks should form a contiguous range.
        let total = (NUM_TASKS * APPENDS_PER_TASK) as usize;
        assert_eq!(all_offsets.len(), total);

        all_offsets.sort_unstable();
        all_offsets.dedup();
        assert_eq!(
            all_offsets.len(),
            total,
            "duplicate offsets detected across tasks"
        );
        assert_eq!(*all_offsets.first().unwrap(), 0);
        assert_eq!(*all_offsets.last().unwrap(), (total - 1) as u64);

        // Verify max_offset.
        let resp = store
            .handle_frame(
                Frame::new(
                    VariableHeader::QueryOffset {
                        request_id: 0,
                        stream_id: sid,
                    },
                    None,
                ),
                None,
            )
            .await
            .unwrap();
        assert_eq!(resp.offset(), Offset(total as u64));

        let throughput = total as f64 / elapsed.as_secs_f64();
        eprintln!(
            "\n=== Concurrent Same-Stream Benchmark ===\n\
             Tasks: {NUM_TASKS}, Appends/task: {APPENDS_PER_TASK}\n\
             Total appends: {total}\n\
             Elapsed: {:.2}ms\n\
             Throughput: {throughput:.0} ops/sec\n\
             =========================================\n",
            elapsed.as_secs_f64() * 1000.0,
        );
    }

    /// Test Bug 1 fix: secondary accepts forwarded records after seal (within committed range).
    /// Simulates: primary commits 4 records, secondary only received 2 before SM seals it
    /// with committed_offset=4. Late Forward frames for offsets 2,3 arrive — secondary
    /// must accept them (not reject with ExtentSealed).
    #[tokio::test]
    async fn secondary_accepts_forwarded_append_after_seal() {
        use rpc::payload::build_register_extent_payload;

        let store = ExtentNodeStore::new();

        // Register as Secondary (RF=2).
        let payload = build_register_extent_payload(&[]);
        store
            .handle_frame(
                Frame::new(
                    VariableHeader::RegisterExtent {
                        request_id: 1,
                        stream_id: StreamId(10),
                        extent_id: ExtentId(50),
                        role: 1,
                        replication_factor: 2,
                        epoch: Epoch(0),
                        extent_capacity: DEFAULT_EXTENT_CAPACITY as u32,
                        min_extent_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
                        max_extent_capacity: DEFAULT_MAX_EXTENT_CAPACITY,
                        extent_growth_factor: DEFAULT_EXTENT_GROWTH_FACTOR,
                        cache_extents: DEFAULT_CACHE_EXTENTS,
                    },
                    Some(payload),
                ),
                None,
            )
            .await
            .unwrap();

        // Secondary receives 2 forwarded messages before seal.
        // Each record: 4 bytes len prefix + payload. "msg0" = 4 bytes, record = 8 bytes.
        for i in 0u32..2 {
            let byte_pos = i as u64 * 8; // "msgN" = 4 bytes payload, record = 8 bytes
            let resp = store
                .handle_frame(
                    Frame::new(
                        VariableHeader::Forward {
                            stream_id: StreamId(10),
                            extent_id: ExtentId(50),
                            epoch: Epoch(0),
                            offset: Offset(i as u64),
                            byte_pos,
                        },
                        Some(Bytes::from(format!("msg{i}"))),
                    ),
                    None,
                )
                .await
                .unwrap();
            assert_eq!(resp.opcode(), Opcode::Watermark);
        }

        // SM seals with committed_offset=4 (primary committed 4 records).
        // Secondary has only 2, so sealed_message_count = 4 - 0 = 4.
        let seal_resp = store
            .handle_frame(
                Frame::new(
                    VariableHeader::SealExtentNodeRequest {
                        request_id: 20,
                        stream_id: StreamId(10),
                        epoch: Epoch(0),
                        extent_id_from: ExtentId(50),
                        start_offset: 0,
                    },
                    None,
                ),
                None,
            )
            .await
            .unwrap();
        assert_eq!(seal_resp.opcode(), Opcode::SealExtentNode);
        // SealExtentNodeResp carries end_offset in the variable header.
        // Secondary had 2 records, so end_offset = 2.
        match &seal_resp.variable_header {
            VariableHeader::SealExtentNodeResp { end_offset, .. } => {
                assert_eq!(*end_offset, 2);
            }
            _ => panic!("expected SealExtentNodeResp"),
        }

        // Late Forward frames for offsets 2 and 3 arrive — but the extent
        // is sealed at end_offset=2 (secondary's local state with no committed_offset),
        // so frames beyond the seal point are rejected.
        for i in 2u32..4 {
            let byte_pos = i as u64 * 8;
            let resp = store
                .handle_frame(
                    Frame::new(
                        VariableHeader::Forward {
                            stream_id: StreamId(10),
                            extent_id: ExtentId(50),
                            epoch: Epoch(0),
                            offset: Offset(i as u64),
                            byte_pos,
                        },
                        Some(Bytes::from(format!("msg{i}"))),
                    ),
                    None,
                )
                .await;
            // With the new seal protocol, the secondary sealed at its local state (2 records).
            // Late forwards beyond the seal point are rejected (return None).
            assert!(
                resp.is_none(),
                "late forward for offset {i} beyond sealed limit should return None"
            );
        }

        // After all late forwards have landed and the extent is fully sealed,
        // further forwards should be rejected (no response).
        let resp = store
            .handle_frame(
                Frame::new(
                    VariableHeader::Forward {
                        stream_id: StreamId(10),
                        extent_id: ExtentId(50),
                        epoch: Epoch(0),
                        offset: Offset(4),
                        byte_pos: 32,
                    },
                    Some(Bytes::from_static(b"should-fail")),
                ),
                None,
            )
            .await;
        assert!(
            resp.is_none(),
            "forward beyond sealed limit should return None"
        );
    }

    /// Test Bug 2 fix: handle_seal is idempotent — sealing twice returns SealAck both times.
    /// Simulates: primary seals via extent-full path, then SM sends seal RPC (idempotent retry).
    #[tokio::test]
    async fn handle_seal_is_idempotent() {
        let store = ExtentNodeStore::new();
        let sid = register_stream(&store, 1, 1).await;

        // Append some messages.
        for i in 0u32..3 {
            let resp = store
                .handle_frame(
                    Frame::new(
                        VariableHeader::Append {
                            request_id: 10 + i,
                            stream_id: sid,
                            epoch: Epoch(0),
                        },
                        Some(Bytes::from(format!("msg{i}"))),
                    ),
                    None,
                )
                .await
                .unwrap();
            assert_eq!(resp.opcode(), Opcode::Append);
        }

        // First seal — no committed_offset (simulates SM seal via SealExtentNodeRequest).
        let seal1 = store
            .handle_frame(
                Frame::new(
                    VariableHeader::SealExtentNodeRequest {
                        request_id: 20,
                        stream_id: sid,
                        epoch: Epoch(0),
                        extent_id_from: ExtentId(1),
                        start_offset: 0,
                    },
                    None,
                ),
                None,
            )
            .await
            .unwrap();
        assert_eq!(seal1.opcode(), Opcode::SealExtentNode);
        // SealExtentNodeResp carries end_offset=3
        match &seal1.variable_header {
            VariableHeader::SealExtentNodeResp { end_offset, .. } => {
                assert_eq!(*end_offset, 3);
            }
            _ => panic!("expected SealExtentNodeResp"),
        }

        // Second seal — should also return SealExtentNodeResp with the same offset (idempotent).
        let seal2 = store
            .handle_frame(
                Frame::new(
                    VariableHeader::SealExtentNodeRequest {
                        request_id: 21,
                        stream_id: sid,
                        epoch: Epoch(0),
                        extent_id_from: ExtentId(1),
                        start_offset: 0,
                    },
                    None,
                ),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            seal2.opcode(),
            Opcode::SealExtentNode,
            "second seal should return SealExtentNodeResp, not Error"
        );
        match &seal2.variable_header {
            VariableHeader::SealExtentNodeResp { end_offset, .. } => {
                assert_eq!(
                    *end_offset, 3,
                    "second seal should report same committed offset"
                );
            }
            _ => panic!("expected SealExtentNodeResp"),
        }
    }

    #[tokio::test]
    async fn append_with_stale_epoch_returns_epoch_stale() {
        let store = ExtentNodeStore::new();
        let sid = register_stream(&store, 1, 1).await;

        // Bump the stream's epoch to 5 (simulates SM epoch bump after failover).
        {
            let guard = store.streams.pin();
            let stream = guard.get(&sid).unwrap();
            stream.set_epoch(Epoch(5));
        }

        // Append with stale epoch (client thinks epoch=1, but stream is at epoch=5).
        let resp = store
            .handle_frame(
                Frame::new(
                    VariableHeader::Append {
                        request_id: 10,
                        stream_id: sid,
                        epoch: Epoch(1),
                    },
                    Some(Bytes::from_static(b"stale")),
                ),
                None,
            )
            .await
            .unwrap();
        assert_eq!(resp.opcode(), Opcode::Append);
        assert!(resp.is_error_response());
        assert_eq!(resp.error_code(), ErrorCode::EpochStale as u16);
    }

    #[tokio::test]
    async fn append_with_epoch_zero_bypasses_epoch_check() {
        let store = ExtentNodeStore::new();
        let sid = register_stream(&store, 1, 1).await;

        // Bump the stream's epoch to 5.
        {
            let guard = store.streams.pin();
            let stream = guard.get(&sid).unwrap();
            stream.set_epoch(Epoch(5));
        }

        // Append with epoch=0 (wildcard) should succeed regardless of stream epoch.
        let resp = store
            .handle_frame(
                Frame::new(
                    VariableHeader::Append {
                        request_id: 10,
                        stream_id: sid,
                        epoch: Epoch(0),
                    },
                    Some(Bytes::from_static(b"wildcard")),
                ),
                None,
            )
            .await
            .unwrap();
        assert_eq!(resp.opcode(), Opcode::Append);
        assert!(!resp.is_error_response());
    }

    #[tokio::test]
    async fn append_with_matching_epoch_succeeds() {
        let store = ExtentNodeStore::new();
        let sid = register_stream(&store, 1, 1).await;

        // Bump the stream's epoch to 3.
        {
            let guard = store.streams.pin();
            let stream = guard.get(&sid).unwrap();
            stream.set_epoch(Epoch(3));
        }

        // Append with matching epoch=3 should succeed.
        let resp = store
            .handle_frame(
                Frame::new(
                    VariableHeader::Append {
                        request_id: 10,
                        stream_id: sid,
                        epoch: Epoch(3),
                    },
                    Some(Bytes::from_static(b"correct")),
                ),
                None,
            )
            .await
            .unwrap();
        assert_eq!(resp.opcode(), Opcode::Append);
        assert!(!resp.is_error_response());
    }
}
