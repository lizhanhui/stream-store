use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::{BufMut, Bytes, BytesMut};
use common::types::{ErrorCode, ExtentId, Offset, Opcode, StreamId, FLAG_FORWARDED};
use dashmap::DashMap;
use rpc::frame::Frame;
use rpc::payload::{parse_register_extent_payload, ROLE_PRIMARY};
use server::handler::RequestHandler;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::extent::DEFAULT_ARENA_CAPACITY;
use crate::stream::Stream;

// ── Broadcast replication types ──────────────────────────────────────────────

/// A pending client ACK waiting for quorum replication.
#[derive(Debug)]
pub struct PendingAck {
    /// The original request_id from the client's Append frame.
    pub request_id: u32,
    /// The stream the append was written to.
    pub stream_id: StreamId,
    /// Channel back to the client connection's write task.
    pub response_tx: mpsc::Sender<Frame>,
    /// The offset assigned to this append.
    pub assigned_offset: u64,
    /// Pre-built payload for the AppendAck (carries byte_pos for index building).
    pub ack_payload: Bytes,
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
}

impl AckQueue {
    pub fn new(required_secondary_acks: u32) -> Self {
        Self {
            pending: VecDeque::new(),
            secondary_acked: HashMap::new(),
            required_secondary_acks,
        }
    }

    /// Compute the quorum offset: the highest offset where at least
    /// `required_secondary_acks` secondaries have confirmed.
    ///
    /// Returns None if quorum cannot be met (not enough secondaries have reported).
    pub fn quorum_offset(&self) -> Option<u64> {
        if self.required_secondary_acks == 0 {
            return None; // RF=1, no quorum needed
        }
        let mut offsets: Vec<u64> = self.secondary_acked.values().copied().collect();
        if offsets.len() < self.required_secondary_acks as usize {
            return None; // Not enough secondaries have reported yet
        }
        offsets.sort_unstable_by(|a, b| b.cmp(a)); // descending
        offsets.get(self.required_secondary_acks as usize - 1).copied()
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
    pub fn drain_quorum(&mut self) {
        let qo = match self.quorum_offset() {
            Some(o) => o,
            None => return,
        };
        while let Some(front) = self.pending.front() {
            if front.assigned_offset <= qo {
                let ack = self.pending.pop_front().unwrap();
                let frame = Frame {
                    opcode: Opcode::AppendAck,
                    flags: 0,
                    request_id: ack.request_id,
                    stream_id: ack.stream_id,
                    extent_id: ExtentId(0),
                    offset: Offset(ack.assigned_offset),
                    payload: ack.ack_payload,
                };
                // Best-effort send — if the client disconnected, the channel is closed.
                let _ = ack.response_tx.try_send(frame);
            } else {
                break;
            }
        }
    }
}

/// Request to forward an append to a secondary node.
/// Sent from the store to the DownstreamManager via channel.
/// Primary emits one ForwardRequest per secondary (broadcast fan-out).
#[derive(Debug, Clone)]
pub struct ForwardRequest {
    pub stream_id: StreamId,
    pub offset: u64,
    pub payload: Bytes,
    pub downstream_addr: String,
}

/// Watermark event received from a secondary.
/// Sent from the DownstreamManager to the WatermarkHandler via channel.
/// Represents a cumulative ACK: all offsets <= acked_offset are confirmed.
#[derive(Debug, Clone)]
pub struct WatermarkEvent {
    pub stream_id: StreamId,
    /// The highest offset this secondary has written (cumulative).
    pub acked_offset: u64,
    /// The address of the secondary that sent this ACK.
    pub source_addr: String,
}

/// Request from the Primary to proactively seal a stream and trigger new extent allocation.
/// Emitted when the arena is full (ExtentFull). Sent to a background task that forwards
/// the Seal request to Stream Manager, avoiding an error storm where every client
/// independently races to trigger seal-and-new.
#[derive(Debug, Clone)]
pub struct SealRequest {
    pub stream_id: StreamId,
    pub extent_id: ExtentId,
    /// Committed offset = base_offset + message_count (the next writable offset).
    /// Stream Manager trusts this value from the primary EN.
    pub offset: u64,
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

// ── ExtentNodeStore ──────────────────────────────────────────────────────────

/// The ExtentNode's in-memory store: holds all streams and their extents.
///
/// Uses per-stream fine-grained locking via `DashMap` instead of a single global
/// Mutex. This ensures:
/// - **Different streams are fully concurrent**: requests to Stream A and Stream B
///   never block each other.
/// - **Reads within a stream don't block other streams**: each DashMap entry has
///   its own RwLock.
/// - **Writes to the same extent** use the lock-free arena (atomic CAS), so even
///   within a stream, multiple appenders only synchronize on slot reservation.
pub struct ExtentNodeStore {
    /// Per-stream data with fine-grained locking (DashMap uses per-shard RwLock).
    streams: DashMap<StreamId, Stream>,
    /// Monotonic stream ID generator (atomic, no lock needed).
    next_stream_id: AtomicU64,
    /// Arena capacity for new extents (bytes). Configurable per ExtentNode.
    /// Set once at startup, read-only thereafter.
    arena_capacity: usize,
    /// Replication info per stream_id (registered via RegisterExtent).
    /// Fine-grained per-stream locking.
    replicas: DashMap<StreamId, ReplicaInfo>,
    /// Channel to send ForwardRequests to the DownstreamManager (None for standalone/test mode).
    forward_tx: Option<mpsc::Sender<ForwardRequest>>,
    /// Channel to send proactive SealRequests when an extent is full (Primary only).
    /// A background task receives these and forwards Seal RPCs to Stream Manager.
    seal_tx: Option<mpsc::Sender<SealRequest>>,
    /// Per-stream ACK queues for the Primary (only used when this node is Primary for a stream).
    /// Fine-grained per-stream locking.
    pub ack_queues: DashMap<StreamId, AckQueue>,
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
            streams: DashMap::new(),
            next_stream_id: AtomicU64::new(1),
            arena_capacity: DEFAULT_ARENA_CAPACITY,
            replicas: DashMap::new(),
            forward_tx: None,
            seal_tx: None,
            ack_queues: DashMap::new(),
            append_count: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
        }
    }

    /// Create a new store with broadcast replication support.
    pub fn with_forward_tx(forward_tx: mpsc::Sender<ForwardRequest>) -> Self {
        Self {
            streams: DashMap::new(),
            next_stream_id: AtomicU64::new(1),
            arena_capacity: DEFAULT_ARENA_CAPACITY,
            replicas: DashMap::new(),
            forward_tx: Some(forward_tx),
            seal_tx: None,
            ack_queues: DashMap::new(),
            append_count: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
        }
    }

    /// Set the arena capacity for new extents (bytes).
    /// Called once during startup before any requests are processed.
    pub fn set_arena_capacity(&mut self, capacity: usize) {
        self.arena_capacity = capacity;
    }

    /// Set the seal request channel (called during ExtentNode bootstrap).
    pub fn set_seal_tx(&mut self, seal_tx: mpsc::Sender<SealRequest>) {
        self.seal_tx = Some(seal_tx);
    }

    /// Get the replication info for a stream, if registered via RegisterExtent.
    pub fn get_replica_info(&self, stream_id: StreamId) -> Option<ReplicaInfo> {
        self.replicas.get(&stream_id).map(|r| r.clone())
    }

    /// Snapshot current metrics and reset counters.
    /// Returns (appends_since_last, bytes_written_since_last, active_extent_count).
    pub fn snapshot_metrics(&self) -> (u64, u64, u32) {
        let appends = self.append_count.swap(0, Ordering::Relaxed);
        let bytes = self.bytes_written.swap(0, Ordering::Relaxed);

        // Count active extents: streams whose last extent is active (mutable).
        let active_count = self.streams.iter()
            .filter(|entry| entry.value().is_mutable())
            .count() as u32;

        (appends, bytes, active_count)
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
        response_tx: Option<&mpsc::Sender<Frame>>,
    ) -> Option<Frame> {
        match frame.opcode {
            Opcode::CreateStream => Some(self.handle_create_stream(frame)),
            Opcode::Append => self.handle_append(frame, response_tx),
            Opcode::Read => Some(self.handle_read(frame)),
            Opcode::QueryOffset => Some(self.handle_query_offset(frame)),
            Opcode::Seal => Some(self.handle_seal(frame)),
            Opcode::RegisterExtent => Some(self.handle_register_extent(frame)),
            Opcode::Connect => {
                Some(Frame {
                    opcode: Opcode::ConnectAck,
                    flags: 0,
                    request_id: frame.request_id,
                    stream_id: StreamId(0),
                    extent_id: ExtentId(0),
                    offset: Offset(0),
                    payload: Bytes::new(),
                })
            }
            Opcode::Heartbeat => Some(Frame {
                opcode: Opcode::Heartbeat,
                flags: 0,
                request_id: frame.request_id,
                stream_id: StreamId(0),
                extent_id: ExtentId(0),
                offset: Offset(0),
                payload: Bytes::new(),
            }),
            _ => Some(Frame::error_response(
                frame.request_id,
                ErrorCode::InternalError,
                "unsupported opcode",
                ExtentId(0),
            )),
        }
    }
}

impl ExtentNodeStore {
    fn handle_create_stream(&self, frame: Frame) -> Frame {
        let stream_id = StreamId(self.next_stream_id.fetch_add(1, Ordering::Relaxed));
        let stream = Stream::with_capacity(stream_id, self.arena_capacity);
        self.streams.insert(stream_id, stream);

        Frame {
            opcode: Opcode::AppendAck, // reuse as generic ack
            flags: 0,
            request_id: frame.request_id,
            stream_id,
            extent_id: ExtentId(0),
            offset: Offset(0),
            payload: Bytes::new(),
        }
    }

    /// Handle RegisterExtent from StreamManager: assign this ExtentNode a role in broadcast replication.
    ///
    /// Creates the stream locally (with the StreamManager-assigned stream_id) and stores replica info.
    fn handle_register_extent(&self, frame: Frame) -> Frame {
        let parsed = match parse_register_extent_payload(&frame.payload) {
            Some(p) => p,
            None => {
                return Frame::error_response(
                    frame.request_id,
                    ErrorCode::InternalError,
                    "invalid RegisterExtent payload",
                    ExtentId(0),
                );
            }
        };

        let stream_id = StreamId(parsed.stream_id);
        let extent_id = ExtentId(parsed.extent_id);

        // Create the stream locally with the StreamManager-assigned stream_id if it doesn't exist.
        if !self.streams.contains_key(&stream_id) {
            let stream = Stream::with_capacity(stream_id, self.arena_capacity);
            self.streams.insert(stream_id, stream);
        }

        // Update next_stream_id to avoid collision with StreamManager-assigned IDs.
        // Use fetch_max to atomically ensure we stay above the assigned ID.
        self.next_stream_id.fetch_max(parsed.stream_id + 1, Ordering::Relaxed);

        let role_name = if parsed.role == ROLE_PRIMARY {
            "Primary"
        } else {
            &format!("Secondary-{}", parsed.role)
        };
        let addrs_info = if parsed.replica_addrs.is_empty() {
            "none".to_string()
        } else {
            parsed.replica_addrs.join(", ")
        };
        info!(
            "RegisterExtent: stream={:?}, extent={:?}, role={role_name}, rf={}, secondaries=[{addrs_info}]",
            stream_id, extent_id, parsed.replication_factor,
        );

        let ri = ReplicaInfo {
            stream_id,
            extent_id,
            role: parsed.role,
            replication_factor: parsed.replication_factor,
            replica_addrs: parsed.replica_addrs,
        };

        // If this node is Primary, initialize an AckQueue.
        if ri.is_primary() {
            self.ack_queues
                .entry(stream_id)
                .or_insert_with(|| AckQueue::new(ri.required_secondary_acks()));
        }

        self.replicas.insert(stream_id, ri);

        Frame {
            opcode: Opcode::RegisterExtentAck,
            flags: 0,
            request_id: frame.request_id,
            stream_id,
            extent_id: ExtentId(0),
            offset: Offset(0),
            payload: Bytes::new(),
        }
    }

    /// Handle Append — behaviour depends on replication role:
    ///
    /// **No replication / standalone**: Write locally, return AppendAck immediately.
    ///
    /// **Primary (no FLAG_FORWARDED)**: Write locally, broadcast ForwardRequests
    /// to all secondaries in parallel, queue PendingAck, return None (deferred ACK).
    /// If standalone (RF=1), ACK immediately.
    ///
    /// **Secondary (FLAG_FORWARDED set)**: Write locally, return Watermark with
    /// the written offset. No forwarding to any other node.
    fn handle_append(
        &self,
        frame: Frame,
        response_tx: Option<&mpsc::Sender<Frame>>,
    ) -> Option<Frame> {
        let stream_id = frame.stream_id;
        let is_forwarded = frame.flags & FLAG_FORWARDED != 0;

        // Get the stream entry (per-stream lock, not global).
        let stream_ref = match self.streams.get(&stream_id) {
            Some(s) => s,
            None => {
                return Some(Frame::error_response(
                    frame.request_id,
                    ErrorCode::UnknownStream,
                    &format!("stream {:?} not found", stream_id),
                    ExtentId(0),
                ));
            }
        };

        // Write locally. The Extent's append is lock-free (atomic CAS).
        let append_result = match stream_ref.append(frame.payload.clone()) {
            Ok(r) => r,
            Err(common::errors::StorageError::ExtentSealed(_)) => {
                return Some(Frame::error_response(
                    frame.request_id,
                    ErrorCode::ExtentSealed,
                    "extent is sealed",
                    ExtentId(0),
                ));
            }
            Err(common::errors::StorageError::ExtentFull(_)) => {
                // Drop the read guard before acquiring write guard for seal.
                drop(stream_ref);

                // Proactively seal the extent so subsequent appends get
                // ExtentSealed (fast path) instead of racing on the full arena.
                let (extent_id, offset) = if let Some(mut stream_mut) = self.streams.get_mut(&stream_id) {
                    let eid = stream_mut.active_extent_id();
                    match stream_mut.seal_active() {
                        Some((base_offset, message_count)) => (eid, base_offset + message_count),
                        None => (eid, 0),
                    }
                } else {
                    (ExtentId(0), 0)
                };

                // Notify background task to send Seal to Stream Manager,
                // triggering new extent allocation before clients retry.
                if let Some(ref tx) = self.seal_tx {
                    let _ = tx.try_send(SealRequest { stream_id, extent_id, offset });
                }

                return Some(Frame::error_response(
                    frame.request_id,
                    ErrorCode::ExtentFull,
                    "extent arena is full, seal initiated",
                    extent_id,
                ));
            }
            Err(e) => {
                return Some(Frame::error_response(
                    frame.request_id,
                    ErrorCode::InternalError,
                    &e.to_string(),
                    ExtentId(0),
                ));
            }
        };

        // Drop per-stream read guard as soon as possible.
        drop(stream_ref);

        let offset = append_result.offset;
        let byte_pos = append_result.byte_pos;

        // Update metrics counters (atomic, no lock needed).
        self.append_count.fetch_add(1, Ordering::Relaxed);
        self.bytes_written.fetch_add(frame.payload.len() as u64, Ordering::Relaxed);

        // Build AppendAck payload with byte_pos so the caller can build an index.
        let ack_payload = {
            let mut buf = BytesMut::with_capacity(8);
            buf.put_u64(byte_pos);
            buf.freeze()
        };

        // Check replica info for this stream (per-stream lock, brief).
        let replica = self.replicas.get(&stream_id).map(|r| r.clone());

        match replica {
            None => {
                // Standalone mode: immediate ACK with byte_pos in payload.
                Some(Frame {
                    opcode: Opcode::AppendAck,
                    flags: 0,
                    request_id: frame.request_id,
                    stream_id,
                    extent_id: ExtentId(0),
                    offset,
                    payload: ack_payload,
                })
            }
            Some(ref ri) if is_forwarded => {
                // Secondary: forwarded append from Primary.
                // Write locally (already done), return Watermark with cumulative offset.
                // No forwarding to any other node.
                Some(Frame {
                    opcode: Opcode::Watermark,
                    flags: 0,
                    request_id: frame.request_id,
                    stream_id,
                    extent_id: ExtentId(0),
                    offset, // cumulative: highest written offset
                    payload: Bytes::new(),
                })
            }
            Some(ref ri) if ri.is_primary() => {
                // Primary.
                if ri.is_standalone() {
                    // RF=1: no secondaries, ACK immediately.
                    return Some(Frame {
                        opcode: Opcode::AppendAck,
                        flags: 0,
                        request_id: frame.request_id,
                        stream_id,
                        extent_id: ExtentId(0),
                        offset,
                        payload: ack_payload,
                    });
                }

                // Broadcast to ALL secondaries in parallel (outside per-stream lock).
                if let Some(ref tx) = self.forward_tx {
                    for secondary_addr in &ri.replica_addrs {
                        let req = ForwardRequest {
                            stream_id,
                            offset: offset.0,
                            payload: frame.payload.clone(),
                            downstream_addr: secondary_addr.clone(),
                        };
                        if let Err(e) = tx.try_send(req) {
                            warn!("failed to send ForwardRequest to {secondary_addr}: {e}");
                        }
                    }
                }

                // Queue deferred ACK (per-stream lock on ack_queues).
                if let Some(resp_tx) = response_tx {
                    let mut ack_queue = self
                        .ack_queues
                        .entry(stream_id)
                        .or_insert_with(|| AckQueue::new(ri.required_secondary_acks()));
                    ack_queue.pending.push_back(PendingAck {
                        request_id: frame.request_id,
                        stream_id,
                        response_tx: resp_tx.clone(),
                        assigned_offset: offset.0,
                        ack_payload,
                    });
                }

                // Deferred: return None.
                None
            }
            Some(_) => {
                // Secondary but not forwarded — shouldn't normally happen.
                Some(Frame {
                    opcode: Opcode::AppendAck,
                    flags: 0,
                    request_id: frame.request_id,
                    stream_id,
                    extent_id: ExtentId(0),
                    offset,
                    payload: Bytes::new(),
                })
            }
        }
    }

    fn handle_read(&self, frame: Frame) -> Frame {
        let stream_id = frame.stream_id;
        let stream_ref = match self.streams.get(&stream_id) {
            Some(s) => s,
            None => {
                return Frame::error_response(
                    frame.request_id,
                    ErrorCode::UnknownStream,
                    &format!("stream {:?} not found", stream_id),
                    ExtentId(0),
                );
            }
        };

        let count = frame.flags as u32;

        // Read payload carries the byte position within the extent arena.
        // If payload is empty (legacy / simple read), default to byte_pos=0.
        let byte_pos = if frame.payload.len() >= 8 {
            u64::from_be_bytes([
                frame.payload[0], frame.payload[1],
                frame.payload[2], frame.payload[3],
                frame.payload[4], frame.payload[5],
                frame.payload[6], frame.payload[7],
            ])
        } else {
            0
        };

        match stream_ref.read(frame.offset, byte_pos, count) {
            Ok(messages) => {
                let mut payload = BytesMut::new();
                for msg in &messages {
                    payload.put_u32(msg.len() as u32);
                    payload.extend_from_slice(msg);
                }
                Frame {
                    opcode: Opcode::ReadResp,
                    flags: messages.len() as u8,
                    request_id: frame.request_id,
                    stream_id,
                    extent_id: ExtentId(0),
                    offset: frame.offset,
                    payload: payload.freeze(),
                }
            }
            Err(e) => Frame::error_response(
                frame.request_id,
                ErrorCode::InternalError,
                &e.to_string(),
                ExtentId(0),
            ),
        }
    }

    fn handle_query_offset(&self, frame: Frame) -> Frame {
        let stream_id = frame.stream_id;
        let stream_ref = match self.streams.get(&stream_id) {
            Some(s) => s,
            None => {
                return Frame::error_response(
                    frame.request_id,
                    ErrorCode::UnknownStream,
                    &format!("stream {:?} not found", stream_id),
                    ExtentId(0),
                );
            }
        };

        Frame {
            opcode: Opcode::QueryOffsetResp,
            flags: 0,
            request_id: frame.request_id,
            stream_id,
            extent_id: ExtentId(0),
            offset: stream_ref.max_offset(),
            payload: Bytes::new(),
        }
    }

    fn handle_seal(&self, frame: Frame) -> Frame {
        let stream_id = frame.stream_id;
        let mut stream_ref = match self.streams.get_mut(&stream_id) {
            Some(s) => s,
            None => {
                return Frame::error_response(
                    frame.request_id,
                    ErrorCode::UnknownStream,
                    &format!("stream {:?} not found", stream_id),
                    ExtentId(0),
                );
            }
        };

        match stream_ref.seal_active() {
            Some((base_offset, message_count)) => {
                let offset = base_offset + message_count;
                info!("sealed active extent for stream {:?}, base_offset={base_offset}, message_count={message_count}, offset={offset}", stream_id);
                Frame {
                    opcode: Opcode::SealAck,
                    flags: 0,
                    request_id: frame.request_id,
                    stream_id,
                    extent_id: ExtentId(0),
                    offset: Offset(offset),
                    payload: Bytes::new(),
                }
            }
            None => Frame::error_response(
                frame.request_id,
                ErrorCode::InternalError,
                &format!("no active extent to seal on stream {:?}", stream_id),
                ExtentId(0),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn create_stream(store: &ExtentNodeStore, req_id: u32) -> StreamId {
        let frame = Frame {
            opcode: Opcode::CreateStream,
            flags: 0,
            request_id: req_id,
            stream_id: StreamId(0),
            extent_id: ExtentId(0),
            offset: Offset(0),
            payload: Bytes::new(),
        };
        let resp = store.handle_frame(frame, None).await.unwrap();
        assert_eq!(resp.opcode, Opcode::AppendAck);
        resp.stream_id
    }

    #[tokio::test]
    async fn create_and_append() {
        let store = ExtentNodeStore::new();
        let sid = create_stream(&store, 1).await;

        let resp = store.handle_frame(Frame {
            opcode: Opcode::Append,
            flags: 0,
            request_id: 2,
            stream_id: sid,
            extent_id: ExtentId(0),
            offset: Offset(0),
            payload: Bytes::from_static(b"hello"),
        }, None).await.unwrap();

        assert_eq!(resp.opcode, Opcode::AppendAck);
        assert_eq!(resp.offset, Offset(0));
        // AppendAck payload carries byte_pos (u64 BE).
        assert_eq!(resp.payload.len(), 8);
        let byte_pos = u64::from_be_bytes(resp.payload[..8].try_into().unwrap());
        assert_eq!(byte_pos, 0); // first record starts at byte 0
    }

    #[tokio::test]
    async fn append_to_unknown_stream() {
        let store = ExtentNodeStore::new();
        let resp = store.handle_frame(Frame {
            opcode: Opcode::Append,
            flags: 0,
            request_id: 1,
            stream_id: StreamId(999),
            extent_id: ExtentId(0),
            offset: Offset(0),
            payload: Bytes::from_static(b"fail"),
        }, None).await.unwrap();
        assert_eq!(resp.opcode, Opcode::Error);
    }

    #[tokio::test]
    async fn append_read_query_offset() {
        let store = ExtentNodeStore::new();
        let sid = create_stream(&store, 1).await;

        let mut byte_positions = Vec::new();
        for i in 0u32..3 {
            let resp = store.handle_frame(Frame {
                opcode: Opcode::Append,
                flags: 0,
                request_id: 10 + i,
                stream_id: sid,
                extent_id: ExtentId(0),
                offset: Offset(0),
                payload: Bytes::from(format!("msg{i}")),
            }, None).await.unwrap();
            assert_eq!(resp.opcode, Opcode::AppendAck);
            assert_eq!(resp.offset, Offset(i as u64));
            let bp = u64::from_be_bytes(resp.payload[..8].try_into().unwrap());
            byte_positions.push(bp);
        }

        let resp = store.handle_frame(Frame {
            opcode: Opcode::QueryOffset,
            flags: 0,
            request_id: 20,
            stream_id: sid,
            extent_id: ExtentId(0),
            offset: Offset(0),
            payload: Bytes::new(),
        }, None).await.unwrap();
        assert_eq!(resp.opcode, Opcode::QueryOffsetResp);
        assert_eq!(resp.offset, Offset(3));

        // Read all 3 from byte_pos=0 (default when payload is empty).
        let resp = store.handle_frame(Frame {
            opcode: Opcode::Read,
            flags: 3,
            request_id: 30,
            stream_id: sid,
            extent_id: ExtentId(0),
            offset: Offset(0),
            payload: Bytes::new(),
        }, None).await.unwrap();
        assert_eq!(resp.opcode, Opcode::ReadResp);
        assert_eq!(resp.flags, 3);

        let mut payload = &resp.payload[..];
        for i in 0..3 {
            let len = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
            payload = &payload[4..];
            let msg = &payload[..len];
            assert_eq!(msg, format!("msg{i}").as_bytes());
            payload = &payload[len..];
        }
        assert!(payload.is_empty());

        // Read msg1 directly via its byte_pos.
        let mut bp_payload = BytesMut::with_capacity(8);
        bp_payload.put_u64(byte_positions[1]);
        let resp = store.handle_frame(Frame {
            opcode: Opcode::Read,
            flags: 1, // count=1
            request_id: 31,
            stream_id: sid,
            extent_id: ExtentId(0),
            offset: Offset(1),
            payload: bp_payload.freeze(),
        }, None).await.unwrap();
        assert_eq!(resp.opcode, Opcode::ReadResp);
        assert_eq!(resp.flags, 1);
        let len = u32::from_be_bytes([
            resp.payload[0], resp.payload[1],
            resp.payload[2], resp.payload[3],
        ]) as usize;
        assert_eq!(&resp.payload[4..4 + len], b"msg1");
    }

    #[tokio::test]
    async fn register_extent_creates_stream() {
        use rpc::payload::build_register_extent_payload;

        let store = ExtentNodeStore::new();

        // RegisterExtent as Primary with 1 secondary (RF=2).
        let payload = build_register_extent_payload(42, 100, 0, 2, &["127.0.0.1:9802"]);
        let resp = store.handle_frame(Frame {
            opcode: Opcode::RegisterExtent,
            flags: 0,
            request_id: 1,
            stream_id: StreamId(42),
            extent_id: ExtentId(0),
            offset: Offset(0),
            payload,
        }, None).await.unwrap();

        assert_eq!(resp.opcode, Opcode::RegisterExtentAck);
        assert_eq!(resp.stream_id, StreamId(42));

        assert!(store.streams.contains_key(&StreamId(42)));

        let ri = store.get_replica_info(StreamId(42)).unwrap();
        assert!(ri.is_primary());
        assert!(!ri.is_standalone());
        assert_eq!(ri.replica_addrs, vec!["127.0.0.1:9802"]);
        assert_eq!(ri.extent_id, ExtentId(100));
        assert_eq!(ri.replication_factor, 2);

        // AckQueue should be initialized for Primary.
        let aq = store.ack_queues.get(&StreamId(42)).unwrap();
        assert_eq!(aq.required_secondary_acks, 1);
    }

    #[tokio::test]
    async fn register_extent_secondary() {
        use rpc::payload::build_register_extent_payload;

        let store = ExtentNodeStore::new();

        // RegisterExtent as Secondary (RF=2, no replica addrs).
        let payload = build_register_extent_payload(42, 100, 1, 2, &[]);
        let resp = store.handle_frame(Frame {
            opcode: Opcode::RegisterExtent,
            flags: 0,
            request_id: 1,
            stream_id: StreamId(42),
            extent_id: ExtentId(0),
            offset: Offset(0),
            payload,
        }, None).await.unwrap();

        assert_eq!(resp.opcode, Opcode::RegisterExtentAck);

        let ri = store.get_replica_info(StreamId(42)).unwrap();
        assert!(!ri.is_primary());
        assert_eq!(ri.role, 1);
        assert!(ri.replica_addrs.is_empty());
        assert_eq!(ri.replication_factor, 2);

        // Secondary should NOT have an AckQueue.
        assert!(!store.ack_queues.contains_key(&StreamId(42)));
    }

    #[tokio::test]
    async fn register_extent_then_append_rf1() {
        use rpc::payload::build_register_extent_payload;

        let store = ExtentNodeStore::new();

        // Register as Primary, RF=1 (standalone).
        let payload = build_register_extent_payload(10, 50, 0, 1, &[]);
        store.handle_frame(Frame {
            opcode: Opcode::RegisterExtent,
            flags: 0,
            request_id: 1,
            stream_id: StreamId(10),
            extent_id: ExtentId(0),
            offset: Offset(0),
            payload,
        }, None).await.unwrap();

        // Append — standalone should ACK immediately.
        let resp = store.handle_frame(Frame {
            opcode: Opcode::Append,
            flags: 0,
            request_id: 2,
            stream_id: StreamId(10),
            extent_id: ExtentId(0),
            offset: Offset(0),
            payload: Bytes::from_static(b"hello standalone"),
        }, None).await.unwrap();

        assert_eq!(resp.opcode, Opcode::AppendAck);
        assert_eq!(resp.offset, Offset(0));
    }

    #[tokio::test]
    async fn primary_append_defers_and_broadcasts() {
        use rpc::payload::build_register_extent_payload;

        let (forward_tx, mut forward_rx) = mpsc::channel(100);
        let (resp_tx, mut resp_rx) = mpsc::channel::<Frame>(100);

        let store = ExtentNodeStore::with_forward_tx(forward_tx);

        // Register as Primary with 2 secondaries (RF=3).
        let payload = build_register_extent_payload(
            10, 50, 0, 3,
            &["127.0.0.1:9802", "127.0.0.1:9803"],
        );
        store.handle_frame(Frame {
            opcode: Opcode::RegisterExtent,
            flags: 0,
            request_id: 1,
            stream_id: StreamId(10),
            extent_id: ExtentId(0),
            offset: Offset(0),
            payload,
        }, None).await.unwrap();

        // Append — should return None (deferred), send 2 ForwardRequests.
        let result = store.handle_frame(Frame {
            opcode: Opcode::Append,
            flags: 0,
            request_id: 2,
            stream_id: StreamId(10),
            extent_id: ExtentId(0),
            offset: Offset(0),
            payload: Bytes::from_static(b"broadcast msg"),
        }, Some(&resp_tx)).await;

        assert!(result.is_none(), "Primary with secondaries should defer ACK");

        // Should have 2 ForwardRequests (one per secondary).
        let fwd1 = forward_rx.try_recv().unwrap();
        let fwd2 = forward_rx.try_recv().unwrap();
        assert_eq!(fwd1.stream_id, StreamId(10));
        assert_eq!(fwd2.stream_id, StreamId(10));
        // Both have same payload and offset.
        assert_eq!(fwd1.offset, 0);
        assert_eq!(fwd2.offset, 0);
        // Different downstream addrs.
        let addrs: Vec<String> = vec![fwd1.downstream_addr, fwd2.downstream_addr];
        assert!(addrs.contains(&"127.0.0.1:9802".to_string()));
        assert!(addrs.contains(&"127.0.0.1:9803".to_string()));

        // PendingAck should be in the ack_queue.
        let ack_queue = store.ack_queues.get(&StreamId(10)).unwrap();
        assert_eq!(ack_queue.pending.len(), 1);
        assert_eq!(ack_queue.pending[0].assigned_offset, 0);
        // RF=3 requires 1 secondary ACK.
        assert_eq!(ack_queue.required_secondary_acks, 1);

        // Simulate watermark from first secondary (quorum met with 1 ACK for RF=3).
        drop(ack_queue); // release DashMap read guard before acquiring write guard
        let mut ack_queue = store.ack_queues.get_mut(&StreamId(10)).unwrap();
        ack_queue.ack_from_secondary("127.0.0.1:9802", 0);
        ack_queue.drain_quorum();

        // The client response channel should now have the AppendAck.
        let ack = resp_rx.try_recv().unwrap();
        assert_eq!(ack.opcode, Opcode::AppendAck);
        assert_eq!(ack.offset, Offset(0));
        assert_eq!(ack.request_id, 2);
    }

    #[tokio::test]
    async fn secondary_returns_watermark() {
        use rpc::payload::build_register_extent_payload;

        let store = ExtentNodeStore::new();

        // Register as Secondary (RF=2).
        let payload = build_register_extent_payload(10, 50, 1, 2, &[]);
        store.handle_frame(Frame {
            opcode: Opcode::RegisterExtent,
            flags: 0,
            request_id: 1,
            stream_id: StreamId(10),
            extent_id: ExtentId(0),
            offset: Offset(0),
            payload,
        }, None).await.unwrap();

        // Forwarded append.
        let resp = store.handle_frame(Frame {
            opcode: Opcode::Append,
            flags: FLAG_FORWARDED,
            request_id: 2,
            stream_id: StreamId(10),
            extent_id: ExtentId(0),
            offset: Offset(0),
            payload: Bytes::from_static(b"forwarded msg"),
        }, None).await.unwrap();

        assert_eq!(resp.opcode, Opcode::Watermark);
        assert_eq!(resp.stream_id, StreamId(10));
        assert_eq!(resp.offset, Offset(0));
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
                response_tx: resp_tx.clone(),
                assigned_offset: i,
                ack_payload: Bytes::new(),
            });
        }

        // Single cumulative ACK at offset 2 from one secondary.
        ack_queue.ack_from_secondary("sec-1", 2);
        ack_queue.drain_quorum();

        // All 3 should be drained.
        let ack0 = resp_rx.try_recv().unwrap();
        let ack1 = resp_rx.try_recv().unwrap();
        let ack2 = resp_rx.try_recv().unwrap();
        assert_eq!(ack0.offset, Offset(0));
        assert_eq!(ack1.offset, Offset(1));
        assert_eq!(ack2.offset, Offset(2));
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

    // ── Concurrent multi-stream benchmark ────────────────────────────────────

    /// Benchmark: N tokio tasks appending concurrently to N independent streams.
    ///
    /// Verifies that the per-stream DashMap design allows true parallelism:
    /// - Each stream's offsets are contiguous [0..APPENDS_PER_STREAM)
    /// - All data is readable and correct after concurrent writes
    /// - No cross-stream interference
    ///
    /// With the old global Mutex, all N tasks would serialize; with DashMap,
    /// they run in parallel on different DashMap shards.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_multi_stream_appends() {
        use std::sync::Arc;
        use std::time::Instant;

        const NUM_STREAMS: u64 = 8;
        const APPENDS_PER_STREAM: u64 = 5_000;
        const PAYLOAD_SIZE: usize = 128; // bytes per message

        let store = Arc::new(ExtentNodeStore::new());

        // Pre-create all streams so IDs are deterministic.
        let mut stream_ids = Vec::new();
        for i in 0..NUM_STREAMS {
            let sid = create_stream(&store, i as u32).await;
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
                    let resp = store.handle_frame(Frame {
                        opcode: Opcode::Append,
                        flags: 0,
                        request_id: seq as u32,
                        stream_id: sid,
                        extent_id: ExtentId(0),
                        offset: Offset(0),
                        payload: Bytes::from(payload_data.clone()),
                    }, None).await.unwrap();

                    assert_eq!(resp.opcode, Opcode::AppendAck,
                        "task {task_idx} seq {seq}: expected AppendAck");
                    offsets.push(resp.offset.0);
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
            assert_eq!(offsets.len(), APPENDS_PER_STREAM as usize,
                "task {task_idx}: wrong number of offsets");

            let mut sorted = offsets.clone();
            sorted.sort_unstable();
            sorted.dedup();
            assert_eq!(sorted.len(), APPENDS_PER_STREAM as usize,
                "task {task_idx}: duplicate offsets detected");
            assert_eq!(*sorted.first().unwrap(), 0,
                "task {task_idx}: first offset should be 0");
            assert_eq!(*sorted.last().unwrap(), APPENDS_PER_STREAM - 1,
                "task {task_idx}: last offset should be {}", APPENDS_PER_STREAM - 1);
        }

        // 2. Each stream should have correct max_offset.
        for (task_idx, &sid) in stream_ids.iter().enumerate() {
            let resp = store.handle_frame(Frame {
                opcode: Opcode::QueryOffset,
                flags: 0,
                request_id: 0,
                stream_id: sid,
                extent_id: ExtentId(0),
                offset: Offset(0),
                payload: Bytes::new(),
            }, None).await.unwrap();
            assert_eq!(resp.offset, Offset(APPENDS_PER_STREAM),
                "task {task_idx}: stream max_offset mismatch");
        }

        // 3. Read all records from each stream and verify payload content.
        for (task_idx, &sid) in stream_ids.iter().enumerate() {
            let expected_byte = b'A' + (task_idx as u8 % 26);
            let resp = store.handle_frame(Frame {
                opcode: Opcode::Read,
                flags: 100, // read up to 100 at a time
                request_id: 0,
                stream_id: sid,
                extent_id: ExtentId(0),
                offset: Offset(0),
                payload: Bytes::new(),
            }, None).await.unwrap();
            assert_eq!(resp.opcode, Opcode::ReadResp);
            let count = resp.flags as usize;
            assert!(count > 0, "task {task_idx}: expected at least 1 message");

            // Verify first record's payload.
            let len = u32::from_be_bytes([
                resp.payload[0], resp.payload[1],
                resp.payload[2], resp.payload[3],
            ]) as usize;
            assert_eq!(len, PAYLOAD_SIZE,
                "task {task_idx}: payload size mismatch");
            assert_eq!(resp.payload[4], expected_byte,
                "task {task_idx}: payload content mismatch");
        }

        // 4. Verify metrics counters.
        let total_expected = NUM_STREAMS * APPENDS_PER_STREAM;
        let (appends, bytes, active_count) = store.snapshot_metrics();
        assert_eq!(appends, total_expected,
            "metrics: append count mismatch");
        assert_eq!(bytes, total_expected * PAYLOAD_SIZE as u64,
            "metrics: bytes_written mismatch");
        assert_eq!(active_count, NUM_STREAMS as u32,
            "metrics: active extent count mismatch");

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
            let sid = create_stream(&store, i as u32).await;
            writer_sids.push(sid);
        }

        // Create reader streams and pre-populate them with data.
        let mut reader_sids = Vec::new();
        for i in 0..NUM_READER_STREAMS {
            let sid = create_stream(&store, (100 + i) as u32).await;
            for j in 0..100u32 {
                store.handle_frame(Frame {
                    opcode: Opcode::Append,
                    flags: 0,
                    request_id: j,
                    stream_id: sid,
                    extent_id: ExtentId(0),
                    offset: Offset(0),
                    payload: Bytes::from(format!("pre-{j}")),
                }, None).await.unwrap();
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
                    let resp = store.handle_frame(Frame {
                        opcode: Opcode::Append,
                        flags: 0,
                        request_id: seq as u32,
                        stream_id: sid,
                        extent_id: ExtentId(0),
                        offset: Offset(0),
                        payload: Bytes::from_static(b"write-payload"),
                    }, None).await.unwrap();
                    assert_eq!(resp.opcode, Opcode::AppendAck);
                }
                "writer_done"
            }));
        }

        // Spawn reader tasks.
        for &sid in &reader_sids {
            let store = Arc::clone(&store);
            handles.push(tokio::spawn(async move {
                for _ in 0..READS_PER_STREAM {
                    let resp = store.handle_frame(Frame {
                        opcode: Opcode::Read,
                        flags: 10, // read 10 messages
                        request_id: 0,
                        stream_id: sid,
                        extent_id: ExtentId(0),
                        offset: Offset(0),
                        payload: Bytes::new(),
                    }, None).await.unwrap();
                    assert_eq!(resp.opcode, Opcode::ReadResp);
                    assert!(resp.flags > 0, "reader should get at least 1 message");
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
            let resp = store.handle_frame(Frame {
                opcode: Opcode::QueryOffset,
                flags: 0,
                request_id: 0,
                stream_id: sid,
                extent_id: ExtentId(0),
                offset: Offset(0),
                payload: Bytes::new(),
            }, None).await.unwrap();
            assert_eq!(resp.offset, Offset(APPENDS_PER_STREAM),
                "writer stream {:?} should have {APPENDS_PER_STREAM} messages", sid);
        }

        // Verify reader streams are untouched (still 100 messages each).
        for &sid in &reader_sids {
            let resp = store.handle_frame(Frame {
                opcode: Opcode::QueryOffset,
                flags: 0,
                request_id: 0,
                stream_id: sid,
                extent_id: ExtentId(0),
                offset: Offset(0),
                payload: Bytes::new(),
            }, None).await.unwrap();
            assert_eq!(resp.offset, Offset(100),
                "reader stream {:?} should still have 100 messages", sid);
        }
    }

    /// Benchmark: multiple tasks appending to the SAME stream concurrently.
    ///
    /// Verifies the lock-free extent append works correctly when multiple
    /// tokio tasks target the same stream. Each append goes through the
    /// DashMap per-stream lock, but the Extent's atomic CAS handles the
    /// actual slot reservation and commit ordering.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_appends_same_stream() {
        use std::sync::Arc;
        use std::time::Instant;

        const NUM_TASKS: u64 = 8;
        const APPENDS_PER_TASK: u64 = 2_000;

        let store = Arc::new(ExtentNodeStore::new());
        let sid = create_stream(&store, 1).await;

        let start = Instant::now();

        let mut handles = Vec::new();
        for task_idx in 0..NUM_TASKS {
            let store = Arc::clone(&store);
            handles.push(tokio::spawn(async move {
                let mut offsets = Vec::with_capacity(APPENDS_PER_TASK as usize);
                for seq in 0..APPENDS_PER_TASK {
                    let resp = store.handle_frame(Frame {
                        opcode: Opcode::Append,
                        flags: 0,
                        request_id: seq as u32,
                        stream_id: sid,
                        extent_id: ExtentId(0),
                        offset: Offset(0),
                        payload: Bytes::from(format!("t{task_idx}-m{seq}")),
                    }, None).await.unwrap();
                    assert_eq!(resp.opcode, Opcode::AppendAck);
                    offsets.push(resp.offset.0);
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
        assert_eq!(all_offsets.len(), total,
            "duplicate offsets detected across tasks");
        assert_eq!(*all_offsets.first().unwrap(), 0);
        assert_eq!(*all_offsets.last().unwrap(), (total - 1) as u64);

        // Verify max_offset.
        let resp = store.handle_frame(Frame {
            opcode: Opcode::QueryOffset,
            flags: 0,
            request_id: 0,
            stream_id: sid,
            extent_id: ExtentId(0),
            offset: Offset(0),
            payload: Bytes::new(),
        }, None).await.unwrap();
        assert_eq!(resp.offset, Offset(total as u64));

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

    // ── ReplicaInfo unit tests ───────────────────────────────────────────────

    #[test]
    fn replica_info_is_primary() {
        let ri = ReplicaInfo {
            stream_id: StreamId(1),
            extent_id: ExtentId(10),
            role: ROLE_PRIMARY,
            replication_factor: 3,
            replica_addrs: vec!["a:1".into(), "b:2".into()],
        };
        assert!(ri.is_primary());
        assert!(!ri.is_standalone());
    }

    #[test]
    fn replica_info_is_secondary() {
        let ri = ReplicaInfo {
            stream_id: StreamId(1),
            extent_id: ExtentId(10),
            role: 1,
            replication_factor: 2,
            replica_addrs: vec![],
        };
        assert!(!ri.is_primary());
    }

    #[test]
    fn replica_info_is_standalone_rf1() {
        let ri = ReplicaInfo {
            stream_id: StreamId(1),
            extent_id: ExtentId(10),
            role: ROLE_PRIMARY,
            replication_factor: 1,
            replica_addrs: vec![],
        };
        assert!(ri.is_primary());
        assert!(ri.is_standalone());
    }

    #[test]
    fn replica_info_is_standalone_no_replicas() {
        // Even with RF > 1, if replica_addrs is empty, it's standalone.
        let ri = ReplicaInfo {
            stream_id: StreamId(1),
            extent_id: ExtentId(10),
            role: ROLE_PRIMARY,
            replication_factor: 3,
            replica_addrs: vec![],
        };
        assert!(ri.is_standalone());
    }

    #[test]
    fn replica_info_required_secondary_acks() {
        // RF=1 -> 0, RF=2 -> 1, RF=3 -> 1, RF=4 -> 2, RF=5 -> 2
        let cases: &[(u16, u32)] = &[(1, 0), (2, 1), (3, 1), (4, 2), (5, 2)];
        for &(rf, expected) in cases {
            let ri = ReplicaInfo {
                stream_id: StreamId(1),
                extent_id: ExtentId(1),
                role: ROLE_PRIMARY,
                replication_factor: rf,
                replica_addrs: vec![],
            };
            assert_eq!(
                ri.required_secondary_acks(),
                expected,
                "RF={rf}: expected {expected}"
            );
        }
    }

    // ── AckQueue additional edge cases ───────────────────────────────────────

    #[test]
    fn ack_queue_quorum_offset_rf1_returns_none() {
        let aq = AckQueue::new(0);
        assert_eq!(aq.quorum_offset(), None);
    }

    #[test]
    fn ack_queue_quorum_offset_no_secondaries_reported() {
        let aq = AckQueue::new(2);
        assert_eq!(aq.quorum_offset(), None);
    }

    #[test]
    fn ack_queue_ack_from_secondary_idempotent_lower_offset() {
        let mut aq = AckQueue::new(1);
        aq.ack_from_secondary("sec-1", 10);
        // Lower offset should not decrease the recorded value.
        aq.ack_from_secondary("sec-1", 5);
        assert_eq!(aq.quorum_offset(), Some(10));
    }

    #[test]
    fn ack_queue_ack_from_secondary_updates_higher_offset() {
        let mut aq = AckQueue::new(1);
        aq.ack_from_secondary("sec-1", 5);
        assert_eq!(aq.quorum_offset(), Some(5));
        aq.ack_from_secondary("sec-1", 10);
        assert_eq!(aq.quorum_offset(), Some(10));
    }

    #[tokio::test]
    async fn ack_queue_drain_quorum_with_no_pending() {
        let mut aq = AckQueue::new(1);
        aq.ack_from_secondary("sec-1", 5);
        // drain_quorum should be a no-op when no pending ACKs exist.
        aq.drain_quorum();
        assert!(aq.pending.is_empty());
    }

    #[tokio::test]
    async fn ack_queue_drain_quorum_partial() {
        // Only some pending ACKs should be drained when quorum_offset < max pending.
        let (resp_tx, mut resp_rx) = mpsc::channel::<Frame>(100);

        let mut aq = AckQueue::new(1);
        for i in 0u64..5 {
            aq.pending.push_back(PendingAck {
                request_id: i as u32,
                stream_id: StreamId(1),
                response_tx: resp_tx.clone(),
                assigned_offset: i,
                ack_payload: Bytes::new(),
            });
        }

        // Only ACK up to offset 2.
        aq.ack_from_secondary("sec-1", 2);
        aq.drain_quorum();

        // Offsets 0, 1, 2 should be drained; 3, 4 should remain.
        assert_eq!(aq.pending.len(), 2);
        assert_eq!(aq.pending[0].assigned_offset, 3);
        assert_eq!(aq.pending[1].assigned_offset, 4);

        // 3 ACKs should have been sent.
        for expected_offset in 0u64..3 {
            let ack = resp_rx.try_recv().unwrap();
            assert_eq!(ack.offset, Offset(expected_offset));
        }
        assert!(resp_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn read_from_unknown_stream() {
        let store = ExtentNodeStore::new();
        let resp = store.handle_frame(Frame {
            opcode: Opcode::Read,
            flags: 1,
            request_id: 1,
            stream_id: StreamId(999),
            extent_id: ExtentId(0),
            offset: Offset(0),
            payload: Bytes::new(),
        }, None).await.unwrap();
        assert_eq!(resp.opcode, Opcode::Error);
    }

    #[tokio::test]
    async fn query_offset_unknown_stream() {
        let store = ExtentNodeStore::new();
        let resp = store.handle_frame(Frame {
            opcode: Opcode::QueryOffset,
            flags: 0,
            request_id: 1,
            stream_id: StreamId(999),
            extent_id: ExtentId(0),
            offset: Offset(0),
            payload: Bytes::new(),
        }, None).await.unwrap();
        assert_eq!(resp.opcode, Opcode::Error);
    }

    #[tokio::test]
    async fn seal_unknown_stream() {
        let store = ExtentNodeStore::new();
        let resp = store.handle_frame(Frame {
            opcode: Opcode::Seal,
            flags: 0,
            request_id: 1,
            stream_id: StreamId(999),
            extent_id: ExtentId(0),
            offset: Offset(0),
            payload: Bytes::new(),
        }, None).await.unwrap();
        assert_eq!(resp.opcode, Opcode::Error);
    }

    #[tokio::test]
    async fn create_multiple_streams_get_unique_ids() {
        let store = ExtentNodeStore::new();
        let sid1 = create_stream(&store, 1).await;
        let sid2 = create_stream(&store, 2).await;
        let sid3 = create_stream(&store, 3).await;
        assert_ne!(sid1, sid2);
        assert_ne!(sid2, sid3);
        assert_ne!(sid1, sid3);
    }

    #[tokio::test]
    async fn append_empty_payload() {
        let store = ExtentNodeStore::new();
        let sid = create_stream(&store, 1).await;

        let resp = store.handle_frame(Frame {
            opcode: Opcode::Append,
            flags: 0,
            request_id: 2,
            stream_id: sid,
            extent_id: ExtentId(0),
            offset: Offset(0),
            payload: Bytes::new(),
        }, None).await.unwrap();

        assert_eq!(resp.opcode, Opcode::AppendAck);
        assert_eq!(resp.offset, Offset(0));
    }
}
