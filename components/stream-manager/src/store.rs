use std::time::Duration;

use bytes::Buf;

use crate::allocator::Allocator;
use crate::metadata::{MetadataStore, SealResult};
use bytes::Bytes;
use common::config::{
    DEFAULT_CACHE_EXTENTS, DEFAULT_EXTENT_GROWTH_FACTOR, DEFAULT_MAX_EXTENT_CAPACITY,
    DEFAULT_MIN_EXTENT_CAPACITY,
};
use common::errors::StorageError;
use common::types::{Epoch, ErrorCode, ExtentId, Offset, Opcode, StreamId};
use futures_util::future;
use rpc::frame::Frame;
use rpc::payload::{
    build_register_extent_payload, encode_extent_info_vec, parse_connect_payload,
    parse_heartbeat_payload, parse_string_payload,
};
use rpc::frame::variable_header::VariableHeader;
use server::handler::RequestHandler;
use tracing::{error, info, warn};

/// Seal an ExtentNode's extent and return the sealed extent info.
///
/// Sends SealExtentNodeRequest to the EN and parses SealExtentNodeResp.
/// Returns (sealed_extent_id, start_offset, end_offset, optional payload with predecessor extents).
async fn seal_extent_node_static(
    addr: &str,
    stream_id: StreamId,
    epoch: Epoch,
    extent_id_from: ExtentId,
    start_offset: u64,
) -> Result<(ExtentId, u64, u64, Option<Bytes>), StorageError> {
    let client = client::StreamClient::connect(addr).await.map_err(|e| {
        StorageError::Internal(format!("connect to ExtentNode {addr} for Seal: {e}"))
    })?;

    let resp = client
        .send_frame(Frame::new(
            VariableHeader::SealExtentNodeRequest {
                request_id: 0,
                stream_id,
                epoch,
                extent_id_from,
                start_offset,
            },
            None,
        ))
        .await
        .map_err(|e| StorageError::Internal(format!("Seal to ExtentNode {addr}: {e}")))?;

    if resp.is_error_response() {
        let msg = String::from_utf8_lossy(resp.payload.as_deref().unwrap_or_default()).to_string();
        return Err(StorageError::Internal(format!(
            "ExtentNode {addr} rejected Seal: {msg}"
        )));
    }

    // Parse SealExtentNodeResp.
    match &resp.variable_header {
        VariableHeader::SealExtentNodeResp {
            extent_id,
            start_offset: so,
            end_offset,
            ..
        } => Ok((*extent_id, *so, *end_offset, resp.payload.clone())),
        _ => Err(StorageError::Internal(format!(
            "ExtentNode {addr} returned unexpected response: {:?}",
            resp.opcode()
        ))),
    }
}

/// Query an ExtentNode for all extents it holds for a stream (used during recovery/reconciliation).
///
/// Sends a ReportExtents RPC and parses the ReportExtentsResp payload.
/// Parse the predecessor extent payload from SealExtentNodeResp.
/// Same format as ReportExtentsResp: [num:u32] per extent: [extent_id:u32][start_offset:u64][end_offset:u64][state:u8]
fn parse_seal_predecessor_payload(
    payload: &Bytes,
) -> Option<Vec<(ExtentId, u64, u64, common::types::ExtentState)>> {
    let mut buf = &payload[..];
    if buf.len() < 4 {
        return None;
    }
    let num = buf.get_u32() as usize;
    let mut result = Vec::with_capacity(num);
    for _ in 0..num {
        if buf.remaining() < 4 + 8 + 8 + 1 {
            return None;
        }
        let eid = ExtentId(buf.get_u32());
        let start = buf.get_u64();
        let end = buf.get_u64();
        let state_val = buf.get_u8();
        let state = common::types::ExtentState::from_u8(state_val)
            .unwrap_or(common::types::ExtentState::Unspecified);
        result.push((eid, start, end, state));
    }
    Some(result)
}

/// Response format: [num_extents:u32] then for each extent: [extent_id:u32][start_offset:u64][end_offset:u64][state:u8]
///
/// Returns Vec of (ExtentId, start_offset, end_offset, ExtentState) tuples.
async fn report_extents_from_node_static(
    addr: &str,
    stream_id: StreamId,
    epoch: Epoch,
) -> Result<Vec<(ExtentId, u64, u64, common::types::ExtentState)>, StorageError> {
    use bytes::Buf;

    let client = client::StreamClient::connect(addr).await.map_err(|e| {
        StorageError::Internal(format!(
            "connect to ExtentNode {addr} for ReportExtents: {e}"
        ))
    })?;

    let resp = client
        .send_frame(Frame::new(
            VariableHeader::ReportExtents {
                request_id: 0,
                stream_id,
                epoch,
            },
            None,
        ))
        .await
        .map_err(|e| StorageError::Internal(format!("ReportExtents to ExtentNode {addr}: {e}")))?;

    if resp.is_error_response() {
        let msg = String::from_utf8_lossy(resp.payload.as_deref().unwrap_or_default()).to_string();
        return Err(StorageError::Internal(format!(
            "ExtentNode {addr} rejected ReportExtents: {msg}"
        )));
    }

    // Parse ReportExtentsResp payload: [num_extents:u32] then for each: [extent_id:u32][start_offset:u64][end_offset:u64][state:u8]
    let payload = resp.payload.clone().unwrap_or_else(Bytes::new);
    let mut buf = &payload[..];

    if buf.len() < 4 {
        return Err(StorageError::Internal(format!(
            "ReportExtentsResp from {addr} has invalid payload: too short"
        )));
    }

    let num_extents = buf.get_u32() as usize;
    let mut extents = Vec::with_capacity(num_extents);

    for _ in 0..num_extents {
        if buf.len() < 4 + 8 + 8 + 1 {
            return Err(StorageError::Internal(format!(
                "ReportExtentsResp from {addr} has truncated extent record"
            )));
        }

        let extent_id = ExtentId(buf.get_u32());
        let start_offset = buf.get_u64();
        let end_offset = buf.get_u64();
        let state_u8 = buf.get_u8();

        let state = common::types::ExtentState::from_u8(state_u8)
            .unwrap_or(common::types::ExtentState::Unspecified);

        extents.push((extent_id, start_offset, end_offset, state));
    }

    Ok(extents)
}

/// The Stream Manager's request handler.
pub struct StreamManagerStore {
    store: MetadataStore,
    allocator: Allocator,
    /// Default replication factor used when a client sends replication_factor=0
    /// (meaning "use server default"). Per-stream replication factor is stored in the stream table.
    default_replication_factor: usize,
}

impl StreamManagerStore {
    pub fn new(store: MetadataStore, default_replication_factor: usize) -> Self {
        assert!(
            default_replication_factor >= 1,
            "default_replication_factor must be >= 1"
        );
        let allocator = Allocator::new(store.clone());
        Self {
            store,
            allocator,
            default_replication_factor,
        }
    }

    /// Access the underlying MetadataStore (e.g., for heartbeat checker).
    pub fn store(&self) -> &MetadataStore {
        &self.store
    }

    /// Reconcile SM metadata with EN state on startup.
    ///
    /// During SM downtime, Primary ENs may have autonomously created extents
    /// (extent-full within an epoch). The NOTIFY_SEALED_EXTENT fire-and-forget
    /// notifications would have been lost. This method queries each Primary EN
    /// for its extent state and updates MySQL metadata accordingly.
    ///
    /// For each stream with an active extent:
    /// 1. Find the Primary EN from replica metadata
    /// 2. Send REPORT_EXTENTS(epoch) to the Primary
    /// 3. Call reconcile_extents to upsert any missing extents into MySQL
    ///
    /// Best-effort: failures for individual streams are logged and skipped.
    pub async fn reconcile_on_startup(&self) {
        let streams = match self.store.get_streams_with_active_extents().await {
            Ok(s) => s,
            Err(e) => {
                warn!("startup reconciliation: failed to get active streams: {e}");
                return;
            }
        };

        if streams.is_empty() {
            info!("startup reconciliation: no active streams to reconcile");
            return;
        }

        info!(
            "startup reconciliation: checking {} stream(s) for missed extent notifications",
            streams.len()
        );

        let mut reconciled = 0u32;
        let mut skipped = 0u32;

        for (stream_id, epoch) in &streams {
            // Find the active extent to look up the Primary's address.
            let active = match self.store.get_active_extent(*stream_id).await {
                Ok(Some(ext)) => ext,
                Ok(None) => {
                    // Active extent may have been sealed between our query and now.
                    continue;
                }
                Err(e) => {
                    warn!(
                        "startup reconciliation: get_active_extent for stream {:?}: {e}",
                        stream_id
                    );
                    skipped += 1;
                    continue;
                }
            };

            let replicas = match self.store.get_replicas(*stream_id, active.epoch).await {
                Ok(r) => r,
                Err(e) => {
                    warn!(
                        "startup reconciliation: get_replicas for stream {:?}: {e}",
                        stream_id
                    );
                    skipped += 1;
                    continue;
                }
            };

            let primary_addr = match replicas.iter().find(|r| r.role == 0) {
                Some(r) => r.node_addr.clone(),
                None => {
                    warn!(
                        "startup reconciliation: no primary replica for stream {:?}",
                        stream_id
                    );
                    skipped += 1;
                    continue;
                }
            };

            // Query the Primary EN for all extents at this epoch.
            let en_extents = match tokio::time::timeout(
                Duration::from_secs(2),
                report_extents_from_node_static(&primary_addr, *stream_id, *epoch),
            )
            .await
            {
                Ok(Ok(extents)) => extents,
                Ok(Err(e)) => {
                    warn!(
                        "startup reconciliation: REPORT_EXTENTS to {primary_addr} for stream {:?}: {e}",
                        stream_id
                    );
                    skipped += 1;
                    continue;
                }
                Err(_) => {
                    warn!(
                        "startup reconciliation: REPORT_EXTENTS to {primary_addr} timed out for stream {:?}",
                        stream_id
                    );
                    skipped += 1;
                    continue;
                }
            };

            if en_extents.is_empty() {
                continue;
            }

            // Reconcile: upsert any missing extents into MySQL, copy replicas.
            if let Err(e) = self
                .store
                .reconcile_extents(*stream_id, *epoch, &en_extents)
                .await
            {
                warn!(
                    "startup reconciliation: reconcile_extents for stream {:?}: {e}",
                    stream_id
                );
                skipped += 1;
                continue;
            }

            // No per-extent replica copy needed — replicas are at (stream, epoch) level.

            reconciled += 1;
        }

        info!(
            "startup reconciliation complete: {reconciled} stream(s) reconciled, {skipped} skipped"
        );
    }

    /// Register the new extent on the Primary ExtentNode and wait for its ACK.
    ///
    /// This guarantees the Primary is ready to accept appends before any client
    /// learns about the new extent (via SealStreamManagerResp or DescribeStream).
    ///
    /// Uses a 1-second timeout covering both TCP connect and the RegisterExtent
    /// round-trip. On a healthy LAN this completes in sub-millisecond; a timeout
    /// indicates the Primary is likely dead or unreachable.
    ///
    /// `primary_addr`: the Primary's listen address.
    /// `secondary_addrs`: addresses of all Secondaries (passed to Primary so it
    /// can broadcast Forward frames).
    async fn register_primary(
        &self,
        stream_id: StreamId,
        extent_id: ExtentId,
        primary_addr: &str,
        secondary_addrs: &[&str],
        replication_factor: u16,
        epoch: Epoch,
        min_extent_capacity: u32,
        max_extent_capacity: u32,
        cache_extents: u32,
        extent_growth_factor: u32,
    ) -> Result<(), StorageError> {
        let payload = build_register_extent_payload(secondary_addrs);
        let addr = primary_addr.to_string();
        let sid = stream_id;
        let eid = extent_id;
        let rf = replication_factor;
        let ep = epoch;
        let minc = min_extent_capacity;
        let maxc = max_extent_capacity;
        let ce = cache_extents;
        let gf = extent_growth_factor;

        let result = tokio::time::timeout(Duration::from_millis(500), async {
            let client = client::StreamClient::connect(&addr).await.map_err(|e| {
                StorageError::Internal(format!(
                    "connect to Primary ExtentNode {addr} for RegisterExtent: {e}"
                ))
            })?;

            let resp = client
                .send_frame(Frame::new(
                    VariableHeader::RegisterExtent {
                        request_id: 0,
                        stream_id: sid,
                        extent_id: eid,
                        role: 0, // Primary
                        replication_factor: rf,
                        epoch: ep,
                        extent_capacity: minc,
                        min_extent_capacity: minc,
                        max_extent_capacity: maxc,
                        cache_extents: ce,
                        extent_growth_factor: gf,
                    },
                    Some(payload),
                ))
                .await
                .map_err(|e| {
                    StorageError::Internal(format!(
                        "RegisterExtent to Primary ExtentNode {addr}: {e}"
                    ))
                })?;

            if resp.is_error_response() {
                let msg = String::from_utf8_lossy(resp.payload.as_deref().unwrap_or_default())
                    .to_string();
                return Err(StorageError::Internal(format!(
                    "Primary ExtentNode {addr} rejected RegisterExtent: {msg}"
                )));
            }

            Ok(())
        })
        .await;

        match result {
            Ok(Ok(())) => {
                info!(
                    "RegisterExtent ACK from Primary {primary_addr}: stream={}, extent={}, rf={replication_factor}, secondaries={}",
                    stream_id,
                    extent_id,
                    secondary_addrs.join(", ")
                );
                Ok(())
            }
            Ok(Err(e)) => Err(e),
            Err(_) => Err(StorageError::Internal(format!(
                "RegisterExtent to Primary {primary_addr} timed out (1s)"
            ))),
        }
    }

    /// Fire-and-forget RegisterExtent to each Secondary ExtentNode.
    ///
    /// Secondaries create extents lazily on the first Forward frame, so these
    /// RPCs are hints for pre-allocation, not required for correctness.
    /// Each is spawned as an independent task to avoid blocking the caller.
    fn notify_secondaries(
        &self,
        stream_id: StreamId,
        extent_id: ExtentId,
        secondary_addrs: &[String],
        replication_factor: u16,
        epoch: Epoch,
        min_extent_capacity: u32,
        max_extent_capacity: u32,
        cache_extents: u32,
        extent_growth_factor: u32,
    ) {
        for (i, addr) in secondary_addrs.iter().enumerate() {
            let role = (i + 1) as u8; // 1, 2, ...
            let addr = addr.clone();
            let sid = stream_id;
            let eid = extent_id;
            let rf = replication_factor;
            let ep = epoch;
            let minc = min_extent_capacity;
            let maxc = max_extent_capacity;
            let ce = cache_extents;
            let gf = extent_growth_factor;

            tokio::spawn(async move {
                let payload = build_register_extent_payload(&[]); // secondaries get no downstream addrs
                match client::StreamClient::connect(&addr).await {
                    Ok(client) => {
                        let result = client
                            .send_frame(Frame::new(
                                VariableHeader::RegisterExtent {
                                    request_id: 0,
                                    stream_id: sid,
                                    extent_id: eid,
                                    role,
                                    replication_factor: rf,
                                    epoch: ep,
                                    extent_capacity: minc,
                                    min_extent_capacity: minc,
                                    max_extent_capacity: maxc,
                                    cache_extents: ce,
                                    extent_growth_factor: gf,
                                },
                                Some(payload),
                            ))
                            .await;
                        match result {
                            Ok(resp) if resp.is_error_response() => {
                                let msg = String::from_utf8_lossy(
                                    resp.payload.as_deref().unwrap_or_default(),
                                );
                                warn!(
                                    "Secondary {addr} rejected RegisterExtent for stream={} extent={}: {msg}",
                                    sid, eid
                                );
                            }
                            Ok(_) => {
                                info!(
                                    "RegisterExtent sent to Secondary {addr}: stream={}, extent={}, role={role}",
                                    sid, eid
                                );
                            }
                            Err(e) => {
                                warn!(
                                    "RegisterExtent to Secondary {addr} failed: {e} (will create lazily on first Forward)"
                                );
                            }
                        }
                    }
                    Err(e) => {
                        warn!(
                            "connect to Secondary {addr} for RegisterExtent failed: {e} (will create lazily on first Forward)"
                        );
                    }
                }
            });
        }
    }

    /// Allocate a replica set for a new extent: pick nodes, store in DB, notify ExtentNodes.
    /// Returns (ExtentId, primary node address).
    ///
    /// `replication_factor` specifies how many replicas to create for this extent.
    /// The stream_replica table stores node *addresses* (not node IDs) so that the StreamManager
    /// can connect to ExtentNodes for seal and RegisterExtent operations.
    async fn allocate_and_notify_replica_set(
        &self,
        stream_id: StreamId,
        start_offset: u64,
        replication_factor: usize,
        epoch: Epoch,
        min_extent_capacity: u32,
        max_extent_capacity: u32,
        cache_extents: u32,
        extent_growth_factor: u32,
    ) -> Result<(ExtentId, String), StorageError> {
        // Initial allocation: require full RF.
        let nodes = self
            .allocator
            .pick_nodes(replication_factor, replication_factor)
            .await?;
        let replicas: Vec<(String, u8)> = nodes
            .iter()
            .enumerate()
            .map(|(i, n)| (n.addr.clone(), i as u8))
            .collect();

        let node_addrs: Vec<String> = replicas.iter().map(|(addr, _)| addr.clone()).collect();

        let extent_id = self
            .store
            .allocate_extent(stream_id, start_offset, &replicas, epoch)
            .await?;

        info!(
            "extent {} allocated for stream {}: replicas={:?}",
            extent_id, stream_id, node_addrs
        );

        // Notify ExtentNodes of their replication roles.
        // Always send RegisterExtent, even for replication_factor=1, so the ExtentNode knows
        // the StreamManager-assigned stream_id and extent_id (required for seal coordination).
        let primary_addr = &node_addrs[0];
        let secondary_addrs: Vec<&str> = node_addrs[1..].iter().map(|s| s.as_str()).collect();
        let rf = node_addrs.len() as u16;

        self.register_primary(stream_id, extent_id, primary_addr, &secondary_addrs, rf, epoch, min_extent_capacity, max_extent_capacity, cache_extents, extent_growth_factor)
            .await
            .unwrap_or_else(|e| {
                warn!("register_primary failed for initial extent {}: {e}; client will discover on first append", extent_id);
            });
        self.notify_secondaries(
            stream_id,
            extent_id,
            &node_addrs[1..],
            rf,
            epoch,
            min_extent_capacity,
            max_extent_capacity,
            cache_extents,
            extent_growth_factor,
        );

        Ok((extent_id, node_addrs[0].clone()))
    }
}

impl RequestHandler for StreamManagerStore {
    async fn handle_frame(
        &self,
        frame: Frame,
        _response_tx: Option<&tokio::sync::mpsc::Sender<Frame>>,
    ) -> Option<Frame> {
        match frame.opcode() {
            // Fire-and-forget: no response frame
            Opcode::UpdateExtent => {
                self.handle_extent_update(frame).await;
                None
            }
            // Request-response opcodes
            _ => {
                let response = match frame.opcode() {
                    Opcode::Connect => self.handle_connect(frame).await,
                    Opcode::Heartbeat => self.handle_heartbeat(frame).await,
                    Opcode::Disconnect => self.handle_disconnect(frame).await,
                    Opcode::CreateStream => self.handle_create_stream(frame).await,
                    Opcode::SealStreamManager => self.handle_seal_stream_manager(frame).await,
                    Opcode::QueryOffset => self.handle_query_offset(frame).await,
                    Opcode::DescribeStream => self.handle_describe_stream(frame).await,
                    Opcode::DescribeExtent => self.handle_describe_extent(frame).await,
                    Opcode::Seek => self.handle_seek(frame).await,
                    Opcode::ReportExtents => self.handle_report_extents(frame).await,
                    Opcode::ConnectAck
                    | Opcode::DisconnectAck
                    | Opcode::RegisterExtent
                    | Opcode::RegisterExtentAck
                    | Opcode::Watermark
                    | Opcode::UpdateExtent
                    | Opcode::ReportExtentsResp
                    | Opcode::CreateStreamResp
                    | Opcode::QueryOffsetResp
                    | Opcode::ReadResp
                    | Opcode::SealExtentNode
                    | Opcode::DescribeStreamResp
                    | Opcode::DescribeExtentResp
                    | Opcode::SeekResp
                    | Opcode::StreamManagerMembershipChange => {
                        warn!(opcode = ?frame.opcode(), "SM received unexpected response/fire-and-forget opcode");
                        return None;
                    }
                    _ => Frame::error_from_request(
                        &frame,
                        ErrorCode::InternalError,
                        &format!("StreamManager: unsupported opcode {:?}", frame.opcode()),
                        ExtentId(0),
                    ),
                };
                Some(response)
            }
        }
    }
}

impl StreamManagerStore {
    /// ExtentNode Connect: register node. Payload = [node_id_len:u16][node_id][addr_len:u16][addr][interval_ms:u32]
    async fn handle_connect(&self, frame: Frame) -> Frame {
        let payload = frame.payload.as_deref().unwrap_or_default();
        match parse_connect_payload(payload) {
            Some((node_id, addr, interval_ms)) => {
                match self.store.register_node(&node_id, &addr, interval_ms).await {
                    Ok(()) => {
                        info!(
                            "ExtentNode registered: node_id={node_id}, addr={addr}, interval={interval_ms}ms"
                        );
                        Frame::new(
                            VariableHeader::ConnectAck {
                                request_id: frame.request_id(),
                            },
                            None,
                        )
                    }
                    Err(e) => {
                        error!("register_node failed: {e}");
                        Frame::error_from_request(
                            &frame,
                            ErrorCode::InternalError,
                            &e.to_string(),
                            ExtentId(0),
                        )
                    }
                }
            }
            None => Frame::error_from_request(
                &frame,
                ErrorCode::InternalError,
                "invalid Connect payload",
                ExtentId(0),
            ),
        }
    }

    /// ExtentNode Heartbeat: update heartbeat timestamp and cache runtime metrics.
    /// Payload = [node_id_len:u16][node_id][metrics:32 bytes]
    async fn handle_heartbeat(&self, frame: Frame) -> Frame {
        let payload = frame.payload.as_deref().unwrap_or_default();
        match parse_heartbeat_payload(payload) {
            Some((node_id, metrics)) => {
                match self.store.update_heartbeat(&node_id).await {
                    Ok(()) => {
                        // Persist metrics to DB for load-aware placement (graceful on failure).
                        if let Err(e) = self.store.persist_node_metrics(&node_id, &metrics).await {
                            warn!("failed to persist node metrics for {node_id}: {e}");
                        }
                        Frame::new(
                            VariableHeader::Heartbeat {
                                request_id: frame.request_id(),
                            },
                            None,
                        )
                    }
                    Err(e) => {
                        error!("update_heartbeat failed: {e}");
                        Frame::error_from_request(
                            &frame,
                            ErrorCode::InternalError,
                            &e.to_string(),
                            ExtentId(0),
                        )
                    }
                }
            }
            None => Frame::error_from_request(
                &frame,
                ErrorCode::InternalError,
                "invalid Heartbeat payload",
                ExtentId(0),
            ),
        }
    }

    /// ExtentNode Disconnect: mark node as dead, clean up metrics, stop allocating.
    /// Payload = [node_id_len:u16][node_id]
    async fn handle_disconnect(&self, frame: Frame) -> Frame {
        let payload = frame.payload.as_deref().unwrap_or_default();
        match parse_string_payload(payload) {
            Some(node_id) => match self.store.mark_node_dead(&node_id).await {
                Ok(()) => {
                    info!("ExtentNode disconnected: node_id={node_id}");
                    Frame::new(
                        VariableHeader::DisconnectAck {
                            request_id: frame.request_id(),
                        },
                        None,
                    )
                }
                Err(e) => {
                    error!("mark_node_dead on disconnect failed: {e}");
                    Frame::error_from_request(
                        &frame,
                        ErrorCode::InternalError,
                        &e.to_string(),
                        ExtentId(0),
                    )
                }
            },
            None => Frame::error_from_request(
                &frame,
                ErrorCode::InternalError,
                "invalid Disconnect payload",
                ExtentId(0),
            ),
        }
    }

    /// CreateStream: create stream in metadata, allocate initial extent replica set, notify ExtentNodes.
    ///
    /// Variable header carries stream_name, replication_factor, and extent_capacity.
    /// If replication_factor=0, the server's default_replication_factor is used.
    /// If min_extent_capacity=0, the default of 8 MiB is used.
    /// If max_extent_capacity=0, the default of 256 MiB is used.
    ///
    /// Response variable header carries primary_addr.
    async fn handle_create_stream(&self, frame: Frame) -> Frame {
        let (
            stream_name,
            replication_factor,
            min_extent_capacity,
            max_extent_capacity,
            cache_extents,
            extent_growth_factor,
        ) = match &frame.variable_header {
            VariableHeader::CreateStream {
                stream_name,
                replication_factor,
                min_extent_capacity,
                max_extent_capacity,
                cache_extents,
                extent_growth_factor,
                ..
            } => (
                String::from_utf8_lossy(stream_name).to_string(),
                *replication_factor,
                *min_extent_capacity,
                *max_extent_capacity,
                *cache_extents,
                *extent_growth_factor,
            ),
            _ => {
                return Frame::error_from_request(
                    &frame,
                    ErrorCode::InternalError,
                    "invalid CreateStream frame",
                    ExtentId(0),
                );
            }
        };

        // Use server default if client sends replication_factor=0.
        let replication_factor = if replication_factor == 0 {
            self.default_replication_factor
        } else {
            replication_factor as usize
        };

        // Use defaults if client sends 0 values.
        let min_extent_capacity = if min_extent_capacity == 0 {
            DEFAULT_MIN_EXTENT_CAPACITY
        } else {
            min_extent_capacity
        };

        let max_extent_capacity = if max_extent_capacity == 0 {
            DEFAULT_MAX_EXTENT_CAPACITY
        } else {
            max_extent_capacity
        };

        // Use default 4 if client sends cache_extents=0.
        let cache_extents = if cache_extents == 0 {
            DEFAULT_CACHE_EXTENTS
        } else {
            cache_extents
        };

        // Use default growth factor if client sends 0.
        let extent_growth_factor = if extent_growth_factor == 0 {
            DEFAULT_EXTENT_GROWTH_FACTOR
        } else {
            extent_growth_factor
        };

        let result = async {
            // 1. Create stream in metadata with per-stream replication factor and extent capacity.
            let stream_id = self.store.create_stream(&stream_name, "DATA", replication_factor as u16, min_extent_capacity, min_extent_capacity, max_extent_capacity, cache_extents, extent_growth_factor).await?;

            // 2. Allocate first extent replica set and notify ExtentNodes.
            let (extent_id, primary_addr) =
                self.allocate_and_notify_replica_set(stream_id, 0, replication_factor, Epoch(0), min_extent_capacity, max_extent_capacity, cache_extents, extent_growth_factor).await?;

            info!(
                "stream {stream_name} created: stream_id={}, extent_id={}, primary={primary_addr}, min_extent_capacity={min_extent_capacity}, max_extent_capacity={max_extent_capacity}, cache_extents={cache_extents}, extent_growth_factor={extent_growth_factor}",
                stream_id, extent_id
            );

            Ok::<(StreamId, ExtentId, String), StorageError>((stream_id, extent_id, primary_addr))
        }
        .await;

        match result {
            Ok((stream_id, extent_id, primary_addr)) => Frame::new(
                VariableHeader::CreateStreamResp {
                    request_id: frame.request_id(),
                    stream_id,
                    extent_id,
                    epoch: Epoch(0),
                    primary_addr: Bytes::from(primary_addr),
                },
                None,
            ),
            Err(e) => {
                error!("create_stream failed: {e}");
                Frame::error_from_request(
                    &frame,
                    ErrorCode::InternalError,
                    &e.to_string(),
                    ExtentId(0),
                )
            }
        }
    }

    /// Seal an extent for a stream and allocate a new replica set.
    ///
    /// Receives SealStreamManagerRequest(stream_id, epoch) from the client.
    /// SM forwards seal to the Primary of that epoch and allocates a new extent.
    async fn handle_seal_stream_manager(&self, frame: Frame) -> Frame {
        let (request_id, stream_id, epoch) = match &frame.variable_header {
            VariableHeader::SealStreamManagerRequest {
                request_id,
                stream_id,
                epoch,
            } => (*request_id, *stream_id, *epoch),
            _ => {
                return Frame::seal_stream_manager_resp_error(
                    frame.request_id(),
                    frame.stream_id(),
                    ErrorCode::InternalError,
                    "invalid SealStreamManagerRequest frame",
                );
            }
        };

        self.handle_epoch_seal(request_id, stream_id, epoch).await
    }

    /// Seal an extent and allocate a new one.
    ///
    /// The new seal protocol sends SealExtentNodeRequest to ENs, which seal the
    /// last mutable extent and return SealExtentNodeResp with extent info.
    ///
    /// **Phase 1 — Seal primary, obtain committed offset.**
    /// - EN-initiated (extent-full): Primary already sealed locally and provides
    ///   `committed_offset=Some(offset)`. Phase 1 is done.
    /// - Client-initiated: `committed_offset=None`. `resolve_committed_offset`
    ///   seals only the primary with a short timeout (100ms).
    ///
    /// **Phase 2 — Seal all replicas (fire-and-forget).**
    /// After `seal_allocate_register`, all replicas are sealed. This is idempotent.
    async fn seal_extent(
        &self,
        stream_id: StreamId,
        extent_id: ExtentId,
        committed_offset: Option<u64>,
        epoch: Epoch,
    ) -> Result<(ExtentId, String), StorageError> {
        // Get extent metadata early — needed for start_offset in seal RPCs.
        let extent_row = self
            .store
            .get_active_extent(stream_id)
            .await?
            .ok_or_else(|| {
                StorageError::Internal(format!(
                    "no active extent found for stream {} during seal_extent",
                    stream_id
                ))
            })?;
        let extent_start_offset = extent_row.start_offset;

        let end_offset = match committed_offset {
            Some(offset) => {
                info!(
                    "seal_extent: offset provided for extent {} stream {}: offset={offset}",
                    extent_id, stream_id
                );
                offset
            }
            None => {
                // Query all EN replicas to determine committed offset via quorum.
                // Use the extent's original epoch for replica lookup (not the bumped epoch).
                self.resolve_committed_offset(
                    stream_id,
                    extent_id,
                    extent_start_offset,
                    extent_row.epoch,
                )
                .await?
            }
        };

        // Seal + allocate + notify new replica set.
        let result = self
            .seal_allocate_register(stream_id, extent_id, end_offset, epoch)
            .await?;

        // Fire-and-forget seal to all replicas (safety net).
        // Look up replicas using the extent's epoch (not the new bumped epoch).
        {
            let replicas = self
                .store
                .get_replicas(stream_id, extent_row.epoch)
                .await
                .unwrap_or_default();
            let addrs: Vec<String> = replicas.into_iter().map(|r| r.node_addr).collect();

            if !addrs.is_empty() {
                let sid = stream_id;
                let eid = extent_id;
                let ep = extent_row.epoch;
                let so = extent_start_offset;
                tokio::spawn(async move {
                    for addr in addrs {
                        match seal_extent_node_static(&addr, sid, ep, eid, so).await {
                            Ok((_sealed_eid, _start, end, _payload)) => {
                                info!("fire-and-forget seal to {addr}: end_offset={end}");
                            }
                            Err(e) => {
                                warn!("fire-and-forget seal to {addr} failed: {e}");
                            }
                        }
                    }
                });
            }
        }

        Ok(result)
    }

    /// Resolve the committed offset for a client-initiated seal.
    ///
    /// **Phase 1** — Seal primary with a short timeout (100ms). If the primary is
    /// alive (common case), it returns its committed offset immediately.
    ///
    /// **Fallback** — If the primary is unreachable (timeout/error), seal ALL
    /// replicas concurrently and compute the committed offset from secondary quorum.
    pub async fn resolve_committed_offset(
        &self,
        stream_id: StreamId,
        extent_id: ExtentId,
        start_offset: u64,
        epoch: Epoch,
    ) -> Result<u64, StorageError> {
        let replicas = self.store.get_replicas(stream_id, epoch).await?;
        if replicas.is_empty() {
            return Err(StorageError::Internal(format!(
                "no replicas found for stream {} extent {}",
                stream_id, extent_id
            )));
        }

        // Partition replicas into primary and secondaries.
        let mut primary_addr: Option<String> = None;
        let mut secondary_replicas: Vec<(String, u8)> = Vec::new();
        for replica in &replicas {
            if replica.role == 0 {
                primary_addr = Some(replica.node_addr.clone());
            } else {
                secondary_replicas.push((replica.node_addr.clone(), replica.role));
            }
        }

        // Phase 1: Seal primary with short timeout (100ms).
        if let Some(ref addr) = primary_addr {
            let addr = addr.clone();
            let sid = stream_id;
            let ep = epoch;
            let eid = extent_id;
            let so = start_offset;
            match tokio::time::timeout(
                Duration::from_millis(100),
                seal_extent_node_static(&addr, sid, ep, eid, so),
            )
            .await
            {
                Ok(Ok((_sealed_eid, _start, end_offset, _payload))) => {
                    info!(
                        "Primary {addr} reports committed offset {end_offset} for stream {} (fast path)",
                        stream_id
                    );
                    return Ok(end_offset);
                }
                Ok(Err(e)) => {
                    warn!("Primary {addr} seal failed for stream {}: {e}", stream_id);
                }
                Err(_) => {
                    warn!(
                        "Primary {addr} seal timed out (100ms) for stream {}, falling back to secondary quorum",
                        stream_id
                    );
                }
            }
        } else {
            warn!(
                "No primary replica found for stream {} extent {}",
                stream_id, extent_id
            );
        }

        // Fallback: primary unreachable — seal ALL replicas concurrently.
        let rf = replicas.len() as u16;
        let required_secondary_acks = (rf as u32) / 2;

        let mut seal_futures = Vec::new();
        for replica in &replicas {
            let addr = replica.node_addr.clone();
            let role = replica.role;
            let sid = stream_id;
            let ep = epoch;
            let eid = extent_id;
            let so = start_offset;
            seal_futures.push(async move {
                let result = seal_extent_node_static(&addr, sid, ep, eid, so).await;
                (addr, role, result)
            });
        }
        let seal_results = future::join_all(seal_futures).await;

        // Determine committed offset from responses.
        let mut primary_offset: Option<u64> = None;
        let mut secondary_offsets: Vec<u64> = Vec::new();

        for (addr, role, result) in &seal_results {
            match result {
                Ok((_sealed_eid, _start, end_offset, payload)) => {
                    if *role == 0 {
                        info!(
                            "Primary {addr} reports committed offset {end_offset} for stream {:?} (fallback)",
                            stream_id
                        );
                        primary_offset = Some(*end_offset);
                    } else {
                        info!(
                            "Secondary {addr} reports offset {end_offset} for stream {:?}",
                            stream_id
                        );
                        secondary_offsets.push(*end_offset);
                    }
                    // Reconcile predecessor extents from the SealExtentNodeResp payload.
                    if let Some(payload) = payload {
                        if let Some(extents) = parse_seal_predecessor_payload(payload) {
                            if !extents.is_empty() {
                                info!(
                                    "Reconciling {} predecessor extents from {addr} for stream {stream_id}",
                                    extents.len()
                                );
                                let _ = self
                                    .store
                                    .reconcile_extents(stream_id, epoch, &extents)
                                    .await;
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to seal ExtentNode {addr} (role={role}): {e}");
                }
            }
        }

        let committed = if let Some(offset) = primary_offset {
            offset
        } else {
            // Primary completely unreachable: compute from secondary quorum.
            if (secondary_offsets.len() as u32) < required_secondary_acks {
                return Err(StorageError::Internal(format!(
                    "insufficient replicas for seal: need {} secondary ACKs, got {}",
                    required_secondary_acks,
                    secondary_offsets.len()
                )));
            }
            if secondary_offsets.is_empty() {
                return Err(StorageError::Internal(
                    "no ExtentNodes responded to seal".into(),
                ));
            }
            // Take kth largest, where k = required_secondary_acks.
            secondary_offsets.sort_unstable_by(|a, b| b.cmp(a));
            let k = required_secondary_acks as usize;
            if k == 0 {
                secondary_offsets[0]
            } else if k <= secondary_offsets.len() {
                secondary_offsets[k - 1]
            } else {
                return Err(StorageError::Internal(format!(
                    "insufficient secondary offsets for quorum: need {k}, have {}",
                    secondary_offsets.len()
                )));
            }
        };

        info!(
            "seal_extent: resolved committed offset for extent {} stream {}: committed={committed}",
            extent_id, stream_id
        );
        Ok(committed)
    }

    /// Shared logic for both seal paths: pick new nodes, seal-and-allocate in DB,
    /// register new replica set, and return (new_extent_id, primary_addr).
    pub async fn seal_allocate_register(
        &self,
        stream_id: StreamId,
        extent_id: ExtentId,
        end_offset: u64,
        epoch: Epoch,
    ) -> Result<(ExtentId, String), StorageError> {
        // Pick nodes for new extent replica set using per-stream replication factor.
        let replication_factor =
            self.store.get_stream_replication_factor(stream_id).await? as usize;
        let _extent_capacity = self.store.get_stream_extent_capacity(stream_id).await?;
        let cache_extents = self.store.get_stream_cache_extents(stream_id).await?;
        let (min_extent_capacity, max_extent_capacity) =
            self.store.get_stream_capacity_bounds(stream_id).await?;
        let extent_growth_factor = self.store.get_stream_growth_factor(stream_id).await?;
        // Failover allocation: degrade RF if necessary, as long as quorum is preserved.
        // Quorum = floor(RF/2) + 1. E.g., RF=3 → quorum=2, so degraded RF=2 is acceptable.
        let quorum = replication_factor / 2 + 1;
        let nodes = self
            .allocator
            .pick_nodes(replication_factor, quorum)
            .await?;

        let new_replicas: Vec<(String, u8)> = nodes
            .iter()
            .enumerate()
            .map(|(i, n)| (n.addr.clone(), i as u8))
            .collect();

        // Transactional seal + allocate (idempotent for already-sealed extents).
        let seal_result = self
            .store
            .seal_and_allocate_transaction(stream_id, extent_id, end_offset, &new_replicas, epoch)
            .await?;

        let (new_extent_id, primary_addr) = match seal_result {
            SealResult::Sealed { new_extent_id } => {
                let primary_addr = new_replicas[0].0.clone();
                let node_addrs: Vec<String> = new_replicas.iter().map(|(a, _)| a.clone()).collect();
                let secondary_addrs: Vec<&str> =
                    node_addrs[1..].iter().map(|s| s.as_str()).collect();
                let rf = node_addrs.len() as u16;

                info!(
                    "new extent {} allocated for stream {}, primary={primary_addr}",
                    new_extent_id, stream_id
                );

                // Register new extent: best-effort. If Primary is dead/slow,
                // client will discover on first append and trigger another seal-and-new.
                if let Err(e) = self
                    .register_primary(
                        stream_id,
                        new_extent_id,
                        &primary_addr,
                        &secondary_addrs,
                        rf,
                        epoch,
                        min_extent_capacity,
                        max_extent_capacity,
                        cache_extents,
                        extent_growth_factor,
                    )
                    .await
                {
                    warn!(
                        "register_primary failed for extent {:?}: {e}; client will discover on first append",
                        new_extent_id
                    );
                }

                // notify extent secondary nodes in fire-and-forget way
                self.notify_secondaries(
                    stream_id,
                    new_extent_id,
                    &node_addrs[1..],
                    rf,
                    epoch,
                    min_extent_capacity,
                    max_extent_capacity,
                    cache_extents,
                    extent_growth_factor,
                );

                (new_extent_id, primary_addr)
            }
            SealResult::AlreadySealed {
                new_extent_id,
                new_start_offset: _,
                primary_addr,
            } => {
                info!(
                    "extent {} already sealed for stream {}; returning successor {}",
                    extent_id, stream_id, new_extent_id
                );
                (new_extent_id, primary_addr)
            }
        };

        Ok((new_extent_id, primary_addr))
    }

    /// QueryOffset on StreamManager: return the total logical end offset for a stream.
    async fn handle_query_offset(&self, frame: Frame) -> Frame {
        let stream_id = frame.stream_id();

        let result = async {
            let extents = self.store.get_extents(stream_id).await?;
            if extents.is_empty() {
                return Ok(Offset(0));
            }
            let last = &extents[extents.len() - 1];
            Ok::<Offset, StorageError>(Offset(last.end_offset))
        }
        .await;

        match result {
            Ok(offset) => Frame::new(
                VariableHeader::QueryOffsetResp {
                    request_id: frame.request_id(),
                    stream_id,
                    offset,
                },
                None,
            ),
            Err(e) => {
                error!("query_offset failed: {e}");
                Frame::error_from_request(
                    &frame,
                    ErrorCode::InternalError,
                    &e.to_string(),
                    ExtentId(0),
                )
            }
        }
    }

    // ── Management API handlers ──

    /// DescribeStream: return extent metadata with replica info and node liveness.
    ///
    /// When FLAG_DESCRIBE_STREAM_BY_NAME is set, resolves stream_name from the variable
    /// header to a StreamId first. Otherwise uses stream_id from the header directly.
    ///
    /// Response: DescribeStreamResp with encoded Vec<ExtentInfo>
    async fn handle_describe_stream(&self, frame: Frame) -> Frame {
        let count = frame.count();

        // Resolve stream_id: by name if FLAG_DESCRIBE_STREAM_BY_NAME, else from header.
        let stream_id = if let VariableHeader::DescribeStream {
            stream_name: Some(ref name),
            ..
        } = frame.variable_header
        {
            let name_str = String::from_utf8_lossy(name);
            match self.store.get_stream_by_name(&name_str).await {
                Ok(Some(row)) => row.stream_id,
                Ok(None) => {
                    return Frame::error_from_request(
                        &frame,
                        ErrorCode::UnknownStream,
                        &format!("stream not found: {name_str}"),
                        ExtentId(0),
                    );
                }
                Err(e) => {
                    error!("get_stream_by_name failed: {e}");
                    return Frame::error_from_request(
                        &frame,
                        ErrorCode::InternalError,
                        &e.to_string(),
                        ExtentId(0),
                    );
                }
            }
        } else {
            frame.stream_id()
        };

        match self.store.describe_stream_extents(stream_id, count).await {
            Ok(extents) => {
                let payload = encode_extent_info_vec(&extents);
                Frame::new(
                    VariableHeader::DescribeStreamResp {
                        request_id: frame.request_id(),
                        stream_id,
                    },
                    Some(payload),
                )
            }
            Err(e) => {
                error!("describe_stream failed: {e}");
                Frame::error_from_request(
                    &frame,
                    ErrorCode::InternalError,
                    &e.to_string(),
                    ExtentId(0),
                )
            }
        }
    }

    /// DescribeExtent: return a single extent's metadata with replica info and node liveness.
    ///
    /// Payload: [extent_id:u32]
    /// Response: DescribeExtentResp with encoded Vec<ExtentInfo> (length 1)
    async fn handle_describe_extent(&self, frame: Frame) -> Frame {
        let stream_id = frame.stream_id();
        let extent_id = frame.extent_id();

        match self.store.describe_extent(stream_id, extent_id).await {
            Ok(Some(info)) => {
                let payload = encode_extent_info_vec(&[info]);
                Frame::new(
                    VariableHeader::DescribeExtentResp {
                        request_id: frame.request_id(),
                        stream_id,
                    },
                    Some(payload),
                )
            }
            Ok(None) => Frame::error_from_request(
                &frame,
                ErrorCode::UnknownStream,
                &format!(
                    "extent not found: stream={}, extent={}",
                    stream_id, extent_id
                ),
                ExtentId(0),
            ),
            Err(e) => {
                error!("describe_extent failed: {e}");
                Frame::error_from_request(
                    &frame,
                    ErrorCode::InternalError,
                    &e.to_string(),
                    ExtentId(0),
                )
            }
        }
    }

    /// Seek: resolve a logical offset to the extent that contains it.
    ///
    /// Uses the frame header's offset field as the target offset (no payload needed).
    /// Response: SeekResp with encoded Vec<ExtentInfo> (length 1).
    async fn handle_seek(&self, frame: Frame) -> Frame {
        let stream_id = frame.stream_id();
        let offset = frame.offset().0;

        match self.store.seek_extent(stream_id, offset).await {
            Ok(Some(info)) => {
                let payload = encode_extent_info_vec(&[info]);
                Frame::new(
                    VariableHeader::SeekResp {
                        request_id: frame.request_id(),
                        stream_id,
                        offset: Offset(offset),
                    },
                    Some(payload),
                )
            }
            Ok(None) => Frame::error_from_request(
                &frame,
                ErrorCode::InvalidOffset,
                &format!(
                    "no extent contains offset {} for stream {:?}",
                    offset, stream_id
                ),
                ExtentId(0),
            ),
            Err(e) => {
                error!("seek failed: {e}");
                Frame::error_from_request(
                    &frame,
                    ErrorCode::InternalError,
                    &e.to_string(),
                    ExtentId(0),
                )
            }
        }
    }

    /// Handle epoch-based seal: SM forwards seal to the Primary of the given epoch.
    ///
    /// Flow:
    /// 1. Look up the Primary EN for the active extent at this epoch.
    /// 2. Forward SealExtentNodeRequest to the Primary — it knows the ground truth.
    /// 3. Primary seals its active extent and responds with SealExtentNodeResp.
    /// 4. SM reconciles metadata, bumps epoch, allocates new extent on new replica set.
    /// 5. SM responds to client with SealStreamManagerResp (new epoch/extent info).
    async fn handle_epoch_seal(&self, request_id: u32, stream_id: StreamId, epoch: Epoch) -> Frame {
        info!("Epoch-based seal: stream={}, epoch={}", stream_id, epoch);

        // Check if the client's epoch is stale. If the stream has already advanced
        // past the requested epoch, return the current epoch's primary info directly
        // — no seal needed.
        let current_epoch = match self.store.get_stream_epoch(stream_id).await {
            Ok(e) => e,
            Err(e) => {
                return Frame::seal_stream_manager_resp_error(
                    request_id,
                    stream_id,
                    ErrorCode::InternalError,
                    &format!("get_stream_epoch: {e}"),
                );
            }
        };
        if epoch.0 < current_epoch.0 {
            info!(
                "Epoch seal: client epoch {} is stale (current={}), returning current state",
                epoch, current_epoch
            );
            // Return the current epoch's primary and let the client reconnect.
            let replicas = match self.store.get_replicas(stream_id, current_epoch).await {
                Ok(r) => r,
                Err(e) => {
                    return Frame::seal_stream_manager_resp_error(
                        request_id,
                        stream_id,
                        ErrorCode::InternalError,
                        &format!("get_replicas for current epoch: {e}"),
                    );
                }
            };
            let primary_addr = replicas
                .iter()
                .find(|r| r.role == 0)
                .map(|r| r.node_addr.clone())
                .unwrap_or_default();
            return Frame::new(
                VariableHeader::SealStreamManagerResp {
                    request_id,
                    stream_id,
                    offset: Offset(0),
                    new_epoch: current_epoch,
                    primary_addr: Bytes::copy_from_slice(primary_addr.as_bytes()),
                },
                None,
            );
        }

        // Get the active extent to find the Primary's address.
        let active = match self.store.get_active_extent(stream_id).await {
            Ok(Some(ext)) => ext,
            Ok(None) => {
                return Frame::seal_stream_manager_resp_error(
                    request_id,
                    stream_id,
                    ErrorCode::InternalError,
                    "no active extent for epoch seal",
                );
            }
            Err(e) => {
                return Frame::seal_stream_manager_resp_error(
                    request_id,
                    stream_id,
                    ErrorCode::InternalError,
                    &format!("get_active_extent: {e}"),
                );
            }
        };

        let replicas = match self.store.get_replicas(stream_id, active.epoch).await {
            Ok(r) => r,
            Err(e) => {
                return Frame::seal_stream_manager_resp_error(
                    request_id,
                    stream_id,
                    ErrorCode::InternalError,
                    &format!("get_replicas: {e}"),
                );
            }
        };

        let primary_addr = replicas
            .iter()
            .find(|r| r.role == 0)
            .map(|r| r.node_addr.clone())
            .unwrap_or_default();

        if primary_addr.is_empty() {
            return Frame::seal_stream_manager_resp_error(
                request_id,
                stream_id,
                ErrorCode::InternalError,
                "no primary replica found for epoch seal",
            );
        }

        // Forward seal to the Primary EN.
        let end_offset = match seal_extent_node_static(
            &primary_addr,
            stream_id,
            active.epoch,
            active.extent_id,
            active.start_offset,
        )
        .await
        {
            Ok((_sealed_eid, _start, end, payload)) => {
                // Reconcile predecessor extents from the primary's SealExtentNodeResp.
                if let Some(ref payload) = payload {
                    if let Some(extents) = parse_seal_predecessor_payload(payload) {
                        if !extents.is_empty() {
                            info!(
                                "Epoch seal: reconciling {} predecessor extents from primary for stream {}",
                                extents.len(),
                                stream_id
                            );
                            let _ = self
                                .store
                                .reconcile_extents(stream_id, active.epoch, &extents)
                                .await;
                        }
                    }
                }
                end
            }
            Err(e) => {
                // Primary unreachable — fall back to quorum seal.
                // resolve_committed_offset sends SealExtentNode to all replicas;
                // their SealExtentNodeResp payloads carry predecessor extents which
                // are reconciled inline — no separate report_extents needed.
                warn!(
                    "Epoch seal: primary unreachable at {primary_addr}, falling back to quorum seal: {e}"
                );

                // Re-read the active extent — it may have changed after reconciliation.
                let active = match self.store.get_active_extent(stream_id).await {
                    Ok(Some(ext)) => ext,
                    Ok(None) => {
                        return Frame::seal_stream_manager_resp_error(
                            request_id,
                            stream_id,
                            ErrorCode::InternalError,
                            "no active extent after reconciliation",
                        );
                    }
                    Err(e) => {
                        return Frame::seal_stream_manager_resp_error(
                            request_id,
                            stream_id,
                            ErrorCode::InternalError,
                            &format!("get_active_extent after reconciliation: {e}"),
                        );
                    }
                };

                // Step 3: Bump epoch and seal the (possibly new) active extent.
                let new_epoch = match self.store.bump_epoch(stream_id).await {
                    Ok(e) => e,
                    Err(e) => {
                        return Frame::seal_stream_manager_resp_error(
                            request_id,
                            stream_id,
                            ErrorCode::InternalError,
                            &format!("epoch seal fallback: bump_epoch failed: {e}"),
                        );
                    }
                };
                info!(
                    "Epoch seal: reconciled, sealing extent {} at new epoch {} for stream {}",
                    active.extent_id, new_epoch, stream_id
                );
                match self
                    .seal_extent(stream_id, active.extent_id, None, new_epoch)
                    .await
                {
                    Ok((_new_extent_id, new_primary_addr)) => {
                        return Frame::new(
                            VariableHeader::SealStreamManagerResp {
                                request_id,
                                stream_id,
                                offset: Offset(0),
                                new_epoch,
                                primary_addr: Bytes::copy_from_slice(new_primary_addr.as_bytes()),
                            },
                            None,
                        );
                    }
                    Err(e2) => {
                        return Frame::seal_stream_manager_resp_error(
                            request_id,
                            stream_id,
                            ErrorCode::InternalError,
                            &format!("epoch seal fallback failed: {e2}"),
                        );
                    }
                }
            }
        };

        // Reconciliation already happened from the SealExtentNodeResp payload above.
        // Re-read the active extent — it may have advanced after reconciliation.
        // If no active extent remains (primary already sealed the last one),
        // use the sealed extent from the SealExtentNodeResp.
        let sealed_extent_id = match self.store.get_active_extent(stream_id).await {
            Ok(Some(ext)) => ext.extent_id,
            Ok(None) => {
                // Primary already sealed the last extent. Ensure it's sealed in DB too.
                let _ = self
                    .store
                    .seal_extent(stream_id, active.extent_id, end_offset)
                    .await;
                active.extent_id
            }
            Err(e) => {
                return Frame::seal_stream_manager_resp_error(
                    request_id,
                    stream_id,
                    ErrorCode::InternalError,
                    &format!("get_active_extent after reconciliation: {e}"),
                );
            }
        };

        // Bump epoch BEFORE seal+allocate so the new extent is created at the new epoch.
        let new_epoch = match self.store.bump_epoch(stream_id).await {
            Ok(e) => e,
            Err(e) => {
                error!("epoch seal: bump_epoch failed: {e}");
                return Frame::seal_stream_manager_resp_error(
                    request_id,
                    stream_id,
                    ErrorCode::InternalError,
                    &format!("epoch seal: bump_epoch: {e}"),
                );
            }
        };

        // Allocate new extent. The sealed extent is already sealed on the EN (and in DB
        // after reconciliation). We just need to allocate the successor.
        match self
            .seal_allocate_register(stream_id, sealed_extent_id, end_offset, new_epoch)
            .await
        {
            Ok((_new_extent_id, new_primary_addr)) => Frame::new(
                VariableHeader::SealStreamManagerResp {
                    request_id,
                    stream_id,
                    offset: Offset(end_offset),
                    new_epoch,
                    primary_addr: Bytes::copy_from_slice(new_primary_addr.as_bytes()),
                },
                None,
            ),
            Err(e) => {
                error!("epoch seal metadata update failed: {e}");
                Frame::seal_stream_manager_resp_error(
                    request_id,
                    stream_id,
                    ErrorCode::InternalError,
                    &format!("epoch seal: {e}"),
                )
            }
        }
    }

    /// UpdateExtent: fire-and-forget extent updates from Primary EN.
    ///
    /// Dispatches on variant:
    /// - Sealed: extent was sealed, insert new extent (autonomous extent creation).
    /// - Progress: periodic offset update for an active extent (observability).
    async fn handle_extent_update(&self, frame: Frame) {
        match &frame.variable_header {
            VariableHeader::UpdateExtentSealed {
                stream_id,
                epoch,
                sealed_extent_id,
                end_offset,
                new_extent_id,
                new_extent_capacity: _,
            } => {
                info!(
                    "UpdateExtentSealed: stream={}, epoch={}, sealed_extent={}, end_offset={}, new_extent={}",
                    stream_id, epoch, sealed_extent_id, end_offset.0, new_extent_id
                );
                if let Err(e) = self
                    .store
                    .record_extent_sealed(
                        *stream_id,
                        *epoch,
                        *sealed_extent_id,
                        end_offset.0,
                        *new_extent_id,
                    )
                    .await
                {
                    warn!(
                        "Failed to record extent sealed for stream {:?}: {e}",
                        stream_id
                    );
                }
            }
            VariableHeader::UpdateExtentProgress {
                stream_id,
                epoch,
                extent_id,
                current_offset,
            } => {
                if let Err(e) = self
                    .store
                    .record_extent_progress(*stream_id, *epoch, *extent_id, current_offset.0)
                    .await
                {
                    warn!(
                        "Failed to record extent progress for stream {:?}: {e}",
                        stream_id
                    );
                }
            }
            _ => {
                warn!(
                    "handle_extent_update called with unexpected header: {:?}",
                    frame.opcode()
                );
            }
        }
    }

    /// ReportExtents: SM queries an EN for all extents it holds for a stream (recovery path).
    ///
    /// Not yet implemented — returns an error response.
    async fn handle_report_extents(&self, frame: Frame) -> Frame {
        Frame::error_from_request(
            &frame,
            ErrorCode::InternalError,
            "ReportExtents not yet implemented",
            ExtentId(0),
        )
    }
}
