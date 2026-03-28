use std::sync::Arc;
use std::time::Duration;

use crate::metadata::{MetadataStore, SealResult};
use bytes::Bytes;
use common::errors::StorageError;
use common::types::{ErrorCode, ExtentId, Offset, Opcode, StreamId};
use futures_util::future;
use rpc::frame::{Frame, VariableHeader};
use rpc::payload::{
    build_register_extent_payload, build_string_payload, encode_extent_info_vec,
    parse_connect_payload, parse_create_stream_payload, parse_heartbeat_payload,
    parse_string_payload,
};
use server::handler::RequestHandler;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

use crate::allocator::Allocator;

/// Seal an ExtentNode's extent and return the committed end_offset.
///
/// This is a free function (not a method) so that it can be called from async tasks
/// spawned for concurrent sealing without borrowing `self`.
async fn seal_extent_node_static(
    addr: &str,
    stream_id: StreamId,
    extent_id: ExtentId,
    committed_offset: Option<u64>,
    start_offset: Option<u64>,
) -> Result<u64, StorageError> {
    let mut client = client::StorageClient::connect(addr).await.map_err(|e| {
        StorageError::Internal(format!("connect to ExtentNode {addr} for Seal: {e}"))
    })?;

    let resp = client
        .send_frame(Frame::new(
            VariableHeader::Seal {
                request_id: 0,
                stream_id,
                extent_id,
                offset: committed_offset.map(Offset),
                start_offset,
            },
            None,
        ))
        .await
        .map_err(|e| StorageError::Internal(format!("Seal to ExtentNode {addr}: {e}")))?;

    if resp.opcode() == Opcode::Error {
        let msg = String::from_utf8_lossy(resp.payload.as_deref().unwrap_or_default()).to_string();
        return Err(StorageError::Internal(format!(
            "ExtentNode {addr} rejected Seal: {msg}"
        )));
    }

    // SealAck carries committed end_offset in the offset field.
    Ok(resp.offset().0)
}

/// The Stream Manager's request handler.
pub struct StreamManagerStore {
    store: MetadataStore,
    allocator: Arc<Mutex<Allocator>>,
    /// Default replication factor used when a client sends replication_factor=0
    /// (meaning "use server default"). Per-stream replication factor is stored in the stream table.
    default_replication_factor: usize,
}

impl StreamManagerStore {
    pub fn new(
        store: MetadataStore,
        allocator: Arc<Mutex<Allocator>>,
        default_replication_factor: usize,
    ) -> Self {
        assert!(
            default_replication_factor >= 1,
            "default_replication_factor must be >= 1"
        );
        Self {
            store,
            allocator,
            default_replication_factor,
        }
    }

    /// Register the new extent on the Primary ExtentNode and wait for its ACK.
    ///
    /// This guarantees the Primary is ready to accept appends before any client
    /// learns about the new extent (via SealAck or DescribeStream).
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
    ) -> Result<(), StorageError> {
        let payload = build_register_extent_payload(secondary_addrs);
        let addr = primary_addr.to_string();
        let sid = stream_id;
        let eid = extent_id;
        let rf = replication_factor;

        let result = tokio::time::timeout(Duration::from_secs(1), async {
            let mut client =
                client::StorageClient::connect(&addr).await.map_err(|e| {
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
                    },
                    Some(payload),
                ))
                .await
                .map_err(|e| {
                    StorageError::Internal(format!(
                        "RegisterExtent to Primary ExtentNode {addr}: {e}"
                    ))
                })?;

            if resp.opcode() == Opcode::Error {
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
                    "RegisterExtent ACK from Primary {primary_addr}: stream={:?}, extent={:?}, rf={replication_factor}, secondaries={}",
                    stream_id, extent_id, secondary_addrs.join(", ")
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
    ) {
        for (i, addr) in secondary_addrs.iter().enumerate() {
            let role = (i + 1) as u8; // 1, 2, ...
            let addr = addr.clone();
            let sid = stream_id;
            let eid = extent_id;
            let rf = replication_factor;

            tokio::spawn(async move {
                let payload = build_register_extent_payload(&[]); // secondaries get no downstream addrs
                match client::StorageClient::connect(&addr).await {
                    Ok(mut client) => {
                        let result = client
                            .send_frame(Frame::new(
                                VariableHeader::RegisterExtent {
                                    request_id: 0,
                                    stream_id: sid,
                                    extent_id: eid,
                                    role,
                                    replication_factor: rf,
                                },
                                Some(payload),
                            ))
                            .await;
                        match result {
                            Ok(resp) if resp.opcode() == Opcode::Error => {
                                let msg = String::from_utf8_lossy(
                                    resp.payload.as_deref().unwrap_or_default(),
                                );
                                warn!(
                                    "Secondary {addr} rejected RegisterExtent for stream={:?} extent={:?}: {msg}",
                                    sid, eid
                                );
                            }
                            Ok(_) => {
                                info!(
                                    "RegisterExtent sent to Secondary {addr}: stream={:?}, extent={:?}, role={role}",
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
    /// The extent_replica table stores node *addresses* (not node IDs) so that the StreamManager
    /// can connect to ExtentNodes for seal and RegisterExtent operations.
    async fn allocate_and_notify_replica_set(
        &self,
        stream_id: StreamId,
        start_offset: u64,
        replication_factor: usize,
    ) -> Result<(ExtentId, String), StorageError> {
        let mut alloc = self.allocator.lock().await;
        let nodes = alloc.pick_nodes(&self.store, replication_factor).await?;
        drop(alloc);

        // Build (addr, role) pairs for the replica set.
        let replicas: Vec<(String, u8)> = nodes
            .iter()
            .enumerate()
            .map(|(i, n)| (n.addr.clone(), i as u8))
            .collect();

        let node_addrs: Vec<String> = replicas.iter().map(|(addr, _)| addr.clone()).collect();

        let extent_id = self
            .store
            .allocate_extent(stream_id, start_offset, &replicas)
            .await?;

        info!(
            "extent {:?} allocated for stream {:?}: replicas={:?}",
            extent_id, stream_id, node_addrs
        );

        // Notify ExtentNodes of their replication roles.
        // Always send RegisterExtent, even for replication_factor=1, so the ExtentNode knows
        // the StreamManager-assigned stream_id and extent_id (required for seal coordination).
        let primary_addr = &node_addrs[0];
        let secondary_addrs: Vec<&str> = node_addrs[1..].iter().map(|s| s.as_str()).collect();
        let rf = node_addrs.len() as u16;

        self.register_primary(stream_id, extent_id, primary_addr, &secondary_addrs, rf)
            .await
            .unwrap_or_else(|e| {
                warn!("register_primary failed for initial extent {:?}: {e}; client will discover on first append", extent_id);
            });
        self.notify_secondaries(stream_id, extent_id, &node_addrs[1..].to_vec(), rf);

        Ok((extent_id, node_addrs[0].clone()))
    }
}

impl RequestHandler for StreamManagerStore {
    async fn handle_frame(
        &self,
        frame: Frame,
        _response_tx: Option<&tokio::sync::mpsc::Sender<Frame>>,
    ) -> Option<Frame> {
        let response = match frame.opcode() {
            Opcode::Connect => self.handle_connect(frame).await,
            Opcode::Heartbeat => self.handle_heartbeat(frame).await,
            Opcode::Disconnect => self.handle_disconnect(frame).await,
            Opcode::CreateStream => self.handle_create_stream(frame).await,
            Opcode::Seal => self.handle_seal(frame).await,
            Opcode::QueryOffset => self.handle_query_offset(frame).await,
            Opcode::DescribeStream => self.handle_describe_stream(frame).await,
            Opcode::DescribeExtent => self.handle_describe_extent(frame).await,
            Opcode::Seek => self.handle_seek(frame).await,
            _ => Frame::error_response(
                frame.request_id(),
                ErrorCode::InternalError,
                &format!("StreamManager: unsupported opcode {:?}", frame.opcode()),
                ExtentId(0),
            ),
        };
        Some(response)
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
                        Frame::error_response(
                            frame.request_id(),
                            ErrorCode::InternalError,
                            &e.to_string(),
                            ExtentId(0),
                        )
                    }
                }
            }
            None => Frame::error_response(
                frame.request_id(),
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
                        // Update in-memory metrics on the allocator for load-aware placement.
                        let mut alloc = self.allocator.lock().await;
                        alloc.update_metrics(&node_id, metrics);
                        Frame::new(
                            VariableHeader::Heartbeat {
                                request_id: frame.request_id(),
                            },
                            None,
                        )
                    }
                    Err(e) => {
                        error!("update_heartbeat failed: {e}");
                        Frame::error_response(
                            frame.request_id(),
                            ErrorCode::InternalError,
                            &e.to_string(),
                            ExtentId(0),
                        )
                    }
                }
            }
            None => Frame::error_response(
                frame.request_id(),
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
            Some(node_id) => {
                match self.store.mark_node_dead(&node_id).await {
                    Ok(()) => {
                        // Clean up in-memory metrics for the disconnected node.
                        let mut alloc = self.allocator.lock().await;
                        alloc.remove_metrics(&node_id);
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
                        Frame::error_response(
                            frame.request_id(),
                            ErrorCode::InternalError,
                            &e.to_string(),
                            ExtentId(0),
                        )
                    }
                }
            }
            None => Frame::error_response(
                frame.request_id(),
                ErrorCode::InternalError,
                "invalid Disconnect payload",
                ExtentId(0),
            ),
        }
    }

    /// CreateStream: create stream in metadata, allocate initial extent replica set, notify ExtentNodes.
    /// Payload = [name_len:u16][stream_name][replication_factor:u16]
    ///
    /// If replication_factor=0 in the payload, the server's default_replication_factor is used.
    ///
    /// Response payload: [addr_len:u16][primary_addr]
    async fn handle_create_stream(&self, frame: Frame) -> Frame {
        let payload = frame.payload.as_deref().unwrap_or_default();
        let (stream_name, replication_factor) = match parse_create_stream_payload(payload) {
            Some((name, rf)) => (name, rf),
            None => {
                return Frame::error_response(
                    frame.request_id(),
                    ErrorCode::InternalError,
                    "invalid CreateStream payload: expected [name_len:u16][name][replication_factor:u16]",
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

        let result = async {
            // 1. Create stream in metadata with per-stream replication factor.
            let stream_id = self.store.create_stream(&stream_name, "DATA", replication_factor as u16).await?;

            // 2. Allocate first extent replica set and notify ExtentNodes.
            let (extent_id, primary_addr) =
                self.allocate_and_notify_replica_set(stream_id, 0, replication_factor).await?;

            info!(
                "stream {stream_name} created: stream_id={:?}, extent_id={:?}, primary={primary_addr}",
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
                },
                Some(build_string_payload(&primary_addr)),
            ),
            Err(e) => {
                error!("create_stream failed: {e}");
                Frame::error_response(
                    frame.request_id(),
                    ErrorCode::InternalError,
                    &e.to_string(),
                    ExtentId(0),
                )
            }
        }
    }

    /// Seal an extent for a stream and allocate a new replica set.
    ///
    /// Dispatches based on whether the caller provides a committed offset:
    /// - Seal variant's `offset` is `None`: SM queries all EN replicas for offset via quorum.
    /// - Seal variant's `offset` is `Some`: SM trusts offset from the caller.
    ///
    /// Both paths seal in MySQL (transaction) + allocate new extent and notify ExtentNodes.
    async fn handle_seal(&self, frame: Frame) -> Frame {
        let stream_id = frame.stream_id();
        let extent_id = frame.extent_id();
        let committed_offset = match &frame.variable_header {
            VariableHeader::Seal { offset, .. } => offset.map(|o| o.0),
            _ => None,
        };

        let result = self
            .seal_extent(stream_id, extent_id, committed_offset)
            .await;

        match result {
            Ok((new_extent_id, primary_addr)) => Frame::new(
                VariableHeader::SealAck {
                    request_id: frame.request_id(),
                    stream_id,
                    extent_id,
                    offset: Offset(0),
                    new_extent_id: Some(new_extent_id),
                    primary_addr: Some(Bytes::copy_from_slice(primary_addr.as_bytes())),
                },
                None,
            ),
            Err(e) => {
                error!("seal failed: {e}");
                Frame::error_response(
                    frame.request_id(),
                    ErrorCode::InternalError,
                    &e.to_string(),
                    ExtentId(0),
                )
            }
        }
    }

    /// Seal an extent and allocate a new one.
    ///
    /// Both seal paths follow the same two-phase protocol:
    ///
    /// **Phase 1 — Seal primary, obtain committed offset.**
    /// - EN-initiated (extent-full): Primary already sealed locally and provides
    ///   `committed_offset=Some(offset)`. Phase 1 is done.
    /// - Client-initiated: `committed_offset=None`. `resolve_committed_offset`
    ///   seals only the primary with a short timeout (100ms). Secondaries are
    ///   left unsealed so they continue accepting in-flight forwarded appends.
    ///
    /// **Phase 2 — Seal all replicas with the committed offset (fire-and-forget).**
    /// After `seal_allocate_register`, all replicas are sealed with the final
    /// committed offset. Secondaries update their `limit` so no further appends
    /// are accepted. This is idempotent — already-sealed nodes return SealAck.
    async fn seal_extent(
        &self,
        stream_id: StreamId,
        extent_id: ExtentId,
        committed_offset: Option<u64>,
    ) -> Result<(ExtentId, String), StorageError> {
        // Get extent metadata early — needed for start_offset in seal RPCs.
        let extent_row = self
            .store
            .get_active_extent(stream_id)
            .await?
            .ok_or_else(|| {
                StorageError::Internal(format!(
                    "no active extent found for stream {:?} during seal_extent",
                    stream_id
                ))
            })?;
        let extent_start_offset = extent_row.start_offset;

        let end_offset = match committed_offset {
            Some(offset) => {
                info!(
                    "seal_extent: offset provided for extent {:?} stream {:?}: offset={offset}",
                    extent_id, stream_id
                );
                offset
            }
            None => {
                // Query all EN replicas to determine committed offset via quorum.
                self.resolve_committed_offset(stream_id, extent_id, extent_start_offset).await?
            }
        };

        // Seal + allocate + notify new replica set.
        let result = self
            .seal_allocate_register(stream_id, extent_id, end_offset)
            .await?;

        // Fire-and-forget seal to all replicas with the committed offset.
        //
        // EN-initiated path: primary already sealed locally, secondaries need sealing.
        //
        // Client-initiated path: resolve_committed_offset already sealed secondaries
        // with the primary's committed offset (two-phase approach). This re-seal is
        // still useful as a safety net — idempotent, no harm if limit is already set.
        {
            let replicas = self
                .store
                .get_replicas(stream_id, extent_id)
                .await
                .unwrap_or_default();
            let addrs: Vec<String> = replicas.into_iter().map(|r| r.node_addr).collect();

            if !addrs.is_empty() {
                let sid = stream_id;
                let eid = extent_id;
                let seal_offset = end_offset;
                let so = extent_start_offset;
                tokio::spawn(async move {
                    for addr in addrs {
                        match seal_extent_node_static(&addr, sid, eid, Some(seal_offset), Some(so)).await {
                            Ok(offset) => {
                                info!("fire-and-forget seal to {addr}: offset={offset}");
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
    /// alive (common case), it returns its committed offset immediately. This is
    /// authoritative. Secondaries are NOT sealed here — they continue accepting
    /// in-flight forwarded appends. The caller (`seal_extent`) will fire-and-forget
    /// seal all replicas with the final committed offset afterwards.
    ///
    /// **Fallback** — If the primary is unreachable (timeout/error), seal ALL
    /// replicas concurrently (secondaries with `offset=None`) and compute the
    /// committed offset from secondary quorum.
    async fn resolve_committed_offset(
        &self,
        stream_id: StreamId,
        extent_id: ExtentId,
        start_offset: u64,
    ) -> Result<u64, StorageError> {
        let replicas = self.store.get_replicas(stream_id, extent_id).await?;
        if replicas.is_empty() {
            return Err(StorageError::Internal(format!(
                "no replicas found for stream {:?} extent {:?}",
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
        // Covers TCP connect + seal RPC round-trip on a healthy node.
        if let Some(ref addr) = primary_addr {
            let addr = addr.clone();
            let sid = stream_id;
            match tokio::time::timeout(
                Duration::from_millis(100),
                seal_extent_node_static(&addr, sid, extent_id, None, None),
            )
            .await
            {
                Ok(Ok(offset)) => {
                    info!(
                        "Primary {addr} reports committed offset {offset} for stream {:?} (fast path)",
                        stream_id
                    );
                    // Primary is authoritative. Secondaries are left unsealed so they
                    // continue accepting in-flight forwarded appends. The caller will
                    // fire-and-forget seal all replicas with this offset.
                    return Ok(offset);
                }
                Ok(Err(e)) => {
                    warn!("Primary {addr} seal failed for stream {:?}: {e}", stream_id);
                }
                Err(_) => {
                    warn!(
                        "Primary {addr} seal timed out (100ms) for stream {:?}, falling back to secondary quorum",
                        stream_id
                    );
                }
            }
        } else {
            warn!(
                "No primary replica found for stream {:?} extent {:?}",
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
            let eid = extent_id;
            let so = start_offset;
            seal_futures.push(async move {
                let result = seal_extent_node_static(&addr, sid, eid, None, Some(so)).await;
                (addr, role, result)
            });
        }
        let seal_results = future::join_all(seal_futures).await;

        // Determine committed offset from responses.
        let mut primary_offset: Option<u64> = None;
        let mut secondary_offsets: Vec<u64> = Vec::new();

        for (addr, role, result) in &seal_results {
            match result {
                Ok(offset) => {
                    if *role == 0 {
                        info!(
                            "Primary {addr} reports committed offset {offset} for stream {:?} (fallback)",
                            stream_id
                        );
                        primary_offset = Some(*offset);
                    } else {
                        info!(
                            "Secondary {addr} reports offset {offset} for stream {:?}",
                            stream_id
                        );
                        secondary_offsets.push(*offset);
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
            "seal_extent: resolved committed offset for extent {:?} stream {:?}: committed={committed}",
            extent_id, stream_id
        );
        Ok(committed)
    }

    /// Shared logic for both seal paths: pick new nodes, seal-and-allocate in DB,
    /// register new replica set, and return (new_extent_id, primary_addr).
    async fn seal_allocate_register(
        &self,
        stream_id: StreamId,
        extent_id: ExtentId,
        end_offset: u64,
    ) -> Result<(ExtentId, String), StorageError> {
        // Pick nodes for new extent replica set using per-stream replication factor.
        let replication_factor =
            self.store.get_stream_replication_factor(stream_id).await? as usize;
        let mut alloc = self.allocator.lock().await;
        let nodes = alloc.pick_nodes(&self.store, replication_factor).await?;
        drop(alloc);

        let new_replicas: Vec<(String, u8)> = nodes
            .iter()
            .enumerate()
            .map(|(i, n)| (n.addr.clone(), i as u8))
            .collect();

        // Transactional seal + allocate (idempotent for already-sealed extents).
        let seal_result = self
            .store
            .seal_and_allocate_transaction(stream_id, extent_id, end_offset, &new_replicas)
            .await?;

        let (new_extent_id, primary_addr) = match seal_result {
            SealResult::Sealed { new_extent_id } => {
                let primary_addr = new_replicas[0].0.clone();
                let node_addrs: Vec<String> = new_replicas.iter().map(|(a, _)| a.clone()).collect();
                let secondary_addrs: Vec<&str> = node_addrs[1..].iter().map(|s| s.as_str()).collect();
                let rf = node_addrs.len() as u16;

                info!(
                    "new extent {:?} allocated for stream {:?}, primary={primary_addr}",
                    new_extent_id, stream_id
                );

                // Register new extent: best-effort. If Primary is dead/slow,
                // client will discover on first append and trigger another seal-and-new.
                if let Err(e) = self.register_primary(stream_id, new_extent_id, &primary_addr, &secondary_addrs, rf)
                    .await
                {
                    warn!("register_primary failed for extent {:?}: {e}; client will discover on first append", new_extent_id);
                }

                // notify extent secondary nodes in fire-and-forget way
                self.notify_secondaries(stream_id, new_extent_id, &node_addrs[1..].to_vec(), rf);

                (new_extent_id, primary_addr)
            }
            SealResult::AlreadySealed {
                new_extent_id,
                new_start_offset: _,
                primary_addr,
            } => {
                info!(
                    "extent {:?} already sealed for stream {:?}; returning successor {:?}",
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
                Frame::error_response(
                    frame.request_id(),
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
    /// Payload: [count:u32]  (0 = all extents, N = at most N from latest to earliest)
    /// Response: DescribeStreamResp with encoded Vec<ExtentInfo>
    async fn handle_describe_stream(&self, frame: Frame) -> Frame {
        let stream_id = frame.stream_id();
        let count = frame.count();

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
                Frame::error_response(
                    frame.request_id(),
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
            Ok(None) => Frame::error_response(
                frame.request_id(),
                ErrorCode::UnknownStream,
                &format!(
                    "extent not found: stream={:?}, extent={:?}",
                    stream_id, extent_id
                ),
                ExtentId(0),
            ),
            Err(e) => {
                error!("describe_extent failed: {e}");
                Frame::error_response(
                    frame.request_id(),
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
            Ok(None) => Frame::error_response(
                frame.request_id(),
                ErrorCode::InvalidOffset,
                &format!(
                    "no extent contains offset {} for stream {:?}",
                    offset, stream_id
                ),
                ExtentId(0),
            ),
            Err(e) => {
                error!("seek failed: {e}");
                Frame::error_response(
                    frame.request_id(),
                    ErrorCode::InternalError,
                    &e.to_string(),
                    ExtentId(0),
                )
            }
        }
    }
}
