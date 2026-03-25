use std::sync::Arc;

use crate::metadata::{MetadataStore, SealResult};
use bytes::Bytes;
use common::errors::StorageError;
use common::types::{
    ErrorCode, ExtentId, FLAG_NEW_EXTENT_PRESENT, FLAG_OFFSET_PRESENT, Offset, Opcode, StreamId,
};
use futures_util::future;
use rpc::frame::Frame;
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
async fn seal_extent_node_static(addr: &str, stream_id: StreamId) -> Result<u64, StorageError> {
    let mut client = client::StorageClient::connect(addr).await.map_err(|e| {
        StorageError::Internal(format!("connect to ExtentNode {addr} for Seal: {e}"))
    })?;

    let resp = client
        .send_frame(Frame {
            opcode: Opcode::Seal,
            stream_id,
            ..Default::default()
        })
        .await
        .map_err(|e| StorageError::Internal(format!("Seal to ExtentNode {addr}: {e}")))?;

    if resp.opcode == Opcode::Error {
        let msg = String::from_utf8_lossy(&resp.payload).to_string();
        return Err(StorageError::Internal(format!(
            "ExtentNode {addr} rejected Seal: {msg}"
        )));
    }

    // SealAck carries committed end_offset in the offset field.
    Ok(resp.offset.0)
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

    /// Send RegisterExtent to each ExtentNode in the replica set for broadcast replication.
    ///
    /// `replica_addrs`: ordered [Primary, Secondary_1, ...].
    /// - Primary receives all secondary addresses so it can broadcast appends.
    /// - Secondaries receive empty replica_addrs (they don't forward to anyone).
    async fn notify_replica_set(
        &self,
        stream_id: StreamId,
        extent_id: ExtentId,
        replica_addrs: &[String],
    ) -> Result<(), StorageError> {
        // Collect secondary addresses (all addresses after the first).
        let secondary_addrs: Vec<&str> = replica_addrs[1..].iter().map(|s| s.as_str()).collect();

        for (i, addr) in replica_addrs.iter().enumerate() {
            let role = i as u8;
            let rf = replica_addrs.len() as u16;

            // Primary gets all secondary addresses; secondaries get none.
            let addrs_for_node: Vec<&str> = if role == 0 {
                secondary_addrs.clone()
            } else {
                vec![]
            };

            let payload =
                build_register_extent_payload(stream_id.0, extent_id.0, role, rf, &addrs_for_node);

            let mut client = client::StorageClient::connect(addr).await.map_err(|e| {
                StorageError::Internal(format!(
                    "connect to ExtentNode {addr} for RegisterExtent: {e}"
                ))
            })?;

            let resp = client
                .send_frame(Frame {
                    opcode: Opcode::RegisterExtent,
                    stream_id,
                    payload,
                    ..Default::default()
                })
                .await
                .map_err(|e| {
                    StorageError::Internal(format!("RegisterExtent to ExtentNode {addr}: {e}"))
                })?;

            if resp.opcode == Opcode::Error {
                let msg = String::from_utf8_lossy(&resp.payload).to_string();
                return Err(StorageError::Internal(format!(
                    "ExtentNode {addr} rejected RegisterExtent: {msg}"
                )));
            }

            let role_name = if role == 0 { "Primary" } else { "Secondary" };
            info!(
                "RegisterExtent sent to ExtentNode {addr}: stream={:?}, extent={:?}, role={role_name}, rf={rf}, secondaries={}",
                stream_id,
                extent_id,
                addrs_for_node.join(", ")
            );
        }
        Ok(())
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

        // Notify each ExtentNode of its replication role.
        // Always send RegisterExtent, even for replication_factor=1, so the ExtentNode knows
        // the StreamManager-assigned stream_id and extent_id (required for seal coordination).
        self.notify_replica_set(stream_id, extent_id, &node_addrs)
            .await?;

        Ok((extent_id, node_addrs[0].clone()))
    }
}

impl RequestHandler for StreamManagerStore {
    async fn handle_frame(
        &self,
        frame: Frame,
        _response_tx: Option<&tokio::sync::mpsc::Sender<Frame>>,
    ) -> Option<Frame> {
        let response = match frame.opcode {
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
                frame.request_id,
                ErrorCode::InternalError,
                &format!("StreamManager: unsupported opcode {:?}", frame.opcode),
                ExtentId(0),
            ),
        };
        Some(response)
    }
}

impl StreamManagerStore {
    /// ExtentNode Connect: register node. Payload = [node_id_len:u16][node_id][addr_len:u16][addr][interval_ms:u32]
    async fn handle_connect(&self, frame: Frame) -> Frame {
        let payload = &frame.payload;
        match parse_connect_payload(payload) {
            Some((node_id, addr, interval_ms)) => {
                match self.store.register_node(&node_id, &addr, interval_ms).await {
                    Ok(()) => {
                        info!(
                            "ExtentNode registered: node_id={node_id}, addr={addr}, interval={interval_ms}ms"
                        );
                        Frame {
                            opcode: Opcode::ConnectAck,
                            request_id: frame.request_id,
                            ..Default::default()
                        }
                    }
                    Err(e) => {
                        error!("register_node failed: {e}");
                        Frame::error_response(
                            frame.request_id,
                            ErrorCode::InternalError,
                            &e.to_string(),
                            ExtentId(0),
                        )
                    }
                }
            }
            None => Frame::error_response(
                frame.request_id,
                ErrorCode::InternalError,
                "invalid Connect payload",
                ExtentId(0),
            ),
        }
    }

    /// ExtentNode Heartbeat: update heartbeat timestamp and cache runtime metrics.
    /// Payload = [node_id_len:u16][node_id][metrics:32 bytes]
    async fn handle_heartbeat(&self, frame: Frame) -> Frame {
        let payload = &frame.payload;
        match parse_heartbeat_payload(payload) {
            Some((node_id, metrics)) => {
                match self.store.update_heartbeat(&node_id).await {
                    Ok(()) => {
                        // Update in-memory metrics on the allocator for load-aware placement.
                        let mut alloc = self.allocator.lock().await;
                        alloc.update_metrics(&node_id, metrics);
                        Frame {
                            opcode: Opcode::Heartbeat,
                            request_id: frame.request_id,
                            ..Default::default()
                        }
                    }
                    Err(e) => {
                        error!("update_heartbeat failed: {e}");
                        Frame::error_response(
                            frame.request_id,
                            ErrorCode::InternalError,
                            &e.to_string(),
                            ExtentId(0),
                        )
                    }
                }
            }
            None => Frame::error_response(
                frame.request_id,
                ErrorCode::InternalError,
                "invalid Heartbeat payload",
                ExtentId(0),
            ),
        }
    }

    /// ExtentNode Disconnect: mark node as dead, clean up metrics, stop allocating.
    /// Payload = [node_id_len:u16][node_id]
    async fn handle_disconnect(&self, frame: Frame) -> Frame {
        let payload = &frame.payload;
        match parse_string_payload(payload) {
            Some(node_id) => {
                match self.store.mark_node_dead(&node_id).await {
                    Ok(()) => {
                        // Clean up in-memory metrics for the disconnected node.
                        let mut alloc = self.allocator.lock().await;
                        alloc.remove_metrics(&node_id);
                        info!("ExtentNode disconnected: node_id={node_id}");
                        Frame {
                            opcode: Opcode::DisconnectAck,
                            request_id: frame.request_id,
                            ..Default::default()
                        }
                    }
                    Err(e) => {
                        error!("mark_node_dead on disconnect failed: {e}");
                        Frame::error_response(
                            frame.request_id,
                            ErrorCode::InternalError,
                            &e.to_string(),
                            ExtentId(0),
                        )
                    }
                }
            }
            None => Frame::error_response(
                frame.request_id,
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
    /// Response payload: [extent_id:u64][addr_len:u16][primary_addr]
    async fn handle_create_stream(&self, frame: Frame) -> Frame {
        let payload = &frame.payload;
        let (stream_name, replication_factor) = match parse_create_stream_payload(payload) {
            Some((name, rf)) => (name, rf),
            None => {
                return Frame::error_response(
                    frame.request_id,
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
            Ok((stream_id, extent_id, primary_addr)) => Frame {
                opcode: Opcode::CreateStreamResp,
                request_id: frame.request_id,
                stream_id,
                extent_id,
                payload: build_string_payload(&primary_addr),
                ..Default::default()
            },
            Err(e) => {
                error!("create_stream failed: {e}");
                Frame::error_response(
                    frame.request_id,
                    ErrorCode::InternalError,
                    &e.to_string(),
                    ExtentId(0),
                )
            }
        }
    }

    /// Seal an extent for a stream and allocate a new replica set.
    ///
    /// Dispatches to one of two paths based on FLAG_OFFSET_PRESENT flag:
    /// - Flag clear → `client_seal`: SM queries all EN replicas for offset via quorum.
    /// - Flag set → `extent_node_seal`: SM trusts offset from primary EN (in frame.offset).
    ///
    /// Both paths seal in MySQL (transaction) + allocate new extent and notify ExtentNodes.
    async fn handle_seal(&self, frame: Frame) -> Frame {
        let stream_id = frame.stream_id;
        let extent_id = frame.extent_id;

        let result = if frame.flags & FLAG_OFFSET_PRESENT != 0 {
            // extent_node_seal: primary EN provides committed offset.
            self.extent_node_seal(stream_id, extent_id, frame.offset.0)
                .await
        } else {
            // client_seal: client doesn't know offset, SM queries all EN replicas.
            self.client_seal(stream_id, extent_id).await
        };

        match result {
            Ok((new_extent_id, primary_addr)) => Frame {
                opcode: Opcode::SealAck,
                flags: FLAG_NEW_EXTENT_PRESENT,
                request_id: frame.request_id,
                stream_id,
                extent_id,
                count: new_extent_id.0, // new_extent_id encoded as u32 in count field
                payload: Bytes::copy_from_slice(primary_addr.as_bytes()), // primary_addr
                ..Default::default()
            },
            Err(e) => {
                error!("seal failed: {e}");
                Frame::error_response(
                    frame.request_id,
                    ErrorCode::InternalError,
                    &e.to_string(),
                    ExtentId(0),
                )
            }
        }
    }

    /// Client-initiated seal: query all EN replicas for committed offset via quorum,
    /// then seal-and-allocate a new extent.
    ///
    /// Flow:
    /// 1. Get replicas for the extent.
    /// 2. Concurrently seal ALL replicas on ExtentNodes (reject further appends).
    /// 3. Determine committed offset from Primary or Secondary quorum.
    /// 4. Seal in MySQL (transaction) + allocate new extent.
    /// 5. Register new extent on ExtentNodes.
    async fn client_seal(
        &self,
        stream_id: StreamId,
        extent_id: ExtentId,
    ) -> Result<(ExtentId, String), StorageError> {
        // 1. Get replicas for the extent, ordered by role (Primary first).
        let replicas = self.store.get_replicas(stream_id, extent_id).await?;
        if replicas.is_empty() {
            return Err(StorageError::Internal(format!(
                "no replicas found for stream {:?} extent {:?}",
                stream_id, extent_id
            )));
        }

        // 2. Concurrently seal ALL replicas on ExtentNodes.
        let mut seal_futures = Vec::new();
        for replica in &replicas {
            let addr = replica.node_addr.clone();
            let role = replica.role;
            let sid = stream_id;
            seal_futures.push(async move {
                let result = seal_extent_node_static(&addr, sid).await;
                (addr, role, result)
            });
        }
        let seal_results = future::join_all(seal_futures).await;

        // 3. Determine committed offset from responses.
        let mut primary_offset: Option<u64> = None;
        let mut secondary_offsets: Vec<u64> = Vec::new();
        let rf = replicas.len() as u16;
        let required_secondary_acks = (rf as u32) / 2;

        for (addr, role, result) in &seal_results {
            match result {
                Ok(offset) => {
                    if *role == 0 {
                        info!(
                            "Primary {addr} reports committed offset {offset} for stream {:?}",
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
            // Primary responded: use its quorum_offset (most accurate).
            offset
        } else {
            // Primary failed: compute from Secondary responses using quorum math.
            if (secondary_offsets.len() as u32) < required_secondary_acks {
                return Err(StorageError::Internal(format!(
                    "insufficient replicas for seal: need {} secondary ACKs, got {}",
                    required_secondary_acks,
                    secondary_offsets.len()
                )));
            }
            // For RF=1 (no secondaries, no primary): this is a failure.
            if secondary_offsets.is_empty() {
                return Err(StorageError::Internal(
                    "no ExtentNodes responded to seal".into(),
                ));
            }
            // Take kth largest, where k = required_secondary_acks.
            secondary_offsets.sort_unstable_by(|a, b| b.cmp(a)); // descending
            let k = required_secondary_acks as usize;
            if k == 0 {
                // RF=2, Primary down, 1 Secondary: take its offset.
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
            "client_seal: sealing extent {:?} for stream {:?}: committed={committed}",
            extent_id, stream_id
        );

        // 4. Verify active extent exists.
        let _extent_row = self
            .store
            .get_active_extent(stream_id)
            .await?
            .ok_or_else(|| {
                StorageError::Internal(format!(
                    "no active extent found for stream {:?} during client_seal",
                    stream_id
                ))
            })?;
        let end_offset = committed;

        // 5. Pick nodes for new extent replica set.
        self.seal_allocate_notify(stream_id, extent_id, end_offset)
            .await
    }

    /// Extent-node-initiated (proactive) seal: the primary EN provides the committed
    /// offset directly. SM trusts it, seals-and-allocates, then fires-and-forgets
    /// seal RPCs to all secondary ENs.
    ///
    /// Flow:
    /// 1. Verify active extent exists.
    /// 2. Seal in MySQL + allocate new extent.
    /// 3. Register new extent on ExtentNodes.
    /// 4. Fire-and-forget: seal secondary ENs asynchronously.
    async fn extent_node_seal(
        &self,
        stream_id: StreamId,
        extent_id: ExtentId,
        offset: u64,
    ) -> Result<(ExtentId, String), StorageError> {
        info!(
            "extent_node_seal: sealing extent {:?} for stream {:?}: offset={offset}",
            extent_id, stream_id
        );

        // 1. Verify active extent exists. The provided offset IS the end_offset.
        let _extent_row = self
            .store
            .get_active_extent(stream_id)
            .await?
            .ok_or_else(|| {
                StorageError::Internal(format!(
                    "no active extent found for stream {:?} during extent_node_seal",
                    stream_id
                ))
            })?;
        let end_offset = offset;

        // 2-3. Seal + allocate + notify new replica set.
        let result = self
            .seal_allocate_notify(stream_id, extent_id, end_offset)
            .await?;

        // 4. Fire-and-forget: seal all secondary ENs of the old extent.
        let replicas = self
            .store
            .get_replicas(stream_id, extent_id)
            .await
            .unwrap_or_default();
        let secondary_addrs: Vec<String> = replicas
            .into_iter()
            .filter(|r| r.role != 0)
            .map(|r| r.node_addr)
            .collect();

        if !secondary_addrs.is_empty() {
            let sid = stream_id;
            tokio::spawn(async move {
                for addr in secondary_addrs {
                    match seal_extent_node_static(&addr, sid).await {
                        Ok(offset) => {
                            info!("fire-and-forget seal to secondary {addr}: offset={offset}");
                        }
                        Err(e) => {
                            warn!("fire-and-forget seal to secondary {addr} failed: {e}");
                        }
                    }
                }
            });
        }

        Ok(result)
    }

    /// Shared logic for both seal paths: pick new nodes, seal-and-allocate in DB,
    /// notify new replica set, and return (new_extent_id, primary_addr).
    async fn seal_allocate_notify(
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

                info!(
                    "new extent {:?} allocated for stream {:?}, primary={primary_addr}",
                    new_extent_id, stream_id
                );

                // Register new extent on ExtentNodes.
                self.notify_replica_set(stream_id, new_extent_id, &node_addrs)
                    .await?;

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
        let stream_id = frame.stream_id;

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
            Ok(offset) => Frame {
                opcode: Opcode::QueryOffsetResp,
                request_id: frame.request_id,
                stream_id,
                offset,
                ..Default::default()
            },
            Err(e) => {
                error!("query_offset failed: {e}");
                Frame::error_response(
                    frame.request_id,
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
        let stream_id = frame.stream_id;
        let count = frame.count;

        match self.store.describe_stream_extents(stream_id, count).await {
            Ok(extents) => {
                let payload = encode_extent_info_vec(&extents);
                Frame {
                    opcode: Opcode::DescribeStreamResp,
                    request_id: frame.request_id,
                    stream_id,
                    payload,
                    ..Default::default()
                }
            }
            Err(e) => {
                error!("describe_stream failed: {e}");
                Frame::error_response(
                    frame.request_id,
                    ErrorCode::InternalError,
                    &e.to_string(),
                    ExtentId(0),
                )
            }
        }
    }

    /// DescribeExtent: return a single extent's metadata with replica info and node liveness.
    ///
    /// Payload: [extent_id:u64]
    /// Response: DescribeExtentResp with encoded Vec<ExtentInfo> (length 1)
    async fn handle_describe_extent(&self, frame: Frame) -> Frame {
        let stream_id = frame.stream_id;
        let extent_id = frame.extent_id;

        match self.store.describe_extent(stream_id, extent_id).await {
            Ok(Some(info)) => {
                let payload = encode_extent_info_vec(&[info]);
                Frame {
                    opcode: Opcode::DescribeExtentResp,
                    request_id: frame.request_id,
                    stream_id,
                    payload,
                    ..Default::default()
                }
            }
            Ok(None) => Frame::error_response(
                frame.request_id,
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
                    frame.request_id,
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
        let stream_id = frame.stream_id;
        let offset = frame.offset.0;

        match self.store.seek_extent(stream_id, offset).await {
            Ok(Some(info)) => {
                let payload = encode_extent_info_vec(&[info]);
                Frame {
                    opcode: Opcode::SeekResp,
                    request_id: frame.request_id,
                    stream_id,
                    offset: Offset(offset),
                    payload,
                    ..Default::default()
                }
            }
            Ok(None) => Frame::error_response(
                frame.request_id,
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
                    frame.request_id,
                    ErrorCode::InternalError,
                    &e.to_string(),
                    ExtentId(0),
                )
            }
        }
    }
}
