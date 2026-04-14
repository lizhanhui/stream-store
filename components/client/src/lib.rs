use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use bytes::{Buf, Bytes};
use common::config::{DEFAULT_CONNECT_TIMEOUT_MS, DEFAULT_SM_REQUEST_TIMEOUT_MS};
use common::errors::StorageError;
use common::types::{
    Epoch, ErrorCode, ExtentId, ExtentInfo, ExtentState, NodeMetrics, Offset, Opcode, StreamId,
};
use futures_util::{SinkExt, StreamExt};
use rpc::codec::FrameCodec;
use rpc::frame::{Frame, VariableHeader};
use rpc::payload::{
    build_connect_payload, build_heartbeat_payload, build_string_payload, parse_extent_info_vec,
};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::warn;

/// Result of a successful append: the logical offset assigned to this record,
/// plus the extent and epoch the record landed on (for diagnostics).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendResult {
    /// Logical offset assigned to this record.
    pub offset: Offset,
    /// The extent the record was written to (server-assigned, for diagnostics).
    pub extent_id: ExtentId,
    pub epoch: Epoch,
}

type PendingMap = HashMap<u32, oneshot::Sender<Result<Frame, StorageError>>>;

struct Inner {
    write_tx: mpsc::Sender<Frame>,
    pending: Mutex<PendingMap>,
    next_request_id: AtomicU32,
}

/// A client for communicating with an Extent Node or Stream Manager.
///
/// Supports pipelining: multiple requests can be in-flight simultaneously.
/// All public methods take `&self`, enabling shared ownership via `Arc`.
pub struct StreamClient {
    inner: Arc<Inner>,
    request_timeout: Duration,
    primary_cache: Mutex<HashMap<StreamId, String>>,
    _reader_handle: JoinHandle<()>,
    _writer_handle: JoinHandle<()>,
}

impl Drop for StreamClient {
    fn drop(&mut self) {
        self._reader_handle.abort();
        self._writer_handle.abort();
    }
}

impl StreamClient {
    /// Connect to a storage service endpoint with default timeouts.
    pub async fn connect(addr: &str) -> Result<Self, StorageError> {
        Self::connect_with_timeouts(
            addr,
            Duration::from_millis(DEFAULT_CONNECT_TIMEOUT_MS),
            Duration::from_millis(DEFAULT_SM_REQUEST_TIMEOUT_MS),
        )
        .await
    }

    /// Connect to a storage service endpoint with custom timeouts.
    pub async fn connect_with_timeouts(
        addr: &str,
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> Result<Self, StorageError> {
        let stream = tokio::time::timeout(connect_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| StorageError::Internal(format!("connect timeout to {addr}")))??;
        stream
            .set_nodelay(true)
            .map_err(|e| StorageError::Internal(format!("set TCP_NODELAY: {e}")))?;

        let (read_half, write_half) = stream.into_split();
        let framed_read = FramedRead::new(read_half, FrameCodec);
        let framed_write = FramedWrite::new(write_half, FrameCodec);

        // Channel for sending frames to the writer task.
        // Buffer of 256 keeps backpressure reasonable.
        let (write_tx, write_rx) = mpsc::channel::<Frame>(256);

        let inner = Arc::new(Inner {
            write_tx,
            pending: Mutex::new(HashMap::new()),
            next_request_id: AtomicU32::new(1),
        });

        let writer_handle = tokio::spawn(Self::writer_task(framed_write, write_rx));
        let reader_handle = tokio::spawn(Self::reader_task(framed_read, Arc::clone(&inner)));

        Ok(Self {
            inner,
            request_timeout,
            primary_cache: Mutex::new(HashMap::new()),
            _reader_handle: reader_handle,
            _writer_handle: writer_handle,
        })
    }

    /// Background writer task: drains frames from the channel into the TCP connection.
    async fn writer_task(
        mut framed_write: FramedWrite<tokio::net::tcp::OwnedWriteHalf, FrameCodec>,
        mut write_rx: mpsc::Receiver<Frame>,
    ) {
        while let Some(frame) = write_rx.recv().await {
            if let Err(e) = framed_write.send(frame).await {
                warn!("StreamClient writer error: {e}");
                break;
            }
        }
    }

    /// Background reader task: reads response frames and dispatches them to pending callers.
    async fn reader_task(
        mut framed_read: FramedRead<tokio::net::tcp::OwnedReadHalf, FrameCodec>,
        inner: Arc<Inner>,
    ) {
        loop {
            match framed_read.next().await {
                Some(Ok(frame)) => {
                    let request_id = frame.request_id();
                    let mut pending = inner.pending.lock().await;
                    if let Some(tx) = pending.remove(&request_id) {
                        // Ignore send error: caller may have timed out and dropped the receiver.
                        let _ = tx.send(Ok(frame));
                    }
                }
                Some(Err(e)) => {
                    warn!("StreamClient reader error: {e}");
                    // Notify all pending callers of the error.
                    let mut pending = inner.pending.lock().await;
                    for (_, tx) in pending.drain() {
                        let _ = tx.send(Err(StorageError::Internal(format!(
                            "connection read error: {e}"
                        ))));
                    }
                    break;
                }
                None => {
                    // Connection closed.
                    let mut pending = inner.pending.lock().await;
                    for (_, tx) in pending.drain() {
                        let _ = tx.send(Err(StorageError::Internal("connection closed".into())));
                    }
                    break;
                }
            }
        }
    }

    fn alloc_request_id(&self) -> u32 {
        self.inner.next_request_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Send a request frame and wait for the corresponding response.
    /// Supports pipelining: multiple send_request calls can be in flight concurrently.
    async fn send_request(&self, frame: Frame) -> Result<Frame, StorageError> {
        let request_id = frame.request_id();
        let (tx, rx) = oneshot::channel();

        // Insert into pending map BEFORE sending to avoid race with reader task.
        {
            let mut pending = self.inner.pending.lock().await;
            pending.insert(request_id, tx);
        }

        // Send the frame to the writer task.
        if self.inner.write_tx.send(frame).await.is_err() {
            // Writer task is gone — clean up and report.
            let mut pending = self.inner.pending.lock().await;
            pending.remove(&request_id);
            return Err(StorageError::Internal("connection closed".into()));
        }

        // Wait for response with timeout.
        match tokio::time::timeout(self.request_timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_recv_err)) => {
                // Sender was dropped (reader task died).
                Err(StorageError::Internal("connection closed".into()))
            }
            Err(_timeout) => {
                // Timed out — clean up pending slot.
                let mut pending = self.inner.pending.lock().await;
                pending.remove(&request_id);
                Err(StorageError::Internal("RPC request timeout".into()))
            }
        }
    }

    /// Send a raw frame and return the response. Used by StreamManager to communicate with ExtentNodes.
    pub async fn send_frame(&self, frame: Frame) -> Result<Frame, StorageError> {
        self.send_request(frame).await
    }

    /// Send a raw frame without waiting for a response (fire-and-forget).
    /// Used for async notifications like NOTIFY_SEALED_EXTENT.
    pub async fn send_frame_no_response(&self, frame: Frame) -> Result<(), StorageError> {
        self.inner
            .write_tx
            .send(frame)
            .await
            .map_err(|e| StorageError::Internal(format!("send failed: {e}")))?;
        Ok(())
    }

    fn check_error(resp: &Frame) -> Result<(), StorageError> {
        if !resp.is_error_response() {
            return Ok(());
        }

        let msg = String::from_utf8_lossy(resp.payload.as_deref().unwrap_or_default()).to_string();
        let error_code = ErrorCode::from_u16(resp.error_code());
        Err(match error_code {
            Some(ErrorCode::UnknownStream) => StorageError::UnknownStream(resp.stream_id()),
            Some(ErrorCode::ExtentSealed) => StorageError::ExtentSealed(resp.extent_id()),
            Some(ErrorCode::EpochStale) => StorageError::EpochStale(resp.stream_id(), resp.epoch()),
            _ => StorageError::Internal(msg),
        })
    }

    /// Create a new stream on the StreamManager.
    /// Variable header carries stream name, per-stream replication factor, per-stream extent capacity bounds,
    /// and per-stream cache_extents (max extents to retain in memory).
    /// If replication_factor=0, the StreamManager uses its default.
    /// If min_extent_capacity=0, the StreamManager uses its default (8 MiB).
    /// If max_extent_capacity=0, the StreamManager uses its default (256 MiB).
    /// If cache_extents=0, the StreamManager uses its default (4).
    /// Returns (StreamId, ExtentId, Epoch, ExtentNode address for the first extent).
    pub async fn create_stream(
        &self,
        name: &str,
        replication_factor: u16,
        min_extent_capacity: u32,
        max_extent_capacity: u32,
        cache_extents: u32,
        extent_growth_factor: u32,
    ) -> Result<(StreamId, ExtentId, Epoch, String), StorageError> {
        let req = Frame::new(
            VariableHeader::CreateStream {
                request_id: self.alloc_request_id(),
                stream_name: Bytes::from(name.to_owned()),
                replication_factor,
                min_extent_capacity,
                max_extent_capacity,
                cache_extents,
                extent_growth_factor,
            },
            None,
        );
        let resp = self.send_request(req).await?;
        Self::check_error(&resp)?;
        if resp.opcode() != Opcode::CreateStreamResp {
            return Err(StorageError::Internal(format!(
                "expected CreateStreamResp, got {:?}",
                resp.opcode()
            )));
        }

        let addr =
            if let VariableHeader::CreateStreamResp { primary_addr, .. } = &resp.variable_header {
                String::from_utf8_lossy(primary_addr).to_string()
            } else {
                return Err(StorageError::Internal(
                    "unexpected variable header in CreateStreamResp".into(),
                ));
            };

        let stream_id = resp.stream_id();
        self.cache_primary(stream_id, &addr).await;
        Ok((stream_id, resp.extent_id(), resp.epoch(), addr))
    }

    /// Append a message to a stream. Returns the assigned offset and diagnostics.
    ///
    /// The `epoch` parameter identifies which replica set the client is targeting.
    /// If the epoch is stale (the Primary has been reassigned via an epoch bump),
    /// the server returns `EpochStale` and the client should re-discover via
    /// `describe_stream`.
    pub async fn append(
        &self,
        stream_id: StreamId,
        epoch: Epoch,
        payload: Bytes,
    ) -> Result<AppendResult, StorageError> {
        let req = Frame::new(
            VariableHeader::Append {
                request_id: self.alloc_request_id(),
                stream_id,
                epoch,
            },
            Some(payload),
        );
        let resp = self.send_request(req).await?;
        Self::check_error(&resp)?;

        Ok(AppendResult {
            offset: resp.offset(),
            extent_id: resp.extent_id(),
            epoch: resp.epoch(),
        })
    }

    /// Read `count` messages from a stream starting at `offset`.
    ///
    /// The server resolves byte positions internally via its index stream,
    /// so only the logical offset is needed.
    pub async fn read(
        &self,
        stream_id: StreamId,
        extent_id: ExtentId,
        offset: Offset,
        count: u16,
    ) -> Result<Vec<Bytes>, StorageError> {
        let req = Frame::new(
            VariableHeader::Read {
                request_id: self.alloc_request_id(),
                stream_id,
                extent_id,
                offset,
                count: count as u32,
            },
            None,
        );
        let resp = self.send_request(req).await?;
        Self::check_error(&resp)?;

        // Decode payload: [u32 len][bytes] repeated.
        let msg_count = resp.count() as usize;
        let mut messages = Vec::with_capacity(msg_count);
        let mut buf: &[u8] = resp.payload.as_deref().unwrap_or_default();

        for _ in 0..msg_count {
            if buf.remaining() < 4 {
                break;
            }
            let len = buf.get_u32() as usize;
            if buf.remaining() < len {
                break;
            }
            messages.push(Bytes::copy_from_slice(&buf[..len]));
            buf.advance(len);
        }

        Ok(messages)
    }

    /// Query the max offset (exclusive) for a stream.
    pub async fn query_offset(&self, stream_id: StreamId) -> Result<Offset, StorageError> {
        let req = Frame::new(
            VariableHeader::QueryOffset {
                request_id: self.alloc_request_id(),
                stream_id,
            },
            None,
        );
        let resp = self.send_request(req).await?;
        Self::check_error(&resp)?;
        Ok(resp.offset())
    }

    // ── Lifecycle operations (ExtentNode -> StreamManager) ──

    /// Send Connect to StreamManager to register an ExtentNode node.
    pub async fn connect_extent_node(
        &self,
        node_id: &str,
        addr: &str,
        heartbeat_interval_ms: u32,
    ) -> Result<(), StorageError> {
        let req = Frame::new(
            VariableHeader::Connect {
                request_id: self.alloc_request_id(),
            },
            Some(build_connect_payload(node_id, addr, heartbeat_interval_ms)),
        );
        let resp = self.send_request(req).await?;
        Self::check_error(&resp)?;
        if resp.opcode() != Opcode::ConnectAck {
            return Err(StorageError::Internal(format!(
                "expected ConnectAck, got {:?}",
                resp.opcode()
            )));
        }
        Ok(())
    }

    /// Send Heartbeat to StreamManager with runtime metrics.
    pub async fn heartbeat(
        &self,
        node_id: &str,
        metrics: &NodeMetrics,
    ) -> Result<(), StorageError> {
        let req = Frame::new(
            VariableHeader::Heartbeat {
                request_id: self.alloc_request_id(),
            },
            Some(build_heartbeat_payload(node_id, metrics)),
        );
        let resp = self.send_request(req).await?;
        Self::check_error(&resp)?;
        Ok(())
    }

    /// Send Disconnect to StreamManager.
    pub async fn disconnect_extent_node(&self, node_id: &str) -> Result<(), StorageError> {
        let req = Frame::new(
            VariableHeader::Disconnect {
                request_id: self.alloc_request_id(),
            },
            Some(build_string_payload(node_id)),
        );
        let resp = self.send_request(req).await?;
        Self::check_error(&resp)?;
        if resp.opcode() != Opcode::DisconnectAck {
            return Err(StorageError::Internal(format!(
                "expected DisconnectAck, got {:?}",
                resp.opcode()
            )));
        }
        Ok(())
    }

    // ── Control operations (StreamManager) ──

    /// Seal an extent on the StreamManager and allocate a new one.
    ///
    /// - `committed_offset = None` (client seal): StreamManager queries all EN replicas
    ///   to determine the committed offset via quorum algorithm.
    /// - `committed_offset = Some(offset)` (extent-node seal): StreamManager trusts the
    ///   provided offset without querying replicas. Used when the primary ExtentNode has
    ///   already sealed the extent locally (e.g. extent full).
    ///
    /// Returns (new_extent_id, new_primary_addr).
    pub async fn seal(
        &self,
        stream_id: StreamId,
        extent_id: ExtentId,
        committed_offset: Option<u64>,
    ) -> Result<(u32, String), StorageError> {
        let req = Frame::new(
            VariableHeader::Seal {
                request_id: self.alloc_request_id(),
                stream_id,
                extent_id,
                offset: committed_offset.map(Offset),
                start_offset: None,
                epoch: None,
            },
            None,
        );
        let resp = self.send_request(req).await?;
        Self::check_error(&resp)?;
        if resp.opcode() != Opcode::SealAck {
            return Err(StorageError::Internal(format!(
                "expected SealAck, got {:?}",
                resp.opcode()
            )));
        }

        // SealAck: new_extent_id and primary_addr from variable header
        if let VariableHeader::SealAck {
            new_extent_id,
            primary_addr,
            ..
        } = &resp.variable_header
        {
            let new_eid = new_extent_id.map(|e| e.0).unwrap_or(0);
            let addr = primary_addr
                .as_ref()
                .map(|b| String::from_utf8_lossy(b).to_string())
                .unwrap_or_default();
            if !addr.is_empty() {
                self.cache_primary(stream_id, &addr).await;
            }
            Ok((new_eid, addr))
        } else {
            Err(StorageError::Internal(
                "unexpected variable header in SealAck".into(),
            ))
        }
    }

    /// Seal a stream by epoch on the StreamManager and allocate a new one.
    ///
    /// The client identifies the stream by `(stream_id, epoch)` — the SM looks up
    /// the active extent at that epoch, seals it, bumps epoch, and allocates a new
    /// extent on a (potentially different) replica set.
    ///
    /// Returns (new_extent_id, new_primary_addr, new_epoch).
    pub async fn seal_by_epoch(
        &self,
        stream_id: StreamId,
        epoch: Epoch,
    ) -> Result<(u32, String, Option<Epoch>), StorageError> {
        let req = Frame::new(
            VariableHeader::Seal {
                request_id: self.alloc_request_id(),
                stream_id,
                extent_id: ExtentId(0), // not used for epoch-based seal
                offset: None,
                start_offset: None,
                epoch: Some(epoch),
            },
            None,
        );
        let resp = self.send_request(req).await?;
        Self::check_error(&resp)?;
        if resp.opcode() != Opcode::SealAck {
            return Err(StorageError::Internal(format!(
                "expected SealAck, got {:?}",
                resp.opcode()
            )));
        }

        if let VariableHeader::SealAck {
            new_extent_id,
            primary_addr,
            epoch: new_epoch,
            ..
        } = &resp.variable_header
        {
            let new_eid = new_extent_id.map(|e| e.0).unwrap_or(0);
            let addr = primary_addr
                .as_ref()
                .map(|b| String::from_utf8_lossy(b).to_string())
                .unwrap_or_default();
            if !addr.is_empty() {
                self.cache_primary(stream_id, &addr).await;
            }
            Ok((new_eid, addr, *new_epoch))
        } else {
            Err(StorageError::Internal(
                "unexpected variable header in SealAck".into(),
            ))
        }
    }

    // ── Management operations (StreamManager) ──

    /// Describe a stream's extents with replica info and node liveness.
    ///
    /// - `count = 0`: return all extents (latest to earliest).
    /// - `count = 1`: return the latest (active/mutable) extent only.
    /// - `count = N`: return at most N extents from latest to earliest.
    pub async fn describe_stream(
        &self,
        stream_id: StreamId,
        count: u32,
    ) -> Result<Vec<ExtentInfo>, StorageError> {
        let req = Frame::new(
            VariableHeader::DescribeStream {
                request_id: self.alloc_request_id(),
                stream_id,
                count,
                stream_name: None,
            },
            None,
        );
        let resp = self.send_request(req).await?;
        Self::check_error(&resp)?;
        if resp.opcode() != Opcode::DescribeStreamResp {
            return Err(StorageError::Internal(format!(
                "expected DescribeStreamResp, got {:?}",
                resp.opcode()
            )));
        }
        let extents = parse_extent_info_vec(resp.payload.as_deref().unwrap_or_default())
            .ok_or_else(|| StorageError::Internal("invalid DescribeStreamResp payload".into()))?;
        self.cache_primary_from_extents(stream_id, &extents).await;
        Ok(extents)
    }

    /// Describe a single extent with replica info and node liveness.
    pub async fn describe_extent(
        &self,
        stream_id: StreamId,
        extent_id: ExtentId,
    ) -> Result<ExtentInfo, StorageError> {
        let req = Frame::new(
            VariableHeader::DescribeExtent {
                request_id: self.alloc_request_id(),
                stream_id,
                extent_id,
            },
            None,
        );
        let resp = self.send_request(req).await?;
        Self::check_error(&resp)?;
        if resp.opcode() != Opcode::DescribeExtentResp {
            return Err(StorageError::Internal(format!(
                "expected DescribeExtentResp, got {:?}",
                resp.opcode()
            )));
        }
        let extents = parse_extent_info_vec(resp.payload.as_deref().unwrap_or_default())
            .ok_or_else(|| StorageError::Internal("invalid DescribeExtentResp payload".into()))?;
        extents.into_iter().next().ok_or_else(|| {
            StorageError::Internal("DescribeExtentResp returned empty result".into())
        })
    }

    /// Seek: resolve a logical stream offset to the extent that contains it.
    ///
    /// Returns the `ExtentInfo` for the extent covering `offset`, including replica
    /// addresses so the caller knows which extent node(s) to read from.
    pub async fn seek(
        &self,
        stream_id: StreamId,
        offset: Offset,
    ) -> Result<ExtentInfo, StorageError> {
        let req = Frame::new(
            VariableHeader::Seek {
                request_id: self.alloc_request_id(),
                stream_id,
                offset,
            },
            None,
        );
        let resp = self.send_request(req).await?;
        Self::check_error(&resp)?;
        if resp.opcode() != Opcode::SeekResp {
            return Err(StorageError::Internal(format!(
                "expected SeekResp, got {:?}",
                resp.opcode()
            )));
        }
        let extents = parse_extent_info_vec(resp.payload.as_deref().unwrap_or_default())
            .ok_or_else(|| StorageError::Internal("invalid SeekResp payload".into()))?;
        extents
            .into_iter()
            .next()
            .ok_or_else(|| StorageError::Internal("SeekResp returned empty result".into()))
    }

    // ── High-level operations ──

    /// Describe a stream by name, returning the resolved StreamId and extent info.
    pub async fn describe_stream_by_name(
        &self,
        name: &str,
        count: u32,
    ) -> Result<(StreamId, Vec<ExtentInfo>), StorageError> {
        let req = Frame::new(
            VariableHeader::DescribeStream {
                request_id: self.alloc_request_id(),
                stream_id: StreamId(0),
                count,
                stream_name: Some(Bytes::from(name.to_owned())),
            },
            None,
        );
        let resp = self.send_request(req).await?;
        Self::check_error(&resp)?;
        if resp.opcode() != Opcode::DescribeStreamResp {
            return Err(StorageError::Internal(format!(
                "expected DescribeStreamResp, got {:?}",
                resp.opcode()
            )));
        }
        let stream_id = resp.stream_id();
        let extents = parse_extent_info_vec(resp.payload.as_deref().unwrap_or_default())
            .ok_or_else(|| StorageError::Internal("invalid DescribeStreamResp payload".into()))?;
        self.cache_primary_from_extents(stream_id, &extents).await;
        Ok((stream_id, extents))
    }

    /// Open a stream by name: describe if it exists, create if absent.
    ///
    /// The creation path exposes the same per-stream settings as `create_stream`.
    /// Returns the `StreamId`. The primary address is cached internally and
    /// can be retrieved via `cached_primary`.
    pub async fn open(
        &self,
        stream_name: &str,
        replication_factor: u16,
        min_extent_capacity: u32,
        max_extent_capacity: u32,
        cache_extents: u32,
        extent_growth_factor: u32,
    ) -> Result<StreamId, StorageError> {
        match self.describe_stream_by_name(stream_name, 1).await {
            Ok((stream_id, _)) => Ok(stream_id),
            Err(StorageError::UnknownStream(_)) => {
                let (stream_id, _, _, _) = self
                    .create_stream(
                        stream_name,
                        replication_factor,
                        min_extent_capacity,
                        max_extent_capacity,
                        cache_extents,
                        extent_growth_factor,
                    )
                    .await?;
                Ok(stream_id)
            }
            Err(e) => Err(e),
        }
    }

    /// Get the cached primary ExtentNode address for a stream.
    pub async fn cached_primary(&self, stream_id: StreamId) -> Option<String> {
        self.primary_cache.lock().await.get(&stream_id).cloned()
    }

    /// Update the primary address cache for a stream.
    async fn cache_primary(&self, stream_id: StreamId, addr: &str) {
        self.primary_cache
            .lock()
            .await
            .insert(stream_id, addr.to_string());
    }

    /// Extract and cache the primary address from extent info.
    async fn cache_primary_from_extents(&self, stream_id: StreamId, extents: &[ExtentInfo]) {
        // Find the active extent's primary replica.
        if let Some(ext) = extents.iter().find(|e| e.state == ExtentState::Active) {
            if let Some(primary) = ext.replicas.iter().find(|r| r.role == 0) {
                self.cache_primary(stream_id, &primary.node_addr).await;
            }
        }
    }
}
