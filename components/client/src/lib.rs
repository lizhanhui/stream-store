use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use bytes::{Buf, Bytes};
use common::config::{RPC_CONNECT_TIMEOUT, RPC_REQUEST_TIMEOUT};
use common::errors::StorageError;
use common::types::{ErrorCode, ExtentId, ExtentInfo, NodeMetrics, Offset, Opcode, StreamId};
use futures_util::{SinkExt, StreamExt};
use rpc::codec::FrameCodec;
use rpc::frame::{Frame, VariableHeader};
use rpc::payload::{
    build_connect_payload, build_create_stream_payload, build_heartbeat_payload,
    build_string_payload, parse_extent_info_vec,
};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::warn;

/// Result of a successful append: the logical offset assigned to this record.
/// The server-side index stream handles byte position tracking internally.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendResult {
    /// Logical offset assigned to this record.
    pub offset: Offset,
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
pub struct StorageClient {
    inner: Arc<Inner>,
    _reader_handle: JoinHandle<()>,
    _writer_handle: JoinHandle<()>,
}

impl Drop for StorageClient {
    fn drop(&mut self) {
        self._reader_handle.abort();
        self._writer_handle.abort();
    }
}

impl StorageClient {
    /// Connect to a storage service endpoint.
    pub async fn connect(addr: &str) -> Result<Self, StorageError> {
        let stream = tokio::time::timeout(RPC_CONNECT_TIMEOUT, TcpStream::connect(addr))
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
                warn!("StorageClient writer error: {e}");
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
                    warn!("StorageClient reader error: {e}");
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
        match tokio::time::timeout(RPC_REQUEST_TIMEOUT, rx).await {
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

    fn check_error(resp: &Frame) -> Result<(), StorageError> {
        if resp.opcode() == Opcode::Error {
            let error_code = ErrorCode::from_u16(resp.error_code());
            let msg =
                String::from_utf8_lossy(resp.payload.as_deref().unwrap_or_default()).to_string();
            return Err(match error_code {
                Some(ErrorCode::UnknownStream) => StorageError::UnknownStream(resp.stream_id()),
                Some(ErrorCode::ExtentFull) => StorageError::ExtentFull(resp.extent_id()),
                Some(ErrorCode::ExtentSealed) => StorageError::ExtentSealed(resp.extent_id()),
                _ => StorageError::Internal(msg),
            });
        }
        Ok(())
    }

    /// Create a new stream on the StreamManager.
    /// Payload carries stream name and per-stream replication factor.
    /// If replication_factor=0, the StreamManager uses its default.
    /// Returns (StreamId, ExtentId, ExtentNode address for the first extent).
    pub async fn create_stream(
        &self,
        name: &str,
        replication_factor: u16,
    ) -> Result<(StreamId, ExtentId, String), StorageError> {
        let req = Frame::new(
            VariableHeader::CreateStream {
                request_id: self.alloc_request_id(),
            },
            Some(build_create_stream_payload(name, replication_factor)),
        );
        let resp = self.send_request(req).await?;
        Self::check_error(&resp)?;
        if resp.opcode() != Opcode::CreateStreamResp {
            return Err(StorageError::Internal(format!(
                "expected CreateStreamResp, got {:?}",
                resp.opcode()
            )));
        }

        // extent_id is in the frame field; payload carries [addr_len:u16][addr]
        let addr = rpc::payload::parse_string_payload(resp.payload.as_deref().unwrap_or_default())
            .ok_or_else(|| {
                StorageError::Internal("invalid CreateStreamResp primary_addr payload".into())
            })?;

        Ok((resp.stream_id(), resp.extent_id(), addr))
    }

    /// Append a message to a stream. Returns the assigned offset.
    pub async fn append(
        &self,
        stream_id: StreamId,
        extent_id: ExtentId,
        payload: Bytes,
    ) -> Result<AppendResult, StorageError> {
        let req = Frame::new(
            VariableHeader::Append {
                request_id: self.alloc_request_id(),
                stream_id,
                extent_id,
            },
            Some(payload),
        );
        let resp = self.send_request(req).await?;
        Self::check_error(&resp)?;

        Ok(AppendResult {
            offset: resp.offset(),
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
    ///   already sealed the extent locally (e.g. arena full).
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
            Ok((new_eid, addr))
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
        parse_extent_info_vec(resp.payload.as_deref().unwrap_or_default())
            .ok_or_else(|| StorageError::Internal("invalid DescribeStreamResp payload".into()))
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
}
