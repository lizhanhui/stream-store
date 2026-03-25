use std::sync::atomic::{AtomicU32, Ordering};

use bytes::{Buf, Bytes};
use common::errors::StorageError;
use common::types::{
    ErrorCode, ExtentId, ExtentInfo, FLAG_OFFSET_PRESENT, NodeMetrics, Offset, Opcode, StreamId,
};
use futures_util::{SinkExt, StreamExt};
use rpc::codec::FrameCodec;
use rpc::frame::Frame;
use rpc::payload::{
    build_connect_payload, build_create_stream_payload, build_heartbeat_payload,
    build_string_payload, parse_extent_info_vec,
};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

/// Result of a successful append: logical offset and byte position within the extent.
/// The byte_pos can be stored in an index stream for O(1) random reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendResult {
    /// Logical offset assigned to this record.
    pub offset: Offset,
    /// Byte position from the beginning of the extent arena.
    pub byte_pos: u64,
}

/// A client for communicating with an Extent Node or Stream Manager.
///
/// Phase 1: simple synchronous request-response over a single connection.
/// No multiplexing or pipelining yet.
pub struct StorageClient {
    framed: Framed<TcpStream, FrameCodec>,
    next_request_id: AtomicU32,
}

impl StorageClient {
    /// Connect to a storage service endpoint.
    pub async fn connect(addr: &str) -> Result<Self, StorageError> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self {
            framed: Framed::new(stream, FrameCodec),
            next_request_id: AtomicU32::new(1),
        })
    }

    fn alloc_request_id(&self) -> u32 {
        self.next_request_id.fetch_add(1, Ordering::Relaxed)
    }

    async fn send_recv(&mut self, frame: Frame) -> Result<Frame, StorageError> {
        self.framed.send(frame).await?;
        match self.framed.next().await {
            Some(Ok(resp)) => Ok(resp),
            Some(Err(e)) => Err(e),
            None => Err(StorageError::Internal("connection closed".into())),
        }
    }

    /// Send a raw frame and return the response. Used by StreamManager to communicate with ExtentNodes.
    pub async fn send_frame(&mut self, frame: Frame) -> Result<Frame, StorageError> {
        self.send_recv(frame).await
    }

    fn check_error(resp: &Frame) -> Result<(), StorageError> {
        if resp.opcode == Opcode::Error {
            let error_code = ErrorCode::from_u16(resp.error_code);
            let msg = String::from_utf8_lossy(&resp.payload).to_string();
            return Err(match error_code {
                Some(ErrorCode::UnknownStream) => StorageError::UnknownStream(resp.stream_id),
                Some(ErrorCode::ExtentFull) => StorageError::ExtentFull(resp.extent_id),
                Some(ErrorCode::ExtentSealed) => StorageError::ExtentSealed(resp.extent_id),
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
        &mut self,
        name: &str,
        replication_factor: u16,
    ) -> Result<(StreamId, ExtentId, String), StorageError> {
        let req = Frame {
            opcode: Opcode::CreateStream,
            request_id: self.alloc_request_id(),
            payload: build_create_stream_payload(name, replication_factor),
            ..Default::default()
        };
        let resp = self.send_recv(req).await?;
        Self::check_error(&resp)?;
        if resp.opcode != Opcode::CreateStreamResp {
            return Err(StorageError::Internal(format!(
                "expected CreateStreamResp, got {:?}",
                resp.opcode
            )));
        }

        // extent_id is in the frame field; payload carries [addr_len:u16][addr]
        let addr = rpc::payload::parse_string_payload(&resp.payload).ok_or_else(|| {
            StorageError::Internal("invalid CreateStreamResp primary_addr payload".into())
        })?;

        Ok((resp.stream_id, resp.extent_id, addr))
    }

    /// Append a message to a stream. Returns the assigned offset and byte position.
    pub async fn append(
        &mut self,
        stream_id: StreamId,
        payload: Bytes,
    ) -> Result<AppendResult, StorageError> {
        let req = Frame {
            opcode: Opcode::Append,
            request_id: self.alloc_request_id(),
            stream_id,
            payload,
            ..Default::default()
        };
        let resp = self.send_recv(req).await?;
        Self::check_error(&resp)?;

        Ok(AppendResult {
            offset: resp.offset,
            byte_pos: resp.byte_pos,
        })
    }

    /// Read `count` messages from a stream starting at `offset` and `byte_pos`.
    ///
    /// The `byte_pos` indicates the byte position within the extent arena to start
    /// reading from. When byte_pos=0, reads from the beginning of the extent.
    pub async fn read(
        &mut self,
        stream_id: StreamId,
        offset: Offset,
        byte_pos: u64,
        count: u16,
    ) -> Result<Vec<Bytes>, StorageError> {
        let req = Frame {
            opcode: Opcode::Read,
            request_id: self.alloc_request_id(),
            stream_id,
            offset,
            byte_pos,
            count: count as u32,
            ..Default::default()
        };
        let resp = self.send_recv(req).await?;
        Self::check_error(&resp)?;

        // Decode payload: [u32 len][bytes] repeated.
        let msg_count = resp.count as usize;
        let mut messages = Vec::with_capacity(msg_count);
        let mut buf = &resp.payload[..];

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
    pub async fn query_offset(&mut self, stream_id: StreamId) -> Result<Offset, StorageError> {
        let req = Frame {
            opcode: Opcode::QueryOffset,
            request_id: self.alloc_request_id(),
            stream_id,
            ..Default::default()
        };
        let resp = self.send_recv(req).await?;
        Self::check_error(&resp)?;
        Ok(resp.offset)
    }

    // ── Lifecycle operations (ExtentNode -> StreamManager) ──

    /// Send Connect to StreamManager to register an ExtentNode node.
    pub async fn connect_extent_node(
        &mut self,
        node_id: &str,
        addr: &str,
        heartbeat_interval_ms: u32,
    ) -> Result<(), StorageError> {
        let req = Frame {
            opcode: Opcode::Connect,
            request_id: self.alloc_request_id(),
            payload: build_connect_payload(node_id, addr, heartbeat_interval_ms),
            ..Default::default()
        };
        let resp = self.send_recv(req).await?;
        Self::check_error(&resp)?;
        if resp.opcode != Opcode::ConnectAck {
            return Err(StorageError::Internal(format!(
                "expected ConnectAck, got {:?}",
                resp.opcode
            )));
        }
        Ok(())
    }

    /// Send Heartbeat to StreamManager with runtime metrics.
    pub async fn heartbeat(
        &mut self,
        node_id: &str,
        metrics: &NodeMetrics,
    ) -> Result<(), StorageError> {
        let req = Frame {
            opcode: Opcode::Heartbeat,
            request_id: self.alloc_request_id(),
            payload: build_heartbeat_payload(node_id, metrics),
            ..Default::default()
        };
        let resp = self.send_recv(req).await?;
        Self::check_error(&resp)?;
        Ok(())
    }

    /// Send Disconnect to StreamManager.
    pub async fn disconnect_extent_node(&mut self, node_id: &str) -> Result<(), StorageError> {
        let req = Frame {
            opcode: Opcode::Disconnect,
            request_id: self.alloc_request_id(),
            payload: build_string_payload(node_id),
            ..Default::default()
        };
        let resp = self.send_recv(req).await?;
        Self::check_error(&resp)?;
        if resp.opcode != Opcode::DisconnectAck {
            return Err(StorageError::Internal(format!(
                "expected DisconnectAck, got {:?}",
                resp.opcode
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
        &mut self,
        stream_id: StreamId,
        extent_id: ExtentId,
        committed_offset: Option<u64>,
    ) -> Result<(u32, String), StorageError> {
        let (flags, offset) = match committed_offset {
            Some(off) => (FLAG_OFFSET_PRESENT, Offset(off)),
            None => (0, Offset(0)),
        };
        let req = Frame {
            opcode: Opcode::Seal,
            flags,
            request_id: self.alloc_request_id(),
            stream_id,
            extent_id,
            offset,
            ..Default::default()
        };
        let resp = self.send_recv(req).await?;
        Self::check_error(&resp)?;
        if resp.opcode != Opcode::SealAck {
            return Err(StorageError::Internal(format!(
                "expected SealAck, got {:?}",
                resp.opcode
            )));
        }

        // SealAck with FLAG_NEW_EXTENT_PRESENT: new_extent_id in count field, primary_addr in payload
        let new_extent_id = resp.count;
        let addr = String::from_utf8_lossy(&resp.payload).to_string();

        Ok((new_extent_id, addr))
    }

    // ── Management operations (StreamManager) ──

    /// Describe a stream's extents with replica info and node liveness.
    ///
    /// - `count = 0`: return all extents (latest to earliest).
    /// - `count = 1`: return the latest (active/mutable) extent only.
    /// - `count = N`: return at most N extents from latest to earliest.
    pub async fn describe_stream(
        &mut self,
        stream_id: StreamId,
        count: u32,
    ) -> Result<Vec<ExtentInfo>, StorageError> {
        let req = Frame {
            opcode: Opcode::DescribeStream,
            request_id: self.alloc_request_id(),
            stream_id,
            count,
            ..Default::default()
        };
        let resp = self.send_recv(req).await?;
        Self::check_error(&resp)?;
        if resp.opcode != Opcode::DescribeStreamResp {
            return Err(StorageError::Internal(format!(
                "expected DescribeStreamResp, got {:?}",
                resp.opcode
            )));
        }
        parse_extent_info_vec(&resp.payload)
            .ok_or_else(|| StorageError::Internal("invalid DescribeStreamResp payload".into()))
    }

    /// Describe a single extent with replica info and node liveness.
    pub async fn describe_extent(
        &mut self,
        stream_id: StreamId,
        extent_id: ExtentId,
    ) -> Result<ExtentInfo, StorageError> {
        let req = Frame {
            opcode: Opcode::DescribeExtent,
            request_id: self.alloc_request_id(),
            stream_id,
            extent_id,
            ..Default::default()
        };
        let resp = self.send_recv(req).await?;
        Self::check_error(&resp)?;
        if resp.opcode != Opcode::DescribeExtentResp {
            return Err(StorageError::Internal(format!(
                "expected DescribeExtentResp, got {:?}",
                resp.opcode
            )));
        }
        let extents = parse_extent_info_vec(&resp.payload)
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
        &mut self,
        stream_id: StreamId,
        offset: Offset,
    ) -> Result<ExtentInfo, StorageError> {
        let req = Frame {
            opcode: Opcode::Seek,
            request_id: self.alloc_request_id(),
            stream_id,
            offset,
            ..Default::default()
        };
        let resp = self.send_recv(req).await?;
        Self::check_error(&resp)?;
        if resp.opcode != Opcode::SeekResp {
            return Err(StorageError::Internal(format!(
                "expected SeekResp, got {:?}",
                resp.opcode
            )));
        }
        let extents = parse_extent_info_vec(&resp.payload)
            .ok_or_else(|| StorageError::Internal("invalid SeekResp payload".into()))?;
        extents
            .into_iter()
            .next()
            .ok_or_else(|| StorageError::Internal("SeekResp returned empty result".into()))
    }
}
