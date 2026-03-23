use std::sync::atomic::{AtomicU32, Ordering};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use common::errors::StorageError;
use common::types::{ErrorCode, ExtentId, ExtentInfo, NodeMetrics, Offset, Opcode, StreamId};
use futures_util::{SinkExt, StreamExt};
use rpc::codec::FrameCodec;
use rpc::frame::Frame;
use rpc::payload::{
    build_client_seal_payload, build_connect_payload, build_create_stream_payload,
    build_describe_extent_payload, build_describe_stream_payload, build_extent_node_seal_payload,
    build_heartbeat_payload, build_string_payload, parse_extent_info_vec,
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
            let payload = &resp.payload;
            if payload.len() >= 2 {
                let code = u16::from_be_bytes([payload[0], payload[1]]);
                let msg = String::from_utf8_lossy(&payload[2..]).to_string();
                let error_code = ErrorCode::from_u16(code);
                return Err(match error_code {
                    Some(ErrorCode::UnknownStream) => StorageError::UnknownStream(resp.stream_id),
                    Some(ErrorCode::ExtentFull) => StorageError::ExtentFull(resp.extent_id),
                    Some(ErrorCode::ExtentSealed) => StorageError::ExtentSealed(resp.extent_id),
                    _ => StorageError::Internal(msg),
                });
            }
            return Err(StorageError::Internal("unknown error".into()));
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
            flags: 0,
            request_id: self.alloc_request_id(),
            stream_id: StreamId(0),
            extent_id: ExtentId(0),
            offset: Offset(0),
            payload: build_create_stream_payload(name, replication_factor),
        };
        let resp = self.send_recv(req).await?;
        Self::check_error(&resp)?;

        // Parse response payload: [extent_id:u32][addr_len:u16][addr]
        let payload = &resp.payload;
        if payload.len() < 6 {
            return Err(StorageError::Internal(
                "CreateStream response payload too short".into(),
            ));
        }
        let extent_id =
            ExtentId(u32::from_be_bytes(payload[0..4].try_into().map_err(
                |_| StorageError::Internal("invalid extent_id bytes".into()),
            )?));
        let addr = rpc::payload::parse_string_payload(&payload[4..]).ok_or_else(|| {
            StorageError::Internal("invalid CreateStream StreamManager response".into())
        })?;

        Ok((resp.stream_id, extent_id, addr))
    }

    /// Append a message to a stream. Returns the assigned offset and byte position.
    pub async fn append(
        &mut self,
        stream_id: StreamId,
        payload: Bytes,
    ) -> Result<AppendResult, StorageError> {
        let req = Frame {
            opcode: Opcode::Append,
            flags: 0,
            request_id: self.alloc_request_id(),
            stream_id,
            extent_id: ExtentId(0),
            offset: Offset(0),
            payload,
        };
        let resp = self.send_recv(req).await?;
        Self::check_error(&resp)?;

        // Parse byte_pos from AppendAck payload (u64 BE).
        let byte_pos = if resp.payload.len() >= 8 {
            u64::from_be_bytes(resp.payload[..8].try_into().unwrap())
        } else {
            0
        };

        Ok(AppendResult {
            offset: resp.offset,
            byte_pos,
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
        let bp_payload = {
            let mut buf = BytesMut::with_capacity(8);
            buf.put_u64(byte_pos);
            buf.freeze()
        };
        let req = Frame {
            opcode: Opcode::Read,
            flags: (count as u8),
            request_id: self.alloc_request_id(),
            stream_id,
            extent_id: ExtentId(0),
            offset,
            payload: bp_payload,
        };
        let resp = self.send_recv(req).await?;
        Self::check_error(&resp)?;

        // Decode payload: [u32 len][bytes] repeated.
        let msg_count = resp.flags as usize;
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
            flags: 0,
            request_id: self.alloc_request_id(),
            stream_id,
            extent_id: ExtentId(0),
            offset: Offset(0),
            payload: Bytes::new(),
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
            flags: 0,
            request_id: self.alloc_request_id(),
            stream_id: StreamId(0),
            extent_id: ExtentId(0),
            offset: Offset(0),
            payload: build_connect_payload(node_id, addr, heartbeat_interval_ms),
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
            flags: 0,
            request_id: self.alloc_request_id(),
            stream_id: StreamId(0),
            extent_id: ExtentId(0),
            offset: Offset(0),
            payload: build_heartbeat_payload(node_id, metrics),
        };
        let resp = self.send_recv(req).await?;
        Self::check_error(&resp)?;
        Ok(())
    }

    /// Send Disconnect to StreamManager.
    pub async fn disconnect_extent_node(&mut self, node_id: &str) -> Result<(), StorageError> {
        let req = Frame {
            opcode: Opcode::Disconnect,
            flags: 0,
            request_id: self.alloc_request_id(),
            stream_id: StreamId(0),
            extent_id: ExtentId(0),
            offset: Offset(0),
            payload: build_string_payload(node_id),
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

    /// Send Seal to StreamManager for client-initiated seal: seal a specific extent
    /// without knowing the committed offset.
    ///
    /// StreamManager will query all EN replicas to determine the committed offset
    /// via quorum algorithm, then seal-and-allocate a new extent.
    ///
    /// Payload: `[extent_id:u32]` (4 bytes).
    ///
    /// Returns (new_extent_id, new_primary_addr).
    pub async fn seal(
        &mut self,
        stream_id: StreamId,
        extent_id: ExtentId,
    ) -> Result<(u64, String), StorageError> {
        let req = Frame {
            opcode: Opcode::Seal,
            flags: 0,
            request_id: self.alloc_request_id(),
            stream_id,
            extent_id: ExtentId(0),
            offset: Offset(0),
            payload: build_client_seal_payload(extent_id.0),
        };
        let resp = self.send_recv(req).await?;
        Self::check_error(&resp)?;
        if resp.opcode != Opcode::SealAck {
            return Err(StorageError::Internal(format!(
                "expected SealAck, got {:?}",
                resp.opcode
            )));
        }

        // Parse response payload: [extent_id:u64][addr_len:u16][addr]
        let payload = &resp.payload;
        if payload.len() < 8 {
            return Err(StorageError::Internal("SealAck payload too short".into()));
        }
        let extent_id = u64::from_be_bytes([
            payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
            payload[7],
        ]);
        let addr = rpc::payload::parse_string_payload(&payload[8..])
            .ok_or_else(|| StorageError::Internal("invalid SealAck addr".into()))?;

        Ok((extent_id, addr))
    }

    /// Send Seal to StreamManager for extent-node-initiated (proactive) seal.
    ///
    /// Used when the primary ExtentNode detects ExtentFull and has already sealed
    /// the extent locally. The `offset` is `base_offset + message_count` — the
    /// committed offset that StreamManager should trust without querying replicas.
    ///
    /// StreamManager will also fire-and-forget seal RPCs to secondary ENs.
    ///
    /// Payload: `[extent_id:u32][offset:u64]` (12 bytes).
    ///
    /// Returns (new_extent_id, new_primary_addr).
    pub async fn extent_node_seal(
        &mut self,
        stream_id: StreamId,
        extent_id: ExtentId,
        offset: u64,
    ) -> Result<(u64, String), StorageError> {
        let req = Frame {
            opcode: Opcode::Seal,
            flags: 0,
            request_id: self.alloc_request_id(),
            stream_id,
            extent_id: ExtentId(0),
            offset: Offset(0),
            payload: build_extent_node_seal_payload(extent_id.0, offset),
        };
        let resp = self.send_recv(req).await?;
        Self::check_error(&resp)?;
        if resp.opcode != Opcode::SealAck {
            return Err(StorageError::Internal(format!(
                "expected SealAck, got {:?}",
                resp.opcode
            )));
        }

        // Parse response payload: [extent_id:u64][addr_len:u16][addr]
        let payload = &resp.payload;
        if payload.len() < 8 {
            return Err(StorageError::Internal("SealAck payload too short".into()));
        }
        let extent_id = u64::from_be_bytes([
            payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
            payload[7],
        ]);
        let addr = rpc::payload::parse_string_payload(&payload[8..])
            .ok_or_else(|| StorageError::Internal("invalid SealAck addr".into()))?;

        Ok((extent_id, addr))
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
            flags: 0,
            request_id: self.alloc_request_id(),
            stream_id,
            extent_id: ExtentId(0),
            offset: Offset(0),
            payload: build_describe_stream_payload(count),
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
            flags: 0,
            request_id: self.alloc_request_id(),
            stream_id,
            extent_id: ExtentId(0),
            offset: Offset(0),
            payload: build_describe_extent_payload(extent_id.0),
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
    ///
    /// - For sealed extents: `base_offset <= offset < base_offset + message_count`.
    /// - For the active extent: `offset >= base_offset` (tail of the stream).
    pub async fn seek(
        &mut self,
        stream_id: StreamId,
        offset: Offset,
    ) -> Result<ExtentInfo, StorageError> {
        let req = Frame {
            opcode: Opcode::Seek,
            flags: 0,
            request_id: self.alloc_request_id(),
            stream_id,
            extent_id: ExtentId(0),
            offset,
            payload: Bytes::new(),
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
