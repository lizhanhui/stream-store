use bytes::{Buf, BufMut, Bytes, BytesMut};
use common::errors::StorageError;
use common::types::{
    Epoch, ErrorCode, ExtentId, FLAG_DESCRIBE_STREAM_BY_NAME, FLAG_EPOCH_PRESENT,
    FLAG_EXTENT_PROGRESS, FLAG_EXTENT_SEALED, FLAG_FORWARD_APPEND, FLAG_FORWARD_CHECKSUM,
    FLAG_FORWARD_INIT_EXTENT, FLAG_NEW_EXTENT_PRESENT, FLAG_OFFSET_PRESENT, FLAG_RESPONSE_ERROR,
    FLAG_START_OFFSET_PRESENT, HEADER_LEN, MAGIC, Offset, Opcode, PROTOCOL_VERSION, StreamId,
};

/// Fixed header fields present in every frame on the wire.
///
/// During encoding, `flags` is computed from `Option` fields in the variable
/// header (eliminating stale-flag bugs). During decoding, `flags` and `version`
/// are populated from the wire bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixedHeader {
    pub opcode: Opcode,
    /// Protocol version. Set from the wire on decode; defaults to PROTOCOL_VERSION on encode.
    pub version: u8,
    /// Flags byte. Computed from Option fields on encode; set from wire on decode.
    pub flags: u8,
}

/// Opcode-specific variable header.
///
/// Each variant contains exactly the fields that are valid for that opcode,
/// enforced at compile time. Flag-dependent fields use `Option<T>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VariableHeader {
    Append {
        request_id: u32,
        stream_id: StreamId,
        epoch: Epoch,
    },
    AppendAck {
        request_id: u32,
        stream_id: StreamId,
        epoch: Epoch,
        extent_id: ExtentId,
        offset: Offset,
    },
    AppendAckError {
        request_id: u32,
        stream_id: StreamId,
        epoch: Epoch,
        extent_id: ExtentId,
        error_code: ErrorCode,
    },
    Read {
        request_id: u32,
        stream_id: StreamId,
        extent_id: ExtentId,
        offset: Offset,
        count: u32,
    },
    ReadResp {
        request_id: u32,
        stream_id: StreamId,
        offset: Offset,
        count: u32,
    },
    ReadRespError {
        request_id: u32,
        stream_id: StreamId,
        extent_id: ExtentId,
        offset: Offset,
        error_code: ErrorCode,
    },
    Seal {
        request_id: u32,
        stream_id: StreamId,
        extent_id: ExtentId,
        offset: Option<Offset>,
        start_offset: Option<u64>,
        /// Epoch for epoch-based seal. When present (FLAG_EPOCH_PRESENT),
        /// the seal targets the active extent at this epoch.
        epoch: Option<Epoch>,
    },
    SealAck {
        request_id: u32,
        stream_id: StreamId,
        extent_id: ExtentId,
        offset: Offset,
        new_extent_id: Option<ExtentId>,
        primary_addr: Option<Bytes>,
        /// New epoch after an epoch bump (FLAG_EPOCH_PRESENT on SealAck).
        epoch: Option<Epoch>,
    },
    SealAckError {
        request_id: u32,
        stream_id: StreamId,
        extent_id: ExtentId,
        error_code: ErrorCode,
    },
    CreateStream {
        request_id: u32,
        stream_name: Bytes,
        replication_factor: u16,
        extent_capacity: u32,
        cache_extents: u32,
    },
    CreateStreamResp {
        request_id: u32,
        stream_id: StreamId,
        extent_id: ExtentId,
        epoch: Epoch,
        primary_addr: Bytes,
    },
    CreateStreamRespError {
        request_id: u32,
        error_code: ErrorCode,
    },
    QueryOffset {
        request_id: u32,
        stream_id: StreamId,
    },
    QueryOffsetResp {
        request_id: u32,
        stream_id: StreamId,
        offset: Offset,
    },
    QueryOffsetRespError {
        request_id: u32,
        stream_id: StreamId,
        error_code: ErrorCode,
    },
    Connect {
        request_id: u32,
    },
    ConnectAck {
        request_id: u32,
    },
    ConnectAckError {
        request_id: u32,
        error_code: ErrorCode,
    },
    Disconnect {
        request_id: u32,
    },
    DisconnectAck {
        request_id: u32,
    },
    DisconnectAckError {
        request_id: u32,
        error_code: ErrorCode,
    },
    Heartbeat {
        request_id: u32,
    },
    HeartbeatError {
        request_id: u32,
        error_code: ErrorCode,
    },
    RegisterExtent {
        request_id: u32,
        stream_id: StreamId,
        extent_id: ExtentId,
        role: u8,
        replication_factor: u16,
        /// Stream epoch for this extent registration.
        epoch: Epoch,
        /// Per-stream extent arena capacity in bytes.
        extent_capacity: u32,
        /// Maximum extents to retain in memory for this stream. 0 = no limit.
        cache_extents: u32,
    },
    RegisterExtentAck {
        request_id: u32,
        stream_id: StreamId,
        extent_id: ExtentId,
    },
    RegisterExtentAckError {
        request_id: u32,
        stream_id: StreamId,
        extent_id: ExtentId,
        error_code: ErrorCode,
    },
    Watermark {
        stream_id: StreamId,
        extent_id: ExtentId,
        offset: Offset,
    },
    /// Extent sealed: Primary EN sealed an extent and created a new one (UpdateExtent, flag=0x00).
    /// Fire-and-forget: no request_id needed.
    UpdateExtentSealed {
        stream_id: StreamId,
        epoch: Epoch,
        sealed_extent_id: ExtentId,
        end_offset: Offset,
        new_extent_id: ExtentId,
    },
    /// Active extent progress report (UpdateExtent, flag=0x01).
    /// Fire-and-forget periodic update of current offset for observability.
    UpdateExtentProgress {
        stream_id: StreamId,
        epoch: Epoch,
        extent_id: ExtentId,
        current_offset: Offset,
    },
    /// SM queries an EN for all extents it holds for a stream at a given epoch (0x19).
    ReportExtents {
        request_id: u32,
        stream_id: StreamId,
        epoch: Epoch,
    },
    /// EN response to ReportExtents with extent state for reconciliation (0x1A).
    ReportExtentsResp {
        request_id: u32,
        stream_id: StreamId,
        epoch: Epoch,
    },
    ReportExtentsRespError {
        request_id: u32,
        stream_id: StreamId,
        epoch: Epoch,
        error_code: ErrorCode,
    },
    /// Per-record replication (Forward, flag=0x00).
    /// Carries byte_pos so the secondary writes each record at the exact same
    /// arena position as the primary.
    /// Fire-and-forget: no request_id; secondary responds with cumulative Watermark.
    Forward {
        stream_id: StreamId,
        extent_id: ExtentId,
        epoch: Epoch,
        offset: Offset,
        byte_pos: u64,
    },
    /// Init-extent notification (Forward, flag=0x01). No payload, no response.
    /// Sent once by primary when it starts using a new extent,
    /// before any Forward frames for that extent. Carries extent metadata
    /// so the secondary can create the extent with the correct capacity.
    ForwardInitExtent {
        stream_id: StreamId,
        extent_id: ExtentId,
        epoch: Epoch,
        start_offset: Offset,
        extent_capacity: u32,
        cache_extents: u32,
    },
    /// CRC32 checksum verification (Forward, flag=0x02). Fire-and-forget.
    /// Sent by primary after sealing an extent so secondaries can verify
    /// data integrity of the replicated extent.
    ForwardChecksum {
        stream_id: StreamId,
        extent_id: ExtentId,
        checksum: u32,
        committed_bytes: u64,
    },
    StreamManagerMembershipChange,
    DescribeStream {
        request_id: u32,
        stream_id: StreamId,
        count: u32,
        /// Stream name for name-based lookup (FLAG_DESCRIBE_STREAM_BY_NAME).
        stream_name: Option<Bytes>,
    },
    DescribeStreamResp {
        request_id: u32,
        stream_id: StreamId,
    },
    DescribeStreamRespError {
        request_id: u32,
        stream_id: StreamId,
        error_code: ErrorCode,
    },
    DescribeExtent {
        request_id: u32,
        stream_id: StreamId,
        extent_id: ExtentId,
    },
    DescribeExtentResp {
        request_id: u32,
        stream_id: StreamId,
    },
    DescribeExtentRespError {
        request_id: u32,
        stream_id: StreamId,
        extent_id: ExtentId,
        error_code: ErrorCode,
    },
    Seek {
        request_id: u32,
        stream_id: StreamId,
        offset: Offset,
    },
    SeekResp {
        request_id: u32,
        stream_id: StreamId,
        offset: Offset,
    },
    SeekRespError {
        request_id: u32,
        stream_id: StreamId,
        offset: Offset,
        error_code: ErrorCode,
    },
}

/// A wire protocol frame.
///
/// Layout: 8-byte fixed header + opcode-specific variable header + optional payload.
///
/// Fixed header:
/// ```text
/// Magic(1) | Version(1) | Opcode(1) | Flags(1) | RemainingLength(4)
/// ```
///
/// RemainingLength = total bytes of variable header + payload section that follow
/// the fixed header. The decoder reads 8 bytes, extracts remaining_length, then
/// waits for exactly that many more bytes.
///
/// Variable header fields and payload presence are determined by the Opcode (and
/// sometimes Flags). See `encode()` and `decode()` for per-opcode layouts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub header: FixedHeader,
    pub variable_header: VariableHeader,
    pub payload: Option<Bytes>,
}

impl Default for Frame {
    fn default() -> Self {
        Frame {
            header: FixedHeader {
                opcode: Opcode::ConnectAck,
                version: PROTOCOL_VERSION,
                flags: 0,
            },
            variable_header: VariableHeader::ConnectAck { request_id: 0 },
            payload: None,
        }
    }
}

impl Frame {
    /// Create a new frame from a variable header and optional payload.
    pub fn new(variable_header: VariableHeader, payload: Option<Bytes>) -> Self {
        let opcode = variable_header.opcode();
        Frame {
            header: FixedHeader {
                opcode,
                version: PROTOCOL_VERSION,
                flags: 0, // computed on encode
            },
            variable_header,
            payload,
        }
    }

    /// Get the opcode for this frame.
    pub fn opcode(&self) -> Opcode {
        self.header.opcode
    }

    /// Get the request_id for this frame (0 for opcodes without request_id).
    pub fn request_id(&self) -> u32 {
        match &self.variable_header {
            VariableHeader::Append { request_id, .. }
            | VariableHeader::AppendAck { request_id, .. }
            | VariableHeader::AppendAckError { request_id, .. }
            | VariableHeader::Read { request_id, .. }
            | VariableHeader::ReadResp { request_id, .. }
            | VariableHeader::ReadRespError { request_id, .. }
            | VariableHeader::Seal { request_id, .. }
            | VariableHeader::SealAck { request_id, .. }
            | VariableHeader::SealAckError { request_id, .. }
            | VariableHeader::CreateStream { request_id, .. }
            | VariableHeader::CreateStreamResp { request_id, .. }
            | VariableHeader::CreateStreamRespError { request_id, .. }
            | VariableHeader::QueryOffset { request_id, .. }
            | VariableHeader::QueryOffsetResp { request_id, .. }
            | VariableHeader::QueryOffsetRespError { request_id, .. }
            | VariableHeader::Connect { request_id }
            | VariableHeader::ConnectAck { request_id }
            | VariableHeader::ConnectAckError { request_id, .. }
            | VariableHeader::Disconnect { request_id }
            | VariableHeader::DisconnectAck { request_id }
            | VariableHeader::DisconnectAckError { request_id, .. }
            | VariableHeader::Heartbeat { request_id }
            | VariableHeader::HeartbeatError { request_id, .. }
            | VariableHeader::RegisterExtent { request_id, .. }
            | VariableHeader::RegisterExtentAck { request_id, .. }
            | VariableHeader::RegisterExtentAckError { request_id, .. }
            | VariableHeader::ReportExtents { request_id, .. }
            | VariableHeader::ReportExtentsResp { request_id, .. }
            | VariableHeader::ReportExtentsRespError { request_id, .. }
            | VariableHeader::DescribeStream { request_id, .. }
            | VariableHeader::DescribeStreamResp { request_id, .. }
            | VariableHeader::DescribeStreamRespError { request_id, .. }
            | VariableHeader::DescribeExtent { request_id, .. }
            | VariableHeader::DescribeExtentResp { request_id, .. }
            | VariableHeader::DescribeExtentRespError { request_id, .. }
            | VariableHeader::Seek { request_id, .. }
            | VariableHeader::SeekResp { request_id, .. }
            | VariableHeader::SeekRespError { request_id, .. } => *request_id,
            VariableHeader::Watermark { .. }
            | VariableHeader::Forward { .. }
            | VariableHeader::ForwardInitExtent { .. }
            | VariableHeader::ForwardChecksum { .. }
            | VariableHeader::UpdateExtentSealed { .. }
            | VariableHeader::UpdateExtentProgress { .. }
            | VariableHeader::StreamManagerMembershipChange => 0,
        }
    }

    /// Get the stream_id for this frame (StreamId(0) for opcodes without stream_id).
    pub fn stream_id(&self) -> StreamId {
        match &self.variable_header {
            VariableHeader::Append { stream_id, .. }
            | VariableHeader::AppendAck { stream_id, .. }
            | VariableHeader::AppendAckError { stream_id, .. }
            | VariableHeader::Read { stream_id, .. }
            | VariableHeader::ReadResp { stream_id, .. }
            | VariableHeader::ReadRespError { stream_id, .. }
            | VariableHeader::Seal { stream_id, .. }
            | VariableHeader::SealAck { stream_id, .. }
            | VariableHeader::SealAckError { stream_id, .. }
            | VariableHeader::CreateStreamResp { stream_id, .. }
            | VariableHeader::QueryOffset { stream_id, .. }
            | VariableHeader::QueryOffsetResp { stream_id, .. }
            | VariableHeader::QueryOffsetRespError { stream_id, .. }
            | VariableHeader::RegisterExtentAck { stream_id, .. }
            | VariableHeader::RegisterExtentAckError { stream_id, .. }
            | VariableHeader::RegisterExtent { stream_id, .. }
            | VariableHeader::Watermark { stream_id, .. }
            | VariableHeader::Forward { stream_id, .. }
            | VariableHeader::ForwardInitExtent { stream_id, .. }
            | VariableHeader::ForwardChecksum { stream_id, .. }
            | VariableHeader::UpdateExtentSealed { stream_id, .. }
            | VariableHeader::UpdateExtentProgress { stream_id, .. }
            | VariableHeader::ReportExtents { stream_id, .. }
            | VariableHeader::ReportExtentsResp { stream_id, .. }
            | VariableHeader::ReportExtentsRespError { stream_id, .. }
            | VariableHeader::DescribeStream { stream_id, .. }
            | VariableHeader::DescribeStreamResp { stream_id, .. }
            | VariableHeader::DescribeStreamRespError { stream_id, .. }
            | VariableHeader::DescribeExtent { stream_id, .. }
            | VariableHeader::DescribeExtentResp { stream_id, .. }
            | VariableHeader::DescribeExtentRespError { stream_id, .. }
            | VariableHeader::Seek { stream_id, .. }
            | VariableHeader::SeekResp { stream_id, .. }
            | VariableHeader::SeekRespError { stream_id, .. } => *stream_id,
            _ => StreamId(0),
        }
    }

    /// Get the offset for this frame (Offset(0) for opcodes without offset).
    pub fn offset(&self) -> Offset {
        match &self.variable_header {
            VariableHeader::AppendAck { offset, .. }
            | VariableHeader::ReadResp { offset, .. }
            | VariableHeader::ReadRespError { offset, .. }
            | VariableHeader::SealAck { offset, .. }
            | VariableHeader::QueryOffsetResp { offset, .. }
            | VariableHeader::Watermark { offset, .. }
            | VariableHeader::Forward { offset, .. }
            | VariableHeader::Seek { offset, .. }
            | VariableHeader::SeekResp { offset, .. }
            | VariableHeader::SeekRespError { offset, .. } => *offset,
            VariableHeader::Read { offset, .. } => *offset,
            VariableHeader::Seal { offset, .. } => offset.unwrap_or(Offset(0)),
            _ => Offset(0),
        }
    }

    /// Get the extent_id for this frame (ExtentId(0) for opcodes without extent_id).
    pub fn extent_id(&self) -> ExtentId {
        match &self.variable_header {
            VariableHeader::AppendAck { extent_id, .. }
            | VariableHeader::AppendAckError { extent_id, .. }
            | VariableHeader::Read { extent_id, .. }
            | VariableHeader::ReadRespError { extent_id, .. }
            | VariableHeader::Seal { extent_id, .. }
            | VariableHeader::SealAck { extent_id, .. }
            | VariableHeader::SealAckError { extent_id, .. }
            | VariableHeader::CreateStreamResp { extent_id, .. }
            | VariableHeader::RegisterExtentAck { extent_id, .. }
            | VariableHeader::RegisterExtentAckError { extent_id, .. }
            | VariableHeader::RegisterExtent { extent_id, .. }
            | VariableHeader::Forward { extent_id, .. }
            | VariableHeader::ForwardInitExtent { extent_id, .. }
            | VariableHeader::ForwardChecksum { extent_id, .. }
            | VariableHeader::Watermark { extent_id, .. }
            | VariableHeader::DescribeExtent { extent_id, .. }
            | VariableHeader::DescribeExtentRespError { extent_id, .. } => *extent_id,
            _ => ExtentId(0),
        }
    }

    /// Get the epoch for this frame (Epoch(0) for opcodes without epoch).
    pub fn epoch(&self) -> Epoch {
        match &self.variable_header {
            VariableHeader::Append { epoch, .. }
            | VariableHeader::AppendAck { epoch, .. }
            | VariableHeader::AppendAckError { epoch, .. }
            | VariableHeader::CreateStreamResp { epoch, .. } => *epoch,
            VariableHeader::Seal { epoch, .. } | VariableHeader::SealAck { epoch, .. } => {
                epoch.unwrap_or(Epoch(0))
            }
            VariableHeader::RegisterExtent { epoch, .. } => *epoch,
            VariableHeader::UpdateExtentSealed { epoch, .. }
            | VariableHeader::UpdateExtentProgress { epoch, .. } => *epoch,
            VariableHeader::Forward { epoch, .. }
            | VariableHeader::ForwardInitExtent { epoch, .. } => *epoch,
            VariableHeader::ReportExtents { epoch, .. }
            | VariableHeader::ReportExtentsResp { epoch, .. }
            | VariableHeader::ReportExtentsRespError { epoch, .. } => *epoch,
            _ => Epoch(0),
        }
    }

    /// Get the count for this frame (0 for opcodes without count).
    pub fn count(&self) -> u32 {
        match &self.variable_header {
            VariableHeader::Read { count, .. }
            | VariableHeader::ReadResp { count, .. }
            | VariableHeader::DescribeStream { count, .. } => *count,
            _ => 0,
        }
    }

    /// Get the error_code for this frame (0 for non-error frames).
    pub fn error_code(&self) -> u16 {
        match &self.variable_header {
            VariableHeader::AppendAckError { error_code, .. }
            | VariableHeader::ReadRespError { error_code, .. }
            | VariableHeader::SealAckError { error_code, .. }
            | VariableHeader::CreateStreamRespError { error_code, .. }
            | VariableHeader::QueryOffsetRespError { error_code, .. }
            | VariableHeader::ConnectAckError { error_code, .. }
            | VariableHeader::DisconnectAckError { error_code, .. }
            | VariableHeader::HeartbeatError { error_code, .. }
            | VariableHeader::RegisterExtentAckError { error_code, .. }
            | VariableHeader::ReportExtentsRespError { error_code, .. }
            | VariableHeader::DescribeStreamRespError { error_code, .. }
            | VariableHeader::DescribeExtentRespError { error_code, .. }
            | VariableHeader::SeekRespError { error_code, .. } => *error_code as u16,
            _ => 0,
        }
    }

    pub fn is_error_response(&self) -> bool {
        self.flags() & FLAG_RESPONSE_ERROR != 0
    }

    /// Get the flags byte for this frame on the wire.
    ///
    /// For Append, Seal, and SealAck, flags are computed from `Option` fields
    /// (eliminating stale-flag bugs). For other opcodes, returns `header.flags`.
    pub fn flags(&self) -> u8 {
        let computed = match &self.variable_header {
            VariableHeader::AppendAckError { .. }
            | VariableHeader::ReadRespError { .. }
            | VariableHeader::SealAckError { .. }
            | VariableHeader::CreateStreamRespError { .. }
            | VariableHeader::QueryOffsetRespError { .. }
            | VariableHeader::ConnectAckError { .. }
            | VariableHeader::DisconnectAckError { .. }
            | VariableHeader::HeartbeatError { .. }
            | VariableHeader::RegisterExtentAckError { .. }
            | VariableHeader::ReportExtentsRespError { .. }
            | VariableHeader::DescribeStreamRespError { .. }
            | VariableHeader::DescribeExtentRespError { .. }
            | VariableHeader::SeekRespError { .. } => FLAG_RESPONSE_ERROR,
            VariableHeader::Seal {
                offset,
                start_offset,
                epoch,
                ..
            } => {
                let mut f = 0u8;
                if offset.is_some() {
                    f |= FLAG_OFFSET_PRESENT;
                }
                if start_offset.is_some() {
                    f |= FLAG_START_OFFSET_PRESENT;
                }
                if epoch.is_some() {
                    f |= FLAG_EPOCH_PRESENT;
                }
                f
            }
            VariableHeader::SealAck {
                new_extent_id,
                epoch,
                ..
            } => {
                let mut f = 0u8;
                if new_extent_id.is_some() {
                    f |= FLAG_NEW_EXTENT_PRESENT;
                }
                if epoch.is_some() {
                    f |= FLAG_EPOCH_PRESENT;
                }
                f
            }
            VariableHeader::UpdateExtentSealed { .. } => FLAG_EXTENT_SEALED,
            VariableHeader::UpdateExtentProgress { .. } => FLAG_EXTENT_PROGRESS,
            VariableHeader::Forward { .. } => FLAG_FORWARD_APPEND,
            VariableHeader::ForwardInitExtent { .. } => FLAG_FORWARD_INIT_EXTENT,
            VariableHeader::ForwardChecksum { .. } => FLAG_FORWARD_CHECKSUM,
            VariableHeader::DescribeStream { stream_name, .. } => {
                if stream_name.is_some() {
                    FLAG_DESCRIBE_STREAM_BY_NAME
                } else {
                    0
                }
            }
            _ => 0,
        };
        self.header.flags | computed
    }

    pub fn append_ack_error(
        request_id: u32,
        stream_id: StreamId,
        epoch: Epoch,
        extent_id: ExtentId,
        error_code: ErrorCode,
        message: &str,
    ) -> Frame {
        Frame::new(
            VariableHeader::AppendAckError {
                request_id,
                stream_id,
                epoch,
                extent_id,
                error_code,
            },
            Some(Bytes::copy_from_slice(message.as_bytes())),
        )
    }

    pub fn read_resp_error(
        request_id: u32,
        stream_id: StreamId,
        extent_id: ExtentId,
        offset: Offset,
        error_code: ErrorCode,
        message: &str,
    ) -> Frame {
        Frame::new(
            VariableHeader::ReadRespError {
                request_id,
                stream_id,
                extent_id,
                offset,
                error_code,
            },
            Some(Bytes::copy_from_slice(message.as_bytes())),
        )
    }

    pub fn seal_ack_error(
        request_id: u32,
        stream_id: StreamId,
        extent_id: ExtentId,
        error_code: ErrorCode,
        message: &str,
    ) -> Frame {
        Frame::new(
            VariableHeader::SealAckError {
                request_id,
                stream_id,
                extent_id,
                error_code,
            },
            Some(Bytes::copy_from_slice(message.as_bytes())),
        )
    }

    pub fn create_stream_resp_error(
        request_id: u32,
        error_code: ErrorCode,
        message: &str,
    ) -> Frame {
        Frame::new(
            VariableHeader::CreateStreamRespError {
                request_id,
                error_code,
            },
            Some(Bytes::copy_from_slice(message.as_bytes())),
        )
    }

    pub fn query_offset_resp_error(
        request_id: u32,
        stream_id: StreamId,
        error_code: ErrorCode,
        message: &str,
    ) -> Frame {
        Frame::new(
            VariableHeader::QueryOffsetRespError {
                request_id,
                stream_id,
                error_code,
            },
            Some(Bytes::copy_from_slice(message.as_bytes())),
        )
    }

    pub fn connect_ack_error(request_id: u32, error_code: ErrorCode, message: &str) -> Frame {
        Frame::new(
            VariableHeader::ConnectAckError {
                request_id,
                error_code,
            },
            Some(Bytes::copy_from_slice(message.as_bytes())),
        )
    }

    pub fn disconnect_ack_error(request_id: u32, error_code: ErrorCode, message: &str) -> Frame {
        Frame::new(
            VariableHeader::DisconnectAckError {
                request_id,
                error_code,
            },
            Some(Bytes::copy_from_slice(message.as_bytes())),
        )
    }

    pub fn heartbeat_error(request_id: u32, error_code: ErrorCode, message: &str) -> Frame {
        Frame::new(
            VariableHeader::HeartbeatError {
                request_id,
                error_code,
            },
            Some(Bytes::copy_from_slice(message.as_bytes())),
        )
    }

    pub fn register_extent_ack_error(
        request_id: u32,
        stream_id: StreamId,
        extent_id: ExtentId,
        error_code: ErrorCode,
        message: &str,
    ) -> Frame {
        Frame::new(
            VariableHeader::RegisterExtentAckError {
                request_id,
                stream_id,
                extent_id,
                error_code,
            },
            Some(Bytes::copy_from_slice(message.as_bytes())),
        )
    }

    pub fn report_extents_resp_error(
        request_id: u32,
        stream_id: StreamId,
        epoch: Epoch,
        error_code: ErrorCode,
        message: &str,
    ) -> Frame {
        Frame::new(
            VariableHeader::ReportExtentsRespError {
                request_id,
                stream_id,
                epoch,
                error_code,
            },
            Some(Bytes::copy_from_slice(message.as_bytes())),
        )
    }

    pub fn describe_stream_resp_error(
        request_id: u32,
        stream_id: StreamId,
        error_code: ErrorCode,
        message: &str,
    ) -> Frame {
        Frame::new(
            VariableHeader::DescribeStreamRespError {
                request_id,
                stream_id,
                error_code,
            },
            Some(Bytes::copy_from_slice(message.as_bytes())),
        )
    }

    pub fn describe_extent_resp_error(
        request_id: u32,
        stream_id: StreamId,
        extent_id: ExtentId,
        error_code: ErrorCode,
        message: &str,
    ) -> Frame {
        Frame::new(
            VariableHeader::DescribeExtentRespError {
                request_id,
                stream_id,
                extent_id,
                error_code,
            },
            Some(Bytes::copy_from_slice(message.as_bytes())),
        )
    }

    pub fn seek_resp_error(
        request_id: u32,
        stream_id: StreamId,
        offset: Offset,
        error_code: ErrorCode,
        message: &str,
    ) -> Frame {
        Frame::new(
            VariableHeader::SeekRespError {
                request_id,
                stream_id,
                offset,
                error_code,
            },
            Some(Bytes::copy_from_slice(message.as_bytes())),
        )
    }

    pub fn error_from_request(
        request: &Frame,
        error_code: ErrorCode,
        message: &str,
        extent_id: ExtentId,
    ) -> Frame {
        let effective_extent_id = if extent_id != ExtentId(0) {
            extent_id
        } else {
            request.extent_id()
        };
        match &request.variable_header {
            VariableHeader::Append {
                request_id,
                stream_id,
                epoch,
            } => Self::append_ack_error(
                *request_id,
                *stream_id,
                *epoch,
                effective_extent_id,
                error_code,
                message,
            ),
            VariableHeader::Read {
                request_id,
                stream_id,
                extent_id,
                offset,
                ..
            } => Self::read_resp_error(
                *request_id,
                *stream_id,
                if effective_extent_id != ExtentId(0) {
                    effective_extent_id
                } else {
                    *extent_id
                },
                *offset,
                error_code,
                message,
            ),
            VariableHeader::Seal {
                request_id,
                stream_id,
                extent_id,
                ..
            } => Self::seal_ack_error(
                *request_id,
                *stream_id,
                if effective_extent_id != ExtentId(0) {
                    effective_extent_id
                } else {
                    *extent_id
                },
                error_code,
                message,
            ),
            VariableHeader::CreateStream { request_id, .. } => {
                Self::create_stream_resp_error(*request_id, error_code, message)
            }
            VariableHeader::QueryOffset {
                request_id,
                stream_id,
            } => Self::query_offset_resp_error(*request_id, *stream_id, error_code, message),
            VariableHeader::Connect { request_id } => {
                Self::connect_ack_error(*request_id, error_code, message)
            }
            VariableHeader::Disconnect { request_id } => {
                Self::disconnect_ack_error(*request_id, error_code, message)
            }
            VariableHeader::Heartbeat { request_id } => {
                Self::heartbeat_error(*request_id, error_code, message)
            }
            VariableHeader::RegisterExtent {
                request_id,
                stream_id,
                extent_id,
                ..
            } => Self::register_extent_ack_error(
                *request_id,
                *stream_id,
                if effective_extent_id != ExtentId(0) {
                    effective_extent_id
                } else {
                    *extent_id
                },
                error_code,
                message,
            ),
            VariableHeader::ReportExtents {
                request_id,
                stream_id,
                epoch,
            } => Self::report_extents_resp_error(
                *request_id,
                *stream_id,
                *epoch,
                error_code,
                message,
            ),
            VariableHeader::DescribeStream {
                request_id,
                stream_id,
                ..
            } => Self::describe_stream_resp_error(*request_id, *stream_id, error_code, message),
            VariableHeader::DescribeExtent {
                request_id,
                stream_id,
                extent_id,
            } => Self::describe_extent_resp_error(
                *request_id,
                *stream_id,
                if effective_extent_id != ExtentId(0) {
                    effective_extent_id
                } else {
                    *extent_id
                },
                error_code,
                message,
            ),
            VariableHeader::Seek {
                request_id,
                stream_id,
                offset,
            } => Self::seek_resp_error(*request_id, *stream_id, *offset, error_code, message),
            _ => panic!(
                "no error response mapping for opcode {:?}",
                request.opcode()
            ),
        }
    }

    /// Compute the remaining length (variable header + payload) for this frame.
    fn remaining_length(&self) -> u32 {
        let vh = self.variable_header_len();
        let pl = if self.has_payload_section() {
            4 + self.payload.as_ref().map_or(0, |p| p.len()) // u32 length prefix + payload bytes
        } else {
            0
        };
        (vh + pl) as u32
    }

    /// Variable header size in bytes for this frame's opcode+flags.
    fn variable_header_len(&self) -> usize {
        match &self.variable_header {
            // request_id(4) + stream_id(8) + epoch(4)
            VariableHeader::Append { .. } => 4 + 8 + 4,
            // request_id(4) + stream_id(8) + epoch(4) + extent_id(4) + offset(8)
            VariableHeader::AppendAck { .. } => 4 + 8 + 4 + 4 + 8,
            // request_id(4) + stream_id(8) + epoch(4) + extent_id(4) + error_code(2)
            VariableHeader::AppendAckError { .. } => 4 + 8 + 4 + 4 + 2,
            // request_id(4) + stream_id(8) + extent_id(4) + offset(8) + count(4)
            VariableHeader::Read { .. } => 4 + 8 + 4 + 8 + 4,
            // request_id(4) + stream_id(8) + offset(8) + count(4)
            VariableHeader::ReadResp { .. } => 4 + 8 + 8 + 4,
            // request_id(4) + stream_id(8) + extent_id(4) + offset(8) + error_code(2)
            VariableHeader::ReadRespError { .. } => 4 + 8 + 4 + 8 + 2,
            // request_id(4) + stream_id(8) + extent_id(4) [+ offset(8) if present]
            VariableHeader::Seal {
                offset,
                start_offset,
                epoch,
                ..
            } => {
                let base = 4 + 8 + 4;
                let so = if start_offset.is_some() { 8 } else { 0 };
                let off = if offset.is_some() { 8 } else { 0 };
                let ep = if epoch.is_some() { 4 } else { 0 };
                base + so + off + ep
            }
            // request_id(4) + stream_id(8) + extent_id(4) + offset(8)
            // [+ new_extent_id(4) + addr_len(2) + addr_bytes if FLAG_NEW_EXTENT_PRESENT]
            VariableHeader::SealAck {
                new_extent_id,
                primary_addr,
                epoch,
                ..
            } => {
                let base = 4 + 8 + 4 + 8;
                let ne = if new_extent_id.is_some() {
                    4 + 2 + primary_addr.as_ref().map_or(0, |a| a.len())
                } else {
                    0
                };
                let ep = if epoch.is_some() { 4 } else { 0 };
                base + ne + ep
            }
            // request_id(4) + stream_id(8) + extent_id(4) + error_code(2)
            VariableHeader::SealAckError { .. } => 4 + 8 + 4 + 2,
            // request_id(4) + name_len(2) + name(N) + replication_factor(2) + extent_capacity(4) + cache_extents(4)
            VariableHeader::CreateStream { stream_name, .. } => {
                4 + 2 + stream_name.len() + 2 + 4 + 4
            }
            // request_id(4) + stream_id(8) + extent_id(4) + epoch(4) + addr_len(2) + addr(N)
            VariableHeader::CreateStreamResp { primary_addr, .. } => {
                4 + 8 + 4 + 4 + 2 + primary_addr.len()
            }
            // request_id(4) + error_code(2)
            VariableHeader::CreateStreamRespError { .. } => 4 + 2,
            // request_id(4) + stream_id(8)
            VariableHeader::QueryOffset { .. } => 4 + 8,
            // request_id(4) + stream_id(8) + offset(8)
            VariableHeader::QueryOffsetResp { .. } => 4 + 8 + 8,
            // request_id(4) + stream_id(8) + error_code(2)
            VariableHeader::QueryOffsetRespError { .. } => 4 + 8 + 2,
            // request_id(4)
            VariableHeader::Connect { .. }
            | VariableHeader::ConnectAck { .. }
            | VariableHeader::Disconnect { .. }
            | VariableHeader::DisconnectAck { .. }
            | VariableHeader::Heartbeat { .. } => 4,
            // request_id(4) + error_code(2)
            VariableHeader::ConnectAckError { .. }
            | VariableHeader::DisconnectAckError { .. }
            | VariableHeader::HeartbeatError { .. } => 4 + 2,
            // request_id(4) + stream_id(8) + extent_id(4) + role(1) + replication_factor(2) + epoch(4) + extent_capacity(4) + cache_extents(4)
            VariableHeader::RegisterExtent { .. } => 4 + 8 + 4 + 1 + 2 + 4 + 4 + 4,
            // request_id(4) + stream_id(8) + extent_id(4)
            VariableHeader::RegisterExtentAck { .. } => 4 + 8 + 4,
            // request_id(4) + stream_id(8) + extent_id(4) + error_code(2)
            VariableHeader::RegisterExtentAckError { .. } => 4 + 8 + 4 + 2,
            // stream_id(8) + extent_id(4) + offset(8) -- no request_id
            VariableHeader::Watermark { .. } => 8 + 4 + 8,
            // stream_id(8) + epoch(4) + sealed_extent_id(4) + end_offset(8) + new_extent_id(4)
            VariableHeader::UpdateExtentSealed { .. } => 8 + 4 + 4 + 8 + 4,
            // stream_id(8) + epoch(4) + extent_id(4) + current_offset(8)
            VariableHeader::UpdateExtentProgress { .. } => 8 + 4 + 4 + 8,
            // request_id(4) + stream_id(8) + epoch(4)
            VariableHeader::ReportExtents { .. } => 4 + 8 + 4,
            // request_id(4) + stream_id(8) + epoch(4)
            VariableHeader::ReportExtentsResp { .. } => 4 + 8 + 4,
            // request_id(4) + stream_id(8) + epoch(4) + error_code(2)
            VariableHeader::ReportExtentsRespError { .. } => 4 + 8 + 4 + 2,
            // stream_id(8) + extent_id(4) + epoch(4) + offset(8) + byte_pos(8)
            VariableHeader::Forward { .. } => 8 + 4 + 4 + 8 + 8,
            // stream_id(8) + extent_id(4) + epoch(4) + start_offset(8) + extent_capacity(4) + cache_extents(4)
            VariableHeader::ForwardInitExtent { .. } => 8 + 4 + 4 + 8 + 4 + 4,
            // stream_id(8) + extent_id(4) + checksum(4) + committed_bytes(8)
            VariableHeader::ForwardChecksum { .. } => 8 + 4 + 4 + 8,
            // no variable header, just payload
            VariableHeader::StreamManagerMembershipChange => 0,
            // request_id(4) + stream_id(8) + count(4) [+ name_len(2) + name(N) if FLAG_DESCRIBE_STREAM_BY_NAME]
            VariableHeader::DescribeStream { stream_name, .. } => {
                let base = 4 + 8 + 4;
                let name = stream_name.as_ref().map_or(0, |n| 2 + n.len());
                base + name
            }
            // request_id(4) + stream_id(8)
            VariableHeader::DescribeStreamResp { .. }
            | VariableHeader::DescribeExtentResp { .. } => 4 + 8,
            // request_id(4) + stream_id(8) + error_code(2)
            VariableHeader::DescribeStreamRespError { .. } => 4 + 8 + 2,
            // request_id(4) + stream_id(8) + extent_id(4) + error_code(2)
            VariableHeader::DescribeExtentRespError { .. } => 4 + 8 + 4 + 2,
            // request_id(4) + stream_id(8) + extent_id(4)
            VariableHeader::DescribeExtent { .. } => 4 + 8 + 4,
            // request_id(4) + stream_id(8) + offset(8)
            VariableHeader::Seek { .. } | VariableHeader::SeekResp { .. } => 4 + 8 + 8,
            // request_id(4) + stream_id(8) + offset(8) + error_code(2)
            VariableHeader::SeekRespError { .. } => 4 + 8 + 8 + 2,
        }
    }

    /// Whether this opcode has a payload section (u32 length prefix + bytes).
    fn has_payload_section(&self) -> bool {
        match &self.variable_header {
            VariableHeader::Append { .. }
            | VariableHeader::ReadResp { .. }
            | VariableHeader::ReadRespError { .. }
            | VariableHeader::Connect { .. }
            | VariableHeader::Disconnect { .. }
            | VariableHeader::Heartbeat { .. }
            | VariableHeader::HeartbeatError { .. }
            | VariableHeader::RegisterExtent { .. }
            | VariableHeader::Forward { .. }
            | VariableHeader::StreamManagerMembershipChange
            | VariableHeader::ReportExtentsResp { .. }
            | VariableHeader::ReportExtentsRespError { .. }
            | VariableHeader::DescribeStreamResp { .. }
            | VariableHeader::DescribeStreamRespError { .. }
            | VariableHeader::DescribeExtentResp { .. }
            | VariableHeader::DescribeExtentRespError { .. }
            | VariableHeader::SeekResp { .. }
            | VariableHeader::SeekRespError { .. }
            | VariableHeader::AppendAckError { .. }
            | VariableHeader::SealAckError { .. }
            | VariableHeader::CreateStreamRespError { .. }
            | VariableHeader::QueryOffsetRespError { .. }
            | VariableHeader::ConnectAckError { .. }
            | VariableHeader::DisconnectAckError { .. }
            | VariableHeader::RegisterExtentAckError { .. } => true,
            _ => false,
        }
    }

    /// Encode this frame into the destination buffer.
    pub fn encode(&self, dst: &mut BytesMut) {
        let remaining = self.remaining_length();
        dst.reserve(HEADER_LEN + remaining as usize);

        // Fixed header (8 bytes).
        dst.put_u8(MAGIC);
        dst.put_u8(PROTOCOL_VERSION);
        dst.put_u8(self.header.opcode as u8);
        dst.put_u8(self.flags());
        dst.put_u32(remaining);

        // Variable header (opcode-specific).
        self.encode_variable_header(dst);

        // Payload section (u32 length prefix + bytes), if applicable.
        if self.has_payload_section() {
            let payload_bytes = self.payload.as_ref().map_or(&[][..], |p| &p[..]);
            dst.put_u32(payload_bytes.len() as u32);
            dst.extend_from_slice(payload_bytes);
        }
    }

    fn encode_variable_header(&self, dst: &mut BytesMut) {
        match &self.variable_header {
            VariableHeader::Append {
                request_id,
                stream_id,
                epoch,
            } => {
                dst.put_u32(*request_id);
                dst.put_u64(stream_id.0);
                dst.put_u32(epoch.0);
            }
            VariableHeader::AppendAck {
                request_id,
                stream_id,
                epoch,
                extent_id,
                offset,
            } => {
                dst.put_u32(*request_id);
                dst.put_u64(stream_id.0);
                dst.put_u32(epoch.0);
                dst.put_u32(extent_id.0);
                dst.put_u64(offset.0);
            }
            VariableHeader::AppendAckError {
                request_id,
                stream_id,
                epoch,
                extent_id,
                error_code,
            } => {
                dst.put_u32(*request_id);
                dst.put_u64(stream_id.0);
                dst.put_u32(epoch.0);
                dst.put_u32(extent_id.0);
                dst.put_u16(*error_code as u16);
            }
            VariableHeader::Read {
                request_id,
                stream_id,
                extent_id,
                offset,
                count,
            } => {
                dst.put_u32(*request_id);
                dst.put_u64(stream_id.0);
                dst.put_u32(extent_id.0);
                dst.put_u64(offset.0);
                dst.put_u32(*count);
            }
            VariableHeader::ReadResp {
                request_id,
                stream_id,
                offset,
                count,
            } => {
                dst.put_u32(*request_id);
                dst.put_u64(stream_id.0);
                dst.put_u64(offset.0);
                dst.put_u32(*count);
            }
            VariableHeader::ReadRespError {
                request_id,
                stream_id,
                extent_id,
                offset,
                error_code,
            } => {
                dst.put_u32(*request_id);
                dst.put_u64(stream_id.0);
                dst.put_u32(extent_id.0);
                dst.put_u64(offset.0);
                dst.put_u16(*error_code as u16);
            }
            VariableHeader::Seal {
                request_id,
                stream_id,
                extent_id,
                offset,
                start_offset,
                epoch,
            } => {
                dst.put_u32(*request_id);
                dst.put_u64(stream_id.0);
                dst.put_u32(extent_id.0);
                if let Some(so) = start_offset {
                    dst.put_u64(*so);
                }
                if let Some(off) = offset {
                    dst.put_u64(off.0);
                }
                if let Some(ep) = epoch {
                    dst.put_u32(ep.0);
                }
            }
            VariableHeader::SealAck {
                request_id,
                stream_id,
                extent_id,
                offset,
                new_extent_id,
                primary_addr,
                epoch,
            } => {
                dst.put_u32(*request_id);
                dst.put_u64(stream_id.0);
                dst.put_u32(extent_id.0);
                dst.put_u64(offset.0);
                if let Some(neid) = new_extent_id {
                    dst.put_u32(neid.0);
                    let addr_bytes = primary_addr.as_ref().map_or(&[][..], |a| &a[..]);
                    dst.put_u16(addr_bytes.len() as u16);
                    dst.extend_from_slice(addr_bytes);
                }
                if let Some(ep) = epoch {
                    dst.put_u32(ep.0);
                }
            }
            VariableHeader::SealAckError {
                request_id,
                stream_id,
                extent_id,
                error_code,
            } => {
                dst.put_u32(*request_id);
                dst.put_u64(stream_id.0);
                dst.put_u32(extent_id.0);
                dst.put_u16(*error_code as u16);
            }
            VariableHeader::CreateStream {
                request_id,
                stream_name,
                replication_factor,
                extent_capacity,
                cache_extents,
            } => {
                dst.put_u32(*request_id);
                dst.put_u16(stream_name.len() as u16);
                dst.extend_from_slice(stream_name);
                dst.put_u16(*replication_factor);
                dst.put_u32(*extent_capacity);
                dst.put_u32(*cache_extents);
            }
            VariableHeader::CreateStreamResp {
                request_id,
                stream_id,
                extent_id,
                epoch,
                primary_addr,
            } => {
                dst.put_u32(*request_id);
                dst.put_u64(stream_id.0);
                dst.put_u32(extent_id.0);
                dst.put_u32(epoch.0);
                dst.put_u16(primary_addr.len() as u16);
                dst.extend_from_slice(primary_addr);
            }
            VariableHeader::CreateStreamRespError {
                request_id,
                error_code,
            } => {
                dst.put_u32(*request_id);
                dst.put_u16(*error_code as u16);
            }
            VariableHeader::QueryOffset {
                request_id,
                stream_id,
            } => {
                dst.put_u32(*request_id);
                dst.put_u64(stream_id.0);
            }
            VariableHeader::QueryOffsetResp {
                request_id,
                stream_id,
                offset,
            } => {
                dst.put_u32(*request_id);
                dst.put_u64(stream_id.0);
                dst.put_u64(offset.0);
            }
            VariableHeader::QueryOffsetRespError {
                request_id,
                stream_id,
                error_code,
            } => {
                dst.put_u32(*request_id);
                dst.put_u64(stream_id.0);
                dst.put_u16(*error_code as u16);
            }
            VariableHeader::Connect { request_id }
            | VariableHeader::Disconnect { request_id }
            | VariableHeader::Heartbeat { request_id } => {
                dst.put_u32(*request_id);
            }
            VariableHeader::RegisterExtent {
                request_id,
                stream_id,
                extent_id,
                role,
                replication_factor,
                epoch,
                extent_capacity,
                cache_extents,
            } => {
                dst.put_u32(*request_id);
                dst.put_u64(stream_id.0);
                dst.put_u32(extent_id.0);
                dst.put_u8(*role);
                dst.put_u16(*replication_factor);
                dst.put_u32(epoch.0);
                dst.put_u32(*extent_capacity);
                dst.put_u32(*cache_extents);
            }
            VariableHeader::ConnectAck { request_id }
            | VariableHeader::DisconnectAck { request_id } => {
                dst.put_u32(*request_id);
            }
            VariableHeader::ConnectAckError {
                request_id,
                error_code,
            }
            | VariableHeader::DisconnectAckError {
                request_id,
                error_code,
            }
            | VariableHeader::HeartbeatError {
                request_id,
                error_code,
            } => {
                dst.put_u32(*request_id);
                dst.put_u16(*error_code as u16);
            }
            VariableHeader::RegisterExtentAck {
                request_id,
                stream_id,
                extent_id,
            } => {
                dst.put_u32(*request_id);
                dst.put_u64(stream_id.0);
                dst.put_u32(extent_id.0);
            }
            VariableHeader::RegisterExtentAckError {
                request_id,
                stream_id,
                extent_id,
                error_code,
            } => {
                dst.put_u32(*request_id);
                dst.put_u64(stream_id.0);
                dst.put_u32(extent_id.0);
                dst.put_u16(*error_code as u16);
            }
            VariableHeader::Watermark {
                stream_id,
                extent_id,
                offset,
            } => {
                dst.put_u64(stream_id.0);
                dst.put_u32(extent_id.0);
                dst.put_u64(offset.0);
            }
            VariableHeader::Forward {
                stream_id,
                extent_id,
                epoch,
                offset,
                byte_pos,
            } => {
                dst.put_u64(stream_id.0);
                dst.put_u32(extent_id.0);
                dst.put_u32(epoch.0);
                dst.put_u64(offset.0);
                dst.put_u64(*byte_pos);
            }
            VariableHeader::ForwardInitExtent {
                stream_id,
                extent_id,
                epoch,
                start_offset,
                extent_capacity,
                cache_extents,
            } => {
                dst.put_u64(stream_id.0);
                dst.put_u32(extent_id.0);
                dst.put_u32(epoch.0);
                dst.put_u64(start_offset.0);
                dst.put_u32(*extent_capacity);
                dst.put_u32(*cache_extents);
            }
            VariableHeader::ForwardChecksum {
                stream_id,
                extent_id,
                checksum,
                committed_bytes,
            } => {
                dst.put_u64(stream_id.0);
                dst.put_u32(extent_id.0);
                dst.put_u32(*checksum);
                dst.put_u64(*committed_bytes);
            }
            VariableHeader::StreamManagerMembershipChange => {
                // no variable header fields, just payload
            }
            VariableHeader::DescribeStream {
                request_id,
                stream_id,
                count,
                stream_name,
            } => {
                dst.put_u32(*request_id);
                dst.put_u64(stream_id.0);
                dst.put_u32(*count);
                if let Some(name) = stream_name {
                    dst.put_u16(name.len() as u16);
                    dst.extend_from_slice(name);
                }
            }
            VariableHeader::DescribeStreamResp {
                request_id,
                stream_id,
            }
            | VariableHeader::DescribeExtentResp {
                request_id,
                stream_id,
            } => {
                dst.put_u32(*request_id);
                dst.put_u64(stream_id.0);
            }
            VariableHeader::DescribeStreamRespError {
                request_id,
                stream_id,
                error_code,
            } => {
                dst.put_u32(*request_id);
                dst.put_u64(stream_id.0);
                dst.put_u16(*error_code as u16);
            }
            VariableHeader::DescribeExtentRespError {
                request_id,
                stream_id,
                extent_id,
                error_code,
            } => {
                dst.put_u32(*request_id);
                dst.put_u64(stream_id.0);
                dst.put_u32(extent_id.0);
                dst.put_u16(*error_code as u16);
            }
            VariableHeader::DescribeExtent {
                request_id,
                stream_id,
                extent_id,
            } => {
                dst.put_u32(*request_id);
                dst.put_u64(stream_id.0);
                dst.put_u32(extent_id.0);
            }
            VariableHeader::Seek {
                request_id,
                stream_id,
                offset,
            } => {
                dst.put_u32(*request_id);
                dst.put_u64(stream_id.0);
                dst.put_u64(offset.0);
            }
            VariableHeader::SeekResp {
                request_id,
                stream_id,
                offset,
            } => {
                dst.put_u32(*request_id);
                dst.put_u64(stream_id.0);
                dst.put_u64(offset.0);
            }
            VariableHeader::SeekRespError {
                request_id,
                stream_id,
                offset,
                error_code,
            } => {
                dst.put_u32(*request_id);
                dst.put_u64(stream_id.0);
                dst.put_u64(offset.0);
                dst.put_u16(*error_code as u16);
            }
            VariableHeader::UpdateExtentSealed {
                stream_id,
                epoch,
                sealed_extent_id,
                end_offset,
                new_extent_id,
            } => {
                dst.put_u64(stream_id.0);
                dst.put_u32(epoch.0);
                dst.put_u32(sealed_extent_id.0);
                dst.put_u64(end_offset.0);
                dst.put_u32(new_extent_id.0);
            }
            VariableHeader::UpdateExtentProgress {
                stream_id,
                epoch,
                extent_id,
                current_offset,
            } => {
                dst.put_u64(stream_id.0);
                dst.put_u32(epoch.0);
                dst.put_u32(extent_id.0);
                dst.put_u64(current_offset.0);
            }
            VariableHeader::ReportExtents {
                request_id,
                stream_id,
                epoch,
            } => {
                dst.put_u32(*request_id);
                dst.put_u64(stream_id.0);
                dst.put_u32(epoch.0);
            }
            VariableHeader::ReportExtentsResp {
                request_id,
                stream_id,
                epoch,
            } => {
                dst.put_u32(*request_id);
                dst.put_u64(stream_id.0);
                dst.put_u32(epoch.0);
            }
            VariableHeader::ReportExtentsRespError {
                request_id,
                stream_id,
                epoch,
                error_code,
            } => {
                dst.put_u32(*request_id);
                dst.put_u64(stream_id.0);
                dst.put_u32(epoch.0);
                dst.put_u16(*error_code as u16);
            }
        }
    }

    /// Try to decode a frame from the source buffer.
    ///
    /// Returns `Ok(None)` if there is not enough data yet.
    pub fn decode(src: &mut BytesMut) -> Result<Option<Frame>, StorageError> {
        if src.len() < HEADER_LEN {
            return Ok(None);
        }

        // Peek at remaining_length without advancing.
        let remaining_len = u32::from_be_bytes([src[4], src[5], src[6], src[7]]) as usize;
        let total_len = HEADER_LEN + remaining_len;

        if src.len() < total_len {
            src.reserve(total_len - src.len());
            return Ok(None);
        }

        // We have a complete frame -- consume it.
        let magic = src.get_u8();
        if magic != MAGIC {
            return Err(StorageError::InvalidFrame(format!(
                "bad magic: expected 0x{MAGIC:02X}, got 0x{magic:02X}"
            )));
        }

        let version = src.get_u8();
        if version != PROTOCOL_VERSION {
            return Err(StorageError::InvalidFrame(format!(
                "unsupported version: {version}"
            )));
        }

        let opcode_byte = src.get_u8();
        let opcode =
            Opcode::from_u8(opcode_byte).ok_or(StorageError::UnknownOpcode(opcode_byte))?;

        let flags = src.get_u8();
        let _remaining_len = src.get_u32(); // already peeked above

        // Read the remaining bytes into a temporary buffer for parsing.
        let mut body_buf = src.split_to(remaining_len);

        let (variable_header, payload) =
            Self::decode_variable_header(opcode, flags, &mut body_buf)?;

        Ok(Some(Frame {
            header: FixedHeader {
                opcode,
                version,
                flags,
            },
            variable_header,
            payload,
        }))
    }

    fn decode_variable_header(
        opcode: Opcode,
        flags: u8,
        body: &mut BytesMut,
    ) -> Result<(VariableHeader, Option<Bytes>), StorageError> {
        match opcode {
            Opcode::Append => {
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u64());
                let epoch = Epoch(body.get_u32());
                let payload = Self::read_payload(body);
                Ok((
                    VariableHeader::Append {
                        request_id,
                        stream_id,
                        epoch,
                    },
                    payload,
                ))
            }
            Opcode::AppendAck => {
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u64());
                let epoch = Epoch(body.get_u32());
                let extent_id = ExtentId(body.get_u32());
                if flags & FLAG_RESPONSE_ERROR != 0 {
                    let error_code = ErrorCode::from_u16(body.get_u16()).ok_or_else(|| {
                        StorageError::InvalidFrame("unknown AppendAck error code".into())
                    })?;
                    let payload = Self::read_payload(body);
                    Ok((
                        VariableHeader::AppendAckError {
                            request_id,
                            stream_id,
                            epoch,
                            extent_id,
                            error_code,
                        },
                        payload,
                    ))
                } else {
                    let offset = Offset(body.get_u64());
                    Ok((
                        VariableHeader::AppendAck {
                            request_id,
                            stream_id,
                            epoch,
                            extent_id,
                            offset,
                        },
                        None,
                    ))
                }
            }
            Opcode::Read => {
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u64());
                let extent_id = ExtentId(body.get_u32());
                let offset = Offset(body.get_u64());
                let count = body.get_u32();
                Ok((
                    VariableHeader::Read {
                        request_id,
                        stream_id,
                        extent_id,
                        offset,
                        count,
                    },
                    None,
                ))
            }
            Opcode::ReadResp => {
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u64());
                if flags & FLAG_RESPONSE_ERROR != 0 {
                    let extent_id = ExtentId(body.get_u32());
                    let offset = Offset(body.get_u64());
                    let error_code = ErrorCode::from_u16(body.get_u16()).ok_or_else(|| {
                        StorageError::InvalidFrame("unknown ReadResp error code".into())
                    })?;
                    let payload = Self::read_payload(body);
                    Ok((
                        VariableHeader::ReadRespError {
                            request_id,
                            stream_id,
                            extent_id,
                            offset,
                            error_code,
                        },
                        payload,
                    ))
                } else {
                    let offset = Offset(body.get_u64());
                    let count = body.get_u32();
                    let payload = Self::read_payload(body);
                    Ok((
                        VariableHeader::ReadResp {
                            request_id,
                            stream_id,
                            offset,
                            count,
                        },
                        payload,
                    ))
                }
            }
            Opcode::Seal => {
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u64());
                let extent_id = ExtentId(body.get_u32());
                let start_offset = if flags & FLAG_START_OFFSET_PRESENT != 0 {
                    Some(body.get_u64())
                } else {
                    None
                };
                let offset = if flags & FLAG_OFFSET_PRESENT != 0 {
                    Some(Offset(body.get_u64()))
                } else {
                    None
                };
                let epoch = if flags & FLAG_EPOCH_PRESENT != 0 {
                    Some(Epoch(body.get_u32()))
                } else {
                    None
                };
                Ok((
                    VariableHeader::Seal {
                        request_id,
                        stream_id,
                        extent_id,
                        offset,
                        start_offset,
                        epoch,
                    },
                    None,
                ))
            }
            Opcode::SealAck => {
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u64());
                let extent_id = ExtentId(body.get_u32());
                if flags & FLAG_RESPONSE_ERROR != 0 {
                    let error_code = ErrorCode::from_u16(body.get_u16()).ok_or_else(|| {
                        StorageError::InvalidFrame("unknown SealAck error code".into())
                    })?;
                    let payload = Self::read_payload(body);
                    Ok((
                        VariableHeader::SealAckError {
                            request_id,
                            stream_id,
                            extent_id,
                            error_code,
                        },
                        payload,
                    ))
                } else {
                    let response_flags = flags & !FLAG_RESPONSE_ERROR;
                    let offset = Offset(body.get_u64());
                    let (new_extent_id, primary_addr) =
                        if response_flags & FLAG_NEW_EXTENT_PRESENT != 0 {
                            let neid = ExtentId(body.get_u32());
                            let addr_len = body.get_u16() as usize;
                            let addr = if body.remaining() >= addr_len {
                                Some(body.split_to(addr_len).freeze())
                            } else {
                                None
                            };
                            (Some(neid), addr)
                        } else {
                            (None, None)
                        };
                    let epoch = if response_flags & FLAG_EPOCH_PRESENT != 0 {
                        Some(Epoch(body.get_u32()))
                    } else {
                        None
                    };
                    Ok((
                        VariableHeader::SealAck {
                            request_id,
                            stream_id,
                            extent_id,
                            offset,
                            new_extent_id,
                            primary_addr,
                            epoch,
                        },
                        None,
                    ))
                }
            }
            Opcode::CreateStream => {
                let request_id = body.get_u32();
                let name_len = body.get_u16() as usize;
                let stream_name = body.split_to(name_len).freeze();
                let replication_factor = body.get_u16();
                let extent_capacity = body.get_u32();
                let cache_extents = body.get_u32();
                Ok((
                    VariableHeader::CreateStream {
                        request_id,
                        stream_name,
                        replication_factor,
                        extent_capacity,
                        cache_extents,
                    },
                    None,
                ))
            }
            Opcode::CreateStreamResp => {
                let request_id = body.get_u32();
                if flags & FLAG_RESPONSE_ERROR != 0 {
                    let error_code = ErrorCode::from_u16(body.get_u16()).ok_or_else(|| {
                        StorageError::InvalidFrame("unknown CreateStreamResp error code".into())
                    })?;
                    let payload = Self::read_payload(body);
                    Ok((
                        VariableHeader::CreateStreamRespError {
                            request_id,
                            error_code,
                        },
                        payload,
                    ))
                } else {
                    let stream_id = StreamId(body.get_u64());
                    let extent_id = ExtentId(body.get_u32());
                    let epoch = Epoch(body.get_u32());
                    let addr_len = body.get_u16() as usize;
                    let primary_addr = body.split_to(addr_len).freeze();
                    Ok((
                        VariableHeader::CreateStreamResp {
                            request_id,
                            stream_id,
                            extent_id,
                            epoch,
                            primary_addr,
                        },
                        None,
                    ))
                }
            }
            Opcode::QueryOffset => {
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u64());
                Ok((
                    VariableHeader::QueryOffset {
                        request_id,
                        stream_id,
                    },
                    None,
                ))
            }
            Opcode::QueryOffsetResp => {
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u64());
                if flags & FLAG_RESPONSE_ERROR != 0 {
                    let error_code = ErrorCode::from_u16(body.get_u16()).ok_or_else(|| {
                        StorageError::InvalidFrame("unknown QueryOffsetResp error code".into())
                    })?;
                    let payload = Self::read_payload(body);
                    Ok((
                        VariableHeader::QueryOffsetRespError {
                            request_id,
                            stream_id,
                            error_code,
                        },
                        payload,
                    ))
                } else {
                    let offset = Offset(body.get_u64());
                    Ok((
                        VariableHeader::QueryOffsetResp {
                            request_id,
                            stream_id,
                            offset,
                        },
                        None,
                    ))
                }
            }
            Opcode::Connect => {
                let request_id = body.get_u32();
                let payload = Self::read_payload(body);
                Ok((VariableHeader::Connect { request_id }, payload))
            }
            Opcode::Disconnect => {
                let request_id = body.get_u32();
                let payload = Self::read_payload(body);
                Ok((VariableHeader::Disconnect { request_id }, payload))
            }
            Opcode::Heartbeat => {
                let request_id = body.get_u32();
                if flags & FLAG_RESPONSE_ERROR != 0 {
                    let error_code = ErrorCode::from_u16(body.get_u16()).ok_or_else(|| {
                        StorageError::InvalidFrame("unknown Heartbeat error code".into())
                    })?;
                    let payload = Self::read_payload(body);
                    Ok((
                        VariableHeader::HeartbeatError {
                            request_id,
                            error_code,
                        },
                        payload,
                    ))
                } else {
                    let payload = Self::read_payload(body);
                    Ok((VariableHeader::Heartbeat { request_id }, payload))
                }
            }
            Opcode::RegisterExtent => {
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u64());
                let extent_id = ExtentId(body.get_u32());
                let role = body.get_u8();
                let replication_factor = body.get_u16();
                let epoch = Epoch(body.get_u32());
                let extent_capacity = body.get_u32();
                let cache_extents = body.get_u32();
                let payload = Self::read_payload(body);
                Ok((
                    VariableHeader::RegisterExtent {
                        request_id,
                        stream_id,
                        extent_id,
                        role,
                        replication_factor,
                        epoch,
                        extent_capacity,
                        cache_extents,
                    },
                    payload,
                ))
            }
            Opcode::ConnectAck => {
                let request_id = body.get_u32();
                if flags & FLAG_RESPONSE_ERROR != 0 {
                    let error_code = ErrorCode::from_u16(body.get_u16()).ok_or_else(|| {
                        StorageError::InvalidFrame("unknown ConnectAck error code".into())
                    })?;
                    let payload = Self::read_payload(body);
                    Ok((
                        VariableHeader::ConnectAckError {
                            request_id,
                            error_code,
                        },
                        payload,
                    ))
                } else {
                    Ok((VariableHeader::ConnectAck { request_id }, None))
                }
            }
            Opcode::DisconnectAck => {
                let request_id = body.get_u32();
                if flags & FLAG_RESPONSE_ERROR != 0 {
                    let error_code = ErrorCode::from_u16(body.get_u16()).ok_or_else(|| {
                        StorageError::InvalidFrame("unknown DisconnectAck error code".into())
                    })?;
                    let payload = Self::read_payload(body);
                    Ok((
                        VariableHeader::DisconnectAckError {
                            request_id,
                            error_code,
                        },
                        payload,
                    ))
                } else {
                    Ok((VariableHeader::DisconnectAck { request_id }, None))
                }
            }
            Opcode::RegisterExtentAck => {
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u64());
                let extent_id = ExtentId(body.get_u32());
                if flags & FLAG_RESPONSE_ERROR != 0 {
                    let error_code = ErrorCode::from_u16(body.get_u16()).ok_or_else(|| {
                        StorageError::InvalidFrame("unknown RegisterExtentAck error code".into())
                    })?;
                    let payload = Self::read_payload(body);
                    Ok((
                        VariableHeader::RegisterExtentAckError {
                            request_id,
                            stream_id,
                            extent_id,
                            error_code,
                        },
                        payload,
                    ))
                } else {
                    Ok((
                        VariableHeader::RegisterExtentAck {
                            request_id,
                            stream_id,
                            extent_id,
                        },
                        None,
                    ))
                }
            }
            Opcode::Watermark => {
                let stream_id = StreamId(body.get_u64());
                let extent_id = ExtentId(body.get_u32());
                let offset = Offset(body.get_u64());
                Ok((
                    VariableHeader::Watermark {
                        stream_id,
                        extent_id,
                        offset,
                    },
                    None,
                ))
            }
            Opcode::Forward => {
                let stream_id = StreamId(body.get_u64());
                let extent_id = ExtentId(body.get_u32());
                match flags {
                    FLAG_FORWARD_CHECKSUM => {
                        let checksum = body.get_u32();
                        let committed_bytes = body.get_u64();
                        Ok((
                            VariableHeader::ForwardChecksum {
                                stream_id,
                                extent_id,
                                checksum,
                                committed_bytes,
                            },
                            None,
                        ))
                    }
                    _ => {
                        let epoch = Epoch(body.get_u32());
                        match flags {
                            FLAG_FORWARD_APPEND => {
                                let offset = Offset(body.get_u64());
                                let byte_pos = body.get_u64();
                                let payload = Self::read_payload(body);
                                Ok((
                                    VariableHeader::Forward {
                                        stream_id,
                                        extent_id,
                                        epoch,
                                        offset,
                                        byte_pos,
                                    },
                                    payload,
                                ))
                            }
                            FLAG_FORWARD_INIT_EXTENT => {
                                let start_offset = Offset(body.get_u64());
                                let extent_capacity = body.get_u32();
                                let cache_extents = body.get_u32();
                                Ok((
                                    VariableHeader::ForwardInitExtent {
                                        stream_id,
                                        extent_id,
                                        epoch,
                                        start_offset,
                                        extent_capacity,
                                        cache_extents,
                                    },
                                    None,
                                ))
                            }
                            _ => Err(StorageError::Internal(format!(
                                "unknown Forward flag: {flags:#x}"
                            ))),
                        }
                    }
                }
            }
            Opcode::StreamManagerMembershipChange => {
                let payload = Self::read_payload(body);
                Ok((VariableHeader::StreamManagerMembershipChange, payload))
            }
            Opcode::DescribeStream => {
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u64());
                let count = body.get_u32();
                let stream_name = if flags & FLAG_DESCRIBE_STREAM_BY_NAME != 0 {
                    let name_len = body.get_u16() as usize;
                    Some(body.split_to(name_len).freeze())
                } else {
                    None
                };
                Ok((
                    VariableHeader::DescribeStream {
                        request_id,
                        stream_id,
                        count,
                        stream_name,
                    },
                    None,
                ))
            }
            Opcode::DescribeStreamResp => {
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u64());
                if flags & FLAG_RESPONSE_ERROR != 0 {
                    let error_code = ErrorCode::from_u16(body.get_u16()).ok_or_else(|| {
                        StorageError::InvalidFrame("unknown DescribeStreamResp error code".into())
                    })?;
                    let payload = Self::read_payload(body);
                    Ok((
                        VariableHeader::DescribeStreamRespError {
                            request_id,
                            stream_id,
                            error_code,
                        },
                        payload,
                    ))
                } else {
                    let payload = Self::read_payload(body);
                    Ok((
                        VariableHeader::DescribeStreamResp {
                            request_id,
                            stream_id,
                        },
                        payload,
                    ))
                }
            }
            Opcode::DescribeExtent => {
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u64());
                let extent_id = ExtentId(body.get_u32());
                Ok((
                    VariableHeader::DescribeExtent {
                        request_id,
                        stream_id,
                        extent_id,
                    },
                    None,
                ))
            }
            Opcode::DescribeExtentResp => {
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u64());
                if flags & FLAG_RESPONSE_ERROR != 0 {
                    let extent_id = ExtentId(body.get_u32());
                    let error_code = ErrorCode::from_u16(body.get_u16()).ok_or_else(|| {
                        StorageError::InvalidFrame("unknown DescribeExtentResp error code".into())
                    })?;
                    let payload = Self::read_payload(body);
                    Ok((
                        VariableHeader::DescribeExtentRespError {
                            request_id,
                            stream_id,
                            extent_id,
                            error_code,
                        },
                        payload,
                    ))
                } else {
                    let payload = Self::read_payload(body);
                    Ok((
                        VariableHeader::DescribeExtentResp {
                            request_id,
                            stream_id,
                        },
                        payload,
                    ))
                }
            }
            Opcode::Seek => {
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u64());
                let offset = Offset(body.get_u64());
                Ok((
                    VariableHeader::Seek {
                        request_id,
                        stream_id,
                        offset,
                    },
                    None,
                ))
            }
            Opcode::SeekResp => {
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u64());
                let offset = Offset(body.get_u64());
                if flags & FLAG_RESPONSE_ERROR != 0 {
                    let error_code = ErrorCode::from_u16(body.get_u16()).ok_or_else(|| {
                        StorageError::InvalidFrame("unknown SeekResp error code".into())
                    })?;
                    let payload = Self::read_payload(body);
                    Ok((
                        VariableHeader::SeekRespError {
                            request_id,
                            stream_id,
                            offset,
                            error_code,
                        },
                        payload,
                    ))
                } else {
                    let payload = Self::read_payload(body);
                    Ok((
                        VariableHeader::SeekResp {
                            request_id,
                            stream_id,
                            offset,
                        },
                        payload,
                    ))
                }
            }
            Opcode::UpdateExtent => {
                let stream_id = StreamId(body.get_u64());
                let epoch = Epoch(body.get_u32());
                match flags {
                    FLAG_EXTENT_SEALED => {
                        let sealed_extent_id = ExtentId(body.get_u32());
                        let end_offset = Offset(body.get_u64());
                        let new_extent_id = ExtentId(body.get_u32());
                        Ok((
                            VariableHeader::UpdateExtentSealed {
                                stream_id,
                                epoch,
                                sealed_extent_id,
                                end_offset,
                                new_extent_id,
                            },
                            None,
                        ))
                    }
                    FLAG_EXTENT_PROGRESS => {
                        let extent_id = ExtentId(body.get_u32());
                        let current_offset = Offset(body.get_u64());
                        Ok((
                            VariableHeader::UpdateExtentProgress {
                                stream_id,
                                epoch,
                                extent_id,
                                current_offset,
                            },
                            None,
                        ))
                    }
                    _ => Err(StorageError::Internal(format!(
                        "unknown UpdateExtent flag: {flags:#x}"
                    ))),
                }
            }
            Opcode::ReportExtents => {
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u64());
                let epoch = Epoch(body.get_u32());
                Ok((
                    VariableHeader::ReportExtents {
                        request_id,
                        stream_id,
                        epoch,
                    },
                    None,
                ))
            }
            Opcode::ReportExtentsResp => {
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u64());
                let epoch = Epoch(body.get_u32());
                if flags & FLAG_RESPONSE_ERROR != 0 {
                    let error_code = ErrorCode::from_u16(body.get_u16()).ok_or_else(|| {
                        StorageError::InvalidFrame("unknown ReportExtentsResp error code".into())
                    })?;
                    let payload = Self::read_payload(body);
                    Ok((
                        VariableHeader::ReportExtentsRespError {
                            request_id,
                            stream_id,
                            epoch,
                            error_code,
                        },
                        payload,
                    ))
                } else {
                    let payload = Self::read_payload(body);
                    Ok((
                        VariableHeader::ReportExtentsResp {
                            request_id,
                            stream_id,
                            epoch,
                        },
                        payload,
                    ))
                }
            }
        }
    }

    /// Read the payload section from the body: [payload_len:u32][payload:bytes].
    fn read_payload(body: &mut BytesMut) -> Option<Bytes> {
        if body.remaining() >= 4 {
            let payload_len = body.get_u32() as usize;
            if body.remaining() >= payload_len && payload_len > 0 {
                return Some(body.split_to(payload_len).freeze());
            }
        }
        None
    }
}

impl VariableHeader {
    /// Return the opcode that corresponds to this variant.
    pub fn opcode(&self) -> Opcode {
        match self {
            VariableHeader::Append { .. } => Opcode::Append,
            VariableHeader::AppendAck { .. } | VariableHeader::AppendAckError { .. } => {
                Opcode::AppendAck
            }
            VariableHeader::Read { .. } => Opcode::Read,
            VariableHeader::ReadResp { .. } | VariableHeader::ReadRespError { .. } => {
                Opcode::ReadResp
            }
            VariableHeader::Seal { .. } => Opcode::Seal,
            VariableHeader::SealAck { .. } | VariableHeader::SealAckError { .. } => Opcode::SealAck,
            VariableHeader::CreateStream { .. } => Opcode::CreateStream,
            VariableHeader::CreateStreamResp { .. }
            | VariableHeader::CreateStreamRespError { .. } => Opcode::CreateStreamResp,
            VariableHeader::QueryOffset { .. } => Opcode::QueryOffset,
            VariableHeader::QueryOffsetResp { .. }
            | VariableHeader::QueryOffsetRespError { .. } => Opcode::QueryOffsetResp,
            VariableHeader::Connect { .. } => Opcode::Connect,
            VariableHeader::ConnectAck { .. } | VariableHeader::ConnectAckError { .. } => {
                Opcode::ConnectAck
            }
            VariableHeader::Disconnect { .. } => Opcode::Disconnect,
            VariableHeader::DisconnectAck { .. } | VariableHeader::DisconnectAckError { .. } => {
                Opcode::DisconnectAck
            }
            VariableHeader::Heartbeat { .. } | VariableHeader::HeartbeatError { .. } => {
                Opcode::Heartbeat
            }
            VariableHeader::RegisterExtent { .. } => Opcode::RegisterExtent,
            VariableHeader::RegisterExtentAck { .. }
            | VariableHeader::RegisterExtentAckError { .. } => Opcode::RegisterExtentAck,
            VariableHeader::Watermark { .. } => Opcode::Watermark,
            VariableHeader::UpdateExtentSealed { .. }
            | VariableHeader::UpdateExtentProgress { .. } => Opcode::UpdateExtent,
            VariableHeader::ReportExtents { .. } => Opcode::ReportExtents,
            VariableHeader::ReportExtentsResp { .. }
            | VariableHeader::ReportExtentsRespError { .. } => Opcode::ReportExtentsResp,
            VariableHeader::Forward { .. }
            | VariableHeader::ForwardInitExtent { .. }
            | VariableHeader::ForwardChecksum { .. } => Opcode::Forward,
            VariableHeader::StreamManagerMembershipChange => Opcode::StreamManagerMembershipChange,
            VariableHeader::DescribeStream { .. } => Opcode::DescribeStream,
            VariableHeader::DescribeStreamResp { .. }
            | VariableHeader::DescribeStreamRespError { .. } => Opcode::DescribeStreamResp,
            VariableHeader::DescribeExtent { .. } => Opcode::DescribeExtent,
            VariableHeader::DescribeExtentResp { .. }
            | VariableHeader::DescribeExtentRespError { .. } => Opcode::DescribeExtentResp,
            VariableHeader::Seek { .. } => Opcode::Seek,
            VariableHeader::SeekResp { .. } | VariableHeader::SeekRespError { .. } => {
                Opcode::SeekResp
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_append_frame() -> Frame {
        Frame::new(
            VariableHeader::Append {
                request_id: 42,
                stream_id: StreamId(100),
                epoch: Epoch(0),
            },
            Some(Bytes::from_static(b"hello world")),
        )
    }

    #[test]
    fn round_trip_encode_decode() {
        let frame = sample_append_frame();
        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame.opcode(), decoded.opcode());
        assert_eq!(frame.request_id(), decoded.request_id());
        assert_eq!(frame.stream_id(), decoded.stream_id());
        assert_eq!(frame.epoch(), decoded.epoch());
        assert_eq!(frame.payload, decoded.payload);
        assert!(buf.is_empty());
    }

    #[test]
    fn partial_frame_returns_none() {
        let frame = sample_append_frame();
        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        // Truncate to only fixed header (no body).
        buf.truncate(HEADER_LEN);
        let mut partial = buf.clone();
        let result = Frame::decode(&mut partial).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn insufficient_header_returns_none() {
        let mut buf = BytesMut::from(&[0u8; 4][..]);
        let result = Frame::decode(&mut buf).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn invalid_magic_returns_error() {
        let mut buf = BytesMut::new();
        buf.put_u8(0xDE); // bad magic
        buf.put_u8(PROTOCOL_VERSION);
        buf.put_u8(Opcode::ConnectAck as u8);
        buf.put_u8(0);
        buf.put_u32(4); // remaining_length = 4 (request_id)
        buf.put_u32(0); // request_id

        let result = Frame::decode(&mut buf);
        assert!(matches!(result, Err(StorageError::InvalidFrame(_))));
    }

    #[test]
    fn unknown_opcode_returns_error() {
        let mut buf = BytesMut::new();
        buf.put_u8(MAGIC);
        buf.put_u8(PROTOCOL_VERSION);
        buf.put_u8(0xFE); // invalid opcode
        buf.put_u8(0);
        buf.put_u32(0); // remaining_length = 0

        let result = Frame::decode(&mut buf);
        assert!(matches!(result, Err(StorageError::UnknownOpcode(0xFE))));
    }

    #[test]
    fn multiple_frames_in_buffer() {
        let f1 = sample_append_frame();
        let f2 = Frame::new(
            VariableHeader::QueryOffset {
                request_id: 99,
                stream_id: StreamId(200),
            },
            None,
        );

        let mut buf = BytesMut::new();
        f1.encode(&mut buf);
        f2.encode(&mut buf);

        let d1 = Frame::decode(&mut buf).unwrap().unwrap();
        let d2 = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(f1.opcode(), d1.opcode());
        assert_eq!(f1.request_id(), d1.request_id());
        assert_eq!(f1.payload, d1.payload);
        assert_eq!(f2.opcode(), d2.opcode());
        assert_eq!(f2.request_id(), d2.request_id());
        assert_eq!(f2.stream_id(), d2.stream_id());
        assert!(buf.is_empty());
    }

    #[test]
    fn heartbeat_frame_with_payload() {
        let frame = Frame::new(
            VariableHeader::Heartbeat { request_id: 7 },
            Some(Bytes::from_static(b"metrics-data")),
        );

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.opcode(), Opcode::Heartbeat);
        assert_eq!(decoded.request_id(), 7);
        assert_eq!(decoded.payload, Some(Bytes::from_static(b"metrics-data")));
    }

    #[test]
    fn connect_ack_minimal() {
        let frame = Frame::new(VariableHeader::ConnectAck { request_id: 1 }, None);

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);
        // 8 (fixed) + 4 (request_id) = 12 bytes total
        assert_eq!(buf.len(), 12);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.opcode(), Opcode::ConnectAck);
        assert_eq!(decoded.request_id(), 1);
    }

    #[test]
    fn watermark_no_request_id() {
        let frame = Frame::new(
            VariableHeader::Watermark {
                stream_id: StreamId(42),
                extent_id: ExtentId(7),
                offset: Offset(100),
            },
            None,
        );

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);
        // 8 (fixed) + 8 (stream_id) + 4 (extent_id) + 8 (offset) = 28 bytes
        assert_eq!(buf.len(), 28);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.opcode(), Opcode::Watermark);
        assert_eq!(decoded.stream_id(), StreamId(42));
        assert_eq!(decoded.extent_id(), ExtentId(7));
        assert_eq!(decoded.offset(), Offset(100));
        assert_eq!(decoded.request_id(), 0); // not present on wire
    }

    #[test]
    fn append_ack_round_trip() {
        let frame = Frame::new(
            VariableHeader::AppendAck {
                request_id: 10,
                stream_id: StreamId(1),
                epoch: Epoch(3),
                extent_id: ExtentId(2),
                offset: Offset(42),
            },
            None,
        );

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.offset(), Offset(42));
        assert_eq!(decoded.stream_id(), StreamId(1));
        assert_eq!(decoded.epoch(), Epoch(3));
        assert_eq!(decoded.extent_id(), ExtentId(2));
    }

    #[test]
    fn read_with_count() {
        let frame = Frame::new(
            VariableHeader::Read {
                request_id: 5,
                stream_id: StreamId(10),
                extent_id: ExtentId(2),
                offset: Offset(50),
                count: 20,
            },
            None,
        );

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.count(), 20);
        assert_eq!(decoded.offset(), Offset(50));
    }

    #[test]
    fn seal_without_offset() {
        let frame = Frame::new(
            VariableHeader::Seal {
                request_id: 1,
                stream_id: StreamId(10),
                extent_id: ExtentId(5),
                offset: None,
                start_offset: None,
                epoch: None,
            },
            None,
        );

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);
        // 8 + 4 + 8 + 4 = 24
        assert_eq!(buf.len(), 24);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.extent_id(), ExtentId(5));
        assert_eq!(decoded.offset(), Offset(0));
    }

    #[test]
    fn seal_with_offset() {
        let frame = Frame::new(
            VariableHeader::Seal {
                request_id: 1,
                stream_id: StreamId(10),
                extent_id: ExtentId(5),
                offset: Some(Offset(42)),
                start_offset: None,
                epoch: None,
            },
            None,
        );

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);
        // 8 + 4 + 8 + 4 + 8 = 32
        assert_eq!(buf.len(), 32);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.flags(), FLAG_OFFSET_PRESENT);
        assert_eq!(decoded.extent_id(), ExtentId(5));
        assert_eq!(decoded.offset(), Offset(42));
    }

    #[test]
    fn seal_with_start_offset_only() {
        let frame = Frame::new(
            VariableHeader::Seal {
                request_id: 1,
                stream_id: StreamId(10),
                extent_id: ExtentId(5),
                offset: None,
                start_offset: Some(100),
                epoch: None,
            },
            None,
        );

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);
        // 8 + 4 + 8 + 4 + 8(start_offset) = 32
        assert_eq!(buf.len(), 32);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.flags(), FLAG_START_OFFSET_PRESENT);
        assert_eq!(decoded.extent_id(), ExtentId(5));
        assert_eq!(decoded.offset(), Offset(0)); // no offset present
        if let VariableHeader::Seal {
            start_offset,
            offset,
            ..
        } = &decoded.variable_header
        {
            assert_eq!(*start_offset, Some(100));
            assert_eq!(*offset, None);
        } else {
            panic!("expected Seal variant");
        }
    }

    #[test]
    fn seal_with_both_offsets() {
        let frame = Frame::new(
            VariableHeader::Seal {
                request_id: 1,
                stream_id: StreamId(10),
                extent_id: ExtentId(5),
                offset: Some(Offset(42)),
                start_offset: Some(100),
                epoch: None,
            },
            None,
        );

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);
        // 8 + 4 + 8 + 4 + 8(start_offset) + 8(offset) = 40
        assert_eq!(buf.len(), 40);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(
            decoded.flags(),
            FLAG_OFFSET_PRESENT | FLAG_START_OFFSET_PRESENT
        );
        assert_eq!(decoded.extent_id(), ExtentId(5));
        assert_eq!(decoded.offset(), Offset(42));
        if let VariableHeader::Seal {
            start_offset,
            offset,
            ..
        } = &decoded.variable_header
        {
            assert_eq!(*start_offset, Some(100));
            assert_eq!(*offset, Some(Offset(42)));
        } else {
            panic!("expected Seal variant");
        }
    }

    #[test]
    fn append_ack_error_frame() {
        let frame = Frame::append_ack_error(
            42,
            StreamId(9),
            Epoch(3),
            ExtentId(7),
            ErrorCode::ExtentSealed,
            "extent sealed",
        );

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.opcode(), Opcode::AppendAck);
        assert!(decoded.is_error_response());
        assert_eq!(decoded.request_id(), 42);
        assert_eq!(decoded.stream_id(), StreamId(9));
        assert_eq!(decoded.epoch(), Epoch(3));
        assert_eq!(decoded.error_code(), ErrorCode::ExtentSealed as u16);
        assert_eq!(decoded.extent_id(), ExtentId(7));
        assert_eq!(decoded.payload, Some(Bytes::from_static(b"extent sealed")));
    }

    #[test]
    fn read_resp_with_count() {
        let frame = Frame::new(
            VariableHeader::ReadResp {
                request_id: 3,
                stream_id: StreamId(1),
                offset: Offset(0),
                count: 5,
            },
            Some(Bytes::from_static(b"messages")),
        );

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.count(), 5);
        assert_eq!(decoded.payload, Some(Bytes::from_static(b"messages")));
    }

    #[test]
    fn describe_stream_with_count() {
        let frame = Frame::new(
            VariableHeader::DescribeStream {
                request_id: 1,
                stream_id: StreamId(10),
                count: 3,
                stream_name: None,
            },
            None,
        );

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.count(), 3);
        assert_eq!(decoded.stream_id(), StreamId(10));
    }

    #[test]
    fn create_stream_round_trip() {
        let frame = Frame::new(
            VariableHeader::CreateStream {
                request_id: 5,
                stream_name: Bytes::from_static(b"my-stream"),
                replication_factor: 3,
                extent_capacity: 67_108_864,
                cache_extents: 4,
            },
            None,
        );

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.opcode(), Opcode::CreateStream);
        assert_eq!(decoded.request_id(), 5);
        match &decoded.variable_header {
            VariableHeader::CreateStream {
                stream_name,
                replication_factor,
                extent_capacity,
                cache_extents,
                ..
            } => {
                assert_eq!(stream_name, &Bytes::from_static(b"my-stream"));
                assert_eq!(*replication_factor, 3);
                assert_eq!(*extent_capacity, 67_108_864);
                assert_eq!(*cache_extents, 4);
            }
            _ => panic!("expected CreateStream"),
        }
        assert!(decoded.payload.is_none());
        assert!(buf.is_empty());
    }

    #[test]
    fn create_stream_resp_round_trip() {
        let frame = Frame::new(
            VariableHeader::CreateStreamResp {
                request_id: 5,
                stream_id: StreamId(42),
                extent_id: ExtentId(1),
                epoch: Epoch(0),
                primary_addr: Bytes::from_static(b"127.0.0.1:9000"),
            },
            None,
        );

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.opcode(), Opcode::CreateStreamResp);
        assert_eq!(decoded.request_id(), 5);
        assert_eq!(decoded.stream_id(), StreamId(42));
        assert_eq!(decoded.extent_id(), ExtentId(1));
        match &decoded.variable_header {
            VariableHeader::CreateStreamResp { primary_addr, .. } => {
                assert_eq!(primary_addr, &Bytes::from_static(b"127.0.0.1:9000"));
            }
            _ => panic!("expected CreateStreamResp"),
        }
        assert!(decoded.payload.is_none());
        assert!(buf.is_empty());
    }

    #[test]
    fn describe_stream_by_name_round_trip() {
        let frame = Frame::new(
            VariableHeader::DescribeStream {
                request_id: 10,
                stream_id: StreamId(0),
                count: 1,
                stream_name: Some(Bytes::from_static(b"my-stream")),
            },
            None,
        );

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.opcode(), Opcode::DescribeStream);
        assert_eq!(decoded.flags(), FLAG_DESCRIBE_STREAM_BY_NAME);
        assert_eq!(decoded.count(), 1);
        match &decoded.variable_header {
            VariableHeader::DescribeStream { stream_name, .. } => {
                assert_eq!(
                    stream_name.as_ref().unwrap(),
                    &Bytes::from_static(b"my-stream")
                );
            }
            _ => panic!("expected DescribeStream"),
        }
        assert!(buf.is_empty());
    }

    #[test]
    fn seal_ack_without_new_extent() {
        let frame = Frame::new(
            VariableHeader::SealAck {
                request_id: 1,
                stream_id: StreamId(10),
                extent_id: ExtentId(5),
                offset: Offset(42),
                new_extent_id: None,
                primary_addr: None,
                epoch: None,
            },
            None,
        );

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);
        // 8 (fixed) + 4 (req) + 8 (stream) + 4 (extent) + 8 (offset) = 32 bytes
        assert_eq!(buf.len(), 32);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.opcode(), Opcode::SealAck);
        assert_eq!(decoded.flags(), 0);
        assert_eq!(decoded.request_id(), 1);
        assert_eq!(decoded.stream_id(), StreamId(10));
        assert_eq!(decoded.extent_id(), ExtentId(5));
        assert_eq!(decoded.offset(), Offset(42));
        // No new_extent_id or primary_addr
        match &decoded.variable_header {
            VariableHeader::SealAck {
                new_extent_id,
                primary_addr,
                ..
            } => {
                assert!(new_extent_id.is_none());
                assert!(primary_addr.is_none());
            }
            _ => panic!("expected SealAck"),
        }
        assert!(buf.is_empty());
    }

    #[test]
    fn seal_ack_with_new_extent() {
        let addr = b"127.0.0.1:9001";
        let frame = Frame::new(
            VariableHeader::SealAck {
                request_id: 2,
                stream_id: StreamId(10),
                extent_id: ExtentId(5),
                offset: Offset(42),
                new_extent_id: Some(ExtentId(6)),
                primary_addr: Some(Bytes::from_static(addr)),
                epoch: None,
            },
            None,
        );

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);
        // 8 (fixed) + 24 (base) + 4 (new_extent_id) + 2 (addr_len) + 14 (addr) = 52 bytes
        assert_eq!(buf.len(), 52);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.opcode(), Opcode::SealAck);
        assert_eq!(decoded.flags(), FLAG_NEW_EXTENT_PRESENT);
        assert_eq!(decoded.request_id(), 2);
        assert_eq!(decoded.stream_id(), StreamId(10));
        assert_eq!(decoded.extent_id(), ExtentId(5));
        assert_eq!(decoded.offset(), Offset(42));
        match &decoded.variable_header {
            VariableHeader::SealAck {
                new_extent_id,
                primary_addr,
                ..
            } => {
                assert_eq!(*new_extent_id, Some(ExtentId(6)));
                assert_eq!(primary_addr.as_ref().unwrap(), &Bytes::from_static(addr));
            }
            _ => panic!("expected SealAck"),
        }
        assert!(buf.is_empty());
    }
}
