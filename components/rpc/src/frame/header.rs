use bytes::Bytes;
use common::types::{Epoch, ErrorCode, ExtentId, Offset, Opcode, StreamId};

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
    SealStreamManagerRequest {
        request_id: u32,
        stream_id: StreamId,
        epoch: Epoch,
    },
    SealStreamManagerResp {
        request_id: u32,
        stream_id: StreamId,
        offset: Offset,
        new_epoch: Epoch,
        primary_addr: Bytes,
    },
    SealStreamManagerRespError {
        request_id: u32,
        stream_id: StreamId,
        error_code: ErrorCode,
    },
    SealExtentNodeRequest {
        request_id: u32,
        stream_id: StreamId,
        epoch: Epoch,
        extent_id_from: ExtentId,
        start_offset: u64,
    },
    SealExtentNodeResp {
        request_id: u32,
        stream_id: StreamId,
        epoch: Epoch,
        extent_id: ExtentId,
        start_offset: u64,
        end_offset: u64,
    },
    SealExtentNodeRespError {
        request_id: u32,
        stream_id: StreamId,
        error_code: ErrorCode,
    },
    CreateStream {
        request_id: u32,
        stream_name: Bytes,
        replication_factor: u16,
        /// Minimum extent capacity for new extents (0 = use default min).
        min_extent_capacity: u32,
        /// Maximum extent capacity for new extents (0 = use default max).
        max_extent_capacity: u32,
        cache_extents: u32,
        /// Growth factor for adaptive capacity scaling (0 = use default).
        /// On extent-full, next_capacity = min(current * growth_factor, max).
        extent_growth_factor: u32,
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
        /// Minimum extent capacity for this stream (0 = use default min).
        min_extent_capacity: u32,
        /// Maximum extent capacity for this stream (0 = use default max).
        max_extent_capacity: u32,
        /// Growth factor for adaptive capacity scaling (0 = use default).
        extent_growth_factor: u32,
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
        /// Capacity of the newly created extent (may be different due to adaptive scaling).
        new_extent_capacity: u32,
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
            VariableHeader::SealStreamManagerRequest { .. }
            | VariableHeader::SealStreamManagerResp { .. }
            | VariableHeader::SealStreamManagerRespError { .. } => Opcode::SealStreamManager,
            VariableHeader::SealExtentNodeRequest { .. }
            | VariableHeader::SealExtentNodeResp { .. }
            | VariableHeader::SealExtentNodeRespError { .. } => Opcode::SealExtentNode,
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
