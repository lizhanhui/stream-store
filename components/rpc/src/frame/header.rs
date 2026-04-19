use bytes::Bytes;
use common::types::{Epoch, ErrorCode, ExtentId, Offset, Opcode, StorageClass, StreamId};

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
    SealExtentNodePrepare {
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
    /// Phase 2 seal commit (SealExtentNode, flag=0x02).
    /// SM broadcasts the authoritative committed offset so replicas commit
    /// their local seal point. Fire-and-forget: no request_id, no response.
    SealExtentNodeCommit {
        stream_id: StreamId,
        extent_id: ExtentId,
        epoch: Epoch,
        start_offset: u64,
        end_offset: u64,
    },
    CreateStream {
        request_id: u32,
        stream_name: Bytes,
        replication_factor: u8,
        /// Minimum extent capacity for new extents (0 = use default min).
        min_extent_capacity: u32,
        /// Maximum extent capacity for new extents (0 = use default max).
        max_extent_capacity: u32,
        cache_extents: u16,
        /// Growth factor for adaptive capacity scaling (0 = use default).
        /// On extent-full, next_capacity = min(current * growth_factor, max).
        extent_growth_factor: u8,
        /// Storage class for sealed extents: S3 (0) or Memory (1).
        storage_class: StorageClass,
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
        replication_factor: u8,
        /// Stream epoch for this extent registration.
        epoch: Epoch,
        /// Maximum extents to retain in memory for this stream. 0 = no limit.
        cache_extents: u16,
        /// Minimum extent capacity for this stream (0 = use default min).
        min_extent_capacity: u32,
        /// Maximum extent capacity for this stream (0 = use default max).
        max_extent_capacity: u32,
        /// Growth factor for adaptive capacity scaling (0 = use default).
        extent_growth_factor: u8,
        /// Storage class for sealed extents: S3 (0) or Memory (1).
        storage_class: StorageClass,
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
        epoch: Epoch,
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
    /// Extent flushed to S3 (UpdateExtent, flag=0x02).
    /// Fire-and-forget: EN notifies SM after successful S3 upload.
    UpdateExtentFlushed {
        stream_id: StreamId,
        epoch: Epoch,
        extent_id: ExtentId,
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
        cache_extents: u16,
        /// Minimum extent capacity for adaptive sizing on this stream.
        min_extent_capacity: u32,
        /// Maximum extent capacity for adaptive sizing on this stream.
        max_extent_capacity: u32,
        /// Growth factor for adaptive capacity scaling.
        extent_growth_factor: u8,
        /// Storage class for sealed extents: S3 (0) or Memory (1).
        storage_class: StorageClass,
    },
    /// CRC32 checksum verification (Forward, flag=0x02). Fire-and-forget.
    /// Sent by primary after sealing an extent so secondaries can verify
    /// data integrity of the replicated extent.
    ForwardChecksum {
        stream_id: StreamId,
        extent_id: ExtentId,
        epoch: Epoch,
        checksum: u32,
        committed_bytes: u64,
    },
    /// Extent flushed to S3 notification (Forward, flag=0x03). Fire-and-forget.
    /// Sent by Primary after successful S3 upload so secondaries can mark
    /// the extent as eligible for memory eviction.
    ForwardFlushed {
        stream_id: StreamId,
        extent_id: ExtentId,
        epoch: Epoch,
    },
    /// SM commands EN to flush a sealed extent to S3 (disaster recovery, 0x1B).
    /// Fire-and-forget: no request_id, no response.
    FlushExtent {
        stream_id: StreamId,
        extent_id: ExtentId,
        epoch: Epoch,
        start_offset: u64,
        end_offset: u64,
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
    ///
    /// All variants in a request/response/error family return the same base opcode.
    /// The direction (request vs response vs error) is indicated by flags, not opcode.
    pub fn opcode(&self) -> Opcode {
        match self {
            VariableHeader::Append { .. }
            | VariableHeader::AppendAck { .. }
            | VariableHeader::AppendAckError { .. } => Opcode::Append,
            VariableHeader::Read { .. }
            | VariableHeader::ReadResp { .. }
            | VariableHeader::ReadRespError { .. } => Opcode::Read,
            VariableHeader::SealStreamManagerRequest { .. }
            | VariableHeader::SealStreamManagerResp { .. }
            | VariableHeader::SealStreamManagerRespError { .. } => Opcode::SealStreamManager,
            VariableHeader::SealExtentNodePrepare { .. }
            | VariableHeader::SealExtentNodeResp { .. }
            | VariableHeader::SealExtentNodeRespError { .. }
            | VariableHeader::SealExtentNodeCommit { .. } => Opcode::SealExtentNode,
            VariableHeader::CreateStream { .. }
            | VariableHeader::CreateStreamResp { .. }
            | VariableHeader::CreateStreamRespError { .. } => Opcode::CreateStream,
            VariableHeader::QueryOffset { .. }
            | VariableHeader::QueryOffsetResp { .. }
            | VariableHeader::QueryOffsetRespError { .. } => Opcode::QueryOffset,
            VariableHeader::Connect { .. }
            | VariableHeader::ConnectAck { .. }
            | VariableHeader::ConnectAckError { .. } => Opcode::Connect,
            VariableHeader::Disconnect { .. }
            | VariableHeader::DisconnectAck { .. }
            | VariableHeader::DisconnectAckError { .. } => Opcode::Disconnect,
            VariableHeader::Heartbeat { .. } | VariableHeader::HeartbeatError { .. } => {
                Opcode::Heartbeat
            }
            VariableHeader::RegisterExtent { .. }
            | VariableHeader::RegisterExtentAck { .. }
            | VariableHeader::RegisterExtentAckError { .. } => Opcode::RegisterExtent,
            VariableHeader::Watermark { .. } => Opcode::Watermark,
            VariableHeader::UpdateExtentSealed { .. }
            | VariableHeader::UpdateExtentProgress { .. }
            | VariableHeader::UpdateExtentFlushed { .. } => Opcode::UpdateExtent,
            VariableHeader::ReportExtents { .. }
            | VariableHeader::ReportExtentsResp { .. }
            | VariableHeader::ReportExtentsRespError { .. } => Opcode::ReportExtents,
            VariableHeader::Forward { .. }
            | VariableHeader::ForwardInitExtent { .. }
            | VariableHeader::ForwardChecksum { .. }
            | VariableHeader::ForwardFlushed { .. } => Opcode::Forward,
            VariableHeader::FlushExtent { .. } => Opcode::FlushExtent,
            VariableHeader::StreamManagerMembershipChange => Opcode::StreamManagerMembershipChange,
            VariableHeader::DescribeStream { .. }
            | VariableHeader::DescribeStreamResp { .. }
            | VariableHeader::DescribeStreamRespError { .. } => Opcode::DescribeStream,
            VariableHeader::DescribeExtent { .. }
            | VariableHeader::DescribeExtentResp { .. }
            | VariableHeader::DescribeExtentRespError { .. } => Opcode::DescribeExtent,
            VariableHeader::Seek { .. }
            | VariableHeader::SeekResp { .. }
            | VariableHeader::SeekRespError { .. } => Opcode::Seek,
        }
    }
}
