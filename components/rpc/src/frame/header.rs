use bytes::Bytes;
use common::types::{
    ArenaClass, Epoch, EpochPolicy, ErrorCode, Offset, Opcode, StorageClass, StreamConfig, StreamId,
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
        offset: Offset,
    },
    AppendAckError {
        request_id: u32,
        stream_id: StreamId,
        epoch: Epoch,
        error_code: ErrorCode,
    },
    Read {
        request_id: u32,
        stream_id: StreamId,
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
        offset: Offset,
        error_code: ErrorCode,
    },
    SealStreamRequest {
        request_id: u32,
        stream_id: StreamId,
        epoch: Epoch,
    },
    SealStreamResp {
        request_id: u32,
        stream_id: StreamId,
        offset: Offset,
        new_epoch: Epoch,
        primary_addr: Bytes,
    },
    SealStreamRespError {
        request_id: u32,
        stream_id: StreamId,
        error_code: ErrorCode,
    },
    SealEpochPrepare {
        request_id: u32,
        stream_id: StreamId,
        epoch: Epoch,
        start_offset: u64,
    },
    SealEpochResp {
        request_id: u32,
        stream_id: StreamId,
        epoch: Epoch,
        start_offset: u64,
        end_offset: u64,
    },
    SealEpochRespError {
        request_id: u32,
        stream_id: StreamId,
        error_code: ErrorCode,
    },
    /// Phase 2 seal commit (SealEpoch, flag=0x02).
    /// SM broadcasts the authoritative committed offset so replicas commit
    /// their local seal point. Request-response with request_id.
    SealEpochCommit {
        request_id: u32,
        stream_id: StreamId,
        epoch: Epoch,
        start_offset: u64,
        end_offset: u64,
    },
    /// Successful SealEpochCommit response (flag=0x03).
    SealEpochCommitResp {
        request_id: u32,
        stream_id: StreamId,
    },
    CreateStream {
        request_id: u32,
        stream_name: Bytes,
        replication_factor: u8,
        /// Storage class for sealed extents: S3 (0) or Memory (1).
        storage_class: StorageClass,
        /// Extent sizing/caching policy.
        policy: EpochPolicy,
    },
    CreateStreamResp {
        request_id: u32,
        stream_id: StreamId,
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
    RegisterEpoch {
        request_id: u32,
        /// 0 = Primary, 1+ = Secondary.
        role: u8,
        /// Stream identity, replication, epoch, durability, and sizing policy.
        config: StreamConfig,
    },
    RegisterEpochAck {
        request_id: u32,
        stream_id: StreamId,
    },
    RegisterEpochAckError {
        request_id: u32,
        stream_id: StreamId,
        error_code: ErrorCode,
    },
    Watermark {
        stream_id: StreamId,
        epoch: Epoch,
        offset: Offset,
    },
    /// Active extent progress report (UpdateEpoch, flag=0x01).
    /// Fire-and-forget periodic update of current offset for observability.
    UpdateEpochProgress {
        stream_id: StreamId,
        epoch: Epoch,
        current_offset: Offset,
    },
    /// Extent flushed to S3 (UpdateEpoch, flag=0x02).
    /// Fire-and-forget: EN notifies SM after successful S3 upload.
    ///
    /// Carries `start_offset`, `end_offset`, and `s3_key` so SM can materialize
    /// the S3 file mapping directly (see `record_arena_flushed` in
    /// stream-manager/metadata.rs).
    UpdateEpochFlushed {
        stream_id: StreamId,
        epoch: Epoch,
        start_offset: Offset,
        end_offset: Offset,
        s3_key: Bytes,
    },
    /// SM queries an EN for all extents it holds for a stream at a given epoch (0x19).
    ReportEpoch {
        request_id: u32,
        stream_id: StreamId,
        epoch: Epoch,
    },
    /// EN response to ReportEpoch with extent state for reconciliation (0x1A).
    ReportEpochResp {
        request_id: u32,
        stream_id: StreamId,
        epoch: Epoch,
    },
    ReportEpochRespError {
        request_id: u32,
        stream_id: StreamId,
        epoch: Epoch,
        error_code: ErrorCode,
    },
    /// Per-record replication (Forward, flag=0x00).
    /// Secondary computes its own byte_pos from strict-order append; TCP FIFO
    /// ensures the secondary advances its cursor by the same amount as the primary.
    /// Fire-and-forget: no request_id; secondary responds with cumulative Watermark.
    Forward {
        stream_id: StreamId,
        epoch: Epoch,
        offset: Offset,
    },
    /// Init-extent notification (Forward, flag=0x01). No payload, no response.
    /// Sent once by primary when it starts using a new extent,
    /// before any Forward frames for that extent. Carries extent metadata
    /// so the secondary can create the extent with the correct capacity.
    ForwardInitEpoch {
        stream_id: StreamId,
        epoch: Epoch,
        start_offset: Offset,
        extent_capacity: u32,
        cache_extents: u16,
        /// Storage class for sealed extents: S3 (0) or Memory (1).
        storage_class: StorageClass,
        /// Arena class for this stream: Dedicated (0) or Shared (1).
        arena_class: ArenaClass,
    },
    /// CRC32 checksum verification (Forward, flag=0x02). Fire-and-forget.
    /// Sent by primary after sealing an extent so secondaries can verify
    /// data integrity of the replicated extent.
    ForwardChecksum {
        stream_id: StreamId,
        epoch: Epoch,
        checksum: u32,
        committed_bytes: u64,
    },
    /// Extent flushed to S3 notification (Forward, flag=0x03). Fire-and-forget.
    /// Sent by Primary after successful S3 upload so secondaries can mark
    /// the extent as eligible for memory eviction.
    ForwardFlushed {
        stream_id: StreamId,
        epoch: Epoch,
    },
    /// SM commands EN to flush a sealed extent to S3 (disaster recovery, 0x1B).
    /// Request-response: SM sends request, EN responds with Resp or RespError.
    FlushEpoch {
        request_id: u32,
        stream_id: StreamId,
        epoch: Epoch,
        start_offset: u64,
        end_offset: u64,
    },
    /// Successful FlushEpoch response: EN accepted the flush (or already flushed).
    FlushEpochResp {
        request_id: u32,
        stream_id: StreamId,
    },
    /// Error FlushEpoch response: EN could not process the flush.
    FlushEpochRespError {
        request_id: u32,
        stream_id: StreamId,
        error_code: ErrorCode,
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
    DescribeEpoch {
        request_id: u32,
        stream_id: StreamId,
    },
    DescribeEpochResp {
        request_id: u32,
        stream_id: StreamId,
    },
    DescribeEpochRespError {
        request_id: u32,
        stream_id: StreamId,
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
            VariableHeader::SealStreamRequest { .. }
            | VariableHeader::SealStreamResp { .. }
            | VariableHeader::SealStreamRespError { .. } => Opcode::SealStream,
            VariableHeader::SealEpochPrepare { .. }
            | VariableHeader::SealEpochResp { .. }
            | VariableHeader::SealEpochRespError { .. }
            | VariableHeader::SealEpochCommit { .. }
            | VariableHeader::SealEpochCommitResp { .. } => Opcode::SealEpoch,
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
            VariableHeader::RegisterEpoch { .. }
            | VariableHeader::RegisterEpochAck { .. }
            | VariableHeader::RegisterEpochAckError { .. } => Opcode::RegisterEpoch,
            VariableHeader::Watermark { .. } => Opcode::Watermark,
            VariableHeader::UpdateEpochProgress { .. }
            | VariableHeader::UpdateEpochFlushed { .. } => Opcode::UpdateEpoch,
            VariableHeader::ReportEpoch { .. }
            | VariableHeader::ReportEpochResp { .. }
            | VariableHeader::ReportEpochRespError { .. } => Opcode::ReportEpoch,
            VariableHeader::Forward { .. }
            | VariableHeader::ForwardInitEpoch { .. }
            | VariableHeader::ForwardChecksum { .. }
            | VariableHeader::ForwardFlushed { .. } => Opcode::Forward,
            VariableHeader::FlushEpoch { .. }
            | VariableHeader::FlushEpochResp { .. }
            | VariableHeader::FlushEpochRespError { .. } => Opcode::FlushEpoch,
            VariableHeader::StreamManagerMembershipChange => Opcode::StreamManagerMembershipChange,
            VariableHeader::DescribeStream { .. }
            | VariableHeader::DescribeStreamResp { .. }
            | VariableHeader::DescribeStreamRespError { .. } => Opcode::DescribeStream,
            VariableHeader::DescribeEpoch { .. }
            | VariableHeader::DescribeEpochResp { .. }
            | VariableHeader::DescribeEpochRespError { .. } => Opcode::DescribeEpoch,
            VariableHeader::Seek { .. }
            | VariableHeader::SeekResp { .. }
            | VariableHeader::SeekRespError { .. } => Opcode::Seek,
        }
    }
}
