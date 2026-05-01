pub mod header;

mod decode;
mod encode;
mod error_constructors;

#[cfg(test)]
mod tests;

use bytes::Bytes;
use common::types::{
    Epoch, ExtentId, FLAG_DESCRIBE_STREAM_BY_NAME, FLAG_EXTENT_FLUSHED, FLAG_EXTENT_PROGRESS,
    FLAG_FORWARD_APPEND, FLAG_FORWARD_CHECKSUM, FLAG_FORWARD_FLUSHED,
    FLAG_FORWARD_INIT_EXTENT, FLAG_RESPONSE, FLAG_RESPONSE_ERROR, FLAG_SEAL_COMMIT,
    FLAG_SEAL_COMMIT_RESP, Offset, Opcode, PROTOCOL_VERSION, StreamId,
};

pub use header::{FixedHeader, VariableHeader};

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
                opcode: Opcode::Connect,
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
            | VariableHeader::SealStreamManagerRequest { request_id, .. }
            | VariableHeader::SealStreamManagerResp { request_id, .. }
            | VariableHeader::SealStreamManagerRespError { request_id, .. }
            | VariableHeader::SealExtentNodePrepare { request_id, .. }
            | VariableHeader::SealExtentNodeResp { request_id, .. }
            | VariableHeader::SealExtentNodeRespError { request_id, .. }
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
            | VariableHeader::SeekRespError { request_id, .. }
            | VariableHeader::FlushExtent { request_id, .. }
            | VariableHeader::FlushExtentResp { request_id, .. }
            | VariableHeader::FlushExtentRespError { request_id, .. }
            | VariableHeader::SealExtentNodeCommit { request_id, .. }
            | VariableHeader::SealExtentNodeCommitResp { request_id, .. } => *request_id,
            VariableHeader::Watermark { .. }
            | VariableHeader::Forward { .. }
            | VariableHeader::ForwardInitExtent { .. }
            | VariableHeader::ForwardChecksum { .. }
            | VariableHeader::ForwardFlushed { .. }
            | VariableHeader::UpdateExtentProgress { .. }
            | VariableHeader::UpdateExtentFlushed { .. }
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
            | VariableHeader::SealStreamManagerRequest { stream_id, .. }
            | VariableHeader::SealStreamManagerResp { stream_id, .. }
            | VariableHeader::SealStreamManagerRespError { stream_id, .. }
            | VariableHeader::SealExtentNodePrepare { stream_id, .. }
            | VariableHeader::SealExtentNodeResp { stream_id, .. }
            | VariableHeader::SealExtentNodeRespError { stream_id, .. }
            | VariableHeader::CreateStreamResp { stream_id, .. }
            | VariableHeader::QueryOffset { stream_id, .. }
            | VariableHeader::QueryOffsetResp { stream_id, .. }
            | VariableHeader::QueryOffsetRespError { stream_id, .. }
            | VariableHeader::RegisterExtentAck { stream_id, .. }
            | VariableHeader::RegisterExtentAckError { stream_id, .. }
            | VariableHeader::Watermark { stream_id, .. }
            | VariableHeader::Forward { stream_id, .. }
            | VariableHeader::ForwardInitExtent { stream_id, .. }
            | VariableHeader::ForwardChecksum { stream_id, .. }
            | VariableHeader::FlushExtent { stream_id, .. }
            | VariableHeader::FlushExtentResp { stream_id, .. }
            | VariableHeader::FlushExtentRespError { stream_id, .. }
            | VariableHeader::SealExtentNodeCommit { stream_id, .. }
            | VariableHeader::SealExtentNodeCommitResp { stream_id, .. }
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
            VariableHeader::RegisterExtent { config, .. } => config.stream_id,
            _ => StreamId(0),
        }
    }

    /// Get the offset for this frame (Offset(0) for opcodes without offset).
    pub fn offset(&self) -> Offset {
        match &self.variable_header {
            VariableHeader::AppendAck { offset, .. }
            | VariableHeader::ReadResp { offset, .. }
            | VariableHeader::ReadRespError { offset, .. }
            | VariableHeader::SealStreamManagerResp { offset, .. }
            | VariableHeader::QueryOffsetResp { offset, .. }
            | VariableHeader::Watermark { offset, .. }
            | VariableHeader::Forward { offset, .. }
            | VariableHeader::Seek { offset, .. }
            | VariableHeader::SeekResp { offset, .. }
            | VariableHeader::SeekRespError { offset, .. } => *offset,
            VariableHeader::Read { offset, .. } => *offset,
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
            | VariableHeader::SealExtentNodeResp { extent_id, .. }
            | VariableHeader::CreateStreamResp { extent_id, .. }
            | VariableHeader::RegisterExtentAck { extent_id, .. }
            | VariableHeader::RegisterExtentAckError { extent_id, .. }
            | VariableHeader::RegisterExtent { extent_id, .. }
            | VariableHeader::Forward { extent_id, .. }
            | VariableHeader::ForwardInitExtent { extent_id, .. }
            | VariableHeader::ForwardChecksum { extent_id, .. }
            | VariableHeader::FlushExtent { extent_id, .. }
            | VariableHeader::FlushExtentResp { extent_id, .. }
            | VariableHeader::FlushExtentRespError { extent_id, .. }
            | VariableHeader::SealExtentNodeCommit { extent_id, .. }
            | VariableHeader::SealExtentNodeCommitResp { extent_id, .. }
            | VariableHeader::Watermark { extent_id, .. }
            | VariableHeader::DescribeExtent { extent_id, .. }
            | VariableHeader::DescribeExtentRespError { extent_id, .. } => *extent_id,
            VariableHeader::SealExtentNodePrepare { extent_id_from, .. } => *extent_id_from,
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
            VariableHeader::SealStreamManagerRequest { epoch, .. }
            | VariableHeader::SealExtentNodePrepare { epoch, .. }
            | VariableHeader::SealExtentNodeResp { epoch, .. }
            | VariableHeader::SealExtentNodeCommit { epoch, .. } => *epoch,
            VariableHeader::SealStreamManagerResp { new_epoch, .. } => *new_epoch,
            VariableHeader::RegisterExtent { config, .. } => config.epoch,
            VariableHeader::UpdateExtentProgress { epoch, .. } => *epoch,
            VariableHeader::Forward { epoch, .. }
            | VariableHeader::ForwardInitExtent { epoch, .. }
            | VariableHeader::FlushExtent { epoch, .. } => *epoch,
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
            | VariableHeader::SealStreamManagerRespError { error_code, .. }
            | VariableHeader::SealExtentNodeRespError { error_code, .. }
            | VariableHeader::CreateStreamRespError { error_code, .. }
            | VariableHeader::QueryOffsetRespError { error_code, .. }
            | VariableHeader::ConnectAckError { error_code, .. }
            | VariableHeader::DisconnectAckError { error_code, .. }
            | VariableHeader::HeartbeatError { error_code, .. }
            | VariableHeader::RegisterExtentAckError { error_code, .. }
            | VariableHeader::ReportExtentsRespError { error_code, .. }
            | VariableHeader::DescribeStreamRespError { error_code, .. }
            | VariableHeader::DescribeExtentRespError { error_code, .. }
            | VariableHeader::SeekRespError { error_code, .. }
            | VariableHeader::FlushExtentRespError { error_code, .. } => *error_code as u16,
            _ => 0,
        }
    }

    pub fn is_error_response(&self) -> bool {
        self.flags() & FLAG_RESPONSE_ERROR != 0
    }

    /// Get the flags byte for this frame on the wire.
    ///
    /// For response/error variants, flags are computed from the variant type.
    /// For other opcodes, returns `header.flags`.
    pub fn flags(&self) -> u8 {
        let computed = match &self.variable_header {
            // ── Error responses: FLAG_RESPONSE_ERROR (0x80) ──
            VariableHeader::AppendAckError { .. }
            | VariableHeader::ReadRespError { .. }
            | VariableHeader::SealStreamManagerRespError { .. }
            | VariableHeader::SealExtentNodeRespError { .. }
            | VariableHeader::CreateStreamRespError { .. }
            | VariableHeader::QueryOffsetRespError { .. }
            | VariableHeader::ConnectAckError { .. }
            | VariableHeader::DisconnectAckError { .. }
            | VariableHeader::HeartbeatError { .. }
            | VariableHeader::RegisterExtentAckError { .. }
            | VariableHeader::ReportExtentsRespError { .. }
            | VariableHeader::DescribeStreamRespError { .. }
            | VariableHeader::DescribeExtentRespError { .. }
            | VariableHeader::SeekRespError { .. }
            | VariableHeader::FlushExtentRespError { .. } => FLAG_RESPONSE_ERROR,
            // ── Success responses: FLAG_RESPONSE (0x01) ──
            VariableHeader::AppendAck { .. }
            | VariableHeader::ReadResp { .. }
            | VariableHeader::SealStreamManagerResp { .. }
            | VariableHeader::SealExtentNodeResp { .. }
            | VariableHeader::CreateStreamResp { .. }
            | VariableHeader::QueryOffsetResp { .. }
            | VariableHeader::ConnectAck { .. }
            | VariableHeader::DisconnectAck { .. }
            | VariableHeader::RegisterExtentAck { .. }
            | VariableHeader::ReportExtentsResp { .. }
            | VariableHeader::DescribeStreamResp { .. }
            | VariableHeader::DescribeExtentResp { .. }
            | VariableHeader::SeekResp { .. }
            | VariableHeader::FlushExtentResp { .. } => FLAG_RESPONSE,
            // ── Per-opcode request-side flags ──
            VariableHeader::UpdateExtentProgress { .. } => FLAG_EXTENT_PROGRESS,
            VariableHeader::UpdateExtentFlushed { .. } => FLAG_EXTENT_FLUSHED,
            VariableHeader::Forward { .. } => FLAG_FORWARD_APPEND,
            VariableHeader::ForwardInitExtent { .. } => FLAG_FORWARD_INIT_EXTENT,
            VariableHeader::ForwardChecksum { .. } => FLAG_FORWARD_CHECKSUM,
            VariableHeader::ForwardFlushed { .. } => FLAG_FORWARD_FLUSHED,
            VariableHeader::DescribeStream {
                stream_name: Some(_),
                ..
            } => FLAG_DESCRIBE_STREAM_BY_NAME,
            VariableHeader::SealExtentNodeCommit { .. } => FLAG_SEAL_COMMIT,
            VariableHeader::SealExtentNodeCommitResp { .. } => FLAG_SEAL_COMMIT_RESP,
            // ── Requests and fire-and-forget: 0x00 ──
            _ => 0,
        };
        self.header.flags | computed
    }
}
