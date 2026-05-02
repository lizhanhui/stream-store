pub mod header;

mod decode;
mod encode;
mod error_constructors;

#[cfg(test)]
mod tests;

use bytes::Bytes;
use common::types::{
    Epoch, FLAG_DESCRIBE_STREAM_BY_NAME, FLAG_EPOCH_FLUSHED, FLAG_EPOCH_PROGRESS,
    FLAG_FORWARD_APPEND, FLAG_FORWARD_CHECKSUM, FLAG_FORWARD_FLUSHED, FLAG_FORWARD_INIT_EPOCH,
    FLAG_RESPONSE, FLAG_RESPONSE_ERROR, FLAG_SEAL_COMMIT, FLAG_SEAL_COMMIT_RESP, Offset, Opcode,
    PROTOCOL_VERSION, StreamId,
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
            | VariableHeader::SealStreamRequest { request_id, .. }
            | VariableHeader::SealStreamResp { request_id, .. }
            | VariableHeader::SealStreamRespError { request_id, .. }
            | VariableHeader::SealEpochPrepare { request_id, .. }
            | VariableHeader::SealEpochResp { request_id, .. }
            | VariableHeader::SealEpochRespError { request_id, .. }
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
            | VariableHeader::RegisterEpoch { request_id, .. }
            | VariableHeader::RegisterEpochAck { request_id, .. }
            | VariableHeader::RegisterEpochAckError { request_id, .. }
            | VariableHeader::ReportEpoch { request_id, .. }
            | VariableHeader::ReportEpochResp { request_id, .. }
            | VariableHeader::ReportEpochRespError { request_id, .. }
            | VariableHeader::DescribeStream { request_id, .. }
            | VariableHeader::DescribeStreamResp { request_id, .. }
            | VariableHeader::DescribeStreamRespError { request_id, .. }
            | VariableHeader::DescribeEpoch { request_id, .. }
            | VariableHeader::DescribeEpochResp { request_id, .. }
            | VariableHeader::DescribeEpochRespError { request_id, .. }
            | VariableHeader::Seek { request_id, .. }
            | VariableHeader::SeekResp { request_id, .. }
            | VariableHeader::SeekRespError { request_id, .. }
            | VariableHeader::FlushEpoch { request_id, .. }
            | VariableHeader::FlushEpochResp { request_id, .. }
            | VariableHeader::FlushEpochRespError { request_id, .. }
            | VariableHeader::SealEpochCommit { request_id, .. }
            | VariableHeader::SealEpochCommitResp { request_id, .. } => *request_id,
            VariableHeader::Watermark { .. }
            | VariableHeader::Forward { .. }
            | VariableHeader::ForwardInitEpoch { .. }
            | VariableHeader::ForwardChecksum { .. }
            | VariableHeader::ForwardFlushed { .. }
            | VariableHeader::UpdateEpochProgress { .. }
            | VariableHeader::UpdateEpochFlushed { .. }
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
            | VariableHeader::SealStreamRequest { stream_id, .. }
            | VariableHeader::SealStreamResp { stream_id, .. }
            | VariableHeader::SealStreamRespError { stream_id, .. }
            | VariableHeader::SealEpochPrepare { stream_id, .. }
            | VariableHeader::SealEpochResp { stream_id, .. }
            | VariableHeader::SealEpochRespError { stream_id, .. }
            | VariableHeader::CreateStreamResp { stream_id, .. }
            | VariableHeader::QueryOffset { stream_id, .. }
            | VariableHeader::QueryOffsetResp { stream_id, .. }
            | VariableHeader::QueryOffsetRespError { stream_id, .. }
            | VariableHeader::RegisterEpochAck { stream_id, .. }
            | VariableHeader::RegisterEpochAckError { stream_id, .. }
            | VariableHeader::Watermark { stream_id, .. }
            | VariableHeader::Forward { stream_id, .. }
            | VariableHeader::ForwardInitEpoch { stream_id, .. }
            | VariableHeader::ForwardChecksum { stream_id, .. }
            | VariableHeader::ForwardFlushed { stream_id, .. }
            | VariableHeader::FlushEpoch { stream_id, .. }
            | VariableHeader::FlushEpochResp { stream_id, .. }
            | VariableHeader::FlushEpochRespError { stream_id, .. }
            | VariableHeader::SealEpochCommit { stream_id, .. }
            | VariableHeader::SealEpochCommitResp { stream_id, .. }
            | VariableHeader::UpdateEpochProgress { stream_id, .. }
            | VariableHeader::UpdateEpochFlushed { stream_id, .. }
            | VariableHeader::ReportEpoch { stream_id, .. }
            | VariableHeader::ReportEpochResp { stream_id, .. }
            | VariableHeader::ReportEpochRespError { stream_id, .. }
            | VariableHeader::DescribeStream { stream_id, .. }
            | VariableHeader::DescribeStreamResp { stream_id, .. }
            | VariableHeader::DescribeStreamRespError { stream_id, .. }
            | VariableHeader::DescribeEpoch { stream_id, .. }
            | VariableHeader::DescribeEpochResp { stream_id, .. }
            | VariableHeader::DescribeEpochRespError { stream_id, .. }
            | VariableHeader::Seek { stream_id, .. }
            | VariableHeader::SeekResp { stream_id, .. }
            | VariableHeader::SeekRespError { stream_id, .. } => *stream_id,
            VariableHeader::RegisterEpoch { config, .. } => config.stream_id,
            _ => StreamId(0),
        }
    }

    /// Get the offset for this frame (Offset(0) for opcodes without offset).
    pub fn offset(&self) -> Offset {
        match &self.variable_header {
            VariableHeader::AppendAck { offset, .. }
            | VariableHeader::ReadResp { offset, .. }
            | VariableHeader::ReadRespError { offset, .. }
            | VariableHeader::SealStreamResp { offset, .. }
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

    /// Get the epoch for this frame (Epoch(0) for opcodes without epoch).
    pub fn epoch(&self) -> Epoch {
        match &self.variable_header {
            VariableHeader::Append { epoch, .. }
            | VariableHeader::AppendAck { epoch, .. }
            | VariableHeader::AppendAckError { epoch, .. }
            | VariableHeader::CreateStreamResp { epoch, .. } => *epoch,
            VariableHeader::SealStreamRequest { epoch, .. }
            | VariableHeader::SealEpochPrepare { epoch, .. }
            | VariableHeader::SealEpochResp { epoch, .. }
            | VariableHeader::SealEpochCommit { epoch, .. } => *epoch,
            VariableHeader::SealStreamResp { new_epoch, .. } => *new_epoch,
            VariableHeader::RegisterEpoch { config, .. } => config.epoch,
            VariableHeader::UpdateEpochProgress { epoch, .. }
            | VariableHeader::UpdateEpochFlushed { epoch, .. } => *epoch,
            VariableHeader::Watermark { epoch, .. } => *epoch,
            VariableHeader::Forward { epoch, .. }
            | VariableHeader::ForwardInitEpoch { epoch, .. }
            | VariableHeader::ForwardChecksum { epoch, .. }
            | VariableHeader::ForwardFlushed { epoch, .. }
            | VariableHeader::FlushEpoch { epoch, .. } => *epoch,
            VariableHeader::ReportEpoch { epoch, .. }
            | VariableHeader::ReportEpochResp { epoch, .. }
            | VariableHeader::ReportEpochRespError { epoch, .. } => *epoch,
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
            | VariableHeader::SealStreamRespError { error_code, .. }
            | VariableHeader::SealEpochRespError { error_code, .. }
            | VariableHeader::CreateStreamRespError { error_code, .. }
            | VariableHeader::QueryOffsetRespError { error_code, .. }
            | VariableHeader::ConnectAckError { error_code, .. }
            | VariableHeader::DisconnectAckError { error_code, .. }
            | VariableHeader::HeartbeatError { error_code, .. }
            | VariableHeader::RegisterEpochAckError { error_code, .. }
            | VariableHeader::ReportEpochRespError { error_code, .. }
            | VariableHeader::DescribeStreamRespError { error_code, .. }
            | VariableHeader::DescribeEpochRespError { error_code, .. }
            | VariableHeader::SeekRespError { error_code, .. }
            | VariableHeader::FlushEpochRespError { error_code, .. } => *error_code as u16,
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
            | VariableHeader::SealStreamRespError { .. }
            | VariableHeader::SealEpochRespError { .. }
            | VariableHeader::CreateStreamRespError { .. }
            | VariableHeader::QueryOffsetRespError { .. }
            | VariableHeader::ConnectAckError { .. }
            | VariableHeader::DisconnectAckError { .. }
            | VariableHeader::HeartbeatError { .. }
            | VariableHeader::RegisterEpochAckError { .. }
            | VariableHeader::ReportEpochRespError { .. }
            | VariableHeader::DescribeStreamRespError { .. }
            | VariableHeader::DescribeEpochRespError { .. }
            | VariableHeader::SeekRespError { .. }
            | VariableHeader::FlushEpochRespError { .. } => FLAG_RESPONSE_ERROR,
            // ── Success responses: FLAG_RESPONSE (0x01) ──
            VariableHeader::AppendAck { .. }
            | VariableHeader::ReadResp { .. }
            | VariableHeader::SealStreamResp { .. }
            | VariableHeader::SealEpochResp { .. }
            | VariableHeader::CreateStreamResp { .. }
            | VariableHeader::QueryOffsetResp { .. }
            | VariableHeader::ConnectAck { .. }
            | VariableHeader::DisconnectAck { .. }
            | VariableHeader::RegisterEpochAck { .. }
            | VariableHeader::ReportEpochResp { .. }
            | VariableHeader::DescribeStreamResp { .. }
            | VariableHeader::DescribeEpochResp { .. }
            | VariableHeader::SeekResp { .. }
            | VariableHeader::FlushEpochResp { .. } => FLAG_RESPONSE,
            // ── Per-opcode request-side flags ──
            VariableHeader::UpdateEpochProgress { .. } => FLAG_EPOCH_PROGRESS,
            VariableHeader::UpdateEpochFlushed { .. } => FLAG_EPOCH_FLUSHED,
            VariableHeader::Forward { .. } => FLAG_FORWARD_APPEND,
            VariableHeader::ForwardInitEpoch { .. } => FLAG_FORWARD_INIT_EPOCH,
            VariableHeader::ForwardChecksum { .. } => FLAG_FORWARD_CHECKSUM,
            VariableHeader::ForwardFlushed { .. } => FLAG_FORWARD_FLUSHED,
            VariableHeader::DescribeStream {
                stream_name: Some(_),
                ..
            } => FLAG_DESCRIBE_STREAM_BY_NAME,
            VariableHeader::SealEpochCommit { .. } => FLAG_SEAL_COMMIT,
            VariableHeader::SealEpochCommitResp { .. } => FLAG_SEAL_COMMIT_RESP,
            // ── Requests and fire-and-forget: 0x00 ──
            _ => 0,
        };
        self.header.flags | computed
    }
}
