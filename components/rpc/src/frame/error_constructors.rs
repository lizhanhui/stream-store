use bytes::Bytes;
use common::types::{Epoch, ErrorCode, Offset, StreamId};

use super::{Frame, VariableHeader};

impl Frame {
    pub fn append_ack_error(
        request_id: u32,
        stream_id: StreamId,
        epoch: Epoch,
        error_code: ErrorCode,
        message: &str,
    ) -> Frame {
        Frame::new(
            VariableHeader::AppendAckError {
                request_id,
                stream_id,
                epoch,
                error_code,
            },
            Some(Bytes::copy_from_slice(message.as_bytes())),
        )
    }

    pub fn read_resp_error(
        request_id: u32,
        stream_id: StreamId,
        offset: Offset,
        error_code: ErrorCode,
        message: &str,
    ) -> Frame {
        Frame::new(
            VariableHeader::ReadRespError {
                request_id,
                stream_id,
                offset,
                error_code,
            },
            Some(Bytes::copy_from_slice(message.as_bytes())),
        )
    }

    pub fn seal_stream_manager_resp_error(
        request_id: u32,
        stream_id: StreamId,
        error_code: ErrorCode,
        message: &str,
    ) -> Frame {
        Frame::new(
            VariableHeader::SealStreamRespError {
                request_id,
                stream_id,
                error_code,
            },
            Some(Bytes::copy_from_slice(message.as_bytes())),
        )
    }

    pub fn seal_epoch_resp_error(
        request_id: u32,
        stream_id: StreamId,
        error_code: ErrorCode,
        message: &str,
    ) -> Frame {
        Frame::new(
            VariableHeader::SealEpochRespError {
                request_id,
                stream_id,
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

    pub fn register_epoch_ack_error(
        request_id: u32,
        stream_id: StreamId,
        error_code: ErrorCode,
        message: &str,
    ) -> Frame {
        Frame::new(
            VariableHeader::RegisterEpochAckError {
                request_id,
                stream_id,
                error_code,
            },
            Some(Bytes::copy_from_slice(message.as_bytes())),
        )
    }

    pub fn report_epoch_resp_error(
        request_id: u32,
        stream_id: StreamId,
        epoch: Epoch,
        error_code: ErrorCode,
        message: &str,
    ) -> Frame {
        Frame::new(
            VariableHeader::ReportEpochRespError {
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

    pub fn describe_epoch_resp_error(
        request_id: u32,
        stream_id: StreamId,
        error_code: ErrorCode,
        message: &str,
    ) -> Frame {
        Frame::new(
            VariableHeader::DescribeEpochRespError {
                request_id,
                stream_id,
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

    pub fn flush_epoch_resp_error(
        request_id: u32,
        stream_id: StreamId,
        error_code: ErrorCode,
        message: &str,
    ) -> Frame {
        Frame::new(
            VariableHeader::FlushEpochRespError {
                request_id,
                stream_id,
                error_code,
            },
            Some(Bytes::copy_from_slice(message.as_bytes())),
        )
    }

    pub fn error_from_request(request: &Frame, error_code: ErrorCode, message: &str) -> Frame {
        match &request.variable_header {
            VariableHeader::Append {
                request_id,
                stream_id,
                epoch,
            } => Self::append_ack_error(*request_id, *stream_id, *epoch, error_code, message),
            VariableHeader::Read {
                request_id,
                stream_id,
                offset,
                ..
            } => Self::read_resp_error(*request_id, *stream_id, *offset, error_code, message),
            VariableHeader::SealStreamRequest {
                request_id,
                stream_id,
                ..
            } => Self::seal_stream_manager_resp_error(*request_id, *stream_id, error_code, message),
            VariableHeader::SealEpochPrepare {
                request_id,
                stream_id,
                ..
            } => Self::seal_epoch_resp_error(*request_id, *stream_id, error_code, message),
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
            VariableHeader::RegisterEpoch {
                request_id, config, ..
            } => Self::register_epoch_ack_error(*request_id, config.stream_id, error_code, message),
            VariableHeader::ReportEpoch {
                request_id,
                stream_id,
                epoch,
            } => {
                Self::report_epoch_resp_error(*request_id, *stream_id, *epoch, error_code, message)
            }
            VariableHeader::DescribeStream {
                request_id,
                stream_id,
                ..
            } => Self::describe_stream_resp_error(*request_id, *stream_id, error_code, message),
            VariableHeader::DescribeEpoch {
                request_id,
                stream_id,
            } => Self::describe_epoch_resp_error(*request_id, *stream_id, error_code, message),
            VariableHeader::Seek {
                request_id,
                stream_id,
                offset,
            } => Self::seek_resp_error(*request_id, *stream_id, *offset, error_code, message),
            VariableHeader::FlushEpoch {
                request_id,
                stream_id,
                ..
            } => Self::flush_epoch_resp_error(*request_id, *stream_id, error_code, message),
            _ => panic!(
                "no error response mapping for opcode {:?}",
                request.opcode()
            ),
        }
    }
}
