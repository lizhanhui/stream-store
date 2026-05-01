use bytes::{Buf, Bytes, BytesMut};
use common::errors::{InternalSnafu, InvalidFrameSnafu, StorageError, UnknownOpcodeSnafu};
use common::types::{
    Epoch, ErrorCode, ExtentId, ExtentPolicy, FLAG_DESCRIBE_STREAM_BY_NAME, FLAG_EXTENT_FLUSHED,
    FLAG_EXTENT_PROGRESS, FLAG_FORWARD_APPEND, FLAG_FORWARD_CHECKSUM,
    FLAG_FORWARD_FLUSHED, FLAG_FORWARD_INIT_EPOCH, FLAG_RESPONSE, FLAG_RESPONSE_ERROR,
    FLAG_SEAL_COMMIT, FLAG_SEAL_COMMIT_RESP, HEADER_LEN, MAGIC, Offset, Opcode, PROTOCOL_VERSION,
    StorageClass, StreamConfig, StreamId,
};

use super::{FixedHeader, Frame, VariableHeader};

impl Frame {
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
            return Err(InvalidFrameSnafu {
                message: format!("bad magic: expected 0x{MAGIC:02X}, got 0x{magic:02X}"),
            }
            .build());
        }

        let version = src.get_u8();
        if version != PROTOCOL_VERSION {
            return Err(InvalidFrameSnafu {
                message: format!("unsupported version: {version}"),
            }
            .build());
        }

        let opcode_byte = src.get_u8();
        let opcode = Opcode::from_u8(opcode_byte).ok_or_else(|| {
            UnknownOpcodeSnafu {
                opcode: opcode_byte,
            }
            .build()
        })?;

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
            // ── Append: request (0x00), ack (0x01), error (0x80) ──
            Opcode::Append => {
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u32());
                let epoch = Epoch(body.get_u32());
                if flags & FLAG_RESPONSE_ERROR != 0 {
                    let extent_id = ExtentId(body.get_u32());
                    let error_code = ErrorCode::from_u16(body.get_u16()).ok_or_else(|| {
                        InvalidFrameSnafu {
                            message: "unknown Append error code",
                        }
                        .build()
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
                } else if flags & FLAG_RESPONSE != 0 {
                    let extent_id = ExtentId(body.get_u32());
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
                } else {
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
            }
            // ── Read: request (0x00), response (0x01), error (0x80) ──
            Opcode::Read => {
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u32());
                if flags & FLAG_RESPONSE_ERROR != 0 {
                    let extent_id = ExtentId(body.get_u32());
                    let offset = Offset(body.get_u64());
                    let error_code = ErrorCode::from_u16(body.get_u16()).ok_or_else(|| {
                        InvalidFrameSnafu {
                            message: "unknown Read error code",
                        }
                        .build()
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
                } else if flags & FLAG_RESPONSE != 0 {
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
                } else {
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
            }
            // ── SealStream: request (0x00), response (0x01), error (0x80) ──
            Opcode::SealStream => {
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u32());
                if flags & FLAG_RESPONSE_ERROR != 0 {
                    let error_code = ErrorCode::from_u16(body.get_u16()).ok_or_else(|| {
                        InvalidFrameSnafu {
                            message: "unknown SealStream error code",
                        }
                        .build()
                    })?;
                    let payload = Self::read_payload(body);
                    Ok((
                        VariableHeader::SealStreamRespError {
                            request_id,
                            stream_id,
                            error_code,
                        },
                        payload,
                    ))
                } else if flags & FLAG_RESPONSE != 0 {
                    let offset = Offset(body.get_u64());
                    let new_epoch = Epoch(body.get_u32());
                    let addr_len = body.get_u16() as usize;
                    let primary_addr = body.split_to(addr_len).freeze();
                    Ok((
                        VariableHeader::SealStreamResp {
                            request_id,
                            stream_id,
                            offset,
                            new_epoch,
                            primary_addr,
                        },
                        None,
                    ))
                } else {
                    let epoch = Epoch(body.get_u32());
                    Ok((
                        VariableHeader::SealStreamRequest {
                            request_id,
                            stream_id,
                            epoch,
                        },
                        None,
                    ))
                }
            }
            // ── SealEpoch: prepare (0x00), response (0x01), error (0x80), commit (0x02), commit_resp (0x03) ──
            Opcode::SealEpoch => {
                // Commit (phase 2) and commit_resp have a different wire layout.
                // Check for them first before consuming shared fields.
                if flags == FLAG_SEAL_COMMIT_RESP {
                    let request_id = body.get_u32();
                    let stream_id = StreamId(body.get_u32());
                    let extent_id = ExtentId(body.get_u32());
                    return Ok((
                        VariableHeader::SealEpochCommitResp {
                            request_id,
                            stream_id,
                            extent_id,
                        },
                        None,
                    ));
                }
                if flags & FLAG_SEAL_COMMIT != 0 {
                    let request_id = body.get_u32();
                    let stream_id = StreamId(body.get_u32());
                    let extent_id = ExtentId(body.get_u32());
                    let epoch = Epoch(body.get_u32());
                    let start_offset = body.get_u64();
                    let end_offset = body.get_u64();
                    return Ok((
                        VariableHeader::SealEpochCommit {
                            request_id,
                            stream_id,
                            extent_id,
                            epoch,
                            start_offset,
                            end_offset,
                        },
                        None,
                    ));
                }
                // Prepare/Response/Error all start with request_id + stream_id.
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u32());
                if flags & FLAG_RESPONSE_ERROR != 0 {
                    let error_code = ErrorCode::from_u16(body.get_u16()).ok_or_else(|| {
                        InvalidFrameSnafu {
                            message: "unknown SealEpoch error code",
                        }
                        .build()
                    })?;
                    let payload = Self::read_payload(body);
                    Ok((
                        VariableHeader::SealEpochRespError {
                            request_id,
                            stream_id,
                            error_code,
                        },
                        payload,
                    ))
                } else if flags & FLAG_RESPONSE != 0 {
                    let epoch = Epoch(body.get_u32());
                    let extent_id = ExtentId(body.get_u32());
                    let start_offset = body.get_u64();
                    let end_offset = body.get_u64();
                    let payload = Self::read_payload(body);
                    Ok((
                        VariableHeader::SealEpochResp {
                            request_id,
                            stream_id,
                            epoch,
                            extent_id,
                            start_offset,
                            end_offset,
                        },
                        payload,
                    ))
                } else if flags & FLAG_SEAL_COMMIT != 0 {
                    // unreachable — handled above, but keep for safety.
                    Err(InternalSnafu {
                        message: "duplicate FLAG_SEAL_COMMIT branch",
                    }
                    .build())
                } else {
                    let epoch = Epoch(body.get_u32());
                    let extent_id_from = ExtentId(body.get_u32());
                    let start_offset = body.get_u64();
                    Ok((
                        VariableHeader::SealEpochPrepare {
                            request_id,
                            stream_id,
                            epoch,
                            extent_id_from,
                            start_offset,
                        },
                        None,
                    ))
                }
            }
            // ── CreateStream: request (0x00), response (0x01), error (0x80) ──
            Opcode::CreateStream => {
                let request_id = body.get_u32();
                if flags & FLAG_RESPONSE_ERROR != 0 {
                    let error_code = ErrorCode::from_u16(body.get_u16()).ok_or_else(|| {
                        InvalidFrameSnafu {
                            message: "unknown CreateStream error code",
                        }
                        .build()
                    })?;
                    let payload = Self::read_payload(body);
                    Ok((
                        VariableHeader::CreateStreamRespError {
                            request_id,
                            error_code,
                        },
                        payload,
                    ))
                } else if flags & FLAG_RESPONSE != 0 {
                    let stream_id = StreamId(body.get_u32());
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
                } else {
                    let name_len = body.get_u16() as usize;
                    let stream_name = body.split_to(name_len).freeze();
                    let replication_factor = body.get_u8();
                    let cache = body.get_u16();
                    let storage_class = StorageClass::from_u8(body.get_u8()).ok_or_else(|| {
                        InvalidFrameSnafu {
                            message: "unknown storage class",
                        }
                        .build()
                    })?;
                    Ok((
                        VariableHeader::CreateStream {
                            request_id,
                            stream_name,
                            replication_factor,
                            storage_class,
                            policy: ExtentPolicy {
                                cache,
                            },
                        },
                        None,
                    ))
                }
            }
            // ── QueryOffset: request (0x00), response (0x01), error (0x80) ──
            Opcode::QueryOffset => {
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u32());
                if flags & FLAG_RESPONSE_ERROR != 0 {
                    let error_code = ErrorCode::from_u16(body.get_u16()).ok_or_else(|| {
                        InvalidFrameSnafu {
                            message: "unknown QueryOffset error code",
                        }
                        .build()
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
                } else if flags & FLAG_RESPONSE != 0 {
                    let offset = Offset(body.get_u64());
                    Ok((
                        VariableHeader::QueryOffsetResp {
                            request_id,
                            stream_id,
                            offset,
                        },
                        None,
                    ))
                } else {
                    Ok((
                        VariableHeader::QueryOffset {
                            request_id,
                            stream_id,
                        },
                        None,
                    ))
                }
            }
            // ── Connect: request (0x00), ack (0x01), error (0x80) ──
            Opcode::Connect => {
                let request_id = body.get_u32();
                if flags & FLAG_RESPONSE_ERROR != 0 {
                    let error_code = ErrorCode::from_u16(body.get_u16()).ok_or_else(|| {
                        InvalidFrameSnafu {
                            message: "unknown Connect error code",
                        }
                        .build()
                    })?;
                    let payload = Self::read_payload(body);
                    Ok((
                        VariableHeader::ConnectAckError {
                            request_id,
                            error_code,
                        },
                        payload,
                    ))
                } else if flags & FLAG_RESPONSE != 0 {
                    Ok((VariableHeader::ConnectAck { request_id }, None))
                } else {
                    let payload = Self::read_payload(body);
                    Ok((VariableHeader::Connect { request_id }, payload))
                }
            }
            // ── Disconnect: request (0x00), ack (0x01), error (0x80) ──
            Opcode::Disconnect => {
                let request_id = body.get_u32();
                if flags & FLAG_RESPONSE_ERROR != 0 {
                    let error_code = ErrorCode::from_u16(body.get_u16()).ok_or_else(|| {
                        InvalidFrameSnafu {
                            message: "unknown Disconnect error code",
                        }
                        .build()
                    })?;
                    let payload = Self::read_payload(body);
                    Ok((
                        VariableHeader::DisconnectAckError {
                            request_id,
                            error_code,
                        },
                        payload,
                    ))
                } else if flags & FLAG_RESPONSE != 0 {
                    Ok((VariableHeader::DisconnectAck { request_id }, None))
                } else {
                    let payload = Self::read_payload(body);
                    Ok((VariableHeader::Disconnect { request_id }, payload))
                }
            }
            // ── Heartbeat: request/echo (0x00), error (0x80) ──
            Opcode::Heartbeat => {
                let request_id = body.get_u32();
                if flags & FLAG_RESPONSE_ERROR != 0 {
                    let error_code = ErrorCode::from_u16(body.get_u16()).ok_or_else(|| {
                        InvalidFrameSnafu {
                            message: "unknown Heartbeat error code",
                        }
                        .build()
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
            // ── RegisterEpoch: request (0x00), ack (0x01), error (0x80) ──
            Opcode::RegisterEpoch => {
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u32());
                let extent_id = ExtentId(body.get_u32());
                if flags & FLAG_RESPONSE_ERROR != 0 {
                    let error_code = ErrorCode::from_u16(body.get_u16()).ok_or_else(|| {
                        InvalidFrameSnafu {
                            message: "unknown RegisterEpoch error code",
                        }
                        .build()
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
                } else if flags & FLAG_RESPONSE != 0 {
                    Ok((
                        VariableHeader::RegisterEpochAck {
                            request_id,
                            stream_id,
                            extent_id,
                        },
                        None,
                    ))
                } else {
                    let role = body.get_u8();
                    let replication_factor = body.get_u8();
                    let epoch = Epoch(body.get_u32());
                    let cache = body.get_u16();
                    let storage_class = StorageClass::from_u8(body.get_u8()).ok_or_else(|| {
                        InvalidFrameSnafu {
                            message: "unknown storage class",
                        }
                        .build()
                    })?;
                    let payload = Self::read_payload(body);
                    Ok((
                        VariableHeader::RegisterEpoch {
                            request_id,
                            extent_id,
                            role,
                            config: StreamConfig {
                                stream_id,
                                replication_factor,
                                epoch,
                                storage_class,
                                policy: ExtentPolicy {
                                    cache,
                                },
                            },
                        },
                        payload,
                    ))
                }
            }
            // ── Watermark: fire-and-forget ──
            Opcode::Watermark => {
                let stream_id = StreamId(body.get_u32());
                let extent_id = ExtentId(body.get_u32());
                let epoch = Epoch(body.get_u32());
                let offset = Offset(body.get_u64());
                Ok((
                    VariableHeader::Watermark {
                        stream_id,
                        extent_id,
                        epoch,
                        offset,
                    },
                    None,
                ))
            }
            // ── Forward: flag-based variants (append/init/checksum/flushed) ──
            Opcode::Forward => {
                let stream_id = StreamId(body.get_u32());
                let extent_id = ExtentId(body.get_u32());
                let epoch = Epoch(body.get_u32());
                match flags {
                    FLAG_FORWARD_CHECKSUM => {
                        let checksum = body.get_u32();
                        let committed_bytes = body.get_u64();
                        Ok((
                            VariableHeader::ForwardChecksum {
                                stream_id,
                                extent_id,
                                epoch,
                                checksum,
                                committed_bytes,
                            },
                            None,
                        ))
                    }
                    FLAG_FORWARD_FLUSHED => Ok((
                        VariableHeader::ForwardFlushed {
                            stream_id,
                            extent_id,
                            epoch,
                        },
                        None,
                    )),
                    FLAG_FORWARD_APPEND => {
                        let offset = Offset(body.get_u64());
                        let payload = Self::read_payload(body);
                        Ok((
                            VariableHeader::Forward {
                                stream_id,
                                extent_id,
                                epoch,
                                offset,
                            },
                            payload,
                        ))
                    }
                    FLAG_FORWARD_INIT_EPOCH => {
                        let start_offset = Offset(body.get_u64());
                        let extent_capacity = body.get_u32();
                        let cache_extents = body.get_u16();
                        let storage_class =
                            StorageClass::from_u8(body.get_u8()).ok_or_else(|| {
                                InvalidFrameSnafu {
                                    message: "unknown storage class",
                                }
                                .build()
                            })?;
                        Ok((
                            VariableHeader::ForwardInitEpoch {
                                stream_id,
                                extent_id,
                                epoch,
                                start_offset,
                                extent_capacity,
                                cache_extents,
                                storage_class,
                            },
                            None,
                        ))
                    }
                    _ => Err(InternalSnafu {
                        message: format!("unknown Forward flag: {flags:#x}"),
                    }
                    .build()),
                }
            }
            // ── StreamManagerMembershipChange: fire-and-forget ──
            Opcode::StreamManagerMembershipChange => {
                let payload = Self::read_payload(body);
                Ok((VariableHeader::StreamManagerMembershipChange, payload))
            }
            // ── DescribeStream: request (0x00 or 0x02 for by-name), response (0x01), error (0x80) ──
            Opcode::DescribeStream => {
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u32());
                if flags & FLAG_RESPONSE_ERROR != 0 {
                    let error_code = ErrorCode::from_u16(body.get_u16()).ok_or_else(|| {
                        InvalidFrameSnafu {
                            message: "unknown DescribeStream error code",
                        }
                        .build()
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
                } else if flags & FLAG_RESPONSE != 0 {
                    let payload = Self::read_payload(body);
                    Ok((
                        VariableHeader::DescribeStreamResp {
                            request_id,
                            stream_id,
                        },
                        payload,
                    ))
                } else {
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
            }
            // ── DescribeExtent: request (0x00), response (0x01), error (0x80) ──
            Opcode::DescribeExtent => {
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u32());
                if flags & FLAG_RESPONSE_ERROR != 0 {
                    let extent_id = ExtentId(body.get_u32());
                    let error_code = ErrorCode::from_u16(body.get_u16()).ok_or_else(|| {
                        InvalidFrameSnafu {
                            message: "unknown DescribeExtent error code",
                        }
                        .build()
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
                } else if flags & FLAG_RESPONSE != 0 {
                    let payload = Self::read_payload(body);
                    Ok((
                        VariableHeader::DescribeExtentResp {
                            request_id,
                            stream_id,
                        },
                        payload,
                    ))
                } else {
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
            }
            // ── Seek: request (0x00), response (0x01), error (0x80) ──
            Opcode::Seek => {
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u32());
                let offset = Offset(body.get_u64());
                if flags & FLAG_RESPONSE_ERROR != 0 {
                    let error_code = ErrorCode::from_u16(body.get_u16()).ok_or_else(|| {
                        InvalidFrameSnafu {
                            message: "unknown Seek error code",
                        }
                        .build()
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
                } else if flags & FLAG_RESPONSE != 0 {
                    let payload = Self::read_payload(body);
                    Ok((
                        VariableHeader::SeekResp {
                            request_id,
                            stream_id,
                            offset,
                        },
                        payload,
                    ))
                } else {
                    Ok((
                        VariableHeader::Seek {
                            request_id,
                            stream_id,
                            offset,
                        },
                        None,
                    ))
                }
            }
            // ── UpdateExtent: fire-and-forget, flag-based variants ──
            Opcode::UpdateExtent => {
                let stream_id = StreamId(body.get_u32());
                let epoch = Epoch(body.get_u32());
                match flags {
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
                    FLAG_EXTENT_FLUSHED => {
                        let extent_id = ExtentId(body.get_u32());
                        let start_offset = Offset(body.get_u64());
                        let end_offset = Offset(body.get_u64());
                        Ok((
                            VariableHeader::UpdateExtentFlushed {
                                stream_id,
                                epoch,
                                extent_id,
                                start_offset,
                                end_offset,
                            },
                            None,
                        ))
                    }
                    _ => Err(InternalSnafu {
                        message: format!("unknown UpdateExtent flag: {flags:#x}"),
                    }
                    .build()),
                }
            }
            // ── ReportExtents: request (0x00), response (0x01), error (0x80) ──
            Opcode::ReportExtents => {
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u32());
                let epoch = Epoch(body.get_u32());
                if flags & FLAG_RESPONSE_ERROR != 0 {
                    let error_code = ErrorCode::from_u16(body.get_u16()).ok_or_else(|| {
                        InvalidFrameSnafu {
                            message: "unknown ReportExtents error code",
                        }
                        .build()
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
                } else if flags & FLAG_RESPONSE != 0 {
                    let payload = Self::read_payload(body);
                    Ok((
                        VariableHeader::ReportExtentsResp {
                            request_id,
                            stream_id,
                            epoch,
                        },
                        payload,
                    ))
                } else {
                    Ok((
                        VariableHeader::ReportExtents {
                            request_id,
                            stream_id,
                            epoch,
                        },
                        None,
                    ))
                }
            }
            // ── FlushExtent: request (0x00), response (0x01), error (0x80) ──
            Opcode::FlushExtent => {
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u32());
                let extent_id = ExtentId(body.get_u32());
                if flags & FLAG_RESPONSE_ERROR != 0 {
                    let error_code = ErrorCode::from_u16(body.get_u16()).ok_or_else(|| {
                        InvalidFrameSnafu {
                            message: "unknown FlushExtent error code",
                        }
                        .build()
                    })?;
                    let payload = Self::read_payload(body);
                    Ok((
                        VariableHeader::FlushExtentRespError {
                            request_id,
                            stream_id,
                            extent_id,
                            error_code,
                        },
                        payload,
                    ))
                } else if flags & FLAG_RESPONSE != 0 {
                    Ok((
                        VariableHeader::FlushExtentResp {
                            request_id,
                            stream_id,
                            extent_id,
                        },
                        None,
                    ))
                } else {
                    let epoch = Epoch(body.get_u32());
                    let start_offset = body.get_u64();
                    let end_offset = body.get_u64();
                    Ok((
                        VariableHeader::FlushExtent {
                            request_id,
                            stream_id,
                            extent_id,
                            epoch,
                            start_offset,
                            end_offset,
                        },
                        None,
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
