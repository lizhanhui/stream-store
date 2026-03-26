use bytes::{Buf, BufMut, Bytes, BytesMut};
use common::errors::StorageError;
use common::types::{
    ErrorCode, ExtentId, FLAG_NEW_EXTENT_PRESENT, FLAG_OFFSET_PRESENT, HEADER_LEN,
    MAGIC, Offset, Opcode, PROTOCOL_VERSION, StreamId,
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
        extent_id: ExtentId,
    },
    AppendAck {
        request_id: u32,
        stream_id: StreamId,
        extent_id: ExtentId,
        offset: Offset,
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
    Seal {
        request_id: u32,
        stream_id: StreamId,
        extent_id: ExtentId,
        offset: Option<Offset>,
    },
    SealAck {
        request_id: u32,
        stream_id: StreamId,
        extent_id: ExtentId,
        offset: Offset,
        new_extent_id: Option<ExtentId>,
        primary_addr: Option<Bytes>,
    },
    CreateStream {
        request_id: u32,
    },
    CreateStreamResp {
        request_id: u32,
        stream_id: StreamId,
        extent_id: ExtentId,
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
    Connect {
        request_id: u32,
    },
    ConnectAck {
        request_id: u32,
    },
    Disconnect {
        request_id: u32,
    },
    DisconnectAck {
        request_id: u32,
    },
    Heartbeat {
        request_id: u32,
    },
    RegisterExtent {
        request_id: u32,
        stream_id: StreamId,
        extent_id: ExtentId,
        role: u8,
        replication_factor: u16,
    },
    RegisterExtentAck {
        request_id: u32,
        stream_id: StreamId,
        extent_id: ExtentId,
    },
    Watermark {
        stream_id: StreamId,
        offset: Offset,
    },
    StreamManagerMembershipChange,
    DescribeStream {
        request_id: u32,
        stream_id: StreamId,
        count: u32,
    },
    DescribeStreamResp {
        request_id: u32,
        stream_id: StreamId,
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
    Error {
        request_id: u32,
        error_code: u16,
        extent_id: ExtentId,
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
                opcode: Opcode::Error,
                version: PROTOCOL_VERSION,
                flags: 0,
            },
            variable_header: VariableHeader::Error {
                request_id: 0,
                error_code: 0,
                extent_id: ExtentId(0),
            },
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
            | VariableHeader::Read { request_id, .. }
            | VariableHeader::ReadResp { request_id, .. }
            | VariableHeader::Seal { request_id, .. }
            | VariableHeader::SealAck { request_id, .. }
            | VariableHeader::CreateStream { request_id }
            | VariableHeader::CreateStreamResp { request_id, .. }
            | VariableHeader::QueryOffset { request_id, .. }
            | VariableHeader::QueryOffsetResp { request_id, .. }
            | VariableHeader::Connect { request_id }
            | VariableHeader::ConnectAck { request_id }
            | VariableHeader::Disconnect { request_id }
            | VariableHeader::DisconnectAck { request_id }
            | VariableHeader::Heartbeat { request_id }
            | VariableHeader::RegisterExtent { request_id, .. }
            | VariableHeader::RegisterExtentAck { request_id, .. }
            | VariableHeader::DescribeStream { request_id, .. }
            | VariableHeader::DescribeStreamResp { request_id, .. }
            | VariableHeader::DescribeExtent { request_id, .. }
            | VariableHeader::DescribeExtentResp { request_id, .. }
            | VariableHeader::Seek { request_id, .. }
            | VariableHeader::SeekResp { request_id, .. }
            | VariableHeader::Error { request_id, .. } => *request_id,
            VariableHeader::Watermark { .. }
            | VariableHeader::StreamManagerMembershipChange => 0,
        }
    }

    /// Get the stream_id for this frame (StreamId(0) for opcodes without stream_id).
    pub fn stream_id(&self) -> StreamId {
        match &self.variable_header {
            VariableHeader::Append { stream_id, .. }
            | VariableHeader::AppendAck { stream_id, .. }
            | VariableHeader::Read { stream_id, .. }
            | VariableHeader::ReadResp { stream_id, .. }
            | VariableHeader::Seal { stream_id, .. }
            | VariableHeader::SealAck { stream_id, .. }
            | VariableHeader::CreateStreamResp { stream_id, .. }
            | VariableHeader::QueryOffset { stream_id, .. }
            | VariableHeader::QueryOffsetResp { stream_id, .. }
            | VariableHeader::RegisterExtentAck { stream_id, .. }
            | VariableHeader::RegisterExtent { stream_id, .. }
            | VariableHeader::Watermark { stream_id, .. }
            | VariableHeader::DescribeStream { stream_id, .. }
            | VariableHeader::DescribeStreamResp { stream_id, .. }
            | VariableHeader::DescribeExtent { stream_id, .. }
            | VariableHeader::DescribeExtentResp { stream_id, .. }
            | VariableHeader::Seek { stream_id, .. }
            | VariableHeader::SeekResp { stream_id, .. } => *stream_id,
            _ => StreamId(0),
        }
    }

    /// Get the offset for this frame (Offset(0) for opcodes without offset).
    pub fn offset(&self) -> Offset {
        match &self.variable_header {
            VariableHeader::AppendAck { offset, .. }
            | VariableHeader::ReadResp { offset, .. }
            | VariableHeader::SealAck { offset, .. }
            | VariableHeader::QueryOffsetResp { offset, .. }
            | VariableHeader::Watermark { offset, .. }
            | VariableHeader::Seek { offset, .. }
            | VariableHeader::SeekResp { offset, .. } => *offset,
            VariableHeader::Read { offset, .. } => *offset,
            VariableHeader::Seal { offset, .. } => offset.unwrap_or(Offset(0)),
            _ => Offset(0),
        }
    }

    /// Get the extent_id for this frame (ExtentId(0) for opcodes without extent_id).
    pub fn extent_id(&self) -> ExtentId {
        match &self.variable_header {
            VariableHeader::Append { extent_id, .. }
            | VariableHeader::AppendAck { extent_id, .. }
            | VariableHeader::Seal { extent_id, .. }
            | VariableHeader::SealAck { extent_id, .. }
            | VariableHeader::CreateStreamResp { extent_id, .. }
            | VariableHeader::RegisterExtentAck { extent_id, .. }
            | VariableHeader::RegisterExtent { extent_id, .. }
            | VariableHeader::DescribeExtent { extent_id, .. }
            | VariableHeader::Error { extent_id, .. } => *extent_id,
            _ => ExtentId(0),
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

    /// Get the error_code for this frame (0 for non-Error opcodes).
    pub fn error_code(&self) -> u16 {
        match &self.variable_header {
            VariableHeader::Error { error_code, .. } => *error_code,
            _ => 0,
        }
    }

    /// Get the flags byte for this frame on the wire.
    ///
    /// For Seal/SealAck, flags are computed from `Option` fields.
    /// For other opcodes, returns `header.flags` (e.g. FLAG_FORWARDED on Append).
    pub fn flags(&self) -> u8 {
        let computed = match &self.variable_header {
            VariableHeader::Seal { offset, .. } => {
                if offset.is_some() {
                    FLAG_OFFSET_PRESENT
                } else {
                    0
                }
            }
            VariableHeader::SealAck {
                new_extent_id, ..
            } => {
                if new_extent_id.is_some() {
                    FLAG_NEW_EXTENT_PRESENT
                } else {
                    0
                }
            }
            _ => 0,
        };
        self.header.flags | computed
    }

    /// Create an error response frame.
    ///
    /// For ExtentFull/ExtentSealed errors, pass the relevant extent_id so the
    /// client can identify which extent triggered the error without a round-trip.
    pub fn error_response(
        request_id: u32,
        code: ErrorCode,
        message: &str,
        extent_id: ExtentId,
    ) -> Frame {
        Frame::new(
            VariableHeader::Error {
                request_id,
                error_code: code as u16,
                extent_id,
            },
            Some(Bytes::copy_from_slice(message.as_bytes())),
        )
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
            // request_id(4) + stream_id(8) + extent_id(4)
            VariableHeader::Append { .. } => 4 + 8 + 4,
            // request_id(4) + stream_id(8) + extent_id(4) + offset(8)
            VariableHeader::AppendAck { .. } => 4 + 8 + 4 + 8,
            // request_id(4) + stream_id(8) + offset(8) + count(4)
            VariableHeader::Read { .. } => 4 + 8 + 8 + 4,
            // request_id(4) + stream_id(8) + offset(8) + count(4)
            VariableHeader::ReadResp { .. } => 4 + 8 + 8 + 4,
            // request_id(4) + stream_id(8) + extent_id(4) [+ offset(8) if present]
            VariableHeader::Seal { offset, .. } => {
                let base = 4 + 8 + 4;
                if offset.is_some() {
                    base + 8
                } else {
                    base
                }
            }
            // request_id(4) + stream_id(8) + extent_id(4) + offset(8)
            // [+ new_extent_id(4) + addr_len(2) + addr_bytes if FLAG_NEW_EXTENT_PRESENT]
            VariableHeader::SealAck {
                new_extent_id,
                primary_addr,
                ..
            } => {
                let base = 4 + 8 + 4 + 8;
                if new_extent_id.is_some() {
                    base + 4 + 2 + primary_addr.as_ref().map_or(0, |a| a.len())
                } else {
                    base
                }
            }
            // request_id(4)
            VariableHeader::CreateStream { .. } => 4,
            // request_id(4) + stream_id(8) + extent_id(4)
            VariableHeader::CreateStreamResp { .. } => 4 + 8 + 4,
            // request_id(4) + stream_id(8)
            VariableHeader::QueryOffset { .. } => 4 + 8,
            // request_id(4) + stream_id(8) + offset(8)
            VariableHeader::QueryOffsetResp { .. } => 4 + 8 + 8,
            // request_id(4)
            VariableHeader::Connect { .. }
            | VariableHeader::ConnectAck { .. }
            | VariableHeader::Disconnect { .. }
            | VariableHeader::DisconnectAck { .. }
            | VariableHeader::Heartbeat { .. } => 4,
            // request_id(4) + stream_id(8) + extent_id(4) + role(1) + replication_factor(2)
            VariableHeader::RegisterExtent { .. } => 4 + 8 + 4 + 1 + 2,
            // request_id(4) + stream_id(8) + extent_id(4)
            VariableHeader::RegisterExtentAck { .. } => 4 + 8 + 4,
            // stream_id(8) + offset(8) -- no request_id
            VariableHeader::Watermark { .. } => 8 + 8,
            // no variable header, just payload
            VariableHeader::StreamManagerMembershipChange => 0,
            // request_id(4) + stream_id(8) + count(4)
            VariableHeader::DescribeStream { .. } => 4 + 8 + 4,
            // request_id(4) + stream_id(8)
            VariableHeader::DescribeStreamResp { .. }
            | VariableHeader::DescribeExtentResp { .. } => 4 + 8,
            // request_id(4) + stream_id(8) + extent_id(4)
            VariableHeader::DescribeExtent { .. } => 4 + 8 + 4,
            // request_id(4) + stream_id(8) + offset(8)
            VariableHeader::Seek { .. } | VariableHeader::SeekResp { .. } => 4 + 8 + 8,
            // request_id(4) + error_code(2) + extent_id(4)
            VariableHeader::Error { .. } => 4 + 2 + 4,
        }
    }

    /// Whether this opcode has a payload section (u32 length prefix + bytes).
    fn has_payload_section(&self) -> bool {
        match &self.variable_header {
            VariableHeader::Append { .. }
            | VariableHeader::ReadResp { .. }
            | VariableHeader::CreateStream { .. }
            | VariableHeader::CreateStreamResp { .. }
            | VariableHeader::Connect { .. }
            | VariableHeader::Disconnect { .. }
            | VariableHeader::Heartbeat { .. }
            | VariableHeader::RegisterExtent { .. }
            | VariableHeader::StreamManagerMembershipChange
            | VariableHeader::DescribeStreamResp { .. }
            | VariableHeader::DescribeExtentResp { .. }
            | VariableHeader::SeekResp { .. }
            | VariableHeader::Error { .. } => true,
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
                extent_id,
            } => {
                dst.put_u32(*request_id);
                dst.put_u64(stream_id.0);
                dst.put_u32(extent_id.0);
            }
            VariableHeader::AppendAck {
                request_id,
                stream_id,
                extent_id,
                offset,
            } => {
                dst.put_u32(*request_id);
                dst.put_u64(stream_id.0);
                dst.put_u32(extent_id.0);
                dst.put_u64(offset.0);
            }
            VariableHeader::Read {
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
            VariableHeader::Seal {
                request_id,
                stream_id,
                extent_id,
                offset,
            } => {
                dst.put_u32(*request_id);
                dst.put_u64(stream_id.0);
                dst.put_u32(extent_id.0);
                if let Some(off) = offset {
                    dst.put_u64(off.0);
                }
            }
            VariableHeader::SealAck {
                request_id,
                stream_id,
                extent_id,
                offset,
                new_extent_id,
                primary_addr,
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
            }
            VariableHeader::CreateStream { request_id } => {
                dst.put_u32(*request_id);
            }
            VariableHeader::CreateStreamResp {
                request_id,
                stream_id,
                extent_id,
            } => {
                dst.put_u32(*request_id);
                dst.put_u64(stream_id.0);
                dst.put_u32(extent_id.0);
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
            } => {
                dst.put_u32(*request_id);
                dst.put_u64(stream_id.0);
                dst.put_u32(extent_id.0);
                dst.put_u8(*role);
                dst.put_u16(*replication_factor);
            }
            VariableHeader::ConnectAck { request_id }
            | VariableHeader::DisconnectAck { request_id } => {
                dst.put_u32(*request_id);
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
            VariableHeader::Watermark { stream_id, offset } => {
                dst.put_u64(stream_id.0);
                dst.put_u64(offset.0);
            }
            VariableHeader::StreamManagerMembershipChange => {
                // no variable header fields, just payload
            }
            VariableHeader::DescribeStream {
                request_id,
                stream_id,
                count,
            } => {
                dst.put_u32(*request_id);
                dst.put_u64(stream_id.0);
                dst.put_u32(*count);
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
            VariableHeader::Error {
                request_id,
                error_code,
                extent_id,
            } => {
                dst.put_u32(*request_id);
                dst.put_u16(*error_code);
                dst.put_u32(extent_id.0);
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

        let (variable_header, payload) = Self::decode_variable_header(opcode, flags, &mut body_buf)?;

        Ok(Some(Frame {
            header: FixedHeader { opcode, version, flags },
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
                let extent_id = ExtentId(body.get_u32());
                let payload = Self::read_payload(body);
                Ok((
                    VariableHeader::Append {
                        request_id,
                        stream_id,
                        extent_id,
                    },
                    payload,
                ))
            }
            Opcode::AppendAck => {
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u64());
                let extent_id = ExtentId(body.get_u32());
                let offset = Offset(body.get_u64());
                Ok((
                    VariableHeader::AppendAck {
                        request_id,
                        stream_id,
                        extent_id,
                        offset,
                    },
                    None,
                ))
            }
            Opcode::Read => {
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u64());
                let offset = Offset(body.get_u64());
                let count = body.get_u32();
                Ok((
                    VariableHeader::Read {
                        request_id,
                        stream_id,
                        offset,
                        count,
                    },
                    None,
                ))
            }
            Opcode::ReadResp => {
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u64());
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
            Opcode::Seal => {
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u64());
                let extent_id = ExtentId(body.get_u32());
                let offset = if flags & FLAG_OFFSET_PRESENT != 0 {
                    Some(Offset(body.get_u64()))
                } else {
                    None
                };
                Ok((
                    VariableHeader::Seal {
                        request_id,
                        stream_id,
                        extent_id,
                        offset,
                    },
                    None,
                ))
            }
            Opcode::SealAck => {
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u64());
                let extent_id = ExtentId(body.get_u32());
                let offset = Offset(body.get_u64());
                let (new_extent_id, primary_addr) = if flags & FLAG_NEW_EXTENT_PRESENT != 0 {
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
                Ok((
                    VariableHeader::SealAck {
                        request_id,
                        stream_id,
                        extent_id,
                        offset,
                        new_extent_id,
                        primary_addr,
                    },
                    None,
                ))
            }
            Opcode::CreateStream => {
                let request_id = body.get_u32();
                let payload = Self::read_payload(body);
                Ok((VariableHeader::CreateStream { request_id }, payload))
            }
            Opcode::CreateStreamResp => {
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u64());
                let extent_id = ExtentId(body.get_u32());
                let payload = Self::read_payload(body);
                Ok((
                    VariableHeader::CreateStreamResp {
                        request_id,
                        stream_id,
                        extent_id,
                    },
                    payload,
                ))
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
                let payload = Self::read_payload(body);
                Ok((VariableHeader::Heartbeat { request_id }, payload))
            }
            Opcode::RegisterExtent => {
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u64());
                let extent_id = ExtentId(body.get_u32());
                let role = body.get_u8();
                let replication_factor = body.get_u16();
                let payload = Self::read_payload(body);
                Ok((
                    VariableHeader::RegisterExtent {
                        request_id,
                        stream_id,
                        extent_id,
                        role,
                        replication_factor,
                    },
                    payload,
                ))
            }
            Opcode::ConnectAck => {
                let request_id = body.get_u32();
                Ok((VariableHeader::ConnectAck { request_id }, None))
            }
            Opcode::DisconnectAck => {
                let request_id = body.get_u32();
                Ok((VariableHeader::DisconnectAck { request_id }, None))
            }
            Opcode::RegisterExtentAck => {
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u64());
                let extent_id = ExtentId(body.get_u32());
                Ok((
                    VariableHeader::RegisterExtentAck {
                        request_id,
                        stream_id,
                        extent_id,
                    },
                    None,
                ))
            }
            Opcode::Watermark => {
                let stream_id = StreamId(body.get_u64());
                let offset = Offset(body.get_u64());
                Ok((VariableHeader::Watermark { stream_id, offset }, None))
            }
            Opcode::StreamManagerMembershipChange => {
                let payload = Self::read_payload(body);
                Ok((VariableHeader::StreamManagerMembershipChange, payload))
            }
            Opcode::DescribeStream => {
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u64());
                let count = body.get_u32();
                Ok((
                    VariableHeader::DescribeStream {
                        request_id,
                        stream_id,
                        count,
                    },
                    None,
                ))
            }
            Opcode::DescribeStreamResp => {
                let request_id = body.get_u32();
                let stream_id = StreamId(body.get_u64());
                let payload = Self::read_payload(body);
                Ok((
                    VariableHeader::DescribeStreamResp {
                        request_id,
                        stream_id,
                    },
                    payload,
                ))
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
                let payload = Self::read_payload(body);
                Ok((
                    VariableHeader::DescribeExtentResp {
                        request_id,
                        stream_id,
                    },
                    payload,
                ))
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
            Opcode::Error => {
                let request_id = body.get_u32();
                let error_code = body.get_u16();
                let extent_id = ExtentId(body.get_u32());
                let payload = Self::read_payload(body);
                Ok((
                    VariableHeader::Error {
                        request_id,
                        error_code,
                        extent_id,
                    },
                    payload,
                ))
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
            VariableHeader::AppendAck { .. } => Opcode::AppendAck,
            VariableHeader::Read { .. } => Opcode::Read,
            VariableHeader::ReadResp { .. } => Opcode::ReadResp,
            VariableHeader::Seal { .. } => Opcode::Seal,
            VariableHeader::SealAck { .. } => Opcode::SealAck,
            VariableHeader::CreateStream { .. } => Opcode::CreateStream,
            VariableHeader::CreateStreamResp { .. } => Opcode::CreateStreamResp,
            VariableHeader::QueryOffset { .. } => Opcode::QueryOffset,
            VariableHeader::QueryOffsetResp { .. } => Opcode::QueryOffsetResp,
            VariableHeader::Connect { .. } => Opcode::Connect,
            VariableHeader::ConnectAck { .. } => Opcode::ConnectAck,
            VariableHeader::Disconnect { .. } => Opcode::Disconnect,
            VariableHeader::DisconnectAck { .. } => Opcode::DisconnectAck,
            VariableHeader::Heartbeat { .. } => Opcode::Heartbeat,
            VariableHeader::RegisterExtent { .. } => Opcode::RegisterExtent,
            VariableHeader::RegisterExtentAck { .. } => Opcode::RegisterExtentAck,
            VariableHeader::Watermark { .. } => Opcode::Watermark,
            VariableHeader::StreamManagerMembershipChange => Opcode::StreamManagerMembershipChange,
            VariableHeader::DescribeStream { .. } => Opcode::DescribeStream,
            VariableHeader::DescribeStreamResp { .. } => Opcode::DescribeStreamResp,
            VariableHeader::DescribeExtent { .. } => Opcode::DescribeExtent,
            VariableHeader::DescribeExtentResp { .. } => Opcode::DescribeExtentResp,
            VariableHeader::Seek { .. } => Opcode::Seek,
            VariableHeader::SeekResp { .. } => Opcode::SeekResp,
            VariableHeader::Error { .. } => Opcode::Error,
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
                extent_id: ExtentId(5),
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
        assert_eq!(frame.extent_id(), decoded.extent_id());
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
        assert_eq!(
            decoded.payload,
            Some(Bytes::from_static(b"metrics-data"))
        );
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
                offset: Offset(100),
            },
            None,
        );

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);
        // 8 (fixed) + 8 (stream_id) + 8 (offset) = 24 bytes
        assert_eq!(buf.len(), 24);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.opcode(), Opcode::Watermark);
        assert_eq!(decoded.stream_id(), StreamId(42));
        assert_eq!(decoded.offset(), Offset(100));
        assert_eq!(decoded.request_id(), 0); // not present on wire
    }

    #[test]
    fn append_ack_round_trip() {
        let frame = Frame::new(
            VariableHeader::AppendAck {
                request_id: 10,
                stream_id: StreamId(1),
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
        assert_eq!(decoded.extent_id(), ExtentId(2));
    }

    #[test]
    fn read_with_count() {
        let frame = Frame::new(
            VariableHeader::Read {
                request_id: 5,
                stream_id: StreamId(10),
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
    fn error_response_frame() {
        let frame = Frame::error_response(42, ErrorCode::ExtentFull, "arena full", ExtentId(7));

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.opcode(), Opcode::Error);
        assert_eq!(decoded.request_id(), 42);
        assert_eq!(decoded.error_code(), ErrorCode::ExtentFull as u16);
        assert_eq!(decoded.extent_id(), ExtentId(7));
        assert_eq!(
            decoded.payload,
            Some(Bytes::from_static(b"arena full"))
        );
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
    fn create_stream_resp_round_trip() {
        let frame = Frame::new(
            VariableHeader::CreateStreamResp {
                request_id: 5,
                stream_id: StreamId(42),
                extent_id: ExtentId(1),
            },
            Some(Bytes::from_static(b"\x00\x0e127.0.0.1:9000")),
        );

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.opcode(), Opcode::CreateStreamResp);
        assert_eq!(decoded.request_id(), 5);
        assert_eq!(decoded.stream_id(), StreamId(42));
        assert_eq!(decoded.extent_id(), ExtentId(1));
        assert_eq!(
            decoded.payload,
            Some(Bytes::from_static(b"\x00\x0e127.0.0.1:9000"))
        );
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
                assert_eq!(
                    primary_addr.as_ref().unwrap(),
                    &Bytes::from_static(addr)
                );
            }
            _ => panic!("expected SealAck"),
        }
        assert!(buf.is_empty());
    }
}
