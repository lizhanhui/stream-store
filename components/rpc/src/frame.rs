use bytes::{Buf, BufMut, Bytes, BytesMut};
use common::errors::StorageError;
use common::types::{
    ErrorCode, ExtentId, FLAG_OFFSET_PRESENT, HEADER_LEN, MAGIC, Offset, Opcode, PROTOCOL_VERSION,
    StreamId,
};

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
    pub opcode: Opcode,
    pub flags: u8,
    pub request_id: u32,
    pub stream_id: StreamId,
    pub offset: Offset,
    pub extent_id: ExtentId,
    /// Byte position within the extent arena (used by APPEND_ACK, READ, READ_RESP).
    pub byte_pos: u64,
    /// Message count (used by READ request count, READ_RESP returned count, DESCRIBE_STREAM).
    pub count: u32,
    /// Error code (used by ERROR frames).
    pub error_code: u16,
    /// Application payload or complex variable-header data (for CONNECT, HEARTBEAT, etc.).
    pub payload: Bytes,
}

impl Default for Frame {
    fn default() -> Self {
        Frame {
            opcode: Opcode::Error,
            flags: 0,
            request_id: 0,
            stream_id: StreamId(0),
            offset: Offset(0),
            extent_id: ExtentId(0),
            byte_pos: 0,
            count: 0,
            error_code: 0,
            payload: Bytes::new(),
        }
    }
}

impl Frame {
    /// Compute the remaining length (variable header + payload) for this frame.
    fn remaining_length(&self) -> u32 {
        let vh = self.variable_header_len();
        let pl = if self.has_payload_section() {
            4 + self.payload.len() // u32 length prefix + payload bytes
        } else {
            0
        };
        (vh + pl) as u32
    }

    /// Variable header size in bytes for this frame's opcode+flags.
    fn variable_header_len(&self) -> usize {
        match self.opcode {
            // request_id(4) + stream_id(8) + extent_id(4)
            Opcode::Append => 4 + 8 + 4,
            // request_id(4) + stream_id(8) + extent_id(4) + offset(8) + byte_pos(8)
            Opcode::AppendAck => 4 + 8 + 4 + 8 + 8,
            // request_id(4) + stream_id(8) + offset(8) + byte_pos(8) + count(4)
            Opcode::Read => 4 + 8 + 8 + 8 + 4,
            // request_id(4) + stream_id(8) + offset(8) + count(4)
            Opcode::ReadResp => 4 + 8 + 8 + 4,
            // request_id(4) + stream_id(8) + extent_id(4) [+ offset(8) if FLAG_OFFSET_PRESENT]
            Opcode::Seal => {
                let base = 4 + 8 + 4;
                if self.flags & FLAG_OFFSET_PRESENT != 0 {
                    base + 8
                } else {
                    base
                }
            }
            // request_id(4) + stream_id(8) + extent_id(4) + offset(8)
            Opcode::SealAck => 4 + 8 + 4 + 8,
            // request_id(4)
            Opcode::CreateStream => 4,
            // request_id(4) + stream_id(8) + extent_id(4)
            Opcode::CreateStreamResp => 4 + 8 + 4,
            // request_id(4) + stream_id(8)
            Opcode::QueryOffset => 4 + 8,
            // request_id(4) + stream_id(8) + offset(8)
            Opcode::QueryOffsetResp => 4 + 8 + 8,
            // request_id(4)
            Opcode::Connect => 4,
            // request_id(4)
            Opcode::ConnectAck => 4,
            // request_id(4)
            Opcode::Disconnect => 4,
            // request_id(4)
            Opcode::DisconnectAck => 4,
            // request_id(4)
            Opcode::Heartbeat => 4,
            // request_id(4)
            Opcode::RegisterExtent => 4,
            // request_id(4) + stream_id(8) + extent_id(4)
            Opcode::RegisterExtentAck => 4 + 8 + 4,
            // stream_id(8) + offset(8) -- no request_id
            Opcode::Watermark => 8 + 8,
            // no variable header, just payload
            Opcode::StreamManagerMembershipChange => 0,
            // request_id(4) + stream_id(8) + count(4)
            Opcode::DescribeStream => 4 + 8 + 4,
            // request_id(4) + stream_id(8)
            Opcode::DescribeStreamResp => 4 + 8,
            // request_id(4) + stream_id(8) + extent_id(4)
            Opcode::DescribeExtent => 4 + 8 + 4,
            // request_id(4) + stream_id(8)
            Opcode::DescribeExtentResp => 4 + 8,
            // request_id(4) + stream_id(8) + offset(8)
            Opcode::Seek => 4 + 8 + 8,
            // request_id(4) + stream_id(8) + offset(8)
            Opcode::SeekResp => 4 + 8 + 8,
            // request_id(4) + error_code(2) + extent_id(4)
            Opcode::Error => 4 + 2 + 4,
        }
    }

    /// Whether this opcode has a payload section (u32 length prefix + bytes).
    fn has_payload_section(&self) -> bool {
        match self.opcode {
            Opcode::Append
            | Opcode::ReadResp
            | Opcode::CreateStream
            | Opcode::CreateStreamResp
            | Opcode::SealAck
            | Opcode::Connect
            | Opcode::Disconnect
            | Opcode::Heartbeat
            | Opcode::RegisterExtent
            | Opcode::StreamManagerMembershipChange
            | Opcode::DescribeStreamResp
            | Opcode::DescribeExtentResp
            | Opcode::SeekResp
            | Opcode::Error => true,
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
        dst.put_u8(self.opcode as u8);
        dst.put_u8(self.flags);
        dst.put_u32(remaining);

        // Variable header (opcode-specific).
        self.encode_variable_header(dst);

        // Payload section (u32 length prefix + bytes), if applicable.
        if self.has_payload_section() {
            dst.put_u32(self.payload.len() as u32);
            dst.extend_from_slice(&self.payload);
        }
    }

    fn encode_variable_header(&self, dst: &mut BytesMut) {
        match self.opcode {
            Opcode::Append => {
                dst.put_u32(self.request_id);
                dst.put_u64(self.stream_id.0);
                dst.put_u32(self.extent_id.0);
            }
            Opcode::AppendAck => {
                dst.put_u32(self.request_id);
                dst.put_u64(self.stream_id.0);
                dst.put_u32(self.extent_id.0);
                dst.put_u64(self.offset.0);
                dst.put_u64(self.byte_pos);
            }
            Opcode::Read => {
                dst.put_u32(self.request_id);
                dst.put_u64(self.stream_id.0);
                dst.put_u64(self.offset.0);
                dst.put_u64(self.byte_pos);
                dst.put_u32(self.count);
            }
            Opcode::ReadResp => {
                dst.put_u32(self.request_id);
                dst.put_u64(self.stream_id.0);
                dst.put_u64(self.offset.0);
                dst.put_u32(self.count);
            }
            Opcode::Seal => {
                dst.put_u32(self.request_id);
                dst.put_u64(self.stream_id.0);
                dst.put_u32(self.extent_id.0);
                if self.flags & FLAG_OFFSET_PRESENT != 0 {
                    dst.put_u64(self.offset.0);
                }
            }
            Opcode::SealAck => {
                dst.put_u32(self.request_id);
                dst.put_u64(self.stream_id.0);
                dst.put_u32(self.extent_id.0);
                dst.put_u64(self.offset.0);
            }
            Opcode::CreateStream => {
                dst.put_u32(self.request_id);
            }
            Opcode::CreateStreamResp => {
                dst.put_u32(self.request_id);
                dst.put_u64(self.stream_id.0);
                dst.put_u32(self.extent_id.0);
            }
            Opcode::QueryOffset => {
                dst.put_u32(self.request_id);
                dst.put_u64(self.stream_id.0);
            }
            Opcode::QueryOffsetResp => {
                dst.put_u32(self.request_id);
                dst.put_u64(self.stream_id.0);
                dst.put_u64(self.offset.0);
            }
            Opcode::Connect | Opcode::Disconnect | Opcode::Heartbeat | Opcode::RegisterExtent => {
                dst.put_u32(self.request_id);
            }
            Opcode::ConnectAck | Opcode::DisconnectAck => {
                dst.put_u32(self.request_id);
            }
            Opcode::RegisterExtentAck => {
                dst.put_u32(self.request_id);
                dst.put_u64(self.stream_id.0);
                dst.put_u32(self.extent_id.0);
            }
            Opcode::Watermark => {
                dst.put_u64(self.stream_id.0);
                dst.put_u64(self.offset.0);
            }
            Opcode::StreamManagerMembershipChange => {
                // no variable header fields, just payload
            }
            Opcode::DescribeStream => {
                dst.put_u32(self.request_id);
                dst.put_u64(self.stream_id.0);
                dst.put_u32(self.count);
            }
            Opcode::DescribeStreamResp | Opcode::DescribeExtentResp => {
                dst.put_u32(self.request_id);
                dst.put_u64(self.stream_id.0);
            }
            Opcode::DescribeExtent => {
                dst.put_u32(self.request_id);
                dst.put_u64(self.stream_id.0);
                dst.put_u32(self.extent_id.0);
            }
            Opcode::Seek => {
                dst.put_u32(self.request_id);
                dst.put_u64(self.stream_id.0);
                dst.put_u64(self.offset.0);
            }
            Opcode::SeekResp => {
                dst.put_u32(self.request_id);
                dst.put_u64(self.stream_id.0);
                dst.put_u64(self.offset.0);
            }
            Opcode::Error => {
                dst.put_u32(self.request_id);
                dst.put_u16(self.error_code);
                dst.put_u32(self.extent_id.0);
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

        // We have a complete frame — consume it.
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
        let mut body = src.split_to(remaining_len);

        let mut frame = Frame {
            opcode,
            flags,
            ..Default::default()
        };

        // Decode variable header + payload from body.
        frame.decode_body(&mut body)?;

        Ok(Some(frame))
    }

    fn decode_body(&mut self, body: &mut BytesMut) -> Result<(), StorageError> {
        match self.opcode {
            Opcode::Append => {
                self.request_id = body.get_u32();
                self.stream_id = StreamId(body.get_u64());
                self.extent_id = ExtentId(body.get_u32());
                self.read_payload(body);
            }
            Opcode::AppendAck => {
                self.request_id = body.get_u32();
                self.stream_id = StreamId(body.get_u64());
                self.extent_id = ExtentId(body.get_u32());
                self.offset = Offset(body.get_u64());
                self.byte_pos = body.get_u64();
            }
            Opcode::Read => {
                self.request_id = body.get_u32();
                self.stream_id = StreamId(body.get_u64());
                self.offset = Offset(body.get_u64());
                self.byte_pos = body.get_u64();
                self.count = body.get_u32();
            }
            Opcode::ReadResp => {
                self.request_id = body.get_u32();
                self.stream_id = StreamId(body.get_u64());
                self.offset = Offset(body.get_u64());
                self.count = body.get_u32();
                self.read_payload(body);
            }
            Opcode::Seal => {
                self.request_id = body.get_u32();
                self.stream_id = StreamId(body.get_u64());
                self.extent_id = ExtentId(body.get_u32());
                if self.flags & FLAG_OFFSET_PRESENT != 0 {
                    self.offset = Offset(body.get_u64());
                }
            }
            Opcode::SealAck => {
                self.request_id = body.get_u32();
                self.stream_id = StreamId(body.get_u64());
                self.extent_id = ExtentId(body.get_u32());
                self.offset = Offset(body.get_u64());
                self.read_payload(body);
            }
            Opcode::CreateStream => {
                self.request_id = body.get_u32();
                self.read_payload(body);
            }
            Opcode::CreateStreamResp => {
                self.request_id = body.get_u32();
                self.stream_id = StreamId(body.get_u64());
                self.extent_id = ExtentId(body.get_u32());
                self.read_payload(body);
            }
            Opcode::QueryOffset => {
                self.request_id = body.get_u32();
                self.stream_id = StreamId(body.get_u64());
            }
            Opcode::QueryOffsetResp => {
                self.request_id = body.get_u32();
                self.stream_id = StreamId(body.get_u64());
                self.offset = Offset(body.get_u64());
            }
            Opcode::Connect | Opcode::Disconnect | Opcode::Heartbeat | Opcode::RegisterExtent => {
                self.request_id = body.get_u32();
                self.read_payload(body);
            }
            Opcode::ConnectAck | Opcode::DisconnectAck => {
                self.request_id = body.get_u32();
            }
            Opcode::RegisterExtentAck => {
                self.request_id = body.get_u32();
                self.stream_id = StreamId(body.get_u64());
                self.extent_id = ExtentId(body.get_u32());
            }
            Opcode::Watermark => {
                self.stream_id = StreamId(body.get_u64());
                self.offset = Offset(body.get_u64());
            }
            Opcode::StreamManagerMembershipChange => {
                self.read_payload(body);
            }
            Opcode::DescribeStream => {
                self.request_id = body.get_u32();
                self.stream_id = StreamId(body.get_u64());
                self.count = body.get_u32();
            }
            Opcode::DescribeStreamResp | Opcode::DescribeExtentResp => {
                self.request_id = body.get_u32();
                self.stream_id = StreamId(body.get_u64());
                self.read_payload(body);
            }
            Opcode::DescribeExtent => {
                self.request_id = body.get_u32();
                self.stream_id = StreamId(body.get_u64());
                self.extent_id = ExtentId(body.get_u32());
            }
            Opcode::Seek => {
                self.request_id = body.get_u32();
                self.stream_id = StreamId(body.get_u64());
                self.offset = Offset(body.get_u64());
            }
            Opcode::SeekResp => {
                self.request_id = body.get_u32();
                self.stream_id = StreamId(body.get_u64());
                self.offset = Offset(body.get_u64());
                self.read_payload(body);
            }
            Opcode::Error => {
                self.request_id = body.get_u32();
                self.error_code = body.get_u16();
                self.extent_id = ExtentId(body.get_u32());
                self.read_payload(body);
            }
        }
        Ok(())
    }

    /// Read the payload section from the body: [payload_len:u32][payload:bytes].
    fn read_payload(&mut self, body: &mut BytesMut) {
        if body.remaining() >= 4 {
            let payload_len = body.get_u32() as usize;
            if body.remaining() >= payload_len {
                self.payload = body.split_to(payload_len).freeze();
            }
        }
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
        Frame {
            opcode: Opcode::Error,
            flags: 0,
            request_id,
            error_code: code as u16,
            extent_id,
            payload: Bytes::copy_from_slice(message.as_bytes()),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_append_frame() -> Frame {
        Frame {
            opcode: Opcode::Append,
            flags: 0x01,
            request_id: 42,
            stream_id: StreamId(100),
            extent_id: ExtentId(5),
            payload: Bytes::from_static(b"hello world"),
            ..Default::default()
        }
    }

    #[test]
    fn round_trip_encode_decode() {
        let frame = sample_append_frame();
        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame.opcode, decoded.opcode);
        assert_eq!(frame.flags, decoded.flags);
        assert_eq!(frame.request_id, decoded.request_id);
        assert_eq!(frame.stream_id, decoded.stream_id);
        assert_eq!(frame.extent_id, decoded.extent_id);
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
        let f2 = Frame {
            opcode: Opcode::QueryOffset,
            request_id: 99,
            stream_id: StreamId(200),
            ..Default::default()
        };

        let mut buf = BytesMut::new();
        f1.encode(&mut buf);
        f2.encode(&mut buf);

        let d1 = Frame::decode(&mut buf).unwrap().unwrap();
        let d2 = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(f1.opcode, d1.opcode);
        assert_eq!(f1.request_id, d1.request_id);
        assert_eq!(f1.payload, d1.payload);
        assert_eq!(f2.opcode, d2.opcode);
        assert_eq!(f2.request_id, d2.request_id);
        assert_eq!(f2.stream_id, d2.stream_id);
        assert!(buf.is_empty());
    }

    #[test]
    fn heartbeat_frame_with_payload() {
        let frame = Frame {
            opcode: Opcode::Heartbeat,
            request_id: 7,
            payload: Bytes::from_static(b"metrics-data"),
            ..Default::default()
        };

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.opcode, Opcode::Heartbeat);
        assert_eq!(decoded.request_id, 7);
        assert_eq!(decoded.payload, Bytes::from_static(b"metrics-data"));
    }

    #[test]
    fn connect_ack_minimal() {
        // ConnectAck: just fixed header + request_id
        let frame = Frame {
            opcode: Opcode::ConnectAck,
            request_id: 1,
            ..Default::default()
        };

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);
        // 8 (fixed) + 4 (request_id) = 12 bytes total
        assert_eq!(buf.len(), 12);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.opcode, Opcode::ConnectAck);
        assert_eq!(decoded.request_id, 1);
    }

    #[test]
    fn watermark_no_request_id() {
        let frame = Frame {
            opcode: Opcode::Watermark,
            stream_id: StreamId(42),
            offset: Offset(100),
            ..Default::default()
        };

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);
        // 8 (fixed) + 8 (stream_id) + 8 (offset) = 24 bytes
        assert_eq!(buf.len(), 24);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.opcode, Opcode::Watermark);
        assert_eq!(decoded.stream_id, StreamId(42));
        assert_eq!(decoded.offset, Offset(100));
        assert_eq!(decoded.request_id, 0); // not present on wire
    }

    #[test]
    fn append_ack_with_byte_pos() {
        let frame = Frame {
            opcode: Opcode::AppendAck,
            request_id: 10,
            stream_id: StreamId(1),
            extent_id: ExtentId(2),
            offset: Offset(42),
            byte_pos: 12345,
            ..Default::default()
        };

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.byte_pos, 12345);
        assert_eq!(decoded.offset, Offset(42));
        assert_eq!(decoded.stream_id, StreamId(1));
        assert_eq!(decoded.extent_id, ExtentId(2));
    }

    #[test]
    fn read_with_count_and_byte_pos() {
        let frame = Frame {
            opcode: Opcode::Read,
            request_id: 5,
            stream_id: StreamId(10),
            offset: Offset(50),
            byte_pos: 999,
            count: 20,
            ..Default::default()
        };

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.count, 20);
        assert_eq!(decoded.byte_pos, 999);
        assert_eq!(decoded.offset, Offset(50));
    }

    #[test]
    fn seal_without_offset() {
        let frame = Frame {
            opcode: Opcode::Seal,
            flags: 0, // no FLAG_OFFSET_PRESENT
            request_id: 1,
            stream_id: StreamId(10),
            extent_id: ExtentId(5),
            ..Default::default()
        };

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);
        // 8 + 4 + 8 + 4 = 24
        assert_eq!(buf.len(), 24);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.extent_id, ExtentId(5));
        assert_eq!(decoded.offset, Offset(0));
    }

    #[test]
    fn seal_with_offset() {
        let frame = Frame {
            opcode: Opcode::Seal,
            flags: FLAG_OFFSET_PRESENT,
            request_id: 1,
            stream_id: StreamId(10),
            extent_id: ExtentId(5),
            offset: Offset(42),
            ..Default::default()
        };

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);
        // 8 + 4 + 8 + 4 + 8 = 32
        assert_eq!(buf.len(), 32);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.flags, FLAG_OFFSET_PRESENT);
        assert_eq!(decoded.extent_id, ExtentId(5));
        assert_eq!(decoded.offset, Offset(42));
    }

    #[test]
    fn error_response_frame() {
        let frame = Frame::error_response(42, ErrorCode::ExtentFull, "arena full", ExtentId(7));

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.opcode, Opcode::Error);
        assert_eq!(decoded.request_id, 42);
        assert_eq!(decoded.error_code, ErrorCode::ExtentFull as u16);
        assert_eq!(decoded.extent_id, ExtentId(7));
        assert_eq!(decoded.payload, Bytes::from_static(b"arena full"));
    }

    #[test]
    fn read_resp_with_count() {
        let frame = Frame {
            opcode: Opcode::ReadResp,
            request_id: 3,
            stream_id: StreamId(1),
            offset: Offset(0),
            count: 5,
            payload: Bytes::from_static(b"messages"),
            ..Default::default()
        };

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.count, 5);
        assert_eq!(decoded.payload, Bytes::from_static(b"messages"));
    }

    #[test]
    fn describe_stream_with_count() {
        let frame = Frame {
            opcode: Opcode::DescribeStream,
            request_id: 1,
            stream_id: StreamId(10),
            count: 3,
            ..Default::default()
        };

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.count, 3);
        assert_eq!(decoded.stream_id, StreamId(10));
    }

    #[test]
    fn create_stream_resp_round_trip() {
        let frame = Frame {
            opcode: Opcode::CreateStreamResp,
            request_id: 5,
            stream_id: StreamId(42),
            extent_id: ExtentId(1),
            payload: Bytes::from_static(b"\x00\x0e127.0.0.1:9000"),
            ..Default::default()
        };

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.opcode, Opcode::CreateStreamResp);
        assert_eq!(decoded.request_id, 5);
        assert_eq!(decoded.stream_id, StreamId(42));
        assert_eq!(decoded.extent_id, ExtentId(1));
        assert_eq!(
            decoded.payload,
            Bytes::from_static(b"\x00\x0e127.0.0.1:9000")
        );
        assert!(buf.is_empty());
    }
}
