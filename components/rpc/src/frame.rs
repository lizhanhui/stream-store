use bytes::{Buf, BufMut, Bytes, BytesMut};
use common::errors::StorageError;
use common::types::{
    ErrorCode, ExtentId, Offset, Opcode, StreamId, HEADER_LEN, MAGIC, PROTOCOL_VERSION,
};

/// A wire protocol frame.
///
/// Layout (32 bytes fixed header + variable payload):
/// ```text
/// Magic(1) | Version(1) | Opcode(1) | Flags(1) | RequestId(4) |
/// StreamId(8) | Offset(8) | ExtentId(4) | PayloadLen(4) | Payload(variable)
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub opcode: Opcode,
    pub flags: u8,
    pub request_id: u32,
    pub stream_id: StreamId,
    pub offset: Offset,
    pub extent_id: ExtentId,
    pub payload: Bytes,
}

impl Frame {
    /// Encode this frame into the destination buffer.
    pub fn encode(&self, dst: &mut BytesMut) {
        dst.reserve(HEADER_LEN + self.payload.len());
        dst.put_u8(MAGIC);
        dst.put_u8(PROTOCOL_VERSION);
        dst.put_u8(self.opcode as u8);
        dst.put_u8(self.flags);
        dst.put_u32(self.request_id);
        dst.put_u64(self.stream_id.0);
        dst.put_u64(self.offset.0);
        dst.put_u32(self.extent_id.0);
        dst.put_u32(self.payload.len() as u32);
        dst.extend_from_slice(&self.payload);
    }

    /// Try to decode a frame from the source buffer.
    ///
    /// Returns `Ok(None)` if there is not enough data yet.
    pub fn decode(src: &mut BytesMut) -> Result<Option<Frame>, StorageError> {
        if src.len() < HEADER_LEN {
            return Ok(None);
        }

        // Peek at payload length without advancing, to check if we have the full frame.
        let payload_len =
            u32::from_be_bytes([src[28], src[29], src[30], src[31]]) as usize;
        let total_len = HEADER_LEN + payload_len;

        if src.len() < total_len {
            // Reserve capacity hint so tokio-util knows how much more to read.
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
        let opcode = Opcode::from_u8(opcode_byte)
            .ok_or(StorageError::UnknownOpcode(opcode_byte))?;

        let flags = src.get_u8();
        let request_id = src.get_u32();
        let stream_id = StreamId(src.get_u64());
        let offset = Offset(src.get_u64());
        let extent_id = ExtentId(src.get_u32());
        let _payload_len = src.get_u32(); // already read above

        let payload = src.split_to(payload_len).freeze();

        Ok(Some(Frame {
            opcode,
            flags,
            request_id,
            stream_id,
            offset,
            extent_id,
            payload,
        }))
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
        let mut payload = BytesMut::new();
        payload.put_u16(code as u16);
        payload.extend_from_slice(message.as_bytes());
        Frame {
            opcode: Opcode::Error,
            flags: 0,
            request_id,
            stream_id: StreamId(0),
            offset: Offset(0),
            extent_id,
            payload: payload.freeze(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_frame() -> Frame {
        Frame {
            opcode: Opcode::Append,
            flags: 0x01,
            request_id: 42,
            stream_id: StreamId(100),
            offset: Offset(0),
            extent_id: ExtentId(0),
            payload: Bytes::from_static(b"hello world"),
        }
    }

    #[test]
    fn round_trip_encode_decode() {
        let frame = sample_frame();
        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, decoded);
        assert!(buf.is_empty());
    }

    #[test]
    fn partial_frame_returns_none() {
        let frame = sample_frame();
        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        // Truncate to only header (no payload).
        buf.truncate(HEADER_LEN);
        let mut partial = buf.clone();
        let result = Frame::decode(&mut partial).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn insufficient_header_returns_none() {
        let mut buf = BytesMut::from(&[0u8; 10][..]);
        let result = Frame::decode(&mut buf).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn invalid_magic_returns_error() {
        let mut buf = BytesMut::new();
        // Write bad magic then pad to HEADER_LEN.
        buf.put_u8(0xDE);
        buf.put_u8(PROTOCOL_VERSION);
        buf.put_u8(Opcode::Append as u8);
        buf.put_u8(0);
        buf.put_u32(0);
        buf.put_u64(0);
        buf.put_u64(0);
        buf.put_u32(0);
        buf.put_u32(0);

        let result = Frame::decode(&mut buf);
        assert!(matches!(result, Err(StorageError::InvalidFrame(_))));
    }

    #[test]
    fn unknown_opcode_returns_error() {
        let mut buf = BytesMut::new();
        buf.put_u8(MAGIC);
        buf.put_u8(PROTOCOL_VERSION);
        buf.put_u8(0xFE); // invalid opcode (not assigned to any variant)
        buf.put_u8(0);
        buf.put_u32(0);
        buf.put_u64(0);
        buf.put_u64(0);
        buf.put_u32(0);
        buf.put_u32(0);

        let result = Frame::decode(&mut buf);
        assert!(matches!(result, Err(StorageError::UnknownOpcode(0xFE))));
    }

    #[test]
    fn multiple_frames_in_buffer() {
        let f1 = sample_frame();
        let f2 = Frame {
            opcode: Opcode::Read,
            flags: 0,
            request_id: 99,
            stream_id: StreamId(200),
            offset: Offset(5),
            extent_id: ExtentId(0),
            payload: Bytes::from_static(b"read"),
        };

        let mut buf = BytesMut::new();
        f1.encode(&mut buf);
        f2.encode(&mut buf);

        let d1 = Frame::decode(&mut buf).unwrap().unwrap();
        let d2 = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(f1, d1);
        assert_eq!(f2, d2);
        assert!(buf.is_empty());
    }

    #[test]
    fn empty_payload_frame() {
        let frame = Frame {
            opcode: Opcode::Heartbeat,
            flags: 0,
            request_id: 0,
            stream_id: StreamId(0),
            offset: Offset(0),
            extent_id: ExtentId(0),
            payload: Bytes::new(),
        };

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);
        assert_eq!(buf.len(), HEADER_LEN);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, decoded);
    }
}
