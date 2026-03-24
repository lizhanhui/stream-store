use bytes::BytesMut;
use common::errors::StorageError;
use tokio_util::codec::{Decoder, Encoder};

use crate::frame::Frame;

/// Tokio codec for encoding and decoding [`Frame`]s on a TCP stream.
#[derive(Debug, Default)]
pub struct FrameCodec;

impl Decoder for FrameCodec {
    type Item = Frame;
    type Error = StorageError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Frame>, StorageError> {
        Frame::decode(src)
    }
}

impl Encoder<Frame> for FrameCodec {
    type Error = StorageError;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), StorageError> {
        item.encode(dst);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use common::types::{ExtentId, Opcode, StreamId};

    #[test]
    fn codec_round_trip() {
        let mut codec = FrameCodec;
        let frame = Frame {
            opcode: Opcode::Append,
            flags: 0,
            request_id: 1,
            stream_id: StreamId(10),
            extent_id: ExtentId(5),
            payload: Bytes::from_static(b"test payload"),
            ..Default::default()
        };

        let mut buf = BytesMut::new();
        Encoder::encode(&mut codec, frame.clone(), &mut buf).unwrap();

        let decoded = Decoder::decode(&mut codec, &mut buf).unwrap().unwrap();
        assert_eq!(frame, decoded);
    }
}
