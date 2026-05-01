use bytes::{BufMut, BytesMut};
use common::types::{ErrorCode, ExtentId};
use rpc::frame::{Frame, VariableHeader};

use super::ExtentNodeStore;

impl ExtentNodeStore {
    pub(crate) fn handle_read(&self, frame: Frame) -> Frame {
        let stream_id = frame.stream_id();
        let guard = self.streams.pin();
        let stream = match guard.get(&stream_id) {
            Some(s) => s,
            None => {
                return Frame::error_from_request(
                    &frame,
                    ErrorCode::UnknownStream,
                    &format!("stream {} not found", stream_id),
                );
            }
        };

        let count = frame.count();
        // `extent_id` no longer travels on the wire; read by logical offset
        // within the stream. Since each epoch owns exactly one extent and the
        // offsets are disjoint, we look up the extent that contains the offset.
        // Fall back to the active extent for the active-epoch case.
        let target_offset = frame.offset();
        let extent_id = stream
            .find_extent_for_offset(target_offset)
            .or_else(|| stream.active_extent_id())
            .unwrap_or(ExtentId(0));

        match stream.read(extent_id, target_offset, count) {
            Ok(messages) => {
                let total_size: usize = messages.iter().map(|m| 4 + m.len()).sum();
                let mut payload = BytesMut::with_capacity(total_size);
                for msg in &messages {
                    payload.put_u32(msg.len() as u32);
                    payload.extend_from_slice(msg);
                }
                Frame::new(
                    VariableHeader::ReadResp {
                        request_id: frame.request_id(),
                        stream_id,
                        offset: frame.offset(),
                        count: messages.len() as u32,
                    },
                    Some(payload.freeze()),
                )
            }
            Err(e) => Frame::error_from_request(
                &frame,
                ErrorCode::InternalError,
                &e.to_string(),
            ),
        }
    }

    pub(crate) fn handle_query_offset(&self, frame: Frame) -> Frame {
        let stream_id = frame.stream_id();
        let guard = self.streams.pin();
        let stream = match guard.get(&stream_id) {
            Some(s) => s,
            None => {
                return Frame::error_from_request(
                    &frame,
                    ErrorCode::UnknownStream,
                    &format!("stream {} not found", stream_id),
                );
            }
        };

        Frame::new(
            VariableHeader::QueryOffsetResp {
                request_id: frame.request_id(),
                stream_id,
                offset: stream.max_offset(),
            },
            None,
        )
    }
}
