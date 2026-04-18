use std::sync::atomic::Ordering;

use bytes::{BufMut, Bytes, BytesMut};
use common::types::{ErrorCode, ExtentId};
use rpc::frame::{Frame, VariableHeader};
use tracing::info;

use super::ExtentNodeStore;

impl ExtentNodeStore {
    /// Handle REPORT_EXTENTS: SM queries this EN for all extents it holds for a stream.
    /// Used during crash recovery so SM can discover extents it doesn't know about.
    pub(crate) fn handle_report_extents(&self, frame: Frame) -> Frame {
        let (stream_id, epoch) = match &frame.variable_header {
            VariableHeader::ReportExtents {
                stream_id, epoch, ..
            } => (*stream_id, *epoch),
            _ => {
                return Frame::error_from_request(
                    &frame,
                    ErrorCode::InternalError,
                    "invalid ReportExtents frame",
                    ExtentId(0),
                );
            }
        };

        let guard = self.streams.pin();
        let stream = match guard.get(&stream_id) {
            Some(s) => s,
            None => {
                // Stream not found — return empty response.
                return Frame::new(
                    VariableHeader::ReportExtentsResp {
                        request_id: frame.request_id(),
                        stream_id,
                        epoch,
                    },
                    Some(Bytes::from(0u32.to_be_bytes().to_vec())), // num_extents = 0
                );
            }
        };

        let report = stream.report_extents(epoch);
        // Encode payload: [num_extents:u32] per extent: [extent_id:u32][start_offset:u64][end_offset:u64][state:u8]
        let mut buf = BytesMut::with_capacity(4 + report.len() * (4 + 8 + 8 + 1));
        buf.put_u32(report.len() as u32);
        for (eid, start, end, state) in &report {
            buf.put_u32(eid.0);
            buf.put_u64(start.0);
            buf.put_u64(*end);
            buf.put_u8(state.as_u8());
        }

        Frame::new(
            VariableHeader::ReportExtentsResp {
                request_id: frame.request_id(),
                stream_id,
                epoch: stream.epoch(),
            },
            Some(buf.freeze()),
        )
    }

    pub(crate) fn handle_seal(&self, frame: Frame) -> Frame {
        // Parse SealExtentNodeRequest fields.
        let (request_id, stream_id, epoch, extent_id_from, req_start_offset) =
            match &frame.variable_header {
                VariableHeader::SealExtentNodeRequest {
                    request_id,
                    stream_id,
                    epoch,
                    extent_id_from,
                    start_offset,
                } => (
                    *request_id,
                    *stream_id,
                    *epoch,
                    *extent_id_from,
                    *start_offset,
                ),
                _ => {
                    return Frame::seal_extent_node_resp_error(
                        frame.request_id(),
                        frame.stream_id(),
                        ErrorCode::InternalError,
                        "invalid SealExtentNodeRequest frame",
                    );
                }
            };

        let guard = self.streams.pin();
        let stream = match guard.get(&stream_id) {
            Some(s) => s,
            None => {
                // Stream not found. The SM is sealing a secondary that never received
                // any Forward frames — respond with start_offset to indicate zero
                // committed records for quorum.
                info!(
                    "seal for absent stream {} epoch {}: responding with start_offset={req_start_offset}",
                    stream_id, epoch
                );
                return Frame::new(
                    VariableHeader::SealExtentNodeResp {
                        request_id,
                        stream_id,
                        epoch,
                        extent_id: extent_id_from,
                        start_offset: req_start_offset,
                        end_offset: req_start_offset,
                    },
                    None,
                );
            }
        };

        // Wait for any active stream-level writer to finish before sealing.
        // With papaya, Stream is internally mutable via RwLock, but an
        // existing writer may have already started (in_flight > 0).
        {
            let mut spin_count = 0u32;
            loop {
                let inflight = stream.in_flight().load(Ordering::Acquire);
                if inflight == 0 {
                    break;
                }
                if spin_count < 6 {
                    for _ in 0..(1 << spin_count) {
                        std::hint::spin_loop();
                    }
                } else {
                    std::thread::yield_now();
                }
                spin_count += 1;
            }
        }

        // Find the LAST MUTABLE extent for (stream_id, epoch).
        // Only seal extents at the requested epoch — newer epochs are untouched.
        let active_id = match stream.active_extent_at_epoch(epoch) {
            Some(id) => id,
            None => {
                // No active extent — all extents already sealed.
                // Return idempotent response with the actual sealed extent's offsets.
                let (extent_id, start_offset, end_offset) = stream
                    .last_sealed_extent_at_epoch(epoch)
                    .unwrap_or((extent_id_from, req_start_offset, req_start_offset));
                let _ = stream;
                let payload =
                    self.build_seal_predecessor_payload(stream_id, extent_id_from, extent_id);
                return Frame::new(
                    VariableHeader::SealExtentNodeResp {
                        request_id,
                        stream_id,
                        epoch,
                        extent_id,
                        start_offset,
                        end_offset,
                    },
                    payload,
                );
            }
        };

        match stream.seal(active_id, None) {
            Some((start_offset, end_offset)) => {
                let sealed_extent_id = active_id;
                let _ = stream;
                info!(
                    "sealed extent {} for stream {}, start_offset={start_offset}, end_offset={end_offset}",
                    sealed_extent_id, stream_id
                );
                // Primary seals finalize CRC32 — send checksum to secondaries inline.
                self.send_forward_checksum(stream_id, sealed_extent_id);

                // Build payload with predecessor extents (extent_id >= extent_id_from AND < sealed).
                let payload = self.build_seal_predecessor_payload(
                    stream_id,
                    extent_id_from,
                    sealed_extent_id,
                );

                Frame::new(
                    VariableHeader::SealExtentNodeResp {
                        request_id,
                        stream_id,
                        epoch,
                        extent_id: sealed_extent_id,
                        start_offset,
                        end_offset,
                    },
                    payload,
                )
            }
            None => {
                // Already sealed — return the sealed extent's end_offset idempotently.
                let end_offset = stream.sealed_end_offset(active_id);
                let start_offset = stream
                    .with_extent(active_id, |e| e.start_offset.0)
                    .unwrap_or(req_start_offset);
                let _ = stream;
                info!(
                    "extent {} for stream {} already sealed, returning end_offset={end_offset} idempotently",
                    active_id, stream_id
                );

                let payload =
                    self.build_seal_predecessor_payload(stream_id, extent_id_from, active_id);

                Frame::new(
                    VariableHeader::SealExtentNodeResp {
                        request_id,
                        stream_id,
                        epoch,
                        extent_id: active_id,
                        start_offset,
                        end_offset,
                    },
                    payload,
                )
            }
        }
    }

    /// Build a payload containing predecessor extents for a seal response.
    ///
    /// Returns extent info for extents with `extent_id >= extent_id_from AND < sealed_extent_id`.
    /// Payload format: [num_extents:u32] then for each: [extent_id:u32][start_offset:u64][end_offset:u64]
    pub(crate) fn build_seal_predecessor_payload(
        &self,
        stream_id: common::types::StreamId,
        extent_id_from: ExtentId,
        sealed_extent_id: ExtentId,
    ) -> Option<Bytes> {
        let guard = self.streams.pin();
        let stream = match guard.get(&stream_id) {
            Some(s) => s,
            None => return None,
        };

        let mut predecessors: Vec<(ExtentId, u64, u64)> = Vec::new();
        // Iterate over all known extents to find predecessors.
        let mut eid = extent_id_from;
        while eid.0 < sealed_extent_id.0 {
            if let Some((start, end)) = stream.with_extent(eid, |ext| {
                let start = ext.start_offset.0;
                let end = start + ext.message_count();
                (start, end)
            }) {
                predecessors.push((eid, start, end));
            }
            eid = ExtentId(eid.0 + 1);
        }

        if predecessors.is_empty() {
            return None;
        }

        let mut buf = BytesMut::with_capacity(4 + predecessors.len() * (4 + 8 + 8));
        buf.put_u32(predecessors.len() as u32);
        for (eid, start, end) in &predecessors {
            buf.put_u32(eid.0);
            buf.put_u64(*start);
            buf.put_u64(*end);
        }
        Some(buf.freeze())
    }
}
