use std::sync::atomic::Ordering;

use bytes::{BufMut, Bytes, BytesMut};
use common::types::{ErrorCode, ExtentId};
use rpc::frame::{Frame, VariableHeader};
use tracing::{info, warn};

use super::ExtentNodeStore;
use crate::s3_flusher::FlushRequest;

impl ExtentNodeStore {
    /// Handle REPORT_EPOCH: SM queries this EN for the stream's current epoch state.
    /// Used during crash recovery so SM can discover epochs it doesn't know about.
    pub(crate) fn handle_report_extents(&self, frame: Frame) -> Frame {
        let (stream_id, epoch) = match &frame.variable_header {
            VariableHeader::ReportEpoch {
                stream_id, epoch, ..
            } => (*stream_id, *epoch),
            _ => {
                return Frame::error_from_request(
                    &frame,
                    ErrorCode::InternalError,
                    "invalid ReportEpoch frame",
                );
            }
        };

        let guard = self.streams.pin();
        let stream = match guard.get(&stream_id) {
            Some(s) => s,
            None => {
                // Stream not found — return empty response.
                return Frame::new(
                    VariableHeader::ReportEpochResp {
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
            VariableHeader::ReportEpochResp {
                request_id: frame.request_id(),
                stream_id,
                epoch: stream.epoch(),
            },
            Some(buf.freeze()),
        )
    }

    pub(crate) fn handle_seal(&self, frame: Frame) -> Frame {
        // Parse SealEpochPrepare fields.
        let (request_id, stream_id, epoch, req_start_offset) =
            match &frame.variable_header {
                VariableHeader::SealEpochPrepare {
                    request_id,
                    stream_id,
                    epoch,
                    start_offset,
                } => (
                    *request_id,
                    *stream_id,
                    *epoch,
                    *start_offset,
                ),
                _ => {
                    return Frame::seal_epoch_resp_error(
                        frame.request_id(),
                        frame.stream_id(),
                        ErrorCode::InternalError,
                        "invalid SealEpochPrepare frame",
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
                    VariableHeader::SealEpochResp {
                        request_id,
                        stream_id,
                        epoch,
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

        // `extent_id_from` no longer travels on the wire; the SealEpoch identity
        // is (stream_id, epoch). For now we derive a synthetic sentinel of 0 to
        // keep the existing local sealing logic unchanged — the plan's later
        // phases remove this threading entirely when the autonomous-create path
        // is deleted.
        let extent_id_from = ExtentId(0);

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
                    VariableHeader::SealEpochResp {
                        request_id,
                        stream_id,
                        epoch,
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

                // Queue sealed extent for S3 flush (Primary only).
                // The Primary uploads to S3 and broadcasts ForwardFlushed to
                // secondaries on completion, enabling eviction across all replicas.
                if let Some(ref tx) = self.flush_tx {
                    let is_primary = self
                        .replicas
                        .pin()
                        .get(&stream_id)
                        .map(|ri| ri.is_primary())
                        .unwrap_or(false);
                    if is_primary {
                        // Deduplicate: mark flush-in-progress before enqueuing.
                        let started = self
                            .streams
                            .pin()
                            .get(&stream_id)
                            .map(|s| s.start_flush(sealed_extent_id))
                            .unwrap_or(false);
                        if started
                            && tx
                                .try_send(FlushRequest {
                                    stream_id,
                                    extent_id: sealed_extent_id,
                                    start_offset,
                                    end_offset,
                                })
                                .is_err()
                            && let Some(s) = self.streams.pin().get(&stream_id)
                        {
                            s.finish_flush(sealed_extent_id);
                        }
                    }
                }

                // Build payload with predecessor extents (extent_id >= extent_id_from AND < sealed).
                let payload = self.build_seal_predecessor_payload(
                    stream_id,
                    extent_id_from,
                    sealed_extent_id,
                );

                Frame::new(
                    VariableHeader::SealEpochResp {
                        request_id,
                        stream_id,
                        epoch,
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
                    VariableHeader::SealEpochResp {
                        request_id,
                        stream_id,
                        epoch,
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
        let stream = guard.get(&stream_id)?;

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

    /// Handle FLUSH_EPOCH (0x1B): SM commands this EN to upload a sealed epoch
    /// to S3 on behalf of a dead Primary (disaster recovery).
    ///
    /// Returns FlushEpochResp on success (accepted), FlushEpochRespError on skip/error.
    pub(crate) fn handle_flush_extent(&self, frame: Frame) -> Frame {
        let (request_id, stream_id, epoch, start_offset, end_offset) =
            match &frame.variable_header {
                VariableHeader::FlushEpoch {
                    request_id,
                    stream_id,
                    epoch,
                    start_offset,
                    end_offset,
                } => (
                    *request_id,
                    *stream_id,
                    *epoch,
                    *start_offset,
                    *end_offset,
                ),
                _ => {
                    return Frame::error_from_request(
                        &frame,
                        ErrorCode::InternalError,
                        "invalid FlushEpoch frame",
                    );
                }
            };
        let _ = epoch;

        // `extent_id` no longer travels on the wire for FlushEpoch. The EN's
        // local bookkeeping still identifies extents by id. Look up the extent
        // whose offset range matches the flush request; fall back to the
        // active extent if no range match.
        let extent_id = {
            let guard = self.streams.pin();
            guard
                .get(&stream_id)
                .and_then(|s| {
                    s.find_extent_for_offset(common::types::Offset(start_offset))
                        .or_else(|| s.active_extent_id())
                })
                .unwrap_or(ExtentId(0))
        };

        // Guard: S3 must be configured.
        let flush_tx = match self.flush_tx {
            Some(ref tx) => tx,
            None => {
                warn!(
                    "FlushExtent: no S3 configured, ignoring stream={} extent={}",
                    stream_id, extent_id,
                );
                return Frame::flush_epoch_resp_error(
                    request_id,
                    stream_id,
                    ErrorCode::InternalError,
                    "no S3 configured",
                );
            }
        };

        // Guard: extent must exist, be sealed, and not already flushed.
        // Phase 2 (SealEpochCommit) should have already sealed and
        // committed the offset. Warn if the extent is Active (unexpected)
        // or if local offset differs from SM's authoritative offset.
        let ready = self
            .streams
            .pin()
            .get(&stream_id)
            .and_then(|s| {
                s.with_extent(extent_id, |ext| {
                    if ext.is_flushed() {
                        return false;
                    }
                    if !ext.is_sealed() {
                        warn!(
                            "FlushExtent: extent is not sealed (unexpected), sealing now: stream={} extent={} end_offset={}",
                            stream_id, extent_id, end_offset,
                        );
                        ext.seal(Some(end_offset));
                    } else {
                        // Defense-in-depth: verify local seal matches SM's offset.
                        let local_end = ext.start_offset.0 + ext.message_count();
                        if local_end != end_offset {
                            warn!(
                                "FlushExtent: local seal offset {} differs from SM offset {}, correcting: stream={} extent={}",
                                local_end, end_offset, stream_id, extent_id,
                            );
                            ext.correct_seal_offset(end_offset);
                        }
                    }
                    true
                })
            })
            .unwrap_or(false);

        if !ready {
            info!(
                "FlushExtent: skipping stream={} extent={} (not found or already flushed)",
                stream_id, extent_id,
            );
            return Frame::new(
                VariableHeader::FlushEpochResp {
                    request_id,
                    stream_id,
                },
                None,
            );
        }

        // Deduplicate: skip if already in progress (covers both Primary and DR paths).
        {
            let started = self
                .streams
                .pin()
                .get(&stream_id)
                .map(|s| s.start_flush(extent_id))
                .unwrap_or(false);
            if !started {
                info!(
                    "FlushExtent: already in progress for stream={} extent={}, skipping",
                    stream_id, extent_id,
                );
                return Frame::new(
                    VariableHeader::FlushEpochResp {
                        request_id,
                        stream_id,
                    },
                    None,
                );
            }
        }

        // Enqueue onto the existing flusher. If the channel is full, clear
        // the flush marker so SM can retry on the next scan.
        if flush_tx
            .try_send(FlushRequest {
                stream_id,
                extent_id,
                start_offset,
                end_offset,
            })
            .is_err()
        {
            warn!(
                "FlushExtent: flush channel full for stream={} extent={}",
                stream_id, extent_id,
            );
            if let Some(s) = self.streams.pin().get(&stream_id) {
                s.finish_flush(extent_id);
            }
            return Frame::flush_epoch_resp_error(
                request_id,
                stream_id,
                ErrorCode::InternalError,
                "flush channel full",
            );
        }

        info!(
            "FlushExtent: queued DR flush for stream={} extent={}",
            stream_id, extent_id,
        );
        Frame::new(
            VariableHeader::FlushEpochResp {
                request_id,
                stream_id,
            },
            None,
        )
    }

    /// Handle SealEpoch phase 2: commit local seal point to SM's
    /// authoritative committed offset. Returns SealEpochCommitResp.
    pub(crate) fn handle_seal_commit(&self, frame: Frame) -> Frame {
        let (request_id, stream_id, _epoch, _start_offset, end_offset) =
            match &frame.variable_header {
                VariableHeader::SealEpochCommit {
                    request_id,
                    stream_id,
                    epoch,
                    start_offset,
                    end_offset,
                } => (
                    *request_id,
                    *stream_id,
                    *epoch,
                    *start_offset,
                    *end_offset,
                ),
                _ => {
                    return Frame::error_from_request(
                        &frame,
                        ErrorCode::InternalError,
                        "invalid SealEpochCommit frame",
                    );
                }
            };

        // `extent_id` no longer travels on the wire for SealEpochCommit.
        // Look up the extent that matches the committed offset range; fall
        // back to the active extent.
        let extent_id = {
            let guard = self.streams.pin();
            guard
                .get(&stream_id)
                .and_then(|s| {
                    s.find_extent_for_offset(common::types::Offset(_start_offset))
                        .or_else(|| s.active_extent_id())
                })
                .unwrap_or(ExtentId(0))
        };

        let guard = self.streams.pin();
        let stream = match guard.get(&stream_id) {
            Some(s) => s,
            None => {
                return Frame::new(
                    VariableHeader::SealEpochCommitResp {
                        request_id,
                        stream_id,
                    },
                    None,
                );
            }
        };

        stream.with_extent(extent_id, |ext| {
            if !ext.is_sealed() {
                // Not yet sealed — seal with SM's authoritative offset.
                ext.seal(Some(end_offset));
                info!(
                    "SealCommit: sealed extent {} for stream {} at end_offset={}",
                    extent_id, stream_id, end_offset,
                );
            } else {
                ext.correct_seal_offset(end_offset);
            }
        });

        Frame::new(
            VariableHeader::SealEpochCommitResp {
                request_id,
                stream_id,
            },
            None,
        )
    }
}
