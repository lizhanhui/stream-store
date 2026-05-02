use std::sync::atomic::Ordering;

use bytes::{BufMut, Bytes, BytesMut};
use common::types::ErrorCode;
use rpc::frame::{Frame, VariableHeader};
use tracing::{info, warn};

use super::ExtentNodeStore;
use crate::s3_flusher::FlushRequest;

impl ExtentNodeStore {
    /// Handle REPORT_EPOCH: SM queries this EN for the stream's current epoch state.
    pub(crate) fn handle_report_epoch(&self, frame: Frame) -> Frame {
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
                return Frame::new(
                    VariableHeader::ReportEpochResp {
                        request_id: frame.request_id(),
                        stream_id,
                        epoch,
                    },
                    Some(Bytes::from(0u32.to_be_bytes().to_vec())),
                );
            }
        };

        let report = stream.report_epoch(epoch);
        let mut buf = BytesMut::with_capacity(4 + report.iter().len() * (8 + 8 + 1));
        buf.put_u32(report.iter().len() as u32);
        if let Some((_reported_epoch, start, end, state)) = report {
            buf.put_u64(start.0);
            buf.put_u64(end.0);
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
        let (request_id, stream_id, epoch, req_start_offset) = match &frame.variable_header {
            VariableHeader::SealEpochPrepare {
                request_id,
                stream_id,
                epoch,
                start_offset,
            } => (*request_id, *stream_id, *epoch, *start_offset),
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

        match stream.seal(epoch, None) {
            Some((start_offset, end_offset)) => {
                info!(
                    "sealed epoch {} for stream {}, start_offset={start_offset}, end_offset={end_offset}",
                    epoch, stream_id
                );
                drop(guard);
                self.send_forward_checksum(stream_id, epoch);

                if let Some(ref tx) = self.flush_tx {
                    let is_primary = self
                        .replicas
                        .pin()
                        .get(&stream_id)
                        .map(|ri| ri.is_primary())
                        .unwrap_or(false);
                    if is_primary {
                        let started = self
                            .streams
                            .pin()
                            .get(&stream_id)
                            .map(|s| s.start_flush(epoch))
                            .unwrap_or(false);
                        if started
                            && tx
                                .try_send(FlushRequest {
                                    stream_id,
                                    epoch,
                                    start_offset,
                                    end_offset,
                                })
                                .is_err()
                            && let Some(s) = self.streams.pin().get(&stream_id)
                        {
                            s.finish_flush(epoch);
                        }
                    }
                }

                Frame::new(
                    VariableHeader::SealEpochResp {
                        request_id,
                        stream_id,
                        epoch,
                        start_offset,
                        end_offset,
                    },
                    None,
                )
            }
            None => {
                let end_offset = stream.sealed_end_offset(epoch);
                let start_offset = stream
                    .with_epoch(epoch, |e| e.start_offset.0)
                    .unwrap_or(req_start_offset);
                info!(
                    "epoch {} for stream {} already sealed, returning end_offset={end_offset} idempotently",
                    epoch, stream_id
                );
                Frame::new(
                    VariableHeader::SealEpochResp {
                        request_id,
                        stream_id,
                        epoch,
                        start_offset,
                        end_offset,
                    },
                    None,
                )
            }
        }
    }

    /// Handle FLUSH_EPOCH (0x1B): SM commands this EN to upload a sealed epoch to S3.
    pub(crate) fn handle_flush_extent(&self, frame: Frame) -> Frame {
        let (request_id, stream_id, epoch, _start_offset, end_offset) = match &frame.variable_header {
            VariableHeader::FlushEpoch {
                request_id,
                stream_id,
                epoch,
                start_offset,
                end_offset,
            } => (*request_id, *stream_id, *epoch, *start_offset, *end_offset),
            _ => {
                return Frame::error_from_request(
                    &frame,
                    ErrorCode::InternalError,
                    "invalid FlushEpoch frame",
                );
            }
        };

        let flush_tx = match self.flush_tx {
            Some(ref tx) => tx,
            None => {
                warn!("FlushEpoch: no S3 configured, ignoring stream={} epoch={}", stream_id, epoch);
                return Frame::flush_epoch_resp_error(
                    request_id,
                    stream_id,
                    ErrorCode::InternalError,
                    "no S3 configured",
                );
            }
        };

        let ready = self
            .streams
            .pin()
            .get(&stream_id)
            .and_then(|s| {
                s.with_epoch(epoch, |ext| {
                    if ext.is_flushed() {
                        return false;
                    }
                    if !ext.is_sealed() {
                        warn!(
                            "FlushEpoch: epoch is not sealed (unexpected), sealing now: stream={} epoch={} end_offset={}",
                            stream_id, epoch, end_offset,
                        );
                        ext.seal(Some(end_offset));
                    } else {
                        let local_end = ext.start_offset.0 + ext.message_count();
                        if local_end != end_offset {
                            warn!(
                                "FlushEpoch: local seal offset {} differs from SM offset {}, correcting: stream={} epoch={}",
                                local_end, end_offset, stream_id, epoch,
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
                "FlushEpoch: skipping stream={} epoch={} (not found or already flushed)",
                stream_id, epoch,
            );
            return Frame::new(
                VariableHeader::FlushEpochResp {
                    request_id,
                    stream_id,
                },
                None,
            );
        }

        let started = self
            .streams
            .pin()
            .get(&stream_id)
            .map(|s| s.start_flush(epoch))
            .unwrap_or(false);
        if !started {
            info!("FlushEpoch: already in progress for stream={} epoch={}, skipping", stream_id, epoch);
            return Frame::new(
                VariableHeader::FlushEpochResp {
                    request_id,
                    stream_id,
                },
                None,
            );
        }

        if flush_tx
            .try_send(FlushRequest {
                stream_id,
                epoch,
                start_offset: _start_offset,
                end_offset,
            })
            .is_err()
        {
            warn!("FlushEpoch: flush channel full for stream={} epoch={}", stream_id, epoch);
            if let Some(s) = self.streams.pin().get(&stream_id) {
                s.finish_flush(epoch);
            }
            return Frame::flush_epoch_resp_error(
                request_id,
                stream_id,
                ErrorCode::InternalError,
                "flush channel full",
            );
        }

        info!("FlushEpoch: queued DR flush for stream={} epoch={}", stream_id, epoch);
        Frame::new(
            VariableHeader::FlushEpochResp {
                request_id,
                stream_id,
            },
            None,
        )
    }

    /// Handle SealEpoch phase 2: commit local seal point to SM's authoritative committed offset.
    pub(crate) fn handle_seal_commit(&self, frame: Frame) -> Frame {
        let (request_id, stream_id, epoch, _start_offset, end_offset) =
            match &frame.variable_header {
                VariableHeader::SealEpochCommit {
                    request_id,
                    stream_id,
                    epoch,
                    start_offset,
                    end_offset,
                } => (*request_id, *stream_id, *epoch, *start_offset, *end_offset),
                _ => {
                    return Frame::error_from_request(
                        &frame,
                        ErrorCode::InternalError,
                        "invalid SealEpochCommit frame",
                    );
                }
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

        stream.with_epoch(epoch, |ext| {
            if !ext.is_sealed() {
                ext.seal(Some(end_offset));
                info!(
                    "SealCommit: sealed epoch {} for stream {} at end_offset={}",
                    epoch, stream_id, end_offset,
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
