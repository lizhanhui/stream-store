use std::sync::Arc;
use std::sync::atomic::Ordering;

use common::errors::StorageError;
use common::types::{Epoch, ErrorCode, Offset, StreamId};
use rpc::frame::{Frame, VariableHeader};
use smallvec::SmallVec;
use tokio::sync::mpsc::Sender;
use tracing::debug;

use super::{AppendJob, ExtentNodeStore};
use crate::arena::WriteBatchJob;
use crate::stream::Stream;

impl ExtentNodeStore {
    /// Handle Append — pipelined group commit with stream-level leader election.
    ///
    /// The Store is a thin router: one papaya lookup to extract
    /// `Arc<Stream>`, then the per-stream leader election / append /
    /// drain happens on the Stream itself (no further map lookups).
    ///
    /// - `prev == 0`: this thread becomes the active writer.
    ///   Calls `stream.append_one(...)`, then `stream.drain_follower_jobs()`
    ///   if followers arrived.
    /// - `prev > 0`: pushes an `AppendJob` to the channel and returns.
    ///
    /// Arena-full is handled inside `StreamEpoch::write_batch` via
    /// internal arena rotation; the store layer never sees it.
    pub(crate) async fn handle_append(
        &self,
        frame: Frame,
        response_tx: Option<&Sender<Frame>>,
    ) -> Option<Frame> {
        let stream_id = frame.stream_id();
        let client_epoch = frame.epoch();

        // Resolve the Stream once, clone the Arc, then drop the pin guard
        // before we touch any .await point.
        let stream: Arc<Stream> = {
            let guard = self.streams.pin();
            match guard.get(&stream_id) {
                Some(s) => Arc::clone(s),
                None => {
                    return Some(Frame::error_from_request(
                        &frame,
                        ErrorCode::UnknownStream,
                        &format!("stream {} not found", stream_id),
                    ));
                }
            }
        };

        let epoch = stream.epoch();
        if client_epoch != Epoch(0) && client_epoch != epoch {
            return Some(Frame::error_from_request(
                &frame,
                ErrorCode::EpochStale,
                &format!("epoch stale: client={}, current={}", client_epoch, epoch),
            ));
        }

        let prev = stream.in_flight().fetch_add(1, Ordering::Acquire);

        if prev > 0 {
            let job = AppendJob {
                request_id: frame.request_id(),
                stream_id,
                payload: frame.payload.clone().unwrap_or_default(),
                response_tx: response_tx.cloned(),
            };
            let _ = stream.job_tx().send(job);
            return None;
        }

        // FAST PATH: I'm the active writer (prev == 0).
        let payload = frame.payload.clone().unwrap_or_default();
        let request_id = frame.request_id();
        let own_result = stream.append_one(request_id, epoch, payload, response_tx.cloned());
        let remaining = stream.in_flight().fetch_sub(1, Ordering::Release);

        if remaining > 1 {
            stream.drain_follower_jobs().await;
        }

        own_result
    }

    /// Send a ForwardChecksum for a sealed extent inline via per-stream channels.
    ///
    /// Fire-and-forget: the secondary defers verification via `try_verify_checksum()`.
    pub(crate) fn send_forward_checksum(&self, stream_id: StreamId, sealed_epoch: Epoch) {
        let guard = self.streams.pin();
        let stream = match guard.get(&stream_id) {
            Some(s) => s,
            None => return,
        };
        let (checksum, committed_bytes) = match stream.with_epoch(sealed_epoch, |ext| {
            (
                ext.finalized_crc32().unwrap_or(0),
                ext.committed_data().len() as u64,
            )
        }) {
            Some(pair) => pair,
            None => return,
        };
        debug!(
            "ForwardChecksum sent: stream={}, epoch={}, crc32={:#x}, bytes={}",
            stream_id, sealed_epoch, checksum, committed_bytes,
        );
        let frame = Frame::new(
            VariableHeader::ForwardChecksum {
                stream_id,
                epoch: stream.epoch(),
                checksum,
                committed_bytes,
            },
            None,
        );
        if let Some(init) = stream.maybe_build_init_forward(&frame) {
            stream.send_forward(init);
        }
        stream.send_forward(frame);
    }

    /// Optimized batch append: all frames share the same stream_id/epoch.
    ///
    /// Amortizes map lookups (3N → 3), leader elections (N → 1),
    /// ReplicaInfo access (N clones → 0, borrow within guard), and
    /// atomic operations (2N → 2).
    ///
    /// Pin guards are scoped in blocks so they're dropped before `.await` points
    /// (papaya pin guards are non-Send).
    pub(crate) async fn handle_append_batch_inner(
        &self,
        frames: &[Frame],
        response_tx: Option<&Sender<Frame>>,
    ) -> Vec<Frame> {
        let stream_id = frames[0].stream_id();

        struct BatchEntry {
            request_id: u32,
            payload_for_forward: bytes::Bytes,
            offset: Offset,
            payload_len: usize,
        }
        let mut responses = Vec::new();
        let mut entries: Vec<BatchEntry> = Vec::with_capacity(frames.len());

        // Resolve the Stream once; drop the pin guard before any await.
        let stream: Arc<Stream> = {
            let guard = self.streams.pin();
            match guard.get(&stream_id) {
                Some(s) => Arc::clone(s),
                None => {
                    for frame in frames {
                        responses.push(Frame::error_from_request(
                            frame,
                            ErrorCode::UnknownStream,
                            &format!("stream {} not found", stream_id),
                        ));
                    }
                    return responses;
                }
            }
        };

        let epoch = stream.epoch();
        let client_epoch = frames[0].epoch();
        if client_epoch != Epoch(0) && client_epoch != epoch {
            for frame in frames {
                responses.push(Frame::error_from_request(
                    frame,
                    ErrorCode::EpochStale,
                    &format!("epoch stale: client={}, current={}", client_epoch, epoch),
                ));
            }
            return responses;
        }

        let batch_len = frames.len() as u64;
        let prev = stream.in_flight().fetch_add(batch_len, Ordering::Acquire);

        if prev > 0 {
            // SLOW PATH: active writer exists. Push all as AppendJobs.
            for frame in frames {
                let job = AppendJob {
                    request_id: frame.request_id(),
                    stream_id,
                    payload: frame.payload.clone().unwrap_or_default(),
                    response_tx: response_tx.cloned(),
                };
                let _ = stream.job_tx().send(job);
            }
            return responses; // All deferred — empty responses.
        }

        // FAST PATH: I'm the active writer (prev == 0).
        //
        // Build one WriteBatch covering every frame, then call the
        // arena pool once. Per-job errors fan back out into the
        // normal ACK path below.
        let mut jobs: SmallVec<[WriteBatchJob; 16]> = SmallVec::with_capacity(frames.len());
        let hint = stream.max_offset();
        for (i, frame) in frames.iter().enumerate() {
            let payload = frame.payload.clone().unwrap_or_default();
            jobs.push(WriteBatchJob::new(Offset(hint.0 + i as u64), payload));
        }
        let results = stream.write_batch_active(&jobs);

        for (i, res) in results.into_iter().enumerate() {
            let request_id = frames[i].request_id();
            let payload = frames[i].payload.clone().unwrap_or_default();
            let payload_len = payload.len();
            let payload_for_forward = payload;
            match res {
                Ok(result) => {
                    entries.push(BatchEntry {
                        request_id,
                        payload_for_forward,
                        offset: result.offset,
                        payload_len,
                    });
                }
                Err(StorageError::EpochSealed { .. }) => {
                    let err = Frame::append_ack_error(
                        request_id,
                        stream_id,
                        epoch,
                        ErrorCode::ExtentSealed,
                        "extent is sealed",
                    );
                    if let Some(tx) = response_tx {
                        let _ = tx.try_send(err);
                    } else {
                        responses.push(err);
                    }
                }
                Err(e) => {
                    let err = Frame::append_ack_error(
                        request_id,
                        stream_id,
                        epoch,
                        ErrorCode::InternalError,
                        &e.to_string(),
                    );
                    if let Some(tx) = response_tx {
                        let _ = tx.try_send(err);
                    } else {
                        responses.push(err);
                    }
                }
            }
        }

        // Process successful entries: metrics, replica info, forwards, ACKs.
        if !entries.is_empty() {
            let total_bytes: u64 = entries.iter().map(|e| e.payload_len as u64).sum();
            stream
                .metrics_handle()
                .append_count
                .fetch_add(entries.len() as u64, Ordering::Relaxed);
            stream
                .metrics_handle()
                .bytes_written
                .fetch_add(total_bytes, Ordering::Relaxed);

            let replica = stream.replica_info();

            match replica.as_ref() {
                None => {
                    for entry in &entries {
                        let ack = Frame::new(
                            VariableHeader::AppendAck {
                                request_id: entry.request_id,
                                stream_id,
                                epoch,
                                offset: entry.offset,
                            },
                            None,
                        );
                        if let Some(tx) = response_tx {
                            let _ = tx.try_send(ack);
                        } else {
                            responses.push(ack);
                        }
                    }
                }
                Some(ri) if ri.is_primary() => {
                    if ri.is_standalone() {
                        for entry in &entries {
                            let ack = Frame::new(
                                VariableHeader::AppendAck {
                                    request_id: entry.request_id,
                                    stream_id,
                                    epoch,
                                    offset: entry.offset,
                                },
                                None,
                            );
                            if let Some(tx) = response_tx {
                                let _ = tx.try_send(ack);
                            } else {
                                responses.push(ack);
                            }
                        }
                    } else {
                        if stream.has_secondaries() {
                            for entry in &entries {
                                let forward_frame = Frame::new(
                                    VariableHeader::Forward {
                                        stream_id,
                                        epoch,
                                        offset: entry.offset,
                                    },
                                    Some(entry.payload_for_forward.clone()),
                                );
                                if let Some(init) = stream.maybe_build_init_forward(&forward_frame)
                                {
                                    stream.send_forward(init);
                                }
                                stream.send_forward(forward_frame);
                            }
                        }
                        if let Some(resp_tx) = response_tx {
                            let aq = stream.init_ack_queue(
                                ri.required_secondary_acks(),
                                stream.replication_timeout(),
                            );
                            let now = std::time::Instant::now();
                            for entry in &entries {
                                aq.enqueue(crate::ack_queue::PendingAck {
                                    request_id: entry.request_id,
                                    stream_id,
                                    response_tx: resp_tx.clone(),
                                    assigned_offset: entry.offset.0,
                                    epoch,
                                    created_at: now,
                                });
                            }
                        }
                    }
                }
                Some(_) => {
                    for entry in &entries {
                        let ack = Frame::new(
                            VariableHeader::AppendAck {
                                request_id: entry.request_id,
                                stream_id,
                                epoch,
                                offset: entry.offset,
                            },
                            None,
                        );
                        if let Some(tx) = response_tx {
                            let _ = tx.try_send(ack);
                        } else {
                            responses.push(ack);
                        }
                    }
                }
            }
        }

        let remaining = stream.in_flight().fetch_sub(batch_len, Ordering::Release);

        if remaining > batch_len {
            stream.drain_follower_jobs().await;
        }

        responses
    }
}
