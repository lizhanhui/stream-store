use std::sync::atomic::Ordering;
use std::time::Instant;

use bytes::Bytes;
use common::errors::StorageError;
use common::types::{Epoch, ErrorCode, Offset, StreamId};
use rpc::frame::{Frame, VariableHeader};
use smallvec::SmallVec;
use tokio::sync::mpsc::Sender;
use tracing::debug;

use super::{AppendJob, ExtentNodeStore};
use crate::ack_queue::PendingAck;
use crate::arena::WriteBatchJob;
use crate::stream::Stream;

impl ExtentNodeStore {
    /// Handle Append — pipelined group commit with stream-level leader election.
    ///
    /// Uses per-stream `in_flight` counter for leader election:
    /// - `prev == 0`: This thread becomes the active writer (fast path).
    ///   Appends its own payload, then drains any follower jobs from the channel.
    /// - `prev > 0`: An active writer exists. Push an `AppendJob` to the channel
    ///   and return `None` immediately (deferred ACK).
    ///
    /// The active writer handles replication (Forward + PendingAck for RF≥2)
    /// or sends immediate AppendAck (RF=1/standalone) for each job.
    ///
    /// Arena-full is handled inside `StreamEpoch::write_batch` via internal
    /// arena rotation; the store layer never sees it.
    ///
    /// Pin guards are scoped in blocks so they're dropped before `.await` points
    /// (papaya pin guards are non-Send).
    pub(crate) async fn handle_append(
        &self,
        frame: Frame,
        response_tx: Option<&Sender<Frame>>,
    ) -> Option<Frame> {
        let stream_id = frame.stream_id();
        let client_epoch = frame.epoch();

        // ── Validation + leader election + own append (scoped pin guard) ──
        let (own_result, remaining) = {
            let guard = self.streams.pin();
            let stream = match guard.get(&stream_id) {
                Some(s) => s,
                None => {
                    return Some(Frame::error_from_request(
                        &frame,
                        ErrorCode::UnknownStream,
                        &format!("stream {} not found", stream_id),
                    ));
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
            let own_result = self.do_append_and_respond(
                stream,
                request_id,
                stream_id,
                epoch,
                payload,
                response_tx.cloned(),
            );
            let remaining = stream.in_flight().fetch_sub(1, Ordering::Release);
            (own_result, remaining)
        };
        // Pin guard dropped — safe to .await.

        // ── Normal path: drain followers if any arrived ──
        if remaining > 1 {
            self.drain_follower_jobs(stream_id).await;
        }

        own_result
    }

    /// Perform a single append via the stream's active extent and handle replication / ACK.
    ///
    /// Returns the response frame or `None` if deferred via `response_tx`.
    ///
    /// Forward frames are pushed **inline** into the stream's cached per-secondary
    /// mpsc channels, while the leader still holds `in_flight > 0`. This guarantees
    /// FIFO ordering — no subsequent leader can push frames before us.
    ///
    /// When response_tx is Some, success ACKs are sent via the channel and the
    /// Frame return is None. When response_tx is None, success ACKs are returned
    /// as Some(Frame).
    pub(crate) fn do_append_and_respond(
        &self,
        stream: &Stream,
        request_id: u32,
        stream_id: StreamId,
        epoch: Epoch,
        payload: Bytes,
        response_tx: Option<Sender<Frame>>,
    ) -> Option<Frame> {
        let payload_len = payload.len();
        let payload_for_forward = payload.clone();

        // Route through the arena pool via a 1-job WriteBatch. Arena-full
        // is handled transparently inside StreamEpoch::write_batch; any
        // error that reaches us is a genuine per-job failure.
        let hint_offset = stream.max_offset();
        let job = WriteBatchJob::new(hint_offset, payload);
        let mut results = stream.write_batch_active(std::slice::from_ref(&job));
        let append_result = match results.pop().expect("one result per job") {
            Ok(r) => r,
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
                    return None;
                }
                return Some(err);
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
                    return None;
                }
                return Some(err);
            }
        };

        let offset = append_result.offset;
        let _extent_start_offset = stream.with_epoch(epoch, |e| e.start_offset.0).unwrap_or(0);

        // Update metrics counters.
        self.metrics.append_count.fetch_add(1, Ordering::Relaxed);
        self.metrics.bytes_written
            .fetch_add(payload_len as u64, Ordering::Relaxed);

        // Check replica info for this stream (Arc clone — one atomic, no deep copy).
        let replica = stream.replica_info();

        match replica {
            None => {
                // Standalone mode: immediate ACK.
                let ack = Frame::new(
                    VariableHeader::AppendAck {
                        request_id,
                        stream_id,
                        epoch,
                        offset,
                    },
                    None,
                );
                if let Some(tx) = response_tx {
                    let _ = tx.try_send(ack);
                    None
                } else {
                    Some(ack)
                }
            }
            Some(ref ri) if ri.is_primary() => {
                if ri.is_standalone() {
                    // RF=1: no secondaries, ACK immediately.
                    let ack = Frame::new(
                        VariableHeader::AppendAck {
                            request_id,
                            stream_id,
                            epoch,
                            offset,
                        },
                        None,
                    );
                    if let Some(tx) = response_tx {
                        let _ = tx.try_send(ack);
                        None
                    } else {
                        Some(ack)
                    }
                } else {
                    // RF≥2: push Forward frames inline into per-stream channels.
                    if stream.has_secondaries() {
                        let forward_frame = Frame::new(
                            VariableHeader::Forward {
                                stream_id,
                                epoch,
                                offset,
                            },
                            Some(payload_for_forward),
                        );
                        // Inject ForwardInitEpoch if this is the first forward for the extent.
                        if let Some(init) = self.maybe_build_init_forward(stream, &forward_frame) {
                            stream.send_forward(init);
                        }
                        stream.send_forward(forward_frame);
                    }

                    // Queue deferred ACK — lock-free, no contention with watermark readers.
                    if let Some(ref resp_tx) = response_tx {
                        let aq = stream
                            .init_ack_queue(ri.required_secondary_acks(), stream.replication_timeout());
                        aq.enqueue(PendingAck {
                            request_id,
                            stream_id,
                            response_tx: resp_tx.clone(),
                            assigned_offset: offset.0,
                            epoch,
                            created_at: Instant::now(),
                        });
                    }

                    None
                }
            }
            Some(_) => {
                // Secondary received normal Append (not Forward) — shouldn't normally happen.
                let ack = Frame::new(
                    VariableHeader::AppendAck {
                        request_id,
                        stream_id,
                        epoch,
                        offset,
                    },
                    None,
                );
                if let Some(tx) = response_tx {
                    let _ = tx.try_send(ack);
                    None
                } else {
                    Some(ack)
                }
            }
        }
    }

    /// Drain follower append jobs from the stream's channel and process them.
    ///
    /// Called by the active writer after its own append when `in_flight > 1`.
    /// Loops until all followers have been processed. Forward frames are
    /// pushed inline by `do_append_and_respond`.
    ///
    /// Arena-full is handled below `StreamEpoch` (internal rotation); this
    /// drain path only deals with per-job logical outcomes (seal, err, ok).
    async fn drain_follower_jobs(&self, stream_id: StreamId) {
        loop {
            // ── Phase 1: Drain jobs from the channel ──
            let mut batch: Vec<AppendJob> = Vec::new();
            let mut epoch = Epoch(0);
            loop {
                // Scope pin guard — must be dropped before yield_now().await.
                let need_yield = {
                    let guard = self.streams.pin();
                    let stream = match guard.get(&stream_id) {
                        Some(s) => s,
                        None => return,
                    };
                    if batch.is_empty() {
                        epoch = stream.epoch();
                    }
                    match stream.job_rx().try_recv() {
                        Ok(job) => {
                            batch.push(job);
                            while let Ok(job) = stream.job_rx().try_recv() {
                                batch.push(job);
                            }
                            false
                        }
                        Err(_) if !batch.is_empty() => false,
                        Err(_) => {
                            // Follower incremented in_flight but hasn't pushed yet.
                            let delegated = stream.in_flight().load(Ordering::Acquire);
                            delegated > 0
                        }
                    }
                };
                if need_yield {
                    tokio::task::yield_now().await;
                } else {
                    break;
                }
            }

            // ── Phase 2: Process the batch ──
            let batch_len = batch.len();
            let done = {
                let guard = self.streams.pin();
                let stream = match guard.get(&stream_id) {
                    Some(s) => s,
                    None => break,
                };
                for job in &batch {
                    self.do_append_and_respond(
                        stream,
                        job.request_id,
                        job.stream_id,
                        epoch,
                        job.payload.clone(),
                        job.response_tx.clone(),
                    );
                }
                let remaining = stream
                    .in_flight()
                    .fetch_sub(batch_len as u64, Ordering::Release);
                remaining <= batch_len as u64
            };
            if done {
                break;
            }
            // More followers arrived during processing — loop again.
        }
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
        if let Some(init) = self.maybe_build_init_forward(stream, &frame) {
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
            payload_for_forward: Bytes,
            offset: Offset,
            payload_len: usize,
        }
        let mut responses = Vec::new();
        let mut entries: Vec<BatchEntry> = Vec::with_capacity(frames.len());

        // ── Validation + leader election + batch appends (scoped pin guard) ──
        let (_epoch, batch_len) = {
            let guard = self.streams.pin();
            let stream = match guard.get(&stream_id) {
                Some(s) => s,
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
                self.metrics.append_count
                    .fetch_add(entries.len() as u64, Ordering::Relaxed);
                self.metrics.bytes_written.fetch_add(total_bytes, Ordering::Relaxed);

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
                                    if let Some(init) =
                                        self.maybe_build_init_forward(stream, &forward_frame)
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
                                let now = Instant::now();
                                for entry in &entries {
                                    aq.enqueue(PendingAck {
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

            (epoch, batch_len)
        };
        // Pin guard dropped — safe to .await.

        // ── Normal path: decrement in_flight and drain followers if any ──
        let remaining = {
            let guard = self.streams.pin();
            if let Some(stream) = guard.get(&stream_id) {
                stream.in_flight().fetch_sub(batch_len, Ordering::Release)
            } else {
                0
            }
        };

        if remaining > batch_len {
            self.drain_follower_jobs(stream_id).await;
        }

        responses
    }
}
