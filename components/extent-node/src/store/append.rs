use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::config::DEFAULT_IDLE_SHRINK_THRESHOLD_SECS;
use common::errors::StorageError;
use common::types::{Epoch, ErrorCode, ExtentId, FLAG_SYSTEM_TICK, Offset, StreamId};
use rpc::frame::{Frame, VariableHeader};
use smallvec::SmallVec;
use tokio::sync::mpsc::Sender;
use tracing::{debug, info};

use super::{AppendJob, ExtentNodeStore, ExtentUpdate};
use crate::ack_queue::{AckQueue, PendingAck};
use crate::stream::{SealNotification, SealReason, Stream};

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
    /// On ExtentFull, the leader seals the active extent, creates a new one,
    /// and retries — all transparently within the same leader tenure.
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
        let (
            epoch,
            own_result,
            extent_full,
            remaining,
            is_tick,
            should_shrink,
            payload,
            request_id,
        ) = {
            let guard = self.streams.pin();
            let stream = match guard.get(&stream_id) {
                Some(s) => s,
                None => {
                    return Some(Frame::error_from_request(
                        &frame,
                        ErrorCode::UnknownStream,
                        &format!("stream {} not found", stream_id),
                        ExtentId(0),
                    ));
                }
            };

            let epoch = stream.epoch();
            if client_epoch != Epoch(0) && client_epoch != epoch {
                return Some(Frame::error_from_request(
                    &frame,
                    ErrorCode::EpochStale,
                    &format!("epoch stale: client={}, current={}", client_epoch, epoch),
                    ExtentId(0),
                ));
            }

            let is_tick = frame.flags() & FLAG_SYSTEM_TICK != 0;
            let prev = stream.in_flight().fetch_add(1, Ordering::Acquire);

            if prev > 0 {
                if is_tick {
                    stream.in_flight().fetch_sub(1, Ordering::Release);
                    return None;
                }
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

            if is_tick {
                let should_shrink = stream
                    .should_idle_shrink(Duration::from_secs(DEFAULT_IDLE_SHRINK_THRESHOLD_SECS));
                let remaining = stream.in_flight().fetch_sub(1, Ordering::Release);
                (
                    epoch,
                    None,
                    false,
                    remaining,
                    true,
                    should_shrink,
                    Bytes::new(),
                    0,
                )
            } else {
                let payload = frame.payload.clone().unwrap_or_default();
                let request_id = frame.request_id();
                let (own_result, extent_full) = self.do_append_and_respond(
                    stream,
                    request_id,
                    stream_id,
                    epoch,
                    payload.clone(),
                    response_tx.cloned(),
                );
                if extent_full {
                    // Don't decrement in_flight — we're still the leader.
                    // Will decrement after seal+create+retry below.
                    (
                        epoch, own_result, true, 0, false, false, payload, request_id,
                    )
                } else {
                    let remaining = stream.in_flight().fetch_sub(1, Ordering::Release);
                    (
                        epoch, own_result, false, remaining, false, false, payload, request_id,
                    )
                }
            }
        };
        // Pin guard dropped — safe to .await.

        // ── System tick path ──
        if is_tick {
            if remaining > 1 {
                let batch_seals = self.drain_follower_jobs(stream_id).await;
                for notification in &batch_seals {
                    self.send_extent_update(stream_id, notification);
                    self.send_forward_checksum(stream_id, notification.sealed_extent_id);
                    self.send_flush_request(stream_id, notification);
                }
            }
            if should_shrink {
                if let Some(ref notification) =
                    self.seal_and_create(stream_id, SealReason::IdleShrink)
                {
                    self.send_extent_update(stream_id, notification);
                    self.send_forward_checksum(stream_id, notification.sealed_extent_id);
                    self.send_flush_request(stream_id, notification);
                    info!(
                        "idle-shrink: stream={}, sealed={}, new={}, capacity={}",
                        stream_id,
                        notification.sealed_extent_id,
                        notification.new_extent_id,
                        notification.new_extent_capacity,
                    );
                }
            }
            return None;
        }

        // ── Extent-full path: seal+create, retry, then drain ──
        if extent_full {
            let seal_notification = self.seal_and_create(stream_id, SealReason::ExtentFull);

            // Re-acquire pin guard for retry on the new extent.
            let (retry_result, remaining) = {
                let guard = self.streams.pin();
                match guard.get(&stream_id) {
                    Some(stream) => {
                        let (retry_result, _) = self.do_append_and_respond(
                            stream,
                            request_id,
                            stream_id,
                            epoch,
                            payload,
                            response_tx.cloned(),
                        );
                        let remaining = stream.in_flight().fetch_sub(1, Ordering::Release);
                        (retry_result, remaining)
                    }
                    None => return None,
                }
            };

            if remaining > 1 {
                let batch_seals = self.drain_follower_jobs(stream_id).await;
                for notification in &batch_seals {
                    self.send_extent_update(stream_id, notification);
                    self.send_forward_checksum(stream_id, notification.sealed_extent_id);
                    self.send_flush_request(stream_id, notification);
                }
            }
            if let Some(ref notification) = seal_notification {
                self.send_extent_update(stream_id, notification);
                self.send_forward_checksum(stream_id, notification.sealed_extent_id);
                self.send_flush_request(stream_id, notification);
            }
            return retry_result;
        }

        // ── Normal path: drain followers if any arrived ──
        if remaining > 1 {
            let batch_seals = self.drain_follower_jobs(stream_id).await;
            for notification in &batch_seals {
                self.send_extent_update(stream_id, notification);
                self.send_flush_request(stream_id, notification);
            }
        }

        own_result
    }

    /// Perform a single append via the stream's active extent and handle replication / ACK.
    ///
    /// Returns `(Option<Frame>, bool)`:
    /// - Option<Frame>: response frame (None if deferred or sent via channel)
    /// - bool: whether ExtentFull occurred (caller should trigger proactive seal)
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
    ) -> (Option<Frame>, bool) {
        let payload_len = payload.len();
        let payload_for_forward = payload.clone();

        // Write locally via single-writer append on the active extent.
        let (append_result, extent_id) = match stream.try_append_active(payload) {
            Ok(r) => r,
            Err(StorageError::ExtentSealed(extent_id)) => {
                let err = Frame::append_ack_error(
                    request_id,
                    stream_id,
                    epoch,
                    extent_id,
                    ErrorCode::ExtentSealed,
                    "extent is sealed",
                );
                if let Some(ref tx) = response_tx {
                    let _ = tx.try_send(err);
                    return (None, false);
                }
                return (Some(err), false);
            }
            Err(StorageError::ExtentFull(_)) => {
                // Don't send error to client — the caller will seal, create a new extent,
                // and retry the append transparently. Return extent_full=true.
                return (None, true);
            }
            Err(e) => {
                let err = Frame::append_ack_error(
                    request_id,
                    stream_id,
                    epoch,
                    ExtentId(0),
                    ErrorCode::InternalError,
                    &e.to_string(),
                );
                if let Some(ref tx) = response_tx {
                    let _ = tx.try_send(err);
                    return (None, false);
                }
                return (Some(err), false);
            }
        };

        let offset = append_result.offset;
        let _extent_start_offset = stream
            .with_extent(extent_id, |e| e.start_offset.0)
            .unwrap_or(0);

        // Update metrics counters.
        self.append_count.fetch_add(1, Ordering::Relaxed);
        self.bytes_written
            .fetch_add(payload_len as u64, Ordering::Relaxed);

        // Check replica info for this stream (Arc clone — one atomic, no deep copy).
        let replica = self.replicas.pin().get(&stream_id).map(Arc::clone);

        match replica {
            None => {
                // Standalone mode: immediate ACK.
                let ack = Frame::new(
                    VariableHeader::AppendAck {
                        request_id,
                        stream_id,
                        epoch,
                        extent_id,
                        offset,
                    },
                    None,
                );
                if let Some(ref tx) = response_tx {
                    let _ = tx.try_send(ack);
                    (None, false)
                } else {
                    (Some(ack), false)
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
                            extent_id,
                            offset,
                        },
                        None,
                    );
                    if let Some(ref tx) = response_tx {
                        let _ = tx.try_send(ack);
                        (None, false)
                    } else {
                        (Some(ack), false)
                    }
                } else {
                    // RF≥2: push Forward frames inline into per-stream channels.
                    if stream.has_secondaries() {
                        let forward_frame = Frame::new(
                            VariableHeader::Forward {
                                stream_id,
                                extent_id,
                                epoch,
                                offset,
                                byte_pos: append_result.byte_pos,
                            },
                            Some(payload_for_forward),
                        );
                        // Inject ForwardInitExtent if this is the first forward for the extent.
                        if let Some(init) = self.maybe_build_init_forward(stream, &forward_frame) {
                            stream.send_forward(init);
                        }
                        stream.send_forward(forward_frame);
                    }

                    // Queue deferred ACK — lock-free, no contention with watermark readers.
                    if let Some(ref resp_tx) = response_tx {
                        let aq_guard = self.ack_queues.pin();
                        let aq = aq_guard.get_or_insert_with(stream_id, || {
                            AckQueue::with_timeout(
                                ri.required_secondary_acks(),
                                self.replication_timeout,
                            )
                        });
                        aq.enqueue(PendingAck {
                            request_id,
                            stream_id,
                            response_tx: resp_tx.clone(),
                            assigned_offset: offset.0,
                            extent_id,
                            epoch,
                            created_at: Instant::now(),
                        });
                    }

                    (None, false)
                }
            }
            Some(_) => {
                // Secondary received normal Append (not Forward) — shouldn't normally happen.
                let ack = Frame::new(
                    VariableHeader::AppendAck {
                        request_id,
                        stream_id,
                        epoch,
                        extent_id,
                        offset,
                    },
                    None,
                );
                if let Some(ref tx) = response_tx {
                    let _ = tx.try_send(ack);
                    (None, false)
                } else {
                    (Some(ack), false)
                }
            }
        }
    }

    /// Drain follower append jobs from the stream's channel and process them.
    ///
    /// Called by the active writer after its own append when `in_flight > 1`.
    /// Loops until all followers have been processed.
    ///
    /// Forward frames are pushed inline by `do_append_and_respond` — this method
    /// only returns seal notifications for the caller to send SM updates.
    ///
    /// On ExtentFull, this method calls `seal_and_create` (which manages its own
    /// pin guard) and retries the remaining jobs on the new extent.
    /// Pin guards are scoped in blocks so they're dropped before `yield_now().await`.
    async fn drain_follower_jobs(&self, stream_id: StreamId) -> SmallVec<[SealNotification; 1]> {
        let mut notifications = SmallVec::with_capacity(1);

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
                        None => return notifications,
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
            let mut extent_full_idx: Option<usize> = None;

            // Process each job (scoped pin guard).
            {
                let guard = self.streams.pin();
                let stream = match guard.get(&stream_id) {
                    Some(s) => s,
                    None => break,
                };
                for (i, job) in batch.iter().enumerate() {
                    let (_, extent_full) = self.do_append_and_respond(
                        stream,
                        job.request_id,
                        job.stream_id,
                        epoch,
                        job.payload.clone(),
                        job.response_tx.clone(),
                    );
                    if extent_full {
                        extent_full_idx = Some(i);
                        break;
                    }
                }
            }
            // Pin guard dropped.

            if let Some(index) = extent_full_idx {
                let seal_notification = self.seal_and_create(stream_id, SealReason::ExtentFull);
                if let Some(ref notification) = seal_notification {
                    notifications.push(notification.clone());
                }

                // Retry the failed job and remaining jobs on the new extent.
                let done = {
                    let guard = self.streams.pin();
                    if let Some(stream) = guard.get(&stream_id) {
                        epoch = stream.epoch();
                        for job in &batch[index..] {
                            let (_, _) = self.do_append_and_respond(
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
                    } else {
                        true
                    }
                };
                if done {
                    break;
                }
            } else {
                // All jobs processed without ExtentFull.
                let done = {
                    let guard = self.streams.pin();
                    if let Some(stream) = guard.get(&stream_id) {
                        let remaining = stream
                            .in_flight()
                            .fetch_sub(batch_len as u64, Ordering::Release);
                        remaining <= batch_len as u64
                    } else {
                        true
                    }
                };
                if done {
                    break;
                }
            }
            // More followers arrived during processing — loop again.
        }

        notifications
    }

    /// Seal the active extent and create a new one.
    ///
    /// Acquires write lock on the stream's inner RwLock. Returns the seal notification
    /// if a seal+create occurred, or None if already sealed / stream not found.
    pub(crate) fn seal_and_create(
        &self,
        stream_id: StreamId,
        reason: SealReason,
    ) -> Option<SealNotification> {
        if let Some(stream) = self.streams.pin().get(&stream_id) {
            // For IdleShrink, re-check eligibility under write guard.
            if matches!(reason, SealReason::IdleShrink)
                && !stream
                    .should_idle_shrink(Duration::from_secs(DEFAULT_IDLE_SHRINK_THRESHOLD_SECS))
            {
                return None;
            }
            let t0 = std::time::Instant::now();
            let notification = stream.seal_and_create_next(reason);
            let seal_us = t0.elapsed().as_micros();
            if let Some(ref n) = notification {
                info!(
                    "seal_and_create: stream={}, sealed={}, new={}, capacity={}, reason={:?}, duration={}us",
                    stream_id,
                    n.sealed_extent_id,
                    n.new_extent_id,
                    n.new_extent_capacity,
                    reason,
                    seal_us,
                );
            }
            notification
        } else {
            None
        }
    }

    /// Send an async UPDATE_EXTENT (Sealed) to SM (fire-and-forget).
    pub(crate) fn send_extent_update(&self, stream_id: StreamId, notification: &SealNotification) {
        if let Some(ref tx) = self.update_tx {
            let _ = tx.try_send(ExtentUpdate::Sealed {
                stream_id,
                sealed_extent_id: notification.sealed_extent_id,
                end_offset: notification.end_offset,
                new_extent_id: notification.new_extent_id,
                new_extent_capacity: notification.new_extent_capacity,
                epoch: notification.epoch,
            });
        }
    }

    /// Queue a sealed extent for S3 flush (Primary only).
    ///
    /// The Primary uploads to S3 and broadcasts `ForwardFlushed` to secondaries
    /// on completion, enabling eviction across all replicas without extra infra.
    pub(crate) fn send_flush_request(&self, stream_id: StreamId, notification: &SealNotification) {
        let tx = match self.flush_tx {
            Some(ref tx) => tx,
            None => return,
        };
        let is_primary = self
            .replicas
            .pin()
            .get(&stream_id)
            .map(|ri| ri.is_primary())
            .unwrap_or(false);
        if !is_primary {
            return;
        }
        // Memory-only streams don't flush to S3.
        let is_memory = self
            .streams
            .pin()
            .get(&stream_id)
            .map(|s| s.storage_medium() == 1)
            .unwrap_or(false);
        if is_memory {
            return;
        }
        let start_offset = self
            .streams
            .pin()
            .get(&stream_id)
            .and_then(|s| s.with_extent(notification.sealed_extent_id, |e| e.start_offset.0))
            .unwrap_or(0);
        let _ = tx.try_send(crate::s3_flusher::FlushRequest {
            stream_id,
            extent_id: notification.sealed_extent_id,
            start_offset,
            end_offset: notification.end_offset,
        });
    }

    /// Send a ForwardChecksum for a sealed extent inline via per-stream channels.
    ///
    /// Fire-and-forget: the secondary defers verification via `try_verify_checksum()`.
    pub(crate) fn send_forward_checksum(&self, stream_id: StreamId, sealed_extent_id: ExtentId) {
        let guard = self.streams.pin();
        let stream = match guard.get(&stream_id) {
            Some(s) => s,
            None => return,
        };
        let (checksum, committed_bytes) = match stream.with_extent(sealed_extent_id, |ext| {
            (
                ext.finalized_crc32().unwrap_or(0),
                ext.committed_data().len() as u64,
            )
        }) {
            Some(pair) => pair,
            None => return,
        };
        debug!(
            "ForwardChecksum sent: stream={}, extent={}, crc32={:#x}, bytes={}",
            stream_id, sealed_extent_id, checksum, committed_bytes,
        );
        let frame = Frame::new(
            VariableHeader::ForwardChecksum {
                stream_id,
                extent_id: sealed_extent_id,
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
            byte_pos: u64,
            payload_len: usize,
            extent_id: ExtentId,
        }
        struct FailedFrame {
            request_id: u32,
            payload: Bytes,
        }

        let mut responses = Vec::new();
        let mut entries: Vec<BatchEntry> = Vec::with_capacity(frames.len());
        let mut failed_frames: Vec<FailedFrame> = Vec::new();
        let mut extent_full = false;

        // ── Validation + leader election + batch appends (scoped pin guard) ──
        let (epoch, batch_len) = {
            let guard = self.streams.pin();
            let stream = match guard.get(&stream_id) {
                Some(s) => s,
                None => {
                    for frame in frames {
                        responses.push(Frame::error_from_request(
                            frame,
                            ErrorCode::UnknownStream,
                            &format!("stream {} not found", stream_id),
                            ExtentId(0),
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
                        ExtentId(0),
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
            for frame in frames {
                let request_id = frame.request_id();
                let payload = frame.payload.clone().unwrap_or_default();
                let payload_len = payload.len();
                let payload_for_forward = payload.clone();

                if extent_full {
                    failed_frames.push(FailedFrame {
                        request_id,
                        payload,
                    });
                    continue;
                }

                match stream.try_append_active(payload.clone()) {
                    Ok((result, eid)) => {
                        entries.push(BatchEntry {
                            request_id,
                            payload_for_forward,
                            offset: result.offset,
                            byte_pos: result.byte_pos,
                            payload_len,
                            extent_id: eid,
                        });
                    }
                    Err(StorageError::ExtentSealed(extent_id)) => {
                        let err = Frame::append_ack_error(
                            request_id,
                            stream_id,
                            epoch,
                            extent_id,
                            ErrorCode::ExtentSealed,
                            "extent is sealed",
                        );
                        if let Some(tx) = response_tx {
                            let _ = tx.try_send(err);
                        } else {
                            responses.push(err);
                        }
                    }
                    Err(StorageError::ExtentFull(_)) => {
                        extent_full = true;
                        failed_frames.push(FailedFrame {
                            request_id,
                            payload,
                        });
                    }
                    Err(e) => {
                        let err = Frame::append_ack_error(
                            request_id,
                            stream_id,
                            epoch,
                            ExtentId(0),
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
                let _extent_start_offset = stream
                    .with_extent(entries[0].extent_id, |e| e.start_offset.0)
                    .unwrap_or(0);

                let total_bytes: u64 = entries.iter().map(|e| e.payload_len as u64).sum();
                self.append_count
                    .fetch_add(entries.len() as u64, Ordering::Relaxed);
                self.bytes_written.fetch_add(total_bytes, Ordering::Relaxed);

                let replica = self.replicas.pin().get(&stream_id).map(Arc::clone);

                match replica.as_ref() {
                    None => {
                        for entry in &entries {
                            let ack = Frame::new(
                                VariableHeader::AppendAck {
                                    request_id: entry.request_id,
                                    stream_id,
                                    epoch,
                                    extent_id: entry.extent_id,
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
                                        extent_id: entry.extent_id,
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
                                            extent_id: entry.extent_id,
                                            epoch,
                                            offset: entry.offset,
                                            byte_pos: entry.byte_pos,
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
                                let aq_guard = self.ack_queues.pin();
                                let aq = aq_guard.get_or_insert_with(stream_id, || {
                                    AckQueue::with_timeout(
                                        ri.required_secondary_acks(),
                                        self.replication_timeout,
                                    )
                                });
                                let now = Instant::now();
                                for entry in &entries {
                                    aq.enqueue(PendingAck {
                                        request_id: entry.request_id,
                                        stream_id,
                                        response_tx: resp_tx.clone(),
                                        assigned_offset: entry.offset.0,
                                        extent_id: entry.extent_id,
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
                                    extent_id: entry.extent_id,
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

        // ── Extent-full: seal+create, retry failed frames, then drain ──
        if extent_full {
            let seal_notification = self.seal_and_create(stream_id, SealReason::ExtentFull);

            // Retry failed frames on the new extent (scoped pin guard).
            let remaining = {
                let guard = self.streams.pin();
                if let Some(stream) = guard.get(&stream_id) {
                    for ff in &failed_frames {
                        let (_, _) = self.do_append_and_respond(
                            stream,
                            ff.request_id,
                            stream_id,
                            epoch,
                            ff.payload.clone(),
                            response_tx.cloned(),
                        );
                    }
                    stream.in_flight().fetch_sub(batch_len, Ordering::Release)
                } else {
                    0
                }
            };

            if remaining > batch_len {
                let batch_seals = self.drain_follower_jobs(stream_id).await;
                for notif in &batch_seals {
                    self.send_extent_update(stream_id, notif);
                    self.send_forward_checksum(stream_id, notif.sealed_extent_id);
                    self.send_flush_request(stream_id, notif);
                }
            }
            if let Some(ref notif) = seal_notification {
                self.send_extent_update(stream_id, notif);
                self.send_forward_checksum(stream_id, notif.sealed_extent_id);
                self.send_flush_request(stream_id, notif);
            }
            return responses;
        }

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
            let batch_seals = self.drain_follower_jobs(stream_id).await;
            for notif in &batch_seals {
                self.send_extent_update(stream_id, notif);
                self.send_forward_checksum(stream_id, notif.sealed_extent_id);
                self.send_flush_request(stream_id, notif);
            }
        }

        responses
    }
}
