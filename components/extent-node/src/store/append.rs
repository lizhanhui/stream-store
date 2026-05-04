use std::sync::Arc;
use std::sync::atomic::Ordering;

use common::types::{Epoch, ErrorCode, StreamId};
use rpc::frame::{Frame, VariableHeader};
use tokio::sync::mpsc::Sender;
use tracing::debug;

use super::{AppendRequest, ExtentNodeStore};
use crate::stream::Stream;

impl ExtentNodeStore {
    /// Handle Append — pipelined group commit with stream-level leader election.
    ///
    /// The Store is a thin router: one papaya lookup to extract
    /// `Arc<Stream>`, then the per-stream leader election / append /
    /// drain happens on the Stream itself (no further map lookups).
    ///
    /// - `prev == 0`: this task becomes the leader writer.
    ///   Calls `stream.append_one(...)`, then `stream.drain_delegated_requests()`
    ///   if followers arrived.
    /// - `prev > 0`: pushes an `AppendRequest` to the channel and returns.
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
            let job = AppendRequest {
                request_id: frame.request_id(),
                stream_id,
                payload: frame.payload.clone().unwrap_or_default(),
                response_tx: response_tx.cloned(),
            };
            let _ = stream.request_tx().send(job);
            return None;
        }

        // FAST PATH: I'm the leader writer (prev == 0).
        let payload = frame.payload.clone().unwrap_or_default();
        let request_id = frame.request_id();
        let own_result = stream.append_one(request_id, epoch, payload, response_tx.cloned());
        let remaining = stream.in_flight().fetch_sub(1, Ordering::Release);

        if remaining > 1 {
            stream.drain_delegated_requests().await;
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
            let pool = stream.pool();
            (
                ext.finalized_crc32().unwrap_or(0),
                pool.committed_data(stream_id, sealed_epoch).len() as u64,
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
    /// Thin router: resolves the stream, checks epoch, elects the leader,
    /// then delegates to `Stream::append_batch_leader` for the actual
    /// write + dispatch.
    ///
    /// Pin guards are scoped in blocks so they're dropped before `.await` points
    /// (papaya pin guards are non-Send).
    pub(crate) async fn handle_append_batch_inner(
        &self,
        frames: &[Frame],
        response_tx: Option<&Sender<Frame>>,
    ) -> Vec<Frame> {
        let stream_id = frames[0].stream_id();

        // Resolve the Stream once; drop the pin guard before any await.
        let stream: Arc<Stream> = {
            let guard = self.streams.pin();
            match guard.get(&stream_id) {
                Some(s) => Arc::clone(s),
                None => {
                    return frames
                        .iter()
                        .map(|frame| {
                            Frame::error_from_request(
                                frame,
                                ErrorCode::UnknownStream,
                                &format!("stream {} not found", stream_id),
                            )
                        })
                        .collect();
                }
            }
        };

        let epoch = stream.epoch();
        let client_epoch = frames[0].epoch();
        if client_epoch != Epoch(0) && client_epoch != epoch {
            return frames
                .iter()
                .map(|frame| {
                    Frame::error_from_request(
                        frame,
                        ErrorCode::EpochStale,
                        &format!("epoch stale: client={}, current={}", client_epoch, epoch),
                    )
                })
                .collect();
        }

        let batch_len = frames.len() as u64;
        let prev = stream.in_flight().fetch_add(batch_len, Ordering::Acquire);

        if prev > 0 {
            // SLOW PATH: leader writer exists. Push all as AppendRequests.
            for frame in frames {
                let job = AppendRequest {
                    request_id: frame.request_id(),
                    stream_id,
                    payload: frame.payload.clone().unwrap_or_default(),
                    response_tx: response_tx.cloned(),
                };
                let _ = stream.request_tx().send(job);
            }
            return Vec::new(); // All deferred — empty responses.
        }

        // FAST PATH: I'm the leader writer (prev == 0).
        let responses = stream.append_batch_leader(epoch, frames, response_tx);

        let remaining = stream.in_flight().fetch_sub(batch_len, Ordering::Release);

        if remaining > batch_len {
            stream.drain_delegated_requests().await;
        }

        responses
    }
}
