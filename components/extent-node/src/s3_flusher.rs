//! Background S3 flusher: uploads sealed arena bytes to S3.
//!
//! Runs as a background tokio task, receiving [`FlushRequest`]s from the seal
//! path. Each request triggers an encode + upload of the sealed arena data.
//! On successful canonical upload, notifies SM via `ExtentUpdate::Flushed`.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use common::types::{Epoch, StreamId};
use tokio::sync::mpsc;
use tokio::time::sleep;
use tracing::{info, warn};

use crate::s3::S3Client;
use crate::s3_codec::{encode_arena_range, s3_key};
use crate::store::{ExtentNodeStore, ExtentUpdate};

use rpc::frame::{Frame, VariableHeader};

/// Maximum backoff delay between S3 upload retries (30 seconds).
const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

/// Initial backoff delay for S3 upload retries (200 milliseconds).
const INITIAL_RETRY_DELAY: Duration = Duration::from_millis(200);

/// A request to flush sealed arena bytes for an epoch to S3.
#[derive(Debug, Clone)]
pub struct FlushRequest {
    pub stream_id: StreamId,
    pub epoch: Epoch,
    pub start_offset: u64,
    pub end_offset: u64,
}

/// Background S3 flusher task.
///
/// Receives sealed epoch notifications and uploads their arena bytes to S3 with retry.
/// Runs until the channel is closed (shutdown).
pub async fn run(
    s3_client: Arc<S3Client>,
    store: Arc<ExtentNodeStore>,
    mut flush_rx: mpsc::Receiver<FlushRequest>,
) {
    info!("S3 flusher started");

    while let Some(req) = flush_rx.recv().await {
        flush(&s3_client, &store, &req).await;
    }

    info!("S3 flusher stopped (channel closed)");
}

/// Encode and upload one epoch's sealed arena bytes to S3 with retry.
async fn flush(s3_client: &S3Client, store: &ExtentNodeStore, req: &FlushRequest) {
    // Encode the sealed arena bytes into S3 object format.
    // Returns (encoded_bytes, actual_end_offset). When local data < requested
    // end_offset, actual_end_offset < req.end_offset and the S3 key uses the
    // actual range — a partial upload under a non-canonical key that won't
    // overwrite the canonical object from a replica with full data.
    let (encoded, actual_end_offset) = {
        let guard = store.streams.pin();
        let stream = match guard.get(&req.stream_id) {
            Some(s) => s,
            None => {
                warn!(
                    "flush: stream {} not found, skipping epoch {}",
                    req.stream_id, req.epoch,
                );
                return;
            }
        };
        match stream.with_epoch(req.epoch, |ext| {
            let pool = stream.pool();
            encode_arena_range(
                req.stream_id,
                ext,
                &**pool,
                s3_client.compression(),
                req.end_offset,
            )
        }) {
            Some(result) => result,
            None => {
                warn!(
                    "flush: epoch {} not found on stream {}, may have been evicted",
                    req.epoch, req.stream_id,
                );
                if let Some(s) = store.streams.pin().get(&req.stream_id) {
                    s.finish_flush(req.epoch);
                }
                return;
            }
        }
    };

    // S3 key uses actual_end_offset: canonical if full data, distinct if partial.
    let key = s3_key(
        s3_client.namespace(),
        req.stream_id,
        req.start_offset,
        actual_end_offset,
    );
    let data_len = encoded.len();

    // Upload with indefinite retry. S3 is the durability layer — giving up
    // means accepting data loss. Backoff exponentially up to MAX_RETRY_DELAY.
    // On retry, check if a peer replica already uploaded (S3 HEAD) to avoid
    // redundant work during concurrent DR flush.
    let mut attempt = 0u32;
    let mut delay = INITIAL_RETRY_DELAY;
    loop {
        attempt += 1;

        // On retries, check if another replica already uploaded this arena object.
        if attempt > 1 && s3_client.exists(&key).await {
            info!(
                "flush: epoch {} for stream {} already exists at s3://{}/{}, skipping upload",
                req.epoch,
                req.stream_id,
                s3_client.bucket(),
                key,
            );
            break;
        }

        match s3_client.upload(&key, encoded.clone()).await {
            Ok(()) => {
                if attempt > 1 {
                    info!(
                        "flushed epoch {} for stream {} to s3://{}/{} ({} bytes, after {} attempts)",
                        req.epoch,
                        req.stream_id,
                        s3_client.bucket(),
                        key,
                        data_len,
                        attempt,
                    );
                } else {
                    info!(
                        "flushed epoch {} for stream {} to s3://{}/{} ({} bytes)",
                        req.epoch,
                        req.stream_id,
                        s3_client.bucket(),
                        key,
                        data_len,
                    );
                }
                break;
            }
            Err(e) => {
                warn!(
                    "flush attempt {} failed for stream {} epoch {}: {}, retrying in {:?}",
                    attempt, req.stream_id, req.epoch, e, delay,
                );
                sleep(delay).await;
                delay = (delay * 2).min(MAX_RETRY_DELAY);
            }
        }
    }

    // Success bookkeeping. Only notify SM and mark flushed if the upload
    // was canonical (actual_end_offset == requested end_offset). A partial
    // upload preserves data in S3 but must not cause SM to transition the
    // epoch to Flushed — a replica with full data still needs to upload
    // the canonical object.
    let is_canonical = actual_end_offset == req.end_offset;

    if is_canonical {
        // Read the sealed epoch's own epoch value. This is distinct from the
        // stream's current epoch, which may have been bumped by RegisterEpoch
        // for a successor epoch.
        let epoch = store
            .streams
            .pin()
            .get(&req.stream_id)
            .and_then(|s| s.with_epoch(req.epoch, |ext| ext.epoch))
            .unwrap_or(common::types::Epoch(0));

        if let Some(ref tx) = store.update_tx {
            let _ = tx.try_send(ExtentUpdate::Flushed {
                stream_id: req.stream_id,
                epoch,
                start_offset: req.start_offset,
                end_offset: req.end_offset,
                s3_key: Bytes::from(key.clone()),
            });
        }

        let flushed_frame = Frame::new(
            VariableHeader::ForwardFlushed {
                stream_id: req.stream_id,
                epoch,
            },
            None,
        );
        if let Some(stream) = store.streams.pin().get(&req.stream_id) {
            stream.with_epoch(req.epoch, |ext| ext.mark_flushed());
            stream.send_forward(flushed_frame);
        }
    } else {
        info!(
            "flush: partial upload for stream {} epoch {} (actual_end={}, requested_end={}), \
             not marking as flushed — waiting for canonical upload",
            req.stream_id, req.epoch, actual_end_offset, req.end_offset,
        );
    }

    // Clear DR flush dedup tracker (no-op if this was a Primary flush).
    if let Some(s) = store.streams.pin().get(&req.stream_id) {
        s.finish_flush(req.epoch);
    }
}
