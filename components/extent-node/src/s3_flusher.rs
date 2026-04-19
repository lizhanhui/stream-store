//! Background S3 flusher: uploads sealed extents to S3.
//!
//! Runs as a background tokio task, receiving [`FlushRequest`]s from the seal
//! path. Each request triggers an encode + upload of the sealed extent data.
//! On successful upload, notifies SM via `ExtentUpdate::Flushed`.

use std::sync::Arc;
use std::time::Duration;

use common::types::{ExtentId, StreamId};
use tokio::sync::mpsc;
use tokio::time::sleep;
use tracing::{info, warn};

use crate::s3::S3Client;
use crate::s3_codec::{encode_extent_range, s3_key};
use crate::store::{ExtentNodeStore, ExtentUpdate};

use rpc::frame::{Frame, VariableHeader};

/// Maximum backoff delay between S3 upload retries (30 seconds).
const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

/// Initial backoff delay for S3 upload retries (200 milliseconds).
const INITIAL_RETRY_DELAY: Duration = Duration::from_millis(200);

/// A request to flush a sealed extent to S3.
#[derive(Debug, Clone)]
pub struct FlushRequest {
    pub stream_id: StreamId,
    pub extent_id: ExtentId,
    pub start_offset: u64,
    pub end_offset: u64,
}

/// Background S3 flusher task.
///
/// Receives sealed extent notifications and uploads them to S3 with retry.
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

/// Encode and upload a single sealed extent to S3 with retry.
async fn flush(s3_client: &S3Client, store: &ExtentNodeStore, req: &FlushRequest) {
    // Encode the sealed extent into S3 file format.
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
                    "flush: stream {} not found, skipping extent {}",
                    req.stream_id, req.extent_id,
                );
                store
                    .dr_flush_in_progress
                    .lock()
                    .unwrap()
                    .remove(&(req.stream_id, req.extent_id));
                return;
            }
        };
        match stream.with_extent(req.extent_id, |ext| {
            encode_extent_range(req.stream_id, ext, s3_client.compression(), req.end_offset)
        }) {
            Some(result) => result,
            None => {
                warn!(
                    "flush: extent {} not found on stream {}, may have been evicted",
                    req.extent_id, req.stream_id,
                );
                store
                    .dr_flush_in_progress
                    .lock()
                    .unwrap()
                    .remove(&(req.stream_id, req.extent_id));
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

        // On retries, check if another replica already uploaded this extent.
        if attempt > 1 && s3_client.exists(&key).await {
            info!(
                "flush: extent {} for stream {} already exists at s3://{}/{}, skipping upload",
                req.extent_id, req.stream_id, s3_client.bucket(), key,
            );
            break;
        }

        match s3_client.upload(&key, encoded.clone()).await {
            Ok(()) => {
                if attempt > 1 {
                    info!(
                        "flushed extent {} for stream {} to s3://{}/{} ({} bytes, after {} attempts)",
                        req.extent_id,
                        req.stream_id,
                        s3_client.bucket(),
                        key,
                        data_len,
                        attempt,
                    );
                } else {
                    info!(
                        "flushed extent {} for stream {} to s3://{}/{} ({} bytes)",
                        req.extent_id,
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
                    "flush attempt {} failed for stream {} extent {}: {}, retrying in {:?}",
                    attempt, req.stream_id, req.extent_id, e, delay,
                );
                sleep(delay).await;
                delay = (delay * 2).min(MAX_RETRY_DELAY);
            }
        }
    }

    // Success bookkeeping. Only notify SM and mark flushed if the upload
    // was canonical (actual_end_offset == requested end_offset). A partial
    // upload preserves data in S3 but must not cause SM to transition the
    // extent to Flushed — a replica with full data still needs to upload
    // the canonical object.
    let is_canonical = actual_end_offset == req.end_offset;

    if is_canonical {
        let epoch = store
            .streams
            .pin()
            .get(&req.stream_id)
            .map(|s| s.epoch())
            .unwrap_or(common::types::Epoch(0));

        if let Some(ref tx) = store.update_tx {
            let _ = tx.try_send(ExtentUpdate::Flushed {
                stream_id: req.stream_id,
                extent_id: req.extent_id,
                epoch,
            });
        }

        let flushed_frame = Frame::new(
            VariableHeader::ForwardFlushed {
                stream_id: req.stream_id,
                extent_id: req.extent_id,
                epoch,
            },
            None,
        );
        if let Some(stream) = store.streams.pin().get(&req.stream_id) {
            stream.with_extent(req.extent_id, |ext| ext.mark_flushed());
            stream.send_forward(flushed_frame);
        }
    } else {
        info!(
            "flush: partial upload for stream {} extent {} (actual_end={}, requested_end={}), \
             not marking as flushed — waiting for canonical upload",
            req.stream_id, req.extent_id, actual_end_offset, req.end_offset,
        );
    }

    // Clear DR flush dedup tracker (no-op if this was a Primary flush).
    store
        .dr_flush_in_progress
        .lock()
        .unwrap()
        .remove(&(req.stream_id, req.extent_id));
}
