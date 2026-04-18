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
use tracing::{error, info, warn};

use crate::s3::S3Client;
use crate::s3_codec::{encode_extent, s3_key};
use crate::store::{ExtentNodeStore, ExtentUpdate};

use rpc::frame::{Frame, VariableHeader};

/// Maximum number of upload retries before giving up on a flush request.
const MAX_RETRIES: u32 = 3;

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
    let encoded = {
        let guard = store.streams.pin();
        let stream = match guard.get(&req.stream_id) {
            Some(s) => s,
            None => {
                warn!(
                    "flush: stream {} not found, skipping extent {}",
                    req.stream_id, req.extent_id,
                );
                return;
            }
        };
        match stream.with_extent(req.extent_id, |ext| {
            encode_extent(req.stream_id, ext, s3_client.compression())
        }) {
            Some(data) => data,
            None => {
                warn!(
                    "flush: extent {} not found on stream {}, may have been evicted",
                    req.extent_id, req.stream_id,
                );
                return;
            }
        }
    };

    let key = s3_key(
        s3_client.namespace(),
        req.stream_id,
        req.start_offset,
        req.end_offset,
    );
    let data_len = encoded.len();

    // Upload with retry.
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        match s3_client.upload(&key, encoded.clone()).await {
            Ok(()) => {
                info!(
                    "flushed extent {} for stream {} to s3://{}/{} ({} bytes)",
                    req.extent_id,
                    req.stream_id,
                    s3_client.bucket(),
                    key,
                    data_len,
                );

                // Look up epoch once for both SM notification and ForwardFlushed.
                let epoch = store
                    .streams
                    .pin()
                    .get(&req.stream_id)
                    .map(|s| s.epoch())
                    .unwrap_or(common::types::Epoch(0));

                // Notify SM that this extent is now flushed to S3.
                if let Some(ref tx) = store.update_tx {
                    let _ = tx.try_send(ExtentUpdate::Flushed {
                        stream_id: req.stream_id,
                        extent_id: req.extent_id,
                        epoch,
                    });
                }

                // Broadcast ForwardFlushed to secondaries so they can mark
                // the extent as eligible for eviction.
                let flushed_frame = Frame::new(
                    VariableHeader::ForwardFlushed {
                        stream_id: req.stream_id,
                        extent_id: req.extent_id,
                        epoch,
                    },
                    None,
                );
                if let Some(stream) = store.streams.pin().get(&req.stream_id) {
                    // Mark locally on Primary as well.
                    stream.with_extent(req.extent_id, |ext| ext.mark_flushed());
                    stream.send_forward(flushed_frame);
                }

                return;
            }
            Err(e) => {
                if attempt >= MAX_RETRIES {
                    error!(
                        "flush failed after {} attempts for stream {} extent {}: {}",
                        MAX_RETRIES, req.stream_id, req.extent_id, e,
                    );
                    return;
                }
                let delay_ms = 100 * (1 << attempt); // 200ms, 400ms
                warn!(
                    "flush attempt {}/{} failed for stream {} extent {}: {}, retrying in {}ms",
                    attempt, MAX_RETRIES, req.stream_id, req.extent_id, e, delay_ms,
                );
                sleep(Duration::from_millis(delay_ms)).await;
            }
        }
    }
}
