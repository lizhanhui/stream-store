use std::error::Error;

use aws_sdk_s3::{self as s3, primitives::ByteStreamError};
use bytes::Bytes;
use common::config::ExtentNodeConfig;
use futures_util::stream::{self, StreamExt};
use tracing::{info, warn};

use crate::s3_codec::Compression;

/// Maximum retries for each individual part upload.
const PART_MAX_RETRIES: u32 = 3;

/// S3 client wrapper for flushed extent storage.
///
/// Initialized from `ExtentNodeConfig::s3_profile` and `s3_bucket`.
/// Uses virtual-hosted-style addressing (required by COS and most S3-compatible services).
///
/// Supports automatic multipart upload for large objects: objects above
/// `multipart_threshold` are split into `part_size` chunks and uploaded
/// concurrently (up to `concurrency` parts in flight).
pub struct S3Client {
    client: s3::Client,
    bucket: String,
    namespace: String,
    compression: Compression,
    multipart_threshold: usize,
    part_size: usize,
    concurrency: usize,
}

impl S3Client {
    /// Create a new S3Client from the given ExtentNodeConfig.
    ///
    /// Returns `None` if `s3_bucket` is empty (S3 flush disabled).
    pub async fn new(config: &ExtentNodeConfig) -> Option<Self> {
        if config.s3_bucket.is_empty() {
            return None;
        }

        let aws_config = aws_config::from_env()
            .profile_name(&config.s3_profile)
            .load()
            .await;

        let mut builder = s3::config::Builder::from(&aws_config);
        builder.set_force_path_style(Some(config.s3_path_style));

        let client = s3::Client::from_conf(builder.build());

        let compression = Compression::from_config(&config.s3_compression).unwrap_or_else(|e| {
            tracing::warn!("invalid s3_compression config: {e}, defaulting to None");
            Compression::None
        });

        info!(
            "S3Client initialized: profile={}, bucket={}, namespace={}, path_style={}, compression={:?}, \
             multipart_threshold={}, part_size={}, concurrency={}",
            config.s3_profile,
            config.s3_bucket,
            config.s3_namespace,
            config.s3_path_style,
            compression,
            config.s3_multipart_threshold,
            config.s3_multipart_part_size,
            config.s3_multipart_concurrency,
        );

        Some(Self {
            client,
            bucket: config.s3_bucket.clone(),
            namespace: config.s3_namespace.clone(),
            compression,
            multipart_threshold: config.s3_multipart_threshold,
            part_size: config.s3_multipart_part_size,
            concurrency: config.s3_multipart_concurrency,
        })
    }

    /// Upload data to S3 at the given key.
    ///
    /// Automatically selects single `put_object` or multipart upload based on
    /// data size vs `multipart_threshold`.
    pub async fn upload(&self, key: &str, data: Vec<u8>) -> Result<(), S3Error> {
        if data.len() < self.multipart_threshold {
            return self.put_object(key, data).await;
        }
        // Spawn multipart as an independent future to avoid &self Send issues.
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = key.to_string();
        let part_size = self.part_size;
        let concurrency = self.concurrency;
        let handle = tokio::spawn(multipart_upload(
            client,
            bucket,
            key,
            data,
            part_size,
            concurrency,
        ));
        handle.await.unwrap()
    }

    /// Single-request upload via `put_object`.
    pub async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), S3Error> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(data.into())
            .send()
            .await
            .map_err(|e| S3Error::PutFailed(key.to_string(), e.into()))?;

        Ok(())
    }

    /// Download an object from S3 by key.
    pub async fn get_object(&self, key: &str) -> Result<Vec<u8>, S3Error> {
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| S3Error::GetFailed(key.to_string(), e.into()))?;

        let data = resp
            .body
            .collect()
            .await
            .map_err(|e| S3Error::BodyReadFailed(key.to_string(), e))?
            .into_bytes();

        Ok(data.into())
    }

    /// Delete an object from S3 by key.
    pub async fn delete_object(&self, key: &str) -> Result<(), S3Error> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| S3Error::DeleteFailed(key.to_string(), e.into()))?;

        Ok(())
    }

    /// The configured S3 bucket name.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// The configured S3 namespace prefix.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// The configured compression algorithm for S3 extent chunks.
    pub fn compression(&self) -> Compression {
        self.compression
    }
}

/// Multipart upload: split data into parts and upload concurrently.
///
/// Free function (not `&self`) so the returned future is `Send` —
/// all captured state is owned, satisfying `tokio::spawn` requirements.
///
/// 1. `create_multipart_upload` → `upload_id`
/// 2. Split data into `part_size` chunks (zero-copy via `Bytes::slice`)
/// 3. Upload parts concurrently (`buffer_unordered(concurrency)`)
/// 4. `complete_multipart_upload` with all ETags
/// 5. On error: `abort_multipart_upload` to clean up leaked parts
async fn multipart_upload(
    client: s3::Client,
    bucket: String,
    key: String,
    data: Vec<u8>,
    part_size: usize,
    concurrency: usize,
) -> Result<(), S3Error> {
    let data_len = data.len();
    let data = Bytes::from(data);

    // 1. Initiate multipart upload.
    let create_resp = client
        .create_multipart_upload()
        .bucket(&bucket)
        .key(&key)
        .send()
        .await
        .map_err(|e| S3Error::CreateMultipartFailed(key.clone(), e.into()))?;

    let upload_id = create_resp
        .upload_id()
        .ok_or_else(|| {
            S3Error::CreateMultipartFailed(key.clone(), "no upload_id in response".into())
        })?
        .to_string();

    // 2. Build part descriptors (zero-copy slices of the Bytes buffer).
    let num_parts = (data_len + part_size - 1) / part_size;
    let parts: Vec<(i32, Bytes)> = (0..num_parts)
        .map(|i| {
            let start = i * part_size;
            let end = std::cmp::min(start + part_size, data_len);
            let part_number = (i + 1) as i32; // S3 part numbers are 1-based
            (part_number, data.slice(start..end))
        })
        .collect();

    info!(
        "multipart upload: key={}, size={}, parts={}, part_size={}, concurrency={}",
        key, data_len, num_parts, part_size, concurrency,
    );

    // 3. Upload parts concurrently with per-part retry.
    let result: Result<Vec<s3::types::CompletedPart>, S3Error> = {
        let num_parts = parts.len();
        let results: Vec<Result<s3::types::CompletedPart, S3Error>> =
            stream::iter(parts.into_iter().map(|(part_number, body)| {
                let client = client.clone();
                let bucket = bucket.clone();
                let key = key.clone();
                let upload_id = upload_id.clone();
                async move {
                    upload_part_with_retry(&client, &bucket, &key, &upload_id, part_number, body)
                        .await
                }
            }))
            .buffer_unordered(concurrency)
            .collect()
            .await;

        let mut completed: Vec<s3::types::CompletedPart> = Vec::with_capacity(num_parts);
        for r in results {
            completed.push(r?);
        }
        completed.sort_by_key(|p| p.part_number());
        Ok(completed)
    };

    match result {
        Ok(completed_parts) => {
            // 4. Complete the multipart upload.
            let completed = s3::types::CompletedMultipartUpload::builder()
                .set_parts(Some(completed_parts))
                .build();

            client
                .complete_multipart_upload()
                .bucket(&bucket)
                .key(&key)
                .upload_id(&upload_id)
                .multipart_upload(completed)
                .send()
                .await
                .map_err(|e| S3Error::CompleteMultipartFailed(key.clone(), e.into()))?;

            Ok(())
        }
        Err(e) => {
            // 5. Abort on failure — clean up uploaded parts.
            warn!(
                "multipart upload failed for {}, aborting upload_id={}: {}",
                key, upload_id, e
            );
            let _ = client
                .abort_multipart_upload()
                .bucket(&bucket)
                .key(&key)
                .upload_id(&upload_id)
                .send()
                .await;
            Err(e)
        }
    }
}

/// Upload a single part with exponential-backoff retry.
async fn upload_part_with_retry(
    client: &s3::Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i32,
    body: Bytes,
) -> Result<s3::types::CompletedPart, S3Error> {
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        // Bytes::clone is O(1) — just an Arc increment.
        let result = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(body.clone().into())
            .send()
            .await;

        match result {
            Ok(resp) => {
                let e_tag = resp.e_tag().unwrap_or_default().to_string();
                return Ok(s3::types::CompletedPart::builder()
                    .part_number(part_number)
                    .e_tag(e_tag)
                    .build());
            }
            Err(e) => {
                if attempt >= PART_MAX_RETRIES {
                    return Err(S3Error::UploadPartFailed(
                        key.to_string(),
                        part_number,
                        e.into(),
                    ));
                }
                let delay_ms = 100 * (1u64 << attempt); // 200ms, 400ms
                warn!(
                    "upload_part {}/{} attempt {}/{} failed: {}, retrying in {}ms",
                    key, part_number, attempt, PART_MAX_RETRIES, e, delay_ms,
                );
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            }
        }
    }
}

/// Errors from S3 operations.
#[derive(Debug, thiserror::Error)]
pub enum S3Error {
    #[error("put_object({0}) failed: {1}")]
    PutFailed(String, #[source] Box<dyn Error + Send + Sync>),

    #[error("get_object({0}) failed: {1}")]
    GetFailed(String, #[source] Box<dyn Error + Send + Sync>),

    #[error("delete_object({0}) failed: {1}")]
    DeleteFailed(String, #[source] Box<dyn Error + Send + Sync>),

    #[error("body read for {0} failed: {1}")]
    BodyReadFailed(String, #[source] ByteStreamError),

    #[error("create_multipart_upload({0}) failed: {1}")]
    CreateMultipartFailed(String, #[source] Box<dyn Error + Send + Sync>),

    #[error("upload_part({0}, part {1}) failed: {2}")]
    UploadPartFailed(String, i32, #[source] Box<dyn Error + Send + Sync>),

    #[error("complete_multipart_upload({0}) failed: {1}")]
    CompleteMultipartFailed(String, #[source] Box<dyn Error + Send + Sync>),
}
