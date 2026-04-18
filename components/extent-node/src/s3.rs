use std::error::Error;

use aws_sdk_s3::{self as s3, primitives::ByteStreamError};
use common::config::ExtentNodeConfig;
use tracing::info;

/// S3 client wrapper for flushed extent storage.
///
/// Initialized from `ExtentNodeConfig::s3_profile` and `s3_bucket`.
/// Uses virtual-hosted-style addressing (required by COS and most S3-compatible services).
pub struct S3Client {
    client: s3::Client,
    bucket: String,
    namespace: String,
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

        info!(
            "S3Client initialized: profile={}, bucket={}, namespace={}, path_style={}",
            config.s3_profile, config.s3_bucket, config.s3_namespace, config.s3_path_style,
        );

        Some(Self {
            client,
            bucket: config.s3_bucket.clone(),
            namespace: config.s3_namespace.clone(),
        })
    }

    /// Upload data to S3 at the given key.
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
}
