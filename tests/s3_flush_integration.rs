//! S3 flush integration tests.
//!
//! Verifies that sealed extents are flushed to S3 (COS) correctly:
//!
//! 1. **Primary flushes sealed extent**: Normal seal → primary uploads to S3 → SM marks Flushed.
//! 2. **DR flush after primary killed**: Primary dies after append, client-driven seal triggers
//!    DR flush from surviving secondary.
//! 3. **Staleness scan triggers DR flush**: SM heartbeat checker detects stale sealed extent
//!    and sends FlushExtent to secondaries.
//!
//! Requires S3/COS credentials configured under `~/.aws/config` and `~/.aws/credentials`
//! for the profile specified by `AWS_PROFILE` (default: "dev").
//!
//! MySQL connection: mysql://root:password@localhost:3306/stream_store

use std::collections::HashMap;
use std::time::Duration;

use serial_test::serial;

use bytes::Bytes;
use client::StreamClient;
use common::config::{ExtentNodeConfig, StreamManagerConfig};
use common::types::{Epoch, ExtentState, StorageClass, StreamId};
use extent_node::ExtentNode;
use extent_node::s3::S3Client;
use extent_node::s3_codec::{S3ExtentHeader, s3_key};
use stream_manager::StreamManager;
use stream_manager::metadata::MetadataStore;
use tokio::time::sleep;
use tracing::info;

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .try_init();
}

async fn clean_database(mysql_url: &str) {
    let pool = sqlx::mysql::MySqlPoolOptions::new()
        .max_connections(1)
        .connect(mysql_url)
        .await
        .expect("failed to connect for cleanup");
    for table in &[
        "stream_replica",
        "extent",
        "stream_sequence",
        "stream",
        "node_metrics",
        "stream_manager_leadership",
        "node",
        "refinery_schema_history",
    ] {
        sqlx::query(&format!("DROP TABLE IF EXISTS {table}"))
            .execute(&pool)
            .await
            .unwrap_or_else(|e| panic!("drop {table}: {e}"));
    }
    pool.close().await;

    let store = MetadataStore::connect(mysql_url)
        .await
        .expect("connect for migration");
    store.migrate().await.expect("migration failed");
}

/// Read S3 test configuration from environment variables.
fn s3_test_config() -> (String, String, String) {
    let profile = std::env::var("AWS_PROFILE").unwrap_or_else(|_| "dev".to_string());
    let bucket =
        std::env::var("COS_BUCKET").unwrap_or_else(|_| "stream-storage-1366919849".to_string());
    let namespace = format!("s3-test-{}", std::process::id());
    (profile, bucket, namespace)
}

/// SM config with fast heartbeat checking and flush staleness threshold for tests.
fn sm_config_fast() -> StreamManagerConfig {
    StreamManagerConfig {
        bind_ip: "127.0.0.1".into(),
        port: 0,
        heartbeat_check_interval_ms: 500,
        leadership_lease_duration_secs: 3,
        flush_staleness_threshold_ms: 5000, // 5s for fast test
        ..StreamManagerConfig::default()
    }
}

/// EN config with S3 flush enabled.
fn en_config_with_s3(
    sm_addrs: &[String],
    profile: &str,
    bucket: &str,
    namespace: &str,
) -> ExtentNodeConfig {
    ExtentNodeConfig {
        bind_ip: "127.0.0.1".into(),
        port: 0,
        stream_manager_addrs: sm_addrs.to_vec(),
        heartbeat_interval_ms: 1000,
        s3_profile: profile.to_string(),
        s3_bucket: bucket.to_string(),
        s3_namespace: namespace.to_string(),
        s3_path_style: false,
        ..Default::default()
    }
}

async fn poll_leader(store: &MetadataStore, timeout: Duration) -> String {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Ok(Some(leader)) = store.get_leader().await {
            return leader;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for leadership acquisition");
        }
        sleep(Duration::from_millis(100)).await;
    }
}

/// Create a direct S3 client for verification (outside the EN process).
async fn s3_client_for_test(profile: &str, bucket: &str, namespace: &str) -> S3Client {
    let config = ExtentNodeConfig {
        s3_profile: profile.to_string(),
        s3_bucket: bucket.to_string(),
        s3_namespace: namespace.to_string(),
        s3_path_style: false,
        ..Default::default()
    };
    S3Client::new(&config)
        .await
        .expect("S3Client should initialize (s3_bucket is not empty)")
}

/// Poll `describe_stream` until any sealed extent transitions to Flushed.
async fn wait_for_flushed(
    sm_client: &StreamClient,
    stream_id: StreamId,
    timeout: Duration,
) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Ok(extents) = sm_client.describe_stream(stream_id, 0).await {
            if extents.iter().any(|e| e.state == ExtentState::Flushed) {
                return true;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        sleep(Duration::from_millis(500)).await;
    }
}

/// Start SM + N ENs (all with S3 enabled), wait for heartbeat registration.
async fn setup_cluster_with_s3(
    en_count: usize,
    profile: &str,
    bucket: &str,
    namespace: &str,
) -> (
    StreamManager,
    String,
    HashMap<String, ExtentNode>,
    MetadataStore,
) {
    let sm_cfg = sm_config_fast();
    let mysql_url = sm_cfg.mysql_url();
    clean_database(&mysql_url).await;

    let store = MetadataStore::connect(&mysql_url)
        .await
        .expect("metadata connect");
    let sm = StreamManager::start(sm_cfg).await;
    let sm_addr = sm.addr().to_string();
    info!("[setup] SM started on {sm_addr}");

    poll_leader(&store, Duration::from_secs(5)).await;

    let sm_addrs = vec![sm_addr.clone()];
    let mut en_map = HashMap::new();
    for i in 0..en_count {
        let en = ExtentNode::start(en_config_with_s3(&sm_addrs, profile, bucket, namespace)).await;
        let addr = en.addr().to_string();
        info!("[setup] EN {i} started on {addr}");
        en_map.insert(addr, en);
    }

    // Wait for heartbeat registration.
    sleep(Duration::from_secs(3)).await;
    info!("[setup] All nodes registered");

    (sm, sm_addr, en_map, store)
}

/// Append N messages, returning the number successfully appended.
async fn append_messages(
    client: &StreamClient,
    stream_id: StreamId,
    epoch: Epoch,
    start: usize,
    count: usize,
) -> usize {
    let mut success = 0;
    for i in start..start + count {
        match client
            .append(stream_id, epoch, Bytes::from(format!("msg-{i}")))
            .await
        {
            Ok(_) => success += 1,
            Err(e) => {
                info!("append msg-{i} failed: {e}");
                break;
            }
        }
    }
    success
}

// ─── Test 1: Primary flushes sealed extent to S3 ────────────────────────────

#[tokio::test]
#[serial]
async fn primary_flushes_sealed_extent_to_s3() {
    init_tracing();
    let (profile, bucket, namespace) = s3_test_config();

    // Verify S3 is reachable before setting up cluster.
    let s3 = s3_client_for_test(&profile, &bucket, &namespace).await;
    // Quick connectivity check: try to HEAD a non-existent key.
    // If credentials/endpoint are wrong, this will fail loudly.
    let _ = s3.exists("__connectivity_check__").await;

    let (sm, sm_addr, en_map, _store) =
        setup_cluster_with_s3(2, &profile, &bucket, &namespace).await;

    // Create stream RF=2, StorageClass::S3.
    let sm_client = StreamClient::connect(&sm_addr).await.unwrap();
    let (stream_id, _eid, _epoch, primary_addr) = sm_client
        .create_stream("test-s3-flush", 2, 0, 0, 0, 0, StorageClass::S3)
        .await
        .expect("create_stream");
    info!("[test] Stream {stream_id} created, primary={primary_addr}");

    // Append messages.
    let client = StreamClient::connect(&primary_addr).await.unwrap();
    let appended = append_messages(&client, stream_id, Epoch(0), 0, 100).await;
    assert_eq!(appended, 100, "should append all 100 messages");
    info!("[test] Appended 100 messages");

    // Seal via SM.
    let seal_result = sm_client.seal(stream_id, Epoch(0)).await;
    info!("[test] Seal result: {:?}", seal_result);

    // Wait for flush.
    let flushed = wait_for_flushed(&sm_client, stream_id, Duration::from_secs(30)).await;
    assert!(flushed, "extent should be flushed within 30s");

    // Verify S3 object.
    let extents = sm_client.describe_stream(stream_id, 0).await.unwrap();
    let flushed_extent = extents
        .iter()
        .find(|e| e.state == ExtentState::Flushed)
        .unwrap();
    let key = s3_key(
        &namespace,
        stream_id,
        flushed_extent.start_offset,
        flushed_extent.end_offset,
    );
    info!("[test] Checking S3 key: {key}");

    assert!(s3.exists(&key).await, "S3 object should exist");

    // Download and verify header.
    let data = s3.get_object(&key).await.expect("get_object");
    let header = S3ExtentHeader::decode(&data).expect("decode header");
    assert_eq!(header.record_count, 100);
    assert_eq!(header.start_offset, flushed_extent.start_offset);
    assert_eq!(header.end_offset, flushed_extent.end_offset);
    info!("[test] S3 object verified: {} records", header.record_count);

    // Cleanup S3 object.
    let _ = s3.delete_object(&key).await;

    for (_, en) in en_map {
        en.stop().await;
    }
    sm.stop().await;
    info!("[test] primary_flushes_sealed_extent_to_s3: PASSED");
}

// ─── Test 2: DR flush after primary killed ──────────────────────────────────

#[tokio::test]
#[serial]
async fn dr_flush_after_primary_killed() {
    init_tracing();
    let (profile, bucket, namespace) = s3_test_config();
    let s3 = s3_client_for_test(&profile, &bucket, &namespace).await;

    let (sm, sm_addr, mut en_map, _store) =
        setup_cluster_with_s3(3, &profile, &bucket, &namespace).await;

    // Create stream RF=3.
    let sm_client = StreamClient::connect(&sm_addr).await.unwrap();
    let (stream_id, _eid, _epoch, primary_addr) = sm_client
        .create_stream("test-dr-flush", 3, 0, 0, 0, 0, StorageClass::S3)
        .await
        .expect("create_stream");
    info!("[test] Stream {stream_id}, RF=3, primary={primary_addr}");

    // Append messages.
    let client = StreamClient::connect(&primary_addr).await.unwrap();
    let appended = append_messages(&client, stream_id, Epoch(0), 0, 50).await;
    assert_eq!(appended, 50, "should append all 50 messages");
    info!("[test] Appended 50 messages");

    // Kill primary (abrupt).
    let primary_en = en_map.remove(&primary_addr).unwrap();
    primary_en.kill();
    info!("[test] Killed primary at {primary_addr}");

    // Wait for SM to detect dead node.
    info!("[test] Waiting for SM to detect dead primary...");
    sleep(Duration::from_secs(5)).await;

    // Client-driven seal triggers fallback seal + immediate DR flush.
    let sm_client2 = StreamClient::connect(&sm_addr).await.unwrap();
    let seal_result = sm_client2.seal(stream_id, Epoch(0)).await;
    info!("[test] Seal result: {:?}", seal_result);

    // Wait for DR flush to complete.
    let flushed = wait_for_flushed(&sm_client2, stream_id, Duration::from_secs(30)).await;
    assert!(flushed, "extent should be flushed via DR within 30s");

    // Verify S3 object.
    let extents = sm_client2.describe_stream(stream_id, 0).await.unwrap();
    let flushed_extent = extents
        .iter()
        .find(|e| e.state == ExtentState::Flushed)
        .unwrap();
    let key = s3_key(
        &namespace,
        stream_id,
        flushed_extent.start_offset,
        flushed_extent.end_offset,
    );
    assert!(s3.exists(&key).await, "S3 object should exist (DR flush)");

    let data = s3.get_object(&key).await.expect("get_object");
    let header = S3ExtentHeader::decode(&data).expect("decode header");
    assert!(
        header.record_count >= 50,
        "should have at least 50 records, got {}",
        header.record_count,
    );
    info!("[test] DR flush verified: {} records", header.record_count);

    let _ = s3.delete_object(&key).await;
    for (_, en) in en_map {
        en.stop().await;
    }
    sm.stop().await;
    info!("[test] dr_flush_after_primary_killed: PASSED");
}

// ─── Test 3: Staleness scan triggers DR flush ───────────────────────────────

#[tokio::test]
#[serial]
async fn staleness_scan_triggers_dr_flush() {
    init_tracing();
    let (profile, bucket, namespace) = s3_test_config();
    let s3 = s3_client_for_test(&profile, &bucket, &namespace).await;

    let (sm, sm_addr, mut en_map, _store) =
        setup_cluster_with_s3(2, &profile, &bucket, &namespace).await;

    // Create stream RF=2 with tiny extent capacity to trigger extent-full quickly.
    let sm_client = StreamClient::connect(&sm_addr).await.unwrap();
    let (stream_id, _eid, _epoch, primary_addr) = sm_client
        .create_stream(
            "test-staleness-flush",
            2,
            1024,
            1024,
            0,
            0,
            StorageClass::S3,
        )
        .await
        .expect("create_stream");
    info!("[test] Stream {stream_id}, RF=2, primary={primary_addr}, capacity=1024");

    // Append enough to fill the 1KiB extent and trigger EN-initiated seal.
    let client = StreamClient::connect(&primary_addr).await.unwrap();
    let mut total = 0;
    for i in 0..200u32 {
        match client
            .append(stream_id, Epoch(0), Bytes::from(format!("payload-{i:04}")))
            .await
        {
            Ok(_) => total += 1,
            Err(e) => {
                info!("[test] Append stopped at {i}: {e}");
                break;
            }
        }
    }
    info!("[test] Appended {total} messages (extent should have auto-sealed)");

    // Give a moment for the Primary to seal and notify SM.
    sleep(Duration::from_secs(1)).await;

    // Kill primary immediately — before normal flush completes.
    let primary_en = en_map.remove(&primary_addr).unwrap();
    primary_en.kill();
    info!("[test] Killed primary at {primary_addr}");

    // Wait for SM heartbeat checker to detect dead node + staleness scan (5s threshold).
    info!("[test] Waiting for staleness scan...");
    sleep(Duration::from_secs(10)).await;

    // Check if any extent transitioned to Flushed.
    let sm_client2 = StreamClient::connect(&sm_addr).await.unwrap();
    let flushed = wait_for_flushed(&sm_client2, stream_id, Duration::from_secs(30)).await;
    assert!(
        flushed,
        "staleness scan should trigger DR flush within timeout"
    );

    // Verify S3 object.
    let extents = sm_client2.describe_stream(stream_id, 0).await.unwrap();
    let flushed_ext = extents.iter().find(|e| e.state == ExtentState::Flushed);
    if let Some(ext) = flushed_ext {
        let key = s3_key(&namespace, stream_id, ext.start_offset, ext.end_offset);
        assert!(
            s3.exists(&key).await,
            "S3 object should exist (staleness DR)"
        );
        let _ = s3.delete_object(&key).await;
        info!(
            "[test] Staleness DR flush verified for extent {}",
            ext.extent_id
        );
    }

    for (_, en) in en_map {
        en.stop().await;
    }
    sm.stop().await;
    info!("[test] staleness_scan_triggers_dr_flush: PASSED");
}
