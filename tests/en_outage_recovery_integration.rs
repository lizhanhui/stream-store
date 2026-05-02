//! ExtentNode outage recovery integration test.
//!
//! Verifies that with RF=3 and 3 extent-nodes, client appending can recover
//! when one of the nodes suffers an outage:
//!
//! 1. **Primary killed**: SM detects via expired heartbeat, seals the extent
//!    via secondary quorum, allocates a new extent on surviving nodes.
//!    Client calls seal_by_epoch on SM, discovers new primary, resumes appending.
//!
//! 2. **Secondary killed**: Primary's PendingAcks timeout (replication quorum
//!    broken), SM detects dead node, seals + reallocates. Client reconnects
//!    and resumes.
//!
//! MySQL connection: mysql://root:password@localhost:3306/stream_store

use std::collections::HashMap;
use std::time::Duration;

use serial_test::serial;

use bytes::Bytes;
use client::StreamClient;
use common::config::{ExtentNodeConfig, StreamManagerConfig};
use common::types::{Epoch, EpochPolicy, EpochState, Offset, StorageClass, StreamId};
use extent_node::ExtentNode;
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

fn sm_config() -> StreamManagerConfig {
    StreamManagerConfig {
        bind_ip: "127.0.0.1".into(),
        port: 0,
        heartbeat_check_interval_ms: 1000,
        leadership_lease_duration_secs: 3,
        ..StreamManagerConfig::default()
    }
}

fn en_config(sm_addrs: &[String]) -> ExtentNodeConfig {
    ExtentNodeConfig {
        bind_ip: "127.0.0.1".into(),
        port: 0,
        stream_manager_addrs: sm_addrs.to_vec(),
        heartbeat_interval_ms: 2000,
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

/// Start SM + 3 ENs, wait for registration. Returns (SM, SM addr, EN map).
async fn setup_cluster() -> (
    StreamManager,
    String,
    HashMap<String, ExtentNode>,
    MetadataStore,
) {
    let sm_cfg = sm_config();
    let mysql_url = sm_cfg.mysql_url();
    clean_database(&mysql_url).await;

    let store = MetadataStore::connect(&mysql_url)
        .await
        .expect("metadata connect");

    let sm = StreamManager::start(sm_cfg).await;
    let sm_addr = sm.addr().to_string();
    info!("[setup] StreamManager started on {sm_addr}");

    poll_leader(&store, Duration::from_secs(5)).await;

    let sm_addrs = vec![sm_addr.clone()];
    let mut en_map = HashMap::new();
    for i in 0..3 {
        let en = ExtentNode::start(en_config(&sm_addrs)).await;
        let addr = en.addr().to_string();
        info!("[setup] ExtentNode {i} started on {addr}");
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

#[tokio::test]
#[serial]
async fn client_recovers_after_primary_killed() {
    init_tracing();
    let (sm, sm_addr, mut en_map, _store) = setup_cluster().await;

    // Create stream with RF=3. When one node dies, SM should degrade to RF=2
    // (quorum=2 preserved) instead of failing allocation.
    let sm_client = StreamClient::connect(&sm_addr).await.unwrap();
    let (stream_id, _epoch, primary_addr) = sm_client
        .create_stream(
            "test-primary-outage",
            3,
            StorageClass::S3,
            EpochPolicy::default(),
        )
        .await
        .expect("create_stream");
    info!("[test] Stream {stream_id} created with RF=3, primary={primary_addr}");

    // Append some messages before the outage.
    let client = StreamClient::connect(&primary_addr).await.unwrap();
    let appended_before = append_messages(&client, stream_id, Epoch(0), 0, 10).await;
    assert_eq!(
        appended_before, 10,
        "should append 10 messages before outage"
    );
    info!("[test] Appended {appended_before} messages before outage");

    // Kill the primary EN (abrupt crash — no Disconnect sent).
    let primary_en = en_map.remove(&primary_addr).unwrap();
    primary_en.kill();
    info!("[test] Killed primary EN at {primary_addr}");

    // The existing TCP connection may not detect the kill immediately.
    // Keep appending until we get an error — confirms the outage is detected.
    let mut appended_after_kill = 0;
    for i in 10..100 {
        match client
            .append(stream_id, Epoch(0), Bytes::from(format!("msg-{i}")))
            .await
        {
            Ok(_) => appended_after_kill += 1,
            Err(e) => {
                info!(
                    "[test] Append failed after kill ({appended_after_kill} succeeded post-kill): {e}"
                );
                break;
            }
        }
    }
    info!("[test] {appended_after_kill} appends succeeded after kill before error detected");

    // Wait for SM to mark the dead primary via expired heartbeat.
    // This ensures the allocator excludes it from new replica sets.
    // EN heartbeat=2000ms, SM check=1000ms, expiry threshold=1.5x → ~4-5s.
    info!("[test] Waiting for SM to mark dead primary...");
    sleep(Duration::from_secs(6)).await;

    // Client-driven recovery: client observed append error, now seals by epoch on SM.
    // SM seals secondaries (primary is dead), allocates new extent with degraded RF.
    let sm_client2 = StreamClient::connect(&sm_addr).await.unwrap();
    let seal_result = sm_client2.seal(stream_id, Epoch(0)).await;
    info!("[test] seal result: {:?}", seal_result);

    // Discover the new primary via describe_stream.
    let extents = sm_client2.describe_stream(stream_id, 0).await.unwrap();
    info!(
        "[test] describe_stream: {} extents, states: {:?}",
        extents.len(),
        extents
            .iter()
            .map(|e| (e.epoch.0, e.state))
            .collect::<Vec<_>>()
    );
    let active = extents
        .iter()
        .find(|e| e.state == EpochState::Active)
        .expect("should have an active extent after recovery");
    let new_primary = active
        .replicas
        .iter()
        .find(|r| r.role == 0 && r.is_alive)
        .expect("active extent should have a live primary");
    info!(
        "[test] New primary={} for active extent {}, replicas={}",
        new_primary.node_addr,
        active.epoch.0,
        active.replicas.len()
    );
    assert_ne!(
        new_primary.node_addr, primary_addr,
        "new primary should differ from killed node"
    );
    // Verify RF degraded from 3 to 2 (only 2 alive nodes available).
    assert_eq!(
        active.replicas.len(),
        2,
        "new extent should have degraded RF=2 (only 2 alive nodes)"
    );
    assert!(
        active.replicas.iter().all(|r| r.node_addr != primary_addr),
        "new extent should not include the killed node"
    );

    // Resume appending on the new primary.
    // Wait briefly for RegisterEpoch to propagate to the new primary.
    sleep(Duration::from_millis(200)).await;
    let new_client = StreamClient::connect(&new_primary.node_addr).await.unwrap();
    let mut appended_after = 0;
    for i in 100..120 {
        match new_client
            .append(stream_id, Epoch(0), Bytes::from(format!("recovery-{i}")))
            .await
        {
            Ok(_) => {
                appended_after += 1;
            }
            Err(e) => {
                info!("[test] Post-recovery append {i} failed: {e}, retrying...");
                sleep(Duration::from_millis(200)).await;
            }
        }
    }
    info!("[test] Appended {appended_after} messages after recovery");
    assert!(appended_after > 0, "should append messages after recovery");

    // Verify data on surviving replicas.
    // Find the sealed extent that has data (start_offset < end_offset).
    // There may be multiple sealed extents if SM heartbeat checker sealed+allocated
    // before our seal_by_epoch.
    let sealed = extents
        .iter()
        .find(|e| e.state == EpochState::Sealed && e.end_offset > e.start_offset)
        .expect("should have a sealed extent with data");
    let live_replica = sealed
        .replicas
        .iter()
        .find(|r| r.is_alive)
        .expect("should have a live replica for sealed extent");

    let reader = StreamClient::connect(&live_replica.node_addr)
        .await
        .unwrap();
    let messages = reader
        .read(
            stream_id,
            Offset(sealed.start_offset),
            100,
        )
        .await
        .unwrap();
    info!(
        "[test] Read {} messages from sealed extent {} on {}",
        messages.len(),
        sealed.epoch.0,
        live_replica.node_addr,
    );
    assert!(
        messages.len() >= appended_before,
        "sealed extent should contain at least the pre-outage messages"
    );

    info!("[test] Primary outage recovery: PASSED");

    // Cleanup.
    for (_, en) in en_map {
        en.stop().await;
    }
    sm.stop().await;
}

#[tokio::test]
#[serial]
async fn client_recovers_after_secondary_killed() {
    init_tracing();
    let (sm, sm_addr, mut en_map, _store) = setup_cluster().await;

    // Create stream with RF=3. When one node dies, SM should degrade to RF=2
    // (quorum=2 preserved) instead of failing allocation.
    let sm_client = StreamClient::connect(&sm_addr).await.unwrap();
    let (stream_id, _epoch, primary_addr) = sm_client
        .create_stream(
            "test-secondary-outage",
            3,
            StorageClass::S3,
            EpochPolicy::default(),
        )
        .await
        .expect("create_stream");
    info!("[test] Stream {stream_id} created with RF=3, primary={primary_addr}");

    // Append some messages before the outage.
    let client = StreamClient::connect(&primary_addr).await.unwrap();
    let appended_before = append_messages(&client, stream_id, Epoch(0), 0, 10).await;
    assert_eq!(
        appended_before, 10,
        "should append 10 messages before outage"
    );
    info!("[test] Appended {appended_before} messages before outage");

    // Kill one secondary EN.
    let secondary_addr = en_map
        .keys()
        .find(|addr| *addr != &primary_addr)
        .cloned()
        .unwrap();
    let secondary_en = en_map.remove(&secondary_addr).unwrap();
    secondary_en.kill();
    info!("[test] Killed secondary EN at {secondary_addr}");

    // With RF=3 and 1 secondary dead, quorum may still work (depends on
    // required_secondary_acks). Try appending — may succeed if quorum is met
    // with remaining secondary, or fail if quorum requires both secondaries.
    sleep(Duration::from_millis(500)).await;

    // Attempt to append. May fail due to replication timeout.
    let mut appended_during = 0;
    for i in 10..20 {
        match client
            .append(stream_id, Epoch(0), Bytes::from(format!("msg-{i}")))
            .await
        {
            Ok(_) => appended_during += 1,
            Err(e) => {
                info!("[test] Append during secondary outage failed at msg-{i}: {e}");
                break;
            }
        }
    }
    info!("[test] Appended {appended_during} messages during secondary outage");

    // Wait for SM to detect dead secondary.
    info!("[test] Waiting for SM to detect dead secondary...");
    sleep(Duration::from_secs(6)).await;

    // SM should NOT seal when only a secondary dies — primary still has quorum.
    // The extent should remain active with the same epoch.
    let sm_client2 = StreamClient::connect(&sm_addr).await.unwrap();
    let extents = sm_client2.describe_stream(stream_id, 0).await.unwrap();

    assert_eq!(
        extents.len(),
        1,
        "should still have exactly 1 extent (no seal)"
    );
    let active = extents
        .iter()
        .find(|e| e.state == EpochState::Active)
        .expect("extent should still be active");
    info!(
        "[test] After secondary outage: extent {} still active, replicas={}",
        active.epoch.0,
        active.replicas.len()
    );

    // Appending should continue on the same primary without reconnection.
    let appended_after = append_messages(&client, stream_id, Epoch(0), 20, 10).await;
    info!("[test] Appended {appended_after} messages after secondary outage detected");
    assert!(
        appended_after > 0,
        "should continue appending on same primary after secondary outage"
    );

    info!("[test] Secondary outage recovery: PASSED");

    // Cleanup.
    for (_, en) in en_map {
        en.stop().await;
    }
    sm.stop().await;
}
