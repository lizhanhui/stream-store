//! Multi-SM leadership failover integration test.
//!
//! Verifies that two StreamManagers can compete for a single leadership lease,
//! that the follower takes over when the leader stops, and that the new leader
//! handles ExtentNode failures correctly (seal + re-allocate).
//!
//! MySQL connection: mysql://root:password@localhost:3306/stream_store

use std::time::Duration;

use serial_test::serial;

use bytes::Bytes;
use client::StreamClient;
use common::config::{ExtentNodeConfig, StreamManagerConfig};
use common::types::{Epoch, ExtentPolicy, ExtentState, Offset, StorageClass};
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

/// Drop all tables and re-migrate for a clean slate.
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
        sqlx::query(sqlx::AssertSqlSafe(format!("DROP TABLE IF EXISTS {table}")))
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

/// Build a StreamManager config with fast timings for testing.
///
/// Note: EN heartbeat_interval_ms must be >= 2000 because MySQL DATETIME has
/// only second precision — the expired-node check uses
/// `heartbeat_interval_ms * 1.5 / 1000` as a SECOND interval, so sub-second
/// values round down and cause false expirations.
fn sm_config() -> StreamManagerConfig {
    StreamManagerConfig {
        bind_ip: "127.0.0.1".into(),
        port: 0,
        heartbeat_check_interval_ms: 1000,
        leadership_lease_duration_secs: 3,
        ..StreamManagerConfig::default()
    }
}

/// Build an ExtentNode config with fast timings for testing.
fn en_config(sm_addrs: &[String]) -> ExtentNodeConfig {
    ExtentNodeConfig {
        bind_ip: "127.0.0.1".into(),
        port: 0,
        stream_manager_addrs: sm_addrs.to_vec(),
        heartbeat_interval_ms: 2000,
        ..Default::default()
    }
}

/// Poll MetadataStore until a leader is elected (lease active). Returns the leader node_id.
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

/// Poll MetadataStore until a specific node becomes the leader.
async fn poll_leader_is(store: &MetadataStore, expected: &str, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Ok(Some(leader)) = store.get_leader().await
            && leader == expected
        {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for {expected} to become leader");
        }
        sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn multi_sm_leadership_failover() {
    init_tracing();

    let config = sm_config();
    let mysql_url = config.mysql_url();
    clean_database(&mysql_url).await;

    // Open an independent MetadataStore connection for assertions.
    let meta = MetadataStore::connect(&mysql_url)
        .await
        .expect("connect metadata for assertions");

    // ── Phase 1: Start SM-1, verify it acquires leadership ──────────────

    info!("Phase 1: starting SM-1");
    let sm1 = StreamManager::start(sm_config()).await;
    let sm1_addr = sm1.addr().to_string();

    let leader = poll_leader(&meta, Duration::from_secs(5)).await;
    assert_eq!(leader, sm1_addr, "SM-1 should be the initial leader");
    info!("Phase 1: SM-1 is leader at {sm1_addr}");

    // ── Phase 2: Start SM-2, verify SM-1 still leads ───────────────────

    info!("Phase 2: starting SM-2");
    let sm2 = StreamManager::start(sm_config()).await;
    let sm2_addr = sm2.addr().to_string();

    // Give SM-2 time to attempt acquisition and fail.
    sleep(Duration::from_secs(2)).await;
    let leader = poll_leader(&meta, Duration::from_secs(2)).await;
    assert_eq!(
        leader, sm1_addr,
        "SM-1 should still be leader after SM-2 starts"
    );
    info!("Phase 2: SM-1 still leads, SM-2 is follower at {sm2_addr}");

    // ── Phase 3: Start 3 ExtentNodes (connected to both SMs) ───────────

    info!("Phase 3: starting 3 ExtentNodes");
    let both_sm_addrs = vec![sm1_addr.clone(), sm2_addr.clone()];
    let en1 = ExtentNode::start(en_config(&both_sm_addrs)).await;
    let en2 = ExtentNode::start(en_config(&both_sm_addrs)).await;
    let en3 = ExtentNode::start(en_config(&both_sm_addrs)).await;
    info!(
        "Phase 3: ENs started at {}, {}, {}",
        en1.addr(),
        en2.addr(),
        en3.addr()
    );

    // Collect ENs into a map so we can kill by address later.
    let mut en_map: std::collections::HashMap<String, ExtentNode> =
        std::collections::HashMap::new();
    en_map.insert(en1.addr().to_string(), en1);
    en_map.insert(en2.addr().to_string(), en2);
    en_map.insert(en3.addr().to_string(), en3);

    // Wait for EN heartbeats to register with SM and become "alive".
    // EN heartbeat interval is 2s; wait long enough for at least one heartbeat
    // round plus the SM heartbeat check to see them.
    sleep(Duration::from_secs(4)).await;

    // ── Phase 4: Create stream and append data ──────────────────────────

    info!("Phase 4: creating stream and appending data");
    let sm_client = StreamClient::connect(&sm1_addr)
        .await
        .expect("connect to SM-1");
    let (stream_id, _extent_id, epoch, primary_addr) = sm_client
        .create_stream(
            "failover-test",
            2,
            StorageClass::S3,
            ExtentPolicy {
                min_capacity: 8 * 1024 * 1024,
                max_capacity: 256 * 1024 * 1024,
                cache: 4,
                scale_factor: 0,
            },
        )
        .await
        .expect("create_stream");
    info!("Phase 4: stream={stream_id}, primary={primary_addr}");

    // Connect to the Primary EN and append messages.
    let en_client = StreamClient::connect(&primary_addr)
        .await
        .expect("connect to primary EN");
    let num_messages = 10u64;
    for i in 0..num_messages {
        en_client
            .append(stream_id, epoch, Bytes::from(format!("msg-{i}")))
            .await
            .unwrap_or_else(|e| panic!("append msg-{i} failed: {e}"));
    }
    info!("Phase 4: appended {num_messages} messages");

    // ── Phase 5: Stop SM-1 (leader), verify SM-2 takes over ────────────

    info!("Phase 5: stopping SM-1 (leader)");
    sm1.stop().await;

    // SM-1 releases lease on graceful stop; SM-2 should pick it up quickly.
    poll_leader_is(&meta, &sm2_addr, Duration::from_secs(5)).await;
    info!("Phase 5: SM-2 is now leader");

    // ── Phase 6: Kill the Primary EN, verify SM-2 handles failover ────

    // Kill the primary EN abruptly (no Disconnect frame) so SM-2 detects it
    // via expired heartbeat and triggers seal + re-allocate.
    info!("Phase 6: killing primary EN at {primary_addr}");
    let primary_en = en_map
        .remove(&primary_addr)
        .expect("primary EN not found in map");
    primary_en.kill();

    // Wait for SM-2 to detect dead node and run failover.
    // EN heartbeat=2000ms, expiry threshold=3s (1.5×), SM check=1000ms.
    // Wait for SM to mark the dead EN, then trigger client-driven seal.
    sleep(Duration::from_secs(6)).await;

    // Client-driven seal: seal the stream at epoch 0.
    let sm2_client = StreamClient::connect(&sm2_addr)
        .await
        .expect("connect to SM-2");
    let seal_result = sm2_client.seal(stream_id, Epoch(0)).await;
    info!("Phase 6: seal result: {:?}", seal_result);

    let extents = sm2_client
        .describe_stream(stream_id, 100)
        .await
        .expect("describe_stream after failover");

    info!(
        "Phase 6: stream has {} extent(s) after failover",
        extents.len()
    );
    for ext in &extents {
        info!(
            "  extent={} state={:?} start={} end={} epoch={:?} replicas={}",
            ext.extent_id,
            ext.state,
            ext.start_offset,
            ext.end_offset,
            ext.epoch,
            ext.replicas.len(),
        );
    }

    // Verify: at least one sealed extent and one active extent.
    let sealed_count = extents
        .iter()
        .filter(|e| e.state == ExtentState::Sealed)
        .count();
    let active_count = extents
        .iter()
        .filter(|e| e.state == ExtentState::Active)
        .count();
    assert!(
        sealed_count >= 1,
        "expected at least 1 sealed extent after failover, got {sealed_count}"
    );
    assert!(
        active_count >= 1,
        "expected at least 1 active extent after failover, got {active_count}"
    );

    // ── Phase 7: Verify data integrity ──────────────────────────────────

    info!("Phase 7: verifying data integrity");
    // The sealed extent should still be readable from surviving replicas.
    let sealed_extent = extents
        .iter()
        .find(|e| e.state == ExtentState::Sealed)
        .expect("no sealed extent found");
    // Find a live replica for the sealed extent.
    let live_replica = sealed_extent
        .replicas
        .iter()
        .find(|r| r.is_alive)
        .expect("no live replica for sealed extent");

    let reader = StreamClient::connect(&live_replica.node_addr)
        .await
        .expect("connect to live replica for read");
    let messages = reader
        .read(
            stream_id,
            common::types::ExtentId(sealed_extent.extent_id),
            Offset(sealed_extent.start_offset),
            num_messages as u16,
        )
        .await
        .expect("read messages from sealed extent");
    assert_eq!(
        messages.len(),
        num_messages as usize,
        "should read back all {num_messages} messages"
    );
    for (i, msg) in messages.iter().enumerate() {
        assert_eq!(
            msg.as_ref(),
            format!("msg-{i}").as_bytes(),
            "message {i} content mismatch"
        );
    }
    info!("Phase 7: all {num_messages} messages verified");

    // ── Cleanup ─────────────────────────────────────────────────────────

    info!("Cleanup: stopping remaining nodes");
    sm2.stop().await;
    for (_addr, en) in en_map {
        en.stop().await;
    }
    info!("Test complete");
}
