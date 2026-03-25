//! Integration test for describe_stream / describe_extent with RF=2 broadcast replication.
//!
//! Exercises the management APIs against a StreamManager + 2 broadcast-replication-enabled
//! ExtentNodes with a real MySQL backend.
//!
//! MySQL connection: mysql://root:password@tx.dev:3306/metadata

use std::sync::Arc;

use bytes::Bytes;
use common::config::StreamManagerConfig;
use common::types::{ExtentState, NodeMetrics, Offset};
use tokio::sync::{broadcast, mpsc};

use extent_node::store::{ExtentNodeStore, ForwardRequest};

/// Initialize tracing for tests. Uses try_init() so it's safe when multiple tests
/// run in the same process. Override log level via RUST_LOG env var.
fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .try_init();
}

/// Start a StreamManager server on a random port with RF=2 default.
/// Drops and recreates tables via Refinery for a clean slate.
async fn start_stream_manager_rf2() -> String {
    use std::sync::Arc;
    use stream_manager::allocator::Allocator;
    use stream_manager::metadata::MetadataStore;
    use stream_manager::store::StreamManagerStore;

    let config = StreamManagerConfig {
        default_replication_factor: 2,
        ..StreamManagerConfig::default()
    };

    // Drop old tables so Refinery can manage schema from scratch.
    let pool = sqlx::mysql::MySqlPoolOptions::new()
        .max_connections(1)
        .connect(&config.mysql_url)
        .await
        .expect("failed to connect for cleanup");
    for table in &[
        "extent_replica",
        "extent",
        "stream_sequence",
        "stream",
        "node",
        "refinery_schema_history",
    ] {
        sqlx::query(&format!("DROP TABLE IF EXISTS {table}"))
            .execute(&pool)
            .await
            .unwrap_or_else(|e| panic!("drop {table}: {e}"));
    }
    pool.close().await;

    // Connect and migrate.
    let store = MetadataStore::connect(&config.mysql_url)
        .await
        .expect("failed to connect to MySQL");
    store.migrate().await.expect("failed to migrate");

    let allocator = Arc::new(tokio::sync::Mutex::new(Allocator::new()));
    let stream_manager_store =
        StreamManagerStore::new(store, allocator, config.default_replication_factor);
    let handler = Arc::new(stream_manager_store);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    tokio::spawn(async move {
        server::Server::builder("StreamManager-test")
            .listener(listener)
            .handler(handler)
            .build()
            .run()
            .await;
    });

    addr
}

/// Start a broadcast-replication-enabled ExtentNode server (with deferred ACK support).
/// Returns the listen address.
async fn start_broadcast_extent_node() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let (forward_tx, forward_rx) = mpsc::channel::<ForwardRequest>(4096);
    let (watermark_tx, watermark_rx) = mpsc::channel(4096);

    let store = Arc::new(ExtentNodeStore::with_forward_tx(forward_tx));

    // Shutdown channel (never sent in tests — leak the sender to keep receivers alive).
    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    // Spawn DownstreamManager.
    let downstream_shutdown = shutdown_tx.subscribe();
    tokio::spawn(async move {
        extent_node::downstream::run_downstream_manager(
            forward_rx,
            watermark_tx,
            downstream_shutdown,
        )
        .await;
    });

    // Spawn WatermarkHandler.
    let store_for_wm = Arc::clone(&store);
    let watermark_shutdown = shutdown_tx.subscribe();
    tokio::spawn(async move {
        extent_node::watermark::run_watermark_handler(
            watermark_rx,
            store_for_wm,
            watermark_shutdown,
        )
        .await;
    });

    // Keep shutdown_tx alive so receivers don't see Closed.
    std::mem::forget(shutdown_tx);

    // Accept connections with deferred response support.
    let store_for_accept = Arc::clone(&store);
    tokio::spawn(async move {
        server::Server::builder("ExtentNode-test")
            .listener(listener)
            .handler(store_for_accept)
            .deferred(true)
            .build()
            .run()
            .await;
    });

    addr
}

#[tokio::test(flavor = "multi_thread")]
async fn describe_stream_rf2_integration() {
    init_tracing();
    // ── Start servers ──
    let extent_node_1_addr = start_broadcast_extent_node().await;
    let extent_node_2_addr = start_broadcast_extent_node().await;
    let stream_manager_addr = start_stream_manager_rf2().await;

    let mut stream_manager = client::StorageClient::connect(&stream_manager_addr)
        .await
        .unwrap();

    // Register both ExtentNodes with the StreamManager.
    stream_manager
        .connect_extent_node("extent-node-1", &extent_node_1_addr, 5000)
        .await
        .unwrap();
    stream_manager
        .heartbeat("extent-node-1", &NodeMetrics::default())
        .await
        .unwrap();

    stream_manager
        .connect_extent_node("extent-node-2", &extent_node_2_addr, 5000)
        .await
        .unwrap();
    stream_manager
        .heartbeat("extent-node-2", &NodeMetrics::default())
        .await
        .unwrap();

    // ── Part 1: Create stream with RF=2 ──
    let stream_name = format!(
        "rf2-describe-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    let (stream_id, first_extent_id, primary_addr) =
        stream_manager.create_stream(&stream_name, 2).await.unwrap();

    assert!(stream_id.0 > 0);
    // Primary should be one of the two registered ENs.
    assert!(
        primary_addr == extent_node_1_addr || primary_addr == extent_node_2_addr,
        "primary_addr {primary_addr} should be one of en1={extent_node_1_addr} or en2={extent_node_2_addr}"
    );

    // Give a moment for RegisterExtent to settle on both ENs.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Append messages to the primary.
    let mut en_client = client::StorageClient::connect(&primary_addr).await.unwrap();
    for i in 0u64..5 {
        en_client
            .append(stream_id, Bytes::from(format!("rf2-msg-{i}")))
            .await
            .unwrap();
    }

    // ── Part 2: Describe active extent — verify 2 replicas ──
    let active = stream_manager.describe_stream(stream_id, 1).await.unwrap();
    assert_eq!(active.len(), 1);
    let ext = &active[0];
    assert_eq!(ext.extent_id, first_extent_id.0);
    assert_eq!(ext.state, ExtentState::Active);
    assert_eq!(ext.start_offset, 0);
    // Active extent end_offset equals start_offset in metadata (only updated on seal).
    assert_eq!(ext.end_offset, 0);

    // RF=2: should have exactly 2 replicas.
    assert_eq!(ext.replicas.len(), 2, "RF=2 should have 2 replicas");

    // One Primary (role=0) and one Secondary (role=1).
    let primary_replica = ext.replicas.iter().find(|r| r.role == 0).unwrap();
    let secondary_replica = ext.replicas.iter().find(|r| r.role == 1).unwrap();

    assert_eq!(primary_replica.node_addr, primary_addr);
    assert!(primary_replica.is_alive);
    assert!(secondary_replica.is_alive);

    // Secondary should be the other EN.
    let expected_secondary = if primary_addr == extent_node_1_addr {
        &extent_node_2_addr
    } else {
        &extent_node_1_addr
    };
    assert_eq!(secondary_replica.node_addr, *expected_secondary);

    // ── Part 3: Seal and create new extent, then describe all ──
    let (second_extent_id, new_primary_addr) = stream_manager
        .seal(stream_id, first_extent_id)
        .await
        .unwrap();

    // Append to new extent.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let mut en_client2 = client::StorageClient::connect(&new_primary_addr)
        .await
        .unwrap();
    for i in 0u64..3 {
        en_client2
            .append(stream_id, Bytes::from(format!("rf2-seal-msg-{i}")))
            .await
            .unwrap();
    }

    // Describe all extents.
    let all = stream_manager.describe_stream(stream_id, 0).await.unwrap();
    assert_eq!(all.len(), 2, "should have 2 extents after one seal");

    // Ordered DESC: newest first.
    let new_ext = &all[0];
    let sealed_ext = &all[1];

    assert_eq!(new_ext.extent_id, (second_extent_id as u32));
    assert_eq!(new_ext.state, ExtentState::Active);
    assert_eq!(new_ext.start_offset, 5);
    assert_eq!(
        new_ext.replicas.len(),
        2,
        "new extent should also have RF=2"
    );

    assert_eq!(sealed_ext.extent_id, first_extent_id.0);
    assert_eq!(sealed_ext.state, ExtentState::Sealed);
    assert_eq!(sealed_ext.start_offset, 0);
    assert_eq!(sealed_ext.end_offset, 5);
    assert_eq!(
        sealed_ext.replicas.len(),
        2,
        "sealed extent should retain RF=2 replicas"
    );

    // Both extents should have Primary + Secondary with both alive.
    for ext in &all {
        assert_eq!(ext.replicas.len(), 2);
        let has_primary = ext.replicas.iter().any(|r| r.role == 0);
        let has_secondary = ext.replicas.iter().any(|r| r.role == 1);
        assert!(has_primary, "extent {} should have Primary", ext.extent_id);
        assert!(
            has_secondary,
            "extent {} should have Secondary",
            ext.extent_id
        );
        for r in &ext.replicas {
            assert!(
                r.is_alive,
                "replica {} on extent {} should be alive",
                r.node_addr, ext.extent_id
            );
        }
    }

    // ── Part 4: describe_extent for the sealed extent ──
    let detail = stream_manager
        .describe_extent(stream_id, first_extent_id)
        .await
        .unwrap();
    assert_eq!(detail.extent_id, first_extent_id.0);
    assert_eq!(detail.state, ExtentState::Sealed);
    assert_eq!(detail.end_offset, 5);
    assert_eq!(detail.replicas.len(), 2);

    // ── Part 5: Disconnect one EN, verify is_alive goes false ──
    //
    // Determine which EN is Secondary on the sealed extent so we disconnect it.
    let sealed_secondary = sealed_ext.replicas.iter().find(|r| r.role == 1).unwrap();
    let dead_addr = &sealed_secondary.node_addr;
    let dead_node_id = if *dead_addr == extent_node_1_addr {
        "extent-node-1"
    } else {
        "extent-node-2"
    };

    // Disconnect the node from SM (this marks it Dead in the node table).
    stream_manager
        .disconnect_extent_node(dead_node_id)
        .await
        .unwrap();

    // Now describe again — the disconnected node should show is_alive=false.
    let after_disconnect = stream_manager.describe_stream(stream_id, 0).await.unwrap();
    for ext in &after_disconnect {
        assert_eq!(ext.replicas.len(), 2, "replica count should not change");
        for r in &ext.replicas {
            if r.node_addr == *dead_addr {
                assert!(
                    !r.is_alive,
                    "disconnected node {} should be is_alive=false on extent {}",
                    r.node_addr, ext.extent_id
                );
            } else {
                assert!(
                    r.is_alive,
                    "remaining node {} should be is_alive=true on extent {}",
                    r.node_addr, ext.extent_id
                );
            }
        }
    }

    // Also verify via describe_extent.
    let detail_after = stream_manager
        .describe_extent(stream_id, first_extent_id)
        .await
        .unwrap();
    let dead_replica = detail_after
        .replicas
        .iter()
        .find(|r| r.node_addr == *dead_addr)
        .unwrap();
    assert!(
        !dead_replica.is_alive,
        "dead node should be is_alive=false in describe_extent"
    );
    let alive_replica = detail_after
        .replicas
        .iter()
        .find(|r| r.node_addr != *dead_addr)
        .unwrap();
    assert!(
        alive_replica.is_alive,
        "surviving node should be is_alive=true in describe_extent"
    );

    // ── Part 6: seek with RF=2 ──
    //
    // Stream layout (2 extents):
    //   first_extent:  base=0, count=5, Sealed
    //   second_extent: base=5, count=0, Active
    // One EN is dead.

    // 6a. Seek offset=0 -> sealed extent with 2 replicas.
    let s = stream_manager.seek(stream_id, Offset(0)).await.unwrap();
    assert_eq!(s.extent_id, first_extent_id.0);
    assert_eq!(s.state, ExtentState::Sealed);
    assert_eq!(s.start_offset, 0);
    assert_eq!(s.end_offset, 5);
    assert_eq!(
        s.replicas.len(),
        2,
        "RF=2 seek result should have 2 replicas"
    );
    // Verify roles.
    assert!(
        s.replicas.iter().any(|r| r.role == 0),
        "should have Primary"
    );
    assert!(
        s.replicas.iter().any(|r| r.role == 1),
        "should have Secondary"
    );

    // 6b. Seek offset=3 -> still first sealed extent (mid-range).
    let s = stream_manager.seek(stream_id, Offset(3)).await.unwrap();
    assert_eq!(s.extent_id, first_extent_id.0);

    // 6c. Seek offset=5 -> active extent (at boundary).
    let s = stream_manager.seek(stream_id, Offset(5)).await.unwrap();
    assert_eq!(s.extent_id, (second_extent_id as u32));
    assert_eq!(s.state, ExtentState::Active);
    assert_eq!(s.start_offset, 5);
    assert_eq!(s.replicas.len(), 2);

    // 6d. Seek offset=100 -> active extent (far beyond committed).
    let s = stream_manager.seek(stream_id, Offset(100)).await.unwrap();
    assert_eq!(s.extent_id, (second_extent_id as u32));
    assert_eq!(s.state, ExtentState::Active);

    // 6e. Seek reflects node liveness: the dead node should show is_alive=false.
    let s = stream_manager.seek(stream_id, Offset(0)).await.unwrap();
    let dead_in_seek = s
        .replicas
        .iter()
        .find(|r| r.node_addr == *dead_addr)
        .unwrap();
    assert!(
        !dead_in_seek.is_alive,
        "dead node should be is_alive=false in seek result"
    );
    let alive_in_seek = s
        .replicas
        .iter()
        .find(|r| r.node_addr != *dead_addr)
        .unwrap();
    assert!(
        alive_in_seek.is_alive,
        "surviving node should be is_alive=true in seek result"
    );
}
