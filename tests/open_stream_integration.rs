//! Integration test for StreamClient.open().
//!
//! Exercises the open() method (create-if-absent, describe-if-exists) and
//! the primary address cache against a StreamManager + ExtentNode with MySQL.
//!
//! MySQL connection: mysql://root:password@tx.dev:3306/metadata

use bytes::Bytes;
use client::StreamClient;
use common::config::StreamManagerConfig;
use common::types::{Epoch, ExtentState, NodeMetrics};

/// Initialize tracing for tests.
fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .try_init();
}

/// Start a StreamManager server on a random port with RF=1.
async fn start_stream_manager_server() -> String {
    use std::sync::Arc;
    use stream_manager::metadata::MetadataStore;
    use stream_manager::store::StreamManagerStore;

    let config = StreamManagerConfig {
        default_replication_factor: 1,
        ..StreamManagerConfig::default()
    };

    let pool = sqlx::mysql::MySqlPoolOptions::new()
        .max_connections(1)
        .connect(&config.mysql_url())
        .await
        .expect("failed to connect for cleanup");
    for table in &[
        "extent_replica",
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

    let store = MetadataStore::connect(&config.mysql_url())
        .await
        .expect("failed to connect to MySQL");
    store.migrate().await.expect("failed to migrate");

    let stream_manager_store = StreamManagerStore::new(store, config.default_replication_factor);
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

/// Start an ExtentNode server on a random port (RF=1, no broadcast).
async fn start_extent_node_server() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let store = std::sync::Arc::new(extent_node::store::ExtentNodeStore::new());

    tokio::spawn(async move {
        server::Server::builder("ExtentNode-test")
            .listener(listener)
            .handler(store)
            .build()
            .run()
            .await;
    });

    addr
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_client_open_integration() {
    init_tracing();

    let extent_node_addr = start_extent_node_server().await;
    let stream_manager_addr = start_stream_manager_server().await;

    let sm = StreamClient::connect(&stream_manager_addr).await.unwrap();

    // Register ExtentNode with StreamManager.
    sm.connect_extent_node("en-1", &extent_node_addr, 5000)
        .await
        .unwrap();
    sm.heartbeat("en-1", &NodeMetrics::default()).await.unwrap();

    // ── Part 1: open() creates stream when absent ──
    let stream_id = sm.open("test-stream", 1).await.unwrap();
    assert!(stream_id.0 > 0, "stream_id should be non-zero");

    // Primary address should be cached.
    let primary_addr = sm.cached_primary(stream_id).await;
    assert!(primary_addr.is_some(), "primary address should be cached after open");
    assert_eq!(
        primary_addr.as_deref().unwrap(),
        &extent_node_addr,
        "cached primary should match the registered ExtentNode"
    );

    // ── Part 2: open() returns same stream when it already exists ──
    let stream_id_2 = sm.open("test-stream", 1).await.unwrap();
    assert_eq!(stream_id, stream_id_2, "open() should return same StreamId for existing stream");

    // Primary cache should still be populated.
    let primary_addr_2 = sm.cached_primary(stream_id_2).await;
    assert_eq!(primary_addr, primary_addr_2);

    // ── Part 3: open() a second distinct stream ──
    let stream_id_b = sm.open("another-stream", 1).await.unwrap();
    assert_ne!(stream_id, stream_id_b, "different names should yield different StreamIds");
    assert!(sm.cached_primary(stream_id_b).await.is_some());

    // ── Part 4: data plane works via cached primary ──
    let en = StreamClient::connect(primary_addr.as_deref().unwrap())
        .await
        .unwrap();

    // Append a few messages.
    for i in 0u64..5 {
        let result = en
            .append(stream_id, Epoch(0), Bytes::from(format!("msg-{i}")))
            .await
            .unwrap();
        assert_eq!(result.offset.0, i);
    }

    // Verify describe_stream shows the active extent.
    let extents = sm.describe_stream(stream_id, 0).await.unwrap();
    assert_eq!(extents.len(), 1);
    assert_eq!(extents[0].state, ExtentState::Active);

    // ── Part 5: describe_stream_by_name returns correct data ──
    let (resolved_id, extents) = sm.describe_stream_by_name("test-stream", 0).await.unwrap();
    assert_eq!(resolved_id, stream_id);
    assert_eq!(extents.len(), 1);

    // Unknown stream name should return UnknownStream error.
    let result = sm.describe_stream_by_name("nonexistent", 0).await;
    assert!(result.is_err());
    assert!(
        matches!(result, Err(common::errors::StorageError::UnknownStream(_))),
        "expected UnknownStream error for nonexistent stream"
    );
}
