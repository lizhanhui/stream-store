//! Phase 2 integration tests requiring a MySQL instance.
//!
//! MySQL connection: mysql://root:password@tx.dev:3306/metadata
//!
//! All StreamManager tests run in a single test function to avoid parallel race conditions
//! (shared StreamManager server + MySQL state).

use bytes::Bytes;
use common::config::StreamManagerConfig;
use common::types::{ExtentId, ExtentState, NodeMetrics, Offset};

/// Initialize tracing for tests. Uses try_init() so it's safe when multiple tests
/// run in the same process. Override log level via RUST_LOG env var.
fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .try_init();
}

/// Start a StreamManager server on a random port with real MySQL.
/// Drops and recreates tables via Refinery for a clean slate.
async fn start_stream_manager_server() -> String {
    use std::sync::Arc;
    use stream_manager::allocator::Allocator;
    use stream_manager::metadata::MetadataStore;
    use stream_manager::store::StreamManagerStore;

    let config = StreamManagerConfig {
        default_replication_factor: 1, // single ExtentNode for this test
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

/// Start an ExtentNode server on a random port.
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

/// Single test function that exercises all StreamManager operations against a shared
/// StreamManager server to avoid parallel table-drop / migration races.
#[tokio::test(flavor = "multi_thread")]
async fn stream_manager_integration() {
    init_tracing();
    // ── Start servers ──
    let extent_node_addr = start_extent_node_server().await;
    let stream_manager_addr = start_stream_manager_server().await;

    // ── Part 1: Connect, heartbeat, disconnect (node lifecycle) ──
    {
        let mut c = client::StorageClient::connect(&stream_manager_addr)
            .await
            .unwrap();

        // Register an ExtentNode node.
        c.connect_extent_node("extent-node-test-1", "127.0.0.1:9801", 5000)
            .await
            .unwrap();

        // Send heartbeat.
        c.heartbeat("extent-node-test-1", &NodeMetrics::default())
            .await
            .unwrap();

        // Disconnect.
        c.disconnect_extent_node("extent-node-test-1")
            .await
            .unwrap();
    }

    // ── Part 2: Create stream, append, seal, query offset ──
    {
        let mut stream_manager_client = client::StorageClient::connect(&stream_manager_addr)
            .await
            .unwrap();

        // Register the ExtentNode with StreamManager.
        stream_manager_client
            .connect_extent_node("extent-node-1", &extent_node_addr, 5000)
            .await
            .unwrap();

        // CreateStream via StreamManager.
        let stream_name = format!(
            "test-stream-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );

        let (stream_id, extent_id, returned_addr) = stream_manager_client
            .create_stream(&stream_name, 1)
            .await
            .unwrap();

        assert!(stream_id.0 > 0);
        assert_eq!(returned_addr, extent_node_addr); // only one ExtentNode registered

        // Append to ExtentNode directly using the SM-assigned stream_id.
        // The EN knows about this stream_id because SM sent RegisterExtent.
        let mut en_client = client::StorageClient::connect(&extent_node_addr)
            .await
            .unwrap();

        for i in 0u64..5 {
            en_client
                .append(stream_id, Bytes::from(format!("msg-{i}")))
                .await
                .unwrap();
        }

        // QueryOffset on StreamManager (before seal).
        let offset = stream_manager_client.query_offset(stream_id).await.unwrap();
        assert_eq!(offset, Offset(0)); // active extent has 0 in metadata

        // Seal via StreamManager with explicit extent_id.
        let (new_extent_id, new_addr) = stream_manager_client
            .seal(stream_id, extent_id, None)
            .await
            .unwrap();
        assert!(new_extent_id > 0);
        assert_eq!(new_addr, extent_node_addr); // only one ExtentNode, seal-and-new goes to same node

        // QueryOffset after seal.
        let offset = stream_manager_client.query_offset(stream_id).await.unwrap();
        assert_eq!(offset, Offset(5)); // sealed extent had 5 messages, new extent at offset 5

        // Append more to the new extent, then seal again.
        // Need a fresh EN client since the stream was sealed and new extent registered.
        let mut en_client2 = client::StorageClient::connect(&extent_node_addr)
            .await
            .unwrap();
        for i in 0u64..10 {
            en_client2
                .append(stream_id, Bytes::from(format!("msg2-{i}")))
                .await
                .unwrap();
        }

        let new_eid = ExtentId(new_extent_id);
        let (third_extent_id, _) = stream_manager_client
            .seal(stream_id, new_eid, None)
            .await
            .unwrap();
        let offset = stream_manager_client.query_offset(stream_id).await.unwrap();
        assert_eq!(offset, Offset(15)); // 5 + 10 = 15

        // ── Part 3: describe_stream and describe_extent ──
        //
        // At this point the stream has 3 extents:
        //   extent_id=1  start_offset=0   end_offset=5    state=Sealed
        //   extent_id=2  start_offset=5   end_offset=15   state=Sealed
        //   extent_id=3  start_offset=15  end_offset=15   state=Active

        // 3a. describe_stream(count=0) — all extents, latest first.
        let all_extents = stream_manager_client
            .describe_stream(stream_id, 0)
            .await
            .unwrap();
        assert_eq!(all_extents.len(), 3);
        // Ordered by extent_id descending: 3, 2, 1.
        assert_eq!(all_extents[0].extent_id, (third_extent_id));
        assert_eq!(all_extents[0].state, ExtentState::Active);
        assert_eq!(all_extents[0].start_offset, 15);
        assert_eq!(all_extents[0].end_offset, 15);

        assert_eq!(all_extents[1].extent_id, (new_extent_id));
        assert_eq!(all_extents[1].state, ExtentState::Sealed);
        assert_eq!(all_extents[1].start_offset, 5);
        assert_eq!(all_extents[1].end_offset, 15);

        assert_eq!(all_extents[2].extent_id, extent_id.0);
        assert_eq!(all_extents[2].state, ExtentState::Sealed);
        assert_eq!(all_extents[2].start_offset, 0);
        assert_eq!(all_extents[2].end_offset, 5);

        // 3b. Each extent should have exactly 1 replica (RF=1) — the registered ExtentNode.
        for ext in &all_extents {
            assert_eq!(
                ext.replicas.len(),
                1,
                "RF=1 should have 1 replica per extent"
            );
            assert_eq!(ext.replicas[0].node_addr, extent_node_addr);
            assert_eq!(ext.replicas[0].role, 0); // Primary
            assert!(ext.replicas[0].is_alive, "node should be alive");
        }

        // 3c. describe_stream(count=1) — latest (active) extent only.
        let latest = stream_manager_client
            .describe_stream(stream_id, 1)
            .await
            .unwrap();
        assert_eq!(latest.len(), 1);
        assert_eq!(latest[0].extent_id, (third_extent_id));
        assert_eq!(latest[0].state, ExtentState::Active);

        // 3d. describe_stream(count=2) — latest 2 extents.
        let top2 = stream_manager_client
            .describe_stream(stream_id, 2)
            .await
            .unwrap();
        assert_eq!(top2.len(), 2);
        assert_eq!(top2[0].extent_id, (third_extent_id));
        assert_eq!(top2[1].extent_id, (new_extent_id));

        // 3e. describe_extent — inspect a specific sealed extent.
        let ext1 = stream_manager_client
            .describe_extent(stream_id, extent_id)
            .await
            .unwrap();
        assert_eq!(ext1.extent_id, extent_id.0);
        assert_eq!(ext1.state, ExtentState::Sealed);
        assert_eq!(ext1.start_offset, 0);
        assert_eq!(ext1.end_offset, 5);
        assert_eq!(ext1.replicas.len(), 1);
        assert_eq!(ext1.replicas[0].node_addr, extent_node_addr);
        assert!(ext1.replicas[0].is_alive);

        // 3f. describe_extent — inspect the active extent.
        let ext3 = stream_manager_client
            .describe_extent(stream_id, ExtentId(third_extent_id))
            .await
            .unwrap();
        assert_eq!(ext3.extent_id, (third_extent_id));
        assert_eq!(ext3.state, ExtentState::Active);
        assert_eq!(ext3.start_offset, 15);
        assert_eq!(ext3.end_offset, 15);

        // 3g. describe_extent for non-existent extent returns error.
        let bad = stream_manager_client
            .describe_extent(stream_id, ExtentId(9999u32))
            .await;
        assert!(bad.is_err(), "non-existent extent should return error");

        // 3h. describe_stream for stream with no extents returns empty.
        // Use a never-created stream_id.
        let empty = stream_manager_client
            .describe_stream(common::types::StreamId(99999), 0)
            .await
            .unwrap();
        assert!(empty.is_empty());

        // ── Part 4: seek ──
        //
        // Stream layout (3 extents):
        //   extent_id=first   start_offset=0   end_offset=5    Sealed   [offsets 0..4]
        //   extent_id=second  start_offset=5   end_offset=15   Sealed   [offsets 5..14]
        //   extent_id=third   start_offset=15  end_offset=15   Active   [offsets 15..)
        let first_eid = extent_id.0;
        let second_eid = new_extent_id;
        let third_eid = third_extent_id;

        // 4a. Seek offset=0 -> first sealed extent (start of first extent).
        let s = stream_manager_client
            .seek(stream_id, Offset(0))
            .await
            .unwrap();
        assert_eq!(s.extent_id, first_eid);
        assert_eq!(s.state, ExtentState::Sealed);
        assert_eq!(s.start_offset, 0);
        assert_eq!(s.end_offset, 5);
        assert_eq!(s.replicas.len(), 1);
        assert_eq!(s.replicas[0].node_addr, extent_node_addr);

        // 4b. Seek offset=3 -> first sealed extent (mid-range).
        let s = stream_manager_client
            .seek(stream_id, Offset(3))
            .await
            .unwrap();
        assert_eq!(s.extent_id, first_eid);

        // 4c. Seek offset=4 -> first sealed extent (last message).
        let s = stream_manager_client
            .seek(stream_id, Offset(4))
            .await
            .unwrap();
        assert_eq!(s.extent_id, first_eid);

        // 4d. Seek offset=5 -> second sealed extent (boundary = start of second).
        let s = stream_manager_client
            .seek(stream_id, Offset(5))
            .await
            .unwrap();
        assert_eq!(s.extent_id, (second_eid));
        assert_eq!(s.state, ExtentState::Sealed);
        assert_eq!(s.start_offset, 5);
        assert_eq!(s.end_offset, 15);

        // 4e. Seek offset=12 -> second sealed extent (mid-range).
        let s = stream_manager_client
            .seek(stream_id, Offset(12))
            .await
            .unwrap();
        assert_eq!(s.extent_id, (second_eid));

        // 4f. Seek offset=14 -> second sealed extent (last message).
        let s = stream_manager_client
            .seek(stream_id, Offset(14))
            .await
            .unwrap();
        assert_eq!(s.extent_id, (second_eid));

        // 4g. Seek offset=15 -> active (mutable) extent (boundary = start of active).
        let s = stream_manager_client
            .seek(stream_id, Offset(15))
            .await
            .unwrap();
        assert_eq!(s.extent_id, (third_eid as u32));
        assert_eq!(s.state, ExtentState::Active);
        assert_eq!(s.start_offset, 15);

        // 4h. Seek offset=999 -> active extent (far beyond committed data, still the tail).
        let s = stream_manager_client
            .seek(stream_id, Offset(999))
            .await
            .unwrap();
        assert_eq!(s.extent_id, (third_eid as u32));
        assert_eq!(s.state, ExtentState::Active);
    }
}
