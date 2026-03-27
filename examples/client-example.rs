//! End-to-end client example demonstrating the full stream lifecycle with real architecture.
//!
//! This example starts a StreamManager and multiple ExtentNodes as separate background tasks,
//! then walks through the complete stream lifecycle:
//!
//! 1. Start StreamManager (with MySQL metadata + heartbeat checker)
//! 2. Start 3 ExtentNodes that auto-register with StreamManager via heartbeat
//! 3. Create a stream with replication_factor=2 (1 Primary + 1 Secondary)
//! 4. Append records to the primary ExtentNode
//! 5. Read records back
//! 6. Seal extent via StreamManager (concurrent quorum-based seal)
//! 7. Append and read on the new extent after seal
//!
//! **Prerequisites**: MySQL running at `mysql://root:password@tx.dev:3306/metadata`
//! (configurable via `StreamManagerConfig::default()`).
//!
//! Run with:
//! ```sh
//! cargo run --example client-example
//! ```

use std::time::Duration;

use bytes::Bytes;
use client::StorageClient;
use common::config::{ExtentNodeConfig, StreamManagerConfig};
use common::types::{ExtentId, Offset};
use sqlx::mysql::MySqlPoolOptions;
use tracing::info;
use tracing_subscriber::EnvFilter;

/// Drop all tables for a clean slate (development phase: always start from scratch).
async fn clean_database(mysql_url: &str) {
    let pool = MySqlPoolOptions::new()
        .max_connections(1)
        .connect(mysql_url)
        .await
        .expect("failed to connect to MySQL for cleanup");
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
}

#[tokio::main]
async fn main() {
    // Initialize tracing. Default level is `info` so internal component logs are visible.
    // Override via RUST_LOG env var, e.g. RUST_LOG=debug or RUST_LOG=stream_manager=debug,extent_node=trace.
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    info!("=== Stream Store Client Example ===");

    // ── 1. Clean database ──
    let stream_manager_config = StreamManagerConfig {
        listen_addr: "127.0.0.1:0".into(),
        ..StreamManagerConfig::default()
    };
    clean_database(&stream_manager_config.mysql_url).await;
    info!("[1] Database cleaned");

    // ── 2. Start StreamManager with port 0 (OS-assigned) ──
    let stream_manager = stream_manager::StreamManager::start(stream_manager_config).await;
    let stream_manager_addr = stream_manager.addr();
    info!("[2] StreamManager started on {stream_manager_addr}");

    // ── 3. Start 3 ExtentNodes with port 0, pointing to StreamManager ──
    let mut extent_nodes = Vec::new();
    for i in 0..3 {
        let extent_node_config = ExtentNodeConfig {
            listen_addr: "127.0.0.1:0".into(),
            stream_manager_addr: stream_manager_addr.to_string(),
            ..Default::default()
        };
        let node = extent_node::ExtentNode::start(extent_node_config).await;
        info!("    ExtentNode {} started on {}", i + 1, node.addr());
        extent_nodes.push(node);
    }

    // Wait for ExtentNodes to connect and register via heartbeat.
    // The Connect happens immediately on startup; we just need to give
    // the background tasks time to complete the TCP handshake + Connect.
    info!("[3] Waiting for ExtentNodes to register with StreamManager...");
    tokio::time::sleep(Duration::from_secs(3)).await;
    info!("    Registration complete");

    // ── 5. Create stream with replication_factor=2 ──
    let mut stream_manager_client = StorageClient::connect(&stream_manager_addr.to_string())
        .await
        .expect("failed to connect to StreamManager");

    let (stream_id, extent_id, primary_addr) = stream_manager_client
        .create_stream("example-stream", 2)
        .await
        .expect("failed to create stream");

    info!(
        "[4] Created stream \"example-stream\" (replication_factor=2): stream_id={:?}, extent_id={:?}, primary={primary_addr}",
        stream_id, extent_id
    );

    // ── 6. Append records to the primary ExtentNode ──
    let mut extent_node_client = StorageClient::connect(&primary_addr)
        .await
        .expect("failed to connect to primary ExtentNode");

    let messages: Vec<String> = (0..5)
        .map(|i| format!("Hello, Stream Store #{i}"))
        .collect();

    info!("[5] Appending {} messages to stream...", messages.len());
    for msg in &messages {
        let offset = extent_node_client
            .append(stream_id, Bytes::from(msg.clone()))
            .await
            .expect("append failed");
        info!("    Appended \"{}\" at offset {}", msg, offset.offset.0);
    }

    // ── 7. Query max offset ──
    let max_offset = extent_node_client
        .query_offset(stream_id)
        .await
        .expect("query_offset failed");
    info!("[6] Max offset on ExtentNode: {}", max_offset.0);

    // ── 8. Read all messages back ──
    let read_messages = extent_node_client
        .read(stream_id, Offset(0), messages.len() as u16)
        .await
        .expect("read failed");

    info!("[7] Read {} messages from Offset(0):", read_messages.len());
    for (i, msg) in read_messages.iter().enumerate() {
        info!("    [{i}] {}", String::from_utf8_lossy(msg));
    }

    // Verify contents match.
    assert_eq!(read_messages.len(), messages.len());
    for (original, read) in messages.iter().zip(read_messages.iter()) {
        assert_eq!(original.as_bytes(), &read[..]);
    }
    info!("    (verified: all messages match)");

    // ── 9. Seal extent via StreamManager (client seal — StreamManager queries ExtentNodes for offset) ──
    let (new_extent_id_raw, new_primary_addr) = stream_manager_client
        .seal(stream_id, extent_id, None)
        .await
        .expect("seal failed");
    let sealed_count = messages.len() as u32;

    info!(
        "[8] Sealed extent {:?} (messages={sealed_count})",
        extent_id
    );
    info!("    New extent_id={new_extent_id_raw}, primary={new_primary_addr}");

    // ── 10. Append more records to the new extent ──
    let mut extent_node_client_2 = StorageClient::connect(&new_primary_addr)
        .await
        .expect("failed to connect to ExtentNode for new extent");

    let new_messages: Vec<String> = (0..3).map(|i| format!("After seal #{i}")).collect();

    info!(
        "[9] Appending {} messages to new extent...",
        new_messages.len()
    );
    for msg in &new_messages {
        let offset = extent_node_client_2
            .append(stream_id, Bytes::from(msg.clone()))
            .await
            .expect("append after seal failed");
        info!("    Appended \"{}\" at offset {}", msg, offset.offset.0);
    }

    // ── 11. Read from new extent ──
    let start_offset = sealed_count as u64; // new extent starts after sealed messages
    let read_new = extent_node_client_2
        .read(stream_id, Offset(start_offset), new_messages.len() as u16)
        .await
        .expect("read after seal failed");

    info!(
        "[10] Read {} messages from Offset({start_offset}):",
        read_new.len()
    );
    for (i, msg) in read_new.iter().enumerate() {
        info!(
            "    [{}] {}",
            start_offset as usize + i,
            String::from_utf8_lossy(msg)
        );
    }

    // Verify.
    assert_eq!(read_new.len(), new_messages.len());
    for (original, read) in new_messages.iter().zip(read_new.iter()) {
        assert_eq!(original.as_bytes(), &read[..]);
    }
    info!("    (verified: all messages match)");

    // ── 12. Seal the second extent ──
    let new_extent_id = ExtentId(new_extent_id_raw);
    let (final_extent_id, final_addr) = stream_manager_client
        .seal(stream_id, new_extent_id, None)
        .await
        .expect("second seal failed");

    // Query total offset from StreamManager.
    let total_offset = stream_manager_client
        .query_offset(stream_id)
        .await
        .expect("query_offset on StreamManager failed");

    info!(
        "[11] Sealed extent {:?} -> new extent_id={final_extent_id}, primary={final_addr}",
        new_extent_id
    );
    info!(
        "     Total stream offset (from StreamManager): {}",
        total_offset.0
    );

    assert_eq!(
        total_offset,
        Offset(sealed_count as u64 + new_messages.len() as u64)
    );

    info!(
        "=== Success: created stream (replication_factor=2), appended {}, sealed twice, verified reads ===",
        sealed_count as u64 + new_messages.len() as u64
    );

    // ── 13. Graceful shutdown ──
    info!("[12] Shutting down...");
    for node in extent_nodes {
        node.stop().await;
    }
    stream_manager.stop().await;
    info!("     All components stopped");
}
