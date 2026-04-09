//! Concurrent multi-stream append benchmark.
//!
//! Launches a full cluster (1 StreamManager + 3 ExtentNodes), then spawns 10 client tasks
//! that each create their own stream (RF=2) and append 1 KiB records for 5 seconds.
//!
//! Extent-full transitions are handled autonomously by the Primary ExtentNode within the
//! current epoch -- the client never sees ExtentSealed errors. The Primary seals the full
//! extent, creates a new one (same replica set, same epoch), and retries the append
//! transparently. Clients just keep appending.
//!
//! Reports aggregate throughput: ops/sec and MB/sec.
//!
//! **Prerequisites**: MySQL running at the default StreamManagerConfig URL.
//!
//! Run with:
//! ```sh
//! cargo bench --bench concurrent_multi_stream
//! ```

use fastant::Instant;
use std::time::Duration;

use bytes::Bytes;
use client::StreamClient;
use common::config::{ExtentNodeConfig, StreamManagerConfig};
use common::types::Epoch;
use extent_node::ExtentNode;
use sqlx::mysql::MySqlPoolOptions;
use stream_manager::StreamManager;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

const NUM_CLIENTS: usize = 10;
const PAYLOAD_SIZE: usize = 1024; // 1 KiB
const BENCH_DURATION: Duration = Duration::from_secs(5);
const REPLICATION_FACTOR: u16 = 2;
const EXTENT_CAPACITY: usize = 4 * 1024 * 1024; // 4 MiB -- triggers frequent seals

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    // -- 1. Clean database --
    let stream_manager_config = StreamManagerConfig {
        bind_ip: "127.0.0.1".into(),
        port: 0,
        ..StreamManagerConfig::default()
    };
    clean_database(&stream_manager_config.mysql_url()).await;
    info!("[setup] Database cleaned");

    // -- 2. Start StreamManager --
    let stream_manager_addr = StreamManager::start(stream_manager_config).await;
    let stream_manager_addr_socket = stream_manager_addr.addr();
    info!("[setup] StreamManager started on {stream_manager_addr_socket}");

    // -- 3. Start 3 ExtentNodes --
    let mut extent_nodes = vec![];
    for i in 0..3 {
        let extent_node_config = ExtentNodeConfig {
            bind_ip: "127.0.0.1".into(),
            port: 0,
            stream_manager_addrs: vec![stream_manager_addr_socket.to_string()],
            ..Default::default()
        };
        let extent_node = ExtentNode::start(extent_node_config).await;

        info!("[setup] ExtentNode {i} started on {}", extent_node.addr());
        extent_nodes.push(extent_node);
    }

    // -- 4. Wait for heartbeat registration --
    info!("[setup] Waiting for ExtentNode registration...");
    tokio::time::sleep(Duration::from_secs(3)).await;
    info!("[setup] Registration complete");

    // -- 5. Spawn client tasks --
    let stream_manager_addr_str = stream_manager_addr_socket.to_string();
    let start = Instant::now();

    let mut handles = Vec::with_capacity(NUM_CLIENTS);
    for client_id in 0..NUM_CLIENTS {
        let addr = stream_manager_addr_str.clone();
        handles.push(tokio::spawn(async move {
            client_task(client_id, addr, BENCH_DURATION).await
        }));
    }

    // -- 6. Collect results --
    let mut total_appends: u64 = 0;
    let mut total_bytes: u64 = 0;
    let mut total_errors: u64 = 0;

    for handle in handles {
        match handle.await {
            Ok(result) => {
                total_appends += result.total_appends;
                total_bytes += result.total_bytes;
                total_errors += result.error_count;
            }
            Err(e) => {
                warn!("[error] Client task panicked: {e}");
            }
        }
    }

    let elapsed = start.elapsed().as_secs_f64();

    for extent_node in extent_nodes.into_iter() {
        extent_node.stop().await;
    }

    // -- 7. Report --
    info!("=== Benchmark Results ===");
    info!("Duration:      {elapsed:.2} seconds");
    info!("Clients:       {NUM_CLIENTS}");
    info!("Payload size:  {PAYLOAD_SIZE} bytes");
    info!("Extent capacity: {} MiB", EXTENT_CAPACITY / (1024 * 1024));
    info!("RF:            {REPLICATION_FACTOR}");
    info!("Total appends: {total_appends}");
    info!(
        "Total bytes:   {total_bytes} ({:.2} MB)",
        total_bytes as f64 / 1_000_000.0
    );
    info!(
        "Throughput:    {:.0} ops/sec",
        total_appends as f64 / elapsed
    );
    info!(
        "Throughput:    {:.2} MB/sec",
        (total_bytes as f64 / 1_000_000.0) / elapsed
    );
    info!("Errors:        {total_errors}");
}

/// Drop all tables for a clean slate (same pattern as client-example).
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
}

/// Per-client result.
struct ClientResult {
    total_appends: u64,
    total_bytes: u64,
    error_count: u64,
}

/// Single client task: create stream, append in a tight loop.
///
/// Extent-full transitions are transparent -- the Primary handles seal-and-new
/// autonomously within the current epoch. The client just keeps appending with
/// the same stream_id; the server accepts appends on whatever the
/// current active extent is.
async fn client_task(
    client_id: usize,
    stream_manager_addr: String,
    duration: Duration,
) -> ClientResult {
    let payload = Bytes::from(vec![0xABu8; PAYLOAD_SIZE]);
    let deadline = Instant::now() + duration;
    let mut total_appends: u64 = 0;
    let mut total_bytes: u64 = 0;
    let mut error_count: u64 = 0;

    // Connect to StreamManager and create stream.
    let stream_manager_client = StreamClient::connect(&stream_manager_addr)
        .await
        .unwrap_or_else(|e| panic!("client {client_id}: StreamManager connect failed: {e}"));

    let (stream_id, _extent_id, _epoch, initial_primary_addr) = stream_manager_client
        .create_stream(&format!("bench-stream-{client_id}"), REPLICATION_FACTOR, 0, 0)
        .await
        .unwrap_or_else(|e| panic!("client {client_id}: create_stream failed: {e}"));

    // Connect to primary ExtentNode.
    let extent_node_client = StreamClient::connect(&initial_primary_addr)
        .await
        .unwrap_or_else(|e| panic!("client {client_id}: ExtentNode connect failed: {e}"));

    // Append loop -- extent-full is handled transparently by the Primary.
    while Instant::now() < deadline {
        match extent_node_client
            .append(stream_id, Epoch(0), payload.clone())
            .await
        {
            Ok(_) => {
                total_appends += 1;
                total_bytes += PAYLOAD_SIZE as u64;
            }
            Err(e) => {
                warn!("client {client_id}: append error: {e}");
                error_count += 1;
            }
        }
    }

    ClientResult {
        total_appends,
        total_bytes,
        error_count,
    }
}
