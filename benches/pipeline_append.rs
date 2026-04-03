//! Pipeline Append Benchmark
//!
//! Launches a full cluster (1 StreamManager + 3 ExtentNodes), creates a **single stream**
//! (RF=2), then spawns N concurrent client connections all appending to the same stream.
//! Each client measures per-append latency; the harness aggregates throughput and
//! latency percentiles (p50/p99/max).
//!
//! With pipelining enabled, each sender keeps up to `PIPELINE_DEPTH` appends in-flight
//! concurrently on a single connection, dramatically improving throughput.
//!
//! Extent-full transitions are handled **autonomously by the Primary ExtentNode** within
//! the current epoch (epoch-based seal-and-new). The client never sees ExtentSealed errors
//! during normal operation -- the Primary seals the full extent, creates a new one with the
//! next sequential ID (same replica set, same epoch), and retries the triggering append
//! transparently. Stream Manager is notified asynchronously via fire-and-forget
//! EXTENT_SEALED_NOTIFY. Clients just keep appending; extent transitions are invisible.
//!
//! **Prerequisites**: MySQL running at the default StreamManagerConfig URL.
//!
//! Run with:
//! ```sh
//! cargo bench --bench pipeline_append
//! ```

use fastant::Instant;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use client::StorageClient;
use common::config::{ExtentNodeConfig, StreamManagerConfig};
use common::types::Epoch;
use extent_node::ExtentNode;
use sqlx::mysql::MySqlPoolOptions;
use stream_manager::StreamManager;
use tokio::sync::{Semaphore, mpsc};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

// -- Benchmark Parameters -----------------------------------------------------

const BENCH_DURATION: Duration = Duration::from_secs(5);
const NUM_SENDERS: usize = 4;
const PAYLOAD_SIZE: usize = 1024; // 1 KiB
const REPLICATION_FACTOR: u16 = 2;
const ARENA_CAPACITY: usize = 64 * 1024 * 1024; // 64 MiB
const PIPELINE_DEPTH: usize = 16; // max in-flight appends per sender

// -- Main ---------------------------------------------------------------------

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    // -- 1. Clean database ----------------------------------------------------
    let stream_manager_config = StreamManagerConfig {
        bind_ip: "127.0.0.1".into(),
        port: 0,
        ..StreamManagerConfig::default()
    };
    clean_database(&stream_manager_config.mysql_url()).await;
    info!("[setup] Database cleaned");

    // -- 2. Start StreamManager -----------------------------------------------
    let stream_manager = StreamManager::start(stream_manager_config).await;
    let stream_manager_addr = stream_manager.addr().to_string();
    info!("[setup] StreamManager started on {stream_manager_addr}");

    // -- 3. Start 3 ExtentNodes -----------------------------------------------
    let mut extent_nodes = vec![];
    for i in 0..3 {
        let config = ExtentNodeConfig {
            bind_ip: "127.0.0.1".into(),
            port: 0,
            stream_manager_addr: stream_manager_addr.clone(),
            extent_arena_capacity: ARENA_CAPACITY,
            ..Default::default()
        };
        let node = ExtentNode::start(config).await;
        info!("[setup] ExtentNode {i} started on {}", node.addr());
        extent_nodes.push(node);
    }

    // -- 4. Wait for heartbeat registration -----------------------------------
    info!("[setup] Waiting for ExtentNode registration...");
    tokio::time::sleep(Duration::from_secs(3)).await;
    info!("[setup] Registration complete");

    // -- 5. Create a single stream via StreamManager --------------------------
    let sm_client = StorageClient::connect(&stream_manager_addr)
        .await
        .expect("connect to StreamManager");
    let (stream_id, initial_extent_id, initial_primary_addr) = sm_client
        .create_stream("bench-pipeline", REPLICATION_FACTOR)
        .await
        .expect("create_stream");
    info!(
        "[setup] Stream {:?} created: extent={:?}, primary={}",
        stream_id, initial_extent_id, initial_primary_addr
    );

    // -- 6. Spawn sender tasks ------------------------------------------------
    let start = Instant::now();

    let mut handles = Vec::with_capacity(NUM_SENDERS);
    for sender_id in 0..NUM_SENDERS {
        let primary_addr = initial_primary_addr.clone();

        handles.push(tokio::spawn(async move {
            sender_task(
                sender_id,
                stream_id,
                primary_addr,
                BENCH_DURATION,
            )
            .await
        }));
    }

    // -- 7. Collect results ---------------------------------------------------
    let mut total_appends: u64 = 0;
    let mut total_bytes: u64 = 0;
    let mut total_errors: u64 = 0;
    let mut all_latencies: Vec<Duration> = Vec::new();

    for handle in handles {
        match handle.await {
            Ok(result) => {
                total_appends += result.total_appends;
                total_bytes += result.total_bytes;
                total_errors += result.error_count;
                all_latencies.extend(result.latencies);
            }
            Err(e) => {
                warn!("[error] Sender task panicked: {e}");
            }
        }
    }

    let elapsed = start.elapsed();

    // -- 8. Shutdown ----------------------------------------------------------
    for node in extent_nodes {
        node.stop().await;
    }

    // -- 9. Report ------------------------------------------------------------
    all_latencies.sort();

    let elapsed_secs = elapsed.as_secs_f64();
    let ops_per_sec = total_appends as f64 / elapsed_secs;
    let mb_per_sec = (total_bytes as f64 / (1024.0 * 1024.0)) / elapsed_secs;

    println!();
    println!("═══════════════════════════════════════════════════════════════");
    println!("  Pipeline Append Benchmark Results");
    println!("═══════════════════════════════════════════════════════════════");
    println!("  Duration:        {elapsed_secs:.2}s");
    println!("  Senders:         {NUM_SENDERS} (single stream)");
    println!("  Payload size:    {PAYLOAD_SIZE} bytes");
    println!("  Arena capacity:  {} MiB", ARENA_CAPACITY / (1024 * 1024));
    println!("  RF:              {REPLICATION_FACTOR}");
    println!("  Pipeline depth:  {PIPELINE_DEPTH}");
    println!("───────────────────────────────────────────────────────────────");
    println!("  Total appends:   {total_appends}");
    println!(
        "  Total bytes:     {total_bytes} ({:.2} MB)",
        total_bytes as f64 / 1_000_000.0
    );
    println!("  Throughput:      {ops_per_sec:.0} ops/sec");
    println!("  Throughput:      {mb_per_sec:.2} MB/sec");
    println!("  Errors:          {total_errors}");
    println!("───────────────────────────────────────────────────────────────");

    if !all_latencies.is_empty() {
        let p50 = all_latencies[all_latencies.len() / 2];
        let p99 = all_latencies[(all_latencies.len() as f64 * 0.99) as usize];
        let max = *all_latencies.last().unwrap();
        println!("  Latency p50:     {p50:?}");
        println!("  Latency p99:     {p99:?}");
        println!("  Latency max:     {max:?}");
    } else {
        println!("  Latency:         (no completed appends)");
    }

    println!("═══════════════════════════════════════════════════════════════");
    println!();
}

// -- Helpers ------------------------------------------------------------------

/// Per-sender result.
struct SenderResult {
    total_appends: u64,
    total_bytes: u64,
    error_count: u64,
    latencies: Vec<Duration>,
}

/// Single sender task: connect to the primary, pipeline appends with up to
/// PIPELINE_DEPTH in-flight requests, measure per-append latency.
///
/// Extent-full transitions are transparent -- the Primary handles seal-and-new
/// autonomously within the current epoch. The client just keeps appending with
/// the same stream_id; the server accepts appends on whatever the
/// current active extent is.
async fn sender_task(
    sender_id: usize,
    stream_id: common::types::StreamId,
    primary_addr: String,
    duration: Duration,
) -> SenderResult {
    let payload = Bytes::from(vec![0xABu8; PAYLOAD_SIZE]);
    let deadline = Instant::now() + duration;

    // Connect to the primary ExtentNode.
    let en_client = Arc::new(
        StorageClient::connect(&primary_addr)
            .await
            .unwrap_or_else(|e| panic!("sender {sender_id}: EN connect failed: {e}")),
    );

    // Semaphore to cap in-flight requests.
    let semaphore = Arc::new(Semaphore::new(PIPELINE_DEPTH));

    // Channel for collecting results from spawned append tasks.
    let (result_tx, mut result_rx) = mpsc::unbounded_channel::<AppendOutcome>();

    // Spawn append tasks until deadline.
    let spawner = {
        let semaphore = Arc::clone(&semaphore);
        let en_client = Arc::clone(&en_client);
        let result_tx = result_tx.clone();

        tokio::spawn(async move {
            while Instant::now() < deadline {
                // Acquire semaphore permit to limit pipeline depth.
                let permit = semaphore.clone().acquire_owned().await.unwrap();

                let en_client = Arc::clone(&en_client);
                let payload = payload.clone();
                let result_tx = result_tx.clone();

                tokio::spawn(async move {
                    let t0 = Instant::now();
                    let outcome = match en_client.append(stream_id, Epoch(0), payload).await {
                        Ok(_) => AppendOutcome::Ok(t0.elapsed()),
                        Err(e) => {
                            warn!("sender {sender_id}: append error: {e}");
                            AppendOutcome::Error
                        }
                    };
                    let _ = result_tx.send(outcome);
                    drop(permit);
                });
            }
        })
    };

    // Drop our copy so the channel closes when spawner + all tasks finish.
    drop(result_tx);

    // Collect results.
    let mut total_appends: u64 = 0;
    let mut total_bytes: u64 = 0;
    let mut error_count: u64 = 0;
    let mut latencies = Vec::with_capacity(65536);

    while let Some(outcome) = result_rx.recv().await {
        match outcome {
            AppendOutcome::Ok(latency) => {
                latencies.push(latency);
                total_appends += 1;
                total_bytes += PAYLOAD_SIZE as u64;
            }
            AppendOutcome::Error => {
                error_count += 1;
            }
        }
    }

    spawner.await.ok();

    SenderResult {
        total_appends,
        total_bytes,
        error_count,
        latencies,
    }
}

enum AppendOutcome {
    Ok(Duration),
    Error,
}

/// Drop all tables for a clean slate.
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
