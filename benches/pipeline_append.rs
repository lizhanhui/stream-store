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
//! When an extent fills up, each client independently seals via StreamManager — the
//! StreamManager's `seal_and_allocate_transaction` is idempotent: the first seal triggers
//! allocation of a new extent, and subsequent seals for the same extent simply return the
//! already-allocated successor.
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
use common::errors::StorageError;
use common::types::ExtentId;
use extent_node::ExtentNode;
use sqlx::mysql::MySqlPoolOptions;
use stream_manager::StreamManager;
use tokio::sync::{RwLock, Semaphore, mpsc};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

// ── Benchmark Parameters ─────────────────────────────────────────────────────

const BENCH_DURATION: Duration = Duration::from_secs(5);
const NUM_SENDERS: usize = 4;
const PAYLOAD_SIZE: usize = 1024; // 1 KiB
const REPLICATION_FACTOR: u16 = 2;
const ARENA_CAPACITY: usize = 64 * 1024 * 1024; // 64 MiB
const PIPELINE_DEPTH: usize = 16; // max in-flight appends per sender

// ── Main ─────────────────────────────────────────────────────────────────────

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    // ── 1. Clean database ────────────────────────────────────────────────────
    let stream_manager_config = StreamManagerConfig {
        listen_addr: "127.0.0.1:0".into(),
        ..StreamManagerConfig::default()
    };
    clean_database(&stream_manager_config.mysql_url()).await;
    info!("[setup] Database cleaned");

    // ── 2. Start StreamManager ───────────────────────────────────────────────
    let stream_manager = StreamManager::start(stream_manager_config).await;
    let stream_manager_addr = stream_manager.addr().to_string();
    info!("[setup] StreamManager started on {stream_manager_addr}");

    // ── 3. Start 3 ExtentNodes ───────────────────────────────────────────────
    let mut extent_nodes = vec![];
    for i in 0..3 {
        let config = ExtentNodeConfig {
            listen_addr: "127.0.0.1:0".into(),
            stream_manager_addr: stream_manager_addr.clone(),
            extent_arena_capacity: ARENA_CAPACITY,
            ..Default::default()
        };
        let node = ExtentNode::start(config).await;
        info!("[setup] ExtentNode {i} started on {}", node.addr());
        extent_nodes.push(node);
    }

    // ── 4. Wait for heartbeat registration ───────────────────────────────────
    info!("[setup] Waiting for ExtentNode registration...");
    tokio::time::sleep(Duration::from_secs(3)).await;
    info!("[setup] Registration complete");

    // ── 5. Create a single stream via StreamManager ──────────────────────────
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

    // ── 6. Spawn sender tasks ────────────────────────────────────────────────
    let start = Instant::now();

    let mut handles = Vec::with_capacity(NUM_SENDERS);
    for sender_id in 0..NUM_SENDERS {
        let sm_addr = stream_manager_addr.clone();
        let primary_addr = initial_primary_addr.clone();

        handles.push(tokio::spawn(async move {
            sender_task(
                sender_id,
                sm_addr,
                stream_id,
                initial_extent_id,
                primary_addr,
                BENCH_DURATION,
            )
            .await
        }));
    }

    // ── 7. Collect results ───────────────────────────────────────────────────
    let mut total_appends: u64 = 0;
    let mut total_bytes: u64 = 0;
    let mut total_seals: u64 = 0;
    let mut all_latencies: Vec<Duration> = Vec::new();

    for handle in handles {
        match handle.await {
            Ok(result) => {
                total_appends += result.total_appends;
                total_bytes += result.total_bytes;
                total_seals += result.seal_count;
                all_latencies.extend(result.latencies);
            }
            Err(e) => {
                warn!("[error] Sender task panicked: {e}");
            }
        }
    }

    let elapsed = start.elapsed();

    // ── 8. Shutdown ──────────────────────────────────────────────────────────
    for node in extent_nodes {
        node.stop().await;
    }

    // ── 9. Report ────────────────────────────────────────────────────────────
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
    println!("  Total seals:     {total_seals}");
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

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Per-sender result.
struct SenderResult {
    total_appends: u64,
    total_bytes: u64,
    seal_count: u64,
    latencies: Vec<Duration>,
}

/// Shared sender state protected by RwLock for extent rotation.
struct SenderState {
    extent_id: ExtentId,
    en_client: Arc<StorageClient>,
}

/// Single sender task: connect to the current primary, pipeline appends with up to
/// PIPELINE_DEPTH in-flight requests, measure per-append latency.
///
/// On ExtentFull/ExtentSealed, seal via StreamManager and reconnect to the new primary.
/// Uses RwLock to coordinate extent rotation across concurrent in-flight tasks.
async fn sender_task(
    sender_id: usize,
    stream_manager_addr: String,
    stream_id: common::types::StreamId,
    initial_extent_id: ExtentId,
    initial_primary_addr: String,
    duration: Duration,
) -> SenderResult {
    let payload = Bytes::from(vec![0xABu8; PAYLOAD_SIZE]);
    let deadline = Instant::now() + duration;

    // Each sender gets its own StreamManager connection for seal RPCs.
    let sm_client = Arc::new(
        StorageClient::connect(&stream_manager_addr)
            .await
            .unwrap_or_else(|e| panic!("sender {sender_id}: SM connect failed: {e}")),
    );

    // Connect to the initial primary.
    let en_client = Arc::new(
        StorageClient::connect(&initial_primary_addr)
            .await
            .unwrap_or_else(|e| panic!("sender {sender_id}: EN connect failed: {e}")),
    );

    let state = Arc::new(RwLock::new(SenderState {
        extent_id: initial_extent_id,
        en_client,
    }));

    // Semaphore to cap in-flight requests.
    let semaphore = Arc::new(Semaphore::new(PIPELINE_DEPTH));

    // Channel for collecting results from spawned append tasks.
    let (result_tx, mut result_rx) = mpsc::unbounded_channel::<AppendOutcome>();

    // Spawn append tasks until deadline.
    let spawner = {
        let semaphore = Arc::clone(&semaphore);
        let state = Arc::clone(&state);
        let sm_client = Arc::clone(&sm_client);
        let result_tx = result_tx.clone();

        tokio::spawn(async move {
            while Instant::now() < deadline {
                // Acquire semaphore permit to limit pipeline depth.
                let permit = semaphore.clone().acquire_owned().await.unwrap();

                let state = Arc::clone(&state);
                let sm_client = Arc::clone(&sm_client);
                let payload = payload.clone();
                let result_tx = result_tx.clone();

                tokio::spawn(async move {
                    let outcome =
                        do_append(sender_id, stream_id, &state, &sm_client, payload).await;
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
    let mut seal_count: u64 = 0;
    let mut latencies = Vec::with_capacity(65536);

    while let Some(outcome) = result_rx.recv().await {
        match outcome {
            AppendOutcome::Ok(latency) => {
                latencies.push(latency);
                total_appends += 1;
                total_bytes += PAYLOAD_SIZE as u64;
            }
            AppendOutcome::Sealed => {
                seal_count += 1;
            }
            AppendOutcome::Error => {
                // Logged inside do_append; skip.
            }
        }
    }

    spawner.await.ok();

    SenderResult {
        total_appends,
        total_bytes,
        seal_count,
        latencies,
    }
}

enum AppendOutcome {
    Ok(Duration),
    Sealed,
    Error,
}

/// Perform a single pipelined append. On ExtentFull/ExtentSealed, handle rotation.
async fn do_append(
    sender_id: usize,
    stream_id: common::types::StreamId,
    state: &Arc<RwLock<SenderState>>,
    sm_client: &Arc<StorageClient>,
    payload: Bytes,
) -> AppendOutcome {
    // Read lock: get current client + extent_id.
    let (client, extent_id) = {
        let s = state.read().await;
        (Arc::clone(&s.en_client), s.extent_id)
    };

    let t0 = Instant::now();
    match client.append(stream_id, extent_id, payload).await {
        Ok(_) => AppendOutcome::Ok(t0.elapsed()),
        Err(StorageError::ExtentFull(_)) | Err(StorageError::ExtentSealed(_)) => {
            // Need to rotate extent. Acquire write lock.
            let mut s = state.write().await;

            // Check if another task already rotated past this extent.
            if s.extent_id != extent_id {
                // Already rotated — just report as sealed (the append was lost, that's ok for bench).
                return AppendOutcome::Sealed;
            }

            // We're responsible for sealing.
            let (new_extent_id_raw, new_primary_addr) =
                match sm_client.seal(stream_id, extent_id, None).await {
                    Ok(result) => result,
                    Err(e) => {
                        warn!("sender {sender_id}: seal failed: {e}");
                        return AppendOutcome::Error;
                    }
                };

            let new_extent_id = ExtentId(new_extent_id_raw);
            info!(
                "sender {sender_id}: sealed extent {:?} → new {:?} @ {}",
                extent_id, new_extent_id, new_primary_addr
            );

            // Reconnect to the (potentially different) primary.
            let new_client = match StorageClient::connect(&new_primary_addr).await {
                Ok(c) => Arc::new(c),
                Err(e) => {
                    warn!("sender {sender_id}: reconnect after seal failed: {e}");
                    return AppendOutcome::Error;
                }
            };

            s.extent_id = new_extent_id;
            s.en_client = new_client;

            AppendOutcome::Sealed
        }
        Err(e) => {
            warn!("sender {sender_id}: unexpected append error: {e}");
            AppendOutcome::Error
        }
    }
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
