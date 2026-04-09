//! Pipeline Append Benchmark
//!
//! Launches a full cluster (1 StreamManager + 3 ExtentNodes), creates a **single stream**
//! (RF=2), then spawns N concurrent client connections all appending to the same stream.
//! Stats (throughput, latency percentiles) are reported periodically every `REPORT_INTERVAL`.
//!
//! With pipelining enabled, each sender keeps up to `PIPELINE_DEPTH` appends in-flight
//! concurrently on a single connection, dramatically improving throughput.
//!
//! Extent-full transitions are handled **autonomously by the Primary ExtentNode** within
//! the current epoch (epoch-based seal-and-new). The client never sees ExtentSealed errors
//! during normal operation -- the Primary seals the full extent, creates a new one with the
//! next sequential ID (same replica set, same epoch), and retries the triggering append
//! transparently. Stream Manager is notified asynchronously via fire-and-forget
//! NOTIFY_SEALED_EXTENT. Clients just keep appending; extent transitions are invisible.
//!
//! **Prerequisites**: MySQL running at the default StreamManagerConfig URL.
//!
//! Run with:
//! ```sh
//! cargo bench --bench pipeline_append
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use client::StreamClient;
use common::config::{ExtentNodeConfig, StreamManagerConfig};
use common::types::Epoch;
use extent_node::ExtentNode;
use hdrhistogram::Histogram;
use sqlx::mysql::MySqlPoolOptions;
use stream_manager::StreamManager;
use tokio::sync::Semaphore;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

// -- Benchmark Parameters -----------------------------------------------------

const BENCH_DURATION: Duration = Duration::from_secs(120);
const REPORT_INTERVAL: Duration = Duration::from_secs(5);
const NUM_SENDERS: usize = 4;
const PAYLOAD_SIZE: usize = 1024; // 1 KiB
const REPLICATION_FACTOR: u16 = 2;
const EXTENT_CAPACITY: u32 = 64 * 1024 * 1024; // 64 MiB
const PIPELINE_DEPTH: usize = 4; // max in-flight appends per sender
const CACHE_EXTENTS: u32 = 4;

// -- Shared counters ----------------------------------------------------------

struct SharedCounters {
    appends: AtomicU64,
    bytes: AtomicU64,
    errors: AtomicU64,
    /// HDR Histogram for latency recording (microseconds, 3 significant figures).
    histogram: Mutex<Histogram<u64>>,
}

impl SharedCounters {
    fn new() -> Self {
        // Track latencies from 1us to 60s with 3 significant figures.
        let hist = Histogram::new_with_bounds(1, 60_000_000, 3).unwrap();
        Self {
            appends: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            histogram: Mutex::new(hist),
        }
    }

    fn record_success(&self, latency: Duration) {
        self.appends.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(PAYLOAD_SIZE as u64, Ordering::Relaxed);
        let us = latency.as_micros() as u64;
        let _ = self.histogram.lock().unwrap().record(us);
    }

    fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Snapshot and reset counters. Returns (appends, bytes, errors, histogram_snapshot).
    fn snapshot_and_reset(&self) -> (u64, u64, u64, Histogram<u64>) {
        let appends = self.appends.swap(0, Ordering::Relaxed);
        let bytes = self.bytes.swap(0, Ordering::Relaxed);
        let errors = self.errors.swap(0, Ordering::Relaxed);
        let hist = {
            let mut guard = self.histogram.lock().unwrap();
            let snapshot = guard.clone();
            guard.reset();
            snapshot
        };
        (appends, bytes, errors, hist)
    }
}

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

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
            stream_manager_addrs: vec![stream_manager_addr.clone()],
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
    let sm_client = StreamClient::connect(&stream_manager_addr)
        .await
        .expect("connect to StreamManager");
    let (stream_id, initial_extent_id, _epoch, initial_primary_addr) = sm_client
        .create_stream("bench-pipeline", REPLICATION_FACTOR, EXTENT_CAPACITY, CACHE_EXTENTS)
        .await
        .expect("create_stream");
    info!(
        "[setup] Stream {} created: extent={}, primary={}",
        stream_id, initial_extent_id, initial_primary_addr
    );

    // -- 6. Shared counters ---------------------------------------------------
    let counters = Arc::new(SharedCounters::new());

    // -- 7. Spawn sender tasks ------------------------------------------------
    let start = std::time::Instant::now();

    for sender_id in 0..NUM_SENDERS {
        let primary_addr = initial_primary_addr.clone();
        let counters = Arc::clone(&counters);

        tokio::spawn(async move {
            sender_task(sender_id, stream_id, primary_addr, BENCH_DURATION, counters).await;
        });
    }

    // -- 8. Periodic reporter (runs on main) ----------------------------------
    print_header();
    let mut cumulative_appends: u64 = 0;
    let mut cumulative_bytes: u64 = 0;
    let mut cumulative_errors: u64 = 0;
    let mut cumulative_hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
    let mut interval = tokio::time::interval(REPORT_INTERVAL);
    interval.tick().await; // first tick fires immediately — skip it

    let total_intervals = (BENCH_DURATION.as_secs() / REPORT_INTERVAL.as_secs()) + 1;
    for _ in 0..total_intervals {
        interval.tick().await;
        let elapsed = start.elapsed();
        let (appends, bytes, errors, hist) = counters.snapshot_and_reset();
        cumulative_appends += appends;
        cumulative_bytes += bytes;
        cumulative_errors += errors;
        cumulative_hist.add(&hist).ok();
        print_interval(elapsed, REPORT_INTERVAL, appends, bytes, errors, &hist);
    }

    // Final summary.
    let elapsed = start.elapsed();
    let elapsed_secs = elapsed.as_secs_f64();
    eprintln!(
        "───────────────────────────────────────────────────────────────────────────────────────────────────────────────"
    );
    eprintln!(
        "  TOTAL    {:>10.0} ops/s  {:>7.2} MB/s  {} appends  {} errors  |  avg {}  p99 {}  p99.9 {}  max {}",
        cumulative_appends as f64 / elapsed_secs,
        (cumulative_bytes as f64 / (1024.0 * 1024.0)) / elapsed_secs,
        cumulative_appends,
        cumulative_errors,
        format_us(cumulative_hist.mean() as u64),
        format_us(cumulative_hist.value_at_quantile(0.99)),
        format_us(cumulative_hist.value_at_quantile(0.999)),
        format_us(cumulative_hist.max()),
    );
    eprintln!(
        "═══════════════════════════════════════════════════════════════════════════════════════════════════════════════"
    );

    // -- 9. Shutdown ----------------------------------------------------------
    for node in extent_nodes {
        node.stop().await;
    }
}

// -- Reporter -----------------------------------------------------------------

fn print_header() {
    eprintln!();
    eprintln!(
        "═══════════════════════════════════════════════════════════════════════════════════════════════════════════════"
    );
    eprintln!(
        "  Pipeline Append Benchmark  |  senders={NUM_SENDERS}  payload={PAYLOAD_SIZE}B  RF={REPLICATION_FACTOR}  pipeline={PIPELINE_DEPTH}  cache_extents={CACHE_EXTENTS}"
    );
    eprintln!(
        "═══════════════════════════════════════════════════════════════════════════════════════════════════════════════"
    );
    eprintln!(
        "  {:>8}  {:>10}  {:>10}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}",
        "elapsed", "ops/sec", "MB/sec", "appends", "errors", "avg", "p99", "p99.9", "max"
    );
    eprintln!(
        "  {:>8}  {:>10}  {:>10}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}",
        "--------",
        "----------",
        "----------",
        "--------",
        "--------",
        "--------",
        "--------",
        "--------",
        "--------"
    );
}

fn print_interval(
    elapsed: Duration,
    interval_dur: Duration,
    appends: u64,
    bytes: u64,
    errors: u64,
    hist: &Histogram<u64>,
) {
    let secs = interval_dur.as_secs_f64();
    let ops = if secs > 0.0 {
        appends as f64 / secs
    } else {
        0.0
    };
    let mb = if secs > 0.0 {
        (bytes as f64 / (1024.0 * 1024.0)) / secs
    } else {
        0.0
    };

    let (avg, p99, p999, max) = if hist.len() > 0 {
        (
            format_us(hist.mean() as u64),
            format_us(hist.value_at_quantile(0.99)),
            format_us(hist.value_at_quantile(0.999)),
            format_us(hist.max()),
        )
    } else {
        ("-".into(), "-".into(), "-".into(), "-".into())
    };

    eprintln!(
        "  {:>7.1}s  {:>10.0}  {:>7.2} MB  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}",
        elapsed.as_secs_f64(),
        ops,
        mb,
        appends,
        errors,
        avg,
        p99,
        p999,
        max,
    );
}

fn format_us(us: u64) -> String {
    if us == 0 {
        "-".into()
    } else if us < 1000 {
        format!("{us}us")
    } else if us < 1_000_000 {
        format!("{:.1}ms", us as f64 / 1000.0)
    } else {
        format!("{:.2}s", us as f64 / 1_000_000.0)
    }
}

// -- Sender -------------------------------------------------------------------

async fn sender_task(
    sender_id: usize,
    stream_id: common::types::StreamId,
    primary_addr: String,
    duration: Duration,
    counters: Arc<SharedCounters>,
) {
    let payload = Bytes::from(vec![0xABu8; PAYLOAD_SIZE]);
    let deadline = std::time::Instant::now() + duration;

    let en_client = Arc::new(
        StreamClient::connect(&primary_addr)
            .await
            .unwrap_or_else(|e| panic!("sender {sender_id}: EN connect failed: {e}")),
    );

    let semaphore = Arc::new(Semaphore::new(PIPELINE_DEPTH));

    while std::time::Instant::now() < deadline {
        let permit = semaphore.clone().acquire_owned().await.unwrap();

        let en_client = Arc::clone(&en_client);
        let payload = payload.clone();
        let counters = Arc::clone(&counters);

        tokio::spawn(async move {
            let t0 = std::time::Instant::now();
            match en_client.append(stream_id, Epoch(0), payload).await {
                Ok(_) => counters.record_success(t0.elapsed()),
                Err(e) => {
                    warn!("sender {sender_id}: append error: {e}");
                    counters.record_error();
                }
            }
            drop(permit);
        });
    }
}

// -- Helpers ------------------------------------------------------------------

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
