//! Pipeline Append Benchmark (External Cluster)
//!
//! Connects to an **already-running** cluster (StreamManager at tx.dev:9800),
//! creates a single stream (RF=2), then spawns N concurrent client connections all
//! appending to the same stream. Stats (throughput, latency percentiles) are reported
//! periodically every `REPORT_INTERVAL`.
//!
//! With pipelining enabled, each sender keeps up to `PIPELINE_DEPTH` appends in-flight
//! concurrently on a single connection using FuturesUnordered (no task-per-append spawning),
//! dramatically improving throughput while minimizing context switches.
//!
//! Extent-full transitions are handled **autonomously by the Primary ExtentNode** within
//! the current epoch (epoch-based seal-and-new). The client never sees ExtentSealed errors
//! during normal operation -- the Primary seals the full extent, creates a new one with the
//! next sequential ID (same replica set, same epoch), and retries the triggering append
//! transparently. Stream Manager is notified asynchronously via fire-and-forget
//! NOTIFY_SEALED_EXTENT. Clients just keep appending; extent transitions are invisible.
//!
//! **Prerequisites**: A running cluster with StreamManager at tx.dev:9800.
//!
//! Run with:
//! ```sh
//! cargo bench --bench pipeline_append_cluster
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use client::StreamClient;
use common::config::DEFAULT_CACHE_EXTENTS;
use common::types::Epoch;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use hdrhistogram::Histogram;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

// -- Benchmark Parameters -----------------------------------------------------

const BENCH_DURATION: Duration = Duration::from_secs(120);
const REPORT_INTERVAL: Duration = Duration::from_secs(5);
const NUM_SENDERS: usize = 4;
const PAYLOAD_SIZE: usize = 1024; // 1 KiB
const REPLICATION_FACTOR: u16 = 2;
const EXTENT_CAPACITY: u32 = 64 * 1024 * 1024; // 64 MiB
const PIPELINE_DEPTH: usize = 16; // max in-flight appends per sender

// -- Shared counters ----------------------------------------------------------

struct SharedCounters {
    appends: AtomicU64,
    bytes: AtomicU64,
    errors: AtomicU64,
    histogram: Mutex<Histogram<u64>>,
}

impl SharedCounters {
    fn new() -> Self {
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

// -- Main ---------------------------------------------------------------------

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let stream_manager_addr = "tx.dev:9800".to_string();

    // -- 1. Create a single stream via StreamManager --------------------------
    let stream_manager_client = StreamClient::connect(&stream_manager_addr)
        .await
        .expect("connect to StreamManager");
    let (stream_id, initial_extent_id, _epoch, initial_primary_addr) = stream_manager_client
        .create_stream(
            "bench-pipeline-cluster",
            REPLICATION_FACTOR,
            EXTENT_CAPACITY,
            DEFAULT_CACHE_EXTENTS,
        )
        .await
        .expect("create_stream");
    info!(
        "[setup] Stream {} created: extent={}, primary={}",
        stream_id, initial_extent_id, initial_primary_addr
    );

    // -- 2. Shared counters ---------------------------------------------------
    let counters = Arc::new(SharedCounters::new());

    // -- 3. Spawn sender tasks ------------------------------------------------
    let start = std::time::Instant::now();

    for sender_id in 0..NUM_SENDERS {
        let primary_addr = initial_primary_addr.clone();
        let counters = Arc::clone(&counters);

        tokio::spawn(async move {
            sender_task(sender_id, stream_id, primary_addr, BENCH_DURATION, counters).await;
        });
    }

    // -- 4. Periodic reporter (runs on main) ----------------------------------
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
}

// -- Reporter -----------------------------------------------------------------

fn print_header() {
    eprintln!();
    eprintln!(
        "═══════════════════════════════════════════════════════════════════════════════════════════════════════════════"
    );
    eprintln!(
        "  Pipeline Append Benchmark (Cluster)  |  senders={NUM_SENDERS}  payload={PAYLOAD_SIZE}B  RF={REPLICATION_FACTOR}  pipeline={PIPELINE_DEPTH}"
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

    // Pipeline up to PIPELINE_DEPTH appends on this single task using
    // FuturesUnordered — no tokio::spawn per append, which eliminates
    // task scheduling overhead and reduces context switches.
    let mut in_flight = FuturesUnordered::new();

    loop {
        // Fill the pipeline up to PIPELINE_DEPTH while deadline hasn't passed.
        while in_flight.len() < PIPELINE_DEPTH && std::time::Instant::now() < deadline {
            let client = Arc::clone(&en_client);
            let data = payload.clone();
            let ctrs = Arc::clone(&counters);
            in_flight.push(async move {
                let t0 = std::time::Instant::now();
                match client.append(stream_id, Epoch(0), data).await {
                    Ok(_) => ctrs.record_success(t0.elapsed()),
                    Err(e) => {
                        warn!("sender {sender_id}: append error: {e}");
                        ctrs.record_error();
                    }
                }
            });
        }

        if in_flight.is_empty() {
            break;
        }

        // Wait for any one in-flight append to complete, then loop back
        // to refill the pipeline.
        in_flight.next().await;
    }
}
