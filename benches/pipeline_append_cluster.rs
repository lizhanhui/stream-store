//! Pipeline Append Benchmark (External Cluster)
//!
//! Connects to an **already-running** cluster (StreamManager at tx.dev:9800),
//! opens a single stream (creating it if absent), then spawns N concurrent client
//! connections all appending to the same stream. Stats (throughput, latency
//! percentiles) are reported periodically every `REPORT_INTERVAL`.
//!
//! With pipelining enabled, each sender keeps up to `PIPELINE_DEPTH` appends in-flight
//! concurrently on a single connection using FuturesUnordered (no task-per-append spawning),
//! dramatically improving throughput while minimizing context switches.
//!
//! Extent-full transitions are handled **autonomously by the Primary ExtentNode** within
//! the current epoch (epoch-based seal-and-new). If a sender observes a recoverable routing
//! error such as `ExtentSealed` or `EpochStale`, it drains the in-flight pipeline, reconnects
//! to the current primary via the Stream Manager, and resumes appending.
//!
//! Connection drops and timeouts are handled with epoch-based seal recovery: the sender
//! seals the stream by epoch on the Stream Manager, which bumps the epoch and allocates a
//! new replica set on healthy nodes. Other senders that observe `EpochStale` from the bump
//! simply refresh their epoch via `describe_stream` without issuing another seal.
//! The benchmark is designed to run indefinitely without getting stuck.
//!
//! **Prerequisites**: A running cluster with StreamManager at tx.dev:9800.
//!
//! Run with:
//! ```sh
//! cargo bench --bench pipeline_append_cluster
//! ```
//!
//! For indefinite running:
//! ```sh
//! BENCH_DURATION_SECS=0 cargo bench --bench pipeline_append_cluster
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use client::StreamClient;
use common::config::{DEFAULT_CACHE_EXTENTS, DEFAULT_EXTENT_GROWTH_FACTOR};
use common::errors::StorageError;
use common::types::{Epoch, StreamId};
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use hdrhistogram::Histogram;
use tokio::time::sleep;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

// -- Benchmark Parameters -----------------------------------------------------

const REPORT_INTERVAL: Duration = Duration::from_secs(5);
const NUM_SENDERS: usize = 4;
const PAYLOAD_SIZE: usize = 1024; // 1 KiB
const REPLICATION_FACTOR: u8 = 2;
const MIN_EXTENT_CAPACITY: u32 = 8 * 1024 * 1024; // 8 MiB
const MAX_EXTENT_CAPACITY: u32 = 256 * 1024 * 1024; // 256 MiB
const PIPELINE_DEPTH: usize = 16; // max in-flight appends per sender

/// Returns the benchmark duration from `BENCH_DURATION_SECS` env var.
/// Default is 120s. Set to `0` for indefinite running.
fn bench_duration() -> Option<Duration> {
    let secs = std::env::var("BENCH_DURATION_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(120);
    if secs == 0 {
        None
    } else {
        Some(Duration::from_secs(secs))
    }
}

// -- Shared counters ----------------------------------------------------------

struct SharedCounters {
    appends: AtomicU64,
    bytes: AtomicU64,
    errors: AtomicU64,
    reconnects: AtomicU64,
    histogram: Mutex<Histogram<u64>>,
}

impl SharedCounters {
    fn new() -> Self {
        let hist = Histogram::new_with_bounds(1, 60_000_000, 3).unwrap();
        Self {
            appends: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            reconnects: AtomicU64::new(0),
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

    fn record_reconnect(&self) {
        self.reconnects.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot_and_reset(&self) -> (u64, u64, u64, u64, Histogram<u64>) {
        let appends = self.appends.swap(0, Ordering::Relaxed);
        let bytes = self.bytes.swap(0, Ordering::Relaxed);
        let errors = self.errors.swap(0, Ordering::Relaxed);
        let reconnects = self.reconnects.swap(0, Ordering::Relaxed);
        let hist = {
            let mut guard = self.histogram.lock().unwrap();
            let snapshot = guard.clone();
            guard.reset();
            snapshot
        };
        (appends, bytes, errors, reconnects, hist)
    }
}

// -- Error classifiers --------------------------------------------------------

/// Returns true if the error indicates the client connection is broken
/// and we need to create a new client (not just retry on the same connection).
fn is_connection_broken(err: &StorageError) -> bool {
    match err {
        StorageError::Internal(msg) => {
            msg.contains("connection closed")
                || msg.contains("connection read error")
                || msg.contains("RPC request timeout")
                || msg.contains("connect timeout")
                || msg.contains("send failed")
        }
        StorageError::Io(_) => true,
        _ => false,
    }
}

/// Returns true if the error indicates an extent/epoch transition
/// requiring re-discovery of the primary.
fn is_routing_error(err: &StorageError) -> bool {
    matches!(
        err,
        StorageError::ExtentSealed(_) | StorageError::EpochStale(_, _)
    )
}

/// Returns true if the error indicates the primary lost the stream
/// (e.g., primary restarted and wiped in-memory state).
fn is_primary_lost_stream(err: &StorageError) -> bool {
    matches!(err, StorageError::UnknownStream(_))
}

/// Returns true if the error requires draining in-flight futures and reconnecting.
fn needs_reconnect(err: &StorageError) -> bool {
    is_connection_broken(err) || is_routing_error(err) || is_primary_lost_stream(err)
}

// -- Main ---------------------------------------------------------------------

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let stream_manager_addr =
        std::env::var("STREAM_MANAGER_ADDR").unwrap_or_else(|_| "tx.dev:9800".to_string());
    let duration = bench_duration();

    // -- 1. Open a single stream via StreamManager ----------------------------
    let stream_manager_client = StreamClient::connect(&stream_manager_addr)
        .await
        .expect("connect to StreamManager");
    let stream_id = stream_manager_client
        .open(
            "bench-pipeline-cluster",
            REPLICATION_FACTOR,
            MIN_EXTENT_CAPACITY,
            MAX_EXTENT_CAPACITY,
            DEFAULT_CACHE_EXTENTS,
            DEFAULT_EXTENT_GROWTH_FACTOR,
            0,
        )
        .await
        .expect("open stream");
    let initial_primary_addr = stream_manager_client
        .cached_primary(stream_id)
        .await
        .expect("primary address cached after open");
    // Discover the current epoch from the active extent so senders can track it.
    let extents = stream_manager_client
        .describe_stream(stream_id, 1)
        .await
        .expect("describe_stream for initial epoch");
    let initial_epoch = extents.first().map(|e| e.epoch).unwrap_or(Epoch(0));
    info!(
        "[setup] Stream {} opened with primary={}, epoch={}",
        stream_id, initial_primary_addr, initial_epoch
    );
    match duration {
        Some(d) => info!("[setup] Running for {}s", d.as_secs()),
        None => info!("[setup] Running indefinitely (BENCH_DURATION_SECS=0)"),
    }

    // -- 2. Shared counters ---------------------------------------------------
    let counters = Arc::new(SharedCounters::new());

    // -- 3. Spawn sender tasks ------------------------------------------------
    let start = Instant::now();

    for sender_id in 0..NUM_SENDERS {
        let primary_addr = initial_primary_addr.clone();
        let stream_manager_addr = stream_manager_addr.clone();
        let counters = Arc::clone(&counters);

        tokio::spawn(async move {
            sender_task(
                sender_id,
                stream_id,
                initial_epoch,
                primary_addr,
                stream_manager_addr,
                duration,
                counters,
            )
            .await;
        });
    }

    // -- 4. Periodic reporter (runs on main) ----------------------------------
    print_header();
    let mut cumulative_appends: u64 = 0;
    let mut cumulative_bytes: u64 = 0;
    let mut cumulative_errors: u64 = 0;
    let mut cumulative_reconnects: u64 = 0;
    let mut cumulative_hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
    let mut interval = tokio::time::interval(REPORT_INTERVAL);
    interval.tick().await; // first tick fires immediately — skip it

    match duration {
        Some(dur) => {
            let total_intervals = (dur.as_secs() / REPORT_INTERVAL.as_secs()) + 1;
            for _ in 0..total_intervals {
                interval.tick().await;
                let elapsed = start.elapsed();
                let (appends, bytes, errors, reconnects, hist) = counters.snapshot_and_reset();
                cumulative_appends += appends;
                cumulative_bytes += bytes;
                cumulative_errors += errors;
                cumulative_reconnects += reconnects;
                cumulative_hist.add(&hist).ok();
                print_interval(
                    elapsed,
                    REPORT_INTERVAL,
                    appends,
                    bytes,
                    errors,
                    reconnects,
                    &hist,
                );
            }
        }
        None =>
        {
            #[allow(unused_assignments)]
            loop {
                interval.tick().await;
                let elapsed = start.elapsed();
                let (appends, bytes, errors, reconnects, hist) = counters.snapshot_and_reset();
                cumulative_appends += appends;
                cumulative_bytes += bytes;
                cumulative_errors += errors;
                cumulative_reconnects += reconnects;
                cumulative_hist.add(&hist).ok();
                print_interval(
                    elapsed,
                    REPORT_INTERVAL,
                    appends,
                    bytes,
                    errors,
                    reconnects,
                    &hist,
                );
            }
        }
    }

    // Final summary.
    let elapsed = start.elapsed();
    let elapsed_secs = elapsed.as_secs_f64();
    eprintln!(
        "───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────"
    );
    eprintln!(
        "  TOTAL    {:>10.0} ops/s  {:>7.2} MB/s  {} appends  {} errors  {} reconn  |  avg {}  p99 {}  p99.9 {}  max {}",
        cumulative_appends as f64 / elapsed_secs,
        (cumulative_bytes as f64 / (1024.0 * 1024.0)) / elapsed_secs,
        cumulative_appends,
        cumulative_errors,
        cumulative_reconnects,
        format_us(cumulative_hist.mean() as u64),
        format_us(cumulative_hist.value_at_quantile(0.99)),
        format_us(cumulative_hist.value_at_quantile(0.999)),
        format_us(cumulative_hist.max()),
    );
    eprintln!(
        "═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════"
    );
}

// -- Reporter -----------------------------------------------------------------

fn print_header() {
    eprintln!();
    eprintln!(
        "═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════"
    );
    eprintln!(
        "  Pipeline Append Benchmark (Cluster)  |  senders={NUM_SENDERS}  payload={PAYLOAD_SIZE}B  RF={REPLICATION_FACTOR}  pipeline={PIPELINE_DEPTH}"
    );
    eprintln!(
        "═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════"
    );
    eprintln!(
        "  {:>8}  {:>10}  {:>10}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}",
        "elapsed", "ops/sec", "MB/sec", "appends", "errors", "reconn", "avg", "p99", "p99.9", "max"
    );
    eprintln!(
        "  {:>8}  {:>10}  {:>10}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}",
        "--------",
        "----------",
        "----------",
        "--------",
        "--------",
        "--------",
        "--------",
        "--------",
        "--------",
        "--------"
    );
}

#[allow(clippy::too_many_arguments)]
fn print_interval(
    elapsed: Duration,
    interval_dur: Duration,
    appends: u64,
    bytes: u64,
    errors: u64,
    reconnects: u64,
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
        "  {:>7.1}s  {:>10.0}  {:>7.2} MB  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}",
        elapsed.as_secs_f64(),
        ops,
        mb,
        appends,
        errors,
        reconnects,
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

// -- Deadline helper ----------------------------------------------------------

fn past_deadline(deadline: Option<Instant>) -> bool {
    deadline.map_or(false, |d| Instant::now() >= d)
}

// -- Sender -------------------------------------------------------------------

async fn sender_task(
    sender_id: usize,
    stream_id: StreamId,
    initial_epoch: Epoch,
    initial_primary_addr: String,
    stream_manager_addr: String,
    duration: Option<Duration>,
    counters: Arc<SharedCounters>,
) {
    let payload = Bytes::from(vec![0xABu8; PAYLOAD_SIZE]);
    let deadline = duration.map(|d| Instant::now() + d);

    let mut primary_addr = initial_primary_addr;
    let mut epoch = initial_epoch;
    let mut backoff = Duration::from_millis(100);
    let max_backoff = Duration::from_secs(5);

    // Outer loop: manages connection lifecycle.
    // Each iteration: connect -> run pipeline -> on error, drain & reconnect.
    'outer: loop {
        if past_deadline(deadline) {
            break;
        }

        // Connect to primary. On failure, rediscover via SM (describe only, no seal).
        let extent_node_client = match StreamClient::connect(&primary_addr).await {
            Ok(c) => Arc::new(c),
            Err(e) => {
                warn!("sender {sender_id}: connect to {primary_addr} failed: {e}");
                counters.record_error();
                sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
                match reconnect_to_primary(&stream_manager_addr, stream_id, deadline).await {
                    Ok((_client, addr, new_epoch)) => {
                        primary_addr = addr;
                        epoch = new_epoch;
                        backoff = Duration::from_millis(100);
                        info!(
                            "sender {sender_id}: rediscovered primary {primary_addr} epoch {epoch}"
                        );
                        counters.record_reconnect();
                        continue 'outer;
                    }
                    Err(_) => continue 'outer,
                }
            }
        };

        backoff = Duration::from_millis(100);

        // Inner loop: pipeline appends on current connection.
        let mut in_flight = FuturesUnordered::new();

        'inner: loop {
            // Fill pipeline up to PIPELINE_DEPTH.
            while in_flight.len() < PIPELINE_DEPTH && !past_deadline(deadline) {
                let client = Arc::clone(&extent_node_client);
                let data = payload.clone();
                let append_epoch = epoch;
                in_flight.push(async move {
                    let started_at = Instant::now();
                    let result = client.append(stream_id, append_epoch, data).await;
                    (started_at, result)
                });
            }

            if in_flight.is_empty() {
                break 'outer; // deadline reached and pipeline fully drained
            }

            let (started_at, result) = in_flight.next().await.unwrap();
            match result {
                Ok(_) => {
                    backoff = Duration::from_millis(100);
                    counters.record_success(started_at.elapsed());
                }
                Err(ref e) if needs_reconnect(e) => {
                    let needs_seal = is_connection_broken(e) || is_primary_lost_stream(e);
                    warn!(
                        "sender {sender_id}: {e} -- draining pipeline and {}",
                        if needs_seal {
                            "sealing epoch to recover"
                        } else {
                            "refreshing epoch"
                        }
                    );
                    counters.record_error();

                    // Drain remaining in-flight futures (all use old client, will fail fast).
                    let mut drained = 0u64;
                    while let Some((_, res)) = in_flight.next().await {
                        if res.is_err() {
                            drained += 1;
                        }
                    }
                    if drained > 0 {
                        counters.errors.fetch_add(drained, Ordering::Relaxed);
                        info!("sender {sender_id}: drained {drained} stale in-flight appends");
                    }
                    counters.record_reconnect();

                    // Connection-broken (RPC timeout, connection closed, send failed):
                    //   Seal by epoch to force SM to bump epoch and allocate new replica set.
                    // Routing error (EpochStale, ExtentSealed):
                    //   Another sender or SM already bumped the epoch. Just describe to refresh.
                    sleep(backoff).await;
                    let reconnect_result = if needs_seal {
                        reconnect_with_seal(&stream_manager_addr, stream_id, epoch, deadline).await
                    } else {
                        reconnect_to_primary(&stream_manager_addr, stream_id, deadline).await
                    };
                    match reconnect_result {
                        Ok((_, addr, new_epoch)) => {
                            primary_addr = addr;
                            epoch = new_epoch;
                            backoff = Duration::from_millis(100);
                            info!(
                                "sender {sender_id}: reconnected to {primary_addr} epoch {epoch}"
                            );
                        }
                        Err(e) => {
                            warn!("sender {sender_id}: reconnect failed: {e}");
                            if past_deadline(deadline) {
                                break 'outer;
                            }
                            backoff = (backoff * 2).min(max_backoff);
                        }
                    }
                    break 'inner;
                }
                Err(e) => {
                    warn!("sender {sender_id}: append error: {e}");
                    counters.record_error();
                }
            }
        }
    }

    info!("sender {sender_id}: shutting down (deadline reached)");
}

// -- Reconnect ----------------------------------------------------------------

/// Reconnect to the active primary with exponential backoff.
/// Retries until success or deadline is reached.
async fn reconnect_to_primary(
    stream_manager_addr: &str,
    stream_id: StreamId,
    deadline: Option<Instant>,
) -> Result<(Arc<StreamClient>, String, Epoch), StorageError> {
    let mut backoff = Duration::from_millis(100);
    let max_backoff = Duration::from_secs(5);

    loop {
        let result = try_reconnect(stream_manager_addr, stream_id).await;
        match result {
            Ok(ok) => return Ok(ok),
            Err(e) => {
                if past_deadline(deadline) {
                    return Err(e);
                }
                warn!("reconnect failed ({e}), retrying in {backoff:?}");
                sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
            }
        }
    }
}

async fn try_reconnect(
    stream_manager_addr: &str,
    stream_id: StreamId,
) -> Result<(Arc<StreamClient>, String, Epoch), StorageError> {
    let stream_manager_client = StreamClient::connect(stream_manager_addr).await?;
    let extents = stream_manager_client.describe_stream(stream_id, 1).await?;
    let primary_addr = stream_manager_client
        .cached_primary(stream_id)
        .await
        .ok_or_else(|| {
            StorageError::Internal(format!(
                "stream {} missing primary after describe_stream",
                stream_id
            ))
        })?;
    let epoch = extents
        .first()
        .map(|e| Epoch(e.epoch.0))
        .unwrap_or(Epoch(0));
    let en_client = Arc::new(StreamClient::connect(&primary_addr).await?);
    Ok((en_client, primary_addr, epoch))
}

/// Reconnect by sealing the current epoch, forcing SM to bump epoch and allocate
/// a new replica set. Falls back to describe_stream if the seal fails (e.g.,
/// another sender or SM failover already bumped the epoch).
async fn try_reconnect_with_seal(
    stream_manager_addr: &str,
    stream_id: StreamId,
    epoch: Epoch,
) -> Result<(Arc<StreamClient>, String, Epoch), StorageError> {
    let sm_client = StreamClient::connect(stream_manager_addr).await?;
    match sm_client.seal(stream_id, epoch).await {
        Ok((new_epoch, primary_addr)) => {
            info!(
                "sealed stream {} epoch {} -> new epoch {}, new primary {}",
                stream_id, epoch, new_epoch, primary_addr
            );
            let en_client = Arc::new(StreamClient::connect(&primary_addr).await?);
            Ok((en_client, primary_addr, new_epoch))
        }
        Err(e) => {
            warn!(
                "seal failed for stream {} epoch {}: {e}, falling back to describe",
                stream_id, epoch
            );
            // Seal failed (epoch already bumped, or other error).
            // Fall back to describe_stream to discover current state.
            drop(sm_client);
            try_reconnect(stream_manager_addr, stream_id).await
        }
    }
}

/// Reconnect by sealing the current epoch with exponential backoff.
/// Retries until success or deadline is reached.
async fn reconnect_with_seal(
    stream_manager_addr: &str,
    stream_id: StreamId,
    epoch: Epoch,
    deadline: Option<Instant>,
) -> Result<(Arc<StreamClient>, String, Epoch), StorageError> {
    let mut backoff = Duration::from_millis(100);
    let max_backoff = Duration::from_secs(5);

    loop {
        let result = try_reconnect_with_seal(stream_manager_addr, stream_id, epoch).await;
        match result {
            Ok(ok) => return Ok(ok),
            Err(e) => {
                if past_deadline(deadline) {
                    return Err(e);
                }
                warn!("reconnect-with-seal failed ({e}), retrying in {backoff:?}");
                sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
            }
        }
    }
}
