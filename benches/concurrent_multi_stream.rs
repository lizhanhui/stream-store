//! Concurrent Multi-Stream Append Benchmark.
//!
//! Launches a full cluster (1 StreamManager + 3 ExtentNodes), creates N streams,
//! then spawns M writers per stream (total M×N writer tasks) all appending 1 KiB
//! records with pipelined I/O for a configurable duration.
//!
//! Reports aggregate throughput (ops/sec, MB/sec) and latency percentiles
//! (avg, p99, p99.9, max) every REPORT_INTERVAL seconds.
//!
//! Extent-full transitions are handled autonomously by the Primary ExtentNode
//! within the current epoch. Connection drops and epoch bumps trigger pipeline
//! drain + reconnect via the Stream Manager.
//!
//! **Prerequisites**: MySQL running at the default StreamManagerConfig URL.
//!
//! Run with:
//! ```sh
//! cargo bench --bench concurrent_multi_stream
//! ```
//!
//! Custom topology:
//! ```sh
//! BENCH_STREAMS=8 BENCH_WRITERS=2 BENCH_DURATION_SECS=30 cargo bench --bench concurrent_multi_stream
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use client::StreamClient;
use common::config::{ExtentNodeConfig, StreamManagerConfig};
use common::errors::{InternalSnafu, StorageError};
use common::types::{Epoch, StorageClass, StreamId};
use extent_node::ExtentNode;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use hdrhistogram::Histogram;
use sqlx::mysql::MySqlPoolOptions;
use stream_manager::StreamManager;
use tokio::time::sleep;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

// -- Benchmark Parameters -----------------------------------------------------

const PAYLOAD_SIZE: usize = 1024; // 1 KiB
const REPORT_INTERVAL: Duration = Duration::from_secs(5);
const REPLICATION_FACTOR: u8 = 2;
const MIN_EXTENT_CAPACITY: u32 = 8 * 1024 * 1024; // 8 MiB
const MAX_EXTENT_CAPACITY: u32 = 256 * 1024 * 1024; // 256 MiB
const CACHE_EXTENTS: u16 = 4;
const EXTENT_GROWTH_FACTOR: u8 = 8;
const PIPELINE_DEPTH: usize = 16;

fn num_streams() -> usize {
    std::env::var("BENCH_STREAMS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4)
}

fn writers_per_stream() -> usize {
    std::env::var("BENCH_WRITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4)
}

fn bench_duration() -> Duration {
    let secs = std::env::var("BENCH_DURATION_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(10);
    Duration::from_secs(secs)
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

fn is_connection_broken(err: &StorageError) -> bool {
    match err {
        StorageError::Internal { message: msg, .. } => {
            msg.contains("connection closed")
                || msg.contains("connection read error")
                || msg.contains("RPC request timeout")
                || msg.contains("connect timeout")
                || msg.contains("send failed")
        }
        StorageError::Io { .. } => true,
        _ => false,
    }
}

fn is_routing_error(err: &StorageError) -> bool {
    matches!(
        err,
        StorageError::ExtentSealed { .. } | StorageError::EpochStale { .. }
    )
}

fn is_primary_lost_stream(err: &StorageError) -> bool {
    matches!(err, StorageError::UnknownStream { .. })
}

fn needs_reconnect(err: &StorageError) -> bool {
    is_connection_broken(err) || is_routing_error(err) || is_primary_lost_stream(err)
}

// -- Main ---------------------------------------------------------------------

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let num_streams = num_streams();
    let writers_per_stream = writers_per_stream();
    let duration = bench_duration();
    let total_writers = num_streams * writers_per_stream;

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
    sleep(Duration::from_secs(3)).await;
    info!("[setup] Registration complete");

    // -- 5. Create N streams --------------------------------------------------
    let sm_client = StreamClient::connect(&stream_manager_addr)
        .await
        .expect("connect to StreamManager");

    struct StreamInfo {
        stream_id: StreamId,
        epoch: Epoch,
        primary_addr: String,
    }

    let mut streams = Vec::with_capacity(num_streams);
    for i in 0..num_streams {
        let stream_id = sm_client
            .open(
                &format!("bench-multi-{i}"),
                REPLICATION_FACTOR,
                MIN_EXTENT_CAPACITY,
                MAX_EXTENT_CAPACITY,
                CACHE_EXTENTS,
                EXTENT_GROWTH_FACTOR,
                StorageClass::Memory,
            )
            .await
            .unwrap_or_else(|e| panic!("open stream {i}: {e}"));

        let primary_addr = sm_client
            .cached_primary(stream_id)
            .await
            .unwrap_or_else(|| panic!("no cached primary for stream {i}"));

        let extents = sm_client
            .describe_stream(stream_id, 1)
            .await
            .unwrap_or_else(|e| panic!("describe_stream {i}: {e}"));
        let epoch = extents.first().map(|e| e.epoch).unwrap_or(Epoch(0));

        info!(
            "[setup] Stream {i} ({stream_id}) opened: primary={primary_addr}, epoch={epoch}"
        );
        streams.push(StreamInfo {
            stream_id,
            epoch,
            primary_addr,
        });
    }

    // -- 6. Shared counters ---------------------------------------------------
    let counters = Arc::new(SharedCounters::new());

    // -- 7. Spawn M×N writer tasks --------------------------------------------
    let start = Instant::now();

    let mut writer_id = 0usize;
    for stream_info in &streams {
        for _ in 0..writers_per_stream {
            let stream_id = stream_info.stream_id;
            let epoch = stream_info.epoch;
            let primary_addr = stream_info.primary_addr.clone();
            let sm_addr = stream_manager_addr.clone();
            let counters = Arc::clone(&counters);
            let wid = writer_id;

            tokio::spawn(async move {
                writer_task(wid, stream_id, epoch, primary_addr, sm_addr, duration, counters).await;
            });
            writer_id += 1;
        }
    }

    // -- 8. Periodic reporter -------------------------------------------------
    print_header(num_streams, writers_per_stream, total_writers);
    let mut cumulative_appends: u64 = 0;
    let mut cumulative_bytes: u64 = 0;
    let mut cumulative_errors: u64 = 0;
    let mut cumulative_reconnects: u64 = 0;
    let mut cumulative_hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
    let mut interval = tokio::time::interval(REPORT_INTERVAL);
    interval.tick().await; // first tick fires immediately — skip it

    let total_intervals = (duration.as_secs() / REPORT_INTERVAL.as_secs()) + 1;
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

    // -- 9. Shutdown ----------------------------------------------------------
    for node in extent_nodes {
        node.stop().await;
    }
}

// -- Reporter -----------------------------------------------------------------

fn print_header(num_streams: usize, writers_per_stream: usize, total_writers: usize) {
    eprintln!();
    eprintln!(
        "═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════"
    );
    eprintln!(
        "  Concurrent Multi-Stream Benchmark  |  streams={num_streams}  writers/stream={writers_per_stream}  total={total_writers}  payload={PAYLOAD_SIZE}B  RF={REPLICATION_FACTOR}  pipeline={PIPELINE_DEPTH}"
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

fn past_deadline(deadline: Instant) -> bool {
    Instant::now() >= deadline
}

// -- Writer -------------------------------------------------------------------

async fn writer_task(
    writer_id: usize,
    stream_id: StreamId,
    initial_epoch: Epoch,
    initial_primary_addr: String,
    stream_manager_addr: String,
    duration: Duration,
    counters: Arc<SharedCounters>,
) {
    let payload = Bytes::from(vec![0xABu8; PAYLOAD_SIZE]);
    let deadline = Instant::now() + duration;

    let mut primary_addr = initial_primary_addr;
    let mut epoch = initial_epoch;
    let mut backoff = Duration::from_millis(100);
    let max_backoff = Duration::from_secs(5);

    // Outer loop: manages connection lifecycle.
    'outer: loop {
        if past_deadline(deadline) {
            break;
        }

        // Connect to primary. On failure, rediscover via SM.
        let en_client = match StreamClient::connect(&primary_addr).await {
            Ok(c) => Arc::new(c),
            Err(e) => {
                warn!("writer {writer_id}: connect to {primary_addr} failed: {e}");
                counters.record_error();
                sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
                match reconnect_to_primary(&stream_manager_addr, stream_id, deadline).await {
                    Ok((_client, addr, new_epoch)) => {
                        primary_addr = addr;
                        epoch = new_epoch;
                        backoff = Duration::from_millis(100);
                        info!(
                            "writer {writer_id}: rediscovered primary {primary_addr} epoch {epoch}"
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
                let client = Arc::clone(&en_client);
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
                        "writer {writer_id}: {e} -- draining pipeline and {}",
                        if needs_seal {
                            "sealing epoch to recover"
                        } else {
                            "refreshing epoch"
                        }
                    );
                    counters.record_error();

                    // Drain remaining in-flight futures.
                    let mut drained = 0u64;
                    while let Some((_, res)) = in_flight.next().await {
                        if res.is_err() {
                            drained += 1;
                        }
                    }
                    if drained > 0 {
                        counters.errors.fetch_add(drained, Ordering::Relaxed);
                        info!("writer {writer_id}: drained {drained} stale in-flight appends");
                    }
                    counters.record_reconnect();

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
                                "writer {writer_id}: reconnected to {primary_addr} epoch {epoch}"
                            );
                        }
                        Err(e) => {
                            warn!("writer {writer_id}: reconnect failed: {e}");
                            if past_deadline(deadline) {
                                break 'outer;
                            }
                            backoff = (backoff * 2).min(max_backoff);
                        }
                    }
                    break 'inner;
                }
                Err(e) => {
                    warn!("writer {writer_id}: append error: {e}");
                    counters.record_error();
                }
            }
        }
    }

    info!("writer {writer_id}: shutting down (deadline reached)");
}

// -- Reconnect ----------------------------------------------------------------

async fn reconnect_to_primary(
    stream_manager_addr: &str,
    stream_id: StreamId,
    deadline: Instant,
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
    let sm_client = StreamClient::connect(stream_manager_addr).await?;
    let extents = sm_client.describe_stream(stream_id, 1).await?;
    let primary_addr = sm_client.cached_primary(stream_id).await.ok_or_else(|| {
        InternalSnafu {
            message: format!("stream {} missing primary after describe_stream", stream_id),
        }
        .build()
    })?;
    let epoch = extents
        .first()
        .map(|e| Epoch(e.epoch.0))
        .unwrap_or(Epoch(0));
    let en_client = Arc::new(StreamClient::connect(&primary_addr).await?);
    Ok((en_client, primary_addr, epoch))
}

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
            drop(sm_client);
            try_reconnect(stream_manager_addr, stream_id).await
        }
    }
}

async fn reconnect_with_seal(
    stream_manager_addr: &str,
    stream_id: StreamId,
    epoch: Epoch,
    deadline: Instant,
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

// -- Helpers ------------------------------------------------------------------

async fn clean_database(mysql_url: &str) {
    let pool = MySqlPoolOptions::new()
        .max_connections(1)
        .connect(mysql_url)
        .await
        .expect("failed to connect to MySQL for cleanup");
    for table in &[
        "stream_replica",
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
