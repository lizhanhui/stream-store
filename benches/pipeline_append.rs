//! Pipeline Append Benchmark
//!
//! Measures pipeline append performance on a single stream — maintaining N in-flight
//! append requests without blocking, measuring both per-append latency (p50/p99/max)
//! and aggregate throughput (ops/sec, MB/sec).
//!
//! Operates in-process against `ExtentNodeStore` directly (no network, no MySQL),
//! with a simulated watermark loop to complete the replication ACK path.
//!
//! Architecture:
//! ```text
//! Sender Tasks (N=4, semaphore-gated) ──→ ExtentNodeStore (Primary, RF=2)
//!         ↑                                      │
//!         │                                      ├──→ forward_tx ──→ Watermark Simulator
//!         │                                      │                        │
//!         └── ACK via response_rx ←── ack_queues ←──── watermark_tx ─────┘
//! ```
//! Run with:
//! ```sh
//! cargo bench --bench pipeline_append
//! ```

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::types::{ExtentId, Opcode, StreamId};
use extent_node::store::{ExtentNodeStore, ForwardRequest, WatermarkEvent};
use rpc::frame::{Frame, VariableHeader};
use rpc::payload::build_register_extent_payload;
use server::handler::RequestHandler;
use tokio::sync::{mpsc, Semaphore};

// ── Benchmark Parameters ─────────────────────────────────────────────────────

const BENCH_DURATION: Duration = Duration::from_secs(5);
const NUM_SENDERS: usize = 4;
const MAX_IN_FLIGHT: usize = 256;
const PAYLOAD_SIZE: usize = 1024;

const STREAM_ID: StreamId = StreamId(1);
const EXTENT_ID: ExtentId = ExtentId(1);
const FAKE_SECONDARY_ADDR: &str = "127.0.0.1:19802";

// ── Main ─────────────────────────────────────────────────────────────────────

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(run_benchmark());
}

async fn run_benchmark() {
    // ── 1. Create channels ───────────────────────────────────────────────────
    let (forward_tx, mut forward_rx) = mpsc::channel::<ForwardRequest>(4096);
    let (watermark_tx, mut watermark_rx) = mpsc::channel::<WatermarkEvent>(4096);

    // ── 2. Build ExtentNodeStore ─────────────────────────────────────────────
    let mut store = ExtentNodeStore::with_forward_tx(forward_tx);
    // Use a large arena (512 MiB) so the extent never fills during the benchmark.
    store.set_arena_capacity(512 * 1024 * 1024);
    let store = Arc::new(store);

    // ── 3. Register stream as Primary RF=2 with one fake secondary ───────────
    let payload = build_register_extent_payload(&[FAKE_SECONDARY_ADDR]);
    let reg_frame = Frame::new(
        VariableHeader::RegisterExtent {
            request_id: 0,
            stream_id: STREAM_ID,
            extent_id: EXTENT_ID,
            role: 0, // Primary
            replication_factor: 2,
        },
        Some(payload),
    );
    let resp = store.handle_frame(reg_frame, None).await;
    assert!(resp.is_some());
    assert_eq!(resp.unwrap().opcode(), Opcode::RegisterExtentAck);

    // ── 4. Shared state ──────────────────────────────────────────────────────
    let stop = Arc::new(AtomicBool::new(false));
    let semaphore = Arc::new(Semaphore::new(MAX_IN_FLIGHT));
    let next_request_id = Arc::new(AtomicU32::new(1));

    // Per-request send timestamps for latency measurement.
    // Key: request_id, Value: send Instant
    let send_times: Arc<dashmap::DashMap<u32, Instant>> = Arc::new(dashmap::DashMap::new());

    // Collected latencies from all ACK collectors
    let total_appends = Arc::new(AtomicU64::new(0));
    let all_latencies: Arc<tokio::sync::Mutex<Vec<Duration>>> =
        Arc::new(tokio::sync::Mutex::new(Vec::new()));

    // ── 5. Create per-sender response channels ───────────────────────────────
    // Each sender has its own (resp_tx, resp_rx) so that PendingAck responses
    // route back to the correct ACK collector task.
    let mut sender_handles = Vec::new();
    let mut collector_handles = Vec::new();

    for _sender_idx in 0..NUM_SENDERS {
        let (resp_tx, mut resp_rx) = mpsc::channel::<Frame>(MAX_IN_FLIGHT);

        // Clone shared state for sender task
        let store_s = Arc::clone(&store);
        let stop_s = Arc::clone(&stop);
        let semaphore_s = Arc::clone(&semaphore);
        let next_request_id_s = Arc::clone(&next_request_id);
        let send_times_s = Arc::clone(&send_times);
        let resp_tx_s = resp_tx.clone();

        // Clone shared state for collector task
        let stop_c = Arc::clone(&stop);
        let semaphore_c = Arc::clone(&semaphore);
        let send_times_c = Arc::clone(&send_times);
        let total_appends_c = Arc::clone(&total_appends);
        let all_latencies_c = Arc::clone(&all_latencies);

        // ── Sender task ──────────────────────────────────────────────────────
        let sender = tokio::spawn(async move {
            let payload = Bytes::from(vec![0xABu8; PAYLOAD_SIZE]);

            while !stop_s.load(Ordering::Relaxed) {
                // Acquire semaphore permit (backpressure when MAX_IN_FLIGHT reached)
                let permit = match semaphore_s.acquire().await {
                    Ok(p) => p,
                    Err(_) => break, // semaphore closed
                };
                // We intentionally forget the permit here; the ACK collector will
                // add permits back when it receives responses.
                permit.forget();

                let request_id = next_request_id_s.fetch_add(1, Ordering::Relaxed);

                // Record send time
                send_times_s.insert(request_id, Instant::now());

                let frame = Frame::new(
                    VariableHeader::Append {
                        request_id,
                        stream_id: STREAM_ID,
                        extent_id: EXTENT_ID,
                    },
                    Some(payload.clone()),
                );

                // handle_frame returns None for deferred Primary appends (RF=2).
                // The ACK will arrive via resp_tx when quorum is reached.
                // If the extent is full/sealed, it returns Some(error frame) — stop sending.
                let result = store_s.handle_frame(frame, Some(&resp_tx_s)).await;
                if result.is_some() {
                    // Extent full or sealed — release the permit and stop
                    semaphore_s.add_permits(1);
                    send_times_s.remove(&request_id);
                    break;
                }
            }
        });
        sender_handles.push(sender);

        // ── ACK collector task ───────────────────────────────────────────────
        let collector = tokio::spawn(async move {
            let mut local_latencies = Vec::with_capacity(65536);

            loop {
                match resp_rx.try_recv() {
                    Ok(frame) => {
                        let req_id = frame.request_id();

                        // Compute latency
                        if let Some((_, sent_at)) = send_times_c.remove(&req_id) {
                            local_latencies.push(sent_at.elapsed());
                        }

                        total_appends_c.fetch_add(1, Ordering::Relaxed);

                        // Release semaphore permit
                        semaphore_c.add_permits(1);
                    }
                    Err(mpsc::error::TryRecvError::Empty) => {
                        if stop_c.load(Ordering::Relaxed) {
                            // Drain remaining
                            while let Ok(frame) = resp_rx.try_recv() {
                                let req_id = frame.request_id();
                                if let Some((_, sent_at)) = send_times_c.remove(&req_id) {
                                    local_latencies.push(sent_at.elapsed());
                                }
                                total_appends_c.fetch_add(1, Ordering::Relaxed);
                                semaphore_c.add_permits(1);
                            }
                            break;
                        }
                        // Yield to avoid busy-spinning
                        tokio::task::yield_now().await;
                    }
                    Err(mpsc::error::TryRecvError::Disconnected) => break,
                }
            }

            // Merge local latencies into global
            all_latencies_c.lock().await.extend(local_latencies);
        });
        collector_handles.push(collector);
    }

    // ── 6. Watermark simulator ───────────────────────────────────────────────
    // Drains forward_rx and immediately sends WatermarkEvent back (simulates
    // instant secondary ACK — no network delay).
    let watermark_tx_clone = watermark_tx.clone();
    let stop_wm_sim = Arc::clone(&stop);
    let wm_simulator = tokio::spawn(async move {
        loop {
            match forward_rx.try_recv() {
                Ok(req) => {
                    let event = WatermarkEvent {
                        stream_id: req.stream_id,
                        acked_offset: req.offset,
                        source_addr: req.downstream_addr,
                    };
                    let _ = watermark_tx_clone.send(event).await;
                }
                Err(mpsc::error::TryRecvError::Empty) => {
                    if stop_wm_sim.load(Ordering::Relaxed) {
                        // Drain remaining
                        while let Ok(req) = forward_rx.try_recv() {
                            let event = WatermarkEvent {
                                stream_id: req.stream_id,
                                acked_offset: req.offset,
                                source_addr: req.downstream_addr,
                            };
                            let _ = watermark_tx_clone.send(event).await;
                        }
                        break;
                    }
                    tokio::task::yield_now().await;
                }
                Err(mpsc::error::TryRecvError::Disconnected) => break,
            }
        }
    });

    // ── 7. Watermark handler ─────────────────────────────────────────────────
    // Reads WatermarkEvents, updates ack_queue, drains quorum — this sends
    // AppendAck frames through each PendingAck's response_tx.
    let store_wm = Arc::clone(&store);
    let stop_wm = Arc::clone(&stop);
    let wm_handler = tokio::spawn(async move {
        loop {
            match watermark_rx.try_recv() {
                Ok(event) => {
                    if let Some(mut ack_queue) = store_wm.ack_queues.get_mut(&event.stream_id) {
                        ack_queue.ack_from_secondary(&event.source_addr, event.acked_offset);
                        ack_queue.drain_quorum();
                    }
                }
                Err(mpsc::error::TryRecvError::Empty) => {
                    if stop_wm.load(Ordering::Relaxed) {
                        // Drain remaining
                        while let Ok(event) = watermark_rx.try_recv() {
                            if let Some(mut ack_queue) =
                                store_wm.ack_queues.get_mut(&event.stream_id)
                            {
                                ack_queue.ack_from_secondary(
                                    &event.source_addr,
                                    event.acked_offset,
                                );
                                ack_queue.drain_quorum();
                            }
                        }
                        break;
                    }
                    tokio::task::yield_now().await;
                }
                Err(mpsc::error::TryRecvError::Disconnected) => break,
            }
        }
    });

    // ── 8. Run for BENCH_DURATION ────────────────────────────────────────────
    let bench_start = Instant::now();
    tokio::time::sleep(BENCH_DURATION).await;
    stop.store(true, Ordering::Relaxed);

    // ── 9. Join all tasks ────────────────────────────────────────────────────
    // Drop watermark_tx so the handler can finish
    drop(watermark_tx);

    for h in sender_handles {
        let _ = h.await;
    }
    // Give watermark pipeline time to drain
    let _ = wm_simulator.await;
    let _ = wm_handler.await;
    for h in collector_handles {
        let _ = h.await;
    }

    let elapsed = bench_start.elapsed();

    // ── 10. Compute and print statistics ─────────────────────────────────────
    let appends = total_appends.load(Ordering::Relaxed);
    let mut latencies = all_latencies.lock().await;
    latencies.sort();

    let ops_per_sec = appends as f64 / elapsed.as_secs_f64();
    let mb_per_sec = (appends as f64 * PAYLOAD_SIZE as f64) / (1024.0 * 1024.0) / elapsed.as_secs_f64();

    println!();
    println!("═══════════════════════════════════════════════════════════════");
    println!("  Pipeline Append Benchmark Results");
    println!("═══════════════════════════════════════════════════════════════");
    println!("  Duration:        {:.2}s", elapsed.as_secs_f64());
    println!("  Senders:         {NUM_SENDERS}");
    println!("  Max in-flight:   {MAX_IN_FLIGHT}");
    println!("  Payload size:    {PAYLOAD_SIZE} bytes");
    println!("  RF:              2 (1 primary + 1 simulated secondary)");
    println!("───────────────────────────────────────────────────────────────");
    println!("  Total appends:   {appends}");
    println!("  Throughput:      {ops_per_sec:.0} ops/sec");
    println!("  Throughput:      {mb_per_sec:.2} MB/sec");
    println!("───────────────────────────────────────────────────────────────");

    if !latencies.is_empty() {
        let p50 = latencies[latencies.len() / 2];
        let p99 = latencies[(latencies.len() as f64 * 0.99) as usize];
        let max = latencies[latencies.len() - 1];
        println!("  Latency p50:     {p50:?}");
        println!("  Latency p99:     {p99:?}");
        println!("  Latency max:     {max:?}");
    } else {
        println!("  Latency:         (no completed appends)");
    }

    println!("═══════════════════════════════════════════════════════════════");
    println!();
}
