//! Micro-benchmark for the production `ExtentNodeStore` pipeline.
//!
//! Exercises `handle_frame()` — the exact code path used in production —
//! including pipelined group-commit where the leader drains follower jobs
//! from a channel and followers return immediately (no spin-wait).
//!
//! Run with:
//! ```sh
//! cargo bench --package extent-node --bench store_append
//! ```
//!
//! View HTML report at:
//!   target/criterion/report/index.html

use std::sync::Arc;

use bytes::Bytes;
use common::types::{ArenaClass, Epoch, EpochPolicy, StorageClass, StreamConfig, StreamId};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use extent_node::store::ExtentNodeStore;
use rpc::frame::{Frame, VariableHeader};
use rpc::payload::build_register_extent_payload;
use server::handler::RequestHandler;
use tokio::sync::mpsc;

/// Payload sizes to benchmark.
const PAYLOAD_SIZES: &[(usize, &str)] =
    &[(64, "64B"), (256, "256B"), (1024, "1KiB"), (4096, "4KiB")];

/// Thread counts for concurrent benchmarks.
const THREAD_COUNTS: &[usize] = &[1, 2, 4, 8];

/// Register a stream epoch on the store, mirroring the test helper in store.rs.
async fn register_bench_stream(store: &ExtentNodeStore, stream_id: u32, epoch: u32) {
    let _ = epoch;
    let payload = build_register_extent_payload(&[]);
    let _ = store
        .handle_frame(
            Frame::new(
                VariableHeader::RegisterEpoch {
                    request_id: 0,
                    role: 0,
                    config: StreamConfig {
                        stream_id: StreamId(stream_id),
                        replication_factor: 1,
                        epoch: Epoch(0),
                        storage_class: StorageClass::S3,
                        arena_class: ArenaClass::Dedicated,
                        policy: EpochPolicy { cache: 4 },
                    },
                },
                Some(payload),
            ),
            None,
        )
        .await;
}

/// Single-threaded store append: measures per-record latency through the
/// full `handle_frame` → `handle_append` → `append_inner` path.
fn bench_store_append_single(c: &mut Criterion) {
    let mut group = c.benchmark_group("store_append_single");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .build()
        .unwrap();

    for &(size, label) in PAYLOAD_SIZES {
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_function(BenchmarkId::new("payload", label), |b| {
            let store = Arc::new(ExtentNodeStore::new());
            rt.block_on(register_bench_stream(&store, 1, 1));
            let payload = Bytes::from(vec![0xABu8; size]);
            let (resp_tx, mut resp_rx) = mpsc::channel::<Frame>(16);

            let mut seq: u32 = 0;
            b.iter(|| {
                let frame = Frame::new(
                    VariableHeader::Append {
                        request_id: seq,
                        stream_id: StreamId(1),
                        epoch: Epoch(0),
                    },
                    Some(payload.clone()),
                );
                seq = seq.wrapping_add(1);
                let result = rt.block_on(store.handle_frame(frame, Some(&resp_tx)));
                if result.is_none() {
                    let _ = rt.block_on(resp_rx.recv()).unwrap();
                }
            });
        });
    }

    group.finish();
}

/// Store-level concurrent append: benchmarks the production pipelined
/// group-commit path via `ExtentNodeStore::handle_frame()`.
///
/// This exercises the real pipeline where the leader drains follower jobs
/// from a channel and followers return immediately (no spin-wait).
fn bench_store_append_concurrent(c: &mut Criterion) {
    let mut group = c.benchmark_group("store_append_concurrent");
    group.sample_size(50);

    // Build a multi-threaded tokio runtime for async handle_frame calls.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .build()
        .unwrap();

    for &(size, label) in PAYLOAD_SIZES {
        for &num_threads in THREAD_COUNTS {
            let param = format!("{label}/t{num_threads}");
            let ops_per_thread: u64 = 10_000;
            group.throughput(Throughput::Bytes(
                size as u64 * num_threads as u64 * ops_per_thread,
            ));

            group.bench_function(BenchmarkId::new("payload", &param), |b| {
                let rt = &rt;
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;

                    for _ in 0..iters {
                        // Setup: create store, register stream+extent.
                        let store = Arc::new(ExtentNodeStore::new());
                        rt.block_on(register_bench_stream(&store, 1, 1));

                        let payload = Bytes::from(vec![0xABu8; size]);
                        let start = fastant::Instant::now();

                        // Spawn N threads, each doing ops_per_thread appends
                        // via handle_frame with the response channel pattern.
                        std::thread::scope(|s| {
                            for t in 0..num_threads {
                                let store = &store;
                                let p = payload.clone();
                                s.spawn(move || {
                                    rt.block_on(async {
                                        let (resp_tx, mut resp_rx) = mpsc::channel::<Frame>(16);
                                        for seq in 0..ops_per_thread {
                                            let frame = Frame::new(
                                                VariableHeader::Append {
                                                    request_id: (t as u64 * ops_per_thread + seq)
                                                        as u32,
                                                    stream_id: StreamId(1),
                                                    epoch: Epoch(0),
                                                },
                                                Some(p.clone()),
                                            );
                                            let result =
                                                store.handle_frame(frame, Some(&resp_tx)).await;
                                            if result.is_none() {
                                                // Follower path: ACK arrives via channel.
                                                let _ = resp_rx.recv().await.unwrap();
                                            }
                                        }
                                    });
                                });
                            }
                        });

                        total += start.elapsed();
                    }

                    total
                });
            });
        }
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_store_append_single,
    bench_store_append_concurrent,
);
criterion_main!(benches);
