//! Micro-benchmark for `Extent::append` and `index_lookup` performance.
//!
//! Uses Criterion for statistically rigorous measurement with confidence
//! intervals, outlier detection, and HTML reports.
//!
//! Run with:
//! ```sh
//! cargo bench --package extent-node --bench extent_append
//! ```
//!
//! View HTML report at:
//!   target/criterion/report/index.html

use std::sync::Arc;

use bytes::Bytes;
use common::types::{ExtentId, Offset};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use extent_node::extent::Extent;

const ARENA_CAPACITY: usize = 64 * 1024 * 1024; // 64 MiB

/// Payload sizes to benchmark.
const PAYLOAD_SIZES: &[(usize, &str)] =
    &[(64, "64B"), (256, "256B"), (1024, "1KiB"), (4096, "4KiB")];

/// Thread counts for concurrent benchmarks.
const THREAD_COUNTS: &[usize] = &[1, 2, 4, 8];

/// Single-threaded append: measures raw per-record append latency.
fn bench_append_single(c: &mut Criterion) {
    let mut group = c.benchmark_group("append_single");

    for &(size, label) in PAYLOAD_SIZES {
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_function(BenchmarkId::new("payload", label), |b| {
            let payload = Bytes::from(vec![0xABu8; size]);
            let extent = Extent::with_capacity(ExtentId(0), Offset(0), ARENA_CAPACITY);

            b.iter(|| {
                let _ = extent.append(payload.clone());
            });
        });
    }

    group.finish();
}

/// Multi-threaded append: measures aggregate throughput under contention.
fn bench_append_concurrent(c: &mut Criterion) {
    let mut group = c.benchmark_group("append_concurrent");
    // Each iteration is fast; use enough samples for stable results.
    group.sample_size(50);

    for &(size, label) in PAYLOAD_SIZES {
        for &num_threads in THREAD_COUNTS {
            let param = format!("{label}/t{num_threads}");
            // Throughput: each iteration does `num_threads * ops_per_thread` appends.
            // We set per-element throughput so Criterion reports bytes/sec correctly.
            let ops_per_thread: u64 = 10_000;
            group.throughput(Throughput::Bytes(
                size as u64 * num_threads as u64 * ops_per_thread,
            ));

            group.bench_function(BenchmarkId::new("payload", &param), |b| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;

                    for _ in 0..iters {
                        let payload = Bytes::from(vec![0xABu8; size]);
                        let extent = Arc::new(Extent::with_capacity(
                            ExtentId(0),
                            Offset(0),
                            ARENA_CAPACITY,
                        ));

                        let start = fastant::Instant::now();

                        std::thread::scope(|s| {
                            for _ in 0..num_threads {
                                let ext = &extent;
                                let p = payload.clone();
                                s.spawn(move || {
                                    for _ in 0..ops_per_thread {
                                        let _ = ext.append(p.clone());
                                    }
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

/// Index lookup: measures read-path offset resolution latency.
fn bench_index_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_lookup");

    for &(size, label) in PAYLOAD_SIZES {
        group.throughput(Throughput::Elements(1));

        group.bench_function(BenchmarkId::new("payload", label), |b| {
            let payload = Bytes::from(vec![0xABu8; size]);
            let extent = Extent::with_capacity(ExtentId(0), Offset(0), ARENA_CAPACITY);

            // Populate extent with records.
            let record_size = 4 + size;
            let num_records = (ARENA_CAPACITY / record_size).min(500_000);
            for _ in 0..num_records {
                if extent.append(payload.clone()).is_err() {
                    break;
                }
            }
            let count = extent.message_count();

            let mut seq: u64 = 0;
            b.iter(|| {
                let result = extent.index_lookup(seq % count);
                seq = seq.wrapping_add(1);
                criterion::black_box(result)
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_append_single,
    bench_append_concurrent,
    bench_index_lookup,
);
criterion_main!(benches);
