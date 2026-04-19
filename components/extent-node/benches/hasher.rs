//! Micro-benchmark: IdentityBuildHasher vs default RandomState for StreamId lookups.
//!
//! Measures the cost of `HashMap::pin().get(&key)` — the exact hot-path pattern
//! used on every append, forward, read, and seal operation — comparing:
//!
//! - **identity**: `papaya::HashMap<StreamId, V, IdentityBuildHasher>` (no hashing)
//! - **siphash**:  `papaya::HashMap<StreamId, V>` (default `RandomState` / SipHash)
//!
//! Run with:
//! ```sh
//! cargo bench --package extent-node --bench hasher
//! ```

use common::hasher::IdentityBuildHasher;
use common::types::StreamId;
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};

/// Number of entries pre-populated in the map.
/// Realistic: an ExtentNode typically hosts 10s–100s of streams.
const MAP_SIZES: &[u32] = &[10, 100, 1_000];

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Populate a papaya::HashMap<StreamId, u64, IdentityBuildHasher>.
fn build_identity_map(n: u32) -> papaya::HashMap<StreamId, u64, IdentityBuildHasher> {
    let map = papaya::HashMap::with_hasher(IdentityBuildHasher);
    {
        let guard = map.pin();
        for i in 0..n {
            guard.insert(StreamId(i), i as u64);
        }
    }
    map
}

/// Populate a papaya::HashMap<StreamId, u64> (default RandomState).
fn build_default_map(n: u32) -> papaya::HashMap<StreamId, u64> {
    let map = papaya::HashMap::new();
    {
        let guard = map.pin();
        for i in 0..n {
            guard.insert(StreamId(i), i as u64);
        }
    }
    map
}

// ── Benchmarks ──────────────────────────────────────────────────────────────

/// Benchmark: single pin().get() lookup — the hot-path operation.
fn bench_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("hasher_lookup");

    for &size in MAP_SIZES {
        // Identity hasher
        group.bench_function(BenchmarkId::new("identity", size), |b| {
            let map = build_identity_map(size);
            let key = StreamId(size / 2); // lookup a key in the middle
            b.iter(|| {
                let guard = map.pin();
                let val = guard.get(&key).copied();
                std::hint::black_box(val)
            });
        });

        // Default SipHash
        group.bench_function(BenchmarkId::new("siphash", size), |b| {
            let map = build_default_map(size);
            let key = StreamId(size / 2);
            b.iter(|| {
                let guard = map.pin();
                let val = guard.get(&key).copied();
                std::hint::black_box(val)
            });
        });
    }

    group.finish();
}

/// Benchmark: sequential lookups across different keys (simulates dispatch loop).
fn bench_sequential_lookups(c: &mut Criterion) {
    let mut group = c.benchmark_group("hasher_sequential_10");

    for &size in MAP_SIZES {
        let keys: Vec<StreamId> = (0..10).map(|i| StreamId(i % size)).collect();

        group.bench_function(BenchmarkId::new("identity", size), |b| {
            let map = build_identity_map(size);
            b.iter(|| {
                let guard = map.pin();
                for key in &keys {
                    std::hint::black_box(guard.get(key).copied());
                }
            });
        });

        group.bench_function(BenchmarkId::new("siphash", size), |b| {
            let map = build_default_map(size);
            b.iter(|| {
                let guard = map.pin();
                for key in &keys {
                    std::hint::black_box(guard.get(key).copied());
                }
            });
        });
    }

    group.finish();
}

/// Benchmark: insert — measures hasher cost during stream registration.
fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("hasher_insert");

    for &size in MAP_SIZES {
        group.bench_function(BenchmarkId::new("identity", size), |b| {
            b.iter_batched(
                || build_identity_map(size),
                |map| {
                    let guard = map.pin();
                    guard.insert(StreamId(size + 1), 42u64);
                },
                BatchSize::SmallInput,
            );
        });

        group.bench_function(BenchmarkId::new("siphash", size), |b| {
            b.iter_batched(
                || build_default_map(size),
                |map| {
                    let guard = map.pin();
                    guard.insert(StreamId(size + 1), 42u64);
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

/// Benchmark: concurrent lookups from multiple threads (simulates production load).
fn bench_concurrent_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("hasher_concurrent_lookup");
    group.sample_size(50);

    let thread_counts: &[usize] = &[2, 4, 8];
    let size: u32 = 100; // realistic stream count
    let ops_per_thread: u64 = 50_000;

    for &num_threads in thread_counts {
        group.bench_function(BenchmarkId::new("identity", num_threads), |b| {
            let map = build_identity_map(size);
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let start = std::time::Instant::now();
                    std::thread::scope(|s| {
                        for t in 0..num_threads {
                            let map = &map;
                            s.spawn(move || {
                                for i in 0..ops_per_thread {
                                    let key = StreamId(((t as u64 * 7 + i) % size as u64) as u32);
                                    let guard = map.pin();
                                    std::hint::black_box(guard.get(&key));
                                }
                            });
                        }
                    });
                    total += start.elapsed();
                }
                total
            });
        });

        group.bench_function(BenchmarkId::new("siphash", num_threads), |b| {
            let map = build_default_map(size);
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let start = std::time::Instant::now();
                    std::thread::scope(|s| {
                        for t in 0..num_threads {
                            let map = &map;
                            s.spawn(move || {
                                for i in 0..ops_per_thread {
                                    let key = StreamId(((t as u64 * 7 + i) % size as u64) as u32);
                                    let guard = map.pin();
                                    std::hint::black_box(guard.get(&key));
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

    group.finish();
}

criterion_group!(
    benches,
    bench_lookup,
    bench_sequential_lookups,
    bench_insert,
    bench_concurrent_lookup,
);
criterion_main!(benches);
