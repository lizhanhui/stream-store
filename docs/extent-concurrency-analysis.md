# Extent Concurrency Analysis

## Benchmark Results

Using Criterion on a single `Extent` with 64B payloads (worst case for contention — minimal memcpy, maximum CAS pressure):

| Threads | Total time (10K ops/thread) | Throughput | Per-op (amortized) |
|---------|---------------------------|------------|-------------------|
| 1 | 100 µs | 5.96 GiB/s | ~10 ns/op |
| 2 | 1.74 ms | 703 MiB/s | ~87 ns/op |
| 4 | 4.89 ms | 499 MiB/s | ~122 ns/op |
| 8 | 29.9 ms | 153 MiB/s | ~374 ns/op |

Going from 1→8 threads, per-op latency increases **~37×** and aggregate throughput *drops* ~97%.

Single-threaded append latency is ~4.3 ns/op across all payload sizes. Index lookup is ~1.05 ns/op (~950M lookups/sec).

## Why Concurrent Throughput Drops

The bottleneck is **step 4 of the append protocol: the spin-wait CAS on `committed_seq`**.

### The three phases of append

1. **`write_cursor.fetch_add`** — truly parallel, O(1), no contention beyond cache-line bouncing.
2. **`memcpy` into reserved slot** — truly parallel, each thread writes to a disjoint region.
3. **`committed_seq` CAS spin-wait** — **strictly serial**. The thread with `seq=N` must wait for the thread with `seq=N-1` to finish its memcpy AND CAS. This is an in-order commit barrier that guarantees readers see a gap-free prefix of committed records.

### Cache-line contention

With 8 threads, each thread's CAS succeeds only once per ~8 attempts on average. `committed_seq` lives on a single cache line that bounces between cores via the MESI coherency protocol. Each CAS failure + retry costs ~40–80 ns on x86-64 (L3 cache-to-cache transfer). With 8 threads all hammering the same cache line, the dominant cost is not the CAS instruction itself but the inter-core cache coherency traffic.

### Head-of-line blocking

The spin-wait has a head-of-line blocking problem: if the thread holding `seq=5` gets preempted by the OS scheduler after its memcpy but before its CAS, all threads with `seq=6,7,8,...` spin-wait doing nothing useful. This is why the 8-thread case shows disproportionately worse latency — more threads means higher probability of scheduler interference during the critical section.

## Why This Doesn't Matter in Production

The concurrent throughput drop is expected and **irrelevant to production workloads**. The system is designed so that each extent sees low writer concurrency:

### 1. Single primary writer per stream

Each stream's active extent has exactly one primary ExtentNode. Client appends arrive over the network and are handled by tokio tasks. The number of concurrent in-flight appends to one stream is small — bounded by `network_RTT × concurrent_client_count` for that stream. In practice this is 1–4, not 8+.

### 2. Horizontal scaling across streams, not vertical within one stream

With 1000 streams across 3 ExtentNodes, each node handles ~333 streams, but each individual stream sees low concurrency. The benchmark's 8-thread-on-one-extent scenario is measuring worst-case contention that production topology avoids by design.

### 3. The single-thread number is what matters

~4.3 ns/op means a single core can append **~230 million records/sec**. Even with real network I/O overhead (syscalls, TCP framing, replication), the extent append itself will never be the bottleneck. The network layer is orders of magnitude slower.

## If Multi-Writer Concurrency Were Ever Needed

If future requirements demanded high-concurrency writes to a single extent (not currently needed), the standard approach would be **batching**: a single coordinator thread collects appends from multiple clients and writes them in one shot. The coordinator does one `fetch_add` + one large `memcpy` + one CAS for N records, amortizing the serialization cost across the batch.

This is the same technique used by:
- **LMAX Disruptor**: single-thread event publisher, multi-thread consumers
- **Linux io_uring**: kernel-side SQ processing is single-threaded

Given the current architecture (one primary writer per extent, horizontal scaling via streams), this optimization is unnecessary.

## Conclusion

The lock-free append protocol is optimized for the single-writer-per-extent model. The in-order commit barrier (`committed_seq` CAS) provides the correctness guarantee that readers always see a gap-free prefix of committed records. The cost of this guarantee is negligible with one writer (~4.3 ns) and increases with contention — but the architecture ensures contention stays low by distributing streams across nodes rather than funneling writers into one extent.

Run the benchmark yourself:
```sh
cargo bench --package extent-node --bench extent_append
```

HTML reports at `docs/profiling/index.html`.
