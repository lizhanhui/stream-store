# Identity Hasher Benchmark Results

Benchmark comparing `IdentityBuildHasher` (identity/no-op hash) vs default `RandomState` (SipHash)
for `papaya::HashMap<StreamId, V>` lookups — the hot-path data structure on every append, forward,
read, and seal operation.

`StreamId` is a newtype over `u32`, server-assigned and sequential. SipHash's DoS resistance is
unnecessary for these non-adversarial keys.

Run with:
```sh
cargo bench --package extent-node --bench hasher
```

## Environment

- **CPU**: 2 vCPU (cloud instance)
- **papaya**: 0.2.4
- **Rust**: edition 2024, `bench` profile (optimized)

## Results

### Single Lookup (`pin().get()`)

The exact hot-path pattern: acquire pin guard, lookup by StreamId.

| Map size | Identity | SipHash | Speedup |
|----------|----------|---------|---------|
| 10       | 16.4 ns  | 27.0 ns | 39%     |
| 100      | 16.4 ns  | 27.2 ns | 40%     |
| 1,000    | 16.4 ns  | 28.8 ns | 43%     |

Identity lookup cost is constant regardless of map size (~16.4 ns).
SipHash degrades slightly at 1,000 entries due to cache effects.

### Sequential 10 Lookups (shared pin guard)

Simulates a dispatch loop processing multiple streams under one pin guard.
Pin guard cost is amortized — pure hash overhead dominates.

| Map size | Identity | SipHash  | Speedup |
|----------|----------|----------|---------|
| 10       | 33.8 ns  | 144.1 ns | 4.3×   |
| 100      | 33.1 ns  | 144.6 ns | 4.4×   |
| 1,000    | 36.4 ns  | 149.5 ns | 4.1×   |

~11 ns/lookup for identity vs ~14.5 ns/lookup for SipHash when pin cost is removed.

### Insert

Measures hasher cost during stream registration (not hot path, but confirms no regression).

| Map size | Identity | SipHash  | Speedup |
|----------|----------|----------|---------|
| 10       | 1.01 µs  | 1.23 µs  | 18%     |
| 100      | 3.47 µs  | 4.64 µs  | 25%     |
| 1,000    | 20.9 µs  | 29.5 µs  | 29%     |

### Concurrent Lookup (100 entries, 50K ops/thread)

Production-realistic contention: multiple threads doing `pin().get()` concurrently.

| Threads | Identity | SipHash  | Speedup |
|---------|----------|----------|---------|
| 2       | 0.80 ms  | 1.57 ms  | 49%     |
| 4       | 0.87 ms  | 1.82 ms  | 52%     |
| 8       | 1.08 ms  | 2.30 ms  | 53%     |

Identity hasher scales better under contention — less CPU time per lookup means
threads spend less time in the hash computation critical section.

## Conclusion

The identity hasher eliminates ~10–12 ns of SipHash overhead per lookup.
On the append hot path, each request does at least 3 map lookups (`streams`, `replicas`, `ack_queues`),
saving ~30–36 ns per append. At 100K appends/sec, this saves ~3 ms of CPU time per second —
meaningful on a latency-sensitive data path.
