# Known Issues & Improvement Backlog

Audit findings not addressed by the Pre-Phase 3 foundation fixes.
Grouped by category for prioritized resolution.

---

## Reliability

- **Mutex held across async DB query in allocator**: `allocator.rs` holds a lock while awaiting MySQL queries for extent allocation. Under contention this can block the tokio runtime thread. Refactor to acquire data under the lock, release, then perform async I/O.

- **Channel backpressure gaps**: `forward_tx` (ForwardRequest) and `seal_tx` (SealRequest) channels are bounded but callers use `try_send` and silently drop on full. Add backpressure signaling or bounded retry so requests are not silently lost under burst load.

- **Heartbeat checker race**: The heartbeat checker task reads `last_heartbeat` timestamps from MySQL without synchronizing with concurrent heartbeat updates. A node could be marked dead immediately after sending a heartbeat if the checker reads a stale row. Use a compare-and-swap or versioned update to avoid false positives.

- **Fire-and-forget seal with no retry**: When Primary detects ExtentFull, it sends a single SealRequest to Stream Manager via the channel. If Stream Manager is temporarily unreachable, the seal is lost and no new extent is allocated. Add retry with exponential backoff for seal requests.

- **Graceful shutdown gaps**: `ExtentNode::run()` does not drain in-flight append requests before stopping the TCP listener. Clients may receive connection-reset errors for requests that were already partially processed. Implement a shutdown drain period.

- **DownstreamManager stale connection cleanup**: TCP connections to secondary Extent Nodes are cached indefinitely in `DownstreamManager`. If a secondary is replaced (e.g., after node failure and re-allocation), the stale connection is never cleaned up. Add TTL-based eviction or health-check pings.

---

## Performance

- **Sequential RegisterExtent RPCs**: Stream Manager sends RegisterExtent to each Extent Node in the replica set sequentially. For RF=3, this triples the allocation latency. Send RegisterExtent RPCs concurrently via `tokio::join!` or `FuturesUnordered`.

- **Clone-heavy replication path**: The forwarding path clones the payload `Bytes` for each secondary. While `Bytes::clone()` is cheap (Arc increment), the `ForwardRequest` struct itself is allocated per-secondary. Consider a shared-payload design where one `Arc<ForwardRequest>` is sent to all secondaries.

- **No read caching for sealed extents**: Sealed extents are read from the in-memory arena, but there is no caching layer for extents that have been evicted from memory but not yet flushed to S3. Add an LRU read cache (e.g., `moka`) for recently-evicted sealed extents.

---

## Type Safety

- **Frame accessors return sentinel values instead of Option**: `frame.stream_id()`, `frame.offset()`, `frame.byte_pos()`, etc. return `StreamId(0)`, `Offset(0)`, `0u64` when the field is not present in the variable header variant. This masks bugs where the wrong opcode's fields are accessed. Refactor to return `Option<T>` and propagate errors at call sites.

- **Bare `u8` for replication role**: `ReplicaInfo.role` and `ReplicaDetail.role` use raw `u8`. Define a `ReplicaRole` enum (`Primary = 0`, `Secondary = 1`) with `TryFrom<u8>` for type-safe matching and exhaustive `match`.

- **Bare `String` for node addresses**: Node addresses are passed as `String` throughout (`replica_addrs`, `primary_addr`, `advertised_addr`). Introduce a `NodeAddr` newtype wrapping `String` to prevent accidental misuse (e.g., passing a stream name where an address is expected).

- **Coarse `ErrorCode` variants**: `ErrorCode` has only 6 variants. Timeout, authentication, version mismatch, and quota errors all collapse into `InternalError`. Add specific variants as the protocol evolves.

---

## Observability

- **Missing replication metrics**: No metrics for watermark latency (time from Primary append to quorum ACK), quorum convergence time, or dropped forward requests. Add histograms and counters for replication health monitoring.

- **No read-path logging**: `handle_read` does not log read requests or errors at any level. Add `tracing::debug!` for request parameters and `tracing::warn!` for read errors.

- **Hardcoded tuning constants**: Reconnect backoff (5s), disconnect ACK timeout (2s), and arena capacity are hardcoded. Expose these as configuration parameters in `ExtentNodeConfig` / `StreamManagerConfig` for operational tuning without recompilation.

---

## Testing

- **No fault-injection tests**: No tests simulate network partitions, slow secondaries, or partial write failures during broadcast replication. Add fault-injection tests using `tokio::time::pause()` and mock TCP streams.

- **No heartbeat failover test**: No integration test verifies that Stream Manager correctly marks a node as dead after missed heartbeats and triggers seal-and-new. Add a test that stops heartbeats and asserts the node transitions to Dead state.

- **No concurrent seal race test**: No test exercises the race between a client-initiated seal and an ExtentFull-triggered seal on the same extent. Add a test that triggers both concurrently and verifies exactly one seal succeeds.
