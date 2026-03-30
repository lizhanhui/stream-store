# Append Latency Analysis (RF=2, Two-Node Deployment)

## Test Setup

- **Deployment**: 2 nodes, 1 StreamManager + 3 ExtentNodes
- **Network**: ~0.45ms RTT between nodes (ping: 0.43–0.55ms)
- **Benchmark**: `benches/pipeline_append_cluster.rs` — 4 senders, 16 pipeline depth, 1KiB payload, RF=2
- **Results**: P99 = 1.5ms, Max = 85ms

## Critical Path (RF=2 Append)

Every hop an append touches with 0.45ms RTT:

```
Client → Primary EN                          [0.225ms one-way]
  1. TCP receive + frame decode
  2. Leader election (fetch_add)
  3. Arena write (memcpy 1KiB, ~nanoseconds)
  4. Push ForwardRequest to mpsc channel       ← channel hop 1
  5. DownstreamManager receives, builds Frame
  6. Push Frame to per-connection channel      ← channel hop 2
  7. connection_writer feed() + flush()

Primary EN → Secondary EN                    [0.225ms one-way]
  8. TCP receive + frame decode
  9. Arena write (replicate)
  10. Send Watermark ACK back

Secondary EN → Primary EN                    [0.225ms one-way]
  11. downstream_reader receives Watermark
  12. Push WatermarkEvent to mpsc channel      ← channel hop 3
  13. WatermarkHandler receives event
  14. Update AckQueue, drain_quorum()
  15. try_send AppendAck to response_tx        ← channel hop 4
  16. Write task feed() + flush()

Primary EN → Client                          [0.225ms one-way]
  17. Client reader task receives, resolves oneshot
```

**Network minimum**: 4 × 0.225ms = **0.9ms**

**Observed P99**: 1.5ms → **~0.6ms software overhead**

## Software Overhead Breakdown (~0.6ms)

| Source | Estimated Cost | Location |
|--------|---------------|----------|
| 4 mpsc channel hops | 20–80μs total | forward_tx, per_connection_tx, watermark_tx, response_tx |
| DownstreamManager dispatch | 5–10μs | HashMap lookup + try_send |
| feed+flush batching delay | 50–200μs | Downstream writer batches frames before flush |
| WatermarkHandler dispatch | 5–10μs | DashMap get_mut + drain_quorum |
| Server write task batching | 50–100μs | Same feed+flush pattern for AppendAck |
| Tokio task scheduling | 20–50μs | Each channel send/recv may wake a task |
| TCP syscalls | 10–30μs per flush | write() syscall overhead |
| Client pending map lock | 5–20μs | Mutex<HashMap> for request→oneshot |

## P99 = 1.5ms: Assessment

**This is very reasonable.** The theoretical minimum is ~0.9ms network. The ~0.6ms
software overhead is excellent for an async Rust system with 4 channel hops and
multiple TCP syscalls. For comparison:

- Kafka RF=2: typically 5–15ms P99
- Pulsar RF=2: typically 5–10ms P99
- NATS JetStream: 2–5ms P99

## Max = 85ms: Root Causes

1. **Extent seal-and-new** (dominant): When the 64MiB arena fills, the client gets
   `ExtentFull`, seals via StreamManager (MySQL transaction: BEGIN → UPDATE extent →
   INSERT extent → INSERT extent_replica × N → COMMIT → RegisterExtent RTT), then
   reconnects to the new primary. The benchmark includes this latency in per-append
   measurements.

2. **Tokio task starvation**: 4 senders × 16 pipeline depth = 64 concurrent tasks,
   plus DownstreamManager, WatermarkHandler, and per-connection tasks. The runtime
   may occasionally delay a task wake by milliseconds.

3. **Allocator pressure**: Each PendingAck holds a clone of `mpsc::Sender<Frame>`.
   Thousands of creates/drops can cause occasional allocator latency.

## Improvement Plan

### #1: Eliminate DownstreamManager channel hops

**Current path** (2 extra channel hops + 2 task wakes):
```
store.handle_append()
  → forward_tx.send(ForwardRequest)        ← channel hop
  → DownstreamManager receives
  → per_connection_tx.try_send(Frame)      ← channel hop
  → connection_writer task sends to TCP
```

**Proposed path** (0 extra channel hops):
```
store.handle_append()
  → downstream.send(addr, frame)           ← direct write to shared FramedWrite
```

The leader already serializes writes per-extent. Give it direct access to a
`DownstreamPool` that holds per-address `Arc<Mutex<FramedWrite>>` connections.
The leader calls `pool.forward(addr, frame).await` which locks the writer, feeds
the frame, and flushes. Since the leader already batches follower payloads, it can
feed multiple Forward frames before a single flush.

**Estimated savings**: 40–80μs per append (2 channel hops + 2 task wakes eliminated).

### #2: Inline WatermarkHandler into downstream reader

**Current path** (1 extra channel hop + 1 task wake):
```
downstream_reader receives Watermark
  → watermark_tx.send(WatermarkEvent)      ← channel hop
  → WatermarkHandler task receives
  → ack_queue.drain_quorum()
```

**Proposed path** (0 extra channel hops):
```
downstream_reader receives Watermark
  → store.handle_watermark(event)          ← direct call
```

The downstream_reader already has the watermark data. Give it an `Arc<ExtentNodeStore>`
reference and call `drain_quorum()` inline. The DashMap already provides per-stream
fine-grained locking, so concurrent readers for different streams don't contend.

**Estimated savings**: 10–20μs per append (1 channel hop + 1 task wake eliminated).

### Combined Impact

| Metric | Current | After #1 + #2 | Improvement |
|--------|---------|---------------|-------------|
| Channel hops per append | 4 | 1 (response_tx only) | 3 fewer hops |
| Task wakes per append | 4+ | 1 | 3 fewer wakes |
| Estimated P99 | 1.5ms | ~1.0–1.1ms | ~30% reduction |

### Future: io_uring

`io_uring` with SQPOLL mode eliminates ~2μs per syscall. With ~4 TCP syscalls per
append path, savings are ~8–16μs — a ~1–2% improvement on P99. The bigger benefit
is for disk I/O (Phase 3: S3 flush). Network-bound workloads see marginal gains
since Tokio's epoll is already efficient.

### Future: Pre-allocate successor extent

Proactively seal and allocate the next extent before the arena reaches 100% capacity.
This eliminates the stop-the-world seal path that causes the 85ms tail. Should be
measured separately from steady-state appends regardless.

## Summary

| Metric | Current | Theoretical Min | Achievable | Main Bottleneck |
|--------|---------|-----------------|------------|-----------------|
| P99 | 1.5ms | ~0.9ms | ~1.0–1.1ms | Channel hops (4 async boundaries) |
| Max | 85ms | ~1.5ms | <5ms | Seal-and-new (MySQL txn + reconnect) |
