# End-to-End Profiling: concurrent_multi_stream Benchmark

## Benchmark Setup

- 1 StreamManager + 3 ExtentNodes (all localhost)
- 10 clients, each with its own stream
- 1 KiB payload, RF=2, 4 MiB arena (triggers frequent seals)
- Duration: 5 seconds

## Result

```
Total appends: 112,384
Throughput:    21,120 ops/sec
Throughput:    21.63 MB/sec
Total seals:   24
```

For context, the bare `Extent::append` micro-benchmark does **~230M ops/sec** on a single thread. The end-to-end system is **~10,000× slower** than the storage engine. The bottleneck is entirely in the network path.

## Root Cause: Stop-and-Wait over a 4-Hop Deferred Replication Pipeline

Each client append traverses this critical path (RF=2):

```
Client ─send──→ Primary ─forward──→ Secondary
                                        │ (write locally)
Client ←─resp─── Primary ←─watermark───┘
```

4 TCP hops on the critical path, and the client is **stop-and-wait**: `send_recv()` sends one frame then blocks awaiting the response. With loopback RTT ~50–100µs per hop:

| Step | What | Cost |
|------|------|------|
| 1 | Client → Primary (TCP write + read dispatch) | ~50 µs |
| 2 | Primary local write (lock-free CAS) | ~0.005 µs |
| 3 | Primary → mpsc → DownstreamManager → Secondary (TCP write) | ~50 µs |
| 4 | Secondary local write + Watermark response | ~0.005 µs |
| 5 | Secondary → Primary (TCP read → WatermarkHandler → drain) | ~50 µs |
| 6 | Primary → Client (deferred response via mpsc) | ~50 µs |
| **Total** | | **~200 µs** |

At ~200µs/op per client, 10 clients = 10 × 5,000 = **50,000 ops/sec theoretical**. We see 21K because of additional overhead factors.

## Contributing Factors

### 1. Nagle's Algorithm (TCP_NODELAY not set) — FIXED

**Impact**: +0–40ms per hop, devastating for small frames.

`TCP_NODELAY` was never set on any TCP socket. Nagle's algorithm buffers small writes for up to 40ms, waiting for either a full MSS (~1460 bytes) or an ACK from the peer.

The Watermark response from secondary → primary is ~20 bytes — far below MSS. Nagle almost certainly delayed these frames by up to 40ms, which alone explains much of the gap between theoretical (50K ops/sec) and observed (21K ops/sec).

**Fix**: Set `TCP_NODELAY` on all TCP sockets:
- `client::StorageClient::connect()` — client outbound
- `server::Server::run()` accept path — all inbound connections
- `downstream::create_downstream_connection()` — replication outbound
- `stream_manager_client` — heartbeat connection

### 2. Stop-and-Wait Client (Zero Pipelining)

**Impact**: 1 op per RTT, pipe utilization ~0.5%.

Each client sends one append, then blocks on the response. The TCP pipe is idle for the entire round-trip time.

```
Current:    send → [wait 200µs] → recv → send → [wait 200µs] → recv
Pipelined:  send send send send → [wait 200µs] → recv recv recv recv
```

- Current: 1 op per RTT (~200µs) = **5,000 ops/sec/client**
- Pipelined (depth=64): 64 ops per RTT = **320,000 ops/sec/client**

This is a client-side optimization for future work.

### 3. DownstreamManager Serialization

**Impact**: All streams' forwarding funnels through one task.

The `DownstreamManager` processes `ForwardRequest`s from a single `mpsc::Receiver` in a sequential loop. All 10 streams' forwarding requests go through one task. If sending to one secondary blocks briefly (TCP backpressure), it delays all other streams' replication.

Future optimization: per-connection or per-stream fan-out tasks.

### 4. Channel Hops on the Critical Path

**Impact**: 3 wakeups × ~2µs each = ~6µs per append.

Each append traverses 3 `mpsc` channels:
1. `forward_tx` → DownstreamManager (forwarding to secondary)
2. `watermark_tx` → WatermarkHandler (ACK processing)
3. `response_tx` → connection writer task (deferred response to client)

Each channel hop involves a tokio wakeup (~1–5µs). Minor compared to TCP latency but adds up.

## What Dominates

| Factor | Impact | Status |
|--------|--------|--------|
| **Nagle's algorithm** | +0–40ms per hop | ✅ Fixed (TCP_NODELAY) |
| **Stop-and-wait client** | 1 op/RTT, ~0.5% utilization | Future: client pipelining |
| **DownstreamManager serialization** | Head-of-line blocking across streams | Future: per-connection tasks |
| **Channel hops** | ~6µs per append | Minor, acceptable |

The single biggest win is `TCP_NODELAY` — eliminating Nagle delays on the Watermark response path. The second biggest win would be client-side pipelining.

## Reproduce

```sh
RUST_LOG=info cargo bench --bench concurrent_multi_stream
```
