# Stream Store

A high-performance, replicated in-memory streaming storage system built in Rust, designed to replace cloud block-based disk storage with S3-compatible object storage while serving hot data from a replicated in-memory layer.

## Motivation

Traditional message brokers rely on cloud block storage for durability. Block storage is expensive at scale. Object storage (S3) is orders of magnitude cheaper per GB but has higher latency and lower IOPS. Stream Store bridges this gap by maintaining a **replicated in-memory layer** for hot data and flushing to S3 for cold storage, inspired by the [Windows Azure Storage](https://dl.acm.org/doi/10.1145/3297858.3304053).

## Key Highlights

### Pipelined Group Commit

The core storage engine uses a **pre-allocated contiguous arena** with a **pipelined group commit** pattern:

- **Leader election** via `AtomicU64::fetch_add` — the first writer becomes the active writer (leader); concurrent writers delegate their payloads to the leader via a lock-free channel.
- **Single-writer append** — the leader uses plain `load`/`store` on cursors (no contention, no spin-wait), then batch-drains all queued follower payloads, amortizing synchronization cost across the group.
- **No cache-line bouncing** — followers push to an unbounded channel and return immediately; only the leader touches arena cursors.
- Internal compressed index (`AtomicU32` pointers) enables **O(1) random reads** at ~950M lookups/sec.
- **Zero-copy reads** via `Bytes::slice` into the arena buffer.
- **Chunk-compressed S3 flush** -- sealed extents are encoded with sparse index and independently compressible 64-record chunks (zstd/lz4) for random-access S3 range reads.

Micro-benchmark: ~230M appends/sec single-threaded, ~10ns per append.

### Broadcast Replication

Configurable replication factor (default RF=2) with quorum-based durability:

- **Primary** is the sole append acceptor, assigning monotonic sequence numbers and broadcasting to all Secondaries in parallel (O(1) hop latency).
- **Secondaries** return cumulative watermark ACKs.
- **Quorum ACK**: Primary waits for a quorum (itself + `RF/2` secondaries) before ACKing clients, tolerating minority failures.
- **Deferred ACK** via per-connection channels for efficient async notification.
- **Lock-free append hot path** -- the entire append path (stream lookup, epoch check, leader election, arena write, forward to secondaries, ACK queue enqueue) executes without any Mutex. Key techniques:
  - `papaya::HashMap` (epoch-based reclamation) for lock-free stream/replica lookups
  - `parking_lot::RwLock` on `Stream` internals -- read lock (~1 uncontended atomic) for append, write lock only for rare lifecycle ops (seal, register)
  - `Arc<ReplicaInfo>` for zero-copy replica info access (one atomic refcount vs Mutex + deep clone)
  - `crossbeam_channel` for lock-free PendingAck handoff from append leader to watermark reader
  - Fixed `[u64; MAX_SECONDARIES]` array with pre-resolved `u8` secondary indices for zero-allocation watermark processing

### Seal-and-New

Inspired by WAS, extents transition through three states: **Active -> Sealed -> Flushed**.

- **Consistency** is resolved on the sealed extent (backward-looking).
- **Availability** is provided by the new extent (forward-looking).
- The system never blocks writes to achieve consistency.
- Triggers include size threshold, time interval, node failure, or arena-full.

### Why Rust?

The storage service runs as a dedicated Rust process:

- **No GC pauses** -- gigabytes of in-memory message data would cause GC stop-the-world events stalling replication ACKs.
- **Zero-copy I/O** -- `bytes::Bytes` reference-counted buffers enable zero-copy broadcast replication.
- **Precise memory control** -- hard memory budget enforced without VM overhead.

### Custom TCP Wire Protocol

A binary protocol with an 8-byte fixed header (Magic | Version | Opcode | Flags | RemainingLength) followed by opcode-specific variable headers and length-prefixed payloads. 25+ opcodes covering data path (APPEND, READ, SEAL), lifecycle (CONNECT, HEARTBEAT, REGISTER_EXTENT, WATERMARK), and cluster management.

## Architecture

```
                      ┌─────────────────────────────────────────────────┐
                      │               Stream Store (Rust)               │
                      │                                                 │
  ┌──────────┐  TCP   │   ┌──────────────┐        ┌──────────────────┐  │   ┌────────┐
  │          │────────┤   │              │  alloc │                  │  │   │        │
  │  Client  │────────┤   │   Stream     │───────►│    Extent        │  ├──►│  S3    │
  │          │  TCP   │   │   Manager    │  seal  │    Node(s)       │  │   │ (cold) │
  └──────────┘        │   │   (MySQL)    │        │   (in-memory,    │  │   │        │
                      │   └──────────────┘        │    replicated)   │  │   └────────┘
                      │     metadata              └──────────────────┘  │
                      │    control plane               data plane       │
                      └─────────────────────────────────────────────────┘
```

- **Client -> Stream Manager**: Metadata operations (create/describe streams, seal extents)
- **Client -> Extent Node**: Data operations (append, read)
- **Stream Manager -> Extent Node**: Extent allocation, seal commands, heartbeat monitoring

```
  Broadcast Replication (RF=2):

       ┌─────────────┐   broadcast    ┌─────────────┐
       │  ExtentNode │───────────────►│  ExtentNode │
       │  (Primary)  │                │ (Secondary) │
       └─────────────┘                └──────┬──────┘
              ◄──────────────────────────────┘
                    watermark ACK               │
                                          S3 Flusher
                                                │
                                          ┌─────▼─────┐
                                          │  S3 Bucket │
                                          └───────────┘
```

### Process Types

- **Extent Node** -- Holds in-memory extent replicas, participates in broadcast replication, serves APPEND/READ requests. Secondary-1 runs background S3 flusher for sealed extents.
- **Stream Manager** -- Stateless metadata coordinator managing stream-to-extent mappings, orchestrating seal-and-new, persisting metadata to MySQL. Fully stateless design (no in-memory caches) allows multiple SM nodes to run against the same database for high availability. Includes load-aware extent placement, heartbeat-based failure detection, and DB-based leadership lease for failover coordination.

## Performance

### Micro-benchmark (single-node, no replication)

Lock-free arena with pipelined group commit:

- **~230M appends/sec** single-threaded (~10ns per append)
- **~950M index lookups/sec** via compressed `AtomicU32` pointer index

### End-to-end (RF=2, broadcast replication)

Pipeline append benchmark: 4 senders, 16 in-flight per sender, 1 KiB payload, RF=2, 8x extent growth.

| Metric | Value |
|--------|-------|
| Throughput | **~200k ops/s (~200 MB/s)** |
| Avg latency | **~150 us** |
| p99 latency | **~240 us** |
| p99.9 latency | **~280 us** |

The append hot path is **Mutex-free**: stream lookup (`papaya` pin), epoch check (`AtomicU32`), leader election (`AtomicU64`), arena write (atomic cursors), forward to secondaries (`tokio::mpsc::try_send`), ACK queue enqueue (`crossbeam_channel`), and in-flight decrement (`AtomicU64`) all execute without any Mutex acquisition. The only synchronization on the critical path is uncontended `parking_lot::RwLock` read locks (~1 atomic each).

At 200k ops/s with RF=2, the system is **pipeline-depth-bound** (Little's Law: 64 in-flight / 150us RTT), not CPU-bound. Profiling shows **52% kernel time** dominated by TCP syscalls (`writev`, `recvfrom`), confirming the application layer is no longer the bottleneck.

## Project Structure

```
stream-store/
├── src/bin/
│   ├── extent-node.rs              # Extent Node binary
│   └── stream-manager.rs           # Stream Manager binary
├── components/
│   ├── common/                     # Shared types, config, errors
│   ├── rpc/                        # Custom TCP wire protocol (frame, codec, payload)
│   ├── server/                     # Server infrastructure (RequestHandler, ServerBuilder)
│   ├── client/                     # StreamClient for Extent Node & Stream Manager
│   ├── extent-node/                # Pipelined group commit arena, stream, replication,
│   │                               # S3 codec (chunk-compressed), S3 flusher, watermark
│   └── stream-manager/             # Metadata store, allocator, heartbeat checker
├── conf/                           # Example TOML configuration files
├── tests/                          # Integration tests
├── benches/                        # End-to-end benchmarks
├── examples/                       # Client usage example
└── docs/                           # Design docs and reference papers
```

## Getting Started

### Prerequisites

- Rust 2024 edition (1.85+)
- MySQL 8.0+ (for Stream Manager metadata)
- S3-compatible object storage (optional, for extent flush -- AWS S3, MinIO, etc.)

### Build

```bash
cargo build --release
```

### Run

Both binaries accept an optional `--config <path>` flag to load a TOML configuration file. Missing fields fall back to defaults.

```bash
# Start Stream Manager (requires MySQL)
cargo run --release --bin stream-manager -- --config conf/stream-manager.toml

# Start Extent Node(s)
cargo run --release --bin extent-node -- --config conf/extent-node.toml

# Or use defaults (no config file needed)
cargo run --release --bin stream-manager
cargo run --release --bin extent-node
```

### Run Tests

```bash
cargo test
```

### Run Benchmarks

```bash
cargo bench
```

## Implementation Status

| Phase | Description | Status |
|-------|-------------|--------|
| 1 | Single-node with lock-free extent, APPEND/READ/QUERY_OFFSET | Done |
| 2 | Broadcast replication, quorum ACK, Stream Manager (MySQL), seal-and-new | Done |
| 2.5 | Stateless multi-active SM with DB-based leader lease, CAS-fenced failover | Done |
| 2b | Lock-free hot-path: papaya HashMap, AckQueue producer/consumer split, Arc\<ReplicaInfo\> | Done |
| 3 | S3 flush: chunk-compressed codec (zstd/lz4), background flusher on Secondary-1, UpdateExtentFlushed notification | In Progress |
| 4 | Multi-Dispatch (data + index streams) | Planned |

## Contributing

Contributions are welcome! This project is under active development. Here's how you can help:

- **Bug reports** -- Open an issue describing the problem, expected behavior, and steps to reproduce.
- **Feature requests** -- Open an issue with a clear description of the proposed feature and its use case.
- **Pull requests** -- Fork the repository, create a feature branch, and submit a PR. Please include tests for new functionality and ensure existing tests pass.

Before contributing, please read [docs/design.md](docs/design.md) for the full architectural design and [docs/issues.md](docs/issues.md) for known issues and the improvement backlog.

### Development Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/lizhanhui/stream-store.git
   cd stream-store
   ```

2. Build and run tests:
   ```bash
   cargo build && cargo test
   ```

3. Format and lint:
   ```bash
   cargo fmt && cargo clippy
   ```

## License

TODO

## References

- [Windows Azure Storage (WAS)](https://dl.acm.org/doi/10.1145/3297858.3304053) -- SOSP 2018
- [The Tail at Scale](https://cacm.acm.org/magazines/2013/2/160173-the-tail-at-scale/fulltext) -- CACM 2013
- Additional papers available in [docs/](docs/)
