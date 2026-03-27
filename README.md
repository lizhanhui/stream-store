# Stream Store

A high-performance, replicated in-memory message storage system built in Rust, designed to replace cloud block-based disk storage with S3-compatible object storage while serving hot data from a replicated in-memory layer.

## Motivation

Traditional message brokers rely on cloud block storage for durability. Block storage is expensive at scale. Object storage (S3) is orders of magnitude cheaper per GB but has higher latency and lower IOPS. Stream Store bridges this gap by maintaining a **replicated in-memory layer** for hot data and flushing to S3 for cold storage, inspired by the [Windows Azure Storage stream layer](https://dl.acm.org/doi/10.1145/3297858.3304053).

## Key Highlights

### Lock-Free In-Memory Arena

The core storage engine uses a **pre-allocated contiguous buffer** with fully lock-free concurrent append:

- Multiple writers reserve slots via `AtomicU64::fetch_add` and write into non-overlapping regions in parallel.
- **In-order commit** uses spin-wait CAS (same pattern as Linux io_uring and LMAX Disruptor) -- typically nanoseconds per message.
- Internal compressed index (`AtomicU32` pointers) enables **O(1) random reads** at ~950M lookups/sec.
- **Zero-copy reads** via `Bytes::slice` into the arena buffer.
- **Zero-copy S3 flush** -- arena bytes are already in wire format.

Micro-benchmark: ~230M appends/sec single-threaded, ~10ns per append.

### Broadcast Replication

Configurable replication factor (default RF=2) with quorum-based durability:

- **Primary** is the sole append acceptor, assigning monotonic sequence numbers and broadcasting to all Secondaries in parallel (O(1) hop latency).
- **Secondaries** return cumulative watermark ACKs.
- **Quorum ACK**: Primary waits for a quorum (itself + `RF/2` secondaries) before ACKing clients, tolerating minority failures.
- **Deferred ACK** via per-connection channels for efficient async notification.

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
  │  Client  │────────┤   │    Stream    │───────►│    Extent        │  ├──►│  S3    │
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
                    watermark ACK
```

### Process Types

- **Extent Node** -- Holds in-memory extent replicas, participates in broadcast replication, serves APPEND/READ requests.
- **Stream Manager** -- Metadata coordinator managing stream-to-extent mappings, orchestrating seal-and-new, persisting metadata to MySQL. Includes load-aware extent placement and heartbeat-based failure detection.

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
│   ├── client/                     # StorageClient for Extent Node & Stream Manager
│   ├── extent-node/                # Lock-free arena, stream, replication, watermark
│   └── stream-manager/             # Metadata store, allocator, heartbeat checker
├── tests/                          # Integration tests
├── benches/                        # End-to-end benchmarks
├── examples/                       # Client usage example
└── docs/                           # Design docs and reference papers
```

## Getting Started

### Prerequisites

- Rust 2024 edition (1.85+)
- MySQL 8.0+ (for Stream Manager metadata)

### Build

```bash
cargo build --release
```

### Run

```bash
# Start Stream Manager (requires MySQL)
cargo run --release --bin stream-manager

# Start Extent Node(s)
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
| 3 | S3 flush/read, S3 Reader/Flusher, LRU read cache | Planned |
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
