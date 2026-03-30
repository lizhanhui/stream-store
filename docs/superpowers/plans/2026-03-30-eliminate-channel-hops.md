# Eliminate Channel Hops in Replication Path — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce P99 append latency by eliminating 3 of 4 async channel hops in the broadcast replication critical path — replacing the DownstreamManager task and WatermarkHandler task with direct inline calls.

**Architecture:** Replace the `forward_tx → DownstreamManager → per_connection_tx → connection_writer` chain with a `DownstreamPool` that the store's append leader writes to directly. Replace the `watermark_tx → WatermarkHandler` chain by having the downstream reader call `drain_quorum()` on the store's AckQueue directly. The only remaining channel is `response_tx` (server write task → client), which is structurally necessary.

**Tech Stack:** Rust, Tokio, tokio-util FramedWrite, DashMap, Arc/Mutex

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `components/extent-node/src/downstream.rs` | **Rewrite** | `DownstreamPool`: shared pool of per-address TCP writers. Sync API for store to call directly. Reader tasks call `drain_quorum` inline. |
| `components/extent-node/src/store.rs` | **Modify** | Replace `forward_tx: Option<mpsc::Sender<ForwardRequest>>` with `downstream: Option<Arc<DownstreamPool>>`. Call `pool.forward()` directly in append path. |
| `components/extent-node/src/watermark.rs` | **Delete** | No longer needed — watermark handling is inlined into downstream reader. |
| `components/extent-node/src/lib.rs` | **Modify** | Remove channel creation. Wire `DownstreamPool` and pass `Arc<ExtentNodeStore>` to it. Remove WatermarkHandler spawn. |

---

### Task 1: Create DownstreamPool with direct forward API

**Files:**
- Rewrite: `components/extent-node/src/downstream.rs`

This replaces the entire DownstreamManager task with a shared pool struct. The key change: instead of receiving ForwardRequests from a channel, the pool exposes a `forward()` method that the store calls directly.

- [ ] **Step 1: Write the new DownstreamPool**

Replace the full content of `components/extent-node/src/downstream.rs` with:

```rust
//! DownstreamPool: shared pool of per-address TCP connections to secondary ExtentNodes.
//!
//! Replaces the channel-based DownstreamManager. The store's append leader calls
//! `pool.forward()` directly, eliminating 2 channel hops (forward_tx → DownstreamManager
//! → per_connection_tx → connection_writer).
//!
//! Each secondary gets its own `FramedWrite` behind an `Arc<Mutex<>>`, and its own
//! reader task that processes Watermark ACKs inline (calling `drain_quorum` on the store
//! directly, eliminating the WatermarkHandler channel hop).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{error, info, warn};

use common::types::{Opcode, StreamId};
use rpc::codec::FrameCodec;
use rpc::frame::Frame;

use crate::store::ExtentNodeStore;

/// A pool of TCP connections to secondary ExtentNodes, keyed by address.
///
/// Thread-safe: the outer HashMap is behind a Mutex for connection creation,
/// and each connection's writer is behind its own Mutex for concurrent writes.
pub struct DownstreamPool {
    /// Per-address write half, lazily created on first forward.
    connections: Mutex<HashMap<String, Arc<Mutex<FramedWrite<tokio::net::tcp::OwnedWriteHalf, FrameCodec>>>>>,
    /// Reference to the store for inline watermark handling.
    store: Arc<ExtentNodeStore>,
}

impl DownstreamPool {
    /// Create a new empty pool.
    pub fn new(store: Arc<ExtentNodeStore>) -> Self {
        Self {
            connections: Mutex::new(HashMap::new()),
            store,
        }
    }

    /// Forward a frame to a secondary. Creates the TCP connection lazily on first use.
    ///
    /// Called directly by the store's append leader — no channel hop.
    pub async fn forward(&self, addr: &str, frame: Frame) {
        let writer = self.get_or_create_writer(addr).await;
        let Some(writer) = writer else {
            warn!("failed to connect to secondary {addr}; frame dropped");
            return;
        };

        let mut w = writer.lock().await;
        if let Err(e) = w.feed(frame.clone()).await {
            warn!("send to secondary {addr} failed: {e}; reconnecting");
            drop(w);
            // Remove stale connection and retry once.
            self.remove_connection(addr).await;
            let new_writer = self.create_connection(addr).await;
            if let Some(nw) = new_writer {
                let mut w = nw.lock().await;
                if let Err(e) = w.send(frame).await {
                    warn!("retry send to {addr} failed: {e}; giving up");
                    drop(w);
                    self.remove_connection(addr).await;
                }
            }
            return;
        }
        if let Err(e) = w.flush().await {
            warn!("flush to secondary {addr} failed: {e}; reconnecting");
            drop(w);
            self.remove_connection(addr).await;
        }
    }

    /// Forward a batch of frames to a secondary with a single flush.
    ///
    /// Called by the store's batch append leader to amortize flush syscalls.
    pub async fn forward_batch(&self, addr: &str, frames: &[Frame]) {
        if frames.is_empty() {
            return;
        }
        let writer = self.get_or_create_writer(addr).await;
        let Some(writer) = writer else {
            warn!("failed to connect to secondary {addr}; {} frames dropped", frames.len());
            return;
        };

        let mut w = writer.lock().await;
        for frame in frames {
            if let Err(e) = w.feed(frame.clone()).await {
                warn!("send to secondary {addr} failed: {e}; {} frames dropped", frames.len());
                drop(w);
                self.remove_connection(addr).await;
                return;
            }
        }
        if let Err(e) = w.flush().await {
            warn!("flush to secondary {addr} failed: {e}; reconnecting");
            drop(w);
            self.remove_connection(addr).await;
        }
    }

    /// Get an existing writer or create a new connection.
    async fn get_or_create_writer(
        &self,
        addr: &str,
    ) -> Option<Arc<Mutex<FramedWrite<tokio::net::tcp::OwnedWriteHalf, FrameCodec>>>> {
        // Fast path: check if connection exists.
        {
            let conns = self.connections.lock().await;
            if let Some(writer) = conns.get(addr) {
                return Some(Arc::clone(writer));
            }
        }
        // Slow path: create connection.
        self.create_connection(addr).await
    }

    /// Create a new TCP connection and spawn its reader task.
    async fn create_connection(
        &self,
        addr: &str,
    ) -> Option<Arc<Mutex<FramedWrite<tokio::net::tcp::OwnedWriteHalf, FrameCodec>>>> {
        let stream = match TcpStream::connect(addr).await {
            Ok(s) => s,
            Err(e) => {
                error!("failed to connect to secondary {addr}: {e}");
                return None;
            }
        };

        stream.set_nodelay(true).ok();

        // Set TCP keepalive to detect half-open connections.
        let sock_ref = socket2::SockRef::from(&stream);
        let keepalive = socket2::TcpKeepalive::new()
            .with_time(Duration::from_secs(10))
            .with_interval(Duration::from_secs(5));
        let _ = sock_ref.set_tcp_keepalive(&keepalive);

        let (read_half, write_half) = stream.into_split();
        let writer = Arc::new(Mutex::new(FramedWrite::new(write_half, FrameCodec)));

        // Insert into connection map.
        {
            let mut conns = self.connections.lock().await;
            conns.insert(addr.to_string(), Arc::clone(&writer));
        }

        // Spawn reader task that handles watermarks inline.
        let store = Arc::clone(&self.store);
        let addr_owned = addr.to_string();
        tokio::spawn(async move {
            Self::downstream_reader(addr_owned, read_half, store).await;
        });

        info!("connected to secondary ExtentNode at {addr}");
        Some(writer)
    }

    /// Remove a connection from the pool (e.g., after send failure).
    async fn remove_connection(&self, addr: &str) {
        let mut conns = self.connections.lock().await;
        conns.remove(addr);
    }

    /// Reader task for a single secondary connection.
    ///
    /// Reads cumulative Watermark ACKs and handles them **inline** by calling
    /// `drain_quorum()` on the store's AckQueue directly — no channel hop.
    async fn downstream_reader(
        addr: String,
        read_half: tokio::net::tcp::OwnedReadHalf,
        store: Arc<ExtentNodeStore>,
    ) {
        let mut framed_read = FramedRead::new(read_half, FrameCodec);

        while let Some(result) = framed_read.next().await {
            match result {
                Ok(frame) => {
                    if frame.opcode() == Opcode::Watermark {
                        let stream_id = frame.stream_id();
                        let acked_offset = frame.offset().0;

                        // Inline watermark handling — replaces WatermarkHandler channel hop.
                        if let Some(mut ack_queue) = store.ack_queues.get_mut(&stream_id) {
                            ack_queue.ack_from_secondary(&addr, acked_offset);
                            ack_queue.drain_quorum();
                        } else {
                            warn!(
                                "received watermark for stream {:?} but no ack_queue exists",
                                stream_id
                            );
                        }
                    } else {
                        warn!(
                            "unexpected opcode {:?} from secondary {addr}",
                            frame.opcode()
                        );
                    }
                }
                Err(e) => {
                    error!("secondary {addr} read error: {e}");
                    return;
                }
            }
        }

        info!("secondary {addr} reader closed");
    }
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo check -p extent-node`
Expected: Compilation errors about missing `store.forward_tx` usages — that's fine, we'll fix in Task 2.

- [ ] **Step 3: Commit**

```bash
git add components/extent-node/src/downstream.rs
git commit -m "feat(downstream): replace DownstreamManager with direct DownstreamPool"
```

---

### Task 2: Wire DownstreamPool into ExtentNodeStore, remove forward_tx

**Files:**
- Modify: `components/extent-node/src/store.rs`

Replace `forward_tx: Option<mpsc::Sender<ForwardRequest>>` with `downstream: Option<Arc<DownstreamPool>>`. Update the two forward sites (single append at ~line 746 and batch append at ~line 1130) to call `pool.forward()` directly.

- [ ] **Step 1: Update ExtentNodeStore struct and constructors**

In `store.rs`, make these changes:

1. Add import at the top:
```rust
use crate::downstream::DownstreamPool;
```

2. In the `ExtentNodeStore` struct, replace:
```rust
    forward_tx: Option<mpsc::Sender<ForwardRequest>>,
```
with:
```rust
    downstream: Option<Arc<DownstreamPool>>,
```

3. In `ExtentNodeStore::new()`, replace:
```rust
            forward_tx: None,
```
with:
```rust
            downstream: None,
```

4. Replace `with_forward_tx` method with:
```rust
    /// Create a new store with broadcast replication support.
    pub fn with_downstream(downstream: Arc<DownstreamPool>) -> Self {
        Self {
            streams: DashMap::new(),
            next_stream_id: AtomicU64::new(1),
            arena_capacity: DEFAULT_ARENA_CAPACITY,
            replicas: DashMap::new(),
            downstream: Some(downstream),
            seal_tx: None,
            ack_queues: DashMap::new(),
            append_count: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
        }
    }
```

- [ ] **Step 2: Update single-append forward path (~line 746)**

Replace the `forward_tx.try_send` block:
```rust
                    if let Some(ref tx) = self.forward_tx {
                        for secondary_addr in &ri.replica_addrs {
                            let req = ForwardRequest { ... };
                            if let Err(e) = tx.try_send(req) { ... }
                        }
                    }
```
with direct pool calls:
```rust
                    if let Some(ref pool) = self.downstream {
                        for secondary_addr in &ri.replica_addrs {
                            let frame = Frame::new(
                                VariableHeader::Forward {
                                    stream_id,
                                    extent_id,
                                    start_offset: Offset(extent_start_offset),
                                    offset,
                                    byte_pos: append_result.byte_pos,
                                },
                                Some(payload_for_forward.clone()),
                            );
                            pool.forward(secondary_addr, frame).await;
                        }
                    }
```

- [ ] **Step 3: Update batch-append forward path (~line 1130)**

Replace the batch `forward_tx.try_send` block with direct pool batch calls. Group entries by secondary address and call `pool.forward_batch()`:
```rust
                    if let Some(ref pool) = self.downstream {
                        for secondary_addr in &ri.replica_addrs {
                            let frames: Vec<Frame> = entries
                                .iter()
                                .map(|e| {
                                    Frame::new(
                                        VariableHeader::Forward {
                                            stream_id,
                                            extent_id,
                                            start_offset: Offset(extent_start_offset),
                                            offset: Offset(e.offset),
                                            byte_pos: e.byte_pos,
                                        },
                                        Some(e.payload.clone()),
                                    )
                                })
                                .collect();
                            pool.forward_batch(secondary_addr, &frames).await;
                        }
                    }
```

- [ ] **Step 4: Remove ForwardRequest struct**

The `ForwardRequest` struct is no longer needed — frames are constructed inline. Remove it from `store.rs`. Keep `WatermarkEvent` for now if referenced elsewhere, or remove if unused.

- [ ] **Step 5: Update unit test `primary_append_defers_and_broadcasts`**

The test at ~line 1810 creates a `forward_tx` channel and checks that ForwardRequests arrive. Update it to use `DownstreamPool` instead. Since the test needs to verify forwarding happened, the simplest approach is to start a mock TCP listener that the pool connects to, then assert frames arrive.

Alternatively, keep the test simpler by testing the DownstreamPool in a dedicated test that verifies frame delivery over TCP.

- [ ] **Step 6: Verify it compiles**

Run: `cargo check -p extent-node`
Expected: compiles. May have warnings about unused `WatermarkEvent` or `ForwardRequest` — clean them up.

- [ ] **Step 7: Commit**

```bash
git add components/extent-node/src/store.rs
git commit -m "refactor(store): replace forward_tx channel with direct DownstreamPool calls"
```

---

### Task 3: Remove WatermarkHandler, update lib.rs wiring

**Files:**
- Delete: `components/extent-node/src/watermark.rs`
- Modify: `components/extent-node/src/lib.rs`

- [ ] **Step 1: Delete watermark.rs**

```bash
rm components/extent-node/src/watermark.rs
```

- [ ] **Step 2: Remove watermark module declaration from lib.rs**

In `components/extent-node/src/lib.rs`, remove:
```rust
pub mod watermark;
```

- [ ] **Step 3: Rewrite the channel wiring in ExtentNode::start()**

Replace the current channel setup and task spawns with the new DownstreamPool wiring. The key changes:

1. Remove `forward_tx`/`forward_rx` channel creation.
2. Remove `watermark_tx`/`watermark_rx` channel creation.
3. Create the store first, then create `DownstreamPool` with `Arc<ExtentNodeStore>`.
4. Remove the DownstreamManager and WatermarkHandler spawns.

Replace the body of `ExtentNode::start()` with:

```rust
    pub async fn start(config: ExtentNodeConfig) -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);
        let mut task_handles = Vec::new();

        // Bind listener and resolve the OS-assigned address.
        let listener = TcpListener::bind(&config.listen_addr())
            .await
            .expect("failed to bind ExtentNode listener");
        let local_addr = listener
            .local_addr()
            .expect("failed to get ExtentNode local address");
        info!("ExtentNode server bound on {local_addr}");

        // Create store with configurable arena capacity.
        let mut store_inner = ExtentNodeStore::new();
        store_inner.set_arena_capacity(config.extent_arena_capacity);
        let store = Arc::new(store_inner);

        // Create DownstreamPool for broadcast replication (direct calls, no channels).
        let downstream = Arc::new(downstream::DownstreamPool::new(Arc::clone(&store)));
        store.set_downstream(Arc::clone(&downstream));

        // Resolve advertise_addr and node_id.
        let effective_port = if config.port == 0 {
            local_addr.port()
        } else {
            config.port
        };
        let effective_ip = resolve_advertise_ip(&config.bind_ip, &config.advertise_ip);
        let advertise_addr = format!("{effective_ip}:{effective_port}");

        let node_id = if config.node_id.is_empty() {
            advertise_addr.clone()
        } else {
            config.node_id.clone()
        };

        // Spawn StreamManagerClient (RAII: sends Disconnect when dropped).
        let stream_manager_client = StreamManagerClient::spawn(
            Arc::clone(&store),
            node_id,
            advertise_addr,
            config.stream_manager_addr.clone(),
            config.heartbeat_interval_ms,
        );

        // Spawn accept loop.
        let server_shutdown = shutdown_tx.subscribe();
        task_handles.push(tokio::spawn(async move {
            server::Server::builder("ExtentNode")
                .listener(listener)
                .handler(store)
                .deferred(true)
                .shutdown(server_shutdown)
                .build()
                .run()
                .await;
        }));

        ExtentNode {
            addr: local_addr,
            shutdown_tx,
            task_handles,
            stream_manager_client,
        }
    }
```

Note: this requires a `set_downstream()` method on `ExtentNodeStore` (similar to `set_seal_tx`).

- [ ] **Step 4: Add `set_downstream` to ExtentNodeStore**

In `store.rs`, add:
```rust
    /// Set the downstream pool (called during ExtentNode bootstrap).
    pub fn set_downstream(&self, downstream: Arc<DownstreamPool>) {
        // SAFETY: called once at startup before any requests.
        // We need interior mutability since store is already in Arc.
        // Use the same pattern as forward_tx but with Arc.
    }
```

Actually, since `ExtentNodeStore` is already behind `Arc` when `set_downstream` would be called, and `downstream` is `Option<Arc<DownstreamPool>>`, we need to handle this differently. The cleanest approach: change `downstream` to use `once_cell::sync::OnceCell` or simply initialize it in the constructor. Since there's a circular dependency (Pool needs Store, Store needs Pool), use `OnceCell`:

In `store.rs`, change:
```rust
    downstream: Option<Arc<DownstreamPool>>,
```
to:
```rust
    downstream: std::sync::OnceLock<Arc<DownstreamPool>>,
```

And update the `set_downstream` method:
```rust
    pub fn set_downstream(&self, downstream: Arc<DownstreamPool>) {
        self.downstream.set(downstream).expect("downstream already set");
    }
```

And update the forward callsites to use `self.downstream.get()` instead of `self.downstream.as_ref()`.

- [ ] **Step 5: Remove unused imports**

Remove unused channel imports from `lib.rs`:
```rust
use tokio::sync::mpsc;  // remove if no longer needed
```

Clean up `store.rs`: remove `ForwardRequest` if unused, remove `WatermarkEvent` if unused (watermark handling is now inline in downstream.rs).

- [ ] **Step 6: Verify it compiles**

Run: `cargo check --workspace --tests --benches --examples`
Expected: compiles with no errors.

- [ ] **Step 7: Run tests**

Run: `cargo test --workspace --lib --test phase2b_integration`
Expected: all tests pass. The phase2b integration tests exercise RF=2 replication and must still work.

- [ ] **Step 8: Commit**

```bash
git add components/extent-node/src/lib.rs components/extent-node/src/store.rs
git rm components/extent-node/src/watermark.rs
git commit -m "refactor: eliminate 3 channel hops — inline downstream + watermark handling"
```

---

### Task 4: Clean up and verify

**Files:**
- Modify: `components/extent-node/src/store.rs` (remove dead code)
- Modify: `components/extent-node/src/downstream.rs` (polish)

- [ ] **Step 1: Remove ForwardRequest and WatermarkEvent structs if unused**

Search for all remaining references:
```bash
grep -rn 'ForwardRequest\|WatermarkEvent' components/extent-node/src/
```

Remove the structs and any lingering references.

- [ ] **Step 2: Full workspace build**

Run: `cargo build --workspace`
Expected: builds cleanly.

- [ ] **Step 3: Run all tests**

Run: `cargo test --workspace --lib --test phase2b_integration`
Expected: all tests pass.

- [ ] **Step 4: Run the cluster benchmark** (if MySQL is available)

Run: `cargo bench --bench pipeline_append_cluster`
Expected: P99 latency should drop from ~1.5ms toward ~1.0-1.1ms.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "chore: remove dead ForwardRequest/WatermarkEvent types"
```

---

## Verification

1. `cargo build --workspace` — full workspace compiles
2. `cargo test --workspace --lib` — all unit tests pass (85+)
3. `cargo test --test phase2b_integration` — RF=2 replication integration tests pass
4. `cargo bench --bench pipeline_append_cluster` — P99 latency improved (target: ~1.0-1.1ms)
5. No remaining references to `ForwardRequest`, `WatermarkEvent`, `run_downstream_manager`, or `run_watermark_handler`
