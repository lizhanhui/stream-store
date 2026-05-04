# Move Append Hot Path onto Stream — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move `do_append_and_respond`, `drain_follower_jobs`, and `maybe_build_init_forward` from `ExtentNodeStore` onto `Stream`. Collapse the Store's `replicas: HashMap<StreamId, Arc<ReplicaInfo>>` and the append/bytes counters into per-stream state, so the hot path is a single `papaya::HashMap::get → Arc<Stream>` lookup followed by pure per-stream method calls — no further map lookups, no EN-wide atomics visited on the append path.

**Why.** The drain-follower loop currently re-acquires the `streams` pin guard four times per iteration because papaya guards are `!Send` across `.await`. `do_append_and_respond` does one more `replicas` map lookup per call. Moving these onto `Stream` eliminates both: the drain owns an `Arc<Stream>` for the whole loop (no guard dance), and replica info is on the Stream struct directly (no second lookup). EN-wide `append_count` / `bytes_written` counters become an `Arc<StoreMetrics>` shared across every Stream, so the aggregate heartbeat view is unchanged.

**Architecture.** Three structural changes on the ExtentNode:

1. **`StoreMetrics`** (new): `append_count`, `bytes_written` move into `Arc<StoreMetrics>`. The Store and every Stream hold a clone; increments happen on the Stream, reads happen on the Store (heartbeat).
2. **`Stream.replica_info: ArcSwap<Option<Arc<ReplicaInfo>>>`** (new): ReplicaInfo is immutable within an epoch; `ArcSwap` matches today's per-epoch-immutable shape and lets the hot path do a single atomic load. `Store.replicas` is deleted.
3. **`streams: papaya::HashMap<StreamId, Arc<Stream>>`** (was `Stream`): required so `drain_follower_jobs` can hold an `Arc<Stream>` across `.await` points and so `Stream::handle_append` can be called without the caller holding the pin guard.

After these, the hot path structure is:

```
Store::handle_append
  ├── streams.pin().get(&id).cloned()  // one Arc bump
  └── stream.handle_append(frame, resp_tx).await
        ├── leader election on stream.in_flight
        ├── stream.append_one(...)     // ex do_append_and_respond
        └── stream.drain_follower_jobs(...) // no guard dance
```

**Scope boundaries.**
- Pure move/rename. No change to wire format, MySQL schema, external behaviour, or algorithm.
- `seal`, `forward` (replicate), `watermark`, `read`, and the S3 flush paths stay on `ExtentNodeStore` — they either need multi-stream access (seal, flush) or are cold enough that the lookup cost is irrelevant.
- No behavioural change to how `ReplicaInfo` mutates (still "immutable within an epoch", overwritten on `RegisterEpoch`). `ArcSwap::store` replaces `HashMap::insert`.

**Tech stack.** Rust 1.80+, existing deps (`papaya`, `arc-swap`, `crossbeam`, `parking_lot`). No new deps.

**Reference.** `docs/superpowers/specs/2026-04-24-shared-arena-design.md` for background; `docs/superpowers/plans/2026-05-04-complete-arena-pool-wiring.md` for the preceding refactor this plan builds on.

**Expected impact.**
- Drain-follower-loop lookups drop from 4 per iteration to 0.
- `do_append_and_respond`'s replicas lookup drops from 1 to 0.
- Net LOC: ~100 reduction (guard-dance cleanup).
- Expected benchmark win on `pipeline_append`: 5–15% at the pipelined-commit path. Bench is optional in this plan.

---

## File Structure

| File | Role |
|---|---|
| `components/extent-node/src/stream.rs` | Add `replica_info`, `metrics`, `replication_timeout` fields; add `handle_append`, `append_one`, `drain_follower_jobs`, `maybe_build_init_forward` methods; `Stream::new` grows a `StoreMetrics` + `Duration` param |
| `components/extent-node/src/store/mod.rs` | Add `pub(crate) struct StoreMetrics`; replace `append_count` / `bytes_written` / `replicas` fields with `metrics: Arc<StoreMetrics>`; switch `streams` value type to `Arc<Stream>`; pass `metrics` / timeout into `Stream::new` in `try_create_stream` |
| `components/extent-node/src/store/append.rs` | `handle_append` + `handle_append_batch_inner` become thin routers; `do_append_and_respond` / `drain_follower_jobs` / `maybe_build_init_forward` move out |
| `components/extent-node/src/store/forward.rs` | Uses `stream.metrics` instead of `self.append_count` / `self.bytes_written` |
| `components/extent-node/src/store/register.rs` | Calls `stream.set_replica_info(...)` instead of inserting into `self.replicas` |
| `components/extent-node/src/store/{seal,read,tests}.rs` | Call-site churn: `guard.get(&id)` returns `Option<&Arc<Stream>>`; use `stream.replica_info()` instead of `store.get_replica_info(id)` where present |
| `tests/*.rs`, `benches/*.rs`, `examples/client-example.rs` | Any direct `store.streams.pin()` iteration needs to deref `Arc<Stream>`; should be a no-op via auto-deref in most cases |

---

## Phase Order + Broken-Build Window

Six phases, ~6 commits, every phase leaves the tree green (no intentional breaks).

1. **Phase 0** — Inventory + baseline (no commit)
2. **Phase 1** — Introduce `StoreMetrics`; plumb it through Store + Stream (Stream gets new field; Store keeps `replicas` / timeout for now)
3. **Phase 2** — Move `replica_info` onto Stream; delete `Store.replicas`
4. **Phase 3** — Move `replication_timeout` onto Stream; Store keeps a field only for passing new streams the current value
5. **Phase 4** — Switch `streams` map value to `Arc<Stream>`
6. **Phase 5** — Move `do_append_and_respond` + `maybe_build_init_forward` + `drain_follower_jobs` onto Stream; Store's append handlers become thin routers
7. **Phase 6** — Validation + optional benchmark

---

## Phase 0: Inventory + Baseline

### Task 0.1: Baseline

**Files:** (read-only)

- [ ] **Step 1: Confirm build + test baseline**

Run: `RUSTFLAGS="-D warnings" cargo check --workspace --tests --benches 2>&1 | tail -3`
Expected: `Finished` clean.

Run: `cargo test --lib --workspace 2>&1 | grep -E '^test result:' | awk '{ p+=$4 } END { printf "passed=%d\n", p }'`
Expected: 132.

- [ ] **Step 2: Inventory the move surface**

Run:
```bash
cd /data/repo/stream-store
grep -rn 'self\.replicas\|\.replicas\.pin\|self\.append_count\|self\.bytes_written\|self\.replication_timeout' \
  components/extent-node/src/ | wc -l
grep -rn 'do_append_and_respond\|drain_follower_jobs\|maybe_build_init_forward' \
  components/extent-node/src/ | wc -l
grep -rn 'self\.streams\.pin()\|streams\.pin()\.get' \
  components/extent-node/src/ | wc -l
```

Record counts for Phase 6 sanity check.

No commit.

---

## Phase 1: Introduce `StoreMetrics`

Goal: extract the two append-side atomics into a shared struct that both `ExtentNodeStore` and `Stream` hold. No behavioural change; the increment sites just route through `self.metrics` instead of `self`.

### Task 1.1: Define `StoreMetrics`

**Files:**
- Modify: `components/extent-node/src/store/mod.rs`

- [ ] **Step 1: Add the struct**

At the top of `store/mod.rs` (above `ExtentNodeStore`):

```rust
/// EN-wide metrics counters, shared by every Stream.
/// Streams increment on each successful append/replicate; the Store
/// reads on heartbeat and resets.
pub(crate) struct StoreMetrics {
    pub(crate) append_count: AtomicU64,
    pub(crate) bytes_written: AtomicU64,
}

impl StoreMetrics {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            append_count: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
        })
    }
}
```

- [ ] **Step 2: Replace the two Store fields**

In `ExtentNodeStore`:

Before:
```rust
pub(crate) append_count: AtomicU64,
pub(crate) bytes_written: AtomicU64,
```

After:
```rust
pub(crate) metrics: Arc<StoreMetrics>,
```

- [ ] **Step 3: Update construction**

In `new_with_ids`, replace the two `AtomicU64::new(0)` initializers with `metrics: StoreMetrics::new()`.

- [ ] **Step 4: Update the heartbeat read site**

`store/mod.rs:230`-`231` currently do:
```rust
let appends = self.append_count.swap(0, Ordering::Relaxed);
let bytes = self.bytes_written.swap(0, Ordering::Relaxed);
```

Change to:
```rust
let appends = self.metrics.append_count.swap(0, Ordering::Relaxed);
let bytes = self.metrics.bytes_written.swap(0, Ordering::Relaxed);
```

- [ ] **Step 5: Update the increment sites in append.rs / forward.rs**

For each of the 4 increment sites listed in Phase 0 inventory, rewrite:
- `self.append_count.fetch_add(...)` → `self.metrics.append_count.fetch_add(...)`
- `self.bytes_written.fetch_add(...)` → `self.metrics.bytes_written.fetch_add(...)`

(Stream still doesn't hold a metrics handle yet; it's accessed off `self` inside `ExtentNodeStore` methods — that's fine, these methods all live on the Store in Phase 1.)

- [ ] **Step 6: Verify**

Run: `RUSTFLAGS="-D warnings" cargo check --workspace --tests --benches`
Expected: clean.

Run: `cargo test --lib --workspace`
Expected: 132 pass.

### Task 1.2: Give `Stream` its own metrics handle

**Files:**
- Modify: `components/extent-node/src/stream.rs`
- Modify: `components/extent-node/src/store/mod.rs`

- [ ] **Step 1: Add the field**

In `Stream`:

```rust
metrics: Arc<crate::store::StoreMetrics>,
```

- [ ] **Step 2: Grow `Stream::new`**

Signature becomes:

```rust
pub(crate) fn new(
    id: StreamId,
    arena_ids: Arc<ArenaIdGenerator>,
    pool: Arc<dyn ArenaPool>,
    metrics: Arc<crate::store::StoreMetrics>,
) -> Self
```

- [ ] **Step 3: Pass it in at `try_create_stream`**

In `store/mod.rs::try_create_stream`, when constructing the new `Stream`, pass `Arc::clone(&self.metrics)` as the final arg.

- [ ] **Step 4: Update test-helper callers**

`components/extent-node/src/stream.rs::tests::new_stream_with_epoch`, the 6 `Stream::new(...)` call sites inside `#[cfg(test)] mod tests`, and any other test helpers that build a `Stream` directly — each needs a fresh `StoreMetrics::new()` passed in.

Add inside the test module:
```rust
fn test_metrics() -> Arc<crate::store::StoreMetrics> {
    crate::store::StoreMetrics::new()
}
```

Use it in every test-helper Stream construction.

- [ ] **Step 5: Make `StoreMetrics` visible to `stream.rs`**

If not already, add `pub(crate) use store::StoreMetrics;` in `lib.rs` or promote `StoreMetrics` to `pub(crate)` in `store/mod.rs`. Usually the re-export from the store module is sufficient since `stream.rs` is a sibling.

- [ ] **Step 6: Verify**

Run: `RUSTFLAGS="-D warnings" cargo check --workspace --tests --benches`
Expected: clean.

Run: `cargo test --lib --workspace`
Expected: 132 pass.

### Task 1.3: Commit Phase 1

- [ ] **Step 1: Commit**

```bash
git add -A
git commit -m "$(cat <<'EOF'
refactor(store): extract StoreMetrics into shared Arc

Prepares ground for moving the append hot path onto Stream. Every
Stream now holds Arc<StoreMetrics>; the two EN-wide counters
(append_count, bytes_written) move behind this handle. No behaviour
change: aggregate heartbeat view still reads the same atomics,
increments still happen from the same call sites.
EOF
)"
```

---

## Phase 2: Move `replica_info` onto `Stream`; delete `Store.replicas`

Goal: ReplicaInfo lives on the Stream it describes. Store's hashmap goes away.

### Task 2.1: Add `Stream::replica_info`

**Files:**
- Modify: `components/extent-node/src/stream.rs`

- [ ] **Step 1: Add the field**

```rust
use arc_swap::ArcSwapOption;

pub struct Stream {
    // ... existing ...
    /// ReplicaInfo for this stream's current epoch. Set at RegisterEpoch
    /// time; `None` for streams that haven't been registered yet (test
    /// construction, or pre-registration Forward arrival). Immutable
    /// within an epoch; overwritten wholesale on RegisterEpoch.
    replica_info: ArcSwapOption<ReplicaInfo>,
}
```

- [ ] **Step 2: Accessor + setter**

```rust
impl Stream {
    pub(crate) fn replica_info(&self) -> Option<Arc<ReplicaInfo>> {
        self.replica_info.load_full()
    }

    pub(crate) fn set_replica_info(&self, info: Arc<ReplicaInfo>) {
        self.replica_info.store(Some(info));
    }
}
```

- [ ] **Step 3: Initialize in `Stream::new`**

`replica_info: ArcSwapOption::from(None)`.

### Task 2.2: Plumb setter through `RegisterEpoch`

**Files:**
- Modify: `components/extent-node/src/store/register.rs`

- [ ] **Step 1: Replace the insert**

`register.rs:116` currently does:
```rust
self.replicas.pin().insert(stream_id, Arc::new(ri));
```

Replace with:
```rust
if let Some(stream) = streams_guard.get(&stream_id) {
    stream.set_replica_info(Arc::new(ri));
}
```

(`streams_guard` is already held earlier in the function; reuse it.)

### Task 2.3: Switch the two hot-path reads

**Files:**
- Modify: `components/extent-node/src/store/append.rs`

- [ ] **Step 1: Reads in `do_append_and_respond` and `handle_append_batch_inner`**

Each currently does:
```rust
let replica = self.replicas.pin().get(&stream_id).map(Arc::clone);
```

Since the caller already holds `stream: &Stream`, replace with:
```rust
let replica = stream.replica_info();
```

No other behavioural change — downstream `match replica { None | Some(_) }` stays the same.

### Task 2.4: Switch cold-path readers

**Files:**
- Modify: `components/extent-node/src/store/mod.rs`
- Modify: other files that call `get_replica_info` or `secondary_index`

- [ ] **Step 1: `get_replica_info`**

`store/mod.rs::get_replica_info`: rewrite to look up via the stream map:

```rust
pub fn get_replica_info(&self, stream_id: StreamId) -> Option<ReplicaInfo> {
    self.streams
        .pin()
        .get(&stream_id)
        .and_then(|s| s.replica_info())
        .map(|arc| (*arc).clone())
}
```

- [ ] **Step 2: `secondary_index`**

Same pattern:

```rust
pub fn secondary_index(&self, stream_id: StreamId, addr: &str) -> Option<u8> {
    let ri = self
        .streams
        .pin()
        .get(&stream_id)
        .and_then(|s| s.replica_info())?;
    ri.replica_addrs
        .iter()
        .position(|a| a == addr)
        .map(|i| i as u8)
}
```

- [ ] **Step 3: Any other callers of `self.replicas`**

Run:
```bash
grep -rn 'self\.replicas\|\.replicas\.pin' components/extent-node/src/
```

Expected hits so far: `store/mod.rs` (the two methods above), `store/append.rs` (two spots covered in Task 2.3), `store/register.rs` (Task 2.2). If any other file references `self.replicas`, rewrite it to go through the stream.

### Task 2.5: Delete `Store.replicas`

**Files:**
- Modify: `components/extent-node/src/store/mod.rs`

- [ ] **Step 1: Remove the field**

Delete from `ExtentNodeStore`:
```rust
pub(crate) replicas: papaya::HashMap<StreamId, Arc<ReplicaInfo>, IdentityBuildHasher>,
```

And from `new_with_ids`:
```rust
replicas: papaya::HashMap::with_hasher(IdentityBuildHasher),
```

- [ ] **Step 2: Verify**

Run: `RUSTFLAGS="-D warnings" cargo check --workspace --tests --benches`
Expected: clean. Any stray reference surfaces as a compile error.

Run: `cargo test --lib --workspace`
Expected: 132 pass.

### Task 2.6: Commit Phase 2

- [ ] **Step 1: Commit**

```bash
git add -A
git commit -m "$(cat <<'EOF'
refactor(store): move ReplicaInfo from Store map to Stream field

Stream.replica_info: ArcSwapOption<ReplicaInfo> replaces
Store.replicas: HashMap<StreamId, Arc<ReplicaInfo>>. ReplicaInfo is
immutable within an epoch and is overwritten wholesale on
RegisterEpoch, so ArcSwapOption matches the shape exactly.

Hot path (do_append_and_respond, handle_append_batch_inner) now does a
single atomic load instead of a papaya lookup. Cold-path queries
(get_replica_info, secondary_index) re-route through the streams map;
they were already paying that lookup to reach ReplicaInfo, so net
cost is unchanged.
EOF
)"
```

---

## Phase 3: Move `replication_timeout` onto `Stream`

Goal: per-stream ownership of the timeout. Store keeps the value only to seed new Streams at `try_create_stream` time.

### Task 3.1: Field + getter on `Stream`

**Files:**
- Modify: `components/extent-node/src/stream.rs`

- [ ] **Step 1: Add the field**

```rust
pub struct Stream {
    // ... existing ...
    replication_timeout: Duration,
}
```

- [ ] **Step 2: Grow `Stream::new`**

Add `replication_timeout: Duration` as a new parameter. Store it verbatim.

- [ ] **Step 3: Getter**

```rust
pub(crate) fn replication_timeout(&self) -> Duration {
    self.replication_timeout
}
```

### Task 3.2: Pipe through `try_create_stream`

**Files:**
- Modify: `components/extent-node/src/store/mod.rs`

- [ ] **Step 1: Pass it to `Stream::new`**

`Stream::new(..., self.replication_timeout)`.

`ExtentNodeStore.replication_timeout` stays — it's the "default for new streams" field.

### Task 3.3: Update the two hot-path reads

**Files:**
- Modify: `components/extent-node/src/store/append.rs`
- Modify: `components/extent-node/src/store/register.rs`

- [ ] **Step 1: `do_append_and_respond` → `handle_append_batch_inner`**

Each currently reads `self.replication_timeout`. Replace with `stream.replication_timeout()`.

- [ ] **Step 2: `register.rs::handle_register_epoch`**

Line ~97 uses `self.replication_timeout` when initializing the AckQueue. Same rewrite.

### Task 3.4: Update test helpers

**Files:**
- Modify: `components/extent-node/src/stream.rs` (test module)

Add `DEFAULT_REPLICATION_TIMEOUT` as the default for test construction. It's already defined in `ack_queue.rs`; import and use.

### Task 3.5: Commit Phase 3

- [ ] **Step 1: Verify + commit**

Run: `cargo test --lib --workspace` → 132 pass.

```bash
git add -A
git commit -m "refactor(stream): move replication_timeout to Stream.replication_timeout"
```

---

## Phase 4: `streams` map values switch to `Arc<Stream>`

Goal: drain-follower-jobs can `Arc::clone` the stream once and keep it across `.await` without holding a papaya pin guard.

### Task 4.1: Change the field type

**Files:**
- Modify: `components/extent-node/src/store/mod.rs`

- [ ] **Step 1: Update the field**

```rust
pub(crate) streams: papaya::HashMap<StreamId, Arc<Stream>, IdentityBuildHasher>,
```

- [ ] **Step 2: Update `try_create_stream`**

Replace the `guard.insert(stream_id, stream)` call so it wraps the Stream in an Arc:

```rust
guard.insert(stream_id, Arc::new(stream));
```

### Task 4.2: Call-site churn

**Files:**
- Modify: all files under `components/extent-node/src/store/` and `components/extent-node/src/`

- [ ] **Step 1: Inventory**

Run:
```bash
grep -rn 'streams\.pin()\|self\.streams\.pin' components/extent-node/src/
```

For every hit, `guard.get(&id)` now returns `Option<&Arc<Stream>>` instead of `Option<&Stream>`. Auto-deref handles most call sites (method calls via `.` work through `Arc`), so the churn is usually zero. Places that need explicit changes:

- Binding patterns like `if let Some(stream) = guard.get(&id)` — the type of `stream` becomes `&Arc<Stream>`; still auto-derefs to `&Stream` for method calls. Usually no change.
- Code that calls `guard.get(&id).cloned()` to get an owned `Stream` — was wrong before (Stream isn't Clone); unlikely to exist.
- Code that calls `guard.get(&id).map(Arc::clone)` to get `Arc<Stream>` — now correct and needed; before Phase 4 this doesn't compile. **The drain-follower migration in Phase 5 relies on this.**

- [ ] **Step 2: Iteration call sites**

`expire_pending_acks` iterates with `for (_k, stream) in guard.iter()`. The loop body calls `stream.ack_queue()` which auto-derefs; unchanged.

Any `.collect::<Vec<_>>()` patterns that used to collect `&Stream` now collect `&Arc<Stream>`; auto-deref on subsequent use.

- [ ] **Step 3: Tests + integration + benchmarks**

Run: `cargo check --workspace --tests --benches`
Any call site that was reaching into the Stream's internals via a non-method path (unlikely) will surface as a compile error. Fix as you go.

### Task 4.3: Commit Phase 4

- [ ] **Step 1: Verify**

Run: `RUSTFLAGS="-D warnings" cargo check --workspace --tests --benches` → clean.
Run: `cargo test --lib --workspace` → 132 pass.

- [ ] **Step 2: Commit**

```bash
git add -A
git commit -m "$(cat <<'EOF'
refactor(store): wrap streams map values in Arc<Stream>

Required to let Stream::drain_follower_jobs (moved in the next phase)
hold an Arc<Stream> across .await points. Most call sites are
unaffected (auto-deref handles method calls through Arc); the only
intentional new pattern is `guard.get(&id).map(Arc::clone)` to extract
an owned handle.

No behaviour change; papaya HashMap amortizes Arc<Stream> storage the
same way it amortized Stream storage.
EOF
)"
```

---

## Phase 5: Move the hot-path methods onto `Stream`

This is the payoff phase. One commit per method move, or one commit for all three — depending on how clean each diff is. The plan describes them in sequence; the implementer can merge commits if they prefer.

### Task 5.1: `maybe_build_init_forward` → `Stream`

**Files:**
- Modify: `components/extent-node/src/store/forward.rs` (remove)
- Modify: `components/extent-node/src/stream.rs` (add)

- [ ] **Step 1: Cut and paste**

Move the function body verbatim from `store/forward.rs:23-48` to an `impl Stream` block in `stream.rs`. Drop the `&self` receiver on the Store; it's `&self` on Stream now. The function uses only `stream: &Stream` + `frame: &Frame` arguments; `&self` replaces the `stream` param.

New signature:
```rust
impl Stream {
    pub(crate) fn maybe_build_init_forward(&self, frame: &Frame) -> Option<Frame> { /* body */ }
}
```

- [ ] **Step 2: Update call sites**

Run:
```bash
grep -rn 'maybe_build_init_forward' components/extent-node/src/
```

For each hit, rewrite `self.maybe_build_init_forward(stream, &frame)` → `stream.maybe_build_init_forward(&frame)`. Two call sites today (single and batch append paths).

### Task 5.2: `do_append_and_respond` → `Stream::append_one`

**Files:**
- Modify: `components/extent-node/src/store/append.rs` (remove)
- Modify: `components/extent-node/src/stream.rs` (add)

- [ ] **Step 1: Cut and paste**

Move the function to `impl Stream`. Rename to `append_one` (the Store-level name was descriptive of its role in the caller flow; the Stream-level method is simpler).

New signature:
```rust
impl Stream {
    pub(crate) fn append_one(
        &self,
        request_id: u32,
        epoch: Epoch,
        payload: Bytes,
        response_tx: Option<Sender<Frame>>,
    ) -> Option<Frame> { /* body */ }
}
```

Drop `stream: &Stream` param (replaced by `&self`) and `stream_id: StreamId` param (replaced by `self.id`).

- [ ] **Step 2: Rewrite internal references**

Inside the body:
- `self.append_count.fetch_add(...)` → `self.metrics.append_count.fetch_add(...)` (Stream now owns `metrics`)
- `self.bytes_written.fetch_add(...)` → `self.metrics.bytes_written.fetch_add(...)`
- `self.replicas.pin().get(&stream_id).map(Arc::clone)` → `self.replica_info()`
- `self.replication_timeout` → `self.replication_timeout()`
- `stream.write_batch_active(...)` → `self.write_batch_active(...)`
- `stream.with_epoch(...)` → `self.with_epoch(...)`
- `stream.has_secondaries()` → `self.has_secondaries()`
- `stream.init_ack_queue(...)` → `self.init_ack_queue(...)`
- `stream.send_forward(...)` → `self.send_forward(...)`
- `self.maybe_build_init_forward(stream, &forward_frame)` → `self.maybe_build_init_forward(&forward_frame)`
- `stream_id` references → `self.id`

- [ ] **Step 3: Update Store's `handle_append` caller**

After the leader-election fast-path check, the call:

```rust
let own_result = self.do_append_and_respond(stream, request_id, stream_id, epoch, payload, response_tx.cloned());
```

becomes:

```rust
let own_result = stream.append_one(request_id, epoch, payload, response_tx.cloned());
```

Same substitution inside `handle_append_batch_inner`'s fast path (there's a batched inline version today, not a call to `do_append_and_respond`; but when the batch is just 1 frame the batch path delegates to `handle_append`, so the hot path is unified).

### Task 5.3: `drain_follower_jobs` → `Stream`

**Files:**
- Modify: `components/extent-node/src/store/append.rs` (remove)
- Modify: `components/extent-node/src/stream.rs` (add)

- [ ] **Step 1: Cut and paste, collapse the guard dance**

Move to `impl Stream`. Critically, the current four per-iteration `self.streams.pin().get(&stream_id)` acquisitions collapse to zero: `&self` (or `self: &Arc<Self>` if you prefer the ergonomic of `.clone()`) is live for the whole loop.

New signature:
```rust
impl Stream {
    pub(crate) async fn drain_follower_jobs(&self) { /* body */ }
}
```

Body rewrites:
- All `let stream = match guard.get(&stream_id) { Some(s) => s, None => return … };` → gone.
- `stream.job_rx().try_recv()` → `self.job_rx().try_recv()`
- `stream.in_flight().load(...)` / `fetch_sub(...)` → `self.in_flight()...`
- `self.do_append_and_respond(stream, ...)` → `self.append_one(...)`
- `stream.epoch()` → `self.epoch()`
- The `break`/`return` exits that were triggered by "stream not found in map" — drop those cases entirely. If the Stream has been evicted from the map, the caller holding the `Arc<Self>` still has a valid handle; draining whatever remains in `job_rx` is correct. (The map only holds the Stream; the channel's Senders live on whoever still holds a clone of `stream.job_tx()`.)

- [ ] **Step 2: Update Store's callers**

`handle_append` currently does:
```rust
if remaining > 1 {
    self.drain_follower_jobs(stream_id).await;
}
```

The `stream` variable holding `Arc<Stream>` is already in scope from the leader-election block. Change to:
```rust
if remaining > 1 {
    stream.drain_follower_jobs().await;
}
```

But `stream` above is the `Arc<Stream>` from the pin guard — it was dropped at the end of the pre-await block. Rewrite the leader-election block to clone `stream: Arc<Stream>` before dropping the guard:

```rust
let (own_result, stream_arc, remaining) = {
    let guard = self.streams.pin();
    let stream = match guard.get(&stream_id) {
        Some(s) => Arc::clone(s),
        None => return Some(/* UnknownStream */),
    };
    // ... leader election, append_one, fetch_sub ...
    (own_result, stream, remaining)
};
// Guard dropped — now .await-safe.
if remaining > 1 {
    stream_arc.drain_follower_jobs().await;
}
own_result
```

Same restructuring in `handle_append_batch_inner` (its batch processing block ends with a similar `remaining > batch_len` check + drain).

- [ ] **Step 3: Delete the three methods from `store/append.rs`**

`do_append_and_respond`, `drain_follower_jobs`, and the use statement for `AppendJob` (if no longer needed locally) come out. Re-check imports.

### Task 5.4: Commit Phase 5

- [ ] **Step 1: Verify**

Run: `RUSTFLAGS="-D warnings" cargo check --workspace --tests --benches` → clean.
Run: `cargo test --lib --workspace` → 132 pass.

- [ ] **Step 2: Commit**

```bash
git add -A
git commit -m "$(cat <<'EOF'
refactor(store): move append hot path methods onto Stream

do_append_and_respond, drain_follower_jobs, and
maybe_build_init_forward move out of ExtentNodeStore onto Stream:

- Store::handle_append becomes a thin router: one papaya lookup to
  extract Arc<Stream>, then delegates to stream.append_one and
  stream.drain_follower_jobs. No further map lookups on the path.
- drain_follower_jobs drops from four `streams.pin().get(&stream_id)`
  calls per iteration to zero — the Arc<Stream> clone at the top of
  handle_append stays live across every .await.
- ReplicaInfo, metrics, and replication_timeout access all become
  direct field reads on Stream instead of indirect Store lookups.

No behaviour change. The structural simplification unblocks per-stream
metric instrumentation (future work) by letting each Stream own its
hot-path state outright.
EOF
)"
```

---

## Phase 6: Validation + Optional Benchmark

### Task 6.1: Grep + test sweep

- [ ] **Step 1: Targets should all be zero**

Run:
```bash
grep -rn 'self\.replicas' components/extent-node/src/
grep -rn 'self\.append_count\|self\.bytes_written' components/extent-node/src/
grep -rn 'self\.do_append_and_respond\|self\.drain_follower_jobs' components/extent-node/src/
grep -rn 'self\.maybe_build_init_forward' components/extent-node/src/
```
Expected: all empty.

- [ ] **Step 2: Full build + test**

Run: `RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets` → clean.
Run: `cargo test --lib --workspace` → 132 pass.
Run: `cargo test --workspace` (includes integration tests if MySQL is available) → all pass.

### Task 6.2: Optional — benchmark

- [ ] **Step 1: Bench before/after**

On a quiet machine with a separate tree at baseline (`git stash` or a sibling worktree):

```bash
cargo bench --bench pipeline_append -- --measurement-time 30 --sample-size 60
```

Record the median throughput. Compare to a run on `HEAD` of this branch.

Expected: 5–15% improvement on the pipelined group-commit path (the number where `in_flight` routinely goes above 1). No regression on the single-writer path (that one only saves one `replicas` lookup per call).

If results deviate significantly from expectations (e.g., regression), `cargo flamegraph --bench pipeline_append` under both configurations; investigate.

No commit expected here; results go in the PR description.

### Task 6.3: PR

- [ ] **Step 1: Open**

```bash
git push -u origin $(git branch --show-current)
gh pr create --title "Move append hot path onto Stream" --body "$(cat <<'EOF'
## Summary

Moves `do_append_and_respond`, `drain_follower_jobs`, and
`maybe_build_init_forward` from `ExtentNodeStore` onto `Stream`.
Eliminates the per-iteration papaya pin-guard acquisition in the
follower drain loop and collapses the `Store.replicas` hashmap lookup
on every append.

## Changes

- `StoreMetrics` (new): `append_count` + `bytes_written` move behind
  `Arc<StoreMetrics>`; shared by Store (for heartbeat read) and
  every Stream (for append-side increment).
- `Stream.replica_info: ArcSwapOption<ReplicaInfo>` replaces
  `Store.replicas: HashMap<StreamId, Arc<ReplicaInfo>>`.
- `Stream.replication_timeout` replaces per-call access to the Store
  field.
- `Store.streams: HashMap<StreamId, Arc<Stream>>` replaces
  `…<StreamId, Stream>` so the drain loop can hold an owned handle
  across `.await`.
- Three methods physically move from Store to Stream; Store's
  `handle_append` / `handle_append_batch_inner` become thin routers.

## Test plan
- [x] 132 lib tests pass
- [x] `RUSTFLAGS="-D warnings" cargo check --workspace --tests --benches` clean
- [x] `cargo clippy --workspace --all-targets` clean
- [ ] (Optional) pipeline_append bench: expected 5–15% win at the
      pipelined-commit path

## Out of scope
- Per-stream metric histograms (enabled by this refactor; future work)
- Moving seal/read/forward (replicate) handlers onto Stream
EOF
)"
```

---

## Self-Review Checklist

**1. Scope coverage:**
- [x] `StoreMetrics` extracted — Phase 1
- [x] `replica_info` moves to Stream — Phase 2
- [x] `replication_timeout` moves to Stream — Phase 3
- [x] `streams` map values become `Arc<Stream>` — Phase 4
- [x] `do_append_and_respond` / `drain_follower_jobs` / `maybe_build_init_forward` move to Stream — Phase 5

**2. Every phase green-builds:**
Every commit compiles clean and passes the 132-test lib suite. No intentionally broken window anywhere.

**3. Placeholder scan:** No TBDs, TODOs, or "similar to" references. Every call-site rewrite is named.

**4. Ambiguity check:**
- Test helpers in `stream.rs::tests` get an explicit `test_metrics()` fixture (Phase 1 Task 1.2 Step 4).
- `drain_follower_jobs` takes `&self` (not `self: Arc<Self>`) because the caller holds the `Arc<Stream>` and awaits inline — no task spawn required (Phase 5 Task 5.3 Step 1).
- The `Arc<Stream>` clone in `handle_append` happens inside the pin-guard block and is returned out of it; guard drops at the end of the block (Phase 5 Task 5.3 Step 2).

**5. Failure modes:**
- Stream evicted from `streams` map while a drain-follower is in progress: now harmless. Before this plan, the guard dance would detect "stream not found" and `return notifications` from the drain. After: drain holds the `Arc<Stream>` and finishes its work; senders hanging on the channel keep the channel alive until they drop. Matches today's behaviour semantically.
- RegisterEpoch arrives for a stream that doesn't exist in the streams map yet: `register.rs` already creates the stream via `try_create_stream` before setting replica info; the rewrite (Task 2.2) preserves that ordering.
