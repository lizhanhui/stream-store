# Complete Arena Pool Wiring — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the P2 gap where `StreamEpoch`, `Arena`, and `DedicatedArenaPool` exist side-by-side with overlapping buffer/directory ownership but production code bypasses the pool entirely and calls `StreamEpoch::append_inner` directly. After this plan lands, `Arena` is the sole byte-pool primitive, `StreamEpoch` owns epoch metadata over one-or-more `Arc<Arena>`s, `DedicatedArenaPool` is a per-stream factory hanging off `Stream.pool: Arc<dyn ArenaPool>`, and the `store/append.rs` + `store/forward.rs` hot paths assemble `WriteBatch`es and flow through the pool. Arena-full inside an epoch is handled by **internal arena rotation** (not epoch seal); `EpochFullSnafu` becomes `ArenaFullSnafu` and stays below the store layer.

**Architecture:** Three structural changes on the ExtentNode:

1. **Split `StreamEpoch` into `Arena` (byte pool) + `StreamEpoch` (epoch metadata).** `Arena` owns `Arc<ArenaBuffer>`, raw write pointer, `capacity`, `write_cursor`, `record_count`, `committed_bytes`, `ArenaDirectory`, `in_flight`, and the `WriteBatch` delegation channels. `StreamEpoch` owns `stream_id`, `epoch`, `start_offset`, `committed_offset` (logical), `limit` (seal), `flags`, `hasher` + `finalized_crc32` (per-epoch CRC), and `arenas: Mutex<SmallVec<[Arc<Arena>; 4]>>` — one arena per rotation within the epoch.
2. **`ArenaPool` becomes a pure factory trait.** `fn allocate(stream_id, epoch, start_offset, capacity) -> Arc<Arena>`. `DedicatedArenaPool` is stateless except for its shared `ArenaIdGenerator`; each Dedicated stream owns its own `DedicatedArenaPool`. `SharedArenaPool` stays a panicking stub (P3 scope).
3. **Arena-full rotates, does not seal.** `Arena::write_batch` returns `Err(ArenaFull)` on capacity overflow. `StreamEpoch::write_batch` catches `ArenaFull`, calls `self.pool.allocate(...)` to mint a successor arena within the same epoch, appends to `self.arenas`, and retries the failing job. The entire epoch-full seal path in `store/append.rs` is deleted.

`WriteBatch { stream_id, epoch, jobs, reply }` becomes the single hand-off type; primary computes offsets from `arena.record_count + start_offset` before the call, secondary uses offsets from `Forward` frames — same code path. Only `Arena::write_batch(&[ArenaAppend])` exists in this plan; the delegation channel + `reply: Option<oneshot>` stay on the struct as fields for P3 but are unused in Dedicated.

**Tech Stack:** Rust 1.80+, Tokio async runtime, `bytes`, `smallvec`, `parking_lot`, `crossbeam-channel`, `arc-swap`, `papaya`. No new deps.

**Reference docs:**
- Spec: `docs/superpowers/specs/2026-04-24-shared-arena-design.md`
- Prior plans (all merged): P1, P2, pre-P3 cleanup

**Out of scope:**
- `SharedArenaPool` real impl — P3
- `ArenaBacking::Shared` variant on `StreamEpoch` — P3
- Shape A / Shape B S3 formats, `epoch_arenas` table, `UpdateArenaFlushed` opcode — P3/P4
- `CreateStream` wire format carrying `arena_class` — P5
- Adaptive runtime transitions between Dedicated ↔ Shared — P5
- `ExtentState` state-machine collapse to `Open | Sealed` — separate plan

---

## File Structure

This plan modifies existing files in place. No new files are created.

| File | Role in refactor |
|---|---|
| `components/extent-node/src/arena/arena.rs` | Rewrite `Arena` as the single byte-pool primitive; drop `ranges`/`record_range`/multi-entry lookup helpers |
| `components/extent-node/src/arena/pool/` | `ArenaPool` trait in `mod.rs`; `DedicatedArenaPool` in `dedicated.rs`; `SharedArenaPool` (with `in_flight` + `tx`/`rx` fields) in `shared.rs` |
| `components/extent-node/src/arena/write_batch.rs` | Keep types; drop `#[allow(dead_code)]` once wired |
| `components/extent-node/src/arena/directory.rs` | No structural change (stays single-entry for Dedicated) |
| `components/extent-node/src/arena/mod.rs` | Fix stale header comment; re-export `Arena` non-dead |
| `components/extent-node/src/stream_epoch.rs` | Replace buffer/directory/cursor fields with `arenas: Mutex<SmallVec<[Arc<Arena>; 4]>>`; rewrite `append_inner`/`replicate`/`read`/`seal`/`try_advance_committed` to delegate to current arena with rotation |
| `components/extent-node/src/stream.rs` | Add `pool: Arc<dyn ArenaPool>` field; `new` takes pool param; `register_epoch` uses `self.pool.allocate`; delete `try_append_active`; add `write_batch_active` |
| `components/extent-node/src/store/mod.rs` | `ExtentNodeStore` gains `shared_pool: Arc<SharedArenaPool>` singleton; `try_create_stream`/register paths pick a pool per `arena_class` |
| `components/extent-node/src/store/append.rs` | `handle_append`/`handle_append_batch_inner`/`drain_delegated_requests` switch to `Stream::write_batch_active(&[ArenaAppend])`; delete epoch-full seal branches |
| `components/extent-node/src/store/forward.rs` | Secondary replicate path uses the same `write_batch_active` call; drop stale `extent_capacity` plumbing |
| `components/extent-node/src/store/register.rs` | Pass `arena_class` through to the right pool factory |
| `components/rpc/src/frame/header.rs` / `encode.rs` / `decode.rs` / `tests.rs` | Drop `extent_capacity` field from `ForwardInitEpoch` |
| `components/common/src/errors.rs` | Rename `EpochFullSnafu` → `ArenaFullSnafu`; semantics: "record doesn't fit even in an empty arena of configured capacity" OR "pool refused to allocate" |

---

## Phase Order + Build-Broken Window

Seven phases, ~14 commits. Build is intentionally broken mid-Phase 1 (between Task 1.2 and Task 1.6) because `Arena` and `StreamEpoch` can't be split without cascading type changes. Every other phase leaves the tree green.

1. **Phase 0** — Inventory + baseline (no commit)
2. **Phase 1** — Split `StreamEpoch` into `Arena` + epoch-metadata; wire `ArenaPool` factory; rewire `store/append.rs` + `store/forward.rs`; rename `EpochFull` → `ArenaFull`
3. **Phase 2** — Delete `Stream::try_append_active` and residual dead-code paths
4. **Phase 3** — Strip `extent_capacity` from `ForwardInitEpoch` wire
5. **Phase 4** — Stale-comment / naming cleanup (`arena/mod.rs` header, `Stream::active_epoch*` method names)
6. **Phase 5** — Rewrite unit tests for new structure
7. **Phase 6** — Validation + final commit

Estimated commits: 14. Largest phases: Phase 1 (the split) and Phase 5 (test rewrite).

---

## Phase 0: Inventory + Baseline

### Task 0.1: Baseline measurements

**Files:** (read-only)

- [ ] **Step 1: Confirm build + test baseline**

Run: `RUSTFLAGS="-D warnings" cargo check --workspace --tests --benches 2>&1 | tail -5`
Expected: `Finished`, no warnings.

Run: `cargo test --lib --workspace 2>&1 | grep -E '^test result:' | awk '{ p+=$4; f+=$6 } END { printf "passed=%d failed=%d\n", p, f }'`
Record the baseline number (expected: 144).

- [ ] **Step 2: Inventory the rewrite surface**

Run:
```bash
cd /data/repo/stream-store
grep -rn 'try_append_active\|append_inner\|EpochFull\|EpochFullSnafu' \
  --include='*.rs' components/ tests/ benches/ | wc -l
grep -rn 'arena::Arena\b\|DedicatedArenaPool\|SharedArenaPool\|ArenaPool\b' \
  --include='*.rs' components/ tests/ benches/ | wc -l
grep -rn 'extent_capacity' --include='*.rs' components/ tests/ benches/ | wc -l
```

Record counts. Phase 6 will verify they go to their target values:
- `try_append_active`: 0
- `EpochFull*` identifiers (code, not docs): 0
- `ArenaFullSnafu`: >0
- `extent_capacity` on wire: 0; as a config field: still >0

No commit.

---

## Phase 1: Split StreamEpoch into Arena + Epoch Metadata

Goal: After this phase, `Arena` owns the byte pool, `StreamEpoch` owns epoch metadata over `SmallVec<Arc<Arena>>`, `DedicatedArenaPool` is a factory, `Stream.pool: Arc<dyn ArenaPool>` is set at construction, and `store/append.rs` + `store/forward.rs` flow through the pool via `WriteBatch`. Arena-full rotates internally.

> **Reviewer note:** Tasks 1.2 through 1.8 form a single logical unit. The build is intentionally broken from Task 1.2 (when `Arena` constructor signature changes) until Task 1.8 (when the full append path switches over). Do not split these across review cycles; review the entire phase diff at Task 1.9.

### Task 1.1: Enumerate the symbols being moved

**Files:** (read-only)

- [ ] **Step 1: List every field of `StreamEpoch` that will relocate to `Arena`**

Run:
```bash
grep -n '^\s*\(pub(crate) \)\?\(arena\|buf\|capacity\|write_cursor\|record_count\|committed_bytes\|directory\|in_flight\|tx\|rx\)' \
  components/extent-node/src/stream_epoch.rs
```

Expected: roughly 10 fields. These move to `Arena`.

- [ ] **Step 2: List every method on `StreamEpoch` that touches those fields**

Run:
```bash
grep -n 'self\.\(arena\|buf\|capacity\|write_cursor\|record_count\|committed_bytes\|directory\)' \
  components/extent-node/src/stream_epoch.rs | awk -F: '{print $1":"$2}' | head -40
```

Note the method names — each gets rewritten in Task 1.6 to delegate to `Arena`.

No commit.

### Task 1.2: Rewrite `Arena` as the byte-pool primitive

**Files:**
- Modify: `components/extent-node/src/arena/arena.rs`

- [ ] **Step 1: Replace `Arena` struct**

Constructor takes full identity up front:

```rust
pub(crate) struct Arena {
    pub(crate) arena_id: ArenaId,
    pub(crate) stream_id: StreamId,
    pub(crate) epoch: Epoch,
    pub(crate) start_offset: Offset,
    buffer: Arc<ArenaBuffer>,
    buf: *mut u8,
    capacity: u32,
    write_cursor: AtomicU64,
    record_count: AtomicU64,
    committed_bytes: AtomicU64,
    directory: ArenaDirectory,
    /// Arena-level leader-election counter. Unused in Dedicated (stream leader
    /// is always the arena leader); wired in P3 for Shared.
    pub(crate) in_flight: AtomicU64,
    /// Delegation channel. Unused in Dedicated; wired in P3.
    pub(crate) tx: Sender<WriteBatch>,
    pub(crate) rx: Receiver<WriteBatch>,
}
unsafe impl Send for Arena {}
unsafe impl Sync for Arena {}
```

- [ ] **Step 2: Replace the constructor**

```rust
impl Arena {
    pub(crate) fn new(
        arena_id: ArenaId,
        stream_id: StreamId,
        epoch: Epoch,
        start_offset: Offset,
        capacity: u32,
    ) -> Self {
        let buffer = ArenaBuffer::new(capacity);
        let buf = buffer.ptr_mut();
        let record_cap = (capacity / MIN_RECORD_SIZE) as usize;
        let entry = EpochArenaEntry::with_capacity(stream_id, epoch, start_offset, record_cap);
        let directory = ArenaDirectory::new(entry);
        let (tx, rx) = unbounded();
        Self {
            arena_id, stream_id, epoch, start_offset,
            buffer, buf, capacity,
            write_cursor: AtomicU64::new(0),
            record_count: AtomicU64::new(0),
            committed_bytes: AtomicU64::new(0),
            directory,
            in_flight: AtomicU64::new(0),
            tx, rx,
        }
    }
}
```

`MIN_RECORD_SIZE` is currently defined in `stream_epoch.rs`; move it to `arena::arena` (or `arena::mod`) — `Arena` is the new owner.

- [ ] **Step 3: Replace `write_batch` with `write_batch`**

```rust
impl Arena {
    /// Single-writer append of a job batch. Caller owns the single-writer
    /// invariant. On `ArenaFull`, caller must rotate to a new arena and
    /// retry the failing job.
    pub(crate) fn write_batch(
        &self,
        jobs: &[ArenaAppend],
    ) -> SmallVec<[Result<ArenaAppendResult, StorageError>; 16]> {
        let mut out = SmallVec::with_capacity(jobs.len());
        for job in jobs {
            out.push(self.write(job));
        }
        out
    }

    fn write(&self, job: &ArenaAppend) -> Result<ArenaAppendResult, StorageError> {
        let payload_len = job.payload.len();
        let record_len = 4 + payload_len as u64;
        let byte_pos = self.write_cursor.load(Ordering::Relaxed);
        if byte_pos + record_len > self.capacity as u64 {
            return Err(ArenaFullSnafu {
                stream_id: self.stream_id,
                epoch: self.epoch,
                arena_id: self.arena_id,
            }.build());
        }
        self.write_cursor.store(byte_pos + record_len, Ordering::Relaxed);
        let seq = self.record_count.load(Ordering::Relaxed);
        self.record_count.store(seq + 1, Ordering::Relaxed);
        unsafe {
            let dst = self.buf.add(byte_pos as usize);
            std::ptr::copy_nonoverlapping(
                (payload_len as u32).to_be_bytes().as_ptr(), dst, 4);
            if payload_len > 0 {
                std::ptr::copy_nonoverlapping(
                    job.payload.as_ptr(), dst.add(4), payload_len);
            }
        }
        self.committed_bytes.store(byte_pos + record_len, Ordering::Release);
        self.directory.single_entry().record(seq, byte_pos);
        Ok(ArenaAppendResult::new(job.offset, self.arena_id, byte_pos as u32))
    }
}
```

- [ ] **Step 4: Drop `ranges`/`ArenaRange`/`record_range`/`start_offset(s,e)`/`end_offset(s,e)`/`contains_offset(s,e)`**

In Dedicated the `(stream_id, epoch, start_offset)` are construction-time constants; no runtime ranges are needed. Replace by accessor methods:

```rust
impl Arena {
    pub(crate) fn capacity(&self) -> u32 { self.capacity }
    pub(crate) fn bytes_written(&self) -> u64 { self.committed_bytes.load(Ordering::Acquire) }
    pub(crate) fn record_count(&self) -> u64 { self.record_count.load(Ordering::Acquire) }
    pub(crate) fn next_offset(&self) -> Offset {
        Offset(self.start_offset.0 + self.record_count())
    }
    pub(crate) fn contains_offset(&self, offset: Offset) -> bool {
        offset.0 >= self.start_offset.0 && offset.0 < self.next_offset().0
    }
    pub(crate) fn buffer(&self) -> &Arc<ArenaBuffer> { &self.buffer }
    pub(crate) fn directory(&self) -> &ArenaDirectory { &self.directory }
}
```

- [ ] **Step 5: Provide `read(offset, count)` delegating to the single-entry directory**

Keep the `read` body approximately as it is today in `arena.rs:157`, but derive `start` from `self.start_offset` (construction-time constant) instead of a runtime lookup.

- [ ] **Step 6: Verify `arena` crate compiles**

Run: `cargo check -p extent-node 2>&1 | tail -20`
Expected: errors in `stream_epoch.rs` and `pool.rs` that reference the old `Arena` signature. These are the fix list for Tasks 1.3–1.6. Build is broken until Task 1.8.

No commit.

### Task 1.3: Rewrite `DedicatedArenaPool` as a factory; define the trait

**Files:**
- Modify: `components/extent-node/src/arena/pool.rs`

- [ ] **Step 1: Replace the `ArenaPool` trait**

```rust
pub(crate) trait ArenaPool: Send + Sync {
    fn class(&self) -> ArenaClass;
    fn allocate(
        &self,
        stream_id: StreamId,
        epoch: Epoch,
        start_offset: Offset,
        capacity: u32,
    ) -> Arc<Arena>;
}
```

No `write_batch` or `read` on the pool — those are per-arena and per-epoch, not per-pool.

- [ ] **Step 2: Rewrite `DedicatedArenaPool`**

```rust
pub(crate) struct DedicatedArenaPool {
    ids: Arc<ArenaIdGenerator>,
}

impl DedicatedArenaPool {
    pub(crate) fn new(ids: Arc<ArenaIdGenerator>) -> Self {
        Self { ids }
    }
}

impl ArenaPool for DedicatedArenaPool {
    fn class(&self) -> ArenaClass { ArenaClass::Dedicated }
    fn allocate(
        &self, stream_id: StreamId, epoch: Epoch, start_offset: Offset, capacity: u32,
    ) -> Arc<Arena> {
        Arc::new(Arena::new(self.ids.next(), stream_id, epoch, start_offset, capacity))
    }
}
```

Drop the old `active` / `arenas: VecDeque` / `rotate_arena` — stream-level `StreamEpoch.arenas` now owns the rotation ring.

- [ ] **Step 3: Keep `SharedArenaPool` as a panicking stub**

```rust
pub(crate) struct SharedArenaPool {
    _ids: Arc<ArenaIdGenerator>,
    _arena_size: u32,
}

impl SharedArenaPool {
    pub(crate) fn new(ids: Arc<ArenaIdGenerator>, arena_size: u32) -> Self {
        Self { _ids: ids, _arena_size: arena_size }
    }
}

impl ArenaPool for SharedArenaPool {
    fn class(&self) -> ArenaClass { ArenaClass::Shared }
    fn allocate(
        &self, _stream_id: StreamId, _epoch: Epoch, _start_offset: Offset, _capacity: u32,
    ) -> Arc<Arena> {
        panic!("SharedArenaPool::allocate not wired — P3 scope")
    }
}
```

- [ ] **Step 4: Delete the pool-level unit tests that exercised the old VecDeque semantics**

Phase 5 adds new factory tests. The current tests assert rotation behavior that now lives on StreamEpoch.

- [ ] **Step 5: Verify**

Run: `cargo check -p extent-node 2>&1 | tail -20`
Expected: errors shift to `stream_epoch.rs` and `stream.rs`. Proceed.

No commit.

### Task 1.4: Rename `EpochFullSnafu` → `ArenaFullSnafu`

**Files:**
- Modify: `components/common/src/errors.rs`

- [ ] **Step 1: Rename the snafu variant**

```rust
#[snafu(display("arena full: stream={stream_id} epoch={epoch} arena_id={arena_id}"))]
ArenaFull {
    stream_id: StreamId,
    epoch: Epoch,
    arena_id: ArenaId,
},
```

Replace every `StorageError::EpochFull` and `EpochFullSnafu` reference workspace-wide. Keep `EpochSealedSnafu` untouched.

- [ ] **Step 2: Update call sites that matched the old variant**

Today there are matches in `store/append.rs` at lines 179, 396, 843 (epoch-full seal path). These will be **deleted** in Task 1.7 entirely because arena-full no longer bubbles up past `StreamEpoch::write_batch`. For now, until Task 1.7, replace `StorageError::EpochFull { .. }` → `StorageError::ArenaFull { .. }` to keep the type checker happy.

The `SealReason::EpochFull` enum discriminant in `stream.rs:25` **stays** (it still describes seal reasons at the Stream level, but after Task 1.7 no one writes it). We'll delete `SealReason::EpochFull` when no variant remains — if that leaves the enum empty, delete the enum and the `reason` param. Keep the deletion inside Task 1.7.

- [ ] **Step 3: Add `arena_id` to `common/src/errors.rs`'s import list**

`ArenaId` already lives in `common/src/types.rs`? Check: `grep -n 'pub struct ArenaId' components/common/src/types.rs`. If it's in `extent-node::arena::id`, either move it to `common` (preferred — ArenaId is a first-class identity type) or keep the error variant simpler by carrying `arena_id: u64`.

Decision: **move `ArenaId` + `ArenaIdGenerator` + `node_prefix_from_id` to `common::types`**. This is small and resolves the error-variant dependency cleanly. Update `components/extent-node/src/arena/mod.rs` to re-export from `common::types` for continuity.

- [ ] **Step 4: Verify**

Run: `cargo check --workspace 2>&1 | tail -20`
Expected: errors still concentrated in `stream_epoch.rs` and `stream.rs`.

No commit.

### Task 1.5: Rewrite `StreamEpoch` to own `Arc<Arena>`

**Files:**
- Modify: `components/extent-node/src/stream_epoch.rs`

This is the single most invasive task in the plan. Approach it in order: struct, constructor, `append_inner`, `replicate`, `read`/`index_lookup`, `seal`/`finalized_crc32`, remaining accessors. Delete the unit-test block at the bottom of the file (Phase 5 rewrites it).

- [ ] **Step 1: Replace the struct**

```rust
pub struct StreamEpoch {
    pub stream_id: StreamId,
    pub epoch: Epoch,
    pub start_offset: Offset,

    /// Arena list for this epoch. Grows on arena-full rotation. In P3, Shared
    /// epochs observe many arenas (one per arena the shared pool rolled through).
    arenas: Mutex<SmallVec<[Arc<Arena>; 4]>>,

    /// The pool that mints arenas for this epoch. Held so that
    /// `write_batch` can request a successor arena on rotation without
    /// routing back through Stream.
    pool: Arc<dyn ArenaPool>,

    /// Fixed capacity for arenas within this epoch.
    arena_capacity: u32,

    /// Committed logical offset: next offset after last fully-written record
    /// (exclusive). Starts at `start_offset.0`.
    committed_offset: AtomicU64,

    /// Seal marker. LIMIT_OPEN until sealed.
    limit: AtomicU64,

    /// Lifecycle flags (FLAG_INIT_FORWARD, FLAG_CHECKSUM_RECEIVED, FLAG_FLUSHED).
    flags: AtomicU8,

    /// Per-epoch CRC32 hasher (covers records across all arenas in order).
    hasher: UnsafeCell<crc32fast::Hasher>,
    finalized_crc32: AtomicU32,

    /// Arena-id list (denormalized from `arenas`) for log / debug / P3 reads.
    pub(crate) resident_arenas: Mutex<SmallVec<[ArenaId; 4]>>,
    pub(crate) directory_ref_count: AtomicU32,
}
unsafe impl Send for StreamEpoch {}
unsafe impl Sync for StreamEpoch {}
```

The previous buffer/cursor/directory fields are gone — `arenas[i]` holds them.

- [ ] **Step 2: Replace the constructor**

```rust
impl StreamEpoch {
    pub(crate) fn new(
        stream_id: StreamId,
        epoch: Epoch,
        start_offset: Offset,
        arena_capacity: u32,
        pool: Arc<dyn ArenaPool>,
    ) -> Self {
        let first = pool.allocate(stream_id, epoch, start_offset, arena_capacity);
        let first_id = first.arena_id;
        Self {
            stream_id, epoch, start_offset,
            arenas: Mutex::new(smallvec![first]),
            pool,
            arena_capacity,
            committed_offset: AtomicU64::new(start_offset.0),
            limit: AtomicU64::new(LIMIT_OPEN),
            flags: AtomicU8::new(FLAG_INIT_FORWARD),
            hasher: UnsafeCell::new(crc32fast::Hasher::new()),
            finalized_crc32: AtomicU32::new(0),
            resident_arenas: Mutex::new(smallvec![first_id]),
            directory_ref_count: AtomicU32::new(1),
        }
    }
}
```

`with_capacity` is deleted. Callers switch to `StreamEpoch::new(stream_id, epoch, start_offset, arena_capacity, pool)`.

- [ ] **Step 3: Implement `write_batch` with rotation**

```rust
impl StreamEpoch {
    pub(crate) fn write_batch(
        &self,
        jobs: &[ArenaAppend],
    ) -> SmallVec<[Result<ArenaAppendResult, StorageError>; 16]> {
        // Seal check applies to the whole batch.
        let limit = self.limit.load(Ordering::Acquire);
        if limit != LIMIT_OPEN {
            let current = self.committed_offset.load(Ordering::Relaxed) - self.start_offset.0;
            if current >= limit {
                let err = EpochSealedSnafu { stream_id: self.stream_id, epoch: self.epoch }.build();
                return jobs.iter().map(|_| Err(err.clone())).collect();
            }
        }

        let mut out = SmallVec::with_capacity(jobs.len());
        let mut remaining = jobs;
        loop {
            if remaining.is_empty() { break; }
            let arena = self.current_arena();
            let mut batch = arena.write_batch(remaining);
            // Consume results in order; on ArenaFull, rotate and resume.
            let mut rotated_at: Option<usize> = None;
            for (i, r) in batch.drain(..).enumerate() {
                match r {
                    Ok(ok) => {
                        // Update per-epoch CRC + committed_offset.
                        let payload = &remaining[i].payload;
                        unsafe {
                            let h = &mut *self.hasher.get();
                            h.update(&(payload.len() as u32).to_be_bytes());
                            if !payload.is_empty() { h.update(payload); }
                        }
                        self.committed_offset.fetch_add(1, Ordering::Release);
                        out.push(Ok(ok));
                    }
                    Err(StorageError::ArenaFull { .. }) => {
                        rotated_at = Some(i);
                        break;
                    }
                    Err(e) => {
                        out.push(Err(e));
                    }
                }
            }
            match rotated_at {
                Some(i) => {
                    self.rotate_arena();
                    remaining = &remaining[i..];
                }
                None => {
                    remaining = &[];
                }
            }
        }
        out
    }

    fn current_arena(&self) -> Arc<Arena> {
        self.arenas.lock().last().expect("at least one arena").clone()
    }

    fn rotate_arena(&self) {
        let next_start = Offset(self.committed_offset.load(Ordering::Acquire));
        let new_arena = self.pool.allocate(
            self.stream_id, self.epoch, next_start, self.arena_capacity,
        );
        let new_id = new_arena.arena_id;
        self.arenas.lock().push(new_arena);
        self.resident_arenas.lock().push(new_id);
        self.directory_ref_count.fetch_add(1, Ordering::Release);
    }
}
```

Note: `start_offset` of a rotated arena is the next logical offset at rotation time, so per-arena directory lookups stay internally consistent.

- [ ] **Step 4: Reimplement `append_inner` as a 1-job `write_batch`**

```rust
pub(crate) fn append_inner(&self, payload: Bytes) -> Result<AppendResult, StorageError> {
    let next_offset = Offset(self.committed_offset.load(Ordering::Relaxed));
    let job = ArenaAppend::new(next_offset, payload);
    let mut results = self.write_batch(std::slice::from_ref(&job));
    match results.pop().expect("one result per job") {
        Ok(r) => Ok(AppendResult { offset: r.offset, byte_pos: r.byte_pos as u64 }),
        Err(e) => Err(e),
    }
}
```

- [ ] **Step 5: Reimplement `replicate` the same way**

```rust
pub fn replicate(&self, offset: Offset, payload: Bytes) -> Result<AppendResult, StorageError> {
    if offset.0 < self.start_offset.0 {
        return Err(InternalSnafu { message: format!("stale forward: {} < {}", offset.0, self.start_offset.0) }.build());
    }
    // Trust FIFO delivery: the offset must equal committed_offset for strict-order replay.
    let expected = self.committed_offset.load(Ordering::Relaxed);
    if offset.0 != expected {
        return Err(InternalSnafu { message: format!("out-of-order forward: got {} expected {}", offset.0, expected) }.build());
    }
    let job = ArenaAppend::new(offset, payload);
    let mut results = self.write_batch(std::slice::from_ref(&job));
    match results.pop().expect("one result") {
        Ok(r) => Ok(AppendResult { offset: r.offset, byte_pos: r.byte_pos as u64 }),
        Err(e) => Err(e),
    }
}
```

- [ ] **Step 6: Reimplement `read` / `index_lookup` over the arena list**

```rust
pub fn read(&self, byte_pos: u64, count: u32) -> Result<Vec<Bytes>, StorageError> {
    // byte_pos + count no longer identify a record unambiguously across
    // rotations; delegate to offset-based lookup at the Stream layer.
    // For internal use: this method is only called inside the single-arena
    // path today. Rewrite callers in `stream.rs::read` to go through
    // offset-based `read_at_offset(offset, count)` defined here.
    unimplemented!("call read_at_offset instead")
}

pub fn read_at_offset(&self, offset: Offset, count: u32) -> Result<Vec<Bytes>, StorageError> {
    // Find which arena contains `offset`.
    let arenas = self.arenas.lock().clone();  // cheap: Arc clones only
    let arena = arenas.iter().find(|a| a.contains_offset(offset))
        .cloned()
        .ok_or_else(|| InternalSnafu { message: format!("offset {} not resident", offset.0) }.build())?;
    arena.read(offset, count)
}

pub fn index_lookup(&self, seq: u64) -> Option<u64> {
    // Walk the arena list; each arena's start_offset + its local record_count
    // tells us the range it owns.
    let arenas = self.arenas.lock();
    for arena in arenas.iter() {
        let local_start = arena.start_offset.0 - self.start_offset.0;
        let local_end = local_start + arena.record_count();
        if seq >= local_start && seq < local_end {
            return arena.directory().single_entry().lookup(seq - local_start);
        }
    }
    None
}
```

Adjust `stream.rs::read` to call `extent.read_at_offset(offset, count)`.

- [ ] **Step 7: Reimplement `seal` / `try_advance_committed` / `finalized_crc32`**

`seal` marks `limit`, finalizes the hasher (clone + `finalize`), stores into `finalized_crc32`. No buffer work needed.

`try_advance_committed` is now redundant — `write_batch` advances `committed_offset` in the success branch. Delete the method and its callers (there are two in `store/forward.rs` that update committed_offset after Forward arrives; remove — `replicate` already advanced it).

`committed_data()` accessor: returns `Bytes` concatenated across `self.arenas` up to `committed_offset`. Use `Bytes::from_owner(OwnedArenaSlice)` per arena and concatenate via `BytesMut::extend_from_slice`.

- [ ] **Step 8: Delete the old `with_capacity`, old tests, and all `#[allow(dead_code)]` related to the relocated fields**

Delete the entire `#[cfg(test)] mod tests { ... }` block at `stream_epoch.rs:1500+`. Phase 5 writes new ones.

- [ ] **Step 9: Verify**

Run: `cargo check -p extent-node 2>&1 | tail -20`
Expected: errors remaining in `stream.rs` (the `register_epoch` call-site and `try_append_active`) and `store/*.rs`. Proceed.

No commit.

### Task 1.6: Plumb `pool` through `Stream`

**Files:**
- Modify: `components/extent-node/src/stream.rs`

- [ ] **Step 1: Add `pool` field**

```rust
pub struct Stream {
    pub id: StreamId,
    // ... existing ...
    pool: Arc<dyn ArenaPool>,
    // ... existing ...
}
```

- [ ] **Step 2: Update `Stream::new`**

```rust
pub(crate) fn new(
    id: StreamId,
    arena_ids: Arc<ArenaIdGenerator>,
    pool: Arc<dyn ArenaPool>,
) -> Self {
    // as before, plus `pool`
}
```

- [ ] **Step 3: Rewrite `register_epoch`**

```rust
pub fn register_epoch(&self, start_offset: Offset, epoch: Epoch, epoch_capacity: u32) {
    self.epoch.store(epoch.0, Ordering::Release);
    {
        let mut inner = self.inner.write();
        inner.epoch_capacity = epoch_capacity;
    }
    let ep = Arc::new(StreamEpoch::new(
        self.id, epoch, start_offset, epoch_capacity, Arc::clone(&self.pool),
    ));
    self.insert_epoch(ep);
    self.evict_oldest_epochs();
}
```

- [ ] **Step 4: Delete `try_append_active`**

Replace with `write_batch_active`:

```rust
pub fn write_batch_active(
    &self,
    jobs: &[ArenaAppend],
) -> SmallVec<[Result<ArenaAppendResult, StorageError>; 16]> {
    match self.active_epoch_ref() {
        Some(ep) => ep.write_batch(jobs),
        None => {
            let err = InternalSnafu { message: format!("stream {}: no active epoch", self.id) }.build();
            jobs.iter().map(|_| Err(err.clone())).collect()
        }
    }
}
```

Keep `try_append_active` as a `#[cfg(test)]` shim temporarily if any test still needs a single-record call; Phase 2 deletes it. Simpler: rewrite callers directly in Task 1.7.

- [ ] **Step 5: Fix test helpers**

Change `test_arena_ids()` to also return a pool; introduce `fn test_pool()` returning `Arc<DedicatedArenaPool>`. `Stream::new` call sites take the pool as third arg.

- [ ] **Step 6: Verify**

Run: `cargo check -p extent-node 2>&1 | tail -20`
Expected: errors now only in `store/append.rs` + `store/forward.rs` + `store/mod.rs`. Proceed.

No commit.

### Task 1.7: Rewire `store/append.rs` to `write_batch_active`

**Files:**
- Modify: `components/extent-node/src/store/append.rs`

- [ ] **Step 1: Rewrite `do_append_and_respond`**

Replace the `stream.try_append_active(payload)` call with a 1-job WriteBatch:

```rust
let job = ArenaAppend::new(
    Offset(0), // sentinel — will be assigned by Arena based on write_cursor
    payload,
);
// Actually: offsets are computed by Arena from start_offset + record_count,
// so the job.offset we pass is ignored on the primary side. We pass the
// expected offset for echo-back only.
let mut results = stream.write_batch_active(std::slice::from_ref(&job));
let append_result = match results.pop().expect("one result") {
    Ok(r) => AppendResult { offset: r.offset, byte_pos: r.byte_pos as u64 },
    Err(StorageError::EpochSealed { .. }) => { /* existing error handling */ }
    Err(e) => { /* existing error handling */ }
};
```

**Design note on offset assignment:** Spec says primary "reserves offset by incrementing write_cursor". Two options:
- (a) Arena assigns offset (`Offset(self.start_offset.0 + seq)`); primary ignores `job.offset` on write, uses it on forward.
- (b) Primary pre-reserves from `epoch.committed_offset` before building the WriteBatch.

Option (a) is atomic within one arena but breaks across rotation (the rotated arena's `start_offset` is the pre-rotation `committed_offset`, so sequence numbers restart from 0 — the `ArenaAppendResult.offset` still resolves correctly). Go with (a): `ArenaAppend.offset` becomes the **echo-back hint** used by secondaries (who pass the authoritative primary-assigned offset); on primary, `Arena::write` ignores `job.offset` and derives offset from its own `start_offset + seq`.

Update `ArenaAppendResult` construction in `Arena::write`:
```rust
let assigned_offset = Offset(self.start_offset.0 + seq);
Ok(ArenaAppendResult::new(assigned_offset, self.arena_id, byte_pos as u32))
```

On secondary, `Arena::write` validates `job.offset == assigned_offset` and returns the authoritative one; mismatch is `Err(InternalSnafu)`.

- [ ] **Step 2: Delete the epoch-full seal branch**

In `handle_append` (lines 100–124) and `handle_append_batch_inner` (lines 842–866): the entire `if extent_full { seal_current_epoch; ... }` block goes away. Arena rotation is internal; no seal happens on arena-full. `extent_full: bool` and the second element of `do_append_and_respond`'s return tuple disappear.

- [ ] **Step 3: Update `drain_delegated_requests`**

Collect all drained `AppendRequest`s into a `SmallVec<ArenaAppend>`, call `stream.write_batch_active` once per drain cycle, iterate results to send ACKs + Forward frames. Delete `extent_full_idx` logic.

- [ ] **Step 4: Convert the batch path**

`handle_append_batch_inner`: build `SmallVec<ArenaAppend>` from the incoming frames, one `stream.write_batch_active` call, iterate results. Delete the per-frame `try_append_active` loop (lines 677–730).

- [ ] **Step 5: Remove `SealReason::EpochFull`**

It's the only variant. Delete the enum + the `reason` parameter of `seal_current_epoch` + the matching `info!` log fields.

- [ ] **Step 6: Verify**

Run: `cargo check -p extent-node 2>&1 | tail -20`
Expected: errors only in `store/forward.rs` and `store/mod.rs`.

No commit.

### Task 1.8: Rewire `store/forward.rs` and `store/mod.rs`

**Files:**
- Modify: `components/extent-node/src/store/forward.rs`
- Modify: `components/extent-node/src/store/mod.rs`

- [ ] **Step 1: Secondary `replicate` path**

Replace `stream.replicate(epoch, offset, payload)` with:
```rust
let job = ArenaAppend::new(offset, payload);
let mut results = stream.write_batch_active(std::slice::from_ref(&job));
```

(`Stream::replicate` can be kept as a thin wrapper around `write_batch_active` that passes the Forward's offset as the job offset, if existing callers need the old signature. Preferred: delete `Stream::replicate`; secondaries call `write_batch_active` directly from `store/forward.rs`.)

- [ ] **Step 2: Plumb `arena_class` through `ExtentNodeStore`**

```rust
pub struct ExtentNodeStore {
    // ... existing ...
    pub(crate) shared_pool: Arc<SharedArenaPool>,  // singleton per EN
}
```

In `ExtentNodeStore::new_with_ids`:
```rust
let shared_pool = Arc::new(SharedArenaPool::new(
    Arc::clone(&arena_ids),
    DEFAULT_EPOCH_CAPACITY,  // or a new DEFAULT_SHARED_ARENA_SIZE config
));
```

- [ ] **Step 3: Pick a pool per arena_class at stream creation**

In `try_create_stream` / `store/register.rs::handle_register_epoch`, read the `arena_class` from the inbound frame's `StreamConfig`, then:

```rust
let pool: Arc<dyn ArenaPool> = match arena_class {
    ArenaClass::Dedicated => Arc::new(DedicatedArenaPool::new(Arc::clone(&self.arena_ids))),
    ArenaClass::Shared => Arc::clone(&self.shared_pool) as Arc<dyn ArenaPool>,
};
let stream = Stream::new(stream_id, Arc::clone(&self.arena_ids), pool);
```

Dedicated streams get a fresh `DedicatedArenaPool` each — per the decision in conversation, "if a stream is with arena_class == dedicated, then that stream has a dedicated DedicatedArenaPool". Shared streams share the EN-wide one.

- [ ] **Step 4: Verify**

Run: `cargo check --workspace 2>&1 | tail -10`
Expected: green.

Run: `cargo test --workspace --tests --no-run 2>&1 | tail -10`
Expected: build succeeds; test binaries produced. Some tests will fail at runtime — that's Phase 5's job.

No commit yet. The tree compiles; individual tests may fail until Phase 5 rewrites them.

### Task 1.9: Commit phase 1

- [ ] **Step 1: Skim the diff**

Run: `git diff --stat`

Expected: changes concentrated in `components/extent-node/src/arena/{arena,pool}.rs`, `stream_epoch.rs`, `stream.rs`, `store/{append,forward,mod,register}.rs`, `components/common/src/errors.rs`, and test helpers.

- [ ] **Step 2: Commit**

```bash
git add -A
git commit -m "$(cat <<'EOF'
refactor(extent-node): split StreamEpoch into Arena + epoch-metadata

Arena becomes the sole byte-pool primitive, owning the buffer, directory,
cursors, record count, and the arena-level leader-election channels.
StreamEpoch owns epoch-level metadata over `SmallVec<[Arc<Arena>; 4]>`;
arena-full triggers internal rotation within the same epoch.

- ArenaPool collapses to a factory trait: `fn allocate(...) -> Arc<Arena>`.
  DedicatedArenaPool is per-stream; SharedArenaPool is a singleton stub
  (panics in allocate, wired in P3).
- Stream.pool: Arc<dyn ArenaPool> is chosen by `arena_class` at stream
  creation. Stream::write_batch_active is the sole entry to the write
  path; try_append_active is gone.
- `EpochFullSnafu` renamed `ArenaFullSnafu` and stays below the store
  layer. The epoch-full seal branch in store/append.rs is deleted
  (arena rotation replaces it). `SealReason::EpochFull` is the last
  variant and is deleted; `seal_current_epoch` loses its `reason` param.
- store/append.rs + store/forward.rs build WriteBatch batches and call
  stream.write_batch_active. Primary assigns offsets from arena
  start_offset + seq; secondary passes the Forward frame's offset and
  Arena validates strict-order replay.
- MIN_RECORD_SIZE + ArenaId + ArenaIdGenerator + node_prefix_from_id
  migrate to common::types so ArenaFullSnafu can reference ArenaId.

Existing unit tests that asserted old internals are deleted here;
Phase 5 replaces them with coverage of the new structure.
EOF
)"
```

---

## Phase 2: Dead-Code Sweep

Goal: remove `#[allow(dead_code)]` attributes that applied to relocated fields, and any now-unreachable helpers.

### Task 2.1: Strip stale allow(dead_code)

**Files:**
- Modify: `components/extent-node/src/stream_epoch.rs`, `arena/write_batch.rs`, `arena/pool.rs`, `arena/mod.rs`

- [ ] **Step 1: Grep for `#[allow(dead_code)]`**

```bash
grep -rn '#\[allow(dead_code)\]' components/extent-node/src --include='*.rs'
```

For each hit, decide: is the item now used? If yes, remove the attribute; if no, delete the item. `WriteBatch.reply`, `WriteBatch::with_reply`, and the arena-level delegation channels stay — they're dead in Dedicated but the P3 plan wires them. Keep `#[allow(dead_code)]` on those, with a comment `// P3: SharedArenaPool wires this; unused in Dedicated path.`

- [ ] **Step 2: Verify**

Run: `RUSTFLAGS="-D warnings" cargo check --workspace --tests --benches 2>&1 | tail -10`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "chore(extent-node): strip dead-code attrs on now-live arena types"
```

---

## Phase 3: Strip `extent_capacity` from `ForwardInitEpoch`

Goal: Primary and Secondary each size arenas from their own EN config. The spec-intended `dedicated_arena_size` / `shared_arena_size` are EN-local.

### Task 3.1: Wire-format change

**Files:**
- Modify: `components/rpc/src/frame/header.rs`
- Modify: `components/rpc/src/frame/encode.rs`
- Modify: `components/rpc/src/frame/decode.rs`
- Modify: `components/rpc/src/frame/tests.rs`
- Modify: `components/extent-node/src/store/forward.rs`

- [ ] **Step 1: Remove the field**

In `components/rpc/src/frame/header.rs:247`:

Before:
```rust
ForwardInitEpoch {
    stream_id: StreamId,
    epoch: Epoch,
    start_offset: Offset,
    extent_capacity: u32,
    cache_extents: u16,
    arena_class: ArenaClass,
    storage_class: StorageClass,
},
```

After:
```rust
ForwardInitEpoch {
    stream_id: StreamId,
    epoch: Epoch,
    start_offset: Offset,
    cache_extents: u16,
    arena_class: ArenaClass,
    storage_class: StorageClass,
},
```

- [ ] **Step 2: Update encoder**

In `components/rpc/src/frame/encode.rs`, find `ForwardInitEpoch`, drop the 4-byte `extent_capacity` from the size calc and write body.

- [ ] **Step 3: Update decoder**

In `components/rpc/src/frame/decode.rs`, drop the `extent_capacity` read.

- [ ] **Step 4: Update secondary handler**

In `components/extent-node/src/store/forward.rs::handle_forward_init_epoch`, replace `extent_capacity` uses with `self.config.epoch_capacity` (read from `ExtentNodeConfig`). The config field already exists (it's the replacement for the adaptive-capacity fields deleted in P1).

- [ ] **Step 5: Fix round-trip tests**

Frame tests in `components/rpc/src/frame/tests.rs` that constructed `ForwardInitEpoch { ..., extent_capacity: X }` drop the field; byte-offset expectations shift by −4.

- [ ] **Step 6: Verify + commit**

```bash
cargo test --workspace 2>&1 | tail -10
```
Expected: all pass.

```bash
git add -A
git commit -m "$(cat <<'EOF'
refactor(wire): drop extent_capacity from ForwardInitEpoch

Per shared-arena spec, each EN sizes arenas from its own config
(`ExtentNodeConfig.epoch_capacity`). The field was dead cargo on the
wire and would actively mislead Shared-class secondaries (P3) that
size their shared arenas independently from the primary.

Saves 4 bytes per epoch initialization on the wire.
EOF
)"
```

---

## Phase 4: Naming Cleanup

### Task 4.1: Fix stale header comment

**Files:**
- Modify: `components/extent-node/src/arena/mod.rs`

- [ ] **Step 1: Replace header**

Before:
```rust
//! Arena-level building blocks used by Extent (to be renamed to
//! StreamEpoch in a later task).
//!
//! Extracted from extent.rs so the same primitives back both Dedicated
//! (one stream per arena) and Shared (many streams per arena, added in
//! a later plan) pools.
```

After:
```rust
//! Arena-level building blocks backing StreamEpoch.
//!
//! `Arena` is the byte-pool primitive: one buffer + one directory +
//! single-writer cursors. Dedicated streams own a per-epoch vector of
//! `Arc<Arena>` (rotated on arena-full); Shared streams (P3) observe
//! arenas minted by a process-wide `SharedArenaPool`.
```

### Task 4.2: Align `Stream::active_epoch*` with spec

**Files:**
- Modify: `components/extent-node/src/stream.rs`

- [ ] **Step 1: Rename**

- `fn active_epoch_ref(&self) -> Option<Arc<StreamEpoch>>` → `pub fn active_epoch(&self) -> Option<Arc<StreamEpoch>>` (promote to `pub`, drop `_ref` suffix).
- `pub fn active_epoch(&self) -> Option<Epoch>` → `pub fn active_epoch_number(&self) -> Option<Epoch>`.

- [ ] **Step 2: Update call sites**

```bash
grep -rn 'active_epoch_ref\|active_epoch\b' components/ tests/ --include='*.rs'
```

Rename accordingly. Spec-facing callers that want `Option<Arc<StreamEpoch>>` use `active_epoch()`; sites that only want the epoch number use `active_epoch_number()`.

- [ ] **Step 3: Verify + commit**

```bash
cargo test --workspace 2>&1 | tail -10
git add -A
git commit -m "refactor(stream): align active_epoch* method naming with spec vocabulary"
```

---

## Phase 5: Rewrite Unit Tests

Goal: cover the new structure with focused tests. All tests deleted in Phase 1.5 are replaced here.

### Task 5.1: `Arena` unit tests

**Files:**
- Modify: `components/extent-node/src/arena/arena.rs`

- [ ] **Step 1: Add `#[cfg(test)] mod tests`**

Test cases:
- `write_batch_single_record_round_trip`: one job in, one `Ok(ArenaAppendResult)` out, `arena.bytes_written()` equals record length, `arena.record_count()` is 1.
- `write_batch_multiple_records_advance_cursor`: three jobs; assert offsets are `start + 0/1/2` and byte positions are contiguous.
- `write_batch_returns_arena_full_at_boundary`: arena capacity 16 bytes; two 4+4-byte records fit; the third returns `Err(ArenaFull)`; cursor + record_count unchanged.
- `arena_read_round_trip`: write three records via `write_batch`, read three back, assert bytewise equality.
- `arena_contains_offset_checks_range`: verify `contains_offset` at boundary (`start_offset`, `next_offset() - 1`, `next_offset()`).

### Task 5.2: `DedicatedArenaPool` factory tests

**Files:**
- Modify: `components/extent-node/src/arena/pool.rs`

- [ ] **Step 1: Factory test**

```rust
#[test]
fn dedicated_pool_allocate_mints_fresh_arena_each_call() {
    let pool = DedicatedArenaPool::new(Arc::new(ArenaIdGenerator::new(1)));
    let a = pool.allocate(StreamId(1), Epoch(1), Offset(0), 4096);
    let b = pool.allocate(StreamId(1), Epoch(1), Offset(100), 4096);
    assert_ne!(a.arena_id, b.arena_id);
    assert_eq!(a.start_offset, Offset(0));
    assert_eq!(b.start_offset, Offset(100));
    assert_eq!(pool.class(), ArenaClass::Dedicated);
}

#[test]
#[should_panic(expected = "SharedArenaPool::allocate not wired")]
fn shared_pool_allocate_panics_until_p3() {
    let pool = SharedArenaPool::new(Arc::new(ArenaIdGenerator::new(1)), 4096);
    let _ = pool.allocate(StreamId(1), Epoch(1), Offset(0), 4096);
}
```

### Task 5.3: `StreamEpoch` rotation tests

**Files:**
- Modify: `components/extent-node/src/stream_epoch.rs`

- [ ] **Step 1: Add tests**

Helper: `fn test_stream_epoch(capacity: u32) -> StreamEpoch` — constructs a StreamEpoch backed by a `DedicatedArenaPool`.

Test cases:
- `append_inner_single_arena_happy_path`: 3 appends, committed_offset advances correctly, `arenas.lock().len() == 1`.
- `append_inner_rotates_on_arena_full`: arena capacity small enough that the 3rd record triggers rotation. Assert: `arenas.lock().len() == 2`, `resident_arenas.lock().len() == 2`, offsets are contiguous across the boundary, `committed_offset` covers all three.
- `read_at_offset_crosses_arena_boundary`: write 5 records spanning two arenas, read all 5 via `read_at_offset(start, 5)`, assert payloads equal.
- `seal_after_rotation_finalizes_hasher`: write records across 2 arenas, seal, assert `finalized_crc32()` equals `crc32(all records in order)`.
- `write_batch_reports_per_job_errors`: mix of sealed / payload-too-large / ok within one batch; results preserve 1:1 mapping.

### Task 5.4: `store/append.rs` integration tests

**Files:**
- Modify: `components/extent-node/src/store/append.rs` (or a new test module)

- [ ] **Step 1: Multi-arena batch append**

Test that a single `handle_append_batch_inner` call with 10 frames whose total size straddles an arena boundary still succeeds with 10 ACKs and that the stream's `arenas.lock().len() == 2`.

### Task 5.5: Secondary cross-arena replicate

**Files:**
- Modify: `components/extent-node/src/store/forward.rs` (or a new test module)

- [ ] **Step 1: Secondary rotates at a different offset**

Secondary's local arena size is smaller than the primary's. Primary sends 10 Forward frames; secondary rotates mid-stream at a different record boundary. Assert secondary can `read_at_offset` the full range correctly.

### Task 5.6: Commit

```bash
cargo test --workspace 2>&1 | tail -10
```
Expected: all pass, including new tests.

```bash
git add -A
git commit -m "test(extent-node): cover Arena + StreamEpoch rotation + pool factory"
```

---

## Phase 6: Validation + Final Commit

### Task 6.1: Full test run + grep sweep

- [ ] **Step 1: Full workspace tests**

Run:
```bash
RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets 2>&1 | tail -10
cargo test --workspace 2>&1 | tail -10
```
Expected: clippy clean; all tests pass.

- [ ] **Step 2: Sweep stale identifiers**

Run:
```bash
grep -rn 'try_append_active\|EpochFull\b\|EpochFullSnafu\|SealReason::EpochFull' \
  --include='*.rs' components/ tests/ benches/
grep -rn 'extent_capacity' \
  --include='*.rs' components/rpc/ components/extent-node/src/store/
```
Expected: both return no output.

Run:
```bash
grep -rn 'active_epoch_ref' --include='*.rs' components/ tests/
```
Expected: no output.

- [ ] **Step 3: Confirm the hot path goes through the pool**

```bash
grep -n 'write_batch_active\|\.pool\.\|ArenaPool' components/extent-node/src/store/append.rs components/extent-node/src/store/forward.rs components/extent-node/src/stream.rs
```
Expected: matches in all three files, confirming production routes through the pool abstraction.

### Task 6.2: Push + PR

- [ ] **Step 1: Review commit log**

Run: `git log --oneline origin/main..HEAD`
Expected: ~7–9 commits (phases 1, 2, 3, 4, 5, 6).

- [ ] **Step 2: Open PR**

```bash
git push -u origin $(git branch --show-current)
gh pr create --title "Complete P2 arena pool wiring (pre-P3)" --body "$(cat <<'EOF'
## Summary

Closes the P2 gap where `StreamEpoch`, `arena::Arena`, and
`DedicatedArenaPool` existed side-by-side but production bypassed the
pool and called `StreamEpoch::append_inner` directly.

Per conversation decisions:
- `Arena` is the sole byte-pool primitive; `StreamEpoch` holds epoch
  metadata over `SmallVec<[Arc<Arena>; 4]>`.
- `ArenaPool` is a factory trait. Dedicated streams own a per-stream
  `DedicatedArenaPool`; Shared streams (P3) share the EN-wide
  `SharedArenaPool` singleton (stub here, wired in P3).
- Arena-full rotates internally within the same epoch; `EpochFullSnafu`
  is renamed `ArenaFullSnafu` and no longer surfaces past `StreamEpoch`.
- Store hot paths (`handle_append`, `handle_append_batch_inner`,
  `drain_delegated_requests`, secondary `replicate`) assemble `WriteBatch`es
  and call `Stream::write_batch_active`.
- Per-epoch CRC (single hasher covering records across all arenas in
  order); per-arena CRC is not introduced.

## Wire format changes (pre-prod)
- `ForwardInitEpoch` drops `extent_capacity` (4 bytes). Each EN sizes
  arenas from its own `ExtentNodeConfig.epoch_capacity`.

## Blocked path unblocked
P3 (`SharedArenaPool` + `ArenaBacking::Shared`) can now plug in behind
the same `Stream.pool: Arc<dyn ArenaPool>` surface without another
round of `StreamEpoch` restructuring.

## Test plan
- [x] `cargo test --workspace` passes (includes new Arena, pool, and
      multi-arena rotation coverage)
- [x] `cargo clippy --workspace --all-targets` with `-D warnings` clean
- [x] Wire-format changes covered by round-trip tests in
      `components/rpc/src/frame/tests.rs`
EOF
)"
```

---

## Self-Review Checklist

**1. Scope coverage:**
- [x] `StreamEpoch` split into epoch metadata + `Arc<Arena>` list — Phase 1 Task 1.5
- [x] `Arena` owns buffer/directory/cursors — Phase 1 Task 1.2
- [x] `ArenaPool` factory trait; Dedicated per-stream, Shared EN-wide — Phase 1 Tasks 1.3, 1.8
- [x] `Stream.pool: Arc<dyn ArenaPool>` — Phase 1 Task 1.6
- [x] `EpochFull` → `ArenaFull`; no seal on arena-full — Phase 1 Tasks 1.4, 1.7
- [x] `store/append.rs` + `store/forward.rs` flow through pool — Phase 1 Tasks 1.7, 1.8
- [x] Per-epoch CRC, not per-arena — Phase 1 Task 1.5 Step 3
- [x] `ForwardInitEpoch.extent_capacity` dropped — Phase 3
- [x] `arena/mod.rs` stale comment + `active_epoch*` naming — Phase 4
- [x] Tests rewritten — Phase 5

**2. Out of scope (explicit):**
- `SharedArenaPool` real impl — P3
- `ArenaBacking::Shared` variant — P3
- Shape A / Shape B / `epoch_arenas` / `UpdateArenaFlushed` — P3/P4
- Adaptive Dedicated↔Shared transitions — P5
- `ExtentState` collapse (`Active|Sealed|Flushed` → `Open|Sealed`) — separate plan

**3. Broken-build window:**
- Task 1.2 intentionally breaks the build; closed by Task 1.8.
- Phases 2–6 each leave the tree green.

**4. Ambiguity check:**
- Offset assignment rule stated explicitly in Task 1.7 Step 1 (primary: Arena assigns; secondary: Arena validates against Forward's offset).
- Rotated arena's `start_offset` = `committed_offset` at rotation time (Task 1.5 Step 3).
- `WriteBatch.reply` stays with `#[allow(dead_code)]` (Phase 2 Task 2.1 Step 1) — P3 wires it.
- `ArenaId` migrates to `common::types` so `ArenaFullSnafu` can reference it (Task 1.4 Step 3).

**5. Failure modes considered:**
- Secondary observes out-of-order Forward frames → `replicate` returns `InternalSnafu`, connection teardown (existing behavior).
- Record larger than arena capacity → `ArenaFull` on an empty arena; rotation mints another same-sized arena; same error; caller loops forever. **Mitigation:** `StreamEpoch::write_batch` must detect `ArenaFull` on a freshly-rotated empty arena and convert to `InternalSnafu` with a "record too large" message. Add this check in Task 1.5 Step 3 alongside the rotation loop.
