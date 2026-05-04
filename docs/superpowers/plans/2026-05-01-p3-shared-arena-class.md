# P3: Shared Arena Class Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable `ArenaClass::Shared` streams end-to-end. A Shared stream's records flow through an EN-wide `SharedArenaPool` that multiplexes many streams into one arena buffer. On arena roll, the Primary's shared arena is compacted to a Shape A S3 object (per-stream compressed blocks of primary-cohort records); a corresponding `epoch_arenas` row is inserted per (stream, epoch) entry.

**Architecture:**
1. **Multi-entry arena directory.** `ArenaDirectory` widens from a single `EpochArenaEntry` to `papaya::HashMap<(StreamId, Epoch), EpochArenaEntry>` so many streams can share one arena buffer.
2. **Functional `SharedArenaPool`.** Uses `ArcSwap<Arc<SharedArena>>` for the `active` arena and `papaya::HashMap<ArenaId, Arc<SharedArena>>` for resident arenas. Arena-level CAS via `in_flight` elects one writer who memcpies inline and drains the MPSC; other streams delegate via `tx`.
3. **Stream routing by class.** `ExtentNodeStore` owns both a per-stream Dedicated pool factory AND one process-wide `SharedArenaPool`. At `try_create_stream` / `register_extent` time, if the stream's `arena_class == Shared`, `Stream.pool` points at `SharedArenaPool`; otherwise `DedicatedArenaPool` (today's path).
4. **Arena roll.** When a shared arena fills, the arena leader hands it off to the flusher and ArcSwaps a new active. Roll is bounded: the in-progress batch may straddle arenas; per-job `JobResult.arena_id` already captures this.
5. **Shape A compaction + upload.** Fresh flush path that iterates the directory, filters by cohort (`ReplicaInfo.primary_node_id == self.node_id` via stream lookup → epoch lookup → replica info), compacts each primary-cohort entry via `encode_extent` (reused from P1), concatenates into a Shape A container, and uploads to `{namespace}/arenas/{arena_id:016x}.dat`.
6. **UpdateArenaFlushed + SM metadata.** New SM opcode that receives a list of flushed entries and writes one `epoch_arenas` row per entry in a single transaction.
7. **ForwardFlushed on Shared path.** Per-entry ForwardFlushed frames (wire unchanged) sent from Primary to secondaries after Shape A upload so secondary-cohort entries can release their memory.
8. **Eviction.** `SharedArenaPool` gets a global LRU over `resident`; arenas in state `Uploaded` (or `Rolled` on Memory-class streams) are LRU-eligible.

**Tech Stack:** Rust, existing crates (papaya, arc-swap, smallvec, crossbeam, aws-sdk-s3, tracing). No new external deps.

**Scope boundaries (deferred to P4+):**
- **DR flush** (`FlushEpochStream` + Shape B per-stream upload): P4.
- **Runtime promotion/demotion** between Dedicated and Shared: P5. In P3, `arena_class` is read once from MySQL at `create_stream` and never changes.
- **Multi-arena StreamEpoch retention**: already plumbed in P2 (`resident_arenas: SmallVec<[ArenaId; 4]>`, `directory_ref_count`). P3 actually populates and decrements them.
- **Metrics / histograms**: P5.
- **`ExtentState` collapse** (`Active | Sealed | Flushed` → `Open | Sealed`): deferred.
- **Benchmarks**: not added in P3.
- **CreateStream wire format to carry `arena_class`**: leave as-is (always 0 = Dedicated from the client). Shared streams created by patching the stream row's `arena_class` to 1 post-create in tests, until P5 adds a proper client-side selector.

---

## File Structure

**Net new files:**
- `components/extent-node/src/arena/shared_pool.rs` — the real `SharedArenaPool` + `SharedArena` concrete type
- `components/extent-node/src/shape_a.rs` — Shape A object builder (header, directory, concatenated per-entry blocks via reused `s3_codec::encode_extent`)
- `components/stream-manager/migrations/V8__create_epoch_arenas.sql` — new `epoch_arenas` table

**Modified:**
- `components/extent-node/src/arena/directory.rs` — widen to multi-entry HashMap (keep single-entry helper for Dedicated)
- `components/extent-node/src/arena/pool.rs` — `SharedArenaPool` gains real impl; `allocate_epoch` now returns either a fresh StreamEpoch (Dedicated) or a handle that references the shared pool
- `components/extent-node/src/arena/write_batch.rs` — `WriteBatchResult.results` now legitimately heterogeneous per arena_id (multi-arena roll within a batch)
- `components/extent-node/src/stream_epoch.rs` — Shared-class `StreamEpoch` becomes a "virtual" epoch: its `write_batch` delegates to the shared pool rather than owning a local buffer; new field `backing: ArenaBacking` enum
- `components/extent-node/src/stream.rs` — routing based on `arena_class` on `register_extent`; backing dispatch at `append_inner` / `read` / `try_verify_checksum`
- `components/extent-node/src/store/mod.rs` — owns `shared_pool: Arc<SharedArenaPool>` alongside `default_pool`; threads it through Stream construction
- `components/extent-node/src/store/register.rs` — plumbs `arena_class` from the `RegisterEpoch` frame into the right pool
- `components/extent-node/src/store/forward.rs` — ForwardInitEpoch handler honors `arena_class` when materializing the Secondary's StreamEpoch; Shared secondaries also hit the shared pool
- `components/extent-node/src/s3_flusher.rs` — gains Shape A path alongside today's Shape B
- `components/extent-node/src/stream_manager_client.rs` — adds `UpdateArenaFlushed` send
- `components/stream-manager/src/metadata.rs` — `get_stream.arena_class` already plumbed (P2); new `record_arena_flushed(entries: Vec<(StreamId, Epoch, u64, u64)>, arena_id, s3_key)` writes N `epoch_arenas` rows in one transaction
- `components/stream-manager/src/store.rs` — handler for the new `UpdateArenaFlushed` opcode
- `components/rpc/src/frame/header.rs` + `encode.rs` + `decode.rs` + `tests.rs` — new `UpdateArenaFlushed` variant (list of entries)
- `components/common/src/types.rs` — add `Opcode::UpdateArenaFlushed` if a new opcode is needed, OR reuse `Opcode::UpdateExtent` with a new flag byte

This structure informs the task decomposition. Each task produces self-contained changes that build and test clean on their own.

---

## Phase Order + Build-Broken Windows

The plan ships in 9 phases. The build stays green at the end of every phase. Mid-phase windows happen only where explicitly noted.

1. **Phase 0** — Inventory + baseline
2. **Phase 1** — Multi-entry `ArenaDirectory` (widen the struct; Dedicated still uses exactly one entry)
3. **Phase 2** — `SharedArena` concrete struct (per-arena buffer + multi-entry directory + CAS state + backing linkage)
4. **Phase 3** — `SharedArenaPool` with `active`/`resident`/LRU; `allocate_epoch` returns a shared-backing StreamEpoch
5. **Phase 4** — `StreamEpoch.backing: ArenaBacking` enum (Dedicated: owns buffer; Shared: references a SharedArena). Write path dispatches. Read path dispatches. Arena roll handled end-to-end.
6. **Phase 5** — Wire routing: `Stream.pool` chosen by `arena_class`; register_extent / ForwardInitEpoch honor class end-to-end
7. **Phase 6** — Shape A compaction + S3 upload (reusing `encode_extent`)
8. **Phase 7** — `UpdateArenaFlushed` wire + SM handler + `epoch_arenas` table + ForwardFlushed broadcast after upload
9. **Phase 8** — Validation + PR

**Estimated commits:** 20-22. Largest phases: Phase 4 (arena backing dispatch — the most intrusive hot-path change) and Phase 6 (Shape A builder).

---

## Phase 0: Inventory + Baseline

### Task 0.1: Verify dependencies + baseline

**Files:** (read-only)

- [ ] **Step 1: Confirm deps**

Run: `cargo tree --workspace 2>&1 | grep -iE 'papaya|arc-swap|smallvec|crossbeam|aws-sdk-s3' | head -10`
Expected: all present.

- [ ] **Step 2: Confirm build baseline**

Run: `RUSTFLAGS="-D warnings" cargo check --workspace --tests --benches 2>&1 | tail -5`
Expected: `Finished \`dev\` profile`, no warnings.

Run: `cargo test --lib --workspace 2>&1 | grep -E '^test result:' | awk '{ p+=$4; f+=$6 } END { printf "passed=%d failed=%d\n", p, f }'`
Expected: `passed=144 failed=0`.

- [ ] **Step 3: Snapshot the Shared-path surface**

Run:
```bash
grep -rn 'SharedArenaPool\|SharedArena\b\|ArenaBacking\|epoch_arenas\|UpdateArenaFlushed\|Shape A\|shape_a' components/ tests/ --include='*.rs' | wc -l
```

Record the baseline count. After P3 lands, this count climbs substantially.

No commit for Phase 0 — pure reconnaissance.

---

## Phase 1: Multi-entry ArenaDirectory

Goal: widen `ArenaDirectory` from holding one `EpochArenaEntry` to holding `papaya::HashMap<(StreamId, Epoch), EpochArenaEntry>` so many streams can share the same arena buffer. Dedicated keeps using exactly one entry (enforced at construction).

### Task 1.1: Widen ArenaDirectory

**Files:**
- Modify: `components/extent-node/src/arena/directory.rs`

- [ ] **Step 1: Rewrite the ArenaDirectory struct**

In `components/extent-node/src/arena/directory.rs`, replace the current single-entry `ArenaDirectory` with:

```rust
use papaya::HashMap as PapayaMap;

pub(crate) struct ArenaDirectory {
    // Per-(stream, epoch) record placement. Multi-entry for Shared arenas;
    // Dedicated arenas hold exactly one entry.
    entries: PapayaMap<(StreamId, Epoch), EpochArenaEntry>,
}

impl ArenaDirectory {
    /// Empty directory. For Shared arenas; entries are inserted lazily as
    /// new (stream, epoch) tuples arrive.
    pub(crate) fn empty() -> Self {
        Self { entries: PapayaMap::new() }
    }

    /// Pre-populated with a single entry. For Dedicated arenas; every
    /// append goes into this one entry.
    pub(crate) fn with_single(entry: EpochArenaEntry) -> Self {
        let d = Self::empty();
        let guard = d.entries.guard();
        d.entries.insert((entry.stream_id, entry.epoch), entry, &guard);
        d
    }

    /// Get the entry for `(stream, epoch)`, or None if no records for that
    /// tuple live in this arena. Takes a closure so the borrow does not
    /// escape the guard.
    pub(crate) fn with_entry<F, R>(
        &self,
        key: (StreamId, Epoch),
        f: F,
    ) -> Option<R>
    where
        F: FnOnce(&EpochArenaEntry) -> R,
    {
        let guard = self.entries.guard();
        self.entries.get(&key, &guard).map(f)
    }

    /// Get or insert an entry. Returns a clone of the newly-inserted entry
    /// via the provided `make` closure if absent. The closure is called
    /// only if the key is missing (one-time allocation of the flat
    /// byte_positions table).
    pub(crate) fn get_or_insert_with<F>(
        &self,
        key: (StreamId, Epoch),
        make: F,
    ) -> ()
    where
        F: FnOnce() -> EpochArenaEntry,
    {
        let guard = self.entries.guard();
        if self.entries.get(&key, &guard).is_none() {
            self.entries.insert(key, make(), &guard);
        }
    }

    /// Number of distinct (stream, epoch) entries.
    pub(crate) fn entry_count(&self) -> usize {
        self.entries.pin().len()
    }

    /// Iterate entries. Takes a closure since the papaya guard is tied
    /// to the iteration lifetime.
    pub(crate) fn for_each<F>(&self, mut f: F)
    where
        F: FnMut(&(StreamId, Epoch), &EpochArenaEntry),
    {
        let guard = self.entries.guard();
        for (k, v) in self.entries.iter(&guard) {
            f(k, v);
        }
    }

    /// Remove the entry for `(stream, epoch)`. Returns whether the key
    /// was present.
    pub(crate) fn remove(&self, key: (StreamId, Epoch)) -> bool {
        let guard = self.entries.guard();
        self.entries.remove(&key, &guard).is_some()
    }
}
```

- [ ] **Step 2: Rewrite the Dedicated-path call sites**

In `components/extent-node/src/stream_epoch.rs`, `StreamEpoch::with_capacity` currently calls `EpochArenaEntry::with_capacity(...)` and wraps in `ArenaDirectory::new(entry)`. Change to:

```rust
let entry = EpochArenaEntry::with_capacity(
    StreamId(0),  // filled in Phase 5 when class routing lands
    epoch,
    start_offset,
    record_cap,
);
let directory = ArenaDirectory::with_single(entry);
```

The accessors in `stream_epoch.rs` that today read `self.directory.single_entry()` — let me grep:

```bash
grep -n 'single_entry' components/extent-node/src/stream_epoch.rs
```

For each hit, rewrite to lookup by `(self.stream_id_placeholder, self.epoch)`. Since P2 used `StreamId(0)` as a placeholder, it's fine to continue using it here — the point is that the lookup works as long as the key matches construction.

Actually simpler: keep a `single_entry` helper on ArenaDirectory that internally calls `with_entry((StreamId(0), self.epoch), …)` for backward compatibility on the Dedicated path. Signature:

```rust
impl ArenaDirectory {
    /// Dedicated fast path: returns the one entry keyed by (StreamId(0), epoch).
    /// Panics if the arena is multi-entry.
    pub(crate) fn single_entry(&self, epoch: Epoch) -> SingleEntryGuard<'_> { ... }
}
```

**Simpler alternative.** Keep today's `single_entry()` method on the directory but have it return an owned wrapper or closure. The cleanest path: add `with_single_entry<F, R>(&self, f: F) -> R` on `ArenaDirectory` that calls `for_each` and invokes `f` on the (one and only) entry:

```rust
impl ArenaDirectory {
    /// Apply `f` to the directory's single entry. Panics if the
    /// directory is empty or multi-entry. For Dedicated-path callers
    /// migrated from the P2 single_entry() API.
    pub(crate) fn with_single_entry<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&EpochArenaEntry) -> R,
    {
        let guard = self.entries.guard();
        let mut iter = self.entries.iter(&guard);
        let (_k, v) = iter.next().expect("directory empty");
        assert!(iter.next().is_none(), "directory is multi-entry");
        f(v)
    }
}
```

Then `stream_epoch.rs`'s `index_record(seq, byte_pos)` becomes `self.directory.with_single_entry(|e| e.record(seq, byte_pos))`, and `index_lookup(seq)` becomes `self.directory.with_single_entry(|e| e.lookup(seq))`. Same for `try_advance_committed`'s `entry.raw_slot(seq)` call — wrap in `with_single_entry`. Phase 4 revises this when Shared arenas need per-key dispatch.

- [ ] **Step 3: Build + test**

Run: `cargo check --workspace --tests --benches 2>&1 | tail -10` → clean
Run: `cargo test --lib --workspace 2>&1 | grep -E '^test result:' | awk '{ p+=$4; f+=$6 } END { printf "passed=%d failed=%d\n", p, f }'` → 144.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "$(cat <<'EOF'
refactor(arena): widen ArenaDirectory to multi-entry (papaya::HashMap)

ArenaDirectory now holds papaya::HashMap<(StreamId, Epoch),
EpochArenaEntry> so Shared arenas (introduced in a later phase) can
multiplex many streams into one buffer. Dedicated arenas continue to
have exactly one entry, constructed via `with_single(entry)` and
accessed via `with_single_entry(|e| ...)` which asserts the invariant.

No behavior change for today's Dedicated path.
EOF
)"
```

---

## Phase 2: SharedArena Concrete Struct

Goal: define the per-arena struct that a `SharedArenaPool` will own. Same shape as `StreamEpoch`'s buffer + directory + CAS state, but sized for the shared `arena_size` and not owned by any one stream.

### Task 2.1: SharedArena struct

**Files:**
- Create: `components/extent-node/src/arena/shared_arena.rs`
- Modify: `components/extent-node/src/arena/mod.rs`

- [ ] **Step 1: Write the struct**

Create `components/extent-node/src/arena/shared_arena.rs`:

```rust
//! A single shared arena: one buffer, multi-entry directory, CAS-based
//! leader election. Owned by `SharedArenaPool`.

use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::Instant;

use bytes::Bytes;
use crossbeam_channel::{Receiver, Sender, unbounded};
use tokio::sync::oneshot;

use common::errors::StorageError;

use crate::arena::{ArenaBuffer, ArenaDirectory, ArenaId, OwnedArenaSlice, WriteBatch, WriteBatchResult};

/// Arena state machine. Mirrors the spec (§ Arena Roll).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SharedArenaState {
    Open = 0,
    Rolled = 1,
    Uploaded = 2,
    Evicted = 3,
}

pub(crate) struct SharedArena {
    pub(crate) id: ArenaId,
    pub(crate) buf: Arc<ArenaBuffer>,
    pub(crate) state: AtomicU8,
    pub(crate) created_at: Instant,

    /// Multi-entry directory: one entry per (stream, epoch) that has
    /// written at least one record to this arena.
    pub(crate) directory: ArenaDirectory,

    /// Arena-level leader election. Matches the spec's in_flight CAS.
    pub(crate) in_flight: AtomicU64,

    /// Delegation channel for followers. Crossbeam bounded to
    /// `shared_writer_channel_capacity` (see config).
    pub(crate) tx: Sender<WriteBatch>,
    pub(crate) rx: Receiver<WriteBatch>,

    /// Write cursor (bytes). Single-writer, updated by whichever
    /// stream leader is currently the arena leader.
    pub(crate) write_cursor: AtomicU64,

    /// Capacity in bytes. Copy of `buf.capacity()` for hot-path access.
    pub(crate) capacity: u32,

    /// CRC32 incremental hasher — single-writer like on StreamEpoch.
    /// Used for the Shape A object's crc32 header field (populated at
    /// roll time).
    pub(crate) hasher: UnsafeCell<crc32fast::Hasher>,
}

// SAFETY: write_cursor + directory guard the single-writer invariant.
unsafe impl Send for SharedArena {}
unsafe impl Sync for SharedArena {}

impl SharedArena {
    pub(crate) fn new(id: ArenaId, arena_size: u32) -> Self {
        let buf = ArenaBuffer::new(arena_size);
        let (tx, rx) = unbounded();
        Self {
            id,
            buf,
            state: AtomicU8::new(SharedArenaState::Open as u8),
            created_at: Instant::now(),
            directory: ArenaDirectory::empty(),
            in_flight: AtomicU64::new(0),
            tx,
            rx,
            write_cursor: AtomicU64::new(0),
            capacity: arena_size,
            hasher: UnsafeCell::new(crc32fast::Hasher::new()),
        }
    }

    pub(crate) fn state(&self) -> SharedArenaState {
        match self.state.load(Ordering::Acquire) {
            0 => SharedArenaState::Open,
            1 => SharedArenaState::Rolled,
            2 => SharedArenaState::Uploaded,
            _ => SharedArenaState::Evicted,
        }
    }

    pub(crate) fn set_state(&self, s: SharedArenaState) {
        self.state.store(s as u8, Ordering::Release);
    }

    /// Current bytes consumed. For backpressure + roll detection.
    pub(crate) fn bytes_used(&self) -> u64 {
        self.write_cursor.load(Ordering::Acquire)
    }

    /// Remaining free bytes in the arena.
    pub(crate) fn remaining(&self) -> u64 {
        self.capacity as u64 - self.bytes_used()
    }
}
```

- [ ] **Step 2: Export from arena/mod.rs**

Add `mod shared_arena;` and `pub(crate) use shared_arena::{SharedArena, SharedArenaState};` to `components/extent-node/src/arena/mod.rs`.

- [ ] **Step 3: Build**

Run: `cargo check --workspace --tests --benches 2>&1 | tail -5` → clean

- [ ] **Step 4: Commit**

```bash
git add components/extent-node/src/arena
git commit -m "$(cat <<'EOF'
feat(arena): SharedArena concrete struct

One SharedArena per shared arena buffer. Owned by SharedArenaPool
(next task). Holds the buffer, multi-entry ArenaDirectory, CAS-based
leader election state (in_flight + crossbeam MPSC for delegated
WriteBatches), write cursor, and incremental CRC32 hasher.

State machine: Open -> Rolled -> Uploaded -> Evicted per the
shared-arena spec.

Not yet routed; P2 Dedicated path is unchanged.
EOF
)"
```

---

## Phase 3: SharedArenaPool with active/resident/LRU

Goal: replace the P2 stub `SharedArenaPool` with a working implementation. One pool per EN. `ArcSwap<Arc<SharedArena>>` for the active arena; `papaya::HashMap<ArenaId, Arc<SharedArena>>` for resident arenas; an intrusive LRU list keyed by ArenaId.

### Task 3.1: SharedArenaPool fields + roll path

**Files:**
- Modify: `components/extent-node/src/arena/pool.rs`

- [ ] **Step 1: Replace the SharedArenaPool stub**

Current (stub):
```rust
pub(crate) struct SharedArenaPool {
    _arena_size: u32,
    _ids: Arc<ArenaIdGenerator>,
}
```

New:
```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use arc_swap::ArcSwap;
use papaya::HashMap as PapayaMap;
use parking_lot::Mutex;

use crate::arena::{ArenaId, ArenaIdGenerator, SharedArena, SharedArenaState};

pub(crate) struct SharedArenaPool {
    cfg: SharedArenaConfig,
    ids: Arc<ArenaIdGenerator>,

    /// The currently-active arena. All new WriteBatches land here.
    /// Swapped via arc-swap on arena roll.
    active: ArcSwap<SharedArena>,

    /// All resident arenas (Open + Rolled + Uploaded), keyed by id.
    /// Arenas transition Open -> Rolled on roll; -> Uploaded when
    /// Shape A upload completes; -> Evicted when the LRU removes
    /// them from `resident`.
    resident: PapayaMap<ArenaId, Arc<SharedArena>>,

    /// LRU access order (most-recently-used at front). Entry is
    /// removed from `resident` when evicted. Simple Mutex<VecDeque>
    /// since LRU writes are rare (touch on read miss, evict on
    /// overflow).
    lru: Mutex<std::collections::VecDeque<ArenaId>>,

    /// Count of currently-resident arenas. Gauge.
    resident_count: AtomicU32,
}

#[derive(Debug, Clone)]
pub(crate) struct SharedArenaConfig {
    pub(crate) arena_size: u32,
    pub(crate) max_resident_shared_arenas: u32,
}
```

- [ ] **Step 2: new() + roll() + evict_oldest()**

```rust
impl SharedArenaPool {
    pub(crate) fn new(cfg: SharedArenaConfig, ids: Arc<ArenaIdGenerator>) -> Arc<Self> {
        let first_id = ids.next();
        let first = Arc::new(SharedArena::new(first_id, cfg.arena_size));
        let pool = Arc::new(Self {
            cfg,
            ids,
            active: ArcSwap::from(Arc::clone(&first)),
            resident: PapayaMap::new(),
            lru: Mutex::new(std::collections::VecDeque::new()),
            resident_count: AtomicU32::new(0),
        });
        pool.insert_resident(Arc::clone(&first));
        pool
    }

    pub(crate) fn active(&self) -> Arc<SharedArena> {
        self.active.load_full()
    }

    pub(crate) fn get(&self, id: ArenaId) -> Option<Arc<SharedArena>> {
        let guard = self.resident.guard();
        self.resident.get(&id, &guard).cloned()
    }

    fn insert_resident(&self, arena: Arc<SharedArena>) {
        let id = arena.id;
        let guard = self.resident.guard();
        self.resident.insert(id, arena, &guard);
        self.lru.lock().push_front(id);
        self.resident_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Roll the currently-active arena. Marks the old Open->Rolled,
    /// mints a fresh Open arena, and swaps it into `active`. The
    /// rolled arena stays in `resident` until upload + LRU eviction.
    ///
    /// Called by whichever stream leader detects that the next record
    /// won't fit in the current arena.
    pub(crate) fn roll(&self, expected_id: ArenaId) -> Arc<SharedArena> {
        // Idempotent if someone else already rolled: check state.
        let current = self.active.load_full();
        if current.id != expected_id {
            return current; // someone else rolled; caller can retry on the new one
        }
        current.set_state(SharedArenaState::Rolled);
        let new_id = self.ids.next();
        let new_arena = Arc::new(SharedArena::new(new_id, self.cfg.arena_size));
        self.active.store(Arc::clone(&new_arena));
        self.insert_resident(Arc::clone(&new_arena));
        // TODO(P3 Phase 6): hand `current` off to the Shape A flush task.
        new_arena
    }

    /// After Shape A upload completes, the flusher calls this to flip
    /// the arena's state and make it LRU-eligible.
    pub(crate) fn mark_uploaded(&self, id: ArenaId) {
        if let Some(a) = self.get(id) {
            a.set_state(SharedArenaState::Uploaded);
        }
        self.maybe_evict();
    }

    fn maybe_evict(&self) {
        let max = self.cfg.max_resident_shared_arenas;
        while self.resident_count.load(Ordering::Relaxed) > max {
            let victim = {
                let mut lru = self.lru.lock();
                // Scan from the back for an eligible victim: Uploaded,
                // and not the active one.
                let active_id = self.active.load().id;
                let mut picked = None;
                for (idx, id) in lru.iter().enumerate().rev() {
                    if *id == active_id { continue; }
                    let guard = self.resident.guard();
                    if let Some(a) = self.resident.get(id, &guard) {
                        if a.state() == SharedArenaState::Uploaded {
                            picked = Some((idx, *id));
                            break;
                        }
                    }
                }
                match picked {
                    Some((idx, id)) => { lru.remove(idx); Some(id) }
                    None => None,
                }
            };
            match victim {
                Some(id) => {
                    let guard = self.resident.guard();
                    if let Some(a) = self.resident.remove(&id, &guard) {
                        a.set_state(SharedArenaState::Evicted);
                        self.resident_count.fetch_sub(1, Ordering::Relaxed);
                    }
                }
                None => break, // nothing evictable yet
            }
        }
    }

    pub(crate) fn resident_count(&self) -> u32 {
        self.resident_count.load(Ordering::Relaxed)
    }
}
```

- [ ] **Step 3: Remove the old stub `allocate_epoch` on SharedArenaPool**

The P2 trait impl on `SharedArenaPool` is `panic!`. It stays a panic for now — `allocate_epoch` in the Shared class doesn't make sense because a Shared epoch doesn't own an arena; Phase 4 introduces `StreamEpoch::new_shared(...)` which takes an `Arc<SharedArenaPool>` instead of allocating a buffer. The pool's `impl ArenaPool` is kept only so any mistaken caller trips `panic!` with a clear message; the trait's `allocate_epoch` signature stays unchanged.

- [ ] **Step 4: Build**

Run: `cargo check --workspace --tests --benches 2>&1 | tail -10` → clean

- [ ] **Step 5: Commit**

```bash
git add components/extent-node/src/arena/pool.rs
git commit -m "$(cat <<'EOF'
feat(arena): functional SharedArenaPool (active / resident / LRU)

SharedArenaPool now holds:
- active: ArcSwap<Arc<SharedArena>>
- resident: papaya::HashMap<ArenaId, Arc<SharedArena>>
- lru: Mutex<VecDeque<ArenaId>>
- resident_count: AtomicU32 gauge

Operations:
- new(cfg, ids): allocates the first Open arena
- active(): Arc load of current active
- get(id): lookup resident by id
- roll(expected_id): idempotent Open->Rolled + swap in a fresh Open
- mark_uploaded(id): Rolled->Uploaded; triggers LRU eviction
- maybe_evict(): drop Uploaded arenas oldest-first when over max

Not yet routed; the stub `ArenaPool::allocate_epoch` on SharedArenaPool
still panics because a shared-class StreamEpoch does not own an arena
(added in Phase 4).
EOF
)"
```

---

## Phase 4: StreamEpoch.backing + write/read dispatch

Goal: make `StreamEpoch` polymorphic over Dedicated (owns its own buffer) vs Shared (references a SharedArenaPool + current arena snapshot). Write path dispatches; read path dispatches. Arena roll handled end-to-end when a Shared StreamEpoch detects the active arena can't fit the next record.

This is the biggest single phase. Break into 4 tasks.

### Task 4.1: ArenaBacking enum

**Files:**
- Modify: `components/extent-node/src/stream_epoch.rs`

- [ ] **Step 1: Define the enum**

Near the top of `stream_epoch.rs`:

```rust
pub(crate) enum ArenaBacking {
    /// Dedicated: the epoch owns its own buffer + directory.
    Dedicated {
        arena_buf: Arc<ArenaBuffer>,
        /// Raw write pointer derived from arena_buf.ptr_mut().
        /// Single-writer invariant upheld by Stream.in_flight.
        buf: *mut u8,
        write_cursor: AtomicU64,
        committed_bytes: AtomicU64,
        capacity: u32,
    },
    /// Shared: the epoch references a pool; appends go through the
    /// pool's currently-active arena.
    Shared {
        pool: Arc<crate::arena::SharedArenaPool>,
    },
}
// SAFETY: Dedicated.buf is derived from the Arc<ArenaBuffer> in the same variant; upheld by single-writer invariant.
unsafe impl Send for ArenaBacking {}
unsafe impl Sync for ArenaBacking {}
```

- [ ] **Step 2: Move Dedicated fields off the top-level struct**

`StreamEpoch` currently has inline `arena`, `buf`, `write_cursor`, `committed_bytes`, `capacity`, etc. Move the Dedicated-specific ones into `ArenaBacking::Dedicated`. `record_count`, `committed_offset`, `limit`, `flags`, `hasher`, `finalized_crc32`, `directory` (for Dedicated path), `resident_arenas`, `directory_ref_count`, `in_flight`, `tx`, `rx` stay on the outer struct.

Wait: `directory` is Dedicated-specific in P3. Keep it on the outer struct for Dedicated; Shared epochs don't have a single `directory` — they write into the pool's currently-active `SharedArena.directory`. Move `directory` into `ArenaBacking::Dedicated`.

New struct shape:

```rust
pub struct StreamEpoch {
    pub id: ExtentId,
    pub start_offset: Offset,
    pub epoch: Epoch,
    pub stream_id: StreamId,  // NEW: Shared path needs it to key directory lookups
    pub arena_id: ArenaId,    // Dedicated: stamped at allocation; Shared: most-recently-resident arena id at construction (replaced lazily as arenas roll — tracked in resident_arenas)

    backing: ArenaBacking,

    // Shared across classes:
    record_count: AtomicU64,
    committed_offset: AtomicU64,
    limit: AtomicU64,
    flags: AtomicU8,
    hasher: UnsafeCell<crc32fast::Hasher>,
    finalized_crc32: AtomicU32,
    pub(crate) resident_arenas: Mutex<SmallVec<[ArenaId; 4]>>,
    pub(crate) directory_ref_count: AtomicU32,
    pub(crate) in_flight: AtomicU64,
    pub(crate) tx: Sender<WriteBatch>,
    pub(crate) rx: Receiver<WriteBatch>,
}
```

- [ ] **Step 3: Split with_capacity into Dedicated + Shared constructors**

```rust
impl StreamEpoch {
    pub(crate) fn new_dedicated(
        id: ExtentId,
        stream_id: StreamId,
        start_offset: Offset,
        capacity: u32,
        epoch: Epoch,
        arena_id: ArenaId,
    ) -> Self {
        let arena_buf = ArenaBuffer::new(capacity);
        let buf = arena_buf.ptr_mut();
        let record_cap = (capacity / MIN_RECORD_SIZE) as usize;
        let entry = EpochArenaEntry::with_capacity(stream_id, epoch, start_offset, record_cap);
        let directory = ArenaDirectory::with_single(entry);
        let (tx, rx) = unbounded();
        Self {
            id,
            start_offset,
            epoch,
            stream_id,
            arena_id,
            backing: ArenaBacking::Dedicated {
                arena_buf,
                buf,
                write_cursor: AtomicU64::new(0),
                committed_bytes: AtomicU64::new(0),
                capacity,
                directory,
            },
            // shared state:
            record_count: AtomicU64::new(0),
            committed_offset: AtomicU64::new(start_offset.0),
            limit: AtomicU64::new(LIMIT_OPEN),
            flags: AtomicU8::new(FLAG_INIT_FORWARD),
            hasher: UnsafeCell::new(crc32fast::Hasher::new()),
            finalized_crc32: AtomicU32::new(0),
            resident_arenas: Mutex::new(smallvec![arena_id]),
            directory_ref_count: AtomicU32::new(1),
            in_flight: AtomicU64::new(0),
            tx, rx,
        }
    }

    pub(crate) fn new_shared(
        id: ExtentId,
        stream_id: StreamId,
        start_offset: Offset,
        epoch: Epoch,
        pool: Arc<crate::arena::SharedArenaPool>,
    ) -> Self {
        let initial_arena_id = pool.active().id;
        let (tx, rx) = unbounded();
        Self {
            id,
            start_offset,
            epoch,
            stream_id,
            arena_id: initial_arena_id,
            backing: ArenaBacking::Shared { pool },
            record_count: AtomicU64::new(0),
            committed_offset: AtomicU64::new(start_offset.0),
            limit: AtomicU64::new(LIMIT_OPEN),
            flags: AtomicU8::new(FLAG_INIT_FORWARD),
            hasher: UnsafeCell::new(crc32fast::Hasher::new()),
            finalized_crc32: AtomicU32::new(0),
            resident_arenas: Mutex::new(smallvec![initial_arena_id]),
            directory_ref_count: AtomicU32::new(1),
            in_flight: AtomicU64::new(0),
            tx, rx,
        }
    }

    /// Back-compat wrapper for P2 callers (tests, etc.) — defaults to Dedicated
    /// with StreamId(0) placeholder.
    #[deprecated(note = "Use new_dedicated with an explicit stream_id")]
    pub(crate) fn with_capacity(
        id: ExtentId,
        start_offset: Offset,
        capacity: u32,
        epoch: Epoch,
        arena_id: ArenaId,
    ) -> Self {
        Self::new_dedicated(id, StreamId(0), start_offset, capacity, epoch, arena_id)
    }
}
```

- [ ] **Step 4: Update every `StreamEpoch::with_capacity` caller**

Grep: `grep -rn 'StreamEpoch::with_capacity' components/ --include='*.rs'`. Keep the back-compat wrapper for test call sites so they don't need to change; production callers (`DedicatedArenaPool::allocate_epoch`, `stream.rs::register_extent`) move to `StreamEpoch::new_dedicated` with the real `stream_id`.

- [ ] **Step 5: Build + test**

Run: `cargo check --workspace --tests --benches 2>&1 | tail -10` → clean (expect `#[deprecated]` warnings, silence them on test callers via `#[allow(deprecated)]` at the module level if noisy).
Run: tests. → 144 passes.

- [ ] **Step 6: Commit**

### Task 4.2: Dispatch append_inner over backing

**Files:**
- Modify: `components/extent-node/src/stream_epoch.rs`

- [ ] **Step 1: Split append_inner**

Today's `append_inner(payload) -> Result<AppendResult, StorageError>` assumes Dedicated. Split:

```rust
impl StreamEpoch {
    pub(crate) fn append_inner(&self, payload: Bytes) -> Result<AppendResult, StorageError> {
        match &self.backing {
            ArenaBacking::Dedicated { .. } => self.append_inner_dedicated(payload),
            ArenaBacking::Shared { pool } => self.append_inner_shared(pool.clone(), payload),
        }
    }

    fn append_inner_dedicated(&self, payload: Bytes) -> Result<AppendResult, StorageError> {
        // Today's body. Reads self.backing as Dedicated { buf, write_cursor, committed_bytes, capacity, directory, ... }.
        // All field accesses that were self.buf / self.write_cursor / self.committed_bytes / self.capacity / self.directory now come from the Dedicated variant via a local binding:
        //     let ArenaBacking::Dedicated { buf, write_cursor, committed_bytes, capacity, directory, .. } = &self.backing else { unreachable!() };
        ...
    }

    fn append_inner_shared(&self, pool: Arc<SharedArenaPool>, payload: Bytes) -> Result<AppendResult, StorageError> {
        // Load the active arena. If the next record would not fit, roll.
        let mut arena = pool.active();
        let record_len = 4 + payload.len() as u64;
        // loop: try memcpy; on ExtentFull, roll and retry. Bounded to 2
        // iterations in practice.
        for _ in 0..2 {
            let cursor_snap = arena.write_cursor.load(Ordering::Acquire);
            if cursor_snap + record_len > arena.capacity as u64 {
                arena = pool.roll(arena.id);
                continue;
            }
            // memcpy
            let byte_pos = arena.write_cursor.fetch_add(record_len, Ordering::AcqRel);
            if byte_pos + record_len > arena.capacity as u64 {
                // raced with another leader; revert and retry
                arena.write_cursor.fetch_sub(record_len, Ordering::AcqRel);
                arena = pool.roll(arena.id);
                continue;
            }
            unsafe {
                let dst = arena.buf.ptr_mut().add(byte_pos as usize);
                let len_be = (payload.len() as u32).to_be_bytes();
                std::ptr::copy_nonoverlapping(len_be.as_ptr(), dst, 4);
                std::ptr::copy_nonoverlapping(payload.as_ptr(), dst.add(4), payload.len());
            }
            // Directory entry for (stream_id, epoch): lazy-create on first touch.
            let seq = self.record_count.fetch_add(1, Ordering::Relaxed);
            let key = (self.stream_id, self.epoch);
            arena.directory.get_or_insert_with(key, || {
                EpochArenaEntry::with_capacity(
                    self.stream_id,
                    self.epoch,
                    self.start_offset,
                    (arena.capacity / MIN_RECORD_SIZE) as usize,
                )
            });
            arena.directory.with_entry(key, |e| e.record(seq, byte_pos + 4)).expect("just inserted");
            // Advance commit markers on self (per-epoch).
            self.committed_offset.store(self.start_offset.0 + seq + 1, Ordering::Release);
            // Track resident arena if not already present.
            {
                let mut r = self.resident_arenas.lock().unwrap();
                if !r.contains(&arena.id) {
                    r.push(arena.id);
                    self.directory_ref_count.fetch_add(1, Ordering::Relaxed);
                }
            }
            return Ok(AppendResult {
                offset: Offset(self.start_offset.0 + seq),
                byte_pos: byte_pos + 4,
            });
        }
        Err(ExtentFullSnafu.build())
    }
}
```

- [ ] **Step 2: Similarly dispatch `append_inner` helpers and `replicate`**

Replicate on a Shared secondary: same shape as append_inner_shared but uses the pre-assigned offset. The payload replay order is strict per TCP FIFO, so a Shared secondary increments its own shared-arena cursor in the same order as the Primary — with independent arena boundaries (different arena_ids per node, matches spec).

- [ ] **Step 3: Build + test**

- [ ] **Step 4: Commit**

### Task 4.3: Dispatch read + index_lookup + try_advance_committed + committed_bytes

Similar pattern: each method grows a `match &self.backing` that calls the Dedicated-specific body (existing code) or the Shared body (iterates resident_arenas to find the entry containing the offset).

### Task 4.4: Dispatch seal / bytes_written / capacity / state accessors

Trivial accessors that read Dedicated fields. Each gets `match &self.backing` with the Shared arm returning aggregated values (total bytes across resident arenas) or an appropriate per-class answer.

---

## Phase 5: Wire routing

Goal: ExtentNodeStore picks the right pool based on `arena_class`; `register_extent` / `ForwardInitEpoch` honor the class from the wire.

### Task 5.1: Plumb shared_pool through ExtentNodeStore

**Files:**
- Modify: `components/extent-node/src/store/mod.rs`
- Modify: `components/extent-node/src/lib.rs`

- [ ] **Step 1: Add shared_pool field**

```rust
pub struct ExtentNodeStore {
    // ... existing ...
    pub(crate) default_pool: Arc<dyn ArenaPool>,
    pub(crate) shared_pool: Arc<SharedArenaPool>,
    pub(crate) arena_ids: Arc<ArenaIdGenerator>,
}
```

- [ ] **Step 2: Construct in new_with_ids**

```rust
pub fn new_with_ids(arena_ids: Arc<ArenaIdGenerator>) -> Self {
    let cfg = SharedArenaConfig {
        arena_size: /* load from ExtentNodeConfig or default */ 64 * 1024 * 1024,
        max_resident_shared_arenas: 64,
    };
    let shared_pool = SharedArenaPool::new(cfg, Arc::clone(&arena_ids));
    // ...
    Self { /* ... */ shared_pool, /* ... */ }
}
```

Add `shared_arena_size` and `max_resident_shared_arenas` to `ExtentNodeConfig` with defaults 64 MiB and 64.

### Task 5.2: register_extent honors arena_class

**Files:**
- Modify: `components/extent-node/src/store/register.rs`
- Modify: `components/extent-node/src/stream.rs`

- [ ] **Step 1: Stream::register_extent grows an arena_class param**

When the SM's `RegisterEpoch` frame arrives, its `StreamConfig.arena_class` is already parsed. Pass it through to `Stream::register_extent(id, start_offset, epoch, capacity, arena_class)`. If Shared, the Stream's register path calls `StreamEpoch::new_shared(...)` using the store's shared_pool; if Dedicated, `StreamEpoch::new_dedicated(...)`.

The `Stream.pool` field established in P2 becomes redundant for routing (it's determined per-epoch) but stays for the autonomous `try_create_next_epoch` path.

### Task 5.3: ForwardInitEpoch honors arena_class on the Secondary

Same plumbing, on the Secondary side in `store/forward.rs`.

### Task 5.4: Integration test — Shared end-to-end on one EN

Add a unit test that:
1. Creates an ExtentNodeStore with node_id
2. Patches a stream's arena_class to Shared manually (until CreateStream wire carries it in P5)
3. Registers an epoch with arena_class = Shared
4. Appends 1000 records across 3 streams sharing one arena
5. Reads them back
6. Rolls the arena (fill it)
7. Verifies second-arena allocation

---

## Phase 6: Shape A compaction + upload

Goal: add a Shape A flush path that iterates the directory of a rolled arena, filters by cohort, compacts each primary-cohort entry (reusing `s3_codec::encode_extent` on a synthesized per-entry StreamEpoch view), concatenates into a Shape A object, uploads to S3.

### Task 6.1: Shape A layout module

**Files:**
- Create: `components/extent-node/src/shape_a.rs`

Define header struct per spec (32-byte fixed header + entry directory + per-entry data). For P3, use `encode_extent` for each entry's data section (gives today's chunk-compressed 64-record blocks) and wrap in the Shape A container.

### Task 6.2: Wire shape_a into s3_flusher

When a rolled arena arrives at the flush task:
1. For each `(stream_id, epoch)` in its directory
2. Look up the StreamEpoch via `ExtentNodeStore.streams.get(stream_id).get_epoch(epoch)` (assumes the epoch is still resident — it is, because its directory_ref_count is non-zero while this entry exists)
3. Check `replica_info.is_primary()` for the (stream, epoch). Skip if secondary-cohort.
4. Synthesize a temporary StreamEpoch view that can be passed to `encode_extent` — OR add a new helper `encode_epoch_arena_entry(arena_buf, entry)` that does the right thing directly on the entry's byte_positions + arena slice.

Prefer the helper path — cleaner than synthesizing a fake StreamEpoch.

### Task 6.3: Upload + mark_uploaded

`shared_pool.mark_uploaded(arena_id)` after S3 PUT succeeds.

---

## Phase 7: UpdateArenaFlushed + SM metadata + ForwardFlushed broadcast

### Task 7.1: epoch_arenas table

**Files:**
- Create: `components/stream-manager/migrations/V8__create_epoch_arenas.sql`

```sql
CREATE TABLE epoch_arenas (
    stream_id    BIGINT UNSIGNED NOT NULL,
    epoch        INT NOT NULL,
    sequence     INT NOT NULL,
    arena_id     BIGINT UNSIGNED NOT NULL,
    s3_key       VARCHAR(512) NOT NULL,
    s3_key_kind  TINYINT UNSIGNED NOT NULL,
    start_offset BIGINT UNSIGNED NOT NULL,
    end_offset   BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (stream_id, epoch, sequence),
    INDEX idx_stream_epoch_offset (stream_id, epoch, start_offset)
);
```

### Task 7.2: UpdateArenaFlushed wire variant

New VariableHeader variant carrying `arena_id`, `s3_key`, and a `Vec<(StreamId, Epoch, start_offset, end_offset)>` list. New `Opcode::UpdateArenaFlushed` byte (pick next free slot).

### Task 7.3: SM handler + MetadataStore method

`MetadataStore::record_arena_flushed(entries, arena_id, s3_key)` inserts N rows in one transaction.

### Task 7.4: EN sends UpdateArenaFlushed after Shape A upload

Wire through the existing StreamManagerClient update channel.

### Task 7.5: ForwardFlushed per entry

After successful Shape A upload, Primary iterates the uploaded entries and sends one ForwardFlushed per (stream_id, epoch) entry to each secondary of that (stream, epoch). Wire shape unchanged from P2.

---

## Phase 8: Validation + PR

### Task 8.1: Full workspace tests + grep sweep

### Task 8.2: Push + PR

---

## Self-Review Notes

- Every spec section referenced is cited (§SharedArenaPool, §SharedArena, §Arena Concurrency Primitive, §Arena Roll, §Shape A, §Cohort-Switch Within a Single Arena).
- Deferred items are explicit: DR path, runtime promotion/demotion, metrics, ExtentState collapse.
- `ArenaBacking` enum is the most intrusive change; split into 4 tasks (4.1–4.4) so each subagent has a bounded context.
- `directory_ref_count` is now a real thing in Shared: incremented when the epoch first writes to an arena that's not already in resident_arenas. It is decremented by Shape A upload release (Phase 6) and ForwardFlushed release on secondaries (Phase 7.5).
