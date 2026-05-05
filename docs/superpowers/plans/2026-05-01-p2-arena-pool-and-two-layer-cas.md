# P2: Arena Pool Abstraction + Two-Layer CAS Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Introduce the arena-pool abstraction and the two-layer CAS plumbing from the shared-arena spec, while keeping every stream on the Dedicated fast path at runtime. When this plan lands, the EN's write path flows through the new pool + arena-level CAS, but no Shared arenas exist yet — P3 will flip streams to `ArenaClass::Shared` end-to-end.

**Architecture:** Three structural changes on the ExtentNode:

1. **Rename `Extent` → `StreamEpoch`** as the per-stream, per-epoch runtime object. Split the raw buffer + directory into `ArenaBuffer` + `ArenaDirectory` structs that the `StreamEpoch` points at. `Stream.epochs` becomes an `ArcSwap<SmallVec<[Arc<StreamEpoch>; 4]>>` replacing `RwLock<Vec<Extent>>`.
2. **Arena pool scaffolding**: introduce an `ArenaPool` trait + a `DedicatedArenaPool` implementation that wraps today's per-stream arena list. Stub out a `SharedArenaPool` skeleton (no runtime consumers yet).
3. **Two-layer CAS write path**: keep the existing stream-level leader election (Layer 1). Introduce a thin arena-level CAS (Layer 2) that is trivially uncontended for Dedicated arenas (one owner per arena). `WriteBatch` and `JobResult` become the hand-off types. The Dedicated path still memcpies directly on the leader thread; the CAS is a no-op fast path.

`ArenaId = (node_id << 48) | local_counter` is introduced as the globally-unique arena identifier. `node_id` comes from the EN's configured `node_id` hashed to 16 bits (matches today's `resolve_advertise_ip`-derived id).

**Tech Stack:** Rust, arc-swap, crossbeam channels (existing), smallvec (new dep), tracing. No new external crates beyond `smallvec` and `arc-swap` (both already transitively in-tree — verify in Task 0.1).

**Scope boundaries (deferred to P3+):**
- `ArenaClass` enum is introduced and plumbed through MySQL + wire, but every stream is allocated with `ArenaClass::Dedicated`. No Shared arenas are actually created at runtime.
- `directory_ref_count` and `resident_arenas` are introduced on `StreamEpoch`, but epoch death is still driven by today's "last extent only" policy — multi-epoch retention is P3.
- `ForwardFlushed` payload format stays as today (per-extent, single entry). Multi-entry `Vec<(stream_id, epoch, start, end)>` payloads are P4 (DR flush).
- No runtime promotion/demotion logic. No EWMA. P5.
- No `FlushEpochStream` / DR path. P4.
- MySQL schema: `stream_epochs.arena_class TINYINT NOT NULL DEFAULT 0` column added; `arena_class` added to `streams` table. No `epoch_arenas` table yet.
- `Forward` wire format unchanged from P1.
- S3 flush keeps today's per-extent identity (Shape B equivalent); Shape A is P4.

---

## File Structure

Net new files:
- `components/extent-node/src/arena/mod.rs` — module root
- `components/extent-node/src/arena/buffer.rs` — `ArenaBuffer` (moved from `extent.rs`)
- `components/extent-node/src/arena/directory.rs` — `ArenaDirectory`, `EpochArenaEntry`
- `components/extent-node/src/arena/id.rs` — `ArenaId` type + generator
- `components/extent-node/src/arena/pool/mod.rs` — `ArenaPool` trait
- `components/extent-node/src/arena/pool/dedicated.rs` — `DedicatedArenaPool`
- `components/extent-node/src/arena/pool/shared.rs` — `SharedArenaPool` stub
- `components/extent-node/src/arena/write_batch.rs` — `WriteBatch`, `JobResult`, `SharedAppendRequest`
- `components/extent-node/src/stream_epoch.rs` — `StreamEpoch` (the renamed `Extent`)
- `components/stream-manager/migrations/V7__add_arena_class.sql` — schema column add

Deleted:
- `components/extent-node/src/extent.rs` — content moved to `stream_epoch.rs` + `arena/buffer.rs`

Modified (rename `Extent`/`extent_id` → `StreamEpoch`/`epoch`-keyed where spec dictates):
- `components/extent-node/src/stream.rs` — big. `StreamInner.extents` → `epochs: ArcSwap<SmallVec<[Arc<StreamEpoch>; 4]>>`. Removes the `RwLock<StreamInner>` around it (atomics + arc-swap replace lock).
- `components/extent-node/src/store/*.rs` — follow `Extent` → `StreamEpoch` rename
- `components/extent-node/src/lib.rs` — `pub mod arena; pub mod stream_epoch;` wiring
- `components/common/src/types.rs` — add `ArenaClass` enum; add `arena_class` field to `ExtentPolicy` (or wherever `RegisterEpoch` payload lives) with default `Dedicated`
- `components/rpc/src/frame/header.rs`, `encode.rs`, `decode.rs` — add `arena_class: u8` to `RegisterEpoch` and `ForwardInitEpoch` variants. Wire byte appends, no other shape changes.
- `components/stream-manager/src/metadata.rs` — read/write `arena_class` column; default `Dedicated`
- `tests/*` — fixture updates where struct fields changed

**CRITICAL:** This is a structural rename + plumbing introduction. It must NOT change externally observable behavior. The Dedicated-only runtime state means Primary/Secondary timing, forward ordering, S3 key shapes, and MySQL row contents must all be byte-identical to post-P1 behavior (modulo the new `arena_class` column, which always stores `0` = `Dedicated` in this plan).

---

## Phase Order + Build-Broken Windows

The plan is sequenced so the build is green at the end of every phase and, where possible, after every task. Phases 1 and 2 will have intentional mid-phase build-broken windows (noted per task); combine the relevant tasks into one subagent dispatch so the window is short.

1. **Phase 0** — Inventory + branch prep (no code changes)
2. **Phase 1** — Introduce `ArenaBuffer` + `ArenaDirectory` as distinct structs inside `extent.rs`. Extent continues to own them. Build stays green the whole phase.
3. **Phase 2** — Rename `Extent` → `StreamEpoch`, move it to `stream_epoch.rs`, wire through all call sites. Field names retained where possible to shrink the diff. This phase has a mid-phase build-broken window.
4. **Phase 3** — Split `Stream.epochs` into `ArcSwap<SmallVec<…>>` with the helper methods from the spec. Remove `StreamInner`'s `RwLock` wrapping of the epoch vec. Keep other `StreamInner` state if still needed; otherwise inline.
5. **Phase 4** — Introduce `ArenaClass` enum + MySQL column + wire field (Dedicated-only default). `RegisterEpoch` / `ForwardInitEpoch` payload shapes grow by one byte.
6. **Phase 5** — Introduce `ArenaPool` trait + `DedicatedArenaPool` wrapping today's per-stream arena list. Add `SharedArenaPool` stub that `panic!("shared arena pool not wired yet")` on every method. Plumb `Stream` to own a `dyn ArenaPool` through `ArenaClass`. Factor the current per-epoch allocate path into pool calls.
7. **Phase 6** — Introduce `ArenaId` (16-bit node id prefix + 48-bit counter). Stamp every arena buffer with its `ArenaId` at allocation. Add `resident_arenas: Mutex<SmallVec<[ArenaId; 4]>>` and `directory_ref_count: AtomicU32` to `StreamEpoch` (still single-entry in practice under Dedicated, but the fields are live).
8. **Phase 7** — Introduce `WriteBatch` / `JobResult` / `SharedAppendRequest` types. Add the arena-level `in_flight` / `request_tx` / `request_rx` on the arena struct. Refactor Dedicated append to go through `ArenaPool::write_batch(batch) -> WriteBatchResult`. The leader still memcpies directly; the CAS is fast-path uncontended.
9. **Phase 8** — Full workspace tests, grep pass, push, PR.

---

## Phase 0: Inventory + Prep

### Task 0.1: Verify dependencies + baseline

**Files:** (read-only inventory)

- [ ] **Step 1: Confirm smallvec and arc-swap are already available**

Run: `cargo tree -p extent-node -e normal 2>&1 | grep -E 'smallvec|arc-swap' | head -5`

Expected: both present (transitively via tokio / papaya). If either is missing, the implementer must add it to `components/extent-node/Cargo.toml` as the first edit of Phase 1.

- [ ] **Step 2: Confirm build baseline**

Run: `cargo check --workspace --tests --benches 2>&1 | tail -5`
Expected: `Finished \`dev\` profile`, no warnings.

Run: `cargo test --lib --workspace 2>&1 | grep -c '^test .* ok$'`
Expected: 136 (or whatever the current count is on `opt/arena`).

- [ ] **Step 3: Snapshot the Extent surface**

Run:
```bash
grep -rn 'ExtentId\b\|\bExtent\b' components/extent-node/src components/stream-manager/src components/common/src components/rpc/src | wc -l
```

Record the count in the task note. After Phase 2 + Phase 3 this count should drop by ≥ 60% (most `Extent` → `StreamEpoch`).

### Task 0.2: Note current Stream shape

Run `grep -n '^    pub\|^    fn\|^    [a-z_]\+:' components/extent-node/src/stream.rs | head -80` and record which methods and fields will be touched by Phase 3.

No commit for Phase 0 — it's pure reconnaissance. Proceed when the numbers above match.

---

## Phase 1: Extract ArenaBuffer + ArenaDirectory from Extent

Goal: Make the arena buffer and the per-record directory into named, independently-testable structs, still owned by the existing `Extent` type. The externally observable behavior of `Extent` is unchanged.

### Task 1.1: Move `ArenaBuffer` to its own module

**Files:**
- Create: `components/extent-node/src/arena/mod.rs`
- Create: `components/extent-node/src/arena/buffer.rs`
- Modify: `components/extent-node/src/extent.rs` (lines ~40-62 removed, `use crate::arena::ArenaBuffer;` added)
- Modify: `components/extent-node/src/lib.rs` (add `pub mod arena;`)

- [ ] **Step 1: Write the ArenaBuffer module**

Create `components/extent-node/src/arena/mod.rs`:

```rust
//! Arena-level building blocks used by `StreamEpoch`.
//!
//! * `ArenaBuffer`: the raw, refcounted heap allocation.
//! * `ArenaDirectory` (added in Task 1.2): per-record byte-position index.
//! * `ArenaPool` (added in Phase 5): allocation + lifecycle trait.
//! * `ArenaId` (added in Phase 6): globally-unique arena identifier.
//!
//! Moved out of `extent.rs` so the same primitives can back both Dedicated
//! (one stream per arena) and Shared (many streams per arena, P3+) pools.

mod buffer;
pub use buffer::{ArenaBuffer, OwnedArenaSlice};
```

Create `components/extent-node/src/arena/buffer.rs` with the exact `ArenaBuffer`, `OwnedArenaSlice`, and `impl`s currently at `extent.rs` lines 40–90 (including the `Send`/`Sync` unsafe impls, `Drop`, `Deref`, and `AsRef<[u8]>`). Make `ArenaBuffer` `pub(crate)` and expose `new(capacity: u32) -> Arc<Self>` plus a `capacity()` accessor plus a `ptr() -> *const u8` accessor. Keep all `unsafe` blocks verbatim — only file location changes.

Reference (copy exactly as-is, just renaming private → `pub(crate)`):

```rust
use std::alloc::{Layout, alloc, dealloc};
use std::ops::Deref;
use std::ptr::NonNull;
use std::sync::Arc;

/// Owns the raw heap allocation for an arena's buffer.
/// Wrapped in `Arc` so that `Bytes` slices keep the buffer alive
/// even after the owning `StreamEpoch` / `SharedArena` is dropped.
pub(crate) struct ArenaBuffer {
    ptr: NonNull<u8>,
    capacity: u32,
    layout: Layout,
}

// SAFETY: The raw allocation is exclusively managed by ArenaBuffer via Arc.
// No aliased mutable access is possible once shared.
unsafe impl Send for ArenaBuffer {}
unsafe impl Sync for ArenaBuffer {}

impl ArenaBuffer {
    pub(crate) fn new(capacity: u32) -> Arc<Self> {
        let layout = Layout::from_size_align(capacity as usize, 4096)
            .expect("valid arena layout");
        // SAFETY: layout is non-zero-sized.
        let raw = unsafe { alloc(layout) };
        let ptr = NonNull::new(raw).expect("arena alloc failed");
        Arc::new(Self { ptr, capacity, layout })
    }

    pub(crate) fn capacity(&self) -> u32 {
        self.capacity
    }

    /// # Safety
    /// Caller must not mutate the returned pointer through more than one
    /// writer simultaneously. `StreamEpoch` upholds this via its single-
    /// leader invariant.
    pub(crate) fn ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    /// # Safety
    /// Same single-writer invariant as `ptr()`. Returns a *mut for the
    /// writer path.
    pub(crate) fn ptr_mut(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }
}

impl Drop for ArenaBuffer {
    fn drop(&mut self) {
        // SAFETY: ptr and layout were produced by alloc() in ArenaBuffer::new().
        unsafe {
            dealloc(self.ptr.as_ptr(), self.layout);
        }
    }
}

/// A reference-counted slice into an `ArenaBuffer`.
pub(crate) struct OwnedArenaSlice {
    pub(crate) _arena: Arc<ArenaBuffer>,
    pub(crate) ptr: *const u8,
    pub(crate) len: u32,
}

// SAFETY: The underlying memory is owned by Arc<ArenaBuffer> which is Send+Sync.
unsafe impl Send for OwnedArenaSlice {}
unsafe impl Sync for OwnedArenaSlice {}

impl Deref for OwnedArenaSlice {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        // SAFETY: ptr is valid for len bytes as long as _arena is alive.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len as usize) }
    }
}

impl AsRef<[u8]> for OwnedArenaSlice {
    fn as_ref(&self) -> &[u8] {
        self.deref()
    }
}
```

- [ ] **Step 2: Rewire extent.rs to use ArenaBuffer**

In `components/extent-node/src/extent.rs`:

1. Delete the old `ArenaBuffer` / `OwnedArenaSlice` definitions and their `impl`s (lines ~40-90).
2. Add `use crate::arena::{ArenaBuffer, OwnedArenaSlice};` at the top of the file.
3. Replace the current direct `alloc`/`dealloc` logic in `Extent::with_capacity` with `ArenaBuffer::new(capacity)`. The returned `Arc<ArenaBuffer>` replaces today's `arena: Arc<ArenaBuffer>` field exactly.
4. Replace every `self.buf` write site with `self.arena.ptr_mut()` followed by offset arithmetic (the `buf: *mut u8` field can stay as a cached derived pointer if you prefer; retain the current comments). **Simpler path:** keep `buf: *mut u8` as-is (derived from `arena.ptr_mut()` in `with_capacity`), so the append hot path doesn't change.
5. Replace any `OwnedArenaSlice { _arena: self.arena.clone(), ptr: …, len: … }` constructions with the same struct literal through the new path (it's still `pub(crate)` on `arena::buffer`).

- [ ] **Step 3: Add the module to lib.rs**

Edit `components/extent-node/src/lib.rs`: add `pub mod arena;` adjacent to the existing `pub mod extent;` declarations (alphabetical order preferred).

- [ ] **Step 4: Build**

Run: `cargo check --workspace --tests --benches 2>&1 | tail -10`
Expected: clean, no warnings.

- [ ] **Step 5: Test**

Run: `cargo test --lib --workspace 2>&1 | grep -E '^test result:'`
Expected: same pass count as baseline.

- [ ] **Step 6: Commit**

```bash
git add components/extent-node/src/arena components/extent-node/src/extent.rs components/extent-node/src/lib.rs
git commit -m "$(cat <<'EOF'
refactor(arena): extract ArenaBuffer + OwnedArenaSlice into arena module

Move the raw-heap arena allocation (and its refcounted slice view) out of
extent.rs into a new components/extent-node/src/arena/buffer.rs. The
Extent struct still owns the buffer; only the type definition relocates.

This is the first step of the P2 shared-arena plumbing: the same buffer
primitive will back both Dedicated (one stream per arena) and Shared
(many streams per arena, added in a later plan) pools.

No runtime behavior change. Hot-path memcpy still goes through the
cached buf: *mut u8 field on Extent.
EOF
)"
```

### Task 1.2: Extract ArenaDirectory

**Files:**
- Create: `components/extent-node/src/arena/directory.rs`
- Modify: `components/extent-node/src/arena/mod.rs` (export `ArenaDirectory`, `EpochArenaEntry`)
- Modify: `components/extent-node/src/extent.rs` — today's `index: Box<[AtomicU32]>` becomes `directory: ArenaDirectory`; helpers on `Extent` (`index_lookup`, `index_record`) delegate.

Today's Extent stores a flat `Box<[AtomicU32]>` keyed by sequence number. The new `ArenaDirectory` keeps the same flat-index layout internally so the hot path is unchanged; it just has a named type. The multi-entry per-(stream, epoch) extension from the spec comes in P3 — in this plan `ArenaDirectory` always has exactly one `EpochArenaEntry`.

- [ ] **Step 1: Write the ArenaDirectory struct**

Create `components/extent-node/src/arena/directory.rs`:

```rust
use std::sync::atomic::{AtomicU32, Ordering};

use common::types::{Epoch, Offset, StreamId};

/// Sentinel for an unwritten directory slot.
///
/// We store `byte_pos + 1` in the slot so that slot==0 means "not yet
/// written", enabling `alloc_zeroed` to init the whole table at near-zero
/// cost. Same trick the old `Extent.index` used — just renamed and moved.
pub(crate) const SLOT_UNSET: u32 = 0;

/// Per-(stream, epoch) record placement inside one arena.
///
/// P2 keeps this as a flat per-sequence u32 table, matching today's
/// Extent.index exactly. P3 will expand the arena directory to hold
/// multiple EpochArenaEntry values (one per (stream, epoch) tuple in
/// the arena) so Shared arenas can multiplex streams.
pub(crate) struct EpochArenaEntry {
    pub(crate) stream_id:     StreamId,
    pub(crate) epoch:         Epoch,
    pub(crate) start_offset:  Offset,

    // `byte_positions[seq]` stores `byte_pos + 1`. 0 means unset.
    // Capacity is `arena_capacity / MIN_RECORD_SIZE`, matching today.
    byte_positions: Box<[AtomicU32]>,
}

impl EpochArenaEntry {
    pub(crate) fn with_capacity(
        stream_id:    StreamId,
        epoch:        Epoch,
        start_offset: Offset,
        record_cap:   usize,
    ) -> Self {
        let mut v: Vec<AtomicU32> = Vec::with_capacity(record_cap);
        // alloc_zeroed path: AtomicU32::new(0) in a loop is fine because
        // the Vec allocation is already zero-initialized by the allocator
        // for large caps; the loop just writes the atomic tag. Keep exactly
        // as Extent did.
        v.resize_with(record_cap, || AtomicU32::new(SLOT_UNSET));
        Self {
            stream_id, epoch, start_offset,
            byte_positions: v.into_boxed_slice(),
        }
    }

    pub(crate) fn record(&self, seq: u64, byte_pos: u64) {
        // Matches Extent::index_record: store (byte_pos + 1) so slot==0
        // retains the sentinel meaning.
        let idx = seq as usize;
        debug_assert!(idx < self.byte_positions.len(), "seq out of directory cap");
        self.byte_positions[idx].store(byte_pos as u32 + 1, Ordering::Release);
    }

    pub(crate) fn lookup(&self, seq: u64) -> Option<u64> {
        let idx = seq as usize;
        let raw = self.byte_positions.get(idx)?.load(Ordering::Acquire);
        if raw == SLOT_UNSET {
            None
        } else {
            Some((raw - 1) as u64)
        }
    }
}

/// Arena-level directory. One per arena buffer.
///
/// In P2: always holds exactly one `EpochArenaEntry` (Dedicated: one stream,
/// one epoch per arena). In P3+: a `HashMap<(StreamId, Epoch), EpochArenaEntry>`
/// so multiple streams can share a Shared arena.
///
/// The single-entry constraint is asserted at the call site.
pub(crate) struct ArenaDirectory {
    entry: EpochArenaEntry,
}

impl ArenaDirectory {
    pub(crate) fn new(entry: EpochArenaEntry) -> Self {
        Self { entry }
    }

    pub(crate) fn single_entry(&self) -> &EpochArenaEntry {
        &self.entry
    }
}
```

- [ ] **Step 2: Wire it into Extent**

Edit `components/extent-node/src/extent.rs`:

1. Replace the `index: Box<[AtomicU32]>` field with `directory: crate::arena::ArenaDirectory`.
2. In `Extent::with_capacity`, construct:
   ```rust
   let record_cap = (capacity / MIN_RECORD_SIZE) as usize;
   let entry = EpochArenaEntry::with_capacity(
       StreamId(0),  // P2 placeholder: filled in by caller in Phase 3
       epoch,
       start_offset,
       record_cap,
   );
   let directory = ArenaDirectory::new(entry);
   ```
   …until Phase 3 wires the real `stream_id`. For P2's purposes the entry's `stream_id` is informational only (hot path doesn't read it). Leave a `// TODO(P3): fill in stream_id when StreamEpoch is introduced` comment.
3. Rewrite `Extent::index_record(seq, byte_pos)` to `self.directory.single_entry().record(seq, byte_pos)`.
4. Rewrite `Extent::index_lookup(seq)` to `self.directory.single_entry().lookup(seq)`.
5. Delete `const INDEX_UNSET: u32 = 0;` at the top of `extent.rs` — the constant moved to `directory.rs`.

- [ ] **Step 3: Export from arena/mod.rs**

Edit `components/extent-node/src/arena/mod.rs`:

```rust
mod buffer;
mod directory;

pub(crate) use buffer::{ArenaBuffer, OwnedArenaSlice};
pub(crate) use directory::{ArenaDirectory, EpochArenaEntry};
```

- [ ] **Step 4: Build + test**

Run: `cargo check --workspace --tests --benches 2>&1 | tail -10`
Expected: clean.

Run: `cargo test --lib --workspace 2>&1 | grep -E '^test result:'`
Expected: same pass count as Phase 1 Task 1.

- [ ] **Step 5: Commit**

```bash
git add components/extent-node/src/arena components/extent-node/src/extent.rs
git commit -m "$(cat <<'EOF'
refactor(arena): extract per-record directory into ArenaDirectory

Move Extent.index + index_record + index_lookup into
components/extent-node/src/arena/directory.rs as ArenaDirectory +
EpochArenaEntry. The sentinel trick (byte_pos + 1 so slot==0 means
unset) is preserved verbatim so alloc_zeroed fast init still works.

P2 always has exactly one entry per directory (Dedicated: one stream,
one epoch per arena). P3 will widen the directory to a HashMap keyed
by (stream_id, epoch) to support Shared arenas multiplexing many
streams into one buffer.

No runtime behavior change.
EOF
)"
```

---

## Phase 2: Rename Extent → StreamEpoch

Goal: Move today's `Extent` (minus the parts extracted in Phase 1) to a new `stream_epoch.rs` file under the name `StreamEpoch`. This phase will intentionally leave the build broken for a short window — combine tasks 2.1 and 2.2 in one subagent dispatch.

### Task 2.1+2.2: Rename + move (single dispatch)

**Files:**
- Create: `components/extent-node/src/stream_epoch.rs`
- Delete: `components/extent-node/src/extent.rs`
- Modify: every file that imports `crate::extent::*` or uses `Extent`, `ExtentState`, `AppendResult`

**Rename table:**

| Before | After |
|---|---|
| `struct Extent` | `struct StreamEpoch` |
| `crate::extent::Extent` | `crate::stream_epoch::StreamEpoch` |
| `mod extent;` in lib.rs | `mod stream_epoch;` |
| method names on `Extent` | unchanged on `StreamEpoch` |
| `AppendResult` | unchanged (moves with the file) |
| local variable names `extent` | `epoch` (spec vocabulary), WHERE the local refers to a `&StreamEpoch`. Do not rename `extent_id` locals — that's still the column name. |

**Scope reduction vs. original P2 draft:** `ExtentState` is NOT renamed in this plan. Today's enum lives in `common/src/types.rs` and is used by 24 files including every integration test and the entire stream-manager metadata layer. The spec's eventual state machine (`Open | Sealed`, collapsing `Active | Sealed | Flushed`) is a bigger semantic change than this plan covers, so the rename happens in a later plan alongside the state-machine collapse. `ExtentState::Active | Sealed | Flushed` stays intact in P2.

Similarly, `ExtentId` (u32 row id in `common/src/types.rs`) is NOT renamed. The MySQL `extent_id` column is still present (P1 deferred column removal), so the Rust row-identity type keeps matching it.

- [ ] **Step 1: Create stream_epoch.rs**

`git mv components/extent-node/src/extent.rs components/extent-node/src/stream_epoch.rs`

Then in `stream_epoch.rs`:
1. Rename `pub struct Extent` → `pub struct StreamEpoch` (replace_all on the file, match whole word).
2. Rename `impl Extent` → `impl StreamEpoch`.
3. Doc comments: replace "extent" with "epoch" only where the spec vocabulary changed. Concretely:
   - "Extent has been flushed" → "StreamEpoch has been flushed"
   - "extent's arena buffer" → "epoch's arena buffer"
   - "sealed extent" → "sealed epoch"
   - "`Extent::` " → "`StreamEpoch::`"
   - Leave "extent_id" literal (it's still the column/field name).
4. Rename local method `Extent::seal` helpers that return `(start, end)` — no signature change, just doc tweaks.
5. `impl Debug for Extent` → `impl Debug for StreamEpoch` if one exists.

- [ ] **Step 2: DO NOT rename ExtentState or ExtentId**

`ExtentState` remains the name of the state enum in `common/src/types.rs`. `ExtentId` remains the u32 row-identity type. Only the `Extent` struct → `StreamEpoch` rename is in scope for this plan.

- [ ] **Step 3: Update lib.rs**

`components/extent-node/src/lib.rs`:
- `pub mod extent;` → `pub mod stream_epoch;`
- If other modules have `use crate::extent::…`, update to `use crate::stream_epoch::…`.

- [ ] **Step 4: Sweep imports across the workspace**

Run:
```bash
grep -rn 'crate::extent::\|use crate::extent\|extent::Extent\|: Extent\b\|-> Extent\b\|&Extent\b\|&mut Extent\b\|Vec<Extent>\|Box<Extent>\|Extent::with_capacity' components/ tests/ --include='*.rs'
```

For every match, rewrite to `StreamEpoch` / `stream_epoch` as appropriate. The pattern set is localized to `components/extent-node/src` — integration tests use `client::` APIs, not `Extent` directly.

- [ ] **Step 5: Update Stream to hold StreamEpoch**

In `components/extent-node/src/stream.rs`:
- `StreamInner.extents: Vec<Extent>` → `StreamInner.extents: Vec<StreamEpoch>` (Phase 3 will replace the Vec with ArcSwap; for now keep the Vec).
- Local variable names: `extent` → `epoch` where it refers to `&StreamEpoch`; preserve `extent_id` identifiers (that's still the ID type).

- [ ] **Step 6: Build + test**

Run: `cargo check --workspace --tests --benches 2>&1 | tail -15`
Expected: clean.

Run: `cargo test --lib --workspace 2>&1 | grep -E '^test result:'`
Expected: same pass count.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "$(cat <<'EOF'
refactor(epoch): rename Extent to StreamEpoch and move to stream_epoch.rs

Introduces the spec's StreamEpoch vocabulary on the ExtentNode side:

  Extent          -> StreamEpoch
  extent.rs       -> stream_epoch.rs
  crate::extent   -> crate::stream_epoch

ExtentId (u32 row identity) and ExtentState (Active | Sealed | Flushed)
both stay: the MySQL extent_id column is still present and the state
machine's eventual collapse to Open | Sealed is a bigger semantic
change deferred to a later plan.

No behavior change. Stream.extents is still a Vec<StreamEpoch>; the
ArcSwap<SmallVec<Arc<StreamEpoch>>> migration is Phase 3.
EOF
)"
```

---

## Phase 3: Stream.epochs → ArcSwap<SmallVec<[Arc<StreamEpoch>; 4]>>

Goal: replace `RwLock<StreamInner { extents: Vec<StreamEpoch> }>` with `ArcSwap<SmallVec<[Arc<StreamEpoch>; 4]>>` and the helper methods from the spec (`active_epoch`, `find_epoch_containing`, `get_epoch`, `insert_epoch`, `remove_epoch`). Other `StreamInner` fields (next_extent_id, extent_capacity, max_extents, downstream_txs, storage_class) stay where they are until P3+ trims them.

### Task 3.1: Introduce ArcSwap<SmallVec<…>> for epochs

**Files:**
- Modify: `components/extent-node/src/stream.rs`

- [ ] **Step 1: Add arc-swap to workspace + extent-node**

`arc-swap` is not yet in the workspace. Before editing Rust source, edit the root `Cargo.toml`:

```toml
# in the [workspace.dependencies] section, alphabetically:
arc-swap = "1.7"
```

Then in `components/extent-node/Cargo.toml` under `[dependencies]`:

```toml
arc-swap = { workspace = true }
```

Run `cargo check -p extent-node 2>&1 | tail -3` and expect the new dep to resolve cleanly. Then proceed with the ArcSwap import edits below.

- [ ] **Step 2: Add imports**

At the top of `components/extent-node/src/stream.rs`:
```rust
use arc_swap::ArcSwap;
use smallvec::{SmallVec, smallvec};
```

- [ ] **Step 3: Replace the field**

Change `StreamInner`:
```rust
struct StreamInner {
    // extents: Vec<StreamEpoch>,       // removed
    next_extent_id: ExtentId,
    extent_capacity: u32,
    max_extents: usize,
    downstream_txs: Vec<mpsc::Sender<Frame>>,
    storage_class: StorageClass,
}
```

Add to `Stream`:
```rust
pub struct Stream {
    pub id: StreamId,
    epoch: AtomicU32,
    in_flight: AtomicU64,
    tx: Sender<AppendRequest>,
    rx: Receiver<AppendRequest>,
    ack_queue: OnceLock<AckQueue>,
    flush_in_progress: papaya::HashMap<ExtentId, ()>,

    /// All StreamEpochs this EN currently tracks for this stream,
    /// sorted by epoch number ascending. Copy-on-write via ArcSwap:
    /// readers take a single Arc load, writers clone the SmallVec,
    /// mutate, and store().
    epochs: ArcSwap<SmallVec<[Arc<StreamEpoch>; 4]>>,

    inner: RwLock<StreamInner>,
}
```

In `Stream::new`:
```rust
epochs: ArcSwap::from_pointee(SmallVec::new()),
```

- [ ] **Step 4: Add the helper methods per spec**

Add to `impl Stream`:

```rust
impl Stream {
    // ── Epoch vec helpers (lock-free reads, CoW writes) ─────────────

    /// The currently-open epoch (the last by epoch number). None if none
    /// registered yet (brand-new stream waiting on RegisterEpoch).
    pub fn active_epoch(&self) -> Option<Arc<StreamEpoch>> {
        self.epochs.load().last().cloned()
    }

    /// Find the epoch whose [start_offset, start_offset + limit) covers
    /// `offset`. Linear scan — snap.len() is 1–3 in practice.
    pub fn find_epoch_containing(&self, offset: u64) -> Option<Arc<StreamEpoch>> {
        let snap = self.epochs.load();
        for ep in snap.iter() {
            let lim = ep.limit_hint();   // u64::MAX if open
            if offset >= ep.start_offset.0
                && offset < ep.start_offset.0.saturating_add(lim)
            {
                return Some(ep.clone());
            }
        }
        None
    }

    pub fn get_epoch(&self, epoch: Epoch) -> Option<Arc<StreamEpoch>> {
        self.epochs
            .load()
            .iter()
            .find(|e| e.epoch == epoch)
            .cloned()
    }

    fn insert_epoch(&self, new_ep: Arc<StreamEpoch>) {
        self.epochs.rcu(|current| {
            let mut next: SmallVec<[Arc<StreamEpoch>; 4]> = (**current).clone();
            next.push(new_ep.clone());
            next.sort_by_key(|e| e.epoch.0);
            next
        });
    }

    fn remove_epoch(&self, epoch: Epoch) {
        self.epochs.rcu(|current| {
            let mut next: SmallVec<[Arc<StreamEpoch>; 4]> = (**current).clone();
            next.retain(|e| e.epoch != epoch);
            next
        });
    }
}
```

Add a `StreamEpoch::limit_hint()` method returning `u64::MAX` while the epoch is `Open` and the actual limit once `Sealed`. Today's `Extent.limit: AtomicU64` already has this value (`LIMIT_OPEN = u64::MAX` sentinel) — just add a tiny accessor:

```rust
impl StreamEpoch {
    pub fn limit_hint(&self) -> u64 {
        self.limit.load(Ordering::Acquire)
    }
}
```

- [ ] **Step 5: Migrate every `inner.read().extents` / `inner.write().extents` call site**

Grep:
```bash
grep -n 'inner\.\(read\|write\)()\.extents\|\.extents\.' components/extent-node/src/stream.rs components/extent-node/src/store/*.rs
```

For each:
- **Read** (`inner.read().extents.iter()` etc.) → replace with `self.epochs.load().iter()` (returns `arc_swap::Guard`). No lock required.
- **Append** (`inner.write().extents.push(e)`) → replace with `self.insert_epoch(Arc::new(e))`.
- **Remove** (`inner.write().extents.remove(0)`) → `self.remove_epoch(e.epoch)`.
- **Find by id** (`inner.read().find_extent(id)`) → `self.active_epoch()` or `self.epochs.load().iter().find(|e| e.id == id)`.
- **Last / active**: `extents.last()` → `self.active_epoch()`.

Keep `inner` for the non-epoch fields (next_extent_id, extent_capacity, etc.).

- [ ] **Step 6: Update StreamInner eviction path**

`StreamInner::evict_oldest_extents` currently mutates `self.extents`. Move the eviction logic out of `StreamInner` onto `Stream`. The body becomes:

```rust
impl Stream {
    fn evict_oldest_epochs(&self) {
        let inner = self.inner.read();
        let max = inner.max_extents;
        let is_s3 = inner.storage_class == StorageClass::S3;
        drop(inner);
        if max == 0 {
            let snap = self.epochs.load();
            if snap.len() > 4 {
                tracing::warn!(
                    "stream {} has {} epochs but max_extents=0 (no eviction)",
                    self.id,
                    snap.len(),
                );
            }
            return;
        }
        // CoW loop: pull snapshot, compute which epoch ids to evict, rcu them out.
        loop {
            let snap = self.epochs.load();
            if snap.len() <= max.max(1) {
                break;
            }
            let head = snap.first().expect("len checked");
            if is_s3 && !head.is_flushed() {
                break;
            }
            let victim = head.epoch;
            drop(snap);
            self.remove_epoch(victim);
        }
    }
}
```

Call sites that used to call `inner.evict_oldest_extents(stream_id)` now call `self.evict_oldest_epochs()`.

- [ ] **Step 7: Update `try_create_next_extent`**

Move the body to `impl Stream` (not `StreamInner`) since it needs `insert_epoch`:

```rust
impl Stream {
    fn try_create_next_epoch(&self, epoch: Epoch) -> Option<(ExtentId, Offset)> {
        // S3 backpressure: refuse to allocate if eviction is blocked.
        {
            let snap = self.epochs.load();
            let inner = self.inner.read();
            if inner.max_extents > 0
                && snap.len() >= inner.max_extents
                && inner.storage_class == StorageClass::S3
                && snap.first().is_some_and(|e| !e.is_flushed())
            {
                return None;
            }
        }
        let (new_id, start_offset, capacity) = {
            let mut inner = self.inner.write();
            let snap = self.epochs.load();
            let start_offset = snap
                .last()
                .map(|e| Offset(e.start_offset.0 + e.message_count()))
                .unwrap_or(Offset(0));
            let new_id = inner.next_extent_id;
            inner.next_extent_id = ExtentId(new_id.0 + 1);
            (new_id, start_offset, inner.extent_capacity)
        };
        let ep = Arc::new(StreamEpoch::with_capacity(new_id, start_offset, capacity, epoch));
        self.insert_epoch(ep);
        self.evict_oldest_epochs();
        Some((new_id, start_offset))
    }
}
```

- [ ] **Step 8: Remove `StreamInner::find_extent` / `seal_extent` / `try_create_next_extent` / `evict_oldest_extents`**

These are now on `Stream`.

- [ ] **Step 9: Build + test**

Run: `cargo check --workspace --tests --benches 2>&1 | tail -10`
Expected: clean.

Run: `cargo test --lib --workspace 2>&1 | grep -E '^test result:'`
Expected: same pass count.

- [ ] **Step 10: Commit**

```bash
git add -A
git commit -m "$(cat <<'EOF'
refactor(stream): Stream.epochs via ArcSwap<SmallVec<Arc<StreamEpoch>>>

Replace the RwLock<StreamInner { extents: Vec<StreamEpoch> }> with an
ArcSwap<SmallVec<[Arc<StreamEpoch>; 4]>> on Stream. Readers now take a
single Arc load (no lock); writers clone the small vec and store().

Adds the helper API from the shared-arena spec:
  - Stream::active_epoch        -> epochs.last()
  - Stream::find_epoch_containing(offset)
  - Stream::get_epoch(epoch)
  - Stream::insert_epoch / remove_epoch  (RCU)
  - StreamEpoch::limit_hint()

Eviction and next-epoch allocation move from StreamInner onto Stream to
match the new ownership. All hot-path reads (append, forward, read) are
now lock-free on the epoch lookup.

No behavior change at the protocol level.
EOF
)"
```

---

## Phase 4: ArenaClass Enum + MySQL Column + Wire Field

Goal: introduce `ArenaClass` end-to-end, always set to `Dedicated` in this plan. The enum must be readable from MySQL, writable on `CreateStream`, and carried on `RegisterEpoch` + `ForwardInitEpoch` over the wire.

### Task 4.1: Define ArenaClass in common

**Files:**
- Modify: `components/common/src/types.rs`

- [ ] **Step 1: Add the enum**

Append to `components/common/src/types.rs` (near `StorageClass`):

```rust
/// Per-stream arena sizing policy.
///
/// - `Dedicated`: the stream has its own arena, one writer per stream
///   (today's fast path). Arena size = `dedicated_arena_size`.
/// - `Shared`: appends flow through a node-wide SharedArenaPool where
///   one arena multiplexes records from many streams. Arena size =
///   `shared_arena_size`. Not wired at runtime yet — all CreateStream
///   requests use Dedicated in P2.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ArenaClass {
    Dedicated = 0,
    Shared    = 1,
}

impl Default for ArenaClass {
    fn default() -> Self { ArenaClass::Dedicated }
}

impl TryFrom<u8> for ArenaClass {
    type Error = ();
    fn try_from(v: u8) -> Result<Self, ()> {
        match v {
            0 => Ok(ArenaClass::Dedicated),
            1 => Ok(ArenaClass::Shared),
            _ => Err(()),
        }
    }
}

impl From<ArenaClass> for u8 {
    fn from(c: ArenaClass) -> u8 { c as u8 }
}

impl std::fmt::Display for ArenaClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArenaClass::Dedicated => write!(f, "dedicated"),
            ArenaClass::Shared    => write!(f, "shared"),
        }
    }
}
```

- [ ] **Step 2: Build**

Run: `cargo check --workspace --tests --benches 2>&1 | tail -5`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add components/common/src/types.rs
git commit -m "$(cat <<'EOF'
feat(types): add ArenaClass enum (Dedicated | Shared)

Introduces the per-stream arena-sizing policy type defined in the
shared-arena spec. P2 uses ArenaClass::Dedicated everywhere; the
Shared path is not wired yet.
EOF
)"
```

### Task 4.2: Add arena_class column to streams table

**Files:**
- Create: `components/stream-manager/migrations/V7__add_arena_class.sql`
- Modify: `components/stream-manager/src/metadata.rs`

- [ ] **Step 1: Write the migration**

Create `components/stream-manager/migrations/V7__add_arena_class.sql`:

```sql
-- Add ArenaClass to stream and stream_epochs.
-- 0 = Dedicated (default), 1 = Shared.
ALTER TABLE stream
    ADD COLUMN arena_class TINYINT UNSIGNED NOT NULL DEFAULT 0;
ALTER TABLE stream_epochs
    ADD COLUMN arena_class TINYINT UNSIGNED NOT NULL DEFAULT 0;
```

Rationale: the stream row records the declared class; every epoch captures the class at allocation time so a runtime class flip (P5) doesn't retroactively rewrite history.

- [ ] **Step 2: Update StreamRow / get_stream / create_stream**

In `components/stream-manager/src/metadata.rs`:
- Add `arena_class: ArenaClass` to `StreamRow` (derive from u8 column via `TryFrom`).
- `INSERT INTO stream (…, arena_class) VALUES (…, ?)` — bind `ArenaClass::Dedicated as u8 = 0`.
- `SELECT … arena_class FROM stream …` — read the column into the struct.
- Similarly for `StreamEpochRow` (existing `ExtentRow` — optional rename is out of scope here; do NOT rename the struct, just add the field).

- [ ] **Step 3: Build + test**

Run: `cargo check --workspace --tests --benches 2>&1 | tail -10`
Expected: clean.

Run: `cargo test --lib --workspace 2>&1 | grep -E '^test result:'`
Expected: same pass count. Integration tests (skipped here) will exercise the migration.

- [ ] **Step 4: Commit**

```bash
git add components/stream-manager/migrations/V7__add_arena_class.sql components/stream-manager/src/metadata.rs
git commit -m "$(cat <<'EOF'
feat(sm): persist ArenaClass on stream and stream_epochs rows

V7 migration adds TINYINT UNSIGNED NOT NULL DEFAULT 0 (Dedicated) to
stream and stream_epochs. create_stream inserts the declared class;
allocate_epoch records the class at allocation so runtime class flips
(added in a future plan) do not rewrite history.

All new rows in P2 default to Dedicated; Shared routing is not wired.
EOF
)"
```

### Task 4.3: Carry arena_class on RegisterEpoch + ForwardInitEpoch

**Files:**
- Modify: `components/rpc/src/frame/header.rs`
- Modify: `components/rpc/src/frame/encode.rs`
- Modify: `components/rpc/src/frame/decode.rs`
- Modify: `components/rpc/src/frame/tests.rs`
- Modify: `components/extent-node/src/store/register.rs`
- Modify: `components/extent-node/src/store/forward.rs` (ForwardInitEpoch handling)
- Modify: `components/extent-node/src/store/append.rs` (ForwardInitEpoch send)
- Modify: `components/stream-manager/src/store.rs` (RegisterEpoch send)

- [ ] **Step 1: Grow the VariableHeader variants**

In `components/rpc/src/frame/header.rs`, find `VariableHeader::RegisterEpoch` and `VariableHeader::ForwardInitEpoch`. Add a `arena_class: u8` field to each. (Using `u8` on the wire and converting to `ArenaClass` at the consumer keeps the frame module free of common-types dependency drift; other variants already follow this pattern for small enums.)

- [ ] **Step 2: Encode + decode one extra byte**

In `encode.rs`: for each of the two variants, append `buf.put_u8(arena_class)` at the end of the existing encode sequence.
In `decode.rs`: for each, read `arena_class = src.get_u8()` at the end.

This is an on-wire breaking change, but we're in-place dev with no rolling upgrades — acceptable.

- [ ] **Step 3: Update all constructors**

Grep:
```bash
grep -rn 'VariableHeader::RegisterEpoch\s*{\|VariableHeader::ForwardInitEpoch\s*{' components/ --include='*.rs'
```

For each construction site add `arena_class: 0` (Dedicated). Sites:
- `stream-manager/src/store.rs`: SM allocates epochs; read the stream row's `arena_class` (always 0 in P2) and pass it through.
- `extent-node/src/store/append.rs` (or wherever ForwardInitEpoch is built by the Primary): read `StreamEpoch.class()` — we need a method on StreamEpoch returning its class. For P2 this can hardcode `ArenaClass::Dedicated as u8`; wiring the real value happens when StreamEpoch gains the `class` field (next step).

- [ ] **Step 4: Store class on StreamEpoch**

Add `pub class: ArenaClass` to `StreamEpoch` in `components/extent-node/src/stream_epoch.rs`. Default to `Dedicated` in `with_capacity`. Add `StreamEpoch::class(&self) -> ArenaClass` accessor.

Extend `with_capacity`'s signature to take `class: ArenaClass` and thread it from every call site. All current call sites pass `ArenaClass::Dedicated`.

- [ ] **Step 5: Propagate class from RegisterEpoch → StreamEpoch**

In `components/extent-node/src/store/register.rs`, when handling RegisterEpoch, decode `arena_class` via `ArenaClass::try_from(raw)` (on decode error, log and default to Dedicated) and pass it to `StreamEpoch::with_capacity`.

Similarly on the Secondary side in `components/extent-node/src/store/forward.rs` for ForwardInitEpoch.

- [ ] **Step 6: Round-trip test**

In `components/rpc/src/frame/tests.rs`, extend the existing `register_epoch_round_trip` and `forward_init_epoch_round_trip` tests (if present; otherwise add them) to set `arena_class: 1` on encode and assert `1` after decode. Confirm encoded length grew by exactly 1 byte.

- [ ] **Step 7: Build + test**

Run: `cargo check --workspace --tests --benches 2>&1 | tail -10`
Expected: clean.

Run: `cargo test --lib --workspace 2>&1 | grep -E '^test result:'`
Expected: all pass including the new round-trip assertions.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "$(cat <<'EOF'
feat(wire): carry arena_class on RegisterEpoch and ForwardInitEpoch

Both frames grow by exactly one byte (TINYINT UNSIGNED: 0=Dedicated,
1=Shared). SM reads streams.arena_class and stamps it on RegisterEpoch;
Primary reads StreamEpoch.class and stamps it on ForwardInitEpoch. A
decode-time ArenaClass::try_from defends against future unknown
variants.

All traffic in P2 carries arena_class=0; the Shared-path routing lands
in a later plan.
EOF
)"
```

---

## Phase 5: ArenaPool trait + DedicatedArenaPool

Goal: introduce the pool abstraction. Today's per-stream arena list is repackaged behind a `DedicatedArenaPool`. A `SharedArenaPool` stub is added but panics on use.

### Task 5.1: Define the ArenaPool trait

**Files:**
- Create: `components/extent-node/src/arena/pool/mod.rs`
- Create: `components/extent-node/src/arena/pool/dedicated.rs`
- Create: `components/extent-node/src/arena/pool/shared.rs`
- Modify: `components/extent-node/src/arena/mod.rs`

- [ ] **Step 1: Write the module**

Create `components/extent-node/src/arena/pool/mod.rs`:

```rust
mod dedicated;
mod shared;

pub(crate) use dedicated::DedicatedArenaPool;
pub(crate) use shared::SharedArenaPool;

use std::sync::Arc;

use common::types::{ArenaClass, Epoch, ExtentId, Offset, StreamId};

use crate::stream_epoch::StreamEpoch;

/// Allocates and retires arena buffers.
///
/// Implementations (P2):
/// - `DedicatedArenaPool`: one pool per Stream; the stream owns its
///   arenas exclusively. Used by every stream in P2.
/// - `SharedArenaPool`: one pool per EN; many streams share arenas.
///   Stubbed in P2 — a later plan wires it in.
pub(crate) trait ArenaPool: Send + Sync {
    /// The class of this pool. Drives which ArenaClass.stream gets
    /// which pool at registration time.
    fn class(&self) -> ArenaClass;

    /// Allocate a fresh arena for (stream_id, epoch). Returns the new
    /// StreamEpoch wrapped in Arc — ready to insert into
    /// Stream::epochs via insert_epoch.
    fn allocate_epoch(
        &self,
        stream_id:    StreamId,
        extent_id:    ExtentId,
        start_offset: Offset,
        epoch:        Epoch,
    ) -> Arc<StreamEpoch>;
}
```

Create `components/extent-node/src/arena/pool/dedicated.rs`:

```rust
use std::sync::Arc;

use common::types::{ArenaClass, Epoch, ExtentId, Offset, StreamId};

use crate::arena::pool::ArenaPool;
use crate::stream_epoch::StreamEpoch;

pub(crate) struct DedicatedArenaPool {
    arena_size: u32,
}

impl DedicatedArenaPool {
    pub(crate) fn new(arena_size: u32) -> Self {
        Self { arena_size }
    }
}

impl ArenaPool for DedicatedArenaPool {
    fn class(&self) -> ArenaClass { ArenaClass::Dedicated }

    fn allocate_epoch(
        &self,
        _stream_id:   StreamId,
        extent_id:    ExtentId,
        start_offset: Offset,
        epoch:        Epoch,
    ) -> Arc<StreamEpoch> {
        Arc::new(StreamEpoch::with_capacity(
            extent_id,
            start_offset,
            self.arena_size,
            epoch,
            ArenaClass::Dedicated,
        ))
    }
}
```

Create `components/extent-node/src/arena/pool/shared.rs`:

```rust
use std::sync::Arc;

use common::types::{ArenaClass, Epoch, ExtentId, Offset, StreamId};

use crate::arena::pool::ArenaPool;
use crate::stream_epoch::StreamEpoch;

pub(crate) struct SharedArenaPool {
    _arena_size: u32,
}

impl SharedArenaPool {
    pub(crate) fn new(arena_size: u32) -> Self {
        Self { _arena_size: arena_size }
    }
}

impl ArenaPool for SharedArenaPool {
    fn class(&self) -> ArenaClass { ArenaClass::Shared }

    fn allocate_epoch(
        &self,
        _stream_id:    StreamId,
        _extent_id:    ExtentId,
        _start_offset: Offset,
        _epoch:        Epoch,
    ) -> Arc<StreamEpoch> {
        panic!("SharedArenaPool not wired in P2; every stream is Dedicated")
    }
}
```

Update `components/extent-node/src/arena/mod.rs`:

```rust
mod buffer;
mod directory;
mod pool;

pub(crate) use buffer::{ArenaBuffer, OwnedArenaSlice};
pub(crate) use directory::{ArenaDirectory, EpochArenaEntry};
pub(crate) use pool::{ArenaPool, DedicatedArenaPool, SharedArenaPool};
```

- [ ] **Step 2: Build**

Run: `cargo check --workspace --tests --benches 2>&1 | tail -5`
Expected: clean (pool is declared but not consumed yet).

- [ ] **Step 3: Commit**

```bash
git add components/extent-node/src/arena
git commit -m "$(cat <<'EOF'
feat(arena): introduce ArenaPool trait with Dedicated + stub Shared impls

DedicatedArenaPool wraps today's per-stream arena allocation behind the
spec's ArenaPool trait so StreamEpoch allocation becomes uniform across
classes. SharedArenaPool is a panicking stub; wiring it at runtime is
deferred to a later plan.
EOF
)"
```

### Task 5.2: Plumb Stream through ArenaPool

**Files:**
- Modify: `components/extent-node/src/stream.rs`
- Modify: `components/extent-node/src/store/register.rs`

- [ ] **Step 1: Store an Arc<dyn ArenaPool> on Stream**

In `components/extent-node/src/stream.rs`:

```rust
use crate::arena::ArenaPool;
// …
pub struct Stream {
    // … existing fields …
    pool: Arc<dyn ArenaPool>,
}
```

Update `Stream::new(id, pool)` — adds the `pool` parameter.

- [ ] **Step 2: Use the pool in try_create_next_epoch**

Replace the direct `StreamEpoch::with_capacity(…)` in `try_create_next_epoch` with `self.pool.allocate_epoch(self.id, new_id, start_offset, epoch)`. Delete the local `capacity` load — the pool knows its arena size.

- [ ] **Step 3: Caller updates**

Every `Stream::new(id)` call site needs to pass a pool. For P2 — always `DedicatedArenaPool` with `config.dedicated_arena_size` (which for P1 is `DEFAULT_EXTENT_CAPACITY`).

The `ExtentNodeStore` owns a `default_pool: Arc<DedicatedArenaPool>` constructed from config at startup; `Stream::new` receives a clone.

Edit `components/extent-node/src/store/mod.rs`:
- Add field: `pub(crate) default_pool: Arc<DedicatedArenaPool>` (or `Arc<dyn ArenaPool>`).
- Construct in `ExtentNodeStore::new` using `DedicatedArenaPool::new(DEFAULT_EXTENT_CAPACITY)`.

Edit `components/extent-node/src/store/register.rs`: when creating a new Stream, pass `store.default_pool.clone()`.

- [ ] **Step 4: Build + test**

Run: `cargo check --workspace --tests --benches 2>&1 | tail -10`
Expected: clean.

Run: `cargo test --lib --workspace 2>&1 | grep -E '^test result:'`
Expected: same pass count.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "$(cat <<'EOF'
refactor(stream): allocate epochs through ArenaPool

Stream now owns an Arc<dyn ArenaPool> and asks it to mint each new
StreamEpoch rather than constructing the arena inline. Every stream in
P2 gets a DedicatedArenaPool; SharedArenaPool routing is wired in a
later plan.

The change centralizes arena sizing in one place (the pool's
configured arena_size) and prepares the way for Shared streams to
instead route allocation through the EN-wide pool.

No behavior change.
EOF
)"
```

---

## Phase 6: ArenaId + resident_arenas + directory_ref_count

Goal: introduce the globally-unique `ArenaId` and the epoch bookkeeping fields from the spec (`resident_arenas`, `directory_ref_count`). Still Dedicated-only, so each `StreamEpoch.resident_arenas` holds exactly one `ArenaId` at a time.

### Task 6.1: ArenaId type + generator

**Files:**
- Create: `components/extent-node/src/arena/id.rs`
- Modify: `components/extent-node/src/arena/mod.rs`

- [ ] **Step 1: Define ArenaId**

Create `components/extent-node/src/arena/id.rs`:

```rust
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

/// Globally-unique arena identifier.
///
/// Layout: `(node_id_hash << 48) | local_counter`, giving 16 bits of
/// node identity (up to 65,535 ENs) and 48 bits of per-node counter.
/// See the shared-arena spec §ArenaId for the scheme.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ArenaId(pub u64);

impl ArenaId {
    pub fn new(node_prefix: u16, counter: u64) -> Self {
        assert!(counter < (1u64 << 48), "ArenaId counter overflow");
        Self(((node_prefix as u64) << 48) | (counter & ((1u64 << 48) - 1)))
    }

    pub fn node_prefix(&self) -> u16 {
        (self.0 >> 48) as u16
    }

    pub fn counter(&self) -> u64 {
        self.0 & ((1u64 << 48) - 1)
    }
}

impl std::fmt::Display for ArenaId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

/// Allocates monotonically-increasing ArenaIds for one EN.
pub struct ArenaIdGenerator {
    node_prefix: u16,
    counter:     AtomicU64,
}

impl ArenaIdGenerator {
    pub fn new(node_prefix: u16) -> Self {
        Self { node_prefix, counter: AtomicU64::new(0) }
    }

    pub fn next(&self) -> ArenaId {
        let c = self.counter.fetch_add(1, Ordering::Relaxed);
        ArenaId::new(self.node_prefix, c)
    }
}

/// Hash an arbitrary node_id string into a 16-bit prefix.
/// Collisions across the cluster mean duplicate ArenaIds, so the
/// 16-bit space should be treated as best-effort; the operator
/// should arrange at most 65k ENs per cluster or use explicit
/// per-node assignments.
pub fn node_prefix_from_id(node_id: &str) -> u16 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    node_id.hash(&mut h);
    (h.finish() as u16).wrapping_add(1)  // avoid 0x0000 so logs make the source obvious
}
```

Update `components/extent-node/src/arena/mod.rs`:

```rust
mod buffer;
mod directory;
mod id;
mod pool;

pub(crate) use buffer::{ArenaBuffer, OwnedArenaSlice};
pub(crate) use directory::{ArenaDirectory, EpochArenaEntry};
pub use id::{ArenaId, ArenaIdGenerator, node_prefix_from_id};
pub(crate) use pool::{ArenaPool, DedicatedArenaPool, SharedArenaPool};
```

- [ ] **Step 2: Plumb an ArenaIdGenerator through ExtentNodeStore**

In `components/extent-node/src/store/mod.rs`:

```rust
pub struct ExtentNodeStore {
    // … existing fields …
    pub(crate) arena_ids: Arc<ArenaIdGenerator>,
}
```

Construct in `ExtentNodeStore::new`: use the node_id string from config, hash to a prefix via `node_prefix_from_id`. If the store is constructed before the node_id is resolved, default to prefix 0x0001 and set the real value on `set_node_id()` (add a method if needed, or simply pass the node_id at construction — prefer the latter).

Edit `lib.rs` to pass `config.node_id` (or the resolved advertise_addr if node_id is empty) through to `ExtentNodeStore::new`.

- [ ] **Step 3: Stamp the ArenaId on DedicatedArenaPool::allocate_epoch**

Change `DedicatedArenaPool::new` to accept an `Arc<ArenaIdGenerator>`:

```rust
pub(crate) struct DedicatedArenaPool {
    arena_size: u32,
    ids:        Arc<ArenaIdGenerator>,
}

impl DedicatedArenaPool {
    pub(crate) fn new(arena_size: u32, ids: Arc<ArenaIdGenerator>) -> Self {
        Self { arena_size, ids }
    }
}
```

In `allocate_epoch` call `self.ids.next()` and pass it to the new `StreamEpoch::with_capacity(..., arena_id)` signature.

- [ ] **Step 4: Add arena_id field to StreamEpoch**

In `components/extent-node/src/stream_epoch.rs`:

```rust
pub struct StreamEpoch {
    pub id: ExtentId,
    pub arena_id: ArenaId,
    // … existing fields …
}

impl StreamEpoch {
    pub fn with_capacity(
        extent_id:    ExtentId,
        start_offset: Offset,
        capacity:     u32,
        epoch:        Epoch,
        class:        ArenaClass,
        arena_id:     ArenaId,
    ) -> Self { /* assign arena_id */ }
}
```

- [ ] **Step 5: Build + test**

Run: `cargo check --workspace --tests --benches 2>&1 | tail -10`
Expected: clean.

Run: `cargo test --lib --workspace 2>&1 | grep -E '^test result:'`
Expected: same pass count.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "$(cat <<'EOF'
feat(arena): introduce ArenaId = (node_prefix<<48) | counter

Each arena allocated by DedicatedArenaPool is stamped with a globally
unique ArenaId. The top 16 bits are a stable hash of the EN's node_id;
the bottom 48 bits are a monotone per-node counter.

This aligns with the shared-arena spec: S3 keys of the form
{namespace}/arenas/{arena_id:016x}.dat will not collide across ENs.
Shape A upload is a later plan; ArenaId is in place so readers added
there have one naming authority.
EOF
)"
```

### Task 6.2: resident_arenas + directory_ref_count on StreamEpoch

**Files:**
- Modify: `components/extent-node/src/stream_epoch.rs`

- [ ] **Step 1: Add the fields**

```rust
use std::sync::Mutex;
use smallvec::SmallVec;

pub struct StreamEpoch {
    // … existing fields …

    /// Which arenas on this EN currently hold at least one directory
    /// entry for this (stream, epoch). P2 always holds exactly one.
    pub(crate) resident_arenas: Mutex<SmallVec<[ArenaId; 4]>>,

    /// Reference count of live directory entries for this epoch
    /// across all resident arenas. When this hits zero, the owning
    /// Stream removes the epoch from Stream::epochs.
    pub(crate) directory_ref_count: AtomicU32,
}
```

- [ ] **Step 2: Initialize in with_capacity**

```rust
impl StreamEpoch {
    pub fn with_capacity(..., arena_id: ArenaId) -> Self {
        // …
        Self {
            // …
            resident_arenas: Mutex::new(smallvec![arena_id]),
            directory_ref_count: AtomicU32::new(1),
        }
    }
}
```

Exactly one entry, ref count = 1: matches the Dedicated single-entry shape.

- [ ] **Step 3: Expose accessors**

```rust
impl StreamEpoch {
    pub fn resident_arenas(&self) -> SmallVec<[ArenaId; 4]> {
        self.resident_arenas.lock().unwrap().clone()
    }

    pub fn incr_directory_ref(&self) -> u32 {
        self.directory_ref_count.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub fn decr_directory_ref(&self) -> u32 {
        let prev = self.directory_ref_count.fetch_sub(1, Ordering::Release);
        prev.saturating_sub(1)
    }
}
```

These aren't consumed by the Dedicated path in P2 but exercise-exist for the eventual P3+ multi-arena-per-epoch behavior. No call site updates needed for P2 beyond the initial `1` ref.

- [ ] **Step 4: Build + test + commit**

```bash
cargo check --workspace --tests --benches 2>&1 | tail -5
cargo test --lib --workspace 2>&1 | grep -E '^test result:'
git add components/extent-node/src/stream_epoch.rs
git commit -m "$(cat <<'EOF'
feat(stream_epoch): add resident_arenas + directory_ref_count

Introduces the two epoch-retention fields from the shared-arena spec:
a SmallVec of resident ArenaIds (always exactly one in P2's Dedicated
path) and an AtomicU32 directory_ref_count initialized to 1.

No consumers in P2 — these are the plumbing that later plans use to
decide when a sealed StreamEpoch can be removed from Stream.epochs.
EOF
)"
```

---

## Phase 7: Two-Layer CAS — WriteBatch / JobResult / Arena-Level in_flight

Goal: introduce the Layer-2 arena-level write primitive so the Dedicated path already goes through it. Because the Dedicated owner is the only writer, the CAS is uncontended and the memcpy still happens inline on the stream leader — no extra channel hop, no thread wakeups.

### Task 7.1: Define WriteBatch + JobResult

**Files:**
- Create: `components/extent-node/src/arena/write_batch.rs`
- Modify: `components/extent-node/src/arena/mod.rs`

- [ ] **Step 1: Write the types**

Create `components/extent-node/src/arena/write_batch.rs`:

```rust
use bytes::Bytes;
use common::types::{Epoch, StreamId};
use smallvec::SmallVec;
use tokio::sync::oneshot;

use crate::arena::ArenaId;

/// One record submitted inside a WriteBatch from a stream leader.
#[derive(Debug)]
pub(crate) struct SharedAppendRequest {
    pub seq:     u64,
    pub payload: Bytes,
}

/// Per-record resolved placement within an arena after the arena
/// writer has performed the memcpy. P2's Dedicated path produces one
/// JobResult per job with the same arena_id; P3's Shared path may
/// straddle an arena roll mid-batch.
#[derive(Debug, Clone, Copy)]
pub(crate) struct JobResult {
    pub arena_id: ArenaId,
    pub byte_pos: u32,
}

/// A batch from one stream leader, routed to the arena-level writer.
#[derive(Debug)]
pub(crate) struct WriteBatch {
    pub stream_id: StreamId,
    pub epoch:     Epoch,
    pub jobs:      SmallVec<[SharedAppendRequest; 16]>,
    pub reply:     oneshot::Sender<WriteBatchResult>,
}

#[derive(Debug)]
pub(crate) struct WriteBatchResult {
    pub results: SmallVec<[JobResult; 16]>,
}
```

Update `components/extent-node/src/arena/mod.rs`:

```rust
mod buffer;
mod directory;
mod id;
mod pool;
mod write_batch;

pub(crate) use buffer::{ArenaBuffer, OwnedArenaSlice};
pub(crate) use directory::{ArenaDirectory, EpochArenaEntry};
pub use id::{ArenaId, ArenaIdGenerator, node_prefix_from_id};
pub(crate) use pool::{ArenaPool, DedicatedArenaPool, SharedArenaPool};
pub(crate) use write_batch::{JobResult, SharedAppendRequest, WriteBatch, WriteBatchResult};
```

- [ ] **Step 2: Build + commit**

```bash
cargo check --workspace --tests --benches 2>&1 | tail -5
git add components/extent-node/src/arena
git commit -m "$(cat <<'EOF'
feat(arena): define WriteBatch / JobResult / SharedAppendRequest

The three types that the spec's Layer-2 arena writer consumes. No
callers yet — they land in the next commit that routes Dedicated
writes through the new Layer-2 CAS fast path.
EOF
)"
```

### Task 7.2: Arena-level in_flight + uncontended Dedicated path

**Files:**
- Modify: `components/extent-node/src/stream_epoch.rs`
- Modify: `components/extent-node/src/store/append.rs`

- [ ] **Step 1: Add arena-level CAS state**

On `StreamEpoch` (since in Dedicated it IS the arena):

> **Note:** `in_flight`, `tx`, and `rx` were
> originally described as StreamEpoch fields here, but have since moved
> to `SharedArenaPool` as `in_flight`, `tx`, and `rx` respectively.
> Dedicated arenas (one owner) don't need the delegation channel; only
> SharedArenaPool uses it. The StreamEpoch struct retains the
> arena-level CAS state only for the in-flight counter when it lives on
> the dedicated path.

```rust
use crossbeam::channel::{Receiver, Sender, unbounded};

pub struct StreamEpoch {
    // … existing fields …

    /// Arena-level leader election. In P2's Dedicated path the owning
    /// stream is the sole submitter, so this CAS is uncontended by
    /// construction.
    /// NOTE: For Shared arenas, this field lives on SharedArenaPool as
    /// `in_flight` instead. Dedicated StreamEpoch keeps it inline.
    in_flight: AtomicU64,

    /// Arena-level delegation channel. Unused on the Dedicated path
    /// (uncontended CAS means no follower ever delegates) but kept to
    /// share one struct shape across classes in P3+.
    /// NOTE: For Shared arenas, these live on SharedArenaPool as
    /// `tx` and `rx` instead.
    tx: Sender<WriteBatch>,
    rx: Receiver<WriteBatch>,
}

impl StreamEpoch {
    pub fn with_capacity(..., arena_id: ArenaId) -> Self {
        let (tx, rx) = unbounded();
        Self {
            // …
            in_flight: AtomicU64::new(0),
            tx, rx,
        }
    }
}
```

- [ ] **Step 2: Add write_batch entry point on StreamEpoch**

```rust
impl StreamEpoch {
    /// Write a batch of records into this epoch's arena.
    /// Dedicated path: caller is the stream leader, no other writers,
    /// uncontended CAS. Memcpies inline; returns per-record JobResults.
    pub(crate) fn write_batch(&self, batch: &[SharedAppendRequest]) -> Result<SmallVec<[JobResult; 16]>, StorageError> {
        let prev = self.in_flight.fetch_add(1, Ordering::Acquire);
        debug_assert_eq!(prev, 0, "Dedicated arena must be uncontended");

        let mut results: SmallVec<[JobResult; 16]> = SmallVec::new();
        for job in batch {
            let byte_pos = self.append_one(job.seq, job.payload.clone())?;
            results.push(JobResult { arena_id: self.arena_id, byte_pos: byte_pos as u32 });
        }

        self.in_flight.fetch_sub(1, Ordering::Release);
        Ok(results)
    }

    /// Append a single record at the current write cursor. Extracted
    /// helper over today's `append` logic so write_batch doesn't
    /// re-implement the memcpy.
    fn append_one(&self, seq: u64, payload: Bytes) -> Result<u64, StorageError> {
        // Refactor today's append() body here, returning byte_pos.
        // The existing append() can stay as a thin wrapper for
        // single-record callers:
        //     let seq = self.record_count.load(...);
        //     self.append_one(seq, payload).map(|bp| AppendResult { offset, byte_pos: bp })
    }
}
```

The refactor should preserve today's append hot path byte-for-byte; just factor the memcpy + cursor-advance logic into `append_one`. Run benchmarks if available to confirm no regression — acceptable: we're comparing two function-call paths that LLVM will inline.

- [ ] **Step 3: Have the stream leader call write_batch**

In `components/extent-node/src/store/append.rs`, at the place where today's leader memcpies each drained AppendRequest into the active epoch: collect the jobs into a SmallVec and issue one `active_epoch.write_batch(&jobs)?` call. Apply the returned JobResults (byte_pos) to each forwarded frame's downstream send.

Note: the per-job `arena_id` returned by `write_batch` is all the same in P2 (one arena per batch since Dedicated doesn't roll mid-batch). Record it anyway for P3-readiness.

- [ ] **Step 4: Build + test**

Run: `cargo check --workspace --tests --benches 2>&1 | tail -10`
Run: `cargo test --lib --workspace 2>&1 | grep -E '^test result:'`
Expected: same pass count.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "$(cat <<'EOF'
feat(arena): Dedicated path flows through Layer-2 CAS write_batch

Stream leader now assembles a batch of SharedAppendRequest and calls
StreamEpoch::write_batch instead of calling today's single-record
append() in a loop. P2 is Dedicated-only so the arena-level
in_flight CAS is uncontended by construction; memcpy still runs
inline on the leader thread.

The channel-delegation fallback (tx / rx) is
present but unused — it lands hot in a later plan when Shared
streams cause arena-level contention.
EOF
)"
```

---

## Phase 8: Full Validation + PR

### Task 8.1: Full workspace tests + grep sweep

- [ ] **Step 1: Run library tests**

Run: `cargo test --lib --workspace 2>&1 | grep -E '^test result:'`
Expected: same count as baseline.

- [ ] **Step 2: Grep sweep for rename leftovers**

Run:
```bash
grep -rn 'struct Extent\b\|impl Extent\b\|-> Extent\b\|fn find_extent\|ExtentState\b' components/ tests/ --include='*.rs'
```
Expected: zero. (`ExtentId` is allowed — the u32 identity type is unchanged.)

- [ ] **Step 3: Grep for adaptive-capacity / SystemTick leftovers one more time**

Run:
```bash
grep -rn 'SystemTick\|adaptive.*capacity\|min_extent_capacity\|growth_factor' components/ --include='*.rs'
```
Expected: zero.

- [ ] **Step 4: Final build warnings check**

Run: `RUSTFLAGS="-D warnings" cargo check --workspace --tests --benches 2>&1 | tail -20`
Expected: clean.

### Task 8.2: Push + PR

- [ ] **Step 1: Push**

```bash
git push -u origin opt/arena
```

- [ ] **Step 2: Open PR**

```bash
gh pr create --title "P2: arena pool abstraction + two-layer CAS scaffolding" --body "$(cat <<'EOF'
## Summary

- Rename `Extent` → `StreamEpoch`; move to `stream_epoch.rs`
- `Stream.epochs` becomes `ArcSwap<SmallVec<[Arc<StreamEpoch>; 4]>>` with the helper API from the shared-arena spec
- Introduce `ArenaBuffer` / `ArenaDirectory` / `ArenaId` / `ArenaPool` as named, independently-testable building blocks
- Plumb `ArenaClass` end-to-end (MySQL column, wire field on `RegisterEpoch` + `ForwardInitEpoch`, EN runtime): every stream is `Dedicated` in this PR
- Introduce the Layer-2 arena-level CAS (`WriteBatch` / `JobResult`) — uncontended on the Dedicated path in this PR

No behavior change at the protocol level. Shared-class routing, Shape A upload, multi-entry directories, and `ForwardFlushed` payload widening are follow-up plans.

## Test plan

- [x] `cargo check --workspace --tests --benches` clean
- [x] `cargo test --lib --workspace` — no regression vs. baseline
- [ ] Manual: bring up 1 SM + 3 EN, run the standard append/read smoke
- [ ] Integration suite under testcontainers (locally, before merge)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-Review Notes

1. **Spec coverage** — The plan covers §ArenaClass (Task 4.1–4.3), §Epoch/StreamEpoch (Phase 2 + 3), §ArenaPool abstraction (Phase 5), §ArenaId (Task 6.1), §StreamEpoch bookkeeping fields (Task 6.2), §Arena Concurrency Primitive partial (Phase 7 introduces the primitive on the Dedicated path; the Shared-arena leader loop is explicitly deferred).

2. **Deferred (gap-closed by later plans)** — Shared arenas' multi-entry directory (P3), ForwardFlushed payload widening (P4), Shape A upload (P4), DR path (P4), runtime class promotion/demotion (P5), `epoch_arenas` table (P4).

3. **Type consistency** — `StreamEpoch::with_capacity` signature evolves across phases: P2 (`extent_id, start_offset, capacity, epoch`) → P4.3 adds `class: ArenaClass` → P6.1 adds `arena_id: ArenaId`. Each phase that changes the signature updates all call sites within that phase's commit. End-of-plan signature: `(extent_id, start_offset, capacity, epoch, class, arena_id)`.

4. **Build-broken windows** — Phase 2 (rename) combines Tasks 2.1 and 2.2 into a single subagent dispatch to minimize the window. Every other phase closes each task with a green build.

5. **Risk** — The biggest risk is the `ArcSwap<SmallVec<…>>` migration in Phase 3: many call sites. Mitigation: keep `StreamInner` for the non-epoch fields so the diff surface is bounded to "where extents were read/mutated".

---

## Glossary + Vocabulary Notes

- "Epoch" in this plan refers to the runtime object `StreamEpoch`, plus the u32 monotonic counter `Epoch` already defined in `common/types.rs`. Both coexist.
- "Extent" survives in: `ExtentId` (u32 row id), the MySQL column `extent_id`, log messages, integration test fixture names. All retained for P2; cleanup is deferred.
- "Arena" is new vocabulary in the EN code — a buffer + directory pair. Previously the Extent was a conflation of epoch-identity + arena; P2 splits them.
