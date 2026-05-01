# P1: Collapse Extent into Epoch — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the `ExtentId` / `extent_id` identifier and the autonomous-extent-creation path from the stream-store codebase. After this plan, `(stream_id, epoch)` is the sole per-stream record-span identity across the wire, DB, and EN code. This is a pre-requisite refactor for the Shared Arena feature (plans P2–P5 build on top).

**Architecture:** Mechanical refactor, touching ~1000 references across 20+ files. Each task leaves the tree compiling and all tests passing. Decomposed into six phases, in order: (1) remove adaptive extent capacity, (2) remove EN-initiated seal path (`UpdateExtentSealed`), (3) rename client-facing opcodes `SealStreamManager` → `SealStream`, (4) rename internal opcodes `SealExtentNode` → `SealEpoch`, `RegisterExtent` → `RegisterEpoch`, `ForwardInitExtent` → `ForwardInitEpoch`, (5) remove `byte_pos` from `Forward` wire format, (6) rename MySQL `extent` table to `stream_epochs` and the `extents` metadata code module.

**Tech Stack:** Rust 1.80+, Tokio async runtime, `bytes` crate, `sqlx` for MySQL, Refinery for migrations, existing custom TCP protocol in `components/rpc/`.

**Reference spec:** `docs/superpowers/specs/2026-04-24-shared-arena-design.md`

**Out of scope for P1:**
- `ArenaClass` (Shared/Dedicated) — that's P3
- Arena pool abstraction, `SharedArena`, directory-based reads — P2
- DR flush (`FlushEpochStream`, Shape B) — P4
- Runtime class transitions, metrics — P5

---

## File Structure

This plan modifies existing files in place. No new files are created in P1.

| File | Role in refactor |
|---|---|
| `components/common/src/types.rs` | Opcode enum, `ExtentId`, `ExtentState`, `StorageClass` (keep); remove `ExtentId` in phase 6 |
| `components/common/src/config.rs` | Delete `DEFAULT_MIN_EXTENT_CAPACITY`, `DEFAULT_MAX_EXTENT_CAPACITY`, `DEFAULT_EXTENT_GROWTH_FACTOR` in phase 1 |
| `components/rpc/src/frame/header.rs` | `Forward` variant: remove `byte_pos`; rename `ForwardInitExtent` → `ForwardInitEpoch`, remove capacity fields; rename `Seal*` variants |
| `components/rpc/src/frame/encode.rs` | Matching encoder changes |
| `components/rpc/src/frame/decode.rs` | Matching decoder changes |
| `components/rpc/src/payload.rs` | `UpdateExtentSealed` payload helpers (phase 2) |
| `components/extent-node/src/stream.rs` | Remove adaptive capacity fields, extent_pool, idle-shrink logic; rename `ExtentId` usage |
| `components/extent-node/src/extent.rs` | Arena sizing fixed; remove growth/shrink methods |
| `components/extent-node/src/store/append.rs` | Delete `seal_and_create` autonomous path |
| `components/extent-node/src/store/register.rs` | Rename to handle `RegisterEpoch`; drop adaptive-capacity normalization |
| `components/extent-node/src/store/forward.rs` | Rename `handle_forward_init_extent`, remove `byte_pos` handling |
| `components/extent-node/src/store/seal.rs` | Rename for `SealEpoch`; remove `FLAG_OFFSET_PRESENT` handling |
| `components/stream-manager/src/metadata.rs` | `StreamRow`: drop capacity fields; rename extents table references to `stream_epochs` |
| `components/stream-manager/src/store.rs` | Rename handlers; remove `notify_extent` on sealed path |
| `components/stream-manager/src/allocator.rs` | Touch only if references capacity fields |
| `components/stream-manager/migrations/V2__create_extent_table.sql` | Rename to `V2__create_stream_epochs_table.sql`, rename table |
| `tests/*.rs` (8 files) | Rename identifier references mechanically |

---

## Phase 1: Remove Adaptive Extent Capacity

Goal: After this phase, extent capacity is a single fixed value per EN (config default). No growth factor, no idle shrink, no `extent_pool` recycling of resized arenas.

> **Reviewer note:** Tasks 1.2 through 1.8 form a single logical unit — the build is intentionally broken in Task 1.2 (one constant rename that cascades through the workspace) and is not closed until Task 1.8. Do not split these across review cycles. Task 1.9 is the phase-closing commit; review the full phase diff there.

### Task 1.1: Inventory — find every adaptive-capacity symbol

**Files:**
- Read: all

- [ ] **Step 1: Enumerate the surface area**

Run:
```bash
cd /data/repo/stream-store
grep -rn 'min_extent_capacity\|max_extent_capacity\|extent_growth_factor\|DEFAULT_MIN_EXTENT_CAPACITY\|DEFAULT_MAX_EXTENT_CAPACITY\|DEFAULT_EXTENT_GROWTH_FACTOR\|extent_pool\|idle_shrink\|DEFAULT_IDLE_SHRINK' \
  --include='*.rs' --include='*.sql' --include='*.toml' -l
```

Expected: The following files (this is the phase-1 work surface):
```
components/common/src/config.rs
components/rpc/src/frame/header.rs
components/rpc/src/frame/encode.rs
components/rpc/src/frame/decode.rs
components/extent-node/src/extent.rs
components/extent-node/src/stream.rs
components/extent-node/src/store/register.rs
components/extent-node/src/store/append.rs
components/extent-node/src/store/types.rs
components/stream-manager/src/metadata.rs
components/stream-manager/src/store.rs
conf/*.toml
tests/*.rs
```

No commit.

### Task 1.2: Pick the fixed extent capacity

**Files:**
- Modify: `components/common/src/config.rs`

- [ ] **Step 1: Read the current constants**

Run:
```bash
grep -n 'DEFAULT_MIN_EXTENT_CAPACITY\|DEFAULT_MAX_EXTENT_CAPACITY\|DEFAULT_EXTENT_GROWTH_FACTOR\|DEFAULT_EXTENT_CAPACITY' components/common/src/config.rs
```

- [ ] **Step 2: Replace three constants with one**

In `components/common/src/config.rs`:
- Delete `DEFAULT_MIN_EXTENT_CAPACITY`, `DEFAULT_MAX_EXTENT_CAPACITY`, `DEFAULT_EXTENT_GROWTH_FACTOR`.
- Add a single `DEFAULT_EXTENT_CAPACITY: u32 = 256 * 1024 * 1024;` (256 MiB — the current `max` default, which becomes the fixed size).
- In `ExtentNodeConfig`, replace any `min_extent_capacity`, `max_extent_capacity`, `extent_growth_factor` fields with `extent_capacity: u32` defaulting to `DEFAULT_EXTENT_CAPACITY`.

- [ ] **Step 3: Check it compiles (expecting errors downstream)**

Run: `cargo check -p common`
Expected: PASS (this crate has no downstream dependencies yet)

Run: `cargo check --workspace`
Expected: FAIL with multiple "no field `min_extent_capacity`" / "cannot find value `DEFAULT_MIN_EXTENT_CAPACITY`" errors. List them — they are the fix list for Tasks 1.3–1.6.

No commit; build is intentionally broken.

### Task 1.3: Remove adaptive capacity from `Extent`

**Files:**
- Modify: `components/extent-node/src/extent.rs`

- [ ] **Step 1: Read the extent module**

Run: `wc -l components/extent-node/src/extent.rs && grep -n 'grow\|shrink\|capacity' components/extent-node/src/extent.rs | head -40`

- [ ] **Step 2: Delete grow / shrink code paths**

In `components/extent-node/src/extent.rs`:
- Remove any `fn grow`, `fn shrink`, `fn resize` methods on `Extent` or `ArenaBuffer`.
- Keep `Extent::with_capacity(capacity: u32, ...)` — the capacity is now a caller-supplied fixed value.
- Delete any `idle_shrink` timer fields on `Extent`.
- Remove any tests for grow/shrink behavior in `#[cfg(test)]` blocks (they will fail to compile anyway).

- [ ] **Step 3: Verify the extent module compiles**

Run: `cargo check -p extent-node 2>&1 | head -40`
Expected: may have errors in files that call the deleted methods. Note their locations for Task 1.4.

No commit.

### Task 1.4: Remove adaptive capacity from `Stream`

**Files:**
- Modify: `components/extent-node/src/stream.rs`

- [ ] **Step 1: Read the current StreamInner**

Run:
```bash
sed -n '40,80p' components/extent-node/src/stream.rs
```

- [ ] **Step 2: Delete the three fields, the pool, and the growth/shrink logic**

In `components/extent-node/src/stream.rs`:
- In `StreamInner`, delete fields: `min_extent_capacity`, `max_extent_capacity`, `extent_growth_factor`, and `extent_pool: VecDeque<Extent>`.
- Add or keep `extent_capacity: u32` (read from config once on stream creation).
- In the constructor, delete initialization of the three removed fields and the `extent_pool` VecDeque.
- In `seal_and_create_next_extent` (or whatever the extent rotation helper is named), delete the capacity scaling block (lines ~780–795 per the earlier exploration).
- Delete any idle-shrink timer task spawned by stream creation.
- Delete `ExtentFull` growth branches.

- [ ] **Step 3: Verify stream module compiles in isolation**

Run: `cargo check -p extent-node 2>&1 | tail -20`
Expected: may still have errors in `store/*.rs`. Note their locations for Task 1.5.

No commit yet.

### Task 1.5: Remove capacity normalization from `store::register` and `store::append`

**Files:**
- Modify: `components/extent-node/src/store/register.rs`
- Modify: `components/extent-node/src/store/append.rs`
- Modify: `components/extent-node/src/store/types.rs`

- [ ] **Step 1: Strip capacity fields from `register`**

In `components/extent-node/src/store/register.rs`:
- Find the `RegisterExtent` handler.
- Remove any code that reads `min_extent_capacity`, `max_extent_capacity`, `extent_growth_factor` from the frame and clamps / normalizes them.
- Pass only `extent_capacity` (or the config's fixed value) to `Stream::register_extent`.

- [ ] **Step 2: Strip capacity scaling from `append`'s seal-and-create**

In `components/extent-node/src/store/append.rs`:
- In `seal_and_create(...)`, where it computes the next extent's capacity via the growth formula, replace with `let next_capacity = self.config.extent_capacity;`.

- [ ] **Step 3: Strip capacity fields from `ExtentUpdate` / `ReplicaInfo` / `AppendJob` structs in types.rs**

In `components/extent-node/src/store/types.rs`: remove any `min_extent_capacity`, `max_extent_capacity`, `extent_growth_factor` fields from structs; keep only `extent_capacity`.

- [ ] **Step 4: Verify**

Run: `cargo check -p extent-node 2>&1 | tail -20`
Expected: PASS (no more errors in this crate).

No commit yet — `rpc` and `stream-manager` are still referencing the deleted constants through wire frames.

### Task 1.6: Strip capacity fields from `ForwardInitExtent` wire format

**Files:**
- Modify: `components/rpc/src/frame/header.rs`
- Modify: `components/rpc/src/frame/encode.rs`
- Modify: `components/rpc/src/frame/decode.rs`

- [ ] **Step 1: Update the `ForwardInitExtent` variant**

In `components/rpc/src/frame/header.rs`, update the `ForwardInitExtent` enum variant to drop the three adaptive fields:

Before:
```rust
ForwardInitExtent {
    stream_id: StreamId,
    extent_id: ExtentId,
    epoch: Epoch,
    start_offset: Offset,
    extent_capacity: u32,
    cache_extents: u16,
    min_extent_capacity: u32,
    max_extent_capacity: u32,
    extent_growth_factor: u8,
    storage_class: StorageClass,
},
```

After:
```rust
ForwardInitExtent {
    stream_id: StreamId,
    extent_id: ExtentId,
    epoch: Epoch,
    start_offset: Offset,
    extent_capacity: u32,
    cache_extents: u16,
    storage_class: StorageClass,
},
```

- [ ] **Step 2: Update the encoder**

In `components/rpc/src/frame/encode.rs`, find `ForwardInitExtent` encoding. Remove the 9 bytes worth of `min_extent_capacity` (4) + `max_extent_capacity` (4) + `extent_growth_factor` (1) from:
- The size calculation at ~line 100
- The write body at ~line 509+

- [ ] **Step 3: Update the decoder**

In `components/rpc/src/frame/decode.rs` at ~line 645, remove the three reads for the deleted fields.

- [ ] **Step 4: Verify rpc**

Run: `cargo check -p rpc`
Expected: PASS.

- [ ] **Step 5: Verify workspace**

Run: `cargo check --workspace 2>&1 | tail -40`
Expected: the only remaining errors are in `stream-manager` (capacity fields in `StreamRow`) and tests. Note them.

No commit yet.

### Task 1.7: Strip capacity fields from Stream Manager metadata

**Files:**
- Modify: `components/stream-manager/src/metadata.rs`
- Modify: `components/stream-manager/src/store.rs`
- Modify: any `*.sql` migration that adds these columns

- [ ] **Step 1: Inventory the SM surface**

Run:
```bash
grep -n 'min_extent_capacity\|max_extent_capacity\|extent_growth_factor' \
  components/stream-manager/src/*.rs components/stream-manager/migrations/*.sql
```

- [ ] **Step 2: Remove fields from `StreamRow` and `create_stream`**

In `components/stream-manager/src/metadata.rs`:
- Remove `min_extent_capacity`, `max_extent_capacity`, `extent_growth_factor` from `StreamRow`.
- Remove these fields from the `create_stream` signature and from any `INSERT INTO streams` query.
- Update the `SELECT ... FROM streams` queries to drop those columns.

- [ ] **Step 3: Update call sites**

In `components/stream-manager/src/store.rs`:
- Remove these fields from any frame decode/encode paths (they should have already been removed in Task 1.6 on the wire side, so this is tightening call sites).
- Any frame construction that sent `ForwardInitExtent` now has fewer arguments.

- [ ] **Step 4: Write a new SQL migration to drop the columns**

Create `components/stream-manager/migrations/V7__drop_adaptive_capacity.sql` (check the next available `V` number with `ls components/stream-manager/migrations`):

```sql
ALTER TABLE streams
  DROP COLUMN IF EXISTS min_extent_capacity,
  DROP COLUMN IF EXISTS max_extent_capacity,
  DROP COLUMN IF EXISTS extent_growth_factor;
```

- [ ] **Step 5: Verify workspace compiles**

Run: `cargo check --workspace 2>&1 | tail -20`
Expected: PASS (may still have test failures).

### Task 1.8: Fix tests broken by capacity removal

**Files:**
- Modify: `components/extent-node/src/*.rs` test modules, `components/stream-manager/src/*.rs` test modules, `tests/*.rs`, `conf/*.toml`

- [ ] **Step 1: Enumerate broken tests**

Run: `cargo build --workspace --tests 2>&1 | grep -E '^error' | head -40`

- [ ] **Step 2: Mechanically fix each reference**

For each file reported, replace test-construction of `StreamRow`, `CreateStream` requests, `ForwardInitExtent` frames, `ExtentNodeConfig` etc. to use the single `extent_capacity` field instead of the three removed fields. Any test that explicitly exercised growth-factor or idle-shrink behavior: delete that test with a comment `// REMOVED: adaptive extent capacity was dropped in P1 of shared-arena refactor`.

- [ ] **Step 3: Update config TOML**

In each `conf/*.toml` and any inline config strings in tests, remove lines for `min_extent_capacity`, `max_extent_capacity`, `extent_growth_factor`; keep `extent_capacity` (or add it with `256MiB` / `268435456`).

- [ ] **Step 4: Run all tests**

Run: `cargo test --workspace 2>&1 | tail -20`
Expected: all tests PASS.

### Task 1.9: Commit phase 1

- [ ] **Step 1: Review the diff**

Run: `git diff --stat`

- [ ] **Step 2: Commit**

```bash
git add -A
git commit -m "$(cat <<'EOF'
refactor(extent): remove adaptive extent capacity

Delete min_extent_capacity, max_extent_capacity, extent_growth_factor
and the idle-shrink timer. Replaced by a single fixed
extent_capacity (default 256 MiB) in ExtentNodeConfig.

Per spec docs/superpowers/specs/2026-04-24-shared-arena-design.md,
ArenaClass (P3) will solve the tradeoff that adaptive capacity was
addressing; fixed capacity is correct for the post-refactor design.

Wire format: ForwardInitExtent drops 9 bytes (min/max/growth).
SQL: V7 migration drops the three columns from `streams`.
EOF
)"
```

Expected: commit succeeds.

---

## Phase 2: Remove EN-initiated Seal Notification

Goal: After this phase, the Primary EN no longer notifies SM when it autonomously seals a full extent. The `UpdateExtentSealed` wire variant and its SM-side handler are deleted. The EN's local `seal_and_create` extent-full path stays intact — it's internal, with no wire visibility — until P2 replaces extent rolling with arena rolling.

This is a narrow, surgical removal that doesn't change correctness for any in-tree scenario today (the notification was fire-and-forget; SM reconciled metadata at epoch bump anyway).

### Task 2.1: Inventory `UpdateExtentSealed`

**Files:**
- Read: all

- [ ] **Step 1: Find references**

Run:
```bash
grep -rn 'UpdateExtentSealed\|UpdateExtent\b\|update_extent_sealed\|notify_extent_sealed\|NOTIFY_SEALED' --include='*.rs' --include='*.sql'
```

Expected: references in `rpc/src/frame/header.rs` (variant), `rpc/src/frame/encode.rs`, `rpc/src/frame/decode.rs`, `rpc/src/payload.rs`, `extent-node/src/store/append.rs` (sends it), `stream-manager/src/store.rs` (receives it).

### Task 2.2: Delete the SM-side handler for `UpdateExtentSealed`

**Files:**
- Modify: `components/stream-manager/src/store.rs`

- [ ] **Step 1: Find and delete the handler**

Locate the match arm or dispatch for `Opcode::UpdateExtent` with the sealed flag variant. Delete the entire handler function `notify_extent_sealed` (or equivalent) and its dispatch.

- [ ] **Step 2: Keep the progress-update variant if any**

`UpdateExtent` has multiple flag variants (sealed vs progress per the opcode comment). Keep the progress variant intact; only the sealed variant is being removed.

- [ ] **Step 3: Verify SM compiles**

Run: `cargo check -p stream-manager`
Expected: PASS.

### Task 2.3: Remove the EN-side send of `UpdateExtentSealed`

**Files:**
- Modify: `components/extent-node/src/store/append.rs`

- [ ] **Step 1: Find the call**

Run: `grep -n 'UpdateExtentSealed\|update_extent_sealed' components/extent-node/src/store/append.rs`

- [ ] **Step 2: Delete**

Inside `seal_and_create` (or wherever `UpdateExtentSealed` is constructed after a successful local seal), delete the frame construction and the fire-and-forget send.

Keep the local seal + next-extent allocation — these are still needed for correctness until P2.

- [ ] **Step 3: Verify**

Run: `cargo check -p extent-node`
Expected: PASS.

### Task 2.4: Remove `UpdateExtentSealed` variant from the wire format

**Files:**
- Modify: `components/rpc/src/frame/header.rs`
- Modify: `components/rpc/src/frame/encode.rs`
- Modify: `components/rpc/src/frame/decode.rs`
- Modify: `components/rpc/src/payload.rs`

- [ ] **Step 1: Identify `UpdateExtent` variants**

In `header.rs`, the `UpdateExtent` opcode has flag-based variants. Find the `UpdateExtentSealed` case in the `VariableHeader` enum.

- [ ] **Step 2: Delete the variant and its encode/decode**

- Delete `UpdateExtentSealed` from the `VariableHeader` enum.
- Delete the matching arm in encode.rs.
- Delete the matching arm in decode.rs (and return `FrameParseError::UnknownFlag` for that flag now).
- Delete any payload helpers in `payload.rs`.

Keep `UpdateExtent` opcode intact; the progress variant (if any) remains.

- [ ] **Step 3: Verify**

Run: `cargo check --workspace 2>&1 | tail -20`
Expected: PASS.

### Task 2.5: Run tests and commit phase 2

- [ ] **Step 1: Fix any broken tests**

Run: `cargo build --workspace --tests 2>&1 | grep -E '^error'`
If any test constructs `UpdateExtentSealed`, delete or update that test.

- [ ] **Step 2: Run tests**

Run: `cargo test --workspace 2>&1 | tail -10`
Expected: all PASS.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "$(cat <<'EOF'
refactor(seal): remove UpdateExtentSealed EN→SM notification

Deletes the fire-and-forget notification sent by the Primary after
autonomous extent seal-and-create (NOTIFY_SEALED_EXTENT / UpdateExtent
sealed flag). Per the shared-arena spec, arena full will not bump
the epoch; the EN rolls its arena internally in P2 without SM
involvement.

EN still performs local seal-and-create for extent-full until P2
replaces it with arena rolling.
EOF
)"
```

---

## Phase 3: Rename `SealStreamManager` → `SealStream`

Goal: Narrow rename of the client↔SM opcode. Pure identifier change; wire byte is unchanged (0x06).

### Task 3.1: Rename the opcode enum variant

**Files:**
- Modify: `components/common/src/types.rs`

- [ ] **Step 1: Rename in the enum**

In `components/common/src/types.rs` line 129:
```rust
SealStream = 0x06,
```
(was `SealStreamManager = 0x06,`)

Update the doc comment:
```rust
/// Client → SM: seal current epoch and allocate next. Flags: 0x00=request, 0x01=response, 0x80=error.
```

- [ ] **Step 2: Rename in `from_u8`**

Line ~177:
```rust
0x06 => Some(Opcode::SealStream),
```

- [ ] **Step 3: Verify**

Run: `cargo check --workspace 2>&1 | grep -c 'SealStreamManager'`
Expected: a small number, all from call sites that need updating.

### Task 3.2: Update call sites

- [ ] **Step 1: Rename across the workspace**

Run:
```bash
grep -rln 'SealStreamManager' --include='*.rs'
```

For each file in the list, rename `SealStreamManager` → `SealStream`. Use an editor find-replace if comfortable; otherwise one file at a time.

Pay attention to:
- Frame decoder match arms in `components/rpc/src/frame/decode.rs`
- Stream Manager store dispatch in `components/stream-manager/src/store.rs`
- Client code in `components/client/src/lib.rs`
- Test files

Do **not** rename variable names like `seal_stream_manager_request` — only the opcode identifier itself. Local variable names can be renamed for clarity in a separate grooming commit if desired, but for this task keep diffs surgical.

- [ ] **Step 2: Build and test**

Run: `cargo test --workspace 2>&1 | tail -10`
Expected: all PASS.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "refactor(opcodes): rename SealStreamManager to SealStream (0x06 unchanged)"
```

---

## Phase 4: Rename Internal Opcodes

Goal: Rename `SealExtentNode` → `SealEpoch`, `RegisterExtent` → `RegisterEpoch`, and the `ForwardInitExtent` variant → `ForwardInitEpoch`. Wire bytes unchanged. Each rename is one task; each leaves the tree green.

### Task 4.1: `SealExtentNode` → `SealEpoch`

**Files:**
- Modify: `components/common/src/types.rs`, then call sites

- [ ] **Step 1: Rename the enum variant**

In `components/common/src/types.rs`:
- Line 132: `SealEpoch = 0x07,` with updated doc comment `/// SM → EN: internal 2-phase Prepare/Commit against one replica.`
- Line 178: `0x07 => Some(Opcode::SealEpoch),`

- [ ] **Step 2: Rename call sites**

Run: `grep -rln 'SealExtentNode' --include='*.rs'`

Rename in each file listed. Also rename any frame variant with the same name in `components/rpc/src/frame/header.rs`, and its encode/decode arms.

- [ ] **Step 3: Function names**

Any `fn seal_extent_node_static` or `fn handle_seal_extent_node` → `fn seal_epoch_static` / `fn handle_seal_epoch`. Update call sites accordingly.

- [ ] **Step 4: Build and test**

Run: `cargo test --workspace 2>&1 | tail -10`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor(opcodes): rename SealExtentNode to SealEpoch (0x07 unchanged)"
```

### Task 4.2: `RegisterExtent` → `RegisterEpoch`

**Files:**
- Modify: `components/common/src/types.rs`, then call sites

- [ ] **Step 1: Rename in the enum**

In `components/common/src/types.rs` line 145:
```rust
/// Register epoch replica on an EN. Flags: 0x00=request, 0x01=ack, 0x80=error.
RegisterEpoch = 0x15,
```

Line 185:
```rust
0x15 => Some(Opcode::RegisterEpoch),
```

- [ ] **Step 2: Rename call sites**

Run: `grep -rln 'RegisterExtent' --include='*.rs'`

Rename. Also rename the `VariableHeader::RegisterExtent` variant in `components/rpc/src/frame/header.rs` to `RegisterEpoch`, plus its encode/decode arms and any payload helper.

- [ ] **Step 3: Function names**

Rename `handle_register_extent` → `handle_register_epoch` in `components/extent-node/src/store/register.rs`.

- [ ] **Step 4: Build and test**

Run: `cargo test --workspace 2>&1 | tail -10`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor(opcodes): rename RegisterExtent to RegisterEpoch (0x15 unchanged)"
```

### Task 4.3: `ForwardInitExtent` → `ForwardInitEpoch`

**Files:**
- Modify: `components/rpc/src/frame/header.rs`, encode.rs, decode.rs, then call sites

- [ ] **Step 1: Rename the `VariableHeader` variant**

In `components/rpc/src/frame/header.rs`, rename the `ForwardInitExtent { ... }` variant to `ForwardInitEpoch { ... }`. Fields stay the same (capacity fields were dropped in phase 1).

- [ ] **Step 2: Rename encoder and decoder arms**

In `encode.rs` and `decode.rs`, rename the match arms.

- [ ] **Step 3: Rename flag constant**

In `components/common/src/types.rs` line ~29: `FLAG_FORWARD_INIT_EXTENT` → `FLAG_FORWARD_INIT_EPOCH`.

- [ ] **Step 4: Rename call sites**

Run: `grep -rln 'ForwardInitExtent\|FLAG_FORWARD_INIT_EXTENT\|maybe_build_init_forward\|handle_forward_init_extent' --include='*.rs'`

Rename in each file. Also rename:
- `maybe_build_init_forward` / `handle_forward_init_extent` → `maybe_build_init_epoch_forward` / `handle_forward_init_epoch` (or similar; pick names and apply consistently) in `components/extent-node/src/store/forward.rs`.

- [ ] **Step 5: Build and test**

Run: `cargo test --workspace 2>&1 | tail -10`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor(opcodes): rename ForwardInitExtent to ForwardInitEpoch (Forward flag 0x01 unchanged)"
```

---

## Phase 5: Remove `byte_pos` from `Forward` Wire Format

Goal: `Forward` frames no longer carry `byte_pos`. Secondaries derive their local byte_pos from strict-order append.

### Task 5.1: Verify strict-order append invariant in secondaries

**Files:**
- Read: `components/extent-node/src/store/forward.rs`

- [ ] **Step 1: Confirm invariant**

Read `handle_forward` in `components/extent-node/src/store/forward.rs`. Confirm the secondary writes records in order received, and a running `cursor` on the extent's arena advances as each record is memcpied. The incoming `byte_pos` value from the frame is used (today) to either validate against the secondary's own cursor or to place at that exact position.

Note: if it's used for validation only, removal is trivial. If it's used for placement (e.g., secondary writes at `byte_pos` ignoring its own cursor), we need to verify that strict-order replay produces the same byte positions as the Primary. In the current implementation, Forward frames are delivered over TCP in order per-connection, so strict order is preserved — placement by `byte_pos` and placement by running cursor are equivalent.

No code change. No commit.

### Task 5.2: Remove `byte_pos` from `Forward` variant

**Files:**
- Modify: `components/rpc/src/frame/header.rs`
- Modify: `components/rpc/src/frame/encode.rs`
- Modify: `components/rpc/src/frame/decode.rs`

- [ ] **Step 1: Update the variant**

In `components/rpc/src/frame/header.rs` ~line 261:

Before:
```rust
Forward {
    stream_id: StreamId,
    extent_id: ExtentId,
    epoch: Epoch,
    offset: Offset,
    byte_pos: u64,
},
```

After:
```rust
Forward {
    stream_id: StreamId,
    extent_id: ExtentId,
    epoch: Epoch,
    offset: Offset,
},
```

Update the doc comment to drop the byte_pos sentence.

- [ ] **Step 2: Update the encoder**

In `components/rpc/src/frame/encode.rs`, find `Forward` encoding. Remove the 8 bytes for `byte_pos` from:
- Size calculation
- Write body

- [ ] **Step 3: Update the decoder**

In `components/rpc/src/frame/decode.rs`, remove the read of `byte_pos`.

- [ ] **Step 4: Verify rpc**

Run: `cargo check -p rpc`
Expected: PASS.

### Task 5.3: Update secondary Forward handler to derive byte_pos locally

**Files:**
- Modify: `components/extent-node/src/store/forward.rs`

- [ ] **Step 1: Remove byte_pos plumbing**

In `handle_forward` (now possibly renamed), remove the `byte_pos` field from the destructure of the `VariableHeader::Forward` frame. Replace any "write at byte_pos" logic with "write at current cursor" (which is the extent's `committed_bytes`).

If the old code had a sanity check like `debug_assert_eq!(byte_pos, cursor)`, remove it (the frame no longer carries byte_pos).

- [ ] **Step 2: Update Primary's Forward sender**

In `components/extent-node/src/store/append.rs`, wherever the Primary constructs `VariableHeader::Forward`, remove the `byte_pos` argument.

- [ ] **Step 3: Build and test**

Run: `cargo test --workspace 2>&1 | tail -20`
Expected: all PASS.

If any test constructed `Forward { ..., byte_pos: X }`, update to drop the field.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "$(cat <<'EOF'
refactor(wire): remove byte_pos from Forward frames

Secondaries derive local byte_pos from strict-order append — the
value was redundant since TCP preserves per-connection ordering and
each record advances the secondary's cursor by exactly
len_prefix + payload.

Saves 8 bytes per forwarded record on the wire.
EOF
)"
```

---

## Phase 6: Rename `extent` Table to `stream_epochs`

Goal: Final DB and metadata-layer rename. The `extent_id` **column** on the table is preserved; the code still uses it as a per-row identifier. Removing the column itself is deferred to a later plan (P2 when arena-level identity replaces extent-level identity in the EN code, or P3 when `ArenaClass` is introduced and `(stream_id, epoch)` fully replaces the per-row `extent_id`).

### Task 6.1: Write the migration

**Files:**
- Create: `components/stream-manager/migrations/V8__rename_extent_to_stream_epochs.sql`
  (check next `V` number — should be V8 after V7 from phase 1)

- [ ] **Step 1: Check migration numbering**

Run: `ls components/stream-manager/migrations/`

- [ ] **Step 2: Write the migration**

Create `components/stream-manager/migrations/V8__rename_extent_to_stream_epochs.sql`:

```sql
-- Rename `extent` table to `stream_epochs` per shared-arena spec P1.
-- Column schema unchanged; only the table identifier changes.
ALTER TABLE extent RENAME TO stream_epochs;
```

- [ ] **Step 3: Verify migration file syntax**

Run: `cat components/stream-manager/migrations/V8__rename_extent_to_stream_epochs.sql`
Expected: file exists, one ALTER TABLE statement.

### Task 6.2: Update all SQL strings in code

**Files:**
- Modify: `components/stream-manager/src/metadata.rs`

- [ ] **Step 1: Find all `FROM extent` / `INTO extent` / `UPDATE extent`**

Run:
```bash
grep -n 'FROM extent\|INTO extent\|UPDATE extent\|extent SET\|JOIN extent\|extent WHERE\|DELETE FROM extent\b' components/stream-manager/src/*.rs
```

- [ ] **Step 2: Rename in every SQL string**

Replace `extent` table reference with `stream_epochs` in each query. Be careful not to rename `extent_id` columns — only the table name.

- [ ] **Step 3: Rename the `ExtentRow` struct if desired**

Optional: rename `ExtentRow` struct to `StreamEpochRow` in `components/stream-manager/src/metadata.rs` for internal consistency. If done, update call sites across `store.rs`, `allocator.rs`, and any other files.

- [ ] **Step 4: Build and test**

Run: `cargo test --workspace 2>&1 | tail -20`
Expected: all PASS.

Note: integration tests that spin up a real MySQL will run the migration and see the rename; they should still pass because SQL strings match the new table name.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "$(cat <<'EOF'
refactor(sm): rename `extent` table to `stream_epochs`

Table column schema is unchanged; only the table identifier is renamed.
V8 migration does ALTER TABLE ... RENAME TO. All SQL strings in
metadata.rs are updated to reference stream_epochs.

This aligns the DB layer with the shared-arena spec where
(stream_id, epoch) is the identity of a record span. Removing the
extent_id column itself is deferred to a later plan, when the EN
code no longer needs the per-row extent identifier.
EOF
)"
```

---

## Phase 7: Run the Full Test Suite and Final Validation

### Task 7.1: Full test run

- [ ] **Step 1: Run all tests**

Run: `cargo test --workspace 2>&1 | tail -20`
Expected: all PASS.

- [ ] **Step 2: Run integration tests specifically**

Run: `cargo test --workspace --test '*' 2>&1 | tail -30`
Expected: all integration tests PASS (they spin up real MySQL + MinIO via testcontainers).

- [ ] **Step 3: Verify no stale references**

Run:
```bash
grep -rn 'SealStreamManager\|SealExtentNode\|RegisterExtent\|ForwardInitExtent\|FLAG_FORWARD_INIT_EXTENT\|UpdateExtentSealed\|NOTIFY_SEALED_EXTENT\|min_extent_capacity\|max_extent_capacity\|extent_growth_factor\|DEFAULT_MIN_EXTENT_CAPACITY\|DEFAULT_MAX_EXTENT_CAPACITY\|DEFAULT_EXTENT_GROWTH_FACTOR' --include='*.rs' --include='*.sql'
```
Expected: **no output.** Every identifier has been renamed or removed.

### Task 7.2: Final commit and PR

- [ ] **Step 1: Review commit log**

Run: `git log --oneline origin/main..HEAD`
Expected: roughly 7–9 commits, one per major task group.

- [ ] **Step 2: Push and open PR**

Run:
```bash
git push -u origin $(git branch --show-current)
gh pr create --title "P1: Collapse Extent into Epoch (shared-arena prereq)" --body "$(cat <<'EOF'
## Summary
Implements phase P1 of the shared-arena refactor per
docs/superpowers/specs/2026-04-24-shared-arena-design.md.

This is a mechanical refactor that prepares the codebase for the
shared-arena feature (P2–P5) by removing concepts that are obsolete
in the post-refactor design.

## Changes
- Removed adaptive extent capacity (min/max capacity, growth factor,
  idle-shrink timer). `extent_capacity` is now a single fixed config
  value, default 256 MiB.
- Removed `UpdateExtentSealed` EN→SM notification (fire-and-forget on
  autonomous seal-and-create). EN still seals locally on extent full;
  P2 replaces this with arena rolling.
- Removed `byte_pos` from the `Forward` wire frame. Secondaries derive
  local byte_pos from strict-order append.
- Renamed opcodes (wire bytes unchanged):
  - `SealStreamManager` → `SealStream` (0x06)
  - `SealExtentNode` → `SealEpoch` (0x07)
  - `RegisterExtent` → `RegisterEpoch` (0x15)
  - `ForwardInitExtent` variant → `ForwardInitEpoch` (Forward flag 0x01)
- Renamed MySQL table `extent` → `stream_epochs`. Column schema
  unchanged. `extent_id` column itself is removed in P2.

## Test plan
- [x] `cargo test --workspace` passes
- [x] Integration tests (MySQL + MinIO) pass
- [x] No references to removed identifiers remain
- [ ] Reviewer confirms wire format changes do not break any
  existing deployments (N/A — pre-production)
EOF
)"
```

---

## Self-Review Checklist (for the plan author)

Run this after writing the plan, not during implementation:

**1. Spec coverage:**
- [x] Epoch-collapse terminology (`extent_id` → `epoch` as identity): phases 3–6
- [x] Remove EN-initiated seal: phase 2
- [x] Remove `byte_pos` from Forward: phase 5
- [x] Remove adaptive extent capacity: phase 1
- [x] Rename MySQL `extents` → `stream_epochs`: phase 6
- [ ] Removal of `ExtentId` column from the table — **deferred to P2** (noted in phase 6)
- [ ] `ArenaClass` addition — out of scope (P3)
- [ ] Shared arena pool — out of scope (P2, P3)

**2. Placeholder scan:** No TBDs, TODOs, or "similar to" references. Every step has exact code or commands.

**3. Type consistency:**
- `extent_capacity` (singular) used consistently after phase 1.
- Opcode names consistent between `types.rs`, `frame/header.rs`, call sites.

**4. Broken-build window:**
- Task 1.2 intentionally breaks the build; the break is closed by end of Task 1.8 (same phase).
- Every other task leaves tree green.

**5. Ambiguity check:**
- Task 5.1 has an inline note explaining the strict-order invariant justifying `byte_pos` removal.
- Task 6.1 is explicit that column-level removal is deferred.
