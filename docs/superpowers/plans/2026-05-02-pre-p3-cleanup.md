# Codebase Cleanup (pre-P3) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Collapse the identity model from `(stream_id, epoch, extent_id)` to `(stream_id, epoch)`, finish the P1/P2 "Extent → StreamEpoch" rename that was partially deferred, shrink the MySQL migration history to a single baseline, and land all outstanding dead-code / clippy wins before P3 begins. Development phase, no backward-compatibility story — every identifier, wire frame, SQL column, and SQL table changes in place.

**Why drop ExtentId:** P1 removed autonomous extent creation within an epoch — each epoch now has exactly one extent. `extent_id` is therefore a redundant alias for `epoch`: two names for the same object, threaded through 23 wire frames, every error, every SQL query, every client call, and a whole MySQL side table (`stream_sequence`) that exists solely to mint it. Removing it is a big one-shot surgery but after it lands the entire codebase reads as *one stream, many epochs, one extent per epoch*.

**Non-goals:** no new features, no performance work, no new tests beyond what's needed to keep existing coverage passing after the rename/removal.

---

## Rename principle (not a mechanical sed)

"Extent" is overloaded in the current codebase. Three distinct concepts hide behind one word; each renames to a different thing.

| Concept | What it is | Renames to |
|---|---|---|
| **The per-stream sealed segment** (from a client's point of view: "the Nth epoch of this stream") | A row in `stream_epochs`, a `StreamEpoch` in memory, an S3 key | `Epoch` / `StreamEpoch` |
| **The byte-pool being written into** (what holds the bytes, appends records, flushes to S3) | `ArenaBuffer` + its directory; per-stream in Dedicated, shared across streams in P3 | `Arena` |
| **A count/collection of the above** | Method arity | Stay plural only when returning many (`get_epochs`); go singular when reporting one current value (`report_epoch`) |

When in doubt, ask: *if a reader sees this name after P3 lands (shared arenas, one arena backing multiple streams), does the name still describe what happens?* If the operation is about bytes being written/flushed/encoded, it's `Arena`. If it's about "which sealed chunk of this stream's history", it's `Epoch`. If it's a method describing the stream's current state, it's singular.

This means the rename table below is **not** a global s/extent/epoch/; some entries become `arena`, and some stay plural.

---

## Identity collapse (authoritative)

**Before:** a stream-epoch is addressed by `(stream_id, epoch, extent_id)`. `ExtentId(u32)` is a u32 newtype minted by a per-stream `stream_sequence.next_extent_id` counter inside the Stream Manager.

**After:** a stream-epoch is addressed by `(stream_id, epoch)`. The `Epoch(u32)` counter already exists and is already monotonic per stream. The Stream Manager allocates a new epoch by bumping `stream.epoch` and inserting a `stream_epochs` row keyed on `(stream_id, epoch)`.

**Direct consequences:**
- `common::types::ExtentId` newtype **deleted**.
- Every `extent_id: ExtentId` field on a `VariableHeader` variant **deleted**. 23 variants carry it today (see exhaustive list in Phase 1).
- `stream_sequence` MySQL table **deleted** (it exists only to mint `extent_id`).
- `stream_epochs.extent_id` column **deleted**; primary key becomes `(stream_id, epoch)`.
- `SealEpochPrepare.extent_id_from` **deleted** — the "from epoch" is the sealing epoch itself.
- `Stream::with_extent(extent_id, F)` → `Stream::with_epoch(epoch, F)` looking up by `Epoch`.
- `Stream::create_next_extent` **deleted** — per-epoch autonomous create is gone. Extent-full now escalates to SM (or panics on Primary today — see § "Extent-full behaviour" below for the decision).
- `Stream::seal_and_create_next` **deleted** — same reason. Callers in `store/append.rs` stop auto-creating a successor within the same epoch; instead the leader simply seals and returns `ExtentFull` up to the client, which re-opens the stream (SM bumps the epoch and re-registers).
- `SealNotification.new_extent_id` / `new_extent_capacity` **deleted** — there is no "new extent" in the autonomous path anymore.
- `StreamEpoch::can_recycle` + `StreamEpoch::reset` **deleted** (they supported the autonomous recycle path).

### Extent-full behaviour after removal

Today `store/append.rs` catches `StorageError::ExtentFull`, calls `seal_and_create`, and transparently advances the primary to a fresh extent in the same epoch. After this cleanup:

- `try_append_active` returns `ExtentFull` up to the caller as a normal error.
- `store/append.rs` **stops** calling `seal_and_create` on `ExtentFull`. It seals the epoch (via the existing `seal_and_create` codepath, which we now rename `seal_current_epoch` and strip the "create next" half from), surfaces the error, and the client is expected to re-open the stream to get a fresh `(epoch, addr)` tuple from SM.
- This is the correct long-term model for P3 (the shared-arena future) because "primary silently rotates the extent" is incompatible with many-streams-per-arena routing. We're paying the re-open cost now instead of writing new code we'll delete in P3.
- Integration tests that relied on the auto-create behaviour (seal-under-load, ExtentFull-on-append) lose their "and then the primary keeps writing" assertion. They're updated to assert `ExtentFull` is returned and the stream is stuck until the client re-opens. Three tests affected: `phase2_integration::extent_full_rotation`, `stream::tests::evict_via_create_next_extent`, `stream::tests::extent_full_backpressure`. The first is renamed + rewritten. The latter two are deleted outright (they only exercise the autonomous-create path).

---

## Rename Table (authoritative)

| Category | Before | After |
|---|---|---|
| u32 row identity type | `ExtentId(u32)` | **deleted** — identity is `(StreamId, Epoch)` |
| State enum | `ExtentState::{Unspecified, Active, Sealed, Flushed}` | `EpochState::{Unspecified, Active, Sealed, Flushed}` |
| SM metadata row | `ExtentRow` | `StreamEpochRow` |
| Public query result | `ExtentInfo { extent_id: u32, ... }` | `StreamEpochInfo { ... }` (no id field) |
| EN→SM notification | `ExtentUpdate::{Progress, Flushed}` | `StreamEpochUpdate::{Progress, Flushed}` |
| Errors | `ExtentFullSnafu { extent_id }` | `EpochFullSnafu { stream_id, epoch }` |
| Errors | `ExtentSealedSnafu { extent_id }` | `EpochSealedSnafu { stream_id, epoch }` |
| Stale-row struct | `StaleExtentRow` | `StaleEpochRow` |
| Policy struct | `ExtentPolicy { cache }` | `EpochPolicy { cache }` |
| Flag constants | `FLAG_EXTENT_PROGRESS`, `FLAG_EXTENT_FLUSHED` | `FLAG_EPOCH_PROGRESS`, `FLAG_EPOCH_FLUSHED` |
| Seal reason | `SealReason::ExtentFull` | `SealReason::EpochFull` |
| Stream method | `Stream::register_extent` | `Stream::register_epoch` |
| Stream method | `Stream::register_extent_simple` (test) | `Stream::register_epoch_simple` |
| Stream method | `Stream::active_extent_id` | `Stream::active_epoch` (returns `Option<Epoch>`) — no "_id" suffix; an epoch *is* its own id |
| Stream method | `Stream::active_extent_at_epoch` | **deleted** (tautological after the collapse — "the active epoch at epoch E" collapses to "is E active?") |
| Stream method | `Stream::last_sealed_extent_at_epoch` | `Stream::sealed_epoch(epoch) -> Option<(Offset, Offset)>` — returns the `(start_offset, end_offset)` of the given epoch if it's sealed, `None` otherwise. The old name (`last_sealed_..._at_epoch`) encoded the autonomous-create reality: "at this epoch, which was the latest sealed extent?" — a lookup that no longer has any ambiguity because each epoch owns one row. |
| Stream method | `Stream::create_next_extent` | **deleted** |
| Stream method | `Stream::seal_and_create_next` | renamed `Stream::seal_current_epoch` and body rewritten — it only seals, does not create a successor |
| Stream method | `Stream::with_extent(extent_id, F)` | `Stream::with_epoch(epoch, F)` |
| Stream method | `Stream::report_extents` | `Stream::report_epoch` — **singular**; returns the stream's current epoch and offset, not a collection |
| Stream method | `Stream::sealed_end_offset(extent_id)` | `Stream::sealed_end_offset(epoch)` — same shape, param type changes |
| Stream field | `StreamInner.next_extent_id` | **deleted** |
| Stream field | `StreamInner.extent_capacity` | `StreamInner.epoch_capacity` |
| Stream field | `StreamInner.max_extents` | `StreamInner.max_epochs` |
| StreamEpoch field | `StreamEpoch.id: ExtentId` | **deleted** — `StreamEpoch.epoch` already identifies it |
| StreamEpoch ctor | `StreamEpoch::with_capacity(id, start, cap, epoch, arena)` | `StreamEpoch::with_capacity(start, cap, epoch, arena)` |
| StreamEpoch method | `StreamEpoch::can_recycle`, `StreamEpoch::reset` | **deleted** |
| SealNotification | `{ sealed_extent_id, end_offset, new_extent_id, epoch, new_extent_capacity }` | `{ sealed_epoch, end_offset }` |
| SM method | `MetadataStore::allocate_extent` | `MetadataStore::allocate_epoch_row` (no longer touches `stream_sequence`) |
| SM method | `MetadataStore::seal_extent` | `MetadataStore::seal_epoch_row` |
| SM method | `MetadataStore::record_extent_flushed` | `MetadataStore::record_epoch_flushed` |
| SM method | `MetadataStore::get_extents` | `MetadataStore::get_epochs` |
| SM method | `MetadataStore::get_stream_with_active_extents` | `MetadataStore::get_streams_with_open_epochs` |
| S3 codec fn | `s3_codec::encode_extent` | `s3_codec::encode_arena` — encodes one arena's sealed bytes (header + chunk index + compressed data) into an S3 object. The thing on disk *is* the arena's serialized form, not the epoch's abstract identity. In P3 this name becomes load-bearing: one `encode_arena` call maps to one S3 object regardless of how many streams share the arena. |
| S3 codec fn | `s3_codec::encode_extent_range` | `s3_codec::encode_arena_range` — same reasoning; the `_range` half is about byte-range truncation (DR path), which is an arena-level concept |
| S3 codec const | `S3_EXTENT_HEADER_SIZE`, `S3_EXTENT_MAGIC`, `S3_EXTENT_VERSION`, `S3ExtentHeader` | `S3_ARENA_HEADER_SIZE`, `S3_ARENA_MAGIC`, `S3_ARENA_VERSION`, `S3ArenaHeader` — the on-disk header is an arena-level header; renaming it now aligns with P3's multistream S3 format |
| Config default | `DEFAULT_EXTENT_CAPACITY` | `DEFAULT_EPOCH_CAPACITY` |
| Config default | `DEFAULT_CACHE_EXTENTS` | `DEFAULT_CACHE_EPOCHS` |
| MySQL table | `stream_epochs` | unchanged (already renamed in P1 V6) |
| MySQL column | `stream_epochs.extent_id` | **dropped** |
| MySQL PK | `PRIMARY KEY (stream_id, extent_id)` | `PRIMARY KEY (stream_id, epoch)` |
| MySQL column | `stream.cache_extents` | `stream.cache_epochs` |
| MySQL table | `stream_sequence` (all columns) | **dropped** |
| Integration test file | `tests/record_extent_flushed_integration.rs` | `tests/record_epoch_flushed_integration.rs` |

### Identifiers that stay (not renamed)
- `ExtentNode` (the node type / binary / crate) — it's the product name; renaming breaks the `components/extent-node/` crate path, binary name, and every log message that refers to "ExtentNode".
- `ExtentNodeStore`, `ExtentNodeConfig` — same reasoning.
- Wire-level `Opcode` enum variants (`Connect`, `SealEpoch`, `RegisterEpoch`, etc.) — already epoch-named in P1.
- `extent-node` crate directory.
- Log messages containing the string "extent" as informational text — leave.

---

## Phase 0: Inventory + Baseline

Record baseline test count (144) and grep counts for every renamed identifier. No commit.

```
# Target numbers after this cleanup lands:
cargo test --lib --workspace                      # 144+ pass
grep -rn 'ExtentId\b' --include='*.rs' | wc -l    # 0
grep -rn 'extent_id' --include='*.rs' | wc -l     # 0
grep -rn 'extent_id' --include='*.sql' | wc -l    # 0
grep -rn 'stream_sequence' | wc -l                # 0
grep -rn 'create_next_extent\|seal_and_create_next\|seal_and_create\b' | wc -l  # 0 (all renamed/deleted)
```

---

## Phase 1: Wire protocol — drop extent_id from VariableHeader

This is the most mechanical, most surface-area-heavy phase. It must land first because every other phase depends on the type signatures being clean.

**The 23 variants to modify** (in `components/rpc/src/frame/header.rs`):

Remove `extent_id: ExtentId` from:
1. `AppendAck`
2. `AppendAckError`
3. `Read`
4. `ReadRespError`
5. `SealEpochResp`
6. `SealEpochCommit`
7. `SealEpochCommitResp`
8. `CreateStreamResp`
9. `RegisterEpoch`
10. `RegisterEpochAck`
11. `RegisterExtentAckError`
12. `Watermark`
13. `UpdateExtentProgress`
14. `UpdateExtentFlushed`
15. `Forward`
16. `ForwardInitEpoch`
17. `ForwardChecksum`
18. `ForwardFlushed`
19. `FlushExtent`
20. `FlushExtentResp`
21. `FlushExtentRespError`
22. `DescribeExtent`
23. `DescribeExtentRespError`

Also remove `extent_id_from: ExtentId` from `SealEpochPrepare` (that's variant #24, and the fact that the field is confusingly-named `extent_id_from` not `extent_id` is itself a smell that goes away now).

**Files:**
- `components/rpc/src/frame/header.rs` — 24 struct updates
- `components/rpc/src/frame/encode.rs` — drop the 4-byte write for each variant
- `components/rpc/src/frame/decode.rs` — drop the 4-byte read for each variant
- `components/rpc/src/frame/tests.rs` — every round-trip test loses one field per relevant variant; byte offsets in the `unknown_*_round_trip` tests shift by -4 per dropped field
- `components/rpc/src/frame/error_constructors.rs` — drop `extent_id` arg from each error builder
- `components/rpc/src/frame/mod.rs` + `codec.rs` — Opcode classification may rely on variant destructuring, check after the header.rs change

**Verification:** `cargo check -p rpc` clean; `cargo test -p rpc` all pass (the frame round-trip suite is the wire-format regression harness).

**Risk:** every EN↔SM↔client roundtrip changes shape on the wire. Pre-production, single repo, no compatibility story — all three peers update atomically in-repo.

**Commit 1:** `refactor(rpc): drop extent_id from 24 VariableHeader variants — identity is (stream_id, epoch)`

### Opcode + variant renames (decided)

After dropping the `extent_id` *field*, several variant and opcode *names* still say "Extent". The post-P1 identity rule (each stream has exactly one active epoch; the word "Extents" plural was a holdover from autonomous creation) collapses every one of these to singular `Epoch`:

| Before | After | Why |
|---|---|---|
| `Opcode::UpdateExtent` | `Opcode::UpdateEpoch` | Notification about a stream's sealed segment |
| `VariableHeader::UpdateExtentProgress` | `UpdateEpochProgress` | " |
| `VariableHeader::UpdateExtentFlushed` | `UpdateEpochFlushed` | " |
| `Opcode::ReportExtents` | `Opcode::ReportEpoch` | **Singular.** A stream has exactly one active epoch; the plural `Extents` only existed because autonomous creation let a stream hold many live extents at once — that path is gone |
| `VariableHeader::ReportExtents` | `ReportEpoch` | " |
| `VariableHeader::ReportExtentsResp` | `ReportEpochResp` | " |
| `VariableHeader::ReportExtentsRespError` | `ReportEpochRespError` | " |
| `Opcode::FlushExtent` | `Opcode::FlushEpoch` | Fallback-seal path: SM commands EN to flush the remaining in-memory records of a specific epoch to S3 ASAP. Addressing is epoch-level; the arena-level byte encoding is behind the command |
| `VariableHeader::FlushExtent` | `FlushEpoch` | " |
| `VariableHeader::FlushExtentResp` | `FlushEpochResp` | " |
| `VariableHeader::FlushExtentRespError` | `FlushEpochRespError` | " |
| `Opcode::DescribeExtent` | `Opcode::DescribeEpoch` | Client asks for one sealed segment's metadata |
| `VariableHeader::DescribeExtent` | `DescribeEpoch` | " |
| `VariableHeader::DescribeExtentResp` | `DescribeEpochResp` | " |
| `VariableHeader::DescribeExtentRespError` | `DescribeEpochRespError` | " |
| `VariableHeader::RegisterExtentAckError` | `RegisterEpochAckError` | Already the error reply for a `RegisterEpoch` request; name was misaligned |
| `VariableHeader::ForwardInitEpoch` | unchanged | Already epoch-named |
| `VariableHeader::ForwardFlushed` | unchanged | Already neutral |

Phase 1's commit now covers both the `extent_id` field drop *and* these variant/opcode renames — they're the same code churn (one pass through `header.rs` + `encode.rs` + `decode.rs` + `tests.rs`). Phase 5 picks up the corresponding `FLAG_EXTENT_*` constant renames as part of the paired naming.

---

## Phase 2: common types — delete ExtentId newtype, rename ExtentState/ExtentInfo/ExtentPolicy

With the wire types clean, `ExtentId` has no more users outside the EN + SM crates. Delete it.

**Subphase 2.1: Delete `ExtentId`**
- Remove `pub struct ExtentId(pub u32);` from `components/common/src/types.rs`.
- Remove its `Display` impl (line 65).
- Remove `extent_id: u32` from `ExtentInfo` (line 357).
- Fix every call site: anywhere that was `.bind(extent_id.0)` on a SQL query becomes `.bind(epoch.0)`, and so on.

**Subphase 2.2: `ExtentState` → `EpochState`**
- Rename the enum + `Display` + `FromStr` + `TryFrom` impls.
- Update every import (~25 files).
- Update pattern matches.

**Subphase 2.3: `ExtentInfo` → `StreamEpochInfo`**
- The struct loses `extent_id`, keeps `stream_id`, `start_offset`, `end_offset`, `state`, `epoch`, etc.

**Subphase 2.4: `ExtentFullSnafu` / `ExtentSealedSnafu` → `EpochFullSnafu` / `EpochSealedSnafu`**
- Payload changes from `{ extent_id: ExtentId }` to `{ stream_id: StreamId, epoch: Epoch }`.

**Subphase 2.5: `ExtentPolicy` → `EpochPolicy`**
- Config default renames: `DEFAULT_EXTENT_CAPACITY` → `DEFAULT_EPOCH_CAPACITY`, `DEFAULT_CACHE_EXTENTS` → `DEFAULT_CACHE_EPOCHS`.

**Commits 2–6:** one commit per subphase, each green-builds and green-tests on its own.

---

## Phase 3: Stream Manager — drop stream_sequence, rekey stream_epochs

**Subphase 3.1: MySQL migration collapse to V1__baseline.sql**

Delete V1..V7, write a single baseline. The baseline captures the post-cleanup schema:
- `stream`: `stream_id`, `stream_name`, `replication_factor`, `cache_epochs`, `storage_class`, `arena_class`, `epoch`, `created_at`, `updated_at`
- `stream_epochs`: `stream_id`, `epoch`, `start_offset`, `end_offset`, `state`, `s3_key`, `arena_class`, `created_at`, `sealed_at`, `flushed_at`, PK `(stream_id, epoch)`
- `stream_replica`: unchanged shape
- `node`, `node_metrics`, `stream_manager_leadership`: unchanged
- **No `stream_sequence` table.**

Files to delete: `V1__create_stream_table.sql`, `V2__create_extent_table.sql`, `V3__create_node_table.sql`, `V4__create_node_metrics_and_leadership.sql`, `V5__drop_adaptive_capacity.sql`, `V6__rename_extent_to_stream_epochs.sql`, `V7__add_arena_class.sql`

File to create: `V1__baseline.sql`.

Integration test setup files (`tests/*.rs`, `benches/*.rs`, `examples/client-example.rs`) each carry a table-drop list used during test setup. Remove `"stream_sequence"` from each list (10 files, one line each).

**Risk:** if someone has a MySQL database with V1..V7 applied, applying this collapsed migration after will fail because refinery's schema-history disagrees. Pre-production; every test run drops tables first.

**Commit 7:** `chore(sm): collapse migrations to V1__baseline; drop stream_sequence, rekey stream_epochs on (stream_id, epoch)`

**Subphase 3.2: MetadataStore method renames + body rewrites**
- `allocate_extent` → `allocate_epoch_row`. Body no longer touches `stream_sequence`; it bumps `stream.epoch` and inserts a row keyed `(stream_id, epoch)`. Return type changes from `Result<ExtentId, StorageError>` to `Result<Epoch, StorageError>`.
- `seal_extent` → `seal_epoch_row`. Body no longer mints a successor extent_id; it updates `state=Sealed` and returns a `SealResult` variant that drops the `new_extent_id` field.
- `record_extent_flushed` → `record_epoch_flushed`.
- `get_extents` → `get_epochs`.
- `get_stream_with_active_extents` → `get_streams_with_open_epochs`.
- Drop the "update stream_sequence to max(extent_id+1)" branch in the reconciliation path (line ~1548) — this table no longer exists.

**Commit 8:** `refactor(sm): rename MetadataStore::*_extent → *_epoch; drop stream_sequence minting`

---

## Phase 4: Extent Node runtime — delete autonomous create, rename Stream API

**Subphase 4.1: delete `Stream::create_next_extent`, `Stream::seal_and_create_next`, `StreamEpoch::can_recycle`, `StreamEpoch::reset`**
- `components/extent-node/src/stream.rs:677` (`create_next_extent`) — deleted.
- `components/extent-node/src/stream.rs:686` (`seal_and_create_next`) — deleted. Replaced by a new `seal_current_epoch(&self) -> Option<(Epoch, Offset)>` that seals the active epoch and returns `(sealed_epoch, end_offset)`; no successor created.
- `components/extent-node/src/stream_epoch.rs:276` (`can_recycle`) — deleted (also gets rid of a `#[allow(dead_code)]`).
- `components/extent-node/src/stream_epoch.rs:285` (`reset`) — deleted.
- Delete associated tests: `stream::tests::evict_via_create_next_extent` (line ~970), and any other test body that exercises `seal_and_create_next`'s create-successor behaviour. Keep tests that exercise *just* the seal half; rewrite to call the new `seal_current_epoch` and assert `(sealed_epoch, end_offset)`.

**Subphase 4.2: `store/append.rs` — stop auto-creating successor extents**
- Line 115, 453, 903: the three `seal_and_create(stream_id, SealReason::ExtentFull)` sites. Replace with `seal_current_epoch(stream_id)`. Remove the downstream `set_epoch` / `register` cascade that followed `seal_and_create`.
- On the Primary `try_append_active → Err(ExtentFull)` path (line ~239, ~758), surface the error to the caller instead of triggering `seal_and_create`. The client will reopen.
- Update `SealReason::ExtentFull` → `SealReason::EpochFull`.

**Subphase 4.3: `SealNotification` shrinks**
- Old: `{ sealed_extent_id, end_offset, new_extent_id, epoch, new_extent_capacity }`.
- New: `{ sealed_epoch: Epoch, end_offset: u64 }`.
- Update `set_downstream_txs` / SM notification emission to match.

**Subphase 4.4: Stream method + field renames**
- `register_extent` → `register_epoch`
- `register_extent_simple` (test helper) → `register_epoch_simple`
- `active_extent_id` → `active_epoch` (returns `Option<Epoch>`) — drop the `_id` suffix; an epoch *is* its own id
- `active_extent_at_epoch` — **deleted** (tautological; see rename table)
- `last_sealed_extent_at_epoch` → `sealed_epoch(epoch) -> Option<(Offset, Offset)>` — returns `(start_offset, end_offset)` if the epoch is sealed, `None` otherwise. Old return was `(ExtentId, u64, u64)`; new return drops the `ExtentId` component because the input *is* the identity.
- `with_extent(extent_id, F)` → `with_epoch(epoch, F)`
- `report_extents` → `report_epoch` — **singular**. Returns the stream's current epoch and offset (one tuple), not a collection
- `sealed_end_offset(extent_id)` → `sealed_end_offset(epoch)` — param type changes
- `StreamInner.next_extent_id` — **deleted** (nothing to mint)
- `StreamInner.extent_capacity` → `epoch_capacity`
- `StreamInner.max_extents` → `max_epochs` (**plural** — this is a bound on a collection)
- `StreamEpoch.id: ExtentId` — **deleted**; `StreamEpoch.epoch` already identifies it
- `StreamEpoch::with_capacity(id, start, cap, epoch, arena)` → `with_capacity(start, cap, epoch, arena)` — drop `id` param

**Subphase 4.5: S3 codec renames (`encode_extent` → `encode_arena`, not `encode_epoch`)**

Rationale lives in the rename table: what `encode_extent` produces is **one arena's serialized bytes as an S3 object**, not an abstract epoch identifier. Naming it `encode_arena` now aligns with P3's multistream S3 format (Shape A), where one S3 object per arena backs multiple streams.

- `s3_codec::encode_extent` → `s3_codec::encode_arena`
- `s3_codec::encode_extent_range` → `s3_codec::encode_arena_range`
- `S3_EXTENT_MAGIC` → `S3_ARENA_MAGIC`
- `S3_EXTENT_VERSION` → `S3_ARENA_VERSION`
- `S3_EXTENT_HEADER_SIZE` → `S3_ARENA_HEADER_SIZE`
- `S3ExtentHeader` → `S3ArenaHeader`
- Doc comments: "Encode a sealed extent into the S3 file format …" → "Encode an arena's sealed bytes into the S3 object format …". Sweep `s3_codec.rs` and `s3_flusher.rs` for the word "extent" in docs and replace with "arena" where it refers to the byte-pool, or "epoch" where it refers to the stream's sealed segment identity. (These two meanings co-exist in the same file today — do not mechanical-replace; read each comment.)

**Commits 9–13:** one commit per subphase.

---

## Phase 5: Flag constants + prose + local variables

- `FLAG_EXTENT_PROGRESS` → `FLAG_EPOCH_PROGRESS` (paired with `UpdateEpoch` opcode, if the Phase 1 open-question table is accepted as-is)
- `FLAG_EXTENT_FLUSHED` → `FLAG_EPOCH_FLUSHED` (same)
- If the `FlushExtent` opcode becomes `FlushArena` instead, revisit any flag it carries.
- Doc comments: sweep `components/extent-node/src/**/*.rs` and `components/rpc/src/**/*.rs` and re-read each comment containing "extent". Replace with `epoch` when the comment discusses the stream's sealed segment (identity, history), with `arena` when it discusses the byte-pool (writing, encoding, S3 upload), and leave alone when it describes the `ExtentNode` product/process.
- Local variables named `extent` / `ext` / `extent_id` → `epoch` / `ep` (for `&StreamEpoch` or `Epoch`), or `arena` (for `&ArenaBuffer` or `ArenaId`) — again, read each binding.

**Commit 14:** `refactor: sweep remaining extent-named locals + FLAG_EXTENT_* constants`

---

## Phase 6: Integration test file rename

- `tests/record_extent_flushed_integration.rs` → `tests/record_epoch_flushed_integration.rs`
- Update the test body to use `UpdateExtentFlushed`'s new (field-shrunk) shape and the renamed `record_epoch_flushed` SM method.
- Rewrite `tests/phase2_integration::extent_full_rotation` to `epoch_full_requires_reopen` — asserts `EpochFull` is surfaced and the client must reopen.

**Commit 15:** `test: rename + rewrite integration tests for epoch-only identity`

---

## Phase 7: Dead code + clippy cleanup

- Replace `impl Default for ArenaClass` with `#[derive(Default)]` + `#[default]` on `Dedicated` variant.
- Remove the two useless casts: `arena.ptr() as *const u8` → `arena.ptr()`; `i as u32` in tests where the LHS is already `u32`.
- Remove speculative `#[allow(dead_code)]` attributes:
  - `ArenaId::{node_prefix, counter}` — delete if no callers, or wire them into a log-fmt path.
  - `#[allow(dead_code)]` on `arena/write_batch.rs` — keep for P3 (the SharedArenaPool path activates these types); revisit at end of P3.

**Commit 16:** `chore: clippy fixes (Default derive, useless casts, dead-code attrs)`

---

## Phase 8: Validation + final sweep

- `RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets` — clean.
- `cargo test --lib --workspace` — 144+ pass.
- Final greps (from Phase 0 "target numbers") all return 0.

---

## Commit-count estimate

16 commits (plus a possible PR commit). Each green-builds and green-tests on its own.

---

## Rollout

Since this is a single deep pass with the wire-format shrink in Phase 1 gating everything else, run it **synchronously on one subagent per commit**, reviewing between each, rather than parallelizing. Expect Phase 1 (wire) and Phase 4 (EN runtime) to be the two hardest phases by a wide margin; budget extra review time on them.
