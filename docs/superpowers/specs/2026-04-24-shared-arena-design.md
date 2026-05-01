# Shared Arena for Multi-Stream Memory, and Collapsing Extent into Epoch

## Motivation

Two independent observations drive this change.

**(1) Per-stream memory floor.** Today each active extent owns a dedicated
arena: `Arc<ArenaBuffer>` allocated via `std::alloc::alloc_zeroed` plus a
`Box<[AtomicU32]>` index sized `capacity / 5`. At `min_extent_capacity = 8
MiB` that's ~8 MiB data + ~6.7 MiB index per minimum extent, and with one
recycled spare in the stream's extent pool a freshly registered stream costs
roughly **30 MiB of RAM**. For MQTT-scale workloads with ~1M low-traffic
queues per ExtentNode, that's ~30 TiB — unusable. We need memory to scale
with bytes-in-flight, not stream count.

**(2) Extent is redundant with Epoch.** The replica set for a stream is
stable within one epoch; it only changes when SM bumps the epoch on failure
or rebalance. Seal is only meaningful on epoch bump. Once arena lifetime is
decoupled from the record-span lifetime (Observation 1 forces a shared arena
to flush independently of any one stream's record span), every role played
by `Extent` is played by either **epoch** (replication target, replica set,
quorum tracking, seal) or **arena** (memory, S3 upload). The intermediate
`Extent` concept carries no information that `(stream_id, epoch)` does not
already carry.

This spec does two things at once:

- Introduces a per-stream `ArenaClass` (Shared vs Dedicated) so low-traffic
  streams multiplex onto shared arenas while high-throughput streams keep a
  private arena.
- Collapses the `Extent` concept into `Epoch` across the wire protocol, SM
  metadata, and EN code. A stream's record-span is identified by
  `(stream_id, epoch)`; there is no separate `extent_id`.

## Scope

This spec is implemented **in place**. The store is pre-production; schemas,
wire protocol, and internal interfaces are modified directly. There is no
dual-run, no feature flag, and no backwards-compatibility layer.

## Terminology

- **Epoch**: a (stream_id, epoch_number, replica_set) triple. Immutable
  replica set. Monotonically increasing epoch numbers within a stream. One
  epoch → one Primary + (RF-1) Secondaries.
- **Arena**: a contiguous in-memory buffer and the unit of S3 upload.
  Shared or Dedicated per the stream's `ArenaClass`.
- **ArenaClass** (per stream):
  - `Dedicated`: the stream has its own arena; one writer per stream
    (today's fast path).
  - `Shared`: records land in arenas from an EN-wide pool; one writer per
    arena, multiplexing records from many streams.
- **Seal**: the SM-driven 2-phase Prepare/Commit protocol, invoked **only
  on epoch bump** (failure / rebalance). There is no EN-initiated seal in
  either class.
- **Arena roll**: the Primary (for Shared) or the stream's writer (for
  Dedicated) closes the current arena and starts a new one. The closed
  arena transitions through `Rolled → Uploaded → Evicted`. Epoch is
  unaffected. "Seal" is reserved for epoch bumps and is **never** used
  for arena lifecycle events.

## What Changes vs Today

| Concept today | New model |
|---|---|
| `extent_id` on the wire, in DB, in code | Replaced by `epoch`. `(stream_id, epoch)` is the identity. |
| `extents` table | Renamed `stream_epochs`. Columns: `stream_id`, `epoch`, `start_offset`, `state`, replica metadata. |
| `extent_s3_objects` (proposed in earlier draft) | Replaced by `epoch_arenas`, keyed by `(stream_id, epoch, sequence)`, one row per arena this epoch wrote to. |
| `RegisterExtent` opcode | Renamed `RegisterEpoch`. Carries `arena_class`. |
| `ForwardInitExtent` opcode | Renamed `ForwardInitEpoch`. Carries `arena_class`. |
| `SealStreamManager` opcode (client → SM) | Renamed `SealStream`. Keyed by `(stream_id, current_epoch)`. Triggers the epoch bump + new replica set; replies with the new epoch and new Primary. |
| `SealExtentNode` opcode (SM → EN internal 2-phase) | Renamed `SealEpoch`. Same 2-phase Prepare/Commit against each replica of the sealing epoch. Invoked by SM while processing `SealStream` or during failover. |
| EN-initiated seal (`FLAG_OFFSET_PRESENT = 1`) | **Removed.** Arena-full never bumps the epoch; no EN-initiated seal path. |
| `NOTIFY_SEALED_EXTENT`, autonomous extent creation | **Removed.** Arena-full never triggers an epoch bump or SM round-trip. |
| `Forward` carrying `byte_pos` | Removed. Secondaries compute byte_pos locally from strict append order. |
| Per-extent arena + per-extent u32 index (Dedicated) | Per-epoch arena pool (Dedicated): one writer per stream, rolls arenas within an epoch. Per-arena directory (same as Shared). |
| S3 key `{ns}/data/{stream}/{start}_{end}.dat` | Two shapes coexist: Shape A (normal arena upload) keyed `{ns}/arenas/{arena_id:016x}.dat`; Shape B (DR per-stream upload) keyed `{ns}/data/{stream}/{start}_{end}.dat` (same as today, used for both Dedicated normal flush and any-class DR flush). |
| Adaptive per-stream extent capacity (`min/max_extent_capacity`, `extent_growth_factor`, idle-shrink) | **Removed.** `ArenaClass` solves the same tradeoff: Dedicated uses a fixed `dedicated_arena_size`, Shared uses the pool's `shared_arena_size`. Primary and Secondary size shared arenas independently. |

## Core Abstractions

### ArenaClass

```rust
enum ArenaClass {
    Dedicated = 0,  // one arena at a time belongs exclusively to this stream
    Shared    = 1,  // arenas multiplex records from many streams
}
```

Stored in the `streams` MySQL row. Propagated from SM to Primary via
`RegisterEpoch` (at epoch allocation). Primary propagates to Secondaries on
the first forwarded append for a new epoch via `ForwardInitEpoch`.

### Epoch (replaces Extent)

A `StreamEpoch` is a per-stream, per-epoch runtime object on every replica.
It owns what `Extent` owned, minus the arena:

```rust
struct StreamEpoch {
    stream_id:       StreamId,
    epoch:           u64,
    start_offset:    u64,                     // first offset this epoch will write
    replica_info:    ReplicaInfo,             // fixed for epoch lifetime
    state:           AtomicU8,                // Open | Sealing | Sealed
    limit:           AtomicU64,               // set at seal time
    record_count:    AtomicU64,
    committed_seq:   AtomicU64,
    committed_bytes: AtomicU64,               // epoch-relative cumulative bytes

    // Which arenas (shared or dedicated, on this EN) currently hold at
    // least one directory entry for this (stream, epoch). In append
    // order. Populated on first write into a new arena; entries are
    // removed as directory entries drop (Shape A upload, ForwardFlushed
    // release, or arena eviction).
    resident_arenas: Mutex<SmallVec<[ArenaId; 4]>>,

    // Reference count of live directory entries for this (stream, epoch)
    // across all resident arenas. When this hits zero, the owning
    // Stream removes the epoch from Stream::epochs and this StreamEpoch
    // is dropped (once outstanding readers release their Arc clones).
    directory_ref_count: AtomicU32,

    class:           ArenaClass,
}
```

`StreamEpoch` is a pure replication/consistency object. It owns no
concurrency primitives — the stream-level leader-election + pipelined-
group-commit state stays on `Stream` (below) and is used identically for
both classes.

```rust
struct Stream {
    stream_id:     StreamId,
    arena_class:   ArenaClass,
    storage_class: StorageClass,

    // All StreamEpochs this EN currently tracks for this stream:
    //   - the currently-open epoch (highest key), and
    //   - every sealed epoch that still has at least one resident arena
    //     directory entry on this EN.
    // An entry is inserted when SM registers a new epoch (RegisterEpoch)
    // and removed when the epoch's directory_ref_count reaches zero.
    // BTreeMap keeps the keys sorted so "latest epoch" is `iter().next_back()`.
    epochs: RwLock<BTreeMap<u64, Arc<StreamEpoch>>>,

    // Cache of the currently-open epoch for hot-path append. Invariant:
    // always equals the highest-numbered entry in `epochs`. Updated
    // atomically with `epochs` during epoch bump.
    active_epoch: ArcSwap<StreamEpoch>,

    ewma: EwmaStats,                          // for runtime class transitions

    // Pipelined group commit (same shape for both classes):
    in_flight: AtomicU64,
    job_tx:    crossbeam::channel::Sender<AppendJob>,
    job_rx:    crossbeam::channel::Receiver<AppendJob>,
}
```

### StreamEpoch Lifecycle

A `StreamEpoch` is tracked on an EN as long as its records are still
needed in memory:

1. **Birth**: on `RegisterEpoch` (SM → Primary) or on the first
   `Forward` for a new epoch (Secondary lazy init). The EN inserts
   `Arc::new(StreamEpoch { ... })` into `Stream::epochs`, then
   `active_epoch.store(it)`.
2. **Epoch bump**: `SealEpoch` Commit transitions the epoch to `Sealed`
   and sets `limit`. A subsequent `RegisterEpoch` for the new epoch
   inserts the new entry and updates `active_epoch`. **The sealed
   epoch remains in `epochs` until its records are no longer resident.**
3. **Directory-entry accounting**: every write that creates a new
   `EpochArenaEntry` for `(stream, epoch)` in some arena increments
   the epoch's `directory_ref_count`. Every release of such an entry
   (Shape A upload primary-cohort skip or include, `ForwardFlushed`
   full-range coverage, or arena eviction) decrements it.
4. **Death**: when `directory_ref_count` hits zero, the owning `Stream`
   removes the epoch from `epochs`. Any outstanding `Arc<StreamEpoch>`
   clones held by in-flight readers or flush tasks keep the struct
   alive until they release; the allocation is then freed.

This guarantees:

- **Shape A compaction** can always look up
  `StreamEpoch.replica_info.primary_node_id` for any directory entry
  it walks: the entry's existence implies the epoch is still in
  `Stream::epochs`.
- **`ForwardFlushed` release** on a secondary can always translate a
  flushed offset range into `byte_positions` indices via
  `StreamEpoch.start_offset`.
- **Reads** resolve against any still-resident epoch of the stream,
  not just the active one.
- **DR flush** (`FlushEpochStream(stream, epoch)`) can always locate
  the epoch's resident records while any are still in memory.

## SharedArenaPool

One pool per ExtentNode, owning all Shared arenas:

```rust
struct SharedArenaPool {
    active:   ArcSwap<SharedArena>,
    resident: RwLock<HashMap<ArenaId, Arc<SharedArena>>>,
    lru:      Mutex<IntrusiveLinkedList<ArenaId>>,
    cfg:      SharedArenaConfig,
    metrics:  Arc<Metrics>,
}

struct SharedArenaConfig {
    arena_size:                 u32,    // default 64 MiB
    max_resident_shared_arenas: u32,    // default 64 → 4 GiB shared budget
    writer_channel_capacity:    usize,  // default 4096
    directory_initial_capacity: usize,  // default 128
}
```

### SharedArena

The `SharedArena` struct is defined together with its concurrency
primitive in the [Arena Concurrency Primitive](#arena-concurrency-primitive)
section below. Its backing state for per-stream record placement lives in
the arena directory:

```rust
struct ArenaDirectory {
    // Keyed by (stream_id, epoch). Within one arena a stream may have at
    // most one entry per epoch; on epoch bump the stream continues writing
    // under a new (stream_id, epoch) directory entry in the same or a
    // subsequent arena.
    entries: HashMap<(StreamId, u64), EpochArenaEntry>,
}

struct EpochArenaEntry {
    // The entry's cohort (Primary/Secondary) is not stored here; it is
    // derived at flush time from StreamEpoch.replica_info.primary_node_id,
    // which is immutable within an epoch.
    start_offset:     u64,
    end_offset:       u64,        // exclusive, running while arena open
    byte_positions:   Vec<u32>,   // per record, within this arena
    arena_start_byte: u32,
    arena_end_byte:   u32,
}

struct SharedAppendJob {
    // One record submitted inside a WriteBatch from a stream leader.
    seq:     u64,
    payload: Bytes,
}
```

### ArenaId

`ArenaId = u64 = (node_id << 48) | local_counter`. Globally unique by
construction. S3 key `{namespace}/arenas/{arena_id:016x}.dat` does not
collide across ENs. 16 bits of node_id (65,535 ENs) and 48 bits of counter.

### Arena Concurrency Primitive

Both Shared and Dedicated arenas expose the same minimal write primitive —
a CAS-based leader election plus an MPSC for delegated batches. There is
**no background writer task**; whichever caller wins the CAS performs the
memcpy inline, then drains delegated work before yielding:

```rust
struct SharedArena {
    id:         ArenaId,
    buf:        Arc<ArenaBuffer>,
    state:      AtomicU8,                     // Open | Rolled | Uploaded | Evicted
    created_at: Instant,
    directory:  Mutex<ArenaDirectory>,

    // Arena-level leader election (CAS on entry):
    in_flight:  AtomicU64,
    job_tx:     crossbeam::channel::Sender<WriteBatch>,
    job_rx:     crossbeam::channel::Receiver<WriteBatch>,

    s3_key:     OnceLock<String>,
}

struct WriteBatch {
    // One batch from one stream leader; records are already in seq order.
    stream_id: StreamId,
    epoch:     u64,
    jobs:      SmallVec<[SharedAppendJob; 16]>,
    reply:     oneshot::Sender<WriteBatchAck>,
}

struct WriteBatchAck {
    // Per-job result, in the same order as WriteBatch.jobs.
    // Each record's resolved (arena_id, byte_pos) — records in a batch may
    // straddle an arena roll so arena_id is per-record.
    results: SmallVec<[JobResult; 16]>,
}

struct JobResult {
    arena_id: ArenaId,
    byte_pos: u32,
}
```

Dedicated arenas use the same shape, except the `in_flight` CAS is
uncontended by construction (only the owning stream's leader ever
submits), so it degenerates to a direct-memcpy fast path.

Per-(stream, epoch) grouping in the directory is trivial: each
`WriteBatch` belongs to exactly one `(stream_id, epoch)`, so the arena
leader drops the batch's records into one directory entry.

## Write Path

The write path is a **two-layer CAS-based leader election**:

| Layer | Scope | Outcome |
|---|---|---|
| Stream-level | One per `Stream` | Elects a single writer per stream, assembles batches, assigns `seq`, drives replication and ACK ordering |
| Arena-level | One per arena | Elects a single writer per arena, performs memcpy, updates the arena directory, handles arena roll |

Shared arenas exercise both layers. Dedicated arenas exercise the stream
layer and trivially "win" the arena-level CAS (the stream owns the arena
exclusively).

### Layer 1: Stream Leader Election (Both Classes)

Identical to today's Dedicated fast path:

```
prev = stream.in_flight.fetch_add(1, Acquire)
if prev > 0:
    // follower
    stream.job_tx.send(AppendJob { payload, client_reply_tx })
    return None
// leader turn
loop:
    own_batch = collect own payload
    drained   = drain_up_to(stream.job_rx, max_stream_batch)
    batch     = own_batch ++ drained
    for payload in batch: assign seq = ep.record_count.load() then ++
    write_batch(batch)                          // class-specific, Layer 2
    for payload in batch:
        if ep.replica_info.rf >= 2:
            forward(stream_id, epoch, seq, payload)
            ack_queue.push(PendingAck { seq, client_reply_tx })
        else:
            client_reply_tx.send(AppendAck { offset: ep.start_offset + seq })
    ep.committed_seq.store(last_seq_in_batch + 1, Release)
    remaining = stream.in_flight.fetch_sub(batch.len(), Release) - batch.len()
    if remaining == 0: break
```

Because only the stream leader ever touches `ep.record_count`, seq
assignment is contention-free (plain load/store). Submission order to
Layer 2 is strict seq order within the stream's turn, and across turns
is FIFO by virtue of `in_flight` gating.

### Layer 2: Arena Writer

`write_batch` is the class-specific step.

**Dedicated** — the stream owns the arena exclusively, so no arena-level
CAS is needed; the stream leader memcpies directly. The arena struct
still exists, but its `in_flight` / `job_tx` / `job_rx` fields are unused
on the Dedicated path (kept only to share one struct definition across
classes):

```
arena = stream.dedicated_arena
for job in batch:
    if arena has insufficient space:
        roll_dedicated(stream)                 // close old, allocate new
        arena = stream.dedicated_arena
    memcpy job.payload into arena at cursor (u32 BE len + payload)
    directory.entries[(stream_id, epoch)].append(byte_pos, len, seq)
    cursor += 4 + len
    record per-job (arena_id, byte_pos) in the stream leader's local list
```

This is the same machine code as today's fast path — an inline memcpy
loop with no atomic overhead.

**Shared** — many streams converge on one arena:

```
arena = pool.active.load()                     // ArcSwap
prev  = arena.in_flight.fetch_add(1, Acquire)
if prev == 0:
    // arena leader
    process_batch(arena, own_batch)            // see below
    loop:
        drained_batches = drain_up_to(arena.job_rx, max_arena_batch)
        for b in drained_batches: process_batch(arena, b); b.reply.send(...)
        remaining = arena.in_flight.fetch_sub(1 + drained.len(), Release)
                    - (1 + drained.len())
        if remaining == 0: break
else:
    // arena follower: delegate own batch and await
    (tx, rx) = oneshot::channel()
    arena.job_tx.send(WriteBatch { stream_id, epoch, jobs: own_batch, reply: tx })
    ack = rx.await
    apply ack.results to the stream leader's local list

fn process_batch(arena, b):
    for job in b.jobs:
        if arena has insufficient space:
            pool.roll(arena)                   // Rolled → resident; new active
            arena = pool.active.load()
        memcpy job.payload at cursor
        directory.entries[(stream_id, epoch)].append(byte_pos, len, job.seq)
        cursor += 4 + len
        results.push(JobResult { arena_id: arena.id, byte_pos })
```

Notes:

- **Inline memcpy on the winning path**: a stream whose leader wins the
  arena CAS memcpies directly, with no channel hop. Multi-stream
  contention is bounded by the arena-level CAS retry, not by waking a
  long-running writer task.
- **Per-stream grouping is automatic**: each `WriteBatch` is one stream
  so the arena directory entry is updated with contiguous records.
- **Arena roll mid-batch is legal**: records in one `WriteBatch` may
  land on two arenas; per-job `JobResult.arena_id` captures this. The
  stream leader then registers multiple `arena_id`s into
  `ep.resident_arenas`.

### Ordering Guarantee

- Within a stream, within a batch: stream leader assembles payloads in
  seq order and passes them to Layer 2 in that order; arena memcpy is
  sequential within a batch. `directory.byte_positions[i]` for record
  `start_offset + i` is strictly monotonic per stream.
- Within a stream, across batches: the stream's `in_flight` CAS
  guarantees that batch B's leader turn only starts after batch A's
  `fetch_sub` completes. The crossbeam `stream.job_rx` is FIFO, so
  A's arena submission precedes B's. The arena's `job_rx` is FIFO, so
  A reaches the arena leader first. Directory entries extend monotonically.
- Across streams: arena FIFO preserves the cross-stream write order in
  the buffer, but cross-stream ordering is irrelevant to correctness —
  each stream's directory entry is independent.

### Arena Roll

Same mechanism for both classes, performed by whichever task currently
holds the arena's exclusive write turn (the stream leader in Dedicated;
the arena leader in Shared):

1. Detect insufficient space for the next record.
2. `state.store(Rolled)` on the current arena.
3. Move from `ArcSwap(active)` to `pool.resident` (Shared) or from
   `stream.dedicated_arena` to the stream's recent-arena list
   (Dedicated).
4. Allocate a new arena (`ArenaBuffer` via `alloc_zeroed`). Install as
   the new `active` (Shared) or stream-arena (Dedicated).
5. Rolled arena handed to the S3 flush task (fire-and-forget,
   compaction and upload run off the critical path).
6. Continue memcpy on the new arena for the remaining records in the
   current batch.

Arena identity is **local to each EN**. A Primary and its Secondaries
each run their own pools and roll arenas independently based on their
own fill pressure; there is no cross-replica coordination on arena
boundaries. The Primary's `arena_id` is never exposed on the replication
wire. What the replicas share is only the per-`(stream, epoch)` record
stream, not the arena packaging.

No SM round-trip on roll. No epoch bump. No `NOTIFY_SEALED_EXTENT`
(deleted).

### Forward Protocol

- `Forward` carries `(stream_id, epoch, seq, payload)`. No `byte_pos`.
  Secondaries replay in strict order per epoch and compute their own
  byte_pos.
- `RegisterEpoch` (SM → Primary): allocates a new epoch. Carries `epoch`,
  `start_offset`, `replica_set`, `arena_class`.
- `ForwardInitEpoch` (Primary → Secondary): sent before the first `Forward`
  for a new epoch. Carries `epoch`, `start_offset`, `arena_class`.
  Secondaries use `arena_class` to decide whether to open a Dedicated arena
  for this epoch or route appends into the shared pool.
- `SealStream` (client → SM): **user-facing** stream-writer API. Request
  carries `(stream_id, current_epoch)`. SM verifies the epoch matches
  MySQL (stale client → SM responds with the newer epoch + redirect
  hint, no seal performed), runs `SealEpoch` against the current
  replica set, records the sealed epoch's `end_offset` in
  `stream_epochs`, allocates a new epoch with a new replica set,
  issues `RegisterEpoch` to the new Primary and waits for ack, then
  responds to the client with the new epoch and Primary address.
- `SealEpoch` (SM → EN): **internal 2-phase sub-protocol** used by SM
  while processing `SealStream` and during failover-driven seal. Keyed
  by `(stream_id, epoch)`. Phase 1 Prepare (flag=0x00): replica
  responds with its local committed offset. Phase 2 Commit (flag=0x02):
  SM broadcasts the authoritative committed offset; replica corrects
  its local seal point and transitions the epoch to Sealed. Broadcast
  to every replica in the epoch's replica_set.
- `NotifyArenaClass` (new, EN → SM): reports a runtime class transition so
  SM persists the change in `streams.arena_class`.
- `UpdateArenaFlushed` (EN → SM): notifies SM after a Shape A arena upload
  completes. Carries `arena_id`, `s3_key`, and the list of `(stream, epoch,
  start, end)` primary-cohort entries in the object.
- `ForwardFlushed` (new, Primary → Secondary, fire-and-forget): sent
  after a successful Shape A upload. Carries the **list of flushed
  entries** `Vec<(stream_id, epoch, start_offset, end_offset)>`
  (the same list sent to SM in `UpdateArenaFlushed`). Arena identity
  is not on the wire — each secondary matches entries against its
  own directory by `(stream_id, epoch, offset range)` and drops
  matching `EpochArenaEntry`s regardless of which local arena they
  live in. Typically ≤ 32 KiB per message for a 64 MiB arena with
  ~1000 streams; fire-and-forget off the hot path.
- `FlushEpochStream` (new, SM → EN): DR request. Carries
  `(stream_id, epoch)`. EN gathers resident records for that tuple across
  all arenas, builds a Shape B object, uploads, and replies with
  `UpdateEpochFlushed`.
- `UpdateEpochFlushed` (EN → SM): DR completion ack. Carries
  `stream_id, epoch, s3_key, start, end`.

### Seal (Epoch Bump Only)

An epoch bump is the **only** event that seals an epoch. Two things can
trigger it:

- **Client-initiated**: a stream writer sends `SealStream(stream_id,
  current_epoch)` to SM (e.g., to recover after a timeout or to force a
  consistency checkpoint).
- **Failover-initiated**: SM's leader detects a dead node in the current
  replica set via the heartbeat checker.

In both cases SM runs the same sub-protocol against the current epoch's
replicas via `SealEpoch`:

1. **Prepare** (`SealEpoch` flag=0x00): SM queries each replica for its
   local committed offset on the current epoch.
2. **Quorum**: SM sorts the offsets descending and takes the k-th value
   where `k = RF/2`. (If the Primary is reachable, its offset is
   authoritative.)
3. **Commit** (`SealEpoch` flag=0x02): SM broadcasts the authoritative
   committed offset to all replicas; each corrects its local seal point
   and transitions the epoch to Sealed.
4. **Allocate**: SM picks a new replica set, bumps the epoch, calls
   `RegisterEpoch` on the new Primary, waits for its ack.
5. **Respond**:
   - For client-initiated: SM responds to the `SealStream` request with
     the new epoch and new Primary address. Writes resume immediately on
     the new epoch.
   - For failover-initiated: no upstream response; new clients discover
     the new epoch on their next `DescribeStream` or when their Primary
     connection fails over.

The sealed epoch's `limit` and `end_offset` are recorded in
`stream_epochs`. Its arenas are unaffected: they stay resident in
whichever pool owns them, get flushed by the normal path, and evict by
the normal LRU. The sealed epoch remains readable from memory until its
arenas are uploaded and evicted, after which reads go through
`epoch_arenas` + S3.

### Backpressure

The arena MPSC is bounded by `writer_channel_capacity` (Shared) or trivially
uncontended (Dedicated, owned by the stream). If the Shared arena MPSC is
full, the stream leader awaits up to `arena_append_timeout_ms`; on timeout
it replies `Busy` to the client. Sustained busy is the signal that drives
runtime promotion of the stream to Dedicated.

### Runtime Promotion / Demotion

Each stream tracks a rolling EWMA of write bytes/s. Evaluated on arena roll
boundaries (not epoch bumps — they are too rare):

- `class == Shared` and rate > `promote_to_dedicated_bytes_per_sec`: the
  **next arena** allocated for this stream is Dedicated. The stream's
  `arena_class` flips. Current epoch is unaffected; future arenas go to the
  new class.
- `class == Dedicated` and rate < `demote_to_shared_bytes_per_sec`: next
  arena is Shared.
- Hysteresis: `class_transition_min_dwell_ms`.

SM is notified via `NotifyArenaClass`; MySQL `streams.arena_class` updated.

This means a single epoch can have arenas of both classes in its
`resident_arenas` list. That's fine — arena state is self-describing
(`SharedArena` vs dedicated-pool arena), and reads route through the pool
that owns the target arena regardless.

## Read Path

Two tiers.

### Tier 1: Per-Arena Directory (Warm, In-Memory)

```
// Locate the target epoch: the request carries (stream_id, offset).
// Binary-search `stream.epochs` for the epoch whose [start_offset, limit)
// covers `offset`. For open epochs, `limit` is effectively infinite.
ep = stream.find_epoch_containing(offset)      // may be sealed but still
                                               // resident on this EN

for arena_id in ep.resident_arenas.lock().iter():
    arena = lookup(arena_id)                   // shared pool or dedicated pool
    if arena is None or arena.state == Evicted:
        continue
    entry = arena.directory.lock().entries.get(&(stream_id, ep.epoch))
    if entry.start_offset <= offset < entry.end_offset:
        idx      = (offset - entry.start_offset) as usize
        byte_pos = entry.byte_positions[idx]
        return bytes_from_arena(arena, byte_pos, count)
```

`resident_arenas` is typically 1–3 entries for live epochs. Zero-copy reads
via `Bytes::from_owner(OwnedArenaSlice)` keep the arena alive against
concurrent eviction.

Lock granularity: `directory` is `Mutex<>` during write; once the arena is
`Rolled`, it is read-only and the lock never contends.

### Tier 2: S3 Cold Read

If no resident arena has the record:

1. Look up `epoch_arenas` by `(stream_id, epoch)`, binary-search by offset
   to find the arena S3 object.
2. Fetch through `S3Reader` + moka LRU. The object's directory locates the
   `(stream_id, epoch)` block; fetch the target block via range read;
   decompress; sparse index resolves `offset → byte_pos_in_block`;
   `Bytes::slice`.

## S3 Object Format — Two Shapes

Shared arenas produce S3 objects on two independent code paths with
different shapes. Readers route to the right parser via the
`s3_key_kind` column in `epoch_arenas`.

### Shape A: Arena Object (normal fill-and-rotate path)

One object per fully-rolled Shared arena. Key:
`{namespace}/arenas/{arena_id:016x}.dat`. Contains **primary-cohort
entries only** — i.e., records for `(stream, epoch)` tuples where this
EN is the Primary. Secondary-cohort entries are dropped at arena-roll
time and not included.

On arena roll, the flush task compacts the arena slice-by-slice: it
already has each primary-cohort entry's `byte_positions` and the
entry's records are already contiguous thanks to the per-stream gather
done at write time, so compaction is a linear walk — one entry at a
time, one stream's records blocked into 64-record compressed chunks:

```
┌─ Header (fixed 32 bytes) ────────────────────────────────────┐
│  magic              : u32  (0x53415248 "SARH")               │
│  version            : u16  (1)                               │
│  flags              : u16                                    │
│  arena_id           : u64                                    │
│  entry_count        : u32   (primary-cohort entries only)    │
│  data_section_start : u32                                    │
│  crc32              : u32  (over directory + data sections)  │
│  compression        : u8   (0=none, 1=zstd, 2=lz4)          │
│  _reserved          : [u8; 3]                                │
├─ Entry Directory (variable) ─────────────────────────────────┤
│  For each entry (entry_count):                               │
│    stream_id         : u64                                   │
│    epoch             : u64                                   │
│    start_offset      : u64                                   │
│    end_offset        : u64                                   │
│    record_count      : u32                                   │
│    block_count       : u32                                   │
│    block_index_start : u32                                   │
│    data_start        : u32                                   │
│    data_size         : u32                                   │
├─ Per-Entry Block Index (variable) ───────────────────────────┤
│  For each entry, at block_index_start:                       │
│    [block_count × u32]  byte offset of compressed block i    │
│    [block_count × u32]  record count in block i              │
├─ Per-Entry Data (variable) ──────────────────────────────────┤
│  Entry A: compressed(records[0..64]), compressed([64..128])  │
│  Entry B: compressed(records[0..64]), ...                    │
└──────────────────────────────────────────────────────────────┘
```

A Dedicated arena is always one primary-cohort entry (one stream, one
epoch). Using the same multi-entry shape with entry_count=1 for
Dedicated keeps the S3 reader uniform, so the existing per-stream
chunk-compressed layout also lives inside this header for Dedicated.

Block size = `s3_index_step` records (default 64). Each block is
independently compressible and is the unit of decompression and moka
cache.

### Shape B: Per-Stream Object (DR upload path)

When SM asks the EN to urgently upload a specific `(stream_id, epoch)`
outside the normal arena-roll schedule (see Flush Lifecycle → DR Flush
below), the EN builds a **per-stream object** in the same
chunk-compressed format used today for Dedicated streams. Key:
`{namespace}/data/{stream_id:016x}/{start_offset:016x}_{end_offset:016x}.dat`.

This is the same key shape and the same in-file format used for
Dedicated extents today, so:

- The S3 reader has one code path for "per-stream object" regardless of
  whether it was produced by Dedicated normal-path flush or by DR
  urgent flush.
- Readers resolve an `epoch_arenas` row to the right parser via
  `s3_key_kind` (arena vs per-stream).

The EN gathers all records for `(stream_id, epoch)` across every
resident arena in the pool by walking each arena's directory for
primary-cohort entries matching the tuple, then concatenates the byte
slices in offset order (zero-copy via `Bytes::slice` over the arena
buffers), compresses into 64-record blocks, and uploads.

## Stream Manager Metadata

`streams` (existing table) gains:

```sql
ALTER TABLE streams
  ADD COLUMN arena_class TINYINT NOT NULL DEFAULT 0;  -- 0=Dedicated, 1=Shared
```

`extents` table is **replaced by** `stream_epochs`:

```sql
CREATE TABLE stream_epochs (
    stream_id     BIGINT  NOT NULL,
    epoch         BIGINT  NOT NULL,
    start_offset  BIGINT  NOT NULL,
    end_offset    BIGINT  NULL,        -- set on seal (epoch bump)
    state         TINYINT NOT NULL,    -- Open | Sealed
    replica_set   TEXT    NOT NULL,    -- JSON array of node_ids
    arena_class   TINYINT NOT NULL,    -- snapshot at epoch allocation
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    sealed_at     TIMESTAMP NULL,
    PRIMARY KEY (stream_id, epoch),
    INDEX (stream_id, start_offset)
);
```

`extent_s3_objects` is replaced by `epoch_arenas`:

```sql
CREATE TABLE epoch_arenas (
    stream_id    BIGINT NOT NULL,
    epoch        BIGINT NOT NULL,
    sequence     INT    NOT NULL,   -- 0..N-1 in offset order within epoch
    arena_id     BIGINT NOT NULL,   -- 0 for Shape B (per-stream) objects
    s3_key       VARCHAR(512) NOT NULL,
    s3_key_kind  TINYINT NOT NULL,  -- 0 = Shape A arena object, 1 = Shape B per-stream object
    start_offset BIGINT NOT NULL,
    end_offset   BIGINT NOT NULL,   -- exclusive
    PRIMARY KEY (stream_id, epoch, sequence),
    INDEX (stream_id, epoch, start_offset)
);
```

A single `(stream_id, epoch, offset)` range may be uploaded under both
shapes (e.g., DR uploaded it urgently, then the arena's normal seal
later uploaded it inside a Shape A object). Both rows exist; readers
prefer whichever they find first. The extra PUT cost is acceptable; S3
PUT is idempotent and storage cost is tiny relative to durability.

A separate operational `arenas` table tracks per-arena state across the
cluster (resident node, flush status). Not on any hot path.

## Flush Lifecycle

Two independent paths produce S3 objects. **Both run on the background
S3 flush task — off the write critical path.** Arena roll completes as
soon as the new arena is installed; the rolled arena is handed to the
flush task for asynchronous compaction-plus-upload. Writers continue
appending to the new active arena without waiting for any S3 work.

### Normal Path — Arena Fill-and-Rotate (Shape A)

Runs once per rolled arena, on the background flush task. Only
includes **primary-cohort** entries; secondary-cohort entries are
dropped at roll time.

1. Arena rolls → `state = Rolled` → rolled arena handed to the S3
   flush task's queue (fire-and-forget from the arena leader's
   perspective).
2. Flush task walks the arena's directory. For each `(stream_id, epoch)`
   entry, it looks up the `Arc<StreamEpoch>` via
   `streams.get(stream_id).epochs.read().get(&epoch)` — the epoch is
   guaranteed to still be in the map because the directory entry's
   existence holds a `directory_ref_count` on it. From the epoch it
   reads `replica_info.primary_node_id`:
   - If that equals this EN's `node_id` → primary-cohort, include in
     the upload.
   - Otherwise → secondary-cohort, skip. In both cases the directory
     entry is dropped at the end of this step and the epoch's
     `directory_ref_count` decremented; if it reaches zero, the
     `Stream` removes the epoch from its `epochs` map.

   For each primary-cohort entry:
   - Records are already contiguous in the arena buffer (per-stream
     gather was done at write time). The compaction step is a linear
     walk over `byte_positions`, grouping into 64-record blocks and
     compressing each block.
3. Task builds the Shape A layout in a staging buffer and uploads via
   `aws-sdk-s3` (multipart above `s3_multipart_threshold`). Retry
   policy matches today's flusher: exponential backoff capped at 30 s,
   S3 HEAD check on retry to skip if a peer already uploaded.
4. On success: `UpdateArenaFlushed(arena_id, s3_key, entries)` to SM,
   where `entries = Vec<(stream_id, epoch, start_offset, end_offset)>`.
5. SM writes one `epoch_arenas` row per entry
   (`s3_key_kind = 0`) in a single transaction.
6. Primary broadcasts `ForwardFlushed(entries)` to peer replicas
   (fire-and-forget), carrying the same `entries` list sent to SM. For
   each flushed `(stream_id, epoch, start, end)`, the secondary walks
   its own arena directories and releases the matching offset range.
   Release is **range-based, not entry-based**: because secondary
   arenas roll independently of the Primary's, one Primary-side flushed
   range may partially or fully cover a secondary's
   `EpochArenaEntry`, and one secondary entry may be covered by
   several incoming `ForwardFlushed` messages over time.
   Each entry tracks a small cumulative-covered range set; when the
   union reaches the entry's `[start, end)`, the entry is dropped and
   its arena buffer refcount decrements. When the refcount hits zero
   the allocation is freed. The covered-range translation uses
   `StreamEpoch.start_offset + byte_positions.index` — the same
   arithmetic used by Shape A compaction and Shape B DR gather, so no
   new primitive is needed.
7. Arena becomes LRU-eligible.

Secondaries never participate in Shape A upload. They accumulate
entries as Forwards arrive; they release them when `ForwardFlushed` or
a DR request resolves the entry.

### DR Path — Per-Stream Urgent Upload (Shape B)

Also runs on the background flush task (given its own priority queue so
DR work can preempt Shape A backlog). SM triggers this when a stream
needs durability guarantees faster than its Shared arena will fill:
after a fallback seal (Primary unreachable, offset quorum resolved from
secondaries).

Opcode: `FlushEpochStream(stream_id, epoch)` — SM → EN. EN flow:

1. Flush task scans every resident arena in the pool. For each arena
   whose directory has an entry for `(stream_id, epoch)` — the cohort
   distinction does not matter here; DR accepts both Primary and
   Secondary records because the purpose is durability regardless of
   who was Primary — collect the entry's slices.
2. Concatenate slices in offset order (zero-copy via `Bytes::slice`
   over the arena buffers).
3. Build Shape B object (same chunk-compressed format used for
   Dedicated extents today), key
   `{namespace}/data/{stream_id:016x}/{start_offset:016x}_{end_offset:016x}.dat`.
4. Upload via `aws-sdk-s3`.
5. Respond to SM with `UpdateEpochFlushed(stream_id, epoch, s3_key,
   start_offset, end_offset)`.
6. SM writes one row to `epoch_arenas` with `arena_id = 0` and
   `s3_key_kind = 1`, covering the full `[start, end)` range that was
   uploaded.

DR path does **not** drop directory entries or flush the arena itself —
the arena continues filling normally, and its eventual Shape A upload
will re-upload the same primary-cohort records. The extra storage cost
is bounded and acceptable. Readers binary-search `epoch_arenas` by
offset and take the first covering row.

### DR Staleness Detection

Explicitly out of scope for this iteration. DR upload is triggered only
by SM's fallback-seal flow (Primary detected dead during `SealEpoch`).
A periodic staleness scan may be added later.

### Cohort-Switch Within a Single Arena

A shared arena's directory may contain entries for the same stream
under both roles — for example, when an EN serves as secondary for
`(stream, epoch_N)`, then epoch_N seals, and SM promotes the same EN
to Primary for `(stream, epoch_N+1)`. New appends land in whichever
arena is currently active at the pool, which may be the same arena
that still holds the secondary-cohort entry for `epoch_N`. The arena
then simultaneously holds:

- A secondary-cohort entry for `(stream, epoch_N)`.
- A primary-cohort entry for `(stream, epoch_N+1)`.

This is handled entirely by the mechanisms already defined:

- **Shape A upload** iterates directory entries and looks up each
  epoch's `replica_info.primary_node_id`. The `epoch_N` entry is
  skipped (other node is Primary); the `epoch_N+1` entry is compacted
  and included. Epochs being strictly monotonic per stream means the
  S3 object unambiguously advertises the correct offset range.
- **ForwardFlushed** for `epoch_N` arrives from the actual Primary of
  `epoch_N` and covers ranges in the secondary-cohort entry. Release
  proceeds normally; the primary-cohort entry for `epoch_N+1` is
  unaffected.
- **Eviction** of the arena waits until both entries are resolved
  (Shape A uploaded the primary-cohort entry AND all of the
  secondary-cohort entry's ranges were released by `ForwardFlushed`).
  The arena may stay pinned longer than one with a single epoch's
  worth of records; this is a memory cost, not a correctness issue.
- **Reads** route to the right entry via offset: reads in
  `[..., epoch_N.end_offset)` resolve through the `epoch_N` entry
  (or its S3 object once flushed by its owning Primary), reads in
  `[epoch_N+1.start_offset, ...)` through `epoch_N+1`.

No new state or code path is required — the existing range-based
release rule and epoch-keyed directory already do the right thing.

## Eviction

Global LRU over the SharedArenaPool (Shared class) plus a small cap
(`cache_arenas` per stream, default 4) over each Dedicated stream's own
arena list.

- Arena becomes LRU-eligible once `state = Uploaded` (or, for Memory
  `StorageClass`, immediately on `Rolled`).
- Rolled-but-not-yet-Uploaded arenas are pinned; if too many accumulate,
  writer backpressure kicks in.
- Eviction drops the `Arc<SharedArena>` from `resident`. In-flight readers
  keep the underlying buffer alive via their own `Arc` clone in the
  `OwnedArenaSlice` that backs `Bytes::from_owner`.

## Configuration

New `ExtentNodeConfig` fields:

| Field | Default | Notes |
|---|---|---|
| `dedicated_arena_size` | 256 MiB | Per-Dedicated-stream arena buffer size. Fixed; no adaptive grow/shrink. |
| `shared_arena_size` | 64 MiB | Per-Shared-arena buffer size (independent between Primary and Secondary) |
| `max_resident_shared_arenas` | 64 | → 4 GiB shared budget |
| `shared_writer_channel_capacity` | 4096 | Bounded arena MPSC size (in `WriteBatch` units) |
| `max_stream_batch` | 64 | Max stream-leader batch size (own + drained followers) |
| `max_arena_batch` | 32 | Max arena-leader drain per turn |
| `arena_append_timeout_ms` | 1000 | Backpressure timeout |
| `arena_max_age_ms` | 60,000 | Idle arena roll |
| `cache_arenas` | 4 | Per-Dedicated-stream arena memory cap |
| `promote_to_dedicated_bytes_per_sec` | 10 MiB/s | Runtime promotion |
| `demote_to_shared_bytes_per_sec` | 100 KiB/s | Runtime demotion |
| `class_transition_min_dwell_ms` | 300,000 | Hysteresis |

New per-stream `CreateStream` field:

| Field | Default |
|---|---|
| `arena_class` | `Dedicated` |

**Removed configuration** (obsolete under the new model):

- `min_extent_capacity`, `max_extent_capacity`, `extent_growth_factor` —
  adaptive per-stream extent sizing is no longer needed. The tradeoff it
  was solving (high-throughput vs low-throughput streams sharing one
  sizing knob) is now resolved by `ArenaClass`: Dedicated uses a fixed
  `dedicated_arena_size`, Shared uses the pool's `shared_arena_size`.
- `cache_extents` — replaced by `cache_arenas` (per-Dedicated-stream) and
  `max_resident_shared_arenas` (global).
- `max_records_per_shared_extent`, `shared_extent_max_age_ms` — no
  per-extent record/age caps in the new model; epochs live until
  epoch-bump seal.

The adaptive-capacity implementation (grow factor, idle-shrink timer)
is deleted from the EN. The `adaptive-capacity.md` design doc is
obsolete.

## Metrics

Per pool:

- `shared_arena_resident_count`, `shared_arena_bytes_resident`
- `shared_arena_rolls_total`, `shared_arena_evictions_total`
- `shared_arena_writer_channel_depth` (per active arena)
- `shared_arena_flush_pending`

Per stream class:

- `streams_by_class{class="shared|dedicated"}` gauge
- `class_promotions_total`, `class_demotions_total`
- `shared_append_latency_seconds`, `dedicated_append_latency_seconds`
  histograms

Per epoch (debug):

- `epoch_record_count`, `epoch_age_seconds`, `epoch_resident_arenas`

## Error Handling

| Failure | Behavior |
|---|---|
| Writer task panics | Arena state → `Failed`; new appends route to a freshly rolled arena. In-flight replies return `AppendError::WriterFailed`; caller propagates to client. SM epoch bump recovers. |
| MPSC channel full | Caller awaits up to `arena_append_timeout_ms`; on timeout returns `Busy`. Sustained busy → runtime promotion for Shared-class streams. |
| Arena allocation OOM | EN refuses new Shared writes; reports via `node_metrics`; SM avoids placing new Shared epochs there. |
| S3 upload fails indefinitely | Existing retry policy. Arena pinned; writer backpressure applies. |
| `ForwardFlushed` arrives for entries a secondary doesn't have | No-op; the entry may have already been evicted or never recorded (e.g., arrived on a secondary that joined the replica set after the record). Safe because the message is advisory for memory reclamation, not for correctness. |
| Reader finds arena `Evicted` mid-lookup | Falls through to Tier 2. |
| Cross-class mismatch Primary vs Secondary | Impossible: `arena_class` carried on `RegisterEpoch` (SM → Primary) and `ForwardInitEpoch` (Primary → Secondary); any mismatch fails epoch registration. |

## Testing

### Unit Tests

- `SharedArenaPool`: allocate, append, roll, evict.
- `ArenaDirectory`: build, lookup by `(stream, epoch)`, frozen-on-roll.
- `ArenaId`: uniqueness from `(node_id, counter)`; wire round-trip.
- `StreamEpoch`: state machine, `resident_arenas` tracking, seal on epoch
  bump.
- Arena roll mid-batch: records in one `WriteBatch` straddling two arenas
  produce per-record `JobResult.arena_id` correctly.
- Two-layer CAS: concurrent stream leaders contending on one arena
  serialize correctly; per-stream directory entries remain monotonic.

### Integration Tests

- End-to-end append + read on one EN, RF=1, both classes.
- RF=2: Primary and Secondary each roll their own shared arenas
  independently with unrelated `arena_id`s; reads succeed on either
  replica; DR flush from secondary (Shape B) produces the per-stream
  key and is readable.
- Epoch spanning 2–3 arenas in both classes: reads work across arena
  boundaries.
- Cold read after eviction: `epoch_arenas` directs to the right object /
  block.
- Runtime promotion mid-epoch: stream starts Shared, rate exceeds
  threshold, subsequent arenas in the same epoch are Dedicated, reads
  work across class boundary.
- Demotion: symmetric.
- **Cohort switch within one arena**: EN is secondary for epoch N,
  epoch N seals, EN is promoted to Primary for epoch N+1, subsequent
  appends land in the same shared arena as N's secondary entry.
  Verify: Shape A upload includes only the primary-cohort entry,
  `ForwardFlushed` for epoch N releases the secondary-cohort entry,
  arena evicts once both resolve, reads resolve to the correct
  epoch's S3 object.
- **Partial ForwardFlushed coverage**: a secondary's `EpochArenaEntry`
  covers offsets `[a, c)`; Primary sends two `ForwardFlushed` covering
  `[a, b)` then `[b, c)`. Verify the entry is dropped only after the
  second message, and the arena buffer refcount releases then.
- **StreamEpoch retention across seal**: epoch N fills multiple arenas,
  epoch N seals, epoch N+1 begins writing. Verify epoch N's
  `StreamEpoch` remains in `Stream.epochs` until all its arenas'
  primary-cohort entries have been uploaded (Shape A) and
  secondary-cohort entries have been released (`ForwardFlushed`);
  `directory_ref_count` reaches zero and the epoch is dropped.
- **Read against a sealed-but-resident epoch**: after epoch N seals
  and N+1 is active, reads against offsets in N's range still resolve
  via `Stream.epochs.get(N)` + Tier 1 per-arena directory (no S3 fetch
  while records remain resident).
- **DR flush for sealed epoch**: SM sends `FlushEpochStream(stream, N)`
  after N has sealed. EN walks `Stream.epochs.get(N).resident_arenas`,
  gathers records, uploads Shape B, and acks. After upload and
  subsequent Shape A uploads resolve remaining entries,
  `StreamEpoch` is dropped.
- 100K Shared streams: memory budget respected; evictions behave.

### Stress Tests

- 1M Shared streams × 1 rec/s for 10 min: RAM within budget, no writer
  starvation.
- Mixed: 10 Dedicated at 100 MiB/s + 100K Shared at 10 KiB/s; both SLOs
  preserved.

### Crash / Recovery

- Primary dies mid-arena: SM epoch bump, new Primary takes over. DR flush
  from surviving replica for the old epoch's unflushed arenas.
- Writer task panic: pool recovers; next append succeeds on the next
  arena.

## Open Questions

- Whether to shard the shared pool's writer into N parallel writers if
  single-writer throughput becomes limiting. The design allows this to be
  added later without changing public schemas.
- Whether `resident_arenas` on `StreamEpoch` should be a `DashMap` instead
  of `Mutex<SmallVec<>>` if read-path contention is observed in practice.
- `NOTIFY_SEALED_EXTENT` is gone; the current code's
  `autonomous-extent-creation` path is deleted with it. The failover
  design relies on SM's periodic staleness scan to flush arenas whose
  Primary died before upload; confirming this is sufficient for the MTTR
  target is deferred to the implementation plan.
