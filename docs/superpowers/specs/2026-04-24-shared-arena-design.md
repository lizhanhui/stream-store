# Shared Arena for Extent Memory

## Motivation

Today each active extent owns a dedicated arena: `Arc<ArenaBuffer>` allocated via
`std::alloc::alloc_zeroed` plus a `Box<[AtomicU32]>` index sized `capacity / 5`.
Defaults are `min_extent_capacity = 8 MiB` and index capacity `~6.7 MiB` per
minimum-sized extent, so a freshly registered stream with one active extent and
one recycled spare in its extent pool costs roughly **30 MiB of RAM**.

This is fine for Kafka-style workloads — a handful of high-throughput streams —
but unusable for MQTT-style workloads where an ExtentNode may host on the order
of **one million low-traffic queues**. At 30 MiB × 1M = 30 TiB, the per-stream
memory floor dominates any cluster design.

The goal is to support both workloads in one system:

- **Shared class**: many low-traffic streams share a pool of arenas, so memory
  scales with total bytes in flight, not with stream count. Target per idle
  stream: **< 4 KiB**.
- **Dedicated class**: a small number of high-throughput streams keep the
  current single-writer-per-stream fast path with zero cross-stream coordination.

Per-stream class is declared at `CreateStream` time and may be overridden at
runtime when observed throughput crosses configured thresholds.

## Scope

This spec is implemented **in place**. The store is pre-production; schemas,
wire protocol, and internal interfaces are modified directly. There is no
dual-run, no feature-flagged rollout, and no backwards-compatibility layer.

## Core Abstractions

### ArenaClass

A new per-stream property, orthogonal to the existing `StorageClass`:

```rust
enum ArenaClass {
    Dedicated = 0,  // per-stream arena + per-extent u32 index (existing fast path)
    Shared    = 1,  // records land in an ExtentNode-wide shared arena pool
}
```

Stored in the `streams` MySQL row. Propagated from SM to ExtentNodes via
`RegisterExtent` (per-extent allocation); the Primary further propagates it
to secondaries on the first forwarded append via `ForwardInitExtent`.

### Extent Storage

The existing `Extent` struct keeps its identity fields (`state`, `start_offset`,
`limit`, `committed_seq`, `committed_bytes`, `record_count`, `epoch`,
`replica_info`, seal/flush flags). Only its backing storage becomes class-aware:

```rust
enum ExtentStorage {
    Dedicated(DedicatedStorage),   // Arc<ArenaBuffer> + Box<[AtomicU32]> (today)
    Shared(SharedRef),
}

struct SharedRef {
    // Arenas this extent has touched, in append order. Small (1–3 typical).
    // Appended under the stream's group-commit leader lock; frozen at seal.
    arenas: SmallVec<[ArenaId; 2]>,
}
```

Extent identity, replication, quorum tracking, and seal are identical across
classes. Only the memcpy target and the read resolution differ.

### Stream

`Stream` is unchanged in shape:

- Stream-level pipelined group commit (`in_flight` + `job_tx/job_rx`) still
  elects a single active writer per stream.
- On the leader's turn:
  - **Dedicated**: memcpy directly into the per-extent arena (today's code).
  - **Shared**: submit an `AppendJob` to the current shared arena's writer
    channel and await the ack.

### Runtime Promotion and Demotion

Each stream maintains a rolling EWMA of write bytes/s and records/s. At every
extent boundary (seal-and-create-next) the stream evaluates:

- If `class == Shared` and observed rate exceeds
  `promote_to_dedicated_bytes_per_sec`, the next extent is created as
  Dedicated.
- If `class == Dedicated` and rate is below
  `demote_to_shared_bytes_per_sec`, the next extent is created as Shared.
- Both are gated by `class_transition_min_dwell_ms` (hysteresis).

Class transitions only happen at extent boundaries. No data migration: the
sealed extent retains the class it was created under; only the new extent
changes class. Stream Manager is notified via `NotifyArenaClass`; MySQL
`streams.arena_class` is updated. Clients continue talking to the same
Primary.

## SharedArenaPool

One pool per ExtentNode, owning all shared-class arenas:

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

```rust
struct SharedArena {
    id:         ArenaId,                  // (node_id << 48) | local_counter
    buf:        Arc<ArenaBuffer>,         // same type the Dedicated path uses
    state:      AtomicU8,                 // Open | Sealed | Uploaded | Evicted
    created_at: Instant,
    directory:  Mutex<ArenaDirectory>,
    job_tx:     mpsc::Sender<SharedAppendJob>,
    s3_key:     OnceLock<String>,
}

struct ArenaDirectory {
    entries: HashMap<StreamId, StreamArenaEntry>,
}

struct StreamArenaEntry {
    extent_id:         ExtentId,
    start_offset:      u64,
    end_offset:        u64,        // exclusive
    byte_positions:    Vec<u32>,   // per record, within this arena
    arena_start_byte:  u32,
    arena_end_byte:    u32,
}

struct SharedAppendJob {
    stream_id: StreamId,
    extent_id: ExtentId,
    seq:       u64,
    payload:   Bytes,
    reply:     oneshot::Sender<SharedAppendAck>,
}

struct SharedAppendAck {
    arena_id:  ArenaId,
    byte_pos:  u32,                // debug / metrics; reads use the directory
    rolled_to: Option<ArenaId>,    // Some(new_id) if this append caused a roll
}
```

### ArenaId

`ArenaId = u64 = (node_id << 48) | local_counter`. Globally unique by
construction, so the S3 key `{namespace}/shared/{arena_id:016x}.dat` does not
collide across ENs. The node_id field is 16 bits (65,535 ENs per cluster) and
the counter is 48 bits — effectively inexhaustible.

### Writer Task

One task per arena, not per pool:

```
loop:
    batch = drain_up_to(job_rx, max_batch_jobs or max_batch_time)
    group batch by stream_id
    for (stream_id, jobs) in grouped:
        ensure sufficient arena space; if not:
            pool.roll()   // pool seals current arena (state ← Sealed),
                          // moves it into `resident`, allocates a new arena
                          // and a new writer task, installs as `active`.
                          // This writer finishes the current batch; replies
                          // for jobs that did not fit carry rolled_to=Some(new_id)
                          // so the stream leader retries on the new arena.
            break   // drain any unreplied jobs into the new arena's channel
        for job in jobs:
            memcpy job.payload into buf at cursor, prefixed with u32 BE len
            directory.entries[stream_id].append(byte_pos, len, seq)
            cursor += 4 + len
            job.reply.send(Ack{ arena_id, byte_pos, rolled_to: None })
```

Per-stream gather inside the writer ensures that, within one arena, records
belonging to the same stream are laid down contiguously. Jobs for a given
stream arrive in-order through the MPSC (single leader per stream), so this
preserves sequence order within the stream. The resulting physical layout is
the same one that will be written to S3 on flush — no re-sorting or
re-grouping.

## Write Path

### Shared-Class Append (Primary)

Inside the stream's group-commit leader turn:

```
ext = stream.active_extent
seq = ext.record_count.load()

if ext is sealed or extent-full by record/age cap:
    seal_and_create_next_extent()   // may flip class
    ext  = stream.active_extent

if ext.storage is Dedicated:
    take existing Dedicated code path and return

// Shared branch
arena = pool.active.load()
job   = SharedAppendJob { stream_id, extent_id: ext.id, seq, payload, reply: tx }
arena.job_tx.send(job).await
ack   = rx.await

ext.record_count.store(seq + 1)
ext.committed_bytes.store(ext.committed_bytes.load() + 4 + payload.len())
ext.storage.shared.register_arena(ack.arena_id)  // idempotent; appends to SmallVec
ext.committed_seq.store(seq + 1, Release)        // record becomes readable

if ext.replica_info.rf >= 2:
    forward(stream_id, extent_id, seq, payload)  // no byte_pos field
    ack_queue.push(PendingAck { seq, client_reply_tx })
else:
    client_reply_tx.send(AppendAck { offset: ext.start_offset + seq })
```

### Forward Protocol

`byte_pos` is **removed** from the `Forward` frame. Secondaries replay Forwards
in strict order per extent; their own byte_pos is computed locally. This
change applies to both Dedicated and Shared — the Forward wire format is
unified.

Three additions:

- `RegisterExtent` (SM → Primary) gains an `arena_class: u8` field. The SM
  reads `streams.arena_class` during extent allocation and carries it to the
  Primary.
- `ForwardInitExtent` (Primary → Secondaries) gains an `arena_class: u8`
  field (consuming a previously reserved byte). Secondaries use it to decide
  whether to open a per-extent arena (Dedicated) or route appends into their
  own `SharedArenaPool`.
- `ForwardInitArena` is a new fire-and-forget opcode sent by a Primary when
  it rolls its active shared arena. It carries `arena_id: u64` and
  `arena_capacity: u32`. Secondaries allocate a shared arena tagged with the
  same `arena_id` and install it as their active arena for subsequent
  Forwards.

### Secondary Path

On receiving a Forward for a Shared-class extent, the secondary submits a
`SharedAppendJob` to its own pool's active arena with the Primary's
`arena_id`. If the secondary has not seen `ForwardInitArena` for that id yet
(race or lost hint), it lazily allocates the arena on first Forward, keyed by
the Primary-assigned id. This is analogous to Lazy Secondary Extent Creation.

### Extent-Full Criteria (Shared)

A Shared extent seals when any holds:

- `record_count >= max_records_per_shared_extent` (default 65,536)
- `now - extent.created_at >= shared_extent_max_age_ms` (default 300,000 ms)
- Explicit seal (client or SM failover)

Arena-full is **not** an extent-full trigger. Extents may span multiple
arenas.

### Backpressure

The writer MPSC is bounded by `writer_channel_capacity`. Full → stream leader
awaits up to `shared_append_timeout_ms`; on timeout, returns `Busy` to the
client. Sustained backpressure is the signal that drives runtime promotion to
Dedicated.

### Hot-Path Cost (Shared vs Dedicated)

Per append, Shared adds:

- 1 `ArcSwap.load()` (Relaxed)
- 1 bounded MPSC send
- 1 oneshot await

The memcpy itself moves from leader to writer task — same total work,
different thread. Dedicated streams are unaffected.

## Read Path

Two tiers, checked in order.

### Tier 1: Per-Arena Directory (Warm, In-Memory)

```
for arena_id in ext.storage.shared.arenas:
    arena = pool.resident.read().get(&arena_id)
    if arena is None or arena.state == Evicted:
        continue
    entry = arena.directory.lock().entries.get(&stream_id)
    if entry.start_offset <= offset < entry.end_offset:
        idx       = (offset - entry.start_offset) as usize
        byte_pos  = entry.byte_positions[idx]
        return bytes_from_arena(arena, byte_pos, count)
```

`ext.storage.shared.arenas` is typically 1–3 entries. The directory lookup is
O(1). `Bytes::slice` returns zero-copy references backed by `Arc<ArenaBuffer>`
via `Bytes::from_owner(OwnedArenaSlice{...})`, keeping the arena alive for the
reader even if eviction races.

Lock granularity: `directory` is `Mutex<>` during the writer's active phase;
once the arena is `Sealed` it is effectively read-only and the lock never
contends. If contention on still-open arenas is observed, the implementation
may switch to `RwLock<>` or to `DashMap` for `entries`.

### Tier 2: S3 Cold Read

If no resident arena has the record, SM metadata resolves which S3 object
holds the offset:

1. Look up `extent_s3_objects` (below) by `(stream_id, extent_id)`,
   binary-search by offset.
2. Fetch the object through `S3Reader` + moka LRU. The object's header names
   the per-stream block index; fetch the target block (single S3 range read);
   decompress; sparse index resolves `offset → byte_pos_in_block`;
   `Bytes::slice` over the decompressed block.

The S3 block is the unit of decompression and of the moka cache. Existing
`S3Reader` machinery is reused; the new code is the multi-stream S3 object
parser described below.

### Unified S3 Metadata

Dedicated and Shared extents both resolve cold reads through
`extent_s3_objects`. Dedicated extents always have exactly one row with
`sequence = 0`; Shared extents have one row per arena touched. The legacy
per-extent `s3_key` column on `extents` is removed.

## Seal, S3 Format, Replication

### Seal Triggers

| Trigger | Applies To | Default |
|---|---|---|
| `record_count >= max_records_per_shared_extent` | Shared extent | 65,536 |
| `now - extent.created_at >= shared_extent_max_age_ms` | Shared extent | 300,000 ms |
| Arena full | Arena only | `shared_arena_size` (64 MiB) |
| Arena idle | Arena only | `shared_arena_max_age_ms` (60,000 ms) |
| Client seal / SM failover | Any extent | as today |

Arena seal and extent seal are independent events.

### S3 Object Format (Shared Arena)

One S3 object per shared arena. Key:
`{namespace}/shared/{arena_id:016x}.dat`.

```
┌─ Header (fixed 32 bytes) ────────────────────────────────────┐
│  magic              : u32  (0x53415248 "SARH")               │
│  version            : u16  (1)                               │
│  flags              : u16                                    │
│  arena_id           : u64                                    │
│  stream_count       : u32                                    │
│  data_section_start : u32                                    │
│  crc32              : u32  (over directory + data sections)  │
│  compression        : u8   (0=none, 1=zstd, 2=lz4)          │
│  _reserved          : [u8; 3]                                │
├─ Stream Directory (variable) ────────────────────────────────┤
│  For each stream (stream_count entries):                     │
│    stream_id         : u64                                   │
│    extent_id         : u64                                   │
│    start_offset      : u64                                   │
│    end_offset        : u64                                   │
│    record_count      : u32                                   │
│    block_count       : u32                                   │
│    block_index_start : u32                                   │
│    data_start        : u32                                   │
│    data_size         : u32                                   │
├─ Per-Stream Block Index (variable) ──────────────────────────┤
│  For each stream, at block_index_start:                      │
│    [block_count × u32]  byte offset of compressed block i    │
│    [block_count × u32]  record count in block i              │
├─ Per-Stream Data (variable) ─────────────────────────────────┤
│  Stream A: compressed(records[0..64]), compressed([64..128]) │
│  Stream B: compressed(records[0..64]), ...                   │
└──────────────────────────────────────────────────────────────┘
```

Block size is `s3_index_step` records (default 64). Each block is
independently compressible via the configured codec, enabling random-access
reads via single-block range fetches.

The writer has already laid each stream's records contiguously in the arena
buffer, so building the S3 object at seal time is a linear walk of the
directory: compress block-sized slices from the arena into a staging buffer
and emit the layout above.

### Dedicated Extent S3 Format

Dedicated extents continue to use today's single-stream chunk-compressed
format. The unified difference is only in where the key is recorded
(`extent_s3_objects` instead of an inline `s3_key` column on `extents`).

### Stream Manager Metadata

The `streams` table gains:

```sql
ALTER TABLE streams
  ADD COLUMN arena_class TINYINT NOT NULL DEFAULT 0;  -- 0=Dedicated, 1=Shared
```

The `extents` table **drops** its `s3_key` column.

A new table tracks the S3 object list for every extent:

```sql
CREATE TABLE extent_s3_objects (
    stream_id    BIGINT NOT NULL,
    extent_id    BIGINT NOT NULL,
    sequence     INT    NOT NULL,   -- 0..N-1 in offset order
    arena_id     BIGINT NOT NULL,   -- 0 for Dedicated
    s3_key       VARCHAR(512) NOT NULL,
    start_offset BIGINT NOT NULL,
    end_offset   BIGINT NOT NULL,   -- exclusive
    PRIMARY KEY (stream_id, extent_id, sequence),
    INDEX (stream_id, extent_id, start_offset)
);
```

Dedicated extents write exactly one row on flush (with `arena_id = 0` as a
sentinel). Shared extents write one row per arena they touched, in offset
order.

An operational `arenas` table tracks per-arena state across the cluster for
visibility. It is not on any hot path.

### Flush Lifecycle (Shared Arena)

1. Arena rolls → `state = Sealed` → S3 flush task notified.
2. Flush task walks the `ArenaDirectory`, compresses per-stream blocks,
   uploads via `aws-sdk-s3` (multipart for objects above
   `s3_multipart_threshold`). Retry policy matches the existing Dedicated
   flusher.
3. On success the task sends `UpdateArenaFlushed(arena_id, s3_key, pairs)` to
   SM. `pairs = Vec<(stream_id, extent_id, start_offset, end_offset)>` for
   every `(stream, extent)` this arena touched.
4. SM writes one `extent_s3_objects` row per pair in a single transaction.
5. Primary broadcasts `ForwardFlushed(arena_id)` to all peer replicas of
   streams in the arena (fire-and-forget). Secondaries transition their copy
   of the arena to `Uploaded`.
6. Arena becomes eligible for eviction (next section).

### DR Flush

Extends the existing `FlushExtent (0x1B)` opcode with a variant flag meaning
"flush arena, not extent." SM's staleness scan also searches for
sealed-but-unflushed arenas (beyond `flush_staleness_threshold_ms`) and
delegates upload to any live replica that still holds the arena in memory. S3
PUT is idempotent and all replicas agree on the arena_id and therefore on the
key, so concurrent uploads produce identical objects safely.

### Replication — What Changes

- `Forward` loses `byte_pos`.
- `RegisterExtent` gains `arena_class` (SM → Primary, on extent allocation).
- `ForwardInitExtent` gains `arena_class` (Primary → Secondaries, on first
  forwarded append for a new extent).
- `ForwardInitArena` is new, fire-and-forget (Primary → secondaries on arena
  roll).
- `NotifyArenaClass` is new, EN → SM, sent when a stream's runtime class
  transitions at an extent boundary.

Quorum tracking, watermark handling, 2-phase seal, extent-level checksum — all
unchanged. Extent state (`committed_seq`, `committed_bytes`, `record_count`,
`limit`, `replica_info`) is class-independent, so the shared-path append
updates it exactly as the dedicated path does.

## Eviction

Global LRU over shared arenas in the pool:

- An arena becomes LRU-eligible once `state == Uploaded` (or, for Memory
  StorageClass streams, immediately on `Sealed` since there is no S3 upload).
- `Sealed` but not-yet-`Uploaded` arenas are pinned; if too many accumulate
  relative to `max_resident_shared_arenas`, the writer channel applies
  backpressure to new appends.
- `resident.len() > max_resident_shared_arenas` drains the LRU head until
  under budget.
- Eviction drops the `Arc<SharedArena>` from `resident`. In-flight readers
  retain the underlying `ArenaBuffer` via their own `Arc` clone through
  `Bytes::from_owner(OwnedArenaSlice)`; the allocation is freed when the last
  reader releases it.

`cache_extents` is retained as a per-stream upper bound on **Dedicated**
extents kept in memory. For Shared extents the concept is subsumed by the
global arena LRU; the field has no effect.

## Configuration

New fields in `ExtentNodeConfig`:

| Field | Default | Notes |
|---|---|---|
| `shared_arena_size` | 64 MiB | Per-arena buffer size |
| `max_resident_shared_arenas` | 64 | → 4 GiB shared memory budget |
| `shared_writer_channel_capacity` | 4096 | MPSC bound per arena |
| `shared_append_timeout_ms` | 1000 | Backpressure timeout |
| `max_records_per_shared_extent` | 65,536 | Extent record cap |
| `shared_extent_max_age_ms` | 300,000 | Extent time cap |
| `shared_arena_max_age_ms` | 60,000 | Idle arena roll |
| `promote_to_dedicated_bytes_per_sec` | 10 MiB/s | Runtime promotion threshold |
| `demote_to_shared_bytes_per_sec` | 100 KiB/s | Runtime demotion threshold |
| `class_transition_min_dwell_ms` | 300,000 | Hysteresis |

New per-stream setting on `CreateStream`:

| Field | Default | Notes |
|---|---|---|
| `arena_class` | `Dedicated` | Declared class; runtime can override |

## Metrics

Per pool:

- `shared_arena_resident_count`, `shared_arena_bytes_resident`
- `shared_arena_rolls_total`, `shared_arena_evictions_total`
- `shared_arena_writer_channel_depth` (gauge, per active arena)
- `shared_arena_flush_pending` (sealed, not-yet-uploaded)

Per stream class:

- `streams_by_class{class="shared|dedicated"}` (gauge)
- `class_promotions_total`, `class_demotions_total`
- `shared_append_latency_seconds`, `dedicated_append_latency_seconds`
  (histograms)

Per arena (debug):

- `arena_stream_count`, `arena_utilization`, `arena_age_seconds`

## Error Handling

| Failure | Behavior |
|---|---|
| Shared writer task panics | Arena state → `Failed`; new appends route to a freshly rolled arena. In-flight replies return `AppendError::WriterFailed`; stream leader propagates to client. SM seal-and-new recovers the extent. |
| MPSC channel full | Stream leader awaits up to `shared_append_timeout_ms`; on timeout returns `Busy`. Sustained busy → runtime promotion. |
| Arena allocation OOM | EN refuses new Shared-class writes; reports via `node_metrics`; SM avoids placing new Shared extents there. |
| S3 upload fails indefinitely | Existing retry policy. Arena pinned in memory; shared append backpressure applies. |
| Secondary missed `ForwardInitArena` | Secondary lazily allocates the arena on the first Forward that references its id. |
| Reader finds arena `Evicted` mid-lookup | Falls through to Tier 2 (S3 cold read). |
| Cross-class mismatch between Primary and Secondary | Impossible by construction: `arena_class` is a stream property carried on `RegisterExtent` (SM → Primary) and on `ForwardInitExtent` (Primary → Secondaries). |

## Testing

### Unit Tests (`components/extent-node/src/shared_arena/tests.rs`)

- `SharedArenaPool`: allocate, append, roll, evict.
- `ArenaDirectory`: build, lookup, frozen-at-seal.
- `ArenaId`: uniqueness from `(node_id, counter)`; wire round-trip.
- `Extent` with `ExtentStorage::Shared`: record and age caps, seal.

### Integration Tests (`tests/shared_arena.rs`)

- End-to-end append + read on one EN at RF=1.
- RF=2: Primary + Secondary agree on `arena_id`; DR flush from secondary
  produces identical S3 key and bytes.
- Extent spanning 2–3 arenas: reads succeed across arena boundaries.
- Cold read after eviction: `extent_s3_objects` directs to the right S3
  object and block.
- Runtime promotion: stream starts Shared, rate exceeds threshold, next
  extent is Dedicated, no data loss at the boundary.
- Demotion: hot stream goes idle, next extent is Shared.
- 100K Shared streams: memory budget respected; evictions behave.

### Stress Tests

- 1M Shared streams × 1 rec/s for 10 min: RAM within budget, no writer
  starvation.
- Mixed: 10 Dedicated at 100 MiB/s + 100K Shared at 10 KiB/s; both SLOs
  preserved.

### Crash / Recovery

- Primary of Shared stream dies mid-arena: SM DR flush from secondary
  produces the canonical S3 object.
- Writer task panic: pool recovers; new appends succeed on the next arena.

## Open Questions

- Whether to shard the shared pool's writer into N parallel writers if
  single-writer throughput becomes limiting in production. The design allows
  this to be added later without changing `ExtentStorage`, `ArenaDirectory`,
  or the S3 format.
- Whether to switch `ArenaDirectory.entries` to `DashMap` based on measured
  hotness of the read-tier-2 path.
