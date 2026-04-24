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
  Dedicated) finishes the current arena and starts a new one. Epoch is
  unaffected.

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
| S3 key `{ns}/data/{stream}/{start}_{end}.dat` | Per-arena key: `{ns}/arenas/{arena_id:016x}.dat` for both classes. |

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
    committed_bytes: AtomicU64,               // extent-relative cumulative bytes

    // Memory accounting (both classes) — which arenas this epoch has touched.
    // In append order. Entries drop off when an arena is evicted from the pool;
    // historical arenas are reachable via `epoch_arenas` table.
    resident_arenas: Mutex<SmallVec<[ArenaId; 4]>>,

    // Class-specific state:
    class:           ArenaClass,
    dedicated_state: Option<DedicatedState>,  // Some iff class == Dedicated
}

struct DedicatedState {
    // The single-writer pipelined group commit state (previously on Stream).
    in_flight: AtomicU64,
    job_tx:    crossbeam::channel::Sender<AppendJob>,
    job_rx:    crossbeam::channel::Receiver<AppendJob>,
}
```

`Stream` becomes a thin map from `stream_id → Arc<StreamEpoch>` (the active
epoch) plus metadata like `arena_class`, `storage_class`, EWMA for runtime
class transitions. No pipelined-group-commit state lives on `Stream` anymore
— it moved to `DedicatedState`, because Shared streams drive their writes
through the pool's per-arena writer rather than a stream-level leader.

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

```rust
struct SharedArena {
    id:         ArenaId,                  // (node_id << 48) | local_counter
    buf:        Arc<ArenaBuffer>,
    state:      AtomicU8,                 // Open | Sealed | Uploaded | Evicted
    created_at: Instant,
    directory:  Mutex<ArenaDirectory>,
    job_tx:     mpsc::Sender<SharedAppendJob>,
    s3_key:     OnceLock<String>,
}

struct ArenaDirectory {
    // Keyed by (stream_id, epoch) — an arena can contain records for multiple
    // epochs of the same stream if a cross-arena epoch bump happened to land
    // on this arena (rare but legal).
    entries: HashMap<(StreamId, u64), EpochArenaEntry>,
}

struct EpochArenaEntry {
    start_offset:     u64,
    end_offset:       u64,        // exclusive, running while arena open
    byte_positions:   Vec<u32>,   // per record, within this arena
    arena_start_byte: u32,
    arena_end_byte:   u32,
}

struct SharedAppendJob {
    stream_id: StreamId,
    epoch:     u64,
    seq:       u64,
    payload:   Bytes,
    reply:     oneshot::Sender<SharedAppendAck>,
}

struct SharedAppendAck {
    arena_id:  ArenaId,
    byte_pos:  u32,                // debug / metrics; reads use directory
    rolled_to: Option<ArenaId>,    // Some(new_id) if this append caused a roll
}
```

### ArenaId

`ArenaId = u64 = (node_id << 48) | local_counter`. Globally unique by
construction. S3 key `{namespace}/arenas/{arena_id:016x}.dat` does not
collide across ENs. 16 bits of node_id (65,535 ENs) and 48 bits of counter.

### Writer Task

One task per arena, not per pool. Behavior is identical for Shared and
Dedicated arenas (they differ only in who populates the MPSC — see Write
Path):

```
loop:
    batch = drain_up_to(job_rx, max_batch_jobs or max_batch_time)
    group batch by (stream_id, epoch)
    for ((stream_id, epoch), jobs) in grouped:
        ensure sufficient arena space; if not:
            pool.roll()   // pool seals current arena (state ← Sealed),
                          // moves it into `resident`, allocates a new arena
                          // and a new writer task, installs as `active`.
                          // Remaining jobs are requeued to the new arena's
                          // channel; their eventual reply carries
                          // rolled_to=Some(new_id).
            break
        for job in jobs:
            memcpy job.payload into buf at cursor, prefixed with u32 BE len
            directory.entries[(stream_id, epoch)].append(byte_pos, len, seq)
            cursor += 4 + len
            job.reply.send(Ack{ arena_id, byte_pos, rolled_to: None })
```

Per-(stream, epoch) grouping inside the writer keeps each epoch's records
contiguous within one arena, so the resulting S3 object layout is produced
with no re-sorting on seal.

## Write Path

### Shared-Class Append (Primary)

```
ep = stream.active_epoch               // StreamEpoch, class = Shared
seq = ep.record_count.fetch_add(1, AcqRel)

arena = pool.active.load()             // ArcSwap — cheap read
job   = SharedAppendJob {
    stream_id, epoch: ep.epoch, seq, payload, reply: tx
}
arena.job_tx.send(job).await
ack = rx.await

ep.committed_bytes.fetch_add(4 + payload.len(), Release)
ep.resident_arenas.register(ack.arena_id)      // idempotent push to SmallVec
ep.committed_seq.store(seq + 1, Release)

if ep.replica_info.rf >= 2:
    forward(stream_id, epoch, seq, payload)    // no byte_pos field
    ack_queue.push(PendingAck { seq, client_reply_tx })
else:
    client_reply_tx.send(AppendAck { offset: ep.start_offset + seq })
```

Note: the stream-level group-commit leader election **is not used in Shared
class** — the pool's per-arena writer already serializes concurrent stream
writers. Shared-class Primaries call `record_count.fetch_add` directly; the
race with other appenders is resolved at the pool's MPSC (FIFO preserves
stream-level order because each stream has only one Primary per epoch, and
that Primary's handler is single-threaded per connection for a given
stream).

### Dedicated-Class Append (Primary)

Unchanged from today's fast path, except:

- The pipelined-group-commit leader writes into a **Dedicated arena**
  obtained from a small per-stream arena slot (one active + one pooled
  spare), not into a per-extent arena.
- When the Dedicated arena fills, the writer calls `pool_for_this_stream.roll()`
  (a thin per-stream variant of the same pool mechanism), sealing the old
  arena and installing a new one. **This does not bump the epoch.** The
  running extent-level state (`record_count`, `committed_seq`,
  `committed_bytes`) continues uninterrupted; only the arena changes
  underneath.
- The `NOTIFY_SEALED_EXTENT` opcode is deleted. The Primary no longer
  notifies SM on arena roll; SM learns about the new arena only when the
  flush task sends `UpdateArenaFlushed` after upload.
- `resident_arenas` on the epoch records the sequence of arenas this epoch
  has written to.

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
- `ForwardInitArena` (new, Primary → Secondary, fire-and-forget): sent on
  arena roll. Carries `arena_id` and `arena_capacity`. Secondaries
  allocate an arena tagged with the same `arena_id` so replicas agree on
  arena identity for DR flush.
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

### Arena Roll

Identical mechanism in both classes:

1. Writer detects insufficient space for the next batch.
2. Old arena: `state = Sealed`, moved from `active` to `resident`.
3. New arena allocated (`ArenaBuffer` via `alloc_zeroed`). New writer task
   started. Installed as `active`.
4. `ForwardInitArena` broadcast to secondaries (fire-and-forget).
5. Sealed arena handed to S3 flush task.
6. In-flight jobs not yet appended are requeued to the new arena's channel;
   their acks carry `rolled_to = Some(new_id)` so the caller's epoch-level
   `resident_arenas` is updated.

### Backpressure

Shared writer MPSC bounded by `writer_channel_capacity`. Dedicated writers
use the same per-arena MPSC. Full → caller awaits up to
`arena_append_timeout_ms`; on timeout, returns `Busy` to the client.

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
for arena_id in ep.resident_arenas:
    arena = lookup(arena_id)               // shared pool or dedicated pool
    if arena is None or arena.state == Evicted:
        continue
    entry = arena.directory.lock().entries.get(&(stream_id, epoch))
    if entry.start_offset <= offset < entry.end_offset:
        idx      = (offset - entry.start_offset) as usize
        byte_pos = entry.byte_positions[idx]
        return bytes_from_arena(arena, byte_pos, count)
```

`resident_arenas` is typically 1–3 entries for live epochs. Zero-copy reads
via `Bytes::from_owner(OwnedArenaSlice)` keep the arena alive against
concurrent eviction.

Lock granularity: `directory` is `Mutex<>` during write; once the arena is
`Sealed`, it is read-only and the lock never contends.

### Tier 2: S3 Cold Read

If no resident arena has the record:

1. Look up `epoch_arenas` by `(stream_id, epoch)`, binary-search by offset
   to find the arena S3 object.
2. Fetch through `S3Reader` + moka LRU. The object's directory locates the
   `(stream_id, epoch)` block; fetch the target block via range read;
   decompress; sparse index resolves `offset → byte_pos_in_block`;
   `Bytes::slice`.

## S3 Object Format (All Arenas)

One S3 object per arena. Key: `{namespace}/arenas/{arena_id:016x}.dat`.

```
┌─ Header (fixed 32 bytes) ────────────────────────────────────┐
│  magic              : u32  (0x53415248 "SARH")               │
│  version            : u16  (1)                               │
│  flags              : u16                                    │
│  arena_id           : u64                                    │
│  entry_count        : u32   (number of (stream, epoch) entries)│
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

A Dedicated arena is usually a single entry (one stream, one epoch). A
Shared arena typically has many entries. The same format supports both
classes so the S3 read path is uniform.

Block size = `s3_index_step` records (default 64). Independently
compressible. Each block is the unit of decompression and of the moka
cache.

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
    arena_id     BIGINT NOT NULL,
    s3_key       VARCHAR(512) NOT NULL,
    start_offset BIGINT NOT NULL,
    end_offset   BIGINT NOT NULL,   -- exclusive
    PRIMARY KEY (stream_id, epoch, sequence),
    INDEX (stream_id, epoch, start_offset)
);
```

A separate operational `arenas` table tracks per-arena state across the
cluster (resident node, flush status). Not on any hot path.

## Flush Lifecycle

Identical for Shared and Dedicated arenas:

1. Arena rolls → `state = Sealed` → S3 flush task notified.
2. Flush task walks the directory, compresses per-entry blocks, uploads
   via `aws-sdk-s3` (multipart above `s3_multipart_threshold`). Retry
   policy matches today's flusher.
3. On success: `UpdateArenaFlushed(arena_id, s3_key, entries)` to SM where
   `entries = Vec<(stream_id, epoch, start_offset, end_offset)>`.
4. SM writes one `epoch_arenas` row per entry in a single transaction.
5. Primary broadcasts `ForwardFlushed(arena_id)` to peers (fire-and-forget).
6. Arena becomes LRU-eligible.

### DR Flush

`FlushExtent (0x1B)` is renamed `FlushArena` with the same opcode number.
SM's staleness scan looks for sealed-but-unflushed arenas beyond
`flush_staleness_threshold_ms` and delegates upload to any live replica
that still holds the arena in memory. S3 PUT is idempotent; all replicas
agree on `arena_id` and therefore on the key.

## Eviction

Global LRU over the SharedArenaPool (Shared class) plus a small cap
(`cache_arenas` per stream, default 4) over each Dedicated stream's own
arena list.

- Arena becomes LRU-eligible once `state = Uploaded` (or, for Memory
  `StorageClass`, immediately on `Sealed`).
- Sealed-but-not-yet-Uploaded arenas are pinned; if too many accumulate,
  writer backpressure kicks in.
- Eviction drops the `Arc<SharedArena>` from `resident`. In-flight readers
  keep the underlying buffer alive via their own `Arc` clone in the
  `OwnedArenaSlice` that backs `Bytes::from_owner`.

## Configuration

New `ExtentNodeConfig` fields:

| Field | Default | Notes |
|---|---|---|
| `shared_arena_size` | 64 MiB | Per-arena buffer size |
| `max_resident_shared_arenas` | 64 | → 4 GiB shared budget |
| `shared_writer_channel_capacity` | 4096 | MPSC bound |
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

Removed: `min_extent_capacity`, `max_extent_capacity`, `extent_growth_factor`,
`cache_extents`, `max_records_per_shared_extent`, `shared_extent_max_age_ms`.
These either become `shared_arena_size` / `cache_arenas` (different unit) or
disappear (no per-extent record/age caps in the new model).

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
| Secondary missed `ForwardInitArena` | Secondary lazily allocates arena on first Forward referencing its id (analogous to Lazy Secondary Extent Creation). |
| Reader finds arena `Evicted` mid-lookup | Falls through to Tier 2. |
| Cross-class mismatch Primary vs Secondary | Impossible: `arena_class` carried on `RegisterEpoch` (SM → Primary) and `ForwardInitEpoch` (Primary → Secondary); any mismatch fails epoch registration. |

## Testing

### Unit Tests

- `SharedArenaPool`: allocate, append, roll, evict.
- `ArenaDirectory`: build, lookup by `(stream, epoch)`, frozen-at-seal.
- `ArenaId`: uniqueness from `(node_id, counter)`; wire round-trip.
- `StreamEpoch`: state machine, `resident_arenas` tracking, seal on epoch
  bump.
- Writer-task roll: in-flight jobs requeue correctly, replies carry
  `rolled_to`.

### Integration Tests

- End-to-end append + read on one EN, RF=1, both classes.
- RF=2: Primary + Secondary agree on `arena_id`; DR flush from secondary
  produces identical S3 key + bytes.
- Epoch spanning 2–3 arenas in both classes: reads work across arena
  boundaries.
- Cold read after eviction: `epoch_arenas` directs to the right object /
  block.
- Runtime promotion mid-epoch: stream starts Shared, rate exceeds
  threshold, subsequent arenas in the same epoch are Dedicated, reads
  work across class boundary.
- Demotion: symmetric.
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
