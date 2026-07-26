# Adaptive Extent Capacity (Heuristic Arena Sizing)

## Problem

Each stream has a fixed `extent_capacity` (default 64 MiB). All extents in a stream use the same arena size. This wastes memory for streams that are mostly idle or have low throughput -- a stream writing 50 KB before idle-seal still consumes the full arena. With thousands of streams, this is infeasible.

## Goal

Start each stream at a small arena (configurable `min_extent_capacity`, default 8 MiB) and double capacity on extent-full, up to `max_extent_capacity` (default 256 MiB). If an extent doesn't reach half-full within 5 minutes, shrink capacity in the next round, freeing memory from both the active extent and the pool.

## Design Decisions

- **Stream-level field on EN**: The `Stream` struct tracks `next_extent_capacity`. Lost on epoch bump, new Primary starts at min and ramps up. Simple, no persistence needed.
- **Growth factor**: 2x doubling (simple start, refine later).
- **Shrink rule (5-minute rule)**: If an extent doesn't reach half-full within 5 minutes, halve `next_extent_capacity` (floor at `min_extent_capacity`), **free pool memory** when extent is completely empty.
- **Per-stream `min_extent_capacity` and `max_extent_capacity`**: Configurable at stream creation time.
- **Tick-as-append**: Both scale-up and scale-down are driven through the existing append code path. User appends drive scale-up (extent-full -> double). A system tick generates a synthetic "tick append" (flagged, carries no payload, not written to the arena) that flows through the same append/leader-election path and triggers scale-down when the extent is under-utilized. No separate seal/shrink code path.

## Semantic Clarification

**`extent_capacity`** always and only means: the associated extent's capacity (the arena size for that specific extent). It appears in `ForwardInitExtent` (which creates secondaries with the Primary's actual extent capacity and carries the authoritative `start_offset`) and `Extent::capacity()`. It does **not** appear in `RegisterExtent`, which is sent only to the Primary and carries stream-level bounds: `min_extent_capacity`, `max_extent_capacity`, `extent_growth_factor`. Stream-level bounds use distinct names: `min_extent_capacity` and `max_extent_capacity`.

## Capacity Scaling Model

### Scale-Up (driven by user appends)

When `seal_and_create_next()` is triggered by extent-full:
- The active extent filled up -> stream needs more space
- `next_extent_capacity = min(current_capacity * 2, max_extent_capacity)`
- New extent created at `next_extent_capacity`

### Scale-Down (driven by tick appends)

A periodic system tick (every 60s with jitter) identifies streams that have been idle > 5 minutes with under-utilized extents. For each such stream, it injects a **tick append** -- a synthetic Append frame with a special flag (`FLAG_SYSTEM_TICK = 0x01` on the Append opcode flags). This tick append enters the normal append path:

1. Tick append arrives at `handle_append` -> leader election via `in_flight.fetch_add`
2. Leader detects the tick flag in `try_append_active` or `do_append_and_respond`
3. Instead of writing to the arena, the leader:
   - Checks the 5-minute rule: `active_extent_created_at.elapsed() > 5 min` AND `bytes_written < capacity / 2`
   - If triggered: calls `seal_and_create_next()` with a "shrink" reason
   - Sends SM notification + ForwardChecksum via existing code paths
   - Returns no AppendAck (tick is fire-and-forget, no client waiting)
   - If not triggered (extent is being used): tick is silently dropped, no effect
4. Followers in the batch (real appends that arrived concurrently) proceed normally on the new extent

This means:
- **Zero new code paths** for seal/notification/forward -- the existing `seal_and_create_next` + `send_extent_update` + `send_forward_checksum` machinery handles everything
- **Correct locking** -- the tick goes through leader election, so no concurrent writer conflict
- **Secondaries learn naturally** -- ForwardInitExtent carries the new (smaller) `extent_capacity`

### Decision Flow

```
try_append_active():
  +-- Normal append -> ExtentFull error
  |   -> seal_and_create_next(reason=ExtentFull)
  |   -> next_capacity = min(current * 2, max)
  |   -> pool: keep (capacity stable for hot streams)
  |   -> retry append on new extent
  |
  +-- Normal append -> OK
  |   -> proceed normally
  |
  +-- Tick append (FLAG_SYSTEM_TICK)
      +-- Already at min capacity AND empty (bytes_written == 0)
      |   -> no-op, drop tick (nothing to reclaim)
      |
      +-- Check: elapsed > 5min AND totally empty (bytes_written == 0)
      |   -> next_capacity = min_extent_capacity (jump to floor)
      |   -> seal_and_create_next(reason=IdleShrink)
      |   -> pool: FREE all pooled extents (stream is idle, reclaim memory)
      |   -> return (no AppendAck)
      |
      +-- Check: elapsed > 5min AND partially filled (0 < fill_ratio < 0.5)
      |   -> next_capacity = max(current / 2, min)
      |   -> seal_and_create_next(reason=IdleShrink)
      |   -> pool: KEEP (resize via realloc when popped)
      |   -> return (no AppendAck)
      |
      +-- Check fails (extent is active enough: fill_ratio >= 0.5 or elapsed < 5min)
          -> drop tick, no-op
```

## Implementation Plan

### 1. Schema: `stream` table -- add min/max columns
**File**: `components/stream-manager/src/metadata.rs`
- SQL migration: rename `extent_capacity` -> `max_extent_capacity`, add `min_extent_capacity`
- Default: `min_extent_capacity = 8 MiB` (8388608), `max_extent_capacity = 256 MiB` (268435456)
- Update all SQL queries that reference `extent_capacity` to use `max_extent_capacity`
- Add `get_stream_capacity_bounds() -> (u32, u32)` returning (min, max)
- `StreamRecord`: replace `extent_capacity` with `min_extent_capacity` + `max_extent_capacity`

### 2. Config constants
**File**: `components/common/src/config.rs`
- Add `DEFAULT_MIN_EXTENT_CAPACITY: u32 = 8 * 1024 * 1024` (8 MiB)
- Rename `DEFAULT_EXTENT_CAPACITY` -> `DEFAULT_MAX_EXTENT_CAPACITY` = `256 * 1024 * 1024` (256 MiB)
- Add `DEFAULT_IDLE_SHRINK_INTERVAL_SECS: u64 = 60`
- Add `DEFAULT_IDLE_SHRINK_THRESHOLD_SECS: u64 = 300` (5 minutes)
- Update all references across the workspace

### 3. Wire protocol changes
**File**: `components/rpc/src/frame.rs`

**Append** (0x03): Add `FLAG_SYSTEM_TICK = 0x01` flag. When set, the append is a system-generated tick for capacity scaling. No payload. No response expected.

**CreateStream** (0x01): Replace `extent_capacity: u32` with:
- `min_extent_capacity: u32` (0 = default 8 MiB)
- `max_extent_capacity: u32` (0 = default 256 MiB)

**RegisterExtent** (0x15): Sent **only to the Primary** Extent Node. Carries stream-level capacity bounds and the SM-assigned authoritative `start_offset` (no per-extent `extent_capacity`):
- `min_extent_capacity: u32` -- stream's floor
- `max_extent_capacity: u32` -- stream's ceiling
- `extent_growth_factor: u8` -- adaptive growth multiplier
- `start_offset: u64` -- SM-authoritative extent start offset

Secondaries are created by the Primary via `ForwardInitExtent`, which carries the per-extent `extent_capacity` and the same authoritative `start_offset`. SM sets the initial extent capacity to `min_extent_capacity` for new and post-epoch-bump extents.

**ForwardInitExtent** (0x05 flag=0x01): Carries the primary's actual `extent_capacity` plus the full adaptive config:
- `extent_capacity: u32` -- this extent's actual arena size
- `min_extent_capacity: u32` -- stream's floor
- `max_extent_capacity: u32` -- stream's ceiling
- `extent_growth_factor: u8` -- adaptive growth multiplier

**UpdateExtentSealed** (0x18 flag=0x00): Add `new_extent_capacity: u32`.

### 4. Stream Manager: control flow
**File**: `components/stream-manager/src/store.rs`

- `handle_create_stream`: parse min/max, store both. Initial `extent_capacity = min_extent_capacity`.
- `seal_allocate_register`: fetch min/max from DB, use `extent_capacity = min_extent_capacity` for new extents. Pass the three bounds + authoritative `start_offset` to `register_primary` (secondaries created by the Primary via `ForwardInitExtent`).
- `handle_extent_update`: record `new_extent_capacity` for observability.

### 5. Stream struct: adaptive capacity state
**File**: `components/extent-node/src/stream.rs`

Add fields:
```rust
min_extent_capacity: u32,
max_extent_capacity: u32,
next_extent_capacity: u32,
active_extent_created_at: Option<Instant>,
```

`register_extent()`: Accept min/max params, store them, set `next_extent_capacity = extent_capacity`, set `active_extent_created_at`.

`seal_and_create_next()`: Now takes a `SealReason` enum:
```rust
pub enum SealReason {
    ExtentFull,   // scale up: double capacity
    IdleShrink,   // scale down: halve capacity or jump to min
}
```
- `ExtentFull`: `next_extent_capacity = min(next * 2, max)`
- `IdleShrink`: If extent is totally empty (`bytes_written == 0`), jump to floor: `next_extent_capacity = min_extent_capacity`, clear pool (`self.extent_pool.clear()` -- free memory, stream is idle). Otherwise gradual halve: `next_extent_capacity = max(next / 2, min)`, keep pool (resize via realloc when popped).

Create new extent at `next_extent_capacity`. Set `active_extent_created_at`. Add `new_extent_capacity` to `SealNotification`.

`create_next_extent()`: Use `self.next_extent_capacity`. Set `active_extent_created_at`.

`should_idle_shrink(threshold: Duration) -> bool`: Read-only check:
- Returns `false` if already at `min_extent_capacity` AND active extent is empty (nothing to reclaim)
- Returns `true` if `active_extent_created_at.elapsed() > threshold` AND active extent `bytes_written < capacity / 2`

### 6. Extent pool: resize via realloc
**File**: `components/extent-node/src/extent.rs`

Add `Extent::resize(new_capacity: u32)`:
- `realloc` the arena buffer to new_capacity
- Reallocate index (`Box<[AtomicU32]>`) with `new_capacity / MIN_RECORD_SIZE` entries via `alloc_zeroed`
- Update `self.capacity`, `self.buf`, `self.index`

In `create_next_extent()`:
```rust
if let Some(mut recycled) = self.extent_pool.pop_front() {
    if recycled.capacity() != self.next_extent_capacity {
        recycled.resize(self.next_extent_capacity);
    }
    recycled.reset(new_id, end_offset, self.epoch);
    self.extents.push(recycled);
} else {
    self.extents.push(Extent::with_capacity(
        new_id, end_offset, self.next_extent_capacity, self.epoch,
    ));
}
```

On `IdleShrink` with empty extent: `self.extent_pool.clear()` to free all pooled memory (stream is fully idle).
On `IdleShrink` with partial fill: pool kept -- pooled extents will be resized via `realloc` when popped in `create_next_extent()`.

### 7. Tick injection in store and bootstrap
**File**: `components/extent-node/src/store.rs`

`handle_append()`: Detect `FLAG_SYSTEM_TICK` on the incoming frame:
- If set: skip payload write. Instead, check `stream.should_idle_shrink(threshold)`
  - If true: proceed with seal_and_create, passing `SealReason::IdleShrink`
  - If false: silently drop, return `None`
  - Either way: no AppendAck response (no client waiting)
- Leader election still happens normally -- tick respects `in_flight` serialization

**File**: `components/extent-node/src/lib.rs`

Spawn a tick injection task:
```rust
tokio::spawn(async move {
    let mut interval = tokio::time::interval(Duration::from_secs(60));
    // Add per-node jitter
    interval.tick().await; // skip first immediate tick
    loop {
        interval.tick().await;
        // Iterate all streams, for each idle one inject tick
        for entry in store.streams.iter() {
            let stream = entry.value();
            if stream.should_idle_shrink(threshold) {
                let tick_frame = Frame::tick_append(stream.id, stream.epoch());
                store.handle_frame(tick_frame, None).await;
            }
        }
    }
});
```

The tick frame is constructed with `FLAG_SYSTEM_TICK`, the stream's current `stream_id` and `epoch`, and no payload.

### 8. Tests

**Unit tests** in `stream.rs`:
- `adaptive_growth_on_extent_full`: register at min, fill, seal_and_create_next(ExtentFull) -> doubles
- `adaptive_cap_at_max`: grow to max, stays at max
- `adaptive_shrink_on_idle`: seal_and_create_next(IdleShrink) with partial fill -> halves
- `adaptive_shrink_empty_jumps_to_min`: completely empty extent -> jumps directly to min_extent_capacity, pool freed
- `adaptive_shrink_noop_at_min_empty`: already at min AND empty -> should_idle_shrink returns false
- `adaptive_floor_at_min`: partially filled at min, stays at min
- `extent_pool_resize`: recycled extent resized when capacity changes via realloc

**Unit tests** in `extent.rs`:
- `resize_basic`: verify arena realloc, index size, capacity

**Unit tests** in `store.rs`:
- `tick_append_triggers_shrink`: inject tick frame for idle stream -> seals + shrinks
- `tick_append_noop_active_stream`: inject tick for active stream -> no-op

## File Change Summary

| File | Changes |
|------|---------|
| `components/common/src/config.rs` | Add `DEFAULT_MIN_EXTENT_CAPACITY`, rename -> `DEFAULT_MAX_EXTENT_CAPACITY`, idle-shrink constants |
| `components/extent-node/src/extent.rs` | Add `Extent::resize()` with arena `realloc` + index reallocation |
| `components/extent-node/src/stream.rs` | Add min/max/next capacity + `active_extent_created_at`; `SealReason` enum; update seal/create; `should_idle_shrink`; pool resize/clear; `SealNotification.new_extent_capacity` |
| `components/extent-node/src/store.rs` | Handle `FLAG_SYSTEM_TICK` in append path; pass `SealReason` through seal_and_create |
| `components/extent-node/src/lib.rs` | Spawn tick injection task with jitter |
| `components/rpc/src/frame.rs` | `FLAG_SYSTEM_TICK` on Append; min/max on `RegisterExtent`/`CreateStream`; `new_extent_capacity` on `UpdateExtentSealed` |
| `components/stream-manager/src/metadata.rs` | Rename column, add `min_extent_capacity`, update queries/`StreamRecord` |
| `components/stream-manager/src/store.rs` | Pass min/max; initial capacity = min; record `new_extent_capacity` |
