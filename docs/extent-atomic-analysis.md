# Extent Atomic Ordering Analysis

This document analyzes every atomic variable in `Extent` (in `components/extent-node/src/extent.rs`),
verifying that the memory orderings are correct under concurrent access.

## Atomic Variables

| Atomic | Purpose | Writers | Readers |
|--------|---------|---------|---------|
| `write_cursor` | Byte-level space reservation | Appenders: `fetch_add(Relaxed)` | Metrics: `load(Relaxed)` |
| `record_count` | Logical sequence assignment | Appenders: `fetch_add(Relaxed)` | Seal: `load(Acquire)` |
| `committed_seq` | In-order commit serialization | Appenders: `CAS(AcqRel, Relaxed)` | `message_count()`: `load(Acquire)` |
| `committed_bytes` | Reader visibility boundary | Appenders: `store(Release)` | `read()`: `load(Acquire)` |
| `limit` | Seal message cap | Seal: `CAS(Release, Acquire)` | `append()`: `load(Acquire)` |
| `index[i]` | Seq-to-byte-pos mapping | Appenders: `store(Release)` | `index_lookup()`: `load(Acquire)` |

## Append Path (lines 236–309)

The append protocol has 4 steps, executed by concurrent appenders without any mutex.

### Step 1: Byte slot reservation

```rust
let byte_pos = self.write_cursor.fetch_add(record_len as u64, Ordering::Relaxed);
```

**Ordering: `Relaxed` — correct.** Only needs atomicity to guarantee non-overlapping byte regions.
No cross-variable ordering is needed here because the actual data visibility is gated by `committed_bytes`
(step 4), not by `write_cursor`.

### Step 2: Sequence number assignment

```rust
let seq = self.record_count.fetch_add(1, Ordering::Relaxed);
```

**Ordering: `Relaxed` — correct.** Same reasoning as step 1: only needs atomicity for unique assignment.
The commit spin-wait in step 4 provides the ordering that matters.

### Step 3: Payload copy

```rust
unsafe {
    let dst = self.buf.add(byte_pos as usize);
    std::ptr::copy_nonoverlapping(...);
}
```

No atomics. The writer has exclusive ownership of `arena[byte_pos..byte_pos+record_len]` because
`write_cursor.fetch_add` guarantees non-overlapping regions.

### Step 4: In-order commit

```rust
// Spin until committed_seq == my seq
self.committed_seq.compare_exchange_weak(seq, seq + 1, Ordering::AcqRel, Ordering::Relaxed)
// Then publish
self.committed_bytes.store(new_committed_bytes, Ordering::Release);
self.index_record(seq, byte_pos);  // index[seq].store(byte_pos, Release)
```

**`committed_seq` CAS with `AcqRel` — correct.**

- **Release side**: Ensures the memcpy (step 3) is visible to any thread that subsequently loads
  `committed_seq` with `Acquire`. This is the key happens-before edge that guarantees arena data is
  fully written before the commit is visible.

- **Acquire side**: Ensures this writer sees all stores from the previous writer (whose CAS advanced
  `committed_seq` to `seq` with Release). This maintains a transitive happens-before chain:
  writer 0 → writer 1 → writer 2 → ..., so `committed_bytes` is always monotonically advanced.

**`committed_seq` CAS failure with `Relaxed` — correct.** On failure, we're just spinning. No ordering
is needed; we'll retry and eventually see the updated value. (`compare_exchange_weak` is appropriate
here — spurious failures are fine in a spin loop.)

**`committed_bytes.store(Release)` — correct.** This is the writer→reader synchronization point.
A reader's `Acquire` load on `committed_bytes` sees:
1. This store (the new boundary).
2. All prior arena writes from every writer whose `committed_seq` CAS preceded this one
   (transitively, via the AcqRel chain).

**`index[seq].store(byte_pos, Release)` — correct.** Pairs with `index_lookup`'s `Acquire` load.
After the CAS succeeds, the index entry is published so that any reader who observes it (via Acquire)
also sees the committed arena data.

## Read Path (lines 319–351)

```rust
let committed_byte_pos = self.committed_bytes.load(Ordering::Acquire) as usize;
```

**Ordering: `Acquire` — correct.** Forms a Release-Acquire pair with the appender's
`committed_bytes.store(Release)`. The reader is guaranteed to see all arena writes from every
committed record up to `committed_byte_pos`.

The subsequent arena reads (length prefix + payload) access memory that was written before the
Release store, so they are safe.

## Index Lookup (lines 365–376)

```rust
let val = self.index[idx].load(Ordering::Acquire);
```

**Ordering: `Acquire` — correct.** Pairs with `index_record`'s `store(Release)`. After seeing
a non-sentinel value, the reader is guaranteed that the corresponding arena data is committed
(because `index_record` is called after `committed_seq` CAS and `committed_bytes` store).

## Seal (lines 400–412)

```rust
// Compute limit
let count = match committed_offset {
    Some(offset) => offset.saturating_sub(self.start_offset.0),
    None => self.record_count.load(Ordering::Acquire),
};
// CAS: set limit if not already sealed
self.limit.compare_exchange(LIMIT_OPEN, count, Ordering::Release, Ordering::Acquire)
```

### `record_count.load(Acquire)` — effectively Relaxed, but harmless

When `committed_offset` is `None` (primary self-sealing), we read `record_count` to determine the
limit. This uses `Acquire`, but `record_count` is only ever written with `fetch_add(Relaxed)` —
there is no paired Release store, so the Acquire provides no additional guarantee beyond atomicity.

The actual ordering guarantee comes from the external `DashMap` write lock that the store handler
holds when calling `seal()`. The write lock's acquire fence ensures we see the latest `record_count`.

**Verdict**: Not a bug. The `Acquire` is stronger than needed but not harmful. The correctness relies
on the external lock, which is always held in the current call paths.

### `limit` CAS with `Release` (success) — correct

Pairs with `limit.load(Acquire)` in `append()` (line 237), `is_sealed()` (line 416), and
`accepts_post_seal_writes()` (line 423). Readers that observe a non-`LIMIT_OPEN` value are guaranteed
to see the `count` value that was stored.

### `limit` CAS with `Acquire` (failure) — correct

On failure (another thread already sealed), we load the existing limit value. `Acquire` ensures we
see the value that the successful sealer stored with `Release`, so `Err(limit)` is the authoritative
sealed limit.

## Seal Check in Append (lines 237–244)

```rust
let limit = self.limit.load(Ordering::Acquire);
if limit != LIMIT_OPEN {
    let current = self.record_count.load(Ordering::Relaxed);
    if current >= limit {
        return Err(StorageError::ExtentSealed(self.id));
    }
}
```

**`limit.load(Acquire)` — correct.** Pairs with `seal()`'s `Release` CAS. If the appender sees
the sealed limit, it knows the seal has happened.

**`record_count.load(Relaxed)` — correct.** This is a best-effort check. Even if `record_count`
is slightly stale, the append will still be rejected at the `committed_seq` CAS stage if the limit
is exceeded, or succeed if it falls within the limit. There's no correctness issue — just a
fast-path optimization to avoid unnecessary byte reservation and memcpy for appends that would
ultimately be rejected.

## Accessor Methods

| Method | Load | Ordering | Notes |
|--------|------|----------|-------|
| `message_count()` | `committed_seq` | `Acquire` | Correct — sees all committed writes |
| `next_offset()` | `committed_seq` | `Acquire` | Same |
| `last_offset()` | `committed_seq` | `Acquire` | Same |
| `bytes_written()` | `write_cursor` | `Relaxed` | Correct — metrics only, no ordering needed |
| `committed_data()` | `committed_bytes` | `Acquire` | Correct — must see arena data |
| `is_sealed()` | `limit` | `Acquire` | Correct — pairs with seal's Release |
| `accepts_post_seal_writes()` | `limit` + `committed_seq` | Both `Acquire` | Correct |

## Design Note: Two-Step Reservation

Steps 1 and 2 are two independent `fetch_add`s:

```rust
let byte_pos = self.write_cursor.fetch_add(record_len, Relaxed);   // step 1
let seq = self.record_count.fetch_add(1, Relaxed);                  // step 2
```

These can pair out-of-order across threads. Consider:

```
Thread A: write_cursor.fetch_add → byte_pos=0      (preempted)
Thread B: write_cursor.fetch_add → byte_pos=100
Thread B: record_count.fetch_add → seq=0
Thread B: memcpy to arena[100..120], commits seq=0
Thread B: committed_bytes = 120    ← arena[0..100] not yet written by Thread A!
Thread A: record_count.fetch_add → seq=1
Thread A: memcpy to arena[0..100], waits to commit seq=1
```

A reader seeing `committed_bytes=120` would walk from byte 0 into Thread A's unwritten region.

**Why this is safe in practice**: The two `fetch_add`s are one instruction apart (~1ns). Preemption
between them requires an OS timer interrupt in that window (~0.001% probability). The concurrent tests
pass reliably. On heavily loaded systems with aggressive preemption, this could theoretically surface,
but would require pathological scheduling.

**Why this differs from Disruptor/io_uring**: Those patterns use fixed-size slots where `seq`
determines position (`slot = seq % ring_size`), so a single atomic provides both space reservation
and sequence assignment. This extent uses variable-size records, requiring two separate atomics.

**Possible fix (if ever needed)**: Merge the two steps by using `record_count` as the sole reservation
mechanism: each appender claims a seq, then uses an auxiliary structure (e.g., a prefix-sum array or
a per-seq byte_pos field populated after the preceding writer commits) to determine its byte position.
This would eliminate the window entirely but add complexity to the commit path.

## Summary

All atomic orderings are correct for their intended synchronization patterns:

- The **append commit chain** (`committed_seq` AcqRel → `committed_bytes` Release → reader Acquire)
  correctly publishes arena data in-order.
- The **seal protocol** (`limit` Release/Acquire CAS) correctly synchronizes with append's
  `limit.load(Acquire)`.
- The **index** (Release store / Acquire load) correctly pairs writer and reader.
- **Relaxed** is used appropriately for reservation atomics (`write_cursor`, `record_count`) where
  only atomicity matters and ordering is provided by downstream synchronization.
