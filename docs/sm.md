# Multi-SM High Availability: Design Analysis

## Goal

Run multiple Stream Manager (SM) nodes against the same MySQL database to
eliminate SM as a single point of failure. Although SM is off the critical
append path (ENs autonomously create extents on extent-full), SM is still
needed for stream creation, client-initiated seals, node failover, and
metadata queries. An SM outage degrades these operations until recovery.

This document catalogues every design decision and implementation detail
that currently assumes a single SM instance, ranked by severity.

---

## Critical Issues

### 1. Each EN connects to exactly one SM

`components/common/src/config.rs:49`:
```rust
pub stream_manager_addr: String,   // single address
```

`components/extent-node/src/stream_manager_client.rs:68`:
```rust
stream_manager_addr: String,       // passed to reconnection loop
```

The EN config takes a single SM address. The `StreamManagerClient`
background task connects to that address, sends `Connect`, and then loops
sending `Heartbeat` + `UpdateExtent` frames to that one SM forever
(reconnecting on failure, 2 s backoff).

**Consequences with multiple SMs:**
- Only the connected SM receives heartbeats and updates allocator metrics.
- Other SMs have no record of the EN in their in-memory `Allocator`
  (they _do_ see it in MySQL via `register_node` upsert, but with no
  live metrics).
- `UpdateExtentSealed` and `UpdateExtentProgress` fire-and-forget frames
  only reach the connected SM. Other SMs have stale extent metadata
  until the next reconciliation (which only runs at startup today).

**Remediation options:**
- **VIP / load-balancer**: All ENs point to a single VIP; on SM failover
  the VIP floats to the standby. Simplest option for active-passive.
- **EN multi-connect**: EN maintains connections to all SMs (or a
  configured list) and sends heartbeats/notifications to each. Needed
  for active-active.
- **Shared notification bus**: ENs publish `UpdateExtent` to a durable
  topic (e.g. MySQL-based event table, Kafka). All SMs consume.

---

### 2. Heartbeat checker runs on every SM — failover races

`components/stream-manager/src/lib.rs:84-90` spawns a heartbeat checker
on every SM instance. Each checker independently calls
`check_expired_nodes` every `heartbeat_check_interval_ms` (default 3 s).

`components/stream-manager/src/heartbeat_checker.rs:61-143` performs
this sequence per expired node:

```
mark_node_dead(node_id)           // idempotent UPDATE
  for each active extent on dead node:
    resolve_committed_offset()    // contacts surviving replicas
    bump_epoch(stream_id)         // unconditional epoch + 1
    seal_allocate_register()      // seal + create successor extent
```

**Race scenario (two SMs, same dead node):**

1. Both SMs query `get_expired_nodes()` and see the same node.
2. Both call `mark_node_dead` — harmless, idempotent.
3. For the same stream, both call `bump_epoch`:

   `components/stream-manager/src/metadata.rs:863`:
   ```rust
   "UPDATE stream SET epoch = epoch + 1 WHERE stream_id = ?"
   ```
   No CAS guard. If both execute, epoch increments twice.

4. Both call `seal_allocate_register`. The `seal_and_allocate_transaction`
   uses `SELECT ... FOR UPDATE` on the extent row (`metadata.rs:337`),
   so one SM will seal and the other will see `AlreadySealed` and return
   the successor — **the seal itself is safe**. But the double epoch bump
   means clients may see an unexpected epoch jump, and the second SM's
   `RegisterExtent` to the new Primary carries a mismatched epoch.

**Remediation options:**
- **Leader-only heartbeat checker**: Only the SM holding a lease runs the
  heartbeat checker (see Issue 4 below).
- **Fenced failover**: Wrap the per-stream failover in a transaction that
  locks the stream row and checks whether failover was already performed
  (e.g. check if the extent is already sealed before bumping epoch).

---

### 3. `UpdateExtent` notifications only reach one SM

`components/extent-node/src/stream_manager_client.rs:350-402`:

When the Primary EN autonomously seals an extent (extent-full), it sends
`UpdateExtentSealed` as a fire-and-forget frame to its connected SM.
After each heartbeat it also sends `UpdateExtentProgress` for all active
extents.

If a client's `Seal` or `DescribeExtent` request arrives at a different
SM, that SM's metadata may be stale — it won't know about the newly
created successor extent until:
- The EN's `record_extent_progress` updates `end_offset` in MySQL (only
  for progress, not for new extents), or
- The SM runs reconciliation (startup only).

**Severity**: For the autonomous extent creation path, this means the
non-connected SM may try to allocate a duplicate successor extent or
return stale extent info to a client.

**Remediation options:**
- Same as Issue 1 (shared bus or multi-connect).
- Alternatively, SMs can re-read extent state from MySQL before acting
  on a seal request, rather than relying on cached in-memory state.
  (SM is already DB-backed, so this is a minor change.)

---

## Medium Severity Issues

### 4. No SM leader election or lease mechanism

There is no concept of SM leadership today. Every SM instance is fully
active: accepting client RPCs, running the heartbeat checker, handling
`Connect` from ENs. This makes Issues 2 and 3 possible.

A leader lease would partition responsibilities:
- **Leader**: Runs heartbeat checker, receives EN connections (or all SMs
  receive connections but only leader acts on failover).
- **Followers**: Forward or reject write operations, serve read-only
  metadata queries.

Implementation sketch (DB-based lease):
```sql
CREATE TABLE stream_manager_leadership (
  id          INT PRIMARY KEY DEFAULT 1,
  node_id     VARCHAR(255) NOT NULL,
  lease_until DATETIME NOT NULL
);

-- Acquire: INSERT ... ON DUPLICATE KEY UPDATE
--          WHERE lease_until < NOW() OR node_id = ?
-- Renew:   UPDATE ... SET lease_until = NOW() + interval
--          WHERE node_id = ?
```

---

### 5. In-memory allocator metrics diverge across SMs

`components/stream-manager/src/allocator.rs:13-17`:
```rust
pub struct Allocator {
    node_metrics: HashMap<String, NodeMetrics>,
}
```

Updated only from heartbeats (`update_metrics`, called from
`handle_heartbeat` in `store.rs`). Since ENs heartbeat to one SM, other
SMs have empty or partial metrics. The `score_node` function returns a
neutral 0.5 for unknown nodes (`allocator.rs:56`), so they'll still be
selected — but without real load awareness, leading to potential
hotspots.

**Remediation options:**
- Persist metrics to a `node_metrics` table on each heartbeat; read from
  DB in `pick_nodes`. Adds ~1 ms per heartbeat write.
- With leader-only allocation, only the leader needs accurate metrics.

---

### 6. Reconciliation runs only at startup

`components/stream-manager/src/store.rs:183-316`:
```rust
pub async fn reconcile_on_startup(&self) { ... }
```

Called once in `lib.rs:75`. Queries each Primary EN via `ReportExtents`
and upserts missing extents into MySQL. There is no periodic background
reconciliation.

If SM-1 is running and SM-2 starts, SM-2 reconciles, but SM-1 never
refreshes. In a single-SM world this is fine. With multiple SMs, the
long-running SM accumulates stale state over time.

**Remediation**: Run reconciliation periodically (e.g. every 60 s) or
make all SM operations read-through to MySQL rather than relying on stale
in-memory state. (SM already reads from MySQL for most operations; the
main gap is the allocator metrics.)

---

## Already Safe

These aspects of the current design are safe under concurrent SM access:

| Component | Why it's safe |
|-----------|--------------|
| `seal_and_allocate_transaction` | Uses `SELECT ... FOR UPDATE` on the extent row; handles `AlreadySealed` idempotently (`metadata.rs:337-469`). |
| `register_node` | Uses `INSERT ... ON DUPLICATE KEY UPDATE` — pure upsert, no race (`metadata.rs:766-787`). |
| `mark_node_dead` | Idempotent `UPDATE node SET state = Dead` (`metadata.rs:828-836`). |
| `record_extent_progress` | Transaction validates epoch with `FOR UPDATE`; skips stale; monotonic offset guard `end_offset < ?` (`metadata.rs:987-1046`). |
| `reconcile_extents` | Uses `INSERT ... ON DUPLICATE KEY UPDATE` with conditional state transitions (`metadata.rs:1052-1118`). |
| Stream/extent ID allocation | `stream_sequence.next_extent_id` incremented inside the seal transaction under the extent row lock. |

---

## Recommended Approach: Active-Passive with DB Lease

Given that SM is off the critical path and the main goal is availability
(not horizontal throughput scaling), the simplest architecture is:

```
  ┌─────────┐   ┌─────────┐
  │  SM-1   │   │  SM-2   │    Both connect to MySQL.
  │ (active)│   │(standby)│    SM-1 holds the leader lease.
  └────┬────┘   └────┬────┘
       │              │
       └──────┬───────┘
              │ VIP or DNS
       ┌──────┴───────┐
       │   EN fleet   │
       └──────────────┘
```

### Changes required

| # | Change | Scope |
|---|--------|-------|
| 1 | **Add `stream_manager_leadership` table and lease logic** | New: `metadata.rs` lease acquire/renew/release |
| 2 | **Gate heartbeat checker on lease ownership** | `heartbeat_checker.rs`: skip `check_expired_nodes` if not leader |
| 3 | **Fence `bump_epoch`** | `metadata.rs`: use CAS (`WHERE epoch = ?`) instead of unconditional increment |
| 4 | **EN config accepts SM address list** | `config.rs`: change to `Vec<String>`, try each on connect failure |
| 5 | **Periodic reconciliation** | `lib.rs` / `store.rs`: spawn background reconciliation every N seconds |
| 6 | **(Optional) Persist allocator metrics** | `allocator.rs` / `metadata.rs`: write metrics to DB, read on `pick_nodes` |

Changes 1-3 are required for correctness. Changes 4-5 improve failover
speed. Change 6 is a refinement for better load balancing.

### Failover sequence

1. SM-1 crashes (or its lease expires).
2. SM-2's lease renewal loop detects the lease is available; acquires it.
3. SM-2 starts the heartbeat checker.
4. ENs reconnect to SM-2 (via VIP failover or address-list retry).
5. SM-2 runs reconciliation to catch up on any missed `UpdateExtent`
   notifications.
6. Normal operation resumes.

No data is lost because all durable state lives in MySQL. The gap is the
failover window during which no heartbeat checker runs and no new streams
can be created — bounded by `lease_duration + EN reconnect time`.

---

## Alternative: Fully Stateless Multi-Active SMs

### Current state analysis

The SM is **almost stateless already**. Examining all in-memory state:

| Location | Field | Purpose | Stateless? |
|----------|-------|---------|------------|
| `allocator.rs:16` | `node_metrics: HashMap<String, NodeMetrics>` | Runtime load metrics for placement | **No** — only in-memory state that matters |
| `store.rs:140` | `default_replication_factor: usize` | Server default RF | Yes — config, immutable |
| `lib.rs:28-32` | `addr`, `shutdown_tx`, `task_handles` | Process lifecycle | N/A — not persistent state |
| `metadata.rs:67` | `pool: MySqlPool` | DB connection pool | N/A — infrastructure |

**Key finding**: The only mutable in-memory state is `Allocator.node_metrics`.
Everything else — streams, extents, replicas, node liveness — is already
read from MySQL on every operation. The SM does not cache metadata.

### Making SM fully stateless

To eliminate the `node_metrics` cache, persist metrics to MySQL:

```sql
CREATE TABLE node_metrics (
  node_id                VARCHAR(255) PRIMARY KEY,
  available_memory_bytes BIGINT UNSIGNED NOT NULL,
  total_memory_bytes     BIGINT UNSIGNED NOT NULL,
  appends_per_sec        INT UNSIGNED NOT NULL,
  active_extent_count    INT UNSIGNED NOT NULL,
  bytes_written_per_sec  BIGINT UNSIGNED NOT NULL,
  updated_at             DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

**Changes required:**

| # | Change | File | Description |
|---|--------|------|-------------|
| 1 | Persist metrics on heartbeat | `metadata.rs` | `INSERT ... ON DUPLICATE KEY UPDATE` into `node_metrics` |
| 2 | Read metrics in `pick_nodes` | `allocator.rs` | Query `node_metrics` table instead of in-memory HashMap |
| 3 | Remove `node_metrics` HashMap | `allocator.rs` | Delete the field; `Allocator` becomes stateless |
| 4 | Add DB-based leader lease | `metadata.rs` | For heartbeat checker and proactive actions |
| 5 | Fence `bump_epoch` | `metadata.rs` | CAS guard to prevent double-bump |

**Result**: Any SM can handle any request by reading current state from MySQL.
Only the lease holder runs proactive actions (heartbeat checker, failover).

### Trade-offs

| Aspect | In-memory metrics | DB-backed metrics |
|--------|-------------------|-------------------|
| Heartbeat latency | ~0 ms (HashMap insert) | ~1 ms (MySQL write) |
| `pick_nodes` latency | ~0 ms (local read) | ~1-2 ms (MySQL query) |
| Consistency | Divergent across SMs | Single source of truth |
| Restart recovery | Metrics lost until heartbeats arrive | Metrics survive restart |
| Complexity | Simple | Slightly more complex |

For a system where SM is off the critical path and heartbeats are every
500 ms–1 s, the additional 1–2 ms latency is negligible. The consistency
benefit is significant for multi-SM deployments.

### Stateless SM architecture

```
                    ┌─────────────────────────────────────┐
                    │              MySQL                  │
                    │  ┌─────────┐ ┌─────────┐ ┌────────┐ │
                    │  │ stream  │ │ extent  │ │  node  │ │
                    │  │ replica │ │ metrics │ │ leader │ │
                    │  │         │ │         │ │  ship  │ │
                    │  └─────────┘ └─────────┘ └────────┘ │
                    └─────────────────────────────────────┘
                           ▲           ▲           ▲
                           │           │           │
              ┌────────────┼───────────┼───────────┼────────────┐
              │            │           │           │            │
        ┌─────┴─────┐ ┌────┴────┐ ┌────┴────┐ ┌────┴────┐ ┌─────┴─────┐
        │   SM-1    │ │  SM-2   │ │  SM-3   │ │  EN-1   │ │   EN-2    │
        │  (leader) │ │         │ │         │ │         │ │           │
        │           │ │         │ │         │ │         │ │           │
        │ heartbeat │ │  query  │ │  query  │ │  HB to  │ │   HB to   │
        │  checker  │ │  only   │ │  only   │ │   any   │ │    any    │
        └───────────┘ └─────────┘ └─────────┘ └─────────┘ └───────────┘
```

- **All SMs** can handle client requests (CreateStream, Seal, Describe, etc.)
  by reading/writing MySQL directly.
- **Only the leader** runs the heartbeat checker and executes failover.
- **ENs** can heartbeat to any SM (or all SMs via broadcast/multicast);
  metrics are persisted to `node_metrics` table.
- **Leader election** uses the `stream_manager_leadership` table with lease TTL.

### Why this works

1. **Seal requests**: `seal_and_allocate_transaction` already uses
   `SELECT ... FOR UPDATE` — concurrent seals from multiple SMs are
   serialized at the DB level.

2. **Stream creation**: Each `create_stream` inserts into `stream` table
   with auto-increment ID; no conflict.

3. **Extent allocation**: `stream_sequence.next_extent_id` is incremented
   inside the seal transaction under row lock.

4. **Node registration**: `register_node` uses upsert; harmless if
   multiple SMs receive the same `Connect`.

5. **Heartbeat/liveness**: `update_heartbeat` sets `last_heartbeat = NOW()`;
   idempotent, latest writer wins.

6. **Failover**: Only the leader runs `check_expired_nodes`. The fenced
   `bump_epoch` (CAS) prevents double-bump even if leadership changes
   mid-failover.

### Recommendation

The stateless approach is cleaner for long-term maintainability:

- No divergent in-memory state to debug
- Any SM can serve any request — true horizontal scaling for reads
- Simpler mental model: MySQL is the single source of truth

The ~1-2 ms additional latency per heartbeat and allocation is acceptable
given SM is off the critical append path. Implement this alongside the
leader lease for proactive actions.
