# Open Design Issues

This document captures foreseeable design problems and proposed solutions that need further discussion before implementation proceeds to the next phase. The storage layer is intended as common infrastructure for multiple cloud products (Kafka, MQTT, AMQP), which shapes the requirements significantly.

---

## Issue 1: Memory Efficiency for the Long Tail of Streams

### Problem

The current design pre-allocates a contiguous arena (default 64 MiB) per active extent. With tens of thousands of streams — common when serving as shared infrastructure — this doesn't scale:

| Scenario | Arena Size | Total Memory (10K streams) |
|----------|-----------|---------------------------|
| Default | 64 MiB | **640 GB** — impossible |
| Small | 1 MiB | 10 GB — still wasteful for mostly-idle streams |
| Tiny | 64 KB | 640 MB — feasible, but defeats the purpose for hot streams |

The `extent_capacity` is already per-stream configurable, but the fundamental issue is that a pre-allocated arena per stream doesn't scale when most streams are nearly idle. A typical deployment has a few hundred hot streams (high-throughput Kafka partitions) and thousands of warm/cold streams (MQTT topics, low-traffic AMQP queues) with only a handful of appends per second.

### Proposed Solution: Tiered Stream Classes

Introduce explicit stream tiers that govern arena sizing and lifecycle:

**Tier 1 — Hot streams** (hundreds of appends/sec): Current design unchanged. Dedicated arena (e.g. 64 MiB), pipelined group commit, broadcast replication. The protocol adapter layer knows Kafka partitions are hot and configures them accordingly.

**Tier 2 — Warm streams** (a few appends/sec): Small dedicated arenas (256 KB–1 MB) with aggressive time-based seal (e.g. 5–10 seconds of inactivity triggers seal + arena free). Arena is allocated lazily on first append after eviction — the stream metadata exists, but no memory is held when idle. Requirements:
- A **time-based idle seal** mechanism (not just extent-full or failure-triggered)
- **Lazy arena allocation**: `RegisterExtent` creates the metadata entry but doesn't allocate the arena until the first actual append
- The extent-full path already handles "no active extent -> create one"; the idle-seal path is the reverse

**Tier 3 — Cold/bursty streams** (long idle periods, occasional burst): No arena held at all. On append arrival, allocate a small arena, write, and schedule aggressive seal. Effectively "allocate on demand, reclaim eagerly."

### Configuration Approach

Start with **explicit configuration** — a `stream_class` or `tier` field in `CreateStream`. The protocol adapter layer has enough information to choose (Kafka partitions = hot, MQTT retained-message topics = cold). Auto-promotion/demotion based on observed append rate can be added later.

### Open Questions

- What idle timeout thresholds make sense for warm streams? 5s? 30s? Should it be configurable per stream?
- Should tier transitions (hot -> warm -> cold) be automatic or operator-driven?
- How does lazy arena allocation interact with the epoch-based autonomous extent creation? The Primary needs an arena before it can accept appends, but allocation adds latency to the first append after idle.

---

## Issue 2: Extent Count Explosion Under Time-Based Seal

### Problem

If we introduce time-based idle seal for warm/cold streams (Issue 1), the extent count grows rapidly:

- A stream with 5 appends/sec sealed every 10 seconds = 8,640 extents/day
- 10K streams x 8,640 extents/day = **86M extent rows/day** in MySQL

This creates MySQL metadata pressure and S3 small-object proliferation.

### Proposed Solution: Extent Compaction

A background process that merges small sealed extents into larger ones:

```
Before compaction:  extent_0 (50 records) -> extent_1 (30 records) -> extent_2 (45 records)
After compaction:   extent_0' (125 records)  [original three deleted]
```

The compaction target size should match the ideal S3 object size (64–256 MB). Similar to LSM-tree compaction or Kafka log segment compaction.

**Implementation sketch:**
1. Leader-only background task (like heartbeat checker) scans for compaction candidates.
2. Candidates: consecutive sealed/flushed extents whose combined size is below a threshold.
3. Merge: read all records from candidate extents, write a single new extent to S3, update metadata atomically (MySQL transaction: insert new extent, delete old extents and replicas).
4. Delete old S3 objects after metadata commit.

### Batched Metadata Updates

For fire-and-forget notifications like `UpdateExtentSealed`, Stream Manager could batch-insert to MySQL rather than one transaction per notification. This reduces MySQL round-trips under high seal rates.

### Open Questions

- Should compaction be synchronous (block further seals on those extents) or asynchronous (allow reads from old extents during merge)?
- What is the compaction trigger: extent count per stream, total small-extent count, or scheduled interval?
- How does compaction interact with consumer offset tracking? Offsets are logical and should be stable, but the extent_id in offset bookmarks changes.

---

## Issue 3: Multi-Protocol Feature Gap Analysis

### Problem

The storage layer is intended to serve Kafka, MQTT, and AMQP. Each protocol has different requirements beyond basic append-and-read:

| Feature | Kafka | MQTT | AMQP | Current Status |
|---------|-------|------|------|----------------|
| High-throughput append | Core | Rare | Varies | Supported |
| Millions of topics/queues | ~thousands | Millions | Thousands | Not feasible (Issue 1) |
| Consumer group offsets | Yes | No | No | `stream_offset` table exists, no group coordination |
| Per-message ACK (not offset) | No | QoS1/2 | Yes | Not supported (offset-based only) |
| Retained messages | No | Last msg/topic | No | Not supported |
| Message TTL | Segment-level | Per-message | Per-message + per-queue | Not supported |
| Dead letter / redelivery | No | No | Yes | Not supported |
| Wildcard subscription | No | Topic filter | Routing key | Not supported (protocol layer concern?) |
| Message dedup (idempotent) | Yes | QoS2 | No | Not supported |
| Priority queues | No | No | Yes | Not supported |

The sub-issues below address the most impactful gaps.

---

## Issue 3a: TTL and Retention Policy

### Problem

Without retention, storage grows unboundedly. Every protocol needs some form of data lifecycle management.

### Proposed Solution

Per-stream retention policy:

```sql
ALTER TABLE stream ADD COLUMN retention_seconds BIGINT DEFAULT -1;  -- -1 = infinite
ALTER TABLE stream ADD COLUMN retention_bytes   BIGINT DEFAULT -1;
```

A **GC background task** (leader-only, like heartbeat checker) periodically scans sealed/flushed extents and deletes those past retention:
- **Time-based**: delete extents where `sealed_at + retention_seconds < NOW()`
- **Size-based**: for a stream exceeding `retention_bytes`, delete oldest extents until within budget

Deletion sequence: remove S3 object (if flushed) -> remove MySQL metadata (extent, replicas) -> done.

### Open Questions

- Per-message TTL (needed for MQTT/AMQP) requires the storage layer to understand message timestamps. This depends on Issue 3d (structured message envelope).
- Should retention be enforced at the extent granularity (simpler) or message granularity (more precise but requires partial extent rewriting)?
- How does retention interact with compaction (Issue 2)? Compacted extents should inherit the latest `sealed_at` of their constituents, or retention is applied per-record during compaction.

---

## Issue 3b: Per-Message ACK Tracking

### Problem

Kafka consumers commit offsets: "I've consumed up to offset N." MQTT QoS1/2 and AMQP require individual message acknowledgment, potentially out of order. The current offset-based model (`stream_offset` table) cannot express "I've consumed messages 1, 3, 5 but not 2, 4."

### Proposed Approaches

**Approach A — ACK bitmap per consumer**: Per-consumer, track which offsets have been individually ACKed as a compressed bitmap alongside the committed offset. The consumer's "effective offset" advances when all messages below it are ACKed (like TCP's cumulative ACK with SACK).

**Approach B — Index stream per consumer + offset advance**: The multi-dispatch index stream design already supports per-consumer streams. Each consumer has its own index stream. ACK = advance that consumer's offset. Redelivery = re-read from the un-ACKed offset. The protocol adapter (MQTT/AMQP layer above stream-store) maintains per-message ACK state and translates it to offset advances on the index stream.

Approach B aligns with the existing design and keeps the storage layer simple (offset-based). The per-message ACK complexity lives in the protocol adapter, not the storage layer.

### Open Questions

- Does Approach B handle the "ACK message 5 before message 3" case efficiently? The index stream offset can't advance past 3 until 3 is ACKed, so messages 4–5 would be redelivered on reconnect.
- Should the storage layer provide a native "ACK bitmap" primitive, or is this strictly a protocol adapter concern?
- What is the redelivery strategy: re-read from the lowest un-ACKed offset (simple, may redeliver already-ACKed messages) or maintain a precise un-ACKed set (complex, but no redundant redelivery)?

---

## Issue 3c: Retained Messages (MQTT)

### Problem

MQTT retained messages = "last message published on this topic." New subscribers to a topic immediately receive the retained message. This is a KV store pattern (topic -> message), not an append-only log.

### Proposed Approaches

**Approach A — Special single-record stream**: A stream with `retention_count = 1` (only keep the latest record). Each publish overwrites the previous. Reuses existing stream infrastructure but is awkward — seal-and-new for every single message.

**Approach B — Separate KV table in MySQL**: Retained messages are typically small and infrequently updated. A simple table:

```sql
CREATE TABLE retained_message (
    topic       VARCHAR(512) PRIMARY KEY,
    payload     MEDIUMBLOB NOT NULL,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
```

Approach B is simpler and more natural for this access pattern. The protocol adapter writes retained messages directly to MySQL, bypassing the stream layer entirely.

### Open Questions

- If retained messages can be large (MQTT spec allows up to 256 MB), does the MySQL approach still work? Would S3 be needed for large retained payloads?
- Should retained messages participate in replication, or is MySQL replication sufficient?
- What about retained message TTL (MQTT 5 feature)?

---

## Issue 3d: Structured Message Envelope

### Problem

The current arena stores raw payload bytes with a simple `[len][payload]` wire format. For multi-protocol support, the storage layer needs to understand message-level metadata (timestamp, TTL, message ID, dedup key, headers) to implement features like per-message TTL, deduplication, and filtering.

### Proposed Approaches

**Approach A — Opaque payload**: Storage layer stores raw bytes; protocol adapters encode/decode their own headers. Simplest, but the storage layer cannot enforce per-message TTL, dedup, or do any message-level filtering.

**Approach B — Structured envelope**: Storage layer defines a common envelope format:

```
[total_len     : u32]
[properties_len: u16]
[properties    : bytes]  -- key-value pairs (timestamp, ttl, dedup_id, ...)
[body_len      : u32]
[body          : bytes]  -- application payload
```

The properties section uses a compact encoding (e.g., tag-length-value). Well-known property tags:
- `0x01` = timestamp (u64 millis)
- `0x02` = TTL (u32 seconds)
- `0x03` = dedup_id (variable-length bytes)
- `0x04` = message_id (variable-length bytes)
- `0x10+` = user-defined headers

Approach B enables the storage layer to handle TTL expiry, dedup, and message-level filtering without coupling to any specific protocol. The wire protocol and S3 format both use this envelope.

### Open Questions

- Does the structured envelope add unacceptable overhead for the Kafka path where none of these features are needed? Kafka already has its own record batch format — should the storage layer accept pre-formatted Kafka batches as opaque blobs?
- Should properties be indexed (searchable) or just carried opaquely with the message?
- How does the envelope interact with the arena layout? The internal index maps offset -> byte_pos; the envelope doesn't change this, but readers need to parse the envelope to extract the body.

---

## Issue 3e: Producer Deduplication

### Problem

Exactly-once semantics require the storage layer to reject duplicate appends. Needed for Kafka idempotent producer and MQTT QoS2.

### Proposed Solution

- Each producer has a `producer_id` + monotonic `sequence_number`
- The Primary tracks the last committed sequence per producer (in-memory map)
- Duplicate detection: `sequence <= last_committed[producer_id]` -> return the original offset without re-appending
- Per-producer state is small (one u64 per active producer)
- State is persisted on seal (included in extent metadata) for recovery
- State can be evicted for producers that have been idle for a configurable TTL

### Wire Protocol Change

The APPEND frame would need optional fields for dedup:

```
Variable Header (optional dedup fields, signaled by flag):
  [producer_id    : u64]
  [sequence_number: u64]
```

When the dedup flag is not set, behavior is unchanged (no dedup overhead for protocols that don't need it).

### Open Questions

- How does producer state survive extent-full transitions? The stream-level leader handles transitions inline, so the per-producer map should live at the stream level (not extent level).
- What happens on epoch bump (replica set change)? The new Primary needs the producer state. Options: (a) persist to MySQL on seal, new Primary loads on register; (b) include in the `ForwardInitExtent` metadata.
- How long should idle producer state be retained? Too long = memory waste; too short = false duplicate acceptance after eviction.

---

## Issue 4: Seal Storm Under Time-Based Idle Seal

### Problem

If time-based idle seal fires for thousands of streams simultaneously (e.g., a periodic batch job finishes and all streams go idle at once), Stream Manager receives a burst of seal notifications. Each seal involves a MySQL transaction, potentially overwhelming the database.

### Proposed Solution

**Jittered idle-seal timeout**: Instead of a fixed 10s timeout, use `10s + random(0, 5s)` to spread seal events over time.

**Batched seal processing**: Stream Manager could batch-process seal notifications, combining multiple seal+allocate operations into fewer MySQL transactions.

**Rate limiting**: SM could rate-limit seal processing (e.g., max 100 seals/sec) and queue excess notifications. Since idle-seal is not latency-critical (the stream is idle by definition), a few seconds of queuing is acceptable.

### Open Questions

- Is the jitter approach sufficient, or do we need explicit rate limiting?
- Should the idle-seal timer be per-extent-node (EN seals locally and notifies SM) or per-stream-manager (SM decides when to seal)? Per-EN is more distributed but harder to rate-limit globally.

---

## Issue 5: Connection and Thread Scalability

### Problem

With 10K+ streams across 20 Extent Nodes:
- Each EN might host thousands of streams (as Primary or Secondary)
- The pipelined group commit `in_flight` counter + channel per stream is lightweight (one AtomicU64 + one channel), so this scales fine
- The DownstreamManager pools connections per node address (not per stream), so connection count = O(nodes^2), not O(streams)

This is **not a critical issue** with the current design, but worth monitoring.

### Potential Concerns

- **Channel memory**: Each stream has an unbounded crossbeam channel for follower delegation. With 10K mostly-idle streams, these channels are empty and consume minimal memory (just the channel metadata). Not a problem.
- **DashMap contention**: The `ExtentNodeStore` uses a `DashMap<StreamId, Stream>`. With 10K entries and sharded locking, contention should be low. Worth benchmarking.
- **Tokio task count**: If each stream spawns background tasks (e.g., idle-seal timer), 10K tasks is well within Tokio's capacity. Alternatively, use a single timer wheel for all streams.

### Open Questions

- Should we benchmark DashMap with 10K+ streams to confirm sharding is sufficient?
- Is a single shared timer wheel preferable to per-stream idle-seal timers?

---

## Priority Matrix

| Priority | Issue | Rationale |
|----------|-------|-----------|
| **P0** | Issue 1: Tiered arena sizing + lazy allocation + idle seal | Without this, 10K streams is infeasible |
| **P0** | Issue 3a: TTL / retention + GC | Every protocol needs it; without it, storage grows forever |
| **P1** | Issue 2: Extent compaction | Required once time-based seal creates many tiny extents |
| **P1** | Issue 3d: Structured message envelope | Enables per-message TTL, dedup, and multi-protocol metadata |
| **P1** | Issue 3e: Producer deduplication | Needed for Kafka idempotent + MQTT QoS2 |
| **P1** | Issue 4: Seal storm mitigation | Required for operational stability at scale |
| **P2** | Issue 3c: Retained message KV store | MQTT-specific |
| **P2** | Issue 3b: Per-message ACK tracking | MQTT QoS1/2, AMQP — may be protocol adapter concern |
| **P2** | Issue 5: Connection scalability | Current design likely sufficient; needs benchmarking |
