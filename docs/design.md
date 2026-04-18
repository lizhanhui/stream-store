# Object Storage Backend with Replicated In-Memory Layer

## Motivation

Replace cloud block-based disk storage with object storage (S3-compatible) to reduce cost. Object storage has low IOPS and higher latency, so a replicated in-memory layer is required to serve hot data. The WAS (Windows Azure Storage) stream employs seal-and-new mechanism to achieve consistency,  availability and partition (CAP) at the same time through separation of concern.

## Core Abstractions

### Stream

An ordered, append-only sequence of messages/records. 

Stream has two types:

- **Data Stream**: Stores sequence of messages/records data. Each extent maintains an internal index (compressed `AtomicU32` pointers) mapping sequence numbers to byte positions.
- **Index Stream**: Stores lightweight pointers into data streams (used for multi-dispatch fan-out).

### Extent

The unit of replication and the unit of S3 flush. A stream is composed of an ordered list of extents.

- **Active (unsealed, mutable)**: At most one per stream. Accept appends. Replicated via broadcast replication across a configurable number of nodes (replication factor, default 2).
- **Sealed**: Immutable. Eligible for S3 flush. Once flushed to S3, they can be evicted from memory. 
- **Flushed**: Sealed + uploaded to S3.  Served from S3 (with read cache) on demand. Flushed extents are supposed to be evicted from memory to free up space for active extents and new appends. They are subject to TTL-based policy in memory and S3.

**Storage Medium** — Each stream has a `storage_medium` property set at creation time:
- **S3** (0, default): Sealed extents are uploaded to S3 by the Primary. Eviction from memory only after the extent is flushed (uploaded). Data is durable in S3.
- **Memory** (1): Sealed extents are NOT uploaded to S3. Eviction happens when the `cache_extents` limit is reached — oldest sealed extents are dropped from memory. Data is lost after eviction (acceptable for ephemeral/transient workloads).

### Seal-and-New

When a trigger fires (size threshold, time interval, node failure, or **extent full**):

For **extent full**: handled autonomously by the Primary Extent Node within the current epoch — see "ExtentFull handling" below. Stream Manager is not involved.

For **client timeout or failure recovery**: the `SEAL_STREAM_MANAGER` opcode (0x06) is used. The client sends `seal(stream_id, epoch)` to the Stream Manager, which:

**Client Seal (`FLAG_EPOCH_PRESENT = 1`)**:
1. Client sends `Seal(stream_id, epoch)` to Stream Manager (client seals by epoch, not extent_id).
2. Stream Manager looks up the Primary for that epoch and forwards the Seal.
3. Primary seals its current active extent and responds with `(extent_id, end_offset)`.
4. Stream Manager reconciles metadata, bumps epoch, allocates a new replica set.
5. Stream Manager responds to client with the new extent info and new epoch.

**Client Seal (`FLAG_OFFSET_PRESENT = 0`, legacy extent-based)**:
1. Client sends `Seal(stream_id, extent_id)` to Stream Manager.
2. Stream Manager sends `Seal` RPC to **each Extent Node holding a replica** (Primary and all Secondaries). Each Extent Node stops accepting appends and responds with its local commit length.
3. Stream Manager determines the committed offset: if the Primary responded, its quorum offset is used (most accurate). Otherwise, SM computes the committed offset from Secondary responses using quorum math (sorts offsets descending, takes the k-th value where `k = RF/2`).
4. Stream Manager updates extent metadata to SEALED with the committed end_offset.
5. Stream Manager allocates a **new** active extent on (potentially different) healthy nodes, sends `RegisterExtent` to the new **Primary** and **waits for its `RegisterExtent` ack (flag=0x01)** before proceeding. `RegisterExtent` to Secondaries is fire-and-forget (see "Lazy Secondary Extent Creation" below).
6. Stream Manager responds to client with the new extent info (Primary address). Writes resume immediately.

**Extent-node Seal** (`FLAG_OFFSET_PRESENT = 1`):
1. Primary ExtentNode proactively seals (e.g. arena full) and sends `Seal(stream_id, extent_id, offset)` with `FLAG_OFFSET_PRESENT` set to Stream Manager. The `offset` is the committed end_offset.
2. Stream Manager trusts the reported offset and records it as the extent's `end_offset` in metadata.
3. Stream Manager updates extent metadata to SEALED.
4. Stream Manager **fire-and-forgets** Seal RPCs to secondary extent nodes only (`tokio::spawn` -- does not block the response), skipping the Primary (already sealed locally). This ensures secondaries learn about the seal asynchronously.
5. Stream Manager allocates a new active extent, sends `RegisterExtent` to the new **Primary** and **waits for its `RegisterExtent` ack (flag=0x01)**, then responds to the Extent Node with the new extent info. `RegisterExtent` to Secondaries is fire-and-forget.

Both paths share the same downstream procedure in Stream Manager: seal in MySQL (transaction), allocate new extent, wait for Primary `RegisterExtent` ack, respond to requester.

**Why SM waits for Primary RegisterExtent ack**: Multiple clients may seal the same extent concurrently. SM may return the new extent info to the client (or the client may discover it via `DescribeStream`) before the target Primary has processed `RegisterExtent`. Waiting for the Primary's ack adds only one SM↔EN round-trip (negligible compared to the MySQL transaction already in the seal path) and guarantees the Primary is ready to accept appends by the time any client learns about the new extent. For EN-initiated seal (ExtentFull), this is especially important — clients that received `ExtentFull` are already spinning on `DescribeStream` and would fail repeatedly if the new Primary isn't registered yet.

**Lazy Secondary Extent Creation**: Secondaries create extents on-demand when they receive the **first Forward frame** from the Primary, rather than requiring `RegisterExtent` to arrive first. The Primary sends a `ForwardInitExtent` (Forward opcode 0x05, flag=0x01) before the first Forward for a new extent, carrying `stream_id`, `extent_id`, `start_offset`, `extent_capacity`, and `cache_extents`. This eliminates the race where a secondary receives forwards before `RegisterExtent` arrives, and reduces the seal-and-new critical path to a single SM↔Primary round-trip. `RegisterExtent` to secondaries is still sent as a fire-and-forget hint for arena pre-allocation, but is **not required for correctness**.

**ExtentFull handling — Epoch-Based Autonomous Extent Creation**: When the Primary's arena is exhausted, the transition is handled **entirely within the Extent Node** — Stream Manager is not on the critical path. The system uses a **stream epoch** model:

1. **Stream Epoch**: Each stream has an epoch. Within an epoch, the replica set (Primary + Secondaries) is fixed. SM only bumps the epoch on failure recovery or rebalancing.

2. **Autonomous Creation**: When the Primary's active extent fills up, the **stream-level leader** (pipelined group commit) handles the transition inline:
   - Seals the current extent locally (atomic `limit` store)
   - Creates a new extent with the next sequential ID (same replica set, same epoch)
   - Retries the triggering append on the new extent — **the client never sees an error**
   - Asynchronously notifies Stream Manager via `NOTIFY_SEALED_EXTENT` (fire-and-forget)
   - Secondaries learn about the new extent via lazy creation on the first Forward frame

3. **Stream-Level Leader Election**: The pipelined group commit leader election (`in_flight` counter + follower channel) operates at the **Stream level**, not the Extent level. This means:
   - Only one thread writes to any extent in a stream at any time
   - Extent transitions happen within the leader's turn — no re-election, no race
   - Followers queued during the transition are processed on the new extent after it's created
   - Message ordering is preserved by construction (single writer + FIFO channel)

4. **SM Metadata Catch-Up**: Stream Manager receives `NOTIFY_SEALED_EXTENT` notifications and updates MySQL metadata asynchronously. If notifications are lost, SM reconciles at the next epoch bump.

5. **Epoch Bump**: SM bumps the epoch when the replica set needs to change (node failure, rebalancing). SM sends `Seal(stream_id, epoch)` to the Primary, waits for it to seal, reconciles metadata, then allocates a new epoch with a new replica set.

This eliminates the SM round-trip (1 EN↔SM RTT + MySQL transaction + 1 SM↔EN RTT) from the extent-full critical path, reducing it to a local seal + arena allocation (~microseconds).

**Consistency** is resolved on the sealed extent (backward-looking). **Availability** is provided by the new extent (forward-looking). The system never blocks writes to achieve consistency.

## Architecture

The storage layer runs as a **dedicated Rust process** (`stream-store`). This layer provides:

- **No GC pauses**: The storage service holds gigabytes of in-memory message data. GC stop-the-world events at this scale would stall replication ACKs and cause false failure detections. Rust gives deterministic deallocation.
- **Zero-copy I/O**: Broadcast replication forwards bytes from Primary to all Secondaries. Rust's `bytes::Bytes` (reference-counted buffers) enables zero-copy forwarding without the copy overhead.
- **Precise memory control**: The service has a hard memory budget. Rust enforces it precisely.
- **Enforced boundary**: A process boundary prevents accidental coupling of protocol logic with storage internals.

### Process Architecture

```
  Client Application                 Rust Process (Storage Service)
 ┌─────────────────────────┐            ┌──────────────────────────────┐
 │                         │            │  stream-store (Rust/Tokio)   │
 │                         │            │                              │
 │                         │            │  Stream Manager              │
 │                         │            │  - Extent lifecycle          │
 │                         │  Custom    │  - Seal-and-new              │
 │                         │  TCP       │  - Metadata (MySQL client)   │
 │                         │ ◄────────► │                              │
 │  - StreamStoreClient    │  Protocol  │  Extent Nodes                │
 │                         │            │  - Broadcast replication     │
 │                         │            │  - S3 flush                  │
 │                         │            │  - Read cache                │
 │                         │            │                              │
 └─────────────────────────┘            └──────────────────────────────┘
                                                     │
                                               ┌─────▼─────┐
                                               │  S3 Bucket │
                                               │  (cold)    │
                                               └────────────┘
```

### Broadcast Replication Topology

The replication factor (RF) is configurable (default 2, supports 1-N). Each active extent is replicated across RF nodes. Following WAS paper terminology, the first node is the **Primary** and subsequent nodes are **Secondaries**.

- **Primary**: Sole append acceptor. Assigns monotonic sequence numbers. Broadcasts writes to all Secondaries in parallel (O(1) hop latency).
- **Secondary**: Receives forwarded writes directly from Primary. Returns cumulative watermark ACKs to Primary.
- **Quorum ACK**: Primary waits for ACKs from a quorum of replicas (itself + `RF/2` secondaries) before ACKing clients. This tolerates minority failures without blocking.

```
RF=2 (default):  Primary broadcasts to Secondary

                          +------------------+
                          | Stream Manager   |
                          | (Metadata via    |
                          |  MySQL)          |
                          +--------+---------+
                                   |
                   stream/extent metadata, seal/allocate
                                   |
              +--------------------+--------------------+
              |                                         |
        +-----+-----+                            +-----+-----+
        | ExtentNode |    broadcast append        | ExtentNode |
        | (Primary)  | =========================> | (Secondary)|
        |  in-mem    | <--- watermark ACK ------- |  in-mem    |
        +-----------+                             +-----------+
              |
        S3 Flusher
              |
        +-----v-----+
        |  S3 Bucket |
        |  (cold)    |
        +------------+

RF=3 (optional, quorum = Primary + 1 Secondary):

                          +-----------+
              +=========> | ExtentNode|
              |           |(Secondary)|
              |           +-----+-----+
              |                 |
              | watermark ACK   | watermark ACK
              |   (from S1)     |   (from S2)
              |                 |
        +-----+-----+    +-----+-----+
        | ExtentNode| <= | ExtentNode|
        | (Primary) |    |(Secondary)|
        +-----------+    +-----------+
              |                ^
              +================+
                broadcast append

        Primary broadcasts to BOTH secondaries in parallel.
        BOTH secondaries send watermark ACKs back to Primary.
        Quorum ACK: Primary + 1 of 2 secondaries (RF/2 = 1).
```

All Extent Nodes, S3 Flusher, and S3 Reader run as Extent Node processes. Stream Manager nodes run as separate Stream Manager processes. Client applications communicate with the Rust storage service via a custom TCP protocol.

**Stream Manager Clustering**: Stream Manager is designed to be fully stateless — all persistent state lives in MySQL, and no in-memory caches are maintained. Multiple SM nodes can run concurrently against the same database. Any SM can handle any client request (CreateStream, Seal, Describe, Seek, QueryOffset) by reading/writing MySQL directly. Node metrics for load-aware placement are persisted to the `node_metrics` table on every heartbeat.

A DB-based leadership lease (`stream_manager_leadership` table) ensures that only one SM at a time runs proactive operations: the heartbeat checker (dead node detection) and failover (epoch bump, seal-and-allocate replacement extents). The `bump_epoch` function uses a compare-and-swap guard (`WHERE epoch = ?`) to prevent double-bumps if leadership transfers mid-failover.

**EN Multi-SM Failover**: Each Extent Node is configured with a list of SM addresses (`stream_manager_addrs`). On connection failure, the EN advances to the next address in round-robin order, ensuring automatic failover when an SM node goes down.

### Components

| Component | Language | Role |
|-----------|----------|------|
| **Storage Service (stream-store)** | Rust | Dedicated process. Extent nodes, stream manager, broadcast replication, S3 flush/read. |
| **Stream Manager** | Rust | Metadata coordinator within storage service. Manages stream->extent mappings, seal/allocate, offset translation. MySQL client for metadata persistence. |
| **Extent Node** | Rust | Holds in-memory extent replicas. Participates in broadcast replication (Primary broadcasts, Secondaries ACK). |
| **S3 Flusher** | Rust | Background task on Primary Extent Node. Encodes sealed extents with chunk compression and uploads to S3 via `aws-sdk-s3`. Broadcasts ForwardFlushed to secondaries and notifies SM on completion. |
| **S3 Reader** | Rust | Fetches flushed extents from S3 with local LRU read cache. |

### Custom TCP Wire Protocol

**Fixed Header + Variable Header + Payload** format for minimal overhead and zero-copy forwarding. Each opcode defines its own variable header layout; only fields relevant to that operation are on the wire. The payload section carries arbitrary application data (e.g., message bytes for APPEND) and is always length-prefixed.

#### Frame Format

```
+--Fixed Header (8 bytes, always present)-----------------------------+
|       0       1       2       3       4       5       6       7     |
|  +-------+-------+-------+-------+-------------------------------+  |
|  | Magic |Version| Opcode| Flags |       Remaining Length        |  |
|  +-------+-------+-------+-------+-------------------------------+  |
+---------------------------------------------------------------------+
+--Variable Header (opcode-specific, 0..N bytes)----------------------+
|  Fields determined by the Opcode. See per-opcode layouts below.     |
+---------------------------------------------------------------------+
+--Payload (optional, length-prefixed)--------------------------------+
|  [Payload Length : u32]  (present when opcode defines a payload)     |
|  [Payload bytes  : ...]                                             |
+---------------------------------------------------------------------+
```

**Fixed Header** (8 bytes):

| Field | Size | Description |
|-------|------|-------------|
| Magic | 1B | `0xEF` -- protocol identification |
| Version | 1B | Protocol version (currently 2) |
| Opcode | 1B | Operation type (see below) |
| Flags | 1B | Per-opcode flags. `0x01` = `FLAG_RESPONSE` (success response), `0x80` = `FLAG_RESPONSE_ERROR` (error response). Lower bits (`0x02`, `0x04`) available for per-opcode request-side semantics. |
| Remaining Length | 4B | Total bytes of variable header + payload section that follow the fixed header |

**Variable Header**: Determined entirely by the Opcode (and sometimes Flags). Each opcode section below specifies the exact fields and their order. Fields carry protocol-level metadata specific to the operation (stream IDs, offsets, extent IDs, counts, request IDs, etc.). Only the fields meaningful for that opcode appear on the wire. Request ID is a variable header field present in request-response opcodes, absent in fire-and-forget opcodes (e.g., WATERMARK, SM_MEMBERSHIP_CHANGE).

**Payload**: Carries arbitrary application data from the ultimate user (e.g., message bytes for APPEND, encoded read batches for READ responses, or an error description when a response is flagged with `FLAG_RESPONSE_ERROR`). When present, a 4-byte `Payload Length` prefix precedes the payload bytes. Opcodes that carry no application payload omit both the length prefix and the payload bytes entirely.

**Rust Representation**: The `Frame` type uses a `FixedHeader` + `VariableHeader` enum + `Option<Bytes>` payload design. Each opcode is a distinct `VariableHeader` variant containing only the fields valid for that opcode — invalid field combinations are rejected at compile time. Flag-dependent fields (e.g., `Seal.offset`, `SealAck.new_extent_id`, response error headers) use distinct variants or `Option<T>` fields so the flags byte is derived from the variable-header shape during encode.

**Flagged response errors**: The protocol uses a **unified opcode model**: each request-response operation uses a single opcode on the wire. The flags byte distinguishes direction: `0x00` = request, `0x01` (`FLAG_RESPONSE`) = success response, `0x80` (`FLAG_RESPONSE_ERROR`) = error response. The opcode identifies the operation (`APPEND`, `SEAL_STREAM_MANAGER`, `DESCRIBE_STREAM`, etc.), while the flag switches between request, success-response, and error-response variable header layouts. Error responses carry a human-readable error message in the payload.

#### Opcodes

Grouped by category with gaps for future growth.

#### Flag Convention

All request-response opcodes use a uniform flag convention:

| Flag | Meaning |
|------|---------|
| 0x00 | Request |
| 0x01 (FLAG_RESPONSE) | Success response |
| 0x80 (FLAG_RESPONSE_ERROR) | Error response |

Lower flag bits (0x02, 0x04) are available for per-opcode request-side semantics:
- APPEND: 0x02 = FLAG_SYSTEM_TICK (synthetic capacity-scaling tick)
- DESCRIBE_STREAM: 0x02 = FLAG_DESCRIBE_STREAM_BY_NAME (name-based lookup)
- FORWARD: uses 0x00/0x01/0x02/0x03 for forward variants (no response)
- UPDATE_EXTENT: uses 0x00/0x01/0x02 for sealed/progress/flushed (fire-and-forget, no response)

**Data path (0x01-0x0F) -- Client <-> Extent Node**

##### 0x01 CREATE_STREAM (Client <-> Stream Manager)

Create a new stream. If `replication_factor = 0`, Stream Manager uses its default. If `min/max_extent_capacity = 0`, defaults apply (8 MiB / 256 MiB). If `cache_extents = 0`, Stream Manager uses 4. If `extent_growth_factor = 0`, default 2 is used.

**Request (flag=0x00): Client -> SM**

```
Fixed Header (8B)
Variable Header:
  [request_id            : u32]
  [name_len              : u16]
  [stream_name           : bytes]  -- human-readable stream name
  [replication_factor    : u8]
  [min_extent_capacity   : u32]    -- minimum arena size in bytes (0 = default 8 MiB)
  [max_extent_capacity   : u32]    -- maximum arena size in bytes (0 = default 256 MiB)
  [cache_extents         : u16]    -- max extents to retain in memory (0 = default 4)
  [extent_growth_factor  : u8]    -- adaptive growth multiplier (0 = default 2)
  [storage_medium       : u8]    -- 0 = S3 (default), 1 = Memory
No Payload.
```

**Response (flag=0x01): SM -> Client**

Returns the newly created stream ID, initial extent ID, and the Primary Extent Node address.

```
Fixed Header (8B)
Variable Header:
  [request_id   : u32]
  [stream_id    : u32]
  [extent_id    : u32]
  [epoch        : u32]    -- initial stream epoch (always 0 for new streams)
  [addr_len     : u16]
  [primary_addr : bytes]  -- address of the initial extent's Primary node
No Payload.
```

**Error (flag=0x80): SM -> Client**

```
Fixed Header (8B)
Variable Header:
  [request_id  : u32]
  [error_code  : u16]
Payload:
  [payload_len : u32]
  [payload     : bytes]  -- human-readable error message
```

##### 0x03 APPEND (Client <-> Primary)

Append a message to a data stream. The client targets a stream by `(stream_id, epoch)` — the epoch identifies the replica set. The Primary routes the append to the current active extent; the client does not choose which extent to write to. If the epoch is stale (the replica set was reassigned via an epoch bump), the server returns `EpochStale`. Client-only operation; replication uses the dedicated Forward opcode (0x05).

**Request (flag=0x00): Client -> Primary**

```
Fixed Header (8B)
Variable Header:
  [request_id   : u32]    -- correlates request/response
  [stream_id    : u32]    -- target stream
  [epoch        : u32]    -- target epoch (0 = accept any epoch)
Payload:
  [payload_len  : u32]    -- length of message bytes
  [payload      : bytes]  -- message body (application data)
```

**Ack (flag=0x01): Primary -> Client**

Confirms a successful append after quorum ACK is achieved. The response includes the epoch and extent_id the record landed on for diagnostics — the client can verify the epoch matches and log which extent was used.

```
Fixed Header (8B)
Variable Header:
  [request_id   : u32]    -- correlates with original APPEND request
  [stream_id    : u32]    -- stream that was appended to
  [epoch        : u32]    -- epoch at append time (diagnostics)
  [extent_id    : u32]    -- extent that was appended to (diagnostics)
  [offset       : u64]    -- assigned logical sequence number
No Payload.
```

**Error (flag=0x80): Primary -> Client**

```
Fixed Header (8B)
Variable Header:
  [request_id   : u32]
  [stream_id    : u32]
  [epoch        : u32]
  [extent_id    : u32]
  [error_code   : u16]
Payload:
  [payload_len  : u32]
  [payload      : bytes]  -- human-readable error message
```

##### 0x05 FORWARD (Primary -> Secondary)

Dedicated opcode for Primary→Secondary broadcast replication. Uses flags to distinguish three variants:

**flag=0x00 Forward** — Per-record replication. Carries the primary-assigned `byte_pos` so the secondary writes each record at the exact same arena position, enabling bit-for-bit identical replicas. Fire-and-forget: no `request_id`; secondary responds with cumulative `Watermark`.

```
Fixed Header (8B)    -- flags=0x00
Variable Header (32B):
  [stream_id    : u32]    -- target stream
  [extent_id    : u32]    -- target extent
  [epoch        : u32]    -- stream epoch
  [offset       : u64]    -- primary-assigned logical offset for this record
  [byte_pos     : u64]    -- primary-assigned byte position in arena
Payload:
  [payload_len  : u32]    -- length of message bytes
  [payload      : bytes]  -- message body
```

**flag=0x01 ForwardInitExtent** — Sent once by the Primary before the first Forward frame for a new extent. Carries extent metadata so the secondary can create the extent with the correct capacity. No payload, no response.

```
Fixed Header (8B)    -- flags=0x01
Variable Header (33B):
  [stream_id        : u32]    -- target stream
  [extent_id        : u32]    -- new extent
  [epoch            : u32]    -- stream epoch
  [start_offset     : u64]    -- extent base offset
  [extent_capacity  : u32]    -- arena size in bytes
  [cache_extents    : u16]    -- max extents to retain in memory
  [storage_medium   : u8]     -- 0 = S3, 1 = Memory
No Payload.
```** — Sent by the Primary after sealing an extent so secondaries can verify data integrity. No response.

```
Fixed Header (8B)    -- flags=0x02
Variable Header (28B):
  [stream_id        : u32]    -- target stream
  [extent_id        : u32]    -- sealed extent
  [epoch            : u32]    -- stream epoch
  [checksum         : u32]    -- CRC32 of the extent's committed data
  [committed_bytes  : u64]    -- byte count of committed data
No Payload.
```

**flag=0x03 ForwardFlushed** — Sent by the Primary after a sealed extent is successfully uploaded to S3. Secondaries mark the extent as eligible for memory eviction. No response.

```
Fixed Header (8B)    -- flags=0x03
Variable Header (16B):
  [stream_id    : u32]    -- target stream
  [extent_id    : u32]    -- flushed extent
  [epoch        : u32]    -- stream epoch
No Payload.
```

##### 0x06 SEAL_STREAM_MANAGER (Client <-> Stream Manager)

Epoch-based seal. Flags distinguish request (0x00), response (0x01), and error (0x80).

**Request (flag=0x00): Client -> SM**

Client requests SM to seal the active extent at the given epoch, bump epoch, and allocate a new extent.

```
Variable Header:
  [request_id : u32]
  [stream_id  : u32]
  [epoch      : u32]    -- seal active extent at this epoch
No Payload.
```

**Response (flag=0x01): SM -> Client**

Returns the new epoch and primary address for the replacement extent.

```
Variable Header:
  [request_id   : u32]
  [stream_id    : u32]
  [offset       : u64]    -- committed end offset of sealed extent
  [new_epoch    : u32]    -- epoch of the newly allocated extent
  [addr_len     : u16]
  [primary_addr : bytes]  -- address of the new extent's Primary node
No Payload.
```

**Error (flag=0x80): SM -> Client**

```
Variable Header:
  [request_id : u32]
  [stream_id  : u32]
  [error_code : u16]
Payload:
  [payload_len : u32]
  [payload     : bytes]  -- human-readable error message
```

##### 0x07 SEAL_EXTENT_NODE (Stream Manager <-> Extent Node)

SM seals the last mutable extent at the given epoch on an EN. The EN returns the sealed extent info in the header and any predecessor extents (unknown to SM) in the payload, enabling one-RPC reconciliation.

**Request (flag=0x00): SM -> EN**

`extent_id_from` is SM's last known extent — the EN returns all extents with `extent_id >= extent_id_from` in the response payload.

```
Variable Header:
  [request_id     : u32]
  [stream_id      : u32]
  [epoch          : u32]    -- seal last mutable extent at this epoch
  [extent_id_from : u32]    -- SM's last known extent
  [start_offset   : u64]    -- hint for absent-extent handling
No Payload.
```

**Response (flag=0x01): EN -> SM**

Header carries the just-sealed extent. Payload carries predecessor extents that SM may not know about (from autonomous seal-and-new before SM was notified).

```
Variable Header:
  [request_id   : u32]
  [stream_id    : u32]
  [epoch        : u32]
  [extent_id    : u32]    -- the just-sealed (last mutable) extent
  [start_offset : u64]
  [end_offset   : u64]    -- committed end offset
Payload (optional):
  [num_extents  : u32]
  per extent:
    [extent_id    : u32]
    [start_offset : u64]
    [end_offset   : u64]
    [state        : u8]
  -- predecessor extents: extent_id >= extent_id_from AND < sealed extent_id
  -- does NOT include the just-sealed extent (already in header)
  -- empty if SM's view was up-to-date
```

**Error (flag=0x80): EN -> SM**

```
Variable Header:
  [request_id : u32]
  [stream_id  : u32]
  [error_code : u16]
Payload:
  [payload_len : u32]
  [payload     : bytes]  -- human-readable error message
```

##### 0x08 QUERY_OFFSET (Client <-> Extent Node / Stream Manager)

Query the max offset (exclusive) for a stream.

**Request (flag=0x00): Client -> EN / SM**

```
Fixed Header (8B)
Variable Header:
  [request_id   : u32]    -- correlates request/response
  [stream_id    : u32]    -- target stream
No Payload.
```

**Response (flag=0x01): EN / SM -> Client**

Returns the current max offset.

```
Fixed Header (8B)
Variable Header:
  [request_id   : u32]    -- correlates with original QUERY_OFFSET request
  [stream_id    : u32]    -- queried stream
  [offset       : u64]    -- max offset (exclusive)
No Payload.
```

**Error (flag=0x80): EN / SM -> Client**

```
Fixed Header (8B)
Variable Header:
  [request_id  : u32]
  [stream_id   : u32]
  [error_code  : u16]
Payload:
  [payload_len : u32]
  [payload     : bytes]  -- human-readable error message
```

##### 0x0A READ (Client <-> Extent Node)

Read messages from a stream starting at a given logical offset. The server resolves the byte position internally via its index stream, so clients only need to provide the logical offset.

**Request (flag=0x00): Client -> EN**

```
Fixed Header (8B)
Variable Header:
  [request_id   : u32]    -- correlates request/response
  [stream_id    : u32]    -- target stream
  [extent_id    : u32]    -- target extent
  [offset       : u64]    -- start logical offset
  [count        : u32]    -- number of messages to read
No Payload.
```

**Response (flag=0x01): EN -> Client**

Read response carrying message data.

```
Fixed Header (8B)
Variable Header:
  [request_id   : u32]    -- correlates with original READ request
  [stream_id    : u32]    -- stream that was read from
  [offset       : u64]    -- starting offset of the returned batch
  [count        : u32]    -- actual number of messages returned
Payload:
  [payload_len  : u32]    -- total length of all encoded messages
  [payload      : bytes]  -- repeated [msg_len:u32][msg_bytes] per message
```

**Error (flag=0x80): EN -> Client**

```
Fixed Header (8B)
Variable Header:
  [request_id  : u32]
  [stream_id   : u32]
  [error_code  : u16]
Payload:
  [payload_len : u32]
  [payload     : bytes]  -- human-readable error message
```

**Lifecycle (0x10-0x1F) -- Extent Node <-> Stream Manager**

##### 0x10 CONNECT (Extent Node <-> Stream Manager)

First frame after an Extent Node connects to Stream Manager. Stream Manager uses 1.5x `interval_ms` as the dead-node timeout.

**Request (flag=0x00): EN -> SM**

```
Fixed Header (8B)
Variable Header:
  [request_id    : u32]
Payload:
  [node_id_len  : u16]
  [node_id      : bytes]  -- unique node identifier
  [addr_len     : u16]
  [addr         : bytes]  -- listen address (host:port)
  [interval_ms  : u32]    -- heartbeat interval in milliseconds
```

**Ack (flag=0x01): SM -> EN**

Acknowledges Extent Node registration.

```
Fixed Header (8B)
Variable Header:
  [request_id    : u32]
No Payload.
```

**Error (flag=0x80): SM -> EN**

```
Fixed Header (8B)
Variable Header:
  [request_id  : u32]
  [error_code  : u16]
Payload:
  [payload_len : u32]
  [payload     : bytes]  -- human-readable error message
```

##### 0x12 DISCONNECT (Extent Node <-> Stream Manager)

Graceful shutdown. Stream Manager stops allocating new extents to this node.

**Request (flag=0x00): EN -> SM**

```
Fixed Header (8B)
Variable Header:
  [request_id    : u32]
Payload:
  [node_id_len  : u16]
  [node_id      : bytes]  -- node identifier
```

**Ack (flag=0x01): SM -> EN**

Acknowledges disconnect.

```
Fixed Header (8B)
Variable Header:
  [request_id    : u32]
No Payload.
```

**Error (flag=0x80): SM -> EN**

```
Fixed Header (8B)
Variable Header:
  [request_id  : u32]
  [error_code  : u16]
Payload:
  [payload_len : u32]
  [payload     : bytes]  -- human-readable error message
```

##### 0x14 HEARTBEAT (Extent Node -> Stream Manager)

Connection keepalive within the interval declared in CONNECT. Carries runtime metrics for load-aware extent placement.

```
Fixed Header (8B)
Variable Header:
  [request_id    : u32]
Payload:
  [node_id_len  : u16]
  [node_id      : bytes]  -- node identifier
  [available_memory_bytes : u64]
  [total_memory_bytes     : u64]
  [appends_per_sec        : u32]
  [active_extent_count    : u32]
  [bytes_written_per_sec  : u64]
```

**Heartbeat response** (Stream Manager -> Extent Node): echoes request_id, no payload.

```
Fixed Header (8B)
Variable Header:
  [request_id    : u32]
No Payload.
```

##### 0x15 REGISTER_EXTENT (Stream Manager <-> Extent Node)

Register an extent's replica membership on an Extent Node. Primary receives all secondary addresses for broadcast forwarding; Secondaries receive an empty address list. SM waits for the **Primary's** `RegisterExtent` ack (flag=0x01) before responding to the seal requester. `RegisterExtent` to Secondaries is fire-and-forget -- secondaries create extents lazily on first forwarded append (see "Lazy Secondary Extent Creation" in Seal-and-New).

**Request (flag=0x00): SM -> EN**

```
Fixed Header (8B)
Variable Header:
  [request_id              : u32]
  [stream_id               : u32]    -- stream this extent belongs to
  [extent_id               : u32]    -- extent being registered
  [role                    : u8]     -- 0 = Primary, 1+ = Secondary
  [replication_factor      : u8]
  [epoch                   : u32]    -- stream epoch for this extent registration
  [extent_capacity         : u32]    -- arena size in bytes for this extent
  [cache_extents           : u16]    -- max extents to retain in memory per stream
  [min_extent_capacity     : u32]    -- floor for adaptive shrink (0 = default)
  [max_extent_capacity     : u32]    -- ceiling for adaptive growth (0 = default)
  [extent_growth_factor    : u8]    -- adaptive growth multiplier (0 = default 2)
  [storage_medium          : u8]    -- 0 = S3, 1 = Memory
Payload:
  [num_addrs    : u16]    -- number of secondary addresses (0 for Secondaries)
  per address:
    [addr_len   : u16]
    [addr       : bytes]
```

**Ack (flag=0x01): EN -> SM**

Acknowledges extent registration.

```
Fixed Header (8B)
Variable Header:
  [request_id  : u32]
  [stream_id   : u32]    -- stream that was registered
  [extent_id   : u32]    -- extent that was registered
No Payload.
```

**Error (flag=0x80): EN -> SM**

```
Fixed Header (8B)
Variable Header:
  [request_id  : u32]
  [stream_id   : u32]
  [extent_id   : u32]
  [error_code  : u16]
Payload:
  [payload_len : u32]
  [payload     : bytes]  -- human-readable error message
```

##### 0x17 WATERMARK (Secondary -> Primary)

Cumulative ACK from Secondary to Primary. Primary uses watermark ACKs from all secondaries to compute quorum offset and ACK clients.

```
Fixed Header (8B)
Variable Header (24B):
  [stream_id    : u32]    -- stream the watermark applies to
  [extent_id    : u32]    -- extent the watermark applies to
  [epoch        : u32]    -- stream epoch for data integrity
  [offset       : u64]    -- highest committed offset (inclusive, cumulative)
No Payload.
```

##### 0x18 UPDATE_EXTENT (ExtentNode -> Stream Manager)

Async notification from EN to SM. Fire-and-forget: no response expected. Uses flags to distinguish three variants:

**flag=0x00 UpdateExtentSealed** — Sent by Primary after autonomous extent creation (extent-full within an epoch). SM updates metadata asynchronously.

```
Fixed Header (8B)    -- flags=0x00
Variable Header (28B):
  [stream_id           : u32]    -- stream that was sealed
  [epoch               : u32]    -- current epoch at time of seal
  [sealed_extent_id    : u32]    -- extent that was sealed
  [end_offset          : u64]    -- committed end_offset of sealed extent
  [new_extent_id       : u32]    -- newly created extent (same epoch, same replica set)
No Payload.
```

**flag=0x01 UpdateExtentProgress** — Periodic progress report for the active extent. SM uses this for monitoring and offset queries.

```
Fixed Header (8B)    -- flags=0x01
Variable Header (24B):
  [stream_id        : u32]    -- stream being reported
  [epoch            : u32]    -- current epoch
  [extent_id        : u32]    -- active extent
  [current_offset   : u64]    -- current committed offset
No Payload.
```

**flag=0x02 UpdateExtentFlushed** — Sent by Secondary-1 (flush role) after a sealed extent is successfully uploaded to S3. SM transitions extent state from Sealed to Flushed.

```
Fixed Header (8B)    -- flags=0x02
Variable Header (16B):
  [stream_id    : u32]    -- stream whose extent was flushed
  [epoch        : u32]    -- epoch at time of flush
  [extent_id    : u32]    -- flushed extent
No Payload.
```

##### 0x19 REPORT_EXTENTS (Stream Manager <-> Extent Node)

SM queries an EN for all extents it holds for a stream at a given epoch (recovery path).

**Request (flag=0x00): SM -> EN**

```
Fixed Header (8B)
Variable Header:
  [request_id   : u32]    -- correlates request/response
  [stream_id    : u32]    -- target stream
  [epoch        : u32]    -- epoch to report
No Payload.
```

**Response (flag=0x01): EN -> SM**

EN response with extent state for reconciliation.

```
Fixed Header (8B)
Variable Header:
  [request_id   : u32]    -- correlates request/response
  [stream_id    : u32]    -- queried stream
  [epoch        : u32]    -- epoch reported
Payload:
  [payload_len  : u32]
  [num_extents  : u32]
  per extent:
    [extent_id    : u32]
    [start_offset : u64]
    [end_offset   : u64]
    [state        : u8]
```

**Error (flag=0x80): EN -> SM**

```
Fixed Header (8B)
Variable Header:
  [request_id  : u32]
  [stream_id   : u32]
  [error_code  : u16]
Payload:
  [payload_len : u32]
  [payload     : bytes]  -- human-readable error message
```

**Cluster management (0x20-0x2F) -- Stream Manager -> Extent Node/Client**

##### 0x20 STREAM_MANAGER_MEMBERSHIP_CHANGE (Stream Manager -> Extent Node/Client)

Stream Manager cluster membership update. Extent Nodes and clients update their connection pools.

```
Fixed Header (8B)
Payload:
  [payload_len  : u32]
  [payload      : bytes]  -- list of active SM peer addresses
                             [num_addrs:u16][addr_len:u16][addr]...
```

**Management (0x30-0x3F) -- Client <-> Stream Manager**

##### 0x30 DESCRIBE_STREAM (Client <-> Stream Manager)

Describe a stream's extents with replica info and node liveness.

**Request (flag=0x00): Client -> SM**

When `FLAG_DESCRIBE_STREAM_BY_NAME` (0x02) is set, the frame includes `stream_name` for name-based lookup; `stream_id` is ignored.

```
Fixed Header (8B)
Variable Header:
  [request_id   : u32]    -- correlates request/response
  [stream_id    : u32]    -- target stream (ignored when flag 0x02 set)
  [count        : u32]    -- 0 = all extents, 1 = active only, N = at most N from latest
  If FLAG_DESCRIBE_STREAM_BY_NAME (0x02):
    [name_len   : u16]
    [stream_name: bytes]  -- resolve stream by name instead of stream_id
No Payload.
```

**Response (flag=0x01): SM -> Client**

Payload = encoded `Vec<ExtentInfo>`, ordered by extent_id **descending** (latest first). When `count > 0`, at most `count` extents are returned starting from the latest.

```
Fixed Header (8B)
Variable Header:
  [request_id   : u32]    -- correlates with original DESCRIBE_STREAM request
  [stream_id    : u32]    -- queried stream
Payload:
  [payload_len  : u32]
  [payload      : bytes]  -- encoded Vec<ExtentInfo> (see ExtentInfo format below)
```

**Error (flag=0x80): SM -> Client**

```
Fixed Header (8B)
Variable Header:
  [request_id  : u32]
  [stream_id   : u32]
  [error_code  : u16]
Payload:
  [payload_len : u32]
  [payload     : bytes]  -- human-readable error message
```

##### 0x32 DESCRIBE_EXTENT (Client <-> Stream Manager)

Describe a single extent.

**Request (flag=0x00): Client -> SM**

```
Fixed Header (8B)
Variable Header:
  [request_id   : u32]    -- correlates request/response
  [stream_id    : u32]    -- target stream
  [extent_id    : u32]    -- target extent
No Payload.
```

**Response (flag=0x01): SM -> Client**

Payload = encoded `Vec<ExtentInfo>` with exactly 1 entry.

```
Fixed Header (8B)
Variable Header:
  [request_id   : u32]    -- correlates with original DESCRIBE_EXTENT request
  [stream_id    : u32]    -- queried stream
Payload:
  [payload_len  : u32]
  [payload      : bytes]  -- encoded Vec<ExtentInfo> (1 entry)
```

**Error (flag=0x80): SM -> Client**

```
Fixed Header (8B)
Variable Header:
  [request_id  : u32]
  [stream_id   : u32]
  [extent_id   : u32]
  [error_code  : u16]
Payload:
  [payload_len : u32]
  [payload     : bytes]  -- human-readable error message
```

##### 0x34 SEEK (Client <-> Stream Manager)

Resolve a logical offset to the extent that contains it.

**Request (flag=0x00): Client -> SM**

```
Fixed Header (8B)
Variable Header:
  [request_id   : u32]    -- correlates request/response
  [stream_id    : u32]    -- target stream
  [offset       : u64]    -- target logical offset
No Payload.
```

**Response (flag=0x01): SM -> Client**

Payload = encoded `Vec<ExtentInfo>` with exactly 1 entry.

```
Fixed Header (8B)
Variable Header:
  [request_id   : u32]    -- correlates with original SEEK request
  [stream_id    : u32]    -- queried stream
  [offset       : u64]    -- resolved offset
Payload:
  [payload_len  : u32]
  [payload      : bytes]  -- encoded Vec<ExtentInfo> (1 entry)
```

**Error (flag=0x80): SM -> Client**

```
Fixed Header (8B)
Variable Header:
  [request_id  : u32]
  [stream_id   : u32]
  [offset      : u64]
  [error_code  : u16]
Payload:
  [payload_len : u32]
  [payload     : bytes]  -- human-readable error message
```

For sealed/flushed extents: `start_offset <= offset < end_offset`. For the active extent: `offset >= start_offset` (end_offset equals start_offset in metadata until sealed).

**ExtentInfo** payload encoding (shared by 0x30, 0x32, 0x34 responses):

```
[num_extents:u32]
  per extent:
    [extent_id:u32][start_offset:u64][end_offset:u64][state:u8][epoch:u32]
    [num_replicas:u16]
      per replica:
        [addr_len:u16][addr_bytes][role:u8][is_alive:u8]
```

Fields:
- `state`: ExtentState (0=Unspecified, 1=Active, 2=Sealed, 3=Flushed)
- `role`: 0=Primary, 1+=Secondary
- `is_alive`: 1 if the ExtentNode's heartbeat is current (node.state=Alive), 0 otherwise


#### Connection Model

- Clients maintain a **connection pool** to the storage service (one pool per Extent Node).
- Connections are multiplexed: multiple in-flight requests per connection, correlated by Request ID.
- Tokio on the Rust side handles async I/O with `tokio::net::TcpListener`.

### Rust Crate Structure (Cargo Workspace)

The Rust storage layer is organized as a **Cargo workspace** with multiple crates. Crates under `components/` are all shared libraries; `src/bin/` contains the binary entry points for the two process types from WAS: **Extent Node** and **Stream Manager**.

```
stream-store/                          (Workspace root)
├── Cargo.toml                         -- Workspace definition + root package
├── Cargo.lock
├── src/bin/
│   ├── extent-node.rs                 -- Binary: Extent Node process (depends on extent-node crate)
│   └── stream-manager.rs             -- Binary: Stream Manager process (depends on stream-manager crate)
│
└── components/                        (All library crates, shared across the workspace)
    │
    ├── common/                        -- Base types, config, errors (no runtime deps)
    │   └── src/lib.rs
    │       ├── types.rs               -- StreamId, ExtentId, Offset, Opcode, ErrorCode, NodeState, ExtentState
    │       ├── config.rs              -- ExtentNodeConfig, StreamManagerConfig
    │       └── errors.rs              -- Error types and conversions
    │
    ├── rpc/                           -- Custom TCP wire protocol (depends: common, tokio, bytes)
    │   └── src/lib.rs
    │       ├── frame.rs               -- Wire format encode/decode: FixedHeader + VariableHeader enum + payload
    │       ├── codec.rs               -- Tokio Encoder/Decoder for frame framing
    │       └── payload.rs             -- Structured payload encode/decode helpers
    │
    ├── server/                        -- Server infrastructure (depends: common, rpc, tokio)
    │   └── src/lib.rs
    │       └── handler.rs             -- RequestHandler trait, serve_connection, accept_loop
    │
    ├── client/                        -- Client library (depends: common, rpc)
    │   └── src/lib.rs                 -- StreamClient: connect/disconnect to Extent Node and Stream Manager,
    │                                     append, read, seal, create_stream
    │
    ├── extent-node/                   -- Extent Node library (depends: common, rpc, server, client)
    │   └── src/
    │       ├── lib.rs                 -- run(): Extent Node bootstrap, heartbeat to Stream Manager
    │       ├── extent.rs              -- Extent: in-memory buffer + state machine (Active/Sealed/Flushed)
    │       ├── stream.rs              -- Stream: ordered extent list, active extent tracking, seal-and-new
    │       ├── store/                 -- ExtentNodeStore: split into focused submodules
    │       │   ├── mod.rs             -- ExtentNodeStore struct, construction, accessors, RequestHandler dispatch
    │       │   ├── types.rs           -- ExtentUpdate, ReplicaInfo, AppendJob
    │       │   ├── append.rs          -- Write/append path, pipelined group commit, seal_and_create
    │       │   ├── forward.rs         -- Replication receive: Forward, ForwardInitExtent, ForwardChecksum
    │       │   ├── register.rs        -- RegisterExtent handler (SM → EN)
    │       │   ├── read.rs            -- Read and QueryOffset handlers
    │       │   ├── seal.rs            -- Seal, ReportExtents, build_seal_predecessor_payload
    │       │   └── tests.rs           -- Unit tests for store operations
    │       ├── stream_manager_client.rs -- StreamManagerClient: RAII connection + heartbeat lifecycle
    │       ├── downstream.rs          -- DownstreamManager: per-node-addr TCP for broadcast forwarding
    │       ├── ack_queue.rs           -- AckQueue: per-stream quorum tracking, PendingAck, timeout expiry
    │       ├── s3.rs                  -- S3Client: aws-sdk-s3 wrapper with namespace and compression config
    │       ├── s3_codec.rs            -- S3 extent file codec: chunk-compressed encode/decode with sparse index
    │       └── s3_flusher.rs          -- Background S3 flusher: uploads sealed extents, notifies SM on completion
    │
    └── stream-manager/                -- Stream Manager library (depends: common, rpc, server, client)
        └── src/
            ├── lib.rs                 -- run(): Stream Manager bootstrap, accept connections
            ├── store.rs               -- StreamManagerStore: request handler, seal_extent_node, notify_extent
            ├── metadata.rs            -- MySQL metadata operations (sqlx): streams, extents, replicas, nodes
            ├── allocator.rs           -- Extent placement: load-aware scoring across healthy Extent Nodes
            └── heartbeat_checker.rs   -- Node liveness checker, dead-node detection
```

**Dependency Graph**:
```
src/bin/extent-node.rs ──> extent-node (lib) ──┬──> server ──┬──> common
                                               │             └──> rpc ──> common
                                               └──> client ──┬──> common
                                                             └──> rpc ──> common

src/bin/stream-manager.rs ──> stream-manager (lib) ──┬──> server ──┬──> common
                                                     │             └──> rpc ──> common
                                                     └──> client ──┬──> common
                                                                   └──> rpc ──> common
```

**Crate Roles**:

| Crate | Type | Role |
|-------|------|------|
| **common** | lib | Shared types (StreamId, ExtentId, Opcode, NodeState, ExtentState), config structs, error types. Zero runtime dependencies. |
| **rpc** | lib | Custom TCP wire protocol: frame codec, payload helpers. |
| **server** | lib | Server infrastructure: RequestHandler trait with deferred response support, connection accept loop. |
| **client** | lib | Client for talking to Extent Node and Stream Manager: append/read messages, seal/create streams. Used by Extent Node (keepalive heartbeat to Stream Manager) and Stream Manager (seal commands to Extent Nodes). |
| **extent-node** | lib | Extent Node logic. Holds in-memory extent replicas, participates in broadcast replication (Primary broadcasts to secondaries, receives watermark ACKs, computes quorum), serves APPEND/READ/SEAL requests. Primary runs background S3 flusher for sealed extents and broadcasts ForwardFlushed to secondaries. Uses client to heartbeat to Stream Manager. Built into a binary via `src/bin/extent-node.rs`. |
| **stream-manager** | lib | Stream Manager logic. Manages stream->extent mappings, orchestrates seal-and-new, allocates extents across Extent Nodes, persists metadata to MySQL. Uses client to issue seal/allocate to Extent Nodes. Built into a binary via `src/bin/stream-manager.rs`. |

The `client` crate is used internally by both process types: Extent Node uses it to send keepalive heartbeats to Stream Manager, and Stream Manager uses it to issue seal/allocate commands to Extent Nodes. It is also the protocol interface for external consumers -- any client can re-implement the same wire format in their language of choice.

### Key Rust Dependencies

| Crate | Purpose |
|-------|---------|
| `tokio` | Async runtime, TCP server, task scheduling |
| `bytes` | Zero-copy byte buffers for broadcast replication forwarding |
| `aws-sdk-s3` | S3-compatible object storage client |
| `sqlx` | Async MySQL client for Stream Manager metadata |
| `moka` | Concurrent LRU cache for S3 read cache |
| `tokio-util` | Codec framework for TCP frame encoding/decoding |
| `tracing` | Structured logging and distributed tracing |
| `serde` + `toml` | Configuration file deserialization (TOML format) |
| `crc32fast` | CRC32 checksums for extent data integrity (replication + S3) |
| `zstd` | Zstandard compression for S3 extent chunks |
| `lz4` | LZ4 compression for S3 extent chunks (alternative to zstd) |

## Replication: Broadcast Replication

Each active extent has an N-node replica set determined by the configurable replication factor (RF). Default RF=2: Primary + one Secondary. RF=1: single node (no forwarding). RF=N: Primary + (N-1) Secondaries.

Unlike chain replication where writes flow sequentially through the chain (O(N) hops), broadcast replication has the Primary fan out appends to **all Secondaries in parallel** (O(1) hop latency). Quorum-based ACKs allow the system to tolerate minority replica failures without blocking.

### Write Path

The Primary is the sole append acceptor. It assigns monotonic sequence numbers and broadcasts appends to all Secondaries in parallel. Each Secondary buffers the append and returns a **cumulative watermark ACK** directly to the Primary. The Primary tracks watermarks from all Secondaries and computes a **quorum offset** -- the highest offset confirmed by at least `RF/2` Secondaries (plus the Primary itself). The Primary ACKs clients in-order: only when their offset <= the quorum offset.

```
CLIENT        PRIMARY             SECONDARY_1          SECONDARY_2 (RF=3)
  |              |                   |                      |
  |--APPEND(m1)->|                   |                      |
  |              |--FWD(m1)--------->|                      |
  |              |--FWD(m1)------------------------------->>|
  |--APPEND(m2)->|                   |                      |
  |              |--FWD(m2)--------->|                      |
  |              |--FWD(m2)------------------------------->>|
  |--APPEND(m3)->|                   |                      |
  |              |--FWD(m3)--------->|                      |
  |              |--FWD(m3)------------------------------->>|
  |              |                   |                      |
  |              |<--WATERMARK(3)----|                      |  (S1 committed up to 3)
  |              |                   |                      |
  |              |    quorum met: Primary + S1 = 2 of 3     |
  |<--ACK(m1)---|                   |                      |
  |<--ACK(m2)---|                   |                      |
  |<--ACK(m3)---|                   |                      |
  |              |                   |                      |
  |              |<--WATERMARK(3)----------------------------|  (S2 committed, but quorum already met)
```

1. Client sends APPEND to Primary. Primary assigns monotonic sequence number, buffers in memory.
2. Primary broadcasts the append to **all Secondaries in parallel** using the dedicated Forward opcode (0x05), which carries the primary-assigned `byte_pos` for deterministic replication.
3. Each Secondary buffers the append and sends a cumulative WATERMARK ACK back to Primary with its highest committed offset.
4. Primary tracks per-secondary watermarks in an AckQueue. It computes the quorum offset: sorts secondary watermarks descending, takes the k-th value where `k = RF/2`.
5. Primary ACKs all pending clients whose offset <= quorum offset (deferred response via per-connection channel).

**Quorum formula**: `required_acks = RF / 2` (integer division). For RF=2: need 1 secondary ACK. For RF=3: need 1 of 2 secondary ACKs. For RF=1: no secondary ACKs needed (single node).

**In-order ACK guarantee**: Primary never ACKs offset N to a client before all offsets < N have reached quorum. This ensures clients observe a consistent, gap-free commit sequence.

**Durability**: Pure in-memory N-way replication (no local WAL). With RF=2, data survives any single node failure. Higher RF tolerates more simultaneous failures. Acceptable trade-off given frequent S3 flush intervals.

**Deferred ACK**: The Primary's request handler returns `None` (no immediate response) for client APPEND requests. The WatermarkHandler, running on the Primary's connection read task for each Secondary, sends AppendAck responses through a per-connection channel (`response_tx`) when the quorum offset advances past pending client offsets.

### Read Path

- **Hot data** (active/sealed-in-memory extents): Read from any replica.
- **Cold data** (flushed extents): Read from S3 via read cache.

### Extent-Node Concurrency: Stream-Level Pipelined Group Commit

Each stream on an Extent Node uses a **pipelined group commit** pattern to maximize append throughput under high concurrency. Instead of multiple writers contending on atomic cursors (which causes cache-line bouncing), a **leader election at the stream level** delegates all writes to a single active writer per stream. This means the leader can transparently handle extent-full transitions (seal + create new extent + retry) within its own turn — no re-election needed.

#### Arena Layout

Each active extent pre-allocates a contiguous buffer (configurable, default 64 MiB per stream via `extent_capacity`). Records are stored sequentially in the arena in wire format: `[payload_len: u32 BE][payload: bytes]`. This is the same format as the S3 object body, enabling zero-copy upload of sealed extents.

The arena has no internal index structure. Records are self-contained: a reader can walk forward from any byte position by reading the length prefix and advancing by `4 + len` bytes. Random access is provided by an **internal index** (see below).

```
Extent Arena (pre-allocated contiguous buffer, configurable size):

  ┌─────────────────────────────────────────────────────────────┐
  │  [len|payload][len|payload][len|payload][   free space   ]  │
  │  ^                                     ^                    │
  │  0                               write_cursor               │
  └─────────────────────────────────────────────────────────────┘

  write_cursor    : AtomicU64 — byte offset of next free slot
  record_count    : AtomicU64 — number of records (sequence counter)
  committed_seq   : AtomicU64 — all records with seq < committed_seq are readable
  committed_bytes : AtomicU64 — byte position up to which all records are fully written

Stream-level (not per-extent):
  in_flight       : AtomicU64 — leader election counter (0 = idle)
  job_tx/job_rx   : crossbeam unbounded channel for follower delegation
```

#### Append Path (Stream-Level Pipelined Group Commit)

```
Writer A arrives at stream: in_flight.fetch_add(1) → prev=0 → LEADER
  ├─ try_append_active(payload_A)
  │   ├─ append_inner on active extent → OK
  │   └─ return (AppendResult, extent_id)
  ├─ broadcast Forward (if RF≥2) or send AppendAck (if RF=1)
  ├─ in_flight.fetch_sub(1) → remaining=3 → drain batch
  │
  │  Writer B arrives: in_flight.fetch_add(1) → prev=1 → FOLLOWER
  │  ├─ push AppendJob to stream.job_tx
  │  └─ return None (deferred)
  │
  │  Writer C arrives: in_flight.fetch_add(1) → prev=2 → FOLLOWER
  │  ├─ push AppendJob to stream.job_tx
  │  └─ return None (deferred)
  │
  └─ drain loop:
     ├─ recv jobs [B, C] from stream.job_rx
     ├─ try_append_active(payload_B) → ExtentFull!
     │   ├─ seal current extent, create_next_extent()
     │   ├─ retry append on new extent → OK
     │   └─ return (AppendResult, new_extent_id, SealNotification)
     ├─ send Forward for B with new_extent_id
     ├─ try_append_active(payload_C) → OK (on new extent)
     ├─ send Forward for C with new_extent_id
     ├─ in_flight.fetch_sub(2) → remaining=0 → done
     └─ break
```

Detailed steps:

1. **Stream-level leader election**: `stream.in_flight.fetch_add(1, Acquire)`. If `prev == 0`, the thread is the **active writer** for the entire stream (fast path). If `prev > 0`, an active writer exists — push `AppendJob` to the stream's channel and return immediately (slow path).

2. **Single-writer append** (`try_append_active` → `append_inner`): The leader uses plain `load`/`store` on `write_cursor` and `record_count` (no `fetch_add`). Same memcpy as before. Direct `store` of `committed_bytes`, index entry, and `committed_seq` — no spin-wait needed since there's only one writer.

3. **Extent-full transition** (inline, within leader's turn): If `append_inner` returns `ExtentFull`, the leader drops the shared DashMap ref, acquires an exclusive ref, calls `seal_and_create_next()`, re-acquires shared ref, retries. All within the same leader turn — no re-election, no race. Followers queued during the transition are processed on the new extent.

4. **Replication / ACK**: After append, the leader checks `ReplicaInfo`:
   - **RF=1 / standalone / no replica**: Send immediate `AppendAck` via `response_tx`.
   - **RF≥2 Primary**: Broadcast `Forward` to all secondaries with the **actual extent_id** the record landed on (may differ from the client's request if transition happened), queue `PendingAck`.

5. **Batch drain**: After own append, `in_flight.fetch_sub(1, Release)`. If `remaining > 1`, drain `stream.job_rx` and process each follower's payload through `try_append_active` (which handles extent-full inline). `in_flight.fetch_sub(batch_size, Release)` after each batch. Loop until `remaining ≤ batch_size`.

6. **Follower return**: Followers return `None` immediately. Their ACK (or error) is sent via `response_tx` by the leader.

#### Atomic Ordering Analysis

- **`fetch_add(1, Acquire)` on entry**: If we see `prev > 0`, Acquire ensures visibility of the leader's prior operations. If `prev == 0`, harmless.
- **`fetch_sub(1, Release)` after own append**: Release ensures the next reader (via Acquire in fetch_add) sees committed writes to the arena.
- **`fetch_sub(batch_size, Release)` after batch drain**: Same Release semantics. The returned value determines whether to loop.

Why not AcqRel everywhere? The `fetch_sub` doesn't need Acquire — the draining leader already has full visibility. The `fetch_add` doesn't need Release — the follower publishes via channel, not via atomic. Minimal orderings reduce overhead on ARM.

#### Internal Extent Index (Compressed u32 Pointers)

Each extent maintains an **internal index** — a lock-free array mapping sequence numbers to byte positions within the arena. The index is populated atomically inside `append_inner()` after the commit stores succeed, and used during reads to resolve logical offsets to physical byte positions. There is no separate `IndexExtent` struct; the index is absorbed directly into `Extent`.

**Index structure:**
- `Box<[AtomicU32]>` — one entry per possible record in the extent, using compressed 32-bit pointers (sufficient for 64 MiB arenas, max byte_pos < 2^32).
- Entry `i` stores the byte_pos for the `i`-th record (where `i = offset - extent.start_offset`).
- Capacity = `extent_capacity / 5` (minimum record = 4 byte header + 1 byte payload).
- Sentinel value `u32::MAX` distinguishes unwritten entries from byte_pos=0.
- Memory savings: 4 bytes per entry vs 8 bytes with `AtomicU64` — halves index memory overhead.

**Write path:** Inside `append_inner()`, after `committed_bytes.store(Release)`, the writer records `index[seq].store(byte_pos as u32, Release)`. Single-writer guarantee means no contention.

**Ordering analysis:** The reader's Acquire load on `index[seq]` synchronizes-with the writer's Release store. Since the index store happens after `committed_bytes.store(Release)`, which happens after the payload memcpy, the reader is guaranteed to see the fully written payload. On x86-64, `AtomicU32` with Release/Acquire compiles to plain `mov` instructions (TSO provides the ordering for free) — zero runtime cost.

**Read path:** When a client sends `READ(stream_id, offset, count)`:
1. The server locates the extent containing the requested offset.
2. Computes `seq = offset - extent.start_offset`.
3. Looks up `byte_pos = extent.index_lookup(seq)` with a single atomic load (Acquire ordering).
4. Reads `count` records forward from `byte_pos` in the data arena.
5. Returns zero-copy `Bytes` slices referencing the arena buffer.

**Key benefits:**
- **Clients only need logical offsets.** The `byte_pos` concept is invisible to the external API (wire protocol, client library, AppendResult).
- **O(1) random reads** — no sequential walk from byte 0. Index lookup is a direct array access.
- **No additional I/O** — the index lives in-memory alongside the data extent.
- **Lock-free reads** — readers use atomic loads on `committed_bytes` and index entries, never blocking the writer.
- **Single-struct ownership** — one `Extent` owns both data and index, simplifying lifecycle management.

#### Configurable Extent Capacity

The extent capacity is configurable per stream (default 64 MiB). Application users can tune this per workload:

- **Smaller extents**: More frequent seal-and-new, lower write amplification for small streams, faster S3 flush cycles.
- **Larger extents**: Fewer seals, better for high-throughput streams that batch many messages per extent.

The capacity is applied to all extents created for the stream, including new extents created during seal-and-new.

#### Seal

Sealing sets `limit` atomically. The store layer waits for `in_flight == 0` (leader has finished draining), then reads the final `record_count`. Subsequent appends see the limit and return `ExtentSealed`. The `committed_seq` at seal time is the definitive record count reported to Stream Manager.

#### Properties

| Property | Guarantee |
|----------|----------|
| Offset uniqueness | Single writer assigns sequences via plain `load`/`store` — no contention |
| No overlap | Single writer advances `write_cursor` — each record occupies a disjoint region |
| Read consistency | `committed_bytes` advances in-order; readers see a gap-free prefix |
| Zero-copy reads | `Bytes::slice` into the arena buffer; no allocation or copy |
| S3 flush | Sealed extent records encoded into chunk-compressed S3 format; uploaded by Secondary-1 |
| No mutex on hot path | Leader election uses a single `fetch_add`; followers push to unbounded channel |
| O(1) random read | Internal extent index resolves offset→byte_pos; no sequential walk needed |
| Scalable under contention | Followers delegate to leader, eliminating cache-line bouncing |

### Failure Handling

1. Stream Manager detects node failure (heartbeat timeout = 1.5x declared interval).
2. Stream Manager seals the current extent with the end_offset from metadata (the dead node cannot report).
3. Stream Manager allocates new extent with new replica set on healthy nodes.
4. Writes resume immediately. Failed replica is lazily re-replicated.

## Multi-Dispatch: Shared Data Stream + Index

### Design

When a message is published to a topic matching multiple subscribers, instead of duplicating the body:

1. Write message body **once** to a **Data Stream** -> returns `(data_stream_id, offset)`.
2. For each subscriber, append a lightweight **index entry** to their **Index Stream**: `(data_stream_id, offset, msg_len)`.

Index entries are ~32 bytes. Data stream writes go through broadcast replication. Index writes are dispatched asynchronously.

### Atomicity

- The data stream write future completes after data stream write ACK.
- Index writes are async. A background **Reconciler** ensures all expected index entries exist.
- Eventual consistency is acceptable for the index stream (the data stream itself is fully consistent via quorum ACK).

### Read Path

```
read(stream, offset, count)
  -> Read Index Stream entries [offset..offset+count]
  -> Batch-resolve data stream references
  -> Read message bodies from Data Stream (memory or S3)
  -> Return batch of messages
```

## S3 Flush

### Triggers

| Trigger | Default Threshold | Rationale |
|---------|-------------------|-----------|
| Size | 64-256 MB | Efficient S3 object size |
| Time | 30-60 seconds | Bounds data-at-risk window |
| Node failure | Immediate | Seal-and-new |
| Extent full | Immediate (Primary-driven) | Arena exhausted; Primary proactively seals and notifies Stream Manager |

### S3 Object Layout

Extent data is stored in S3 using a chunk-compressed format designed for random-access range reads.

**S3 key**: `{namespace}/data/{stream_id}/{start_offset}_{end_offset}.dat`

The key uses offset ranges (not extent IDs) to support future compaction of small extents into merged objects.

```
┌─ Header (fixed 64 bytes) ──────────────────────────────┐
│  magic              : u32  (0x53455854 "SEXT")          │
│  version            : u16  (2)                          │
│  flags              : u16  (reserved, 0)                │
│  stream_id          : u64                               │
│  start_offset       : u64                               │
│  end_offset         : u64  (exclusive)                  │
│  record_count       : u32                               │
│  index_interval     : u32  (64)                         │
│  chunk_count        : u32  (ceil(record_count / 64))    │
│  data_size          : u32  (total compressed data bytes)│
│  crc32              : u32  (over chunk index + data)    │
│  compression        : u8   (0=none, 1=zstd, 2=lz4)     │
│  _reserved          : [u8; 11]                          │
├─ Chunk Index ───────────────────────────────────────────┤
│  [chunk_count × u32]                                    │
│  entry[i] = byte offset of chunk[i] within data section │
├─ Data (compressed chunks) ──────────────────────────────┤
│  chunk[0]: compress(records[0..64])                     │
│  chunk[1]: compress(records[64..128])                   │
│  ...                                                    │
│  Each chunk is independently (de)compressible.          │
│  Uncompressed: [len:u32 BE][payload]... (arena format)  │
└─────────────────────────────────────────────────────────┘
```

**Chunk-based compression**: Records are grouped into chunks of 64 (aligned with the sparse index interval). Each chunk is compressed independently using the configured algorithm (zstd or lz4), enabling random-access reads without decompressing the entire object. The chunk index stores byte offsets into the compressed data section, so a reader can fetch a single chunk via an S3 range read and decompress it locally.

**Index interval = 64**: Chosen to ensure the header + chunk index fits within a 1 MiB initial S3 range read even for large extents (~1 GiB data, ~1 KiB avg record size → ~16K records → 256 chunks → 1 KiB index).

**Flush role**: The Primary uploads sealed extents to S3 via a background flusher task. After successful upload, the Primary broadcasts `ForwardFlushed` (Forward opcode 0x05, flag=0x03) to all secondaries so they can mark the extent as eligible for memory eviction. The Primary also sends `UpdateExtentFlushed` to SM, which transitions the extent state from Sealed to Flushed in the database. This reuses the existing broadcast replication channels — no additional infrastructure is needed for eviction notification.

**Compression config**: Global EN setting (`s3_compression` in config, default "none"). Valid values: "none", "zstd", "lz4". Per-stream compression may be added in the future.

### Post-Flush

1. Primary broadcasts `ForwardFlushed` to all secondaries after successful S3 upload.
2. Primary sends `UpdateExtentFlushed` to Stream Manager.
3. Stream Manager transitions extent state from Sealed to Flushed in MySQL (idempotent, epoch-validated).
4. All replicas (Primary + secondaries) can evict the extent from memory (per-stream `cache_extents` policy, default 4).

## Stream Manager Metadata

Stored in MySQL. Uses sqlx (async MySQL client) and Refinery (schema migrations).

### Tables

```sql
CREATE TABLE stream (
    stream_id            INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    stream_name          VARCHAR(512) NOT NULL UNIQUE,
    stream_type          VARCHAR(32) NOT NULL DEFAULT 'DATA',
    replication_factor   TINYINT UNSIGNED NOT NULL DEFAULT 2,
    extent_capacity      INT NOT NULL DEFAULT 67108864,   -- initial arena size (default 64 MiB)
    min_extent_capacity  INT NOT NULL DEFAULT 8388608,    -- floor for adaptive shrink (8 MiB)
    max_extent_capacity  INT NOT NULL DEFAULT 268435456,  -- ceiling for adaptive growth (256 MiB)
    extent_growth_factor TINYINT UNSIGNED NOT NULL DEFAULT 2,          -- adaptive growth multiplier
    cache_extents        SMALLINT UNSIGNED NOT NULL DEFAULT 4,          -- max extents retained in memory
    storage_medium       TINYINT UNSIGNED NOT NULL DEFAULT 0,           -- 0 = S3, 1 = Memory
    epoch                INT NOT NULL DEFAULT 0,          -- current stream epoch
    created_at           DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3),
    updated_at           DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
);

CREATE TABLE extent (
    stream_id     INT UNSIGNED NOT NULL,
    epoch         INT NOT NULL DEFAULT 0,
    extent_id     INT NOT NULL,
    start_offset  BIGINT NOT NULL,
    end_offset    BIGINT NOT NULL DEFAULT 0,
    state         TINYINT NOT NULL DEFAULT 1,   -- ExtentState: 1=Active, 2=Sealed, 3=Flushed
    s3_key        VARCHAR(1024),
    created_at    DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3),
    sealed_at     DATETIME(3) NULL,
    flushed_at    DATETIME(3) NULL,
    PRIMARY KEY (stream_id, extent_id),
    INDEX idx_stream_state (stream_id, state)
);

CREATE TABLE stream_replica (
    stream_id     INT UNSIGNED NOT NULL,
    epoch         INT NOT NULL,                 -- epoch this replica set belongs to
    node_addr     VARCHAR(256) NOT NULL,
    role          TINYINT NOT NULL DEFAULT 0,   -- 0=Primary, 1+=Secondary
    created_at    DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3),
    updated_at    DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (stream_id, epoch, node_addr),
    INDEX idx_node (node_addr)
);

CREATE TABLE stream_sequence (
    stream_id      INT UNSIGNED PRIMARY KEY,
    next_extent_id INT NOT NULL DEFAULT 0,
    created_at     DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3),
    updated_at     DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
);

CREATE TABLE node (
    node_id               VARCHAR(256) PRIMARY KEY,
    addr                  VARCHAR(256) NOT NULL,
    heartbeat_interval_ms INT NOT NULL DEFAULT 5000,
    last_heartbeat        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    state                 TINYINT NOT NULL DEFAULT 1  -- NodeState: 0=Unspecified, 1=Alive, 2=Dead
);

-- Planned for future: subscription offset tracking (not yet implemented).
-- CREATE TABLE stream_offset (
--     subscription_id VARCHAR(512) NOT NULL,
--     stream_id      BIGINT NOT NULL,
--     committed_offset BIGINT NOT NULL DEFAULT 0,
--     updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
--     PRIMARY KEY (subscription_id, stream_id)
-- );

CREATE TABLE node_metrics (
    node_id                VARCHAR(256) PRIMARY KEY,
    available_memory_bytes BIGINT UNSIGNED NOT NULL DEFAULT 0,
    total_memory_bytes     BIGINT UNSIGNED NOT NULL DEFAULT 0,
    appends_per_sec        INT UNSIGNED NOT NULL DEFAULT 0,
    active_extent_count    INT UNSIGNED NOT NULL DEFAULT 0,
    bytes_written_per_sec  BIGINT UNSIGNED NOT NULL DEFAULT 0,
    updated_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE stream_manager_leadership (
    id          INT PRIMARY KEY DEFAULT 1,       -- single-row table
    node_id     VARCHAR(256) NOT NULL DEFAULT '', -- current leader's listen address
    lease_until DATETIME NOT NULL                 -- lease expiry (DB server time)
);
```

### Stream Manager High Availability

The Stream Manager is fully stateless — all durable state lives in MySQL. This enables multi-active SM deployments for high availability.

#### Stateless Design

- **No in-memory caches**: Node metrics for load-aware placement are persisted to `node_metrics` on every heartbeat and read from DB on each allocation.
- **All reads go to MySQL**: Streams, extents, replicas, node liveness — queried from DB for every operation.
- **Any SM handles any request**: Client requests (CreateStream, Seal, Describe, Seek) are routed to any available SM.

#### Leadership Lease

A single-row `stream_manager_leadership` table provides leader election:

- **Acquire**: `UPDATE ... SET node_id = ?, lease_until = NOW() + interval WHERE lease_until < NOW() OR node_id = ?`
- **Renew**: `UPDATE ... SET lease_until = NOW() + interval WHERE node_id = ?`
- **Release**: `UPDATE ... SET node_id = '' WHERE node_id = ?` (graceful shutdown)

Only the leader runs the heartbeat checker (dead node detection + failover). Default lease duration: 10 seconds, renewed every 3 seconds (heartbeat check interval).

#### Consistency Guarantees

| Operation | Safety Mechanism |
|-----------|------------------|
| Concurrent seals | `SELECT ... FOR UPDATE` on extent row |
| Concurrent epoch bumps | CAS guard: `UPDATE ... WHERE epoch = ?` |
| Concurrent node registration | Upsert: `INSERT ... ON DUPLICATE KEY UPDATE` |
| Concurrent heartbeat | Last-write-wins: `last_heartbeat = NOW()` |
| Double failover | Leader-only + fenced `bump_epoch` + idempotent `seal_and_allocate` |

#### Failover Sequence

1. Active SM holds leadership lease, runs heartbeat checker.
2. SM crashes (or lease expires after 10s).
3. Another SM detects expired lease on next check interval, acquires it.
4. New leader runs heartbeat checker, detects expired ENs, executes failover.
5. ENs reconnect to any available SM (via VIP or address list).

No data is lost because all durable state lives in MySQL. The failover window (during which no heartbeat checker runs and no new streams can be created) is bounded by `lease_duration + heartbeat_check_interval`.

### Offset Translation

```
read(stream, offset=1050, count=10)
  -> Find extent where start_offset <= 1050 < end_offset
  -> local_offset = 1050 - start_offset
  -> Read from extent (memory or S3)
```

## Implementation Phases

### Phase 1: Rust Storage Service Foundation
- Rust project scaffolding (Cargo workspace, CI)
- Custom TCP protocol: frame codec, connection handler (Tokio)
- Extent data structure: lock-free pre-allocated arena with atomic cursors, seal state machine
- Stream abstraction: ordered extent list, active extent tracking
- Basic single-node operation (no replication yet): APPEND, READ, QUERY_OFFSET
- Unit tests for extent lifecycle and protocol codec

### Phase 2: Broadcast Replication
- Broadcast replication protocol: configurable RF (Primary broadcasts to all Secondaries in parallel)
- Quorum-based ACK: Primary waits for RF/2 secondary cumulative watermark ACKs before ACKing clients
- Deferred ACK mechanism: WatermarkHandler sends responses through per-connection channel when quorum advances
- Stream Manager-driven seal: Stream Manager queries each Extent Node for commit length, takes min, allocates new replica set
- Stream Manager sends RegisterExtent to each Extent Node after extent allocation (Primary gets secondary addrs, Secondaries get empty addrs)
- Stream Manager: extent allocation across nodes, seal orchestration
- Failure detection (heartbeat) and seal-and-new recovery
- MySQL metadata store (sqlx) for extent/stream/replica/node tables
- Integration tests with multi-node setup

### Phase 3: S3 Flush and Read
- S3 extent codec: chunk-compressed binary format with 64-byte header, sparse chunk index, and independently compressible 64-record chunks (zstd/lz4/none)
- S3 Flusher: background task on Primary encodes sealed extents and uploads via aws-sdk-s3 with retry
- ForwardFlushed broadcast: Primary → Secondaries after successful upload, enabling eviction across all replicas
- UpdateExtentFlushed notification: Primary → SM after successful upload; SM transitions Sealed → Flushed in MySQL
- S3 Reader: range-read with local LRU cache (moka)
- Flush triggers: size, time, node failure
- Post-flush memory eviction
- Integration tests with MinIO container

### Phase 4: Multi-Dispatch (Data + Index Streams)
- Index stream: lightweight pointer entries
- APPEND opcode support for multi-dispatch (data body + index targets)
- Reconciler: background index consistency checker
- Batch read path: index entries -> data stream lookups

## Key Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Process model | Dedicated Rust process | No GC pauses, zero-copy I/O, precise memory control, enforced architectural boundary |
| Implementation language | Rust (Tokio async runtime) | Deterministic memory, zero-cost abstractions, mature async ecosystem (aws-sdk-s3, sqlx, moka) |
| RPC protocol | Custom TCP with fixed header + variable header + payload | Minimal overhead, zero-copy broadcast forwarding, per-opcode wire layout, full control over batching and framing |
| Object storage API | S3-compatible | Widest ecosystem (AWS, MinIO, Ceph, Alibaba OSS S3-compat) |
| Replication protocol | Broadcast replication with quorum ACK | O(1) hop latency (vs O(N) for chain), tolerates minority failures, simple parallel fan-out |
| Durability before S3 | Pure in-memory N-way (default 2-way) | Low latency; single-node failure tolerated; S3 flush bounds risk |
| Stream concurrency | Stream-level pipelined group commit with leader election, lock-free arena with internal compressed index for O(1) reads | Single active writer per stream eliminates cache-line bouncing; extent-full transition is inline (no re-election); followers delegate via channel; batch drain amortizes cost; no mutex on hot path |
| Multi-dispatch | Shared data + index streams | Storage efficient; avoids body duplication across subscribers |
| Stream Manager metadata store | MySQL (sqlx) | Reuses existing infra; metadata ops are infrequent (per-extent, not per-message) |
| Consistency model | Seal-and-new (WAS) | Separates consistency (sealed extent) from availability (new extent) |
| Seal-and-new readiness | SM waits for Primary `RegisterExtent` ack; secondaries create extents lazily on first forwarded append | Guarantees Primary is ready before clients learn about new extent; eliminates secondary registration race; reduces critical path to 1 SM↔Primary RTT |
