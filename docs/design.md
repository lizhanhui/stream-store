# Object Storage Backend with Replicated In-Memory Layer

## Motivation

Replace RocketMQ broker's cloud block-based disk storage with object storage (S3-compatible) to reduce cost. Object storage has low IOPS and higher latency, so a replicated in-memory layer is required to serve hot data. The WAS (Windows Azure Storage) stream employs seal-and-new mechanism to achieve consistency,  availability and partition (CAP) at the same time through separation of concern.

## Core Abstractions

### Stream

An ordered, append-only sequence of messages/records. 

Stream has two types:

- **Data Stream**: Stores sequence of messages/records data.
- **Index Stream**: Stores lightweight pointers into data streams.

### Extent

The unit of replication and the unit of S3 flush. A stream is composed of an ordered list of extents.

- **Active (unsealed, mutable)**: At most one per stream. Accept appends. Replicated via broadcast replication across a configurable number of nodes (replication factor, default 2).
- **Sealed**: Immutable. Eligible for S3 flush. Once flushed to S3, they can be evicted from memory. 
- **Flushed**: Sealed + uploaded to S3.  Served from S3 (with read cache) on demand. Flushed extents are supposed to be evicted from memory to free up space for active extents and new appends. They are subject to TTL-based policy in memory and S3.

### Seal-and-New

When a trigger fires (size threshold, time interval, node failure, or **extent full**):

A single `SEAL` opcode (0x05) covers all trigger sources. `Flags` bit 0 (`FLAG_OFFSET_PRESENT`) distinguishes whether the caller provides the resolved end offset:

**Client Seal** (`FLAG_OFFSET_PRESENT = 0`):
1. Client sends `Seal(stream_id, extent_id)` to Stream Manager.
2. Stream Manager sends `Seal` RPC to **each Extent Node holding a replica** (Primary and all Secondaries). Each Extent Node stops accepting appends and responds with its local commit length.
3. Stream Manager determines the committed offset: if the Primary responded, its quorum offset is used (most accurate). Otherwise, SM computes the committed offset from Secondary responses using quorum math (sorts offsets descending, takes the k-th value where `k = RF/2`).
4. Stream Manager updates extent metadata to SEALED with the committed end_offset.
5. Stream Manager allocates a **new** active extent on (potentially different) healthy nodes, sends `RegisterExtent` to each new Extent Node.
6. Stream Manager responds to client with the new extent info (Primary address). Writes resume immediately.

**Extent-node Seal** (`FLAG_OFFSET_PRESENT = 1`):
1. Primary ExtentNode proactively seals (e.g. arena full) and sends `Seal(stream_id, extent_id, offset)` with `FLAG_OFFSET_PRESENT` set to Stream Manager. The `offset` is the committed end_offset.
2. Stream Manager trusts the reported offset and records it as the extent's `end_offset` in metadata.
3. Stream Manager updates extent metadata to SEALED.
4. Stream Manager **fire-and-forgets** Seal RPCs to secondary extent nodes only (`tokio::spawn` -- does not block the response), skipping the Primary (already sealed locally). This ensures secondaries learn about the seal asynchronously.
5. Stream Manager allocates a new active extent and responds to the Extent Node with the new extent info.

Both paths share the same downstream procedure in Stream Manager: seal in MySQL (transaction), allocate new extent, notify ExtentNodes via RegisterExtent.

**ExtentFull handling**: When the Primary's arena is exhausted, it takes two actions:

1. **Returns `ErrorCode::ExtentFull` (5)** to the client whose append triggered the overflow. This client knows to retry after obtaining the new extent from Stream Manager.
2. **Proactively seals the extent and sends `Seal(stream_id, extent_id, offset)` with `FLAG_OFFSET_PRESENT` to Stream Manager** in the background. The `offset` is the committed end_offset that Stream Manager trusts without querying replicas. Stream Manager updates metadata, fire-and-forgets Seal RPCs to secondary ExtentNodes, allocates a new extent, and responds -- all before most clients even see the error.

This avoids an error storm where every concurrent client independently discovers the extent is full and races to trigger seal-and-new. Only the Primary initiates the seal -- once. Subsequent clients that arrive after the local seal see `ExtentSealed` and call `DescribeStream(count=1)` to get the new extent, which is already being allocated.

**Consistency** is resolved on the sealed extent (backward-looking). **Availability** is provided by the new extent (forward-looking). The system never blocks writes to achieve consistency.

## Architecture

The storage layer runs as a **dedicated Rust process** (`stream-store`), separate from the Java MQTT proxy. This separation provides:

- **No GC pauses**: The storage service holds gigabytes of in-memory message data. Java GC stop-the-world events at this scale would stall replication ACKs and cause false failure detections. Rust gives deterministic deallocation.
- **Zero-copy I/O**: Broadcast replication forwards bytes from Primary to all Secondaries. Rust's `bytes::Bytes` (reference-counted buffers) enables zero-copy forwarding without the copy overhead of Java ByteBuf conversions.
- **Precise memory control**: The service has a hard memory budget. Rust enforces it precisely. Java's RSS is opaque due to JVM overhead, metaspace, and GC headroom.
- **Enforced boundary**: A process boundary prevents accidental coupling of MQTT protocol logic with storage internals.

### Process Architecture

```
  Java Process (MQTT Proxy)              Rust Process (Storage Service)
 ┌─────────────────────────┐            ┌──────────────────────────────┐
 │  mqtt-cs                │            │  stream-store (Rust/Tokio)   │
 │  (Connection Server)    │            │                              │
 │  - MQTT protocol        │            │  Stream Manager              │
 │  - Session management   │            │  - Extent lifecycle          │
 │  - Subscription match   │  Custom    │  - Seal-and-new              │
 │                         │  TCP       │  - Metadata (MySQL client)   │
 │  mqtt-ds                │ ◄────────► │                              │
 │  (Data Server)          │  Protocol  │  Extent Nodes                │
 │  - StreamStoreClient    │            │  - In-memory extents         │
 │    (implements          │            │  - Broadcast replication     │
 │     LmqQueueStore)      │            │  - S3 flush                  │
 │  - Pop state (MySQL)    │            │  - Read cache                │
 │  - Notify/routing       │            │                              │
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
        +-----------+                             +-----+------+
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

All Extent Nodes, S3 Flusher, and S3 Reader run as Extent Node processes. Stream Manager nodes run as separate Stream Manager processes. The Java MQTT proxy communicates with the Rust storage service via a custom TCP protocol.

**Stream Manager Clustering**: Stream Manager is peer-based -- all Stream Manager nodes are equivalent and can handle any request. MySQL provides transactional metadata coordination (the database is the single source of truth), so no Stream Manager-level leader election or consensus is needed. Stream Manager nodes register themselves in the database and broadcast membership changes to Extent Nodes and clients via `STREAM_MANAGER_MEMBERSHIP_CHANGE` frames.

### Components

| Component | Language | Role |
|-----------|----------|------|
| **MQTT Proxy (mqtt-cs, mqtt-ds)** | Java | Protocol handling, session state, subscription matching, message routing. `StreamStoreClient` implements `LmqQueueStore` as a TCP client to the Rust service. |
| **Storage Service (stream-store)** | Rust | Dedicated process. Extent nodes, stream manager, broadcast replication, S3 flush/read. |
| **Stream Manager** | Rust | Metadata coordinator within storage service. Manages stream->extent mappings, seal/allocate, offset translation. MySQL client for metadata persistence. |
| **Extent Node** | Rust | Holds in-memory extent replicas. Participates in broadcast replication (Primary broadcasts, Secondaries ACK). |
| **S3 Flusher** | Rust | Background task on Extent Node. Uploads sealed extents to S3 via `aws-sdk-s3`. |
| **S3 Reader** | Rust | Fetches flushed extents from S3 with local LRU read cache. |

### Custom TCP Wire Protocol

MQTT-style **Fixed Header + Variable Header + Payload** format for minimal overhead and zero-copy forwarding. Each opcode defines its own variable header layout; only fields relevant to that operation are on the wire. The payload section carries arbitrary application data (e.g., message bytes for APPEND) and is always length-prefixed.

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
| Version | 1B | Protocol version (currently 1) |
| Opcode | 1B | Operation type (see below) |
| Flags | 1B | Per-opcode flags (e.g., `FLAG_FORWARDED = 0x01` for replication) |
| Remaining Length | 4B | Total bytes of variable header + payload section that follow the fixed header |

**Variable Header**: Determined entirely by the Opcode (and sometimes Flags). Each opcode section below specifies the exact fields and their order. Fields carry protocol-level metadata specific to the operation (stream IDs, offsets, extent IDs, counts, request IDs, etc.). Only the fields meaningful for that opcode appear on the wire. Request ID is a variable header field present in request-response opcodes, absent in fire-and-forget opcodes (e.g., WATERMARK, SM_MEMBERSHIP_CHANGE).

**Payload**: Carries arbitrary application data from the ultimate user (e.g., message bytes for APPEND, error description for ERROR). When present, a 4-byte `Payload Length` prefix precedes the payload bytes. Opcodes that carry no application payload omit both the length prefix and the payload bytes entirely.

**Rust Representation**: The `Frame` type uses a `FixedHeader` + `VariableHeader` enum + `Option<Bytes>` payload design. Each opcode is a distinct `VariableHeader` variant containing only the fields valid for that opcode — invalid field combinations are rejected at compile time. Flag-dependent fields (e.g., `Seal.offset`, `SealAck.new_extent_id`) use `Option<T>`; the flags byte is computed during encode from the `Option` state, eliminating stale-flag bugs.

#### Opcodes

Grouped by category with gaps for future growth.

**Data path (0x01-0x0F) -- Client <-> Extent Node**

##### 0x01 APPEND (Client -> Primary; Primary -> Secondary when forwarded)

Append a message to a data stream. `Flags` bit 0 (`FLAG_FORWARDED`): 0 = client request, 1 = forwarded from Primary to Secondary for broadcast replication.

```
Fixed Header (8B)
  Flags: bit 0 = FLAG_FORWARDED
Variable Header:
  [stream_id    : u64]    -- target stream
  [extent_id    : u32]    -- target extent (for server-side validation)
Payload:
  [payload_len  : u32]    -- length of message bytes
  [payload      : bytes]  -- message body (application data)
```

When forwarded (Primary -> Secondary), `request_id = 0` (not meaningful) and the Secondary derives the logical offset locally from its arena's `record_count`.

##### 0x02 APPEND_ACK (Primary -> Client)

Confirms a successful append after quorum ACK is achieved.

```
Fixed Header (8B)
Variable Header:
  [stream_id    : u64]    -- stream that was appended to
  [extent_id    : u32]    -- extent that was appended to
  [offset       : u64]    -- assigned logical sequence number
  [byte_pos     : u64]    -- byte position within the extent arena (for building external index)
No Payload.
```

##### 0x03 READ (Client -> Extent Node)

Read messages from a stream starting at a given offset and byte position.

```
Fixed Header (8B)
Variable Header:
  [stream_id    : u64]    -- target stream
  [offset       : u64]    -- start logical offset
  [byte_pos     : u64]    -- byte position within the extent arena (0 = scan from start)
  [count        : u32]    -- number of messages to read
No Payload.
```

##### 0x04 READ_RESP (Extent Node -> Client)

Read response carrying message data.

```
Fixed Header (8B)
Variable Header:
  [stream_id    : u64]    -- stream that was read from
  [offset       : u64]    -- starting offset of the returned batch
  [count        : u32]    -- actual number of messages returned
Payload:
  [payload_len  : u32]    -- total length of all encoded messages
  [payload      : bytes]  -- repeated [msg_len:u32][msg_bytes] per message
```

##### 0x05 SEAL (Client -> Stream Manager; Extent Node -> Stream Manager; Stream Manager -> Extent Node)

Seal an extent. A single opcode covers all trigger sources; `Flags` bit 0 (`FLAG_OFFSET_PRESENT`) distinguishes whether the caller provides the resolved end offset:

- **Client seal** (`FLAG_OFFSET_PRESENT = 0`): Client doesn't know the committed offset. Stream Manager concurrently seals all Extent Node replicas and determines the committed offset via quorum (Primary's offset if available, otherwise k-th largest Secondary offset where `k = RF/2`).
- **Extent-node seal** (`FLAG_OFFSET_PRESENT = 1`): Primary ExtentNode proactively seals (e.g. ExtentFull) and reports its committed end_offset. Stream Manager trusts the offset, skips sealing the Primary (already sealed locally), and fire-and-forgets Seal RPCs to secondary ExtentNodes only.
- **Stream Manager -> Extent Node**: Stream Manager sends Seal to individual Extent Nodes during client-seal quorum collection. Variable header carries `stream_id` only; Extent Node seals locally and responds with SEAL_ACK.

Both client-initiated and EN-initiated paths end with Stream Manager allocating a new extent and responding with SEAL_ACK.

```
FLAG_OFFSET_PRESENT = 0 (client seal, or SM -> EN):
  Fixed Header (8B)
    Flags: 0x00
  Variable Header:
    [stream_id    : u64]    -- stream to seal
    [extent_id    : u32]    -- extent to seal
  No Payload.

FLAG_OFFSET_PRESENT = 1 (extent-node seal):
  Fixed Header (8B)
    Flags: 0x01
  Variable Header:
    [stream_id    : u64]    -- stream to seal
    [extent_id    : u32]    -- extent to seal
    [offset       : u64]    -- committed end_offset, trusted by SM
  No Payload.
```

##### 0x06 SEAL_ACK (Extent Node -> Stream Manager; Stream Manager -> Client)

**Extent Node -> Stream Manager**: Seal confirmation from an individual EN. Carries the committed offset so Stream Manager can compute quorum.

```
Fixed Header (8B)
  Flags: 0x00
Variable Header:
  [request_id   : u32]    -- request id
  [stream_id    : u64]    -- stream that was sealed
  [extent_id    : u32]    -- extent that was sealed
  [offset       : u64]    -- committed end_offset
No Payload.
```

**Stream Manager -> Client**: Returns new extent info after seal-and-new allocation. Uses `FLAG_NEW_EXTENT_PRESENT` (0x01) to carry the new extent info in the variable header.

```
Fixed Header (8B)
  Flags: 0x01 (FLAG_NEW_EXTENT_PRESENT)
Variable Header:
  [request_id   : u32]    -- request id
  [stream_id    : u64]    -- stream that was sealed
  [extent_id    : u32]    -- extent that was sealed
  [offset       : u64]    -- committed end_offset
  [new_extent_id: u32]    -- newly allocated extent (in count field)
  [primary_addr_len : u16]
  [primary_addr : bytes]  -- address of the new extent's Primary node
No Payload.
```

##### 0x07 CREATE_STREAM (Client -> Stream Manager)

Create a new stream. If `replication_factor = 0`, Stream Manager uses its default.

```
Fixed Header (8B)
Variable Header:
  [name_len     : u16]
  [stream_name  : bytes]  -- stream name (maps to MQTT queue name)
  [replication_factor : u16]
No Payload.
```

**Response** (via APPEND_ACK opcode): `stream_id` in variable header, plus initial extent info.

##### 0x08 QUERY_OFFSET (Client -> Extent Node / Stream Manager)

Query the max offset (exclusive) for a stream.

```
Fixed Header (8B)
Variable Header:
  [stream_id    : u64]    -- target stream
No Payload.
```

##### 0x09 QUERY_OFFSET_RESP (Extent Node / Stream Manager -> Client)

Returns the current max offset.

```
Fixed Header (8B)
Variable Header:
  [stream_id    : u64]    -- queried stream
  [offset       : u64]    -- max offset (exclusive)
No Payload.
```

**Lifecycle (0x10-0x1F) -- Extent Node <-> Stream Manager**

##### 0x10 CONNECT (Extent Node -> Stream Manager)

First frame after an Extent Node connects to Stream Manager. Stream Manager uses 1.5x `interval_ms` as the dead-node timeout.

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

##### 0x11 CONNECT_ACK (Stream Manager -> Extent Node)

Acknowledges Extent Node registration.

```
Fixed Header (8B)
Variable Header:
  [request_id    : u32]
No Payload.
```

##### 0x12 DISCONNECT (Extent Node -> Stream Manager)

Graceful shutdown. Stream Manager stops allocating new extents to this node.

```
Fixed Header (8B)
Variable Header:
  [request_id    : u32]
Payload:
  [node_id_len  : u16]
  [node_id      : bytes]  -- node identifier
```

##### 0x13 DISCONNECT_ACK (Stream Manager -> Extent Node)

Acknowledges disconnect.

```
Fixed Header (8B)
Variable Header:
  [request_id    : u32]
No Payload.
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

##### 0x15 REGISTER_EXTENT (Stream Manager -> Extent Node)

Register an extent's replica membership on an Extent Node. Primary receives all secondary addresses for broadcast forwarding; Secondaries receive an empty address list.

```
Fixed Header (8B)
Variable Header:
  [request_id          : u32]
  [stream_id           : u64]    -- stream this extent belongs to
  [extent_id           : u32]    -- extent being registered
  [role                : u8]     -- 0 = Primary, 1+ = Secondary
  [replication_factor  : u16]
Payload:
  [num_addrs    : u16]    -- number of secondary addresses (0 for Secondaries)
  per address:
    [addr_len   : u16]
    [addr       : bytes]
```

##### 0x16 REGISTER_EXTENT_ACK (Extent Node -> Stream Manager)

Acknowledges extent registration.

```
Fixed Header (8B)
Variable Header:
  [request_id  : u32]
  [stream_id   : u64]    -- stream that was registered
  [extent_id   : u32]    -- extent that was registered
No Payload.
```

##### 0x17 WATERMARK (Secondary -> Primary)

Cumulative ACK from Secondary to Primary. Primary uses watermark ACKs from all secondaries to compute quorum offset and ACK clients.

```
Fixed Header (8B)
Variable Header:
  [stream_id    : u64]    -- stream the watermark applies to
  [offset       : u64]    -- highest committed offset (inclusive, cumulative)
No Payload.
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

##### 0x30 DESCRIBE_STREAM (Client -> Stream Manager)

Describe a stream's extents with replica info and node liveness.

```
Fixed Header (8B)
Variable Header:
  [stream_id    : u64]    -- target stream
  [count        : u32]    -- 0 = all extents, 1 = active only, N = at most N from latest
No Payload.
```

##### 0x31 DESCRIBE_STREAM_RESP (Stream Manager -> Client)

Response to DESCRIBE_STREAM. Payload = encoded `Vec<ExtentInfo>`, ordered by extent_id **descending** (latest first). When `count > 0`, at most `count` extents are returned starting from the latest.

```
Fixed Header (8B)
Variable Header:
  [stream_id    : u64]    -- queried stream
Payload:
  [payload_len  : u32]
  [payload      : bytes]  -- encoded Vec<ExtentInfo> (see ExtentInfo format below)
```

##### 0x32 DESCRIBE_EXTENT (Client -> Stream Manager)

Describe a single extent.

```
Fixed Header (8B)
Variable Header:
  [stream_id    : u64]    -- target stream
  [extent_id    : u32]    -- target extent
No Payload.
```

##### 0x33 DESCRIBE_EXTENT_RESP (Stream Manager -> Client)

Response to DESCRIBE_EXTENT. Payload = encoded `Vec<ExtentInfo>` with exactly 1 entry.

```
Fixed Header (8B)
Variable Header:
  [stream_id    : u64]    -- queried stream
Payload:
  [payload_len  : u32]
  [payload      : bytes]  -- encoded Vec<ExtentInfo> (1 entry)
```

##### 0x34 SEEK (Client -> Stream Manager)

Resolve a logical offset to the extent that contains it.

```
Fixed Header (8B)
Variable Header:
  [stream_id    : u64]    -- target stream
  [offset       : u64]    -- target logical offset
No Payload.
```

##### 0x35 SEEK_RESP (Stream Manager -> Client)

Response to SEEK. Payload = encoded `Vec<ExtentInfo>` with exactly 1 entry.

```
Fixed Header (8B)
Variable Header:
  [stream_id    : u64]    -- queried stream
  [offset       : u64]    -- resolved offset
Payload:
  [payload_len  : u32]
  [payload      : bytes]  -- encoded Vec<ExtentInfo> (1 entry)
```

For sealed/flushed extents: `start_offset <= offset < end_offset`. For the active extent: `offset >= start_offset` (end_offset equals start_offset in metadata until sealed).

**ExtentInfo** payload encoding (shared by 0x31, 0x33, 0x35):

```
[num_extents:u32]
  per extent:
    [extent_id:u32][start_offset:u64][end_offset:u64][state:u8]
    [num_replicas:u16]
      per replica:
        [addr_len:u16][addr_bytes][role:u8][is_alive:u8]
```

Fields:
- `state`: ExtentState (0=Unspecified, 1=Active, 2=Sealed, 3=Flushed)
- `role`: 0=Primary, 1+=Secondary
- `is_alive`: 1 if the ExtentNode's heartbeat is current (node.state=Alive), 0 otherwise

**Control (0xFE-0xFF)**

##### 0xFF ERROR (Any -> Any)

Error response. Variable header carries the error code and the relevant extent ID (for ExtentFull/ExtentSealed errors so the client can identify which extent triggered the error without a round-trip).

```
Fixed Header (8B)
Variable Header:
  [error_code   : u16]    -- 0=Ok, 1=UnknownStream, 2=InvalidOffset,
                              3=ExtentSealed, 4=InternalError, 5=ExtentFull
  [extent_id    : u32]    -- relevant extent (0 when not applicable)
Payload:
  [payload_len  : u32]
  [payload      : bytes]  -- human-readable error message (UTF-8)
```

#### Connection Model

- Java proxy maintains a **connection pool** to the storage service (one pool per Extent Node).
- Connections are multiplexed: multiple in-flight requests per connection, correlated by Request ID.
- Tokio on the Rust side handles async I/O with `tokio::net::TcpListener`.
- Java side uses Netty for async TCP client (already a dependency via RocketMQ client).

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
    │   └── src/lib.rs                 -- StorageClient: connect/disconnect to Extent Node and Stream Manager,
    │                                     append, read, seal, create_stream
    │
    ├── extent-node/                   -- Extent Node library (depends: common, rpc, server, client)
    │   └── src/
    │       ├── lib.rs                 -- run(): Extent Node bootstrap, heartbeat to Stream Manager
    │       ├── extent.rs              -- Extent: in-memory buffer + state machine (Active/Sealed/Flushed)
    │       ├── stream.rs              -- Stream: ordered extent list, active extent tracking, seal-and-new
    │       ├── store.rs               -- ExtentNodeStore: request handler, ReplicaInfo, PendingAck, AckQueue
    │       ├── downstream.rs          -- DownstreamManager: per-node-addr TCP for broadcast forwarding
    │       └── watermark.rs           -- WatermarkHandler: cumulative ACK + quorum drain, deferred client ACK
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
| **client** | lib | Client for talking to Extent Node and Stream Manager: append/read messages, seal/create streams. Used by Extent Node (keepalive heartbeat to Stream Manager) and Stream Manager (seal commands to Extent Nodes). Also the protocol the Java proxy re-implements via Netty. |
| **extent-node** | lib | Extent Node logic. Holds in-memory extent replicas, participates in broadcast replication (Primary broadcasts to secondaries, receives watermark ACKs, computes quorum), serves APPEND/READ/SEAL requests. Uses client to heartbeat to Stream Manager. Built into a binary via `src/bin/extent-node.rs`. |
| **stream-manager** | lib | Stream Manager logic. Manages stream->extent mappings, orchestrates seal-and-new, allocates extents across Extent Nodes, persists metadata to MySQL. Uses client to issue seal/allocate to Extent Nodes. Built into a binary via `src/bin/stream-manager.rs`. |

The `client` crate is used internally by both process types: Extent Node uses it to send keepalive heartbeats to Stream Manager, and Stream Manager uses it to issue seal/allocate commands to Extent Nodes. It is also the protocol interface for external consumers -- the Java MQTT proxy's `StreamStoreClient` re-implements the same wire format via Netty.

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
2. Primary broadcasts the append to **all Secondaries in parallel** (O(1) hop, FLAG_FORWARDED set in Flags).
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

### Extent-Node Concurrency: Lock-Free Arena

The active extent on each Extent Node uses a **lock-free pre-allocated memory arena** to maximize append throughput under high concurrency. Multiple client connections (and the replication path) can append to the same extent concurrently without any mutex.

#### Arena Layout

Each active extent pre-allocates a contiguous buffer (configurable, default 64 MiB via `ExtentNodeConfig.extent_arena_capacity`). Records are stored sequentially in the arena in wire format: `[payload_len: u32 BE][payload: bytes]`. This is the same format as the S3 object body, enabling zero-copy upload of sealed extents.

The arena has no internal index structure. Records are self-contained: a reader can walk forward from any byte position by reading the length prefix and advancing by `4 + len` bytes. Random access is provided by an **external index** maintained by the application layer (see "Index-Based Read Path" below).

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
  sealed          : AtomicBool
```

#### Append Path (Lock-Free, Multiple Concurrent Writers)

```
Thread A ──► fetch_add(write_cursor, recA_len) ──► got byte_pos=0
Thread B ──► fetch_add(write_cursor, recB_len) ──► got byte_pos=recA_len
Thread C ──► fetch_add(write_cursor, recC_len) ──► got byte_pos=recA_len+recB_len

  Each thread now owns an exclusive, non-overlapping region.
  They copy their payload into their region in parallel.

Thread A ──► memcpy into [0..recA_len]
Thread B ──► memcpy into [recA_len..recA_len+recB_len]      (parallel)
Thread C ──► memcpy into [recA_len+recB_len..]              (parallel)
```

Detailed steps:

1. **Check sealed** (atomic load, Acquire). If sealed, return `ExtentSealed`.
2. **Reserve byte slot**: `write_cursor.fetch_add(record_len)` -- atomically claims a non-overlapping region in the arena. If the cursor exceeds capacity, return `ExtentFull` (triggers seal-and-new).
3. **Reserve logical sequence**: `record_count.fetch_add(1)` -- atomically assigns a monotonic sequence number.
4. **Copy payload**: Write `[len][payload]` into the reserved region. No lock needed -- each writer owns its region exclusively.
5. **Advance committed_seq and committed_bytes** (spin-wait CAS, Approach A): The writer spins on `committed_seq.compare_exchange_weak(seq, seq+1)` until it succeeds, then stores the new `committed_bytes`. This ensures both cursors advance **in-order** -- a reader seeing `committed_bytes=N` is guaranteed that all bytes in `0..N` are fully written.
6. **Return `AppendResult { offset, byte_pos }`**: The caller receives both the logical offset and the byte position within the arena. The byte position enables the caller to build an external offset-to-position index for O(1) random reads (see below).

#### Commit Cursor: Spin-Wait CAS (Approach A)

The commit advancement step is the only point where writers interact with each other. The spin waits for the **immediately preceding writer** to finish its memcpy. For typical MQTT messages (<1 KB), memcpy completes in tens of nanoseconds, so the spin is negligible.

This is the same technique used by:
- Linux kernel's io_uring submission queue
- LMAX Disruptor's multi-producer sequencer
- Intel DPDK ring buffer

```
committed_seq: 0

Thread A (seq=0): memcpy done → CAS(0→1) succeeds immediately
Thread C (seq=2): memcpy done → CAS(0→3)? NO, spin... CAS(1→3)? NO, spin...
Thread B (seq=1): memcpy done → CAS(1→2) succeeds
Thread C (seq=2): CAS(2→3) succeeds  ← waited only for Thread B's memcpy
```

#### Index-Based Read Path

The data stream arena has **no internal index**. Instead, the application layer maintains a separate **index stream** with fixed-width records that map logical offsets to byte positions within data extent arenas.

After appending to a data stream, the application receives `AppendResult { offset, byte_pos }` and writes a fixed-width index record (32 bytes) to the index stream:

```
Index record (32 bytes, fixed width):
  [stream_id: u64][extent_id: u32][offset: u64][byte_pos: u64]
```

**Read flow**:

1. Read the index stream at the desired logical offset (fixed-width records, so the byte position is `offset * 32`).
2. Parse `(stream_id, extent_id, byte_pos)` from the index record.
3. Send `READ(stream_id, offset, byte_pos, count)` to the Extent Node holding the data extent.
4. The Extent Node seeks directly to `byte_pos` in the arena and reads `count` records forward.
5. Return zero-copy `Bytes` slices referencing the arena buffer.

This two-stream design (data + index) means:
- **Data stream reads** are O(1) random access -- no sequential walk from byte 0.
- **Index stream reads** are also O(1) -- fixed-width records enable direct offset calculation.
- **Readers never block writers**. The only synchronization is atomic loads on `committed_bytes`.

#### Configurable Arena Capacity

The arena capacity is configurable per ExtentNode via `ExtentNodeConfig.extent_arena_capacity` (default 64 MiB). Application users can tune this per workload:

- **Smaller arenas**: More frequent seal-and-new, lower write amplification for small streams, faster S3 flush cycles.
- **Larger arenas**: Fewer seals, better for high-throughput streams that batch many messages per extent.

The capacity is applied to all extents created on the ExtentNode, including new extents created during seal-and-new.

#### Seal (Atomic Flag)

Sealing sets `sealed.store(true, Release)`. Subsequent appends see the flag and return `ExtentSealed`. The `committed_seq` at seal time is the definitive record count reported to Stream Manager.

#### Properties

| Property | Guarantee |
|----------|----------|
| Offset uniqueness | `record_count.fetch_add` is atomic -- no two writers get the same sequence |
| No overlap | `write_cursor.fetch_add` gives each writer a disjoint byte region |
| Read consistency | `committed_bytes` advances in-order; readers see a gap-free prefix |
| Zero-copy reads | `Bytes::slice` into the arena buffer; no allocation or copy |
| Zero-copy S3 flush | Arena bytes are in wire format; sealed extent uploads the buffer directly |
| No mutex on hot path | Append and read use only atomic operations and brief spin-wait |
| O(1) random read | External index provides byte_pos; no sequential walk needed |

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

- `putMessage` future completes after data stream write ACK.
- Index writes are async. A background **Reconciler** ensures all expected index entries exist.
- Eventual consistency is acceptable per MQTT QoS semantics (QoS 0: at-most-once; QoS 1: client retransmit covers gaps; QoS 2: protocol-level dedup).

### Read Path

```
pullMessage(queue, group, offset, count)
  -> Read Index Stream entries [offset..offset+count]
  -> Batch-resolve data stream references
  -> Read message bodies from Data Stream (memory or S3)
  -> Return PullResult
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

```
s3://{bucket}/{namespace}/data/{stream_id}/{extent_id}.dat
s3://{bucket}/{namespace}/index/{queue_name}/{extent_id}.idx
```

Each extent object is self-contained:

```
+-----------------------------------+
| Extent Header (magic, version,    |
|   stream_id, start_offset,        |
|   end_offset)                     |
| Message 0: [len][headers][body]   |
| Message 1: [len][headers][body]   |
| ...                               |
| Message N: [len][headers][body]   |
| Footer: offset_index[]            |
|   seq_0 -> byte_offset_0          |
|   seq_1 -> byte_offset_1          |
|   ...                             |
| CRC32                             |
+-----------------------------------+
```

Footer index enables efficient random reads within an extent without downloading the whole object (S3 range reads).

### Post-Flush

1. Stream Manager marks extent as "flushed" with S3 key in metadata.
2. In-memory replicas eligible for eviction (LRU policy, configurable retention).
3. Sealed extents can optionally be erasure-coded (e.g., Reed-Solomon 4+2) to reduce S3 storage from 3x to ~1.5x.

## Stream Manager Metadata

Stored in MySQL. Rust side uses sqlx (async MySQL client) and Refinery (schema migrations). Java side uses JDBC/HikariCP for consumer offset management.

### Tables

```sql
CREATE TABLE stream (
    stream_id    BIGINT PRIMARY KEY AUTO_INCREMENT,
    stream_name  VARCHAR(512) NOT NULL UNIQUE,  -- maps to MQTT queue name
    stream_type  VARCHAR(32) NOT NULL DEFAULT 'DATA',  -- 'DATA' or 'INDEX'
    replication_factor SMALLINT NOT NULL DEFAULT 2, -- per-stream RF (1-N)
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE extent (
    stream_id     BIGINT NOT NULL,
    extent_id     INT NOT NULL,                    -- per-stream monotonic via stream_sequence table (u32: 4.3B IDs)
    start_offset  BIGINT NOT NULL DEFAULT 0,    -- first logical offset in this extent
    end_offset    BIGINT NOT NULL DEFAULT 0,   -- exclusive upper bound (updated on seal; equals start_offset while active)
    state         TINYINT NOT NULL DEFAULT 1,   -- ExtentState: 0=Unspecified, 1=Active, 2=Sealed, 3=Flushed
    s3_key        VARCHAR(1024),                -- set after flush
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    sealed_at     TIMESTAMP NULL,
    flushed_at    TIMESTAMP NULL,
    PRIMARY KEY (stream_id, extent_id)
);

CREATE TABLE extent_replica (
    stream_id  BIGINT NOT NULL,
    extent_id  INT NOT NULL,
    node_addr  VARCHAR(256) NOT NULL,
    role       TINYINT NOT NULL,                -- 0=Primary, 1+=Secondary
    PRIMARY KEY (stream_id, extent_id, node_addr)
);

CREATE TABLE stream_sequence (
    stream_id      BIGINT PRIMARY KEY,
    next_extent_id INT NOT NULL DEFAULT 0
);

CREATE TABLE node (
    node_id               VARCHAR(256) PRIMARY KEY,
    addr                  VARCHAR(256) NOT NULL,
    heartbeat_interval_ms INT NOT NULL DEFAULT 5000,
    last_heartbeat        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    state                 TINYINT NOT NULL DEFAULT 1  -- NodeState: 0=Unspecified, 1=Alive, 2=Dead
);

CREATE TABLE stream_offset (
    consumer_group VARCHAR(512) NOT NULL,
    stream_id      BIGINT NOT NULL,
    committed_offset BIGINT NOT NULL DEFAULT 0,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (consumer_group, stream_id)
);
```

### Offset Translation

```
pullMessage(queue, group, offset=1050, count=10)
  -> Stream Manager lookup: stream for queue
  -> Find extent where start_offset <= 1050 < end_offset
  -> local_offset = 1050 - start_offset
  -> Read from extent (memory or S3)
```

## Integration with rocketmq-mqtt

### Interface Boundary

The `LmqQueueStore` interface (12 methods) is the clean integration boundary. `mqtt-cs` (Connection Server) depends only on this interface and is entirely unaffected.

### New Java Module: mqtt-store (Thin Client)

The Java-side `mqtt-store` module is a **thin client** -- it implements `LmqQueueStore` by translating method calls into custom TCP protocol requests to the Rust storage service. No storage logic lives in Java.

```
mqtt-store/
  src/main/java/org/apache/rocketmq/mqtt/store/
    StreamStoreClient.java         -- implements LmqQueueStore (TCP client)
    StreamOffsetStore.java         -- implements LmqOffsetStore (TCP client)
    StreamQueueCache.java          -- implements QueueCache (local cache + TCP)
    client/
      StorageConnection.java       -- Single TCP connection with multiplexing
      StorageConnectionPool.java   -- Connection pool to storage service nodes
      FrameEncoder.java            -- Encode request frames (Netty ChannelHandler)
      FrameDecoder.java            -- Decode response frames (Netty ChannelHandler)
      RequestFuture.java           -- CompletableFuture correlated by Request ID
    config/
      StorageClientConfig.java     -- Storage service endpoints, pool size, timeouts
```

### Method Mapping

| LmqQueueStore Method | StreamStoreClient (Java) | Storage Service (Rust) |
|---|---|---|
| `putMessage(queues, msg)` | Encode APPEND frame with message body + index target stream IDs. Send to Primary Extent Node. | Primary receives APPEND, assigns seq, broadcasts to all secondaries in parallel. After quorum ACK confirms majority committed, ACKs client. Async-dispatch index entries. Return APPEND_ACK with offset. |
| `pullMessage(queue, group, offset, count)` | Encode READ frame with stream_id, offset, count. | Read index entries. Batch-resolve data refs. Return READ_RESP with message bodies (from memory or S3). |
| `popMessage(group, queue, count)` | Same as pull, but Java side manages pop-state in MySQL (receipt_handle -> offset + invisible_until). | Storage service is unaware of pop semantics. It just serves reads. |
| `popAck(topic, group, handle)` | Java-side only: delete pop reservation in MySQL, advance committed offset. | Not involved. |
| `changeInvisibleTime(...)` | Java-side only: update invisible_until in MySQL. | Not involved. |
| `queryMaxOffset(queue)` | Encode QUERY_OFFSET frame. | Return current max offset from Stream Manager metadata. |
| `getLag(group, queue)` | maxOffset (from storage) - committedOffset (from MySQL). | Serves max offset query. |
| `getReadableBrokers()` | Returns storage service node addresses from config/service discovery. | N/A. |

### Configuration

```properties
# storage.conf (Java side)
storage.backend=stream          # 'rocketmq' for legacy, 'stream' for new
storage.service.endpoints=10.0.0.1:9801,10.0.0.2:9801,10.0.0.3:9801
storage.client.poolSize=8       # connections per endpoint
storage.client.timeout=3000     # request timeout ms
```

```toml
# stream-store.toml (Rust side)
[server]
listen_addr = "0.0.0.0:9801"

[s3]
endpoint = "https://s3.amazonaws.com"
bucket = "mqtt-data"
region = "us-east-1"

[extent]
max_size = 67_108_864           # 64 MB (matches extent_arena_capacity)
max_age_secs = 30               # 30 seconds
arena_capacity = 67_108_864     # 64 MiB default; tune per workload

[replication]
factor = 2                      # replica count (default 2, supports 1-N)

[cache]
read_cache_size = 1_073_741_824 # 1 GB local read cache
max_memory = 34_359_738_368     # 32 GB total memory budget for extents

[metadata]
mysql_url = "mysql://user:pass@db-host:3306/mqtt_storage"
```

### Migration Strategy

1. **Phase 1**: Build `mqtt-store` module. Integration tests against embedded S3 (e.g., MinIO testcontainer).
2. **Phase 2**: `storage.backend` config switch. Spring wiring selects `LmqQueueStoreManager` or `StreamStoreManager`.
3. **Phase 3**: Shadow/dual-write mode. Write to both backends, read from new. Compare results.
4. **Phase 4**: Cutover. Read and write from new backend only.

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
- Extent codec: binary format with header, messages, footer index
- S3 Flusher: sealed extent upload via aws-sdk-s3
- S3 Reader: range-read with local LRU cache (moka)
- Flush triggers: size, time, node failure
- Post-flush memory eviction
- Integration tests with MinIO container

### Phase 4: Multi-Dispatch (Data + Index Streams)
- Index stream: lightweight pointer entries
- APPEND opcode support for multi-dispatch (data body + index targets)
- Reconciler: background index consistency checker
- Batch read path: index entries -> data stream lookups

### Phase 5: Java Client Module (mqtt-store)
- Netty-based TCP client: FrameEncoder/FrameDecoder/ConnectionPool
- StreamStoreClient implementing LmqQueueStore
- StreamOffsetStore implementing LmqOffsetStore
- StreamQueueCache implementing QueueCache
- Pop-mode state management in MySQL (Java-side only)
- Spring configuration for backend selection

### Phase 6: Integration and Migration
- End-to-end integration tests (MQTT publish -> storage -> MQTT subscribe)
- Dual-write mode for production migration
- Performance benchmarks (throughput, latency, memory usage)
- Production cutover plan

## Key Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Process model | Dedicated Rust process | No GC pauses, zero-copy I/O, precise memory control, enforced architectural boundary |
| Implementation language | Rust (Tokio async runtime) | Deterministic memory, zero-cost abstractions, mature async ecosystem (aws-sdk-s3, sqlx, moka) |
| RPC protocol | Custom TCP with fixed header + variable header + payload (MQTT-style) | Minimal overhead, zero-copy broadcast forwarding, per-opcode wire layout, full control over batching and framing |
| Object storage API | S3-compatible | Widest ecosystem (AWS, MinIO, Ceph, Alibaba OSS S3-compat) |
| Replication protocol | Broadcast replication with quorum ACK | O(1) hop latency (vs O(N) for chain), tolerates minority failures, simple parallel fan-out |
| Durability before S3 | Pure in-memory N-way (default 2-way) | Low latency; single-node failure tolerated; S3 flush bounds risk |
| Extent concurrency | Lock-free arena with atomic cursors (spin-wait commit), external index for O(1) reads | No mutex on append/read hot path; parallel memcpy for concurrent writers; byte-position-based random access |
| Multi-dispatch | Shared data + index streams | Storage efficient; avoids body duplication across subscribers |
| Stream Manager metadata store | MySQL (sqlx on Rust, JDBC on Java) | Reuses existing infra; metadata ops are infrequent (per-extent, not per-message) |
| Consistency model | Seal-and-new (WAS) | Separates consistency (sealed extent) from availability (new extent) |
| Integration boundary | LmqQueueStore interface | mqtt-cs unchanged; Java mqtt-store is a thin TCP client to Rust service |
