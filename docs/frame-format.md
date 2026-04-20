# Frame Format & Opcodes

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
  [storage_class       : u8]    -- 0 = S3 (default), 1 = Memory
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

**flag=0x01 ForwardInitExtent** — Sent once by the Primary before the first Forward frame for a new extent. Carries extent metadata so the secondary can create the extent with the correct capacity and adaptive sizing config. No payload, no response.

```
Fixed Header (8B)    -- flags=0x01
Variable Header (36B):
  [stream_id            : u32]    -- target stream
  [extent_id            : u32]    -- new extent
  [epoch                : u32]    -- stream epoch
  [start_offset         : u64]    -- extent base offset
  [extent_capacity      : u32]    -- arena size in bytes (primary's actual capacity)
  [cache_extents        : u16]    -- max extents to retain in memory
  [min_extent_capacity  : u32]    -- floor for adaptive shrink (0 = default 8 MiB)
  [max_extent_capacity  : u32]    -- ceiling for adaptive growth (0 = default 256 MiB)
  [extent_growth_factor : u8]     -- adaptive growth multiplier (0 = default 2)
  [storage_class        : u8]     -- 0 = S3, 1 = Memory
No Payload.
```

**flag=0x02 ForwardChecksum**

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

Two-phase seal protocol. **Phase 1 (Prepare)**: SM queries each EN to seal its last mutable extent and report the committed offset. SM collects responses and computes the quorum-committed offset. **Phase 2 (Commit)**: SM broadcasts the authoritative committed offset to all replicas so they correct their local seal point. Phase 2 is fire-and-forget.

**Prepare (flag=0x00): SM -> EN**

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

**Commit (flag=0x02): SM -> EN**

Fire-and-forget. SM broadcasts the authoritative quorum-committed offset after computing it from Prepare responses. ENs correct their local seal point (which may differ if they sealed at their own local offset during Prepare).

```
Fixed Header (8B)    -- flags=0x02
Variable Header (28B):
  [stream_id     : u32]    -- stream whose extent was sealed
  [extent_id     : u32]    -- sealed extent
  [epoch         : u32]    -- stream epoch
  [start_offset  : u64]    -- extent start offset
  [end_offset    : u64]    -- authoritative committed end offset
No Payload.
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
  [cache_extents           : u16]    -- max extents to retain in memory per stream
  [min_extent_capacity     : u32]    -- floor for adaptive shrink (0 = default 8 MiB)
  [max_extent_capacity     : u32]    -- ceiling for adaptive growth (0 = default 256 MiB)
  [extent_growth_factor    : u8]     -- adaptive growth multiplier (0 = default 2)
  [storage_class           : u8]     -- 0 = S3, 1 = Memory
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

**flag=0x02 UpdateExtentFlushed** — Sent by the EN that uploaded a sealed extent to S3. Normally the Primary; in disaster recovery, a secondary delegated by SM. SM transitions extent state from Sealed to Flushed.

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

##### 0x1B FLUSH_EXTENT (Stream Manager -> Extent Node)

SM commands an EN to upload a sealed extent to S3 (disaster recovery). Fire-and-forget: no response. The EN queues the upload on its background S3 flusher and sends `UpdateExtentFlushed` (0x18, flag=0x02) back to SM on success. EN deduplicates concurrent FlushExtent requests via a per-stream in-progress tracking set (`Stream::flush_in_progress`), checked via `start_flush`/`finish_flush`. SM may send this to ALL replicas concurrently — S3 PUT is idempotent.

**Request (flag=0x00): SM -> EN**

```
Fixed Header (8B)
Variable Header (28B):
  [stream_id     : u32]    -- stream whose extent should be flushed
  [extent_id     : u32]    -- sealed extent to upload
  [epoch         : u32]    -- stream epoch
  [start_offset  : u64]    -- extent start offset (for S3 key)
  [end_offset    : u64]    -- extent end offset (for S3 key)
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