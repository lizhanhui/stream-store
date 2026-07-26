# Codebase Consistency Findings

This document records inconsistencies found while comparing the implementation with [`design.md`](design.md). Findings are ordered by impact. Line references identify the reviewed code and may move as the implementation changes.

## P0: Data Integrity and Consistency

### 1. Secondary nodes accept client appends — Resolved

The design requires the Primary to be the sole append acceptor. Previously, the append path wrote the record before checking the replica role, and the Secondary branch then returned a successful `AppendAck`. A stale or misrouted client could therefore receive success for a record written to only one Secondary, without forwarding or quorum replication.

**Resolution:** APPEND now requires an explicit Primary `ReplicaInfo` before leader election and repeats the check at the mutation boundary. Explicit and lazily created Secondaries return `NotPrimary` (error code 6) without changing the stream. The client maps this response to `StorageError::NotPrimary` so callers can rediscover through `DescribeStream`. Regression tests cover both Secondary states and verify that their maximum offset is unchanged.

### 2. Replication transport can drop records and acknowledge across the gap — Resolved

The downstream channel and TCP session can lose Forward frames. Previously, the Secondary trusted FIFO ordering as lossless delivery, advanced `committed_offset` directly to any received offset, and returned that value as a cumulative Watermark. A later record could therefore falsely acknowledge a missing predecessor.

**Resolution:** `Extent::replicate` now serializes Secondary writers and requires both the received offset and byte position to equal the extent's next contiguous frontiers before writing any data. Gaps return `StorageError::ReplicationGap`; layout divergence returns `StorageError::ReplicationPositionMismatch`. Either error leaves the arena, index, CRC, counters, and committed frontiers unchanged, so `finish_forward` withholds the Watermark. Tests cover logical gaps, byte-position gaps, concurrent duplicate Forwards, unchanged state and CRC, post-seal late contiguous replication, and the absence of a Watermark after a skipped Forward. Reliable replay remains a separate availability enhancement; the current fix restores quorum safety by stalling the affected Secondary.

### 3. `RegisterExtent` cannot establish an authoritative start offset — Resolved

The `RegisterExtent` protocol does not include `start_offset` (`design.md:710-735`). When an Extent Node creates an extent from this request, it guesses the start offset using its local `stream.max_offset()` (`components/extent-node/src/store/register.rs:101-109`). A newly assigned node can have no preceding extent and choose zero; a lagging node can choose a stale value.

`ForwardInitExtent` carries the correct start offset, but only creates the extent if it does not already exist (`components/extent-node/src/store/forward.rs:163-177`). When the SM's `RegisterExtent` and the Primary's `ForwardInitExtent` disagree, the stale/guessed value can be installed.

This can restart or overlap logical offsets after an epoch change.

**Resolution:** `RegisterExtent` (sent only to the **Primary**) now carries the SM-assigned authoritative `start_offset` (u64) in its variable header, encoded after `epoch`. The Stream Manager populates it from the values it persists: the allocation `start_offset` for initial extents and the sealed extent's `end_offset` for seal-and-new successors (`components/stream-manager/src/store.rs`). The Primary installs the extent at the wire value and propagates the same authoritative `start_offset` once to secondaries via in-band `ForwardInitExtent` (the sole secondary creation path; the SM no longer sends `RegisterExtent` to secondaries). ForwardInitExtent installs an epoch-qualified Secondary role and atomically creates the extent if absent; a newer init demotes a former Primary, while stale/conflicting init is ignored. Forward and ForwardChecksum validate the Secondary role plus stream/extent epoch before mutation. Initialization remains best-effort with no replay: lost init or a replication gap quarantines that Secondary for the rest of the epoch and withholds Watermarks; same-epoch successor extents cannot make it rejoin, while RF=3 may operate temporarily degraded until a later epoch. Watermark quorum accounting is extent-qualified so successor progress cannot acknowledge missing predecessor records. Primary registration is idempotent and uses the wire value instead of `stream.max_offset()`. Regression tests cover fresh/stale local state, role transition, Secondary append rejection, batch rejection without mutation, and init-before-Forward ordering.

### 4. Primary seal reports the local append frontier rather than the quorum frontier

The design describes the Primary seal offset as the committed, quorum-confirmed end offset (`design.md:47-55`, `design.md:1183-1188`). Quorum progress is tracked in `AckQueue`, but sealing returns `start_offset + record_count` (`components/extent-node/src/extent.rs:663-691`).

If the Primary has locally appended records that have not reached quorum, those records can still be included in the sealed metadata range. Losing the Primary afterward can leave surviving replicas without data that metadata declares committed.

The design itself is internally inconsistent: `design.md:1324-1326` calls final `record_count` definitive, conflicting with the earlier quorum definition.

### 5. Epoch CAS does not fence the epoch observed by the client request

`handle_epoch_seal` reads and validates the requested epoch early (`components/stream-manager/src/store.rs:1488-1503`). Later, `bump_epoch` rereads the current database value and CASes that value instead of accepting the originally observed epoch (`components/stream-manager/src/metadata.rs:1133-1157`).

Two requests that both passed the initial check can consequently perform consecutive bumps. The second request can advance the stream to an epoch for which no corresponding active extent or replica set exists.

### 6. Epoch bump and successor allocation are separate transactions

The stream epoch is committed before `seal_allocate_register` is called (`components/stream-manager/src/store.rs:1711-1729`). The later transaction covers sealing, successor insertion, and replica rows, but not the earlier epoch update (`components/stream-manager/src/metadata.rs:484-647`).

An allocation or database failure after the bump can leave `stream.epoch` advanced while the active extent and replica set remain in the old epoch. This conflicts with the single transition described in `design.md:38-43`, `design.md:60`, and `design.md:85`.

### 7. Seal predecessor payload is incompatible with its parser

The documented payload contains `[extent_id][start_offset][end_offset][state]` (`design.md:482-501`). The Stream Manager parser expects the state byte (`components/stream-manager/src/store.rs:89-112`), but the Extent Node encoder emits only the first three fields (`components/extent-node/src/store/seal.rs:246-287`).

The parser returns `None`, and the caller silently skips reconciliation. Lost autonomous-seal notifications therefore cannot be recovered through the intended epoch-seal path.

## P1: Recovery, Availability, and Durability

### 8. Primary `RegisterExtent` failures are ignored

The design says the Stream Manager waits for the Primary registration ACK before exposing a new extent (`design.md:50-62`, `design.md:710-747`). Although `register_primary` waits for a response, its error is logged and suppressed during seal-and-new (`components/stream-manager/src/store.rs:1240-1262`). Initial stream creation similarly suppresses registration failure (`components/stream-manager/src/store.rs:590-594`).

Clients may receive successful metadata pointing to a Primary that has not registered the extent.

### 9. Automatic dead-node failover is not implemented

The design assigns dead-node detection and proactive failover to the Stream Manager leader (`design.md:184-186`, `design.md:1341-1346`, `design.md:1555-1563`). The heartbeat checker only marks the node dead and explicitly leaves recovery to a future client-triggered seal (`components/stream-manager/src/heartbeat_checker.rs:91-115`).

Existing streams can remain assigned to a dead Primary until a client encounters the failure and initiates recovery.

### 10. Secondary extents are not sealed during autonomous transitions

The design permits at most one active extent per stream (`design.md:22`). On receipt of `ForwardChecksum`, the Secondary stores and attempts to verify the checksum but never seals the old extent (`components/extent-node/src/store/forward.rs:291-340`). Checksum verification itself refuses to run until the extent is sealed (`components/extent-node/src/extent.rs:805-819`).

A Secondary can retain multiple extents in the Active state, report incorrect lifecycle metadata, and never complete normal checksum verification.

### 11. Forward and Watermark epochs are not validated

Forward and Watermark frames carry epochs for data integrity (`design.md:367-371`, `design.md:770-775`). `handle_forward` extracts but does not validate the epoch (`components/extent-node/src/store/forward.rs:189-228`). The Primary watermark reader uses the stream and offset while ignoring the frame's extent and epoch (`components/extent-node/src/downstream.rs:296-326`).

Delayed frames from an old Primary or epoch can mutate current state or advance current ACK accounting.

### 12. Flushed extents can be evicted but cannot be read from S3

The design promises S3-backed cold reads with a local read cache (`design.md:24`, `design.md:197-198`, `design.md:1194-1197`, `design.md:1603`). Eviction of flushed extents is active, but `handle_read` only searches in-memory streams and extents (`components/extent-node/src/store/read.rs:8-49`).

`S3Client::get_object` is not integrated into the read path, does not implement range reads, and there is no `moka` cache dependency. Data remains in S3 but becomes inaccessible through the Extent Node after eviction.

### 13. Flush scheduling can silently lose sealed extents

The flush queue is bounded (`components/extent-node/src/lib.rs:90-93`). Autonomous and SM-driven seal paths use `try_send` and discard its result (`components/extent-node/src/store/append.rs:620-655`, `components/extent-node/src/store/seal.rs:177-193`).

When the queue is full or disconnected, no retry or reconciliation process rediscovers the sealed extent. It can remain permanently unflushed and keep the stream in S3 backpressure.

### 14. Post-flush notifications are lossy

After upload, the Stream Manager notification uses an ignored `try_send`, and `ForwardFlushed` uses the lossy downstream path (`components/extent-node/src/s3_flusher.rs:111-134`, `components/extent-node/src/stream.rs:544-570`). Stream Manager socket-send failures are also dropped rather than retried (`components/extent-node/src/stream_manager_client.rs:496-566`).

MySQL can remain in Sealed state and Secondaries can remain ineligible for eviction after the Primary has successfully uploaded and locally marked the extent Flushed.

### 15. Memory-class streams can be uploaded during SM-driven seal

The storage-class contract says Memory streams are not uploaded (`design.md:26-28`, `design.md:1601`). Autonomous sealing checks the storage class, but the SM-driven seal handler queues every Primary extent for flushing without that check (`components/extent-node/src/store/seal.rs:177-194`).

### 16. Oversized appends can be abandoned after the second `ExtentFull`

The extent-full path seals, creates a larger extent, and retries as documented. The retry result's second `extent_full` flag is ignored (`components/extent-node/src/store/append.rs:205-238`, `components/extent-node/src/store/append.rs:516-530`, `components/extent-node/src/store/append.rs:963-1000`).

A payload larger than the maximum extent capacity can therefore receive neither a success response nor an error response.

### 17. CreateStream is not atomic

The stream and sequence rows are inserted using separate autocommitted statements (`components/stream-manager/src/metadata.rs:164-188`). Initial extent allocation occurs later in another transaction (`components/stream-manager/src/store.rs:849-855`).

If sequence initialization, placement, allocation, or registration fails, the unique stream name can remain reserved without a usable initial extent, preventing a normal retry.

### 18. Reconciliation can retain an incorrect zero start offset

Unknown sealed extents can be inserted with `start_offset = 0` (`components/stream-manager/src/metadata.rs:1291-1303`). Later duplicate-update reconciliation updates state and end offset but not start offset (`components/stream-manager/src/metadata.rs:1533-1550`).

The placeholder can become permanent and cause overlapping ranges or incorrect `Seek` resolution.

## P2: Protocol and API Robustness

### 19. Decoder trusts unbounded and malformed frame lengths

The decoder reserves capacity directly from the wire-provided 32-bit remaining length without enforcing a maximum (`components/rpc/src/frame/decode.rs:16-27`). Per-opcode parsing then uses unchecked `get_*` and `split_to` operations.

A remote peer can request very large allocations or provide a complete but undersized opcode body that panics the connection task. The protocol also lacks a documented maximum frame or payload size.

### 20. Heartbeat success response does not follow the response convention

The design specifies `FLAG_RESPONSE = 0x01` and a response containing only `request_id` (`design.md:246-252`, `design.md:701-707`). The implementation has no distinct Heartbeat ACK variant; it emits a Heartbeat frame with request flags and a zero-length payload field.

Internal Rust peers tolerate this shared representation, but an independent protocol implementation following the design can reject or misparse it.

### 21. Several documented wire layouts differ from implementation

- READ errors include undocumented `extent_id` and `offset` fields in code (`components/rpc/src/frame/header.rs:56-62`, `components/rpc/src/frame/encode.rs:243-255`; compare `design.md:589-600`).
- REPORT_EXTENTS errors include an undocumented epoch (`components/rpc/src/frame/header.rs:241-246`, `components/rpc/src/frame/encode.rs:668-677`; compare `design.md:853-864`).
- `UpdateExtentSealed` includes `new_extent_capacity`, which is missing from the listed fields (`components/rpc/src/frame/header.rs:203-213`; compare `design.md:782-792`).
- The documented byte counts for Forward, ForwardChecksum, ForwardFlushed, Watermark, UpdateExtentProgress, and UpdateExtentFlushed are each four bytes larger than the sum of their fields (`design.md:358-416`, `design.md:764-815`).
- Integer byte order is not documented, although the implementation consistently uses big-endian/network order.

These discrepancies can break non-Rust clients even when internal peers agree.

### 22. Public frame construction does not enforce valid flag combinations

The design says flags are derived from the variable-header shape and invalid combinations are rejected at compile time (`design.md:232-236`). `FixedHeader.flags` and `Frame.header` are public, and encoding ORs caller-provided flags into computed flags (`components/rpc/src/frame/header.rs:9-16`, `components/rpc/src/frame/mod.rs:296-350`).

Callers can construct ambiguous combinations such as a success body with the error flag set.

### 23. Length-prefixed strings can overflow their `u16` length

Protocol strings use `u16` lengths. Encoding casts an unrestricted Rust string length with `as u16` and still writes all bytes (`components/rpc/src/frame/encode.rs:325-343`, `components/rpc/src/payload.rs:64-69`). Values over 65,535 bytes wrap the declared length and shift subsequent fields.

### 24. Client response validation is inconsistent

`create_stream` validates the response opcode and variant, while append, read, and query methods accept generic non-error frames and extract fields through defaulting accessors (`components/client/src/lib.rs:301-318`, `components/client/src/lib.rs:345-410`). A mismatched response can become a plausible success containing zero-valued IDs or offsets.

Malformed READ payloads are also returned as successful partial results: decoding stops on truncation instead of validating the advertised count (`components/client/src/lib.rs:379-396`).

### 25. The documented client connection pool/router is absent

The design describes one connection pool per Extent Node and automatic use of discovered Primary addresses (`design.md:1035-1039`). `StreamClient` owns one TCP connection, while discovered addresses are cached but not used to route append/read operations (`components/client/src/lib.rs:66-121`, `components/client/src/lib.rs:728-754`).

Callers must manually create and select separate clients.

## P3: Smaller Correctness Issues

### 26. Internal index capacity does not account for empty payloads

The index is sized as `extent_capacity / 5`, assuming a minimum record size of a four-byte header plus one payload byte (`design.md:1287-1291`, `components/extent-node/src/extent.rs:21-24`). Empty payloads are valid four-byte records. Once the number of records exceeds the index length, insertion is silently skipped and logical reads of the tail fail.

### 27. Zero growth factor is not normalized on RegisterExtent

The protocol defines growth factor zero as the default value two (`design.md:264`, `design.md:726-729`). `ForwardInitExtent` normalizes it, but `RegisterExtent` passes zero through (`components/extent-node/src/store/register.rs:63-99`). The next extent capacity can consequently be multiplied by zero (`components/extent-node/src/stream.rs:752-760`).

### 28. One follower-triggered transition omits ForwardChecksum

Most autonomous transition paths send the metadata update, checksum, and flush request. When a drained follower triggers extent-full after the leader's own append succeeds, the final notification loop sends only the update and flush request (`components/extent-node/src/store/append.rs:241-247`). Integrity behavior therefore depends on which append in the leader batch crosses the extent boundary.

### 29. Multipart completion failure does not abort the upload

Part-upload failures trigger `abort_multipart_upload`, but a failure from `complete_multipart_upload` returns directly without cleanup (`components/extent-node/src/s3.rs:270-305`). This can leave incomplete multipart uploads in object storage.

### 30. S3 metadata fields are not maintained

The extent schema contains `s3_key` and `flushed_at` (`design.md:1458-1468`, `components/stream-manager/migrations/V2__create_extent_table.sql:1-13`). The normal flush update changes only state (`components/stream-manager/src/metadata.rs:1481-1494`). Neither field records the successful upload.

## Documentation Drift and Internal Contradictions

1. The introductory seal section uses obsolete request flags and legacy extent-based behavior that conflict with the current epoch-only protocol (`design.md:36-60` versus `design.md:419-460`).
2. The index sentinel is documented as `u32::MAX`, while the implementation uses zero and stores `byte_pos + 1` (`design.md:1287-1296`, `components/extent-node/src/extent.rs:13-19`).
3. S3 flushing is mostly assigned to the Primary, but two sections assign it to Secondary-1 (`design.md:807`, `design.md:1336`). The implementation consistently uses the Primary.
4. Phase 2 says seal resolution takes the minimum offset, while the current design and implementation use quorum order statistics (`design.md:1584-1590`).
5. Phase 3 lists cold reads, time-based flush, a read cache, and MinIO integration tests as deliverables, but these are not implemented (`design.md:1595-1606`).
6. The crate tree omits the SNAFU crates and describes RPC framing as a single file rather than a module directory (`design.md:1041-1103`).
7. `common` is described as having zero runtime dependencies, but it directly depends on `sqlx`, `serde`, `toml`, and `nix` (`design.md:1120-1123`, `components/common/Cargo.toml:6-12`).
8. `moka` is listed as a dependency but is absent from the workspace and Extent Node manifests (`design.md:1139`).
9. The sample S3 configuration documents a bucket default that differs from the actual empty default (`conf/extent-node.toml:40-42`, `components/common/src/config.rs:102-104`).
10. Several SQL definitions in the design differ from the migrations in integer signedness, timestamp types, defaults, and leadership seed data (`design.md:1484-1522`).

## Verified Areas

The review also confirmed that these areas broadly match the design:

- Opcode numbers, protocol magic, protocol version, and fixed-header size.
- Stream-level leader election and group-commit ownership.
- Quorum formula `required_secondary_acks = RF / 2`.
- Healthy-path replication of the Primary-assigned byte position.
- Normal inclusive Watermark and exclusive QueryOffset conventions.
- O(1) internal index lookup on the normal path.
- Adaptive extent growth, cap, and idle shrink behavior.
- Normal S3-versus-Memory eviction gating.
- S3 v2 object header, offset-based key, chunk index, compression, and CRC layout.
- Leadership acquire, renew, and release SQL.
- Persisted node metrics and load-aware allocation.

## Test Status During Review

- `cargo test --workspace --lib`: 121 tests passed.
- `cargo test --workspace`: unit tests passed, then the suite stopped at `describe_stream_rf2_integration` because the configured external MySQL connection timed out at `tests/describe_rf2_integration.rs:45-49`.
- Integration tests after that failure were not executed.
