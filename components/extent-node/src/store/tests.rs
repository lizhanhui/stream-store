use super::*;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::config::{
    DEFAULT_CACHE_EXTENTS, DEFAULT_EXTENT_GROWTH_FACTOR, DEFAULT_MAX_EXTENT_CAPACITY,
    DEFAULT_MIN_EXTENT_CAPACITY,
};
use common::types::{
    Epoch, ErrorCode, ExtentId, ExtentPolicy, Offset, Opcode, StorageClass, StreamConfig, StreamId,
};
use rpc::frame::{Frame, VariableHeader};
use server::handler::RequestHandler;
use tokio::sync::mpsc;

use crate::ack_queue::{AckQueue, DEFAULT_REPLICATION_TIMEOUT, PendingAck};
use crate::stream::SealReason;

/// Build a default `StreamConfig` for tests.
fn test_config(stream_id: u32, replication_factor: u8) -> StreamConfig {
    StreamConfig {
        stream_id: StreamId(stream_id),
        replication_factor,
        epoch: Epoch(0),
        storage_class: StorageClass::S3,
        policy: ExtentPolicy {
            cache: DEFAULT_CACHE_EXTENTS,
            min_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
            max_capacity: DEFAULT_MAX_EXTENT_CAPACITY,
            scale_factor: DEFAULT_EXTENT_GROWTH_FACTOR,
        },
    }
}

/// Register a stream on the ExtentNode via RegisterExtent (RF=1, Primary, no secondaries).
/// This is the production path: StreamManager assigns a stream_id and sends RegisterExtent.
async fn register_stream(store: &ExtentNodeStore, stream_id: u32, req_id: u32) -> StreamId {
    use rpc::payload::build_register_extent_payload;

    let sid = StreamId(stream_id);
    let payload = build_register_extent_payload(&[]);
    let resp = store
        .handle_frame(
            Frame::new(
                VariableHeader::RegisterExtent {
                    request_id: req_id,
                    extent_id: ExtentId(1),
                    role: 0,
                    start_offset: Offset(0),
                    config: test_config(stream_id, 1),
                },
                Some(payload),
            ),
            None,
        )
        .await
        .unwrap();
    assert_eq!(resp.opcode(), Opcode::RegisterExtent);
    sid
}

async fn register_stream_at_epoch(
    store: &ExtentNodeStore,
    stream_id: u32,
    req_id: u32,
    epoch: Epoch,
) -> StreamId {
    use rpc::payload::build_register_extent_payload;

    let sid = StreamId(stream_id);
    let mut config = test_config(stream_id, 1);
    config.epoch = epoch;
    store
        .handle_frame(
            Frame::new(
                VariableHeader::RegisterExtent {
                    request_id: req_id,
                    extent_id: ExtentId(1),
                    role: 0,
                    start_offset: Offset(0),
                    config,
                },
                Some(build_register_extent_payload(&[])),
            ),
            None,
        )
        .await
        .unwrap();
    sid
}

#[tokio::test]
async fn create_and_append() {
    let store = ExtentNodeStore::new();
    let sid = register_stream(&store, 1, 1).await;

    let resp = store
        .handle_frame(
            Frame::new(
                VariableHeader::Append {
                    request_id: 2,
                    stream_id: sid,
                    epoch: Epoch(0),
                },
                Some(Bytes::from_static(b"hello")),
            ),
            None,
        )
        .await
        .unwrap();

    assert_eq!(resp.opcode(), Opcode::Append);
    assert_eq!(resp.offset(), Offset(0));
}

#[tokio::test]
async fn append_to_unknown_stream() {
    let store = ExtentNodeStore::new();
    let resp = store
        .handle_frame(
            Frame::new(
                VariableHeader::Append {
                    request_id: 1,
                    stream_id: StreamId(999),
                    epoch: Epoch(0),
                },
                Some(Bytes::from_static(b"fail")),
            ),
            None,
        )
        .await
        .unwrap();
    assert_eq!(resp.opcode(), Opcode::Append);
    assert!(resp.is_error_response());
}

#[tokio::test]
async fn append_to_sealed_stream_reports_extent_id() {
    let store = ExtentNodeStore::new();
    let sid = register_stream(&store, 1, 1).await;

    {
        let streams = store.streams.pin();
        let stream = streams.get(&sid).unwrap();
        assert_eq!(stream.seal(ExtentId(1), None), Some((0, 0)));
    }

    let resp = store
        .handle_frame(
            Frame::new(
                VariableHeader::Append {
                    request_id: 2,
                    stream_id: sid,
                    epoch: Epoch(0),
                },
                Some(Bytes::from_static(b"sealed")),
            ),
            None,
        )
        .await
        .unwrap();

    assert_eq!(resp.opcode(), Opcode::Append);
    assert!(resp.is_error_response());
    assert_eq!(resp.error_code(), ErrorCode::ExtentSealed as u16);
    assert_eq!(resp.extent_id(), ExtentId(1));
}

#[tokio::test]
async fn append_read_query_offset() {
    let store = ExtentNodeStore::new();
    let sid = register_stream(&store, 1, 1).await;

    for i in 0u32..3 {
        let resp = store
            .handle_frame(
                Frame::new(
                    VariableHeader::Append {
                        request_id: 10 + i,
                        stream_id: sid,
                        epoch: Epoch(0),
                    },
                    Some(Bytes::from(format!("msg{i}"))),
                ),
                None,
            )
            .await
            .unwrap();
        assert_eq!(resp.opcode(), Opcode::Append);
        assert_eq!(resp.offset(), Offset(i as u64));
    }

    let resp = store
        .handle_frame(
            Frame::new(
                VariableHeader::QueryOffset {
                    request_id: 20,
                    stream_id: sid,
                },
                None,
            ),
            None,
        )
        .await
        .unwrap();
    assert_eq!(resp.opcode(), Opcode::QueryOffset);
    assert_eq!(resp.offset(), Offset(3));

    // Read all 3 from offset 0.
    let resp = store
        .handle_frame(
            Frame::new(
                VariableHeader::Read {
                    request_id: 30,
                    stream_id: sid,
                    extent_id: ExtentId(1),
                    offset: Offset(0),
                    count: 3,
                },
                None,
            ),
            None,
        )
        .await
        .unwrap();
    assert_eq!(resp.opcode(), Opcode::Read);
    assert_eq!(resp.count(), 3);

    let resp_payload = resp.payload.as_ref().unwrap();
    let mut payload = &resp_payload[..];
    for i in 0..3 {
        let len = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
        payload = &payload[4..];
        let msg = &payload[..len];
        assert_eq!(msg, format!("msg{i}").as_bytes());
        payload = &payload[len..];
    }
    assert!(payload.is_empty());

    // Read msg1 directly via its offset.
    let resp = store
        .handle_frame(
            Frame::new(
                VariableHeader::Read {
                    request_id: 31,
                    stream_id: sid,
                    extent_id: ExtentId(1),
                    offset: Offset(1),
                    count: 1,
                },
                None,
            ),
            None,
        )
        .await
        .unwrap();
    assert_eq!(resp.opcode(), Opcode::Read);
    assert_eq!(resp.count(), 1);
    let resp_payload = resp.payload.as_ref().unwrap();
    let len = u32::from_be_bytes([
        resp_payload[0],
        resp_payload[1],
        resp_payload[2],
        resp_payload[3],
    ]) as usize;
    assert_eq!(&resp_payload[4..4 + len], b"msg1");
}

#[tokio::test]
async fn register_extent_creates_stream() {
    use rpc::payload::build_register_extent_payload;

    let store = ExtentNodeStore::new();

    // RegisterExtent as Primary with 1 secondary (RF=2).
    let payload = build_register_extent_payload(&["127.0.0.1:9802"]);
    let resp = store
        .handle_frame(
            Frame::new(
                VariableHeader::RegisterExtent {
                    request_id: 1,
                    extent_id: ExtentId(100),
                    role: 0,
                    start_offset: Offset(0),
                    config: test_config(42, 2),
                },
                Some(payload),
            ),
            None,
        )
        .await
        .unwrap();

    assert_eq!(resp.opcode(), Opcode::RegisterExtent);
    assert_eq!(resp.stream_id(), StreamId(42));

    assert!(store.streams.pin().contains_key(&StreamId(42)));

    let ri = store.get_replica_info(StreamId(42)).unwrap();
    assert!(ri.is_primary());
    assert!(!ri.is_standalone());
    assert_eq!(ri.replica_addrs, vec!["127.0.0.1:9802"]);
    assert_eq!(ri.extent_id, ExtentId(100));
    assert_eq!(ri.replication_factor, 2);

    // AckQueue should be initialized for Primary.
    {
        let streams = store.streams.pin();
        let stream = streams.get(&StreamId(42)).unwrap();
        let aq = stream
            .ack_queue()
            .expect("Primary should have AckQueue")
            .lock_inner();
        assert_eq!(aq.required_acks, 1);
    }
}

/// Secondaries are created solely via ForwardInitExtent (the Primary's in-band
/// init frame), not via a RegisterExtent RPC. The extent must be created at the
/// wire `start_offset` with no AckQueue.
#[tokio::test]
async fn forward_init_extent_creates_secondary_extent() {
    let store = ExtentNodeStore::new();

    // Primary sends ForwardInitExtent to the secondary (role=1, RF=2).
    store.handle_forward_init_extent(Frame::new(
        VariableHeader::ForwardInitExtent {
            stream_id: StreamId(42),
            extent_id: ExtentId(100),
            epoch: Epoch(1),
            start_offset: Offset(1_000),
            extent_capacity: 8 * 1024 * 1024,
            cache_extents: 4,
            min_extent_capacity: 8 * 1024 * 1024,
            max_extent_capacity: 256 * 1024 * 1024,
            extent_growth_factor: 2,
            storage_class: StorageClass::S3,
        },
        None,
    ));

    // The extent exists at the wire start_offset.
    {
        let streams = store.streams.pin();
        let stream = streams.get(&StreamId(42)).unwrap();
        let start = stream.with_extent(ExtentId(100), |ext| ext.start_offset);
        assert_eq!(start, Some(Offset(1_000)));
        // Secondaries have no AckQueue (no group-commit ownership).
        assert!(stream.ack_queue().is_none());
    }
}

/// Regression test (finding 3): a node with no prior state for the stream must
/// create the extent at the SM-assigned start_offset carried by RegisterExtent,
/// not at its local (empty) frontier of 0.
#[tokio::test]
async fn register_extent_uses_wire_start_offset_on_fresh_node() {
    use rpc::payload::build_register_extent_payload;

    let store = ExtentNodeStore::new();

    let mut config = test_config(42, 1);
    config.epoch = Epoch(1);
    let payload = build_register_extent_payload(&[]);
    let resp = store
        .handle_frame(
            Frame::new(
                VariableHeader::RegisterExtent {
                    request_id: 1,
                    extent_id: ExtentId(7),
                    role: 0,
                    start_offset: Offset(1_000),
                    config,
                },
                Some(payload),
            ),
            None,
        )
        .await
        .unwrap();
    assert_eq!(resp.opcode(), Opcode::RegisterExtent);

    // The first append after the epoch change must land at offset 1000, not 0.
    let resp = store
        .handle_frame(
            Frame::new(
                VariableHeader::Append {
                    request_id: 2,
                    stream_id: StreamId(42),
                    epoch: Epoch(1),
                },
                Some(Bytes::from_static(b"hello")),
            ),
            None,
        )
        .await
        .unwrap();
    assert_eq!(resp.offset(), Offset(1_000));
}

/// Regression test (finding 3): a lagging node holding stale local state must
/// also honor the SM-assigned start_offset rather than its stale frontier.
#[tokio::test]
async fn register_extent_uses_wire_start_offset_over_stale_local_state() {
    use rpc::payload::build_register_extent_payload;

    let store = ExtentNodeStore::new();
    let sid = register_stream(&store, 42, 1).await;

    // Local frontier advances to 1 — stale relative to the SM's view (1000).
    let resp = store
        .handle_frame(
            Frame::new(
                VariableHeader::Append {
                    request_id: 2,
                    stream_id: sid,
                    epoch: Epoch(0),
                },
                Some(Bytes::from_static(b"old")),
            ),
            None,
        )
        .await
        .unwrap();
    assert_eq!(resp.offset(), Offset(0));

    // SM registers the successor extent (epoch 1) with authoritative start_offset.
    let mut config = test_config(42, 1);
    config.epoch = Epoch(1);
    let payload = build_register_extent_payload(&[]);
    store
        .handle_frame(
            Frame::new(
                VariableHeader::RegisterExtent {
                    request_id: 3,
                    extent_id: ExtentId(2),
                    role: 0,
                    start_offset: Offset(1_000),
                    config,
                },
                Some(payload),
            ),
            None,
        )
        .await
        .unwrap();

    // Append under the new epoch lands at 1000, not at the stale frontier (1).
    let resp = store
        .handle_frame(
            Frame::new(
                VariableHeader::Append {
                    request_id: 4,
                    stream_id: sid,
                    epoch: Epoch(1),
                },
                Some(Bytes::from_static(b"new")),
            ),
            None,
        )
        .await
        .unwrap();
    assert_eq!(resp.offset(), Offset(1_000));
}

/// RegisterExtent is idempotent: when the extent already exists (lazily created
/// by ForwardInitExtent), registration must not clobber the established
/// start_offset — neither with a matching nor with a mismatched value.
#[tokio::test]
async fn register_extent_after_lazy_init_keeps_start_offset() {
    use rpc::payload::build_register_extent_payload;

    let store = ExtentNodeStore::new();

    // Primary's ForwardInitExtent arrives first and creates the extent at 1000.
    store.handle_forward_init_extent(Frame::new(
        VariableHeader::ForwardInitExtent {
            stream_id: StreamId(42),
            extent_id: ExtentId(7),
            epoch: Epoch(1),
            start_offset: Offset(1_000),
            extent_capacity: 8 * 1024 * 1024,
            cache_extents: 4,
            min_extent_capacity: 8 * 1024 * 1024,
            max_extent_capacity: 256 * 1024 * 1024,
            extent_growth_factor: 2,
            storage_class: StorageClass::S3,
        },
        None,
    ));

    // Late RegisterExtent with the matching value: no-op.
    let mut config = test_config(42, 1);
    config.epoch = Epoch(1);
    let payload = build_register_extent_payload(&[]);
    store
        .handle_frame(
            Frame::new(
                VariableHeader::RegisterExtent {
                    request_id: 1,
                    extent_id: ExtentId(7),
                    role: 0,
                    start_offset: Offset(1_000),
                    config,
                },
                Some(payload.clone()),
            ),
            None,
        )
        .await
        .unwrap();
    {
        let streams = store.streams.pin();
        let stream = streams.get(&StreamId(42)).unwrap();
        assert_eq!(stream.max_offset(), Offset(1_000));
    }

    // Late RegisterExtent with a mismatched value must not clobber either.
    store
        .handle_frame(
            Frame::new(
                VariableHeader::RegisterExtent {
                    request_id: 2,
                    extent_id: ExtentId(7),
                    role: 0,
                    start_offset: Offset(0),
                    config,
                },
                Some(payload),
            ),
            None,
        )
        .await
        .unwrap();
    {
        let streams = store.streams.pin();
        let stream = streams.get(&StreamId(42)).unwrap();
        assert_eq!(stream.max_offset(), Offset(1_000));
    }
}

#[tokio::test]
async fn register_extent_rejects_secondary_role() {
    use rpc::payload::build_register_extent_payload;

    let store = ExtentNodeStore::new();
    let stream_id = StreamId(42);
    let response = store
        .handle_frame(
            Frame::new(
                VariableHeader::RegisterExtent {
                    request_id: 1,
                    extent_id: ExtentId(100),
                    role: 1,
                    start_offset: Offset(0),
                    config: test_config(stream_id.0, 2),
                },
                Some(build_register_extent_payload(&[])),
            ),
            None,
        )
        .await
        .unwrap();

    assert!(response.is_error_response());
    assert_eq!(response.error_code(), ErrorCode::NotPrimary as u16);
    assert!(store.streams.pin().get(&stream_id).is_none());
    assert!(store.get_replica_info(stream_id).is_none());
}

#[tokio::test]
async fn lazy_secondary_rejects_client_append_without_mutation() {
    let store = ExtentNodeStore::new();
    let stream_id = StreamId(43);

    assert!(
        store
            .handle_frame(
                Frame::new(
                    VariableHeader::ForwardInitExtent {
                        stream_id,
                        extent_id: ExtentId(101),
                        epoch: Epoch(7),
                        start_offset: Offset(12),
                        extent_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
                        cache_extents: DEFAULT_CACHE_EXTENTS,
                        min_extent_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
                        max_extent_capacity: DEFAULT_MAX_EXTENT_CAPACITY,
                        extent_growth_factor: DEFAULT_EXTENT_GROWTH_FACTOR,
                        storage_class: StorageClass::S3,
                    },
                    None,
                ),
                None,
            )
            .await
            .is_none()
    );
    let replica = store
        .get_replica_info(stream_id)
        .expect("ForwardInitExtent must install explicit Secondary authority");
    assert!(!replica.is_primary());
    assert_eq!(replica.epoch, Epoch(7));
    assert_eq!(replica.extent_id, ExtentId(101));

    let resp = store
        .handle_frame(
            Frame::new(
                VariableHeader::Append {
                    request_id: 2,
                    stream_id,
                    epoch: Epoch(7),
                },
                Some(Bytes::from_static(b"must not be written")),
            ),
            None,
        )
        .await
        .unwrap();

    assert!(resp.is_error_response());
    assert_eq!(resp.error_code(), ErrorCode::NotPrimary as u16);
    let streams = store.streams.pin();
    assert_eq!(streams.get(&stream_id).unwrap().max_offset(), Offset(12));
}

#[tokio::test]
async fn forward_init_newer_epoch_demotes_former_primary() {
    use rpc::payload::build_register_extent_payload;

    let store = ExtentNodeStore::new();
    let stream_id = StreamId(44);
    let mut primary_config = test_config(stream_id.0, 2);
    primary_config.epoch = Epoch(3);
    store
        .handle_frame(
            Frame::new(
                VariableHeader::RegisterExtent {
                    request_id: 1,
                    extent_id: ExtentId(10),
                    role: 0,
                    start_offset: Offset(20),
                    config: primary_config,
                },
                Some(build_register_extent_payload(&["127.0.0.1:9802"])),
            ),
            None,
        )
        .await
        .unwrap();
    assert!(store.get_replica_info(stream_id).unwrap().is_primary());

    store.handle_forward_init_extent(Frame::new(
        VariableHeader::ForwardInitExtent {
            stream_id,
            extent_id: ExtentId(11),
            epoch: Epoch(4),
            start_offset: Offset(20),
            extent_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
            cache_extents: DEFAULT_CACHE_EXTENTS,
            min_extent_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
            max_extent_capacity: DEFAULT_MAX_EXTENT_CAPACITY,
            extent_growth_factor: DEFAULT_EXTENT_GROWTH_FACTOR,
            storage_class: StorageClass::S3,
        },
        None,
    ));

    let replica = store.get_replica_info(stream_id).unwrap();
    assert!(!replica.is_primary());
    assert_eq!(replica.epoch, Epoch(4));
    assert_eq!(replica.extent_id, ExtentId(11));

    let resp = store
        .handle_frame(
            Frame::new(
                VariableHeader::Append {
                    request_id: 2,
                    stream_id,
                    epoch: Epoch(4),
                },
                Some(Bytes::from_static(b"must not be written")),
            ),
            None,
        )
        .await
        .unwrap();
    assert!(resp.is_error_response());
    assert_eq!(resp.error_code(), ErrorCode::NotPrimary as u16);
    let streams = store.streams.pin();
    let stream = streams.get(&stream_id).unwrap();
    assert_eq!(stream.max_offset(), Offset(20));
    assert!(!stream.has_secondaries());
    assert!(!stream.ack_queue().unwrap().is_active_at(Epoch(4)));
    drop(streams);
    assert!(
        store
            .seal_and_create(stream_id, SealReason::ExtentFull)
            .is_none(),
        "a demoted Primary must not autonomously seal/create"
    );
}

#[tokio::test]
async fn forward_rejects_primary_role_without_mutation() {
    let store = ExtentNodeStore::new();
    let stream_id = register_stream(&store, 45, 1).await;

    let resp = store.handle_forward(Frame::new(
        VariableHeader::Forward {
            stream_id,
            extent_id: ExtentId(1),
            epoch: Epoch(0),
            offset: Offset(0),
            byte_pos: 0,
        },
        Some(Bytes::from_static(b"must not replicate into primary")),
    ));

    assert!(resp.is_none());
    let streams = store.streams.pin();
    assert_eq!(streams.get(&stream_id).unwrap().max_offset(), Offset(0));
}

#[tokio::test]
async fn older_or_equal_forward_init_does_not_demote_current_primary() {
    use rpc::payload::build_register_extent_payload;

    let store = ExtentNodeStore::new();
    let stream_id = StreamId(46);
    let mut config = test_config(stream_id.0, 1);
    config.epoch = Epoch(5);
    store
        .handle_frame(
            Frame::new(
                VariableHeader::RegisterExtent {
                    request_id: 1,
                    extent_id: ExtentId(20),
                    role: 0,
                    start_offset: Offset(100),
                    config,
                },
                Some(build_register_extent_payload(&[])),
            ),
            None,
        )
        .await
        .unwrap();

    for (init_epoch, init_extent) in [(Epoch(4), ExtentId(19)), (Epoch(5), ExtentId(21))] {
        store.handle_forward_init_extent(Frame::new(
            VariableHeader::ForwardInitExtent {
                stream_id,
                extent_id: init_extent,
                epoch: init_epoch,
                start_offset: Offset(0),
                extent_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
                cache_extents: DEFAULT_CACHE_EXTENTS,
                min_extent_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
                max_extent_capacity: DEFAULT_MAX_EXTENT_CAPACITY,
                extent_growth_factor: DEFAULT_EXTENT_GROWTH_FACTOR,
                storage_class: StorageClass::S3,
            },
            None,
        ));

        let replica = store.get_replica_info(stream_id).unwrap();
        assert!(replica.is_primary());
        assert_eq!(replica.epoch, Epoch(5));
        let streams = store.streams.pin();
        let stream = streams.get(&stream_id).unwrap();
        assert_eq!(stream.epoch(), Epoch(5));
        assert!(stream.with_extent(init_extent, |_| ()).is_none());
    }
}

#[tokio::test]
async fn register_extent_rejects_same_id_from_new_epoch() {
    use rpc::payload::build_register_extent_payload;

    let store = ExtentNodeStore::new();
    let stream_id = StreamId(50);
    let mut first = test_config(stream_id.0, 1);
    first.epoch = Epoch(1);
    store
        .handle_frame(
            Frame::new(
                VariableHeader::RegisterExtent {
                    request_id: 1,
                    extent_id: ExtentId(20),
                    role: 0,
                    start_offset: Offset(10),
                    config: first,
                },
                Some(build_register_extent_payload(&[])),
            ),
            None,
        )
        .await
        .unwrap();

    let mut conflicting = first;
    conflicting.epoch = Epoch(2);
    let response = store
        .handle_frame(
            Frame::new(
                VariableHeader::RegisterExtent {
                    request_id: 2,
                    extent_id: ExtentId(20),
                    role: 0,
                    start_offset: Offset(10),
                    config: conflicting,
                },
                Some(build_register_extent_payload(&[])),
            ),
            None,
        )
        .await
        .unwrap();
    assert!(response.is_error_response());
    assert_eq!(response.error_code(), ErrorCode::EpochStale as u16);
    assert_eq!(store.get_replica_info(stream_id).unwrap().epoch, Epoch(1));
}

#[tokio::test]
async fn batch_append_secondary_returns_not_primary_without_mutation() {
    let store = ExtentNodeStore::new();
    let stream_id = StreamId(47);
    store.handle_forward_init_extent(Frame::new(
        VariableHeader::ForwardInitExtent {
            stream_id,
            extent_id: ExtentId(30),
            epoch: Epoch(7),
            start_offset: Offset(12),
            extent_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
            cache_extents: DEFAULT_CACHE_EXTENTS,
            min_extent_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
            max_extent_capacity: DEFAULT_MAX_EXTENT_CAPACITY,
            extent_growth_factor: DEFAULT_EXTENT_GROWTH_FACTOR,
            storage_class: StorageClass::S3,
        },
        None,
    ));
    let frames = vec![
        Frame::new(
            VariableHeader::Append {
                request_id: 1,
                stream_id,
                epoch: Epoch(7),
            },
            Some(Bytes::from_static(b"a")),
        ),
        Frame::new(
            VariableHeader::Append {
                request_id: 2,
                stream_id,
                epoch: Epoch(7),
            },
            Some(Bytes::from_static(b"b")),
        ),
    ];

    let responses = store.handle_append_batch_inner(&frames, None).await;
    assert_eq!(responses.len(), 2);
    assert!(responses.iter().all(Frame::is_error_response));
    assert!(
        responses
            .iter()
            .all(|response| response.error_code() == ErrorCode::NotPrimary as u16)
    );
    let streams = store.streams.pin();
    assert_eq!(streams.get(&stream_id).unwrap().max_offset(), Offset(12));
}

#[tokio::test]
async fn mixed_epoch_batch_validates_each_frame() {
    let store = ExtentNodeStore::new();
    let stream_id = register_stream_at_epoch(&store, 48, 1, Epoch(3)).await;
    let frames = vec![
        Frame::new(
            VariableHeader::Append {
                request_id: 1,
                stream_id,
                epoch: Epoch(3),
            },
            Some(Bytes::from_static(b"accepted")),
        ),
        Frame::new(
            VariableHeader::Append {
                request_id: 2,
                stream_id,
                epoch: Epoch(4),
            },
            Some(Bytes::from_static(b"rejected")),
        ),
    ];

    let responses = RequestHandler::handle_append_batch(&store, &frames, None).await;
    assert_eq!(responses.len(), 2);
    assert!(!responses[0].is_error_response());
    assert!(responses[1].is_error_response());
    assert_eq!(responses[1].error_code(), ErrorCode::EpochStale as u16);
    let streams = store.streams.pin();
    assert_eq!(streams.get(&stream_id).unwrap().max_offset(), Offset(1));
}

#[test]
fn replication_gap_quarantines_secondary_until_new_epoch() {
    let store = ExtentNodeStore::new();
    let stream_id = StreamId(49);
    store.handle_forward_init_extent(Frame::new(
        VariableHeader::ForwardInitExtent {
            stream_id,
            extent_id: ExtentId(40),
            epoch: Epoch(7),
            start_offset: Offset(0),
            extent_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
            cache_extents: DEFAULT_CACHE_EXTENTS,
            min_extent_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
            max_extent_capacity: DEFAULT_MAX_EXTENT_CAPACITY,
            extent_growth_factor: DEFAULT_EXTENT_GROWTH_FACTOR,
            storage_class: StorageClass::S3,
        },
        None,
    ));
    assert!(
        store
            .handle_forward(Frame::new(
                VariableHeader::Forward {
                    stream_id,
                    extent_id: ExtentId(40),
                    epoch: Epoch(7),
                    offset: Offset(2),
                    byte_pos: 0,
                },
                Some(Bytes::from_static(b"gap")),
            ))
            .is_none()
    );

    // A successor in the same epoch must not let the gapped replica rejoin.
    store.handle_forward_init_extent(Frame::new(
        VariableHeader::ForwardInitExtent {
            stream_id,
            extent_id: ExtentId(41),
            epoch: Epoch(7),
            start_offset: Offset(3),
            extent_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
            cache_extents: DEFAULT_CACHE_EXTENTS,
            min_extent_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
            max_extent_capacity: DEFAULT_MAX_EXTENT_CAPACITY,
            extent_growth_factor: DEFAULT_EXTENT_GROWTH_FACTOR,
            storage_class: StorageClass::S3,
        },
        None,
    ));
    {
        let streams = store.streams.pin();
        assert!(
            streams
                .get(&stream_id)
                .unwrap()
                .with_extent(ExtentId(41), |_| ())
                .is_none()
        );
    }

    // Only a newer epoch clears the quarantine.
    store.handle_forward_init_extent(Frame::new(
        VariableHeader::ForwardInitExtent {
            stream_id,
            extent_id: ExtentId(42),
            epoch: Epoch(8),
            start_offset: Offset(3),
            extent_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
            cache_extents: DEFAULT_CACHE_EXTENTS,
            min_extent_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
            max_extent_capacity: DEFAULT_MAX_EXTENT_CAPACITY,
            extent_growth_factor: DEFAULT_EXTENT_GROWTH_FACTOR,
            storage_class: StorageClass::S3,
        },
        None,
    ));
    let streams = store.streams.pin();
    assert!(
        streams
            .get(&stream_id)
            .unwrap()
            .with_extent(ExtentId(42), |_| ())
            .is_some()
    );
}

#[test]
fn same_epoch_successor_rejects_missing_predecessor_tail() {
    let store = ExtentNodeStore::new();
    let stream_id = StreamId(51);
    store.handle_forward_init_extent(Frame::new(
        VariableHeader::ForwardInitExtent {
            stream_id,
            extent_id: ExtentId(50),
            epoch: Epoch(7),
            start_offset: Offset(0),
            extent_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
            cache_extents: DEFAULT_CACHE_EXTENTS,
            min_extent_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
            max_extent_capacity: DEFAULT_MAX_EXTENT_CAPACITY,
            extent_growth_factor: DEFAULT_EXTENT_GROWTH_FACTOR,
            storage_class: StorageClass::S3,
        },
        None,
    ));
    assert!(
        store
            .handle_forward(Frame::new(
                VariableHeader::Forward {
                    stream_id,
                    extent_id: ExtentId(50),
                    epoch: Epoch(7),
                    offset: Offset(0),
                    byte_pos: 0,
                },
                Some(Bytes::from_static(b"only-prefix")),
            ))
            .is_some()
    );

    // start_offset 2 proves offset 1 was committed on the Primary but absent here.
    store.handle_forward_init_extent(Frame::new(
        VariableHeader::ForwardInitExtent {
            stream_id,
            extent_id: ExtentId(51),
            epoch: Epoch(7),
            start_offset: Offset(2),
            extent_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
            cache_extents: DEFAULT_CACHE_EXTENTS,
            min_extent_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
            max_extent_capacity: DEFAULT_MAX_EXTENT_CAPACITY,
            extent_growth_factor: DEFAULT_EXTENT_GROWTH_FACTOR,
            storage_class: StorageClass::S3,
        },
        None,
    ));

    assert!(store.forwarding_quarantined(stream_id, Epoch(7)));
    let streams = store.streams.pin();
    assert!(
        streams
            .get(&stream_id)
            .unwrap()
            .with_extent(ExtentId(51), |_| ())
            .is_none()
    );
}

#[test]
fn quarantine_epoch_update_is_monotonic() {
    let store = Arc::new(ExtentNodeStore::new());
    let stream_id = StreamId(52);
    let mut threads = Vec::new();
    for epoch in [7, 8].into_iter().cycle().take(64) {
        let store = Arc::clone(&store);
        threads.push(std::thread::spawn(move || {
            store.quarantine_forwarding(stream_id, Epoch(epoch));
        }));
    }
    for thread in threads {
        thread.join().unwrap();
    }
    assert!(store.forwarding_quarantined(stream_id, Epoch(8)));
}

#[tokio::test]
async fn register_extent_then_append_rf1() {
    use rpc::payload::build_register_extent_payload;

    let store = ExtentNodeStore::new();

    // Register as Primary, RF=1 (standalone).
    let payload = build_register_extent_payload(&[]);
    store
        .handle_frame(
            Frame::new(
                VariableHeader::RegisterExtent {
                    request_id: 1,
                    extent_id: ExtentId(50),
                    role: 0,
                    start_offset: Offset(0),
                    config: test_config(10, 1),
                },
                Some(payload),
            ),
            None,
        )
        .await
        .unwrap();

    // Append — standalone should ACK immediately.
    let resp = store
        .handle_frame(
            Frame::new(
                VariableHeader::Append {
                    request_id: 2,
                    stream_id: StreamId(10),
                    epoch: Epoch(0),
                },
                Some(Bytes::from_static(b"hello standalone")),
            ),
            None,
        )
        .await
        .unwrap();

    assert_eq!(resp.opcode(), Opcode::Append);
    assert_eq!(resp.offset(), Offset(0));
}

#[tokio::test]
async fn primary_append_defers_and_broadcasts() {
    use futures_util::StreamExt;
    use rpc::codec::FrameCodec;
    use rpc::payload::build_register_extent_payload;
    use tokio_util::codec::FramedRead;

    let (resp_tx, mut resp_rx) = mpsc::channel::<Frame>(100);

    // Start two mock TCP listeners (acting as secondaries).
    let listener1 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr1 = listener1.local_addr().unwrap().to_string();
    let listener2 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr2 = listener2.local_addr().unwrap().to_string();

    let store = Arc::new(ExtentNodeStore::new());
    let pool = Arc::new(crate::downstream::DownstreamPool::new(Arc::clone(&store)));
    store.set_downstream(Arc::clone(&pool));

    // Register as Primary with 2 secondaries (RF=3).
    let payload = build_register_extent_payload(&[&addr1, &addr2]);
    store
        .handle_frame(
            Frame::new(
                VariableHeader::RegisterExtent {
                    request_id: 1,
                    extent_id: ExtentId(50),
                    role: 0,
                    start_offset: Offset(0),
                    config: test_config(10, 3),
                },
                Some(payload),
            ),
            None,
        )
        .await
        .unwrap();

    // Append — should return None (deferred), send 2 Forward frames over TCP.
    let result = store
        .handle_frame(
            Frame::new(
                VariableHeader::Append {
                    request_id: 2,
                    stream_id: StreamId(10),
                    epoch: Epoch(0),
                },
                Some(Bytes::from_static(b"broadcast msg")),
            ),
            Some(&resp_tx),
        )
        .await;

    assert!(
        result.is_none(),
        "Primary with secondaries should defer ACK"
    );

    // Each Secondary observes the one-shot init before the first data Forward.
    let (conn1, _) = listener1.accept().await.unwrap();
    let mut reader1 = FramedRead::new(conn1, FrameCodec);
    let init1 = reader1.next().await.unwrap().unwrap();
    assert!(matches!(
        init1.variable_header,
        VariableHeader::ForwardInitExtent {
            stream_id: StreamId(10),
            extent_id: ExtentId(50),
            ..
        }
    ));
    let fwd1 = reader1.next().await.unwrap().unwrap();
    assert!(matches!(
        fwd1.variable_header,
        VariableHeader::Forward {
            stream_id: StreamId(10),
            extent_id: ExtentId(50),
            offset: Offset(0),
            ..
        }
    ));

    let (conn2, _) = listener2.accept().await.unwrap();
    let mut reader2 = FramedRead::new(conn2, FrameCodec);
    let init2 = reader2.next().await.unwrap().unwrap();
    assert!(matches!(
        init2.variable_header,
        VariableHeader::ForwardInitExtent {
            stream_id: StreamId(10),
            extent_id: ExtentId(50),
            ..
        }
    ));
    let fwd2 = reader2.next().await.unwrap().unwrap();
    assert!(matches!(
        fwd2.variable_header,
        VariableHeader::Forward {
            stream_id: StreamId(10),
            extent_id: ExtentId(50),
            offset: Offset(0),
            ..
        }
    ));

    let streams_guard = store.streams.pin();
    let stream = streams_guard.get(&StreamId(10)).unwrap();
    let aq = stream.ack_queue().expect("Primary should have AckQueue");

    // PendingAck should be in the ack_queue.
    {
        let mut inner = aq.lock_inner();
        inner.receive_pending();
        assert_eq!(inner.pending.len(), 1);
        assert_eq!(inner.pending[0].assigned_offset, 0);
        // RF=3 requires 1 secondary ACK.
        assert_eq!(inner.required_acks, 1);
    }

    // Simulate watermark from first secondary (quorum met with 1 ACK for RF=3).
    {
        let mut inner = aq.lock_inner();
        inner.update_watermark(0, ExtentId(50), 0);
        inner.drain_quorum();
    }

    // The client response channel should now have the AppendAck.
    let ack = resp_rx.try_recv().unwrap();
    assert_eq!(ack.opcode(), Opcode::Append);
    assert_eq!(ack.offset(), Offset(0));
    assert_eq!(ack.request_id(), 2);
}

#[tokio::test]
async fn secondary_returns_watermark() {
    let store = ExtentNodeStore::new();

    store.handle_forward_init_extent(Frame::new(
        VariableHeader::ForwardInitExtent {
            stream_id: StreamId(10),
            extent_id: ExtentId(50),
            epoch: Epoch(0),
            start_offset: Offset(0),
            extent_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
            cache_extents: DEFAULT_CACHE_EXTENTS,
            min_extent_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
            max_extent_capacity: DEFAULT_MAX_EXTENT_CAPACITY,
            extent_growth_factor: DEFAULT_EXTENT_GROWTH_FACTOR,
            storage_class: StorageClass::S3,
        },
        None,
    ));

    // Forward frame (dedicated opcode for replication).
    let resp = store
        .handle_frame(
            Frame::new(
                VariableHeader::Forward {
                    stream_id: StreamId(10),
                    extent_id: ExtentId(50),
                    epoch: Epoch(0),
                    offset: Offset(0),
                    byte_pos: 0,
                },
                Some(Bytes::from_static(b"forwarded msg")),
            ),
            None,
        )
        .await
        .unwrap();

    assert_eq!(resp.opcode(), Opcode::Watermark);
    assert_eq!(resp.stream_id(), StreamId(10));
    assert_eq!(resp.offset(), Offset(0));
}

#[tokio::test]
async fn secondary_withholds_watermark_after_forward_gap() {
    let store = ExtentNodeStore::new();
    let stream_id = StreamId(10);
    let extent_id = ExtentId(50);

    store.handle_forward_init_extent(Frame::new(
        VariableHeader::ForwardInitExtent {
            stream_id,
            extent_id,
            epoch: Epoch(0),
            start_offset: Offset(0),
            extent_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
            cache_extents: DEFAULT_CACHE_EXTENTS,
            min_extent_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
            max_extent_capacity: DEFAULT_MAX_EXTENT_CAPACITY,
            extent_growth_factor: DEFAULT_EXTENT_GROWTH_FACTOR,
            storage_class: StorageClass::S3,
        },
        None,
    ));

    let first = store
        .handle_frame(
            Frame::new(
                VariableHeader::Forward {
                    stream_id,
                    extent_id,
                    epoch: Epoch(0),
                    offset: Offset(0),
                    byte_pos: 0,
                },
                Some(Bytes::from_static(b"msg0")),
            ),
            None,
        )
        .await
        .unwrap();
    assert_eq!(first.offset(), Offset(0));

    let gap = store
        .handle_frame(
            Frame::new(
                VariableHeader::Forward {
                    stream_id,
                    extent_id,
                    epoch: Epoch(0),
                    offset: Offset(2),
                    byte_pos: 8,
                },
                Some(Bytes::from_static(b"msg2")),
            ),
            None,
        )
        .await;

    assert!(
        gap.is_none(),
        "a gap must not produce a cumulative watermark"
    );
    let streams = store.streams.pin();
    assert_eq!(streams.get(&stream_id).unwrap().max_offset(), Offset(1));
}

#[test]
fn watermark_cannot_ack_pending_from_another_extent() {
    let (resp_tx, mut resp_rx) = mpsc::channel::<Frame>(4);
    let ack_queue = AckQueue::new(1);
    ack_queue.enqueue(PendingAck {
        request_id: 1,
        stream_id: StreamId(10),
        extent_id: ExtentId(40),
        epoch: Epoch(0),
        response_tx: resp_tx,
        assigned_offset: 10,
        created_at: Instant::now(),
    });

    ack_queue.update_watermark(Epoch(0), ExtentId(41), 0, 20);

    assert!(resp_rx.try_recv().is_err());
    let mut inner = ack_queue.lock_inner();
    inner.receive_pending();
    assert_eq!(inner.pending.len(), 1);
    assert_eq!(inner.pending[0].extent_id, ExtentId(40));
}

#[tokio::test]
async fn cumulative_ack_drains_multiple_pending() {
    // Test that a single watermark can drain multiple pending ACKs.
    let (resp_tx, mut resp_rx) = mpsc::channel::<Frame>(100);

    let ack_queue = AckQueue::new(1); // need 1 secondary ACK

    // Queue 3 pending ACKs at offsets 0, 1, 2.
    for i in 0u64..3 {
        ack_queue.enqueue(PendingAck {
            request_id: i as u32,
            stream_id: StreamId(10),
            extent_id: ExtentId(0),
            epoch: Epoch(0),
            response_tx: resp_tx.clone(),
            assigned_offset: i,
            created_at: Instant::now(),
        });
    }

    // Single cumulative ACK at offset 2 from one secondary.
    let mut inner = ack_queue.lock_inner();
    inner.receive_pending();
    inner.update_watermark(0, ExtentId(0), 2);
    inner.drain_quorum();
    drop(inner);

    // All 3 should be drained.
    let ack0 = resp_rx.try_recv().unwrap();
    let ack1 = resp_rx.try_recv().unwrap();
    let ack2 = resp_rx.try_recv().unwrap();
    assert_eq!(ack0.offset(), Offset(0));
    assert_eq!(ack1.offset(), Offset(1));
    assert_eq!(ack2.offset(), Offset(2));
    assert!(resp_rx.try_recv().is_err()); // no more
}

#[tokio::test]
async fn quorum_offset_with_multiple_secondaries() {
    let aq = AckQueue::new(2); // RF=4: need 2 secondary ACKs
    let mut inner = aq.lock_inner();

    // Only 1 secondary has reported — not enough for quorum.
    inner.update_watermark(0, ExtentId(0), 5);
    assert!(inner.quorum_offset().is_none());

    // Second secondary reports — now we have quorum.
    inner.update_watermark(1, ExtentId(0), 3);
    // quorum_offset = min of top-2 = 3
    assert_eq!(inner.quorum_offset(), Some(3));

    // Third secondary reports higher.
    inner.update_watermark(2, ExtentId(0), 10);
    // top-2 descending: [10, 5], so quorum_offset = 5
    assert_eq!(inner.quorum_offset(), Some(5));
}

#[tokio::test]
async fn pending_ack_timeout() {
    // Verify that PendingAcks expire after the configured replication timeout.
    let (resp_tx, mut resp_rx) = mpsc::channel::<Frame>(100);

    let ack_queue = AckQueue::new(1); // need 1 secondary ACK

    // Queue a PendingAck with a creation time far in the past (simulates timeout).
    ack_queue.enqueue(PendingAck {
        request_id: 42,
        stream_id: StreamId(10),
        extent_id: ExtentId(0),
        epoch: Epoch(0),
        response_tx: resp_tx.clone(),
        assigned_offset: 0,
        created_at: Instant::now() - DEFAULT_REPLICATION_TIMEOUT - Duration::from_secs(1),
    });

    // Queue a second PendingAck that is NOT expired.
    ack_queue.enqueue(PendingAck {
        request_id: 43,
        stream_id: StreamId(10),
        extent_id: ExtentId(0),
        epoch: Epoch(0),
        response_tx: resp_tx.clone(),
        assigned_offset: 1,
        created_at: Instant::now(),
    });

    // No quorum (no secondary has acked), but timeout sweep should fire.
    let mut inner = ack_queue.lock_inner();
    inner.receive_pending();
    inner.drain_quorum();

    // First PendingAck should have been expired with an error.
    let err_frame = resp_rx.try_recv().unwrap();
    assert_eq!(err_frame.opcode(), Opcode::Append);
    assert!(err_frame.is_error_response());
    assert_eq!(err_frame.request_id(), 42);

    // Second PendingAck should still be pending (not expired).
    assert!(resp_rx.try_recv().is_err());
    assert_eq!(inner.pending.len(), 1);
    assert_eq!(inner.pending[0].request_id, 43);
}

// ── Concurrent multi-stream benchmark ────────────────────────────────────

/// Benchmark: N tokio tasks appending concurrently to N independent streams.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_multi_stream_appends() {
    const NUM_STREAMS: u32 = 8;
    const APPENDS_PER_STREAM: u64 = 5_000;
    const PAYLOAD_SIZE: usize = 128;

    let store = Arc::new(ExtentNodeStore::new());

    let mut stream_ids = Vec::new();
    for i in 0..NUM_STREAMS {
        let sid = register_stream(&store, i + 1, i).await;
        stream_ids.push(sid);
    }

    let start = Instant::now();

    let mut handles = Vec::new();
    for (task_idx, &sid) in stream_ids.iter().enumerate() {
        let store = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            let payload_data = vec![b'A' + (task_idx as u8 % 26); PAYLOAD_SIZE];
            let mut offsets = Vec::with_capacity(APPENDS_PER_STREAM as usize);

            for seq in 0..APPENDS_PER_STREAM {
                let resp = store
                    .handle_frame(
                        Frame::new(
                            VariableHeader::Append {
                                request_id: seq as u32,
                                stream_id: sid,
                                epoch: Epoch(0),
                            },
                            Some(Bytes::from(payload_data.clone())),
                        ),
                        None,
                    )
                    .await
                    .unwrap();

                assert_eq!(
                    resp.opcode(),
                    Opcode::Append,
                    "task {task_idx} seq {seq}: expected AppendAck"
                );
                offsets.push(resp.offset().0);
            }
            offsets
        }));
    }

    let mut all_offsets: Vec<Vec<u64>> = Vec::new();
    for handle in handles {
        all_offsets.push(handle.await.unwrap());
    }

    let elapsed = start.elapsed();

    for (task_idx, offsets) in all_offsets.iter().enumerate() {
        assert_eq!(
            offsets.len(),
            APPENDS_PER_STREAM as usize,
            "task {task_idx}: wrong number of offsets"
        );
        let mut sorted = offsets.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(
            sorted.len(),
            APPENDS_PER_STREAM as usize,
            "task {task_idx}: duplicate offsets detected"
        );
        assert_eq!(*sorted.first().unwrap(), 0);
        assert_eq!(*sorted.last().unwrap(), APPENDS_PER_STREAM - 1);
    }

    for (task_idx, &sid) in stream_ids.iter().enumerate() {
        let resp = store
            .handle_frame(
                Frame::new(
                    VariableHeader::QueryOffset {
                        request_id: 0,
                        stream_id: sid,
                    },
                    None,
                ),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            resp.offset(),
            Offset(APPENDS_PER_STREAM),
            "task {task_idx}: stream max_offset mismatch"
        );
    }

    for (task_idx, &sid) in stream_ids.iter().enumerate() {
        let expected_byte = b'A' + (task_idx as u8 % 26);
        let resp = store
            .handle_frame(
                Frame::new(
                    VariableHeader::Read {
                        request_id: 0,
                        stream_id: sid,
                        extent_id: ExtentId(1),
                        offset: Offset(0),
                        count: 100,
                    },
                    None,
                ),
                None,
            )
            .await
            .unwrap();
        assert_eq!(resp.opcode(), Opcode::Read);
        let count = resp.count() as usize;
        assert!(count > 0, "task {task_idx}: expected at least 1 message");

        let resp_payload = resp.payload.as_ref().unwrap();
        let len = u32::from_be_bytes([
            resp_payload[0],
            resp_payload[1],
            resp_payload[2],
            resp_payload[3],
        ]) as usize;
        assert_eq!(len, PAYLOAD_SIZE, "task {task_idx}: payload size mismatch");
        assert_eq!(
            resp_payload[4], expected_byte,
            "task {task_idx}: payload content mismatch"
        );
    }

    let total_expected = NUM_STREAMS as u64 * APPENDS_PER_STREAM;
    let (appends, bytes, active_count) = store.snapshot_metrics();
    assert_eq!(appends, total_expected, "metrics: append count mismatch");
    assert_eq!(
        bytes,
        total_expected * PAYLOAD_SIZE as u64,
        "metrics: bytes_written mismatch"
    );
    assert_eq!(
        active_count, NUM_STREAMS,
        "metrics: active extent count mismatch"
    );

    let total_ops = total_expected;
    let throughput = total_ops as f64 / elapsed.as_secs_f64();
    let mb_per_sec = (bytes as f64) / elapsed.as_secs_f64() / (1024.0 * 1024.0);
    eprintln!(
        "\n=== Concurrent Multi-Stream Benchmark ===\n\
         Streams: {NUM_STREAMS}, Appends/stream: {APPENDS_PER_STREAM}, \
         Payload: {PAYLOAD_SIZE}B\n\
         Total appends: {total_ops}\n\
         Elapsed: {:.2}ms\n\
         Throughput: {throughput:.0} ops/sec ({mb_per_sec:.1} MiB/sec)\n\
         ==========================================\n",
        elapsed.as_secs_f64() * 1000.0,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_readers_and_writers_different_streams() {
    const NUM_WRITER_STREAMS: u32 = 4;
    const NUM_READER_STREAMS: u32 = 4;
    const APPENDS_PER_STREAM: u64 = 2_000;
    const READS_PER_STREAM: u64 = 2_000;

    let store = Arc::new(ExtentNodeStore::new());

    let mut writer_sids = Vec::new();
    for i in 0..NUM_WRITER_STREAMS {
        let sid = register_stream(&store, i + 1, i).await;
        writer_sids.push(sid);
    }

    let mut reader_sids = Vec::new();
    for i in 0..NUM_READER_STREAMS {
        let sid = register_stream(&store, 100 + i + 1, 100 + i).await;
        for j in 0..100u32 {
            store
                .handle_frame(
                    Frame::new(
                        VariableHeader::Append {
                            request_id: j,
                            stream_id: sid,
                            epoch: Epoch(0),
                        },
                        Some(Bytes::from(format!("pre-{j}"))),
                    ),
                    None,
                )
                .await
                .unwrap();
        }
        reader_sids.push(sid);
    }

    store.snapshot_metrics();

    let mut handles = Vec::new();

    for &sid in &writer_sids {
        let store = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            for seq in 0..APPENDS_PER_STREAM {
                let resp = store
                    .handle_frame(
                        Frame::new(
                            VariableHeader::Append {
                                request_id: seq as u32,
                                stream_id: sid,
                                epoch: Epoch(0),
                            },
                            Some(Bytes::from_static(b"write-payload")),
                        ),
                        None,
                    )
                    .await
                    .unwrap();
                assert_eq!(resp.opcode(), Opcode::Append);
            }
            "writer_done"
        }));
    }

    for &sid in &reader_sids {
        let store = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            for _ in 0..READS_PER_STREAM {
                let resp = store
                    .handle_frame(
                        Frame::new(
                            VariableHeader::Read {
                                request_id: 0,
                                stream_id: sid,
                                extent_id: ExtentId(1),
                                offset: Offset(0),
                                count: 10,
                            },
                            None,
                        ),
                        None,
                    )
                    .await
                    .unwrap();
                assert_eq!(resp.opcode(), Opcode::Read);
                assert!(resp.count() > 0, "reader should get at least 1 message");
            }
            "reader_done"
        }));
    }

    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result == "writer_done" || result == "reader_done");
    }

    for &sid in &writer_sids {
        let resp = store
            .handle_frame(
                Frame::new(
                    VariableHeader::QueryOffset {
                        request_id: 0,
                        stream_id: sid,
                    },
                    None,
                ),
                None,
            )
            .await
            .unwrap();
        assert_eq!(resp.offset(), Offset(APPENDS_PER_STREAM));
    }

    for &sid in &reader_sids {
        let resp = store
            .handle_frame(
                Frame::new(
                    VariableHeader::QueryOffset {
                        request_id: 0,
                        stream_id: sid,
                    },
                    None,
                ),
                None,
            )
            .await
            .unwrap();
        assert_eq!(resp.offset(), Offset(100));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_appends_same_stream() {
    const NUM_TASKS: u64 = 8;
    const APPENDS_PER_TASK: u64 = 2_000;

    let store = Arc::new(ExtentNodeStore::new());
    let sid = register_stream(&store, 1, 1).await;

    let start = Instant::now();

    let mut handles = Vec::new();
    for task_idx in 0..NUM_TASKS {
        let store = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            let (resp_tx, mut resp_rx) = mpsc::channel::<Frame>(16);
            let mut offsets = Vec::with_capacity(APPENDS_PER_TASK as usize);
            for seq in 0..APPENDS_PER_TASK {
                let result = store
                    .handle_frame(
                        Frame::new(
                            VariableHeader::Append {
                                request_id: seq as u32,
                                stream_id: sid,
                                epoch: Epoch(0),
                            },
                            Some(Bytes::from(format!("t{task_idx}-m{seq}"))),
                        ),
                        Some(&resp_tx),
                    )
                    .await;

                let resp = if let Some(frame) = result {
                    frame
                } else {
                    resp_rx.recv().await.unwrap()
                };

                assert_eq!(resp.opcode(), Opcode::Append);
                offsets.push(resp.offset().0);
            }
            offsets
        }));
    }

    let mut all_offsets: Vec<u64> = Vec::new();
    for handle in handles {
        all_offsets.extend(handle.await.unwrap());
    }

    let elapsed = start.elapsed();

    let total = (NUM_TASKS * APPENDS_PER_TASK) as usize;
    assert_eq!(all_offsets.len(), total);

    all_offsets.sort_unstable();
    all_offsets.dedup();
    assert_eq!(
        all_offsets.len(),
        total,
        "duplicate offsets detected across tasks"
    );
    assert_eq!(*all_offsets.first().unwrap(), 0);
    assert_eq!(*all_offsets.last().unwrap(), (total - 1) as u64);

    let resp = store
        .handle_frame(
            Frame::new(
                VariableHeader::QueryOffset {
                    request_id: 0,
                    stream_id: sid,
                },
                None,
            ),
            None,
        )
        .await
        .unwrap();
    assert_eq!(resp.offset(), Offset(total as u64));

    let throughput = total as f64 / elapsed.as_secs_f64();
    eprintln!(
        "\n=== Concurrent Same-Stream Benchmark ===\n\
         Tasks: {NUM_TASKS}, Appends/task: {APPENDS_PER_TASK}\n\
         Total appends: {total}\n\
         Elapsed: {:.2}ms\n\
         Throughput: {throughput:.0} ops/sec\n\
         =========================================\n",
        elapsed.as_secs_f64() * 1000.0,
    );
}

#[tokio::test]
async fn secondary_accepts_forwarded_append_after_seal() {
    let store = ExtentNodeStore::new();

    store.handle_forward_init_extent(Frame::new(
        VariableHeader::ForwardInitExtent {
            stream_id: StreamId(10),
            extent_id: ExtentId(50),
            epoch: Epoch(0),
            start_offset: Offset(0),
            extent_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
            cache_extents: DEFAULT_CACHE_EXTENTS,
            min_extent_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
            max_extent_capacity: DEFAULT_MAX_EXTENT_CAPACITY,
            extent_growth_factor: DEFAULT_EXTENT_GROWTH_FACTOR,
            storage_class: StorageClass::S3,
        },
        None,
    ));

    for i in 0u32..2 {
        let byte_pos = i as u64 * 8;
        let resp = store
            .handle_frame(
                Frame::new(
                    VariableHeader::Forward {
                        stream_id: StreamId(10),
                        extent_id: ExtentId(50),
                        epoch: Epoch(0),
                        offset: Offset(i as u64),
                        byte_pos,
                    },
                    Some(Bytes::from(format!("msg{i}"))),
                ),
                None,
            )
            .await
            .unwrap();
        assert_eq!(resp.opcode(), Opcode::Watermark);
    }

    let seal_resp = store
        .handle_frame(
            Frame::new(
                VariableHeader::SealExtentNodePrepare {
                    request_id: 20,
                    stream_id: StreamId(10),
                    epoch: Epoch(0),
                    extent_id_from: ExtentId(50),
                    start_offset: 0,
                },
                None,
            ),
            None,
        )
        .await
        .unwrap();
    assert_eq!(seal_resp.opcode(), Opcode::SealExtentNode);
    match &seal_resp.variable_header {
        VariableHeader::SealExtentNodeResp { end_offset, .. } => {
            assert_eq!(*end_offset, 2);
        }
        _ => panic!("expected SealExtentNodeResp"),
    }

    for i in 2u32..4 {
        let byte_pos = i as u64 * 8;
        let resp = store
            .handle_frame(
                Frame::new(
                    VariableHeader::Forward {
                        stream_id: StreamId(10),
                        extent_id: ExtentId(50),
                        epoch: Epoch(0),
                        offset: Offset(i as u64),
                        byte_pos,
                    },
                    Some(Bytes::from(format!("msg{i}"))),
                ),
                None,
            )
            .await;
        assert!(
            resp.is_none(),
            "late forward for offset {i} beyond sealed limit should return None"
        );
    }

    let resp = store
        .handle_frame(
            Frame::new(
                VariableHeader::Forward {
                    stream_id: StreamId(10),
                    extent_id: ExtentId(50),
                    epoch: Epoch(0),
                    offset: Offset(4),
                    byte_pos: 32,
                },
                Some(Bytes::from_static(b"should-fail")),
            ),
            None,
        )
        .await;
    assert!(
        resp.is_none(),
        "forward beyond sealed limit should return None"
    );
}

#[tokio::test]
async fn handle_seal_is_idempotent() {
    let store = ExtentNodeStore::new();
    let sid = register_stream(&store, 1, 1).await;

    for i in 0u32..3 {
        let resp = store
            .handle_frame(
                Frame::new(
                    VariableHeader::Append {
                        request_id: 10 + i,
                        stream_id: sid,
                        epoch: Epoch(0),
                    },
                    Some(Bytes::from(format!("msg{i}"))),
                ),
                None,
            )
            .await
            .unwrap();
        assert_eq!(resp.opcode(), Opcode::Append);
    }

    let seal1 = store
        .handle_frame(
            Frame::new(
                VariableHeader::SealExtentNodePrepare {
                    request_id: 20,
                    stream_id: sid,
                    epoch: Epoch(0),
                    extent_id_from: ExtentId(1),
                    start_offset: 0,
                },
                None,
            ),
            None,
        )
        .await
        .unwrap();
    assert_eq!(seal1.opcode(), Opcode::SealExtentNode);
    match &seal1.variable_header {
        VariableHeader::SealExtentNodeResp { end_offset, .. } => {
            assert_eq!(*end_offset, 3);
        }
        _ => panic!("expected SealExtentNodeResp"),
    }

    let seal2 = store
        .handle_frame(
            Frame::new(
                VariableHeader::SealExtentNodePrepare {
                    request_id: 21,
                    stream_id: sid,
                    epoch: Epoch(0),
                    extent_id_from: ExtentId(1),
                    start_offset: 0,
                },
                None,
            ),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        seal2.opcode(),
        Opcode::SealExtentNode,
        "second seal should return SealExtentNodeResp, not Error"
    );
    match &seal2.variable_header {
        VariableHeader::SealExtentNodeResp { end_offset, .. } => {
            assert_eq!(
                *end_offset, 3,
                "second seal should report same committed offset"
            );
        }
        _ => panic!("expected SealExtentNodeResp"),
    }
}

#[tokio::test]
async fn append_with_stale_epoch_returns_epoch_stale() {
    let store = ExtentNodeStore::new();
    let sid = register_stream(&store, 1, 1).await;

    {
        let guard = store.streams.pin();
        let stream = guard.get(&sid).unwrap();
        stream.set_epoch(Epoch(5));
    }

    let resp = store
        .handle_frame(
            Frame::new(
                VariableHeader::Append {
                    request_id: 10,
                    stream_id: sid,
                    epoch: Epoch(1),
                },
                Some(Bytes::from_static(b"stale")),
            ),
            None,
        )
        .await
        .unwrap();
    assert_eq!(resp.opcode(), Opcode::Append);
    assert!(resp.is_error_response());
    assert_eq!(resp.error_code(), ErrorCode::EpochStale as u16);
}

#[tokio::test]
async fn append_with_epoch_zero_bypasses_epoch_check() {
    let store = ExtentNodeStore::new();
    let sid = register_stream_at_epoch(&store, 1, 1, Epoch(5)).await;

    let resp = store
        .handle_frame(
            Frame::new(
                VariableHeader::Append {
                    request_id: 10,
                    stream_id: sid,
                    epoch: Epoch(0),
                },
                Some(Bytes::from_static(b"wildcard")),
            ),
            None,
        )
        .await
        .unwrap();
    assert_eq!(resp.opcode(), Opcode::Append);
    assert!(!resp.is_error_response());
}

#[tokio::test]
async fn append_with_matching_epoch_succeeds() {
    let store = ExtentNodeStore::new();
    let sid = register_stream_at_epoch(&store, 1, 1, Epoch(3)).await;

    let resp = store
        .handle_frame(
            Frame::new(
                VariableHeader::Append {
                    request_id: 10,
                    stream_id: sid,
                    epoch: Epoch(3),
                },
                Some(Bytes::from_static(b"correct")),
            ),
            None,
        )
        .await
        .unwrap();
    assert_eq!(resp.opcode(), Opcode::Append);
    assert!(!resp.is_error_response());
}

// ── Helper: append N records to a stream ─────────────────────────────────

/// Append `n` records to the given stream and return the last offset.
async fn append_n(store: &ExtentNodeStore, sid: StreamId, n: u32) -> Offset {
    let mut last = Offset(0);
    for i in 0..n {
        let resp = store
            .handle_frame(
                Frame::new(
                    VariableHeader::Append {
                        request_id: 100 + i,
                        stream_id: sid,
                        epoch: Epoch(0),
                    },
                    Some(Bytes::from(format!("rec{i}"))),
                ),
                None,
            )
            .await
            .unwrap();
        assert_eq!(resp.opcode(), Opcode::Append);
        last = resp.offset();
    }
    last
}

/// Seal an extent via SealExtentNodePrepare (the production RPC path).
/// Returns the end_offset from the SealExtentNodeResp.
async fn seal_via_rpc(store: &ExtentNodeStore, sid: StreamId, extent_id: ExtentId) -> u64 {
    let resp = store
        .handle_frame(
            Frame::new(
                VariableHeader::SealExtentNodePrepare {
                    request_id: 200,
                    stream_id: sid,
                    epoch: Epoch(0),
                    extent_id_from: extent_id,
                    start_offset: 0,
                },
                None,
            ),
            None,
        )
        .await
        .unwrap();
    assert_eq!(resp.opcode(), Opcode::SealExtentNode);
    match &resp.variable_header {
        VariableHeader::SealExtentNodeResp { end_offset, .. } => *end_offset,
        other => panic!("expected SealExtentNodeResp, got {:?}", other),
    }
}

// ── B. handle_flush_extent tests ─────────────────────────────────────────

#[tokio::test]
async fn flush_extent_seals_active_extent() {
    // B6: FlushExtent on an Active extent should seal it and enqueue a FlushRequest.
    let (flush_tx, mut flush_rx) = mpsc::channel::<crate::s3_flusher::FlushRequest>(16);
    let mut store = ExtentNodeStore::new();
    store.set_flush_tx(flush_tx);

    let sid = register_stream(&store, 1, 1).await;
    append_n(&store, sid, 3).await;

    // Extent is Active — send FlushExtent with end_offset=3.
    let result = store
        .handle_frame(
            Frame::new(
                VariableHeader::FlushExtent {
                    request_id: 50,
                    stream_id: sid,
                    extent_id: ExtentId(1),
                    epoch: Epoch(0),
                    start_offset: 0,
                    end_offset: 3,
                },
                None,
            ),
            None,
        )
        .await;
    let resp = result.expect("FlushExtent should return a response");
    assert_eq!(resp.opcode(), Opcode::FlushExtent);
    assert!(!resp.is_error_response());

    // Verify extent is now sealed.
    {
        let guard = store.streams.pin();
        let stream = guard.get(&sid).unwrap();
        let sealed = stream
            .with_extent(ExtentId(1), |ext| ext.is_sealed())
            .unwrap();
        assert!(sealed, "extent should be sealed after FlushExtent");
    }

    // Verify FlushRequest was received.
    let req = flush_rx.try_recv().expect("expected FlushRequest");
    assert_eq!(req.stream_id, sid);
    assert_eq!(req.extent_id, ExtentId(1));
    assert_eq!(req.end_offset, 3);
}

#[tokio::test]
async fn flush_extent_corrects_sealed_extent() {
    // B7: FlushExtent on a Sealed extent with a lower end_offset (SM quorum < local)
    // should correct the seal offset downward and enqueue a FlushRequest.
    let (flush_tx, mut flush_rx) = mpsc::channel::<crate::s3_flusher::FlushRequest>(16);
    let mut store = ExtentNodeStore::new();
    store.set_flush_tx(flush_tx);

    let sid = register_stream(&store, 1, 1).await;
    append_n(&store, sid, 5).await;
    let end = seal_via_rpc(&store, sid, ExtentId(1)).await;
    assert_eq!(end, 5);

    // Drain the FlushRequest enqueued by handle_seal (Primary, RF=1 auto-flush).
    let seal_req = flush_rx
        .try_recv()
        .expect("seal should have enqueued FlushRequest");
    assert_eq!(seal_req.end_offset, 5);

    // Simulate s3_flusher completing: clear the flush-in-progress marker.
    {
        let guard = store.streams.pin();
        let stream = guard.get(&sid).unwrap();
        stream.finish_flush(ExtentId(1));
    }

    // SM says quorum committed offset is 3 (lower than local 5).
    let result = store
        .handle_frame(
            Frame::new(
                VariableHeader::FlushExtent {
                    request_id: 51,
                    stream_id: sid,
                    extent_id: ExtentId(1),
                    epoch: Epoch(0),
                    start_offset: 0,
                    end_offset: 3,
                },
                None,
            ),
            None,
        )
        .await;
    let resp = result.expect("FlushExtent should return a response");
    assert_eq!(resp.opcode(), Opcode::FlushExtent);
    assert!(!resp.is_error_response());

    // Verify extent is still sealed and the FlushRequest carries the corrected offset.
    {
        let guard = store.streams.pin();
        let stream = guard.get(&sid).unwrap();
        let sealed = stream
            .with_extent(ExtentId(1), |ext| ext.is_sealed())
            .unwrap();
        assert!(sealed, "extent should still be sealed after FlushExtent");
    }

    let req = flush_rx
        .try_recv()
        .expect("FlushExtent should enqueue FlushRequest");
    assert_eq!(req.stream_id, sid);
    assert_eq!(
        req.end_offset, 3,
        "FlushRequest should carry SM's corrected offset"
    );
}

#[tokio::test]
async fn flush_extent_skips_flushed() {
    // B8: FlushExtent on an already-flushed extent → no FlushRequest.
    let (flush_tx, mut flush_rx) = mpsc::channel::<crate::s3_flusher::FlushRequest>(16);
    let mut store = ExtentNodeStore::new();
    store.set_flush_tx(flush_tx);

    let sid = register_stream(&store, 1, 1).await;
    append_n(&store, sid, 2).await;
    seal_via_rpc(&store, sid, ExtentId(1)).await;

    // Drain the FlushRequest enqueued by handle_seal (Primary, RF=1 auto-flush).
    let _seal_req = flush_rx
        .try_recv()
        .expect("seal should have enqueued FlushRequest");

    // Simulate s3_flusher completing: clear the flush-in-progress marker.
    {
        let guard = store.streams.pin();
        let stream = guard.get(&sid).unwrap();
        stream.finish_flush(ExtentId(1));
    }

    // Mark as flushed.
    {
        let guard = store.streams.pin();
        let stream = guard.get(&sid).unwrap();
        stream.with_extent(ExtentId(1), |ext| ext.mark_flushed());
    }

    let result = store
        .handle_frame(
            Frame::new(
                VariableHeader::FlushExtent {
                    request_id: 52,
                    stream_id: sid,
                    extent_id: ExtentId(1),
                    epoch: Epoch(0),
                    start_offset: 0,
                    end_offset: 2,
                },
                None,
            ),
            None,
        )
        .await;
    let resp = result.expect("FlushExtent should return a response");
    assert_eq!(resp.opcode(), Opcode::FlushExtent);
    assert!(!resp.is_error_response());
    assert!(
        flush_rx.try_recv().is_err(),
        "no FlushRequest should be sent for already-flushed extent"
    );
}

#[tokio::test]
async fn flush_extent_skips_missing_extent() {
    // B9: FlushExtent for a non-existent extent → no FlushRequest, no panic.
    let (flush_tx, mut flush_rx) = mpsc::channel::<crate::s3_flusher::FlushRequest>(16);
    let mut store = ExtentNodeStore::new();
    store.set_flush_tx(flush_tx);

    let _sid = register_stream(&store, 1, 1).await;

    let result = store
        .handle_frame(
            Frame::new(
                VariableHeader::FlushExtent {
                    request_id: 53,
                    stream_id: StreamId(1),
                    extent_id: ExtentId(99),
                    epoch: Epoch(0),
                    start_offset: 0,
                    end_offset: 10,
                },
                None,
            ),
            None,
        )
        .await;
    let resp = result.expect("FlushExtent should return a response");
    assert_eq!(resp.opcode(), Opcode::FlushExtent);
    assert!(!resp.is_error_response());
    assert!(
        flush_rx.try_recv().is_err(),
        "no FlushRequest for missing extent"
    );
}

#[tokio::test]
async fn flush_extent_skips_missing_stream() {
    // B10: FlushExtent for a non-existent stream → no FlushRequest, no panic.
    let (flush_tx, mut flush_rx) = mpsc::channel::<crate::s3_flusher::FlushRequest>(16);
    let mut store = ExtentNodeStore::new();
    store.set_flush_tx(flush_tx);

    let result = store
        .handle_frame(
            Frame::new(
                VariableHeader::FlushExtent {
                    request_id: 54,
                    stream_id: StreamId(999),
                    extent_id: ExtentId(1),
                    epoch: Epoch(0),
                    start_offset: 0,
                    end_offset: 5,
                },
                None,
            ),
            None,
        )
        .await;
    let resp = result.expect("FlushExtent should return a response");
    assert_eq!(resp.opcode(), Opcode::FlushExtent);
    assert!(!resp.is_error_response());
    assert!(
        flush_rx.try_recv().is_err(),
        "no FlushRequest for missing stream"
    );
}

#[tokio::test]
async fn flush_extent_dedup() {
    // B11: Duplicate FlushExtent for the same (stream, extent) should be deduplicated.
    let (flush_tx, mut flush_rx) = mpsc::channel::<crate::s3_flusher::FlushRequest>(16);
    let mut store = ExtentNodeStore::new();
    store.set_flush_tx(flush_tx);

    let sid = register_stream(&store, 1, 1).await;
    append_n(&store, sid, 3).await;
    seal_via_rpc(&store, sid, ExtentId(1)).await;

    // Drain the FlushRequest enqueued by handle_seal (Primary, RF=1 auto-flush).
    let _seal_req = flush_rx
        .try_recv()
        .expect("seal should have enqueued FlushRequest");

    // Simulate s3_flusher completing: clear the flush-in-progress marker.
    {
        let guard = store.streams.pin();
        let stream = guard.get(&sid).unwrap();
        stream.finish_flush(ExtentId(1));
    }

    let flush_frame = Frame::new(
        VariableHeader::FlushExtent {
            request_id: 55,
            stream_id: sid,
            extent_id: ExtentId(1),
            epoch: Epoch(0),
            start_offset: 0,
            end_offset: 3,
        },
        None,
    );

    // First FlushExtent → should enqueue.
    let resp1 = store.handle_frame(flush_frame.clone(), None).await;
    assert!(resp1.is_some());
    let req = flush_rx
        .try_recv()
        .expect("first FlushExtent should enqueue");
    assert_eq!(req.stream_id, sid);

    // Second FlushExtent → deduplicated, no new FlushRequest.
    let resp2 = store.handle_frame(flush_frame, None).await;
    assert!(resp2.is_some());
    assert!(
        flush_rx.try_recv().is_err(),
        "second FlushExtent should be deduplicated"
    );
}

#[tokio::test]
async fn flush_extent_no_s3_configured() {
    // B12: FlushExtent on a store without flush_tx (S3 not configured) → no panic.
    let store = ExtentNodeStore::new();
    let sid = register_stream(&store, 1, 1).await;
    append_n(&store, sid, 2).await;
    seal_via_rpc(&store, sid, ExtentId(1)).await;

    // flush_tx is None — should return error response without panicking.
    let result = store
        .handle_frame(
            Frame::new(
                VariableHeader::FlushExtent {
                    request_id: 56,
                    stream_id: sid,
                    extent_id: ExtentId(1),
                    epoch: Epoch(0),
                    start_offset: 0,
                    end_offset: 2,
                },
                None,
            ),
            None,
        )
        .await;
    let resp = result.expect("FlushExtent should return a response");
    assert_eq!(resp.opcode(), Opcode::FlushExtent);
    assert!(resp.is_error_response());
}

// ── C. handle_seal_commit tests ──────────────────────────────────────────

#[tokio::test]
async fn seal_commit_corrects_higher_offset() {
    // C13: SealExtentNodeCommit with a lower end_offset than local seal
    // should correct the seal point downward.
    let store = ExtentNodeStore::new();
    let sid = register_stream(&store, 1, 1).await;
    append_n(&store, sid, 5).await;
    let end = seal_via_rpc(&store, sid, ExtentId(1)).await;
    assert_eq!(end, 5);

    // SM says committed offset is 3 (quorum < local).
    let result = store
        .handle_frame(
            Frame::new(
                VariableHeader::SealExtentNodeCommit {
                    request_id: 60,
                    stream_id: sid,
                    extent_id: ExtentId(1),
                    epoch: Epoch(0),
                    start_offset: 0,
                    end_offset: 3,
                },
                None,
            ),
            None,
        )
        .await;
    let resp = result.expect("SealExtentNodeCommit should return a response");
    assert_eq!(resp.opcode(), Opcode::SealExtentNode);
    assert!(!resp.is_error_response());

    // Verify extent is still sealed. The limit was corrected to 3 internally
    // (correct_seal_offset), but message_count() returns committed_offset - start_offset
    // which reflects actually-written data (5), not the seal limit.
    // The correction ensures the S3 flusher encodes only 3 records.
    {
        let guard = store.streams.pin();
        let stream = guard.get(&sid).unwrap();
        let sealed = stream
            .with_extent(ExtentId(1), |ext| ext.is_sealed())
            .unwrap();
        assert!(sealed, "extent should still be sealed after SealCommit");
        // committed_offset is unchanged (5 records were written).
        let count = stream
            .with_extent(ExtentId(1), |ext| ext.message_count())
            .unwrap();
        assert_eq!(
            count, 5,
            "committed data is unchanged; limit correction is internal"
        );
    }
}

#[tokio::test]
async fn seal_commit_seals_active_extent() {
    // C14: SealExtentNodeCommit on an Active extent should seal it.
    let store = ExtentNodeStore::new();
    let sid = register_stream(&store, 1, 1).await;
    append_n(&store, sid, 3).await;

    let result = store
        .handle_frame(
            Frame::new(
                VariableHeader::SealExtentNodeCommit {
                    request_id: 61,
                    stream_id: sid,
                    extent_id: ExtentId(1),
                    epoch: Epoch(0),
                    start_offset: 0,
                    end_offset: 3,
                },
                None,
            ),
            None,
        )
        .await;
    let resp = result.expect("SealExtentNodeCommit should return a response");
    assert_eq!(resp.opcode(), Opcode::SealExtentNode);
    assert!(!resp.is_error_response());

    // Verify extent is now sealed.
    {
        let guard = store.streams.pin();
        let stream = guard.get(&sid).unwrap();
        let sealed = stream
            .with_extent(ExtentId(1), |ext| ext.is_sealed())
            .unwrap();
        assert!(sealed, "extent should be sealed after SealExtentNodeCommit");
        let count = stream
            .with_extent(ExtentId(1), |ext| ext.message_count())
            .unwrap();
        assert_eq!(count, 3);
    }
}

#[tokio::test]
async fn seal_commit_noop_lower_offset() {
    // C15: SealExtentNodeCommit with end_offset > local seal → no-op
    // (correct_seal_offset only corrects downward).
    let store = ExtentNodeStore::new();
    let sid = register_stream(&store, 1, 1).await;
    append_n(&store, sid, 3).await;
    let end = seal_via_rpc(&store, sid, ExtentId(1)).await;
    assert_eq!(end, 3);

    // SM says end_offset=5 — higher than local 3 → no change.
    let result = store
        .handle_frame(
            Frame::new(
                VariableHeader::SealExtentNodeCommit {
                    request_id: 62,
                    stream_id: sid,
                    extent_id: ExtentId(1),
                    epoch: Epoch(0),
                    start_offset: 0,
                    end_offset: 5,
                },
                None,
            ),
            None,
        )
        .await;
    let resp = result.expect("SealExtentNodeCommit should return a response");
    assert_eq!(resp.opcode(), Opcode::SealExtentNode);
    assert!(!resp.is_error_response());

    // Verify limit stays at 3 (not 5).
    {
        let guard = store.streams.pin();
        let stream = guard.get(&sid).unwrap();
        let count = stream
            .with_extent(ExtentId(1), |ext| ext.message_count())
            .unwrap();
        assert_eq!(
            count, 3,
            "seal commit with higher offset should not change limit"
        );
    }
}

#[tokio::test]
async fn seal_commit_unknown_stream() {
    // C16: SealExtentNodeCommit for a non-existent stream → no panic.
    let store = ExtentNodeStore::new();

    let result = store
        .handle_frame(
            Frame::new(
                VariableHeader::SealExtentNodeCommit {
                    request_id: 63,
                    stream_id: StreamId(999),
                    extent_id: ExtentId(1),
                    epoch: Epoch(0),
                    start_offset: 0,
                    end_offset: 5,
                },
                None,
            ),
            None,
        )
        .await;
    let resp = result.expect("SealExtentNodeCommit should return a response");
    assert_eq!(resp.opcode(), Opcode::SealExtentNode);
    assert!(
        !resp.is_error_response(),
        "should return success for unknown stream"
    );
}

/// Concurrent ForwardInitExtent and Forward for the same new extent must not
/// create duplicate extents. The atomic register_extent_if_absent guarantees a
/// single extent exists at the wire start_offset.
#[tokio::test]
async fn forward_init_and_forward_concurrent_create_single_extent() {
    use common::types::StorageClass;

    let store = ExtentNodeStore::new();
    let stream_id = StreamId(10);
    let extent_id = ExtentId(50);

    let init = Frame::new(
        VariableHeader::ForwardInitExtent {
            stream_id,
            extent_id,
            epoch: Epoch(1),
            start_offset: Offset(1_000),
            extent_capacity: 8 * 1024 * 1024,
            cache_extents: 4,
            min_extent_capacity: 8 * 1024 * 1024,
            max_extent_capacity: 256 * 1024 * 1024,
            extent_growth_factor: 2,
            storage_class: StorageClass::S3,
        },
        None,
    );
    let forward = Frame::new(
        VariableHeader::Forward {
            stream_id,
            extent_id,
            epoch: Epoch(1),
            offset: Offset(1_000),
            byte_pos: 0,
        },
        Some(Bytes::from_static(b"msg0")),
    );

    // Run both deliveries concurrently; ordering is nondeterministic.
    let (init_res, fwd_res) = tokio::join!(
        async {
            store.handle_forward_init_extent(init);
        },
        async { store.handle_forward(forward).is_some() },
    );
    let _ = (init_res, fwd_res);

    let streams = store.streams.pin();
    let stream = streams.get(&stream_id).unwrap();
    // Exactly one extent with this id exists (no duplicate from the race).
    let reported = stream.report_extents(Epoch(1));
    assert_eq!(
        reported.len(),
        1,
        "expected exactly one extent after concurrent init+forward"
    );
    assert_eq!(reported[0].0, extent_id);
    assert_eq!(
        stream.with_extent(extent_id, |ext| ext.start_offset),
        Some(Offset(1_000))
    );
}
