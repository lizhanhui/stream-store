use super::*;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::config::{
    DEFAULT_CACHE_EXTENTS, DEFAULT_EXTENT_CAPACITY, DEFAULT_EXTENT_GROWTH_FACTOR,
    DEFAULT_MAX_EXTENT_CAPACITY, DEFAULT_MIN_EXTENT_CAPACITY,
};
use common::types::{Epoch, ErrorCode, ExtentId, Offset, Opcode, StreamId};
use rpc::frame::{Frame, VariableHeader};
use server::handler::RequestHandler;
use tokio::sync::mpsc;

use crate::ack_queue::{AckQueue, DEFAULT_REPLICATION_TIMEOUT, PendingAck};

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
                    stream_id: sid,
                    extent_id: ExtentId(1),
                    role: 0,
                    replication_factor: 1,
                    epoch: Epoch(0),
                    extent_capacity: DEFAULT_EXTENT_CAPACITY,
                    min_extent_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
                    max_extent_capacity: DEFAULT_MAX_EXTENT_CAPACITY,
                    extent_growth_factor: DEFAULT_EXTENT_GROWTH_FACTOR,
                    cache_extents: DEFAULT_CACHE_EXTENTS,
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
                    stream_id: StreamId(42),
                    extent_id: ExtentId(100),
                    role: 0,
                    replication_factor: 2,
                    epoch: Epoch(0),
                    extent_capacity: DEFAULT_EXTENT_CAPACITY as u32,
                    min_extent_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
                    max_extent_capacity: DEFAULT_MAX_EXTENT_CAPACITY,
                    extent_growth_factor: DEFAULT_EXTENT_GROWTH_FACTOR,
                    cache_extents: DEFAULT_CACHE_EXTENTS,
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
        let aq_guard = store.ack_queues.pin();
        let aq = aq_guard.get(&StreamId(42)).unwrap().lock_inner();
        assert_eq!(aq.required_acks, 1);
    }
}

#[tokio::test]
async fn register_extent_secondary() {
    use rpc::payload::build_register_extent_payload;

    let store = ExtentNodeStore::new();

    // RegisterExtent as Secondary (RF=2, no replica addrs).
    let payload = build_register_extent_payload(&[]);
    let resp = store
        .handle_frame(
            Frame::new(
                VariableHeader::RegisterExtent {
                    request_id: 1,
                    stream_id: StreamId(42),
                    extent_id: ExtentId(100),
                    role: 1,
                    replication_factor: 2,
                    epoch: Epoch(0),
                    extent_capacity: DEFAULT_EXTENT_CAPACITY as u32,
                    min_extent_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
                    max_extent_capacity: DEFAULT_MAX_EXTENT_CAPACITY,
                    extent_growth_factor: DEFAULT_EXTENT_GROWTH_FACTOR,
                    cache_extents: DEFAULT_CACHE_EXTENTS,
                },
                Some(payload),
            ),
            None,
        )
        .await
        .unwrap();

    assert_eq!(resp.opcode(), Opcode::RegisterExtent);

    let ri = store.get_replica_info(StreamId(42)).unwrap();
    assert!(!ri.is_primary());
    assert_eq!(ri.role, 1);
    assert!(ri.replica_addrs.is_empty());
    assert_eq!(ri.replication_factor, 2);

    // Secondary should NOT have an AckQueue.
    assert!(!store.ack_queues.pin().contains_key(&StreamId(42)));
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
                    stream_id: StreamId(10),
                    extent_id: ExtentId(50),
                    role: 0,
                    replication_factor: 1,
                    epoch: Epoch(0),
                    extent_capacity: DEFAULT_EXTENT_CAPACITY as u32,
                    min_extent_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
                    max_extent_capacity: DEFAULT_MAX_EXTENT_CAPACITY,
                    extent_growth_factor: DEFAULT_EXTENT_GROWTH_FACTOR,
                    cache_extents: DEFAULT_CACHE_EXTENTS,
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
                    stream_id: StreamId(10),
                    extent_id: ExtentId(50),
                    role: 0,
                    replication_factor: 3,
                    epoch: Epoch(0),
                    extent_capacity: DEFAULT_EXTENT_CAPACITY as u32,
                    min_extent_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
                    max_extent_capacity: DEFAULT_MAX_EXTENT_CAPACITY,
                    extent_growth_factor: DEFAULT_EXTENT_GROWTH_FACTOR,
                    cache_extents: DEFAULT_CACHE_EXTENTS,
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

    // Accept connections and read Forward frames from mock secondaries.
    let (conn1, _) = listener1.accept().await.unwrap();
    let mut reader1 = FramedRead::new(conn1, FrameCodec);
    let fwd1 = reader1.next().await.unwrap().unwrap();
    assert_eq!(fwd1.opcode(), Opcode::Forward);
    assert_eq!(fwd1.stream_id(), StreamId(10));
    assert_eq!(fwd1.offset(), Offset(0));

    let (conn2, _) = listener2.accept().await.unwrap();
    let mut reader2 = FramedRead::new(conn2, FrameCodec);
    let fwd2 = reader2.next().await.unwrap().unwrap();
    assert_eq!(fwd2.opcode(), Opcode::Forward);
    assert_eq!(fwd2.stream_id(), StreamId(10));
    assert_eq!(fwd2.offset(), Offset(0));

    let ack_queues = store.ack_queues.pin();

    // PendingAck should be in the ack_queue.
    {
        let mut inner = ack_queues.get(&StreamId(10)).unwrap().lock_inner();
        inner.receive_pending();
        assert_eq!(inner.pending.len(), 1);
        assert_eq!(inner.pending[0].assigned_offset, 0);
        // RF=3 requires 1 secondary ACK.
        assert_eq!(inner.required_acks, 1);
    }

    // Simulate watermark from first secondary (quorum met with 1 ACK for RF=3).
    {
        let mut inner = ack_queues.get(&StreamId(10)).unwrap().lock_inner();
        inner.ack_from_secondary(0, 0);
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
    use rpc::payload::build_register_extent_payload;

    let store = ExtentNodeStore::new();

    // Register as Secondary (RF=2).
    let payload = build_register_extent_payload(&[]);
    store
        .handle_frame(
            Frame::new(
                VariableHeader::RegisterExtent {
                    request_id: 1,
                    stream_id: StreamId(10),
                    extent_id: ExtentId(50),
                    role: 1,
                    replication_factor: 2,
                    epoch: Epoch(0),
                    extent_capacity: DEFAULT_EXTENT_CAPACITY as u32,
                    min_extent_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
                    max_extent_capacity: DEFAULT_MAX_EXTENT_CAPACITY,
                    extent_growth_factor: DEFAULT_EXTENT_GROWTH_FACTOR,
                    cache_extents: DEFAULT_CACHE_EXTENTS,
                },
                Some(payload),
            ),
            None,
        )
        .await
        .unwrap();

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
    inner.ack_from_secondary(0, 2);
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
    inner.ack_from_secondary(0, 5);
    assert!(inner.quorum_offset().is_none());

    // Second secondary reports — now we have quorum.
    inner.ack_from_secondary(1, 3);
    // quorum_offset = min of top-2 = 3
    assert_eq!(inner.quorum_offset(), Some(3));

    // Third secondary reports higher.
    inner.ack_from_secondary(2, 10);
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
        let sid = register_stream(&store, i + 1, i as u32).await;
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
        active_count, NUM_STREAMS as u32,
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
    use rpc::payload::build_register_extent_payload;

    let store = ExtentNodeStore::new();

    let payload = build_register_extent_payload(&[]);
    store
        .handle_frame(
            Frame::new(
                VariableHeader::RegisterExtent {
                    request_id: 1,
                    stream_id: StreamId(10),
                    extent_id: ExtentId(50),
                    role: 1,
                    replication_factor: 2,
                    epoch: Epoch(0),
                    extent_capacity: DEFAULT_EXTENT_CAPACITY as u32,
                    min_extent_capacity: DEFAULT_MIN_EXTENT_CAPACITY,
                    max_extent_capacity: DEFAULT_MAX_EXTENT_CAPACITY,
                    extent_growth_factor: DEFAULT_EXTENT_GROWTH_FACTOR,
                    cache_extents: DEFAULT_CACHE_EXTENTS,
                },
                Some(payload),
            ),
            None,
        )
        .await
        .unwrap();

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
                VariableHeader::SealExtentNodeRequest {
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
                VariableHeader::SealExtentNodeRequest {
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
                VariableHeader::SealExtentNodeRequest {
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
    let sid = register_stream(&store, 1, 1).await;

    {
        let guard = store.streams.pin();
        let stream = guard.get(&sid).unwrap();
        stream.set_epoch(Epoch(3));
    }

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
