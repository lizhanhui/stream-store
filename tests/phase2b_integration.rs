//! Phase 2b integration tests: broadcast replication end-to-end.
//!
//! Tests the full flow: Primary ExtentNode broadcasts appends to all Secondary ExtentNodes
//! in parallel, with quorum-based deferred ACKs and cumulative watermark
//! propagation.

use std::sync::Arc;

use bytes::Bytes;
use common::types::{
    Epoch, ExtentId, ExtentPolicy, Offset, Opcode, StorageClass, StreamConfig, StreamId,
};
use rpc::frame::{Frame, VariableHeader};
use rpc::payload::build_register_extent_payload;

use extent_node::downstream::DownstreamPool;
use extent_node::store::ExtentNodeStore;

/// Initialize tracing for tests. Uses try_init() so it's safe when multiple tests
/// run in the same process. Override log level via RUST_LOG env var.
fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .try_init();
}

/// Start a broadcast-replication-enabled ExtentNode server.
/// Returns (addr, store handle).
async fn start_extent_node() -> (String, Arc<ExtentNodeStore>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    // Create store with DownstreamPool (direct TCP, no channels).
    let store = Arc::new(ExtentNodeStore::new());
    let downstream = Arc::new(DownstreamPool::new(Arc::clone(&store)));
    store.set_downstream(downstream);

    // Accept connections with deferred response support.
    let store_for_accept = Arc::clone(&store);
    tokio::spawn(async move {
        server::Server::builder("ExtentNode-test")
            .listener(listener)
            .handler(store_for_accept)
            .deferred(true)
            .build()
            .run()
            .await;
    });

    (addr, store)
}

/// Helper: send a RegisterExtent to an ExtentNode via raw connection.
///
/// For broadcast replication:
/// - Primary receives `replica_addrs` = all secondary addresses.
/// - Secondaries receive `replica_addrs` = empty.
async fn register_extent(
    addr: &str,
    stream_id: u32,
    extent_id: u32,
    role: u8,
    replication_factor: u8,
    replica_addrs: &[&str],
) {
    use futures_util::{SinkExt, StreamExt};
    use rpc::codec::FrameCodec;
    use tokio_util::codec::Framed;

    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut framed = Framed::new(stream, FrameCodec);

    let payload = build_register_extent_payload(replica_addrs);
    framed
        .send(Frame::new(
            VariableHeader::RegisterExtent {
                request_id: 0,
                extent_id: ExtentId(extent_id),
                role,
                start_offset: Offset(0),
                config: StreamConfig {
                    stream_id: StreamId(stream_id),
                    replication_factor,
                    epoch: Epoch(0),
                    storage_class: StorageClass::S3,
                    policy: ExtentPolicy {
                        cache: 4,
                        min_capacity: 8 * 1024 * 1024,
                        max_capacity: 256 * 1024 * 1024,
                        scale_factor: 2,
                    },
                },
            },
            Some(payload),
        ))
        .await
        .unwrap();

    let resp = framed.next().await.unwrap().unwrap();
    assert_eq!(resp.opcode(), Opcode::RegisterExtent);
}

/// Test RF=2: Primary broadcasts to 1 Secondary.
///
/// 1. Start two ExtentNode servers (Primary, Secondary).
/// 2. Register broadcast replication: Primary knows about Secondary.
/// 3. Client appends to Primary.
/// 4. Primary defers ACK, broadcasts to Secondary.
/// 5. Secondary writes locally and returns Watermark.
/// 6. Primary receives Watermark, quorum met (1 of 1), sends deferred ACK to client.
/// 7. Verify data is readable from both Primary and Secondary.
#[tokio::test]
async fn broadcast_replication_rf2() {
    init_tracing();
    // Start both ExtentNodes.
    let (primary_addr, _primary_store) = start_extent_node().await;
    let (secondary_addr, _secondary_store) = start_extent_node().await;

    let stream_id = 100;
    let extent_id = 1u32;

    // Register broadcast replication.
    // Secondary must be registered first so it's ready to accept forwarded appends.
    register_extent(&secondary_addr, stream_id, extent_id, 1, 2, &[]).await;
    register_extent(
        &primary_addr,
        stream_id,
        extent_id,
        0,
        2,
        &[&secondary_addr],
    )
    .await;

    // Give a moment for connections to settle.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Client connects to Primary and appends.
    let client = client::StreamClient::connect(&primary_addr).await.unwrap();

    // Append 5 messages — each should be replicated and ACKed.
    for i in 0u64..5 {
        let r = client
            .append(
                StreamId(stream_id),
                Epoch(0), // epoch
                Bytes::from(format!("msg-{i}")),
            )
            .await
            .unwrap();
        assert_eq!(r.offset, Offset(i), "message {i} should get offset {i}");
    }

    // Query offset on Primary.
    let max = client.query_offset(StreamId(stream_id)).await.unwrap();
    assert_eq!(max, Offset(5));

    // Read messages from Primary (byte_pos removed, offset-only API).
    let msgs = client
        .read(StreamId(stream_id), ExtentId(1), Offset(0), 10)
        .await
        .unwrap();
    assert_eq!(msgs.len(), 5);
    for (i, msg) in msgs.iter().enumerate().take(5) {
        assert_eq!(*msg, Bytes::from(format!("msg-{i}")));
    }

    // Also verify data is on the Secondary.
    let secondary_client = client::StreamClient::connect(&secondary_addr)
        .await
        .unwrap();
    let secondary_max = secondary_client
        .query_offset(StreamId(stream_id))
        .await
        .unwrap();
    assert_eq!(secondary_max, Offset(5));

    let secondary_msgs = secondary_client
        .read(StreamId(stream_id), ExtentId(1), Offset(0), 10)
        .await
        .unwrap();
    assert_eq!(secondary_msgs.len(), 5);
    for (i, msg) in secondary_msgs.iter().enumerate().take(5) {
        assert_eq!(*msg, Bytes::from(format!("msg-{i}")));
    }
}

/// Test RF=3: Primary broadcasts to 2 Secondaries in parallel.
///
/// Quorum = RF/2 = 1, so only 1 of 2 secondaries needs to ACK before
/// the client gets its response. But data is written to all 3 nodes.
#[tokio::test]
async fn broadcast_replication_rf3() {
    init_tracing();
    let (primary_addr, _) = start_extent_node().await;
    let (secondary1_addr, _) = start_extent_node().await;
    let (secondary2_addr, _) = start_extent_node().await;

    let stream_id = 200;
    let extent_id = 1u32;

    // Register broadcast replication.
    // Secondaries first so they're ready.
    register_extent(&secondary1_addr, stream_id, extent_id, 1, 3, &[]).await;
    register_extent(&secondary2_addr, stream_id, extent_id, 2, 3, &[]).await;
    // Primary knows about both secondaries.
    register_extent(
        &primary_addr,
        stream_id,
        extent_id,
        0,
        3,
        &[&secondary1_addr, &secondary2_addr],
    )
    .await;

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let client = client::StreamClient::connect(&primary_addr).await.unwrap();

    // Append 3 messages.
    for i in 0u64..3 {
        let r = client
            .append(
                StreamId(stream_id),
                Epoch(0), // epoch
                Bytes::from(format!("rf3-msg-{i}")),
            )
            .await
            .unwrap();
        assert_eq!(r.offset, Offset(i));
    }

    // Verify data on all 3 nodes.
    for (label, addr) in [
        ("primary", &primary_addr),
        ("secondary1", &secondary1_addr),
        ("secondary2", &secondary2_addr),
    ] {
        let c = client::StreamClient::connect(addr).await.unwrap();
        let max = c.query_offset(StreamId(stream_id)).await.unwrap();
        assert_eq!(max, Offset(3), "{label} should have offset 3");

        let msgs = c
            .read(StreamId(stream_id), ExtentId(1), Offset(0), 10)
            .await
            .unwrap();
        assert_eq!(msgs.len(), 3, "{label} should have 3 messages");
        for (i, msg) in msgs.iter().enumerate().take(3) {
            assert_eq!(
                *msg,
                Bytes::from(format!("rf3-msg-{i}")),
                "{label} message {i} mismatch"
            );
        }
    }
}

/// Test that multiple streams can share the same ExtentNode nodes.
///
/// Two different streams, each with their own broadcast configuration, both
/// routing through the same secondary ExtentNode. Verifies data isolation between streams.
#[tokio::test]
async fn multi_stream_shared_downstream() {
    init_tracing();
    let (primary_addr, _) = start_extent_node().await;
    let (secondary_addr, _) = start_extent_node().await;

    let stream_a = 300;
    let stream_b = 301;

    // Both streams use the same secondary.
    register_extent(&secondary_addr, stream_a, 1, 1, 2, &[]).await;
    register_extent(&primary_addr, stream_a, 1, 0, 2, &[&secondary_addr]).await;

    register_extent(&secondary_addr, stream_b, 1, 1, 2, &[]).await;
    register_extent(&primary_addr, stream_b, 1, 0, 2, &[&secondary_addr]).await;

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let client = client::StreamClient::connect(&primary_addr).await.unwrap();

    // Append to stream A.
    for i in 0u64..3 {
        client
            .append(
                StreamId(stream_a),
                Epoch(0), // epoch
                Bytes::from(format!("a-msg-{i}")),
            )
            .await
            .unwrap();
    }

    // Append to stream B.
    for i in 0u64..2 {
        client
            .append(
                StreamId(stream_b),
                Epoch(0), // epoch
                Bytes::from(format!("b-msg-{i}")),
            )
            .await
            .unwrap();
    }

    // Verify stream A.
    let msgs_a = client
        .read(StreamId(stream_a), ExtentId(1), Offset(0), 10)
        .await
        .unwrap();
    assert_eq!(msgs_a.len(), 3);

    // Verify stream B.
    let msgs_b = client
        .read(StreamId(stream_b), ExtentId(1), Offset(0), 10)
        .await
        .unwrap();
    assert_eq!(msgs_b.len(), 2);

    // Verify on secondary too.
    let sec_client = client::StreamClient::connect(&secondary_addr)
        .await
        .unwrap();
    let sec_a = sec_client
        .read(StreamId(stream_a), ExtentId(1), Offset(0), 10)
        .await
        .unwrap();
    assert_eq!(sec_a.len(), 3);

    let sec_b = sec_client
        .read(StreamId(stream_b), ExtentId(1), Offset(0), 10)
        .await
        .unwrap();
    assert_eq!(sec_b.len(), 2);
}

/// Test Primary-only (RF=1): Primary is standalone, should ACK immediately.
/// No secondaries needed.
#[tokio::test]
async fn broadcast_replication_rf1_immediate_ack() {
    init_tracing();
    let (primary_addr, _) = start_extent_node().await;

    let stream_id = 400;
    let extent_id = 1u32;

    // Register as Primary with RF=1, no secondaries.
    register_extent(&primary_addr, stream_id, extent_id, 0, 1, &[]).await;

    let client = client::StreamClient::connect(&primary_addr).await.unwrap();

    // Append should ACK immediately.
    for i in 0u64..5 {
        let r = client
            .append(
                StreamId(stream_id),
                Epoch(0), // epoch
                Bytes::from(format!("rf1-msg-{i}")),
            )
            .await
            .unwrap();
        assert_eq!(r.offset, Offset(i));
    }

    let max = client.query_offset(StreamId(stream_id)).await.unwrap();
    assert_eq!(max, Offset(5));
}
