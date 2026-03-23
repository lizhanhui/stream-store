use bytes::Bytes;
use common::types::Offset;

/// Initialize tracing for tests. Uses try_init() so it's safe when multiple tests
/// run in the same process. Override log level via RUST_LOG env var.
fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "warn".into()),
        )
        .try_init();
}

/// Start an ExtentNode server on a random port and return the address.
async fn start_extent_node_server() -> String {
    // Bind to port 0 to let the OS assign a free port.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let store = std::sync::Arc::new(
        extent_node::store::ExtentNodeStore::new(),
    );

    tokio::spawn(async move {
        server::Server::builder("ExtentNode-test")
            .listener(listener)
            .handler(store)
            .build()
            .run()
            .await;
    });

    addr
}

#[tokio::test]
async fn create_append_query_read() {
    init_tracing();
    let addr = start_extent_node_server().await;

    let mut c = client::StorageClient::connect(&addr).await.unwrap();

    // Create a stream.
    let sid = c.create_stream().await.unwrap();

    // Append 5 messages, collecting byte positions for later reads.
    let mut results = Vec::new();
    for i in 0u64..5 {
        let r = c
            .append(sid, Bytes::from(format!("message-{i}")))
            .await
            .unwrap();
        assert_eq!(r.offset, Offset(i));
        results.push(r);
    }

    // Query max offset.
    let max = c.query_offset(sid).await.unwrap();
    assert_eq!(max, Offset(5));

    // Read all 5 messages from byte_pos=0.
    let msgs = c.read(sid, Offset(0), 0, 5).await.unwrap();
    assert_eq!(msgs.len(), 5);
    for i in 0..5 {
        assert_eq!(msgs[i], Bytes::from(format!("message-{i}")));
    }

    // Read 2 messages starting from record 2's byte_pos (random access).
    let msgs = c.read(sid, Offset(2), results[2].byte_pos, 2).await.unwrap();
    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0], Bytes::from("message-2"));
    assert_eq!(msgs[1], Bytes::from("message-3"));
}

#[tokio::test]
async fn read_from_unknown_stream() {
    init_tracing();
    let addr = start_extent_node_server().await;

    let mut c = client::StorageClient::connect(&addr).await.unwrap();

    let result = c
        .read(common::types::StreamId(999), Offset(0), 0, 10)
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn append_to_unknown_stream() {
    init_tracing();
    let addr = start_extent_node_server().await;

    let mut c = client::StorageClient::connect(&addr).await.unwrap();

    let result = c
        .append(common::types::StreamId(999), Bytes::from_static(b"fail"))
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn multiple_streams_independent() {
    init_tracing();
    let addr = start_extent_node_server().await;

    let mut c = client::StorageClient::connect(&addr).await.unwrap();

    let s1 = c.create_stream().await.unwrap();
    let s2 = c.create_stream().await.unwrap();

    c.append(s1, Bytes::from_static(b"s1-msg0")).await.unwrap();
    c.append(s1, Bytes::from_static(b"s1-msg1")).await.unwrap();
    c.append(s2, Bytes::from_static(b"s2-msg0")).await.unwrap();

    assert_eq!(c.query_offset(s1).await.unwrap(), Offset(2));
    assert_eq!(c.query_offset(s2).await.unwrap(), Offset(1));

    let msgs = c.read(s1, Offset(0), 0, 10).await.unwrap();
    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0], Bytes::from_static(b"s1-msg0"));

    let msgs = c.read(s2, Offset(0), 0, 10).await.unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0], Bytes::from_static(b"s2-msg0"));
}

#[tokio::test]
async fn extent_node_seal_and_continue_append() {
    init_tracing();
    let addr = start_extent_node_server().await;

    let mut c = client::StorageClient::connect(&addr).await.unwrap();

    let sid = c.create_stream().await.unwrap();

    // Append 3 messages.
    for i in 0u64..3 {
        let r = c
            .append(sid, Bytes::from(format!("msg-{i}")))
            .await
            .unwrap();
        assert_eq!(r.offset, Offset(i));
    }
    assert_eq!(c.query_offset(sid).await.unwrap(), Offset(3));

    // Seal the active extent on ExtentNode directly.
    let seal_frame = rpc::frame::Frame {
        opcode: common::types::Opcode::Seal,
        flags: 0,
        request_id: 100,
        stream_id: sid,
        extent_id: common::types::ExtentId(0),
        offset: Offset(0),
        payload: Bytes::new(),
    };
    // We need to send seal via raw framed connection since StorageClient.seal goes to StreamManager.
    // Instead, let's use a second raw connection.
    use futures_util::{SinkExt, StreamExt};
    use rpc::codec::FrameCodec;
    use tokio_util::codec::Framed;

    let stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let mut framed = Framed::new(stream, FrameCodec);
    framed.send(seal_frame).await.unwrap();
    let resp = framed.next().await.unwrap().unwrap();
    assert_eq!(resp.opcode, common::types::Opcode::SealAck);
    // offset field carries the message_count of sealed extent
    assert_eq!(resp.offset, Offset(3));

    // Now append more messages to the new extent (after seal-and-new).
    let r = c
        .append(sid, Bytes::from_static(b"after-seal"))
        .await
        .unwrap();
    assert_eq!(r.offset, Offset(3));

    // Read the after-seal message from the new extent (byte_pos=0 since it's a new extent).
    let msgs = c.read(sid, r.offset, r.byte_pos, 1).await.unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0], Bytes::from_static(b"after-seal"));
}
