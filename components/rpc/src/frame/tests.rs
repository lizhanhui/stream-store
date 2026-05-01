use super::*;
use bytes::{BufMut, BytesMut};
use common::errors::StorageError;
use common::types::{
    ArenaClass, Epoch, ErrorCode, ExtentId, ExtentPolicy, FLAG_DESCRIBE_STREAM_BY_NAME,
    FLAG_RESPONSE, FLAG_SEAL_COMMIT, HEADER_LEN, MAGIC, Offset, Opcode, PROTOCOL_VERSION,
    StorageClass, StreamConfig, StreamId,
};

fn sample_append_frame() -> Frame {
    Frame::new(
        VariableHeader::Append {
            request_id: 42,
            stream_id: StreamId(100),
            epoch: Epoch(0),
        },
        Some(Bytes::from_static(b"hello world")),
    )
}

#[test]
fn round_trip_encode_decode() {
    let frame = sample_append_frame();
    let mut buf = BytesMut::new();
    frame.encode(&mut buf);

    let decoded = Frame::decode(&mut buf).unwrap().unwrap();
    assert_eq!(frame.opcode(), decoded.opcode());
    assert_eq!(frame.request_id(), decoded.request_id());
    assert_eq!(frame.stream_id(), decoded.stream_id());
    assert_eq!(frame.epoch(), decoded.epoch());
    assert_eq!(frame.payload, decoded.payload);
    assert!(buf.is_empty());
}

#[test]
fn partial_frame_returns_none() {
    let frame = sample_append_frame();
    let mut buf = BytesMut::new();
    frame.encode(&mut buf);

    // Truncate to only fixed header (no body).
    buf.truncate(HEADER_LEN);
    let mut partial = buf.clone();
    let result = Frame::decode(&mut partial).unwrap();
    assert!(result.is_none());
}

#[test]
fn insufficient_header_returns_none() {
    let mut buf = BytesMut::from(&[0u8; 4][..]);
    let result = Frame::decode(&mut buf).unwrap();
    assert!(result.is_none());
}

#[test]
fn invalid_magic_returns_error() {
    let mut buf = BytesMut::new();
    buf.put_u8(0xDE); // bad magic
    buf.put_u8(PROTOCOL_VERSION);
    buf.put_u8(Opcode::Connect as u8);
    buf.put_u8(0);
    buf.put_u32(4); // remaining_length = 4 (request_id)
    buf.put_u32(0); // request_id

    let result = Frame::decode(&mut buf);
    assert!(matches!(
        result,
        Err(StorageError::InvalidFrame { message: _, .. })
    ));
}

#[test]
fn unknown_opcode_returns_error() {
    let mut buf = BytesMut::new();
    buf.put_u8(MAGIC);
    buf.put_u8(PROTOCOL_VERSION);
    buf.put_u8(0xFE); // invalid opcode
    buf.put_u8(0);
    buf.put_u32(0); // remaining_length = 0

    let result = Frame::decode(&mut buf);
    assert!(matches!(
        result,
        Err(StorageError::UnknownOpcode { opcode: 0xFE, .. })
    ));
}

#[test]
fn multiple_frames_in_buffer() {
    let f1 = sample_append_frame();
    let f2 = Frame::new(
        VariableHeader::QueryOffset {
            request_id: 99,
            stream_id: StreamId(200),
        },
        None,
    );

    let mut buf = BytesMut::new();
    f1.encode(&mut buf);
    f2.encode(&mut buf);

    let d1 = Frame::decode(&mut buf).unwrap().unwrap();
    let d2 = Frame::decode(&mut buf).unwrap().unwrap();
    assert_eq!(f1.opcode(), d1.opcode());
    assert_eq!(f1.request_id(), d1.request_id());
    assert_eq!(f1.payload, d1.payload);
    assert_eq!(f2.opcode(), d2.opcode());
    assert_eq!(f2.request_id(), d2.request_id());
    assert_eq!(f2.stream_id(), d2.stream_id());
    assert!(buf.is_empty());
}

#[test]
fn heartbeat_frame_with_payload() {
    let frame = Frame::new(
        VariableHeader::Heartbeat { request_id: 7 },
        Some(Bytes::from_static(b"metrics-data")),
    );

    let mut buf = BytesMut::new();
    frame.encode(&mut buf);

    let decoded = Frame::decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.opcode(), Opcode::Heartbeat);
    assert_eq!(decoded.request_id(), 7);
    assert_eq!(decoded.payload, Some(Bytes::from_static(b"metrics-data")));
}

#[test]
fn connect_ack_minimal() {
    let frame = Frame::new(VariableHeader::ConnectAck { request_id: 1 }, None);

    let mut buf = BytesMut::new();
    frame.encode(&mut buf);
    // 8 (fixed) + 4 (request_id) = 12 bytes total
    assert_eq!(buf.len(), 12);

    let decoded = Frame::decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.opcode(), Opcode::Connect);
    assert_eq!(decoded.request_id(), 1);
}

#[test]
fn watermark_no_request_id() {
    let frame = Frame::new(
        VariableHeader::Watermark {
            stream_id: StreamId(42),
            extent_id: ExtentId(7),
            epoch: Epoch(1),
            offset: Offset(100),
        },
        None,
    );

    let mut buf = BytesMut::new();
    frame.encode(&mut buf);
    // 8 (fixed) + 4 (stream_id) + 4 (extent_id) + 4 (epoch) + 8 (offset) = 28 bytes
    assert_eq!(buf.len(), 28);

    let decoded = Frame::decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.opcode(), Opcode::Watermark);
    assert_eq!(decoded.stream_id(), StreamId(42));
    assert_eq!(decoded.extent_id(), ExtentId(7));
    assert_eq!(decoded.offset(), Offset(100));
    assert_eq!(decoded.request_id(), 0); // not present on wire
}

#[test]
fn append_ack_round_trip() {
    let frame = Frame::new(
        VariableHeader::AppendAck {
            request_id: 10,
            stream_id: StreamId(1),
            epoch: Epoch(3),
            extent_id: ExtentId(2),
            offset: Offset(42),
        },
        None,
    );

    let mut buf = BytesMut::new();
    frame.encode(&mut buf);

    let decoded = Frame::decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.offset(), Offset(42));
    assert_eq!(decoded.stream_id(), StreamId(1));
    assert_eq!(decoded.epoch(), Epoch(3));
    assert_eq!(decoded.extent_id(), ExtentId(2));
}

#[test]
fn read_with_count() {
    let frame = Frame::new(
        VariableHeader::Read {
            request_id: 5,
            stream_id: StreamId(10),
            extent_id: ExtentId(2),
            offset: Offset(50),
            count: 20,
        },
        None,
    );

    let mut buf = BytesMut::new();
    frame.encode(&mut buf);

    let decoded = Frame::decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.count(), 20);
    assert_eq!(decoded.offset(), Offset(50));
}

#[test]
fn seal_stream_manager_request_round_trip() {
    let frame = Frame::new(
        VariableHeader::SealStreamRequest {
            request_id: 1,
            stream_id: StreamId(10),
            epoch: Epoch(5),
        },
        None,
    );

    let mut buf = BytesMut::new();
    frame.encode(&mut buf);
    // 8 (fixed) + 4 + 4 + 4 = 20
    assert_eq!(buf.len(), 20);

    let decoded = Frame::decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.opcode(), Opcode::SealStream);
    assert_eq!(decoded.flags(), 0);
    assert_eq!(decoded.request_id(), 1);
    assert_eq!(decoded.stream_id(), StreamId(10));
    assert_eq!(decoded.epoch(), Epoch(5));
    assert!(!decoded.is_error_response());
    assert!(buf.is_empty());
}

#[test]
fn seal_stream_manager_resp_round_trip() {
    let addr = Bytes::from_static(b"127.0.0.1:9001");
    let frame = Frame::new(
        VariableHeader::SealStreamResp {
            request_id: 2,
            stream_id: StreamId(10),
            offset: Offset(42),
            new_epoch: Epoch(6),
            primary_addr: addr.clone(),
        },
        None,
    );

    let mut buf = BytesMut::new();
    frame.encode(&mut buf);
    // 8 (fixed) + 4 + 4 + 8 + 4 + 2 + 14 = 44
    assert_eq!(buf.len(), 44);

    let decoded = Frame::decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.opcode(), Opcode::SealStream);
    assert_eq!(decoded.flags(), FLAG_RESPONSE);
    assert_eq!(decoded.request_id(), 2);
    assert_eq!(decoded.stream_id(), StreamId(10));
    assert_eq!(decoded.offset(), Offset(42));
    assert_eq!(decoded.epoch(), Epoch(6));
    assert!(!decoded.is_error_response());
    match &decoded.variable_header {
        VariableHeader::SealStreamResp {
            primary_addr,
            new_epoch,
            ..
        } => {
            assert_eq!(primary_addr, &addr);
            assert_eq!(*new_epoch, Epoch(6));
        }
        _ => panic!("expected SealStreamResp"),
    }
    assert!(buf.is_empty());
}

#[test]
fn seal_stream_manager_resp_error_round_trip() {
    let frame = Frame::seal_stream_manager_resp_error(
        3,
        StreamId(10),
        ErrorCode::ExtentSealed,
        "stream sealed",
    );

    let mut buf = BytesMut::new();
    frame.encode(&mut buf);

    let decoded = Frame::decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.opcode(), Opcode::SealStream);
    assert!(decoded.is_error_response());
    assert_eq!(decoded.request_id(), 3);
    assert_eq!(decoded.stream_id(), StreamId(10));
    assert_eq!(decoded.error_code(), ErrorCode::ExtentSealed as u16);
    assert_eq!(decoded.payload, Some(Bytes::from_static(b"stream sealed")));
    assert!(buf.is_empty());
}

#[test]
fn seal_epoch_request_round_trip() {
    let frame = Frame::new(
        VariableHeader::SealEpochPrepare {
            request_id: 4,
            stream_id: StreamId(20),
            epoch: Epoch(3),
            extent_id_from: ExtentId(7),
            start_offset: 100,
        },
        None,
    );

    let mut buf = BytesMut::new();
    frame.encode(&mut buf);
    // 8 (fixed) + 4 + 4 + 4 + 4 + 8 = 32
    assert_eq!(buf.len(), 32);

    let decoded = Frame::decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.opcode(), Opcode::SealEpoch);
    assert_eq!(decoded.flags(), 0);
    assert_eq!(decoded.request_id(), 4);
    assert_eq!(decoded.stream_id(), StreamId(20));
    assert_eq!(decoded.epoch(), Epoch(3));
    assert_eq!(decoded.extent_id(), ExtentId(7));
    assert!(!decoded.is_error_response());
    match &decoded.variable_header {
        VariableHeader::SealEpochPrepare {
            extent_id_from,
            start_offset,
            ..
        } => {
            assert_eq!(*extent_id_from, ExtentId(7));
            assert_eq!(*start_offset, 100);
        }
        _ => panic!("expected SealEpochPrepare"),
    }
    assert!(buf.is_empty());
}

#[test]
fn seal_epoch_resp_round_trip() {
    let frame = Frame::new(
        VariableHeader::SealEpochResp {
            request_id: 5,
            stream_id: StreamId(20),
            epoch: Epoch(4),
            extent_id: ExtentId(8),
            start_offset: 100,
            end_offset: 500,
        },
        Some(Bytes::from_static(b"extra-data")),
    );

    let mut buf = BytesMut::new();
    frame.encode(&mut buf);
    // 8 (fixed) + 4 + 4 + 4 + 4 + 8 + 8 = 40 (vh) + 4 + 10 = 54
    assert_eq!(buf.len(), 54);

    let decoded = Frame::decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.opcode(), Opcode::SealEpoch);
    assert_eq!(decoded.flags(), FLAG_RESPONSE);
    assert_eq!(decoded.request_id(), 5);
    assert_eq!(decoded.stream_id(), StreamId(20));
    assert_eq!(decoded.epoch(), Epoch(4));
    assert_eq!(decoded.extent_id(), ExtentId(8));
    assert!(!decoded.is_error_response());
    match &decoded.variable_header {
        VariableHeader::SealEpochResp {
            start_offset,
            end_offset,
            ..
        } => {
            assert_eq!(*start_offset, 100);
            assert_eq!(*end_offset, 500);
        }
        _ => panic!("expected SealEpochResp"),
    }
    assert_eq!(decoded.payload, Some(Bytes::from_static(b"extra-data")));
    assert!(buf.is_empty());
}

#[test]
fn seal_epoch_resp_error_round_trip() {
    let frame =
        Frame::seal_epoch_resp_error(6, StreamId(20), ErrorCode::ExtentSealed, "node error");

    let mut buf = BytesMut::new();
    frame.encode(&mut buf);

    let decoded = Frame::decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.opcode(), Opcode::SealEpoch);
    assert!(decoded.is_error_response());
    assert_eq!(decoded.request_id(), 6);
    assert_eq!(decoded.stream_id(), StreamId(20));
    assert_eq!(decoded.error_code(), ErrorCode::ExtentSealed as u16);
    assert_eq!(decoded.payload, Some(Bytes::from_static(b"node error")));
    assert!(buf.is_empty());
}

#[test]
fn append_ack_error_frame() {
    let frame = Frame::append_ack_error(
        42,
        StreamId(9),
        Epoch(3),
        ExtentId(7),
        ErrorCode::ExtentSealed,
        "extent sealed",
    );

    let mut buf = BytesMut::new();
    frame.encode(&mut buf);

    let decoded = Frame::decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.opcode(), Opcode::Append);
    assert!(decoded.is_error_response());
    assert_eq!(decoded.request_id(), 42);
    assert_eq!(decoded.stream_id(), StreamId(9));
    assert_eq!(decoded.epoch(), Epoch(3));
    assert_eq!(decoded.error_code(), ErrorCode::ExtentSealed as u16);
    assert_eq!(decoded.extent_id(), ExtentId(7));
    assert_eq!(decoded.payload, Some(Bytes::from_static(b"extent sealed")));
}

#[test]
fn read_resp_with_count() {
    let frame = Frame::new(
        VariableHeader::ReadResp {
            request_id: 3,
            stream_id: StreamId(1),
            offset: Offset(0),
            count: 5,
        },
        Some(Bytes::from_static(b"messages")),
    );

    let mut buf = BytesMut::new();
    frame.encode(&mut buf);

    let decoded = Frame::decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.count(), 5);
    assert_eq!(decoded.payload, Some(Bytes::from_static(b"messages")));
}

#[test]
fn describe_stream_with_count() {
    let frame = Frame::new(
        VariableHeader::DescribeStream {
            request_id: 1,
            stream_id: StreamId(10),
            count: 3,
            stream_name: None,
        },
        None,
    );

    let mut buf = BytesMut::new();
    frame.encode(&mut buf);

    let decoded = Frame::decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.count(), 3);
    assert_eq!(decoded.stream_id(), StreamId(10));
}

#[test]
fn create_stream_round_trip() {
    let frame = Frame::new(
        VariableHeader::CreateStream {
            request_id: 5,
            stream_name: Bytes::from_static(b"my-stream"),
            replication_factor: 3,
            storage_class: StorageClass::S3,
            policy: ExtentPolicy { cache: 4 },
        },
        None,
    );

    let mut buf = BytesMut::new();
    frame.encode(&mut buf);

    let decoded = Frame::decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.opcode(), Opcode::CreateStream);
    assert_eq!(decoded.request_id(), 5);
    match &decoded.variable_header {
        VariableHeader::CreateStream {
            stream_name,
            replication_factor,
            policy,
            ..
        } => {
            assert_eq!(stream_name, &Bytes::from_static(b"my-stream"));
            assert_eq!(*replication_factor, 3);
            assert_eq!(policy.cache, 4);
        }
        _ => panic!("expected CreateStream"),
    }
    assert!(decoded.payload.is_none());
    assert!(buf.is_empty());
}

#[test]
fn create_stream_resp_round_trip() {
    let frame = Frame::new(
        VariableHeader::CreateStreamResp {
            request_id: 5,
            stream_id: StreamId(42),
            extent_id: ExtentId(1),
            epoch: Epoch(0),
            primary_addr: Bytes::from_static(b"127.0.0.1:9000"),
        },
        None,
    );

    let mut buf = BytesMut::new();
    frame.encode(&mut buf);

    let decoded = Frame::decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.opcode(), Opcode::CreateStream);
    assert_eq!(decoded.request_id(), 5);
    assert_eq!(decoded.stream_id(), StreamId(42));
    assert_eq!(decoded.extent_id(), ExtentId(1));
    match &decoded.variable_header {
        VariableHeader::CreateStreamResp { primary_addr, .. } => {
            assert_eq!(primary_addr, &Bytes::from_static(b"127.0.0.1:9000"));
        }
        _ => panic!("expected CreateStreamResp"),
    }
    assert!(decoded.payload.is_none());
    assert!(buf.is_empty());
}

#[test]
fn describe_stream_by_name_round_trip() {
    let frame = Frame::new(
        VariableHeader::DescribeStream {
            request_id: 10,
            stream_id: StreamId(0),
            count: 1,
            stream_name: Some(Bytes::from_static(b"my-stream")),
        },
        None,
    );

    let mut buf = BytesMut::new();
    frame.encode(&mut buf);

    let decoded = Frame::decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.opcode(), Opcode::DescribeStream);
    assert_eq!(decoded.flags(), FLAG_DESCRIBE_STREAM_BY_NAME);
    assert_eq!(decoded.count(), 1);
    match &decoded.variable_header {
        VariableHeader::DescribeStream { stream_name, .. } => {
            assert_eq!(
                stream_name.as_ref().unwrap(),
                &Bytes::from_static(b"my-stream")
            );
        }
        _ => panic!("expected DescribeStream"),
    }
    assert!(buf.is_empty());
}

#[test]
fn seal_epoch_commit_round_trip() {
    let frame = Frame::new(
        VariableHeader::SealEpochCommit {
            request_id: 77,
            stream_id: StreamId(7),
            extent_id: ExtentId(3),
            epoch: Epoch(2),
            start_offset: 100,
            end_offset: 500,
        },
        None,
    );

    let mut buf = BytesMut::new();
    frame.encode(&mut buf);

    let decoded = Frame::decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.opcode(), Opcode::SealEpoch);
    assert_eq!(decoded.flags(), FLAG_SEAL_COMMIT);
    assert_eq!(decoded.request_id(), 77);
    match &decoded.variable_header {
        VariableHeader::SealEpochCommit {
            request_id,
            stream_id,
            extent_id,
            epoch,
            start_offset,
            end_offset,
        } => {
            assert_eq!(*request_id, 77);
            assert_eq!(*stream_id, StreamId(7));
            assert_eq!(*extent_id, ExtentId(3));
            assert_eq!(*epoch, Epoch(2));
            assert_eq!(*start_offset, 100);
            assert_eq!(*end_offset, 500);
        }
        _ => panic!("expected SealEpochCommit"),
    }
    assert!(decoded.payload.is_none());
    assert!(buf.is_empty());
}

#[test]
fn flush_extent_round_trip() {
    let frame = Frame::new(
        VariableHeader::FlushExtent {
            request_id: 88,
            stream_id: StreamId(12),
            extent_id: ExtentId(5),
            epoch: Epoch(3),
            start_offset: 0,
            end_offset: 1000,
        },
        None,
    );

    let mut buf = BytesMut::new();
    frame.encode(&mut buf);

    let decoded = Frame::decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.opcode(), Opcode::FlushExtent);
    assert_eq!(decoded.flags(), 0x00);
    assert_eq!(decoded.request_id(), 88);
    match &decoded.variable_header {
        VariableHeader::FlushExtent {
            request_id,
            stream_id,
            extent_id,
            epoch,
            start_offset,
            end_offset,
        } => {
            assert_eq!(*request_id, 88);
            assert_eq!(*stream_id, StreamId(12));
            assert_eq!(*extent_id, ExtentId(5));
            assert_eq!(*epoch, Epoch(3));
            assert_eq!(*start_offset, 0);
            assert_eq!(*end_offset, 1000);
        }
        _ => panic!("expected FlushExtent"),
    }
    assert!(decoded.payload.is_none());
    assert!(buf.is_empty());
}

#[test]
fn update_extent_flushed_round_trip() {
    let frame = Frame::new(
        VariableHeader::UpdateExtentFlushed {
            stream_id: StreamId(91),
            epoch: Epoch(5),
            extent_id: ExtentId(17),
            start_offset: Offset(1_234),
            end_offset: Offset(9_999),
        },
        None,
    );

    let mut buf = BytesMut::new();
    frame.encode(&mut buf);

    let decoded = Frame::decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.opcode(), Opcode::UpdateExtent);
    assert_eq!(decoded.flags(), 0x02); // FLAG_EXTENT_FLUSHED
    match &decoded.variable_header {
        VariableHeader::UpdateExtentFlushed {
            stream_id,
            epoch,
            extent_id,
            start_offset,
            end_offset,
        } => {
            assert_eq!(*stream_id, StreamId(91));
            assert_eq!(*epoch, Epoch(5));
            assert_eq!(*extent_id, ExtentId(17));
            assert_eq!(*start_offset, Offset(1_234));
            assert_eq!(*end_offset, Offset(9_999));
        }
        _ => panic!("expected UpdateExtentFlushed"),
    }
    assert!(decoded.payload.is_none());
    assert!(buf.is_empty());
}

#[test]
fn register_epoch_arena_class_round_trip() {
    let config = StreamConfig {
        stream_id: StreamId(77),
        replication_factor: 3,
        epoch: Epoch(2),
        storage_class: StorageClass::S3,
        arena_class: ArenaClass::Shared,
        policy: ExtentPolicy { cache: 4 },
    };
    let frame = Frame::new(
        VariableHeader::RegisterEpoch {
            request_id: 55,
            extent_id: ExtentId(9),
            role: 0,
            config,
        },
        None,
    );

    let mut buf = BytesMut::new();
    frame.encode(&mut buf);

    let decoded = Frame::decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.opcode(), Opcode::RegisterEpoch);
    match &decoded.variable_header {
        VariableHeader::RegisterEpoch {
            request_id,
            extent_id,
            role,
            config: decoded_config,
        } => {
            assert_eq!(*request_id, 55);
            assert_eq!(*extent_id, ExtentId(9));
            assert_eq!(*role, 0);
            assert_eq!(decoded_config.stream_id, StreamId(77));
            assert_eq!(decoded_config.replication_factor, 3);
            assert_eq!(decoded_config.epoch, Epoch(2));
            assert_eq!(decoded_config.arena_class, ArenaClass::Shared);
        }
        _ => panic!("expected RegisterEpoch"),
    }
    assert!(buf.is_empty());
}

#[test]
fn forward_init_epoch_arena_class_round_trip() {
    let frame = Frame::new(
        VariableHeader::ForwardInitEpoch {
            stream_id: StreamId(42),
            extent_id: ExtentId(3),
            epoch: Epoch(7),
            start_offset: Offset(0),
            extent_capacity: 1024,
            cache_extents: 2,
            storage_class: StorageClass::S3,
            arena_class: ArenaClass::Shared,
        },
        None,
    );

    let mut buf = BytesMut::new();
    frame.encode(&mut buf);

    let decoded = Frame::decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.opcode(), Opcode::Forward);
    match &decoded.variable_header {
        VariableHeader::ForwardInitEpoch {
            stream_id,
            extent_id,
            epoch,
            start_offset,
            extent_capacity,
            cache_extents,
            storage_class,
            arena_class,
        } => {
            assert_eq!(*stream_id, StreamId(42));
            assert_eq!(*extent_id, ExtentId(3));
            assert_eq!(*epoch, Epoch(7));
            assert_eq!(*start_offset, Offset(0));
            assert_eq!(*extent_capacity, 1024);
            assert_eq!(*cache_extents, 2);
            assert_eq!(*storage_class, StorageClass::S3);
            assert_eq!(*arena_class, ArenaClass::Shared);
        }
        _ => panic!("expected ForwardInitEpoch"),
    }
    assert!(buf.is_empty());
}
