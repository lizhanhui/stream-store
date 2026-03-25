//! Wire-format payload encoding/decoding helpers used by ExtentNode, StreamManager, and clients.

use bytes::{BufMut, Bytes, BytesMut};
use common::types::{ExtentInfo, ExtentState, NodeMetrics, ReplicaDetail};

/// Build a Connect payload: [node_id_len:u16][node_id][addr_len:u16][addr][interval_ms:u32]
pub fn build_connect_payload(node_id: &str, addr: &str, interval_ms: u32) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_u16(node_id.len() as u16);
    buf.extend_from_slice(node_id.as_bytes());
    buf.put_u16(addr.len() as u16);
    buf.extend_from_slice(addr.as_bytes());
    buf.put_u32(interval_ms);
    buf.freeze()
}

/// Parse a Connect payload: [node_id_len:u16][node_id][addr_len:u16][addr][interval_ms:u32]
pub fn parse_connect_payload(payload: &[u8]) -> Option<(String, String, u32)> {
    let mut pos = 0;

    if payload.len() < 2 {
        return None;
    }
    let id_len = u16::from_be_bytes([payload[pos], payload[pos + 1]]) as usize;
    pos += 2;
    if payload.len() < pos + id_len {
        return None;
    }
    let node_id = String::from_utf8_lossy(&payload[pos..pos + id_len]).to_string();
    pos += id_len;

    if payload.len() < pos + 2 {
        return None;
    }
    let addr_len = u16::from_be_bytes([payload[pos], payload[pos + 1]]) as usize;
    pos += 2;
    if payload.len() < pos + addr_len {
        return None;
    }
    let addr = String::from_utf8_lossy(&payload[pos..pos + addr_len]).to_string();
    pos += addr_len;

    if payload.len() < pos + 4 {
        return None;
    }
    let interval_ms = u32::from_be_bytes([
        payload[pos],
        payload[pos + 1],
        payload[pos + 2],
        payload[pos + 3],
    ]);

    Some((node_id, addr, interval_ms))
}

/// Build a Disconnect payload: [node_id_len:u16][node_id]
///
/// Sent by ExtentNode to StreamManager during graceful shutdown.
/// StreamManager's `handle_disconnect` parses this with `parse_string_payload`.
pub fn build_disconnect_payload(node_id: &str) -> Bytes {
    build_string_payload(node_id)
}

/// Build a simple string payload: [len:u16][string]
pub fn build_string_payload(s: &str) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_u16(s.len() as u16);
    buf.extend_from_slice(s.as_bytes());
    buf.freeze()
}

/// Parse a simple string payload: [len:u16][string]
pub fn parse_string_payload(payload: &[u8]) -> Option<String> {
    if payload.len() < 2 {
        return None;
    }
    let len = u16::from_be_bytes([payload[0], payload[1]]) as usize;
    if payload.len() < 2 + len {
        return None;
    }
    Some(String::from_utf8_lossy(&payload[2..2 + len]).to_string())
}

/// Build a Heartbeat payload carrying node_id and runtime metrics.
///
/// Wire format:
/// `[node_id_len:u16][node_id][available_memory_bytes:u64][total_memory_bytes:u64][appends_per_sec:u32][active_extent_count:u32][bytes_written_per_sec:u64]`
pub fn build_heartbeat_payload(node_id: &str, metrics: &NodeMetrics) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_u16(node_id.len() as u16);
    buf.extend_from_slice(node_id.as_bytes());
    buf.put_u64(metrics.available_memory_bytes);
    buf.put_u64(metrics.total_memory_bytes);
    buf.put_u32(metrics.appends_per_sec);
    buf.put_u32(metrics.active_extent_count);
    buf.put_u64(metrics.bytes_written_per_sec);
    buf.freeze()
}

/// Parse a Heartbeat payload into (node_id, NodeMetrics).
///
/// Wire format:
/// `[node_id_len:u16][node_id][available_memory_bytes:u64][total_memory_bytes:u64][appends_per_sec:u32][active_extent_count:u32][bytes_written_per_sec:u64]`
pub fn parse_heartbeat_payload(payload: &[u8]) -> Option<(String, NodeMetrics)> {
    if payload.len() < 2 {
        return None;
    }
    let mut pos = 0;
    let id_len = u16::from_be_bytes([payload[pos], payload[pos + 1]]) as usize;
    pos += 2;
    // Need id_len bytes for node_id + 32 bytes for metrics
    if payload.len() < pos + id_len + 32 {
        return None;
    }
    let node_id = String::from_utf8_lossy(&payload[pos..pos + id_len]).to_string();
    pos += id_len;

    let available_memory_bytes = u64::from_be_bytes(payload[pos..pos + 8].try_into().ok()?);
    pos += 8;
    let total_memory_bytes = u64::from_be_bytes(payload[pos..pos + 8].try_into().ok()?);
    pos += 8;
    let appends_per_sec = u32::from_be_bytes(payload[pos..pos + 4].try_into().ok()?);
    pos += 4;
    let active_extent_count = u32::from_be_bytes(payload[pos..pos + 4].try_into().ok()?);
    pos += 4;
    let bytes_written_per_sec = u64::from_be_bytes(payload[pos..pos + 8].try_into().ok()?);

    Some((
        node_id,
        NodeMetrics {
            available_memory_bytes,
            total_memory_bytes,
            appends_per_sec,
            active_extent_count,
            bytes_written_per_sec,
        },
    ))
}

/// Replication role constants.
pub const ROLE_PRIMARY: u8 = 0;

/// Build a RegisterExtent payload for broadcast replication.
///
/// Build a RegisterExtent payload containing the replica addresses.
///
/// Wire format (payload only — stream_id, extent_id, role, replication_factor
/// are now in the variable header):
/// `[num_addrs:u16][addr1_len:u16][addr1]...[addrN_len:u16][addrN]`
///
/// `replica_addrs`: all secondary addresses (for Primary); empty for Secondaries.
pub fn build_register_extent_payload(
    replica_addrs: &[&str],
) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_u16(replica_addrs.len() as u16);
    for addr in replica_addrs {
        buf.put_u16(addr.len() as u16);
        buf.extend_from_slice(addr.as_bytes());
    }
    buf.freeze()
}

/// Parse a RegisterExtent payload (replica addresses only).
///
/// Wire format: `[num_addrs:u16][addr1_len:u16][addr1]...`
///
/// Returns the list of replica addresses.
pub fn parse_register_extent_payload(payload: &[u8]) -> Option<Vec<String>> {
    if payload.len() < 2 {
        return None;
    }
    let num_addrs = u16::from_be_bytes([payload[0], payload[1]]) as usize;

    let mut pos = 2;
    let mut replica_addrs = Vec::with_capacity(num_addrs);
    for _ in 0..num_addrs {
        if payload.len() < pos + 2 {
            return None;
        }
        let addr_len = u16::from_be_bytes([payload[pos], payload[pos + 1]]) as usize;
        pos += 2;
        if payload.len() < pos + addr_len {
            return None;
        }
        let addr = String::from_utf8_lossy(&payload[pos..pos + addr_len]).to_string();
        pos += addr_len;
        replica_addrs.push(addr);
    }

    Some(replica_addrs)
}

/// Build a CreateStream payload: [name_len:u16][stream_name][replication_factor:u16]
pub fn build_create_stream_payload(name: &str, replication_factor: u16) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_u16(name.len() as u16);
    buf.extend_from_slice(name.as_bytes());
    buf.put_u16(replication_factor);
    buf.freeze()
}

/// Parse a CreateStream payload: [name_len:u16][stream_name][replication_factor:u16]
///
/// Returns `(stream_name, replication_factor)`.
pub fn parse_create_stream_payload(payload: &[u8]) -> Option<(String, u16)> {
    if payload.len() < 2 {
        return None;
    }
    let name_len = u16::from_be_bytes([payload[0], payload[1]]) as usize;
    let pos = 2;
    if payload.len() < pos + name_len + 2 {
        return None;
    }
    let name = String::from_utf8_lossy(&payload[pos..pos + name_len]).to_string();
    let rf_pos = pos + name_len;
    let replication_factor = u16::from_be_bytes([payload[rf_pos], payload[rf_pos + 1]]);
    Some((name, replication_factor))
}

/// Encode a Vec<ExtentInfo> into a response payload.
///
/// Wire format:
/// ```text
/// [num_extents:u32]
///   per extent:
///     [extent_id:u32][start_offset:u64][end_offset:u64][state:u8]
///     [num_replicas:u16]
///       per replica:
///         [addr_len:u16][addr_bytes][role:u8][is_alive:u8]
/// ```
pub fn encode_extent_info_vec(extents: &[ExtentInfo]) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_u32(extents.len() as u32);
    for ext in extents {
        buf.put_u32(ext.extent_id);
        buf.put_u64(ext.start_offset);
        buf.put_u64(ext.end_offset);
        buf.put_u8(ext.state.as_u8());
        buf.put_u16(ext.replicas.len() as u16);
        for r in &ext.replicas {
            buf.put_u16(r.node_addr.len() as u16);
            buf.extend_from_slice(r.node_addr.as_bytes());
            buf.put_u8(r.role);
            buf.put_u8(if r.is_alive { 1 } else { 0 });
        }
    }
    buf.freeze()
}

/// Decode a Vec<ExtentInfo> from a response payload.
pub fn parse_extent_info_vec(payload: &[u8]) -> Option<Vec<ExtentInfo>> {
    if payload.len() < 4 {
        return None;
    }
    let mut pos = 0;
    let num_extents = u32::from_be_bytes(payload[pos..pos + 4].try_into().ok()?) as usize;
    pos += 4;

    let mut extents = Vec::with_capacity(num_extents);
    for _ in 0..num_extents {
        // Need at least 4+8+8+1+2 = 23 bytes for extent header
        if payload.len() < pos + 23 {
            return None;
        }
        let extent_id = u32::from_be_bytes(payload[pos..pos + 4].try_into().ok()?);
        pos += 4;
        let start_offset = u64::from_be_bytes(payload[pos..pos + 8].try_into().ok()?);
        pos += 8;
        let end_offset = u64::from_be_bytes(payload[pos..pos + 8].try_into().ok()?);
        pos += 8;
        let state = ExtentState::from_u8(payload[pos]).unwrap_or(ExtentState::Unspecified);
        pos += 1;
        let num_replicas = u16::from_be_bytes([payload[pos], payload[pos + 1]]) as usize;
        pos += 2;

        let mut replicas = Vec::with_capacity(num_replicas);
        for _ in 0..num_replicas {
            if payload.len() < pos + 2 {
                return None;
            }
            let addr_len = u16::from_be_bytes([payload[pos], payload[pos + 1]]) as usize;
            pos += 2;
            if payload.len() < pos + addr_len + 2 {
                return None;
            }
            let node_addr = String::from_utf8_lossy(&payload[pos..pos + addr_len]).to_string();
            pos += addr_len;
            let role = payload[pos];
            pos += 1;
            let is_alive = payload[pos] == 1;
            pos += 1;
            replicas.push(ReplicaDetail {
                node_addr,
                role,
                is_alive,
            });
        }

        extents.push(ExtentInfo {
            extent_id,
            start_offset,
            end_offset,
            state,
            replicas,
        });
    }
    Some(extents)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_payload_roundtrip() {
        let payload = build_connect_payload("extent-node-1", "127.0.0.1:9801", 5000);
        let (node_id, addr, interval) = parse_connect_payload(&payload).unwrap();
        assert_eq!(node_id, "extent-node-1");
        assert_eq!(addr, "127.0.0.1:9801");
        assert_eq!(interval, 5000);
    }

    #[test]
    fn string_payload_roundtrip() {
        let payload = build_string_payload("my-stream");
        let s = parse_string_payload(&payload).unwrap();
        assert_eq!(s, "my-stream");
    }

    #[test]
    fn connect_payload_too_short() {
        assert!(parse_connect_payload(&[]).is_none());
        assert!(parse_connect_payload(&[0x00]).is_none());
        assert!(parse_connect_payload(&[0x00, 0x05, 0x41, 0x42]).is_none());
    }

    #[test]
    fn string_payload_too_short() {
        assert!(parse_string_payload(&[]).is_none());
        assert!(parse_string_payload(&[0x00]).is_none());
        assert!(parse_string_payload(&[0x00, 0x03, 0x41]).is_none());
    }

    #[test]
    fn register_extent_payload_primary_with_secondaries() {
        // Primary with 2 secondary addresses (RF=3).
        let payload =
            build_register_extent_payload(&["127.0.0.1:9802", "127.0.0.1:9803"]);
        let parsed = parse_register_extent_payload(&payload).unwrap();
        assert_eq!(
            parsed,
            vec!["127.0.0.1:9802", "127.0.0.1:9803"]
        );
    }

    #[test]
    fn register_extent_payload_secondary() {
        // Secondary receives no replica addresses.
        let payload = build_register_extent_payload(&[]);
        let parsed = parse_register_extent_payload(&payload).unwrap();
        assert!(parsed.is_empty());
    }

    #[test]
    fn register_extent_payload_rf1() {
        // RF=1: Primary only, no secondaries.
        let payload = build_register_extent_payload(&[]);
        let parsed = parse_register_extent_payload(&payload).unwrap();
        assert!(parsed.is_empty());
    }

    #[test]
    fn register_extent_payload_too_short() {
        assert!(parse_register_extent_payload(&[]).is_none());
        // claims num_addrs=1 with addr_len=5 but no addr data
        let mut buf = BytesMut::new();
        buf.put_u16(1); // num_addrs = 1
        buf.put_u16(5); // addr_len = 5 but nothing follows
        assert!(parse_register_extent_payload(&buf).is_none());
    }

    #[test]
    fn heartbeat_payload_roundtrip() {
        let metrics = NodeMetrics {
            available_memory_bytes: 8_000_000_000,
            total_memory_bytes: 16_000_000_000,
            appends_per_sec: 12345,
            active_extent_count: 42,
            bytes_written_per_sec: 50_000_000,
        };
        let payload = build_heartbeat_payload("extent-node-1", &metrics);
        let (node_id, parsed) = parse_heartbeat_payload(&payload).unwrap();
        assert_eq!(node_id, "extent-node-1");
        assert_eq!(parsed, metrics);
    }

    #[test]
    fn heartbeat_payload_too_short() {
        assert!(parse_heartbeat_payload(&[]).is_none());
        assert!(parse_heartbeat_payload(&[0x00]).is_none());
        // node_id_len=5 but only 3 bytes of node_id + no metrics
        assert!(parse_heartbeat_payload(&[0x00, 0x05, 0x41, 0x42, 0x43]).is_none());
        // Valid node_id but truncated metrics (only 16 bytes instead of 32)
        let metrics = NodeMetrics::default();
        let full = build_heartbeat_payload("n1", &metrics);
        // Truncate: keep node_id header (2 + 2 = 4 bytes) + only 16 of 32 metric bytes
        assert!(parse_heartbeat_payload(&full[..4 + 16]).is_none());
    }

    #[test]
    fn create_stream_payload_roundtrip() {
        let payload = build_create_stream_payload("my-stream", 3);
        let (name, replication_factor) = parse_create_stream_payload(&payload).unwrap();
        assert_eq!(name, "my-stream");
        assert_eq!(replication_factor, 3);
    }

    #[test]
    fn create_stream_payload_too_short() {
        assert!(parse_create_stream_payload(&[]).is_none());
        assert!(parse_create_stream_payload(&[0x00]).is_none());
        // name_len=3, "abc" but missing replication_factor bytes
        assert!(parse_create_stream_payload(&[0x00, 0x03, 0x61, 0x62, 0x63]).is_none());
        // name_len=3, "abc" + only 1 byte for replication_factor
        assert!(parse_create_stream_payload(&[0x00, 0x03, 0x61, 0x62, 0x63, 0x00]).is_none());
    }

    #[test]
    fn extent_info_vec_roundtrip_empty() {
        let payload = encode_extent_info_vec(&[]);
        let parsed = parse_extent_info_vec(&payload).unwrap();
        assert!(parsed.is_empty());
    }

    #[test]
    fn extent_info_vec_roundtrip_multiple() {
        let extents = vec![
            ExtentInfo {
                extent_id: 3,
                start_offset: 200,
                end_offset: 300,
                state: ExtentState::Sealed,
                replicas: vec![
                    ReplicaDetail {
                        node_addr: "127.0.0.1:9801".to_string(),
                        role: 0,
                        is_alive: true,
                    },
                    ReplicaDetail {
                        node_addr: "127.0.0.1:9802".to_string(),
                        role: 1,
                        is_alive: false,
                    },
                ],
            },
            ExtentInfo {
                extent_id: 4,
                start_offset: 300,
                end_offset: 300,
                state: ExtentState::Active,
                replicas: vec![ReplicaDetail {
                    node_addr: "127.0.0.1:9803".to_string(),
                    role: 0,
                    is_alive: true,
                }],
            },
        ];

        let payload = encode_extent_info_vec(&extents);
        let parsed = parse_extent_info_vec(&payload).unwrap();
        assert_eq!(parsed, extents);
    }

    #[test]
    fn extent_info_vec_single_no_replicas() {
        let extents = vec![ExtentInfo {
            extent_id: 1,
            start_offset: 0,
            end_offset: 50,
            state: ExtentState::Flushed,
            replicas: vec![],
        }];

        let payload = encode_extent_info_vec(&extents);
        let parsed = parse_extent_info_vec(&payload).unwrap();
        assert_eq!(parsed, extents);
    }

    #[test]
    fn extent_info_vec_too_short() {
        assert!(parse_extent_info_vec(&[]).is_none());
        assert!(parse_extent_info_vec(&[0x00, 0x00]).is_none());
        // Claims 1 extent but no data follows
        assert!(parse_extent_info_vec(&[0x00, 0x00, 0x00, 0x01]).is_none());
    }
}
