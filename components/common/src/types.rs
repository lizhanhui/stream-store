/// Magic byte identifying our wire protocol.
pub const MAGIC: u8 = 0xEF;

/// Current protocol version.
pub const PROTOCOL_VERSION: u8 = 1;

/// Fixed header length in bytes (Magic 1 + Version 1 + Opcode 1 + Flags 1
/// + RequestId 4 + StreamId 8 + Offset 8 + ExtentId 4 + PayloadLen 4 = 32).
pub const HEADER_LEN: usize = 32;

/// Flag bit indicating a forwarded append (broadcast replication).
/// When set on an Append frame, the receiving ExtentNode acts as a Secondary
/// (writes locally and returns a Watermark).
pub const FLAG_FORWARDED: u8 = 0x01;

/// Unique identifier for a stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StreamId(pub u64);

/// Unique identifier for an extent within a stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExtentId(pub u32);

/// Logical offset within a stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Offset(pub u64);

/// Unique identifier for an ExtentNode node (typically its listen address).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeId(pub String);

/// Wire protocol operation codes, grouped by category with gaps for future growth.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Opcode {
    // -- Data path (0x01-0x0F): Client <-> ExtentNode --
    Append = 0x01,
    AppendAck = 0x02,
    Read = 0x03,
    ReadResp = 0x04,
    Seal = 0x05,
    SealAck = 0x06,
    CreateStream = 0x07,
    QueryOffset = 0x08,
    QueryOffsetResp = 0x09,

    // -- Lifecycle (0x10-0x1F): ExtentNode <-> StreamManager --
    Connect = 0x10,
    ConnectAck = 0x11,
    Disconnect = 0x12,
    DisconnectAck = 0x13,
    Heartbeat = 0x14,
    RegisterExtent = 0x15,
    RegisterExtentAck = 0x16,
    Watermark = 0x17,

    // -- Cluster management (0x20-0x2F): StreamManager -> ExtentNode/Client --
    StreamManagerMembershipChange = 0x20,

    // -- Management (0x30-0x3F): Client <-> StreamManager --
    DescribeStream = 0x30,
    DescribeStreamResp = 0x31,
    DescribeExtent = 0x32,
    DescribeExtentResp = 0x33,
    Seek = 0x34,
    SeekResp = 0x35,

    // -- Control (0xFE-0xFF) --
    Error = 0xFF,
}

impl Opcode {
    pub fn from_u8(value: u8) -> Option<Opcode> {
        match value {
            // Data path
            0x01 => Some(Opcode::Append),
            0x02 => Some(Opcode::AppendAck),
            0x03 => Some(Opcode::Read),
            0x04 => Some(Opcode::ReadResp),
            0x05 => Some(Opcode::Seal),
            0x06 => Some(Opcode::SealAck),
            0x07 => Some(Opcode::CreateStream),
            0x08 => Some(Opcode::QueryOffset),
            0x09 => Some(Opcode::QueryOffsetResp),
            // Lifecycle
            0x10 => Some(Opcode::Connect),
            0x11 => Some(Opcode::ConnectAck),
            0x12 => Some(Opcode::Disconnect),
            0x13 => Some(Opcode::DisconnectAck),
            0x14 => Some(Opcode::Heartbeat),
            0x15 => Some(Opcode::RegisterExtent),
            0x16 => Some(Opcode::RegisterExtentAck),
            0x17 => Some(Opcode::Watermark),
            // Cluster management
            0x20 => Some(Opcode::StreamManagerMembershipChange),
            // Management
            0x30 => Some(Opcode::DescribeStream),
            0x31 => Some(Opcode::DescribeStreamResp),
            0x32 => Some(Opcode::DescribeExtent),
            0x33 => Some(Opcode::DescribeExtentResp),
            0x34 => Some(Opcode::Seek),
            0x35 => Some(Opcode::SeekResp),
            // Control
            0xFF => Some(Opcode::Error),
            _ => None,
        }
    }
}

/// State of an extent in metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ExtentState {
    Unspecified = 0,
    Active = 1,
    Sealed = 2,
    Flushed = 3,
}

impl ExtentState {
    pub fn from_u8(value: u8) -> Option<ExtentState> {
        match value {
            0 => Some(ExtentState::Unspecified),
            1 => Some(ExtentState::Active),
            2 => Some(ExtentState::Sealed),
            3 => Some(ExtentState::Flushed),
            _ => None,
        }
    }

    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

/// Operational state of an ExtentNode node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum NodeState {
    Unspecified = 0,
    Alive = 1,
    Dead = 2,
}

impl NodeState {
    pub fn from_u8(value: u8) -> Option<NodeState> {
        match value {
            0 => Some(NodeState::Unspecified),
            1 => Some(NodeState::Alive),
            2 => Some(NodeState::Dead),
            _ => None,
        }
    }

    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

/// Runtime metrics reported by an ExtentNode in each heartbeat.
/// Used by StreamManager's allocator for load-aware extent placement.
///
/// Wire size: 32 bytes (u64 + u64 + u32 + u32 + u64).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NodeMetrics {
    /// Available (free) memory in bytes.
    pub available_memory_bytes: u64,
    /// Total system memory in bytes.
    pub total_memory_bytes: u64,
    /// Current append operations per second (measured over last heartbeat interval).
    pub appends_per_sec: u32,
    /// Number of active (non-sealed) extents hosted on this node.
    pub active_extent_count: u32,
    /// Bytes written per second (measured over last heartbeat interval).
    pub bytes_written_per_sec: u64,
}

impl Default for NodeMetrics {
    fn default() -> Self {
        Self {
            available_memory_bytes: 0,
            total_memory_bytes: 0,
            appends_per_sec: 0,
            active_extent_count: 0,
            bytes_written_per_sec: 0,
        }
    }
}

/// Describes a single extent with its replica set — returned by management APIs.
#[derive(Debug, Clone, PartialEq)]
pub struct ExtentInfo {
    pub extent_id: u32,
    pub base_offset: u64,
    pub message_count: u32,
    pub state: ExtentState,
    pub replicas: Vec<ReplicaDetail>,
}

/// One replica of an extent with node health info.
#[derive(Debug, Clone, PartialEq)]
pub struct ReplicaDetail {
    /// TCP address of the ExtentNode serving this replica (e.g., "host:port").
    pub node_addr: String,
    /// Replication role: 0 = Primary, 1+ = Secondary.
    pub role: u8,
    /// Whether the serving ExtentNode is currently alive (heartbeat active).
    pub is_alive: bool,
}

/// Error codes sent in Error response frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum ErrorCode {
    Ok = 0,
    UnknownStream = 1,
    InvalidOffset = 2,
    ExtentSealed = 3,
    InternalError = 4,
    /// The extent's arena is full. Client should trigger seal-and-new
    /// via Stream Manager, then retry the append on the new extent.
    ExtentFull = 5,
}

impl ErrorCode {
    pub fn from_u16(value: u16) -> Option<ErrorCode> {
        match value {
            0 => Some(ErrorCode::Ok),
            1 => Some(ErrorCode::UnknownStream),
            2 => Some(ErrorCode::InvalidOffset),
            3 => Some(ErrorCode::ExtentSealed),
            4 => Some(ErrorCode::InternalError),
            5 => Some(ErrorCode::ExtentFull),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Opcode ───────────────────────────────────────────────────────────────

    #[test]
    fn opcode_from_u8_all_valid() {
        let cases: &[(u8, Opcode)] = &[
            (0x01, Opcode::Append),
            (0x02, Opcode::AppendAck),
            (0x03, Opcode::Read),
            (0x04, Opcode::ReadResp),
            (0x05, Opcode::Seal),
            (0x06, Opcode::SealAck),
            (0x07, Opcode::CreateStream),
            (0x08, Opcode::QueryOffset),
            (0x09, Opcode::QueryOffsetResp),
            (0x10, Opcode::Connect),
            (0x11, Opcode::ConnectAck),
            (0x12, Opcode::Disconnect),
            (0x13, Opcode::DisconnectAck),
            (0x14, Opcode::Heartbeat),
            (0x15, Opcode::RegisterExtent),
            (0x16, Opcode::RegisterExtentAck),
            (0x17, Opcode::Watermark),
            (0x20, Opcode::StreamManagerMembershipChange),
            (0x30, Opcode::DescribeStream),
            (0x31, Opcode::DescribeStreamResp),
            (0x32, Opcode::DescribeExtent),
            (0x33, Opcode::DescribeExtentResp),
            (0x34, Opcode::Seek),
            (0x35, Opcode::SeekResp),
            (0xFF, Opcode::Error),
        ];
        for &(byte, expected) in cases {
            assert_eq!(
                Opcode::from_u8(byte),
                Some(expected),
                "Opcode::from_u8(0x{byte:02X}) failed"
            );
        }
    }

    #[test]
    fn opcode_from_u8_invalid_returns_none() {
        let invalid: &[u8] = &[0x00, 0x0A, 0x0F, 0x18, 0x1F, 0x21, 0x2F, 0x36, 0xFE];
        for &byte in invalid {
            assert_eq!(
                Opcode::from_u8(byte),
                None,
                "Opcode::from_u8(0x{byte:02X}) should be None"
            );
        }
    }

    // ── ExtentState ──────────────────────────────────────────────────────────

    #[test]
    fn extent_state_round_trip() {
        let states = [
            (0u8, ExtentState::Unspecified),
            (1, ExtentState::Active),
            (2, ExtentState::Sealed),
            (3, ExtentState::Flushed),
        ];
        for (byte, state) in states {
            assert_eq!(ExtentState::from_u8(byte), Some(state));
            assert_eq!(state.as_u8(), byte);
        }
    }

    #[test]
    fn extent_state_from_u8_invalid() {
        for v in [4u8, 10, 128, 255] {
            assert_eq!(ExtentState::from_u8(v), None);
        }
    }

    // ── NodeState ────────────────────────────────────────────────────────────

    #[test]
    fn node_state_round_trip() {
        let states = [
            (0u8, NodeState::Unspecified),
            (1, NodeState::Alive),
            (2, NodeState::Dead),
        ];
        for (byte, state) in states {
            assert_eq!(NodeState::from_u8(byte), Some(state));
            assert_eq!(state.as_u8(), byte);
        }
    }

    #[test]
    fn node_state_from_u8_invalid() {
        for v in [3u8, 10, 128, 255] {
            assert_eq!(NodeState::from_u8(v), None);
        }
    }

    // ── ErrorCode ────────────────────────────────────────────────────────────

    #[test]
    fn error_code_from_u16_all_valid() {
        let cases: &[(u16, ErrorCode)] = &[
            (0, ErrorCode::Ok),
            (1, ErrorCode::UnknownStream),
            (2, ErrorCode::InvalidOffset),
            (3, ErrorCode::ExtentSealed),
            (4, ErrorCode::InternalError),
            (5, ErrorCode::ExtentFull),
        ];
        for &(val, expected) in cases {
            assert_eq!(
                ErrorCode::from_u16(val),
                Some(expected),
                "ErrorCode::from_u16({val}) failed"
            );
        }
    }

    #[test]
    fn error_code_from_u16_invalid() {
        for v in [6u16, 100, 1000, u16::MAX] {
            assert_eq!(ErrorCode::from_u16(v), None);
        }
    }

    // ── Offset ordering ──────────────────────────────────────────────────────

    #[test]
    fn offset_ordering() {
        assert!(Offset(0) < Offset(1));
        assert!(Offset(1) < Offset(u64::MAX));
        assert_eq!(Offset(42), Offset(42));

        let mut offsets = vec![Offset(5), Offset(1), Offset(3), Offset(0), Offset(2)];
        offsets.sort();
        assert_eq!(offsets, vec![Offset(0), Offset(1), Offset(2), Offset(3), Offset(5)]);
    }

    // ── NodeMetrics default ──────────────────────────────────────────────────

    #[test]
    fn node_metrics_default_is_zero() {
        let m = NodeMetrics::default();
        assert_eq!(m.available_memory_bytes, 0);
        assert_eq!(m.total_memory_bytes, 0);
        assert_eq!(m.appends_per_sec, 0);
        assert_eq!(m.active_extent_count, 0);
        assert_eq!(m.bytes_written_per_sec, 0);
    }

    // ── StreamId / ExtentId / NodeId equality & hashing ──────────────────────

    #[test]
    fn stream_id_equality_and_copy() {
        let a = StreamId(42);
        let b = a; // Copy
        assert_eq!(a, b);
        assert_ne!(StreamId(1), StreamId(2));
    }

    #[test]
    fn extent_id_equality_and_copy() {
        let a = ExtentId(100);
        let b = a; // Copy
        assert_eq!(a, b);
        assert_ne!(ExtentId(0), ExtentId(1));
    }

    #[test]
    fn node_id_equality_and_clone() {
        let a = NodeId("node-1".to_string());
        let b = a.clone();
        assert_eq!(a, b);
        assert_ne!(NodeId("x".to_string()), NodeId("y".to_string()));
    }

    // ── Constants ────────────────────────────────────────────────────────────

    #[test]
    fn protocol_constants() {
        assert_eq!(MAGIC, 0xEF);
        assert_eq!(PROTOCOL_VERSION, 1);
        assert_eq!(HEADER_LEN, 32);
        assert_eq!(FLAG_FORWARDED, 0x01);
    }

    // ── ExtentInfo / ReplicaDetail construction ──────────────────────────────

    #[test]
    fn extent_info_construction() {
        let info = ExtentInfo {
            extent_id: 1,
            base_offset: 100,
            message_count: 50,
            state: ExtentState::Active,
            replicas: vec![
                ReplicaDetail {
                    node_addr: "10.0.0.1:9801".to_string(),
                    role: 0,
                    is_alive: true,
                },
                ReplicaDetail {
                    node_addr: "10.0.0.2:9801".to_string(),
                    role: 1,
                    is_alive: false,
                },
            ],
        };
        assert_eq!(info.extent_id, 1);
        assert_eq!(info.state, ExtentState::Active);
        assert_eq!(info.replicas.len(), 2);
        assert!(info.replicas[0].is_alive);
        assert!(!info.replicas[1].is_alive);
    }
}
