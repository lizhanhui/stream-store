/// Magic byte identifying our wire protocol.
pub const MAGIC: u8 = 0xEF;

/// Current protocol version.
pub const PROTOCOL_VERSION: u8 = 1;

/// Fixed header length in bytes (Magic 1 + Version 1 + Opcode 1 + Flags 1
/// + RemainingLength 4 = 8).
pub const HEADER_LEN: usize = 8;

/// Flag bit indicating a forwarded append (broadcast replication).
/// When set on an Append frame, the receiving ExtentNode acts as a Secondary
/// (writes locally and returns a Watermark).
pub const FLAG_FORWARDED: u8 = 0x01;

/// Flag bit on SEAL indicating the caller provides the resolved end offset.
/// When clear (client seal): Stream Manager queries all EN replicas for offset.
/// When set (extent-node seal): offset field is present and trusted by SM.
pub const FLAG_OFFSET_PRESENT: u8 = 0x01;

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
    CreateStreamResp = 0x0A,

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
            0x0A => Some(Opcode::CreateStreamResp),
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
