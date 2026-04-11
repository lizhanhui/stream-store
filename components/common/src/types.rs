use std::fmt::{Display, Formatter, Result};

/// Magic byte identifying our wire protocol.
pub const MAGIC: u8 = 0xEF;

/// Current protocol version.
pub const PROTOCOL_VERSION: u8 = 2;

/// Fixed header length in bytes (Magic 1 + Version 1 + Opcode 1 + Flags 1
/// + RemainingLength 4 = 8).
pub const HEADER_LEN: usize = 8;

/// Flag bit on SEAL indicating the caller provides the resolved end offset.
/// When clear (client seal): Stream Manager queries all EN replicas for offset.
/// When set (extent-node seal): offset field is present and trusted by SM.
pub const FLAG_OFFSET_PRESENT: u8 = 0x01;

/// Flag bit on SEAL indicating the frame carries the extent's start_offset.
/// When set, secondaries that have no extent can respond with SealAck(start_offset)
/// to indicate zero committed records, enabling Stream Manager quorum resolution.
pub const FLAG_START_OFFSET_PRESENT: u8 = 0x02;

/// Flag bit on SEAL_ACK indicating the response carries new extent info.
/// When clear (EN→SM): only base variable header (no new extent info).
/// When set (SM→Client): variable header includes new_extent_id + primary_addr.
pub const FLAG_NEW_EXTENT_PRESENT: u8 = 0x01;

/// Flag bit on SEAL / SEAL_ACK indicating the frame carries an epoch field.
/// Used for epoch-based seal (client seals by epoch rather than extent_id)
/// and for SM responses that include the new epoch after an epoch bump.
pub const FLAG_EPOCH_PRESENT: u8 = 0x04;

/// Flag on UPDATE_EXTENT: extent was sealed, new extent created.
pub const FLAG_EXTENT_SEALED: u8 = 0x00;
/// Flag on UPDATE_EXTENT: progress report for an active extent.
pub const FLAG_EXTENT_PROGRESS: u8 = 0x01;

/// Flag on FORWARD: normal per-record replication.
pub const FLAG_FORWARD_APPEND: u8 = 0x00;
/// Flag on FORWARD: init-extent notification (new extent metadata).
pub const FLAG_FORWARD_INIT_EXTENT: u8 = 0x01;
/// Flag on FORWARD: CRC32 checksum verification for sealed extent.
pub const FLAG_FORWARD_CHECKSUM: u8 = 0x02;

/// Flag on DESCRIBE_STREAM: lookup by stream name instead of stream_id.
/// When set, variable header carries [name_len:u16][name_bytes] after count.
pub const FLAG_DESCRIBE_STREAM_BY_NAME: u8 = 0x01;

/// Shared flag on response opcodes indicating the response carries an
/// opcode-specific error header instead of the success header layout.
pub const FLAG_RESPONSE_ERROR: u8 = 0x80;

/// Unique identifier for a stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StreamId(pub u64);

impl Display for StreamId {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", self.0)
    }
}

/// Unique identifier for an extent within a stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExtentId(pub u32);

impl Display for ExtentId {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", self.0)
    }
}

/// Stream epoch. Identifies a replica set assignment. Bumped by Stream Manager
/// on node failure or rebalancing; within an epoch the Primary can autonomously
/// create extents on extent-full without SM involvement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Epoch(pub u32);

impl Display for Epoch {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", self.0)
    }
}

/// Logical offset within a stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Offset(pub u64);

impl Display for Offset {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", self.0)
    }
}

/// Unique identifier for an ExtentNode node (typically its listen address).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeId(pub String);

impl Display for NodeId {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", self.0)
    }
}

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
    /// Dedicated forward opcode for Primary→Secondary replication.
    /// Carries all metadata (including byte_pos) so the secondary writes
    /// each record at the exact same position as the primary.
    Forward = 0x0B,

    // -- Lifecycle (0x10-0x1F): ExtentNode <-> StreamManager --
    Connect = 0x10,
    ConnectAck = 0x11,
    Disconnect = 0x12,
    DisconnectAck = 0x13,
    Heartbeat = 0x14,
    RegisterExtent = 0x15,
    RegisterExtentAck = 0x16,
    Watermark = 0x17,
    /// Async extent update from Primary EN to SM. Fire-and-forget.
    /// Flags distinguish variants: sealed (0x00) or progress (0x01).
    UpdateExtent = 0x18,
    /// SM queries an EN for all extents it holds for a stream (recovery path).
    ReportExtents = 0x19,
    /// EN response to ReportExtents with extent state for reconciliation.
    ReportExtentsResp = 0x1A,

    // -- Cluster management (0x20-0x2F): StreamManager -> ExtentNode/Client --
    StreamManagerMembershipChange = 0x20,

    // -- Management (0x30-0x3F): Client <-> StreamManager --
    DescribeStream = 0x30,
    DescribeStreamResp = 0x31,
    DescribeExtent = 0x32,
    DescribeExtentResp = 0x33,
    Seek = 0x34,
    SeekResp = 0x35,
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
            0x0B => Some(Opcode::Forward),
            // Lifecycle
            0x10 => Some(Opcode::Connect),
            0x11 => Some(Opcode::ConnectAck),
            0x12 => Some(Opcode::Disconnect),
            0x13 => Some(Opcode::DisconnectAck),
            0x14 => Some(Opcode::Heartbeat),
            0x15 => Some(Opcode::RegisterExtent),
            0x16 => Some(Opcode::RegisterExtentAck),
            0x17 => Some(Opcode::Watermark),
            0x18 => Some(Opcode::UpdateExtent),
            0x19 => Some(Opcode::ReportExtents),
            0x1A => Some(Opcode::ReportExtentsResp),
            // Cluster management
            0x20 => Some(Opcode::StreamManagerMembershipChange),
            // Management
            0x30 => Some(Opcode::DescribeStream),
            0x31 => Some(Opcode::DescribeStreamResp),
            0x32 => Some(Opcode::DescribeExtent),
            0x33 => Some(Opcode::DescribeExtentResp),
            0x34 => Some(Opcode::Seek),
            0x35 => Some(Opcode::SeekResp),
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
#[derive(Debug, Clone, Copy, PartialEq, Default)]
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

/// Describes a single extent with its replica set — returned by management APIs.
#[derive(Debug, Clone, PartialEq)]
pub struct ExtentInfo {
    pub extent_id: u32,
    pub start_offset: u64,
    pub end_offset: u64,
    pub state: ExtentState,
    pub epoch: Epoch,
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
///
/// Note: `ExtentFull` (formerly 5) was removed from the wire protocol — in the
/// epoch-based seal model the server handles extent rotation internally, so
/// clients never observe that condition.  The internal `StorageError::ExtentFull`
/// still exists for extent-node-local use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum ErrorCode {
    Ok = 0,
    UnknownStream = 1,
    InvalidOffset = 2,
    ExtentSealed = 3,
    InternalError = 4,
    EpochStale = 5,
}

impl ErrorCode {
    pub fn from_u16(value: u16) -> Option<ErrorCode> {
        match value {
            0 => Some(ErrorCode::Ok),
            1 => Some(ErrorCode::UnknownStream),
            2 => Some(ErrorCode::InvalidOffset),
            3 => Some(ErrorCode::ExtentSealed),
            4 => Some(ErrorCode::InternalError),
            5 => Some(ErrorCode::EpochStale),
            _ => None,
        }
    }
}
