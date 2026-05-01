use std::fmt::{Display, Formatter, Result};

/// Magic byte identifying our wire protocol.
pub const MAGIC: u8 = 0xEF;

/// Current protocol version.
pub const PROTOCOL_VERSION: u8 = 2;

/// Fixed header length in bytes (Magic 1 + Version 1 + Opcode 1 + Flags 1
/// + RemainingLength 4 = 8).
pub const HEADER_LEN: usize = 8;

/// Flag on all request-response opcodes: success response.
/// When clear (0x00) and FLAG_RESPONSE_ERROR also clear: request frame.
/// When set (0x01): success response frame.
/// Error responses use FLAG_RESPONSE_ERROR (0x80) instead.
pub const FLAG_RESPONSE: u8 = 0x01;

/// Flag on UPDATE_EXTENT: progress report for an active extent.
pub const FLAG_EXTENT_PROGRESS: u8 = 0x01;
/// Flag on UPDATE_EXTENT: extent was flushed to S3.
pub const FLAG_EXTENT_FLUSHED: u8 = 0x02;

/// Flag on FORWARD: normal per-record replication.
pub const FLAG_FORWARD_APPEND: u8 = 0x00;
/// Flag on FORWARD: init-extent notification (new extent metadata).
pub const FLAG_FORWARD_INIT_EPOCH: u8 = 0x01;
/// Flag on FORWARD: CRC32 checksum verification for sealed extent.
pub const FLAG_FORWARD_CHECKSUM: u8 = 0x02;
/// Flag on FORWARD: extent flushed to S3 notification.
pub const FLAG_FORWARD_FLUSHED: u8 = 0x03;

/// Flag on DESCRIBE_STREAM: lookup by stream name instead of stream_id.
/// When set, variable header carries [name_len:u16][name_bytes] after count.
/// Uses 0x02 (not 0x01) to avoid conflict with FLAG_RESPONSE.
pub const FLAG_DESCRIBE_STREAM_BY_NAME: u8 = 0x02;

/// Shared flag on response opcodes indicating the response carries an
/// opcode-specific error header instead of the success header layout.
pub const FLAG_RESPONSE_ERROR: u8 = 0x80;

/// Flag on SEAL_EXTENT_NODE: phase 2 commit with authoritative committed offset.
/// SM broadcasts after computing quorum offset so replicas correct their local
/// seal point. Uses 0x02 (not 0x01) to avoid conflict with FLAG_RESPONSE.
pub const FLAG_SEAL_COMMIT: u8 = 0x02;

/// Flag on SEAL_EXTENT_NODE: phase 2 commit acknowledgement.
/// Sent by EN back to SM after processing SealEpochCommit.
pub const FLAG_SEAL_COMMIT_RESP: u8 = 0x03;

/// Unique identifier for a stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StreamId(pub u32);

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
///
/// All request-response opcodes use a single opcode with flags to distinguish
/// direction: flag=0x00 request, flag=0x01 (FLAG_RESPONSE) success response,
/// flag=0x80 (FLAG_RESPONSE_ERROR) error response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Opcode {
    // -- Data path (0x01-0x0F): Client <-> ExtentNode --
    /// Create a new stream. Flags: 0x00=request, 0x01=response, 0x80=error.
    CreateStream = 0x01,
    /// Append a message. Flags: 0x00=request, 0x01=ack, 0x80=error.
    Append = 0x03,
    /// Dedicated forward opcode for Primary→Secondary replication.
    /// Carries all metadata (including byte_pos) so the secondary writes
    /// each record at the exact same position as the primary.
    Forward = 0x05,
    /// Epoch-based seal: Client ↔ StreamManager.
    /// Flags: 0x00=request, 0x01=response, 0x80=error.
    SealStream = 0x06,
    /// Epoch-based seal: StreamManager ↔ ExtentNode.
    /// Flags: 0x00=request, 0x01=response, 0x80=error.
    SealEpoch = 0x07,
    /// Query max offset. Flags: 0x00=request, 0x01=response, 0x80=error.
    QueryOffset = 0x08,
    /// Read messages. Flags: 0x00=request, 0x01=response, 0x80=error.
    Read = 0x0A,

    // -- Lifecycle (0x10-0x1F): ExtentNode <-> StreamManager --
    /// ExtentNode Connect. Flags: 0x00=request, 0x01=ack, 0x80=error.
    Connect = 0x10,
    /// ExtentNode Disconnect. Flags: 0x00=request, 0x01=ack, 0x80=error.
    Disconnect = 0x12,
    Heartbeat = 0x14,
    /// Register extent replica. Flags: 0x00=request, 0x01=ack, 0x80=error.
    RegisterEpoch = 0x15,
    Watermark = 0x17,
    /// Async extent update from EN to SM. Fire-and-forget.
    /// Flags distinguish variants: sealed (0x00) or progress (0x01).
    UpdateExtent = 0x18,
    /// SM queries an EN for all extents it holds for a stream (recovery path).
    /// Flags: 0x00=request, 0x01=response, 0x80=error.
    ReportExtents = 0x19,
    /// SM commands EN to flush a sealed extent to S3 (disaster recovery).
    /// Fire-and-forget: no response.
    FlushExtent = 0x1B,

    // -- Cluster management (0x20-0x2F): StreamManager -> ExtentNode/Client --
    StreamManagerMembershipChange = 0x20,

    // -- Management (0x30-0x3F): Client <-> StreamManager --
    /// Describe stream extents. Flags: 0x00=request, 0x01=response, 0x80=error.
    /// Request flag 0x02=by-name lookup (FLAG_DESCRIBE_STREAM_BY_NAME).
    DescribeStream = 0x30,
    /// Describe a single extent. Flags: 0x00=request, 0x01=response, 0x80=error.
    DescribeExtent = 0x32,
    /// Seek: resolve offset to extent. Flags: 0x00=request, 0x01=response, 0x80=error.
    Seek = 0x34,
}

impl Opcode {
    pub fn from_u8(value: u8) -> Option<Opcode> {
        match value {
            // Data path
            0x01 => Some(Opcode::CreateStream),
            0x03 => Some(Opcode::Append),
            0x05 => Some(Opcode::Forward),
            0x06 => Some(Opcode::SealStream),
            0x07 => Some(Opcode::SealEpoch),
            0x08 => Some(Opcode::QueryOffset),
            0x0A => Some(Opcode::Read),
            // Lifecycle
            0x10 => Some(Opcode::Connect),
            0x12 => Some(Opcode::Disconnect),
            0x14 => Some(Opcode::Heartbeat),
            0x15 => Some(Opcode::RegisterEpoch),
            0x17 => Some(Opcode::Watermark),
            0x18 => Some(Opcode::UpdateExtent),
            0x19 => Some(Opcode::ReportExtents),
            0x1B => Some(Opcode::FlushExtent),
            // Cluster management
            0x20 => Some(Opcode::StreamManagerMembershipChange),
            // Management
            0x30 => Some(Opcode::DescribeStream),
            0x32 => Some(Opcode::DescribeExtent),
            0x34 => Some(Opcode::Seek),
            _ => None,
        }
    }
}

/// Storage class for a stream's sealed extents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StorageClass {
    /// Sealed extents are uploaded to S3 for durability. Eviction only after flush.
    S3 = 0,
    /// Sealed extents stay in memory only. Evicted when cache_extents limit is hit.
    /// Data is lost after eviction (acceptable for ephemeral workloads).
    Memory = 1,
}

impl StorageClass {
    pub fn from_u8(value: u8) -> Option<StorageClass> {
        match value {
            0 => Some(StorageClass::S3),
            1 => Some(StorageClass::Memory),
            _ => None,
        }
    }

    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

/// Capacity-scaling policy for extents within a stream. Describes only
/// sizing/caching — no durability or identity semantics.
///
/// A zero on any field means "use the server default".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ExtentPolicy {
    /// Maximum extents to retain in memory for the stream (0 = default).
    pub cache: u16,
}

/// Full per-stream configuration threaded through replica registration,
/// seal-and-new allocation, and forwarded extent init.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamConfig {
    pub stream_id: StreamId,
    pub replication_factor: u8,
    pub epoch: Epoch,
    pub storage_class: StorageClass,
    pub policy: ExtentPolicy,
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
