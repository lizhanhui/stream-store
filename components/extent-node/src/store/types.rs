use bytes::Bytes;
use common::types::{Epoch, ExtentId, StreamId};
use rpc::payload::ROLE_PRIMARY;
use tokio::sync::mpsc::Sender;

use rpc::frame::Frame;

/// Notification emitted by the Primary to update Stream Manager about extent state.
/// Sent to the SM connection task which forwards it as an UPDATE_EXTENT frame.
#[derive(Debug, Clone)]
pub enum ExtentUpdate {
    /// Extent was sealed and a new one created (autonomous extent creation).
    Sealed {
        stream_id: StreamId,
        sealed_extent_id: ExtentId,
        end_offset: u64,
        new_extent_id: ExtentId,
        new_extent_capacity: u32,
        epoch: Epoch,
    },
    /// Periodic progress report for an active extent (observability).
    Progress {
        stream_id: StreamId,
        extent_id: ExtentId,
        current_offset: u64,
        epoch: Epoch,
    },
}

// ── Replica info ─────────────────────────────────────────────────────────────

/// Replication role and topology info for a single extent on this ExtentNode.
#[derive(Debug, Clone)]
pub struct ReplicaInfo {
    /// Stream this extent belongs to.
    pub stream_id: StreamId,
    /// Extent this replica assignment covers.
    pub extent_id: ExtentId,
    /// 0 = Primary, 1+ = Secondary.
    pub role: u8,
    /// Total replication factor (used for quorum calculation).
    pub replication_factor: u16,
    /// All secondary addresses (Primary only). Empty for secondaries.
    pub replica_addrs: Vec<String>,
}

impl ReplicaInfo {
    pub fn is_primary(&self) -> bool {
        self.role == ROLE_PRIMARY
    }

    /// True if RF=1 (no secondaries needed). Immediate ACK.
    pub fn is_standalone(&self) -> bool {
        self.replication_factor <= 1 || self.replica_addrs.is_empty()
    }

    /// Number of secondary ACKs required for quorum.
    /// Formula: rf / 2 (integer division).
    /// RF=1: 0, RF=2: 1, RF=3: 1, RF=4: 2
    pub fn required_secondary_acks(&self) -> u32 {
        (self.replication_factor as u32) / 2
    }
}

// ── Pipelined group commit types ─────────────────────────────────────────────

/// A pending append job delegated from a follower to the active writer.
///
/// When a thread arrives at a stream and finds another writer already active
/// (via `in_flight` counter), it pushes an `AppendJob` into the stream's channel
/// and returns immediately. The active writer drains these jobs as a batch.
pub(crate) struct AppendJob {
    pub request_id: u32,
    pub stream_id: StreamId,
    pub payload: Bytes,
    /// Channel back to the client connection for sending response frames.
    /// `None` in test mode (no client connection).
    pub response_tx: Option<Sender<Frame>>,
}
