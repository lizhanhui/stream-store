//! Per-stream ACK queue for quorum-based broadcast replication.
//!
//! Split into a lock-free producer half ([`AckQueue::enqueue`]) and a
//! Mutex-protected consumer half ([`AckQueue::drain_quorum`]).
//!
//! The append leader pushes `PendingAck` entries via a crossbeam unbounded
//! channel — **no Mutex, no contention** with the watermark reader.
//! The watermark reader locks `AckQueue.inner` to drain the channel,
//! update per-secondary offsets, and send AppendAck frames back to clients.

use std::collections::VecDeque;
use std::fmt::{Debug, Formatter, Result};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use common::config::{DEFAULT_REPLICATION_TIMEOUT_MS, MAX_REPLICATION_FACTOR};
use common::types::{Epoch, ErrorCode, ExtentId, Offset, StreamId};
use crossbeam_channel::Receiver;
use rpc::frame::{Frame, VariableHeader};
use tokio::sync::mpsc::Sender;
use tracing::warn;

/// Default replication timeout used when no config is provided (e.g., in tests).
pub(crate) const DEFAULT_REPLICATION_TIMEOUT: Duration =
    Duration::from_millis(DEFAULT_REPLICATION_TIMEOUT_MS);

/// Maximum number of secondaries (MAX_REPLICATION_FACTOR - 1, excluding the Primary).
const MAX_SECONDARIES: usize = MAX_REPLICATION_FACTOR - 1;

/// A pending client ACK waiting for quorum replication.
#[derive(Debug)]
pub struct PendingAck {
    /// The original request_id from the client's Append frame.
    pub request_id: u32,
    /// The stream the append was written to.
    pub stream_id: StreamId,
    /// Channel back to the client connection's write task.
    pub response_tx: Sender<Frame>,
    /// The offset assigned to this append.
    pub assigned_offset: u64,
    /// The extent the record landed on (for diagnostics in AppendAck).
    pub extent_id: ExtentId,
    pub epoch: Epoch,
    /// When this PendingAck was created, for timeout expiry.
    pub created_at: Instant,
}

/// Per-stream ACK queue on the Primary with cumulative quorum tracking.
///
/// Split into two halves to eliminate lock contention between the append
/// hot path (producer) and watermark readers (consumer):
///
/// - **Producer** ([`enqueue`]): pushes `PendingAck` via a lock-free
///   crossbeam channel. The append leader never acquires a Mutex.
///
/// - **Consumer** ([`lock_inner`]): watermark readers lock `inner` to
///   drain the channel, update `acked[]`, and send AppendAck frames.
///
/// `acked` is a fixed-size array indexed by secondary ordinal (role - 1).
/// Entries are `u64::MAX` (sentinel = "never reported").
pub struct AckQueue {
    /// Lock-free producer channel. Append leaders push PendingAcks here.
    tx: crossbeam_channel::Sender<PendingAck>,

    /// Epoch for which this queue is active; u64::MAX means inactive.
    active_epoch: AtomicU64,

    /// Consumer state, protected by Mutex. Only watermark readers touch this.
    inner: Mutex<AckQueueInner>,
}

/// Consumer-side state, protected by the Mutex inside [`AckQueue`].
pub struct AckQueueInner {
    /// Receiver end of the lock-free channel.
    rx: Receiver<PendingAck>,

    /// Pending client ACKs, ordered by offset (front = lowest).
    /// Populated by [`receive_pending`] which drains `rx`.
    pub(crate) pending: VecDeque<PendingAck>,

    /// Extent being acknowledged by each secondary.
    extents: [Option<ExtentId>; MAX_SECONDARIES],

    // Highest offset acknowledged by each secondary.
    /// `u64::MAX` = never reported for the corresponding extent.
    acked: [u64; MAX_SECONDARIES],

    /// Number of secondary ACKs needed for quorum.
    pub required_acks: u32,

    /// Timeout for expiring stale PendingAcks.
    replication_timeout: Duration,
}

impl AckQueue {
    pub fn new(required_secondary_acks: u32) -> Self {
        Self::with_timeout(required_secondary_acks, DEFAULT_REPLICATION_TIMEOUT)
    }

    pub fn with_timeout(required_secondary_acks: u32, replication_timeout: Duration) -> Self {
        let (tx, rx) = crossbeam_channel::unbounded();
        Self {
            tx,
            active_epoch: AtomicU64::new(0),
            inner: Mutex::new(AckQueueInner {
                rx,
                pending: VecDeque::new(),
                extents: [None; MAX_SECONDARIES],
                acked: [u64::MAX; MAX_SECONDARIES],
                required_acks: required_secondary_acks,
                replication_timeout,
            }),
        }
    }

    /// Enqueue a PendingAck from the append leader — **lock-free**.
    ///
    /// Uses crossbeam's unbounded channel internally, so this never blocks
    /// and never contends with the watermark reader's Mutex.
    pub fn enqueue(&self, ack: PendingAck) {
        if !self.is_active_at(ack.epoch) {
            let frame = Frame::append_ack_error(
                ack.request_id,
                ack.stream_id,
                ack.epoch,
                ack.extent_id,
                ErrorCode::NotPrimary,
                "Primary assignment changed",
            );
            let _ = ack.response_tx.try_send(frame);
            return;
        }
        let _ = self.tx.send(ack);
    }

    pub fn is_active_at(&self, epoch: Epoch) -> bool {
        self.active_epoch.load(Ordering::Acquire) == epoch.0 as u64
    }

    /// Activate this queue for a Primary assignment. A new epoch or quorum
    /// topology invalidates all old pending acknowledgments and watermarks.
    pub fn activate(&self, epoch: Epoch, required_acks: u32) {
        if self.is_active_at(epoch) {
            let inner = self.inner.lock().unwrap();
            if inner.required_acks == required_acks {
                return;
            }
            drop(inner);
        }

        self.active_epoch.store(u64::MAX, Ordering::Release);
        let mut inner = self.inner.lock().unwrap();
        inner.fail_all_pending("Primary assignment changed");
        inner.extents = [None; MAX_SECONDARIES];
        inner.acked = [u64::MAX; MAX_SECONDARIES];
        inner.required_acks = required_acks;
        self.active_epoch.store(epoch.0 as u64, Ordering::Release);
    }

    /// Deactivate the queue when this node becomes a Secondary.
    pub fn deactivate(&self) {
        self.active_epoch.store(u64::MAX, Ordering::Release);
        self.inner
            .lock()
            .unwrap()
            .fail_all_pending("this extent node is not the stream primary");
    }

    pub fn update_watermark(
        &self,
        epoch: Epoch,
        extent_id: ExtentId,
        secondary_index: u8,
        offset: u64,
    ) {
        if !self.is_active_at(epoch) {
            return;
        }
        let mut inner = self.inner.lock().unwrap();
        if !self.is_active_at(epoch) {
            return;
        }
        inner.update_watermark(secondary_index, extent_id, offset);
        inner.drain_quorum();
    }

    /// Lock the consumer-side state for watermark processing.
    ///
    /// Call [`AckQueueInner::receive_pending`] first to drain the channel,
    /// then [`AckQueueInner::ack_from_secondary`] and [`AckQueueInner::drain_quorum`].
    pub fn lock_inner(&self) -> MutexGuard<'_, AckQueueInner> {
        self.inner.lock().unwrap()
    }
}

impl AckQueueInner {
    fn fail_all_pending(&mut self, message: &str) {
        self.receive_pending();
        while let Some(ack) = self.pending.pop_front() {
            let frame = Frame::append_ack_error(
                ack.request_id,
                ack.stream_id,
                ack.epoch,
                ack.extent_id,
                ErrorCode::NotPrimary,
                message,
            );
            let _ = ack.response_tx.try_send(frame);
        }
    }

    /// Drain all available PendingAcks from the lock-free channel into
    /// the local `pending` VecDeque. Call this before `drain_quorum`.
    pub(crate) fn receive_pending(&mut self) {
        while let Ok(ack) = self.rx.try_recv() {
            self.pending.push_back(ack);
        }
    }

    /// Compute the quorum offset: the highest offset where at least
    /// `required_acks` secondaries have confirmed.
    ///
    /// Returns None if quorum cannot be met (not enough secondaries have reported).
    pub fn quorum_offset(&self, extent_id: ExtentId) -> Option<u64> {
        if self.required_acks == 0 {
            return None; // RF=1, no quorum needed
        }
        let required = self.required_acks as usize;

        // Collect offsets from secondaries that have reported (not u64::MAX).
        let mut offsets = [0u64; MAX_SECONDARIES];
        let mut count = 0;
        for (index, &value) in self.acked.iter().enumerate() {
            if self.extents[index] == Some(extent_id) && value != u64::MAX {
                offsets[count] = value;
                count += 1;
            }
        }
        if count < required {
            return None;
        }

        // Fast path for RF=2 (required=1): return the max.
        if required == 1 {
            return Some(offsets[..count].iter().copied().max().unwrap_or(0));
        }

        // General case: sort descending, pick the required-th highest.
        let slice = &mut offsets[..count];
        slice.sort_unstable_by(|a, b| b.cmp(a));
        slice.get(required - 1).copied()
    }

    /// Record a cumulative ACK from a secondary at the given index and offset.
    ///
    /// `secondary_index` is the secondary's ordinal (role - 1): secondary-1 → 0,
    /// secondary-2 → 1, etc. Callers resolve this once per (stream, connection)
    /// pair and cache it locally.
    pub fn update_watermark(&mut self, secondary: u8, extent_id: ExtentId, offset: u64) {
        self.receive_pending();

        let idx = secondary as usize;
        debug_assert!(
            idx < MAX_SECONDARIES,
            "secondary_index {} exceeds MAX_SECONDARIES {}",
            idx,
            MAX_SECONDARIES,
        );
        if idx >= MAX_SECONDARIES {
            return;
        }
        if self.extents[idx] != Some(extent_id) {
            self.extents[idx] = Some(extent_id);
            self.acked[idx] = offset;
            return;
        }
        let current = self.acked[idx];
        if current == u64::MAX || offset > current {
            self.acked[idx] = offset;
        }
    }

    /// Drain all pending ACKs that have reached quorum, sending AppendAck
    /// frames back to the client connections.
    ///
    /// Call [`receive_pending`] first to transfer PendingAcks from the
    /// lock-free channel into the local VecDeque.
    ///
    /// After the normal quorum drain, sweeps the front of the queue for expired
    /// entries (older than the configured replication timeout) and sends error responses.
    pub fn drain_quorum(&mut self) {
        while let Some(front) = self.pending.front() {
            let Some(quorum_offset) = self.quorum_offset(front.extent_id) else {
                break;
            };
            if front.assigned_offset > quorum_offset {
                break;
            }
            let ack = self.pending.pop_front().unwrap();
            let frame = Frame::new(
                VariableHeader::AppendAck {
                    request_id: ack.request_id,
                    stream_id: ack.stream_id,
                    epoch: ack.epoch,
                    extent_id: ack.extent_id,
                    offset: Offset(ack.assigned_offset),
                },
                None,
            );
            // Best-effort send — if the client disconnected, the channel is closed.
            let _ = ack.response_tx.try_send(frame);
        }

        // Timeout sweep: expire PendingAcks older than the configured replication timeout.
        // Queue is ordered by creation time, so stop at the first non-expired entry.
        let now = Instant::now();
        while let Some(front) = self.pending.front() {
            if now.duration_since(front.created_at) > self.replication_timeout {
                let ack = self.pending.pop_front().unwrap();
                warn!(
                    request_id = ack.request_id,
                    stream_id = %ack.stream_id,
                    extent_id = %ack.extent_id,
                    offset = ack.assigned_offset,
                    "PendingAck expired after replication timeout",
                );
                let frame = Frame::append_ack_error(
                    ack.request_id,
                    ack.stream_id,
                    ack.epoch,
                    ack.extent_id,
                    ErrorCode::InternalError,
                    "replication timeout",
                );
                let _ = ack.response_tx.try_send(frame);
            } else {
                break;
            }
        }
    }
}

// AckQueue must be Send + Sync for papaya::HashMap storage.
// crossbeam Sender/Receiver are Send + Sync, Mutex<AckQueueInner> is Send + Sync.
const _: () = {
    fn _assert_send<T: Send>() {}
    fn _assert_sync<T: Sync>() {}
    fn _assert_all() {
        _assert_send::<AckQueue>();
        _assert_sync::<AckQueue>();
    }
};

impl Debug for AckQueue {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.debug_struct("AckQueue")
            .field("inner", &"<locked>")
            .finish()
    }
}

impl Debug for AckQueueInner {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.debug_struct("AckQueueInner")
            .field("pending_len", &self.pending.len())
            .field("acked", &self.acked)
            .field("required_acks", &self.required_acks)
            .finish()
    }
}
