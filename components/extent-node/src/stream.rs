use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use common::errors::StorageError;
use common::types::{Epoch, ExtentId, ExtentState, Offset, StreamId};
use crossbeam_channel::{Receiver, Sender, unbounded};

use crate::extent::{AppendResult, DEFAULT_ARENA_CAPACITY, Extent};
use crate::store::AppendJob;

/// A stream: an ordered, append-only sequence of messages backed by a list of extents.
///
/// The active (last) extent is a lock-free arena. Multiple concurrent appenders
/// can write to it without any external mutex -- offset assignment, payload copy,
/// and commit advancement are all handled by the Extent's internal atomics.
///
/// Each extent maintains an internal index mapping sequence numbers to byte
/// positions (compressed u32 pointers). The index is populated atomically during
/// append and used during read to resolve offsets without client-side byte_pos.
///
/// Stream-level mutation (`seal`, adding new extents) still requires `&mut self`
/// because these operations change the extent list. In the ExtentNodeStore, this is
/// handled at a higher level (DashMap per-stream write lock or equivalent).
///
/// Pipelined group commit is coordinated at the stream level via `in_flight`,
/// `job_tx`, and `job_rx`. This ensures extent transitions (seal + create) are
/// handled transparently by the stream-level leader without callers needing to
/// know about individual extent boundaries.
pub struct Stream {
    pub id: StreamId,
    extents: Vec<Extent>,
    /// Current epoch assigned by Stream Manager. Within an epoch, the replica set
    /// is fixed and the Primary can autonomously create extents on extent-full.
    epoch: Epoch,
    /// Next extent ID for autonomous creation within the current epoch.
    /// Initialized to `first_extent_id + 1` when SM sends RegisterExtent.
    next_extent_id: ExtentId,
    /// Arena capacity for autonomously created extents (bytes).
    arena_capacity: u32,
    /// Leader election counter for pipelined group commit (stream-level).
    /// 0 = idle. The leader owns the entire stream, handling extent transitions inline.
    in_flight: AtomicU64,
    /// Channel for followers to submit append jobs to the active writer.
    job_tx: Sender<AppendJob>,
    job_rx: Receiver<AppendJob>,
}

impl Stream {
    /// Create a new stream with no extents. Extents are added via `register_extent()`.
    pub fn new(id: StreamId) -> Self {
        let (job_tx, job_rx) = unbounded();
        Self {
            id,
            extents: Vec::new(),
            epoch: Epoch(0),
            next_extent_id: ExtentId(0),
            arena_capacity: DEFAULT_ARENA_CAPACITY,
            in_flight: AtomicU64::new(0),
            job_tx,
            job_rx,
        }
    }

    /// Register a new extent on this stream (called when SM sends RegisterExtent).
    /// Updates the epoch and sets up the next extent ID for autonomous creation.
    pub fn register_extent(
        &mut self,
        id: ExtentId,
        start_offset: Offset,
        capacity: u32,
        epoch: Epoch,
    ) {
        self.epoch = epoch;
        self.arena_capacity = capacity;
        self.next_extent_id = ExtentId(id.0 + 1);
        self.extents
            .push(Extent::with_capacity(id, start_offset, capacity, epoch));
    }

    /// Return the arena capacity configured for this stream.
    pub fn arena_capacity(&self) -> u32 {
        self.arena_capacity
    }

    /// Append a message to the specified extent. Returns the assigned
    /// offset and byte position within the extent arena.
    ///
    /// Only requires `&self` -- the Extent is internally synchronized (lock-free).
    /// The byte_pos is recorded in the extent's internal index automatically.
    ///
    /// Returns an error if the extent doesn't exist.
    pub fn append(
        &self,
        extent_id: ExtentId,
        payload: Bytes,
    ) -> Result<AppendResult, StorageError> {
        let extent = self.find_extent(extent_id).ok_or_else(|| {
            StorageError::Internal(format!(
                "stream {}: extent {} not found",
                self.id, extent_id
            ))
        })?;
        extent.append(payload)
    }

    /// Replicate a record at the exact position assigned by the primary.
    ///
    /// Delegates to `Extent::replicate()` for deterministic replication.
    /// Only requires `&self` — the Extent handles writes internally.
    pub fn replicate(
        &self,
        extent_id: ExtentId,
        seq: u64,
        byte_pos: u64,
        payload: Bytes,
    ) -> Result<AppendResult, StorageError> {
        let extent = self.find_extent(extent_id).ok_or_else(|| {
            StorageError::Internal(format!(
                "stream {}: extent {} not found",
                self.id, extent_id
            ))
        })?;
        extent.replicate(seq, byte_pos, payload)
    }

    /// Read `count` messages starting from the given logical `offset` within
    /// the specified extent.
    ///
    /// The server resolves `offset → byte_pos` internally via the index stream,
    /// so callers only need to provide the logical offset. This keeps byte_pos
    /// as an internal implementation detail invisible to clients.
    pub fn read(
        &self,
        extent_id: ExtentId,
        offset: Offset,
        count: u32,
    ) -> Result<Vec<Bytes>, StorageError> {
        let extent = self.find_extent(extent_id).ok_or_else(|| {
            StorageError::Internal(format!(
                "stream {}: extent {} not found",
                self.id, extent_id
            ))
        })?;

        // Check offset is within this extent's range.
        if offset.0 < extent.start_offset.0 || offset.0 >= extent.next_offset().0 {
            return Ok(Vec::new());
        }

        let seq = offset.0 - extent.start_offset.0;
        let byte_pos = extent.index_lookup(seq).ok_or_else(|| {
            StorageError::Internal(format!("index lookup failed for offset {}", offset.0))
        })?;
        extent.read(byte_pos, count)
    }

    /// Whether this stream can accept appends (its last extent is active/unsealed).
    pub fn is_mutable(&self) -> bool {
        self.extents
            .last()
            .map(|e| e.state() == ExtentState::Active)
            .unwrap_or(false)
    }

    /// The extent ID of the active (last) extent, or None if no extents.
    pub fn active_extent_id(&self) -> Option<ExtentId> {
        self.extents.last().map(|e| e.id)
    }

    /// The maximum offset (exclusive): the next offset that would be assigned.
    /// Returns `Offset(0)` if the stream has no extents.
    pub fn max_offset(&self) -> Offset {
        self.extents
            .last()
            .map(|e| e.next_offset())
            .unwrap_or(Offset(0))
    }

    /// Seal the extent identified by `extent_id`.
    /// Returns `(start_offset, end_offset)` of the sealed extent, or `None` if:
    /// - no extents exist
    /// - the active extent doesn't match `extent_id`
    /// - the extent is already sealed
    ///
    /// `end_offset` = `start_offset + message_count` (exclusive upper bound).
    ///
    /// If `committed_offset` is `Some`, it's the primary's committed offset propagated
    /// via SM. The sealed extent will accept late forwarded appends up to that offset.
    /// If `None`, the extent uses its local record_count (primary sealing itself).
    ///
    /// After seal, the stream has no active extent until SM sends a new `RegisterExtent`
    /// or the Primary autonomously creates one via `create_next_extent()`.
    ///
    /// Requires `&mut self` because it modifies the extent list.
    pub fn seal(
        &mut self,
        extent_id: ExtentId,
        committed_offset: Option<u64>,
    ) -> Option<(u64, u64)> {
        let last = self.extents.last()?;
        if last.id != extent_id {
            return None;
        }
        if last.state() == ExtentState::Sealed {
            return None;
        }
        let start_offset = last.start_offset.0;
        let end_offset = last.seal(committed_offset);
        Some((start_offset, end_offset))
    }

    /// Autonomously create the next extent on extent-full (Primary only, within same epoch).
    ///
    /// The new extent uses the same arena capacity and starts at the sealed extent's end_offset.
    /// Extent ID is incremented locally — no SM round-trip needed.
    ///
    /// Returns `(new_extent_id, start_offset)` of the created extent.
    pub fn create_next_extent(&mut self) -> (ExtentId, Offset) {
        let end_offset = self
            .extents
            .last()
            .map(|e| Offset(e.start_offset.0 + e.message_count()))
            .unwrap_or(Offset(0));
        let new_id = self.next_extent_id;
        self.next_extent_id = ExtentId(new_id.0 + 1);
        self.extents.push(Extent::with_capacity(
            new_id,
            end_offset,
            self.arena_capacity,
            self.epoch,
        ));
        (new_id, end_offset)
    }

    /// Current epoch of this stream.
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// Update the epoch (e.g., when RegisterExtent arrives for an already lazily-created extent).
    pub fn set_epoch(&mut self, epoch: Epoch) {
        self.epoch = epoch;
    }

    /// Report extents for this stream that belong to the specified epoch.
    ///
    /// During recovery, SM only cares about extents created in the specified epoch
    /// (extents from prior epochs are already reconciled in MySQL metadata).
    /// Filters by per-extent epoch, so only extents actually created under the
    /// requested epoch are returned.
    pub fn report_extents(&self, epoch: Epoch) -> Vec<(ExtentId, Offset, u64, ExtentState)> {
        self.extents
            .iter()
            .filter(|e| e.epoch == epoch)
            .map(|e| {
                let end_offset = if e.is_sealed() {
                    e.start_offset.0 + e.message_count()
                } else {
                    0 // active extent, end_offset not yet determined
                };
                (e.id, e.start_offset, end_offset, e.state())
            })
            .collect()
    }

    /// The end_offset of the specified sealed extent.
    /// Used by handle_seal to return committed offset idempotently when the
    /// extent was already sealed (e.g., primary already sealed via extent-full path).
    /// Returns 0 if the extent is not found or not sealed.
    pub fn sealed_end_offset(&self, extent_id: ExtentId) -> u64 {
        if let Some(extent) = self.find_extent(extent_id)
            && extent.is_sealed()
        {
            return extent.start_offset.0 + extent.message_count();
        }
        0
    }

    /// Find an extent by its ID.
    pub fn find_extent(&self, extent_id: ExtentId) -> Option<&Extent> {
        self.extents.iter().find(|e| e.id == extent_id)
    }

    /// Return the stream-level in_flight counter (for pipelined group commit).
    pub(crate) fn in_flight(&self) -> &AtomicU64 {
        &self.in_flight
    }

    /// Return a reference to the job sender channel.
    pub(crate) fn job_tx(&self) -> &Sender<AppendJob> {
        &self.job_tx
    }

    /// Return a reference to the job receiver channel.
    pub(crate) fn job_rx(&self) -> &Receiver<AppendJob> {
        &self.job_rx
    }

    /// Append to the active extent (single-writer, called by stream-level leader).
    ///
    /// Returns `Ok((result, extent_id))` on success, or `Err(ExtentFull)` when the
    /// caller should seal + create + retry.
    pub fn try_append_active(
        &self,
        payload: Bytes,
    ) -> Result<(AppendResult, ExtentId), StorageError> {
        let extent = self.extents.last().ok_or_else(|| {
            StorageError::Internal(format!("stream {}: no active extent", self.id))
        })?;
        let result = extent.append_inner(payload)?;
        Ok((result, extent.id))
    }

    /// Seal the active extent and create a new one. Returns seal notification.
    ///
    /// Must be called under exclusive access (`&mut self`, i.e., DashMap `get_mut`).
    pub fn seal_and_create_next(&mut self) -> Option<SealNotification> {
        let active_id = self.active_extent_id()?;
        let (_, end_offset) = self.seal(active_id, None)?;
        let (new_id, _) = self.create_next_extent();
        Some(SealNotification {
            sealed_extent_id: active_id,
            end_offset,
            new_extent_id: new_id,
            epoch: self.epoch,
        })
    }
}

/// Information about an extent that was sealed during an append.
#[derive(Debug, Clone)]
pub struct SealNotification {
    pub sealed_extent_id: ExtentId,
    pub end_offset: u64,
    pub new_extent_id: ExtentId,
    pub epoch: Epoch,
}

impl std::fmt::Debug for Stream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Stream")
            .field("id", &self.id)
            .field("extents", &self.extents)
            .field("epoch", &self.epoch)
            .field("next_extent_id", &self.next_extent_id)
            .field("arena_capacity", &self.arena_capacity)
            .field("in_flight", &self.in_flight.load(Ordering::Relaxed))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extent::DEFAULT_ARENA_CAPACITY;

    /// Helper: create a stream with one active extent (simulating RegisterExtent from SM).
    fn new_stream_with_extent(id: StreamId) -> Stream {
        let mut stream = Stream::new(id);
        stream.register_extent(ExtentId(0), Offset(0), DEFAULT_ARENA_CAPACITY, Epoch(0));
        stream
    }

    #[test]
    fn basic_append_and_read() {
        let stream = new_stream_with_extent(StreamId(1));
        let extent_id = ExtentId(0);
        let r0 = stream
            .append(extent_id, Bytes::from_static(b"msg0"))
            .unwrap();
        let r1 = stream
            .append(extent_id, Bytes::from_static(b"msg1"))
            .unwrap();
        let r2 = stream
            .append(extent_id, Bytes::from_static(b"msg2"))
            .unwrap();

        assert_eq!(r0.offset, Offset(0));
        assert_eq!(r1.offset, Offset(1));
        assert_eq!(r2.offset, Offset(2));
        assert_eq!(stream.max_offset(), Offset(3));

        // Read all 3 from offset 0.
        let msgs = stream.read(extent_id, Offset(0), 3).unwrap();
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0], Bytes::from_static(b"msg0"));
        assert_eq!(msgs[2], Bytes::from_static(b"msg2"));

        // Random access: read msg1 directly via its offset.
        let msgs = stream.read(extent_id, r1.offset, 1).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0], Bytes::from_static(b"msg1"));
    }

    #[test]
    fn read_from_offset() {
        let stream = new_stream_with_extent(StreamId(1));
        let extent_id = ExtentId(0);
        let mut results = Vec::new();
        for i in 0..10 {
            results.push(
                stream
                    .append(extent_id, Bytes::from(format!("msg{i}")))
                    .unwrap(),
            );
        }

        // Read 3 messages starting at offset 5.
        let r5 = &results[5];
        let msgs = stream.read(extent_id, r5.offset, 3).unwrap();
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0], Bytes::from("msg5"));
        assert_eq!(msgs[1], Bytes::from("msg6"));
        assert_eq!(msgs[2], Bytes::from("msg7"));
    }

    #[test]
    fn read_beyond_end_returns_available() {
        let stream = new_stream_with_extent(StreamId(1));
        let extent_id = ExtentId(0);
        let r = stream
            .append(extent_id, Bytes::from_static(b"only"))
            .unwrap();

        let msgs = stream.read(extent_id, r.offset, 100).unwrap();
        assert_eq!(msgs.len(), 1);
    }

    #[test]
    fn read_empty_stream() {
        let stream = Stream::new(StreamId(1));
        assert_eq!(stream.max_offset(), Offset(0));

        // Stream with no extents: read returns error (extent not found).
        let result = stream.read(ExtentId(0), Offset(0), 10);
        assert!(result.is_err());
    }

    #[test]
    fn empty_stream_properties() {
        let stream = Stream::new(StreamId(1));
        assert_eq!(stream.max_offset(), Offset(0));
        assert!(!stream.is_mutable());
        assert_eq!(stream.active_extent_id(), None);
        assert!(
            stream
                .append(ExtentId(0), Bytes::from_static(b"fail"))
                .is_err()
        );
    }

    #[test]
    fn seal_and_new() {
        let mut stream = new_stream_with_extent(StreamId(1));
        let first_extent_id = ExtentId(0);
        // Append 3 messages to first extent.
        for i in 0..3 {
            stream
                .append(first_extent_id, Bytes::from(format!("msg{i}")))
                .unwrap();
        }
        assert_eq!(stream.max_offset(), Offset(3));

        // Seal active extent.
        let (start_offset, end_offset) = stream.seal(first_extent_id, None).unwrap();
        assert_eq!(start_offset, 0);
        assert_eq!(end_offset, 3);

        // After seal, stream has no active extent until register_extent.
        assert!(!stream.is_mutable());

        // Register a new extent (simulating SM sending RegisterExtent).
        let second_extent_id = ExtentId(1);
        stream.register_extent(
            second_extent_id,
            Offset(3),
            DEFAULT_ARENA_CAPACITY,
            Epoch(0),
        );
        assert!(stream.is_mutable());
        assert_eq!(stream.max_offset(), Offset(3)); // new extent is empty

        // Append to the new extent.
        let r = stream
            .append(second_extent_id, Bytes::from_static(b"after-seal"))
            .unwrap();
        assert_eq!(r.offset, Offset(3));
        assert_eq!(r.byte_pos, 0); // new extent, byte_pos starts at 0
        assert_eq!(stream.max_offset(), Offset(4));

        // Read from the new extent.
        let msgs = stream.read(second_extent_id, r.offset, 1).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0], Bytes::from_static(b"after-seal"));
    }

    #[test]
    fn seal_already_sealed_returns_none() {
        let mut stream = new_stream_with_extent(StreamId(1));
        let first_extent_id = ExtentId(0);
        let r = stream
            .append(first_extent_id, Bytes::from_static(b"a"))
            .unwrap();
        assert_eq!(r.offset, Offset(0));
        stream.seal(first_extent_id, None); // seals extent with 1 msg
        assert_eq!(stream.seal(first_extent_id, None), None); // already sealed, returns None

        // Register a new extent and append.
        let second_extent_id = ExtentId(1);
        stream.register_extent(
            second_extent_id,
            Offset(1),
            DEFAULT_ARENA_CAPACITY,
            Epoch(0),
        );
        let r = stream
            .append(second_extent_id, Bytes::from_static(b"b"))
            .unwrap();
        assert_eq!(r.offset, Offset(1));
        assert_eq!(stream.max_offset(), Offset(2));
    }
}
