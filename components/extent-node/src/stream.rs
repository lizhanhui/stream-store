use bytes::Bytes;
use common::errors::StorageError;
use common::types::{ExtentId, ExtentState, Offset, StreamId};

use crate::extent::{AppendResult, Extent};

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
/// Stream-level mutation (`seal_active`, adding new extents) still requires `&mut self`
/// because these operations change the extent list. In the ExtentNodeStore, this is
/// handled at a higher level (DashMap per-stream write lock or equivalent).
#[derive(Debug)]
pub struct Stream {
    pub id: StreamId,
    extents: Vec<Extent>,
}

impl Stream {
    /// Create a new stream with no extents. Extents are added via `register_extent()`.
    pub fn new(id: StreamId) -> Self {
        Self {
            id,
            extents: Vec::new(),
        }
    }

    /// Register a new extent on this stream (called when SM sends RegisterExtent).
    pub fn register_extent(&mut self, id: ExtentId, start_offset: Offset, capacity: usize) {
        self.extents
            .push(Extent::with_capacity(id, start_offset, capacity));
    }

    /// Append a message to the active (last) extent. Returns the assigned
    /// offset and byte position within the extent arena.
    ///
    /// Only requires `&self` -- the Extent is internally synchronized (lock-free).
    /// The byte_pos is recorded in the extent's internal index automatically.
    ///
    /// Returns an error if there are no extents (no active extent registered yet).
    pub fn append(&self, payload: Bytes) -> Result<AppendResult, StorageError> {
        let active = self.extents.last().ok_or_else(|| {
            StorageError::Internal(format!("stream {:?} has no active extent", self.id))
        })?;
        active.append(payload)
    }

    /// Read `count` messages starting from the given logical `offset`.
    ///
    /// The server resolves `offset → byte_pos` internally via the index stream,
    /// so callers only need to provide the logical offset. This keeps byte_pos
    /// as an internal implementation detail invisible to clients.
    pub fn read(&self, offset: Offset, count: u32) -> Result<Vec<Bytes>, StorageError> {
        for extent in &self.extents {
            // Skip extents entirely before our read range.
            if extent.next_offset().0 <= offset.0 {
                continue;
            }

            // Skip extents entirely after our read range (shouldn't happen with ordered extents).
            if offset.0 < extent.start_offset.0 {
                break;
            }

            // Found the extent — resolve byte_pos via internal index lookup.
            let seq = offset.0 - extent.start_offset.0;
            let byte_pos = extent.index_lookup(seq).ok_or_else(|| {
                StorageError::Internal(format!("index lookup failed for offset {}", offset.0))
            })?;
            return extent.read(byte_pos, count);
        }

        Ok(Vec::new())
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

    /// Seal the active (last) extent.
    /// Returns `(start_offset, end_offset)` of the sealed extent, or None if no active extent.
    ///
    /// `end_offset` = `start_offset + message_count` (exclusive upper bound).
    ///
    /// If `committed_offset` is `Some`, it's the primary's committed offset propagated
    /// via SM. The sealed extent will accept late forwarded appends up to that offset.
    /// If `None`, the extent uses its local record_count (primary sealing itself).
    ///
    /// After seal, the stream has no active extent until SM sends a new `RegisterExtent`.
    ///
    /// Requires `&mut self` because it modifies the extent list.
    pub fn seal_active(&mut self, committed_offset: Option<u64>) -> Option<(u64, u64)> {
        let last = self.extents.last()?;
        if last.state() == ExtentState::Sealed {
            return None;
        }
        let start_offset = last.start_offset.0;
        let message_count = last.message_count();
        let end_offset = start_offset + message_count;
        last.seal(committed_offset);
        Some((start_offset, end_offset))
    }

    /// The end_offset of the most recently sealed extent.
    /// Used by handle_seal to return committed offset idempotently when the
    /// extent was already sealed (e.g., primary already sealed via extent-full path).
    pub fn sealed_end_offset(&self) -> u64 {
        for extent in self.extents.iter().rev() {
            if extent.is_sealed() {
                return extent.start_offset.0 + extent.message_count();
            }
        }
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extent::DEFAULT_ARENA_CAPACITY;

    /// Helper: create a stream with one active extent (simulating RegisterExtent from SM).
    fn new_stream_with_extent(id: StreamId) -> Stream {
        let mut stream = Stream::new(id);
        stream.register_extent(ExtentId(0), Offset(0), DEFAULT_ARENA_CAPACITY);
        stream
    }

    #[test]
    fn basic_append_and_read() {
        let stream = new_stream_with_extent(StreamId(1));
        let r0 = stream.append(Bytes::from_static(b"msg0")).unwrap();
        let r1 = stream.append(Bytes::from_static(b"msg1")).unwrap();
        let r2 = stream.append(Bytes::from_static(b"msg2")).unwrap();

        assert_eq!(r0.offset, Offset(0));
        assert_eq!(r1.offset, Offset(1));
        assert_eq!(r2.offset, Offset(2));
        assert_eq!(stream.max_offset(), Offset(3));

        // Read all 3 from offset 0.
        let msgs = stream.read(Offset(0), 3).unwrap();
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0], Bytes::from_static(b"msg0"));
        assert_eq!(msgs[2], Bytes::from_static(b"msg2"));

        // Random access: read msg1 directly via its offset.
        let msgs = stream.read(r1.offset, 1).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0], Bytes::from_static(b"msg1"));
    }

    #[test]
    fn read_from_offset() {
        let stream = new_stream_with_extent(StreamId(1));
        let mut results = Vec::new();
        for i in 0..10 {
            results.push(stream.append(Bytes::from(format!("msg{i}"))).unwrap());
        }

        // Read 3 messages starting at offset 5.
        let r5 = &results[5];
        let msgs = stream.read(r5.offset, 3).unwrap();
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0], Bytes::from("msg5"));
        assert_eq!(msgs[1], Bytes::from("msg6"));
        assert_eq!(msgs[2], Bytes::from("msg7"));
    }

    #[test]
    fn read_beyond_end_returns_available() {
        let stream = new_stream_with_extent(StreamId(1));
        let r = stream.append(Bytes::from_static(b"only")).unwrap();

        let msgs = stream.read(r.offset, 100).unwrap();
        assert_eq!(msgs.len(), 1);
    }

    #[test]
    fn read_empty_stream() {
        let stream = Stream::new(StreamId(1));
        assert_eq!(stream.max_offset(), Offset(0));

        // Stream with no extents: read returns empty.
        let msgs = stream.read(Offset(0), 10).unwrap();
        assert!(msgs.is_empty());
    }

    #[test]
    fn empty_stream_properties() {
        let stream = Stream::new(StreamId(1));
        assert_eq!(stream.max_offset(), Offset(0));
        assert!(!stream.is_mutable());
        assert_eq!(stream.active_extent_id(), None);
        assert!(stream.append(Bytes::from_static(b"fail")).is_err());
    }

    #[test]
    fn seal_and_new() {
        let mut stream = new_stream_with_extent(StreamId(1));
        // Append 3 messages to first extent.
        for i in 0..3 {
            stream.append(Bytes::from(format!("msg{i}"))).unwrap();
        }
        assert_eq!(stream.max_offset(), Offset(3));

        // Seal active extent.
        let (start_offset, end_offset) = stream.seal_active(None).unwrap();
        assert_eq!(start_offset, 0);
        assert_eq!(end_offset, 3);

        // After seal, stream has no active extent until register_extent.
        assert!(!stream.is_mutable());

        // Register a new extent (simulating SM sending RegisterExtent).
        stream.register_extent(ExtentId(1), Offset(3), DEFAULT_ARENA_CAPACITY);
        assert!(stream.is_mutable());
        assert_eq!(stream.max_offset(), Offset(3)); // new extent is empty

        // Append to the new extent.
        let r = stream.append(Bytes::from_static(b"after-seal")).unwrap();
        assert_eq!(r.offset, Offset(3));
        assert_eq!(r.byte_pos, 0); // new extent, byte_pos starts at 0
        assert_eq!(stream.max_offset(), Offset(4));

        // Read from the new extent.
        let msgs = stream.read(r.offset, 1).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0], Bytes::from_static(b"after-seal"));
    }

    #[test]
    fn seal_already_sealed_returns_none() {
        let mut stream = new_stream_with_extent(StreamId(1));
        let r = stream.append(Bytes::from_static(b"a")).unwrap();
        assert_eq!(r.offset, Offset(0));
        stream.seal_active(None); // seals extent with 1 msg
        assert_eq!(stream.seal_active(None), None); // already sealed, returns None

        // Register a new extent and append.
        stream.register_extent(ExtentId(1), Offset(1), DEFAULT_ARENA_CAPACITY);
        let r = stream.append(Bytes::from_static(b"b")).unwrap();
        assert_eq!(r.offset, Offset(1));
        assert_eq!(stream.max_offset(), Offset(2));
    }
}
