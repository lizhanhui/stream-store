use bytes::Bytes;
use common::errors::StorageError;
use common::types::{ExtentId, ExtentState, Offset, StreamId};

use crate::extent::{AppendResult, DEFAULT_ARENA_CAPACITY, Extent};

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
    /// Next extent ID to allocate (local counter for seal-and-new).
    next_extent_id: u32,
    /// Arena capacity for new extents created during seal-and-new.
    arena_capacity: usize,
}

impl Stream {
    /// Create a new stream with a single active extent starting at offset 0
    /// and the default arena capacity (64 MiB).
    pub fn new(id: StreamId) -> Self {
        let extent = Extent::new(ExtentId(0), Offset(0));
        Self {
            id,
            extents: vec![extent],
            next_extent_id: 1,
            arena_capacity: DEFAULT_ARENA_CAPACITY,
        }
    }

    /// Create a new stream with a single active extent starting at offset 0
    /// and the specified arena capacity in bytes.
    pub fn with_capacity(id: StreamId, arena_capacity: usize) -> Self {
        let extent = Extent::with_capacity(ExtentId(0), Offset(0), arena_capacity);
        Self {
            id,
            extents: vec![extent],
            next_extent_id: 1,
            arena_capacity,
        }
    }

    /// Append a message to the active (last) extent. Returns the assigned
    /// offset and byte position within the extent arena.
    ///
    /// Only requires `&self` -- the Extent is internally synchronized (lock-free).
    /// The byte_pos is recorded in the extent's internal index automatically.
    pub fn append(&self, payload: Bytes) -> Result<AppendResult, StorageError> {
        let active = self
            .extents
            .last()
            .expect("stream always has at least one extent");
        active.append(payload)
    }

    /// Read `count` messages starting from the given logical `offset`.
    ///
    /// The server resolves `offset → byte_pos` internally via the index stream,
    /// so callers only need to provide the logical offset. This keeps byte_pos
    /// as an internal implementation detail invisible to clients.
    pub fn read(
        &self,
        offset: Offset,
        count: u32,
    ) -> Result<Vec<Bytes>, StorageError> {
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
            let byte_pos = extent
                .index_lookup(seq)
                .ok_or_else(|| StorageError::Internal(
                    format!("index lookup failed for offset {}", offset.0)
                ))?;
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

    /// The extent ID of the active (last) extent.
    pub fn active_extent_id(&self) -> ExtentId {
        self.extents
            .last()
            .expect("stream always has at least one extent")
            .id
    }

    /// The maximum offset (exclusive): the next offset that would be assigned.
    pub fn max_offset(&self) -> Offset {
        self.extents
            .last()
            .expect("stream always has at least one extent")
            .next_offset()
    }

    /// Seal the active (last) extent and create a new one.
    /// Returns `(start_offset, end_offset)` of the sealed extent, or None if no active extent.
    ///
    /// `end_offset` = `start_offset + message_count` (exclusive upper bound).
    ///
    /// Requires `&mut self` because it modifies the extent list.
    pub fn seal_active(&mut self) -> Option<(u64, u64)> {
        let last = self.extents.last()?;
        if last.state() == ExtentState::Sealed {
            return None;
        }
        let start_offset = last.start_offset.0;
        let message_count = last.message_count();
        let end_offset = start_offset + message_count;
        last.seal();

        // Create a new active extent starting at the end offset.
        let new_id = ExtentId(self.next_extent_id);
        self.next_extent_id += 1;
        self.extents
            .push(Extent::with_capacity(new_id, Offset(end_offset), self.arena_capacity));

        Some((start_offset, end_offset))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_append_and_read() {
        let stream = Stream::new(StreamId(1));
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
        let stream = Stream::new(StreamId(1));
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
        let stream = Stream::new(StreamId(1));
        let r = stream.append(Bytes::from_static(b"only")).unwrap();

        let msgs = stream.read(r.offset, 100).unwrap();
        assert_eq!(msgs.len(), 1);
    }

    #[test]
    fn read_empty_stream() {
        let stream = Stream::new(StreamId(1));
        assert_eq!(stream.max_offset(), Offset(0));

        let msgs = stream.read(Offset(0), 10).unwrap();
        assert!(msgs.is_empty());
    }

    #[test]
    fn seal_and_new() {
        let mut stream = Stream::new(StreamId(1));
        // Append 3 messages to first extent.
        for i in 0..3 {
            stream.append(Bytes::from(format!("msg{i}"))).unwrap();
        }
        assert_eq!(stream.max_offset(), Offset(3));

        // Seal active extent.
        let (start_offset, end_offset) = stream.seal_active().unwrap();
        assert_eq!(start_offset, 0);
        assert_eq!(end_offset, 3);

        // A new extent should exist with start_offset = 3.
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
        let mut stream = Stream::new(StreamId(1));
        let r = stream.append(Bytes::from_static(b"a")).unwrap();
        assert_eq!(r.offset, Offset(0));
        stream.seal_active(); // seals extent with 1 msg, creates new at offset 1
        stream.seal_active(); // seals empty extent, creates new at offset 1
        // Now append to the third extent.
        let r = stream.append(Bytes::from_static(b"b")).unwrap();
        assert_eq!(r.offset, Offset(1));
        assert_eq!(stream.max_offset(), Offset(2));
    }
}
