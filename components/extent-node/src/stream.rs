use bytes::Bytes;
use common::errors::StorageError;
use common::types::{ExtentId, ExtentState, Offset, StreamId};

use crate::extent::{AppendResult, Extent, DEFAULT_ARENA_CAPACITY};

/// A stream: an ordered, append-only sequence of messages backed by a list of extents.
///
/// The active (last) extent is a lock-free arena. Multiple concurrent appenders
/// can write to it without any external mutex -- offset assignment, payload copy,
/// and commit advancement are all handled by the Extent's internal atomics.
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
    pub fn append(&self, payload: Bytes) -> Result<AppendResult, StorageError> {
        let active = self
            .extents
            .last()
            .expect("stream always has at least one extent");
        active.append(payload)
    }

    /// Read `count` messages starting from the given byte position within the
    /// extent that contains `offset`.
    ///
    /// The caller provides a logical `offset` to locate the correct extent,
    /// and a `byte_pos` to seek directly within the extent's arena.
    /// This enables O(1) random access when the caller has an external index.
    ///
    /// If `byte_pos` is 0, this reads from the beginning of the extent.
    pub fn read(&self, offset: Offset, byte_pos: u64, count: u32) -> Result<Vec<Bytes>, StorageError> {
        for extent in &self.extents {
            // Skip extents entirely before our read range.
            if extent.next_offset().0 <= offset.0 {
                continue;
            }

            // Skip extents entirely after our read range (shouldn't happen with ordered extents).
            if offset.0 < extent.base_offset.0 {
                break;
            }

            // Found the extent — read directly at the given byte position.
            return extent.read(byte_pos, count);
        }

        Ok(Vec::new())
    }

    /// Whether this stream can accept appends (its last extent is active/unsealed).
    pub fn is_mutable(&self) -> bool {
        self.extents.last()
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
    /// Returns `(base_offset, message_count)` of the sealed extent, or None if no active extent.
    ///
    /// The committed offset for the sealed extent is `base_offset + message_count`.
    ///
    /// Requires `&mut self` because it modifies the extent list.
    pub fn seal_active(&mut self) -> Option<(u64, u64)> {
        let last = self.extents.last()?;
        if last.state() == ExtentState::Sealed {
            return None;
        }
        let base_offset = last.base_offset.0;
        let message_count = last.message_count();
        last.seal();

        // Create a new active extent starting at the next offset.
        let new_base = Offset(base_offset + message_count);
        let new_id = ExtentId(self.next_extent_id);
        self.next_extent_id += 1;
        self.extents.push(Extent::with_capacity(new_id, new_base, self.arena_capacity));

        Some((base_offset, message_count))
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

        // Read all 3 from byte_pos 0.
        let msgs = stream.read(Offset(0), r0.byte_pos, 3).unwrap();
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0], Bytes::from_static(b"msg0"));
        assert_eq!(msgs[2], Bytes::from_static(b"msg2"));

        // Random access: read msg1 directly via its byte_pos.
        let msgs = stream.read(r1.offset, r1.byte_pos, 1).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0], Bytes::from_static(b"msg1"));
    }

    #[test]
    fn read_from_byte_pos() {
        let stream = Stream::new(StreamId(1));
        let mut results = Vec::new();
        for i in 0..10 {
            results.push(
                stream
                    .append(Bytes::from(format!("msg{i}")))
                    .unwrap(),
            );
        }

        // Read 3 messages starting at record 5's byte_pos.
        let r5 = &results[5];
        let msgs = stream.read(r5.offset, r5.byte_pos, 3).unwrap();
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0], Bytes::from("msg5"));
        assert_eq!(msgs[1], Bytes::from("msg6"));
        assert_eq!(msgs[2], Bytes::from("msg7"));
    }

    #[test]
    fn read_beyond_end_returns_available() {
        let stream = Stream::new(StreamId(1));
        let r = stream.append(Bytes::from_static(b"only")).unwrap();

        let msgs = stream.read(r.offset, r.byte_pos, 100).unwrap();
        assert_eq!(msgs.len(), 1);
    }

    #[test]
    fn read_empty_stream() {
        let stream = Stream::new(StreamId(1));
        assert_eq!(stream.max_offset(), Offset(0));

        let msgs = stream.read(Offset(0), 0, 10).unwrap();
        assert!(msgs.is_empty());
    }

    #[test]
    fn seal_and_new() {
        let mut stream = Stream::new(StreamId(1));
        // Append 3 messages to first extent.
        let mut results = Vec::new();
        for i in 0..3 {
            results.push(stream.append(Bytes::from(format!("msg{i}"))).unwrap());
        }
        assert_eq!(stream.max_offset(), Offset(3));

        // Seal active extent.
        let (base_offset, count) = stream.seal_active().unwrap();
        assert_eq!(base_offset, 0);
        assert_eq!(count, 3);

        // A new extent should exist with base_offset = 3.
        assert_eq!(stream.max_offset(), Offset(3)); // new extent is empty

        // Append to the new extent.
        let r = stream.append(Bytes::from_static(b"after-seal")).unwrap();
        assert_eq!(r.offset, Offset(3));
        assert_eq!(r.byte_pos, 0); // new extent, byte_pos starts at 0
        assert_eq!(stream.max_offset(), Offset(4));

        // Read from the new extent.
        let msgs = stream.read(r.offset, r.byte_pos, 1).unwrap();
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

    #[test]
    fn is_mutable_new_stream() {
        let stream = Stream::new(StreamId(1));
        assert!(stream.is_mutable());
    }

    #[test]
    fn is_mutable_after_seal_and_new() {
        let mut stream = Stream::new(StreamId(1));
        stream.append(Bytes::from_static(b"msg")).unwrap();
        stream.seal_active();
        // After seal_active, a new active extent is created, so still mutable.
        assert!(stream.is_mutable());
    }

    #[test]
    fn active_extent_id_changes_after_seal() {
        let mut stream = Stream::new(StreamId(1));
        assert_eq!(stream.active_extent_id(), ExtentId(0));

        stream.seal_active();
        assert_eq!(stream.active_extent_id(), ExtentId(1));

        stream.seal_active();
        assert_eq!(stream.active_extent_id(), ExtentId(2));
    }

    #[test]
    fn with_capacity_uses_custom_arena_size() {
        let stream = Stream::with_capacity(StreamId(1), 1024);
        // Should be able to append, but with smaller arena.
        stream.append(Bytes::from_static(b"test")).unwrap();
        assert_eq!(stream.max_offset(), Offset(1));
    }

    #[test]
    fn multiple_seal_and_new_cycles() {
        let mut stream = Stream::with_capacity(StreamId(1), 4096);

        // Cycle 1: append 5 messages, seal.
        for i in 0..5 {
            let r = stream.append(Bytes::from(format!("c1-{i}"))).unwrap();
            assert_eq!(r.offset, Offset(i as u64));
        }
        let (base, count) = stream.seal_active().unwrap();
        assert_eq!(base, 0);
        assert_eq!(count, 5);

        // Cycle 2: append 3 messages, seal.
        for i in 0..3 {
            let r = stream.append(Bytes::from(format!("c2-{i}"))).unwrap();
            assert_eq!(r.offset, Offset(5 + i as u64));
        }
        let (base, count) = stream.seal_active().unwrap();
        assert_eq!(base, 5);
        assert_eq!(count, 3);

        // Cycle 3: verify new extent starts at offset 8.
        let r = stream.append(Bytes::from_static(b"c3")).unwrap();
        assert_eq!(r.offset, Offset(8));
        assert_eq!(stream.max_offset(), Offset(9));
    }

    #[test]
    fn read_across_extent_boundary_returns_from_correct_extent() {
        let mut stream = Stream::with_capacity(StreamId(1), 4096);

        // First extent: offsets 0, 1.
        stream.append(Bytes::from_static(b"first-0")).unwrap();
        stream.append(Bytes::from_static(b"first-1")).unwrap();
        stream.seal_active();

        // Second extent: offset 2 starts at byte_pos 0.
        let r = stream.append(Bytes::from_static(b"second-0")).unwrap();
        assert_eq!(r.offset, Offset(2));
        assert_eq!(r.byte_pos, 0);

        // Reading offset 2 from the second extent should work.
        let msgs = stream.read(Offset(2), 0, 1).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0], Bytes::from_static(b"second-0"));
    }
}
