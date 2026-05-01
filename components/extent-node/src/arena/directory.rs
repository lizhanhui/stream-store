//! Per-arena record-placement directory.
//!
//! Factored out of extent.rs in P2. The per-entry byte_positions table
//! retains the compressed-u32 sentinel encoding (store byte_pos + 1 so
//! slot == 0 means unset) so alloc_zeroed fast init continues to work.
//!
//! P2 always holds exactly one EpochArenaEntry per directory (Dedicated:
//! one stream, one epoch per arena). A later plan will widen the
//! directory to a HashMap<(StreamId, Epoch), EpochArenaEntry> so Shared
//! arenas can multiplex many streams into one buffer.

use std::alloc::{Layout, alloc_zeroed, handle_alloc_error};
use std::fmt;
use std::sync::atomic::{AtomicU32, Ordering};

use common::types::{Epoch, Offset, StreamId};

/// Sentinel stored in a slot that has not yet been written.
///
/// We store `byte_pos + 1` in the slot so slot == 0 means unset. This
/// lets `alloc_zeroed` (backed by MAP_ANONYMOUS zero pages on Linux)
/// initialize the table at near-zero cost — critical for large arenas
/// where iterating record-capacity entries caused ~80ms stalls in
/// practice.
pub(crate) const SLOT_UNSET: u32 = 0;

/// Per-(stream, epoch) record placement inside one arena.
///
/// Internally a flat table keyed by the record's sequence number within
/// the entry (offset - start_offset). Entry i stores `byte_pos + 1`;
/// slot == 0 is the unset sentinel.
pub(crate) struct EpochArenaEntry {
    pub(crate) stream_id:    StreamId,
    pub(crate) epoch:        Epoch,
    pub(crate) start_offset: Offset,

    // Capacity = arena_capacity / MIN_RECORD_SIZE (same value used by
    // today's Extent.index). Allocated via alloc_zeroed so initial
    // state is entirely SLOT_UNSET.
    byte_positions: Box<[AtomicU32]>,
}

impl EpochArenaEntry {
    /// Allocate a fresh entry with `record_cap` slots, all set to SLOT_UNSET.
    pub(crate) fn with_capacity(
        stream_id:    StreamId,
        epoch:        Epoch,
        start_offset: Offset,
        record_cap:   usize,
    ) -> Self {
        // alloc_zeroed path, identical to today's Extent.index init.
        let byte_positions = {
            let layout = Layout::from_size_align(
                record_cap * std::mem::size_of::<AtomicU32>(),
                std::mem::align_of::<AtomicU32>(),
            )
            .expect("invalid directory layout");
            // SAFETY: layout valid, nonzero size. alloc_zeroed -> all-zero bytes.
            // AtomicU32 has the same layout as u32, and 0u32 == SLOT_UNSET.
            let ptr = unsafe { alloc_zeroed(layout) };
            if ptr.is_null() {
                handle_alloc_error(layout);
            }
            // SAFETY: ptr points to record_cap * 4 zeroed bytes; we
            // reconstruct a Vec<AtomicU32> of exactly that capacity so
            // the Box<[AtomicU32]> conversion is well-formed.
            unsafe {
                Vec::from_raw_parts(ptr as *mut AtomicU32, record_cap, record_cap)
            }
            .into_boxed_slice()
        };
        Self {
            stream_id,
            epoch,
            start_offset,
            byte_positions,
        }
    }

    /// Record `byte_pos` for record `seq` (relative to this entry's
    /// start_offset). Stores `byte_pos + 1` to distinguish from the
    /// unset sentinel. Out-of-bounds seqs are silently dropped — matches
    /// today's index_record.
    pub(crate) fn record(&self, seq: u64, byte_pos: u64) {
        let idx = seq as usize;
        if idx < self.byte_positions.len() {
            self.byte_positions[idx].store(byte_pos as u32 + 1, Ordering::Release);
        }
    }

    /// Look up `byte_pos` for record `seq`. Returns None if the slot
    /// is out of bounds or still holds the unset sentinel.
    pub(crate) fn lookup(&self, seq: u64) -> Option<u64> {
        let idx = seq as usize;
        let val = self.byte_positions.get(idx)?.load(Ordering::Acquire);
        if val == SLOT_UNSET {
            None
        } else {
            Some((val - 1) as u64)
        }
    }

    /// Total slot capacity. Used by the secondary replay path in
    /// try_advance_committed to bound its seq iteration.
    pub(crate) fn record_capacity(&self) -> usize {
        self.byte_positions.len()
    }

    /// Raw load of slot `seq`'s stored value WITHOUT decoding.
    ///
    /// Returns the raw u32 (the sentinel SLOT_UNSET or byte_pos + 1).
    /// Used by try_advance_committed where the decoded byte_pos is
    /// already needed plus a sentinel check in one read.
    pub(crate) fn raw_slot(&self, seq: u64) -> Option<u32> {
        let idx = seq as usize;
        Some(self.byte_positions.get(idx)?.load(Ordering::Acquire))
    }
}

/// Arena-level directory.
///
/// P2: exactly one entry per directory. P3+ will widen this to a
/// HashMap keyed by (StreamId, Epoch).
pub(crate) struct ArenaDirectory {
    entry: EpochArenaEntry,
}

impl fmt::Debug for EpochArenaEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EpochArenaEntry")
            .field("stream_id", &self.stream_id)
            .field("epoch", &self.epoch)
            .field("start_offset", &self.start_offset)
            .field("record_capacity", &self.byte_positions.len())
            .finish()
    }
}

impl ArenaDirectory {
    pub(crate) fn new(entry: EpochArenaEntry) -> Self {
        Self { entry }
    }

    /// Access the directory's single entry. Panics if called on a
    /// (future) multi-entry directory; P2 always has exactly one.
    pub(crate) fn single_entry(&self) -> &EpochArenaEntry {
        &self.entry
    }
}
