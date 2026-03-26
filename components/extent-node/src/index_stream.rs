use std::sync::atomic::{AtomicU64, Ordering};

/// Sentinel for unwritten index entries.
const INDEX_UNSET: u64 = u64::MAX;

/// Lock-free index for a single extent. Maps sequence number → byte_pos.
///
/// Entry `i` holds the byte_pos for the `i`-th record appended to the
/// corresponding data extent. The logical offset is implicit:
/// `offset = extent.start_offset + i`.
///
/// Writers call [`record()`] atomically after each append. Readers call
/// [`lookup()`] to resolve a sequence number to its byte position.
///
/// Capacity is sized to the maximum number of records an extent can hold,
/// calculated from `arena_capacity / MIN_RECORD_SIZE` where the minimum
/// record is 4 bytes header + 1 byte payload = 5 bytes.
pub struct IndexExtent {
    /// Packed array of byte positions. Entry `i` is written atomically
    /// by the same thread that wrote record `i` to the data extent.
    entries: Box<[AtomicU64]>,
}

impl IndexExtent {
    /// Create a new index extent with the given capacity (max number of records).
    pub fn new(capacity: usize) -> Self {
        let mut entries = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            entries.push(AtomicU64::new(INDEX_UNSET));
        }
        Self {
            entries: entries.into_boxed_slice(),
        }
    }

    /// Record the byte_pos for sequence number `seq`. Lock-free.
    ///
    /// Called by the writer thread immediately after appending a record to the
    /// data extent. `seq` is the zero-based sequence within this extent (not
    /// the global logical offset).
    pub fn record(&self, seq: u64, byte_pos: u64) {
        if (seq as usize) < self.entries.len() {
            self.entries[seq as usize].store(byte_pos, Ordering::Release);
        }
    }

    /// Lookup the byte_pos for sequence number `seq`.
    ///
    /// Returns `None` if `seq` is out of bounds or the entry has not been
    /// committed yet (still holds the sentinel value).
    pub fn lookup(&self, seq: u64) -> Option<u64> {
        let idx = seq as usize;
        if idx >= self.entries.len() {
            return None;
        }
        let val = self.entries[idx].load(Ordering::Acquire);
        if val == INDEX_UNSET {
            None
        } else {
            Some(val)
        }
    }
}

impl std::fmt::Debug for IndexExtent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexExtent")
            .field("capacity", &self.entries.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_record_and_lookup() {
        let idx = IndexExtent::new(100);

        // Initially, all entries are unset.
        assert_eq!(idx.lookup(0), None);
        assert_eq!(idx.lookup(99), None);

        // Record some entries.
        idx.record(0, 0);
        idx.record(1, 8);
        idx.record(2, 16);

        assert_eq!(idx.lookup(0), Some(0));
        assert_eq!(idx.lookup(1), Some(8));
        assert_eq!(idx.lookup(2), Some(16));

        // Unwritten entries still return None.
        assert_eq!(idx.lookup(3), None);
    }

    #[test]
    fn out_of_bounds_returns_none() {
        let idx = IndexExtent::new(10);
        assert_eq!(idx.lookup(10), None);
        assert_eq!(idx.lookup(100), None);

        // Recording out-of-bounds is a no-op (doesn't panic).
        idx.record(10, 999);
        idx.record(100, 999);
    }

    #[test]
    fn concurrent_record_and_lookup() {
        use std::sync::Arc;
        use std::thread;

        let idx = Arc::new(IndexExtent::new(10_000));
        let num_threads = 8;
        let records_per_thread = 1000;

        // Each thread records a disjoint range of sequence numbers.
        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let idx = Arc::clone(&idx);
                thread::spawn(move || {
                    let base = t * records_per_thread;
                    for i in 0..records_per_thread {
                        let seq = (base + i) as u64;
                        let byte_pos = seq * 10; // deterministic mapping
                        idx.record(seq, byte_pos);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // Verify all entries.
        for seq in 0..(num_threads * records_per_thread) {
            let byte_pos = idx.lookup(seq as u64).expect(&format!("seq {} should be set", seq));
            assert_eq!(byte_pos, seq as u64 * 10);
        }
    }
}
