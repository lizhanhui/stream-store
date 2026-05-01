//! Raw refcounted arena allocation + slice view.
//!
//! Moved out of extent.rs in P2 so ArenaBuffer can back both Dedicated
//! (one stream per arena) and Shared (many streams per arena) pools.

use std::alloc::{Layout, alloc, dealloc, handle_alloc_error};
use std::ops::Deref;
use std::ptr::NonNull;
use std::sync::Arc;

/// Owns the raw heap allocation for an arena's buffer.
/// Wrapped in `Arc` so that `Bytes` slices keep the buffer alive
/// even after the owning extent/epoch is dropped.
pub(crate) struct ArenaBuffer {
    ptr: NonNull<u8>,
    capacity: u32,
    layout: Layout,
}

// SAFETY: The raw allocation is exclusively managed by ArenaBuffer via Arc.
// No aliased mutable access is possible once shared.
unsafe impl Send for ArenaBuffer {}
unsafe impl Sync for ArenaBuffer {}

impl ArenaBuffer {
    /// Allocate a fresh buffer of `capacity` bytes, 8-byte aligned (matches
    /// the pre-P2 in-extent allocation exactly).
    pub(crate) fn new(capacity: u32) -> Arc<Self> {
        let layout = Layout::from_size_align(capacity as usize, 8).expect("invalid layout");
        // SAFETY: Layout is non-zero; handle_alloc_error on null.
        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            handle_alloc_error(layout);
        }
        Arc::new(Self {
            ptr: NonNull::new(ptr).unwrap(),
            capacity,
            layout,
        })
    }

    pub(crate) fn capacity(&self) -> u32 {
        self.capacity
    }

    /// Read-only view of the underlying allocation.
    pub(crate) fn ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    /// Writable pointer into the allocation.
    ///
    /// # Safety
    /// The caller must ensure at most one writer accesses the allocation
    /// at a time. The Extent / StreamEpoch single-leader invariant
    /// upholds this today.
    pub(crate) fn ptr_mut(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }
}

impl Drop for ArenaBuffer {
    fn drop(&mut self) {
        // SAFETY: ptr and layout were produced by alloc() in ArenaBuffer::new().
        unsafe {
            dealloc(self.ptr.as_ptr(), self.layout);
        }
    }
}

/// A reference-counted slice into an `ArenaBuffer`.
/// Implements `Deref<Target=[u8]>` so it can be passed to `Bytes::from_owner()`.
pub(crate) struct OwnedArenaSlice {
    pub(crate) _arena: Arc<ArenaBuffer>,
    pub(crate) ptr: *const u8,
    pub(crate) len: u32,
}

// SAFETY: The underlying memory is owned by Arc<ArenaBuffer> which is Send+Sync.
// The ptr/len describe an immutable view into that allocation.
unsafe impl Send for OwnedArenaSlice {}
unsafe impl Sync for OwnedArenaSlice {}

impl Deref for OwnedArenaSlice {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        // SAFETY: ptr is valid for len bytes as long as _arena is alive.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len as usize) }
    }
}

impl AsRef<[u8]> for OwnedArenaSlice {
    fn as_ref(&self) -> &[u8] {
        self.deref()
    }
}
