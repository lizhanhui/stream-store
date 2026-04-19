//! Identity hasher for u32-keyed concurrent maps.
//!
//! `StreamId` is a newtype over `u32` — sequential, non-adversarial, small-space.
//! SipHash (the default) spends ~15 ns per hash to defend against hash-flooding
//! DoS, which is irrelevant for internal data structures keyed by server-assigned
//! IDs.  The identity hasher uses the `u32` value directly as the hash,
//! eliminating all hashing overhead on the hot append path.

use std::hash::{BuildHasher, Hasher};

/// A [`Hasher`] that uses the last 8 bytes written as the hash value directly.
///
/// Only valid for keys that write exactly 4 bytes (u32) or 8 bytes (u64).
/// Using this with variable-length or multi-field keys would produce collisions.
#[derive(Default)]
pub struct IdentityHasher(u64);

impl Hasher for IdentityHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        // For u32: 4 bytes, zero-extended.
        // For u64: 8 bytes, used as-is.
        debug_assert!(
            bytes.len() <= 8,
            "IdentityHasher is only valid for ≤8-byte keys"
        );
        let mut buf = [0u8; 8];
        buf[..bytes.len()].copy_from_slice(bytes);
        self.0 = u64::from_ne_bytes(buf);
    }
}

/// [`BuildHasher`] that produces [`IdentityHasher`] instances.
///
/// Use with `papaya::HashMap<StreamId, V, IdentityBuildHasher>` to eliminate
/// hashing overhead for u32-keyed lookups on the hot path.
#[derive(Clone, Default)]
pub struct IdentityBuildHasher;

impl BuildHasher for IdentityBuildHasher {
    type Hasher = IdentityHasher;

    #[inline]
    fn build_hasher(&self) -> IdentityHasher {
        IdentityHasher(0)
    }
}
