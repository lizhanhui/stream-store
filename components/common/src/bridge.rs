//! Transitional bridge: synthesize the legacy `ExtentId` from an `Epoch`.
//!
//! During the pre-P3 cleanup (see `docs/superpowers/plans/2026-05-02-pre-p3-cleanup.md`)
//! the wire protocol dropped the `extent_id` field from 24 VariableHeader variants,
//! but the EN runtime (`StreamEpoch` lookups, `stream_epochs` metadata rows, and
//! the `stream_sequence` minting path) still keys on `ExtentId`. This helper
//! produces the same `ExtentId` that `stream_sequence` would have minted for
//! the given epoch: `extent_id = epoch + 1`.
//!
//! **TODO(pre-P3 Phase 4):** delete this module along with every caller when
//! `Stream::with_extent` / `Stream::register_extent` / SM's `allocate_extent`
//! (+ `stream_sequence` table) are rewritten to key on `Epoch`.
use crate::types::{Epoch, ExtentId};

/// Synthesize the `ExtentId` corresponding to a given `Epoch`, using the
/// pre-cleanup allocator invariant `extent_id = epoch + 1`.
pub fn synth_extent_id(epoch: Epoch) -> ExtentId {
    ExtentId(epoch.0 + 1)
}
