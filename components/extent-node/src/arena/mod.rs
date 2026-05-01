//! Arena-level building blocks used by Extent (to be renamed to
//! StreamEpoch in a later task).
//!
//! Extracted from extent.rs so the same primitives back both Dedicated
//! (one stream per arena) and Shared (many streams per arena, added in
//! a later plan) pools.

mod buffer;
mod directory;

pub(crate) use buffer::{ArenaBuffer, OwnedArenaSlice};
pub(crate) use directory::{ArenaDirectory, EpochArenaEntry, SLOT_UNSET};
