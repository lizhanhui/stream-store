//! Arena-level building blocks used by StreamEpoch (née Extent).
//!
//! Extracted from extent.rs so the same primitives can back both
//! Dedicated (one stream per arena) and Shared (many streams per arena)
//! pools. Shared pools land in a later plan; this module currently
//! only exposes ArenaBuffer + OwnedArenaSlice.

mod buffer;

pub(crate) use buffer::{ArenaBuffer, OwnedArenaSlice};
