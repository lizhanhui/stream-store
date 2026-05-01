//! Arena-level building blocks used by Extent (to be renamed to
//! StreamEpoch in a later task).
//!
//! Extracted from extent.rs so the same primitives back both Dedicated
//! (one stream per arena) and Shared (many streams per arena, added in
//! a later plan) pools.

mod buffer;
mod directory;
mod id;
mod pool;
mod write_batch;

pub(crate) use buffer::{ArenaBuffer, OwnedArenaSlice};
pub(crate) use directory::{ArenaDirectory, EpochArenaEntry, SLOT_UNSET};
pub(crate) use id::{ArenaId, ArenaIdGenerator, node_prefix_from_id};
pub(crate) use pool::{ArenaPool, DedicatedArenaPool};
#[allow(unused_imports)]
pub(crate) use pool::SharedArenaPool;
#[allow(unused_imports)]
pub(crate) use write_batch::{JobResult, SharedAppendJob, WriteBatch, WriteBatchAck};
