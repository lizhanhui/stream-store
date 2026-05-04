//! Arena-level building blocks backing StreamEpoch.
//!
//! `Arena` is the byte-pool primitive: one buffer + one directory +
//! single-writer cursors. Dedicated streams own a per-epoch vector of
//! `Arc<Arena>` (rotated on arena-full); Shared streams (P3) observe
//! arenas minted by a process-wide `SharedArenaPool`.

mod arena;
mod buffer;
mod directory;
mod id;
mod pool;
mod write_batch;

pub(crate) use arena::Arena;
pub(crate) use buffer::{ArenaBuffer, OwnedArenaSlice};
pub(crate) use directory::{ArenaDirectory, EpochArenaEntry};
pub(crate) use id::{ArenaId, ArenaIdGenerator, node_prefix_from_id};
pub(crate) use pool::{ArenaPool, DedicatedArenaPool, SharedArenaPool};
pub(crate) use write_batch::{ArenaAppendResult, WriteBatch, WriteBatchJob};
