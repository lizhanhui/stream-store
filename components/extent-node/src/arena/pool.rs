//! Arena allocation abstraction.
//!
//! P2 keeps the shared-arena pool type as a stub so P3 can wire routing without
//! reintroducing the module. Dedicated streams allocate epochs directly in
//! `Stream::register_epoch` for now.

use std::sync::Arc;

use crate::arena::ArenaIdGenerator;

/// Stub for the future EN-wide shared-arena pool. Routing is not wired
/// in P2; any caller that lands here signals a bug in stream setup.
#[allow(dead_code)]
pub(crate) struct SharedArenaPool {
    _arena_size: u32,
    _generator: Arc<ArenaIdGenerator>,
}

impl SharedArenaPool {
    #[allow(dead_code)]
    pub(crate) fn new(arena_size: u32, generator: Arc<ArenaIdGenerator>) -> Self {
        Self {
            _arena_size: arena_size,
            _generator: generator,
        }
    }
}
