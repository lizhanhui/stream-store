use crate::types::{ExtentId, Offset, StreamId};

/// Errors returned by the storage system.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid frame: {0}")]
    InvalidFrame(String),

    #[error("unknown opcode: 0x{0:02X}")]
    UnknownOpcode(u8),

    #[error("unknown stream: {0:?}")]
    UnknownStream(StreamId),

    #[error("invalid offset: stream {stream:?}, requested {requested:?}, max {max:?}")]
    InvalidOffset {
        stream: StreamId,
        requested: Offset,
        max: Offset,
    },

    #[error("extent sealed: {0:?}")]
    ExtentSealed(ExtentId),

    #[error("extent full: {0:?}")]
    ExtentFull(ExtentId),

    #[error("internal error: {0}")]
    Internal(String),
}
