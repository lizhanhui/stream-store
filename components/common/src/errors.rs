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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_error_display() {
        let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pipe broken");
        let err = StorageError::Io(io_err);
        let msg = format!("{err}");
        assert!(msg.contains("pipe broken"), "got: {msg}");
    }

    #[test]
    fn invalid_frame_display() {
        let err = StorageError::InvalidFrame("bad header".to_string());
        assert_eq!(format!("{err}"), "invalid frame: bad header");
    }

    #[test]
    fn unknown_opcode_display_hex_formatting() {
        assert_eq!(
            format!("{}", StorageError::UnknownOpcode(0xAB)),
            "unknown opcode: 0xAB"
        );
        assert_eq!(
            format!("{}", StorageError::UnknownOpcode(0x00)),
            "unknown opcode: 0x00"
        );
        assert_eq!(
            format!("{}", StorageError::UnknownOpcode(0xFF)),
            "unknown opcode: 0xFF"
        );
    }

    #[test]
    fn unknown_stream_display() {
        let err = StorageError::UnknownStream(StreamId(42));
        let msg = format!("{err}");
        assert!(msg.contains("42"), "got: {msg}");
    }

    #[test]
    fn invalid_offset_display() {
        let err = StorageError::InvalidOffset {
            stream: StreamId(1),
            requested: Offset(100),
            max: Offset(50),
        };
        let msg = format!("{err}");
        assert!(msg.contains("100"), "should contain requested offset, got: {msg}");
        assert!(msg.contains("50"), "should contain max offset, got: {msg}");
    }

    #[test]
    fn extent_sealed_display() {
        let err = StorageError::ExtentSealed(ExtentId(7));
        let msg = format!("{err}");
        assert!(msg.contains("7"), "got: {msg}");
    }

    #[test]
    fn extent_full_display() {
        let err = StorageError::ExtentFull(ExtentId(99));
        let msg = format!("{err}");
        assert!(msg.contains("99"), "got: {msg}");
    }

    #[test]
    fn internal_error_display() {
        let err = StorageError::Internal("something went wrong".to_string());
        assert_eq!(format!("{err}"), "internal error: something went wrong");
    }

    #[test]
    fn io_error_from_conversion() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "not found");
        let storage_err: StorageError = io_err.into();
        assert!(matches!(storage_err, StorageError::Io(_)));
    }
}
