use snafu::prelude::*;
use snafu_virtstack::stack_trace_debug;

use crate::types::{Epoch, ExtentId, Offset, StreamId};

/// Errors returned by the storage system.
///
/// Uses SNAFU with virtual stack traces: each variant carries an implicit
/// `location` field that captures file:line:column at the `.context()` call site.
/// When errors propagate through multiple layers, `#[stack_trace_debug]` formats
/// the full propagation path — no system backtrace overhead.
#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum StorageError {
    // ── Protocol / frame errors ──────────────────────────────────────────
    #[snafu(display("invalid frame: {message}"))]
    InvalidFrame {
        message: String,
        #[snafu(implicit)]
        location: snafu::Location,
    },

    #[snafu(display("unknown opcode: 0x{opcode:02X}"))]
    UnknownOpcode {
        opcode: u8,
        #[snafu(implicit)]
        location: snafu::Location,
    },

    // ── Domain errors ────────────────────────────────────────────────────
    #[snafu(display("unknown stream: {stream_id}"))]
    UnknownStream {
        stream_id: StreamId,
        #[snafu(implicit)]
        location: snafu::Location,
    },

    #[snafu(display("invalid offset: stream {stream_id}, requested {requested}, max {max}"))]
    InvalidOffset {
        stream_id: StreamId,
        requested: Offset,
        max: Offset,
        #[snafu(implicit)]
        location: snafu::Location,
    },

    #[snafu(display("extent sealed: {extent_id}"))]
    ExtentSealed {
        extent_id: ExtentId,
        #[snafu(implicit)]
        location: snafu::Location,
    },

    #[snafu(display("extent full: {extent_id}"))]
    ExtentFull {
        extent_id: ExtentId,
        #[snafu(implicit)]
        location: snafu::Location,
    },

    #[snafu(display("epoch stale: stream {stream_id}, epoch {epoch}"))]
    EpochStale {
        stream_id: StreamId,
        epoch: Epoch,
        #[snafu(implicit)]
        location: snafu::Location,
    },

    // ── Database errors (wraps sqlx::Error) ──────────────────────────────
    #[snafu(display("database error: {message}"))]
    Database {
        message: String,
        source: sqlx::Error,
        #[snafu(implicit)]
        location: snafu::Location,
    },

    // ── Migration errors (refinery / mysql_async — heterogeneous sources) ─
    #[snafu(display("migration error: {message}"))]
    Migration {
        message: String,
        #[snafu(implicit)]
        location: snafu::Location,
    },

    // ── Network / connection errors ──────────────────────────────────────
    #[snafu(display("network error: {message}"))]
    Network {
        message: String,
        #[snafu(implicit)]
        location: snafu::Location,
    },

    // ── I/O errors (wraps std::io::Error) ────────────────────────────────
    #[snafu(display("I/O error: {message}"))]
    Io {
        message: String,
        source: std::io::Error,
        #[snafu(implicit)]
        location: snafu::Location,
    },

    // ── Catch-all for truly unclassifiable errors ────────────────────────
    #[snafu(display("{message}"))]
    Internal {
        message: String,
        #[snafu(implicit)]
        location: snafu::Location,
    },
}

/// Required by `tokio_util::codec::Decoder::Error` and `Encoder::Error`.
/// Wraps `io::Error` into the `Io` variant with a generic message.
impl From<std::io::Error> for StorageError {
    #[track_caller]
    fn from(source: std::io::Error) -> Self {
        let message = source.to_string();
        StorageError::Io {
            message,
            source,
            location: snafu::GenerateImplicitData::generate(),
        }
    }
}
