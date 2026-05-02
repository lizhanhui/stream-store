use std::sync::atomic::Ordering;

use common::errors::StorageError;
use common::types::{ArenaClass, Epoch, EpochPolicy, Offset, StreamId};
use rpc::frame::{Frame, VariableHeader};
use tracing::{info, warn};

use super::ExtentNodeStore;
use crate::stream::Stream;
use crate::stream_epoch::AppendResult;

impl ExtentNodeStore {
    /// Check if a Forward or ForwardChecksum frame targets an extent that
    /// needs ForwardInitEpoch. Returns the init frame if so.
    ///
    /// Called on the leader side before pushing to the channel. FIFO channel
    /// ordering guarantees ForwardInitEpoch arrives before the Forward frame
    /// on the wire. The atomic `take_init_forward()` ensures exactly-once
    /// semantics — the init frame is built once and cloned to all secondaries.
    ///
    /// Accepts a `&Stream` reference to avoid re-acquiring the map pin
    /// (the caller already holds a guard).
    pub(crate) fn maybe_build_init_forward(&self, stream: &Stream, frame: &Frame) -> Option<Frame> {
        let (stream_id, epoch) = match &frame.variable_header {
            VariableHeader::Forward { stream_id, epoch, .. }
            | VariableHeader::ForwardChecksum { stream_id, epoch, .. } => (*stream_id, *epoch),
            _ => return None,
        };

        stream.with_epoch(epoch, |ext| {
            if !ext.take_init_forward() {
                return None;
            }

            Some(Frame::new(
                VariableHeader::ForwardInitEpoch {
                    stream_id,
                    epoch,
                    start_offset: ext.start_offset,
                    extent_capacity: ext.capacity(),
                    cache_extents: stream.max_epochs() as u16,
                    storage_class: stream.storage_class(),
                    arena_class: ArenaClass::Dedicated,
                },
                None,
            ))
        })?
    }

    /// Handle ForwardInitEpoch (0x0B, flag=0x01) — init-extent notification.
    ///
    /// Ensures the stream exists (creating it if needed), then registers the
    /// extent with the primary's actual capacity and adaptive config.
    /// Fire-and-forget: no response.
    pub(crate) fn handle_forward_init_epoch(&self, frame: Frame) {
        let (
            stream_id,
            epoch,
            start_offset,
            extent_capacity,
            cache_extents,
            storage_class,
            arena_class,
        ) = match &frame.variable_header {
            VariableHeader::ForwardInitEpoch {
                stream_id,
                epoch,
                start_offset,
                extent_capacity,
                cache_extents,
                storage_class,
                arena_class,
            } => (
                *stream_id,
                *epoch,
                *start_offset,
                *extent_capacity,
                *cache_extents,
                *storage_class,
                *arena_class,
            ),
            _ => return,
        };

        tracing::debug!(
            arena_class = ?arena_class,
            stream_id = %stream_id,
            "ForwardInitEpoch arena_class"
        );

        let is_new = self.try_create_stream(
            stream_id,
            storage_class,
            &EpochPolicy {
                cache: cache_extents,
            },
        );
        self.try_register_epoch(stream_id, start_offset, epoch, extent_capacity);

        if is_new {
            info!(
                "ForwardInitEpoch (new stream): stream={}, epoch={}, start_offset={}, capacity={}",
                stream_id, epoch, start_offset, extent_capacity,
            );
        } else {
            info!(
                "ForwardInitEpoch: stream={}, epoch={}, start_offset={}, capacity={}",
                stream_id, epoch, start_offset, extent_capacity,
            );
        }
    }

    /// Register an extent on a stream if it doesn't already exist.
    fn try_register_epoch(
        &self,
        stream_id: StreamId,
        start_offset: Offset,
        epoch: Epoch,
        extent_capacity: u32,
    ) {
        let guard = self.streams.pin();
        if let Some(stream) = guard.get(&stream_id)
            && stream.with_epoch(epoch, |_| ()).is_none()
        {
            stream.register_epoch(start_offset, epoch, extent_capacity);
        }
    }

    /// Handle Forward (0x0B, flag=0x00) — per-record primary→secondary replication.
    ///
    /// The Forward frame carries (stream_id, epoch, epoch, offset);
    /// the secondary derives byte_pos from its own write_cursor (strict-order
    /// TCP FIFO guarantees deterministic replay). The stream/extent must already
    /// exist (created by a prior ForwardInitEpoch or RegisterEpoch).
    ///
    /// Returns a cumulative Watermark with the contiguous committed offset,
    /// or None if the forward cannot be processed (bad frame, unknown stream, etc.).
    pub(crate) fn handle_forward(&self, frame: Frame) -> Option<Frame> {
        let (stream_id, epoch, offset) = match &frame.variable_header {
            VariableHeader::Forward {
                stream_id,
                epoch,
                offset,
            } => (*stream_id, *epoch, *offset),
            _ => return None,
        };

        // Look up the stream — must exist (created by ForwardInitEpoch or RegisterEpoch).
        let streams = self.streams.pin();
        let stream = match streams.get(&stream_id) {
            Some(s) => s,
            None => {
                warn!(
                    "Forward for unknown stream {}, epoch {} — missing ForwardInitEpoch?",
                    stream_id, epoch,
                );
                return None;
            }
        };

        let replicate_result =
            stream.replicate(epoch, offset, frame.payload.clone().unwrap_or_default());

        self.finish_forward(
            stream,
            stream_id,
            epoch,
            replicate_result,
            &frame,
        )
    }

    /// Shared tail of handle_forward: process replicate result, update metrics, return watermark.
    pub(crate) fn finish_forward(
        &self,
        stream: &Stream,
        stream_id: StreamId,
        epoch: Epoch,
        replicate_result: Result<AppendResult, StorageError>,
        frame: &Frame,
    ) -> Option<Frame> {
        match replicate_result {
            Ok(_r) => {}
            Err(e) => {
                warn!(
                    "Forward replicate failed for stream={}, epoch={}: {}",
                    stream_id, epoch, e,
                );
                return None;
            }
        };

        // Update metrics counters.
        self.append_count.fetch_add(1, Ordering::Relaxed);
        self.bytes_written.fetch_add(
            frame.payload.as_ref().map_or(0, |p| p.len()) as u64,
            Ordering::Relaxed,
        );

        // Check if deferred CRC32 verification can now complete.
        // Also read the contiguous watermark for the response.
        let watermark = stream.with_epoch(epoch, |extent| {
            match extent.try_verify_checksum() {
                Some(true) => {
                    info!(
                        "CRC32 checksum verified (deferred): stream={}, epoch={}",
                        stream_id, epoch,
                    );
                }
                Some(false) => {
                    warn!(
                        "CRC32 checksum mismatch (deferred): stream={}, epoch={}",
                        stream_id, epoch,
                    );
                }
                None => {} // not ready yet
            }
            extent.last_offset()
        })??;

        Some(Frame::new(
            VariableHeader::Watermark {
                stream_id,
                epoch,
                offset: watermark,
            },
            None,
        ))
    }

    /// Handle ForwardChecksum (0x0B, flag=0x02) — CRC32 verification for sealed extent.
    pub(crate) fn handle_forward_checksum(&self, frame: Frame) {
        let (stream_id, epoch, primary_crc32, primary_committed_bytes) =
            match &frame.variable_header {
                VariableHeader::ForwardChecksum {
                    stream_id,
                    epoch,
                    checksum,
                    committed_bytes,
                    ..
                } => (*stream_id, *epoch, *checksum, *committed_bytes),
                _ => return,
            };
        let guard = self.streams.pin();
        let stream = match guard.get(&stream_id) {
            Some(s) => s,
            None => {
                warn!(
                    "ForwardChecksum for unknown stream {}, epoch {}",
                    stream_id, epoch,
                );
                return;
            }
        };

        let found = stream.with_epoch(epoch, |extent| {
            extent.store_primary_checksum(primary_crc32);
            extent.try_advance_committed();
            match extent.try_verify_checksum() {
                Some(true) => {
                    info!(
                        "CRC32 checksum verified: stream={}, epoch={}, crc32={:#010x}, bytes={}",
                        stream_id, epoch, primary_crc32, primary_committed_bytes,
                    );
                }
                Some(false) => {
                    warn!(
                        "CRC32 checksum mismatch: stream={}, epoch={}, \
                         primary_crc32={:#010x}, primary_bytes={}",
                        stream_id, epoch, primary_crc32, primary_committed_bytes,
                    );
                }
                None => {
                    info!(
                        "ForwardChecksum stored (deferred): stream={}, epoch={}, \
                         crc32={:#010x}, bytes={} — waiting for remaining records",
                        stream_id, epoch, primary_crc32, primary_committed_bytes,
                    );
                }
            }
        });
        if found.is_none() {
            warn!(
                "ForwardChecksum for unknown epoch {} on stream {}",
                epoch, stream_id,
            );
        }
    }

    /// Handle ForwardFlushed (0x05, flag=0x03) — extent flushed to S3 notification.
    pub(crate) fn handle_forward_flushed(&self, frame: Frame) {
        let (stream_id, epoch) = match &frame.variable_header {
            VariableHeader::ForwardFlushed {
                stream_id, epoch, ..
            } => (*stream_id, *epoch),
            _ => return,
        };
        let guard = self.streams.pin();
        let stream = match guard.get(&stream_id) {
            Some(s) => s,
            None => {
                warn!(
                    "ForwardFlushed for unknown stream {}, epoch {}",
                    stream_id, epoch,
                );
                return;
            }
        };

        let found = stream.with_epoch(epoch, |ext| {
            ext.mark_flushed();
        });

        if found.is_some() {
            info!(
                "ForwardFlushed: stream={}, epoch={} — marked flushed, eligible for eviction",
                stream_id, epoch,
            );
        } else {
            warn!(
                "ForwardFlushed for unknown epoch {} on stream {}",
                epoch, stream_id,
            );
        }
    }
}
