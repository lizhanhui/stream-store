use std::sync::atomic::Ordering;

use common::config::{
    DEFAULT_EXTENT_GROWTH_FACTOR, DEFAULT_MAX_EXTENT_CAPACITY,
};
use common::errors::StorageError;
use common::types::{Epoch, ExtentId, StreamId};
use rpc::frame::{Frame, VariableHeader};
use tracing::{info, warn};

use super::ExtentNodeStore;
use crate::extent::AppendResult;
use crate::stream::Stream;

impl ExtentNodeStore {
    /// Check if a Forward or ForwardChecksum frame targets an extent that
    /// needs ForwardInitExtent. Returns the init frame if so.
    ///
    /// Called on the leader side before pushing to the channel. FIFO channel
    /// ordering guarantees ForwardInitExtent arrives before the Forward frame
    /// on the wire. The atomic `take_init_forward()` ensures exactly-once
    /// semantics — the init frame is built once and cloned to all secondaries.
    ///
    /// Accepts a `&Stream` reference to avoid re-acquiring the map pin
    /// (the caller already holds a guard).
    pub(crate) fn maybe_build_init_forward(&self, stream: &Stream, frame: &Frame) -> Option<Frame> {
        let (stream_id, extent_id, epoch) = match &frame.variable_header {
            VariableHeader::Forward {
                stream_id,
                extent_id,
                epoch,
                ..
            } => (*stream_id, *extent_id, Some(*epoch)),
            VariableHeader::ForwardChecksum {
                stream_id,
                extent_id,
                ..
            } => (*stream_id, *extent_id, None),
            _ => return None,
        };

        stream.with_extent(extent_id, |ext| {
            if !ext.take_init_forward() {
                return None;
            }

            let epoch = epoch.unwrap_or(ext.epoch);
            Some(Frame::new(
                VariableHeader::ForwardInitExtent {
                    stream_id,
                    extent_id,
                    epoch,
                    start_offset: ext.start_offset,
                    extent_capacity: ext.capacity(),
                    cache_extents: stream.max_extents() as u16,
                    storage_class: stream.storage_class(),
                },
                None,
            ))
        })?
    }

    /// Handle ForwardInitExtent (0x0B, flag=0x01) — init-extent notification.
    ///
    /// Creates the stream (if needed) and registers the extent with the provided
    /// start_offset and extent_capacity. Fire-and-forget: no response.
    pub(crate) fn handle_forward_init_extent(&self, frame: Frame) {
        let (
            stream_id,
            extent_id,
            epoch,
            start_offset,
            extent_capacity,
            cache_extents,
            storage_class,
        ) = match &frame.variable_header {
            VariableHeader::ForwardInitExtent {
                stream_id,
                extent_id,
                epoch,
                start_offset,
                extent_capacity,
                cache_extents,
                storage_class,
            } => (
                *stream_id,
                *extent_id,
                *epoch,
                *start_offset,
                *extent_capacity,
                *cache_extents,
                *storage_class,
            ),
            _ => return,
        };

        let guard = self.streams.pin();
        if let Some(stream) = guard.get(&stream_id) {
            // Apply cache policy if not yet set (RegisterExtent may arrive later).
            if cache_extents > 0 && stream.max_extents() == 0 {
                stream.set_max_extents(cache_extents as usize);
            }
            stream.set_storage_class(storage_class);
            if stream.with_extent(extent_id, |_| ()).is_none() {
                stream.register_extent(
                    extent_id,
                    start_offset,
                    epoch,
                    extent_capacity,
                    DEFAULT_MAX_EXTENT_CAPACITY,
                    DEFAULT_EXTENT_GROWTH_FACTOR,
                );
                info!(
                    "ForwardInitExtent: stream={}, extent={}, start_offset={}, capacity={}",
                    stream_id, extent_id, start_offset, extent_capacity,
                );
            }
        } else {
            let stream = Stream::new(stream_id);
            stream.set_max_extents(cache_extents as usize);
            stream.set_storage_class(storage_class);
            stream.register_extent(
                extent_id,
                start_offset,
                epoch,
                extent_capacity,
                DEFAULT_MAX_EXTENT_CAPACITY,
                DEFAULT_EXTENT_GROWTH_FACTOR,
            );
            guard.insert(stream_id, stream);
            self.next_stream_id
                .fetch_max(stream_id.0 + 1, Ordering::Relaxed);
            info!(
                "ForwardInitExtent (new stream): stream={}, extent={}, start_offset={}, capacity={}",
                stream_id, extent_id, start_offset, extent_capacity,
            );
        }
    }

    /// Handle Forward (0x0B, flag=0x00) — per-record primary→secondary replication.
    ///
    /// The Forward frame carries (stream_id, extent_id, epoch, offset, byte_pos)
    /// so the secondary writes each record at the exact same arena position as
    /// the primary. The stream/extent must already exist (created by a prior
    /// ForwardInitExtent or RegisterExtent).
    ///
    /// Returns a cumulative Watermark with the contiguous committed offset,
    /// or None if the forward cannot be processed (bad frame, unknown stream, etc.).
    pub(crate) fn handle_forward(&self, frame: Frame) -> Option<Frame> {
        let (stream_id, extent_id, epoch, offset, byte_pos) = match &frame.variable_header {
            VariableHeader::Forward {
                stream_id,
                extent_id,
                epoch,
                offset,
                byte_pos,
            } => (*stream_id, *extent_id, *epoch, *offset, *byte_pos),
            _ => return None,
        };

        // Look up the stream — must exist (created by ForwardInitExtent or RegisterExtent).
        let streams = self.streams.pin();
        let stream = match streams.get(&stream_id) {
            Some(s) => s,
            None => {
                warn!(
                    "Forward for unknown stream {}, extent {} — missing ForwardInitExtent?",
                    stream_id, extent_id,
                );
                return None;
            }
        };

        let replicate_result = stream.replicate(
            extent_id,
            offset,
            byte_pos,
            frame.payload.clone().unwrap_or_default(),
        );

        self.finish_forward(
            stream,
            stream_id,
            extent_id,
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
        extent_id: ExtentId,
        epoch: Epoch,
        replicate_result: Result<AppendResult, StorageError>,
        frame: &Frame,
    ) -> Option<Frame> {
        match replicate_result {
            Ok(_r) => {}
            Err(e) => {
                warn!(
                    "Forward replicate failed for stream={}, extent={}: {}",
                    stream_id, extent_id, e,
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
        let watermark = stream.with_extent(extent_id, |extent| {
            match extent.try_verify_checksum() {
                Some(true) => {
                    info!(
                        "CRC32 checksum verified (deferred): stream={}, extent={}",
                        stream_id, extent_id,
                    );
                }
                Some(false) => {
                    warn!(
                        "CRC32 checksum mismatch (deferred): stream={}, extent={}",
                        stream_id, extent_id,
                    );
                }
                None => {} // not ready yet
            }
            extent.last_offset()
        })??;

        Some(Frame::new(
            VariableHeader::Watermark {
                stream_id,
                extent_id,
                epoch,
                offset: watermark,
            },
            None,
        ))
    }

    /// Handle ForwardChecksum (0x0B, flag=0x02) — CRC32 verification for sealed extent.
    ///
    /// Due to leader Mutex races on the primary, this frame may arrive before all
    /// Forward frames have been processed on the secondary. The primary's checksum
    /// and committed_bytes are stored in the extent for deferred verification.
    /// `try_advance_committed()` is called to advance as far as possible, and
    /// `try_verify_checksum()` checks if all records have been hashed.
    /// If not yet ready, verification will complete on a subsequent `replicate()` call.
    pub(crate) fn handle_forward_checksum(&self, frame: Frame) {
        let (stream_id, extent_id, primary_crc32, primary_committed_bytes) =
            match &frame.variable_header {
                VariableHeader::ForwardChecksum {
                    stream_id,
                    extent_id,
                    checksum,
                    committed_bytes,
                    ..
                } => (*stream_id, *extent_id, *checksum, *committed_bytes),
                _ => return,
            };

        let guard = self.streams.pin();
        let stream = match guard.get(&stream_id) {
            Some(s) => s,
            None => {
                warn!(
                    "ForwardChecksum for unknown stream {}, extent {}",
                    stream_id, extent_id,
                );
                return;
            }
        };

        let found = stream.with_extent(extent_id, |extent| {
            // Store primary's checksum for deferred comparison.
            extent.store_primary_checksum(primary_crc32);

            // Advance incremental CRC32 as far as possible.
            extent.try_advance_committed();

            // Check if verification can complete now.
            match extent.try_verify_checksum() {
                Some(true) => {
                    info!(
                        "CRC32 checksum verified: stream={}, extent={}, crc32={:#010x}, bytes={}",
                        stream_id, extent_id, primary_crc32, primary_committed_bytes,
                    );
                }
                Some(false) => {
                    warn!(
                        "CRC32 checksum mismatch: stream={}, extent={}, \
                         primary_crc32={:#010x}, primary_bytes={} \
                         (verification will be logged by try_verify_checksum)",
                        stream_id, extent_id, primary_crc32, primary_committed_bytes,
                    );
                }
                None => {
                    info!(
                        "ForwardChecksum stored (deferred): stream={}, extent={}, \
                         crc32={:#010x}, bytes={} — waiting for remaining records",
                        stream_id, extent_id, primary_crc32, primary_committed_bytes,
                    );
                }
            }
        });
        if found.is_none() {
            warn!(
                "ForwardChecksum for unknown extent {} on stream {}",
                extent_id, stream_id,
            );
        }
    }

    /// Handle ForwardFlushed (0x05, flag=0x03) — extent flushed to S3 notification.
    ///
    /// Sent by the Primary after a sealed extent is uploaded to S3.
    /// Secondaries use this to mark the extent as eligible for memory eviction.
    /// Fire-and-forget: no response.
    pub(crate) fn handle_forward_flushed(&self, frame: Frame) {
        let (stream_id, extent_id) = match &frame.variable_header {
            VariableHeader::ForwardFlushed {
                stream_id,
                extent_id,
                ..
            } => (*stream_id, *extent_id),
            _ => return,
        };

        let guard = self.streams.pin();
        let stream = match guard.get(&stream_id) {
            Some(s) => s,
            None => {
                warn!(
                    "ForwardFlushed for unknown stream {}, extent {}",
                    stream_id, extent_id,
                );
                return;
            }
        };

        let found = stream.with_extent(extent_id, |ext| {
            ext.mark_flushed();
        });

        if found.is_some() {
            info!(
                "ForwardFlushed: stream={}, extent={} — marked flushed, eligible for eviction",
                stream_id, extent_id,
            );
        } else {
            warn!(
                "ForwardFlushed for unknown extent {} on stream {}",
                extent_id, stream_id,
            );
        }
    }
}
