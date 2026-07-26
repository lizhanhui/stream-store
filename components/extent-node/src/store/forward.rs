use std::sync::Arc;
use std::sync::atomic::Ordering;

use common::config::{
    DEFAULT_EXTENT_GROWTH_FACTOR, DEFAULT_MAX_EXTENT_CAPACITY, DEFAULT_MIN_EXTENT_CAPACITY,
};
use common::errors::StorageError;
use common::types::{Epoch, ExtentId, ExtentPolicy, Offset, StreamId};
use rpc::frame::{Frame, VariableHeader};
use tracing::{info, warn};

use super::{ExtentNodeStore, ReplicaInfo};
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

        let (min_cap, max_cap, growth) = stream.capacity_bounds();
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
                    min_extent_capacity: min_cap,
                    max_extent_capacity: max_cap,
                    extent_growth_factor: growth,
                    storage_class: stream.storage_class(),
                },
                None,
            ))
        })?
    }

    /// Send the one-shot init before the first replication event for an extent.
    /// Both frames use the same per-secondary FIFO channels. Delivery remains
    /// best-effort; a lost frame fails closed because the Secondary withholds
    /// its Watermark.
    pub(crate) fn send_forward_with_init(&self, stream: &Stream, event: Frame) {
        if let Some(init) = self.maybe_build_init_forward(stream, &event) {
            stream.send_forward(init);
        }
        stream.send_forward(event);
    }

    /// Handle ForwardInitExtent (0x0B, flag=0x01) — init-extent notification.
    ///
    /// Ensures the stream exists (creating it if needed), then registers the
    /// extent with the primary's actual capacity and adaptive config.
    /// Fire-and-forget: no response.
    pub(crate) fn handle_forward_init_extent(&self, frame: Frame) {
        let (
            stream_id,
            extent_id,
            epoch,
            start_offset,
            extent_capacity,
            cache_extents,
            min_extent_capacity,
            max_extent_capacity,
            extent_growth_factor,
            storage_class,
        ) = match &frame.variable_header {
            VariableHeader::ForwardInitExtent {
                stream_id,
                extent_id,
                epoch,
                start_offset,
                extent_capacity,
                cache_extents,
                min_extent_capacity,
                max_extent_capacity,
                extent_growth_factor,
                storage_class,
            } => (
                *stream_id,
                *extent_id,
                *epoch,
                *start_offset,
                *extent_capacity,
                *cache_extents,
                *min_extent_capacity,
                *max_extent_capacity,
                *extent_growth_factor,
                *storage_class,
            ),
            _ => return,
        };

        // Normalize: 0 means use default.
        let min_extent_capacity = if min_extent_capacity == 0 {
            DEFAULT_MIN_EXTENT_CAPACITY
        } else {
            min_extent_capacity
        };
        let max_extent_capacity = if max_extent_capacity == 0 {
            DEFAULT_MAX_EXTENT_CAPACITY
        } else {
            max_extent_capacity
        };
        let extent_growth_factor = if extent_growth_factor == 0 {
            DEFAULT_EXTENT_GROWTH_FACTOR
        } else {
            extent_growth_factor
        };

        let _transition = self.role_transition.write().unwrap();
        if self.forwarding_quarantined(stream_id, epoch) {
            warn!(
                "ignoring ForwardInitExtent for quarantined epoch: stream={}, extent={}, epoch={}",
                stream_id, extent_id, epoch,
            );
            return;
        }
        self.clear_older_forward_quarantine(stream_id, epoch);
        let existing = self.replicas.pin().get(&stream_id).map(Arc::clone);

        // A stale init must never demote a newer assignment. A current Primary
        // also ignores same-epoch init from a peer; the SM registration wins.
        if existing.as_ref().is_some_and(|replica| {
            replica.epoch.0 > epoch.0 || (replica.is_primary() && replica.epoch == epoch)
        }) {
            warn!(
                "ignoring stale/conflicting ForwardInitExtent: stream={}, extent={}, epoch={}",
                stream_id, extent_id, epoch,
            );
            return;
        }
        if existing.as_ref().is_some_and(|replica| {
            !replica.is_primary() && replica.epoch == epoch && replica.extent_id == extent_id
        }) {
            // Exact duplicate delivery is idempotent. The first init remains
            // authoritative for immutable extent metadata and stream policy.
            return;
        }
        if existing
            .as_ref()
            .is_some_and(|replica| replica.epoch == epoch && replica.extent_id.0 > extent_id.0)
        {
            return;
        }

        if let Some(replica) = existing
            .as_ref()
            .filter(|replica| !replica.is_primary() && replica.epoch == epoch)
        {
            let contiguous = self
                .streams
                .pin()
                .get(&stream_id)
                .is_some_and(|stream| stream.max_offset() == start_offset);
            if extent_id.0 != replica.extent_id.0.saturating_add(1) || !contiguous {
                warn!(
                    "ForwardInitExtent successor mismatch; quarantining stream={}, extent={}, epoch={}",
                    stream_id, extent_id, epoch,
                );
                self.quarantine_forwarding(stream_id, epoch);
                return;
            }
        }

        if self
            .streams
            .pin()
            .get(&stream_id)
            .and_then(|stream| stream.with_extent(extent_id, |extent| extent.epoch != epoch))
            .unwrap_or(false)
        {
            self.quarantine_forwarding(stream_id, epoch);
            return;
        }

        // Publish Secondary authority before changing stream state. Appends that
        // race with this transition fail closed as NotPrimary.
        let replication_factor = existing
            .as_ref()
            .map_or(0, |replica| replica.replication_factor);
        self.replicas.pin().insert(
            stream_id,
            Arc::new(ReplicaInfo {
                stream_id,
                extent_id,
                epoch,
                role: 1,
                replication_factor,
                replica_addrs: Vec::new(),
            }),
        );

        let is_new = self.try_create_stream(
            stream_id,
            storage_class,
            &ExtentPolicy {
                cache: cache_extents,
                min_capacity: min_extent_capacity,
                max_capacity: max_extent_capacity,
                scale_factor: extent_growth_factor,
            },
        );
        self.try_register_extent(stream_id, extent_id, start_offset, epoch, extent_capacity);
        if let Some(stream) = self.streams.pin().get(&stream_id) {
            stream.set_epoch(epoch);
            stream.set_downstream_txs(Vec::new());
            stream.deactivate_ack_queue();
        }

        if is_new {
            info!(
                "ForwardInitExtent (new stream): stream={}, extent={}, start_offset={}, capacity={}, min={}, max={}, gf={}",
                stream_id,
                extent_id,
                start_offset,
                extent_capacity,
                min_extent_capacity,
                max_extent_capacity,
                extent_growth_factor,
            );
        } else {
            info!(
                "ForwardInitExtent: stream={}, extent={}, start_offset={}, capacity={}, min={}, max={}, gf={}",
                stream_id,
                extent_id,
                start_offset,
                extent_capacity,
                min_extent_capacity,
                max_extent_capacity,
                extent_growth_factor,
            );
        }
    }

    /// Register an extent on a stream if it doesn't already exist.
    ///
    /// Uses an atomic check-and-insert so concurrent `ForwardInitExtent` and
    /// `Forward` deliveries cannot create duplicate extents.
    fn try_register_extent(
        &self,
        stream_id: StreamId,
        extent_id: ExtentId,
        start_offset: Offset,
        epoch: Epoch,
        extent_capacity: u32,
    ) {
        let guard = self.streams.pin();
        if let Some(stream) = guard.get(&stream_id)
            && let Some(existing) =
                stream.register_extent_if_absent(extent_id, start_offset, epoch, extent_capacity)
            && existing != start_offset
        {
            warn!(
                "ForwardInitExtent start_offset mismatch: stream={}, extent={}, existing={}, from_primary={} — keeping existing",
                stream_id, extent_id, existing.0, start_offset.0,
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
        let _transition = self.role_transition.read().unwrap();

        if self.forwarding_quarantined(stream_id, epoch) {
            return None;
        }

        // Forward is accepted only by an explicit Secondary assignment for the
        // same epoch. Missing/lost ForwardInitExtent therefore fails closed.
        let replica = self.replicas.pin().get(&stream_id).map(Arc::clone);
        if !replica
            .as_ref()
            .is_some_and(|replica| !replica.is_primary() && replica.epoch == epoch)
        {
            warn!(
                "Forward rejected for role/epoch mismatch: stream={}, extent={}, epoch={}",
                stream_id, extent_id, epoch,
            );
            self.quarantine_forwarding(stream_id, epoch);
            return None;
        }

        let streams = self.streams.pin();
        let stream = match streams.get(&stream_id) {
            Some(s) => s,
            None => {
                warn!(
                    "Forward for unknown stream {}, extent {} — missing ForwardInitExtent?",
                    stream_id, extent_id,
                );
                self.quarantine_forwarding(stream_id, epoch);
                return None;
            }
        };

        if stream.epoch() != epoch
            || !stream
                .with_extent(extent_id, |extent| extent.epoch == epoch)
                .unwrap_or(false)
        {
            warn!(
                "Forward rejected for unknown/stale extent: stream={}, extent={}, epoch={}",
                stream_id, extent_id, epoch,
            );
            self.quarantine_forwarding(stream_id, epoch);
            return None;
        }

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
                self.quarantine_forwarding(stream_id, epoch);
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
    pub(crate) fn handle_forward_checksum(&self, frame: Frame) {
        let (stream_id, extent_id, epoch, primary_crc32, primary_committed_bytes) =
            match &frame.variable_header {
                VariableHeader::ForwardChecksum {
                    stream_id,
                    extent_id,
                    epoch,
                    checksum,
                    committed_bytes,
                } => (*stream_id, *extent_id, *epoch, *checksum, *committed_bytes),
                _ => return,
            };
        let _transition = self.role_transition.read().unwrap();
        if self.forwarding_quarantined(stream_id, epoch) {
            return;
        }

        let replica = self.replicas.pin().get(&stream_id).map(Arc::clone);
        if !replica
            .as_ref()
            .is_some_and(|replica| !replica.is_primary() && replica.epoch == epoch)
        {
            warn!(
                "ForwardChecksum rejected for role/epoch mismatch: stream={}, extent={}, epoch={}",
                stream_id, extent_id, epoch,
            );
            self.quarantine_forwarding(stream_id, epoch);
            return;
        }

        let guard = self.streams.pin();
        let stream = match guard.get(&stream_id) {
            Some(s) => s,
            None => {
                warn!(
                    "ForwardChecksum for unknown stream {}, extent {}",
                    stream_id, extent_id,
                );
                self.quarantine_forwarding(stream_id, epoch);
                return;
            }
        };

        if stream.epoch() != epoch {
            self.quarantine_forwarding(stream_id, epoch);
            return;
        }
        if !stream
            .with_extent(extent_id, |extent| extent.epoch == epoch)
            .unwrap_or(false)
        {
            self.quarantine_forwarding(stream_id, epoch);
            return;
        }
        let found = stream.with_extent(extent_id, |extent| {
            extent.store_primary_checksum(primary_crc32);
            extent.try_advance_committed();
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
                         primary_crc32={:#010x}, primary_bytes={}",
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
