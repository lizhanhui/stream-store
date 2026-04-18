use std::sync::Arc;
use std::sync::atomic::Ordering;

use common::config::{DEFAULT_MAX_EXTENT_CAPACITY, DEFAULT_MIN_EXTENT_CAPACITY};
use common::types::{ErrorCode, ExtentId, Offset};
use rpc::frame::{Frame, VariableHeader};
use rpc::payload::{ROLE_PRIMARY, parse_register_extent_payload};
use tracing::info;

use super::{ExtentNodeStore, ReplicaInfo};
use crate::ack_queue::AckQueue;
use crate::stream::Stream;

impl ExtentNodeStore {
    /// Handle RegisterExtent from StreamManager: assign this ExtentNode a role in broadcast replication.
    ///
    /// Creates the stream locally (with the StreamManager-assigned stream_id) and stores replica info.
    pub(crate) fn handle_register_extent(&self, frame: Frame) -> Frame {
        // Extract stream_id, extent_id, role, replication_factor from the variable header.
        let (
            stream_id,
            extent_id,
            role,
            replication_factor,
            epoch,
            extent_capacity,
            cache_extents,
            extent_growth_factor,
            storage_medium,
        ) = match &frame.variable_header {
            VariableHeader::RegisterExtent {
                stream_id,
                extent_id,
                role,
                replication_factor,
                epoch,
                extent_capacity,
                cache_extents,
                extent_growth_factor,
                storage_medium,
                ..
            } => (
                *stream_id,
                *extent_id,
                *role,
                *replication_factor,
                *epoch,
                *extent_capacity,
                *cache_extents,
                *extent_growth_factor,
                *storage_medium,
            ),
            _ => {
                return Frame::error_from_request(
                    &frame,
                    ErrorCode::InternalError,
                    "invalid RegisterExtent frame",
                    ExtentId(0),
                );
            }
        };

        // Parse replica addresses from the payload.
        let replica_addrs =
            match parse_register_extent_payload(frame.payload.as_deref().unwrap_or_default()) {
                Some(addrs) => addrs,
                None => {
                    return Frame::error_from_request(
                        &frame,
                        ErrorCode::InternalError,
                        "invalid RegisterExtent payload",
                        ExtentId(0),
                    );
                }
            };

        // Create the stream locally if it doesn't exist, then register the new extent.
        // Skip extent creation if it already exists (idempotent — extent may have been
        // lazily created by a forwarded append that arrived before this RegisterExtent).
        let streams_guard = self.streams.pin();
        if let Some(stream) = streams_guard.get(&stream_id) {
            // RegisterExtent is the authoritative source for cache policy.
            // Always apply — the stream may have been lazily created by
            // ForwardInitExtent before this arrives with max_extents=0.
            stream.set_max_extents(cache_extents as usize);
            stream.set_storage_medium(storage_medium);
            if stream.with_extent(extent_id, |_| ()).is_none() {
                stream.register_extent(
                    extent_id,
                    stream.max_offset(),
                    extent_capacity,
                    epoch,
                    DEFAULT_MIN_EXTENT_CAPACITY,
                    DEFAULT_MAX_EXTENT_CAPACITY,
                    extent_growth_factor,
                );
            } else {
                // Extent already exists (lazy creation from Forward), but update epoch
                // from authoritative source (RegisterExtent carries the real epoch).
                stream.set_epoch(epoch);
            }
        } else {
            let stream = Stream::new(stream_id);
            stream.set_max_extents(cache_extents as usize);
            stream.set_storage_medium(storage_medium);
            stream.register_extent(
                extent_id,
                Offset(0),
                extent_capacity,
                epoch,
                DEFAULT_MIN_EXTENT_CAPACITY,
                DEFAULT_MAX_EXTENT_CAPACITY,
                extent_growth_factor,
            );
            streams_guard.insert(stream_id, stream);
        };

        // Update next_stream_id to avoid collision with StreamManager-assigned IDs.
        // Use fetch_max to atomically ensure we stay above the assigned ID.
        self.next_stream_id
            .fetch_max(stream_id.0 + 1, Ordering::Relaxed);

        let role_name = if role == ROLE_PRIMARY {
            "Primary"
        } else {
            &format!("Secondary-{}", role)
        };
        let addrs_info = if replica_addrs.is_empty() {
            "none".to_string()
        } else {
            replica_addrs.join(", ")
        };
        info!(
            "RegisterExtent: stream={}, extent={}, role={role_name}, rf={}, secondaries=[{addrs_info}]",
            stream_id, extent_id, replication_factor,
        );

        let ri = ReplicaInfo {
            stream_id,
            extent_id,
            role,
            replication_factor,
            replica_addrs,
        };

        // If this node is Primary, initialize an AckQueue.
        if ri.is_primary() {
            {
                let aq_guard = self.ack_queues.pin();
                aq_guard.get_or_insert_with(stream_id, || {
                    AckQueue::with_timeout(ri.required_secondary_acks(), self.replication_timeout)
                });
            }

            // Cache per-secondary Sender handles in the Stream so the
            // hot append path can push Forward frames with zero lookup overhead.
            if !ri.replica_addrs.is_empty() {
                if let Some(pool) = self.downstream.get() {
                    let txs: Vec<_> = ri
                        .replica_addrs
                        .iter()
                        .map(|addr| pool.get_or_create_sender(addr))
                        .collect();
                    if let Some(stream) = streams_guard.get(&stream_id) {
                        stream.set_downstream_txs(txs);
                    }
                }
            }
        }

        self.replicas.pin().insert(stream_id, Arc::new(ri));

        Frame::new(
            VariableHeader::RegisterExtentAck {
                request_id: frame.request_id(),
                stream_id,
                extent_id,
            },
            None,
        )
    }
}
