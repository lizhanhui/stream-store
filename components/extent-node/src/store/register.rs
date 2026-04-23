use std::sync::Arc;

use common::config::{DEFAULT_MAX_EXTENT_CAPACITY, DEFAULT_MIN_EXTENT_CAPACITY};
use common::types::{ErrorCode, ExtentId, ExtentPolicy};
use rpc::frame::{Frame, VariableHeader};
use rpc::payload::{ROLE_PRIMARY, parse_register_extent_payload};
use tracing::info;

use super::{ExtentNodeStore, ReplicaInfo};

impl ExtentNodeStore {
    /// Handle RegisterExtent from StreamManager: assign this ExtentNode a role in broadcast replication.
    ///
    /// Creates the stream locally (with the StreamManager-assigned stream_id) and stores replica info.
    pub(crate) fn handle_register_extent(&self, frame: Frame) -> Frame {
        let (extent_id, role, config) = match &frame.variable_header {
            VariableHeader::RegisterExtent {
                extent_id,
                role,
                config,
                ..
            } => (*extent_id, *role, *config),
            _ => {
                return Frame::error_from_request(
                    &frame,
                    ErrorCode::InternalError,
                    "invalid RegisterExtent frame",
                    ExtentId(0),
                );
            }
        };

        let stream_id = config.stream_id;
        let epoch = config.epoch;
        let replication_factor = config.replication_factor;

        // Normalize capacity bounds: 0 means use default.
        let policy = ExtentPolicy {
            cache: config.policy.cache,
            min_capacity: if config.policy.min_capacity == 0 {
                DEFAULT_MIN_EXTENT_CAPACITY
            } else {
                config.policy.min_capacity
            },
            max_capacity: if config.policy.max_capacity == 0 {
                DEFAULT_MAX_EXTENT_CAPACITY
            } else {
                config.policy.max_capacity
            },
            scale_factor: config.policy.scale_factor,
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
        self.try_create_stream(stream_id, config.storage_class, &policy);

        // Register the extent (idempotent — skips if already exists).
        let streams_guard = self.streams.pin();
        if let Some(stream) = streams_guard.get(&stream_id) {
            if stream.with_extent(extent_id, |_| ()).is_none() {
                stream.register_extent(extent_id, stream.max_offset(), epoch, policy.min_capacity);
            } else {
                // Extent already exists (lazy creation from Forward), but update epoch
                // from authoritative source (RegisterExtent carries the real epoch).
                stream.set_epoch(epoch);
            }
        }

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

        // If this node is Primary, initialize an AckQueue on the stream.
        if ri.is_primary() {
            if let Some(stream) = streams_guard.get(&stream_id) {
                stream.init_ack_queue(ri.required_secondary_acks(), self.replication_timeout);
            }

            // Cache per-secondary Sender handles in the Stream so the
            // hot append path can push Forward frames with zero lookup overhead.
            if !ri.replica_addrs.is_empty()
                && let Some(pool) = self.downstream.get()
            {
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
