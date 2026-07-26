use std::sync::Arc;

use common::config::{DEFAULT_MAX_EXTENT_CAPACITY, DEFAULT_MIN_EXTENT_CAPACITY};
use common::types::{ErrorCode, ExtentId, ExtentPolicy};
use rpc::frame::{Frame, VariableHeader};
use rpc::payload::{ROLE_PRIMARY, parse_register_extent_payload};
use tracing::{info, warn};

use super::{ExtentNodeStore, ReplicaInfo};

impl ExtentNodeStore {
    /// Handle RegisterExtent from StreamManager: assign this ExtentNode a role in broadcast replication.
    ///
    /// Creates the stream locally (with the StreamManager-assigned stream_id) and stores replica info.
    pub(crate) fn handle_register_extent(&self, frame: Frame) -> Frame {
        let (extent_id, role, start_offset, config) = match &frame.variable_header {
            VariableHeader::RegisterExtent {
                extent_id,
                role,
                start_offset,
                config,
                ..
            } => (*extent_id, *role, *start_offset, *config),
            _ => {
                return Frame::error_from_request(
                    &frame,
                    ErrorCode::InternalError,
                    "invalid RegisterExtent frame",
                    ExtentId(0),
                );
            }
        };

        if role != ROLE_PRIMARY {
            return Frame::error_from_request(
                &frame,
                ErrorCode::NotPrimary,
                "RegisterExtent is Primary-only; secondaries use ForwardInitExtent",
                extent_id,
            );
        }

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

        // Serialize role transitions with ForwardInitExtent. This is a cold path
        // and must atomically publish the role/epoch together with extent state.
        let _transition = self.role_transition.write().unwrap();
        if self
            .replicas
            .pin()
            .get(&stream_id)
            .is_some_and(|existing| existing.epoch.0 > epoch.0)
        {
            return Frame::error_from_request(
                &frame,
                ErrorCode::EpochStale,
                "stale RegisterExtent epoch",
                extent_id,
            );
        }

        self.try_create_stream(stream_id, config.storage_class, &policy);

        let streams_guard = self.streams.pin();
        if let Some(stream) = streams_guard.get(&stream_id) {
            if stream
                .with_extent(extent_id, |extent| extent.epoch != epoch)
                .unwrap_or(false)
            {
                return Frame::error_from_request(
                    &frame,
                    ErrorCode::EpochStale,
                    "extent id already belongs to another epoch",
                    extent_id,
                );
            }
            if let Some(existing) = stream.register_extent_if_absent(
                extent_id,
                start_offset,
                epoch,
                policy.min_capacity,
            ) {
                if existing != start_offset {
                    warn!(
                        "RegisterExtent start_offset mismatch: stream={}, extent={}, existing={}, from_sm={} — keeping existing",
                        stream_id, extent_id, existing.0, start_offset.0,
                    );
                }
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
            epoch,
            role,
            replication_factor,
            replica_addrs,
        };

        // If this node is Primary, initialize an AckQueue on the stream.
        if ri.is_primary() {
            if let Some(stream) = streams_guard.get(&stream_id) {
                stream.init_ack_queue(
                    epoch,
                    ri.required_secondary_acks(),
                    self.replication_timeout,
                );
            }

            // Replace the cached topology even when RF=1 so sender handles from
            // an older Primary assignment cannot survive re-registration.
            let txs = self
                .downstream
                .get()
                .map(|pool| {
                    ri.replica_addrs
                        .iter()
                        .map(|addr| pool.get_or_create_sender(addr))
                        .collect()
                })
                .unwrap_or_default();
            if let Some(stream) = streams_guard.get(&stream_id) {
                stream.set_downstream_txs(txs);
            }
        } else if let Some(stream) = streams_guard.get(&stream_id) {
            stream.set_downstream_txs(Vec::new());
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
