use std::sync::Arc;

use common::config::DEFAULT_EXTENT_CAPACITY;
use common::types::{ErrorCode, ExtentId, ExtentPolicy};
use rpc::frame::{Frame, VariableHeader};
use rpc::payload::{ROLE_PRIMARY, parse_register_extent_payload};
use tracing::info;

use super::{ExtentNodeStore, ReplicaInfo};

impl ExtentNodeStore {
    /// Handle RegisterEpoch from StreamManager: assign this ExtentNode a role in broadcast replication.
    ///
    /// Creates the stream locally (with the StreamManager-assigned stream_id) and stores replica info.
    pub(crate) fn handle_register_epoch(&self, frame: Frame) -> Frame {
        let (role, config) = match &frame.variable_header {
            VariableHeader::RegisterEpoch { role, config, .. } => (*role, *config),
            _ => {
                return Frame::error_from_request(
                    &frame,
                    ErrorCode::InternalError,
                    "invalid RegisterEpoch frame",
                );
            }
        };

        let stream_id = config.stream_id;
        let epoch = config.epoch;
        let replication_factor = config.replication_factor;

        // `extent_id` no longer travels on the wire. The EN's local bookkeeping
        // still identifies extents by id; later phases collapse that to
        // (stream_id, epoch). For now, synthesize a per-epoch extent id (the
        // old allocator produced 1-based ids, so we use `epoch + 1`).
        let extent_id = ExtentId(epoch.0 + 1);

        tracing::debug!(
            arena_class = ?config.arena_class,
            stream_id = %stream_id,
            "RegisterEpoch arena_class"
        );

        // Normalize capacity bounds: 0 means use default.
        let policy = ExtentPolicy {
            cache: config.policy.cache,
        };

        // Parse replica addresses from the payload.
        let replica_addrs =
            match parse_register_extent_payload(frame.payload.as_deref().unwrap_or_default()) {
                Some(addrs) => addrs,
                None => {
                    return Frame::error_from_request(
                        &frame,
                        ErrorCode::InternalError,
                        "invalid RegisterEpoch payload",
                    );
                }
            };

        // Create the stream locally if it doesn't exist, then register the new extent.
        // Skip extent creation if it already exists (idempotent — extent may have been
        // lazily created by a forwarded append that arrived before this RegisterEpoch).
        self.try_create_stream(stream_id, config.storage_class, &policy);

        // Register the extent (idempotent — skips if already exists).
        let streams_guard = self.streams.pin();
        if let Some(stream) = streams_guard.get(&stream_id) {
            if stream.with_extent(extent_id, |_| ()).is_none() {
                stream.register_extent(extent_id, stream.max_offset(), epoch, DEFAULT_EXTENT_CAPACITY);
            } else {
                // Extent already exists (lazy creation from Forward), but update epoch
                // from authoritative source (RegisterEpoch carries the real epoch).
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
            "RegisterEpoch: stream={}, extent={}, role={role_name}, rf={}, secondaries=[{addrs_info}]",
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
            VariableHeader::RegisterEpochAck {
                request_id: frame.request_id(),
                stream_id,
            },
            None,
        )
    }
}
