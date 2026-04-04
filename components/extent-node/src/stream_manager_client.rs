//! RAII client for the StreamManager connection lifecycle.
//!
//! [`StreamManagerClient`] manages the full lifecycle of the connection to
//! StreamManager: TCP connect, Connect handshake, periodic Heartbeat,
//! UPDATE_EXTENT notifications (sealed + progress), reconnection on failure,
//! and graceful Disconnect on drop.
//!
//! Created via [`StreamManagerClient::spawn`], which starts an internal
//! background task. When the value is dropped, the background task receives
//! a signal (via `oneshot::Sender` drop) and sends a Disconnect frame before
//! exiting.

use std::sync::Arc;
use std::time::Duration;

use common::errors::StorageError;
use common::types::{NodeMetrics, Offset, Opcode};
use futures_util::{SinkExt, StreamExt};
use rpc::codec::FrameCodec;
use rpc::frame::{Frame, VariableHeader};
use rpc::payload::{build_connect_payload, build_disconnect_payload, build_heartbeat_payload};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::codec::Framed;
use tracing::{error, info, warn};

use crate::ExtentNode;
use crate::store::{ExtentNodeStore, ExtentUpdate};

/// Manages the full lifecycle of the connection to StreamManager:
/// TCP connect, Connect handshake, periodic Heartbeat, UPDATE_EXTENT
/// notifications (sealed + progress), reconnection on failure, and graceful
/// Disconnect on drop (RAII).
///
/// Created via [`StreamManagerClient::spawn`], which starts an internal background
/// task. When the `StreamManagerClient` value is dropped, the background task
/// receives a signal and sends a Disconnect frame before exiting.
///
/// For guaranteed delivery of the Disconnect frame, call [`stop`](Self::stop)
/// which awaits the background task. A plain drop signals the task but cannot
/// await it (Rust's `Drop` is synchronous).
pub struct StreamManagerClient {
    /// Dropping this sender signals the background task to shut down.
    /// The implicit drop is the RAII mechanism — no explicit `send()` needed.
    _shutdown_tx: oneshot::Sender<()>,
    /// Handle to the background task for explicit join-on-stop.
    task_handle: JoinHandle<()>,
}

impl StreamManagerClient {
    /// Spawn the background connection + heartbeat task.
    ///
    /// The task immediately attempts to connect to StreamManager and enters
    /// the reconnection loop. Returns a handle that, when dropped, triggers
    /// graceful Disconnect.
    ///
    /// `update_rx` receives extent update notifications from the store's
    /// autonomous extent creation path. These are multiplexed onto the same
    /// SM connection alongside heartbeats. Progress updates for active extents
    /// are also sent after each heartbeat.
    pub fn spawn(
        store: Arc<ExtentNodeStore>,
        node_id: String,
        advertise_addr: String,
        stream_manager_addr: String,
        heartbeat_interval_ms: u32,
        rpc_connect_timeout: Duration,
        rpc_request_timeout: Duration,
        update_rx: mpsc::Receiver<ExtentUpdate>,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let task_handle = tokio::spawn(async move {
            Self::run_loop(
                store,
                node_id,
                advertise_addr,
                stream_manager_addr,
                heartbeat_interval_ms,
                shutdown_rx,
                rpc_connect_timeout,
                rpc_request_timeout,
                update_rx,
            )
            .await;
        });

        StreamManagerClient {
            _shutdown_tx: shutdown_tx,
            task_handle,
        }
    }

    /// Explicitly stop and wait for the background task to complete.
    ///
    /// Consumes `self`, which drops `_shutdown_tx` and signals the background
    /// task. Then awaits the task handle so the Disconnect frame is guaranteed
    /// to be sent (or attempted) before this method returns.
    pub async fn stop(self) {
        // Dropping self._shutdown_tx signals the task.
        // We need to destructure to get task_handle without triggering
        // implicit Drop ordering issues.
        let task_handle = self.task_handle;
        // _shutdown_tx is dropped here when `self` is consumed.
        drop(self._shutdown_tx);
        // Wait for the task to finish, but cap at 5 seconds to avoid blocking
        // shutdown if the task is stuck in an RPC or reconnect wait.
        match tokio::time::timeout(Duration::from_secs(2), task_handle).await {
            Ok(_) => {}
            Err(_) => {
                warn!("StreamManagerClient stop timed out after 5s; abandoning task");
            }
        }
    }

    /// Reconnection loop. Runs until shutdown signal.
    async fn run_loop(
        store: Arc<ExtentNodeStore>,
        node_id: String,
        advertise_addr: String,
        stream_manager_addr: String,
        heartbeat_interval_ms: u32,
        mut shutdown_rx: oneshot::Receiver<()>,
        rpc_connect_timeout: Duration,
        rpc_request_timeout: Duration,
        mut update_rx: mpsc::Receiver<ExtentUpdate>,
    ) {
        loop {
            match Self::connect_and_heartbeat(
                &store,
                &node_id,
                &advertise_addr,
                &stream_manager_addr,
                heartbeat_interval_ms,
                &mut shutdown_rx,
                rpc_connect_timeout,
                rpc_request_timeout,
                &mut update_rx,
            )
            .await
            {
                Ok(true) => {
                    // Cleanly disconnected via shutdown signal.
                    info!("sent Disconnect to StreamManager; shutting down");
                    return;
                }
                Ok(false) => {
                    info!("StreamManager connection closed gracefully");
                }
                Err(e) => {
                    warn!("StreamManager connection error: {e}; will retry in 5s");
                }
            }

            // Wait before reconnecting, but also listen for shutdown.
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(2)) => {}
                _ = &mut shutdown_rx => {
                    info!("heartbeat loop received shutdown signal during reconnect wait");
                    return;
                }
            }
        }
    }

    /// Single connection attempt: TCP connect, Connect handshake, heartbeat loop,
    /// Disconnect on shutdown.
    ///
    /// Returns `Ok(true)` if shutdown was handled cleanly (Disconnect sent),
    /// `Ok(false)` if the connection ended for other reasons.
    async fn connect_and_heartbeat(
        store: &Arc<ExtentNodeStore>,
        node_id: &str,
        advertise_addr: &str,
        stream_manager_addr: &str,
        heartbeat_interval_ms: u32,
        shutdown_rx: &mut oneshot::Receiver<()>,
        rpc_connect_timeout: Duration,
        rpc_request_timeout: Duration,
        update_rx: &mut mpsc::Receiver<ExtentUpdate>,
    ) -> Result<bool, StorageError> {
        let stream =
            tokio::time::timeout(rpc_connect_timeout, TcpStream::connect(stream_manager_addr))
                .await
                .map_err(|_| {
                    StorageError::Internal(format!("connect timeout to {stream_manager_addr}"))
                })??;
        stream
            .set_nodelay(true)
            .map_err(|e| StorageError::Internal(format!("set TCP_NODELAY: {e}")))?;
        let mut framed = Framed::new(stream, FrameCodec);
        info!("connected to StreamManager at {stream_manager_addr}");

        // Send Connect.
        let connect_payload = build_connect_payload(node_id, advertise_addr, heartbeat_interval_ms);
        let connect_frame = Frame::new(
            VariableHeader::Connect { request_id: 0 },
            Some(connect_payload),
        );
        tokio::time::timeout(rpc_request_timeout, framed.send(connect_frame))
            .await
            .map_err(|_| StorageError::Internal("timeout sending Connect frame".into()))??;

        match tokio::time::timeout(rpc_request_timeout, framed.next()).await {
            Ok(Some(Ok(resp))) if resp.opcode() == Opcode::ConnectAck => {
                info!("registered with StreamManager");
            }
            Ok(Some(Ok(resp))) => {
                error!("unexpected Connect response: {:?}", resp.opcode());
                return Err(StorageError::Internal("unexpected Connect response".into()));
            }
            Ok(Some(Err(e))) => return Err(e),
            Ok(None) => {
                return Err(StorageError::Internal(
                    "StreamManager connection closed after Connect".into(),
                ));
            }
            Err(_) => {
                return Err(StorageError::Internal(
                    "timeout waiting for ConnectAck from StreamManager".into(),
                ));
            }
        }

        // Periodic heartbeat with runtime metrics.
        let interval = Duration::from_millis(heartbeat_interval_ms as u64);
        let mut request_id = 1u32;

        loop {
            // Sleep until the next heartbeat, but also watch for shutdown and extent updates.
            tokio::select! {
                _ = tokio::time::sleep(interval) => {}
                Some(update) = update_rx.recv() => {
                    // Extent update: send on the existing connection (fire-and-forget).
                    Self::send_extent_update(&mut framed, update, rpc_request_timeout).await;
                    // Drain any additional queued updates to batch them.
                    while let Ok(update) = update_rx.try_recv() {
                        Self::send_extent_update(&mut framed, update, rpc_request_timeout).await;
                    }
                    continue;
                }
                _ = &mut *shutdown_rx => {
                    // Graceful shutdown: send Disconnect before closing.
                    info!("shutdown signal received; sending Disconnect to StreamManager");
                    let disconnect_frame = Frame::new(VariableHeader::Disconnect { request_id }, Some(build_disconnect_payload(node_id)));
                    if let Err(e) = framed.send(disconnect_frame).await {
                        warn!("failed to send Disconnect to StreamManager: {e}");
                        return Ok(true);
                    }
                    // Wait for DisconnectAck (best-effort, with a short timeout).
                    match tokio::time::timeout(Duration::from_millis(500), framed.next()).await {
                        Ok(Some(Ok(resp))) if resp.opcode() == Opcode::DisconnectAck => {
                            info!("received DisconnectAck from StreamManager");
                        }
                        Ok(Some(Ok(resp))) => {
                            warn!("unexpected response to Disconnect: {:?}", resp.opcode());
                        }
                        Ok(Some(Err(e))) => {
                            warn!("error reading DisconnectAck: {e}");
                        }
                        Ok(None) => {
                            warn!("StreamManager closed connection before DisconnectAck");
                        }
                        Err(_) => {
                            warn!("timed out waiting for DisconnectAck");
                        }
                    }
                    return Ok(true);
                }
            }

            // Snapshot metrics from the store (lock-free: uses atomic swap).
            let (appends, bytes_written, active_count) = store.snapshot_metrics();

            // Compute per-second rates.
            let elapsed_secs = (heartbeat_interval_ms as f64) / 1000.0;
            let appends_per_sec = if elapsed_secs > 0.0 {
                (appends as f64 / elapsed_secs) as u32
            } else {
                0
            };
            let bytes_per_sec = if elapsed_secs > 0.0 {
                (bytes_written as f64 / elapsed_secs) as u64
            } else {
                0
            };

            let (avail_mem, total_mem) = ExtentNode::get_memory_info();

            let metrics = NodeMetrics {
                available_memory_bytes: avail_mem,
                total_memory_bytes: total_mem,
                appends_per_sec,
                active_extent_count: active_count,
                bytes_written_per_sec: bytes_per_sec,
            };

            let heartbeat_payload = build_heartbeat_payload(node_id, &metrics);

            let hb_frame = Frame::new(
                VariableHeader::Heartbeat { request_id },
                Some(heartbeat_payload),
            );
            request_id = request_id.wrapping_add(1);

            tokio::time::timeout(rpc_request_timeout, framed.send(hb_frame))
                .await
                .map_err(|_| StorageError::Internal("timeout sending Heartbeat".into()))??;

            match tokio::time::timeout(rpc_request_timeout, framed.next()).await {
                Ok(Some(Ok(resp))) if resp.opcode() == Opcode::Heartbeat => {
                    // Heartbeat acknowledged.
                }
                Ok(Some(Ok(resp))) => {
                    warn!("unexpected heartbeat response: {:?}", resp.opcode());
                }
                Ok(Some(Err(e))) => return Err(e),
                Ok(None) => {
                    return Err(StorageError::Internal(
                        "StreamManager connection closed".into(),
                    ));
                }
                Err(_) => {
                    return Err(StorageError::Internal(
                        "timeout waiting for Heartbeat response".into(),
                    ));
                }
            }

            // After each heartbeat, send progress updates for all active extents.
            for (stream_id, extent_id, current_offset, epoch) in store.snapshot_active_extents() {
                Self::send_extent_update(
                    &mut framed,
                    ExtentUpdate::Progress {
                        stream_id,
                        extent_id,
                        current_offset,
                        epoch,
                    },
                    rpc_request_timeout,
                )
                .await;
            }
        }
    }

    /// Send a single UPDATE_EXTENT frame on the SM connection.
    /// Fire-and-forget: logs and drops on failure.
    async fn send_extent_update(
        framed: &mut Framed<TcpStream, FrameCodec>,
        update: ExtentUpdate,
        rpc_request_timeout: Duration,
    ) {
        let (frame, desc) = match update {
            ExtentUpdate::Sealed {
                stream_id,
                sealed_extent_id,
                end_offset,
                new_extent_id,
                epoch,
            } => (
                Frame::new(
                    VariableHeader::UpdateExtentSealed {
                        stream_id,
                        epoch,
                        sealed_extent_id,
                        end_offset: Offset(end_offset),
                        new_extent_id,
                    },
                    None,
                ),
                format!("UpdateExtentSealed stream={stream_id:?}"),
            ),
            ExtentUpdate::Progress {
                stream_id,
                extent_id,
                current_offset,
                epoch,
            } => (
                Frame::new(
                    VariableHeader::UpdateExtentProgress {
                        stream_id,
                        epoch,
                        extent_id,
                        current_offset: Offset(current_offset),
                    },
                    None,
                ),
                format!("UpdateExtentProgress stream={stream_id:?}"),
            ),
        };
        match tokio::time::timeout(rpc_request_timeout, framed.send(frame)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                warn!("failed to send {desc}: {e}");
            }
            Err(_) => {
                warn!("timeout sending {desc}");
            }
        }
    }
}
