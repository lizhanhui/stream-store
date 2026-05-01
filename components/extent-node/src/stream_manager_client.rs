//! RAII client for the StreamManager connection lifecycle.
//!
//! [`StreamManagerClient`] manages two independent connections to StreamManager:
//!
//! 1. **Heartbeat connection** — dedicated to periodic heartbeats. No extent
//!    updates touch this connection, so heartbeat latency is bounded by metric
//!    collection (microseconds) + network RTT + SM MySQL heartbeat queries.
//!
//! 2. **Update connection** — dedicated to extent updates (Progress + Flushed).
//!    Fire-and-forget sends on a separate TCP stream, so slow SM MySQL queries
//!    cannot cause TCP backpressure on the heartbeat connection.
//!
//! Both connections perform their own Connect handshake and reconnect
//! independently on failure. On shutdown, the heartbeat task sends Disconnect;
//! the update task simply drops its connection.
//!
//! Created via [`StreamManagerClient::spawn`], which starts both background
//! tasks. When the value is dropped, both tasks receive a signal and shut down.

use std::sync::Arc;
use std::time::Duration;

use common::errors::{InternalSnafu, StorageError};
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

/// Manages the full lifecycle of two connections to StreamManager:
/// a heartbeat connection and an extent update connection.
///
/// Created via [`StreamManagerClient::spawn`]. When dropped, both background
/// tasks are signaled to shut down.
pub struct StreamManagerClient {
    /// Dropping these senders signals the background tasks to shut down.
    _heartbeat_shutdown_tx: oneshot::Sender<()>,
    _update_shutdown_tx: oneshot::Sender<()>,
    /// Handles to the background tasks for explicit join-on-stop.
    heartbeat_handle: JoinHandle<()>,
    update_handle: JoinHandle<()>,
}

impl StreamManagerClient {
    /// Spawn both background tasks: heartbeat and extent update.
    ///
    /// The heartbeat task manages the heartbeat connection exclusively.
    /// The update task manages a separate connection for extent updates.
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        store: Arc<ExtentNodeStore>,
        node_id: String,
        advertise_addr: String,
        stream_manager_addrs: Vec<String>,
        heartbeat_interval_ms: u32,
        rpc_connect_timeout: Duration,
        rpc_request_timeout: Duration,
        update_rx: mpsc::Receiver<ExtentUpdate>,
    ) -> Self {
        let (hb_shutdown_tx, hb_shutdown_rx) = oneshot::channel::<()>();
        let (upd_shutdown_tx, upd_shutdown_rx) = oneshot::channel::<()>();

        // Heartbeat task — dedicated connection, no extent updates.
        let hb_store = Arc::clone(&store);
        let hb_node_id = node_id.clone();
        let hb_addr = advertise_addr.clone();
        let hb_sm_addrs = stream_manager_addrs.clone();
        let heartbeat_handle = tokio::spawn(async move {
            Self::heartbeat_loop(
                hb_store,
                hb_node_id,
                hb_addr,
                hb_sm_addrs,
                heartbeat_interval_ms,
                hb_shutdown_rx,
                rpc_connect_timeout,
                rpc_request_timeout,
            )
            .await;
        });

        // Update task — dedicated connection for extent updates.
        let upd_store = Arc::clone(&store);
        let upd_node_id = node_id;
        let upd_addr = advertise_addr;
        let upd_sm_addrs = stream_manager_addrs;
        let update_handle = tokio::spawn(async move {
            Self::update_loop(
                upd_store,
                upd_node_id,
                upd_addr,
                upd_sm_addrs,
                heartbeat_interval_ms,
                upd_shutdown_rx,
                rpc_connect_timeout,
                rpc_request_timeout,
                update_rx,
            )
            .await;
        });

        StreamManagerClient {
            _heartbeat_shutdown_tx: hb_shutdown_tx,
            _update_shutdown_tx: upd_shutdown_tx,
            heartbeat_handle,
            update_handle,
        }
    }

    /// Explicitly stop and wait for both background tasks to complete.
    pub async fn stop(self) {
        drop(self._heartbeat_shutdown_tx);
        drop(self._update_shutdown_tx);
        match tokio::time::timeout(Duration::from_secs(2), self.heartbeat_handle).await {
            Ok(_) => {}
            Err(_) => warn!("heartbeat task stop timed out"),
        }
        match tokio::time::timeout(Duration::from_secs(2), self.update_handle).await {
            Ok(_) => {}
            Err(_) => warn!("update task stop timed out"),
        }
    }

    /// Abort both background tasks immediately without sending Disconnect.
    pub fn abort(self) {
        self.heartbeat_handle.abort();
        self.update_handle.abort();
    }

    // ── Heartbeat connection ────────────────────────────────────────────

    /// Reconnection loop for the heartbeat connection.
    #[allow(clippy::too_many_arguments)]
    async fn heartbeat_loop(
        store: Arc<ExtentNodeStore>,
        node_id: String,
        advertise_addr: String,
        stream_manager_addrs: Vec<String>,
        heartbeat_interval_ms: u32,
        mut shutdown_rx: oneshot::Receiver<()>,
        rpc_connect_timeout: Duration,
        rpc_request_timeout: Duration,
    ) {
        let mut addr_index: usize = 0;
        loop {
            let addr = &stream_manager_addrs[addr_index % stream_manager_addrs.len()];
            match Self::heartbeat_session(
                &store,
                &node_id,
                &advertise_addr,
                addr,
                heartbeat_interval_ms,
                &mut shutdown_rx,
                rpc_connect_timeout,
                rpc_request_timeout,
            )
            .await
            {
                Ok(true) => {
                    info!("sent Disconnect to StreamManager; shutting down");
                    return;
                }
                Ok(false) => {
                    info!("StreamManager heartbeat connection to {addr} closed gracefully");
                }
                Err(e) => {
                    warn!("StreamManager heartbeat connection to {addr} failed: {e}");
                }
            }
            addr_index += 1;
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(2)) => {}
                _ = &mut shutdown_rx => {
                    info!("heartbeat loop received shutdown signal during reconnect wait");
                    return;
                }
            }
        }
    }

    /// Single heartbeat session: Connect, heartbeat loop, Disconnect on shutdown.
    #[allow(clippy::too_many_arguments)]
    async fn heartbeat_session(
        store: &Arc<ExtentNodeStore>,
        node_id: &str,
        advertise_addr: &str,
        stream_manager_addr: &str,
        heartbeat_interval_ms: u32,
        shutdown_rx: &mut oneshot::Receiver<()>,
        rpc_connect_timeout: Duration,
        rpc_request_timeout: Duration,
    ) -> Result<bool, StorageError> {
        let mut framed = Self::connect_and_handshake(
            node_id,
            advertise_addr,
            stream_manager_addr,
            heartbeat_interval_ms,
            rpc_connect_timeout,
            rpc_request_timeout,
        )
        .await?;
        info!("heartbeat connection established to StreamManager at {stream_manager_addr}");

        let interval_duration = Duration::from_millis(heartbeat_interval_ms as u64);
        let mut heartbeat_interval = tokio::time::interval(interval_duration);
        heartbeat_interval.tick().await; // consume the first immediate tick
        let mut request_id = 1u32;

        loop {
            tokio::select! {
                _ = heartbeat_interval.tick() => {}
                _ = &mut *shutdown_rx => {
                    // Graceful shutdown: send Disconnect before closing.
                    info!("shutdown signal received; sending Disconnect to StreamManager");
                    let disconnect_frame = Frame::new(
                        VariableHeader::Disconnect { request_id },
                        Some(build_disconnect_payload(node_id)),
                    );
                    if let Err(e) = framed.send(disconnect_frame).await {
                        warn!("failed to send Disconnect to StreamManager: {e}");
                        return Ok(true);
                    }
                    match tokio::time::timeout(Duration::from_millis(500), framed.next()).await {
                        Ok(Some(Ok(resp))) if resp.opcode() == Opcode::Disconnect => {
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
                .map_err(|_| {
                    InternalSnafu {
                        message: "timeout sending Heartbeat",
                    }
                    .build()
                })??;

            match tokio::time::timeout(rpc_request_timeout, framed.next()).await {
                Ok(Some(Ok(resp))) if resp.opcode() == Opcode::Heartbeat => {
                    // Heartbeat acknowledged.
                }
                Ok(Some(Ok(resp))) => {
                    warn!("unexpected heartbeat response: {:?}", resp.opcode());
                }
                Ok(Some(Err(e))) => return Err(e),
                Ok(None) => {
                    return Err(InternalSnafu {
                        message: "StreamManager connection closed",
                    }
                    .build());
                }
                Err(_) => {
                    return Err(InternalSnafu {
                        message: "timeout waiting for Heartbeat response",
                    }
                    .build());
                }
            }
        }
    }

    // ── Update connection ───────────────────────────────────────────────

    /// Reconnection loop for the extent update connection.
    #[allow(clippy::too_many_arguments)]
    async fn update_loop(
        store: Arc<ExtentNodeStore>,
        node_id: String,
        advertise_addr: String,
        stream_manager_addrs: Vec<String>,
        heartbeat_interval_ms: u32,
        mut shutdown_rx: oneshot::Receiver<()>,
        rpc_connect_timeout: Duration,
        rpc_request_timeout: Duration,
        mut update_rx: mpsc::Receiver<ExtentUpdate>,
    ) {
        let mut addr_index: usize = 0;
        loop {
            let addr = &stream_manager_addrs[addr_index % stream_manager_addrs.len()];
            match Self::update_session(
                &store,
                &node_id,
                &advertise_addr,
                addr,
                heartbeat_interval_ms,
                &mut shutdown_rx,
                rpc_connect_timeout,
                rpc_request_timeout,
                &mut update_rx,
            )
            .await
            {
                Ok(()) => {
                    // Shutdown signal received.
                    return;
                }
                Err(e) => {
                    warn!("StreamManager update connection to {addr} failed: {e}");
                }
            }
            addr_index += 1;
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(2)) => {}
                _ = &mut shutdown_rx => {
                    info!("update loop received shutdown signal during reconnect wait");
                    return;
                }
            }
        }
    }

    /// Single update session: Connect, drain extent updates, send progress.
    #[allow(clippy::too_many_arguments)]
    async fn update_session(
        store: &Arc<ExtentNodeStore>,
        node_id: &str,
        advertise_addr: &str,
        stream_manager_addr: &str,
        heartbeat_interval_ms: u32,
        shutdown_rx: &mut oneshot::Receiver<()>,
        rpc_connect_timeout: Duration,
        rpc_request_timeout: Duration,
        update_rx: &mut mpsc::Receiver<ExtentUpdate>,
    ) -> Result<(), StorageError> {
        let mut framed = Self::connect_and_handshake(
            node_id,
            advertise_addr,
            stream_manager_addr,
            heartbeat_interval_ms,
            rpc_connect_timeout,
            rpc_request_timeout,
        )
        .await?;
        info!("update connection established to StreamManager at {stream_manager_addr}");

        // Use the heartbeat interval for periodic progress updates too.
        let interval_duration = Duration::from_millis(heartbeat_interval_ms as u64);
        let mut progress_interval = tokio::time::interval(interval_duration);
        progress_interval.tick().await; // consume the first immediate tick

        loop {
            tokio::select! {
                Some(update) = update_rx.recv() => {
                    Self::send_update_frame(&mut framed, update, rpc_request_timeout).await;
                    // Drain any additional queued updates to batch them.
                    while let Ok(update) = update_rx.try_recv() {
                        Self::send_update_frame(&mut framed, update, rpc_request_timeout).await;
                    }
                }
                _ = progress_interval.tick() => {
                    // Periodic progress updates for all active extents.
                    for (stream_id, extent_id, current_offset, epoch) in store.snapshot_active_extents() {
                        Self::send_update_frame(
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
                _ = &mut *shutdown_rx => {
                    info!("update connection received shutdown signal");
                    return Ok(());
                }
            }
        }
    }

    // ── Shared helpers ──────────────────────────────────────────────────

    /// TCP connect + Connect handshake. Shared by both connections.
    async fn connect_and_handshake(
        node_id: &str,
        advertise_addr: &str,
        stream_manager_addr: &str,
        heartbeat_interval_ms: u32,
        rpc_connect_timeout: Duration,
        rpc_request_timeout: Duration,
    ) -> Result<Framed<TcpStream, FrameCodec>, StorageError> {
        let stream =
            tokio::time::timeout(rpc_connect_timeout, TcpStream::connect(stream_manager_addr))
                .await
                .map_err(|_| {
                    InternalSnafu {
                        message: format!("connect timeout to {stream_manager_addr}"),
                    }
                    .build()
                })??;
        stream.set_nodelay(true).map_err(|e| {
            InternalSnafu {
                message: format!("set TCP_NODELAY: {e}"),
            }
            .build()
        })?;
        let mut framed = Framed::new(stream, FrameCodec);

        // Send Connect.
        let connect_payload = build_connect_payload(node_id, advertise_addr, heartbeat_interval_ms);
        let connect_frame = Frame::new(
            VariableHeader::Connect { request_id: 0 },
            Some(connect_payload),
        );
        tokio::time::timeout(rpc_request_timeout, framed.send(connect_frame))
            .await
            .map_err(|_| {
                InternalSnafu {
                    message: "timeout sending Connect frame",
                }
                .build()
            })??;

        match tokio::time::timeout(rpc_request_timeout, framed.next()).await {
            Ok(Some(Ok(resp))) if resp.opcode() == Opcode::Connect => {
                info!("registered with StreamManager at {stream_manager_addr}");
            }
            Ok(Some(Ok(resp))) => {
                error!("unexpected Connect response: {:?}", resp.opcode());
                return Err(InternalSnafu {
                    message: "unexpected Connect response",
                }
                .build());
            }
            Ok(Some(Err(e))) => return Err(e),
            Ok(None) => {
                return Err(InternalSnafu {
                    message: "StreamManager connection closed after Connect",
                }
                .build());
            }
            Err(_) => {
                return Err(InternalSnafu {
                    message: "timeout waiting for ConnectAck from StreamManager",
                }
                .build());
            }
        }

        Ok(framed)
    }

    /// Send a single UPDATE_EXTENT frame on an SM connection.
    /// Fire-and-forget: logs and drops on failure.
    async fn send_update_frame(
        framed: &mut Framed<TcpStream, FrameCodec>,
        update: ExtentUpdate,
        rpc_request_timeout: Duration,
    ) {
        let (frame, desc) = match update {
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
                format!("UpdateExtentProgress stream={stream_id}"),
            ),
            ExtentUpdate::Flushed {
                stream_id,
                extent_id,
                epoch,
                start_offset,
                end_offset,
            } => (
                Frame::new(
                    VariableHeader::UpdateExtentFlushed {
                        stream_id,
                        epoch,
                        extent_id,
                        start_offset: Offset(start_offset),
                        end_offset: Offset(end_offset),
                    },
                    None,
                ),
                format!("UpdateExtentFlushed stream={stream_id} extent={extent_id}"),
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
