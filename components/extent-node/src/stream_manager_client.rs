//! RAII client for the StreamManager connection lifecycle.
//!
//! [`StreamManagerClient`] manages the full lifecycle of the connection to
//! StreamManager: TCP connect, Connect handshake, periodic Heartbeat,
//! reconnection on failure, and graceful Disconnect on drop.
//!
//! Created via [`StreamManagerClient::spawn`], which starts an internal
//! background task. When the value is dropped, the background task receives
//! a signal (via `oneshot::Sender` drop) and sends a Disconnect frame before
//! exiting.

use std::sync::Arc;
use std::time::Duration;

use common::types::Opcode;
use futures_util::{SinkExt, StreamExt};
use rpc::codec::FrameCodec;
use rpc::frame::Frame;
use rpc::payload::{build_connect_payload, build_disconnect_payload, build_heartbeat_payload};
use common::errors::StorageError;
use common::types::NodeMetrics;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_util::codec::Framed;
use tracing::{error, info, warn};

use crate::store::ExtentNodeStore;
use crate::ExtentNode;

/// Manages the full lifecycle of the connection to StreamManager:
/// TCP connect, Connect handshake, periodic Heartbeat, reconnection on failure,
/// and graceful Disconnect on drop (RAII).
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
    pub fn spawn(
        store: Arc<ExtentNodeStore>,
        advertised_addr: String,
        stream_manager_addr: String,
        heartbeat_interval_ms: u32,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let task_handle = tokio::spawn(async move {
            Self::run_loop(
                store,
                advertised_addr,
                stream_manager_addr,
                heartbeat_interval_ms,
                shutdown_rx,
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
        let _ = task_handle.await;
    }

    /// Reconnection loop. Runs until shutdown signal.
    async fn run_loop(
        store: Arc<ExtentNodeStore>,
        advertised_addr: String,
        stream_manager_addr: String,
        heartbeat_interval_ms: u32,
        mut shutdown_rx: oneshot::Receiver<()>,
    ) {
        loop {
            match Self::connect_and_heartbeat(
                &store,
                &advertised_addr,
                &stream_manager_addr,
                heartbeat_interval_ms,
                &mut shutdown_rx,
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
                _ = tokio::time::sleep(Duration::from_secs(5)) => {}
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
        advertised_addr: &str,
        stream_manager_addr: &str,
        heartbeat_interval_ms: u32,
        shutdown_rx: &mut oneshot::Receiver<()>,
    ) -> Result<bool, StorageError> {
        let stream = TcpStream::connect(stream_manager_addr).await?;
        let mut framed = Framed::new(stream, FrameCodec);
        info!("connected to StreamManager at {stream_manager_addr}");

        // Send Connect.
        let connect_payload = build_connect_payload(
            advertised_addr,
            advertised_addr,
            heartbeat_interval_ms,
        );
        let connect_frame = Frame {
            opcode: Opcode::Connect,
            payload: connect_payload,
            ..Default::default()
        };
        framed.send(connect_frame).await?;

        match framed.next().await {
            Some(Ok(resp)) if resp.opcode == Opcode::ConnectAck => {
                info!("registered with StreamManager");
            }
            Some(Ok(resp)) => {
                error!("unexpected Connect response: {:?}", resp.opcode);
                return Err(StorageError::Internal(
                    "unexpected Connect response".into(),
                ));
            }
            Some(Err(e)) => return Err(e),
            None => {
                return Err(StorageError::Internal(
                    "StreamManager connection closed after Connect".into(),
                ));
            }
        }

        // Periodic heartbeat with runtime metrics.
        let interval = Duration::from_millis(heartbeat_interval_ms as u64);
        let mut request_id = 1u32;

        loop {
            // Sleep until the next heartbeat, but also watch for shutdown.
            tokio::select! {
                _ = tokio::time::sleep(interval) => {}
                _ = &mut *shutdown_rx => {
                    // Graceful shutdown: send Disconnect before closing.
                    info!("shutdown signal received; sending Disconnect to StreamManager");
                    let disconnect_frame = Frame {
                        opcode: Opcode::Disconnect,
                        request_id,
                        payload: build_disconnect_payload(advertised_addr),
                        ..Default::default()
                    };
                    if let Err(e) = framed.send(disconnect_frame).await {
                        warn!("failed to send Disconnect to StreamManager: {e}");
                        return Ok(true);
                    }
                    // Wait for DisconnectAck (best-effort, with a short timeout).
                    match tokio::time::timeout(Duration::from_secs(2), framed.next()).await {
                        Ok(Some(Ok(resp))) if resp.opcode == Opcode::DisconnectAck => {
                            info!("received DisconnectAck from StreamManager");
                        }
                        Ok(Some(Ok(resp))) => {
                            warn!("unexpected response to Disconnect: {:?}", resp.opcode);
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

            let heartbeat_payload = build_heartbeat_payload(advertised_addr, &metrics);

            let hb_frame = Frame {
                opcode: Opcode::Heartbeat,
                request_id,
                payload: heartbeat_payload,
                ..Default::default()
            };
            request_id = request_id.wrapping_add(1);

            framed.send(hb_frame).await?;

            match framed.next().await {
                Some(Ok(resp)) if resp.opcode == Opcode::Heartbeat => {
                    // Heartbeat acknowledged.
                }
                Some(Ok(resp)) => {
                    warn!("unexpected heartbeat response: {:?}", resp.opcode);
                }
                Some(Err(e)) => return Err(e),
                None => {
                    return Err(StorageError::Internal(
                        "StreamManager connection closed".into(),
                    ));
                }
            }
        }
    }
}
