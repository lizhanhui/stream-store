pub mod downstream;
pub mod extent;
pub mod store;
pub mod stream;
pub mod stream_manager_client;

use std::net::SocketAddr;
use std::sync::Arc;

use common::config::{ExtentNodeConfig, resolve_advertise_ip};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::downstream::DownstreamPool;
use crate::store::{ExtentNodeStore, SealRequest};
use crate::stream_manager_client::StreamManagerClient;

/// A running ExtentNode with lifecycle management.
///
/// Created via [`ExtentNode::start`], which binds the listener, spawns all background
/// tasks, and returns a handle. Call [`ExtentNode::stop`] for graceful shutdown.
pub struct ExtentNode {
    /// The address this ExtentNode is listening on.
    addr: SocketAddr,
    /// Shutdown signal sender — sending triggers graceful stop of non-heartbeat tasks.
    shutdown_tx: broadcast::Sender<()>,
    /// JoinHandles for spawned background tasks (accept loop, seal notify).
    task_handles: Vec<JoinHandle<()>>,
    /// RAII client managing the StreamManager connection lifecycle.
    /// Sends Disconnect on drop; call `stop()` for guaranteed delivery.
    stream_manager_client: StreamManagerClient,
    /// Downstream connection pool — needs explicit shutdown to abort reader tasks.
    downstream: Arc<DownstreamPool>,
}

impl ExtentNode {
    /// Background task that receives SealRequest notifications and sends
    /// NOTIFY_SEALED_EXTENT frames to Stream Manager.
    ///
    /// Fire-and-forget: if the SM connection fails, the notification is logged
    /// and dropped. SM will reconcile during the next epoch bump.
    async fn seal_notify_task(
        mut seal_rx: mpsc::Receiver<SealRequest>,
        sm_addr: String,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) {
        use common::types::Offset;
        use rpc::frame::{Frame, VariableHeader};

        while let Some(req) = tokio::select! {
            req = seal_rx.recv() => req,
            _ = shutdown_rx.recv() => None,
        } {
            if sm_addr.is_empty() {
                continue;
            }
            // Send NOTIFY_SEALED_EXTENT to SM via a fire-and-forget connection.
            match client::StorageClient::connect(&sm_addr).await {
                Ok(client) => {
                    let frame = Frame::new(
                        VariableHeader::NotifySealedExtent {
                            stream_id: req.stream_id,
                            epoch: req.epoch,
                            sealed_extent_id: req.sealed_extent_id,
                            end_offset: Offset(req.end_offset),
                            new_extent_id: req.new_extent_id,
                        },
                        None,
                    );
                    if let Err(e) = client.send_frame_no_response(frame).await {
                        warn!(
                            "Failed to send NotifySealedExtent to SM for stream {:?}: {e}",
                            req.stream_id
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to connect to SM for NotifySealedExtent: {e}"
                    );
                }
            }
        }
    }

    /// Query available and total system memory via sysinfo.
    pub(crate) fn get_memory_info() -> (u64, u64) {
        use sysinfo::System;
        let mut sys = System::new();
        sys.refresh_memory();
        (sys.available_memory(), sys.total_memory())
    }

    /// Start the ExtentNode.
    ///
    /// 1. Bind the listener and determine the actual bound address.
    /// 2. Create ExtentNodeStore and DownstreamPool (direct TCP, no channels).
    /// 3. Spawn the StreamManagerClient (heartbeat lifecycle with RAII Disconnect).
    /// 4. Spawn the accept loop.
    ///
    /// Returns an `ExtentNode` handle for lifecycle management.
    pub async fn start(config: ExtentNodeConfig) -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);
        let mut task_handles = Vec::new();

        // Bind listener and resolve the OS-assigned address (especially when port 0 is used).
        let listener = TcpListener::bind(&config.listen_addr())
            .await
            .expect("failed to bind ExtentNode listener");
        let local_addr = listener
            .local_addr()
            .expect("failed to get ExtentNode local address");
        info!("ExtentNode server bound on {local_addr}");

        // Create store first (OnceLock for downstream breaks circular dep).
        let mut store_inner = ExtentNodeStore::new();
        store_inner.set_arena_capacity(config.extent_arena_capacity);

        // Wire up the seal notification channel for autonomous extent creation.
        // The receiver task sends NOTIFY_SEALED_EXTENT frames to Stream Manager.
        let (seal_tx, seal_rx) = mpsc::channel::<SealRequest>(64);
        store_inner.set_seal_tx(seal_tx);

        let store = Arc::new(store_inner);

        // Spawn background task for NOTIFY_SEALED_EXTENT notifications to SM.
        let sm_addr_for_notify = config.stream_manager_addr.clone();
        let seal_shutdown_rx = shutdown_tx.subscribe();
        task_handles.push(tokio::spawn(
            Self::seal_notify_task(seal_rx, sm_addr_for_notify, seal_shutdown_rx),
        ));

        // Create DownstreamPool with back-reference to store (for inline watermark processing).
        let downstream = Arc::new(DownstreamPool::new(Arc::clone(&store)));
        // Wire pool into store.
        store.set_downstream(Arc::clone(&downstream));

        // Resolve advertise_addr: auto-detect IP if bind_ip is 0.0.0.0 and advertise_ip not set.
        // If port was 0 (OS-assigned), use the actual bound port instead.
        let effective_port = if config.port == 0 {
            local_addr.port()
        } else {
            config.port
        };
        let effective_ip = resolve_advertise_ip(&config.bind_ip, &config.advertise_ip);
        let advertise_addr = format!("{effective_ip}:{effective_port}");

        // Resolve node_id: use config value if set, otherwise fall back to advertise_addr.
        let node_id = if config.node_id.is_empty() {
            advertise_addr.clone()
        } else {
            config.node_id.clone()
        };

        // Spawn StreamManagerClient (RAII: sends Disconnect when dropped).
        let stream_manager_client = StreamManagerClient::spawn(
            Arc::clone(&store),
            node_id,
            advertise_addr,
            config.stream_manager_addr.clone(),
            config.heartbeat_interval_ms,
        );

        // Spawn accept loop.
        let server_shutdown = shutdown_tx.subscribe();
        task_handles.push(tokio::spawn(async move {
            server::Server::builder("ExtentNode")
                .listener(listener)
                .handler(store)
                .deferred(true)
                .shutdown(server_shutdown)
                .build()
                .run()
                .await;
        }));

        ExtentNode {
            addr: local_addr,
            shutdown_tx,
            task_handles,
            stream_manager_client,
            downstream,
        }
    }

    /// The address this ExtentNode is listening on.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Gracefully stop the ExtentNode: signal all tasks and await their completion.
    pub async fn stop(self) {
        info!("ExtentNode stopping...");
        // 1. Signal non-heartbeat tasks (accept loop, seal notify).
        let _ = self.shutdown_tx.send(());
        // 2. Abort downstream reader tasks (they block on TCP reads indefinitely).
        self.downstream.shutdown().await;
        // 3. Stop StreamManagerClient (sends Disconnect, awaits task).
        self.stream_manager_client.stop().await;
        // 4. Await remaining task handles.
        for handle in self.task_handles {
            let _ = handle.await;
        }
        info!("ExtentNode stopped");
    }
}
