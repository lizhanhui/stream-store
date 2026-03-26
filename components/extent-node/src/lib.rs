pub mod downstream;
pub mod extent;
pub mod index_stream;
pub mod store;
pub mod stream;
pub mod stream_manager_client;
pub mod watermark;

use std::net::SocketAddr;
use std::sync::Arc;

use common::config::ExtentNodeConfig;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tracing::info;

use crate::store::ExtentNodeStore;
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
    /// JoinHandles for spawned background tasks (downstream, watermark, accept loop).
    task_handles: Vec<JoinHandle<()>>,
    /// RAII client managing the StreamManager connection lifecycle.
    /// Sends Disconnect on drop; call `stop()` for guaranteed delivery.
    stream_manager_client: StreamManagerClient,
}

impl ExtentNode {
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
    /// 2. Create channels for broadcast replication (ForwardRequest, WatermarkEvent).
    /// 3. Spawn DownstreamManager and WatermarkHandler tasks.
    /// 4. Spawn the StreamManagerClient (heartbeat lifecycle with RAII Disconnect).
    /// 5. Spawn the accept loop.
    ///
    /// Returns an `ExtentNode` handle for lifecycle management.
    pub async fn start(config: ExtentNodeConfig) -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);
        let mut task_handles = Vec::new();

        // Bind listener and resolve the OS-assigned address (especially when port 0 is used).
        let listener = TcpListener::bind(&config.listen_addr)
            .await
            .expect("failed to bind ExtentNode listener");
        let local_addr = listener
            .local_addr()
            .expect("failed to get ExtentNode local address");
        info!("ExtentNode server bound on {local_addr}");

        // Create broadcast replication channels.
        let (forward_tx, forward_rx) = mpsc::channel(4096);
        let (watermark_tx, watermark_rx) = mpsc::channel(4096);

        // Create store with broadcast replication support and per-stream fine-grained locking.
        let mut store_inner = ExtentNodeStore::with_forward_tx(forward_tx);
        store_inner.set_arena_capacity(config.extent_arena_capacity);
        let store = Arc::new(store_inner);

        // Spawn DownstreamManager task.
        let downstream_shutdown = shutdown_tx.subscribe();
        task_handles.push(tokio::spawn(async move {
            downstream::run_downstream_manager(forward_rx, watermark_tx, downstream_shutdown).await;
        }));

        // Spawn WatermarkHandler task.
        let store_for_watermark = Arc::clone(&store);
        let watermark_shutdown = shutdown_tx.subscribe();
        task_handles.push(tokio::spawn(async move {
            watermark::run_watermark_handler(watermark_rx, store_for_watermark, watermark_shutdown)
                .await;
        }));

        // Spawn StreamManagerClient (RAII: sends Disconnect when dropped).
        let stream_manager_client = StreamManagerClient::spawn(
            Arc::clone(&store),
            local_addr.to_string(),
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
        }
    }

    /// The address this ExtentNode is listening on.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Gracefully stop the ExtentNode: signal all tasks and await their completion.
    pub async fn stop(self) {
        info!("ExtentNode stopping...");
        // 1. Signal non-heartbeat tasks (downstream, watermark, accept loop).
        let _ = self.shutdown_tx.send(());
        // 2. Stop StreamManagerClient (sends Disconnect, awaits task).
        self.stream_manager_client.stop().await;
        // 3. Await remaining task handles.
        for handle in self.task_handles {
            let _ = handle.await;
        }
        info!("ExtentNode stopped");
    }
}
