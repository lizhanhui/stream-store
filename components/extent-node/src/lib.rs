pub mod downstream;
pub mod extent;
pub mod offload;
pub mod store;
pub mod stream;
pub mod stream_manager_client;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use common::config::{ExtentNodeConfig, resolve_advertise_ip};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tracing::info;

use crate::downstream::DownstreamPool;
use crate::offload::{OffloadTask, OffloadWorker};
use crate::store::{ExtentNodeStore, ExtentUpdate};
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
    /// JoinHandles for spawned background tasks (accept loop).
    task_handles: Vec<JoinHandle<()>>,
    /// RAII client managing the StreamManager connection lifecycle.
    /// Sends Disconnect on drop; call `stop()` for guaranteed delivery.
    stream_manager_client: StreamManagerClient,
    /// Downstream connection pool — needs explicit shutdown to abort reader tasks.
    downstream: Arc<DownstreamPool>,
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
        store_inner.set_replication_timeout(Duration::from_millis(config.replication_timeout_ms));

        // Wire up the extent update channel for autonomous extent creation
        // and periodic progress reporting. The receiver is passed to
        // StreamManagerClient which sends UPDATE_EXTENT frames on the
        // existing heartbeat connection.
        let (update_tx, update_rx) = mpsc::channel::<ExtentUpdate>(64);
        store_inner.set_update_tx(update_tx);

        // Wire up the offload channel for non-critical background tasks
        // (e.g., CRC32 checksum verification after extent sealing).
        let (offload_tx, offload_rx) = mpsc::channel::<OffloadTask>(128);
        store_inner.set_offload_tx(offload_tx);

        let store = Arc::new(store_inner);

        // Spawn offload worker — processes fire-and-forget tasks off the hot path.
        let offload_store = Arc::clone(&store);
        let offload_shutdown = shutdown_tx.subscribe();
        tokio::spawn(async move {
            OffloadWorker::run(offload_store, offload_rx, offload_shutdown).await;
        });

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
        // The update_rx channel is passed here so UPDATE_EXTENT frames
        // are sent on the same connection as heartbeats.
        let stream_manager_client = StreamManagerClient::spawn(
            Arc::clone(&store),
            node_id,
            advertise_addr,
            config.stream_manager_addrs.clone(),
            config.heartbeat_interval_ms,
            Duration::from_millis(config.connect_timeout_ms),
            Duration::from_millis(config.request_timeout_ms),
            update_rx,
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
        // 1. Signal non-heartbeat tasks (accept loop).
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

    /// Simulate an abrupt crash: abort all tasks without sending Disconnect.
    ///
    /// This is useful for testing SM failover — the SM will detect the dead node
    /// via expired heartbeat instead of receiving a graceful Disconnect.
    pub fn kill(self) {
        info!("ExtentNode killed (simulated crash)");
        for handle in self.task_handles {
            handle.abort();
        }
        self.stream_manager_client.abort();
        // Drop everything else — TCP connections will be reset.
    }
}
