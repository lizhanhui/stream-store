pub mod ack_queue;
pub mod downstream;
pub mod extent;
pub mod s3;
pub mod s3_codec;
pub mod s3_flusher;
pub mod store;
pub mod stream;
pub mod stream_manager_client;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::config::{ExtentNodeConfig, resolve_advertise_ip};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tracing::info;

use crate::downstream::DownstreamPool;
use crate::s3::S3Client;
use crate::s3_flusher::FlushRequest;
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
        // StreamManagerClient which sends UPDATE_EXTENT frames on a
        // dedicated connection, separate from heartbeats.
        let (update_tx, update_rx) = mpsc::channel::<ExtentUpdate>(64);
        store_inner.set_update_tx(update_tx);

        // Wire up the S3 flush channel. The flusher task is spawned below
        // after the S3 client is initialized.
        let (flush_tx, flush_rx) = mpsc::channel::<FlushRequest>(64);
        store_inner.set_flush_tx(flush_tx);

        let store = Arc::new(store_inner);

        // Create DownstreamPool with back-reference to store (for inline watermark processing).
        let downstream = Arc::new(DownstreamPool::new(Arc::clone(&store)));
        // Wire pool into store.
        store.set_downstream(Arc::clone(&downstream));

        // Initialize S3 client eagerly (async: reads ~/.aws/config and ~/.aws/credentials).
        // Returns None if s3_bucket is empty (S3 flush disabled).
        if let Some(s3_client) = S3Client::new(&config).await {
            let s3_client = Arc::new(s3_client);
            store.set_s3_client(Arc::clone(&s3_client));

            // Spawn the background S3 flusher task.
            let flusher_store = Arc::clone(&store);
            let mut flusher_shutdown = shutdown_tx.subscribe();
            task_handles.push(tokio::spawn(async move {
                tokio::select! {
                    _ = crate::s3_flusher::run(s3_client, flusher_store, flush_rx) => {}
                    _ = flusher_shutdown.recv() => {
                        info!("S3 flusher received shutdown signal");
                    }
                }
            }));
        }

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
        // Manages two connections: heartbeat (dedicated) and extent updates (dedicated).
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

        // Spawn accept loop (plain tokio::spawn — worker pinning is handled
        // at the runtime level via on_thread_start core affinity).
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
        self.downstream.shutdown();
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

/// Build a tokio multi-thread runtime with worker threads pinned to the
/// configured CPU cores via `core_affinity`.
///
/// If `config.worker_cores` is empty, uses all available cores (no pinning).
/// Otherwise, creates exactly `len(worker_cores)` worker threads, each pinned
/// to the corresponding core ID.
///
/// The main thread (running the accept loop, SM client, etc.) is NOT pinned —
/// only tokio worker threads are pinned.
pub fn build_runtime(config: &ExtentNodeConfig) -> tokio::runtime::Runtime {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();

    if config.worker_cores.is_empty() {
        // No pinning — use default tokio behavior.
        tracing::info!("tokio runtime: default worker threads (no core pinning)");
    } else {
        let cores = config.worker_cores.clone();
        let num_workers = cores.len();
        let next_idx = Arc::new(AtomicUsize::new(0));

        builder.worker_threads(num_workers);
        builder.on_thread_start(move || {
            let idx = next_idx.fetch_add(1, Ordering::SeqCst);
            if idx < cores.len() {
                let core_id = core_affinity::CoreId { id: cores[idx] };
                if core_affinity::set_for_current(core_id) {
                    tracing::info!("tokio worker thread pinned to core {}", cores[idx]);
                } else {
                    tracing::warn!("failed to pin tokio worker thread to core {}", cores[idx]);
                }
            }
        });

        tracing::info!(
            "tokio runtime: {} worker threads pinned to cores {:?}",
            num_workers,
            config.worker_cores
        );
    }

    builder.build().expect("failed to build tokio runtime")
}
