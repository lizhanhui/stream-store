pub mod allocator;
pub mod heartbeat_checker;
pub mod metadata;
pub mod store;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use crate::metadata::MetadataStore;
use common::config::StreamManagerConfig;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tracing::info;

use crate::heartbeat_checker::run_heartbeat_checker;
use crate::store::StreamManagerStore;

/// A running StreamManager with lifecycle management.
///
/// Created via [`StreamManager::start`], which connects to MySQL, binds the listener,
/// spawns all background tasks, and returns a handle. Call [`StreamManager::stop`]
/// for graceful shutdown.
pub struct StreamManager {
    /// The address this StreamManager is listening on.
    addr: SocketAddr,
    /// Shutdown signal sender — sending triggers graceful stop of all tasks.
    shutdown_tx: broadcast::Sender<()>,
    /// JoinHandles for all spawned background tasks.
    task_handles: Vec<JoinHandle<()>>,
}

impl StreamManager {
    /// Start the StreamManager.
    ///
    /// 1. Connect to MySQL and run migrations.
    /// 2. Bind the TCP listener and determine the actual bound address.
    /// 3. Start the heartbeat checker background task (with leadership lease).
    /// 4. Spawn the accept loop.
    ///
    /// Returns a `StreamManager` handle for lifecycle management.
    pub async fn start(config: StreamManagerConfig) -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);
        let mut task_handles = Vec::new();

        // 1. Connect to MySQL metadata store.
        let store = MetadataStore::connect(&config.mysql_url())
            .await
            .expect("failed to connect to MySQL");
        store.migrate().await.expect("failed to run migrations");
        info!("StreamManager connected to MySQL: {}", config.mysql_url());

        // 2. Bind TCP listener and get the actual bound address.
        let listener = TcpListener::bind(&config.listen_addr())
            .await
            .expect("failed to bind StreamManager listener");
        let local_addr = listener
            .local_addr()
            .expect("failed to get StreamManager local address");
        info!("StreamManager server bound on {local_addr}");

        // 3. Create StreamManagerStore (stateless — all state lives in MySQL).
        let stream_manager_store = Arc::new(StreamManagerStore::new(
            store,
            config.default_replication_factor,
        ));

        // 4. Reconcile metadata with EN state (catch up on missed UPDATE_EXTENT
        //    notifications from any prior SM downtime).
        stream_manager_store.reconcile_on_startup().await;

        // 5. Start heartbeat checker with leadership lease (only the leader
        //    runs failover; all SMs compete for the lease).
        let heartbeat_sm_store = Arc::clone(&stream_manager_store);
        let effective_port = if config.port == 0 {
            local_addr.port()
        } else {
            config.port
        };
        let node_id = config.advertise_addr(effective_port);
        info!("StreamManager node-id={node_id}");
        let heartbeat_check_interval =
            Duration::from_millis(config.heartbeat_check_interval_ms as u64);
        let lease_duration_secs = config.leadership_lease_duration_secs;
        let flush_staleness_threshold_ms = config.flush_staleness_threshold_ms;
        let heartbeat_shutdown = shutdown_tx.subscribe();
        task_handles.push(tokio::spawn(async move {
            run_heartbeat_checker(
                heartbeat_sm_store,
                node_id,
                heartbeat_check_interval,
                lease_duration_secs,
                flush_staleness_threshold_ms,
                heartbeat_shutdown,
            )
            .await;
        }));

        // 6. Spawn accept loop.
        let server_shutdown = shutdown_tx.subscribe();
        task_handles.push(tokio::spawn(async move {
            server::Server::builder("StreamManager")
                .listener(listener)
                .handler(stream_manager_store)
                .shutdown(server_shutdown)
                .build()
                .run()
                .await;
        }));

        StreamManager {
            addr: local_addr,
            shutdown_tx,
            task_handles,
        }
    }

    /// The address this StreamManager is listening on.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Gracefully stop the StreamManager: signal all tasks and await their completion.
    pub async fn stop(self) {
        info!("StreamManager stopping...");
        // Send shutdown signal to all tasks. Ignore error if no receivers remain.
        let _ = self.shutdown_tx.send(());
        // Await all task handles.
        for handle in self.task_handles {
            let _ = handle.await;
        }
        info!("StreamManager stopped");
    }
}
