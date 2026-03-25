use std::time::Duration;

/// Timeout for establishing an RPC TCP connection.
pub const RPC_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Timeout for a single RPC request-response round trip.
pub const RPC_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Base server configuration (shared fields).
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Address to listen on, e.g. "0.0.0.0:9801".
    pub listen_addr: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:9801".to_string(),
        }
    }
}

/// Configuration for an ExtentNode process.
#[derive(Debug, Clone)]
pub struct ExtentNodeConfig {
    /// Address to listen on for client/StreamManager connections.
    pub listen_addr: String,
    /// StreamManager address to connect to for registration and heartbeat.
    pub stream_manager_addr: String,
    /// Heartbeat interval in milliseconds. StreamManager uses 1.5x as dead-node timeout.
    pub heartbeat_interval_ms: u32,
    /// Arena capacity in bytes for each extent. Defaults to 64 MiB.
    /// Application users can tune this per workload: smaller values trigger
    /// more frequent seal-and-new (lower write amplification for small streams),
    /// larger values reduce seal frequency (better for high-throughput streams).
    pub extent_arena_capacity: usize,
}

impl Default for ExtentNodeConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:9801".to_string(),
            stream_manager_addr: "127.0.0.1:9800".to_string(),
            heartbeat_interval_ms: 5000,
            extent_arena_capacity: 64 * 1024 * 1024, // 64 MiB
        }
    }
}

/// Configuration for a StreamManager process.
#[derive(Debug, Clone)]
pub struct StreamManagerConfig {
    /// Address to listen on for ExtentNode/client connections.
    pub listen_addr: String,
    /// MySQL connection URL for metadata persistence.
    pub mysql_url: String,
    /// Default replication factor: number of nodes per extent replica set (default 2 = 1 Primary + 1 Secondary).
    /// Used as fallback when a client sends replication_factor=0 (meaning "use server default").
    /// Per-stream replication factor is stored in the stream table.
    pub default_replication_factor: usize,
    /// How often the heartbeat checker polls for expired nodes, in milliseconds.
    /// Should be aligned with `ExtentNodeConfig.heartbeat_interval_ms` so the checker
    /// runs frequently enough to detect expired nodes in a timely manner.
    pub heartbeat_check_interval_ms: u32,
}

impl Default for StreamManagerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:9800".to_string(),
            mysql_url: "mysql://root:password@tx.dev:3306/metadata".to_string(),
            default_replication_factor: 2,
            heartbeat_check_interval_ms: 3000,
        }
    }
}
