use serde::Deserialize;
use std::time::Duration;

/// Timeout for establishing an RPC TCP connection.
pub const RPC_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Timeout for a single RPC request-response round trip.
pub const RPC_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Base server configuration (shared fields).
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
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
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ExtentNodeConfig {
    /// Unique node identifier for registration with StreamManager.
    /// If empty, defaults to the advertise address (`{advertise_ip}:{port}`).
    pub node_id: String,
    /// IP to bind the TCP listener on. Defaults to `0.0.0.0` (all interfaces).
    pub bind_ip: String,
    /// Port to listen on. Defaults to 9801.
    pub port: u16,
    /// IP advertised to StreamManager and other nodes for inbound connections.
    /// Required when `bind_ip` is `0.0.0.0`, since other nodes cannot connect to
    /// a wildcard address. If empty, defaults to `bind_ip`.
    pub advertise_ip: String,
    /// StreamManager address to connect to for registration and heartbeat.
    pub stream_manager_addr: String,
    /// Heartbeat interval in milliseconds. StreamManager uses 1.5x as dead-node timeout.
    pub heartbeat_interval_ms: u32,
    /// Arena capacity in bytes for each extent. Defaults to 64 MiB.
    pub extent_arena_capacity: usize,
}

impl Default for ExtentNodeConfig {
    fn default() -> Self {
        Self {
            node_id: String::new(),
            bind_ip: "0.0.0.0".to_string(),
            port: 9801,
            advertise_ip: String::new(),
            stream_manager_addr: "127.0.0.1:9800".to_string(),
            heartbeat_interval_ms: 5000,
            extent_arena_capacity: 64 * 1024 * 1024, // 64 MiB
        }
    }
}

impl ExtentNodeConfig {
    /// The address to bind the TCP listener on: `{bind_ip}:{port}`.
    pub fn listen_addr(&self) -> String {
        format!("{}:{}", self.bind_ip, self.port)
    }

    /// The address advertised to other nodes: `{advertise_ip}:{port}`.
    /// Falls back to `{bind_ip}:{port}` if `advertise_ip` is empty.
    pub fn advertise_addr(&self) -> String {
        let ip = if self.advertise_ip.is_empty() {
            &self.bind_ip
        } else {
            &self.advertise_ip
        };
        format!("{ip}:{}", self.port)
    }
}

/// Configuration for a StreamManager process.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct StreamManagerConfig {
    /// IP to bind the TCP listener on. Defaults to `0.0.0.0` (all interfaces).
    pub bind_ip: String,
    /// Port to listen on. Defaults to 9800.
    pub port: u16,
    /// MySQL host.
    pub mysql_host: String,
    /// MySQL port.
    pub mysql_port: u16,
    /// MySQL username.
    pub mysql_username: String,
    /// MySQL password.
    pub mysql_password: String,
    /// MySQL database name.
    pub mysql_database: String,
    /// Default replication factor: number of nodes per extent replica set.
    pub default_replication_factor: usize,
    /// How often the heartbeat checker polls for expired nodes, in milliseconds.
    pub heartbeat_check_interval_ms: u32,
}

impl Default for StreamManagerConfig {
    fn default() -> Self {
        Self {
            bind_ip: "0.0.0.0".to_string(),
            port: 9800,
            mysql_host: "localhost".to_string(),
            mysql_port: 3306,
            mysql_username: "root".to_string(),
            mysql_password: "password".to_string(),
            mysql_database: "stream_store".to_string(),
            default_replication_factor: 2,
            heartbeat_check_interval_ms: 3000,
        }
    }
}

impl StreamManagerConfig {
    /// The address to bind the TCP listener on: `{bind_ip}:{port}`.
    pub fn listen_addr(&self) -> String {
        format!("{}:{}", self.bind_ip, self.port)
    }

    /// Build the MySQL connection URL from individual fields.
    pub fn mysql_url(&self) -> String {
        format!(
            "mysql://{}:{}@{}:{}/{}",
            self.mysql_username,
            self.mysql_password,
            self.mysql_host,
            self.mysql_port,
            self.mysql_database
        )
    }
}

/// Load a config from a TOML file, falling back to defaults for missing fields.
pub fn load_config_from_file<T>(path: &str) -> Result<T, String>
where
    T: serde::de::DeserializeOwned + Default,
{
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("failed to read config file '{}': {}", path, e))?;
    toml::from_str(&content).map_err(|e| format!("failed to parse config file '{}': {}", path, e))
}
