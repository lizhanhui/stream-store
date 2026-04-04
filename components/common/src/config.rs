use serde::Deserialize;
use std::net::Ipv4Addr;
use std::time::Duration;

/// Timeout for establishing an RPC TCP connection.
pub const RPC_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// Timeout for a single RPC request-response round trip.
pub const RPC_REQUEST_TIMEOUT: Duration = Duration::from_secs(3);

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
    /// If empty and `bind_ip` is `0.0.0.0`, auto-detects the primary non-loopback
    /// interface IP. Set explicitly in multi-homed environments.
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

    /// The address advertised to other nodes: `{resolved_ip}:{port}`.
    ///
    /// Resolution order:
    /// 1. `advertise_ip` if explicitly configured.
    /// 2. `bind_ip` if it's a usable address (not `0.0.0.0`).
    /// 3. Auto-detected local IP (primary non-loopback interface).
    /// 4. Falls back to `bind_ip` if auto-detection fails.
    pub fn advertise_addr(&self) -> String {
        let ip = resolve_advertise_ip(&self.bind_ip, &self.advertise_ip);
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
            mysql_host: "tx.dev".to_string(),
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

/// Detect the primary non-loopback IPv4 address of this machine.
///
/// Enumerates network interfaces via `getifaddrs` and collects all non-loopback
/// IPv4 addresses. If exactly one is found, returns it — this covers the common
/// single-NIC case. If multiple are found, returns `None` (the user should set
/// `advertise_ip` explicitly in multi-homed environments).
///
/// Returns `None` if no suitable address is found or if enumeration fails.
pub fn detect_local_ip() -> Option<String> {
    use nix::ifaddrs::getifaddrs;

    let addrs = getifaddrs().ok()?;
    let candidates: Vec<Ipv4Addr> = addrs
        .filter_map(|ifa| {
            let addr = ifa.address?;
            let inet = addr.as_sockaddr_in()?;
            let ip = Ipv4Addr::from(inet.ip());
            if !ip.is_loopback() && !ip.is_unspecified() {
                Some(ip)
            } else {
                None
            }
        })
        .collect();

    if candidates.len() == 1 {
        Some(candidates[0].to_string())
    } else {
        None
    }
}

/// Resolve the effective advertise IP given a `bind_ip` and an optional `advertise_ip`.
///
/// - If `advertise_ip` is non-empty, returns it as-is.
/// - If `bind_ip` is a usable address (not `0.0.0.0`), returns `bind_ip`.
/// - Otherwise, auto-detects the local IP via [`detect_local_ip`].
/// - If auto-detection fails, falls back to `bind_ip` unchanged.
pub fn resolve_advertise_ip(bind_ip: &str, advertise_ip: &str) -> String {
    if !advertise_ip.is_empty() {
        return advertise_ip.to_string();
    }
    if bind_ip != "0.0.0.0" {
        return bind_ip.to_string();
    }
    detect_local_ip().unwrap_or_else(|| bind_ip.to_string())
}
