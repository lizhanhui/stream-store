use serde::Deserialize;
use std::net::Ipv4Addr;

/// Default timeout for establishing an RPC TCP connection (LAN, RTT < 10ms).
pub const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 500;

/// Default timeout for a single RPC request-response round trip.
/// Covers EN-to-EN operations (sub-ms) with generous headroom.
pub const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 500;

/// Default timeout for SM-facing RPC requests that may involve MySQL transactions.
pub const DEFAULT_SM_REQUEST_TIMEOUT_MS: u64 = 2000;

/// Default timeout for replication quorum ACK (Primary waiting for Secondary watermarks).
pub const DEFAULT_REPLICATION_TIMEOUT_MS: u64 = 500;

/// Default extent capacity: 64 MiB per extent arena.
pub const DEFAULT_EXTENT_CAPACITY: u32 = 64 * 1024 * 1024;

/// Default cache_extents: max extents to retain in memory per stream.
pub const DEFAULT_CACHE_EXTENTS: u32 = 4;

/// Maximum supported replication factor (RF is normally 1-3).
pub const MAX_REPLICATION_FACTOR: usize = 5;

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
    /// StreamManager address(es) for registration and heartbeat.
    /// The EN tries each address in round-robin order on connection failure.
    pub stream_manager_addrs: Vec<String>,
    /// Heartbeat interval in milliseconds. StreamManager uses 1.5x as dead-node timeout.
    pub heartbeat_interval_ms: u32,
    /// Timeout (ms) for replication quorum ACK. PendingAcks older than this are
    /// expired with an error to the client. Defaults to 500ms.
    pub replication_timeout_ms: u64,
    /// Timeout (ms) for establishing an RPC TCP connection. Defaults to 500ms.
    pub connect_timeout_ms: u64,
    /// Timeout (ms) for SM-facing RPC request-response round trips. Defaults to 2000ms.
    pub request_timeout_ms: u64,
}

impl Default for ExtentNodeConfig {
    fn default() -> Self {
        Self {
            node_id: String::new(),
            bind_ip: "0.0.0.0".to_string(),
            port: 9801,
            advertise_ip: String::new(),
            stream_manager_addrs: vec!["127.0.0.1:9800".to_string()],
            heartbeat_interval_ms: 5000,
            replication_timeout_ms: DEFAULT_REPLICATION_TIMEOUT_MS,
            connect_timeout_ms: DEFAULT_CONNECT_TIMEOUT_MS,
            request_timeout_ms: DEFAULT_SM_REQUEST_TIMEOUT_MS,
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
    /// IP advertised to other SM instances for leadership identification.
    /// If empty and `bind_ip` is `0.0.0.0`, auto-detects the primary non-loopback
    /// interface IP. Set explicitly in multi-homed environments.
    pub advertise_ip: String,
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
    /// Timeout (ms) for establishing an RPC TCP connection to ExtentNodes. Defaults to 500ms.
    pub connect_timeout_ms: u64,
    /// Timeout (ms) for RPC request-response round trips to ExtentNodes. Defaults to 2000ms.
    pub request_timeout_ms: u64,
    /// Leadership lease duration in seconds. Only the lease holder runs the heartbeat
    /// checker and failover. Defaults to 10s (renewed every heartbeat_check_interval).
    pub leadership_lease_duration_secs: u32,
}

impl Default for StreamManagerConfig {
    fn default() -> Self {
        Self {
            bind_ip: "0.0.0.0".to_string(),
            port: 9800,
            advertise_ip: String::new(),
            mysql_host: "localhost".to_string(),
            mysql_port: 3306,
            mysql_username: "root".to_string(),
            mysql_password: "password".to_string(),
            mysql_database: "stream_store".to_string(),
            default_replication_factor: 2,
            heartbeat_check_interval_ms: 3000,
            connect_timeout_ms: DEFAULT_CONNECT_TIMEOUT_MS,
            request_timeout_ms: DEFAULT_SM_REQUEST_TIMEOUT_MS,
            leadership_lease_duration_secs: 10,
        }
    }
}

impl StreamManagerConfig {
    /// The address to bind the TCP listener on: `{bind_ip}:{port}`.
    pub fn listen_addr(&self) -> String {
        format!("{}:{}", self.bind_ip, self.port)
    }

    /// The address used as this SM's identity (leadership lease, logging).
    ///
    /// Resolution order:
    /// 1. `advertise_ip` if explicitly configured.
    /// 2. `bind_ip` if it's a usable address (not `0.0.0.0`).
    /// 3. Auto-detected local IP (primary non-loopback interface).
    /// 4. Falls back to `bind_ip` if auto-detection fails.
    pub fn advertise_addr(&self, effective_port: u16) -> String {
        let ip = resolve_advertise_ip(&self.bind_ip, &self.advertise_ip);
        format!("{ip}:{effective_port}")
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
/// Enumerates network interfaces via `getifaddrs` and filters out:
/// - Loopback and unspecified addresses.
/// - Virtual/bridge interfaces: `docker*`, `br-*`, `veth*`, `virbr*`, `lxc*`,
///   `flannel*`, `cni*`, `podman*`.
///
/// If multiple candidates remain after filtering, prefers addresses outside the
/// `172.16.0.0/12` range (commonly used by Docker/container bridges). If still
/// ambiguous, returns the first candidate in sorted order for determinism.
///
/// Returns `None` if no suitable address is found or if enumeration fails.
pub fn detect_local_ip() -> Option<String> {
    use nix::ifaddrs::getifaddrs;

    let addrs = getifaddrs().ok()?;

    // Virtual/bridge interface prefixes to skip.
    const VIRTUAL_PREFIXES: &[&str] = &[
        "docker", "br-", "veth", "virbr", "lxc", "flannel", "cni", "podman",
    ];

    let candidates: Vec<(String, Ipv4Addr)> = addrs
        .filter_map(|ifa| {
            // Skip virtual/bridge interfaces.
            let name = &ifa.interface_name;
            if VIRTUAL_PREFIXES.iter().any(|p| name.starts_with(p)) {
                return None;
            }

            let addr = ifa.address?;
            let inet = addr.as_sockaddr_in()?;
            let ip = inet.ip();
            if ip.is_loopback() || ip.is_unspecified() || ip.is_link_local() {
                return None;
            }
            Some((name.clone(), ip))
        })
        .collect();

    if candidates.is_empty() {
        return None;
    }
    if candidates.len() == 1 {
        return Some(candidates[0].1.to_string());
    }

    // Multiple candidates: deprioritize 172.16.0.0/12 (Docker default range).
    let is_docker_range = |ip: &Ipv4Addr| {
        let octets = ip.octets();
        octets[0] == 172 && (16..=31).contains(&octets[1])
    };

    let non_docker: Vec<(String, Ipv4Addr)> = candidates
        .iter()
        .filter(|(_, ip)| !is_docker_range(ip))
        .cloned()
        .collect();

    // Pick from non-docker if possible, otherwise from all candidates.
    let mut pool = if !non_docker.is_empty() {
        non_docker
    } else {
        candidates
    };

    pool.sort_by(|a, b| a.0.cmp(&b.0));
    Some(pool[0].1.to_string())
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
