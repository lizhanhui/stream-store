# Configuration File Support for stream-manager and extent-node

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add TOML-based configuration file support so stream-manager's database credentials and extent-node's stream-manager address are configurable at startup.

**Architecture:** Add `serde` + `toml` dependencies to the `common` crate. Derive `Deserialize` on config structs. Each binary accepts an optional `--config <path>` CLI argument (parsed manually, no clap). If provided, load and merge the TOML file over defaults; if omitted, use defaults as today.

**Tech Stack:** `serde` (derive), `toml` (deserialization), Rust std for file I/O and CLI arg parsing.

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `components/common/Cargo.toml` | Modify | Add `serde`, `toml` dependencies |
| `components/common/src/config.rs` | Modify | Add `Deserialize` derives, add `load_from_file()` helper |
| `src/bin/stream-manager.rs` | Modify | Parse `--config` arg, load config file |
| `src/bin/extent-node.rs` | Modify | Parse `--config` arg, load config file |
| `conf/stream-manager.toml` | Create | Example config for stream-manager |
| `conf/extent-node.toml` | Create | Example config for extent-node |

---

### Task 1: Add serde + toml dependencies to common crate

**Files:**
- Modify: `components/common/Cargo.toml`
- Modify: `Cargo.toml` (workspace dependencies)

- [ ] **Step 1: Add workspace dependencies for serde and toml**

In `Cargo.toml` (workspace root), add to `[workspace.dependencies]`:

```toml
serde = { version = "1", features = ["derive"] }
toml = "0.8"
```

- [ ] **Step 2: Add serde and toml to common crate**

In `components/common/Cargo.toml`, add to `[dependencies]`:

```toml
serde = { workspace = true }
toml = { workspace = true }
```

- [ ] **Step 3: Verify it compiles**

Run: `cargo check -p common`
Expected: compiles with no errors

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml components/common/Cargo.toml
git commit -m "deps: add serde and toml to common crate for config file support"
```

---

### Task 2: Add Deserialize to config structs and add config loading helper

**Files:**
- Modify: `components/common/src/config.rs`

The key design: use `serde(default)` on each field so a partial TOML file works — any missing field falls back to the `Default` impl value. For `StreamManagerConfig`, instead of a single `mysql_url` string, we break it into `mysql_host`, `mysql_port`, `mysql_username`, `mysql_password`, `mysql_database` fields so each can be configured independently, and generate the URL from parts. The original `mysql_url` field is kept (renamed internally) as the computed connection string.

- [ ] **Step 1: Add serde derives and the config loader function**

Replace the full content of `components/common/src/config.rs` with:

```rust
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
    /// Address to listen on for client/StreamManager connections.
    pub listen_addr: String,
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
            listen_addr: "0.0.0.0:9801".to_string(),
            stream_manager_addr: "127.0.0.1:9800".to_string(),
            heartbeat_interval_ms: 5000,
            extent_arena_capacity: 64 * 1024 * 1024, // 64 MiB
        }
    }
}

/// Configuration for a StreamManager process.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct StreamManagerConfig {
    /// Address to listen on for ExtentNode/client connections.
    pub listen_addr: String,
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
            listen_addr: "0.0.0.0:9800".to_string(),
            mysql_host: "127.0.0.1".to_string(),
            mysql_port: 3306,
            mysql_username: "root".to_string(),
            mysql_password: "password".to_string(),
            mysql_database: "metadata".to_string(),
            default_replication_factor: 2,
            heartbeat_check_interval_ms: 3000,
        }
    }
}

impl StreamManagerConfig {
    /// Build the MySQL connection URL from individual fields.
    pub fn mysql_url(&self) -> String {
        format!(
            "mysql://{}:{}@{}:{}/{}",
            self.mysql_username, self.mysql_password, self.mysql_host, self.mysql_port, self.mysql_database
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
    toml::from_str(&content)
        .map_err(|e| format!("failed to parse config file '{}': {}", path, e))
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo check -p common`
Expected: compiles with no errors

- [ ] **Step 3: Commit**

```bash
git add components/common/src/config.rs
git commit -m "feat(config): add serde Deserialize to config structs and add TOML loader"
```

---

### Task 3: Update StreamManager to use mysql_url() method

**Files:**
- Grep for `mysql_url` usage across the codebase and update all call sites from `config.mysql_url` (field) to `config.mysql_url()` (method).

- [ ] **Step 1: Find all usages of config.mysql_url**

Run: `grep -rn "mysql_url" components/stream-manager/src/`

Update every `config.mysql_url` or `self.config.mysql_url` field access to `config.mysql_url()` or `self.config.mysql_url()`. The exact files will depend on grep output — likely in `components/stream-manager/src/lib.rs` or sub-modules.

- [ ] **Step 2: Verify it compiles**

Run: `cargo check -p stream-manager`
Expected: compiles with no errors

- [ ] **Step 3: Commit**

```bash
git add components/stream-manager/
git commit -m "refactor: use mysql_url() method instead of mysql_url field"
```

---

### Task 4: Update stream-manager binary to accept --config flag

**Files:**
- Modify: `src/bin/stream-manager.rs`

- [ ] **Step 1: Update the binary to parse --config**

Replace the full content of `src/bin/stream-manager.rs` with:

```rust
use common::config::{load_config_from_file, StreamManagerConfig};
use stream_manager::StreamManager;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config = match parse_config_path() {
        Some(path) => load_config_from_file::<StreamManagerConfig>(&path)
            .unwrap_or_else(|e| {
                eprintln!("{e}");
                std::process::exit(1);
            }),
        None => StreamManagerConfig::default(),
    };

    tracing::info!("StreamManager starting with config: {:?}", config);

    let mgr = StreamManager::start(config).await;

    // Wait for Ctrl+C, then gracefully shut down.
    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen for ctrl_c");

    mgr.stop().await;
}

fn parse_config_path() -> Option<String> {
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        if args[i] == "--config" || args[i] == "-c" {
            if i + 1 < args.len() {
                return Some(args[i + 1].clone());
            } else {
                eprintln!("--config requires a file path argument");
                std::process::exit(1);
            }
        }
        i += 1;
    }
    None
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo check --bin stream-manager`
Expected: compiles with no errors

- [ ] **Step 3: Commit**

```bash
git add src/bin/stream-manager.rs
git commit -m "feat(stream-manager): accept --config flag for TOML config file"
```

---

### Task 5: Update extent-node binary to accept --config flag

**Files:**
- Modify: `src/bin/extent-node.rs`

- [ ] **Step 1: Update the binary to parse --config**

Replace the full content of `src/bin/extent-node.rs` with:

```rust
use common::config::{load_config_from_file, ExtentNodeConfig};
use extent_node::ExtentNode;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config = match parse_config_path() {
        Some(path) => load_config_from_file::<ExtentNodeConfig>(&path)
            .unwrap_or_else(|e| {
                eprintln!("{e}");
                std::process::exit(1);
            }),
        None => ExtentNodeConfig::default(),
    };

    tracing::info!("ExtentNode starting with config: {:?}", config);

    let node = ExtentNode::start(config).await;

    // Wait for Ctrl+C, then gracefully shut down.
    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen for ctrl_c");

    node.stop().await;
}

fn parse_config_path() -> Option<String> {
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        if args[i] == "--config" || args[i] == "-c" {
            if i + 1 < args.len() {
                return Some(args[i + 1].clone());
            } else {
                eprintln!("--config requires a file path argument");
                std::process::exit(1);
            }
        }
        i += 1;
    }
    None
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo check --bin extent-node`
Expected: compiles with no errors

- [ ] **Step 3: Commit**

```bash
git add src/bin/extent-node.rs
git commit -m "feat(extent-node): accept --config flag for TOML config file"
```

---

### Task 6: Create example configuration files

**Files:**
- Create: `conf/stream-manager.toml`
- Create: `conf/extent-node.toml`

- [ ] **Step 1: Create conf/stream-manager.toml**

```toml
# StreamManager configuration
# All fields are optional — missing fields use defaults.

listen_addr = "0.0.0.0:9800"

# MySQL connection settings
mysql_host = "127.0.0.1"
mysql_port = 3306
mysql_username = "root"
mysql_password = "password"
mysql_database = "metadata"

# Replication
default_replication_factor = 2

# Heartbeat checker interval (ms)
heartbeat_check_interval_ms = 3000
```

- [ ] **Step 2: Create conf/extent-node.toml**

```toml
# ExtentNode configuration
# All fields are optional — missing fields use defaults.

listen_addr = "0.0.0.0:9801"

# StreamManager address for registration and heartbeat
stream_manager_addr = "127.0.0.1:9800"

# Heartbeat interval (ms)
heartbeat_interval_ms = 5000

# Arena capacity per extent (bytes). Default: 64 MiB
extent_arena_capacity = 67108864
```

- [ ] **Step 3: Commit**

```bash
git add conf/
git commit -m "docs: add example TOML config files for stream-manager and extent-node"
```

---

### Task 7: End-to-end verification

- [ ] **Step 1: Full workspace build**

Run: `cargo build`
Expected: all binaries build successfully

- [ ] **Step 2: Test stream-manager with config file**

Run: `cargo run --bin stream-manager -- --config conf/stream-manager.toml`
Expected: starts up, log line shows config loaded with values from TOML file. Ctrl+C to stop.

- [ ] **Step 3: Test extent-node with config file**

Run: `cargo run --bin extent-node -- --config conf/extent-node.toml`
Expected: starts up, log line shows config loaded with `stream_manager_addr = "127.0.0.1:9800"`. Ctrl+C to stop.

- [ ] **Step 4: Test defaults still work (no --config)**

Run: `cargo run --bin stream-manager`
Expected: starts with defaults, same behavior as before.

---

## Verification

1. `cargo build` — full workspace compiles
2. `cargo run --bin stream-manager -- --config conf/stream-manager.toml` — starts with TOML values
3. `cargo run --bin extent-node -- --config conf/extent-node.toml` — starts with TOML values
4. Both binaries start without `--config` flag (defaults preserved)
5. Partial TOML files work (e.g., only `mysql_host` specified, rest default)
