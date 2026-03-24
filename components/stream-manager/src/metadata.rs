use common::errors::StorageError;
use common::types::{ExtentId, ExtentInfo, ExtentState, NodeState, ReplicaDetail, StreamId};
use sqlx::mysql::MySqlPoolOptions;
use sqlx::{Acquire, MySqlPool, Row};
use tracing::info;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("./migrations");
}

/// A row from the `stream` table.
#[derive(Debug, Clone)]
pub struct StreamRow {
    pub stream_id: StreamId,
    pub stream_name: String,
    pub stream_type: String,
    pub replication_factor: u16,
}

/// A row from the `extent` table.
#[derive(Debug, Clone)]
pub struct ExtentRow {
    pub extent_id: ExtentId,
    pub stream_id: StreamId,
    pub base_offset: u64,
    pub message_count: u32,
    pub state: ExtentState,
}

/// A row from the `extent_replica` table.
#[derive(Debug, Clone)]
pub struct ExtentReplicaRow {
    pub stream_id: StreamId,
    pub extent_id: ExtentId,
    pub node_addr: String,
    pub role: u8,
}

/// Result of a transactional seal-and-allocate operation.
#[derive(Debug, Clone)]
pub enum SealResult {
    /// The extent was active and has been sealed; a new extent was allocated.
    Sealed { new_extent_id: ExtentId },
    /// The extent was already sealed by another client. Returns the successor extent
    /// that was already allocated (the next Active extent after the sealed one).
    AlreadySealed {
        new_extent_id: ExtentId,
        new_base_offset: u64,
        primary_addr: String,
    },
}

/// A row from the `node` table.
#[derive(Debug, Clone)]
pub struct NodeRow {
    pub node_id: String,
    pub addr: String,
    pub heartbeat_interval_ms: i32,
    pub state: NodeState,
}

/// MySQL-backed metadata store for StreamManager.
#[derive(Clone)]
pub struct MetadataStore {
    pool: MySqlPool,
    /// MySQL URL retained for Refinery migrations (which uses mysql_async, not sqlx).
    url: String,
}

impl MetadataStore {
    /// Connect to MySQL and return a MetadataStore.
    pub async fn connect(url: &str) -> Result<Self, StorageError> {
        let pool = MySqlPoolOptions::new()
            .max_connections(10)
            .connect(url)
            .await
            .map_err(|e| StorageError::Internal(format!("MySQL connect: {e}")))?;
        Ok(Self {
            pool,
            url: url.to_string(),
        })
    }

    /// Run Refinery migrations against the database.
    ///
    /// Refinery uses `mysql_async` under the hood (separate from the sqlx pool).
    /// It creates a `refinery_schema_history` table to track applied migrations
    /// and only runs new ones.
    pub async fn migrate(&self) -> Result<(), StorageError> {
        let opts = mysql_async::Opts::from_url(&self.url)
            .map_err(|e| StorageError::Internal(format!("parse mysql url: {e}")))?;
        let mut pool = mysql_async::Pool::new(opts);
        embedded::migrations::runner()
            .run_async(&mut pool)
            .await
            .map_err(|e| StorageError::Internal(format!("migration: {e}")))?;
        pool.disconnect()
            .await
            .map_err(|e| StorageError::Internal(format!("disconnect migration pool: {e}")))?;
        info!("database migrations applied");
        Ok(())
    }

    // ── Stream operations ──

    /// Create a new stream with a per-stream replication factor. Returns the assigned StreamId.
    /// Also initializes the stream_sequence row for per-stream extent_id generation.
    pub async fn create_stream(
        &self,
        name: &str,
        stream_type: &str,
        replication_factor: u16,
    ) -> Result<StreamId, StorageError> {
        let result = sqlx::query(
            "INSERT INTO stream (stream_name, stream_type, replication_factor) VALUES (?, ?, ?)",
        )
        .bind(name)
        .bind(stream_type)
        .bind(replication_factor as i16)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(format!("create_stream: {e}")))?;

        let stream_id = StreamId(result.last_insert_id());

        // Initialize stream_sequence for per-stream extent_id generation.
        sqlx::query("INSERT INTO stream_sequence (stream_id, next_extent_id) VALUES (?, 0)")
            .bind(stream_id.0 as i64)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(format!("init stream_sequence: {e}")))?;

        Ok(stream_id)
    }

    /// Get a stream by ID.
    pub async fn get_stream(&self, id: StreamId) -> Result<Option<StreamRow>, StorageError> {
        let row = sqlx::query(
            "SELECT stream_id, stream_name, stream_type, replication_factor FROM stream WHERE stream_id = ?",
        )
        .bind(id.0 as i64)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(format!("get_stream: {e}")))?;

        Ok(row.map(|r| StreamRow {
            stream_id: StreamId(r.get::<i64, _>("stream_id") as u64),
            stream_name: r.get("stream_name"),
            stream_type: r.get("stream_type"),
            replication_factor: r.get::<i16, _>("replication_factor") as u16,
        }))
    }

    /// Get the replication factor for a stream.
    pub async fn get_stream_replication_factor(
        &self,
        stream_id: StreamId,
    ) -> Result<u16, StorageError> {
        let row = sqlx::query("SELECT replication_factor FROM stream WHERE stream_id = ?")
            .bind(stream_id.0 as i64)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(format!("get_stream_replication_factor: {e}")))?;

        Ok(row.get::<i16, _>("replication_factor") as u16)
    }

    // ── Extent operations ──

    /// Allocate a new extent for a stream on a replica set. Returns ExtentId.
    ///
    /// `nodes` is ordered: [(primary_addr, 0), (secondary_1_addr, 1), ...].
    /// This method runs in a single MySQL transaction to prevent race conditions:
    /// 1. Locks the stream_sequence row with SELECT ... FOR UPDATE.
    /// 2. Increments next_extent_id and reads the new value atomically.
    /// 3. Inserts the extent row.
    /// 4. Inserts extent_replica rows for each node.
    pub async fn allocate_extent(
        &self,
        stream_id: StreamId,
        base_offset: u64,
        nodes: &[(String, u8)],
    ) -> Result<ExtentId, StorageError> {
        if nodes.is_empty() {
            return Err(StorageError::Internal(
                "allocate_extent: empty node list".into(),
            ));
        }

        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(|e| StorageError::Internal(format!("acquire connection: {e}")))?;
        let mut tx = conn
            .begin()
            .await
            .map_err(|e| StorageError::Internal(format!("begin transaction: {e}")))?;

        // Step 1: Lock and increment stream_sequence atomically within the transaction.
        sqlx::query(
            "SELECT next_extent_id FROM stream_sequence WHERE stream_id = ? FOR UPDATE",
        )
        .bind(stream_id.0 as i64)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(format!("lock stream_sequence: {e}")))?;

        sqlx::query(
            "UPDATE stream_sequence SET next_extent_id = next_extent_id + 1 WHERE stream_id = ?",
        )
        .bind(stream_id.0 as i64)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(format!("increment stream_sequence: {e}")))?;

        let row = sqlx::query("SELECT next_extent_id FROM stream_sequence WHERE stream_id = ?")
            .bind(stream_id.0 as i64)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(format!("read stream_sequence: {e}")))?;

        // next_extent_id was already incremented, so the allocated ID is next_extent_id (post-increment value).
        let extent_id = ExtentId(row.get::<i64, _>("next_extent_id") as u32);

        // Step 2: Insert extent row.
        sqlx::query(
            "INSERT INTO extent (stream_id, extent_id, base_offset, state) VALUES (?, ?, ?, ?)",
        )
        .bind(stream_id.0 as i64)
        .bind(extent_id.0 as i64)
        .bind(base_offset as i64)
        .bind(ExtentState::Active.as_u8())
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(format!("insert extent: {e}")))?;

        // Step 3: Insert extent_replica rows.
        for (addr, role) in nodes {
            sqlx::query(
                "INSERT INTO extent_replica (stream_id, extent_id, node_addr, role) VALUES (?, ?, ?, ?)",
            )
            .bind(stream_id.0 as i64)
            .bind(extent_id.0 as i64)
            .bind(addr)
            .bind(*role)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(format!("insert extent_replica: {e}")))?;
        }

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(format!("commit: {e}")))?;

        Ok(extent_id)
    }

    /// Seal an extent: update state to SEALED and record message_count.
    pub async fn seal_extent(
        &self,
        stream_id: StreamId,
        extent_id: ExtentId,
        message_count: u32,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "UPDATE extent SET state = ?, message_count = ?, sealed_at = NOW() \
             WHERE stream_id = ? AND extent_id = ?",
        )
        .bind(ExtentState::Sealed.as_u8())
        .bind(message_count as i64)
        .bind(stream_id.0 as i64)
        .bind(extent_id.0 as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(format!("seal_extent: {e}")))?;
        Ok(())
    }

    /// Seal an extent and allocate a new one in a single MySQL transaction.
    ///
    /// Uses `SELECT ... FOR UPDATE` to ensure concurrent safety:
    /// - If the extent is Active: seal it, allocate a new extent, return `Sealed`.
    /// - If the extent is already Sealed: find the successor extent and return `AlreadySealed`.
    pub async fn seal_and_allocate_transaction(
        &self,
        stream_id: StreamId,
        extent_id: ExtentId,
        message_count: u32,
        nodes: &[(String, u8)],
    ) -> Result<SealResult, StorageError> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(|e| StorageError::Internal(format!("acquire connection: {e}")))?;
        let mut tx = conn
            .begin()
            .await
            .map_err(|e| StorageError::Internal(format!("begin transaction: {e}")))?;

        // Step 1: Lock the target extent row and check state.
        let row = sqlx::query(
            "SELECT state, message_count, base_offset \
             FROM extent WHERE stream_id = ? AND extent_id = ? FOR UPDATE",
        )
        .bind(stream_id.0 as i64)
        .bind(extent_id.0 as i64)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(format!("lock extent: {e}")))?;

        let row = row.ok_or_else(|| {
            StorageError::Internal(format!(
                "extent not found: stream={:?}, extent={:?}",
                stream_id, extent_id
            ))
        })?;

        let state_val = row.get::<i8, _>("state") as u8;
        let state = ExtentState::from_u8(state_val).unwrap_or(ExtentState::Unspecified);

        if state == ExtentState::Sealed {
            // Already sealed — find the successor (next extent with higher extent_id).
            let successor = sqlx::query(
                "SELECT extent_id, base_offset FROM extent \
                 WHERE stream_id = ? AND extent_id > ? \
                 ORDER BY extent_id ASC LIMIT 1",
            )
            .bind(stream_id.0 as i64)
            .bind(extent_id.0 as i64)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(format!("find successor: {e}")))?;

            let successor = successor.ok_or_else(|| {
                StorageError::Internal(format!(
                    "extent {:?} is sealed but no successor found for stream {:?}",
                    extent_id, stream_id
                ))
            })?;

            let new_extent_id = ExtentId(successor.get::<i64, _>("extent_id") as u32);
            let new_base_offset = successor.get::<i64, _>("base_offset") as u64;

            // Get primary replica address for the successor extent.
            let replica = sqlx::query(
                "SELECT node_addr FROM extent_replica \
                 WHERE stream_id = ? AND extent_id = ? AND role = 0",
            )
            .bind(stream_id.0 as i64)
            .bind(new_extent_id.0 as i64)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(format!("find successor primary: {e}")))?;

            let primary_addr = replica
                .map(|r| r.get::<String, _>("node_addr"))
                .unwrap_or_default();

            tx.commit()
                .await
                .map_err(|e| StorageError::Internal(format!("commit: {e}")))?;

            return Ok(SealResult::AlreadySealed {
                new_extent_id,
                new_base_offset,
                primary_addr,
            });
        }

        // Step 2: Seal the active extent.
        sqlx::query(
            "UPDATE extent SET state = ?, message_count = ?, sealed_at = NOW() \
             WHERE stream_id = ? AND extent_id = ? AND state = ?",
        )
        .bind(ExtentState::Sealed.as_u8())
        .bind(message_count as i64)
        .bind(stream_id.0 as i64)
        .bind(extent_id.0 as i64)
        .bind(ExtentState::Active.as_u8())
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(format!("seal extent: {e}")))?;

        // Step 3: Allocate new extent_id.
        sqlx::query(
            "UPDATE stream_sequence SET next_extent_id = next_extent_id + 1 WHERE stream_id = ?",
        )
        .bind(stream_id.0 as i64)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(format!("increment stream_sequence: {e}")))?;

        let seq_row = sqlx::query("SELECT next_extent_id FROM stream_sequence WHERE stream_id = ?")
            .bind(stream_id.0 as i64)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(format!("read stream_sequence: {e}")))?;

        let new_extent_id = ExtentId(seq_row.get::<i64, _>("next_extent_id") as u32);
        let new_base_offset = row.get::<i64, _>("base_offset") as u64 + message_count as u64;

        // Step 4: Insert new extent row.
        sqlx::query(
            "INSERT INTO extent (stream_id, extent_id, base_offset, state) VALUES (?, ?, ?, ?)",
        )
        .bind(stream_id.0 as i64)
        .bind(new_extent_id.0 as i64)
        .bind(new_base_offset as i64)
        .bind(ExtentState::Active.as_u8())
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(format!("insert extent: {e}")))?;

        // Step 5: Insert extent_replica rows for the new extent.
        for (addr, role) in nodes {
            sqlx::query(
                "INSERT INTO extent_replica (stream_id, extent_id, node_addr, role) VALUES (?, ?, ?, ?)",
            )
            .bind(stream_id.0 as i64)
            .bind(new_extent_id.0 as i64)
            .bind(addr)
            .bind(*role)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(format!("insert extent_replica: {e}")))?;
        }

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(format!("commit: {e}")))?;

        Ok(SealResult::Sealed { new_extent_id })
    }

    /// Get the active extent for a stream (there should be at most one).
    pub async fn get_active_extent(
        &self,
        stream_id: StreamId,
    ) -> Result<Option<ExtentRow>, StorageError> {
        let row = sqlx::query(
            "SELECT extent_id, stream_id, base_offset, message_count, state \
             FROM extent WHERE stream_id = ? AND state = ? LIMIT 1",
        )
        .bind(stream_id.0 as i64)
        .bind(ExtentState::Active.as_u8())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(format!("get_active_extent: {e}")))?;

        Ok(row.map(Self::map_extent_row))
    }

    /// Get all extents for a stream, ordered by extent_id.
    pub async fn get_extents(&self, stream_id: StreamId) -> Result<Vec<ExtentRow>, StorageError> {
        let rows = sqlx::query(
            "SELECT extent_id, stream_id, base_offset, message_count, state \
             FROM extent WHERE stream_id = ? ORDER BY extent_id",
        )
        .bind(stream_id.0 as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(format!("get_extents: {e}")))?;

        Ok(rows.into_iter().map(Self::map_extent_row).collect())
    }

    /// Get all active extents that have a replica on the given node address.
    pub async fn get_active_extents_on_node(
        &self,
        node_addr: &str,
    ) -> Result<Vec<ExtentRow>, StorageError> {
        let rows = sqlx::query(
            "SELECT e.extent_id, e.stream_id, e.base_offset, e.message_count, e.state \
             FROM extent e \
             INNER JOIN extent_replica r ON e.stream_id = r.stream_id AND e.extent_id = r.extent_id \
             WHERE r.node_addr = ? AND e.state = ?",
        )
        .bind(node_addr)
        .bind(ExtentState::Active.as_u8())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(format!("get_active_extents_on_node: {e}")))?;

        Ok(rows.into_iter().map(Self::map_extent_row).collect())
    }

    /// Get all replicas for an extent, ordered by role.
    pub async fn get_replicas(
        &self,
        stream_id: StreamId,
        extent_id: ExtentId,
    ) -> Result<Vec<ExtentReplicaRow>, StorageError> {
        let rows = sqlx::query(
            "SELECT stream_id, extent_id, node_addr, role \
             FROM extent_replica \
             WHERE stream_id = ? AND extent_id = ? ORDER BY role",
        )
        .bind(stream_id.0 as i64)
        .bind(extent_id.0 as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(format!("get_replicas: {e}")))?;

        Ok(rows
            .into_iter()
            .map(|r| ExtentReplicaRow {
                stream_id: StreamId(r.get::<i64, _>("stream_id") as u64),
                extent_id: ExtentId(r.get::<i64, _>("extent_id") as u32),
                node_addr: r.get("node_addr"),
                role: r.get::<i8, _>("role") as u8,
            })
            .collect())
    }

    /// Map a sqlx Row to an ExtentRow.
    fn map_extent_row(r: sqlx::mysql::MySqlRow) -> ExtentRow {
        let state_val = r.get::<i8, _>("state") as u8;
        ExtentRow {
            extent_id: ExtentId(r.get::<i64, _>("extent_id") as u32),
            stream_id: StreamId(r.get::<i64, _>("stream_id") as u64),
            base_offset: r.get::<i64, _>("base_offset") as u64,
            message_count: r.get::<i64, _>("message_count") as u32,
            state: ExtentState::from_u8(state_val).unwrap_or(ExtentState::Unspecified),
        }
    }

    // ── Management API queries ──

    /// Get replicas for an extent with node liveness info, ordered by role (Primary first).
    async fn get_replicas_with_liveness(
        &self,
        stream_id: StreamId,
        extent_id: ExtentId,
    ) -> Result<Vec<ReplicaDetail>, StorageError> {
        let rows = sqlx::query(
            "SELECT r.node_addr, r.role, COALESCE(n.state, 0) AS node_state \
             FROM extent_replica r \
             LEFT JOIN node n ON r.node_addr = n.addr \
             WHERE r.stream_id = ? AND r.extent_id = ? \
             ORDER BY r.role",
        )
        .bind(stream_id.0 as i64)
        .bind(extent_id.0 as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(format!("get_replicas_with_liveness: {e}")))?;

        Ok(rows
            .into_iter()
            .map(|r| {
                let state_val = r.get::<i8, _>("node_state") as u8;
                ReplicaDetail {
                    node_addr: r.get("node_addr"),
                    role: r.get::<i8, _>("role") as u8,
                    is_alive: state_val == NodeState::Alive.as_u8(),
                }
            })
            .collect())
    }

    /// Describe extents for a stream with full replica info and node liveness.
    ///
    /// Returns extents ordered by extent_id **descending** (latest first).
    /// - `count = 0`: return all extents.
    /// - `count = 1`: return just the latest (typically the active/mutable) extent.
    /// - `count = N`: return at most N extents from latest to earliest.
    pub async fn describe_stream_extents(
        &self,
        stream_id: StreamId,
        count: u32,
    ) -> Result<Vec<ExtentInfo>, StorageError> {
        let extent_rows = if count == 0 {
            sqlx::query(
                "SELECT extent_id, stream_id, base_offset, message_count, state \
                 FROM extent WHERE stream_id = ? ORDER BY extent_id DESC",
            )
            .bind(stream_id.0 as i64)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query(
                "SELECT extent_id, stream_id, base_offset, message_count, state \
                 FROM extent WHERE stream_id = ? ORDER BY extent_id DESC LIMIT ?",
            )
            .bind(stream_id.0 as i64)
            .bind(count)
            .fetch_all(&self.pool)
            .await
        }
        .map_err(|e| StorageError::Internal(format!("describe_stream_extents: {e}")))?;

        let mut result = Vec::with_capacity(extent_rows.len());
        for row in extent_rows {
            let ext = Self::map_extent_row(row);
            let replicas = self
                .get_replicas_with_liveness(stream_id, ext.extent_id)
                .await?;
            result.push(ExtentInfo {
                extent_id: ext.extent_id.0,
                base_offset: ext.base_offset,
                message_count: ext.message_count,
                state: ext.state,
                replicas,
            });
        }
        Ok(result)
    }

    /// Describe a single extent with full replica info and node liveness.
    ///
    /// Returns `None` if the extent does not exist.
    pub async fn describe_extent(
        &self,
        stream_id: StreamId,
        extent_id: ExtentId,
    ) -> Result<Option<ExtentInfo>, StorageError> {
        let row = sqlx::query(
            "SELECT extent_id, stream_id, base_offset, message_count, state \
             FROM extent WHERE stream_id = ? AND extent_id = ?",
        )
        .bind(stream_id.0 as i64)
        .bind(extent_id.0 as i64)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(format!("describe_extent: {e}")))?;

        match row {
            None => Ok(None),
            Some(r) => {
                let ext = Self::map_extent_row(r);
                let replicas = self
                    .get_replicas_with_liveness(stream_id, extent_id)
                    .await?;
                Ok(Some(ExtentInfo {
                    extent_id: ext.extent_id.0,
                    base_offset: ext.base_offset,
                    message_count: ext.message_count,
                    state: ext.state,
                    replicas,
                }))
            }
        }
    }

    /// Seek: find the extent containing the given logical offset.
    ///
    /// Resolution order:
    /// 1. Sealed/Flushed extent where `base_offset <= offset < base_offset + message_count`.
    /// 2. Active extent where `base_offset <= offset` (active extent's `message_count` in
    ///    metadata is 0 until sealed, but it may contain data beyond `base_offset`).
    ///
    /// Returns `None` if no extent can serve the offset (e.g., stream has no extents, or
    /// offset is negative/invalid).
    pub async fn seek_extent(
        &self,
        stream_id: StreamId,
        offset: u64,
    ) -> Result<Option<ExtentInfo>, StorageError> {
        // Try sealed/flushed extents first: base_offset <= offset < base_offset + message_count.
        let row = sqlx::query(
            "SELECT extent_id, stream_id, base_offset, message_count, state \
             FROM extent \
             WHERE stream_id = ? AND state IN (?, ?) \
               AND base_offset <= ? AND ? < base_offset + message_count \
             ORDER BY extent_id ASC LIMIT 1",
        )
        .bind(stream_id.0 as i64)
        .bind(ExtentState::Sealed.as_u8())
        .bind(ExtentState::Flushed.as_u8())
        .bind(offset as i64)
        .bind(offset as i64)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(format!("seek_extent (sealed): {e}")))?;

        if let Some(r) = row {
            let ext = Self::map_extent_row(r);
            let replicas = self
                .get_replicas_with_liveness(stream_id, ext.extent_id)
                .await?;
            return Ok(Some(ExtentInfo {
                extent_id: ext.extent_id.0,
                base_offset: ext.base_offset,
                message_count: ext.message_count,
                state: ext.state,
                replicas,
            }));
        }

        // Fall back to the Active extent where base_offset <= offset.
        let row = sqlx::query(
            "SELECT extent_id, stream_id, base_offset, message_count, state \
             FROM extent \
             WHERE stream_id = ? AND state = ? AND base_offset <= ? \
             ORDER BY extent_id DESC LIMIT 1",
        )
        .bind(stream_id.0 as i64)
        .bind(ExtentState::Active.as_u8())
        .bind(offset as i64)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(format!("seek_extent (active): {e}")))?;

        match row {
            None => Ok(None),
            Some(r) => {
                let ext = Self::map_extent_row(r);
                let replicas = self
                    .get_replicas_with_liveness(stream_id, ext.extent_id)
                    .await?;
                Ok(Some(ExtentInfo {
                    extent_id: ext.extent_id.0,
                    base_offset: ext.base_offset,
                    message_count: ext.message_count,
                    state: ext.state,
                    replicas,
                }))
            }
        }
    }

    // ── Node registry ──

    /// Register (or update) an ExtentNode node.
    pub async fn register_node(
        &self,
        node_id: &str,
        addr: &str,
        heartbeat_interval_ms: u32,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO node (node_id, addr, heartbeat_interval_ms, last_heartbeat, state) \
             VALUES (?, ?, ?, NOW(), ?) \
             ON DUPLICATE KEY UPDATE addr = VALUES(addr), \
             heartbeat_interval_ms = VALUES(heartbeat_interval_ms), \
             last_heartbeat = NOW(), state = VALUES(state)",
        )
        .bind(node_id)
        .bind(addr)
        .bind(heartbeat_interval_ms as i32)
        .bind(NodeState::Alive.as_u8())
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(format!("register_node: {e}")))?;
        Ok(())
    }

    /// Update heartbeat timestamp for a node.
    pub async fn update_heartbeat(&self, node_id: &str) -> Result<(), StorageError> {
        sqlx::query("UPDATE node SET last_heartbeat = NOW() WHERE node_id = ?")
            .bind(node_id)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(format!("update_heartbeat: {e}")))?;
        Ok(())
    }

    /// Get all alive nodes.
    pub async fn get_alive_nodes(&self) -> Result<Vec<NodeRow>, StorageError> {
        let rows = sqlx::query(
            "SELECT node_id, addr, heartbeat_interval_ms, state FROM node WHERE state = ?",
        )
        .bind(NodeState::Alive.as_u8())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(format!("get_alive_nodes: {e}")))?;

        Ok(rows.into_iter().map(Self::map_node_row).collect())
    }

    /// Get nodes whose last_heartbeat is older than 1.5x their declared interval.
    pub async fn get_expired_nodes(&self) -> Result<Vec<NodeRow>, StorageError> {
        let rows = sqlx::query(
            "SELECT node_id, addr, heartbeat_interval_ms, state FROM node \
             WHERE state = ? \
             AND last_heartbeat < NOW() - INTERVAL (heartbeat_interval_ms * 1.5 / 1000) SECOND",
        )
        .bind(NodeState::Alive.as_u8())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(format!("get_expired_nodes: {e}")))?;

        Ok(rows.into_iter().map(Self::map_node_row).collect())
    }

    /// Mark a node as dead.
    pub async fn mark_node_dead(&self, node_id: &str) -> Result<(), StorageError> {
        sqlx::query("UPDATE node SET state = ? WHERE node_id = ?")
            .bind(NodeState::Dead.as_u8())
            .bind(node_id)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(format!("mark_node_dead: {e}")))?;
        Ok(())
    }

    /// Map a sqlx Row to a NodeRow.
    fn map_node_row(r: sqlx::mysql::MySqlRow) -> NodeRow {
        let state_val = r.get::<i8, _>("state") as u8;
        NodeRow {
            node_id: r.get("node_id"),
            addr: r.get("addr"),
            heartbeat_interval_ms: r.get("heartbeat_interval_ms"),
            state: NodeState::from_u8(state_val).unwrap_or(NodeState::Unspecified),
        }
    }
}
