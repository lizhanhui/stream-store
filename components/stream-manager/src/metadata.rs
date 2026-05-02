use std::collections::HashMap;

use common::errors::{DatabaseSnafu, InternalSnafu, MigrationSnafu, StorageError};
use common::types::{
    ArenaClass, Epoch, EpochPolicy, EpochState, NodeMetrics, NodeState, ReplicaDetail, StorageClass,
    StreamEpochInfo, StreamId,
};
use snafu::ResultExt;
use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions};
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
    pub replication_factor: u8,
    pub cache_epochs: u16,
    pub storage_class: StorageClass,
    /// Declared per-stream arena class. P2 always persists `Dedicated`;
    /// Shared routing is wired in a later plan.
    pub arena_class: ArenaClass,
}

/// A row from the `stream_epochs` table.
#[derive(Debug, Clone)]
pub struct StreamEpochRow {
    pub stream_id: StreamId,
    pub start_offset: u64,
    pub end_offset: u64,
    pub state: EpochState,
    pub epoch: Epoch,
}

/// A sealed epoch that has been stuck past the staleness threshold.
#[derive(Debug, Clone)]
pub struct StaleExtentRow {
    pub stream_id: StreamId,
    pub epoch: Epoch,
    pub start_offset: u64,
    pub end_offset: u64,
}

/// A row from the `stream_replica` table (keyed by stream_id + epoch).
#[derive(Debug, Clone)]
pub struct StreamReplicaRow {
    pub stream_id: StreamId,
    pub epoch: Epoch,
    pub node_addr: String,
    pub role: u8,
}

/// Result of a transactional seal-and-allocate operation.
#[derive(Debug, Clone)]
pub enum SealResult {
    /// The current epoch was Active and has been sealed; a new epoch row was
    /// allocated and the stream's current epoch was advanced.
    Sealed { new_epoch: Epoch },
    /// The current epoch was already sealed by another client. Returns the
    /// successor epoch that was already allocated.
    AlreadySealed {
        new_epoch: Epoch,
        new_start_offset: u64,
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
        // MySQL servers with binary collation (e.g. utf8mb4_0900_bin) return
        // VARCHAR columns as VARBINARY, which sqlx cannot decode into Rust
        // String. Append charset to URL and set on options for belt-and-suspenders.
        let sqlx_url = if url.contains('?') {
            format!("{url}&charset=utf8mb4")
        } else {
            format!("{url}?charset=utf8mb4")
        };
        let options: MySqlConnectOptions = sqlx_url.parse().map_err(|e| {
            InternalSnafu {
                message: format!("parse MySQL URL: {e}"),
            }
            .build()
        })?;
        let options = options.charset("utf8mb4");

        let pool = MySqlPoolOptions::new()
            .max_connections(10)
            .connect_with(options)
            .await
            .context(DatabaseSnafu {
                message: "MySQL connect",
            })?;
        Ok(Self {
            pool,
            // Store the base URL (without charset) for Refinery migrations
            // (mysql_async doesn't support the charset URL parameter).
            url: url.to_string(),
        })
    }

    /// Run Refinery migrations against the database.
    ///
    /// Refinery uses `mysql_async` under the hood (separate from the sqlx pool).
    /// It creates a `refinery_schema_history` table to track applied migrations
    /// and only runs new ones.
    pub async fn migrate(&self) -> Result<(), StorageError> {
        let opts = mysql_async::Opts::from_url(&self.url).map_err(|e| {
            MigrationSnafu {
                message: format!("parse mysql url: {e}"),
            }
            .build()
        })?;
        let mut pool = mysql_async::Pool::new(opts);
        embedded::migrations::runner()
            .run_async(&mut pool)
            .await
            .map_err(|e| {
                MigrationSnafu {
                    message: format!("migration: {e}"),
                }
                .build()
            })?;
        pool.disconnect().await.map_err(|e| {
            MigrationSnafu {
                message: format!("disconnect migration pool: {e}"),
            }
            .build()
        })?;
        info!("database migrations applied");
        Ok(())
    }

    // ── Stream operations ──

    /// Create a new stream with a per-stream replication factor. Returns the assigned StreamId.
    pub async fn create_stream(
        &self,
        name: &str,
        replication_factor: u8,
        storage_class: StorageClass,
        policy: EpochPolicy,
    ) -> Result<StreamId, StorageError> {
        let result = sqlx::query(
            "INSERT INTO stream (stream_name, replication_factor, cache_epochs, storage_class) VALUES (?, ?, ?, ?)",
        )
        .bind(name)
        .bind(replication_factor)
        .bind(policy.cache)
        .bind(storage_class.as_u8())
        .execute(&self.pool)
        .await
        .context(DatabaseSnafu { message: "create_stream" })?;

        let stream_id = StreamId(result.last_insert_id() as u32);

        Ok(stream_id)
    }

    /// Get a stream by ID.
    pub async fn get_stream(&self, id: StreamId) -> Result<Option<StreamRow>, StorageError> {
        let row = sqlx::query(
            "SELECT stream_id, stream_name, replication_factor, cache_epochs, storage_class, arena_class FROM stream WHERE stream_id = ?",
        )
        .bind(id.0 as i64)
        .fetch_optional(&self.pool)
        .await
        .context(DatabaseSnafu { message: "get_stream" })?;

        Ok(row.map(|r| StreamRow {
            stream_id: StreamId(r.get::<u32, _>("stream_id")),
            stream_name: r.get("stream_name"),
            replication_factor: r.get::<u8, _>("replication_factor"),
            cache_epochs: r.get::<u16, _>("cache_epochs"),
            storage_class: StorageClass::from_u8(r.get::<u8, _>("storage_class"))
                .unwrap_or(StorageClass::S3),
            arena_class: ArenaClass::from_u8(r.get::<u8, _>("arena_class")).unwrap_or_default(),
        }))
    }

    /// Get a stream by name.
    pub async fn get_stream_by_name(&self, name: &str) -> Result<Option<StreamRow>, StorageError> {
        let row = sqlx::query(
            "SELECT stream_id, stream_name, replication_factor, cache_epochs, storage_class, arena_class FROM stream WHERE stream_name = ?",
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await
        .context(DatabaseSnafu { message: "get_stream_by_name" })?;

        Ok(row.map(|r| StreamRow {
            stream_id: StreamId(r.get::<u32, _>("stream_id")),
            stream_name: r.get("stream_name"),
            replication_factor: r.get::<u8, _>("replication_factor"),
            cache_epochs: r.get::<u16, _>("cache_epochs"),
            storage_class: StorageClass::from_u8(r.get::<u8, _>("storage_class"))
                .unwrap_or(StorageClass::S3),
            arena_class: ArenaClass::from_u8(r.get::<u8, _>("arena_class")).unwrap_or_default(),
        }))
    }

    /// Get all streams that have at least one open (Active) epoch row.
    /// Used during SM startup reconciliation to discover streams that may have
    /// epochs created autonomously by ENs during SM downtime.
    pub async fn get_streams_with_open_epochs(
        &self,
    ) -> Result<Vec<(StreamId, Epoch)>, StorageError> {
        let rows = sqlx::query(
            "SELECT DISTINCT e.stream_id, s.epoch \
             FROM stream_epochs e \
             INNER JOIN stream s ON e.stream_id = s.stream_id \
             WHERE e.state = ?",
        )
        .bind(EpochState::Active.as_u8())
        .fetch_all(&self.pool)
        .await
        .context(DatabaseSnafu {
            message: "get_streams_with_open_epochs",
        })?;

        Ok(rows
            .into_iter()
            .map(|r| {
                (
                    StreamId(r.get::<u32, _>("stream_id")),
                    Epoch(r.get::<i32, _>("epoch") as u32),
                )
            })
            .collect())
    }

    /// Get the replication factor for a stream.
    pub async fn get_stream_replication_factor(
        &self,
        stream_id: StreamId,
    ) -> Result<u8, StorageError> {
        let row = sqlx::query("SELECT replication_factor FROM stream WHERE stream_id = ?")
            .bind(stream_id.0)
            .fetch_one(&self.pool)
            .await
            .context(DatabaseSnafu {
                message: "get_stream_replication_factor",
            })?;

        Ok(row.get::<u8, _>("replication_factor"))
    }

    /// Get the cache_epochs (max epochs to retain in memory) for a stream.
    pub async fn get_stream_cache_epochs(&self, stream_id: StreamId) -> Result<u16, StorageError> {
        let row = sqlx::query("SELECT cache_epochs FROM stream WHERE stream_id = ?")
            .bind(stream_id.0)
            .fetch_one(&self.pool)
            .await
            .context(DatabaseSnafu {
                message: "get_stream_cache_epochs",
            })?;

        Ok(row.get::<u16, _>("cache_epochs"))
    }

    // ── Epoch-row operations ──

    /// Allocate a new epoch row for a stream on a replica set. Returns the
    /// newly-minted `Epoch`.
    ///
    /// `nodes` is ordered: [(primary_addr, 0), (secondary_1_addr, 1), ...].
    /// This method runs in a single MySQL transaction to prevent race conditions:
    /// 1. Bumps `stream.epoch` (the stream's current epoch is the new value).
    /// 2. Inserts a `stream_epochs` row keyed on `(stream_id, epoch)`.
    /// 3. Inserts `stream_replica` rows for the `(stream, epoch)` if not already present.
    pub async fn allocate_epoch_row(
        &self,
        stream_id: StreamId,
        start_offset: u64,
        nodes: &[(String, u8)],
        epoch: Epoch,
    ) -> Result<Epoch, StorageError> {
        if nodes.is_empty() {
            return InternalSnafu {
                message: "allocate_epoch_row: empty node list",
            }
            .fail();
        }

        let mut conn = self.pool.acquire().await.context(DatabaseSnafu {
            message: "acquire connection",
        })?;
        let mut tx = conn.begin().await.context(DatabaseSnafu {
            message: "begin transaction",
        })?;

        // Step 1: Persist the stream's current epoch. The caller supplies the
        // epoch being allocated; create-stream allocates epoch 0, while seal /
        // failover callers pass the already-bumped successor epoch.
        sqlx::query("UPDATE stream SET epoch = GREATEST(epoch, ?) WHERE stream_id = ?")
            .bind(epoch.0 as i32)
            .bind(stream_id.0)
            .execute(&mut *tx)
            .await
            .context(DatabaseSnafu {
                message: "set stream epoch",
            })?;

        // Step 2: Insert the epoch row.
        sqlx::query(
            "INSERT INTO stream_epochs (stream_id, epoch, start_offset, end_offset, state) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(stream_id.0)
        .bind(epoch.0 as i32)
        .bind(start_offset as i64)
        .bind(start_offset as i64) // end_offset = start_offset for new active epoch
        .bind(EpochState::Active.as_u8())
        .execute(&mut *tx)
        .await
        .context(DatabaseSnafu { message: "insert stream_epochs row" })?;

        // Step 3: Insert stream_replica rows keyed by (stream_id, epoch).
        // INSERT IGNORE — within an epoch the replica set is written once.
        for (addr, role) in nodes {
            sqlx::query(
                "INSERT IGNORE INTO stream_replica (stream_id, epoch, node_addr, role) VALUES (?, ?, ?, ?)",
            )
            .bind(stream_id.0)
            .bind(epoch.0 as i32)
            .bind(addr)
            .bind(*role)
            .execute(&mut *tx)
            .await
            .context(DatabaseSnafu { message: "insert stream_replica" })?;
        }

        tx.commit()
            .await
            .context(DatabaseSnafu { message: "commit" })?;

        Ok(epoch)
    }

    /// Seal an epoch row: update state to SEALED and record end_offset.
    pub async fn seal_epoch_row(
        &self,
        stream_id: StreamId,
        epoch: Epoch,
        end_offset: u64,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "UPDATE stream_epochs SET state = ?, end_offset = ?, sealed_at = NOW() \
             WHERE stream_id = ? AND epoch = ?",
        )
        .bind(EpochState::Sealed.as_u8())
        .bind(end_offset as i64)
        .bind(stream_id.0)
        .bind(epoch.0 as i32)
        .execute(&self.pool)
        .await
        .context(DatabaseSnafu {
            message: "seal_epoch_row",
        })?;
        Ok(())
    }

    /// Seal the current epoch row and allocate the next one in a single MySQL
    /// transaction.
    ///
    /// Uses `SELECT ... FOR UPDATE` to ensure concurrent safety:
    /// - If the current epoch row is Active: seal it, bump `stream.epoch`,
    ///   insert a new row for the new epoch, and return `Sealed`.
    /// - If the current epoch row is already Sealed: find the successor
    ///   (next-higher epoch row) and return `AlreadySealed`.
    pub async fn seal_and_allocate_next_epoch_transaction(
        &self,
        stream_id: StreamId,
        sealed_epoch: Epoch,
        new_epoch: Epoch,
        end_offset: u64,
        nodes: &[(String, u8)],
    ) -> Result<SealResult, StorageError> {
        let mut conn = self.pool.acquire().await.context(DatabaseSnafu {
            message: "acquire connection",
        })?;
        let mut tx = conn.begin().await.context(DatabaseSnafu {
            message: "begin transaction",
        })?;

        // Step 1: Lock the target epoch row and check state.
        let row = sqlx::query(
            "SELECT state, start_offset \
             FROM stream_epochs WHERE stream_id = ? AND epoch = ? FOR UPDATE",
        )
        .bind(stream_id.0)
        .bind(sealed_epoch.0 as i32)
        .fetch_optional(&mut *tx)
        .await
        .context(DatabaseSnafu {
            message: "lock epoch row",
        })?;

        let row = row.ok_or_else(|| {
            InternalSnafu {
                message: format!(
                    "epoch row not found: stream={}, epoch={}",
                    stream_id, sealed_epoch
                ),
            }
            .build()
        })?;

        let state_val = row.get::<i8, _>("state") as u8;
        let state = EpochState::from_u8(state_val).unwrap_or(EpochState::Unspecified);

        if state == EpochState::Sealed {
            // Already sealed — find the successor (next epoch row with higher epoch).
            let successor = sqlx::query(
                "SELECT epoch, start_offset FROM stream_epochs \
                 WHERE stream_id = ? AND epoch > ? \
                 ORDER BY epoch ASC LIMIT 1",
            )
            .bind(stream_id.0)
            .bind(sealed_epoch.0 as i32)
            .fetch_optional(&mut *tx)
            .await
            .context(DatabaseSnafu {
                message: "find successor",
            })?;

            if let Some(successor) = successor {
                let new_epoch = Epoch(successor.get::<i32, _>("epoch") as u32);
                let new_start_offset = successor.get::<i64, _>("start_offset") as u64;

                // Get primary replica address from the successor epoch's replica set.
                let replica = sqlx::query(
                    "SELECT node_addr FROM stream_replica \
                     WHERE stream_id = ? AND epoch = ? AND role = 0",
                )
                .bind(stream_id.0)
                .bind(new_epoch.0 as i32)
                .fetch_optional(&mut *tx)
                .await
                .context(DatabaseSnafu {
                    message: "find successor primary",
                })?;

                let primary_addr = replica
                    .map(|r| r.get::<String, _>("node_addr"))
                    .unwrap_or_default();

                tx.commit()
                    .await
                    .context(DatabaseSnafu { message: "commit" })?;

                return Ok(SealResult::AlreadySealed {
                    new_epoch,
                    new_start_offset,
                    primary_addr,
                });
            }

            // No successor — fall through to allocate a new epoch row.
            // Use the sealed epoch's end_offset as the new start_offset.
        }

        // Step 2: Seal the active epoch row (idempotent if already sealed).
        sqlx::query(
            "UPDATE stream_epochs SET state = ?, end_offset = ?, sealed_at = NOW() \
             WHERE stream_id = ? AND epoch = ? AND state = ?",
        )
        .bind(EpochState::Sealed.as_u8())
        .bind(end_offset as i64)
        .bind(stream_id.0)
        .bind(sealed_epoch.0 as i32)
        .bind(EpochState::Active.as_u8())
        .execute(&mut *tx)
        .await
        .context(DatabaseSnafu {
            message: "seal epoch row",
        })?;

        // Step 3: Persist the already-minted successor epoch.
        sqlx::query("UPDATE stream SET epoch = GREATEST(epoch, ?) WHERE stream_id = ?")
            .bind(new_epoch.0 as i32)
            .bind(stream_id.0)
            .execute(&mut *tx)
            .await
            .context(DatabaseSnafu {
                message: "set stream epoch",
            })?;

        let new_start_offset = end_offset;

        // Step 4: Insert new epoch row.
        sqlx::query(
            "INSERT INTO stream_epochs (stream_id, epoch, start_offset, end_offset, state) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(stream_id.0)
        .bind(new_epoch.0 as i32)
        .bind(new_start_offset as i64)
        .bind(new_start_offset as i64) // end_offset = start_offset for new active epoch
        .bind(EpochState::Active.as_u8())
        .execute(&mut *tx)
        .await
        .context(DatabaseSnafu { message: "insert stream_epochs row" })?;

        // Step 5: Insert stream_replica rows for the new (stream, epoch).
        // INSERT IGNORE — within an epoch the replica set is written once.
        for (addr, role) in nodes {
            sqlx::query(
                "INSERT IGNORE INTO stream_replica (stream_id, epoch, node_addr, role) VALUES (?, ?, ?, ?)",
            )
            .bind(stream_id.0)
            .bind(new_epoch.0 as i32)
            .bind(addr)
            .bind(*role)
            .execute(&mut *tx)
            .await
            .context(DatabaseSnafu { message: "insert stream_replica" })?;
        }

        tx.commit()
            .await
            .context(DatabaseSnafu { message: "commit" })?;

        Ok(SealResult::Sealed { new_epoch })
    }

    /// Get the active epoch for a stream (there should be at most one).
    pub async fn get_active_extent(
        &self,
        stream_id: StreamId,
    ) -> Result<Option<StreamEpochRow>, StorageError> {
        let row = sqlx::query(
            "SELECT stream_id, start_offset, end_offset, state, epoch \
             FROM stream_epochs WHERE stream_id = ? AND state = ? LIMIT 1",
        )
        .bind(stream_id.0)
        .bind(EpochState::Active.as_u8())
        .fetch_optional(&self.pool)
        .await
        .context(DatabaseSnafu {
            message: "get_active_extent",
        })?;

        Ok(row.map(Self::map_epoch_row))
    }

    /// Get all epochs for a stream, ordered by epoch.
    pub async fn get_extents(&self, stream_id: StreamId) -> Result<Vec<StreamEpochRow>, StorageError> {
        let rows = sqlx::query(
            "SELECT stream_id, start_offset, end_offset, state, epoch \
             FROM stream_epochs WHERE stream_id = ? ORDER BY epoch",
        )
        .bind(stream_id.0)
        .fetch_all(&self.pool)
        .await
        .context(DatabaseSnafu {
            message: "get_extents",
        })?;

        Ok(rows.into_iter().map(Self::map_epoch_row).collect())
    }

    /// Get all active epochs that have a replica on the given node address.
    pub async fn get_active_extents_on_node(
        &self,
        node_addr: &str,
    ) -> Result<Vec<StreamEpochRow>, StorageError> {
        let rows = sqlx::query(
            "SELECT e.stream_id, e.start_offset, e.end_offset, e.state, e.epoch \
             FROM stream_epochs e \
             INNER JOIN stream_replica r ON e.stream_id = r.stream_id AND e.epoch = r.epoch \
             WHERE r.node_addr = ? AND e.state = ?",
        )
        .bind(node_addr)
        .bind(EpochState::Active.as_u8())
        .fetch_all(&self.pool)
        .await
        .context(DatabaseSnafu {
            message: "get_active_extents_on_node",
        })?;

        Ok(rows.into_iter().map(Self::map_epoch_row).collect())
    }

    /// Get all replicas for a (stream, epoch) pair, ordered by role.
    pub async fn get_replicas(
        &self,
        stream_id: StreamId,
        epoch: Epoch,
    ) -> Result<Vec<StreamReplicaRow>, StorageError> {
        let rows = sqlx::query(
            "SELECT stream_id, epoch, node_addr, role \
             FROM stream_replica \
             WHERE stream_id = ? AND epoch = ? ORDER BY role",
        )
        .bind(stream_id.0)
        .bind(epoch.0 as i32)
        .fetch_all(&self.pool)
        .await
        .context(DatabaseSnafu {
            message: "get_replicas",
        })?;

        Ok(rows
            .into_iter()
            .map(|r| StreamReplicaRow {
                stream_id: StreamId(r.get::<u32, _>("stream_id")),
                epoch: Epoch(r.get::<i32, _>("epoch") as u32),
                node_addr: r.get("node_addr"),
                role: r.get::<i8, _>("role") as u8,
            })
            .collect())
    }

    /// Map a sqlx Row to a StreamEpochRow.
    fn map_epoch_row(r: sqlx::mysql::MySqlRow) -> StreamEpochRow {
        let state_val = r.get::<i8, _>("state") as u8;
        let epoch = Epoch(r.get::<i32, _>("epoch") as u32);
        StreamEpochRow {
            stream_id: StreamId(r.get::<u32, _>("stream_id")),
            start_offset: r.get::<i64, _>("start_offset") as u64,
            end_offset: r.get::<i64, _>("end_offset") as u64,
            state: EpochState::from_u8(state_val).unwrap_or(EpochState::Unspecified),
            epoch,
        }
    }

    // ── Management API queries ──

    /// Get replicas for a (stream, epoch) with node liveness info, ordered by role (Primary first).
    async fn get_replicas_with_liveness(
        &self,
        stream_id: StreamId,
        epoch: Epoch,
    ) -> Result<Vec<ReplicaDetail>, StorageError> {
        let rows = sqlx::query(
            "SELECT r.node_addr, r.role, COALESCE(n.state, 0) AS node_state \
             FROM stream_replica r \
             LEFT JOIN node n ON r.node_addr = n.addr \
             WHERE r.stream_id = ? AND r.epoch = ? \
             ORDER BY r.role",
        )
        .bind(stream_id.0)
        .bind(epoch.0 as i32)
        .fetch_all(&self.pool)
        .await
        .context(DatabaseSnafu {
            message: "get_replicas_with_liveness",
        })?;

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
    /// Returns epochs ordered by epoch descending (latest first).
    /// - `count = 0`: return all extents.
    /// - `count = 1`: return just the latest (typically the active/mutable) extent.
    /// - `count = N`: return at most N extents from latest to earliest.
    pub async fn describe_stream_extents(
        &self,
        stream_id: StreamId,
        count: u32,
    ) -> Result<Vec<StreamEpochInfo>, StorageError> {
        let extent_rows = if count == 0 {
            sqlx::query(
                "SELECT stream_id, start_offset, end_offset, state, epoch \
                 FROM stream_epochs WHERE stream_id = ? ORDER BY epoch DESC",
            )
            .bind(stream_id.0)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query(
                "SELECT stream_id, start_offset, end_offset, state, epoch \
                 FROM stream_epochs WHERE stream_id = ? ORDER BY epoch DESC LIMIT ?",
            )
            .bind(stream_id.0)
            .bind(count)
            .fetch_all(&self.pool)
            .await
        }
        .context(DatabaseSnafu {
            message: "describe_stream_extents",
        })?;

        let mut result = Vec::with_capacity(extent_rows.len());
        for row in extent_rows {
            let ext = Self::map_epoch_row(row);
            let replicas = self
                .get_replicas_with_liveness(stream_id, ext.epoch)
                .await?;
            result.push(StreamEpochInfo {
                start_offset: ext.start_offset,
                end_offset: ext.end_offset,
                epoch: ext.epoch,
                state: ext.state,
                replicas,
            });
        }
        Ok(result)
    }

    /// Describe a single extent with full replica info and node liveness.
    ///
    /// Returns `None` if the extent does not exist.
    pub async fn describe_epoch(
        &self,
        stream_id: StreamId,
        epoch: Epoch,
    ) -> Result<Option<StreamEpochInfo>, StorageError> {
        let row = sqlx::query(
            "SELECT stream_id, start_offset, end_offset, state, epoch \
             FROM stream_epochs WHERE stream_id = ? AND epoch = ?",
        )
        .bind(stream_id.0)
        .bind(epoch.0 as i32)
        .fetch_optional(&self.pool)
        .await
        .context(DatabaseSnafu {
            message: "describe_epoch",
        })?;

        match row {
            None => Ok(None),
            Some(r) => {
                let ext = Self::map_epoch_row(r);
                let replicas = self
                    .get_replicas_with_liveness(stream_id, ext.epoch)
                    .await?;
                Ok(Some(StreamEpochInfo {
                        start_offset: ext.start_offset,
                    end_offset: ext.end_offset,
                    epoch: ext.epoch,
                    state: ext.state,
                    replicas,
                }))
            }
        }
    }

    /// Seek: find the extent containing the given logical offset.
    ///
    /// Resolution order:
    /// 1. Sealed/Flushed extent where `start_offset <= offset < end_offset`.
    /// 2. Active extent where `start_offset <= offset` (active extent's `end_offset` in
    ///    metadata equals `start_offset` until sealed, but it may contain data beyond that).
    ///
    /// Returns `None` if no extent can serve the offset (e.g., stream has no extents, or
    /// offset is negative/invalid).
    pub async fn seek_extent(
        &self,
        stream_id: StreamId,
        offset: u64,
    ) -> Result<Option<StreamEpochInfo>, StorageError> {
        // Try sealed/flushed extents first: start_offset <= offset < end_offset.
        let row = sqlx::query(
            "SELECT stream_id, start_offset, end_offset, state, epoch \
             FROM stream_epochs \
             WHERE stream_id = ? AND state IN (?, ?) \
               AND start_offset <= ? AND ? < end_offset \
             ORDER BY epoch ASC LIMIT 1",
        )
        .bind(stream_id.0)
        .bind(EpochState::Sealed.as_u8())
        .bind(EpochState::Flushed.as_u8())
        .bind(offset as i64)
        .bind(offset as i64)
        .fetch_optional(&self.pool)
        .await
        .context(DatabaseSnafu {
            message: "seek_extent (sealed)",
        })?;

        if let Some(r) = row {
            let ext = Self::map_epoch_row(r);
            let replicas = self
                .get_replicas_with_liveness(stream_id, ext.epoch)
                .await?;
            return Ok(Some(StreamEpochInfo {
                start_offset: ext.start_offset,
                end_offset: ext.end_offset,
                epoch: ext.epoch,
                state: ext.state,
                replicas,
            }));
        }

        // Fall back to the Active extent where start_offset <= offset.
        let row = sqlx::query(
            "SELECT stream_id, start_offset, end_offset, state, epoch \
             FROM stream_epochs \
             WHERE stream_id = ? AND state = ? AND start_offset <= ? \
             ORDER BY epoch DESC LIMIT 1",
        )
        .bind(stream_id.0)
        .bind(EpochState::Active.as_u8())
        .bind(offset as i64)
        .fetch_optional(&self.pool)
        .await
        .context(DatabaseSnafu {
            message: "seek_extent (active)",
        })?;

        match row {
            None => Ok(None),
            Some(r) => {
                let ext = Self::map_epoch_row(r);
                let replicas = self
                    .get_replicas_with_liveness(stream_id, ext.epoch)
                    .await?;
                Ok(Some(StreamEpochInfo {
                        start_offset: ext.start_offset,
                    end_offset: ext.end_offset,
                    epoch: ext.epoch,
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
        .context(DatabaseSnafu {
            message: "register_node",
        })?;
        Ok(())
    }

    /// Update heartbeat timestamp for a node.
    pub async fn update_heartbeat(&self, node_id: &str) -> Result<(), StorageError> {
        sqlx::query("UPDATE node SET last_heartbeat = NOW() WHERE node_id = ?")
            .bind(node_id)
            .execute(&self.pool)
            .await
            .context(DatabaseSnafu {
                message: "update_heartbeat",
            })?;
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
        .context(DatabaseSnafu {
            message: "get_alive_nodes",
        })?;

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
        .context(DatabaseSnafu {
            message: "get_expired_nodes",
        })?;

        Ok(rows.into_iter().map(Self::map_node_row).collect())
    }

    /// Mark a node as dead.
    pub async fn mark_node_dead(&self, node_id: &str) -> Result<(), StorageError> {
        sqlx::query("UPDATE node SET state = ? WHERE node_id = ?")
            .bind(NodeState::Dead.as_u8())
            .bind(node_id)
            .execute(&self.pool)
            .await
            .context(DatabaseSnafu {
                message: "mark_node_dead",
            })?;
        Ok(())
    }

    // ── Node metrics persistence ──

    /// Persist runtime metrics for a node (called on every heartbeat).
    pub async fn persist_node_metrics(
        &self,
        node_id: &str,
        metrics: &NodeMetrics,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO node_metrics \
             (node_id, available_memory_bytes, total_memory_bytes, \
              appends_per_sec, active_extent_count, bytes_written_per_sec) \
             VALUES (?, ?, ?, ?, ?, ?) \
             ON DUPLICATE KEY UPDATE \
               available_memory_bytes = VALUES(available_memory_bytes), \
               total_memory_bytes = VALUES(total_memory_bytes), \
               appends_per_sec = VALUES(appends_per_sec), \
               active_extent_count = VALUES(active_extent_count), \
               bytes_written_per_sec = VALUES(bytes_written_per_sec)",
        )
        .bind(node_id)
        .bind(metrics.available_memory_bytes)
        .bind(metrics.total_memory_bytes)
        .bind(metrics.appends_per_sec)
        .bind(metrics.active_extent_count)
        .bind(metrics.bytes_written_per_sec)
        .execute(&self.pool)
        .await
        .context(DatabaseSnafu {
            message: "persist_node_metrics",
        })?;
        Ok(())
    }

    /// Load all node metrics from the database (for load-aware placement).
    pub async fn get_all_node_metrics(&self) -> Result<HashMap<String, NodeMetrics>, StorageError> {
        let rows = sqlx::query(
            "SELECT node_id, available_memory_bytes, total_memory_bytes, \
                    appends_per_sec, active_extent_count, bytes_written_per_sec \
             FROM node_metrics",
        )
        .fetch_all(&self.pool)
        .await
        .context(DatabaseSnafu {
            message: "get_all_node_metrics",
        })?;

        Ok(rows
            .into_iter()
            .map(|r| {
                let node_id: String = r.get("node_id");
                let metrics = NodeMetrics {
                    available_memory_bytes: r.get::<u64, _>("available_memory_bytes"),
                    total_memory_bytes: r.get::<u64, _>("total_memory_bytes"),
                    appends_per_sec: r.get::<u32, _>("appends_per_sec"),
                    active_extent_count: r.get::<u32, _>("active_extent_count"),
                    bytes_written_per_sec: r.get::<u64, _>("bytes_written_per_sec"),
                };
                (node_id, metrics)
            })
            .collect())
    }

    /// Check if a node is alive by its address.
    pub async fn is_node_alive_by_addr(&self, addr: &str) -> Result<bool, StorageError> {
        let row = sqlx::query("SELECT state FROM node WHERE addr = ?")
            .bind(addr)
            .fetch_optional(&self.pool)
            .await
            .context(DatabaseSnafu {
                message: "is_node_alive_by_addr",
            })?;
        Ok(row
            .map(|r| r.get::<i8, _>("state") as u8 == NodeState::Alive.as_u8())
            .unwrap_or(false))
    }

    /// Query sealed extents that have been stuck past the given threshold.
    /// Returns only S3-class streams with RF > 1 (must have secondaries).
    pub async fn get_stale_sealed_extents(
        &self,
        threshold_secs: u64,
    ) -> Result<Vec<StaleExtentRow>, StorageError> {
        let rows = sqlx::query(
            "SELECT e.stream_id, e.epoch, e.start_offset, e.end_offset \
             FROM stream_epochs e \
             JOIN stream s ON e.stream_id = s.stream_id \
             WHERE e.state = ? \
               AND e.sealed_at IS NOT NULL \
               AND e.sealed_at < NOW() - INTERVAL ? SECOND \
               AND s.storage_class = 0 \
               AND s.replication_factor > 1 \
             ORDER BY e.sealed_at ASC",
        )
        .bind(EpochState::Sealed.as_u8())
        .bind(threshold_secs as i64)
        .fetch_all(&self.pool)
        .await
        .context(DatabaseSnafu {
            message: "get_stale_sealed_extents",
        })?;

        Ok(rows
            .iter()
            .map(|row| {
                let epoch = Epoch(row.get::<i32, _>("epoch") as u32);
                StaleExtentRow {
                    stream_id: StreamId(row.get::<u32, _>("stream_id")),
                    epoch,
                    start_offset: row.get::<i64, _>("start_offset") as u64,
                    end_offset: row.get::<i64, _>("end_offset") as u64,
                }
            })
            .collect())
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

    // ── Epoch operations ──

    /// Get the current epoch for a stream.
    pub async fn get_stream_epoch(&self, stream_id: StreamId) -> Result<Epoch, StorageError> {
        let row = sqlx::query("SELECT epoch FROM stream WHERE stream_id = ?")
            .bind(stream_id.0)
            .fetch_one(&self.pool)
            .await
            .context(DatabaseSnafu {
                message: "get_stream_epoch",
            })?;
        Ok(Epoch(row.get::<i32, _>("epoch") as u32))
    }

    /// Bump the epoch for a stream. Returns the new epoch.
    ///
    /// Uses compare-and-swap: only increments if the current epoch matches
    /// `expected`. Returns an error if a concurrent bump was detected.
    pub async fn bump_epoch(&self, stream_id: StreamId) -> Result<Epoch, StorageError> {
        let current = self.get_stream_epoch(stream_id).await?;

        let result =
            sqlx::query("UPDATE stream SET epoch = epoch + 1 WHERE stream_id = ? AND epoch = ?")
                .bind(stream_id.0)
                .bind(current.0 as i32)
                .execute(&self.pool)
                .await
                .context(DatabaseSnafu {
                    message: "bump_epoch",
                })?;

        if result.rows_affected() == 0 {
            return InternalSnafu {
                message: "epoch CAS failed: concurrent bump detected",
            }
            .fail();
        }

        Ok(Epoch(current.0 + 1))
    }

    // ── Leadership lease operations ──

    /// Try to acquire the leadership lease. Returns true if acquired.
    ///
    /// Succeeds if the lease is expired or already held by this node.
    pub async fn try_acquire_leadership(
        &self,
        node_id: &str,
        lease_duration_secs: u32,
    ) -> Result<bool, StorageError> {
        let result = sqlx::query(
            "UPDATE stream_manager_leadership \
             SET node_id = ?, lease_until = DATE_ADD(NOW(), INTERVAL ? SECOND) \
             WHERE id = 1 AND (lease_until < NOW() OR node_id = ?)",
        )
        .bind(node_id)
        .bind(lease_duration_secs)
        .bind(node_id)
        .execute(&self.pool)
        .await
        .context(DatabaseSnafu {
            message: "acquire_leadership",
        })?;

        Ok(result.rows_affected() > 0)
    }

    /// Renew the leadership lease. Returns true if renewed (caller still holds it).
    pub async fn renew_leadership(
        &self,
        node_id: &str,
        lease_duration_secs: u32,
    ) -> Result<bool, StorageError> {
        let result = sqlx::query(
            "UPDATE stream_manager_leadership \
             SET lease_until = DATE_ADD(NOW(), INTERVAL ? SECOND) \
             WHERE id = 1 AND node_id = ?",
        )
        .bind(lease_duration_secs)
        .bind(node_id)
        .execute(&self.pool)
        .await
        .context(DatabaseSnafu {
            message: "renew_leadership",
        })?;

        Ok(result.rows_affected() > 0)
    }

    /// Release the leadership lease (graceful shutdown).
    pub async fn release_leadership(&self, node_id: &str) -> Result<(), StorageError> {
        sqlx::query(
            "UPDATE stream_manager_leadership \
             SET node_id = '', lease_until = '2000-01-01 00:00:00' \
             WHERE id = 1 AND node_id = ?",
        )
        .bind(node_id)
        .execute(&self.pool)
        .await
        .context(DatabaseSnafu {
            message: "release_leadership",
        })?;
        Ok(())
    }

    /// Return the current leader's node_id if the lease is still active.
    pub async fn get_leader(&self) -> Result<Option<String>, StorageError> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT node_id FROM stream_manager_leadership \
             WHERE id = 1 AND node_id != '' AND lease_until >= NOW()",
        )
        .fetch_optional(&self.pool)
        .await
        .context(DatabaseSnafu {
            message: "get_leader",
        })?;
        Ok(row.map(|(id,)| id))
    }

    /// Record a progress update for an active extent (periodic observability report).
    ///
    /// Updates end_offset for the extent if the reported offset is larger than
    /// the current value. Only updates Active extents. Idempotent and epoch-validated.
    pub async fn record_epoch_progress(
        &self,
        stream_id: StreamId,
        epoch: Epoch,
        current_offset: u64,
    ) -> Result<(), StorageError> {
        let mut conn = self.pool.acquire().await.context(DatabaseSnafu {
            message: "acquire connection",
        })?;
        let mut tx = conn.begin().await.context(DatabaseSnafu {
            message: "begin transaction",
        })?;

        // Validate epoch matches current stream epoch.
        let row = sqlx::query("SELECT epoch FROM stream WHERE stream_id = ? FOR UPDATE")
            .bind(stream_id.0)
            .fetch_optional(&mut *tx)
            .await
            .context(DatabaseSnafu {
                message: "lock stream",
            })?;

        if let Some(row) = row {
            let current_epoch = Epoch(row.get::<i32, _>("epoch") as u32);
            if epoch != current_epoch {
                // Stale notification from an old epoch — skip.
                tx.commit()
                    .await
                    .context(DatabaseSnafu { message: "commit" })?;
                return Ok(());
            }
        } else {
            return InternalSnafu {
                message: format!("stream {:?} not found", stream_id),
            }
            .fail();
        }

        // Update end_offset only if the new value is larger (monotonic progress).
        sqlx::query(
            "UPDATE stream_epochs SET end_offset = ? \
             WHERE stream_id = ? AND epoch = ? AND state = ? AND end_offset < ?",
        )
        .bind(current_offset as i64)
        .bind(stream_id.0)
        .bind(epoch.0 as i32)
        .bind(EpochState::Active.as_u8())
        .bind(current_offset as i64)
        .execute(&mut *tx)
        .await
        .context(DatabaseSnafu {
            message: "update extent progress",
        })?;

        tx.commit()
            .await
            .context(DatabaseSnafu { message: "commit" })?;

        Ok(())
    }

    /// Record that an extent was flushed to S3 (EN confirms upload via UpdateExtentFlushed).
    ///
    /// Terminal state is always `Flushed` and this is an upsert: the extent row may
    /// not yet exist when this notification arrives. The handler accepts any starting
    /// row state:
    ///
    /// - **row missing**  → `INSERT` a fresh row directly in state `Flushed` with the
    ///   offsets from the notification.
    /// - **`Active`**    → `UPDATE` to `Flushed`, set `end_offset` (may have been a
    ///   stale placeholder) and backfill `sealed_at`.
    /// - **`Sealed`**    → `UPDATE` to `Flushed` (the normal, in-order path).
    /// - **`Flushed`**   → idempotent no-op.
    ///
    /// Epoch semantics: the reported epoch is the **per-extent creation epoch**
    /// (immutable after allocation), NOT the stream's current epoch — which may be
    /// higher after an SM-driven seal/failover bumped it. We validate the reported
    /// epoch against the *extent row's* stored epoch only (after the seeding
    /// INSERT IGNORE + re-read). A mismatch is a stale/bogus notification and is
    /// skipped.
    pub async fn record_arena_flushed(
        &self,
        stream_id: StreamId,
        epoch: Epoch,
        start_offset: u64,
        end_offset: u64,
        s3_key: &str,
    ) -> Result<(), StorageError> {
        let mut conn = self.pool.acquire().await.context(DatabaseSnafu {
            message: "acquire connection",
        })?;
        let mut tx = conn.begin().await.context(DatabaseSnafu {
            message: "begin transaction",
        })?;

        // Verify the stream exists, but do NOT compare against its current epoch:
        // SM-driven seal bumps the stream epoch ahead of the extent's creation
        // epoch, so a legitimate flush notification routinely reports an older
        // epoch than the stream's current one. The per-extent epoch check below
        // (after re-reading the extent row) is the correct guard.
        let stream_row = sqlx::query("SELECT epoch FROM stream WHERE stream_id = ? FOR UPDATE")
            .bind(stream_id.0)
            .fetch_optional(&mut *tx)
            .await
            .context(DatabaseSnafu {
                message: "lock stream",
            })?;

        let Some(stream_row) = stream_row else {
            return InternalSnafu {
                message: format!("stream {:?} not found", stream_id),
            }
            .fail();
        };
        let current_epoch = Epoch(stream_row.get::<i32, _>("epoch") as u32);
        if epoch.0 > current_epoch.0 {
            tx.commit()
                .await
                .context(DatabaseSnafu { message: "commit" })?;
            return Ok(());
        }

        sqlx::query(
            "INSERT IGNORE INTO stream_epoch_s3 \
                 (stream_id, epoch, start_offset, end_offset, s3_key) \
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(stream_id.0)
        .bind(epoch.0 as i32)
        .bind(start_offset as i64)
        .bind(end_offset as i64)
        .bind(s3_key)
        .execute(&mut *tx)
        .await
        .context(DatabaseSnafu {
            message: "insert stream_epoch_s3 row",
        })?;

        // Seed the row if it doesn't yet exist. INSERT IGNORE leaves an existing
        // row alone — the next query (SELECT FOR UPDATE) picks it up and the match
        // below handles it. If the INSERT takes, the row is already terminal
        // (Flushed) with correct offsets; the subsequent SELECT will observe state
        // = Flushed and hit the idempotent no-op branch.
        sqlx::query(
            "INSERT IGNORE INTO stream_epochs \
                 (stream_id, epoch, start_offset, end_offset, state, sealed_at, flushed_at) \
             VALUES (?, ?, ?, ?, ?, NOW(3), NOW(3))",
        )
        .bind(stream_id.0)
        .bind(epoch.0 as i32)
        .bind(start_offset as i64)
        .bind(end_offset as i64)
        .bind(EpochState::Flushed.as_u8())
        .execute(&mut *tx)
        .await
        .context(DatabaseSnafu {
            message: "insert flushed extent",
        })?;

        // Re-read the row's authoritative state + epoch after the seeding insert.
        // If the row pre-existed, this returns its original state (Active / Sealed /
        // Flushed). If we just inserted it, this returns Flushed and we no-op.
        let row = sqlx::query(
            "SELECT epoch, state FROM stream_epochs WHERE stream_id = ? AND epoch = ? FOR UPDATE",
        )
        .bind(stream_id.0)
        .bind(epoch.0 as i32)
        .fetch_one(&mut *tx)
        .await
        .context(DatabaseSnafu {
            message: "lock epoch",
        })?;

        let stored_epoch = Epoch(row.get::<i32, _>("epoch") as u32);
        let state_raw = row.get::<i8, _>("state") as u8;
        let epoch_state = EpochState::from_u8(state_raw).unwrap_or_else(|| {
            tracing::error!(
                "record_arena_flushed: unknown epoch state {} for stream {} epoch {}, treating as Active",
                state_raw, stream_id, epoch,
            );
            EpochState::Active
        });

        if epoch != stored_epoch {
            tracing::warn!(
                "record_arena_flushed: epoch mismatch for stream {}: \
                 reported epoch={}, DB epoch={}, current state={:?} — skipping",
                stream_id,
                epoch.0,
                stored_epoch.0,
                epoch_state,
            );
            tx.commit()
                .await
                .context(DatabaseSnafu { message: "commit" })?;
            return Ok(());
        }

        match epoch_state {
            EpochState::Flushed => {
                // Either we just inserted a terminal row, or a racing caller already
                // finalized it. Either way: idempotent no-op.
                tracing::debug!(
                    "record_arena_flushed: stream {} epoch {} already Flushed, idempotent",
                    stream_id,
                    epoch.0,
                );
            }
            EpochState::Sealed => {
                // Normal in-order path: Sealed → Flushed.
                sqlx::query(
                    "UPDATE stream_epochs SET state = ?, flushed_at = NOW(3) \
                     WHERE stream_id = ? AND epoch = ? AND state = ?",
                )
                .bind(EpochState::Flushed.as_u8())
                .bind(stream_id.0)
                .bind(epoch.0 as i32)
                .bind(EpochState::Sealed.as_u8())
                .execute(&mut *tx)
                .await
                .context(DatabaseSnafu {
                    message: "update extent flushed (from Sealed)",
                })?;
                tracing::info!(
                    "record_arena_flushed: stream {} epoch {} Sealed→Flushed",
                    stream_id,
                    epoch.0,
                );
            }
            EpochState::Active => {
                // Out-of-order: flushed notification beat the seal notification.
                // Fold the transition into a single UPDATE: set state to Flushed,
                // adopt the authoritative end_offset from the flush notification
                // (the row's end_offset may be a placeholder or stale progress),
                // and backfill sealed_at if it was never written.
                sqlx::query(
                    "UPDATE stream_epochs SET state = ?, end_offset = ?, flushed_at = NOW(3), \
                        sealed_at = IFNULL(sealed_at, NOW(3)) \
                     WHERE stream_id = ? AND epoch = ? AND state = ?",
                )
                .bind(EpochState::Flushed.as_u8())
                .bind(end_offset as i64)
                .bind(stream_id.0)
                .bind(epoch.0 as i32)
                .bind(EpochState::Active.as_u8())
                .execute(&mut *tx)
                .await
                .context(DatabaseSnafu {
                    message: "update extent flushed (from Active)",
                })?;
                tracing::info!(
                    "record_arena_flushed: stream {} epoch {} Active→Flushed \
                     (end_offset={}) — out-of-order flush notification",
                    stream_id,
                    epoch.0,
                    end_offset,
                );
            }
            EpochState::Unspecified => {
                // Should be impossible — the INSERT IGNORE above can only land a
                // Flushed row, and SM never persists Unspecified. Treat as skip.
                tracing::warn!(
                    "record_arena_flushed: stream {} epoch {} in Unspecified state, \
                     cannot transition to Flushed",
                    stream_id,
                    epoch.0,
                );
            }
        }

        tx.commit()
            .await
            .context(DatabaseSnafu { message: "commit" })?;

        Ok(())
    }

    /// Reconcile extents reported by a surviving EN during crash recovery.
    ///
    /// For each epoch report, insert it if missing.
    /// Reports that are sealed get their end_offset set.
    pub async fn reconcile_epochs(
        &self,
        stream_id: StreamId,
        epoch: Epoch,
        epochs: &[(u64, u64, EpochState)],
    ) -> Result<(), StorageError> {
        let mut conn = self.pool.acquire().await.context(DatabaseSnafu {
            message: "acquire connection",
        })?;
        let mut tx = conn.begin().await.context(DatabaseSnafu {
            message: "begin transaction",
        })?;

        for (start_offset, end_offset, state) in epochs {
            let db_state = if *state == EpochState::Active {
                EpochState::Active
            } else {
                EpochState::Sealed
            };

            // Insert if not exists; if exists and sealed, update end_offset.
            sqlx::query(
                "INSERT INTO stream_epochs (stream_id, epoch, start_offset, end_offset, state) \
                 VALUES (?, ?, ?, ?, ?) \
                 ON DUPLICATE KEY UPDATE \
                   end_offset = IF(state = 1 AND VALUES(state) = 2, VALUES(end_offset), end_offset), \
                   state = IF(state = 1 AND VALUES(state) = 2, VALUES(state), state), \
                   sealed_at = IF(state = 1 AND VALUES(state) = 2, NOW(), sealed_at)",
            )
            .bind(stream_id.0)
            .bind(epoch.0 as i32)
            .bind(*start_offset as i64)
            .bind(*end_offset as i64)
            .bind(db_state.as_u8())
            .execute(&mut *tx)
            .await
            .context(DatabaseSnafu { message: "reconcile extent" })?;
        }

        tx.commit()
            .await
            .context(DatabaseSnafu { message: "commit" })?;

        Ok(())
    }
}
