use std::collections::HashMap;
use std::time::Duration;

use common::errors::{DatabaseSnafu, EpochStaleSnafu, InternalSnafu, MigrationSnafu, StorageError};
use common::types::{
    Epoch, ExtentId, ExtentInfo, ExtentPolicy, ExtentState, NodeMetrics, NodeState, ReplicaDetail,
    StorageClass, StreamId,
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
    pub min_extent_capacity: u32,
    pub max_extent_capacity: u32,
    pub cache_extents: u16,
    pub extent_growth_factor: u8,
    pub storage_class: StorageClass,
}

/// A row from the `extent` table.
#[derive(Debug, Clone)]
pub struct ExtentRow {
    pub extent_id: ExtentId,
    pub stream_id: StreamId,
    pub start_offset: u64,
    pub end_offset: u64,
    pub state: ExtentState,
    pub epoch: Epoch,
}

/// A sealed extent that has been stuck past the staleness threshold.
#[derive(Debug, Clone)]
pub struct StaleExtentRow {
    pub stream_id: StreamId,
    pub extent_id: ExtentId,
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
///
/// Both variants carry the epoch the stream is left at, so callers never have to
/// re-read it: `Sealed` advanced the stream, `AlreadySealed` left it untouched.
#[derive(Debug, Clone)]
pub enum SealResult {
    /// The extent was active and has been sealed; the stream epoch was advanced
    /// and a new extent was allocated at that epoch.
    ///
    /// `new_start_offset` is the offset the successor was persisted with. It is
    /// usually the sealed extent's end offset, but can be further along when a
    /// sealed successor chain already reached past it, so callers must register
    /// the extent with this value rather than recomputing it.
    Sealed {
        new_extent_id: ExtentId,
        new_start_offset: u64,
        new_epoch: Epoch,
    },
    /// The extent was already sealed and a successor exists at the current epoch
    /// (an Extent Node created it autonomously). The epoch is left unchanged.
    AlreadySealed {
        new_extent_id: ExtentId,
        new_start_offset: u64,
        primary_addr: String,
        epoch: Epoch,
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

/// Pool size for a Stream Manager process.
const DEFAULT_MAX_CONNECTIONS: u32 = 10;

/// How long an unused connection is kept before it is closed. Pools that only
/// see occasional traffic shrink back instead of pinning a server slot for the
/// life of the process.
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Hard cap on connection age. Bounds how long a connection can hold
/// server-side state (locks from an abandoned transaction, for instance) when
/// the peer goes away without a clean close.
const POOL_MAX_LIFETIME: Duration = Duration::from_secs(600);

impl MetadataStore {
    /// Connect to MySQL and return a MetadataStore.
    pub async fn connect(url: &str) -> Result<Self, StorageError> {
        Self::connect_with_max_connections(url, DEFAULT_MAX_CONNECTIONS).await
    }

    /// Connect with an explicit pool size.
    ///
    /// Tests run many stores against one shared MySQL, where the default pool
    /// size per store can exhaust `max_connections` on the server.
    pub async fn connect_with_max_connections(
        url: &str,
        max_connections: u32,
    ) -> Result<Self, StorageError> {
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
            .max_connections(max_connections)
            .idle_timeout(POOL_IDLE_TIMEOUT)
            .max_lifetime(POOL_MAX_LIFETIME)
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
    /// Also initializes the stream_sequence row for per-stream extent_id generation.
    pub async fn create_stream(
        &self,
        name: &str,
        replication_factor: u8,
        storage_class: StorageClass,
        policy: ExtentPolicy,
    ) -> Result<StreamId, StorageError> {
        let result = sqlx::query(
            "INSERT INTO stream (stream_name, replication_factor, min_extent_capacity, max_extent_capacity, cache_extents, extent_growth_factor, storage_class) VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(name)
        .bind(replication_factor)
        .bind(policy.min_capacity as i32)
        .bind(policy.max_capacity as i32)
        .bind(policy.cache)
        .bind(policy.scale_factor)
        .bind(storage_class.as_u8())
        .execute(&self.pool)
        .await
        .context(DatabaseSnafu { message: "create_stream" })?;

        let stream_id = StreamId(result.last_insert_id() as u32);

        // Initialize stream_sequence for per-stream extent_id generation.
        sqlx::query("INSERT INTO stream_sequence (stream_id, next_extent_id) VALUES (?, 0)")
            .bind(stream_id.0)
            .execute(&self.pool)
            .await
            .context(DatabaseSnafu {
                message: "init stream_sequence",
            })?;

        Ok(stream_id)
    }

    /// Get a stream by ID.
    pub async fn get_stream(&self, id: StreamId) -> Result<Option<StreamRow>, StorageError> {
        let row = sqlx::query(
            "SELECT stream_id, stream_name, replication_factor, min_extent_capacity, max_extent_capacity, cache_extents, extent_growth_factor, storage_class FROM stream WHERE stream_id = ?",
        )
        .bind(id.0 as i64)
        .fetch_optional(&self.pool)
        .await
        .context(DatabaseSnafu { message: "get_stream" })?;

        Ok(row.map(|r| StreamRow {
            stream_id: StreamId(r.get::<u32, _>("stream_id")),
            stream_name: r.get("stream_name"),
            replication_factor: r.get::<u8, _>("replication_factor"),
            min_extent_capacity: r.get::<i32, _>("min_extent_capacity") as u32,
            max_extent_capacity: r.get::<i32, _>("max_extent_capacity") as u32,
            cache_extents: r.get::<u16, _>("cache_extents"),
            extent_growth_factor: r.get::<u8, _>("extent_growth_factor"),
            storage_class: StorageClass::from_u8(r.get::<u8, _>("storage_class"))
                .unwrap_or(StorageClass::S3),
        }))
    }

    /// Get a stream by name.
    pub async fn get_stream_by_name(&self, name: &str) -> Result<Option<StreamRow>, StorageError> {
        let row = sqlx::query(
            "SELECT stream_id, stream_name, replication_factor, min_extent_capacity, max_extent_capacity, cache_extents, extent_growth_factor, storage_class FROM stream WHERE stream_name = ?",
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await
        .context(DatabaseSnafu { message: "get_stream_by_name" })?;

        Ok(row.map(|r| StreamRow {
            stream_id: StreamId(r.get::<u32, _>("stream_id")),
            stream_name: r.get("stream_name"),
            replication_factor: r.get::<u8, _>("replication_factor"),
            min_extent_capacity: r.get::<i32, _>("min_extent_capacity") as u32,
            max_extent_capacity: r.get::<i32, _>("max_extent_capacity") as u32,
            cache_extents: r.get::<u16, _>("cache_extents"),
            extent_growth_factor: r.get::<u8, _>("extent_growth_factor"),
            storage_class: StorageClass::from_u8(r.get::<u8, _>("storage_class"))
                .unwrap_or(StorageClass::S3),
        }))
    }

    /// Get all streams that have at least one active extent.
    /// Used during SM startup reconciliation to discover streams that may have
    /// extents created autonomously by ENs during SM downtime.
    pub async fn get_streams_with_active_extents(
        &self,
    ) -> Result<Vec<(StreamId, Epoch)>, StorageError> {
        let rows = sqlx::query(
            "SELECT DISTINCT e.stream_id, s.epoch \
             FROM extent e \
             INNER JOIN stream s ON e.stream_id = s.stream_id \
             WHERE e.state = ?",
        )
        .bind(ExtentState::Active.as_u8())
        .fetch_all(&self.pool)
        .await
        .context(DatabaseSnafu {
            message: "get_streams_with_active_extents",
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

    /// Get the cache_extents (max extents to retain in memory) for a stream.
    pub async fn get_stream_cache_extents(&self, stream_id: StreamId) -> Result<u16, StorageError> {
        let row = sqlx::query("SELECT cache_extents FROM stream WHERE stream_id = ?")
            .bind(stream_id.0)
            .fetch_one(&self.pool)
            .await
            .context(DatabaseSnafu {
                message: "get_stream_cache_extents",
            })?;

        Ok(row.get::<u16, _>("cache_extents"))
    }

    /// Get the min and max extent capacity bounds for a stream.
    pub async fn get_stream_capacity_bounds(
        &self,
        stream_id: StreamId,
    ) -> Result<(u32, u32), StorageError> {
        let row = sqlx::query(
            "SELECT min_extent_capacity, max_extent_capacity FROM stream WHERE stream_id = ?",
        )
        .bind(stream_id.0)
        .fetch_one(&self.pool)
        .await
        .context(DatabaseSnafu {
            message: "get_stream_capacity_bounds",
        })?;

        let min = row.get::<i32, _>("min_extent_capacity") as u32;
        let max = row.get::<i32, _>("max_extent_capacity") as u32;
        Ok((min, max))
    }

    /// Get the extent growth factor for a stream.
    pub async fn get_stream_growth_factor(&self, stream_id: StreamId) -> Result<u8, StorageError> {
        let row = sqlx::query("SELECT extent_growth_factor FROM stream WHERE stream_id = ?")
            .bind(stream_id.0)
            .fetch_one(&self.pool)
            .await
            .context(DatabaseSnafu {
                message: "get_stream_growth_factor",
            })?;

        Ok(row.get::<u8, _>("extent_growth_factor"))
    }

    /// Get the minimum extent capacity for a stream.
    pub async fn get_stream_min_capacity(&self, stream_id: StreamId) -> Result<u32, StorageError> {
        let row = sqlx::query("SELECT min_extent_capacity FROM stream WHERE stream_id = ?")
            .bind(stream_id.0)
            .fetch_one(&self.pool)
            .await
            .context(DatabaseSnafu {
                message: "get_stream_min_capacity",
            })?;

        Ok(row.get::<i32, _>("min_extent_capacity") as u32)
    }

    /// Get the maximum extent capacity for a stream.
    pub async fn get_stream_max_capacity(&self, stream_id: StreamId) -> Result<u32, StorageError> {
        let row = sqlx::query("SELECT max_extent_capacity FROM stream WHERE stream_id = ?")
            .bind(stream_id.0)
            .fetch_one(&self.pool)
            .await
            .context(DatabaseSnafu {
                message: "get_stream_max_capacity",
            })?;

        Ok(row.get::<i32, _>("max_extent_capacity") as u32)
    }

    // ── Extent operations ──

    /// Allocate a new extent for a stream on a replica set. Returns ExtentId.
    ///
    /// `nodes` is ordered: [(primary_addr, 0), (secondary_1_addr, 1), ...].
    /// This method runs in a single MySQL transaction to prevent race conditions:
    /// 1. Locks the stream_sequence row with SELECT ... FOR UPDATE.
    /// 2. Increments next_extent_id and reads the new value atomically.
    /// 3. Inserts the extent row.
    /// 4. Inserts stream_replica rows for the (stream, epoch) if not already present.
    pub async fn allocate_extent(
        &self,
        stream_id: StreamId,
        start_offset: u64,
        nodes: &[(String, u8)],
        epoch: Epoch,
    ) -> Result<ExtentId, StorageError> {
        if nodes.is_empty() {
            return InternalSnafu {
                message: "allocate_extent: empty node list",
            }
            .fail();
        }

        let mut conn = self.pool.acquire().await.context(DatabaseSnafu {
            message: "acquire connection",
        })?;
        let mut tx = conn.begin().await.context(DatabaseSnafu {
            message: "begin transaction",
        })?;

        // Step 1: Lock and increment stream_sequence atomically within the transaction.
        sqlx::query("SELECT next_extent_id FROM stream_sequence WHERE stream_id = ? FOR UPDATE")
            .bind(stream_id.0)
            .fetch_one(&mut *tx)
            .await
            .context(DatabaseSnafu {
                message: "lock stream_sequence",
            })?;

        sqlx::query(
            "UPDATE stream_sequence SET next_extent_id = next_extent_id + 1 WHERE stream_id = ?",
        )
        .bind(stream_id.0)
        .execute(&mut *tx)
        .await
        .context(DatabaseSnafu {
            message: "increment stream_sequence",
        })?;

        let row = sqlx::query("SELECT next_extent_id FROM stream_sequence WHERE stream_id = ?")
            .bind(stream_id.0)
            .fetch_one(&mut *tx)
            .await
            .context(DatabaseSnafu {
                message: "read stream_sequence",
            })?;

        // next_extent_id was already incremented, so the allocated ID is next_extent_id (post-increment value).
        let extent_id = ExtentId(row.get::<i64, _>("next_extent_id") as u32);

        // Step 2: Insert extent row.
        sqlx::query(
            "INSERT INTO extent (stream_id, extent_id, start_offset, end_offset, state, epoch) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(stream_id.0)
        .bind(extent_id.0 as i64)
        .bind(start_offset as i64)
        .bind(start_offset as i64) // end_offset = start_offset for new active extent
        .bind(ExtentState::Active.as_u8())
        .bind(epoch.0 as i32)
        .execute(&mut *tx)
        .await
        .context(DatabaseSnafu { message: "insert extent" })?;

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

        Ok(extent_id)
    }

    /// Seal an extent, advance the stream epoch, and allocate the successor
    /// extent and its replica set in a single MySQL transaction.
    ///
    /// `expected_epoch` is the epoch carried by the request being served. The
    /// transaction locks the stream row and refuses to proceed unless the
    /// persisted epoch still matches, so concurrent transitions cannot perform
    /// consecutive bumps and a failed allocation can never leave the stream
    /// epoch ahead of its active extent.
    ///
    /// - If the extent is Active: seal it, bump the epoch, allocate a new extent
    ///   and replica rows at the new epoch, return `Sealed`.
    /// - If the extent is no longer Active (sealed or already flushed) and an
    ///   active successor exists: return `AlreadySealed` without touching the
    ///   epoch, keeping the stream epoch and the active extent's epoch equal.
    pub async fn seal_and_allocate_transaction(
        &self,
        stream_id: StreamId,
        extent_id: ExtentId,
        end_offset: u64,
        nodes: &[(String, u8)],
        expected_epoch: Epoch,
    ) -> Result<SealResult, StorageError> {
        let mut conn = self.pool.acquire().await.context(DatabaseSnafu {
            message: "acquire connection",
        })?;
        let mut tx = conn.begin().await.context(DatabaseSnafu {
            message: "begin transaction",
        })?;

        // Step 0: Lock the stream row and fence the request epoch. Taking this
        // lock first serializes epoch transitions for the stream, so a losing
        // request observes the winner's epoch instead of racing past it.
        let stream_row = sqlx::query("SELECT epoch FROM stream WHERE stream_id = ? FOR UPDATE")
            .bind(stream_id.0)
            .fetch_optional(&mut *tx)
            .await
            .context(DatabaseSnafu {
                message: "lock stream",
            })?;

        let current_epoch = match stream_row {
            Some(row) => Epoch(row.get::<i32, _>("epoch") as u32),
            None => {
                return InternalSnafu {
                    message: format!("stream not found: {stream_id}"),
                }
                .fail();
            }
        };

        if current_epoch != expected_epoch {
            return EpochStaleSnafu {
                stream_id,
                epoch: expected_epoch,
            }
            .fail();
        }

        let new_epoch = Epoch(expected_epoch.0 + 1);

        // Step 1: Lock the target extent row and check state.
        let row = sqlx::query(
            "SELECT state, start_offset \
             FROM extent WHERE stream_id = ? AND extent_id = ? FOR UPDATE",
        )
        .bind(stream_id.0)
        .bind(extent_id.0 as i64)
        .fetch_optional(&mut *tx)
        .await
        .context(DatabaseSnafu {
            message: "lock extent",
        })?;

        let row = row.ok_or_else(|| {
            InternalSnafu {
                message: format!(
                    "extent not found: stream={}, extent={}",
                    stream_id, extent_id
                ),
            }
            .build()
        })?;

        let state_val = row.get::<i8, _>("state") as u8;
        let state = ExtentState::from_u8(state_val).unwrap_or(ExtentState::Unspecified);

        // Offset the successor extent must start at. Normally this is the end of
        // the extent being sealed, but an already-sealed extent may sit in front
        // of a sealed successor chain that reaches further.
        let mut start_floor = end_offset;

        // Anything that is not Active has already been sealed — by an autonomous
        // Extent Node transition, by reconciliation, or by an earlier attempt of
        // this same request. `Flushed` counts: it is a sealed extent that has
        // since reached S3. Treating only `Sealed` as sealed would let a flushed
        // predecessor fall through to allocation and produce a second active
        // extent alongside the successor the Extent Node already created.
        if state != ExtentState::Active {
            // A sealed extent's recorded boundary can lag what this request
            // learned, because an out-of-order notification may have installed
            // the row with a placeholder offset. Advance it, but never past the
            // point where the next extent already begins: a delayed retry
            // reporting a further offset would otherwise stretch the predecessor
            // across its successor and make the same offsets readable from two
            // extents.
            //
            // A Flushed extent is left alone. Its bytes are already in S3, so
            // advertising a boundary beyond what was uploaded would point
            // readers at offsets that do not exist in the object.
            if state == ExtentState::Sealed {
                let successor_start: Option<i64> = sqlx::query_scalar(
                    "SELECT MIN(start_offset) FROM extent WHERE stream_id = ? AND extent_id > ?",
                )
                .bind(stream_id.0)
                .bind(extent_id.0 as i64)
                .fetch_one(&mut *tx)
                .await
                .context(DatabaseSnafu {
                    message: "read successor start_offset",
                })?;

                let bounded_end = match successor_start {
                    Some(start) => end_offset.min(start as u64),
                    None => end_offset,
                };

                sqlx::query(
                    "UPDATE extent SET end_offset = ?, sealed_at = COALESCE(sealed_at, NOW()) \
                     WHERE stream_id = ? AND extent_id = ? AND end_offset < ?",
                )
                .bind(bounded_end as i64)
                .bind(stream_id.0)
                .bind(extent_id.0 as i64)
                .bind(bounded_end as i64)
                .execute(&mut *tx)
                .await
                .context(DatabaseSnafu {
                    message: "advance sealed end_offset",
                })?;
            }

            // The caller wants the extent that is now taking writes, which is the
            // *active* successor at the stream's own epoch. A sealed successor is
            // not a usable answer — the caller would hand it to a client as the
            // new append target — and neither is an active row left behind at
            // some other epoch, which clients can no longer append to.
            //
            // Exactly one row should match. Two can exist in metadata written
            // before the seal transaction became atomic, so take the lowest
            // extent id (the one that continues the offset chain) and record the
            // rest rather than picking arbitrarily.
            let successors = sqlx::query(
                "SELECT extent_id, start_offset FROM extent \
                 WHERE stream_id = ? AND extent_id > ? AND state = ? AND epoch = ? \
                 ORDER BY extent_id ASC LIMIT 2",
            )
            .bind(stream_id.0)
            .bind(extent_id.0 as i64)
            .bind(ExtentState::Active.as_u8())
            .bind(current_epoch.0 as i32)
            .fetch_all(&mut *tx)
            .await
            .context(DatabaseSnafu {
                message: "find successor",
            })?;

            if successors.len() > 1 {
                tracing::warn!(
                    "stream {stream_id} has more than one active extent at epoch {current_epoch}; \
                     using the lowest as the successor",
                );
            }

            if let Some(successor) = successors.first() {
                let new_extent_id = ExtentId(successor.get::<i64, _>("extent_id") as u32);
                let new_start_offset = successor.get::<i64, _>("start_offset") as u64;

                // Get primary replica address from the stream-level replica set.
                let replica = sqlx::query(
                    "SELECT node_addr FROM stream_replica \
                     WHERE stream_id = ? AND epoch = ? AND role = 0",
                )
                .bind(stream_id.0)
                .bind(current_epoch.0 as i32)
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
                    new_extent_id,
                    new_start_offset,
                    primary_addr,
                    epoch: current_epoch,
                });
            }

            // No successor is taking writes — fall through and allocate one,
            // starting past the furthest sealed boundary so the new extent
            // cannot overlap a sealed successor.
            let max_end: Option<i64> =
                sqlx::query_scalar("SELECT MAX(end_offset) FROM extent WHERE stream_id = ?")
                    .bind(stream_id.0)
                    .fetch_one(&mut *tx)
                    .await
                    .context(DatabaseSnafu {
                        message: "read max end_offset",
                    })?;
            start_floor = start_floor.max(max_end.unwrap_or(0) as u64);
        }

        // Step 2: Seal the active extent (idempotent if already sealed).
        sqlx::query(
            "UPDATE extent SET state = ?, end_offset = ?, sealed_at = NOW() \
             WHERE stream_id = ? AND extent_id = ? AND state = ?",
        )
        .bind(ExtentState::Sealed.as_u8())
        .bind(end_offset as i64)
        .bind(stream_id.0)
        .bind(extent_id.0 as i64)
        .bind(ExtentState::Active.as_u8())
        .execute(&mut *tx)
        .await
        .context(DatabaseSnafu {
            message: "seal extent",
        })?;

        // Step 3: Allocate new extent_id.
        sqlx::query(
            "UPDATE stream_sequence SET next_extent_id = next_extent_id + 1 WHERE stream_id = ?",
        )
        .bind(stream_id.0)
        .execute(&mut *tx)
        .await
        .context(DatabaseSnafu {
            message: "increment stream_sequence",
        })?;

        let seq_row = sqlx::query("SELECT next_extent_id FROM stream_sequence WHERE stream_id = ?")
            .bind(stream_id.0)
            .fetch_one(&mut *tx)
            .await
            .context(DatabaseSnafu {
                message: "read stream_sequence",
            })?;

        let new_extent_id = ExtentId(seq_row.get::<i64, _>("next_extent_id") as u32);
        let new_start_offset = start_floor;

        // Step 4: Insert new extent row.
        sqlx::query(
            "INSERT INTO extent (stream_id, extent_id, start_offset, end_offset, state, epoch) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(stream_id.0)
        .bind(new_extent_id.0 as i64)
        .bind(new_start_offset as i64)
        .bind(new_start_offset as i64) // end_offset = start_offset for new active extent
        .bind(ExtentState::Active.as_u8())
        .bind(new_epoch.0 as i32)
        .execute(&mut *tx)
        .await
        .context(DatabaseSnafu { message: "insert extent" })?;

        // Step 5: Insert stream_replica rows for this (stream, epoch).
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

        // Step 6: Advance the stream epoch. This commits with the seal, the
        // successor extent, and the replica rows, so the stream epoch and its
        // active extent's epoch can never diverge.
        sqlx::query("UPDATE stream SET epoch = ? WHERE stream_id = ?")
            .bind(new_epoch.0 as i32)
            .bind(stream_id.0)
            .execute(&mut *tx)
            .await
            .context(DatabaseSnafu {
                message: "advance stream epoch",
            })?;

        tx.commit()
            .await
            .context(DatabaseSnafu { message: "commit" })?;

        Ok(SealResult::Sealed {
            new_extent_id,
            new_start_offset,
            new_epoch,
        })
    }

    /// Get the active extent for a stream (there should be at most one).
    pub async fn get_active_extent(
        &self,
        stream_id: StreamId,
    ) -> Result<Option<ExtentRow>, StorageError> {
        let row = sqlx::query(
            "SELECT extent_id, stream_id, start_offset, end_offset, state, epoch \
             FROM extent WHERE stream_id = ? AND state = ? LIMIT 1",
        )
        .bind(stream_id.0)
        .bind(ExtentState::Active.as_u8())
        .fetch_optional(&self.pool)
        .await
        .context(DatabaseSnafu {
            message: "get_active_extent",
        })?;

        Ok(row.map(Self::map_extent_row))
    }

    /// Get all extents for a stream, ordered by extent_id.
    pub async fn get_extents(&self, stream_id: StreamId) -> Result<Vec<ExtentRow>, StorageError> {
        let rows = sqlx::query(
            "SELECT extent_id, stream_id, start_offset, end_offset, state, epoch \
             FROM extent WHERE stream_id = ? ORDER BY extent_id",
        )
        .bind(stream_id.0)
        .fetch_all(&self.pool)
        .await
        .context(DatabaseSnafu {
            message: "get_extents",
        })?;

        Ok(rows.into_iter().map(Self::map_extent_row).collect())
    }

    /// Get all active extents that have a replica on the given node address.
    pub async fn get_active_extents_on_node(
        &self,
        node_addr: &str,
    ) -> Result<Vec<ExtentRow>, StorageError> {
        let rows = sqlx::query(
            "SELECT e.extent_id, e.stream_id, e.start_offset, e.end_offset, e.state, e.epoch \
             FROM extent e \
             INNER JOIN stream_replica r ON e.stream_id = r.stream_id AND e.epoch = r.epoch \
             WHERE r.node_addr = ? AND e.state = ?",
        )
        .bind(node_addr)
        .bind(ExtentState::Active.as_u8())
        .fetch_all(&self.pool)
        .await
        .context(DatabaseSnafu {
            message: "get_active_extents_on_node",
        })?;

        Ok(rows.into_iter().map(Self::map_extent_row).collect())
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

    /// Map a sqlx Row to an ExtentRow.
    fn map_extent_row(r: sqlx::mysql::MySqlRow) -> ExtentRow {
        let state_val = r.get::<i8, _>("state") as u8;
        ExtentRow {
            extent_id: ExtentId(r.get::<i64, _>("extent_id") as u32),
            stream_id: StreamId(r.get::<u32, _>("stream_id")),
            start_offset: r.get::<i64, _>("start_offset") as u64,
            end_offset: r.get::<i64, _>("end_offset") as u64,
            state: ExtentState::from_u8(state_val).unwrap_or(ExtentState::Unspecified),
            epoch: Epoch(r.get::<i32, _>("epoch") as u32),
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
                "SELECT extent_id, stream_id, start_offset, end_offset, state, epoch \
                 FROM extent WHERE stream_id = ? ORDER BY extent_id DESC",
            )
            .bind(stream_id.0)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query(
                "SELECT extent_id, stream_id, start_offset, end_offset, state, epoch \
                 FROM extent WHERE stream_id = ? ORDER BY extent_id DESC LIMIT ?",
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
            let ext = Self::map_extent_row(row);
            let replicas = self
                .get_replicas_with_liveness(stream_id, ext.epoch)
                .await?;
            result.push(ExtentInfo {
                extent_id: ext.extent_id.0,
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
    pub async fn describe_extent(
        &self,
        stream_id: StreamId,
        extent_id: ExtentId,
    ) -> Result<Option<ExtentInfo>, StorageError> {
        let row = sqlx::query(
            "SELECT extent_id, stream_id, start_offset, end_offset, state, epoch \
             FROM extent WHERE stream_id = ? AND extent_id = ?",
        )
        .bind(stream_id.0)
        .bind(extent_id.0 as i64)
        .fetch_optional(&self.pool)
        .await
        .context(DatabaseSnafu {
            message: "describe_extent",
        })?;

        match row {
            None => Ok(None),
            Some(r) => {
                let ext = Self::map_extent_row(r);
                let replicas = self
                    .get_replicas_with_liveness(stream_id, ext.epoch)
                    .await?;
                Ok(Some(ExtentInfo {
                    extent_id: ext.extent_id.0,
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
    ) -> Result<Option<ExtentInfo>, StorageError> {
        // Try sealed/flushed extents first: start_offset <= offset < end_offset.
        let row = sqlx::query(
            "SELECT extent_id, stream_id, start_offset, end_offset, state, epoch \
             FROM extent \
             WHERE stream_id = ? AND state IN (?, ?) \
               AND start_offset <= ? AND ? < end_offset \
             ORDER BY extent_id ASC LIMIT 1",
        )
        .bind(stream_id.0)
        .bind(ExtentState::Sealed.as_u8())
        .bind(ExtentState::Flushed.as_u8())
        .bind(offset as i64)
        .bind(offset as i64)
        .fetch_optional(&self.pool)
        .await
        .context(DatabaseSnafu {
            message: "seek_extent (sealed)",
        })?;

        if let Some(r) = row {
            let ext = Self::map_extent_row(r);
            let replicas = self
                .get_replicas_with_liveness(stream_id, ext.epoch)
                .await?;
            return Ok(Some(ExtentInfo {
                extent_id: ext.extent_id.0,
                start_offset: ext.start_offset,
                end_offset: ext.end_offset,
                epoch: ext.epoch,
                state: ext.state,
                replicas,
            }));
        }

        // Fall back to the Active extent where start_offset <= offset.
        let row = sqlx::query(
            "SELECT extent_id, stream_id, start_offset, end_offset, state, epoch \
             FROM extent \
             WHERE stream_id = ? AND state = ? AND start_offset <= ? \
             ORDER BY extent_id DESC LIMIT 1",
        )
        .bind(stream_id.0)
        .bind(ExtentState::Active.as_u8())
        .bind(offset as i64)
        .fetch_optional(&self.pool)
        .await
        .context(DatabaseSnafu {
            message: "seek_extent (active)",
        })?;

        match row {
            None => Ok(None),
            Some(r) => {
                let ext = Self::map_extent_row(r);
                let replicas = self
                    .get_replicas_with_liveness(stream_id, ext.epoch)
                    .await?;
                Ok(Some(ExtentInfo {
                    extent_id: ext.extent_id.0,
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
            "SELECT e.stream_id, e.extent_id, e.epoch, e.start_offset, e.end_offset \
             FROM extent e \
             JOIN stream s ON e.stream_id = s.stream_id \
             WHERE e.state = ? \
               AND e.sealed_at IS NOT NULL \
               AND e.sealed_at < NOW() - INTERVAL ? SECOND \
               AND s.storage_class = 0 \
               AND s.replication_factor > 1 \
             ORDER BY e.sealed_at ASC",
        )
        .bind(ExtentState::Sealed.as_u8())
        .bind(threshold_secs as i64)
        .fetch_all(&self.pool)
        .await
        .context(DatabaseSnafu {
            message: "get_stale_sealed_extents",
        })?;

        Ok(rows
            .iter()
            .map(|row| StaleExtentRow {
                stream_id: StreamId(row.get::<u32, _>("stream_id")),
                extent_id: ExtentId(row.get::<i64, _>("extent_id") as u32),
                epoch: Epoch(row.get::<i32, _>("epoch") as u32),
                start_offset: row.get::<i64, _>("start_offset") as u64,
                end_offset: row.get::<i64, _>("end_offset") as u64,
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

    // The stream epoch is advanced only by `seal_and_allocate_transaction`, which
    // commits the bump together with the seal, the successor extent, and the new
    // replica set. A standalone bump is deliberately not offered: committing it
    // separately can leave the stream epoch ahead of its active extent, which no
    // later request can repair.

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

    /// Record an extent sealed notification from a Primary EN (autonomous extent creation).
    ///
    /// Handles out-of-order notifications gracefully:
    /// 1. Insert the sealed extent if it doesn't exist yet (out-of-order case).
    /// 2. Seal it (idempotent — only updates Active extents).
    /// 3. Insert the new active extent (idempotent — INSERT IGNORE).
    /// 4. Fix start_offset of the new extent if it was inserted out-of-order
    ///    with a placeholder (start_offset=0 from a later notification).
    /// 5. Update stream_sequence.
    ///
    /// No replica manipulation needed — replicas are stored at (stream_id, epoch)
    /// level and all extents within an epoch inherit the same replica set.
    ///
    /// Safe against a racing `record_extent_flushed`: if the flushed notification
    /// arrived first and installed a row in state `Flushed`, the `INSERT IGNORE`
    /// below is a no-op and the `UPDATE ... WHERE state = Active` also matches zero
    /// rows — state is not regressed.
    ///
    /// Safe against a racing `record_extent_flushed`: if the flushed notification
    /// arrived first and installed a row in state `Flushed`, the `INSERT IGNORE`
    /// below is a no-op and the `UPDATE ... WHERE state = Active` also matches zero
    /// rows — state is not regressed.
    pub async fn record_extent_sealed(
        &self,
        stream_id: StreamId,
        epoch: Epoch,
        sealed_extent_id: ExtentId,
        end_offset: u64,
        new_extent_id: ExtentId,
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

        // Insert sealed extent if not yet known (handles out-of-order arrival).
        sqlx::query(
            "INSERT IGNORE INTO extent (stream_id, extent_id, start_offset, end_offset, state, epoch) \
             VALUES (?, ?, 0, ?, ?, ?)",
        )
        .bind(stream_id.0)
        .bind(sealed_extent_id.0 as i64)
        .bind(end_offset as i64)
        .bind(ExtentState::Sealed.as_u8())
        .bind(epoch.0 as i32)
        .execute(&mut *tx)
        .await
        .context(DatabaseSnafu { message: "insert sealed extent" })?;

        // Seal it (idempotent — only updates if currently Active).
        sqlx::query(
            "UPDATE extent SET state = ?, end_offset = ?, sealed_at = NOW() \
             WHERE stream_id = ? AND extent_id = ? AND state = ?",
        )
        .bind(ExtentState::Sealed.as_u8())
        .bind(end_offset as i64)
        .bind(stream_id.0)
        .bind(sealed_extent_id.0 as i64)
        .bind(ExtentState::Active.as_u8())
        .execute(&mut *tx)
        .await
        .context(DatabaseSnafu {
            message: "seal extent",
        })?;

        // Insert new active extent (idempotent — ignore duplicate).
        sqlx::query(
            "INSERT IGNORE INTO extent (stream_id, extent_id, start_offset, end_offset, state, epoch) \
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(stream_id.0)
        .bind(new_extent_id.0 as i64)
        .bind(end_offset as i64)
        .bind(end_offset as i64)
        .bind(ExtentState::Active.as_u8())
        .bind(epoch.0 as i32)
        .execute(&mut *tx)
        .await
        .context(DatabaseSnafu { message: "insert new extent" })?;

        // Fix start_offset on the sealed extent if it was inserted out-of-order
        // with a placeholder (start_offset=0). Also fix start_offset on the new
        // extent if a later notification already inserted it with start_offset=0.
        sqlx::query(
            "UPDATE extent SET start_offset = ? \
             WHERE stream_id = ? AND extent_id = ? AND start_offset = 0 AND ? > 0",
        )
        .bind(end_offset as i64)
        .bind(stream_id.0)
        .bind(new_extent_id.0 as i64)
        .bind(end_offset as i64)
        .execute(&mut *tx)
        .await
        .context(DatabaseSnafu {
            message: "fix new extent start_offset",
        })?;

        // No replica manipulation — replicas are stored at (stream_id, epoch)
        // level and all extents within an epoch inherit the same replica set.

        // Update stream_sequence to be at least new_extent_id + 1.
        sqlx::query(
            "UPDATE stream_sequence SET next_extent_id = GREATEST(next_extent_id, ?) WHERE stream_id = ?",
        )
        .bind((new_extent_id.0 + 1) as i64)
        .bind(stream_id.0)
        .execute(&mut *tx)
        .await
        .context(DatabaseSnafu { message: "update stream_sequence" })?;

        tx.commit()
            .await
            .context(DatabaseSnafu { message: "commit" })?;

        Ok(())
    }

    /// Record a progress update for an active extent (periodic observability report).
    ///
    /// Updates end_offset for the extent if the reported offset is larger than
    /// the current value. Only updates Active extents. Idempotent and epoch-validated.
    pub async fn record_extent_progress(
        &self,
        stream_id: StreamId,
        epoch: Epoch,
        extent_id: ExtentId,
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
            "UPDATE extent SET end_offset = ? \
             WHERE stream_id = ? AND extent_id = ? AND state = ? AND end_offset < ?",
        )
        .bind(current_offset as i64)
        .bind(stream_id.0)
        .bind(extent_id.0 as i64)
        .bind(ExtentState::Active.as_u8())
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
    /// not yet exist when this notification arrives, because the preceding
    /// `UpdateExtentSealed` is an independent fire-and-forget frame that may be
    /// reordered, dropped on a full channel, or lost across an SM reconnect/failover.
    /// The handler therefore accepts any starting row state:
    ///
    /// - **row missing**  → `INSERT` a fresh row directly in state `Flushed` with the
    ///   offsets from the notification. A later `record_extent_sealed` for the same
    ///   extent is a no-op (its `INSERT IGNORE` finds the row; its
    ///   `UPDATE ... WHERE state = Active` matches zero rows).
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
    pub async fn record_extent_flushed(
        &self,
        stream_id: StreamId,
        epoch: Epoch,
        extent_id: ExtentId,
        start_offset: u64,
        end_offset: u64,
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
        let stream_exists = sqlx::query("SELECT 1 FROM stream WHERE stream_id = ? FOR UPDATE")
            .bind(stream_id.0)
            .fetch_optional(&mut *tx)
            .await
            .context(DatabaseSnafu {
                message: "lock stream",
            })?;

        if stream_exists.is_none() {
            return InternalSnafu {
                message: format!("stream {:?} not found", stream_id),
            }
            .fail();
        }

        // Seed the row if it doesn't yet exist. INSERT IGNORE leaves an existing
        // row alone — the next query (SELECT FOR UPDATE) picks it up and the match
        // below handles it. If the INSERT takes, the row is already terminal
        // (Flushed) with correct offsets; the subsequent SELECT will observe state
        // = Flushed and hit the idempotent no-op branch.
        sqlx::query(
            "INSERT IGNORE INTO extent \
                 (stream_id, extent_id, start_offset, end_offset, state, epoch, \
                  sealed_at, flushed_at) \
             VALUES (?, ?, ?, ?, ?, ?, NOW(3), NOW(3))",
        )
        .bind(stream_id.0)
        .bind(extent_id.0 as i64)
        .bind(start_offset as i64)
        .bind(end_offset as i64)
        .bind(ExtentState::Flushed.as_u8())
        .bind(epoch.0 as i32)
        .execute(&mut *tx)
        .await
        .context(DatabaseSnafu {
            message: "insert flushed extent",
        })?;

        // Re-read the row's authoritative state + epoch after the seeding insert.
        // If the row pre-existed, this returns its original state (Active / Sealed /
        // Flushed). If we just inserted it, this returns Flushed and we no-op.
        let row = sqlx::query(
            "SELECT epoch, state FROM extent WHERE stream_id = ? AND extent_id = ? FOR UPDATE",
        )
        .bind(stream_id.0)
        .bind(extent_id.0 as i64)
        .fetch_one(&mut *tx)
        .await
        .context(DatabaseSnafu {
            message: "lock extent",
        })?;

        let extent_epoch = Epoch(row.get::<i32, _>("epoch") as u32);
        let state_raw = row.get::<i8, _>("state") as u8;
        let extent_state = ExtentState::from_u8(state_raw).unwrap_or_else(|| {
            tracing::error!(
                "record_extent_flushed: unknown extent state {} for stream {} extent {}, treating as Active",
                state_raw, stream_id, extent_id,
            );
            ExtentState::Active
        });

        // Per-extent epoch sanity check: the row's stored epoch must match the
        // reporter's epoch. If an older row exists under the same extent_id but a
        // different creation epoch, skip — this is a stale notification from a
        // pre-failover replica set.
        if epoch != extent_epoch {
            tracing::warn!(
                "record_extent_flushed: extent-epoch mismatch for stream {} extent {}: \
                 reported epoch={}, DB epoch={}, current state={:?} — skipping",
                stream_id,
                extent_id,
                epoch.0,
                extent_epoch.0,
                extent_state,
            );
            tx.commit()
                .await
                .context(DatabaseSnafu { message: "commit" })?;
            return Ok(());
        }

        match extent_state {
            ExtentState::Flushed => {
                // Either we just inserted a terminal row, or a racing caller already
                // finalized it. Either way: idempotent no-op.
                tracing::debug!(
                    "record_extent_flushed: stream {} extent {} already Flushed \
                     (epoch={}), idempotent",
                    stream_id,
                    extent_id,
                    epoch.0,
                );
            }
            ExtentState::Sealed => {
                // Normal in-order path: Sealed → Flushed.
                sqlx::query(
                    "UPDATE extent SET state = ?, flushed_at = NOW(3) \
                     WHERE stream_id = ? AND extent_id = ? AND state = ?",
                )
                .bind(ExtentState::Flushed.as_u8())
                .bind(stream_id.0)
                .bind(extent_id.0 as i64)
                .bind(ExtentState::Sealed.as_u8())
                .execute(&mut *tx)
                .await
                .context(DatabaseSnafu {
                    message: "update extent flushed (from Sealed)",
                })?;
                tracing::info!(
                    "record_extent_flushed: stream {} extent {} Sealed→Flushed \
                     (epoch={})",
                    stream_id,
                    extent_id,
                    epoch.0,
                );
            }
            ExtentState::Active => {
                // Out-of-order: flushed notification beat the seal notification.
                // Fold the transition into a single UPDATE: set state to Flushed,
                // adopt the authoritative end_offset from the flush notification
                // (the row's end_offset may be a placeholder or stale progress),
                // and backfill sealed_at if it was never written.
                sqlx::query(
                    "UPDATE extent SET state = ?, end_offset = ?, flushed_at = NOW(3), \
                        sealed_at = IFNULL(sealed_at, NOW(3)) \
                     WHERE stream_id = ? AND extent_id = ? AND state = ?",
                )
                .bind(ExtentState::Flushed.as_u8())
                .bind(end_offset as i64)
                .bind(stream_id.0)
                .bind(extent_id.0 as i64)
                .bind(ExtentState::Active.as_u8())
                .execute(&mut *tx)
                .await
                .context(DatabaseSnafu {
                    message: "update extent flushed (from Active)",
                })?;
                tracing::info!(
                    "record_extent_flushed: stream {} extent {} Active→Flushed \
                     (epoch={}, end_offset={}) — out-of-order flush notification",
                    stream_id,
                    extent_id,
                    epoch.0,
                    end_offset,
                );
            }
            ExtentState::Unspecified => {
                // Should be impossible — the INSERT IGNORE above can only land a
                // Flushed row, and SM never persists Unspecified. Treat as skip.
                tracing::warn!(
                    "record_extent_flushed: stream {} extent {} in Unspecified state, \
                     cannot transition to Flushed (epoch={})",
                    stream_id,
                    extent_id,
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
    /// For each extent in the report, insert it if missing and update stream_sequence.
    /// Extents that are reported as sealed get their end_offset set.
    ///
    /// Fenced on `epoch`: the reported extents describe the stream as of that
    /// epoch, so writing them after a concurrent transition has advanced the
    /// stream would install rows for an epoch the stream has already left.
    /// Callers that lost the race get `EpochStale` and no rows are written.
    pub async fn reconcile_extents(
        &self,
        stream_id: StreamId,
        epoch: Epoch,
        extents: &[(ExtentId, u64, u64, ExtentState)], // (extent_id, start_offset, end_offset, state)
    ) -> Result<(), StorageError> {
        let mut conn = self.pool.acquire().await.context(DatabaseSnafu {
            message: "acquire connection",
        })?;
        let mut tx = conn.begin().await.context(DatabaseSnafu {
            message: "begin transaction",
        })?;

        let stream_row = sqlx::query("SELECT epoch FROM stream WHERE stream_id = ? FOR UPDATE")
            .bind(stream_id.0)
            .fetch_optional(&mut *tx)
            .await
            .context(DatabaseSnafu {
                message: "lock stream for reconcile",
            })?;

        let current_epoch = match stream_row {
            Some(row) => Epoch(row.get::<i32, _>("epoch") as u32),
            None => {
                return InternalSnafu {
                    message: format!("stream not found: {stream_id}"),
                }
                .fail();
            }
        };

        if current_epoch != epoch {
            return EpochStaleSnafu { stream_id, epoch }.fail();
        }

        let mut max_extent_id: u32 = 0;

        for (extent_id, start_offset, end_offset, state) in extents {
            if extent_id.0 > max_extent_id {
                max_extent_id = extent_id.0;
            }

            let db_state = if *state == ExtentState::Active {
                ExtentState::Active
            } else {
                ExtentState::Sealed
            };

            // Insert if not exists; if exists and sealed, update end_offset.
            sqlx::query(
                "INSERT INTO extent (stream_id, extent_id, start_offset, end_offset, state, epoch) \
                 VALUES (?, ?, ?, ?, ?, ?) \
                 ON DUPLICATE KEY UPDATE \
                   end_offset = IF(state = 1 AND VALUES(state) = 2, VALUES(end_offset), end_offset), \
                   state = IF(state = 1 AND VALUES(state) = 2, VALUES(state), state), \
                   sealed_at = IF(state = 1 AND VALUES(state) = 2, NOW(), sealed_at)",
            )
            .bind(stream_id.0)
            .bind(extent_id.0 as i64)
            .bind(*start_offset as i64)
            .bind(*end_offset as i64)
            .bind(db_state.as_u8())
            .bind(epoch.0 as i32)
            .execute(&mut *tx)
            .await
            .context(DatabaseSnafu { message: "reconcile extent" })?;
        }

        // Update stream_sequence to be at least max_extent_id + 1.
        if max_extent_id > 0 {
            sqlx::query(
                "UPDATE stream_sequence SET next_extent_id = GREATEST(next_extent_id, ?) WHERE stream_id = ?",
            )
            .bind((max_extent_id + 1) as i64)
            .bind(stream_id.0)
            .execute(&mut *tx)
            .await
            .context(DatabaseSnafu { message: "update stream_sequence" })?;
        }

        tx.commit()
            .await
            .context(DatabaseSnafu { message: "commit" })?;

        Ok(())
    }
}
