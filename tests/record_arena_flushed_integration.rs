//! Integration tests for `MetadataStore::record_arena_flushed`.
//!
//! `record_arena_flushed` must be an upsert that drives the extent row to
//! `Flushed` from any starting state (missing, Active, Sealed, or already Flushed).
//!
//! MySQL connection: inherited from `StreamManagerConfig::default().mysql_url()`.
//!
//! All scenarios run in a single `#[serial]` test to avoid races on shared
//! tables (matches the pattern in `phase2_integration.rs`).

use common::config::StreamManagerConfig;
use common::types::{Epoch, EpochPolicy, EpochState, StorageClass, StreamId};
use serial_test::serial;
use stream_manager::metadata::MetadataStore;

async fn fresh_store() -> MetadataStore {
    let url = StreamManagerConfig::default().mysql_url();

    // Drop tables for a clean slate, then run migrations via the store.
    let pool = sqlx::mysql::MySqlPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .expect("mysql connect (cleanup)");
    for table in &[
        "stream_replica",
        "stream_epoch_s3",
        "stream_epochs",
        "extent",
        "stream",
        "node_metrics",
        "stream_manager_leadership",
        "node",
        "refinery_schema_history",
    ] {
        sqlx::query(&format!("DROP TABLE IF EXISTS {table}"))
            .execute(&pool)
            .await
            .unwrap_or_else(|e| panic!("drop {table}: {e}"));
    }
    pool.close().await;

    let store = MetadataStore::connect(&url)
        .await
        .expect("metadata connect");
    store.migrate().await.expect("migrate");
    store
}

/// Look up a single epoch row so tests can assert on its state / offsets.
async fn get_epoch(
    store: &MetadataStore,
    stream_id: StreamId,
    epoch: Epoch,
) -> EpochRowSnapshot {
    let epochs = store.get_extents(stream_id).await.expect("get_extents");
    let row = epochs
        .into_iter()
        .find(|e| e.epoch == epoch)
        .unwrap_or_else(|| panic!("epoch {epoch} missing on stream {stream_id}"));
    EpochRowSnapshot {
        state: row.state,
        end_offset: row.end_offset,
    }
}

#[derive(Debug)]
struct EpochRowSnapshot {
    state: EpochState,
    end_offset: u64,
}

/// Create a stream and allocate its first epoch so Active-state tests have
/// something to transition. Returns `(stream_id, epoch)`.
async fn setup_stream_with_active_epoch(
    store: &MetadataStore,
    name: &str,
) -> (StreamId, Epoch) {
    let stream_id = store
        .create_stream(name, 1, StorageClass::S3, EpochPolicy::default())
        .await
        .expect("create_stream");

    // Epoch 0 is the initial stream epoch written by create_stream.
    let epoch = Epoch(0);
    let epoch = store
        .allocate_epoch_row(stream_id, 0, &[("127.0.0.1:1".to_string(), 0)], epoch)
        .await
        .expect("allocate_epoch_row");
    (stream_id, epoch)
}

#[tokio::test]
#[serial]
async fn record_arena_flushed_ordering_variants() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .try_init();

    let store = fresh_store().await;

    // ── Scenario 3: flushed_twice — idempotent ──────────────────────────────
    let (s3, ep3) = setup_stream_with_active_epoch(&store, "scenario3").await;
    store
        .record_arena_flushed(s3, ep3, 0, 512, "default/stream-3/0-512")
        .await
        .expect("flushed s3 first");
    store
        .record_arena_flushed(s3, ep3, 0, 512, "default/stream-3/0-512")
        .await
        .expect("flushed s3 second (idempotent)");

    let snap3 = get_epoch(&store, s3, ep3).await;
    assert_eq!(
        snap3.state,
        EpochState::Flushed,
        "s3 still Flushed after double notification"
    );
    assert_eq!(snap3.end_offset, 512);

    // ── Scenario 4: flushed_with_wrong_epoch — stale notification skipped ───
    //
    // The extent's epoch is 0. A flush notification carrying a different
    // epoch must be ignored (both the stream-epoch and extent-epoch guards).
    let (s4, ep4) = setup_stream_with_active_epoch(&store, "scenario4").await;
    let wrong_epoch = Epoch(99);
    store
        .record_arena_flushed(s4, wrong_epoch, 0, 777, "default/stream-4/0-777")
        .await
        .expect("flushed s4 wrong-epoch must not error");

    let snap4 = get_epoch(&store, s4, ep4).await;
    assert_eq!(
        snap4.state,
        EpochState::Active,
        "s4 must remain Active under wrong epoch"
    );
    assert_eq!(
        snap4.end_offset, 0,
        "s4 end_offset unchanged under wrong epoch"
    );
    let epochs4 = store.get_extents(s4).await.expect("get_extents s4");
    assert!(
        epochs4.iter().all(|e| e.epoch != wrong_epoch),
        "wrong-epoch flush must not materialize a future epoch row"
    );
}
