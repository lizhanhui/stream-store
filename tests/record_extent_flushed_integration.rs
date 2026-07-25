//! Integration tests for `MetadataStore::record_extent_flushed`.
//!
//! `UpdateExtentSealed` and `UpdateExtentFlushed` are independent fire-and-forget
//! notifications. Their relative ordering at Stream Manager is not guaranteed
//! (channel backpressure `try_send` drops, SM reconnect between frames, SM
//! failover splitting the pair). `record_extent_flushed` must therefore be an
//! upsert that drives the extent row to `Flushed` from any starting state.
//!
//! MySQL connection: inherited from `StreamManagerConfig::default().mysql_url()`.
//!
//! All scenarios run in a single `#[serial]` test to avoid races on shared
//! tables (matches the pattern in `phase2_integration.rs`).

use common::config::StreamManagerConfig;
use common::types::{Epoch, ExtentId, ExtentPolicy, ExtentState, StorageClass, StreamId};
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
        "extent",
        "stream_sequence",
        "stream",
        "node_metrics",
        "stream_manager_leadership",
        "node",
        "refinery_schema_history",
    ] {
        sqlx::query(sqlx::AssertSqlSafe(format!("DROP TABLE IF EXISTS {table}")))
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

/// Look up a single extent row so tests can assert on its state / offsets.
async fn get_extent(
    store: &MetadataStore,
    stream_id: StreamId,
    extent_id: ExtentId,
) -> ExtentRowSnapshot {
    let extents = store.get_extents(stream_id).await.expect("get_extents");
    let row = extents
        .into_iter()
        .find(|e| e.extent_id == extent_id)
        .unwrap_or_else(|| panic!("extent {extent_id} missing on stream {stream_id}"));
    ExtentRowSnapshot {
        state: row.state,
        start_offset: row.start_offset,
        end_offset: row.end_offset,
        epoch: row.epoch,
    }
}

#[derive(Debug)]
struct ExtentRowSnapshot {
    state: ExtentState,
    start_offset: u64,
    end_offset: u64,
    epoch: Epoch,
}

/// Create a stream and allocate its first extent so Active-state tests have
/// something to transition. Returns `(stream_id, extent_id, epoch)`.
async fn setup_stream_with_active_extent(
    store: &MetadataStore,
    name: &str,
) -> (StreamId, ExtentId, Epoch) {
    let stream_id = store
        .create_stream(name, 1, StorageClass::S3, ExtentPolicy::default())
        .await
        .expect("create_stream");

    // Epoch 0 is the initial stream epoch written by create_stream.
    let epoch = Epoch(0);
    let extent_id = store
        .allocate_extent(stream_id, 0, &[("127.0.0.1:1".to_string(), 0)], epoch)
        .await
        .expect("allocate_extent");
    (stream_id, extent_id, epoch)
}

#[tokio::test]
#[serial]
async fn record_extent_flushed_ordering_variants() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .try_init();

    let store = fresh_store().await;

    // ── Scenario 1: sealed_then_flushed — baseline, no regression ───────────
    //
    // Sequence: record_extent_sealed → record_extent_flushed. Row starts
    // Active, transitions to Sealed, then to Flushed. Offsets set by seal.
    let (s1, e1, ep1) = setup_stream_with_active_extent(&store, "scenario1").await;
    let new_ext1 = ExtentId(e1.0 + 1);
    store
        .record_extent_sealed(s1, ep1, e1, 1000, new_ext1)
        .await
        .expect("sealed s1");
    store
        .record_extent_flushed(s1, ep1, e1, 0, 1000)
        .await
        .expect("flushed s1");

    let snap = get_extent(&store, s1, e1).await;
    assert_eq!(snap.state, ExtentState::Flushed, "s1 final state");
    assert_eq!(snap.start_offset, 0, "s1 start_offset");
    assert_eq!(snap.end_offset, 1000, "s1 end_offset");
    assert_eq!(snap.epoch, ep1);

    // ── Scenario 2: flushed_then_sealed — out-of-order race ─────────────────
    //
    // Sequence: record_extent_flushed arrives FIRST (row is still Active),
    // record_extent_sealed arrives later. The flushed handler must upsert the
    // row directly to Flushed. The later sealed handler must NOT regress the
    // state.
    let (s2, e2, ep2) = setup_stream_with_active_extent(&store, "scenario2").await;
    let new_ext2 = ExtentId(e2.0 + 1);

    // At this point e2 is Active.
    assert_eq!(get_extent(&store, s2, e2).await.state, ExtentState::Active);

    store
        .record_extent_flushed(s2, ep2, e2, 0, 2048)
        .await
        .expect("flushed s2 out-of-order");

    let after_flushed = get_extent(&store, s2, e2).await;
    assert_eq!(
        after_flushed.state,
        ExtentState::Flushed,
        "s2 after out-of-order flushed"
    );
    assert_eq!(
        after_flushed.end_offset, 2048,
        "s2 adopted end_offset from flush notification"
    );

    // Now the late sealed notification arrives.
    store
        .record_extent_sealed(s2, ep2, e2, 2048, new_ext2)
        .await
        .expect("sealed s2 late");

    let after_late_sealed = get_extent(&store, s2, e2).await;
    assert_eq!(
        after_late_sealed.state,
        ExtentState::Flushed,
        "s2 must stay Flushed after late sealed"
    );
    assert_eq!(after_late_sealed.end_offset, 2048);

    // ── Scenario 3: flushed_twice — idempotent ──────────────────────────────
    let (s3, e3, ep3) = setup_stream_with_active_extent(&store, "scenario3").await;
    store
        .record_extent_flushed(s3, ep3, e3, 0, 512)
        .await
        .expect("flushed s3 first");
    store
        .record_extent_flushed(s3, ep3, e3, 0, 512)
        .await
        .expect("flushed s3 second (idempotent)");

    let snap3 = get_extent(&store, s3, e3).await;
    assert_eq!(
        snap3.state,
        ExtentState::Flushed,
        "s3 still Flushed after double notification"
    );
    assert_eq!(snap3.end_offset, 512);

    // ── Scenario 4: flushed_with_wrong_epoch — stale notification skipped ───
    //
    // The extent's epoch is 0. A flush notification carrying a different
    // epoch must be ignored (both the stream-epoch and extent-epoch guards).
    let (s4, e4, _ep4) = setup_stream_with_active_extent(&store, "scenario4").await;
    let wrong_epoch = Epoch(99);
    store
        .record_extent_flushed(s4, wrong_epoch, e4, 0, 777)
        .await
        .expect("flushed s4 wrong-epoch must not error");

    let snap4 = get_extent(&store, s4, e4).await;
    assert_eq!(
        snap4.state,
        ExtentState::Active,
        "s4 must remain Active under wrong epoch"
    );
    assert_eq!(
        snap4.end_offset, 0,
        "s4 end_offset unchanged under wrong epoch"
    );
}
