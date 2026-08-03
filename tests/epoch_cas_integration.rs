//! Epoch transition tests for the seal-and-allocate transaction.
//!
//! MySQL connection: see `StreamManagerConfig::mysql_url`.

use std::time::{SystemTime, UNIX_EPOCH};

use common::config::StreamManagerConfig;
use common::errors::StorageError;
use common::types::{Epoch, ExtentId, ExtentPolicy, ExtentState, StorageClass, StreamId};
use serial_test::serial;
use sqlx::mysql::MySqlPoolOptions;
use stream_manager::metadata::{MetadataStore, SealResult};

/// Create a stream with one active extent at epoch 0.
async fn stream_with_active_extent(store: &MetadataStore, label: &str) -> (StreamId, ExtentId) {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_nanos();
    let stream_id = store
        .create_stream(
            &format!("{label}-{unique}"),
            1,
            StorageClass::Memory,
            ExtentPolicy::default(),
        )
        .await
        .expect("create stream");

    let extent_id = store
        .allocate_extent(stream_id, 0, &[("127.0.0.1:1".to_string(), 0u8)], Epoch(0))
        .await
        .expect("allocate initial extent");

    (stream_id, extent_id)
}

async fn connect() -> MetadataStore {
    let config = StreamManagerConfig::default();
    let store = MetadataStore::connect_with_max_connections(&config.mysql_url(), 3)
        .await
        .expect("connect to MySQL");
    store.migrate().await.expect("apply migrations");
    store
}

/// A raw pool, for fixtures that must write states `MetadataStore` has no API for.
async fn raw_pool() -> sqlx::MySqlPool {
    MySqlPoolOptions::new()
        .max_connections(1)
        .connect(&StreamManagerConfig::default().mysql_url())
        .await
        .expect("connect for fixture")
}

async fn extent(store: &MetadataStore, stream_id: StreamId, extent_id: ExtentId) -> (u64, u64) {
    let row = store
        .get_extents(stream_id)
        .await
        .expect("read extents")
        .into_iter()
        .find(|e| e.extent_id == extent_id)
        .expect("extent present");
    (row.start_offset, row.end_offset)
}

#[tokio::test]
#[serial]
async fn concurrent_transitions_from_same_epoch_advance_once() {
    let store = connect().await;
    let (stream_id, extent_id) = stream_with_active_extent(&store, "epoch-cas").await;

    let nodes = [("127.0.0.1:2".to_string(), 0u8)];
    let (first, second) = tokio::join!(
        store.seal_and_allocate_transaction(stream_id, extent_id, 10, &nodes, Epoch(0)),
        store.seal_and_allocate_transaction(stream_id, extent_id, 10, &nodes, Epoch(0)),
    );

    let outcomes = [first, second];
    assert_eq!(
        outcomes
            .iter()
            .filter(|result| matches!(
                result,
                Ok(SealResult::Sealed {
                    new_epoch: Epoch(1),
                    ..
                })
            ))
            .count(),
        1,
        "exactly one transition should advance the epoch",
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|result| matches!(
                result,
                Err(StorageError::EpochStale {
                    stream_id: stale_stream,
                    epoch: Epoch(0),
                    ..
                }) if *stale_stream == stream_id
            ))
            .count(),
        1,
        "the losing transition should report the request epoch as stale",
    );

    assert_eq!(
        store
            .get_stream_epoch(stream_id)
            .await
            .expect("read final epoch"),
        Epoch(1),
    );

    // The winner's successor is the only new extent, and it sits at the epoch the
    // stream now advertises.
    let extents = store.get_extents(stream_id).await.expect("read extents");
    assert_eq!(extents.len(), 2, "expected exactly one successor extent");
    let successor = extents.last().expect("successor extent");
    assert_eq!(successor.state, ExtentState::Active);
    assert_eq!(successor.epoch, Epoch(1));

    assert!(
        !store
            .get_replicas(stream_id, Epoch(1))
            .await
            .expect("read replicas")
            .is_empty(),
        "the new epoch must have a replica set",
    );
}

/// A seal request can arrive late, after the Extent Node has already sealed the
/// extent and started its successor, and report a further end offset. Honouring
/// it verbatim would stretch the predecessor across the successor's start and
/// make the same offsets readable from two extents.
#[tokio::test]
#[serial]
async fn delayed_seal_cannot_extend_predecessor_past_its_successor() {
    let store = connect().await;
    let (stream_id, extent_id) = stream_with_active_extent(&store, "epoch-overlap").await;

    let successor_id = ExtentId(extent_id.0 + 1);
    store
        .record_extent_sealed(stream_id, Epoch(0), extent_id, 10, successor_id)
        .await
        .expect("record autonomous seal");

    // The delayed request claims the extent runs to 20, but the successor has
    // owned everything from 10 onwards since the autonomous transition.
    store
        .seal_and_allocate_transaction(
            stream_id,
            extent_id,
            20,
            &[("127.0.0.1:2".to_string(), 0u8)],
            Epoch(0),
        )
        .await
        .expect("delayed seal must succeed");

    let (_, predecessor_end) = extent(&store, stream_id, extent_id).await;
    let (successor_start, _) = extent(&store, stream_id, successor_id).await;
    assert_eq!(
        predecessor_end, successor_start,
        "the predecessor must end exactly where its successor begins",
    );
    assert_eq!(predecessor_end, 10);
}

/// The successor can be persisted past the sealed extent's end offset when a
/// sealed chain already reached further. `Sealed` must report the offset that
/// was persisted, because the caller registers the extent on the Extent Node
/// with it — recomputing it there would have MySQL and the Extent Node describe
/// the same extent differently.
#[tokio::test]
#[serial]
async fn sealed_reports_the_start_offset_it_persisted() {
    let store = connect().await;
    let (stream_id, first) = stream_with_active_extent(&store, "epoch-startoff").await;

    // Build a sealed chain reaching to 30, then seal its tail so no active
    // successor remains and the transaction has to allocate one.
    let second = ExtentId(first.0 + 1);
    let third = ExtentId(first.0 + 2);
    store
        .record_extent_sealed(stream_id, Epoch(0), first, 10, second)
        .await
        .expect("seal first");
    store
        .record_extent_sealed(stream_id, Epoch(0), second, 30, third)
        .await
        .expect("seal second");
    sqlx::query(
        "UPDATE extent SET state = ?, end_offset = 30 WHERE stream_id = ? AND extent_id = ?",
    )
    .bind(ExtentState::Sealed.as_u8())
    .bind(stream_id.0)
    .bind(third.0 as i64)
    .execute(&raw_pool().await)
    .await
    .expect("seal the tail of the chain");

    // Reported end offset is 10, but the chain already reaches 30.
    let result = store
        .seal_and_allocate_transaction(
            stream_id,
            first,
            10,
            &[("127.0.0.1:2".to_string(), 0u8)],
            Epoch(0),
        )
        .await
        .expect("seal must succeed");

    let (new_extent_id, new_start_offset) = match result {
        SealResult::Sealed {
            new_extent_id,
            new_start_offset,
            ..
        } => (new_extent_id, new_start_offset),
        other => panic!("expected Sealed, got {other:?}"),
    };

    assert_eq!(
        new_start_offset, 30,
        "the successor must start past the sealed chain, not at the reported end offset",
    );
    let (persisted_start, _) = extent(&store, stream_id, new_extent_id).await;
    assert_eq!(
        new_start_offset, persisted_start,
        "the reported start offset must be the one persisted",
    );
}

/// An Extent Node can seal an extent autonomously, create the successor, and
/// flush the sealed extent to S3 before the Stream Manager's seal request lands.
/// The request must recognise the flushed predecessor as already sealed and
/// return the existing successor — allocating another one would leave the stream
/// with two active extents.
#[tokio::test]
#[serial]
async fn flushed_predecessor_is_already_sealed_and_allocates_nothing() {
    let store = connect().await;
    let (stream_id, extent_id) = stream_with_active_extent(&store, "epoch-flushed").await;

    // Autonomous transition: seal the extent and install the successor, then
    // flush the sealed extent so it leaves the Sealed state.
    let successor_id = ExtentId(extent_id.0 + 1);
    store
        .record_extent_sealed(stream_id, Epoch(0), extent_id, 10, successor_id)
        .await
        .expect("record autonomous seal");
    store
        .record_extent_flushed(stream_id, Epoch(0), extent_id, 0, 10)
        .await
        .expect("record flush");

    // Report a further end offset than was uploaded. A flushed extent's bytes
    // are already in S3, so its boundary must not move.
    let result = store
        .seal_and_allocate_transaction(
            stream_id,
            extent_id,
            20,
            &[("127.0.0.1:2".to_string(), 0u8)],
            Epoch(0),
        )
        .await
        .expect("seal of a flushed extent must succeed");

    match result {
        SealResult::AlreadySealed { new_extent_id, .. } => {
            assert_eq!(
                new_extent_id, successor_id,
                "the existing successor must be reported",
            );
        }
        other => panic!("expected AlreadySealed, got {other:?}"),
    }

    assert_eq!(
        store.get_stream_epoch(stream_id).await.expect("read epoch"),
        Epoch(0),
        "an already-sealed extent must not advance the epoch",
    );

    let active: Vec<_> = store
        .get_extents(stream_id)
        .await
        .expect("read extents")
        .into_iter()
        .filter(|e| e.state == ExtentState::Active)
        .collect();
    assert_eq!(
        active.len(),
        1,
        "the stream must keep exactly one active extent, found {active:?}",
    );
    assert_eq!(active[0].extent_id, successor_id);

    let (_, flushed_end) = extent(&store, stream_id, extent_id).await;
    assert_eq!(
        flushed_end, 10,
        "a flushed extent must not advertise bytes beyond what reached S3",
    );
}

#[tokio::test]
#[serial]
async fn failed_allocation_leaves_epoch_and_active_extent_aligned() {
    let store = connect().await;
    let (stream_id, extent_id) = stream_with_active_extent(&store, "epoch-atomic").await;

    let pool = MySqlPoolOptions::new()
        .max_connections(1)
        .connect(&StreamManagerConfig::default().mysql_url())
        .await
        .expect("connect for fixture");

    // Plant the extent id the transaction is about to allocate so the successor
    // INSERT collides. The failure then lands *after* the seal and the sequence
    // bump, which is the only way to observe that they roll back — a request
    // rejected up front (unknown extent, stale epoch) never writes at all.
    let sequence_before: i64 =
        sqlx::query_scalar("SELECT next_extent_id FROM stream_sequence WHERE stream_id = ?")
            .bind(stream_id.0)
            .fetch_one(&pool)
            .await
            .expect("read stream_sequence");

    sqlx::query(
        "INSERT INTO extent (stream_id, extent_id, start_offset, end_offset, state, epoch) \
         VALUES (?, ?, 0, 0, ?, 0)",
    )
    .bind(stream_id.0)
    .bind(sequence_before + 1)
    .bind(ExtentState::Sealed.as_u8())
    .execute(&pool)
    .await
    .expect("plant colliding successor");

    let result = store
        .seal_and_allocate_transaction(
            stream_id,
            extent_id,
            10,
            &[("127.0.0.1:2".to_string(), 0u8)],
            Epoch(0),
        )
        .await;
    // Pin the failure to the successor INSERT. If the transaction bailed out
    // earlier the assertions below would hold trivially and prove nothing about
    // rollback.
    match result {
        Err(StorageError::Database { ref message, .. }) if message == "insert extent" => {}
        other => panic!("expected the successor insert to fail, got {other:?}"),
    }

    assert_eq!(
        store
            .get_stream_epoch(stream_id)
            .await
            .expect("read epoch after failure"),
        Epoch(0),
        "a failed allocation must not advance the stream epoch",
    );

    let active = store
        .get_active_extent(stream_id)
        .await
        .expect("read active extent")
        .expect("active extent still present");
    assert_eq!(
        active.extent_id, extent_id,
        "the seal must roll back and leave the original extent active",
    );
    assert_eq!(
        active.epoch,
        Epoch(0),
        "the active extent must stay at the stream epoch",
    );

    let sequence_after: i64 =
        sqlx::query_scalar("SELECT next_extent_id FROM stream_sequence WHERE stream_id = ?")
            .bind(stream_id.0)
            .fetch_one(&pool)
            .await
            .expect("read stream_sequence after failure");
    assert_eq!(
        sequence_after, sequence_before,
        "the extent id sequence must roll back with the failed allocation",
    );

    assert!(
        store
            .get_replicas(stream_id, Epoch(1))
            .await
            .expect("read replicas")
            .is_empty(),
        "the abandoned epoch must not keep a replica set",
    );
}
