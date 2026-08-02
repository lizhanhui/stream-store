//! Epoch transition tests for the seal-and-allocate transaction.
//!
//! MySQL connection: see `StreamManagerConfig::mysql_url`.

use std::time::{SystemTime, UNIX_EPOCH};

use common::config::StreamManagerConfig;
use common::errors::StorageError;
use common::types::{Epoch, ExtentId, ExtentPolicy, ExtentState, StorageClass, StreamId};
use serial_test::serial;
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
    let store = MetadataStore::connect(&config.mysql_url())
        .await
        .expect("connect to MySQL");
    store.migrate().await.expect("apply migrations");
    store
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

#[tokio::test]
#[serial]
async fn failed_allocation_leaves_epoch_and_active_extent_aligned() {
    let store = connect().await;
    let (stream_id, _extent_id) = stream_with_active_extent(&store, "epoch-atomic").await;

    // An unknown extent id fails the transaction after the epoch has been
    // fenced but before anything is committed.
    let result = store
        .seal_and_allocate_transaction(
            stream_id,
            ExtentId(999),
            10,
            &[("127.0.0.1:2".to_string(), 0u8)],
            Epoch(0),
        )
        .await;
    assert!(result.is_err(), "seal of an unknown extent must fail");

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
        active.epoch,
        Epoch(0),
        "the active extent must stay at the stream epoch",
    );
}
