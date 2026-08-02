use std::time::{SystemTime, UNIX_EPOCH};

use common::config::StreamManagerConfig;
use common::errors::StorageError;
use common::types::{Epoch, ExtentPolicy, StorageClass};
use serial_test::serial;
use stream_manager::metadata::MetadataStore;

#[tokio::test]
#[serial]
async fn concurrent_bumps_from_same_expected_epoch_advance_once() {
    let config = StreamManagerConfig::default();
    let store = MetadataStore::connect(&config.mysql_url())
        .await
        .expect("connect to MySQL");
    store.migrate().await.expect("apply migrations");

    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_nanos();
    let stream_id = store
        .create_stream(
            &format!("epoch-cas-{unique}"),
            1,
            StorageClass::Memory,
            ExtentPolicy::default(),
        )
        .await
        .expect("create stream");

    let expected = Epoch(0);
    let (first, second) = tokio::join!(
        store.bump_epoch(stream_id, expected),
        store.bump_epoch(stream_id, expected),
    );

    let outcomes = [first, second];
    assert_eq!(
        outcomes
            .iter()
            .filter(|result| matches!(result, Ok(Epoch(1))))
            .count(),
        1,
        "exactly one CAS should advance the epoch",
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
        "the losing CAS should report the request epoch as stale",
    );
    assert_eq!(
        store
            .get_stream_epoch(stream_id)
            .await
            .expect("read final epoch"),
        Epoch(1),
    );
}
