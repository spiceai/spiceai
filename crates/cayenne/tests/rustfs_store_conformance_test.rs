/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! What Cayenne's data path needs from an S3-compatible store, checked on the
//! store `CAYENNE_TEST_DATA_STORE=rustfs` names: ranged reads, including the
//! short read of a range past the end that marks a truncated file, multipart
//! uploads completed and aborted, and conditional writes. Skipped unless that
//! store is selected; selected without its variables, the fixture fails.

use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path;
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};

use crate::common::{BackendType, TestFixture};

/// The store and the prefix the fixture gives its tables, or `None` when
/// another store is selected.
async fn rustfs() -> Option<(Arc<dyn ObjectStore>, String)> {
    if std::env::var("CAYENNE_TEST_DATA_STORE").as_deref() != Ok("rustfs") {
        eprintln!("skipped: set CAYENNE_TEST_DATA_STORE=rustfs and CAYENNE_TEST_RUSTFS_* to run");
        return None;
    }
    let fixture = TestFixture::new(BackendType::Sqlite)
        .await
        .unwrap_or_else(|error| panic!("the rustfs fixture: {error}"));
    let config = fixture
        .data_store
        .config
        .expect("the rustfs fixture has a store");
    let prefix = config.url.path().trim_matches('/').to_string();
    Some((config.store, prefix))
}

/// Deterministic bytes, distinct at every offset that a range reads.
fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|i| u8::try_from(i % 251).expect("byte")).collect()
}

#[tokio::test]
async fn ranged_reads_return_exactly_the_bytes_asked_for() {
    let Some((store, prefix)) = rustfs().await else {
        return;
    };
    let location = Path::from(format!("{prefix}/ranges/object.vortex"));
    let bytes = pattern(1 << 20);
    store
        .put(&location, PutPayload::from(bytes.clone()))
        .await
        .expect("put");
    let len = u64::try_from(bytes.len()).expect("len");
    for range in [0..10, 500_000..500_100, len - 7..len] {
        let got = store.get_range(&location, range.clone()).await.expect("ranged get");
        let (start, end) = (
            usize::try_from(range.start).expect("start"),
            usize::try_from(range.end).expect("end"),
        );
        assert_eq!(got.as_ref(), &bytes[start..end], "range {range:?}");
    }
    let ranges = [3..4, 1000..2000, len - 1..len];
    let got = store.get_ranges(&location, &ranges).await.expect("get_ranges");
    for (range, got) in ranges.iter().zip(&got) {
        let (start, end) = (
            usize::try_from(range.start).expect("start"),
            usize::try_from(range.end).expect("end"),
        );
        assert_eq!(got.as_ref(), &bytes[start..end], "range {range:?}");
    }
    // A range past the end of the object, as a reader of a truncated file asks
    // for: either refused, or answered with fewer bytes and that range.
    let past_end = store
        .get_opts(
            &location,
            GetOptions {
                range: Some(GetRange::Bounded(len - 10..len + 10)),
                ..GetOptions::default()
            },
        )
        .await;
    match past_end {
        Ok(result) => {
            assert_eq!(result.range, len - 10..len, "the range the store answered");
            assert_eq!(result.bytes().await.expect("bytes").len(), 10);
        }
        Err(error) => eprintln!("a range past the end is refused: {error}"),
    }
    let start_past_end = store
        .get_opts(
            &location,
            GetOptions {
                range: Some(GetRange::Bounded(len + 1..len + 10)),
                ..GetOptions::default()
            },
        )
        .await;
    assert!(
        start_past_end.is_err(),
        "a range that starts past the end is refused"
    );
}

#[tokio::test]
async fn a_multipart_upload_completes_whole_and_an_aborted_one_leaves_nothing() {
    // S3 parts are at least 5 MiB, except the last.
    const PART: usize = 5 << 20;
    let Some((store, prefix)) = rustfs().await else {
        return;
    };
    let bytes = pattern(PART * 2 + 12_345);
    let completed = Path::from(format!("{prefix}/multipart/completed.vortex"));
    let mut upload = store.put_multipart(&completed).await.expect("start");
    for part in bytes.chunks(PART) {
        upload
            .put_part(PutPayload::from(Bytes::copy_from_slice(part)))
            .await
            .expect("part");
    }
    upload.complete().await.expect("complete");
    let read = store
        .get(&completed)
        .await
        .expect("get")
        .bytes()
        .await
        .expect("bytes");
    assert_eq!(read.len(), bytes.len());
    assert!(read.as_ref() == bytes.as_slice(), "the completed object holds every part, in order");

    let aborted = Path::from(format!("{prefix}/multipart/aborted.vortex"));
    let mut upload = store.put_multipart(&aborted).await.expect("start");
    upload
        .put_part(PutPayload::from(Bytes::copy_from_slice(&bytes[..PART])))
        .await
        .expect("part");
    upload.abort().await.expect("abort");
    assert!(
        matches!(store.head(&aborted).await, Err(object_store::Error::NotFound { .. })),
        "an aborted upload leaves no object"
    );
}

#[tokio::test]
async fn conditional_writes_refuse_a_lost_race() {
    let Some((store, prefix)) = rustfs().await else {
        return;
    };
    let location = Path::from(format!("{prefix}/conditional/head"));
    let created = store
        .put_opts(
            &location,
            PutPayload::from_static(b"1"),
            PutOptions::from(PutMode::Create),
        )
        .await
        .expect("create");
    assert!(
        matches!(
            store
                .put_opts(&location, PutPayload::from_static(b"x"), PutOptions::from(PutMode::Create))
                .await,
            Err(object_store::Error::AlreadyExists { .. })
        ),
        "a second create is refused"
    );
    let version = UpdateVersion {
        e_tag: created.e_tag.clone(),
        version: created.version.clone(),
    };
    store
        .put_opts(
            &location,
            PutPayload::from_static(b"2"),
            PutOptions::from(PutMode::Update(version.clone())),
        )
        .await
        .expect("an update on the version read");
    assert!(
        matches!(
            store
                .put_opts(&location, PutPayload::from_static(b"3"), PutOptions::from(PutMode::Update(version)))
                .await,
            Err(object_store::Error::Precondition { .. })
        ),
        "an update on a replaced version is refused"
    );
    let read = store.get(&location).await.expect("get").bytes().await.expect("bytes");
    assert_eq!(read.as_ref(), b"2");
}
