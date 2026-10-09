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

//! Public-trait conformance tests with local I/O and faults after write application.

#![expect(
    clippy::expect_used,
    reason = "test failures include operation context"
)]

use std::fmt::{self, Display, Formatter};
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::{TryStreamExt, stream::BoxStream};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};
use object_store_occ::LocalConditionalPut;
use object_store_occ::store::{
    Error, ExpectedRevision, MAX_VALUE_BYTES, ObjectStoreState, StateChange, StateStore, WriteId,
    WriteOutcome,
};
use tokio::sync::Notify;

fn state(store: Arc<dyn ObjectStore>) -> Arc<dyn StateStore> {
    Arc::new(ObjectStoreState::new(store))
}

fn set(value: &'static str) -> StateChange {
    StateChange::Set(Bytes::from_static(value.as_bytes()))
}

fn applied(outcome: &WriteOutcome) {
    assert!(matches!(outcome, WriteOutcome::Applied), "{outcome:?}");
}

/// Runs against independently constructed clients of the same storage namespace.
async fn conformance(first: Arc<dyn StateStore>, second: Arc<dyn StateStore>) {
    let key = Path::from("table/root");
    assert!(first.read(&key).await.expect("initial read").is_none());
    let left_id = WriteId::new();
    let right_id = WriteId::new();
    let (left, right) = tokio::join!(
        first.compare_exchange(&key, ExpectedRevision::Absent, set("left"), left_id),
        second.compare_exchange(&key, ExpectedRevision::Absent, set("right"), right_id),
    );
    let pair = (left.expect("left create"), right.expect("right create"));
    assert!(
        matches!(
            pair,
            (WriteOutcome::Applied, WriteOutcome::Conflict)
                | (WriteOutcome::Conflict, WriteOutcome::Applied)
        ),
        "one conditional create must win: {pair:?}"
    );
    let initial = first
        .read(&key)
        .await
        .expect("first read")
        .expect("created record");
    let stale = second
        .read(&key)
        .await
        .expect("second read")
        .expect("same record");
    assert_eq!(initial.write_id, stale.write_id);
    assert!(initial.write_id == left_id || initial.write_id == right_id);
    let expected_value = if initial.write_id == left_id {
        b"left".as_slice()
    } else {
        b"right".as_slice()
    };
    assert_eq!(initial.value.as_deref(), Some(expected_value));

    let update_id = WriteId::new();
    applied(
        &first
            .compare_exchange(
                &key,
                ExpectedRevision::Exact(&initial.revision),
                set("updated"),
                update_id,
            )
            .await
            .expect("update"),
    );
    let conflict = second
        .compare_exchange(
            &key,
            ExpectedRevision::Exact(&stale.revision),
            set("stale"),
            WriteId::new(),
        )
        .await
        .expect("stale update");
    assert!(matches!(conflict, WriteOutcome::Conflict), "{conflict:?}");
    let updated = first
        .read(&key)
        .await
        .expect("updated read")
        .expect("updated record");
    assert_eq!(updated.value, Some(Bytes::from_static(b"updated")));
    assert_eq!(updated.write_id, update_id);

    applied(
        &first
            .compare_exchange(
                &key,
                ExpectedRevision::Exact(&updated.revision),
                StateChange::Tombstone,
                WriteId::new(),
            )
            .await
            .expect("tombstone"),
    );
    let deleted = first
        .read(&key)
        .await
        .expect("tombstone read")
        .expect("tombstone remains present");
    assert_eq!(deleted.value, None);
    assert!(matches!(
        second
            .compare_exchange(
                &key,
                ExpectedRevision::Absent,
                set("recreate"),
                WriteId::new()
            )
            .await
            .expect("create over tombstone"),
        WriteOutcome::Conflict
    ));
    applied(
        &first
            .compare_exchange(
                &key,
                ExpectedRevision::Exact(&deleted.revision),
                StateChange::Set(Bytes::new()),
                WriteId::new(),
            )
            .await
            .expect("conditional recreate"),
    );
    assert!(matches!(
        first
            .compare_exchange(
                &key,
                ExpectedRevision::Exact(&updated.revision),
                set("old generation"),
                WriteId::new()
            )
            .await
            .expect("old generation"),
        WriteOutcome::Conflict
    ));
    assert_eq!(
        first
            .read(&key)
            .await
            .expect("empty read")
            .expect("empty value record")
            .value,
        Some(Bytes::new())
    );
}

#[tokio::test]
async fn independent_local_clients_obey_the_contract() {
    let dir = tempfile::tempdir().expect("state directory");
    let first = state(Arc::new(
        LocalConditionalPut::new(dir.path()).expect("first local store"),
    ));
    let second = state(Arc::new(
        LocalConditionalPut::new(dir.path()).expect("second local store"),
    ));
    conformance(first, second).await;
}

#[tokio::test]
async fn concurrent_local_updates_from_the_same_revision_have_one_winner() {
    let dir = tempfile::tempdir().expect("state directory");
    let first = state(Arc::new(
        LocalConditionalPut::new(dir.path()).expect("first store"),
    ));
    let second = state(Arc::new(
        LocalConditionalPut::new(dir.path()).expect("second store"),
    ));
    let key = Path::from("root");
    applied(
        &first
            .compare_exchange(
                &key,
                ExpectedRevision::Absent,
                set("initial"),
                WriteId::new(),
            )
            .await
            .expect("create"),
    );
    let left = first.read(&key).await.expect("left read").expect("record");
    let right = second
        .read(&key)
        .await
        .expect("right read")
        .expect("record");
    let left_id = WriteId::new();
    let right_id = WriteId::new();
    let (left, right) = tokio::join!(
        first.compare_exchange(
            &key,
            ExpectedRevision::Exact(&left.revision),
            set("left"),
            left_id
        ),
        second.compare_exchange(
            &key,
            ExpectedRevision::Exact(&right.revision),
            set("right"),
            right_id
        ),
    );
    let pair = (left.expect("left update"), right.expect("right update"));
    let record = first
        .read(&key)
        .await
        .expect("winner read")
        .expect("record");
    match pair {
        (WriteOutcome::Applied, WriteOutcome::Conflict) => {
            assert_eq!(record.write_id, left_id);
            assert_eq!(record.value, Some(Bytes::from_static(b"left")));
        }
        (WriteOutcome::Conflict, WriteOutcome::Applied) => {
            assert_eq!(record.write_id, right_id);
            assert_eq!(record.value, Some(Bytes::from_static(b"right")));
        }
        outcomes => panic!("expected one update winner, got {outcomes:?}"),
    }
}

#[tokio::test]
async fn independent_memory_clients_obey_the_contract() {
    let backend: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    conformance(state(Arc::clone(&backend)), state(backend)).await;
}

#[tokio::test]
async fn identical_values_with_new_write_ids_invalidate_old_revisions() {
    let store = ObjectStoreState::new(Arc::new(object_store::memory::InMemory::new()));
    let key = Path::from("root");
    applied(
        &store
            .compare_exchange(&key, ExpectedRevision::Absent, set("same"), WriteId::new())
            .await
            .expect("create"),
    );
    let before = store.read(&key).await.expect("read").expect("record");
    applied(
        &store
            .compare_exchange(
                &key,
                ExpectedRevision::Exact(&before.revision),
                set("same"),
                WriteId::new(),
            )
            .await
            .expect("replace"),
    );
    let after = store.read(&key).await.expect("read").expect("record");
    assert_eq!(before.value, after.value);
    assert_ne!(before.write_id, after.write_id);
    assert!(matches!(
        store
            .compare_exchange(
                &key,
                ExpectedRevision::Exact(&before.revision),
                set("stale"),
                WriteId::new()
            )
            .await
            .expect("stale write"),
        WriteOutcome::Conflict
    ));
}

#[tokio::test]
async fn revisions_are_scoped_to_key_and_handle_but_allow_handle_clones() {
    let backend: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let store = ObjectStoreState::new(Arc::clone(&backend));
    let other = ObjectStoreState::new(backend);
    let key = Path::from("root");
    let wrong_key = Path::from("other");
    applied(
        &store
            .compare_exchange(
                &key,
                ExpectedRevision::Absent,
                set("initial"),
                WriteId::new(),
            )
            .await
            .expect("create"),
    );
    let before = store.read(&key).await.expect("read").expect("record");
    for (target, path) in [(&other, &key), (&store, &wrong_key)] {
        let error = target
            .compare_exchange(
                path,
                ExpectedRevision::Exact(&before.revision),
                set("wrong"),
                WriteId::new(),
            )
            .await
            .expect_err("invalid revision");
        assert!(matches!(error, Error::InvalidRevision { .. }));
    }
    assert!(store.read(&wrong_key).await.expect("other read").is_none());
    assert_eq!(
        store
            .read(&key)
            .await
            .expect("root read")
            .expect("record")
            .value,
        before.value
    );
    applied(
        &store
            .clone()
            .compare_exchange(
                &key,
                ExpectedRevision::Exact(&before.revision),
                set("clone"),
                WriteId::new(),
            )
            .await
            .expect("clone update"),
    );
}

#[tokio::test]
async fn value_limits_allow_the_boundary_and_reject_oversized_writes_before_io() {
    let store = state(Arc::new(object_store::memory::InMemory::new()));
    let key = Path::from("root");
    applied(
        &store
            .compare_exchange(
                &key,
                ExpectedRevision::Absent,
                StateChange::Set(Bytes::from(vec![7; MAX_VALUE_BYTES])),
                WriteId::new(),
            )
            .await
            .expect("maximum value"),
    );
    let before = store
        .read(&key)
        .await
        .expect("maximum read")
        .expect("record");
    assert_eq!(before.value.as_ref().map(Bytes::len), Some(MAX_VALUE_BYTES));
    let error = store
        .compare_exchange(
            &key,
            ExpectedRevision::Exact(&before.revision),
            StateChange::Set(Bytes::from(vec![8; MAX_VALUE_BYTES + 1])),
            WriteId::new(),
        )
        .await
        .expect_err("oversized value");
    assert!(matches!(error, Error::TooLarge { .. }));
    assert_eq!(
        store
            .read(&key)
            .await
            .expect("unchanged read")
            .expect("record")
            .write_id,
        before.write_id
    );
}

#[tokio::test]
async fn malformed_and_unknown_records_are_errors_not_absence() {
    let backend: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let store = state(Arc::clone(&backend));
    let key = Path::from("root");
    applied(
        &store
            .compare_exchange(
                &key,
                ExpectedRevision::Absent,
                StateChange::Tombstone,
                WriteId::new(),
            )
            .await
            .expect("tombstone"),
    );
    let valid = backend
        .get(&key)
        .await
        .expect("raw read")
        .bytes()
        .await
        .expect("raw bytes");
    let mut invalid_kind = valid.to_vec();
    invalid_kind[8] = 2;
    let mut tombstone_payload = valid.to_vec();
    tombstone_payload.push(0);
    for value in [
        Vec::new(),
        b"legacy JSON".to_vec(),
        valid[..24].to_vec(),
        invalid_kind,
        tombstone_payload,
    ] {
        backend
            .put(&key, Bytes::from(value).into())
            .await
            .expect("inject invalid bytes");
        assert!(matches!(
            store.read(&key).await.expect_err("invalid record"),
            Error::InvalidRecord { .. }
        ));
    }
    let mut future = valid.to_vec();
    future[7] = 2;
    backend
        .put(&key, Bytes::from(future).into())
        .await
        .expect("inject future version");
    assert!(matches!(
        store.read(&key).await.expect_err("unknown version"),
        Error::UnsupportedVersion { version: 2, .. }
    ));
}

#[derive(Debug)]
enum Fault {
    LostResponse,
    RetriedPrecondition,
    CancelAfterApply(Arc<Notify>),
    FalseLength,
    NoVersion,
}

#[derive(Debug)]
struct FaultStore {
    inner: Arc<dyn ObjectStore>,
    fault: Fault,
}

impl Display for FaultStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "FaultStore")
    }
}

#[deny(clippy::missing_trait_methods)]
#[async_trait]
impl ObjectStore for FaultStore {
    async fn put_opts(
        &self,
        key: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let result = self.inner.put_opts(key, payload, opts).await?;
        match &self.fault {
            Fault::LostResponse => Err(object_store::Error::Generic {
                store: "FaultStore",
                source: "injected response loss after apply".into(),
            }),
            Fault::RetriedPrecondition => Err(object_store::Error::Precondition {
                path: key.to_string(),
                source: "injected retry precondition after apply".into(),
            }),
            Fault::CancelAfterApply(notify) => {
                notify.notify_one();
                std::future::pending().await
            }
            Fault::FalseLength | Fault::NoVersion => Ok(result),
        }
    }
    async fn get_opts(&self, key: &Path, opts: GetOptions) -> object_store::Result<GetResult> {
        let mut result = self.inner.get_opts(key, opts).await?;
        match self.fault {
            Fault::FalseLength => result.meta.size = 0,
            Fault::NoVersion => {
                result.meta.e_tag = None;
                result.meta.version = None;
            }
            _ => (),
        }
        Ok(result)
    }
    async fn put_multipart_opts(
        &self,
        key: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(key, opts).await
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn get_ranges(
        &self,
        key: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.inner.get_ranges(key, ranges).await
    }
    fn delete_stream(
        &self,
        keys: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(keys)
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, opts).await
    }
    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: RenameOptions,
    ) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, opts).await
    }
}

#[tokio::test]
async fn lost_success_and_retried_preconditions_do_not_imply_no_effect() {
    for fault in [Fault::LostResponse, Fault::RetriedPrecondition] {
        let lost = matches!(fault, Fault::LostResponse);
        let store = state(Arc::new(FaultStore {
            inner: Arc::new(object_store::memory::InMemory::new()),
            fault,
        }));
        let key = Path::from("root");
        let write_id = WriteId::new();
        let saved_id = serde_json::to_vec(write_id.as_bytes()).expect("persist request identity");
        let outcome = store
            .compare_exchange(&key, ExpectedRevision::Absent, set("durable"), write_id)
            .await
            .expect("submitted write");
        assert!(
            matches!(
                (&outcome, lost),
                (WriteOutcome::Unknown { .. }, true) | (WriteOutcome::Conflict, false)
            ),
            "{outcome:?}"
        );
        let record = store
            .read(&key)
            .await
            .expect("resolve read")
            .expect("applied record");
        let recovered_id = WriteId::from_bytes(
            serde_json::from_slice(&saved_id).expect("recover request identity"),
        );
        assert_eq!(record.write_id, recovered_id);
        assert_eq!(record.value, Some(Bytes::from_static(b"durable")));
    }
}

#[tokio::test]
async fn cancellation_after_apply_does_not_rollback_the_write() {
    let applied_notify = Arc::new(Notify::new());
    let store = state(Arc::new(FaultStore {
        inner: Arc::new(object_store::memory::InMemory::new()),
        fault: Fault::CancelAfterApply(Arc::clone(&applied_notify)),
    }));
    let task_store = Arc::clone(&store);
    let write_id = WriteId::new();
    let task = tokio::spawn(async move {
        task_store
            .compare_exchange(
                &Path::from("root"),
                ExpectedRevision::Absent,
                set("durable"),
                write_id,
            )
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), applied_notify.notified())
        .await
        .expect("backend apply notification");
    task.abort();
    assert!(task.await.expect_err("cancelled task").is_cancelled());
    let record = store
        .read(&Path::from("root"))
        .await
        .expect("resolve cancelled write")
        .expect("applied record");
    assert_eq!(record.write_id, write_id);
    assert_eq!(record.value, Some(Bytes::from_static(b"durable")));
}

#[tokio::test]
async fn read_enforces_both_advertised_and_streamed_size_limits() {
    let backend: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let key = Path::from("root");
    backend
        .put(&key, Bytes::from(vec![0; MAX_VALUE_BYTES + 26]).into())
        .await
        .expect("oversized raw object");
    let normal = state(Arc::clone(&backend));
    assert!(matches!(
        normal.read(&key).await.expect_err("oversized metadata"),
        Error::TooLarge { .. }
    ));
    let lying = state(Arc::new(FaultStore {
        inner: backend,
        fault: Fault::FalseLength,
    }));
    assert!(matches!(
        lying.read(&key).await.expect_err("oversized stream"),
        Error::TooLarge { .. }
    ));
}

#[tokio::test]
async fn missing_version_tokens_are_rejected() {
    let backend: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    backend
        .put(&Path::from("root"), Bytes::from_static(b"small").into())
        .await
        .expect("small object");
    let store = state(Arc::new(FaultStore {
        inner: backend,
        fault: Fault::NoVersion,
    }));
    assert!(matches!(
        store
            .read(&Path::from("root"))
            .await
            .expect_err("missing token"),
        Error::MissingVersion { .. }
    ));
}

#[tokio::test]
async fn unsupported_conditional_updates_never_fall_back_to_overwrite() {
    let dir = tempfile::tempdir().expect("local directory");
    let backend = object_store::local::LocalFileSystem::new_with_prefix(dir.path())
        .expect("local store without CAS");
    let store = state(Arc::new(backend));
    let key = Path::from("root");
    applied(
        &store
            .compare_exchange(
                &key,
                ExpectedRevision::Absent,
                set("initial"),
                WriteId::new(),
            )
            .await
            .expect("create"),
    );
    let record = store.read(&key).await.expect("read").expect("record");
    let outcome = store
        .compare_exchange(
            &key,
            ExpectedRevision::Exact(&record.revision),
            set("unsupported"),
            WriteId::new(),
        )
        .await
        .expect("submitted update");
    assert!(
        matches!(
            outcome,
            WriteOutcome::Unknown {
                source: object_store::Error::NotImplemented { .. }
            }
        ),
        "{outcome:?}"
    );
    assert_eq!(
        store
            .read(&key)
            .await
            .expect("unchanged read")
            .expect("record")
            .value,
        record.value
    );
}

#[tokio::test]
#[ignore = "requires AWS_S3_BUCKET and AWS credentials; creates a unique temporary prefix"]
async fn s3_state_store_conformance() {
    let bucket = std::env::var("AWS_S3_BUCKET").expect("AWS_S3_BUCKET");
    let backend: Arc<dyn ObjectStore> = Arc::new(
        object_store::aws::AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .build()
            .expect("S3 client"),
    );
    let prefix = Path::from(format!(
        "{}/state-store-conformance/{}",
        std::env::var("AWS_S3_PREFIX")
            .unwrap_or_default()
            .trim_matches('/'),
        uuid::Uuid::new_v4()
    ));
    let scoped: Arc<dyn ObjectStore> = Arc::new(object_store::prefix::PrefixStore::new(
        Arc::clone(&backend),
        prefix.clone(),
    ));
    conformance(state(Arc::clone(&scoped)), state(scoped)).await;
    let mut objects = backend.list(Some(&prefix));
    while let Some(object) = objects.try_next().await.expect("test prefix listing") {
        backend
            .delete(&object.location)
            .await
            .expect("delete test object");
    }
}
