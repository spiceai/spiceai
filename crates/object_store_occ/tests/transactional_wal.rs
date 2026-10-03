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

//! Integration histories through the public WAL API and real object-store adapters.

#![expect(
    clippy::expect_used,
    reason = "test failures include operation context"
)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use object_store::ObjectStore;
use object_store::path::Path;
use object_store_occ::LocalConditionalPut;
use object_store_occ::store::{
    self, ExpectedRevision, ObjectStoreState, StateChange, StateRecord, StateStore, WriteId,
    WriteOutcome,
};
use object_store_occ::wal::{
    CommitOutcome, CommitReceipt, Error, Limits, PreparedCommit, Resolution, Snapshot,
    WalStateStore,
};
use tokio::sync::Notify;

fn memory() -> Arc<dyn StateStore> {
    Arc::new(ObjectStoreState::new(Arc::new(
        object_store::memory::InMemory::new(),
    )))
}

async fn open(state: Arc<dyn StateStore>) -> WalStateStore {
    WalStateStore::open(state, Path::from("domain"), Limits::default())
        .await
        .expect("open domain")
}

async fn prepare(store: &WalStateStore, key: &str, value: &'static [u8]) -> PreparedCommit {
    let mut tx = store.begin().await.expect("begin");
    tx.put(key, Bytes::from_static(value))
        .expect("buffer value");
    tx.prepare().await.expect("prepare")
}

fn committed(outcome: &CommitOutcome) {
    assert!(
        matches!(outcome, CommitOutcome::Committed { .. }),
        "{outcome:?}"
    );
}

async fn conformance(first: WalStateStore, second: WalStateStore) {
    let empty = first.snapshot().await.expect("empty snapshot");
    let mut tx = empty.transaction();
    tx.put("accounts/a", Bytes::from_static(b"100")).expect("a");
    tx.put("accounts/b", Bytes::new()).expect("empty b");
    assert_eq!(tx.get("accounts/b"), Some(&Bytes::new()));
    assert_eq!(
        tx.scan_prefix("accounts/")
            .await
            .expect("own predicate")
            .len(),
        2
    );
    let attempt = tx.prepare().await.expect("prepare initial batch");
    let receipt: CommitReceipt =
        serde_json::from_slice(&serde_json::to_vec(attempt.receipt()).expect("persist receipt"))
            .expect("recover receipt");
    committed(&first.commit(&attempt).await.expect("commit initial batch"));
    assert_eq!(
        second
            .resolve(&receipt)
            .await
            .expect("resolve independent client"),
        Resolution::Committed { sequence: 1 }
    );
    let before = second.snapshot().await.expect("snapshot before updates");
    assert_eq!(before.sequence(), 1);
    assert!(empty.get("accounts/a").is_none());
    assert_eq!(before.get("accounts/b"), Some(&Bytes::new()));

    let mut left = first.begin().await.expect("left begin");
    let mut right = second.begin().await.expect("right begin");
    assert_eq!(left.get("accounts/a"), right.get("accounts/a"));
    for tx in [&mut left, &mut right] {
        tx.put("accounts/a", Bytes::from_static(b"90"))
            .expect("debit");
        tx.put("accounts/b", Bytes::from_static(b"10"))
            .expect("credit");
    }
    let left = left.prepare().await.expect("left prepare");
    let right = right.prepare().await.expect("right prepare");
    let (a, b) = tokio::join!(first.commit(&left), second.commit(&right));
    let (a, b) = (a.expect("left commit"), b.expect("right commit"));
    assert!(
        matches!(
            (&a, &b),
            (CommitOutcome::Committed { .. }, CommitOutcome::Conflict)
                | (CommitOutcome::Conflict, CommitOutcome::Committed { .. })
        ),
        "{a:?}, {b:?}"
    );
    let after = first.snapshot().await.expect("atomic transfer");
    assert_eq!(after.get("accounts/a"), Some(&Bytes::from_static(b"90")));
    assert_eq!(after.get("accounts/b"), Some(&Bytes::from_static(b"10")));
    assert_eq!(before.get("accounts/a"), Some(&Bytes::from_static(b"100")));
    assert_eq!(before.get("accounts/b"), Some(&Bytes::new()));

    let mut deletion = after.transaction();
    deletion.delete("accounts/b").expect("delete b");
    assert!(deletion.get("accounts/b").is_none());
    assert_eq!(
        deletion
            .scan_prefix("accounts/")
            .await
            .expect("delete overlay")
            .len(),
        1
    );
    committed(
        &first
            .commit(&deletion.prepare().await.expect("delete prepare"))
            .await
            .expect("delete commit"),
    );
    let deleted = first.snapshot().await.expect("deleted version");
    assert!(deleted.get("accounts/b").is_none());
    assert!(matches!(
        first.checkpoint(&deleted).await.expect("checkpoint"),
        WriteOutcome::Applied
    ));
    let recovered = second.snapshot().await.expect("checkpoint recovery");
    assert!(recovered.get("accounts/b").is_none());
    committed(
        &first
            .commit(&prepare(&first, "accounts/b", b"recreated").await)
            .await
            .expect("recreate"),
    );
    assert!(deleted.get("accounts/b").is_none());
    assert_eq!(before.get("accounts/b"), Some(&Bytes::new()));
    assert_eq!(
        second
            .resolve(&receipt)
            .await
            .expect("old receipt after checkpoint and overwrite"),
        Resolution::Committed { sequence: 1 }
    );
    // Retrying the identical already-applied attempt is resolved, not re-applied.
    committed(&first.commit(&attempt).await.expect("exact retry"));
    assert_eq!(
        second.snapshot().await.expect("final snapshot").sequence(),
        4
    );
}

#[tokio::test]
async fn memory_clients_atomic_batches_and_mvcc() {
    let backend = memory();
    conformance(open(Arc::clone(&backend)).await, open(backend).await).await;
}

#[tokio::test]
async fn independent_local_clients_atomic_batches_and_mvcc() {
    let dir = tempfile::tempdir().expect("directory");
    let first = Arc::new(ObjectStoreState::new(Arc::new(
        LocalConditionalPut::new(dir.path()).expect("first client"),
    )));
    let second = Arc::new(ObjectStoreState::new(Arc::new(
        LocalConditionalPut::new(dir.path()).expect("second client"),
    )));
    conformance(open(first).await, open(second).await).await;
}

#[tokio::test]
async fn predicate_and_absent_reads_conflict_on_any_intervening_commit() {
    let backend = memory();
    let first = open(Arc::clone(&backend)).await;
    let second = open(backend).await;
    let mut tx = first.begin().await.expect("read transaction");
    assert!(tx.get("missing").is_none());
    assert!(
        tx.scan_prefix("items/")
            .await
            .expect("predicate")
            .is_empty()
    );
    tx.put("summary", Bytes::from_static(b"zero"))
        .expect("write based on reads");
    let attempt = tx.prepare().await.expect("prepare predicate write");
    committed(
        &second
            .commit(&prepare(&second, "items/new", b"one").await)
            .await
            .expect("concurrent insertion"),
    );
    assert!(matches!(
        first.commit(&attempt).await.expect("conflict"),
        CommitOutcome::Conflict
    ));
    assert!(
        first
            .snapshot()
            .await
            .expect("fresh")
            .get("summary")
            .is_none()
    );
}

#[tokio::test]
async fn checkpoint_fences_prepared_attempt_without_advancing_logical_sequence() {
    let store = open(memory()).await;
    let snapshot = store.snapshot().await.expect("snapshot");
    let attempt = prepare(&store, "key", b"old base").await;
    assert_eq!(
        store.resolve(attempt.receipt()).await.expect("pending"),
        Resolution::Pending
    );
    assert!(matches!(
        store.checkpoint(&snapshot).await.expect("checkpoint"),
        WriteOutcome::Applied
    ));
    assert_eq!(store.snapshot().await.expect("same sequence").sequence(), 0);
    assert_eq!(
        store.resolve(attempt.receipt()).await.expect("fenced"),
        Resolution::Rejected
    );
    assert!(matches!(
        store.commit(&attempt).await.expect("old attempt"),
        CommitOutcome::Conflict
    ));
    assert!(
        store
            .snapshot()
            .await
            .expect("no effect")
            .get("key")
            .is_none()
    );
}

#[tokio::test]
async fn stale_checkpoint_cannot_hide_new_commit() {
    let store = open(memory()).await;
    let snapshot = store.snapshot().await.expect("old snapshot");
    let attempt = prepare(&store, "key", b"new").await;
    committed(&store.commit(&attempt).await.expect("new commit"));
    assert!(matches!(
        store.checkpoint(&snapshot).await.expect("stale checkpoint"),
        WriteOutcome::Conflict
    ));
    assert_eq!(
        store.snapshot().await.expect("latest").get("key"),
        Some(&Bytes::from_static(b"new"))
    );
}

#[derive(Default)]
struct Faults {
    mode: AtomicU8,
    reached: Notify,
    resume: Notify,
}

struct FaultStore {
    inner: Arc<dyn StateStore>,
    faults: Arc<Faults>,
}

fn lost() -> WriteOutcome {
    WriteOutcome::Unknown {
        source: object_store::Error::Generic {
            store: "WAL integration fault",
            source: "injected transport response loss".into(),
        },
    }
}

#[async_trait]
impl StateStore for FaultStore {
    async fn read(&self, key: &Path) -> store::Result<Option<StateRecord>> {
        if self.faults.mode.load(Ordering::SeqCst) == 5 && key.as_ref().ends_with("/head") {
            return Err(store::Error::Read {
                key: key.clone(),
                source: object_store::Error::Generic {
                    store: "WAL integration fault",
                    source: "injected head read failure".into(),
                },
            });
        }
        self.inner.read(key).await
    }

    async fn compare_exchange(
        &self,
        key: &Path,
        expected: ExpectedRevision<'_>,
        change: StateChange,
        id: WriteId,
    ) -> store::Result<WriteOutcome> {
        let mode = self.faults.mode.load(Ordering::SeqCst);
        let head = key.as_ref().ends_with("/head");
        if head && mode == 3 {
            return Ok(lost());
        }
        if head && mode == 7 {
            std::process::exit(86);
        }
        if head && mode == 6 {
            self.faults.reached.notify_one();
            self.faults.resume.notified().await;
        }
        let outcome = self
            .inner
            .compare_exchange(key, expected, change, id)
            .await?;
        if matches!(outcome, WriteOutcome::Applied) {
            if head && mode == 8 {
                std::process::exit(86);
            }
            if head && mode == 4 {
                self.faults.reached.notify_one();
                std::future::pending::<()>().await;
            }
            if head && mode == 5 {
                return Ok(lost());
            }
            if mode == 1 {
                return Ok(lost());
            }
            if mode == 2 {
                return Ok(WriteOutcome::Conflict);
            }
        }
        Ok(outcome)
    }
}

async fn fault_store() -> (WalStateStore, Arc<dyn StateStore>, Arc<Faults>) {
    let backend = memory();
    let faults = Arc::new(Faults::default());
    let wrapper: Arc<dyn StateStore> = Arc::new(FaultStore {
        inner: Arc::clone(&backend),
        faults: Arc::clone(&faults),
    });
    (open(wrapper).await, backend, faults)
}

#[tokio::test]
async fn lost_upload_and_publication_responses_resolve_exact_attempts() {
    for mode in [1, 2] {
        let (store, backend, faults) = fault_store().await;
        let attempt = prepare(&store, "key", b"once").await;
        faults.mode.store(mode, Ordering::SeqCst);
        committed(&store.commit(&attempt).await.expect("resolve response loss"));
        faults.mode.store(0, Ordering::SeqCst);
        let reopened = open(backend).await;
        let snapshot = reopened.snapshot().await.expect("recovery");
        assert_eq!(snapshot.sequence(), 1);
        assert_eq!(snapshot.get("key"), Some(&Bytes::from_static(b"once")));
        committed(&store.commit(&attempt).await.expect("retry old attempt"));
        assert_eq!(
            reopened
                .snapshot()
                .await
                .expect("not duplicated")
                .sequence(),
            1
        );
    }
}

#[tokio::test]
async fn staged_wal_is_not_a_commit_and_unknown_can_be_retried_exactly() {
    let (store, backend, faults) = fault_store().await;
    let attempt = prepare(&store, "key", b"staged").await;
    faults.mode.store(3, Ordering::SeqCst);
    assert!(matches!(
        store.commit(&attempt).await.expect("unknown before apply"),
        CommitOutcome::Unknown { .. }
    ));
    let reopened = open(backend).await;
    assert_eq!(
        reopened.resolve(attempt.receipt()).await.expect("pending"),
        Resolution::Pending
    );
    assert!(
        reopened
            .snapshot()
            .await
            .expect("ignore orphan")
            .get("key")
            .is_none()
    );
    faults.mode.store(0, Ordering::SeqCst);
    committed(&store.commit(&attempt).await.expect("same attempt retry"));
}

#[tokio::test]
async fn unreadable_head_after_apply_returns_unknown_until_resolution() {
    let (store, backend, faults) = fault_store().await;
    let attempt = prepare(&store, "key", b"durable").await;
    faults.mode.store(5, Ordering::SeqCst);
    assert!(matches!(
        store.commit(&attempt).await.expect("unknown"),
        CommitOutcome::Unknown { .. }
    ));
    let reopened = open(backend).await;
    assert_eq!(
        reopened.resolve(attempt.receipt()).await.expect("resolve"),
        Resolution::Committed { sequence: 1 }
    );
}

#[tokio::test]
async fn cancellation_after_publication_recovers_from_persisted_receipt() {
    let (store, backend, faults) = fault_store().await;
    let attempt = prepare(&store, "key", b"ack lost").await;
    let persisted = serde_json::to_vec(attempt.receipt()).expect("persist receipt before dispatch");
    faults.mode.store(4, Ordering::SeqCst);
    let task = tokio::spawn(async move { store.commit(&attempt).await });
    faults.reached.notified().await;
    task.abort();
    assert!(task.await.expect_err("aborted response").is_cancelled());
    let receipt = serde_json::from_slice(&persisted).expect("restore receipt");
    let reopened = open(backend).await;
    assert_eq!(
        reopened.resolve(&receipt).await.expect("recover outcome"),
        Resolution::Committed { sequence: 1 }
    );
    assert_eq!(
        reopened.snapshot().await.expect("recover data").get("key"),
        Some(&Bytes::from_static(b"ack lost"))
    );
}

#[tokio::test]
async fn delayed_publication_is_fenced_by_checkpoint() {
    let (store, backend, faults) = fault_store().await;
    let attempt = prepare(&store, "key", b"late").await;
    let receipt = attempt.receipt().clone();
    faults.mode.store(6, Ordering::SeqCst);
    let task = tokio::spawn(async move { store.commit(&attempt).await });
    faults.reached.notified().await;
    let other = open(backend).await;
    let base = other
        .snapshot()
        .await
        .expect("snapshot during staged write");
    assert!(base.get("key").is_none());
    assert!(matches!(
        other.checkpoint(&base).await.expect("fence"),
        WriteOutcome::Applied
    ));
    assert_eq!(
        other
            .resolve(&receipt)
            .await
            .expect("rejected before late write"),
        Resolution::Rejected
    );
    faults.resume.notify_one();
    assert!(matches!(
        task.await.expect("join").expect("late write"),
        CommitOutcome::Conflict
    ));
    assert!(
        other
            .snapshot()
            .await
            .expect("still absent")
            .get("key")
            .is_none()
    );
}

#[tokio::test]
async fn limits_are_persisted_and_checkpoint_only_resets_replay_budget() {
    let state = memory();
    let limits = Limits {
        max_replay_commits: 1,
        max_commits: 2,
        max_keys: 1,
        max_state_bytes: 32,
    };
    let store = WalStateStore::open(Arc::clone(&state), Path::from("domain"), limits)
        .await
        .expect("small limits");
    let incompatible = WalStateStore::open(state, Path::from("domain"), Limits::default())
        .await
        .expect_err("limits must agree");
    assert!(matches!(incompatible, Error::IncompatibleLimits));
    committed(
        &store
            .commit(&prepare(&store, "key", b"first").await)
            .await
            .expect("first"),
    );
    let mut tx = store.begin().await.expect("full tail");
    tx.put("key", Bytes::from_static(b"second"))
        .expect("buffer");
    assert!(matches!(
        tx.prepare().await.expect_err("tail bound"),
        Error::Limit { .. }
    ));
    let snapshot = store.snapshot().await.expect("checkpoint base");
    assert!(matches!(
        store.checkpoint(&snapshot).await.expect("checkpoint"),
        WriteOutcome::Applied
    ));
    let mut overflow = store.begin().await.expect("state bound");
    overflow.put("extra", Bytes::new()).expect("extra key");
    assert!(matches!(
        overflow.prepare().await.expect_err("key bound"),
        Error::Limit { .. }
    ));
    committed(
        &store
            .commit(&prepare(&store, "key", b"second").await)
            .await
            .expect("second"),
    );
    let snapshot = store.snapshot().await.expect("checkpoint base");
    assert!(matches!(
        store.checkpoint(&snapshot).await.expect("checkpoint"),
        WriteOutcome::Applied
    ));
    let mut tx = store.begin().await.expect("exhausted history");
    tx.delete("key").expect("delete");
    assert!(matches!(
        tx.prepare().await.expect_err("permanent history cap"),
        Error::Limit { .. }
    ));
}

#[tokio::test]
async fn foreign_handles_receipts_and_invalid_batches_are_rejected() {
    let state = memory();
    let first = open(Arc::clone(&state)).await;
    let second = open(Arc::clone(&state)).await;
    let other = WalStateStore::open(state, Path::from("other"), Limits::default())
        .await
        .expect("other domain");
    let attempt = prepare(&first, "key", b"value").await;
    assert!(matches!(
        second.commit(&attempt).await.expect_err("different handle"),
        Error::WrongStore
    ));
    assert!(matches!(
        other
            .resolve(attempt.receipt())
            .await
            .expect_err("foreign receipt"),
        Error::InvalidReceipt
    ));
    let mut tx = first.begin().await.expect("begin invalid changes");
    tx.put("", Bytes::new()).expect_err("empty key");
    tx.put("oversized", Bytes::from(vec![0; 128 * 1024]))
        .expect_err("oversized batch");
    tx.prepare().await.expect_err("empty transaction");
    assert!(
        first
            .snapshot()
            .await
            .expect("unchanged")
            .get("oversized")
            .is_none()
    );
}

#[tokio::test]
async fn corrupted_wal_content_fails_recovery_and_resolution() {
    let state = memory();
    let store = open(Arc::clone(&state)).await;
    let attempt = prepare(&store, "key", b"value").await;
    committed(&store.commit(&attempt).await.expect("commit"));
    let json = serde_json::to_value(attempt.receipt()).expect("receipt format");
    let id: [u8; 16] = serde_json::from_value(json["record"]["id"].clone()).expect("id");
    let key = Path::from(format!("domain/wal/{}", uuid::Uuid::from_bytes(id)));
    let record = state.read(&key).await.expect("raw read").expect("WAL");
    assert!(matches!(
        state
            .compare_exchange(
                &key,
                ExpectedRevision::Exact(&record.revision),
                StateChange::Set(Bytes::from_static(b"corrupt")),
                WriteId::new()
            )
            .await
            .expect("inject corruption"),
        WriteOutcome::Applied
    ));
    assert!(matches!(
        store.snapshot().await.expect_err("reject corrupt history"),
        Error::InvalidRecord { .. }
    ));
    assert!(matches!(
        store
            .resolve(attempt.receipt())
            .await
            .expect_err("no false outcome"),
        Error::InvalidRecord { .. }
    ));
}

#[tokio::test]
async fn missing_wal_and_unsupported_head_format_fail_closed() {
    let state = memory();
    let store = open(Arc::clone(&state)).await;
    let attempt = prepare(&store, "key", b"value").await;
    committed(&store.commit(&attempt).await.expect("commit"));
    let head_key = Path::from("domain/head");
    let head = state
        .read(&head_key)
        .await
        .expect("head read")
        .expect("head");
    let mut json: serde_json::Value =
        serde_json::from_slice(head.value.as_ref().expect("head value")).expect("decode head");
    let id: [u8; 16] = serde_json::from_value(json["commit"]["id"].clone()).expect("WAL id");
    let wal_key = Path::from(format!("domain/wal/{}", uuid::Uuid::from_bytes(id)));
    let wal = state.read(&wal_key).await.expect("WAL read").expect("WAL");
    assert!(matches!(
        state
            .compare_exchange(
                &wal_key,
                ExpectedRevision::Exact(&wal.revision),
                StateChange::Tombstone,
                WriteId::new()
            )
            .await
            .expect("inject missing WAL"),
        WriteOutcome::Applied
    ));
    assert!(matches!(
        store.snapshot().await.expect_err("missing committed data"),
        Error::InvalidRecord { .. }
    ));
    json["format"] = serde_json::json!(2);
    assert!(matches!(
        state
            .compare_exchange(
                &head_key,
                ExpectedRevision::Exact(&head.revision),
                StateChange::Set(Bytes::from(serde_json::to_vec(&json).expect("future head"))),
                WriteId::new()
            )
            .await
            .expect("inject future head"),
        WriteOutcome::Applied
    ));
    assert!(matches!(
        store.snapshot().await.expect_err("future format"),
        Error::InvalidRecord { .. }
    ));
}

#[tokio::test]
async fn lost_checkpoint_response_preserves_reads_and_receipt_evidence() {
    let (store, backend, faults) = fault_store().await;
    let attempt = prepare(&store, "key", b"value").await;
    committed(&store.commit(&attempt).await.expect("commit"));
    let snapshot = store.snapshot().await.expect("checkpoint snapshot");
    faults.mode.store(1, Ordering::SeqCst);
    assert!(matches!(
        store
            .checkpoint(&snapshot)
            .await
            .expect("lost checkpoint response"),
        WriteOutcome::Unknown { .. }
    ));
    let reopened = open(backend).await;
    assert_eq!(
        reopened
            .snapshot()
            .await
            .expect("checkpoint recovery")
            .get("key"),
        Some(&Bytes::from_static(b"value"))
    );
    assert_eq!(
        reopened
            .resolve(attempt.receipt())
            .await
            .expect("retained evidence"),
        Resolution::Committed { sequence: 1 }
    );
}

#[tokio::test]
async fn state_byte_limit_accepts_exact_boundary_and_rejects_overflow() {
    let limits = Limits {
        max_state_bytes: 32,
        ..Limits::default()
    };
    let store = WalStateStore::open(memory(), Path::from("domain"), limits)
        .await
        .expect("bounded state");
    let mut tx = store.begin().await.expect("boundary begin");
    tx.put("key", Bytes::from(vec![0; 29])).expect("exact size");
    committed(
        &store
            .commit(&tx.prepare().await.expect("exact prepare"))
            .await
            .expect("exact commit"),
    );
    let mut tx = store.begin().await.expect("overflow begin");
    tx.put("key", Bytes::from(vec![0; 30]))
        .expect("within batch bound");
    assert!(matches!(
        tx.prepare().await.expect_err("state bound"),
        Error::Limit { .. }
    ));
    assert_eq!(
        store
            .snapshot()
            .await
            .expect("unchanged")
            .get("key")
            .expect("key")
            .len(),
        29
    );
}

#[tokio::test]
async fn deterministic_histories_match_model_across_checkpoints_and_reopen() {
    let directory = tempfile::tempdir().expect("directory");
    let backend: Arc<dyn ObjectStore> =
        Arc::new(LocalConditionalPut::new(directory.path()).expect("local backend"));
    let mut store = open(Arc::new(ObjectStoreState::new(Arc::clone(&backend)))).await;
    let mut model = BTreeMap::<String, Bytes>::new();
    let mut snapshots: Vec<(Snapshot, BTreeMap<String, Bytes>)> = Vec::new();
    let mut seed = 17_u64;
    for step in 0..48_u64 {
        let mut tx = store.begin().await.expect("model begin");
        for _ in 0..3 {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            let key = format!("keys/{}", (seed >> 32) % 11);
            if seed.is_multiple_of(4) {
                tx.delete(&key).expect("model delete");
                model.remove(&key);
            } else {
                let value = Bytes::from(seed.to_le_bytes().to_vec());
                tx.put(&key, value.clone()).expect("model put");
                model.insert(key, value);
            }
        }
        committed(
            &store
                .commit(&tx.prepare().await.expect("model prepare"))
                .await
                .expect("model commit"),
        );
        let snapshot = store.snapshot().await.expect("model snapshot");
        let actual: BTreeMap<_, _> = snapshot
            .scan_prefix("")
            .map(|(k, v)| (k.to_owned(), v.clone()))
            .collect();
        assert_eq!(actual, model, "step {step}");
        if step % 5 == 0 {
            snapshots.push((snapshot.clone(), model.clone()));
            assert!(matches!(
                store.checkpoint(&snapshot).await.expect("model checkpoint"),
                WriteOutcome::Applied
            ));
            store = open(Arc::new(ObjectStoreState::new(Arc::clone(&backend)))).await;
        }
    }
    for (snapshot, expected) in snapshots {
        assert_eq!(
            snapshot
                .scan_prefix("")
                .map(|(k, v)| (k.to_owned(), v.clone()))
                .collect::<BTreeMap<_, _>>(),
            expected
        );
    }
}

#[tokio::test]
async fn checkpoints_span_pages_and_preserve_boundary_values() {
    let backend = memory();
    let store = open(Arc::clone(&backend)).await;
    let mut model = BTreeMap::new();
    for batch in 0..5 {
        let mut tx = store.begin().await.expect("begin page batch");
        for offset in 0..3 {
            let key = format!("page/{batch}/{offset}");
            let value = Bytes::from(vec![255; 32 * 1024]);
            tx.put(&key, value.clone()).expect("page value");
            model.insert(key, value);
        }
        committed(
            &store
                .commit(&tx.prepare().await.expect("page prepare"))
                .await
                .expect("page commit"),
        );
    }
    let mut boundary = store.begin().await.expect("boundary begin");
    let large = Bytes::from(vec![255; 128 * 1024 - 1]);
    boundary
        .put("k", large.clone())
        .expect("exact batch byte limit");
    committed(
        &store
            .commit(&boundary.prepare().await.expect("boundary prepare"))
            .await
            .expect("boundary commit"),
    );
    model.insert("k".to_owned(), large);
    let snapshot = store.snapshot().await.expect("checkpoint input");
    assert!(matches!(
        store
            .checkpoint(&snapshot)
            .await
            .expect("multi-page checkpoint"),
        WriteOutcome::Applied
    ));
    let reopened = open(backend).await;
    let recovered = reopened.snapshot().await.expect("recover checkpoint pages");
    assert_eq!(
        recovered
            .scan_prefix("")
            .map(|(k, v)| (k.to_owned(), v.clone()))
            .collect::<BTreeMap<_, _>>(),
        model
    );
    let delete = {
        let mut tx = reopened.begin().await.expect("delete after checkpoint");
        tx.delete("k").expect("delete boundary key");
        tx.prepare().await.expect("delete prepare")
    };
    committed(&reopened.commit(&delete).await.expect("delete"));
    assert!(
        reopened
            .snapshot()
            .await
            .expect("replay deletion")
            .get("k")
            .is_none()
    );
    assert!(snapshot.get("k").is_some());
}

#[test]
#[ignore = "subprocess helper invoked by process_restart_at_wal_publication_boundaries"]
fn subprocess_writer() {
    let directory = std::env::var("SPICE_WAL_TEST_DIRECTORY").expect("subprocess directory");
    let mode: u8 = std::env::var("SPICE_WAL_TEST_BOUNDARY")
        .expect("boundary")
        .parse()
        .expect("boundary number");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("child runtime");
    let backend: Arc<dyn StateStore> = Arc::new(ObjectStoreState::new(Arc::new(
        LocalConditionalPut::new(&directory).expect("child local store"),
    )));
    let faults = Arc::new(Faults::default());
    let wrapper: Arc<dyn StateStore> = Arc::new(FaultStore {
        inner: backend,
        faults: Arc::clone(&faults),
    });
    let store = runtime.block_on(open(wrapper));
    let attempt = runtime.block_on(prepare(&store, "key", b"survives process exit"));
    std::fs::write(
        std::path::Path::new(&directory).join("receipt.json"),
        serde_json::to_vec(attempt.receipt()).expect("serialize receipt"),
    )
    .expect("save before dispatch");
    faults.mode.store(mode, Ordering::SeqCst);
    let outcome = runtime
        .block_on(store.commit(&attempt))
        .expect("child exits inside publication boundary");
    panic!("fault injection did not exit the subprocess: {outcome:?}");
}

#[test]
fn process_restart_at_wal_publication_boundaries() {
    for mode in [7, 8] {
        let directory = tempfile::tempdir().expect("process directory");
        let output = std::process::Command::new(std::env::current_exe().expect("test executable"))
            .args(["--exact", "subprocess_writer", "--ignored", "--nocapture"])
            .env("SPICE_WAL_TEST_DIRECTORY", directory.path())
            .env("SPICE_WAL_TEST_BOUNDARY", mode.to_string())
            .output()
            .expect("run child process");
        assert_eq!(
            output.status.code(),
            Some(86),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let receipt: CommitReceipt = serde_json::from_slice(
            &std::fs::read(directory.path().join("receipt.json")).expect("saved receipt"),
        )
        .expect("restore child receipt");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("recovery runtime");
        runtime.block_on(async {
            let store = open(Arc::new(ObjectStoreState::new(Arc::new(
                LocalConditionalPut::new(directory.path()).expect("recovery store"),
            ))))
            .await;
            let snapshot = store.snapshot().await.expect("recovery after abrupt exit");
            let outcome = store.resolve(&receipt).await.expect("resolve child");
            if mode == 7 {
                assert_eq!(snapshot.sequence(), 0);
                assert!(snapshot.get("key").is_none());
                assert_eq!(outcome, Resolution::Pending);
            } else {
                assert_eq!(snapshot.sequence(), 1);
                assert_eq!(
                    snapshot.get("key"),
                    Some(&Bytes::from_static(b"survives process exit"))
                );
                assert_eq!(outcome, Resolution::Committed { sequence: 1 });
            }
            println!(
                "process exit boundary={mode}: sequence={}, outcome={outcome:?}",
                snapshot.sequence()
            );
        });
    }
}

async fn replace_json(state: &dyn StateStore, key: &Path, value: &serde_json::Value) {
    let previous = state
        .read(key)
        .await
        .expect("read record to damage")
        .expect("record exists");
    assert!(matches!(
        state
            .compare_exchange(
                key,
                ExpectedRevision::Exact(&previous.revision),
                StateChange::Set(Bytes::from(
                    serde_json::to_vec(value).expect("encode injected record")
                )),
                WriteId::new(),
            )
            .await
            .expect("replace damaged record"),
        WriteOutcome::Applied
    ));
}

fn referenced_key(kind: &str, reference: &serde_json::Value) -> Path {
    let id: [u8; 16] = serde_json::from_value(reference["id"].clone()).expect("object identity");
    Path::from(format!("domain/{kind}/{}", uuid::Uuid::from_bytes(id)))
}

async fn json_record(state: &dyn StateStore, key: &Path) -> serde_json::Value {
    let record = state
        .read(key)
        .await
        .expect("read JSON record")
        .expect("record");
    serde_json::from_slice(&record.value.expect("record value")).expect("decode JSON record")
}

#[tokio::test]
async fn missing_and_corrupt_checkpoint_artifacts_fail_closed() {
    for kind in ["pages", "checkpoints"] {
        for missing in [true, false] {
            let state = memory();
            let store = open(Arc::clone(&state)).await;
            let attempt = prepare(&store, "key", b"value").await;
            committed(&store.commit(&attempt).await.expect("commit"));
            let retained = store.snapshot().await.expect("retained snapshot");
            assert!(matches!(
                store.checkpoint(&retained).await.expect("checkpoint"),
                WriteOutcome::Applied
            ));
            let head = json_record(state.as_ref(), &Path::from("domain/head")).await;
            let manifest_key = referenced_key("checkpoints", &head["checkpoint"]["object"]);
            let target = if kind == "pages" {
                let checkpoint = json_record(state.as_ref(), &manifest_key).await;
                referenced_key("pages", &checkpoint["pages"][0])
            } else {
                manifest_key
            };
            let record = state
                .read(&target)
                .await
                .expect("target read")
                .expect("target");
            let damage = if missing {
                StateChange::Tombstone
            } else {
                StateChange::Set(Bytes::from_static(b"damaged"))
            };
            assert!(matches!(
                state
                    .compare_exchange(
                        &target,
                        ExpectedRevision::Exact(&record.revision),
                        damage,
                        WriteId::new()
                    )
                    .await
                    .expect("inject damage"),
                WriteOutcome::Applied
            ));
            let reopened = open(Arc::clone(&state)).await;
            assert!(matches!(
                reopened
                    .snapshot()
                    .await
                    .expect_err("do not expose partial state"),
                Error::InvalidRecord { .. }
            ));
            assert_eq!(retained.get("key"), Some(&Bytes::from_static(b"value")));
            println!(
                "checkpoint artifact={kind}, missing={missing}: recovery rejected, retained snapshot unchanged"
            );
        }
    }
}

#[tokio::test]
async fn inconsistent_checkpoint_metadata_fails_even_with_valid_content_digest() {
    for damage in [
        "sequence",
        "incarnation",
        "commit",
        "duplicate_page",
        "reversed_pages",
        "totals",
    ] {
        let state = memory();
        let store = open(Arc::clone(&state)).await;
        for key in ["a", "b"] {
            let mut tx = store.begin().await.expect("begin");
            tx.put(key, Bytes::from(vec![1; 80 * 1024]))
                .expect("page-sized value");
            committed(
                &store
                    .commit(&tx.prepare().await.expect("prepare"))
                    .await
                    .expect("commit"),
            );
        }
        let snapshot = store.snapshot().await.expect("snapshot");
        assert!(matches!(
            store.checkpoint(&snapshot).await.expect("checkpoint"),
            WriteOutcome::Applied
        ));
        let head_key = Path::from("domain/head");
        let mut head = json_record(state.as_ref(), &head_key).await;
        let key = referenced_key("checkpoints", &head["checkpoint"]["object"]);
        let mut checkpoint = json_record(state.as_ref(), &key).await;
        match damage {
            "sequence" => checkpoint["sequence"] = serde_json::json!(1),
            "incarnation" => checkpoint["incarnation"] = serde_json::json!(vec![0; 16]),
            "commit" => checkpoint["commit"] = serde_json::Value::Null,
            "duplicate_page" => {
                let first = checkpoint["pages"][0].clone();
                checkpoint["pages"]
                    .as_array_mut()
                    .expect("pages")
                    .push(first);
            }
            "reversed_pages" => checkpoint["pages"].as_array_mut().expect("pages").reverse(),
            "totals" => checkpoint["keys"] = serde_json::json!(0),
            _ => panic!("unknown test damage"),
        }
        let bytes = serde_json::to_vec(&checkpoint).expect("damaged manifest");
        head["checkpoint"]["object"]["digest"] = serde_json::json!(blake3::hash(&bytes).as_bytes());
        replace_json(state.as_ref(), &key, &checkpoint).await;
        replace_json(state.as_ref(), &head_key, &head).await;
        assert!(matches!(
            store
                .snapshot()
                .await
                .expect_err("reject inconsistent manifest"),
            Error::InvalidRecord { .. }
        ));
        println!("checkpoint metadata={damage}: recovery rejected");
    }
}
