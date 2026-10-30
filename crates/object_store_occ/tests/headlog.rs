// Copyright 2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Protocol histories through actual memory, durable-local, and optional S3 stores.
#![expect(
    clippy::expect_used,
    reason = "integration assertions identify failed operations"
)]

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};
use object_store_occ::headlog::{
    HeadLog, HeadLogConfig, Outcome, Poll, PreparedAttempt, Reader, Record, Writer,
};
use object_store_occ::{Expected, LocalConditionalPut, conditional_put};
use prost::Message;

fn config() -> HeadLogConfig {
    HeadLogConfig {
        head_budget_bytes: 4096,
        batch_budget_bytes: 180,
        batch_max_records: 4,
        max_record_bytes: 100,
        // headlog_500_records_cold_open_and_lagging_poll lists 125 batches.
        max_listed_batches: 256,
        spill_above_bytes: 100,
    }
}

struct Backend {
    store: Arc<dyn ObjectStore>,
    _directory: Option<tempfile::TempDir>,
    prefix: Path,
}

fn backends() -> Vec<Backend> {
    let directory = tempfile::tempdir().expect("tempdir");
    let local = LocalConditionalPut::new_durable(directory.path()).expect("durable local store");
    let mut result = vec![
        Backend {
            store: Arc::new(object_store::memory::InMemory::new()),
            _directory: None,
            prefix: Path::from("memory"),
        },
        Backend {
            store: Arc::new(local),
            _directory: Some(directory),
            prefix: Path::from("local"),
        },
    ];
    if let Ok(bucket) = std::env::var("AWS_S3_BUCKET") {
        let store = object_store::aws::AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .build()
            .expect("S3 config");
        let base = std::env::var("AWS_S3_PREFIX").unwrap_or_default();
        result.push(Backend {
            store: Arc::new(store),
            _directory: None,
            prefix: Path::from(format!(
                "{}/headlog-test/{}",
                base.trim_matches('/'),
                uuid::Uuid::new_v4()
            )),
        });
    }
    result
}

async fn writer(backend: &Backend) -> Writer {
    HeadLog::create(Arc::clone(&backend.store), backend.prefix.clone(), config())
        .await
        .expect("create");
    Writer::take_over(
        Arc::clone(&backend.store),
        backend.prefix.clone(),
        config(),
        1,
    )
    .await
    .expect("take over")
}

async fn append(writer: &mut Writer, payload: Bytes) -> PreparedAttempt {
    let attempt = writer.prepare(payload).await.expect("prepare");
    // The persisted receipt is the sole input to send, including after a restart.
    let receipt =
        serde_json::from_slice(&serde_json::to_vec(&attempt).expect("persist")).expect("recover");
    let next = writer.state().head.commit + 1;
    assert_eq!(
        writer.commit(&receipt).await.expect("commit"),
        Outcome::Accepted { commit: next }
    );
    receipt
}

#[tokio::test]
async fn headlog_500_records_cold_open_and_lagging_poll() {
    for backend in backends() {
        let mut writer = writer(&backend).await;
        let (mut reader, empty) = Reader::open(Arc::clone(&backend.store), backend.prefix.clone())
            .await
            .expect("empty open");
        assert!(empty.records.is_empty());
        assert_eq!(reader.poll().await.expect("304"), Poll::Unchanged);
        let mut expected = Vec::new();
        let mut seen = Vec::new();
        for index in 0..500u64 {
            let payload = Bytes::from(format!(
                "{index:04}:{}",
                "x".repeat(usize::try_from(index % 65).expect("small remainder"))
            ));
            append(&mut writer, payload.clone()).await;
            expected.push(payload);
            if index % 73 == 72 || index == 499 {
                let Poll::Records(records) = reader.poll().await.expect("lagging poll") else {
                    panic!("new records required")
                };
                seen.extend(records.into_iter().map(|entry| entry.record.payload));
            }
        }
        let (_, opened) = Reader::open(Arc::clone(&backend.store), backend.prefix.clone())
            .await
            .expect("cold open");
        assert_eq!(opened.records.len(), 500);
        for (index, entry) in opened.records.iter().enumerate() {
            assert_eq!(entry.commit, index as u64 + 1);
        }
        assert_eq!(
            opened
                .records
                .into_iter()
                .map(|entry| entry.record.payload)
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(seen, expected);
        assert_eq!(reader.poll().await.expect("304"), Poll::Unchanged);
        println!(
            "{}: 500 ordered records, cold open and lagging polls agree",
            backend.prefix
        );
    }
}

#[derive(Debug)]
struct FaultStore {
    inner: Arc<dyn ObjectStore>,
    mode: AtomicU8,
    gets: AtomicUsize,
    sends: parking_lot::Mutex<Vec<(Bytes, String)>>,
}
impl fmt::Display for FaultStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "headlog fault store")
    }
}
fn lost() -> object_store::Error {
    object_store::Error::Generic {
        store: "headlog test",
        source: "injected response loss".into(),
    }
}
#[async_trait]
impl ObjectStore for FaultStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        if location.as_ref().ends_with("/head") && matches!(options.mode, PutMode::Update(_)) {
            let bytes = payload
                .iter()
                .flat_map(|chunk| chunk.iter().copied())
                .collect::<Vec<_>>();
            self.sends
                .lock()
                .push((bytes.into(), format!("{:?}", options.mode)));
            let mode = self.mode.swap(0, Ordering::SeqCst);
            if mode == 2 {
                return Err(lost());
            }
            if mode == 3 {
                self.mode.store(4, Ordering::SeqCst);
                return Err(lost());
            }
            if mode == 1 || mode == 5 {
                self.inner.put_opts(location, payload, options).await?;
                return Err(if mode == 5 {
                    object_store::Error::Precondition {
                        path: location.to_string(),
                        source: "accepted but SDK retry got 412".into(),
                    }
                } else {
                    lost()
                });
            }
        }
        self.inner.put_opts(location, payload, options).await
    }
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        if self.mode.load(Ordering::SeqCst) == 4 && location.as_ref().ends_with("/head") {
            return Err(lost());
        }
        self.inner.get_opts(location, options).await
    }
    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(paths)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[tokio::test]
async fn headlog_uncertain_responses_identical_retry_and_restart() {
    for backend in backends() {
        let fault = Arc::new(FaultStore {
            inner: Arc::clone(&backend.store),
            mode: AtomicU8::new(0),
            gets: AtomicUsize::new(0),
            sends: parking_lot::Mutex::new(Vec::new()),
        });
        let store = Arc::clone(&fault);
        let store: Arc<dyn ObjectStore> = store;
        HeadLog::create(Arc::clone(&store), backend.prefix.clone(), config())
            .await
            .expect("create");
        let mut writer = Writer::take_over(Arc::clone(&store), backend.prefix.clone(), config(), 1)
            .await
            .expect("owner");
        for (index, mode) in [1, 2, 5, 0].into_iter().enumerate() {
            let attempt = writer
                .prepare(Bytes::from_static(b"one logical write"))
                .await
                .expect("prepare");
            fault.mode.store(mode, Ordering::SeqCst);
            let gets = fault.gets.load(Ordering::SeqCst);
            let sends = fault.sends.lock().len();
            assert_eq!(
                writer.commit(&attempt).await.expect("resolved"),
                Outcome::Accepted {
                    commit: index as u64 + 1
                }
            );
            if mode == 0 {
                assert_eq!(
                    fault.gets.load(Ordering::SeqCst),
                    gets,
                    "own success performs no GET"
                );
            }
            if mode == 2 {
                let sent = fault.sends.lock();
                assert_eq!(
                    sent[sends],
                    sent[sends + 1],
                    "retry uses IDENTICAL bytes AND condition"
                );
            }
            assert_eq!(
                writer.resolve(&attempt).await.expect("repeat resolution"),
                Outcome::Accepted {
                    commit: index as u64 + 1
                }
            );
        }
        let attempt = writer
            .prepare(Bytes::from_static(b"restart receipt"))
            .await
            .expect("prepare restart");
        let saved = serde_json::to_vec(&attempt).expect("persist before send");
        fault.mode.store(3, Ordering::SeqCst);
        let sends = fault.sends.lock().len();
        writer
            .commit(&attempt)
            .await
            .expect_err("unreadable head leaves undecided");
        assert_eq!(
            fault.sends.lock().len(),
            sends + 1,
            "no retry while head unreadable"
        );
        writer
            .prepare(Bytes::new())
            .await
            .expect_err("cannot replace undecided write");
        fault.mode.store(0, Ordering::SeqCst);
        let mut resumed = Writer::resume(Arc::clone(&store), backend.prefix.clone(), config(), 1)
            .await
            .expect("resume");
        let receipt = serde_json::from_slice(&saved).expect("recover exact receipt");
        fault.mode.store(4, Ordering::SeqCst);
        resumed
            .resolve(&receipt)
            .await
            .expect_err("restart resolution remains undecided");
        resumed
            .prepare(Bytes::new())
            .await
            .expect_err("recovered undecided receipt blocks replacement");
        fault.mode.store(0, Ordering::SeqCst);
        assert_eq!(
            resumed.resolve(&receipt).await.expect("resolve restart"),
            Outcome::Accepted { commit: 5 }
        );
        {
            let sent = fault.sends.lock();
            assert_eq!(sent[sends], *sent.last().expect("retry"));
        }
        let (_, opened) = Reader::open(store, backend.prefix).await.expect("open");
        assert_eq!(opened.records.len(), 5, "no duplicate applications");
    }
}

#[tokio::test]
async fn headlog_takeover_fences_stale_owner_and_preserves_accepted_evidence() {
    for backend in backends() {
        let mut first = writer(&backend).await;
        let accepted = append(&mut first, Bytes::from_static(b"before takeover")).await;
        let stale = first
            .prepare(Bytes::from_static(b"must never land"))
            .await
            .expect("stale proposal");
        let mut second = Writer::take_over(
            Arc::clone(&backend.store),
            backend.prefix.clone(),
            config(),
            2,
        )
        .await
        .expect("new owner");
        assert_eq!(
            first.commit(&stale).await.expect("fenced"),
            Outcome::Fenced { head_lease: 2 }
        );
        let mut recovered_old = Writer::resume(
            Arc::clone(&backend.store),
            backend.prefix.clone(),
            config(),
            1,
        )
        .await
        .expect("recover old generation for resolution only");
        assert_eq!(
            recovered_old
                .resolve(&accepted)
                .await
                .expect("resolve historical receipt after restart and takeover"),
            Outcome::Accepted { commit: 1 }
        );
        assert_eq!(
            recovered_old
                .resolve(&stale)
                .await
                .expect("resolve stale receipt after restart and takeover"),
            Outcome::Fenced { head_lease: 2 }
        );
        recovered_old
            .prepare(Bytes::new())
            .await
            .expect_err("recovered fenced generation cannot prepare");

        first
            .prepare(Bytes::new())
            .await
            .expect_err("operation must fail");
        assert_eq!(
            first.resolve(&accepted).await.expect("prior acceptance"),
            Outcome::Accepted { commit: 1 }
        );
        append(&mut second, Bytes::from_static(b"after takeover")).await;
        let (_, opened) = Reader::open(Arc::clone(&backend.store), backend.prefix)
            .await
            .expect("open");
        assert_eq!(
            opened
                .records
                .iter()
                .map(|r| r.record.generation)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
    }
}

#[tokio::test]
async fn headlog_checkpoint_publishes_on_next_commit_and_requires_reopen() {
    for backend in backends() {
        let mut writer = writer(&backend).await;
        let (mut lagging, _) = Reader::open(Arc::clone(&backend.store), backend.prefix.clone())
            .await
            .expect("reader");
        let oldest = append(&mut writer, Bytes::from_static(b"oldest")).await;
        for _ in 0..19 {
            append(&mut writer, Bytes::from_static(b"later")).await;
        }
        let floor = writer.state().head.batches[1];
        writer
            .publish_checkpoint(floor - 1, Bytes::new())
            .await
            .expect_err("no split batches");
        writer
            .publish_checkpoint(floor, Bytes::from_static(b"caller state at floor"))
            .await
            .expect("stage checkpoint");
        let (_, before) = Reader::open(Arc::clone(&backend.store), backend.prefix.clone())
            .await
            .expect("unpublished root ignored");
        assert_eq!(before.floor, 0);
        append(&mut writer, Bytes::from_static(b"publishes floor")).await;
        assert_eq!(writer.state().head.floor, floor);
        assert_eq!(lagging.poll().await.expect("behind floor"), Poll::Reopen);
        let (_, opened) = Reader::open(Arc::clone(&backend.store), backend.prefix.clone())
            .await
            .expect("checkpoint cold open");
        assert_eq!(
            opened.checkpoint_root,
            Some(Bytes::from_static(b"caller state at floor"))
        );
        assert_eq!(opened.records.first().expect("tail").commit, floor + 1);
        assert_eq!(opened.records.len() as u64, 21 - floor);
        assert_eq!(
            writer.resolve(&oldest).await.expect("retained evidence"),
            Outcome::Accepted { commit: 1 }
        );
        let checkpoint_path = backend
            .prefix
            .clone()
            .join("checkpoints")
            .join(format!("{floor:020}"));
        backend
            .store
            .delete(&checkpoint_path)
            .await
            .expect("remove referenced root");
        Reader::open(Arc::clone(&backend.store), backend.prefix)
            .await
            .expect_err("operation must fail");
    }
}

#[tokio::test]
async fn headlog_missing_listed_batch_is_an_error_for_open_and_poll() {
    for backend in backends() {
        let mut writer = writer(&backend).await;
        let (mut reader, _) = Reader::open(Arc::clone(&backend.store), backend.prefix.clone())
            .await
            .expect("reader");
        for _ in 0..10 {
            append(&mut writer, Bytes::from_static(b"record")).await;
        }
        let path = backend
            .prefix
            .clone()
            .join("wal")
            .join("00000000000000000001");
        backend
            .store
            .delete(&path)
            .await
            .expect("remove listed batch");
        Reader::open(Arc::clone(&backend.store), backend.prefix)
            .await
            .expect_err("operation must fail");
        reader.poll().await.expect_err("operation must fail");
        assert_eq!(reader.state().head.commit, 0, "failed poll keeps cursor");
    }
}

#[tokio::test]
async fn headlog_creates_each_sealed_batch_once() {
    for backend in backends() {
        let store = Arc::new(object_store_occ::sim::fault::FaultStore::new(Arc::clone(
            &backend.store,
        )));
        let shared: Arc<dyn ObjectStore> = Arc::clone(&store) as Arc<dyn ObjectStore>;
        HeadLog::create(Arc::clone(&shared), backend.prefix.clone(), config())
            .await
            .expect("create");
        let mut writer = Writer::take_over(shared, backend.prefix.clone(), config(), 1)
            .await
            .expect("take over");
        for i in 0..40u8 {
            append(&mut writer, Bytes::from(vec![i; 20])).await;
        }
        writer.wait_for_sealing().await;
        assert!(
            writer.sealing_error().is_none(),
            "{:?}",
            writer.sealing_error()
        );
        let creates = store
            .log()
            .into_iter()
            .filter(|applied| {
                applied.op == object_store_occ::sim::Op::Create
                    && applied.path.as_ref().contains("/wal/")
            })
            .map(|applied| applied.path.to_string())
            .collect::<Vec<_>>();
        let batches = creates.iter().collect::<std::collections::BTreeSet<_>>();
        assert!(batches.len() >= 8, "too few sealed batches: {batches:?}");
        assert_eq!(
            creates.len(),
            batches.len(),
            "one create per sealed batch, got {creates:?}"
        );
    }
}

#[derive(Clone, PartialEq, Message)]
struct TestBatch {
    #[prost(bytes = "vec", repeated, tag = "1")]
    records: Vec<Vec<u8>>,
}

#[tokio::test]
async fn headlog_copy_preserves_original_unknown_fields_and_checks_duplicates() {
    for backend in backends() {
        let mut writer = writer(&backend).await;
        for _ in 0..3 {
            append(&mut writer, Bytes::from_static(b"original")).await;
        }
        let mut head = writer.state().head.clone();
        // Advisory field 100 is not understood by this binary. Re-encoding the
        // record would drop it; a sealer must copy the original bytes instead.
        head.window[0]
            .encoded
            .extend_from_slice(&[0xa0, 0x06, 0x07]);
        conditional_put(
            backend.store.as_ref(),
            &backend.prefix.clone().join("head"),
            head.encode_to_vec().into(),
            &Expected::Version(writer.state().version.clone()),
        )
        .await
        .expect("publish advisory field");
        let original_first = head.window[0].encoded.clone();
        let mut writer = Writer::resume(
            Arc::clone(&backend.store),
            backend.prefix.clone(),
            config(),
            1,
        )
        .await
        .expect("resume observed bytes");
        let batch_path = backend
            .prefix
            .clone()
            .join("wal")
            .join("00000000000000000001");
        append(&mut writer, Bytes::from_static(b"fourth")).await;
        writer.wait_for_sealing().await;
        let original = writer
            .state()
            .head
            .window
            .iter()
            .map(|r| r.encoded.clone())
            .collect::<Vec<_>>();
        assert_eq!(original[0], original_first);
        let correct = TestBatch {
            records: original.clone(),
        }
        .encode_to_vec();
        assert_eq!(
            backend
                .store
                .get(&batch_path)
                .await
                .expect("inline copy")
                .bytes()
                .await
                .expect("bytes"),
            correct
        );
        // Next prepare encounters AlreadyExists and must verify identical bytes.
        append(&mut writer, Bytes::from_static(b"next")).await;
        let bytes = backend
            .store
            .get(&batch_path)
            .await
            .expect("sealed batch")
            .bytes()
            .await
            .expect("bytes");
        assert_eq!(
            TestBatch::decode(bytes).expect("decode batch").records,
            original
        );
        // A different immutable object cannot be silently accepted as a copy.
        for _ in 0..2 {
            append(&mut writer, Bytes::from_static(b"more")).await;
        }
        let next = writer.state().head.commit - writer.state().head.window.len() as u64 + 1;
        let path = backend
            .prefix
            .clone()
            .join("wal")
            .join(format!("{next:020}"));
        conditional_put(
            backend.store.as_ref(),
            &path,
            Bytes::from_static(b"different").into(),
            &Expected::Absent,
        )
        .await
        .expect("inject mismatched batch");
        append(
            &mut writer,
            Bytes::from_static(b"finalizes despite copy failure"),
        )
        .await;
        writer.wait_for_sealing().await;
        assert!(
            writer.sealing_error().is_some(),
            "post-acceptance copy error is observable"
        );
        let before = writer.state().head.commit;
        writer
            .prepare(Bytes::from_static(b"cannot drop"))
            .await
            .expect_err("operation must fail");
        assert_eq!(writer.state().head.commit, before);
    }
}

#[tokio::test]
async fn headlog_invalid_config_record_limit_and_unknown_features_fail_closed() {
    for backend in backends() {
        let bad = HeadLogConfig {
            head_budget_bytes: 200,
            ..config()
        };
        HeadLog::create(Arc::clone(&backend.store), backend.prefix.clone(), bad)
            .await
            .expect_err("operation must fail");
        let mut writer = writer(&backend).await;
        writer
            .prepare(Bytes::from(vec![0; 101]))
            .await
            .expect_err("operation must fail");
        let mut head = writer.state().head.clone();
        head.features.push("unknown-required-action".into());
        conditional_put(
            backend.store.as_ref(),
            &backend.prefix.clone().join("head"),
            head.encode_to_vec().into(),
            &Expected::Version(writer.state().version.clone()),
        )
        .await
        .expect("newer format");
        Reader::open(Arc::clone(&backend.store), backend.prefix)
            .await
            .expect_err("operation must fail");
    }
}

#[tokio::test]
async fn headlog_resolution_rejected_and_below_floor_without_evidence_unresolved() {
    for backend in backends() {
        let mut first = writer(&backend).await;
        let rejected = first
            .prepare(Bytes::from_static(b"loser"))
            .await
            .expect("prepare loser");
        // Two handles in one externally arbitrated generation deliberately race
        // the same version to exercise OCC rejection independently of fencing.
        let mut winner = Writer::resume(
            Arc::clone(&backend.store),
            backend.prefix.clone(),
            config(),
            1,
        )
        .await
        .expect("same owner handle");
        append(&mut winner, Bytes::from_static(b"winner")).await;
        assert_eq!(
            first.resolve(&rejected).await.expect("different record"),
            Outcome::Rejected
        );
        for _ in 0..11 {
            append(&mut winner, Bytes::from_static(b"tail")).await;
        }
        let floor = winner.state().head.batches[0];
        winner
            .publish_checkpoint(floor, Bytes::from_static(b"snapshot"))
            .await
            .expect("checkpoint");
        append(&mut winner, Bytes::from_static(b"publish floor")).await;
        assert_eq!(
            first.resolve(&rejected).await.expect("evidence expired"),
            Outcome::Unresolved
        );
    }
}

#[derive(Clone, PartialEq, Message)]
struct TestGroup {
    #[prost(bytes = "bytes", repeated, tag = "1")]
    payloads: Vec<Bytes>,
}

#[tokio::test]
async fn headlog_group_persists_before_send_and_upload_is_immutable() {
    for backend in backends() {
        let mut writer = writer(&backend).await;
        let reference = writer
            .upload("pages/a", Bytes::from_static(b"page"))
            .await
            .expect("upload");
        assert!(
            reference
                .path
                .as_ref()
                .ends_with("/objects/00000000000000000001/pages/a")
        );
        let again = writer
            .upload("pages/a", Bytes::from_static(b"page"))
            .await
            .expect("identical create");
        assert_eq!(again, reference);
        writer
            .upload("pages/a", Bytes::from_static(b"different"))
            .await
            .expect_err("immutable conflict");
        let payloads = vec![
            Bytes::new(),
            Bytes::from_static(b"first"),
            Bytes::from_static(b"second"),
        ];
        let mut saved = Vec::new();
        let outcome = writer
            .commit_group(payloads.clone(), |attempt| {
                saved = serde_json::to_vec(attempt).expect("caller persists receipt");
                Ok(())
            })
            .await
            .expect("group commit");
        assert!(!saved.is_empty());
        assert_eq!(outcome, Outcome::Accepted { commit: 1 });
        let (_, opened) = Reader::open(Arc::clone(&backend.store), backend.prefix.clone())
            .await
            .expect("group open");
        assert_eq!(
            opened.records.len(),
            1,
            "one CAS record for the whole queue"
        );
        assert_eq!(
            TestGroup::decode(opened.records[0].record.payload.clone())
                .expect("group payload")
                .payloads,
            payloads
        );
        let pending = writer
            .prepare(Bytes::from_static(b"undecided"))
            .await
            .expect("pending attempt");
        let accepted: PreparedAttempt =
            serde_json::from_slice(&saved).expect("old accepted receipt");
        writer
            .resolve(&accepted)
            .await
            .expect_err("old receipt must not clear a different pending attempt");
        assert_eq!(
            writer.commit(&pending).await.expect("pending acceptance"),
            Outcome::Accepted { commit: 2 }
        );
    }
}

#[tokio::test]
async fn headlog_byte_cut_waits_for_published_successor_and_exact_record_limit() {
    for backend in backends() {
        HeadLog::create(Arc::clone(&backend.store), backend.prefix.clone(), config())
            .await
            .expect("create");
        let byte_config = HeadLogConfig {
            batch_max_records: 64,
            ..config()
        };
        let mut writer = Writer::take_over(
            Arc::clone(&backend.store),
            backend.prefix.clone(),
            byte_config,
            1,
        )
        .await
        .expect("owner");
        // 18 bytes attempt id + 2 generation + 2 payload header + 78 payload = R.
        append(&mut writer, Bytes::from(vec![7; 78])).await;
        let batch_path = backend
            .prefix
            .clone()
            .join("wal")
            .join("00000000000000000001");
        backend
            .store
            .get(&batch_path)
            .await
            .expect_err("a one-record non-final batch must not be sealed");
        let attempt = writer
            .prepare(Bytes::from(vec![9; 78]))
            .await
            .expect("second exact-limit record");
        backend
            .store
            .get(&batch_path)
            .await
            .expect_err("planned successor must not finalize batch");
        assert_eq!(
            writer.commit(&attempt).await.expect("publish successor"),
            Outcome::Accepted { commit: 2 }
        );
        writer.wait_for_sealing().await;
        let bytes = backend
            .store
            .get(&batch_path)
            .await
            .expect("now final")
            .bytes()
            .await
            .expect("batch bytes");
        let batch = TestBatch::decode(bytes).expect("batch");
        assert_eq!(batch.records.len(), 1, "two maximum records exceed B");
        assert_eq!(batch.records[0].len(), 100);
        writer
            .prepare(Bytes::from(vec![0; 79]))
            .await
            .expect_err("R + 1 must fail");
    }
}

#[tokio::test]
async fn headlog_budget_below_b_plus_r_plus_m_is_refused_at_construction() {
    for backend in backends() {
        // H >= B + R alone: a third maximum record cannot fit beside the
        // head's own fields, record envelopes and batch index.
        let bounds = HeadLogConfig {
            head_budget_bytes: 300,
            batch_budget_bytes: 200,
            max_record_bytes: 100,
            batch_max_records: 64,
            max_listed_batches: 16,
            spill_above_bytes: 100,
        };
        let error = HeadLog::create(Arc::clone(&backend.store), backend.prefix.clone(), bounds)
            .await
            .expect_err("H = 300 is below B + R + M");
        let overhead = bounds.head_overhead_bytes().expect("M");
        assert_eq!(
            error.to_string(),
            format!(
                "Invalid head log: head budget 300 is below B + R + M = 200 + 100 + {overhead}; \
                 raise head_budget_bytes or lower the batch or record budget"
            )
        );
        let feasible = HeadLogConfig {
            head_budget_bytes: 300 + overhead,
            ..bounds
        };
        HeadLog::create(Arc::clone(&backend.store), backend.prefix.clone(), feasible)
            .await
            .expect("H = B + R + M");
        Writer::take_over(
            Arc::clone(&backend.store),
            backend.prefix.clone(),
            bounds,
            1,
        )
        .await
        .err()
        .expect("a writer refuses the same infeasible budgets");
        let mut writer = Writer::take_over(
            Arc::clone(&backend.store),
            backend.prefix.clone(),
            feasible,
            1,
        )
        .await
        .expect("owner");
        // 78 payload bytes encode to a 100-byte record: every record is R.
        for _ in 0..20 {
            append(&mut writer, Bytes::from(vec![0; 78])).await;
        }
        let head = &writer.state().head;
        assert_eq!(head.commit, 20);
        assert_eq!(
            head.batches.len() * 2 + head.window.len(),
            20,
            "two maximum records per batch"
        );
    }
}

#[tokio::test]
async fn headlog_spills_large_records_and_verifies_them_on_read() {
    for backend in backends() {
        let spilling = HeadLogConfig {
            spill_above_bytes: 70,
            ..config()
        };
        HeadLog::create(Arc::clone(&backend.store), backend.prefix.clone(), spilling)
            .await
            .expect("create");
        let mut writer = Writer::take_over(
            Arc::clone(&backend.store),
            backend.prefix.clone(),
            spilling,
            1,
        )
        .await
        .expect("owner");
        // Inline below the threshold, spilled above it, and spilled far above R.
        let payloads = [vec![1_u8; 10], vec![2; 78], vec![3; 10_000]];
        for payload in &payloads {
            append(&mut writer, Bytes::from(payload.clone())).await;
        }
        let spilled = writer
            .state()
            .head
            .window
            .iter()
            .map(|r| {
                let record = Record::decode(r.encoded.as_slice()).expect("record");
                (
                    record.spilled.is_some(),
                    record.payload.len(),
                    r.encoded.len(),
                )
            })
            .collect::<Vec<_>>();
        // The reference record encodes to 58 bytes: id, generation, digest, length.
        assert_eq!(
            spilled,
            vec![(false, 10, 32), (true, 0, 58), (true, 0, 59)],
            "window keeps references, not payloads"
        );
        let digest = blake3::hash(&payloads[2]);
        let path = backend
            .prefix
            .clone()
            .join("objects")
            .join("00000000000000000001")
            .join("records")
            .join(digest.to_hex().as_str());
        assert_eq!(
            backend
                .store
                .get(&path)
                .await
                .expect("spilled object")
                .bytes()
                .await
                .expect("bytes"),
            payloads[2]
        );
        let (_, opened) = Reader::open(Arc::clone(&backend.store), backend.prefix.clone())
            .await
            .expect("open");
        assert_eq!(
            opened
                .records
                .iter()
                .map(|r| r.record.payload.to_vec())
                .collect::<Vec<_>>(),
            payloads.to_vec()
        );
        // A spilled object whose bytes no longer match the reference is an error,
        // never a wrong payload.
        backend
            .store
            .put(&path, Bytes::from(vec![4_u8; 10_000]).into())
            .await
            .expect("corrupt spilled object");
        let error = Reader::open(Arc::clone(&backend.store), backend.prefix.clone())
            .await
            .expect_err("corrupt payload must fail the open");
        assert_eq!(
            error.to_string(),
            "Invalid head log: spilled payload does not match its reference"
        );
    }
}

#[derive(Clone, PartialEq, Message)]
struct TestCheckpoint {
    #[prost(bytes = "vec", repeated, tag = "2")]
    attempts: Vec<Vec<u8>>,
    #[prost(uint64, repeated, tag = "5")]
    seen_ms: Vec<u64>,
}

async fn checkpoint_attempts(backend: &Backend, floor: u64) -> usize {
    let bytes = backend
        .store
        .get(
            &backend
                .prefix
                .clone()
                .join("checkpoints")
                .join(format!("{floor:020}")),
        )
        .await
        .expect("checkpoint")
        .bytes()
        .await
        .expect("checkpoint bytes");
    let checkpoint = TestCheckpoint::decode(bytes).expect("decode checkpoint");
    assert_eq!(checkpoint.attempts.len(), checkpoint.seen_ms.len());
    checkpoint.attempts.len()
}

/// Checkpoints drop attempt ids older than the horizon on the head's store
/// time, and such an attempt then resolves as `Unresolved`, never `Rejected`.
/// With the default horizon the same history keeps the evidence.
#[tokio::test]
async fn headlog_attempt_evidence_expires_after_the_horizon() {
    for (horizon, expired) in [
        (std::time::Duration::from_millis(50), true),
        (object_store_occ::headlog::DEFAULT_ATTEMPT_HORIZON, false),
    ] {
        for backend in backends() {
            let mut writer = writer(&backend).await;
            writer.set_attempt_horizon(horizon);
            let oldest = append(&mut writer, Bytes::from_static(b"oldest")).await;
            for _ in 0..7 {
                append(&mut writer, Bytes::from_static(b"early")).await;
            }
            let first = *writer.state().head.batches.last().expect("sealed batch");
            writer
                .publish_checkpoint(first, Bytes::from_static(b"first"))
                .await
                .expect("first checkpoint");
            append(&mut writer, Bytes::from_static(b"publishes first")).await;
            assert_eq!(checkpoint_attempts(&backend, first).await as u64, first);
            assert_eq!(
                writer.resolve(&oldest).await.expect("fresh evidence"),
                Outcome::Accepted { commit: 1 }
            );

            // Store time passes the horizon before the next checkpoint.
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            for _ in 0..8 {
                append(&mut writer, Bytes::from_static(b"late")).await;
            }
            let second = *writer.state().head.batches.last().expect("sealed batch");
            writer
                .publish_checkpoint(second, Bytes::from_static(b"second"))
                .await
                .expect("second checkpoint");
            append(&mut writer, Bytes::from_static(b"publishes second")).await;
            let kept = checkpoint_attempts(&backend, second).await as u64;
            if expired {
                assert_eq!(kept, second - first, "only attempts after the first floor");
                assert_eq!(
                    writer.resolve(&oldest).await.expect("expired evidence"),
                    Outcome::Unresolved
                );
            } else {
                assert_eq!(kept, second);
                assert_eq!(
                    writer.resolve(&oldest).await.expect("retained evidence"),
                    Outcome::Accepted { commit: 1 }
                );
            }
        }
    }
}
