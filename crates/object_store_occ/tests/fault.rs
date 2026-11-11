// Copyright 2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0
//! Real storage tests for deterministic transport faults.
#![expect(
    clippy::expect_used,
    reason = "test helpers fail loudly on setup and on a missing fault"
)]
use bytes::Bytes;
use futures::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion, path::Path};
use object_store_occ::LocalConditionalPut;
use object_store_sim::{
    Op,
    fault::{Fault, FaultStore},
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[tokio::test]
async fn faults_memory_and_durable_local() {
    let dir = tempfile::tempdir().expect("directory");
    let backends: Vec<Arc<dyn ObjectStore>> = vec![
        Arc::new(object_store::memory::InMemory::new()),
        Arc::new(LocalConditionalPut::new_durable(dir.path()).expect("local")),
    ];
    for inner in backends {
        let store = Arc::new(FaultStore::new(Arc::clone(&inner)));
        let path = Path::from("request");
        let initial = inner
            .put(&path, Bytes::from_static(b"base").into())
            .await
            .expect("base");
        store.arm(Op::Cas, |_| true, Fault::Hold);
        let caller = Arc::clone(&store);
        let task = tokio::spawn(async move {
            caller
                .put_opts(
                    &Path::from("request"),
                    Bytes::from_static(b"new").into(),
                    PutOptions {
                        mode: PutMode::Update(UpdateVersion::from(initial)),
                        ..PutOptions::default()
                    },
                )
                .await
        });
        let id = store.wait_held(None).await;
        assert!(!task.is_finished());
        assert_eq!(
            inner
                .get(&path)
                .await
                .expect("get")
                .bytes()
                .await
                .expect("bytes"),
            "base"
        );
        let hooks = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&hooks);
        store
            .before(
                id,
                Arc::new(move || {
                    let count = Arc::clone(&count);
                    Box::pin(async move {
                        count.fetch_add(1, Ordering::SeqCst);
                    })
                }),
            )
            .expect("hook");
        store.deliver(id).await.expect("first lands");
        assert!(
            store.deliver(id).await.is_err(),
            "duplicate must fail original condition"
        );
        assert_eq!(hooks.load(Ordering::SeqCst), 2);
        assert!(!task.is_finished(), "delivery is independent of response");
        store.respond(id).expect("response");
        task.await
            .expect("join")
            .expect("original success retained");
        store.arm(Op::Put, |_| true, Fault::DropResponse);
        store
            .put(&path, Bytes::from_static(b"ambiguous").into())
            .await
            .expect_err("lost response");
        assert_eq!(
            inner
                .get(&path)
                .await
                .expect("get")
                .bytes()
                .await
                .expect("bytes"),
            "ambiguous"
        );
        let applied = store.log().len();
        store.arm(Op::Put, |_| true, Fault::Fail);
        store
            .put(&path, Bytes::from_static(b"lost").into())
            .await
            .expect_err("pre-I/O failure");
        store.arm(Op::Get, |_| true, Fault::Fail);
        store.get(&path).await.expect_err("unreadable");
        assert_eq!(
            store.log().len(),
            applied,
            "pre-I/O failures never reach inner"
        );
        assert_eq!(
            inner
                .get(&path)
                .await
                .expect("get")
                .bytes()
                .await
                .expect("bytes"),
            "ambiguous"
        );
        store.arm(Op::Put, |_| true, Fault::Hold);
        let caller = Arc::clone(&store);
        let task = tokio::spawn(async move {
            caller
                .put(&Path::from("request"), Bytes::new().into())
                .await
        });
        let second = store.wait_held(Some(id)).await;
        store.deliver(second).await.expect("land");
        store.drop_response(second).expect("lose response");
        task.await.expect("join").expect_err("dropped response");
        let log = store.log();
        assert_eq!(
            log.iter().map(|e| e.result.is_ok()).collect::<Vec<_>>(),
            vec![true, false, true, true]
        );
        println!(
            "{inner}: hold, delayed response, duplicate condition, before-hook, ambiguous landing, failed put/get, dropped held response"
        );
    }
}

#[tokio::test]
async fn canceled_call_still_delivers_and_forwarded_operations_are_logged() {
    use futures::TryStreamExt;
    use object_store::{CopyOptions, PutMultipartOptions};
    use object_store_sim::fault::BeforeHook;
    let directory = tempfile::tempdir().expect("directory");
    let backends: Vec<Arc<dyn ObjectStore>> = vec![
        Arc::new(object_store::memory::InMemory::new()),
        Arc::new(LocalConditionalPut::new_durable(directory.path()).expect("local")),
    ];
    for inner in backends {
        let store = Arc::new(FaultStore::new(Arc::clone(&inner)));
        store.arm(Op::Create, |p| p.as_ref() == "held", Fault::Hold);
        let caller = Arc::clone(&store);
        let call = tokio::spawn(async move {
            caller
                .put_opts(
                    &Path::from("held"),
                    Bytes::from_static(b"late").into(),
                    PutOptions {
                        mode: PutMode::Create,
                        ..PutOptions::default()
                    },
                )
                .await
        });
        let id = store.wait_held(None).await;
        call.abort();
        call.await.expect_err("caller killed");
        // The hook performs real storage work through the wrapper before the
        // held operation; the ordered log must reflect that causal order.
        let weak = Arc::downgrade(&store);
        let hook: BeforeHook = Arc::new(move || {
            let weak = std::sync::Weak::clone(&weak);
            Box::pin(async move {
                weak.upgrade()
                    .expect("store alive")
                    .put(&Path::from("hook"), Bytes::new().into())
                    .await
                    .expect("hook I/O");
            })
        });
        store.before(id, hook).expect("before");
        store
            .deliver(id)
            .await
            .expect("delivery survives cancellation");
        store.respond(id).expect("response to canceled caller");
        store
            .get(&Path::from("held"))
            .await
            .expect("get")
            .bytes()
            .await
            .expect("bytes");
        store
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .expect("list");
        store.list_with_delimiter(None).await.expect("delimiter");
        store
            .copy_opts(
                &Path::from("held"),
                &Path::from("copy"),
                CopyOptions::default(),
            )
            .await
            .expect("copy");
        store.delete(&Path::from("copy")).await.expect("delete");
        let mut upload = store
            .put_multipart_opts(&Path::from("multipart"), PutMultipartOptions::default())
            .await
            .expect("multipart");
        upload
            .put_part(Bytes::from_static(b"part").into())
            .await
            .expect("part");
        upload.complete().await.expect("complete");
        let log = store.log();
        assert_eq!(
            log.iter().map(|e| e.op).collect::<Vec<_>>(),
            vec![
                Op::Put,
                Op::Create,
                Op::Get,
                Op::List,
                Op::List,
                Op::Copy,
                Op::Delete,
                Op::MultipartBegin,
                Op::MultipartPart,
                Op::MultipartComplete
            ]
        );
        assert!(log.iter().all(|e| e.result.is_ok()));
        assert_eq!(log[0].path, Path::from("hook"));
        assert_eq!(log[1].path, Path::from("held"));
        println!(
            "{inner}: canceled caller retains delivery; hook/put/get/list/copy/delete/multipart log order holds"
        );
    }
}

fn backends(directory: &std::path::Path) -> Vec<Arc<dyn ObjectStore>> {
    vec![
        Arc::new(object_store::memory::InMemory::new()),
        Arc::new(LocalConditionalPut::new_durable(directory).expect("local")),
    ]
}

fn refused(op: Op) -> String {
    format!("Generic FaultStore error: {op:?} failed before I/O")
}

const LOST: &str = "Generic FaultStore error: response lost after I/O";

/// The injected error's text, checking it is the wrapper's own error.
fn injected<T: std::fmt::Debug>(result: object_store::Result<T>) -> String {
    match result.expect_err("fault must surface") {
        error @ object_store::Error::Generic {
            store: "FaultStore",
            ..
        } => error.to_string(),
        other => panic!("not an injected error: {other:?}"),
    }
}

/// The stored bytes, or `None` when the object is absent.
async fn stored(store: &Arc<dyn ObjectStore>, path: &str) -> Option<Bytes> {
    match store.get(&Path::from(path)).await {
        Ok(result) => Some(result.bytes().await.expect("bytes")),
        Err(object_store::Error::NotFound { .. }) => None,
        Err(other) => panic!("read {path}: {other:?}"),
    }
}

fn ops(store: &FaultStore) -> Vec<(Op, String, bool)> {
    store
        .log()
        .into_iter()
        .map(|e| (e.op, e.path.to_string(), e.result.is_ok()))
        .collect()
}

#[tokio::test]
async fn multipart_faults_refuse_or_apply_each_step() {
    let directory = tempfile::tempdir().expect("directory");
    for inner in backends(directory.path()) {
        let store = Arc::new(FaultStore::new(Arc::clone(&inner)));

        // Begin: refused before storage, or created with the response lost.
        store.arm(Op::MultipartBegin, |p| p.as_ref() == "m/begin", Fault::Fail);
        let begin = store.put_multipart(&Path::from("m/begin")).await;
        assert_eq!(injected(begin), refused(Op::MultipartBegin));
        assert!(store.log().is_empty(), "refused begin reached storage");
        store.arm(Op::MultipartBegin, |_| true, Fault::DropResponse);
        let begin = store.put_multipart(&Path::from("m/begin")).await;
        assert_eq!(injected(begin), LOST);
        assert_eq!(
            ops(&store),
            vec![(Op::MultipartBegin, "m/begin".into(), true)]
        );
        assert_eq!(stored(&inner, "m/begin").await, None);

        // Part: a refused part never lands; a lost-response part does.
        let mut upload = store
            .put_multipart(&Path::from("m/part_fail"))
            .await
            .expect("begin");
        store.arm(Op::MultipartPart, |_| true, Fault::Fail);
        let part = upload.put_part(Bytes::from_static(b"refused").into()).await;
        assert_eq!(injected(part), refused(Op::MultipartPart));
        upload
            .put_part(Bytes::from_static(b"kept").into())
            .await
            .expect("part");
        upload.complete().await.expect("complete");
        assert_eq!(
            stored(&inner, "m/part_fail").await.as_deref(),
            Some(&b"kept"[..])
        );
        let mut upload = store
            .put_multipart(&Path::from("m/part_drop"))
            .await
            .expect("begin");
        store.arm(Op::MultipartPart, |_| true, Fault::DropResponse);
        let part = upload.put_part(Bytes::from_static(b"landed").into()).await;
        assert_eq!(injected(part), LOST);
        upload.complete().await.expect("complete");
        assert_eq!(
            stored(&inner, "m/part_drop").await.as_deref(),
            Some(&b"landed"[..])
        );

        // Complete: refused leaves the object absent and the upload open;
        // a lost response leaves it present.
        let mut upload = store
            .put_multipart(&Path::from("m/complete_fail"))
            .await
            .expect("begin");
        upload
            .put_part(Bytes::from_static(b"body").into())
            .await
            .expect("part");
        store.arm(Op::MultipartComplete, |_| true, Fault::Fail);
        assert_eq!(
            injected(upload.complete().await),
            refused(Op::MultipartComplete)
        );
        assert_eq!(stored(&inner, "m/complete_fail").await, None);
        upload.complete().await.expect("retried complete");
        assert_eq!(
            stored(&inner, "m/complete_fail").await.as_deref(),
            Some(&b"body"[..])
        );
        let mut upload = store
            .put_multipart(&Path::from("m/complete_drop"))
            .await
            .expect("begin");
        upload
            .put_part(Bytes::from_static(b"body").into())
            .await
            .expect("part");
        store.arm(Op::MultipartComplete, |_| true, Fault::DropResponse);
        assert_eq!(injected(upload.complete().await), LOST);
        assert_eq!(
            stored(&inner, "m/complete_drop").await.as_deref(),
            Some(&b"body"[..])
        );

        // Abort: refused never reaches storage; a lost response does.
        let mut upload = store
            .put_multipart(&Path::from("m/abort"))
            .await
            .expect("begin");
        let before = store.log().len();
        store.arm(Op::MultipartAbort, |_| true, Fault::Fail);
        assert_eq!(injected(upload.abort().await), refused(Op::MultipartAbort));
        assert_eq!(store.log().len(), before, "refused abort reached storage");
        store.arm(Op::MultipartAbort, |_| true, Fault::DropResponse);
        assert_eq!(injected(upload.abort().await), LOST);
        assert_eq!(
            ops(&store)[before..],
            [(Op::MultipartAbort, "m/abort".into(), true)]
        );
        assert_eq!(stored(&inner, "m/abort").await, None);
        println!("{inner}: multipart begin/part/complete/abort Fail and DropResponse");
    }
}

#[tokio::test]
async fn held_multipart_complete_lands_only_on_delivery() {
    let directory = tempfile::tempdir().expect("directory");
    for inner in backends(directory.path()) {
        let store = Arc::new(FaultStore::new(Arc::clone(&inner)));
        let mut previous = None;
        for (name, lose) in [("m/held", false), ("m/held_lost", true)] {
            let mut upload = store.put_multipart(&Path::from(name)).await.expect("begin");
            upload
                .put_part(Bytes::from_static(b"held body").into())
                .await
                .expect("part");
            store.arm(Op::MultipartComplete, |_| true, Fault::Hold);
            let task = tokio::spawn(async move { upload.complete().await });
            let id = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                store.wait_held(previous),
            )
            .await
            .expect("completion was not held");
            previous = Some(id);
            assert_eq!(stored(&inner, name).await, None, "held completion landed");
            assert!(!task.is_finished());
            store.deliver(id).await.expect("delivery completes");
            assert_eq!(
                stored(&inner, name).await.as_deref(),
                Some(&b"held body"[..])
            );
            if lose {
                store.drop_response(id).expect("lose response");
                let answer = task.await.expect("join");
                assert_eq!(injected(answer), "Generic FaultStore error: response lost");
            } else {
                store.respond(id).expect("respond");
                task.await.expect("join").expect("original success");
            }
        }
        println!("{inner}: held multipart complete lands on delivery, response released or lost");
    }
}

#[tokio::test]
async fn delete_faults_match_each_path() {
    let directory = tempfile::tempdir().expect("directory");
    for inner in backends(directory.path()) {
        let store = Arc::new(FaultStore::new(Arc::clone(&inner)));
        for name in ["d/a", "d/b", "d/c"] {
            inner
                .put(&Path::from(name), Bytes::from(name).into())
                .await
                .expect("seed");
        }
        store.arm(Op::Delete, |p| p.as_ref() == "d/b", Fault::Fail);
        store.arm(Op::Delete, |p| p.as_ref() == "d/c", Fault::DropResponse);
        let answers: Vec<Result<String, String>> = store
            .delete_stream(
                futures::stream::iter(["d/a", "d/b", "d/c"].map(|p| Ok(Path::from(p)))).boxed(),
            )
            .map(|r| r.map(|p| p.to_string()).map_err(|e| e.to_string()))
            .collect()
            .await;
        assert_eq!(
            answers,
            vec![Ok("d/a".into()), Err(refused(Op::Delete)), Err(LOST.into())]
        );
        assert_eq!(stored(&inner, "d/a").await, None);
        assert_eq!(stored(&inner, "d/b").await.as_deref(), Some(&b"d/b"[..]));
        assert_eq!(stored(&inner, "d/c").await, None);
        assert_eq!(
            ops(&store),
            vec![
                (Op::Delete, "d/a".into(), true),
                (Op::Delete, "d/c".into(), true)
            ]
        );
        println!("{inner}: per-path delete Fail keeps, DropResponse deletes");
    }
}

#[tokio::test]
async fn copy_and_rename_faults_refuse_or_apply() {
    let directory = tempfile::tempdir().expect("directory");
    for inner in backends(directory.path()) {
        let store = Arc::new(FaultStore::new(Arc::clone(&inner)));
        for name in ["c/src", "r/one", "r/two"] {
            inner
                .put(&Path::from(name), Bytes::from_static(b"body").into())
                .await
                .expect("seed");
        }
        let body = Some(Bytes::from_static(b"body"));

        store.arm(Op::Copy, |p| p.as_ref() == "c/fail", Fault::Fail);
        let copy = store
            .copy(&Path::from("c/src"), &Path::from("c/fail"))
            .await;
        assert_eq!(injected(copy), refused(Op::Copy));
        assert_eq!(stored(&inner, "c/fail").await, None);
        store.arm(Op::Copy, |p| p.as_ref() == "c/drop", Fault::DropResponse);
        let copy = store
            .copy(&Path::from("c/src"), &Path::from("c/drop"))
            .await;
        assert_eq!(injected(copy), LOST);
        assert_eq!(stored(&inner, "c/drop").await, body);

        store.arm(Op::Rename, |p| p.as_ref() == "r/fail", Fault::Fail);
        let rename = store
            .rename(&Path::from("r/one"), &Path::from("r/fail"))
            .await;
        assert_eq!(injected(rename), refused(Op::Rename));
        assert_eq!(stored(&inner, "r/one").await, body);
        assert_eq!(stored(&inner, "r/fail").await, None);
        store.arm(Op::Rename, |p| p.as_ref() == "r/drop", Fault::DropResponse);
        let rename = store
            .rename(&Path::from("r/two"), &Path::from("r/drop"))
            .await;
        assert_eq!(injected(rename), LOST);
        assert_eq!(stored(&inner, "r/two").await, None);
        assert_eq!(stored(&inner, "r/drop").await, body);
        assert_eq!(
            ops(&store),
            vec![
                (Op::Copy, "c/drop".into(), true),
                (Op::Rename, "r/drop".into(), true)
            ]
        );
        println!("{inner}: copy and rename Fail before, DropResponse after storage");
    }
}

#[tokio::test]
async fn listing_faults_refuse_every_listing_call() {
    use futures::TryStreamExt;
    let directory = tempfile::tempdir().expect("directory");
    for inner in backends(directory.path()) {
        let store = Arc::new(FaultStore::new(Arc::clone(&inner)));
        for name in ["l/a", "l/b"] {
            inner
                .put(&Path::from(name), Bytes::new().into())
                .await
                .expect("seed");
        }
        let prefix = Path::from("l");
        let is_prefix = |p: &Path| p.as_ref() == "l";
        for fault in [Fault::Fail, Fault::DropResponse, Fault::Hold] {
            store.arm(Op::List, is_prefix, fault);
            let listed: Vec<_> = store.list(Some(&prefix)).collect().await;
            assert_eq!(
                listed.len(),
                1,
                "{fault:?}: an error stream yields one item"
            );
            assert_eq!(
                injected(listed.into_iter().next().expect("item")),
                refused(Op::List)
            );
            store.arm(Op::List, is_prefix, fault);
            let listed: Vec<_> = store
                .list_with_offset(Some(&prefix), &Path::from("l/a"))
                .collect()
                .await;
            assert_eq!(
                listed.len(),
                1,
                "{fault:?}: an error stream yields one item"
            );
            assert_eq!(
                injected(listed.into_iter().next().expect("item")),
                refused(Op::List)
            );
            store.arm(Op::List, is_prefix, fault);
            assert_eq!(
                injected(store.list_with_delimiter(Some(&prefix)).await),
                refused(Op::List)
            );
        }
        assert!(store.log().is_empty(), "refused listing reached storage");
        let mut after: Vec<_> = store
            .list_with_offset(Some(&prefix), &Path::from("l/a"))
            .map_ok(|m| m.location.to_string())
            .try_collect()
            .await
            .expect("list");
        after.sort();
        assert_eq!(after, vec!["l/b".to_string()]);
        assert_eq!(
            ops(&store),
            vec![(Op::List, "l".into(), true)],
            "an unfaulted offset listing is logged once"
        );
        println!("{inner}: list, list_with_offset and list_with_delimiter faults");
    }
}
