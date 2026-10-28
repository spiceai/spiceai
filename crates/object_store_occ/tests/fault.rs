// Copyright 2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0
//! Real storage tests for deterministic transport faults.
use bytes::Bytes;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion, path::Path};
use object_store_occ::{
    LocalConditionalPut,
    sim::{
        Op,
        fault::{Fault, FaultStore},
    },
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
    use object_store_occ::sim::fault::BeforeHook;
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
                Op::Put,
                Op::Put,
                Op::Put
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
