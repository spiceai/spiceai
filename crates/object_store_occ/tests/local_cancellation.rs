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

//! Cancellation during real filesystem publication must preserve CAS exclusion.
#![expect(
    clippy::expect_used,
    reason = "test assertions include operation context"
)]

use bytes::Bytes;
use object_store::{
    ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion, path::Path,
};
use object_store_occ::LocalConditionalPut;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn cancellation_case(shutdown: bool, queue_write: bool) {
    let directory = tempfile::tempdir().expect("directory");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(if shutdown { 1 } else { 4 })
        .enable_all()
        .build()
        .expect("runtime");
    let first = Arc::new(LocalConditionalPut::new(directory.path()).expect("first client"));
    let second = LocalConditionalPut::new(directory.path()).expect("second client");
    let path = Path::from("state");
    let initial = runtime
        .block_on(first.put(&path, Bytes::from_static(b"base").into()))
        .expect("initialize");
    let version = UpdateVersion {
        e_tag: initial.e_tag,
        version: initial.version,
    };
    let queued_key = Path::from("queued");
    let queued_initial = runtime
        .block_on(first.put(&queued_key, Bytes::from_static(b"base").into()))
        .expect("initialize queued key");
    let opts = PutOptions::from(PutMode::Update(version.clone()));
    // Small chunks keep the real blocking upload active long enough to cancel
    // after staging begins; no mock replaces locking or filesystem publication.
    let payload: PutPayload = std::iter::repeat_n(Bytes::from_static(b"x"), 262_144).collect();
    let writer = Arc::clone(&first);
    let task =
        runtime.spawn(async move { writer.put_opts(&Path::from("state"), payload, opts).await });
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut staged = None;
    while Instant::now() < deadline {
        staged = std::fs::read_dir(directory.path())
            .expect("list staging files")
            .filter_map(Result::ok)
            .find(|e| {
                e.file_name().to_string_lossy().starts_with("state#")
                    && e.metadata().is_ok_and(|m| m.len() > 0 && m.len() < 262_144)
            })
            .map(|e| e.path());
        if staged.is_some() {
            break;
        }
        assert!(
            !task.is_finished(),
            "upload finished before cancellation was exercised"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    let staged = staged.expect("observe an active staged write");
    if queue_write {
        // The active write occupies the worker, leaving exactly one queue slot.
        // Polling this send once fills it and reaches the pending reply future.
        let mut queued = Box::pin(first.put_opts(
            &queued_key,
            Bytes::from_static(b"queued publication").into(),
            PutOptions::from(PutMode::Update(UpdateVersion {
                e_tag: queued_initial.e_tag,
                version: queued_initial.version,
            })),
        ));
        runtime.block_on(async {
            assert!(
                futures::poll!(&mut queued).is_pending(),
                "queued caller waits for publication"
            );
        });
        drop(queued);
    }
    drop(first);
    task.abort();
    assert!(
        runtime
            .block_on(task)
            .expect_err("cancelled caller")
            .is_cancelled()
    );
    // Runtime shutdown must not release a lock owned by an active publication.
    let runtime = if shutdown {
        runtime.shutdown_background();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .expect("recovery runtime")
    } else {
        runtime
    };
    let contender = runtime.block_on(second.put_opts(
        &path,
        Bytes::from_static(b"acknowledged").into(),
        PutOptions::from(PutMode::Update(version)),
    ));
    println!("contender after cancellation: {contender:?}");
    while staged.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(!staged.exists(), "cancelled publication did not finish");
    let final_bytes = runtime.block_on(async {
        second
            .get(&path)
            .await
            .expect("get final")
            .bytes()
            .await
            .expect("read final")
    });
    if queue_write {
        loop {
            let value = runtime.block_on(async {
                second
                    .get(&queued_key)
                    .await
                    .expect("queued get")
                    .bytes()
                    .await
                    .expect("queued read")
            });
            if value == Bytes::from_static(b"queued publication") {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "last handle dropped without draining its accepted write"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    println!(
        "final length={}, first byte={:?}",
        final_bytes.len(),
        final_bytes.first()
    );
    if let Err(error) = &contender {
        assert!(
            matches!(error, object_store::Error::Precondition { .. }),
            "unexpected storage error: {error}"
        );
        assert_eq!(final_bytes.len(), 262_144);
        assert!(final_bytes.iter().all(|byte| *byte == b'x'));
    } else {
        assert!(
            final_bytes == Bytes::from_static(b"acknowledged"),
            "cancelled write overwrote acknowledged state: length={}",
            final_bytes.len()
        );
    }
}

#[test]
fn cancelled_local_write_cannot_overwrite_an_acknowledged_contender() {
    cancellation_case(false, false);
}

#[test]
fn runtime_shutdown_does_not_release_an_active_publication_lock() {
    cancellation_case(true, false);
}

#[test]
fn dropping_last_handle_drains_active_and_queued_writes() {
    cancellation_case(true, true);
}
