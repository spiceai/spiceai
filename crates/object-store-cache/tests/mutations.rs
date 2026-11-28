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

//! Writes, renames and deletes through the wrapper: a cached object is
//! evicted durably before the store changes it, so a deleted or replaced
//! object is never served, not even after a restart; a failed eviction turns
//! the cache off for the namespace and still lets the delete through; and the
//! wrapper remembers the objects uploaded through it.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use object_store_cache::testing::{
    self, BlockKey, BlockStore, SlabStore, StoredMeta, Tuning, entry_len,
};
use object_store_cache::{Cacheable, Error, NodeCache, Prefetched};
use parking_lot::Mutex;

use crate::read_path::{MIB, TestStore, assert_same, config, data, put};
use crate::support::{FaultStore, poll_until, reclaim_one};

const NS: &str = "s3://bucket/";

/// Whole-object reads of a few blocks would be scans under the default rule
/// (an eighth of a 1 GiB budget is far above them, but keep it explicit).
fn tuning() -> Tuning {
    Tuning {
        scan_bytes: Some(u64::MAX),
        ..Tuning::default()
    }
}

/// Opens a cache in `dir`. A cache dropped just before releases the
/// directory once its last write has finished, so `Locked` is retried for a
/// bounded time.
async fn open(dir: &std::path::Path) -> Arc<NodeCache> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match testing::open_with(config(dir), tuning()).await {
            Ok(cache) => return cache,
            Err(Error::Locked { .. }) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Err(e) => panic!("open cache: {e:?}"),
        }
    }
}

/// `.vortex` paths only, as the integration caches immutable data files.
fn vortex_only() -> Cacheable {
    Arc::new(|p: &Path| p.as_ref().ends_with(".vortex"))
}

fn wrap(cache: &Arc<NodeCache>, store: &Arc<TestStore>) -> Arc<dyn ObjectStore> {
    cache.wrap(NS, Arc::clone(store) as Arc<dyn ObjectStore>, vortex_only())
}

async fn read_all(cached: &Arc<dyn ObjectStore>, p: &Path, want: &Bytes) {
    let got = cached
        .get_range(p, 0..want.len() as u64)
        .await
        .expect("cached read");
    assert_same(&got, want, p.as_ref());
}

fn is_not_found(e: &object_store::Error) -> bool {
    matches!(e, object_store::Error::NotFound { .. })
}

/// Records the cache's resident entries each time a delete reaches the store.
fn record_entries_at_delete(store: &TestStore, cache: &Arc<NodeCache>) -> Arc<Mutex<Vec<u64>>> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let weak: Weak<NodeCache> = Arc::downgrade(cache);
    let log = Arc::clone(&seen);
    *store.delete_hook.lock() = Some(Arc::new(move |_p: &Path| {
        if let Some(cache) = weak.upgrade() {
            log.lock().push(cache.stats().entries);
        }
    }));
    seen
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_evicts_before_the_store_deletes() {
    let store = Arc::new(TestStore::default());
    let a = Path::from("t/a.vortex");
    let b = Path::from("t/b.vortex");
    let c = Path::from("t/c.vortex");
    let (a_bytes, b_bytes, c_bytes) = (data(1, 3 * MIB), data(2, 2 * MIB), data(3, MIB));
    put(&store, a.as_ref(), &a_bytes).await;
    put(&store, b.as_ref(), &b_bytes).await;
    put(&store, c.as_ref(), &c_bytes).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = open(tmp.path()).await;
    let cached = wrap(&cache, &store);
    read_all(&cached, &a, &a_bytes).await;
    read_all(&cached, &b, &b_bytes).await;
    read_all(&cached, &c, &c_bytes).await;
    cache.wait_idle().await;
    assert_eq!(cache.stats().entries, 6);
    let seen = record_entries_at_delete(&store, &cache);

    // One delete: a's three blocks are gone before the store sees it.
    cached.delete(&a).await.expect("delete a");
    assert_eq!(*seen.lock(), vec![3], "entries when a's delete arrived");
    // A batch: both objects are gone before the first delete arrives.
    let batch = futures::stream::iter([Ok(b.clone()), Ok(c.clone())]).boxed();
    let deleted: Vec<Path> = cached
        .delete_stream(batch)
        .map(|r| r.expect("delete"))
        .collect()
        .await;
    assert_eq!(deleted, vec![b.clone(), c.clone()]);
    assert_eq!(
        *seen.lock(),
        vec![3, 0, 0],
        "entries when each delete arrived"
    );
    let stats = cache.stats();
    assert_eq!(stats.entries, 0, "{stats:?}");
    assert_eq!(stats.evicted_bytes_unreferenced, 6 * MIB, "{stats:?}");
    // Deleted objects are not served.
    for p in [&a, &b, &c] {
        let err = cached
            .get_range(p, 0..10)
            .await
            .expect_err("a deleted object is not served");
        assert!(is_not_found(&err), "{p}: {err}");
    }
    cache.wait_idle().await;
    assert_eq!(cache.stats().entries, 0, "nothing was filled");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_eviction_turns_the_namespace_off_and_forwards_the_delete() {
    let store = Arc::new(TestStore::default());
    let a = Path::from("t/a.vortex");
    let b = Path::from("t/b.vortex");
    let (a_bytes, b_bytes) = (data(4, 2 * MIB), data(5, 2 * MIB));
    put(&store, a.as_ref(), &a_bytes).await;
    put(&store, b.as_ref(), &b_bytes).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let slabs = SlabStore::open(tmp.path(), 256 * MIB).await.expect("slabs");
    let failing = Arc::new(FaultStore::new(slabs));
    let cache = testing::with_block_store(
        config(tmp.path()),
        tuning(),
        Arc::clone(&failing) as Arc<dyn BlockStore>,
    )
    .expect("cache");
    let cached = wrap(&cache, &store);
    let other = cache.wrap(
        "s3://other/",
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        vortex_only(),
    );
    read_all(&cached, &a, &a_bytes).await;
    read_all(&cached, &b, &b_bytes).await;
    read_all(&other, &b, &b_bytes).await;
    cache.wait_idle().await;
    assert_eq!(cache.stats().entries, 6);

    failing.fail_remove.store(true, Ordering::SeqCst);
    let fallbacks = cache.stats().fallbacks;
    cached
        .delete(&a)
        .await
        .expect("the delete is forwarded although the eviction failed");
    let gone = store.inner.head(&a).await.expect_err("a is deleted");
    assert!(is_not_found(&gone), "{gone}");
    // a's blocks are still on disk (the removal failed), but the namespace is
    // off: a is not served, and b is read from the store every time.
    assert_eq!(cache.stats().entries, 6);
    let err = cached
        .get_range(&a, 0..10)
        .await
        .expect_err("a deleted object is not served");
    assert!(is_not_found(&err), "{err}");
    for round in 0..2 {
        let gets = store.gets();
        read_all(&cached, &b, &b_bytes).await;
        assert_eq!(
            store.gets() - gets,
            1,
            "round {round}: b read from the store"
        );
    }
    assert_eq!(
        cache.prefetch(NS, &b, 2 * MIB, None).expect("prefetch"),
        Prefetched::default(),
        "nothing is prefetched into a namespace that is off"
    );
    cache.wait_idle().await;
    let stats = cache.stats();
    assert_eq!(stats.entries, 6, "nothing filled: {stats:?}");
    assert_eq!(stats.fallbacks, fallbacks + 1, "{stats:?}");
    let last = cache.last_error().expect("the failure is reported");
    assert!(
        last.contains("injected tombstone failure") && last.contains(NS),
        "{last}"
    );
    // Another namespace keeps its cache.
    let gets = store.gets();
    read_all(&other, &b, &b_bytes).await;
    assert_eq!(store.gets() - gets, 0, "the other namespace is served");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eviction_before_a_failed_delete_survives_a_restart() {
    let store = Arc::new(TestStore::default());
    let a = Path::from("t/a.vortex");
    let a_bytes = data(6, 3 * MIB);
    put(&store, a.as_ref(), &a_bytes).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    {
        let cache = open(tmp.path()).await;
        let cached = wrap(&cache, &store);
        read_all(&cached, &a, &a_bytes).await;
        cache.wait_idle().await;
        assert_eq!(cache.stats().entries, 3);
        store.fail_deletes.store(true, Ordering::SeqCst);
        let err = cached.delete(&a).await.expect_err("the store refused");
        assert!(err.to_string().contains("refused"), "{err}");
        // Dropped without closing: the process died after the delete failed.
    }
    store.fail_deletes.store(false, Ordering::SeqCst);
    let cache = open(tmp.path()).await;
    assert_eq!(cache.stats().entries, 0, "the eviction was durable");
    let cached = wrap(&cache, &store);
    let gets = store.gets();
    read_all(&cached, &a, &a_bytes).await;
    assert_eq!(store.gets() - gets, 1, "a is fetched again, with its bytes");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rename_evicts_source_and_target() {
    let store = Arc::new(TestStore::default());
    let a = Path::from("t/a.vortex");
    let b = Path::from("t/b.vortex");
    // The same size, so blocks keyed by size alone would serve b's old bytes.
    let (a_bytes, b_bytes) = (data(7, 2 * MIB), data(8, 2 * MIB));
    put(&store, a.as_ref(), &a_bytes).await;
    put(&store, b.as_ref(), &b_bytes).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = open(tmp.path()).await;
    let cached = wrap(&cache, &store);
    read_all(&cached, &a, &a_bytes).await;
    read_all(&cached, &b, &b_bytes).await;
    cache.wait_idle().await;
    assert_eq!(cache.stats().entries, 4);

    cached.rename(&a, &b).await.expect("rename");
    assert_eq!(cache.stats().entries, 0, "both evicted");
    read_all(&cached, &b, &a_bytes).await;
    let err = cached
        .get_range(&a, 0..10)
        .await
        .expect_err("the source is gone");
    assert!(is_not_found(&err), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn put_over_a_resident_object_evicts_it() {
    let store = Arc::new(TestStore::default());
    let a = Path::from("t/a.vortex");
    let b = Path::from("t/b.vortex");
    let (old_a, new_a) = (data(9, 2 * MIB), data(10, 2 * MIB));
    let (old_b, new_b) = (data(11, 2 * MIB), data(12, 2 * MIB));
    put(&store, a.as_ref(), &old_a).await;
    put(&store, b.as_ref(), &old_b).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = open(tmp.path()).await;
    let cached = wrap(&cache, &store);
    read_all(&cached, &a, &old_a).await;
    read_all(&cached, &b, &old_b).await;
    cache.wait_idle().await;
    assert_eq!(cache.stats().entries, 4);

    cached
        .put(&a, PutPayload::from_bytes(new_a.clone()))
        .await
        .expect("put");
    assert_eq!(cache.stats().entries, 2, "a evicted");
    read_all(&cached, &a, &new_a).await;
    // a's new blocks are written, so the count below is b's eviction alone.
    cache.wait_idle().await;
    assert_eq!(cache.stats().entries, 4, "a's new blocks");

    let mut upload = cached.put_multipart(&b).await.expect("multipart");
    assert_eq!(cache.stats().entries, 2, "b evicted when the upload starts");
    upload
        .put_part(PutPayload::from_bytes(new_b.clone()))
        .await
        .expect("part");
    upload.complete().await.expect("complete");
    read_all(&cached, &b, &new_b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uploads_are_written_here_and_not_prefetched() {
    let store = Arc::new(TestStore::default());
    let a = Path::from("t/a.vortex");
    let c = Path::from("t/c.vortex");
    let d = Path::from("t/d.vortex");
    let head = Path::from("t/_head.json");
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = open(tmp.path()).await;
    let cached = wrap(&cache, &store);

    let a_bytes = data(13, 2 * MIB);
    cached
        .put(&a, PutPayload::from_bytes(a_bytes.clone()))
        .await
        .expect("put a");
    assert!(cache.written_here(NS, &a));
    assert!(!cache.written_here("s3://other/", &a), "per namespace");
    cached
        .put(&head, PutPayload::from_static(b"{}"))
        .await
        .expect("put head");
    assert!(!cache.written_here(NS, &head), "not cacheable");

    let mut upload = cached.put_multipart(&c).await.expect("multipart");
    upload
        .put_part(PutPayload::from_bytes(data(14, MIB)))
        .await
        .expect("part");
    assert!(
        !cache.written_here(NS, &c),
        "not until the upload completes"
    );
    upload.complete().await.expect("complete");
    assert!(cache.written_here(NS, &c));

    store.fail.store(true, Ordering::SeqCst);
    cached
        .put(&d, PutPayload::from_static(b"x"))
        .await
        .expect_err("the store is down");
    store.fail.store(false, Ordering::SeqCst);
    assert!(!cache.written_here(NS, &d), "a failed put wrote nothing");

    // A prefetch of an object written here is skipped.
    let gets = store.gets();
    assert_eq!(
        cache.prefetch(NS, &a, 2 * MIB, None).expect("prefetch"),
        Prefetched::default()
    );
    cache.wait_idle().await;
    assert_eq!(store.gets() - gets, 0);
    assert_eq!(cache.stats().entries, 0);

    // Deleting or renaming it away forgets it.
    cached.delete(&a).await.expect("delete");
    assert!(!cache.written_here(NS, &a));
    cached.rename(&c, &d).await.expect("rename");
    assert!(!cache.written_here(NS, &c));
}

/// A read that leads a block's fetch hands it to later reads of the block
/// until its entry write runs. A put of the object in between (same size, new
/// bytes) is never served the bytes fetched before it: the write of the old
/// fetch is still queued, behind a write that holds the only writer, when a
/// read after the put asks for the block.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_after_a_put_never_joins_a_fetch_from_before_it() {
    let store = Arc::new(TestStore::default());
    let a = Path::from("t/a.vortex");
    let p = Path::from("t/p.vortex");
    let (a_bytes, old_p, new_p) = (data(30, MIB), data(31, MIB), data(32, MIB));
    put(&store, a.as_ref(), &a_bytes).await;
    put(&store, p.as_ref(), &old_p).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let slabs = SlabStore::open(tmp.path(), 256 * MIB).await.expect("slabs");
    let gated = Arc::new(FaultStore {
        gate_puts: AtomicBool::new(true),
        ..FaultStore::new(slabs)
    });
    let cache = testing::with_block_store(
        config(tmp.path()),
        Tuning {
            concurrent_writes: 1,
            shared_fetch_wait: Duration::from_secs(30),
            ..tuning()
        },
        Arc::clone(&gated) as Arc<dyn BlockStore>,
    )
    .expect("cache");
    let cached = wrap(&cache, &store);

    // a's write takes the only writer and waits; p's write queues behind it,
    // holding p's fetched (old) bytes for reads of the block.
    let started = gated.put_started.notified();
    read_all(&cached, &a, &a_bytes).await;
    started.await;
    read_all(&cached, &p, &old_p).await;
    cached
        .put(&p, PutPayload::from_bytes(new_p.clone()))
        .await
        .expect("put p");

    let got = tokio::time::timeout(Duration::from_secs(10), cached.get_range(&p, 0..MIB))
        .await
        .expect("the read after the put returns")
        .expect("read p");
    assert_same(&got, &new_p, "a read after the put");

    gated.open_gate();
    cache.wait_idle().await;
    read_all(&cached, &p, &new_p).await;
    assert_eq!(
        gated.gated_puts.load(Ordering::SeqCst),
        1,
        "only a's write was gated"
    );
}

/// The `.dat` of the slab holding `dir`'s cached blocks (one slab).
fn only_slab(dir: &std::path::Path) -> std::path::PathBuf {
    let slabs: Vec<_> = std::fs::read_dir(dir.join("slabs"))
        .expect("list slabs")
        .map(|e| e.expect("entry").path())
        .filter(|p| p.extension().is_some_and(|e| e == "dat"))
        .collect();
    assert_eq!(slabs.len(), 1, "{slabs:?}");
    slabs.into_iter().next().expect("one slab")
}

/// An eviction whose tombstones cannot be written (a directory sits where the
/// slab's tombstone file goes) still never lets the evicted bytes come back:
/// after the object is deleted and written again with the same size, a
/// reopened cache serves the new bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_tombstone_never_brings_evicted_bytes_back_after_restart() {
    let store = Arc::new(TestStore::default());
    let a = Path::from("t/a.vortex");
    let (old, new) = (data(33, 2 * MIB), data(34, 2 * MIB));
    put(&store, a.as_ref(), &old).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let del = {
        let cache = open(tmp.path()).await;
        let cached = wrap(&cache, &store);
        read_all(&cached, &a, &old).await;
        cache.wait_idle().await;
        assert_eq!(cache.stats().entries, 2);
        let del = only_slab(tmp.path()).with_extension("del");
        std::fs::create_dir(&del).expect("block the tombstone file");
        cached.delete(&a).await.expect("delete a");
        assert_eq!(cache.stats().entries, 0, "{:?}", cache.stats());
        cached
            .put(&a, PutPayload::from_bytes(new.clone()))
            .await
            .expect("put a again");
        // Dropped without closing: the process died.
        del
    };
    std::fs::remove_dir(&del).expect("unblock");
    let cache = open(tmp.path()).await;
    assert_eq!(cache.stats().entries, 0, "{:?}", cache.stats());
    let cached = wrap(&cache, &store);
    read_all(&cached, &a, &new).await;
}

/// An eviction while a reclaim of the object's slab cannot delete the slab's
/// file (a directory sits in its place) is still durable: after the object is
/// deleted and written again with the same size, a reopened cache serves the
/// new bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eviction_during_a_failed_reclaim_survives_a_restart() {
    const BLOCK: u64 = 64 << 10;
    let entry = entry_len(BLOCK);
    let store = Arc::new(TestStore::default());
    let a = Path::from("t/a.vortex");
    let b = Path::from("t/b.vortex");
    let (old, new, b_bytes) = (data(35, 2 * BLOCK), data(36, 2 * BLOCK), data(37, BLOCK));
    put(&store, a.as_ref(), &old).await;
    put(&store, b.as_ref(), &b_bytes).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let tuning = Tuning {
        block_bytes: BLOCK,
        concurrent_writes: 1,
        ..tuning()
    };
    let open_on = |slabs: Arc<SlabStore>| {
        testing::with_block_store(
            config(tmp.path()),
            tuning.clone(),
            slabs as Arc<dyn BlockStore>,
        )
        .expect("cache")
    };
    let dat = tmp.path().join("slabs").join("slab-00000000.dat");
    let aside = tmp.path().join("slabs").join("aside");
    {
        // Slabs of two entries: a's two blocks fill slab 0, b's starts slab 1.
        let slabs = SlabStore::open(tmp.path(), 2 * entry).await.expect("slabs");
        let cache = open_on(Arc::clone(&slabs));
        let cached = wrap(&cache, &store);
        read_all(&cached, &a, &old).await;
        cache.wait_idle().await;
        read_all(&cached, &b, &b_bytes).await;
        cache.wait_idle().await;
        assert_eq!(slabs.slab_count(), 2);

        std::fs::rename(&dat, &aside).expect("move slab 0 aside");
        std::fs::create_dir(&dat).expect("dir in its place");
        std::fs::write(dat.join("x"), b"x").expect("non-empty");
        let reclaimed = reclaim_one(&*slabs).await;
        assert!(
            matches!(
                reclaimed,
                Err(Error::Io {
                    action: "remove",
                    ..
                })
            ),
            "{reclaimed:?}"
        );
        cached.delete(&a).await.expect("delete a");
        cached
            .put(&a, PutPayload::from_bytes(new.clone()))
            .await
            .expect("put a again");
    }
    std::fs::remove_dir_all(&dat).expect("remove the dir");
    std::fs::rename(&aside, &dat).expect("restore slab 0");
    let slabs = SlabStore::open(tmp.path(), 2 * entry)
        .await
        .expect("reopen");
    let cache = open_on(slabs);
    let cached = wrap(&cache, &store);
    let got = cached.get_range(&a, 0..2 * BLOCK).await.expect("read a");
    assert_same(&got, &new, "a after the restart");
}

/// The `.dat` files under `dir`'s slab directory, with their lengths.
fn dat_files(dir: &std::path::Path) -> Vec<(String, u64)> {
    let mut files: Vec<(String, u64)> = std::fs::read_dir(dir.join("slabs"))
        .expect("list slabs")
        .map(|e| e.expect("entry").path())
        .filter(|p| p.extension().is_some_and(|e| e == "dat"))
        .map(|p| {
            let len = std::fs::metadata(&p).expect("stat").len();
            (
                p.file_name().expect("name").to_string_lossy().into_owned(),
                len,
            )
        })
        .collect();
    files.sort();
    files
}

/// A slab dropped whole because its tombstones cannot be written (a directory
/// sits where its `.del` goes) while a write into it is still running: the
/// write must not create the slab's file again outside the store's disk
/// accounting, nor list its block in a slab the store no longer counts (one
/// no reclaim could ever free). The store's disk use stays the length of the
/// slab files on disk, and every listed block stays in a counted slab.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_into_a_discarded_slab_is_not_left_outside_the_budget() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let slabs = SlabStore::open(tmp.path(), 256 * MIB).await.expect("slabs");
    let key = |path: &str, size: u64| BlockKey {
        store: NS.to_string(),
        path: path.to_string(),
        object_size: size,
        block_size: size,
        index: 0,
    };
    let small = key("t/small.vortex", 4096);
    slabs
        .put(&small, data(40, 4096), &StoredMeta::NONE)
        .await
        .expect("put small");
    let del = tmp.path().join("slabs").join("slab-00000000.del");
    std::fs::create_dir(&del).expect("block the tombstone file");

    // A large entry, so its write (encode, checksum, write) is still running
    // when the removal below drops the slab.
    let big_len = 64 * MIB;
    let big = key("t/big.vortex", big_len);
    let writer = {
        let slabs = Arc::clone(&slabs);
        let big = big.clone();
        let bytes = Bytes::from(vec![7u8; usize::try_from(big_len).expect("len")]);
        tokio::spawn(async move { slabs.put(&big, bytes, &StoredMeta::NONE).await })
    };
    poll_until(
        Duration::from_secs(5),
        "the large write reserved its entry",
        || slabs.pending_bytes() > 0,
    )
    .await;
    slabs
        .remove(&small)
        .await
        .expect("the slab is dropped whole instead of tombstoned");
    let put = writer.await.expect("join");
    std::fs::remove_dir(&del).expect("unblock");

    let files = dat_files(tmp.path());
    let state = (
        slabs.disk_bytes(),
        slabs.slab_count(),
        slabs.len(),
        put.as_ref().map_err(ToString::to_string).err(),
    );
    eprintln!("slab files {files:?}; (disk_bytes, slab_count, entries, put error) {state:?}");
    assert_eq!(
        (slabs.disk_bytes(), slabs.slab_count()),
        (files.iter().map(|(_, len)| len).sum(), files.len()),
        "the store counts exactly the slab files on disk: {files:?}, {state:?}"
    );
    assert!(
        slabs.is_empty() || slabs.slab_count() > 0,
        "a block is listed in no counted slab: {state:?}"
    );
    assert!(
        !slabs.contains(&big),
        "the dropped slab's write is listed: {state:?}"
    );
}
