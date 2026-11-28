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

//! Eviction and restart through the node cache: the budget holds and keeps
//! the volume's reserve free, frequently read blocks outlive cold slabs,
//! entries and object sizes survive a restart, removals stay removed,
//! temporary files are swept, short entries are refetched, eviction by
//! reference is scoped to its prefix and ordered against fills of the same
//! object, and one cache at a time opens a directory.

use std::ops::Range;
use std::os::unix::fs::{FileExt, PermissionsExt};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use object_store_cache::testing::{
    self, BlockKey, BlockStore, Reserve, SlabStore, StoredMeta, Tuning, entry_len,
};
use object_store_cache::{
    Error, NodeCache, NodeCacheConfig, NodeCacheStats, Space, Statvfs, VolumeSpace,
};
use tokio::sync::Notify;

use crate::read_path::{MIB, TestStore, all_paths, assert_same, data, find_in_slabs, put};
use crate::support::{FaultStore, RoomyVolume, poll_until};

// ---------------------------------------------------------------------------
// Volumes and block stores
// ---------------------------------------------------------------------------

/// A volume of `capacity` bytes whose used space is the length of every file
/// under `root`: a size-capped test volume over real files.
#[derive(Debug)]
struct CappedVolume {
    root: PathBuf,
    capacity: u64,
}

fn bytes_under(dir: &std::path::Path) -> u64 {
    let mut total = 0;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in rd.flatten() {
            match e.metadata() {
                Ok(m) if m.is_dir() => stack.push(e.path()),
                Ok(m) => total += m.len(),
                Err(_) => {}
            }
        }
    }
    total
}

impl VolumeSpace for CappedVolume {
    fn space(&self, _dir: &std::path::Path) -> std::io::Result<Space> {
        let used = bytes_under(&self.root);
        Ok(Space {
            total: self.capacity,
            available: self.capacity.saturating_sub(used),
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Plenty of room on the volume. The tests here read whole objects into
/// budgets of a few blocks, which the scan rule (tested by
/// `read_path::scan_read_bypasses_admission`) would leave uncached, so it is
/// off.
fn roomy() -> Tuning {
    Tuning {
        scan_bytes: Some(u64::MAX),
        ..Tuning::default()
    }
}

/// One entry write at a time, so a read's blocks land in the slab in order.
fn one_writer() -> Tuning {
    Tuning {
        concurrent_writes: 1,
        ..roomy()
    }
}

/// A cache in `dir` of `budget` bytes on a volume with plenty of room.
fn roomy_config(dir: &std::path::Path, budget: u64) -> NodeCacheConfig {
    NodeCacheConfig {
        dir: dir.to_path_buf(),
        budget_bytes: budget,
        volume: Arc::new(RoomyVolume),
    }
}

/// Opens a cache on a volume with plenty of room.
async fn open(dir: &std::path::Path, budget: u64, block: u64, tuning: Tuning) -> Arc<NodeCache> {
    open_on(dir, budget, block, Arc::new(RoomyVolume), tuning).await
}

/// Opens a cache on `volume`. A cache dropped just before releases the
/// directory once its last write has finished, so `Locked` is retried for a
/// bounded time.
async fn open_on(
    dir: &std::path::Path,
    budget: u64,
    block: u64,
    volume: Arc<dyn VolumeSpace>,
    tuning: Tuning,
) -> Arc<NodeCache> {
    let config = NodeCacheConfig {
        dir: dir.to_path_buf(),
        budget_bytes: budget,
        volume,
    };
    let tuning = Tuning {
        block_bytes: block,
        ..tuning
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match testing::open_with(config.clone(), tuning.clone()).await {
            Ok(cache) => return cache,
            Err(Error::Locked { .. }) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Err(e) => panic!("open cache: {e:?}"),
        }
    }
}

/// A volume with plenty of room whose `space` blocks, while gated, until the
/// gate opens. Counts the calls blocked.
#[derive(Debug, Default)]
struct GatedVolume {
    gated: parking_lot::Mutex<bool>,
    opened: parking_lot::Condvar,
    blocked: AtomicU64,
}

impl GatedVolume {
    fn close_gate(&self) {
        *self.gated.lock() = true;
    }
    fn open_gate(&self) {
        *self.gated.lock() = false;
        self.opened.notify_all();
    }
}

impl VolumeSpace for GatedVolume {
    fn space(&self, _dir: &std::path::Path) -> std::io::Result<Space> {
        let mut gated = self.gated.lock();
        if *gated {
            self.blocked.fetch_add(1, Ordering::SeqCst);
            while *gated {
                self.opened.wait(&mut gated);
            }
            self.blocked.fetch_sub(1, Ordering::SeqCst);
        }
        Ok(Space {
            total: 1 << 40,
            available: 1 << 40,
        })
    }
}

/// Opens the gate when dropped, so a failing test never leaves a call blocked
/// on it: the runtime waits for blocking tasks when it shuts down.
struct OpenOnDrop(std::sync::Weak<GatedVolume>);

impl Drop for OpenOnDrop {
    fn drop(&mut self) {
        if let Some(v) = self.0.upgrade() {
            v.open_gate();
        }
    }
}

/// A volume whose available space the test sets.
#[derive(Debug)]
struct SettableVolume {
    available: AtomicU64,
}

impl VolumeSpace for SettableVolume {
    fn space(&self, _dir: &std::path::Path) -> std::io::Result<Space> {
        Ok(Space {
            total: 1 << 40,
            available: self.available.load(Ordering::SeqCst),
        })
    }
}

/// Sorted names of the files in `dir`.
fn names_in(dir: &std::path::Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .expect("list")
        .map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

/// Number of files in `slabs/` with the extension `ext`.
fn count_ext(dir: &std::path::Path, ext: &str) -> u64 {
    names_in(&dir.join("slabs"))
        .iter()
        .filter(|n| {
            std::path::Path::new(n)
                .extension()
                .is_some_and(|e| e == ext)
        })
        .count() as u64
}

fn wrap(cache: &Arc<NodeCache>, bucket: &str, store: &Arc<TestStore>) -> Arc<dyn ObjectStore> {
    cache.wrap(
        bucket,
        Arc::clone(store) as Arc<dyn ObjectStore>,
        all_paths(),
    )
}

fn usize_range(r: &Range<u64>) -> Range<usize> {
    usize::try_from(r.start).expect("start")..usize::try_from(r.end).expect("end")
}

/// Reads `range` of `path` through `cached` and checks it against `want`.
async fn read_check(cached: &Arc<dyn ObjectStore>, path: &Path, range: Range<u64>, want: &Bytes) {
    let got = cached
        .get_range(path, range.clone())
        .await
        .expect("cached read");
    assert_same(
        &got,
        &want.slice(usize_range(&range)),
        &format!("{path} {range:?}"),
    );
}

/// Sum of the lengths of the slab data files under `dir`.
fn dat_bytes(dir: &std::path::Path) -> u64 {
    std::fs::read_dir(dir.join("slabs"))
        .expect("list slabs")
        .map(|e| e.expect("entry").path())
        .filter(|p| p.extension().is_some_and(|e| e == "dat"))
        .map(|p| std::fs::metadata(&p).expect("stat").len())
        .sum()
}

/// Polls `cond` every 20 ms until it holds or `timeout` passes; returns the
/// last stats seen.
async fn wait_for(
    cache: &NodeCache,
    timeout: Duration,
    what: &str,
    cond: impl Fn(&NodeCacheStats) -> bool,
) -> NodeCacheStats {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let stats = cache.stats();
        if cond(&stats) {
            return stats;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{what}: not reached within {timeout:?}; last {stats:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn budget_is_respected() {
    // 8 MiB budget, 1 MiB blocks: 2 MiB slabs of one entry each, so disk use
    // is exactly entries times the entry size, with no dead space.
    let store = Arc::new(TestStore::default());
    let objects: Vec<(Path, Bytes)> = (0..8u64)
        .map(|i| {
            (
                Path::from(format!("t/obj-{i}.vortex")),
                data(100 + i, 8 * MIB),
            )
        })
        .collect();
    for (p, b) in &objects {
        put(&store, p.as_ref(), b).await;
    }
    let tmp = tempfile::tempdir().expect("tempdir");
    let budget = 8 * MIB;
    let cache = open(tmp.path(), budget, MIB, roomy()).await;
    let cached = wrap(&cache, "s3://bucket/", &store);
    let entry = entry_len(MIB);
    for (n, (p, b)) in objects.iter().enumerate() {
        read_check(&cached, p, 0..8 * MIB, b).await;
        cache.wait_idle().await;
        let stats = cache.stats();
        let on_disk = dat_bytes(tmp.path());
        assert!(stats.used_bytes <= budget, "object {n}: {stats:?}");
        assert!(stats.resident_bytes <= budget, "object {n}: {stats:?}");
        assert_eq!(on_disk, stats.used_bytes, "object {n}: files vs accounting");
        assert_eq!(
            stats.entries * entry,
            stats.used_bytes,
            "object {n}: {stats:?}"
        );
        assert_eq!(stats.dropped_fills, 0, "object {n}: {stats:?}");
        // One `.dat` per resident entry, an `.idx` for every slab but the
        // active one, and nothing else: no tombstones, no temporaries.
        let names = names_in(&tmp.path().join("slabs"));
        let (dat, idx) = (count_ext(tmp.path(), "dat"), count_ext(tmp.path(), "idx"));
        assert_eq!(dat, stats.entries, "object {n}: {names:?}");
        assert_eq!(idx, dat - 1, "object {n}: {names:?}");
        assert_eq!(names.len() as u64, dat + idx, "object {n}: {names:?}");
        assert_eq!(
            names_in(tmp.path()),
            vec!["lock".to_string(), "slabs".to_string()]
        );
    }
    let stats = cache.stats();
    // 64 blocks were written; what is not resident was reclaimed, one entry
    // per slab.
    assert_eq!(stats.fill_bytes, 64 * MIB, "{stats:?}");
    assert_eq!(
        stats.evicted_bytes_size,
        (64 - stats.entries) * entry,
        "{stats:?}"
    );
    assert_eq!(stats.capacity_bytes, budget);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writes_stay_within_budget() {
    // One-entry slabs and a budget of three and a half entries. Every put
    // waits 300 ms before reserving its space, so the four writes of one read
    // are all admitted before any of them shows up in the disk accounting:
    // admission alone must keep them within the budget.
    let store = Arc::new(TestStore::default());
    let p = Path::from("t/obj.vortex");
    let bytes = data(8, 4 * MIB);
    put(&store, p.as_ref(), &bytes).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let entry = entry_len(MIB);
    let slabs = SlabStore::open(tmp.path(), entry).await.expect("slabs");
    let slow = Arc::new(FaultStore {
        put_delay: Duration::from_millis(300),
        ..FaultStore::new(slabs)
    });
    let budget = 3 * entry + entry / 2;
    let cache = testing::with_block_store(
        roomy_config(tmp.path(), budget),
        Tuning {
            block_bytes: MIB,
            // No periodic check during the test: only admission is tested.
            budget_check_interval: Duration::from_secs(3600),
            ..roomy()
        },
        slow as Arc<dyn BlockStore>,
    )
    .expect("cache");
    let cached = wrap(&cache, "s3://bucket/", &store);
    read_check(&cached, &p, 0..4 * MIB, &bytes).await;
    cache.wait_idle().await;
    let stats = cache.stats();
    assert!(stats.used_bytes <= budget, "{stats:?}");
    // The fourth write found no room (the first three were still running, so
    // nothing could be reclaimed) and was dropped.
    assert_eq!(stats.entries, 3, "{stats:?}");
    assert_eq!(stats.dropped_fills, 1, "{stats:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_keeps_entries() {
    let store = Arc::new(TestStore::default());
    let a = Path::from("t/a.vortex");
    let b = Path::from("t/b.vortex");
    let a_bytes = data(1, 3 * MIB);
    let b_bytes = data(2, 3 * MIB / 2);
    put(&store, a.as_ref(), &a_bytes).await;
    put(&store, b.as_ref(), &b_bytes).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    {
        let cache = open(tmp.path(), 1 << 30, MIB, roomy()).await;
        let cached = wrap(&cache, "s3://bucket/", &store);
        read_check(&cached, &a, 0..3 * MIB, &a_bytes).await;
        read_check(&cached, &b, 0..3 * MIB / 2, &b_bytes).await;
        cache.wait_idle().await;
        assert_eq!(cache.stats().entries, 5);
    }
    let idx = tmp.path().join("slabs").join("slab-00000000.idx");
    let idx_version = |p: &std::path::Path| {
        let b = std::fs::read(p).expect("read index");
        u16::from_le_bytes([b[4], b[5]])
    };
    // Round 0: the slab active when the cache was dropped has no index and is
    // rescanned (and its index written). Round 1: that index (version 4)
    // alone.
    for round in 0..2 {
        if round == 0 {
            assert!(!idx.exists(), "no index before the first restart");
        } else {
            assert_eq!(idx_version(&idx), 4);
        }
        let (gets, heads) = (store.gets(), store.heads.load(Ordering::SeqCst));
        let cache = open(tmp.path(), 1 << 30, MIB, roomy()).await;
        let cached = wrap(&cache, "s3://bucket/", &store);
        assert_eq!(cache.stats().entries, 5, "round {round}");
        assert_eq!(idx_version(&idx), 4, "round {round}");
        let whole = cached.get(&a).await.expect("get");
        assert_eq!(whole.meta.size, 3 * MIB, "round {round}");
        assert_same(
            &whole.bytes().await.expect("bytes"),
            &a_bytes,
            &format!("round {round}: a after restart"),
        );
        read_check(&cached, &b, 0..3 * MIB / 2, &b_bytes).await;
        read_check(&cached, &a, MIB / 2..5 * MIB / 2, &a_bytes).await;
        let stats = cache.stats();
        assert_eq!(
            store.gets() - gets,
            0,
            "round {round}: GETs after restart: {stats:?}"
        );
        assert_eq!(
            store.heads.load(Ordering::SeqCst) - heads,
            0,
            "round {round}: HEADs after restart"
        );
        assert_eq!(
            (stats.accesses, stats.hits),
            (8, 8),
            "round {round}: every block read is a hit: {stats:?}"
        );
        assert_eq!(stats.fetched_bytes_query, 0, "round {round}: {stats:?}");
        assert_eq!(stats.fallbacks, 0, "round {round}: {stats:?}");
    }
}

#[tokio::test]
async fn tmp_files_removed_on_open() {
    let store = Arc::new(TestStore::default());
    let a = Path::from("t/a.vortex");
    let a_bytes = data(3, 2 * MIB);
    put(&store, a.as_ref(), &a_bytes).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    {
        let cache = open(tmp.path(), 1 << 30, MIB, roomy()).await;
        let cached = wrap(&cache, "s3://bucket/", &store);
        read_check(&cached, &a, 0..2 * MIB, &a_bytes).await;
        cache.wait_idle().await;
    }
    let slabs = tmp.path().join("slabs");
    // An interrupted write's temporary, an interrupted index write, and an
    // index and tombstones whose slab is gone.
    let leftovers = [
        "x.tmp",
        "slab-00000003.idx.tmp",
        "slab-00000042.idx",
        "slab-00000042.del",
    ];
    for name in leftovers {
        std::fs::write(slabs.join(name), b"torn").expect("plant");
    }
    let cache = open(tmp.path(), 1 << 30, MIB, roomy()).await;
    for name in leftovers {
        assert!(!slabs.join(name).exists(), "{name} survived open");
    }
    // Exactly the slab and the index its rescan wrote are left.
    assert_eq!(
        names_in(&slabs),
        vec![
            "slab-00000000.dat".to_string(),
            "slab-00000000.idx".to_string()
        ]
    );
    assert_eq!(
        names_in(tmp.path()),
        vec!["lock".to_string(), "slabs".to_string()]
    );
    assert_eq!(cache.stats().entries, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn short_entry_is_refetched() {
    let store = Arc::new(TestStore::default());
    let a = Path::from("t/a.vortex");
    let a_bytes = data(4, 3 * MIB);
    put(&store, a.as_ref(), &a_bytes).await;
    let block2 = a_bytes.slice(usize_range(&(2 * MIB..3 * MIB)));

    // Cut short under a running cache.
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = open(tmp.path(), 1 << 30, MIB, one_writer()).await;
    let cached = wrap(&cache, "s3://bucket/", &store);
    read_check(&cached, &a, 0..3 * MIB, &a_bytes).await;
    cache.wait_idle().await;
    let (slab, at) = find_in_slabs(tmp.path(), &block2);
    std::fs::OpenOptions::new()
        .write(true)
        .open(&slab)
        .expect("open slab")
        .set_len(at + MIB - 1000)
        .expect("truncate");
    let gets = store.gets();
    read_check(&cached, &a, 0..3 * MIB, &a_bytes).await;
    assert_eq!(
        store.gets() - gets,
        1,
        "only the short block is fetched: {:?}",
        cache.stats()
    );
    assert_eq!(cache.stats().fallbacks, 1, "{:?}", cache.stats());
    cache.wait_idle().await;
    // Rewritten at full length: exactly one complete copy, read without a GET.
    find_in_slabs(tmp.path(), &block2);
    let gets = store.gets();
    read_check(&cached, &a, 0..3 * MIB, &a_bytes).await;
    assert_eq!(store.gets() - gets, 0);
    assert_eq!(cache.stats().entries, 3);
    drop(cached);
    drop(cache);

    // Cut short while the cache is closed, in the slab left without an index:
    // the rescan does not list the short entry.
    let tmp = tempfile::tempdir().expect("tempdir");
    {
        let cache = open(tmp.path(), 1 << 30, MIB, one_writer()).await;
        let cached = wrap(&cache, "s3://bucket/", &store);
        read_check(&cached, &a, 0..3 * MIB, &a_bytes).await;
        cache.wait_idle().await;
    }
    let (slab, at) = find_in_slabs(tmp.path(), &block2);
    std::fs::OpenOptions::new()
        .write(true)
        .open(&slab)
        .expect("open slab")
        .set_len(at + MIB - 1000)
        .expect("truncate");
    let cache = open(tmp.path(), 1 << 30, MIB, one_writer()).await;
    assert_eq!(cache.stats().entries, 2, "the short entry is not listed");
    let cached = wrap(&cache, "s3://bucket/", &store);
    let gets = store.gets();
    read_check(&cached, &a, 0..3 * MIB, &a_bytes).await;
    assert_eq!(store.gets() - gets, 1, "only the short block is fetched");
    assert_eq!(cache.stats().fallbacks, 0);
    cache.wait_idle().await;
    find_in_slabs(tmp.path(), &block2);
    let gets = store.gets();
    read_check(&cached, &a, 0..3 * MIB, &a_bytes).await;
    assert_eq!(store.gets() - gets, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn evicted_object_stays_evicted_after_restart() {
    let store = Arc::new(TestStore::default());
    let a = Path::from("t/a.vortex");
    let b = Path::from("t/b.vortex");
    let a_bytes = data(5, 2 * MIB);
    let b_bytes = data(6, 3 * MIB);
    put(&store, a.as_ref(), &a_bytes).await;
    put(&store, b.as_ref(), &b_bytes).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    {
        let cache = open(tmp.path(), 1 << 30, MIB, roomy()).await;
        let cached = wrap(&cache, "s3://bucket/", &store);
        read_check(&cached, &a, 0..2 * MIB, &a_bytes).await;
        read_check(&cached, &b, 0..3 * MIB, &b_bytes).await;
        cache.wait_idle().await;
        let keep_a = a.clone();
        let evicted = cache
            .evict_unreferenced("s3://bucket/", &Path::from(""), |p| *p == keep_a)
            .await;
        assert_eq!(evicted, 3 * MIB, "b's three blocks");
        assert_eq!(cache.stats().entries, 2);
    }
    for round in 0..2 {
        let cache = open(tmp.path(), 1 << 30, MIB, roomy()).await;
        assert_eq!(cache.stats().entries, 2, "round {round}");
        let cached = wrap(&cache, "s3://bucket/", &store);
        let gets = store.gets();
        read_check(&cached, &a, 0..2 * MIB, &a_bytes).await;
        assert_eq!(store.gets() - gets, 0, "round {round}: a is still cached");
    }
    // b is fetched again when read (its three blocks in one ranged GET).
    let cache = open(tmp.path(), 1 << 30, MIB, roomy()).await;
    let cached = wrap(&cache, "s3://bucket/", &store);
    let gets = store.gets();
    read_check(&cached, &b, 0..3 * MIB, &b_bytes).await;
    assert_eq!(store.gets() - gets, 1, "b was evicted");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn evict_unreferenced_after_a_restart_needs_no_wrap() {
    let store = Arc::new(TestStore::default());
    let a = Path::from("t/a.vortex");
    let a_bytes = data(7, 2 * MIB);
    put(&store, a.as_ref(), &a_bytes).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    {
        let cache = open(tmp.path(), 1 << 30, MIB, roomy()).await;
        let cached = wrap(&cache, "s3://bucket/", &store);
        read_check(&cached, &a, 0..2 * MIB, &a_bytes).await;
        cache.wait_idle().await;
        cache.close().await.expect("close");
    }
    // Reopened, and the namespace not yet named in this process: its blocks
    // from before the restart are still evicted.
    let cache = open(tmp.path(), 1 << 30, MIB, roomy()).await;
    assert_eq!(cache.stats().entries, 2);
    let evicted = cache
        .evict_unreferenced("s3://bucket/", &Path::from(""), |_| false)
        .await;
    assert_eq!(evicted, 2 * MIB);
    assert_eq!(cache.stats().entries, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn foreign_files_fill_volume() {
    // By default a 64 MiB size-capped volume: its used space is the length of
    // every file under `root`, where the cache directory and another writer's
    // files both live. With `OSC_TEST_VOLUME` set to a directory on a small
    // real volume (a disk image), the cache reads that volume's space from the
    // operating system, and its writes can fail for lack of space. Budget half
    // the volume, 1 MiB blocks, slabs of a quarter of the budget. The volume's
    // reserve is 4 MiB (the default floor of 4 GiB would leave a test volume
    // no room), refilled by 5 % of the volume.
    let real = std::env::var_os("OSC_TEST_VOLUME").map(PathBuf::from);
    let root = match &real {
        Some(dir) => tempfile::tempdir_in(dir).expect("tempdir on the test volume"),
        None => tempfile::tempdir().expect("tempdir"),
    };
    let volume: Arc<dyn VolumeSpace> = match &real {
        Some(_) => Arc::new(Statvfs),
        None => Arc::new(CappedVolume {
            root: root.path().to_path_buf(),
            capacity: 64 * MIB,
        }),
    };
    let space = |v: &Arc<dyn VolumeSpace>| v.space(root.path()).expect("volume space");
    let cache_dir = root.path().join("cache");
    let foreign = root.path().join("foreign");
    std::fs::create_dir_all(&foreign).expect("mkdir");
    let capacity = space(&volume).total;
    let budget = (capacity / 2) / MIB * MIB;
    let reserve = 4 * MIB;
    let options = Tuning {
        budget_check_interval: Duration::from_millis(50),
        volume_reserve: Reserve {
            permille: 0,
            min_bytes: reserve,
            refill_permille: 50,
        },
        ..roomy()
    };
    let store = Arc::new(TestStore::default());
    let objects: Vec<(Path, Bytes)> = (0..6u64)
        .map(|i| {
            (
                Path::from(format!("t/obj-{i}.vortex")),
                data(200 + i, 8 * MIB),
            )
        })
        .collect();
    for (p, b) in &objects {
        put(&store, p.as_ref(), b).await;
    }
    let cache = open_on(&cache_dir, budget, MIB, Arc::clone(&volume), options).await;
    let cached = wrap(&cache, "s3://bucket/", &store);
    for (p, b) in &objects[..3] {
        read_check(&cached, p, 0..8 * MIB, b).await;
        cache.wait_idle().await;
    }
    let before = cache.stats();
    assert_eq!(before.entries, 24, "{before:?}");
    assert_eq!(before.capacity_bytes, budget, "{before:?}");

    // Readers keep going over all six objects (half of them not cached, so
    // fills keep coming) while another writer fills the volume to 2 MiB free.
    let stop = Arc::new(AtomicBool::new(false));
    let reads = Arc::new(AtomicU64::new(0));
    let mut readers = Vec::new();
    for r in 0..4u64 {
        let cached = Arc::clone(&cached);
        let objects = objects.clone();
        let stop = Arc::clone(&stop);
        let reads = Arc::clone(&reads);
        readers.push(tokio::spawn(async move {
            let mut i = r;
            while !stop.load(Ordering::SeqCst) {
                i = i.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                let (p, b) = &objects[usize::try_from((i >> 33) % 6).expect("index")];
                let block = (i >> 40) % 8;
                // Panics (failing the test) on an error or wrong bytes.
                read_check(&cached, p, block * MIB..(block + 1) * MIB, b).await;
                reads.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }
    let chunk = vec![0x5Au8; usize::try_from(MIB).expect("MiB")];
    let mut written = 0u64;
    let mut n = 0;
    loop {
        let available = space(&volume).available;
        if available <= 2 * MIB {
            break;
        }
        let len = (available - 2 * MIB).min(MIB);
        tokio::fs::write(
            foreign.join(format!("f-{n}")),
            &chunk[..usize::try_from(len).expect("len")],
        )
        .await
        .expect("foreign write");
        written += len;
        n += 1;
    }
    // The space the cache can reach is its own plus what the other writer
    // left free, and the capacity is that less the reserve; while the free
    // space is below the reserve, the low watermark is the refill below the
    // capacity. While writes run, the
    // cache counts entries it has reserved before their bytes reach the
    // files, so the capacity can sit up to the default four concurrent
    // writes' worth above the reachable space measured from the volume.
    let in_flight = 4 * entry_len(MIB);
    let settled = |s: &NodeCacheStats, slack: u64| {
        let reachable = (space(&volume).available + s.used_bytes).saturating_sub(reserve);
        s.capacity_bytes < budget
            && s.capacity_bytes <= reachable + slack
            && s.capacity_bytes + MIB > reachable
    };
    // Waits for a check that saw the finished foreign writes (the 50 ms
    // check interval is what is under test), then for the cache to get back
    // under the low watermark while the readers keep filling.
    let after = wait_for(
        &cache,
        Duration::from_secs(10),
        "back under the low watermark",
        |s| settled(s, in_flight) && s.used_bytes <= s.low_watermark_bytes,
    )
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    stop.store(true, Ordering::SeqCst);
    for r in readers {
        r.await.expect("a reader failed");
    }
    cache.wait_idle().await;
    // Three check intervals with nothing in flight: the watermark is exact.
    tokio::time::sleep(Duration::from_millis(150)).await;
    let end = cache.stats();
    eprintln!(
        "volume {} ({capacity} bytes), foreign {written} bytes, reads {}; before {before:?}; after {after:?}; end {end:?}",
        if real.is_some() { "real" } else { "capped" },
        reads.load(Ordering::SeqCst)
    );
    assert!(
        reads.load(Ordering::SeqCst) > 100,
        "the readers kept reading"
    );
    assert!(settled(&end, 0), "{end:?}");
    assert!(end.evicted_bytes_size > 0, "{end:?}");
    assert!(end.used_bytes <= end.capacity_bytes, "{end:?}");
    assert_eq!(dat_bytes(&cache_dir), end.used_bytes, "files vs accounting");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hot_blocks_survive_slab_eviction() {
    // 256 KiB blocks under a 4 MiB budget: 1 MiB slabs of three entries.
    // Writes run one at a time, each read waiting for its write, so slab k
    // holds blocks 3k, 3k+1, 3k+2.
    const BLOCK: u64 = 256 << 10;
    let store = Arc::new(TestStore::default());
    let p = Path::from("t/obj.vortex");
    let bytes = data(7, 32 * BLOCK);
    put(&store, p.as_ref(), &bytes).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = open(
        tmp.path(),
        4 * MIB,
        BLOCK,
        Tuning {
            concurrent_writes: 1,
            ..roomy()
        },
    )
    .await;
    let cached = wrap(&cache, "s3://bucket/", &store);
    let block = |i: u64| i * BLOCK..(i + 1) * BLOCK;
    for i in 0..12 {
        read_check(&cached, &p, block(i), &bytes).await;
        cache.wait_idle().await;
    }
    // Block 0 is read often; the other blocks were read once.
    for _ in 0..10 {
        read_check(&cached, &p, block(0), &bytes).await;
    }
    // The 16th entry crosses the high watermark (4 MiB) and reclaims one slab.
    for i in 12..16 {
        read_check(&cached, &p, block(i), &bytes).await;
        cache.wait_idle().await;
    }
    let stats = cache.stats();
    assert_eq!(stats.evicted_bytes_size, 3 * entry_len(BLOCK), "{stats:?}");
    // The hot block's slab stayed; the next slab, as cold as the others and
    // the oldest of them, was reclaimed.
    for (i, fetched) in [(0, 0), (1, 0), (2, 0), (3, 1), (6, 0)] {
        let gets = store.gets();
        read_check(&cached, &p, block(i), &bytes).await;
        assert_eq!(store.gets() - gets, fetched, "block {i}: {stats:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reclaim_weighs_value_per_byte_of_unequal_slabs() {
    // 256 KiB blocks under a 4 MiB budget: 1 MiB slabs of three entries.
    // Blocks 0, 1, 2 fill slab 0 and block 3 starts slab 1; after a restart
    // both are sealed, slab 1 with a single entry. Block 3 is then read once
    // more: slab 1 is worth 2 entry-lengths in all but about 2 per byte,
    // slab 0 (cold) 3 entry-lengths in all but about 1 per byte. A reclaim
    // by value per byte drops slab 0; one by total value would drop slab 1.
    const BLOCK: u64 = 256 << 10;
    let store = Arc::new(TestStore::default());
    let p = Path::from("t/obj.vortex");
    let bytes = data(9, 32 * BLOCK);
    put(&store, p.as_ref(), &bytes).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let options = || Tuning {
        concurrent_writes: 1,
        ..roomy()
    };
    let block = |i: u64| i * BLOCK..(i + 1) * BLOCK;
    {
        let cache = open(tmp.path(), 4 * MIB, BLOCK, options()).await;
        let cached = wrap(&cache, "s3://bucket/", &store);
        for i in 0..4 {
            read_check(&cached, &p, block(i), &bytes).await;
            cache.wait_idle().await;
        }
    }
    let entry = entry_len(BLOCK);
    let cache = open(tmp.path(), 4 * MIB, BLOCK, options()).await;
    assert_eq!(cache.stats().used_bytes, 4 * entry);
    let cached = wrap(&cache, "s3://bucket/", &store);
    read_check(&cached, &p, block(3), &bytes).await;
    // 11 more entries fit under 4 MiB; the 12th reclaims, once.
    for i in 4..16 {
        read_check(&cached, &p, block(i), &bytes).await;
        cache.wait_idle().await;
    }
    let stats = cache.stats();
    assert_eq!(stats.evicted_bytes_size, 3 * entry, "{stats:?}");
    // Hits first: the refetches after them write, and may reclaim again.
    for (i, fetched) in [(3, 0), (4, 0), (0, 1), (1, 1), (2, 1)] {
        let gets = store.gets();
        read_check(&cached, &p, block(i), &bytes).await;
        assert_eq!(store.gets() - gets, fetched, "block {i}: {stats:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn evict_unreferenced_is_scoped_to_its_prefix() {
    let store = Arc::new(TestStore::default());
    let names = ["d1/a.vortex", "d1/b.vortex", "d2/a.vortex", "d10/a.vortex"];
    let paths: Vec<Path> = names.iter().map(|n| Path::from(*n)).collect();
    let sizes = [2 * MIB, 3 * MIB / 2, 2 * MIB, MIB];
    let blobs: Vec<Bytes> = sizes
        .iter()
        .enumerate()
        .map(|(i, s)| data(10 + i as u64, *s))
        .collect();
    for (p, b) in paths.iter().zip(&blobs) {
        put(&store, p.as_ref(), b).await;
    }
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = open(tmp.path(), 1 << 30, MIB, roomy()).await;
    let in_a = wrap(&cache, "s3://bucket-a/", &store);
    let in_b = wrap(&cache, "s3://bucket-b/", &store);
    for ((p, b), s) in paths.iter().zip(&blobs).zip(sizes) {
        read_check(&in_a, p, 0..s, b).await;
    }
    // The same path under another namespace is another object.
    read_check(&in_b, &paths[1], 0..sizes[1], &blobs[1]).await;
    cache.wait_idle().await;
    assert_eq!(cache.stats().entries, 2 + 2 + 2 + 1 + 2);

    let gets_for = |cached: &Arc<dyn ObjectStore>, i: usize| {
        let cached = Arc::clone(cached);
        let (p, b, s) = (paths[i].clone(), blobs[i].clone(), sizes[i]);
        let store = Arc::clone(&store);
        async move {
            let gets = store.gets();
            read_check(&cached, &p, 0..s, &b).await;
            store.gets() - gets
        }
    };

    // Only objects under `d1` are asked about, and only b is unreferenced
    // there: it goes on the first call (no retention is kept), and the
    // bytes evicted are its resident block bytes. `d10` shares the `d1`
    // string prefix but not the path segment.
    let asked = std::sync::Mutex::new(Vec::new());
    let evicted = cache
        .evict_unreferenced("s3://bucket-a/", &Path::from("d1"), |p| {
            asked.lock().expect("asked").push(p.to_string());
            p.as_ref() != "d1/b.vortex"
        })
        .await;
    assert_eq!(evicted, 3 * MIB / 2, "b's bytes");
    let mut asked = asked.into_inner().expect("asked");
    asked.sort();
    assert_eq!(asked, vec!["d1/a.vortex", "d1/b.vortex"]);
    let stats = cache.stats();
    assert_eq!(stats.entries, 2 + 2 + 1 + 2, "{stats:?}");
    assert_eq!(gets_for(&in_a, 0).await, 0, "d1/a is kept");
    assert_eq!(gets_for(&in_a, 2).await, 0, "d2/a is outside the prefix");
    assert_eq!(gets_for(&in_a, 3).await, 0, "d10/a is outside the prefix");
    assert_eq!(
        gets_for(&in_b, 1).await,
        0,
        "bucket-b's b is another object"
    );
    assert_eq!(
        gets_for(&in_a, 1).await,
        1,
        "b was evicted and is fetched again"
    );
    cache.wait_idle().await;

    // Nothing unreferenced under the prefix: nothing evicted.
    assert_eq!(
        cache
            .evict_unreferenced("s3://bucket-a/", &Path::from("d2"), |_| true)
            .await,
        0
    );
    // The empty prefix covers the whole namespace.
    assert_eq!(
        cache
            .evict_unreferenced("s3://bucket-b/", &Path::from(""), |_| false)
            .await,
        3 * MIB / 2
    );
    let stats = cache.stats();
    assert_eq!(stats.entries, 2 + 2 + 2 + 1, "{stats:?}");
    assert_eq!(stats.evicted_bytes_unreferenced, 3 * MIB, "{stats:?}");
    assert_eq!(gets_for(&in_a, 0).await, 0, "a is still cached");
}

/// A volume of `total` bytes whose free space is `free` less what the cache
/// counts as its own use.
#[derive(Debug)]
struct ReservedVolume {
    cache: parking_lot::Mutex<std::sync::Weak<NodeCache>>,
    total: u64,
    free: AtomicU64,
}

impl VolumeSpace for ReservedVolume {
    fn space(&self, _dir: &std::path::Path) -> std::io::Result<Space> {
        let used = self
            .cache
            .lock()
            .upgrade()
            .map_or(0, |c| c.stats().used_bytes);
        Ok(Space {
            total: self.total,
            available: self.free.load(Ordering::SeqCst).saturating_sub(used),
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn capacity_keeps_the_volume_reserve() {
    // A 100 GiB volume: the cache leaves max(15 %, 4 GiB) = 15 GiB of it free.
    const GIB: u64 = 1 << 30;
    let tmp = tempfile::tempdir().expect("tempdir");
    let volume = Arc::new(ReservedVolume {
        cache: parking_lot::Mutex::new(std::sync::Weak::new()),
        total: 100 * GIB,
        free: AtomicU64::new(15 * GIB + 6 * MIB),
    });
    let budget = 64 * MIB;
    let tuning = Tuning {
        budget_check_interval: Duration::from_millis(20),
        concurrent_writes: 1,
        ..roomy()
    };
    let store = Arc::new(TestStore::default());
    let p = Path::from("t/obj.vortex");
    let bytes = data(300, 16 * MIB);
    put(&store, p.as_ref(), &bytes).await;
    let cache = open_on(
        tmp.path(),
        budget,
        MIB,
        Arc::clone(&volume) as Arc<dyn VolumeSpace>,
        tuning,
    )
    .await;
    *volume.cache.lock() = Arc::downgrade(&cache);
    let cached = wrap(&cache, "s3://bucket/", &store);
    // 6 MiB above the reserve: that, not the 64 MiB budget, is the capacity.
    let s = wait_for(
        &cache,
        Duration::from_secs(5),
        "capacity from the volume",
        |s| s.capacity_bytes != budget,
    )
    .await;
    assert_eq!(s.capacity_bytes, 6 * MIB, "{s:?}");
    for i in 0..16 {
        read_check(&cached, &p, i * MIB..(i + 1) * MIB, &bytes).await;
        cache.wait_idle().await;
    }
    let s = cache.stats();
    assert_eq!(s.capacity_bytes, 6 * MIB, "{s:?}");
    assert!(s.used_bytes <= 6 * MIB, "{s:?}");
    assert!(s.evicted_bytes_size > 0, "{s:?}");

    // Plenty of free space: the budget rules.
    volume.free.store(90 * GIB, Ordering::SeqCst);
    let s = wait_for(&cache, Duration::from_secs(5), "the budget", |s| {
        s.capacity_bytes == budget
    })
    .await;
    assert_eq!(s.capacity_bytes, budget);

    // Another writer takes the volume below the reserve: the cache gives back
    // everything it holds, as the 5 GiB refill above the reserve is more
    // than it has.
    volume.free.store(15 * GIB - MIB, Ordering::SeqCst);
    let s = wait_for(&cache, Duration::from_secs(5), "emptied", |s| {
        s.capacity_bytes == 0 && s.used_bytes == 0
    })
    .await;
    assert_eq!(s.entries, 0, "{s:?}");
}

#[tokio::test]
async fn second_open_on_same_dir_fails() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let other = tempfile::tempdir().expect("tempdir");
    let first = open(tmp.path(), 1 << 30, MIB, roomy()).await;
    let config = |dir: &std::path::Path| roomy_config(dir, 1 << 30);
    match testing::open_with(config(tmp.path()), roomy()).await {
        Err(Error::Locked { path }) => {
            assert_eq!(path, tmp.path());
        }
        other => panic!("second open on the same directory: {other:?}"),
    }
    let elsewhere = testing::open_with(config(other.path()), roomy()).await;
    assert!(elsewhere.is_ok(), "another directory: {elsewhere:?}");
    drop(first);
    // Released once the dropped cache's tasks are gone.
    drop(open(tmp.path(), 1 << 30, MIB, roomy()).await);

    // A dropped cache with an entry write still running keeps the directory
    // until that write is done.
    let store = Arc::new(TestStore::default());
    let a = Path::from("t/a.vortex");
    let a_bytes = data(30, MIB);
    put(&store, a.as_ref(), &a_bytes).await;
    let volume = Arc::new(GatedVolume::default());
    let _open = OpenOnDrop(Arc::downgrade(&volume));
    let (cache, writing) = stall_a_write(
        tmp.path(),
        &volume,
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        &a,
        &a_bytes,
    )
    .await;
    drop(writing);
    drop(cache);
    for _ in 0..3 {
        match testing::open_with(config(tmp.path()), roomy()).await {
            Err(Error::Locked { path }) => assert_eq!(path, tmp.path()),
            other => panic!("open while a dropped cache's write runs: {other:?}"),
        }
    }
    volume.open_gate();
    drop(open(tmp.path(), 1 << 30, MIB, roomy()).await);
}

/// An object store that forwards every call and whose drop blocks for
/// `delay`.
#[derive(Debug)]
struct SlowDropStore {
    inner: Arc<dyn ObjectStore>,
    delay: Duration,
}

impl Drop for SlowDropStore {
    fn drop(&mut self) {
        std::thread::sleep(self.delay);
    }
}

impl std::fmt::Display for SlowDropStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SlowDropStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for SlowDropStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<Path>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Opens a cache on `dir` over `volume` (one write at a time, one entry per
/// slab, a 20 ms budget check) and starts an entry write of `path` that
/// stalls inside its budget check, before it reaches the block store.
/// Returns the cache and its wrapper of `store`.
async fn stall_a_write(
    dir: &std::path::Path,
    volume: &Arc<GatedVolume>,
    store: Arc<dyn ObjectStore>,
    path: &Path,
    bytes: &Bytes,
) -> (Arc<NodeCache>, Arc<dyn ObjectStore>) {
    let options = Tuning {
        budget_check_interval: Duration::from_millis(20),
        concurrent_writes: 1,
        ..roomy()
    };
    // A 4 MiB budget gives slabs of one 1 MiB entry.
    let volume_space = Arc::clone(volume) as Arc<dyn VolumeSpace>;
    let cache = open_on(dir, 4 * MIB, MIB, volume_space, options).await;
    let cached = cache.wrap("s3://bucket/", store, all_paths());
    volume.close_gate();
    // The background check blocks first. Once it has, and the last
    // watermarks are older than the check interval, the write's own check
    // blocks too: two blocked calls.
    poll_until(Duration::from_secs(5), "the budget check blocks", || {
        volume.blocked.load(Ordering::SeqCst) >= 1
    })
    .await;
    // Time is what is under test: the watermarks must be stale.
    tokio::time::sleep(Duration::from_millis(60)).await;
    read_check(&cached, path, 0..bytes.len() as u64, bytes).await;
    poll_until(Duration::from_secs(5), "the entry write blocks", || {
        volume.blocked.load(Ordering::SeqCst) >= 2
    })
    .await;
    (cache, cached)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_cache_writes_do_not_reach_a_reopened_directory() {
    // One entry per slab. The first cache fills `a` (slab 0); its write of
    // `b` is stalled before it reserves space, and the cache is dropped. A
    // write still running when its cache is dropped must not touch files a
    // later cache on the same directory owns: here, the later cache's first
    // slab, which has the number the stalled write would create.
    let store = Arc::new(TestStore::default());
    let (a, b, c) = (
        Path::from("t/a.vortex"),
        Path::from("t/b.vortex"),
        Path::from("t/c.vortex"),
    );
    let (a_bytes, b_bytes, c_bytes) = (data(31, MIB), data(32, MIB), data(33, MIB));
    for (p, x) in [(&a, &a_bytes), (&b, &b_bytes), (&c, &c_bytes)] {
        put(&store, p.as_ref(), x).await;
    }
    let tmp = tempfile::tempdir().expect("tempdir");
    let volume = Arc::new(GatedVolume::default());
    let _open = OpenOnDrop(Arc::downgrade(&volume));
    let (cache, cached) = {
        let options = Tuning {
            budget_check_interval: Duration::from_millis(20),
            concurrent_writes: 1,
            ..roomy()
        };
        let volume_space = Arc::clone(&volume) as Arc<dyn VolumeSpace>;
        let cache = open_on(tmp.path(), 4 * MIB, MIB, volume_space, options).await;
        let cached = wrap(&cache, "s3://bucket/", &store);
        read_check(&cached, &a, 0..MIB, &a_bytes).await;
        cache.wait_idle().await;
        drop(cached);
        drop(cache);
        // That cache's tasks are gone, so none of them can block on the gate.
        poll_until(Duration::from_secs(5), "the filling cache is gone", || {
            Arc::strong_count(&volume) == 1
        })
        .await;
        // The stalled cache is the only owner of this wrapper, whose drop
        // takes 300 ms: the cache's destruction pauses after it has let go of
        // the volume and before it releases the directory.
        let slow_drop = Arc::new(SlowDropStore {
            inner: Arc::clone(&store) as Arc<dyn ObjectStore>,
            delay: Duration::from_millis(300),
        });
        stall_a_write(tmp.path(), &volume, slow_drop, &b, &b_bytes).await
    };
    drop(cached);
    drop(cache);
    let config = roomy_config(tmp.path(), 4 * MIB);
    // The first cache's tasks are gone once nothing else holds the volume.
    let old_gone = || Arc::strong_count(&volume) == 1;
    let second = match testing::open_with(config, one_writer()).await {
        Ok(second) => {
            // The directory was released with the write still running: the
            // second cache writes `c` into its first new slab, then the
            // stalled write goes on.
            let cached = wrap(&second, "s3://bucket/", &store);
            read_check(&cached, &c, 0..MIB, &c_bytes).await;
            second.wait_idle().await;
            volume.open_gate();
            poll_until(Duration::from_secs(5), "the first cache is gone", old_gone).await;
            second
        }
        Err(Error::Locked { .. }) => {
            volume.open_gate();
            poll_until(Duration::from_secs(5), "the first cache is gone", old_gone).await;
            // The directory is released once the first cache's state is
            // destroyed, after its last write. The volume's count drops
            // partway through that destruction, so it does not prove the
            // release: `open` retries `Locked` for a bounded time.
            let second = open(tmp.path(), 4 * MIB, MIB, one_writer()).await;
            let cached = wrap(&second, "s3://bucket/", &store);
            read_check(&cached, &c, 0..MIB, &c_bytes).await;
            second.wait_idle().await;
            second
        }
        Err(e) => panic!("second open: {e:?}"),
    };
    // `c` is served from the cache, intact: no GET, no entry that failed to
    // verify.
    let cached = wrap(&second, "s3://bucket/", &store);
    let gets = store.gets();
    read_check(&cached, &c, 0..MIB, &c_bytes).await;
    let stats = second.stats();
    assert_eq!(
        (store.gets() - gets, stats.fallbacks),
        (0, 0),
        "c's entry was damaged by the dropped cache's write: {stats:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn entry_embedded_in_a_payload_is_never_served() {
    // Object a's first block holds, at its start, a complete entry for b's
    // first block (header, key and data all valid) with bytes that are not
    // b's. a is cached, a's own header is damaged, and the cache reopens
    // with the slab unindexed. Reading b must return b's bytes from the
    // store, never the bytes embedded in a.
    let store = Arc::new(TestStore::default());
    let a = Path::from("t/a.vortex");
    let b = Path::from("t/b.vortex");
    let b_len = 100_000u64;
    let b_bytes = data(41, b_len);
    let fake = data(42, b_len);
    let embedded = {
        let scratch = tempfile::tempdir().expect("tempdir");
        let slabs = SlabStore::open(scratch.path(), 8 * MIB)
            .await
            .expect("scratch");
        let k = BlockKey {
            store: "s3://bucket/".to_string(),
            path: b.as_ref().to_string(),
            object_size: b_len,
            block_size: MIB,
            index: 0,
        };
        slabs
            .put(&k, fake.clone(), &StoredMeta::NONE)
            .await
            .expect("put");
        let (file, data_at) = slabs.data_location(&k).expect("located");
        drop(slabs);
        let raw = std::fs::read(file).expect("read");
        let start = usize::try_from(data_at - 4096).expect("start");
        raw[start..start + usize::try_from(entry_len(b_len)).expect("len")].to_vec()
    };
    let mut a_vec = embedded;
    a_vec.extend_from_slice(&data(43, MIB - a_vec.len() as u64));
    let a_bytes = Bytes::from(a_vec);
    put(&store, a.as_ref(), &a_bytes).await;
    put(&store, b.as_ref(), &b_bytes).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    {
        let cache = open(tmp.path(), 1 << 30, MIB, one_writer()).await;
        let cached = wrap(&cache, "s3://bucket/", &store);
        read_check(&cached, &a, 0..MIB, &a_bytes).await;
        cache.wait_idle().await;
    }
    let slabs = tmp.path().join("slabs");
    assert!(!slabs.join("slab-00000000.idx").exists());
    // Damage a's header (its magic).
    let (slab, at) = find_in_slabs(tmp.path(), &a_bytes);
    {
        use std::os::unix::fs::FileExt as _;
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(&slab)
            .expect("open slab");
        f.write_all_at(b"X", at - 4096).expect("damage a's header");
    }
    let cache = open(tmp.path(), 1 << 30, MIB, one_writer()).await;
    let cached = wrap(&cache, "s3://bucket/", &store);
    let got = cached.get_range(&b, 0..b_len).await.expect("read b");
    assert!(
        got != fake,
        "b was served from the entry embedded in a's payload: {:?}",
        cache.stats()
    );
    assert_same(&got, &b_bytes, "b");
    read_check(&cached, &a, 0..MIB, &a_bytes).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_active_slab_is_reclaimed_when_the_volume_fills() {
    // 64 MiB budget, 1 MiB blocks: 16 MiB slabs, so four entries sit in the
    // one active slab. Another writer then takes the whole volume and the
    // cache writes nothing more: the periodic check must still bring the
    // cache down to its low watermark.
    let store = Arc::new(TestStore::default());
    let p = Path::from("t/obj.vortex");
    let bytes = data(50, 4 * MIB);
    put(&store, p.as_ref(), &bytes).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let volume = Arc::new(SettableVolume {
        available: AtomicU64::new(1 << 40),
    });
    let options = Tuning {
        budget_check_interval: Duration::from_millis(20),
        ..roomy()
    };
    let volume_space = Arc::clone(&volume) as Arc<dyn VolumeSpace>;
    let cache = open_on(tmp.path(), 64 * MIB, MIB, volume_space, options).await;
    let cached = wrap(&cache, "s3://bucket/", &store);
    read_check(&cached, &p, 0..4 * MIB, &bytes).await;
    cache.wait_idle().await;
    let entry = entry_len(MIB);
    assert_eq!(cache.stats().used_bytes, 4 * entry);
    assert_eq!(count_ext(tmp.path(), "dat"), 1);
    volume.available.store(0, Ordering::SeqCst);
    let stats = wait_for(
        &cache,
        Duration::from_secs(2),
        "under the low watermark with the volume full",
        |s| s.capacity_bytes < 64 * MIB && s.used_bytes <= s.low_watermark_bytes,
    )
    .await;
    assert_eq!(stats.entries, 0, "{stats:?}");
    assert_eq!(stats.evicted_bytes_size, 4 * entry, "{stats:?}");
    assert_eq!(dat_bytes(tmp.path()), stats.used_bytes);
    // The cache still works: the next read fetches and serves the bytes.
    read_check(&cached, &p, 0..MIB, &bytes).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eviction_waits_for_inflight_fill() {
    // A fetch in flight when the object is evicted: its block is not written.
    let store = Arc::new(TestStore::default());
    let x = Path::from("t/x.vortex");
    let x_bytes = data(20, 3 * MIB);
    put(&store, x.as_ref(), &x_bytes).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = open(tmp.path(), 1 << 30, MIB, roomy()).await;
    let cached = wrap(&cache, "s3://bucket/", &store);
    read_check(&cached, &x, 0..MIB, &x_bytes).await;
    cache.wait_idle().await;
    assert_eq!(cache.stats().entries, 1);

    let gate = Arc::new(Notify::new());
    *store.stall.lock() = Some((x.clone(), Arc::clone(&gate)));
    let started = store.get_started.notified();
    let reader = {
        let cached = Arc::clone(&cached);
        let (x, x_bytes) = (x.clone(), x_bytes.clone());
        tokio::spawn(async move { read_check(&cached, &x, MIB..2 * MIB, &x_bytes).await })
    };
    started.await;
    let evicted = tokio::time::timeout(
        Duration::from_secs(5),
        cache.evict_unreferenced("s3://bucket/", &Path::from(""), |_| false),
    )
    .await
    .expect("the eviction does not wait for a fetch");
    assert_eq!(evicted, MIB, "the resident block");
    assert_eq!(cache.stats().entries, 0);
    gate.notify_one();
    reader.await.expect("the stalled reader");
    cache.wait_idle().await;
    let stats = cache.stats();
    assert_eq!(stats.entries, 0, "the fetched block was written: {stats:?}");
    assert_eq!(stats.dropped_fills, 1, "{stats:?}");
    let gets = store.gets();
    read_check(&cached, &x, MIB..2 * MIB, &x_bytes).await;
    assert_eq!(store.gets() - gets, 1);
    drop(cached);
    drop(cache);

    // A write running when the object is evicted: the eviction waits for it,
    // then removes what it wrote.
    let tmp = tempfile::tempdir().expect("tempdir");
    let slabs = SlabStore::open(tmp.path(), 256 * MIB).await.expect("slabs");
    let gated = Arc::new(FaultStore {
        gate_puts: AtomicBool::new(true),
        ..FaultStore::new(slabs)
    });
    let cache = testing::with_block_store(
        roomy_config(tmp.path(), 1 << 30),
        Tuning {
            block_bytes: MIB,
            ..roomy()
        },
        Arc::clone(&gated) as Arc<dyn BlockStore>,
    )
    .expect("cache");
    let cached = wrap(&cache, "s3://bucket/", &store);
    let started = gated.put_started.notified();
    read_check(&cached, &x, 0..MIB, &x_bytes).await;
    started.await;
    assert_eq!(gated.gated_puts.load(Ordering::SeqCst), 1);
    let eviction = {
        let cache = Arc::clone(&cache);
        tokio::spawn(async move {
            cache
                .evict_unreferenced("s3://bucket/", &Path::from(""), |_| false)
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !eviction.is_finished(),
        "the eviction finished while a write of the object was running"
    );
    gated.open_gate();
    let evicted = tokio::time::timeout(Duration::from_secs(5), eviction)
        .await
        .expect("the eviction finishes once the write does")
        .expect("join");
    assert_eq!(evicted, MIB, "the block the running write wrote");
    cache.wait_idle().await;
    let stats = cache.stats();
    assert_eq!(stats.entries, 0, "{stats:?}");
    assert_eq!(
        stats.fill_bytes, MIB,
        "the write itself completed: {stats:?}"
    );
}

async fn fill_separate_slabs(
    cache: &Arc<NodeCache>,
    store: &Arc<TestStore>,
    paths: &[Path],
    values: &[Bytes],
) {
    let cached = wrap(cache, "s3://bucket/", store);
    for (path, value) in paths.iter().zip(values) {
        read_check(&cached, path, 0..value.len() as u64, value).await;
        cache.wait_idle().await;
    }
    assert_eq!(cache.stats().entries, paths.len() as u64);
}

#[tokio::test]
async fn corrupt_tombstone_discards_only_its_slab() {
    const BLOCK: u64 = 100_000;
    let per_slab = 3 * entry_len(BLOCK);
    let budget = 4 * per_slab;
    let store = Arc::new(TestStore::default());
    let paths: Vec<Path> = (0..6)
        .map(|i| Path::from(format!("t/tombstone-{i}.vortex")))
        .collect();
    let values: Vec<Bytes> = (0..6).map(|i| data(100 + i, BLOCK)).collect();
    for (path, value) in paths.iter().zip(&values) {
        put(&store, path.as_ref(), value).await;
    }
    let tmp = tempfile::tempdir().expect("tempdir");
    {
        let cache = open(tmp.path(), budget, BLOCK, one_writer()).await;
        fill_separate_slabs(&cache, &store, &paths, &values).await;
        let removed = cache
            .evict_unreferenced("s3://bucket/", &Path::from(""), |path| path != &paths[1])
            .await;
        assert_eq!(removed, BLOCK);
        cache.close().await.expect("close");
    }

    let tombstones = tmp.path().join("slabs/slab-00000000.del");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&tombstones)
        .expect("open tombstones");
    let mut crc = [0u8; 1];
    file.read_exact_at(&mut crc, 8).expect("read tombstone CRC");
    crc[0] ^= 0x80;
    file.write_all_at(&crc, 8).expect("corrupt tombstone CRC");
    drop(file);

    let cache = open(tmp.path(), budget, BLOCK, one_writer()).await;
    assert_eq!(
        cache.stats().entries,
        3,
        "the corrupt slab must contribute no entries"
    );
    assert_eq!(cache.stats().quarantined_slabs, 1, "{:?}", cache.stats());
    let cached = wrap(&cache, "s3://bucket/", &store);
    let before = store.gets();
    read_check(&cached, &paths[1], 0..BLOCK, &values[1]).await;
    assert_eq!(store.gets() - before, 1, "removed block was resurrected");
    let before = store.gets();
    read_check(&cached, &paths[3], 0..BLOCK, &values[3]).await;
    assert_eq!(store.gets() - before, 0, "the other slab was discarded");
}

#[tokio::test]
async fn corrupt_index_quarantines_only_its_slab() {
    const BLOCK: u64 = 100_000;
    let per_slab = 3 * entry_len(BLOCK);
    let budget = 4 * per_slab;
    let store = Arc::new(TestStore::default());
    let paths: Vec<Path> = (0..6)
        .map(|i| Path::from(format!("t/index-{i}.vortex")))
        .collect();
    let values: Vec<Bytes> = (0..6).map(|i| data(200 + i, BLOCK)).collect();
    for (path, value) in paths.iter().zip(&values) {
        put(&store, path.as_ref(), value).await;
    }
    let tmp = tempfile::tempdir().expect("tempdir");
    {
        let cache = open(tmp.path(), budget, BLOCK, one_writer()).await;
        fill_separate_slabs(&cache, &store, &paths, &values).await;
        cache.close().await.expect("close");
    }
    std::fs::write(
        tmp.path().join("slabs/slab-00000000.idx"),
        b"not a slab index",
    )
    .expect("corrupt index");

    let cache = open(tmp.path(), budget, BLOCK, one_writer()).await;
    assert_eq!(
        cache.stats().entries,
        3,
        "the corrupt slab must contribute no entries"
    );
    assert_eq!(cache.stats().quarantined_slabs, 1, "{:?}", cache.stats());
    let cached = wrap(&cache, "s3://bucket/", &store);
    let before = store.gets();
    read_check(&cached, &paths[0], 0..BLOCK, &values[0]).await;
    assert_eq!(store.gets() - before, 1, "quarantined slab was served");
    let before = store.gets();
    read_check(&cached, &paths[3], 0..BLOCK, &values[3]).await;
    assert_eq!(store.gets() - before, 0, "the other slab was discarded");
}

/// Not a test on its own: run as the child process of
/// `cache_directories_and_files_are_owner_only`, it clears the umask (process
/// state, hence a process of its own), then fills a cache in the given
/// directory, tombstones one object and closes it. Returns at once in a
/// normal run.
#[tokio::test]
async fn owner_only_child() {
    const BLOCK: u64 = 100_000;
    let Ok(dir) = std::env::var("OSC_OWNER_ONLY_DIR") else {
        return;
    };
    let dir = PathBuf::from(dir);
    // SAFETY: `umask` only sets this process's file mode creation mask.
    unsafe { libc::umask(0) };
    let store = Arc::new(TestStore::default());
    let paths = [
        Path::from("t/permissions-a.vortex"),
        Path::from("t/permissions-b.vortex"),
    ];
    let values = [data(301, BLOCK), data(302, BLOCK)];
    for (path, value) in paths.iter().zip(&values) {
        put(&store, path.as_ref(), value).await;
    }
    let cache = open(&dir, 16 * entry_len(BLOCK), BLOCK, one_writer()).await;
    fill_separate_slabs(&cache, &store, &paths, &values).await;
    assert_eq!(
        cache
            .evict_unreferenced("s3://bucket/", &Path::from(""), |path| path != &paths[0])
            .await,
        BLOCK
    );
    cache.close().await.expect("close");
}

#[tokio::test]
async fn cache_directories_and_files_are_owner_only() {
    // The cache directory exists already, open to everyone; the child that
    // writes the cache runs with a umask of 0, so every mode the cache does
    // not set itself would be world-readable.
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join("cache");
    std::fs::create_dir(&dir).expect("mkdir");
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o777)).expect("chmod");
    let exe = std::env::current_exe().expect("test binary");
    let out = std::process::Command::new(exe)
        .args(["--exact", "eviction::owner_only_child", "--nocapture"])
        .env("OSC_OWNER_ONLY_DIR", &dir)
        .output()
        .expect("run the child");
    assert!(
        out.status.success(),
        "child failed: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    assert!(
        String::from_utf8_lossy(&out.stdout).contains("1 passed"),
        "the child test did not run: {}",
        String::from_utf8_lossy(&out.stdout)
    );

    let mode = |path: &std::path::Path| {
        std::fs::metadata(path)
            .expect("metadata")
            .permissions()
            .mode()
            & 0o777
    };
    assert_eq!(mode(&dir), 0o700, "cache directory");
    assert_eq!(mode(&dir.join("slabs")), 0o700, "slab directory");
    assert_eq!(mode(&dir.join("lock")), 0o600, "lock file");
    let mut kinds = Vec::new();
    for entry in std::fs::read_dir(dir.join("slabs")).expect("list slabs") {
        let path = entry.expect("slab entry").path();
        kinds.extend(path.extension().map(|e| e.to_string_lossy().into_owned()));
        assert_eq!(mode(&path), 0o600, "{}", path.display());
    }
    kinds.sort();
    kinds.dedup();
    assert_eq!(
        kinds,
        ["dat", "del", "idx"],
        "every kind of slab file was checked"
    );
}
