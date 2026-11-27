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

//! Prefetch, scan admission, the write lanes, stored object metadata and
//! clean close: prefetch yields to queries and shares one fetch per block
//! with them; it fills free room only and never evicts; scans are not
//! admitted; prefetch fills wait for room in their queue while demand fills
//! are dropped; e-tags survive a restart; and a closed cache reopens from
//! indexes alone.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use object_store_cache::testing::{self, BlockStore, SlabStore, Tuning};
use object_store_cache::{NodeCache, NodeCacheConfig, Prefetched};
use tokio::sync::Notify;
use tokio::time::Instant;

use crate::modeled::ModeledStore;
use crate::read_path::{MIB, TestStore, all_paths, assert_same, data, find_in_slabs, put};
use crate::support::{FaultStore, RoomyVolume, poll_until};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn roomy() -> Tuning {
    Tuning::default()
}

/// A cache in `dir` of `budget` bytes on a volume with plenty of room.
fn config(dir: &std::path::Path, budget: u64) -> NodeCacheConfig {
    NodeCacheConfig {
        dir: dir.to_path_buf(),
        budget_bytes: budget,
        volume: Arc::new(RoomyVolume),
    }
}

async fn open(dir: &std::path::Path, budget: u64, block: u64, tuning: Tuning) -> Arc<NodeCache> {
    let tuning = Tuning {
        block_bytes: block,
        ..tuning
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match testing::open_with(config(dir, budget), tuning.clone()).await {
            Ok(cache) => return cache,
            Err(object_store_cache::Error::Locked { .. }) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Err(e) => panic!("open cache: {e:?}"),
        }
    }
}

const BUCKET: &str = "s3://bucket/";

fn wrap(cache: &Arc<NodeCache>, store: &Arc<TestStore>) -> Arc<dyn ObjectStore> {
    cache.wrap(
        BUCKET,
        Arc::clone(store) as Arc<dyn ObjectStore>,
        all_paths(),
    )
}

async fn read(cached: &Arc<dyn ObjectStore>, p: &Path, r: std::ops::Range<u64>, want: &Bytes) {
    let got = cached.get_range(p, r.clone()).await.expect("read");
    let span = usize::try_from(r.start).expect("start")..usize::try_from(r.end).expect("end");
    assert_same(&got, &want.slice(span), &format!("{p} {r:?}"));
}

/// GETs `f` made, read from the store's counter around it.
async fn gets_during<F: std::future::Future<Output = ()>>(store: &TestStore, f: F) -> u64 {
    let before = store.gets();
    f.await;
    store.gets() - before
}

/// Nearest-rank percentile `p` (0–100) of `samples`.
fn percentile(samples: &[Duration], p: usize) -> Duration {
    let mut sorted = samples.to_vec();
    sorted.sort();
    let rank = (p * sorted.len()).div_ceil(100).max(1);
    sorted[rank - 1]
}

// ---------------------------------------------------------------------------
// Prefetch yields to queries
// ---------------------------------------------------------------------------

/// One query: a HEAD (the object is not yet sized), its footer, then eight
/// dependent 1 MiB reads. Returns how long it took.
async fn timed_query(cached: &Arc<dyn ObjectStore>, p: &Path, want: &Bytes) -> Duration {
    let size = want.len() as u64;
    let t0 = Instant::now();
    read(cached, p, size - 64 * 1024..size, want).await;
    for i in 0..8u64 {
        read(cached, p, i * MIB..(i + 1) * MIB, want).await;
    }
    t0.elapsed()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn first_read_not_delayed_by_prefetch() {
    // A 25 ms first byte and a 150 MB/s link shared in arrival order. Eight
    // prefetch chunks of 1 MiB (7 ms on the link each) in flight saturate it,
    // so a query read that queues behind new chunks waits about 24 ms more;
    // one that prefetch yields to waits only for the chunks already in
    // flight, mostly during its HEAD. Arms alternate, a fresh cache and store
    // each trial.
    const TRIALS: usize = 10;
    let query = data(1, 12 * MIB);
    let big = data(2, 128 * MIB);
    let qp = Path::from("t/query.vortex");
    let bp = Path::from("t/big.vortex");
    let block = 256 * 1024;
    let mut with = Vec::new();
    let mut without = Vec::new();
    for trial in 0..2 * TRIALS {
        let prefetching = trial % 2 == 0;
        let store = Arc::new(ModeledStore::new(Duration::from_millis(25), 150e6));
        for (p, b) in [(&qp, &query), (&bp, &big)] {
            store
                .inner
                .put(p, PutPayload::from_bytes(b.clone()))
                .await
                .expect("put");
        }
        let tmp = tempfile::tempdir().expect("tempdir");
        let cache = open(tmp.path(), 1 << 30, block, roomy()).await;
        let cached = cache.wrap(
            BUCKET,
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            all_paths(),
        );
        if prefetching {
            cache
                .prefetch(BUCKET, &bp, big.len() as u64, None)
                .expect("prefetch");
            // Steady state: the link is busy with prefetch chunks.
            tokio::time::sleep(Duration::from_millis(150)).await;
            assert!(
                cache.stats().fetched_bytes_prefetch > 0,
                "prefetch is running"
            );
        }
        let took = timed_query(&cached, &qp, &query).await;
        if prefetching {
            let stats = cache.stats();
            assert!(
                stats.fetched_bytes_prefetch < big.len() as u64,
                "prefetch still had work when the query ran: {stats:?}"
            );
            with.push(took);
        } else {
            without.push(took);
        }
    }
    let (p90_with, p90_without) = (percentile(&with, 90), percentile(&without, 90));
    eprintln!("query with prefetch running: {with:?}");
    eprintln!("query with no prefetch: {without:?}");
    eprintln!("P90 with {p90_with:?}, without {p90_without:?}");
    assert!(
        p90_with.as_secs_f64() <= 1.25 * p90_without.as_secs_f64(),
        "P90 of {TRIALS} first queries with prefetch running is {p90_with:?}, more than 1.25x the {p90_without:?} with none"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prefetch_backs_off_after_slow_disk_read() {
    // A query's hit that takes 100 ms on disk (over the 20 ms threshold)
    // holds prefetch back for the 600 ms back-off period.
    let store = Arc::new(TestStore::default());
    let x = data(3, MIB);
    let y = data(4, MIB);
    put(&store, "t/x.vortex", &x).await;
    put(&store, "t/y.vortex", &y).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let slabs = SlabStore::open(tmp.path(), 64 * MIB).await.expect("slabs");
    let slow = Arc::new(FaultStore {
        get_delay: Duration::from_millis(100),
        ..FaultStore::new(slabs)
    });
    let backoff = Duration::from_millis(600);
    let cache = testing::with_block_store(
        config(tmp.path(), 1 << 30),
        Tuning {
            block_bytes: MIB,
            prefetch_quiet: Duration::from_millis(1),
            prefetch_slow_read: Duration::from_millis(20),
            prefetch_backoff: backoff,
            ..roomy()
        },
        Arc::clone(&slow) as Arc<dyn BlockStore>,
    )
    .expect("cache");
    let cached = wrap(&cache, &store);
    let (xp, yp) = (Path::from("t/x.vortex"), Path::from("t/y.vortex"));
    read(&cached, &xp, 0..MIB, &x).await;
    cache.wait_idle().await;

    slow.slow_gets.store(true, Ordering::SeqCst);
    read(&cached, &xp, 0..1000, &x).await;
    let slow_read_end = Instant::now();
    let started = store.get_started.notified();
    let mut started = std::pin::pin!(started);
    started.as_mut().enable();
    cache.prefetch(BUCKET, &yp, MIB, None).expect("prefetch");
    tokio::time::timeout(Duration::from_secs(5), started)
        .await
        .expect("the prefetch GET started within 5 s");
    let waited = slow_read_end.elapsed();
    assert!(
        waited >= Duration::from_millis(500) && waited < Duration::from_secs(2),
        "prefetch started {waited:?} after a slow disk read; the back-off is {backoff:?}"
    );
    cache.wait_idle().await;
    assert_eq!(cache.stats().fetched_bytes_prefetch, MIB);
}

// ---------------------------------------------------------------------------
// What prefetch fetches
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prefetch_fills_named_ranges() {
    let store = Arc::new(TestStore::default());
    let obj = data(5, 8 * MIB);
    put(&store, "t/a.vortex", &obj).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = open(tmp.path(), 1 << 30, MIB, roomy()).await;
    let cached = wrap(&cache, &store);
    let p = Path::from("t/a.vortex");
    // Block 0, and blocks 5 and 6.
    let ranges = vec![0..100, 5 * MIB + 10..6 * MIB + 5];
    cache
        .prefetch(BUCKET, &p, 8 * MIB, Some(ranges.clone()))
        .expect("prefetch");
    cache.wait_idle().await;
    let stats = cache.stats();
    assert_eq!(stats.entries, 3, "{stats:?}");
    assert_eq!(stats.fetched_bytes_prefetch, 3 * MIB, "{stats:?}");
    // One GET per run of consecutive blocks.
    assert_eq!(store.gets(), 2, "{stats:?}");

    let gets = gets_during(&store, async {
        for r in &ranges {
            read(&cached, &p, r.clone(), &obj).await;
        }
        read(&cached, &p, 5 * MIB..7 * MIB, &obj).await;
    })
    .await;
    assert_eq!(gets, 0, "reads of prefetched ranges: {:?}", cache.stats());
    assert_eq!(
        store.heads.load(Ordering::SeqCst),
        0,
        "sized from the blocks"
    );
    let gets = gets_during(&store, read(&cached, &p, 2 * MIB..2 * MIB + 1, &obj)).await;
    assert_eq!(gets, 1, "block 2 was not asked for");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prefetch_around_a_resident_block_keeps_each_block_its_bytes() {
    // Block 1 of a four-block object is resident. A prefetch of the whole
    // object (one chunk of four blocks) fetches blocks 0, 2 and 3, as two
    // GETs: every block written must hold its own bytes.
    let store = Arc::new(TestStore::default());
    let obj = data(32, 4 * MIB);
    put(&store, "t/a.vortex", &obj).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = open(tmp.path(), 1 << 30, MIB, roomy()).await;
    let cached = wrap(&cache, &store);
    let p = Path::from("t/a.vortex");
    read(&cached, &p, MIB..2 * MIB, &obj).await;
    cache.wait_idle().await;
    let gets = store.gets();
    cache.prefetch(BUCKET, &p, 4 * MIB, None).expect("prefetch");
    cache.wait_idle().await;
    let prefetch_gets = store.gets() - gets;
    for i in 0..4u64 {
        let got = gets_during(&store, read(&cached, &p, i * MIB..(i + 1) * MIB, &obj)).await;
        assert_eq!(got, 0, "block {i}");
    }
    assert_eq!(prefetch_gets, 2, "{:?}", cache.stats());
    let stats = cache.stats();
    assert_eq!(stats.entries, 4, "{stats:?}");
    assert_eq!(stats.fetched_bytes_prefetch, 3 * MIB, "{stats:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_fetch_per_block() {
    // (a) A prefetch is fetching the block when 64 readers ask for it: they
    // wait on its fetch.
    let store = Arc::new(TestStore::default());
    let obj = data(6, MIB);
    put(&store, "t/a.vortex", &obj).await;
    *store.get_delay.lock() = Duration::from_millis(300);
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = open(tmp.path(), 1 << 30, MIB, roomy()).await;
    let cached = wrap(&cache, &store);
    let p = Path::from("t/a.vortex");
    let started = store.get_started.notified();
    let mut started = std::pin::pin!(started);
    started.as_mut().enable();
    cache.prefetch(BUCKET, &p, MIB, None).expect("prefetch");
    tokio::time::timeout(Duration::from_secs(5), started)
        .await
        .expect("the prefetch GET started");
    let readers: Vec<_> = (0..64u64)
        .map(|i| {
            let cached = Arc::clone(&cached);
            let p = p.clone();
            tokio::spawn(async move { cached.get_range(&p, i * 100..i * 100 + 1000).await })
        })
        .collect();
    for (i, r) in (0u64..).zip(readers) {
        let got = r.await.expect("join").expect("read");
        let from = usize::try_from(i * 100).expect("from");
        assert_same(&got, &obj.slice(from..from + 1000), "reader");
    }
    cache.wait_idle().await;
    let stats = cache.stats();
    assert_eq!(store.gets(), 1, "{stats:?}");
    assert_eq!(stats.entries, 1, "{stats:?}");
    assert_eq!(stats.shared_wait_timeouts, 0, "{stats:?}");

    // (b) The prefetch's GET stalls: a reader waits one shared wait (200 ms),
    // then reads the store itself.
    let store = Arc::new(TestStore::default());
    put(&store, "t/a.vortex", &obj).await;
    let gate = Arc::new(Notify::new());
    *store.stall.lock() = Some((p.clone(), Arc::clone(&gate)));
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = open(
        tmp.path(),
        1 << 30,
        MIB,
        Tuning {
            shared_fetch_wait: Duration::from_millis(200),
            ..roomy()
        },
    )
    .await;
    let cached = wrap(&cache, &store);
    let started = store.get_started.notified();
    let mut started = std::pin::pin!(started);
    started.as_mut().enable();
    cache.prefetch(BUCKET, &p, MIB, None).expect("prefetch");
    tokio::time::timeout(Duration::from_secs(5), started)
        .await
        .expect("the prefetch GET started");
    let t0 = Instant::now();
    read(&cached, &p, 0..1000, &obj).await;
    let waited = t0.elapsed();
    assert!(
        waited >= Duration::from_millis(200) && waited < Duration::from_secs(1),
        "the reader waited {waited:?} on a stalled prefetch; the bound is 200 ms"
    );
    assert_eq!(cache.stats().shared_wait_timeouts, 1);
    assert_eq!(store.gets(), 2);
    gate.notify_one();
    cache.wait_idle().await;
}

// ---------------------------------------------------------------------------
// The policy: free room only
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_does_not_evict_hotter_blocks() {
    // Seven hot blocks (read three times each) fill an 8 MiB budget to just
    // below its low watermark. A prefetch of an object a query read recently
    // (through a scan, which admits nothing) is declined: none of its blocks
    // fits below the low watermark, and prefetch never evicts.
    let store = Arc::new(TestStore::default());
    let hot = data(8, 7 * MIB);
    let cold = data(9, 4 * MIB);
    put(&store, "t/hot.vortex", &hot).await;
    put(&store, "t/cold.vortex", &cold).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = open(tmp.path(), 8 * MIB, MIB, roomy()).await;
    let cached = wrap(&cache, &store);
    let (hp, cp) = (Path::from("t/hot.vortex"), Path::from("t/cold.vortex"));
    // One write at a time: a running write counts in the budget twice for a
    // moment, which near the high watermark would reclaim a slab early.
    for _ in 0..3 {
        for i in 0..7u64 {
            read(&cached, &hp, i * MIB..(i + 1) * MIB, &hot).await;
            cache.wait_idle().await;
        }
    }
    let filled = cache.stats();
    assert_eq!(
        (filled.entries, filled.evicted_bytes_size),
        (7, 0),
        "{filled:?}"
    );
    // A scan of the cold object: it admits nothing.
    read(&cached, &cp, 0..4 * MIB, &cold).await;
    cache.wait_idle().await;
    let before = cache.stats();
    assert_eq!(before.entries, 7, "{before:?}");
    assert_eq!(before.admission_bypass_reads, 1, "{before:?}");

    let gets = store.gets();
    cache
        .prefetch(BUCKET, &cp, 4 * MIB, None)
        .expect("prefetch");
    cache.wait_idle().await;
    let stats = cache.stats();
    assert_eq!(store.gets() - gets, 0, "nothing fetched: {stats:?}");
    assert_eq!(stats.prefetch_declined_bytes, 4 * MIB, "{stats:?}");
    assert_eq!(stats.fetched_bytes_prefetch, 0, "{stats:?}");
    assert_eq!(stats.evicted_bytes_size, 0, "{stats:?}");
    let hot_gets = gets_during(&store, async {
        for i in 0..7u64 {
            read(&cached, &hp, i * MIB..i * MIB + 10, &hot).await;
        }
    })
    .await;
    assert_eq!(hot_gets, 0, "every hot block stayed: {:?}", cache.stats());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_prefetches_into_free_room() {
    // Free room below the low watermark: a never-read object is prefetched.
    let store = Arc::new(TestStore::default());
    let obj = data(10, 4 * MIB);
    put(&store, "t/a.vortex", &obj).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = open(tmp.path(), 64 * MIB, MIB, roomy()).await;
    let cached = wrap(&cache, &store);
    let p = Path::from("t/a.vortex");
    cache.prefetch(BUCKET, &p, 4 * MIB, None).expect("prefetch");
    cache.wait_idle().await;
    let stats = cache.stats();
    assert_eq!(stats.entries, 4, "{stats:?}");
    assert_eq!(stats.fetched_bytes_prefetch, 4 * MIB, "{stats:?}");
    assert_eq!(stats.prefetch_declined_bytes, 0, "{stats:?}");
    assert_eq!(store.gets(), 1, "one chunk of four blocks");
    let gets = gets_during(&store, read(&cached, &p, 0..4 * MIB, &obj)).await;
    assert_eq!(gets, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[expect(clippy::single_range_in_vec_init, reason = "lists of one byte range")]
async fn prefetch_returns_the_bytes_it_queued_and_declined() {
    let store = Arc::new(TestStore::default());
    let a = Path::from("t/a.vortex");
    let b = Path::from("t/b.vortex");
    let c = Path::from("t/c.vortex");
    let size_a = 3 * MIB + 5;
    put(&store, a.as_ref(), &data(30, size_a)).await;
    let b_bytes = data(31, 4 * MIB);
    put(&store, b.as_ref(), &b_bytes).await;
    put(&store, c.as_ref(), &data(32, 16 * MIB)).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    // An 8 MiB budget: the low watermark (90 %) holds seven 1 MiB entries.
    let cache = open(tmp.path(), 8 * MIB, MIB, roomy()).await;
    let cached = wrap(&cache, &store);

    // Nothing resident: the whole object, partial last block included.
    assert_eq!(
        cache.prefetch(BUCKET, &a, size_a, None).expect("a"),
        Prefetched {
            queued_bytes: size_a,
            declined_bytes: 0,
        }
    );
    cache.wait_idle().await;
    assert_eq!(cache.stats().entries, 4);
    // Resident already: nothing queued, nothing fetched.
    let gets = store.gets();
    assert_eq!(
        cache.prefetch(BUCKET, &a, size_a, None).expect("a again"),
        Prefetched::default()
    );
    cache.wait_idle().await;
    assert_eq!(store.gets(), gets);
    // Block 0 of b resident: of the first two blocks only block 1 is queued.
    read(&cached, &b, 0..10, &b_bytes).await;
    cache.wait_idle().await;
    assert_eq!(
        cache
            .prefetch(BUCKET, &b, 4 * MIB, Some(vec![0..2 * MIB]))
            .expect("b"),
        Prefetched {
            queued_bytes: MIB,
            declined_bytes: 0,
        }
    );
    cache.wait_idle().await;
    assert_eq!(cache.stats().entries, 6);
    // Five full entries and a small one below 7.2 MiB: two blocks of c fit,
    // the other fourteen are declined.
    let declined = cache.stats().prefetch_declined_bytes;
    assert_eq!(
        cache.prefetch(BUCKET, &c, 16 * MIB, None).expect("c"),
        Prefetched {
            queued_bytes: 2 * MIB,
            declined_bytes: 14 * MIB,
        }
    );
    cache.wait_idle().await;
    let s = cache.stats();
    assert_eq!(s.entries, 8, "{s:?}");
    assert_eq!(s.prefetch_declined_bytes - declined, 14 * MIB, "{s:?}");
    // No free room left: every block not resident is declined.
    assert_eq!(
        cache.prefetch(BUCKET, &c, 16 * MIB, None).expect("c again"),
        Prefetched {
            queued_bytes: 0,
            declined_bytes: 14 * MIB,
        }
    );
}

// ---------------------------------------------------------------------------
// Admission classes
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scan_read_bypasses_admission() {
    // An 8 MiB budget makes reads over 1 MiB scans.
    let store = Arc::new(TestStore::default());
    let hot = data(13, 4 * MIB);
    let scan = data(14, 6 * MIB);
    put(&store, "t/hot.vortex", &hot).await;
    put(&store, "t/scan.vortex", &scan).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = open(tmp.path(), 8 * MIB, MIB, roomy()).await;
    let cached = wrap(&cache, &store);
    let (hp, sp) = (Path::from("t/hot.vortex"), Path::from("t/scan.vortex"));
    for _ in 0..3 {
        for i in 0..4u64 {
            read(&cached, &hp, i * MIB..(i + 1) * MIB, &hot).await;
        }
        cache.wait_idle().await;
    }
    let before = cache.stats();
    assert_eq!(before.entries, 4, "{before:?}");

    // A scan is served and admits nothing.
    let got = cached
        .get(&sp)
        .await
        .expect("get")
        .bytes()
        .await
        .expect("bytes");
    assert_same(&got, &scan, "scan");
    cache.wait_idle().await;
    let stats = cache.stats();
    assert_eq!(stats.entries, 4, "the scan admitted blocks: {stats:?}");
    assert_eq!(stats.fill_bytes, before.fill_bytes, "{stats:?}");
    assert_eq!(stats.admission_bypass_reads, 1, "{stats:?}");
    let gets = gets_during(&store, async {
        for i in 0..4u64 {
            read(&cached, &hp, i * MIB..i * MIB + 10, &hot).await;
        }
    })
    .await;
    assert_eq!(gets, 0, "hot blocks stayed resident");

    // A query read of the scanned object's first block fetches it.
    let gets = gets_during(&store, read(&cached, &sp, 0..10, &scan)).await;
    assert_eq!(gets, 1, "the scanned block was not cached");
}

// ---------------------------------------------------------------------------
// Write lanes
// ---------------------------------------------------------------------------

/// A cache over a slab store whose puts take `put_delay`, with one write at
/// a time and a queue of one per lane.
async fn slow_writer_cache(dir: &std::path::Path, put_delay: Duration) -> Arc<NodeCache> {
    let slabs = SlabStore::open(dir, 64 * MIB).await.expect("slabs");
    let slow = Arc::new(FaultStore {
        put_delay,
        ..FaultStore::new(slabs)
    });
    testing::with_block_store(
        config(dir, 1 << 30),
        Tuning {
            block_bytes: MIB,
            write_queue: 1,
            concurrent_writes: 1,
            scan_bytes: Some(u64::MAX),
            ..roomy()
        },
        slow as Arc<dyn BlockStore>,
    )
    .expect("cache")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prefetch_fill_not_dropped_when_queue_full() {
    // The demand read fills the one-slot queues while each put takes 100 ms,
    // so demand fills are dropped; the prefetch's four fills wait for room in
    // their queue and land.
    let store = Arc::new(TestStore::default());
    let demand = data(17, 8 * MIB);
    let prefetched = data(18, 4 * MIB);
    put(&store, "t/demand.vortex", &demand).await;
    put(&store, "t/prefetched.vortex", &prefetched).await;
    let (dp, pp) = (
        Path::from("t/demand.vortex"),
        Path::from("t/prefetched.vortex"),
    );
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = slow_writer_cache(tmp.path(), Duration::from_millis(100)).await;
    let cached = wrap(&cache, &store);
    read(&cached, &dp, 0..8 * MIB, &demand).await;
    cache
        .prefetch(BUCKET, &pp, 4 * MIB, None)
        .expect("prefetch");
    cache.wait_idle().await;
    let stats = cache.stats();
    assert!(stats.dropped_fills >= 1, "the queue was full: {stats:?}");
    assert_eq!(stats.entries, 4 + 8 - stats.dropped_fills, "{stats:?}");
    let gets = gets_during(&store, read(&cached, &pp, 0..4 * MIB, &prefetched)).await;
    assert_eq!(
        gets,
        0,
        "every prefetched block was written: {:?}",
        cache.stats()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn demand_fill_drop_is_counted() {
    let store = Arc::new(TestStore::default());
    let obj = data(19, 8 * MIB);
    put(&store, "t/a.vortex", &obj).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = slow_writer_cache(tmp.path(), Duration::from_millis(200)).await;
    let cached = wrap(&cache, &store);
    read(&cached, &Path::from("t/a.vortex"), 0..8 * MIB, &obj).await;
    cache.wait_idle().await;
    let stats = cache.stats();
    // One write running and one queued; the other six or so are dropped.
    assert!(stats.dropped_fills >= 6, "{stats:?}");
    assert_eq!(stats.entries + stats.dropped_fills, 8, "{stats:?}");
    assert_eq!(stats.fill_bytes, stats.entries * MIB, "{stats:?}");
}

// ---------------------------------------------------------------------------
// Restart: metadata and clean close
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn etag_survives_restart() {
    // Round 0 closes cleanly (the index carries the metadata); round 1 drops
    // the cache without closing (the slab is rescanned and the entry headers
    // carry it).
    let store = Arc::new(TestStore::default());
    let tmp = tempfile::tempdir().expect("tempdir");
    for (round, close) in [(0, true), (1, false)] {
        let p = Path::from(format!("t/obj-{round}.vortex"));
        let obj = data(22 + round, 3 * MIB);
        put(&store, p.as_ref(), &obj).await;
        let want = store.inner.head(&p).await.expect("head");
        assert!(want.e_tag.is_some());
        {
            let cache = open(tmp.path(), 1 << 30, MIB, roomy()).await;
            let cached = wrap(&cache, &store);
            read(&cached, &p, 0..3 * MIB, &obj).await;
            cache.wait_idle().await;
            if close {
                cache.close().await.expect("close");
            }
        }
        let heads = store.heads.load(Ordering::SeqCst);
        let cache = open(tmp.path(), 1 << 30, MIB, roomy()).await;
        let cached = wrap(&cache, &store);
        let got = cached.get(&p).await.expect("get");
        assert_eq!(got.meta.e_tag, want.e_tag, "round {round}");
        assert_eq!(got.meta.last_modified, want.last_modified, "round {round}");
        assert_eq!(got.meta.size, want.size, "round {round}");
        assert_same(&got.bytes().await.expect("bytes"), &obj, "after restart");
        assert_eq!(
            store.heads.load(Ordering::SeqCst) - heads,
            0,
            "round {round}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn close_then_open_does_not_rescan() {
    // The last-written entry's data is corrupted while the cache is closed.
    // An open that reads only indexes lists it (and the read that finds the
    // bad CRC refetches it); an open that rescans the slab verifies every
    // entry's data and drops it. So after `close` the entry is listed, after
    // a drop without `close` it is not.
    let store = Arc::new(TestStore::default());
    let obj = data(30, 4 * MIB);
    put(&store, "t/a.vortex", &obj).await;
    let p = Path::from("t/a.vortex");
    for close in [true, false] {
        let tmp = tempfile::tempdir().expect("tempdir");
        {
            let cache = open(tmp.path(), 1 << 30, MIB, roomy()).await;
            let cached = wrap(&cache, &store);
            read(&cached, &p, 0..4 * MIB, &obj).await;
            cache.wait_idle().await;
            if close {
                cache.close().await.expect("close");
            }
        }
        let slabs = tmp.path().join("slabs");
        let has_idx = slabs.join("slab-00000000.idx").exists();
        assert_eq!(has_idx, close, "close={close}: index written");
        // The entry at the highest offset was written last.
        let (file, at, block) = (0..4u64)
            .map(|i| {
                let needle = obj.slice(
                    usize::try_from(i * MIB).expect("i")..usize::try_from(i * MIB + 64).expect("i"),
                );
                let (file, at) = find_in_slabs(tmp.path(), &needle);
                (file, at, i)
            })
            .max_by_key(|(_, at, _)| *at)
            .expect("four blocks");
        {
            use std::os::unix::fs::FileExt;
            let f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&file)
                .expect("open slab");
            let mut b = [0u8; 1];
            f.read_exact_at(&mut b, at + 1000).expect("read");
            b[0] ^= 0xff;
            f.write_all_at(&b, at + 1000).expect("write");
        }
        let cache = open(tmp.path(), 1 << 30, MIB, roomy()).await;
        let listed = cache.stats().entries;
        let want = if close { 4 } else { 3 };
        assert_eq!(listed, want, "close={close}: entries listed at open");
        let cached = wrap(&cache, &store);
        let gets = gets_during(
            &store,
            read(&cached, &p, block * MIB..(block + 1) * MIB, &obj),
        )
        .await;
        assert_eq!(gets, 1, "close={close}: the damaged block is refetched");
        let stats = cache.stats();
        assert_eq!(
            stats.fallbacks,
            u64::from(close),
            "close={close}: {stats:?}"
        );
    }

    // Close with entry writes in flight: one write at a time, each put taking
    // 300 ms. `close` waits for the running write and skips the queued ones,
    // then seals the slab. The next open lists that one entry from the index:
    // its data is damaged first, which a rescan would have dropped.
    let tmp = tempfile::tempdir().expect("tempdir");
    let slow_cache = |put_delay: Duration| {
        let dir = tmp.path().to_path_buf();
        async move {
            let slabs = SlabStore::open(&dir, 64 * MIB).await.expect("slabs");
            let slow = Arc::new(FaultStore {
                put_delay,
                ..FaultStore::new(slabs)
            });
            let cache = testing::with_block_store(
                config(&dir, 1 << 30),
                Tuning {
                    block_bytes: MIB,
                    concurrent_writes: 1,
                    ..roomy()
                },
                Arc::clone(&slow) as Arc<dyn BlockStore>,
            )
            .expect("cache");
            (cache, slow)
        }
    };
    {
        let (cache, slow) = slow_cache(Duration::from_millis(300)).await;
        let cached = wrap(&cache, &store);
        read(&cached, &p, 0..4 * MIB, &obj).await;
        let deadline = Instant::now() + Duration::from_secs(5);
        while slow.puts_started.load(Ordering::SeqCst) == 0 {
            assert!(Instant::now() < deadline, "no entry write started");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        cache.close().await.expect("close");
        let stats = cache.stats();
        assert_eq!(
            (stats.entries, slow.puts_started.load(Ordering::SeqCst)),
            (1, 1),
            "the running write landed, the queued ones were skipped: {stats:?}"
        );
    }
    let slabs = tmp.path().join("slabs");
    assert!(slabs.join("slab-00000000.idx").exists(), "index written");
    let block = (0..4u64)
        .find(|i| {
            let from = usize::try_from(i * MIB).expect("i");
            find_block(tmp.path(), &obj.slice(from..from + 64))
        })
        .expect("the written block");
    let (file, at) = {
        let from = usize::try_from(block * MIB).expect("i");
        find_in_slabs(tmp.path(), &obj.slice(from..from + 64))
    };
    {
        use std::os::unix::fs::FileExt;
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file)
            .expect("open slab");
        let mut b = [0u8; 1];
        f.read_exact_at(&mut b, at + 1000).expect("read");
        b[0] ^= 0xff;
        f.write_all_at(&b, at + 1000).expect("write");
    }
    let (cache, _) = slow_cache(Duration::ZERO).await;
    assert_eq!(cache.stats().entries, 1, "listed from the index");
    let cached = wrap(&cache, &store);
    let gets = gets_during(
        &store,
        read(&cached, &p, block * MIB..(block + 1) * MIB, &obj),
    )
    .await;
    assert_eq!(gets, 1, "the damaged block is refetched");
    assert_eq!(cache.stats().fallbacks, 1);
}

/// True when some slab holds `needle`.
fn find_block(dir: &std::path::Path, needle: &[u8]) -> bool {
    std::fs::read_dir(dir.join("slabs"))
        .expect("list slabs")
        .map(|e| e.expect("entry").path())
        .filter(|p| p.extension().is_some_and(|e| e == "dat"))
        .any(|p| {
            std::fs::read(&p)
                .expect("read slab")
                .windows(needle.len())
                .any(|w| w == needle)
        })
}

#[tokio::test]
async fn prefetch_of_unknown_store_is_an_error() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = open(tmp.path(), 1 << 30, MIB, roomy()).await;
    let err = cache
        .prefetch("s3://other/", &Path::from("t/a.vortex"), MIB, None)
        .expect_err("not wrapped");
    assert!(
        matches!(&err, object_store_cache::Error::UnknownStore { namespace } if namespace == "s3://other/"),
        "{err:?}"
    );
}

// ---------------------------------------------------------------------------
// Review regressions
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queued_prefetch_does_not_resurrect_an_evicted_object() {
    // `b` has a resident block. A prefetch of `a` stalls in its GET, and a
    // prefetch of `b` queues behind it (one prefetch job at a time). `b` is
    // evicted by reference while its prefetch is queued, then `a` is
    // released: `b`'s prefetch must not write `b` back, in this process or
    // after a restart.
    let store = Arc::new(TestStore::default());
    let a = data(50, 4 * MIB);
    let b = data(51, 4 * MIB);
    put(&store, "t/a.vortex", &a).await;
    put(&store, "t/b.vortex", &b).await;
    let (ap, bp) = (Path::from("t/a.vortex"), Path::from("t/b.vortex"));
    let tmp = tempfile::tempdir().expect("tempdir");
    let options = Tuning {
        prefetch_jobs_in_flight: 1,
        ..roomy()
    };
    let cache = open(tmp.path(), 1 << 30, MIB, options.clone()).await;
    let cached = wrap(&cache, &store);
    read(&cached, &bp, 0..MIB, &b).await;
    cache.wait_idle().await;
    assert_eq!(cache.stats().entries, 1);

    let gate = Arc::new(Notify::new());
    *store.stall.lock() = Some((ap.clone(), Arc::clone(&gate)));
    let started = store.get_started.notified();
    let mut started = std::pin::pin!(started);
    started.as_mut().enable();
    cache
        .prefetch(BUCKET, &ap, 4 * MIB, None)
        .expect("prefetch a");
    tokio::time::timeout(Duration::from_secs(5), started)
        .await
        .expect("a's prefetch GET started");
    cache
        .prefetch(BUCKET, &bp, 4 * MIB, None)
        .expect("prefetch b");
    let evicted = cache
        .evict_unreferenced(BUCKET, &Path::from(""), |p| p.as_ref() != "t/b.vortex")
        .await;
    assert_eq!(evicted, MIB, "b's resident block");
    assert_eq!(cache.stats().entries, 0);
    let gets = store.gets();
    gate.notify_one();
    cache.wait_idle().await;
    let stats = cache.stats();
    assert_eq!(store.gets() - gets, 0, "b was fetched: {stats:?}");
    assert_eq!(stats.entries, 4, "a's blocks only: {stats:?}");
    assert_eq!(stats.fetched_bytes_prefetch, 4 * MIB, "{stats:?}");

    cache.close().await.expect("close");
    drop(cached);
    drop(cache);
    let cache = open(tmp.path(), 1 << 30, MIB, options).await;
    assert_eq!(cache.stats().entries, 4, "{:?}", cache.stats());
    let check = wrap(&cache, &store);
    let b_gets = gets_during(&store, async {
        for i in 0..4u64 {
            read(&check, &bp, i * MIB..i * MIB + 10, &b).await;
        }
    })
    .await;
    assert_eq!(b_gets, 4, "no block of b after a restart");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn omitted_metadata_is_asked_of_the_store() {
    // A key long enough that the object's metadata does not fit beside it in
    // the entry header. Dropped without `close`, the slab is rescanned at the
    // next open and the entry comes back without metadata: a read that
    // returns metadata must report the store's (asking it once), not an
    // epoch timestamp and no e-tag, while the bytes still come from the
    // cache.
    let store = Arc::new(TestStore::default());
    // key = 8 + store URL + 8 + path + 24 bytes; the header holds 4072 bytes
    // of key and metadata, and the metadata here takes 16.
    let key_overhead = 8 + BUCKET.len() + 8 + 24;
    let path_len = 4072 - 4 - key_overhead;
    let p = Path::from(format!("t/{}", "x".repeat(path_len - 2)));
    assert_eq!(p.as_ref().len(), path_len);
    let obj = data(52, 3 * MIB);
    put(&store, p.as_ref(), &obj).await;
    let want = store.inner.head(&p).await.expect("head");
    assert!(want.e_tag.is_some());
    let tmp = tempfile::tempdir().expect("tempdir");
    {
        let cache = open(tmp.path(), 1 << 30, MIB, roomy()).await;
        let cached = wrap(&cache, &store);
        read(&cached, &p, 0..3 * MIB, &obj).await;
        cache.wait_idle().await;
        assert_eq!(cache.stats().entries, 3);
    }
    let cache = open(tmp.path(), 1 << 30, MIB, roomy()).await;
    assert_eq!(cache.stats().entries, 3, "rescanned");
    let cached = wrap(&cache, &store);
    let (gets, heads) = (store.gets(), store.heads.load(Ordering::SeqCst));
    let got = cached.get(&p).await.expect("get");
    assert_eq!(got.meta.size, want.size);
    assert_eq!(got.meta.e_tag, want.e_tag);
    assert_eq!(got.meta.last_modified, want.last_modified);
    assert_same(&got.bytes().await.expect("bytes"), &obj, "after rescan");
    assert_eq!(
        (
            store.gets() - gets,
            store.heads.load(Ordering::SeqCst) - heads
        ),
        (0, 1)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bypassed_query_reads_hold_prefetch_back() {
    // A query read the cache passes to the store (an explicit HEAD, a
    // conditional GET, a read of a rejected path, by `get_opts` or
    // `get_ranges`) stalls in the store. While it waits, a prefetch queued
    // for another object starts no GET; once it returns, the prefetch runs.
    for case in ["head", "conditional", "rejected", "rejected ranges"] {
        let store = Arc::new(TestStore::default());
        let s_bytes = data(70, MIB);
        let y = data(71, MIB);
        let sp = if case.starts_with("rejected") {
            Path::from("t/_meta/head.json")
        } else {
            Path::from("t/s.vortex")
        };
        let yp = Path::from("t/y.vortex");
        put(&store, sp.as_ref(), &s_bytes).await;
        put(&store, yp.as_ref(), &y).await;
        let e_tag = store.inner.head(&sp).await.expect("head").e_tag;
        let tmp = tempfile::tempdir().expect("tempdir");
        let cache = open(tmp.path(), 1 << 30, MIB, roomy()).await;
        let cached = cache.wrap(
            BUCKET,
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            Arc::new(|p: &Path| !p.as_ref().contains("_meta")),
        );
        let gate = Arc::new(Notify::new());
        store.stall_heads.store(case == "head", Ordering::SeqCst);
        *store.stall.lock() = Some((sp.clone(), Arc::clone(&gate)));
        let started = store.get_started.notified();
        let mut started = std::pin::pin!(started);
        started.as_mut().enable();
        let request = {
            let (cached, sp) = (Arc::clone(&cached), sp.clone());
            tokio::spawn(async move {
                match case {
                    "head" => cached.head(&sp).await.map(|m| m.size),
                    "conditional" => {
                        let opts = object_store::GetOptions {
                            if_match: e_tag,
                            ..Default::default()
                        };
                        let got = cached.get_opts(&sp, opts).await?;
                        got.bytes().await.map(|b| b.len() as u64)
                    }
                    "rejected" => {
                        let got = cached.get(&sp).await?;
                        got.bytes().await.map(|b| b.len() as u64)
                    }
                    _ => cached
                        .get_ranges(&sp, &[0..10, 20..30])
                        .await
                        .map(|v| v[0].len() as u64),
                }
            })
        };
        tokio::time::timeout(Duration::from_secs(5), started)
            .await
            .expect("the query read reached the store");
        let gets = store.gets();
        cache.prefetch(BUCKET, &yp, MIB, None).expect("prefetch");
        // Time is under test: nothing may start within this window.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            (store.gets() - gets, cache.stats().fetched_bytes_prefetch),
            (0, 0),
            "{case}: prefetch started while a query read waited on the store"
        );
        gate.notify_one();
        let got = request.await.expect("join").expect("request");
        let want = if case == "rejected ranges" { 10 } else { MIB };
        assert_eq!(got, want, "{case}");
        cache.wait_idle().await;
        assert_eq!(cache.stats().fetched_bytes_prefetch, MIB, "{case}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stalled_prefetch_does_not_hold_up_other_jobs() {
    // `a`'s prefetch GET stalls and is not released until the end. A prefetch
    // of `b`, queued after it, must still land.
    let store = Arc::new(TestStore::default());
    let a = data(80, 2 * MIB);
    let b = data(81, 2 * MIB);
    put(&store, "t/a.vortex", &a).await;
    put(&store, "t/b.vortex", &b).await;
    let (ap, bp) = (Path::from("t/a.vortex"), Path::from("t/b.vortex"));
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = open(tmp.path(), 1 << 30, MIB, roomy()).await;
    let _cached = wrap(&cache, &store);
    let gate = Arc::new(Notify::new());
    *store.stall.lock() = Some((ap.clone(), Arc::clone(&gate)));
    let started = store.get_started.notified();
    let mut started = std::pin::pin!(started);
    started.as_mut().enable();
    cache
        .prefetch(BUCKET, &ap, 2 * MIB, None)
        .expect("prefetch a");
    tokio::time::timeout(Duration::from_secs(5), started)
        .await
        .expect("a's prefetch GET started");
    cache
        .prefetch(BUCKET, &bp, 2 * MIB, None)
        .expect("prefetch b");
    poll_until(Duration::from_secs(5), "b's prefetch landed", || {
        let s = cache.stats();
        s.entries == 2 && s.fetched_bytes_prefetch == 2 * MIB
    })
    .await;
    let check = wrap(&cache, &store);
    let b_gets = gets_during(&store, read(&check, &bp, 0..2 * MIB, &b)).await;
    assert_eq!(b_gets, 0, "{:?}", cache.stats());
    gate.notify_one();
    cache.wait_idle().await;
    assert_eq!(cache.stats().entries, 4, "{:?}", cache.stats());
}

/// Prefetch jobs that have not run count against the room of later
/// prefetches: with no chunk able to start (a query just ended, and prefetch
/// waits an hour after one), only what fits below the low watermark is queued,
/// and at most `prefetch_queue_jobs` jobs; the rest is declined and counted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queued_prefetch_is_bounded_by_room_and_job_count() {
    const BLOCK: u64 = 64 << 10;
    let entry = 4096 + BLOCK;
    let store = Arc::new(TestStore::default());
    let n: u64 = 200;
    let path = |i: u64| Path::from(format!("t/{i:03}.vortex"));
    for i in 0..n {
        put(&store, path(i).as_ref(), &data(i, BLOCK)).await;
    }
    let held_back = Tuning {
        prefetch_quiet: Duration::from_secs(3600),
        prefetch_jobs_in_flight: 1,
        ..roomy()
    };
    for (budget, jobs) in [(8 << 20, 4096), (1 << 30, 3)] {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cache = open(
            tmp.path(),
            budget,
            BLOCK,
            Tuning {
                prefetch_queue_jobs: jobs,
                ..held_back.clone()
            },
        )
        .await;
        let cached = wrap(&cache, &store);
        read(&cached, &path(0), 0..BLOCK, &data(0, BLOCK)).await;
        cache.wait_idle().await;
        let stats = cache.stats();
        let fits = ((stats.low_watermark_bytes - stats.used_bytes) / entry).min(jobs as u64);
        let mut queued = 0;
        for i in 1..n {
            queued += cache
                .prefetch(BUCKET, &path(i), BLOCK, None)
                .expect("prefetch")
                .queued_bytes;
        }
        assert_eq!(queued, fits * BLOCK, "budget {budget}: {:?}", cache.stats());
        assert_eq!(
            cache.stats().prefetch_declined_bytes,
            (n - 1 - fits) * BLOCK,
            "budget {budget}"
        );
    }
}
