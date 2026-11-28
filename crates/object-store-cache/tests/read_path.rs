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

//! The node cache's read path over an in-memory object store: results equal
//! the store's for every range shape, the store's namespace is part of the
//! key (two backends behind one URL never share blocks), HEAD
//! and conditional reads and rejected paths bypass, cache errors (corrupt,
//! unwritable, deleted, failing block store) serve from the store, an
//! unreachable store still serves cached ranges and fails the rest, entry
//! writes stay off the read path, and a wait on a stalled shared fetch is
//! bounded.

use std::fmt;
use std::ops::Range;
use std::os::unix::fs::{FileExt, PermissionsExt};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetRange, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    RenameOptions,
};
use object_store_cache::testing::{self, BlockKey, BlockStore, SlabStore, StoredMeta, Tuning};
use object_store_cache::{Cacheable, NodeCache, NodeCacheConfig, NodeCacheStats};
use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::support::{FaultStore, RoomyVolume};

pub(crate) const MIB: u64 = 1 << 20;

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

/// Called with each path as it reaches [`TestStore`]'s delete.
pub(crate) type DeleteHook = Arc<dyn Fn(&Path) + Send + Sync>;

/// An in-memory store that counts GETs and HEADs, and can fail every call,
/// fail only HEADs, fail deletes, delay every GET, or stall the first GET (or
/// `get_ranges`, or, with `stall_heads`, HEAD) of one path until released (and
/// then fail it, with `fail_stalled`). A delete hook sees each path as its
/// delete arrives.
#[derive(Default)]
pub(crate) struct TestStore {
    pub(crate) inner: InMemory,
    pub(crate) gets: AtomicU64,
    pub(crate) heads: AtomicU64,
    pub(crate) fail: AtomicBool,
    pub(crate) fail_heads: AtomicBool,
    pub(crate) fail_stalled: AtomicBool,
    pub(crate) stall_heads: AtomicBool,
    pub(crate) get_delay: Mutex<Duration>,
    pub(crate) stall: Mutex<Option<(Path, Arc<Notify>)>>,
    pub(crate) get_started: Notify,
    pub(crate) fail_deletes: AtomicBool,
    pub(crate) delete_hook: Mutex<Option<DeleteHook>>,
}

impl TestStore {
    fn check(&self) -> object_store::Result<()> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(object_store::Error::Generic {
                store: "TestStore",
                source: "store unreachable".into(),
            });
        }
        Ok(())
    }

    pub(crate) fn gets(&self) -> u64 {
        self.gets.load(Ordering::SeqCst)
    }

    /// Waits for the release if this is the stalled call; signals
    /// `get_started` first.
    async fn stall_point(&self, location: &Path) -> object_store::Result<()> {
        self.get_started.notify_waiters();
        let gate = {
            let mut stall = self.stall.lock();
            if stall.as_ref().is_some_and(|(p, _)| p == location) {
                stall.take().map(|(_, n)| n)
            } else {
                None
            }
        };
        if let Some(gate) = gate {
            gate.notified().await;
            if self.fail_stalled.load(Ordering::SeqCst) {
                return Err(object_store::Error::Generic {
                    store: "TestStore",
                    source: "stalled GET failed".into(),
                });
            }
        }
        Ok(())
    }
}

impl fmt::Display for TestStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TestStore")
    }
}

impl fmt::Debug for TestStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TestStore")
    }
}

#[async_trait]
impl ObjectStore for TestStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.check()?;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.check()?;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.check()?;
        if options.head {
            self.heads.fetch_add(1, Ordering::SeqCst);
            if self.fail_heads.load(Ordering::SeqCst) {
                return Err(object_store::Error::Generic {
                    store: "TestStore",
                    source: "HEAD refused".into(),
                });
            }
            if self.stall_heads.load(Ordering::SeqCst) {
                self.stall_point(location).await?;
            }
        } else {
            self.gets.fetch_add(1, Ordering::SeqCst);
            self.stall_point(location).await?;
            let delay = *self.get_delay.lock();
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
        }
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.check()?;
        self.gets.fetch_add(1, Ordering::SeqCst);
        self.stall_point(location).await?;
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let hook = self.delete_hook.lock().clone();
        let fail = self.fail_deletes.load(Ordering::SeqCst);
        let locations = locations
            .map(move |location| {
                let location = location?;
                if let Some(hook) = &hook {
                    hook(&location);
                }
                if fail {
                    return Err(object_store::Error::Generic {
                        store: "TestStore",
                        source: format!("delete of {location} refused").into(),
                    });
                }
                Ok(location)
            })
            .boxed();
        self.inner.delete_stream(locations)
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
        self.check()?;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.check()?;
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.check()?;
        self.inner.rename_opts(from, to, options).await
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// A 1 GiB cache in `dir`, with the default 1 MiB blocks, on a volume with
/// plenty of room (the volume a test runs on may be short of the free space
/// the cache keeps for other writers).
pub(crate) fn config(dir: &std::path::Path) -> NodeCacheConfig {
    NodeCacheConfig {
        dir: dir.to_path_buf(),
        budget_bytes: 1 << 30,
        volume: Arc::new(RoomyVolume),
    }
}

pub(crate) fn all_paths() -> Cacheable {
    Arc::new(|_| true)
}

/// Deterministic bytes (splitmix64 of `seed`).
pub(crate) fn data(seed: u64, len: u64) -> Bytes {
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03;
    let len = usize::try_from(len).expect("len");
    let mut out = Vec::with_capacity(len + 8);
    while out.len() < len {
        x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = x;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        out.extend_from_slice(&(z ^ (z >> 31)).to_le_bytes());
    }
    out.truncate(len);
    Bytes::from(out)
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next() % n }
    }
}

/// Panics with a short message (not the 1 MiB values) when the two differ.
pub(crate) fn assert_same(got: &Bytes, want: &Bytes, what: &str) {
    if got != want {
        let first_diff = got.iter().zip(want.iter()).position(|(a, b)| a != b);
        panic!(
            "{what}: got {} bytes, want {} bytes, first differing byte at {first_diff:?}",
            got.len(),
            want.len()
        );
    }
}

pub(crate) async fn put(store: &TestStore, path: &str, bytes: &Bytes) {
    store
        .inner
        .put(&Path::from(path), PutPayload::from_bytes(bytes.clone()))
        .await
        .expect("put");
}

/// The slab file holding `needle` and the offset of its first byte.
pub(crate) fn find_in_slabs(dir: &std::path::Path, needle: &[u8]) -> (PathBuf, u64) {
    let mut found = Vec::new();
    for entry in std::fs::read_dir(dir.join("slabs")).expect("list slabs") {
        let path = entry.expect("entry").path();
        if path.extension().is_none_or(|e| e != "dat") {
            continue;
        }
        let hay = std::fs::read(&path).expect("read slab");
        let mut at = 0;
        while let Some(pos) = hay[at..].windows(needle.len()).position(|w| w == needle) {
            found.push((path.clone(), (at + pos) as u64));
            at += pos + 1;
        }
    }
    assert_eq!(
        found.len(),
        1,
        "expected one copy of the block in the slabs, found {found:?}"
    );
    found.remove(0)
}

fn crc32c_parts(parts: &[&[u8]]) -> u32 {
    let mut digest = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32Iscsi);
    for part in parts {
        digest.update(part);
    }
    let [b0, b1, b2, b3, ..] = digest.finalize().to_le_bytes();
    u32::from_le_bytes([b0, b1, b2, b3])
}

/// Re-encodes the one-entry fixture as entry version 2 and slab-index
/// version 3, including their checksums.
fn write_old_format(dir: &std::path::Path) {
    const HEADER: usize = 4096;
    let mut dat = None;
    let mut idx = None;
    for entry in std::fs::read_dir(dir.join("slabs")).expect("list slabs") {
        let path = entry.expect("entry").path();
        match path.extension().and_then(|extension| extension.to_str()) {
            Some("dat") => dat = Some(path),
            Some("idx") => idx = Some(path),
            _ => {}
        }
    }
    let dat = dat.expect("slab data");
    let mut page = vec![0u8; HEADER];
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&dat)
        .expect("open slab data");
    file.read_exact_at(&mut page, 0).expect("read header");
    let meta_len = usize::from(u16::from_le_bytes([page[6], page[7]]));
    let key_len = usize::from(u16::from_le_bytes([page[8], page[9]]));
    let chunk_len = 1usize << page[10];
    let data_len = u32::from_le_bytes(page[12..16].try_into().expect("data length")) as usize;
    let table = 24 + key_len + meta_len;
    let table_end = table + data_len.div_ceil(chunk_len) * 4;
    page[4..6].copy_from_slice(&2u16.to_le_bytes());
    page[8..12].copy_from_slice(
        &u32::try_from(key_len)
            .expect("old key length")
            .to_le_bytes(),
    );
    page[table..table_end].fill(0);
    let crc = crc32c_parts(&[&page[0..20], &page[24..table]]);
    page[20..24].copy_from_slice(&crc.to_le_bytes());
    file.write_all_at(&page, 0).expect("write old header");

    let idx = idx.expect("slab index");
    let mut index = std::fs::read(&idx).expect("read index");
    let crc_at = index.len() - 4;
    index[4..6].copy_from_slice(&3u16.to_le_bytes());
    let crc = crc_fast::crc32_iscsi(&index[..crc_at]);
    index[crc_at..].copy_from_slice(&crc.to_le_bytes());
    std::fs::write(idx, index).expect("write old index");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum Op {
    Range(Range<u64>),
    Ranges(Vec<Range<u64>>),
    Opts(Option<GetRange>),
}

#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    /// Returned bytes (and for `get_opts`, the reported range and size).
    Bytes(Vec<Bytes>, Option<(Range<u64>, u64)>),
    Error(String),
}

impl Outcome {
    /// Bytes a cached read of this outcome serves: all of them when the read
    /// succeeded with a non-empty answer, none otherwise.
    fn served_bytes(&self) -> u64 {
        match self {
            Outcome::Bytes(parts, _) if parts.iter().all(|p| !p.is_empty()) => {
                parts.iter().map(|p| p.len() as u64).sum()
            }
            _ => 0,
        }
    }
}

async fn run_op(store: &dyn ObjectStore, path: &Path, op: &Op) -> Outcome {
    match op {
        Op::Range(r) => match store.get_range(path, r.clone()).await {
            Ok(b) => Outcome::Bytes(vec![b], None),
            Err(e) => Outcome::Error(e.to_string()),
        },
        Op::Ranges(rs) => match store.get_ranges(path, rs).await {
            Ok(v) => Outcome::Bytes(v, None),
            Err(e) => Outcome::Error(e.to_string()),
        },
        Op::Opts(range) => {
            let options = GetOptions {
                range: range.clone(),
                ..GetOptions::default()
            };
            match store.get_opts(path, options).await {
                Ok(r) => {
                    let meta = (r.range.clone(), r.meta.size);
                    match r.bytes().await {
                        Ok(b) => Outcome::Bytes(vec![b], Some(meta)),
                        Err(e) => Outcome::Error(e.to_string()),
                    }
                }
                Err(e) => Outcome::Error(e.to_string()),
            }
        }
    }
}

fn random_range(rng: &mut Rng, size: u64) -> Range<u64> {
    // Starts anywhere in 0..=size (start == size is an invalid range), lengths
    // up to 3 MiB, some running past the end of the object.
    let start = rng.below(size + 1);
    let len = 1 + rng.below(3 * MIB);
    start..start + len
}

fn random_op(rng: &mut Rng, size: u64) -> Op {
    match rng.below(6) {
        0 => Op::Range(random_range(rng, size)),
        1 => Op::Ranges(
            (0..=rng.below(4))
                .map(|_| random_range(rng, size))
                .collect(),
        ),
        2 => Op::Opts(Some(GetRange::Bounded(random_range(rng, size)))),
        3 => Op::Opts(Some(GetRange::Offset(rng.below(size + 1)))),
        4 => Op::Opts(Some(GetRange::Suffix(rng.below(size + 2)))),
        _ => Op::Opts(None),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ranges_match_inner_store() {
    let seed: u64 = std::env::var("OSC_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0x05EE_D0B1);
    eprintln!("ranges_match_inner_store seed={seed} (rerun with OSC_SEED={seed})");
    // Mix the seed so nearby seeds give unrelated sequences (xorshift needs a
    // non-zero state).
    let mut rng = Rng((seed ^ 0x9E37_79B9_7F4A_7C15).wrapping_mul(0xBF58_476D_1CE4_E5B9) | 1);

    // Two stores with the same paths and sizes but different bytes: a key
    // without the store URL would serve one store's bytes for the other.
    let stores = [
        Arc::new(TestStore::default()),
        Arc::new(TestStore::default()),
    ];
    let mut objects = Vec::new();
    for i in 0..10u64 {
        let size = match i {
            0 => 0,
            1 => 1,
            2 => 2 * MIB,
            3 => 9 * MIB,
            _ => rng.below(9 * MIB + 1),
        };
        let path = format!("tables/t/data/obj-{i}.vortex");
        for (s, store) in stores.iter().enumerate() {
            put(store, &path, &data(seed ^ (i << 8) ^ s as u64, size)).await;
        }
        objects.push((Path::from(path), size));
    }

    let tmp = tempfile::tempdir().expect("tempdir");
    let options = Tuning {
        write_queue: 4096,
        ..Tuning::default()
    };
    let cache = testing::open_with(config(tmp.path()), options)
        .await
        .expect("open");
    let cached: Vec<Arc<dyn ObjectStore>> = vec![
        cache.wrap(
            "s3://bucket-a/",
            Arc::clone(&stores[0]) as Arc<dyn ObjectStore>,
            all_paths(),
        ),
        cache.wrap(
            "s3://bucket-b/",
            Arc::clone(&stores[1]) as Arc<dyn ObjectStore>,
            all_paths(),
        ),
    ];

    let ops: Vec<(usize, usize, Op)> = (0..2000)
        .map(|_| {
            let s = usize::try_from(rng.below(2)).expect("index");
            let o = usize::try_from(rng.below(objects.len() as u64)).expect("index");
            let op = random_op(&mut rng, objects[o].1);
            (s, o, op)
        })
        .collect();

    let mut store_reads_per_pass = 0u64;
    let mut before = NodeCacheStats::default();
    for pass in 0..2 {
        let gets_before: u64 = stores.iter().map(|s| s.gets()).sum();
        for (n, (s, o, op)) in ops.iter().enumerate() {
            let path = &objects[*o].0;
            let want = run_op(&stores[*s].inner, path, op).await;
            let got = run_op(cached[*s].as_ref(), path, op).await;
            assert!(
                got == want,
                "seed {seed} pass {pass} op {n}: store {s} {path} (size {}) {op:?}: cached {} vs store {}",
                objects[*o].1,
                summarize(&got),
                summarize(&want)
            );
            if pass == 0 {
                // `get_ranges` with a range past the end reaches the store.
                let past_end =
                    matches!(op, Op::Ranges(rs) if rs.iter().any(|r| r.end > objects[*o].1));
                store_reads_per_pass += u64::from(want.served_bytes() == 0 || past_end);
            }
        }
        cache.wait_idle().await;
        let stats = cache.stats();
        eprintln!("pass {pass}: {stats:?}");
        assert_eq!(stats.dropped_fills, 0, "seed {seed}: fills were dropped");
        assert_eq!(stats.fallbacks, 0, "seed {seed}: cache errors");
        // One fetch and one write per block, also when the ranges of one
        // `get_ranges` call share blocks.
        assert_eq!(
            stats.fetched_bytes_query, stats.resident_bytes,
            "seed {seed}: a block was fetched twice"
        );
        assert_eq!(
            stats.fill_bytes, stats.resident_bytes,
            "seed {seed}: a block was written twice"
        );
        if pass == 1 {
            // Everything the first pass read is resident: the second pass
            // serves every non-empty answer from the cache and fetches nothing
            // to fill it; only reads that bypass reach the store.
            assert_eq!(
                stats.fetched_bytes_query, before.fetched_bytes_query,
                "seed {seed}"
            );
            assert!(stats.accesses > before.accesses, "seed {seed}");
            assert_eq!(
                stats.hits - before.hits,
                stats.accesses - before.accesses,
                "seed {seed}: every block read through the cache is a hit"
            );
            let gets_after: u64 = stores.iter().map(|s| s.gets()).sum();
            assert_eq!(
                gets_after - gets_before,
                store_reads_per_pass,
                "seed {seed}"
            );
        }
        before = stats;
    }
    assert!(before.hits > 0);
}

fn summarize(o: &Outcome) -> String {
    match o {
        Outcome::Bytes(parts, meta) => format!(
            "Ok({:?} bytes, range/size {meta:?})",
            parts.iter().map(Bytes::len).collect::<Vec<_>>()
        ),
        Outcome::Error(e) => format!("Err({e})"),
    }
}

#[tokio::test]
async fn head_and_conditional_reads_bypass() {
    let store = Arc::new(TestStore::default());
    let head_v1 = data(1, 4096);
    let head_v2 = data(2, 4096);
    put(&store, "t/head", &head_v1).await;
    let obj = data(3, 3 * MIB);
    put(&store, "t/data/a.vortex", &obj).await;

    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = NodeCache::open(config(tmp.path())).await.expect("open");
    // The table's head object is mutable: the predicate rejects it, as the
    // caller's allowlist of immutable kinds would.
    let cacheable: Cacheable = Arc::new(|p: &Path| p.filename() != Some("head"));
    let cached = cache.wrap(
        "s3://bucket/",
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        cacheable,
    );

    let head = Path::from("t/head");
    let got = cached
        .get(&head)
        .await
        .expect("get")
        .bytes()
        .await
        .expect("bytes");
    assert_same(&got, &head_v1, "head v1");
    // Same-size overwrite of the head: the next read sees the new bytes.
    put(&store, "t/head", &head_v2).await;
    let got = cached
        .get(&head)
        .await
        .expect("get")
        .bytes()
        .await
        .expect("bytes");
    assert_same(&got, &head_v2, "head v2");

    let a = Path::from("t/data/a.vortex");
    let got = cached.get_range(&a, 10..1000).await.expect("range");
    assert_same(&got, &obj.slice(10..1000), "cacheable range");
    let etag = store.inner.head(&a).await.expect("head").e_tag;
    let etag_before_rewrite = etag.clone();

    // Conditional reads go to the store and return its result.
    let not_modified = cached
        .get_opts(
            &a,
            GetOptions {
                if_none_match: etag.clone(),
                ..GetOptions::default()
            },
        )
        .await;
    assert!(
        matches!(not_modified, Err(object_store::Error::NotModified { .. })),
        "if_none_match: {not_modified:?}"
    );
    let precondition = cached
        .get_opts(
            &a,
            GetOptions {
                if_match: Some("\"not-the-etag\"".to_string()),
                ..GetOptions::default()
            },
        )
        .await;
    assert!(
        matches!(precondition, Err(object_store::Error::Precondition { .. })),
        "if_match: {precondition:?}"
    );
    let matching = cached
        .get_opts(
            &a,
            GetOptions {
                if_match: etag,
                range: Some(GetRange::Bounded(0..100)),
                ..GetOptions::default()
            },
        )
        .await
        .expect("if_match with the current etag")
        .bytes()
        .await
        .expect("bytes");
    assert_same(&matching, &obj.slice(0..100), "if_match current");

    // A HEAD request reports the store's current metadata, not the metadata
    // the wrapper sized its blocks by: rewrite the object with the same size
    // (new etag and modification time) and HEAD it through the wrapper. A
    // same size matters: a new size would also be caught by the size check
    // on the next fetch.
    put(&store, "t/data/a.vortex", &data(4, 3 * MIB)).await;
    let via_cache = cached.head(&a).await.expect("head via cache");
    let direct = store.inner.head(&a).await.expect("head direct");
    assert_ne!(
        direct.e_tag, etag_before_rewrite,
        "the rewrite changed the etag"
    );
    assert_eq!(via_cache, direct);

    let stats = cache.stats();
    // get(head) x2, if_none_match, if_match (wrong), if_match (current), HEAD.
    assert_eq!(stats.bypass_reads, 6, "{stats:?}");
    // Only the cacheable range was cached: one block.
    cache.wait_idle().await;
    assert_eq!(cache.stats().entries, 1);
}

#[tokio::test]
async fn same_url_in_another_namespace_is_a_miss() {
    // Two backends behind the same URL (one bucket name on two endpoints)
    // hold an object at the same path with the same size but other bytes.
    // Wrapped under namespaces that name the endpoint, a read through the
    // second never serves the first's blocks.
    let url = "s3://bucket/";
    let (a, b) = (
        Arc::new(TestStore::default()),
        Arc::new(TestStore::default()),
    );
    let p = Path::from("t/same.vortex");
    let (a_bytes, b_bytes) = (data(90, 2 * MIB), data(91, 2 * MIB));
    put(&a, p.as_ref(), &a_bytes).await;
    put(&b, p.as_ref(), &b_bytes).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = NodeCache::open(config(tmp.path())).await.expect("open");
    let through = |store: &Arc<TestStore>, endpoint: &str| {
        cache.wrap(
            &format!("{url} at {endpoint}"),
            Arc::clone(store) as Arc<dyn ObjectStore>,
            all_paths(),
        )
    };
    let (cached_a, cached_b) = (
        through(&a, "https://a.example"),
        through(&b, "https://b.example"),
    );
    let got = cached_a.get_range(&p, 0..2 * MIB).await.expect("read a");
    assert_same(&got, &a_bytes, "a");
    cache.wait_idle().await;
    assert_eq!(cache.stats().entries, 2);
    let got = cached_b.get_range(&p, 0..2 * MIB).await.expect("read b");
    assert_same(&got, &b_bytes, "b, after a's blocks were cached");
    assert_eq!(b.gets(), 1, "b's read was a miss");
    cache.wait_idle().await;
    assert_eq!(
        cache.stats().entries,
        4,
        "each namespace keeps its own blocks"
    );
    // Each keeps serving its own bytes from the cache.
    let (a_gets, b_gets) = (a.gets(), b.gets());
    assert_same(
        &cached_a.get_range(&p, 0..2 * MIB).await.expect("read a"),
        &a_bytes,
        "a again",
    );
    assert_same(
        &cached_b.get_range(&p, 0..2 * MIB).await.expect("read b"),
        &b_bytes,
        "b again",
    );
    assert_eq!((a.gets() - a_gets, b.gets() - b_gets), (0, 0));
}

#[tokio::test]
async fn rejected_path_is_not_cached() {
    let store = Arc::new(TestStore::default());
    let wal = data(5, 2 * MIB);
    put(&store, "x/_wal.json", &wal).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = NodeCache::open(config(tmp.path())).await.expect("open");
    let cacheable: Cacheable = Arc::new(|p: &Path| p.filename() != Some("_wal.json"));
    let cached = cache.wrap(
        "s3://bucket/",
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        cacheable,
    );
    let p = Path::from("x/_wal.json");

    let got = cached
        .get(&p)
        .await
        .expect("get")
        .bytes()
        .await
        .expect("bytes");
    assert_same(&got, &wal, "first read");
    let got = cached.get_range(&p, 100..MIB + 100).await.expect("range");
    assert_same(
        &got,
        &wal.slice(100..usize::try_from(MIB).expect("mib") + 100),
        "second read",
    );
    cache.wait_idle().await;

    let stats = cache.stats();
    assert_eq!(stats.entries, 0, "{stats:?}");
    assert_eq!(stats.fill_bytes, 0, "{stats:?}");
    assert_eq!(stats.bypass_reads, 2, "{stats:?}");
    assert_eq!(store.gets(), 2);
    assert_eq!(store.heads.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn unwritable_dir_serves_from_store() {
    let store = Arc::new(TestStore::default());
    let obj = data(6, 3 * MIB);
    put(&store, "d/a.vortex", &obj).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = NodeCache::open(config(tmp.path())).await.expect("open");
    let cached = cache.wrap(
        "s3://bucket/",
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        all_paths(),
    );
    let slabs = tmp.path().join("slabs");
    std::fs::set_permissions(&slabs, std::fs::Permissions::from_mode(0o555)).expect("chmod");

    let p = Path::from("d/a.vortex");
    for round in 1..=2u64 {
        let got = cached
            .get(&p)
            .await
            .expect("get")
            .bytes()
            .await
            .expect("bytes");
        assert_same(&got, &obj, "read with an unwritable cache directory");
        cache.wait_idle().await;
        let stats = cache.stats();
        // Every block's entry write failed and was counted; nothing cached.
        assert_eq!(stats.fallbacks, 3 * round, "{stats:?}");
        assert_eq!(stats.entries, 0, "{stats:?}");
        assert_eq!(store.gets(), round);
    }

    // Opening a cache whose directory cannot be created is an error, not a
    // panic.
    let blocked = tmp.path().join("slabs").join("nested");
    let err = NodeCache::open(config(&blocked))
        .await
        .expect_err("open in unwritable dir");
    assert!(
        matches!(
            err,
            object_store_cache::Error::Io {
                action: "create",
                ..
            }
        ),
        "{err:?}"
    );
    std::fs::set_permissions(&slabs, std::fs::Permissions::from_mode(0o755)).expect("chmod back");
}

#[tokio::test]
async fn deleted_dir_serves_from_store() {
    let store = Arc::new(TestStore::default());
    let obj = data(7, 3 * MIB);
    put(&store, "d/a.vortex", &obj).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join("cache");
    let cache = NodeCache::open(config(&dir)).await.expect("open");
    let cached = cache.wrap(
        "s3://bucket/",
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        all_paths(),
    );
    std::fs::remove_dir_all(&dir).expect("delete cache dir");

    let p = Path::from("d/a.vortex");
    for round in 1..=2u64 {
        let got = cached
            .get(&p)
            .await
            .expect("get")
            .bytes()
            .await
            .expect("bytes");
        assert_same(&got, &obj, "read with a deleted cache directory");
        cache.wait_idle().await;
        let stats = cache.stats();
        assert_eq!(stats.fallbacks, 3 * round, "{stats:?}");
        assert_eq!(stats.entries, 0, "{stats:?}");
        assert_eq!(store.gets(), round);
    }

    // A directory deleted under a warm cache: cached and new ranges still read
    // the right bytes. An 8 MiB budget makes 2 MiB slabs, one 1 MiB block
    // each, so every new block needs a new slab file in the deleted directory.
    let dir2 = tmp.path().join("warm_cache");
    let warm_config = NodeCacheConfig {
        budget_bytes: 8 * MIB,
        ..config(&dir2)
    };
    // Reads of 2 and 3 MiB are scans under an 8 MiB budget; the scan rule is
    // off so they fill the cache (`scan_read_bypasses_admission` tests it).
    let warm_cache = testing::open_with(
        warm_config,
        Tuning {
            scan_bytes: Some(u64::MAX),
            ..Tuning::default()
        },
    )
    .await
    .expect("open");
    let warm_store = warm_cache.wrap(
        "s3://bucket/",
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        all_paths(),
    );
    let first = warm_store.get_range(&p, 0..2 * MIB).await.expect("fill");
    assert_same(&first, &obj.slice(0..2 << 20), "fill");
    warm_cache.wait_idle().await;
    let filled = warm_cache.stats();
    assert_eq!(filled.entries, 2, "{filled:?}");
    assert_eq!(filled.fallbacks, 0, "{filled:?}");
    std::fs::remove_dir_all(&dir2).expect("delete warm cache dir");

    for round in 1..=2u64 {
        let gets_before = store.gets();
        let hits_before = warm_cache.stats().hits;
        let all = warm_store
            .get(&p)
            .await
            .expect("get")
            .bytes()
            .await
            .expect("bytes");
        assert_same(&all, &obj, "warm cache after deletion");
        warm_cache.wait_idle().await;
        let stats = warm_cache.stats();
        // Blocks 0 and 1 are served from the open slab files; block 2 is
        // fetched once and its write, which needs a new slab file, fails.
        assert_eq!(stats.hits - hits_before, 2, "{stats:?}");
        assert_eq!(store.gets() - gets_before, 1, "round {round}");
        assert_eq!(stats.fallbacks, round, "{stats:?}");
        assert_eq!(stats.entries, 2, "{stats:?}");
        assert_eq!(stats.fill_bytes, 2 * MIB, "{stats:?}");
    }
}

#[tokio::test]
async fn failing_block_store_serves_from_store() {
    let store = Arc::new(TestStore::default());
    let obj = data(8, 3 * MIB);
    put(&store, "d/a.vortex", &obj).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let slab = SlabStore::open(tmp.path(), 64 * MIB).await.expect("slab");
    let failing = Arc::new(FaultStore::new(slab));
    let cache = testing::with_block_store(
        config(tmp.path()),
        Tuning::default(),
        Arc::clone(&failing) as Arc<dyn BlockStore>,
    )
    .expect("cache");
    let cached = cache.wrap(
        "s3://bucket/",
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        all_paths(),
    );
    let p = Path::from("d/a.vortex");
    let got = cached
        .get(&p)
        .await
        .expect("get")
        .bytes()
        .await
        .expect("bytes");
    assert_same(&got, &obj, "fill");
    cache.wait_idle().await;
    assert_eq!(cache.stats().entries, 3);
    assert_eq!(store.gets(), 1);

    failing.fail_gets.store(true, Ordering::SeqCst);
    let got = cached
        .get(&p)
        .await
        .expect("get")
        .bytes()
        .await
        .expect("bytes");
    assert_same(&got, &obj, "read with failing block-store reads");
    let stats = cache.stats();
    assert_eq!(stats.fallbacks, 3, "{stats:?}");
    assert_eq!(store.gets(), 2);
}

#[tokio::test]
async fn unreachable_store_serves_cached_ranges_only() {
    let store = Arc::new(TestStore::default());
    let obj = data(9, 4 * MIB);
    put(&store, "d/a.vortex", &obj).await;
    put(&store, "d/b.vortex", &obj).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = NodeCache::open(config(tmp.path())).await.expect("open");
    let cached = cache.wrap(
        "s3://bucket/",
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        all_paths(),
    );
    let a = Path::from("d/a.vortex");
    let warm = cached.get_range(&a, 0..2 * MIB).await.expect("fill");
    assert_same(&warm, &obj.slice(0..2 << 20), "fill");
    cache.wait_idle().await;

    store.fail.store(true, Ordering::SeqCst);
    let hit = cached
        .get_range(&a, 100..2 * MIB - 5)
        .await
        .expect("cached range");
    assert_same(
        &hit,
        &obj.slice(100..(2 << 20) - 5),
        "cached range with the store down",
    );
    let ranges = cached
        .get_ranges(&a, &[0..10, MIB..MIB + 10])
        .await
        .expect("cached ranges");
    assert_same(&ranges[0], &obj.slice(0..10), "cached ranges[0]");
    assert_same(
        &ranges[1],
        &obj.slice(1 << 20..(1 << 20) + 10),
        "cached ranges[1]",
    );

    let unreachable = |r: &object_store::Result<Bytes>| {
        matches!(
            r,
            Err(object_store::Error::Generic {
                store: "TestStore",
                ..
            })
        )
    };
    let miss = cached.get_range(&a, 3 * MIB..3 * MIB + 10).await;
    assert!(unreachable(&miss), "uncached range: {miss:?}");
    let straddle = cached.get_range(&a, 2 * MIB - 10..2 * MIB + 10).await;
    assert!(unreachable(&straddle), "half-cached range: {straddle:?}");
    let mixed = cached.get_ranges(&a, &[0..10, 3 * MIB..3 * MIB + 10]).await;
    assert!(
        matches!(
            mixed,
            Err(object_store::Error::Generic {
                store: "TestStore",
                ..
            })
        ),
        "{mixed:?}"
    );
    // An object never read in this process: its size is unknown, so the HEAD
    // fails with the store's error.
    let never = cached.get_range(&Path::from("d/b.vortex"), 0..10).await;
    assert!(unreachable(&never), "never-read object: {never:?}");
}

#[tokio::test]
async fn corrupt_block_same_length_is_refetched() {
    let store = Arc::new(TestStore::default());
    let obj = data(10, 3 * MIB);
    put(&store, "d/a.vortex", &obj).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = NodeCache::open(config(tmp.path())).await.expect("open");
    let cached = cache.wrap(
        "s3://bucket/",
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        all_paths(),
    );
    let p = Path::from("d/a.vortex");
    let got = cached
        .get(&p)
        .await
        .expect("get")
        .bytes()
        .await
        .expect("bytes");
    assert_same(&got, &obj, "fill");
    cache.wait_idle().await;
    assert_eq!(cache.stats().entries, 3);
    assert_eq!(store.gets(), 1);

    // Flip one byte inside block 1's data, in place: same length.
    let block1 = &obj[1 << 20..(1 << 20) + 64];
    let (slab, at) = find_in_slabs(tmp.path(), block1);
    let f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&slab)
        .expect("open slab");
    let mut b = [0u8; 1];
    f.read_exact_at(&mut b, at + 777).expect("read byte");
    b[0] ^= 0x5A;
    f.write_all_at(&b, at + 777).expect("write byte");
    drop(f);

    let got = cached
        .get(&p)
        .await
        .expect("get")
        .bytes()
        .await
        .expect("bytes");
    assert_same(&got, &obj, "read after corruption");
    let stats = cache.stats();
    assert_eq!(stats.fallbacks, 1, "{stats:?}");
    assert_eq!(store.gets(), 2, "the corrupt block is fetched again");

    // The block was rewritten: the next read is all hits.
    cache.wait_idle().await;
    assert_eq!(cache.stats().entries, 3);
    let hits_before = cache.stats().hits;
    let got = cached
        .get(&p)
        .await
        .expect("get")
        .bytes()
        .await
        .expect("bytes");
    assert_same(&got, &obj, "read after refetch");
    assert_eq!(store.gets(), 2);
    assert_eq!(cache.stats().hits - hits_before, 3);
}

#[tokio::test]
async fn corrupt_requested_chunk_is_dropped_and_refetched() {
    const CHUNK: u64 = 64 << 10;
    let store = Arc::new(TestStore::default());
    let obj = data(0x00C0_FFEE, MIB);
    put(&store, "d/chunk.vortex", &obj).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = NodeCache::open(config(tmp.path())).await.expect("open");
    let cached = cache.wrap(
        "s3://bucket/",
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        all_paths(),
    );
    let path = Path::from("d/chunk.vortex");
    let filled = cached.get_range(&path, 0..MIB).await.expect("fill");
    assert_same(&filled, &obj, "fill bytes");
    cache.wait_idle().await;
    assert_eq!(store.gets(), 1);

    let (slab, data_at) = find_in_slabs(tmp.path(), &obj[..64]);
    let corrupt_at = 5 * CHUNK + 123;
    let slab_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(slab)
        .expect("open slab");
    let mut byte = [0u8; 1];
    slab_file
        .read_exact_at(&mut byte, data_at + corrupt_at)
        .expect("read byte");
    byte[0] ^= 0x80;
    slab_file
        .write_all_at(&byte, data_at + corrupt_at)
        .expect("write byte");

    let range = 5 * CHUNK + 100..5 * CHUNK + 16_484;
    let got = cached.get_range(&path, range.clone()).await.expect("read");
    let expected = usize::try_from(range.start).expect("range start")
        ..usize::try_from(range.end).expect("range end");
    assert_same(&got, &obj.slice(expected), "read after chunk corruption");
    let stats = cache.stats();
    assert_eq!(stats.fallbacks, 1, "{stats:?}");
    assert_eq!(stats.evicted_bytes_corrupt, MIB, "{stats:?}");
    assert_eq!(store.gets(), 2, "corrupt chunk did not fall back");
}

#[tokio::test]
async fn old_format_directory_is_quarantined_and_refilled() {
    let store = Arc::new(TestStore::default());
    let obj = data(0x0D1D_F04D, MIB);
    put(&store, "d/old.vortex", &obj).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let key = BlockKey {
        store: "s3://bucket/".to_string(),
        path: "d/old.vortex".to_string(),
        object_size: MIB,
        block_size: MIB,
        index: 0,
    };
    let slabs = SlabStore::open(tmp.path(), 8 * MIB)
        .await
        .expect("open slabs");
    slabs
        .put(&key, obj.clone(), &StoredMeta::NONE)
        .await
        .expect("put old entry");
    slabs.close().await.expect("close slabs");
    drop(slabs);
    write_old_format(tmp.path());

    let cache = NodeCache::open(config(tmp.path()))
        .await
        .expect("open cache");
    let stats = cache.stats();
    assert_eq!(stats.quarantined_slabs, 1, "{stats:?}");
    assert_eq!(stats.entries, 0, "old entry was listed: {stats:?}");
    let cached = cache.wrap(
        "s3://bucket/",
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        all_paths(),
    );
    let path = Path::from("d/old.vortex");
    let range = 77..77 + 32_000;
    let first = cached
        .get_range(&path, range.clone())
        .await
        .expect("refill");
    let expected = usize::try_from(range.start).expect("range start")
        ..usize::try_from(range.end).expect("range end");
    assert_same(&first, &obj.slice(expected.clone()), "refilled bytes");
    assert_eq!(store.gets(), 1, "old entry was served");
    cache.wait_idle().await;
    let hits = cache.stats().hits;
    let second = cached.get_range(&path, range.clone()).await.expect("hit");
    assert_same(&second, &obj.slice(expected), "bytes after refill");
    assert_eq!(store.gets(), 1, "refilled entry missed");
    assert_eq!(cache.stats().hits - hits, 1, "refilled entry was not hit");
}

#[tokio::test]
async fn seeded_subranges_match_source_across_chunk_and_block_boundaries() {
    const CHUNK: u64 = 64 << 10;
    const SEED: u64 = 0x51A8_7EED_1A2B_3C4D;
    eprintln!("random sub-range seed: {SEED:#018x}");
    let store = Arc::new(TestStore::default());
    let obj = data(SEED, 5 * MIB + 12_345);
    put(&store, "d/ranges.vortex", &obj).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = NodeCache::open(config(tmp.path())).await.expect("open");
    let cached = cache.wrap(
        "s3://bucket/",
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        all_paths(),
    );
    let path = Path::from("d/ranges.vortex");
    let fill = cached
        .get(&path)
        .await
        .expect("fill")
        .bytes()
        .await
        .expect("fill bytes");
    assert_same(&fill, &obj, "fill");
    cache.wait_idle().await;
    let before = cache.stats();
    assert_eq!(store.gets(), 1);

    let mut rng = Rng(SEED);
    for case in 0..256 {
        let boundary = if case % 2 == 0 {
            let block = rng.below(5);
            let chunk = 1 + rng.below(15);
            block * MIB + chunk * CHUNK
        } else {
            (1 + rng.below(4)) * MIB
        };
        let start = boundary - 1 - rng.below(CHUNK - 1);
        let end = (boundary + 1 + rng.below(CHUNK - 1)).min(obj.len() as u64);
        let got = cached
            .get_range(&path, start..end)
            .await
            .unwrap_or_else(|error| panic!("seed {SEED:#018x} case {case}: {error}"));
        let expected =
            usize::try_from(start).expect("range start")..usize::try_from(end).expect("range end");
        assert_same(
            &got,
            &obj.slice(expected),
            &format!("seed {SEED:#018x} case {case} range {start}..{end}"),
        );
    }
    let after = cache.stats();
    assert_eq!(store.gets(), 1, "a random sub-range missed");
    assert_eq!(after.fallbacks - before.fallbacks, 0, "{after:?}");
    assert_eq!(
        after.hits - before.hits,
        after.accesses - before.accesses,
        "not every block access was a cache hit: {after:?}"
    );
    assert!(
        after.accesses - before.accesses > 256,
        "block boundaries were not crossed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_entry_write_does_not_delay_read() {
    let store = Arc::new(TestStore::default());
    *store.get_delay.lock() = Duration::from_millis(50);
    let obj = data(11, 4 * MIB);
    put(&store, "d/a.vortex", &obj).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let slab = SlabStore::open(tmp.path(), 64 * MIB).await.expect("slab");
    let slow = Arc::new(FaultStore {
        put_delay: Duration::from_secs(2),
        ..FaultStore::new(slab)
    });
    let cache =
        testing::with_block_store(config(tmp.path()), Tuning::default(), slow).expect("cache");
    let cached = cache.wrap(
        "s3://bucket/",
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        all_paths(),
    );
    let p = Path::from("d/a.vortex");

    let t0 = Instant::now();
    let got = cached
        .get(&p)
        .await
        .expect("get")
        .bytes()
        .await
        .expect("bytes");
    let elapsed = t0.elapsed();
    assert_same(&got, &obj, "cold read");
    // One HEAD and one 4 MiB GET at 50 ms each; the 2 s entry writes are not
    // waited for.
    assert!(
        elapsed < Duration::from_millis(1000),
        "the read took {elapsed:?}; entry writes take 2 s each"
    );
    assert_eq!(cache.stats().entries, 0, "no entry should be written yet");

    cache.wait_idle().await;
    let stats = cache.stats();
    assert_eq!(stats.entries, 4, "{stats:?}");
    assert_eq!(stats.fill_bytes, 4 * MIB, "{stats:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stalled_shared_fetch_wait_is_bounded() {
    let store = Arc::new(TestStore::default());
    let obj = data(12, 2 * MIB);
    put(&store, "d/a.vortex", &obj).await;
    let gate = Arc::new(Notify::new());
    let p = Path::from("d/a.vortex");
    *store.stall.lock() = Some((p.clone(), Arc::clone(&gate)));

    let tmp = tempfile::tempdir().expect("tempdir");
    let options = Tuning {
        shared_fetch_wait: Duration::from_millis(200),
        ..Tuning::default()
    };
    let cache = testing::open_with(config(tmp.path()), options)
        .await
        .expect("open");
    let cached = cache.wrap(
        "s3://bucket/",
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        all_paths(),
    );

    // Reader 1 leads the fetch of block 0, which stalls in the store.
    let started = store.get_started.notified();
    let leader = {
        let cached = Arc::clone(&cached);
        let p = p.clone();
        tokio::spawn(async move { cached.get_range(&p, 0..1000).await })
    };
    tokio::time::timeout(Duration::from_secs(5), started)
        .await
        .expect("the leader's GET reached the store");

    // Reader 2 wants the same block: it waits 200 ms, then reads the store.
    let t0 = Instant::now();
    let follower =
        tokio::time::timeout(Duration::from_secs(5), cached.get_range(&p, 500..1500)).await;
    let elapsed = t0.elapsed();
    let follower = follower
        .expect("the second reader returned within 5 s while the shared fetch stalled")
        .expect("follower bytes");
    assert_same(&follower, &obj.slice(500..1500), "follower");
    assert!(
        elapsed >= Duration::from_millis(200) && elapsed < Duration::from_secs(2),
        "follower took {elapsed:?}"
    );
    let stats = cache.stats();
    assert_eq!(stats.shared_wait_timeouts, 1, "{stats:?}");
    assert_eq!(store.gets(), 2);

    gate.notify_one();
    let led = leader.await.expect("join").expect("leader bytes");
    assert_same(&led, &obj.slice(0..1000), "leader");
    cache.wait_idle().await;
    assert_eq!(cache.stats().entries, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stalled_shared_fetch_wait_is_bounded_across_blocks() {
    let store = Arc::new(TestStore::default());
    let obj = data(14, 4 * MIB);
    put(&store, "d/a.vortex", &obj).await;
    let gate = Arc::new(Notify::new());
    let p = Path::from("d/a.vortex");
    *store.stall.lock() = Some((p.clone(), Arc::clone(&gate)));
    let tmp = tempfile::tempdir().expect("tempdir");
    let options = Tuning {
        shared_fetch_wait: Duration::from_millis(300),
        ..Tuning::default()
    };
    let cache = testing::open_with(config(tmp.path()), options)
        .await
        .expect("open");
    let cached = cache.wrap(
        "s3://bucket/",
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        all_paths(),
    );

    // Reader 1 leads blocks 0 and 1 in one GET, which stalls.
    let started = store.get_started.notified();
    let leader = {
        let cached = Arc::clone(&cached);
        let p = p.clone();
        tokio::spawn(async move { cached.get_range(&p, 0..2 * MIB).await })
    };
    tokio::time::timeout(Duration::from_secs(5), started)
        .await
        .expect("the leader's GET reached the store");

    // Reader 2 waits on blocks 0 and 1 under one deadline (300 ms in all, not
    // 300 ms per block) and leads block 2.
    let t0 = Instant::now();
    let follower = tokio::time::timeout(
        Duration::from_secs(5),
        cached.get_range(&p, MIB / 2..3 * MIB),
    )
    .await;
    let elapsed = t0.elapsed();
    let follower = follower
        .expect("the second reader returned within 5 s")
        .expect("follower bytes");
    assert_same(
        &follower,
        &obj.slice((1 << 20) / 2..3 << 20),
        "follower across blocks",
    );
    assert!(
        elapsed >= Duration::from_millis(300) && elapsed < Duration::from_millis(550),
        "follower took {elapsed:?}: one shared deadline is 300 ms, a wait per block 600 ms"
    );
    let stats = cache.stats();
    assert_eq!(stats.shared_wait_timeouts, 2, "{stats:?}");
    // The stalled GET, the follower's fill of block 2, and one direct GET of
    // blocks 0 and 1 together.
    assert_eq!(store.gets(), 3);

    gate.notify_one();
    let led = leader.await.expect("join").expect("leader bytes");
    assert_same(&led, &obj.slice(0..2 << 20), "leader");
    cache.wait_idle().await;
    assert_eq!(cache.stats().entries, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_shared_fetch_wakes_waiters() {
    let store = Arc::new(TestStore::default());
    let obj = data(15, 2 * MIB);
    put(&store, "d/a.vortex", &obj).await;
    let gate = Arc::new(Notify::new());
    let p = Path::from("d/a.vortex");
    *store.stall.lock() = Some((p.clone(), Arc::clone(&gate)));
    store.fail_stalled.store(true, Ordering::SeqCst);
    let tmp = tempfile::tempdir().expect("tempdir");
    // A wait long enough that only the leader's failure can end it in time.
    let options = Tuning {
        shared_fetch_wait: Duration::from_secs(30),
        ..Tuning::default()
    };
    let cache = testing::open_with(config(tmp.path()), options)
        .await
        .expect("open");
    let cached = cache.wrap(
        "s3://bucket/",
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        all_paths(),
    );

    let started = store.get_started.notified();
    let leader = {
        let cached = Arc::clone(&cached);
        let p = p.clone();
        tokio::spawn(async move { cached.get_range(&p, 0..1000).await })
    };
    tokio::time::timeout(Duration::from_secs(5), started)
        .await
        .expect("the leader's GET reached the store");

    // One poll claims the block and starts waiting on the leader's fetch.
    let mut follower = Box::pin(cached.get_range(&p, 500..1500));
    assert!(
        futures::poll!(follower.as_mut()).is_pending(),
        "the follower waits on the leader"
    );
    let t0 = Instant::now();
    gate.notify_one();
    let got = tokio::time::timeout(Duration::from_secs(5), follower)
        .await
        .expect("the follower was woken by the failed fetch")
        .expect("follower bytes");
    assert_same(&got, &obj.slice(500..1500), "follower");
    assert!(t0.elapsed() < Duration::from_secs(5));
    let led = leader.await.expect("join");
    assert!(
        matches!(
            led,
            Err(object_store::Error::Generic {
                store: "TestStore",
                ..
            })
        ),
        "leader: {led:?}"
    );
    let stats = cache.stats();
    assert_eq!(stats.shared_wait_timeouts, 0, "{stats:?}");
    assert_eq!(store.gets(), 2);
    // The follower read the store directly: nothing was written.
    cache.wait_idle().await;
    assert_eq!(cache.stats().entries, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn abandoned_stalled_fill_is_released() {
    let store = Arc::new(TestStore::default());
    let obj = data(16, 2 * MIB);
    put(&store, "d/a.vortex", &obj).await;
    // The first GET stalls forever: the gate is never opened.
    let gate = Arc::new(Notify::new());
    let p = Path::from("d/a.vortex");
    *store.stall.lock() = Some((p.clone(), Arc::clone(&gate)));
    let tmp = tempfile::tempdir().expect("tempdir");
    let wait = Duration::from_millis(400);
    let options = Tuning {
        shared_fetch_wait: wait,
        ..Tuning::default()
    };
    let cache = testing::open_with(config(tmp.path()), options)
        .await
        .expect("open");
    let cached = cache.wrap(
        "s3://bucket/",
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        all_paths(),
    );

    let started = store.get_started.notified();
    let leader = {
        let cached = Arc::clone(&cached);
        let p = p.clone();
        tokio::spawn(async move { cached.get_range(&p, 0..1000).await })
    };
    tokio::time::timeout(Duration::from_secs(5), started)
        .await
        .expect("the leader's GET reached the store");
    // The query that led the fetch is cancelled.
    leader.abort();
    assert!(leader.await.expect_err("aborted").is_cancelled());
    let abandoned = Instant::now();

    // Time is under test: an abandoned fetch keeps its blocks for exactly one
    // shared wait (400 ms) after its reader is gone. A reader arriving half
    // a wait later waits on it until then (about 200 ms), which is before
    // its own wait would time out (600 ms after the abandonment).
    tokio::time::sleep(wait / 2).await;
    let t0 = Instant::now();
    let got = tokio::time::timeout(Duration::from_secs(5), cached.get_range(&p, 500..1500))
        .await
        .expect("the next reader returned")
        .expect("bytes");
    let waited = t0.elapsed();
    assert_same(&got, &obj.slice(500..1500), "next reader");
    let stats = cache.stats();
    assert_eq!(
        stats.shared_wait_timeouts,
        0,
        "the abandoned fetch held its block past the next reader's wait (released after {:?}): {stats:?}",
        abandoned.elapsed()
    );
    assert!(
        waited >= wait / 4,
        "the abandoned fetch had let its block go within {:?} of being abandoned, before one shared wait",
        abandoned.elapsed().saturating_sub(waited)
    );
    // The stalled GET, then the next reader's own GET once the block was let
    // go; that read went to the store directly, so nothing was written.
    assert_eq!(store.gets(), 2);
    cache.wait_idle().await;
    assert_eq!(cache.stats().entries, 0);
}

#[tokio::test]
async fn failed_head_reads_from_store() {
    let store = Arc::new(TestStore::default());
    let obj = data(17, 3 * MIB);
    put(&store, "d/a.vortex", &obj).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = NodeCache::open(config(tmp.path())).await.expect("open");
    let cached = cache.wrap(
        "s3://bucket/",
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        all_paths(),
    );
    let p = Path::from("d/a.vortex");
    store.fail_heads.store(true, Ordering::SeqCst);

    let got = cached
        .get(&p)
        .await
        .expect("get with HEAD refused")
        .bytes()
        .await
        .expect("bytes");
    assert_same(&got, &obj, "get");
    let got = cached
        .get_range(&p, 10..MIB + 10)
        .await
        .expect("get_range with HEAD refused");
    assert_same(&got, &obj.slice(10..(1 << 20) + 10), "get_range");
    let got = cached
        .get_ranges(&p, &[0..10, 2 * MIB..2 * MIB + 10])
        .await
        .expect("get_ranges with HEAD refused");
    assert_same(&got[0], &obj.slice(0..10), "get_ranges[0]");
    assert_same(
        &got[1],
        &obj.slice(2 << 20..(2 << 20) + 10),
        "get_ranges[1]",
    );
    cache.wait_idle().await;
    let stats = cache.stats();
    assert_eq!(stats.bypass_reads, 3, "{stats:?}");
    assert_eq!(stats.entries, 0, "{stats:?}");
    assert_eq!(store.heads.load(Ordering::SeqCst), 3);

    // With HEAD answered again, the object is cached.
    store.fail_heads.store(false, Ordering::SeqCst);
    let got = cached
        .get(&p)
        .await
        .expect("get")
        .bytes()
        .await
        .expect("bytes");
    assert_same(&got, &obj, "get after HEAD recovers");
    cache.wait_idle().await;
    assert_eq!(cache.stats().entries, 3);
}

#[tokio::test]
async fn empty_get_ranges_reaches_the_store() {
    let store = Arc::new(TestStore::default());
    let obj = data(18, MIB);
    put(&store, "d/a.vortex", &obj).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = NodeCache::open(config(tmp.path())).await.expect("open");
    let cached = cache.wrap(
        "s3://bucket/",
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        all_paths(),
    );
    let outcome = |r: object_store::Result<Vec<Bytes>>| match r {
        Ok(v) => Ok(v.len()),
        Err(e) => Err(e.to_string()),
    };
    for (path, down) in [
        ("d/a.vortex", false),
        ("d/missing.vortex", false),
        ("d/a.vortex", true),
    ] {
        let path = Path::from(path);
        store.fail.store(down, Ordering::SeqCst);
        let want = outcome(store.get_ranges(&path, &[]).await);
        let gets_before = store.gets();
        let got = outcome(cached.get_ranges(&path, &[]).await);
        assert_eq!(got, want, "{path} with the store down: {down}");
        if !down {
            assert_eq!(
                store.gets() - gets_before,
                1,
                "{path}: the call reached the store"
            );
        }
    }
}

/// A store with one object of `u64::MAX` bytes whose byte at offset `i` is
/// `i % 251`. Reads of more than 64 MiB fail.
#[derive(Debug)]
struct HugeStore {
    meta: ObjectMeta,
}

impl fmt::Display for HugeStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HugeStore")
    }
}

fn huge_bytes(range: Range<u64>) -> Bytes {
    range
        .map(|i| u8::try_from(i % 251).expect("byte"))
        .collect::<Vec<u8>>()
        .into()
}

fn not_supported() -> object_store::Error {
    object_store::Error::Generic {
        store: "HugeStore",
        source: "not supported".into(),
    }
}

#[async_trait]
impl ObjectStore for HugeStore {
    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        Err(not_supported())
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(not_supported())
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if *location != self.meta.location {
            return Err(object_store::Error::NotFound {
                path: location.to_string(),
                source: "no such object".into(),
            });
        }
        let size = self.meta.size;
        let range = match (&options.range, options.head) {
            (_, true) => 0..0,
            (None, false) => 0..size,
            (Some(r), false) => r.as_range(size).map_err(|e| object_store::Error::Generic {
                store: "HugeStore",
                source: e.into(),
            })?,
        };
        if range.end - range.start > 64 * MIB {
            return Err(object_store::Error::Generic {
                store: "HugeStore",
                source: format!("read of {range:?} is too large").into(),
            });
        }
        let bytes = huge_bytes(range.clone());
        Ok(GetResult {
            payload: object_store::GetResultPayload::Stream(
                futures::stream::once(async move { Ok(bytes) }).boxed(),
            ),
            meta: self.meta.clone(),
            range,
            attributes: object_store::Attributes::default(),
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        locations.map(|_| Err(not_supported())).boxed()
    }

    fn list(&self, _prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        futures::stream::empty().boxed()
    }

    async fn list_with_delimiter(
        &self,
        _prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        Err(not_supported())
    }

    async fn copy_opts(
        &self,
        _from: &Path,
        _to: &Path,
        _options: CopyOptions,
    ) -> object_store::Result<()> {
        Err(not_supported())
    }
}

#[tokio::test]
async fn huge_object_ranges_do_not_overflow() {
    // An `ObjectMeta` from a real store, resized to u64::MAX.
    let mem = InMemory::new();
    let p = Path::from("d/huge.vortex");
    mem.put(&p, PutPayload::from_static(b"x"))
        .await
        .expect("put");
    let mut meta = mem.head(&p).await.expect("head");
    meta.size = u64::MAX;
    let huge = Arc::new(HugeStore { meta });

    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = NodeCache::open(config(tmp.path())).await.expect("open");
    let cached = cache.wrap(
        "s3://bucket/",
        Arc::clone(&huge) as Arc<dyn ObjectStore>,
        all_paths(),
    );
    let max = u64::MAX;
    let ops = [
        Op::Range(max - 10..max),
        Op::Range(0..10),
        Op::Ranges(vec![max - 5000..max - 4000, max - 2..max]),
        Op::Opts(Some(GetRange::Suffix(3 * MIB + 5))),
        Op::Opts(Some(GetRange::Offset(max - 100))),
    ];
    for pass in 0..2 {
        for op in &ops {
            let want = run_op(huge.as_ref(), &p, op).await;
            let got = run_op(cached.as_ref(), &p, op).await;
            assert!(
                got == want,
                "pass {pass} {op:?}: cached {} vs store {}",
                summarize(&got),
                summarize(&want)
            );
            assert!(
                matches!(got, Outcome::Bytes(..)),
                "{op:?}: {}",
                summarize(&got)
            );
        }
        cache.wait_idle().await;
    }
    let stats = cache.stats();
    assert_eq!(stats.fallbacks, 0, "{stats:?}");
    // The last four blocks and the first one are resident.
    assert_eq!(stats.entries, 5, "{stats:?}");
}

#[tokio::test]
async fn failed_reclaim_keeps_disk_within_budget() {
    // 256 KiB blocks under a 4 MiB budget: 1 MiB slabs of three entries, and
    // the 16th entry needs a reclaim. Writes run one at a time so the order
    // is fixed.
    const BLOCK: u64 = 256 << 10;
    let store = Arc::new(TestStore::default());
    let objects: Vec<(Path, Bytes)> = (0..3u64)
        .map(|i| {
            (
                Path::from(format!("d/obj-{i}.vortex")),
                data(30 + i, 16 * BLOCK),
            )
        })
        .collect();
    for (p, bytes) in &objects {
        put(&store, p.as_ref(), bytes).await;
    }
    let tmp = tempfile::tempdir().expect("tempdir");
    let budget = 4 * MIB;
    let cache = testing::open_with(
        NodeCacheConfig {
            budget_bytes: budget,
            ..config(tmp.path())
        },
        Tuning {
            block_bytes: BLOCK,
            concurrent_writes: 1,
            // Whole-object reads into a 4 MiB budget are scans; the scan rule
            // is off so they fill the cache.
            scan_bytes: Some(u64::MAX),
            ..Tuning::default()
        },
    )
    .await
    .expect("open");
    let cached = cache.wrap(
        "s3://bucket/",
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        all_paths(),
    );
    let slabs = tmp.path().join("slabs");
    let dat_bytes = || -> u64 {
        std::fs::read_dir(&slabs)
            .expect("list")
            .map(|e| e.expect("entry").path())
            .filter(|p| p.extension().is_some_and(|e| e == "dat"))
            .map(|p| std::fs::metadata(&p).expect("stat").len())
            .sum()
    };
    let read = |i: usize, r: Range<u64>| {
        let cached = Arc::clone(&cached);
        let (p, bytes) = objects[i].clone();
        async move {
            let got = cached.get_range(&p, r.clone()).await.expect("read");
            assert_same(
                &got,
                &bytes.slice(
                    usize::try_from(r.start).expect("start")..usize::try_from(r.end).expect("end"),
                ),
                "read",
            );
        }
    };

    // 13 entries: slabs 0 to 3 full, slab 4 open with room for two more.
    read(0, 0..13 * BLOCK).await;
    cache.wait_idle().await;
    assert_eq!(cache.stats().entries, 13);

    // Unlinking fails while three more blocks are written: the 16th needs a
    // reclaim, which cannot remove slab 0's files.
    std::fs::set_permissions(&slabs, std::fs::Permissions::from_mode(0o555)).expect("chmod");
    read(0, 13 * BLOCK..16 * BLOCK).await;
    cache.wait_idle().await;
    std::fs::set_permissions(&slabs, std::fs::Permissions::from_mode(0o755)).expect("chmod back");
    // The failed reclaim left slab 0 as it was, its three entries listed.
    let stats = cache.stats();
    assert_eq!(stats.entries, 15, "{stats:?}");
    assert_eq!(stats.dropped_fills, 1, "{stats:?}");
    assert!(slabs.join("slab-00000000.dat").exists());

    // Keep filling: slab 0 is reclaimed again, and disk stays within budget.
    read(1, 0..16 * BLOCK).await;
    read(2, 0..16 * BLOCK).await;
    cache.wait_idle().await;
    let on_disk = dat_bytes();
    let stats = cache.stats();
    eprintln!("slab files {on_disk} bytes, budget {budget}: {stats:?}");
    assert!(
        !slabs.join("slab-00000000.dat").exists(),
        "slab 0, whose reclaim failed, is still on disk"
    );
    assert!(
        on_disk <= budget,
        "slab files {on_disk} bytes > budget {budget}"
    );
}

/// A store with one object whose bytes are generated as they are streamed,
/// in 1 MiB chunks, counting the bytes it has produced. Word `w` (8 bytes,
/// little-endian) of the object is `w * 0x9E37_79B9_7F4A_7C15`, so no two
/// blocks hold the same bytes.
#[derive(Debug)]
struct GeneratedStore {
    meta: ObjectMeta,
    pulled: Arc<AtomicU64>,
}

const GEN_CHUNK: u64 = 1 << 20;

/// The generated bytes of `range`.
fn generated(range: Range<u64>) -> Bytes {
    let first = range.start / 8;
    let last = range.end.div_ceil(8);
    let mut out = Vec::with_capacity(usize::try_from((last - first) * 8).expect("len"));
    for w in first..last {
        out.extend_from_slice(&w.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_le_bytes());
    }
    let skip = usize::try_from(range.start - first * 8).expect("skip");
    let len = usize::try_from(range.end - range.start).expect("len");
    Bytes::from(out).slice(skip..skip + len)
}

impl fmt::Display for GeneratedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GeneratedStore")
    }
}

#[async_trait]
impl ObjectStore for GeneratedStore {
    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        Err(not_supported())
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(not_supported())
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if *location != self.meta.location {
            return Err(object_store::Error::NotFound {
                path: location.to_string(),
                source: "no such object".into(),
            });
        }
        let size = self.meta.size;
        let range = match (&options.range, options.head) {
            (_, true) => 0..0,
            (None, false) => 0..size,
            (Some(r), false) => r.as_range(size).map_err(|e| object_store::Error::Generic {
                store: "GeneratedStore",
                source: e.into(),
            })?,
        };
        let pulled = Arc::clone(&self.pulled);
        let end = range.end;
        let chunks = futures::stream::unfold(range.start, move |at| {
            let pulled = Arc::clone(&pulled);
            async move {
                if at >= end {
                    return None;
                }
                let next = (at + GEN_CHUNK).min(end);
                pulled.fetch_add(next - at, Ordering::SeqCst);
                Some((Ok(generated(at..next)), next))
            }
        });
        Ok(GetResult {
            payload: object_store::GetResultPayload::Stream(chunks.boxed()),
            meta: self.meta.clone(),
            range,
            attributes: object_store::Attributes::default(),
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        locations.map(|_| Err(not_supported())).boxed()
    }

    fn list(&self, _prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        futures::stream::empty().boxed()
    }

    async fn list_with_delimiter(
        &self,
        _prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        Err(not_supported())
    }

    async fn copy_opts(
        &self,
        _from: &Path,
        _to: &Path,
        _options: CopyOptions,
    ) -> object_store::Result<()> {
        Err(not_supported())
    }
}

/// The process's peak resident set size so far, in bytes.
fn peak_rss() -> u64 {
    // SAFETY: `getrusage` only writes into the zeroed struct it is given.
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    // SAFETY: a valid pointer to an `rusage`.
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &raw mut usage) };
    let max = u64::try_from(usage.ru_maxrss).unwrap_or(0);
    // Bytes on macOS, KiB on Linux.
    if cfg!(target_os = "macos") {
        max
    } else {
        max * 1024
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scan_read_streams_from_the_store() {
    // A whole-object read that is a scan: a 256 MiB object under a 32 MiB
    // budget (more than
    // an eighth of the budget), and a 200 MiB object under a 2 GiB budget
    // (less than an eighth, but over the fixed 64 MiB the cache ever holds in
    // memory for one read). The wrapper must return the store's stream, not
    // read the object first: when the caller holds the first chunk, the store
    // has produced at most one chunk beyond it.
    for (size, budget) in [(256 * MIB, 32 * MIB), (200 * MIB, 2 << 30)] {
        let mem = InMemory::new();
        let p = Path::from("t/big.vortex");
        mem.put(&p, PutPayload::from_static(b"x"))
            .await
            .expect("put");
        let mut meta = mem.head(&p).await.expect("head");
        meta.size = size;
        let pulled = Arc::new(AtomicU64::new(0));
        let generated_store = Arc::new(GeneratedStore {
            meta,
            pulled: Arc::clone(&pulled),
        });
        let tmp = tempfile::tempdir().expect("tempdir");
        let cache = NodeCache::open(NodeCacheConfig {
            budget_bytes: budget,
            ..config(tmp.path())
        })
        .await
        .expect("open");
        let cached = cache.wrap(
            "s3://bucket/",
            Arc::clone(&generated_store) as Arc<dyn ObjectStore>,
            all_paths(),
        );
        let rss_before = peak_rss();
        let result = cached.get(&p).await.expect("get");
        let at_return = pulled.load(Ordering::SeqCst);
        assert_eq!(result.meta.size, size);
        assert_eq!(result.range, 0..size);
        let mut stream = result.into_stream();
        let first = stream.next().await.expect("a chunk").expect("chunk");
        let at_first = pulled.load(Ordering::SeqCst);
        let rss_first = peak_rss();
        eprintln!(
            "first chunk; peak RSS {rss_before} -> {rss_first} (+{} bytes)",
            rss_first.saturating_sub(rss_before)
        );
        assert!(
            at_first <= 2 * GEN_CHUNK,
            "{size}: the store produced {at_first} bytes ({at_return} before the wrapper returned) before the caller got its first chunk of {} bytes",
            first.len()
        );
        // The rest, checked chunk by chunk without holding the object.
        let mut at = first.len() as u64;
        assert_same(&first, &generated(0..at), "first chunk");
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.expect("chunk");
            let end = at + chunk.len() as u64;
            assert_same(&chunk, &generated(at..end), &format!("{at}..{end}"));
            at = end;
        }
        assert_eq!(at, size, "{size}");
        let rss_after = peak_rss();
        eprintln!(
            "produced {at_return} bytes before the wrapper returned, {at_first} before the first chunk; peak RSS {rss_before} -> {rss_after} (+{} bytes)",
            rss_after.saturating_sub(rss_before)
        );
        cache.wait_idle().await;
        let stats = cache.stats();
        assert_eq!((stats.entries, stats.fill_bytes), (0, 0), "{stats:?}");
        assert_eq!(stats.admission_bypass_reads, 1, "{stats:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[expect(clippy::single_range_in_vec_init, reason = "lists of one byte range")]
async fn noted_size_needs_no_head_after_a_restart() {
    let store = Arc::new(TestStore::default());
    let a = Path::from("d/a.vortex");
    let b = Path::from("d/b.vortex");
    let c = Path::from("d/c.vortex");
    let (a_bytes, b_bytes, c_bytes) = (data(40, 3 * MIB), data(41, 2 * MIB), data(42, MIB));
    put(&store, a.as_ref(), &a_bytes).await;
    put(&store, b.as_ref(), &b_bytes).await;
    put(&store, c.as_ref(), &c_bytes).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    {
        let cache = NodeCache::open(config(tmp.path())).await.expect("open");
        let cached = cache.wrap(
            "s3://bucket/",
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            all_paths(),
        );
        let got = cached.get_range(&a, 0..3 * MIB).await.expect("read a");
        assert_same(&got, &a_bytes, "a");
        cache.wait_idle().await;
        cache.close().await.expect("close");
    }
    // The dropped cache releases the directory once its last task ends.
    let deadline = Instant::now() + Duration::from_secs(5);
    let cache = loop {
        match NodeCache::open(config(tmp.path())).await {
            Ok(cache) => break cache,
            Err(object_store_cache::Error::Locked { .. }) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Err(e) => panic!("reopen: {e:?}"),
        }
    };
    let cached = cache.wrap(
        "s3://bucket/",
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        all_paths(),
    );
    for (p, size) in [(&a, 3 * MIB), (&b, 2 * MIB), (&c, MIB)] {
        cache.note_size("s3://bucket/", p, size);
    }
    let calls = || {
        (
            store.heads.load(Ordering::SeqCst),
            store.gets.load(Ordering::SeqCst),
        )
    };
    let usize_of = |n: u64| usize::try_from(n).expect("usize");

    // Resident blocks of a sized object: no request at all.
    let before = calls();
    let got = cached
        .get_ranges(&a, &[0..10, MIB..2 * MIB + 7])
        .await
        .expect("ranges of a");
    assert_same(&got[0], &a_bytes.slice(0..10), "a 0..10");
    assert_same(
        &got[1],
        &a_bytes.slice(usize_of(MIB)..usize_of(2 * MIB + 7)),
        "a block 1",
    );
    let got = cached.get_range(&a, 5..MIB).await.expect("range of a");
    assert_same(&got, &a_bytes.slice(5..usize_of(MIB)), "a 5..MiB");
    assert_eq!(calls(), before, "(HEADs, GETs) for resident blocks");

    // Missing blocks of a sized object: one GET each, no HEAD, by
    // `get_ranges` and by `get_opts` (whose metadata comes from the GET).
    let got = cached.get_ranges(&b, &[0..2 * MIB]).await.expect("b");
    assert_same(&got[0], &b_bytes, "b");
    assert_eq!(calls(), (before.0, before.1 + 1), "(HEADs, GETs) for b");
    let result = cached
        .get_opts(&c, GetOptions::new().with_range(Some(0..MIB)))
        .await
        .expect("c");
    let want_meta = store.inner.head(&c).await.expect("head c");
    assert_eq!(result.meta.size, MIB);
    assert_eq!(result.meta.last_modified, want_meta.last_modified);
    assert_eq!(result.meta.e_tag, want_meta.e_tag);
    assert_same(&result.bytes().await.expect("c bytes"), &c_bytes, "c");
    assert_eq!(calls(), (before.0, before.1 + 2), "(HEADs, GETs) for c");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stats_count_blocks_bytes_and_errors() {
    let store = Arc::new(TestStore::default());
    let a = Path::from("d/a.vortex");
    let b = Path::from("d/b.vortex");
    let (a_bytes, b_bytes) = (data(43, 3 * MIB), data(44, 2 * MIB));
    put(&store, a.as_ref(), &a_bytes).await;
    put(&store, b.as_ref(), &b_bytes).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let slabs = SlabStore::open(tmp.path(), 256 * MIB).await.expect("slabs");
    let failing = Arc::new(FaultStore::new(slabs));
    let cache = testing::with_block_store(
        config(tmp.path()),
        Tuning::default(),
        Arc::clone(&failing) as Arc<dyn BlockStore>,
    )
    .expect("cache");
    let cached = cache.wrap(
        "s3://bucket/",
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        all_paths(),
    );
    let read = |p: &Path, r: Range<u64>| {
        let cached = Arc::clone(&cached);
        let p = p.clone();
        async move { cached.get_range(&p, r).await.expect("read") }
    };

    // A miss on three blocks, then hits on them.
    read(&a, 0..3 * MIB).await;
    cache.wait_idle().await;
    let s = cache.stats();
    assert_eq!((s.accesses, s.hits), (3, 0), "{s:?}");
    assert_eq!(s.fetched_bytes_query, 3 * MIB, "{s:?}");
    read(&a, 0..3 * MIB).await;
    read(&a, 10..20).await;
    let s = cache.stats();
    assert_eq!((s.accesses, s.hits), (7, 4), "{s:?}");
    assert_eq!(s.fetched_bytes_query, 3 * MIB, "{s:?}");

    // Prefetch is counted apart from queries.
    cache
        .prefetch("s3://bucket/", &b, 2 * MIB, None)
        .expect("prefetch");
    cache.wait_idle().await;
    let s = cache.stats();
    assert_eq!(s.fetched_bytes_prefetch, 2 * MIB, "{s:?}");
    assert_eq!(s.fetched_bytes_query, 3 * MIB, "{s:?}");
    assert_eq!(s.entries, 5, "{s:?}");
    assert_eq!(s.used_bytes, 5 * (MIB + 4096), "{s:?}");
    assert_eq!(s.capacity_bytes, 1 << 30, "{s:?}");
    assert_eq!(s.fallbacks, 0, "{s:?}");
    assert_eq!(cache.last_error(), None);

    // Eviction by reference.
    let evicted = cache
        .evict_unreferenced("s3://bucket/", &Path::from("d"), |p| *p != b)
        .await;
    assert_eq!(evicted, 2 * MIB);
    assert_eq!(cache.stats().evicted_bytes_unreferenced, 2 * MIB);

    // A corrupt entry is dropped and read from the store.
    let block0 = a_bytes.slice(0..usize::try_from(MIB).expect("MiB"));
    let (slab, at) = find_in_slabs(tmp.path(), &block0[..4096]);
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&slab)
        .expect("open slab");
    file.write_all_at(&[block0[100] ^ 0xFF], at + 100)
        .expect("corrupt");
    drop(file);
    let got = read(&a, 0..MIB).await;
    assert_same(&got, &block0, "a block 0 after corruption");
    let s = cache.stats();
    assert_eq!(s.evicted_bytes_corrupt, MIB, "{s:?}");
    assert_eq!(s.fallbacks, 1, "{s:?}");

    // A failing disk read is a fallback, and its error is reported.
    failing.fail_gets.store(true, Ordering::SeqCst);
    let got = read(&a, MIB..2 * MIB).await;
    assert_same(
        &got,
        &a_bytes
            .slice(usize::try_from(MIB).expect("MiB")..usize::try_from(2 * MIB).expect("2 MiB")),
        "a block 1",
    );
    assert_eq!(cache.stats().fallbacks, 2);
    let last = cache.last_error().expect("an error");
    assert!(last.contains("injected read failure"), "{last}");
}

#[tokio::test]
#[expect(clippy::single_range_in_vec_init, reason = "lists of one byte range")]
async fn get_ranges_past_the_end_match_the_store() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().join("store");
    std::fs::create_dir_all(&root).expect("mkdir");
    let local: Arc<dyn ObjectStore> = Arc::new(
        object_store::local::LocalFileSystem::new_with_prefix(&root).expect("local store"),
    );
    let p = Path::from("d/a.vortex");
    let size = 3 * MIB;
    local
        .put(&p, PutPayload::from_bytes(data(45, size)))
        .await
        .expect("put");
    let cache = NodeCache::open(config(&tmp.path().join("cache")))
        .await
        .expect("open");
    let cached = cache.wrap("file:///store/", Arc::clone(&local), all_paths());
    // Fill the blocks, so the size is known and the bytes resident.
    cached.get_range(&p, 0..size).await.expect("fill");
    cache.wait_idle().await;
    for ranges in [
        vec![0..10, size - 5..size + 5],
        vec![size - 1..size + MIB],
        vec![size..size + 1],
        vec![0..size + 1],
    ] {
        let want = local.get_ranges(&p, &ranges).await;
        let got = cached.get_ranges(&p, &ranges).await;
        match (&want, &got) {
            (Err(w), Err(g)) => {
                assert_eq!(
                    std::mem::discriminant(w),
                    std::mem::discriminant(g),
                    "{ranges:?}: want {w}, got {g}"
                );
                assert_eq!(w.to_string(), g.to_string(), "{ranges:?}");
            }
            (Ok(w), Ok(g)) => assert_eq!(w, g, "{ranges:?}"),
            _ => panic!("{ranges:?}: want {want:?}, got {got:?}"),
        }
    }
}
