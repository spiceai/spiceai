// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::collections::{HashMap, HashSet};
use std::hash::BuildHasherDefault;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use async_trait::async_trait;
use moka::future::Cache;
use object_store::path::Path;
use opentelemetry::metrics::{Gauge, Meter};
use opentelemetry::{KeyValue, global};
use parking_lot::Mutex;
use twox_hash::XxHash3_64;
use vortex::buffer::ByteBuffer;
use vortex::error::VortexResult;
use vortex::layout::segments::{SegmentCache, SegmentId};

/// Hasher for the segment cache key `(Path, SegmentId)`. XXH3 matches the
/// project-wide cache hashing default and is markedly faster than moka's default
/// `SipHash` on the per-segment hot path.
type SegmentCacheHasher = BuildHasherDefault<XxHash3_64>;
type PathStates = Arc<Mutex<HashMap<Path, Weak<PathCacheState>>>>;

/// Publish a segment-cache stats sample once every this many `get` calls (per
/// table cache).
const SEGMENT_CACHE_STATS_SAMPLE_EVERY: u64 = 200_000;

static METER: LazyLock<Meter> = LazyLock::new(|| global::meter("cayenne_segment_cache"));

/// Cumulative `get` calls, by `dataset`.
static ACCESSES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("cayenne_segment_cache_accesses")
        .with_description("Cumulative Vortex segment cache get() calls.")
        .build()
});
/// Cumulative cache hits, by `dataset`. Miss count and hit rate derive from this
/// and `cayenne_segment_cache_accesses`.
static HITS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("cayenne_segment_cache_hits")
        .with_description("Cumulative Vortex segment cache hits.")
        .build()
});
/// Live cache fill in bytes, by `dataset`. Compare to
/// `cayenne_segment_cache_capacity_bytes` for the fill level — the key
/// right-sizing signal.
static WEIGHTED_BYTES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("cayenne_segment_cache_weighted_bytes")
        .with_description("Live Vortex segment cache size in bytes.")
        .with_unit("By")
        .build()
});
/// Configured cache capacity in bytes, by `dataset`.
static CAPACITY_BYTES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("cayenne_segment_cache_capacity_bytes")
        .with_description("Configured Vortex segment cache capacity in bytes.")
        .with_unit("By")
        .build()
});
/// Live cached entry count, by `dataset`.
static ENTRIES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("cayenne_segment_cache_entries")
        .with_description("Live Vortex segment cache entry count.")
        .build()
});

/// Shared segment cache keyed by file path and Vortex segment id.
///
/// Vortex segment ids are local to each file. This wrapper keeps a single
/// bounded cache across files while presenting each open file with the
/// `SegmentCache` interface it expects.
#[derive(Clone, Debug)]
pub(crate) struct SharedSegmentCache {
    cache: Cache<(Arc<Path>, SegmentId), ByteBuffer, SegmentCacheHasher>,
    /// Weak per-path insertion states. An open file keeps its state alive; the
    /// last [`PathSegmentCache`] drop removes the weak registry entry, so paths
    /// retired over the table's lifetime do not accumulate as tombstones.
    path_states: Option<PathStates>,
    /// Total `get` calls; drives periodic stats sampling.
    accesses: Arc<AtomicU64>,
    /// `get` calls that returned a cached buffer (a hit).
    hits: Arc<AtomicU64>,
    /// Configured byte capacity, reported next to the live fill.
    capacity_bytes: u64,
    /// Dataset label applied to the emitted metrics (table name; `unknown` when
    /// the cache is built without one — e.g. the plain `DataFusion` format path).
    dataset: Arc<str>,
}

impl SharedSegmentCache {
    pub(crate) fn new(
        max_capacity_bytes: u64,
        dataset: Option<Arc<str>>,
        track_retirement: bool,
    ) -> Self {
        let path_states = track_retirement.then(|| Arc::new(Mutex::new(HashMap::new())));
        Self {
            cache: Cache::builder()
                .name("vortex-datafusion-segment-cache")
                .max_capacity(max_capacity_bytes)
                .weigher(|_, buffer: &ByteBuffer| {
                    u32::try_from(buffer.len().min(u32::MAX as usize)).unwrap_or(u32::MAX)
                })
                .build_with_hasher(SegmentCacheHasher::default()),
            path_states,
            accesses: Arc::new(AtomicU64::new(0)),
            hits: Arc::new(AtomicU64::new(0)),
            capacity_bytes: max_capacity_bytes,
            dataset: dataset.unwrap_or_else(|| Arc::from("unknown")),
        }
    }

    pub(crate) fn for_path(&self, path: Path) -> Arc<dyn SegmentCache> {
        let state = self.path_states.as_ref().map(|path_states| {
            let mut states = path_states.lock();
            states
                .get(&path)
                .and_then(Weak::upgrade)
                .unwrap_or_else(|| {
                    let state = Arc::new(PathCacheState::default());
                    states.insert(path.clone(), Arc::downgrade(&state));
                    state
                })
        });
        Arc::new(PathSegmentCache {
            shared: self.clone(),
            path: Arc::new(path),
            state,
        })
    }

    pub(crate) async fn invalidate_paths(&self, paths: HashSet<Path>) {
        if paths.is_empty() {
            return;
        }

        // Mark every still-open path retired before enumerating keys. A put
        // that started before this mark increments `active_puts`, so waiting
        // for zero proves its insert is visible to the enumeration. A later
        // put observes `retired` and skips insertion. This closes the
        // delete/invalidate/late-put race without permanent path tombstones:
        // the state disappears when the last open file cache is dropped.
        let states: Vec<_> = self
            .path_states
            .as_ref()
            .map_or_else(Vec::new, |path_states| {
                let states = path_states.lock();
                paths
                    .iter()
                    .filter_map(|path| states.get(path).and_then(Weak::upgrade))
                    .collect()
            });
        for state in &states {
            state.retired.store(true, Ordering::SeqCst);
        }
        for state in &states {
            while state.active_puts.load(Ordering::SeqCst) > 0 {
                // Back off instead of spinning on a Moka insert. This is not a
                // safety timeout: the path is not enumerated until every put
                // that registered before retirement has completed or canceled.
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        }

        // Enumerate the exact keys and use Moka's direct async invalidation so
        // returning means the buffers are removed from the cache table;
        // predicate invalidation would defer physical eviction to bounded
        // maintenance passes.
        let keys: Vec<_> = self
            .cache
            .iter()
            .filter_map(|(key, _)| paths.contains(key.0.as_ref()).then(|| key.as_ref().clone()))
            .collect();
        for key in keys {
            self.cache.invalidate(&key).await;
        }
        // Direct invalidation removes the hash-table entries immediately, but
        // Moka's queued policy-removal records still retain the removed values
        // until housekeeping drains them. Unlike predicate scanning, this pass
        // only has to consume the already-enqueued exact-key removals.
        self.run_pending_tasks().await;

        drop(states);
        if let Some(path_states) = self.path_states.as_ref() {
            path_states
                .lock()
                .retain(|_, state| state.strong_count() > 0);
        }
    }

    pub(crate) async fn run_pending_tasks(&self) {
        self.cache.run_pending_tasks().await;
    }

    pub(crate) async fn entry_count(&self) -> u64 {
        self.run_pending_tasks().await;
        self.cache.entry_count()
    }
}

struct PathSegmentCache {
    shared: SharedSegmentCache,
    // `Arc<Path>` so forming the `(path, segment)` cache key on every `get`/`put`
    // is a refcount bump, not a `Path` (string) clone — segment reads are hot.
    path: Arc<Path>,
    state: Option<Arc<PathCacheState>>,
}

#[derive(Debug, Default)]
struct PathCacheState {
    retired: AtomicBool,
    active_puts: AtomicUsize,
}

struct ActivePutGuard<'a>(&'a PathCacheState);

impl Drop for ActivePutGuard<'_> {
    fn drop(&mut self) {
        self.0.active_puts.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl SegmentCache for PathSegmentCache {
    async fn get(&self, id: SegmentId) -> VortexResult<Option<ByteBuffer>> {
        let result = self.shared.cache.get(&(Arc::clone(&self.path), id)).await;

        // Segment-cache right-sizing telemetry: count accesses/hits and, every
        // SEGMENT_CACHE_STATS_SAMPLE_EVERY calls, publish the cumulative hit rate
        // and live fill (weighted bytes vs capacity) as OpenTelemetry gauges
        // labelled by `dataset`. When no meter provider is installed (Prometheus
        // off) the records are cheap no-ops.
        if result.is_some() {
            self.shared.hits.fetch_add(1, Ordering::Relaxed);
        }
        let accesses = self.shared.accesses.fetch_add(1, Ordering::Relaxed) + 1;
        if accesses.is_multiple_of(SEGMENT_CACHE_STATS_SAMPLE_EVERY) {
            // Rare branch: flush moka bookkeeping so the reported fill is accurate.
            self.shared.cache.run_pending_tasks().await;
            let hits = self.shared.hits.load(Ordering::Relaxed);
            let labels = [KeyValue::new("dataset", self.shared.dataset.to_string())];
            ACCESSES.record(accesses, &labels);
            HITS.record(hits, &labels);
            WEIGHTED_BYTES.record(self.shared.cache.weighted_size(), &labels);
            CAPACITY_BYTES.record(self.shared.capacity_bytes, &labels);
            ENTRIES.record(self.shared.cache.entry_count(), &labels);
        }

        Ok(result)
    }

    async fn put(&self, id: SegmentId, buffer: ByteBuffer) -> VortexResult<()> {
        // Two checks around the active-put registration close both races with
        // retirement: a put already registered makes invalidation wait; a put
        // that read `retired = false` just before the mark observes it on the
        // second check and never inserts.
        let _active_put = if let Some(state) = self.state.as_ref() {
            if state.retired.load(Ordering::SeqCst) {
                return Ok(());
            }
            state.active_puts.fetch_add(1, Ordering::SeqCst);
            let guard = ActivePutGuard(state);
            if state.retired.load(Ordering::SeqCst) {
                return Ok(());
            }
            Some(guard)
        } else {
            None
        };
        self.shared
            .cache
            .insert((Arc::clone(&self.path), id), buffer)
            .await;
        Ok(())
    }
}

impl Drop for PathSegmentCache {
    fn drop(&mut self) {
        let (Some(state), Some(path_states)) =
            (self.state.as_ref(), self.shared.path_states.as_ref())
        else {
            return;
        };
        if Arc::strong_count(state) != 1 {
            return;
        }
        let mut states = path_states.lock();
        if states
            .get(self.path.as_ref())
            .and_then(Weak::upgrade)
            .is_some_and(|registered| Arc::ptr_eq(&registered, state))
        {
            states.remove(self.path.as_ref());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn get_put_roundtrip_and_path_isolation() {
        let shared = SharedSegmentCache::new(1 << 20, None, false);
        let cache_a = shared.for_path(Path::from("a.vortex"));
        let cache_b = shared.for_path(Path::from("b.vortex"));
        let id = SegmentId::from(1);

        // Miss before insert.
        assert!(
            cache_a
                .get(id)
                .await
                .expect("get should not error")
                .is_none()
        );

        cache_a
            .put(id, ByteBuffer::from(vec![1u8, 2, 3, 4]))
            .await
            .expect("put should not error");

        // Hit on the same path + segment id.
        assert_eq!(
            cache_a
                .get(id)
                .await
                .expect("get should not error")
                .map(|b| b.len()),
            Some(4)
        );

        // The cache key is (path, segment id): a different path with the same
        // segment id must not collide — path isolation must survive the switch to
        // an `Arc<Path>` key.
        assert!(
            cache_b
                .get(id)
                .await
                .expect("get should not error")
                .is_none()
        );
    }

    #[tokio::test]
    async fn invalidates_exact_paths_only() {
        let shared = SharedSegmentCache::new(1 << 20, Some(Arc::from("test")), true);
        let path_a = Path::from("snapshot-a/a.vortex");
        let path_b = Path::from("snapshot-b/b.vortex");
        let cache_a = shared.for_path(path_a.clone());
        let cache_b = shared.for_path(path_b.clone());
        let id = SegmentId::from(1);

        cache_a
            .put(id, ByteBuffer::from(vec![1u8, 2, 3, 4]))
            .await
            .expect("put for retired path should not error");
        cache_b
            .put(id, ByteBuffer::from(vec![5u8, 6, 7, 8]))
            .await
            .expect("put for live path should not error");

        shared.invalidate_paths(HashSet::from([path_a])).await;

        assert!(
            cache_a
                .get(id)
                .await
                .expect("get for retired path should not error")
                .is_none(),
            "the retired path must be invalidated"
        );
        assert!(
            cache_b
                .get(id)
                .await
                .expect("get for live path should not error")
                .is_some(),
            "an unrelated live path must remain cached"
        );

        let late_id = SegmentId::from(2);
        cache_a
            .put(late_id, ByteBuffer::from(vec![9u8, 10, 11, 12]))
            .await
            .expect("a late put for a retired path should be ignored, not fail");
        assert!(
            cache_a
                .get(late_id)
                .await
                .expect("get after a late retired-path put should not error")
                .is_none(),
            "an already-open file cache must not repopulate a retired path"
        );
    }

    #[tokio::test]
    async fn invalidation_physically_evicts_entries_without_later_cache_activity() {
        let shared = SharedSegmentCache::new(1 << 20, Some(Arc::from("test")), true);
        let retired_path = Path::from("snapshot-a/retired.vortex");
        let live_path = Path::from("snapshot-b/live.vortex");
        let retired = shared.for_path(retired_path.clone());
        let live = shared.for_path(live_path);
        let id = SegmentId::from(1);

        retired
            .put(id, ByteBuffer::from(vec![1u8, 2, 3, 4]))
            .await
            .expect("put for retired path should not error");
        live.put(id, ByteBuffer::from(vec![5u8, 6, 7, 8]))
            .await
            .expect("put for live path should not error");
        for index in 0..256 {
            shared
                .for_path(Path::from(format!("unrelated/{index}.vortex")))
                .put(id, ByteBuffer::from(vec![9u8, 10, 11, 12]))
                .await
                .expect("put for unrelated path should not error");
        }
        shared.run_pending_tasks().await;
        assert_eq!(
            shared.cache.entry_count(),
            258,
            "retired, live, and more than one maintenance batch of unrelated paths should be resident"
        );

        shared.invalidate_paths(HashSet::from([retired_path])).await;

        assert_eq!(
            shared.cache.entry_count(),
            257,
            "invalidation must physically evict only the retired buffers before it returns"
        );
    }
}
