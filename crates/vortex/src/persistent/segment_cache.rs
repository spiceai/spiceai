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
    cache: Cache<(Path, SegmentId), ByteBuffer, SegmentCacheHasher>,
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
            path,
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
        // put observes `retired` and skips insertion.
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
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        }

        let keys: Vec<_> = self
            .cache
            .iter()
            .filter_map(|(key, _)| paths.contains(&key.0).then(|| key.as_ref().clone()))
            .collect();
        for key in keys {
            self.cache.invalidate(&key).await;
        }
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
    path: Path,
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
        let result = self.shared.cache.get(&(self.path.clone(), id)).await;

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
            .insert((self.path.clone(), id), buffer)
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
        let mut states = path_states.lock();
        // `for_path` takes this same mutex before upgrading the registry's
        // weak entry. Check ownership only after acquiring it.
        let is_registered_state = states
            .get(&self.path)
            .is_some_and(|registered| std::ptr::addr_eq(registered.as_ptr(), Arc::as_ptr(state)));
        if is_registered_state && Arc::strong_count(state) == 1 {
            states.remove(&self.path);
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
        assert_eq!(
            cache_a
                .get(id)
                .await
                .expect("get should not error")
                .map(|b| b.len()),
            Some(4)
        );
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
        let cache_b = shared.for_path(path_b);
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
                .expect("get should not error")
                .is_none()
        );
        assert!(
            cache_b
                .get(id)
                .await
                .expect("get should not error")
                .is_some()
        );

        let late_id = SegmentId::from(2);
        cache_a
            .put(late_id, ByteBuffer::from(vec![9u8, 10, 11, 12]))
            .await
            .expect("late put should not error");
        assert!(
            cache_a
                .get(late_id)
                .await
                .expect("get should not error")
                .is_none()
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
            .expect("put should not error");
        live.put(id, ByteBuffer::from(vec![5u8, 6, 7, 8]))
            .await
            .expect("put should not error");
        for index in 0..256 {
            shared
                .for_path(Path::from(format!("unrelated/{index}.vortex")))
                .put(id, ByteBuffer::from(vec![9u8, 10, 11, 12]))
                .await
                .expect("put should not error");
        }
        shared.run_pending_tasks().await;
        assert_eq!(shared.cache.entry_count(), 258);

        shared.invalidate_paths(HashSet::from([retired_path])).await;
        assert_eq!(shared.cache.entry_count(), 257);
    }

    #[tokio::test]
    async fn path_state_stays_registered_until_the_last_open_file_drops() {
        let shared = SharedSegmentCache::new(1 << 20, Some(Arc::from("test")), true);
        let path = Path::from("snapshot/shared.vortex");
        let first = shared.for_path(path.clone());
        let second = shared.for_path(path.clone());
        let states = shared.path_states.as_ref().expect("tracking enabled");

        assert_eq!(
            states
                .lock()
                .get(&path)
                .map_or(0, std::sync::Weak::strong_count),
            2
        );
        drop(first);
        assert_eq!(
            states
                .lock()
                .get(&path)
                .map_or(0, std::sync::Weak::strong_count),
            1
        );

        shared.invalidate_paths(HashSet::from([path.clone()])).await;
        second
            .put(SegmentId::from(1), ByteBuffer::from(vec![1u8, 2, 3, 4]))
            .await
            .expect("late put is ignored");
        assert!(
            second
                .get(SegmentId::from(1))
                .await
                .expect("get after retirement")
                .is_none()
        );

        drop(second);
        assert!(!states.lock().contains_key(&path));
    }
}
