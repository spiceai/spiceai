// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::collections::{HashMap, HashSet};
use std::hash::BuildHasherDefault;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, Weak};

use async_trait::async_trait;
use moka::future::Cache;
use object_store::path::Path;
use opentelemetry::global;
use opentelemetry::metrics::{Meter, ObservableCounter, ObservableGauge};
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

/// The process-wide segment cache, installed once at startup by
/// [`install_process_segment_cache`].
static PROCESS_CACHE: OnceLock<Arc<SharedSegmentCache>> = OnceLock::new();

/// Keeps the process cache's observable instruments alive. `OpenTelemetry`
/// retains observable callbacks for the meter provider's lifetime, so the
/// instruments are registered once and never rebuilt.
static PROCESS_METRICS: OnceLock<SegmentCacheMetrics> = OnceLock::new();

/// Install the process-wide segment cache with a total budget of
/// `max_capacity_bytes`, shared by every table.
///
/// One cache for the whole process rather than one per table: the key carries
/// the full object-store path, so tables cannot collide, and a single budget
/// means adding a table divides a fixed pool instead of reserving another
/// full-sized cache. Moka fixes capacity at build time, so per-table caches
/// could not be resized to share a budget without discarding their contents.
///
/// Returns `false` when a cache is already installed (the existing one keeps its
/// capacity) or when `max_capacity_bytes` is zero, which disables caching.
pub fn install_process_segment_cache(max_capacity_bytes: u64) -> bool {
    if max_capacity_bytes == 0 {
        return false;
    }
    // Retirement tracking is always on here: Cayenne needs it to keep a late put
    // from repopulating a retired path, the per-put cost is one atomic load, and
    // read-only formats share this cache with mutable ones.
    let cache = Arc::new(SharedSegmentCache::new(max_capacity_bytes, true));
    if PROCESS_CACHE.set(Arc::clone(&cache)).is_err() {
        return false;
    }
    let _ = PROCESS_METRICS.set(SegmentCacheMetrics::register(
        &global::meter("cayenne_segment_cache"),
        &cache,
    ));
    true
}

/// The installed process-wide cache, or `None` when segment caching is disabled.
pub(crate) fn process_segment_cache() -> Option<&'static Arc<SharedSegmentCache>> {
    PROCESS_CACHE.get()
}

/// Observable instruments over one segment cache.
///
/// Held for the process (or, in tests, for the harness) lifetime. The callbacks
/// keep only a [`Weak`] reference so a dropped cache is not kept alive and a
/// scrape after the drop simply reports nothing.
struct SegmentCacheMetrics {
    _accesses: ObservableCounter<u64>,
    _hits: ObservableCounter<u64>,
    _weighted_bytes: ObservableGauge<u64>,
    _capacity_bytes: ObservableGauge<u64>,
    _entries: ObservableGauge<u64>,
}

impl SegmentCacheMetrics {
    fn register(meter: &Meter, cache: &Arc<SharedSegmentCache>) -> Self {
        let accesses = {
            let cache = Arc::downgrade(cache);
            meter
                .u64_observable_counter("cayenne_segment_cache_accesses")
                .with_description("Cumulative Vortex segment cache get() calls.")
                .with_callback(move |observer| {
                    if let Some(cache) = cache.upgrade() {
                        cache.observe_accesses(|value| observer.observe(value, &[]));
                    }
                })
                .build()
        };
        let hits = {
            let cache = Arc::downgrade(cache);
            meter
                .u64_observable_counter("cayenne_segment_cache_hits")
                .with_description("Cumulative Vortex segment cache hits.")
                .with_callback(move |observer| {
                    if let Some(cache) = cache.upgrade() {
                        cache.observe_hits(|value| observer.observe(value, &[]));
                    }
                })
                .build()
        };
        let weighted_bytes = {
            let cache = Arc::downgrade(cache);
            meter
                .u64_observable_gauge("cayenne_segment_cache_weighted_bytes")
                .with_description("Approximate live Vortex segment cache size in bytes.")
                .with_unit("By")
                .with_callback(move |observer| {
                    if let Some(cache) = cache.upgrade() {
                        observer.observe(cache.cache.weighted_size(), &[]);
                    }
                })
                .build()
        };
        let capacity_bytes = {
            let cache = Arc::downgrade(cache);
            meter
                .u64_observable_gauge("cayenne_segment_cache_capacity_bytes")
                .with_description("Configured Vortex segment cache capacity in bytes.")
                .with_unit("By")
                .with_callback(move |observer| {
                    if let Some(cache) = cache.upgrade() {
                        observer.observe(cache.capacity_bytes, &[]);
                    }
                })
                .build()
        };
        let entries = {
            let cache = Arc::downgrade(cache);
            meter
                .u64_observable_gauge("cayenne_segment_cache_entries")
                .with_description("Approximate live Vortex segment cache entry count.")
                .with_callback(move |observer| {
                    if let Some(cache) = cache.upgrade() {
                        observer.observe(cache.cache.entry_count(), &[]);
                    }
                })
                .build()
        };

        Self {
            _accesses: accesses,
            _hits: hits,
            _weighted_bytes: weighted_bytes,
            _capacity_bytes: capacity_bytes,
            _entries: entries,
        }
    }
}

/// Segment cache keyed by file path and Vortex segment id.
///
/// Vortex segment ids are local to each file, so the key pairs the id with the
/// file's object-store path. That key is globally unique, which is what lets one
/// cache — and one byte budget — serve every table in the process. Nothing here
/// is per-table, so the metrics above carry no `dataset` label: fill, capacity
/// and hit rate describe the single shared resource.
#[derive(Debug)]
pub(crate) struct SharedSegmentCache {
    cache: Cache<(Arc<Path>, SegmentId), ByteBuffer, SegmentCacheHasher>,
    /// Weak per-path insertion states. An open file keeps its state alive; the
    /// last [`PathSegmentCache`] drop removes the weak registry entry, so paths
    /// retired over the process's lifetime do not accumulate as tombstones.
    path_states: Option<PathStates>,
    /// Configured byte capacity, reported next to the live fill.
    capacity_bytes: u64,
    /// Cumulative `get` calls. Read directly during collection, so the hot path
    /// neither allocates labels nor records synchronously.
    accesses: AtomicU64,
    /// `get` calls that returned a cached buffer (a hit).
    hits: AtomicU64,
    /// Access total published by the last collection. Observable callbacks can
    /// run independently and readers can collect concurrently; clamping hits to
    /// this bound keeps a hit total from being published against an access total
    /// no reader has seen yet, while leaving the read path lock-free.
    last_observed_accesses: Mutex<u64>,
}

impl SharedSegmentCache {
    /// A cache with a budget of its own. Callers outside the process cache use
    /// this — a standalone `VortexFormat` configured with
    /// `segment_cache_size_bytes`, and tests that need an isolated budget.
    pub(crate) fn new(max_capacity_bytes: u64, track_retirement: bool) -> Self {
        Self {
            cache: Cache::builder()
                .name("vortex-datafusion-segment-cache")
                .max_capacity(max_capacity_bytes)
                .weigher(|_, buffer: &ByteBuffer| {
                    u32::try_from(buffer.len().min(u32::MAX as usize)).unwrap_or(u32::MAX)
                })
                .build_with_hasher(SegmentCacheHasher::default()),
            path_states: track_retirement.then(|| Arc::new(Mutex::new(HashMap::new()))),
            capacity_bytes: max_capacity_bytes,
            accesses: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            last_observed_accesses: Mutex::new(0),
        }
    }

    fn observe_accesses(&self, observe: impl FnOnce(u64)) {
        let mut last_observed_accesses = self.last_observed_accesses.lock();
        let accesses = self.accesses.load(Ordering::Relaxed);
        observe(accesses);
        *last_observed_accesses = accesses;
    }

    fn observe_hits(&self, observe: impl FnOnce(u64)) {
        let last_observed_accesses = self.last_observed_accesses.lock();
        let hits = self.hits.load(Ordering::Relaxed);
        observe(hits.min(*last_observed_accesses));
    }

    pub(crate) fn for_path(self: &Arc<Self>, path: Path) -> Arc<dyn SegmentCache> {
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
            shared: Arc::clone(self),
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
    shared: Arc<SharedSegmentCache>,
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

        // Collection reads these atomics directly, so the hot path never
        // allocates labels or synchronously records metrics.
        self.shared.accesses.fetch_add(1, Ordering::Relaxed);
        if result.is_some() {
            self.shared.hits.fetch_add(1, Ordering::Relaxed);
        }

        Ok(result)
    }

    async fn put(&self, id: SegmentId, buffer: ByteBuffer) -> VortexResult<()> {
        // Copy into an exact-sized allocation before inserting.
        //
        // The buffer handed to us is usually a view into a coalesced read block:
        // `ObjectStoreReadAt` merges requests within 1 MiB into reads of up to
        // 16 MiB, and the reader hands each segment `base.slice(..)` over that one
        // allocation. `ByteBuffer` is backed by `bytes::Bytes`, so a slice shares
        // the allocation and keeps all of it alive while the weigher counts only
        // the slice's length. Caching a view would therefore let one small
        // segment pin its whole block, and — worse — the overshoot would grow as
        // eviction proceeded, because dropping some slices frees nothing until
        // the last one goes. Copying costs one memcpy on the miss path (the read
        // that produced the buffer cost far more) and makes the weight the true
        // resident size, so `max_capacity` bounds real memory.
        let buffer = ByteBuffer::copy_from_aligned(buffer.as_slice(), buffer.alignment());

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
        let mut states = path_states.lock();
        // `for_path` takes this same mutex before upgrading the registry's
        // weak entry. Check ownership only after acquiring it: otherwise an
        // opener can upgrade between an out-of-lock last-owner check and this
        // removal, leaving that opener on an unregistered state that a later
        // retirement cannot mark.
        let is_registered_state = states
            .get(self.path.as_ref())
            .is_some_and(|registered| std::ptr::addr_eq(registered.as_ptr(), Arc::as_ptr(state)));
        if is_registered_state && Arc::strong_count(state) == 1 {
            states.remove(self.path.as_ref());
        }
    }
}
#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Weak;
    use std::time::Duration;

    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry_sdk::Resource;
    use opentelemetry_sdk::error::OTelSdkResult;
    use opentelemetry_sdk::metrics::SdkMeterProvider;
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
    use opentelemetry_sdk::metrics::reader::MetricReader;
    use opentelemetry_sdk::metrics::{ManualReader, Pipeline, Temporality};
    use prometheus::proto::MetricType;

    use super::*;

    struct MetricsHarness {
        registry: prometheus::Registry,
        _provider: SdkMeterProvider,
        metrics: SegmentCacheMetrics,
    }

    impl MetricsHarness {
        fn new() -> Self {
            let registry = prometheus::Registry::new();
            let exporter = opentelemetry_prometheus::exporter()
                .with_registry(registry.clone())
                .without_scope_info()
                .without_units()
                .without_counter_suffixes()
                .without_target_info()
                .build()
                .expect("build the Prometheus exporter");
            let provider = SdkMeterProvider::builder()
                .with_resource(Resource::builder_empty().build())
                .with_reader(exporter)
                .build();
            let metrics = SegmentCacheMetrics::new(&provider.meter("cayenne_segment_cache_test"));

            Self {
                registry,
                _provider: provider,
                metrics,
            }
        }

        fn cache(&self, capacity_bytes: u64, dataset: &str) -> SharedSegmentCache {
            SharedSegmentCache::new_registered(
                capacity_bytes,
                Some(Arc::from(dataset)),
                false,
                &self.metrics,
            )
        }

        fn gather(&self) -> MetricSamples {
            MetricSamples::from_registry(&self.registry)
        }
    }

    #[derive(Clone, Debug)]
    struct SharedManualReader(Arc<ManualReader>);

    impl MetricReader for SharedManualReader {
        fn register_pipeline(&self, pipeline: Weak<Pipeline>) {
            self.0.register_pipeline(pipeline);
        }

        fn collect(&self, metrics: &mut ResourceMetrics) -> OTelSdkResult {
            self.0.collect(metrics)
        }

        fn force_flush(&self) -> OTelSdkResult {
            self.0.force_flush()
        }

        fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
            self.0.shutdown_with_timeout(timeout)
        }

        fn temporality(&self, kind: opentelemetry_sdk::metrics::InstrumentKind) -> Temporality {
            self.0.temporality(kind)
        }
    }

    struct DeltaMetricsHarness {
        registry: prometheus::Registry,
        _provider: SdkMeterProvider,
        reader: SharedManualReader,
        metrics: SegmentCacheMetrics,
    }

    impl DeltaMetricsHarness {
        fn new() -> Self {
            let registry = prometheus::Registry::new();
            let exporter = opentelemetry_prometheus::exporter()
                .with_registry(registry.clone())
                .without_scope_info()
                .without_units()
                .without_counter_suffixes()
                .without_target_info()
                .build()
                .expect("build the Prometheus exporter");
            let reader = SharedManualReader(Arc::new(
                ManualReader::builder()
                    .with_temporality(Temporality::Delta)
                    .build(),
            ));
            let provider = SdkMeterProvider::builder()
                .with_resource(Resource::builder_empty().build())
                .with_reader(exporter)
                .with_reader(reader.clone())
                .build();
            let metrics = SegmentCacheMetrics::new(&provider.meter("segment_cache_delta_test"));

            Self {
                registry,
                _provider: provider,
                reader,
                metrics,
            }
        }

        fn cache(&self, capacity_bytes: u64, dataset: &str) -> SharedSegmentCache {
            SharedSegmentCache::new_registered(
                capacity_bytes,
                Some(Arc::from(dataset)),
                false,
                &self.metrics,
            )
        }

        fn gather(&self) -> MetricSamples {
            MetricSamples::from_registry(&self.registry)
        }

        fn collect_accesses(&self, dataset: &str) -> Option<u64> {
            let mut resource_metrics = ResourceMetrics::default();
            self.reader
                .collect(&mut resource_metrics)
                .expect("collect delta segment-cache metrics");

            for metric in resource_metrics
                .scope_metrics()
                .flat_map(|scope| scope.metrics())
                .filter(|metric| metric.name() == "cayenne_segment_cache_accesses")
            {
                let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() else {
                    panic!("segment-cache accesses must be a u64 sum");
                };
                if let Some(point) = sum.data_points().find(|point| {
                    point.attributes().any(|attribute| {
                        attribute.key.as_str() == "dataset" && attribute.value.as_str() == dataset
                    })
                }) {
                    return Some(point.value());
                }
            }

            None
        }
    }

    struct MetricSamples {
        values: HashMap<(String, String), (MetricType, f64)>,
        series_counts: HashMap<(String, String), usize>,
    }

    impl MetricSamples {
        fn from_registry(registry: &prometheus::Registry) -> Self {
            let mut values = HashMap::new();
            let mut series_counts = HashMap::new();

            for family in registry
                .gather()
                .into_iter()
                .filter(|family| family.name().starts_with("cayenne_segment_cache_"))
            {
                let metric_type = family.get_field_type();
                for metric in family.get_metric() {
                    let dataset = metric
                        .get_label()
                        .iter()
                        .find(|label| label.name() == "dataset")
                        .map_or_else(String::new, |label| label.value().to_string());
                    let value = match metric_type {
                        MetricType::COUNTER => metric.get_counter().value(),
                        MetricType::GAUGE => metric.get_gauge().value(),
                        other => panic!("unexpected segment-cache metric type {other:?}"),
                    };
                    let key = (family.name().to_string(), dataset);
                    *series_counts.entry(key.clone()).or_insert(0) += 1;
                    values.insert(key, (metric_type, value));
                }
            }

            Self {
                values,
                series_counts,
            }
        }

        fn assert_value(
            &self,
            metric: &str,
            dataset: &str,
            metric_type: MetricType,
            expected: u32,
        ) {
            let key = (metric.to_string(), dataset.to_string());
            let actual = self
                .values
                .get(&key)
                .unwrap_or_else(|| panic!("missing metric {metric} for dataset {dataset}"));
            assert_eq!(actual.0, metric_type, "wrong type for {metric}");
            assert!(
                (actual.1 - f64::from(expected)).abs() < f64::EPSILON,
                "wrong value for {metric}: expected {expected}, got {}",
                actual.1
            );
            assert_eq!(
                self.series_counts.get(&key),
                Some(&1),
                "metric {metric} must have one series for dataset {dataset}"
            );
        }

        fn assert_live_gauges_absent(&self, dataset: &str) {
            assert!(
                self.values.keys().all(|(metric, label)| {
                    label != dataset
                        || matches!(
                            metric.as_str(),
                            "cayenne_segment_cache_accesses" | "cayenne_segment_cache_hits"
                        )
                }),
                "retired dataset {dataset} must not report live cache gauges"
            );
        }
    }

    async fn settle_cache_bookkeeping(cache: &SharedSegmentCache) {
        cache.state.cache.run_pending_tasks().await;
    }

    #[tokio::test]
    async fn put_trims_a_coalesced_slice_so_the_weight_is_the_resident_size() {
        // The reader coalesces adjacent segment requests into one read of up to
        // 16 MiB and hands each segment a slice over that single allocation. A
        // cached slice would keep the whole block alive while weighing only its
        // own length, so `put` must copy.
        const BLOCK_BYTES: usize = 16 * 1024 * 1024;
        const SEGMENT_BYTES: usize = 64 * 1024;

        let block = ByteBuffer::copy_from(vec![7u8; BLOCK_BYTES]);
        let segment = block.slice(0..SEGMENT_BYTES);
        let segment_ptr = segment.as_slice().as_ptr();

        let shared = Arc::new(SharedSegmentCache::new(8 * 1024 * 1024, false));
        let cache = shared.for_path(Path::from("coalesced.vortex"));
        let id = SegmentId::from(1);
        cache
            .put(id, segment.clone())
            .await
            .expect("put should not error");

        let cached = cache
            .get(id)
            .await
            .expect("get should not error")
            .expect("the segment should be cached");
        assert_eq!(cached.len(), SEGMENT_BYTES, "the segment round-trips whole");
        assert_eq!(
            cached.as_slice(),
            segment.as_slice(),
            "trimming must preserve the bytes"
        );
        assert!(
            !std::ptr::eq(cached.as_slice().as_ptr(), segment_ptr),
            "the cached buffer must own its allocation, not alias the 16 MiB block"
        );
        assert_eq!(
            cached.alignment(),
            segment.alignment(),
            "trimming must preserve alignment so decode stays zero-copy"
        );

        shared.run_pending_tasks().await;
        assert_eq!(
            shared.cache.weighted_size(),
            SEGMENT_BYTES as u64,
            "the accounted weight is the segment, and now so is the resident size"
        );
    }

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

    #[test]
    fn metrics_report_initial_zero_state() {
        let harness = MetricsHarness::new();
        let _cache = harness.cache(2_048, "initial_state");

        let samples = harness.gather();
        samples.assert_value(
            "cayenne_segment_cache_accesses",
            "initial_state",
            MetricType::COUNTER,
            0,
        );
        samples.assert_value(
            "cayenne_segment_cache_hits",
            "initial_state",
            MetricType::COUNTER,
            0,
        );
        samples.assert_value(
            "cayenne_segment_cache_weighted_bytes",
            "initial_state",
            MetricType::GAUGE,
            0,
        );
        samples.assert_value(
            "cayenne_segment_cache_capacity_bytes",
            "initial_state",
            MetricType::GAUGE,
            2_048,
        );
        samples.assert_value(
            "cayenne_segment_cache_entries",
            "initial_state",
            MetricType::GAUGE,
            0,
        );
    }

    #[test]
    fn counter_callbacks_cannot_publish_hits_ahead_of_accesses() {
        let counters = SegmentCacheCounters::new("callback_order");
        counters.accesses.store(1, Ordering::Relaxed);
        counters.hits.store(1, Ordering::Relaxed);

        let mut hits = u64::MAX;
        counters.observe_hits(|value| hits = value);
        assert_eq!(hits, 0, "hits wait for a completed access observation");

        let mut accesses = u64::MAX;
        counters.observe_accesses(|value| accesses = value);
        assert_eq!(accesses, 1);

        counters.accesses.store(2, Ordering::Relaxed);
        counters.hits.store(2, Ordering::Relaxed);
        counters.observe_hits(|value| hits = value);
        assert_eq!(hits, 1, "hits use the last completed access snapshot");

        counters.observe_accesses(|value| accesses = value);
        counters.observe_hits(|value| hits = value);
        assert_eq!((accesses, hits), (2, 2));
    }

    #[tokio::test]
    async fn metrics_update_below_sampling_threshold_without_clone_duplicates() {
        let harness = MetricsHarness::new();
        let shared = harness.cache(1_024, "active");
        let cache = shared.for_path(Path::from("active.vortex"));
        let id_one = SegmentId::from(1);
        let id_two = SegmentId::from(2);

        assert!(
            cache
                .get(id_one)
                .await
                .expect("initial cache miss")
                .is_none()
        );
        cache
            .put(id_one, ByteBuffer::from(vec![1_u8, 2, 3, 4]))
            .await
            .expect("insert first segment");
        settle_cache_bookkeeping(&shared).await;
        assert!(
            cache
                .get(id_one)
                .await
                .expect("read first cached segment")
                .is_some()
        );

        let first = harness.gather();
        first.assert_value(
            "cayenne_segment_cache_accesses",
            "active",
            MetricType::COUNTER,
            2,
        );
        first.assert_value(
            "cayenne_segment_cache_hits",
            "active",
            MetricType::COUNTER,
            1,
        );
        first.assert_value(
            "cayenne_segment_cache_weighted_bytes",
            "active",
            MetricType::GAUGE,
            4,
        );
        first.assert_value(
            "cayenne_segment_cache_entries",
            "active",
            MetricType::GAUGE,
            1,
        );

        // A second path wrapper clones the shared cache but must not register a
        // second observation of the same dataset.
        let cache_clone = shared.for_path(Path::from("active.vortex"));
        assert!(
            cache_clone
                .get(id_one)
                .await
                .expect("read through cloned path cache")
                .is_some()
        );
        assert!(
            cache
                .get(id_two)
                .await
                .expect("second cache miss")
                .is_none()
        );
        cache
            .put(id_two, ByteBuffer::from(vec![5_u8, 6, 7]))
            .await
            .expect("insert second segment");
        settle_cache_bookkeeping(&shared).await;

        let second = harness.gather();
        second.assert_value(
            "cayenne_segment_cache_accesses",
            "active",
            MetricType::COUNTER,
            4,
        );
        second.assert_value(
            "cayenne_segment_cache_hits",
            "active",
            MetricType::COUNTER,
            2,
        );
        second.assert_value(
            "cayenne_segment_cache_weighted_bytes",
            "active",
            MetricType::GAUGE,
            7,
        );
        second.assert_value(
            "cayenne_segment_cache_entries",
            "active",
            MetricType::GAUGE,
            2,
        );
    }

    #[tokio::test]
    async fn metrics_keep_datasets_separate() {
        let harness = MetricsHarness::new();
        let alpha = harness.cache(1_024, "alpha");
        let beta = harness.cache(4_096, "beta");
        let alpha_path = alpha.for_path(Path::from("alpha.vortex"));
        let beta_path = beta.for_path(Path::from("beta.vortex"));
        let id = SegmentId::from(1);

        assert!(
            alpha_path
                .get(id)
                .await
                .expect("alpha cache miss")
                .is_none()
        );
        alpha_path
            .put(id, ByteBuffer::from(vec![1_u8; 11]))
            .await
            .expect("insert alpha segment");
        settle_cache_bookkeeping(&alpha).await;
        assert!(alpha_path.get(id).await.expect("alpha cache hit").is_some());
        assert!(beta_path.get(id).await.expect("beta cache miss").is_none());

        let samples = harness.gather();
        samples.assert_value(
            "cayenne_segment_cache_accesses",
            "alpha",
            MetricType::COUNTER,
            2,
        );
        samples.assert_value(
            "cayenne_segment_cache_hits",
            "alpha",
            MetricType::COUNTER,
            1,
        );
        samples.assert_value(
            "cayenne_segment_cache_weighted_bytes",
            "alpha",
            MetricType::GAUGE,
            11,
        );
        samples.assert_value(
            "cayenne_segment_cache_capacity_bytes",
            "alpha",
            MetricType::GAUGE,
            1_024,
        );
        samples.assert_value(
            "cayenne_segment_cache_accesses",
            "beta",
            MetricType::COUNTER,
            1,
        );
        samples.assert_value("cayenne_segment_cache_hits", "beta", MetricType::COUNTER, 0);
        samples.assert_value(
            "cayenne_segment_cache_weighted_bytes",
            "beta",
            MetricType::GAUGE,
            0,
        );
        samples.assert_value(
            "cayenne_segment_cache_capacity_bytes",
            "beta",
            MetricType::GAUGE,
            4_096,
        );
    }

    #[tokio::test]
    async fn metrics_aggregate_same_dataset_caches_without_counter_resets() {
        let harness = MetricsHarness::new();
        let primary = harness.cache(1_024, "shared");
        let cache = primary.for_path(Path::from("primary.vortex"));
        let id = SegmentId::from(1);

        assert!(cache.get(id).await.expect("primary cache miss").is_none());
        cache
            .put(id, ByteBuffer::from(vec![1_u8, 2, 3, 4]))
            .await
            .expect("insert primary segment");
        settle_cache_bookkeeping(&primary).await;
        assert!(cache.get(id).await.expect("primary cache hit").is_some());

        let before = harness.gather();
        before.assert_value(
            "cayenne_segment_cache_accesses",
            "shared",
            MetricType::COUNTER,
            2,
        );
        before.assert_value(
            "cayenne_segment_cache_hits",
            "shared",
            MetricType::COUNTER,
            1,
        );

        let overlapping = harness.cache(4_096, "shared");
        assert!(
            Arc::ptr_eq(&primary.dataset_metrics, &overlapping.dataset_metrics),
            "same-label cache instances must share monotonic counters"
        );
        let during = harness.gather();
        during.assert_value(
            "cayenne_segment_cache_accesses",
            "shared",
            MetricType::COUNTER,
            2,
        );
        during.assert_value(
            "cayenne_segment_cache_hits",
            "shared",
            MetricType::COUNTER,
            1,
        );
        during.assert_value(
            "cayenne_segment_cache_weighted_bytes",
            "shared",
            MetricType::GAUGE,
            4,
        );
        during.assert_value(
            "cayenne_segment_cache_capacity_bytes",
            "shared",
            MetricType::GAUGE,
            5_120,
        );
        during.assert_value(
            "cayenne_segment_cache_entries",
            "shared",
            MetricType::GAUGE,
            1,
        );

        drop(overlapping);
        let after = harness.gather();
        after.assert_value(
            "cayenne_segment_cache_accesses",
            "shared",
            MetricType::COUNTER,
            2,
        );
        after.assert_value(
            "cayenne_segment_cache_hits",
            "shared",
            MetricType::COUNTER,
            1,
        );
        after.assert_value(
            "cayenne_segment_cache_capacity_bytes",
            "shared",
            MetricType::GAUGE,
            1_024,
        );
    }

    #[tokio::test]
    async fn metrics_report_unsettled_moka_estimates_without_flushing() {
        let harness = MetricsHarness::new();
        let shared = harness.cache(1_024, "unsettled");
        let cache = shared.for_path(Path::from("unsettled.vortex"));
        cache
            .put(SegmentId::from(1), ByteBuffer::from(vec![1_u8; 9]))
            .await
            .expect("insert unsettled segment");

        let expected_weighted_bytes = u32::try_from(shared.state.cache.weighted_size())
            .expect("test cache weighted size fits in u32");
        let expected_entries = u32::try_from(shared.state.cache.entry_count())
            .expect("test cache entry count fits in u32");
        let samples = harness.gather();
        samples.assert_value(
            "cayenne_segment_cache_weighted_bytes",
            "unsettled",
            MetricType::GAUGE,
            expected_weighted_bytes,
        );
        samples.assert_value(
            "cayenne_segment_cache_entries",
            "unsettled",
            MetricType::GAUGE,
            expected_entries,
        );
    }

    #[test]
    fn metrics_stop_after_cache_drop() {
        let harness = MetricsHarness::new();
        let shared = harness.cache(1_024, "retired");
        let state = Arc::downgrade(&shared.state);
        let cache = shared.for_path(Path::from("retired.vortex"));

        let present = harness.gather();
        present.assert_value(
            "cayenne_segment_cache_capacity_bytes",
            "retired",
            MetricType::GAUGE,
            1_024,
        );

        drop(cache);
        drop(shared);
        assert!(
            state.upgrade().is_none(),
            "observable callbacks must not keep the cache alive"
        );

        let retired = harness.gather();
        retired.assert_live_gauges_absent("retired");
        retired.assert_value(
            "cayenne_segment_cache_accesses",
            "retired",
            MetricType::COUNTER,
            0,
        );
        retired.assert_value(
            "cayenne_segment_cache_hits",
            "retired",
            MetricType::COUNTER,
            0,
        );
    }

    #[tokio::test]
    async fn counters_remain_monotonic_after_full_dataset_recreation() {
        let harness = MetricsHarness::new();
        let shared = harness.cache(1_024, "recreated");
        let cache = shared.for_path(Path::from("before.vortex"));
        let id = SegmentId::from(1);

        assert!(
            cache
                .get(id)
                .await
                .expect("cache miss before drop")
                .is_none()
        );
        cache
            .put(id, ByteBuffer::from(vec![1_u8; 4]))
            .await
            .expect("insert before drop");
        assert!(
            cache
                .get(id)
                .await
                .expect("cache hit before drop")
                .is_some()
        );

        let before = harness.gather();
        before.assert_value(
            "cayenne_segment_cache_accesses",
            "recreated",
            MetricType::COUNTER,
            2,
        );
        before.assert_value(
            "cayenne_segment_cache_hits",
            "recreated",
            MetricType::COUNTER,
            1,
        );

        drop(cache);
        drop(shared);
        let retired = harness.gather();
        retired.assert_live_gauges_absent("recreated");
        retired.assert_value(
            "cayenne_segment_cache_accesses",
            "recreated",
            MetricType::COUNTER,
            2,
        );

        let recreated = harness.cache(2_048, "recreated");
        let recreated_cache = recreated.for_path(Path::from("after.vortex"));
        let resumed = harness.gather();
        resumed.assert_value(
            "cayenne_segment_cache_accesses",
            "recreated",
            MetricType::COUNTER,
            2,
        );
        resumed.assert_value(
            "cayenne_segment_cache_hits",
            "recreated",
            MetricType::COUNTER,
            1,
        );
        assert!(
            recreated_cache
                .get(SegmentId::from(2))
                .await
                .expect("cache miss after recreation")
                .is_none()
        );
        let after = harness.gather();
        after.assert_value(
            "cayenne_segment_cache_accesses",
            "recreated",
            MetricType::COUNTER,
            3,
        );
        after.assert_value(
            "cayenne_segment_cache_hits",
            "recreated",
            MetricType::COUNTER,
            1,
        );
    }

    #[tokio::test]
    async fn delta_counter_stays_safe_when_another_reader_observes_a_dataset_gap() {
        let harness = DeltaMetricsHarness::new();
        let shared = harness.cache(1_024, "delta_recreated");
        let cache = shared.for_path(Path::from("before.vortex"));
        let id = SegmentId::from(1);

        assert!(
            cache
                .get(id)
                .await
                .expect("cache miss before drop")
                .is_none()
        );
        cache
            .put(id, ByteBuffer::from(vec![1_u8; 4]))
            .await
            .expect("insert before drop");
        assert!(
            cache
                .get(id)
                .await
                .expect("cache hit before drop")
                .is_some()
        );
        harness.gather().assert_value(
            "cayenne_segment_cache_accesses",
            "delta_recreated",
            MetricType::COUNTER,
            2,
        );
        assert_eq!(harness.collect_accesses("delta_recreated"), Some(2));

        drop(cache);
        drop(shared);
        // A callback run by one reader writes observations to every SDK
        // pipeline, so another reader can have one already-buffered live gauge
        // sample. Its first collection drains that sample; the next collection
        // proves the retired cache is no longer contributing observations.
        let buffered = harness.gather();
        buffered.assert_value(
            "cayenne_segment_cache_accesses",
            "delta_recreated",
            MetricType::COUNTER,
            2,
        );
        let retired = harness.gather();
        retired.assert_live_gauges_absent("delta_recreated");
        retired.assert_value(
            "cayenne_segment_cache_accesses",
            "delta_recreated",
            MetricType::COUNTER,
            2,
        );
        assert_eq!(harness.collect_accesses("delta_recreated"), Some(0));

        let recreated = harness.cache(2_048, "delta_recreated");
        let recreated_cache = recreated.for_path(Path::from("after.vortex"));
        assert!(
            recreated_cache
                .get(SegmentId::from(2))
                .await
                .expect("cache miss after recreation")
                .is_none()
        );
        harness.gather().assert_value(
            "cayenne_segment_cache_accesses",
            "delta_recreated",
            MetricType::COUNTER,
            3,
        );
        assert_eq!(
            harness.collect_accesses("delta_recreated"),
            Some(1),
            "one reader's gap collection must not reset the absolute total seen by a delta reader"
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
            shared.state.cache.entry_count(),
            258,
            "retired, live, and more than one maintenance batch of unrelated paths should be resident"
        );

        shared.invalidate_paths(HashSet::from([retired_path])).await;

        assert_eq!(
            shared.state.cache.entry_count(),
            257,
            "invalidation must physically evict only the retired buffers before it returns"
        );
    }

    #[tokio::test]
    async fn path_state_stays_registered_until_the_last_open_file_drops() {
        let shared = SharedSegmentCache::new(1 << 20, Some(Arc::from("test")), true);
        let path = Path::from("snapshot/shared.vortex");
        let first = shared.for_path(path.clone());
        let second = shared.for_path(path.clone());
        let states = shared
            .path_states
            .as_ref()
            .expect("retirement tracking enabled");

        assert_eq!(
            states
                .lock()
                .get(&path)
                .map_or(0, std::sync::Weak::strong_count),
            2,
            "both file-cache handles must share one registered path state"
        );
        drop(first);
        assert_eq!(
            states
                .lock()
                .get(&path)
                .map_or(0, std::sync::Weak::strong_count),
            1,
            "dropping one opener must not unregister the state used by the other"
        );

        shared.invalidate_paths(HashSet::from([path.clone()])).await;
        second
            .put(SegmentId::from(1), ByteBuffer::from(vec![1u8, 2, 3, 4]))
            .await
            .expect("a late put on the still-registered retired state is ignored");
        assert!(
            second
                .get(SegmentId::from(1))
                .await
                .expect("get after retirement")
                .is_none(),
            "the surviving opener must observe retirement"
        );

        drop(second);
        assert!(
            !states.lock().contains_key(&path),
            "the registry entry is removed when the last opener drops"
        );
    }
}
