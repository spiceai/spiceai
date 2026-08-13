// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Weak;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use moka::future::Cache;
use object_store::path::Path;
use opentelemetry::metrics::{Meter, ObservableCounter, ObservableGauge};
use opentelemetry::{KeyValue, global};
use parking_lot::{Mutex, RwLock};
use twox_hash::XxHash3_64;
use vortex::buffer::ByteBuffer;
use vortex::error::VortexResult;
use vortex::layout::segments::{SegmentCache, SegmentId};

/// Hasher for the segment cache key `(Path, SegmentId)`. XXH3 matches the
/// project-wide cache hashing default and is markedly faster than moka's default
/// `SipHash` on the per-segment hot path.
type SegmentCacheHasher = BuildHasherDefault<XxHash3_64>;

/// The process-lifetime observable instruments and the cache instances they
/// inspect during metrics collection.
///
/// OpenTelemetry 0.32 retains observable callbacks for the meter provider's
/// lifetime and does not unregister them when an instrument handle is dropped.
/// A single callback set with weak cache references therefore avoids both
/// per-table callback accumulation and keeping retired tables alive.
struct SegmentCacheMetrics {
    datasets: Arc<RwLock<Vec<Weak<SegmentCacheDatasetMetrics>>>>,
    // Reader pipelines collect independently but observable callbacks write to
    // all of them. Retaining only these small counter cells prevents an idle
    // collection by one reader from resetting the absolute value still tracked
    // by a Delta reader. Live cache state remains weak and is pruned separately.
    counters: Arc<RwLock<HashMap<Arc<str>, Arc<SegmentCacheCounters>>>>,
    _accesses: ObservableCounter<u64>,
    _hits: ObservableCounter<u64>,
    _weighted_bytes: ObservableGauge<u64>,
    _capacity_bytes: ObservableGauge<u64>,
    _entries: ObservableGauge<u64>,
}

impl SegmentCacheMetrics {
    fn new(meter: &Meter) -> Self {
        let datasets = Arc::new(RwLock::new(Vec::<Weak<SegmentCacheDatasetMetrics>>::new()));
        let counters = Arc::new(RwLock::new(HashMap::new()));

        let accesses = {
            let counters = Arc::clone(&counters);
            meter
                .u64_observable_counter("cayenne_segment_cache_accesses")
                .with_description("Cumulative Vortex segment cache get() calls.")
                .with_callback(move |observer| {
                    observe_counters(&counters, |dataset| {
                        dataset.observe_accesses(|value| {
                            observer.observe(value, &dataset.dataset_label);
                        });
                    });
                })
                .build()
        };
        let hits = {
            let counters = Arc::clone(&counters);
            meter
                .u64_observable_counter("cayenne_segment_cache_hits")
                .with_description("Cumulative Vortex segment cache hits.")
                .with_callback(move |observer| {
                    observe_counters(&counters, |dataset| {
                        dataset.observe_hits(|value| {
                            observer.observe(value, &dataset.dataset_label);
                        });
                    });
                })
                .build()
        };
        let weighted_bytes = {
            let datasets = Arc::clone(&datasets);
            meter
                .u64_observable_gauge("cayenne_segment_cache_weighted_bytes")
                .with_description("Combined approximate live Vortex segment cache size in bytes.")
                .with_unit("By")
                .with_callback(move |observer| {
                    observe_datasets(&datasets, |dataset| {
                        observer.observe(
                            dataset.total_live_cache_metric(|cache| cache.cache.weighted_size()),
                            &dataset.counters.dataset_label,
                        );
                    });
                })
                .build()
        };
        let capacity_bytes = {
            let datasets = Arc::clone(&datasets);
            meter
                .u64_observable_gauge("cayenne_segment_cache_capacity_bytes")
                .with_description("Combined configured Vortex segment cache capacity in bytes.")
                .with_unit("By")
                .with_callback(move |observer| {
                    observe_datasets(&datasets, |dataset| {
                        observer.observe(
                            dataset.total_live_cache_metric(|cache| cache.capacity_bytes),
                            &dataset.counters.dataset_label,
                        );
                    });
                })
                .build()
        };
        let entries = {
            let datasets = Arc::clone(&datasets);
            meter
                .u64_observable_gauge("cayenne_segment_cache_entries")
                .with_description("Combined approximate live Vortex segment cache entry count.")
                .with_callback(move |observer| {
                    observe_datasets(&datasets, |dataset| {
                        observer.observe(
                            dataset.total_live_cache_metric(|cache| cache.cache.entry_count()),
                            &dataset.counters.dataset_label,
                        );
                    });
                })
                .build()
        };

        Self {
            datasets,
            counters,
            _accesses: accesses,
            _hits: hits,
            _weighted_bytes: weighted_bytes,
            _capacity_bytes: capacity_bytes,
            _entries: entries,
        }
    }

    fn register(
        &self,
        dataset: Arc<str>,
        cache: &Arc<SegmentCacheState>,
    ) -> Arc<SegmentCacheDatasetMetrics> {
        let mut datasets = self.datasets.write();
        datasets.retain(|dataset| dataset.strong_count() > 0);

        if let Some(existing) = datasets
            .iter()
            .filter_map(Weak::upgrade)
            .find(|existing| existing.dataset == dataset)
        {
            {
                let mut caches = existing.caches.write();
                caches.retain(|cache| cache.strong_count() > 0);
                caches.push(Arc::downgrade(cache));
            }
            return existing;
        }

        let counters = Arc::clone(
            self.counters
                .write()
                .entry(Arc::clone(&dataset))
                .or_insert_with(|| Arc::new(SegmentCacheCounters::new(&dataset))),
        );
        let metrics = Arc::new(SegmentCacheDatasetMetrics {
            dataset,
            counters,
            caches: RwLock::new(vec![Arc::downgrade(cache)]),
        });
        datasets.push(Arc::downgrade(&metrics));
        metrics
    }
}

fn live_datasets(
    datasets: &RwLock<Vec<Weak<SegmentCacheDatasetMetrics>>>,
) -> Vec<Arc<SegmentCacheDatasetMetrics>> {
    let (live, had_retired) = {
        let datasets = datasets.read();
        let live: Vec<_> = datasets.iter().filter_map(Weak::upgrade).collect();
        let had_retired = live.len() != datasets.len();
        (live, had_retired)
    };
    if had_retired {
        datasets
            .write()
            .retain(|dataset| dataset.strong_count() > 0);
    }
    live
}

fn observe_counters(
    counters: &RwLock<HashMap<Arc<str>, Arc<SegmentCacheCounters>>>,
    mut observe: impl FnMut(&SegmentCacheCounters),
) {
    for dataset in counters.read().values() {
        observe(dataset);
    }
}

fn observe_datasets(
    datasets: &RwLock<Vec<Weak<SegmentCacheDatasetMetrics>>>,
    mut observe: impl FnMut(&SegmentCacheDatasetMetrics),
) {
    let live = live_datasets(datasets);
    for dataset in live {
        observe(&dataset);
    }
}

static SEGMENT_CACHE_METRICS: LazyLock<SegmentCacheMetrics> =
    LazyLock::new(|| SegmentCacheMetrics::new(&global::meter("cayenne_segment_cache")));

#[derive(Debug)]
struct SegmentCacheState {
    cache: Cache<(Arc<Path>, SegmentId), ByteBuffer, SegmentCacheHasher>,
    capacity_bytes: u64,
}

#[derive(Debug)]
struct SegmentCacheDatasetMetrics {
    // Cache instances with the same dataset share these counters so observable
    // instruments emit exactly one monotonic series per label set. This occurs
    // when multiple read-capable Vortex formats for a table overlap.
    dataset: Arc<str>,
    counters: Arc<SegmentCacheCounters>,
    caches: RwLock<Vec<Weak<SegmentCacheState>>>,
}

#[derive(Debug)]
struct SegmentCacheCounters {
    dataset_label: [KeyValue; 1],
    accesses: AtomicU64,
    hits: AtomicU64,
    // Observable callbacks can execute independently and readers can collect
    // concurrently. Serializing only collection publishes a hit total against
    // an access total that every reader has already observed, while keeping the
    // cache read path lock-free.
    last_observed_accesses: Mutex<u64>,
}

impl SegmentCacheCounters {
    fn new(dataset: &str) -> Self {
        Self {
            dataset_label: [KeyValue::new("dataset", dataset.to_string())],
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
}

impl SegmentCacheDatasetMetrics {
    fn total_live_cache_metric(&self, value: impl Fn(&SegmentCacheState) -> u64) -> u64 {
        let (live, had_retired) = {
            let caches = self.caches.read();
            let live: Vec<_> = caches.iter().filter_map(Weak::upgrade).collect();
            let had_retired = live.len() != caches.len();
            (live, had_retired)
        };
        if had_retired {
            self.caches.write().retain(|cache| cache.strong_count() > 0);
        }
        live.iter()
            .fold(0, |total, cache| total.saturating_add(value(cache)))
    }
}

/// Shared segment cache keyed by file path and Vortex segment id.
///
/// Vortex segment ids are local to each file. This wrapper keeps a single
/// bounded cache across files while presenting each open file with the
/// `SegmentCache` interface it expects.
#[derive(Clone, Debug)]
pub(crate) struct SharedSegmentCache {
    state: Arc<SegmentCacheState>,
    dataset_metrics: Arc<SegmentCacheDatasetMetrics>,
}

impl SharedSegmentCache {
    pub(crate) fn new(max_capacity_bytes: u64, dataset: Option<Arc<str>>) -> Self {
        Self::new_registered(max_capacity_bytes, dataset, &SEGMENT_CACHE_METRICS)
    }

    fn new_registered(
        max_capacity_bytes: u64,
        dataset: Option<Arc<str>>,
        metrics: &SegmentCacheMetrics,
    ) -> Self {
        let dataset = dataset.unwrap_or_else(|| Arc::from("unknown"));
        let state = Arc::new(SegmentCacheState {
            cache: Cache::builder()
                .name("vortex-datafusion-segment-cache")
                .max_capacity(max_capacity_bytes)
                .weigher(|_, buffer: &ByteBuffer| {
                    u32::try_from(buffer.len().min(u32::MAX as usize)).unwrap_or(u32::MAX)
                })
                .build_with_hasher(SegmentCacheHasher::default()),
            capacity_bytes: max_capacity_bytes,
        });
        let dataset_metrics = metrics.register(dataset, &state);
        Self {
            state,
            dataset_metrics,
        }
    }

    pub(crate) fn for_path(&self, path: Path) -> Arc<dyn SegmentCache> {
        Arc::new(PathSegmentCache {
            shared: self.clone(),
            path: Arc::new(path),
        })
    }
}

struct PathSegmentCache {
    shared: SharedSegmentCache,
    // `Arc<Path>` so forming the `(path, segment)` cache key on every `get`/`put`
    // is a refcount bump, not a `Path` (string) clone — segment reads are hot.
    path: Arc<Path>,
}

#[async_trait]
impl SegmentCache for PathSegmentCache {
    async fn get(&self, id: SegmentId) -> VortexResult<Option<ByteBuffer>> {
        let result = self
            .shared
            .state
            .cache
            .get(&(Arc::clone(&self.path), id))
            .await;

        // Collection reads these atomics directly, so the hot path never
        // allocates labels or synchronously records metrics.
        self.shared
            .dataset_metrics
            .counters
            .accesses
            .fetch_add(1, Ordering::Relaxed);
        if result.is_some() {
            self.shared
                .dataset_metrics
                .counters
                .hits
                .fetch_add(1, Ordering::Relaxed);
        }

        Ok(result)
    }

    async fn put(&self, id: SegmentId, buffer: ByteBuffer) -> VortexResult<()> {
        self.shared
            .state
            .cache
            .insert((Arc::clone(&self.path), id), buffer)
            .await;
        Ok(())
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
    async fn get_put_roundtrip_and_path_isolation() {
        let shared = SharedSegmentCache::new(1 << 20, None);
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
}
