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

use std::sync::LazyLock;

use opentelemetry::{
    KeyValue, global,
    metrics::{Counter, Gauge, Meter, ObservableGauge},
};

use crate::result::{
    embeddings::CachedEmbeddingResult, query::CachedQueryResult, search::CachedSearchResult,
};

/// Why an entry left the cache, reported as the `reason` attribute on
/// `*_cache_evictions`.
///
/// The attribute is what lets the counter carry invalidation at all. On an
/// accelerated dataset with a periodic refresh, [`EvictionReason::Invalidated`]
/// is the dominant — often the only — cause, so folding it into an unlabelled
/// total would swamp the size and expiry counts that indicate actual cache
/// pressure. Split by reason, each stays readable on its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionReason {
    /// The cache exceeded `max_size` and reclaimed an entry.
    Size,
    /// The entry outlived `item_ttl`.
    Expired,
    /// A refresh or a DML write dropped the entries referencing a table.
    Invalidated,
}

impl EvictionReason {
    /// Every variant, so a caller publishing the series up front cannot omit one.
    pub const ALL: [Self; 3] = [Self::Size, Self::Expired, Self::Invalidated];

    fn as_str(self) -> &'static str {
        match self {
            Self::Size => "size",
            Self::Expired => "expired",
            Self::Invalidated => "invalidated",
        }
    }

    fn key_value(self) -> KeyValue {
        KeyValue::new("reason", self.as_str())
    }
}

/// Instruments for the process-wide Arrow schema pool.
///
/// Interned schemas are shared by every entry of the same shape, so they are
/// not charged to any one entry's weight — see
/// [`crate::result::query::CachedQueryResult::memory_size`]. This is where that
/// memory is reported instead: once per distinct schema, rather than once per
/// entry holding it.
///
/// The callbacks only read: `stats()` does not sweep, so collecting these
/// gauges never changes what the pool holds. Reclamation is driven separately
/// by the runtime's cache-maintenance loop, which runs whether or not metrics
/// are enabled — tying it to collection would have made the pool's memory
/// depend on `--metrics`, which is off by default.
pub struct SchemaInternerMetrics {
    _rows: ObservableGauge<u64>,
    _schema_bytes: ObservableGauge<u64>,
    _self_bytes: ObservableGauge<u64>,
}

static SCHEMA_INTERNER_METRICS: LazyLock<SchemaInternerMetrics> = LazyLock::new(|| {
    let meter = global::meter("schema_interner");
    SchemaInternerMetrics {
        _rows: meter
            .u64_observable_gauge("schema_interner_schemas")
            .with_description("Distinct Arrow schemas currently shared by the interner.")
            .with_callback(|observer| {
                let stats = arrow_tools::schema_intern::global().stats();
                observer.observe(stats.rows as u64, &[]);
            })
            .build(),
        _schema_bytes: meter
            .u64_observable_gauge("schema_interner_schema_bytes")
            .with_description(
                "Total size of the Arrow schemas the interner shares. Counted once per distinct schema, not once per cache entry holding it.",
            )
            .with_unit("By")
            .with_callback(|observer| {
                observer.observe(
                    arrow_tools::schema_intern::global().stats().schema_bytes as u64,
                    &[],
                );
            })
            .build(),
        _self_bytes: meter
            .u64_observable_gauge("schema_interner_overhead_bytes")
            .with_description("The interner's own bookkeeping: its hash-map slots and bucket vectors.")
            .with_unit("By")
            .with_callback(|observer| {
                observer.observe(
                    arrow_tools::schema_intern::global().stats().self_bytes as u64,
                    &[],
                );
            })
            .build(),
    }
});

/// Registers the schema-pool gauges. Idempotent.
///
/// Must run after the meter provider is installed, like the cache instruments.
pub fn init_schema_interner_metrics() {
    LazyLock::force(&SCHEMA_INTERNER_METRICS);
}

macro_rules! generate_cache_metrics {
    ($prefix:literal, $name:ident) => {
        pub mod $name {
            use super::*;

            static METER: LazyLock<Meter> =
                LazyLock::new(|| global::meter(concat!($prefix, "_cache")));

            pub static SIZE_BYTES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
                METER
                    .u64_gauge(concat!($prefix, "_cache_size_bytes"))
                    .with_description("Size of the cache in bytes.")
                    .with_unit("By")
                    .build()
            });

            pub static MAX_SIZE_BYTES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
                METER
                    .u64_gauge(concat!($prefix, "_cache_max_size_bytes"))
                    .with_description("Maximum allowed size of the cache in bytes.")
                    .with_unit("By")
                    .build()
            });

            pub static REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
                METER
                    .u64_counter(concat!($prefix, "_cache_requests"))
                    .with_description("Number of requests to get a key from the cache.")
                    .build()
            });

            pub static HITS: LazyLock<Counter<u64>> = LazyLock::new(|| {
                METER
                    .u64_counter(concat!($prefix, "_cache_hits"))
                    .with_description("Cache hit count.")
                    .build()
            });

            pub static MISSES: LazyLock<Counter<u64>> = LazyLock::new(|| {
                METER
                    .u64_counter(concat!($prefix, "_cache_misses"))
                    .with_description("Cache miss count.")
                    .build()
            });

            pub static HIT_RATIO: LazyLock<Gauge<f64>> = LazyLock::new(|| {
                METER
                    .f64_gauge(concat!($prefix, "_cache_hit_ratio"))
                    .with_description("Cache hit ratio (hits / total requests).")
                    .build()
            });

            pub static ITEMS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
                METER
                    .u64_gauge(concat!($prefix, "_cache_items_count"))
                    .with_description("Number of items currently in the cache.")
                    .build()
            });

            pub static EVICTIONS: LazyLock<Counter<u64>> = LazyLock::new(|| {
                METER
                    .u64_counter(concat!($prefix, "_cache_evictions"))
                    .with_description(
                        "Number of entries evicted because they expired or the cache was over capacity. Entries dropped by table invalidation are counted by the invalidations metric instead.",
                    )
                    .build()
            });

            pub static STALE_REJECTIONS: LazyLock<Counter<u64>> = LazyLock::new(|| {
                METER
                    .u64_counter(concat!($prefix, "_cache_stale_rejections"))
                    .with_description(
                        "Number of lookups that found an entry but refused to serve it because a table it read had since been invalidated. These are also counted as misses.",
                    )
                    .build()
            });

            pub static STALE_WHILE_REVALIDATE_SKIPPED: LazyLock<Counter<u64>> =
                LazyLock::new(|| {
                    METER
                        .u64_counter(concat!(
                            $prefix,
                            "_cache_stale_swr_count"
                        ))
                        .with_description(
                            "Number of stale-while-revalidate background refreshes skipped due to existing in-flight revalidation.",
                        )
                        .build()
                });

            pub static STALE_WHILE_REVALIDATE_BACKGROUND_QUERIES: LazyLock<Counter<u64>> =
                LazyLock::new(|| {
                    METER
                        .u64_counter(concat!(
                            $prefix,
                            "_cache_swr_background_query_count"
                        ))
                        .with_description(
                            "Number of background queries triggered for stale-while-revalidate cache refreshes.",
                        )
                        .build()
                });

            /// Publishes every counter in this module at zero.
            ///
            /// A `LazyLock` counter is only built — and so only registered with
            /// the meter — when something first derefs it, so a counter that has
            /// not yet fired exports no series at all, rather than a zero. An
            /// absent series is indistinguishable from a broken exporter or a bad
            /// scrape config (#12687). Adding zero builds the instrument and emits
            /// the series.
            ///
            /// Living in the macro keeps this list from drifting away from the
            /// counters declared beside it.
            pub fn publish_counters_at_zero() {
                REQUESTS.add(0, &[]);
                HITS.add(0, &[]);
                MISSES.add(0, &[]);
                STALE_REJECTIONS.add(0, &[]);
                STALE_WHILE_REVALIDATE_SKIPPED.add(0, &[]);
                STALE_WHILE_REVALIDATE_BACKGROUND_QUERIES.add(0, &[]);
                for reason in EvictionReason::ALL {
                    EVICTIONS.add(0, &[reason.key_value()]);
                }
            }
        }
    };
}

generate_cache_metrics!("results", sql_results); // TODO: update the prefix to `sql_results` in v2.0 - https://github.com/spiceai/spiceai/issues/6128
generate_cache_metrics!("search_results", search_results);
generate_cache_metrics!("embeddings", embeddings);

pub trait CacheMetrics: Send + Sync {
    fn init()
    where
        Self: Sized,
    {
        Self::record_item_count(0);
        Self::record_size(0);
        Self::record_max_size(0);
        Self::update_hit_ratio(0, 0);
        Self::publish_counters_at_zero();
    }

    fn record_hit()
    where
        Self: Sized;
    fn record_miss()
    where
        Self: Sized;
    fn record_request()
    where
        Self: Sized;
    fn record_item_count(count: u64)
    where
        Self: Sized;
    fn record_size(size: u64)
    where
        Self: Sized;
    fn record_max_size(size: u64)
    where
        Self: Sized;
    fn record_eviction(reason: EvictionReason)
    where
        Self: Sized;
    /// Records a lookup that found an entry but could not serve it because a
    /// table it read had since been invalidated.
    fn record_stale_rejection()
    where
        Self: Sized;
    fn update_hit_ratio(hits: u64, total: u64)
    where
        Self: Sized;
    /// Emits every counter this cache owns at zero, so a runtime on which one has
    /// never fired still exports its series rather than nothing at all. Each
    /// implementation forwards to the like-named function in its metrics module,
    /// which is generated alongside the counters it publishes.
    fn publish_counters_at_zero()
    where
        Self: Sized;
}

#[expect(clippy::cast_precision_loss)]
fn calculate_hit_ratio(hits: u64, total: u64) -> f64 {
    if total > 0 {
        hits as f64 / total as f64
    } else {
        0.0
    }
}

impl CacheMetrics for CachedSearchResult {
    fn record_hit() {
        search_results::HITS.add(1, &[]);
    }

    fn record_miss() {
        search_results::MISSES.add(1, &[]);
    }

    fn record_request() {
        search_results::REQUESTS.add(1, &[]);
    }

    fn record_item_count(count: u64) {
        search_results::ITEMS.record(count, &[]);
    }

    fn record_size(size: u64) {
        search_results::SIZE_BYTES.record(size, &[]);
    }

    fn record_max_size(size: u64) {
        search_results::MAX_SIZE_BYTES.record(size, &[]);
    }

    fn record_eviction(reason: EvictionReason) {
        search_results::EVICTIONS.add(1, &[reason.key_value()]);
    }

    fn record_stale_rejection() {
        search_results::STALE_REJECTIONS.add(1, &[]);
    }

    fn update_hit_ratio(hits: u64, total: u64) {
        let ratio = calculate_hit_ratio(hits, total);
        search_results::HIT_RATIO.record(ratio, &[]);
    }

    fn publish_counters_at_zero() {
        search_results::publish_counters_at_zero();
    }
}

impl CacheMetrics for CachedQueryResult {
    fn record_hit() {
        sql_results::HITS.add(1, &[]);
    }

    fn record_miss() {
        sql_results::MISSES.add(1, &[]);
    }

    fn record_request() {
        sql_results::REQUESTS.add(1, &[]);
    }

    fn record_item_count(count: u64) {
        sql_results::ITEMS.record(count, &[]);
    }

    fn record_size(size: u64) {
        sql_results::SIZE_BYTES.record(size, &[]);
    }

    fn record_max_size(size: u64) {
        sql_results::MAX_SIZE_BYTES.record(size, &[]);
    }

    fn record_eviction(reason: EvictionReason) {
        sql_results::EVICTIONS.add(1, &[reason.key_value()]);
    }

    fn record_stale_rejection() {
        sql_results::STALE_REJECTIONS.add(1, &[]);
    }

    fn update_hit_ratio(hits: u64, total: u64) {
        let ratio = calculate_hit_ratio(hits, total);
        sql_results::HIT_RATIO.record(ratio, &[]);
    }

    fn publish_counters_at_zero() {
        sql_results::publish_counters_at_zero();
    }
}

impl CacheMetrics for CachedEmbeddingResult {
    fn record_hit() {
        embeddings::HITS.add(1, &[]);
    }

    fn record_miss() {
        embeddings::MISSES.add(1, &[]);
    }

    fn record_request() {
        embeddings::REQUESTS.add(1, &[]);
    }

    fn record_item_count(count: u64) {
        embeddings::ITEMS.record(count, &[]);
    }

    fn record_size(size: u64) {
        embeddings::SIZE_BYTES.record(size, &[]);
    }

    fn record_max_size(size: u64) {
        embeddings::MAX_SIZE_BYTES.record(size, &[]);
    }

    fn record_eviction(reason: EvictionReason) {
        embeddings::EVICTIONS.add(1, &[reason.key_value()]);
    }

    fn record_stale_rejection() {
        embeddings::STALE_REJECTIONS.add(1, &[]);
    }

    fn update_hit_ratio(hits: u64, total: u64) {
        let ratio = calculate_hit_ratio(hits, total);
        embeddings::HIT_RATIO.record(ratio, &[]);
    }

    fn publish_counters_at_zero() {
        embeddings::publish_counters_at_zero();
    }
}
