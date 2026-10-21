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

use parking_lot::Mutex;
use std::sync::LazyLock;

use opentelemetry::{
    KeyValue, global,
    metrics::{Counter, Gauge, Meter, ObservableCounter, ObservableGauge},
};

use crate::result::{
    embeddings::CachedEmbeddingResult, query::CachedQueryResult, search::CachedSearchResult,
};

/// Why an entry left the cache, reported as the `reason` attribute on
/// `*_cache_evictions`.
///
/// The attribute keeps invalidation apart from cache pressure. On a dataset with
/// frequent client writes, [`EvictionReason::Invalidated`] can dominate, so
/// folding it into an unlabelled total would swamp the size and expiry counts
/// that indicate actual cache pressure. Split by reason, each stays readable on
/// its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionReason {
    /// The cache exceeded `max_size` and reclaimed an entry.
    Size,
    /// The entry outlived `item_ttl`.
    Expired,
    /// A client write, dataset reload or unload dropped the entries that read a
    /// table. Counted per entry dropped.
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

/// Instruments for one process-wide interner pool.
///
/// Interned values are shared by every entry of the same shape, so they are not
/// charged to any one entry's weight — see
/// [`crate::result::query::CachedQueryResult::memory_size`]. This is where that
/// memory is reported instead: once per distinct value, rather than once per
/// entry holding it.
///
/// The callbacks only read: `stats()` does not sweep, so collecting these
/// gauges never changes what a pool holds. Reclamation is driven separately by
/// the runtime's cache-maintenance loop, which runs whether or not metrics are
/// enabled — tying it to collection would have made a pool's memory depend on
/// `--metrics`, which is off by default.
struct InternerMetrics {
    _rows: ObservableGauge<u64>,
    _value_bytes: ObservableGauge<u64>,
    _self_bytes: ObservableGauge<u64>,
    _collapsed: ObservableCounter<u64>,
    _already_shared: ObservableCounter<u64>,
    _misses: ObservableCounter<u64>,
}

/// How a pool reports itself. One set per pool, so the instruments below are
/// built once and read the right globals.
///
/// One accessor, not four: `stats()` already carries the counters, and taking
/// them from the same snapshot is what makes a scrape internally consistent.
struct InternerSource {
    /// Instrument-name prefix, e.g. `schema_interner`.
    prefix: &'static str,
    /// What the pool holds, for the descriptions: e.g. "Arrow schemas".
    noun: &'static str,
    stats: fn() -> crate::intern::InternerStats,
}

/// The last snapshot taken of a pool, and when.
///
/// `stats()` takes all 16 shard mutexes and walks every row and bucket — the
/// same mutexes the cache write path contends for. Six instruments read from
/// one pool, so a scrape that called it per instrument would do that six times
/// over and could still report six mutually inconsistent numbers. Prometheus
/// pulls, so the rate is whatever the scraper chooses. One snapshot per pool
/// per scrape window serves every instrument from the same walk.
struct StatsCache {
    taken_at: std::time::Instant,
    stats: crate::intern::InternerStats,
}

/// How long a snapshot serves. Shorter than any sane scrape interval, so a
/// scrape still sees fresh numbers, but long enough that the six instruments of
/// one scrape share a single walk.
const STATS_FRESH_FOR: std::time::Duration = std::time::Duration::from_millis(250);

impl InternerSource {
    /// The pool's stats, recomputed only when the last snapshot has aged out.
    fn snapshot(&'static self, cell: &Mutex<Option<StatsCache>>) -> crate::intern::InternerStats {
        let mut held = cell.lock();
        if let Some(cached) = held.as_ref()
            && cached.taken_at.elapsed() < STATS_FRESH_FOR
        {
            return cached.stats;
        }
        let stats = (self.stats)();
        *held = Some(StatsCache {
            taken_at: std::time::Instant::now(),
            stats,
        });
        stats
    }
}

fn build_interner_metrics(
    source: &'static InternerSource,
    snapshot: &'static Mutex<Option<StatsCache>>,
) -> InternerMetrics {
    let meter = global::meter(source.prefix);
    let (noun, prefix) = (source.noun, source.prefix);
    InternerMetrics {
        _rows: meter
            .u64_observable_gauge(format!("{prefix}_rows"))
            .with_description(format!("Distinct {noun} currently shared by the interner."))
            .with_callback(move |observer| {
                observer.observe(source.snapshot(snapshot).rows as u64, &[]);
            })
            .build(),
        _value_bytes: meter
            .u64_observable_gauge(format!("{prefix}_value_bytes"))
            .with_description(format!(
                "Total size of the {noun} the interner shares. Counted once per distinct value, not once per cache entry holding it."
            ))
            .with_unit("By")
            .with_callback(move |observer| {
                observer.observe(source.snapshot(snapshot).value_bytes as u64, &[]);
            })
            .build(),
        _self_bytes: meter
            .u64_observable_gauge(format!("{prefix}_overhead_bytes"))
            .with_description(
                "The interner's own bookkeeping: its hash-map slots and bucket vectors.",
            )
            .with_unit("By")
            .with_callback(move |observer| {
                observer.observe(source.snapshot(snapshot).self_bytes as u64, &[]);
            })
            .build(),
        // Cumulative, so counters rather than gauges — and read through the
        // dedicated accessors, which load one atomic instead of walking every
        // shard the way `stats()` does.
        _collapsed: meter
            .u64_observable_counter(format!("{prefix}_collapsed"))
            .with_description(format!(
                "Distinct {noun} allocations collapsed onto a shared one. The direct evidence that interning is removing duplicates."
            ))
            .with_callback(move |observer| {
                observer.observe(source.snapshot(snapshot).collapsed, &[]);
            })
            .build(),
        _already_shared: meter
            .u64_observable_counter(format!("{prefix}_already_shared"))
            .with_description(
                "Interns whose caller already held the shared allocation. Counted apart from collapsed values because no duplicate was removed.",
            )
            .with_callback(move |observer| {
                observer.observe(source.snapshot(snapshot).already_shared, &[]);
            })
            .build(),
        _misses: meter
            .u64_observable_counter(format!("{prefix}_misses"))
            .with_description(format!(
                "Interns that adopted {noun} the pool had not seen. Misses without collapses means values are arriving distinct and nothing is being shared."
            ))
            .with_callback(move |observer| {
                observer.observe(source.snapshot(snapshot).misses, &[]);
            })
            .build(),
    }
}

static SCHEMA_POOL: InternerSource = InternerSource {
    prefix: "schema_interner",
    noun: "Arrow schemas",
    stats: || crate::intern::schema::global().stats(),
};
static SCHEMA_POOL_SNAPSHOT: Mutex<Option<StatsCache>> = Mutex::new(None);

static TABLE_SET_POOL: InternerSource = InternerSource {
    prefix: "table_set_interner",
    noun: "input-table sets",
    stats: || crate::intern::table_set::global().stats(),
};
static TABLE_SET_POOL_SNAPSHOT: Mutex<Option<StatsCache>> = Mutex::new(None);

static INTERNER_METRICS: LazyLock<Vec<InternerMetrics>> = LazyLock::new(|| {
    vec![
        build_interner_metrics(&SCHEMA_POOL, &SCHEMA_POOL_SNAPSHOT),
        build_interner_metrics(&TABLE_SET_POOL, &TABLE_SET_POOL_SNAPSHOT),
    ]
});

/// Registers the interner-pool gauges. Idempotent.
///
/// Must run after the meter provider is installed, like the cache instruments.
pub fn init_interner_metrics() {
    LazyLock::force(&INTERNER_METRICS);
}

/// Hard table invalidations from client writes, dataset reload and unload,
/// reported as the `mode` attribute on `*_cache_table_invalidations`.
///
/// Every table invalidation evicts dependent entries and reports `evict`.
/// SQL results do not record table invalidations for refresh, CDC or retention;
/// those operations leave SQL results to expire by age.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidationMode {
    /// The entries that read the table were dropped from the cache.
    Evict,
}

impl InvalidationMode {
    /// Every variant, so a caller publishing the series up front cannot omit one.
    pub const ALL: [Self; 1] = [Self::Evict];

    fn as_str(self) -> &'static str {
        match self {
            Self::Evict => "evict",
        }
    }

    fn key_value(self) -> KeyValue {
        KeyValue::new("mode", self.as_str())
    }
}

/// Why a lookup found an entry but would not serve it, reported as the `reason`
/// attribute on `*_cache_stale_rejections`.
///
/// An entry that is simply too old is not a rejection: the lookup counts it as an
/// ordinary miss.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StaleRejectionReason {
    /// A client write, dataset reload or unload changed a table the entry read,
    /// at or after the query that produced it started. The entry may hold rows
    /// from before the change, so it is never served.
    WriteDuringRead,
}

impl StaleRejectionReason {
    /// Every rejection reason, for seeding the metric series.
    pub const ALL: [Self; 1] = [Self::WriteDuringRead];

    fn as_str(self) -> &'static str {
        match self {
            Self::WriteDuringRead => "write_during_read",
        }
    }

    fn key_value(self) -> KeyValue {
        KeyValue::new("reason", self.as_str())
    }
}

/// How a background stale-while-revalidate revalidation ended, reported as the
/// `outcome` attribute on `*_cache_swr_revalidations`.
///
/// Every non-[`RevalidationOutcome::Stored`] outcome leaves the entry it was
/// meant to replace in place, to be served stale until it expires. That is the
/// failure mode with no other symptom — the queries keep succeeding — so it is
/// only visible if it is counted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RevalidationOutcome {
    /// The fresh result replaced the entry.
    Stored,
    /// The revalidation query itself failed to execute.
    QueryFailed,
    /// The query ran but its result stream failed part-way through.
    CollectFailed,
    /// A table the revalidation read was invalidated while it ran, and no stale
    /// window could serve the result, so it was discarded.
    InvalidatedMidFlight,
    /// The origin answered with a transient HTTP error (5xx/429), either as
    /// rows carrying the status or as the failure that ended the revalidation,
    /// so the previous entry was preserved rather than overwritten.
    TransientErrors,
    /// The result held a column no copy can decouple from the memory its
    /// producer owns, so an entry over it could not be bounded by `max_size`.
    Unboundable,
    /// The result could not be encoded for storage.
    EncodeFailed,
    /// The cache write itself failed.
    PutFailed,
}

impl RevalidationOutcome {
    /// Every variant, so a caller publishing the series up front cannot omit one.
    pub const ALL: [Self; 8] = [
        Self::Stored,
        Self::QueryFailed,
        Self::CollectFailed,
        Self::InvalidatedMidFlight,
        Self::TransientErrors,
        Self::Unboundable,
        Self::EncodeFailed,
        Self::PutFailed,
    ];

    fn as_str(self) -> &'static str {
        match self {
            Self::Stored => "stored",
            Self::QueryFailed => "query_failed",
            Self::CollectFailed => "collect_failed",
            Self::InvalidatedMidFlight => "invalidated_mid_flight",
            Self::TransientErrors => "transient_errors",
            Self::Unboundable => "unboundable",
            Self::EncodeFailed => "encode_failed",
            Self::PutFailed => "put_failed",
        }
    }

    /// The `outcome` attribute for this variant. Public because the
    /// revalidation it describes runs in `runtime`, which reaches the counter
    /// directly — as the sibling stale-while-revalidate counters already do.
    #[must_use]
    pub fn key_value(self) -> KeyValue {
        KeyValue::new("outcome", self.as_str())
    }
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
                        "Number of entries evicted, split by `reason`: `expired` past their age, `size` to stay under max_size, or `invalidated` dropped by a client write, dataset reload or unload of a table they read.",
                    )
                    .build()
            });

            pub static STALE_REJECTIONS: LazyLock<Counter<u64>> = LazyLock::new(|| {
                METER
                    .u64_counter(concat!($prefix, "_cache_stale_rejections"))
                    .with_description(
                        "Number of lookups that found an entry but refused to serve it because a client write, dataset reload or unload changed a table it read after its query started. Reported with reason `write_during_read`. These are also counted as misses.",
                    )
                    .build()
            });

            pub static TABLE_INVALIDATIONS: LazyLock<Counter<u64>> = LazyLock::new(|| {
                METER
                    .u64_counter(concat!($prefix, "_cache_table_invalidations"))
                    .with_description(
                        "Number of hard table invalidations applied to this cache, always with mode `evict`. SQL results record client writes, dataset reload and unload, but not refresh, CDC or retention. Counts invalidations, not the entries each one affected.",
                    )
                    .build()
            });

            pub static SWR_REVALIDATIONS: LazyLock<Counter<u64>> = LazyLock::new(|| {
                METER
                    .u64_counter(concat!($prefix, "_cache_swr_revalidations"))
                    .with_description(
                        "Number of completed background stale-while-revalidate revalidations, split by `outcome`. Any outcome other than `stored` leaves the previous entry in place to be served stale until it expires.",
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
                STALE_WHILE_REVALIDATE_SKIPPED.add(0, &[]);
                STALE_WHILE_REVALIDATE_BACKGROUND_QUERIES.add(0, &[]);
                for reason in EvictionReason::ALL {
                    EVICTIONS.add(0, &[reason.key_value()]);
                }
                for reason in StaleRejectionReason::ALL {
                    STALE_REJECTIONS.add(0, &[reason.key_value()]);
                }
                for mode in InvalidationMode::ALL {
                    TABLE_INVALIDATIONS.add(0, &[mode.key_value()]);
                }
                for outcome in RevalidationOutcome::ALL {
                    SWR_REVALIDATIONS.add(0, &[outcome.key_value()]);
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
    /// client write, dataset reload or unload changed a table it read after its
    /// query started.
    fn record_stale_rejection(reason: StaleRejectionReason)
    where
        Self: Sized;
    /// Records a table invalidation applied to this cache, and what it did to
    /// the dependent entries. Counts invalidations, not the entries each one
    /// affected — the per-entry count is [`Self::record_eviction`] under
    /// [`EvictionReason::Invalidated`].
    fn record_table_invalidation(mode: InvalidationMode)
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

    fn record_stale_rejection(reason: StaleRejectionReason) {
        search_results::STALE_REJECTIONS.add(1, &[reason.key_value()]);
    }

    fn record_table_invalidation(mode: InvalidationMode) {
        search_results::TABLE_INVALIDATIONS.add(1, &[mode.key_value()]);
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

    fn record_stale_rejection(reason: StaleRejectionReason) {
        sql_results::STALE_REJECTIONS.add(1, &[reason.key_value()]);
    }

    fn record_table_invalidation(mode: InvalidationMode) {
        sql_results::TABLE_INVALIDATIONS.add(1, &[mode.key_value()]);
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

    fn record_stale_rejection(reason: StaleRejectionReason) {
        embeddings::STALE_REJECTIONS.add(1, &[reason.key_value()]);
    }

    fn record_table_invalidation(mode: InvalidationMode) {
        embeddings::TABLE_INVALIDATIONS.add(1, &[mode.key_value()]);
    }

    fn update_hit_ratio(hits: u64, total: u64) {
        let ratio = calculate_hit_ratio(hits, total);
        embeddings::HIT_RATIO.record(ratio, &[]);
    }

    fn publish_counters_at_zero() {
        embeddings::publish_counters_at_zero();
    }
}
