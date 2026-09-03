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
    metrics::{Counter, Gauge, Meter},
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

/// What an invalidation did to the entries that read the table, reported as the
/// `mode` attribute on `*_cache_table_invalidations`.
///
/// Without this attribute the mode switch is invisible: a cache configured with
/// `stale_while_revalidate_ttl` stops reporting
/// [`EvictionReason::Invalidated`] entirely, because there is no longer an
/// eviction to report, and nothing else records that a refresh happened at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidationMode {
    /// The entries that read the table were dropped from the cache.
    Evict,
    /// The entries were left resident and marked stale as of this instant, to
    /// be served once more inside `stale_while_revalidate_ttl` while a
    /// background revalidation replaces each of them.
    MarkStale,
}

impl InvalidationMode {
    /// Every variant, so a caller publishing the series up front cannot omit one.
    pub const ALL: [Self; 2] = [Self::Evict, Self::MarkStale];

    fn as_str(self) -> &'static str {
        match self {
            Self::Evict => "evict",
            Self::MarkStale => "mark_stale",
        }
    }

    fn key_value(self) -> KeyValue {
        KeyValue::new("mode", self.as_str())
    }
}

/// Why a lookup found an entry but would not serve it, reported as the `reason`
/// attribute on `*_cache_stale_rejections`.
///
/// The three are operationally distinct, and an unlabelled total cannot be
/// acted on: only [`StaleRejectionReason::WindowExpired`] says the configured
/// `stale_while_revalidate_ttl` is too short for the rate queries arrive at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StaleRejectionReason {
    /// No `stale_while_revalidate_ttl` is configured, so an invalidation is a
    /// hard one and the entry stopped being servable the moment it was marked.
    NoStaleWindow,
    /// A window is configured but the query arrived after it closed, so the
    /// entry expired unserved instead of absorbing the refresh it was kept for.
    WindowExpired,
    /// The entry is inside the window, but this lookup serves only fresh
    /// results and so declined it — a miss the stale-while-revalidate path
    /// would have absorbed.
    FreshRequired,
}

impl StaleRejectionReason {
    /// Every variant, so a caller publishing the series up front cannot omit one.
    pub const ALL: [Self; 3] = [
        Self::NoStaleWindow,
        Self::WindowExpired,
        Self::FreshRequired,
    ];

    fn as_str(self) -> &'static str {
        match self {
            Self::NoStaleWindow => "no_window",
            Self::WindowExpired => "window_expired",
            Self::FreshRequired => "fresh_required",
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
    /// A table the revalidation read was invalidated while it ran, so its
    /// result may predate that invalidation and was discarded.
    InvalidatedMidFlight,
    /// The result carried transient HTTP error responses (5xx/429), so the
    /// previous entry was preserved rather than overwritten with them.
    TransientErrors,
    /// The result could not be encoded for storage.
    EncodeFailed,
    /// The cache write itself failed.
    PutFailed,
}

impl RevalidationOutcome {
    /// Every variant, so a caller publishing the series up front cannot omit one.
    pub const ALL: [Self; 7] = [
        Self::Stored,
        Self::QueryFailed,
        Self::CollectFailed,
        Self::InvalidatedMidFlight,
        Self::TransientErrors,
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
                        "Number of entries evicted because they expired or the cache was over capacity. Entries dropped by table invalidation are counted by the invalidations metric instead.",
                    )
                    .build()
            });

            pub static STALE_REJECTIONS: LazyLock<Counter<u64>> = LazyLock::new(|| {
                METER
                    .u64_counter(concat!($prefix, "_cache_stale_rejections"))
                    .with_description(
                        "Number of lookups that found an entry but refused to serve it because a table it read had since been invalidated. Split by `reason` into whether no stale-while-revalidate window is configured, the window had closed, or the caller serves only fresh results. These are also counted as misses.",
                    )
                    .build()
            });

            pub static TABLE_INVALIDATIONS: LazyLock<Counter<u64>> = LazyLock::new(|| {
                METER
                    .u64_counter(concat!($prefix, "_cache_table_invalidations"))
                    .with_description(
                        "Number of table invalidations applied to this cache, split by `mode` into whether the dependent entries were evicted or left resident and marked stale. Counts invalidations, not the entries each one affected.",
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

            pub static INVALIDATION_STALE_HITS: LazyLock<Counter<u64>> = LazyLock::new(|| {
                METER
                    .u64_counter(concat!($prefix, "_cache_invalidation_stale_hits"))
                    .with_description(
                        "Number of results served from an entry a table invalidation had marked stale, within the configured stale-while-revalidate window. Counted where the result is handed back, so a request whose own `max-stale` is shorter than the window, or whose entry fails to decode, is not counted here even though the entry was found.",
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
                INVALIDATION_STALE_HITS.add(0, &[]);
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
    /// table it read had since been invalidated.
    fn record_stale_rejection(reason: StaleRejectionReason)
    where
        Self: Sized;
    /// Records a table invalidation applied to this cache, and what it did to
    /// the dependent entries. Counts invalidations, not the entries each one
    /// affected — the per-entry count is [`Self::record_eviction`] under
    /// [`EvictionReason::Invalidated`], which reports nothing at all when the
    /// invalidation marks entries stale instead of removing them.
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
