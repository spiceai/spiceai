/*
Copyright 2024-2025 The Spice.ai OSS Authors

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
    global,
    metrics::{Counter, Gauge, Meter},
};

use crate::result::{
    embeddings::CachedEmbeddingResult, query::CachedQueryResult, search::CachedSearchResult,
};

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
                    .with_description("Number of cache evictions.")
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
    fn record_eviction()
    where
        Self: Sized;
    fn update_hit_ratio(hits: u64, total: u64)
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

    fn record_eviction() {
        search_results::EVICTIONS.add(1, &[]);
    }

    fn update_hit_ratio(hits: u64, total: u64) {
        let ratio = calculate_hit_ratio(hits, total);
        search_results::HIT_RATIO.record(ratio, &[]);
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

    fn record_eviction() {
        sql_results::EVICTIONS.add(1, &[]);
    }

    fn update_hit_ratio(hits: u64, total: u64) {
        let ratio = calculate_hit_ratio(hits, total);
        sql_results::HIT_RATIO.record(ratio, &[]);
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

    fn record_eviction() {
        embeddings::EVICTIONS.add(1, &[]);
    }

    fn update_hit_ratio(hits: u64, total: u64) {
        let ratio = calculate_hit_ratio(hits, total);
        embeddings::HIT_RATIO.record(ratio, &[]);
    }
}
