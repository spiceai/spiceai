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

use test_framework::opentelemetry::metrics::{Gauge, Histogram};
use test_framework::telemetry::METER;

pub static ITERATIONS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("iterations")
        .with_description("Number of query iterations.")
        .with_unit("iterations")
        .build()
});

pub static QUERY_STATUS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("query_status")
        .with_description("Query pass status.")
        .with_unit("status")
        .build()
});

pub static ROW_COUNT: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("row_count")
        .with_description("Number of rows returned from the query.")
        .with_unit("rows")
        .build()
});

pub static ACCELERATION_SIZE_BYTES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("acceleration_size_bytes")
        .with_description("Size of acceleration data on disk.")
        .with_unit("bytes")
        .build()
});

pub static READY_DURATION: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("ready_duration_ms")
        .with_description("Duration until the spicepod is ready.")
        .with_unit("ms")
        .build()
});

pub static HEALTH_LATENCY: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    METER
        .f64_histogram("health_latency_ms")
        .with_description("Latency of /health and /v1/ready probes.")
        .with_unit("ms")
        .build()
});

pub static MEDIAN_DURATION: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("median_duration_ms")
        .with_description("Median duration of the query.")
        .with_unit("ms")
        .build()
});

pub static MIN_DURATION: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("min_duration_ms")
        .with_description("Minimum duration of the query.")
        .with_unit("ms")
        .build()
});

pub static MAX_DURATION: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("max_duration_ms")
        .with_description("Maximum duration of the query.")
        .with_unit("ms")
        .build()
});

pub static P90_DURATION: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("p90_duration_ms")
        .with_description("90th percentile duration of the query.")
        .with_unit("ms")
        .build()
});

pub static P95_DURATION: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("p95_duration_ms")
        .with_description("95th percentile duration of the query.")
        .with_unit("ms")
        .build()
});

pub static P99_DURATION: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("p99_duration_ms")
        .with_description("99th percentile duration of the query.")
        .with_unit("ms")
        .build()
});

pub static TEST_DURATION: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("test_duration_ms")
        .with_description("The entire duration of the test.")
        .with_unit("ms")
        .build()
});

pub static VECTOR_INDEX_CREATION_DURATION: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("vector_index_creation_duration_ms")
        .with_description("Duration of vector search index (embeddings) creation.")
        .with_unit("ms")
        .build()
});

pub static SEARCH_DURATION: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("search_duration_ms")
        .with_description("Total duration to process all search queries.")
        .with_unit("ms")
        .build()
});

pub static SEARCH_RPS: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    METER
        .f64_gauge("search_rps")
        .with_description("Search queries per second.")
        .with_unit("rps")
        .build()
});

pub static SEARCH_P95_RESPONSE_TIME: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    METER
        .f64_gauge("search_p95_time_ms")
        .with_description("95th percentile response time for search queries.")
        .with_unit("ms")
        .build()
});

pub static PEAK_MEMORY_USAGE: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    METER
        .f64_gauge("peak_memory_usage_mb")
        .with_description("The maximum observed memory usage during the test.")
        .with_unit("mb")
        .build()
});

pub static MEDIAN_MEMORY_USAGE: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    METER
        .f64_gauge("median_memory_usage_mb")
        .with_description("The median observed memory usage during the test.")
        .with_unit("mb")
        .build()
});

pub static STATUS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("status")
        .with_description("Test execution status.")
        .with_unit("status")
        .build()
});

pub static SCORE: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    METER
        .f64_gauge("score")
        .with_description("Test score.")
        .with_unit("score")
        .build()
});

// Spiced runtime metrics (scraped from /metrics endpoint)

pub static SPICED_QUERY_COUNT: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    METER
        .f64_gauge("spiced_query_count")
        .with_description("Total number of queries executed by spiced.")
        .with_unit("queries")
        .build()
});

#[expect(dead_code)]
pub static SPICED_QUERY_DURATION_AVG: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    METER
        .f64_gauge("spiced_query_duration_avg_ms")
        .with_description("Average query duration from spiced metrics.")
        .with_unit("ms")
        .build()
});

pub static SPICED_CACHE_HIT_RATE: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    METER
        .f64_gauge("spiced_cache_hit_rate")
        .with_description("Cache hit rate from spiced metrics.")
        .with_unit("ratio")
        .build()
});

pub static SPICED_ACTIVE_CONNECTIONS: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    METER
        .f64_gauge("spiced_active_connections")
        .with_description("Peak active connections during test.")
        .with_unit("connections")
        .build()
});
