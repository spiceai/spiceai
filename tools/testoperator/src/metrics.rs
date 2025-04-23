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

use test_framework::opentelemetry::metrics::Gauge;
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
