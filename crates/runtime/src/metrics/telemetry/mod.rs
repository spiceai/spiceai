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

use std::time::Duration;

use super::{Counter, Histogram, LazyLock, Meter, UpDownCounter, global};
use opentelemetry::KeyValue;

pub(crate) static TELEMETRY_METER: LazyLock<Meter> = LazyLock::new(|| global::meter("telemetry"));

static QUERY_COUNT: LazyLock<Counter<u64>> = LazyLock::new(|| {
    TELEMETRY_METER
        .u64_counter("query_executions")
        .with_description("Number of query executions.")
        .with_unit("queries")
        .build()
});

pub fn track_query_count(dimensions: &[KeyValue]) {
    telemetry::track_query_count(dimensions);
    QUERY_COUNT.add(1, dimensions);
}

static QUERY_ACTIVE_COUNT: LazyLock<UpDownCounter<i64>> = LazyLock::new(|| {
    TELEMETRY_METER
        .i64_up_down_counter("query_active_count")
        .with_description(
            "Number of concurrent top-level queries actively being processed in the runtime.",
        )
        .with_unit("queries")
        .build()
});

pub fn inc_query_active_count(dimensions: &[KeyValue]) {
    telemetry::inc_query_active_count(dimensions);
    QUERY_ACTIVE_COUNT.add(1, dimensions);
}

pub fn dec_query_active_count(dimensions: &[KeyValue]) {
    telemetry::dec_query_active_count(dimensions);
    QUERY_ACTIVE_COUNT.add(-1, dimensions);
}

static BYTES_PROCESSED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    TELEMETRY_METER
        .u64_counter("query_processed_bytes")
        .with_description("Number of bytes processed by the runtime.")
        .with_unit("By")
        .build()
});

pub fn track_bytes_processed(bytes: u64, dimensions: &[KeyValue]) {
    telemetry::track_bytes_processed(bytes, dimensions);
    BYTES_PROCESSED.add(bytes, dimensions);
}

static BYTES_RETURNED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    TELEMETRY_METER
        .u64_counter("query_returned_bytes")
        .with_description("Number of bytes returned to query clients.")
        .with_unit("By")
        .build()
});

pub fn track_bytes_returned(bytes: u64, dimensions: &[KeyValue]) {
    telemetry::track_bytes_returned(bytes, dimensions);
    BYTES_RETURNED.add(bytes, dimensions);
}

static ROWS_RETURNED: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    TELEMETRY_METER
        .u64_histogram("query_returned_rows")
        .with_description("Number of rows returned to query clients.")
        .with_boundaries(telemetry::ROWS_RETURNED_HISTOGRAM_BUCKETS.to_vec())
        .with_unit("rows")
        .build()
});

pub fn track_rows_returned(rows: u64, dimensions: &[KeyValue]) {
    telemetry::track_rows_returned(rows, dimensions);
    ROWS_RETURNED.record(rows, dimensions);
}

static QUERY_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    TELEMETRY_METER
        .f64_histogram("query_duration_ms")
        .with_description(
            "The total amount of time spent planning and executing queries in milliseconds.",
        )
        .with_unit("ms")
        .build()
});

pub fn track_query_duration(duration: Duration, dimensions: &[KeyValue]) {
    telemetry::track_query_duration(duration, dimensions);
    QUERY_DURATION_MS.record(duration.as_secs_f64() * 1000.0, dimensions);
}

static QUERY_EXECUTION_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    TELEMETRY_METER
        .f64_histogram("query_execution_duration_ms")
        .with_description(
            "The total amount of time spent only executing queries. This is 0 for cached queries.",
        )
        .with_unit("ms")
        .build()
});

pub fn track_query_execution_duration(duration: Duration, dimensions: &[KeyValue]) {
    telemetry::track_query_execution_duration(duration, dimensions);
    QUERY_EXECUTION_DURATION_MS.record(duration.as_secs_f64() * 1000.0, dimensions);
}

static AI_INFERENCES_WITH_SPICE_COUNT: LazyLock<Counter<u64>> = LazyLock::new(|| {
    TELEMETRY_METER
        .u64_counter("ai_inferences_with_spice_count")
        .with_description("AI Inferences with Spice count")
        .with_unit("inferences")
        .build()
});

pub fn track_ai_inferences_with_spice_count(dimensions: &[KeyValue]) {
    telemetry::track_ai_inferences_with_spice_count(dimensions);
    AI_INFERENCES_WITH_SPICE_COUNT.add(1, dimensions);
}

static QUERY_PRODUCED_SPILLS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    TELEMETRY_METER
        .u64_counter("query_produced_spills")
        .with_description("Number of spills produced by the query")
        .with_unit("spills")
        .build()
});

pub fn track_produced_spills(value: u64, dimensions: &[KeyValue]) {
    telemetry::track_produced_spills(value, dimensions);
    QUERY_PRODUCED_SPILLS.add(value, dimensions);
}

static QUERY_SPILLED_BYTES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    TELEMETRY_METER
        .u64_counter("query_spilled_bytes")
        .with_description("Number of spilled bytes produced by the query")
        .with_unit("By")
        .build()
});

pub fn track_spilled_bytes(value: u64, dimensions: &[KeyValue]) {
    telemetry::track_spilled_bytes(value, dimensions);
    QUERY_SPILLED_BYTES.add(value, dimensions);
}

static QUERY_SPILLED_ROWS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    TELEMETRY_METER
        .u64_counter("query_spilled_rows")
        .with_description("Number of spilled rows produced by the query")
        .with_unit("rows")
        .build()
});

pub fn track_spilled_rows(value: u64, dimensions: &[KeyValue]) {
    telemetry::track_spilled_rows(value, dimensions);
    QUERY_SPILLED_ROWS.add(value, dimensions);
}
