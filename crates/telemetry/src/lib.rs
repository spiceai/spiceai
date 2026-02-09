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

pub use opentelemetry::KeyValue;
use opentelemetry::metrics::UpDownCounter;
use opentelemetry::metrics::{Counter, Histogram};
use std::{sync::OnceLock, time::Duration};

#[cfg(feature = "anonymous_telemetry")]
pub mod anonymous;
pub mod exporter;
pub mod hardware;
pub mod meter;
pub mod noop;
pub mod reader;

// As recommended by the OpenTelemetry Semantic Conventions:
// https://opentelemetry.io/docs/specs/semconv/database/database-metrics/#metric-dbclientresponsereturned_rows
// We added following buckets: 25000.0, 50000.0, 100000.0, 250000.0, 500000.0
pub const ROWS_RETURNED_HISTOGRAM_BUCKETS: [f64; 18] = [
    1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0, 1000.0, 2000.0, 5000.0, 10000.0, 25000.0,
    50000.0, 100_000.0, 250_000.0, 500_000.0,
];

// Extended default buckets for duration histogram: 25000.0, 50000.0, 100000.0, 250000.0, 500000.0
pub const DURATION_MS_HISTOGRAM_BUCKETS: [f64; 15] = [
    0.0, 100.0, 250.0, 500.0, 750.0, 1000.0, 2500.0, 5000.0, 7500.0, 10000.0, 25000.0, 50000.0,
    100_000.0, 250_000.0, 500_000.0,
];

static QUERY_COUNT: OnceLock<Counter<u64>> = OnceLock::new();

pub fn track_query_count(dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    QUERY_COUNT
        .get_or_init(|| {
            m.u64_counter("query_executions")
                .with_description("Number of query executions.")
                .with_unit("queries")
                .build()
        })
        .add(1, dimensions);
}

/// Register the query counter instrument so it appears in the initial export
/// without recording a phantom count.
pub fn register_query_counter(dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    QUERY_COUNT
        .get_or_init(|| {
            m.u64_counter("query_executions")
                .with_description("Number of query executions.")
                .with_unit("queries")
                .build()
        })
        .add(0, dimensions);
}

static QUERY_ACTIVE_COUNT: OnceLock<UpDownCounter<i64>> = OnceLock::new();

pub fn inc_query_active_count(dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    QUERY_ACTIVE_COUNT
        .get_or_init(|| {
            m.i64_up_down_counter("query_active_count")
                .with_description(
                    "Number of concurrent top-level queries actively being processed in the runtime.",
                )
                .with_unit("queries")
                .build()
        })
        .add(1, dimensions);
}

pub fn dec_query_active_count(dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    QUERY_ACTIVE_COUNT
        .get_or_init(|| {
            m.i64_up_down_counter("query_active_count")
                .with_description(
                    "Number of concurrent top-level queries actively being processed in the runtime.",
                )
                .with_unit("queries")
                .build()
        })
        .add(-1, dimensions);
}

static BYTES_PROCESSED: OnceLock<Counter<u64>> = OnceLock::new();

pub fn track_bytes_processed(bytes: u64, dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    BYTES_PROCESSED
        .get_or_init(|| {
            m.u64_counter("query_processed_bytes")
                .with_description("Number of bytes processed by the runtime.")
                .with_unit("By")
                .build()
        })
        .add(bytes, dimensions);
}

static BYTES_RETURNED: OnceLock<Counter<u64>> = OnceLock::new();

pub fn track_bytes_returned(bytes: u64, dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    BYTES_RETURNED
        .get_or_init(|| {
            m.u64_counter("query_returned_bytes")
                .with_description("Number of bytes returned to query clients.")
                .with_unit("By")
                .build()
        })
        .add(bytes, dimensions);
}

static ROWS_RETURNED: OnceLock<Histogram<u64>> = OnceLock::new();

pub fn track_rows_returned(rows: u64, dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    ROWS_RETURNED
        .get_or_init(|| {
            m.u64_histogram("query_returned_rows")
                .with_description("Number of rows returned to query clients.")
                .with_boundaries(ROWS_RETURNED_HISTOGRAM_BUCKETS.to_vec())
                .with_unit("rows")
                .build()
        })
        .record(rows, dimensions);
}

static QUERY_DURATION_MS: OnceLock<Histogram<f64>> = OnceLock::new();

pub fn track_query_duration(duration: Duration, dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    QUERY_DURATION_MS
        .get_or_init(|| {
            m.f64_histogram("query_duration_ms")
                .with_description(
                    "The total amount of time spent planning and executing queries in milliseconds.",
                )
                .with_unit("ms")
                .with_boundaries(DURATION_MS_HISTOGRAM_BUCKETS.to_vec())
                .build()
        })
        .record(duration.as_secs_f64() * 1000.0, dimensions);
}

static QUERY_EXECUTION_DURATION_MS: OnceLock<Histogram<f64>> = OnceLock::new();

pub fn track_query_execution_duration(duration: Duration, dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    QUERY_EXECUTION_DURATION_MS
        .get_or_init(|| {
            m.f64_histogram("query_execution_duration_ms")
                .with_description(
                    "The total amount of time spent only executing queries. This is 0 for cached queries.",
                )
                .with_unit("ms")
                .build()
        })
        .record(duration.as_secs_f64() * 1000.0, dimensions);
}

static AI_INFERENCES_WITH_SPICE_COUNT: OnceLock<Counter<u64>> = OnceLock::new();

pub fn track_ai_inferences_with_spice_count(dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    AI_INFERENCES_WITH_SPICE_COUNT
        .get_or_init(|| {
            m.u64_counter("ai_inferences_with_spice_count")
                .with_description("AI Inferences with Spice count")
                .with_unit("inferences")
                .build()
        })
        .add(1, dimensions);
}

static TEXT_EMBEDDINGS: OnceLock<Counter<u64>> = OnceLock::new();

pub fn track_text_embedding(dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    TEXT_EMBEDDINGS
        .get_or_init(|| {
            m.u64_counter("text_embeddings")
                .with_description("Number of text embeddings requests.")
                .with_unit("embedding")
                .build()
        })
        .add(1, dimensions);
}

static TEXT_SEARCHES: OnceLock<Counter<u64>> = OnceLock::new();

pub fn track_text_search(dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    TEXT_SEARCHES
        .get_or_init(|| {
            m.u64_counter("text_searches")
                .with_description("Number of text search requests.")
                .with_unit("search")
                .build()
        })
        .add(1, dimensions);
}

static VECTOR_SEARCHES: OnceLock<Counter<u64>> = OnceLock::new();

pub fn track_vector_search(dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    VECTOR_SEARCHES
        .get_or_init(|| {
            m.u64_counter("vector_searches")
                .with_description("Number of vector search requests.")
                .with_unit("search")
                .build()
        })
        .add(1, dimensions);
}

static QUERY_PRODUCED_SPILLS: OnceLock<Counter<u64>> = OnceLock::new();

pub fn track_produced_spills(value: u64, dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    QUERY_PRODUCED_SPILLS
        .get_or_init(|| {
            m.u64_counter("query_produced_spills")
                .with_description("Number of spills produced by the query")
                .with_unit("spills")
                .build()
        })
        .add(value, dimensions);
}

static QUERY_SPILLED_BYTES: OnceLock<Counter<u64>> = OnceLock::new();

pub fn track_spilled_bytes(value: u64, dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    QUERY_SPILLED_BYTES
        .get_or_init(|| {
            m.u64_counter("query_spilled_bytes")
                .with_description("Number of spilled bytes produced by the query")
                .with_unit("By")
                .build()
        })
        .add(value, dimensions);
}

static QUERY_SPILLED_ROWS: OnceLock<Counter<u64>> = OnceLock::new();

pub fn track_spilled_rows(value: u64, dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    QUERY_SPILLED_ROWS
        .get_or_init(|| {
            m.u64_counter("query_spilled_rows")
                .with_description("Number of spilled rows produced by the query")
                .with_unit("rows")
                .build()
        })
        .add(value, dimensions);
}

// Hash Index Metrics

static HASH_INDEX_BUILDS: OnceLock<Counter<u64>> = OnceLock::new();

pub fn track_hash_index_build(dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    HASH_INDEX_BUILDS
        .get_or_init(|| {
            m.u64_counter("hash_index_builds")
                .with_description("Number of hash index builds completed.")
                .with_unit("builds")
                .build()
        })
        .add(1, dimensions);
}

static HASH_INDEX_BUILD_DURATION_MS: OnceLock<Histogram<f64>> = OnceLock::new();

pub fn track_hash_index_build_duration(duration: Duration, dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    HASH_INDEX_BUILD_DURATION_MS
        .get_or_init(|| {
            m.f64_histogram("hash_index_build_duration_ms")
                .with_description("Time spent building hash indexes in milliseconds.")
                .with_unit("ms")
                .with_boundaries(DURATION_MS_HISTOGRAM_BUCKETS.to_vec())
                .build()
        })
        .record(duration.as_secs_f64() * 1000.0, dimensions);
}

static HASH_INDEX_ENTRIES: OnceLock<Histogram<u64>> = OnceLock::new();

pub fn track_hash_index_entries(entries: u64, dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    HASH_INDEX_ENTRIES
        .get_or_init(|| {
            m.u64_histogram("hash_index_entries")
                .with_description("Number of entries in hash indexes.")
                .with_boundaries(ROWS_RETURNED_HISTOGRAM_BUCKETS.to_vec())
                .with_unit("entries")
                .build()
        })
        .record(entries, dimensions);
}

static HASH_INDEX_MEMORY_BYTES: OnceLock<Histogram<u64>> = OnceLock::new();

pub fn track_hash_index_memory_bytes(bytes: u64, dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    HASH_INDEX_MEMORY_BYTES
        .get_or_init(|| {
            m.u64_histogram("hash_index_memory_bytes")
                .with_description("Memory used by hash indexes in bytes.")
                .with_unit("By")
                .build()
        })
        .record(bytes, dimensions);
}

static HASH_INDEX_LOOKUPS: OnceLock<Counter<u64>> = OnceLock::new();

pub fn track_hash_index_lookups(count: u64, dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    HASH_INDEX_LOOKUPS
        .get_or_init(|| {
            m.u64_counter("hash_index_lookups")
                .with_description("Number of hash index point lookups performed.")
                .with_unit("lookups")
                .build()
        })
        .add(count, dimensions);
}

static HASH_INDEX_LOOKUP_ROWS: OnceLock<Counter<u64>> = OnceLock::new();

pub fn track_hash_index_lookup_rows(rows: u64, dimensions: &[KeyValue]) {
    let Some(m) = meter::METER.get() else { return };
    HASH_INDEX_LOOKUP_ROWS
        .get_or_init(|| {
            m.u64_counter("hash_index_lookup_rows")
                .with_description("Number of rows returned from hash index lookups.")
                .with_unit("rows")
                .build()
        })
        .add(rows, dimensions);
}
