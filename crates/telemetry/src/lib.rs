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

use meter::METER;
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram},
};
use std::{sync::LazyLock, time::Duration};

#[cfg(feature = "anonymous_telemetry")]
pub mod anonymous;
pub mod exporter;
pub mod meter;
pub mod noop;
pub mod reader;

static QUERY_COUNT: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("query_executions")
        .with_description("Number of query executions.")
        .with_unit("queries")
        .build()
});

pub fn track_query_count(dimensions: &[KeyValue]) {
    QUERY_COUNT.add(1, dimensions);
}

static BYTES_PROCESSED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("query_processed_bytes")
        .with_description("Number of bytes processed by the runtime.")
        .with_unit("By")
        .build()
});

pub fn track_bytes_processed(bytes: u64, dimensions: &[KeyValue]) {
    BYTES_PROCESSED.add(bytes, dimensions);
}

static BYTES_RETURNED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("query_returned_bytes")
        .with_description("Number of bytes returned to query clients.")
        .with_unit("By")
        .build()
});

pub fn track_bytes_returned(bytes: u64, dimensions: &[KeyValue]) {
    BYTES_RETURNED.add(bytes, dimensions);
}

static QUERY_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    METER
        .f64_histogram("query_duration_ms")
        .with_description(
            "The total amount of time spent planning and executing queries in milliseconds.",
        )
        .with_unit("ms")
        .build()
});

pub fn track_query_duration(duration: Duration, dimensions: &[KeyValue]) {
    QUERY_DURATION_MS.record(duration.as_secs_f64() * 1000.0, dimensions);
}

static QUERY_EXECUTION_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    METER
        .f64_histogram("query_execution_duration_ms")
        .with_description(
            "The total amount of time spent only executing queries. This is 0 for cached queries.",
        )
        .with_unit("ms")
        .build()
});

pub fn track_query_execution_duration(duration: Duration, dimensions: &[KeyValue]) {
    QUERY_EXECUTION_DURATION_MS.record(duration.as_secs_f64() * 1000.0, dimensions);
}

static AI_INFERENCES_WITH_SPICE_COUNT: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("ai_inferences_with_spice_count")
        .with_description("AI Inferences with Spice count")
        .with_unit("inferences")
        .build()
});

pub fn track_ai_inferences_with_spice_count(dimensions: &[KeyValue]) {
    AI_INFERENCES_WITH_SPICE_COUNT.add(1, dimensions);
}

static TEXT_EMBEDDINGS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("text_embeddings")
        .with_description("Number of text embeddings requests.")
        .with_unit("embedding")
        .build()
});

pub fn track_text_embedding(dimensions: &[KeyValue]) {
    TEXT_EMBEDDINGS.add(1, dimensions);
}

static TEXT_SEARCHES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("text_searches")
        .with_description("Number of text search requests.")
        .with_unit("search")
        .build()
});

pub fn track_text_search(dimensions: &[KeyValue]) {
    TEXT_SEARCHES.add(1, dimensions);
}

static VECTOR_SEARCHES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("vector_searches")
        .with_description("Number of vector search requests.")
        .with_unit("search")
        .build()
});

pub fn track_vector_search(dimensions: &[KeyValue]) {
    VECTOR_SEARCHES.add(1, dimensions);
}
