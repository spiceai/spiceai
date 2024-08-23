/*
Copyright 2024 The Spice.ai OSS Authors

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
    metrics::{Counter, Histogram},
    KeyValue,
};
use std::{
    sync::{Arc, LazyLock},
    time::Duration,
};

#[cfg(feature = "anonymous_telemetry")]
pub mod anonymous;
mod exporter;
mod meter;

static QUERY_COUNT: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("query_count")
        .with_description("Number of queries run.")
        .with_unit("queries")
        .init()
});

pub fn track_query_count() {
    QUERY_COUNT.add(1, &[]);
}

static BYTES_PROCESSED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("bytes_processed")
        .with_description("Number of bytes processed by the runtime.")
        .with_unit("bytes")
        .init()
});

pub fn track_bytes_processed(bytes: u64, protocol: Arc<str>) {
    let dimensions = create_dimensions(protocol);
    BYTES_PROCESSED.add(bytes, &dimensions);
}

static BYTES_RETURNED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("bytes_returned")
        .with_description("Number of bytes returned to query clients.")
        .with_unit("bytes")
        .init()
});

pub fn track_bytes_returned(bytes: u64, protocol: Arc<str>) {
    let dimensions = create_dimensions(protocol);
    BYTES_RETURNED.add(bytes, &dimensions);
}

static QUERY_DURATION: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    METER
        .f64_histogram("query_duration")
        .with_description("The total amount of time spent planning and executing queries.")
        .with_unit("ms")
        .init()
});

pub fn track_query_duration(duration: Duration, protocol: Arc<str>) {
    let dimensions = create_dimensions(protocol);
    QUERY_DURATION.record(duration.as_secs_f64() * 1000.0, &dimensions);
}

static QUERY_EXECUTION_DURATION: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    METER
        .f64_histogram("query_execution_duration")
        .with_description(
            "The total amount of time spent only executing queries. This is 0 for cached queries.",
        )
        .with_unit("ms")
        .init()
});

pub fn track_query_execution_duration(duration: Duration, protocol: Arc<str>) {
    let dimensions = create_dimensions(protocol);
    QUERY_EXECUTION_DURATION.record(duration.as_secs_f64() * 1000.0, &dimensions);
}

fn create_dimensions(protocol: Arc<str>) -> [KeyValue; 1] {
    [KeyValue::new("protocol", protocol)]
}
