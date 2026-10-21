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
    global,
    metrics::{Counter, Histogram, Meter},
};

use telemetry::DURATION_MS_HISTOGRAM_BUCKETS;

static METER: LazyLock<Meter> = LazyLock::new(|| global::meter("http"));

/// Deprecated, to be removed in the future
pub static REQUESTS_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("http_requests_total")
        .with_description("Number of HTTP requests. Deprecated, use http_requests instead.")
        .build()
});

pub static REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("http_requests")
        .with_description("Number of HTTP requests.")
        .build()
});

pub static REQUESTS_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    METER
        .f64_histogram("http_requests_duration_ms")
        .with_unit("ms")
        .with_boundaries(DURATION_MS_HISTOGRAM_BUCKETS.to_vec())
        .build()
});

pub static RESPONSES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("http_responses")
        .with_description(
            "Number of HTTP responses, counted once the response body terminates. The 'outcome' \
             label reports how it terminated ('complete', 'error', or 'incomplete'), so a \
             streaming response that fails after its 200 head is not counted as a success.",
        )
        .build()
});

pub static RESPONSES_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    METER
        .f64_histogram("http_responses_duration_ms")
        .with_description(
            "End-to-end HTTP response duration, from the request arriving to the response body \
             terminating. Unlike http_requests_duration_ms, this includes the time spent \
             streaming the body.",
        )
        .with_unit("ms")
        .with_boundaries(DURATION_MS_HISTOGRAM_BUCKETS.to_vec())
        .build()
});
