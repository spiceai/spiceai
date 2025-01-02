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
    metrics::{Counter, Histogram, Meter},
};

static METER: LazyLock<Meter> = LazyLock::new(|| global::meter("http"));

/// Deprecated, to be removed in the future
pub(crate) static REQUESTS_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("http_requests_total")
        .with_description("Number of HTTP requests. Deprecated, use http_requests instead.")
        .build()
});

pub(crate) static REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("http_requests")
        .with_description("Number of HTTP requests.")
        .build()
});

pub(crate) static REQUESTS_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    METER
        .f64_histogram("http_requests_duration_ms")
        .with_unit("ms")
        .build()
});
