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

//! Gauges for the HTTP connector's response cache.
//!
//! This cache is not one of the caches under `runtime.caching`, so it had no
//! instrumentation at all: whatever it held appeared only as process memory with
//! no attribution, and an operator watching cache gauges would see nothing
//! however large it grew. These two make its occupancy answerable.

use std::sync::LazyLock;

use opentelemetry::{
    global,
    metrics::{Gauge, Meter},
};

static METER: LazyLock<Meter> = LazyLock::new(|| global::meter("http_response_cache"));

/// Bytes currently retained by the connector's response cache: the response
/// bodies, their headers, and the request-shaped keys.
pub static HTTP_RESPONSE_CACHE_SIZE_BYTES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("http_response_cache_size_bytes")
        .with_description(
            "Bytes retained by the HTTP connector's response cache, including response bodies and their request keys.",
        )
        .with_unit("By")
        .build()
});

/// Entries currently held. Read beside the byte gauge this says whether the
/// cache is holding a few large responses or very many small ones, which is what
/// distinguishes a payload-bound cache from a cardinality-bound one.
pub static HTTP_RESPONSE_CACHE_ITEMS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("http_response_cache_items_count")
        .with_description("Number of responses held by the HTTP connector's response cache.")
        .build()
});
