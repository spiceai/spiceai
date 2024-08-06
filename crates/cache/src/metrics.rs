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

use std::sync::LazyLock;

use opentelemetry::{
    global,
    metrics::{Counter, Gauge, Meter},
};

static METER: LazyLock<Meter> = LazyLock::new(|| global::meter("results_cache"));

pub(crate) static SIZE: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("size")
        .with_description("Size of the cache in bytes.")
        .with_unit("b")
        .init()
});

pub(crate) static MAX_SIZE: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("max_size")
        .with_description("Maximum allowed size of the cache in bytes.")
        .with_unit("b")
        .init()
});

pub(crate) static REQUEST_COUNT: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("request_count")
        .with_description("Number of requests to get a key from the cache.")
        .init()
});

pub(crate) static HIT_COUNT: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("hit_count")
        .with_description("Cache hit count")
        .init()
});

pub(crate) static ITEM_COUNT: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("item_count")
        .with_description("Number of items currently in the cache.")
        .init()
});
