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
    metrics::{Counter, Gauge, Histogram, Meter},
};

static METER: LazyLock<Meter> = LazyLock::new(|| global::meter("datasets_acceleration"));

pub(crate) static REFRESH_ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("refresh_errors")
        .with_description("Number of errors refreshing the dataset.")
        .init()
});

pub(crate) static LAST_REFRESH_TIME: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    METER
        .f64_gauge("last_refresh_time")
        .with_description("Seconds elapsed since the last successful refresh.")
        .with_unit("s")
        .init()
});

pub(crate) static LOAD_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    METER
        .f64_histogram("load_duration_ms")
        .with_description("Duration in milliseconds to load a full refresh acceleration.")
        .with_unit("ms")
        .init()
});

pub(crate) static APPEND_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    METER
        .f64_histogram("append_duration_ms")
        .with_description("Duration in milliseconds to append to an acceleration.")
        .with_unit("ms")
        .init()
});
