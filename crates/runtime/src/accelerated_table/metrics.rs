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
        .u64_counter("datasets_acceleration_refresh_errors")
        .with_description("Number of errors refreshing the dataset.")
        .init()
});

pub(crate) static LAST_REFRESH_TIME_MS: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    METER
        .f64_gauge("datasets_acceleration_last_refresh_time_ms")
        .with_description("Unix timestamp in seconds when the last refresh completed.")
        .with_unit("ms")
        .init()
});

pub(crate) static LOAD_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    METER
        .f64_histogram("datasets_acceleration_load_duration_ms")
        .with_description("Duration in milliseconds to load a full refresh acceleration.")
        .with_unit("ms")
        .init()
});

pub(crate) static APPEND_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    METER
        .f64_histogram("datasets_acceleration_append_duration_ms")
        .with_description("Duration in milliseconds to append to an acceleration.")
        .with_unit("ms")
        .init()
});

pub(crate) static READY_STATE_FALLBACK: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("accelerated_ready_state_federated_fallback")
        .with_description("Number of times the federated table was queried due to the accelerated table loading the initial data.")
        .init()
});
