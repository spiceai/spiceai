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

use super::{Histogram, LazyLock, Meter, UpDownCounter, global};

pub(crate) static WORKERS_METER: LazyLock<Meter> = LazyLock::new(|| global::meter("worker"));

pub(crate) static COUNT: LazyLock<UpDownCounter<i64>> = LazyLock::new(|| {
    WORKERS_METER
        .i64_up_down_counter("worker_active_count")
        .with_description("Number of currently loaded workers.")
        .build()
});

pub(crate) static LOAD_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    WORKERS_METER
        .f64_histogram("workers_load_duration_ms")
        .with_description("Duration in milliseconds to load the worker.")
        .build()
});
