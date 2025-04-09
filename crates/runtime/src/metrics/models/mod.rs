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

use super::{Counter, Gauge, Histogram, LazyLock, Meter, UpDownCounter, global};

pub(crate) static MODELS_METER: LazyLock<Meter> = LazyLock::new(|| global::meter("model"));

pub(crate) static LOAD_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
    MODELS_METER
        .u64_counter("model_load_errors")
        .with_description("Number of errors loading the model.")
        .build()
});

pub(crate) static LOAD_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    MODELS_METER
        .f64_histogram("model_load_duration_ms")
        .with_description("Duration in milliseconds to load the model.")
        .with_unit("ms")
        .build()
});

pub(crate) static COUNT: LazyLock<UpDownCounter<i64>> = LazyLock::new(|| {
    MODELS_METER
        .i64_up_down_counter("model_active_count")
        .with_description("Number of currently loaded models.")
        .build()
});

pub(crate) static STATUS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    MODELS_METER
        .u64_gauge("model_load_state")
        .with_description(
            "Status of the model. 1=Initializing, 2=Ready, 3=Disabled, 4=Error, 5=Refreshing.",
        )
        .build()
});
