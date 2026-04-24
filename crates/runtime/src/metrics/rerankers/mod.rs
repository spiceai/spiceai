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

use super::{Counter, Gauge, LazyLock, Meter, UpDownCounter, global};

pub(crate) static RERANKERS_METER: LazyLock<Meter> = LazyLock::new(|| global::meter("rerankers"));

pub(crate) static LOAD_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
    RERANKERS_METER
        .u64_counter("rerankers_load_errors")
        .with_description("Number of errors loading the reranker.")
        .build()
});

pub(crate) static COUNT: LazyLock<UpDownCounter<i64>> = LazyLock::new(|| {
    RERANKERS_METER
        .i64_up_down_counter("rerankers_active_count")
        .with_description("Number of currently loaded rerankers.")
        .build()
});

pub(crate) static STATUS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    RERANKERS_METER
        .u64_gauge("rerankers_load_state")
        .with_description(
            "Status of the reranker. 0=Initializing, 1=Ready, 2=Disabled, 3=Error, 4=Refreshing, 5=ShuttingDown.",
        )
        .build()
});
