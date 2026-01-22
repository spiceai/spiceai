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

use super::{Counter, Gauge, LazyLock, Meter, global};

pub(crate) static VIEWS_METER: LazyLock<Meter> = LazyLock::new(|| global::meter("view"));

pub(crate) static LOAD_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
    VIEWS_METER
        .u64_counter("view_load_errors")
        .with_description("Number of errors loading the view.")
        .build()
});

pub(crate) static STATUS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    VIEWS_METER
        .u64_gauge("view_load_state")
        .with_description(
            "Status of the views. 0=Initializing, 1=Ready, 2=Disabled, 3=Error, 4=Refreshing, 5=ShuttingDown.",
        )
        .build()
});
