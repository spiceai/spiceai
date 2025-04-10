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

use super::{Counter, LazyLock, Meter, global};

pub(crate) static RUNTIME_METER: LazyLock<Meter> = LazyLock::new(|| global::meter("runtime"));

pub(crate) static FLIGHT_SERVER_START: LazyLock<Counter<u64>> = LazyLock::new(|| {
    RUNTIME_METER
        .u64_counter("runtime_flight_server_started")
        .with_description("Indicates the runtime Flight server has started.")
        .build()
});

pub(crate) static HTTP_SERVER_START: LazyLock<Counter<u64>> = LazyLock::new(|| {
    RUNTIME_METER
        .u64_counter("runtime_http_server_started")
        .with_description("Indicates the runtime HTTP server has started.")
        .build()
});
