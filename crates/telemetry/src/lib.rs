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

use meter::METER;
use opentelemetry::metrics::Counter;
use std::sync::LazyLock;

#[cfg(feature = "anonymous_telemetry")]
pub mod anonymous;
mod exporter;
mod meter;

pub static QUERY_COUNT: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("query_count")
        .with_description("Number of queries run.")
        .with_unit("queries")
        .init()
});
