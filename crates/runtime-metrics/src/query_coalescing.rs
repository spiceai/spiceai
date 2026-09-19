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

//! Measurements for shared point-lookup execution.

use opentelemetry::metrics::{Counter, Histogram};
use std::sync::LazyLock;

pub struct Metrics {
    pub requests: Counter<u64>,
    pub batches: Counter<u64>,
    pub flushes: Counter<u64>,
    pub unique_keys: Counter<u64>,
    pub candidate_rows: Counter<u64>,
    pub batch_size: Histogram<u64>,
    pub collection_ms: Histogram<f64>,
    pub admission_ms: Histogram<f64>,
    pub execution_ms: Histogram<f64>,
    pub planning_ms: Histogram<f64>,
    pub first_batch_ms: Histogram<f64>,
}

// Initialized on first use, after installation of the runtime meter provider.
pub static METRICS: LazyLock<Metrics> = LazyLock::new(|| {
    let meter = opentelemetry::global::meter("query_coalescing");
    let timings = vec![
        0.0, 0.25, 0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0, 4096.0,
        16_384.0, 65_536.0,
    ];
    Metrics {
        requests: meter.u64_counter("query_coalescing_requests").build(),
        batches: meter.u64_counter("query_coalescing_batches").build(),
        flushes: meter.u64_counter("query_coalescing_flushes").build(),
        unique_keys: meter.u64_counter("query_coalescing_unique_keys").build(),
        candidate_rows: meter.u64_counter("query_coalescing_candidate_rows").build(),
        batch_size: meter
            .u64_histogram("query_coalescing_batch_size")
            .with_boundaries(vec![
                1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0,
                1024.0, 4096.0, 16_384.0, 65_536.0,
            ])
            .build(),
        collection_ms: meter
            .f64_histogram("query_coalescing_collection_ms")
            .with_unit("ms")
            .with_description("Time from the first request joining a batch to admission; includes admission wait.")
            .with_boundaries(timings.clone())
            .build(),
        admission_ms: meter
            .f64_histogram("query_coalescing_admission_ms")
            .with_unit("ms")
            .with_description("Time waiting for family and query permits after the collection window; overlaps collection time.")
            .with_boundaries(timings.clone())
            .build(),
        execution_ms: meter
            .f64_histogram("query_coalescing_execution_ms")
            .with_unit("ms")
            .with_description("Time planning and executing one shared scan.")
            .with_boundaries(timings.clone())
            .build(),
        planning_ms: meter
            .f64_histogram("query_coalescing_planning_ms")
            .with_unit("ms")
            .with_description("Time creating the shared physical plan.")
            .with_boundaries(timings.clone())
            .build(),
        first_batch_ms: meter
            .f64_histogram("query_coalescing_first_batch_ms")
            .with_unit("ms")
            .with_description("Time from planning start to the first Arrow batch, which may be empty.")
            .with_boundaries(timings)
            .build(),
    }
});
