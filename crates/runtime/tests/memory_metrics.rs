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

//! The memory gauges must reach the operator's metrics pipeline.
//!
//! These describe resident memory and pool pressure — the numbers an operator
//! reads while diagnosing an OOM — so they belong on the OpenTelemetry global
//! provider that `init_metrics` installs behind `/metrics` and OTLP. Recording
//! them on the anonymous-telemetry meter instead still compiles and still looks
//! correct at the call site; the gauge simply never appears on the dashboard.
//! Asserting on a scraped Prometheus registry is what distinguishes the two.
//!
//! This needs its own test binary, for the reason `query_metrics.rs` documents:
//! an instrument first touched before the `MeterProvider` is installed binds to
//! the no-op meter for the whole process.

use std::collections::HashSet;

use opentelemetry::global;
use opentelemetry_sdk::{Resource, metrics::SdkMeterProvider};

/// Gauges that must be scrapable once recorded. Emitted by the mem-tier
/// repartition sampler in `DataFusion::spawn_mem_tier_repartition_sampler`.
const EXPECTED_MEMORY_METRICS: &[&str] = &[
    "process_resident_memory_bytes",
    "query_memory_pool_used_bytes",
    "cayenne_compaction_memory_pool_used_bytes",
];

/// Installs a `MeterProvider` backed by a scrapable Prometheus registry, as
/// `spiced` does for the `--metrics` endpoint.
fn install_prometheus_meter_provider() -> prometheus::Registry {
    let registry = prometheus::Registry::new();

    let exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .without_scope_info()
        .without_units()
        .without_counter_suffixes()
        .without_target_info()
        .build()
        .expect("to build the prometheus exporter");

    let provider = SdkMeterProvider::builder()
        .with_resource(Resource::builder().build())
        .with_reader(exporter)
        .build();
    global::set_meter_provider(provider);

    registry
}

#[test]
fn memory_gauges_are_exported_to_the_operator_metrics_pipeline() {
    let registry = install_prometheus_meter_provider();

    // Record through the same entry points the sampler calls, so a gauge wired
    // to the wrong meter fails here rather than silently on an operator's host.
    telemetry::track_process_resident_memory_bytes(1_024, &[]);
    telemetry::cayenne::track_query_memory_pool_used_bytes(2_048, &[]);
    telemetry::cayenne::track_compaction_memory_pool_used_bytes(4_096, &[]);

    let reported: HashSet<String> = registry
        .gather()
        .iter()
        .map(|family| family.name().to_string())
        .collect();
    let mut reported_names: Vec<&String> = reported.iter().collect();
    reported_names.sort();

    let missing: Vec<&str> = EXPECTED_MEMORY_METRICS
        .iter()
        .copied()
        .filter(|metric| !reported.contains(*metric))
        .collect();
    assert!(
        missing.is_empty(),
        "memory gauges {missing:?} were recorded but not exported to the Prometheus registry \
         (a gauge on the anonymous-telemetry meter never reaches /metrics); reported: {reported_names:?}"
    );
}

#[test]
fn resident_memory_is_a_plausible_reading() {
    let rss = runtime::resource_monitor::process_resident_memory_bytes()
        .expect("resident set size to be readable on a supported platform");

    // A live test process holds more than a page and less than a terabyte. The
    // point is unit sanity: a kB-vs-bytes mix-up on either arm lands far outside
    // this band rather than merely looking small.
    assert!(
        rss > 1_048_576,
        "resident set size {rss} B is below 1 MiB, which suggests a kB value was reported as bytes"
    );
    assert!(
        rss < 1_099_511_627_776,
        "resident set size {rss} B exceeds 1 TiB, which suggests a bytes value was scaled again"
    );
}
