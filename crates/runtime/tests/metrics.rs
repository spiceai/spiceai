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

//! Metrics must reach the operator's metrics pipeline, whatever the config says.
//!
//! Recording a metric on the wrong meter — the anonymous-telemetry one instead of
//! the `OpenTelemetry` global provider that `init_metrics` installs behind
//! `/metrics` and OTLP — still compiles and still looks correct at the call site.
//! The gauge simply never appears on the dashboard. Asserting on a scraped
//! Prometheus registry is what distinguishes the two.
//!
//! Metrics live in their own test binary because the install order is global: the
//! metric handles are `LazyLock`s over `global::meter(..)`, so an instrument first
//! touched before the `MeterProvider` is installed binds to the no-op meter for the
//! whole process. Every test here shares one provider, installed by [`PROMETHEUS`]
//! before any instrument exists — see its comment for what that means for
//! assertions.

use std::{
    collections::{HashMap, HashSet},
    sync::LazyLock,
};

use app::AppBuilder;
use futures::StreamExt;
use opentelemetry::global;
use opentelemetry_sdk::{Resource, metrics::SdkMeterProvider};
use runtime::{Runtime, datafusion::query::QueryBuilder};
use spicepod::component::runtime::{Query, Runtime as SpicepodRuntime, TaskHistory};

/// Metrics every query must report, whatever `runtime.task_history.enabled` is
/// set to. All are emitted from the query tracker.
const EXPECTED_QUERY_METRICS: &[&str] = &[
    "query_duration_ms",
    "query_execution_duration_ms",
    "query_executions",
    "query_failures",
    "query_returned_rows",
    "query_returned_bytes",
];

/// Gauges that must be scrapable once recorded. Emitted by the mem-tier
/// repartition sampler in `DataFusion::spawn_mem_tier_repartition_sampler`.
const EXPECTED_MEMORY_METRICS: &[&str] = &[
    "process_resident_memory_bytes",
    "query_memory_pool_used_bytes",
    "cayenne_compaction_memory_pool_used_bytes",
];

/// Small enough that the sort below cannot fit, large enough that the runtime
/// still starts.
const QUERY_MEMORY_LIMIT: &str = "16MiB";

/// A `TopK` cannot spill, so it refuses while batches are polled — the path
/// under test. `fetch` is deliberately larger than the limit can hold.
const EXHAUSTING_QUERY: &str =
    "SELECT value FROM generate_series(1, 5000000) ORDER BY value DESC LIMIT 4000000";

/// The one `MeterProvider` for this binary, installed before any instrument is
/// built. Forcing this is the first statement of every test that asserts on a
/// metric, so the install wins the race however the harness schedules them:
/// `cargo nextest` gives each test its own process, `cargo test` gives them
/// threads, and `LazyLock` covers both.
///
/// Sharing the registry means an assertion must not depend on what siblings
/// recorded. Presence assertions are safe, because each test records what it
/// checks. A count assertion is not, so it must compare a before/after delta —
/// see [`a_mid_stream_pool_refusal_is_counted_as_resources_exhausted`].
static PROMETHEUS: LazyLock<prometheus::Registry> =
    LazyLock::new(install_prometheus_meter_provider);

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

/// Every metric family on the registry, by name.
fn reported_metric_names(registry: &prometheus::Registry) -> HashSet<String> {
    registry
        .gather()
        .iter()
        .map(|family| family.name().to_string())
        .collect()
}

/// The names in `reported`, sorted, for an assertion message.
fn sorted(reported: &HashSet<String>) -> Vec<&String> {
    let mut names: Vec<&String> = reported.iter().collect();
    names.sort();
    names
}

/// The count recorded on `query_failures` for each `err_code` label.
fn failures_by_err_code(registry: &prometheus::Registry) -> HashMap<String, f64> {
    registry
        .gather()
        .iter()
        .filter(|family| family.name() == "query_failures")
        .flat_map(|family| {
            family.get_metric().iter().map(|metric| {
                let err_code = metric
                    .get_label()
                    .iter()
                    .find(|label| label.name() == "err_code")
                    .map_or_else(String::new, |label| label.value().to_string());
                (err_code, metric.get_counter().value())
            })
        })
        .collect()
}

/// How much `query_failures{err_code}` grew between two [`failures_by_err_code`]
/// snapshots.
fn increment(before: &HashMap<String, f64>, after: &HashMap<String, f64>, err_code: &str) -> f64 {
    let at = |counts: &HashMap<String, f64>| counts.get(err_code).copied().unwrap_or(0.0);
    at(after) - at(before)
}

#[tokio::test]
async fn query_metrics_are_reported_when_task_history_is_disabled() {
    let registry = &*PROMETHEUS;

    let app = AppBuilder::new("query_metrics_without_task_history")
        .with_runtime(SpicepodRuntime {
            task_history: TaskHistory {
                enabled: false,
                ..Default::default()
            },
            ..Default::default()
        })
        .build();

    let rt = Runtime::builder().with_app(app).build().await;

    let mut result = QueryBuilder::new("SELECT 1", rt.datafusion())
        .build()
        .run()
        .await
        .expect("query to run");

    // The metrics are recorded when the result stream terminates, so drain it.
    let mut rows = 0;
    while let Some(batch) = result.data.next().await {
        rows += batch.expect("batch to stream without error").num_rows();
    }
    assert_eq!(rows, 1, "SELECT 1 returns a single row");

    // `query_failures` is recorded on the error path, on its own meter.
    let failed = QueryBuilder::new("SELECT * FROM does_not_exist", rt.datafusion())
        .build()
        .run()
        .await;
    assert!(failed.is_err(), "a query on a missing table must fail");

    let reported = reported_metric_names(registry);
    let missing: Vec<&str> = EXPECTED_QUERY_METRICS
        .iter()
        .copied()
        .filter(|metric| !reported.contains(*metric))
        .collect();
    assert!(
        missing.is_empty(),
        "query metrics {missing:?} were not reported with task history disabled; reported: {:?}",
        sorted(&reported)
    );
}

#[test]
fn memory_gauges_are_exported_to_the_operator_metrics_pipeline() {
    let registry = &*PROMETHEUS;

    // Record through the same entry points the sampler calls, so a gauge wired
    // to the wrong meter fails here rather than silently on an operator's host.
    telemetry::track_process_resident_memory_bytes(1_024, &[]);
    telemetry::cayenne::track_query_memory_pool_used_bytes(2_048, &[]);
    telemetry::cayenne::track_compaction_memory_pool_used_bytes(4_096, &[]);

    let reported = reported_metric_names(registry);
    let missing: Vec<&str> = EXPECTED_MEMORY_METRICS
        .iter()
        .copied()
        .filter(|metric| !reported.contains(*metric))
        .collect();
    assert!(
        missing.is_empty(),
        "memory gauges {missing:?} were recorded but not exported to the Prometheus registry \
         (a gauge on the anonymous-telemetry meter never reaches /metrics); reported: {:?}",
        sorted(&reported)
    );
}

/// Reads the resident set size directly and touches no instrument, so it needs
/// no meter provider.
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

/// `query_failures{err_code}` must name the condition that failed.
///
/// A memory-pool refusal is characteristically mid-stream, and that path used to
/// finish the tracker with a hardcoded `QueryExecutionError`.
#[tokio::test]
async fn a_mid_stream_pool_refusal_is_counted_as_resources_exhausted() {
    let registry = &*PROMETHEUS;

    let app = AppBuilder::new("query_failure_err_code")
        .with_runtime(SpicepodRuntime {
            query: Some(Query {
                memory_limit: Some(QUERY_MEMORY_LIMIT.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        })
        .build();

    let rt = Runtime::builder().with_app(app).build().await;

    // Other tests share this registry and also record failures, so bracket the
    // query and attribute only the change to it.
    let before = failures_by_err_code(registry);

    let result = QueryBuilder::new(EXHAUSTING_QUERY, rt.datafusion())
        .build()
        .run()
        .await;

    // The refusal can surface from `run()` or from the stream, depending on how
    // eagerly the plan is polled. Both must reach the same label.
    let error = match result {
        Ok(mut result) => {
            let mut stream_error = None;
            while let Some(batch) = result.data.next().await {
                if let Err(e) = batch {
                    stream_error = Some(e.to_string());
                    break;
                }
            }
            stream_error.expect("the query must not succeed within the memory limit")
        }
        Err(e) => e.to_string(),
    };
    assert!(
        error.contains("Resources exhausted"),
        "the query must fail for want of memory, not for another reason: {error}"
    );

    let after = failures_by_err_code(registry);
    assert!(
        increment(&before, &after, "ResourcesExhausted") > 0.0,
        "a memory-pool refusal must be counted as err_code=\"ResourcesExhausted\"; \
         counts went from {before:?} to {after:?}"
    );
    // A counter grows by whole failures, so anything below one is no failure at all.
    assert!(
        increment(&before, &after, "QueryExecutionError") < 1.0,
        "capacity must not also be counted as a generic execution failure; \
         counts went from {before:?} to {after:?}"
    );
}
