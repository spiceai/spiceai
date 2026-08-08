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
//! This needs its own test binary: the metric handles are `LazyLock`s over
//! `global::meter(..)`, so an instrument first touched before the
//! `MeterProvider` is installed binds to the no-op meter for the whole process.
//! Every test here shares one provider, installed by [`PROMETHEUS`] before any
//! instrument exists — see its comment for what that means for assertions.

use std::{collections::HashSet, sync::LazyLock};

use app::AppBuilder;
use cache::{metrics::CacheMetrics, result::query::CachedQueryResult};
use futures::StreamExt;
use opentelemetry::global;
use opentelemetry_sdk::{Resource, metrics::SdkMeterProvider};
use runtime::{Runtime, datafusion::query::QueryBuilder};
use spicepod::component::runtime::{Runtime as SpicepodRuntime, TaskHistory};

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

/// Series the SQL results cache must export as soon as it is enabled, whether or
/// not anything has been recorded on them yet.
///
/// Each instrument is a `LazyLock` that only registers with the meter when
/// something first derefs it, so a counter that has not fired exports nothing at
/// all — not a zero. On a healthy runtime `results_cache_evictions` and both
/// stale-while-revalidate counters are exactly the ones that never fire, and an
/// absent series reads as a broken exporter rather than as "nothing happened".
const EXPECTED_RESULTS_CACHE_METRICS: &[&str] = &[
    "results_cache_requests",
    "results_cache_hits",
    "results_cache_misses",
    "results_cache_evictions",
    "results_cache_stale_swr_count",
    "results_cache_swr_background_query_count",
    "results_cache_items_count",
    "results_cache_size_bytes",
    "results_cache_max_size_bytes",
];

/// The one `MeterProvider` for this binary, installed before any instrument is
/// built. Forcing this is the first statement of every test that asserts on a
/// metric, so the install wins the race however the harness schedules them:
/// `cargo nextest` gives each test its own process, `cargo test` gives them
/// threads, and `LazyLock` covers both.
///
/// Sharing the registry means an assertion must not depend on what siblings
/// recorded. Presence assertions are safe, because each test records what it
/// checks.
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

/// The runtime calls `init` for each configured cache at startup, and that is the
/// only chance an instrument nothing has touched gets to appear on `/metrics`.
#[test]
fn cache_counters_are_exported_before_anything_is_recorded() {
    let registry = &*PROMETHEUS;

    // Exactly what `Runtime::init_cache_metrics` calls once `runtime.caching.sql_results`
    // is configured. Nothing else in this test touches a cache instrument, so a series
    // present afterwards was published by `init` rather than by a real cache event.
    CachedQueryResult::init();

    let reported = reported_metric_names(registry);
    let missing: Vec<&str> = EXPECTED_RESULTS_CACHE_METRICS
        .iter()
        .copied()
        .filter(|metric| !reported.contains(*metric))
        .collect();
    assert!(
        missing.is_empty(),
        "cache metrics {missing:?} were not exported after the cache was initialised, so an \
         operator scraping a healthy runtime cannot tell them from a broken exporter; \
         reported: {:?}",
        sorted(&reported)
    );
}
