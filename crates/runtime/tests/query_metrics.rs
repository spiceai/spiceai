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

//! Query metrics must not depend on `runtime.task_history.enabled`.
//!
//! This needs its own test binary: the metric handles are `LazyLock`s over
//! `global::meter(..)`, so an instrument first touched before the
//! `MeterProvider` is installed binds to the no-op meter for the whole process.
//! Keep it to a single test so the install order holds under `cargo test` too.

use std::collections::HashSet;

use app::AppBuilder;
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

#[tokio::test]
async fn query_metrics_are_reported_when_task_history_is_disabled() {
    let registry = install_prometheus_meter_provider();

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

    let reported: HashSet<String> = registry
        .gather()
        .iter()
        .map(|family| family.name().to_string())
        .collect();
    let mut reported_names: Vec<&String> = reported.iter().collect();
    reported_names.sort();

    let missing: Vec<&str> = EXPECTED_QUERY_METRICS
        .iter()
        .copied()
        .filter(|metric| !reported.contains(*metric))
        .collect();
    assert!(
        missing.is_empty(),
        "query metrics {missing:?} were not reported with task history disabled; reported: {reported_names:?}"
    );
}
