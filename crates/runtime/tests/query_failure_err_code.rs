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

//! `query_failures{err_code}` must name the condition that failed.
//!
//! A memory-pool refusal is characteristically mid-stream, and that path used
//! to finish the tracker with a hardcoded `QueryExecutionError`.
//!
//! This needs its own test binary, for the same reason `query_metrics.rs` does:
//! the metric handles are `LazyLock`s over `global::meter(..)`, so an instrument
//! first touched before the `MeterProvider` is installed binds to the no-op
//! meter for the whole process. Keep it to a single test.

use app::AppBuilder;
use futures::StreamExt;
use opentelemetry::global;
use opentelemetry_sdk::{Resource, metrics::SdkMeterProvider};
use runtime::{Runtime, datafusion::query::QueryBuilder};
use spicepod::component::runtime::{Query, Runtime as SpicepodRuntime};

/// Small enough that the sort below cannot fit, large enough that the runtime
/// still starts.
const QUERY_MEMORY_LIMIT: &str = "16MiB";

/// A `TopK` cannot spill, so it refuses while batches are polled — the path
/// under test. `fetch` is deliberately larger than the limit can hold.
const EXHAUSTING_QUERY: &str =
    "SELECT value FROM generate_series(1, 5000000) ORDER BY value DESC LIMIT 4000000";

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

/// Every `err_code` label recorded on `query_failures`, with its count.
fn failures_by_err_code(registry: &prometheus::Registry) -> Vec<(String, f64)> {
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

#[tokio::test]
async fn a_mid_stream_pool_refusal_is_counted_as_resources_exhausted() {
    let registry = install_prometheus_meter_provider();

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

    let recorded = failures_by_err_code(&registry);
    assert!(
        recorded
            .iter()
            .any(|(code, count)| code == "ResourcesExhausted" && *count > 0.0),
        "a memory-pool refusal must be counted as err_code=\"ResourcesExhausted\"; recorded: {recorded:?}"
    );
    assert!(
        !recorded
            .iter()
            .any(|(code, count)| code == "QueryExecutionError" && *count > 0.0),
        "capacity must not also be counted as a generic execution failure; recorded: {recorded:?}"
    );
}
