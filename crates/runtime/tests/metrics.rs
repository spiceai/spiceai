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

#![recursion_limit = "256"]

use std::{
    collections::{HashMap, HashSet},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{Arc, LazyLock},
    time::Duration,
};

use app::AppBuilder;
use arrow_flight::{FlightClient, FlightDescriptor, Ticket};
use cache::{metrics::CacheMetrics, result::query::CachedQueryResult};
use futures::{StreamExt, TryStreamExt};
use opentelemetry::global;
use opentelemetry_sdk::{Resource, metrics::SdkMeterProvider};
use runtime::{Runtime, auth::EndpointAuth, config::Config, datafusion::query::QueryBuilder};
use spicepod::component::runtime::{Query, Runtime as SpicepodRuntime, TaskHistory};
use tonic::transport::Channel;

const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

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

/// The sampler's first sample lands before the operator's provider exists, and
/// the gauge must still reach `/metrics` afterwards.
///
/// `tokio::time::interval` fires its first tick immediately, so
/// `spawn_mem_tier_repartition_sampler` records before `init_metrics` installs
/// the Prometheus provider. An instrument cached at that moment binds to the
/// startup noop provider for the life of the process, which is what kept
/// `query_memory_pool_used_bytes` and `cayenne_compaction_memory_pool_used_bytes`
/// off `/metrics` entirely — regression test for #12667.
///
/// `process_resident_memory_bytes` is asserted alongside them even though it was
/// scrapable, because it was only ever scrapable by accident: it is recorded
/// after a `spawn_blocking(...).await` in the same loop iteration, and that
/// round-trip happened to outlast the window. Nothing holds that ordering, so it
/// is pinned here rather than left to survive on timing.
///
/// [`memory_gauges_are_exported_to_the_operator_metrics_pipeline`] cannot catch
/// that: it forces [`PROMETHEUS`] first, so it only ever records after the
/// install. This test records *before* it, which under `cargo nextest` — one
/// process per test, the gate's runner — means the global provider really is
/// the noop one at that point. Under `cargo test` a sibling thread may have
/// installed it already, which costs the test its teeth but not its safety: the
/// seal below always follows the install, never precedes it.
#[test]
fn a_gauge_recorded_before_the_provider_is_installed_still_reaches_metrics() {
    // Deliberately ahead of `&*PROMETHEUS`: this is the sampler's first tick.
    telemetry::track_process_resident_memory_bytes(1_024, &[]);
    telemetry::cayenne::track_query_memory_pool_used_bytes(2_048, &[]);
    telemetry::cayenne::track_compaction_memory_pool_used_bytes(4_096, &[]);

    // `init_metrics` installing the operator's provider, then declaring it final.
    let registry = &*PROMETHEUS;
    telemetry::seal_operator_meter_provider();

    // The sampler's next tick, two seconds later in production.
    telemetry::track_process_resident_memory_bytes(8_192, &[]);
    telemetry::cayenne::track_query_memory_pool_used_bytes(16_384, &[]);
    telemetry::cayenne::track_compaction_memory_pool_used_bytes(32_768, &[]);

    let reported = reported_metric_names(registry);
    let missing: Vec<&str> = EXPECTED_MEMORY_METRICS
        .iter()
        .copied()
        .filter(|metric| !reported.contains(*metric))
        .collect();
    assert!(
        missing.is_empty(),
        "memory gauges {missing:?} were recorded before the meter provider was installed and \
         never recovered; a gauge cached against the startup noop provider never reaches \
         /metrics. Reported: {:?}",
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

/// Every `flight_requests` increment recorded so far, summed across label sets.
///
/// Summed rather than read per-series because a sample's labels depend on where
/// it is recorded: the handler that knows the command adds a `command` label the
/// `FlightService` impl cannot. A per-series read would pass a duplicate off as a
/// relabelling.
fn flight_requests_total(registry: &prometheus::Registry) -> f64 {
    registry
        .gather()
        .iter()
        .filter(|family| family.name() == "flight_requests")
        .flat_map(prometheus::proto::MetricFamily::get_metric)
        .map(|metric| metric.get_counter().value())
        .sum()
}

/// Asserts `rpc` recorded exactly one `flight_requests` increment.
///
/// `#[track_caller]` so a failure names the RPC rather than this line.
#[track_caller]
fn assert_recorded_once(registry: &prometheus::Registry, before: f64, rpc: &str) {
    let recorded = flight_requests_total(registry) - before;
    assert!(
        (recorded - 1.0).abs() < f64::EPSILON,
        "{rpc} must record exactly one flight_requests increment, recorded {recorded}"
    );
}

/// Starts a runtime with no datasets and returns a channel to its Flight server.
///
/// No dataset is needed: every RPC below either fails before planning or runs a
/// literal query, and a dataset load would only add series to the registry.
async fn start_flight_server() -> Result<Channel, anyhow::Error> {
    // Both listeners are bound before either is dropped, so the two ports are
    // guaranteed to differ — freeing the first before binding the second would let
    // the OS hand back the same ephemeral port.
    let flight_listener = std::net::TcpListener::bind(SocketAddr::new(LOCALHOST, 0))?;
    let http_listener = std::net::TcpListener::bind(SocketAddr::new(LOCALHOST, 0))?;
    let flight_port = flight_listener.local_addr()?.port();
    let http_port = http_listener.local_addr()?.port();
    drop(flight_listener);
    drop(http_listener);

    let app = AppBuilder::new("flight_request_metrics").build();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);
    Arc::clone(&rt).load_components().await;

    let api_config = Config::new()
        .with_http_bind_address(SocketAddr::new(LOCALHOST, http_port))
        .with_flight_bind_address(SocketAddr::new(LOCALHOST, flight_port));

    tokio::spawn(async move {
        Box::pin(rt.start_servers(api_config, None, EndpointAuth::default())).await
    });

    // Poll for the bind rather than sleeping: the server binds asynchronously and a
    // connection refused before it does is not a failure.
    let endpoint = Channel::from_shared(format!("http://127.0.0.1:{flight_port}"))?;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        match endpoint.clone().connect().await {
            Ok(channel) => return Ok(channel),
            Err(e) if std::time::Instant::now() >= deadline => {
                return Err(anyhow::anyhow!(
                    "Flight server did not accept a connection on port {flight_port} within 30s: {e}"
                ));
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
}

/// One Flight RPC records exactly one `flight_requests` increment.
///
/// Two ways of getting that wrong are invisible to the compiler and to lints, and
/// asserting the *delta* per RPC is what separates them — a duplicate reads as
/// `2`, a lost sample as `0`, where an assertion that the series merely exists
/// passes under both:
///
/// - A timer in the `FlightService` impl on top of the one in the handler it
///   delegates to counts the RPC twice. The two are not interchangeable: the
///   handler's spans the response stream, the outer one ends when the stream is
///   constructed, so the histogram also mixes each real latency with a shorter
///   prefix of itself.
/// - `let _start = track_flight_request(..)` without `.await` binds the future
///   rather than the measurement, and a future dropped unpolled records nothing at
///   all. Naming the binding suppresses `unused_must_use`, and
///   `clippy::let_underscore_future` only fires on the `let _` form.
///
/// Regression test for <https://github.com/spiceai/spiceai/issues/12844>.
///
/// The delta assertions are safe on this binary's shared registry because no
/// sibling test here emits `flight_requests`.
#[tokio::test]
async fn each_flight_rpc_records_exactly_one_request() -> Result<(), anyhow::Error> {
    let registry = &*PROMETHEUS;
    let channel = start_flight_server().await?;
    let mut client = FlightClient::new(channel);

    // One increment, under one method name: the service impl labelled this
    // `do_handshake` while the handler labels it `handshake`.
    let before = flight_requests_total(registry);
    client
        .handshake("flight_request_metrics")
        .await
        .map_err(|e| anyhow::anyhow!("handshake: {e}"))?;
    assert_recorded_once(registry, before, "handshake");

    // A ticket that is not a FlightSQL command is served by `do_get_simple`. The
    // stream is drained because the handler's measurement spans the drain; the
    // counter increments before it, so this pins the ordering the histogram
    // depends on.
    let before = flight_requests_total(registry);
    let stream = client
        .do_get(Ticket::new("SELECT 1"))
        .await
        .map_err(|e| anyhow::anyhow!("do_get: {e}"))?;
    let batches: Vec<_> = stream
        .try_collect()
        .await
        .map_err(|e| anyhow::anyhow!("do_get stream: {e}"))?;
    assert_eq!(batches.len(), 1, "SELECT 1 returns a single batch");
    assert_recorded_once(registry, before, "do_get");

    let before = flight_requests_total(registry);
    client
        .get_schema(FlightDescriptor::new_cmd("SELECT 1"))
        .await
        .map_err(|e| anyhow::anyhow!("get_schema: {e}"))?;
    assert_recorded_once(registry, before, "get_schema");

    let before = flight_requests_total(registry);
    let actions: Vec<_> = client
        .list_actions()
        .await
        .map_err(|e| anyhow::anyhow!("list_actions: {e}"))?
        .try_collect()
        .await
        .map_err(|e| anyhow::anyhow!("list_actions stream: {e}"))?;
    assert!(
        !actions.is_empty(),
        "list_actions advertises the FlightSQL actions"
    );
    assert_recorded_once(registry, before, "list_actions");

    // A rejection reached before any command-specific handler runs must still be
    // counted. These are the paths that recorded nothing, so a re-dropped future
    // reads as zero here rather than as a duplicate.
    let before = flight_requests_total(registry);
    let empty = futures::stream::empty::<arrow_flight::error::Result<arrow_flight::FlightData>>();
    let refused = async { client.do_put(empty).await?.try_collect::<Vec<_>>().await }
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("do_put with no flight data must be refused"))?
        .to_string();
    assert!(
        refused.contains("No flight data provided"),
        "do_put with no flight data must be refused for the reason under test, got: {refused}"
    );
    assert_recorded_once(registry, before, "do_put with no flight data");

    Ok(())
}
