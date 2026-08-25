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
//! whole process. Tests are grouped by what drives the metric rather than one per
//! family, so the surface needs only two runtimes between them.

#![recursion_limit = "256"]

// Accelerator engines are their own crates and self-register through a linkme slice. Each
// integration test is a separate binary that links independently, and the linker drops an
// unreferenced slice static, so a binary exercising Cayenne must name the crate itself.
#[cfg(not(windows))]
use accelerator_cayenne as _;

// Accelerator engines are their own crates and self-register through a linkme slice. A
// dev-dependency alone does not put an entry in a test binary — the linker drops the
// unreferenced static — and every integration binary links separately, so each one that
// exercises an engine needs its own reference. `integration.rs`'s
// `accelerator_crates_register_their_engines` guards the mechanism.
#[cfg(feature = "duckdb")]
use accelerator_duckdb as _;
#[cfg(feature = "postgres-accel")]
use accelerator_postgres as _;
#[cfg(feature = "sqlite")]
use accelerator_sqlite as _;
#[cfg(feature = "turso")]
use accelerator_turso as _;

use std::{
    collections::{HashMap, HashSet},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{Arc, LazyLock},
    time::Duration,
};

use app::AppBuilder;
use arrow_flight::{FlightClient, FlightDescriptor, Ticket};
use cache::{
    metrics::CacheMetrics,
    result::{
        embeddings::CachedEmbeddingResult, query::CachedQueryResult, search::CachedSearchResult,
    },
};
use futures::{StreamExt, TryStreamExt};
use opentelemetry::global;
use opentelemetry_sdk::{Resource, metrics::SdkMeterProvider};
use runtime::{Runtime, auth::EndpointAuth, config::Config, datafusion::query::QueryBuilder};
use spicepod::{
    acceleration::{Acceleration, Mode, RefreshMode},
    component::dataset::{Dataset, TimeFormat},
    component::runtime::{Query, Runtime as SpicepodRuntime, TaskHistory},
};
use tonic::transport::Channel;

const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

/// Metrics every query must report, whatever `runtime.task_history.enabled` is set
/// to. `query_processed_bytes` needs a real table: the bytes-processed optimizer
/// only wraps a plan that scans something.
const EXPECTED_QUERY_METRICS: &[&str] = &[
    "query_duration_ms",
    "query_execution_duration_ms",
    "query_executions",
    "query_failures",
    "query_returned_rows",
    "query_returned_bytes",
    "query_processed_bytes",
    "query_active_count",
    "query_produced_spills",
    "query_spilled_bytes",
    "query_spilled_rows",
];

/// Series a loaded, refreshed dataset must report. The `max_timestamp` and `lag`
/// series are off by default: they need `runtime.metrics` to name them and the
/// dataset to declare a `time_column`.
const EXPECTED_DATASET_METRICS: &[&str] = &[
    "dataset_active_count",
    "dataset_acceleration_refresh_duration_ms",
    "dataset_acceleration_last_refresh_unix_time_ms",
    "dataset_acceleration_max_timestamp_before_refresh_ms",
    "dataset_acceleration_max_timestamp_after_refresh_ms",
    "dataset_acceleration_refresh_lag_ms",
    "dataset_acceleration_ingestion_lag_ms",
];

/// Gauges the startup registrations record, all on the global meter.
///
/// The per-worker tokio gauges (`tokio_runtime_worker_busy_seconds`,
/// `..._park_count`, `..._steal_count`) are deliberately absent: they need
/// `RUSTFLAGS="--cfg tokio_unstable"`, which no Spice build sets.
const EXPECTED_RUNTIME_GAUGES: &[&str] = &[
    "process_resident_memory_bytes",
    "query_memory_pool_used_bytes",
    "cayenne_compaction_memory_pool_used_bytes",
    "cayenne_compaction_memory_pool_bytes",
    "spiced_cpu_budget_cores",
    "spiced_cpu_budget_millicores",
    "spiced_cpu_limit_millicores",
    "spiced_cpu_request_millicores",
    "tokio_runtime_alive_tasks",
    "tokio_runtime_workers",
    "tokio_runtime_global_queue_depth",
];

/// Counters `publish_component_counters_at_zero` must emit.
const EXPECTED_COMPONENT_COUNTERS: &[&str] = &[
    "dataset_load_errors",
    "catalog_load_errors",
    "view_load_errors",
    "model_load_errors",
    "embeddings_load_errors",
    "rerankers_load_errors",
    "tool_load_errors",
    "component_metric_registered_count",
];

/// The caches `Runtime::init_cache_metrics` publishes, by exported name prefix.
/// `results` rather than `sql_results` is the shipped prefix — see
/// <https://github.com/spiceai/spiceai/issues/6128>.
const CACHE_PREFIXES: &[&str] = &["results", "search_results", "embeddings"];

/// Every family `generate_cache_metrics!` declares, minus the prefix.
const CACHE_SUFFIXES: &[&str] = &[
    "size_bytes",
    "max_size_bytes",
    "requests",
    "hits",
    "misses",
    "hit_ratio",
    "items_count",
    "evictions",
    "stale_rejections",
    "stale_swr_count",
    "swr_background_query_count",
];

/// Small enough that the sort below cannot fit, large enough that the runtime
/// still starts.
const QUERY_MEMORY_LIMIT: &str = "16MiB";

/// A `TopK` cannot spill, so it refuses while batches are polled — the path under
/// test. `fetch` is deliberately larger than the limit can hold.
const EXHAUSTING_QUERY: &str =
    "SELECT value FROM generate_series(1, 5000000) ORDER BY value DESC LIMIT 4000000";

/// The epoch second the fixture's first generation of rows carries.
const FIXTURE_EPOCH_SECONDS: i64 = 1_700_000_000;

/// How much later the second generation is stamped. Large enough that a lag
/// computed in the wrong unit lands nowhere near it.
const FIXTURE_EPOCH_STEP_SECONDS: i32 = 3_600;

/// The one `MeterProvider` for this binary. Every test that asserts on a metric
/// forces this first, so the install wins the race under either test harness.
///
/// The registry is shared, so an assertion must not depend on what siblings
/// recorded: presence is safe, a count must compare a before/after delta — see
/// [`a_mid_stream_pool_refusal_is_counted_as_resources_exhausted`].
static PROMETHEUS: LazyLock<prometheus::Registry> =
    LazyLock::new(install_prometheus_meter_provider);

/// Installs a `MeterProvider` backed by a scrapable Prometheus registry, as
/// `spiced` does for the `--metrics` endpoint.
///
/// The reader comes from [`runtime::prometheus_reader`] so the test and `spiced`
/// cannot drift on exposition naming.
fn install_prometheus_meter_provider() -> prometheus::Registry {
    let registry = prometheus::Registry::new();

    let provider = SdkMeterProvider::builder()
        .with_resource(Resource::builder().build())
        .with_reader(
            runtime::prometheus_reader(registry.clone()).expect("to build the prometheus reader"),
        )
        .build();
    global::set_meter_provider(provider);

    registry
}

fn reported_metric_names(registry: &prometheus::Registry) -> HashSet<String> {
    registry
        .gather()
        .iter()
        .map(|family| family.name().to_string())
        .collect()
}

fn sorted(reported: &HashSet<String>) -> Vec<&String> {
    let mut names: Vec<&String> = reported.iter().collect();
    names.sort();
    names
}

/// Fails naming every metric in `expected` that `reported` does not carry.
fn assert_all_reported(reported: &HashSet<String>, expected: &[&str], what: &str) {
    let missing: Vec<&str> = expected
        .iter()
        .copied()
        .filter(|metric| !reported.contains(*metric))
        .collect();
    assert!(
        missing.is_empty(),
        "{what}: {missing:?} did not reach the metrics pipeline. Reported: {:?}",
        sorted(reported)
    );
}

/// The value of the gauge `name`, which must carry exactly one series.
///
/// Taking the first of several would silently pick a winner, so a second dataset
/// in the fixture app has to fail here rather than assert against an arbitrary one.
fn gauge_value(registry: &prometheus::Registry, name: &str) -> Option<f64> {
    let family = registry
        .gather()
        .into_iter()
        .find(|family| family.name() == name)?;
    let series = family.get_metric();
    assert_eq!(
        series.len(),
        1,
        "{name} carries {} series, so there is no single value to assert on",
        series.len()
    );
    series.first().map(|m| m.get_gauge().value())
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

/// How much `query_failures{err_code}` grew between two snapshots.
fn increment(before: &HashMap<String, f64>, after: &HashMap<String, f64>, err_code: &str) -> f64 {
    let at = |counts: &HashMap<String, f64>| counts.get(err_code).copied().unwrap_or(0.0);
    at(after) - at(before)
}

fn expected_cache_metrics() -> Vec<String> {
    CACHE_PREFIXES
        .iter()
        .flat_map(|prefix| {
            CACHE_SUFFIXES
                .iter()
                .map(move |suffix| format!("{prefix}_cache_{suffix}"))
        })
        .collect()
}

/// The acceleration metrics the fixture enables. They are off unless named here.
///
/// This is `runtime.metrics`, not the dataset's own `metrics:` — the latter gates
/// connector component metrics, and setting it here would silently do nothing.
fn opt_in_acceleration_metrics() -> spicepod::metric::Metrics {
    let metrics = [
        runtime_metrics::acceleration::METRIC_MAX_TIMESTAMP_BEFORE_REFRESH_MS,
        runtime_metrics::acceleration::METRIC_MAX_TIMESTAMP_AFTER_REFRESH_MS,
        runtime_metrics::acceleration::METRIC_REFRESH_LAG_MS,
        runtime_metrics::acceleration::METRIC_INGESTION_LAG_MS,
    ]
    .into_iter()
    .map(|name| spicepod::metric::Metric {
        enabled: true,
        name: name.to_string(),
    })
    .collect();
    spicepod::metric::Metrics { metrics }
}

fn fixture_csv(dir: &std::path::Path, name: &str) -> std::path::PathBuf {
    dir.join(format!("{name}.csv"))
}

fn write_fixture_csv(path: &std::path::Path, base_epoch_seconds: i64) {
    use std::fmt::Write as _;

    let mut csv = String::from("id,score,ts\n");
    for i in 0..3 {
        let _ = writeln!(csv, "{},{},{}", i + 1, (i + 1) * 10, base_epoch_seconds + i);
    }
    std::fs::write(path, csv).expect("to write the fixture csv");
}

/// A local file and the built-in `arrow` accelerator need no cargo feature and no
/// connector registration — `file` self-registers through
/// `register_data_connector!` — so this works in the gate's feature-less build.
/// `ts` is a Unix-seconds integer because CSV inference types it as `Int64`.
fn csv_backed_dataset(dir: &std::path::Path, name: &str) -> Dataset {
    let csv = fixture_csv(dir, name);
    write_fixture_csv(&csv, FIXTURE_EPOCH_SECONDS);

    let mut dataset = Dataset::new(format!("file://{}", csv.display()), name);
    dataset.time_column = Some("ts".to_string());
    dataset.time_format = Some(TimeFormat::UnixSeconds);
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("arrow".to_string()),
        mode: Mode::Memory,
        refresh_mode: Some(RefreshMode::Full),
        ..Acceleration::default()
    });
    dataset
}

async fn wait_until<F, Fut>(timeout: Duration, mut f: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if f().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

/// Startup must publish every counter a dashboard panel reads, at zero.
///
/// These are exactly the calls `init_cache_metrics` and `init_component_metrics`
/// make, and nothing else here touches a cache or component instrument, so a
/// series present afterwards was published by startup rather than by a real event.
#[test]
fn startup_publishes_the_cache_and_component_counters_at_zero() {
    let registry = &*PROMETHEUS;

    CachedQueryResult::init();
    CachedSearchResult::init();
    CachedEmbeddingResult::init();
    runtime_metrics::publish_component_counters_at_zero();

    let reported = reported_metric_names(registry);

    let cache_metrics = expected_cache_metrics();
    let cache_metrics: Vec<&str> = cache_metrics.iter().map(String::as_str).collect();
    assert_all_reported(
        &reported,
        &cache_metrics,
        "cache metrics were not exported after the caches were initialised, so an operator \
         scraping a healthy runtime cannot tell them from a broken exporter",
    );

    assert_all_reported(
        &reported,
        EXPECTED_COMPONENT_COUNTERS,
        "component counters were not published at zero, so a dashboard panel on a healthy \
         runtime reads \"No data\" rather than zero",
    );
}

/// The startup registrations must land on the operator's provider.
///
/// `register_cpu_budget_metrics` and `register_tokio_runtime_metrics` both require
/// that `init_metrics` has run; forcing [`PROMETHEUS`] first is that ordering.
#[tokio::test]
async fn startup_registrations_export_the_process_and_runtime_gauges() {
    let registry = &*PROMETHEUS;

    telemetry::track_process_resident_memory_bytes(1_024, &[]);
    telemetry::cayenne::track_query_memory_pool_used_bytes(2_048, &[]);
    telemetry::cayenne::track_compaction_memory_pool_used_bytes(4_096, &[]);
    telemetry::cayenne::track_compaction_memory_pool_bytes(8_192, &[]);

    // Both optional arms are `Some`: a `None` records nothing at all, so the limit
    // and request gauges would simply be absent.
    telemetry::register_cpu_budget_metrics(4, 4_000, "test", Some(4_000), Some(2_000));
    telemetry::register_tokio_runtime_metrics(vec![("main", tokio::runtime::Handle::current())]);

    assert_all_reported(
        &reported_metric_names(registry),
        EXPECTED_RUNTIME_GAUGES,
        "process and runtime gauges were recorded but not exported to the Prometheus registry \
         (a gauge on the anonymous-telemetry meter never reaches /metrics)",
    );
}

/// A gauge recorded before the provider is installed must still reach `/metrics`.
///
/// `tokio::time::interval` fires its first tick immediately, so the mem-tier
/// sampler records before `init_metrics` installs the Prometheus provider. An
/// instrument cached at that moment binds to the startup noop provider for the life
/// of the process, which kept `query_memory_pool_used_bytes` and
/// `cayenne_compaction_memory_pool_used_bytes` off `/metrics` entirely — regression
/// test for #12667.
///
/// Only meaningful under `cargo nextest`, where this test gets its own process. In
/// a shared process a sibling may have installed the provider already, which costs
/// the test its teeth but not its safety.
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

    assert_all_reported(
        &reported_metric_names(registry),
        &[
            "process_resident_memory_bytes",
            "query_memory_pool_used_bytes",
            "cayenne_compaction_memory_pool_used_bytes",
        ],
        "memory gauges were recorded before the meter provider was installed and never \
         recovered; a gauge cached against the startup noop provider never reaches /metrics",
    );
}

/// Reads the resident set size directly and touches no instrument, so it needs no
/// meter provider.
#[test]
fn resident_memory_is_a_plausible_reading() {
    let rss = runtime::resource_monitor::process_resident_memory_bytes()
        .expect("resident set size to be readable on a supported platform");

    // A live test process holds more than a page and less than a terabyte. A
    // kB-vs-bytes mix-up on either arm lands far outside this band.
    assert!(
        rss > 1_048_576,
        "resident set size {rss} B is below 1 MiB, which suggests a kB value was reported as bytes"
    );
    assert!(
        rss < 1_099_511_627_776,
        "resident set size {rss} B exceeds 1 TiB, which suggests a bytes value was scaled again"
    );
}

/// A refreshed, accelerated dataset must report the query and refresh families.
///
/// `task_history` is disabled because the query metrics must not depend on it.
#[tokio::test]
async fn an_accelerated_dataset_reports_the_query_and_refresh_families() {
    let registry = &*PROMETHEUS;

    let dir = tempfile::tempdir().expect("a temporary directory for the fixture");
    let app = AppBuilder::new("metrics_accelerated_dataset")
        .with_dataset(csv_backed_dataset(dir.path(), "scores"))
        .with_runtime(SpicepodRuntime {
            task_history: TaskHistory {
                enabled: false,
                ..Default::default()
            },
            metrics: Some(opt_in_acceleration_metrics()),
            ..Default::default()
        })
        .build();

    let rt = Arc::new(Runtime::builder().with_app(app).build().await);

    tokio::time::timeout(Duration::from_mins(1), Arc::clone(&rt).load_components())
        .await
        .expect("the dataset to load within a minute");
    assert!(
        wait_until(Duration::from_mins(1), || async { rt.status().is_ready() }).await,
        "the runtime never reported ready, so the dataset never loaded"
    );

    // Move the source forward before refreshing. An unchanged file lets the
    // connector answer "nothing to fetch" and return before any lag metric is
    // recorded, and identical timestamps would make a lag of zero indistinguishable
    // from a lag never computed.
    write_fixture_csv(
        &fixture_csv(dir.path(), "scores"),
        FIXTURE_EPOCH_SECONDS + i64::from(FIXTURE_EPOCH_STEP_SECONDS),
    );

    let notifier = rt
        .datafusion()
        .refresh_table(&datafusion::common::TableReference::from("scores"), None)
        .await
        .expect("the refresh request to be accepted")
        .expect("an accelerated table to return a refresh notifier");
    tokio::time::timeout(Duration::from_mins(1), notifier.notified())
        .await
        .expect("the refresh to complete within a minute");

    let mut result = QueryBuilder::new("SELECT id, score FROM scores", rt.datafusion())
        .build()
        .run()
        .await
        .expect("query to run");

    // The metrics are recorded when the result stream terminates, so drain it.
    let mut rows = 0;
    while let Some(batch) = result.data.next().await {
        rows += batch.expect("batch to stream without error").num_rows();
    }
    assert_eq!(rows, 3, "the fixture csv holds three rows");

    let failed = QueryBuilder::new("SELECT * FROM does_not_exist", rt.datafusion())
        .build()
        .run()
        .await;
    assert!(failed.is_err(), "a query on a missing table must fail");

    let reported = reported_metric_names(registry);
    assert_all_reported(
        &reported,
        EXPECTED_QUERY_METRICS,
        "query metrics were not reported with task history disabled",
    );
    assert_all_reported(
        &reported,
        EXPECTED_DATASET_METRICS,
        "a loaded and refreshed dataset did not report its acceleration metrics",
    );
    // Presence is not enough for a lag gauge: a unit or sign slip leaves the series
    // on the dashboard and the number wrong.
    let refresh_lag_ms = gauge_value(registry, "dataset_acceleration_refresh_lag_ms")
        .expect("the refresh-lag gauge to carry a value");
    let expected_lag_ms = f64::from(FIXTURE_EPOCH_STEP_SECONDS * 1_000);
    assert!(
        (refresh_lag_ms - expected_lag_ms).abs() < f64::EPSILON,
        "the source moved {FIXTURE_EPOCH_STEP_SECONDS}s forward, so the refresh lag must be \
         {expected_lag_ms}ms, not {refresh_lag_ms}ms"
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
