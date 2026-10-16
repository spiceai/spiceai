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
        CacheStatus, embeddings::CachedEmbeddingResult, query::CachedQueryResult,
        search::CachedSearchResult,
    },
};
use futures::{StreamExt, TryStreamExt};
use opentelemetry::global;
use opentelemetry_sdk::{Resource, metrics::SdkMeterProvider};
use runtime::{Runtime, auth::EndpointAuth, config::Config, datafusion::query::QueryBuilder};
use spicepod::param::Params;
use spicepod::{
    acceleration::{Acceleration, Mode, RefreshMode},
    component::caching::{Caching, SQLResultsCacheConfig},
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
    "process_resident_anon_bytes",
    "process_resident_file_bytes",
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

/// A Cayenne file-mode dataset whose background maintenance tick fires fast
/// enough for a test to observe one pass.
///
/// The footprint sample runs on that tick, so the interval has to be short; its
/// own 30-second floor does not suppress the FIRST sample, which is the one this
/// test reads.
///
/// Not built on Windows: the engine crate is a `cfg(not(windows))`
/// dev-dependency, so the `cayenne` accelerator is unregistered there and a
/// dataset naming it could never load.
#[cfg(not(windows))]
fn cayenne_backed_dataset(dir: &std::path::Path, name: &str) -> Dataset {
    let csv = fixture_csv(dir, name);
    write_fixture_csv(&csv, FIXTURE_EPOCH_SECONDS);

    let mut dataset = Dataset::new(format!("file://{}", csv.display()), name);
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("cayenne".to_string()),
        mode: Mode::File,
        refresh_mode: Some(RefreshMode::Full),
        params: Some(Params::from_string_map(
            [
                (
                    "cayenne_file_path".to_string(),
                    dir.join(format!("{name}-cayenne")).display().to_string(),
                ),
                (
                    "cayenne_compaction_background_interval_ms".to_string(),
                    "250".to_string(),
                ),
            ]
            .into_iter()
            .collect(),
        )),
        ..Acceleration::default()
    });
    dataset
}

/// Cayenne's maintenance and footprint families that a loaded dataset reports
/// with no workload at all — every one reachable from the background sample or a
/// decision the fixture's own tick makes.
///
/// Presence is the whole assertion: each is emitted from a decision point or a
/// sample that had no instrumentation at all, so a refactor that drops the call
/// site leaves the operator back where they started — reading debug logs — with
/// nothing failing. The list is therefore exhaustive over the always-sampled
/// families; the ones that need a specific event are in
/// [`EVENT_GATED_CAYENNE_MAINTENANCE_METRICS`], and the test asserts they are
/// *absent* so neither list can silently go stale.
#[cfg(not(windows))]
const EXPECTED_CAYENNE_MAINTENANCE_METRICS: &[&str] = &[
    "cayenne_compaction_outcome_total",
    "cayenne_maintenance_outcome_total",
    "cayenne_maintenance_reclaimed_files_total",
    "cayenne_maintenance_reclaimed_bytes_total",
    "cayenne_maintenance_reclaimed_rows_total",
    // Deliberately a family of its own: it counts rows a pass MARKED deleted,
    // the opposite event to the tombstone removal `reclaimed_rows` counts, so
    // one summable series cannot hold both. Registered at zero with the reclaim
    // family, so its absence here means the call site or the registration went
    // missing rather than that no retention pass has run.
    "cayenne_maintenance_tombstoned_rows_total",
    "cayenne_deletion_index_len",
    "cayenne_deletion_index_reinserts",
    "cayenne_deletion_index_bytes",
    "cayenne_pk_index_format",
    "cayenne_pk_index_keys",
    "cayenne_pk_index_bytes",
    "cayenne_pk_index_budget_bytes",
    "cayenne_storage_files",
    "cayenne_storage_bytes",
    "cayenne_storage_rows",
    "cayenne_snapshot_manifest_rows",
    "cayenne_metastore_table_rows",
    "cayenne_memory_account_bytes",
    "cayenne_mem_tier_bytes",
    "cayenne_scan_file_statistics_entries",
    "cayenne_memory_account_reserved_bytes",
    "cayenne_inline_cache_bytes",
    "cayenne_inline_cache_batches",
    "cayenne_data_dir_bytes",
    "cayenne_data_dir_files",
    "cayenne_data_dir_snapshot_dirs",
    "cayenne_metastore_db_bytes",
    "cayenne_metastore_wal_bytes",
    "cayenne_autotune_bake_deletion_index_trigger",
    // Zeroed rather than skipped while the index is exact, which is what makes
    // them readable against `cayenne_pk_index_format` instead of retaining a
    // stale bloom shape.
    "cayenne_pk_bloom_bits",
    "cayenne_pk_bloom_insertions",
    "cayenne_pk_bloom_bits_per_insertion",
];

/// Families that need an event a freshly-loaded, unwritten dataset cannot
/// produce, with what each one waits for.
///
/// Kept as a list rather than a comment because the test asserts they are absent:
/// if one starts arriving without a workload it has become always-sampled, and
/// leaving it here would mean nothing ever checks that its call site survives.
#[cfg(not(windows))]
const EVENT_GATED_CAYENNE_MAINTENANCE_METRICS: &[&str] = &[
    // Recorded when a threshold actually fires. This fixture holds one inline
    // file, so every pass declines before any trigger is evaluated.
    "cayenne_compaction_trigger_total",
    // Recorded at an encode decision; the fixture's write takes the inline path.
    "cayenne_write_shape_shards",
    // Require a keyset to be dropped or carried across a compaction.
    "cayenne_pk_index_discard_total",
    "cayenne_pk_index_preserved_total",
    // Requires a bloom-mode apply to split a batch.
    "cayenne_pk_bloom_split_rows_total",
];

/// The value of counter `name` on the series carrying every label in `labels`,
/// or `None` when no series does.
fn counter_value(
    registry: &prometheus::Registry,
    name: &str,
    labels: &[(&str, &str)],
) -> Option<f64> {
    registry
        .gather()
        .into_iter()
        .filter(|family| family.name() == name)
        .flat_map(|family| family.get_metric().to_vec())
        .find(|series| {
            labels.iter().all(|(key, value)| {
                series
                    .get_label()
                    .iter()
                    .any(|label| label.name() == *key && label.value() == *value)
            })
        })
        .map(|series| series.get_counter().value())
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
    telemetry::track_process_resident_split(640, 384, &[]);
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
    tokio::time::timeout(Duration::from_mins(1), notifier.wait())
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

/// Cayenne's maintenance and footprint families must reach `/metrics`.
///
/// They are emitted from decision points and a background sample that had no
/// instrumentation at all, and the failure mode is silent: drop the call site
/// and the operator is back to reading debug logs with nothing failing. So this
/// asserts the families arrive, which is the one thing a unit test on the label
/// vocabulary cannot cover.
///
/// The dataset sets a 250 ms compaction interval because the footprint sample
/// rides that tick. Its own 30-second floor does not suppress the first sample,
/// which is the one read here.
#[cfg(not(windows))]
#[tokio::test]
async fn a_cayenne_dataset_reports_its_maintenance_and_footprint_families() {
    let registry = &*PROMETHEUS;

    let dir = tempfile::tempdir().expect("a temporary directory for the fixture");
    let app = AppBuilder::new("metrics_cayenne_dataset")
        .with_dataset(cayenne_backed_dataset(dir.path(), "cayenne_scores"))
        .with_runtime(SpicepodRuntime {
            task_history: TaskHistory {
                enabled: false,
                ..Default::default()
            },
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

    // Poll for the families rather than sleeping the tick out: the sample fires
    // on a background wake whose exact timing is not ours to predict, and a
    // fixed sleep would either flake or be needlessly slow.
    let arrived = wait_until(Duration::from_mins(1), || async {
        let reported = reported_metric_names(registry);
        EXPECTED_CAYENNE_MAINTENANCE_METRICS
            .iter()
            .all(|metric| reported.contains(*metric))
    })
    .await;
    let reported = reported_metric_names(registry);
    assert!(
        arrived,
        "a loaded Cayenne dataset did not report its maintenance and footprint families"
    );
    assert_all_reported(
        &reported,
        EXPECTED_CAYENNE_MAINTENANCE_METRICS,
        "a loaded Cayenne dataset did not report its maintenance and footprint families",
    );

    // The other half of completeness: every family this PR's instrumentation can
    // emit is classified as either always-sampled (above) or event-gated (below),
    // and an event-gated family appearing here means the classification has gone
    // stale — the family is now always-sampled and belongs in the asserted set,
    // where a dropped call site would fail.
    let unexpectedly_present: Vec<&&str> = EVENT_GATED_CAYENNE_MAINTENANCE_METRICS
        .iter()
        .filter(|metric| reported.contains(**metric))
        .collect();
    assert!(
        unexpectedly_present.is_empty(),
        "these families are documented as needing an event this fixture cannot produce, but \
         arrived anyway — move them into EXPECTED_CAYENNE_MAINTENANCE_METRICS so a dropped \
         call site fails: {unexpectedly_present:?}"
    );

    // Presence of the family is not enough for the decision telemetry: its whole
    // value is the `outcome` label, and a call site rewritten to emit a bare
    // counter would still satisfy the check above. This fixture's background tick
    // deterministically declines the current-snapshot pass (one inline file, no
    // Vortex files to compact), so assert that exact decline is what arrives.
    let outcomes = compaction_outcomes(registry);
    let declines = outcomes
        .get(&(
            "cayenne_scores".to_string(),
            "subset_current".to_string(),
            "declined_below_trigger".to_string(),
        ))
        .copied()
        .unwrap_or(0.0);
    assert!(
        declines > 0.0,
        "the background tick must report WHY it declined, not just that it ran: this fixture \
         holds one inline file and no Vortex files, so every pass declines below trigger. \
         Observed: {outcomes:?}"
    );
    // Nothing in this fixture can commit a compaction, so a `committed` sample
    // would mean the outcome labels are being attached to the wrong exits.
    assert!(
        !outcomes
            .keys()
            .any(|(_, _, outcome)| outcome == "committed"),
        "no compaction can commit against a single inline file. Observed: {outcomes:?}"
    );
}

/// `cayenne_compaction_outcome_total` counts, keyed by `(table, kind, outcome)`.
///
/// The table label is part of the key because the registry is shared across the
/// tests in this file, so a sibling fixture's series must not satisfy an
/// assertion about this one.
#[cfg(not(windows))]
fn compaction_outcomes(registry: &prometheus::Registry) -> HashMap<(String, String, String), f64> {
    registry
        .gather()
        .iter()
        .filter(|family| family.name() == "cayenne_compaction_outcome_total")
        .flat_map(|family| {
            family.get_metric().iter().map(|metric| {
                let label = |name: &str| {
                    metric
                        .get_label()
                        .iter()
                        .find(|l| l.name() == name)
                        .map(|l| l.value().to_string())
                        .unwrap_or_default()
                };
                (
                    (label("table"), label("kind"), label("outcome")),
                    metric.get_counter().value(),
                )
            })
        })
        .collect()
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

/// Whether a series was recorded for a request whose user agent carries `token`.
///
/// Each test that counts a delta on this binary's shared registry sends its own
/// user agent and reads only its own series, so a sibling running in the same
/// process cannot move its count.
fn sent_by(metric: &prometheus::proto::Metric, token: &str) -> bool {
    metric
        .get_label()
        .iter()
        .any(|label| label.name() == "user_agent" && label.value().contains(token))
}

/// The user agent `each_flight_rpc_records_exactly_one_request` sends.
const FLIGHT_RPC_COUNT_USER_AGENT: &str = "flight-rpc-count-test/1.0";

/// Every `flight_requests` increment the client with user agent `token` caused,
/// summed across label sets.
///
/// Summed rather than read per-series because a sample's labels depend on where
/// it is recorded: the handler that knows the command adds a `command` label the
/// `FlightService` impl cannot. A per-series read would pass a duplicate off as a
/// relabelling.
fn flight_requests_total(registry: &prometheus::Registry, token: &str) -> f64 {
    registry
        .gather()
        .iter()
        .filter(|family| family.name() == "flight_requests")
        .flat_map(prometheus::proto::MetricFamily::get_metric)
        .filter(|metric| sent_by(metric, token))
        .map(|metric| metric.get_counter().value())
        .sum()
}

/// Asserts `rpc` recorded exactly one `flight_requests` increment.
///
/// `#[track_caller]` so a failure names the RPC rather than this line.
#[track_caller]
fn assert_recorded_once(registry: &prometheus::Registry, before: f64, rpc: &str) {
    let recorded = flight_requests_total(registry, FLIGHT_RPC_COUNT_USER_AGENT) - before;
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
    let (_http_port, channel) = start_servers(FLIGHT_RPC_COUNT_USER_AGENT).await?;
    Ok(channel)
}

/// Starts a runtime with no datasets and returns its HTTP port and a channel to
/// its Flight server that sends `user_agent`.
async fn start_servers(user_agent: &str) -> Result<(u16, Channel), anyhow::Error> {
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
    let endpoint =
        Channel::from_shared(format!("http://127.0.0.1:{flight_port}"))?.user_agent(user_agent)?;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        match endpoint.clone().connect().await {
            Ok(channel) => return Ok((http_port, channel)),
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
/// The delta assertions are safe on this binary's shared registry because they
/// count only the series this test's own user agent produced.
#[tokio::test]
async fn each_flight_rpc_records_exactly_one_request() -> Result<(), anyhow::Error> {
    let registry = &*PROMETHEUS;
    let channel = start_flight_server().await?;
    let mut client = FlightClient::new(channel);

    // One increment, under one method name: the service impl labelled this
    // `do_handshake` while the handler labels it `handshake`.
    let before = flight_requests_total(registry, FLIGHT_RPC_COUNT_USER_AGENT);
    client
        .handshake("flight_request_metrics")
        .await
        .map_err(|e| anyhow::anyhow!("handshake: {e}"))?;
    assert_recorded_once(registry, before, "handshake");

    // A ticket that is not a FlightSQL command is served by `do_get_simple`. The
    // stream is drained because the handler's measurement spans the drain; the
    // counter increments before it, so this pins the ordering the histogram
    // depends on.
    let before = flight_requests_total(registry, FLIGHT_RPC_COUNT_USER_AGENT);
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

    let before = flight_requests_total(registry, FLIGHT_RPC_COUNT_USER_AGENT);
    client
        .get_schema(FlightDescriptor::new_cmd("SELECT 1"))
        .await
        .map_err(|e| anyhow::anyhow!("get_schema: {e}"))?;
    assert_recorded_once(registry, before, "get_schema");

    let before = flight_requests_total(registry, FLIGHT_RPC_COUNT_USER_AGENT);
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
    let before = flight_requests_total(registry, FLIGHT_RPC_COUNT_USER_AGENT);
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

/// The value of the series of counter `name` whose `label` is `value`, or 0 when it
/// is not exported.
fn counter_with_label(
    registry: &prometheus::Registry,
    name: &str,
    label: &str,
    value: &str,
) -> f64 {
    registry
        .gather()
        .iter()
        .filter(|family| family.name() == name)
        .flat_map(prometheus::proto::MetricFamily::get_metric)
        .filter(|metric| {
            metric
                .get_label()
                .iter()
                .any(|pair| pair.name() == label && pair.value() == value)
        })
        .map(|metric| metric.get_counter().value())
        .sum()
}

/// Asserts a counter moved by exactly `expected`. Counter values are whole numbers
/// held in an `f64`, so the comparison is exact up to rounding.
fn assert_counter_delta(recorded: f64, expected: f64, what: &str) {
    assert!(
        (recorded - expected).abs() < f64::EPSILON,
        "{what}: recorded {recorded}, expected {expected}"
    );
}

/// The sum of every series of a counter family.
fn counter_total(registry: &prometheus::Registry, name: &str) -> f64 {
    registry
        .gather()
        .iter()
        .filter(|family| family.name() == name)
        .flat_map(prometheus::proto::MetricFamily::get_metric)
        .map(|metric| metric.get_counter().value())
        .sum()
}

/// A runtime serving the `scores` fixture, with the SQL results cache on and task
/// history off, loaded and ready.
async fn runtime_with_results_cache(dir: &std::path::Path, app_name: &str) -> Arc<Runtime> {
    runtime_with_results_cache_ttls(dir, app_name, "10m", None).await
}

/// [`runtime_with_results_cache`] with an explicit `item_ttl` and
/// `stale_while_revalidate_ttl`.
async fn runtime_with_results_cache_ttls(
    dir: &std::path::Path,
    app_name: &str,
    item_ttl: &str,
    stale_while_revalidate_ttl: Option<&str>,
) -> Arc<Runtime> {
    let app = AppBuilder::new(app_name)
        .with_dataset(csv_backed_dataset(dir, "scores"))
        .with_runtime(SpicepodRuntime {
            caching: Caching {
                sql_results: Some(SQLResultsCacheConfig {
                    item_ttl: Some(item_ttl.to_string()),
                    stale_while_revalidate_ttl: stale_while_revalidate_ttl.map(str::to_string),
                    ..Default::default()
                }),
                ..Default::default()
            },
            task_history: TaskHistory {
                enabled: false,
                ..Default::default()
            },
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
    rt
}

/// A query looks the results cache up once, however it is answered.
///
/// The lookup happens before anything is planned, and a miss found there is handed
/// to the planned path so it does not look the same key up again. Getting that
/// handoff wrong changes no response — every query still returns the right rows —
/// and shows only here, as each miss counted twice.
///
/// The early lookup is made under SQL keys from a query's first run, but under plan
/// keys only once the plan is cached, so each is exercised where it looks up: a
/// SQL-keyed query from its first run, and a plan-keyed query whose result was
/// invalidated while its plan stayed cached.
///
/// Counts compare before/after deltas, which only `cargo nextest` isolates by giving
/// the test its own process.
#[tokio::test]
async fn a_query_counts_one_results_cache_lookup() {
    use runtime_request_context::{CacheControl, CacheKeyType, Protocol, RequestContext};

    let registry = &*PROMETHEUS;

    let dir = tempfile::tempdir().expect("a temporary directory for the fixture");
    let rt = runtime_with_results_cache(dir.path(), "metrics_results_cache_lookups").await;
    let context = |key_type| {
        Arc::new(
            RequestContext::builder(Protocol::Http)
                .with_cache_control(CacheControl::Cache(key_type))
                .build(),
        )
    };
    let miss_then_hit = [
        (CacheStatus::CacheMiss, [1.0, 0.0, 1.0]),
        (CacheStatus::CacheHit, [1.0, 1.0, 0.0]),
    ];

    // SQL keys: looked up early from the query's first run.
    let sql_keyed = context(CacheKeyType::Raw);
    for (status, recorded) in miss_then_hit {
        assert_lookups(
            registry,
            &rt,
            &sql_keyed,
            "SELECT id, score FROM scores WHERE id >= 0",
            status,
            recorded,
        )
        .await;
    }

    // Plan keys: looked up early only once the plan is cached. The first run caches the
    // plan and the result; invalidating the result keeps the plan, so the runs after it
    // look up early.
    let plan_keyed = context(CacheKeyType::Default);
    let sql = "SELECT id, score FROM scores";
    assert_lookups(
        registry,
        &rt,
        &plan_keyed,
        sql,
        CacheStatus::CacheMiss,
        [1.0, 0.0, 1.0],
    )
    .await;
    rt.datafusion()
        .results_cache_provider()
        .expect("the results cache is configured")
        .invalidate_for_table(datafusion::common::TableReference::bare("scores"))
        .await
        .expect("the cached result to be invalidated");
    for (status, recorded) in miss_then_hit {
        assert_lookups(registry, &rt, &plan_keyed, sql, status, recorded).await;
    }
}

/// A request whose `max-stale` is narrower than the entry's age gets a miss, and the
/// lookup is counted as one: a hit there would claim a result the request never got.
///
/// The entry is older than `item_ttl` (1s) plus the request's `max-stale` (1s) but
/// well inside the server's stale window (60s), so only the request's own bound turns
/// it away. The sleep is the entry's age, which is what is under test.
#[tokio::test]
async fn a_request_max_stale_narrower_than_the_entry_age_counts_one_miss() {
    use runtime_request_context::{CacheControl, CacheKeyType, Protocol, RequestContext};

    let registry = &*PROMETHEUS;
    let dir = tempfile::tempdir().expect("a temporary directory for the fixture");
    let rt = runtime_with_results_cache_ttls(
        dir.path(),
        "metrics_results_cache_request_max_stale",
        "1s",
        Some("60s"),
    )
    .await;
    let sql = "SELECT id, score FROM scores WHERE id >= 0";
    let populate = Arc::new(
        RequestContext::builder(Protocol::Http)
            .with_cache_control(CacheControl::Cache(CacheKeyType::Raw))
            .build(),
    );
    assert_lookups(
        registry,
        &rt,
        &populate,
        sql,
        CacheStatus::CacheMiss,
        [1.0, 0.0, 1.0],
    )
    .await;

    tokio::time::sleep(Duration::from_millis(2_500)).await;

    let narrow = Arc::new(
        RequestContext::builder(Protocol::Http)
            .with_cache_control(CacheControl::MaxStale(
                CacheKeyType::Raw,
                Some(Duration::from_secs(1)),
            ))
            .build(),
    );
    assert_lookups(
        registry,
        &rt,
        &narrow,
        sql,
        CacheStatus::CacheMiss,
        [1.0, 0.0, 1.0],
    )
    .await;
}

/// SQL results record only hard invalidations and table-clock rejections.
/// The Caching API makes the write/refresh sequence deterministic; the fixture
/// still runs a real accelerated dataset and exports through the runtime reader.
#[tokio::test]
async fn results_cache_invalidation_metrics_contract() {
    use cache::key::RawCacheKey;
    use datafusion::common::TableReference;

    let registry = &*PROMETHEUS;
    let dir = tempfile::tempdir().expect("a temporary directory for the fixture");
    let rt = runtime_with_results_cache(dir.path(), "metrics_invalidation_contract").await;
    let df = rt.datafusion();
    let provider = df
        .results_cache_provider()
        .expect("SQL results cache configured");
    let table = TableReference::bare("scores");
    let mut query = QueryBuilder::new("SELECT id, score FROM scores", Arc::clone(&df))
        .build()
        .run()
        .await
        .expect("query the accelerated fixture");
    assert_eq!(query.cache_status, CacheStatus::CacheMiss);
    let mut batches = Vec::new();
    while let Some(batch) = query.data.next().await {
        batches.push(batch.expect("read the fixture batch"));
    }
    let schema = batches.first().expect("fixture returns a batch").schema();
    let key = RawCacheKey::new(u64::MAX);
    let read_started_at = std::time::Instant::now();
    let captured = CachedQueryResult::new_raw(
        batches,
        schema,
        Arc::new(HashSet::from([table.clone()])),
        read_started_at,
        read_started_at,
    );
    provider
        .put_raw_key(&key, captured.clone())
        .await
        .expect("store captured result");
    assert_eq!(
        provider
            .get_raw_key(&key)
            .await
            .expect("lookup captured result")
            .expect("captured result is initially servable")
            .read_started_at,
        read_started_at
    );

    let invalidations_before = counter_total(registry, "results_cache_table_invalidations");
    let evicted_before =
        counter_with_label(registry, "results_cache_evictions", "reason", "invalidated");
    let rejections_before = counter_total(registry, "results_cache_stale_rejections");
    df.caching()
        .invalidate_for_table(table.clone())
        .await
        .expect("invalidate for client write");
    assert_counter_delta(
        counter_total(registry, "results_cache_table_invalidations") - invalidations_before,
        1.0,
        "a client write records exactly one eviction invalidation",
    );
    // Two entries read `scores`: the result the fixture query above stored, and the
    // captured one. Each is counted as its own eviction.
    assert_counter_delta(
        counter_with_label(registry, "results_cache_evictions", "reason", "invalidated")
            - evicted_before,
        2.0,
        "every entry a client write drops is counted as an eviction with reason `invalidated`",
    );
    df.caching()
        .invalidate_for_refresh(table)
        .await
        .expect("invalidate for accelerated refresh");
    assert_counter_delta(
        counter_total(registry, "results_cache_table_invalidations") - invalidations_before,
        1.0,
        "an accelerated refresh must not record a SQL results invalidation",
    );

    // Publish the pre-write read after eviction, reproducing an in-flight read
    // that completes after a client write. Lookup must reject it by the clock.
    provider
        .put_raw_key(&key, captured)
        .await
        .expect("publish pre-write read");
    assert!(
        provider
            .get_raw_key(&key)
            .await
            .expect("lookup invalidated read")
            .is_none()
    );
    assert_counter_delta(
        counter_total(registry, "results_cache_stale_rejections") - rejections_before,
        1.0,
        "a read published after a client write is rejected once as `write_during_read`",
    );
    for (name, label_name, expected_value) in [
        ("results_cache_table_invalidations", "mode", "evict"),
        (
            "results_cache_stale_rejections",
            "reason",
            "write_during_read",
        ),
    ] {
        let families = registry.gather();
        let family = families
            .iter()
            .find(|family| family.name() == name)
            .expect("the metric family is exported");
        assert_eq!(
            family.get_metric().len(),
            1,
            "only the current contract series exists for {name}"
        );
        let labels: Vec<_> = family.get_metric()[0]
            .get_label()
            .iter()
            .filter(|label| label.name() == label_name)
            .map(prometheus::proto::LabelPair::value)
            .collect();
        assert_eq!(
            labels,
            [expected_value],
            "{name} exports the exact label value"
        );
    }
    assert!(
        registry
            .gather()
            .iter()
            .all(|family| family.name() != "results_cache_invalidation_stale_hits")
    );
}

/// Runs `sql` under `request_context`, reads its whole result, and asserts how it was
/// answered and the `[requests, hits, misses]` it added to the results cache counters.
async fn assert_lookups(
    registry: &prometheus::Registry,
    rt: &Arc<Runtime>,
    request_context: &Arc<runtime_request_context::RequestContext>,
    sql: &str,
    expected_status: CacheStatus,
    expected: [f64; 3],
) {
    let lookups = || {
        [
            "results_cache_requests",
            "results_cache_hits",
            "results_cache_misses",
        ]
        .map(|name| counter_total(registry, name))
    };

    let before = lookups();
    let status = Arc::clone(request_context)
        .scope(async {
            let mut result = QueryBuilder::new(sql, rt.datafusion())
                .build()
                .run()
                .await
                .expect("query to run");
            while let Some(batch) = result.data.next().await {
                batch.expect("batch to stream without error");
            }
            result.cache_status
        })
        .await;
    let after = lookups();

    assert_eq!(status, expected_status, "{sql}");
    let recorded = [
        after[0] - before[0],
        after[1] - before[1],
        after[2] - before[2],
    ];
    assert!(
        recorded
            .iter()
            .zip(expected)
            .all(|(got, want)| (got - want).abs() < f64::EPSILON),
        "a {expected_status:?} of {sql} must be one results cache request, recorded [requests, hits, misses] = {recorded:?}, expected {expected:?}"
    );
}

/// The number of cache hits `query_duration_ms` has recorded, and the milliseconds
/// they add up to, over every series.
fn cache_hit_query_durations(registry: &prometheus::Registry) -> (u64, f64) {
    registry
        .gather()
        .iter()
        .filter(|family| family.name() == "query_duration_ms")
        .flat_map(prometheus::proto::MetricFamily::get_metric)
        .filter(|metric| {
            metric
                .get_label()
                .iter()
                .any(|label| label.name() == "tags" && label.value() == "cache-hit")
        })
        .fold((0, 0.0), |(count, sum), metric| {
            let histogram = metric.get_histogram();
            (
                count + histogram.get_sample_count(),
                sum + histogram.get_sample_sum(),
            )
        })
}

/// How long a slow caller waits before reading a cache hit it has been served.
const SLOW_READER_DELAY: Duration = Duration::from_millis(250);

/// The number of observations and the sum of a histogram family.
fn histogram_count_and_sum(registry: &prometheus::Registry, name: &str) -> (u64, f64) {
    registry
        .gather()
        .iter()
        .filter(|family| family.name() == name)
        .flat_map(prometheus::proto::MetricFamily::get_metric)
        .fold((0, 0.0), |(count, sum), metric| {
            let histogram = metric.get_histogram();
            (
                count + histogram.get_sample_count(),
                sum + histogram.get_sample_sum(),
            )
        })
}

/// A cache hit's duration and returned-output counters are recorded when the
/// result stream is consumed, matching a miss.
///
/// Finalizing at serve time would change `query_duration_ms` and the
/// task-history duration, and would add every cached batch to
/// `query_returned_rows` / `query_returned_bytes` before HTTP or Flight read
/// them. Both are user-facing contracts; neither changes without an Enhancement.
///
/// Counts compare before/after deltas, which only `cargo nextest` isolates by giving
/// the test its own process.
#[tokio::test]
async fn a_cache_hit_is_recorded_when_the_stream_is_consumed() {
    let registry = &*PROMETHEUS;

    let dir = tempfile::tempdir().expect("a temporary directory for the fixture");
    let rt = runtime_with_results_cache(dir.path(), "metrics_served_cache_hit").await;
    let run = || {
        QueryBuilder::new("SELECT id, score FROM scores", rt.datafusion())
            .build()
            .run()
    };

    let mut miss = run().await.expect("query to run");
    assert_eq!(miss.cache_status, CacheStatus::CacheMiss);
    while let Some(batch) = miss.data.next().await {
        batch.expect("batch to stream without error");
    }

    let (hits_before, ms_before) = cache_hit_query_durations(registry);
    let (rows_before, rows_sum_before) = histogram_count_and_sum(registry, "query_returned_rows");
    let bytes_before = counter_total(registry, "query_returned_bytes");
    let mut hit = run().await.expect("query to run");
    assert_eq!(hit.cache_status, CacheStatus::CacheHit);
    let (hits_served, _) = cache_hit_query_durations(registry);
    let (rows_served, _) = histogram_count_and_sum(registry, "query_returned_rows");
    let bytes_served = counter_total(registry, "query_returned_bytes");
    assert_eq!(
        hits_served - hits_before,
        0,
        "a cache hit must not record query_duration_ms until its stream is consumed"
    );
    assert_eq!(
        rows_served - rows_before,
        0,
        "a cache hit must not record query_returned_rows until its stream is consumed"
    );
    assert!(
        (bytes_served - bytes_before).abs() < f64::EPSILON,
        "a cache hit must not record query_returned_bytes until its stream is consumed"
    );

    // The caller's delay is the behavior under test, so it is a fixed sleep.
    tokio::time::sleep(SLOW_READER_DELAY).await;
    let mut rows = 0;
    while let Some(batch) = hit.data.next().await {
        rows += batch.expect("batch to stream without error").num_rows();
    }
    assert!(rows > 0, "the hit must return the fixture's rows");

    let (hits_read, ms_read) = cache_hit_query_durations(registry);
    let (rows_read, rows_sum_read) = histogram_count_and_sum(registry, "query_returned_rows");
    let bytes_read = counter_total(registry, "query_returned_bytes");
    assert_eq!(
        hits_read - hits_before,
        1,
        "consuming a cache hit must record query_duration_ms once"
    );
    assert!(
        rows_read > rows_before,
        "consuming a cache hit must record query_returned_rows"
    );
    assert!(
        rows_sum_read - rows_sum_before
            >= f64::from(u32::try_from(rows).expect("the fixture's row count fits in a u32")),
        "query_returned_rows recorded {} rows, expected at least the {rows} the caller read",
        rows_sum_read - rows_sum_before
    );
    assert!(
        bytes_read - bytes_before > 0.0,
        "consuming a cache hit must record query_returned_bytes"
    );
    let recorded_ms = ms_read - ms_before;
    assert!(
        recorded_ms >= SLOW_READER_DELAY.as_secs_f64() * 1000.0,
        "the hit recorded {recorded_ms}ms, missing the {SLOW_READER_DELAY:?} its caller took to read it"
    );
}

/// A Cayenne refresh reports the rows it received but did not keep — each copy
/// of a key settled by arrival, identical or not — as `arrival`, while
/// `rows_written` counts every row received. A dataset with a primary key
/// publishes `arrival` at zero at load, before its first repeated key; one
/// without a primary key publishes no series.
#[cfg(not(windows))]
#[tokio::test(flavor = "multi_thread")]
async fn a_cayenne_refresh_reports_the_rows_it_supersedes_by_reason() {
    let registry = &*PROMETHEUS;
    let dir = tempfile::tempdir().expect("a temporary directory for the fixture");
    let keyed = |name: &str, primary_key: Option<&str>, csv: &str| {
        let path = dir.path().join(format!("{name}.csv"));
        std::fs::write(&path, csv).expect("write the fixture CSV");
        let mut dataset = Dataset::new(format!("file://{}", path.display()), name);
        dataset.acceleration = Some(Acceleration {
            enabled: true,
            engine: Some("cayenne".to_string()),
            mode: Mode::File,
            refresh_mode: Some(RefreshMode::Full),
            params: Some(Params::from_string_map(
                [(
                    "cayenne_file_path".to_string(),
                    dir.path()
                        .join(format!("{name}-cayenne"))
                        .display()
                        .to_string(),
                )]
                .into_iter()
                .collect(),
            )),
            primary_key: primary_key.map(ToString::to_string),
            ..Acceleration::default()
        });
        dataset
    };
    // 8,192 distinct keys fill the first record batch; the second repeats key 0
    // unchanged and key 1 with a new value.
    let repeated: String = std::iter::once("id,v\n".to_string())
        .chain((0..8_192).map(|id| format!("{id},first\n")))
        .chain(["0,first\n".to_string(), "1,second\n".to_string()])
        .collect();
    let app = AppBuilder::new("metrics_superseded_rows")
        .with_dataset(keyed("superseded_arrival", Some("id"), &repeated))
        .with_dataset(keyed("superseded_unique", Some("id"), "id,v\n1,a\n2,b\n"))
        .with_dataset(keyed("superseded_keyless", None, "id,v\n1,a\n1,a\n"))
        .with_runtime(SpicepodRuntime {
            task_history: TaskHistory {
                enabled: false,
                ..Default::default()
            },
            ..Default::default()
        })
        .build();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);
    tokio::time::timeout(Duration::from_mins(1), Arc::clone(&rt).load_components())
        .await
        .expect("the datasets to load within a minute");
    assert!(
        wait_until(Duration::from_mins(1), || async { rt.status().is_ready() }).await,
        "the runtime never reported ready, so the datasets never loaded"
    );

    let superseded = |dataset: &str, reason: &str| {
        counter_value(
            registry,
            "dataset_acceleration_rows_superseded",
            &[("dataset", dataset), ("reason", reason)],
        )
    };
    assert_eq!(superseded("superseded_arrival", "arrival"), Some(2.0));
    assert_eq!(
        counter_value(
            registry,
            "dataset_acceleration_refresh_rows_written",
            &[("dataset", "superseded_arrival")],
        ),
        Some(8_194.0)
    );
    assert_eq!(superseded("superseded_unique", "arrival"), Some(0.0));
    assert_eq!(superseded("superseded_keyless", "arrival"), None);
}

/// A user's statement reports the rows it received but did not keep: an `INSERT`
/// repeating a key counts the copy that arrived first as `arrival`.
#[cfg(not(windows))]
#[tokio::test(flavor = "multi_thread")]
async fn a_cayenne_statement_reports_the_rows_it_supersedes() {
    use runtime_request_context::{Protocol, RequestContext};
    use spicepod::acceleration::OnConflictBehavior;
    use spicepod::component::access::AccessMode;

    let registry = &*PROMETHEUS;
    let dir = tempfile::tempdir().expect("a temporary directory for the fixture");
    let path = dir.path().join("statement.csv");
    std::fs::write(&path, "id,v\n1,seed\n").expect("write the fixture CSV");
    let mut dataset = Dataset::new(format!("file://{}", path.display()), "superseded_statement");
    dataset.access = AccessMode::ReadWrite;
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("cayenne".to_string()),
        mode: Mode::File,
        refresh_mode: Some(RefreshMode::Full),
        params: Some(Params::from_string_map(
            [(
                "cayenne_file_path".to_string(),
                dir.path().join("cayenne").display().to_string(),
            )]
            .into_iter()
            .collect(),
        )),
        primary_key: Some("id".to_string()),
        // Keeps the statement's writes in the acceleration.
        on_conflict: HashMap::from([("id".to_string(), OnConflictBehavior::Upsert)]),
        write_mode: spicepod::acceleration::WriteMode::Acceleration,
        ..Acceleration::default()
    });
    let app = AppBuilder::new("metrics_superseded_statement")
        .with_dataset(dataset)
        .with_runtime(SpicepodRuntime {
            task_history: TaskHistory {
                enabled: false,
                ..Default::default()
            },
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

    Arc::new(RequestContext::builder(Protocol::Http).build())
        .scope(async {
            let mut result = QueryBuilder::new(
                "INSERT INTO superseded_statement VALUES (2, 'a'), (2, 'b'), (3, 'c')",
                rt.datafusion(),
            )
            .build()
            .run()
            .await
            .expect("the insert to run");
            while let Some(batch) = result.data.next().await {
                batch.expect("the insert to complete");
            }
        })
        .await;

    assert_eq!(
        counter_value(
            registry,
            "dataset_acceleration_rows_superseded",
            &[("dataset", "superseded_statement"), ("reason", "arrival")],
        ),
        Some(1.0)
    );
}

/// The milliseconds in a `Server-Timing` value, which must be `total;dur=<ms>`.
#[track_caller]
fn server_timing_total(value: &str) -> f64 {
    value
        .strip_prefix("total;dur=")
        .and_then(|duration| duration.parse::<f64>().ok())
        .unwrap_or_else(|| panic!("expected `total;dur=<ms>`, got `{value}`"))
}

/// The `(count, sum)` of histogram `name` over the series the user agent
/// carrying `token` produced whose labels include every pair in `labels`.
fn histogram_sent_by(
    registry: &prometheus::Registry,
    name: &str,
    token: &str,
    labels: &[(&str, &str)],
) -> (u64, f64) {
    registry
        .gather()
        .iter()
        .filter(|family| family.name() == name)
        .flat_map(prometheus::proto::MetricFamily::get_metric)
        .filter(|metric| sent_by(metric, token))
        .filter(|metric| {
            labels.iter().all(|(name, value)| {
                metric
                    .get_label()
                    .iter()
                    .any(|label| label.name() == *name && label.value() == *value)
            })
        })
        .fold((0, 0.0), |(count, sum), metric| {
            let histogram = metric.get_histogram();
            (
                count + histogram.get_sample_count(),
                sum + histogram.get_sample_sum(),
            )
        })
}

/// The largest difference rounding a sample to the field's three decimals makes.
const SERVER_TIMING_ROUNDING_MS: f64 = 0.0005 + 1e-9;

/// `Server-Timing` reports durations the runtime already recorded, never a
/// second clock, so the field and the dashboards agree: the head of a buffered
/// `/v1/sql` response carries its `http_requests_duration_ms` sample, a
/// streamed one's trailer its `http_responses_duration_ms` sample, and a Flight
/// query's `DoGet` trailer the `flight_request_duration_ms` samples of its
/// `GetFlightInfo` and `DoGet` added together.
///
/// Each request sends its own user agent and only its own series are read, so
/// siblings on this binary's shared registry cannot move the counts.
#[tokio::test]
async fn server_timing_reports_the_recorded_duration_samples() -> Result<(), anyhow::Error> {
    const BUFFERED: &str = "server-timing-buffered/1.0";
    const STREAMED: &str = "server-timing-streamed/1.0";
    const FLIGHT: &str = "server-timing-flight/1.0";

    let registry = &*PROMETHEUS;
    let (http_port, channel) = start_servers(FLIGHT).await?;
    let sql = "SELECT 1 AS n";

    // A buffered format: the head carries the response-head sample.
    let response = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{http_port}/v1/sql"))
        .header("user-agent", BUFFERED)
        .header("accept", "text/csv")
        .body(sql)
        .send()
        .await?;
    let header = response
        .headers()
        .get("server-timing")
        .ok_or_else(|| anyhow::anyhow!("a buffered response must carry `Server-Timing`"))?
        .to_str()?
        .to_string();
    response.bytes().await?;
    let (count, sample) = histogram_sent_by(
        registry,
        "http_requests_duration_ms",
        BUFFERED,
        &[("path", "/v1/sql")],
    );
    assert_eq!(
        count, 1,
        "one request, one http_requests_duration_ms sample"
    );
    let reported = server_timing_total(&header);
    assert!(
        (reported - sample).abs() <= SERVER_TIMING_ROUNDING_MS,
        "the header reports {reported} ms but http_requests_duration_ms recorded {sample} ms"
    );

    // The streamed JSON body: the trailer carries the end-of-body sample.
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", http_port)).await?;
    tokio::io::AsyncWriteExt::write_all(
        &mut stream,
        format!(
            "POST /v1/sql HTTP/1.1\r\nHost: 127.0.0.1:{http_port}\r\nUser-Agent: {STREAMED}\r\nContent-Type: text/plain\r\nTE: trailers\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{sql}",
            sql.len()
        )
        .as_bytes(),
    )
    .await?;
    let mut raw = String::new();
    tokio::io::AsyncReadExt::read_to_string(&mut stream, &mut raw).await?;
    let trailer = raw
        .rsplit("\r\n0\r\n")
        .next()
        .and_then(|trailers| {
            trailers
                .lines()
                .find_map(|line| line.strip_prefix("server-timing: "))
        })
        .ok_or_else(|| anyhow::anyhow!("no `server-timing` trailer in `{raw}`"))?
        .to_string();
    let (count, sample) = histogram_sent_by(
        registry,
        "http_responses_duration_ms",
        STREAMED,
        &[("path", "/v1/sql"), ("outcome", "complete")],
    );
    assert_eq!(
        count, 1,
        "one response, one http_responses_duration_ms sample"
    );
    let reported = server_timing_total(&trailer);
    assert!(
        (reported - sample).abs() <= SERVER_TIMING_ROUNDING_MS,
        "the trailer reports {reported} ms but http_responses_duration_ms recorded {sample} ms"
    );

    // A Flight query: the DoGet trailer adds the GetFlightInfo sample to its own.
    let mut client = FlightClient::new(channel);
    let info = client
        .get_flight_info(FlightDescriptor::new_cmd(sql.to_string()))
        .await?;
    let ticket = info
        .endpoint
        .first()
        .and_then(|endpoint| endpoint.ticket.clone())
        .ok_or_else(|| anyhow::anyhow!("GetFlightInfo returned no ticket"))?;
    let mut results = client.do_get(ticket).await?;
    while results.try_next().await?.is_some() {}
    let trailer = results
        .trailers()
        .and_then(|trailers| {
            trailers
                .get("server-timing")
                .and_then(|value| value.to_str().ok().map(str::to_string))
        })
        .ok_or_else(|| anyhow::anyhow!("a query DoGet must end with a `server-timing` trailer"))?;
    let (planned, get_flight_info) = histogram_sent_by(
        registry,
        "flight_request_duration_ms",
        FLIGHT,
        &[("method", "get_flight_info")],
    );
    let (ran, do_get) = histogram_sent_by(
        registry,
        "flight_request_duration_ms",
        FLIGHT,
        &[("method", "do_get")],
    );
    assert_eq!((planned, ran), (1, 1), "one sample for each RPC");
    let reported = server_timing_total(&trailer);
    assert!(
        (reported - (get_flight_info + do_get)).abs() <= SERVER_TIMING_ROUNDING_MS,
        "the trailer reports {reported} ms but GetFlightInfo recorded {get_flight_info} ms and \
         DoGet {do_get} ms"
    );
    assert!(
        reported > do_get,
        "the trailer must include the GetFlightInfo time, not only DoGet's {do_get} ms"
    );

    Ok(())
}

/// How long [`start_slow_openai_stub`] takes to answer. A duration recorded in seconds
/// instead of milliseconds lands three orders of magnitude below it.
#[cfg(feature = "models")]
const STUB_PROVIDER_DELAY: Duration = Duration::from_millis(200);

/// An `OpenAI`-compatible provider that answers every chat completion and embedding
/// request after [`STUB_PROVIDER_DELAY`].
#[cfg(feature = "models")]
async fn start_slow_openai_stub() -> SocketAddr {
    use axum::{Json, Router, routing::post};
    use serde_json::{Value, json};

    async fn chat_completions(Json(body): Json<Value>) -> Json<Value> {
        tokio::time::sleep(STUB_PROVIDER_DELAY).await;
        Json(json!({
            "id": "chatcmpl-stub",
            "object": "chat.completion",
            "created": 0,
            "model": body["model"],
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "ok"},
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}
        }))
    }

    async fn embeddings(Json(body): Json<Value>) -> Json<Value> {
        tokio::time::sleep(STUB_PROVIDER_DELAY).await;
        let inputs = body["input"].as_array().map_or(1, Vec::len);
        let data: Vec<Value> = (0..inputs)
            .map(|index| json!({"object": "embedding", "index": index, "embedding": [0.1, 0.2, 0.3]}))
            .collect();
        Json(json!({
            "object": "list",
            "model": body["model"],
            "data": data,
            "usage": {"prompt_tokens": 1, "total_tokens": 1}
        }))
    }

    let listener = tokio::net::TcpListener::bind((LOCALHOST, 0))
        .await
        .expect("to bind the stub provider");
    let addr = listener.local_addr().expect("the stub provider's address");
    let router = Router::new()
        .route("/v1/chat/completions", post(chat_completions))
        .route("/v1/embeddings", post(embeddings));
    tokio::spawn(async move { axum::serve(listener, router).await });
    addr
}

/// The sample count and sum of histogram `name`, over all of its series.
#[cfg(feature = "models")]
fn histogram_totals(registry: &prometheus::Registry, name: &str) -> (u64, f64) {
    registry
        .gather()
        .iter()
        .filter(|family| family.name() == name)
        .flat_map(prometheus::proto::MetricFamily::get_metric)
        .fold((0, 0.0), |(count, sum), metric| {
            let histogram = metric.get_histogram();
            (
                count + histogram.get_sample_count(),
                sum + histogram.get_sample_sum(),
            )
        })
}

/// `llm_internal_request_duration_ms` and `embeddings_internal_request_duration_ms`
/// record milliseconds, as their names and units say: a request the provider takes
/// [`STUB_PROVIDER_DELAY`] to answer records at least that many milliseconds.
#[cfg(feature = "models")]
#[tokio::test]
async fn model_request_durations_are_recorded_in_milliseconds() {
    use spicepod::component::{embeddings::Embeddings, model::Model};

    let registry = &*PROMETHEUS;

    let endpoint = format!("http://{}/v1", start_slow_openai_stub().await);
    let provider_params = || {
        HashMap::from([
            ("endpoint".to_string(), endpoint.clone().into()),
            (
                "openai_api_key".to_string(),
                "sk-not-a-real-key".to_string().into(),
            ),
        ])
    };
    let mut model = Model::new("openai:stub-chat", "stub_chat");
    model.params = provider_params();
    let mut embedding = Embeddings::new("openai:stub-embed", "stub_embed");
    embedding.params = provider_params();
    let app = AppBuilder::new("metrics_model_request_durations")
        .with_model(model)
        .with_embedding(embedding)
        .with_runtime(SpicepodRuntime {
            task_history: TaskHistory {
                enabled: false,
                ..Default::default()
            },
            ..Default::default()
        })
        .build();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);
    tokio::time::timeout(Duration::from_mins(1), Arc::clone(&rt).load_components())
        .await
        .expect("the model and embedding to load within a minute");
    assert!(
        wait_until(Duration::from_mins(1), || async { rt.status().is_ready() }).await,
        "the runtime never reported ready, so the model or embedding never loaded"
    );

    let chat = rt
        .completion_llms()
        .read()
        .await
        .get("stub_chat")
        .cloned()
        .expect("the chat model to be loaded");
    let embed = rt
        .embeds()
        .read()
        .await
        .get("stub_embed")
        .cloned()
        .expect("the embedding model to be loaded");

    let llm_before = histogram_totals(registry, "llm_internal_request_duration_ms");
    chat.chat_request(
        serde_json::from_value(serde_json::json!({
            "model": "stub_chat",
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .expect("a chat completion request"),
    )
    .await
    .expect("the stub to answer the chat completion");
    let llm_after = histogram_totals(registry, "llm_internal_request_duration_ms");

    let embeddings_before = histogram_totals(registry, "embeddings_internal_request_duration_ms");
    embed
        .embed(llms::embeddings::EmbeddingInput::String(
            "a text no other request embeds".to_string(),
        ))
        .await
        .expect("the stub to answer the embedding request");
    let embeddings_after = histogram_totals(registry, "embeddings_internal_request_duration_ms");

    let floor_ms = STUB_PROVIDER_DELAY.as_secs_f64() * 1000.0;
    for (name, before, after) in [
        ("llm_internal_request_duration_ms", llm_before, llm_after),
        (
            "embeddings_internal_request_duration_ms",
            embeddings_before,
            embeddings_after,
        ),
    ] {
        let (samples, recorded) = (after.0 - before.0, after.1 - before.1);
        assert_eq!(samples, 1, "{name}: one request records one sample");
        assert!(
            (floor_ms..60_000.0).contains(&recorded),
            "{name} recorded {recorded} for a request the provider took {floor_ms} ms to answer, \
             so it is not in milliseconds"
        );
    }
}

/// The `dataset_acceleration_snapshot_*` families must reach `/metrics` with the
/// right values: a published snapshot reports what it wrote, a bootstrap reports
/// what it downloaded, and `dataset_acceleration_snapshot_failure_count` counts
/// every snapshot a dataset gave up on, whichever step failed, from zero.
///
/// Built only with `snapshots`: without it every snapshot behavior resolves to
/// `Disabled` and nothing is created. Cayenne is the engine because its crate needs
/// no feature, so the gate's build runs these.
#[cfg(all(feature = "snapshots", not(windows)))]
mod snapshot_metrics {
    use std::{
        path::{Path, PathBuf},
        sync::{
            Arc, LazyLock,
            atomic::{AtomicI64, AtomicUsize, Ordering},
        },
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use app::AppBuilder;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use async_trait::async_trait;
    use datafusion::common::TableReference;
    use runtime::Runtime;
    use runtime_acceleration::{
        acceleration::{Acceleration as RuntimeAcceleration, RefreshMode as RuntimeRefreshMode},
        dataset_checkpoint::{self, DatasetCheckpointer},
        snapshot::{
            AccelerationEngine, AccelerationLayout, ForceCreate, SnapshotBehavior, SnapshotManager,
            engine::DefaultSnapshotEngine,
        },
    };
    use spicepod::{
        acceleration::{
            Acceleration, Mode, RefreshMode, SnapshotBehavior as DatasetSnapshotBehavior,
            SnapshotsCompaction,
        },
        component::{dataset::Dataset, snapshot::Snapshots},
        param::Params,
    };

    use super::{
        FIXTURE_EPOCH_SECONDS, PROMETHEUS, counter_value, fixture_csv, wait_until,
        write_fixture_csv,
    };

    const FAILURE_COUNT: &str = "dataset_acceleration_snapshot_failure_count";
    const SKIPPED_COUNT: &str = "dataset_acceleration_snapshot_skipped_count";
    const WRITE_TIMESTAMP: &str = "dataset_acceleration_snapshot_write_timestamp";
    const WRITE_DURATION_MS: &str = "dataset_acceleration_snapshot_write_duration_ms";
    const WRITE_BYTES: &str = "dataset_acceleration_snapshot_write_bytes";
    const WRITE_CHECKSUM: &str = "dataset_acceleration_snapshot_write_checksum";
    const BOOTSTRAP_DURATION_MS: &str = "dataset_acceleration_snapshot_bootstrap_duration_ms";
    const BOOTSTRAP_BYTES: &str = "dataset_acceleration_snapshot_bootstrap_bytes";
    const BOOTSTRAP_CHECKSUM: &str = "dataset_acceleration_snapshot_bootstrap_checksum";

    /// One attempt plus `SNAPSHOT_MAX_RETRIES` in `runtime_table`'s snapshot path.
    const SNAPSHOT_ATTEMPTS: usize = 4;

    fn failures(dataset: &str) -> Option<f64> {
        counter_value(&PROMETHEUS, FAILURE_COUNT, &[("dataset", dataset)])
    }

    /// Every series of `name` that carries `dataset`.
    fn series_for(name: &str, dataset: &str) -> Vec<prometheus::proto::Metric> {
        PROMETHEUS
            .gather()
            .iter()
            .filter(|family| family.name() == name)
            .flat_map(prometheus::proto::MetricFamily::get_metric)
            .filter(|series| {
                series
                    .get_label()
                    .iter()
                    .any(|label| label.name() == "dataset" && label.value() == dataset)
            })
            .cloned()
            .collect()
    }

    fn has_series_for(name: &str, dataset: &str) -> bool {
        !series_for(name, dataset).is_empty()
    }

    /// The value of gauge `name` for `dataset`, which must carry one series.
    fn gauge_for(name: &str, dataset: &str) -> f64 {
        let series = series_for(name, dataset);
        assert_eq!(
            series.len(),
            1,
            "{name} for '{dataset}' carries {} series",
            series.len()
        );
        series[0].get_gauge().value()
    }

    /// The `checksum` label of the one series of checksum gauge `name` for `dataset`.
    fn checksum_for(name: &str, dataset: &str) -> String {
        let series = series_for(name, dataset);
        assert_eq!(
            series.len(),
            1,
            "{name} for '{dataset}' carries {} series",
            series.len()
        );
        series[0]
            .get_label()
            .iter()
            .find(|label| label.name() == "checksum")
            .map(|label| label.value().to_string())
            .expect("a checksum gauge to carry its checksum label")
    }

    /// The sample count of histogram `name` for `dataset`.
    fn histogram_count_for(name: &str, dataset: &str) -> u64 {
        series_for(name, dataset)
            .iter()
            .map(|series| series.get_histogram().get_sample_count())
            .sum()
    }

    /// A snapshot location that opens, but where no snapshot archive can be
    /// written: archives go under a `month=YYYY-MM/` directory, and a regular file
    /// already holds that name. Tomorrow's month is blocked too, so a run across a
    /// month boundary still fails the same way.
    fn unwritable_location(dir: &Path) -> String {
        let snapshots = dir.join("snapshots");
        std::fs::create_dir_all(&snapshots).expect("to create the snapshot directory");
        let now = chrono::Utc::now();
        for instant in [now, now + chrono::Duration::days(1)] {
            let partition = snapshots.join(instant.format("month=%Y-%m").to_string());
            std::fs::write(&partition, b"not a directory")
                .expect("to block the snapshot partition with a file");
        }
        format!("file://{}/", snapshots.display())
    }

    fn writable_location(dir: &Path) -> String {
        let snapshots = dir.join("snapshots");
        std::fs::create_dir_all(&snapshots).expect("to create the snapshot directory");
        format!("file://{}/", snapshots.display())
    }

    fn snapshots_config(location: String) -> Snapshots {
        Snapshots {
            enabled: true,
            location: Some(location),
            ..Snapshots::default()
        }
    }

    /// A Cayenne file-mode dataset that publishes a snapshot after each refresh.
    fn snapshot_writer_dataset(dir: &Path, name: &str) -> Dataset {
        let csv = fixture_csv(dir, name);
        write_fixture_csv(&csv, FIXTURE_EPOCH_SECONDS);

        let mut dataset = Dataset::new(format!("file://{}", csv.display()), name);
        dataset.acceleration = Some(Acceleration {
            enabled: true,
            engine: Some("cayenne".to_string()),
            mode: Mode::File,
            refresh_mode: Some(RefreshMode::Full),
            snapshots: DatasetSnapshotBehavior::CreateOnly,
            params: Some(Params::from_string_map(
                [(
                    "cayenne_file_path".to_string(),
                    dir.join(format!("{name}-cayenne")).display().to_string(),
                )]
                .into_iter()
                .collect(),
            )),
            ..Acceleration::default()
        });
        dataset
    }

    async fn load_snapshot_writer(dir: &Path, name: &str, location: String) -> Arc<Runtime> {
        let app = AppBuilder::new(name)
            .with_snapshots(snapshots_config(location))
            .with_dataset(snapshot_writer_dataset(dir, name))
            .build();
        let rt = Arc::new(Runtime::builder().with_app(app).build().await);
        tokio::time::timeout(Duration::from_mins(1), Arc::clone(&rt).load_components())
            .await
            .expect("the dataset to load within a minute");
        rt
    }

    fn create_only(rt: &Runtime, location: String) -> SnapshotBehavior {
        SnapshotBehavior::create_only(
            Arc::new(snapshots_config(location)),
            rt.secrets_weak(),
            rt.tokio_io_runtime(),
            SnapshotsCompaction::Disabled,
        )
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    /// A local acceleration file for the snapshot engine to archive.
    fn acceleration_file(dir: &Path, name: &str) -> PathBuf {
        let path = dir.join(format!("{name}.db"));
        std::fs::write(&path, b"acceleration bytes").expect("to write the acceleration file");
        path
    }

    /// A checkpointer that records nothing and counts its checkpoint attempts,
    /// failing each one when `fail` is set.
    #[derive(Default)]
    struct StubCheckpointer {
        fail: bool,
        attempts: AtomicUsize,
    }

    #[async_trait]
    impl DatasetCheckpointer for StubCheckpointer {
        async fn exists(&self) -> bool {
            true
        }

        async fn checkpoint(
            &self,
            _schema: &SchemaRef,
            _refresh_sql: Option<&str>,
        ) -> dataset_checkpoint::Result<()> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                return Err("injected checkpoint failure".into());
            }
            Ok(())
        }

        async fn get_schema(&self) -> dataset_checkpoint::Result<Option<SchemaRef>> {
            Ok(None)
        }

        async fn last_checkpoint_time(&self) -> dataset_checkpoint::Result<Option<SystemTime>> {
            Ok(None)
        }

        async fn get_refresh_sql(&self) -> dataset_checkpoint::Result<Option<String>> {
            Ok(None)
        }

        async fn set_schema(&self, _schema: &SchemaRef) -> dataset_checkpoint::Result<()> {
            Ok(())
        }

        async fn delete(&self) -> dataset_checkpoint::Result<()> {
            Ok(())
        }
    }

    /// A published snapshot reports what it wrote and a failure count of zero,
    /// and a runtime bootstrapping from it reports downloading the same bytes.
    ///
    /// The bootstrap is checked against the write rather than on its own: the two
    /// sizes and checksums can only agree if both describe the same archive.
    #[tokio::test]
    async fn a_snapshot_reports_its_write_and_bootstrap_metrics() {
        // Before anything records: an instrument first touched without it binds to
        // the no-op provider for the whole process.
        LazyLock::force(&PROMETHEUS);
        let dataset = "snapshot_round_trip";
        let dir = tempfile::tempdir().expect("a temporary directory for the fixture");
        let location = writable_location(dir.path());
        let started_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("a clock after the epoch")
            .as_secs();

        let writer = load_snapshot_writer(dir.path(), dataset, location.clone()).await;
        assert!(
            wait_until(Duration::from_mins(1), || async {
                has_series_for(WRITE_TIMESTAMP, dataset)
            })
            .await,
            "dataset '{dataset}' never published a snapshot. Runtime ready: {}",
            writer.status().is_ready()
        );
        let finished_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("a clock after the epoch")
            .as_secs();

        assert_eq!(
            failures(dataset),
            Some(0.0),
            "a dataset that creates snapshots must report {FAILURE_COUNT} at zero"
        );
        #[expect(
            clippy::cast_precision_loss,
            reason = "epoch seconds fit an f64 exactly"
        )]
        let window = (started_secs as f64)..=(finished_secs as f64);
        let written_at = gauge_for(WRITE_TIMESTAMP, dataset);
        assert!(
            window.contains(&written_at),
            "{WRITE_TIMESTAMP} is {written_at}, outside the test's window {window:?}, so it \
             is not in epoch seconds"
        );
        assert_eq!(
            histogram_count_for(WRITE_DURATION_MS, dataset),
            1,
            "one published snapshot records one write duration"
        );
        let written_bytes = gauge_for(WRITE_BYTES, dataset);
        assert!(written_bytes > 0.0, "a published snapshot cannot be empty");
        let written_checksum = checksum_for(WRITE_CHECKSUM, dataset);
        assert!(
            !written_checksum.is_empty(),
            "a published snapshot carries its checksum"
        );

        // A second runtime with no local acceleration of its own restores from the
        // snapshot the first one wrote.
        let reader_dir = tempfile::tempdir().expect("a temporary directory for the reader");
        let mut reader_dataset = snapshot_writer_dataset(reader_dir.path(), dataset);
        if let Some(acceleration) = reader_dataset.acceleration.as_mut() {
            acceleration.snapshots = DatasetSnapshotBehavior::BootstrapOnly;
        }
        let app = AppBuilder::new("snapshot_round_trip_reader")
            .with_snapshots(snapshots_config(location))
            .with_dataset(reader_dataset)
            .build();
        let reader = Arc::new(Runtime::builder().with_app(app).build().await);
        tokio::time::timeout(
            Duration::from_mins(1),
            Arc::clone(&reader).load_components(),
        )
        .await
        .expect("the bootstrapping dataset to load within a minute");
        assert!(
            wait_until(Duration::from_mins(1), || async {
                has_series_for(BOOTSTRAP_BYTES, dataset)
            })
            .await,
            "dataset '{dataset}' never bootstrapped from the published snapshot. Runtime ready: {}",
            reader.status().is_ready()
        );

        assert!(
            (gauge_for(BOOTSTRAP_BYTES, dataset) - written_bytes).abs() < f64::EPSILON,
            "the bootstrap downloaded {} bytes of a snapshot that was written as {written_bytes}",
            gauge_for(BOOTSTRAP_BYTES, dataset)
        );
        assert_eq!(
            checksum_for(BOOTSTRAP_CHECKSUM, dataset),
            written_checksum,
            "the bootstrap must report the checksum of the snapshot it restored"
        );
        let bootstrap_ms =
            counter_value(&PROMETHEUS, BOOTSTRAP_DURATION_MS, &[("dataset", dataset)])
                .expect("a bootstrap to record its duration");
        assert!(
            (0.0..60_000.0).contains(&bootstrap_ms) && bootstrap_ms > 0.0,
            "{BOOTSTRAP_DURATION_MS} is {bootstrap_ms} for a local restore, so it is not in \
             milliseconds"
        );
    }

    /// Under the default `on_change` policy, a snapshot requested when nothing
    /// changed since the last one has nothing to publish: it is counted as
    /// skipped, and neither written nor failed. (The first snapshot of a dataset is
    /// always written, so one is published first.)
    #[tokio::test]
    async fn an_unchanged_snapshot_is_counted_as_skipped() {
        LazyLock::force(&PROMETHEUS);
        let dir = tempfile::tempdir().expect("a temporary directory for the fixture");
        let rt = Runtime::builder().build().await;

        let dataset = "snapshot_skipped";
        let manager = SnapshotManager::try_new(
            dataset.to_string(),
            create_only(&rt, writable_location(dir.path())),
            AccelerationLayout::file(acceleration_file(dir.path(), dataset)),
            AccelerationEngine::Cayenne,
        )
        .await
        .expect("a snapshot manager for a writable location");
        let checkpointer: Arc<dyn DatasetCheckpointer> = Arc::new(StubCheckpointer::default());

        let manager = Arc::new(manager);
        // The data's last change, as of both requests.
        let last_updated_at = Arc::new(AtomicI64::new(1_700_000_000_000));
        let write_mutex = Arc::new(tokio::sync::Mutex::new(()));
        let table = TableReference::bare(dataset);
        let schema = schema();
        let request = |force| {
            runtime_table::accelerated::snapshots::create_checkpoint_and_snapshot(
                &checkpointer,
                Some(&manager),
                &schema,
                &write_mutex,
                &table,
                &last_updated_at,
                ForceCreate(force),
                None,
                None,
                None,
            )
        };

        request(false).await;
        assert_eq!(
            histogram_count_for(WRITE_DURATION_MS, dataset),
            1,
            "the dataset's first snapshot must be published"
        );
        assert_eq!(
            counter_value(&PROMETHEUS, SKIPPED_COUNT, &[("dataset", dataset)]),
            None,
            "a published snapshot is not a skip"
        );

        request(false).await;
        assert_eq!(
            counter_value(&PROMETHEUS, SKIPPED_COUNT, &[("dataset", dataset)]),
            Some(1.0),
            "a snapshot with nothing new to publish is one skip"
        );
        assert_eq!(
            histogram_count_for(WRITE_DURATION_MS, dataset),
            1,
            "a skipped snapshot must not be reported as written"
        );
        assert_eq!(
            failures(dataset),
            None,
            "a skipped snapshot is not a failure"
        );
    }

    /// An upload that keeps failing is counted once for the snapshot, not once per
    /// retried attempt.
    #[tokio::test]
    async fn a_failed_snapshot_upload_is_counted() {
        LazyLock::force(&PROMETHEUS);
        let dataset = "snapshot_upload_fails";
        let dir = tempfile::tempdir().expect("a temporary directory for the fixture");
        let rt = load_snapshot_writer(dir.path(), dataset, unwritable_location(dir.path())).await;

        assert!(
            wait_until(Duration::from_mins(2), || async {
                failures(dataset).is_some_and(|count| count >= 1.0)
            })
            .await,
            "the failed upload was never counted: {FAILURE_COUNT} is {:?}. Runtime ready: {}",
            failures(dataset),
            rt.status().is_ready()
        );
        assert_eq!(
            failures(dataset),
            Some(1.0),
            "one snapshot that failed after its retries is one failure"
        );
        assert!(
            !has_series_for(WRITE_TIMESTAMP, dataset),
            "no snapshot was written, so none may be reported as written"
        );
    }

    /// A snapshot whose checkpoint step fails is a failed snapshot. The same
    /// function without a snapshot manager only checkpoints, so its failure is
    /// not a snapshot failure.
    #[tokio::test]
    async fn a_failed_snapshot_checkpoint_is_counted() {
        LazyLock::force(&PROMETHEUS);
        let dir = tempfile::tempdir().expect("a temporary directory for the fixture");
        let rt = Runtime::builder().build().await;

        let dataset = "snapshot_checkpoint_fails";
        let manager = SnapshotManager::try_new(
            dataset.to_string(),
            create_only(&rt, writable_location(dir.path())),
            AccelerationLayout::file(acceleration_file(dir.path(), dataset)),
            AccelerationEngine::Cayenne,
        )
        .await
        .expect("a snapshot manager for a writable location");
        let failing = Arc::new(StubCheckpointer {
            fail: true,
            ..StubCheckpointer::default()
        });
        let checkpointer: Arc<dyn DatasetCheckpointer> = Arc::<StubCheckpointer>::clone(&failing);

        runtime_table::accelerated::snapshots::create_checkpoint_and_snapshot(
            &checkpointer,
            Some(&Arc::new(manager)),
            &schema(),
            &Arc::new(tokio::sync::Mutex::new(())),
            &TableReference::bare(dataset),
            &Arc::new(AtomicI64::new(0)),
            ForceCreate(true),
            None,
            None,
            None,
        )
        .await;

        assert_eq!(
            failing.attempts.load(Ordering::SeqCst),
            SNAPSHOT_ATTEMPTS,
            "the checkpoint must be retried before the snapshot is given up"
        );
        assert_eq!(
            failures(dataset),
            Some(1.0),
            "a snapshot given up at its checkpoint step is one failure"
        );

        let checkpoint_only = "checkpoint_without_snapshots_fails";
        runtime_table::accelerated::snapshots::create_checkpoint_and_snapshot(
            &checkpointer,
            None,
            &schema(),
            &Arc::new(tokio::sync::Mutex::new(())),
            &TableReference::bare(checkpoint_only),
            &Arc::new(AtomicI64::new(0)),
            ForceCreate(true),
            None,
            None,
            None,
        )
        .await;
        assert_eq!(
            failures(checkpoint_only),
            None,
            "a dataset without snapshots has no snapshot failure count"
        );
    }

    /// The snapshot taken before an acceleration is recreated is counted when it
    /// fails, though the recreation goes ahead.
    #[tokio::test]
    async fn a_failed_pre_recreation_snapshot_is_counted() {
        LazyLock::force(&PROMETHEUS);
        let dir = tempfile::tempdir().expect("a temporary directory for the fixture");
        let rt = Runtime::builder().build().await;

        let dataset = "pre_recreation_snapshot_fails";
        let acceleration = RuntimeAcceleration {
            snapshot_behavior: create_only(&rt, unwritable_location(dir.path())),
            ..RuntimeAcceleration::default()
        };

        data_accelerator_api::snapshots::snapshot_before_recreate(
            &acceleration,
            dataset,
            AccelerationLayout::file(acceleration_file(dir.path(), dataset)),
            AccelerationEngine::Cayenne,
            schema(),
            Some(Arc::new(DefaultSnapshotEngine)),
            RuntimeRefreshMode::Full,
        )
        .await;

        assert_eq!(
            failures(dataset),
            Some(1.0),
            "a pre-recreation snapshot that could not be uploaded is one failure"
        );
    }
}
