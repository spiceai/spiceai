/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use super::{RowCounts, get_dataset_app_and_start_request, load_app};
use crate::{args::DatasetTestArgs, health::HealthMonitor, spiced_metrics::MetricsScraper};
use chbench_driver::ChBenchDriver as _;
use std::{
    path::Path,
    time::{Duration, Instant},
};
use test_framework::{
    TestType, anyhow,
    app::App,
    arrow::util::pretty::print_batches,
    git,
    metrics::{MetricCollector, NoExtendedMetrics, QueryMetrics, QueryStatus},
    opentelemetry::KeyValue,
    opentelemetry_sdk::Resource,
    spiced::SpicedInstance,
    spicepod::acceleration::Mode,
    spicetest::{
        SpiceTest,
        datasets::{EndCondition, NotStarted},
    },
    tokio_util::sync::CancellationToken,
    utils::{observe_memory, recursively_get_dir_size},
};

pub(crate) fn emit_acceleration_size_if_applicable(
    app: &App,
    app_path: &Path,
) -> anyhow::Result<()> {
    // determine if any dataset has acceleration enabled with a file mode engine
    if !app.datasets.iter().any(|ds| {
        ds.acceleration.as_ref().is_some_and(|accel| {
            matches!(accel.mode, Mode::File | Mode::FileCreate | Mode::FileUpdate)
                && accel.enabled
                && matches!(
                    accel.engine.as_deref(),
                    Some("sqlite" | "duckdb" | "cayenne")
                )
        })
    }) {
        return Ok(());
    }

    // calculate the total size of all files inside .spice
    let spice_dir = app_path.join(".spice");
    let total_size = recursively_get_dir_size(&spice_dir)?;

    println!("Total acceleration size on disk: {total_size} bytes");

    crate::metrics::ACCELERATION_SIZE_BYTES.record(total_size.try_into().unwrap_or_default(), &[]);

    Ok(())
}

pub(crate) async fn run(args: &DatasetTestArgs) -> anyhow::Result<RowCounts> {
    // Two SUT acquisition paths: when a system adapter is configured, delegate
    // setup() to it over JSON-RPC; otherwise spawn a local `spiced` as before.
    // The rest of the benchmark flow is identical apart from a few
    // local-spawn-only side metrics (process memory, on-disk acceleration size)
    // that we skip when the SUT lives elsewhere.
    let (app, spiced_instance, system_adapter_session) = if args.common.is_system_adapter() {
        let app = load_app(&args.common).await?;
        let (instance, session) = crate::system_adapter::acquire(&args.common).await?;
        (app, instance, Some(session))
    } else {
        let (app, start_request) = get_dataset_app_and_start_request(args).await?;

        // For chbench, prepare the Postgres source database (schema + seed data) before starting spiced.
        let query_set = args.load_query_set()?;
        if query_set == test_framework::queries::QuerySet::ChBench {
            let scale_factor = args.scale_factor.unwrap_or(1.0);
            #[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let terminals = (scale_factor * 10.0) as usize;
            prepare_chbench_source(scale_factor, terminals, None).await?;
        }

        let instance = SpicedInstance::start(start_request).await?;
        (app, instance, None)
    };

    // From here on, we may early-return on errors but must still run adapter
    // teardown if one was opened. Wrap the rest of the run in an inner helper
    // whose `Result` we hold onto, run teardown unconditionally, then return.
    let result = run_inner(args, app, spiced_instance).await;

    if let Some(session) = system_adapter_session {
        session.teardown().await;
    }

    result
}

async fn run_inner(
    args: &DatasetTestArgs,
    app: App,
    mut spiced_instance: SpicedInstance,
) -> anyhow::Result<RowCounts> {
    let ready_wait_start = Instant::now();

    // Process-memory tracking is only meaningful when testoperator owns the
    // spiced subprocess. For SUTs acquired via a system adapter, the
    // adapter's `metrics()` RPC is the canonical source of SUT-side resource
    // usage, so we skip the local watcher entirely.
    let memory_token = CancellationToken::new();
    let memory_readings = spiced_instance
        .process()
        .map(|process| process.watch_memory(&memory_token));

    spiced_instance
        .wait_for_ready(Duration::from_secs(args.common.ready_wait))
        .await?;

    let ready_wait_duration = ready_wait_start.elapsed();

    // Build resource with attributes known upfront, before creating telemetry.
    // This ensures the SdkMeterProvider is created with the correct resource,
    // so all metrics (including HealthMonitor) have proper resource attributes.
    let spiced_version = spiced_instance.version().to_string();
    let spiced_commit_sha =
        std::env::var("SPICED_COMMIT").unwrap_or_else(|_| "unknown".to_string());
    let testoperator_commit_sha = git::get_commit_sha();
    let branch_name = git::get_branch_name();

    let query_set = args.load_query_set()?;
    let mut benchmark_attributes = vec![
        KeyValue::new("service.name", "testoperator"),
        KeyValue::new("type", "benchmark_query"),
        KeyValue::new("name", app.name.clone()),
        KeyValue::new("spiced_version", spiced_version),
        KeyValue::new("query_set", query_set.to_string()),
        KeyValue::new("testoperator_commit_sha", testoperator_commit_sha),
        KeyValue::new("spiced_commit_sha", spiced_commit_sha),
        KeyValue::new("branch_name", branch_name),
        KeyValue::new("scale_factor", args.scale_factor.unwrap_or(1.0).to_string()),
    ];
    benchmark_attributes.extend(super::run_mode_attributes(&args.common, args));
    let benchmark_resource = Resource::builder_empty()
        .with_attributes(benchmark_attributes)
        .build();

    // Create telemetry with resource upfront, before any metrics calls
    let telemetry = super::create_telemetry_with_resource(&args.common, benchmark_resource);

    let health_monitor = HealthMonitor::spawn()?;

    // Start metrics scraper if enabled
    let metrics_scraper = if args.common.scrape_spiced_metrics {
        Some(MetricsScraper::spawn()?)
    } else {
        None
    };

    // baseline run
    println!("Running benchmark test");

    // Create the appropriate query executor based on args
    let executor = super::create_query_executor(args, &spiced_instance).await?;

    let (_, test_builder) = super::build_test_with_validation(
        args,
        &app,
        NotStarted::new()
            .with_parallel_count(1)
            .with_end_condition(EndCondition::QuerySetCompleted(5))
            .with_validate(args.validate)
            .with_scale_factor(args.scale_factor.unwrap_or(1.0))
            .with_query_executor(executor),
    )
    .await?;

    let benchmark_test = SpiceTest::new(app.name.clone(), test_builder)
        .with_spiced_instance(spiced_instance)
        .with_results_snapshot(snapshot_predicate)
        .with_progress_bars(!args.common.disable_progress_bars)
        .with_explain_plan_snapshot();

    let benchmark_test = benchmark_test.start()?;

    let test = match benchmark_test.wait().await {
        Ok(test) => test,
        Err(e) => {
            // Best-effort memory drain on failure, mirroring `wait_test_and_memory!`
            // for the local-spawn case. For adapter-acquired SUTs there's no local
            // process being watched, so skip the call entirely.
            if let Some(handle) = memory_readings {
                let _ = observe_memory(memory_token, handle).await;
            }
            return Err(e);
        }
    };

    let row_counts = test.validate_returned_row_counts()?;
    let metrics: QueryMetrics<_, NoExtendedMetrics> = test.collect(TestType::Benchmark)?;
    let test_succeeded = test.succeeded();
    let mut spiced_instance = test.end()?;
    // Only present when a local spiced subprocess is being watched. For
    // adapter-acquired SUTs we deliberately leave this as `None` rather than
    // recording a misleading 0.0 — the adapter's own metrics RPC is the right
    // source of SUT-side resource numbers.
    let memory_usage = match memory_readings {
        Some(handle) => Some(observe_memory(memory_token, handle).await?),
        None => None,
    };

    let mut failures = Vec::new();
    for query in &metrics.metrics {
        let query_name = &query.query_name;
        let row_count = row_counts.get(query_name).unwrap_or(&0);
        let attributes = vec![KeyValue::new("query_name", query_name.to_string())];

        let status: u64 = u64::from(match &query.query_status {
            QueryStatus::Passed => true,
            QueryStatus::Failed(reason) => {
                if let Some(reason) = reason {
                    failures.push(format!("{query_name}: {reason}"));
                } else {
                    failures.push(format!("{query_name}: failed with an undetermined error"));
                }
                false
            }
        });

        crate::metrics::QUERY_STATUS.record(status, &attributes);
        crate::metrics::MEDIAN_DURATION.record(query.median_duration_ms, &attributes);
        crate::metrics::MIN_DURATION.record(query.min_duration_ms, &attributes);
        crate::metrics::MAX_DURATION.record(query.max_duration_ms, &attributes);
        crate::metrics::ITERATIONS.record(query.iterations.try_into()?, &attributes);
        crate::metrics::P90_DURATION.record(query.percentile_90_duration_ms, &attributes);
        crate::metrics::P95_DURATION.record(query.percentile_95_duration_ms, &attributes);
        crate::metrics::P99_DURATION.record(query.percentile_99_duration_ms, &attributes);
        crate::metrics::ROW_COUNT.record((*row_count).try_into()?, &attributes);
    }

    crate::metrics::READY_DURATION.record(ready_wait_duration.as_millis().try_into()?, &[]);
    crate::metrics::TEST_DURATION
        .record((metrics.finished_at - metrics.started_at).try_into()?, &[]);
    if let Some((max_memory, median_memory)) = memory_usage {
        crate::metrics::PEAK_MEMORY_USAGE.record(max_memory * 1024.0, &[]);
        crate::metrics::MEDIAN_MEMORY_USAGE.record(median_memory * 1024.0, &[]);
    }

    // On-disk acceleration size only applies to the local-spawn path (the SUT
    // tempdir is testoperator's own). When the SUT lives elsewhere there's
    // nothing to measure from here.
    if let Ok(tempdir_path) = spiced_instance.get_tempdir_path() {
        emit_acceleration_size_if_applicable(&app, &tempdir_path)?;
    }

    let metrics = match memory_usage {
        Some((max_memory, _)) => metrics.with_memory_usage(max_memory),
        None => metrics,
    };
    let records = metrics.build_records()?;
    print_batches(&records)?;

    let health_report = health_monitor.stop().await;

    // Stop and process metrics scraper if enabled
    super::process_spiced_metrics(metrics_scraper, args.common.metrics, &[]).await;

    telemetry.emit().await?;

    spiced_instance.stop()?;

    let health_report = health_report?;
    let mut error_messages = Vec::new();

    if !test_succeeded {
        error_messages.push(format!(
            "Benchmark test failed due to failed queries:\n{}",
            failures.join("\n")
        ));
    }

    if let Some(message) = health_report.failure_message() {
        eprintln!("Warning: {message}");
    }

    if !error_messages.is_empty() {
        return Err(anyhow::anyhow!(error_messages.join("\n")));
    }

    Ok(row_counts)
}

/// List of query results that should not be snapshotted because they don't return deterministic results
const DISABLED_SNAPSHOT_QUERIES: &[&str] = &[
    "tpcds_q77", // The ORDER BY clause specifies columns that have multiple matches, so the order is unspecified between those rows
];

/// Only snapshot the official TPCH and TPCDS queries, not the "simple" extensions as they don't return consistent results
fn snapshot_predicate(query_name: &str) -> bool {
    (query_name.starts_with("tpch_q") || query_name.starts_with("tpcds_q"))
        && !DISABLED_SNAPSHOT_QUERIES.contains(&query_name)
}

/// Build CH-benCH Postgres source config from environment variables.
///
/// | Variable | Default |
/// |----------|---------|
/// | `CHBENCH_PG_HOST` | `127.0.0.1` |
/// | `CHBENCH_PG_PORT` | `5432` |
/// | `CHBENCH_PG_DB` | `chbench` |
/// | `CHBENCH_PG_USER` | `bench` |
/// | `CHBENCH_PG_PASS` | `bench` |
fn chbench_source_from_env() -> anyhow::Result<chbench_driver::PostgresSourceConfig> {
    let mut source = chbench_driver::PostgresSourceConfig::default();
    if let Ok(v) = std::env::var("CHBENCH_PG_HOST") {
        source.host = v;
    }
    if let Ok(v) = std::env::var("CHBENCH_PG_PORT") {
        source.port = v.parse().map_err(|e| {
            anyhow::anyhow!("CHBENCH_PG_PORT={v:?} is not a valid port number: {e}")
        })?;
    }
    if let Ok(v) = std::env::var("CHBENCH_PG_DB") {
        source.db = v;
    }
    if let Ok(v) = std::env::var("CHBENCH_PG_USER") {
        source.user = v;
    }
    if let Ok(v) = std::env::var("CHBENCH_PG_PASS") {
        source.pass = v;
    }
    Ok(source)
}

/// Validate scale factor, build the CH-benCH config, connect to the source
/// Postgres, create the schema and load seed data.
///
/// `scale_factor` maps to TPC-C warehouses (must be a positive integer >= 1).
/// `terminals` specifies the target number of terminals.
/// `rate` optionally caps the workload-wide transaction rate (txn/s); `None` runs the OLTP workload closed-loop at maximum throughput.
pub(crate) async fn prepare_chbench_source(
    scale_factor: f64,
    terminals: usize,
    rate: Option<u32>,
) -> anyhow::Result<chbench_driver::PostgresChBenchDriver> {
    if scale_factor < 1.0 || scale_factor.fract() != 0.0 {
        anyhow::bail!(
            "CH-benCH --scale-factor must be a positive integer (>= 1), got {scale_factor}. \
             Scale factor maps directly to TPC-C warehouse count."
        );
    }

    // Scale factor is validated >= 1.0 and integer above, so the cast is safe.
    #[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let warehouses = scale_factor as usize;
    let config = chbench_driver::ChBenchConfig {
        warehouses,
        terminals,
        rate,
        ..Default::default()
    };

    println!(
        "Preparing CH-benCHmark source, SF{scale_factor}: {warehouses} warehouse(s), {terminals} terminal(s)"
    );

    let source = chbench_source_from_env()?;
    let driver = chbench_driver::PostgresChBenchDriver::connect(config, source).await?;
    driver.prepare().await?;

    println!("CH-benCHmark source is ready");
    Ok(driver)
}
