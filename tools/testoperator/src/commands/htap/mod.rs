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

//! HTAP test command — runs concurrent TPC-C OLTP workload against the source
//! Postgres database while executing CH-benCH analytical queries through spiced.

mod correctness;
mod staleness;

use std::sync::Arc;
use std::time::{Duration, Instant};

use test_framework::{
    TestType, anyhow,
    arrow::util::pretty::print_batches,
    git,
    metrics::{MetricCollector, NoExtendedMetrics, QueryMetrics, QueryStatus},
    opentelemetry::KeyValue,
    opentelemetry_sdk::Resource,
    queries::QuerySet,
    spiced::SpicedInstance,
    spicetest::{
        SpiceTest,
        datasets::{EndCondition, NotStarted},
    },
    tokio_util::sync::CancellationToken,
    utils::observe_memory,
};

use crate::{
    args::HtapArgs, commands::bench::prepare_chbench_source, health::HealthMonitor,
    spiced_metrics::MetricsScraper, wait_test_and_memory,
};

pub(crate) async fn run(args: &HtapArgs) -> anyhow::Result<()> {
    let test_args = &args.test_args;
    let (app, mut start_request) = super::get_app_and_start_request(&test_args.common).await?;

    let query_set = test_args.load_query_set()?;
    if !matches!(query_set, QuerySet::ChBench) {
        anyhow::bail!(
            "HTAP command requires the 'chbench' query set, but got '{query_set}'. \
             Use '--query-set chbench' or run 'testoperator run bench' for other query sets."
        );
    }

    // Always enable the metrics endpoint in HTAP mode for replication metrics.
    if !test_args.common.scrape_spiced_metrics {
        start_request = start_request
            .with_additional_args(vec!["--metrics".to_string(), "127.0.0.1:9090".to_string()]);
    }

    // 1. Prepare the source (schema + seed data).
    let scale_factor = test_args.scale_factor.unwrap_or(1.0);
    #[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let terminals = args.terminals.unwrap_or((scale_factor * 10.0) as usize);
    let duration = Duration::from_secs(test_args.common.duration);
    let driver: Arc<dyn chbench_driver::ChBenchDriver> =
        Arc::new(prepare_chbench_source(scale_factor, terminals).await?);

    // 2. Start spiced.
    let mut spiced_instance = SpicedInstance::start(start_request).await?;
    let ready_wait_start = Instant::now();

    let memory_token = CancellationToken::new();
    let memory_readings = spiced_instance.process()?.watch_memory(&memory_token);

    spiced_instance
        .wait_for_ready(Duration::from_secs(test_args.common.ready_wait))
        .await?;

    let ready_wait_duration = ready_wait_start.elapsed();

    // Build telemetry resource.
    let spiced_version = spiced_instance.version().to_string();
    let spiced_commit_sha =
        std::env::var("SPICED_COMMIT").unwrap_or_else(|_| "unknown".to_string());
    let testoperator_commit_sha = git::get_commit_sha();
    let branch_name = git::get_branch_name();

    let benchmark_resource = Resource::builder_empty()
        .with_attributes(vec![
            KeyValue::new("service.name", "testoperator"),
            KeyValue::new("type", "htap"),
            KeyValue::new("name", app.name.clone()),
            KeyValue::new("spiced_version", spiced_version),
            KeyValue::new("query_set", query_set.to_string()),
            KeyValue::new("testoperator_commit_sha", testoperator_commit_sha),
            KeyValue::new("spiced_commit_sha", spiced_commit_sha),
            KeyValue::new("branch_name", branch_name),
            KeyValue::new("scale_factor", scale_factor.to_string()),
            KeyValue::new("terminals", terminals.to_string()),
            KeyValue::new("duration_secs", duration.as_secs().to_string()),
        ])
        .build();

    let telemetry = super::create_telemetry_with_resource(&test_args.common, benchmark_resource);

    let health_monitor = HealthMonitor::spawn()?;

    // Always scrape spiced metrics in HTAP mode — replication metrics are essential.
    let metrics_scraper = Some(MetricsScraper::spawn()?);

    // 3. Start the OLTP workload in the background.
    let oltp_stop = CancellationToken::new();
    let oltp_handle = {
        let stop = oltp_stop.clone();
        let driver = Arc::clone(&driver);
        tokio::spawn(async move { driver.run(stop).await })
    };

    // 3b. Start staleness probe alongside the OLTP workload.
    // Disable caching so MAX(_bench_ts) always reflects the latest replicated data.
    let staleness_spice_client = spiced_instance.spice_client(None, true).await?;
    let staleness_handle = staleness::spawn_staleness_probe(
        Arc::clone(&driver),
        staleness_spice_client,
        oltp_stop.clone(),
    );

    // 4. Run analytical queries through spiced concurrently with the OLTP load.
    println!("Running HTAP analytical queries under OLTP load");

    let executor = super::create_query_executor(test_args, &spiced_instance).await?;

    let (_, test_builder) = super::build_test_with_validation(
        test_args,
        &app,
        NotStarted::new()
            .with_parallel_count(1)
            .with_end_condition(EndCondition::Duration(Duration::from_secs(
                test_args.common.duration,
            )))
            .with_validate(test_args.validate)
            .with_scale_factor(test_args.scale_factor.unwrap_or(1.0))
            .with_query_executor(executor),
    )
    .await?;

    let benchmark_test = SpiceTest::new(app.name.clone(), test_builder)
        .with_spiced_instance(spiced_instance)
        .with_results_snapshot(|_| false) // No snapshots for HTAP — results change under OLTP
        .with_progress_bars(!test_args.common.disable_progress_bars)
        // Concurrent OLTP mutations make row counts non-deterministic; 0 rows is expected.
        .with_validate_row_count(false)
        .start()?;

    let test = wait_test_and_memory!(benchmark_test, memory_token, memory_readings);

    // 5. Capture replication metrics while OLTP is still running.
    //    lag_ms = now() − commit_time(last_processed_txn), so it inflates with idle time
    //    after OLTP stops. Stopping the scraper here gives accurate under-load values.
    let spiced_metrics =
        super::process_spiced_metrics(metrics_scraper, test_args.common.metrics, &[]).await;

    // 6. Stop OLTP and collect results.
    oltp_stop.cancel();
    let oltp_result = oltp_handle.await;
    let staleness_result = staleness_handle.await;

    // Skip row count consistency validation — OLTP mutations cause row counts to vary between iterations.
    let metrics: QueryMetrics<_, NoExtendedMetrics> = test.collect(TestType::Htap)?;
    let test_succeeded = test.succeeded();
    let mut spiced_instance = test.end()?;
    let (max_memory, median_memory) = observe_memory(memory_token, memory_readings).await?;

    // 7. Report analytical query metrics.
    let mut failures: Vec<String> = Vec::new();
    for query in &metrics.metrics {
        let query_name = &query.query_name;
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
    }

    crate::metrics::READY_DURATION.record(ready_wait_duration.as_millis().try_into()?, &[]);
    crate::metrics::TEST_DURATION
        .record((metrics.finished_at - metrics.started_at).try_into()?, &[]);
    crate::metrics::PEAK_MEMORY_USAGE.record(max_memory * 1024.0, &[]);
    crate::metrics::MEDIAN_MEMORY_USAGE.record(median_memory * 1024.0, &[]);

    let records = metrics.with_memory_usage(max_memory).build_records()?;
    println!("\n=== Analytical Queries ===");
    print_batches(&records)?;

    // 8. Report OLTP results.
    println!("\n=== TPC-C OLTP ===");
    match oltp_result {
        Ok(Ok(report)) => {
            report.print_summary();
            crate::metrics::OLTP_TPMC.record(report.tpmc, &[]);
        }
        Ok(Err(e)) => {
            eprintln!("OLTP workload error: {e}");
        }
        Err(e) => {
            eprintln!("OLTP task join error: {e}");
        }
    }

    // 9. Report data freshness and replication metrics.
    match staleness_result {
        Ok(Ok(report)) => {
            report.emit();
        }
        Ok(Err(e)) => {
            eprintln!("Staleness probe error: {e}");
        }
        Err(e) => {
            eprintln!("Staleness probe join error: {e}");
        }
    }

    // 10. Data-correctness gate: OLTP has stopped, so wait for replication to
    //     fully drain (bounded by the test duration) and then assert that
    //     source and Spice row counts match for every replicated table.
    let probe_tables: Vec<String> = driver
        .probe_tables()
        .iter()
        .map(|t| (*t).to_string())
        .collect();
    let correctness_result = {
        let spice_client = spiced_instance.spice_client(None, true).await?;
        correctness::verify_after_drain(Arc::clone(&driver), &spice_client, &probe_tables, duration)
            .await
    };

    let health_report = health_monitor.stop().await;

    if let Some(ref metrics) = spiced_metrics {
        emit_replication_metrics(metrics);
    }

    let mut error_messages = Vec::new();

    // Record correctness results (including OpenTelemetry metrics) before flushing telemetry below.
    match correctness_result {
        Ok(report) => {
            report.emit();
            if let Some(message) = report.failure_message() {
                error_messages.push(message);
            }
        }
        Err(e) => {
            error_messages.push(format!("HTAP data-correctness error: {e}"));
        }
    }

    telemetry.emit().await?;
    spiced_instance.stop()?;

    let health_report = health_report?;

    if !test_succeeded {
        error_messages.push(format!(
            "HTAP test failed due to failed queries:\n{}",
            failures.join("\n")
        ));
    }

    if let Some(message) = health_report.failure_message() {
        eprintln!("Warning: {message}");
    }

    if !error_messages.is_empty() {
        return Err(anyhow::anyhow!(error_messages.join("\n")));
    }

    Ok(())
}

/// Emits replication metrics scraped from spiced's `/metrics` endpoint.
fn emit_replication_metrics(metrics: &crate::spiced_metrics::SpicedMetrics) {
    use std::collections::{BTreeMap, BTreeSet};

    // Collect replication metrics per dataset from scraped samples.
    // Gauges (lag_ms, lag_bytes): use the last observed value — represents the
    // pipeline state when the scraper stopped (while OLTP was still active).
    // Counters (inserts, updates, deletes): use the last value (monotonic total).
    let mut lag_ms: BTreeMap<String, f64> = BTreeMap::new();
    let mut lag_bytes: BTreeMap<String, f64> = BTreeMap::new();
    let mut inserts: BTreeMap<String, f64> = BTreeMap::new();
    let mut updates: BTreeMap<String, f64> = BTreeMap::new();
    let mut deletes: BTreeMap<String, f64> = BTreeMap::new();
    let mut recv_errors: BTreeMap<String, f64> = BTreeMap::new();
    let mut reconnects: BTreeMap<String, f64> = BTreeMap::new();

    let gauge_metrics = [
        (
            "dataset_postgres_replication_lag_ms",
            &mut lag_ms as &mut BTreeMap<String, f64>,
        ),
        ("dataset_postgres_replication_lag_bytes", &mut lag_bytes),
    ];
    let counter_metrics = [
        (
            "dataset_postgres_replication_inserts_total",
            &mut inserts as &mut BTreeMap<String, f64>,
        ),
        ("dataset_postgres_replication_updates_total", &mut updates),
        ("dataset_postgres_replication_deletes_total", &mut deletes),
        (
            "dataset_postgres_replication_recv_errors_total",
            &mut recv_errors,
        ),
        (
            "dataset_postgres_replication_reconnects_total",
            &mut reconnects,
        ),
    ];

    for (metric_name, map) in gauge_metrics {
        if let Some(samples) = metrics.samples.get(metric_name) {
            for sample in samples {
                let dataset = sample
                    .labels
                    .get("name")
                    .cloned()
                    .unwrap_or_else(|| "unknown".to_string());
                // Gauge: last value wins (overwrites previous).
                map.insert(dataset, sample.value);
            }
        }
    }

    for (metric_name, map) in counter_metrics {
        if let Some(samples) = metrics.samples.get(metric_name) {
            for sample in samples {
                let dataset = sample
                    .labels
                    .get("name")
                    .cloned()
                    .unwrap_or_else(|| "unknown".to_string());
                // Counter: last observed value is the total.
                map.insert(dataset, sample.value);
            }
        }
    }

    if lag_ms.is_empty()
        && lag_bytes.is_empty()
        && inserts.is_empty()
        && updates.is_empty()
        && deletes.is_empty()
        && recv_errors.is_empty()
        && reconnects.is_empty()
    {
        return;
    }

    println!("\nReplication Metrics (last scrape from spiced)");
    // Header
    println!(
        "  {:<14} {:>10} {:>12} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "dataset",
        "lag_ms",
        "lag_bytes",
        "inserts",
        "updates",
        "deletes",
        "recv_errs",
        "reconnects"
    );

    let all_datasets: BTreeSet<&String> = lag_ms
        .keys()
        .chain(lag_bytes.keys())
        .chain(inserts.keys())
        .chain(updates.keys())
        .chain(deletes.keys())
        .chain(recv_errors.keys())
        .chain(reconnects.keys())
        .collect();

    let mut worst_lag_ms: f64 = 0.0;
    for dataset in &all_datasets {
        let l_ms = lag_ms.get(*dataset).copied().unwrap_or(0.0);
        let l_bytes = lag_bytes.get(*dataset).copied().unwrap_or(0.0);
        let ins = inserts.get(*dataset).copied().unwrap_or(0.0);
        let upd = updates.get(*dataset).copied().unwrap_or(0.0);
        let del = deletes.get(*dataset).copied().unwrap_or(0.0);
        let recv = recv_errors.get(*dataset).copied().unwrap_or(0.0);
        let reconn = reconnects.get(*dataset).copied().unwrap_or(0.0);
        println!(
            "  {dataset:<14} {l_ms:>10.0} {l_bytes:>12.0} {ins:>10.0} {upd:>10.0} {del:>10.0} {recv:>10.0} {reconn:>10.0}",
        );

        crate::metrics::REPLICATION_LAG_MS
            .record(l_ms, &[KeyValue::new("dataset", (*dataset).clone())]);
        if l_ms > worst_lag_ms {
            worst_lag_ms = l_ms;
        }
    }
    println!();

    // Headline: worst replication lag across all datasets.
    crate::metrics::REPLICATION_LAG_MS.record(worst_lag_ms, &[]);
}
