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
mod reporting;
mod spice;
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
    args::{HtapArgs, SourceType},
    commands::bench::prepare_chbench_source,
    health::HealthMonitor,
    probe::{self, Phase},
    spiced_metrics::MetricsScraper,
};

/// Cap on the *default* terminal count (when `--terminals` is not passed
/// explicitly) — see the comment at its use site below.
const DEFAULT_TERMINALS_CAP: usize = 100;

pub(crate) async fn run(args: &HtapArgs) -> anyhow::Result<()> {
    let test_args = &args.test_args;
    // spiced names replication metrics per source connector; select the prefix
    // matching the configured source so scraping picks up the right series.
    let replication_engine = match test_args.source_type {
        SourceType::Postgres => "postgres",
        SourceType::Mysql => "mysql",
    };
    let (app, mut start_request) = super::get_app_and_start_request(&test_args.common).await?;

    let query_set = test_args.load_query_set()?;
    if !matches!(query_set, QuerySet::ChBench | QuerySet::ChBenchFts) {
        anyhow::bail!(
            "HTAP command requires the 'chbench' or 'chbench-fts' query set, but got '{query_set}'. \
             Use '--query-set chbench' or run 'testoperator run bench' for other query sets."
        );
    }
    super::ensure_shared_client_connections(test_args, "htap")?;

    // Always enable the metrics endpoint in HTAP mode for replication metrics.
    if !test_args.common.scrape_spiced_metrics {
        start_request = start_request
            .with_additional_args(vec!["--metrics".to_string(), "127.0.0.1:9090".to_string()]);
    }

    // 1. Prepare the source: seed schema + data — or, with --skip-prepare,
    //    connect to an already-prepared source and verify it matches the SF.
    let scale_factor = test_args.scale_factor.unwrap_or(1.0);
    // Each OLTP terminal opens its own dedicated source-DB connection, and the
    // benchmark source containers run with max-connections=200 (see
    // setup-chbench-mysql/postgres). An unbounded default here can exhaust
    // that well before the scale factor gets large — a manual SF1000 dispatch
    // that omitted --terminals hit exactly this ("Too many connections"),
    // since scale_factor * 10 = 10,000 terminals. Scheduled dispatch configs
    // already avoid it by hardcoding `terminals: 100`; cap the *default* the
    // same way so an omitted --terminals doesn't reproduce the failure.
    #[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let terminals = args
        .terminals
        .unwrap_or_else(|| ((scale_factor * 10.0) as usize).min(DEFAULT_TERMINALS_CAP));
    let duration = Duration::from_secs(test_args.common.duration);
    // Seeding an SF1000 source runs for the better part of an hour and prints
    // little; without this, `/v1/ready` could not tell it apart from a run stuck
    // on the source connection.
    probe::set_phase(Phase::PreparingSource);
    let driver: Arc<dyn chbench_driver::ChBenchDriver> = prepare_chbench_source(
        scale_factor,
        terminals,
        args.rate,
        args.skip_prepare,
        test_args.source_type,
    )
    .await?;

    // --prepare-only: the source is now seeded; exit before starting spiced so
    // an external harness can snapshot the pristine source (e.g. to a Postgres
    // template database) for fast reuse across subsequent runs.
    if args.prepare_only {
        println!("--prepare-only: source prepared, exiting without running the workload");
        probe::set_phase(Phase::Finished);
        return Ok(());
    }

    // 2. Start spiced.
    probe::set_phase(Phase::WaitingForSpiced);
    let mut spiced_instance = SpicedInstance::start(start_request).await?;
    let ready_wait_start = Instant::now();

    let memory_token = CancellationToken::new();
    let memory_readings = spiced_instance
        .process()
        .map(|process| process.watch_memory(&memory_token));

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

    // Source↔local clock-skew estimate for the artifact: lag gauges mix the source's
    // commit timestamps with spiced's clock, so this offset (local − server) biases
    // them and the waterfall can subtract it. Best-effort; `null` if the source is
    // unreachable. Probed once up front (a few ms round trips).
    let clock_skew_ms_estimate = crate::pg_stats::probe_clock_skew_ms().await;
    if let Some(skew) = clock_skew_ms_estimate {
        println!(
            "source clock-skew estimate: {skew}ms (local − server; subtracted from lag in analysis)"
        );
    }

    // Run metadata for the `--metrics-dump` artifact — captured before the values
    // below are moved into the telemetry `Resource`. Mirrors the resource
    // attributes so the waterfall analysis has commit + config alongside the
    // series.
    let run_metadata = serde_json::json!({
        "app_name": app.name.clone(),
        "spiced_version": spiced_version.clone(),
        "spiced_commit_sha": spiced_commit_sha.clone(),
        "testoperator_commit_sha": testoperator_commit_sha.clone(),
        "branch_name": branch_name.clone(),
        "query_set": query_set.to_string(),
        "scale_factor": scale_factor,
        "terminals": terminals,
        "duration_secs": duration.as_secs(),
        "concurrency": test_args.common.concurrency,
        "target_oltp_rate": args.rate
            .map_or_else(|| "unlimited".to_string(), |r| r.to_string()),
        "spicepod_path": test_args.common.spicepod_path.display().to_string(),
        "clock_skew_ms_estimate": clock_skew_ms_estimate,
        "skip_analytic_gate": args.skip_analytic_gate,
    });

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
            KeyValue::new("concurrency", test_args.common.concurrency.to_string()),
            KeyValue::new(
                "target_oltp_rate",
                args.rate
                    .map_or_else(|| "unlimited".to_string(), |r| r.to_string()),
            ),
            KeyValue::new("skip_analytic_gate", args.skip_analytic_gate.to_string()),
        ])
        .build();

    let telemetry = super::create_telemetry_with_resource(&test_args.common, benchmark_resource);

    let health_monitor = HealthMonitor::spawn()?;

    // Always scrape spiced metrics in HTAP mode — replication metrics are essential.
    let metrics_scraper = Some(MetricsScraper::spawn()?);

    // Also sample source-side Postgres stats (walsender waits, OLTP lock
    // contention, WAL-production/commit rate) — best-effort; never blocks the run.
    let pg_stats_scraper = match crate::pg_stats::source_conn_from_env() {
        Ok((conn_str, db)) => match crate::pg_stats::PgStatsScraper::spawn(conn_str, db).await {
            Ok(scraper) => scraper,
            Err(e) => {
                eprintln!("pg_stats: scraper failed to start: {e}");
                None
            }
        },
        Err(e) => {
            eprintln!("pg_stats: could not resolve source config: {e}");
            None
        }
    };

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
        // Reject absurd freshness samples (> 2x the run duration): a bootstrap-era /
        // catch-up probe reading can otherwise poison p99/max (a single ~2-day sample
        // dominated a 10-sample window in a prior run).
        duration.saturating_mul(2),
    );

    // 4. Run analytical queries through spiced concurrently with the OLTP load.
    // Load is being applied from here on, so this is where the run becomes
    // ready: a window that starts earlier would include the seed, and one that
    // started at the first query would miss the OLTP writes under it.
    probe::set_phase(Phase::Running);
    println!("Running HTAP analytical queries under OLTP load");

    let executor = super::create_query_executor(test_args, &spiced_instance).await?;

    let (_, test_builder) = super::build_test_with_validation(
        test_args,
        &app,
        NotStarted::new()
            .with_parallel_count(test_args.common.concurrency)
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
        .with_results_snapshot(|_, _| test_framework::snapshot::SnapshotMode::Skip) // No snapshots for HTAP — results change under OLTP
        .with_progress_bars(!test_args.common.disable_progress_bars)
        // Concurrent OLTP mutations make row counts non-deterministic; 0 rows is expected.
        .with_validate_row_count(false)
        .start()?;

    let test = match benchmark_test.wait().await {
        Ok(test) => test,
        Err(e) => {
            if let Some(handle) = memory_readings {
                let _ = observe_memory(memory_token, handle).await;
            }
            return Err(e);
        }
    };

    // 5. Capture replication metrics while OLTP is still running.
    //    lag_ms = now() − commit_time(last_processed_txn), so it inflates with idle time
    //    after OLTP stops. Stopping the scraper here gives accurate under-load values.
    let spiced_metrics =
        super::process_spiced_metrics(metrics_scraper, test_args.common.metrics, &[]).await;
    // Snapshot the probe latencies for the same reason, while OLTP is still running:
    // the monitor keeps sampling through the post-drain gate below, and that idle
    // window would dilute the under-load percentiles.
    let probe_snapshot = health_monitor.snapshot();
    // Stop the source-PG stats scraper at the same point (under load), so its view
    // aligns with the spiced-side scrape window.
    let pg_stats = match pg_stats_scraper {
        Some(scraper) => scraper.stop().await,
        None => Vec::new(),
    };

    // 6. Stop OLTP and collect results. No load is applied past this point, so
    //    the run stops reporting itself ready while the gates and reporting
    //    below (which can take minutes) still run.
    probe::set_phase(Phase::Finalizing);
    oltp_stop.cancel();
    let oltp_result = oltp_handle.await;
    let staleness_result = staleness_handle.await;

    // Skip row count consistency validation — OLTP mutations cause row counts to vary between iterations.
    let metrics: QueryMetrics<_, NoExtendedMetrics> = test.collect(TestType::Htap)?;
    let test_succeeded = test.succeeded();
    let mut spiced_instance = test.end()?;
    let memory_usage = match memory_readings {
        Some(handle) => Some(observe_memory(memory_token, handle).await?),
        None => None,
    };

    probe_snapshot.print_latency_summary("under load");

    // 7. Report analytical query metrics.
    let mut failures: Vec<String> = Vec::new();
    let mut query_summary_rows: Vec<reporting::QuerySummaryRow> = Vec::new();
    for query in &metrics.metrics {
        let query_name = &query.query_name;
        let attributes = vec![KeyValue::new("query_name", query_name.to_string())];

        let passed = match &query.query_status {
            QueryStatus::Passed => true,
            QueryStatus::Failed(reason) => {
                if let Some(reason) = reason {
                    failures.push(format!("{query_name}: {reason}"));
                } else {
                    failures.push(format!("{query_name}: failed with an undetermined error"));
                }
                false
            }
        };
        let status: u64 = u64::from(passed);

        query_summary_rows.push(reporting::QuerySummaryRow {
            query_name: query_name.to_string(),
            passed,
            iterations: query.iterations,
            median_ms: query.median_duration_ms,
            p90_ms: query.percentile_90_duration_ms,
            p99_ms: query.percentile_99_duration_ms,
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
    if let Some((max_memory, median_memory)) = memory_usage {
        crate::metrics::PEAK_MEMORY_USAGE.record(max_memory * 1024.0, &[]);
        crate::metrics::MEDIAN_MEMORY_USAGE.record(median_memory * 1024.0, &[]);
    }

    // Calculate analytical throughput — QPH (queries per hour).
    let completed_queries: usize = metrics.metrics.iter().map(|q| q.iterations).sum();
    let elapsed = Duration::from_millis(
        u64::try_from(metrics.finished_at.saturating_sub(metrics.started_at)).unwrap_or(0),
    );
    let elapsed_secs = elapsed.as_secs_f64();
    #[expect(clippy::cast_precision_loss)]
    let qph = if elapsed_secs > 0.0 {
        completed_queries as f64 / elapsed_secs * 3600.0
    } else {
        0.0
    };
    crate::metrics::QPH.record(qph, &[]);

    let metrics = match memory_usage {
        Some((max_memory, _)) => metrics.with_memory_usage(max_memory),
        None => metrics,
    };
    let records = metrics.build_records()?;
    println!("\n=== Analytical Queries ===");
    print_batches(&records)?;
    println!(
        "  QPH (analytical queries/hour): {qph:.1} ({completed_queries} queries in {elapsed_secs:.1}s)"
    );

    // 8. Report OLTP results.
    println!("\n=== TPC-C OLTP ===");
    let mut oltp_summary: Option<reporting::OltpSummary> = None;
    match oltp_result {
        Ok(Ok(report)) => {
            report.print_summary();
            crate::metrics::OLTP_TPMC.record(report.tpmc, &[]);
            oltp_summary = Some(reporting::OltpSummary {
                tpmc: report.tpmc,
                total_committed: report.total_committed,
                total_aborted: report.total_aborted,
                abort_rate: report.abort_rate,
            });
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

    // Apply-phase coverage violations (populated below), gated at the end of the run.
    let mut coverage_violations: Vec<(String, f64)> = Vec::new();
    let mut lag_summary: Option<reporting::ReplicationLagSummary> = None;
    if let Some(metrics) = &spiced_metrics {
        lag_summary = reporting::emit_replication_metrics(
            metrics,
            replication_engine,
            &pg_stats,
            "under load",
            true,
        );
        // For Cayenne backend report additional metrics
        reporting::emit_cayenne_read_amp_percentiles(metrics);
        // Localize CDC backpressure across the pipeline stages (prefetch channel,
        // encode budget, compaction semaphore, mem-tier budget).
        reporting::emit_backpressure_summary(metrics);
        // Instrumentation self-check: flag (and optionally fail on) tables whose apply
        // time is mostly un-instrumented — a blind spot hides a real bottleneck there.
        coverage_violations = reporting::emit_phase_coverage(metrics, args.min_phase_coverage);
    }

    // Persist the full scraped time-series + run metadata for offline waterfall
    // analysis (scripts/chbench-waterfall.py) and CI artifact upload.
    if let Some(dump_path) = &args.metrics_dump {
        match reporting::write_metrics_dump(
            dump_path,
            &run_metadata,
            spiced_metrics.as_ref(),
            &pg_stats,
        )
        .await
        {
            Ok(()) => println!("\nWrote metrics dump to {}", dump_path.display()),
            Err(e) => eprintln!(
                "Failed to write metrics dump to {}: {e}",
                dump_path.display()
            ),
        }
    }

    // Emit the headline results (tpmC, QPH, worst lag, per-query latencies) as a
    // Markdown summary CI appends to the job summary.
    if let Some(summary_path) = &args.summary_out {
        let summary = reporting::RunSummary {
            qph,
            completed_queries,
            elapsed_secs,
            oltp: oltp_summary,
            lag: lag_summary,
            queries: query_summary_rows,
        };
        match reporting::write_run_summary(summary_path, &summary).await {
            Ok(()) => println!("\nWrote run summary to {}", summary_path.display()),
            Err(e) => eprintln!(
                "Failed to write run summary to {}: {e}",
                summary_path.display()
            ),
        }
    }

    // 10. Data-correctness gate: OLTP has stopped, so wait for replication to
    //     fully drain (bounded by 2x the test duration) and then assert that
    //     source and Spice row counts match for every replicated table.
    let probe_tables: Vec<String> = driver
        .probe_tables()
        .iter()
        .map(|t| (*t).to_string())
        .collect();
    // One handle over both the query client and the low-level Flight client,
    // reused by the correctness and analytical gates below.
    let spice_clients = {
        let query = spiced_instance.spice_client(None, true).await?;
        let flight = spiced_instance.flight_client(None).await?;
        spice::SpiceClients::new(query, flight)
    };

    let correctness_result = correctness::verify_after_drain(
        Arc::clone(&driver),
        &spice_clients,
        &probe_tables,
        // Allow up to 2x the test duration for replication to converge post-drain.
        duration.saturating_mul(2),
    )
    .await;

    let health_report = health_monitor.stop().await;

    let mut error_messages = Vec::new();

    // Record correctness results (including OpenTelemetry metrics) before flushing telemetry below.
    match correctness_result {
        Ok(report) => {
            report.emit();
            // If replication failed to converge, re-scrape the live lag one more time for diagnostics
            if !report.convergence.converged() {
                match crate::spiced_metrics::MetricsScraper::scrape_once().await {
                    Ok(metrics) => {
                        // Re-sample source-side stats fresh: the background scraper
                        // stopped under load, so its `pg_stats` are stale and would make
                        // the authoritative slot-retained check report against load-time
                        // WAL rather than the current (post-drain) state.
                        let fresh_pg_stats =
                            crate::pg_stats::PgStatsScraper::sample_once_now().await;
                        // Diagnostic re-scrape: the lag summary return is unused here
                        // (the headline was already captured from the under-load scrape).
                        let _ = reporting::emit_replication_metrics(
                            &metrics,
                            replication_engine,
                            &fresh_pg_stats,
                            "post-drain re-scrape",
                            false,
                        );
                    }
                    Err(e) => {
                        eprintln!(
                            "Failed to re-scrape replication metrics after non-convergence: {e}"
                        );
                    }
                }
            }
            let row_count_message = report.failure_message();
            if let Some(message) = row_count_message.clone() {
                error_messages.push(message);
            }

            // Analytical-correctness gate runs only when not explicitly skipped AND the
            // row-count gate fully passed (replication converged + every table matches).
            // Otherwise the underlying data is known to diverge, so comparing analytical
            // query results adds no signal.
            let skip_reason = match (args.skip_analytic_gate, row_count_message.is_some()) {
                (true, true) => Some("--skip-analytic-gate set (row-count gate also did not pass)"),
                (true, false) => Some("--skip-analytic-gate set"),
                (false, true) => Some("row-count gate did not pass"),
                (false, false) => None,
            };

            if let Some(reason) = skip_reason {
                println!("\nSkipping analytical-query gate — {reason}");
            } else {
                let query_overrides = test_args
                    .query_overrides
                    .clone()
                    .map(test_framework::queries::QueryOverrides::from);
                let analytical_result = correctness::verify_analytical_results(
                    Arc::clone(&driver),
                    &spice_clients,
                    query_overrides,
                    args.analytic_gate_concurrency,
                )
                .await;

                match analytical_result {
                    Ok(analytical) => {
                        analytical.emit();
                        if let Some(message) = analytical.failure_message() {
                            error_messages.push(message);
                        }
                    }
                    Err(e) => {
                        error_messages.push(format!("HTAP analytical-query error: {e}"));
                    }
                }

                // The analytical gate above compares against the source engine,
                // which has no `text_search` UDTF — the FTS queries are checked
                // separately against a deterministic expectation instead. Gated
                // the same way (row-count convergence + not skipped), since a
                // diverged source/Spice row set makes this check meaningless too.
                if matches!(query_set, QuerySet::ChBenchFts) {
                    match correctness::verify_fts_results(&driver, &spice_clients).await {
                        Ok(fts_report) => {
                            fts_report.emit();
                            if let Some(message) = fts_report.failure_message() {
                                error_messages.push(message);
                            }
                        }
                        Err(e) => {
                            error_messages.push(format!("HTAP full-text-search gate error: {e}"));
                        }
                    }
                }
            }
        }
        Err(e) => {
            error_messages.push(format!("HTAP data-correctness error: {e}"));
        }
    }

    // For Cayenne backend report additional metrics
    match crate::spiced_metrics::MetricsScraper::scrape_once().await {
        Ok(final_metrics) => reporting::emit_cayenne_compaction_metrics(&final_metrics),
        Err(e) => eprintln!("Failed to scrape final Cayenne compaction metrics: {e}"),
    }

    telemetry.emit().await?;

    // Optional: hold spiced alive after the benchmark so you can run ad-hoc
    // queries against it. Set SPICED_KEEP_ALIVE to block here until you press
    // Enter; spiced is still serving on its usual HTTP (8090) / Flight ports.
    if std::env::var_os("SPICED_KEEP_ALIVE").is_some() {
        let workdir = spiced_instance
            .get_tempdir_path()
            .map_or_else(|_| "<unknown>".to_string(), |p| p.display().to_string());
        println!(
            "\nSPICED_KEEP_ALIVE set — spiced is still running for manual queries \
             spiced working dir: {workdir}\n\
             Press Enter to stop spiced and finish the run..."
        );
        let mut line = String::new();
        std::io::stdin().read_line(&mut line)?;
    }

    spiced_instance.stop()?;
    // Everything measurable is done; what follows only assembles the verdict.
    // Set here rather than beside the final `Ok(())` so a run that exits with a
    // gate failure reports the same phase as one that passes.
    probe::set_phase(Phase::Finished);

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

    if !coverage_violations.is_empty() {
        let detail = coverage_violations
            .iter()
            .map(|(t, c)| {
                format!(
                    "{t}: {:.1}% < {:.0}%",
                    c * 100.0,
                    args.min_phase_coverage * 100.0
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        error_messages.push(format!(
            "HTAP apply-phase coverage below --min-phase-coverage: {detail} \
             (a CDC apply bottleneck is hiding in un-instrumented code)"
        ));
    }

    if !error_messages.is_empty() {
        return Err(anyhow::anyhow!(error_messages.join("\n")));
    }

    Ok(())
}
