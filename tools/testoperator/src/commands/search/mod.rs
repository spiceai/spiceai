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

mod harness;
mod per_query;
mod spicepod;
use self::spicepod::BenchmarkDataset;
use super::{duration_millis_between, get_app_and_start_request};
use crate::{
    args::{SearchTestArgs, SqlPipelineArg},
    health::HealthMonitor,
};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};
use test_framework::{
    TestType, anyhow, git,
    metrics::{MetricCollector, QueryMetrics},
    opentelemetry::KeyValue,
    opentelemetry_sdk::Resource,
    spiced::SpicedInstance,
    spicetest::{
        SpiceTest,
        search::{NotStarted, RetrievalMetrics, SearchMethod, SearchRunMetric, SqlPipeline},
    },
    telemetry::Telemetry,
    tokio_util::sync::CancellationToken,
    utils::observe_memory,
};
use tokio::time::sleep;

pub(crate) async fn run(args: &SearchTestArgs) -> anyhow::Result<()> {
    // The spicepod loads its own data; the run only needs the tables it queries and the label.
    let benchmark_dataset = BenchmarkDataset::from_spicepod_file(&args.common.spicepod_path)?;
    let search_method = load_search_method(args.sql_pipeline.as_ref())?;
    let (app, start_request) = get_app_and_start_request(&args.common).await?;
    spicepod::validate_required_tables(&app)?;

    let started_at = Instant::now();

    let mut spiced_instance = SpicedInstance::start(start_request).await?;
    let memory_token = CancellationToken::new();
    let memory_readings = spiced_instance
        .process()
        .map(|process| process.watch_memory(&memory_token));

    println!("Starting benchmark Spicepod...");

    spiced_instance
        .wait_for_ready(Duration::from_secs(args.common.ready_wait))
        .await?;

    // Build resource with attributes known upfront, before creating telemetry.
    // This ensures the SdkMeterProvider is created with the correct resource.
    let spiced_commit_sha =
        std::env::var("SPICED_COMMIT").unwrap_or_else(|_| git::get_commit_sha());
    let mut search_attributes = vec![
        KeyValue::new("service.name", "testoperator"),
        KeyValue::new("type", "search"),
        KeyValue::new("name", app.name.clone()),
        KeyValue::new("spiced_version", spiced_instance.version().to_string()),
        KeyValue::new("spiced_commit_sha", spiced_commit_sha),
        KeyValue::new("testoperator_commit_sha", git::get_commit_sha()),
        KeyValue::new("branch_name", git::get_branch_name()),
        KeyValue::new("config_name", app.name.clone()),
        KeyValue::new("benchmark_dataset", benchmark_dataset.as_str().to_string()),
        KeyValue::new("search_method", search_method.name()),
    ];
    if let Some(sql_pipeline) = &args.sql_pipeline {
        search_attributes.push(KeyValue::new("sql_pipeline", sql_pipeline.name()));
    }
    search_attributes.extend(spicepod::corpus_attributes(&app));

    let search_resource = Resource::builder_empty()
        .with_attributes(search_attributes)
        .build();

    // Create telemetry with resource upfront, before any metrics calls
    let telemetry = Telemetry::new_with_resource(&search_resource, "SPICEAI_BENCHMARK_METRICS_KEY");

    let health_monitor = HealthMonitor::spawn()?;

    let index_finished_at = Instant::now();

    // Allow Spicepod traces to be fully printed before running the test
    sleep(Duration::from_millis(200)).await;

    println!("Running search ({})", search_method.name());

    let config = harness::init_search_config(&spiced_instance, Some(10))
        .await?
        .with_method(search_method);

    // retrieve query relevance data
    let qrels = harness::get_query_relevance_data(&spiced_instance).await?;

    let search_started_at = Instant::now();

    let vector_test = SpiceTest::new(
        app.name.clone(),
        NotStarted::new()
            .with_config(config)
            .with_parallel_count(args.common.concurrency),
    )
    .with_spiced_instance(spiced_instance)
    .start()?;

    let test = match vector_test.wait().await {
        Ok(test) => test,
        Err(e) => {
            if let Some(handle) = memory_readings {
                let _ = observe_memory(memory_token, handle).await;
            }
            return Err(e);
        }
    };
    let finished_at = Instant::now();

    println!("Search requests completed, calculating results...");

    let p95 = test.get_p95_response_time_metric()?;
    let rps = test.get_rps_metric()?;
    let evaluation =
        test.evaluate_search_results(&qrels, harness::transform_search_results_for_eval)?;
    let retrieval_metrics_at_all_k = evaluation.mean_at_all_k();

    // Report the metric-vs-k curve, then pick the primary cutoff (k=10, matching MTEB; falling back
    // to the largest available k when fewer results were returned) for the fixed-schema run row.
    print_retrieval_metrics_table(&retrieval_metrics_at_all_k);
    let (primary_k, retrieval_metrics) = per_query::primary_k(&evaluation)
        .and_then(|k| evaluation.mean_at_k(k).map(|metrics| (k, metrics)))
        .ok_or_else(|| anyhow::anyhow!("No retrieval metrics were computed for any rank cutoff"))?;

    let metrics: QueryMetrics<_, _> =
        test.collect(TestType::Search)?
            .with_run_metric(SearchRunMetric::new(
                rps,
                p95,
                retrieval_metrics.ndcg,
                retrieval_metrics.recall,
                retrieval_metrics.mrr,
                retrieval_metrics.precision,
            ));

    let mut spiced_instance = test.end()?;
    let memory_usage = match memory_readings {
        Some(handle) => Some(observe_memory(memory_token, handle).await?),
        None => None,
    };

    let metrics = match memory_usage {
        Some((max_memory, _)) => metrics.with_memory_usage(max_memory),
        None => metrics,
    };
    metrics.show_run(None)?; // no additional test pass logic applies

    // Record benchmark results
    crate::metrics::TEST_DURATION.record(duration_millis_between(finished_at, started_at)?, &[]);
    crate::metrics::VECTOR_INDEX_CREATION_DURATION
        .record(duration_millis_between(index_finished_at, started_at)?, &[]);
    crate::metrics::SEARCH_DURATION.record(
        duration_millis_between(finished_at, search_started_at)?,
        &[],
    );

    crate::metrics::SEARCH_RPS.record(rps, &[]);
    crate::metrics::SEARCH_P95_RESPONSE_TIME.record(p95, &[]);
    // Emit each retrieval metric as a `k`-dimensioned series so the full metric-vs-k curve is
    // recorded, not just the primary cutoff.
    for (&k, metrics_at_k) in &retrieval_metrics_at_all_k {
        let k_attr = [KeyValue::new("k", i64::try_from(k)?)];
        crate::metrics::SCORE.record(metrics_at_k.ndcg, &k_attr);
        crate::metrics::SEARCH_RECALL.record(metrics_at_k.recall, &k_attr);
        crate::metrics::SEARCH_MRR.record(metrics_at_k.mrr, &k_attr);
        crate::metrics::SEARCH_PRECISION.record(metrics_at_k.precision, &k_attr);
    }
    per_query::record_metrics(&evaluation, primary_k)?;
    if let Some((max_memory, median_memory)) = memory_usage {
        crate::metrics::PEAK_MEMORY_USAGE.record(max_memory * 1024.0, &[]);
        crate::metrics::MEDIAN_MEMORY_USAGE.record(median_memory * 1024.0, &[]);
    }

    telemetry.emit().await?;

    let health_report = health_monitor.stop().await;
    spiced_instance.stop()?;
    let health_report = health_report?;

    if let Some(message) = health_report.failure_message() {
        return Err(anyhow::anyhow!(message));
    }

    println!("Benchmark completed successfully!");

    Ok(())
}

/// Gets the `--sql-pipeline` SQL, if set, before spiced starts so that an empty pipeline fails
/// fast. Without the flag, the run uses `POST /v1/search`.
fn load_search_method(sql_pipeline: Option<&SqlPipelineArg>) -> anyhow::Result<SearchMethod> {
    let sql = match sql_pipeline {
        None => return Ok(SearchMethod::SearchApi),
        Some(SqlPipelineArg::Inline(sql)) => sql.clone(),
        Some(SqlPipelineArg::File(path)) => {
            let sql = std::fs::read_to_string(path).map_err(|e| {
                anyhow::anyhow!("Failed to read --sql-pipeline {}: {e}", path.display())
            })?;
            if sql.trim().is_empty() {
                return Err(anyhow::anyhow!(
                    "--sql-pipeline {} has no SQL",
                    path.display()
                ));
            }
            sql
        }
    };

    Ok(SearchMethod::Sql(SqlPipeline::new(sql)))
}

/// Print the retrieval-quality metrics at every computed rank cutoff `k` as an aligned table.
fn print_retrieval_metrics_table(metrics_by_k: &BTreeMap<usize, RetrievalMetrics>) {
    if metrics_by_k.is_empty() {
        println!("No retrieval metrics were computed (no query returned results).");
        return;
    }

    println!("Retrieval metrics @k:");
    println!(
        "{:>4}  {:>8}  {:>8}  {:>8}  {:>9}",
        "k", "ndcg", "recall", "mrr", "precision"
    );
    for (k, metrics) in metrics_by_k {
        println!(
            "{k:>4}  {:>8.4}  {:>8.4}  {:>8.4}  {:>9.4}",
            metrics.ndcg, metrics.recall, metrics.mrr, metrics.precision
        );
    }
}
