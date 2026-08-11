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

mod dataset;
mod mteb;
use self::dataset::SearchDataset;
use super::{duration_millis_between, get_app_and_start_request};
use crate::{args::SearchTestArgs, health::HealthMonitor};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};
use test_framework::{
    TestType, anyhow,
    app::App,
    git,
    metrics::{MetricCollector, QueryMetrics},
    opentelemetry::KeyValue,
    opentelemetry_sdk::Resource,
    spiced::SpicedInstance,
    spicetest::{
        SpiceTest,
        search::{NotStarted, RetrievalMetrics, SearchRunMetric},
    },
    telemetry::Telemetry,
    tokio_util::sync::CancellationToken,
    utils::observe_memory,
};
use tokio::time::sleep;

pub(crate) async fn run(args: &SearchTestArgs) -> anyhow::Result<()> {
    let dataset = SearchDataset::from(args.benchmark_dataset);
    let (app, start_request) = get_app_and_start_request(&args.common).await?;

    dataset
        .prepare(
            &args
                .common
                .data_dir
                .clone()
                .unwrap_or(start_request.get_tempdir_path()),
        )
        .await?;

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
        KeyValue::new("benchmark_dataset", dataset.name()),
    ];
    search_attributes.extend(search_dataset_attributes(&app));

    let search_resource = Resource::builder_empty()
        .with_attributes(search_attributes)
        .build();

    // Create telemetry with resource upfront, before any metrics calls
    let telemetry = Telemetry::new_with_resource(&search_resource, "SPICEAI_BENCHMARK_METRICS_KEY");

    let health_monitor = HealthMonitor::spawn()?;

    let index_finished_at = Instant::now();

    // Allow Spicepod traces to be fully printed before running the test
    sleep(Duration::from_millis(200)).await;

    println!("Running search");

    let config = dataset
        .init_search_config(&spiced_instance, Some(10))
        .await?;

    // retrieve query relevance data
    let qrels = dataset.query_relevance_data(&spiced_instance).await?;

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
    let retrieval_metrics_at_all_k = test
        .calculate_search_score_metrics_at_all_k(&qrels, |results| {
            dataset.transform_results(results)
        })?;

    // Report the metric-vs-k curve, then pick the primary cutoff (k=10, matching MTEB; falling back
    // to the largest available k when fewer results were returned) for the fixed-schema run row.
    print_retrieval_metrics_table(&retrieval_metrics_at_all_k);
    let retrieval_metrics = retrieval_metrics_at_all_k
        .get(&10)
        .or_else(|| retrieval_metrics_at_all_k.values().next_back())
        .copied()
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

fn search_dataset_attributes(app: &App) -> Vec<KeyValue> {
    let Some(ds) = app.datasets.iter().find(|ds| ds.name == "corpus") else {
        return vec![];
    };
    let mut attributes = vec![];
    if let Some(engine) = ds
        .acceleration
        .as_ref()
        .map(|acc| acc.engine.clone().unwrap_or("arrow".to_string()))
    {
        attributes.push(KeyValue::new("engine", engine));
    }

    if let Some(acc) = ds.acceleration.as_ref() {
        attributes.push(KeyValue::new("engine_mode", acc.mode.to_string()));
    }

    let Some(text_col) = ds.columns.iter().find(|c| c.name == "text") else {
        return attributes;
    };

    if let Some(embed) = text_col.embeddings.first()
        && let Some(e) = app.embeddings.iter().find(|e| e.name == embed.model)
    {
        attributes.push(KeyValue::new("vector_search", "true"));
        attributes.push(KeyValue::new("model", e.from.clone()));
    } else {
        attributes.push(KeyValue::new("vector_search", "false"));
    }

    attributes.push(KeyValue::new(
        "full_text_search",
        text_col
            .full_text_search
            .as_ref()
            .is_some_and(|fts| fts.enabled)
            .to_string(),
    ));

    attributes
}
