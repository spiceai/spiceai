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

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::path::{Path, PathBuf};

    use clap::Parser;
    use test_framework::{gh_utils::map_numbers_to_strings, utils::scan_directory_for_yamls};

    use super::{load_search_method, spicepod::validate_required_tables};
    use crate::{
        args::{
            SearchTestArgs,
            dispatch::{DispatchTestFile, SearchArgs},
        },
        commands::load_app,
    };

    /// The `testoperator run search` arguments that `testoperator_run_search.yml` builds from a
    /// dispatch entry. Paths resolve against `repo_root`, as the workflow resolves them against
    /// its checkout.
    fn run_search_args(entry: &SearchArgs, repo_root: &Path) -> SearchTestArgs {
        // The workflow inputs, exactly as `testoperator dispatch` sends them.
        let inputs = map_numbers_to_strings(
            serde_json::to_value(entry).expect("should serialize the search inputs"),
        );
        let input = |name: &str| inputs.get(name).and_then(serde_json::Value::as_str);

        let spicepod_path = input("spicepod_path").expect("spicepod_path is a required input");
        let mut command_line: Vec<OsString> = vec![
            "testoperator".into(),
            "--spicepod-path".into(),
            repo_root.join(spicepod_path).into(),
        ];
        if let Some(dependencies) = input("spicepod_dependencies") {
            command_line.push("--spicepod-dependencies".into());
            command_line.push(repo_root.join(dependencies).into());
        }
        if let Some(sql_pipeline) = input("sql_pipeline") {
            // A file path is relative to the checkout; SQL text passes as is.
            let file = repo_root.join(sql_pipeline);
            command_line.push("--sql-pipeline".into());
            command_line.push(if file.is_file() {
                file.into()
            } else {
                sql_pipeline.into()
            });
        }

        SearchTestArgs::try_parse_from(command_line)
            .unwrap_or_else(|e| panic!("should translate to testoperator arguments: {e}"))
    }

    /// Each search dispatch entry translates to `testoperator run search` arguments, and its
    /// spicepod (dependencies included) loads with the tables the harness queries. A broken
    /// entry fails here instead of in the scheduled run.
    #[tokio::test]
    async fn search_dispatches_translate_and_load() {
        let repo_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let dispatch_directory = repo_root.join("tools/testoperator/dispatch/search");
        let mut checked = 0;
        for dispatch_path in scan_directory_for_yamls(&dispatch_directory)
            .expect("should scan the search dispatch directory")
        {
            let dispatch_name = dispatch_path.display();
            let dispatch_file =
                std::fs::File::open(&dispatch_path).expect("should open the dispatch file");
            let dispatch: DispatchTestFile =
                yaml::from_reader(dispatch_file).expect("should parse the dispatch file");
            for entry in &dispatch.tests.search {
                let args = run_search_args(entry, &repo_root);
                load_search_method(args.sql_pipeline.as_ref())
                    .unwrap_or_else(|e| panic!("{dispatch_name}: {e}"));
                let app = load_app(&args.common).await.unwrap_or_else(|e| {
                    panic!(
                        "{dispatch_name} should load {}: {e:#}",
                        entry.spicepod_path.display()
                    )
                });
                validate_required_tables(&app).unwrap_or_else(|e| panic!("{dispatch_name}: {e}"));
                checked += 1;
            }
        }
        assert!(checked > 0, "should find search dispatch entries");
    }

    /// A temporary repository with a root spicepod that gets its search tables from one
    /// dependency under `spicepods/`.
    fn write_spicepod_with_dependency() -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "testoperator-search-dependencies-{}",
            std::process::id()
        ));
        let dependency = root.join("spicepods/data/search");
        std::fs::create_dir_all(&dependency).expect("create temp dirs");
        std::fs::write(
            root.join("root.yaml"),
            "version: v1\nkind: Spicepod\nname: root\ndependencies:\n  - data/search\n",
        )
        .expect("write root spicepod");
        std::fs::write(
            dependency.join("spicepod.yaml"),
            "version: v1
kind: Spicepod
name: search-data
datasets:
  - from: file:corpus.parquet
    name: corpus
  - from: file:test_queries.parquet
    name: test_queries
  - from: file:relevance_data.parquet
    name: relevance_data
",
        )
        .expect("write dependency spicepod");
        root
    }

    #[tokio::test]
    async fn search_dispatch_entry_loads_spicepod_dependencies() {
        let repo_root = write_spicepod_with_dependency();
        let dispatch: DispatchTestFile = yaml::from_str(
            "
tests:
  search:
    spicepod_path: root.yaml
    runner_type: spiceai-dev-runners
    spicepod_dependencies: spicepods
",
        )
        .expect("should parse the dispatch file");
        let mut entry = dispatch.tests.search[0].clone();

        let args = run_search_args(&entry, &repo_root);
        let app = load_app(&args.common)
            .await
            .unwrap_or_else(|e| panic!("should load the dependency: {e:#}"));
        validate_required_tables(&app).expect("the dependency defines the search tables");

        // Without the directory, the dependency cannot load: fail and say how to fix it.
        entry.spicepod_dependencies = None;
        let args = run_search_args(&entry, &repo_root);
        let err = load_app(&args.common)
            .await
            .expect_err("a spicepod with dependencies needs --spicepod-dependencies");
        let message = err.to_string();
        assert!(
            message.contains("data/search"),
            "error must name the dependency: {message}"
        );
        assert!(
            message.contains("--spicepod-dependencies"),
            "error must name the flag: {message}"
        );

        let _ = std::fs::remove_dir_all(repo_root);
    }
}
