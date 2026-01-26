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

use crate::{args::TextToSqlArgs, commands::run_or_connect_spiced};
use test_framework::{
    TestType,
    anyhow::{self, Context},
    git,
    metrics::{MetricCollector, QueryMetrics, QueryStatus},
    opentelemetry::KeyValue,
    opentelemetry_sdk::Resource,
    process::MemoryReading,
    spicetest::{
        SpiceTest,
        text_to_sql::{NotStarted, TextToSqlMetric, TextToSqlRunMetric},
    },
    telemetry::Telemetry,
    utils::observe_memory,
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

type SpicedMemoryUsageMonitor = (
    CancellationToken,
    JoinHandle<anyhow::Result<Vec<MemoryReading>>>,
);

pub(crate) async fn run(args: &TextToSqlArgs) -> anyhow::Result<()> {
    let (app, spiced_instance) = run_or_connect_spiced(&args.common).await?;

    // Build resource with attributes known upfront, before creating telemetry.
    let spiced_version = spiced_instance.version().to_string();
    let spiced_commit_sha =
        std::env::var("SPICED_COMMIT").unwrap_or_else(|_| "unknown".to_string());
    let testoperator_commit_sha = git::get_commit_sha();
    let branch_name = git::get_branch_name();
    let run_name = args.get_configuration_name(&app.name);

    let text_to_sql_resource = Resource::builder_empty()
        .with_attributes(vec![
            KeyValue::new("service.name", "testoperator"),
            KeyValue::new("type", "text_to_sql"),
            KeyValue::new("name", run_name.clone()),
            KeyValue::new("spiced_version", spiced_version),
            KeyValue::new("spiced_commit_sha", spiced_commit_sha),
            KeyValue::new("model_name", args.model.clone()),
            KeyValue::new("testoperator_commit_sha", testoperator_commit_sha),
            KeyValue::new("branch_name", branch_name),
        ])
        .build();

    let telemetry =
        Telemetry::new_with_resource(&text_to_sql_resource, "SPICEAI_BENCHMARK_METRICS_KEY");

    // If we are running `spiced`, monitor its memory usage.
    let memory_handle_opt: Option<SpicedMemoryUsageMonitor> =
        spiced_instance.process().ok().map(|p| {
            let memory_token = CancellationToken::new();
            let handle = p.watch_memory(&memory_token);
            (memory_token, handle)
        });

    let test = SpiceTest::new(
        run_name,
        NotStarted::new()
            .with_config(
                args.construct_requests()
                    .context("Cannot make text-to-SQL test cases")?,
            )
            .with_parallel_count(args.common.concurrency),
    )
    .with_spiced_instance(spiced_instance)
    .start()
    .await?;

    let test = test.wait().await?;
    let memory_usage_opt = if let Some((memory_token, memory_readings)) = memory_handle_opt {
        let (max, median) = observe_memory(memory_token, memory_readings).await?;
        Some((max, median))
    } else {
        None
    };

    println!("Text-to-SQL requests completed, calculating results...");

    let metrics = test
        .collect(TestType::TextToSql)?
        .with_run_metric(test.get_run_metrics()?);

    let run_status = metrics.run_metric.as_ref().map(|rm| {
        if rm.error_rate > 0.0 {
            QueryStatus::Failed(None)
        } else {
            QueryStatus::Passed
        }
    });
    metrics.show_run(run_status)?;
    let () = emit_telemetry(telemetry, &metrics, memory_usage_opt).await?;

    let mut spiced_instance = test.end()?;
    if !args.common.is_external_instance() {
        spiced_instance.stop()?;
    }

    Ok(())
}

#[expect(clippy::cast_sign_loss)]
#[expect(clippy::cast_possible_truncation)]
async fn emit_telemetry(
    telemetry: Telemetry,
    metrics: &QueryMetrics<TextToSqlMetric, TextToSqlRunMetric>,
    memory_usage: Option<(f64, f64)>,
) -> Result<(), anyhow::Error> {
    crate::metrics::TEST_DURATION.record(
        u64::try_from(metrics.finished_at - metrics.started_at)?,
        &[],
    );

    if let Some((max_memory, median_memory)) = memory_usage {
        crate::metrics::PEAK_MEMORY_USAGE.record(max_memory * 1024.0, &[]);
        crate::metrics::MEDIAN_MEMORY_USAGE.record(median_memory * 1024.0, &[]);
    }

    // Record metrics, per query
    metrics
        .metrics
        .iter()
        .filter_map(|qm| qm.extended_metrics.as_ref())
        .for_each(|qm| {
            let attributes = vec![KeyValue::new("query_id", qm.question.clone())];

            crate::metrics::TEXT_TO_SQL_LATENCY.record(qm.latency_ms, &attributes);
            crate::metrics::TEXT_TO_SQL_SQL_DURATION.record(qm.sql_duration_ms, &attributes);
            crate::metrics::TEXT_TO_SQL_SQL_QUERY_COUNT
                .record(qm.sql_query_count as u64, &attributes);
            crate::metrics::TEXT_TO_SQL_LLM_DURATION.record(qm.llm_duration_ms, &attributes);
            crate::metrics::TEXT_TO_SQL_LLM_COUNT.record(qm.llm_count as u64, &attributes);
            crate::metrics::TEXT_TO_SQL_LLM_INPUT_TOKENS.record(qm.llm_input_tokens, &attributes);
            crate::metrics::TEXT_TO_SQL_LLM_OUTPUT_TOKENS.record(qm.llm_output_tokens, &attributes);
            crate::metrics::TEXT_TO_SQL_EXACT_MATCH.record(qm.exact_match, &attributes);
            crate::metrics::TEXT_TO_SQL_EXACT_LOGICAL_PLAN_MATCH
                .record(qm.exact_logical_plan_match, &attributes);
            crate::metrics::TEXT_TO_SQL_ERROR.record(u64::from(qm.is_error), &attributes);
            crate::metrics::TEXT_TO_SQL_CORRECT_TABLES.record(qm.correct_tables, &attributes);
            crate::metrics::TEXT_TO_SQL_CORRECT_TABLE_PROJECTIONS
                .record(qm.correct_table_projections, &attributes);
            crate::metrics::TEXT_TO_SQL_CORRECT_OUTPUT_SCHEMA
                .record(qm.correct_output_schema, &attributes);
        });

    // Record metrics, aggregate run-level
    if let Some(run_metrics) = &metrics.run_metric {
        crate::metrics::TEXT_TO_SQL_ERROR_RATE.record(run_metrics.error_rate, &[]);
        crate::metrics::TEXT_TO_SQL_EXACT_MATCH_RATE.record(run_metrics.exact_match_rate, &[]);
        crate::metrics::P95_DURATION.record(run_metrics.p95_latency_ms as u64, &[]);
        crate::metrics::MEDIAN_DURATION.record(run_metrics.median_latency_ms as u64, &[]);
        crate::metrics::TEXT_TO_SQL_MEAN_SQL_QUERY_COUNT
            .record(run_metrics.mean_sql_query_count, &[]);
        crate::metrics::TEXT_TO_SQL_MEAN_LLM_INPUT_TOKENS
            .record(run_metrics.mean_llm_input_tokens, &[]);
        crate::metrics::TEXT_TO_SQL_MEAN_LLM_OUTPUT_TOKENS
            .record(run_metrics.mean_llm_output_tokens, &[]);
    }

    telemetry.emit().await?;

    Ok(())
}
