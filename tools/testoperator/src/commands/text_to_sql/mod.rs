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
    let telemetry = Telemetry::new("SPICEAI_BENCHMARK_METRICS_KEY");

    let (app, spiced_instance) = run_or_connect_spiced(&args.common).await?;

    // If we are running `spiced`, monitor its memory usage.
    let memory_handle_opt: Option<SpicedMemoryUsageMonitor> =
        spiced_instance.process().ok().map(|p| {
            let memory_token = CancellationToken::new();
            let handle = p.watch_memory(&memory_token);
            (memory_token, handle)
        });

    let test = SpiceTest::new(
        app.name.clone(),
        NotStarted::new().with_config(
            args.construct_requests()
                .context("Cannot make text-to-SQL test cases")?,
        ),
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

    metrics.show_run(metrics.run_metric.as_ref().map(|m| {
        if m.error_rate > 0.0 {
            QueryStatus::Failed(None)
        } else {
            QueryStatus::Passed
        }
    }))?;
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
    mut telemetry: Telemetry,
    metrics: &QueryMetrics<TextToSqlMetric, TextToSqlRunMetric>,
    memory_usage: Option<(f64, f64)>,
) -> Result<(), anyhow::Error> {
    telemetry.set_resource(
        Resource::builder_empty()
            .with_attributes(vec![
                KeyValue::new("service.name", "testoperator"),
                KeyValue::new("type", "text_to_sql"),
                KeyValue::new("name", metrics.run_name.clone()),
                KeyValue::new("spiced_version", metrics.spiced_version.clone()),
                KeyValue::new("spiced_commit_sha", metrics.commit_sha.clone()),
                KeyValue::new("testoperator_commit_sha", git::get_commit_sha()),
                KeyValue::new("branch_name", git::get_branch_name()),
            ])
            .build(),
    );
    crate::metrics::TEST_DURATION.record(
        u64::try_from(metrics.finished_at - metrics.started_at)?,
        &[],
    );

    if let Some((max_memory, median_memory)) = memory_usage {
        crate::metrics::PEAK_MEMORY_USAGE.record(max_memory * 1024.0, &[]);
        crate::metrics::MEDIAN_MEMORY_USAGE.record(median_memory * 1024.0, &[]);
    }

    if let Some(run_metrics) = &metrics.run_metric {
        crate::metrics::TEXT_TO_SQL_ERROR_RATE.record(run_metrics.error_rate, &[]);
        crate::metrics::TEXT_TO_SQL_EXACT_MATCH_RATE.record(run_metrics.exact_match_rate, &[]);
        crate::metrics::AVERAGE_TEXT_TO_SQL_ATTEMPTS.record(run_metrics.avg_attempts, &[]);
        crate::metrics::P95_DURATION.record(run_metrics.p95_latency_ms as u64, &[]);
        crate::metrics::MEDIAN_DURATION.record(run_metrics.median_latency_ms as u64, &[]);
    }

    telemetry.emit().await?;

    Ok(())
}
