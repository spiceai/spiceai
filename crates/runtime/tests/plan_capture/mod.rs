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

//! Integration tests for execution-time plan capture (`captured_plan: explain analyze`).
//!
//! Plan-row assertions are scoped by exact `input` so `wait_for_count` polling
//! queries (which themselves emit plan rows under `ExplainAnalyze`) cannot
//! inflate unscoped `task = 'plan'` counts.

use std::sync::Arc;
use std::time::Duration;

use app::AppBuilder;
use arrow::array::{AsArray, RecordBatch, StringArray};
use arrow::datatypes::Int64Type;
use futures::TryStreamExt;
use runtime::Runtime;
use spicepod::component::caching::SQLResultsCacheConfig;
use spicepod::component::dataset::Dataset;
use spicepod::component::runtime::{TaskHistoryCapturedContext, TaskHistoryCapturedPlan};
use spicepod::param::Params;

use crate::{
    configure_test_datafusion,
    utils::{
        init_tracing_with_task_history_plan_capture, register_test_connectors, runtime_ready_check,
        test_request_context,
    },
};

fn make_file_dataset(csv_path: &std::path::Path, name: &str) -> Dataset {
    let mut ds = Dataset::new(format!("file:{}", csv_path.display()), name.to_string());
    ds.params = Some(Params::from_string_map(
        vec![("file_format".to_string(), "csv".to_string())]
            .into_iter()
            .collect(),
    ));
    ds
}

async fn run_sql(rt: &Runtime, sql: &str) -> Result<Vec<RecordBatch>, anyhow::Error> {
    let result = rt
        .datafusion()
        .query_builder(sql)
        .build()
        .run()
        .await
        .map_err(|e| anyhow::Error::msg(e.to_string()))?;
    result
        .data
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| anyhow::Error::msg(e.to_string()))
}

async fn wait_for_count(
    rt: &Runtime,
    sql: &str,
    min: i64,
    timeout: Duration,
) -> Result<i64, anyhow::Error> {
    let deadline = std::time::Instant::now() + timeout;
    let mut last;
    loop {
        let rows = run_sql(rt, sql).await?;
        last = rows
            .first()
            .and_then(|b| b.column(0).as_primitive_opt::<Int64Type>())
            .map_or(0, |c| c.value(0));
        if last >= min {
            return Ok(last);
        }
        if std::time::Instant::now() >= deadline {
            return Err(anyhow::Error::msg(format!(
                "timeout waiting for ≥{min} rows from `{sql}`; last={last}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn col_str(batch: &RecordBatch, col: usize, row: usize) -> String {
    batch
        .column(col)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string column")
        .value(row)
        .to_string()
}

fn sql_escape(s: &str) -> String {
    s.replace('\'', "''")
}

#[tokio::test]
async fn plan_capture_explain_analyze_from_executed_plan() -> Result<(), anyhow::Error> {
    test_request_context()
        .scope(async {
            let tempdir = tempfile::tempdir().expect("tempdir");
            let csv_path = tempdir.path().join("names.csv");
            tokio::fs::write(&csv_path, "id,name\n1,alice\n2,bob\n3,carol\n").await?;

            configure_test_datafusion();
            register_test_connectors().await;

            let app = AppBuilder::new("plan_capture_analyze")
                .with_dataset(make_file_dataset(&csv_path, "names"))
                .build();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            let (_guard, provider) = init_tracing_with_task_history_plan_capture(
                Some("integration=debug,info"),
                &rt,
                TaskHistoryCapturedContext::Truncated,
                TaskHistoryCapturedPlan::ExplainAnalyze,
                None,
                None,
            );

            tokio::select! {
                () = tokio::time::sleep(Duration::from_mins(1)) => {
                    return Err(anyhow::anyhow!("timed out loading components"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }
            runtime_ready_check(&rt).await;

            let sql = "SELECT id, name FROM names ORDER BY id";
            let plan_input = format!("EXPLAIN ANALYZE {sql}");
            let plan_input_sql = sql_escape(&plan_input);
            let _ = run_sql(&rt, sql).await?;
            let _ = provider.force_flush();

            wait_for_count(
                &rt,
                &format!(
                    "SELECT count(*)::bigint FROM runtime.task_history \
                     WHERE task = 'sql_query' AND input = '{sql}'"
                ),
                1,
                Duration::from_secs(10),
            )
            .await?;
            wait_for_count(
                &rt,
                &format!(
                    "SELECT count(*)::bigint FROM runtime.task_history \
                     WHERE task = 'plan' AND labels['plan_capture'] = 'true' \
                     AND input = '{plan_input_sql}'"
                ),
                1,
                Duration::from_secs(10),
            )
            .await?;

            // Exactly one sql_query for the original input — no EXPLAIN ANALYZE re-run.
            let sql_query_count = wait_for_count(
                &rt,
                &format!(
                    "SELECT count(*)::bigint FROM runtime.task_history \
                     WHERE task = 'sql_query' AND input = '{sql}'"
                ),
                1,
                Duration::from_secs(2),
            )
            .await?;
            assert_eq!(sql_query_count, 1, "query must execute exactly once");

            let explain_rerun = wait_for_count(
                &rt,
                "SELECT count(*)::bigint FROM runtime.task_history \
                 WHERE task = 'sql_query' AND input LIKE 'EXPLAIN ANALYZE%'",
                0,
                Duration::from_secs(1),
            )
            .await
            .unwrap_or(0);
            assert_eq!(
                explain_rerun, 0,
                "ExplainAnalyze must not spawn a second sql_query"
            );

            let plan_rows = run_sql(
                &rt,
                &format!(
                    "SELECT span_id, parent_span_id, input, captured_output, labels['plan_capture'] \
                     FROM runtime.task_history \
                     WHERE task = 'plan' AND input = '{plan_input_sql}'"
                ),
            )
            .await?;
            assert_eq!(plan_rows.len(), 1);
            assert_eq!(plan_rows[0].num_rows(), 1);
            let parent = col_str(&plan_rows[0], 1, 0);
            let input = col_str(&plan_rows[0], 2, 0);
            let output = col_str(&plan_rows[0], 3, 0);
            let plan_capture = col_str(&plan_rows[0], 4, 0);
            assert_eq!(input, plan_input);
            assert_eq!(plan_capture, "true");
            assert!(
                output.contains("Plan with Metrics"),
                "missing Plan with Metrics wrapper: {output}"
            );
            assert!(
                output.contains("output_rows="),
                "missing output_rows metric from executed plan: {output}"
            );

            let parent_match = wait_for_count(
                &rt,
                &format!(
                    "SELECT count(*)::bigint FROM runtime.task_history \
                     WHERE task = 'sql_query' AND span_id = '{parent}'"
                ),
                1,
                Duration::from_secs(2),
            )
            .await?;
            assert_eq!(parent_match, 1, "plan row must parent to sql_query span");

            Ok(())
        })
        .await
}

#[tokio::test]
async fn plan_capture_skips_cache_hits() -> Result<(), anyhow::Error> {
    test_request_context()
        .scope(async {
            let tempdir = tempfile::tempdir().expect("tempdir");
            let csv_path = tempdir.path().join("names.csv");
            tokio::fs::write(&csv_path, "id,name\n1,alice\n2,bob\n3,carol\n").await?;

            configure_test_datafusion();
            register_test_connectors().await;

            let app = AppBuilder::new("plan_capture_cache")
                .with_sql_cache(SQLResultsCacheConfig {
                    item_ttl: Some("60s".to_string()),
                    ..Default::default()
                })
                .with_dataset(make_file_dataset(&csv_path, "names"))
                .build();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            let (_guard, provider) = init_tracing_with_task_history_plan_capture(
                Some("integration=debug,info"),
                &rt,
                TaskHistoryCapturedContext::Truncated,
                TaskHistoryCapturedPlan::ExplainAnalyze,
                None,
                None,
            );

            tokio::select! {
                () = tokio::time::sleep(Duration::from_mins(1)) => {
                    return Err(anyhow::anyhow!("timed out loading components"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }
            runtime_ready_check(&rt).await;

            let sql = "SELECT id FROM names WHERE id = 1";
            let plan_input = format!("EXPLAIN ANALYZE {sql}");
            let plan_input_sql = sql_escape(&plan_input);
            let _ = run_sql(&rt, sql).await?;
            let _ = run_sql(&rt, sql).await?;
            let _ = provider.force_flush();

            wait_for_count(
                &rt,
                &format!(
                    "SELECT count(*)::bigint FROM runtime.task_history \
                     WHERE task = 'plan' AND input = '{plan_input_sql}'"
                ),
                1,
                Duration::from_secs(10),
            )
            .await?;

            // Give the second (cache-hit) query time to flush if it wrongly emitted a plan.
            tokio::time::sleep(Duration::from_millis(500)).await;
            let _ = provider.force_flush();

            let plan_count = wait_for_count(
                &rt,
                &format!(
                    "SELECT count(*)::bigint FROM runtime.task_history \
                     WHERE task = 'plan' AND input = '{plan_input_sql}'"
                ),
                1,
                Duration::from_secs(2),
            )
            .await?;
            assert_eq!(
                plan_count, 1,
                "cache-hit query must not emit a second plan row"
            );

            Ok(())
        })
        .await
}

#[tokio::test]
async fn plan_capture_explain_mode_still_replans() -> Result<(), anyhow::Error> {
    test_request_context()
        .scope(async {
            let tempdir = tempfile::tempdir().expect("tempdir");
            let csv_path = tempdir.path().join("names.csv");
            tokio::fs::write(&csv_path, "id,name\n1,alice\n2,bob\n3,carol\n").await?;

            configure_test_datafusion();
            register_test_connectors().await;

            let app = AppBuilder::new("plan_capture_explain")
                .with_dataset(make_file_dataset(&csv_path, "names"))
                .build();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            let (_guard, provider) = init_tracing_with_task_history_plan_capture(
                Some("integration=debug,info"),
                &rt,
                TaskHistoryCapturedContext::Truncated,
                TaskHistoryCapturedPlan::Explain,
                None,
                None,
            );

            tokio::select! {
                () = tokio::time::sleep(Duration::from_mins(1)) => {
                    return Err(anyhow::anyhow!("timed out loading components"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }
            runtime_ready_check(&rt).await;

            let sql = "SELECT name FROM names WHERE id = 2";
            // Exporter Explain path prefixes with `EXPLAIN ` (not ANALYZE).
            let plan_input = format!("EXPLAIN {sql}");
            let plan_input_sql = sql_escape(&plan_input);
            let _ = run_sql(&rt, sql).await?;
            // Flush writes the sql_query and spawns the Explain re-plan, which
            // writes the plan row via TaskSpan::write on the exporter worker.
            let _ = provider.force_flush();

            wait_for_count(
                &rt,
                &format!(
                    "SELECT count(*)::bigint FROM runtime.task_history \
                     WHERE task = 'plan' AND input = '{plan_input_sql}'"
                ),
                1,
                Duration::from_secs(15),
            )
            .await?;

            let plan_rows = run_sql(
                &rt,
                &format!(
                    "SELECT input FROM runtime.task_history \
                     WHERE task = 'plan' AND input = '{plan_input_sql}'"
                ),
            )
            .await?;
            assert_eq!(plan_rows[0].num_rows(), 1);
            let input = col_str(&plan_rows[0], 0, 0);
            assert_eq!(input, plan_input);
            assert!(
                !input.starts_with("EXPLAIN ANALYZE "),
                "Explain mode should re-plan with EXPLAIN, got: {input}"
            );

            Ok(())
        })
        .await
}
