/*
Copyright 2026 The Spice.ai OSS Authors

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

use crate::utils::wait_until_true;
use std::{sync::Arc, time::Duration};
use tokio::time::sleep;

use anyhow::Result;
use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use futures::TryStreamExt;
use opentelemetry::trace::TraceId;

/// Metrics from `runtime.task_history` for a `nsql` operation.
#[derive(Debug, Clone, Default)]
pub struct TaskHistoryMetrics {
    pub sql_duration_ms: f64,
    pub sql_count: usize,
    pub llm_duration_ms: f64,
    pub llm_count: usize,
    pub llm_input_tokens: u64,
    pub llm_output_tokens: u64,
    /// The last SQL query generated as a candidate answer (after an AI completion).
    pub generated_sql: String,
    /// All SQL queries that followed an `ai_completion` task (candidate answers).
    pub attempted_sql: Vec<String>,
}

/// Fetches metrics from `runtime.task_history` for an nsql operation with the given `trace_id`.
///
/// This query uses the same logic as the Go implementation:
/// - Uses `ordered_tasks` CTE to find the first `ai_completion` task
/// - `last_candidate_sql` gets the last SQL query that occurred after an AI completion
/// - `attempted_sql` gets all SQL queries that occurred after the first AI completion
///
/// Returns: `(generated_sql, task_history_metrics)` for backward compatibility.
/// The `generated_sql` is also available in `TaskHistoryMetrics::generated_sql`.
pub async fn find_task_history_metrics(
    spice_client: &spiceai::Client,
    trace_id: &TraceId,
) -> Result<(Option<String>, TaskHistoryMetrics)> {
    // Query to get metrics with token counts extracted via SQL
    // This matches the Go implementation in queryTaskHistoryMetrics
    // Updated to exclude SQL queries that are children of tool_use::* tasks
    let query = format!(
        r"
WITH all_tasks AS (
    SELECT
        task,
        span_id,
        parent_span_id,
        input,
        execution_duration_ms,
        start_time,
        labels
    FROM runtime.task_history
    WHERE trace_id = '{trace_id}'
),
tool_spans AS (
    SELECT span_id
    FROM all_tasks
    WHERE task LIKE 'tool_use::%'
),
ordered_tasks AS (
    SELECT
        task,
        parent_span_id,
        input,
        execution_duration_ms,
        start_time,
        labels,
        MIN(CASE WHEN task = 'ai_completion' THEN start_time END) OVER () AS first_ai_completion_time
    FROM all_tasks
    WHERE task IN ('sql_query', 'ai_completion')
),
candidate_sql AS (
    SELECT *
    FROM ordered_tasks
    WHERE task = 'sql_query'
      AND first_ai_completion_time IS NOT NULL
      AND start_time > first_ai_completion_time
      AND parent_span_id NOT IN (SELECT span_id FROM tool_spans)
),
sql_stats AS (
    SELECT
        COUNT(*) AS sql_count,
        COALESCE(SUM(execution_duration_ms), 0) AS sql_duration_ms
    FROM candidate_sql
),
llm_stats AS (
    SELECT
        COUNT(*) AS llm_count,
        COALESCE(SUM(execution_duration_ms), 0) AS llm_duration_ms,
        SUM(CAST(COALESCE(labels['prompt_tokens'], '0') AS BIGINT)) AS llm_input_tokens,
        SUM(CAST(COALESCE(labels['completion_tokens'], '0') AS BIGINT)) AS llm_output_tokens
    FROM ordered_tasks
    WHERE task = 'ai_completion'
),
last_candidate_sql AS (
    SELECT COALESCE(input, '') AS generated_sql
    FROM candidate_sql
    ORDER BY start_time DESC
    LIMIT 1
),
trace_check AS (
    SELECT COUNT(*) > 0 AS trace_exists
    FROM runtime.task_history
    WHERE trace_id = '{trace_id}'
)
SELECT
    COALESCE(s.sql_count, 0) AS sql_count,
    COALESCE(s.sql_duration_ms, 0.0) AS sql_duration_ms,
    COALESCE(l.llm_count, 0) AS llm_count,
    COALESCE(l.llm_duration_ms, 0.0) AS llm_duration_ms,
    COALESCE(l.llm_input_tokens, 0) AS llm_input_tokens,
    COALESCE(l.llm_output_tokens, 0) AS llm_output_tokens,
    COALESCE(ls.generated_sql, '') AS generated_sql
FROM sql_stats s
LEFT JOIN last_candidate_sql ls ON 1=1
LEFT JOIN llm_stats l ON 1=1
CROSS JOIN trace_check tc
WHERE tc.trace_exists = true
"
    );

    let data = retry_query_until_llm_found(spice_client, &query, Duration::from_secs(15)).await;

    let Some(rb) = data.as_ref().and_then(|s| s.first()) else {
        return Err(anyhow::anyhow!(
            "could not find task history metrics for text to SQL"
        ));
    };

    if rb.num_rows() == 0 {
        return Err(anyhow::anyhow!(
            "no task history rows found for text to SQL"
        ));
    }

    #[expect(clippy::cast_possible_truncation)]
    #[expect(clippy::cast_sign_loss)]
    let sql_count = rb
        .column_by_name("sql_count")
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        .map_or(0, |a| a.value(0) as usize);

    let sql_duration_ms = rb
        .column_by_name("sql_duration_ms")
        .and_then(|c| c.as_any().downcast_ref::<arrow::array::Float64Array>())
        .map_or(0.0, |a| a.value(0));

    #[expect(clippy::cast_possible_truncation)]
    #[expect(clippy::cast_sign_loss)]
    let llm_count = rb
        .column_by_name("llm_count")
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        .map_or(0, |a| a.value(0) as usize);

    let llm_duration_ms = rb
        .column_by_name("llm_duration_ms")
        .and_then(|c| c.as_any().downcast_ref::<arrow::array::Float64Array>())
        .map_or(0.0, |a| a.value(0));

    #[expect(clippy::cast_sign_loss)]
    let llm_input_tokens = rb
        .column_by_name("llm_input_tokens")
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        .map_or(0, |a| a.value(0) as u64);

    #[expect(clippy::cast_sign_loss)]
    let llm_output_tokens = rb
        .column_by_name("llm_output_tokens")
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        .map_or(0, |a| a.value(0) as u64);

    let generated_sql = rb
        .column_by_name("generated_sql")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .map(|a| a.value(0).to_string())
        .unwrap_or_default();

    // Get all candidate SQL queries (those after first ai_completion)
    let attempted_sql = fetch_attempted_sql(spice_client, trace_id, Duration::from_secs(5)).await;

    let metrics = TaskHistoryMetrics {
        sql_duration_ms,
        sql_count,
        llm_duration_ms,
        llm_count,
        llm_input_tokens,
        llm_output_tokens,
        generated_sql: generated_sql.clone(),
        attempted_sql,
    };

    let generated_sql_opt = if generated_sql.is_empty() {
        None
    } else {
        Some(generated_sql)
    };

    Ok((generated_sql_opt, metrics))
}

/// Fetches all SQL queries that occurred after the first `ai_completion` task.
/// These are considered candidate answers generated by the LLM.
async fn fetch_attempted_sql(
    spice_client: &spiceai::Client,
    trace_id: &TraceId,
    timeout: Duration,
) -> Vec<String> {
    // This query finds SQL queries that are candidate answers (not tool sampling queries).
    // It excludes SQL queries that are children of tool_use::* tasks (like sample_data, table_schema).
    let query = format!(
        r"
WITH trace_tasks AS (
    SELECT
        task,
        span_id,
        parent_span_id,
        input,
        start_time
    FROM runtime.task_history
    WHERE trace_id = '{trace_id}'
),
tool_spans AS (
    SELECT span_id
    FROM trace_tasks
    WHERE task LIKE 'tool_use::%'
),
first_ai_completion AS (
    SELECT MIN(start_time) AS first_ai_time
    FROM trace_tasks
    WHERE task = 'ai_completion'
)
SELECT t.input
FROM trace_tasks t
CROSS JOIN first_ai_completion f
WHERE t.task = 'sql_query'
  AND f.first_ai_time IS NOT NULL
  AND t.start_time > f.first_ai_time
  AND t.input IS NOT NULL
  AND t.input != ''
  AND t.parent_span_id NOT IN (SELECT span_id FROM tool_spans)
ORDER BY t.start_time ASC
"
    );

    let data = retry_query_expecting_results(spice_client, &query, timeout).await;

    let Some(batches) = data else {
        return Vec::new();
    };

    let mut attempted = Vec::new();
    for rb in batches {
        if let Some(input_col) = rb
            .column_by_name("input")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        {
            for i in 0..input_col.len() {
                if !input_col.is_null(i) {
                    let input = input_col.value(i);
                    if !input.is_empty() {
                        attempted.push(input.to_string());
                    }
                }
            }
        }
    }

    attempted
}

/// Retry query until we get results with `llm_count > 0`.
/// This ensures we wait for the `ai_completion` tasks to be written to `task_history`.
async fn retry_query_until_llm_found(
    spice_client: &spiceai::Client,
    query: &str,
    wait_for: Duration,
) -> Option<Vec<RecordBatch>> {
    let query = query.to_string();
    let data = Arc::new(tokio::sync::Mutex::new(None));

    wait_until_true(wait_for, || {
        let spice_client = spice_client.clone();
        let query = query.clone();
        let data = Arc::clone(&data);
        async move {
            match spice_client.sql(&query).await {
                Ok(stream) => {
                    let Some(rbs) = stream.try_collect::<Vec<RecordBatch>>().await.ok() else {
                        sleep(Duration::from_secs(1)).await;
                        return false;
                    };

                    let Some(rb) = rbs.first() else {
                        sleep(Duration::from_secs(1)).await;
                        return false;
                    };

                    if rb.num_rows() == 0 {
                        sleep(Duration::from_secs(1)).await;
                        return false;
                    }

                    // Check if llm_count > 0
                    let llm_count = rb
                        .column_by_name("llm_count")
                        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
                        .map_or(0, |a| a.value(0));

                    if llm_count > 0 {
                        *data.lock().await = Some(rbs);
                        true
                    } else {
                        sleep(Duration::from_secs(1)).await;
                        false
                    }
                }
                Err(_) => false,
            }
        }
    })
    .await;

    (data.lock().await).clone()
}

async fn retry_query_expecting_results(
    spice_client: &spiceai::Client,
    query: &str,
    wait_for: Duration,
) -> Option<Vec<RecordBatch>> {
    let query = query.to_string();
    let data = Arc::new(tokio::sync::Mutex::new(None));

    wait_until_true(wait_for, || {
        let spice_client = spice_client.clone();
        let query = query.clone();
        let data = Arc::clone(&data);
        async move {
            match spice_client.sql(&query).await {
                Ok(stream) => {
                    let Some(rbs) = stream.try_collect::<Vec<RecordBatch>>().await.ok() else {
                        sleep(Duration::from_secs(1)).await;
                        return false;
                    };
                    if rbs.first().is_none_or(|rb| rb.num_rows() == 0) {
                        sleep(Duration::from_secs(1)).await;
                        false
                    } else {
                        *data.lock().await = Some(rbs);
                        true
                    }
                }
                Err(_) => false,
            }
        }
    })
    .await;

    (data.lock().await).clone()
}
