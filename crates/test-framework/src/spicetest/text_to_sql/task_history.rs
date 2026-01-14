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
use arrow::array::{Int64Array, RecordBatch, StringArray};
use futures::TryStreamExt;
use opentelemetry::trace::TraceId;

/// Metrics from `runtime.task_history` for a `nsql` operation.
#[derive(Debug, Clone)]
pub(crate) struct TaskHistoryMetrics {
    pub sql_duration_ms: f64,
    pub sql_count: usize,
    pub llm_duration_ms: f64,
    pub llm_count: usize,
    pub llm_input_tokens: u64,
    pub llm_output_tokens: u64,
}

/// Fetches metrics from `runtime.task_history` for an nsql operation with the given `trace_id`.
///
/// Returns: `(generated_sql, task_history_metrics)`
pub(super) async fn find_task_history_metrics(
    spice_client: &spiceai::Client,
    trace_id: &TraceId,
) -> Result<(Option<String>, TaskHistoryMetrics)> {
    let query = format!(
        "
WITH sql_stats AS (
    SELECT
        COUNT(*) AS sql_count,
        COALESCE(SUM(execution_duration_ms), 0) AS sql_duration_ms
    FROM runtime.task_history
    WHERE trace_id = '{trace_id}'
      AND task = 'sql_query'
),
llm_stats AS (
    SELECT
        COUNT(*) AS llm_count,
        COALESCE(SUM(execution_duration_ms), 0) AS llm_duration_ms,
        SUM(CAST(COALESCE(labels['prompt_tokens'], '0') AS BIGINT)) AS llm_input_tokens,
        SUM(CAST(COALESCE(labels['completion_tokens'], '0') AS BIGINT)) AS llm_output_tokens
    FROM runtime.task_history
    WHERE trace_id = '{trace_id}'
      AND task = 'ai_completion'
),
last_sql AS (
    SELECT COALESCE(input, '')  AS generated_sql
    FROM runtime.task_history
    WHERE trace_id = '{trace_id}'
      AND task = 'sql_query'
    ORDER BY end_time DESC
    LIMIT 1
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
LEFT JOIN last_sql ls ON 1=1
LEFT JOIN llm_stats l ON 1=1
"
    );
    let data = retry_query_expecting_results(spice_client, &query, Duration::from_secs(15)).await;

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
        .map(|a| a.value(0).to_string());

    Ok((
        generated_sql,
        TaskHistoryMetrics {
            sql_duration_ms,
            sql_count,
            llm_duration_ms,
            llm_count,
            llm_input_tokens,
            llm_output_tokens,
        },
    ))
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
            match spice_client.query(&query).await {
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
