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

use crate::args::EvalsTestArgs;

use super::get_app_and_start_request;
use serde_json::json;
use std::time::Duration;
use test_framework::{
    anyhow,
    arrow::{array::RecordBatch, util::pretty::pretty_format_batches},
    flight_client::FlightClient,
    futures::TryStreamExt,
    spiced::SpicedInstance,
};

#[allow(clippy::too_many_lines)]
pub(crate) async fn run(args: &EvalsTestArgs) -> anyhow::Result<()> {
    let (_, start_request) = get_app_and_start_request(&args.common)?;
    let mut spiced_instance = SpicedInstance::start(start_request).await?;

    spiced_instance
        .wait_for_ready(Duration::from_secs(args.common.ready_wait))
        .await?;

    println!(
        "Executing {} eval benchmark. It might take several minutes...",
        args.eval
    );

    let http_client = spiced_instance.http_client()?;

    let url = format!("http://localhost:8090/v1/evals/{}", args.eval);
    let body = json!({"model": args.model}).to_string();

    let response = http_client
        .post(&url)
        .header("Content-Type", "application/json")
        .body(body)
        .send()
        .await?;

    if !response.status().is_success() {
        return Err(anyhow::anyhow!(
            "Failed to execute evals: {}",
            response.text().await?
        ));
    }

    println!("Execution completed, retrieving results...");

    let mut flight_client = spiced_instance.flight_client().await?;

    let eval_result = execute_sql(&mut flight_client, QUERY_EVAL_BENCHMARK_MAIN_METRICS).await?;
    println!("Result:\n{}\n", pretty_format_batches(&eval_result)?);

    let tasks_calls = execute_sql(&mut flight_client, QUERY_EVAL_BENCHMARK_TASKS).await?;
    println!(
        "Executed tasks:\n{}\n",
        pretty_format_batches(&tasks_calls)?
    );

    let top_errors = execute_sql(&mut flight_client, QUERY_EVAL_BENCHMARK_TOP_ERRORS).await?;
    println!("Top errors:\n{}\n", pretty_format_batches(&top_errors)?);

    spiced_instance.stop()?;

    println!("Benchmark completed");

    Ok(())
}

async fn execute_sql(
    flight_client: &mut FlightClient,
    sql: &str,
) -> Result<Vec<RecordBatch>, anyhow::Error> {
    let res = flight_client
        .query(sql)
        .await?
        .try_collect::<Vec<RecordBatch>>()
        .await?;
    Ok(res)
}

static QUERY_EVAL_BENCHMARK_MAIN_METRICS: &str = "
WITH latest_run AS (
    SELECT id, created_at, EXTRACT(EPOCH FROM (completed_at - created_at)) AS duration_seconds
    FROM spice.eval.runs
    ORDER BY created_at DESC LIMIT 1
),
score AS (
    SELECT run_id, AVG(value) AS overall_score, COUNT(*) AS evals_count
    FROM spice.eval.results
    WHERE run_id = (SELECT id FROM latest_run)
    GROUP BY run_id
),
tool_stats AS (
    SELECT 
        COUNT(*) AS task_calls,
        COUNT(CASE WHEN error_message IS NOT NULL THEN 1 END) AS task_errors
    FROM runtime.task_history
    WHERE 
        task != 'test_connectivity'
        AND start_time BETWEEN (SELECT created_at FROM latest_run)
        AND COALESCE(end_time, NOW())
)
SELECT r.id AS run_id, r.model, r.status, s.evals_count AS tests, lr.duration_seconds, ROUND(s.overall_score, 4) as score, ts.task_calls, ts.task_errors
FROM spice.eval.runs r
JOIN latest_run lr ON r.id = lr.id
LEFT JOIN score s ON r.id = s.run_id
LEFT JOIN tool_stats ts ON 1 = 1;
";

static QUERY_EVAL_BENCHMARK_TASKS: &str = "
WITH latest_run AS (
  SELECT id 
  FROM spice.eval.runs 
  ORDER BY created_at DESC 
  LIMIT 1
)
SELECT 
  task, 
  COUNT(*) AS calls,
  COUNT(CASE WHEN error_message IS NOT NULL THEN 1 END) AS failures,
  SUM(CAST((end_time - start_time) AS Float) /  1000000) AS duration_ms
FROM runtime.task_history
WHERE 
  task != 'test_connectivity'
  AND start_time BETWEEN (SELECT created_at FROM spice.eval.runs WHERE id = (SELECT id FROM latest_run)) AND 
  COALESCE(end_time, NOW())
GROUP BY task
ORDER BY duration_ms DESC;
";

static QUERY_EVAL_BENCHMARK_TOP_ERRORS: &str = "
WITH latest_run AS (
  SELECT id 
  FROM spice.eval.runs 
  ORDER BY created_at DESC 
  LIMIT 1
)
SELECT 
    task,
    COUNT(*) AS count,
    error_message as message,
    input
FROM 
    runtime.task_history
WHERE 
    error_message IS NOT NULL
    AND start_time BETWEEN (SELECT created_at FROM spice.eval.runs WHERE id = (SELECT id FROM latest_run)) AND 
  	COALESCE(end_time, NOW())
GROUP BY 
    task, input, message
ORDER BY 
    count DESC
LIMIT 20;
";
