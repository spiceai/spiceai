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
    let (app, start_request) = get_app_and_start_request(&args.common)?;
    let mut spiced_instance = SpicedInstance::start(start_request).await?;

    let eval = args
        .eval
        .as_ref()
        .or_else(|| app.evals.first().map(|eval| &eval.name))
        .ok_or_else(|| anyhow::anyhow!("No evals defined"))?;

    let model = args
        .model
        .as_ref()
        .or_else(|| app.models.first().map(|model| &model.name))
        .ok_or_else(|| anyhow::anyhow!("No models defined"))?;

    spiced_instance
        .wait_for_ready(Duration::from_secs(args.common.ready_wait))
        .await?;

    println!("Executing {eval} eval benchmark for model {model}. It might take several minutes...");

    let http_client = spiced_instance.http_client()?;

    let url = format!("http://localhost:8090/v1/evals/{eval}");
    let body = json!({"model": model}).to_string();

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

    let failed_tests = execute_sql(&mut flight_client, QUERY_EVAL_BENCHMARK_FAILED_TESTS).await?;
    // json format is easier to read as table could be too wide
    println!("Failed tests:\n{}\n", arrow_to_json(&failed_tests)?);

    let top_errors = execute_sql(&mut flight_client, QUERY_EVAL_BENCHMARK_TOP_ERRORS).await?;
    // json format is easier to read as table could be too wide
    println!("Top errors:\n{}\n", arrow_to_json(&top_errors)?);

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

/**
 * Fetches key metrics for the latest evaluation run, including duration, evaluation score, task call counts, and errors.
 *
 * Output:
 * - `run_id`: Evaluation run ID  
 * - `model`: Model name  
 * - `status`: Run status  
 * - `tests`: Number of tests performed
 * - `duration_seconds`: Eval duration (seconds)  
 * - `score`: Rounded average score
 * - `task_calls`: Total task invocations  
 * - `task_errors`: Task task errors  
 *
 * Example:
 * +----------------------------------+-------------+-----------+-------+------------------+--------+------------+-------------+
 * | `run_id`                         | `model`     | `status`  |`tests`|`duration_seconds`|`score` |`task_calls`|`task_errors`|
 * +----------------------------------+-------------+-----------+-------+------------------+--------+------------+-------------+
 * | c74a65614ea314bc7036489bbc6f7ba3 | gpt-4o-mini | Completed | 11    | 83.0             | 0.8182 | 209        | 100         |
 * +----------------------------------+-------------+-----------+-------+------------------+--------+------------+-------------+
 */
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

/**
 * Retrieves statistis on executed tasks/tools during the latest evaluation run.
 *
 * Output:
 * - `task`: Task name  
 * - `calls`: Total number of task calls  
 * - `failures`: Total number of task failures  
 * - `duration_ms`: Aggregated task duration in milliseconds  
 *
 * Example:
 * +-------------------------+-------+----------+--------------------+
 * | `task`                  |`calls`|`failures`| `duration_ms`      |
 * +-------------------------+-------+----------+--------------------+
 * |`ai_completion`          | 72    | 0        | 77136.66854858398  |
 * |`sql_query`              | 64    | 50       | 140.39500185847282 |
 * |`tool_use::sql`          | 62    | 50       | 135.9049990773201  |
 * |`tool_use::list_datasets`| 11    | 0        | 0.9290000051259995 |
 * +-------------------------+-------+----------+--------------------+
 */
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

/**
 * Fetches the top task errors for the latest evaluation run aggregated by associated task name, error message, and input
 *
 * Output:
 * - `task`: Task name  
 * - `count`: Number of error occurrences  
 * - `message`: Error message  
 * - `input`: Input causing the error  
 *
 * Example:
 * +---------------+-------+---------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------+
 * | `task`        |`count`| `message`                                                                             | `input`                                                                           |
 * +---------------+-------+---------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------+
 * |`tool_use::sql`| 17    | Failed to execute query: SQL error: ParserError("Expected: an expression:, found: '") | {"query":"SELECT `nation` FROM `spice`.`public`.`customer` WHERE `c_custkey` = 1"}|
 * +---------------+-------+---------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------+
 */
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

/**
 * Fetches the failed tests for the latest evaluation run.
 *
 * Output:
 * - `run_id`: Evaluation run ID  
 * - `input`: Test input query  
 * - `output`: Model response  
 * - `expected`: Expected response  
 * - `score`: Test score  
 *
 * Example:
 * +----------------------------------+----------------------------------+----------------------------------+----------------------------------+-------+
 * | `run_id`                         | `input`                          | `output`                         | `expected`                       |`score`|
 * +----------------------------------+----------------------------------+----------------------------------+----------------------------------+-------+
 * | c74a65614ea314bc7036489bbc6f7ba3 | get part brand for part key 3    | Information is not available     | {`part_brand`: `Brand#42`}       | 0.0   |
 * +----------------------------------+----------------------------------+----------------------------------+----------------------------------+-------+
 */
static QUERY_EVAL_BENCHMARK_FAILED_TESTS: &str = "
WITH latest_run AS (
    SELECT id FROM spice.eval.runs ORDER BY created_at DESC LIMIT 1
)
SELECT run_id, input, output, actual as expected, value as score
FROM eval.results
WHERE run_id = (SELECT id FROM latest_run) and value < 1;
";

/// Converts a vector of `RecordBatch` to a JSON string.
fn arrow_to_json(data: &[RecordBatch]) -> Result<String, anyhow::Error> {
    let buf = Vec::new();
    let mut writer = arrow_json::ArrayWriter::new(buf);

    writer.write_batches(&data.iter().collect::<Vec<_>>())?;
    writer.finish()?;

    let json_str = String::from_utf8(writer.into_inner()).map_err(anyhow::Error::from)?;

    // Pretty-print the JSON output
    let json_value: serde_json::Value = serde_json::from_str(&json_str)?;
    serde_json::to_string_pretty(&json_value).map_err(anyhow::Error::from)
}
