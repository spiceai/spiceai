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

use crate::args::{CommonArgs, TextToSqlArgs, TextToSqlQuery};
use arrow::array::{Int64Array, RecordBatch, StringArray};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use test_framework::{
    anyhow,
    constants::HTTP_BASE_URL,
    futures::TryStreamExt,
    spiced::{SpicedInstance, StartRequest},
    utils::wait_until_true,
};
use tokio::time::{Instant, sleep};

use super::get_app_and_start_request;
use crate::health::HealthMonitor;

pub(crate) async fn run(args: &TextToSqlArgs) -> anyhow::Result<()> {
    let queries = args.load_queries()?;

    println!("Running text-to-sql test with {} queries", queries.len());

    let (_app, start_request) = get_app_and_start_request(&args.common).await?;
    let mut spiced_instance = run_spice(&args.common, start_request).await?;
    let spiced_client: spiceai::Client = spiced_instance.spice_client(None, true).await?;

    let health_monitor = HealthMonitor::spawn()?;

    for sample_data_enabled in args.sample_data_enabled.values() {
        for return_sql in args.return_sql.values() {
            for TextToSqlQuery {
                question,
                expected_sql,
            } in &queries
            {
                match run_single_test(
                    &spiced_instance,
                    &spiced_client,
                    &args.model,
                    &question,
                    sample_data_enabled,
                    return_sql,
                )
                .await
                {
                    Ok(TestRunOutputs {
                        sql,
                        number_of_attempts,
                        duration,
                    }) => {
                        let payload = json!({
                            "query": question,
                            "sample_data_enabled": sample_data_enabled,
                            "return_sql": return_sql,
                            "expected_sql": expected_sql.replace("\n", "\n      "),
                            "generated_sql": sql.replace("\n", "\n      "),
                            "duration_ms": duration.as_millis(),
                            "number_of_attempts": number_of_attempts,
                        });
                        println!(
                            "{}",
                            serde_json::to_string(&payload)
                                .expect("could not serialize text-to-sql test result to JSON")
                        );
                    }
                    Err(e) => {
                        println!("✗ Query: {question} - ERROR: {e}");
                    }
                }
            }
        }
    }

    let health_report = health_monitor.stop().await;

    if !args.common.is_external_instance() {
        spiced_instance.stop()?;
    }

    let health_report = health_report?;
    if let Some(message) = health_report.failure_message() {
        return Err(anyhow::anyhow!(message));
    }

    Ok(())
}

async fn run_spice(
    common: &CommonArgs,
    start_request: StartRequest,
) -> anyhow::Result<SpicedInstance> {
    if common.is_external_instance() {
        println!("Using external spiced instance at {}", common.spiced_path);
        Ok(SpicedInstance::external(common.spiced_path.clone()))
    } else {
        let mut spiced = SpicedInstance::start(start_request).await?;
        spiced
            .wait_for_ready(Duration::from_secs(common.ready_wait))
            .await?;
        Ok(spiced)
    }
}

/// Data needed from the result of text-to-SQL attempt to use to generate measurements.
pub struct TestRunOutputs {
    sql: String,
    // Internally, determine how many attempts were made to get a valid SQL response.
    // When `Accept: application/sql` this is currently 1.
    number_of_attempts: usize,
    duration: Duration,
}

async fn run_single_test(
    spiced_instance: &SpicedInstance,
    spice_client: &spiceai::Client,
    model_name: &str,
    question: &str,
    sample_data_enabled: bool,
    return_sql: bool,
) -> anyhow::Result<TestRunOutputs> {
    let http_client = spiced_instance.http_client()?;

    let url = format!("{HTTP_BASE_URL}/v1/nsql");
    let body = json!({
        "query": question,
        "model": model_name,
        "sample_data_enabled": sample_data_enabled,
        "stream": false
    });
    let accept_header = if return_sql {
        "application/sql"
    } else {
        "application/json"
    };

    let start = Instant::now();
    let request = http_client
        .post(&url)
        .header("Content-Type", "application/json")
        .header("Accept", accept_header);

    let response = request.body(body.to_string()).send().await?;
    let status = response.status();
    let text = response.text().await?;

    let duration = start.elapsed();

    if !status.is_success() {
        return Err(anyhow::anyhow!("HTTP error: {text}"));
    }

    let (number_of_attempts, sql) = if return_sql {
        (1, text)
    } else {
        // Must get SQL first, since `number_of_attempts` will return 0 if trace is not in `runtime.task_history`.
        let sql = find_last_sql_statement(spice_client)
            .await
            .map_err(|e| anyhow::anyhow!("could not find last sql_query statement. Error: {e}"))?;
        let number_of_attempts = find_number_of_sql_attempts(spice_client)
            .await
            .map_err(|e| {
                anyhow::anyhow!("could not find number of sql_query attempts. Error: {e}")
            })?;

        (number_of_attempts, sql)
    };

    Ok(TestRunOutputs {
        sql,
        number_of_attempts,
        duration,
    })
}

/// When text-to-SQL returns data (i.e. not 'Accept: application/sql`), find how many internal SQL queries it attempted before returning a valid result.
async fn find_number_of_sql_attempts(
    spice_client: &spiceai::Client,
) -> Result<usize, anyhow::Error> {
    let data = retry_query_expecting_results(
        spice_client,
         r#"
SELECT count(1) AS cnt
FROM runtime.task_history
WHERE trace_id=(SELECT trace_id from runtime.task_history where task='nsql' order by start_time desc limit 1) and task='sql_query'
"#,
Duration::from_secs(10)
    )
    .await;

    let Some(Some(rb)) = data.as_ref().map(|s| s.first().clone()) else {
        return Err(anyhow::anyhow!(
            "could not find task history for text to SQL"
        ));
    };
    let count: i64 = rb
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow::anyhow!("could not downcast input column to Int64Array"))?
        .value(0);
    Ok(count as usize)
}

/// Finds the input of the  last `sql_query` in the `runtime.task_history` for the given trace_id.
async fn find_last_sql_statement(spice_client: &spiceai::Client) -> Result<String, anyhow::Error> {
    let data = retry_query_expecting_results(
        spice_client,
        &format!(
            r#"
SELECT input
FROM runtime.task_history
WHERE trace_id=(SELECT trace_id from runtime.task_history where task='nsql' order by start_time desc limit 1)
  AND task='sql_query'
ORDER BY end_time DESC
LIMIT 1"#,
        ), Duration::from_secs(10)
    )
    .await;

    let Some(Some(rb)) = data.as_ref().map(|s| s.first().clone()) else {
        return Err(anyhow::anyhow!(
            "could not find last sql_query task in runtime.task_history"
        ));
    };

    let sql: String = rb
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            test_framework::anyhow::anyhow!("could not downcast input column to StringArray")
        })?
        .value(0)
        .to_string();

    Ok(sql)
}

async fn retry_query_expecting_results(
    spice_client: &spiceai::Client,
    query: &str,
    wait_for: Duration,
) -> Option<Vec<RecordBatch>> {
    let spice_client = Arc::new(spice_client.clone());
    let query = query.to_string();
    let data = Arc::new(tokio::sync::Mutex::new(None));

    wait_until_true(wait_for, || {
        let spice_client = spice_client.clone();
        let query = query.clone();
        let data = data.clone();
        async move {
            match spice_client.query(&query).await {
                Ok(stream) => {
                    let z = stream.try_collect::<Vec<RecordBatch>>().await.ok();
                    let no_data = z
                        .as_ref()
                        .is_none_or(|z| !z.first().is_some_and(|rb| rb.num_rows() > 0));
                    if no_data {
                        return false;
                    }
                    *data.lock().await = z;
                    sleep(Duration::from_secs(1)).await;
                    true
                }
                Err(_) => false,
            }
        }
    })
    .await;

    (data.lock().await).clone()
}
