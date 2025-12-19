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
use serde_json::json;
use std::time::Duration;
use test_framework::{
    anyhow,
    constants::HTTP_BASE_URL,
    spiced::{SpicedInstance, StartRequest},
};
use tokio::time::Instant;

use super::get_app_and_start_request;
use crate::health::HealthMonitor;

pub(crate) async fn run(args: &TextToSqlArgs) -> anyhow::Result<()> {
    let queries = args.load_queries()?;

    println!("Running text-to-sql test with {} queries", queries.len());

    let (_app, start_request) = get_app_and_start_request(&args.common).await?;
    let mut spiced_instance = run_spice(&args.common, start_request).await?;

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

    let sql = if return_sql {
        text
    } else {
        // TODO: use `spice trace` to get the sql generated.
        "select 1".to_string()
    };
    let number_of_attempts = if return_sql {
        1
    } else {
        // TODO: get number of `sql_query` attempts under v1/nsql
        2
    };

    Ok(TestRunOutputs {
        sql,
        number_of_attempts,
        duration,
    })
}

fn normalize_sql(sql: &str) -> String {
    sql.trim()
        .lines()
        .map(|line| line.trim())
        .filter(|line| !line.is_empty() && !line.starts_with("--"))
        .collect::<Vec<_>>()
        .join(" ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}
