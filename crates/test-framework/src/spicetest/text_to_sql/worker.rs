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

use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Result;
use arrow::array::{Int64Array, RecordBatch, StringArray};
use futures::TryStreamExt;
use reqwest::Client;
use serde_json::json;
use tokio::task::JoinHandle;

use crate::constants::HTTP_BASE_URL;

#[derive(Debug, Clone)]
pub struct TextToSqlRequest {
    pub id: String,
    pub question: String,
    pub expected_sql: String,
    pub model: String,
    pub sample_data_enabled: bool,
    pub return_sql: bool,
}

impl TextToSqlRequest {
    #[must_use]
    pub fn new(
        id: impl Into<String>,
        question: impl Into<String>,
        expected_sql: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            question: question.into(),
            expected_sql: expected_sql.into(),
            model: model.into(),
            sample_data_enabled: false,
            return_sql: false,
        }
    }

    #[must_use]
    pub fn with_sample_data_enabled(mut self, sample_data_enabled: bool) -> Self {
        self.sample_data_enabled = sample_data_enabled;
        self
    }

    #[must_use]
    pub fn with_return_sql(mut self, return_sql: bool) -> Self {
        self.return_sql = return_sql;
        self
    }
}

#[derive(Debug, Clone, Default)]
pub struct TextToSqlConfig {
    pub requests: Vec<TextToSqlRequest>,
}

impl TextToSqlConfig {
    #[must_use]
    pub fn new(requests: Vec<TextToSqlRequest>) -> Self {
        Self { requests }
    }
}

pub(crate) struct TextToSqlWorkerResult {
    pub(crate) results: BTreeMap<String, TextToSqlResult>,
}

pub struct TextToSqlResult {
    pub generated_sql: String,
    pub expected_sql: String,
    pub is_error: bool,
    pub number_of_attempts: usize,
    pub duration: Duration,
    pub sample_data_enabled: bool,
    pub return_sql: bool,
}

pub(crate) struct TextToSqlWorker {
    http_client: Client,
    spice_client: spiceai::Client,
    config: TextToSqlConfig,
}

impl TextToSqlWorker {
    pub fn new(
        http_client: Client,
        spice_client: spiceai::Client,
        config: TextToSqlConfig,
    ) -> Self {
        Self {
            http_client,
            spice_client,
            config,
        }
    }

    pub fn start(self) -> JoinHandle<Result<TextToSqlWorkerResult>> {
        tokio::spawn(async move {
            let mut results: BTreeMap<String, TextToSqlResult> = BTreeMap::new();
            let total_requests = self.config.requests.len();
            let mut last_progress_time = Instant::now();

            println!("[TextToSqlWorker] STARTED, {total_requests} remaining");

            for (index, request) in self.config.requests.into_iter().enumerate() {
                let start = Instant::now();

                let url = format!("{HTTP_BASE_URL}/v1/nsql");
                let body = json!({
                    "query": request.question,
                    "model": request.model,
                    "sample_data_enabled": request.sample_data_enabled,
                    "stream": false
                });
                let accept_header = if request.return_sql {
                    "application/sql"
                } else {
                    "application/json"
                };

                let response = self
                    .http_client
                    .post(&url)
                    .header("Content-Type", "application/json")
                    .header("Accept", accept_header)
                    .body(body.to_string())
                    .send()
                    .await?;

                let mut is_error = false;
                let text = match response.text().await {
                    Ok(t) => t,
                    Err(e) => {
                        is_error = true;
                        format!("HTTP error: {e}")
                    }
                };

                let (number_of_attempts, sql) = if request.return_sql {
                    (1, text)
                } else {
                    let sql = find_last_sql_statement(&self.spice_client)
                        .await
                        .map_err(|e| {
                            anyhow::anyhow!("could not find last sql_query statement. Error: {e}")
                        })?;
                    let number_of_attempts = find_number_of_sql_attempts(&self.spice_client)
                        .await
                        .map_err(|e| {
                            anyhow::anyhow!(
                                "could not find number of sql_query attempts. Error: {e}"
                            )
                        })?;

                    (number_of_attempts, sql)
                };

                let duration = start.elapsed();

                results.insert(
                    request.id.clone(),
                    TextToSqlResult {
                        generated_sql: sql,
                        expected_sql: request.expected_sql,
                        number_of_attempts,
                        duration,
                        is_error,
                        sample_data_enabled: request.sample_data_enabled,
                        return_sql: request.return_sql,
                    },
                );

                if last_progress_time.elapsed() >= Duration::from_secs(10) {
                    let completed = index + 1;
                    #[expect(clippy::cast_precision_loss)]
                    let completed_percent = (completed as f64 / total_requests as f64) * 100.0;
                    println!(
                        "[TextToSqlWorker]: {completed}/{total_requests} completed ({completed_percent:.1}%)"
                    );
                    last_progress_time = Instant::now();
                }
            }

            println!("[TextToSqlWorker]: DONE, {total_requests} completed");

            Ok(TextToSqlWorkerResult { results })
        })
    }
}

async fn find_number_of_sql_attempts(spice_client: &spiceai::Client) -> Result<usize> {
    let data = retry_query_expecting_results(
        spice_client,
        "
SELECT count(1) AS cnt
FROM runtime.task_history
WHERE trace_id=(SELECT trace_id from runtime.task_history where task='nsql' order by start_time desc limit 1) and task='sql_query'
",
        Duration::from_secs(10),
    )
    .await;

    let Some(rb) = data.as_ref().and_then(|s| s.first()) else {
        return Err(anyhow::anyhow!(
            "could not find task history for text to SQL"
        ));
    };
    #[expect(clippy::cast_possible_truncation)]
    #[expect(clippy::cast_sign_loss)]
    let count = rb
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow::anyhow!("could not downcast input column to Int64Array"))?
        .value(0) as usize;
    Ok(count)
}

async fn find_last_sql_statement(spice_client: &spiceai::Client) -> Result<String> {
    let data = retry_query_expecting_results(
        spice_client,
        "
SELECT input
FROM runtime.task_history
WHERE trace_id=(SELECT trace_id from runtime.task_history where task='nsql' order by start_time desc limit 1)
  AND task='sql_query'
ORDER BY end_time DESC
LIMIT 1",
        Duration::from_secs(10),
    )
    .await;

    let Some(rb) = data.as_ref().and_then(|s| s.first()) else {
        return Err(anyhow::anyhow!(
            "could not find last sql_query task in runtime.task_history"
        ));
    };

    let sql: String = rb
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("could not downcast input column to StringArray"))?
        .value(0)
        .to_string();

    Ok(sql)
}

async fn retry_query_expecting_results(
    spice_client: &spiceai::Client,
    query: &str,
    wait_for: Duration,
) -> Option<Vec<RecordBatch>> {
    use crate::utils::wait_until_true;
    use tokio::time::sleep;

    let query = query.to_string();
    let data = Arc::new(tokio::sync::Mutex::new(None));

    wait_until_true(wait_for, || {
        let spice_client = spice_client.clone();
        let query = query.clone();
        let data = Arc::clone(&data);
        async move {
            match spice_client.query(&query).await {
                Ok(stream) => {
                    let rb_opt = stream.try_collect::<Vec<RecordBatch>>().await.ok();
                    let no_data = rb_opt
                        .as_ref()
                        .is_none_or(|z| z.first().is_none_or(|rb| rb.num_rows() == 0));
                    if no_data {
                        sleep(Duration::from_secs(1)).await;
                        false
                    } else {
                        *data.lock().await = rb_opt;
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
