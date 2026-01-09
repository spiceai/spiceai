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
    time::{Duration, Instant},
};

use anyhow::Result;
use reqwest::Client;
use serde_json::json;
use tokio::task::JoinHandle;

use crate::spicetest::text_to_sql::task_history::find_task_history_metrics;

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
    pub question: String,
    pub generated_sql: String,
    pub expected_sql: String,
    pub is_error: bool,
    pub duration: Duration,
    pub sample_data_enabled: bool,
    pub return_sql: bool,

    // Non-functional metrics from task_history
    pub query_count: usize,
    pub sql_duration_ms: f64,
    pub llm_duration_ms: f64,
    pub llm_count: usize,
    pub llm_input_tokens: u64,
    pub llm_output_tokens: u64,
}

pub(crate) struct TextToSqlWorker {
    http_client: Client,
    http_base_url: String,
    spice_client: spiceai::Client,
    config: TextToSqlConfig,
}

impl TextToSqlWorker {
    pub fn new(
        http_client: Client,
        http_base_url: impl Into<String>,
        spice_client: spiceai::Client,
        config: TextToSqlConfig,
    ) -> Self {
        Self {
            http_client,
            http_base_url: http_base_url.into(),
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
                let mut is_error = false;
                let mut generated_sql: Option<String> = None;
                match nsql_request(&self.http_client, &self.http_base_url, &request).await {
                    Ok(sql) if request.return_sql => {
                        generated_sql = Some(sql);
                    }
                    Ok(_) => {} // NSQL returned data. Must get SQL from task_history
                    Err(_) => {
                        is_error = true;
                    }
                }

                let duration = start.elapsed();

                let (sql, task_metrics) = find_task_history_metrics(&self.spice_client)
                    .await
                    .map_err(|e| {
                        anyhow::anyhow!("could not find task history metrics. Error: {e}")
                    })?;

                results.insert(
                    request.id.clone(),
                    TextToSqlResult {
                        question: request.question,
                        generated_sql: generated_sql.or(sql).unwrap_or_default(),
                        expected_sql: request.expected_sql,
                        duration,
                        is_error,
                        query_count: task_metrics.sql_count,
                        sample_data_enabled: request.sample_data_enabled,
                        return_sql: request.return_sql,
                        sql_duration_ms: task_metrics.sql_duration_ms,
                        llm_duration_ms: task_metrics.llm_duration_ms,
                        llm_count: task_metrics.llm_count,
                        llm_input_tokens: task_metrics.llm_input_tokens,
                        llm_output_tokens: task_metrics.llm_output_tokens,
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

/// Runs a text to SQL HTTP operation. Returns the generated SQL or generated data (based on [`TextToSqlRequest::return_sql`]).
async fn nsql_request(
    client: &Client,
    http_base_url: &str,
    req: &TextToSqlRequest,
) -> Result<String, reqwest::Error> {
    let body = json!({
        "query": req.question,
        "model": req.model,
        "sample_data_enabled": req.sample_data_enabled,
        "stream": false
    });
    let accept_header = if req.return_sql {
        "application/sql"
    } else {
        "application/json"
    };

    client
        .post(format!("{http_base_url}/v1/nsql"))
        .header("Content-Type", "application/json")
        .header("Accept", accept_header)
        .body(body.to_string())
        .send()
        .await?
        .text()
        .await
}
