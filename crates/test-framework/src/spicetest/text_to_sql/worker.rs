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
use async_channel::Receiver;
use opentelemetry::trace::TraceId;
use rand::RngCore;
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
    id: usize,
    http_client: Client,
    http_base_url: String,
    spice_client: spiceai::Client,
    request_rx: Receiver<TextToSqlRequest>,
}

impl TextToSqlWorker {
    pub fn new(
        id: usize,
        http_client: Client,
        http_base_url: impl Into<String>,
        spice_client: spiceai::Client,
        request_rx: Receiver<TextToSqlRequest>,
    ) -> Self {
        Self {
            id,
            http_client,
            http_base_url: http_base_url.into(),
            spice_client,
            request_rx,
        }
    }

    pub fn start(self) -> JoinHandle<Result<TextToSqlWorkerResult>> {
        tokio::spawn(async move {
            let mut results: BTreeMap<String, TextToSqlResult> = BTreeMap::new();
            let mut processed_count = 0usize;

            while let Ok(request) = self.request_rx.recv().await {
                let start = Instant::now();
                let mut is_error = false;
                let mut generated_sql: Option<String> = None;

                let trace_id = random_trace_id();
                match nsql_request(&self.http_client, &self.http_base_url, &request, &trace_id)
                    .await
                {
                    Ok(sql) if request.return_sql => {
                        generated_sql = Some(sql);
                    }
                    Ok(_) => {} // NSQL returned data. Must get SQL from task_history
                    Err(_) => {
                        is_error = true;
                    }
                }

                let duration = start.elapsed();

                let (sql, task_metrics) = find_task_history_metrics(&self.spice_client, &trace_id)
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

                processed_count += 1;
                if processed_count.is_multiple_of(10) {
                    println!(
                        "[TextToSqlWorker-{}]: processed {processed_count} requests",
                        self.id
                    );
                }
            }

            println!(
                "[TextToSqlWorker-{}]: DONE, {processed_count} completed",
                self.id
            );

            Ok(TextToSqlWorkerResult { results })
        })
    }
}

/// Runs a text to SQL HTTP operation. Returns the generated SQL or generated data (based on [`TextToSqlRequest::return_sql`]).
async fn nsql_request(
    client: &Client,
    http_base_url: &str,
    req: &TextToSqlRequest,
    trace_id: &TraceId,
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
        .header("traceparent", format_traceparent(trace_id))
        .body(body.to_string())
        .send()
        .await?
        .text()
        .await
}

/// Generates a random W3C Trace ID.
fn random_trace_id() -> TraceId {
    let mut bytes = [0u8; 16];
    let mut rng = rand::rng();
    rng.fill_bytes(&mut bytes);

    // Ensure the TraceId is not all zeros
    if bytes.iter().all(|&b| b == 0) {
        return random_trace_id();
    }

    TraceId::from_bytes(bytes)
}

/// Formats a W3C traceparent header value.
fn format_traceparent(trace_id: &TraceId) -> String {
    use std::fmt::Write;
    // Format: version-traceid-parentid-flags
    // version: 00, parentid: random 16 hex chars, flags: 01 (sampled)
    let mut parent_bytes = [0u8; 8];
    rand::rng().fill_bytes(&mut parent_bytes);
    let parent_id = parent_bytes
        .iter()
        .fold(String::with_capacity(16), |mut acc, b| {
            let _ = write!(acc, "{b:02x}");
            acc
        });
    format!("00-{trace_id}-{parent_id}-01")
}
