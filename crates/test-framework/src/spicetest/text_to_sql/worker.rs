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

use std::{collections::BTreeMap, time::Instant};

use anyhow::Result;
use arrow::datatypes::{Field, Schema};
use async_channel::Receiver;
use opentelemetry::trace::TraceId;
use rand::RngCore;
use reqwest::Client;
use serde_json::{Value, json};
use tokio::task::JoinHandle;

use crate::spicetest::text_to_sql::{
    TextToSqlMetric,
    metrics::intersection_over_union,
    parse::{logical_plan, sql_schema},
    task_history::find_task_history_metrics,
};

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
    pub(crate) results: BTreeMap<String, TextToSqlMetric>,
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
            let mut results: BTreeMap<String, TextToSqlMetric> = BTreeMap::new();

            while let Ok(request) = self.request_rx.recv().await {
                match self.process_request(&request).await {
                    Ok(metric) => {
                        results.insert(request.id.clone(), metric);
                    }
                    Err(e) => {
                        eprintln!(
                            "[TextToSqlWorker-{}]: Failed to process request '{}': {e}",
                            self.id, request.id
                        );
                    }
                }
            }

            println!("[TextToSqlWorker-{}]: DONE", self.id);

            Ok(TextToSqlWorkerResult { results })
        })
    }

    async fn process_request(&self, request: &TextToSqlRequest) -> Result<TextToSqlMetric> {
        let start = Instant::now();
        let mut is_error = false;
        let mut generated_sql_opt: Option<String> = None;
        let mut generated_schema_opt: Option<Schema> = None;

        let trace_id = random_trace_id();
        match nsql_request(&self.http_client, &self.http_base_url, request, &trace_id).await {
            Ok(NSQLResponse::Sql(sql)) => {
                generated_sql_opt = Some(sql);
            }
            Ok(NSQLResponse::Data(schema)) => {
                generated_schema_opt = Some(schema);
            }
            Err(e) => {
                eprintln!(
                    "[TextToSqlWorker-{}]: NSQL request failed for '{}': {e}",
                    self.id, request.id
                );
                is_error = true;
            }
        }

        let duration = start.elapsed();

        let (sql, task_history_metrics) = find_task_history_metrics(&self.spice_client, &trace_id)
            .await
            .map_err(|e| anyhow::anyhow!("could not find task history metrics. Error: {e}"))?;

        let generated_sql = generated_sql_opt.or(sql).unwrap_or_default();

        // Calculate generated schema & logical plan if absent.
        let generated_schema = match generated_schema_opt {
            Some(schema) => Some(schema),
            None => sql_schema(
                self.http_client.clone(),
                &self.http_base_url,
                &generated_sql,
            )
            .await
            .ok(),
        };

        let generated_logical_plan = logical_plan(
            self.http_client.clone(),
            &self.http_base_url,
            &generated_sql,
        )
        .await
        .inspect_err(|e| eprintln!("could not compute logical plan for generated SQL. Error: {e}"))
        .ok();

        // Calculate expected schema & logical plan if absent.
        let expected_schema = sql_schema(
            self.http_client.clone(),
            &self.http_base_url,
            &request.expected_sql,
        )
        .await
        .inspect_err(|e| eprintln!("could not compute schema for expected SQL. Error: {e}"))
        .ok();

        let expected_logical_plan = logical_plan(
            self.http_client.clone(),
            &self.http_base_url,
            &request.expected_sql,
        )
        .await
        .map_err(|e| anyhow::anyhow!("could not compute expected logical plan. Error: {e}"))?;

        Ok(TextToSqlMetric::new(
            request.question.clone(),
            &generated_sql,
            &request.expected_sql,
            &expected_logical_plan,
            generated_logical_plan.as_ref(),
            is_error,
            duration,
            request.sample_data_enabled,
            request.return_sql,
            &task_history_metrics,
            schema_similarity(generated_schema.as_ref(), expected_schema.as_ref()),
        ))
    }
}

/// Computes the schema similarity between two Arrow schemas using Intersection over Union (`IoU`).
fn schema_similarity(a: Option<&Schema>, b: Option<&Schema>) -> f64 {
    match (a, b) {
        (Some(schema_a), Some(schema_b)) => {
            let fields_a = schema_a.fields().into_iter().collect();
            let fields_b = schema_b.fields().into_iter().collect();
            intersection_over_union(&fields_a, &fields_b)
        }
        _ => 0.0,
    }
}

/// The data returned from `v1/nsql` endpoint.
/// Determined by [`TextToSqlRequest::return_sql`] (specifically the `Accept` HTTP header).
pub enum NSQLResponse {
    Sql(String),
    Data(Schema),
}

/// Runs a text to SQL HTTP operation. Returns the generated SQL or generated data (based on [`TextToSqlRequest::return_sql`]).
async fn nsql_request(
    client: &Client,
    http_base_url: &str,
    req: &TextToSqlRequest,
    trace_id: &TraceId,
) -> Result<NSQLResponse, reqwest::Error> {
    let TextToSqlRequest {
        question,
        model,
        sample_data_enabled,
        return_sql,
        ..
    } = req;
    let body = json!({
        "query": question,
        "model": model,
        "sample_data_enabled": sample_data_enabled,
        "stream": false
    });
    let accept_header = if *return_sql {
        "application/sql"
    } else {
        "application/vnd.spiceai.nsql.v1+json"
    };

    let resp = client
        .post(format!("{http_base_url}/v1/nsql"))
        .header("Content-Type", "application/json")
        .header("Accept", accept_header)
        .header("traceparent", format_traceparent(trace_id))
        .body(body.to_string())
        .send()
        .await?;

    if *return_sql {
        resp.text().await.map(NSQLResponse::Sql)
    } else {
        let schema = resp
            .json::<serde_json::Value>()
            .await?
            .get("schema")
            .and_then(|s| s.get("fields"))
            .and_then(|fields| match fields {
                Value::Array(arr) if !arr.is_empty() => Some(
                    arr.iter()
                        .filter_map(|a| serde_json::from_value(a.clone()).ok())
                        .collect::<Vec<Field>>(),
                ),
                _ => None,
            })
            .map_or(Schema::empty(), Schema::new);
        Ok(NSQLResponse::Data(schema))
    }
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
