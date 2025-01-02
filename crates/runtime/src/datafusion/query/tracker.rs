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

use std::{collections::HashSet, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::sql::TableReference;
use opentelemetry::KeyValue;
use tokio::time::Instant;

use crate::request::RequestContext;

use super::{error_code::ErrorCode, metrics};

pub(crate) struct QueryTracker {
    pub(crate) schema: Option<SchemaRef>,
    pub(crate) query_duration_secs: Option<f32>,
    pub(crate) query_execution_duration_secs: Option<f32>,
    pub(crate) rows_produced: u64,
    pub(crate) results_cache_hit: Option<bool>,
    pub(crate) is_accelerated: Option<bool>,
    pub(crate) error_message: Option<String>,
    pub(crate) error_code: Option<ErrorCode>,
    pub(crate) query_duration_timer: Instant,
    pub(crate) query_execution_duration_timer: Instant,
    pub(crate) datasets: Arc<HashSet<TableReference>>,
}

impl QueryTracker {
    pub fn finish_with_error(
        mut self,
        request_context: &RequestContext,
        error_message: String,
        error_code: ErrorCode,
    ) {
        tracing::debug!("Query finished with error: {error_message}; code: {error_code}",);
        self.error_message = Some(error_message);
        self.error_code = Some(error_code);
        self.finish(request_context, &Arc::from(""));
    }

    pub fn finish(mut self, request_context: &RequestContext, captured_output: &Arc<str>) {
        let query_duration = self.query_duration_timer.elapsed();
        let query_execution_duration = self.query_execution_duration_timer.elapsed();

        if self.query_duration_secs.is_none() {
            self.query_duration_secs = Some(query_duration.as_secs_f32());
        }

        if self.query_execution_duration_secs.is_none() {
            self.query_execution_duration_secs = Some(query_execution_duration.as_secs_f32());
        }

        let mut tags = vec![];
        match self.results_cache_hit {
            Some(true) => {
                tags.push("cache-hit");
            }
            Some(false) => {
                tags.push("cache-miss");
            }
            None => {}
        }

        if self.error_message.is_some() {
            tags.push("error");
        }

        let mut labels = vec![
            KeyValue::new("tags", tags.join(",")),
            KeyValue::new(
                "datasets",
                self.datasets
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<String>>()
                    .join(","),
            ),
        ];

        labels.extend(request_context.to_dimensions());

        crate::metrics::telemetry::track_query_duration(query_duration, &labels);
        crate::metrics::telemetry::track_query_execution_duration(
            query_execution_duration,
            &labels,
        );

        if let Some(err) = &self.error_code {
            labels.push(KeyValue::new("err_code", err.to_string()));
            metrics::FAILURES.add(1, &labels);
        }

        trace_query(request_context, &self, captured_output);
    }

    #[must_use]
    pub(crate) fn schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    #[must_use]
    pub(crate) fn rows_produced(mut self, rows_produced: u64) -> Self {
        self.rows_produced = rows_produced;
        self
    }

    #[must_use]
    pub(crate) fn results_cache_hit(mut self, cache_hit: bool) -> Self {
        self.results_cache_hit = Some(cache_hit);
        self
    }

    #[must_use]
    pub(crate) fn datasets(mut self, datasets: Arc<HashSet<TableReference>>) -> Self {
        self.datasets = datasets;
        self
    }
}

fn trace_query(
    request_context: &RequestContext,
    query_tracker: &QueryTracker,
    captured_output: &str,
) {
    if let Some(error_code) = &query_tracker.error_code {
        tracing::info!(target: "task_history", error_code = %error_code, "labels");
    }

    if let Some(error_message) = &query_tracker.error_message {
        tracing::error!(target: "task_history", "{error_message}");
    }

    if let Some(query_execution_duration_secs) = &query_tracker.query_execution_duration_secs {
        tracing::info!(target: "task_history", query_execution_duration_ms = %query_execution_duration_secs * 1000.0, "labels");
    }

    tracing::info!(target: "task_history", rows_produced = %query_tracker.rows_produced, "labels");

    if let Some(true) = query_tracker.results_cache_hit {
        tracing::info!(target: "task_history", results_cache_hit = true, "labels");
    }

    if let Some(true) = &query_tracker.is_accelerated {
        tracing::info!(target: "task_history", accelerated = true, "labels");
    }

    let datasets_str = query_tracker
        .datasets
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<String>>()
        .join(",");
    tracing::info!(target: "task_history", protocol = ?request_context.protocol(), datasets = datasets_str, "labels");
    tracing::info!(target: "task_history", captured_output = %captured_output);
}
