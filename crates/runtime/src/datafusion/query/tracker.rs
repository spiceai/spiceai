/*
Copyright 2024 The Spice.ai OSS Authors

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

use std::{collections::HashSet, sync::Arc, time::SystemTime};

use arrow::datatypes::SchemaRef;
use tokio::time::Instant;
use uuid::Uuid;

use super::{error_code::ErrorCode, Protocol};

pub(crate) struct QueryTracker {
    pub(crate) df: Arc<crate::datafusion::DataFusion>,
    pub(crate) schema: Option<SchemaRef>,
    pub(crate) query_id: Uuid,
    pub(crate) sql: Arc<str>,
    pub(crate) nsql: Option<Arc<str>>,
    pub(crate) start_time: SystemTime,
    pub(crate) end_time: Option<SystemTime>,
    pub(crate) execution_time: Option<f32>,
    pub(crate) rows_produced: u64,
    pub(crate) results_cache_hit: Option<bool>,
    pub(crate) error_message: Option<String>,
    pub(crate) error_code: Option<ErrorCode>,
    pub(crate) timer: Instant,
    pub(crate) datasets: Arc<HashSet<String>>,
    pub(crate) protocol: Protocol,
}

impl QueryTracker {
    pub async fn finish_with_error(mut self, error_message: String, error_code: ErrorCode) {
        tracing::debug!("Query finished with error: {error_message}; code: {error_code}",);
        self.error_message = Some(error_message);
        self.error_code = Some(error_code);
        self.finish().await;
    }

    pub async fn finish(mut self) {
        if self.end_time.is_none() {
            self.end_time = Some(SystemTime::now());
        }

        let duration = self.timer.elapsed();

        if self.execution_time.is_none() {
            self.execution_time = Some(duration.as_secs_f32());
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
            ("tags", tags.join(",")),
            (
                "datasets",
                self.datasets
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<String>>()
                    .join(","),
            ),
            ("protocol", self.protocol.to_string()),
        ];

        metrics::histogram!("query_duration_seconds", &labels).record(duration.as_secs_f32());

        if let Some(err) = &self.error_code {
            labels.push(("err_code", err.to_string()));
            metrics::counter!("query_failures", &labels).increment(1);
        }

        if let Err(err) = self.write_query_history().await {
            tracing::error!("Error writing query history: {err}");
        };
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
    pub(crate) fn datasets(mut self, datasets: Arc<HashSet<String>>) -> Self {
        self.datasets = datasets;
        self
    }
}
