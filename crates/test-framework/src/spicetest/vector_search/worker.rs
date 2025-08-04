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
    collections::{BTreeMap, HashMap},
    time::{Duration, Instant},
};

use anyhow::Result;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;

#[derive(Debug, Default, Serialize, Clone)]
pub struct SearchRequest {
    #[serde(skip)]
    pub id: String,

    pub text: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub datasets: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub where_cond: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub additional_columns: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub keywords: Vec<String>,
}

impl SearchRequest {
    #[must_use]
    pub fn new(id: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            text: text.into(),
            datasets: vec![],
            limit: None,
            where_cond: None,
            additional_columns: vec![],
            keywords: vec![],
        }
    }

    #[must_use]
    pub fn with_datasets(mut self, datasets: Vec<impl Into<String>>) -> Self {
        self.datasets = datasets.into_iter().map(Into::into).collect();
        self
    }

    #[must_use]
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    #[must_use]
    pub fn with_where_cond(mut self, where_cond: impl Into<String>) -> Self {
        self.where_cond = Some(where_cond.into());
        self
    }

    #[must_use]
    pub fn with_additional_columns(mut self, additional_columns: Vec<impl Into<String>>) -> Self {
        self.additional_columns = additional_columns.into_iter().map(Into::into).collect();
        self
    }

    #[must_use]
    pub fn with_keywords(mut self, keywords: Vec<impl Into<String>>) -> Self {
        self.keywords = keywords.into_iter().map(Into::into).collect();
        self
    }
}

#[derive(Debug, Clone, Default)]
pub struct SearchConfig {
    requests: Vec<SearchRequest>,
}

impl SearchConfig {
    #[must_use]
    pub fn new() -> Self {
        Self { requests: vec![] }
    }

    #[must_use]
    pub fn add_request(mut self, request: SearchRequest) -> Self {
        self.requests.push(request);
        self
    }

    #[must_use]
    pub fn add_requests(mut self, requests: impl IntoIterator<Item = SearchRequest>) -> Self {
        self.requests.extend(requests);
        self
    }

    #[must_use]
    pub fn requests(&self) -> &[SearchRequest] {
        &self.requests
    }

    #[must_use]
    pub fn into_requests(self) -> Vec<SearchRequest> {
        self.requests
    }
}

pub(crate) struct VectorSearchWorkerResult {
    pub(crate) search_results: BTreeMap<String, SearchResult>,
}

#[allow(dead_code)]
pub struct SearchResult {
    pub response: SearchResponse,
    pub score: f64,
    pub duration: Duration,
}

pub(crate) struct VectorSearchWorker {
    worker_id: usize,
    http_client: Client,
    config: SearchConfig,
}

#[derive(Deserialize)]
#[allow(dead_code)]
pub struct SearchResponse {
    pub results: Vec<SearchResponseResult>,
    pub duration_ms: Option<u64>,
}

#[derive(Deserialize)]
#[allow(dead_code)]
pub struct SearchResponseResult {
    // `matches` is left as a generic JSON value (`serde_json::Value`) instead of a strongly typed struct.
    // The search API can return different sets of fields here depending on dataset or configuration
    pub matches: serde_json::Value,
    pub score: f64,
    pub dataset: String,
    /// Primary key can be different types depending on the dataset. Default to empty map, if not present.
    #[serde(default)]
    pub primary_key: HashMap<String, serde_json::Value>,
}

impl VectorSearchWorker {
    pub fn new(worker_id: usize, http_client: Client, config: SearchConfig) -> Self {
        Self {
            worker_id,
            http_client,
            config,
        }
    }

    pub fn start(self) -> JoinHandle<Result<VectorSearchWorkerResult>> {
        tokio::spawn(async move {
            let mut results: BTreeMap<String, SearchResult> = BTreeMap::new();
            let total_requests = self.config.requests.len();
            let mut last_progress_time = Instant::now();

            println!(
                "[SearchWorker-{:02}] STARTED, {total_requests} remaining",
                self.worker_id
            );

            for (index, request) in self.config.requests.into_iter().enumerate() {
                let start = Instant::now();
                let res = self
                    .http_client
                    .post("http://localhost:8090/v1/search")
                    .json(&request)
                    .send()
                    .await?;

                let response: SearchResponse = res.json().await?;
                let duration = start.elapsed();
                results.insert(
                    request.id,
                    SearchResult {
                        score: response
                            .results
                            .iter()
                            .map(|m| m.score)
                            .max_by(f64::total_cmp)
                            .unwrap_or(0.0),
                        response,
                        duration,
                    },
                );

                // Trace progress every 10 seconds
                if last_progress_time.elapsed() >= Duration::from_secs(10) {
                    let completed = index + 1;
                    #[allow(clippy::cast_precision_loss)]
                    let completed_percent = (completed as f64 / total_requests as f64) * 100.0;
                    println!(
                        "[SearchWorker-{:02}]: {completed}/{total_requests} completed ({completed_percent:.1}%)",
                        self.worker_id
                    );
                    last_progress_time = Instant::now();
                }
            }

            println!(
                "[SearchWorker-{:02}]: DONE, {total_requests} completed",
                self.worker_id
            );

            Ok(VectorSearchWorkerResult {
                search_results: results,
            })
        })
    }
}
