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
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Result;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;

use crate::constants::{HTTP_BASE_URL, SEARCH_ENDPOINT, SQL_ENDPOINT};

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

/// How each [`SearchRequest`] is sent to the runtime.
#[derive(Debug, Clone, Default)]
pub enum SearchMethod {
    /// `POST /v1/search`, with the request as the JSON body.
    #[default]
    SearchApi,
    /// `POST /v1/sql`, with the request bound to a parameterized SQL pipeline.
    Sql(SqlPipeline),
}

impl SearchMethod {
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            SearchMethod::SearchApi => "search_api",
            SearchMethod::Sql(_) => "sql",
        }
    }
}

/// A parameterized SQL search pipeline, sent to `POST /v1/sql` once per [`SearchRequest`].
///
/// Each request binds the named parameters `:query` (the request text) and `:limit` (the request
/// limit, when set). The SQL must return an `_id` column (the corpus id, as text or a number) and can
/// return a numeric `_score` column. Rows must be in rank order: the first row is the best match.
#[derive(Debug, Clone)]
pub struct SqlPipeline {
    sql: Arc<str>,
}

impl SqlPipeline {
    #[must_use]
    pub fn new(sql: impl Into<Arc<str>>) -> Self {
        Self { sql: sql.into() }
    }

    #[must_use]
    pub fn sql(&self) -> &str {
        &self.sql
    }

    fn body<'a>(&'a self, request: &'a SearchRequest) -> SqlRequestBody<'a> {
        SqlRequestBody {
            sql: &self.sql,
            parameters: SqlPipelineParameters {
                query: &request.text,
                limit: request.limit,
            },
        }
    }
}

/// The JSON body of a parameterized `POST /v1/sql` request.
#[derive(Serialize)]
struct SqlRequestBody<'a> {
    sql: &'a str,
    parameters: SqlPipelineParameters<'a>,
}

/// The named parameters that every [`SqlPipeline`] can use.
#[derive(Serialize)]
struct SqlPipelineParameters<'a> {
    query: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<usize>,
}

#[derive(Debug, Clone, Default)]
pub struct SearchConfig {
    requests: Vec<SearchRequest>,
    method: SearchMethod,
}

impl SearchConfig {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_method(mut self, method: SearchMethod) -> Self {
        self.method = method;
        self
    }

    #[must_use]
    pub fn method(&self) -> &SearchMethod {
        &self.method
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

/// The response to one [`SearchRequest`], in the shape of the [`SearchMethod`] that sent it.
pub enum SearchResponse {
    SearchApi(SearchApiResponse),
    Sql(SqlSearchResponse),
}

impl SearchResponse {
    /// The highest score in the response, or `0.0` when no result has a score.
    fn top_score(&self) -> f64 {
        match self {
            SearchResponse::SearchApi(response) => response
                .results
                .iter()
                .map(|m| m.score)
                .max_by(f64::total_cmp),
            SearchResponse::Sql(response) => response
                .hits
                .iter()
                .filter_map(|hit| hit.score)
                .max_by(f64::total_cmp),
        }
        .unwrap_or(0.0)
    }
}

/// The JSON body of a `POST /v1/search` response.
#[derive(Deserialize)]
pub struct SearchApiResponse {
    pub results: Vec<SearchResponseResult>,
    pub duration_ms: Option<u64>,
}

/// The rows of a [`SqlPipeline`], in the rank order that the pipeline returned them.
pub struct SqlSearchResponse {
    pub hits: Vec<SqlSearchHit>,
}

/// One row of a [`SqlPipeline`] result.
#[derive(Debug, Deserialize, PartialEq)]
pub struct SqlSearchHit {
    #[serde(rename = "_id")]
    pub id: CorpusId,
    /// Absent, or `null`, when the pipeline does not return `_score`.
    #[serde(rename = "_score", default)]
    pub score: Option<f64>,
}

/// A corpus id from a [`SqlPipeline`] row. Its JSON type follows the `_id` column type.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum CorpusId {
    Text(String),
    Number(serde_json::Number),
}

impl std::fmt::Display for CorpusId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CorpusId::Text(id) => f.write_str(id),
            CorpusId::Number(id) => write!(f, "{id}"),
        }
    }
}

#[derive(Deserialize)]
pub struct SearchResponseResult {
    // `matches` is left as a generic JSON value (`serde_json::Value`) instead of a strongly typed struct.
    // The search API can return different sets of fields here depending on dataset or configuration
    pub matches: serde_json::Value,
    // The runtime serializes this field as `_score` (see `crates/runtime/src/search/types.rs`).
    // Accept the legacy `score` name as an alias to stay compatible with older runtimes.
    #[serde(rename = "_score", alias = "score")]
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
            let Self {
                worker_id,
                http_client,
                config: SearchConfig { requests, method },
            } = self;
            let mut results: BTreeMap<String, SearchResult> = BTreeMap::new();
            let total_requests = requests.len();
            let mut last_progress_time = Instant::now();

            println!("[SearchWorker-{worker_id:02}] STARTED, {total_requests} remaining");

            for (index, request) in requests.into_iter().enumerate() {
                let start = Instant::now();
                let response = send_request(&http_client, &method, &request).await?;
                let duration = start.elapsed();
                results.insert(
                    request.id,
                    SearchResult {
                        score: response.top_score(),
                        response,
                        duration,
                    },
                );

                // Trace progress every 10 seconds
                if last_progress_time.elapsed() >= Duration::from_secs(10) {
                    let completed = index + 1;
                    #[expect(clippy::cast_precision_loss)]
                    let completed_percent = (completed as f64 / total_requests as f64) * 100.0;
                    println!(
                        "[SearchWorker-{worker_id:02}]: {completed}/{total_requests} completed ({completed_percent:.1}%)"
                    );
                    last_progress_time = Instant::now();
                }
            }

            println!("[SearchWorker-{worker_id:02}]: DONE, {total_requests} completed");

            Ok(VectorSearchWorkerResult {
                search_results: results,
            })
        })
    }
}

/// Sends one request with `method` and reads the response. A non-success status is an error that
/// includes the response body.
async fn send_request(
    http_client: &Client,
    method: &SearchMethod,
    request: &SearchRequest,
) -> Result<SearchResponse> {
    let builder = match method {
        SearchMethod::SearchApi => http_client
            .post(format!("{HTTP_BASE_URL}{SEARCH_ENDPOINT}"))
            .json(request),
        SearchMethod::Sql(pipeline) => http_client
            .post(format!("{HTTP_BASE_URL}{SQL_ENDPOINT}"))
            .json(&pipeline.body(request)),
    };
    let res = builder.send().await?;

    let status = res.status();
    if !status.is_success() {
        let body = res.text().await.unwrap_or_default();
        // Collapse whitespace so the error stays on a single line.
        let body = body.split_whitespace().collect::<Vec<_>>().join(" ");
        anyhow::bail!(
            "Search request '{}' failed with status {status}: {body}",
            request.id
        );
    }

    Ok(match method {
        SearchMethod::SearchApi => SearchResponse::SearchApi(res.json().await?),
        SearchMethod::Sql(_) => {
            let hits = res.json().await.map_err(|e| {
                anyhow::anyhow!(
                    "Search request '{}': SQL pipeline rows must have an `_id` column (text or \
                    number) and can have a numeric `_score` column: {e}",
                    request.id
                )
            })?;
            SearchResponse::Sql(SqlSearchResponse { hits })
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{CorpusId, SearchRequest, SqlPipeline, SqlSearchHit};

    #[test]
    fn sql_pipeline_body_binds_query_and_limit() {
        let pipeline = SqlPipeline::new("SELECT _id FROM text_search(corpus, :query) LIMIT :limit");
        let request = SearchRequest::new("q1", "brown fox").with_limit(10);

        let body = serde_json::to_value(pipeline.body(&request)).expect("body serializes");
        assert_eq!(
            body,
            serde_json::json!({
                "sql": "SELECT _id FROM text_search(corpus, :query) LIMIT :limit",
                "parameters": {"query": "brown fox", "limit": 10}
            })
        );
    }

    #[test]
    fn sql_pipeline_body_omits_unset_limit() {
        let pipeline = SqlPipeline::new("SELECT _id FROM corpus");
        let request = SearchRequest::new("q1", "brown fox");

        let body = serde_json::to_value(pipeline.body(&request)).expect("body serializes");
        assert_eq!(
            body["parameters"],
            serde_json::json!({"query": "brown fox"})
        );
    }

    #[test]
    fn sql_search_hits_accept_text_and_number_ids_and_optional_score() {
        let hits: Vec<SqlSearchHit> = serde_json::from_str(
            r#"[{"_id":"doc-1","_score":1.5},{"_id":7},{"_id":8,"_score":null,"text":"x"}]"#,
        )
        .expect("rows parse");

        assert_eq!(
            hits,
            vec![
                SqlSearchHit {
                    id: CorpusId::Text("doc-1".to_string()),
                    score: Some(1.5),
                },
                SqlSearchHit {
                    id: CorpusId::Number(7.into()),
                    score: None,
                },
                SqlSearchHit {
                    id: CorpusId::Number(8.into()),
                    score: None,
                },
            ]
        );
        assert_eq!(hits[0].id.to_string(), "doc-1");
        assert_eq!(hits[1].id.to_string(), "7");
    }

    #[test]
    fn sql_search_hits_reject_missing_id_and_non_numeric_score() {
        assert!(serde_json::from_str::<Vec<SqlSearchHit>>(r#"[{"id":1}]"#).is_err());
        assert!(
            serde_json::from_str::<Vec<SqlSearchHit>>(r#"[{"_id":1,"_score":"high"}]"#).is_err()
        );
        assert!(serde_json::from_str::<Vec<SqlSearchHit>>(r#"[{"_id":null}]"#).is_err());
    }
}
