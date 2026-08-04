/*
Copyright 2026 The Spice.ai OSS Authors

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

#![allow(clippy::missing_errors_doc)]

//! Thin Elasticsearch REST API client for Spice.ai runtime.
//!
//! Provides typed access to the Elasticsearch APIs needed by the data connector:
//! index mappings, search (query DSL, kNN, full-text), and bulk operations.

use bytes::Bytes;
use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use std::collections::HashMap;
use std::time::Duration;
use url::Url;

// ── Error ──────────────────────────────────────────────────────────────────

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("HTTP request failed: {source}"))]
    HttpRequest { source: reqwest::Error },

    #[snafu(display("Failed to parse JSON response: {source}"))]
    JsonParse { source: reqwest::Error },

    #[snafu(display("Elasticsearch error (HTTP {status}): {message}"))]
    ElasticsearchError { status: u16, message: String },

    #[snafu(display("Invalid URL: {source}"))]
    InvalidUrl { source: url::ParseError },

    #[snafu(display("Failed to serialize JSON: {source}"))]
    JsonSerialize { source: serde_json::Error },
}

impl Error {
    /// Returns true if this error represents a transient condition that may
    /// succeed on retry. Transient includes HTTP 429 (too many requests), any
    /// 5xx response (500–599), or a timed-out/connect-reset transport error.
    #[must_use]
    pub fn is_transient(&self) -> bool {
        match self {
            Error::ElasticsearchError { status, .. } => {
                *status == 429 || (500..=599).contains(status)
            }
            Error::HttpRequest { source } => source.is_timeout() || source.is_connect(),
            _ => false,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

// ── Types ──────────────────────────────────────────────────────────────────

/// Top-level response from `GET /<index>/_mapping`.
pub type MappingResponse = HashMap<String, IndexMapping>;

#[derive(Debug, Clone, Deserialize)]
pub struct IndexMapping {
    pub mappings: Mappings,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Mappings {
    #[serde(default)]
    pub properties: HashMap<String, FieldMapping>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FieldMapping {
    #[serde(rename = "type")]
    pub field_type: Option<String>,
    #[serde(default)]
    pub properties: Option<HashMap<String, FieldMapping>>,
    /// For `dense_vector` fields.
    pub dims: Option<i64>,
    /// Similarity metric for `dense_vector` (e.g. `cosine`, `l2_norm`, `dot_product`).
    pub similarity: Option<String>,
}

/// A search request body sent to `POST /<index>/_search`.
#[derive(Debug, Clone, Serialize, Default)]
pub struct SearchRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub knn: Option<KnnQuery>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<serde_json::Value>,
    #[serde(rename = "_source", skip_serializing_if = "Option::is_none")]
    pub source: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct KnnQuery {
    pub field: String,
    pub query_vector: Vec<f32>,
    pub k: usize,
    pub num_candidates: usize,
}

/// Top-level search response.
#[derive(Debug, Clone, Deserialize)]
pub struct SearchResponse {
    #[serde(default)]
    pub pit_id: Option<String>,
    pub hits: HitsEnvelope,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HitsEnvelope {
    #[serde(default)]
    pub total: Option<HitsTotal>,
    pub hits: Vec<Hit>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HitsTotal {
    pub value: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Hit {
    #[serde(rename = "_id")]
    pub id: String,
    #[serde(default, rename = "_score")]
    pub score: Option<f64>,
    #[serde(default)]
    pub sort: Option<Vec<serde_json::Value>>,
    #[serde(default, rename = "_source")]
    pub source: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct OpenPointInTimeResponse {
    id: String,
}

// ── Client ─────────────────────────────────────────────────────────────────

/// A lightweight Elasticsearch HTTP client.
#[derive(Debug, Clone)]
pub struct Client {
    http: reqwest::Client,
    /// Base URL as a string, without trailing slash (e.g. `http://localhost:9200`).
    base_url: String,
    username: Option<String>,
    password: Option<String>,
    retry: RetryConfig,
}

/// Configuration for retrying transient Elasticsearch errors (HTTP 429 / 5xx).
#[derive(Debug, Clone, Copy)]
pub struct RetryConfig {
    /// Maximum number of retry attempts after the initial request. `0` disables retries.
    pub max_retries: u32,
    /// Initial backoff delay; each subsequent retry doubles the delay (capped at 30s).
    pub initial_backoff: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff: Duration::from_millis(200),
        }
    }
}

/// Builder-style options for [`Client::new_with_options`].
#[derive(Debug, Clone)]
pub struct ClientOptions {
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub retry: RetryConfig,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
            retry: RetryConfig::default(),
        }
    }
}

impl Client {
    /// Create a new client pointing at the given Elasticsearch base URL.
    ///
    /// Optional basic-auth credentials are applied to every request.
    pub fn new(base_url: &str, username: Option<&str>, password: Option<&str>) -> Result<Self> {
        Self::new_with_options(base_url, username, password, &ClientOptions::default())
    }

    /// Create a new client with explicit timeout and retry configuration.
    pub fn new_with_options(
        base_url: &str,
        username: Option<&str>,
        password: Option<&str>,
        opts: &ClientOptions,
    ) -> Result<Self> {
        // Validate the URL.
        let _: Url = base_url.parse().context(InvalidUrlSnafu)?;

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        let http = reqwest::Client::builder()
            .default_headers(headers)
            .user_agent(concat!("spiceai/", env!("CARGO_PKG_VERSION")))
            .connect_timeout(opts.connect_timeout)
            .timeout(opts.request_timeout)
            .build()
            .context(HttpRequestSnafu)?;

        Ok(Self {
            http,
            base_url: base_url.trim_end_matches('/').to_string(),
            username: username.map(ToString::to_string),
            password: password.map(ToString::to_string),
            retry: opts.retry,
        })
    }

    /// Apply basic-auth credentials to a request builder if configured.
    fn auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match (&self.username, &self.password) {
            (Some(user), Some(pass)) => req.basic_auth(user, Some(pass)),
            (Some(user), None) => req.basic_auth(user, None::<&str>),
            _ => req,
        }
    }

    /// Retry `f` on transient errors (HTTP 429/5xx, connect/timeout) with
    /// exponential backoff. Bounded by `self.retry.max_retries`.
    ///
    /// Retry is opt-in per call site: only operations that wrap their request
    /// in `with_retry` are retried. Today this is scoped to `bulk_index`, which
    /// is the hot ingestion path. Other operations (`search`, `index_exists`,
    /// `create_index`, `put_mapping`, `get_mapping`, `index_document`, index
    /// settings, refresh, and force-merge calls) bypass retries to keep
    /// startup/query failures fast and attributable.
    async fn with_retry<'a, F, Fut, T>(&'a self, op: &'static str, mut f: F) -> Result<T>
    where
        F: FnMut(&'a Self) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let mut backoff = self.retry.initial_backoff;
        let mut attempt: u32 = 0;
        loop {
            match f(self).await {
                Ok(v) => return Ok(v),
                Err(e) if attempt < self.retry.max_retries && e.is_transient() => {
                    tracing::warn!(
                        "Elasticsearch {op} attempt {n}/{max} failed with transient error; retrying after {backoff:?}: {e}",
                        n = attempt + 1,
                        max = self.retry.max_retries + 1,
                    );
                    tokio::time::sleep(backoff).await;
                    attempt += 1;
                    backoff = std::cmp::min(backoff.saturating_mul(2), Duration::from_secs(30));
                }
                Err(e) => return Err(e),
            }
        }
    }

    // ── Index Mapping ──────────────────────────────────────────────────

    /// Retrieve the mapping for `index`.
    pub async fn get_mapping(&self, index: &str) -> Result<MappingResponse> {
        let url = format!("{}/{}/_mapping", self.base_url, index);
        let resp = self
            .auth(self.http.get(&url))
            .send()
            .await
            .context(HttpRequestSnafu)?;
        let resp = check_status(resp).await?;
        resp.json().await.context(JsonParseSnafu)
    }

    // ── Search ─────────────────────────────────────────────────────────

    /// Execute a search request against `index`.
    pub async fn search(&self, index: &str, body: &SearchRequest) -> Result<SearchResponse> {
        let url = format!("{}/{}/_search", self.base_url, index);
        let resp = self
            .auth(self.http.post(&url))
            .json(body)
            .send()
            .await
            .context(HttpRequestSnafu)?;
        let resp = check_status(resp).await?;
        resp.json().await.context(JsonParseSnafu)
    }

    /// Execute a raw JSON search against `index`.
    pub async fn search_raw(
        &self,
        index: &str,
        body: &serde_json::Value,
    ) -> Result<SearchResponse> {
        let url = format!("{}/{}/_search", self.base_url, index);
        let resp = self
            .auth(self.http.post(&url))
            .json(body)
            .send()
            .await
            .context(HttpRequestSnafu)?;
        let resp = check_status(resp).await?;
        resp.json().await.context(JsonParseSnafu)
    }

    /// Open an Elasticsearch point-in-time reader for `index`.
    pub async fn open_point_in_time(&self, index: &str, keep_alive: &str) -> Result<String> {
        let url = format!("{}/{index}/_pit", self.base_url);
        let resp = self
            .auth(self.http.post(&url))
            .query(&[("keep_alive", keep_alive)])
            .send()
            .await
            .context(HttpRequestSnafu)?;
        let resp = check_status(resp).await?;
        let body: OpenPointInTimeResponse = resp.json().await.context(JsonParseSnafu)?;
        Ok(body.id)
    }

    /// Execute a raw JSON search using a point-in-time reader.
    pub async fn search_point_in_time(&self, body: &serde_json::Value) -> Result<SearchResponse> {
        let url = format!("{}/_search", self.base_url);
        let resp = self
            .auth(self.http.post(&url))
            .json(body)
            .send()
            .await
            .context(HttpRequestSnafu)?;
        let resp = check_status(resp).await?;
        resp.json().await.context(JsonParseSnafu)
    }

    /// Close an Elasticsearch point-in-time reader.
    pub async fn close_point_in_time(&self, pit_id: &str) -> Result<()> {
        let url = format!("{}/_pit", self.base_url);
        let resp = self
            .auth(self.http.delete(&url))
            .json(&serde_json::json!({ "id": pit_id }))
            .send()
            .await
            .context(HttpRequestSnafu)?;
        check_status(resp).await?;
        Ok(())
    }

    // ── Index Management ───────────────────────────────────────────────

    /// Check whether an index exists via `HEAD /<index>`.
    pub async fn index_exists(&self, index: &str) -> Result<bool> {
        let url = format!("{}/{}", self.base_url, index);
        let resp = self
            .auth(self.http.head(&url))
            .send()
            .await
            .context(HttpRequestSnafu)?;
        let status = resp.status();
        if status.is_success() {
            return Ok(true);
        }
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(false);
        }
        let body = resp
            .text()
            .await
            .unwrap_or_default()
            .replace(['\n', '\r'], " ");
        Err(Error::ElasticsearchError {
            status: status.as_u16(),
            message: body.trim().to_string(),
        })
    }

    /// Create an index with the provided mapping/settings body via `PUT /<index>`.
    pub async fn create_index(
        &self,
        index: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let url = format!("{}/{}", self.base_url, index);
        let resp = self
            .auth(self.http.put(&url))
            .json(body)
            .send()
            .await
            .context(HttpRequestSnafu)?;
        let resp = check_status(resp).await?;
        resp.json().await.context(JsonParseSnafu)
    }

    /// Update the mapping of an existing index via `PUT /<index>/_mapping`.
    pub async fn put_mapping(
        &self,
        index: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let url = format!("{}/{}/_mapping", self.base_url, index);
        let resp = self
            .auth(self.http.put(&url))
            .json(body)
            .send()
            .await
            .context(HttpRequestSnafu)?;
        let resp = check_status(resp).await?;
        resp.json().await.context(JsonParseSnafu)
    }

    /// Fetch the current `index.refresh_interval` for an index via `GET /<index>/_settings`.
    pub async fn get_index_refresh_interval(&self, index: &str) -> Result<Option<String>> {
        let url = format!("{}/{}/_settings", self.base_url, index);
        let resp = self
            .auth(self.http.get(&url))
            .send()
            .await
            .context(HttpRequestSnafu)?;
        let resp = check_status(resp).await?;
        let settings: serde_json::Value = resp.json().await.context(JsonParseSnafu)?;

        let index_settings = settings
            .get(index)
            .or_else(|| settings.as_object().and_then(|o| o.values().next()));

        Ok(index_settings
            .and_then(|s| s.pointer("/settings/index/refresh_interval"))
            .and_then(|v| {
                v.as_str().map(ToString::to_string).or_else(|| {
                    if v.is_null() {
                        None
                    } else {
                        Some(v.to_string())
                    }
                })
            }))
    }

    /// Update dynamic index settings via `PUT /<index>/_settings`.
    pub async fn put_index_settings(
        &self,
        index: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let url = format!("{}/{}/_settings", self.base_url, index);
        let resp = self
            .auth(self.http.put(&url))
            .json(body)
            .send()
            .await
            .context(HttpRequestSnafu)?;
        let resp = check_status(resp).await?;
        resp.json().await.context(JsonParseSnafu)
    }

    /// Make recently indexed documents searchable via `POST /<index>/_refresh`.
    pub async fn refresh_index(&self, index: &str) -> Result<serde_json::Value> {
        let url = format!("{}/{}/_refresh", self.base_url, index);
        let resp = self
            .auth(self.http.post(&url))
            .send()
            .await
            .context(HttpRequestSnafu)?;
        let resp = check_status(resp).await?;
        resp.json().await.context(JsonParseSnafu)
    }

    /// Force-merge an index via `POST /<index>/_forcemerge`.
    pub async fn force_merge(
        &self,
        index: &str,
        max_num_segments: u32,
    ) -> Result<serde_json::Value> {
        let url = format!("{}/{}/_forcemerge", self.base_url, index);
        let max_num_segments = max_num_segments.to_string();
        let resp = self
            .auth(self.http.post(&url))
            .query(&[("max_num_segments", max_num_segments.as_str())])
            .send()
            .await
            .context(HttpRequestSnafu)?;
        let resp = check_status(resp).await?;
        resp.json().await.context(JsonParseSnafu)
    }

    // ── Document CRUD ──────────────────────────────────────────────────

    /// Index (upsert) a single document.
    pub async fn index_document(
        &self,
        index: &str,
        id: &str,
        doc: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let url = format!("{}/{}/_doc/{}", self.base_url, index, id);
        let resp = self
            .auth(self.http.put(&url))
            .json(doc)
            .send()
            .await
            .context(HttpRequestSnafu)?;
        // The request body *is* a document, so a 4xx echoing it would report its content.
        let resp = check_status_without_body(resp).await?;
        resp.json().await.context(JsonParseSnafu)
    }

    /// Bulk index documents. Each element is `(optional_id, source_doc)`.
    pub async fn bulk_index(
        &self,
        index: &str,
        docs: &[(Option<String>, serde_json::Value)],
    ) -> Result<serde_json::Value> {
        let url = format!("{}/_bulk", self.base_url);
        let mut ndjson = String::new();
        for (id, doc) in docs {
            let action = if let Some(id) = id {
                serde_json::json!({"index": {"_index": index, "_id": id}})
            } else {
                serde_json::json!({"index": {"_index": index}})
            };
            ndjson.push_str(&serde_json::to_string(&action).context(JsonSerializeSnafu)?);
            ndjson.push('\n');
            ndjson.push_str(&serde_json::to_string(doc).context(JsonSerializeSnafu)?);
            ndjson.push('\n');
        }
        let url_ref = &url;
        // Freeze the NDJSON payload into `Bytes` so retries reuse the same
        // underlying buffer (cheap refcount clone) instead of copying the
        // entire payload on every attempt.
        let ndjson_bytes = Bytes::from(ndjson);
        let ndjson_ref = &ndjson_bytes;
        self.with_retry("bulk_index", |c| async move {
            let resp = c
                .auth(c.http.post(url_ref))
                .header(CONTENT_TYPE, "application/x-ndjson")
                .body(ndjson_ref.clone())
                .send()
                .await
                .context(HttpRequestSnafu)?;
            let resp = check_status_without_body(resp).await?;
            resp.json().await.context(JsonParseSnafu)
        })
        .await
    }

    /// Delete every document matching `query` via `POST /<index>/_delete_by_query`.
    pub async fn delete_by_query(
        &self,
        index: &str,
        query: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let url = format!("{}/{}/_delete_by_query", self.base_url, index);
        let body = serde_json::json!({ "query": query });
        let resp = self
            .auth(self.http.post(&url))
            .json(&body)
            .send()
            .await
            .context(HttpRequestSnafu)?;
        // The query is built from primary keys, so a 4xx echoing it would report them.
        let resp = check_status_without_body(resp).await?;
        resp.json().await.context(JsonParseSnafu)
    }
}

async fn check_status(resp: reqwest::Response) -> Result<reqwest::Response> {
    if resp.status().is_client_error() || resp.status().is_server_error() {
        let status = resp.status();
        let body = resp
            .text()
            .await
            .unwrap_or_default()
            .replace(['\n', '\r'], " ");
        return Err(Error::ElasticsearchError {
            status: status.as_u16(),
            message: body.trim().to_string(),
        });
    }
    Ok(resp)
}

/// Longest `error.type` this client will echo. Elasticsearch exception class names are
/// short (`mapper_parsing_exception`, `es_rejected_execution_exception`); anything longer
/// is not one, and is dropped rather than truncated.
const MAX_ERROR_CLASS_LEN: usize = 64;

/// Stands in for a response body that was read but deliberately not reported.
const WITHHELD_BODY: &str = "response body withheld (it can contain document data)";

/// True when `s` has the shape of an Elasticsearch exception class name: a short
/// `snake_case` run of ASCII lowercase letters, digits and underscores.
///
/// A shape check rather than an allow-list, so an exception type this build has never
/// heard of still reaches the operator, while anything that could carry row data — a
/// quoted document fragment, a key/value pair, whitespace, punctuation, non-ASCII text —
/// is refused.
fn is_error_class_token(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= MAX_ERROR_CLASS_LEN
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

/// Pull just the exception class out of an Elasticsearch error envelope.
///
/// Everything else the body carries — `error.reason`, `error.caused_by`, the document
/// `_id`, any fragment of the request a proxy echoed back — is deliberately dropped.
fn error_class_from_body(body: &str) -> Option<String> {
    let envelope: serde_json::Value = serde_json::from_str(body).ok()?;
    let class = envelope.get("error")?.get("type")?.as_str()?;
    is_error_class_token(class).then(|| class.to_string())
}

/// Status check for the requests whose body carries row data: `_bulk`, a single-document
/// index, and delete-by-query — whose query is built from primary keys.
///
/// `check_status` copies the whole response body into the error message. For the
/// metadata and query endpoints that body is the useful diagnostic and carries no row
/// data, but here it can quote document content: `error.reason` echoes the offending
/// fragment, and a proxy in front of the cluster may echo part of the request. Document
/// `_id`s are derived from the row's primary key, so letting the body through puts row
/// data into an error message, into `with_retry`'s warning on *every* attempt, and into
/// `runtime.task_history`.
///
/// So the body is read only to classify it: the status, and an `error.type` of the right
/// shape, are all that survive.
async fn check_status_without_body(resp: reqwest::Response) -> Result<reqwest::Response> {
    if !(resp.status().is_client_error() || resp.status().is_server_error()) {
        return Ok(resp);
    }

    let status = resp.status().as_u16();
    // Nothing derived from `body` other than the exception class escapes this scope.
    let class = match resp.text().await {
        Ok(body) => error_class_from_body(&body),
        // The body could not be read — a truncated or interrupted response. The status is
        // still the actionable signal, and there is nothing left to classify; surfacing the
        // transport error instead would reclassify an HTTP-status failure as a transport one
        // and change how `is_transient` treats it.
        Err(_) => None,
    };

    Err(Error::ElasticsearchError {
        status,
        // Still an `ElasticsearchError`, so `is_transient` — and with it the 429/5xx
        // retry classification in `with_retry` — keeps working unchanged.
        message: class.unwrap_or_else(|| WITHHELD_BODY.to_string()),
    })
}

// ── Helpers for building common queries ────────────────────────────────────

/// Build a `match_all` query.
#[must_use]
pub fn match_all_query() -> serde_json::Value {
    serde_json::json!({"match_all": {}})
}

/// Build a `match` query for full-text search on a single field.
#[must_use]
pub fn match_query(field: &str, text: &str) -> serde_json::Value {
    serde_json::json!({"match": {field: {"query": text}}})
}

/// Build a `multi_match` query across multiple fields.
#[must_use]
pub fn multi_match_query(fields: &[&str], text: &str) -> serde_json::Value {
    serde_json::json!({"multi_match": {"query": text, "fields": fields}})
}

/// Build a kNN query clause.
#[must_use]
pub fn knn_query(field: &str, vector: Vec<f32>, k: usize, num_candidates: usize) -> KnnQuery {
    KnnQuery {
        field: field.to_string(),
        query_vector: vector,
        k,
        num_candidates,
    }
}

/// Trait for pluggable Elasticsearch backends (useful for testing).
#[async_trait::async_trait]
pub trait Elasticsearch: std::fmt::Debug + Send + Sync {
    async fn get_mapping(&self, index: &str) -> Result<MappingResponse>;
    async fn search(&self, index: &str, body: &SearchRequest) -> Result<SearchResponse>;
    async fn search_raw(&self, index: &str, body: &serde_json::Value) -> Result<SearchResponse>;
    async fn open_point_in_time(&self, index: &str, keep_alive: &str) -> Result<String>;
    async fn search_point_in_time(&self, body: &serde_json::Value) -> Result<SearchResponse>;
    async fn close_point_in_time(&self, pit_id: &str) -> Result<()>;
    async fn index_exists(&self, index: &str) -> Result<bool>;
    async fn create_index(
        &self,
        index: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value>;
    async fn put_mapping(&self, index: &str, body: &serde_json::Value)
    -> Result<serde_json::Value>;
    async fn get_index_refresh_interval(&self, index: &str) -> Result<Option<String>>;
    async fn put_index_settings(
        &self,
        index: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value>;
    async fn refresh_index(&self, index: &str) -> Result<serde_json::Value>;
    async fn force_merge(&self, index: &str, max_num_segments: u32) -> Result<serde_json::Value>;
    async fn index_document(
        &self,
        index: &str,
        id: &str,
        doc: &serde_json::Value,
    ) -> Result<serde_json::Value>;
    async fn bulk_index(
        &self,
        index: &str,
        docs: &[(Option<String>, serde_json::Value)],
    ) -> Result<serde_json::Value>;
    async fn delete_by_query(
        &self,
        index: &str,
        query: &serde_json::Value,
    ) -> Result<serde_json::Value>;
}

#[async_trait::async_trait]
impl Elasticsearch for Client {
    async fn get_mapping(&self, index: &str) -> Result<MappingResponse> {
        self.get_mapping(index).await
    }

    async fn search(&self, index: &str, body: &SearchRequest) -> Result<SearchResponse> {
        self.search(index, body).await
    }

    async fn search_raw(&self, index: &str, body: &serde_json::Value) -> Result<SearchResponse> {
        self.search_raw(index, body).await
    }

    async fn open_point_in_time(&self, index: &str, keep_alive: &str) -> Result<String> {
        Client::open_point_in_time(self, index, keep_alive).await
    }

    async fn search_point_in_time(&self, body: &serde_json::Value) -> Result<SearchResponse> {
        Client::search_point_in_time(self, body).await
    }

    async fn close_point_in_time(&self, pit_id: &str) -> Result<()> {
        Client::close_point_in_time(self, pit_id).await
    }

    async fn index_exists(&self, index: &str) -> Result<bool> {
        self.index_exists(index).await
    }

    async fn create_index(
        &self,
        index: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        self.create_index(index, body).await
    }

    async fn put_mapping(
        &self,
        index: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        self.put_mapping(index, body).await
    }

    async fn get_index_refresh_interval(&self, index: &str) -> Result<Option<String>> {
        self.get_index_refresh_interval(index).await
    }

    async fn put_index_settings(
        &self,
        index: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        self.put_index_settings(index, body).await
    }

    async fn refresh_index(&self, index: &str) -> Result<serde_json::Value> {
        self.refresh_index(index).await
    }

    async fn force_merge(&self, index: &str, max_num_segments: u32) -> Result<serde_json::Value> {
        self.force_merge(index, max_num_segments).await
    }

    async fn index_document(
        &self,
        index: &str,
        id: &str,
        doc: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        self.index_document(index, id, doc).await
    }

    async fn bulk_index(
        &self,
        index: &str,
        docs: &[(Option<String>, serde_json::Value)],
    ) -> Result<serde_json::Value> {
        self.bulk_index(index, docs).await
    }

    async fn delete_by_query(
        &self,
        index: &str,
        query: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        Client::delete_by_query(self, index, query).await
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Client, ClientOptions, Error, RetryConfig, WITHHELD_BODY, error_class_from_body,
        is_error_class_token,
    };
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    /// Stands in for document content. A real `_bulk` rejection quotes the offending
    /// field value, and the `_id` is derived from the row's primary key, so this is the
    /// shape of thing that must not reach an error, a log line, or `task_history`.
    const SENTINEL: &str = "SENTINEL-ROW-VALUE-9F3A";

    /// Number of requests a stub served, shared with the test thread.
    type Hits = Arc<AtomicUsize>;

    /// A blocking HTTP stub that answers `max_conns` requests with the same status and
    /// body, and reports how many it served.
    ///
    /// `std::net` on its own thread rather than `tokio::net`: the workspace's tokio does
    /// not enable the `net` feature, and a blocking listener needs nothing extra.
    fn spawn_stub(status_line: &'static str, body: String, max_conns: usize) -> (String, Hits) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral port");
        let addr = listener.local_addr().expect("read the bound address");
        let hits = Arc::new(AtomicUsize::new(0));
        let served = Arc::clone(&hits);

        std::thread::spawn(move || {
            for stream in listener.incoming().take(max_conns) {
                let Ok(mut stream) = stream else { break };
                served.fetch_add(1, Ordering::SeqCst);

                // Consume the request head so the client has finished sending before we
                // answer. The stub never inspects it; the loop exists only to reach the
                // blank line that ends the head.
                {
                    let mut reader = BufReader::new(&mut stream);
                    let mut line = String::new();
                    loop {
                        line.clear();
                        match reader.read_line(&mut line) {
                            // 0 bytes is EOF; a bare CRLF is the end of the head.
                            Ok(0) => break,
                            Ok(_) if line == "\r\n" || line == "\n" => break,
                            Ok(_) => {}
                            Err(_) => break,
                        }
                    }
                }

                let response = format!(
                    "HTTP/1.1 {status_line}\r\nContent-Type: application/json\r\nContent-Length: {len}\r\nConnection: close\r\n\r\n{body}",
                    len = body.len(),
                );
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.flush();
            }
        });

        (format!("http://{addr}"), hits)
    }

    /// A client pointed at `url` with retries wound down so tests do not sleep.
    fn stub_client(url: &str, max_retries: u32) -> Client {
        let opts = ClientOptions {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(5),
            retry: RetryConfig {
                max_retries,
                initial_backoff: Duration::from_millis(1),
            },
        };
        Client::new_with_options(url, None, None, &opts).expect("build the stub client")
    }

    fn one_doc() -> Vec<(Option<String>, serde_json::Value)> {
        vec![(Some("row-1".to_string()), serde_json::json!({"body": "x"}))]
    }

    // ── The shape check ────────────────────────────────────────────────────

    #[test]
    fn error_class_tokens_admit_class_names_and_refuse_prose() {
        // Real exception classes, including ones this build has never heard of.
        assert!(is_error_class_token("mapper_parsing_exception"));
        assert!(is_error_class_token("es_rejected_execution_exception"));
        assert!(is_error_class_token("some_future_exception_v2"));

        // Anything that could carry a document fragment.
        assert!(!is_error_class_token(""));
        assert!(!is_error_class_token("failed to parse field [amount]"));
        assert!(!is_error_class_token("ROW-VALUE-9F3A"));
        assert!(!is_error_class_token("Mapper_Parsing"));
        assert!(!is_error_class_token("id='row-1'"));
        assert!(!is_error_class_token("значение"));
        // Long enough that it is prose wearing a class name's clothes.
        assert!(!is_error_class_token(&"a".repeat(65)));
    }

    #[test]
    fn error_class_extraction_keeps_only_the_class() {
        let body = format!(
            r#"{{"error":{{"type":"mapper_parsing_exception","reason":"failed to parse [{SENTINEL}]"}},"status":400}}"#
        );
        assert_eq!(
            error_class_from_body(&body).as_deref(),
            Some("mapper_parsing_exception")
        );

        // Not JSON at all — an HTML page from a proxy in front of the cluster.
        assert_eq!(
            error_class_from_body(&format!("<html><body>{SENTINEL}</body></html>")),
            None
        );
        // `error` as a bare string, which some endpoints return.
        assert_eq!(
            error_class_from_body(&format!(r#"{{"error":"{SENTINEL}"}}"#)),
            None
        );
        // A `type` that is prose rather than a class name is refused, not truncated.
        assert_eq!(
            error_class_from_body(&format!(r#"{{"error":{{"type":"{SENTINEL} bad"}}}}"#)),
            None
        );
    }

    // ── The HTTP paths ────────────────────────────────────────────────────

    #[tokio::test]
    async fn bulk_index_4xx_reports_the_class_and_not_the_body() {
        let body = format!(
            r#"{{"error":{{"type":"mapper_parsing_exception","reason":"failed to parse field [amount] in document id '{SENTINEL}'"}},"status":400}}"#
        );
        let (url, hits) = spawn_stub("400 Bad Request", body, 1);

        let err = stub_client(&url, 3)
            .bulk_index("idx", &one_doc())
            .await
            .expect_err("a 400 must surface as an error");

        let rendered = err.to_string();
        assert!(
            !rendered.contains(SENTINEL),
            "the response body reached the error: {rendered}"
        );
        assert!(
            rendered.contains("mapper_parsing_exception"),
            "the exception class should survive: {rendered}"
        );
        // 400 is not transient, so it must not have been retried.
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn bulk_index_429_withholds_the_body_from_every_attempt() {
        let body = format!(
            r#"{{"error":{{"type":"es_rejected_execution_exception","reason":"rejected doc '{SENTINEL}'"}},"status":429}}"#
        );
        let (url, hits) = spawn_stub("429 Too Many Requests", body, 4);

        let err = stub_client(&url, 3)
            .bulk_index("idx", &one_doc())
            .await
            .expect_err("an exhausted 429 must surface as an error");

        let rendered = err.to_string();
        // `with_retry` logs the error with `{e}` on every attempt, so this one assertion
        // covers the retry log as well as the returned error — they render the same value.
        assert!(
            !rendered.contains(SENTINEL),
            "the response body reached the error, and therefore the retry log: {rendered}"
        );
        assert!(rendered.contains("es_rejected_execution_exception"));
        // Still classified transient, so the retry budget was spent: 1 try + 3 retries.
        let still_es_error = matches!(err, Error::ElasticsearchError { status: 429, .. });
        assert!(still_es_error, "a 429 must stay an ElasticsearchError");
        assert_eq!(hits.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn bulk_index_withholds_a_body_it_cannot_classify() {
        // A proxy's HTML error page: no envelope to pull a class from, so nothing is echoed.
        let (url, _hits) = spawn_stub(
            "502 Bad Gateway",
            format!("<html><body>upstream said {SENTINEL}</body></html>"),
            1,
        );

        let err = stub_client(&url, 0)
            .bulk_index("idx", &one_doc())
            .await
            .expect_err("a 502 must surface as an error");

        let rendered = err.to_string();
        assert!(
            !rendered.contains(SENTINEL),
            "the response body reached the error: {rendered}"
        );
        assert!(rendered.contains(WITHHELD_BODY), "got: {rendered}");
    }

    #[tokio::test]
    async fn delete_by_query_4xx_does_not_report_the_query() {
        // The query is built from primary keys, so the echoed query is row data.
        let body = format!(
            r#"{{"error":{{"type":"search_phase_execution_exception","reason":"failed on ids ['{SENTINEL}']"}},"status":400}}"#
        );
        let (url, _hits) = spawn_stub("400 Bad Request", body, 1);
        let query = serde_json::json!({"ids": {"values": [SENTINEL]}});

        let err = stub_client(&url, 0)
            .delete_by_query("idx", &query)
            .await
            .expect_err("a 400 must surface as an error");

        let rendered = err.to_string();
        assert!(
            !rendered.contains(SENTINEL),
            "the response body reached the error: {rendered}"
        );
        assert!(rendered.contains("search_phase_execution_exception"));
    }

    #[tokio::test]
    async fn index_document_4xx_does_not_report_the_document() {
        let body = format!(
            r#"{{"error":{{"type":"strict_dynamic_mapping_exception","reason":"mapping set to strict, field [{SENTINEL}] not allowed"}},"status":400}}"#
        );
        let (url, _hits) = spawn_stub("400 Bad Request", body, 1);
        let doc = serde_json::json!({"amount": SENTINEL});

        let err = stub_client(&url, 0)
            .index_document("idx", "row-1", &doc)
            .await
            .expect_err("a 400 must surface as an error");

        let rendered = err.to_string();
        assert!(
            !rendered.contains(SENTINEL),
            "the response body reached the error: {rendered}"
        );
        assert!(rendered.contains("strict_dynamic_mapping_exception"));
    }

    #[tokio::test]
    async fn the_metadata_endpoints_still_report_their_body() {
        // The counterpart guard: redaction is scoped to the row-data paths, so an endpoint
        // whose body is the useful diagnostic keeps reporting it in full.
        let detail = "unknown setting [index.nope]";
        let (url, _hits) = spawn_stub(
            "400 Bad Request",
            format!(r#"{{"error":{{"type":"illegal_argument_exception","reason":"{detail}"}}}}"#),
            1,
        );

        let settings = serde_json::json!({"settings": {}});
        let err = stub_client(&url, 0)
            .create_index("idx", &settings)
            .await
            .expect_err("a 400 must surface as an error");

        let rendered = err.to_string();
        assert!(
            rendered.contains(detail),
            "the metadata diagnostic was lost: {rendered}"
        );
    }
}
