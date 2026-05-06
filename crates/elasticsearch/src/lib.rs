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
        let resp = check_status(resp).await?;
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
            let resp = check_status(resp).await?;
            resp.json().await.context(JsonParseSnafu)
        })
        .await
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
}
