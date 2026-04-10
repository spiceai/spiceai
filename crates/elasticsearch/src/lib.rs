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

use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use std::collections::HashMap;
use url::Url;

// ── Error ──────────────────────────────────────────────────────────────────

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("HTTP request failed: {source}"))]
    HttpRequest { source: reqwest::Error },

    #[snafu(display("Failed to parse JSON response: {source}"))]
    JsonParse { source: reqwest::Error },

    #[snafu(display("Elasticsearch error: {message}"))]
    ElasticsearchError { message: String },

    #[snafu(display("Invalid URL: {source}"))]
    InvalidUrl { source: url::ParseError },
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
    pub hits: HitsEnvelope,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HitsEnvelope {
    pub total: HitsTotal,
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
    #[serde(default, rename = "_source")]
    pub source: serde_json::Value,
}

// ── Client ─────────────────────────────────────────────────────────────────

/// A lightweight Elasticsearch HTTP client.
#[derive(Debug, Clone)]
pub struct Client {
    http: reqwest::Client,
    base_url: Url,
    username: Option<String>,
    password: Option<String>,
}

impl Client {
    /// Create a new client pointing at the given Elasticsearch base URL.
    ///
    /// Optional basic-auth credentials are applied to every request.
    pub fn new(base_url: &str, username: Option<&str>, password: Option<&str>) -> Result<Self> {
        let mut url: Url = base_url.parse().context(InvalidUrlSnafu)?;

        // Strip trailing slash for clean path joining.
        let path = url.path().trim_end_matches('/').to_string();
        url.set_path(&path);

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        let http = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .context(HttpRequestSnafu)?;

        Ok(Self {
            http,
            base_url: url,
            username: username.map(ToString::to_string),
            password: password.map(ToString::to_string),
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

    // ── Index Mapping ──────────────────────────────────────────────────

    /// Retrieve the mapping for `index`.
    pub async fn get_mapping(&self, index: &str) -> Result<MappingResponse> {
        let url = format!("{}/{}/_mapping", self.base_url, index);
        let resp = self
            .auth(self.http.get(&url))
            .send()
            .await
            .context(HttpRequestSnafu)?;
        check_status(&resp)?;
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
        check_status(&resp)?;
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
        check_status(&resp)?;
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
        check_status(&resp)?;
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
            ndjson.push_str(&serde_json::to_string(&action).unwrap_or_default());
            ndjson.push('\n');
            ndjson.push_str(&serde_json::to_string(doc).unwrap_or_default());
            ndjson.push('\n');
        }

        let resp = self
            .auth(self.http.post(&url))
            .header(CONTENT_TYPE, "application/x-ndjson")
            .body(ndjson)
            .send()
            .await
            .context(HttpRequestSnafu)?;
        check_status(&resp)?;
        resp.json().await.context(JsonParseSnafu)
    }
}

fn check_status(resp: &reqwest::Response) -> Result<()> {
    if resp.status().is_client_error() || resp.status().is_server_error() {
        return Err(Error::ElasticsearchError {
            message: format!("HTTP {}", resp.status()),
        });
    }
    Ok(())
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
