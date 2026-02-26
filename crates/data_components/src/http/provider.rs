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

use arrow::{
    array::{ArrayRef, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    error::ArrowError,
};
use arrow_array::UInt16Array;
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    common::{Constraints, project_schema},
    datasource::{TableProvider, TableType},
    error::{DataFusionError, Result as DataFusionResult},
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown, expr::InList},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
    scalar::ScalarValue,
};
use globset::{Glob, GlobSet, GlobSetBuilder};
use http::Uri;
use reqwest::{
    Client,
    header::{CACHE_CONTROL, HeaderMap},
};
use snafu::prelude::*;
use std::collections::{HashMap, HashSet};
use std::{
    any::Any,
    borrow::ToOwned,
    fmt,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::sync::RwLock;
use url::Url;
use util::{
    RetryError, retry,
    retry_strategy::{BackoffMethod, RetryBackoff, RetryBackoffBuilder},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("HTTP request failed: {source}"))]
    HttpRequest { source: reqwest::Error },

    #[snafu(display("HTTP request failed with status code {status}"))]
    HttpServerError { status: u16 },

    #[snafu(display("HTTP client error ({status}): {message}"))]
    HttpClientError { status: u16, message: String },

    #[snafu(display(
        "All {max_retries} retry attempts failed for HTTP request to {url}. Check network connectivity and endpoint availability."
    ))]
    AllRetriesFailed { max_retries: usize, url: String },

    #[snafu(display("Invalid URL: {source}"))]
    InvalidUrl { source: url::ParseError },

    #[snafu(display("Failed to process HTTP response data: {source}"))]
    Arrow { source: ArrowError },

    #[snafu(display("Failed to execute HTTP query: {source}"))]
    DataFusion { source: DataFusionError },

    #[snafu(display("Filter rejected: {message}"))]
    FilterRejected { message: String },

    #[snafu(display("HTTP provider configuration error: {message}"))]
    Configuration { message: String },
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<Error> for DataFusionError {
    fn from(err: Error) -> Self {
        match err {
            // Client errors (4xx) are query/plan errors - user's fault
            Error::HttpClientError { status, message } => {
                DataFusionError::Plan(format!("HTTP client error ({status}): {message}"))
            }
            // Server errors (5xx) are external errors
            Error::HttpServerError { status } => DataFusionError::External(Box::new(
                std::io::Error::other(format!("HTTP request failed with status code {status}")),
            )),
            // Retry exhaustion is an external error
            Error::AllRetriesFailed { max_retries, url } => {
                DataFusionError::External(Box::new(std::io::Error::other(format!(
                    "All {max_retries} retry attempts failed for HTTP request to {url}. Check network connectivity and endpoint availability."
                ))))
            }
            // All other errors are internal/external errors
            Error::HttpRequest { source } => DataFusionError::External(Box::new(source)),
            Error::InvalidUrl { source } => DataFusionError::External(Box::new(source)),
            Error::Arrow { source } => DataFusionError::ArrowError(Box::new(source), None),
            Error::DataFusion { source } => source,
            Error::FilterRejected { message } | Error::Configuration { message } => {
                DataFusionError::Plan(message)
            }
        }
    }
}

pub const DEFAULT_MAX_QUERY_LENGTH: usize = 1024;
pub const DEFAULT_MAX_BODY_BYTES: usize = 16 * 1024; // 16 KiB
const MAX_REQUEST_PATH_LENGTH: usize = 1024;
type PartitionSpec = (Option<String>, Option<String>, Option<String>);

#[derive(Clone)]
struct CachedResponse {
    content: Arc<String>,
    cached_at: SystemTime,
    max_age: Duration,
    detected_format: Option<String>,
    response_date: Option<SystemTime>,
    response_status: u16,
}

impl CachedResponse {
    fn is_fresh(&self) -> bool {
        self.cached_at
            .elapsed()
            .ok()
            .is_some_and(|elapsed| elapsed < self.max_age)
    }
}

#[derive(Default)]
struct PartitionAccumulator {
    paths: HashSet<String>,
    queries: Vec<Option<String>>,
    bodies: Vec<Option<String>>,
    has_path_filter: bool,
    has_query_filter: bool,
    has_body_filter: bool,
}

impl PartitionAccumulator {
    fn new() -> Self {
        Self::default()
    }

    fn record_path(&mut self, value: String) {
        self.paths.insert(value);
        self.has_path_filter = true;
    }

    fn record_query(&mut self, value: String) {
        let entry = Some(value);
        if !self.queries.contains(&entry) {
            self.queries.push(entry);
        }
        self.has_query_filter = true;
    }

    fn record_body(&mut self, value: String) {
        let entry = Some(value);
        if !self.bodies.contains(&entry) {
            self.bodies.push(entry);
        }
        self.has_body_filter = true;
    }

    fn finalize(mut self) -> (Vec<String>, Vec<Option<String>>, Vec<Option<String>>) {
        let mut paths: Vec<String> = if self.has_path_filter {
            self.paths.into_iter().collect()
        } else {
            vec![String::new()]
        };
        // Sort paths for deterministic ordering
        paths.sort();

        if !self.has_query_filter {
            self.queries.push(None);
        }
        if !self.has_body_filter {
            self.bodies.push(None);
        }
        (paths, self.queries, self.bodies)
    }
}

struct HttpFetchResult {
    content: String,
    max_age: Duration,
    detected_format: String,
    response_date: Option<SystemTime>,
    response_status: u16,
}

impl HttpFetchResult {
    fn should_cache(&self) -> bool {
        // We don't explicitly disable caching for 5xx responses because well-behaved servers
        // should return Cache-Control: no-cache or max-age=0 for transient error responses.
        // This keeps the caching logic simple and respects server-specified cache directives.
        self.max_age.as_secs() > 0
    }
}

/// A table provider that fetches data from HTTP endpoints based on path and query filters
#[derive(Clone)]
pub struct HttpTableProvider {
    base_url: Url,
    client: Client,
    file_format: String,
    schema: SchemaRef,
    constraints: Constraints,
    cache: Arc<RwLock<HashMap<String, CachedResponse>>>,
    acceleration_enabled: bool,
    retry_strategy: RetryBackoff,
    content_type: Option<String>,
    custom_headers: HeaderMap,
    allowed_paths: Option<(GlobSet, Vec<String>)>,
    allow_query_filters: bool,
    max_query_length: usize,
    allow_body_filters: bool,
    max_body_bytes: usize,
    health_probe: Option<String>,
}

impl std::fmt::Debug for HttpTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpTableProvider")
            .field("base_url", &self.base_url)
            .field("file_format", &self.file_format)
            .field("acceleration_enabled", &self.acceleration_enabled)
            .finish_non_exhaustive()
    }
}

impl HttpTableProvider {
    #[must_use]
    pub fn new(
        base_url: Url,
        client: Client,
        file_format: String,
        acceleration_enabled: bool,
    ) -> Self {
        Self {
            base_url,
            client,
            file_format,
            schema: Arc::new(Self::base_table_schema()),
            // No primary key constraints - HTTP responses can contain multiple rows
            // with the same (request_path, request_query, request_body) but different content
            // (e.g., search API results). Caching mode uses filter values as cache keys instead.
            constraints: Constraints::new_unverified(vec![]),
            cache: Arc::new(RwLock::new(HashMap::new())),
            acceleration_enabled,
            retry_strategy: RetryBackoffBuilder::new()
                .method(BackoffMethod::Fibonacci)
                .max_retries(Some(3))
                .build(),
            content_type: None,
            custom_headers: HeaderMap::new(),
            allowed_paths: None,
            allow_query_filters: false,
            max_query_length: DEFAULT_MAX_QUERY_LENGTH,
            allow_body_filters: false,
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
            health_probe: None,
        }
    }

    pub fn with_allowed_paths<I, S>(mut self, paths: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut patterns = Vec::new();
        let mut builder = GlobSetBuilder::new();

        for path in paths {
            let value = path.into().trim().to_string();
            ensure!(
                !value.is_empty(),
                ConfigurationSnafu {
                    message: "allowed_request_paths entries cannot be empty".to_string()
                }
            );
            ensure!(
                value.starts_with('/'),
                ConfigurationSnafu {
                    message: format!(
                        "allowed_request_paths entries must start with '/'. Invalid entry: {value}"
                    )
                }
            );
            ensure!(
                value.len() <= MAX_REQUEST_PATH_LENGTH,
                ConfigurationSnafu {
                    message: format!(
                        "allowed_request_paths entry {value} exceeds the maximum supported length of {MAX_REQUEST_PATH_LENGTH} characters"
                    )
                }
            );

            let glob = Glob::new(&value).map_err(|e| Error::Configuration {
                message: format!("Invalid glob pattern in allowed_request_paths '{value}': {e}"),
            })?;
            builder.add(glob);
            patterns.push(value);
        }

        self.allowed_paths = if patterns.is_empty() {
            None
        } else {
            let globset = builder.build().map_err(|e| Error::Configuration {
                message: format!("Failed to build glob matcher for allowed_request_paths: {e}"),
            })?;
            Some((globset, patterns))
        };
        Ok(self)
    }

    #[must_use]
    pub fn enable_query_filters(mut self, max_length: usize) -> Self {
        self.allow_query_filters = true;
        self.max_query_length = max_length.min(DEFAULT_MAX_QUERY_LENGTH * 4);
        self
    }

    #[must_use]
    pub fn enable_body_filters(mut self, max_bytes: usize) -> Self {
        self.allow_body_filters = true;
        self.max_body_bytes = max_bytes.min(DEFAULT_MAX_BODY_BYTES * 4);
        self
    }

    #[must_use]
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.retry_strategy = RetryBackoffBuilder::new()
            .method(self.retry_strategy.method)
            .max_retries(Some(max_retries as usize))
            .randomization_factor(self.retry_strategy.randomization_factor)
            .build();
        self
    }

    #[must_use]
    pub fn with_backoff_method(mut self, method: BackoffMethod) -> Self {
        self.retry_strategy = RetryBackoffBuilder::new()
            .method(method)
            .max_retries(self.retry_strategy.max_retries)
            .max_duration(self.retry_strategy.max_duration)
            .randomization_factor(self.retry_strategy.randomization_factor)
            .build();
        self
    }

    #[must_use]
    pub fn with_max_retry_duration(mut self, max_duration: Option<Duration>) -> Self {
        self.retry_strategy = RetryBackoffBuilder::new()
            .method(self.retry_strategy.method)
            .max_retries(self.retry_strategy.max_retries)
            .max_duration(max_duration)
            .randomization_factor(self.retry_strategy.randomization_factor)
            .build();
        self
    }

    #[must_use]
    pub fn with_retry_jitter(mut self, jitter: f64) -> Self {
        self.retry_strategy = RetryBackoffBuilder::new()
            .method(self.retry_strategy.method)
            .max_retries(self.retry_strategy.max_retries)
            .max_duration(self.retry_strategy.max_duration)
            .randomization_factor(jitter)
            .build();
        self
    }

    #[must_use]
    pub fn with_content_type(mut self, content_type: Option<String>) -> Self {
        self.content_type = content_type;
        self
    }

    #[must_use]
    pub fn with_headers(mut self, headers: HeaderMap) -> Self {
        self.custom_headers = headers;
        self
    }

    pub fn with_health_probe(mut self, health_probe: Option<String>) -> Result<Self> {
        if let Some(ref path) = health_probe {
            // Basic validation for health probe path
            ensure!(
                path.starts_with('/'),
                ConfigurationSnafu {
                    message: format!("health_probe path must start with '/'. Got: '{path}'",)
                }
            );
            ensure!(
                path.len() <= MAX_REQUEST_PATH_LENGTH,
                ConfigurationSnafu {
                    message: format!(
                        "health_probe path is too long ({} characters). Maximum allowed is {}",
                        path.len(),
                        MAX_REQUEST_PATH_LENGTH
                    )
                }
            );
        }
        self.health_probe = health_probe;
        Ok(self)
    }

    #[must_use]
    pub fn base_table_schema() -> Schema {
        Schema::new(vec![
            Field::new("request_path", DataType::Utf8, false),
            Field::new("request_query", DataType::Utf8, true),
            Field::new("request_body", DataType::Utf8, true),
            Field::new("content", DataType::Utf8, false),
            Field::new("response_status", DataType::UInt16, false),
            Field::new(
                "fetched_at",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                true,
            ),
        ])
    }

    /// Extract path and query from filters
    fn get_cache_key(path: &str, query: Option<&str>, body: Option<&str>) -> String {
        format!(
            "{}?{}&body={}",
            path,
            query.unwrap_or(""),
            body.unwrap_or("")
        )
    }

    /// Validates the HTTP endpoint by attempting a request to a custom health probe path if configured,
    /// or a non-existent path otherwise.
    /// This helps detect issues like DNS errors, connection problems,
    /// or invalid URLs early in the initialization process.
    pub async fn validate_endpoint(&self) -> Result<()> {
        let test_url = if let Some(ref health_probe_path) = self.health_probe {
            let mut test_url = self.base_url.clone();
            test_url.set_path(health_probe_path);
            test_url
        } else {
            use rand::Rng;
            use rand::distr::Alphanumeric;

            // Generate a random path that should return 404
            let random_suffix: String = rand::rng()
                .sample_iter(Alphanumeric)
                .take(16)
                .map(char::from)
                .collect();
            let test_path = format!("/__spice_health_check_{random_suffix}");

            let mut test_url = self.base_url.clone();
            test_url.set_path(&test_path);
            test_url
        };

        tracing::debug!("Validating HTTP endpoint: {test_url}");

        match self.client.get(test_url.clone()).send().await {
            Ok(response) => {
                let status = response.status();
                if self.health_probe.is_some() {
                    tracing::debug!(
                        "HTTP endpoint validation response using health probe: {test_url} (status: {status})"
                    );
                    // For custom health probe, require successful status (2xx)
                    if !status.is_success() {
                        return Err(Error::HttpClientError {
                            status: status.as_u16(),
                            message: format!(
                                "Failed to validate HTTP endpoint {}: Health probe {} returned non-success status {status}. Ensure the health probe endpoint is accessible and returns a 2xx status code.",
                                self.base_url,
                                test_url.path()
                            ),
                        });
                    }
                } else {
                    tracing::debug!(
                        "HTTP endpoint validation response: {test_url} (status: {status}). Any status (including 404) is expected for the random probe path."
                    );
                    // Any response (including 404) means the endpoint is reachable
                }
                Ok(())
            }
            Err(e) => {
                // Check the error type to provide more specific messages and just return the error
                Err(Error::HttpRequest { source: e })
            }
        }
    }

    fn parse_cache_control(cache_control_header: Option<&str>) -> Duration {
        let mut max_age = Duration::from_secs(0);

        if let Some(header) = cache_control_header {
            for directive in header.split(',') {
                let directive = directive.trim();
                if let Some(value) = directive.strip_prefix("max-age=")
                    && let Ok(seconds) = value.parse::<u64>()
                {
                    max_age = Duration::from_secs(seconds);
                }
            }
        }

        max_age
    }

    /// Detect file format from Content-Type header, path extension, or content
    fn detect_file_format(response: &reqwest::Response, path: &str) -> String {
        // 1. Try to detect from Content-Type header
        if let Some(content_type) = response.headers().get(reqwest::header::CONTENT_TYPE)
            && let Ok(content_type_str) = content_type.to_str()
        {
            let content_type_lower = content_type_str.to_lowercase();
            if content_type_lower.contains("application/json")
                || content_type_lower.contains("text/json")
            {
                return "json".to_string();
            } else if content_type_lower.contains("text/csv")
                || content_type_lower.contains("application/csv")
            {
                return "csv".to_string();
            } else if content_type_lower.contains("application/x-ndjson")
                || content_type_lower.contains("application/jsonlines")
                || content_type_lower.contains("application/jsonl")
                || content_type_lower.contains("application/x-jsonl")
            {
                return "ndjson".to_string();
            } else if content_type_lower.contains("application/x-parquet")
                || content_type_lower.contains("parquet")
            {
                return "parquet".to_string();
            } else if content_type_lower.contains("text/xml")
                || content_type_lower.contains("application/xml")
            {
                return "xml".to_string();
            }
        }

        // 2. Try to detect from path extension
        if let Some(extension) = std::path::Path::new(path).extension()
            && let Some(ext_str) = extension.to_str()
        {
            let ext_lower = ext_str.to_lowercase();
            match ext_lower.as_str() {
                "json" => return "json".to_string(),
                "csv" => return "csv".to_string(),
                "ndjson" | "jsonl" => return "ndjson".to_string(),
                "parquet" => return "parquet".to_string(),
                "xml" => return "xml".to_string(),
                _ => {}
            }
        }

        // 3. Return empty string if we can't detect - caller will try content-based detection
        String::new()
    }

    /// Infer file format from content by examining the first line
    fn infer_format_from_content(content: &str) -> String {
        let first_line = content.lines().next().unwrap_or("");
        let trimmed = first_line.trim();

        if trimmed.is_empty() {
            return "json".to_string();
        }

        // Check if it starts with JSON object or array
        if trimmed.starts_with('{') || trimmed.starts_with('[') {
            return "json".to_string();
        }

        // Check if it looks like XML
        if trimmed.starts_with('<') {
            return "xml".to_string();
        }

        // Check if it looks like CSV (has commas and doesn't start with {, [, or <)
        if trimmed.contains(',') && !trimmed.starts_with('{') && !trimmed.starts_with('[') {
            return "csv".to_string();
        }

        // Default to json
        "json".to_string()
    }

    fn build_request_url(&self, path: &str, query: Option<&str>) -> Result<Url> {
        let mut url = self.base_url.clone();

        if !path.is_empty() {
            let base_path = self.base_url.path();
            let full_path = if base_path == "/" || base_path.is_empty() {
                path.to_string()
            } else if path.starts_with('/') {
                format!("{}{}", base_path.trim_end_matches('/'), path)
            } else {
                format!("{}/{}", base_path.trim_end_matches('/'), path)
            };
            url.set_path(&full_path);
        }

        if let Some(q) = query {
            url.set_query(Some(q));
        }

        let final_url = url.as_str().to_owned();
        final_url
            .parse::<Uri>()
            .map_err(|err| Error::FilterRejected {
                message: format!("Constructed request URI '{final_url}' is invalid: {err}"),
            })?;

        Ok(url)
    }

    async fn cache_response(
        &self,
        path: &str,
        query: Option<&str>,
        body: Option<&str>,
        result: &HttpFetchResult,
    ) {
        let cache_key = Self::get_cache_key(path, query, body);
        let cached_response = CachedResponse {
            content: Arc::new(result.content.clone()),
            cached_at: SystemTime::now(),
            max_age: result.max_age,
            detected_format: Some(result.detected_format.clone()),
            response_date: result.response_date,
            response_status: result.response_status,
        };

        let mut cache_write = self.cache.write().await;
        cache_write.insert(cache_key, cached_response);
    }

    async fn perform_request_with_retry(
        &self,
        url: Url,
        body: Option<&str>,
        path_label: &str,
    ) -> Result<HttpFetchResult> {
        let retry_strategy = self.retry_strategy.clone();
        let this = self.clone();
        let url_clone = url.clone();
        let body_owned = body.map(ToOwned::to_owned);
        let path_owned = path_label.to_string();

        let result = retry(retry_strategy, || {
            let this = this.clone();
            let url = url_clone.clone();
            let body = body_owned.clone();
            let path = path_owned.clone();

            async move {
                this.perform_single_request(&url, body.as_deref(), &path, false)
                    .await
            }
        })
        .await;

        // If retries exhausted due to transient errors (5xx/429), make one final attempt
        // and return whatever response we get - the response is still valid data.
        // Don't retry on permanent errors (e.g., failed to read response body).
        if let Ok(fetch_result) = result {
            Ok(fetch_result)
        } else {
            tracing::debug!(
                "Retries exhausted for {url}, making final attempt accepting any status"
            );
            self.perform_single_request(&url, body, path_label, true)
                .await
                .map_err(|e| match e {
                    RetryError::Permanent(err) | RetryError::Transient { err, .. } => err,
                })
        }
    }

    /// Returns true for HTTP status codes that should trigger retry with backoff.
    ///
    /// Currently retries:
    /// - 5xx server errors (transient server issues)
    /// - 429 Too Many Requests (rate limiting)
    fn is_retryable_status(status_code: u16) -> bool {
        (500..600).contains(&status_code) || status_code == 429
    }

    /// Perform a single HTTP request without retry logic.
    ///
    /// If `accept_retryable` is false, returns a transient error on 5xx/429 to trigger retry.
    /// If `accept_retryable` is true, accepts any status code and returns the response.
    async fn perform_single_request(
        &self,
        url: &Url,
        body: Option<&str>,
        path_label: &str,
        accept_retryable: bool,
    ) -> std::result::Result<HttpFetchResult, RetryError<Error>> {
        let mut request_builder = if let Some(body_content) = body {
            let mut req = self.client.post(url.clone());
            let ct = self.content_type.as_deref().unwrap_or("application/json");
            req = req.header("Content-Type", ct);
            req.body(body_content.to_owned())
        } else {
            self.client.get(url.clone())
        };

        for (name, value) in &self.custom_headers {
            request_builder = request_builder.header(name, value);
        }

        let response = request_builder.send().await.map_err(|e| {
            tracing::debug!("HTTP request failed: {e}");
            RetryError::transient(Error::HttpRequest { source: e })
        })?;

        let status_code = response.status().as_u16();

        // 5xx/429: retry with backoff (transient server issue or rate limiting)
        // After retries exhausted, we'll accept the response as valid data.
        if !accept_retryable && Self::is_retryable_status(status_code) {
            tracing::debug!("HTTP retryable status ({status_code}), will retry");
            if let Err(e) = response.error_for_status() {
                return Err(RetryError::transient(Error::HttpRequest { source: e }));
            }
            // Defensive: should never reach here since 4xx and 5xx always produce error_for_status Err
            return Err(RetryError::transient(Error::HttpServerError {
                status: status_code,
            }));
        }

        // 2xx, 3xx, 4xx (and 5xx/429 when accept_retryable=true): valid response
        // 4xx like 404 "not found" is a valid business response, not an error
        Self::extract_response(response, status_code, path_label).await
    }

    /// Extract content and metadata from an HTTP response.
    async fn extract_response(
        response: reqwest::Response,
        status_code: u16,
        path_label: &str,
    ) -> std::result::Result<HttpFetchResult, RetryError<Error>> {
        let detected_format = Self::detect_file_format(&response, path_label);
        tracing::debug!(
            "Detected file format from Content-Type header: {}",
            detected_format
        );

        let cache_control_header = response
            .headers()
            .get(CACHE_CONTROL)
            .and_then(|v| v.to_str().ok());
        let max_age = Self::parse_cache_control(cache_control_header);

        // Extract Date header from response
        let response_date = response
            .headers()
            .get(reqwest::header::DATE)
            .and_then(|v| v.to_str().ok())
            .and_then(|date_str| {
                // Parse HTTP date format (RFC 2822/RFC 1123)
                httpdate::parse_http_date(date_str).ok()
            });

        let content = response
            .text()
            .await
            .map_err(|e| RetryError::permanent(Error::HttpRequest { source: e }))?;

        let detected_format = if detected_format.is_empty() {
            let inferred = Self::infer_format_from_content(&content);
            tracing::debug!("Inferred file format from content: {}", inferred);
            inferred
        } else {
            detected_format
        };

        Ok(HttpFetchResult {
            content,
            max_age,
            detected_format,
            response_date,
            response_status: status_code,
        })
    }

    async fn fetch_and_cache(
        &self,
        path: &str,
        query: Option<&str>,
        body: Option<&str>,
    ) -> Result<HttpFetchResult> {
        let url = self.build_request_url(path, query)?;
        let path_owned = path.to_string();
        let query_owned = query.map(ToOwned::to_owned);
        let body_owned = body.map(ToOwned::to_owned);

        let fetch_result = self
            .perform_request_with_retry(url, body_owned.as_deref(), &path_owned)
            .await?;

        if fetch_result.should_cache() {
            self.cache_response(
                &path_owned,
                query_owned.as_deref(),
                body_owned.as_deref(),
                &fetch_result,
            )
            .await;
        }

        Ok(fetch_result)
    }

    async fn get_response(
        &self,
        path: &str,
        query: Option<&str>,
        body: Option<&str>,
    ) -> Result<HttpFetchResult> {
        // When acceleration is enabled, skip HTTP-level caching - the acceleration layer handles it
        if self.acceleration_enabled {
            return self.fetch_and_cache(path, query, body).await;
        }

        let cache_key = Self::get_cache_key(path, query, body);

        // Try to get from cache
        let cached = {
            let cache = self.cache.read().await;
            cache.get(&cache_key).cloned()
        };

        if let Some(cached_response) = cached
            && cached_response.is_fresh()
        {
            if let Some(ref format) = cached_response.detected_format {
                tracing::debug!(
                    "Returning fresh cached content for {} (detected format: {})",
                    cache_key,
                    format
                );
            } else {
                tracing::debug!("Returning fresh cached content for {}", cache_key);
            }
            return Ok(HttpFetchResult {
                content: (*cached_response.content).clone(),
                max_age: cached_response.max_age,
                detected_format: cached_response.detected_format.clone().unwrap_or_default(),
                response_date: cached_response.response_date,
                response_status: cached_response.response_status,
            });
        }

        // Fetch fresh content
        self.fetch_and_cache(path, query, body).await
    }

    fn get_projected_schema(
        schema: &SchemaRef,
        projection: Option<&Vec<usize>>,
    ) -> DataFusionResult<SchemaRef> {
        let mut projected_schema = project_schema(schema, projection)?;
        if projected_schema.fields.is_empty() {
            let idx = schema.index_of("content")?;
            projected_schema = SchemaRef::from(schema.project(&[idx])?);
        }
        Ok(projected_schema)
    }
}

#[async_trait]
impl TableProvider for HttpTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn constraints(&self) -> Option<&Constraints> {
        Some(&self.constraints)
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // Push down filters as Inexact - they'll be used in scan() but not as partitions
        // This allows DataFusion to apply the filters while we extract values for HTTP requests
        Ok(filters
            .iter()
            .map(|f| {
                // Check if this specific filter can be pushed down
                if Self::can_pushdown_filter(f) {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        tracing::trace!(
            "HTTP scan called with {} filters, limit={:?}",
            filters.len(),
            limit
        );
        for (i, filter) in filters.iter().enumerate() {
            tracing::trace!("  Filter {}: {:?}", i, filter);
        }

        // Extract all (path, query, body) combinations that are allowed for this provider
        let partitions = self.extract_partitions(filters)?;

        tracing::trace!("Extracted {} partitions from filters", partitions.len());
        for (i, partition) in partitions.iter().enumerate() {
            tracing::trace!(
                "  Partition {}: path={:?}, query={:?}, body={:?}",
                i,
                partition.0,
                partition.1,
                partition.2
            );
        }

        Ok(Arc::new(HttpExec::new(
            Self::get_projected_schema(&self.schema, projection)?,
            Arc::new(self.clone()),
            partitions,
            limit,
        )))
    }
}

#[derive(Clone)]
pub struct HttpExec {
    projected_schema: SchemaRef,
    provider: Arc<HttpTableProvider>,
    partitions: Vec<PartitionSpec>,
    limit: Option<usize>,
    properties: PlanProperties,
}

impl HttpExec {
    #[must_use]
    pub fn new(
        projected_schema: SchemaRef,
        provider: Arc<HttpTableProvider>,
        partitions: Vec<PartitionSpec>,
        limit: Option<usize>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&projected_schema)),
            Partitioning::UnknownPartitioning(partitions.len()),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            projected_schema,
            provider,
            partitions,
            limit,
            properties,
        }
    }

    async fn fetch_and_create_batch(
        &self,
        provider: &HttpTableProvider,
        partition: usize,
    ) -> DataFusionResult<RecordBatch> {
        let (path, query, body) = &self.partitions[partition];

        // Use the filter path or empty string (base URL only)
        let path_val = path.as_deref().unwrap_or("");
        let query_val = query.as_deref();
        let body_val = body.as_deref();

        tracing::debug!(
            "HttpExec fetching partition {}: request_path={:?}, request_query={:?}, request_body={:?}",
            partition,
            path_val,
            query_val,
            body_val
        );

        // Fetch content with path, query, and body
        let result = provider
            .get_response(path_val, query_val, body_val)
            .await
            .map_err(DataFusionError::from)?;

        let HttpFetchResult {
            content,
            response_date,
            response_status,
            ..
        } = result;

        // Store the actual values from the partition for the primary key
        let path_for_batch = path.as_deref().unwrap_or("");
        let query_for_batch = query.as_deref().unwrap_or("");
        let body_for_batch = body.as_deref().unwrap_or("");

        tracing::debug!(
            "Creating batch with request_path={:?}, content_len={}",
            path_for_batch,
            content.len()
        );

        // Parse content to determine how many rows we'll create
        let content_rows = Self::parse_content(&content, self.limit);
        let num_rows = content_rows.len();

        if num_rows == 0 {
            tracing::warn!("No rows found in HTTP response for partition {}", partition);
            return Err(DataFusionError::Execution(
                "No rows found in HTTP response".to_string(),
            ));
        }

        // Create columns with the same number of rows
        // Use response Date header if available, otherwise use current time
        let timestamp_nanos = if let Some(date) = response_date {
            i64::try_from(
                date.duration_since(std::time::UNIX_EPOCH)
                    .map_err(|e| DataFusionError::Execution(format!("Invalid response date: {e}")))?
                    .as_nanos(),
            )
            .map_err(|e| DataFusionError::Execution(format!("Timestamp overflow: {e}")))?
        } else {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|e| {
                    DataFusionError::Execution(format!("Failed to get current time: {e}"))
                })?;
            i64::try_from(now.as_nanos())
                .map_err(|e| DataFusionError::Execution(format!("Timestamp overflow: {e}")))?
        };

        let columns = self
            .projected_schema
            .fields()
            .iter()
            .map(|field| match field.name().as_str() {
                "request_path" => {
                    Ok(Arc::new(StringArray::from(vec![path_for_batch; num_rows])) as ArrayRef)
                }
                "request_query" => {
                    Ok(Arc::new(StringArray::from(vec![query_for_batch; num_rows])) as ArrayRef)
                }
                "request_body" => {
                    Ok(Arc::new(StringArray::from(vec![body_for_batch; num_rows])) as ArrayRef)
                }
                "content" => Ok(Arc::new(StringArray::from(content_rows.clone())) as ArrayRef),
                "response_status" => {
                    Ok(Arc::new(UInt16Array::from(vec![response_status; num_rows])) as ArrayRef)
                }
                "fetched_at" => {
                    use arrow::array::TimestampNanosecondArray;
                    Ok(Arc::new(TimestampNanosecondArray::from(vec![
                        timestamp_nanos;
                        num_rows
                    ])) as ArrayRef)
                }
                _ => Err(DataFusionError::Execution(format!(
                    "Unsupported field name: {}",
                    field.name()
                ))),
            })
            .collect::<DataFusionResult<Vec<ArrayRef>>>()?;

        let batch = RecordBatch::try_new(Arc::clone(&self.projected_schema), columns)
            .map_err(DataFusionError::from)?;
        Ok(batch)
    }

    /// Parse content into individual rows
    /// - For JSON arrays: each element becomes a row
    /// - For JSON objects: single row
    /// - For newline-delimited JSON: each line becomes a row
    /// - For other content: single row
    ///
    /// If limit is provided, only returns up to that many rows
    fn parse_content(content: &str, limit: Option<usize>) -> Vec<String> {
        let trimmed = content.trim();

        // Handle empty content - return a single row with empty content
        // This is important for HTTP responses that return empty bodies (e.g., 5xx errors)
        if trimmed.is_empty() {
            return vec![content.to_string()];
        }

        // Try to parse as JSON
        if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(trimmed) {
            match json_value {
                serde_json::Value::Array(arr) => {
                    // JSON array: each element is a row
                    let mut rows: Vec<String> = arr
                        .into_iter()
                        .take(limit.unwrap_or(usize::MAX))
                        .map(|item| item.to_string())
                        .collect();
                    if let Some(lim) = limit
                        && rows.len() > lim
                    {
                        rows.truncate(lim);
                    }
                    return rows;
                }
                _ => {
                    // Single JSON object or primitive value: one row
                    return vec![json_value.to_string()];
                }
            }
        }

        // Try newline-delimited JSON (NDJSON)
        if trimmed.lines().all(|line| {
            let line_trimmed = line.trim();
            !line_trimmed.is_empty()
                && serde_json::from_str::<serde_json::Value>(line_trimmed).is_ok()
        }) {
            return trimmed
                .lines()
                .filter(|line| !line.trim().is_empty())
                .take(limit.unwrap_or(usize::MAX))
                .map(std::string::ToString::to_string)
                .collect();
        }

        // For non-JSON content (CSV, plain text, etc.), return as single row
        // In the future, we could parse CSV here too
        vec![content.to_string()]
    }
}

impl std::fmt::Debug for HttpExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "HttpExec")
    }
}

impl DisplayAs for HttpExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "HttpExec: base_url={}, format={}, partitions=[",
            self.provider.base_url, self.provider.file_format
        )?;

        for (i, (path, query, body)) in self.partitions.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(
                f,
                "(path={:?}, query={:?}, body={:?})",
                path.as_deref().unwrap_or(""),
                query.as_deref().unwrap_or(""),
                body.as_deref().unwrap_or("")
            )?;
        }

        write!(f, "]")
    }
}

impl ExecutionPlan for HttpExec {
    fn name(&self) -> &'static str {
        "HttpExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        tracing::trace!(
            "HttpExec::execute called for partition {}, total partitions: {}",
            partition,
            self.partitions.len()
        );

        let exec = Arc::new(self.clone());
        let provider = Arc::clone(&self.provider);
        let schema = Arc::clone(&self.projected_schema);

        // Use futures::stream::once to create a stream from a single async operation
        let stream = futures::stream::once(async move {
            tracing::trace!("Fetching partition {}", partition);
            let batch = exec.fetch_and_create_batch(&provider, partition).await?;
            tracing::trace!(
                "Yielding batch for partition {}: {} rows",
                partition,
                batch.num_rows()
            );
            Ok(batch)
        });

        let stream_adapter = RecordBatchStreamAdapter::new(schema, stream);
        Ok(Box::pin(stream_adapter))
    }
}

impl HttpTableProvider {
    /// Extract paths from filters for creating partitions. Query and body filters are validated but not used for partitioning.
    fn extract_partitions(&self, filters: &[Expr]) -> DataFusionResult<Vec<PartitionSpec>> {
        tracing::trace!(
            "extract_partitions called with {} filters, allowed_paths={:?}, allow_query_filters={}, allow_body_filters={}",
            filters.len(),
            self.allowed_paths,
            self.allow_query_filters,
            self.allow_body_filters
        );

        let mut accumulator = PartitionAccumulator::new();

        for filter in filters {
            self.extract_filter_values(filter, &mut accumulator)
                .map_err(DataFusionError::from)?;
        }

        tracing::trace!(
            "After processing filters: has_path_filter={}, has_query_filter={}, has_body_filter={}",
            accumulator.has_path_filter,
            accumulator.has_query_filter,
            accumulator.has_body_filter
        );

        let (paths, queries, bodies) = accumulator.finalize();

        tracing::trace!(
            "After finalize: paths={:?}, queries={:?}, bodies={:?}",
            paths,
            queries,
            bodies
        );

        let mut partitions = vec![];
        for p in &paths {
            for q in &queries {
                for b in &bodies {
                    partitions.push((
                        if p.is_empty() { None } else { Some(p.clone()) },
                        q.clone(),
                        b.clone(),
                    ));
                }
            }
        }

        Ok(partitions)
    }

    fn extract_filter_values(
        &self,
        filter: &Expr,
        accumulator: &mut PartitionAccumulator,
    ) -> Result<()> {
        match filter {
            Expr::BinaryExpr(expr) => self.handle_binary_expr(expr, accumulator),
            Expr::InList(in_list) => self.handle_in_list(in_list, accumulator),
            _ => Ok(()),
        }
    }

    fn handle_binary_expr(
        &self,
        expr: &BinaryExpr,
        accumulator: &mut PartitionAccumulator,
    ) -> Result<()> {
        match expr.op {
            Operator::Eq => self.handle_equality_expr(expr, accumulator),
            Operator::Or | Operator::And => {
                self.extract_filter_values(expr.left.as_ref(), accumulator)?;
                self.extract_filter_values(expr.right.as_ref(), accumulator)
            }
            _ => Ok(()),
        }
    }

    fn handle_equality_expr(
        &self,
        expr: &BinaryExpr,
        accumulator: &mut PartitionAccumulator,
    ) -> Result<()> {
        if let Expr::Column(column) = expr.left.as_ref()
            && let Expr::Literal(ScalarValue::Utf8(Some(value)), _) = expr.right.as_ref()
        {
            self.apply_literal_filter(column.name.as_str(), value, accumulator)?;
        }
        Ok(())
    }

    fn handle_in_list(
        &self,
        in_list: &InList,
        accumulator: &mut PartitionAccumulator,
    ) -> Result<()> {
        if let Expr::Column(column) = in_list.expr.as_ref()
            && matches!(
                column.name.as_str(),
                "request_path" | "request_query" | "request_body"
            )
        {
            for expr in &in_list.list {
                if let Expr::Literal(ScalarValue::Utf8(Some(value)), _) = expr {
                    self.apply_literal_filter(column.name.as_str(), value, accumulator)?;
                }
            }
        }
        Ok(())
    }

    fn apply_literal_filter(
        &self,
        column_name: &str,
        value: &str,
        accumulator: &mut PartitionAccumulator,
    ) -> Result<()> {
        tracing::trace!(
            "apply_literal_filter: column={}, value={}",
            column_name,
            value
        );
        match column_name {
            "request_path" => {
                let normalized = self.ensure_allowed_path(value)?;
                tracing::trace!("Path filter validated and normalized: {}", normalized);
                accumulator.record_path(normalized);
            }
            "request_query" => {
                let normalized = self.ensure_allowed_query(value)?;
                tracing::trace!("Query filter validated and normalized: {}", normalized);
                accumulator.record_query(normalized);
            }
            "request_body" => {
                let normalized = self.ensure_allowed_body(value)?;
                tracing::trace!("Body filter validated and normalized: {}", normalized);
                accumulator.record_body(normalized);
            }
            _ => {
                tracing::debug!("Ignoring filter on column: {}", column_name);
            }
        }
        Ok(())
    }

    /// Check if a filter expression can be pushed down to HTTP requests
    /// Note: This returns true if the filter is on `request_path`, `request_query`, or `request_body` columns.
    /// Actual validation (whether the feature is enabled/configured) happens in `extract_partitions` with user-friendly errors.
    fn can_pushdown_filter(filter: &Expr) -> bool {
        match filter {
            // Simple equality on request_path, request_query, or request_body
            Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::Eq => {
                if let Expr::Column(col) = left.as_ref() {
                    if let Expr::Literal(ScalarValue::Utf8(Some(_value)), _) = right.as_ref() {
                        matches!(
                            col.name.as_str(),
                            "request_path" | "request_query" | "request_body"
                        )
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            // IN list on request_path, request_query, or request_body
            Expr::InList(in_list) => {
                if let Expr::Column(col) = in_list.expr.as_ref() {
                    matches!(
                        col.name.as_str(),
                        "request_path" | "request_query" | "request_body"
                    )
                } else {
                    false
                }
            }
            // OR/AND expressions - recursively check both sides
            Expr::BinaryExpr(BinaryExpr { left, op, right })
                if *op == Operator::Or || *op == Operator::And =>
            {
                Self::can_pushdown_filter(left) && Self::can_pushdown_filter(right)
            }
            _ => false,
        }
    }

    fn ensure_allowed_path(&self, raw: &str) -> Result<String> {
        tracing::debug!(
            "ensure_allowed_path called with raw={}, allowed_paths={:?}",
            raw,
            self.allowed_paths
        );

        if raw.is_empty() {
            return Err(Error::FilterRejected {
                message: "The 'request_path' filter cannot be empty. Provide a valid path starting with '/', such as '/api/endpoint'.".to_string(),
            });
        }
        if raw.len() > MAX_REQUEST_PATH_LENGTH {
            return Err(Error::FilterRejected {
                message: format!(
                    "The 'request_path' value '{raw}' is too long ({} characters). Maximum allowed length is {MAX_REQUEST_PATH_LENGTH} characters.",
                    raw.len()
                ),
            });
        }
        if !raw.starts_with('/') {
            return Err(Error::FilterRejected {
                message: format!(
                    "The 'request_path' value '{raw}' must start with '/'. For example: '/api/endpoint' instead of '{raw}'."
                ),
            });
        }
        if raw.contains("..") {
            return Err(Error::FilterRejected {
                message: format!(
                    "The 'request_path' value '{raw}' contains '..' segments, which are not allowed for security reasons."
                ),
            });
        }

        let Some((globset, patterns)) = &self.allowed_paths else {
            tracing::warn!("Path filter attempted but allowed_paths is None");
            return Err(Error::FilterRejected {
                message:
                    "Cannot filter by 'request_path' because path filtering is disabled for this dataset. To enable, add the 'allowed_request_paths' parameter with a comma-separated list of allowed path patterns in your dataset configuration."
                        .to_string(),
            });
        };

        if !globset.is_match(raw) {
            return Err(Error::FilterRejected {
                message: format!(
                    "The 'request_path' value '{raw}' does not match any allowed path patterns. Allowed patterns are: [{}]. Update the 'allowed_request_paths' parameter in your dataset configuration to include a matching pattern.",
                    patterns
                        .iter()
                        .map(|p| format!("'{p}'"))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            });
        }

        Ok(raw.to_string())
    }

    fn ensure_allowed_query(&self, raw: &str) -> Result<String> {
        tracing::debug!(
            "ensure_allowed_query called with raw={}, allow_query_filters={}",
            raw,
            self.allow_query_filters
        );

        if !self.allow_query_filters {
            tracing::warn!("Query filter attempted but allow_query_filters is false");
            return Err(Error::FilterRejected {
                message:
                    "Cannot filter by 'request_query' because query filtering is disabled for this dataset. To enable, set the 'request_query_filters' parameter to 'enabled' in your dataset configuration.".to_string(),
            });
        }
        if raw.len() > self.max_query_length {
            return Err(Error::FilterRejected {
                message: format!(
                    "The 'request_query' value is too long ({} characters). Maximum allowed length is {} characters. You can increase this limit using the 'max_request_query_length' parameter.",
                    raw.len(),
                    self.max_query_length
                ),
            });
        }
        if raw.chars().any(char::is_control) {
            return Err(Error::FilterRejected {
                message: "The 'request_query' value contains control characters, which are not allowed for security reasons.".to_string(),
            });
        }

        let query = raw.strip_prefix('?').unwrap_or(raw);

        // We preserve the original query parameter order without sorting.
        // DataFusion's FilterExec uses the original filter value for matching:
        //   FilterExec: request_query@1 = q=test&page=1
        // If we sorted params to `page=1&q=test`, the stored data wouldn't match
        // the filter and queries would return no results.
        Ok(query.to_string())
    }

    fn ensure_allowed_body(&self, raw: &str) -> Result<String> {
        tracing::debug!(
            "ensure_allowed_body called with raw={}, allow_body_filters={}",
            raw,
            self.allow_body_filters
        );

        if !self.allow_body_filters {
            tracing::warn!("Body filter attempted but allow_body_filters is false");
            return Err(Error::FilterRejected {
                message:
                    "Cannot filter by 'request_body' because body filtering is disabled for this dataset. To enable, set the 'request_body_filters' parameter to 'enabled' in your dataset configuration.".to_string(),
            });
        }
        if raw.len() > self.max_body_bytes {
            return Err(Error::FilterRejected {
                message: format!(
                    "The 'request_body' value is too large ({} bytes). Maximum allowed size is {} bytes. You can increase this limit using the 'max_request_body_bytes' parameter.",
                    raw.len(),
                    self.max_body_bytes
                ),
            });
        }

        Ok(raw.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::Column;
    use datafusion::logical_expr::{BinaryExpr, Expr, Operator, expr::InList};
    use datafusion::scalar::ScalarValue;
    use std::sync::Arc;
    use url::Url;

    fn base_provider() -> HttpTableProvider {
        HttpTableProvider::new(
            Url::parse("https://api.example.com").expect("valid URL"),
            Client::new(),
            "json".to_string(),
            false,
        )
    }

    #[test]
    fn test_extract_partitions_with_path_and_query_filters() {
        let provider = base_provider()
            .with_allowed_paths(vec!["/singlesearch/shows".to_string()])
            .expect("allowed paths")
            .enable_query_filters(128);
        // Create filters: path = '/singlesearch/shows' AND query = 'q=South%20Park'
        let filters = vec![
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Column(Column::from_name("request_path"))),
                op: Operator::Eq,
                right: Box::new(Expr::Literal(
                    ScalarValue::Utf8(Some("/singlesearch/shows".to_string())),
                    None,
                )),
            }),
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Column(Column::from_name("request_query"))),
                op: Operator::Eq,
                right: Box::new(Expr::Literal(
                    ScalarValue::Utf8(Some("q=South%20Park".to_string())),
                    None,
                )),
            }),
        ];

        let partitions = provider.extract_partitions(&filters).expect("partitions");

        // Only path creates partition, query is validated but not used for partitioning
        assert_eq!(partitions.len(), 1);
        assert_eq!(
            partitions[0],
            (
                Some("/singlesearch/shows".to_string()),
                Some("q=South%20Park".to_string()),
                None
            )
        );
    }

    #[test]
    fn test_extract_partitions_with_only_path_filter() {
        let provider = base_provider()
            .with_allowed_paths(vec!["/api/data".to_string()])
            .expect("allowed paths");
        let filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("request_path"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("/api/data".to_string())),
                None,
            )),
        })];

        let partitions = provider.extract_partitions(&filters).expect("partitions");

        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0], (Some("/api/data".to_string()), None, None));
    }

    #[test]
    fn test_extract_partitions_with_no_filters() {
        let filters = vec![];

        let partitions = base_provider()
            .extract_partitions(&filters)
            .expect("partitions");

        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0], (None, None, None));
    }

    #[test]
    fn test_extract_partitions_multiple_paths() {
        let provider = base_provider()
            .with_allowed_paths(vec!["/path1".to_string(), "/path2".to_string()])
            .expect("allowed paths");
        let filters = vec![
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Column(Column::from_name("request_path"))),
                op: Operator::Eq,
                right: Box::new(Expr::Literal(
                    ScalarValue::Utf8(Some("/path1".to_string())),
                    None,
                )),
            }),
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Column(Column::from_name("request_path"))),
                op: Operator::Eq,
                right: Box::new(Expr::Literal(
                    ScalarValue::Utf8(Some("/path2".to_string())),
                    None,
                )),
            }),
        ];

        let partitions = provider.extract_partitions(&filters).expect("partitions");

        assert_eq!(partitions.len(), 2);
        assert!(partitions.contains(&(Some("/path1".to_string()), None, None)));
        assert!(partitions.contains(&(Some("/path2".to_string()), None, None)));
    }

    #[test]
    fn test_extract_partitions_with_in_list_path() {
        let provider = base_provider()
            .with_allowed_paths(vec![
                "/api/v1/users".to_string(),
                "/api/v1/posts".to_string(),
            ])
            .expect("allowed paths");
        // Create filter: path IN ('/api/v1/users', '/api/v1/posts')
        let filters = vec![Expr::InList(InList::new(
            Box::new(Expr::Column(Column::from_name("request_path"))),
            vec![
                Expr::Literal(ScalarValue::Utf8(Some("/api/v1/users".to_string())), None),
                Expr::Literal(ScalarValue::Utf8(Some("/api/v1/posts".to_string())), None),
            ],
            false,
        ))];

        let partitions = provider.extract_partitions(&filters).expect("partitions");

        assert_eq!(partitions.len(), 2);
        assert!(partitions.contains(&(Some("/api/v1/users".to_string()), None, None)));
        assert!(partitions.contains(&(Some("/api/v1/posts".to_string()), None, None)));
    }

    #[test]
    fn test_extract_partitions_with_in_list_query() {
        let provider = base_provider().enable_query_filters(64);
        // Create filter: query IN ('limit=10', 'limit=20')
        let filters = vec![Expr::InList(InList::new(
            Box::new(Expr::Column(Column::from_name("request_query"))),
            vec![
                Expr::Literal(ScalarValue::Utf8(Some("limit=10".to_string())), None),
                Expr::Literal(ScalarValue::Utf8(Some("limit=20".to_string())), None),
            ],
            false,
        ))];

        let partitions = provider.extract_partitions(&filters).expect("partitions");

        // Query filters don't create partitions, only path filters do
        // This will create a single partition with no path
        assert_eq!(partitions.len(), 2);
        assert_eq!(partitions[0], (None, Some("limit=10".to_string()), None));
        assert_eq!(partitions[1], (None, Some("limit=20".to_string()), None));
    }

    #[test]
    fn test_extract_partitions_with_or_expression() {
        let provider = base_provider()
            .with_allowed_paths(vec!["/api/v1".to_string(), "/api/v2".to_string()])
            .expect("allowed paths");
        // Create filter: path = '/api/v1' OR path = '/api/v2'
        let filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Column(Column::from_name("request_path"))),
                op: Operator::Eq,
                right: Box::new(Expr::Literal(
                    ScalarValue::Utf8(Some("/api/v1".to_string())),
                    None,
                )),
            })),
            op: Operator::Or,
            right: Box::new(Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Column(Column::from_name("request_path"))),
                op: Operator::Eq,
                right: Box::new(Expr::Literal(
                    ScalarValue::Utf8(Some("/api/v2".to_string())),
                    None,
                )),
            })),
        })];

        let partitions = provider.extract_partitions(&filters).expect("partitions");

        assert_eq!(partitions.len(), 2);
        assert!(partitions.contains(&(Some("/api/v1".to_string()), None, None)));
        assert!(partitions.contains(&(Some("/api/v2".to_string()), None, None)));
    }

    #[test]
    fn test_extract_partitions_with_combined_filters() {
        let provider = base_provider()
            .with_allowed_paths(vec!["/api/users".to_string()])
            .expect("allowed paths")
            .enable_query_filters(64);
        // Create filters: path = '/api/users' AND query IN ('limit=10', 'limit=20')
        let filters = vec![
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Column(Column::from_name("request_path"))),
                op: Operator::Eq,
                right: Box::new(Expr::Literal(
                    ScalarValue::Utf8(Some("/api/users".to_string())),
                    None,
                )),
            }),
            Expr::InList(InList::new(
                Box::new(Expr::Column(Column::from_name("request_query"))),
                vec![
                    Expr::Literal(ScalarValue::Utf8(Some("limit=10".to_string())), None),
                    Expr::Literal(ScalarValue::Utf8(Some("limit=20".to_string())), None),
                ],
                false,
            )),
        ];

        let partitions = provider.extract_partitions(&filters).expect("partitions");

        // Only path creates partition; query filters are validated but don't create separate partitions
        assert_eq!(partitions.len(), 2);
        assert_eq!(
            partitions[0],
            (
                Some("/api/users".to_string()),
                Some("limit=10".to_string()),
                None
            )
        );
        assert_eq!(
            partitions[1],
            (
                Some("/api/users".to_string()),
                Some("limit=20".to_string()),
                None
            )
        );
    }

    #[test]
    fn test_request_path_filter_rejected_without_allowlist() {
        let provider = base_provider();
        let filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("request_path"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("/blocked".to_string())),
                None,
            )),
        })];

        let err = provider
            .extract_partitions(&filters)
            .expect_err("expected rejection");
        match err {
            DataFusionError::Plan(message) => {
                assert!(message.contains("allowed_request_paths"));
            }
            other => panic!("Unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_request_query_filter_needs_enable() {
        let provider = base_provider();
        let filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("request_query"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("limit=1".to_string())),
                None,
            )),
        })];

        let err = provider
            .extract_partitions(&filters)
            .expect_err("expected rejection");
        match err {
            DataFusionError::Plan(message) => {
                assert!(message.contains("request_query_filters"));
            }
            other => panic!("Unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_request_body_filter_needs_enable() {
        let provider = base_provider();
        let filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("request_body"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("{".to_string())),
                None,
            )),
        })];

        let err = provider
            .extract_partitions(&filters)
            .expect_err("expected rejection");
        match err {
            DataFusionError::Plan(message) => {
                assert!(message.contains("request_body_filters"));
            }
            other => panic!("Unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_request_query_length_limit() {
        let provider = base_provider().enable_query_filters(4);
        let filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("request_query"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("long-value".to_string())),
                None,
            )),
        })];

        let err = provider
            .extract_partitions(&filters)
            .expect_err("expected rejection");
        match err {
            DataFusionError::Plan(message) => {
                assert!(message.contains("too long"));
            }
            other => panic!("Unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_request_body_size_limit() {
        let provider = base_provider().enable_body_filters(2);
        let filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("request_body"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("more".to_string())),
                None,
            )),
        })];

        let err = provider
            .extract_partitions(&filters)
            .expect_err("expected rejection");
        match err {
            DataFusionError::Plan(message) => {
                assert!(message.contains("too large"));
            }
            other => panic!("Unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_url_construction_with_base_path() {
        // Test that path from filter is appended to base URL path
        let base_url = Url::parse("https://api.example.com/v1").expect("valid URL");
        let provider = HttpTableProvider::new(base_url, Client::new(), "json".to_string(), false);

        // Simulate what fetch_and_cache does
        let mut url = provider.base_url.clone();
        let filter_path = "/users";

        let base_path = provider.base_url.path();
        let full_path = if base_path == "/" || base_path.is_empty() {
            filter_path.to_string()
        } else if filter_path.starts_with('/') {
            format!("{}{}", base_path.trim_end_matches('/'), filter_path)
        } else {
            format!("{}/{}", base_path.trim_end_matches('/'), filter_path)
        };
        url.set_path(&full_path);

        assert_eq!(url.path(), "/v1/users");
        assert_eq!(url.as_str(), "https://api.example.com/v1/users");
    }

    #[test]
    fn test_url_construction_without_base_path() {
        let base_url = Url::parse("https://api.example.com/").expect("valid URL");
        let provider = HttpTableProvider::new(base_url, Client::new(), "json".to_string(), false);

        let mut url = provider.base_url.clone();
        let filter_path = "/singlesearch/shows";

        let base_path = provider.base_url.path();
        let full_path = if base_path == "/" || base_path.is_empty() {
            filter_path.to_string()
        } else if filter_path.starts_with('/') {
            format!("{}{}", base_path.trim_end_matches('/'), filter_path)
        } else {
            format!("{}/{}", base_path.trim_end_matches('/'), filter_path)
        };
        url.set_path(&full_path);

        assert_eq!(url.path(), "/singlesearch/shows");
        assert_eq!(url.as_str(), "https://api.example.com/singlesearch/shows");
    }

    #[test]
    fn test_url_construction_with_query() {
        let base_url = Url::parse("https://api.example.com/").expect("valid URL");
        let provider = HttpTableProvider::new(base_url, Client::new(), "json".to_string(), false);

        let mut url = provider.base_url.clone();
        let filter_path = "/singlesearch/shows";
        let filter_query = "q=South%20Park";

        let base_path = provider.base_url.path();
        let full_path = if base_path == "/" || base_path.is_empty() {
            filter_path.to_string()
        } else if filter_path.starts_with('/') {
            format!("{}{}", base_path.trim_end_matches('/'), filter_path)
        } else {
            format!("{}/{}", base_path.trim_end_matches('/'), filter_path)
        };
        url.set_path(&full_path);
        url.set_query(Some(filter_query));

        assert_eq!(
            url.as_str(),
            "https://api.example.com/singlesearch/shows?q=South%20Park"
        );
    }

    #[test]
    fn test_cache_key_generation() {
        let key1 = HttpTableProvider::get_cache_key("/path", Some("query"), None);
        let key2 = HttpTableProvider::get_cache_key("/path", None, None);
        let key3 = HttpTableProvider::get_cache_key("/other", Some("query"), None);
        let key4 = HttpTableProvider::get_cache_key("/path", Some("query"), Some("body"));

        assert_eq!(key1, "/path?query&body=");
        assert_eq!(key2, "/path?&body=");
        assert_eq!(key3, "/other?query&body=");
        assert_eq!(key4, "/path?query&body=body");
        assert_ne!(key1, key2);
        assert_ne!(key1, key3);
        assert_ne!(key1, key4);
    }

    #[test]
    fn test_parse_content_empty_body() {
        // Empty body should return single row with empty content
        let rows = HttpExec::parse_content("", None);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], "");

        // Whitespace-only should also return single row
        let rows = HttpExec::parse_content("   ", None);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], "   ");
    }

    #[test]
    fn test_parse_content_json_object() {
        let content = r#"{"id": 1, "name": "test"}"#;
        let rows = HttpExec::parse_content(content, None);
        assert_eq!(rows.len(), 1);
        assert!(rows[0].contains("\"id\""));
    }

    #[test]
    fn test_parse_content_json_array() {
        let content = r#"[{"id": 1}, {"id": 2}, {"id": 3}]"#;
        let rows = HttpExec::parse_content(content, None);
        assert_eq!(rows.len(), 3);

        // With limit
        let rows = HttpExec::parse_content(content, Some(2));
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_parse_content_ndjson() {
        let content = "{\"id\": 1}\n{\"id\": 2}\n{\"id\": 3}";
        let rows = HttpExec::parse_content(content, None);
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn test_parse_content_plain_text() {
        let content = "This is plain text content";
        let rows = HttpExec::parse_content(content, None);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], content);
    }

    #[test]
    fn test_base_table_schema() {
        let schema = HttpTableProvider::base_table_schema();

        assert_eq!(schema.fields().len(), 6);
        assert_eq!(schema.field(0).name(), "request_path");
        assert_eq!(schema.field(1).name(), "request_query");
        assert_eq!(schema.field(2).name(), "request_body");
        assert_eq!(schema.field(3).name(), "content");
        assert_eq!(schema.field(4).name(), "response_status");
        assert_eq!(schema.field(5).name(), "fetched_at");
        assert_eq!(*schema.field(0).data_type(), DataType::Utf8);
        assert_eq!(*schema.field(1).data_type(), DataType::Utf8);
        assert_eq!(*schema.field(2).data_type(), DataType::Utf8);
        assert_eq!(*schema.field(3).data_type(), DataType::Utf8);
        assert_eq!(*schema.field(4).data_type(), DataType::UInt16);
        assert_eq!(
            *schema.field(5).data_type(),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None)
        );
        assert!(!schema.field(0).is_nullable()); // request_path is not nullable
        assert!(schema.field(1).is_nullable()); // request_query is nullable
        assert!(schema.field(2).is_nullable()); // request_body is nullable
        assert!(!schema.field(3).is_nullable()); // content is not nullable
        assert!(!schema.field(4).is_nullable()); // response_status is not nullable
        assert!(schema.field(5).is_nullable()); // fetched_at is nullable
    }

    #[test]
    fn test_get_projected_schema() {
        // Create a base schema as would be returned by base_table_schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("request_path", DataType::Utf8, false),
            Field::new("request_query", DataType::Utf8, true),
            Field::new("request_body", DataType::Utf8, true),
            Field::new("content", DataType::Utf8, false),
        ]));

        // Projection includes all
        let all_fields = vec![0, 1, 2, 3];
        let projected_schema =
            HttpTableProvider::get_projected_schema(&schema, Some(&all_fields)).expect("schema");
        let projected_field_names: Vec<_> =
            projected_schema.fields().iter().map(|f| f.name()).collect();
        assert_eq!(
            projected_field_names,
            &["request_path", "request_query", "request_body", "content"]
        );

        // Projection with some fields
        let some_fields = vec![0, 3];
        let projected_schema =
            HttpTableProvider::get_projected_schema(&schema, Some(&some_fields)).expect("schema");
        let projected_field_names: Vec<_> =
            projected_schema.fields().iter().map(|f| f.name()).collect();
        assert_eq!(projected_field_names, &["request_path", "content"]);

        // Empty projection triggers fallback to "content"
        let empty_fields: Vec<usize> = vec![];
        let projected_schema =
            HttpTableProvider::get_projected_schema(&schema, Some(&empty_fields)).expect("schema");
        let projected_field_names: Vec<_> =
            projected_schema.fields().iter().map(|f| f.name()).collect();
        assert_eq!(projected_field_names, &["content"]);

        // None projection defaults to all fields
        let projected_schema =
            HttpTableProvider::get_projected_schema(&schema, None).expect("schema");
        let projected_field_names: Vec<_> =
            projected_schema.fields().iter().map(|f| f.name()).collect();
        assert_eq!(
            projected_field_names,
            &["request_path", "request_query", "request_body", "content"]
        );
    }

    #[test]
    fn test_supports_filters_pushdown_returns_inexact() {
        use datafusion::logical_expr::TableProviderFilterPushDown;

        let provider = base_provider()
            .with_allowed_paths(vec!["/allowed/path".to_string()])
            .expect("allowed paths");

        // All request_path/query/body filters return Inexact
        // Actual validation happens during scan/extract_partitions
        let filter = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("request_path"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("/allowed/path".to_string())),
                None,
            )),
        });

        let result = provider
            .supports_filters_pushdown(&[&filter])
            .expect("should support");
        assert_eq!(result, vec![TableProviderFilterPushDown::Inexact]);

        // Even disallowed paths return Inexact (rejection happens in extract_partitions)
        let disallowed_filter = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("request_path"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("/not/allowed".to_string())),
                None,
            )),
        });

        let result = provider
            .supports_filters_pushdown(&[&disallowed_filter])
            .expect("should support");
        assert_eq!(result, vec![TableProviderFilterPushDown::Inexact]);
    }

    #[test]
    fn test_supports_filters_pushdown_always_inexact() {
        use datafusion::logical_expr::TableProviderFilterPushDown;

        // Provider without query filters enabled
        let provider = base_provider();

        let filter = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("request_query"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("q=test".to_string())),
                None,
            )),
        });

        // Returns Inexact even though query filters are disabled
        // Rejection happens during extract_partitions
        let result = provider
            .supports_filters_pushdown(&[&filter])
            .expect("should support");
        assert_eq!(result, vec![TableProviderFilterPushDown::Inexact]);
    }

    #[tokio::test]
    async fn test_query_params_any_order_works() {
        use datafusion::prelude::SessionContext;

        let url = Url::parse("https://api.tvmaze.com").expect("valid URL");
        let provider = HttpTableProvider::new(url, Client::new(), "json".to_string(), false)
            .with_allowed_paths(vec!["/search/people".to_string()])
            .expect("allowed paths")
            .enable_query_filters(128);

        let ctx = SessionContext::new();
        ctx.register_table("tvmaze", Arc::new(provider))
            .expect("register table");

        // Query with unordered params (q first, page second)
        let df1 = ctx
            .sql("SELECT content FROM tvmaze WHERE request_path = '/search/people' AND request_query = 'q=lauren&page=1'")
            .await
            .expect("unordered query should succeed");

        let results1 = df1.collect().await.expect("collect should succeed");
        assert!(
            !results1.is_empty(),
            "Should have results for unordered params"
        );
        assert!(
            results1[0].num_rows() > 0,
            "Should have rows for unordered params"
        );

        // Query with alphabetically ordered params (page first, q second)
        let df2 = ctx
            .sql("SELECT content FROM tvmaze WHERE request_path = '/search/people' AND request_query = 'page=1&q=michael'")
            .await
            .expect("alphabetical query should succeed");

        let results2 = df2.collect().await.expect("collect should succeed");
        assert!(
            !results2.is_empty(),
            "Should have results for alphabetical params"
        );
        assert!(
            results2[0].num_rows() > 0,
            "Should have rows for alphabetical params"
        );
    }

    // Integration tests that make real HTTP requests
    // These are marked with #[ignore] by default to avoid network dependencies in CI

    // Tests for globset pattern matching
    #[test]
    fn test_glob_pattern_wildcard() {
        let provider = base_provider()
            .with_allowed_paths(vec!["/api/*".to_string()])
            .expect("allowed paths");

        let filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("request_path"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("/api/users".to_string())),
                None,
            )),
        })];

        let partitions = provider.extract_partitions(&filters).expect("partitions");
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0], (Some("/api/users".to_string()), None, None));
    }

    #[test]
    fn test_glob_pattern_double_wildcard() {
        let provider = base_provider()
            .with_allowed_paths(vec!["/api/**".to_string()])
            .expect("allowed paths");

        // Should match nested paths
        let filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("request_path"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("/api/v1/users/123".to_string())),
                None,
            )),
        })];

        let partitions = provider.extract_partitions(&filters).expect("partitions");
        assert_eq!(partitions.len(), 1);
        assert_eq!(
            partitions[0],
            (Some("/api/v1/users/123".to_string()), None, None)
        );
    }

    #[test]
    fn test_glob_pattern_character_class() {
        let provider = base_provider()
            .with_allowed_paths(vec!["/api/v[0-9]/*".to_string()])
            .expect("allowed paths");

        // Should match v1, v2, etc.
        let filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("request_path"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("/api/v1/users".to_string())),
                None,
            )),
        })];

        let partitions = provider.extract_partitions(&filters).expect("partitions");
        assert_eq!(partitions.len(), 1);
        assert_eq!(
            partitions[0],
            (Some("/api/v1/users".to_string()), None, None)
        );
    }

    #[test]
    fn test_glob_pattern_rejection() {
        let provider = base_provider()
            .with_allowed_paths(vec!["/api/*".to_string()])
            .expect("allowed paths");

        // Should reject paths that don't match the pattern
        let filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("request_path"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("/admin/users".to_string())),
                None,
            )),
        })];

        let err = provider
            .extract_partitions(&filters)
            .expect_err("expected rejection");
        match err {
            DataFusionError::Plan(message) => {
                assert!(message.contains("does not match any allowed path patterns"));
                assert!(message.contains("/admin/users"));
            }
            other => panic!("Unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_glob_pattern_multiple_patterns() {
        let provider = base_provider()
            .with_allowed_paths(vec!["/api/*".to_string(), "/search/**".to_string()])
            .expect("allowed paths");

        // Test first pattern matches
        let filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("request_path"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("/api/posts".to_string())),
                None,
            )),
        })];

        let partitions = provider.extract_partitions(&filters).expect("partitions");
        assert_eq!(partitions.len(), 1);

        // Test second pattern matches
        let filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("request_path"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("/search/deep/nested/path".to_string())),
                None,
            )),
        })];

        let partitions = provider.extract_partitions(&filters).expect("partitions");
        assert_eq!(partitions.len(), 1);
    }

    #[test]
    fn test_glob_pattern_exact_match() {
        let provider = base_provider()
            .with_allowed_paths(vec!["/api/users".to_string()])
            .expect("allowed paths");

        // Exact string (no glob chars) should still work
        let filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("request_path"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("/api/users".to_string())),
                None,
            )),
        })];

        let partitions = provider.extract_partitions(&filters).expect("partitions");
        assert_eq!(partitions.len(), 1);
    }

    #[test]
    fn test_glob_pattern_question_mark() {
        let provider = base_provider()
            .with_allowed_paths(vec!["/api/user?".to_string()])
            .expect("allowed paths");

        // ? matches single character
        let filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("request_path"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("/api/users".to_string())),
                None,
            )),
        })];

        let partitions = provider.extract_partitions(&filters).expect("partitions");
        assert_eq!(partitions.len(), 1);
    }

    #[test]
    fn test_glob_pattern_invalid_pattern() {
        // Invalid glob pattern should fail gracefully
        let result = base_provider().with_allowed_paths(vec!["/[invalid".to_string()]);

        assert!(result.is_err());
        let err = result.expect_err("should fail");
        match &err {
            Error::Configuration { message } => {
                // globset error message contains pattern syntax errors
                assert!(
                    message.contains("Invalid glob pattern")
                        || message.contains("unclosed")
                        || message.contains("regex")
                );
            }
            other => panic!("Unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_glob_pattern_with_in_list() {
        let provider = base_provider()
            .with_allowed_paths(vec!["/api/*".to_string(), "/v[0-9]/search".to_string()])
            .expect("allowed paths");

        // Test IN list with multiple values matching different patterns
        let filters = vec![Expr::InList(InList::new(
            Box::new(Expr::Column(Column::from_name("request_path"))),
            vec![
                Expr::Literal(ScalarValue::Utf8(Some("/api/users".to_string())), None),
                Expr::Literal(ScalarValue::Utf8(Some("/v1/search".to_string())), None),
            ],
            false,
        ))];

        let partitions = provider.extract_partitions(&filters).expect("partitions");
        assert_eq!(partitions.len(), 2);
        assert!(partitions.contains(&(Some("/api/users".to_string()), None, None)));
        assert!(partitions.contains(&(Some("/v1/search".to_string()), None, None)));
    }

    #[test]
    fn test_glob_pattern_wildcard_matches_single_level() {
        let provider = base_provider()
            .with_allowed_paths(vec!["/api/*".to_string()])
            .expect("allowed paths");

        // Single * matches one path segment (no slash in the matched part)
        let filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("request_path"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("/api/users".to_string())),
                None,
            )),
        })];

        let partitions = provider.extract_partitions(&filters).expect("should match");
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0], (Some("/api/users".to_string()), None, None));
    }

    #[test]
    fn test_glob_pattern_mixed_exact_and_patterns() {
        let provider = base_provider()
            .with_allowed_paths(vec![
                "/exact/path".to_string(),
                "/api/*".to_string(),
                "/search/**".to_string(),
            ])
            .expect("allowed paths");

        // Test exact match
        let filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("request_path"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("/exact/path".to_string())),
                None,
            )),
        })];
        provider.extract_partitions(&filters).expect("should match");

        // Test * pattern
        let filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("request_path"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("/api/posts".to_string())),
                None,
            )),
        })];
        provider.extract_partitions(&filters).expect("should match");

        // Test ** pattern
        let filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("request_path"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("/search/a/b/c".to_string())),
                None,
            )),
        })];
        provider.extract_partitions(&filters).expect("should match");

        // Test non-matching path
        let filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("request_path"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("/other/path".to_string())),
                None,
            )),
        })];
        provider
            .extract_partitions(&filters)
            .expect_err("should not match");
    }

    #[tokio::test]
    async fn test_integration_jsonplaceholder_single_post() {
        use datafusion::prelude::SessionContext;

        let url = Url::parse("https://jsonplaceholder.typicode.com").expect("valid URL");
        let provider = HttpTableProvider::new(url, Client::new(), "json".to_string(), false)
            .with_allowed_paths(vec!["/posts/1".to_string()])
            .expect("allowed paths");

        let ctx = SessionContext::new();
        ctx.register_table("posts", Arc::new(provider))
            .expect("register table");

        // Test basic query
        let df = ctx
            .sql("SELECT request_path, content, response_status FROM posts WHERE request_path = '/posts/1'")
            .await
            .expect("query should succeed");

        let results = df.collect().await.expect("collect should succeed");
        assert!(!results.is_empty(), "Should have results");

        let batch = &results[0];
        assert!(batch.num_rows() > 0, "Should have rows");
        assert_eq!(batch.num_columns(), 3);

        // Validate response_status is 200 for successful request
        let status_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::UInt16Array>()
            .expect("response_status should be UInt16Array");
        assert_eq!(
            status_col.value(0),
            200,
            "Successful request should have response_status 200"
        );

        // Validate content contains expected post fields
        let content_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("content should be string array");

        let content = content_col.value(0);
        assert!(content.contains("userId"), "Should contain userId field");
        assert!(
            content.contains("\"id\"") && content.contains('1'),
            "Should contain id field with value 1"
        );
        assert!(content.contains("title"), "Should contain title field");
        assert!(content.contains("body"), "Should contain body field");

        // Validate actual field values from the API
        assert!(
            content.contains("sunt aut facere repellat provident"),
            "Should contain expected title text"
        );
        assert!(
            content.contains("quia et suscipit"),
            "Should contain expected body text"
        );
    }

    #[tokio::test]
    async fn test_integration_jsonplaceholder_multiple_posts() {
        use datafusion::prelude::SessionContext;

        let url = Url::parse("https://jsonplaceholder.typicode.com").expect("valid URL");
        let provider = HttpTableProvider::new(url, Client::new(), "json".to_string(), false)
            .with_allowed_paths(vec![
                "/posts/1".to_string(),
                "/posts/2".to_string(),
                "/posts/3".to_string(),
            ])
            .expect("allowed paths");

        let ctx = SessionContext::new();
        ctx.register_table("posts", Arc::new(provider))
            .expect("register table");

        // Test IN list filter for multiple paths
        let df = ctx
            .sql("SELECT request_path, content, response_status FROM posts WHERE request_path IN ('/posts/1', '/posts/2', '/posts/3')")
            .await
            .expect("query should succeed");

        let results = df.collect().await.expect("collect should succeed");
        assert!(!results.is_empty(), "Should have results");

        let total_rows: usize = results.iter().map(arrow_array::RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 3, "Should have exactly 3 rows for 3 posts");

        // Verify response_status is 200 for all successful requests and content contains expected post IDs
        let mut found_posts = [false, false, false]; // Track posts 1, 2, 3
        for batch in &results {
            let content_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .expect("content should be string array");

            let status_col = batch
                .column(2)
                .as_any()
                .downcast_ref::<arrow::array::UInt16Array>()
                .expect("response_status should be UInt16Array");

            for i in 0..batch.num_rows() {
                // Validate response_status is 200
                assert_eq!(
                    status_col.value(i),
                    200,
                    "All successful requests should have response_status 200"
                );

                let content = content_col.value(i);
                assert!(content.contains("userId"), "Should contain userId field");
                assert!(content.contains("id"), "Should contain id field");
                assert!(content.contains("title"), "Should contain title field");

                // Check which post this is by title
                if content.contains("sunt aut facere repellat provident") {
                    found_posts[0] = true;
                } else if content.contains("qui est esse") {
                    found_posts[1] = true;
                } else if content.contains("ea molestias quasi exercitationem") {
                    found_posts[2] = true;
                }
            }
        }

        assert!(found_posts[0], "Should have found post 1");
        assert!(found_posts[1], "Should have found post 2");
        assert!(found_posts[2], "Should have found post 3");
    }
    #[tokio::test]
    async fn test_integration_jsonplaceholder_all_posts() {
        use datafusion::prelude::SessionContext;

        let url = Url::parse("https://jsonplaceholder.typicode.com").expect("valid URL");
        let provider = HttpTableProvider::new(url, Client::new(), "json".to_string(), false)
            .with_allowed_paths(vec!["/posts".to_string()])
            .expect("allowed paths");

        let ctx = SessionContext::new();
        ctx.register_table("posts", Arc::new(provider))
            .expect("register table");

        // Test fetching all posts (returns JSON array)
        let df = ctx
            .sql("SELECT request_path, content FROM posts WHERE request_path = '/posts'")
            .await
            .expect("query should succeed");

        let results = df.collect().await.expect("collect should succeed");
        assert!(!results.is_empty(), "Should have results");

        // JSONPlaceholder /posts returns exactly 100 posts as a JSON array
        let total_rows: usize = results.iter().map(arrow_array::RecordBatch::num_rows).sum();
        assert_eq!(
            total_rows, 100,
            "Should have exactly 100 posts from /posts endpoint"
        );

        // Verify first post has expected structure
        let batch = &results[0];
        let content_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("content should be string array");

        let first_post = content_col.value(0);
        assert!(first_post.contains("userId"), "Should contain userId field");
        assert!(first_post.contains("id"), "Should contain id field");
        assert!(first_post.contains("title"), "Should contain title field");
        assert!(first_post.contains("body"), "Should contain body field");

        // Validate first post has expected values
        assert!(
            first_post.contains("sunt aut facere repellat provident"),
            "First post should have expected title"
        );

        // Verify we can find a post with id 100 (last post)
        let mut found_last_post = false;
        for batch in &results {
            let content_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .expect("content should be string array");

            for i in 0..batch.num_rows() {
                let content = content_col.value(i);
                // Last post has id 100
                if content.contains("\"id\"")
                    && content.contains("100")
                    && !content.contains("1000")
                {
                    found_last_post = true;
                    break;
                }
            }
        }
        assert!(found_last_post, "Should have found post with id 100");
    }
    #[tokio::test]
    async fn test_integration_tvmaze_single_show() {
        use datafusion::prelude::SessionContext;

        let url = Url::parse("https://api.tvmaze.com").expect("valid URL");
        let provider = HttpTableProvider::new(url, Client::new(), "json".to_string(), false)
            .with_allowed_paths(vec!["/shows/1".to_string()])
            .expect("allowed paths");

        let ctx = SessionContext::new();
        ctx.register_table("shows", Arc::new(provider))
            .expect("register table");

        // Test basic query with filter
        let df = ctx
            .sql("SELECT request_path, content FROM shows WHERE request_path = '/shows/1'")
            .await
            .expect("query should succeed");

        let results = df.collect().await.expect("collect should succeed");
        assert!(!results.is_empty(), "Should have results");

        let batch = &results[0];
        assert!(batch.num_rows() > 0, "Should have rows");

        // Verify content is JSON
        let content_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("content should be string array");

        let content = content_col.value(0);
        assert!(content.starts_with('{'), "Should be JSON object");
        assert!(
            content.contains("\"id\"") && content.contains('1'),
            "Should contain id field with value 1"
        );
        assert!(
            content.contains("\"name\"") && content.contains("Under the Dome"),
            "Should be 'Under the Dome'"
        );
        assert!(content.contains("url"), "Should contain url field");
        assert!(content.contains("genres"), "Should contain genres field");
        assert!(content.contains("summary"), "Should contain summary field");

        // Validate specific field values
        assert!(content.contains("Scripted"), "Should have type 'Scripted'");
        assert!(content.contains("Drama"), "Should have Drama genre");
        assert!(
            content.contains("Science-Fiction"),
            "Should have Science-Fiction genre"
        );
        assert!(
            content.contains("sealed off from the rest of the world"),
            "Should contain expected summary text"
        );
    }

    #[tokio::test]
    async fn test_integration_tvmaze_404_not_found() {
        use datafusion::prelude::SessionContext;

        // Use an invalid route that returns 404 with JSON error body
        let url = Url::parse("https://api.tvmaze.com").expect("valid URL");
        let provider = HttpTableProvider::new(url, Client::new(), "json".to_string(), false)
            .with_allowed_paths(vec!["/search/invalid_404".to_string()])
            .expect("allowed paths");

        let ctx = SessionContext::new();
        ctx.register_table("tvmaze", Arc::new(provider))
            .expect("register table");

        // Query for an invalid route - should return a row with 404 status and error JSON
        let df = ctx
            .sql("SELECT request_path, content, response_status FROM tvmaze WHERE request_path = '/search/invalid_404'")
            .await
            .expect("query should succeed");

        let results = df.collect().await.expect("collect should succeed");
        assert!(!results.is_empty(), "Should have results even for 404");

        let batch = &results[0];
        assert_eq!(batch.num_rows(), 1, "Should have exactly 1 row");

        // Validate response_status is 404
        let status_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::UInt16Array>()
            .expect("response_status should be UInt16Array");
        assert_eq!(
            status_col.value(0),
            404,
            "Invalid route should have response_status 404"
        );

        // Validate content contains the 404 JSON error response body
        let content_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("content should be string array");

        let content = content_col.value(0);
        // TVMaze returns JSON error: {"name":"Not Found","message":"Page not found.","code":0,"status":404,...}
        assert!(
            content.contains("Not Found"),
            "404 response should contain 'Not Found' in body"
        );
    }

    #[tokio::test]
    async fn test_integration_httpbin_500_server_error() {
        use datafusion::prelude::SessionContext;

        // httpbin.org provides endpoints that return specific HTTP status codes
        let url = Url::parse("https://httpbin.org").expect("valid URL");
        let provider = HttpTableProvider::new(url, Client::new(), "json".to_string(), false)
            .with_allowed_paths(vec!["/status/500".to_string()])
            .expect("allowed paths");

        let ctx = SessionContext::new();
        ctx.register_table("httpbin", Arc::new(provider))
            .expect("register table");

        // Query for a 500 status endpoint - should return a row with 500 status
        let df = ctx
            .sql("SELECT request_path, content, response_status FROM httpbin WHERE request_path = '/status/500'")
            .await
            .expect("query should succeed");

        let results = df.collect().await.expect("collect should succeed");
        assert!(!results.is_empty(), "Should have results even for 5xx");

        let batch = &results[0];
        assert_eq!(batch.num_rows(), 1, "Should have exactly 1 row");

        // Validate response_status is 500
        let status_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::UInt16Array>()
            .expect("response_status should be UInt16Array");
        assert_eq!(
            status_col.value(0),
            500,
            "Server error should have response_status 500"
        );

        // Validate content is empty (httpbin /status/500 returns empty body)
        let content_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("content should be string array");
        let content = content_col.value(0);
        assert!(
            content.is_empty(),
            "httpbin 500 response should have empty content body"
        );
    }

    #[tokio::test]
    async fn test_integration_tvmaze_multiple_shows() {
        use datafusion::prelude::SessionContext;

        let url = Url::parse("https://api.tvmaze.com").expect("valid URL");
        let provider = HttpTableProvider::new(url, Client::new(), "json".to_string(), false)
            .with_allowed_paths(vec![
                "/shows/1".to_string(),
                "/shows/2".to_string(),
                "/shows/82".to_string(),
            ])
            .expect("allowed paths");

        let ctx = SessionContext::new();
        ctx.register_table("shows", Arc::new(provider))
            .expect("register table");

        // Test OR filter for multiple paths
        let df = ctx
            .sql("SELECT request_path, content FROM shows WHERE request_path = '/shows/1' OR request_path = '/shows/2' OR request_path = '/shows/82'")
            .await
            .expect("query should succeed");

        let results = df.collect().await.expect("collect should succeed");
        assert!(!results.is_empty(), "Should have results");

        let total_rows: usize = results.iter().map(arrow_array::RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 3, "Should have exactly 3 rows for 3 shows");

        // Collect all show names to verify we got the right shows
        let mut show_names = Vec::new();
        let mut found_under_dome = false;
        let mut found_person_interest = false;
        let mut found_game_thrones = false;

        for batch in &results {
            let content_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .expect("content should be string array");

            for i in 0..batch.num_rows() {
                let content = content_col.value(i);
                if content.contains("Under the Dome") {
                    show_names.push("Under the Dome");
                    // Validate Under the Dome specific values
                    assert!(content.contains("\"id\"") && content.contains('1'));
                    assert!(content.contains("Drama"));
                    assert!(content.contains("Science-Fiction"));
                    found_under_dome = true;
                } else if content.contains("Person of Interest") {
                    show_names.push("Person of Interest");
                    // Validate Person of Interest specific values
                    assert!(content.contains("\"id\"") && content.contains('2'));
                    assert!(content.contains("Action"));
                    assert!(content.contains("Crime"));
                    found_person_interest = true;
                } else if content.contains("Game of Thrones") {
                    show_names.push("Game of Thrones");
                    // Validate Game of Thrones specific values
                    assert!(content.contains("\"id\"") && content.contains("82"));
                    assert!(content.contains("Fantasy"));
                    assert!(content.contains("Adventure"));
                    found_game_thrones = true;
                }
            }
        }

        assert_eq!(show_names.len(), 3, "Should have found all 3 shows");
        assert!(found_under_dome, "Should have found Under the Dome");
        assert!(
            found_person_interest,
            "Should have found Person of Interest"
        );
        assert!(found_game_thrones, "Should have found Game of Thrones");
    }
    #[tokio::test]
    async fn test_integration_tvmaze_projection() {
        use datafusion::prelude::SessionContext;

        let url = Url::parse("https://api.tvmaze.com").expect("valid URL");
        let provider = HttpTableProvider::new(url, Client::new(), "json".to_string(), false)
            .with_allowed_paths(vec!["/shows/1".to_string()])
            .expect("allowed paths");

        let ctx = SessionContext::new();
        ctx.register_table("shows", Arc::new(provider))
            .expect("register table");

        // Test with projection - only select content column
        let df = ctx
            .sql("SELECT content FROM shows WHERE request_path = '/shows/1'")
            .await
            .expect("query should succeed");

        let results = df.collect().await.expect("collect should succeed");
        assert!(!results.is_empty(), "Should have results");

        let batch = &results[0];
        assert_eq!(batch.num_columns(), 1, "Should only have content column");
        assert!(batch.num_rows() > 0, "Should have rows");

        // Verify the content is valid JSON with expected fields
        let content_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("content should be string array");

        let content = content_col.value(0);
        assert!(
            content.contains("Under the Dome"),
            "Should be Under the Dome"
        );
        assert!(content.contains("genres"), "Should contain genres field");

        // Validate specific values in the projection
        assert!(content.contains("Drama"), "Should contain Drama genre");
        assert!(
            content.contains("Science-Fiction"),
            "Should contain Science-Fiction genre"
        );
    }

    #[tokio::test]
    async fn test_integration_tvmaze_aggregation() {
        use datafusion::prelude::SessionContext;

        let url = Url::parse("https://api.tvmaze.com").expect("valid URL");
        let provider = HttpTableProvider::new(url, Client::new(), "json".to_string(), false)
            .with_allowed_paths(vec!["/shows/1".to_string(), "/shows/2".to_string()])
            .expect("allowed paths");

        let ctx = SessionContext::new();
        ctx.register_table("shows", Arc::new(provider))
            .expect("register table");

        // First validate that we get the actual content before testing aggregation
        let df_content = ctx
            .sql("SELECT content FROM shows WHERE request_path IN ('/shows/1', '/shows/2')")
            .await
            .expect("query should succeed");

        let content_results = df_content.collect().await.expect("collect should succeed");
        assert!(!content_results.is_empty(), "Should have content results");

        let mut found_under_dome = false;
        let mut found_person_interest = false;

        for batch in &content_results {
            let content_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .expect("content should be string array");

            for i in 0..batch.num_rows() {
                let content = content_col.value(i);
                if content.contains("Under the Dome") {
                    assert!(content.contains("Drama"));
                    found_under_dome = true;
                }
                if content.contains("Person of Interest") {
                    assert!(content.contains("Action"));
                    found_person_interest = true;
                }
            }
        }

        assert!(
            found_under_dome,
            "Should have found Under the Dome with Drama genre"
        );
        assert!(
            found_person_interest,
            "Should have found Person of Interest with Action genre"
        );

        // Test count aggregation
        let df = ctx
            .sql("SELECT COUNT(*) as total FROM shows WHERE request_path IN ('/shows/1', '/shows/2')")
            .await
            .expect("query should succeed");

        let results = df.collect().await.expect("collect should succeed");
        assert!(!results.is_empty(), "Should have results");

        let batch = &results[0];
        let count_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("count should be int64 array");

        let count = count_col.value(0);
        assert_eq!(count, 2, "Should have counted exactly 2 rows for 2 shows");
    }
}
