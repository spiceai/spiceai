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
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    common::{Constraint, Constraints, project_schema},
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

    #[snafu(display("HTTP client error ({status}): {message}"))]
    HttpClientError { status: u16, message: String },

    #[snafu(display(
        "All {max_retries} retry attempts failed for HTTP request to {url}. Check network connectivity and endpoint availability."
    ))]
    AllRetriesFailed { max_retries: usize, url: String },

    #[snafu(display("Invalid URL: {source}"))]
    InvalidUrl { source: url::ParseError },

    #[snafu(display("Arrow error: {source}"))]
    Arrow { source: ArrowError },

    #[snafu(display("DataFusion error: {source}"))]
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
}

impl HttpFetchResult {
    fn should_cache(&self) -> bool {
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
    allowed_paths: Option<HashSet<String>>,
    allow_query_filters: bool,
    max_query_length: usize,
    allow_body_filters: bool,
    max_body_bytes: usize,
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
            // Mark `request_path`, `request_query`, and `request_body` as primary key components
            constraints: Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![0, 1, 2])]),
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
        }
    }

    pub fn with_allowed_paths<I, S>(mut self, paths: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut normalized = HashSet::new();
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
            normalized.insert(value);
        }

        self.allowed_paths = if normalized.is_empty() {
            None
        } else {
            Some(normalized)
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

    #[must_use]
    pub fn base_table_schema() -> Schema {
        Schema::new(vec![
            Field::new("request_path", DataType::Utf8, false),
            Field::new("request_query", DataType::Utf8, true),
            Field::new("request_body", DataType::Utf8, true),
            Field::new("content", DataType::Utf8, false),
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

    /// Validates the HTTP endpoint by attempting a request to a non-existent path.
    /// This helps detect issues like DNS errors, connection problems, or invalid URLs
    /// early in the initialization process.
    pub async fn validate_endpoint(&self) -> Result<()> {
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

        tracing::debug!("Validating HTTP endpoint: {}", self.base_url);

        match self.client.get(test_url).send().await {
            Ok(response) => {
                let status = response.status();
                tracing::debug!(
                    "HTTP endpoint validation response: {} (status: {})",
                    self.base_url,
                    status
                );
                // Any response (including 404) means the endpoint is reachable
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
        result: HttpFetchResult,
    ) -> String {
        let cache_key = Self::get_cache_key(path, query, body);
        let content_arc = Arc::new(result.content);
        let cached_response = CachedResponse {
            content: Arc::clone(&content_arc),
            cached_at: SystemTime::now(),
            max_age: result.max_age,
            detected_format: Some(result.detected_format),
        };

        let mut cache_write = self.cache.write().await;
        cache_write.insert(cache_key, cached_response);

        Arc::unwrap_or_clone(content_arc)
    }

    async fn perform_request_with_retry(
        &self,
        url: Url,
        body: Option<&str>,
        path_label: &str,
    ) -> Result<HttpFetchResult> {
        let retry_strategy = self.retry_strategy.clone();
        let client = self.client.clone();
        let content_type = self.content_type.clone();
        let custom_headers = self.custom_headers.clone();
        let path_owned = path_label.to_string();
        let body_owned = body.map(ToOwned::to_owned);

        retry(retry_strategy, || {
            let client = client.clone();
            let url = url.clone();
            let headers = custom_headers.clone();
            let content_type = content_type.clone();
            let path_for_detection = path_owned.clone();
            let body_for_request = body_owned.clone();

            async move {
                let mut request_builder = if let Some(ref body_content) = body_for_request {
                    let mut req = client.post(url.clone());
                    let ct = content_type.as_deref().unwrap_or("application/json");
                    req = req.header("Content-Type", ct);
                    req.body(body_content.clone())
                } else {
                    client.get(url.clone())
                };

                for (name, value) in &headers {
                    request_builder = request_builder.header(name, value);
                }

                let response = request_builder.send().await.map_err(|e| {
                    tracing::debug!("HTTP request failed: {}", e);
                    RetryError::transient(Error::HttpRequest { source: e })
                })?;

                if let Err(err) = response.error_for_status_ref() {
                    if let Some(status) = err.status() {
                        let status_code = status.as_u16();
                        if (400..500).contains(&status_code) {
                            return Err(RetryError::permanent(Error::HttpClientError {
                                status: status_code,
                                message: format!(
                                    "{} for url ({})",
                                    status.canonical_reason().unwrap_or("Client Error"),
                                    url
                                ),
                            }));
                        }
                    }
                    tracing::debug!("HTTP request returned server error, will retry: {}", err);
                    return Err(RetryError::transient(Error::HttpRequest { source: err }));
                }

                let detected_format = Self::detect_file_format(&response, &path_for_detection);
                tracing::debug!(
                    "Detected file format from Content-Type header: {}",
                    detected_format
                );

                let cache_control_header = response
                    .headers()
                    .get(CACHE_CONTROL)
                    .and_then(|v| v.to_str().ok());
                let max_age = Self::parse_cache_control(cache_control_header);

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
                })
            }
        })
        .await
    }

    async fn fetch_and_cache(
        &self,
        path: &str,
        query: Option<&str>,
        body: Option<&str>,
    ) -> Result<String> {
        let url = self.build_request_url(path, query)?;
        let path_owned = path.to_string();
        let query_owned = query.map(ToOwned::to_owned);
        let body_owned = body.map(ToOwned::to_owned);

        let fetch_result = self
            .perform_request_with_retry(url, body_owned.as_deref(), &path_owned)
            .await?;

        if fetch_result.should_cache() {
            return Ok(self
                .cache_response(
                    &path_owned,
                    query_owned.as_deref(),
                    body_owned.as_deref(),
                    fetch_result,
                )
                .await);
        }

        Ok(fetch_result.content)
    }

    async fn get_content(
        &self,
        path: &str,
        query: Option<&str>,
        body: Option<&str>,
    ) -> Result<String> {
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
            return Ok((*cached_response.content).clone());
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
                if self.can_pushdown_filter(f) {
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
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        tracing::debug!("HTTP scan called with {} filters", filters.len());
        for (i, filter) in filters.iter().enumerate() {
            tracing::trace!("  Filter {}: {:?}", i, filter);
        }

        // Extract all (path, query, body) combinations that are allowed for this provider
        let partitions = self.extract_partitions(filters)?;

        tracing::debug!("Extracted {} partitions from filters", partitions.len());
        for (i, partition) in partitions.iter().enumerate() {
            tracing::debug!(
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
        )))
    }
}

#[derive(Clone)]
pub struct HttpExec {
    projected_schema: SchemaRef,
    provider: Arc<HttpTableProvider>,
    partitions: Vec<PartitionSpec>,
    properties: PlanProperties,
}

impl HttpExec {
    #[must_use]
    pub fn new(
        projected_schema: SchemaRef,
        provider: Arc<HttpTableProvider>,
        partitions: Vec<PartitionSpec>,
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
            properties,
        }
    }

    async fn fetch_and_create_batch(
        &self,
        provider: &HttpTableProvider,
        partition: usize,
    ) -> DataFusionResult<RecordBatch> {
        let (path, _query, _body) = &self.partitions[partition];

        // Use the filter path or empty string (base URL only)
        let path_val = path.as_deref().unwrap_or("");

        tracing::debug!(
            "HttpExec fetching partition {}: request_path={:?}",
            partition,
            path_val
        );

        // Fetch content with only the path, no query or body
        let content = provider
            .get_content(path_val, None, None)
            .await
            .map_err(DataFusionError::from)?;

        // Set path from partition, but leave query and body empty
        // DataFusion's FilterExec will filter based on these columns if needed
        let path_for_batch = path.as_deref().unwrap_or("");
        let query_for_batch = "";
        let body_for_batch = "";

        tracing::debug!(
            "Creating batch with request_path={:?}, content_len={}",
            path_for_batch,
            content.len()
        );

        // Parse content to determine how many rows we'll create
        let content_rows = Self::parse_content(&content);
        let num_rows = content_rows.len();

        if num_rows == 0 {
            tracing::warn!("No rows found in HTTP response for partition {}", partition);
            return Err(DataFusionError::Execution(
                "No rows found in HTTP response".to_string(),
            ));
        }

        // Create columns with the same number of rows
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
    fn parse_content(content: &str) -> Vec<String> {
        let trimmed = content.trim();

        // Try to parse as JSON
        if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(trimmed) {
            match json_value {
                serde_json::Value::Array(arr) => {
                    // JSON array: each element is a row
                    return arr.into_iter().map(|item| item.to_string()).collect();
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
        tracing::debug!(
            "HttpExec::execute called for partition {}, total partitions: {}",
            partition,
            self.partitions.len()
        );

        let exec = Arc::new(self.clone());
        let provider = Arc::clone(&self.provider);
        let schema = Arc::clone(&self.projected_schema);

        // Use futures::stream::once to create a stream from a single async operation
        let stream = futures::stream::once(async move {
            tracing::debug!("Fetching partition {}", partition);
            let batch = exec.fetch_and_create_batch(&provider, partition).await?;
            tracing::debug!(
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
        let mut accumulator = PartitionAccumulator::new();

        for filter in filters {
            self.extract_filter_values(filter, &mut accumulator)
                .map_err(DataFusionError::from)?;
        }

        let (paths, _queries, _bodies) = accumulator.finalize();

        // Create partitions only from paths, not from query/body combinations
        // Query and body filters will be applied by DataFusion's FilterExec
        // Paths are already deduplicated and sorted by the accumulator
        let partitions = paths
            .into_iter()
            .map(|path| {
                (
                    if path.is_empty() { None } else { Some(path) },
                    None, // No query in partition spec
                    None, // No body in partition spec
                )
            })
            .collect();

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
        match column_name {
            "request_path" => {
                let normalized = self.ensure_allowed_path(value)?;
                accumulator.record_path(normalized);
            }
            "request_query" => {
                let normalized = self.ensure_allowed_query(value)?;
                accumulator.record_query(normalized);
            }
            "request_body" => {
                let normalized = self.ensure_allowed_body(value)?;
                accumulator.record_body(normalized);
            }
            _ => {}
        }
        Ok(())
    }

    /// Check if a filter expression can be pushed down to HTTP requests
    fn can_pushdown_filter(&self, filter: &Expr) -> bool {
        match filter {
            // Simple equality on request_path, request_query, or request_body
            Expr::BinaryExpr(BinaryExpr { left, op, right: _ }) if *op == Operator::Eq => {
                if let Expr::Column(col) = left.as_ref() {
                    match col.name.as_str() {
                        "request_path" => self.allowed_paths.is_some(),
                        "request_query" => self.allow_query_filters,
                        "request_body" => self.allow_body_filters,
                        _ => false,
                    }
                } else {
                    false
                }
            }
            // IN list on request_path, request_query, or request_body
            Expr::InList(in_list) => {
                if let Expr::Column(col) = in_list.expr.as_ref() {
                    match col.name.as_str() {
                        "request_path" => self.allowed_paths.is_some(),
                        "request_query" => self.allow_query_filters,
                        "request_body" => self.allow_body_filters,
                        _ => false,
                    }
                } else {
                    false
                }
            }
            // OR/AND expressions - recursively check both sides
            Expr::BinaryExpr(BinaryExpr { left, op, right })
                if *op == Operator::Or || *op == Operator::And =>
            {
                self.can_pushdown_filter(left) && self.can_pushdown_filter(right)
            }
            _ => false,
        }
    }

    fn ensure_allowed_path(&self, raw: &str) -> Result<String> {
        if raw.is_empty() {
            return Err(Error::FilterRejected {
                message: "request_path filter cannot be empty".to_string(),
            });
        }
        if raw.len() > MAX_REQUEST_PATH_LENGTH {
            return Err(Error::FilterRejected {
                message: format!(
                    "request_path exceeds the maximum supported length of {MAX_REQUEST_PATH_LENGTH} characters"
                ),
            });
        }
        if !raw.starts_with('/') {
            return Err(Error::FilterRejected {
                message: "request_path filters must start with '/'".to_string(),
            });
        }
        if raw.contains("..") {
            return Err(Error::FilterRejected {
                message: "request_path cannot contain '..' segments".to_string(),
            });
        }

        let Some(allowed) = &self.allowed_paths else {
            return Err(Error::FilterRejected {
                message:
                    "request_path filters are disabled for this dataset. Configure allowed_request_paths to enable them."
                        .to_string(),
            });
        };

        if !allowed.contains(raw) {
            return Err(Error::FilterRejected {
                message: format!("request_path '{raw}' is not included in allowed_request_paths"),
            });
        }

        Ok(raw.to_string())
    }

    fn ensure_allowed_query(&self, raw: &str) -> Result<String> {
        if !self.allow_query_filters {
            return Err(Error::FilterRejected {
                message:
                    "request_query filters are disabled for this dataset. Enable allow_request_query_filters to use them.".to_string(),
            });
        }
        if raw.len() > self.max_query_length {
            return Err(Error::FilterRejected {
                message: format!(
                    "request_query exceeds the configured max length of {} characters",
                    self.max_query_length
                ),
            });
        }
        if raw.chars().any(char::is_control) {
            return Err(Error::FilterRejected {
                message: "request_query cannot contain control characters".to_string(),
            });
        }

        Ok(raw.strip_prefix('?').unwrap_or(raw).to_string())
    }

    fn ensure_allowed_body(&self, raw: &str) -> Result<String> {
        if !self.allow_body_filters {
            return Err(Error::FilterRejected {
                message:
                    "request_body filters are disabled for this dataset. Enable allow_request_body_filters to use them.".to_string(),
            });
        }
        if raw.len() > self.max_body_bytes {
            return Err(Error::FilterRejected {
                message: format!(
                    "request_body exceeds the configured max size of {} bytes",
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
            (Some("/singlesearch/shows".to_string()), None, None)
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
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0], (None, None, None));
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
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0], (Some("/api/users".to_string()), None, None));
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
                assert!(message.contains("allow_request_query_filters"));
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
                assert!(message.contains("allow_request_body_filters"));
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
                assert!(message.contains("max length"));
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
                assert!(message.contains("max size"));
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
    fn test_base_table_schema() {
        let schema = HttpTableProvider::base_table_schema();

        assert_eq!(schema.fields().len(), 4);
        assert_eq!(schema.field(0).name(), "request_path");
        assert_eq!(schema.field(1).name(), "request_query");
        assert_eq!(schema.field(2).name(), "request_body");
        assert_eq!(schema.field(3).name(), "content");
        assert_eq!(*schema.field(0).data_type(), DataType::Utf8);
        assert_eq!(*schema.field(1).data_type(), DataType::Utf8);
        assert_eq!(*schema.field(2).data_type(), DataType::Utf8);
        assert_eq!(*schema.field(3).data_type(), DataType::Utf8);
        assert!(!schema.field(0).is_nullable()); // request_path is not nullable
        assert!(schema.field(1).is_nullable()); // request_query is nullable
        assert!(schema.field(2).is_nullable()); // request_body is nullable
        assert!(!schema.field(3).is_nullable()); // content is not nullable
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
}
