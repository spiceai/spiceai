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
    logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
    scalar::ScalarValue,
};
use reqwest::{
    Client,
    header::{CACHE_CONTROL, HeaderMap},
};
use snafu::prelude::*;
use std::collections::HashMap;
use std::{
    any::Any,
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
        }
    }
}

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
        }
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

    async fn fetch_and_cache(
        &self,
        path: &str,
        query: Option<&str>,
        body: Option<&str>,
    ) -> Result<String> {
        let mut url = self.base_url.clone();

        // Append the path to the base URL's path (only if path is non-empty)
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

        let method_str = if body.is_some() { "POST" } else { "GET" };
        tracing::debug!("Fetching HTTP content from: {} using {}", url, method_str);

        // Use the common retry library with Fibonacci backoff
        let retry_strategy = self.retry_strategy.clone();
        let client = self.client.clone();
        let content_type = self.content_type.clone();
        let custom_headers = self.custom_headers.clone();
        let body_owned = body.map(String::from);
        let cache = Arc::clone(&self.cache);
        let path_owned = path.to_string();
        let query_owned = query.map(String::from);

        retry(retry_strategy, || async {
            // Build request based on whether body is present
            let mut request_builder = if let Some(ref body_content) = body_owned {
                let mut req = client.post(url.clone());

                // Set Content-Type header, defaulting to application/json if not specified
                let ct = content_type.as_deref().unwrap_or("application/json");
                req = req.header("Content-Type", ct);

                req.body(body_content.clone())
            } else {
                client.get(url.clone())
            };

            // Add custom headers
            for (name, value) in &custom_headers {
                request_builder = request_builder.header(name, value);
            }

            // Reqwest automatically handles compression (gzip, br/brotli, zstd, deflate)
            // It adds Accept-Encoding header and decompresses responses automatically

            let response = request_builder.send().await.map_err(|e| {
                tracing::debug!("HTTP request failed: {}", e);
                RetryError::transient(Error::HttpRequest { source: e })
            })?;

            // Check for HTTP errors and classify them appropriately
            if let Err(err) = response.error_for_status_ref() {
                if let Some(status) = err.status() {
                    let status_code = status.as_u16();
                    // 4xx errors are client errors (user's fault - bad query, wrong path, etc.)
                    // Don't retry these as they won't succeed
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
                // 5xx and other errors are server/network errors - retry these
                tracing::debug!("HTTP request returned server error, will retry: {}", err);
                return Err(RetryError::transient(Error::HttpRequest { source: err }));
            }

            // Success! Process the response
            // Try to detect file format from Content-Type header in response
            // This takes priority over the configured format
            let detected_format = Self::detect_file_format(&response, &path_owned);
            tracing::debug!(
                "Detected file format from Content-Type header: {}",
                detected_format
            );

            // Parse Cache-Control header
            let cache_control_header = response
                .headers()
                .get(CACHE_CONTROL)
                .and_then(|v| v.to_str().ok());

            let max_age = Self::parse_cache_control(cache_control_header);

            let content = response
                .text()
                .await
                .map_err(|e| RetryError::permanent(Error::HttpRequest { source: e }))?;

            // If we couldn't detect format from headers, try inferring from content
            let detected_format = if detected_format.is_empty() {
                let inferred = Self::infer_format_from_content(&content);
                tracing::debug!("Inferred file format from content: {}", inferred);
                inferred
            } else {
                detected_format
            };

            // Cache the response if max_age > 0
            if max_age.as_secs() > 0 {
                let cache_key =
                    Self::get_cache_key(&path_owned, query_owned.as_deref(), body_owned.as_deref());
                let content_arc = Arc::new(content);
                let cached_response = CachedResponse {
                    content: Arc::clone(&content_arc),
                    cached_at: SystemTime::now(),
                    max_age,
                    detected_format: Some(detected_format),
                };

                let mut cache_write = cache.write().await;
                cache_write.insert(cache_key, cached_response);

                // Return the content from the Arc to avoid cloning
                Ok(Arc::unwrap_or_clone(content_arc))
            } else {
                Ok(content)
            }
        })
        .await
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
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        tracing::debug!("HTTP scan called with {} filters", filters.len());
        for (i, filter) in filters.iter().enumerate() {
            tracing::trace!("  Filter {}: {:?}", i, filter);
        }

        // Extract all (path, query) pairs from filters (supporting IN/OR)
        let partitions = Self::extract_partitions(filters);

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
    partitions: Vec<(Option<String>, Option<String>, Option<String>)>,
    properties: PlanProperties,
}

impl HttpExec {
    #[must_use]
    pub fn new(
        projected_schema: SchemaRef,
        provider: Arc<HttpTableProvider>,
        partitions: Vec<(Option<String>, Option<String>, Option<String>)>,
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

        let content = provider
            .get_content(path_val, query_val, body_val)
            .await
            .map_err(DataFusionError::from)?;

        // The path, query, and body values in the batch MUST match the filter values exactly
        // so that DataFusion's FilterExec will keep these rows
        let path_for_batch = path.as_deref().unwrap_or("");
        let query_for_batch = query.as_deref().unwrap_or("");
        let body_for_batch = body.as_deref().unwrap_or("");

        tracing::debug!(
            "Creating batch with _path={:?}, _query={:?}, _body={:?}, content_len={}",
            path_for_batch,
            query_for_batch,
            body_for_batch,
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
    /// Extract all (path, query) pairs from filters, supporting =, IN, and OR expressions
    fn extract_partitions(
        filters: &[Expr],
    ) -> Vec<(Option<String>, Option<String>, Option<String>)> {
        // Extract path, query, and body values from filters
        let mut paths: Vec<String> = vec![];
        let mut queries: Vec<Option<String>> = vec![];
        let mut bodies: Vec<Option<String>> = vec![];
        let mut has_path_filter = false;
        let mut has_query_filter = false;
        let mut has_body_filter = false;

        for filter in filters {
            Self::extract_filter_values(
                filter,
                &mut paths,
                &mut queries,
                &mut bodies,
                &mut has_path_filter,
                &mut has_query_filter,
                &mut has_body_filter,
            );
        }

        // If no path filter, use empty path (will use base URL's path as-is)
        if !has_path_filter {
            paths.push(String::new());
        }

        // If no query filter, use None (no query string)
        if !has_query_filter {
            queries.push(None);
        }

        // If no body filter, use None (GET request)
        if !has_body_filter {
            bodies.push(None);
        }

        // Cross product of paths, queries, and bodies to create all partition combinations
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

        partitions
    }

    /// Recursively extract path, query, and body values from filter expressions
    fn extract_filter_values(
        filter: &Expr,
        paths: &mut Vec<String>,
        queries: &mut Vec<Option<String>>,
        bodies: &mut Vec<Option<String>>,
        has_path_filter: &mut bool,
        has_query_filter: &mut bool,
        has_body_filter: &mut bool,
    ) {
        match filter {
            // Handle equality: request_path = 'value', request_query = 'value', or request_body = 'value'
            Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::Eq => {
                if let Expr::Column(col) = left.as_ref()
                    && let Expr::Literal(ScalarValue::Utf8(Some(value)), _) = right.as_ref()
                {
                    match col.name.as_str() {
                        "request_path" => {
                            paths.push(value.clone());
                            *has_path_filter = true;
                        }
                        "request_query" => {
                            queries.push(Some(value.clone()));
                            *has_query_filter = true;
                        }
                        "request_body" => {
                            bodies.push(Some(value.clone()));
                            *has_body_filter = true;
                        }
                        _ => {}
                    }
                }
            }
            // Handle IN list: request_path IN (...), request_query IN (...), or request_body IN (...)
            Expr::InList(in_list) => {
                if let Expr::Column(col) = in_list.expr.as_ref() {
                    let column_name = col.name.as_str();
                    if matches!(
                        column_name,
                        "request_path" | "request_query" | "request_body"
                    ) {
                        for expr in &in_list.list {
                            if let Expr::Literal(ScalarValue::Utf8(Some(value)), _) = expr {
                                match column_name {
                                    "request_path" => {
                                        paths.push(value.clone());
                                        *has_path_filter = true;
                                    }
                                    "request_query" => {
                                        queries.push(Some(value.clone()));
                                        *has_query_filter = true;
                                    }
                                    "request_body" => {
                                        bodies.push(Some(value.clone()));
                                        *has_body_filter = true;
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
            // Handle OR: recursively extract from both sides
            Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::Or => {
                Self::extract_filter_values(
                    left,
                    paths,
                    queries,
                    bodies,
                    has_path_filter,
                    has_query_filter,
                    has_body_filter,
                );
                Self::extract_filter_values(
                    right,
                    paths,
                    queries,
                    bodies,
                    has_path_filter,
                    has_query_filter,
                    has_body_filter,
                );
            }
            // Handle AND: recursively extract from both sides
            Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::And => {
                Self::extract_filter_values(
                    left,
                    paths,
                    queries,
                    bodies,
                    has_path_filter,
                    has_query_filter,
                    has_body_filter,
                );
                Self::extract_filter_values(
                    right,
                    paths,
                    queries,
                    bodies,
                    has_path_filter,
                    has_query_filter,
                    has_body_filter,
                );
            }
            _ => {}
        }
    }

    /// Check if a filter expression can be pushed down to HTTP requests
    fn can_pushdown_filter(filter: &Expr) -> bool {
        match filter {
            // Simple equality on request_path, request_query, or request_body
            Expr::BinaryExpr(BinaryExpr { left, op, right: _ }) if *op == Operator::Eq => {
                if let Expr::Column(col) = left.as_ref() {
                    matches!(
                        col.name.as_str(),
                        "request_path" | "request_query" | "request_body"
                    )
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

    #[test]
    fn test_extract_partitions_with_path_and_query_filters() {
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

        let partitions = HttpTableProvider::extract_partitions(&filters);

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
        let filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("request_path"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("/api/data".to_string())),
                None,
            )),
        })];

        let partitions = HttpTableProvider::extract_partitions(&filters);

        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0], (Some("/api/data".to_string()), None, None));
    }

    #[test]
    fn test_extract_partitions_with_no_filters() {
        let filters = vec![];

        let partitions = HttpTableProvider::extract_partitions(&filters);

        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0], (None, None, None));
    }

    #[test]
    fn test_extract_partitions_multiple_paths() {
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

        let partitions = HttpTableProvider::extract_partitions(&filters);

        assert_eq!(partitions.len(), 2);
        assert!(partitions.contains(&(Some("/path1".to_string()), None, None)));
        assert!(partitions.contains(&(Some("/path2".to_string()), None, None)));
    }

    #[test]
    fn test_extract_partitions_with_in_list_path() {
        // Create filter: path IN ('/api/v1/users', '/api/v1/posts')
        let filters = vec![Expr::InList(InList::new(
            Box::new(Expr::Column(Column::from_name("request_path"))),
            vec![
                Expr::Literal(ScalarValue::Utf8(Some("/api/v1/users".to_string())), None),
                Expr::Literal(ScalarValue::Utf8(Some("/api/v1/posts".to_string())), None),
            ],
            false,
        ))];

        let partitions = HttpTableProvider::extract_partitions(&filters);

        assert_eq!(partitions.len(), 2);
        assert!(partitions.contains(&(Some("/api/v1/users".to_string()), None, None)));
        assert!(partitions.contains(&(Some("/api/v1/posts".to_string()), None, None)));
    }

    #[test]
    fn test_extract_partitions_with_in_list_query() {
        // Create filter: query IN ('limit=10', 'limit=20')
        let filters = vec![Expr::InList(InList::new(
            Box::new(Expr::Column(Column::from_name("request_query"))),
            vec![
                Expr::Literal(ScalarValue::Utf8(Some("limit=10".to_string())), None),
                Expr::Literal(ScalarValue::Utf8(Some("limit=20".to_string())), None),
            ],
            false,
        ))];

        let partitions = HttpTableProvider::extract_partitions(&filters);

        assert_eq!(partitions.len(), 2);
        assert!(partitions.contains(&(None, Some("limit=10".to_string()), None)));
        assert!(partitions.contains(&(None, Some("limit=20".to_string()), None)));
    }

    #[test]
    fn test_extract_partitions_with_or_expression() {
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

        let partitions = HttpTableProvider::extract_partitions(&filters);

        assert_eq!(partitions.len(), 2);
        assert!(partitions.contains(&(Some("/api/v1".to_string()), None, None)));
        assert!(partitions.contains(&(Some("/api/v2".to_string()), None, None)));
    }

    #[test]
    fn test_extract_partitions_with_combined_filters() {
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

        let partitions = HttpTableProvider::extract_partitions(&filters);

        // Should create cross product: 1 path * 2 queries = 2 partitions
        assert_eq!(partitions.len(), 2);
        assert!(partitions.contains(&(
            Some("/api/users".to_string()),
            Some("limit=10".to_string()),
            None
        )));
        assert!(partitions.contains(&(
            Some("/api/users".to_string()),
            Some("limit=20".to_string()),
            None
        )));
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
