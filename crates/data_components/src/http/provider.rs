/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

use super::json_nest::{HttpJsonNesting, decompose_json_row};
use crate::rate_limit::RateLimiter;
use arrow::{
    array::{ArrayRef, MapBuilder, MapFieldNames, RecordBatch, StringArray, StringBuilder},
    compute::cast,
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
    header::{AUTHORIZATION, CACHE_CONTROL, HeaderMap, HeaderName, HeaderValue},
};
use runtime_rate_control::{Permit, RateController};
use snafu::prelude::*;
use std::collections::{HashMap, HashSet, VecDeque, hash_map::DefaultHasher};
use std::{
    any::Any,
    borrow::ToOwned,
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::sync::RwLock;
use url::Url;
use util::{
    RetryError, format_datafusion_error, retry,
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

    #[snafu(display("HTTP request was rate limited: {message}"))]
    RateLimited { message: String },

    #[snafu(display(
        "All {max_retries} retry attempts failed for HTTP request to {url}. Check network connectivity and endpoint availability."
    ))]
    AllRetriesFailed { max_retries: usize, url: String },

    #[snafu(display("Invalid URL: {source}"))]
    InvalidUrl { source: url::ParseError },

    #[snafu(display("Failed to process HTTP response data: {source}"))]
    Arrow { source: ArrowError },

    #[snafu(display("Failed to execute HTTP query: {}", format_datafusion_error(source)))]
    DataFusion { source: DataFusionError },

    #[snafu(display("Filter rejected: {message}"))]
    FilterRejected { message: String },

    #[snafu(display("HTTP provider configuration error: {message}"))]
    Configuration { message: String },

    #[snafu(display("HTTP pagination error: {message}"))]
    Pagination { message: String },

    #[snafu(display("Failed to decompose HTTP response row into declared columns: {source}"))]
    JsonNesting { source: super::json_nest::Error },
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
            Error::RateLimited { message } => DataFusionError::External(Box::new(
                std::io::Error::other(format!("HTTP request was rate limited: {message}")),
            )),
            // All other errors are internal/external errors
            Error::HttpRequest { source } => DataFusionError::External(Box::new(source)),
            Error::InvalidUrl { source } => DataFusionError::External(Box::new(source)),
            Error::Arrow { source } => DataFusionError::ArrowError(Box::new(source), None),
            Error::DataFusion { source } => source,
            err @ Error::JsonNesting { .. } => DataFusionError::External(Box::new(err)),
            Error::FilterRejected { message } | Error::Configuration { message } => {
                DataFusionError::Plan(message)
            }
            Error::Pagination { message } => {
                DataFusionError::External(Box::new(std::io::Error::other(message)))
            }
        }
    }
}

pub const DEFAULT_MAX_QUERY_LENGTH: usize = 1024;
pub const DEFAULT_MAX_BODY_BYTES: usize = 16 * 1024; // 16 KiB
pub const DEFAULT_MAX_HEADERS_LENGTH: usize = 16 * 1024; // 16 KiB
pub const DEFAULT_PAGINATION_MAX_PAGES: usize = 100;
const MAX_REQUEST_PATH_LENGTH: usize = 1024;
const PAGINATION_REPEAT_DETECTION_WINDOW: usize = 1024;
pub type PartitionSpec = (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

/// Configuration for paginated HTTP API requests.
///
/// Supports three modes:
/// - **URL mode**: The response body (via `next_pointer`) or HTTP `Link` header contains
///   the full URL for the next page.
/// - **Token mode**: The response contains a cursor/token (via `next_pointer`) that is
///   passed as a query parameter (specified by `token_param`) in the next request.
/// - **Query-parameter mode**: The client drives pagination by expanding a template
///   (`query_params`) with `{offset}`, `{limit}`, and `{page}` variables, stopping
///   when a page returns fewer rows than `page_size`.
#[derive(Clone, Debug)]
pub struct PaginationConfig {
    /// JSON pointer (RFC 6901) to the next page URL or cursor in the response body.
    /// Example: `/next`, `/pagination/cursor`, `/links/next`
    pub next_pointer: Option<String>,

    /// Use the HTTP `Link` header with `rel="next"` for pagination. Default: `true`.
    /// Set to `false` to disable Link header auto-detection.
    pub use_link_header: bool,

    /// When set, the value from `next_pointer` is treated as a cursor/token
    /// and passed as this query parameter name in subsequent requests.
    /// When not set, the value is treated as a full URL.
    pub token_param: Option<String>,

    /// JSON pointer (RFC 6901) to the data array in each page's response.
    /// Example: `/data`, `/results`, `/items`
    /// When set, only the array at this path is returned as data rows.
    pub data_pointer: Option<String>,

    /// Maximum number of pages to fetch. Default: 100. `None` disables the limit.
    pub max_pages: Option<usize>,

    /// When `true`, if the data at `data_pointer` (or the top-level response) is a JSON
    /// object/map, extract its values as rows instead of treating it as a single row.
    pub data_map_to_array: bool,

    /// Query parameter template for client-driven pagination.
    /// Supports `{offset}`, `{limit}`, and `{page}` variables.
    /// Example: `"offset={offset}&limit={limit}"`
    /// Requires `page_size` to be set.
    pub query_params: Option<String>,

    /// Number of items per page for query-parameter pagination.
    /// Used to expand `{limit}` in `query_params` and to detect the last page
    /// (fewer results than `page_size` means done).
    pub page_size: Option<usize>,
}

impl Default for PaginationConfig {
    fn default() -> Self {
        Self {
            next_pointer: None,
            use_link_header: true,
            token_param: None,
            data_pointer: None,
            max_pages: Some(DEFAULT_PAGINATION_MAX_PAGES),
            data_map_to_array: false,
            query_params: None,
            page_size: None,
        }
    }
}

#[derive(Clone)]
struct CachedResponse {
    content: Arc<String>,
    cached_at: SystemTime,
    max_age: Duration,
    detected_format: Option<String>,
    response_date: Option<SystemTime>,
    response_status: u16,
    response_headers: Arc<Vec<(String, String)>>,
}

impl CachedResponse {
    fn is_fresh(&self) -> bool {
        self.cached_at
            .elapsed()
            .ok()
            .is_some_and(|elapsed| elapsed < self.max_age)
    }
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
enum RequestFilterKind {
    Path,
    Query,
    Body,
    Headers,
}

#[derive(Default)]
struct PartitionAccumulator {
    paths: HashSet<String>,
    queries: Vec<Option<String>>,
    bodies: Vec<Option<String>>,
    headers: Vec<Option<String>>,
    seen_filters: HashSet<RequestFilterKind>,
}

struct PartitionValues {
    paths: Vec<String>,
    queries: Vec<Option<String>>,
    bodies: Vec<Option<String>>,
    headers: Vec<Option<String>>,
}

impl PartitionAccumulator {
    fn new() -> Self {
        Self::default()
    }

    fn has_filter(&self, kind: RequestFilterKind) -> bool {
        self.seen_filters.contains(&kind)
    }

    fn record_path(&mut self, value: String) {
        self.paths.insert(value);
        self.seen_filters.insert(RequestFilterKind::Path);
    }

    fn record_query(&mut self, value: String) {
        let entry = Some(value);
        if !self.queries.contains(&entry) {
            self.queries.push(entry);
        }
        self.seen_filters.insert(RequestFilterKind::Query);
    }

    fn record_body(&mut self, value: String) {
        let entry = Some(value);
        if !self.bodies.contains(&entry) {
            self.bodies.push(entry);
        }
        self.seen_filters.insert(RequestFilterKind::Body);
    }

    fn record_headers(&mut self, value: String) {
        let entry = Some(value);
        if !self.headers.contains(&entry) {
            self.headers.push(entry);
        }
        self.seen_filters.insert(RequestFilterKind::Headers);
    }

    fn finalize(mut self) -> PartitionValues {
        let has_path_filter = self.has_filter(RequestFilterKind::Path);
        let has_query_filter = self.has_filter(RequestFilterKind::Query);
        let has_body_filter = self.has_filter(RequestFilterKind::Body);
        let has_header_filter = self.has_filter(RequestFilterKind::Headers);

        let mut paths: Vec<String> = if has_path_filter {
            self.paths.into_iter().collect()
        } else {
            vec![String::new()]
        };
        // Sort paths for deterministic ordering
        paths.sort();

        if !has_query_filter {
            self.queries.push(None);
        }
        if !has_body_filter {
            self.bodies.push(None);
        }
        if !has_header_filter {
            self.headers.push(None);
        }
        PartitionValues {
            paths,
            queries: self.queries,
            bodies: self.bodies,
            headers: self.headers,
        }
    }
}

#[derive(Clone)]
struct RequestFilterOptions {
    enabled_filters: HashSet<RequestFilterKind>,
    max_query_length: usize,
    max_body_bytes: usize,
    max_headers_length: usize,
    allowed_headers: HashSet<HeaderName>,
}

impl Default for RequestFilterOptions {
    fn default() -> Self {
        Self {
            enabled_filters: HashSet::new(),
            max_query_length: DEFAULT_MAX_QUERY_LENGTH,
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
            max_headers_length: DEFAULT_MAX_HEADERS_LENGTH,
            allowed_headers: HashSet::new(),
        }
    }
}

impl RequestFilterOptions {
    fn enable(&mut self, kind: RequestFilterKind) {
        self.enabled_filters.insert(kind);
    }

    fn is_enabled(&self, kind: RequestFilterKind) -> bool {
        self.enabled_filters.contains(&kind)
    }
}

struct HttpFetchResult {
    content: String,
    max_age: Duration,
    detected_format: String,
    response_date: Option<SystemTime>,
    response_status: u16,
    response_headers: Vec<(String, String)>,
}

impl HttpFetchResult {
    fn should_cache(&self) -> bool {
        // We don't explicitly disable caching for 5xx responses because well-behaved servers
        // should return Cache-Control: no-cache or max-age=0 for transient error responses.
        // This keeps the caching logic simple and respects server-specified cache directives.
        self.max_age.as_secs() > 0
    }
}

enum CacheWriteMode {
    Enabled,
    Disabled,
}

#[derive(Clone, Eq, Hash, PartialEq)]
struct CacheKey {
    path: String,
    query: Option<String>,
    body: Option<String>,
    request_headers: Option<String>,
}

impl CacheKey {
    fn new(
        path: &str,
        query: Option<&str>,
        body: Option<&str>,
        request_headers: Option<&str>,
    ) -> Self {
        Self {
            path: path.to_string(),
            query: query.map(ToString::to_string),
            body: body.map(ToString::to_string),
            request_headers: request_headers.map(ToString::to_string),
        }
    }

    fn redacted_label(&self) -> String {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        format!("http-cache-key:{:016x}", hasher.finish())
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
    cache: Arc<RwLock<HashMap<CacheKey, CachedResponse>>>,
    acceleration_enabled: bool,
    retry_strategy: RetryBackoff,
    content_type: Option<String>,
    custom_headers: HeaderMap,
    allowed_paths: Option<(GlobSet, Vec<String>)>,
    request_filter_options: RequestFilterOptions,
    max_request_partitions: Option<usize>,
    health_probe: Option<String>,
    pagination: Option<PaginationConfig>,
    auth: Option<Arc<dyn super::auth::HttpAuthenticator>>,
    rate_limiter: Option<Arc<dyn RateLimiter>>,
    rate_controller: Option<Arc<RateController>>,
    /// When set, JSON response rows are decomposed into the declared
    /// static columns plus a catch-all JSON column. Schema is replaced
    /// with the user-declared columns (all `Utf8`).
    json_nesting: Option<HttpJsonNesting>,
}

impl std::fmt::Debug for HttpTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpTableProvider")
            .field("base_url", &self.base_url)
            .field("file_format", &self.file_format)
            .field("acceleration_enabled", &self.acceleration_enabled)
            .field("pagination", &self.pagination)
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
            request_filter_options: RequestFilterOptions::default(),
            max_request_partitions: None,
            health_probe: None,
            pagination: None,
            auth: None,
            rate_limiter: None,
            rate_controller: None,
            json_nesting: None,
        }
    }

    #[must_use]
    pub fn with_rate_limiter(mut self, rate_limiter: Option<Arc<dyn RateLimiter>>) -> Self {
        self.rate_limiter = rate_limiter;
        self
    }

    #[must_use]
    pub fn with_rate_controller(mut self, rate_controller: Option<Arc<RateController>>) -> Self {
        self.rate_controller = rate_controller;
        self
    }

    /// Configure JSON schema decomposition. Replaces the provider's
    /// schema with one built from the user-declared columns. Body-derived
    /// columns are typed as `Utf8` (nullable); columns declared with
    /// names matching [`Self::base_table_schema`] fields are passed
    /// through with their original type so queries can reference HTTP
    /// metadata (e.g. filter on `request_path` for direct fetches).
    /// Each scanned JSON response row is decomposed at query time via
    /// [`decompose_json_row`].
    ///
    /// # Panics
    ///
    /// Panics if `nesting.metadata_fields` contains a name that is not
    /// a column in [`Self::base_table_schema`]. Callers (notably
    /// `parse_http_json_nesting` in the HTTPS data connector) are
    /// responsible for validating this invariant.
    #[must_use]
    /// Replace the provider schema with a caller-supplied JSON-nesting
    /// schema. The schema's field order MUST equal `nesting.column_order`,
    /// and it MUST contain a field named `nesting.json_field_name`. Field
    /// types are taken from the supplied schema (typically the catch-all
    /// JSON column is `Utf8`; other static fields use whatever types the
    /// caller declared, defaulting to `Utf8` when no type was specified).
    /// HTTP metadata fields (see [`HttpJsonNesting::metadata_fields`]) MUST
    /// be present in `schema` with the types from
    /// [`Self::base_table_schema`]; the caller is responsible for
    /// resolving them.
    pub fn with_json_nesting(mut self, nesting: HttpJsonNesting, schema: SchemaRef) -> Self {
        self.schema = schema;
        self.json_nesting = Some(nesting);
        self
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
        self.request_filter_options.enable(RequestFilterKind::Query);
        self.request_filter_options.max_query_length = max_length.min(DEFAULT_MAX_QUERY_LENGTH * 4);
        self
    }

    #[must_use]
    pub fn enable_body_filters(mut self, max_bytes: usize) -> Self {
        self.request_filter_options.enable(RequestFilterKind::Body);
        self.request_filter_options.max_body_bytes = max_bytes.min(DEFAULT_MAX_BODY_BYTES * 4);
        self
    }

    pub fn enable_header_filters<I, S>(mut self, max_length: usize, header_names: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut allowed_headers = HashSet::new();
        for header_name in header_names {
            let raw = header_name.as_ref().trim();
            ensure!(
                !raw.is_empty(),
                ConfigurationSnafu {
                    message: "request_header_allowlist entries cannot be empty".to_string()
                }
            );
            let parsed = HeaderName::try_from(raw).map_err(|e| Error::Configuration {
                message: format!("Invalid request_header_allowlist entry '{raw}': {e}"),
            })?;
            ensure!(
                !(self.auth.is_some() && parsed == AUTHORIZATION),
                ConfigurationSnafu {
                    message: "request_header_allowlist cannot include 'authorization' when HTTP authentication is configured. Remove 'authorization' from request_header_allowlist or disable HTTP authentication.".to_string()
                }
            );
            allowed_headers.insert(parsed);
        }

        ensure!(
            !allowed_headers.is_empty(),
            ConfigurationSnafu {
                message: "request_header_filters requires request_header_allowlist to contain at least one header name".to_string()
            }
        );

        self.request_filter_options
            .enable(RequestFilterKind::Headers);
        self.request_filter_options.max_headers_length =
            max_length.min(DEFAULT_MAX_HEADERS_LENGTH * 4);
        self.request_filter_options.allowed_headers = allowed_headers;
        Ok(self)
    }

    #[must_use]
    pub fn max_request_partitions(&self) -> Option<usize> {
        self.max_request_partitions
    }

    #[must_use]
    pub fn with_max_request_partitions(mut self, max_request_partitions: Option<usize>) -> Self {
        self.max_request_partitions = max_request_partitions;
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

    /// Read-only access to the currently configured custom headers.
    #[must_use]
    pub fn custom_headers(&self) -> &HeaderMap {
        &self.custom_headers
    }

    /// Attach an [`HttpAuthenticator`](super::auth::HttpAuthenticator) that decorates
    /// every outgoing data request (e.g. to apply a bearer token refreshed in the
    /// background by [`RefreshTokenAuth`](super::auth::RefreshTokenAuth)).
    #[must_use]
    pub fn with_auth(mut self, auth: Arc<dyn super::auth::HttpAuthenticator>) -> Self {
        self.auth = Some(auth);
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

    /// Configure pagination for this HTTP table provider.
    ///
    /// At least one of `next_pointer`, `use_link_header`, or `query_params` must be set.
    pub fn with_pagination(mut self, config: PaginationConfig) -> Result<Self> {
        if let Some(ref template) = config.query_params {
            // Query-param pagination mode
            if config.page_size.is_none() || config.page_size == Some(0) {
                return Err(Error::Configuration {
                    message:
                        "pagination_query_params requires pagination_page_size to be set (and > 0)."
                            .to_string(),
                });
            }
            if config.next_pointer.is_some() || config.token_param.is_some() {
                return Err(Error::Configuration {
                    message: "pagination_query_params is mutually exclusive with pagination_next_pointer and pagination_token_param.".to_string(),
                });
            }
            if !template.contains("{offset}") && !template.contains("{page}") {
                return Err(Error::Configuration {
                    message: "pagination_query_params must contain at least one pagination variable ({offset} or {page}) to advance between pages.".to_string(),
                });
            }
        } else {
            if config.page_size.is_some() {
                return Err(Error::Configuration {
                    message:
                        "pagination_page_size requires pagination_query_params to be configured."
                            .to_string(),
                });
            }
            if config.next_pointer.is_none() && !config.use_link_header {
                return Err(Error::Configuration {
                    message: "Pagination requires either 'pagination_next_pointer', 'pagination_link_header', or 'pagination_query_params' to be configured.".to_string(),
                });
            } else if config.token_param.is_some() && config.next_pointer.is_none() {
                return Err(Error::Configuration {
                    message: "Pagination 'pagination_token_param' requires 'pagination_next_pointer' to be configured.".to_string(),
                });
            }
        }
        if let Some(max_pages) = config.max_pages {
            ensure!(
                max_pages > 0,
                ConfigurationSnafu {
                    message: "pagination_max_pages must be greater than 0".to_string()
                }
            );
        }
        if let Some(ref pointer) = config.next_pointer {
            ensure!(
                pointer.starts_with('/'),
                ConfigurationSnafu {
                    message: format!(
                        "pagination_next_pointer must be a valid JSON Pointer starting with '/': got '{pointer}'"
                    )
                }
            );
        }
        if let Some(ref pointer) = config.data_pointer {
            ensure!(
                pointer.starts_with('/'),
                ConfigurationSnafu {
                    message: format!(
                        "pagination_data_pointer must be a valid JSON Pointer starting with '/': got '{pointer}'"
                    )
                }
            );
        }
        self.pagination = Some(config);
        Ok(self)
    }

    #[must_use]
    pub fn is_paginated(&self) -> bool {
        self.pagination.is_some()
    }

    #[must_use]
    pub fn base_table_schema() -> Schema {
        Schema::new(vec![
            Field::new("request_path", DataType::Utf8, false),
            Field::new("request_query", DataType::Utf8, true),
            Field::new("request_body", DataType::Utf8, true),
            Field::new("request_headers", DataType::Utf8, true),
            Field::new("content", DataType::Utf8, false),
            Field::new("response_status", DataType::UInt16, false),
            Field::new(
                "response_headers",
                DataType::Map(
                    Arc::new(Field::new_struct(
                        "entries",
                        vec![
                            Arc::new(Field::new("keys", DataType::Utf8, false)),
                            Arc::new(Field::new("values", DataType::Utf8, true)),
                        ],
                        false,
                    )),
                    false,
                ),
                true,
            ),
            Field::new(
                "fetched_at",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                true,
            ),
        ])
    }

    /// Extract path and query from filters
    fn get_cache_key(
        path: &str,
        query: Option<&str>,
        body: Option<&str>,
        request_headers: Option<&str>,
    ) -> CacheKey {
        CacheKey::new(path, query, body, request_headers)
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
            use rand::RngExt;
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

        let _rate_control_permit = self.acquire_rate_control_permit().await?;

        match self.client.get(test_url.clone()).send().await {
            Ok(response) => {
                self.update_rate_limiter_from_headers(response.headers())
                    .await;
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

    async fn acquire_rate_control_permit(&self) -> Result<Option<Permit>> {
        if let Some(rate_limiter) = &self.rate_limiter {
            rate_limiter
                .check_rate_limit()
                .await
                .map_err(|e| Error::RateLimited {
                    message: e.to_string(),
                })?;
        }

        if let Some(rate_controller) = &self.rate_controller {
            return rate_controller
                .acquire()
                .await
                .map(Some)
                .map_err(|e| Error::RateLimited {
                    message: e.to_string(),
                });
        }

        Ok(None)
    }

    async fn update_rate_limiter_from_headers(&self, headers: &HeaderMap) {
        if let Some(rate_limiter) = &self.rate_limiter {
            rate_limiter.update_from_headers(headers).await;
        }
    }

    async fn cache_response(
        &self,
        path: &str,
        query: Option<&str>,
        body: Option<&str>,
        request_headers: Option<&str>,
        result: &HttpFetchResult,
    ) {
        let cache_key = Self::get_cache_key(path, query, body, request_headers);
        let cached_response = CachedResponse {
            content: Arc::new(result.content.clone()),
            cached_at: SystemTime::now(),
            max_age: result.max_age,
            detected_format: Some(result.detected_format.clone()),
            response_date: result.response_date,
            response_status: result.response_status,
            response_headers: Arc::new(result.response_headers.clone()),
        };

        let mut cache_write = self.cache.write().await;
        cache_write.insert(cache_key, cached_response);
    }

    async fn perform_request_with_retry(
        &self,
        url: Url,
        body: Option<&str>,
        request_headers: Option<&HeaderMap>,
        path_label: &str,
    ) -> Result<HttpFetchResult> {
        let retry_strategy = self.retry_strategy.clone();
        let this = self.clone();
        let url_clone = url.clone();
        let body_owned = body.map(ToOwned::to_owned);
        let request_headers_owned = request_headers.cloned();
        let path_owned = path_label.to_string();

        let result = retry(retry_strategy, || {
            let this = this.clone();
            let url = url_clone.clone();
            let body = body_owned.clone();
            let request_headers = request_headers_owned.clone();
            let path = path_owned.clone();

            async move {
                this.perform_single_request(
                    &url,
                    body.as_deref(),
                    request_headers.as_ref(),
                    &path,
                    false,
                )
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
            self.perform_single_request(&url, body, request_headers, path_label, true)
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
        request_headers: Option<&HeaderMap>,
        path_label: &str,
        accept_retryable: bool,
    ) -> std::result::Result<HttpFetchResult, RetryError<Error>> {
        let _rate_control_permit = self
            .acquire_rate_control_permit()
            .await
            .map_err(RetryError::transient)?;

        let mut request_builder = if let Some(body_content) = body {
            let mut req = self.client.post(url.clone());
            let ct = self.content_type.as_deref().unwrap_or("application/json");
            req = req.header("Content-Type", ct);
            req.body(body_content.to_owned())
        } else {
            self.client.get(url.clone())
        };

        if let Some(request_headers) = request_headers {
            let mut merged_headers = self.custom_headers.clone();
            for (name, value) in request_headers {
                merged_headers.insert(name.clone(), value.clone());
            }
            for (name, value) in &merged_headers {
                request_builder = request_builder.header(name, value);
            }
        } else {
            for (name, value) in &self.custom_headers {
                request_builder = request_builder.header(name, value);
            }
        }

        if let Some(auth) = self.auth.as_ref() {
            request_builder = auth.apply(request_builder);
        }

        let response = request_builder.send().await.map_err(|e| {
            tracing::debug!("HTTP request failed: {e}");
            RetryError::transient(Error::HttpRequest { source: e })
        })?;

        let status_code = response.status().as_u16();
        let response_headers = response.headers().clone();
        self.update_rate_limiter_from_headers(&response_headers)
            .await;

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

        // Capture response headers before consuming the response body
        let response_headers: Vec<(String, String)> = response
            .headers()
            .iter()
            .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();

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
            response_headers,
        })
    }

    async fn fetch_response(
        &self,
        path: &str,
        query: Option<&str>,
        body: Option<&str>,
        request_headers: Option<&str>,
        cache_write_mode: CacheWriteMode,
    ) -> Result<HttpFetchResult> {
        let url = self.build_request_url(path, query)?;
        let path_owned = path.to_string();
        let query_owned = query.map(ToOwned::to_owned);
        let body_owned = body.map(ToOwned::to_owned);
        let request_headers_owned = request_headers.map(ToOwned::to_owned);
        let parsed_request_headers = request_headers_owned
            .as_deref()
            .map(|headers| self.parse_request_headers(headers))
            .transpose()?;

        let fetch_result = self
            .perform_request_with_retry(
                url,
                body_owned.as_deref(),
                parsed_request_headers.as_ref(),
                &path_owned,
            )
            .await?;

        if matches!(cache_write_mode, CacheWriteMode::Enabled) && fetch_result.should_cache() {
            self.cache_response(
                &path_owned,
                query_owned.as_deref(),
                body_owned.as_deref(),
                request_headers_owned.as_deref(),
                &fetch_result,
            )
            .await;
        }

        Ok(fetch_result)
    }

    async fn fetch_and_cache(
        &self,
        path: &str,
        query: Option<&str>,
        body: Option<&str>,
        request_headers: Option<&str>,
    ) -> Result<HttpFetchResult> {
        self.fetch_response(path, query, body, request_headers, CacheWriteMode::Enabled)
            .await
    }

    async fn get_response(
        &self,
        path: &str,
        query: Option<&str>,
        body: Option<&str>,
        request_headers: Option<&str>,
    ) -> Result<HttpFetchResult> {
        // When acceleration is enabled, skip HTTP-level caching - the acceleration layer handles it.
        if self.acceleration_enabled {
            return self
                .fetch_response(path, query, body, request_headers, CacheWriteMode::Disabled)
                .await;
        }

        let cache_key = Self::get_cache_key(path, query, body, request_headers);

        // Try to get from cache
        let cached = {
            let cache = self.cache.read().await;
            cache.get(&cache_key).cloned()
        };

        if let Some(cached_response) = cached
            && cached_response.is_fresh()
        {
            if tracing::enabled!(tracing::Level::DEBUG) {
                let cache_key_label = cache_key.redacted_label();
                if let Some(ref format) = cached_response.detected_format {
                    tracing::debug!(
                        "Returning fresh cached content for {} (detected format: {})",
                        cache_key_label,
                        format
                    );
                } else {
                    tracing::debug!("Returning fresh cached content for {}", cache_key_label);
                }
            }
            return Ok(HttpFetchResult {
                content: (*cached_response.content).clone(),
                max_age: cached_response.max_age,
                detected_format: cached_response.detected_format.clone().unwrap_or_default(),
                response_date: cached_response.response_date,
                response_status: cached_response.response_status,
                response_headers: (*cached_response.response_headers).clone(),
            });
        }

        // Fetch fresh content
        self.fetch_and_cache(path, query, body, request_headers)
            .await
    }

    fn get_projected_schema(
        schema: &SchemaRef,
        projection: Option<&Vec<usize>>,
    ) -> DataFusionResult<SchemaRef> {
        let mut projected_schema = project_schema(schema, projection)?;
        if projected_schema.fields.is_empty() {
            // Fall back to a single column so downstream operators
            // (e.g. COUNT(*)) have something to scan. Prefer `content`
            // for the default schema; otherwise use the first field
            // (e.g. when `with_json_nesting` has replaced the schema).
            let idx = schema.index_of("content").unwrap_or(0);
            if !schema.fields.is_empty() {
                projected_schema = SchemaRef::from(schema.project(&[idx])?);
            }
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
    properties: Arc<PlanProperties>,
    /// When `true`, the partitions are a template that will be expanded
    /// at runtime by `HttpWithDeferredParamsExec`. Display shows `partitions=deferred`.
    deferred_partitions: bool,
}

impl HttpExec {
    /// Returns the provider used by this exec.
    #[must_use]
    pub fn provider(&self) -> &Arc<HttpTableProvider> {
        &self.provider
    }

    /// Returns the maximum number of request partitions allowed, if configured.
    #[must_use]
    pub fn max_request_partitions(&self) -> Option<usize> {
        self.provider.max_request_partitions()
    }

    /// Returns the partition specs.
    #[must_use]
    pub fn partitions(&self) -> &[PartitionSpec] {
        &self.partitions
    }

    /// Returns the limit.
    #[must_use]
    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    /// Returns the projected schema.
    #[must_use]
    pub fn projected_schema(&self) -> &SchemaRef {
        &self.projected_schema
    }

    #[must_use]
    pub fn new(
        projected_schema: SchemaRef,
        provider: Arc<HttpTableProvider>,
        partitions: Vec<PartitionSpec>,
        limit: Option<usize>,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&projected_schema)),
            Partitioning::UnknownPartitioning(partitions.len()),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            projected_schema,
            provider,
            partitions,
            limit,
            properties,
            deferred_partitions: false,
        }
    }

    /// Mark this `HttpExec` as having dynamic partitions that will be
    /// expanded at runtime. Affects EXPLAIN display only.
    #[must_use]
    pub fn with_deferred_partitions(mut self) -> Self {
        self.deferred_partitions = true;
        self
    }

    /// Create a new `HttpExec` whose partitions are the cross-product of the
    /// current partitions and the given `values`, injected into the column
    /// identified by `col_name` (`request_path`, `request_query`,
    /// `request_body`, or `request_headers`).
    ///
    /// Returns an error if the resulting partition count would exceed
    /// `max_request_partitions`.
    pub fn with_expanded_params(
        &self,
        col_name: &str,
        values: &[String],
    ) -> DataFusionResult<Self> {
        let existing = &self.partitions;
        let new_count = existing.len() * values.len();

        if let Some(max) = self.max_request_partitions()
            && new_count > max
        {
            return Err(DataFusionError::Plan(format!(
                "HttpExec: expanding params would create {new_count} partitions (existing {} x {} values), which exceeds max_request_partitions={max}. Reduce the number of dynamic values or increase max_request_partitions.",
                existing.len(),
                values.len(),
            )));
        }

        let mut new_partitions = Vec::with_capacity(new_count);

        for partition in existing {
            for value in values {
                let mut p = partition.clone();
                match col_name {
                    "request_path" => {
                        p.0 = Some(self.provider.ensure_allowed_path(value)?);
                    }
                    "request_query" => {
                        p.1 = Some(self.provider.ensure_allowed_query(value)?);
                    }
                    "request_body" => {
                        p.2 = Some(self.provider.ensure_allowed_body(value)?);
                    }
                    "request_headers" => {
                        p.3 = Some(self.provider.ensure_allowed_headers(value)?);
                    }
                    other => {
                        return Err(DataFusionError::Internal(format!(
                            "HttpExec::with_expanded_params: unsupported column '{other}'. Expected one of: request_path, request_query, request_body, request_headers"
                        )));
                    }
                }
                new_partitions.push(p);
            }
        }

        tracing::debug!(
            "HttpExec::with_expanded_params: replacing partitions with {} (was {}) for column '{col_name}'",
            new_partitions.len(),
            existing.len(),
        );

        Ok(Self::new(
            Arc::clone(&self.projected_schema),
            Arc::clone(&self.provider),
            new_partitions,
            self.limit,
        ))
    }

    async fn fetch_and_create_batch(
        &self,
        provider: &HttpTableProvider,
        partition: usize,
    ) -> DataFusionResult<RecordBatch> {
        let (path, query, body, request_headers) = &self.partitions[partition];

        // Use the filter path or empty string (base URL only)
        let path_val = path.as_deref().unwrap_or("");
        let query_val = query.as_deref();
        let body_val = body.as_deref();
        let request_headers_val = request_headers.as_deref();

        tracing::debug!(
            "HttpExec fetching partition {}: request_path={:?}, request_query={:?}, request_body={:?}, request_headers_present={}",
            partition,
            path_val,
            query_val,
            body_val,
            request_headers_val.is_some()
        );

        // Fetch content with path, query, and body
        let result = provider
            .get_response(path_val, query_val, body_val, request_headers_val)
            .await
            .map_err(DataFusionError::from)?;

        // Parse content to determine how many rows we'll create
        let map_to_array = provider
            .pagination
            .as_ref()
            .is_some_and(|p| p.data_map_to_array);
        let content_rows =
            parse_content_with_map_to_array(&result.content, self.limit, map_to_array);

        self.create_batch_from_rows(
            path.as_deref(),
            query.as_deref(),
            body.as_deref(),
            request_headers.as_deref(),
            &content_rows,
            &result,
        )
    }

    /// Create a `RecordBatch` from pre-parsed content rows and HTTP response metadata.
    fn create_batch_from_rows(
        &self,
        path: Option<&str>,
        query: Option<&str>,
        body: Option<&str>,
        request_headers: Option<&str>,
        content_rows: &[String],
        fetch_result: &HttpFetchResult,
    ) -> DataFusionResult<RecordBatch> {
        let num_rows = content_rows.len();

        if num_rows == 0 {
            return RecordBatch::try_new(
                Arc::clone(&self.projected_schema),
                self.projected_schema
                    .fields()
                    .iter()
                    .map(|f| arrow::array::new_empty_array(f.data_type()))
                    .collect(),
            )
            .map_err(DataFusionError::from);
        }

        if let Some(nesting) = &self.provider.json_nesting {
            return self.create_batch_from_rows_nested(
                path,
                query,
                body,
                request_headers,
                content_rows,
                fetch_result,
                nesting,
            );
        }

        // Store the actual values from the partition for the primary key
        let path_for_batch = path.unwrap_or("");
        let query_for_batch = query.unwrap_or("");
        let body_for_batch = body.unwrap_or("");
        let headers_for_batch = request_headers.unwrap_or("");

        tracing::debug!(
            "Creating batch with request_path={:?}, content_len={}, num_rows={}",
            path_for_batch,
            fetch_result.content.len(),
            num_rows
        );

        // Use response Date header if available, otherwise use current time
        let timestamp_nanos = Self::compute_fetched_at_nanos(fetch_result)?;

        let columns = self
            .projected_schema
            .fields()
            .iter()
            .map(|field| {
                Self::build_metadata_array(
                    field.name().as_str(),
                    path_for_batch,
                    query_for_batch,
                    body_for_batch,
                    headers_for_batch,
                    content_rows,
                    fetch_result,
                    timestamp_nanos,
                    num_rows,
                )
            })
            .collect::<DataFusionResult<Vec<ArrayRef>>>()?;

        let batch = RecordBatch::try_new(Arc::clone(&self.projected_schema), columns)
            .map_err(DataFusionError::from)?;
        Ok(batch)
    }

    /// Build a single Arrow array for one of the HTTP connector's
    /// built-in metadata columns. Used by both the default scan path
    /// and the JSON-decomposition path so that metadata columns behave
    /// identically in both modes.
    #[expect(clippy::too_many_arguments)]
    fn build_metadata_array(
        name: &str,
        path_for_batch: &str,
        query_for_batch: &str,
        body_for_batch: &str,
        headers_for_batch: &str,
        content_rows: &[String],
        fetch_result: &HttpFetchResult,
        timestamp_nanos: i64,
        num_rows: usize,
    ) -> DataFusionResult<ArrayRef> {
        match name {
            "request_path" => {
                Ok(Arc::new(StringArray::from(vec![path_for_batch; num_rows])) as ArrayRef)
            }
            "request_query" => {
                Ok(Arc::new(StringArray::from(vec![query_for_batch; num_rows])) as ArrayRef)
            }
            "request_body" => {
                Ok(Arc::new(StringArray::from(vec![body_for_batch; num_rows])) as ArrayRef)
            }
            "request_headers" => {
                Ok(Arc::new(StringArray::from(vec![headers_for_batch; num_rows])) as ArrayRef)
            }
            "content" => Ok(Arc::new(StringArray::from_iter_values(
                content_rows.iter().map(String::as_str),
            )) as ArrayRef),
            "response_status" => Ok(Arc::new(UInt16Array::from(vec![
                fetch_result.response_status;
                num_rows
            ])) as ArrayRef),
            "response_headers" => {
                let mut builder = MapBuilder::new(
                    Some(MapFieldNames {
                        entry: "entries".to_string(),
                        key: "keys".to_string(),
                        value: "values".to_string(),
                    }),
                    StringBuilder::new(),
                    StringBuilder::new(),
                );
                for _ in 0..num_rows {
                    for (k, v) in &fetch_result.response_headers {
                        builder.keys().append_value(k);
                        builder.values().append_value(v);
                    }
                    builder
                        .append(true)
                        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                }
                Ok(Arc::new(builder.finish()) as ArrayRef)
            }
            "fetched_at" => {
                use arrow::array::TimestampNanosecondArray;
                Ok(Arc::new(TimestampNanosecondArray::from(vec![
                    timestamp_nanos;
                    num_rows
                ])) as ArrayRef)
            }
            other => Err(DataFusionError::Execution(format!(
                "Unsupported field name: {other}"
            ))),
        }
    }

    /// Compute the per-batch `fetched_at` timestamp in nanoseconds since
    /// the Unix epoch, preferring the response `Date` header and falling
    /// back to the current system time.
    fn compute_fetched_at_nanos(fetch_result: &HttpFetchResult) -> DataFusionResult<i64> {
        if let Some(date) = fetch_result.response_date {
            i64::try_from(
                date.duration_since(std::time::UNIX_EPOCH)
                    .map_err(|e| DataFusionError::Execution(format!("Invalid response date: {e}")))?
                    .as_nanos(),
            )
            .map_err(|e| DataFusionError::Execution(format!("Timestamp overflow: {e}")))
        } else {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|e| {
                    DataFusionError::Execution(format!("Failed to get current time: {e}"))
                })?;
            i64::try_from(now.as_nanos())
                .map_err(|e| DataFusionError::Execution(format!("Timestamp overflow: {e}")))
        }
    }

    /// Create a `RecordBatch` for the user-declared columns by
    /// decomposing each JSON response row according to the nesting
    /// configuration. Body-derived columns are produced as `Utf8` from
    /// the decomposed JSON; columns whose names match HTTP metadata
    /// fields (see [`HttpJsonNesting::metadata_fields`]) are populated
    /// from the request/response metadata via [`Self::build_metadata_array`]
    /// with their original types.
    ///
    /// Fast path: when the catch-all column is not in the projected
    /// schema we skip building the catch-all `BTreeMap` and re-
    /// serializing it, which is the dominant cost for wide JSON rows.
    /// Non-object rows still fall through to `decompose_json_row` so
    /// static columns become NULL and no data is dropped.
    #[expect(clippy::too_many_arguments)]
    fn create_batch_from_rows_nested(
        &self,
        path: Option<&str>,
        query: Option<&str>,
        body: Option<&str>,
        request_headers: Option<&str>,
        content_rows: &[String],
        fetch_result: &HttpFetchResult,
        nesting: &HttpJsonNesting,
    ) -> DataFusionResult<RecordBatch> {
        let fields = self.projected_schema.fields();
        let num_rows = content_rows.len();

        // Per-batch values for metadata columns. Computed lazily-ish:
        // cheap enough to always derive, and only used when at least
        // one metadata column is projected.
        let path_for_batch = path.unwrap_or("");
        let query_for_batch = query.unwrap_or("");
        let body_for_batch = body.unwrap_or("");
        let headers_for_batch = request_headers.unwrap_or("");
        let timestamp_nanos = Self::compute_fetched_at_nanos(fetch_result)?;

        // Identify which projected fields are body-derived vs metadata.
        let body_field_names: Vec<&str> = fields
            .iter()
            .filter(|f| !nesting.metadata_fields.contains(f.name()))
            .map(|f| f.name().as_str())
            .collect();
        let catchall_projected = body_field_names.contains(&nesting.json_field_name.as_str());

        // Build body-derived columns via string builders, in projected
        // (not full-schema) order, restricted to non-metadata fields.
        let mut body_builders: Vec<StringBuilder> = std::iter::repeat_with(StringBuilder::new)
            .take(body_field_names.len())
            .collect();

        for row in content_rows {
            if !catchall_projected
                && let Ok(serde_json::Value::Object(obj)) =
                    serde_json::from_str::<serde_json::Value>(row)
            {
                for (builder, name) in body_builders.iter_mut().zip(body_field_names.iter()) {
                    match obj.get(*name) {
                        None | Some(serde_json::Value::Null) => builder.append_null(),
                        Some(serde_json::Value::String(s)) => builder.append_value(s),
                        Some(other) => builder.append_value(other.to_string()),
                    }
                }
                continue;
            }

            let decomposed = decompose_json_row(row, nesting).map_err(|source| {
                DataFusionError::External(Box::new(Error::JsonNesting { source }))
            })?;
            for (builder, name) in body_builders.iter_mut().zip(body_field_names.iter()) {
                match decomposed.get(*name).and_then(|v| v.as_deref()) {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
        }

        let mut body_arrays: std::collections::VecDeque<ArrayRef> = body_builders
            .into_iter()
            .zip(body_field_names.iter())
            .map(|(mut b, name)| {
                let arr: ArrayRef = Arc::new(b.finish());
                let field = fields.iter().find(|f| f.name() == *name).ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "json-nested batch: body field `{name}` missing from projected schema"
                    ))
                })?;
                if field.data_type() == &DataType::Utf8 {
                    Ok(arr)
                } else {
                    cast(&arr, field.data_type()).map_err(|e| {
                        DataFusionError::External(Box::new(ArrowError::CastError(format!(
                            "failed to cast json_nest column `{}` from Utf8 to {:?}: {e}",
                            field.name(),
                            field.data_type()
                        ))))
                    })
                }
            })
            .collect::<DataFusionResult<_>>()?;

        // Stitch together the final column list in projected-schema
        // order, slotting metadata-built arrays where appropriate.
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(fields.len());
        for field in fields {
            if nesting.metadata_fields.contains(field.name()) {
                columns.push(Self::build_metadata_array(
                    field.name().as_str(),
                    path_for_batch,
                    query_for_batch,
                    body_for_batch,
                    headers_for_batch,
                    content_rows,
                    fetch_result,
                    timestamp_nanos,
                    num_rows,
                )?);
            } else {
                let array = body_arrays.pop_front().ok_or_else(|| {
                    DataFusionError::Internal(
                        "json-nested batch: body array count does not match non-metadata projected fields"
                            .to_string(),
                    )
                })?;
                columns.push(array);
            }
        }

        RecordBatch::try_new(Arc::clone(&self.projected_schema), columns)
            .map_err(DataFusionError::from)
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
            "HttpExec: base_url={}, format={}, ",
            self.provider.base_url, self.provider.file_format
        )?;

        if self.deferred_partitions {
            return write!(f, "partitions=deferred");
        }

        write!(f, "partitions=[")?;

        for (i, (path, query, body, request_headers)) in self.partitions.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(
                f,
                "(path={:?}, query={:?}, body={:?}, request_headers_present={})",
                path.as_deref().unwrap_or(""),
                query.as_deref().unwrap_or(""),
                body.as_deref().unwrap_or(""),
                request_headers.is_some()
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

    fn properties(&self) -> &Arc<PlanProperties> {
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

        if provider.is_paginated() {
            let (path, query, body, request_headers) = self.partitions[partition].clone();
            let limit = self.limit;

            let initial_state = PaginationState {
                page: 0,
                next_info: None,
                rows_fetched: 0,
                path,
                query,
                body,
                request_headers,
                limit,
                done: false,
                last_page_path: None,
                last_page_query: None,
                recent_page_urls: VecDeque::new(),
            };

            let stream = futures::stream::try_unfold(initial_state, move |mut state| {
                let exec = Arc::clone(&exec);
                let provider = Arc::clone(&provider);

                async move {
                    loop {
                        if state.done {
                            return Ok::<_, DataFusionError>(None);
                        }

                        let config = provider.pagination.as_ref().ok_or_else(|| {
                            DataFusionError::Internal("Pagination config missing".to_string())
                        })?;

                        if let Some(max_pages) = config.max_pages
                            && state.page >= max_pages
                        {
                            tracing::warn!(
                                "HTTP pagination reached the configured safety limit of {} pages. Increase `pagination_max_pages` to fetch additional pages.",
                                max_pages
                            );
                            return Ok(None);
                        }

                        if let Some(limit) = state.limit
                            && state.rows_fetched >= limit
                        {
                            return Ok(None);
                        }

                        let page_limit = state.limit.map(|l| l.saturating_sub(state.rows_fetched));

                        // Fetch this page
                        let fetch_result = if state.page == 0 {
                            let path_val = state.path.clone().unwrap_or_default();
                            let merged_query = if let Some(ref template) = config.query_params {
                                let page_size = config.page_size.unwrap_or(0);
                                let expanded =
                                    expand_query_params_template(template, 0, page_size)?;
                                Some(merge_base_and_partition_queries_with_override(
                                    provider.base_url.query(),
                                    state.query.as_deref(),
                                    &expanded,
                                ))
                            } else {
                                merge_base_and_partition_queries(
                                    provider.base_url.query(),
                                    state.query.as_deref(),
                                )
                            };
                            let request_url = provider
                                .build_request_url(&path_val, merged_query.as_deref())
                                .map_err(DataFusionError::from)?;
                            record_pagination_request_url(&mut state, &request_url)
                                .map_err(DataFusionError::from)?;
                            state.last_page_path = state.path.clone();
                            state.last_page_query = merged_query.clone();
                            let body_val = state.body.as_deref();
                            provider
                                .get_response(
                                    &path_val,
                                    merged_query.as_deref(),
                                    body_val,
                                    state.request_headers.as_deref(),
                                )
                                .await
                                .map_err(DataFusionError::from)?
                        } else {
                            let parsed_request_headers = state
                                .request_headers
                                .as_deref()
                                .map(|headers| provider.parse_request_headers(headers))
                                .transpose()
                                .map_err(DataFusionError::from)?;

                            // Subsequent pages bypass the HTTP cache intentionally:
                            // each page has unique content that shouldn't be cached
                            // under the same key as the base request.
                            match state.next_info.clone() {
                                Some(NextPageInfo::Url(url)) => {
                                    if let Some((globset, patterns)) = &provider.allowed_paths
                                        && !globset.is_match(url.path())
                                    {
                                        return Err(DataFusionError::External(Box::new(
                                            Error::Pagination {
                                                message: format!(
                                                    "Next page URL path '{}' does not match any allowed path patterns: [{}]. Update 'allowed_request_paths' to include a matching pattern.",
                                                    url.path(),
                                                    patterns
                                                        .iter()
                                                        .map(|p| format!("'{p}'"))
                                                        .collect::<Vec<_>>()
                                                        .join(", ")
                                                ),
                                            },
                                        )));
                                    }
                                    record_pagination_request_url(&mut state, &url)
                                        .map_err(DataFusionError::from)?;
                                    state.last_page_path = Some(url.path().to_string());
                                    state.last_page_query = url.query().map(ToString::to_string);
                                    provider
                                        .perform_request_with_retry(
                                            url,
                                            state.body.as_deref(),
                                            parsed_request_headers.as_ref(),
                                            &format!("page_{}", state.page),
                                        )
                                        .await
                                        .map_err(DataFusionError::from)?
                                }
                                Some(NextPageInfo::Token(token)) => {
                                    let path_val = state.path.as_deref().unwrap_or("");
                                    let token_param =
                                        config.token_param.as_deref().unwrap_or("cursor");
                                    let base_query = provider.base_url.query();
                                    let merged_query = merge_queries(
                                        base_query,
                                        state.query.as_deref(),
                                        token_param,
                                        &token,
                                    );
                                    state.last_page_path = state.path.clone();
                                    state.last_page_query = Some(merged_query.clone());
                                    let url = provider
                                        .build_request_url(path_val, Some(&merged_query))
                                        .map_err(DataFusionError::from)?;
                                    record_pagination_request_url(&mut state, &url)
                                        .map_err(DataFusionError::from)?;
                                    provider
                                        .perform_request_with_retry(
                                            url,
                                            state.body.as_deref(),
                                            parsed_request_headers.as_ref(),
                                            &format!("page_{}", state.page),
                                        )
                                        .await
                                        .map_err(DataFusionError::from)?
                                }
                                Some(NextPageInfo::QueryParams { page }) => {
                                    let path_val = state.path.as_deref().unwrap_or("");
                                    let template = config.query_params.as_deref().unwrap_or("");
                                    let page_size = config.page_size.unwrap_or(0);
                                    let expanded =
                                        expand_query_params_template(template, page, page_size)?;
                                    let merged_query =
                                        merge_base_and_partition_queries_with_override(
                                            provider.base_url.query(),
                                            state.query.as_deref(),
                                            &expanded,
                                        );
                                    state.last_page_path = state.path.clone();
                                    state.last_page_query = Some(merged_query.clone());
                                    let url = provider
                                        .build_request_url(path_val, Some(&merged_query))
                                        .map_err(DataFusionError::from)?;
                                    record_pagination_request_url(&mut state, &url)
                                        .map_err(DataFusionError::from)?;
                                    provider
                                        .perform_request_with_retry(
                                            url,
                                            state.body.as_deref(),
                                            parsed_request_headers.as_ref(),
                                            &format!("page_{}", state.page),
                                        )
                                        .await
                                        .map_err(DataFusionError::from)?
                                }
                                None => {
                                    return Err(DataFusionError::Internal(
                                        "page > 0 but no next page info".to_string(),
                                    ));
                                }
                            }
                        };

                        // Parse response JSON once for both next-page and data extraction
                        let parsed_json = if config.next_pointer.is_some()
                            || config.data_pointer.is_some()
                        {
                            Some(
                                    serde_json::from_str::<serde_json::Value>(
                                        &fetch_result.content,
                                    )
                                    .map_err(|source| {
                                        let pointers: Vec<&str> = [
                                            config.next_pointer.as_deref(),
                                            config.data_pointer.as_deref(),
                                        ]
                                        .into_iter()
                                        .flatten()
                                        .collect();
                                        DataFusionError::Execution(format!(
                                            "Failed to parse paginated HTTP response as JSON for pointer(s) {pointers:?}: {source}"
                                        ))
                                    })?,
                                )
                        } else {
                            None
                        };

                        // Extract next page info first (before checking rows)
                        let next_info = extract_next_page_info(
                            parsed_json.as_ref(),
                            &fetch_result.response_headers,
                            config,
                            &provider.base_url,
                            state.page,
                        )
                        .map_err(DataFusionError::from)?;

                        // Extract data rows using data_pointer if configured
                        let content_rows = extract_page_data(
                            &fetch_result.content,
                            parsed_json.as_ref(),
                            config,
                            page_limit,
                        )?;

                        // Update pagination state
                        state.page += 1;
                        state.next_info = next_info;
                        if state.next_info.is_none() {
                            state.done = true;
                        }

                        // Query-param pagination stop condition: fewer rows than page_size = last page
                        if config.query_params.is_some()
                            && config
                                .page_size
                                .is_some_and(|page_size| content_rows.len() < page_size)
                        {
                            state.done = true;
                        }

                        // Skip empty pages internally — loop again instead of yielding
                        if content_rows.is_empty() {
                            if state.done {
                                return Ok(None);
                            }
                            continue;
                        }

                        let num_rows = content_rows.len();
                        let batch = exec.create_batch_from_rows(
                            state.last_page_path.as_deref(),
                            state.last_page_query.as_deref(),
                            state.body.as_deref(),
                            state.request_headers.as_deref(),
                            &content_rows,
                            &fetch_result,
                        )?;

                        state.rows_fetched += num_rows;

                        tracing::debug!(
                            "Pagination page {}: {} rows fetched, total so far: {}",
                            state.page - 1,
                            num_rows,
                            state.rows_fetched
                        );

                        return Ok(Some((batch, state)));
                    }
                }
            });

            let stream_adapter = RecordBatchStreamAdapter::new(schema, stream);
            Ok(Box::pin(stream_adapter))
        } else {
            // Non-paginated: single fetch
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
}

// --- Pagination types and helpers ---

#[derive(Clone, Debug)]
enum NextPageInfo {
    /// Full URL for the next page.
    Url(Url),
    /// Cursor/token to add as a query parameter.
    Token(String),
    /// Client-driven query-parameter pagination; carries the next page number.
    QueryParams { page: usize },
}

struct PaginationState {
    page: usize,
    next_info: Option<NextPageInfo>,
    rows_fetched: usize,
    path: Option<String>,
    query: Option<String>,
    body: Option<String>,
    request_headers: Option<String>,
    limit: Option<usize>,
    done: bool,
    /// The actual path/query used for the most recent page fetch.
    /// Used to populate accurate `request_path`/`request_query` columns.
    last_page_path: Option<String>,
    last_page_query: Option<String>,
    recent_page_urls: VecDeque<String>,
}

fn pagination_request_label(url: &Url) -> String {
    let mut hasher = DefaultHasher::new();
    url.as_str().hash(&mut hasher);
    format!("http-pagination-request:{:016x}", hasher.finish())
}

fn record_pagination_request_url(state: &mut PaginationState, url: &Url) -> Result<()> {
    let request_url = url.as_str().to_string();
    ensure!(
        !state.recent_page_urls.contains(&request_url),
        PaginationSnafu {
            message: format!(
                "HTTP pagination detected a repeated next page request ({}). The connector stopped before fetching duplicate rows. Check pagination_next_pointer, pagination_link_header, pagination_token_param, or pagination_query_params.",
                pagination_request_label(url)
            )
        }
    );
    state.recent_page_urls.push_back(request_url);
    if state.recent_page_urls.len() > PAGINATION_REPEAT_DETECTION_WINDOW {
        state.recent_page_urls.pop_front();
    }
    Ok(())
}

/// Resolve a next-page URL string (absolute or relative) against the base URL
/// and validate same-origin for SSRF protection.
fn resolve_and_validate_url(raw: &str, base_url: &Url, context: &str) -> Result<Url> {
    // Try absolute first, fall back to resolving relative against base
    let resolved = Url::parse(raw)
        .or_else(|_| base_url.join(raw))
        .map_err(|e| Error::Pagination {
            message: format!("Invalid next page URL in {context}: '{raw}': {e}"),
        })?;
    if resolved.origin() != base_url.origin() {
        return Err(Error::Pagination {
            message: format!(
                "{context} URL origin '{}' does not match base URL origin '{}'. The next page URL must stay on the same origin.",
                resolved.origin().ascii_serialization(),
                base_url.origin().ascii_serialization(),
            ),
        });
    }
    Ok(resolved)
}

/// Extract the next page info from an HTTP response body and/or headers.
///
/// When `next_pointer` finds an explicit termination signal (null or empty string),
/// pagination stops immediately. When the pointer path is missing from the response,
/// we fall through to check the `Link` header (if configured) before giving up.
///
/// In query-params mode, always returns `QueryParams { page: current_page + 1 }`;
/// the stop condition (row count < `page_size`) is checked separately in the loop.
fn extract_next_page_info(
    parsed_json: Option<&serde_json::Value>,
    response_headers: &[(String, String)],
    config: &PaginationConfig,
    base_url: &Url,
    current_page: usize,
) -> Result<Option<NextPageInfo>> {
    // Query-param pagination: always return next page; stop is checked by row count
    if config.query_params.is_some() {
        return Ok(Some(NextPageInfo::QueryParams {
            page: current_page + 1,
        }));
    }

    // Try response body JSON pointer first
    if let Some(ref pointer) = config.next_pointer {
        let parsed = parsed_json.ok_or_else(|| Error::Pagination {
            message: format!("JSON not parsed but next_pointer '{pointer}' is configured"),
        })?;

        if let Some(value) = parsed.pointer(pointer) {
            match value {
                serde_json::Value::String(next_str) if !next_str.is_empty() => {
                    if config.token_param.is_some() {
                        return Ok(Some(NextPageInfo::Token(next_str.clone())));
                    }
                    let next_url = resolve_and_validate_url(
                        next_str,
                        base_url,
                        &format!("JSON pointer '{pointer}'"),
                    )?;
                    return Ok(Some(NextPageInfo::Url(next_url)));
                }
                serde_json::Value::Number(n) => {
                    // Numeric values (e.g. page numbers) are only valid in token mode.
                    let token = n.to_string();
                    if config.token_param.is_some() {
                        return Ok(Some(NextPageInfo::Token(token)));
                    }
                    return PaginationSnafu {
                        message: format!(
                            "Failed to extract pagination value from JSON pointer '{pointer}': numeric values require 'pagination_token_param' to be configured"
                        ),
                    }
                    .fail();
                }
                serde_json::Value::Null | serde_json::Value::String(_) => {
                    // Null or empty string is an explicit end of pagination.
                    return Ok(None);
                }
                _ => {
                    return PaginationSnafu {
                        message: format!(
                            "Failed to extract pagination value from JSON pointer '{pointer}': expected a string, number, or null"
                        ),
                    }
                    .fail();
                }
            }
        }
        // Pointer path not found in response — fall through to Link header
        // (the API may not include the field on the last page)
    }

    // Try HTTP Link header with rel="next"
    if config.use_link_header {
        for (name, value) in response_headers {
            if name.eq_ignore_ascii_case("link")
                && let Some(next_url_str) = parse_link_header_next(value)
            {
                let next_url = resolve_and_validate_url(&next_url_str, base_url, "Link header")?;
                return Ok(Some(NextPageInfo::Url(next_url)));
            }
        }
    }

    Ok(None)
}

/// Split a Link header value on a delimiter only when it appears at the
/// top level — outside `<...>` URI references and `"..."` quoted strings.
fn split_link_header_top_level(value: &str, delimiter: char) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut in_angle = false;
    let mut in_quotes = false;
    let mut escaped = false;

    for (idx, ch) in value.char_indices() {
        if in_quotes {
            if escaped {
                escaped = false;
                continue;
            }
            match ch {
                '\\' => escaped = true,
                '"' => in_quotes = false,
                _ => {}
            }
            continue;
        }

        match ch {
            '"' => in_quotes = true,
            '<' => in_angle = true,
            '>' => in_angle = false,
            _ if ch == delimiter && !in_angle => {
                parts.push(value[start..idx].trim());
                start = idx + ch.len_utf8();
            }
            _ => {}
        }
    }

    parts.push(value[start..].trim());
    parts
}

/// Parse an HTTP `Link` header to find a URI with `rel="next"`.
/// Handles quoted (`rel="next"`), single-quoted (`rel='next'`), and
/// unquoted (`rel=next`) forms, as well as multi-value rel lists
/// (e.g., `rel="next prev"`).
///
/// Splits on commas and semicolons only at the top level (outside `<...>`
/// and `"..."`) so that URIs containing commas are handled correctly per
/// RFC 8288.
fn parse_link_header_next(header_value: &str) -> Option<String> {
    for link in split_link_header_top_level(header_value, ',') {
        let link = link.trim();
        if !link.starts_with('<') {
            continue;
        }

        let end = link.find('>')?;
        let url_part = &link[1..end];
        let params = link[end + 1..].trim();

        let is_next = split_link_header_top_level(params, ';')
            .into_iter()
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .any(|param| {
                let Some((name, value)) = param.split_once('=') else {
                    return false;
                };
                if !name.trim().eq_ignore_ascii_case("rel") {
                    return false;
                }
                let value = value.trim().trim_matches('"').trim_matches('\'');
                value
                    .split_whitespace()
                    .any(|relation| relation.eq_ignore_ascii_case("next"))
            });

        if is_next {
            return Some(url_part.to_string());
        }
    }
    None
}

/// Extract data rows from a page response, using `data_pointer` if configured.
fn extract_page_data(
    content: &str,
    parsed_json: Option<&serde_json::Value>,
    config: &PaginationConfig,
    limit: Option<usize>,
) -> DataFusionResult<Vec<String>> {
    if let Some(ref pointer) = config.data_pointer {
        let json = parsed_json.ok_or_else(|| {
            DataFusionError::Execution(format!(
                "JSON not parsed but data_pointer '{pointer}' is configured"
            ))
        })?;

        let data = json.pointer(pointer).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "Failed to extract paginated HTTP response data: configured data pointer '{pointer}' was not found in the response"
            ))
        })?;

        if let Some(arr) = data.as_array() {
            return Ok(arr
                .iter()
                .take(limit.unwrap_or(usize::MAX))
                .map(std::string::ToString::to_string)
                .collect());
        }
        if config.data_map_to_array
            && let Some(obj) = data.as_object()
        {
            return Ok(obj
                .values()
                .take(limit.unwrap_or(usize::MAX))
                .map(std::string::ToString::to_string)
                .collect());
        }
        // Not an array (and not a map-to-array) — return as a single row
        return Ok(vec![data.to_string()]);
    }

    // No data_pointer — use normal parse_content logic
    Ok(parse_content_with_map_to_array(
        content,
        limit,
        config.data_map_to_array,
    ))
}

/// Like `HttpExec::parse_content` but when `data_map_to_array` is `true`,
/// a top-level JSON object has its values extracted as rows.
fn parse_content_with_map_to_array(
    content: &str,
    limit: Option<usize>,
    data_map_to_array: bool,
) -> Vec<String> {
    if data_map_to_array {
        let trimmed = content.trim();
        if let Ok(serde_json::Value::Object(map)) =
            serde_json::from_str::<serde_json::Value>(trimmed)
        {
            return map
                .values()
                .take(limit.unwrap_or(usize::MAX))
                .map(std::string::ToString::to_string)
                .collect();
        }
    }
    HttpExec::parse_content(content, limit)
}

/// Merge base URL query params, partition query params, and a pagination token
/// into a single query string. Base URL params come first, then partition params
/// (overriding any base duplicates), then the token param (overriding any existing).
/// Merge base URL query params with partition query params.
/// Partition params override base params with the same key.
/// Returns `None` if both inputs are `None`.
fn merge_base_and_partition_queries(
    base_query: Option<&str>,
    partition_query: Option<&str>,
) -> Option<String> {
    if base_query.is_none() && partition_query.is_none() {
        return None;
    }

    let mut pairs: Vec<(String, String)> = Vec::new();

    if let Some(base) = base_query {
        pairs.extend(
            url::form_urlencoded::parse(base.as_bytes())
                .map(|(k, v)| (k.into_owned(), v.into_owned())),
        );
    }

    if let Some(partition) = partition_query {
        for (key, value) in url::form_urlencoded::parse(partition.as_bytes()) {
            let key_str: &str = &key;
            pairs.retain(|(k, _)| k != key_str);
            pairs.push((key.into_owned(), value.into_owned()));
        }
    }

    Some(
        url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs(pairs)
            .finish(),
    )
}

fn merge_queries(
    base_query: Option<&str>,
    partition_query: Option<&str>,
    token_param: &str,
    token: &str,
) -> String {
    let override_params = url::form_urlencoded::Serializer::new(String::new())
        .append_pair(token_param, token)
        .finish();
    merge_base_and_partition_queries_with_override(base_query, partition_query, &override_params)
}

/// Expand `{offset}`, `{limit}`, and `{page}` variables in a query-param template.
fn expand_query_params_template(
    template: &str,
    page: usize,
    page_size: usize,
) -> DataFusionResult<String> {
    let offset = page.checked_mul(page_size).ok_or_else(|| {
        DataFusionError::Execution(format!(
            "Pagination offset overflow: page ({page}) * page_size ({page_size}) exceeds maximum"
        ))
    })?;
    Ok(template
        .replace("{offset}", &offset.to_string())
        .replace("{limit}", &page_size.to_string())
        .replace("{page}", &page.to_string()))
}

/// Merge base + partition queries, then override with additional query params.
/// Override params replace any base/partition params with the same key.
fn merge_base_and_partition_queries_with_override(
    base_query: Option<&str>,
    partition_query: Option<&str>,
    override_params: &str,
) -> String {
    let merged = merge_base_and_partition_queries(base_query, partition_query).unwrap_or_default();

    let mut pairs: Vec<(String, String)> = url::form_urlencoded::parse(merged.as_bytes())
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();

    for (key, value) in url::form_urlencoded::parse(override_params.as_bytes()) {
        let key_str: &str = &key;
        pairs.retain(|(k, _)| k != key_str);
        pairs.push((key.into_owned(), value.into_owned()));
    }

    url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(pairs)
        .finish()
}

impl HttpTableProvider {
    /// Extract request partition values from filters.
    ///
    /// Path, query, body, and header filters are all used to build the partition
    /// cross product, with each unique combination producing a separate HTTP
    /// request partition.
    fn extract_partitions(&self, filters: &[Expr]) -> DataFusionResult<Vec<PartitionSpec>> {
        tracing::trace!(
            "extract_partitions called with {} filters, allowed_paths={:?}, allow_query_filters={}, allow_body_filters={}, allow_header_filters={}",
            filters.len(),
            self.allowed_paths,
            self.request_filter_options
                .is_enabled(RequestFilterKind::Query),
            self.request_filter_options
                .is_enabled(RequestFilterKind::Body),
            self.request_filter_options
                .is_enabled(RequestFilterKind::Headers)
        );

        let mut accumulator = PartitionAccumulator::new();

        for filter in filters {
            self.extract_filter_values(filter, &mut accumulator)
                .map_err(DataFusionError::from)?;
        }

        tracing::trace!(
            "After processing filters: has_path_filter={}, has_query_filter={}, has_body_filter={}, has_header_filter={}",
            accumulator.has_filter(RequestFilterKind::Path),
            accumulator.has_filter(RequestFilterKind::Query),
            accumulator.has_filter(RequestFilterKind::Body),
            accumulator.has_filter(RequestFilterKind::Headers)
        );

        let partition_values = accumulator.finalize();

        tracing::trace!(
            "After finalize: paths={:?}, queries={:?}, bodies={:?}, headers_count={}",
            partition_values.paths,
            partition_values.queries,
            partition_values.bodies,
            partition_values.headers.len()
        );

        self.ensure_request_partition_count(
            partition_values.paths.len(),
            partition_values.queries.len(),
            partition_values.bodies.len(),
            partition_values.headers.len(),
        )?;

        let mut partitions = vec![];
        for p in &partition_values.paths {
            for q in &partition_values.queries {
                for b in &partition_values.bodies {
                    for h in &partition_values.headers {
                        partitions.push((
                            if p.is_empty() { None } else { Some(p.clone()) },
                            q.clone(),
                            b.clone(),
                            h.clone(),
                        ));
                    }
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
            Operator::And => {
                self.extract_filter_values(expr.left.as_ref(), accumulator)?;
                self.extract_filter_values(expr.right.as_ref(), accumulator)
            }
            Operator::Or => {
                // OR within a single HTTP virtual filter column is treated as
                // an IN list (alternative values). OR across different
                // columns would be silently rewritten as a cross product
                // (AND) by the partition accumulator, causing the connector
                // to issue combined HTTP requests instead of separate ones.
                // Reject the cross-column case explicitly. (Note: SQL
                // `IN (...)` is sometimes pre-rewritten by DataFusion into a
                // chain of OR-of-equality, which is why same-column OR must
                // still be accepted.)
                let mut columns = HashSet::new();
                Self::collect_http_filter_columns(expr.left.as_ref(), &mut columns);
                Self::collect_http_filter_columns(expr.right.as_ref(), &mut columns);
                if columns.len() > 1 {
                    let mut names: Vec<&str> = columns.into_iter().collect();
                    names.sort_unstable();
                    return Err(Error::FilterRejected {
                        message: format!(
                            "OR across different HTTP filter columns ({}) is not supported because the connector would otherwise issue combined HTTP requests instead of separate ones. Use IN (...) to enumerate values on a single column, or run separate queries (e.g. UNION ALL) for alternative requests.",
                            names.join(", ")
                        ),
                    });
                }
                self.extract_filter_values(expr.left.as_ref(), accumulator)?;
                self.extract_filter_values(expr.right.as_ref(), accumulator)
            }
            _ => Ok(()),
        }
    }

    /// Walk an expression tree and collect the names of any HTTP virtual
    /// filter columns (`request_path`, `request_query`, `request_body`,
    /// `request_headers`) referenced anywhere inside it.
    fn collect_http_filter_columns(expr: &Expr, columns: &mut HashSet<&'static str>) {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, right, .. }) => {
                Self::collect_http_filter_columns(left.as_ref(), columns);
                Self::collect_http_filter_columns(right.as_ref(), columns);
            }
            Expr::InList(in_list) => {
                Self::collect_http_filter_columns(in_list.expr.as_ref(), columns);
            }
            Expr::Column(column) => {
                if let Some(static_name) = match column.name.as_str() {
                    "request_path" => Some("request_path"),
                    "request_query" => Some("request_query"),
                    "request_body" => Some("request_body"),
                    "request_headers" => Some("request_headers"),
                    _ => None,
                } {
                    columns.insert(static_name);
                }
            }
            _ => {}
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
                "request_path" | "request_query" | "request_body" | "request_headers"
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
        if column_name == "request_headers" {
            tracing::trace!(
                "apply_literal_filter: column={}, value=<redacted {} bytes>",
                column_name,
                value.len()
            );
        } else {
            tracing::trace!(
                "apply_literal_filter: column={}, value={}",
                column_name,
                value
            );
        }
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
            "request_headers" => {
                let normalized = self.ensure_allowed_headers(value)?;
                tracing::trace!("Header filter validated: {} bytes", normalized.len());
                accumulator.record_headers(normalized);
            }
            _ => {
                tracing::debug!("Ignoring filter on column: {}", column_name);
            }
        }
        Ok(())
    }

    /// Check if a filter expression can be pushed down to HTTP requests
    /// Note: This returns true if the filter is on `request_path`, `request_query`, `request_body`, or `request_headers` columns.
    /// Actual validation (whether the feature is enabled/configured) happens in `extract_partitions` with user-friendly errors.
    fn can_pushdown_filter(filter: &Expr) -> bool {
        match filter {
            // Simple equality on request_path, request_query, or request_body
            Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::Eq => {
                if let Expr::Column(col) = left.as_ref() {
                    if let Expr::Literal(ScalarValue::Utf8(Some(_value)), _) = right.as_ref() {
                        matches!(
                            col.name.as_str(),
                            "request_path" | "request_query" | "request_body" | "request_headers"
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
                        "request_path" | "request_query" | "request_body" | "request_headers"
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
            self.request_filter_options
                .is_enabled(RequestFilterKind::Query)
        );

        if !self
            .request_filter_options
            .is_enabled(RequestFilterKind::Query)
        {
            tracing::warn!("Query filter attempted but allow_query_filters is false");
            return Err(Error::FilterRejected {
                message:
                    "Cannot filter by 'request_query' because query filtering is disabled for this dataset. To enable, set the 'request_query_filters' parameter to 'enabled' in your dataset configuration.".to_string(),
            });
        }
        if raw.len() > self.request_filter_options.max_query_length {
            return Err(Error::FilterRejected {
                message: format!(
                    "The 'request_query' value is too long ({} characters). Maximum allowed length is {} characters. You can increase this limit using the 'max_request_query_length' parameter.",
                    raw.len(),
                    self.request_filter_options.max_query_length
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
            self.request_filter_options
                .is_enabled(RequestFilterKind::Body)
        );

        if !self
            .request_filter_options
            .is_enabled(RequestFilterKind::Body)
        {
            tracing::warn!("Body filter attempted but allow_body_filters is false");
            return Err(Error::FilterRejected {
                message:
                    "Cannot filter by 'request_body' because body filtering is disabled for this dataset. To enable, set the 'request_body_filters' parameter to 'enabled' in your dataset configuration.".to_string(),
            });
        }
        if raw.len() > self.request_filter_options.max_body_bytes {
            return Err(Error::FilterRejected {
                message: format!(
                    "The 'request_body' value is too large ({} bytes). Maximum allowed size is {} bytes. You can increase this limit using the 'max_request_body_bytes' parameter.",
                    raw.len(),
                    self.request_filter_options.max_body_bytes
                ),
            });
        }

        Ok(raw.to_string())
    }

    fn ensure_allowed_headers(&self, raw: &str) -> Result<String> {
        tracing::debug!(
            "ensure_allowed_headers called with allow_header_filters={}, bytes={}",
            self.request_filter_options
                .is_enabled(RequestFilterKind::Headers),
            raw.len()
        );

        if !self
            .request_filter_options
            .is_enabled(RequestFilterKind::Headers)
        {
            tracing::warn!("Header filter attempted but allow_header_filters is false");
            return Err(Error::FilterRejected {
                message:
                    "Cannot filter by 'request_headers' because header filtering is disabled for this dataset. To enable, set the 'request_header_filters' parameter to 'enabled' and configure 'request_header_allowlist' with the header names that may vary."
                        .to_string(),
            });
        }
        if raw.len() > self.request_filter_options.max_headers_length {
            return Err(Error::FilterRejected {
                message: format!(
                    "The 'request_headers' value is too large ({} bytes). Maximum allowed size is {} bytes. You can increase this limit using the 'max_request_headers_length' parameter.",
                    raw.len(),
                    self.request_filter_options.max_headers_length
                ),
            });
        }

        self.parse_request_headers(raw)?;
        Ok(raw.to_string())
    }

    fn parse_request_headers(&self, raw: &str) -> Result<HeaderMap> {
        let parsed = serde_json::from_str::<serde_json::Value>(raw).map_err(|e| {
            Error::FilterRejected {
                message: format!(
                    "The 'request_headers' value must be a JSON object with string header values. Failed to parse JSON: {e}"
                ),
            }
        })?;

        let serde_json::Value::Object(headers_object) = parsed else {
            return Err(Error::FilterRejected {
                message: "The 'request_headers' value must be a JSON object with string header values, such as '{\"x-sandbox-id\":\"sandbox-1\"}'.".to_string(),
            });
        };

        let mut headers = HeaderMap::new();
        for (name, value) in headers_object {
            let header_name = HeaderName::try_from(name.as_str()).map_err(|e| {
                Error::FilterRejected {
                    message: format!(
                        "The 'request_headers' object contains invalid HTTP header name '{name}': {e}"
                    ),
                }
            })?;

            if !self
                .request_filter_options
                .allowed_headers
                .contains(&header_name)
            {
                return Err(Error::FilterRejected {
                    message: format!(
                        "The 'request_headers' object contains header '{name}', which is not in request_header_allowlist. Add '{name}' to request_header_allowlist or remove it from the filter."
                    ),
                });
            }

            if self.auth.is_some() && header_name == AUTHORIZATION {
                return Err(Error::FilterRejected {
                    message: "The 'request_headers' object cannot set 'authorization' when HTTP authentication is configured. Remove 'authorization' from request_headers or disable HTTP authentication.".to_string(),
                });
            }

            let Some(header_value) = value.as_str() else {
                return Err(Error::FilterRejected {
                    message: format!(
                        "The 'request_headers' value for header '{name}' must be a string."
                    ),
                });
            };

            let header_value = HeaderValue::from_str(header_value).map_err(|_| {
                Error::FilterRejected {
                    message: format!(
                        "The 'request_headers' value for header '{name}' is not a valid HTTP header value."
                    ),
                }
            })?;
            headers.insert(header_name, header_value);
        }

        Ok(headers)
    }

    fn ensure_request_partition_count(
        &self,
        path_count: usize,
        query_count: usize,
        body_count: usize,
        header_count: usize,
    ) -> Result<()> {
        let partition_count = path_count
            .checked_mul(query_count)
            .and_then(|count| count.checked_mul(body_count))
            .and_then(|count| count.checked_mul(header_count))
            .ok_or_else(|| Error::FilterRejected {
                message: "The HTTP request partition count overflowed while combining request_path, request_query, request_body, and request_headers filters. Reduce the number of filter values.".to_string(),
            })?;

        if let Some(max_request_partitions) = self.max_request_partitions {
            ensure!(
                partition_count <= max_request_partitions,
                FilterRejectedSnafu {
                    message: format!(
                        "The HTTP connector would create {partition_count} request partitions, which exceeds max_request_partitions={max_request_partitions}. Reduce the number of request_path, request_query, request_body, or request_headers filter values, or increase max_request_partitions."
                    )
                }
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::Column;
    use datafusion::logical_expr::{BinaryExpr, Expr, Operator, expr::InList};
    use datafusion::scalar::ScalarValue;
    use std::sync::{Arc, atomic::AtomicUsize};
    use std::time::Duration;
    use url::Url;

    #[derive(Debug)]
    struct TestAuthenticator;

    impl super::super::auth::HttpAuthenticator for TestAuthenticator {
        fn apply(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
            builder.header(AUTHORIZATION, "Bearer token")
        }
    }

    /// Build a query string by adding or replacing a token parameter.
    fn build_query_with_token(existing_query: Option<&str>, param: &str, token: &str) -> String {
        merge_queries(None, existing_query, param, token)
    }

    /// Test helper: parse content and call `extract_next_page_info`
    fn extract_next_page_info(
        content: &str,
        headers: &[(String, String)],
        config: &PaginationConfig,
        base_url: &Url,
    ) -> super::Result<Option<NextPageInfo>> {
        extract_next_page_info_at_page(content, headers, config, base_url, 0)
    }

    fn extract_next_page_info_at_page(
        content: &str,
        headers: &[(String, String)],
        config: &PaginationConfig,
        base_url: &Url,
        current_page: usize,
    ) -> super::Result<Option<NextPageInfo>> {
        let parsed = if config.next_pointer.is_some() {
            Some(
                serde_json::from_str::<serde_json::Value>(content)
                    .expect("test content should be valid JSON"),
            )
        } else {
            None
        };
        super::extract_next_page_info(parsed.as_ref(), headers, config, base_url, current_page)
    }

    /// Test helper: parse content and call `extract_page_data`
    fn extract_page_data(
        content: &str,
        config: &PaginationConfig,
        limit: Option<usize>,
    ) -> datafusion::common::Result<Vec<String>> {
        let parsed = if config.data_pointer.is_some() {
            Some(
                serde_json::from_str::<serde_json::Value>(content)
                    .expect("test content should be valid JSON"),
            )
        } else {
            None
        };
        super::extract_page_data(content, parsed.as_ref(), config, limit)
    }

    fn base_provider() -> HttpTableProvider {
        HttpTableProvider::new(
            Url::parse("https://api.example.com").expect("valid URL"),
            Client::new(),
            "json".to_string(),
            false,
        )
    }

    /// Test helper: build the legacy all-Utf8 nesting schema that
    /// `with_json_nesting` produced before it became schema-driven.
    fn nesting_schema_utf8(nesting: &HttpJsonNesting) -> SchemaRef {
        let fields: Vec<Field> = nesting
            .column_order
            .iter()
            .map(|name| Field::new(name, DataType::Utf8, true))
            .collect();
        Arc::new(Schema::new(fields))
    }

    /// Test helper: build a nesting schema where HTTP metadata
    /// columns inherit base-schema types and other columns are `Utf8`.
    fn nesting_schema_with_metadata(nesting: &HttpJsonNesting) -> SchemaRef {
        let base = HttpTableProvider::base_table_schema();
        let fields: Vec<Field> = nesting
            .column_order
            .iter()
            .map(|name| {
                if nesting.metadata_fields.contains(name)
                    && let Ok(f) = base.field_with_name(name)
                {
                    return f.clone();
                }
                Field::new(name, DataType::Utf8, true)
            })
            .collect();
        Arc::new(Schema::new(fields))
    }

    fn header_provider() -> HttpTableProvider {
        base_provider()
            .enable_header_filters(DEFAULT_MAX_HEADERS_LENGTH, vec!["x-sandbox-id", "x-region"])
            .expect("header filters should enable")
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

        // Path and query filters together produce one partition tuple containing both values.
        assert_eq!(partitions.len(), 1);
        assert_eq!(
            partitions[0],
            (
                Some("/singlesearch/shows".to_string()),
                Some("q=South%20Park".to_string()),
                None,
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
        assert_eq!(
            partitions[0],
            (Some("/api/data".to_string()), None, None, None)
        );
    }

    #[test]
    fn test_extract_partitions_with_no_filters() {
        let filters = vec![];

        let partitions = base_provider()
            .extract_partitions(&filters)
            .expect("partitions");

        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0], (None, None, None, None));
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
        assert!(partitions.contains(&(Some("/path1".to_string()), None, None, None)));
        assert!(partitions.contains(&(Some("/path2".to_string()), None, None, None)));
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
        assert!(partitions.contains(&(Some("/api/v1/users".to_string()), None, None, None)));
        assert!(partitions.contains(&(Some("/api/v1/posts".to_string()), None, None, None)));
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

        // Query filters in an IN list produce one extracted partition tuple per query value.
        // These partitions do not constrain the path, so `request_path` remains `None`.
        assert_eq!(partitions.len(), 2);
        assert_eq!(
            partitions[0],
            (None, Some("limit=10".to_string()), None, None)
        );
        assert_eq!(
            partitions[1],
            (None, Some("limit=20".to_string()), None, None)
        );
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
        assert!(partitions.contains(&(Some("/api/v1".to_string()), None, None, None)));
        assert!(partitions.contains(&(Some("/api/v2".to_string()), None, None, None)));
    }

    #[test]
    fn test_extract_partitions_with_or_across_columns_is_rejected() {
        let provider = base_provider()
            .with_allowed_paths(vec!["/a".to_string()])
            .expect("allowed paths")
            .enable_query_filters(64);
        // request_path = '/a' OR request_query = 'b=1'
        let filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Column(Column::from_name("request_path"))),
                op: Operator::Eq,
                right: Box::new(Expr::Literal(
                    ScalarValue::Utf8(Some("/a".to_string())),
                    None,
                )),
            })),
            op: Operator::Or,
            right: Box::new(Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Column(Column::from_name("request_query"))),
                op: Operator::Eq,
                right: Box::new(Expr::Literal(
                    ScalarValue::Utf8(Some("b=1".to_string())),
                    None,
                )),
            })),
        })];

        let err = provider
            .extract_partitions(&filters)
            .expect_err("OR across HTTP virtual columns must be rejected");
        let message = err.to_string();
        assert!(
            message.contains("OR across different HTTP filter columns"),
            "unexpected error message: {message}"
        );
        assert!(message.contains("request_path"));
        assert!(message.contains("request_query"));
    }

    #[test]
    fn test_extract_partitions_with_or_across_columns_nested_is_rejected() {
        let provider = base_provider()
            .with_allowed_paths(vec!["/a".to_string()])
            .expect("allowed paths")
            .enable_header_filters(DEFAULT_MAX_HEADERS_LENGTH, vec!["x-sandbox-id".to_string()])
            .expect("enable header filters");
        // request_path = '/a' OR request_headers IN ('{"x-sandbox-id":"a"}')
        let filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Column(Column::from_name("request_path"))),
                op: Operator::Eq,
                right: Box::new(Expr::Literal(
                    ScalarValue::Utf8(Some("/a".to_string())),
                    None,
                )),
            })),
            op: Operator::Or,
            right: Box::new(Expr::InList(InList {
                expr: Box::new(Expr::Column(Column::from_name("request_headers"))),
                list: vec![Expr::Literal(
                    ScalarValue::Utf8(Some(r#"{"x-sandbox-id":"a"}"#.to_string())),
                    None,
                )],
                negated: false,
            })),
        })];

        let err = provider
            .extract_partitions(&filters)
            .expect_err("OR across HTTP virtual columns must be rejected");
        assert!(
            err.to_string()
                .contains("OR across different HTTP filter columns")
        );
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

        // The path filter is crossed with each query value to produce separate partitions.
        assert_eq!(partitions.len(), 2);
        assert_eq!(
            partitions[0],
            (
                Some("/api/users".to_string()),
                Some("limit=10".to_string()),
                None,
                None
            )
        );
        assert_eq!(
            partitions[1],
            (
                Some("/api/users".to_string()),
                Some("limit=20".to_string()),
                None,
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
    fn test_extract_partitions_with_request_headers_in_list() {
        let provider = header_provider();
        let headers_1 = r#"{"x-sandbox-id":"sandbox-1"}"#.to_string();
        let headers_2 = r#"{"x-sandbox-id":"sandbox-2","x-region":"us-west"}"#.to_string();
        let filters = vec![Expr::InList(InList::new(
            Box::new(Expr::Column(Column::from_name("request_headers"))),
            vec![
                Expr::Literal(ScalarValue::Utf8(Some(headers_1.clone())), None),
                Expr::Literal(ScalarValue::Utf8(Some(headers_2.clone())), None),
            ],
            false,
        ))];

        let partitions = provider.extract_partitions(&filters).expect("partitions");

        assert_eq!(partitions.len(), 2);
        assert_eq!(partitions[0], (None, None, None, Some(headers_1)));
        assert_eq!(partitions[1], (None, None, None, Some(headers_2)));
    }

    #[test]
    fn test_request_headers_filter_needs_enable() {
        let provider = base_provider();
        let filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("request_headers"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some(r#"{"x-sandbox-id":"sandbox-1"}"#.to_string())),
                None,
            )),
        })];

        let err = provider
            .extract_partitions(&filters)
            .expect_err("expected rejection");
        match err {
            DataFusionError::Plan(message) => {
                assert!(message.contains("request_header_filters"));
            }
            other => panic!("Unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_request_headers_filter_rejects_unallowlisted_header() {
        let provider = header_provider();
        let filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("request_headers"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some(r#"{"authorization":"secret"}"#.to_string())),
                None,
            )),
        })];

        let err = provider
            .extract_partitions(&filters)
            .expect_err("expected rejection");
        match err {
            DataFusionError::Plan(message) => {
                assert!(message.contains("request_header_allowlist"));
                assert!(message.contains("authorization"));
                assert!(!message.contains("secret"));
            }
            other => panic!("Unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_request_headers_filter_rejects_authorization_with_auth() {
        let provider = base_provider()
            .enable_header_filters(DEFAULT_MAX_HEADERS_LENGTH, vec!["authorization"])
            .expect("authorization should be allowlisted before auth is configured")
            .with_auth(Arc::new(TestAuthenticator));
        let filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("request_headers"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some(r#"{"authorization":"secret"}"#.to_string())),
                None,
            )),
        })];

        let err = provider
            .extract_partitions(&filters)
            .expect_err("expected rejection");
        match err {
            DataFusionError::Plan(message) => {
                assert!(message.contains("authorization"));
                assert!(message.contains("HTTP authentication"));
                assert!(!message.contains("secret"));
            }
            other => panic!("Unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_request_headers_filter_rejects_invalid_json() {
        let provider = header_provider();
        let filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("request_headers"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("not json".to_string())),
                None,
            )),
        })];

        let err = provider
            .extract_partitions(&filters)
            .expect_err("expected rejection");
        match err {
            DataFusionError::Plan(message) => {
                assert!(message.contains("JSON object"));
            }
            other => panic!("Unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_max_request_partitions_rejects_large_cross_product() {
        let provider = base_provider()
            .with_allowed_paths(vec!["/api/users".to_string(), "/api/posts".to_string()])
            .expect("allowed paths")
            .enable_query_filters(64)
            .with_max_request_partitions(Some(3));
        let filters = vec![
            Expr::InList(InList::new(
                Box::new(Expr::Column(Column::from_name("request_path"))),
                vec![
                    Expr::Literal(ScalarValue::Utf8(Some("/api/users".to_string())), None),
                    Expr::Literal(ScalarValue::Utf8(Some("/api/posts".to_string())), None),
                ],
                false,
            )),
            Expr::InList(InList::new(
                Box::new(Expr::Column(Column::from_name("request_query"))),
                vec![
                    Expr::Literal(ScalarValue::Utf8(Some("status=active".to_string())), None),
                    Expr::Literal(ScalarValue::Utf8(Some("status=inactive".to_string())), None),
                ],
                false,
            )),
        ];

        let err = provider
            .extract_partitions(&filters)
            .expect_err("expected partition cap rejection");
        match err {
            DataFusionError::Plan(message) => {
                assert!(message.contains("4 request partitions"));
                assert!(message.contains("max_request_partitions=3"));
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
        let key1 = HttpTableProvider::get_cache_key("/path", Some("query"), None, None);
        let key2 = HttpTableProvider::get_cache_key("/path", None, None, None);
        let key3 = HttpTableProvider::get_cache_key("/other", Some("query"), None, None);
        let key4 = HttpTableProvider::get_cache_key("/path", Some("query"), Some("body"), None);
        let key5 = HttpTableProvider::get_cache_key(
            "/path",
            Some("query"),
            Some("body"),
            Some(r#"{"x-sandbox-id":"sandbox-1"}"#),
        );
        let collision_candidate_1 =
            HttpTableProvider::get_cache_key("/path", Some("q&body=b"), Some(""), None);
        let collision_candidate_2 =
            HttpTableProvider::get_cache_key("/path", Some("q"), Some("b&body="), None);

        assert!(key1 == CacheKey::new("/path", Some("query"), None, None));
        assert!(key1 != key2);
        assert!(key1 != key3);
        assert!(key1 != key4);
        assert!(key4 != key5);
        assert!(collision_candidate_1 != collision_candidate_2);

        let redacted_label = key5.redacted_label();
        assert!(redacted_label.starts_with("http-cache-key:"));
        assert!(!redacted_label.contains("/path"));
        assert!(!redacted_label.contains("body"));
        assert!(!redacted_label.contains("sandbox-1"));
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

        assert_eq!(schema.fields().len(), 8);
        assert_eq!(schema.field(0).name(), "request_path");
        assert_eq!(schema.field(1).name(), "request_query");
        assert_eq!(schema.field(2).name(), "request_body");
        assert_eq!(schema.field(3).name(), "request_headers");
        assert_eq!(schema.field(4).name(), "content");
        assert_eq!(schema.field(5).name(), "response_status");
        assert_eq!(schema.field(6).name(), "response_headers");
        assert_eq!(schema.field(7).name(), "fetched_at");
        assert_eq!(*schema.field(0).data_type(), DataType::Utf8);
        assert_eq!(*schema.field(1).data_type(), DataType::Utf8);
        assert_eq!(*schema.field(2).data_type(), DataType::Utf8);
        assert_eq!(*schema.field(3).data_type(), DataType::Utf8);
        assert_eq!(*schema.field(4).data_type(), DataType::Utf8);
        assert_eq!(*schema.field(5).data_type(), DataType::UInt16);
        assert!(matches!(
            schema.field(6).data_type(),
            DataType::Map(_, false)
        ));
        assert_eq!(
            *schema.field(7).data_type(),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None)
        );
        assert!(!schema.field(0).is_nullable()); // request_path is not nullable
        assert!(schema.field(1).is_nullable()); // request_query is nullable
        assert!(schema.field(2).is_nullable()); // request_body is nullable
        assert!(schema.field(3).is_nullable()); // request_headers is nullable
        assert!(!schema.field(4).is_nullable()); // content is not nullable
        assert!(!schema.field(5).is_nullable()); // response_status is not nullable
        assert!(schema.field(6).is_nullable()); // response_headers is nullable
        assert!(schema.field(7).is_nullable()); // fetched_at is nullable
    }

    #[tokio::test]
    async fn test_fetch_and_create_batch_includes_response_headers() {
        let provider = Arc::new(base_provider());
        let request_headers = r#"{"x-sandbox-id":"sandbox-1"}"#.to_string();
        let fetch_result = HttpFetchResult {
            content: r#"[{"id":1},{"id":2}]"#.to_string(),
            max_age: Duration::from_secs(60),
            detected_format: "json".to_string(),
            response_date: None,
            response_status: 200,
            response_headers: vec![
                ("content-type".to_string(), "application/json".to_string()),
                ("x-request-id".to_string(), "req-123".to_string()),
            ],
        };
        provider
            .cache_response("/posts", None, None, Some(&request_headers), &fetch_result)
            .await;

        let exec = HttpExec::new(
            HttpTableProvider::base_table_schema().into(),
            Arc::clone(&provider),
            vec![(
                Some("/posts".to_string()),
                None,
                None,
                Some(request_headers.clone()),
            )],
            None,
        );

        let batch = exec
            .fetch_and_create_batch(provider.as_ref(), 0)
            .await
            .expect("batch should be created from cached response");

        assert_eq!(
            batch.num_rows(),
            2,
            "JSON array content should yield two rows"
        );

        let request_headers_col = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("request_headers should be a StringArray");
        assert_eq!(request_headers_col.value(0), request_headers);
        assert_eq!(request_headers_col.value(1), request_headers);

        let headers_col = batch
            .column(6)
            .as_any()
            .downcast_ref::<arrow::array::MapArray>()
            .expect("response_headers should be a MapArray");
        assert_eq!(headers_col.len(), 2);

        let keys = headers_col
            .keys()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("header keys should be a StringArray");
        let values = headers_col
            .values()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("header values should be a StringArray");

        assert_eq!(keys.len(), 4);
        assert_eq!(values.len(), 4);
        assert_eq!(keys.value(0), "content-type");
        assert_eq!(values.value(0), "application/json");
        assert_eq!(keys.value(1), "x-request-id");
        assert_eq!(values.value(1), "req-123");
        assert_eq!(keys.value(2), "content-type");
        assert_eq!(values.value(2), "application/json");
        assert_eq!(keys.value(3), "x-request-id");
        assert_eq!(values.value(3), "req-123");
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
        assert_eq!(
            partitions[0],
            (Some("/api/users".to_string()), None, None, None)
        );
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
            (Some("/api/v1/users/123".to_string()), None, None, None)
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
            (Some("/api/v1/users".to_string()), None, None, None)
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
        assert!(partitions.contains(&(Some("/api/users".to_string()), None, None, None)));
        assert!(partitions.contains(&(Some("/v1/search".to_string()), None, None, None)));
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
        assert_eq!(
            partitions[0],
            (Some("/api/users".to_string()), None, None, None)
        );
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

    /// Integration test: Open Library search API with query-parameter pagination.
    /// Uses `pagination_query_params` with `offset={offset}&limit={limit}` to
    /// paginate through search results, and `pagination_data_pointer` to extract
    /// the `docs` array from each page.
    #[tokio::test]
    async fn test_integration_openlibrary_query_param_pagination() {
        use datafusion::prelude::SessionContext;

        let url = Url::parse("https://openlibrary.org/search.json?q=tolkien").expect("valid URL");
        let provider = HttpTableProvider::new(url, Client::new(), "json".to_string(), false)
            .with_pagination(PaginationConfig {
                query_params: Some("offset={offset}&limit={limit}".to_string()),
                page_size: Some(3),
                data_pointer: Some("/docs".to_string()),
                max_pages: Some(2),
                use_link_header: false,
                ..Default::default()
            })
            .expect("pagination config");

        let ctx = SessionContext::new();
        ctx.register_table("books", Arc::new(provider))
            .expect("register table");

        let df = ctx
            .sql("SELECT content FROM books")
            .await
            .expect("query should succeed");

        let results = df.collect().await.expect("collect should succeed");
        let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();

        // With page_size=3 and max_pages=2, we expect up to 6 rows.
        // If the last page has fewer than 3 rows, we get fewer.
        assert!(
            total_rows >= 4,
            "Should have fetched multiple pages of results, got {total_rows}"
        );
        assert!(
            total_rows <= 6,
            "Should not exceed 2 pages * 3 items = 6 rows, got {total_rows}"
        );

        // Verify content looks like book records
        let content_col = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("content should be string array");
        let first_row = content_col.value(0);
        assert!(
            first_row.contains("title"),
            "Book records should contain a title field: {first_row}"
        );
    }

    // --- Pagination tests ---

    #[test]
    fn test_parse_link_header_next() {
        assert_eq!(
            parse_link_header_next(
                r#"<https://api.example.com/items?page=2>; rel="next", <https://api.example.com/items?page=1>; rel="prev""#
            ),
            Some("https://api.example.com/items?page=2".to_string())
        );

        assert_eq!(
            parse_link_header_next(r#"<https://api.example.com/items?page=3>; rel="next""#),
            Some("https://api.example.com/items?page=3".to_string())
        );

        // No rel="next"
        assert_eq!(
            parse_link_header_next(r#"<https://api.example.com/items?page=1>; rel="prev""#),
            None
        );

        // Empty header
        assert_eq!(parse_link_header_next(""), None);
    }

    #[test]
    fn test_extract_next_page_info_json_pointer_url() {
        let base_url = Url::parse("https://api.example.com").expect("valid URL");
        let config = PaginationConfig {
            next_pointer: Some("/next".to_string()),
            ..Default::default()
        };
        let content = r#"{"data": [1, 2], "next": "https://api.example.com/items?page=2"}"#;
        let headers = vec![];

        let result =
            extract_next_page_info(content, &headers, &config, &base_url).expect("should succeed");
        match result {
            Some(NextPageInfo::Url(url)) => {
                assert_eq!(url.as_str(), "https://api.example.com/items?page=2");
            }
            other => panic!("Expected Url, got: {other:?}"),
        }
    }

    #[test]
    fn test_extract_next_page_info_json_pointer_token() {
        let base_url = Url::parse("https://api.example.com").expect("valid URL");
        let config = PaginationConfig {
            next_pointer: Some("/cursor".to_string()),
            token_param: Some("cursor".to_string()),
            ..Default::default()
        };
        let content = r#"{"data": [1, 2], "cursor": "abc123"}"#;
        let headers = vec![];

        let result =
            extract_next_page_info(content, &headers, &config, &base_url).expect("should succeed");
        match result {
            Some(NextPageInfo::Token(token)) => {
                assert_eq!(token, "abc123");
            }
            other => panic!("Expected Token, got: {other:?}"),
        }
    }

    #[test]
    fn test_extract_next_page_info_null_means_no_more_pages() {
        let base_url = Url::parse("https://api.example.com").expect("valid URL");
        let config = PaginationConfig {
            next_pointer: Some("/next".to_string()),
            ..Default::default()
        };
        let content = r#"{"data": [1, 2], "next": null}"#;
        let headers = vec![];

        let result =
            extract_next_page_info(content, &headers, &config, &base_url).expect("should succeed");
        assert!(result.is_none(), "null next should mean no more pages");
    }

    #[test]
    fn test_extract_next_page_info_missing_pointer_means_no_more_pages() {
        let base_url = Url::parse("https://api.example.com").expect("valid URL");
        let config = PaginationConfig {
            next_pointer: Some("/next_url".to_string()),
            ..Default::default()
        };
        let content = r#"{"data": [1, 2]}"#;
        let headers = vec![];

        let result =
            extract_next_page_info(content, &headers, &config, &base_url).expect("should succeed");
        assert!(
            result.is_none(),
            "missing pointer should mean no more pages"
        );
    }

    #[test]
    fn test_extract_next_page_info_ssrf_protection() {
        let base_url = Url::parse("https://api.example.com").expect("valid URL");
        let config = PaginationConfig {
            next_pointer: Some("/next".to_string()),
            ..Default::default()
        };
        let content = r#"{"data": [1], "next": "https://evil.com/steal-data"}"#;
        let headers = vec![];

        let result = extract_next_page_info(content, &headers, &config, &base_url);
        assert!(result.is_err(), "should reject cross-origin next page URLs");
        let err_msg = result.expect_err("expected error").to_string();
        assert!(
            err_msg.contains("does not match"),
            "error should mention origin mismatch: {err_msg}"
        );
    }

    #[test]
    fn test_extract_next_page_info_link_header() {
        let base_url = Url::parse("https://api.example.com").expect("valid URL");
        let config = PaginationConfig {
            use_link_header: true,
            ..Default::default()
        };
        let content = r"[1, 2, 3]";
        let headers = vec![(
            "link".to_string(),
            r#"<https://api.example.com/items?page=2>; rel="next""#.to_string(),
        )];

        let result =
            extract_next_page_info(content, &headers, &config, &base_url).expect("should succeed");
        match result {
            Some(NextPageInfo::Url(url)) => {
                assert_eq!(url.as_str(), "https://api.example.com/items?page=2");
            }
            other => panic!("Expected Url from Link header, got: {other:?}"),
        }
    }

    #[test]
    fn test_extract_page_data_with_data_pointer() {
        let config = PaginationConfig {
            data_pointer: Some("/results".to_string()),
            ..Default::default()
        };
        let content = r#"{"results": [{"id": 1}, {"id": 2}], "next": "url"}"#;
        let rows = extract_page_data(content, &config, None).expect("should extract page data");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], r#"{"id":1}"#);
        assert_eq!(rows[1], r#"{"id":2}"#);
    }

    #[test]
    fn test_extract_page_data_with_limit() {
        let config = PaginationConfig {
            data_pointer: Some("/items".to_string()),
            ..Default::default()
        };
        let content = r#"{"items": [{"id": 1}, {"id": 2}, {"id": 3}]}"#;
        let rows = extract_page_data(content, &config, Some(2)).expect("should extract page data");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_extract_page_data_without_data_pointer() {
        let config = PaginationConfig::default();
        let content = r#"[{"id": 1}, {"id": 2}]"#;
        let rows = extract_page_data(content, &config, None).expect("should extract page data");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_extract_page_data_missing_pointer() {
        let config = PaginationConfig {
            data_pointer: Some("/nonexistent".to_string()),
            ..Default::default()
        };
        let content = r#"{"results": [1, 2, 3]}"#;
        let result = extract_page_data(content, &config, None);
        assert!(result.is_err(), "missing pointer should return error");
    }

    #[test]
    fn test_build_query_with_token_no_existing() {
        let result = build_query_with_token(None, "cursor", "abc123");
        assert_eq!(result, "cursor=abc123");
    }

    #[test]
    fn test_build_query_with_token_append() {
        let result = build_query_with_token(Some("sort=date&limit=10"), "cursor", "abc123");
        assert_eq!(result, "sort=date&limit=10&cursor=abc123");
    }

    #[test]
    fn test_build_query_with_token_replace() {
        let result =
            build_query_with_token(Some("sort=date&cursor=old_token"), "cursor", "new_token");
        assert_eq!(result, "sort=date&cursor=new_token");
    }

    #[test]
    fn test_pagination_config_validation_requires_next_source() {
        let provider = base_provider();
        let result = provider.with_pagination(PaginationConfig {
            use_link_header: false,
            ..Default::default()
        });
        assert!(
            result.is_err(),
            "should require next_pointer or link_header"
        );
    }

    #[test]
    fn test_pagination_default_enables_link_header() {
        let provider = base_provider();
        let result = provider.with_pagination(PaginationConfig::default());
        assert!(
            result.is_ok(),
            "default config with link_header=true should be valid"
        );
    }

    #[test]
    fn test_pagination_config_validation_token_needs_pointer() {
        let provider = base_provider();
        let result = provider.with_pagination(PaginationConfig {
            use_link_header: true,
            token_param: Some("cursor".to_string()),
            ..Default::default()
        });
        assert!(
            result.is_err(),
            "token_param without next_pointer should fail"
        );
    }

    #[test]
    fn test_pagination_config_valid_with_next_pointer() {
        let provider = base_provider();
        let result = provider.with_pagination(PaginationConfig {
            next_pointer: Some("/next".to_string()),
            ..Default::default()
        });
        assert!(result.is_ok(), "should accept valid pagination config");
        assert!(
            result.expect("valid config").is_paginated(),
            "should report as paginated"
        );
    }

    #[test]
    fn test_pagination_config_valid_with_link_header() {
        let provider = base_provider();
        let result = provider.with_pagination(PaginationConfig {
            use_link_header: true,
            ..Default::default()
        });
        assert!(result.is_ok(), "should accept link_header pagination");
    }

    #[test]
    fn test_pagination_config_valid_with_no_max_pages_limit() {
        let provider = base_provider();
        let result = provider.with_pagination(PaginationConfig {
            next_pointer: Some("/next".to_string()),
            use_link_header: false,
            max_pages: None,
            ..Default::default()
        });
        assert!(result.is_ok(), "should accept no max-pages limit");
    }

    #[test]
    fn test_pagination_config_rejects_zero_max_pages() {
        let provider = base_provider();
        let error = provider
            .with_pagination(PaginationConfig {
                next_pointer: Some("/next".to_string()),
                use_link_header: false,
                max_pages: Some(0),
                ..Default::default()
            })
            .expect_err("zero max_pages should fail");
        match error {
            Error::Configuration { message } => {
                assert!(
                    message.contains("pagination_max_pages"),
                    "error should mention pagination_max_pages: {message}"
                );
            }
            other => panic!("Unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_pagination_rejects_repeated_request_url() {
        let mut state = PaginationState {
            page: 1,
            next_info: None,
            rows_fetched: 0,
            path: None,
            query: None,
            body: None,
            request_headers: None,
            limit: None,
            done: false,
            last_page_path: None,
            last_page_query: None,
            recent_page_urls: VecDeque::new(),
        };
        let url =
            Url::parse("https://api.example.com/items?page=2").expect("test URL should be valid");

        record_pagination_request_url(&mut state, &url).expect("first request should be accepted");
        let error = record_pagination_request_url(&mut state, &url)
            .expect_err("repeated request should fail");

        match error {
            Error::Pagination { message } => {
                assert!(
                    message.contains("repeated next page request"),
                    "error should mention repeated pagination request: {message}"
                );
            }
            other => panic!("Unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_pagination_repeated_request_tracking_is_bounded() {
        let mut state = PaginationState {
            page: 1,
            next_info: None,
            rows_fetched: 0,
            path: None,
            query: None,
            body: None,
            request_headers: None,
            limit: None,
            done: false,
            last_page_path: None,
            last_page_query: None,
            recent_page_urls: VecDeque::new(),
        };

        for page in 0..=PAGINATION_REPEAT_DETECTION_WINDOW {
            let url = Url::parse(&format!("https://api.example.com/items?page={page}"))
                .expect("test URL should be valid");
            record_pagination_request_url(&mut state, &url)
                .expect("unique request should be accepted");
        }

        assert_eq!(
            state.recent_page_urls.len(),
            PAGINATION_REPEAT_DETECTION_WINDOW,
            "recent URL tracking should be capped"
        );

        let evicted_url =
            Url::parse("https://api.example.com/items?page=0").expect("test URL should be valid");
        record_pagination_request_url(&mut state, &evicted_url)
            .expect("evicted URL should not be retained forever");
    }

    async fn start_query_param_pagination_server(stop_offset: usize) -> (Url, Arc<AtomicUsize>) {
        use std::sync::atomic::Ordering;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("mock server should bind");
        let address = listener.local_addr().expect("mock server should have addr");
        let request_count = Arc::new(AtomicUsize::new(0));
        let request_count_for_server = Arc::clone(&request_count);

        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let request_count = Arc::clone(&request_count_for_server);

                tokio::spawn(async move {
                    let mut buffer = [0_u8; 1024];
                    let bytes_read = stream.read(&mut buffer).await.unwrap_or(0);
                    request_count.fetch_add(1, Ordering::SeqCst);

                    let request = String::from_utf8_lossy(&buffer[..bytes_read]);
                    let request_target = request
                        .lines()
                        .next()
                        .and_then(|line| line.split_whitespace().nth(1))
                        .unwrap_or("/");
                    let request_url = Url::parse(&format!("http://localhost{request_target}"))
                        .expect("request target should form a valid URL");
                    let offset = request_url
                        .query_pairs()
                        .find_map(|(key, value)| {
                            (key == "offset")
                                .then(|| value.parse::<usize>().ok())
                                .flatten()
                        })
                        .unwrap_or(0);

                    let body = if offset < stop_offset {
                        format!(r#"{{"docs":[{{"id":{offset}}}]}}"#)
                    } else {
                        r#"{"docs":[]}"#.to_string()
                    };
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                });
            }
        });

        (
            Url::parse(&format!("http://{address}/items")).expect("mock URL should be valid"),
            request_count,
        )
    }

    #[tokio::test]
    async fn test_pagination_without_max_pages_fetches_past_default_limit() {
        use datafusion::prelude::SessionContext;
        use std::sync::atomic::Ordering;

        let rows_to_return = DEFAULT_PAGINATION_MAX_PAGES + 5;
        let (base_url, request_count) = start_query_param_pagination_server(rows_to_return).await;
        let provider = HttpTableProvider::new(base_url, Client::new(), "json".to_string(), false)
            .with_pagination(PaginationConfig {
                query_params: Some("offset={offset}&limit={limit}".to_string()),
                page_size: Some(1),
                data_pointer: Some("/docs".to_string()),
                max_pages: None,
                use_link_header: false,
                ..Default::default()
            })
            .expect("pagination config should be valid");

        let ctx = SessionContext::new();
        ctx.register_table("items", Arc::new(provider))
            .expect("table should register");

        let results = ctx
            .sql("SELECT content FROM items")
            .await
            .expect("query should plan")
            .collect()
            .await
            .expect("query should execute");

        let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(
            total_rows, rows_to_return,
            "unlimited pagination should fetch beyond the default safety limit"
        );
        assert!(
            request_count.load(Ordering::SeqCst) > DEFAULT_PAGINATION_MAX_PAGES,
            "execution should request pages beyond the old safety limit"
        );
    }

    #[test]
    fn test_extract_next_page_info_nested_pointer() {
        let base_url = Url::parse("https://api.example.com").expect("valid URL");
        let config = PaginationConfig {
            next_pointer: Some("/pagination/next_url".to_string()),
            ..Default::default()
        };
        let content = r#"{"data": [], "pagination": {"next_url": "https://api.example.com/items?offset=20"}}"#;
        let headers = vec![];

        let result =
            extract_next_page_info(content, &headers, &config, &base_url).expect("should succeed");
        match result {
            Some(NextPageInfo::Url(url)) => {
                assert_eq!(url.as_str(), "https://api.example.com/items?offset=20");
            }
            other => panic!("Expected Url from nested pointer, got: {other:?}"),
        }
    }

    #[test]
    fn test_extract_next_page_info_empty_string_means_no_more_pages() {
        let base_url = Url::parse("https://api.example.com").expect("valid URL");
        let config = PaginationConfig {
            next_pointer: Some("/next".to_string()),
            ..Default::default()
        };
        let content = r#"{"data": [1], "next": ""}"#;
        let headers = vec![];

        let result =
            extract_next_page_info(content, &headers, &config, &base_url).expect("should succeed");
        assert!(
            result.is_none(),
            "empty string next should mean no more pages"
        );
    }

    #[test]
    fn test_extract_next_page_info_relative_url() {
        let base_url = Url::parse("https://api.example.com/v1").expect("valid URL");
        let config = PaginationConfig {
            next_pointer: Some("/next".to_string()),
            ..Default::default()
        };
        let content = r#"{"data": [1], "next": "/v1/items?page=2"}"#;
        let headers = vec![];

        let result =
            extract_next_page_info(content, &headers, &config, &base_url).expect("should succeed");
        match result {
            Some(NextPageInfo::Url(url)) => {
                assert_eq!(url.as_str(), "https://api.example.com/v1/items?page=2");
            }
            other => panic!("Expected Url from relative path, got: {other:?}"),
        }
    }

    #[test]
    fn test_extract_next_page_info_relative_link_header() {
        let base_url = Url::parse("https://api.example.com").expect("valid URL");
        let config = PaginationConfig {
            use_link_header: true,
            ..Default::default()
        };
        let content = r"[1, 2]";
        let headers = vec![(
            "link".to_string(),
            r#"</items?page=3>; rel="next""#.to_string(),
        )];

        let result =
            extract_next_page_info(content, &headers, &config, &base_url).expect("should succeed");
        match result {
            Some(NextPageInfo::Url(url)) => {
                assert_eq!(url.as_str(), "https://api.example.com/items?page=3");
            }
            other => panic!("Expected Url from relative Link header, got: {other:?}"),
        }
    }

    #[test]
    fn test_extract_next_page_info_pointer_missing_falls_through_to_link_header() {
        let base_url = Url::parse("https://api.example.com").expect("valid URL");
        let config = PaginationConfig {
            next_pointer: Some("/pagination/next_url".to_string()),
            use_link_header: true,
            ..Default::default()
        };
        // Response has no /pagination/next_url field, but does have a Link header
        let content = r#"{"data": [1, 2]}"#;
        let headers = vec![(
            "link".to_string(),
            r#"<https://api.example.com/items?page=2>; rel="next""#.to_string(),
        )];

        let result =
            extract_next_page_info(content, &headers, &config, &base_url).expect("should succeed");
        match result {
            Some(NextPageInfo::Url(url)) => {
                assert_eq!(
                    url.as_str(),
                    "https://api.example.com/items?page=2",
                    "should fall through to Link header when pointer is missing"
                );
            }
            other => panic!("Expected Url from Link header fallthrough, got: {other:?}"),
        }
    }

    #[test]
    fn test_build_query_with_token_special_chars() {
        // Tokens with special characters should be properly percent-encoded
        let result = build_query_with_token(None, "cursor", "abc 123&foo=bar");
        // url::form_urlencoded encodes spaces as + and & as %26
        assert!(
            result.contains("cursor="),
            "should contain cursor param: {result}"
        );
        assert!(
            !result.contains("&foo="),
            "special chars in token should be encoded, not treated as params: {result}"
        );
    }

    #[test]
    fn test_extract_next_page_info_json_parse_error() {
        let base_url = Url::parse("https://api.example.com").expect("valid URL");
        let config = PaginationConfig {
            next_pointer: Some("/next".to_string()),
            ..Default::default()
        };
        // When next_pointer is set but parsed_json is None, it should error
        let headers = vec![];

        let result = super::extract_next_page_info(None, &headers, &config, &base_url, 0);
        assert!(
            result.is_err(),
            "missing parsed JSON should return error when next_pointer is configured"
        );
    }

    #[test]
    fn test_extract_next_page_info_numeric_pointer_as_token() {
        let base_url = Url::parse("https://api.example.com").expect("valid URL");
        let config = PaginationConfig {
            next_pointer: Some("/page".to_string()),
            token_param: Some("page".to_string()),
            ..Default::default()
        };
        let content = r#"{"data": [1, 2], "page": 3}"#;
        let headers = vec![];

        let result =
            extract_next_page_info(content, &headers, &config, &base_url).expect("should succeed");
        match result {
            Some(NextPageInfo::Token(token)) => {
                assert_eq!(token, "3");
            }
            other => panic!("Expected Token with numeric value, got: {other:?}"),
        }
    }

    #[test]
    fn test_extract_next_page_info_numeric_pointer_without_token_param_errors() {
        let base_url = Url::parse("https://api.example.com").expect("valid URL");
        let config = PaginationConfig {
            next_pointer: Some("/page".to_string()),
            // No token_param — numeric values should error in URL mode
            ..Default::default()
        };
        let content = r#"{"data": [1, 2], "page": 3}"#;
        let headers = vec![];

        let result = extract_next_page_info(content, &headers, &config, &base_url);
        assert!(
            result.is_err(),
            "numeric pointer without token_param should error"
        );
    }

    #[test]
    fn test_extract_next_page_info_non_string_non_number_pointer_value_errors() {
        let base_url = Url::parse("https://api.example.com").expect("valid URL");
        let config = PaginationConfig {
            next_pointer: Some("/next".to_string()),
            ..Default::default()
        };
        // Boolean value is not a valid pagination pointer
        let content = r#"{"next": true}"#;
        let headers = vec![];

        let result = extract_next_page_info(content, &headers, &config, &base_url);
        assert!(
            result.is_err(),
            "non-string/non-number pointer value should return error"
        );
    }

    #[test]
    fn test_extract_page_data_missing_json() {
        let config = PaginationConfig {
            data_pointer: Some("/results".to_string()),
            ..Default::default()
        };
        // When data_pointer is set but parsed_json is None, it should error
        let result = super::extract_page_data("", None, &config, None);
        assert!(
            result.is_err(),
            "missing parsed JSON should return error when data_pointer is configured"
        );
    }

    #[test]
    fn test_with_pagination_invalid_next_pointer() {
        let provider = HttpTableProvider::new(
            Url::parse("https://example.com").expect("valid URL"),
            Client::new(),
            "json".to_string(),
            false,
        );
        let result = provider.with_pagination(PaginationConfig {
            next_pointer: Some("next".to_string()),
            use_link_header: false,
            ..Default::default()
        });
        assert!(
            result.is_err(),
            "next_pointer without leading '/' should fail"
        );
    }

    #[test]
    fn test_with_pagination_invalid_data_pointer() {
        let provider = HttpTableProvider::new(
            Url::parse("https://example.com").expect("valid URL"),
            Client::new(),
            "json".to_string(),
            false,
        );
        let result = provider.with_pagination(PaginationConfig {
            next_pointer: Some("/next".to_string()),
            data_pointer: Some("results".to_string()),
            use_link_header: false,
            ..Default::default()
        });
        assert!(
            result.is_err(),
            "data_pointer without leading '/' should fail"
        );
    }

    #[test]
    fn test_parse_link_header_unquoted_rel() {
        assert_eq!(
            parse_link_header_next("<https://api.example.com/items?page=2>; rel=next"),
            Some("https://api.example.com/items?page=2".to_string())
        );
    }

    #[test]
    fn test_parse_link_header_multi_rel() {
        // rel with multiple values: "next prev"
        assert_eq!(
            parse_link_header_next(r#"<https://api.example.com/items?page=2>; rel="next prev""#),
            Some("https://api.example.com/items?page=2".to_string())
        );
    }

    #[test]
    fn test_parse_link_header_uri_with_comma() {
        // URI containing a comma inside <...> must not be split
        assert_eq!(
            parse_link_header_next(r#"<https://api.example.com/items?a=1,2&page=2>; rel="next""#),
            Some("https://api.example.com/items?a=1,2&page=2".to_string())
        );

        // Multiple links where the first URI contains a comma
        assert_eq!(
            parse_link_header_next(
                r#"<https://api.example.com/items?x=a,b>; rel="prev", <https://api.example.com/items?page=3>; rel="next""#
            ),
            Some("https://api.example.com/items?page=3".to_string())
        );
    }

    #[test]
    fn test_split_link_header_top_level_basic() {
        // Comma outside angle brackets splits normally
        let result = split_link_header_top_level("<a>; rel=prev, <b>; rel=next", ',');
        assert_eq!(result, vec!["<a>; rel=prev", "<b>; rel=next"]);

        // Comma inside angle brackets is preserved
        let result = split_link_header_top_level("<a?x=1,2>; rel=next", ',');
        assert_eq!(result, vec!["<a?x=1,2>; rel=next"]);

        // Semicolons inside quoted strings are not split
        let result = split_link_header_top_level(r#"<a>; title="a;b"; rel=next"#, ';');
        assert_eq!(result, vec!["<a>", r#"title="a;b""#, "rel=next"]);
    }

    #[test]
    fn test_split_link_header_top_level_escaped_quotes() {
        // Escaped quote inside a quoted string should not close the string
        let result =
            split_link_header_top_level(r#"<a>; title="has \"escaped\" quotes"; rel=next"#, ';');
        assert_eq!(
            result,
            vec!["<a>", r#"title="has \"escaped\" quotes""#, "rel=next"]
        );
    }

    #[test]
    fn test_split_link_header_top_level_empty_and_single() {
        assert_eq!(split_link_header_top_level("", ','), vec![""]);
        assert_eq!(
            split_link_header_top_level("<a>; rel=next", ','),
            vec!["<a>; rel=next"]
        );
    }

    #[test]
    fn test_parse_link_header_case_insensitive_rel() {
        // REL and NEXT should be matched case-insensitively
        assert_eq!(
            parse_link_header_next(r#"<https://api.example.com/page2>; REL="NEXT""#),
            Some("https://api.example.com/page2".to_string())
        );

        assert_eq!(
            parse_link_header_next("<https://api.example.com/page2>; Rel=Next"),
            Some("https://api.example.com/page2".to_string())
        );
    }

    #[test]
    fn test_parse_link_header_extra_params() {
        // Link with additional params like type and title
        assert_eq!(
            parse_link_header_next(
                r#"<https://api.example.com/page2>; rel="next"; type="application/json"; title="Next Page""#
            ),
            Some("https://api.example.com/page2".to_string())
        );
    }

    #[test]
    fn test_parse_link_header_whitespace_variations() {
        // No spaces around semicolons
        assert_eq!(
            parse_link_header_next(r#"<https://api.example.com/page2>;rel="next""#),
            Some("https://api.example.com/page2".to_string())
        );

        // Extra whitespace
        assert_eq!(
            parse_link_header_next(r#"  <https://api.example.com/page2> ;  rel="next"  "#),
            Some("https://api.example.com/page2".to_string())
        );
    }

    #[test]
    fn test_parse_link_header_malformed() {
        // Missing angle brackets
        assert_eq!(
            parse_link_header_next(r#"https://api.example.com/page2; rel="next""#),
            None
        );

        // No rel param at all
        assert_eq!(
            parse_link_header_next(r#"<https://api.example.com/page2>; type="text/html""#),
            None
        );

        // rel="last" only
        assert_eq!(
            parse_link_header_next(r#"<https://api.example.com/page2>; rel="last""#),
            None
        );
    }

    #[test]
    fn test_parse_link_header_next_is_second_link() {
        // rel="next" is on the second link, not the first
        assert_eq!(
            parse_link_header_next(
                r#"<https://api.example.com/page1>; rel="first", <https://api.example.com/page2>; rel="next", <https://api.example.com/page99>; rel="last""#
            ),
            Some("https://api.example.com/page2".to_string())
        );
    }

    #[test]
    fn test_parse_link_header_semicolon_in_quoted_title() {
        // Semicolons inside quoted title param must not break parsing
        assert_eq!(
            parse_link_header_next(
                r#"<https://api.example.com/page2>; title="Page; 2"; rel="next""#
            ),
            Some("https://api.example.com/page2".to_string())
        );
    }

    #[test]
    fn test_merge_queries_all_sources() {
        let result = merge_queries(
            Some("api_key=secret"),
            Some("filter=active"),
            "cursor",
            "abc123",
        );
        assert!(
            result.contains("api_key=secret"),
            "should include base URL params: {result}"
        );
        assert!(
            result.contains("filter=active"),
            "should include partition params: {result}"
        );
        assert!(
            result.contains("cursor=abc123"),
            "should include token: {result}"
        );
    }

    #[test]
    fn test_merge_queries_partition_overrides_base() {
        let result = merge_queries(
            Some("page=1&api_key=secret"),
            Some("page=5"),
            "cursor",
            "abc",
        );
        // "page" from partition should override base
        let page_count = result.matches("page=").count();
        assert_eq!(
            page_count, 1,
            "partition should override base param, got: {result}"
        );
        assert!(
            result.contains("page=5"),
            "partition value should win: {result}"
        );
    }

    #[test]
    fn test_merge_queries_no_base() {
        let result = merge_queries(None, Some("filter=active"), "cursor", "abc123");
        assert!(
            result.contains("filter=active"),
            "should include partition params: {result}"
        );
        assert!(
            result.contains("cursor=abc123"),
            "should include token: {result}"
        );
    }

    #[test]
    fn test_merge_queries_no_partition() {
        let result = merge_queries(Some("api_key=secret"), None, "cursor", "abc123");
        assert!(
            result.contains("api_key=secret"),
            "should include base params: {result}"
        );
        assert!(
            result.contains("cursor=abc123"),
            "should include token: {result}"
        );
    }

    #[test]
    fn test_merge_base_and_partition_queries() {
        // Both present — partition overrides base
        let result =
            merge_base_and_partition_queries(Some("api_key=secret&page=1"), Some("page=2"));
        let result = result.expect("should return Some");
        assert!(result.contains("api_key=secret"), "base param: {result}");
        assert!(result.contains("page=2"), "partition override: {result}");
        assert_eq!(
            result.matches("page=").count(),
            1,
            "no duplicates: {result}"
        );

        // Only base
        let result = merge_base_and_partition_queries(Some("api_key=secret"), None);
        let result = result.expect("should return Some");
        assert!(result.contains("api_key=secret"), "base only: {result}");

        // Only partition
        let result = merge_base_and_partition_queries(None, Some("filter=active"));
        let result = result.expect("should return Some");
        assert!(result.contains("filter=active"), "partition only: {result}");

        // Neither
        assert!(
            merge_base_and_partition_queries(None, None).is_none(),
            "both None should return None"
        );
    }

    // --- Tests for data_map_to_array ---

    #[test]
    fn test_extract_page_data_map_to_array() {
        let content = r#"{"data": {"1": {"id": "1", "name": "a"}, "2": {"id": "2", "name": "b"}}}"#;
        let config = PaginationConfig {
            data_pointer: Some("/data".to_string()),
            data_map_to_array: true,
            ..Default::default()
        };
        let rows = extract_page_data(content, &config, None).expect("should extract");
        assert_eq!(rows.len(), 2);
        // Values should be the inner objects
        for row in &rows {
            assert!(row.contains("\"id\""), "row should contain id: {row}");
        }
    }

    #[test]
    fn test_extract_page_data_map_to_array_disabled() {
        let content = r#"{"data": {"1": {"id": "1"}, "2": {"id": "2"}}}"#;
        let config = PaginationConfig {
            data_pointer: Some("/data".to_string()),
            data_map_to_array: false,
            ..Default::default()
        };
        let rows = extract_page_data(content, &config, None).expect("should extract");
        // Without map_to_array, the object is returned as a single row
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn test_parse_content_map_to_array() {
        let content = r#"{"1": {"id": "1"}, "2": {"id": "2"}, "3": {"id": "3"}}"#;
        let rows = parse_content_with_map_to_array(content, None, true);
        assert_eq!(rows.len(), 3);
        for row in &rows {
            assert!(row.contains("\"id\""), "row should contain id: {row}");
        }

        // With limit
        let rows = parse_content_with_map_to_array(content, Some(2), true);
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_parse_content_map_to_array_disabled_is_single_row() {
        let content = r#"{"1": {"id": "1"}, "2": {"id": "2"}}"#;
        let rows = parse_content_with_map_to_array(content, None, false);
        assert_eq!(rows.len(), 1, "without flag, object is a single row");
    }

    #[test]
    fn test_parse_content_map_to_array_array_still_works() {
        let content = r#"[{"id": 1}, {"id": 2}]"#;
        let rows = parse_content_with_map_to_array(content, None, true);
        assert_eq!(rows.len(), 2, "array input still works with flag enabled");
    }

    // --- Tests for query_params pagination ---

    #[test]
    fn test_expand_query_params_template() {
        let result = expand_query_params_template("offset={offset}&limit={limit}", 0, 100)
            .expect("page 0 should not overflow");
        assert_eq!(result, "offset=0&limit=100");

        let result = expand_query_params_template("offset={offset}&limit={limit}", 3, 50)
            .expect("page 3 should not overflow");
        assert_eq!(result, "offset=150&limit=50");

        let result = expand_query_params_template("page={page}&size={limit}", 2, 25)
            .expect("page 2 should not overflow");
        assert_eq!(result, "page=2&size=25");

        // Overflow should return an error
        expand_query_params_template("offset={offset}", usize::MAX, 2)
            .expect_err("usize::MAX * 2 should overflow");
    }

    #[test]
    fn test_pagination_config_query_params_requires_page_size() {
        let config = PaginationConfig {
            query_params: Some("offset={offset}&limit={limit}".to_string()),
            page_size: None,
            use_link_header: false,
            ..Default::default()
        };
        let err = base_provider()
            .with_pagination(config)
            .expect_err("should fail without page_size");
        match err {
            Error::Configuration { message } => {
                assert!(
                    message.contains("pagination_page_size"),
                    "error should mention page_size: {message}"
                );
            }
            other => panic!("Unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_pagination_config_query_params_mutually_exclusive_with_token() {
        let config = PaginationConfig {
            query_params: Some("offset={offset}&limit={limit}".to_string()),
            page_size: Some(100),
            token_param: Some("cursor".to_string()),
            next_pointer: Some("/next".to_string()),
            use_link_header: false,
            ..Default::default()
        };
        let err = base_provider()
            .with_pagination(config)
            .expect_err("should fail with both query_params and token_param");
        match err {
            Error::Configuration { message } => {
                assert!(
                    message.contains("mutually exclusive"),
                    "error should mention mutual exclusion: {message}"
                );
            }
            other => panic!("Unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_pagination_config_query_params_valid() {
        let config = PaginationConfig {
            query_params: Some("offset={offset}&limit={limit}".to_string()),
            page_size: Some(100),
            use_link_header: false,
            ..Default::default()
        };
        base_provider()
            .with_pagination(config)
            .expect("should succeed with query_params and page_size");
    }

    #[test]
    fn test_pagination_config_page_size_requires_query_params() {
        let config = PaginationConfig {
            page_size: Some(100),
            ..Default::default()
        };
        let err = base_provider()
            .with_pagination(config)
            .expect_err("should fail with page_size but no query_params");
        match err {
            Error::Configuration { message } => {
                assert!(
                    message.contains("pagination_query_params"),
                    "error should mention query_params: {message}"
                );
            }
            other => panic!("Unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_pagination_config_query_params_requires_pagination_variable() {
        let config = PaginationConfig {
            query_params: Some("limit=100".to_string()),
            page_size: Some(100),
            use_link_header: false,
            ..Default::default()
        };
        let err = base_provider()
            .with_pagination(config)
            .expect_err("should fail without pagination variable");
        match err {
            Error::Configuration { message } => {
                assert!(
                    message.contains("{offset}") || message.contains("{page}"),
                    "error should mention pagination variables: {message}"
                );
            }
            other => panic!("Unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_extract_next_page_info_query_params_mode() {
        let config = PaginationConfig {
            query_params: Some("offset={offset}&limit={limit}".to_string()),
            page_size: Some(100),
            use_link_header: false,
            ..Default::default()
        };
        let base_url = Url::parse("https://api.example.com").expect("valid URL");
        let result =
            extract_next_page_info_at_page("{}", &[], &config, &base_url, 2).expect("should work");
        match result {
            Some(NextPageInfo::QueryParams { page }) => {
                assert_eq!(page, 3);
            }
            other => panic!("Expected QueryParams, got: {other:?}"),
        }
    }

    #[test]
    fn test_merge_base_and_partition_queries_with_override() {
        // Override replaces existing keys
        let result = merge_base_and_partition_queries_with_override(
            Some("api_key=secret&offset=0"),
            None,
            "offset=100&limit=50",
        );
        assert!(
            result.contains("api_key=secret"),
            "base param kept: {result}"
        );
        assert!(result.contains("offset=100"), "offset overridden: {result}");
        assert!(result.contains("limit=50"), "limit added: {result}");
        assert_eq!(
            result.matches("offset=").count(),
            1,
            "no duplicates: {result}"
        );
    }

    fn nested_exec(column_order: &[&str], json_field: &str) -> (HttpExec, HttpJsonNesting) {
        let nesting = HttpJsonNesting::new(
            column_order.iter().map(|s| (*s).to_string()).collect(),
            json_field.to_string(),
            std::collections::HashSet::new(),
        );
        let provider = Arc::new(
            base_provider().with_json_nesting(nesting.clone(), nesting_schema_utf8(&nesting)),
        );
        let schema = provider.schema();
        let exec = HttpExec::new(schema, provider, vec![(None, None, None, None)], None);
        (exec, nesting)
    }

    fn empty_fetch_result() -> HttpFetchResult {
        HttpFetchResult {
            content: String::new(),
            max_age: std::time::Duration::from_secs(0),
            detected_format: "application/json".to_string(),
            response_date: None,
            response_status: 200,
            response_headers: Vec::new(),
        }
    }

    /// Like `nested_exec`, but accepts an explicit Arrow schema so a
    /// caller can mix typed and Utf8 fields. Used to verify that
    /// `create_batch_from_rows_nested` casts decomposed JSON values to
    /// the declared field types.
    fn nested_exec_with_schema(nesting: HttpJsonNesting, schema: SchemaRef) -> HttpExec {
        let provider = Arc::new(base_provider().with_json_nesting(nesting, schema));
        let schema = provider.schema();
        HttpExec::new(schema, provider, vec![(None, None, None, None)], None)
    }

    fn string_col(batch: &RecordBatch, name: &str) -> Vec<Option<String>> {
        let idx = batch
            .schema()
            .index_of(name)
            .expect("column should exist in batch");
        let arr = batch
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("column should be StringArray");
        (0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i).to_string())
                }
            })
            .collect()
    }

    #[test]
    fn create_batch_from_rows_nested_casts_typed_columns() {
        // Mixed schema: id is Int64 (was Utf8 before typed-json_nest
        // support), name is Utf8 (default), completed is Boolean,
        // details is the catch-all Utf8 JSON column.
        let nesting = HttpJsonNesting::new(
            vec![
                "id".to_string(),
                "name".to_string(),
                "completed".to_string(),
                "details".to_string(),
            ],
            "details".to_string(),
            std::collections::HashSet::new(),
        );
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("completed", DataType::Boolean, true),
            Field::new("details", DataType::Utf8, true),
        ]));
        let exec = nested_exec_with_schema(nesting.clone(), schema);
        let rows = vec![
            r#"{"id":1,"name":"alpha","completed":true,"extra":"x"}"#.to_string(),
            r#"{"id":2,"name":"beta","completed":false,"k":42}"#.to_string(),
        ];
        let batch = exec
            .create_batch_from_rows_nested(
                None,
                None,
                None,
                None,
                &rows,
                &empty_fetch_result(),
                &nesting,
            )
            .expect("batch should be created with cast columns");
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 4);
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);
        assert_eq!(batch.schema().field(2).data_type(), &DataType::Boolean);

        let id = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .expect("id should be Int64");
        assert_eq!(id.value(0), 1);
        assert_eq!(id.value(1), 2);

        let completed = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .expect("completed should be Boolean");
        assert!(completed.value(0));
        assert!(!completed.value(1));

        // Catch-all stays Utf8 with the residual JSON.
        let details = string_col(&batch, "details");
        assert_eq!(details[0].as_deref(), Some(r#"{"extra":"x"}"#));
        assert_eq!(details[1].as_deref(), Some(r#"{"k":42}"#));
    }

    #[test]
    fn create_batch_from_rows_nested_decomposes_object_rows() {
        let (exec, nesting) = nested_exec(&["id", "name", "details"], "details");
        let rows = vec![
            r#"{"id":"1","name":"alpha","extra":"x"}"#.to_string(),
            r#"{"id":"2","name":"beta","k":42}"#.to_string(),
        ];
        let batch = exec
            .create_batch_from_rows_nested(
                None,
                None,
                None,
                None,
                &rows,
                &empty_fetch_result(),
                &nesting,
            )
            .expect("batch should be created");
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);
        assert_eq!(
            string_col(&batch, "id"),
            vec![Some("1".to_string()), Some("2".to_string())]
        );
        assert_eq!(
            string_col(&batch, "name"),
            vec![Some("alpha".to_string()), Some("beta".to_string())]
        );
        let details = string_col(&batch, "details");
        assert_eq!(details[0].as_deref(), Some(r#"{"extra":"x"}"#));
        assert_eq!(details[1].as_deref(), Some(r#"{"k":42}"#));
    }

    #[test]
    fn create_batch_from_rows_nested_missing_key_is_null() {
        let (exec, nesting) = nested_exec(&["id", "name", "details"], "details");
        let rows = vec![r#"{"id":"1"}"#.to_string()];
        let batch = exec
            .create_batch_from_rows_nested(
                None,
                None,
                None,
                None,
                &rows,
                &empty_fetch_result(),
                &nesting,
            )
            .expect("batch should be created");
        assert_eq!(string_col(&batch, "id"), vec![Some("1".to_string())]);
        assert_eq!(string_col(&batch, "name"), vec![None]);
        assert_eq!(string_col(&batch, "details"), vec![None]);
    }

    #[test]
    fn create_batch_from_rows_nested_missing_keys_become_null() {
        let (exec, nesting) = nested_exec(&["id", "name", "details"], "details");
        let rows = vec![
            r#"{"id":"1","extra":"x"}"#.to_string(),
            r#"{"name":"beta"}"#.to_string(),
        ];
        let batch = exec
            .create_batch_from_rows_nested(
                None,
                None,
                None,
                None,
                &rows,
                &empty_fetch_result(),
                &nesting,
            )
            .expect("batch should be created");
        assert_eq!(batch.num_rows(), 2);

        assert_eq!(string_col(&batch, "id"), vec![Some("1".to_string()), None]);
        assert_eq!(
            string_col(&batch, "name"),
            vec![None, Some("beta".to_string())]
        );
        let details = string_col(&batch, "details");
        assert_eq!(details[0].as_deref(), Some(r#"{"extra":"x"}"#));
        assert!(details[1].is_none());
    }

    #[test]
    fn create_batch_from_rows_nested_non_object_rows_go_to_catchall() {
        let (exec, nesting) = nested_exec(&["id", "details"], "details");
        let rows = vec![
            "[1,2,3]".to_string(),
            "\"scalar\"".to_string(),
            "42".to_string(),
        ];
        let batch = exec
            .create_batch_from_rows_nested(
                None,
                None,
                None,
                None,
                &rows,
                &empty_fetch_result(),
                &nesting,
            )
            .expect("batch should be created");
        assert_eq!(batch.num_rows(), 3);
        for v in string_col(&batch, "id") {
            assert!(
                v.is_none(),
                "non-object rows should have NULL for static fields"
            );
        }
        let details = string_col(&batch, "details");
        assert_eq!(details[0].as_deref(), Some("[1,2,3]"));
        assert_eq!(details[1].as_deref(), Some("\"scalar\""));
        assert_eq!(details[2].as_deref(), Some("42"));
    }

    #[test]
    fn create_batch_from_rows_nested_empty_catchall_is_null_when_all_keys_declared() {
        let (exec, nesting) = nested_exec(&["id", "name", "details"], "details");
        let rows = vec![r#"{"id":"1","name":"alpha"}"#.to_string()];
        let batch = exec
            .create_batch_from_rows_nested(
                None,
                None,
                None,
                None,
                &rows,
                &empty_fetch_result(),
                &nesting,
            )
            .expect("batch should be created");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(string_col(&batch, "id")[0].as_deref(), Some("1"));
        assert_eq!(string_col(&batch, "name")[0].as_deref(), Some("alpha"));
        assert!(
            string_col(&batch, "details")[0].is_none(),
            "catch-all should be NULL when no non-declared keys are present"
        );
    }

    #[test]
    fn create_batch_from_rows_nested_fast_path_when_catchall_not_projected() {
        // Projection keeps only a static column; the catch-all column
        // should never be built.
        let nesting = HttpJsonNesting::new(
            vec!["id".to_string(), "name".to_string(), "details".to_string()],
            "details".to_string(),
            std::collections::HashSet::new(),
        );
        let provider = Arc::new(
            base_provider().with_json_nesting(nesting.clone(), nesting_schema_utf8(&nesting)),
        );
        // Project only static columns, not "details".
        let full_schema = provider.schema();
        let projected = Arc::new(
            full_schema
                .project(&[
                    full_schema.index_of("id").expect("id in schema"),
                    full_schema.index_of("name").expect("name in schema"),
                ])
                .expect("projection should succeed"),
        );
        let exec = HttpExec::new(projected, provider, vec![(None, None, None, None)], None);
        let rows = vec![r#"{"id":"42","name":"fast","extra":"ignored"}"#.to_string()];
        let batch = exec
            .create_batch_from_rows_nested(
                None,
                None,
                None,
                None,
                &rows,
                &empty_fetch_result(),
                &nesting,
            )
            .expect("fast-path batch should be created");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(string_col(&batch, "id"), vec![Some("42".to_string())]);
        assert_eq!(string_col(&batch, "name"), vec![Some("fast".to_string())]);
    }

    #[test]
    fn create_batch_from_rows_nested_fast_path_falls_through_on_non_object_rows() {
        // When catch-all isn't projected but a row isn't a JSON object,
        // the fast path must not apply blindly — decompose_json_row still
        // runs and yields NULL for static fields.
        let nesting = HttpJsonNesting::new(
            vec!["id".to_string(), "details".to_string()],
            "details".to_string(),
            std::collections::HashSet::new(),
        );
        let provider = Arc::new(
            base_provider().with_json_nesting(nesting.clone(), nesting_schema_utf8(&nesting)),
        );
        let full_schema = provider.schema();
        let id_idx = full_schema.index_of("id").expect("id in schema");
        let projected = datafusion::common::project_schema(&full_schema, Some(&vec![id_idx]))
            .expect("project schema");
        let exec = HttpExec::new(projected, provider, vec![(None, None, None, None)], None);
        let rows = vec!["[1,2,3]".to_string(), r#"{"id":"x"}"#.to_string()];
        let batch = exec
            .create_batch_from_rows_nested(
                None,
                None,
                None,
                None,
                &rows,
                &empty_fetch_result(),
                &nesting,
            )
            .expect("batch should be created");
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 1);
        let id = string_col(&batch, "id");
        assert!(id[0].is_none(), "non-object row: id should be NULL");
        assert_eq!(id[1].as_deref(), Some("x"));
    }

    #[test]
    fn create_batch_from_rows_empty_projection_nested_falls_back_to_first_column() {
        let nesting = HttpJsonNesting::new(
            vec!["id".to_string(), "details".to_string()],
            "details".to_string(),
            std::collections::HashSet::new(),
        );
        let provider = Arc::new(
            base_provider().with_json_nesting(nesting.clone(), nesting_schema_utf8(&nesting)),
        );
        let full_schema = provider.schema();
        let projected =
            HttpTableProvider::get_projected_schema(&full_schema, Some(&vec![])).expect("schema");
        assert_eq!(
            projected.fields().len(),
            1,
            "empty projection should fall back to a single field"
        );
        let exec = HttpExec::new(projected, provider, vec![(None, None, None, None)], None);
        let rows = vec![
            r#"{"id":"1","extra":"x"}"#.to_string(),
            r#"{"id":"2"}"#.to_string(),
        ];
        let batch = exec
            .create_batch_from_rows_nested(
                None,
                None,
                None,
                None,
                &rows,
                &empty_fetch_result(),
                &nesting,
            )
            .expect("batch should be created");
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 1);
    }

    #[test]
    fn create_batch_from_rows_nested_passes_through_metadata_columns() {
        // Mix decomposed body columns with HTTP metadata columns. The
        // metadata columns should keep their base-schema types and be
        // populated from the per-batch HTTP request/response values,
        // not from the JSON body.
        let nesting = HttpJsonNesting::new(
            vec![
                "request_path".to_string(),
                "response_status".to_string(),
                "id".to_string(),
                "details".to_string(),
            ],
            "details".to_string(),
            ["request_path".to_string(), "response_status".to_string()]
                .into_iter()
                .collect(),
        );
        let provider = Arc::new(
            base_provider()
                .with_json_nesting(nesting.clone(), nesting_schema_with_metadata(&nesting)),
        );
        let schema = provider.schema();

        // Schema should keep base-table types for metadata columns.
        assert_eq!(
            schema
                .field_with_name("request_path")
                .expect("request_path field")
                .data_type(),
            &arrow::datatypes::DataType::Utf8
        );
        assert_eq!(
            schema
                .field_with_name("response_status")
                .expect("response_status field")
                .data_type(),
            &arrow::datatypes::DataType::UInt16
        );

        let exec = HttpExec::new(
            Arc::clone(&schema),
            provider,
            vec![(Some("/shows/1".to_string()), None, None, None)],
            None,
        );
        let fetch_result = HttpFetchResult {
            content: String::new(),
            max_age: Duration::from_secs(0),
            detected_format: "json".to_string(),
            response_date: None,
            response_status: 201,
            response_headers: Vec::new(),
        };
        // Body has a key colliding with a metadata name; it must be
        // ignored in favor of the actual HTTP value.
        let rows = vec![
            r#"{"id":"1","request_path":"/from-body","extra":"x"}"#.to_string(),
            r#"{"id":"2"}"#.to_string(),
        ];
        let batch = exec
            .create_batch_from_rows_nested(
                Some("/shows/1"),
                None,
                None,
                None,
                &rows,
                &fetch_result,
                &nesting,
            )
            .expect("batch should be created");

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 4);

        // request_path: from HTTP metadata, repeated per row.
        assert_eq!(
            string_col(&batch, "request_path"),
            vec![Some("/shows/1".to_string()), Some("/shows/1".to_string())]
        );

        // response_status: typed UInt16 from fetch_result.
        let status = batch
            .column(
                batch
                    .schema()
                    .index_of("response_status")
                    .expect("response_status column index"),
            )
            .as_any()
            .downcast_ref::<UInt16Array>()
            .expect("response_status should be UInt16");
        assert_eq!(status.value(0), 201);
        assert_eq!(status.value(1), 201);

        // id: decomposed from body.
        assert_eq!(
            string_col(&batch, "id"),
            vec![Some("1".to_string()), Some("2".to_string())]
        );

        // catch-all: must NOT contain `request_path` (it was a metadata
        // collision), but must contain `extra` for row 0.
        let details = string_col(&batch, "details");
        let parsed: serde_json::Value =
            serde_json::from_str(details[0].as_deref().expect("row 0 details"))
                .expect("row 0 details parses as JSON");
        assert!(parsed.get("request_path").is_none());
        assert_eq!(parsed["extra"], "x");
        assert!(details[1].is_none());
    }

    #[test]
    fn create_batch_from_rows_dispatches_to_nested_when_configured() {
        let (exec, _nesting) = nested_exec(&["id", "name", "details"], "details");
        let rows = vec![
            r#"{"id":"1","name":"alpha","extra":"x"}"#.to_string(),
            r#"{"id":"2","name":"beta"}"#.to_string(),
        ];
        let fetch_result = HttpFetchResult {
            content: String::new(),
            max_age: Duration::from_secs(0),
            detected_format: "json".to_string(),
            response_date: None,
            response_status: 200,
            response_headers: Vec::new(),
        };
        let batch = exec
            .create_batch_from_rows(None, None, None, None, &rows, &fetch_result)
            .expect("batch should be created");
        assert_eq!(batch.num_rows(), 2);
        let schema = batch.schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(field_names, vec!["id", "name", "details"]);
    }

    // -----------------------------------------------------------------------
    // with_expanded_params unit tests
    // -----------------------------------------------------------------------

    /// Helper: build an `HttpExec` with the given partitions and optional max.
    ///
    /// Enables all filter types (path, query, body, headers) so that
    /// `with_expanded_params` validation passes for any column.
    fn make_exec(
        partitions: Vec<PartitionSpec>,
        max_request_partitions: Option<usize>,
    ) -> HttpExec {
        let provider = base_provider()
            .with_allowed_paths(["/*"])
            .expect("valid path glob")
            .enable_query_filters(DEFAULT_MAX_QUERY_LENGTH)
            .enable_body_filters(DEFAULT_MAX_BODY_BYTES)
            .enable_header_filters(DEFAULT_MAX_HEADERS_LENGTH, vec!["x-test"])
            .expect("header filters should enable")
            .with_max_request_partitions(max_request_partitions);
        HttpExec::new(
            HttpTableProvider::base_table_schema().into(),
            Arc::new(provider),
            partitions,
            None,
        )
    }

    #[test]
    fn test_with_expanded_params_request_path() {
        let exec = make_exec(vec![(None, None, None, None)], None);
        let result = exec
            .with_expanded_params("request_path", &["/a".to_string(), "/b".to_string()])
            .expect("expand should succeed");

        assert_eq!(result.partitions.len(), 2);
        assert_eq!(result.partitions[0].0, Some("/a".to_string()));
        assert_eq!(result.partitions[1].0, Some("/b".to_string()));
        // Other tuple positions remain None.
        assert_eq!(result.partitions[0].1, None);
        assert_eq!(result.partitions[0].2, None);
        assert_eq!(result.partitions[0].3, None);
    }

    #[test]
    fn test_with_expanded_params_cross_product() {
        let exec = make_exec(
            vec![
                (Some("/a".to_string()), None, None, None),
                (Some("/b".to_string()), None, None, None),
            ],
            None,
        );
        let result = exec
            .with_expanded_params(
                "request_query",
                &["q1".to_string(), "q2".to_string(), "q3".to_string()],
            )
            .expect("expand should succeed");

        // 2 existing × 3 values = 6 partitions
        assert_eq!(result.partitions.len(), 6);

        // First existing partition (/a) crossed with all three values
        assert_eq!(result.partitions[0].0, Some("/a".to_string()));
        assert_eq!(result.partitions[0].1, Some("q1".to_string()));
        assert_eq!(result.partitions[1].0, Some("/a".to_string()));
        assert_eq!(result.partitions[1].1, Some("q2".to_string()));
        assert_eq!(result.partitions[2].0, Some("/a".to_string()));
        assert_eq!(result.partitions[2].1, Some("q3".to_string()));

        // Second existing partition (/b) crossed with all three values
        assert_eq!(result.partitions[3].0, Some("/b".to_string()));
        assert_eq!(result.partitions[3].1, Some("q1".to_string()));
        assert_eq!(result.partitions[4].0, Some("/b".to_string()));
        assert_eq!(result.partitions[4].1, Some("q2".to_string()));
        assert_eq!(result.partitions[5].0, Some("/b".to_string()));
        assert_eq!(result.partitions[5].1, Some("q3".to_string()));
    }

    #[test]
    fn test_with_expanded_params_exceeds_max() {
        // max=3, but 2 partitions × 2 query values = 4 → should fail
        let exec = make_exec(
            vec![
                (Some("/a".to_string()), None, None, None),
                (Some("/b".to_string()), None, None, None),
            ],
            Some(3),
        );

        _ = exec
            .with_expanded_params("request_query", &["q=1".to_string(), "q=2".to_string()])
            .expect_err("should exceed max_request_partitions");
    }

    type PartitionAccessor = Box<dyn Fn(&PartitionSpec) -> &Option<String>>;

    #[test]
    fn test_with_expanded_params_all_columns() {
        // Values must satisfy each column's validation rules:
        // - request_path: must start with '/'
        // - request_query: plain query string
        // - request_body: plain body text
        // - request_headers: JSON with allowed header names
        let cases: Vec<(&str, &str, PartitionAccessor)> = vec![
            ("/val", "request_path", Box::new(|p: &PartitionSpec| &p.0)),
            ("val", "request_query", Box::new(|p: &PartitionSpec| &p.1)),
            ("val", "request_body", Box::new(|p: &PartitionSpec| &p.2)),
            (
                r#"{"x-test":"val"}"#,
                "request_headers",
                Box::new(|p: &PartitionSpec| &p.3),
            ),
        ];

        for (test_value, col_name, accessor) in &cases {
            let exec = make_exec(vec![(None, None, None, None)], None);
            let result = exec
                .with_expanded_params(col_name, &[test_value.to_string()])
                .unwrap_or_else(|e| panic!("expand for {col_name} should succeed: {e}"));

            assert_eq!(
                result.partitions.len(),
                1,
                "one partition expected for {col_name}"
            );
            assert_eq!(
                *accessor(&result.partitions[0]),
                Some(test_value.to_string()),
                "{col_name} should be set"
            );

            // Verify the OTHER positions are still None.
            let all_accessors: Vec<PartitionAccessor> = vec![
                Box::new(|p: &PartitionSpec| &p.0),
                Box::new(|p: &PartitionSpec| &p.1),
                Box::new(|p: &PartitionSpec| &p.2),
                Box::new(|p: &PartitionSpec| &p.3),
            ];
            let col_names = [
                "request_path",
                "request_query",
                "request_body",
                "request_headers",
            ];
            for (other_name, other_accessor) in col_names.iter().zip(all_accessors.iter()) {
                if *other_name != *col_name {
                    assert_eq!(
                        *other_accessor(&result.partitions[0]),
                        None,
                        "{other_name} should remain None when expanding {col_name}"
                    );
                }
            }
        }
    }

    #[test]
    fn test_with_expanded_params_unknown_column_errors() {
        let exec = make_exec(vec![(Some("/orig".to_string()), None, None, None)], None);
        let _ = exec
            .with_expanded_params("nonexistent_column", &["x".to_string()])
            .expect_err("unknown column should error");
    }
}
