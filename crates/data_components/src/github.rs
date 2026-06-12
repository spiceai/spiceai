/*
Copyright 2025 The Spice.ai OSS Authors

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
use async_trait::async_trait;
use chrono::DateTime;
use futures::future;
use globset::GlobSet;
use serde_json::Value;
use snafu::{ResultExt, Snafu};

use crate::{arrow::write::MemTable, graphql, rate_limit::RateLimiter};
use arrow::{
    array::{ArrayRef, Int64Builder, RecordBatch, StringBuilder, TimestampMillisecondBuilder},
    datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
};
use datafusion::{
    catalog::Session,
    datasource::{TableProvider, TableType},
    error::DataFusionError,
    execution::SendableRecordBatchStream,
    logical_expr::{Expr, Operator, TableProviderFilterPushDown},
    physical_plan::{ExecutionPlan, stream::RecordBatchStreamAdapter},
    scalar::ScalarValue,
};
use std::{any::Any, collections::HashMap, path::Path, sync::Arc, time::Duration};
use token_provider::TokenProvider;
use util::ExponentialBackoff;
use util::fibonacci_backoff::{Backoff, FibonacciBackoffBuilder};

use reqwest::{
    StatusCode, Url,
    header::{ACCEPT, AUTHORIZATION, HeaderMap, HeaderValue, USER_AGENT},
};
use serde::Deserialize;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to process GitHub API response: {source}"))]
    UnableToConstructRecordBatchError { source: arrow::error::ArrowError },

    #[snafu(display("{source}"))]
    GithubApiError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("{message}"))]
    RateLimited { message: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    #[must_use]
    pub fn is_transient(&self) -> bool {
        match self {
            Self::RateLimited { .. } => true,
            Self::GithubApiError { source } => {
                let message = source.to_string().to_ascii_lowercase();
                [
                    "rate limit exceeded",
                    "temporarily unavailable",
                    "timed out",
                    "could not connect",
                    "throttled the request",
                    "retried automatically",
                    "after automatic retries",
                ]
                .iter()
                .any(|needle| message.contains(needle))
            }
            Self::UnableToConstructRecordBatchError { .. } => false,
        }
    }
}

#[derive(Debug)]
pub struct GithubFilesTableProvider {
    client: GithubRestClient,
    owner: Arc<str>,
    repo: Arc<str>,
    requested_ref: Option<Arc<str>>,
    default_ref: Option<Arc<str>>,
    schema: SchemaRef,
    include: Option<Arc<GlobSet>>,
    fetch_content: bool,
    include_commits: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GithubRef {
    pub name: String,
    pub qualified_name: String,
}

impl GithubFilesTableProvider {
    pub async fn new(
        client: GithubRestClient,
        owner: &str,
        repo: &str,
        requested_ref: Option<&str>,
        include: Option<Arc<GlobSet>>,
        fetch_content: bool,
        include_commits: bool,
    ) -> Result<Self> {
        let requested_ref = requested_ref.filter(|ref_name| !ref_name.is_empty());
        let default_ref = if requested_ref.is_none() {
            match client.fetch_default_branch(owner, repo).await {
                Ok(default_ref) => Some(Arc::<str>::from(default_ref)),
                Err(err) if err.is_transient() => {
                    tracing::warn!(
                        "Failed to retrieve the default branch for GitHub repository {owner}/{repo} during provider initialization: {err} The branch will be resolved lazily on the next query or refresh."
                    );
                    None
                }
                Err(err) => return Err(err),
            }
        } else {
            None
        };

        let mut fields = vec![
            Field::new("ref", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("path", DataType::Utf8, true),
            Field::new("size", DataType::Int64, true),
            Field::new("sha", DataType::Utf8, true),
            Field::new("mode", DataType::Utf8, true),
            Field::new("url", DataType::Utf8, true),
            Field::new("download_url", DataType::Utf8, true),
        ];

        if include_commits {
            fields.push(Field::new(
                "created_at",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                true,
            ));
            fields.push(Field::new(
                "updated_at",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                true,
            ));
        }

        if fetch_content {
            fields.push(Field::new("content", DataType::Utf8, true));
        }

        let schema = Arc::new(Schema::new(fields));

        if let Some(effective_ref) = requested_ref.or(default_ref.as_deref()) {
            // ensure configuration is correct when GitHub is currently reachable
            if let Err(err) = client
                .fetch_files(
                    owner,
                    repo,
                    effective_ref,
                    Some(1),
                    None,
                    fetch_content,
                    include_commits,
                    Arc::clone(&schema),
                )
                .await
            {
                if err.is_transient() {
                    tracing::warn!(
                        "GitHub provider initialization for repository {owner}/{repo} could not validate ref `{effective_ref}` because GitHub is temporarily unavailable: {err} The dataset will retry on the next query or refresh."
                    );
                } else {
                    return Err(err);
                }
            }
        }

        Ok(Self {
            client,
            owner: owner.into(),
            repo: repo.into(),
            requested_ref: requested_ref.map(Arc::from),
            default_ref,
            schema,
            include,
            fetch_content,
            include_commits,
        })
    }
}

fn scalar_utf8_value(scalar: &ScalarValue) -> Option<&str> {
    match scalar {
        ScalarValue::Utf8(Some(v))
        | ScalarValue::LargeUtf8(Some(v))
        | ScalarValue::Utf8View(Some(v)) => Some(v.as_str()),
        _ => None,
    }
}

fn ref_from_filter(expr: &Expr) -> Option<String> {
    match expr {
        Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::Eq => {
            match (&*binary_expr.left, &*binary_expr.right) {
                (Expr::Column(column), Expr::Literal(value, _))
                | (Expr::Literal(value, _), Expr::Column(column))
                    if column.name == "ref" =>
                {
                    scalar_utf8_value(value)
                        .filter(|v| !v.is_empty())
                        .map(ToString::to_string)
                }
                _ => None,
            }
        }
        _ => None,
    }
}

fn expr_references_ref(expr: &Expr) -> bool {
    expr.column_refs().iter().any(|column| column.name == "ref")
}

fn unsupported_ref_filter_error() -> DataFusionError {
    DataFusionError::Execution(
        "GitHub files only support a single non-empty ref = '<value>' predicate. Queries using ref with OR, IN, inequality, or multiple values are not supported because they can return incorrect results.".to_string(),
    )
}

fn merge_requested_refs(
    current: Option<String>,
    next: Option<String>,
) -> datafusion::error::Result<Option<String>> {
    match (current, next) {
        (Some(current), Some(next)) if current != next => Err(unsupported_ref_filter_error()),
        (Some(current), _) => Ok(Some(current)),
        (None, Some(next)) => Ok(Some(next)),
        (None, None) => Ok(None),
    }
}

fn requested_ref_from_filter(expr: &Expr) -> datafusion::error::Result<Option<String>> {
    match expr {
        Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::And => merge_requested_refs(
            requested_ref_from_filter(binary_expr.left.as_ref())?,
            requested_ref_from_filter(binary_expr.right.as_ref())?,
        ),
        _ => {
            if let Some(value) = ref_from_filter(expr) {
                return Ok(Some(value));
            }

            if expr_references_ref(expr) {
                return Err(unsupported_ref_filter_error());
            }

            Ok(None)
        }
    }
}

fn requested_ref_from_filters(filters: &[Expr]) -> datafusion::error::Result<Option<String>> {
    filters.iter().try_fold(None, |current, filter| {
        merge_requested_refs(current, requested_ref_from_filter(filter)?)
    })
}

#[async_trait]
impl TableProvider for GithubFilesTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
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
    ) -> std::result::Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        Ok(filters
            .iter()
            .map(|filter| {
                if ref_from_filter(filter).is_some() {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let requested_ref = if let Some(requested_ref) = requested_ref_from_filters(filters)?
            .or_else(|| self.requested_ref.as_deref().map(ToString::to_string))
        {
            requested_ref
        } else if let Some(default_ref) = self.default_ref.as_deref() {
            default_ref.to_string()
        } else {
            self.client
                .fetch_default_branch(&self.owner, &self.repo)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
        };

        let res: Vec<RecordBatch> = self
            .client
            .fetch_files(
                &self.owner,
                &self.repo,
                &requested_ref,
                None,
                self.include.clone(),
                self.fetch_content,
                self.include_commits,
                Arc::clone(&self.schema),
            )
            .await
            .boxed()
            .map_err(DataFusionError::External)?;
        let table = MemTable::try_new(Arc::clone(&self.schema), vec![res])?;
        table.scan(state, projection, filters, limit).await
    }
}

#[derive(Clone)]
pub struct GithubRestClient {
    client: reqwest::Client,
    token: Option<Arc<dyn TokenProvider>>,
    rate_limiter: Arc<dyn RateLimiter>,
}

impl std::fmt::Debug for GithubRestClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GithubRestClient")
            .field("token", &self.token.as_ref().map(|_| "[REDACTED]"))
            .finish_non_exhaustive()
    }
}

fn add_optional_github_auth(headers: &mut HeaderMap, token: Option<&Arc<dyn TokenProvider>>) {
    if let Some(token) = token
        && let Ok(header) = HeaderValue::from_str(&format!("token {}", token.get_token()))
    {
        headers.insert(AUTHORIZATION, header);
    }
}

static SPICE_USER_AGENT: &str = "spice";
const NUM_FILE_CONTENT_DOWNLOAD_WORKERS: usize = 10;

#[derive(Debug, Clone, Copy)]
enum RetryableErrorType {
    RateLimit,   // 408, 429 - use exponential backoff
    ServerError, // 5xx - use fibonacci backoff
    Network,     // connection/timeout errors - use fibonacci backoff
}

/// Determines if a reqwest error should be retried and what type of error it is
fn classify_retryable_error(error: &reqwest::Error) -> Option<RetryableErrorType> {
    // Check for network errors first
    if error.is_connect() || error.is_timeout() {
        return Some(RetryableErrorType::Network);
    }

    // Check HTTP status codes
    if let Some(status) = error.status() {
        let code = status.as_u16();
        match code {
            408 | 429 => Some(RetryableErrorType::RateLimit),
            500..=599 => Some(RetryableErrorType::ServerError),
            _ => None,
        }
    } else {
        None
    }
}

/// Retry with adaptive backoff - exponential for rate limits, fibonacci for server errors
/// The `rate_limiter` is checked before each retry attempt to ensure concurrency control
async fn retry_with_adaptive_backoff<F, Fut, T>(
    operation_name: &str,
    max_retries: usize,
    rate_limiter: &Arc<dyn RateLimiter>,
    operation: F,
) -> Result<T, reqwest::Error>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, reqwest::Error>>,
{
    let mut fibonacci_backoff = FibonacciBackoffBuilder::new()
        .max_retries(Some(max_retries))
        .build();

    let mut exponential_backoff = ExponentialBackoff {
        max_elapsed_time: Some(std::time::Duration::from_mins(5)), // 5 minutes max total retry time
        ..ExponentialBackoff::default()
    };

    let mut exponential_retry_count = 0_usize;

    loop {
        // Check rate limit before each attempt
        // The rate limiter handles waiting based on rate limit info from previous responses
        // This always returns Ok(()) after waiting if needed
        if let Err(err) = rate_limiter.check_rate_limit().await {
            tracing::warn!(
                "Failed to evaluate GitHub API rate limits before {operation_name}: {err}"
            );
        }

        match operation().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                match classify_retryable_error(&e) {
                    Some(RetryableErrorType::RateLimit) => {
                        // Check if we've exceeded max retries
                        if exponential_retry_count >= max_retries {
                            tracing::warn!(
                                "GitHub API request to {operation_name} remained rate limited after {max_retries} retries: {e}"
                            );
                            return Err(e);
                        }
                        exponential_retry_count += 1;

                        // For rate limits, the response headers have been updated in the rate limiter.
                        // On the next loop iteration, check_rate_limit() will handle the waiting
                        // based on the retry-after or x-ratelimit-reset header from the response.
                        // We add a small exponential backoff as additional protection.
                        if let Some(duration) = Backoff::next_backoff(&mut exponential_backoff) {
                            tracing::warn!(
                                "GitHub API rate limited {operation_name}; retrying attempt {exponential_retry_count}/{max_retries} after {duration:?}: {e}"
                            );
                            tokio::time::sleep(duration).await;
                        } else {
                            return Err(e);
                        }
                    }
                    Some(RetryableErrorType::ServerError | RetryableErrorType::Network) => {
                        // Use fibonacci backoff for server errors and network issues
                        if let Some(duration) = Backoff::next_backoff(&mut fibonacci_backoff) {
                            tracing::warn!(
                                "GitHub API request to {operation_name} failed; retrying in {duration:?}: {e}",
                            );
                            tokio::time::sleep(duration).await;
                        } else {
                            return Err(e);
                        }
                    }
                    None => {
                        // Non-retryable error
                        return Err(e);
                    }
                }
            }
        }
    }
}

fn github_response_message(response: &Value) -> Option<String> {
    let message = response
        .get("message")
        .and_then(Value::as_str)
        .or_else(|| {
            response
                .get("error")
                .and_then(|error| error.get("message"))
                .and_then(Value::as_str)
        })
        .or_else(|| {
            response
                .get("errors")
                .and_then(Value::as_array)
                .and_then(|errors| {
                    errors
                        .iter()
                        .find_map(|error| error.get("message").and_then(Value::as_str))
                })
        })
        .map(str::trim)
        .filter(|message| !message.is_empty())
        .map(ToString::to_string);

    let documentation_url = response
        .get("documentation_url")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|url| !url.is_empty());

    match (message, documentation_url) {
        (Some(message), Some(documentation_url)) if !message.contains(documentation_url) => {
            Some(format!("{message} See {documentation_url}"))
        }
        (Some(message), _) => Some(message),
        (None, Some(documentation_url)) => Some(format!("See {documentation_url}")),
        (None, None) => None,
    }
}

fn github_response_message_from_text(response_text: &str) -> Option<String> {
    let trimmed = response_text.trim();
    if trimmed.is_empty() {
        return None;
    }

    serde_json::from_str::<Value>(trimmed)
        .ok()
        .as_ref()
        .and_then(github_response_message)
        .or_else(|| Some(sanitize_github_error_detail(trimmed)))
}

fn github_is_rate_limit_message(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("rate limit") || message.contains("secondary rate")
}

fn sanitize_github_error_detail(detail: &str) -> String {
    detail.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn append_github_error_detail(mut message: String, detail: Option<&str>) -> String {
    if let Some(detail) = detail
        .map(sanitize_github_error_detail)
        .filter(|detail| !detail.is_empty())
        && !message.contains(&detail)
    {
        message.push_str(" Details: ");
        message.push_str(&detail);
    }

    message
}

fn format_github_status_error(
    action: &str,
    owner: &str,
    repo: &str,
    status: StatusCode,
    detail: Option<&str>,
) -> String {
    let repo_name = format!("{owner}/{repo}");
    let message = match status {
        StatusCode::UNAUTHORIZED => format!(
            "Failed to {action} for GitHub repository {repo_name}: authentication failed (HTTP {status}). Verify the GitHub token is correct."
        ),
        StatusCode::FORBIDDEN if detail.is_some_and(github_is_rate_limit_message) => format!(
            "Failed to {action} for GitHub repository {repo_name}: GitHub API rate limit exceeded (HTTP {status}). Reduce GitHub request concurrency or retry later."
        ),
        StatusCode::FORBIDDEN => format!(
            "Failed to {action} for GitHub repository {repo_name}: permission denied (HTTP {status}). Verify the GitHub token has the required permissions."
        ),
        StatusCode::NOT_FOUND => format!(
            "Failed to {action} for GitHub repository {repo_name}: the requested resource was not found or is not accessible (HTTP {status})."
        ),
        StatusCode::GONE => format!(
            "Failed to {action} for GitHub repository {repo_name}: the requested resource is no longer available (HTTP {status})."
        ),
        StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_MANY_REQUESTS => format!(
            "Failed to {action} for GitHub repository {repo_name}: GitHub timed out or throttled the request (HTTP {status}). Spice retried automatically."
        ),
        _ if status.is_server_error() => format!(
            "Failed to {action} for GitHub repository {repo_name}: GitHub is temporarily unavailable (HTTP {status}). Spice retried automatically."
        ),
        _ => format!("Failed to {action} for GitHub repository {repo_name} (HTTP {status})."),
    };

    append_github_error_detail(message, detail)
}

fn format_github_request_error(
    action: &str,
    owner: &str,
    repo: &str,
    error: &reqwest::Error,
) -> String {
    let repo_name = format!("{owner}/{repo}");

    if let Some(status) = error.status() {
        return format_github_status_error(action, owner, repo, status, None);
    }

    if error.is_timeout() {
        return format!(
            "Failed to {action} for GitHub repository {repo_name}: the request timed out after automatic retries."
        );
    }

    if error.is_connect() {
        return format!(
            "Failed to {action} for GitHub repository {repo_name}: could not connect to GitHub after automatic retries."
        );
    }

    if error.is_body() || error.is_decode() {
        return format!(
            "Failed to {action} for GitHub repository {repo_name}: GitHub returned an unreadable response after automatic retries: {error}"
        );
    }

    format!("Failed to {action} for GitHub repository {repo_name}: {error}")
}

fn github_api_error_from_message(message: String) -> Error {
    Error::GithubApiError {
        source: message.into(),
    }
}

fn boxed_github_request_error(
    action: &str,
    owner: &str,
    repo: &str,
    error: &reqwest::Error,
) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(github_api_error_from_message(format_github_request_error(
        action, owner, repo, error,
    )))
}

fn boxed_github_status_error(
    action: &str,
    owner: &str,
    repo: &str,
    status: StatusCode,
    detail: Option<&str>,
) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(github_api_error_from_message(format_github_status_error(
        action, owner, repo, status, detail,
    )))
}

async fn read_github_error_response(
    response: reqwest::Response,
) -> (HeaderMap, StatusCode, Option<Value>, Option<String>) {
    let response_headers = response.headers().clone();
    let response_status = response.status();
    let response_text = response
        .text()
        .await
        .unwrap_or_else(|err| format!("Unable to read GitHub API response body: {err}"));
    let response_json = serde_json::from_str::<Value>(&response_text).ok();
    let detail = response_json
        .as_ref()
        .and_then(github_response_message)
        .or_else(|| github_response_message_from_text(&response_text));

    (response_headers, response_status, response_json, detail)
}

impl GithubRestClient {
    pub fn new(
        token: Option<Arc<dyn TokenProvider>>,
        rate_limiter: Arc<dyn RateLimiter>,
    ) -> reqwest::Result<Self> {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_mins(2))
            .build()?;

        Ok(GithubRestClient {
            client,
            token,
            rate_limiter,
        })
    }

    #[expect(clippy::too_many_arguments)]
    #[expect(clippy::missing_panics_doc)]
    #[expect(clippy::expect_used)]
    pub async fn fetch_files(
        &self,
        owner: &str,
        repo: &str,
        tree_sha: &str,
        limit: Option<usize>,
        include_pattern: Option<Arc<GlobSet>>,
        fetch_content: bool,
        include_commits: bool,
        schema: SchemaRef,
    ) -> Result<Vec<RecordBatch>> {
        tracing::debug!(owner, repo, ref_name = tree_sha, "Fetching GitHub files");

        let git_tree = self
            .fetch_git_tree(owner, repo, tree_sha)
            .await
            .context(GithubApiSnafu)?;

        let mut tree: Vec<GitTreeNode> = git_tree
            .tree
            .into_iter()
            .filter(|node| node.node_type == "blob")
            .collect();

        if let Some(pattern) = include_pattern.as_ref() {
            tree.retain(|node| pattern.is_match(&node.path));
        }

        if let Some(limit) = limit {
            tree.truncate(limit);
        }

        let mut ref_builder = StringBuilder::new();
        let mut name_builder = StringBuilder::new();
        let mut path_builder = StringBuilder::new();
        let mut size_builder = Int64Builder::new();
        let mut sha_builder = StringBuilder::new();
        let mut mode_builder = StringBuilder::new();
        let mut url_builder = StringBuilder::new();
        let mut download_url_builder = StringBuilder::new();
        let mut created_at_builder = if include_commits {
            Some(TimestampMillisecondBuilder::new())
        } else {
            None
        };
        let mut updated_at_builder = if include_commits {
            Some(TimestampMillisecondBuilder::new())
        } else {
            None
        };

        // Process files in chunks, fetching commit information if requested
        for chunk in tree.chunks(NUM_FILE_CONTENT_DOWNLOAD_WORKERS) {
            // Fetch commits in parallel for this chunk if requested
            let commits_results = if include_commits {
                let commit_fetch_futures = chunk
                    .iter()
                    .map(|node| self.fetch_file_commits(owner, repo, tree_sha, &node.path))
                    .collect::<Vec<_>>();
                Some(future::join_all(commit_fetch_futures).await)
            } else {
                None
            };

            // Build record batch fields for this chunk
            for (idx, node) in chunk.iter().enumerate() {
                // Add basic file information (shared between both code paths)
                ref_builder.append_value(tree_sha);
                name_builder.append_value(extract_name_from_path(&node.path).unwrap_or_default());
                path_builder.append_value(&node.path);
                size_builder.append_value(node.size.unwrap_or(0));
                sha_builder.append_value(&node.sha);
                mode_builder.append_value(&node.mode);
                match &node.url {
                    Some(url) => url_builder.append_value(url),
                    None => url_builder.append_null(),
                }
                download_url_builder
                    .append_value(get_download_url(owner, repo, tree_sha, &node.path));

                // Add timestamps from commits if we fetched them
                if let Some(ref results) = commits_results {
                    match &results[idx] {
                        Ok(commits) if !commits.is_empty() => {
                            // First commit is the most recent (updated_at)
                            if let Ok(dt) =
                                DateTime::parse_from_rfc3339(&commits[0].commit.author.date)
                            {
                                updated_at_builder
                                .as_mut()
                                .expect("updated_at_builder should exist when include_commits is true")
                                .append_value(dt.timestamp_millis());
                            } else {
                                updated_at_builder.as_mut().expect("updated_at_builder should exist when include_commits is true").append_null();
                            }

                            // Last commit is the oldest (created_at)
                            let last_commit = commits
                                .last()
                                .expect("commits should not be empty based on match guard");
                            if let Ok(dt) =
                                DateTime::parse_from_rfc3339(&last_commit.commit.author.date)
                            {
                                created_at_builder
                                .as_mut()
                                .expect("created_at_builder should exist when include_commits is true")
                                .append_value(dt.timestamp_millis());
                            } else {
                                created_at_builder.as_mut().expect("created_at_builder should exist when include_commits is true").append_null();
                            }
                        }
                        _ => {
                            created_at_builder
                                .as_mut()
                                .expect(
                                    "created_at_builder should exist when include_commits is true",
                                )
                                .append_null();
                            updated_at_builder
                                .as_mut()
                                .expect(
                                    "updated_at_builder should exist when include_commits is true",
                                )
                                .append_null();
                        }
                    }
                }
            }
        }

        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(ref_builder.finish()),
            Arc::new(name_builder.finish()),
            Arc::new(path_builder.finish()),
            Arc::new(size_builder.finish()),
            Arc::new(sha_builder.finish()),
            Arc::new(mode_builder.finish()),
            Arc::new(url_builder.finish()),
            Arc::new(download_url_builder.finish()),
        ];

        if include_commits {
            columns.push(Arc::new(
                created_at_builder
                    .expect("created_at_builder should exist when include_commits is true")
                    .finish(),
            ));
            columns.push(Arc::new(
                updated_at_builder
                    .expect("updated_at_builder should exist when include_commits is true")
                    .finish(),
            ));
        }

        if fetch_content {
            let mut content_builder = StringBuilder::new();

            // download content in parallel using chunks to avoid lifetime issues
            for chunk in tree.chunks(NUM_FILE_CONTENT_DOWNLOAD_WORKERS) {
                let download_futures: Vec<_> = chunk
                    .iter()
                    .map(|node| self.fetch_file_content(owner, repo, tree_sha, &node.path))
                    .collect();

                let results = future::join_all(download_futures).await;

                for (node, res) in chunk.iter().zip(results) {
                    match res {
                        Ok(content) => content_builder.append_value(content),
                        Err(err) => {
                            tracing::warn!(
                                "Failed to download file content for GitHub repository {owner}/{repo} path {} at ref `{tree_sha}`: {} The 'content' column will be null for this row.",
                                node.path,
                                err
                            );
                            content_builder.append_null();
                        }
                    }
                }
            }
            columns.push(Arc::new(content_builder.finish()));
        }

        let record_batch = RecordBatch::try_new(Arc::clone(&schema), columns)
            .context(UnableToConstructRecordBatchSnafu)?;

        Ok(vec![record_batch])
    }

    async fn fetch_git_tree(
        &self,
        owner: &str,
        repo: &str,
        tree_sha: &str,
    ) -> Result<GitTree, Box<dyn std::error::Error + Send + Sync>> {
        self.rate_limiter.check_rate_limit().await?;

        let endpoint = format!(
            "https://api.github.com/repos/{owner}/{repo}/git/trees/{tree_sha}?recursive=true"
        );
        let action = format!("retrieve the file tree for ref `{tree_sha}`");

        let client = &self.client;
        let token = &self.token;
        let rate_limiter = &self.rate_limiter;

        let response = retry_with_adaptive_backoff(&action, 5, rate_limiter, || async {
            let mut headers = HeaderMap::new();
            headers.insert(USER_AGENT, HeaderValue::from_static(SPICE_USER_AGENT));
            headers.insert(
                ACCEPT,
                HeaderValue::from_static("application/vnd.github.v3+json"),
            );

            add_optional_github_auth(&mut headers, token.as_ref());

            tracing::debug!(owner, repo, ref_name = tree_sha, endpoint = %endpoint, "Requesting GitHub file tree");

            client.get(&endpoint).headers(headers).send().await
        })
        .await
        .map_err(|e: reqwest::Error| -> Box<dyn std::error::Error + Send + Sync> {
            boxed_github_request_error(&action, owner, repo, &e)
        })?;

        rate_limiter.update_from_headers(response.headers()).await;

        if response.status().is_success() {
            let git_tree = response.json::<GitTree>().await?;
            tracing::trace!(
                owner,
                repo,
                ref_name = tree_sha,
                entries = git_tree.tree.len(),
                "Received GitHub file tree"
            );
            return Ok(git_tree);
        }

        let (response_headers, response_status, response_json, detail) =
            read_github_error_response(response).await;

        if let Some(response_json) = response_json.as_ref() {
            error_checker(&response_headers, response_json).map_err(|e| {
                if let graphql::Error::RateLimited { message } = e {
                    Error::RateLimited { message }
                } else {
                    Error::GithubApiError { source: e.into() }
                }
            })?;
        }

        Err(boxed_github_status_error(
            &action,
            owner,
            repo,
            response_status,
            detail.as_deref(),
        ))
    }

    async fn fetch_default_branch(&self, owner: &str, repo: &str) -> Result<String> {
        self.rate_limiter
            .check_rate_limit()
            .await
            .context(GithubApiSnafu)?;

        let endpoint = format!("https://api.github.com/repos/{owner}/{repo}");
        let action = "retrieve the default branch".to_string();

        let client = &self.client;
        let token = &self.token;
        let rate_limiter = &self.rate_limiter;

        let response = retry_with_adaptive_backoff(&action, 3, rate_limiter, || async {
            let mut headers = HeaderMap::new();
            headers.insert(USER_AGENT, HeaderValue::from_static(SPICE_USER_AGENT));
            headers.insert(
                ACCEPT,
                HeaderValue::from_static("application/vnd.github.v3+json"),
            );

            add_optional_github_auth(&mut headers, token.as_ref());

            tracing::debug!(owner, repo, endpoint = %endpoint, "Requesting GitHub repository metadata");

            client.get(&endpoint).headers(headers).send().await
        })
        .await
        .map_err(|e: reqwest::Error| Error::GithubApiError {
            source: format_github_request_error(&action, owner, repo, &e).into(),
        })?;

        rate_limiter.update_from_headers(response.headers()).await;

        if response.status().is_success() {
            let repo_metadata = response
                .json::<GitHubRepository>()
                .await
                .map_err(|e| Error::GithubApiError { source: e.into() })?;
            return Ok(repo_metadata.default_branch);
        }

        let (response_headers, response_status, response_json, detail) =
            read_github_error_response(response).await;

        if let Some(response_json) = response_json.as_ref() {
            error_checker(&response_headers, response_json).map_err(|e| {
                if let graphql::Error::RateLimited { message } = e {
                    Error::RateLimited { message }
                } else {
                    Error::GithubApiError { source: e.into() }
                }
            })?;
        }

        Err(Error::GithubApiError {
            source: format_github_status_error(
                &action,
                owner,
                repo,
                response_status,
                detail.as_deref(),
            )
            .into(),
        })
    }

    pub async fn fetch_refs(
        &self,
        owner: &str,
        repo: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        let mut refs = self
            .fetch_qualified_refs(owner, repo)
            .await?
            .into_iter()
            .map(|git_ref| git_ref.name)
            .collect::<Vec<_>>();
        refs.sort_unstable();
        refs.dedup();
        Ok(refs)
    }

    pub async fn fetch_qualified_refs(
        &self,
        owner: &str,
        repo: &str,
    ) -> Result<Vec<GithubRef>, Box<dyn std::error::Error + Send + Sync>> {
        let mut refs = self
            .fetch_refs_for_resource(owner, repo, "branches", "refs/heads/", None)
            .await?;
        refs.extend(
            self.fetch_refs_for_resource(owner, repo, "tags", "refs/tags/", None)
                .await?,
        );
        refs.sort_unstable_by(|left, right| left.qualified_name.cmp(&right.qualified_name));
        refs.dedup_by(|left, right| left.qualified_name == right.qualified_name);
        Ok(refs)
    }

    pub async fn fetch_qualified_refs_bounded(
        &self,
        owner: &str,
        repo: &str,
        max_refs: usize,
    ) -> Result<Vec<GithubRef>, Box<dyn std::error::Error + Send + Sync>> {
        let mut refs = self
            .fetch_refs_for_resource(owner, repo, "branches", "refs/heads/", Some(max_refs))
            .await?;
        let remaining_refs = max_refs.saturating_sub(refs.len());
        if remaining_refs == 0 {
            refs.sort_unstable_by(|left, right| left.qualified_name.cmp(&right.qualified_name));
            refs.dedup_by(|left, right| left.qualified_name == right.qualified_name);
            return Ok(refs);
        }

        refs.extend(
            self.fetch_refs_for_resource(owner, repo, "tags", "refs/tags/", Some(remaining_refs))
                .await?,
        );
        refs.sort_unstable_by(|left, right| left.qualified_name.cmp(&right.qualified_name));
        refs.dedup_by(|left, right| left.qualified_name == right.qualified_name);
        Ok(refs)
    }

    pub async fn fetch_qualified_ref(
        &self,
        owner: &str,
        repo: &str,
        qualified_name: &str,
    ) -> Result<Option<GithubRef>, Box<dyn std::error::Error + Send + Sync>> {
        self.rate_limiter.check_rate_limit().await?;

        let Some(git_ref_path) = qualified_name.strip_prefix("refs/") else {
            return Ok(None);
        };

        let action = format!("retrieve GitHub ref {qualified_name}");
        let endpoint = git_ref_endpoint(owner, repo, git_ref_path)?;

        let client = &self.client;
        let token = &self.token;
        let rate_limiter = &self.rate_limiter;

        let response = retry_with_adaptive_backoff(&action, 3, rate_limiter, || async {
            let mut headers = HeaderMap::new();
            headers.insert(USER_AGENT, HeaderValue::from_static(SPICE_USER_AGENT));
            headers.insert(
                ACCEPT,
                HeaderValue::from_static("application/vnd.github.v3+json"),
            );

            add_optional_github_auth(&mut headers, token.as_ref());

            tracing::debug!(owner, repo, qualified_name, endpoint = %endpoint, "Requesting GitHub ref");

            client.get(endpoint.clone()).headers(headers).send().await
        })
        .await
        .map_err(|e: reqwest::Error| -> Box<dyn std::error::Error + Send + Sync> {
            boxed_github_request_error(&action, owner, repo, &e)
        })?;

        rate_limiter.update_from_headers(response.headers()).await;

        if response.status().is_success() {
            let git_ref = response.json::<GitQualifiedRef>().await?;
            return Ok(Some(GithubRef {
                name: short_ref_name(&git_ref.qualified_name),
                qualified_name: git_ref.qualified_name,
            }));
        }

        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }

        let (response_headers, response_status, response_json, detail) =
            read_github_error_response(response).await;

        if let Some(response_json) = response_json.as_ref() {
            error_checker(&response_headers, response_json).map_err(|e| {
                if let graphql::Error::RateLimited { message } = e {
                    Error::RateLimited { message }
                } else {
                    Error::GithubApiError { source: e.into() }
                }
            })?;
        }

        Err(boxed_github_status_error(
            &action,
            owner,
            repo,
            response_status,
            detail.as_deref(),
        ))
    }

    async fn fetch_refs_for_resource(
        &self,
        owner: &str,
        repo: &str,
        resource: &str,
        qualified_name_prefix: &str,
        max_refs: Option<usize>,
    ) -> Result<Vec<GithubRef>, Box<dyn std::error::Error + Send + Sync>> {
        if max_refs == Some(0) {
            return Ok(Vec::new());
        }

        self.rate_limiter.check_rate_limit().await?;

        let action = format!("retrieve GitHub {resource} refs");
        let client = &self.client;
        let token = &self.token;
        let rate_limiter = &self.rate_limiter;

        let mut refs = Vec::new();
        let mut page = 1;
        let per_page = 100;

        loop {
            let endpoint = format!(
                "https://api.github.com/repos/{owner}/{repo}/{resource}?per_page={per_page}&page={page}"
            );

            let response = retry_with_adaptive_backoff(&action, 3, rate_limiter, || async {
                let mut headers = HeaderMap::new();
                headers.insert(USER_AGENT, HeaderValue::from_static(SPICE_USER_AGENT));
                headers.insert(
                    ACCEPT,
                    HeaderValue::from_static("application/vnd.github.v3+json"),
                );

                add_optional_github_auth(&mut headers, token.as_ref());

                tracing::debug!(owner, repo, resource, page, endpoint = %endpoint, "Requesting GitHub refs");

                client.get(&endpoint).headers(headers).send().await
            })
            .await
            .map_err(|e: reqwest::Error| -> Box<dyn std::error::Error + Send + Sync> {
                boxed_github_request_error(&action, owner, repo, &e)
            })?;

            rate_limiter.update_from_headers(response.headers()).await;

            if !response.status().is_success() {
                let (response_headers, response_status, response_json, detail) =
                    read_github_error_response(response).await;

                if let Some(response_json) = response_json.as_ref() {
                    error_checker(&response_headers, response_json).map_err(|e| {
                        if let graphql::Error::RateLimited { message } = e {
                            Error::RateLimited { message }
                        } else {
                            Error::GithubApiError { source: e.into() }
                        }
                    })?;
                }

                return Err(boxed_github_status_error(
                    &action,
                    owner,
                    repo,
                    response_status,
                    detail.as_deref(),
                ));
            }

            let page_refs = response.json::<Vec<GitRefName>>().await?;
            if page_refs.is_empty() {
                break;
            }

            let page_len = page_refs.len();
            if let Some(max_refs) = max_refs
                && refs.len() + page_len > max_refs
            {
                // Truncate to max_refs instead of erroring so dynamic scans
                // work on repos with more refs than the limit. Only the first
                // max_refs refs are included; the caller is responsible for
                // documenting that dynamic ref scans are best-effort.
                let take = max_refs.saturating_sub(refs.len());
                refs.extend(page_refs.into_iter().take(take).map(|git_ref| GithubRef {
                    qualified_name: format!("{qualified_name_prefix}{}", git_ref.name),
                    name: git_ref.name,
                }));
                break;
            }

            refs.extend(page_refs.into_iter().map(|git_ref| GithubRef {
                qualified_name: format!("{qualified_name_prefix}{}", git_ref.name),
                name: git_ref.name,
            }));

            if page_len < per_page {
                break;
            }

            page += 1;
        }

        Ok(refs)
    }

    async fn fetch_file_content(
        &self,
        owner: &str,
        repo: &str,
        tree_sha: &str,
        path: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        self.rate_limiter.check_rate_limit().await?;

        let download_url = get_download_url(owner, repo, tree_sha, path);
        let action = format!("download file content for `{path}` at ref `{tree_sha}`");

        let client = &self.client;
        let token = &self.token;
        let rate_limiter = &self.rate_limiter;

        let response = retry_with_adaptive_backoff(&action, 3, rate_limiter, || async {
            let mut headers = HeaderMap::new();
            headers.insert(USER_AGENT, HeaderValue::from_static(SPICE_USER_AGENT));

            add_optional_github_auth(&mut headers, token.as_ref());

            client.get(&download_url).headers(headers).send().await
        })
        .await
        .map_err(
            |e: reqwest::Error| -> Box<dyn std::error::Error + Send + Sync> {
                boxed_github_request_error(&action, owner, repo, &e)
            },
        )?;

        rate_limiter.update_from_headers(response.headers()).await;

        if response.status().is_success() {
            let content = response.text().await?;
            Ok(content)
        } else {
            let (response_headers, response_status, response_json, detail) =
                read_github_error_response(response).await;

            if let Some(response_json) = response_json.as_ref() {
                error_checker(&response_headers, response_json).map_err(|e| {
                    if let graphql::Error::RateLimited { message } = e {
                        Error::RateLimited { message }
                    } else {
                        Error::GithubApiError { source: e.into() }
                    }
                })?;
            }

            Err(boxed_github_status_error(
                &action,
                owner,
                repo,
                response_status,
                detail.as_deref(),
            ))
        }
    }

    async fn fetch_file_commits(
        &self,
        owner: &str,
        repo: &str,
        tree_sha: &str,
        path: &str,
    ) -> Result<Vec<GitCommit>, Box<dyn std::error::Error + Send + Sync>> {
        self.rate_limiter.check_rate_limit().await?;

        let endpoint = format!(
            "https://api.github.com/repos/{owner}/{repo}/commits?sha={tree_sha}&path={path}&per_page=100"
        );
        let action = format!("retrieve commit metadata for file `{path}` at ref `{tree_sha}`");

        let client = &self.client;
        let token = &self.token;
        let rate_limiter = &self.rate_limiter;

        let response = retry_with_adaptive_backoff(&action, 3, rate_limiter, || async {
            let mut headers = HeaderMap::new();
            headers.insert(USER_AGENT, HeaderValue::from_static(SPICE_USER_AGENT));
            headers.insert(
                ACCEPT,
                HeaderValue::from_static("application/vnd.github.v3+json"),
            );

            add_optional_github_auth(&mut headers, token.as_ref());

            client.get(&endpoint).headers(headers).send().await
        })
        .await;

        let response = match response {
            Ok(resp) => resp,
            Err(e) => {
                // Return empty vec on error rather than failing the entire operation
                tracing::warn!(
                    "{} The file metadata columns 'created_at' and 'updated_at' will be null.",
                    format_github_request_error(&action, owner, repo, &e)
                );
                return Ok(Vec::new());
            }
        };

        rate_limiter.update_from_headers(response.headers()).await;

        if response.status().is_success() {
            let commits = response.json::<Vec<GitCommit>>().await?;
            Ok(commits)
        } else {
            let (_, response_status, _, detail) = read_github_error_response(response).await;
            let message = format_github_status_error(
                &action,
                owner,
                repo,
                response_status,
                detail.as_deref(),
            );

            if matches!(response_status, StatusCode::NOT_FOUND | StatusCode::GONE) {
                tracing::debug!(
                    "{} The file metadata columns 'created_at' and 'updated_at' will be null.",
                    message
                );
            } else {
                tracing::warn!(
                    "{} The file metadata columns 'created_at' and 'updated_at' will be null.",
                    message
                );
            }

            Ok(Vec::new())
        }
    }

    #[expect(clippy::too_many_lines)]
    pub async fn fetch_workflow_runs(
        self: Arc<Self>,
        owner: Arc<str>,
        repo: Arc<str>,
        workflow_id: Arc<str>,
        query_params: Option<HashMap<String, String>>,
        limit: Option<usize>,
        fetch_logs: bool,
    ) -> Result<SendableRecordBatchStream, Box<dyn std::error::Error + Send + Sync>> {
        self.rate_limiter.check_rate_limit().await?;

        let endpoint = format!(
            "https://api.github.com/repos/{owner}/{repo}/actions/workflows/{workflow_id}/runs"
        );
        let action = format!("retrieve workflow runs for workflow `{workflow_id}`");

        let client = &self.client;
        let token = &self.token;
        let rate_limiter = &self.rate_limiter;

        let mut all_runs = Vec::new();
        let mut page = 1;
        let per_page = 100; // Maximum allowed by GitHub API

        loop {
            let mut url = url::Url::parse(&endpoint)?;
            url.query_pairs_mut()
                .append_pair("per_page", &per_page.to_string())
                .append_pair("page", &page.to_string());

            // Add query parameters if provided
            if let Some(ref params) = query_params {
                for (key, value) in params {
                    url.query_pairs_mut().append_pair(key, value);
                }
            }

            let url = url.to_string();

            let response = retry_with_adaptive_backoff(&action, 3, rate_limiter, || async {
                let mut headers = HeaderMap::new();
                headers.insert(USER_AGENT, HeaderValue::from_static(SPICE_USER_AGENT));
                headers.insert(
                    ACCEPT,
                    HeaderValue::from_static("application/vnd.github.v3+json"),
                );

                add_optional_github_auth(&mut headers, token.as_ref());

                tracing::debug!(owner = %owner, repo = %repo, workflow_id = %workflow_id, endpoint = %url, "Requesting GitHub workflow runs");

                client.get(&url).headers(headers).send().await
            })
            .await
            .map_err(|e: reqwest::Error| -> Box<dyn std::error::Error + Send + Sync> {
                boxed_github_request_error(&action, &owner, &repo, &e)
            })?;

            rate_limiter.update_from_headers(response.headers()).await;

            if !response.status().is_success() {
                let (response_headers, response_status, response_json, detail) =
                    read_github_error_response(response).await;

                if let Some(response_json) = response_json.as_ref() {
                    error_checker(&response_headers, response_json).map_err(|e| {
                        if let graphql::Error::RateLimited { message } = e {
                            Error::RateLimited { message }
                        } else {
                            Error::GithubApiError { source: e.into() }
                        }
                    })?;
                }

                return Err(boxed_github_status_error(
                    &action,
                    &owner,
                    &repo,
                    response_status,
                    detail.as_deref(),
                ));
            }

            let runs_response: WorkflowRunsResponse = response.json().await?;

            if runs_response.workflow_runs.is_empty() {
                break;
            }

            let num_runs = runs_response.workflow_runs.len();
            all_runs.extend(runs_response.workflow_runs);

            // Check if we've reached the limit
            if let Some(limit) = limit
                && all_runs.len() >= limit
            {
                all_runs.truncate(limit);
                break;
            }

            // If we got fewer than per_page results, we've reached the end
            if num_runs < per_page {
                break;
            }

            page += 1;
        }

        // Fetch logs for each run if requested
        let run_logs = if fetch_logs {
            let mut logs_map = std::collections::HashMap::new();
            for run in &all_runs {
                match self.fetch_workflow_run_logs(&owner, &repo, run.id).await {
                    Ok(logs) => {
                        logs_map.insert(run.id, logs);
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to retrieve workflow logs for GitHub repository {}/{} run {}: {} The 'logs' column will be empty for this run.",
                            owner,
                            repo,
                            run.id,
                            e
                        );
                        logs_map.insert(run.id, std::collections::HashMap::new());
                    }
                }
            }
            Some(logs_map)
        } else {
            None
        };

        // Build the RecordBatch from the collected runs
        let mut id_builder = arrow::array::Int64Builder::new();
        let mut name_builder = arrow::array::StringBuilder::new();
        let mut head_branch_builder = arrow::array::StringBuilder::new();
        let mut head_sha_builder = arrow::array::StringBuilder::new();
        let mut run_number_builder = arrow::array::Int64Builder::new();
        let mut display_title_builder = arrow::array::StringBuilder::new();
        let mut event_builder = arrow::array::StringBuilder::new();
        let mut status_builder = arrow::array::StringBuilder::new();
        let mut conclusion_builder = arrow::array::StringBuilder::new();
        let mut workflow_id_builder = arrow::array::Int64Builder::new();
        let mut run_started_at_builder = arrow::array::TimestampMillisecondBuilder::new();
        let mut jobs_url_builder = arrow::array::StringBuilder::new();

        for run in &all_runs {
            id_builder.append_value(run.id);
            match &run.name {
                Some(name) => name_builder.append_value(name),
                None => name_builder.append_null(),
            }
            match &run.head_branch {
                Some(branch) => head_branch_builder.append_value(branch),
                None => head_branch_builder.append_null(),
            }
            head_sha_builder.append_value(&run.head_sha);
            run_number_builder.append_value(run.run_number);
            display_title_builder.append_value(&run.display_title);
            event_builder.append_value(&run.event);
            match &run.status {
                Some(status) => status_builder.append_value(status),
                None => status_builder.append_null(),
            }
            match &run.conclusion {
                Some(conclusion) => conclusion_builder.append_value(conclusion),
                None => conclusion_builder.append_null(),
            }
            workflow_id_builder.append_value(run.workflow_id);
            match &run.run_started_at {
                Some(timestamp) => {
                    if let Ok(dt) = DateTime::parse_from_rfc3339(timestamp) {
                        run_started_at_builder.append_value(dt.timestamp_millis());
                    } else {
                        run_started_at_builder.append_null();
                    }
                }
                None => run_started_at_builder.append_null(),
            }
            jobs_url_builder.append_value(&run.jobs_url);
        }

        let mut fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("head_branch", DataType::Utf8, true),
            Field::new("head_sha", DataType::Utf8, false),
            Field::new("run_number", DataType::Int64, false),
            Field::new("display_title", DataType::Utf8, false),
            Field::new("event", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, true),
            Field::new("conclusion", DataType::Utf8, true),
            Field::new("workflow_id", DataType::Int64, false),
            Field::new(
                "run_started_at",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                true,
            ),
            Field::new("jobs_url", DataType::Utf8, false),
        ];

        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(id_builder.finish()),
            Arc::new(name_builder.finish()),
            Arc::new(head_branch_builder.finish()),
            Arc::new(head_sha_builder.finish()),
            Arc::new(run_number_builder.finish()),
            Arc::new(display_title_builder.finish()),
            Arc::new(event_builder.finish()),
            Arc::new(status_builder.finish()),
            Arc::new(conclusion_builder.finish()),
            Arc::new(workflow_id_builder.finish()),
            Arc::new(run_started_at_builder.finish()),
            Arc::new(jobs_url_builder.finish()),
        ];

        if let Some(logs_map) = run_logs {
            use arrow::array::{MapBuilder, StringBuilder as MapStringBuilder};

            fields.push(Field::new(
                "logs",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Field::new("keys", DataType::Utf8, false),
                                Field::new("values", DataType::Utf8, true),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ),
                true,
            ));

            let mut map_builder =
                MapBuilder::new(None, MapStringBuilder::new(), MapStringBuilder::new());

            for run in &all_runs {
                if let Some(logs) = logs_map.get(&run.id) {
                    for (key, value) in logs {
                        map_builder.keys().append_value(key);
                        map_builder.values().append_value(value);
                    }
                    map_builder.append(true)?;
                } else {
                    map_builder.append(false)?;
                }
            }

            columns.push(Arc::new(map_builder.finish()));
        }

        let schema = Arc::new(Schema::new(fields));

        let record_batch = RecordBatch::try_new(Arc::clone(&schema), columns)
            .context(UnableToConstructRecordBatchSnafu)?;

        let stream_adapter = RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter(vec![Ok(record_batch)]),
        );

        Ok(Box::pin(stream_adapter))
    }

    pub async fn fetch_workflow_run_logs(
        &self,
        owner: &str,
        repo: &str,
        run_id: i64,
    ) -> Result<std::collections::HashMap<String, String>, Box<dyn std::error::Error + Send + Sync>>
    {
        self.rate_limiter.check_rate_limit().await?;

        let endpoint =
            format!("https://api.github.com/repos/{owner}/{repo}/actions/runs/{run_id}/logs");
        let action = format!("retrieve workflow logs for run `{run_id}`");

        let client = &self.client;
        let token = &self.token;
        let rate_limiter = &self.rate_limiter;

        // GitHub returns a redirect to the actual ZIP file location
        let response = retry_with_adaptive_backoff(&action, 3, rate_limiter, || async {
            let mut headers = HeaderMap::new();
            headers.insert(USER_AGENT, HeaderValue::from_static(SPICE_USER_AGENT));
            headers.insert(
                ACCEPT,
                HeaderValue::from_static("application/vnd.github.v3+json"),
            );

            add_optional_github_auth(&mut headers, token.as_ref());

            tracing::debug!(owner, repo, run_id, endpoint = %endpoint, "Requesting GitHub workflow logs");

            // Don't follow redirects automatically - we need to handle them manually
            client.get(&endpoint).headers(headers).send().await
        })
        .await
        .map_err(|e: reqwest::Error| -> Box<dyn std::error::Error + Send + Sync> {
            boxed_github_request_error(&action, owner, repo, &e)
        })?;

        rate_limiter.update_from_headers(response.headers()).await;

        if !response.status().is_success() {
            let (_, response_status, _, detail) = read_github_error_response(response).await;
            let message = format_github_status_error(
                &action,
                owner,
                repo,
                response_status,
                detail.as_deref(),
            );

            if matches!(response_status, StatusCode::NOT_FOUND | StatusCode::GONE) {
                tracing::debug!(
                    "{} Workflow logs may have expired or are unavailable for this run.",
                    message
                );
            } else {
                tracing::warn!("{} Workflow logs will be omitted for this run.", message);
            }

            // Return empty map if logs aren't available
            return Ok(std::collections::HashMap::new());
        }

        // Download the ZIP file
        let zip_bytes = response.bytes().await?;

        // Offload ZIP parsing to another thread to avoid blocking async runtime
        let logs = tokio::task::spawn_blocking(move || {
            // Parse the ZIP file
            let cursor = std::io::Cursor::new(zip_bytes);
            let mut zip = zip::ZipArchive::new(cursor)?;

            let mut logs = std::collections::HashMap::new();

            // Extract only .txt files from the root of the ZIP
            for i in 0..zip.len() {
                let mut file = zip.by_index(i)?;
                let file_name = file.name().to_string();

                // Only process .txt files in the root (no directory separator)
                if std::path::Path::new(&file_name)
                    .extension()
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("txt"))
                    && !file_name.contains('/')
                {
                    let mut content = String::new();
                    std::io::Read::read_to_string(&mut file, &mut content)?;
                    logs.insert(file_name, content);
                }
            }

            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(logs)
        })
        .await??;

        Ok(logs)
    }

    pub async fn fetch_workflows(
        self: Arc<Self>,
        owner: Arc<str>,
        repo: Arc<str>,
        limit: Option<usize>,
    ) -> Result<SendableRecordBatchStream, Box<dyn std::error::Error + Send + Sync>> {
        self.rate_limiter.check_rate_limit().await?;

        let endpoint = format!("https://api.github.com/repos/{owner}/{repo}/actions/workflows");
        let action = "retrieve workflows".to_string();

        let client = &self.client;
        let token = &self.token;
        let rate_limiter = &self.rate_limiter;

        let mut all_workflows = Vec::new();
        let mut page = 1;
        let per_page = 100; // Maximum allowed by GitHub API

        loop {
            let mut url = format!("{endpoint}?per_page={per_page}&page={page}");
            if let Some(limit) = limit {
                let remaining_items = limit.saturating_sub(all_workflows.len());
                if remaining_items == 0 {
                    break;
                }
                let current_per_page = std::cmp::min(per_page, remaining_items);
                url = format!("{endpoint}?per_page={current_per_page}&page={page}");
            }

            tracing::debug!(owner = %owner, repo = %repo, endpoint = %url, "Requesting GitHub workflows");

            let response = retry_with_adaptive_backoff(&action, 3, rate_limiter, || async {
                let mut headers = HeaderMap::new();
                headers.insert(USER_AGENT, HeaderValue::from_static(SPICE_USER_AGENT));
                headers.insert(ACCEPT, HeaderValue::from_static("application/json"));

                add_optional_github_auth(&mut headers, token.as_ref());

                client.get(&url).headers(headers).send().await
            })
            .await
            .map_err(
                |e: reqwest::Error| -> Box<dyn std::error::Error + Send + Sync> {
                    boxed_github_request_error(&action, &owner, &repo, &e)
                },
            )?;

            rate_limiter.update_from_headers(response.headers()).await;

            if !response.status().is_success() {
                let (response_headers, response_status, response_json, detail) =
                    read_github_error_response(response).await;

                if let Some(response_json) = response_json.as_ref() {
                    error_checker(&response_headers, response_json).map_err(|e| {
                        if let graphql::Error::RateLimited { message } = e {
                            Error::RateLimited { message }
                        } else {
                            Error::GithubApiError { source: e.into() }
                        }
                    })?;
                }

                return Err(boxed_github_status_error(
                    &action,
                    &owner,
                    &repo,
                    response_status,
                    detail.as_deref(),
                ));
            }

            let workflows_response: WorkflowsResponse = response.json().await?;

            if workflows_response.workflows.is_empty() {
                break;
            }

            all_workflows.extend(workflows_response.workflows);

            if let Some(limit) = limit
                && all_workflows.len() >= limit
            {
                all_workflows.truncate(limit);
                break;
            }

            if all_workflows.len()
                >= usize::try_from(workflows_response.total_count).map_err(Box::new)?
            {
                break;
            }

            page += 1;
        }

        // Build the RecordBatch from the collected workflows
        let mut id_builder = arrow::array::Int64Builder::new();
        let mut name_builder = arrow::array::StringBuilder::new();
        let mut path_builder = arrow::array::StringBuilder::new();
        let mut state_builder = arrow::array::StringBuilder::new();
        let mut created_at_builder = arrow::array::TimestampMillisecondBuilder::new();
        let mut updated_at_builder = arrow::array::TimestampMillisecondBuilder::new();
        let mut badge_url_builder = arrow::array::StringBuilder::new();

        for workflow in &all_workflows {
            id_builder.append_value(workflow.id);
            name_builder.append_value(&workflow.name);
            path_builder.append_value(&workflow.path);
            state_builder.append_value(&workflow.state);

            // Parse created_at timestamp
            if let Ok(dt) = DateTime::parse_from_rfc3339(&workflow.created_at) {
                created_at_builder.append_value(dt.timestamp_millis());
            } else {
                created_at_builder.append_null();
            }

            // Parse updated_at timestamp
            if let Ok(dt) = DateTime::parse_from_rfc3339(&workflow.updated_at) {
                updated_at_builder.append_value(dt.timestamp_millis());
            } else {
                updated_at_builder.append_null();
            }

            badge_url_builder.append_value(&workflow.badge_url);
        }

        let fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("path", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new(
                "updated_at",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new("badge_url", DataType::Utf8, false),
        ];

        let columns: Vec<ArrayRef> = vec![
            Arc::new(id_builder.finish()),
            Arc::new(name_builder.finish()),
            Arc::new(path_builder.finish()),
            Arc::new(state_builder.finish()),
            Arc::new(created_at_builder.finish()),
            Arc::new(updated_at_builder.finish()),
            Arc::new(badge_url_builder.finish()),
        ];

        let schema = Arc::new(Schema::new(fields));

        let record_batch = RecordBatch::try_new(Arc::clone(&schema), columns)
            .context(UnableToConstructRecordBatchSnafu)?;

        let stream_adapter = RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter(vec![Ok(record_batch)]),
        );

        Ok(Box::pin(stream_adapter))
    }
}

fn extract_name_from_path(path: &str) -> Option<&str> {
    Path::new(path).file_name().and_then(|name| name.to_str())
}

fn get_download_url(owner: &str, repo: &str, tree_sha: &str, path: &str) -> String {
    format!("https://raw.githubusercontent.com/{owner}/{repo}/{tree_sha}/{path}")
}

#[derive(Debug, Deserialize)]
struct GitTree {
    tree: Vec<GitTreeNode>,
}

#[derive(Debug, Deserialize)]
struct GitHubRepository {
    default_branch: String,
}

#[derive(Debug, Deserialize)]
struct GitRefName {
    name: String,
}

#[derive(Debug, Deserialize)]
struct GitQualifiedRef {
    #[serde(rename = "ref")]
    qualified_name: String,
}

fn short_ref_name(qualified_name: &str) -> String {
    qualified_name
        .strip_prefix("refs/heads/")
        .or_else(|| qualified_name.strip_prefix("refs/tags/"))
        .or_else(|| qualified_name.strip_prefix("refs/"))
        .unwrap_or(qualified_name)
        .to_string()
}

fn git_ref_endpoint(
    owner: &str,
    repo: &str,
    git_ref_path: &str,
) -> Result<Url, Box<dyn std::error::Error + Send + Sync>> {
    let mut endpoint = Url::parse(&format!(
        "https://api.github.com/repos/{owner}/{repo}/git/ref/"
    ))
    .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;

    {
        let mut path_segments = endpoint.path_segments_mut().map_err(|()| {
            Box::new(std::io::Error::other(format!(
                "Failed to construct GitHub ref URL for {owner}/{repo}: {git_ref_path}"
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;
        path_segments.pop_if_empty();
        for segment in git_ref_path.split('/') {
            path_segments.push(segment);
        }
    }

    Ok(endpoint)
}

#[derive(Debug, Deserialize)]
struct GitTreeNode {
    path: String,
    mode: String,
    #[serde(rename = "type")]
    node_type: String,
    sha: String,
    size: Option<i64>,
    url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GitCommit {
    /// SHA of the commit. Required for deserialization but not used.
    #[serde(rename = "sha")]
    _sha: String,
    commit: GitCommitDetails,
}

#[derive(Debug, Deserialize)]
struct GitCommitDetails {
    author: GitCommitAuthor,
    /// Committer information. Required for deserialization but not used.
    #[serde(rename = "committer")]
    _committer: GitCommitAuthor,
}

#[derive(Debug, Deserialize)]
struct GitCommitAuthor {
    date: String,
}

#[derive(Debug, Deserialize)]
pub struct WorkflowsResponse {
    pub total_count: i64,
    pub workflows: Vec<Workflow>,
}

#[derive(Debug, Deserialize)]
pub struct Workflow {
    pub id: i64,
    pub name: String,
    pub path: String,
    pub state: String,
    pub created_at: String,
    pub updated_at: String,
    pub badge_url: String,
}

#[derive(Debug, Deserialize)]
pub struct WorkflowRunsResponse {
    pub total_count: i64,
    pub workflow_runs: Vec<WorkflowRun>,
}

#[derive(Debug, Deserialize)]
pub struct WorkflowRun {
    pub id: i64,
    pub name: Option<String>,
    pub head_branch: Option<String>,
    pub head_sha: String,
    pub run_number: i64,
    pub display_title: String,
    pub event: String,
    pub status: Option<String>,
    pub conclusion: Option<String>,
    pub workflow_id: i64,
    pub run_started_at: Option<String>,
    pub jobs_url: String,
}

// For GitHub, first checks if an explicit rate limit error was returned, then checks the headers
pub fn error_checker(
    headers: &HeaderMap<HeaderValue>,
    response: &Value,
) -> Result<(), graphql::Error> {
    // check if there's an explicit rate limit error
    let rate_limited: Option<bool> = response["message"]
        .as_str()
        .map(|s| s.to_lowercase().contains("rate limit"));
    if rate_limited == Some(true) {
        // A secondary rate limit was exceeded
        return Err(graphql::Error::RateLimited {
            message: "GitHub API rate limit exceeded. Consider reducing dataset 'max_concurrent_requests' or runtime.source_rate_control.github_concurrent_connections_limit in your spicepod to avoid rate limits. See: https://spiceai.org/docs/components/data-connectors/github".to_string(),
        });
    }

    // Check if the primary rate limit is exceeded
    if let Some(ratelimit_remaining) = headers.get("x-ratelimit-remaining") {
        let ratelimit_remaining = ratelimit_remaining
            .to_str()
            .unwrap_or("1")
            .parse::<u32>()
            .unwrap_or(1);
        if ratelimit_remaining == 0 {
            return Err(graphql::Error::RateLimited {
                message: "GitHub API rate limit exceeded. Consider reducing dataset 'max_concurrent_requests' or runtime.source_rate_control.github_concurrent_connections_limit in your spicepod to avoid rate limits. See: https://spiceai.org/docs/components/data-connectors/github".to_string(),
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        Error, boxed_github_status_error, format_github_status_error, git_ref_endpoint,
        github_response_message, github_response_message_from_text, ref_from_filter,
        requested_ref_from_filters,
    };
    use datafusion::prelude::{col, lit};
    use datafusion::scalar::ScalarValue;
    use reqwest::StatusCode;
    use serde_json::json;

    #[test]
    fn test_ref_from_filter_supports_simple_equality() {
        let expr = col("ref").eq(lit("trunk"));

        assert_eq!(ref_from_filter(&expr).as_deref(), Some("trunk"));
    }

    #[test]
    fn test_ref_from_filter_supports_utf8view_literal() {
        use datafusion::logical_expr::Expr;
        let expr = col("ref").eq(Expr::Literal(
            ScalarValue::Utf8View(Some("trunk".to_string())),
            None,
        ));

        assert_eq!(ref_from_filter(&expr).as_deref(), Some("trunk"));
    }

    #[test]
    fn test_ref_from_filter_supports_large_utf8_literal() {
        use datafusion::logical_expr::Expr;
        let expr = col("ref").eq(Expr::Literal(
            ScalarValue::LargeUtf8(Some("trunk".to_string())),
            None,
        ));

        assert_eq!(ref_from_filter(&expr).as_deref(), Some("trunk"));
    }

    #[test]
    fn test_ref_from_filter_rejects_empty_string() {
        let expr = col("ref").eq(lit(""));

        assert_eq!(ref_from_filter(&expr), None);
    }

    #[test]
    fn test_requested_ref_from_filters_uses_first_supported_ref_filter() {
        let filters = vec![
            col("ref").eq(lit("release/v1")),
            col("path").eq(lit("README.md")),
        ];

        assert_eq!(
            requested_ref_from_filters(&filters)
                .expect("simple ref filter should be supported")
                .as_deref(),
            Some("release/v1")
        );
    }

    #[test]
    fn test_requested_ref_from_filters_supports_conjunctive_filter() {
        let filters = vec![
            col("ref")
                .eq(lit("trunk"))
                .and(col("path").eq(lit("README.md"))),
        ];

        assert_eq!(
            requested_ref_from_filters(&filters)
                .expect("conjunctive ref filter should be supported")
                .as_deref(),
            Some("trunk")
        );
    }

    #[test]
    fn test_requested_ref_from_filters_rejects_multiple_ref_values() {
        let filters = vec![col("ref").eq(lit("trunk")), col("ref").eq(lit("main"))];

        let _ = requested_ref_from_filters(&filters)
            .expect_err("multiple ref values should be rejected");
    }

    #[test]
    fn test_requested_ref_from_filters_rejects_unsupported_ref_or_predicate() {
        let filters = vec![col("ref").eq(lit("trunk")).or(col("ref").eq(lit("main")))];

        let _ = requested_ref_from_filters(&filters)
            .expect_err("unsupported ref OR predicates should be rejected");
    }

    #[test]
    fn test_github_response_message_prefers_message_and_docs_url() {
        let response = json!({
            "message": "Resource not accessible by integration",
            "documentation_url": "https://docs.github.com/rest"
        });

        assert_eq!(
            github_response_message(&response).as_deref(),
            Some("Resource not accessible by integration See https://docs.github.com/rest")
        );
    }

    #[test]
    fn test_format_github_status_error_includes_detail_and_no_newlines() {
        let message = format_github_status_error(
            "retrieve workflows",
            "spiceai",
            "spiceai",
            StatusCode::FORBIDDEN,
            Some("Resource not accessible by integration"),
        );

        assert!(message.contains("spiceai/spiceai"));
        assert!(message.contains("required permissions"));
        assert!(message.contains("Resource not accessible by integration"));
        assert!(!message.contains('\n'));
    }

    #[test]
    fn test_github_response_message_from_text_sanitizes_newlines() {
        assert_eq!(
            github_response_message_from_text("first line\nsecond line\r\nthird line").as_deref(),
            Some("first line second line third line")
        );
    }

    #[test]
    fn test_boxed_github_status_error_preserves_github_error_type() {
        let err = boxed_github_status_error(
            "retrieve workflows",
            "spiceai",
            "spiceai",
            StatusCode::SERVICE_UNAVAILABLE,
            Some("GitHub is down"),
        );

        assert!(matches!(
            err.downcast_ref::<Error>(),
            Some(Error::GithubApiError { .. })
        ));
    }

    #[test]
    fn test_git_ref_endpoint_percent_encodes_each_path_segment() {
        let endpoint =
            git_ref_endpoint("spiceai", "spiceai", "heads/feature#1").expect("valid endpoint");

        assert_eq!(
            endpoint.as_str(),
            "https://api.github.com/repos/spiceai/spiceai/git/ref/heads/feature%231"
        );
    }
}
