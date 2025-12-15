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
use async_trait::async_trait;
use chrono::DateTime;
use futures::future;
use globset::GlobSet;
use serde_json::Value;
use snafu::{ResultExt, Snafu};

use crate::{arrow::write::MemTable, graphql, rate_limit::RateLimiter};
use arrow::{
    array::{ArrayRef, Int64Builder, RecordBatch, StringBuilder, TimestampMillisecondBuilder},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use datafusion::{
    catalog::Session,
    datasource::{TableProvider, TableType},
    error::DataFusionError,
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_plan::ExecutionPlan,
};
use std::{any::Any, path::Path, sync::Arc, time::Duration};
use token_provider::TokenProvider;
use util::ExponentialBackoff;
use util::fibonacci_backoff::{Backoff, FibonacciBackoffBuilder};

use reqwest::header::{ACCEPT, AUTHORIZATION, HeaderMap, HeaderValue, USER_AGENT};
use serde::Deserialize;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error executing query. {source}"))]
    UnableToConstructRecordBatchError { source: arrow::error::ArrowError },

    #[snafu(display("Error executing query. {source}"))]
    GithubApiError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("{message}"))]
    RateLimited { message: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct GithubFilesTableProvider {
    client: GithubRestClient,
    owner: Arc<str>,
    repo: Arc<str>,
    tree_sha: Arc<str>,
    schema: SchemaRef,
    include: Option<Arc<GlobSet>>,
    fetch_content: bool,
    include_commits: bool,
}

impl GithubFilesTableProvider {
    pub async fn new(
        client: GithubRestClient,
        owner: &str,
        repo: &str,
        tree_sha: &str,
        include: Option<Arc<GlobSet>>,
        fetch_content: bool,
        include_commits: bool,
    ) -> Result<Self> {
        let mut fields = vec![
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

        // ensure configuration is correct
        client
            .fetch_files(
                owner,
                repo,
                tree_sha,
                Some(1),
                None,
                fetch_content,
                include_commits,
                Arc::clone(&schema),
            )
            .await?;

        Ok(Self {
            client,
            owner: owner.into(),
            repo: repo.into(),
            tree_sha: tree_sha.into(),
            schema,
            include,
            fetch_content,
            include_commits,
        })
    }
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
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let res: Vec<RecordBatch> = self
            .client
            .fetch_files(
                &self.owner,
                &self.repo,
                &self.tree_sha,
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

pub struct GithubRestClient {
    client: reqwest::Client,
    token: Arc<dyn TokenProvider>,
    rate_limiter: Arc<dyn RateLimiter>,
}

impl std::fmt::Debug for GithubRestClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GithubRestClient")
            .field("token", &self.token)
            .finish_non_exhaustive()
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
        max_elapsed_time: Some(std::time::Duration::from_secs(300)), // 5 minutes max total retry time
        ..ExponentialBackoff::default()
    };

    let mut exponential_retry_count = 0_usize;

    loop {
        // Check rate limit before each attempt
        // The rate limiter handles waiting based on rate limit info from previous responses
        // This always returns Ok(()) after waiting if needed
        rate_limiter.check_rate_limit().await.ok();

        match operation().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                match classify_retryable_error(&e) {
                    Some(RetryableErrorType::RateLimit) => {
                        // Check if we've exceeded max retries
                        if exponential_retry_count >= max_retries {
                            tracing::warn!(
                                "GitHub API rate limit error, max retries ({max_retries}) exceeded: {e}"
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
                                "GitHub API rate limit error, will check rate limit and retry (attempt {exponential_retry_count}/{max_retries}): {e}"
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
                                "GitHub API server/network error, retrying with fibonacci backoff in {:?}: {}",
                                duration,
                                e
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

impl GithubRestClient {
    pub fn new(
        token: Arc<dyn TokenProvider>,
        rate_limiter: Arc<dyn RateLimiter>,
    ) -> reqwest::Result<Self> {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(120))
            .build()?;

        Ok(GithubRestClient {
            client,
            token,
            rate_limiter,
        })
    }

    #[expect(clippy::too_many_arguments)]
    #[expect(clippy::too_many_lines)]
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

                for res in results {
                    content_builder.append_value(res.context(GithubApiSnafu)?);
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

        let client = &self.client;
        let token = &self.token;
        let rate_limiter = &self.rate_limiter;

        let response = retry_with_adaptive_backoff(5, rate_limiter, || async {
            let mut headers = HeaderMap::new();
            headers.insert(USER_AGENT, HeaderValue::from_static(SPICE_USER_AGENT));
            headers.insert(
                ACCEPT,
                HeaderValue::from_static("application/vnd.github.v3+json"),
            );

            if let Ok(header) = HeaderValue::from_str(&format!("token {}", token.get_token())) {
                headers.insert(AUTHORIZATION, header);
            }

            tracing::debug!("fetch_git_tree: endpoint: {}", endpoint);

            client.get(&endpoint).headers(headers).send().await
        })
        .await
        .map_err(|e: reqwest::Error| -> Box<dyn std::error::Error + Send + Sync> {
            if let Some(status) = e.status() {
                let code = status.as_u16();
                if (500..600).contains(&code) {
                    format!(
                        "GitHub API returned server error ({code}) for endpoint: {endpoint}. Spice automatically retried with fibonacci backoff.",
                    )
                    .into()
                } else if code == 408 || code == 429 {
                    format!(
                        "GitHub API returned rate limit/timeout error ({code}) for endpoint: {endpoint}. Spice automatically retried with exponential backoff.",
                    )
                    .into()
                } else {
                    e.into()
                }
            } else {
                e.into()
            }
        })?;

        if response.status().is_success() {
            let git_tree = response.json::<GitTree>().await?;
            tracing::trace!("fetch_git_tree returned {} entities", git_tree.tree.len());
            return Ok(git_tree);
        }

        let response_headers = response.headers().clone();
        let response_status = response.status().as_u16();
        let response: Value = response.json().await?;

        rate_limiter.update_from_headers(&response_headers).await;

        error_checker(&response_headers, &response).map_err(|e| {
            if let graphql::Error::RateLimited { message } = e {
                Error::RateLimited { message }
            } else {
                Error::GithubApiError { source: e.into() }
            }
        })?;

        match response_status {
            404 => {
                let err_msg = format!(
                    "The Github API ({endpoint}) failed with status code {response_status}. Verify that org `{owner}`, repo `{repo}` and git tree `{tree_sha}`are correct.",
                );
                Err(err_msg.into())
            }
            401 => {
                let err_msg = format!(
                    "The Github API ({endpoint}) failed with status code {response_status}. Verify the token is correct.",
                );
                Err(err_msg.into())
            }
            403 => {
                let err_msg = format!(
                    "The Github API ({endpoint}) failed with status code {response_status}. Verify the token has the necessary permissions.",
                );
                Err(err_msg.into())
            }
            503 => {
                let err_msg = format!(
                    "The Github API ({endpoint}) is temporarily unavailable (503 Service Unavailable). This typically means GitHub is experiencing issues. Spice will automatically retry with fibonacci backoff.",
                );
                Err(err_msg.into())
            }
            502 | 504 => {
                let err_msg = format!(
                    "The Github API ({endpoint}) returned a gateway error ({response_status}). This is typically a temporary issue. Spice will automatically retry with fibonacci backoff.",
                );
                Err(err_msg.into())
            }
            _ => {
                let err_msg = format!(
                    "The Github API ({endpoint}) failed with status code {response_status}",
                );
                Err(err_msg.into())
            }
        }
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

        let client = &self.client;
        let token = &self.token;
        let rate_limiter = &self.rate_limiter;

        let response = retry_with_adaptive_backoff(3, rate_limiter, || async {
            let mut headers = HeaderMap::new();
            headers.insert(USER_AGENT, HeaderValue::from_static(SPICE_USER_AGENT));

            if let Ok(header) = HeaderValue::from_str(&format!("token {}", token.get_token())) {
                headers.insert(AUTHORIZATION, header);
            }

            client.get(&download_url).headers(headers).send().await
        })
        .await
        .map_err(|e: reqwest::Error| -> Box<dyn std::error::Error + Send + Sync> { e.into() })?;

        rate_limiter.update_from_headers(response.headers()).await;

        if response.status().is_success() {
            let content = response.text().await?;
            Ok(content)
        } else {
            let status = response.status();
            let code = status.as_u16();
            let err_msg = if (500..600).contains(&code) {
                format!(
                    "GitHub API returned server error ({code}) when downloading file content for: {path}. Spice automatically retried with fibonacci backoff.",
                )
            } else if code == 408 || code == 429 {
                format!(
                    "GitHub API returned rate limit/timeout error ({code}) when downloading file content for: {path}. Spice automatically retried with exponential backoff.",
                )
            } else {
                format!("Failed to download file content for {path}: {status}")
            };
            Err(err_msg.into())
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

        let client = &self.client;
        let token = &self.token;
        let rate_limiter = &self.rate_limiter;

        let response = retry_with_adaptive_backoff(3, rate_limiter, || async {
            let mut headers = HeaderMap::new();
            headers.insert(USER_AGENT, HeaderValue::from_static(SPICE_USER_AGENT));
            headers.insert(
                ACCEPT,
                HeaderValue::from_static("application/vnd.github.v3+json"),
            );

            if let Ok(header) = HeaderValue::from_str(&format!("token {}", token.get_token())) {
                headers.insert(AUTHORIZATION, header);
            }

            client.get(&endpoint).headers(headers).send().await
        })
        .await;

        let response = match response {
            Ok(resp) => resp,
            Err(e) => {
                // Return empty vec on error rather than failing the entire operation
                tracing::warn!("Failed to fetch commits for file: {path}: {e}");
                return Ok(Vec::new());
            }
        };

        rate_limiter.update_from_headers(response.headers()).await;

        if response.status().is_success() {
            let commits = response.json::<Vec<GitCommit>>().await?;
            Ok(commits)
        } else {
            // Return empty vec on error rather than failing the entire operation
            tracing::debug!(
                "GitHub API returned status {} for commits fetch",
                response.status()
            );
            Ok(Vec::new())
        }
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
            message: "GitHub API rate limit exceeded. Consider reducing 'github_max_concurrent_connections' in your spicepod to avoid rate limits. See: https://spiceai.org/docs/components/data-connectors/github".to_string(),
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
                message: "GitHub API rate limit exceeded. Consider reducing 'github_max_concurrent_connections' in your spicepod to avoid rate limits. See: https://spiceai.org/docs/components/data-connectors/github".to_string(),
            });
        }
    }

    Ok(())
}
