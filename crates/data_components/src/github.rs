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
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_plan::{ExecutionPlan, stream::RecordBatchStreamAdapter},
};
use std::{any::Any, collections::HashMap, path::Path, sync::Arc, time::Duration};
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

#[derive(Clone)]
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
                                "GitHub API server/network error, retrying with fibonacci backoff in {duration:?}: {e}",
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

            tracing::debug!("fetch_git_tree: endpoint: {endpoint}");

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

            let response = retry_with_adaptive_backoff(3, rate_limiter, || async {
                let mut headers = HeaderMap::new();
                headers.insert(USER_AGENT, HeaderValue::from_static(SPICE_USER_AGENT));
                headers.insert(
                    ACCEPT,
                    HeaderValue::from_static("application/vnd.github.v3+json"),
                );

                if let Ok(header) = HeaderValue::from_str(&format!("token {}", token.get_token()))
                {
                    headers.insert(AUTHORIZATION, header);
                }

                tracing::debug!("fetch_workflow_runs: endpoint: {url}");

                client.get(&url).headers(headers).send().await
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

            rate_limiter.update_from_headers(response.headers()).await;

            if !response.status().is_success() {
                let response_headers = response.headers().clone();
                let response_status = response.status().as_u16();
                let response_json: Value = response.json().await?;

                error_checker(&response_headers, &response_json).map_err(|e| {
                    if let graphql::Error::RateLimited { message } = e {
                        Error::RateLimited { message }
                    } else {
                        Error::GithubApiError { source: e.into() }
                    }
                })?;

                match response_status {
                    404 => {
                        return Err(format!(
                            "The GitHub API ({endpoint}) failed with status code {response_status}. Verify that org `{owner}`, repo `{repo}` and workflow `{workflow_id}` are correct.",
                        ).into());
                    }
                    401 => {
                        return Err(format!(
                            "The GitHub API ({endpoint}) failed with status code {response_status}. Verify the token is correct.",
                        ).into());
                    }
                    403 => {
                        return Err(format!(
                            "The GitHub API ({endpoint}) failed with status code {response_status}. Verify the token has the necessary permissions.",
                        ).into());
                    }
                    _ => {
                        return Err(format!(
                            "The GitHub API ({endpoint}) failed with status code {response_status}",
                        )
                        .into());
                    }
                }
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
                        tracing::warn!("Failed to fetch logs for workflow run {}: {e}", run.id);
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

        let client = &self.client;
        let token = &self.token;
        let rate_limiter = &self.rate_limiter;

        // GitHub returns a redirect to the actual ZIP file location
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

            tracing::debug!("fetch_workflow_run_logs: endpoint: {endpoint}");

            // Don't follow redirects automatically - we need to handle them manually
            client.get(&endpoint).headers(headers).send().await
        })
        .await
        .map_err(|e: reqwest::Error| -> Box<dyn std::error::Error + Send + Sync> { e.into() })?;

        rate_limiter.update_from_headers(response.headers()).await;

        if !response.status().is_success() {
            tracing::debug!(
                "Failed to fetch logs for run {run_id}: status {}",
                response.status()
            );
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

            tracing::debug!("fetch_workflows: endpoint: {url}");

            let response = retry_with_adaptive_backoff(3, rate_limiter, || async {
                let mut headers = HeaderMap::new();
                headers.insert(USER_AGENT, HeaderValue::from_static(SPICE_USER_AGENT));
                headers.insert(ACCEPT, HeaderValue::from_static("application/json"));

                if let Ok(header) = HeaderValue::from_str(&format!("token {}", token.get_token())) {
                    headers.insert(AUTHORIZATION, header);
                }

                client.get(&url).headers(headers).send().await
            })
            .await
            .map_err(
                |e: reqwest::Error| -> Box<dyn std::error::Error + Send + Sync> { e.into() },
            )?;

            rate_limiter.update_from_headers(response.headers()).await;

            if !response.status().is_success() {
                let status = response.status();
                let error_body = response.text().await.unwrap_or_default();
                return Err(format!(
                    "Failed to fetch workflows from GitHub API. Status: {status}, Error: {error_body}"
                )
                .into());
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
