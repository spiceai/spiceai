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
use futures::future;
use globset::GlobSet;
use serde_json::Value;
use snafu::{ResultExt, Snafu};

use crate::{
    arrow::write::MemTable, graphql, rate_limit::RateLimiter, token_provider::TokenProvider,
};
use arrow::{
    array::{ArrayRef, Int64Builder, RecordBatch, StringBuilder},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use datafusion::{
    catalog::Session,
    datasource::{TableProvider, TableType},
    error::DataFusionError,
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_plan::ExecutionPlan,
};
use std::{any::Any, path::Path, sync::Arc};

use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, AUTHORIZATION, USER_AGENT};
use serde::Deserialize;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error executing query.\n{source}"))]
    UnableToConstructRecordBatchError { source: arrow::error::ArrowError },

    #[snafu(display("Error executing query.\n{source}"))]
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
}

impl GithubFilesTableProvider {
    pub async fn new(
        client: GithubRestClient,
        owner: &str,
        repo: &str,
        tree_sha: &str,
        include: Option<Arc<GlobSet>>,
        fetch_content: bool,
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

impl GithubRestClient {
    #[must_use]
    pub fn new(token: Arc<dyn TokenProvider>, rate_limiter: Arc<dyn RateLimiter>) -> Self {
        let client = reqwest::Client::new();
        GithubRestClient {
            client,
            token,
            rate_limiter,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn fetch_files(
        &self,
        owner: &str,
        repo: &str,
        tree_sha: &str,
        limit: Option<usize>,
        include_pattern: Option<Arc<GlobSet>>,
        fetch_content: bool,
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
        for node in &tree {
            name_builder.append_value(extract_name_from_path(&node.path).unwrap_or_default());
            path_builder.append_value(&node.path);
            size_builder.append_value(node.size.unwrap_or(0));
            sha_builder.append_value(&node.sha);
            mode_builder.append_value(&node.mode);
            match &node.url {
                Some(url) => url_builder.append_value(url),
                None => url_builder.append_null(),
            }
            download_url_builder.append_value(get_download_url(owner, repo, tree_sha, &node.path));
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

        if fetch_content {
            let mut content_builder = StringBuilder::new();

            // download content in parallel
            for chunk in tree.chunks(NUM_FILE_CONTENT_DOWNLOAD_WORKERS) {
                let content_fetch_futures = chunk
                    .iter()
                    .map(|node| self.fetch_file_content(owner, repo, tree_sha, &node.path))
                    .collect::<Vec<_>>();

                for res in future::join_all(content_fetch_futures).await {
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

        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static(SPICE_USER_AGENT));
        headers.insert(
            ACCEPT,
            HeaderValue::from_static("application/vnd.github.v3+json"),
        );

        let token = self.token.get_token().await?;

        if let Ok(header) = HeaderValue::from_str(&format!("token {token}")) {
            headers.insert(AUTHORIZATION, header);
        }

        tracing::debug!("fetch_git_tree: endpoint: {}", endpoint);

        let response = self.client.get(&endpoint).headers(headers).send().await?;

        if response.status().is_success() {
            let git_tree = response.json::<GitTree>().await?;
            tracing::trace!("fetch_git_tree returned {} entities", git_tree.tree.len());
            return Ok(git_tree);
        }

        let response_headers = response.headers().clone();
        let response_status = response.status().as_u16();
        let response: Value = response.json().await?;

        self.rate_limiter
            .update_from_headers(&response_headers)
            .await;

        error_checker(&response_headers, &response).map_err(|e| {
            if let graphql::Error::RateLimited { message } = e {
                Error::RateLimited { message }
            } else {
                Error::GithubApiError { source: e.into() }
            }
        })?;

        #[allow(clippy::single_match_else)]
        match response_status {
            404 => {
                let err_msg = format!(
                    "The Github API ({endpoint}) failed with status code {response_status}.\nVerify that org `{owner}`, repo `{repo}` and git tree `{tree_sha}`are correct.",
                );
                Err(err_msg.into())
            }
            401 => {
                let err_msg = format!(
                    "The Github API ({endpoint}) failed with status code {response_status}.\nVerify the token is correct.",
                );
                Err(err_msg.into())
            }
            403 => {
                let err_msg = format!(
                    "The Github API ({endpoint}) failed with status code {response_status}.\nVerify the token has the necessary permissions.",
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

        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static(SPICE_USER_AGENT));

        let token = self.token.get_token().await?;

        if let Ok(header) = HeaderValue::from_str(&format!("token {token}")) {
            headers.insert(AUTHORIZATION, header);
        }

        let response = self
            .client
            .get(&download_url)
            .headers(headers)
            .send()
            .await?;

        self.rate_limiter
            .update_from_headers(response.headers())
            .await;

        if response.status().is_success() {
            let content = response.text().await?;
            Ok(content)
        } else {
            let err_msg = format!("Failed to download file content: {}", response.status());
            Err(err_msg.into())
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

// For GitHub, first checks if an explicit rate limit error was returned, then checks the headers
pub fn error_checker(
    headers: &HeaderMap<HeaderValue>,
    response: &Value,
) -> Result<(), graphql::Error> {
    // check if there's an explicit rate limit error
    let rate_limited: Option<bool> = response["message"]
        .as_str()
        .map(|s| s.to_lowercase().contains("rate limit"));
    if let Some(true) = rate_limited {
        // A secondary rate limit was exceeded
        return Err(graphql::Error::RateLimited {
            message: "GitHub API rate limit exceeded. Try again later, and add a LIMIT clause to your query to reduce the number of requests.".to_string(),
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
                message: "GitHub API rate limit exceeded. Try again later, and add a LIMIT clause to your query to reduce the number of requests.".to_string(),
            });
        }
    }

    Ok(())
}
