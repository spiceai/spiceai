/*
Copyright 2024 The Spice.ai OSS Authors

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
use octocrab::{models::commits::Commit, Octocrab, Page};
use snafu::{ResultExt, Snafu};

use crate::{arrow::write::MemTable, graphql::GraphQLOptimizer};
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
use std::{any::Any, path::Path, sync::Arc};

use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, AUTHORIZATION, USER_AGENT};
use serde::Deserialize;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error executing query: {source}"))]
    UnableToConstructRecordBatchError { source: arrow::error::ArrowError },

    #[snafu(display("Error executing query: {source}"))]
    GithubApiError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

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
    octocrab_client: Option<Octocrab>,
    token: Arc<str>,
}

static SPICE_USER_AGENT: &str = "spice";
const NUM_FILE_CONTENT_DOWNLOAD_WORKERS: usize = 10;

impl GithubRestClient {
    #[must_use]
    pub fn new(token: &str) -> Self {
        let client = reqwest::Client::new();
        GithubRestClient {
            client,
            octocrab_client: None,
            token: token.into(),
        }
    }

    #[must_use]
    pub fn with_octocrab(mut self) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        self.octocrab_client = Some(
            Octocrab::builder()
                .personal_token(self.token.to_string())
                .build()?,
        );
        Ok(self)
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
        let endpoint = format!(
            "https://api.github.com/repos/{owner}/{repo}/git/trees/{tree_sha}?recursive=true"
        );

        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static(SPICE_USER_AGENT));
        headers.insert(
            ACCEPT,
            HeaderValue::from_static("application/vnd.github.v3+json"),
        );

        if let Ok(header) = HeaderValue::from_str(&format!("token {}", self.token)) {
            headers.insert(AUTHORIZATION, header);
        }

        tracing::debug!("fetch_git_tree: endpoint: {}", endpoint);

        let response = self.client.get(&endpoint).headers(headers).send().await?;

        if response.status().is_success() {
            let git_tree = response.json::<GitTree>().await?;
            tracing::trace!("fetch_git_tree returned {} entities", git_tree.tree.len());
            return Ok(git_tree);
        }

        #[allow(clippy::single_match_else)]
        match response.status().as_u16() {
            404 => {
                let err_msg = format!(
                    "The Github API ({endpoint}) failed with status code {}; Please check that org `{owner}`, repo `{repo}` and git tree `{tree_sha}`are correct.",
                    response.status()
                );
                Err(err_msg.into())
            }
            401 => {
                let err_msg = format!(
                    "The Github API ({endpoint}) failed with status code {}; Please check if the token is correct.",
                    response.status()
                );
                Err(err_msg.into())
            }
            403 => {
                let err_msg = format!(
                    "The Github API ({endpoint}) failed with status code {}; Please check if the token has the necessary permissions.",
                    response.status()
                );
                Err(err_msg.into())
            }
            _ => {
                let err_msg = format!(
                    "The Github API ({endpoint}) failed with status code {}",
                    response.status()
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
        let download_url = get_download_url(owner, repo, tree_sha, path);

        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static(SPICE_USER_AGENT));
        if let Ok(header) = HeaderValue::from_str(&format!("token {}", self.token)) {
            headers.insert(AUTHORIZATION, header);
        }

        let response = self
            .client
            .get(&download_url)
            .headers(headers)
            .send()
            .await?;
        if response.status().is_success() {
            let content = response.text().await?;
            Ok(content)
        } else {
            let err_msg = format!("Failed to download file content: {}", response.status());
            Err(err_msg.into())
        }
    }

    async fn search_commits(
        &self,
        owner: &str,
        repo: &str,
        parameters: Option<&str>,
    ) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(octocrab) = &self.octocrab_client {
            let page = octocrab.search_commits(owner, repo, parameters).await?;
            let mut commits = page.items;
            println!("{}", commits.len());

            while let Some(next_page) = octocrab.get_page(&page.next).await? {
                commits.extend(next_page.items);
                println!("{}", commits.len());
            }

            let batch = commits_to_record_batch(commits)?;

            Ok(vec![batch])
        } else {
            Err("Cannot search commits on the REST API - Octocrab client is not initialized".into())
        }
    }
}

pub struct GitHubCommitsTableProvider {
    client: GithubRestClient,
    owner: Arc<str>,
    repo: Arc<str>,
    schema: SchemaRef,
    optimizer: Arc<dyn GraphQLOptimizer>,
}

impl GitHubCommitsTableProvider {
    pub fn new(
        client: GithubRestClient,
        owner: &str,
        repo: &str,
        optimizer: Arc<dyn GraphQLOptimizer>,
    ) -> Result<Self> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("sha", DataType::Utf8, true),
            Field::new("id", DataType::Utf8, true),
            Field::new("author_name", DataType::Utf8, true),
            Field::new("author_email", DataType::Utf8, true),
            Field::new(
                "committed_date",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                true,
            ),
            Field::new("message", DataType::Utf8, true),
            Field::new("message_body", DataType::Utf8, true),
            Field::new("message_head_line", DataType::Utf8, true),
            Field::new("additions", DataType::Int64, true),
            Field::new("deletions", DataType::Int64, true),
        ]));

        // TODO: ensure configuration is correct

        Ok(Self {
            client,
            owner: owner.into(),
            repo: repo.into(),
            schema,
            optimizer,
        })
    }
}

#[async_trait]
impl TableProvider for GitHubCommitsTableProvider {
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
        filters
            .iter()
            .map(|f| self.optimizer.filter_pushdown(f).map(|r| r.filter_pushdown))
            .collect::<Result<Vec<_>, datafusion::error::DataFusionError>>()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let parameters = filters
            .iter()
            .filter_map(|f| {
                self.optimizer
                    .filter_pushdown(f)
                    .map(|r| r.context)
                    .ok()
                    .flatten()
            })
            .collect::<Vec<_>>();

        println!("commits parameters {}", parameters.join(" "));

        let res = self
            .client
            .search_commits(&self.owner, &self.repo, Some(&parameters.join(" ")))
            .await?;

        let table = MemTable::try_new(Arc::clone(&self.schema), vec![res])?;
        table.scan(state, projection, filters, limit).await
    }
}

#[async_trait]
trait CommitSearchExt {
    async fn search_commits(
        &self,
        owner: &str,
        repo: &str,
        parameters: Option<&str>,
    ) -> Result<Page<Commit>, DataFusionError>;
}

#[async_trait]
impl CommitSearchExt for Octocrab {
    async fn search_commits(
        &self,
        owner: &str,
        repo: &str,
        parameters: Option<&str>,
    ) -> Result<Page<Commit>, DataFusionError> {
        let Some(parameters) = parameters else {
            return Err(DataFusionError::Execution("No GitHub commit search parameters provided. A commit message search term is required.".to_string()));
        };

        let query = format!("repo:{owner}/{repo} {parameters}",);

        self.get("/search/commits", Some(&[("q", query)]))
            .await
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))
    }
}

fn commits_to_record_batch(commits: Vec<Commit>) -> Result<RecordBatch> {
    // prepare column builders
    let mut sha_builder = StringBuilder::new();
    let mut id_builder = StringBuilder::new();
    let mut author_name_builder = StringBuilder::new();
    let mut author_email_builder = StringBuilder::new();
    let mut committed_date_builder = TimestampMillisecondBuilder::new();
    let mut message_builder = StringBuilder::new();
    let mut message_body_builder = StringBuilder::new();
    let mut message_head_line_builder = StringBuilder::new();
    let mut additions_builder = Int64Builder::new();
    let mut deletions_builder = Int64Builder::new();

    for commit in commits {
        sha_builder.append_value(commit.sha);
        id_builder.append_value(commit.node_id);

        if let Some(author) = commit.author {
            author_name_builder.append_value(author.login);
            author_email_builder.append_option(author.email);
        } else {
            author_name_builder.append_null();
            author_email_builder.append_null();
        }

        if let Some(committer) = commit.commit.committer {
            // parse the ISO8601 date to get out the i64 millis
            if let Some(Ok(date)) = committer
                .date
                .map(|date| chrono::DateTime::parse_from_rfc3339(&date))
            {
                committed_date_builder.append_value(date.timestamp_millis());
            } else {
                committed_date_builder.append_null();
            }
        } else {
            committed_date_builder.append_null();
        }

        message_builder.append_value(commit.commit.message);
        message_body_builder.append_null();
        message_head_line_builder.append_null();

        if let Some(stats) = commit.stats {
            additions_builder.append_option(stats.additions);
            deletions_builder.append_option(stats.deletions);
        } else {
            additions_builder.append_null();
            deletions_builder.append_null();
        }
    }

    // build columns
    let columns: Vec<ArrayRef> = vec![
        Arc::new(sha_builder.finish()),
        Arc::new(id_builder.finish()),
        Arc::new(author_name_builder.finish()),
        Arc::new(author_email_builder.finish()),
        Arc::new(committed_date_builder.finish()),
        Arc::new(message_builder.finish()),
        Arc::new(message_body_builder.finish()),
        Arc::new(message_head_line_builder.finish()),
        Arc::new(additions_builder.finish()),
        Arc::new(deletions_builder.finish()),
    ];

    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("sha", DataType::Utf8, true),
            Field::new("id", DataType::Utf8, true),
            Field::new("author_name", DataType::Utf8, true),
            Field::new("author_email", DataType::Utf8, true),
            Field::new(
                "committed_date",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                true,
            ),
            Field::new("message", DataType::Utf8, true),
            Field::new("message_body", DataType::Utf8, true),
            Field::new("message_head_line", DataType::Utf8, true),
            Field::new("additions", DataType::Int64, true),
            Field::new("deletions", DataType::Int64, true),
        ])),
        columns,
    )
    .context(UnableToConstructRecordBatchSnafu)
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
