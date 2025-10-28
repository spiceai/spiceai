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
use globset::GlobSet;
use snafu::{ResultExt, Snafu};
use std::{any::Any, path::Path, sync::Arc};

use crate::{arrow::write::MemTable, rate_limit::RateLimiter};
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
use git2::{Oid, Repository, TreeWalkMode, TreeWalkResult};
use std::path::PathBuf;
use tokio::task;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error constructing record batch: {source}"))]
    UnableToConstructRecordBatchError { source: arrow::error::ArrowError },

    #[snafu(display("Git error: {source}"))]
    GitError { source: git2::Error },

    #[snafu(display("IO error: {source}"))]
    IoError { source: std::io::Error },

    #[snafu(display("{message}"))]
    InvalidConfiguration { message: String },

    #[snafu(display("Failed to spawn blocking task: {source}"))]
    SpawnBlockingError { source: tokio::task::JoinError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Represents a file entry in a Git repository with version information
#[derive(Debug, Clone)]
pub struct GitFileEntry {
    pub name: String,
    pub path: String,
    pub size: i64,
    pub sha: String,
    pub mode: String,
    pub tree_sha: String,
    pub commit_sha: Option<String>,
    pub version: Option<String>,
    pub created_at: Option<i64>,
    pub updated_at: Option<i64>,
    pub content: Option<String>,
}

#[derive(Debug)]
pub struct GitTableProvider {
    client: GitClient,
    schema: SchemaRef,
    include: Option<Arc<GlobSet>>,
    fetch_content: bool,
}

impl GitTableProvider {
    pub async fn new(
        repo_url: &str,
        reference: Option<&str>,
        include: Option<Arc<GlobSet>>,
        fetch_content: bool,
        rate_limiter: Arc<dyn RateLimiter>,
        cache_path: Option<PathBuf>,
    ) -> Result<Self> {
        let client = GitClient::new(repo_url, reference, rate_limiter, cache_path)?;

        let mut fields = vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("path", DataType::Utf8, true),
            Field::new("size", DataType::Int64, true),
            Field::new("sha", DataType::Utf8, true),
            Field::new("mode", DataType::Utf8, true),
            Field::new("tree_sha", DataType::Utf8, true),
            Field::new("commit_sha", DataType::Utf8, true),
            Field::new("version", DataType::Utf8, true),
            Field::new(
                "created_at",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                true,
            ),
            Field::new(
                "updated_at",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                true,
            ),
        ];

        if fetch_content {
            fields.push(Field::new("content", DataType::Utf8, true));
        }

        let schema = Arc::new(Schema::new(fields));

        // Validate configuration by fetching a small sample
        client
            .fetch_files(Some(1), None, false, Arc::clone(&schema))
            .await?;

        Ok(Self {
            client,
            schema,
            include,
            fetch_content,
        })
    }
}

#[async_trait]
impl TableProvider for GitTableProvider {
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

#[derive(Debug, Clone)]
pub struct GitClient {
    repo_url: String,
    reference: Option<String>,
    cache_path: PathBuf,
    rate_limiter: Arc<dyn RateLimiter>,
}

impl GitClient {
    pub fn new(
        repo_url: &str,
        reference: Option<&str>,
        rate_limiter: Arc<dyn RateLimiter>,
        cache_path: Option<PathBuf>,
    ) -> Result<Self> {
        let cache_path = cache_path.unwrap_or_else(|| {
            std::env::temp_dir().join("spice_git_cache").join(
                repo_url
                    .replace("https://", "")
                    .replace("git@", "")
                    .replace([':', '/'], "_"),
            )
        });

        Ok(Self {
            repo_url: repo_url.to_string(),
            reference: reference.map(ToString::to_string),
            cache_path,
            rate_limiter,
        })
    }

    /// Clone or open the repository, ensuring it's up to date
    async fn get_repository(&self) -> Result<Repository> {
        let repo_url = self.repo_url.clone();
        let cache_path = self.cache_path.clone();

        task::spawn_blocking(move || {
            if cache_path.exists() {
                tracing::debug!("Opening existing repository at {}", cache_path.display());
                let repo = Repository::open(&cache_path).context(GitSnafu)?;

                // Fetch latest changes
                {
                    let mut remote = repo.find_remote("origin").context(GitSnafu)?;
                    remote
                        .fetch(&["refs/heads/*:refs/remotes/origin/*"], None, None)
                        .context(GitSnafu)?;
                }

                Ok(repo)
            } else {
                tracing::info!(
                    "Cloning repository {} to {}",
                    repo_url,
                    cache_path.display()
                );
                std::fs::create_dir_all(&cache_path).context(IoSnafu)?;
                Repository::clone(&repo_url, &cache_path).context(GitSnafu)
            }
        })
        .await
        .context(SpawnBlockingSnafu)?
    }

    /// Fetch files from the repository
    pub async fn fetch_files(
        &self,
        limit: Option<usize>,
        include: Option<Arc<GlobSet>>,
        fetch_content: bool,
        schema: SchemaRef,
    ) -> Result<Vec<RecordBatch>> {
        self.rate_limiter.check_rate_limit().await.ok();

        let repo = self.get_repository().await?;
        let reference = self.reference.clone();

        let entries = task::spawn_blocking(move || {
            let commit_oid = Self::resolve_reference_blocking(&repo, reference.as_deref())?;
            let commit = repo.find_commit(commit_oid).context(GitSnafu)?;
            let tree = commit.tree().context(GitSnafu)?;
            let tree_sha = tree.id().to_string();
            let commit_sha = commit.id().to_string();
            let version = commit.id().to_string()[..7].to_string();

            let mut entries = Vec::new();
            let mut count = 0;

            tree.walk(TreeWalkMode::PreOrder, |root, entry| {
                // Apply limit if specified
                if let Some(limit) = limit
                    && count >= limit
                {
                    return TreeWalkResult::Abort;
                }

                // Only process blob entries (files)
                if entry.kind() != Some(git2::ObjectType::Blob) {
                    return TreeWalkResult::Ok;
                }

                let entry_name = entry.name().unwrap_or("");
                let full_path = if root.is_empty() {
                    entry_name.to_string()
                } else {
                    format!("{root}{entry_name}")
                };

                // Apply glob filtering
                if let Some(ref glob_set) = include
                    && !glob_set.is_match(&full_path)
                {
                    return TreeWalkResult::Ok;
                }

                let object = match entry.to_object(&repo) {
                    Ok(obj) => obj,
                    Err(e) => {
                        tracing::warn!("Failed to get object for {}: {}", full_path, e);
                        return TreeWalkResult::Ok;
                    }
                };

                let Some(blob) = object.as_blob() else {
                    return TreeWalkResult::Ok;
                };

                let Ok(size) = i64::try_from(blob.size()) else {
                    tracing::warn!(
                        "File {} is too large to represent ({} bytes), skipping",
                        full_path,
                        blob.size()
                    );
                    return TreeWalkResult::Ok;
                };
                let sha = entry.id().to_string();
                let mode = format!("{:o}", entry.filemode());

                let content = if fetch_content {
                    if let Ok(text) = std::str::from_utf8(blob.content()) {
                        Some(text.to_string())
                    } else {
                        tracing::debug!("File {} is not valid UTF-8, skipping content", full_path);
                        None
                    }
                } else {
                    None
                };

                // Get commit history for this file to determine created/updated times
                let (created_at, updated_at) =
                    Self::get_file_timestamps(&repo, &full_path, commit_oid);

                entries.push(GitFileEntry {
                    name: entry_name.to_string(),
                    path: full_path,
                    size,
                    sha,
                    mode,
                    tree_sha: tree_sha.clone(),
                    commit_sha: Some(commit_sha.clone()),
                    version: Some(version.clone()),
                    created_at,
                    updated_at,
                    content,
                });

                count += 1;
                TreeWalkResult::Ok
            })
            .context(GitSnafu)?;

            Ok::<Vec<GitFileEntry>, Error>(entries)
        })
        .await
        .context(SpawnBlockingSnafu)??;

        // Convert entries to RecordBatch
        Self::entries_to_record_batch(&entries, schema)
    }

    /// Blocking version of `resolve_reference` for use in `spawn_blocking`
    fn resolve_reference_blocking(repo: &Repository, reference: Option<&str>) -> Result<Oid> {
        let reference = reference.unwrap_or("HEAD");

        // Try to resolve as a reference (branch or tag)
        if let Ok(reference_obj) = repo.find_reference(reference) {
            return reference_obj
                .peel_to_commit()
                .context(GitSnafu)
                .map(|c| c.id());
        }

        // Try to resolve as a short or full commit SHA
        if let Ok(oid) = Oid::from_str(reference) {
            return Ok(oid);
        }

        // Try with refs/heads/ prefix for branches
        let branch_ref = format!("refs/heads/{reference}");
        if let Ok(reference_obj) = repo.find_reference(&branch_ref) {
            return reference_obj
                .peel_to_commit()
                .context(GitSnafu)
                .map(|c| c.id());
        }

        // Try with refs/tags/ prefix for tags
        let tag_ref = format!("refs/tags/{reference}");
        if let Ok(reference_obj) = repo.find_reference(&tag_ref) {
            return reference_obj
                .peel_to_commit()
                .context(GitSnafu)
                .map(|c| c.id());
        }

        // Try with refs/remotes/origin/ prefix for remote branches
        let remote_ref = format!("refs/remotes/origin/{reference}");
        if let Ok(reference_obj) = repo.find_reference(&remote_ref) {
            return reference_obj
                .peel_to_commit()
                .context(GitSnafu)
                .map(|c| c.id());
        }

        Err(Error::InvalidConfiguration {
            message: format!("Could not resolve reference '{reference}' to a commit"),
        })
    }

    /// Get timestamps for a file by walking its commit history
    fn get_file_timestamps(
        repo: &Repository,
        path: &str,
        start_commit: Oid,
    ) -> (Option<i64>, Option<i64>) {
        let Ok(mut revwalk) = repo.revwalk() else {
            return (None, None);
        };

        if revwalk.push(start_commit).is_err() {
            return (None, None);
        }

        let mut first_commit_time = None;
        let mut last_commit_time = None;

        for oid in revwalk.flatten() {
            let Ok(commit) = repo.find_commit(oid) else {
                continue;
            };

            let Ok(tree) = commit.tree() else {
                continue;
            };

            // Check if this commit contains the file
            if tree.get_path(Path::new(path)).is_ok() {
                let timestamp = commit.time().seconds() * 1000; // Convert to milliseconds

                if last_commit_time.is_none() {
                    last_commit_time = Some(timestamp);
                }
                first_commit_time = Some(timestamp);
            }
        }

        (first_commit_time, last_commit_time)
    }

    /// Convert file entries to Arrow `RecordBatch`
    fn entries_to_record_batch(
        entries: &[GitFileEntry],
        schema: SchemaRef,
    ) -> Result<Vec<RecordBatch>> {
        let mut name_builder = StringBuilder::new();
        let mut path_builder = StringBuilder::new();
        let mut size_builder = Int64Builder::new();
        let mut sha_builder = StringBuilder::new();
        let mut mode_builder = StringBuilder::new();
        let mut tree_sha_builder = StringBuilder::new();
        let mut commit_sha_builder = StringBuilder::new();
        let mut version_builder = StringBuilder::new();
        let mut created_at_builder = TimestampMillisecondBuilder::new();
        let mut updated_at_builder = TimestampMillisecondBuilder::new();
        let mut content_builder = if schema.fields().iter().any(|f| f.name() == "content") {
            Some(StringBuilder::new())
        } else {
            None
        };

        for entry in entries {
            name_builder.append_value(&entry.name);
            path_builder.append_value(&entry.path);
            size_builder.append_value(entry.size);
            sha_builder.append_value(&entry.sha);
            mode_builder.append_value(&entry.mode);
            tree_sha_builder.append_value(&entry.tree_sha);

            if let Some(ref commit_sha) = entry.commit_sha {
                commit_sha_builder.append_value(commit_sha);
            } else {
                commit_sha_builder.append_null();
            }

            if let Some(ref version) = entry.version {
                version_builder.append_value(version);
            } else {
                version_builder.append_null();
            }

            if let Some(created_at) = entry.created_at {
                created_at_builder.append_value(created_at);
            } else {
                created_at_builder.append_null();
            }

            if let Some(updated_at) = entry.updated_at {
                updated_at_builder.append_value(updated_at);
            } else {
                updated_at_builder.append_null();
            }

            if let Some(ref mut builder) = content_builder {
                if let Some(ref content) = entry.content {
                    builder.append_value(content);
                } else {
                    builder.append_null();
                }
            }
        }

        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(name_builder.finish()),
            Arc::new(path_builder.finish()),
            Arc::new(size_builder.finish()),
            Arc::new(sha_builder.finish()),
            Arc::new(mode_builder.finish()),
            Arc::new(tree_sha_builder.finish()),
            Arc::new(commit_sha_builder.finish()),
            Arc::new(version_builder.finish()),
            Arc::new(created_at_builder.finish()),
            Arc::new(updated_at_builder.finish()),
        ];

        if let Some(mut builder) = content_builder {
            columns.push(Arc::new(builder.finish()));
        }

        let batch =
            RecordBatch::try_new(schema, columns).context(UnableToConstructRecordBatchSnafu)?;

        Ok(vec![batch])
    }
}
