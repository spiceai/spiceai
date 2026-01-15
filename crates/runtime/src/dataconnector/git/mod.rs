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

use crate::component::dataset::Dataset;
use crate::dataconnector::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    DataConnectorResult, ParameterSpec, Parameters,
};
use crate::register_data_connector;
use async_trait::async_trait;
use data_components::git::{
    DEFAULT_MAX_FILE_BYTES, DEFAULT_MAX_FILES, GitTableConfig, GitTableProvider,
};
use data_components::rate_limit::RateLimiter;
use datafusion::datasource::TableProvider;
use globset::{Glob, GlobSet, GlobSetBuilder};
use std::path::PathBuf;
use std::{any::Any, future::Future, pin::Pin, sync::Arc};

#[derive(Debug)]
pub struct Git {
    params: Parameters,
}

impl Git {
    #[must_use]
    pub fn new(params: Parameters) -> Self {
        Self { params }
    }

    /// Parse the Git URL from the dataset path
    /// Supports formats like:
    /// - git:https://github.com/spiceai/spiceai.git
    /// - git:git@github.com:spiceai/spiceai.git
    fn parse_git_url(path: &str) -> Result<(String, Option<String>), String> {
        let path = path.strip_prefix("git:").unwrap_or(path).trim();
        if path.is_empty() {
            return Err("Git path is empty".to_string());
        }

        // Check for reference specification (e.g., @branch or @tag or @commit)
        if let Some(at_pos) = path.rfind('@') {
            // Check if this @ is part of git@github.com (SSH format)
            // In SSH format, @ appears before the colon
            if let Some(colon_pos) = path.find(':')
                && at_pos < colon_pos
            {
                // This is SSH format like git@github.com:org/repo
                // Check for a second @ for reference
                if let Some(ref_pos) = path[at_pos + 1..].rfind('@') {
                    let actual_ref_pos = at_pos + 1 + ref_pos;
                    let url = path[..actual_ref_pos].to_string();
                    let reference = path[actual_ref_pos + 1..].to_string();
                    return Ok((url, Some(reference)));
                }
                return Ok((path.to_string(), None));
            }

            // Otherwise, @ is a reference separator
            let url = path[..at_pos].to_string();
            let reference = path[at_pos + 1..].to_string();
            Ok((url, Some(reference)))
        } else {
            Ok((path.to_string(), None))
        }
    }

    /// Create a `GitTableProvider` from dataset configuration
    async fn create_table_provider(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        let path = dataset.path();
        let component = ConnectorComponent::from(dataset);

        let (repo_url, reference) =
            Self::parse_git_url(path).map_err(|e| DataConnectorError::UnableToGetReadProvider {
                dataconnector: "git".to_string(),
                connector_component: component.clone(),
                source: format!("Invalid Git URL: {e}").into(),
            })?;

        tracing::debug!(
            "Connecting to Git repository: {} (reference: {:?})",
            repo_url,
            reference
        );

        // Parse include patterns if provided
        let include_patterns = dataset.params.get("git_include").cloned().or_else(|| {
            self.params
                .get("include")
                .expose()
                .ok()
                .map(ToString::to_string)
        });
        let include = include_patterns
            .map(|patterns| {
                parse_globs(&component, &patterns).map_err(|e| {
                    DataConnectorError::UnableToGetReadProvider {
                        dataconnector: "git".to_string(),
                        connector_component: component.clone(),
                        source: format!("Failed to parse include patterns: {e}").into(),
                    }
                })
            })
            .transpose()?;

        // Check if content fetching is enabled
        let fetch_content = dataset
            .params
            .get("git_fetch_content")
            .and_then(|v| v.parse::<bool>().ok())
            .or_else(|| {
                self.params
                    .get("fetch_content")
                    .expose()
                    .ok()
                    .and_then(|v| v.parse::<bool>().ok())
            })
            .unwrap_or(false);

        // Get cache path if specified
        let cache_path = dataset
            .params
            .get("git_cache_path")
            .cloned()
            .or_else(|| {
                self.params
                    .get("cache_path")
                    .expose()
                    .ok()
                    .map(ToString::to_string)
            })
            .map(PathBuf::from);

        let max_files = dataset
            .params
            .get("git_max_files")
            .and_then(|v| v.parse::<usize>().ok())
            .or_else(|| {
                self.params
                    .get("max_files")
                    .expose()
                    .ok()
                    .and_then(|v| v.parse::<usize>().ok())
            })
            .unwrap_or(DEFAULT_MAX_FILES);

        let max_file_bytes = dataset
            .params
            .get("git_max_file_bytes")
            .and_then(|v| v.parse::<usize>().ok())
            .or_else(|| {
                self.params
                    .get("max_file_bytes")
                    .expose()
                    .ok()
                    .and_then(|v| v.parse::<usize>().ok())
            })
            .unwrap_or(DEFAULT_MAX_FILE_BYTES);

        // Create a no-op rate limiter (Git operations are local after initial clone)
        let rate_limiter: Arc<dyn RateLimiter> = Arc::new(NoOpRateLimiter);

        let config = GitTableConfig {
            fetch_content,
            rate_limiter,
            cache_path,
            max_files,
            max_file_bytes,
        };

        let table_provider =
            GitTableProvider::new(&repo_url, reference.as_deref(), include, config)
                .await
                .map_err(|e| DataConnectorError::UnableToGetReadProvider {
                    dataconnector: "git".to_string(),
                    connector_component: component,
                    source: Box::new(e),
                })?;

        Ok(Arc::new(table_provider))
    }
}

#[async_trait]
impl DataConnector for Git {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        self.create_table_provider(dataset).await
    }
}

#[derive(Default, Debug, Copy, Clone)]
pub struct GitFactory {}

impl GitFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::runtime("include")
        .description("Include only files matching the glob pattern. Multiple patterns can be separated by comma or semicolon.")
        .examples(&["*.rs", "**/*.yaml;src/**/*.json"]),
    ParameterSpec::runtime("fetch_content")
        .description("Whether to fetch file content. Set to 'true' to include file content in the 'content' column.")
        .default("false"),
    ParameterSpec::runtime("cache_path")
        .description("Custom path for the local Git repository cache. If not specified, uses system temp directory."),
    ParameterSpec::runtime("max_files")
        .description("Maximum number of files to materialize from a Git repository. Default: 5000. Hard limit: 50000.")
        .default("5000"),
    ParameterSpec::runtime("max_file_bytes")
        .description("Maximum size (bytes) for an individual file when fetching content. Files larger than this value are skipped. Default: 524288. Maximum: 5242880 (5 MiB)."),
];

impl DataConnectorFactory for GitFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move { Ok(Arc::new(Git::new(params.parameters)) as Arc<dyn DataConnector>) })
    }

    fn prefix(&self) -> &'static str {
        "git"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

/// Parse glob patterns from a comma or semicolon separated string
pub fn parse_globs(
    component: &ConnectorComponent,
    input: &str,
) -> Result<Arc<GlobSet>, Box<dyn std::error::Error + Send + Sync>> {
    let patterns: Vec<&str> = input.split(&[',', ';'][..]).collect();
    let mut builder = GlobSetBuilder::new();

    for pattern in patterns {
        let trimmed_pattern = pattern.trim();
        if !trimmed_pattern.is_empty() {
            builder.add(Glob::new(trimmed_pattern).map_err(|e| {
                format!("Invalid glob pattern '{trimmed_pattern}' for {component}: {e}")
            })?);
        }
    }

    let glob_set = builder
        .build()
        .map_err(|e| format!("Failed to build glob set for {component}: {e}"))?;

    Ok(Arc::new(glob_set))
}

/// A no-op rate limiter for Git operations (local operations after clone)
#[derive(Debug)]
struct NoOpRateLimiter;

#[async_trait]
impl RateLimiter for NoOpRateLimiter {
    async fn update_from_headers(&self, _headers: &reqwest::header::HeaderMap) {
        // No rate limiting needed for local Git operations
    }

    async fn check_rate_limit(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}

register_data_connector!("git", GitFactory);
