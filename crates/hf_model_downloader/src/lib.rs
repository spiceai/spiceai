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

//! `HuggingFace` Model Downloader
//!
//! This crate provides reusable functionality for downloading models from `HuggingFace` Hub.
//! It handles authentication, caching, and supports various repository types (models, datasets, spaces).

use hf_hub::{
    Repo, RepoType,
    api::{
        RepoInfo,
        tokio::{Api, ApiBuilder, ApiRepo},
    },
};
use secrecy::{ExposeSecret, SecretString};
use snafu::prelude::*;
use std::path::PathBuf;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Failed to build HuggingFace API: {source}"))]
    FailedToBuildApi {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to download file '{file}' from HuggingFace: {source}"))]
    FailedToDownloadFile {
        file: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to get repository info: {source}"))]
    FailedToGetRepoInfo {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// Configuration for downloading models from `HuggingFace`
#[derive(Debug, Clone)]
pub struct DownloadConfig {
    /// `HuggingFace` repository ID (e.g., "meta-llama/Llama-3.3-70B-Instruct")
    pub repo_id: String,

    /// Optional revision/branch (e.g., "main", commit SHA)
    pub revision: Option<String>,

    /// Type of repository (Model, Dataset, Space)
    pub repo_type: RepoType,

    /// Optional `HuggingFace` API token for private repos
    pub token: Option<SecretString>,

    /// Whether to show progress during download (default: true)
    pub show_progress: bool,

    /// Optional custom cache directory
    pub cache_dir: Option<PathBuf>,
}

impl DownloadConfig {
    /// Create a new download configuration for a model repository
    ///
    /// By default, progress is shown during downloads.
    pub fn new(repo_id: impl Into<String>) -> Self {
        Self {
            repo_id: repo_id.into(),
            revision: None,
            repo_type: RepoType::Model,
            token: None,
            show_progress: true,
            cache_dir: None,
        }
    }

    /// Set the revision/branch
    #[must_use]
    pub fn with_revision(mut self, revision: impl Into<String>) -> Self {
        self.revision = Some(revision.into());
        self
    }

    /// Set the repository type
    #[must_use]
    pub fn with_repo_type(mut self, repo_type: RepoType) -> Self {
        self.repo_type = repo_type;
        self
    }

    /// Set the `HuggingFace` API token
    #[must_use]
    pub fn with_token(mut self, token: SecretString) -> Self {
        self.token = Some(token);
        self
    }

    /// Enable progress display during download
    #[must_use]
    pub fn with_progress(mut self, show_progress: bool) -> Self {
        self.show_progress = show_progress;
        self
    }

    /// Set custom cache directory
    #[must_use]
    pub fn with_cache_dir(mut self, cache_dir: PathBuf) -> Self {
        self.cache_dir = Some(cache_dir);
        self
    }
}

/// `HuggingFace` model downloader
pub struct HfDownloader {
    api: Api,
    config: DownloadConfig,
}

impl HfDownloader {
    /// Create a new downloader with the given configuration
    ///
    /// # Errors
    ///
    /// Returns an error if the `HuggingFace` API cannot be initialized.
    pub fn new(config: DownloadConfig) -> Result<Self> {
        let mut builder = ApiBuilder::new()
            .with_progress(config.show_progress)
            .with_token(config.token.as_ref().map(|t| t.expose_secret().to_string()));

        // Use custom cache directory if provided
        if let Some(cache_dir) = &config.cache_dir {
            if cache_dir.exists() {
                tracing::debug!("Using custom HF cache directory: {:?}", cache_dir);
                builder = builder.with_cache_dir(cache_dir.clone());
            } else {
                tracing::warn!(
                    "Custom HF cache directory {:?} does not exist, using default",
                    cache_dir
                );
            }
        } else if let Ok(cache_dir) = std::env::var("HF_HUB_CACHE") {
            // Use environment variable if set
            let cache_path: PathBuf = cache_dir.into();
            if cache_path.exists() {
                tracing::debug!("Using HF_HUB_CACHE directory: {:?}", cache_path);
                builder = builder.with_cache_dir(cache_path);
            } else {
                tracing::debug!(
                    "HF_HUB_CACHE directory {:?} does not exist, ignoring.",
                    cache_path
                );
            }
        }

        let api = builder
            .build()
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            .context(FailedToBuildApiSnafu)?;

        Ok(Self { api, config })
    }

    /// Get the API repository handle
    fn get_api_repo(&self) -> ApiRepo {
        let repo = if let Some(revision) = &self.config.revision {
            Repo::with_revision(
                self.config.repo_id.clone(),
                self.config.repo_type,
                revision.clone(),
            )
        } else {
            Repo::new(self.config.repo_id.clone(), self.config.repo_type)
        };

        self.api.repo(repo)
    }

    /// Download a specific file from the repository
    ///
    /// Returns the local path to the downloaded file in the cache
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be downloaded from `HuggingFace`.
    pub async fn download_file(&self, filename: &str) -> Result<PathBuf> {
        let api_repo = self.get_api_repo();

        tracing::debug!(
            "Downloading file '{}' from repository '{}'",
            filename,
            self.config.repo_id
        );

        api_repo
            .get(filename)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            .context(FailedToDownloadFileSnafu {
                file: filename.to_string(),
            })
    }

    /// Download multiple files from the repository
    ///
    /// Returns a vector of local paths to the downloaded files in the cache
    ///
    /// # Errors
    ///
    /// Returns an error if any file cannot be downloaded from `HuggingFace`.
    pub async fn download_files(&self, filenames: &[&str]) -> Result<Vec<PathBuf>> {
        let mut paths = Vec::with_capacity(filenames.len());

        for filename in filenames {
            let path = self.download_file(filename).await?;
            paths.push(path);
        }

        Ok(paths)
    }

    /// Get repository information
    ///
    /// Returns metadata about the repository including available files
    ///
    /// # Errors
    ///
    /// Returns an error if repository information cannot be fetched from `HuggingFace`.
    pub async fn get_repo_info(&self) -> Result<RepoInfo> {
        let api_repo = self.get_api_repo();

        api_repo
            .info()
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            .context(FailedToGetRepoInfoSnafu)
    }

    /// Download all files from the repository
    ///
    /// Returns the root directory containing all downloaded files
    /// Note: This downloads ALL files, which can be large. Use with caution.
    ///
    /// # Errors
    ///
    /// Returns an error if files cannot be downloaded or if the repository is empty.
    pub async fn download_all(&self) -> Result<PathBuf> {
        let repo_info = self.get_repo_info().await?;

        tracing::info!(
            "Downloading {} files from repository '{}'",
            repo_info.siblings.len(),
            self.config.repo_id
        );

        let mut first_file_dir = None;

        for sibling in repo_info.siblings {
            let path = self.download_file(&sibling.rfilename).await?;

            if first_file_dir.is_none() {
                // Get the parent directory (repo cache dir)
                if let Some(parent) = path.parent() {
                    first_file_dir = Some(parent.to_path_buf());
                }
            }
        }

        first_file_dir.ok_or_else(|| Error::FailedToGetRepoInfo {
            source: "No files found in repository".into(),
        })
    }

    /// Get the repository ID
    #[must_use]
    pub fn repo_id(&self) -> &str {
        &self.config.repo_id
    }

    /// Get the revision (if set)
    #[must_use]
    pub fn revision(&self) -> Option<&str> {
        self.config.revision.as_deref()
    }

    /// Find all GGUF files in the repository
    ///
    /// Returns a list of GGUF filenames found in the repository
    ///
    /// # Errors
    ///
    /// Returns an error if repository information cannot be fetched.
    pub async fn find_gguf_files(&self) -> Result<Vec<String>> {
        let repo_info = self.get_repo_info().await?;

        let gguf_files: Vec<String> = repo_info
            .siblings
            .iter()
            .filter(|sibling| sibling.rfilename.to_lowercase().ends_with(".gguf"))
            .map(|sibling| sibling.rfilename.clone())
            .collect();

        Ok(gguf_files)
    }

    /// Automatically find and download the best GGUF file from the repository
    ///
    /// This method will:
    /// 1. List all GGUF files in the repository
    /// 2. Select the "best" one (prefers `Q4_K_M` quantization if available)
    /// 3. Download and return the path to the file
    ///
    /// # Errors
    ///
    /// Returns an error if no GGUF files are found or if download fails.
    pub async fn download_best_gguf(&self) -> Result<PathBuf> {
        let gguf_files = self.find_gguf_files().await?;

        if gguf_files.is_empty() {
            return Err(Error::FailedToGetRepoInfo {
                source: format!(
                    "No GGUF files found in repository '{}'",
                    self.config.repo_id
                )
                .into(),
            });
        }

        // Select the best GGUF file
        let selected_file = Self::select_best_gguf(&gguf_files);

        tracing::info!(
            "Auto-selected GGUF file '{}' from {} available files in repository '{}'",
            selected_file,
            gguf_files.len(),
            self.config.repo_id
        );

        self.download_file(&selected_file).await
    }

    /// Select the best GGUF file from a list
    ///
    /// Preference order:
    /// 1. `Q4_K_M` quantization (good balance of quality and size)
    /// 2. `Q4_K_S` quantization
    /// 3. `Q5_K_M` quantization
    /// 4. Other Q4 variants
    /// 5. Smallest file (by name length as heuristic)
    fn select_best_gguf(files: &[String]) -> String {
        // Preference order for quantization types
        let preferences = [
            "q4_k_m", "q4_0_k_m", "q4_k_s", "q5_k_m", "q4_0", "q4_1", "q5_0", "q5_1",
        ];

        // Try to find preferred quantizations
        for pref in &preferences {
            if let Some(file) = files.iter().find(|f| f.to_lowercase().contains(pref)) {
                return file.clone();
            }
        }

        // Fallback: return the shortest filename (likely the smallest/simplest model)
        files
            .iter()
            .min_by_key(|f| f.len())
            .map_or_else(|| files[0].clone(), Clone::clone)
    }
}

/// Helper function to download a single file from `HuggingFace`
///
/// This is a convenience wrapper around `HfDownloader` for simple use cases
///
/// # Errors
///
/// Returns an error if the downloader cannot be created or the file cannot be downloaded.
pub async fn download_file(
    repo_id: &str,
    filename: &str,
    revision: Option<&str>,
    token: Option<&str>,
) -> Result<PathBuf> {
    let mut config = DownloadConfig::new(repo_id);

    if let Some(rev) = revision {
        config = config.with_revision(rev);
    }

    if let Some(tok) = token {
        config = config.with_token(SecretString::from(tok.to_string()));
    }

    let downloader = HfDownloader::new(config)?;
    downloader.download_file(filename).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_download_config_builder() {
        let config = DownloadConfig::new("test/model").with_revision("main");

        assert_eq!(config.repo_id, "test/model");
        assert_eq!(config.revision, Some("main".to_string()));
        assert!(config.show_progress); // Progress is shown by default

        // Test disabling progress
        let config_no_progress = DownloadConfig::new("test/model").with_progress(false);
        assert!(!config_no_progress.show_progress);
    }

    #[tokio::test]
    #[ignore] // Requires network access
    async fn test_download_file() {
        // Test downloading a small config file from a public model
        let result = download_file(
            "hf-internal-testing/tiny-random-bert",
            "config.json",
            None,
            None,
        )
        .await;

        assert!(result.is_ok());
        let path = result.expect("Path should exist");
        assert!(path.exists());
    }
}
