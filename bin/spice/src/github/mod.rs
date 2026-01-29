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

//! GitHub API client for downloading releases.

mod release;

pub use release::{
    Arch, ReleaseAsset, RepoRelease, SystemType, download_release_asset,
    download_release_asset_with_fallback, get_latest_release, get_release, upgrade_cli_in_place,
};

use reqwest::Client;
use serde::de::DeserializeOwned;
use std::time::Duration;

const GITHUB_API_BASE: &str = "https://api.github.com";
const RUNTIME_OWNER: &str = "spiceai";
const RUNTIME_REPO: &str = "spiceai";

/// GitHub API client.
#[derive(Clone)]
pub struct GitHubClient {
    client: Client,
    token: Option<String>,
    pub owner: String,
    pub repo: String,
}

impl GitHubClient {
    /// Create a new GitHub client for the spiceai/spiceai repository.
    #[must_use]
    pub fn new_runtime_client() -> Self {
        Self::new(RUNTIME_OWNER, RUNTIME_REPO)
    }

    /// Create a new GitHub client for a specific repository.
    #[must_use]
    pub fn new(owner: &str, repo: &str) -> Self {
        // Check for GitHub token in environment
        let token = std::env::var("GH_TOKEN")
            .or_else(|_| std::env::var("GITHUB_TOKEN"))
            .ok();

        let client = Client::builder()
            .timeout(Duration::from_secs(120))
            .user_agent("spice")
            .build()
            .unwrap_or_default();

        Self {
            client,
            token,
            owner: owner.to_string(),
            repo: repo.to_string(),
        }
    }

    /// Make a GET request to the GitHub API.
    pub async fn get<T: DeserializeOwned>(&self, url: &str) -> Result<T, GitHubError> {
        let mut request = self
            .client
            .get(url)
            .header("Accept", "application/vnd.github.v3+json");

        if let Some(token) = &self.token {
            request = request.header("Authorization", format!("Bearer {token}"));
        }

        let response = request.send().await.map_err(|e| GitHubError::Request {
            message: e.to_string(),
        })?;

        let status = response.status();

        if status == reqwest::StatusCode::UNAUTHORIZED {
            return Err(GitHubError::Unauthorized);
        }

        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(GitHubError::Api {
                status: status.as_u16(),
                message: body,
            });
        }

        response.json().await.map_err(|e| GitHubError::Parse {
            message: e.to_string(),
        })
    }

    /// Download a file with progress tracking.
    pub async fn download_with_progress<F>(
        &self,
        url: &str,
        mut on_progress: F,
    ) -> Result<Vec<u8>, GitHubError>
    where
        F: FnMut(u64, Option<u64>),
    {
        use futures::StreamExt;

        let mut request = self
            .client
            .get(url)
            .header("Accept", "application/octet-stream");

        if let Some(token) = &self.token {
            // Only add auth for GitHub domains
            if url.contains("github.com") || url.contains("githubusercontent.com") {
                request = request.header("Authorization", format!("Bearer {token}"));
            }
        }

        let response = request.send().await.map_err(|e| GitHubError::Request {
            message: e.to_string(),
        })?;

        if !response.status().is_success() {
            return Err(GitHubError::Api {
                status: response.status().as_u16(),
                message: "Failed to download asset".to_string(),
            });
        }

        let total_size = response.content_length();
        let mut downloaded: u64 = 0;
        let mut data = Vec::new();
        let mut stream = response.bytes_stream();

        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| GitHubError::Request {
                message: e.to_string(),
            })?;
            downloaded += chunk.len() as u64;
            data.extend_from_slice(&chunk);
            on_progress(downloaded, total_size);
        }

        Ok(data)
    }

    /// Get the releases API URL.
    #[must_use]
    pub fn releases_url(&self) -> String {
        format!(
            "{GITHUB_API_BASE}/repos/{}/{}/releases",
            self.owner, self.repo
        )
    }

    /// Get the latest release API URL.
    #[must_use]
    pub fn latest_release_url(&self) -> String {
        format!("{}/latest", self.releases_url())
    }
}

/// Errors that can occur when interacting with GitHub.
#[derive(Debug)]
pub enum GitHubError {
    Request { message: String },
    Unauthorized,
    Api { status: u16, message: String },
    Parse { message: String },
    AssetNotFound { name: String },
    ReleaseNotFound { version: String },
    Io { message: String },
}

impl std::fmt::Display for GitHubError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Request { message } => write!(f, "HTTP request failed: {message}"),
            Self::Unauthorized => write!(
                f,
                "GitHub token from GH_TOKEN or GITHUB_TOKEN is invalid. Check the token and try again."
            ),
            Self::Api { status, message } => {
                write!(f, "GitHub API error (status {status}): {message}")
            }
            Self::Parse { message } => write!(f, "Failed to parse response: {message}"),
            Self::AssetNotFound { name } => write!(f, "Asset not found: {name}"),
            Self::ReleaseNotFound { version } => write!(f, "Release not found: {version}"),
            Self::Io { message } => write!(f, "IO error: {message}"),
        }
    }
}

impl std::error::Error for GitHubError {}
