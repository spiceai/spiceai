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
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_mins(2))
            .user_agent(format!(
                "spice/{} ({}; {})",
                env!("CARGO_PKG_VERSION"),
                std::env::consts::OS,
                std::env::consts::ARCH
            ))
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

    /// Download a release asset with progress tracking, verifying the body is the whole asset.
    ///
    /// The releases API publishes an exact `size` for every asset, so a body of any other
    /// length is a failed download — a dropped connection, a truncating proxy, or an
    /// interstitial page served with a `200`. Verifying it here means the bytes only ever
    /// reach a decoder once they are known to be the right ones; otherwise a short body
    /// surfaces as whatever the decoder makes of it, which reads as a corrupt archive
    /// rather than as the transfer failure it is.
    ///
    /// The asset is taken whole rather than as a URL and a length so that the size checked
    /// against is always the one published for the body being fetched. `Content-Length` is
    /// deliberately not the authority: it describes whatever the server chose to send, so a
    /// proxy substituting its own complete response satisfies it. Any `Content-Encoding` is
    /// a transport wrapper the client unwraps before this sees the body, leaving the stored
    /// bytes the published size refers to.
    pub async fn download_asset<F>(
        &self,
        asset: &ReleaseAsset,
        mut on_progress: F,
    ) -> Result<Vec<u8>, GitHubError>
    where
        F: FnMut(u64),
    {
        use futures::StreamExt;

        let url = &asset.browser_download_url;
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

        let mut downloaded: u64 = 0;
        let mut data = Vec::new();
        let mut stream = response.bytes_stream();

        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| GitHubError::Request {
                message: e.to_string(),
            })?;
            downloaded += chunk.len() as u64;
            data.extend_from_slice(&chunk);
            on_progress(downloaded);
        }

        if downloaded != asset.size {
            return Err(GitHubError::IncompleteDownload {
                name: asset.name.clone(),
                expected: asset.size,
                received: downloaded,
            });
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
    Request {
        message: String,
    },
    Unauthorized,
    Api {
        status: u16,
        message: String,
    },
    Parse {
        message: String,
    },
    AssetNotFound {
        name: String,
    },
    ReleaseNotFound {
        version: String,
    },
    IncompleteDownload {
        name: String,
        expected: u64,
        received: u64,
    },
    Io {
        message: String,
    },
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
            // Exact byte counts, not human-readable sizes: rounding would print an expected
            // and a received figure that read as identical for a small truncation.
            Self::IncompleteDownload {
                name,
                expected,
                received,
            } => write!(
                f,
                "Download of {name} did not complete: expected {expected} bytes but received {received}. Check the network connection and any proxy between this machine and GitHub, then run the command again."
            ),
            Self::Io { message } => write!(f, "IO error: {message}"),
        }
    }
}

impl std::error::Error for GitHubError {}
