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

//! GitHub release types and operations.

use super::{GitHubClient, GitHubError};
use flate2::read::GzDecoder;
use serde::Deserialize;
use std::path::Path;
use tar::Archive;

/// A GitHub release.
#[derive(Debug, Deserialize)]
pub struct RepoRelease {
    pub url: String,
    pub html_url: String,
    pub assets_url: String,
    pub tag_name: String,
    pub name: Option<String>,
    pub draft: bool,
    pub prerelease: bool,
    pub created_at: String,
    pub published_at: Option<String>,
    pub assets: Vec<ReleaseAsset>,
}

impl RepoRelease {
    /// Check if the release has a specific asset.
    #[must_use]
    pub fn has_asset(&self, name: &str) -> bool {
        self.assets.iter().any(|a| a.name == name)
    }

    /// Get a specific asset by name.
    #[must_use]
    pub fn get_asset(&self, name: &str) -> Option<&ReleaseAsset> {
        self.assets.iter().find(|a| a.name == name)
    }
}

/// A GitHub release asset.
#[derive(Debug, Deserialize)]
pub struct ReleaseAsset {
    pub url: String,
    pub id: u64,
    pub name: String,
    pub content_type: String,
    pub size: u64,
    pub download_count: u64,
    pub browser_download_url: String,
}

/// Get the latest release from GitHub.
pub async fn get_latest_release(client: &GitHubClient) -> Result<RepoRelease, GitHubError> {
    let url = client.latest_release_url();
    client.get(&url).await
}

/// Get a specific release by version tag.
pub async fn get_release(client: &GitHubClient, version: &str) -> Result<RepoRelease, GitHubError> {
    let url = client.releases_url();
    let releases: Vec<RepoRelease> = client.get(&url).await?;

    releases
        .into_iter()
        .find(|r| r.tag_name == version)
        .ok_or(GitHubError::ReleaseNotFound {
            version: version.to_string(),
        })
}

/// Download a release asset and extract it to a directory.
pub async fn download_release_asset(
    client: &GitHubClient,
    release: &RepoRelease,
    asset_name: &str,
    download_dir: &Path,
) -> Result<(), GitHubError> {
    let asset = release
        .get_asset(asset_name)
        .ok_or_else(|| GitHubError::AssetNotFound {
            name: asset_name.to_string(),
        })?;

    tracing::debug!(
        "Downloading asset: {} ({})",
        asset.name,
        format_size(asset.size)
    );

    // Download with progress
    let total_size = asset.size;
    let start_time = std::time::Instant::now();

    let data = client
        .download_with_progress(&asset.browser_download_url, |downloaded, _| {
            let elapsed = start_time.elapsed().as_secs_f64();
            let speed = if elapsed > 0.0 {
                downloaded as f64 / elapsed / 1024.0 / 1024.0
            } else {
                0.0
            };
            let percent = (downloaded as f64 / total_size as f64) * 100.0;

            eprint!(
                "\rDownloading: {:.1}% ({}/{}) @ {:.1} MB/s",
                percent,
                format_size(downloaded),
                format_size(total_size),
                speed
            );
        })
        .await?;

    eprintln!(); // New line after progress

    // Extract tar.gz
    extract_tar_gz(&data, download_dir)?;

    Ok(())
}

/// Extract a tar.gz archive to a directory.
fn extract_tar_gz(data: &[u8], dest: &Path) -> Result<(), GitHubError> {
    let decoder = GzDecoder::new(data);
    let mut archive = Archive::new(decoder);

    archive.unpack(dest).map_err(|e| GitHubError::Io {
        message: format!("Failed to extract archive: {e}"),
    })?;

    Ok(())
}

/// Format a byte size as a human-readable string.
fn format_size(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

/// Get the runtime asset name for the current platform.
#[must_use]
pub fn get_runtime_asset_name(flavor: &str, allow_accelerator: bool) -> String {
    let os = std::env::consts::OS;
    let arch = get_rust_arch();

    let flavor_suffix = match flavor {
        "ai" | "default" => {
            if allow_accelerator {
                if let Some(accelerator) = detect_accelerator() {
                    format!("_models_{accelerator}")
                } else {
                    "_models".to_string()
                }
            } else {
                "_models".to_string()
            }
        }
        _ => String::new(),
    };

    format!("spiced{flavor_suffix}_{os}_{arch}.tar.gz")
}

/// Get the CLI asset name for the current platform.
#[must_use]
pub fn get_cli_asset_name() -> String {
    let os = std::env::consts::OS;
    let arch = get_rust_arch();
    format!("spice_{os}_{arch}.tar.gz")
}

/// Map Go arch names to Rust target names.
fn get_rust_arch() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "x86_64",
        "aarch64" => "aarch64",
        other => other,
    }
}

/// Detect hardware accelerator (Metal on macOS, CUDA on Linux).
fn detect_accelerator() -> Option<String> {
    #[cfg(target_os = "macos")]
    {
        if has_metal_device() {
            return Some("metal".to_string());
        }
    }

    #[cfg(target_os = "linux")]
    {
        if let Some(cuda_version) = get_cuda_version() {
            // Supported CUDA compute capabilities
            let supported = ["80", "86", "87", "89", "90"];
            if supported.contains(&cuda_version.as_str()) {
                return Some(format!("cuda_{cuda_version}"));
            }
            tracing::warn!(
                "Detected GPU with compute capability {cuda_version}, but this version is not supported for model acceleration. Falling back to CPU."
            );
        }
    }

    None
}

/// Check if the system has a Metal-capable GPU (macOS only).
#[cfg(target_os = "macos")]
fn has_metal_device() -> bool {
    use std::process::Command;

    tracing::debug!("Checking for Metal support via system_profiler");

    match Command::new("system_profiler")
        .args(["SPDisplaysDataType", "-detailLevel", "mini"])
        .output()
    {
        Ok(output) => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            stdout.contains("Metal Support: Metal")
        }
        Err(_) => false,
    }
}

/// Get CUDA compute capability (Linux only).
#[cfg(target_os = "linux")]
fn get_cuda_version() -> Option<String> {
    use std::process::Command;

    tracing::debug!("Checking for CUDA via nvidia-smi");

    let output = Command::new("nvidia-smi")
        .args(["--query-gpu=compute_cap", "--format=csv,noheader"])
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let version = String::from_utf8_lossy(&output.stdout)
        .trim()
        .replace('.', "");

    if version.is_empty() {
        None
    } else {
        Some(version)
    }
}
