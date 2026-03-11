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

/// Download a release asset with fallback to alternative asset names.
///
/// Tries each asset name in order until one succeeds.
/// Returns the name of the asset that was successfully downloaded.
pub async fn download_release_asset_with_fallback(
    client: &GitHubClient,
    release: &RepoRelease,
    asset_names: &[String],
    download_dir: &Path,
) -> Result<String, GitHubError> {
    for asset_name in asset_names {
        if release.has_asset(asset_name) {
            download_release_asset(client, release, asset_name, download_dir).await?;
            return Ok(asset_name.clone());
        }
        tracing::debug!("Asset not found: {asset_name}, trying next...");
    }

    // None of the asset names were found
    let tried = asset_names.join(", ");
    Err(GitHubError::AssetNotFound { name: tried })
}

/// Upgrade the CLI binary in-place by downloading and replacing the current executable.
///
/// This function:
/// 1. Downloads the CLI asset from the release
/// 2. Extracts the binary to a temporary file
/// 3. Atomically replaces the current executable with the new one
///
/// # Errors
///
/// Returns an error if the download fails, extraction fails, or the file replacement fails.
pub async fn upgrade_cli_in_place(
    client: &GitHubClient,
    release: &RepoRelease,
    asset_name: &str,
) -> Result<(), GitHubError> {
    let asset = release
        .get_asset(asset_name)
        .ok_or_else(|| GitHubError::AssetNotFound {
            name: asset_name.to_string(),
        })?;

    tracing::debug!(
        "Downloading CLI asset: {} ({})",
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
                "\rDownloading CLI: {:.1}% ({}/{}) @ {:.1} MB/s",
                percent,
                format_size(downloaded),
                format_size(total_size),
                speed
            );
        })
        .await?;

    eprintln!(); // New line after progress

    // Get the current executable path
    let current_exe = std::env::current_exe().map_err(|e| GitHubError::Io {
        message: format!("Failed to get current executable path: {e}"),
    })?;

    // Create a temporary directory for extraction
    let temp_dir = tempfile::tempdir().map_err(|e| GitHubError::Io {
        message: format!("Failed to create temporary directory: {e}"),
    })?;

    // Extract tar.gz to temp directory
    extract_tar_gz(&data, temp_dir.path())?;

    // Find the extracted CLI binary
    let cli_binary_name = if cfg!(windows) { "spice.exe" } else { "spice" };
    let extracted_binary = temp_dir.path().join(cli_binary_name);

    if !extracted_binary.exists() {
        return Err(GitHubError::Io {
            message: format!(
                "Extracted CLI binary not found at {}",
                extracted_binary.display()
            ),
        });
    }

    // On Unix, we can replace the binary directly even while running
    // On Windows, we need to rename the old binary first
    #[cfg(windows)]
    {
        let backup_path = current_exe.with_extension("old.exe");
        // Try to remove old backup if it exists
        let _ = std::fs::remove_file(&backup_path);
        // Rename current executable to backup
        std::fs::rename(&current_exe, &backup_path).map_err(|e| GitHubError::Io {
            message: format!("Failed to backup current executable: {e}"),
        })?;
    }

    // Copy the new binary to the current executable location
    std::fs::copy(&extracted_binary, &current_exe).map_err(|e| GitHubError::Io {
        message: format!("Failed to replace CLI binary: {e}"),
    })?;

    // Make the binary executable on Unix
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let permissions = std::fs::Permissions::from_mode(0o755);
        std::fs::set_permissions(&current_exe, permissions).map_err(|e| GitHubError::Io {
            message: format!("Failed to set executable permissions: {e}"),
        })?;
    }

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

pub struct Arch(pub String);

pub enum SystemType {
    Linux(Arch),
    Darwin(Arch),
    Windows(Arch),
    Other(Arch, String),
}

impl SystemType {
    /// Get the OS type for the current platform.
    pub fn this_pc() -> SystemType {
        let arch = Arch(get_rust_arch().to_string());
        match std::env::consts::OS {
            "linux" => SystemType::Linux(arch),
            "macos" => SystemType::Darwin(arch),
            "windows" => SystemType::Windows(arch),
            other => SystemType::Other(arch, other.to_string()),
        }
    }

    fn arch(&self) -> &str {
        match self {
            SystemType::Linux(Arch(a))
            | SystemType::Darwin(Arch(a))
            | SystemType::Windows(Arch(a))
            | SystemType::Other(Arch(a), _) => a,
        }
    }

    /// Get the OS type name for the current platform.
    fn os_type_name(&self) -> &str {
        match self {
            SystemType::Linux(_) => "linux",
            SystemType::Darwin(_) => "darwin",
            SystemType::Windows(_) => "windows",
            SystemType::Other(_, name) => name,
        }
    }

    /// Get the CLI asset prefix for the current platform.
    fn cli_asset_prefix(&self) -> &str {
        match self {
            SystemType::Windows(_) => "spice.exe",
            _ => "spice",
        }
    }

    /// Get the runtime asset prefix for the current platform.
    fn runtime_asset_prefix(&self) -> &str {
        match self {
            SystemType::Windows(_) => "spiced.exe",
            _ => "spiced",
        }
    }

    /// Get the runtime asset names for the current platform.
    ///
    /// Returns a list of possible asset names to try, in order of preference.
    /// This handles the naming change between versions:
    /// - v1.11+/trunk: `spiced_metal_...` (models included by default)
    /// - v1.11 and earlier: `spiced_models_metal_...` (models suffix explicit)
    ///
    /// # Arguments
    /// * `flavor` - The flavor to install: "default" (auto-detect), or "cuda" (explicit CUDA)
    pub fn runtime_asset_names(&self, flavor: &str) -> Vec<String> {
        let mut names = Vec::new();

        // Determine the accelerator based on flavor
        let accelerator = match flavor {
            "cuda" => {
                // Explicit CUDA request - try to detect CUDA version
                #[cfg(target_os = "linux")]
                {
                    get_cuda_version()
                }
                #[cfg(not(target_os = "linux"))]
                {
                    tracing::warn!("CUDA flavor is only supported on Linux");
                    None
                }
            }
            _ => {
                // Default: auto-detect accelerator
                detect_accelerator()
            }
        };

        if let Some(accel) = accelerator {
            // New naming (v1.11+/trunk): accelerator without "models_" prefix
            names.push(format!(
                "{prefix}_{accel}_{os}_{arch}.tar.gz",
                prefix = self.runtime_asset_prefix(),
                os = self.os_type_name(),
                arch = self.arch()
            ));

            // Old naming (v1.11 and earlier): with "models_" prefix
            names.push(format!(
                "{prefix}_models_{accel}_{os}_{arch}.tar.gz",
                prefix = self.runtime_asset_prefix(),
                os = self.os_type_name(),
                arch = self.arch()
            ));
        }

        // Fallback: base runtime without accelerator
        names.push(format!(
            "{prefix}_{os}_{arch}.tar.gz",
            prefix = self.runtime_asset_prefix(),
            os = self.os_type_name(),
            arch = self.arch()
        ));

        names
    }

    /// Get the CLI asset name for the current platform.
    pub fn cli_asset_name(&self) -> String {
        format!(
            "{prefix}_{os}_{arch}.tar.gz",
            prefix = self.cli_asset_prefix(),
            os = self.os_type_name(),
            arch = self.arch()
        )
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    impl Arch {
        fn x86() -> Arch {
            Arch("x86_64".to_string())
        }

        fn arm() -> Arch {
            Arch("aarch64".to_string())
        }
    }

    impl SystemType {
        fn linux_x86() -> SystemType {
            SystemType::Linux(Arch::x86())
        }

        fn darwin_x86() -> SystemType {
            SystemType::Darwin(Arch::x86())
        }

        fn windows_x86() -> SystemType {
            SystemType::Windows(Arch::x86())
        }

        fn linux_arm() -> SystemType {
            SystemType::Linux(Arch::arm())
        }

        fn darwin_arm() -> SystemType {
            SystemType::Darwin(Arch::arm())
        }

        fn windows_arm() -> SystemType {
            SystemType::Windows(Arch::arm())
        }
    }

    #[rstest]
    #[case(SystemType::linux_x86(), "spice_linux_x86_64.tar.gz")]
    #[case(SystemType::darwin_x86(), "spice_darwin_x86_64.tar.gz")]
    #[case(SystemType::windows_x86(), "spice.exe_windows_x86_64.tar.gz")]
    #[case(SystemType::linux_arm(), "spice_linux_aarch64.tar.gz")]
    #[case(SystemType::darwin_arm(), "spice_darwin_aarch64.tar.gz")]
    #[case(SystemType::windows_arm(), "spice.exe_windows_aarch64.tar.gz")]
    fn test_cli_asset_name(#[case] os_type: SystemType, #[case] expected: &str) {
        assert_eq!(os_type.cli_asset_name(), expected);
    }

    #[rstest]
    // default flavor on x86 - auto-detects accelerator, but in tests no accelerator is present
    #[case(SystemType::linux_x86(), "default", "spiced_linux_x86_64.tar.gz")]
    #[case(SystemType::darwin_x86(), "default", "spiced_darwin_x86_64.tar.gz")]
    #[case(
        SystemType::windows_x86(),
        "default",
        "spiced.exe_windows_x86_64.tar.gz"
    )]
    // default flavor on arm
    #[case(SystemType::linux_arm(), "default", "spiced_linux_aarch64.tar.gz")]
    #[case(SystemType::darwin_arm(), "default", "spiced_darwin_aarch64.tar.gz")]
    #[case(
        SystemType::windows_arm(),
        "default",
        "spiced.exe_windows_aarch64.tar.gz"
    )]
    // unknown flavor falls back to default behavior
    #[case(SystemType::linux_x86(), "unknown", "spiced_linux_x86_64.tar.gz")]
    #[case(SystemType::darwin_x86(), "unknown", "spiced_darwin_x86_64.tar.gz")]
    #[case(
        SystemType::windows_x86(),
        "unknown",
        "spiced.exe_windows_x86_64.tar.gz"
    )]
    fn test_runtime_asset_names(
        #[case] os_type: SystemType,
        #[case] flavor: &str,
        #[case] expected: &str,
    ) {
        // In test environment, no accelerator is detected, so the list contains only the base name
        let names = os_type.runtime_asset_names(flavor);
        // The last entry should always be the fallback base name
        assert!(names.last().is_some_and(|n| n == expected));
    }
}
