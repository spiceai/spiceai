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

//! Version command implementation.

use crate::context::RuntimeContext;
use crate::error::Result;
use crate::github::{GitHubClient, get_latest_release};
use clap::Args;
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

/// Cache validity duration for version checks.
const VERSION_CACHE_DURATION: Duration = Duration::from_secs(24 * 60 * 60); // 24 hours

/// Arguments for the version command.
#[derive(Args, Debug)]
pub struct VersionArgs {
    /// Show only the CLI version (no runtime version)
    #[arg(long)]
    cli_only: bool,
}

/// Get the CLI version string.
#[must_use]
pub fn cli_version() -> String {
    let version = env!("CARGO_PKG_VERSION");
    if cfg!(feature = "release") {
        format!("v{version}")
    } else {
        let git_hash = option_env!("GIT_COMMIT_HASH").unwrap_or("unknown");
        format!("v{version} ({git_hash})")
    }
}

/// Get just the semver version (e.g., "v1.2.3").
#[must_use]
fn cli_semver() -> String {
    let version = env!("CARGO_PKG_VERSION");
    format!("v{version}")
}

/// Get the path to the cached version file.
fn version_cache_path(ctx: &RuntimeContext) -> PathBuf {
    ctx.spice_runtime_dir().join("cli_version.txt")
}

/// Check if a version string indicates a pre-release build.
/// Matches Go CLI logic: prefix "local" or contains "build".
fn is_prerelease(version: &str) -> bool {
    version.starts_with("local") || version.contains("build")
}

/// Compare two semver versions. Returns true if `latest` is newer than `current`.
fn is_newer_version(current: &str, latest: &str) -> bool {
    // Strip 'v' prefix if present
    let current = current.strip_prefix('v').unwrap_or(current);
    let latest = latest.strip_prefix('v').unwrap_or(latest);

    // Parse versions
    let parse_version = |s: &str| -> (u32, u32, u32) {
        let parts: Vec<&str> = s.split('-').next().unwrap_or(s).split('.').collect();
        (
            parts.first().and_then(|s| s.parse().ok()).unwrap_or(0),
            parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0),
            parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(0),
        )
    };

    let current_parts = parse_version(current);
    let latest_parts = parse_version(latest);

    latest_parts > current_parts
}

/// Check for the latest CLI release version.
async fn check_latest_version(ctx: &RuntimeContext) -> Option<String> {
    let cache_path = version_cache_path(ctx);

    // Check cache first - if valid, return cached version
    if let Ok(metadata) = fs::metadata(&cache_path)
        && let Ok(modified) = metadata.modified()
        && let Ok(age) = SystemTime::now().duration_since(modified)
        && age < VERSION_CACHE_DURATION
        && let Ok(version) = fs::read_to_string(&cache_path)
    {
        return Some(version.trim().to_string());
    }

    // Fetch from GitHub
    let client = GitHubClient::new_runtime_client();
    let release = get_latest_release(&client).await.ok()?;
    let version = release.tag_name;

    // Update cache
    if let Some(parent) = cache_path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let _ = fs::write(&cache_path, format!("{version}\n"));

    Some(version)
}

/// Display upgrade notification if a newer version is available.
async fn check_and_notify_upgrade(ctx: &RuntimeContext) {
    let current = cli_semver();

    // Skip check for pre-release builds
    if is_prerelease(&current) {
        return;
    }

    let Some(latest) = check_latest_version(ctx).await else {
        return;
    };

    if is_newer_version(&current, &latest) {
        // Use green color for the version
        let green_version = format!("\x1b[92m{latest}\x1b[0m");
        println!();
        tracing::info!("CLI version {green_version} is now available!");
        tracing::info!("To upgrade, run \"spice upgrade\".");
    }
}

/// Execute the version command.
///
/// # Errors
///
/// Returns an error if the runtime version cannot be determined.
pub async fn execute(ctx: &RuntimeContext, args: &VersionArgs) -> Result<()> {
    println!("CLI version:     {}", cli_version());

    if !args.cli_only {
        match ctx.runtime_version() {
            Ok(version) => {
                println!("Runtime version: {version}");
            }
            Err(_) => {
                println!("Runtime version: not installed");
            }
        }
    }

    // Check for newer version (non-blocking, failures are silent)
    check_and_notify_upgrade(ctx).await;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_version_format() {
        let version = cli_version();
        assert!(version.starts_with('v'));
        assert!(version.contains('('));
        assert!(version.contains(')'));
    }

    #[test]
    fn test_is_newer_version() {
        // Basic version comparisons
        assert!(is_newer_version("v1.0.0", "v1.0.1"));
        assert!(is_newer_version("v1.0.0", "v1.1.0"));
        assert!(is_newer_version("v1.0.0", "v2.0.0"));

        // Same version
        assert!(!is_newer_version("v1.0.0", "v1.0.0"));

        // Older version
        assert!(!is_newer_version("v1.0.1", "v1.0.0"));
        assert!(!is_newer_version("v2.0.0", "v1.0.0"));

        // Without 'v' prefix
        assert!(is_newer_version("1.0.0", "1.0.1"));

        // Pre-release suffix is stripped (both compare as 1.0.0)
        assert!(!is_newer_version("v1.0.0-alpha", "v1.0.0"));
        assert!(is_newer_version("v1.0.0-alpha", "v1.0.1"));
    }

    #[test]
    fn test_is_prerelease() {
        // Matches Go CLI: starts with "local" or contains "build"
        assert!(is_prerelease("local"));
        assert!(is_prerelease("local-dev"));
        assert!(is_prerelease("v1.0.0-build.123"));
        assert!(is_prerelease("v1.0.0+build"));

        // These are NOT pre-release (matches Go behavior)
        assert!(!is_prerelease("v1.0.0"));
        assert!(!is_prerelease("v1.2.3"));
        assert!(!is_prerelease("v1.0.0-alpha")); // No "local" prefix or "build"
        assert!(!is_prerelease("v1.0.0-dev")); // No "local" prefix or "build"
    }
}
