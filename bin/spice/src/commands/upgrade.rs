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

//! Upgrade command implementation - upgrades the Spice.ai CLI and runtime to the latest or specified version.

use crate::context::RuntimeContext;
use crate::error::Result;
use crate::github::{
    GitHubClient, SystemType, download_release_asset_with_fallback, get_latest_release,
    get_release, upgrade_cli_in_place,
};
use clap::Args;

/// Arguments for the upgrade command.
#[derive(Args, Debug)]
#[command(
    about = "Upgrades the Spice CLI and runtime to the latest or specified version",
    long_about = r#"Upgrades the Spice CLI and runtime to the latest or specified version

Examples:
  spice upgrade              # Upgrade to latest version
  spice upgrade v1.8.3       # Upgrade to specific version

See more at: https://spiceai.org/docs/"#,
    disable_version_flag = true
)]
pub struct UpgradeArgs {
    /// Version to upgrade to (e.g., v1.8.3)
    #[arg(name = "target_version")]
    pub version: Option<String>,

    /// Force upgrade even if already at the target version
    #[arg(short, long)]
    pub force: bool,
}

/// Execute the upgrade command.
pub async fn execute(ctx: &RuntimeContext, args: &UpgradeArgs) -> Result<()> {
    // Validate version format if provided
    if let Some(ref version) = args.version
        && !version.starts_with('v')
    {
        tracing::error!("Invalid version format: {version}. Expected format: v1.8.3");
        return Ok(());
    }

    // Get current CLI version
    let current_cli_version = env!("CARGO_PKG_VERSION");
    tracing::info!("Current CLI version: v{current_cli_version}");

    // Get current runtime version if installed
    let current_runtime_version = if ctx.is_runtime_installed() {
        ctx.runtime_version().ok()
    } else {
        None
    };
    if let Some(ref runtime_version) = current_runtime_version {
        tracing::info!("Current runtime version: {runtime_version}");
    } else {
        tracing::info!("Runtime is not installed.");
    }

    let client = GitHubClient::new_runtime_client();

    // Get the target release
    let release = if let Some(version) = &args.version {
        tracing::info!("Checking for Spice release {version}...");
        get_release(&client, version).await.map_err(|e| {
            tracing::error!("Failed to fetch release {version}: {e}");
            crate::error::Error::RuntimeVersion {
                message: e.to_string(),
            }
        })?
    } else {
        tracing::info!("Checking for latest Spice release...");
        get_latest_release(&client).await.map_err(|e| {
            tracing::error!("Failed to fetch latest release: {e}");
            crate::error::Error::RuntimeVersion {
                message: e.to_string(),
            }
        })?
    };

    let target_version = &release.tag_name;

    // Check if CLI upgrade is needed
    let cli_needs_upgrade =
        args.force || !format!("v{current_cli_version}").contains(target_version);

    // Check if runtime upgrade is needed
    let runtime_needs_upgrade = args.force
        || current_runtime_version.is_none()
        || !current_runtime_version
            .as_ref()
            .is_some_and(|v| v.contains(target_version));

    if !cli_needs_upgrade && !runtime_needs_upgrade {
        tracing::info!(
            "Already using version {target_version}. CLI and runtime upgrade not required."
        );
        return Ok(());
    }

    // Upgrade CLI first (in-place)
    if cli_needs_upgrade {
        let cli_asset_name = SystemType::this_pc().cli_asset_name();
        tracing::info!("Upgrading Spice CLI to {target_version} ({cli_asset_name})...");

        upgrade_cli_in_place(&client, &release, &cli_asset_name)
            .await
            .map_err(|e| {
                tracing::error!("Failed to upgrade CLI: {e}");
                crate::error::Error::RuntimeVersion {
                    message: e.to_string(),
                }
            })?;

        tracing::info!("Spice CLI upgraded to {target_version} successfully.");
    } else {
        tracing::info!("CLI is already at {target_version}.");
    }

    // Upgrade runtime
    if runtime_needs_upgrade {
        // Prepare installation directory
        ctx.prepare_install_dir()?;

        // Get possible runtime asset names (handles version-specific naming)
        // Default flavor auto-detects accelerator (Metal on macOS, CUDA on Linux)
        let asset_names = SystemType::this_pc().runtime_asset_names("default");
        tracing::info!("Upgrading Spice runtime to {target_version}...");

        let downloaded_asset = download_release_asset_with_fallback(
            &client,
            &release,
            &asset_names,
            ctx.spice_bin_dir(),
        )
        .await
        .map_err(|e| {
            tracing::error!("Failed to download runtime: {e}");
            crate::error::Error::RuntimeVersion {
                message: e.to_string(),
            }
        })?;

        tracing::debug!("Downloaded runtime asset: {downloaded_asset}");

        // Make the binary executable
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let spiced_path = ctx.spiced_path();
            if spiced_path.exists() {
                let permissions = std::fs::Permissions::from_mode(0o755);
                std::fs::set_permissions(&spiced_path, permissions).ok();
            }
        }

        // Update version file
        let version_file = ctx.spice_runtime_dir().join("runtime_version.txt");
        std::fs::write(&version_file, format!("{target_version}\n")).ok();

        tracing::info!("Spice runtime upgraded to {target_version} successfully.");
    } else {
        tracing::info!("Runtime is already at {target_version}.");
    }

    Ok(())
}
