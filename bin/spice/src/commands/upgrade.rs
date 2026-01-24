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

//! Upgrade command implementation - upgrades the Spice.ai runtime to the latest or specified version.

use crate::context::RuntimeContext;
use crate::error::Result;
use crate::github::{
    GitHubClient, download_release_asset, get_latest_release, get_release, get_runtime_asset_name,
};
use clap::Args;

/// Arguments for the upgrade command.
#[derive(Args, Debug)]
#[command(
    about = "Upgrades the Spice runtime to the latest or specified version",
    long_about = r#"Upgrades the Spice runtime to the latest or specified version

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

    // Check if runtime is installed
    if !ctx.is_runtime_installed() {
        tracing::info!(
            "Spice runtime is not installed. Run `spice install` to install the runtime."
        );
        return Ok(());
    }

    // Get current version
    let current_version = ctx
        .runtime_version()
        .unwrap_or_else(|_| "unknown".to_string());
    tracing::info!("Current version: {current_version}");

    let client = GitHubClient::new_runtime_client();

    // Get the target release
    let release = if let Some(version) = &args.version {
        tracing::info!("Checking for Spice runtime release {version}...");
        get_release(&client, version).await.map_err(|e| {
            tracing::error!("Failed to fetch release {version}: {e}");
            crate::error::Error::RuntimeVersion {
                message: e.to_string(),
            }
        })?
    } else {
        tracing::info!("Checking for latest Spice runtime release...");
        get_latest_release(&client).await.map_err(|e| {
            tracing::error!("Failed to fetch latest release: {e}");
            crate::error::Error::RuntimeVersion {
                message: e.to_string(),
            }
        })?
    };

    // Check if upgrade is needed
    if current_version.contains(&release.tag_name) && !args.force {
        tracing::info!(
            "Using version {}. Runtime upgrade not required.",
            release.tag_name
        );
        return Ok(());
    }

    // Prepare installation directory
    ctx.prepare_install_dir()?;

    // Determine flavor from current installation
    // For now, default to "default" flavor - could be enhanced to detect current flavor
    let flavor = "default";
    let allow_accelerator = true;

    // Download and install the runtime
    let asset_name = get_runtime_asset_name(flavor, allow_accelerator);
    tracing::info!(
        "Upgrading Spice.ai runtime to {} ({})...",
        release.tag_name,
        asset_name
    );

    download_release_asset(&client, &release, &asset_name, ctx.spice_bin_dir())
        .await
        .map_err(|e| {
            tracing::error!("Failed to download runtime: {e}");
            crate::error::Error::RuntimeVersion {
                message: e.to_string(),
            }
        })?;

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
    std::fs::write(&version_file, format!("{}\n", release.tag_name)).ok();

    tracing::info!(
        "Spice runtime upgraded to {} successfully.",
        release.tag_name
    );

    Ok(())
}
