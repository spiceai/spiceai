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

//! Install command implementation - installs or reinstalls the Spice.ai runtime.

use crate::context::RuntimeContext;
use crate::error::Result;
use crate::github::{
    GitHubClient, download_release_asset, get_latest_release, get_release, get_runtime_asset_name,
};
use clap::Args;

/// Arguments for the install command.
#[derive(Args, Debug, Default)]
#[command(
    about = "Install or reinstall the Spice.ai runtime and CLI",
    long_about = r#"Install or reinstall the Spice.ai runtime and CLI

Examples:
  spice install              # Install latest version
  spice install ai           # Install AI flavor
  spice install v1.8.3       # Install specific version
  spice install v1.8.3 ai    # Install specific version with AI flavor

See more at: https://spiceai.org/docs/"#
)]
pub struct InstallArgs {
    /// Version to install (e.g., v1.8.3) and/or flavor (ai)
    #[arg(num_args = 0..=2)]
    args: Vec<String>,

    /// Force installation even if already installed
    #[arg(short, long)]
    force: bool,

    /// Install the CPU-only version (only valid with 'ai' flavor)
    #[arg(short, long)]
    cpu: bool,
}

/// Parsed install arguments.
struct ParsedArgs {
    version: Option<String>,
    flavor: Flavor,
}

/// Runtime flavor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flavor {
    Default,
    Ai,
}

impl Flavor {
    fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Ai => "ai",
        }
    }
}

/// Execute the install command.
pub async fn execute(ctx: &RuntimeContext, args: &InstallArgs) -> Result<()> {
    let parsed = parse_args(&args.args)?;

    // Validate CPU flag
    if args.cpu && parsed.flavor != Flavor::Ai {
        tracing::error!(
            "CPU flag is only allowed when installing the 'ai' flavor. Try: `spice install ai --cpu`"
        );
        return Ok(());
    }

    let allow_accelerator = !args.cpu;

    // Prepare installation directory
    ctx.prepare_install_dir()?;

    let client = GitHubClient::new_runtime_client();

    // Get the release
    let release = if let Some(version) = &parsed.version {
        tracing::info!("Installing Spice version {version}...");
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

    // Check if already installed (unless force)
    if !args.force
        && ctx.is_runtime_installed()
        && let Ok(installed_version) = ctx.runtime_version()
        && installed_version.contains(&release.tag_name)
    {
        tracing::info!("Spice.ai runtime {} already installed", release.tag_name);
        return Ok(());
    }

    // Download and install the runtime
    let asset_name = get_runtime_asset_name(parsed.flavor.as_str(), allow_accelerator);
    tracing::info!(
        "Installing Spice.ai runtime {} ({})...",
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

    tracing::info!(
        "Spice.ai runtime {} installed successfully",
        release.tag_name
    );

    // Write version file for caching
    let version_file = ctx.spice_runtime_dir().join("runtime_version.txt");
    std::fs::write(&version_file, format!("{}\n", release.tag_name)).ok();

    Ok(())
}

/// Parse the command arguments into version and flavor.
fn parse_args(args: &[String]) -> Result<ParsedArgs> {
    let mut version = None;
    let mut flavor = Flavor::Default;

    for arg in args {
        if arg.starts_with('v') && is_semver(arg) {
            version = Some(arg.clone());
        } else {
            match arg.to_lowercase().as_str() {
                "ai" => flavor = Flavor::Ai,
                "default" => flavor = Flavor::Default,
                _ => {
                    tracing::error!(
                        "Invalid argument: {arg}. Expected version (e.g., v1.8.3) or flavor (ai)"
                    );
                    return Err(crate::error::Error::InvalidArgument {
                        message: format!(
                            "Invalid argument: {arg}. Expected version (e.g., v1.8.3) or flavor (ai)"
                        ),
                    });
                }
            }
        }
    }

    Ok(ParsedArgs { version, flavor })
}

/// Check if a string is a valid semver version.
fn is_semver(s: &str) -> bool {
    // Simple semver check: vX.Y.Z or vX.Y.Z-suffix
    let s = s.strip_prefix('v').unwrap_or(s);
    let parts: Vec<&str> = s.split('-').next().unwrap_or(s).split('.').collect();

    if parts.len() < 2 {
        return false;
    }

    parts.iter().take(3).all(|p| p.parse::<u32>().is_ok())
}
