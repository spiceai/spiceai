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
use crate::github::nightly::{self, NightlyError};
use crate::github::{
    GitHubClient, SystemType, download_release_asset_with_fallback, get_latest_release,
    get_release, replace_current_exe,
};
use chrono::{NaiveDate, Utc};
use clap::Args;
use std::io::Write as _;
use std::path::Path;

/// Arguments for the install command.
#[derive(Args, Debug, Default)]
#[command(
    about = "Install or reinstall the Spice.ai runtime",
    long_about = r#"Install or reinstall the Spice.ai runtime

Examples:
  spice install              # Install latest version (auto-detects Metal/CUDA)
  spice install v1.8.3       # Install specific version
  spice install cuda         # Install with CUDA support (Linux only)
  spice install nightly             # Install the latest nightly build of spice and spiced
  spice install nightly 2026-11-10  # Install the nightly build that ran on that date (UTC)

A nightly build is the CLI and runtime the build_nightly workflow builds from trunk
each day. GitHub serves them only to signed-in users: sign in with `gh auth login`,
or set GITHUB_TOKEN.

See more at: https://spiceai.org/docs/"#
)]
pub struct InstallArgs {
    /// Version to install (e.g., v1.8.3) and/or flavor (cuda), or `nightly` and optionally
    /// the date of a nightly build (e.g., `nightly 2026-11-10`)
    #[arg(num_args = 0..=2)]
    args: Vec<String>,

    /// Force installation even if already installed
    #[arg(short, long)]
    force: bool,
}

/// Parsed install arguments.
struct ParsedArgs {
    version: Option<String>,
    flavor: Flavor,
    /// `Some` for `nightly`, holding the date when one is given.
    nightly: Option<Option<NaiveDate>>,
}

/// Runtime flavor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Flavor {
    /// Default flavor - auto-detects hardware accelerator (Metal on macOS, CUDA on Linux if available)
    #[default]
    Default,
    /// Explicit CUDA flavor for Linux systems with NVIDIA GPUs
    Cuda,
}

impl Flavor {
    fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Cuda => "cuda",
        }
    }
}

/// Execute the install command.
pub async fn execute(ctx: &RuntimeContext, args: &InstallArgs) -> Result<()> {
    ctx.ensure_local_runtime_supported()?;

    let parsed = parse_args(&args.args)?;

    // Prepare installation directory
    ctx.prepare_install_dir()?;
    if let Some(date) = parsed.nightly {
        return install_nightly(ctx, date, args.force).await;
    }
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
        && let Ok(installed_version) = ctx.managed_runtime_version()
        && installed_version.contains(&release.tag_name)
    {
        tracing::info!("Spice.ai runtime {} already installed", release.tag_name);
        // Warned here too, not only after a download. This is the path a user
        // takes when they are trying to work out why the runtime they installed
        // is not the one running, so it is the path where being shadowed most
        // needs saying — and "already installed" on its own reads as agreement
        // that the install is the one in use.
        crate::context::warn_if_install_will_not_run(ctx);
        return Ok(());
    }

    // Get possible runtime asset names (handles version-specific naming)
    let asset_names =
        SystemType::this_pc().runtime_asset_names(parsed.flavor.as_str(), &release.tag_name);
    tracing::info!("Installing Spice.ai runtime {}...", release.tag_name);

    let downloaded_asset =
        download_release_asset_with_fallback(&client, &release, &asset_names, ctx.spice_bin_dir())
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

    tracing::info!(
        "Spice.ai runtime {} installed successfully",
        release.tag_name
    );
    crate::context::warn_if_install_will_not_run(ctx);

    // Write version file for caching
    let version_file = ctx.spice_runtime_dir().join("runtime_version.txt");
    std::fs::write(&version_file, format!("{}\n", release.tag_name)).ok();

    Ok(())
}

/// Install the nightly build that ran on `date`, or the newest one: `spiced` where `spice
/// install` puts it, and the CLI over the running one, as `spice upgrade` does.
async fn install_nightly(ctx: &RuntimeContext, date: Option<NaiveDate>, force: bool) -> Result<()> {
    let failed = |e: NightlyError| crate::error::Error::NightlyInstall {
        message: e.to_string(),
    };
    let platform =
        nightly::platform(std::env::consts::OS, std::env::consts::ARCH).map_err(failed)?;
    let mut client = GitHubClient::new_runtime_client();
    if !client.has_token() {
        let token = gh_token()
            .await
            .ok_or_else(|| failed(NightlyError::NeedsToken))?;
        client = client.with_token(token);
    }
    match date {
        Some(date) => tracing::info!("Finding the Spice nightly build of {date}..."),
        None => tracing::info!("Finding the latest Spice nightly build..."),
    }
    let build = nightly::find(&client, date, platform, Utc::now())
        .await
        .map_err(failed)?;

    if !force
        && let Ok(installed_version) = ctx.managed_runtime_version()
        && installed_version.contains(&build.version_marker())
    {
        tracing::info!("Spice nightly {} already installed", build.describe());
        crate::context::warn_if_install_will_not_run(ctx);
        return Ok(());
    }

    tracing::info!("Installing Spice nightly {}...", build.describe());
    let spiced = nightly::download_binary(&client, &build.runtime, "spiced")
        .await
        .map_err(failed)?;
    let spiced_path = ctx.spiced_path();
    install_binary(&spiced, &spiced_path).map_err(|e| crate::error::Error::NightlyInstall {
        message: format!("Failed to write {}: {e}", spiced_path.display()),
    })?;
    let cli = nightly::download_binary(&client, &build.cli, "spice")
        .await
        .map_err(failed)?;
    let cli_path = replace_current_exe(&cli).map_err(|e| failed(NightlyError::GitHub(e)))?;

    let version_file = ctx.spice_runtime_dir().join("runtime_version.txt");
    std::fs::write(&version_file, format!("{}\n", build.version_marker())).ok();
    tracing::info!(
        "Spice nightly {} installed: the runtime at {} and the CLI at {}",
        build.describe(),
        spiced_path.display(),
        cli_path.display()
    );
    crate::context::warn_if_install_will_not_run(ctx);
    Ok(())
}

/// The token `gh` holds for github.com, when `gh` is installed and signed in.
async fn gh_token() -> Option<String> {
    let output = tokio::process::Command::new("gh")
        .args(["auth", "token", "--hostname", "github.com"])
        .output()
        .await
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let token = String::from_utf8(output.stdout).ok()?.trim().to_string();
    (!token.is_empty()).then_some(token)
}

/// Put `binary` at `path`, executable, replacing what is there in one rename, so a runtime
/// that is running keeps its file and the next start finds the new one.
fn install_binary(binary: &[u8], path: &Path) -> std::io::Result<()> {
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let mut file = tempfile::NamedTempFile::new_in(dir)?;
    file.write_all(binary)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(file.path(), std::fs::Permissions::from_mode(0o755))?;
    }
    file.persist(path).map_err(|e| e.error)?;
    Ok(())
}

/// Parse the command arguments into version and flavor.
fn parse_args(args: &[String]) -> Result<ParsedArgs> {
    if args
        .first()
        .is_some_and(|arg| arg.eq_ignore_ascii_case("nightly"))
    {
        let date = match args.get(1) {
            None => None,
            Some(flavor) if flavor.eq_ignore_ascii_case("cuda") => {
                return Err(crate::error::Error::InvalidArgument {
                    message: "Nightly builds have no CUDA flavor. Run `spice install nightly` for this machine's nightly build, or `spice install cuda` for the latest release with CUDA.".to_string(),
                });
            }
            Some(date) => Some(nightly::parse_date(date).map_err(|e| {
                crate::error::Error::InvalidArgument {
                    message: e.to_string(),
                }
            })?),
        };
        return Ok(ParsedArgs {
            version: None,
            flavor: Flavor::Default,
            nightly: Some(date),
        });
    }

    let mut version = None;
    let mut flavor = Flavor::Default;

    for arg in args {
        if arg.starts_with('v') && is_semver(arg) {
            version = Some(arg.clone());
        } else {
            match arg.to_lowercase().as_str() {
                "cuda" => flavor = Flavor::Cuda,
                "default" => flavor = Flavor::Default,
                _ => {
                    tracing::error!(
                        "Invalid argument: {arg}. Expected version (e.g., v1.8.3), flavor (cuda), or nightly"
                    );
                    return Err(crate::error::Error::InvalidArgument {
                        message: format!(
                            "Invalid argument: {arg}. Expected version (e.g., v1.8.3), flavor (cuda), or nightly"
                        ),
                    });
                }
            }
        }
    }

    Ok(ParsedArgs {
        version,
        flavor,
        nightly: None,
    })
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
