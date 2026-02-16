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

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use flate2::read::GzDecoder;
use serde::Deserialize;
use std::env;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use tar::Archive;
use walkdir::WalkDir;

#[derive(Parser)]
#[command(name = "pr-builds")]
#[command(about = "Manage PR builds for Spice.ai", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Trigger a build for the current (or specified) branch.
    ///
    /// Triggers the '`build_and_release`' workflow in GitHub Actions.
    /// If an active build exists for the latest commit (SHA-based), it will be reused.
    /// If a successful build exists (for this commit), no action is taken.
    Trigger {
        /// Branch to trigger build for. Defaults to current branch.
        #[arg(short, long)]
        branch: Option<String>,

        /// PR number to resolve to a branch.
        #[arg(short, long)]
        pr: Option<u64>,

        /// Wait for the build to complete.
        ///
        /// If an existing build is reused, this will wait for that build.
        #[arg(short, long)]
        wait: bool,
    },
    /// Install the latest binary for the current (or specified) branch.
    ///
    /// Downloads and installs the 'spiced' binary from the latest successful
    /// GitHub Actions run for the branch.
    Install {
        /// Branch to install binary for. Defaults to current branch.
        #[arg(short, long)]
        branch: Option<String>,

        /// PR number to resolve to a branch.
        #[arg(short, long)]
        pr: Option<u64>,
    },
    /// Run the binary for the current (or specified) branch.
    ///
    /// If the binary is not installed locally, it will be automatically downloaded
    /// and installed from the latest successful build.
    Run {
        /// Branch to run binary for. Defaults to current branch.
        #[arg(short, long)]
        branch: Option<String>,

        /// PR number to resolve to a branch.
        #[arg(short, long)]
        pr: Option<u64>,

        /// Interactive mode: select a branch from installed binaries
        #[arg(short, long)]
        interactive: bool,

        /// Arguments to pass to spiced
        #[arg(last = true)]
        args: Vec<String>,
    },
}

#[derive(Deserialize, Debug)]
struct GhRun {
    #[serde(rename = "databaseId")]
    database_id: u64,
}

#[derive(Deserialize, Debug)]
struct GhPr {
    #[serde(rename = "headRefName")]
    head_ref_name: String,
}

#[derive(Deserialize)]
struct GhRepo {
    #[serde(rename = "nameWithOwner")]
    name_with_owner: String,
}

#[cfg(unix)]
fn main() -> Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Trigger { branch, pr, wait } => trigger_build(branch.as_deref(), *pr, *wait),
        Commands::Install { branch, pr } => install_build(branch.as_deref(), *pr),
        Commands::Run {
            branch,
            pr,
            interactive,
            args,
        } => run_build(branch.as_deref(), *pr, *interactive, args),
    }
}

#[cfg(not(unix))]
fn main() -> Result<()> {
    anyhow::bail!("This tool is currently only supported on Unix-like systems.");
}

#[cfg(unix)]
fn resolve_branch_or_pr(branch: Option<&str>, pr: Option<u64>) -> Result<String> {
    if let Some(b) = branch {
        return Ok(b.to_string());
    }

    if let Some(pr_num) = pr {
        println!("Resolving PR #{pr_num} to branch...");
        let output = Command::new("gh")
            .args(["pr", "view", &pr_num.to_string(), "--json", "headRefName"])
            .output()
            .context("Failed to execute gh pr view")?;

        if !output.status.success() {
            anyhow::bail!(
                "Failed to resolve PR #{}: {}",
                pr_num,
                String::from_utf8_lossy(&output.stderr)
            );
        }

        let pr_data: GhPr = serde_json::from_slice(&output.stdout)?;
        println!(
            "Resolved PR #{} to branch '{}'",
            pr_num, pr_data.head_ref_name
        );
        return Ok(pr_data.head_ref_name);
    }

    get_current_branch()
}

#[cfg(unix)]
fn get_current_branch() -> Result<String> {
    let output = Command::new("git")
        .args(["branch", "--show-current"])
        .output()
        .context("Failed to execute git command")?;

    if !output.status.success() {
        anyhow::bail!("git branch --show-current failed");
    }

    let branch = String::from_utf8(output.stdout)
        .context("Branch name is not valid UTF-8")?
        .trim()
        .to_string();

    if branch.is_empty() {
        anyhow::bail!("Could not determine current branch");
    }
    Ok(branch)
}

fn validate_branch_name(branch: &str) -> Result<()> {
    if branch.is_empty() {
        anyhow::bail!("Branch name cannot be empty");
    }
    if branch.contains("..") {
        anyhow::bail!("Branch name cannot contain '..'");
    }
    if branch.starts_with('/') || branch.contains(':') {
        anyhow::bail!("Branch name contains invalid characters");
    }
    Ok(())
}

#[cfg(unix)]
fn get_repo_owner_name() -> Result<String> {
    let output = Command::new("gh")
        .args(["repo", "view", "--json", "nameWithOwner"])
        .output()
        .context("Failed to determine current repository via gh repo view")?;

    if !output.status.success() {
        anyhow::bail!(
            "gh repo view failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let gh_repo: GhRepo =
        serde_json::from_slice(&output.stdout).context("Failed to parse gh repo view output")?;
    Ok(gh_repo.name_with_owner)
}

#[cfg(unix)]
fn trigger_build(branch: Option<&str>, pr: Option<u64>, wait: bool) -> Result<()> {
    let branch = resolve_branch_or_pr(branch, pr)?;
    validate_branch_name(&branch)?;
    let repo_owner_name = get_repo_owner_name()?;

    // Get the latest commit SHA for the branch
    println!("Fetching latest commit SHA for branch '{branch}'...");
    let output = Command::new("gh")
        .args([
            "api",
            &format!("repos/{repo_owner_name}/branches/{branch}"),
            "--jq",
            ".commit.sha",
        ])
        .output()
        .context("Failed to fetch branch SHA")?;

    if !output.status.success() {
        anyhow::bail!(
            "Failed to get branch SHA: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let latest_sha = String::from_utf8(output.stdout)?.trim().to_string();
    println!("Latest SHA: {latest_sha}");

    // Check for existing runs for this SHA
    println!("Checking for existing builds for this SHA...");
    let output = Command::new("gh")
        .args([
            "run",
            "list",
            "--workflow",
            "build_and_release.yml",
            "--branch",
            &branch,
            "--limit",
            "5", // Check a few recent runs
            "--json",
            "databaseId,headSha,status,conclusion",
        ])
        .output()
        .context("Failed to list recent runs")?;

    let mut existing_run_id: Option<u64> = None;
    let mut max_seen_id: u64 = 0;

    if output.status.success() {
        let runs: Vec<serde_json::Value> = serde_json::from_slice(&output.stdout)?;
        for run in runs {
            if let Some(id) = run["databaseId"].as_u64()
                && id > max_seen_id
            {
                max_seen_id = id;
            }

            if let Some(run_sha) = run["headSha"].as_str()
                && run_sha == latest_sha
            {
                // Found a run for the same SHA
                let status = run["status"].as_str().unwrap_or("");
                let conclusion = run["conclusion"].as_str().unwrap_or("");
                let id = run["databaseId"].as_u64();

                if let Some(id) = id {
                    if status == "completed" && conclusion == "success" {
                        println!(
                            "Found successful build for this SHA (Run ID: {id}). No need to trigger.",
                        );
                        return Ok(());
                    } else if status != "completed" {
                        println!("Found active build for this SHA (Run ID: {id}). Reusing it.",);
                        existing_run_id = Some(id);
                        break;
                    }

                    println!(
                        "Found failed/cancelled build for this SHA (Run ID: {id}). Retrying...",
                    );
                    // If failed, we probably want to trigger a new one, so continue loop or stop?
                    // Default behavior: trigger new if latest is failed.
                }
            }
        }
    }

    let run_id_to_watch = if let Some(id) = existing_run_id {
        id
    } else {
        println!("No active build found for latest SHA. Triggering new build...");

        let platform_option = {
            let os = env::consts::OS;
            let arch = env::consts::ARCH;
            match (os, arch) {
                ("linux", "aarch64") => "Linux aarch64".to_string(),
                ("linux", "x86_64") => "Linux x64".to_string(),
                ("macos", "aarch64") => "macOS aarch64 (Apple Silicon)".to_string(),
                ("macos", "x86_64") => {
                    anyhow::bail!(
                        "Failed to determine build platform for OS macOS and architecture x86_64: Intel macOS is not supported by the build_and_release workflow. Use an Apple Silicon macOS or supported Linux machine to trigger this build."
                    );
                }
                (other_os, other_arch) => {
                    anyhow::bail!(
                        "Failed to determine build platform for OS {other_os} and architecture {other_arch}: Unsupported combination for build_and_release workflow. Supported combinations are: linux/aarch64, linux/x86_64, macos/aarch64.",
                    );
                }
            }
        };

        println!("Requesting platform: {platform_option}");

        let status = Command::new("gh")
            .args([
                "workflow",
                "run",
                "build_and_release.yml",
                "--ref",
                &branch,
                "-f",
                &format!("platform_option={platform_option}"),
            ])
            .status()
            .context("Failed to execute gh workflow run")?;

        if !status.success() {
            anyhow::bail!("Failed to trigger build");
        }

        println!("Build triggered successfully.");

        if !wait {
            println!("You can check the status with:");
            println!("  gh run list --workflow build_and_release.yml --branch \"{branch}\"",);
            return Ok(());
        }

        println!("Waiting for build to start...");

        let mut found_new_run_id: Option<u64> = None;

        // Poll for up to 30 seconds to find the new run
        for _ in 0..10 {
            std::thread::sleep(std::time::Duration::from_secs(3));

            let output = Command::new("gh")
                .args([
                    "run",
                    "list",
                    "--workflow",
                    "build_and_release.yml",
                    "--branch",
                    &branch,
                    "--limit",
                    "1",
                    "--json",
                    "databaseId,status,createdAt,headSha",
                ])
                .output()
                .context("Failed to fetch latest run ID")?;

            if output.status.success() {
                let runs: Vec<serde_json::Value> = serde_json::from_slice(&output.stdout)?;
                if let Some(run) = runs.first() {
                    let status = run["status"].as_str().unwrap_or("");
                    let run_sha = run["headSha"].as_str().unwrap_or("");
                    let id = run["databaseId"].as_u64();

                    // Ensure we picked up a run for the correct SHA that is active and NEW (id > max_seen_id)
                    if let Some(id) = id
                        && id > max_seen_id
                        && (status == "queued"
                            || status == "in_progress"
                            || status == "requested"
                            || status == "waiting")
                        && run_sha == latest_sha
                    {
                        found_new_run_id = Some(id);
                        break;
                    }
                }
            }
        }

        found_new_run_id
            .context("Could not find the newly triggered run (or it completed instantly).")?
    };

    if wait {
        println!("Waiting for Run ID: {run_id_to_watch}...");
        let status = Command::new("gh")
            .args(["run", "watch", &run_id_to_watch.to_string()])
            .status()
            .context("Failed to watch run")?;

        if status.success() {
            println!("Build completed successfully!");
        } else {
            anyhow::bail!("Build failed or was cancelled.");
        }
    }

    Ok(())
}

fn get_artifact_names() -> Result<Vec<String>> {
    let (os, arch) = match env::consts::OS {
        // CI only builds aarch64 for macOS (Apple Silicon)
        "macos" => ("darwin", "aarch64"),
        // For Linux, the CI uses standard architecture names (x86_64, aarch64)
        "linux" => ("linux", env::consts::ARCH),
        // Fallback/Default
        other => {
            anyhow::bail!(
                "Unsupported OS '{other}' for PR build artifacts; only 'macos' and 'linux' are supported",
            );
        }
    };

    // Prioritize metal for darwin
    let mut names = Vec::new();
    if os == "darwin" {
        names.push(format!("spiced_metal_{os}_{arch}"));
    }
    names.push(format!("spiced_{os}_{arch}"));

    Ok(names)
}

#[derive(Deserialize, Debug)]
struct Artifact {
    name: String,
}

#[derive(Deserialize, Debug)]
struct ArtifactList {
    artifacts: Vec<Artifact>,
}

#[cfg(unix)]
fn install_build(branch: Option<&str>, pr: Option<u64>) -> Result<()> {
    let branch = resolve_branch_or_pr(branch, pr)?;
    validate_branch_name(&branch)?;
    let repo_owner_name = get_repo_owner_name()?;

    let target_dir = dirs::home_dir()
        .context("Could not find home directory")?
        .join(".spice/bin")
        .join(&branch);

    println!("Looking for latest successful build for branch: {branch}...",);

    let output = Command::new("gh")
        .args([
            "run",
            "list",
            "--workflow",
            "build_and_release.yml",
            "--branch",
            &branch,
            "--status",
            "success",
            "--limit",
            "1",
            "--json",
            "databaseId",
        ])
        .output()
        .context("Failed to execute gh run list")?;

    if !output.status.success() {
        anyhow::bail!(
            "gh run list failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let runs: Vec<GhRun> = serde_json::from_slice(&output.stdout)?;
    let run = runs
        .first()
        .context(format!("No successful build found for branch '{branch}'"))?;
    let run_id = run.database_id;

    println!("Found Run ID: {run_id}");

    // List artifacts for the run to check which one exists
    println!("Checking available artifacts...");
    let output = Command::new("gh")
        .args([
            "api",
            &format!("repos/{repo_owner_name}/actions/runs/{run_id}/artifacts",),
        ])
        .output()
        .context("Failed to fetch artifacts list")?;

    if !output.status.success() {
        anyhow::bail!(
            "Failed to list artifacts: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let artifact_list: ArtifactList = serde_json::from_slice(&output.stdout)?;
    let available_artifacts: Vec<String> = artifact_list
        .artifacts
        .into_iter()
        .map(|a| a.name)
        .collect();

    let wanted_artifacts = get_artifact_names()?;
    let artifact_to_download = wanted_artifacts
        .iter()
        .find(|name| available_artifacts.contains(name))
        .context("No compatible artifact found in this build")?;

    let temp_dir = tempfile::tempdir()?;
    let temp_path = temp_dir.path();

    println!(
        "Downloading artifact '{}' to {}...",
        artifact_to_download,
        temp_path.display()
    );

    let status = Command::new("gh")
        .args([
            "run",
            "download",
            &run_id.to_string(),
            "-n",
            artifact_to_download,
            "-D",
        ])
        .arg(temp_path)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .status()
        .context("Failed to execute gh run download")?;

    if !status.success() {
        anyhow::bail!("Failed to download artifact");
    }

    println!("Installing to {}...", target_dir.display());
    fs::create_dir_all(&target_dir)?;

    // Find tar.gz
    let mut tar_file: Option<PathBuf> = None;
    for entry in fs::read_dir(temp_path)? {
        let entry = entry?;
        let path = entry.path();
        if let Some(ext) = path.extension()
            && ext == "gz"
        {
            tar_file = Some(path);
            break;
        }
    }

    let tar_file = tar_file.context("No tar.gz file found in downloaded artifact")?;

    // Extract directly to target location
    let tar_gz = fs::File::open(&tar_file)?;
    let tar = GzDecoder::new(tar_gz);
    let mut archive = Archive::new(tar);

    // Use a temp file in the target directory for extraction
    let temp_target = target_dir.join("spiced.tmp");
    let mut found_binary = false;

    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?;
        if path.ends_with("spiced") {
            println!("Extracting binary to {}...", temp_target.display());
            entry.unpack(&temp_target)?;
            found_binary = true;
            break;
        }
    }

    if !found_binary {
        anyhow::bail!("Could not find 'spiced' binary in archive");
    }

    let target_binary = target_dir.join("spiced");

    // Make executable before rename
    let mut perms = fs::metadata(&temp_target)?.permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&temp_target, perms)?;

    // Rename might fail if cross-device link (EXDEV), so we try copy+remove as fallback
    if let Err(e) = fs::rename(&temp_target, &target_binary) {
        if e.kind() == std::io::ErrorKind::CrossesDevices {
            fs::copy(&temp_target, &target_binary)?;
            let _ = fs::remove_file(&temp_target);
        } else {
            return Err(e.into());
        }
    }

    println!("Installed spiced to {}", target_binary.display());

    // Symlink
    let symlink_path = dirs::home_dir()
        .context("Could not find home directory")?
        .join(".spice/bin/spiced-dev");

    // Remove existing symlink if it exists
    if symlink_path.exists() || fs::symlink_metadata(&symlink_path).is_ok() {
        fs::remove_file(&symlink_path).ok();
    }

    std::os::unix::fs::symlink(&target_binary, &symlink_path)?;

    println!(
        "Updated symlink: {} -> {}",
        symlink_path.display(),
        target_binary.display()
    );
    println!(
        "You can now run it comfortably via: {}",
        symlink_path.display()
    );

    Ok(())
}

#[cfg(unix)]
fn run_build(
    branch: Option<&str>,
    pr: Option<u64>,
    interactive: bool,
    args: &[String],
) -> Result<()> {
    let branch = if interactive {
        select_branch()?
    } else {
        resolve_branch_or_pr(branch, pr)?
    };
    validate_branch_name(&branch)?;

    let binary_path = dirs::home_dir()
        .context("Could not find home directory")?
        .join(".spice/bin")
        .join(&branch)
        .join("spiced");

    if !binary_path.exists() {
        println!(
            "Binary not found at {}. Installing...",
            binary_path.display()
        );
        install_build(Some(&branch), None)?;
    }

    println!("Running spiced from branch '{branch}'...");
    println!("Exec: {} {}", binary_path.display(), args.join(" "));

    // Replace the current process with spiced (Unix only)
    let error = Command::new(&binary_path).args(args).exec();

    // If we're here, exec failed
    anyhow::bail!("Failed to execute spiced: {error}");
}

#[cfg(unix)]
fn select_branch() -> Result<String> {
    let base_dir = dirs::home_dir()
        .context("Could not find home directory")?
        .join(".spice/bin");

    if !base_dir.exists() {
        anyhow::bail!("No binaries found in {}", base_dir.display());
    }

    let mut branches = Vec::new();

    // Walk directory to find all 'spiced' binaries
    for entry in WalkDir::new(&base_dir).min_depth(1) {
        let entry = entry.ok();
        if let Some(entry) = entry
            && entry.file_type().is_file()
            && entry.file_name() == "spiced"
        {
            // Found a spiced binary, the parent dir is the branch path
            if let Some(branch_dir) = entry.path().parent() {
                // Get relative path from base_dir to get the branch name
                if let Ok(rel_path) = branch_dir.strip_prefix(&base_dir)
                    && let Some(branch_name) = rel_path.to_str()
                    && !branch_name.is_empty()
                {
                    branches.push(branch_name.to_string());
                }
            }
        }
    }

    if branches.is_empty() {
        anyhow::bail!("No installed branches found in {}", base_dir.display());
    }

    branches.sort();

    let selection = dialoguer::Select::new()
        .with_prompt("Select a branch to run")
        .items(&branches)
        .default(0)
        .interact()
        .context("Failed to select branch")?;

    Ok(branches[selection].clone())
}
