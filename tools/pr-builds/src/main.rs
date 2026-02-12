use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use flate2::read::GzDecoder;
use serde::Deserialize;
use std::env;
use std::fs;
use std::os::unix::fs::PermissionsExt;
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
    /// Trigger a build for the current (or specified) branch
    Trigger {
        /// Branch to trigger build for. Defaults to current branch.
        #[arg(short, long)]
        branch: Option<String>,
    },
    /// Install the latest binary for the current (or specified) branch
    Install {
        /// Branch to install binary for. Defaults to current branch.
        #[arg(short, long)]
        branch: Option<String>,
    },
    /// Run the binary for the current (or specified) branch
    Run {
        /// Branch to run binary for. Defaults to current branch.
        #[arg(short, long)]
        branch: Option<String>,

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

fn main() -> Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Trigger { branch } => trigger_build(branch.as_deref()),
        Commands::Install { branch } => install_build(branch.as_deref()),
        Commands::Run {
            branch,
            interactive,
            args,
        } => run_build(branch.as_deref(), *interactive, args),
    }
}

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

fn trigger_build(branch: Option<&str>) -> Result<()> {
    let branch = match branch {
        Some(b) => b.to_string(),
        None => get_current_branch()?,
    };

    println!("Triggering build for branch: {}...", branch);

    // Determine platform option based on current OS and Arch
    let platform_option = match env::consts::OS {
        "linux" => match env::consts::ARCH {
            "aarch64" => "Linux aarch64",
            _ => "Linux x64",
        },
        // Only Apple Silicon is supported for macOS in the CI matrix
        "macos" => "macOS aarch64 (Apple Silicon)",
        // Fallback for others
        _ => "Linux x64",
    };

    println!("Requesting platform: {}", platform_option);

    let status = Command::new("gh")
        .args([
            "workflow",
            "run",
            "build_and_release.yml",
            "--ref",
            &branch,
            "-f",
            &format!("platform_option={}", platform_option),
        ])
        .status()
        .context("Failed to execute gh workflow run")?;

    if status.success() {
        println!("Build triggered successfully.");
        println!("You can check the status with:");
        println!(
            "  gh run list --workflow build_and_release.yml --branch \"{}\"",
            branch
        );
    } else {
        anyhow::bail!("Failed to trigger build");
    }

    Ok(())
}

fn get_artifact_names() -> Vec<String> {
    let (os, arch) = match env::consts::OS {
        // CI only builds aarch64 for macOS (Apple Silicon)
        "macos" => ("darwin", "aarch64"),
        // For Linux, the CI uses standard architecture names (x86_64, aarch64)
        "linux" => ("linux", env::consts::ARCH),
        // Fallback/Default
        _ => ("linux", "x86_64"),
    };

    // Prioritize metal for darwin
    let mut names = Vec::new();
    if os == "darwin" {
        names.push(format!("spiced_metal_{}_{}", os, arch));
    }
    names.push(format!("spiced_{}_{}", os, arch));

    names
}

fn install_build(branch: Option<&str>) -> Result<()> {
    let branch = match branch {
        Some(b) => b.to_string(),
        None => get_current_branch()?,
    };

    let target_dir = dirs::home_dir()
        .context("Could not find home directory")?
        .join(".spice/bin")
        .join(&branch);

    println!("Looking for latest successful build for branch: {}...", branch);

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
    let run = runs.first().context(format!(
        "No successful build found for branch '{}'",
        branch
    ))?;
    let run_id = run.database_id;

    println!("Found Run ID: {}", run_id);

    let temp_dir = tempfile::tempdir()?;
    let temp_path = temp_dir.path();

    println!("Downloading artifact to {}...", temp_path.display());

    let artifacts = get_artifact_names();
    let mut downloaded = false;

    for artifact_name in &artifacts {
        println!("Attempting to download artifact: {}", artifact_name);
        // Using temp_path to convert to string safely
        let temp_path_str = temp_path
            .to_str()
            .context("Temp path contains invalid UTF-8")?;

        let status = Command::new("gh")
            .args([
                "run",
                "download",
                &run_id.to_string(),
                "-n",
                artifact_name,
                "-D",
                temp_path_str,
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .status()
            .context("Failed to execute gh run download")?;

        if status.success() {
            downloaded = true;
            break;
        }
    }

    if !downloaded {
        anyhow::bail!(
            "Failed to download any compatible artifacts. Tried: {:?}",
            artifacts
        );
    }

    println!("Installing to {}...", target_dir.display());
    fs::create_dir_all(&target_dir)?;

    // Find tar.gz
    let mut tar_file: Option<PathBuf> = None;
    for entry in fs::read_dir(temp_path)? {
        let entry = entry?;
        let path = entry.path();
        // Check for extension safely
        if let Some(ext) = path.extension() {
            if ext == "gz" {
                tar_file = Some(path);
                break;
            }
        }
    }

    let tar_file = tar_file.context("No tar.gz file found in downloaded artifact")?;

    // Extract
    let tar_gz = fs::File::open(&tar_file)?;
    let tar = GzDecoder::new(tar_gz);
    let mut archive = Archive::new(tar);
    archive.unpack(temp_path)?;

    // Find binary
    let spiced_path = temp_path.join("spiced");
    if !spiced_path.exists() {
        anyhow::bail!(
            "Could not find 'spiced' binary in extracted archive at {}",
            spiced_path.display()
        );
    }

    let target_binary = target_dir.join("spiced");
    // Rename might fail if cross-device link, but unlikely for temp to home usually on same mount or different.
    // fs::rename is atomic but platform specific. If it fails, we might need copy+delete.
    // For now, assuming fs::rename works or fails loudly.
    if target_binary.exists() {
        fs::remove_file(&target_binary)?;
    }
    fs::rename(&spiced_path, &target_binary)?;

    // Make executable
    let mut perms = fs::metadata(&target_binary)?.permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&target_binary, perms)?;

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
    println!("You can now run it comfortably via: {}", symlink_path.display());

    Ok(())
}

fn run_build(branch: Option<&str>, interactive: bool, args: &[String]) -> Result<()> {
    let branch = if interactive {
        select_branch()?
    } else {
        match branch {
            Some(b) => b.to_string(),
            None => get_current_branch()?,
        }
    };

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
        install_build(Some(&branch))?;
    }

    println!("Running spiced from branch '{}'...", branch);
    println!("Exec: {} {}", binary_path.display(), args.join(" "));

    // Replace the current process with spiced (Unix only)
    use std::os::unix::process::CommandExt;
    let error = Command::new(&binary_path).args(args).exec();

    // If we're here, exec failed
    anyhow::bail!("Failed to execute spiced: {}", error);
}

fn select_branch() -> Result<String> {
    let base_dir = dirs::home_dir()
        .context("Could not find home directory")?
        .join(".spice/bin");

    if !base_dir.exists() {
        anyhow::bail!("No binaries found in {}", base_dir.display());
    }

    let mut branches = Vec::new();

    // Walk directory to find all 'spiced' binaries
    for entry in WalkDir::new(&base_dir).min_depth(1).max_depth(5) {
        let entry = entry.ok();
        if let Some(entry) = entry {
            if entry.file_type().is_file() && entry.file_name() == "spiced" {
                // Found a spiced binary, the parent dir is the branch path
                if let Some(branch_dir) = entry.path().parent() {
                    // Get relative path from base_dir to get the branch name
                    if let Ok(rel_path) = branch_dir.strip_prefix(&base_dir) {
                        if let Some(branch_name) = rel_path.to_str() {
                            if !branch_name.is_empty() {
                                branches.push(branch_name.to_string());
                            }
                        }
                    }
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
