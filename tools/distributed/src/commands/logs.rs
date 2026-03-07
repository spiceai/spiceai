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
use clap::Args;
use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use crate::cluster::{paths::expand_tilde, process, state::*};
use crate::output;

#[derive(Args)]
pub struct LogsArgs {
    /// Component to show logs for (scheduler, executor1, executor2, etc.)
    component: String,

    /// Log directory
    #[arg(long, default_value = "~/.spice/distributed/logs")]
    log_dir: PathBuf,

    /// Working directory (for state file)
    #[arg(long, default_value = "~/.spice/distributed")]
    work_dir: PathBuf,

    /// Follow log output (tail -f style)
    #[arg(short, long)]
    follow: bool,

    /// Number of lines to show from end
    #[arg(short = 'n', default_value = "50")]
    tail: usize,
}

pub async fn execute(args: LogsArgs) -> Result<()> {
    // Expand tilde in paths
    let work_dir = expand_tilde(&args.work_dir);
    let log_dir = expand_tilde(&args.log_dir);

    // Check if cluster is running
    if !state_exists(&work_dir) {
        output::error("No cluster is currently running.");
        output::info("Note: You can still view logs from the log directory if they exist.");

        // Try to show logs from log directory anyway
        let log_file = log_dir.join(format!("{}.log", args.component));
        if !log_file.exists() {
            output::error(&format!("Log file not found: {}", log_file.display()));
            return Err(anyhow::anyhow!("Log file not found"));
        }

        return show_logs_from_file(&log_file, args.follow, args.tail);
    }

    // Load state
    let state = load_state(&work_dir).context("Failed to load cluster state")?;

    // Get log file path from state
    let log_file = match state.get_log_path(&args.component) {
        Some(path) => path.clone(),
        None => {
            output::error(&format!("Component '{}' not found.", args.component));
            output::info("Available components:");
            for component in state.list_components() {
                println!("  - {component}");
            }
            return Err(anyhow::anyhow!("Component not found"));
        }
    };

    // Check if log file exists
    if !log_file.exists() {
        output::error(&format!("Log file not found: {}", log_file.display()));
        output::info(&format!(
            "The component '{}' may not have started successfully.",
            args.component
        ));
        return Err(anyhow::anyhow!("Log file not found"));
    }

    show_logs_from_file(&log_file, args.follow, args.tail)
}

fn show_logs_from_file(log_file: &PathBuf, follow: bool, tail_lines: usize) -> Result<()> {
    if follow {
        // Follow mode: print initial tail, then poll for new content
        follow_log_file(log_file, tail_lines)
    } else {
        // Read last N lines
        let tail =
            process::read_log_tail(log_file, tail_lines).context("Failed to read log file")?;
        println!("{tail}");
        Ok(())
    }
}

fn follow_log_file(log_file: &PathBuf, initial_lines: usize) -> Result<()> {
    // Print initial tail
    let tail =
        process::read_log_tail(log_file, initial_lines).context("Failed to read log file")?;
    println!("{tail}");

    // Follow for new content
    let mut last_size = fs::metadata(log_file)
        .context("Failed to get log file metadata")?
        .len();

    println!("\n--- Following (Ctrl+C to stop) ---\n");

    loop {
        std::thread::sleep(Duration::from_millis(500));

        let current_size = match fs::metadata(log_file) {
            Ok(metadata) => metadata.len(),
            Err(_) => {
                // File was deleted, stop following
                break;
            }
        };

        if current_size > last_size {
            // File grew, read new content
            match fs::read_to_string(log_file).context("Failed to read log file") {
                Ok(contents) => {
                    let lines: Vec<&str> = contents.lines().collect();
                    // Find where we left off based on approximate line count
                    if !lines.is_empty() {
                        // Estimate position based on file size ratio
                        let estimated_start =
                            (lines.len() as u64 * last_size / current_size).max(0) as usize;
                        for line in lines.iter().skip(estimated_start) {
                            println!("{line}");
                        }
                    }
                }
                Err(e) => {
                    output::warning(&format!("Error reading updated log: {e}"));
                }
            }
            last_size = current_size;
        }
    }

    Ok(())
}
