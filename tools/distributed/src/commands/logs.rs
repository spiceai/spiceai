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
use std::path::PathBuf;
use std::time::Duration;
use tokio::fs as tokio_fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::cluster::{
    paths::expand_tilde,
    process,
    state::{load_state, state_exists},
};
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
    if !state_exists(&work_dir).await {
        output::error("No cluster is currently running.");
        output::info("Note: You can still view logs from the log directory if they exist.");

        // Try to show logs from log directory anyway
        let log_file = log_dir.join(format!("{}.log", args.component));
        if !log_file.exists() {
            output::error(&format!("Log file not found: {}", log_file.display()));
            return Err(anyhow::anyhow!("Log file not found"));
        }

        return show_logs_from_file(&log_file, args.follow, args.tail).await;
    }

    // Load state
    let state = load_state(&work_dir)
        .await
        .context("Failed to load cluster state")?;

    // Get log file path from state
    let log_file = if let Some(path) = state.get_log_path(&args.component) {
        path.clone()
    } else {
        output::error(&format!("Component '{}' not found.", args.component));
        output::info("Available components:");
        for component in state.list_components() {
            println!("  - {component}");
        }
        return Err(anyhow::anyhow!("Component not found"));
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

    show_logs_from_file(&log_file, args.follow, args.tail).await
}

async fn show_logs_from_file(log_file: &PathBuf, follow: bool, tail_lines: usize) -> Result<()> {
    if follow {
        // Follow mode: print initial tail, then poll for new content
        follow_log_file(log_file, tail_lines).await
    } else {
        // Read last N lines (offload blocking I/O to a blocking thread)
        let log_path = log_file.clone();
        let tail =
            tokio::task::spawn_blocking(move || process::read_log_tail(&log_path, tail_lines))
                .await
                .context("Failed to join log tail reader task")?
                .context("Failed to read log file")?;
        println!("{tail}");
        Ok(())
    }
}

async fn follow_log_file(log_file: &PathBuf, initial_lines: usize) -> Result<()> {
    // Print initial tail and get the actual file byte offset
    let log_path = log_file.clone();
    let (tail, file_offset) = tokio::task::spawn_blocking(move || {
        process::read_log_tail_with_offset(&log_path, initial_lines)
    })
    .await
    .context("Failed to join log tail reader task")?
    .context("Failed to read log file")?;

    println!("{tail}");

    // Track byte offset from the actual end-of-file position
    let mut byte_offset = file_offset;

    println!("\n--- Following (Ctrl+C to stop) ---\n");

    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;

        match tokio_fs::File::open(log_file).await {
            Ok(mut file) => {
                // Seek to the last known position
                match file.seek(std::io::SeekFrom::Start(byte_offset)).await {
                    Ok(_) => {
                        // Read new content
                        let mut buffer = Vec::new();
                        match file.read_to_end(&mut buffer).await {
                            Ok(bytes_read) => {
                                if bytes_read > 0 {
                                    // Convert new bytes to string (lossy) and print line by line;
                                    // always advance offset even if bytes are not valid UTF-8
                                    let new_content = String::from_utf8_lossy(&buffer);
                                    for line in new_content.lines() {
                                        println!("{line}");
                                    }
                                    byte_offset += bytes_read as u64;
                                }
                            }
                            Err(e) => {
                                output::warning(&format!("Error reading log file: {e}"));
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        output::warning(&format!("Error seeking in log file: {e}"));
                        break;
                    }
                }
            }
            Err(e) => {
                // File deleted or permission denied - stop following
                if e.kind() != std::io::ErrorKind::NotFound {
                    output::warning(&format!("Error reading log file: {e}"));
                }
                break;
            }
        }
    }

    Ok(())
}
