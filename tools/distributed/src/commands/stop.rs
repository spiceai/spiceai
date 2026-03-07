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

use crate::cluster::{paths::expand_tilde, process, state::*};
use crate::output;

#[derive(Args)]
pub struct StopArgs {
    /// Working directory
    #[arg(long, default_value = "~/.spice/distributed")]
    work_dir: PathBuf,

    /// Force kill processes if graceful shutdown fails
    #[arg(long)]
    force: bool,

    /// Timeout for graceful shutdown (seconds)
    #[arg(long, default_value = "10")]
    timeout: u64,
}

pub async fn execute(args: StopArgs) -> Result<()> {
    // Expand tilde in paths
    let work_dir = expand_tilde(&args.work_dir);

    // Check if cluster is running
    if !state_exists(&work_dir) {
        output::error("No cluster is currently running.");
        return Err(anyhow::anyhow!("Cluster not running"));
    }

    // Load state
    let state = load_state(&work_dir).context("Failed to load cluster state")?;

    output::info("Stopping distributed Spice cluster...");

    // Stop executors first (sequentially)
    let mut executor_errors = Vec::new();
    for executor in &state.executors {
        output::info(&format!("Stopping {}...", executor.name));
        let result = if args.force {
            process::kill_process(executor.pid)
        } else {
            process::stop_process(executor.pid, args.timeout)
        };

        match result {
            Ok(()) => output::success(&format!("{} stopped", executor.name)),
            Err(e) => {
                output::warning(&format!("Failed to stop {}: {e}", executor.name));
                executor_errors.push((executor.name.clone(), e));
            }
        }
    }

    // Stop scheduler
    output::info("Stopping scheduler...");
    let scheduler_result = if args.force {
        process::kill_process(state.scheduler.pid)
    } else {
        process::stop_process(state.scheduler.pid, args.timeout)
    };

    let scheduler_failed = scheduler_result.is_err();
    match scheduler_result {
        Ok(()) => output::success("Scheduler stopped"),
        Err(e) => {
            output::warning(&format!("Failed to stop scheduler: {e}"));
        }
    }

    // Report results and remove state file only if all components stopped successfully
    let has_errors = !executor_errors.is_empty() || scheduler_failed;
    if !has_errors {
        remove_state(&work_dir).context("Failed to remove cluster state")?;
        output::success("Cluster stopped successfully!");
        Ok(())
    } else {
        output::warning("Cluster stopped with some errors. Check the output above for details.");
        output::info(
            "State file retained for inspection; use 'distributed stop --force' to force cleanup.",
        );
        Err(anyhow::anyhow!(
            "Failed to stop all cluster processes; some components may still be running"
        ))
    }
}
