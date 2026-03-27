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

use crate::cluster::{
    paths::expand_tilde,
    process,
    state::{load_state, remove_state, state_exists},
};
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
    if !state_exists(&work_dir).await {
        output::error("No cluster is currently running.");
        return Err(anyhow::anyhow!("Cluster not running"));
    }

    // Load state
    let state = load_state(&work_dir)
        .await
        .context("Failed to load cluster state")?;

    output::info("Stopping distributed Spice cluster...");

    // Stop executors first (sequentially, in blocking tasks)
    let mut has_executor_errors = false;
    for executor in &state.executors {
        output::info(&format!("Stopping {}...", executor.name));
        let name = executor.name.clone();
        let pid = executor.pid;
        let timeout = args.timeout;
        let force = args.force;

        let result = tokio::task::spawn_blocking(move || {
            if force {
                process::kill_process(pid)
            } else {
                process::stop_process(pid, timeout)
            }
        })
        .await;

        match result {
            Ok(Ok(())) => output::success(&format!("{name} stopped")),
            Ok(Err(e)) => {
                output::warning(&format!("Failed to stop {name}: {e}"));
                has_executor_errors = true;
            }
            Err(e) => {
                output::warning(&format!("Failed to stop {name}: task failed: {e}"));
                has_executor_errors = true;
            }
        }
    }

    // Stop scheduler
    output::info("Stopping scheduler...");
    let scheduler_pid = state.scheduler.pid;
    let scheduler_timeout = args.timeout;
    let scheduler_force = args.force;

    let scheduler_result = tokio::task::spawn_blocking(move || {
        if scheduler_force {
            process::kill_process(scheduler_pid)
        } else {
            process::stop_process(scheduler_pid, scheduler_timeout)
        }
    })
    .await;

    let mut scheduler_failed = false;
    match scheduler_result {
        Ok(Ok(())) => output::success("Scheduler stopped"),
        Ok(Err(e)) => {
            output::warning(&format!("Failed to stop scheduler: {e}"));
            scheduler_failed = true;
        }
        Err(e) => {
            output::warning(&format!("Failed to stop scheduler: task failed: {e}"));
            scheduler_failed = true;
        }
    }

    // Report results and remove state file only if all components stopped successfully
    if !has_executor_errors && !scheduler_failed {
        remove_state(&work_dir)
            .await
            .context("Failed to remove cluster state")?;
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
