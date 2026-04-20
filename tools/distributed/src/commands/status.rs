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
use serde::Serialize;
use std::path::PathBuf;

use crate::cluster::{
    health::{HealthCheck, get_health_url},
    paths::expand_tilde,
    process,
    state::{load_state, state_exists},
};
use crate::output;

#[derive(Args)]
pub struct StatusArgs {
    /// Working directory
    #[arg(long, default_value = "~/.spice/distributed")]
    work_dir: PathBuf,

    /// Output status as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Serialize, Clone)]
struct NodeStatus {
    name: String,
    pid: u32,
    http_port: u16,
    status: String,
}

#[derive(Serialize)]
struct ClusterStatus {
    running: bool,
    scheduler: NodeStatus,
    executors: Vec<NodeStatus>,
}

pub async fn execute(args: StatusArgs) -> Result<()> {
    // Expand tilde in paths
    let work_dir = expand_tilde(&args.work_dir);

    // Check if cluster is running
    if !state_exists(&work_dir).await {
        if args.json {
            let status = serde_json::json!({
                "running": false,
                "message": "No cluster is currently running"
            });
            println!("{}", serde_json::to_string_pretty(&status)?);
        } else {
            output::info("No cluster is currently running.");
        }
        return Ok(());
    }

    // Load state
    let state = load_state(&work_dir)
        .await
        .context("Failed to load cluster state")?;

    // Check scheduler status
    let scheduler_status = check_node_status(
        &state.scheduler.name,
        state.scheduler.pid,
        state.scheduler.http_port,
    )
    .await;

    // Check executor statuses
    let mut executor_statuses = Vec::new();
    for executor in &state.executors {
        let status = check_node_status(&executor.name, executor.pid, executor.http_port).await;
        executor_statuses.push(status);
    }

    // Output
    if args.json {
        // Compute running status from node statuses: running if any node is healthy or running
        let running = scheduler_status.status != "stopped"
            || executor_statuses.iter().any(|s| s.status != "stopped");
        let cluster_status = ClusterStatus {
            running,
            scheduler: scheduler_status.clone(),
            executors: executor_statuses.clone(),
        };
        println!("{}", serde_json::to_string_pretty(&cluster_status)?);
    } else {
        println!("Cluster Status:");
        println!();
        println!("Scheduler:");
        print_node_status(&scheduler_status);
        println!();
        println!("Executors:");
        for executor_status in &executor_statuses {
            print_node_status(executor_status);
        }
    }

    Ok(())
}

async fn check_node_status(name: &str, pid: u32, http_port: u16) -> NodeStatus {
    let is_alive = process::is_process_alive(pid);

    let status = if is_alive {
        let health_check = HealthCheck::default();
        let health_url = get_health_url(http_port);
        match health_check.check_health(&health_url).await {
            Ok(true) => "healthy".to_string(),
            Ok(false) => "unhealthy".to_string(),
            Err(_) => "not responding".to_string(),
        }
    } else {
        "stopped".to_string()
    };

    NodeStatus {
        name: name.to_string(),
        pid,
        http_port,
        status,
    }
}

fn print_node_status(status: &NodeStatus) {
    let symbol = match status.status.as_str() {
        "healthy" => "✓",
        "stopped" => "✗",
        "unhealthy" | "not responding" => "⚠",
        _ => "?",
    };

    let colored_status = match status.status.as_str() {
        "healthy" => ansi_colors::Color::Green.paint(&status.status).to_string(),
        "stopped" => ansi_colors::Color::Red.paint(&status.status).to_string(),
        "unhealthy" | "not responding" => {
            ansi_colors::Color::Yellow.paint(&status.status).to_string()
        }
        _ => status.status.clone(),
    };

    println!(
        "  {} (port {}): {} {}",
        status.name, status.http_port, symbol, colored_status
    );
}
