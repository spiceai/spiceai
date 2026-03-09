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

use crate::cluster::{
    config::ClusterConfig,
    health::{HealthCheck, get_health_url},
    paths::expand_tilde,
    process,
    state::{ClusterState, NodeState, remove_state, save_state, state_exists},
    tls,
};
use crate::output;

#[derive(Args)]
pub struct StartArgs {
    /// Number of executors
    #[arg(short, long, default_value = "3")]
    executors: usize,

    /// Scheduler HTTP port
    #[arg(short = 's', long, default_value = "8090")]
    scheduler_http: u16,

    /// Scheduler Flight port
    #[arg(long, default_value = "50051")]
    scheduler_flight: u16,

    /// Scheduler node port
    #[arg(long, default_value = "50052")]
    scheduler_node: u16,

    /// Base executor HTTP port
    #[arg(long, default_value = "9090")]
    executor_http: u16,

    /// Base executor node port
    #[arg(long, default_value = "50062")]
    executor_node: u16,

    /// Log directory
    #[arg(long, default_value = "~/.spice/distributed/logs")]
    log_dir: PathBuf,

    /// Working directory
    #[arg(long, default_value = "~/.spice/distributed")]
    work_dir: PathBuf,

    /// Project directory
    #[arg(long, default_value = ".")]
    project_dir: PathBuf,

    /// Path to spiced binary
    #[arg(long)]
    spiced_path: Option<PathBuf>,

    /// Skip automatic TLS initialization
    #[arg(long)]
    no_tls_init: bool,

    /// Skip health checks after startup
    #[arg(long)]
    no_health_check: bool,

    /// Start and exit (don't wait for Ctrl+C)
    #[arg(short, long)]
    detach: bool,
}

pub async fn execute(args: StartArgs) -> Result<()> {
    // Expand tilde in paths
    let work_dir = expand_tilde(&args.work_dir);
    let log_dir = expand_tilde(&args.log_dir);
    let project_dir = expand_tilde(&args.project_dir);

    // Check if cluster already running
    if state_exists(&work_dir) {
        output::error("Cluster is already running. Use 'distributed stop' to stop it first.");
        return Err(anyhow::anyhow!("Cluster already running"));
    }

    // Build configuration
    #[expect(clippy::field_reassign_with_default)]
    let config = {
        let mut cfg = ClusterConfig::default();
        cfg.num_executors = args.executors;
        cfg.scheduler.http_port = args.scheduler_http;
        cfg.scheduler.flight_port = args.scheduler_flight;
        cfg.scheduler.node_port = args.scheduler_node;
        cfg.executors.base_http_port = args.executor_http;
        cfg.executors.base_node_port = args.executor_node;
        cfg.paths.log_dir = log_dir;
        cfg.paths.work_dir = work_dir;
        cfg.paths.project_dir = project_dir;
        if let Some(spiced_path) = args.spiced_path {
            cfg.paths.spiced_path = expand_tilde(&spiced_path);
        }
        cfg.detach = args.detach;
        cfg.skip_tls_init = args.no_tls_init;
        cfg.skip_health_check = args.no_health_check;
        cfg
    };

    // Validate spiced binary exists
    if !config.paths.spiced_path.exists() {
        output::error(&format!(
            "spiced binary not found at: {}",
            config.paths.spiced_path.display()
        ));
        output::info("Please install Spice.ai or specify the correct path with --spiced-path");
        return Err(anyhow::anyhow!("spiced binary not found"));
    }

    output::info("Starting distributed Spice cluster...");

    // Initialize TLS if needed
    if config.skip_tls_init {
        // When skipping TLS initialization, validate that required files already exist
        output::info("Validating existing TLS certificates...");
        tls::validate_tls_files(config.num_executors).context(
            "TLS certificates not found. Please run TLS initialization or use 'spice cluster tls' commands",
        )?;
    } else {
        output::info("Initializing TLS certificates...");
        tls::ensure_tls_initialized().context("Failed to initialize TLS")?;

        // Generate certificates for scheduler and executors
        // Note: scheduler uses "scheduler1" as the cert name to match bash script convention
        let mut node_names = vec!["scheduler1"];
        let executor_names: Vec<String> = (0..config.num_executors)
            .map(|i| config.executor_name(i))
            .collect();
        let executor_refs: Vec<&str> = executor_names.iter().map(String::as_str).collect();
        node_names.extend(&executor_refs);

        tls::ensure_certificates(&node_names).context("Failed to generate certificates")?;
    }

    // Start scheduler
    output::info(&format!(
        "Starting scheduler on port {}...",
        config.scheduler.http_port
    ));
    let scheduler_node = process::start_scheduler(&config).context("Failed to start scheduler")?;

    // Wait for scheduler health check
    if !config.skip_health_check {
        let health_check = HealthCheck::default();
        let health_url = get_health_url(config.scheduler.http_port);
        match health_check.wait_for_ready(&health_url).await {
            Ok(()) => output::success("Scheduler is ready"),
            Err(e) => {
                output::error(&format!("Scheduler health check failed: {e}"));
                output::info("Last 10 lines of scheduler log:");
                if let Ok(tail) = process::read_log_tail(&scheduler_node.log_file, 10) {
                    println!("{tail}");
                }
                // Clean up scheduler process
                let _ = process::kill_process(scheduler_node.pid);
                return Err(e);
            }
        }
    }

    // Start executors
    let scheduler_addr = format!("127.0.0.1:{}", config.scheduler.node_port);
    let mut executor_nodes: Vec<NodeState> = Vec::new();

    for i in 0..config.num_executors {
        let name = config.executor_name(i);
        let http_port = config.executor_http_port(i);
        let node_port = config.executor_node_port(i);

        output::info(&format!("Starting {name} on port {http_port}..."));
        let executor_node =
            process::start_executor(&name, http_port, node_port, &config, &scheduler_addr)
                .context(format!("Failed to start {name}"))?;

        // Wait for executor health check
        if !config.skip_health_check {
            let health_check = HealthCheck::default();
            let health_url = get_health_url(http_port);
            match health_check.wait_for_ready(&health_url).await {
                Ok(()) => output::success(&format!("{name} is ready")),
                Err(e) => {
                    output::error(&format!("{name} health check failed: {e}"));
                    output::info(&format!("Last 10 lines of {name} log:"));
                    if let Ok(tail) = process::read_log_tail(&executor_node.log_file, 10) {
                        println!("{tail}");
                    }
                    // Clean up all processes
                    let _ = process::kill_process(executor_node.pid);
                    for node in &executor_nodes {
                        let _ = process::kill_process(node.pid);
                    }
                    let _ = process::kill_process(scheduler_node.pid);
                    return Err(e);
                }
            }
        }

        executor_nodes.push(executor_node);

        // Delay between starting executors
        if i < config.num_executors - 1 {
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    // Save state
    let state = ClusterState::new(
        config.paths.project_dir.clone(),
        scheduler_node.clone(),
        executor_nodes.clone(),
    );
    save_state(&state, &config.paths.work_dir)
        .await
        .context("Failed to save cluster state")?;

    // Print status
    println!();
    output::success("Cluster started successfully!");
    println!();
    println!("Scheduler:");
    println!(
        "  scheduler (port {}): PID {}",
        scheduler_node.http_port, scheduler_node.pid
    );
    println!();
    println!("Executors:");
    for node in &executor_nodes {
        println!(
            "  {} (port {}): PID {}",
            node.name, node.http_port, node.pid
        );
    }
    println!();
    output::info(&format!(
        "Logs directory: {}",
        config.paths.log_dir.display()
    ));
    println!();

    // Handle detach vs background mode
    if config.detach {
        output::info("Cluster is running in detached mode.");
        output::info("Use 'distributed status' to check cluster health.");
        output::info("Use 'distributed stop' to stop the cluster.");
    } else {
        output::info("Press Ctrl+C to stop the cluster...");
        wait_for_shutdown(&state).await?;
    }

    Ok(())
}

async fn wait_for_shutdown(state: &ClusterState) -> Result<()> {
    // Set up signal handler
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            println!();
            output::info("Received Ctrl+C, shutting down cluster...");
        }
    }

    // Stop all processes in background tasks to avoid blocking the runtime
    for executor in &state.executors {
        output::info(&format!("Stopping {}...", executor.name));
        let name = executor.name.clone();
        let pid = executor.pid;
        let stop_result = tokio::task::spawn_blocking(move || process::stop_process(pid, 10)).await;
        match stop_result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                output::warning(&format!("Failed to stop {name}: {e}"));
            }
            Err(e) => {
                output::warning(&format!("Failed to stop {name}: shutdown task failed: {e}"));
            }
        }
    }

    output::info("Stopping scheduler...");
    let scheduler_pid = state.scheduler.pid;
    let scheduler_stop_result =
        tokio::task::spawn_blocking(move || process::stop_process(scheduler_pid, 10)).await;
    match scheduler_stop_result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            output::warning(&format!("Failed to stop scheduler: {e}"));
        }
        Err(e) => {
            output::warning(&format!(
                "Failed to stop scheduler: shutdown task failed: {e}"
            ));
        }
    }

    // Remove state file
    if let Some(parent) = state.scheduler.work_dir.parent() {
        if let Err(e) = remove_state(parent).await {
            output::warning(&format!("Failed to remove state file: {e}"));
        }
    } else {
        output::warning("Failed to remove state file: scheduler work_dir has no parent directory");
    }

    output::success("Cluster stopped successfully!");
    Ok(())
}
