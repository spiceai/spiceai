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
    state::{ClusterState, NodeState, load_state, remove_state, save_state, state_exists},
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
    if state_exists(&work_dir).await {
        // Validate that recorded processes are actually alive before refusing to start
        if let Ok(state) = load_state(&work_dir).await {
            let any_alive = process::is_process_alive(state.scheduler.pid)
                || state
                    .executors
                    .iter()
                    .any(|e| process::is_process_alive(e.pid));

            if any_alive {
                output::error(
                    "Cluster is already running. Use 'distributed stop' to stop it first.",
                );
                return Err(anyhow::anyhow!("Cluster already running"));
            }
            // All recorded processes are dead — stale state file from a crash
            output::warning(
                "Found stale cluster state (all recorded processes are dead). Cleaning up...",
            );
            remove_state(&work_dir)
                .await
                .context("Failed to remove stale cluster state")?;
        } else {
            // Corrupt state file — remove it and proceed
            output::warning("Found corrupt cluster state file. Cleaning up...");
            remove_state(&work_dir)
                .await
                .context("Failed to remove corrupt cluster state")?;
        }
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
        let num_executors = config.num_executors;
        tokio::task::spawn_blocking(move || tls::validate_tls_files(num_executors))
            .await
            .context("TLS validation task panicked or was cancelled")?
            .context(
                "TLS certificates not found. Please run TLS initialization or use 'spice cluster tls' commands",
            )?;
    } else {
        output::info("Initializing TLS certificates...");

        // Precompute node names in the async context (cheap, non-blocking)
        let mut node_names: Vec<String> = vec!["scheduler1".to_string()];
        let executor_names: Vec<String> = (0..config.num_executors)
            .map(|i| config.executor_name(i))
            .collect();
        node_names.extend(executor_names);

        // Run TLS initialization and certificate generation in a blocking task to avoid
        // blocking the Tokio runtime thread with process::Command invocations.
        tokio::task::spawn_blocking(move || -> Result<()> {
            tls::ensure_tls_initialized().context("Failed to initialize TLS")?;

            let node_name_refs: Vec<&str> = node_names.iter().map(String::as_str).collect();
            tls::ensure_certificates(&node_name_refs).context("Failed to generate certificates")?;

            Ok(())
        })
        .await
        .context("TLS initialization task panicked or was cancelled")??;
    }

    // Start scheduler (blocking filesystem + process spawn — run off the async runtime)
    output::info(&format!(
        "Starting scheduler on port {}...",
        config.scheduler.http_port
    ));
    let config_clone = config.clone();
    let scheduler_node =
        tokio::task::spawn_blocking(move || process::start_scheduler(&config_clone))
            .await
            .context("Scheduler start task panicked or was cancelled")?
            .context("Failed to start scheduler")?;

    // Wait for scheduler health check
    if !config.skip_health_check {
        let health_check = HealthCheck::default();
        let health_url = get_health_url(config.scheduler.http_port);
        match health_check.wait_for_ready(&health_url).await {
            Ok(()) => output::success("Scheduler is ready"),
            Err(e) => {
                output::error(&format!("Scheduler health check failed: {e}"));
                output::info("Last 10 lines of scheduler log:");
                let log_file = scheduler_node.log_file.clone();
                if let Ok(Ok(tail)) =
                    tokio::task::spawn_blocking(move || process::read_log_tail(&log_file, 10)).await
                {
                    println!("{tail}");
                }
                // Clean up scheduler process
                let pid = scheduler_node.pid;
                let _ = tokio::task::spawn_blocking(move || process::kill_process(pid)).await;
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
        let executor_name = name.clone();
        let config_clone = config.clone();
        let scheduler_addr_clone = scheduler_addr.clone();
        let executor_node = tokio::task::spawn_blocking(move || {
            process::start_executor(
                &executor_name,
                http_port,
                node_port,
                &config_clone,
                &scheduler_addr_clone,
            )
        })
        .await
        .context(format!("Start task for {name} panicked or was cancelled"))?
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
                    let log_file = executor_node.log_file.clone();
                    if let Ok(Ok(tail)) =
                        tokio::task::spawn_blocking(move || process::read_log_tail(&log_file, 10))
                            .await
                    {
                        println!("{tail}");
                    }
                    // Clean up all processes
                    let executor_pid = executor_node.pid;
                    let _ =
                        tokio::task::spawn_blocking(move || process::kill_process(executor_pid))
                            .await;
                    for node in &executor_nodes {
                        let pid = node.pid;
                        let _ =
                            tokio::task::spawn_blocking(move || process::kill_process(pid)).await;
                    }
                    let sched_pid = scheduler_node.pid;
                    let _ =
                        tokio::task::spawn_blocking(move || process::kill_process(sched_pid)).await;
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
