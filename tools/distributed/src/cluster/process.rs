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

use anyhow::{anyhow, Context, Result};
use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use std::fs::{self, File};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;
use sysinfo::{ProcessRefreshKind, System};

use super::config::ClusterConfig;
use super::state::NodeState;

/// Start the scheduler process.
pub fn start_scheduler(config: &ClusterConfig) -> Result<NodeState> {
    let work_dir = config.paths.work_dir.join("scheduler");
    let log_file = config.paths.log_dir.join("scheduler.log");

    fs::create_dir_all(&work_dir).context("Failed to create scheduler working directory")?;
    fs::create_dir_all(&config.paths.log_dir).context("Failed to create log directory")?;

    let log_handle = File::create(&log_file).context("Failed to create scheduler log file")?;

    let pki_dir = dirs::home_dir()
        .context("Failed to get home directory")?
        .join(".spice/pki");

    // Convert project_dir to absolute path before changing working directory
    let project_dir = std::env::current_dir()
        .context("Failed to get current directory")?
        .join(&config.paths.project_dir)
        .canonicalize()
        .context("Failed to canonicalize project directory")?;

    let mut cmd = Command::new(&config.paths.spiced_path);
    cmd.arg("--role")
        .arg("scheduler")
        .arg("--http")
        .arg(format!("127.0.0.1:{}", config.scheduler.http_port))
        .arg("--flight")
        .arg(format!("127.0.0.1:{}", config.scheduler.flight_port))
        .arg("--node-bind-address")
        .arg(format!("127.0.0.1:{}", config.scheduler.node_port))
        .arg("--node-advertise-address")
        .arg(format!("127.0.0.1:{}", config.scheduler.node_port))
        .arg("--node-mtls-ca-certificate-file")
        .arg(pki_dir.join("ca.crt"))
        .arg("--node-mtls-certificate-file")
        .arg(pki_dir.join("scheduler1.crt"))
        .arg("--node-mtls-key-file")
        .arg(pki_dir.join("scheduler1.key"))
        .arg(&project_dir)
        .current_dir(&work_dir)
        .stdout(Stdio::from(log_handle.try_clone()?))
        .stderr(Stdio::from(log_handle));

    let child = cmd.spawn().context("Failed to spawn scheduler process")?;

    Ok(NodeState {
        name: "scheduler".to_string(),
        pid: child.id(),
        http_port: config.scheduler.http_port,
        flight_port: Some(config.scheduler.flight_port),
        node_port: config.scheduler.node_port,
        work_dir,
        log_file,
    })
}

/// Start an executor process.
pub fn start_executor(
    name: &str,
    http_port: u16,
    node_port: u16,
    config: &ClusterConfig,
    scheduler_addr: &str,
) -> Result<NodeState> {
    let work_dir = config.paths.work_dir.join(name);
    let log_file = config.paths.log_dir.join(format!("{name}.log"));

    fs::create_dir_all(&work_dir).context("Failed to create executor working directory")?;

    let log_handle = File::create(&log_file).context("Failed to create executor log file")?;

    let pki_dir = dirs::home_dir()
        .context("Failed to get home directory")?
        .join(".spice/pki");

    // Convert project_dir to absolute path before changing working directory
    let project_dir = std::env::current_dir()
        .context("Failed to get current directory")?
        .join(&config.paths.project_dir)
        .canonicalize()
        .context("Failed to canonicalize project directory")?;

    let mut cmd = Command::new(&config.paths.spiced_path);
    cmd.arg("--role")
        .arg("executor")
        .arg("--http")
        .arg(format!("127.0.0.1:{http_port}"))
        .arg("--scheduler-address")
        .arg(scheduler_addr)
        .arg("--node-bind-address")
        .arg(format!("127.0.0.1:{node_port}"))
        .arg("--node-advertise-address")
        .arg(format!("127.0.0.1:{node_port}"))
        .arg("--node-mtls-ca-certificate-file")
        .arg(pki_dir.join("ca.crt"))
        .arg("--node-mtls-certificate-file")
        .arg(pki_dir.join(format!("{name}.crt")))
        .arg("--node-mtls-key-file")
        .arg(pki_dir.join(format!("{name}.key")))
        .arg(&project_dir)
        .current_dir(&work_dir)
        .stdout(Stdio::from(log_handle.try_clone()?))
        .stderr(Stdio::from(log_handle));

    let child = cmd.spawn().context("Failed to spawn executor process")?;

    Ok(NodeState {
        name: name.to_string(),
        pid: child.id(),
        http_port,
        flight_port: None,
        node_port,
        work_dir,
        log_file,
    })
}

/// Check if a process is alive using sysinfo.
pub fn is_process_alive(pid: u32) -> bool {
    let mut system = System::new();
    system.refresh_processes_specifics(
        sysinfo::ProcessesToUpdate::Some(&[sysinfo::Pid::from_u32(pid)]),
        true,
        ProcessRefreshKind::everything(),
    );
    system.process(sysinfo::Pid::from_u32(pid)).is_some()
}

/// Stop a process gracefully with SIGTERM, falling back to SIGKILL if timeout exceeded.
pub fn stop_process(pid: u32, timeout_secs: u64) -> Result<()> {
    let pid = Pid::from_raw(pid as i32);

    // Check if process is alive
    if !is_process_alive(pid.as_raw() as u32) {
        return Ok(());
    }

    // Send SIGTERM
    signal::kill(pid, Signal::SIGTERM).context("Failed to send SIGTERM")?;

    // Wait for process to terminate
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(timeout_secs) {
        if !is_process_alive(pid.as_raw() as u32) {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    // Process didn't terminate, send SIGKILL
    signal::kill(pid, Signal::SIGKILL).context("Failed to send SIGKILL")?;

    // Wait a bit more for SIGKILL to take effect
    std::thread::sleep(Duration::from_secs(1));

    if is_process_alive(pid.as_raw() as u32) {
        return Err(anyhow!("Failed to kill process {pid}"));
    }

    Ok(())
}

/// Force kill a process with SIGKILL.
pub fn kill_process(pid: u32) -> Result<()> {
    let pid = Pid::from_raw(pid as i32);

    if !is_process_alive(pid.as_raw() as u32) {
        return Ok(());
    }

    signal::kill(pid, Signal::SIGKILL).context("Failed to send SIGKILL")?;

    std::thread::sleep(Duration::from_secs(1));

    if is_process_alive(pid.as_raw() as u32) {
        return Err(anyhow!("Failed to kill process {pid}"));
    }

    Ok(())
}

/// Read the last N lines from a log file.
pub fn read_log_tail(log_file: &Path, lines: usize) -> Result<String> {
    let contents = fs::read_to_string(log_file).context("Failed to read log file")?;
    let all_lines: Vec<&str> = contents.lines().collect();
    let start_index = all_lines.len().saturating_sub(lines);
    Ok(all_lines[start_index..].join("\n"))
}
