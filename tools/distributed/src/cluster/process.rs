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

use anyhow::{Context, Result, anyhow};
use std::fs::{self, File};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;
use sysinfo::{ProcessRefreshKind, System};

#[cfg(unix)]
use nix::sys::signal::{self, Signal};
#[cfg(unix)]
use nix::unistd::Pid;

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
#[cfg(unix)]
pub fn is_process_alive(pid: u32) -> bool {
    let mut system = System::new();
    system.refresh_processes_specifics(
        sysinfo::ProcessesToUpdate::Some(&[sysinfo::Pid::from_u32(pid)]),
        true,
        ProcessRefreshKind::everything(),
    );
    system.process(sysinfo::Pid::from_u32(pid)).is_some()
}

#[cfg(not(unix))]
pub fn is_process_alive(_pid: u32) -> bool {
    false
}

/// Stop a process gracefully with SIGTERM, falling back to SIGKILL if timeout exceeded.
/// Note: This function uses blocking operations and should be called from `spawn_blocking` context
/// when used in async code.
#[cfg(unix)]
#[expect(clippy::cast_possible_wrap)]
pub fn stop_process(pid: u32, timeout_secs: u64) -> Result<()> {
    let nix_pid = Pid::from_raw(pid as i32);
    let spid = sysinfo::Pid::from_u32(pid);

    // Reuse a single System instance to avoid repeated allocation during the polling loop
    let mut system = System::new();
    system.refresh_processes_specifics(
        sysinfo::ProcessesToUpdate::Some(&[spid]),
        true,
        ProcessRefreshKind::everything(),
    );
    if system.process(spid).is_none() {
        return Ok(());
    }

    // Send SIGTERM
    signal::kill(nix_pid, Signal::SIGTERM).context("Failed to send SIGTERM")?;

    // Wait for process to terminate
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(timeout_secs) {
        system.refresh_processes_specifics(
            sysinfo::ProcessesToUpdate::Some(&[spid]),
            true,
            ProcessRefreshKind::everything(),
        );
        if system.process(spid).is_none() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    // Process didn't terminate, send SIGKILL
    signal::kill(nix_pid, Signal::SIGKILL).context("Failed to send SIGKILL")?;

    // Wait a bit more for SIGKILL to take effect
    std::thread::sleep(Duration::from_secs(1));

    system.refresh_processes_specifics(
        sysinfo::ProcessesToUpdate::Some(&[spid]),
        true,
        ProcessRefreshKind::everything(),
    );
    if system.process(spid).is_some() {
        return Err(anyhow!("Failed to kill process {nix_pid}"));
    }

    Ok(())
}

#[cfg(not(unix))]
pub fn stop_process(_pid: u32, _timeout_secs: u64) -> Result<()> {
    Err(anyhow!(
        "Process control is not supported on non-Unix platforms"
    ))
}

/// Force kill a process with SIGKILL.
#[cfg(unix)]
#[expect(clippy::cast_possible_wrap, clippy::cast_sign_loss)]
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

#[cfg(not(unix))]
pub fn kill_process(_pid: u32) -> Result<()> {
    Err(anyhow!(
        "Process control is not supported on non-Unix platforms"
    ))
}

/// Read the last N lines from a log file.
/// Uses a bounded tail implementation that seeks from the end of the file
/// to avoid reading the entire file into memory.
pub fn read_log_tail(log_file: &Path, lines: usize) -> Result<String> {
    read_log_tail_with_offset(log_file, lines).map(|(content, _offset)| content)
}

/// Read the last N lines from a log file and return the content along with
/// the byte offset at end-of-file (for follow mode).
pub fn read_log_tail_with_offset(log_file: &Path, lines: usize) -> Result<(String, u64)> {
    use std::io::{Read, Seek, SeekFrom};

    // Chunk size for reading from end of file — 8KB balances syscall count and memory.
    const CHUNK_SIZE: u64 = 8192;

    let mut file = fs::File::open(log_file).context("Failed to open log file")?;
    let file_len = file
        .metadata()
        .context("Failed to read log file metadata")?
        .len();

    if file_len == 0 || lines == 0 {
        return Ok((String::new(), file_len));
    }

    // Read chunks from the end of the file, scanning backwards for newlines.
    let mut remaining = file_len;
    let mut tail_bytes: Vec<u8> = Vec::new();
    // We need `lines` newlines to capture `lines` lines (the last line may not end with \n)
    let target_newlines = lines;

    while remaining > 0 {
        let read_size = remaining.min(CHUNK_SIZE);
        remaining -= read_size;

        file.seek(SeekFrom::Start(remaining))
            .context("Failed to seek in log file")?;

        let mut chunk = vec![0u8; read_size as usize];
        file.read_exact(&mut chunk)
            .context("Failed to read chunk from log file")?;

        // Prepend chunk to our accumulated bytes
        chunk.append(&mut tail_bytes);
        tail_bytes = chunk;

        // Count newlines in accumulated buffer
        #[expect(clippy::naive_bytecount)]
        let newline_count = tail_bytes.iter().filter(|&&b| b == b'\n').count();

        // If the buffer ends with \n, the last newline is a line terminator, not a separator,
        // so we need one extra newline to have `lines` complete lines.
        let ends_with_newline = tail_bytes.last() == Some(&b'\n');
        let needed = if ends_with_newline {
            target_newlines
        } else {
            // Last line has no trailing \n, so N newlines give us N+1 lines;
            // we only need target_newlines - 1 newlines for target_newlines lines.
            target_newlines.saturating_sub(1)
        };

        if newline_count >= needed {
            break;
        }
    }

    // Convert to string (lossy for safety) and take the last N lines
    let content = String::from_utf8_lossy(&tail_bytes);
    let all_lines: Vec<&str> = content.lines().collect();
    let start = all_lines.len().saturating_sub(lines);
    let result = all_lines[start..].join("\n");

    Ok((result, file_len))
}
