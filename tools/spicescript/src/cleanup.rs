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

//! What this runner started and has not yet torn down: source containers and
//! `spiced` processes. A script removes them as it ends; this is for the ways a
//! run can end early. A container dropped while still running (a failed
//! start, a panic, a cancelled future) is removed in its `Drop`, and SIGINT or
//! SIGTERM removes everything still here before the runner exits. Only SIGKILL
//! escapes both, and the next runner's reaper (`reap.rs`) covers that.
//!
//! The removals here use the `docker` CLI synchronously, since they run in
//! `Drop` and after a signal, where no async runtime can be relied on.

use std::{
    collections::HashMap,
    process::Command,
    sync::{
        LazyLock,
        atomic::{AtomicBool, Ordering},
    },
};

use nix::{sys::signal::Signal, unistd::Pid};
use parking_lot::Mutex;

/// A live container's name and the network it joined.
type ContainerInfo = (String, Option<String>);

/// Live containers by id.
static CONTAINERS: LazyLock<Mutex<HashMap<String, ContainerInfo>>> = LazyLock::new(Mutex::default);
/// Live `spiced` processes: pid to the HTTP port that identifies them.
static PROCESSES: LazyLock<Mutex<HashMap<u32, u16>>> = LazyLock::new(Mutex::default);
/// `--keep`: leave source containers running, whatever ends the run.
static KEEP_CONTAINERS: AtomicBool = AtomicBool::new(false);
/// Set once a signal has started the teardown: scripts still running may go
/// on starting containers and processes until the runner exits, and those are
/// removed as they start rather than missed.
static SHUTTING_DOWN: AtomicBool = AtomicBool::new(false);

/// Whether a signal has started the teardown: nothing new should start.
pub fn shutting_down() -> bool {
    SHUTTING_DOWN.load(Ordering::SeqCst)
}

pub fn keep_containers() {
    KEEP_CONTAINERS.store(true, Ordering::Relaxed);
}

pub fn track_container(id: &str, name: &str, network: Option<&str>) {
    CONTAINERS.lock().insert(
        id.to_string(),
        (name.to_string(), network.map(str::to_string)),
    );
    if SHUTTING_DOWN.load(Ordering::SeqCst) {
        remove_if_tracked(id);
    }
}

/// Forgets a container the runner removed itself.
pub fn untrack_container(id: &str) {
    CONTAINERS.lock().remove(id);
}

/// Removes a container that is still tracked (and its network, once empty),
/// unless containers are kept. For `Drop`.
pub fn remove_if_tracked(id: &str) {
    if KEEP_CONTAINERS.load(Ordering::Relaxed) {
        return;
    }
    let Some((name, network)) = CONTAINERS.lock().remove(id) else {
        return;
    };
    remove_container(&name, network.as_deref());
}

pub fn track_process(pid: u32, http_port: u16) {
    // Registered before the check, as containers are: a teardown that set the
    // flag either drains this entry or is seen here, never neither.
    PROCESSES.lock().insert(pid, http_port);
    if SHUTTING_DOWN.load(Ordering::SeqCst)
        && PROCESSES.lock().remove(&pid).is_some()
        && let Ok(raw) = i32::try_from(pid)
    {
        let _ = nix::sys::signal::kill(Pid::from_raw(raw), Signal::SIGKILL);
    }
}

pub fn untrack_process(pid: u32) {
    PROCESSES.lock().remove(&pid);
}

fn remove_container(name: &str, network: Option<&str>) {
    let _ = Command::new("docker")
        .args(["rm", "-f", "-v", name])
        .output();
    // Fails while another container is still on it; the last one removes it.
    if let Some(network) = network {
        let _ = Command::new("docker")
            .args(["network", "rm", network])
            .output();
    }
}

/// Kills every tracked `spiced` and removes every tracked container: what a
/// signal leaves to do.
fn remove_all() {
    // Registered before draining, so nothing registered later is missed.
    SHUTTING_DOWN.store(true, Ordering::SeqCst);
    let processes: Vec<(u32, u16)> = PROCESSES.lock().drain().collect();
    for (pid, port) in processes {
        let Ok(raw) = i32::try_from(pid) else {
            continue;
        };
        // Only the process this runner started, not one that reused its pid.
        if crate::reap::command_line(raw)
            .is_some_and(|c| c.contains(&format!("--http 127.0.0.1:{port}")))
        {
            let _ = nix::sys::signal::kill(Pid::from_raw(raw), Signal::SIGKILL);
        }
    }
    if KEEP_CONTAINERS.load(Ordering::Relaxed) {
        return;
    }
    let containers: Vec<ContainerInfo> = CONTAINERS.lock().drain().map(|(_, v)| v).collect();
    for (name, network) in &containers {
        remove_container(name, network.as_deref());
    }
    // A container whose create request was in flight when the drain ran is
    // tracked by nobody; give such requests a moment, then sweep by name.
    std::thread::sleep(std::time::Duration::from_millis(500));
    if let Ok(me) = i32::try_from(std::process::id()) {
        crate::reap::remove_own_docker(me);
    }
}

/// On SIGINT or SIGTERM, tears down what is left and exits (130 or 143); a
/// second signal exits at once.
pub fn install_signal_handler() {
    tokio::spawn(async {
        use tokio::signal::unix::{SignalKind, signal};
        let (Ok(mut interrupt), Ok(mut terminate)) = (
            signal(SignalKind::interrupt()),
            signal(SignalKind::terminate()),
        ) else {
            eprintln!(
                "warning: could not install signal handlers; an interrupted run leaves its containers to the next run's reaper"
            );
            return;
        };
        let code = tokio::select! {
            _ = interrupt.recv() => 130,
            _ = terminate.recv() => 143,
        };
        eprintln!(
            "interrupted: stopping this run's spiced processes and removing its containers (signal again to exit at once)"
        );
        tokio::spawn(async move {
            tokio::select! {
                _ = interrupt.recv() => {}
                _ = terminate.recv() => {}
            }
            std::process::exit(code);
        });
        let _ = tokio::task::spawn_blocking(remove_all).await;
        std::process::exit(code);
    });
}
