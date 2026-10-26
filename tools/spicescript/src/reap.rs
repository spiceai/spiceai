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

//! Cleans up after runs whose runner died without stopping what it started.
//!
//! A runner kills its `spiced` processes and containers when it exits, but
//! not when it is killed outright; they then hold ports, memory and CPU and
//! skew every later run. Each run directory is named `<script>-<n>-<runner
//! pid>`, and each `spiced` records its pid and HTTP port in `spiced.pid`
//! beside its log. At startup, for every run directory whose runner is no
//! longer alive, this stops the recorded processes that are still the same
//! `spiced` (same port on their command line, so a reused pid is left
//! alone) and removes the run's containers. Runs of a runner that is still
//! alive are never touched.

use std::path::Path;

use nix::{sys::signal::Signal, unistd::Pid};

/// The file a `spiced` records `<pid> <http port>` in, in its directory.
pub const PID_FILE: &str = "spiced.pid";

/// Whether a process with this pid exists.
fn alive(pid: i32) -> bool {
    nix::sys::signal::kill(Pid::from_raw(pid), None).is_ok()
}

/// The runner pid a run directory's name ends with, if it has one.
fn runner_pid(dir_name: &str) -> Option<i32> {
    dir_name.rsplit('-').next()?.parse().ok()
}

/// Every `spiced.pid` in `dir` and the two levels below it: an instance's
/// directory (`<run>/<variant>`) and its executors' (`<run>/<variant>/executor`).
/// Acceleration data lives deeper and is never walked.
fn pid_files(dir: &Path, depth: usize, found: &mut Vec<std::path::PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.file_name().is_some_and(|n| n == PID_FILE) {
            found.push(path);
        } else if depth > 0 && entry.file_type().is_ok_and(|t| t.is_dir()) {
            pid_files(&path, depth - 1, found);
        }
    }
}

/// The command line of `pid`, as `ps` prints it.
pub(crate) fn command_line(pid: i32) -> Option<String> {
    let out = std::process::Command::new("ps")
        .args(["-o", "command=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// Stops what dead runners left behind, and says what it did.
pub fn reap(work_dir: &Path) {
    reap_with(work_dir, remove_docker_leftovers);
}

/// Stops the `spiced` processes dead runners left under `work_dir`, then runs
/// `sweep` for their containers and networks. The sweep runs even when
/// `work_dir` does not exist: a run with a fresh `--work-dir` still cleans up
/// after one that was killed.
fn reap_with(work_dir: &Path, sweep: impl FnOnce(i32)) {
    let me = i32::try_from(std::process::id()).unwrap_or(-1);
    if let Ok(entries) = std::fs::read_dir(work_dir) {
        stop_orphaned_spiced(entries, me);
    }
    sweep(me);
}

/// Stops each `spiced` whose pid file, under a dead runner's directory among
/// `entries`, still names a live process.
fn stop_orphaned_spiced(entries: std::fs::ReadDir, me: i32) {
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        let Some(owner) = runner_pid(&name) else {
            continue;
        };
        if owner == me || alive(owner) {
            continue;
        }
        let mut files = Vec::new();
        pid_files(&entry.path(), 2, &mut files);
        for file in files {
            let Ok(text) = std::fs::read_to_string(&file) else {
                continue;
            };
            let mut parts = text.split_whitespace();
            let (Some(Ok(pid)), Some(port)) = (parts.next().map(str::parse::<i32>), parts.next())
            else {
                continue;
            };
            let same = alive(pid)
                && command_line(pid)
                    .is_some_and(|c| c.contains(&format!("--http 127.0.0.1:{port}")));
            if same {
                println!(
                    "stopping spiced {pid} left by a runner that is gone ({})",
                    file.display()
                );
                let _ = nix::sys::signal::kill(Pid::from_raw(pid), Signal::SIGKILL);
            }
            // Reaped or already gone: later runs need not look again.
            let _ = std::fs::remove_file(&file);
        }
    }
}

/// Ids and runner pids of the containers (`kind` = `container`) or networks
/// this harness created: those carrying [`crate::docker::RUNNER_LABEL`].
fn docker_owners(kind: &str) -> Vec<(String, i32)> {
    let label = crate::docker::RUNNER_LABEL;
    let filter = format!("label={label}");
    let format = format!("{{{{.ID}}}} {{{{.Label \"{label}\"}}}}");
    let mut args = vec![kind, "ls"];
    if kind == "container" {
        args.push("-a");
    }
    args.extend(["--filter", &filter, "--format", &format]);
    let Ok(out) = std::process::Command::new("docker").args(&args).output() else {
        return Vec::new();
    };
    owners(&String::from_utf8_lossy(&out.stdout))
}

/// Parses `docker … ls` lines of `<id> <runner pid>`.
fn owners(listing: &str) -> Vec<(String, i32)> {
    listing
        .lines()
        .filter_map(|l| {
            let (id, pid) = l.split_once(' ')?;
            Some((id.to_string(), pid.trim().parse().ok()?))
        })
        .collect()
}

/// Removes containers, then networks, whose runner is gone — whether or not
/// its run directory still exists.
fn remove_docker_leftovers(me: i32) {
    remove_docker_where(|pid| pid != me && !alive(pid), "left by runs that are gone");
}

/// Removes this runner's own containers and networks: the last
/// sweep of a shutdown, for any created while it was already under way.
pub(crate) fn remove_own_docker(me: i32) {
    remove_docker_where(|pid| pid == me, "this run started");
}

fn remove_docker_where(owned: impl Fn(i32) -> bool, whose: &str) {
    for (kind, remove) in [("container", ["rm", "-f"]), ("network", ["network", "rm"])] {
        let ids: Vec<String> = docker_owners(kind)
            .into_iter()
            .filter(|(_, pid)| owned(*pid))
            .map(|(id, _)| id)
            .collect();
        if ids.is_empty() {
            continue;
        }
        println!("removing {} {kind}(s) {whose}", ids.len());
        let _ = std::process::Command::new("docker")
            .args(remove)
            .args(&ids)
            .output();
    }
}

#[cfg(test)]
mod tests {
    use super::runner_pid;

    #[test]
    fn sweeps_docker_even_when_the_work_dir_does_not_exist() {
        let missing =
            std::env::temp_dir().join(format!("spicescript-reap-missing-{}", std::process::id()));
        assert!(!missing.exists(), "{} exists", missing.display());
        let mut swept_for = None;
        super::reap_with(&missing, |me| swept_for = Some(me));
        assert_eq!(
            swept_for,
            Some(i32::try_from(std::process::id()).expect("pid fits i32"))
        );
    }

    #[test]
    fn a_docker_listing_names_each_resources_runner() {
        use super::owners;
        assert_eq!(
            owners("abc 555\ndef 2147483647\nghi \njkl x\n"),
            vec![("abc".to_string(), 555), ("def".to_string(), 2_147_483_647)]
        );
    }

    #[test]
    fn a_run_directory_names_its_runner() {
        assert_eq!(runner_pid("pg_dml-0-60856"), Some(60856));
        assert_eq!(runner_pid("http_caching_more-0-98319"), Some(98319));
        assert_eq!(runner_pid("sources"), None);
    }
}
