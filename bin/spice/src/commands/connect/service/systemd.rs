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

//! The Linux back end: one systemd unit per instance directory.

use std::path::{Path, PathBuf};

use super::{InstalledService, PreflightFailure, SYSTEMD_RUNTIME_MARKER};
use crate::error::{Error, Result};

/// Directory systemd reads administrator-provided unit files from.
const SYSTEMD_UNIT_DIR: &str = "/etc/systemd/system";

/// Shared prefix of every unit this command installs. Also the glob root used
/// to discover installed instances.
const UNIT_PREFIX: &str = "spiced-cloud-connect";

/// Suffix of every unit this command installs.
const UNIT_SUFFIX: &str = ".service";

/// Derive this instance directory's unit name.
pub(super) fn unit_name_for_dir(dir: &Path) -> String {
    format!(
        "{UNIT_PREFIX}-{stem}{UNIT_SUFFIX}",
        stem = super::name_stem_for_dir(dir)
    )
}

/// Render the unit file for an instance.
///
/// `instance_dir` is baked in as `WorkingDirectory` so the service resolves its
/// spicepod and `<dir>/.spice` state from the directory the operator enrolled,
/// not from wherever systemd happens to start it. `spiced_path` is the absolute
/// binary path resolved at install time.
pub(super) fn render_unit(instance_dir: &Path, spiced_path: &Path) -> String {
    format!(
        "[Unit]\n\
         Description=Spice runtime connected to Spice Cloud ({instance_dir})\n\
         Documentation=https://spiceai.org/docs\n\
         After=network-online.target\n\
         Wants=network-online.target\n\
         \n\
         [Service]\n\
         Type=simple\n\
         WorkingDirectory={instance_dir}\n\
         ExecStart={spiced} --cloud-connect\n\
         # Every deployment applies by restart: the runtime validates and\n\
         # persists the new spicepod, exits 0, and this relaunches it on the\n\
         # new configuration. Without Restart=always a deployment would stop\n\
         # the instance instead of updating it.\n\
         Restart=always\n\
         RestartSec=5\n\
         # A crash loop must not be given up on: an instance that lost its\n\
         # network for an hour has to come back on its own.\n\
         StartLimitIntervalSec=0\n\
         KillSignal=SIGTERM\n\
         TimeoutStopSec=30\n\
         \n\
         [Install]\n\
         WantedBy=multi-user.target\n",
        instance_dir = instance_dir.display(),
        spiced = spiced_path.display(),
    )
}

pub(super) fn preflight() -> std::result::Result<(), PreflightFailure> {
    if !Path::new(SYSTEMD_RUNTIME_MARKER).is_dir() {
        return Err(PreflightFailure::SystemdUnavailable);
    }
    if !super::is_root() {
        return Err(PreflightFailure::NotRoot);
    }
    Ok(())
}

pub(super) fn install(instance_dir: &Path, spiced_path: &Path) -> Result<InstalledService> {
    // systemd reports a unit whose `ExecStart` will not run, so the staged copy
    // needs no separate check.
    let staged_runtime = super::stage_runtime(spiced_path, |_, _| Ok(()))?;

    let name = unit_name_for_dir(instance_dir);
    let path = PathBuf::from(SYSTEMD_UNIT_DIR).join(&name);
    let unit = render_unit(instance_dir, &staged_runtime);

    std::fs::write(&path, unit).map_err(|e| Error::CloudConnectIo {
        message: format!(
            "write systemd unit {}: {e}. The identity is staged at {}/.spice — fix the problem \
             and re-run `sudo spice connect --install` to finish.",
            path.display(),
            instance_dir.display()
        ),
    })?;

    systemctl(&["daemon-reload"])?;
    // `enable --now` starts the service and persists the boot-time link in one
    // step. On a reinstall the unit is already enabled and already running, so
    // follow with an explicit restart to pick up the rewritten unit and the
    // upgraded binary — `enable --now` alone would leave the old process up.
    systemctl(&["enable", "--now", &name])?;
    systemctl(&["restart", &name])?;

    Ok(InstalledService {
        name,
        path,
        working_dir: instance_dir.to_path_buf(),
        runtime: staged_runtime,
    })
}

/// Stop, disable, and delete the unit for `instance_dir`.
///
/// Stop/disable failures are tolerated — a unit file left on disk would restart
/// a service against a released identity forever, so the deletion is what must
/// happen.
pub(super) fn uninstall(instance_dir: &Path) -> Result<Option<InstalledService>> {
    let Some(unit) = find_for_dir(instance_dir) else {
        return Ok(None);
    };

    // Best-effort: a unit that is already stopped, already disabled, or whose
    // systemd is not running must not block removing the file.
    if let Err(err) = systemctl(&["disable", "--now", &unit.name]) {
        tracing::debug!("systemctl disable --now {}: {err}", unit.name);
    }

    std::fs::remove_file(&unit.path).map_err(|e| Error::CloudConnectIo {
        message: format!(
            "remove systemd unit {}: {e}. The service would keep restarting against a released \
             identity — delete the file and run `sudo systemctl daemon-reload`.",
            unit.path.display()
        ),
    })?;

    if let Err(err) = systemctl(&["daemon-reload"]) {
        tracing::debug!("systemctl daemon-reload: {err}");
    }

    Ok(Some(unit))
}

pub(super) fn find_for_dir(instance_dir: &Path) -> Option<InstalledService> {
    let name = unit_name_for_dir(instance_dir);
    let path = PathBuf::from(SYSTEMD_UNIT_DIR).join(&name);
    if !path.is_file() {
        return None;
    }
    let unit = std::fs::read_to_string(&path).ok();
    let working_dir = unit
        .as_deref()
        .and_then(parse_working_dir)
        .unwrap_or_else(|| instance_dir.to_path_buf());
    let runtime = unit
        .as_deref()
        .and_then(parse_exec_runtime)
        .unwrap_or_else(super::staged_runtime_path);
    Some(InstalledService {
        name,
        path,
        working_dir,
        runtime,
    })
}

pub(super) fn discover_all() -> Vec<InstalledService> {
    let Ok(entries) = std::fs::read_dir(SYSTEMD_UNIT_DIR) else {
        return Vec::new();
    };

    let mut units: Vec<InstalledService> = entries
        .filter_map(std::result::Result::ok)
        .filter_map(|entry| {
            let path = entry.path();
            let name = path.file_name()?.to_str()?.to_string();
            if !name.starts_with(UNIT_PREFIX) || !name.ends_with(UNIT_SUFFIX) {
                return None;
            }
            // Skip a symlink systemd itself created (the `multi-user.target.wants`
            // links live elsewhere, but a hand-made alias here would double-report).
            if !path.is_file() {
                return None;
            }
            let unit = std::fs::read_to_string(&path).ok()?;
            let working_dir = parse_working_dir(&unit)?;
            let runtime = parse_exec_runtime(&unit).unwrap_or_else(super::staged_runtime_path);
            Some(InstalledService {
                name,
                path,
                working_dir,
                runtime,
            })
        })
        .collect();
    units.sort_by(|a, b| a.name.cmp(&b.name));
    units
}

/// Parse the `WorkingDirectory=` value out of a rendered unit.
fn parse_working_dir(unit: &str) -> Option<PathBuf> {
    unit_directive(unit, "WorkingDirectory=").map(PathBuf::from)
}

/// Parse the binary `ExecStart=` runs out of a rendered unit, dropping its
/// arguments. Lets `status` report which runtime an installed service is
/// actually running, including a unit written before the runtime moved.
fn parse_exec_runtime(unit: &str) -> Option<PathBuf> {
    let exec = unit_directive(unit, "ExecStart=")?;
    // systemd allows `-`/`@`/`+` prefixes on the executable; strip them so the
    // reported path is the binary itself.
    let exec = exec.trim_start_matches(['-', '@', '+', '!', ':']);
    exec.split_whitespace().next().map(PathBuf::from)
}

/// The value of a `Key=` directive in a rendered unit, trimmed and non-empty.
fn unit_directive<'a>(unit: &'a str, key: &str) -> Option<&'a str> {
    unit.lines()
        .map(str::trim)
        .find_map(|line| line.strip_prefix(key))
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

/// The service's current state as `systemctl is-active` reports it
/// (`active`, `inactive`, `failed`, …), or `None` when it cannot be determined.
pub(super) fn is_active(unit_name: &str) -> Option<String> {
    let output = std::process::Command::new("systemctl")
        .arg("is-active")
        .arg(unit_name)
        .output()
        .ok()?;
    // `is-active` exits non-zero for anything but `active`, and prints the
    // state either way — so read stdout regardless of the exit status.
    let state = String::from_utf8_lossy(&output.stdout).trim().to_string();
    (!state.is_empty()).then_some(state)
}

pub(super) fn manage_hints(unit_name: &str) -> Vec<String> {
    vec![
        format!("systemctl status {unit_name}"),
        format!("journalctl -u {unit_name} -f"),
    ]
}

/// Run `systemctl <args>`, turning a non-zero exit into an error carrying
/// systemd's own stderr — which names the actual problem far better than an
/// exit code.
fn systemctl(args: &[&str]) -> Result<()> {
    let output = std::process::Command::new("systemctl")
        .args(args)
        .output()
        .map_err(|e| Error::CloudConnectIo {
            message: format!("run `systemctl {}`: {e}", args.join(" ")),
        })?;

    if output.status.success() {
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    Err(Error::CloudConnectIo {
        message: format!(
            "`systemctl {}` failed: {}",
            args.join(" "),
            if stderr.is_empty() {
                format!("exit status {}", output.status)
            } else {
                stderr
            }
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_name_is_legible_and_prefixed() {
        let name = unit_name_for_dir(Path::new("/opt/edge-1"));
        assert!(name.starts_with("spiced-cloud-connect-edge-1-"), "{name}");
        assert!(name.ends_with(".service"), "{name}");
    }

    #[test]
    fn unit_name_handles_a_directory_with_no_usable_name() {
        let name = unit_name_for_dir(Path::new("/"));
        assert!(name.starts_with("spiced-cloud-connect-"), "{name}");
        assert!(name.ends_with(".service"), "{name}");
    }

    #[test]
    fn unit_name_stays_manageable_for_a_long_directory() {
        let long = unit_name_for_dir(Path::new(&format!("/opt/{}", "a".repeat(120))));
        assert!(long.len() < 80, "unit name should stay manageable: {long}");
        assert!(long.ends_with(".service"));
    }

    #[test]
    fn rendered_unit_bakes_the_absolute_working_directory() {
        let unit = render_unit(
            Path::new("/opt/edge-1"),
            Path::new("/root/.spice/bin/spiced"),
        );
        assert!(unit.contains("WorkingDirectory=/opt/edge-1\n"));
        assert_eq!(
            parse_working_dir(&unit),
            Some(PathBuf::from("/opt/edge-1")),
            "the working directory must round-trip so status can discover it"
        );
    }

    #[test]
    fn rendered_unit_runs_spiced_with_cloud_connect() {
        let unit = render_unit(
            Path::new("/opt/edge-1"),
            Path::new("/root/.spice/bin/spiced"),
        );
        assert!(unit.contains("ExecStart=/root/.spice/bin/spiced --cloud-connect\n"));
    }

    #[test]
    fn rendered_unit_always_restarts() {
        // `Restart=always` is the contract every deployment depends on: the
        // runtime exits 0 to apply an update and systemd relaunches it.
        let unit = render_unit(Path::new("/opt/edge-1"), Path::new("/usr/bin/spiced"));
        assert!(unit.contains("\nRestart=always\n"));
        // A rate limit would let a crash loop give up permanently.
        assert!(unit.contains("StartLimitIntervalSec=0"));
        assert!(unit.contains("WantedBy=multi-user.target"));
    }

    #[test]
    fn rendered_unit_runs_the_root_owned_staged_runtime() {
        // The unit runs as root, so ExecStart must be the staged root-owned copy
        // — never the operator's `~/.spice/bin/spiced`, which they could later
        // replace to gain root.
        let staged = super::super::staged_runtime_path();
        let unit = render_unit(Path::new("/opt/edge-1"), &staged);
        assert_eq!(parse_exec_runtime(&unit), Some(staged));
    }

    #[test]
    fn parse_exec_runtime_drops_arguments_and_prefixes() {
        assert_eq!(
            parse_exec_runtime(
                "[Service]\nExecStart=/usr/local/lib/spice/spiced --cloud-connect\n"
            ),
            Some(PathBuf::from("/usr/local/lib/spice/spiced")),
            "the reported runtime is the binary, not the whole command line"
        );
        // systemd's special executable prefixes are not part of the path.
        assert_eq!(
            parse_exec_runtime("ExecStart=-/usr/bin/spiced --cloud-connect\n"),
            Some(PathBuf::from("/usr/bin/spiced"))
        );
        assert_eq!(
            parse_exec_runtime("[Service]\nWorkingDirectory=/opt/x\n"),
            None
        );
    }

    #[test]
    fn parse_working_dir_ignores_units_without_one() {
        assert_eq!(parse_working_dir("[Service]\nExecStart=/bin/true\n"), None);
        assert_eq!(parse_working_dir("WorkingDirectory=\n"), None);
        assert_eq!(
            parse_working_dir("[Service]\n  WorkingDirectory=/opt/x  \n"),
            Some(PathBuf::from("/opt/x"))
        );
    }

    #[test]
    fn manage_hints_name_the_unit() {
        let hints = manage_hints("spiced-cloud-connect-edge-1-1a2b3c4d.service");
        assert_eq!(
            hints,
            vec![
                "systemctl status spiced-cloud-connect-edge-1-1a2b3c4d.service",
                "journalctl -u spiced-cloud-connect-edge-1-1a2b3c4d.service -f",
            ]
        );
    }
}
