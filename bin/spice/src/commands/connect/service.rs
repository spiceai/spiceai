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

//! `spice connect --install`: run `spiced --cloud-connect` as a persistent
//! systemd service.
//!
//! The supervisor is not a convenience here, it is the mechanism deployments
//! depend on: a deployment applies by having the runtime validate and persist
//! the new spicepod and then exit 0, which only becomes a deployment if
//! something relaunches it. The unit therefore sets `Restart=always`.
//!
//! ## Support matrix
//!
//! `--install` is Linux/systemd. Containers get the same guarantee from their
//! runtime's restart policy (`docker run --restart unless-stopped`) and use the
//! env-var flow instead. macOS and Windows enroll and run `spiced
//! --cloud-connect` under the user's own supervisor.
//!
//! ## One unit per instance directory
//!
//! Two instances on one host must produce two independently-running services,
//! so the unit name is derived deterministically from the *absolute* instance
//! directory (see [`unit_name_for_dir`]) and the directory is baked into the
//! unit as `WorkingDirectory`. A `--install` or `remove` in one instance
//! directory can therefore never touch another instance's unit.
//!
//! v1 runs `spiced` as root. A dedicated service user is planned hardening.

use std::path::{Path, PathBuf};

use sha2::{Digest as _, Sha256};

use crate::error::{Error, Result};

/// Directory systemd reads administrator-provided unit files from.
const SYSTEMD_UNIT_DIR: &str = "/etc/systemd/system";

/// Marks a booted systemd system. Present only when systemd is PID 1 and
/// running, which is exactly the condition `systemctl` needs.
const SYSTEMD_RUNTIME_MARKER: &str = "/run/systemd/system";

/// Shared prefix of every unit this command installs. Also the glob root used
/// to discover installed instances.
const UNIT_PREFIX: &str = "spiced-cloud-connect";

/// Longest directory-name fragment carried into a unit name, so a deeply-named
/// instance directory cannot produce an unwieldy unit. The appended digest is
/// what makes the name unique; the fragment only makes it legible.
const MAX_NAME_FRAGMENT: usize = 32;

/// A systemd unit installed for one instance directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InstalledUnit {
    /// Unit name including the `.service` suffix.
    pub(crate) name: String,
    /// Absolute path of the unit file.
    pub(crate) path: PathBuf,
    /// The instance directory baked in as `WorkingDirectory`.
    pub(crate) working_dir: PathBuf,
}

/// Derive this instance directory's unit name.
///
/// The name is `spiced-cloud-connect-<fragment>-<digest>.service`: a legible
/// fragment from the directory name plus the first 8 hex digits of the SHA-256
/// of the absolute path. The digest is what guarantees two directories never
/// collide — including two directories with the same basename
/// (`/srv/a/edge` and `/srv/b/edge`) — while the fragment keeps
/// `systemctl status` output recognisable.
///
/// `dir` must already be absolute (`CloudConnectConfig::resolve_config_dir`
/// absolutises it at enroll time): the same instance must always derive the
/// same unit name, and a relative path would derive a different one depending
/// on where the command ran.
pub(crate) fn unit_name_for_dir(dir: &Path) -> String {
    let digest = Sha256::digest(dir.as_os_str().as_encoded_bytes());
    let mut short = String::with_capacity(8);
    for byte in digest.iter().take(4) {
        use std::fmt::Write as _;
        // Writing into a String is infallible; the Result exists only to
        // satisfy the `Write` trait.
        let _ = write!(short, "{byte:02x}");
    }

    let fragment = dir
        .file_name()
        .and_then(|name| name.to_str())
        .map(sanitize_fragment)
        .filter(|fragment| !fragment.is_empty());

    match fragment {
        Some(fragment) => format!("{UNIT_PREFIX}-{fragment}-{short}.service"),
        // A directory with no usable name (`/`, or non-UTF-8) still installs;
        // the digest alone names it.
        None => format!("{UNIT_PREFIX}-{short}.service"),
    }
}

/// Reduce a directory name to the characters systemd unit names accept,
/// collapsing runs of anything else to a single `-` and bounding the length.
fn sanitize_fragment(name: &str) -> String {
    let mut out = String::with_capacity(name.len().min(MAX_NAME_FRAGMENT));
    let mut last_was_dash = false;
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            last_was_dash = false;
        } else if !last_was_dash && !out.is_empty() {
            out.push('-');
            last_was_dash = true;
        }
        if out.len() >= MAX_NAME_FRAGMENT {
            break;
        }
    }
    out.trim_matches('-').to_string()
}

/// Render the unit file for an instance.
///
/// `instance_dir` is baked in as `WorkingDirectory` so the service resolves its
/// spicepod and `<dir>/.spice` state from the directory the operator enrolled,
/// not from wherever systemd happens to start it. `spiced_path` is the absolute
/// binary path resolved at install time.
pub(crate) fn render_unit(instance_dir: &Path, spiced_path: &Path) -> String {
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

/// Why a host cannot have a service installed on it.
///
/// Distinguished from a generic error so the caller can run the check *before*
/// the HTTPS enroll: a failed preflight must consume nothing, leaving the
/// adoption code valid.
#[derive(Debug)]
pub(crate) enum PreflightFailure {
    /// Not Linux — `--install` has no systemd to install into.
    UnsupportedPlatform,
    /// Linux, but systemd is not the running init.
    SystemdUnavailable,
    /// Not running as root, so `/etc/systemd/system` is not writable and
    /// `systemctl` cannot manage units.
    NotRoot,
}

impl PreflightFailure {
    /// The operator-facing message, naming the fix and the alternative path.
    fn message(&self) -> String {
        match self {
            Self::UnsupportedPlatform => format!(
                "Failed to install the Spice Cloud Connect service: --install requires \
                 Linux with systemd (this host is {}). Enroll without --install and run \
                 `spiced --cloud-connect` under your own supervisor, or run Spice in a \
                 container with a restart policy (`docker run --restart unless-stopped`). \
                 See: https://spiceai.org/docs",
                std::env::consts::OS
            ),
            Self::SystemdUnavailable => format!(
                "Failed to install the Spice Cloud Connect service: systemd is not running on \
                 this host ({SYSTEMD_RUNTIME_MARKER} is absent). This is normal inside a \
                 container: set SPICE_CONNECT_ADOPT_CODE and run `spiced --cloud-connect` under \
                 the container runtime's restart policy (`docker run --restart unless-stopped`) \
                 instead of --install. See: https://spiceai.org/docs"
            ),
            Self::NotRoot => "Failed to install the Spice Cloud Connect service: installing a \
                 systemd unit requires root. Re-run with sudo: `sudo spice connect --install`. \
                 See: https://spiceai.org/docs"
                .to_string(),
        }
    }
}

impl From<PreflightFailure> for Error {
    fn from(failure: PreflightFailure) -> Self {
        Error::InvalidArgument {
            message: failure.message(),
        }
    }
}

/// Check that this host can have a service installed, **before** anything
/// irreversible happens.
///
/// Called ahead of the HTTPS enroll so a host without systemd or without root
/// fails with nothing installed and the adoption code still redeemable.
pub(crate) fn preflight() -> std::result::Result<(), PreflightFailure> {
    if !cfg!(target_os = "linux") {
        return Err(PreflightFailure::UnsupportedPlatform);
    }
    if !Path::new(SYSTEMD_RUNTIME_MARKER).is_dir() {
        return Err(PreflightFailure::SystemdUnavailable);
    }
    if !is_root() {
        return Err(PreflightFailure::NotRoot);
    }
    Ok(())
}

/// `true` when this process is running as root.
///
/// Read from the owner of `/proc/self` rather than via `libc::geteuid`, since
/// the only platform that reaches here has procfs and this keeps the check free
/// of `unsafe`.
#[cfg(unix)]
fn is_root() -> bool {
    use std::os::unix::fs::MetadataExt as _;
    std::fs::metadata("/proc/self").is_ok_and(|meta| meta.uid() == 0)
}

#[cfg(not(unix))]
fn is_root() -> bool {
    false
}

/// Install (or reinstall) and start the unit for `instance_dir`.
///
/// Idempotent, and the in-place upgrade path: re-running rewrites the unit
/// against the current `spiced` binary and restarts the service, leaving the
/// staged identity untouched. Returns the installed unit.
///
/// # Errors
///
/// Returns an error when the unit cannot be written or when `systemctl` fails.
/// Since this runs *after* the enroll, the messages say that the identity is
/// already staged and how to resume.
pub(crate) fn install(instance_dir: &Path, spiced_path: &Path) -> Result<InstalledUnit> {
    let name = unit_name_for_dir(instance_dir);
    let path = PathBuf::from(SYSTEMD_UNIT_DIR).join(&name);
    let unit = render_unit(instance_dir, spiced_path);

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

    Ok(InstalledUnit {
        name,
        path,
        working_dir: instance_dir.to_path_buf(),
    })
}

/// Stop, disable, and delete the unit for `instance_dir`.
///
/// Returns the unit that was removed, or `None` when no unit was installed for
/// this directory. Stop/disable failures are tolerated — a unit file left on
/// disk would restart a service against a released identity forever, so the
/// deletion is what must happen.
pub(crate) fn uninstall(instance_dir: &Path) -> Result<Option<InstalledUnit>> {
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

/// The unit installed for `instance_dir`, if any.
///
/// Matches on the derived name rather than by scanning `WorkingDirectory`, so
/// the lookup is exact and cannot pick up another instance's unit.
pub(crate) fn find_for_dir(instance_dir: &Path) -> Option<InstalledUnit> {
    let name = unit_name_for_dir(instance_dir);
    let path = PathBuf::from(SYSTEMD_UNIT_DIR).join(&name);
    if !path.is_file() {
        return None;
    }
    let working_dir = read_working_dir(&path).unwrap_or_else(|| instance_dir.to_path_buf());
    Some(InstalledUnit {
        name,
        path,
        working_dir,
    })
}

/// Every unit this command installed on the host, sorted by name.
///
/// This is what lets `spice connect status` report against installed services
/// when the resolved directory holds no instance state, instead of printing a
/// misleading "not connected" for a host that is in fact connected from
/// somewhere else.
pub(crate) fn discover_all() -> Vec<InstalledUnit> {
    let Ok(entries) = std::fs::read_dir(SYSTEMD_UNIT_DIR) else {
        return Vec::new();
    };

    let mut units: Vec<InstalledUnit> = entries
        .filter_map(std::result::Result::ok)
        .filter_map(|entry| {
            let path = entry.path();
            let name = path.file_name()?.to_str()?.to_string();
            if !name.starts_with(UNIT_PREFIX) || !name.ends_with(".service") {
                return None;
            }
            // Skip a symlink systemd itself created (the `multi-user.target.wants`
            // links live elsewhere, but a hand-made alias here would double-report).
            if !path.is_file() {
                return None;
            }
            let working_dir = read_working_dir(&path)?;
            Some(InstalledUnit {
                name,
                path,
                working_dir,
            })
        })
        .collect();
    units.sort_by(|a, b| a.name.cmp(&b.name));
    units
}

/// Extract `WorkingDirectory=` from a unit file's `[Service]` section.
fn read_working_dir(path: &Path) -> Option<PathBuf> {
    let contents = std::fs::read_to_string(path).ok()?;
    parse_working_dir(&contents)
}

/// Parse the `WorkingDirectory=` value out of a rendered unit.
fn parse_working_dir(unit: &str) -> Option<PathBuf> {
    unit.lines()
        .map(str::trim)
        .find_map(|line| line.strip_prefix("WorkingDirectory="))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

/// The service's current state as `systemctl is-active` reports it
/// (`active`, `inactive`, `failed`, …), or `None` when it cannot be determined.
pub(crate) fn is_active(unit_name: &str) -> Option<String> {
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
    fn unit_name_is_deterministic() {
        let dir = Path::new("/opt/edge-1");
        assert_eq!(unit_name_for_dir(dir), unit_name_for_dir(dir));
    }

    #[test]
    fn unit_name_is_legible_and_prefixed() {
        let name = unit_name_for_dir(Path::new("/opt/edge-1"));
        assert!(name.starts_with("spiced-cloud-connect-edge-1-"), "{name}");
        assert!(name.ends_with(".service"), "{name}");
    }

    #[test]
    fn two_directories_yield_two_units() {
        // The acceptance criterion: two installs in two directories must be
        // two independently-running units.
        let a = unit_name_for_dir(Path::new("/opt/edge-1"));
        let b = unit_name_for_dir(Path::new("/opt/edge-2"));
        assert_ne!(a, b);
    }

    #[test]
    fn same_basename_in_different_parents_does_not_collide() {
        // The legible fragment is identical here, so only the path digest keeps
        // these apart — a `remove` in one must never touch the other's unit.
        let a = unit_name_for_dir(Path::new("/srv/a/edge"));
        let b = unit_name_for_dir(Path::new("/srv/b/edge"));
        assert_ne!(a, b);
        assert!(a.starts_with("spiced-cloud-connect-edge-"));
        assert!(b.starts_with("spiced-cloud-connect-edge-"));
    }

    #[test]
    fn unit_name_sanitizes_and_bounds_the_fragment() {
        let name = unit_name_for_dir(Path::new("/opt/My Edge_Site!!/"));
        assert!(
            name.starts_with("spiced-cloud-connect-my-edge-site-"),
            "{name}"
        );

        // A long directory name is truncated, not carried whole.
        let long = unit_name_for_dir(Path::new(&format!("/opt/{}", "a".repeat(120))));
        assert!(long.len() < 80, "unit name should stay manageable: {long}");
        assert!(long.ends_with(".service"));
    }

    #[test]
    fn unit_name_handles_a_directory_with_no_usable_name() {
        let name = unit_name_for_dir(Path::new("/"));
        assert!(name.starts_with("spiced-cloud-connect-"), "{name}");
        assert!(name.ends_with(".service"), "{name}");
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
    fn parse_working_dir_ignores_units_without_one() {
        assert_eq!(parse_working_dir("[Service]\nExecStart=/bin/true\n"), None);
        assert_eq!(parse_working_dir("WorkingDirectory=\n"), None);
        assert_eq!(
            parse_working_dir("[Service]\n  WorkingDirectory=/opt/x  \n"),
            Some(PathBuf::from("/opt/x"))
        );
    }

    #[test]
    fn preflight_failures_name_the_fix() {
        assert!(
            PreflightFailure::NotRoot.message().contains("sudo"),
            "the root failure must name sudo"
        );
        assert!(
            PreflightFailure::SystemdUnavailable
                .message()
                .contains("restart policy"),
            "the no-systemd failure must point containers at the env-var flow"
        );
        assert!(
            PreflightFailure::UnsupportedPlatform
                .message()
                .contains("spiced --cloud-connect"),
            "a non-Linux host must be told the supported way to run"
        );
    }
}
