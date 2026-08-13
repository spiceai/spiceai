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
//!
//! ## Two domains, one code path
//!
//! An unprivileged install writes a user unit under the account's own
//! configuration directory and drives it with `systemctl --user`; a root
//! install writes a system unit under `/etc/systemd/system` and runs the
//! runtime as the invoking operator rather than as root. The user unit is the
//! least-privileged normal case, so it is what an ordinary invocation gets.
//!
//! ## One runtime per service
//!
//! Each unit executes a copy of `spiced` staged for that one instance. A single
//! host-wide copy would make an upgrade in one instance directory silently
//! change what every other instance runs at its next restart, and would leave
//! an uninstall unable to say whether the binary it is deleting is still in
//! use.
//!
//! ## An install is not finished when systemd accepts the unit
//!
//! `systemctl restart` returning zero means the service was started, not that
//! it works. So an install or upgrade gives the service [`HEALTH_GATE`] to
//! reach `active` and answer its health URL, and anything else puts the
//! previous unit and the previous runtime back before failing — an upgrade must
//! never leave an instance worse off than the one it replaced.

use std::io::{Read as _, Write as _};
use std::net::{TcpStream, ToSocketAddrs as _};
use std::path::{Path, PathBuf};
use std::time::Duration;

use super::backend::{InstallRequest, LogRequest, ServiceBackend, ServiceObservation};
use super::manifest::ServiceManifest;
use super::model::{LogSource, ServiceScope, ServiceStarts, ServiceState, Supervisor};
use super::{InstalledService, PreflightFailure, SYSTEMD_RUNTIME_MARKER, ServiceAccount};
use crate::error::{Error, Result};

/// Directory systemd reads administrator-provided unit files from.
const SYSTEMD_UNIT_DIR: &str = "/etc/systemd/system";

/// Directory systemd reads a user's own unit files from, relative to the
/// account's XDG config directory.
const SYSTEMD_USER_UNIT_SUBDIR: &str = "systemd/user";

/// Shared prefix of every unit this command installs. Also the glob root used
/// to discover installed instances.
const UNIT_PREFIX: &str = "spiced-cloud-connect";

/// Suffix of every unit this command installs.
const UNIT_SUFFIX: &str = ".service";

/// The target a system unit is wanted by: reached during boot with nobody
/// logged in.
const SYSTEM_WANTED_BY: &str = "multi-user.target";

/// The target a user unit is wanted by: reached when the account's own manager
/// starts, which is at login, or at boot for an account that lingers.
const USER_WANTED_BY: &str = "default.target";

/// Directory a user service's own runtime is staged into, relative to the
/// account's XDG data directory.
const USER_RUNTIME_SUBDIR: &str = "spice/services";

/// Mode a unit file is written with: readable by the supervisor and by the
/// operator reading it back, writable only by whoever installed it.
const UNIT_MODE: u32 = 0o644;

const SYSTEMCTL: &str = "systemctl";
const JOURNALCTL: &str = "journalctl";
const LOGINCTL: &str = "loginctl";

/// How long an install or upgrade has to prove the service healthy before it is
/// rolled back.
const HEALTH_GATE: Duration = Duration::from_secs(30);

/// How often the health gate asks. Bounded by attempts rather than by a clock
/// so the whole gate is exercised deterministically in tests.
const HEALTH_POLL_INTERVAL: Duration = Duration::from_millis(500);
const HEALTH_ATTEMPTS: u32 = 60;

/// How long a `start`, `stop`, or `restart` has to reach the state it asked
/// for. systemd returns from these once the job is *done*, so this only covers
/// a unit that is still settling.
const LIFECYCLE_POLL_INTERVAL: Duration = Duration::from_millis(250);
const LIFECYCLE_ATTEMPTS: u32 = 40;

/// How long the health probe waits for the instance to accept a connection and
/// answer. Short: it is retried for the whole of the gate.
const PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// Bytes of a health response read before giving up on finding a status line.
const PROBE_READ_LIMIT: usize = 512;

/// Consecutive `active` readings that stand in for an unanswered health probe:
/// a runtime that has served uninterrupted for this long is up, whatever the
/// recorded health URL points at.
const SETTLE_ATTEMPTS: u32 = 20;

/// Consecutive `failed` readings that end the health gate early.
///
/// One is not enough: a unit that is being restarted reports its state through
/// several words on the way, and calling the first `failed` terminal would roll
/// back an install that was about to succeed.
const FAILED_READINGS_BEFORE_GIVING_UP: u32 = 2;

/// Derive this instance directory's unit name.
pub(super) fn unit_name_for_dir(dir: &Path) -> String {
    format!(
        "{UNIT_PREFIX}-{stem}{UNIT_SUFFIX}",
        stem = super::name_stem_for_dir(dir)
    )
}

/// One completed supervisor command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CommandOutput {
    /// Whether the command reported success.
    pub(super) success: bool,
    /// The exit code, when the command exited on its own.
    pub(super) code: Option<i32>,
    pub(super) stdout: String,
    pub(super) stderr: String,
}

impl CommandOutput {
    /// How a failure is named in an error message: the supervisor's own words
    /// when it said any, because they diagnose the problem far better than an
    /// exit code.
    fn describe_failure(&self) -> String {
        let stderr = folded(self.stderr.trim());
        if !stderr.is_empty() {
            return stderr;
        }
        match self.code {
            Some(code) => format!("exit status {code}"),
            None => "terminated by a signal".to_string(),
        }
    }
}

/// Everything this back end does outside its own process.
///
/// Every supervisor call, the health probe, and the waiting between polls go
/// through here, so the whole lifecycle — including the states a test host
/// cannot be put into on demand, like a service that never becomes healthy —
/// is exercised deterministically against a scripted host.
pub(super) trait SystemdHost {
    /// Run a command to completion and capture what it said.
    ///
    /// # Errors
    ///
    /// Returns an error when the command could not be run at all, which is a
    /// different answer from a command that ran and reported a failure.
    fn output(&self, program: &str, args: &[&str]) -> std::io::Result<CommandOutput>;

    /// Run a command with this process's own stdout and stderr attached, for
    /// output that is streamed rather than captured.
    ///
    /// Returns the exit code, or `None` when the command was ended by a signal
    /// — which is how an interrupted `logs --follow` ends.
    ///
    /// # Errors
    ///
    /// Returns an error when the command could not be run at all.
    fn stream(&self, program: &str, args: &[&str]) -> std::io::Result<Option<i32>>;

    /// Whether the instance answers `url` right now.
    fn probe_health(&self, url: &str) -> bool;

    /// Wait before polling again.
    fn sleep(&self, duration: Duration);
}

/// The host as it really is.
pub(super) struct ProcessHost;

impl SystemdHost for ProcessHost {
    fn output(&self, program: &str, args: &[&str]) -> std::io::Result<CommandOutput> {
        let output = std::process::Command::new(program).args(args).output()?;
        Ok(CommandOutput {
            success: output.status.success(),
            code: output.status.code(),
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
        })
    }

    fn stream(&self, program: &str, args: &[&str]) -> std::io::Result<Option<i32>> {
        let status = std::process::Command::new(program).args(args).status()?;
        Ok(status.code())
    }

    fn probe_health(&self, url: &str) -> bool {
        probe_http_health(url)
    }

    fn sleep(&self, duration: Duration) {
        std::thread::sleep(duration);
    }
}

/// Whether a health URL is one this back end can probe.
///
/// Only plain HTTP is: the recorded URL is the loopback endpoint the runtime
/// serves, and a gate that cannot reach an unusual one must not fail an install
/// that is fine — it gates on what systemd reports instead.
fn is_probeable(url: &str) -> bool {
    url.strip_prefix("http://")
        .is_some_and(|rest| !rest.is_empty() && !rest.starts_with('/'))
}

/// `GET` the health URL and report whether it answered `2xx`.
///
/// Blocking and dependency-free on purpose: this runs inside the installer's
/// own thread, between two filesystem operations, and answers one question
/// about one loopback address.
fn probe_http_health(url: &str) -> bool {
    let Some(rest) = url.strip_prefix("http://") else {
        return false;
    };
    let (authority, path) = match rest.find('/') {
        Some(index) => (&rest[..index], &rest[index..]),
        None => (rest, "/"),
    };
    let authority = if authority.contains(':') {
        authority.to_string()
    } else {
        format!("{authority}:80")
    };

    let Ok(mut addrs) = authority.to_socket_addrs() else {
        return false;
    };
    let Some(addr) = addrs.next() else {
        return false;
    };
    let Ok(mut stream) = TcpStream::connect_timeout(&addr, PROBE_TIMEOUT) else {
        return false;
    };
    if stream.set_read_timeout(Some(PROBE_TIMEOUT)).is_err()
        || stream.set_write_timeout(Some(PROBE_TIMEOUT)).is_err()
    {
        return false;
    }

    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {authority}\r\nUser-Agent: spice\r\nConnection: close\r\n\r\n"
    );
    if stream.write_all(request.as_bytes()).is_err() {
        return false;
    }

    let mut response = Vec::with_capacity(PROBE_READ_LIMIT);
    let mut chunk = [0_u8; 128];
    while response.len() < PROBE_READ_LIMIT {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(read) => response.extend_from_slice(&chunk[..read]),
            Err(_) => return false,
        }
        if response.contains(&b'\n') {
            break;
        }
    }
    status_line_is_success(&String::from_utf8_lossy(&response))
}

/// Whether an HTTP response begins with a `2xx` status line.
fn status_line_is_success(response: &str) -> bool {
    let Some(line) = response.lines().next() else {
        return false;
    };
    let mut fields = line.split_whitespace();
    let Some(version) = fields.next() else {
        return false;
    };
    if !version.starts_with("HTTP/") {
        return false;
    }
    fields
        .next()
        .and_then(|code| code.parse::<u16>().ok())
        .is_some_and(|code| (200..300).contains(&code))
}

/// Render the unit file for an instance.
///
/// `instance_dir` is baked in as `WorkingDirectory` so the service resolves its
/// spicepod from the directory the operator enrolled, not from wherever systemd
/// happens to start it. `config_dir` preserves the resolved Spice state
/// directory and `spiced_path` is the absolute binary path resolved at install
/// time.
///
/// `account` is `Some` only for a system unit: a user unit is already running
/// as its owner, and systemd refuses `User=` in one.
fn render_unit(
    instance_dir: &Path,
    config_dir: &Path,
    spiced_path: &Path,
    account: Option<ServiceAccount>,
    scope: ServiceScope,
) -> Result<String> {
    use std::fmt::Write as _;

    let instance_dir = escape_systemd_path(instance_dir)?;
    let config_dir = escape_systemd_path(config_dir)?;
    let spiced = escape_systemd_path(spiced_path)?;

    let mut unit = String::from(
        "[Unit]\n\
         Description=Spice runtime connected to Spice Cloud\n\
         Documentation=https://spiceai.org/docs\n\
         After=network-online.target\n\
         Wants=network-online.target\n\
         \n\
         [Service]\n\
         Type=simple\n",
    );
    // Writing into a String is infallible; the Result exists only to satisfy
    // the `Write` trait.
    if let Some(account) = account {
        let _ = writeln!(unit, "User={}", account.uid);
        let _ = writeln!(unit, "Group={}", account.gid);
    }
    let _ = write!(
        unit,
        "WorkingDirectory=\"{instance_dir}\"\n\
         Environment=\"SPICE_CONFIG_DIR={config_dir}\"\n\
         ExecStart=\"{spiced}\"\n\
         # A deployment applies to the running instance and never ends its\n\
         # process, so this is what brings the instance back from the things\n\
         # that do end it — an OOM kill, an unhandled failure. A clean exit is\n\
         # left alone: it is what an operator's `stop` produces.\n\
         Restart=on-failure\n\
         RestartSec=5\n\
         # A crash loop must not be given up on: an instance that lost its\n\
         # network for an hour has to come back on its own.\n\
         StartLimitIntervalSec=0\n\
         KillSignal=SIGTERM\n\
         TimeoutStopSec=30\n\
         \n\
         [Install]\n\
         WantedBy={wanted_by}\n",
        wanted_by = match scope {
            ServiceScope::System => SYSTEM_WANTED_BY,
            ServiceScope::User => USER_WANTED_BY,
        }
    );
    Ok(unit)
}

/// Escape a path for a double-quoted systemd directive value.
///
/// `%` still expands specifiers inside quotes, so it is doubled. Newlines and
/// other control characters are rejected rather than allowed to become new
/// directives, and non-UTF-8 paths are rejected because unit files are UTF-8.
fn escape_systemd_path(path: &Path) -> Result<String> {
    let value = path.to_str().ok_or_else(|| Error::InvalidArgument {
        message: format!(
            "Failed to install the Spice Cloud Connect service: path {} is not valid UTF-8 and cannot be represented safely in a systemd unit.",
            path.display()
        ),
    })?;
    if value.chars().any(char::is_control) {
        return Err(Error::InvalidArgument {
            message: format!(
                "Failed to install the Spice Cloud Connect service: path {} contains a control character and cannot be represented safely in a systemd unit.",
                path.display()
            ),
        });
    }

    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '%' => escaped.push_str("%%"),
            _ => escaped.push(ch),
        }
    }
    Ok(escaped)
}

/// Map a systemd `is-active` word onto the normalized vocabulary.
///
/// systemd's transient states are the reason this mapping exists: reporting
/// `activating` verbatim would make every backend's words part of the public
/// schema.
fn normalize_systemd_state(reported: &str) -> ServiceState {
    match reported.trim() {
        // `reloading` is a running service applying a new configuration.
        "active" | "active (reloading)" | "reloading" => ServiceState::Running,
        "activating" => ServiceState::Starting,
        "deactivating" => ServiceState::Stopping,
        "inactive" => ServiceState::Stopped,
        "failed" => ServiceState::Failed,
        // Everything else, including systemd's own `unknown` for a unit it has
        // no record of and the empty answer of a query that did not run. The
        // definition file is what decides `not_installed`, so a unit systemd
        // cannot account for is a reading, not an absence.
        _ => ServiceState::Unavailable,
    }
}

/// Whether this host can host a service in `scope`.
///
/// A user service needs the account's own manager, which is not running for an
/// account with no session — a `su` shell, or a container that never logged
/// anyone in. Detected here so the refusal comes before anything is written
/// rather than as a `systemctl --user` failure halfway through an install.
fn preflight(scope: ServiceScope) -> std::result::Result<(), PreflightFailure> {
    if !Path::new(SYSTEMD_RUNTIME_MARKER).is_dir() {
        return Err(PreflightFailure::SystemdUnavailable);
    }
    if scope == ServiceScope::User && !user_runtime_dir().is_dir() {
        return Err(PreflightFailure::SystemdUserManagerUnavailable);
    }
    Ok(())
}

/// The runtime directory the account's own systemd manager lives in, which is
/// also the one `systemctl --user` needs to reach it.
fn user_runtime_dir() -> PathBuf {
    std::env::var_os("XDG_RUNTIME_DIR").map_or_else(
        || {
            PathBuf::from(format!(
                "/run/user/{}",
                nix::unistd::Uid::effective().as_raw()
            ))
        },
        PathBuf::from,
    )
}

/// Where units of `scope` live. `None` when the account has no discoverable
/// configuration directory, which is the one case a user unit path cannot be
/// derived from.
fn unit_dir(scope: ServiceScope) -> Option<PathBuf> {
    match scope {
        ServiceScope::System => Some(PathBuf::from(SYSTEMD_UNIT_DIR)),
        ServiceScope::User => Some(dirs::config_dir()?.join(SYSTEMD_USER_UNIT_SUBDIR)),
    }
}

/// The directory holding the `spiced` copy this instance's service runs.
///
/// Per instance, and inside the domain that owns the service: a system service
/// runs a root-owned copy no operator can replace behind the supervisor's back,
/// and a user service runs one out of the account's own data directory. `None`
/// when a user account has no discoverable data directory.
fn runtime_dir(instance_dir: &Path, scope: ServiceScope) -> Option<PathBuf> {
    let stem = super::name_stem_for_dir(instance_dir);
    match scope {
        ServiceScope::System => Some(Path::new(super::RUNTIME_STAGE_DIR).join(stem)),
        ServiceScope::User => Some(dirs::data_local_dir()?.join(USER_RUNTIME_SUBDIR).join(stem)),
    }
}

/// The `spiced` copy this instance's service runs.
fn runtime_path(instance_dir: &Path, scope: ServiceScope) -> Option<PathBuf> {
    Some(runtime_dir(instance_dir, scope)?.join(super::STAGED_RUNTIME_FILE))
}

/// Everything an install writes, resolved before it writes any of it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct InstallPaths {
    unit: PathBuf,
    runtime_dir: PathBuf,
    runtime: PathBuf,
}

/// Resolve where this install will write, or say which directory could not be
/// derived.
fn install_paths(name: &str, instance_dir: &Path, scope: ServiceScope) -> Result<InstallPaths> {
    let missing = |what: &str| Error::CloudConnectIo {
        message: format!(
            "locate the {what} a {scope} service needs: this account has no such directory. Set \
             XDG_CONFIG_HOME and XDG_DATA_HOME, or install a system service with \
             `sudo spice connect service install`."
        ),
    };
    let unit = unit_dir(scope).ok_or_else(|| missing("systemd unit directory"))?;
    let runtime_dir =
        runtime_dir(instance_dir, scope).ok_or_else(|| missing("Spice runtime directory"))?;
    Ok(InstallPaths {
        unit: unit.join(name),
        runtime: runtime_dir.join(super::STAGED_RUNTIME_FILE),
        runtime_dir,
    })
}

/// Prepare the account the service will run as, and give it access to the
/// enrolled identity.
///
/// `None` for a user service: it already runs as the account that owns the
/// state, so there is nothing to resolve and nothing to chown.
fn prepare_account(request: &InstallRequest<'_>) -> Result<Option<ServiceAccount>> {
    if request.scope == ServiceScope::User {
        return Ok(None);
    }
    let account = super::service_account(request.instance_dir)?;
    super::provision_config_ownership(request.config_dir, account)?;
    Ok(Some(account))
}

fn install(host: &dyn SystemdHost, request: &InstallRequest<'_>) -> Result<InstalledService> {
    let name = unit_name_for_dir(request.instance_dir);
    let paths = install_paths(&name, request.instance_dir, request.scope)?;
    let account = prepare_account(request)?;
    install_at(host, request, &name, &paths, account)
}

/// Install into explicitly resolved paths, and leave the previous installation
/// in force if the new one does not come up healthy.
fn install_at(
    host: &dyn SystemdHost,
    request: &InstallRequest<'_>,
    name: &str,
    paths: &InstallPaths,
    account: Option<ServiceAccount>,
) -> Result<InstalledService> {
    ensure_stage_dir(&paths.runtime_dir, request.scope)?;

    // Captured before anything is overwritten: an upgrade that does not come up
    // has to be able to put back exactly what was serving before it.
    let rollback = Rollback::capture(paths)?;

    let applied = apply(host, request, name, paths, account);
    let verdict = match applied {
        Ok(()) => health_gate(host, name, request.scope, request.health_url),
        Err(err) => Err(folded(&err.to_string())),
    };

    match verdict {
        Ok(()) => {
            rollback.discard();
            Ok(InstalledService {
                name: name.to_string(),
                path: paths.unit.clone(),
                working_dir: request.instance_dir.to_path_buf(),
                runtime: paths.runtime.clone(),
            })
        }
        Err(why) => {
            let restored = rollback.restore(host, name, request.scope);
            Err(Error::CloudConnectIo {
                message: format!(
                    "install the Spice Cloud Connect service {name} (systemd): {why}. {restored} \
                     Read what the runtime said with `spice connect service logs -n 200 --dir {dir}`.",
                    dir = request.instance_dir.display(),
                ),
            })
        }
    }
}

/// Stage the runtime, write the unit, and hand the service to systemd.
fn apply(
    host: &dyn SystemdHost,
    request: &InstallRequest<'_>,
    name: &str,
    paths: &InstallPaths,
    account: Option<ServiceAccount>,
) -> Result<()> {
    // systemd reports a unit whose `ExecStart` will not run, and the health
    // gate below is what proves the staged binary actually serves, so the copy
    // needs no separate check.
    super::stage_runtime_at(request.spiced_path, &paths.runtime, |_, _| Ok(()))?;

    let unit = render_unit(
        request.instance_dir,
        request.config_dir,
        &paths.runtime,
        account,
        request.scope,
    )?;
    write_unit(&paths.unit, &unit)?;

    systemctl(host, request.scope, &["daemon-reload"])?;
    // `enable` writes the persistent boot-time link; `restart` is what picks up
    // a rewritten unit and an upgraded binary, and starts a service that was
    // not running. `enable --now` alone would leave the old process up.
    systemctl(host, request.scope, &["enable", name])?;
    systemctl(host, request.scope, &["restart", name])?;

    if request.scope == ServiceScope::User {
        // Best effort: a policy that refuses lingering is reported by `status`
        // as `login_only` with the command to run, not as a failed install.
        let _ = enable_linger(host);
    }
    Ok(())
}

/// Wait for the service to prove it is serving, or say what it is doing
/// instead.
///
/// Two things count as proof, and both have to be here. Answering the health
/// URL is the strongest, and ends the gate as soon as it happens. Staying
/// `active` without interruption for [`SETTLE_ATTEMPTS`] polls is the other,
/// and it is what a healthy instance whose endpoint this CLI cannot reach
/// looks like: the recorded health URL is the address the *CLI* was pointed at,
/// while the service serves whatever its spicepod configures, so refusing an
/// install because a probe went unanswered would fail an instance that is
/// working. What neither accepts is the failure this gate exists for — a
/// runtime that exits and is restarted, which never accumulates an
/// uninterrupted run and never answers.
fn health_gate(
    host: &dyn SystemdHost,
    name: &str,
    scope: ServiceScope,
    health_url: &str,
) -> std::result::Result<(), String> {
    let probeable = is_probeable(health_url);
    let mut failures = 0;
    let mut settled = 0;
    let mut last = format!(
        "systemd did not report {name} as running within {}s",
        HEALTH_GATE.as_secs()
    );

    for attempt in 0..HEALTH_ATTEMPTS {
        if attempt > 0 {
            host.sleep(HEALTH_POLL_INTERVAL);
        }
        let reported = is_active(host, name, scope).unwrap_or_default();
        match normalize_systemd_state(&reported) {
            ServiceState::Running => {
                failures = 0;
                settled += 1;
                if probeable && host.probe_health(health_url) {
                    return Ok(());
                }
                if settled >= SETTLE_ATTEMPTS {
                    return Ok(());
                }
                last = format!(
                    "{name} did not stay running for {}s, and did not answer {health_url}",
                    settle_window().as_secs()
                );
            }
            ServiceState::Failed => {
                settled = 0;
                failures += 1;
                last = format!("systemd reports {name} as failed");
                if failures >= FAILED_READINGS_BEFORE_GIVING_UP {
                    return Err(last);
                }
            }
            other => {
                settled = 0;
                failures = 0;
                last = format!(
                    "systemd reports {name} as {other} rather than running, {}s after it was \
                     started",
                    HEALTH_GATE.as_secs()
                );
            }
        }
    }
    Err(last)
}

/// How long a service has to run without interruption to count as up.
fn settle_window() -> Duration {
    SETTLE_ATTEMPTS * HEALTH_POLL_INTERVAL
}

/// What an install has to be able to put back.
///
/// The unit and the runtime together decide what the host runs, so both are
/// captured: restoring one without the other would leave a unit pointing at a
/// binary that is no longer the one it was written for.
struct Rollback {
    unit: PathBuf,
    /// The unit file that was in force, or `None` when nothing was installed.
    previous_unit: Option<Vec<u8>>,
    runtime: PathBuf,
    /// A second name for the runtime that was in force, or `None` when there
    /// was none.
    previous_runtime: Option<PathBuf>,
}

impl Rollback {
    /// The name the previous runtime is held under while the new one is
    /// installed.
    fn backup_name(runtime: &Path) -> PathBuf {
        runtime.with_extension("previous")
    }

    /// Capture what is in force now.
    ///
    /// The runtime is captured by hard link where the filesystem allows one —
    /// the publish that follows replaces the directory entry rather than the
    /// bytes, so a second name preserves the old binary for nothing — and by
    /// copy where it does not. A capture that cannot be made fails the install
    /// rather than proceeding: an upgrade nobody can undo is exactly the one
    /// that must not start.
    fn capture(paths: &InstallPaths) -> Result<Self> {
        let previous_unit = std::fs::read(&paths.unit).ok();
        let previous_runtime = if paths.runtime.is_file() {
            let backup = Self::backup_name(&paths.runtime);
            let _ = std::fs::remove_file(&backup);
            if std::fs::hard_link(&paths.runtime, &backup).is_err() {
                std::fs::copy(&paths.runtime, &backup).map_err(|e| Error::CloudConnectIo {
                    message: format!(
                        "keep a copy of the Spice runtime {} before upgrading it: {e}. The \
                         upgrade was not started, so the installed service is untouched.",
                        paths.runtime.display()
                    ),
                })?;
            }
            Some(backup)
        } else {
            None
        };
        Ok(Self {
            unit: paths.unit.clone(),
            previous_unit,
            runtime: paths.runtime.clone(),
            previous_runtime,
        })
    }

    /// Let go of the captured runtime, after the new one proved healthy.
    fn discard(&self) {
        if let Some(backup) = &self.previous_runtime {
            let _ = std::fs::remove_file(backup);
        }
    }

    /// Put back what was in force, and describe what an operator is left with.
    ///
    /// Best effort by design: this already runs on a failure path, and a
    /// restoration step that also fails must not replace the diagnosis of the
    /// original failure with its own.
    fn restore(&self, host: &dyn SystemdHost, name: &str, scope: ServiceScope) -> String {
        match &self.previous_unit {
            Some(unit) => {
                let _ = write_unit_bytes(&self.unit, unit);
            }
            None => {
                let _ = std::fs::remove_file(&self.unit);
            }
        }
        match &self.previous_runtime {
            Some(backup) => {
                let _ = std::fs::rename(backup, &self.runtime);
            }
            None => {
                let _ = std::fs::remove_file(&self.runtime);
            }
        }
        // The stamp describes the source the staged runtime was copied from,
        // and the restored binary did not come from it. Dropping it is what
        // makes the next install copy again instead of trusting a stamp that
        // now describes the runtime this rollback just removed.
        let _ = std::fs::remove_file(self.runtime.with_extension("stamp"));

        let _ = systemctl(host, scope, &["daemon-reload"]);
        if self.previous_unit.is_some() {
            let _ = systemctl(host, scope, &["restart", name]);
            "The service and runtime that were installed before have been put back.".to_string()
        } else {
            let _ = systemctl(host, scope, &["disable", "--now", name]);
            "Nothing was left installed for this directory.".to_string()
        }
    }
}

/// Create the directory a service's runtime is staged into, and refuse it
/// unless only the domain that owns the service can change what it holds.
fn ensure_stage_dir(dir: &Path, scope: ServiceScope) -> Result<()> {
    match scope {
        ServiceScope::System => super::ensure_root_only_dir(dir),
        ServiceScope::User => ensure_account_only_dir(dir),
    }
}

/// Create `dir` if absent and refuse it unless this account alone can change
/// what it holds.
///
/// The leaf is what is checked, not the whole path to it: everything above a
/// user's data directory is the account's own home, and anyone who can write
/// that can already replace the `spiced` on the operator's `PATH`. What this
/// rules out is the thing that is not implied — a staging directory shared with
/// another account, or a symlink pointing the service's binary somewhere this
/// check cannot vouch for.
fn ensure_account_only_dir(dir: &Path) -> Result<()> {
    use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _, PermissionsExt as _};

    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o755)
        .create(dir)
        .map_err(|e| Error::CloudConnectIo {
            message: format!("create {}: {e}", dir.display()),
        })?;

    let meta = std::fs::symlink_metadata(dir).map_err(|e| Error::CloudConnectIo {
        message: format!("inspect {}: {e}", dir.display()),
    })?;
    let uid = nix::unistd::Uid::effective().as_raw();
    let mode = meta.permissions().mode() & 0o7777;
    if meta.file_type().is_symlink() || meta.uid() != uid || mode & 0o022 != 0 {
        return Err(Error::InvalidArgument {
            message: format!(
                "Failed to install the Spice Cloud Connect service: {dir} must be a real \
                 directory owned by this account (uid {uid}) that no other account can write, so \
                 the runtime the service executes cannot be replaced behind it. Fix it \
                 (`chown {uid} {dir}` and `chmod go-w {dir}`) and re-run \
                 `spice connect service install`.",
                dir = dir.display(),
            ),
        });
    }
    Ok(())
}

/// Write a unit file, creating its directory and replacing it atomically.
fn write_unit(path: &Path, unit: &str) -> Result<()> {
    write_unit_bytes(path, unit.as_bytes())
}

fn write_unit_bytes(path: &Path, unit: &[u8]) -> Result<()> {
    use std::os::unix::fs::OpenOptionsExt as _;

    let io_error = |e: std::io::Error| Error::CloudConnectIo {
        message: format!("write systemd unit {}: {e}", path.display()),
    };

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(io_error)?;
    }

    // A sibling that is renamed into place, so systemd never reads half a unit,
    // and an explicit mode, so the file does not inherit whatever the process
    // umask happens to be.
    let staging = path.with_extension("service.incoming");
    let _ = std::fs::remove_file(&staging);
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(UNIT_MODE)
        .open(&staging)
        .map_err(io_error)?;
    file.write_all(unit).map_err(io_error)?;
    file.sync_all().map_err(io_error)?;
    drop(file);

    std::fs::rename(&staging, path).map_err(|e| {
        let _ = std::fs::remove_file(&staging);
        io_error(e)
    })
}

/// Stop, disable, and delete the unit the manifest describes, together with the
/// runtime staged for it.
///
/// Stop/disable failures are tolerated — a unit file left on disk would restart
/// a service against a released identity forever, so the deletion is what must
/// happen. The journal is left to systemd's own retention.
fn uninstall(host: &dyn SystemdHost, manifest: &ServiceManifest) -> Result<()> {
    ensure_authorized(manifest, "uninstall")?;

    // Best-effort: a unit that is already stopped, already disabled, or whose
    // systemd is not running must not block removing the file.
    if let Err(err) = systemctl(host, manifest.scope, &["disable", "--now", &manifest.name]) {
        tracing::debug!("systemctl disable --now {}: {err}", manifest.name);
    }

    match std::fs::remove_file(&manifest.definition_path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(Error::CloudConnectIo {
                message: format!(
                    "remove systemd unit {}: {e}. The service would keep restarting against a \
                     released identity — delete the file and run `{}`.",
                    manifest.definition_path.display(),
                    systemctl_command(manifest.scope, &["daemon-reload"]),
                ),
            });
        }
    }

    remove_staged_runtime(manifest);

    if let Err(err) = systemctl(host, manifest.scope, &["daemon-reload"]) {
        tracing::debug!("systemctl daemon-reload: {err}");
    }

    Ok(())
}

/// The staging directory holding the runtime this service — and only this
/// service — executes.
///
/// `None` when the manifest records a runtime somewhere else, which describes a
/// binary this command cannot prove is unshared: one uninstall must never take
/// out another service's runtime.
fn owned_runtime_dir(manifest: &ServiceManifest) -> Option<PathBuf> {
    let expected = runtime_dir(&manifest.directory, manifest.scope)?;
    (manifest.runtime_path.parent() == Some(expected.as_path())).then_some(expected)
}

/// Delete the runtime staged for this service, and nothing else.
fn remove_staged_runtime(manifest: &ServiceManifest) {
    let Some(owned) = owned_runtime_dir(manifest) else {
        tracing::debug!(
            "leaving {} in place: it is not this service's own staged runtime",
            manifest.runtime_path.display()
        );
        return;
    };
    if let Err(e) = std::fs::remove_dir_all(&owned)
        && e.kind() != std::io::ErrorKind::NotFound
    {
        tracing::debug!("remove staged runtime {}: {e}", owned.display());
    }
}

/// Start an installed service and confirm it came up.
fn start(host: &dyn SystemdHost, manifest: &ServiceManifest) -> Result<()> {
    ensure_authorized(manifest, "start")?;
    systemctl_action(host, manifest, "start", &["start", &manifest.name])?;
    await_state(host, manifest, "start", ServiceState::Running)
}

/// Stop a running service and confirm it went down, leaving it installed.
fn stop(host: &dyn SystemdHost, manifest: &ServiceManifest) -> Result<()> {
    ensure_authorized(manifest, "stop")?;
    systemctl_action(host, manifest, "stop", &["stop", &manifest.name])?;
    await_state(host, manifest, "stop", ServiceState::Stopped)
}

/// Restart through the supervisor and wait for the service to come back.
///
/// Never asks `spiced` to exit itself: the supervisor owns the stop and the
/// start, so a restart that fails is systemd's failure to report rather than a
/// runtime that quietly went away.
fn restart(host: &dyn SystemdHost, manifest: &ServiceManifest) -> Result<()> {
    ensure_authorized(manifest, "restart")?;
    systemctl_action(host, manifest, "restart", &["restart", &manifest.name])?;
    await_state(host, manifest, "restart", ServiceState::Running)
}

/// Wait for the unit to reach `wanted`, or report what it is doing instead.
///
/// `systemctl` returns once its job is done, so this only covers a unit that is
/// still settling — and it is what turns "the command exited zero" into "the
/// service is in the state you asked for".
fn await_state(
    host: &dyn SystemdHost,
    manifest: &ServiceManifest,
    action: &str,
    wanted: ServiceState,
) -> Result<()> {
    let mut observed = ServiceState::Unavailable;
    for attempt in 0..LIFECYCLE_ATTEMPTS {
        if attempt > 0 {
            host.sleep(LIFECYCLE_POLL_INTERVAL);
        }
        let reported = is_active(host, &manifest.name, manifest.scope).unwrap_or_default();
        observed = normalize_systemd_state(&reported);
        if observed == wanted {
            return Ok(());
        }
        // A unit stopped after a failure stays `failed`, which is systemd's
        // word for "not running, and it did not end cleanly". The service is
        // down, which is what a stop asked for.
        if wanted == ServiceState::Stopped && observed == ServiceState::Failed {
            return Ok(());
        }
    }

    Err(Error::CloudConnectIo {
        message: format!(
            "{action} the Spice Cloud Connect service {name} (systemd): systemd reports it as \
             {observed} rather than {wanted}. Inspect it with `{status}` and \
             `spice connect service logs -n 200 --dir {dir}`.",
            name = manifest.name,
            status = systemctl_command(manifest.scope, &["status", &manifest.name]),
            dir = manifest.directory.display(),
        ),
    })
}

/// Print the service's output from the journal, bounded and without a pager.
fn logs(host: &dyn SystemdHost, manifest: &ServiceManifest, request: LogRequest) -> Result<()> {
    let args = journal_args(&manifest.name, manifest.scope, request);
    let args: Vec<&str> = args.iter().map(String::as_str).collect();

    if request.follow {
        // Streamed rather than captured: the snapshot systemd prints first and
        // everything after it has to reach the terminal as it arrives, and it
        // does not end until the viewer is interrupted.
        return match host.stream(JOURNALCTL, &args) {
            // Ended by a signal, or by the SIGINT that also reached this
            // process: the viewer stopped, the service did not change.
            Ok(None | Some(130)) => Err(Error::Interrupted),
            Ok(Some(0)) => Ok(()),
            Ok(Some(code)) => Err(journal_failure(
                manifest,
                &format!("`journalctl` exited with status {code}"),
            )),
            Err(e) => Err(journal_failure(manifest, &format!("run `journalctl`: {e}"))),
        };
    }

    let output = host
        .output(JOURNALCTL, &args)
        .map_err(|e| journal_failure(manifest, &format!("run `journalctl`: {e}")))?;
    if !output.success {
        return Err(journal_failure(manifest, &output.describe_failure()));
    }

    if output.stdout.trim().is_empty() {
        println!("No logs yet for {}.", manifest.name);
        if manifest.scope == ServiceScope::System && !super::is_root() {
            println!(
                "If this account cannot read the system journal, retry with \
                 `sudo spice connect service logs --dir {}`.",
                manifest.directory.display()
            );
        }
        return Ok(());
    }

    print!("{}", output.stdout);
    if !output.stdout.ends_with('\n') {
        println!();
    }
    Ok(())
}

/// The exact `journalctl` invocation for one service.
///
/// Scoped to the unit by exact name, bounded by `-n`, and never paged: a log
/// command that opens a pager cannot be used from a script, and one that is
/// unbounded prints whatever the host has kept.
fn journal_args(name: &str, scope: ServiceScope, request: LogRequest) -> Vec<String> {
    let mut args: Vec<String> = Vec::with_capacity(9);
    if scope == ServiceScope::User {
        args.push("--user".to_string());
    }
    args.extend([
        "-u".to_string(),
        name.to_string(),
        "--no-pager".to_string(),
        "-q".to_string(),
        "-o".to_string(),
        "cat".to_string(),
        "-n".to_string(),
        request.number.to_string(),
    ]);
    if request.follow {
        args.push("-f".to_string());
    }
    args
}

/// The error a journal read fails with, naming the elevation it may need
/// rather than performing it.
fn journal_failure(manifest: &ServiceManifest, reason: &str) -> Error {
    let retry = match manifest.scope {
        ServiceScope::System if !super::is_root() => format!(
            " If this account cannot read the system journal, retry with \
             `sudo spice connect service logs --dir {}`.",
            manifest.directory.display()
        ),
        _ => String::new(),
    };
    Error::CloudConnectIo {
        message: format!(
            "read the logs of the Spice Cloud Connect service {name} (systemd): {reason}.{retry}",
            name = manifest.name,
        ),
    }
}

/// Refuse an action this invocation cannot perform, naming the exact retry.
///
/// Spice never elevates on its own, and never drives a user service through
/// `sudo`: root's user manager is a different manager from the operator's, so a
/// `sudo systemctl --user` would report on a service that is not the installed
/// one.
fn ensure_authorized(manifest: &ServiceManifest, action: &str) -> Result<()> {
    let effective = nix::unistd::Uid::effective().as_raw();
    match manifest.scope {
        ServiceScope::System if effective != 0 => Err(Error::InvalidArgument {
            message: format!(
                "Failed to {action} the Spice Cloud Connect service {name} (systemd): a system \
                 service is managed with root privileges, and Spice never elevates on its own. \
                 Retry with `sudo spice connect service {action} --dir {dir}`. Nothing was \
                 changed. See: https://spiceai.org/docs",
                name = manifest.name,
                dir = manifest.directory.display(),
            ),
        }),
        ServiceScope::User if effective != manifest.owner.uid => Err(Error::InvalidArgument {
            message: format!(
                "Failed to {action} the Spice Cloud Connect service {name} (systemd): a user \
                 service is managed by the account that owns it ({owner}), and running this \
                 through sudo would target root's user manager instead of that account's. Run \
                 `spice connect service {action} --dir {dir}` as {owner}. Nothing was changed. \
                 See: https://spiceai.org/docs",
                name = manifest.name,
                owner = manifest.owner.describe(),
                dir = manifest.directory.display(),
            ),
        }),
        _ => Ok(()),
    }
}

/// The service installed for `instance_dir` in `scope`, if the definition under
/// this directory's derived name names this directory as its working directory.
///
/// The working-directory check is what makes this a verification rather than a
/// search: a unit left behind by a directory that has since moved carries the
/// same derived name, and taking it over would control an instance nobody asked
/// about.
pub(super) fn find_for_dir(instance_dir: &Path, scope: ServiceScope) -> Option<InstalledService> {
    let name = unit_name_for_dir(instance_dir);
    let path = unit_dir(scope)?.join(&name);
    if !path.is_file() {
        return None;
    }
    let unit = std::fs::read_to_string(&path).ok()?;
    if parse_working_dir(&unit)? != instance_dir {
        return None;
    }
    let runtime = parse_exec_runtime(&unit)
        .or_else(|| runtime_path(instance_dir, scope))
        .unwrap_or_default();
    Some(InstalledService {
        name,
        path,
        working_dir: instance_dir.to_path_buf(),
        runtime,
    })
}

/// Parse the `WorkingDirectory=` value out of a rendered unit.
fn parse_working_dir(unit: &str) -> Option<PathBuf> {
    parse_systemd_word(unit_directive(unit, "WorkingDirectory=")?).map(PathBuf::from)
}

/// Parse the binary `ExecStart=` runs out of a rendered unit, dropping its
/// arguments. Lets `status` report which runtime an installed service is
/// actually running, including a unit written before the runtime moved.
fn parse_exec_runtime(unit: &str) -> Option<PathBuf> {
    let exec = unit_directive(unit, "ExecStart=")?;
    // systemd allows `-`/`@`/`+` prefixes on the executable; strip them so the
    // reported path is the binary itself.
    let exec = exec.trim_start_matches(['-', '@', '+', '!', ':']);
    parse_systemd_word(exec).map(PathBuf::from)
}

/// Parse the first systemd word emitted by [`escape_systemd_path`], while also
/// accepting the unquoted values written by older Spice releases.
fn parse_systemd_word(value: &str) -> Option<String> {
    let value = value.trim_start();
    let word = if let Some(rest) = value.strip_prefix('"') {
        let mut word = String::new();
        let mut escaped = false;
        let mut closed = false;
        for ch in rest.chars() {
            if escaped {
                word.push(ch);
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                closed = true;
                break;
            } else {
                word.push(ch);
            }
        }
        if escaped || !closed {
            return None;
        }
        word
    } else {
        value.split_whitespace().next()?.to_string()
    };
    if word.is_empty() {
        return None;
    }

    // `%%` is how the renderer writes a literal percent sign. Preserve a lone
    // `%` from a hand-written/legacy unit rather than guessing at a specifier.
    let mut decoded = String::with_capacity(word.len());
    let mut chars = word.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '%' && chars.peek() == Some(&'%') {
            let _ = chars.next();
        }
        decoded.push(ch);
    }
    Some(decoded)
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
fn is_active(host: &dyn SystemdHost, unit_name: &str, scope: ServiceScope) -> Option<String> {
    // `is-active` exits non-zero for anything but `active`, and prints the
    // state either way — so read stdout regardless of the exit status.
    let state = systemctl_query(host, scope, &["is-active", unit_name])?;
    (!state.is_empty()).then_some(state)
}

/// Observe the unit: the state systemd reports plus whether it will come back
/// on its own.
fn observe(host: &dyn SystemdHost, manifest: &ServiceManifest) -> ServiceObservation {
    let Some(reported) = is_active(host, &manifest.name, manifest.scope) else {
        return ServiceObservation::unavailable(format!(
            "systemctl could not be asked about {}. Check that systemd is running and that this \
             account may query it ({}).",
            manifest.name,
            systemctl_command(manifest.scope, &["is-active", &manifest.name])
        ));
    };
    let (starts, starts_action) = observe_persistence(host, manifest);
    let state = normalize_systemd_state(&reported);
    ServiceObservation {
        state,
        starts,
        // An `unavailable` state always says why. Carrying systemd's own answer
        // is what makes the report actionable: `unknown` means systemd has no
        // record of a unit whose file is on disk, which is a different repair
        // from a word this release does not recognise.
        diagnostic: (state == ServiceState::Unavailable)
            .then(|| unrecognized_state_diagnostic(&reported, manifest)),
        starts_action,
    }
}

/// Why a state Spice does not recognise is reported as `unavailable`, naming
/// systemd's own answer and the command that produced it.
fn unrecognized_state_diagnostic(reported: &str, manifest: &ServiceManifest) -> String {
    format!(
        "`{command}` answered `{reported}`, which is not a state Spice can act on. Run it to \
         see what systemd reports for this unit.",
        command = systemctl_command(manifest.scope, &["is-active", &manifest.name]),
        reported = reported.trim(),
    )
}

/// Whether the unit is enabled — and, for a user unit, whether its account
/// lingers — translated into the operator outcome.
fn observe_persistence(
    host: &dyn SystemdHost,
    manifest: &ServiceManifest,
) -> (ServiceStarts, Option<String>) {
    let Some(reported) = systemctl_query(host, manifest.scope, &["is-enabled", &manifest.name])
    else {
        return (ServiceStarts::Unavailable, None);
    };
    let lingers = match manifest.scope {
        ServiceScope::System => None,
        ServiceScope::User => account_lingers(host, &manifest.owner.describe()),
    };
    classify_persistence(&reported, lingers, manifest)
}

/// [`observe_persistence`] without the process: the classification of what
/// `systemctl is-enabled` and `loginctl` answered, so every branch is testable
/// on a host that has no systemd.
///
/// Only a plain `enabled` establishes persistence. `enabled-runtime` is
/// deliberately not enough: its link lives under `/run` and is gone after a
/// reboot, so reporting boot persistence for it would promise exactly the thing
/// that will not happen. Everything else systemd can answer here — `disabled`,
/// `linked`, `static`, `indirect`, `alias`, `generated` — leaves the unit
/// without a persistent boot-time link too, so all of them report `disabled`
/// with the command that fixes it.
///
/// A *masked* unit cannot be enabled until it is unmasked, so its remediation
/// says so rather than printing an `enable` that is guaranteed to fail.
///
/// An enabled system unit comes up at boot with nobody logged in. An enabled
/// *user* unit comes up when its owner's manager starts, which is at login and
/// no earlier unless that account lingers — so `lingers` decides between the
/// two operator outcomes, and an account whose lingering could not be read is
/// reported as `login_only` with the command that settles it rather than as a
/// boot-persistence claim that may not hold.
fn classify_persistence(
    reported: &str,
    lingers: Option<bool>,
    manifest: &ServiceManifest,
) -> (ServiceStarts, Option<String>) {
    let enable = || systemctl_command(manifest.scope, &["enable", &manifest.name]);
    match reported.trim() {
        "enabled" => match manifest.scope {
            ServiceScope::System => (ServiceStarts::BootWithoutLogin, None),
            ServiceScope::User if lingers == Some(true) => (ServiceStarts::BootWithoutLogin, None),
            ServiceScope::User => (
                ServiceStarts::LoginOnly,
                Some(format!(
                    "loginctl enable-linger {}",
                    manifest.owner.describe()
                )),
            ),
        },
        "masked" | "masked-runtime" => (
            ServiceStarts::Disabled,
            Some(format!(
                "{} && {}",
                systemctl_command(manifest.scope, &["unmask", &manifest.name]),
                enable()
            )),
        ),
        // Includes the empty answer of a query that ran and said nothing, which
        // is not a state to invent an outcome for.
        "" => (ServiceStarts::Unavailable, None),
        _ => (ServiceStarts::Disabled, Some(enable())),
    }
}

/// Ask logind to keep this account's manager running with nobody logged in,
/// and report what it did.
///
/// The verification is the point: `enable-linger` can be refused by policy, and
/// an install that assumed it worked would promise boot persistence the host
/// will not deliver.
fn enable_linger(host: &dyn SystemdHost) -> Option<bool> {
    let account = super::account_name(nix::unistd::Uid::effective().as_raw())?;
    let _ = host.output(LOGINCTL, &["enable-linger", &account]);
    account_lingers(host, &account)
}

/// Whether logind reports `account` as lingering. `None` when it could not be
/// asked, which is not the same as an account that does not linger.
fn account_lingers(host: &dyn SystemdHost, account: &str) -> Option<bool> {
    let output = host
        .output(
            LOGINCTL,
            &["show-user", account, "--property=Linger", "--value"],
        )
        .ok()?;
    if !output.success {
        return None;
    }
    match output.stdout.trim() {
        "yes" => Some(true),
        "no" => Some(false),
        _ => None,
    }
}

fn recovery_hints(manifest: &ServiceManifest) -> Vec<String> {
    let scope = manifest.scope;
    let name = &manifest.name;
    vec![
        systemctl_command(scope, &["status", name]),
        match scope {
            ServiceScope::System => format!("journalctl -u {name} -f"),
            ServiceScope::User => format!("journalctl --user -u {name} -f"),
        },
    ]
}

/// The `--user` flag every `systemctl` invocation for a user service needs.
fn scope_args(scope: ServiceScope) -> &'static [&'static str] {
    match scope {
        ServiceScope::System => &[],
        ServiceScope::User => &["--user"],
    }
}

/// The command line as an operator would type it, for messages that ask them
/// to run it themselves.
fn systemctl_command(scope: ServiceScope, args: &[&str]) -> String {
    let mut parts = Vec::with_capacity(args.len() + 2);
    if scope == ServiceScope::System && !super::is_root() {
        parts.push("sudo");
    }
    parts.push(SYSTEMCTL);
    parts.extend(scope_args(scope));
    parts.extend(args);
    parts.join(" ")
}

/// Ask `systemctl` a question and return its trimmed stdout.
///
/// The exit status is ignored on purpose: the query commands report their
/// answer on stdout and use the exit status to encode that answer. `None`
/// means `systemctl` could not be run at all.
fn systemctl_query(host: &dyn SystemdHost, scope: ServiceScope, args: &[&str]) -> Option<String> {
    let mut all = scope_args(scope).to_vec();
    all.extend_from_slice(args);
    let output = host.output(SYSTEMCTL, &all).ok()?;
    Some(output.stdout.trim().to_string())
}

/// Run `systemctl <args>`, turning a non-zero exit into an error carrying
/// systemd's own stderr — which names the actual problem far better than an
/// exit code.
fn systemctl(host: &dyn SystemdHost, scope: ServiceScope, args: &[&str]) -> Result<()> {
    let mut all = scope_args(scope).to_vec();
    all.extend_from_slice(args);
    let output = host
        .output(SYSTEMCTL, &all)
        .map_err(|e| Error::CloudConnectIo {
            message: format!("run `{}`: {e}", systemctl_command(scope, args)),
        })?;

    if output.success {
        return Ok(());
    }

    Err(Error::CloudConnectIo {
        message: format!(
            "`{}` failed: {}",
            systemctl_command(scope, args),
            output.describe_failure()
        ),
    })
}

/// [`systemctl`] for a lifecycle action, adding the retry an authorization
/// refusal needs.
///
/// The pre-flight authorization check catches the usual case, but polkit can
/// refuse a root-equivalent invocation too — and the answer is still a command
/// the operator runs, never an elevation this CLI performs.
fn systemctl_action(
    host: &dyn SystemdHost,
    manifest: &ServiceManifest,
    action: &str,
    args: &[&str],
) -> Result<()> {
    systemctl(host, manifest.scope, args).map_err(|err| {
        let denied = ["denied", "not authorized", "authentication", "permitted"]
            .iter()
            .any(|word| err.to_string().to_ascii_lowercase().contains(word));
        let retry = if denied && manifest.scope == ServiceScope::System {
            format!(
                " Retry with `sudo spice connect service {action} --dir {}`.",
                manifest.directory.display()
            )
        } else {
            String::new()
        };
        Error::CloudConnectIo {
            message: format!(
                "{action} the Spice Cloud Connect service {name} (systemd): {reason}.{retry}",
                name = manifest.name,
                reason = folded(&err.to_string()),
            ),
        }
    })
}

/// One line, whatever the source said.
///
/// Supervisor output arrives with newlines in it, and a log line that carries
/// them is two records that only one of which can be searched for.
fn folded(message: &str) -> String {
    message.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The Linux back end.
pub(super) struct SystemdBackend;

impl ServiceBackend for SystemdBackend {
    fn supervisor(&self) -> Supervisor {
        Supervisor::Systemd
    }

    fn preflight(&self, scope: ServiceScope) -> std::result::Result<(), PreflightFailure> {
        preflight(scope)
    }

    fn name_for_dir(&self, instance_dir: &Path) -> String {
        unit_name_for_dir(instance_dir)
    }

    fn definition_path(&self, name: &str, scope: ServiceScope) -> PathBuf {
        unit_dir(scope)
            .unwrap_or_else(|| PathBuf::from(SYSTEMD_UNIT_DIR))
            .join(name)
    }

    fn log_source(&self, name: &str, scope: ServiceScope) -> Option<LogSource> {
        Some(LogSource::Journal {
            unit: name.to_string(),
            scope,
        })
    }

    fn find_installed(&self, instance_dir: &Path, scope: ServiceScope) -> Option<InstalledService> {
        find_for_dir(instance_dir, scope)
    }

    fn install(&self, request: &InstallRequest<'_>) -> Result<InstalledService> {
        install(&ProcessHost, request)
    }

    fn uninstall(&self, manifest: &ServiceManifest) -> Result<()> {
        uninstall(&ProcessHost, manifest)
    }

    fn start(&self, manifest: &ServiceManifest) -> Result<()> {
        start(&ProcessHost, manifest)
    }

    fn stop(&self, manifest: &ServiceManifest) -> Result<()> {
        stop(&ProcessHost, manifest)
    }

    fn restart(&self, manifest: &ServiceManifest) -> Result<()> {
        restart(&ProcessHost, manifest)
    }

    fn observe(&self, manifest: &ServiceManifest) -> ServiceObservation {
        observe(&ProcessHost, manifest)
    }

    fn logs(&self, manifest: &ServiceManifest, request: LogRequest) -> Result<()> {
        logs(&ProcessHost, manifest, request)
    }

    fn recovery_hints(&self, manifest: &ServiceManifest) -> Vec<String> {
        recovery_hints(manifest)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::{HashMap, VecDeque};

    use super::*;

    /// A host whose every answer is scripted, so the lifecycle can be exercised
    /// against states no test machine can be put into on demand — a service
    /// that never becomes healthy, a `loginctl` that refuses to linger, a
    /// journal that cannot be read.
    struct ScriptedHost {
        answers: RefCell<HashMap<String, VecDeque<CommandOutput>>>,
        health: RefCell<VecDeque<bool>>,
        calls: RefCell<Vec<String>>,
        streamed: RefCell<Vec<String>>,
        stream_code: Option<i32>,
        slept: RefCell<Duration>,
    }

    impl ScriptedHost {
        fn new() -> Self {
            Self {
                answers: RefCell::new(HashMap::new()),
                health: RefCell::new(VecDeque::new()),
                calls: RefCell::new(Vec::new()),
                streamed: RefCell::new(Vec::new()),
                stream_code: Some(0),
                slept: RefCell::new(Duration::ZERO),
            }
        }

        /// The command line as this host keys its answers by.
        fn key(program: &str, args: &[&str]) -> String {
            if args.is_empty() {
                program.to_string()
            } else {
                format!("{program} {}", args.join(" "))
            }
        }

        /// Answer `command` with `stdout`, repeating the last answer once the
        /// scripted ones run out.
        fn says(self, command: &str, stdout: &str) -> Self {
            self.sequence(command, &[stdout])
        }

        fn sequence(self, command: &str, stdouts: &[&str]) -> Self {
            let answers = stdouts
                .iter()
                .map(|stdout| CommandOutput {
                    success: true,
                    code: Some(0),
                    stdout: (*stdout).to_string(),
                    stderr: String::new(),
                })
                .collect();
            self.answers
                .borrow_mut()
                .insert(command.to_string(), answers);
            self
        }

        fn fails(self, command: &str, stderr: &str) -> Self {
            self.answers.borrow_mut().insert(
                command.to_string(),
                VecDeque::from(vec![CommandOutput {
                    success: false,
                    code: Some(1),
                    stdout: String::new(),
                    stderr: stderr.to_string(),
                }]),
            );
            self
        }

        fn health(self, verdicts: &[bool]) -> Self {
            *self.health.borrow_mut() = verdicts.iter().copied().collect();
            self
        }

        fn streaming(mut self, code: Option<i32>) -> Self {
            self.stream_code = code;
            self
        }

        fn calls(&self) -> Vec<String> {
            self.calls.borrow().clone()
        }

        fn ran(&self, command: &str) -> bool {
            self.calls.borrow().iter().any(|call| call == command)
        }
    }

    impl SystemdHost for ScriptedHost {
        fn output(&self, program: &str, args: &[&str]) -> std::io::Result<CommandOutput> {
            let key = Self::key(program, args);
            self.calls.borrow_mut().push(key.clone());
            let mut answers = self.answers.borrow_mut();
            let Some(queued) = answers.get_mut(&key) else {
                return Ok(CommandOutput {
                    success: true,
                    code: Some(0),
                    stdout: String::new(),
                    stderr: String::new(),
                });
            };
            // The last scripted answer stands for every later ask, so a poll
            // that settles does not need one answer per attempt.
            let answer = if queued.len() > 1 {
                queued.pop_front()
            } else {
                queued.front().cloned()
            };
            Ok(answer.expect("a scripted command always has an answer"))
        }

        fn stream(&self, program: &str, args: &[&str]) -> std::io::Result<Option<i32>> {
            self.streamed.borrow_mut().push(Self::key(program, args));
            Ok(self.stream_code)
        }

        fn probe_health(&self, _url: &str) -> bool {
            let mut health = self.health.borrow_mut();
            if health.len() > 1 {
                health.pop_front().unwrap_or(false)
            } else {
                health.front().copied().unwrap_or(false)
            }
        }

        fn sleep(&self, duration: Duration) {
            *self.slept.borrow_mut() += duration;
        }
    }

    const TEST_ACCOUNT: ServiceAccount = ServiceAccount {
        uid: 1000,
        gid: 1001,
    };

    const HEALTH_URL: &str = "http://127.0.0.1:8090/health";

    fn rendered(instance_dir: &str, config_dir: &str, spiced_path: &str) -> String {
        render_unit(
            Path::new(instance_dir),
            Path::new(config_dir),
            Path::new(spiced_path),
            Some(TEST_ACCOUNT),
            ServiceScope::System,
        )
        .expect("test paths are safe systemd values")
    }

    /// A manifest for the unit under test, without touching the filesystem.
    ///
    /// The owner is this account, so the authorization check lets a user-scope
    /// lifecycle action through; the tests that exercise a refusal say so.
    fn manifest(scope: ServiceScope) -> ServiceManifest {
        let name = unit_name_for_dir(Path::new("/opt/edge-1"));
        let uid = nix::unistd::Uid::effective().as_raw();
        ServiceManifest {
            schema_version: super::super::manifest::MANIFEST_SCHEMA_VERSION,
            directory: PathBuf::from("/opt/edge-1"),
            name: name.clone(),
            scope,
            supervisor: Supervisor::Systemd,
            owner: super::super::ServiceOwner {
                uid,
                gid: uid,
                name: Some("alice".to_string()),
            },
            definition_path: SystemdBackend.definition_path(&name, scope),
            runtime_path: runtime_path(Path::new("/opt/edge-1"), scope).unwrap_or_default(),
            log_source: SystemdBackend.log_source(&name, scope),
            runtime_digest: String::new(),
            runtime_version: "v2.2.0".to_string(),
            health_url: HEALTH_URL.to_string(),
        }
    }

    #[test]
    fn every_systemd_state_normalizes() {
        for (reported, expected) in [
            ("active", ServiceState::Running),
            ("reloading", ServiceState::Running),
            ("activating", ServiceState::Starting),
            ("deactivating", ServiceState::Stopping),
            ("inactive", ServiceState::Stopped),
            ("failed", ServiceState::Failed),
            ("unknown", ServiceState::Unavailable),
            ("", ServiceState::Unavailable),
            // A word a future systemd invents must not be published raw.
            ("maintenance", ServiceState::Unavailable),
        ] {
            assert_eq!(
                normalize_systemd_state(reported),
                expected,
                "systemd `{reported}`"
            );
        }
        // `is-active` output arrives with a trailing newline.
        assert_eq!(normalize_systemd_state("active\n"), ServiceState::Running);
    }

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
        let unit = rendered(
            "/opt/edge-1",
            "/opt/edge-1/.spice",
            "/usr/local/lib/spice/edge-1/spiced",
        );
        assert!(unit.contains("WorkingDirectory=\"/opt/edge-1\"\n"));
        assert_eq!(
            parse_working_dir(&unit),
            Some(PathBuf::from("/opt/edge-1")),
            "the working directory must round-trip so status can discover it"
        );
    }

    #[test]
    fn rendered_unit_runs_spiced_from_the_enrolled_directory() {
        let unit = rendered(
            "/opt/edge-1",
            "/opt/edge-1/.spice",
            "/usr/local/lib/spice/edge-1/spiced",
        );
        // No flag: the enrolled identity under SPICE_CONFIG_DIR is what
        // activates Cloud Connect on every start.
        assert!(unit.contains("ExecStart=\"/usr/local/lib/spice/edge-1/spiced\"\n"));
        assert!(!unit.contains("--cloud-connect"));
    }

    #[test]
    fn rendered_unit_restarts_a_failure_and_leaves_a_clean_exit_alone() {
        // A deployment applies to the running instance without ending it, so
        // the restart policy is there for the things that do end it. A clean
        // exit is what an operator's `stop` produces and must stay stopped.
        for scope in [ServiceScope::System, ServiceScope::User] {
            let unit = render_unit(
                Path::new("/opt/edge-1"),
                Path::new("/opt/edge-1/.spice"),
                Path::new("/usr/bin/spiced"),
                None,
                scope,
            )
            .expect("render unit");
            assert!(unit.contains("\nRestart=on-failure\n"), "{scope}");
            assert!(unit.contains("\nRestartSec=5\n"), "{scope}");
            // A rate limit would let a crash loop give up permanently.
            assert!(unit.contains("\nStartLimitIntervalSec=0\n"), "{scope}");
        }
    }

    #[test]
    fn a_system_unit_runs_as_the_operator_and_starts_before_anyone_logs_in() {
        let unit = rendered("/opt/edge-1", "/opt/edge-1/.spice", "/usr/bin/spiced");
        assert!(unit.contains("User=1000\n"));
        assert!(unit.contains("Group=1001\n"));
        assert!(unit.contains("WantedBy=multi-user.target\n"));
    }

    #[test]
    fn a_user_unit_names_no_account_and_is_wanted_by_the_default_target() {
        // systemd refuses `User=` in a user unit — the manager is already
        // running as that account — and `multi-user.target` is not a target a
        // user manager reaches.
        let unit = render_unit(
            Path::new("/opt/edge-1"),
            Path::new("/opt/edge-1/.spice"),
            Path::new("/home/alice/.local/share/spice/services/edge-1/spiced"),
            None,
            ServiceScope::User,
        )
        .expect("render unit");
        assert!(!unit.contains("User="), "{unit}");
        assert!(!unit.contains("Group="), "{unit}");
        assert!(unit.contains("WantedBy=default.target\n"), "{unit}");
    }

    #[test]
    fn rendered_unit_preserves_a_custom_config_directory() {
        let unit = rendered(
            "/opt/edge-1",
            "/var/lib/spice/custom config",
            "/usr/bin/spiced",
        );
        assert!(unit.contains("Environment=\"SPICE_CONFIG_DIR=/var/lib/spice/custom config\"\n"));
    }

    #[test]
    fn rendered_unit_escapes_specifiers_quotes_and_spaces() {
        let dir = Path::new("/opt/edge %i/quoted\"dir");
        let unit = render_unit(
            dir,
            &dir.join(".spice"),
            Path::new("/usr/local/lib/spice/spiced"),
            Some(TEST_ACCOUNT),
            ServiceScope::System,
        )
        .expect("hostile but valid path is escaped");
        assert!(unit.contains("WorkingDirectory=\"/opt/edge %%i/quoted\\\"dir\""));
        assert_eq!(parse_working_dir(&unit), Some(dir.to_path_buf()));
    }

    #[test]
    fn rendered_unit_rejects_a_directive_injection_path() {
        let err = render_unit(
            Path::new("/opt/edge\nExecStart=/bin/sh"),
            Path::new("/opt/edge/.spice"),
            Path::new("/usr/bin/spiced"),
            Some(TEST_ACCOUNT),
            ServiceScope::System,
        )
        .expect_err("newlines must not enter unit syntax");
        assert!(matches!(err, Error::InvalidArgument { .. }));
    }

    #[test]
    fn parse_exec_runtime_drops_arguments_and_prefixes() {
        assert_eq!(
            parse_exec_runtime("[Service]\nExecStart=/usr/local/lib/spice/spiced\n"),
            Some(PathBuf::from("/usr/local/lib/spice/spiced")),
            "the reported runtime is the binary, not the whole command line"
        );
        // systemd's special executable prefixes are not part of the path.
        assert_eq!(
            parse_exec_runtime("ExecStart=-/usr/bin/spiced\n"),
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
    fn each_instance_directory_stages_its_own_runtime() {
        // A shared binary would make an upgrade in one directory change what
        // every other instance runs at its next restart.
        let one = runtime_path(Path::new("/opt/edge-1"), ServiceScope::System)
            .expect("a system runtime path is always derivable");
        let two = runtime_path(Path::new("/opt/edge-2"), ServiceScope::System)
            .expect("a system runtime path is always derivable");
        assert_ne!(one, two);
        assert!(one.starts_with(super::super::RUNTIME_STAGE_DIR), "{one:?}");
        assert!(one.ends_with("spiced"), "{one:?}");
        assert_ne!(
            one,
            Path::new(super::super::RUNTIME_STAGE_DIR).join("spiced"),
            "the runtime must not be the host-wide shared copy"
        );
    }

    #[test]
    fn a_user_service_stages_its_runtime_under_the_accounts_own_data_directory() {
        let Some(user) = runtime_path(Path::new("/opt/edge-1"), ServiceScope::User) else {
            // No discoverable data directory on this host; the installer
            // refuses rather than guessing, which `install_paths` covers.
            return;
        };
        assert!(
            !user.starts_with(super::super::RUNTIME_STAGE_DIR),
            "{user:?}"
        );
        assert!(user.ends_with("spiced"), "{user:?}");
    }

    #[test]
    fn recovery_hints_name_the_unit_in_its_own_domain() {
        let system = recovery_hints(&manifest(ServiceScope::System));
        assert!(
            system[0].ends_with(&format!(
                "systemctl status {}",
                manifest(ServiceScope::System).name
            )),
            "{system:?}"
        );
        assert!(system[1].starts_with("journalctl -u "), "{system:?}");

        // A user service is managed by its owning account's own manager, so
        // every command has to name that manager rather than root's.
        let user = recovery_hints(&manifest(ServiceScope::User));
        assert!(user[0].contains("systemctl --user status"), "{user:?}");
        assert!(user[1].starts_with("journalctl --user -u "), "{user:?}");
        assert!(
            !user.iter().any(|hint| hint.starts_with("sudo")),
            "a user service must never be driven through sudo: {user:?}"
        );
    }

    #[test]
    fn a_user_unit_lives_under_the_accounts_own_configuration() {
        let name = unit_name_for_dir(Path::new("/opt/edge-1"));
        let system = SystemdBackend.definition_path(&name, ServiceScope::System);
        let user = SystemdBackend.definition_path(&name, ServiceScope::User);
        assert!(system.starts_with(SYSTEMD_UNIT_DIR), "{system:?}");
        assert_ne!(system, user);
        assert!(
            user.to_string_lossy().contains(SYSTEMD_USER_UNIT_SUBDIR),
            "{user:?}"
        );
    }

    #[test]
    fn the_log_source_is_the_journal_for_the_units_own_domain() {
        let name = unit_name_for_dir(Path::new("/opt/edge-1"));
        assert_eq!(
            SystemdBackend.log_source(&name, ServiceScope::System),
            Some(LogSource::Journal {
                unit: name.clone(),
                scope: ServiceScope::System
            })
        );
        assert_eq!(
            SystemdBackend.log_source(&name, ServiceScope::User),
            Some(LogSource::Journal {
                unit: name,
                scope: ServiceScope::User
            })
        );
    }

    #[test]
    fn only_a_persistent_enable_reports_boot_persistence() {
        // `enabled-runtime` links live under /run and are gone after a reboot,
        // so reporting boot persistence for it would promise exactly what will
        // not happen. The rest leave no persistent boot-time link either.
        for reported in [
            "enabled-runtime",
            "disabled",
            "linked",
            "linked-runtime",
            "static",
            "indirect",
            "alias",
            "generated",
            "transient",
        ] {
            let (starts, action) =
                classify_persistence(reported, None, &manifest(ServiceScope::System));
            assert_eq!(starts, ServiceStarts::Disabled, "{reported}");
            let action = action.unwrap_or_else(|| panic!("{reported} needs a remediation"));
            assert!(action.contains("systemctl"), "{reported}: {action}");
            assert!(action.contains("enable"), "{reported}: {action}");
        }
    }

    #[test]
    fn a_masked_unit_is_told_to_unmask_before_enabling() {
        // `systemctl enable` on a masked unit fails, so printing it alone would
        // hand the operator a command that cannot work.
        for reported in ["masked", "masked-runtime"] {
            let (starts, action) =
                classify_persistence(reported, None, &manifest(ServiceScope::System));
            assert_eq!(starts, ServiceStarts::Disabled, "{reported}");
            let action = action.unwrap_or_else(|| panic!("{reported} needs a remediation"));
            assert!(action.contains("unmask"), "{reported}: {action}");
            assert!(action.contains("enable"), "{reported}: {action}");
        }
    }

    #[test]
    fn an_unanswerable_enablement_query_is_not_an_outcome() {
        let (starts, action) = classify_persistence("", None, &manifest(ServiceScope::System));
        assert_eq!(starts, ServiceStarts::Unavailable);
        assert_eq!(action, None, "there is nothing to advise yet");
    }

    #[test]
    fn a_user_service_starts_at_boot_only_once_its_account_lingers() {
        // Denied, and not asked, are both `login_only` with the command that
        // changes it: claiming boot persistence that is not there is the
        // failure this avoids.
        for lingers in [Some(false), None] {
            let (starts, action) =
                classify_persistence("enabled", lingers, &manifest(ServiceScope::User));
            assert_eq!(starts, ServiceStarts::LoginOnly, "{lingers:?}");
            let action = action.unwrap_or_else(|| panic!("{lingers:?} needs a remediation"));
            assert!(action.contains("enable-linger"), "{action}");
        }

        // Lingering verified: the account's manager runs with nobody logged in,
        // so the service really does come up at boot.
        let (starts, action) =
            classify_persistence("enabled", Some(true), &manifest(ServiceScope::User));
        assert_eq!(starts, ServiceStarts::BootWithoutLogin);
        assert_eq!(action, None, "nothing to fix");

        // Trailing newline from `systemctl` output, and the system answer.
        let (starts, action) =
            classify_persistence("enabled\n", None, &manifest(ServiceScope::System));
        assert_eq!(starts, ServiceStarts::BootWithoutLogin);
        assert_eq!(action, None, "nothing to fix");
    }

    #[test]
    fn lingering_is_verified_rather_than_assumed() {
        let account = super::super::account_name(nix::unistd::Uid::effective().as_raw());
        let Some(account) = account else {
            // No account name on this host, so there is nobody to ask about.
            return;
        };
        let query = format!("loginctl show-user {account} --property=Linger --value");

        let granted = ScriptedHost::new().says(&query, "yes");
        assert_eq!(enable_linger(&granted), Some(true));
        assert!(
            granted.ran(&format!("loginctl enable-linger {account}")),
            "{:?}",
            granted.calls()
        );

        // A policy that refuses lingering is reported, not assumed away.
        let denied = ScriptedHost::new()
            .fails(
                &format!("loginctl enable-linger {account}"),
                "Access denied",
            )
            .says(&query, "no");
        assert_eq!(enable_linger(&denied), Some(false));

        // A logind that cannot be asked is not an answer.
        let silent = ScriptedHost::new().fails(&query, "Failed to get user: no such user");
        assert_eq!(enable_linger(&silent), None);
    }

    #[test]
    fn an_unrecognized_active_state_explains_itself() {
        // The model promises every `unavailable` state says why.
        let manifest = manifest(ServiceScope::System);
        let diagnostic = unrecognized_state_diagnostic("maintenance", &manifest);
        assert!(diagnostic.contains("maintenance"), "{diagnostic}");
        assert!(diagnostic.contains("is-active"), "{diagnostic}");
    }

    #[test]
    fn observing_a_user_service_asks_that_accounts_own_manager() {
        let manifest = manifest(ServiceScope::User);
        let host = ScriptedHost::new()
            .says(
                &format!("systemctl --user is-active {}", manifest.name),
                "active",
            )
            .says(
                &format!("systemctl --user is-enabled {}", manifest.name),
                "enabled",
            );

        let observed = observe(&host, &manifest);
        assert_eq!(observed.state, ServiceState::Running);
        assert_eq!(observed.starts, ServiceStarts::LoginOnly);
        assert!(
            host.calls()
                .iter()
                .all(|call| !call.starts_with("systemctl is-")),
            "a user service must never be queried through the system manager: {:?}",
            host.calls()
        );
    }

    #[test]
    fn starting_waits_for_the_service_to_be_running() {
        let manifest = manifest(ServiceScope::User);
        let is_active = format!("systemctl --user is-active {}", manifest.name);
        // systemd returns from `start` while the unit is still activating.
        let host =
            ScriptedHost::new().sequence(&is_active, &["activating", "activating", "active"]);

        start(&host, &manifest).expect("the service reaches running");
        assert!(
            host.ran(&format!("systemctl --user start {}", manifest.name)),
            "{:?}",
            host.calls()
        );
        assert!(*host.slept.borrow() > Duration::ZERO, "it polled");
    }

    #[test]
    fn starting_is_idempotent_for_a_running_service() {
        let manifest = manifest(ServiceScope::User);
        let host = ScriptedHost::new().says(
            &format!("systemctl --user is-active {}", manifest.name),
            "active",
        );
        start(&host, &manifest).expect("an already-running service succeeds");
    }

    #[test]
    fn stopping_waits_for_the_service_to_be_down_and_leaves_it_installed() {
        let manifest = manifest(ServiceScope::User);
        let host = ScriptedHost::new().sequence(
            &format!("systemctl --user is-active {}", manifest.name),
            &["deactivating", "inactive"],
        );

        stop(&host, &manifest).expect("the service reaches stopped");
        assert!(host.ran(&format!("systemctl --user stop {}", manifest.name)));
        assert!(
            !host
                .calls()
                .iter()
                .any(|call| call.contains("disable") || call.contains("remove")),
            "a stop must leave the service installed and enabled: {:?}",
            host.calls()
        );
    }

    #[test]
    fn restarting_is_owned_by_the_supervisor() {
        let manifest = manifest(ServiceScope::User);
        let host = ScriptedHost::new().sequence(
            &format!("systemctl --user is-active {}", manifest.name),
            &["activating", "active"],
        );

        restart(&host, &manifest).expect("the service comes back");
        assert!(host.ran(&format!("systemctl --user restart {}", manifest.name)));
    }

    #[test]
    fn a_service_that_does_not_reach_its_state_is_reported_rather_than_assumed() {
        let manifest = manifest(ServiceScope::User);
        let host = ScriptedHost::new().says(
            &format!("systemctl --user is-active {}", manifest.name),
            "failed",
        );

        let error = restart(&host, &manifest).expect_err("a failed unit must fail the restart");
        assert!(error.to_string().contains("failed"), "{error}");
        assert!(error.to_string().contains("logs"), "{error}");
    }

    #[test]
    fn a_system_service_never_self_elevates_and_names_the_retry() {
        // Root can drive a system service; everyone else is told exactly what
        // to run instead of having it run for them.
        if nix::unistd::Uid::effective().is_root() {
            return;
        }
        let manifest = manifest(ServiceScope::System);
        let host = ScriptedHost::new();
        for (action, result) in [
            ("start", start(&host, &manifest)),
            ("stop", stop(&host, &manifest)),
            ("restart", restart(&host, &manifest)),
            ("uninstall", uninstall(&host, &manifest)),
        ] {
            let error = result.expect_err("a non-root system-service mutation must be refused");
            assert!(
                error.to_string().contains(&format!(
                    "sudo spice connect service {action} --dir /opt/edge-1"
                )),
                "{action}: {error}"
            );
        }
        assert!(
            host.calls().is_empty(),
            "a refusal must not touch the supervisor: {:?}",
            host.calls()
        );
    }

    #[test]
    fn a_user_service_is_never_driven_through_another_accounts_manager() {
        // What `sudo spice connect service restart` on a user service would
        // otherwise do: talk to root's user manager, which holds no such unit.
        let mut manifest = manifest(ServiceScope::User);
        manifest.owner.uid = nix::unistd::Uid::effective().as_raw() + 1;
        manifest.owner.name = Some("alice".to_string());

        let host = ScriptedHost::new();
        let error = restart(&host, &manifest).expect_err("another account's service is refused");
        assert!(error.to_string().contains("alice"), "{error}");
        assert!(
            error
                .to_string()
                .contains("spice connect service restart --dir /opt/edge-1"),
            "{error}"
        );
        assert!(!error.to_string().contains("sudo spice"), "{error}");
        assert!(host.calls().is_empty(), "{:?}", host.calls());
    }

    #[test]
    fn the_journal_is_read_by_exact_unit_bounded_and_unpaged() {
        let name = unit_name_for_dir(Path::new("/opt/edge-1"));
        let args = journal_args(
            &name,
            ServiceScope::System,
            LogRequest {
                number: 100,
                follow: false,
            },
        );
        assert_eq!(
            args,
            vec!["-u", &name, "--no-pager", "-q", "-o", "cat", "-n", "100"]
        );

        // A user service's output lives in that account's own journal.
        let user = journal_args(
            &name,
            ServiceScope::User,
            LogRequest {
                number: 0,
                follow: true,
            },
        );
        assert_eq!(user.first().map(String::as_str), Some("--user"));
        assert_eq!(user.last().map(String::as_str), Some("-f"));
        // `-n 0 -f` follows without printing any history.
        assert!(user.windows(2).any(|pair| pair == ["-n", "0"]), "{user:?}");
    }

    #[test]
    fn following_logs_streams_and_reports_an_interruption_without_changing_the_service() {
        let manifest = manifest(ServiceScope::User);
        let host = ScriptedHost::new().streaming(None);
        let error = logs(
            &host,
            &manifest,
            LogRequest {
                number: 100,
                follow: true,
            },
        )
        .expect_err("an interrupted viewer exits as interrupted");
        assert!(matches!(error, Error::Interrupted), "{error}");
        assert_eq!(
            host.streamed.borrow().len(),
            1,
            "the snapshot and the follow are one bounded journalctl"
        );
        assert!(
            host.calls().is_empty(),
            "reading logs must not touch the service: {:?}",
            host.calls()
        );
    }

    #[test]
    fn an_empty_journal_is_a_successful_answer() {
        let manifest = manifest(ServiceScope::User);
        let host = ScriptedHost::new();
        logs(
            &host,
            &manifest,
            LogRequest {
                number: 100,
                follow: false,
            },
        )
        .expect("an empty history is not a failure");
    }

    #[test]
    fn a_journal_that_cannot_be_read_says_so() {
        let manifest = manifest(ServiceScope::User);
        let host = ScriptedHost::new().fails(
            &format!(
                "journalctl --user -u {} --no-pager -q -o cat -n 100",
                manifest.name
            ),
            "Failed to add match: Invalid argument",
        );
        let error = logs(
            &host,
            &manifest,
            LogRequest {
                number: 100,
                follow: false,
            },
        )
        .expect_err("a journal failure is reported");
        assert!(error.to_string().contains("Invalid argument"), "{error}");
    }

    #[test]
    fn the_health_gate_accepts_a_service_that_comes_up_and_answers() {
        let name = unit_name_for_dir(Path::new("/opt/edge-1"));
        let host = ScriptedHost::new()
            .sequence(
                &format!("systemctl --user is-active {name}"),
                &["activating", "active"],
            )
            .health(&[false, true]);

        health_gate(&host, &name, ServiceScope::User, HEALTH_URL).expect("a healthy install");
    }

    #[test]
    fn a_service_that_stays_up_without_answering_is_accepted_once_it_settles() {
        // The health URL is the address the CLI was pointed at, while the
        // service serves whatever its spicepod configures. An unanswered probe
        // is therefore not proof of a broken install — an uninterrupted run is
        // what stands in for it.
        let name = unit_name_for_dir(Path::new("/opt/edge-1"));
        let host = ScriptedHost::new()
            .says(&format!("systemctl --user is-active {name}"), "active")
            .health(&[false]);

        health_gate(&host, &name, ServiceScope::User, HEALTH_URL)
            .expect("a service that stays up is up");
        assert!(
            *host.slept.borrow() >= settle_window(),
            "it may only be accepted after it has stayed up: {:?}",
            host.slept
        );
    }

    #[test]
    fn the_health_gate_rejects_a_service_that_never_comes_up() {
        // The failure an install has to catch: systemd accepted the unit and
        // the runtime never started serving.
        let name = unit_name_for_dir(Path::new("/opt/edge-1"));
        let host = ScriptedHost::new()
            .says(&format!("systemctl --user is-active {name}"), "activating")
            .health(&[false]);

        let why = health_gate(&host, &name, ServiceScope::User, HEALTH_URL)
            .expect_err("a runtime that never comes up must not pass");
        assert!(why.contains("rather than running"), "{why}");
    }

    #[test]
    fn the_health_gate_rejects_a_runtime_that_keeps_being_restarted() {
        // A crash loop never accumulates an uninterrupted run and never
        // answers, which is exactly what the gate has to refuse.
        let name = unit_name_for_dir(Path::new("/opt/edge-1"));
        let flapping: Vec<&str> = (0..HEALTH_ATTEMPTS)
            .map(|attempt| {
                if attempt % 2 == 0 {
                    "active"
                } else {
                    "activating"
                }
            })
            .collect();
        let host = ScriptedHost::new()
            .sequence(&format!("systemctl --user is-active {name}"), &flapping)
            .health(&[false]);

        let why = health_gate(&host, &name, ServiceScope::User, HEALTH_URL)
            .expect_err("a restarting runtime must not pass");
        assert!(!why.is_empty(), "the refusal has to say what it saw");
    }

    #[test]
    fn the_health_gate_gives_up_on_a_unit_systemd_calls_failed() {
        let name = unit_name_for_dir(Path::new("/opt/edge-1"));
        let host = ScriptedHost::new().says(&format!("systemctl is-active {name}"), "failed");

        let why = health_gate(&host, &name, ServiceScope::System, HEALTH_URL)
            .expect_err("a failed unit must not pass");
        assert!(why.contains("failed"), "{why}");
        // Given up on early rather than after the whole gate, but not on the
        // first reading — a restarting unit passes through several words.
        assert!(*host.slept.borrow() < HEALTH_GATE, "{:?}", host.slept);
    }

    #[test]
    fn an_unprobeable_health_url_gates_on_what_systemd_reports() {
        // A health URL this back end cannot reach must not fail an install that
        // is fine.
        assert!(is_probeable(HEALTH_URL));
        assert!(!is_probeable("https://127.0.0.1:8090/health"));

        let name = unit_name_for_dir(Path::new("/opt/edge-1"));
        let host = ScriptedHost::new()
            .says(&format!("systemctl is-active {name}"), "active")
            .health(&[false]);
        health_gate(
            &host,
            &name,
            ServiceScope::System,
            "https://127.0.0.1:8090/health",
        )
        .expect("systemd's own report is the answer here");
    }

    #[test]
    fn only_a_success_status_line_counts_as_healthy() {
        assert!(status_line_is_success("HTTP/1.1 200 OK\r\nDate: now\r\n"));
        assert!(status_line_is_success("HTTP/1.0 204 No Content\r\n"));
        assert!(!status_line_is_success("HTTP/1.1 503 Service Unavailable"));
        assert!(!status_line_is_success("HTTP/1.1 200"), "no reason phrase");
        assert!(!status_line_is_success(""));
        assert!(!status_line_is_success("garbage"));
    }

    /// An install request pointing at a runtime and directories under `root`.
    fn install_request<'a>(
        instance_dir: &'a Path,
        config_dir: &'a Path,
        spiced: &'a Path,
    ) -> InstallRequest<'a> {
        InstallRequest {
            instance_dir,
            config_dir,
            spiced_path: spiced,
            scope: ServiceScope::User,
            health_url: HEALTH_URL,
        }
    }

    /// The paths an install under `root` writes, so the whole install can run
    /// without touching a real systemd directory.
    fn install_paths_under(root: &Path, name: &str) -> InstallPaths {
        let runtime_dir = root.join("runtime");
        InstallPaths {
            unit: root.join("units").join(name),
            runtime: runtime_dir.join("spiced"),
            runtime_dir,
        }
    }

    #[test]
    fn an_install_that_comes_up_healthy_records_what_it_installed() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let instance_dir = dir.path().join("edge-1");
        let config_dir = instance_dir.join(".spice");
        let spiced = dir.path().join("spiced");
        std::fs::create_dir_all(&instance_dir).expect("create instance dir");
        std::fs::write(&spiced, b"runtime-v1").expect("write runtime");

        let name = unit_name_for_dir(&instance_dir);
        let paths = install_paths_under(dir.path(), &name);
        let host = ScriptedHost::new()
            .says(&format!("systemctl --user is-active {name}"), "active")
            .health(&[true]);

        let installed = install_at(
            &host,
            &install_request(&instance_dir, &config_dir, &spiced),
            &name,
            &paths,
            None,
        )
        .expect("install");

        assert_eq!(installed.runtime, paths.runtime);
        assert_eq!(installed.path, paths.unit);
        assert_eq!(
            std::fs::read(&paths.runtime).expect("read staged runtime"),
            b"runtime-v1",
            "the service runs its own staged copy"
        );
        let unit = std::fs::read_to_string(&paths.unit).expect("read unit");
        assert_eq!(
            parse_working_dir(&unit).as_deref(),
            Some(instance_dir.as_path())
        );
        assert_eq!(
            parse_exec_runtime(&unit).as_deref(),
            Some(paths.runtime.as_path())
        );
        // Enabled for boot, then restarted so the new unit and binary are what
        // is actually running.
        assert!(host.ran(&format!("systemctl --user enable {name}")));
        assert!(host.ran(&format!("systemctl --user restart {name}")));
        assert!(
            !paths.runtime.with_extension("previous").exists(),
            "a successful install keeps no rollback copy"
        );
    }

    #[test]
    fn an_upgrade_that_does_not_come_up_puts_the_previous_one_back() {
        // The acceptance criterion an upgrade lives or dies by: a runtime that
        // does not serve must leave the instance running exactly what it was
        // running before.
        let dir = tempfile::tempdir().expect("create tempdir");
        let instance_dir = dir.path().join("edge-1");
        let config_dir = instance_dir.join(".spice");
        let spiced = dir.path().join("spiced");
        std::fs::create_dir_all(&instance_dir).expect("create instance dir");
        std::fs::write(&spiced, b"runtime-v1").expect("write runtime");

        let name = unit_name_for_dir(&instance_dir);
        let paths = install_paths_under(dir.path(), &name);
        let healthy = ScriptedHost::new()
            .says(&format!("systemctl --user is-active {name}"), "active")
            .health(&[true]);
        install_at(
            &healthy,
            &install_request(&instance_dir, &config_dir, &spiced),
            &name,
            &paths,
            None,
        )
        .expect("the first install");
        let installed_unit = std::fs::read(&paths.unit).expect("read unit");

        // The upgrade: a new binary systemd cannot keep running.
        std::fs::write(&spiced, b"runtime-v2-broken").expect("write the upgrade");
        let broken = ScriptedHost::new()
            .says(&format!("systemctl --user is-active {name}"), "failed")
            .health(&[false]);
        let error = install_at(
            &broken,
            &install_request(&instance_dir, &config_dir, &spiced),
            &name,
            &paths,
            None,
        )
        .expect_err("an upgrade that does not serve must fail");

        assert!(error.to_string().contains("put back"), "{error}");
        assert_eq!(
            std::fs::read(&paths.runtime).expect("read staged runtime"),
            b"runtime-v1",
            "the runtime that was serving must be back"
        );
        assert_eq!(
            std::fs::read(&paths.unit).expect("read unit"),
            installed_unit,
            "the unit that was in force must be back"
        );
        assert!(
            !paths.runtime.with_extension("previous").exists(),
            "the rollback copy is consumed"
        );
        assert!(
            broken.ran(&format!("systemctl --user restart {name}")),
            "the restored service is started again: {:?}",
            broken.calls()
        );
    }

    #[test]
    fn a_first_install_that_does_not_come_up_leaves_nothing_behind() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let instance_dir = dir.path().join("edge-1");
        let config_dir = instance_dir.join(".spice");
        let spiced = dir.path().join("spiced");
        std::fs::create_dir_all(&instance_dir).expect("create instance dir");
        std::fs::write(&spiced, b"runtime-v1").expect("write runtime");

        let name = unit_name_for_dir(&instance_dir);
        let paths = install_paths_under(dir.path(), &name);
        let host = ScriptedHost::new()
            .says(&format!("systemctl --user is-active {name}"), "inactive")
            .health(&[false]);

        let error = install_at(
            &host,
            &install_request(&instance_dir, &config_dir, &spiced),
            &name,
            &paths,
            None,
        )
        .expect_err("a service that never comes up must fail the install");
        assert!(
            error.to_string().contains("Nothing was left installed"),
            "{error}"
        );
        assert!(!paths.unit.exists(), "the unit must be gone");
        assert!(!paths.runtime.exists(), "the staged runtime must be gone");
        assert!(
            host.ran(&format!("systemctl --user disable --now {name}")),
            "{:?}",
            host.calls()
        );
    }

    #[test]
    fn an_uninstall_removes_only_this_services_own_runtime() {
        let mut manifest = manifest(ServiceScope::System);

        // Its own staging directory goes with it.
        let own = runtime_dir(&manifest.directory, ServiceScope::System)
            .expect("a system staging directory is always derivable");
        manifest.runtime_path = own.join("spiced");
        assert_eq!(owned_runtime_dir(&manifest).as_ref(), Some(&own));

        // A runtime recorded anywhere else — a host-wide copy another service
        // may still execute, another instance's staged copy, or an edited
        // manifest — is not this uninstall's to delete.
        for elsewhere in [
            PathBuf::from(super::super::RUNTIME_STAGE_DIR).join("spiced"),
            runtime_dir(Path::new("/opt/edge-2"), ServiceScope::System)
                .expect("a system staging directory is always derivable")
                .join("spiced"),
            PathBuf::from("/usr/bin/spiced"),
        ] {
            manifest.runtime_path.clone_from(&elsewhere);
            assert_eq!(owned_runtime_dir(&manifest), None, "{elsewhere:?}");
        }
    }

    #[test]
    fn uninstalling_deletes_the_definition_and_leaves_the_journal_to_systemd() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let mut manifest = manifest(ServiceScope::User);
        manifest.definition_path = dir.path().join("unit.service");
        // Recorded elsewhere, so the runtime removal is out of the way of what
        // this test is about; its own guard is covered separately.
        manifest.runtime_path = dir.path().join("spiced");
        std::fs::write(&manifest.definition_path, "[Unit]\n").expect("write unit");
        std::fs::write(&manifest.runtime_path, b"runtime").expect("write runtime");

        let host = ScriptedHost::new();
        uninstall(&host, &manifest).expect("uninstall");

        assert!(!manifest.definition_path.exists(), "the unit must be gone");
        assert!(host.ran(&format!("systemctl --user disable --now {}", manifest.name)));
        assert!(host.ran("systemctl --user daemon-reload"));
        assert!(
            !host.calls().iter().any(|call| call.contains("journal")),
            "log history is systemd's to retain: {:?}",
            host.calls()
        );

        // Idempotent: a second uninstall has nothing left to remove and still
        // succeeds.
        uninstall(&host, &manifest).expect("second uninstall");
    }

    #[test]
    fn a_user_scope_stage_directory_must_be_this_accounts_own() {
        use std::os::unix::fs::PermissionsExt as _;

        let dir = tempfile::tempdir().expect("create tempdir");
        let stage = dir.path().join("runtime");
        ensure_account_only_dir(&stage).expect("a fresh directory is this account's");

        // A symlink points the runtime somewhere this check cannot vouch for.
        let linked = dir.path().join("linked");
        std::os::unix::fs::symlink(&stage, &linked).expect("symlink");
        let error = ensure_account_only_dir(&linked).expect_err("a symlinked stage is refused");
        assert!(error.to_string().contains("real directory"), "{error}");

        // So does a directory anyone else can write.
        std::fs::set_permissions(&stage, std::fs::Permissions::from_mode(0o777))
            .expect("widen the mode");
        let error = ensure_account_only_dir(&stage).expect_err("a world-writable stage is refused");
        assert!(error.to_string().contains("chmod go-w"), "{error}");
    }

    #[test]
    fn a_user_install_needs_the_accounts_own_manager() {
        // Asking for a user service on a host with no user manager has to fail
        // before anything is written, naming the path that does work.
        let failure = preflight(ServiceScope::User);
        if let Err(failure) = failure {
            let message = Error::from(failure).to_string();
            assert!(
                message.contains("spice connect service install"),
                "{message}"
            );
        }
    }

    #[test]
    fn the_gates_poll_for_exactly_as_long_as_they_promise() {
        // The messages both gates print name a duration, and the loops are
        // bounded by attempts — so the two have to be kept in step here rather
        // than in a comment.
        assert_eq!(HEALTH_POLL_INTERVAL * HEALTH_ATTEMPTS, HEALTH_GATE);
        assert_eq!(
            LIFECYCLE_POLL_INTERVAL * LIFECYCLE_ATTEMPTS,
            Duration::from_secs(10)
        );
    }

    #[test]
    fn a_folded_message_stays_on_one_line() {
        assert_eq!(
            folded("Failed to start.\nSee `systemctl status`.\n"),
            "Failed to start. See `systemctl status`."
        );
    }
}
