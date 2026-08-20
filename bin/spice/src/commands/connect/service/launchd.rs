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

//! The macOS back end: one launchd job per instance directory.
//!
//! ## Two domains, and only one of them starts without a login
//!
//! An unprivileged install writes a `LaunchAgent` under
//! `~/Library/LaunchAgents` and bootstraps it into the account's GUI domain
//! (`gui/<uid>`); a root install writes a `LaunchDaemon` under
//! `/Library/LaunchDaemons`, bootstraps it into the `system` domain, and runs
//! the runtime as the invoking operator rather than as root.
//!
//! These are not two spellings of the same thing. A `LaunchDaemon` comes up
//! while the host boots, with nobody logged in. A `LaunchAgent` comes up with
//! its owner's GUI login session and cannot be made to come up before one —
//! there is no launchd equivalent of `loginctl enable-linger`. So an agent
//! reports [`ServiceStarts::LoginOnly`] and says what to run for boot
//! persistence, and no message here ever implies otherwise.
//!
//! ## launchd fails quietly, so every step is confirmed
//!
//! It refuses a daemon definition whose ownership or permissions are wrong, and
//! says so only to the system log. It will bootstrap a job whose program the
//! kernel then declines to execute — a binary carrying `com.apple.quarantine`,
//! or one whose code signature is broken — and report nothing. And it keeps
//! serving the definition a job was bootstrapped with, so writing a new plist
//! over a job that is still loaded leaves the old one in force.
//!
//! So: the definition's ownership is set and read back, the staged runtime is
//! executed once before the job is created, the previous job is booted out and
//! its absence confirmed before the new one is bootstrapped, and the install
//! does not return until the service has held one uninterrupted run and
//! answered its health URL.
//!
//! ## An install is not finished when launchd accepts the job
//!
//! An install or upgrade gives the service [`HEALTH_GATE`] to prove itself, and
//! anything else puts the previous definition and the previous runtime back
//! before failing. An upgrade must never leave an instance worse off than the
//! one it replaced.
//!
//! ## Logs
//!
//! launchd owns no log store. It writes a job's output to whatever files
//! `StandardOutPath` and `StandardErrorPath` name and never bounds them, which
//! is how a long-running daemon fills a disk. So the definition names neither,
//! and the runtime is started with `--service-log-dir` instead: it writes its
//! own bounded, rotating files under one fixed policy
//! (`runtime_cloud_connect::service_log`) that `spice cloud logs`
//! reads back against the same constants.

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use runtime_cloud_connect::service_log::{self, ServiceLogReader};

use super::backend::{
    InstallRequest, LogRequest, ServiceBackend, ServiceConflict, ServiceObservation,
};
use super::host::{CommandOutput, HealthProbe, folded, is_probeable, probe_http_health};
use super::manifest::ServiceManifest;
use super::model::{LogSource, ServiceScope, ServiceStarts, ServiceState, Supervisor};
use super::{InstalledService, PreflightFailure, ServiceAccount};
use crate::error::{Error, Result};

/// Absolute path to `launchctl`, so a caller-controlled `PATH` cannot select an
/// attacker-supplied binary during a documented `sudo spice cloud service …`
/// invocation.
pub(super) const LAUNCHCTL: &str = "/bin/launchctl";

/// Directory launchd reads administrator-provided daemon definitions from.
const LAUNCH_DAEMON_DIR: &str = "/Library/LaunchDaemons";

/// Directory launchd reads an account's own agent definitions from, relative to
/// that account's home.
const LAUNCH_AGENT_SUBDIR: &str = "Library/LaunchAgents";

/// Shared prefix of every job label this command installs. Also the prefix used
/// to discover installed instances.
const LABEL_PREFIX: &str = "ai.spice.cloud-connect";

/// Suffix of every definition this command installs.
const PLIST_SUFFIX: &str = ".plist";

/// Mode a definition is written with: readable by launchd and by the operator
/// reading it back, writable only by whoever installed it.
const PLIST_MODE: u32 = 0o644;

/// Directory a user service's own runtime is staged into, relative to the
/// account's data directory.
const USER_RUNTIME_SUBDIR: &str = "spice/services";

/// Root-owned directory holding the per-service log directories of system
/// services. `/Library/Logs` is where macOS expects a system component's own
/// log files.
const SYSTEM_LOG_ROOT: &str = "/Library/Logs/Spice";

/// Directory a user service's log files live in, relative to the account's
/// home — the macOS convention for a per-user component's logs.
const USER_LOG_SUBDIR: &str = "Library/Logs/Spice";

/// A generated definition is only a few KiB. Bound discovery reads generously
/// so an attacker-controlled matching path cannot make a privileged install
/// follow a device or consume unbounded memory.
const MAX_PLIST_DEFINITION_BYTES: u64 = 1024 * 1024;

/// What `launchctl` says when the job is not in the domain, as opposed to when
/// it could not answer at all.
const NO_SUCH_SERVICE: &str = "Could not find service";

/// How long the runtime is given to stop before launchd kills it, and how long
/// launchd holds a failing job down before relaunching it. Both are written
/// into the definition, and the lifecycle waits below are sized against them.
const EXIT_TIMEOUT_SECONDS: u32 = 30;
const THROTTLE_INTERVAL_SECONDS: u32 = 5;

/// How long an install or upgrade has to prove the service healthy before it is
/// rolled back.
const HEALTH_GATE: Duration = Duration::from_secs(30);

/// How often the health gate asks.
const HEALTH_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// How long one uninterrupted run stands in for an unanswered health probe: a
/// runtime that has served this long on the same start is up, whatever the
/// recorded health URL points at. Measured on the clock and against launchd's
/// own run counter, not by counting polls.
const SETTLE_WINDOW: Duration = Duration::from_secs(10);

/// Consecutive `failed` readings that end the health gate early.
///
/// One is not enough: a job launchd is relaunching passes through several
/// states on the way, and calling the first one terminal would roll back an
/// install that was about to succeed.
const FAILED_READINGS_BEFORE_GIVING_UP: u32 = 2;

/// How long a `start`, `stop`, or `restart` has to reach the state it asked
/// for. `launchctl` returns as soon as launchd has been told, not once the
/// runtime has acted, and the runtime gets [`EXIT_TIMEOUT_SECONDS`] to stop, so
/// this has to outlast that with room to spare.
const LIFECYCLE_POLL_INTERVAL: Duration = Duration::from_millis(250);
const LIFECYCLE_ATTEMPTS: u32 = 160;

/// How long to wait for a job to leave the domain after `bootout` was accepted,
/// and how long when `bootout` itself failed — nothing is coming in that case,
/// so it is only enough to catch a job that was already on its way out.
const BOOTOUT_ATTEMPTS: u32 = 160;
const BOOTOUT_GRACE_ATTEMPTS: u32 = 4;
const BOOTOUT_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// A short cushion for a `bootstrap` that lands while launchd is still settling
/// after the previous job left the domain.
const BOOTSTRAP_ATTEMPTS: u32 = 5;
const BOOTSTRAP_RETRY_INTERVAL: Duration = Duration::from_millis(200);

/// Derive this instance directory's job label.
pub(super) fn job_label_for_dir(dir: &Path) -> String {
    format!(
        "{LABEL_PREFIX}.{stem}",
        stem = super::name_stem_for_dir(dir)
    )
}

/// The instance stem a label carries, or `None` for a label this command did
/// not write.
fn stem_of_label(label: &str) -> Option<&str> {
    label
        .strip_prefix(LABEL_PREFIX)
        .and_then(|rest| rest.strip_prefix('.'))
        .filter(|stem| !stem.is_empty())
}

/// The GUI domain of the account running this command.
pub(super) fn gui_domain() -> String {
    format!("gui/{}", nix::unistd::Uid::effective().as_raw())
}

/// The launchd domain a scope's jobs live in.
fn domain_target(scope: ServiceScope) -> String {
    match scope {
        ServiceScope::System => "system".to_string(),
        ServiceScope::User => gui_domain(),
    }
}

/// How `launchctl` is asked about one job. Always the exact domain and label:
/// a lifecycle command that fell back to another target would drive a service
/// belonging to a different instance.
fn service_target(label: &str, scope: ServiceScope) -> String {
    format!("{}/{label}", domain_target(scope))
}

/// Everything this back end does outside its own process.
///
/// Every `launchctl` call, the health probe, and the waiting between polls go
/// through here, so the whole lifecycle — including the states a test host
/// cannot be put into on demand, like a service that never becomes healthy — is
/// exercised deterministically against a scripted host.
pub(super) trait LaunchdHost {
    /// Run a command to completion and capture what it said.
    ///
    /// # Errors
    ///
    /// Returns an error when the command could not be run at all, which is a
    /// different answer from a command that ran and reported a failure.
    fn output(&self, program: &str, args: &[&str]) -> std::io::Result<CommandOutput>;

    /// What the instance answered at `url` right now.
    fn probe_health(&self, url: &str) -> HealthProbe;

    /// Now, on a clock that only moves forward.
    ///
    /// The gates are bounded by this rather than by a count of polls: a health
    /// probe can block for seconds, so counting attempts would promise a
    /// 30-second gate and take minutes.
    fn now(&self) -> Instant;

    /// Wait before polling again.
    fn sleep(&self, duration: Duration);
}

/// The host as it really is.
pub(super) struct ProcessHost;

impl LaunchdHost for ProcessHost {
    fn output(&self, program: &str, args: &[&str]) -> std::io::Result<CommandOutput> {
        let program = trusted_supervisor_program(program)?;
        let output = std::process::Command::new(program).args(args).output()?;
        Ok(CommandOutput {
            success: output.status.success(),
            code: output.status.code(),
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
        })
    }

    fn probe_health(&self, url: &str) -> HealthProbe {
        probe_http_health(url)
    }

    fn now(&self) -> Instant {
        Instant::now()
    }

    fn sleep(&self, duration: Duration) {
        std::thread::sleep(duration);
    }
}

/// Resolve the one supervisor tool this back end runs, without consulting
/// `PATH`.
fn trusted_supervisor_program(program: &str) -> std::io::Result<PathBuf> {
    if program != LAUNCHCTL {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("unrecognized launchd supervisor program {program:?}"),
        ));
    }
    let path = PathBuf::from(LAUNCHCTL);
    if path.is_file() {
        return Ok(path);
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!("{LAUNCHCTL} is not present on this host"),
    ))
}

/// What launchd answered when asked about a job.
///
/// The three cases are deliberately distinct. "launchd has no such job" is a
/// fact about the service; "launchd could not be asked" is a fact about the
/// query, and reading the second as the first is exactly how a daemon survives
/// its own removal.
#[derive(Debug, Clone, PartialEq, Eq)]
enum JobReport {
    /// launchd has the job, and this is its report.
    Held(String),
    /// launchd answered, in as many words, that it has no such job.
    Absent,
    /// launchd could not be asked, and this is why.
    Unanswerable(String),
}

/// Ask launchd about one job.
fn print_job(host: &dyn LaunchdHost, label: &str, scope: ServiceScope) -> JobReport {
    let target = service_target(label, scope);
    match host.output(LAUNCHCTL, &["print", &target]) {
        Ok(output) if output.success => JobReport::Held(output.stdout),
        Ok(output) => {
            let said = format!("{} {}", output.stdout, output.stderr);
            if said.contains(NO_SUCH_SERVICE) {
                JobReport::Absent
            } else {
                JobReport::Unanswerable(output.describe_failure())
            }
        }
        Err(e) => JobReport::Unanswerable(format!(
            "run `{}`: {e}",
            launchctl_command(scope, &["print", &target])
        )),
    }
}

/// Map what `launchctl print` reports onto the normalized vocabulary.
///
/// launchd has no `failed` state of its own: a job that ended badly is simply
/// not running, with the exit code that ended it still on the report. So the
/// exit code is what tells a service an operator stopped from one that fell
/// over — which is the same distinction systemd draws between `inactive` and
/// `failed`.
fn normalize_launchd_state(printed: &str) -> ServiceState {
    match top_level_field(printed, "state").unwrap_or_default().trim() {
        "running" => ServiceState::Running,
        // launchd is about to run it, or is holding it down for its throttle
        // interval before relaunching it.
        "spawn scheduled" | "spawning" | "waiting" => ServiceState::Starting,
        "not running" | "exited" => match last_exit_code(printed) {
            Some(0) | None => ServiceState::Stopped,
            Some(_) => ServiceState::Failed,
        },
        // Including the empty answer of a report with no `state` line at all,
        // which is not a state to invent an outcome for.
        _ => ServiceState::Unavailable,
    }
}

/// The exit status of the job's last run, or `None` when it has never exited
/// or launchd did not say.
fn last_exit_code(printed: &str) -> Option<i32> {
    top_level_field(printed, "last exit code")?.parse().ok()
}

/// launchd's own counters for a job that is not running as it should be.
///
/// `runs = 0` is the difference between a job launchd never managed to spawn —
/// a program it cannot execute, a working directory the account cannot reach —
/// and one that started and then ended, which the exit code accounts for. The
/// state alone reads the same for both, so both are named.
fn launchd_counters(printed: &str) -> String {
    let reported: Vec<String> = ["runs", "last exit code"]
        .into_iter()
        .filter_map(|key| top_level_field(printed, key).map(|value| format!("{key} = {value}")))
        .collect();
    if reported.is_empty() {
        String::new()
    } else {
        format!(" ({})", reported.join(", "))
    }
}

/// Which run of the job is up: how many times launchd has started it, and the
/// process it is currently running as.
///
/// `None` when the report does not name both, in which case the health gate
/// falls back to what sampling alone can see.
fn run_identity(printed: &str) -> Option<String> {
    let runs = top_level_field(printed, "runs")?;
    let pid = top_level_field(printed, "pid")?;
    Some(format!("{runs}/{pid}"))
}

/// Read a `key = value` field from the top level of `launchctl print` output.
///
/// The report nests: a job's own fields sit at one tab, and the same names
/// reappear deeper for its endpoints. Matching the single-tab indent is what
/// keeps an endpoint's `state` from being read as the job's.
fn top_level_field(printed: &str, key: &str) -> Option<String> {
    let prefix = format!("\t{key} = ");
    printed
        .lines()
        .find_map(|line| line.strip_prefix(prefix.as_str()))
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// Whether launchd's own database has this label disabled. `None` when it could
/// not be asked, or when the label is not listed at all — which is the same as
/// enabled, and must not be reported as a persistence problem.
fn label_is_disabled(host: &dyn LaunchdHost, label: &str, scope: ServiceScope) -> Option<bool> {
    let domain = domain_target(scope);
    let output = host.output(LAUNCHCTL, &["print-disabled", &domain]).ok()?;
    if !output.success {
        return None;
    }
    parse_disabled(&output.stdout, label)
}

/// Parse one label's verdict out of `launchctl print-disabled`.
///
/// The value has been spelled both ways across macOS releases — `=> true` and
/// `=> disabled` — so both are read.
fn parse_disabled(printed: &str, label: &str) -> Option<bool> {
    let needle = format!("\"{label}\" => ");
    printed.lines().find_map(|line| {
        let value = line.trim().strip_prefix(needle.as_str())?;
        match value.trim() {
            "disabled" | "true" => Some(true),
            "enabled" | "false" => Some(false),
            _ => None,
        }
    })
}

/// Where definitions of `scope` live. `None` when the account has no
/// discoverable home, which is the one case a `LaunchAgent` path cannot be
/// derived from.
fn definition_dir(scope: ServiceScope) -> Option<PathBuf> {
    match scope {
        ServiceScope::System => Some(PathBuf::from(LAUNCH_DAEMON_DIR)),
        ServiceScope::User => Some(dirs::home_dir()?.join(LAUNCH_AGENT_SUBDIR)),
    }
}

/// The directory holding the `spiced` copy this instance's service runs.
///
/// Per instance, and inside the domain that owns the service: a system service
/// runs a root-owned copy no operator can replace behind launchd's back, and a
/// user service runs one out of the account's own data directory.
fn runtime_dir(instance_dir: &Path, scope: ServiceScope) -> Option<PathBuf> {
    let stem = super::name_stem_for_dir(instance_dir);
    match scope {
        ServiceScope::System => Some(Path::new(super::RUNTIME_STAGE_DIR).join(stem)),
        ServiceScope::User => Some(dirs::data_local_dir()?.join(USER_RUNTIME_SUBDIR).join(stem)),
    }
}

/// The directory this instance's service writes its bounded log files into.
fn log_dir(stem: &str, scope: ServiceScope) -> Option<PathBuf> {
    match scope {
        ServiceScope::System => Some(Path::new(SYSTEM_LOG_ROOT).join(stem)),
        ServiceScope::User => Some(dirs::home_dir()?.join(USER_LOG_SUBDIR).join(stem)),
    }
}

/// The log directory of an installed service, recovered from its label.
fn log_dir_for_label(label: &str, scope: ServiceScope) -> Option<PathBuf> {
    log_dir(stem_of_label(label)?, scope)
}

/// Everything an install writes, resolved before it writes any of it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct InstallPaths {
    plist: PathBuf,
    runtime_dir: PathBuf,
    runtime: PathBuf,
    log_dir: PathBuf,
}

/// Resolve where this install will write, or say which directory could not be
/// derived.
fn install_paths(label: &str, instance_dir: &Path, scope: ServiceScope) -> Result<InstallPaths> {
    let missing = |what: &str| Error::CloudConnectIo {
        message: format!(
            "locate the {what} a {scope} service needs: this account has no such directory. Log \
             in as the account that will run the instance, or install a system service with \
             `sudo spice cloud service install`."
        ),
    };
    let definitions =
        definition_dir(scope).ok_or_else(|| missing("launchd definition directory"))?;
    let runtime_dir =
        runtime_dir(instance_dir, scope).ok_or_else(|| missing("Spice runtime directory"))?;
    let stem = super::name_stem_for_dir(instance_dir);
    let log_dir = log_dir(&stem, scope).ok_or_else(|| missing("Spice service log directory"))?;
    Ok(InstallPaths {
        plist: definitions.join(format!("{label}{PLIST_SUFFIX}")),
        runtime: runtime_dir.join(super::STAGED_RUNTIME_FILE),
        runtime_dir,
        log_dir,
    })
}

/// Render the definition for an instance.
///
/// `instance_dir` is baked in as `WorkingDirectory` so the job resolves its
/// spicepod from the directory the operator enrolled, not from wherever launchd
/// happens to start it. `config_dir` preserves the resolved Spice state
/// directory, and `runtime` is the staged binary this one service executes.
///
/// `account` is `Some` only for a `LaunchDaemon`: a `LaunchAgent` already runs
/// as its owner, and launchd ignores `UserName` in one.
fn render_plist(
    label: &str,
    instance_dir: &Path,
    config_dir: &Path,
    runtime: &Path,
    log_dir: &Path,
    account: Option<(String, String)>,
) -> Result<String> {
    use std::fmt::Write as _;

    let mut plist = String::with_capacity(1024);
    // Writing into a String is infallible; the Result exists only to satisfy
    // the `Write` trait.
    let _ = write!(
        plist,
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>Label</key>
	<string>{label}</string>
	<!-- launchd bounds nothing it writes to StandardOutPath, so neither is
	     named here: the runtime writes its own bounded, rotating files under
	     the directory below, and `spice cloud logs` reads them. -->
	<key>ProgramArguments</key>
	<array>
		<string>{runtime}</string>
		<string>--service-log-dir</string>
		<string>{log_dir}</string>
	</array>
	<key>WorkingDirectory</key>
	<string>{instance_dir}</string>
"#,
        label = escape_plist_text(label)?,
        runtime = escape_plist_path(runtime)?,
        log_dir = escape_plist_path(log_dir)?,
        instance_dir = escape_plist_path(instance_dir)?,
    );
    if let Some((user, group)) = account {
        let _ = write!(
            plist,
            "\t<key>UserName</key>\n\t<string>{user}</string>\n\
             \t<key>GroupName</key>\n\t<string>{group}</string>\n",
            user = escape_plist_text(&user)?,
            group = escape_plist_text(&group)?,
        );
    }
    let _ = write!(
        plist,
        r"	<key>EnvironmentVariables</key>
	<dict>
		<key>SPICE_CONFIG_DIR</key>
		<string>{config_dir}</string>
	</dict>
	<key>RunAtLoad</key>
	<true/>
	<!-- A deployment applies to the running instance and never ends its
	     process, so this is what brings the instance back from the things that
	     do end it — an OOM kill, an unhandled failure. A clean exit is left
	     alone: it is what an operator's `stop` produces. An unconditional
	     KeepAlive would relaunch that one too. -->
	<key>KeepAlive</key>
	<dict>
		<key>SuccessfulExit</key>
		<false/>
	</dict>
	<key>ThrottleInterval</key>
	<integer>{throttle}</integer>
	<key>ExitTimeOut</key>
	<integer>{exit_timeout}</integer>
	<!-- Background would have macOS throttle the runtime's I/O. -->
	<key>ProcessType</key>
	<string>Standard</string>
</dict>
</plist>
",
        config_dir = escape_plist_path(config_dir)?,
        throttle = THROTTLE_INTERVAL_SECONDS,
        exit_timeout = EXIT_TIMEOUT_SECONDS,
    );
    Ok(plist)
}

/// Escape a path for XML element content, refusing what cannot be represented.
///
/// Paths are operator-supplied, so a directory named `a&b` must not produce a
/// definition launchd cannot parse. Control characters are rejected rather than
/// written, because a plist carrying one is a plist `plutil` rejects and
/// launchd silently declines to load.
fn escape_plist_path(path: &Path) -> Result<String> {
    let value = path.to_str().ok_or_else(|| Error::InvalidArgument {
        message: format!(
            "Failed to install the Spice Cloud Connect service: path {} is not valid UTF-8 and cannot be represented safely in a launchd definition.",
            path.display()
        ),
    })?;
    escape_plist_text(value).map_err(|_| Error::InvalidArgument {
        message: format!(
            "Failed to install the Spice Cloud Connect service: path {} contains a control character and cannot be represented safely in a launchd definition.",
            path.display()
        ),
    })
}

/// Escape a string for XML element content.
fn escape_plist_text(value: &str) -> Result<String> {
    if value.chars().any(|ch| ch.is_control() && ch != '\t') {
        return Err(Error::InvalidArgument {
            message: format!(
                "Failed to install the Spice Cloud Connect service: {value:?} contains a control character and cannot be represented safely in a launchd definition."
            ),
        });
    }
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            _ => out.push(ch),
        }
    }
    Ok(out)
}

/// Reverse [`escape_plist_text`], plus the two entities a hand-edited
/// definition may carry. `&amp;` is undone last so `&amp;lt;` reads back as
/// `&lt;`.
fn unescape_plist_text(value: &str) -> String {
    value
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

/// Parse the `WorkingDirectory` value out of a definition.
fn parse_working_dir(plist: &str) -> Option<PathBuf> {
    first_string_after_key(plist, "WorkingDirectory").map(PathBuf::from)
}

/// Parse the binary the job runs — the first entry of `ProgramArguments` —
/// dropping its arguments.
fn parse_program(plist: &str) -> Option<PathBuf> {
    first_string_after_key(plist, "ProgramArguments").map(PathBuf::from)
}

/// Parse the Cloud identity the definition activates.
fn parse_config_dir(plist: &str) -> Option<PathBuf> {
    first_string_after_key(plist, "SPICE_CONFIG_DIR").map(PathBuf::from)
}

/// The first `<string>` following `<key>key</key>`, unescaped.
///
/// Enough for the values read back: each is either a bare string or the first
/// element of an array. Every value written is escaped, so no value can contain
/// a key element and be mistaken for one.
fn first_string_after_key(plist: &str, key: &str) -> Option<String> {
    let marker = format!("<key>{key}</key>");
    let rest = &plist[plist.find(&marker)? + marker.len()..];
    let open = rest.find("<string>")? + "<string>".len();
    let close = open + rest[open..].find("</string>")?;
    let value = unescape_plist_text(rest[open..close].trim());
    (!value.is_empty()).then_some(value)
}

/// Whether this host can host a service in `scope`.
///
/// A `LaunchAgent` needs a GUI domain to be bootstrapped into, which does not
/// exist for an account with no desktop login — over SSH, or on a headless
/// host. Detected here so the refusal comes before anything is written rather
/// than as a `launchctl bootstrap` failure halfway through an install.
fn preflight(
    host: &dyn LaunchdHost,
    scope: ServiceScope,
) -> std::result::Result<(), PreflightFailure> {
    if !Path::new(LAUNCHCTL).is_file() {
        return Err(PreflightFailure::LaunchctlUnavailable);
    }
    if scope == ServiceScope::User && !gui_domain_is_available(host) {
        return Err(PreflightFailure::LaunchdGuiDomainUnavailable);
    }
    Ok(())
}

/// Whether launchd has a GUI domain for this account right now.
fn gui_domain_is_available(host: &dyn LaunchdHost) -> bool {
    let domain = gui_domain();
    host.output(LAUNCHCTL, &["print", &domain])
        .is_ok_and(|output| output.success)
}

/// Resolve the account the service will run as and verify it already owns the
/// enrolled state.
///
/// `None` for a user service: a `LaunchAgent` already runs as the account that
/// owns the state, so there is nothing additional to verify here.
fn prepare_account(request: &InstallRequest<'_>) -> Result<Option<ServiceAccount>> {
    if request.scope == ServiceScope::User {
        return Ok(None);
    }
    // Allow: a `LaunchDaemon` installed by a direct root session runs as root,
    // because root already owns both the state and the service and there is no
    // operator to drop to.
    let account = super::service_account(request.instance_dir, super::RootFallback::Allow)?;
    super::verify_config_ownership(request.config_dir, account)?;
    Ok(Some(account))
}

/// launchd names accounts rather than taking the numeric ids systemd accepts.
fn account_names(account: ServiceAccount) -> Result<(String, String)> {
    let user = nix::unistd::User::from_uid(nix::unistd::Uid::from_raw(account.uid))
        .map_err(|e| Error::CloudConnectIo {
            message: format!("look up service user {}: {e}", account.uid),
        })?
        .ok_or_else(|| Error::InvalidArgument {
            message: format!(
                "Failed to install the Spice Cloud Connect service: uid {} has no local account name, and launchd names the account a daemon runs as rather than numbering it. Create the account or install from one that exists.",
                account.uid
            ),
        })?;
    let group = nix::unistd::Group::from_gid(nix::unistd::Gid::from_raw(account.gid))
        .map_err(|e| Error::CloudConnectIo {
            message: format!("look up service group {}: {e}", account.gid),
        })?
        .ok_or_else(|| Error::InvalidArgument {
            message: format!(
                "Failed to install the Spice Cloud Connect service: gid {} has no local group name, and launchd names the group a daemon runs as rather than numbering it. Create the group or install from an account whose group exists.",
                account.gid
            ),
        })?;
    Ok((user.name, group.name))
}

fn install(host: &dyn LaunchdHost, request: &InstallRequest<'_>) -> Result<InstalledService> {
    let label = job_label_for_dir(request.instance_dir);
    let paths = install_paths(&label, request.instance_dir, request.scope)?;
    let account = prepare_account(request)?;
    install_at(host, request, &label, &paths, account)
}

/// Install into explicitly resolved paths, and leave the previous installation
/// in force if the new one does not come up healthy.
fn install_at(
    host: &dyn LaunchdHost,
    request: &InstallRequest<'_>,
    label: &str,
    paths: &InstallPaths,
    account: Option<ServiceAccount>,
) -> Result<InstalledService> {
    ensure_stage_dir(&paths.runtime_dir, request.scope)?;
    ensure_log_dir(&paths.log_dir, request.scope, account)?;

    // Captured before anything is overwritten: an upgrade that does not come up
    // has to be able to put back exactly what was serving before it.
    let rollback = Rollback::capture(host, paths, label, request.scope)?;

    let applied = apply(host, request, label, paths, account);
    let verdict = match applied {
        Ok(()) => health_gate(host, label, request.scope, request.health_url),
        Err(err) => Err(folded(&err.to_string())),
    };

    match verdict {
        Ok(()) => {
            rollback.discard();
            Ok(InstalledService {
                name: label.to_string(),
                path: paths.plist.clone(),
                working_dir: request.instance_dir.to_path_buf(),
                config_dir: Some(request.config_dir.to_path_buf()),
                runtime: paths.runtime.clone(),
            })
        }
        Err(why) => {
            let restored = rollback.restore(host, label, request.scope);
            Err(Error::CloudConnectIo {
                message: format!(
                    "install the Spice Cloud Connect service {label} (launchd): {why}. {restored} \
                     Read what the runtime said with `cd {dir} && spice cloud logs -n 200`.",
                    dir = request.instance_dir.display(),
                ),
            })
        }
    }
}

/// Stage the runtime, write the definition, and hand the job to launchd.
fn apply(
    host: &dyn LaunchdHost,
    request: &InstallRequest<'_>,
    label: &str,
    paths: &InstallPaths,
    account: Option<ServiceAccount>,
) -> Result<()> {
    // launchd bootstraps a job whose program cannot be executed and then
    // reports nothing: the job exists, never runs, and the instance is silently
    // offline. Running the staged copy once turns every cause of that — a
    // quarantine attribute, a code signature the kernel rejects, the wrong
    // architecture — into an error before the job is created.
    super::stage_runtime_at(
        request.spiced_path,
        &paths.runtime,
        move |staged, source| verify_staged_runtime_executes(staged, source, account),
    )?;

    let names = account.map(account_names).transpose()?;
    let plist = render_plist(
        label,
        request.instance_dir,
        request.config_dir,
        &paths.runtime,
        &paths.log_dir,
        names,
    )?;
    write_plist(&paths.plist, plist.as_bytes(), request.scope)?;

    // Before bootstrapping, not after: launchd keeps serving the definition a
    // job was bootstrapped with, so loading over a job that is still there
    // would leave the previous definition — and the previous binary — in force
    // while this reported the new one.
    bootout(host, label, request.scope)?;
    // A label `launchctl disable` has touched stays disabled in launchd's own
    // override database, where `bootstrap` succeeds and the job still never
    // runs. Only flipped when it is actually set: `launchctl enable` writes a
    // persistent per-label record that nothing can remove afterwards, so
    // enabling unconditionally would leave one behind for every service ever
    // installed on the host.
    if label_is_disabled(host, label, request.scope) == Some(true)
        && let Err(err) = launchctl(
            host,
            request.scope,
            &["enable", &service_target(label, request.scope)],
        )
    {
        tracing::debug!("launchctl enable {label}: {err}");
    }
    bootstrap(host, request.scope, &paths.plist)?;
    // `RunAtLoad` starts it, and this is the idempotent way to say so
    // explicitly rather than inferring it from the definition.
    kickstart(host, label, request.scope, false)
}

/// Wait for the service to prove it is serving, or say what it is doing
/// instead.
///
/// Every success requires one uninterrupted run of [`SETTLE_WINDOW`]. A health
/// response alone is not enough: the default URL is host-wide, so another
/// instance could answer while this job crash-loops. The probe still improves
/// the diagnostic, while an instance whose configured endpoint differs from the
/// CLI's can prove itself through the same-run settle window alone.
///
/// What neither accepts is the failure this gate exists for — a runtime that
/// exits and is relaunched. Sampling the state cannot see that on its own,
/// because launchd may well be back to `running` by the next poll, so the run
/// is identified by launchd's run counter and the pid it is running as: a
/// relaunch between two samples changes it, and the window starts again.
fn health_gate(
    host: &dyn LaunchdHost,
    label: &str,
    scope: ServiceScope,
    health_url: &str,
) -> std::result::Result<(), String> {
    let probeable = is_probeable(health_url);
    let deadline = host.now() + HEALTH_GATE;
    let mut failures = 0;
    // When the run that is up now began, and which run it is.
    let mut running_since: Option<(Instant, Option<String>)> = None;
    let mut saw_unhealthy = false;
    let mut last = format!(
        "launchd did not report {label} as running within {}s",
        HEALTH_GATE.as_secs()
    );

    loop {
        let sampled_at = host.now();
        let report = print_job(host, label, scope);
        let state = match &report {
            JobReport::Held(printed) => normalize_launchd_state(printed),
            JobReport::Absent | JobReport::Unanswerable(_) => ServiceState::Unavailable,
        };
        match (state, &report) {
            (ServiceState::Running, JobReport::Held(printed)) => {
                failures = 0;
                let identity = run_identity(printed);
                if running_since
                    .as_ref()
                    .is_none_or(|(_, running)| *running != identity)
                {
                    running_since = Some((sampled_at, identity));
                    saw_unhealthy = false;
                }
                let probe = if probeable {
                    host.probe_health(health_url)
                } else {
                    HealthProbe::Unreachable
                };
                if probe == HealthProbe::Unhealthy {
                    saw_unhealthy = true;
                }
                let uninterrupted = running_since.as_ref().map_or(Duration::ZERO, |(since, _)| {
                    host.now().saturating_duration_since(*since)
                });
                if uninterrupted >= SETTLE_WINDOW
                    && (probe == HealthProbe::Healthy
                        || (probe == HealthProbe::Unreachable && !saw_unhealthy))
                {
                    return Ok(());
                }
                last = match probe {
                    HealthProbe::Healthy => format!(
                        "{health_url} answered, but {label} has not stayed in the same run for {}s",
                        SETTLE_WINDOW.as_secs()
                    ),
                    HealthProbe::Unhealthy => format!(
                        "{health_url} explicitly reported an unhealthy HTTP status for {label}"
                    ),
                    HealthProbe::Unreachable if saw_unhealthy => format!(
                        "{health_url} reported an unhealthy HTTP status for this run of {label} and is now unreachable"
                    ),
                    HealthProbe::Unreachable => format!(
                        "{label} has not stayed running for {}s, and has not answered {health_url}",
                        SETTLE_WINDOW.as_secs()
                    ),
                };
            }
            (ServiceState::Failed, JobReport::Held(printed)) => {
                running_since = None;
                saw_unhealthy = false;
                failures += 1;
                last = format!(
                    "launchd reports {label} as not running after exiting with {}",
                    last_exit_code(printed).unwrap_or_default()
                );
                if failures >= FAILED_READINGS_BEFORE_GIVING_UP {
                    return Err(last);
                }
            }
            (_, JobReport::Absent) => {
                running_since = None;
                saw_unhealthy = false;
                failures = 0;
                last = format!("launchd is not holding a job named {label}");
            }
            (_, JobReport::Unanswerable(why)) => {
                running_since = None;
                saw_unhealthy = false;
                failures = 0;
                last = format!("launchd could not be asked about {label}: {why}");
            }
            (other, JobReport::Held(printed)) => {
                running_since = None;
                saw_unhealthy = false;
                failures = 0;
                last = format!(
                    "launchd reports {label} as {other} rather than running{}",
                    launchd_counters(printed)
                );
            }
        }

        // Checked after the sample, so a probe that blocked past the deadline
        // still ends the gate rather than buying another round.
        if host.now() >= deadline {
            return Err(format!(
                "{last}, {}s after it was started",
                HEALTH_GATE.as_secs()
            ));
        }
        host.sleep(HEALTH_POLL_INTERVAL);
    }
}

/// What an install has to be able to put back.
///
/// The definition and the runtime together decide what the host *runs*, and
/// whether launchd is holding the job decides *whether* it runs — so all three
/// are captured. Restoring the files alone would leave a service that was
/// deliberately stopped started again by a rolled-back upgrade.
struct Rollback {
    plist: PathBuf,
    /// The definition that was in force, or `None` when nothing was installed.
    previous_plist: Option<Vec<u8>>,
    runtime: PathBuf,
    /// A second name for the runtime that was in force, or `None` when there
    /// was none.
    previous_runtime: Option<PathBuf>,
    /// Whether launchd was holding the job. `None` means launchd could not be
    /// asked, and that part of the state is left as the install found it rather
    /// than guessed at.
    was_loaded: Option<bool>,
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
    ///
    /// Only a genuinely absent file reads as "nothing was installed". A
    /// definition or runtime that exists but cannot be read is reported,
    /// because taking it for an empty slot would have the rollback *delete* the
    /// installation it was supposed to protect.
    fn capture(
        host: &dyn LaunchdHost,
        paths: &InstallPaths,
        label: &str,
        scope: ServiceScope,
    ) -> Result<Self> {
        let previous_plist = match std::fs::read(&paths.plist) {
            Ok(plist) => Some(plist),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => {
                return Err(Error::CloudConnectIo {
                    message: format!(
                        "read the installed launchd definition {} before replacing it: {e}. The \
                         upgrade was not started, so the installed service is untouched.",
                        paths.plist.display()
                    ),
                });
            }
        };
        let previous_runtime = Self::capture_runtime(paths)?;
        // Only meaningful when there is something to restore.
        let was_loaded = if previous_plist.is_some() {
            match print_job(host, label, scope) {
                JobReport::Held(_) => Some(true),
                JobReport::Absent => Some(false),
                JobReport::Unanswerable(_) => None,
            }
        } else {
            None
        };

        Ok(Self {
            plist: paths.plist.clone(),
            previous_plist,
            runtime: paths.runtime.clone(),
            previous_runtime,
            was_loaded,
        })
    }

    /// Hold on to the runtime that is in force, so an upgrade can be undone.
    fn capture_runtime(paths: &InstallPaths) -> Result<Option<PathBuf>> {
        let io_error = |e: std::io::Error| Error::CloudConnectIo {
            message: format!(
                "keep a copy of the Spice runtime {} before upgrading it: {e}. The upgrade was \
                 not started, so the installed service is untouched.",
                paths.runtime.display()
            ),
        };

        match std::fs::symlink_metadata(&paths.runtime) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(io_error(e)),
            Ok(metadata) if !metadata.is_file() => {
                return Err(Error::InvalidArgument {
                    message: format!(
                        "Failed to install the Spice Cloud Connect service: {} is not a regular \
                         file, so the runtime it stands for cannot be preserved across the \
                         upgrade. Remove it and re-run `spice cloud service install`.",
                        paths.runtime.display()
                    ),
                });
            }
            Ok(_) => {}
        }

        let backup = Self::backup_name(&paths.runtime);
        match std::fs::remove_file(&backup) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(io_error(e)),
        }
        if std::fs::hard_link(&paths.runtime, &backup).is_err() {
            std::fs::copy(&paths.runtime, &backup).map_err(io_error)?;
        }
        Ok(Some(backup))
    }

    /// Let go of the captured runtime, after the new one proved healthy.
    fn discard(&self) {
        if let Some(backup) = &self.previous_runtime {
            let _ = std::fs::remove_file(backup);
        }
    }

    /// Put back what was in force, and say exactly what an operator is left
    /// with.
    ///
    /// Every step is attempted even after an earlier one fails — a definition
    /// that could not be rewritten must not stop the runtime from being put
    /// back — but nothing is *claimed*: a restoration that did not fully
    /// succeed reports which steps failed and the commands that finish the job
    /// by hand, because an operator told recovery worked will not go looking.
    ///
    /// A service that was loaded comes back loaded, and `RunAtLoad` starts it.
    /// One that was booted out — which is the state `spice cloud service
    /// stop` leaves — stays booted out.
    fn restore(&self, host: &dyn LaunchdHost, label: &str, scope: ServiceScope) -> String {
        let mut failed: Vec<String> = Vec::new();
        let mut manual: Vec<String> = Vec::new();

        match &self.previous_plist {
            Some(plist) => {
                if let Err(err) = write_plist(&self.plist, plist, scope) {
                    failed.push(format!("rewrite the definition {}", self.plist.display()));
                    tracing::debug!("restore {}: {err}", self.plist.display());
                }
            }
            None => Self::remove_file(&self.plist, "remove the definition", &mut failed),
        }

        match &self.previous_runtime {
            Some(backup) => {
                if let Err(err) = std::fs::rename(backup, &self.runtime) {
                    failed.push(format!(
                        "put the previous runtime back as {} (it is at {})",
                        self.runtime.display(),
                        backup.display()
                    ));
                    tracing::debug!("restore {}: {err}", self.runtime.display());
                }
            }
            None => Self::remove_file(&self.runtime, "remove the staged runtime", &mut failed),
        }
        // The stamp describes the source the staged runtime was copied from,
        // and the restored binary did not come from it. Dropping it is what
        // makes the next install copy again instead of trusting a stamp that
        // now describes the runtime this rollback just removed.
        Self::remove_file(
            &self.runtime.with_extension("stamp"),
            "remove the staging stamp",
            &mut failed,
        );

        // The job that the failed install bootstrapped has to go whatever
        // happens next: it is running the definition that did not work.
        if let Err(err) = bootout(host, label, scope) {
            let command = launchctl_command(scope, &["bootout", &service_target(label, scope)]);
            failed.push(format!("run `{command}`"));
            manual.push(command);
            tracing::debug!("restore: {err}");
        } else if self.previous_plist.is_some()
            && self.was_loaded != Some(false)
            && let Err(err) = bootstrap(host, scope, &self.plist)
        {
            let command = launchctl_command(
                scope,
                &[
                    "bootstrap",
                    &domain_target(scope),
                    &self.plist.to_string_lossy(),
                ],
            );
            failed.push(format!("run `{command}`"));
            manual.push(command);
            tracing::debug!("restore: {err}");
        }

        Self::describe(
            if self.previous_plist.is_none() {
                "Nothing was left installed for this directory."
            } else {
                "The service and runtime that were installed before have been put back."
            },
            &failed,
            &manual,
        )
    }

    /// Delete a file this rollback owns, recording the failure rather than
    /// swallowing it.
    fn remove_file(path: &Path, what: &str, failed: &mut Vec<String>) {
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                failed.push(format!("{what} {}", path.display()));
                tracing::debug!("restore {}: {e}", path.display());
            }
        }
    }

    /// The sentence the install failure carries: what the rollback achieved,
    /// and what it could not.
    fn describe(succeeded: &str, failed: &[String], manual: &[String]) -> String {
        if failed.is_empty() {
            return succeeded.to_string();
        }
        let mut message = format!(
            "The previous installation could not be fully restored: this could not {}.",
            failed.join("; ")
        );
        if !manual.is_empty() {
            use std::fmt::Write as _;
            // Writing into a String is infallible; the Result exists only to
            // satisfy the `Write` trait.
            let _ = write!(
                message,
                " Finish it by hand with `{}`.",
                manual.join("` and `")
            );
        }
        message
    }
}

/// Create the directory a service's runtime is staged into, and refuse it
/// unless only the domain that owns the service can change what it holds.
fn ensure_stage_dir(dir: &Path, scope: ServiceScope) -> Result<()> {
    match scope {
        ServiceScope::System => super::ensure_root_only_dir(dir),
        ServiceScope::User => super::ensure_account_only_dir(dir),
    }
}

/// Create the directory a service writes its log files into.
///
/// A system service's logs live under a root-only tree, but the daemon itself
/// runs as the operator, so the leaf is handed to that account — the only
/// directory this installer changes the ownership of, and one it created. The
/// path *to* it stays root-only, so nobody else can substitute the directory
/// root is about to hand over.
fn ensure_log_dir(dir: &Path, scope: ServiceScope, account: Option<ServiceAccount>) -> Result<()> {
    use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _, PermissionsExt as _};

    if scope == ServiceScope::User {
        return super::ensure_account_only_dir(dir);
    }

    let root = dir.parent().unwrap_or_else(|| Path::new(SYSTEM_LOG_ROOT));
    super::ensure_root_only_dir(root)?;

    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o755)
        .create(dir)
        .map_err(|e| Error::CloudConnectIo {
            message: format!("create {}: {e}", dir.display()),
        })?;

    let uid = account.map_or(0, |account| account.uid);
    let gid = account.map_or(0, |account| account.gid);
    let meta = std::fs::symlink_metadata(dir).map_err(|e| Error::CloudConnectIo {
        message: format!("inspect {}: {e}", dir.display()),
    })?;
    if meta.file_type().is_symlink() {
        return Err(Error::InvalidArgument {
            message: format!(
                "Failed to install the Spice Cloud Connect service: {dir} is a symlink. The \
                 service's log files are managed by Spice, so this must be a real directory. \
                 Replace it and re-run `sudo spice cloud service install`.",
                dir = dir.display()
            ),
        });
    }
    std::os::unix::fs::chown(dir, Some(uid), Some(gid)).map_err(|e| Error::CloudConnectIo {
        message: format!(
            "give the service log directory {} to uid {uid}: {e}",
            dir.display()
        ),
    })?;

    let meta = std::fs::symlink_metadata(dir).map_err(|e| Error::CloudConnectIo {
        message: format!("inspect {}: {e}", dir.display()),
    })?;
    let mode = meta.permissions().mode() & 0o7777;
    if meta.uid() != uid || mode & 0o022 != 0 {
        return Err(Error::InvalidArgument {
            message: format!(
                "Failed to install the Spice Cloud Connect service: the service log directory \
                 {dir} is owned by uid {found} with mode {mode:04o}, but the service runs as uid \
                 {uid}. Fix it (`sudo chown {uid} {dir}` and `sudo chmod go-w {dir}`) and re-run \
                 `sudo spice cloud service install`.",
                dir = dir.display(),
                found = meta.uid(),
            ),
        });
    }
    Ok(())
}

/// Write a definition, creating its directory and replacing it atomically.
///
/// launchd refuses a `LaunchDaemon` that is not owned by root or that anyone
/// but root can write, and reports the refusal only to the system log — so
/// ownership and mode are set explicitly and then read back, rather than left
/// to the process umask and to whatever the file happened to be before.
fn write_plist(path: &Path, plist: &[u8], scope: ServiceScope) -> Result<()> {
    use std::fs::OpenOptions;
    use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _};

    let io_error = |e: std::io::Error| Error::CloudConnectIo {
        message: format!("write the launchd definition {}: {e}", path.display()),
    };

    if let Some(parent) = path.parent() {
        match scope {
            ServiceScope::System => super::ensure_root_only_dir(parent)?,
            ServiceScope::User => super::ensure_account_only_dir(parent)?,
        }
    }

    // Written to a sibling and renamed, so launchd never reads a half-written
    // definition, and created with its final mode so it is never briefly
    // writable by anyone else. The sibling is a dotfile: launchd skips those
    // when it walks the directory at boot, so an interrupted install cannot
    // leave behind something it tries to load.
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| Error::CloudConnectIo {
            message: format!(
                "write the launchd definition {}: the path has no file name",
                path.display()
            ),
        })?;
    let staging = path.with_file_name(format!(".{file_name}.incoming"));
    let _ = std::fs::remove_file(&staging);

    // Anything short of the rename leaves the sibling behind, and a
    // half-written definition is not worth keeping.
    let written = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(PLIST_MODE)
            .open(&staging)
            .map_err(io_error)?;
        file.write_all(plist).map_err(io_error)?;
        file.sync_all().map_err(io_error)?;
        drop(file);

        // `create_new` honours the process umask, which can clear bits the mode
        // asked for; set them back before launchd is pointed at the file.
        std::fs::set_permissions(&staging, std::fs::Permissions::from_mode(PLIST_MODE))
            .map_err(io_error)?;
        if scope == ServiceScope::System {
            std::os::unix::fs::chown(&staging, Some(0), Some(0)).map_err(|e| {
                Error::CloudConnectIo {
                    message: format!(
                        "give the launchd daemon definition {} to root: {e}",
                        path.display()
                    ),
                }
            })?;
        }
        Ok(())
    })();
    if let Err(err) = written {
        let _ = std::fs::remove_file(&staging);
        return Err(err);
    }

    std::fs::rename(&staging, path).map_err(|e| {
        let _ = std::fs::remove_file(&staging);
        io_error(e)
    })?;

    if scope == ServiceScope::System {
        let meta = std::fs::metadata(path).map_err(io_error)?;
        let mode = meta.permissions().mode() & 0o7777;
        if meta.uid() != 0 || meta.gid() != 0 || mode & 0o022 != 0 {
            return Err(Error::InvalidArgument {
                message: format!(
                    "Failed to install the Spice Cloud Connect service: {plist_path} is owned by \
                     uid {uid} gid {gid} with mode {mode:04o}. launchd silently refuses to load a \
                     daemon that is not owned by root:wheel and writable only by root. Fix it with \
                     `sudo chown root:wheel {plist_path}` and `sudo chmod 644 {plist_path}`, then \
                     re-run `sudo spice cloud service install`.",
                    plist_path = path.display(),
                    uid = meta.uid(),
                    gid = meta.gid(),
                ),
            });
        }
    }

    Ok(())
}

/// Prove the staged runtime is a binary this kernel will actually execute.
///
/// The copy has to be the subject rather than `source`, because copying is what
/// drops `com.apple.quarantine`: checking the source would condemn a runtime
/// that stages perfectly well.
///
/// `account` is `Some` for a system install, where the probe drops to the
/// unprivileged account launchd will use rather than executing
/// operator-supplied code as root.
fn verify_staged_runtime_executes(
    staged: &Path,
    source: &Path,
    account: Option<ServiceAccount>,
) -> Result<()> {
    use std::os::unix::process::CommandExt as _;

    let mut command = std::process::Command::new(staged);
    command
        .arg("--version")
        // The candidate is operator-supplied. Give it no installer secrets or
        // dynamic-loader overrides while probing it.
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .current_dir("/");
    if let Some(account) = account {
        command.uid(account.uid).gid(account.gid);
    }
    let failure = match command.output() {
        Ok(output) if output.status.success() => return Ok(()),
        Ok(output) => {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            if stderr.is_empty() {
                format!("it exited with {}", output.status)
            } else {
                folded(&stderr)
            }
        }
        Err(e) => e.to_string(),
    };

    // Named even though staging drops it, since it is the likeliest thing the
    // operator can act on.
    let quarantine = if has_quarantine(source) {
        format!(
            " It carries com.apple.quarantine — clear it with `xattr -d com.apple.quarantine {source}`.",
            source = source.display()
        )
    } else {
        String::new()
    };

    Err(Error::InvalidArgument {
        message: format!(
            "Failed to install the Spice Cloud Connect service: a copy of the runtime at {source} \
             does not run: {failure}. launchd would load the job and never start it, so nothing \
             was installed and any service already on this host keeps the runtime it has.\
             {quarantine} Check the binary with `codesign --verify {source}` and \
             `spctl --assess --type execute {source}`, then re-run \
             `spice cloud service install`. See: https://spiceai.org/docs",
            source = source.display(),
        ),
    })
}

/// `true` when `path` carries the quarantine attribute macOS puts on downloads.
fn has_quarantine(path: &Path) -> bool {
    std::process::Command::new("/usr/bin/xattr")
        .args(["-p", "com.apple.quarantine"])
        .arg(path)
        .output()
        .is_ok_and(|output| output.status.success())
}

/// Take the job out of its domain and confirm it is gone. Idempotent.
fn bootout(host: &dyn LaunchdHost, label: &str, scope: ServiceScope) -> Result<()> {
    if matches!(print_job(host, label, scope), JobReport::Absent) {
        return Ok(());
    }

    let target = service_target(label, scope);
    let requested = launchctl(host, scope, &["bootout", &target]);
    if let Err(ref err) = requested {
        tracing::debug!("launchctl bootout {target}: {err}");
    }

    // `bootout` returns once launchd has been told, not once the runtime has
    // stopped, and the runtime gets `ExitTimeOut` to do that. When `bootout`
    // itself failed there is nothing on the way, so only a job that was already
    // leaving is worth waiting for.
    let attempts = if requested.is_ok() {
        BOOTOUT_ATTEMPTS
    } else {
        BOOTOUT_GRACE_ATTEMPTS
    };
    for attempt in 0..attempts {
        if matches!(print_job(host, label, scope), JobReport::Absent) {
            return Ok(());
        }
        if attempt + 1 < attempts {
            host.sleep(BOOTOUT_POLL_INTERVAL);
        }
    }

    let detail = requested
        .err()
        .map_or_else(String::new, |err| format!(": {}", folded(&err.to_string())));
    Err(Error::CloudConnectIo {
        message: format!(
            "launchd is still holding the job {label} after being asked to unload it{detail}. \
             Unload it with `{}`.",
            launchctl_command(scope, &["bootout", &target])
        ),
    })
}

/// Put the job into its domain.
fn bootstrap(host: &dyn LaunchdHost, scope: ServiceScope, plist: &Path) -> Result<()> {
    let domain = domain_target(scope);
    let plist = plist.to_string_lossy().into_owned();
    let mut last = None;
    for attempt in 0..BOOTSTRAP_ATTEMPTS {
        match launchctl(host, scope, &["bootstrap", &domain, &plist]) {
            Ok(()) => return Ok(()),
            Err(err) => {
                last = Some(err);
                if attempt + 1 < BOOTSTRAP_ATTEMPTS {
                    host.sleep(BOOTSTRAP_RETRY_INTERVAL);
                }
            }
        }
    }

    let detail = last.map_or_else(String::new, |err| format!(": {}", folded(&err.to_string())));
    Err(Error::CloudConnectIo {
        message: format!(
            "launchd would not load the Spice Cloud Connect service from {plist}{detail}. It \
             refuses a definition it cannot parse, and a daemon that is not owned by root:wheel \
             and writable only by root — check it with `plutil -lint {plist}` and `ls -l {plist}`."
        ),
    })
}

/// Ask launchd to run the job now, optionally killing the run that is up.
fn kickstart(host: &dyn LaunchdHost, label: &str, scope: ServiceScope, kill: bool) -> Result<()> {
    let target = service_target(label, scope);
    let args: Vec<&str> = if kill {
        vec!["kickstart", "-k", &target]
    } else {
        vec!["kickstart", &target]
    };
    launchctl(host, scope, &args)
}

/// Stop, remove, and forget the job the manifest describes, together with the
/// runtime staged for it.
///
/// The definition is deleted even if the bootout fails, because leaving it on
/// disk would start the runtime again at the next boot against a released
/// identity. The bootout failure is still returned unless a subsequent query
/// proves the job is gone.
///
/// The log files are deliberately left behind: they are what an operator reads
/// to find out why the service they just removed was misbehaving, they are
/// bounded by the same policy that bounded them while it ran, and a reinstall
/// of the same directory continues them.
fn uninstall(host: &dyn LaunchdHost, manifest: &ServiceManifest) -> Result<()> {
    ensure_authorized(manifest, "uninstall")?;

    let stop_failure = bootout(host, &manifest.name, manifest.scope).err();

    match std::fs::remove_file(&manifest.definition_path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(Error::CloudConnectIo {
                message: format!(
                    "remove the launchd definition {}: {e}. The service would start again at boot \
                     against a released identity — delete the file and run `{}`.",
                    manifest.definition_path.display(),
                    launchctl_command(
                        manifest.scope,
                        &["bootout", &service_target(&manifest.name, manifest.scope)]
                    ),
                ),
            });
        }
    }

    let runtime_removal = remove_staged_runtime(manifest);
    if let Some(stop_failure) = stop_failure {
        let stopped = matches!(
            print_job(host, &manifest.name, manifest.scope),
            JobReport::Absent
        );
        if !stopped {
            let cleanup_detail = runtime_removal
                .as_ref()
                .err()
                .map_or_else(String::new, |error| {
                    format!(" Staged-runtime cleanup also failed: {error}.")
                });
            return Err(Error::CloudConnectIo {
                message: format!(
                    "{stop_failure}; removed the launchd definition {}, but could not prove the running process stopped. Stop pid(s) for {} before clearing its Cloud identity.{cleanup_detail}",
                    manifest.definition_path.display(),
                    manifest.name,
                ),
            });
        }
    }
    runtime_removal
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
///
/// A failure is reported rather than logged: the caller deletes the manifest
/// once this returns, and a manifest that is gone while the staged runtime is
/// still on disk leaves nobody who knows what the orphan belongs to.
fn remove_staged_runtime(manifest: &ServiceManifest) -> Result<()> {
    let Some(owned) = owned_runtime_dir(manifest) else {
        tracing::debug!(
            "leaving {} in place: it is not this service's own staged runtime",
            manifest.runtime_path.display()
        );
        return Ok(());
    };
    match std::fs::remove_dir_all(&owned) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(Error::CloudConnectIo {
            message: format!(
                "remove the runtime staged for the Spice Cloud Connect service {name} at {}: {e}. \
                 The service definition is already gone, so nothing runs it — remove the \
                 directory and re-run `spice cloud service uninstall`.",
                owned.display(),
                name = manifest.name,
            ),
        }),
    }
}

/// Start an installed, stopped service and confirm it came up.
///
/// A `stop` boots the job out of its domain and leaves the definition on disk,
/// so starting one is bootstrapping it again. A job launchd is still holding is
/// kickstarted instead, which is a no-op for one that is already running.
fn start(host: &dyn LaunchdHost, manifest: &ServiceManifest) -> Result<()> {
    ensure_authorized(manifest, "start")?;
    match print_job(host, &manifest.name, manifest.scope) {
        JobReport::Absent => launchd_action(
            manifest,
            "start",
            bootstrap(host, manifest.scope, &manifest.definition_path),
        )?,
        JobReport::Held(_) => launchd_action(
            manifest,
            "start",
            kickstart(host, &manifest.name, manifest.scope, false),
        )?,
        JobReport::Unanswerable(why) => return Err(unanswerable(manifest, "start", &why)),
    }
    await_state(host, manifest, "start", ServiceState::Running)
}

/// Take the job out of its domain, leaving the definition installed.
///
/// launchd has no "loaded but told to stay down": `KeepAlive` relaunches a job
/// that was killed, and a `launchctl disable` would survive the reboot this
/// service is meant to come back from. Booting it out is the one action that
/// stops the runtime now and still starts it at the next boot, which is exactly
/// what a stop means.
fn stop(host: &dyn LaunchdHost, manifest: &ServiceManifest) -> Result<()> {
    ensure_authorized(manifest, "stop")?;
    launchd_action(
        manifest,
        "stop",
        bootout(host, &manifest.name, manifest.scope),
    )?;
    await_state(host, manifest, "stop", ServiceState::Stopped)
}

/// Restart through launchd and wait for the service to come back.
///
/// Never asks `spiced` to exit itself: launchd owns the stop and the start, so
/// a restart that fails is launchd's failure to report rather than a runtime
/// that quietly went away.
fn restart(host: &dyn LaunchdHost, manifest: &ServiceManifest) -> Result<()> {
    ensure_authorized(manifest, "restart")?;
    match print_job(host, &manifest.name, manifest.scope) {
        JobReport::Absent => launchd_action(
            manifest,
            "restart",
            bootstrap(host, manifest.scope, &manifest.definition_path),
        )?,
        JobReport::Held(_) => launchd_action(
            manifest,
            "restart",
            kickstart(host, &manifest.name, manifest.scope, true),
        )?,
        JobReport::Unanswerable(why) => return Err(unanswerable(manifest, "restart", &why)),
    }
    await_state(host, manifest, "restart", ServiceState::Running)
}

/// Wait for the job to reach `wanted`, or report what it is doing instead.
fn await_state(
    host: &dyn LaunchdHost,
    manifest: &ServiceManifest,
    action: &str,
    wanted: ServiceState,
) -> Result<()> {
    let mut observed = ServiceState::Unavailable;
    for attempt in 0..LIFECYCLE_ATTEMPTS {
        if attempt > 0 {
            host.sleep(LIFECYCLE_POLL_INTERVAL);
        }
        observed = match print_job(host, &manifest.name, manifest.scope) {
            JobReport::Held(printed) => normalize_launchd_state(&printed),
            // The definition is still on disk — the manifest names it — so a
            // job launchd is not holding is an installed service that is down.
            JobReport::Absent => ServiceState::Stopped,
            JobReport::Unanswerable(_) => ServiceState::Unavailable,
        };
        if observed == wanted {
            return Ok(());
        }
        // A job that stopped after a failure is still down, which is what a
        // stop asked for.
        if wanted == ServiceState::Stopped && observed == ServiceState::Failed {
            return Ok(());
        }
    }

    Err(Error::CloudConnectIo {
        message: format!(
            "{action} the Spice Cloud Connect service {name} (launchd): launchd reports it as \
             {observed} rather than {wanted}. Inspect it with `{print}` and \
             `cd {dir} && spice cloud logs -n 200`.",
            name = manifest.name,
            print = launchctl_command(
                manifest.scope,
                &["print", &service_target(&manifest.name, manifest.scope)]
            ),
            dir = manifest.directory.display(),
        ),
    })
}

/// Observe the job: the state launchd reports plus whether it will come back on
/// its own.
fn observe(host: &dyn LaunchdHost, manifest: &ServiceManifest) -> ServiceObservation {
    let (state, diagnostic) = match print_job(host, &manifest.name, manifest.scope) {
        JobReport::Held(printed) => {
            let state = normalize_launchd_state(&printed);
            let diagnostic = (state == ServiceState::Unavailable)
                .then(|| unrecognized_state_diagnostic(&printed, manifest));
            (state, diagnostic)
        }
        JobReport::Absent => (ServiceState::Stopped, None),
        JobReport::Unanswerable(why) => {
            return ServiceObservation::unavailable(format!(
                "launchd could not be asked about {name}: {why}. Check that this account may \
                 query the {domain} domain (`{command}`).",
                name = manifest.name,
                domain = domain_target(manifest.scope),
                command = launchctl_command(
                    manifest.scope,
                    &["print", &service_target(&manifest.name, manifest.scope)]
                ),
            ));
        }
    };
    let (starts, starts_action) = observe_persistence(host, manifest);
    ServiceObservation {
        state,
        starts,
        diagnostic,
        starts_action,
    }
}

/// Why a state Spice does not recognise is reported as `unavailable`, naming
/// launchd's own answer and the command that produced it.
fn unrecognized_state_diagnostic(printed: &str, manifest: &ServiceManifest) -> String {
    format!(
        "`{command}` answered `{reported}`, which is not a state Spice can act on. Run it to see \
         what launchd reports for this job.",
        command = launchctl_command(
            manifest.scope,
            &["print", &service_target(&manifest.name, manifest.scope)]
        ),
        reported = top_level_field(printed, "state").unwrap_or_else(|| "no state".to_string()),
    )
}

/// Boot persistence as an operator outcome.
///
/// A `LaunchDaemon` starts at boot with nobody logged in. A `LaunchAgent`
/// starts with its owner's GUI login session, and there is no launchd setting
/// that changes that — so the remediation is to install a system service, not
/// to enable something on the agent. A label launchd's own database has
/// disabled starts under neither condition until it is enabled again.
fn observe_persistence(
    host: &dyn LaunchdHost,
    manifest: &ServiceManifest,
) -> (ServiceStarts, Option<String>) {
    if label_is_disabled(host, &manifest.name, manifest.scope) == Some(true) {
        return (
            ServiceStarts::Disabled,
            Some(launchctl_command(
                manifest.scope,
                &["enable", &service_target(&manifest.name, manifest.scope)],
            )),
        );
    }
    match manifest.scope {
        ServiceScope::System => (ServiceStarts::BootWithoutLogin, None),
        ServiceScope::User => (
            ServiceStarts::LoginOnly,
            Some(format!(
                "cd {dir} && spice cloud service uninstall && sudo spice cloud service install",
                dir = manifest.directory.display()
            )),
        ),
    }
}

/// Print the service's output from its own bounded files.
fn logs(manifest: &ServiceManifest, request: LogRequest) -> Result<Option<Vec<String>>> {
    let directory = log_dir_for_manifest(manifest)?;
    let reader = ServiceLogReader::new(&directory);
    let read_error = |what: &str, e: &std::io::Error| Error::CloudConnectIo {
        message: format!(
            "read the logs of the Spice Cloud Connect service {name} (launchd): {what} {dir}: {e}. \
             The runtime writes them there itself; check the directory exists and that this \
             account can read it.",
            name = manifest.name,
            dir = directory.display(),
        ),
    };

    let lines = usize::try_from(request.number).unwrap_or(usize::MAX);
    let (history, cursor) = reader
        .read_history(lines)
        .map_err(|e| read_error("open the service log directory", &e))?;

    if request.capture {
        return Ok(Some(history));
    }

    let mut out = std::io::stdout().lock();
    for line in &history {
        if writeln!(out, "{line}").is_err() {
            // The reader on the other end of the pipe went away, which is what
            // `| head` looks like. Nothing is wrong with the service.
            return Ok(None);
        }
    }

    if !request.follow {
        if history.is_empty() {
            // A service that has not written anything yet is a fact, not a
            // failure — the same answer the journal-backed back end gives.
            let _ = writeln!(out, "No logs yet for {}.", manifest.name);
        }
        return Ok(None);
    }

    // Follows until the command is interrupted, which is what `--follow` asks
    // for; a stdout that went away — `| head`, or a closed terminal — ends it
    // too, rather than spinning against a pipe nobody is reading.
    let streaming = std::cell::Cell::new(true);
    reader
        .follow(
            cursor,
            |line| {
                if writeln!(out, "{line}").is_err() {
                    streaming.set(false);
                }
            },
            || streaming.get(),
        )
        .map_err(|e| read_error("follow the service log in", &e))?;
    Ok(None)
}

/// The directory an installed service writes its log files into.
fn log_dir_for_manifest(manifest: &ServiceManifest) -> Result<PathBuf> {
    // The manifest's recorded source is authoritative — it is what was in force
    // when the service was installed — and the label is the fallback for a
    // manifest adopted from a definition alone.
    if let Some(LogSource::Files { stdout, .. }) = &manifest.log_source
        && let Some(parent) = stdout.parent()
    {
        return Ok(parent.to_path_buf());
    }
    log_dir_for_label(&manifest.name, manifest.scope).ok_or_else(|| Error::CloudConnectIo {
        message: format!(
            "locate the log files of the Spice Cloud Connect service {name} (launchd): its \
             manifest names no log source and its label carries no instance name. Re-run \
             `spice cloud service install` to rewrite both.",
            name = manifest.name,
        ),
    })
}

/// Refuse an action this invocation cannot perform, naming the exact retry.
///
/// Spice never elevates on its own, and never drives a user agent through
/// `sudo`: root's GUI domain is not the operator's, so a `sudo` command aimed at
/// `gui/<uid>` would report on a domain that is not the installed one.
fn ensure_authorized(manifest: &ServiceManifest, action: &str) -> Result<()> {
    let effective = nix::unistd::Uid::effective().as_raw();
    match manifest.scope {
        ServiceScope::System if effective != 0 => Err(Error::InvalidArgument {
            message: format!(
                "Failed to {action} the Spice Cloud Connect service {name} (launchd): a system \
                 service is managed with root privileges, and Spice never elevates on its own. \
                 Retry with `cd {dir} && sudo spice cloud service {action}`. Nothing was \
                 changed. See: https://spiceai.org/docs",
                name = manifest.name,
                dir = manifest.directory.display(),
            ),
        }),
        ServiceScope::User if effective != manifest.owner.uid => Err(Error::InvalidArgument {
            message: format!(
                "Failed to {action} the Spice Cloud Connect service {name} (launchd): a user \
                 agent is managed by the account that owns it ({owner}), and running this through \
                 sudo would target root's launchd domain instead of that account's. Run \
                 `cd {dir} && spice cloud service {action}` as {owner}. Nothing was changed. \
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
/// search: a definition left behind by a directory that has since moved carries
/// the same derived label, and taking it over would control an instance nobody
/// asked about.
///
/// The definition on disk is what is read, not what launchd is currently
/// serving. launchd has no drop-ins, so the file *is* the definition — and it
/// is the one that takes effect at the next load, which is what an install has
/// to reason about.
pub(super) fn find_for_dir(instance_dir: &Path, scope: ServiceScope) -> Option<InstalledService> {
    let label = job_label_for_dir(instance_dir);
    let path = definition_dir(scope)?.join(format!("{label}{PLIST_SUFFIX}"));
    let (bytes, _) = super::super::state::read_bounded_regular_file_with_metadata(
        &path,
        MAX_PLIST_DEFINITION_BYTES,
    )
    .ok()?;
    let plist = std::str::from_utf8(&bytes).ok()?;

    let working_dir = parse_working_dir(plist)?;
    if working_dir != instance_dir {
        return None;
    }
    Some(InstalledService {
        name: label,
        path,
        working_dir,
        config_dir: parse_config_dir(plist),
        runtime: parse_program(plist)?,
    })
}

/// Every managed service currently uses spiced's fixed HTTP and Flight listener
/// defaults. Until endpoint selections are part of the manifest and the
/// definition, installing a second one would either fail to bind or, worse, let
/// its health gate mistake the first runtime for the one it just started.
fn find_conflicting_definition(instance_dir: &Path) -> Result<Option<ServiceConflict>> {
    let own_label = job_label_for_dir(instance_dir);
    for directory in installation_definition_dirs(instance_dir) {
        let entries = match std::fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => continue,
            Err(source) => {
                return Err(Error::CloudConnectIo {
                    message: format!(
                        "inspect Spice Cloud Connect service definitions in {}: {source}",
                        directory.display()
                    ),
                });
            }
        };
        for entry in entries {
            let entry = entry.map_err(|source| Error::CloudConnectIo {
                message: format!(
                    "inspect a Spice Cloud Connect service definition in {}: {source}",
                    directory.display()
                ),
            })?;
            let file_name = entry.file_name().to_string_lossy().into_owned();
            let Some(label) = file_name.strip_suffix(PLIST_SUFFIX) else {
                continue;
            };
            if label == own_label || stem_of_label(label).is_none() {
                continue;
            }
            let path = entry.path();
            let (plist, _) = super::super::state::read_bounded_regular_file_with_metadata(
                &path,
                MAX_PLIST_DEFINITION_BYTES,
            )
            .map_err(|source| Error::CloudConnectIo {
                message: format!(
                    "read the existing Spice Cloud Connect service definition {} as a bounded, non-linked regular file: {source}",
                    path.display()
                ),
            })?;
            let plist = std::str::from_utf8(&plist).map_err(|source| Error::CloudConnectIo {
                message: format!(
                    "read the existing Spice Cloud Connect service definition {} as UTF-8: {source}",
                    path.display()
                ),
            })?;
            let working_dir = parse_working_dir(plist).ok_or_else(|| Error::CloudConnectIo {
                message: format!(
                    "inspect the existing Spice Cloud Connect service definition {}: it has no readable WorkingDirectory",
                    path.display()
                ),
            })?;
            return Ok(Some(ServiceConflict {
                name: label.to_string(),
                path,
                working_dir,
            }));
        }
    }
    Ok(None)
}

/// Definition directories visible to this installation, including the sudo
/// caller's own `LaunchAgents` for a system install. The same path can be
/// derived twice, so sort and deduplicate before scanning it.
fn installation_definition_dirs(instance_dir: &Path) -> Vec<PathBuf> {
    let mut directories = vec![PathBuf::from(LAUNCH_DAEMON_DIR)];
    if let Some(agents) = definition_dir(ServiceScope::User) {
        directories.push(agents);
    }
    if let Ok(account) = super::service_account(instance_dir, super::RootFallback::Allow)
        && let Ok(Some(user)) = nix::unistd::User::from_uid(nix::unistd::Uid::from_raw(account.uid))
    {
        directories.push(user.dir.join(LAUNCH_AGENT_SUBDIR));
    }
    directories.sort();
    directories.dedup();
    directories
}

fn recovery_hints(manifest: &ServiceManifest) -> Vec<String> {
    let mut hints = vec![launchctl_command(
        manifest.scope,
        &["print", &service_target(&manifest.name, manifest.scope)],
    )];
    if let Ok(directory) = log_dir_for_manifest(manifest) {
        hints.push(format!(
            "tail -f {}",
            service_log::live_path(&directory).display()
        ));
    }
    hints
}

/// The command line as an operator would type it, for messages that ask them to
/// run it themselves.
fn launchctl_command(scope: ServiceScope, args: &[&str]) -> String {
    let mut parts = Vec::with_capacity(args.len() + 2);
    if scope == ServiceScope::System && !super::is_root() {
        parts.push("sudo");
    }
    parts.push("launchctl");
    parts.extend(args);
    parts.join(" ")
}

/// Run `launchctl <args>`, turning a non-zero exit into an error carrying
/// launchd's own words — which name the actual problem far better than an exit
/// code.
fn launchctl(host: &dyn LaunchdHost, scope: ServiceScope, args: &[&str]) -> Result<()> {
    let output = host
        .output(LAUNCHCTL, args)
        .map_err(|e| Error::CloudConnectIo {
            message: format!("run `{}`: {e}", launchctl_command(scope, args)),
        })?;

    if output.success {
        return Ok(());
    }

    Err(Error::CloudConnectIo {
        message: format!(
            "`{}` failed: {}",
            launchctl_command(scope, args),
            output.describe_failure()
        ),
    })
}

/// Wrap a lifecycle failure with the action that produced it, adding the retry
/// an authorization refusal needs.
fn launchd_action(manifest: &ServiceManifest, action: &str, result: Result<()>) -> Result<()> {
    result.map_err(|err| {
        let reason = folded(&err.to_string());
        let denied = ["denied", "not permitted", "not authorized", "operation not"]
            .iter()
            .any(|word| reason.to_ascii_lowercase().contains(word));
        let retry = if denied && manifest.scope == ServiceScope::System && !super::is_root() {
            format!(
                " Retry with `cd {} && sudo spice cloud service {action}`.",
                manifest.directory.display()
            )
        } else {
            String::new()
        };
        Error::CloudConnectIo {
            message: format!(
                "{action} the Spice Cloud Connect service {name} (launchd): {reason}.{retry}",
                name = manifest.name,
            ),
        }
    })
}

/// The error a lifecycle action fails with when launchd could not be asked at
/// all — as opposed to answering that it has no such job.
fn unanswerable(manifest: &ServiceManifest, action: &str, why: &str) -> Error {
    Error::CloudConnectIo {
        message: format!(
            "{action} the Spice Cloud Connect service {name} (launchd): launchd could not be asked \
             about it: {why}. Nothing was changed. Check the {domain} domain with `{command}`.",
            name = manifest.name,
            domain = domain_target(manifest.scope),
            command = launchctl_command(
                manifest.scope,
                &["print", &service_target(&manifest.name, manifest.scope)]
            ),
        ),
    }
}

/// The macOS back end.
pub(super) struct LaunchdBackend;

impl ServiceBackend for LaunchdBackend {
    fn supervisor(&self) -> Supervisor {
        Supervisor::Launchd
    }

    fn preflight(&self, scope: ServiceScope) -> std::result::Result<(), PreflightFailure> {
        preflight(&ProcessHost, scope)
    }

    fn name_for_dir(&self, instance_dir: &Path) -> String {
        job_label_for_dir(instance_dir)
    }

    fn definition_path(&self, name: &str, scope: ServiceScope) -> PathBuf {
        definition_dir(scope)
            .unwrap_or_else(|| PathBuf::from(LAUNCH_DAEMON_DIR))
            .join(format!("{name}{PLIST_SUFFIX}"))
    }

    fn log_source(&self, name: &str, scope: ServiceScope) -> Option<LogSource> {
        let live = service_log::live_path(&log_dir_for_label(name, scope)?);
        // One file, not two: the runtime writes one stream into its own bounded
        // files rather than letting launchd split stdout and stderr into two
        // unbounded ones.
        Some(LogSource::Files {
            stdout: live.clone(),
            stderr: live,
        })
    }

    fn find_installed(&self, instance_dir: &Path, scope: ServiceScope) -> Option<InstalledService> {
        find_for_dir(instance_dir, scope)
    }

    fn find_conflicting_installation(
        &self,
        instance_dir: &Path,
    ) -> Result<Option<ServiceConflict>> {
        find_conflicting_definition(instance_dir)
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

    fn logs(&self, manifest: &ServiceManifest, request: LogRequest) -> Result<Option<Vec<String>>> {
        logs(manifest, request)
    }

    fn recovery_hints(&self, manifest: &ServiceManifest) -> Vec<String> {
        recovery_hints(manifest)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::{HashMap, VecDeque};

    use super::super::ServiceOwner;
    use super::super::manifest::MANIFEST_SCHEMA_VERSION;
    use super::*;

    const HEALTH_URL: &str = "http://127.0.0.1:8090/health";

    /// A host whose every answer is scripted, so the lifecycle can be exercised
    /// against states no test machine can be put into on demand — a job that
    /// never becomes healthy, a launchd that cannot be asked, a domain that
    /// refuses a bootstrap.
    struct ScriptedHost {
        answers: RefCell<HashMap<String, VecDeque<CommandOutput>>>,
        health: RefCell<VecDeque<HealthProbe>>,
        calls: RefCell<Vec<String>>,
        /// Whether a `bootout` actually takes the job out of the domain, the
        /// way launchd's does. A host that ignores it stands for the one case
        /// the lifecycle has to report rather than wait out forever.
        bootout_honoured: bool,
        /// Whether the job is currently out of its domain. A `bootout` sets it
        /// and a `bootstrap` clears it, so a test scripts what launchd says
        /// about a job it *has* and gets the absence for free.
        booted_out: RefCell<bool>,
        /// The scripted clock: it starts here and only what this host is asked
        /// to wait for moves it, so a gate bounded by a deadline runs in the
        /// time a test takes rather than in the time it measures.
        started: Instant,
        slept: RefCell<Duration>,
    }

    impl ScriptedHost {
        fn new() -> Self {
            Self {
                answers: RefCell::new(HashMap::new()),
                health: RefCell::new(VecDeque::new()),
                calls: RefCell::new(Vec::new()),
                bootout_honoured: true,
                booted_out: RefCell::new(false),
                started: Instant::now(),
                slept: RefCell::new(Duration::ZERO),
            }
        }

        /// A launchd that accepts a `bootout` and keeps the job anyway.
        fn bootout_ignored(mut self) -> Self {
            self.bootout_honoured = false;
            self
        }

        /// The answer launchd gives for a job it does not have.
        fn no_such_job() -> CommandOutput {
            CommandOutput {
                success: false,
                code: Some(113),
                stdout: String::new(),
                stderr: format!("{NO_SUCH_SERVICE} \"x\" in domain"),
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

        /// Answer `command` the way launchd answers for a job it does not have.
        fn has_no_job(self, command: &str) -> Self {
            self.answers.borrow_mut().insert(
                command.to_string(),
                VecDeque::from(vec![Self::no_such_job()]),
            );
            self
        }

        /// Answer `command` as a job that is absent for the first `absent`
        /// asks and then held with `then`.
        fn absent_then(self, command: &str, absent: usize, then: &str) -> Self {
            let mut answers: VecDeque<CommandOutput> =
                (0..absent).map(|_| Self::no_such_job()).collect();
            answers.push_back(CommandOutput {
                success: true,
                code: Some(0),
                stdout: then.to_string(),
                stderr: String::new(),
            });
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

        fn health(self, verdicts: &[HealthProbe]) -> Self {
            *self.health.borrow_mut() = verdicts.iter().copied().collect();
            self
        }

        fn calls(&self) -> Vec<String> {
            self.calls.borrow().clone()
        }

        fn ran(&self, command: &str) -> bool {
            self.calls.borrow().iter().any(|call| call == command)
        }
    }

    impl LaunchdHost for ScriptedHost {
        fn output(&self, program: &str, args: &[&str]) -> std::io::Result<CommandOutput> {
            let key = Self::key(program, args);
            self.calls.borrow_mut().push(key.clone());

            // launchd's own effect on what a later `print` answers, so a test
            // scripts only what the job says while it is loaded.
            match args.first().copied() {
                Some("bootout") if self.bootout_honoured => *self.booted_out.borrow_mut() = true,
                Some("bootstrap") => *self.booted_out.borrow_mut() = false,
                Some("print") if *self.booted_out.borrow() => return Ok(Self::no_such_job()),
                _ => {}
            }

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
            if queued.len() > 1 {
                Ok(queued.pop_front().unwrap_or_else(|| CommandOutput {
                    success: true,
                    code: Some(0),
                    stdout: String::new(),
                    stderr: String::new(),
                }))
            } else {
                Ok(queued.front().cloned().unwrap_or(CommandOutput {
                    success: true,
                    code: Some(0),
                    stdout: String::new(),
                    stderr: String::new(),
                }))
            }
        }

        fn probe_health(&self, _url: &str) -> HealthProbe {
            let mut health = self.health.borrow_mut();
            if health.len() > 1 {
                health.pop_front().unwrap_or(HealthProbe::Unreachable)
            } else {
                health.front().copied().unwrap_or(HealthProbe::Unreachable)
            }
        }

        fn now(&self) -> Instant {
            self.started + *self.slept.borrow()
        }

        fn sleep(&self, duration: Duration) {
            *self.slept.borrow_mut() += duration;
        }
    }

    /// A `launchctl print` report with the given top-level fields.
    fn report(fields: &[(&str, &str)]) -> String {
        use std::fmt::Write as _;

        let mut out = String::from("system/ai.spice.cloud-connect.x = {\n");
        for (key, value) in fields {
            // Writing into a String is infallible.
            let _ = writeln!(out, "\t{key} = {value}");
        }
        out.push_str("}\n");
        out
    }

    /// The `launchctl print` command this back end runs for a system job.
    fn print_command(label: &str) -> String {
        format!("{LAUNCHCTL} print system/{label}")
    }

    fn rendered(dir: &str) -> (String, String) {
        let label = job_label_for_dir(Path::new(dir));
        let instance = Path::new(dir);
        let plist = render_plist(
            &label,
            instance,
            &instance.join(".spice"),
            Path::new("/usr/local/lib/spice/edge/spiced"),
            Path::new("/Library/Logs/Spice/edge"),
            Some(("spice-operator".to_string(), "spice-users".to_string())),
        )
        .expect("render the definition");
        (label, plist)
    }

    fn manifest_for(dir: &Path, scope: ServiceScope) -> ServiceManifest {
        let label = job_label_for_dir(dir);
        ServiceManifest {
            schema_version: MANIFEST_SCHEMA_VERSION,
            directory: dir.to_path_buf(),
            name: label.clone(),
            scope,
            supervisor: Supervisor::Launchd,
            owner: ServiceOwner {
                uid: nix::unistd::Uid::effective().as_raw(),
                gid: nix::unistd::Gid::effective().as_raw(),
                name: Some("operator".to_string()),
            },
            definition_path: PathBuf::from(format!("/Library/LaunchDaemons/{label}.plist")),
            runtime_path: PathBuf::from("/usr/local/lib/spice/edge/spiced"),
            log_source: Some(LogSource::Files {
                stdout: PathBuf::from("/Library/Logs/Spice/edge/spiced.log"),
                stderr: PathBuf::from("/Library/Logs/Spice/edge/spiced.log"),
            }),
            runtime_digest: String::new(),
            runtime_version: "2.2.0".to_string(),
            health_url: HEALTH_URL.to_string(),
        }
    }

    #[test]
    fn a_label_is_reverse_dns_and_legible() {
        let label = job_label_for_dir(Path::new("/opt/edge-1"));
        assert!(
            label.starts_with("ai.spice.cloud-connect.edge-1-"),
            "{label}"
        );
        assert_eq!(
            stem_of_label(&label),
            Some(&label["ai.spice.cloud-connect.".len()..])
        );
        assert_eq!(stem_of_label("com.apple.something"), None);
        assert_eq!(stem_of_label("ai.spice.cloud-connect."), None);
    }

    #[test]
    fn two_directories_yield_two_independent_jobs() {
        // Two instance directories install two jobs, stored as two files, so
        // removing one cannot disturb the other.
        let a = job_label_for_dir(Path::new("/opt/edge-1"));
        let b = job_label_for_dir(Path::new("/opt/edge-2"));
        assert_ne!(a, b);
        assert_ne!(
            LaunchdBackend.definition_path(&a, ServiceScope::System),
            LaunchdBackend.definition_path(&b, ServiceScope::System)
        );
        assert_ne!(
            log_dir_for_label(&a, ServiceScope::System),
            log_dir_for_label(&b, ServiceScope::System)
        );
    }

    #[test]
    fn the_same_basename_in_different_parents_does_not_collide() {
        let a = job_label_for_dir(Path::new("/srv/a/edge"));
        let b = job_label_for_dir(Path::new("/srv/b/edge"));
        assert_ne!(a, b);
        assert!(a.starts_with("ai.spice.cloud-connect.edge-"));
        assert!(b.starts_with("ai.spice.cloud-connect.edge-"));
    }

    #[test]
    fn a_label_is_deterministic_for_the_same_directory() {
        // Re-running `install` must land on the same job rather than adding a
        // second one.
        let dir = Path::new("/opt/edge-1");
        assert_eq!(job_label_for_dir(dir), job_label_for_dir(dir));
    }

    #[test]
    fn a_daemon_lives_in_launch_daemons_and_an_agent_in_launch_agents() {
        let label = "ai.spice.cloud-connect.edge-1-1a2b3c4d";
        assert_eq!(
            LaunchdBackend.definition_path(label, ServiceScope::System),
            PathBuf::from(format!("/Library/LaunchDaemons/{label}.plist"))
        );
        let agent = LaunchdBackend.definition_path(label, ServiceScope::User);
        assert!(
            agent.ends_with(format!("Library/LaunchAgents/{label}.plist")),
            "{}",
            agent.display()
        );
        // The label carries dots of its own, so a definition's label has to be
        // recovered by dropping the suffix rather than by splitting on the last
        // dot.
        assert_eq!(
            agent
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| name.strip_suffix(PLIST_SUFFIX)),
            Some(label)
        );
    }

    #[test]
    fn a_daemon_and_an_agent_are_addressed_in_their_own_domains() {
        let label = "ai.spice.cloud-connect.x";
        assert_eq!(
            service_target(label, ServiceScope::System),
            format!("system/{label}")
        );
        assert_eq!(
            service_target(label, ServiceScope::User),
            format!("{}/{label}", gui_domain())
        );
        assert!(gui_domain().starts_with("gui/"));
    }

    #[test]
    fn a_rendered_definition_restarts_a_failure_and_leaves_a_clean_exit_alone() {
        let (label, plist) = rendered("/opt/edge-1");
        assert!(plist.contains(&format!("<string>{label}</string>")));
        assert!(plist.contains("<key>RunAtLoad</key>\n\t<true/>"), "{plist}");
        // The whole point: an unconditional KeepAlive would relaunch the clean
        // exit an operator's stop produces.
        assert!(
            plist.contains(
                "<key>KeepAlive</key>\n\t<dict>\n\t\t<key>SuccessfulExit</key>\n\t\t<false/>\n\t</dict>"
            ),
            "{plist}"
        );
        // Background would have macOS throttle the runtime's I/O.
        assert!(plist.contains("<key>ProcessType</key>\n\t<string>Standard</string>"));
        assert!(plist.contains(&format!(
            "<key>ExitTimeOut</key>\n\t<integer>{EXIT_TIMEOUT_SECONDS}</integer>"
        )));
    }

    #[test]
    fn a_rendered_definition_names_no_unbounded_launchd_log_file() {
        // launchd bounds neither, so naming one is how a daemon fills a disk.
        // The runtime is pointed at its own bounded files instead.
        let (_, plist) = rendered("/opt/edge-1");
        assert!(!plist.contains("<key>StandardOutPath</key>"), "{plist}");
        assert!(!plist.contains("<key>StandardErrorPath</key>"), "{plist}");
        assert!(
            plist.contains(
                "<string>--service-log-dir</string>\n\t\t<string>/Library/Logs/Spice/edge</string>"
            ),
            "{plist}"
        );
    }

    #[test]
    fn a_rendered_definition_bakes_the_absolute_working_directory_and_identity() {
        let (_, plist) = rendered("/opt/edge-1");
        assert_eq!(
            parse_working_dir(&plist),
            Some(PathBuf::from("/opt/edge-1")),
            "the working directory must round-trip so status can discover it"
        );
        assert_eq!(
            parse_config_dir(&plist),
            Some(PathBuf::from("/opt/edge-1/.spice"))
        );
        // The program is the staged copy, never the operator's replaceable
        // runtime, and the arguments are dropped.
        assert_eq!(
            parse_program(&plist),
            Some(PathBuf::from("/usr/local/lib/spice/edge/spiced"))
        );
    }

    #[test]
    fn only_a_daemon_names_the_account_it_runs_as() {
        let (_, daemon) = rendered("/opt/edge-1");
        assert!(daemon.contains("<key>UserName</key>\n\t<string>spice-operator</string>"));
        assert!(daemon.contains("<key>GroupName</key>\n\t<string>spice-users</string>"));

        // A LaunchAgent already runs as its owner, and launchd ignores
        // `UserName` in one.
        let agent = render_plist(
            "ai.spice.cloud-connect.x",
            Path::new("/opt/edge-1"),
            Path::new("/opt/edge-1/.spice"),
            Path::new("/opt/spiced"),
            Path::new("/opt/logs"),
            None,
        )
        .expect("render an agent");
        assert!(!agent.contains("UserName"), "{agent}");
        assert!(!agent.contains("GroupName"), "{agent}");
    }

    #[test]
    fn a_directory_that_would_break_the_xml_is_escaped_and_reads_back() {
        let dir = Path::new("/opt/a&b<c>");
        let plist = render_plist(
            "ai.spice.cloud-connect.x",
            dir,
            &dir.join(".spice"),
            Path::new("/usr/bin/spiced"),
            Path::new("/var/log/x"),
            None,
        )
        .expect("render");
        assert!(
            plist.contains("<string>/opt/a&amp;b&lt;c&gt;</string>"),
            "{plist}"
        );
        assert_eq!(parse_working_dir(&plist), Some(dir.to_path_buf()));
    }

    #[test]
    fn a_path_that_cannot_be_represented_is_refused_rather_than_written() {
        // A plist carrying a raw control character is one `plutil` rejects and
        // launchd silently declines to load, so it never reaches disk.
        let refused = escape_plist_path(Path::new("/opt/edge\u{1}1"));
        assert!(refused.is_err(), "{refused:?}");
        assert!(
            escape_plist_path(Path::new("/opt/edge-1")).is_ok(),
            "an ordinary path must render"
        );
    }

    #[test]
    fn xml_round_trips_the_characters_that_would_break_a_definition() {
        for value in ["/opt/a&b", "/opt/<x>", "/opt/plain", "/opt/a&amp;b"] {
            assert_eq!(
                unescape_plist_text(&escape_plist_text(value).expect("escape")),
                value,
                "{value}"
            );
        }
    }

    #[test]
    fn parsing_ignores_definitions_without_the_key() {
        assert_eq!(parse_working_dir("<dict></dict>"), None);
        assert_eq!(
            parse_working_dir("<key>WorkingDirectory</key>\n<string></string>"),
            None
        );
        assert_eq!(
            parse_program("<key>WorkingDirectory</key><string>/opt/x</string>"),
            None
        );
        assert_eq!(parse_config_dir("<dict></dict>"), None);
    }

    #[test]
    fn a_top_level_field_is_read_and_not_an_endpoints_field() {
        // `launchctl print` repeats field names for a job's endpoints at a
        // deeper indent; reading one of those as the job's state would report a
        // dead instance as active.
        let printed = "system/ai.spice.x = {\n\tendpoints = {\n\t\tstate = active\n\t}\n\
                       \tstate = not running\n\tlast exit code = 78\n}\n";
        assert_eq!(
            top_level_field(printed, "state"),
            Some("not running".to_string())
        );
        assert_eq!(last_exit_code(printed), Some(78));
        assert_eq!(top_level_field(printed, "pid"), None);
    }

    #[test]
    fn launchd_words_become_the_normalized_vocabulary() {
        assert_eq!(
            normalize_launchd_state(&report(&[("state", "running"), ("pid", "42")])),
            ServiceState::Running
        );
        for word in ["waiting", "spawn scheduled", "spawning"] {
            assert_eq!(
                normalize_launchd_state(&report(&[("state", word)])),
                ServiceState::Starting,
                "{word}"
            );
        }
        // launchd has no `failed` state of its own, so the exit code is what
        // tells a service an operator stopped from one that fell over.
        assert_eq!(
            normalize_launchd_state(&report(&[
                ("state", "not running"),
                ("last exit code", "0")
            ])),
            ServiceState::Stopped
        );
        assert_eq!(
            normalize_launchd_state(&report(&[
                ("state", "not running"),
                ("last exit code", "(never exited)")
            ])),
            ServiceState::Stopped
        );
        assert_eq!(
            normalize_launchd_state(&report(&[
                ("state", "not running"),
                ("last exit code", "78")
            ])),
            ServiceState::Failed
        );
        // A report with no state at all is a reading Spice cannot act on, not a
        // second name for "stopped".
        assert_eq!(
            normalize_launchd_state(&report(&[("pid", "42")])),
            ServiceState::Unavailable
        );
        assert_eq!(
            normalize_launchd_state(&report(&[("state", "a word from a later macOS")])),
            ServiceState::Unavailable
        );
    }

    #[test]
    fn a_run_is_identified_by_the_start_count_and_the_process() {
        let first = report(&[("state", "running"), ("runs", "1"), ("pid", "100")]);
        let relaunched = report(&[("state", "running"), ("runs", "2"), ("pid", "101")]);
        assert_ne!(run_identity(&first), run_identity(&relaunched));
        assert_eq!(run_identity(&first), run_identity(&first.clone()));
        // A pid can be reused; the run counter is what makes the pair unique.
        let reused = report(&[("state", "running"), ("runs", "2"), ("pid", "100")]);
        assert_ne!(run_identity(&first), run_identity(&reused));
        assert_eq!(run_identity(&report(&[("state", "running")])), None);
    }

    #[test]
    fn a_job_launchd_could_not_be_asked_about_is_not_a_job_it_does_not_have() {
        // Reading "I could not answer" as "there is no such job" is exactly how
        // a daemon survives its own removal.
        let absent = ScriptedHost::new().has_no_job(&print_command("x"));
        assert_eq!(
            print_job(&absent, "x", ServiceScope::System),
            JobReport::Absent
        );

        let refused = ScriptedHost::new().fails(&print_command("x"), "Operation not permitted");
        assert!(matches!(
            print_job(&refused, "x", ServiceScope::System),
            JobReport::Unanswerable(_)
        ));
    }

    #[test]
    fn a_disabled_label_is_read_from_either_spelling() {
        let printed = "\tdisabled services = {\n\t\t\"ai.spice.cloud-connect.a\" => disabled\n\
                       \t\t\"ai.spice.cloud-connect.b\" => enabled\n\t\t\"legacy\" => true\n\
                       \t\t\"legacy-off\" => false\n\t}\n";
        assert_eq!(
            parse_disabled(printed, "ai.spice.cloud-connect.a"),
            Some(true)
        );
        assert_eq!(
            parse_disabled(printed, "ai.spice.cloud-connect.b"),
            Some(false)
        );
        assert_eq!(parse_disabled(printed, "legacy"), Some(true));
        assert_eq!(parse_disabled(printed, "legacy-off"), Some(false));
        // A label launchd does not list is not disabled, and must not be
        // reported as a persistence problem.
        assert_eq!(parse_disabled(printed, "not-listed"), None);
    }

    #[test]
    fn a_daemon_starts_at_boot_and_an_agent_only_at_login() {
        let dir = Path::new("/opt/edge-1");
        let host = ScriptedHost::new();

        let daemon = manifest_for(dir, ServiceScope::System);
        let (starts, action) = observe_persistence(&host, &daemon);
        assert_eq!(starts, ServiceStarts::BootWithoutLogin);
        assert_eq!(action, None);

        // A LaunchAgent cannot be made to start before its owner logs in, so
        // the remediation is a system service — never something to enable on
        // the agent, and never the word "lingering".
        let agent = manifest_for(dir, ServiceScope::User);
        let (starts, action) = observe_persistence(&host, &agent);
        assert_eq!(starts, ServiceStarts::LoginOnly);
        let action = action.expect("an agent has a remediation");
        assert!(
            action.contains("sudo spice cloud service install"),
            "{action}"
        );
        assert!(!action.contains("linger"), "{action}");
    }

    #[test]
    fn a_label_launchd_disabled_reports_disabled_with_the_command_that_fixes_it() {
        let dir = Path::new("/opt/edge-1");
        let manifest = manifest_for(dir, ServiceScope::System);
        let host = ScriptedHost::new().says(
            &format!("{LAUNCHCTL} print-disabled system"),
            &format!(
                "\tdisabled services = {{\n\t\t\"{}\" => disabled\n\t}}\n",
                manifest.name
            ),
        );
        let (starts, action) = observe_persistence(&host, &manifest);
        assert_eq!(starts, ServiceStarts::Disabled);
        assert_eq!(
            action,
            Some(format!("sudo launchctl enable system/{}", manifest.name))
        );
    }

    #[test]
    fn a_job_launchd_is_not_holding_is_installed_and_stopped() {
        // `stop` boots the job out and leaves the definition on disk, so this
        // is the state a stopped service is actually in.
        let dir = Path::new("/opt/edge-1");
        let manifest = manifest_for(dir, ServiceScope::System);
        let host = ScriptedHost::new().has_no_job(&print_command(&manifest.name));
        let observed = observe(&host, &manifest);
        assert_eq!(observed.state, ServiceState::Stopped);
        assert_eq!(observed.starts, ServiceStarts::BootWithoutLogin);
        assert_eq!(observed.diagnostic, None);
    }

    #[test]
    fn a_launchd_that_cannot_be_asked_is_unavailable_with_the_command_that_asks() {
        let dir = Path::new("/opt/edge-1");
        let manifest = manifest_for(dir, ServiceScope::System);
        let host = ScriptedHost::new().fails(
            &print_command(&manifest.name),
            "Could not find domain for system",
        );
        let observed = observe(&host, &manifest);
        assert_eq!(observed.state, ServiceState::Unavailable);
        let diagnostic = observed.diagnostic.expect("an unavailable state says why");
        assert!(diagnostic.contains("launchctl print"), "{diagnostic}");
    }

    #[test]
    fn a_state_this_release_does_not_recognise_says_what_launchd_answered() {
        let dir = Path::new("/opt/edge-1");
        let manifest = manifest_for(dir, ServiceScope::System);
        let host = ScriptedHost::new().says(
            &print_command(&manifest.name),
            &report(&[("state", "a word from a later macOS")]),
        );
        let observed = observe(&host, &manifest);
        assert_eq!(observed.state, ServiceState::Unavailable);
        let diagnostic = observed.diagnostic.expect("an unavailable state says why");
        assert!(
            diagnostic.contains("a word from a later macOS"),
            "{diagnostic}"
        );
    }

    #[test]
    fn a_healthy_run_passes_the_gate_only_after_it_has_held() {
        let label = "ai.spice.cloud-connect.x";
        let host = ScriptedHost::new()
            .says(
                &print_command(label),
                &report(&[("state", "running"), ("runs", "1"), ("pid", "100")]),
            )
            .health(&[HealthProbe::Healthy]);
        health_gate(&host, label, ServiceScope::System, HEALTH_URL)
            .expect("a run that holds and answers must pass");
        assert!(
            *host.slept.borrow() >= SETTLE_WINDOW,
            "the gate must require a whole settle window, slept {:?}",
            host.slept.borrow()
        );
    }

    #[test]
    fn a_runtime_that_is_relaunched_never_finishes_the_settle_window() {
        // The failure this gate exists for: launchd is back to `running` by the
        // next poll, so only the run counter reveals the relaunch.
        let label = "ai.spice.cloud-connect.x";
        let restarting: Vec<String> = (0..200)
            .map(|i| {
                report(&[
                    ("state", "running"),
                    ("runs", &i.to_string()),
                    ("pid", "100"),
                ])
            })
            .collect();
        let restarting: Vec<&str> = restarting.iter().map(String::as_str).collect();
        let host = ScriptedHost::new()
            .sequence(&print_command(label), &restarting)
            .health(&[HealthProbe::Healthy]);
        let why = health_gate(&host, label, ServiceScope::System, HEALTH_URL)
            .expect_err("a runtime that keeps being relaunched is not healthy");
        assert!(why.contains("has not stayed in the same run"), "{why}");
    }

    #[test]
    fn an_explicitly_unhealthy_answer_fails_the_gate_even_while_the_run_holds() {
        let label = "ai.spice.cloud-connect.x";
        let host = ScriptedHost::new()
            .says(
                &print_command(label),
                &report(&[("state", "running"), ("runs", "1"), ("pid", "100")]),
            )
            .health(&[HealthProbe::Unhealthy]);
        let why = health_gate(&host, label, ServiceScope::System, HEALTH_URL)
            .expect_err("an instance that reports itself unhealthy must not pass");
        assert!(why.contains("unhealthy"), "{why}");
    }

    #[test]
    fn a_job_that_keeps_exiting_badly_ends_the_gate_early_but_not_on_one_reading() {
        let label = "ai.spice.cloud-connect.x";
        let host = ScriptedHost::new().says(
            &print_command(label),
            &report(&[("state", "not running"), ("last exit code", "1")]),
        );
        let why = health_gate(&host, label, ServiceScope::System, HEALTH_URL)
            .expect_err("a job that exits badly must not pass");
        assert!(why.contains("exiting with 1"), "{why}");
        // Given up on early rather than after the whole gate, but not on the
        // first reading — a job launchd is relaunching passes through several
        // states on the way.
        assert!(*host.slept.borrow() < HEALTH_GATE, "{:?}", host.slept);
        assert!(*host.slept.borrow() > Duration::ZERO, "{:?}", host.slept);
    }

    #[test]
    fn a_job_launchd_never_managed_to_spawn_says_it_never_ran() {
        // `waiting` with `runs = 0` is a job launchd accepted and could not
        // start — a program it cannot execute, or a working directory the
        // account it runs as cannot reach. The state alone reads the same as a
        // job between two relaunches, so the counters are what tell them apart.
        let label = "ai.spice.cloud-connect.x";
        let host = ScriptedHost::new().says(
            &print_command(label),
            &report(&[("state", "waiting"), ("runs", "0")]),
        );
        let why = health_gate(&host, label, ServiceScope::System, HEALTH_URL)
            .expect_err("a job that never ran is not healthy");
        assert!(why.contains("runs = 0"), "{why}");
        assert!(why.contains("starting rather than running"), "{why}");
    }

    #[test]
    fn a_job_launchd_never_took_fails_the_gate_at_the_deadline() {
        let label = "ai.spice.cloud-connect.x";
        let host = ScriptedHost::new().has_no_job(&print_command(label));
        let why = health_gate(&host, label, ServiceScope::System, HEALTH_URL)
            .expect_err("a job that was never loaded is not healthy");
        assert!(why.contains("not holding a job"), "{why}");
        assert!(*host.slept.borrow() >= HEALTH_GATE, "{:?}", host.slept);
    }

    #[test]
    fn an_unprobeable_health_url_gates_on_what_launchd_reports() {
        // A health URL this back end cannot reach must not fail an install that
        // is fine.
        let label = "ai.spice.cloud-connect.x";
        let host = ScriptedHost::new()
            .says(
                &print_command(label),
                &report(&[("state", "running"), ("runs", "1"), ("pid", "100")]),
            )
            .health(&[HealthProbe::Unreachable]);
        health_gate(
            &host,
            label,
            ServiceScope::System,
            "https://127.0.0.1:8090/health",
        )
        .expect("launchd's own report is the answer here");
    }

    #[test]
    fn a_bootout_that_launchd_accepts_and_ignores_is_reported_rather_than_assumed() {
        // `bootout` returns as soon as launchd has been told, not once the
        // runtime has stopped — so a job that is still there afterwards has to
        // be reported, never taken for gone.
        let label = "ai.spice.cloud-connect.x";
        let host = ScriptedHost::new().bootout_ignored().says(
            &print_command(label),
            &report(&[("state", "running"), ("pid", "100")]),
        );
        let failed = bootout(&host, label, ServiceScope::System)
            .expect_err("a job that never leaves must be reported");
        assert!(failed.to_string().contains("still holding"), "{failed}");
        assert!(host.ran(&format!("{LAUNCHCTL} bootout system/{label}")));
    }

    #[test]
    fn a_bootout_that_is_honoured_leaves_the_job_out_of_the_domain() {
        let label = "ai.spice.cloud-connect.x";
        let host = ScriptedHost::new().says(
            &print_command(label),
            &report(&[("state", "running"), ("pid", "100")]),
        );
        bootout(&host, label, ServiceScope::System).expect("bootout");
        assert_eq!(
            print_job(&host, label, ServiceScope::System),
            JobReport::Absent
        );
    }

    #[test]
    fn a_bootout_of_a_job_launchd_does_not_have_changes_nothing() {
        let label = "ai.spice.cloud-connect.x";
        let host = ScriptedHost::new().has_no_job(&print_command(label));
        bootout(&host, label, ServiceScope::System).expect("idempotent");
        assert!(
            !host.ran(&format!("{LAUNCHCTL} bootout system/{label}")),
            "{:?}",
            host.calls()
        );
    }

    #[test]
    fn starting_a_stopped_service_bootstraps_it_rather_than_kickstarting_nothing() {
        let dir = Path::new("/opt/edge-1");
        let manifest = manifest_for(dir, ServiceScope::System);
        let running = report(&[("state", "running"), ("runs", "1"), ("pid", "100")]);
        let host = ScriptedHost::new().absent_then(&print_command(&manifest.name), 1, &running);
        // Not root in a test run, so authorization refuses a system service
        // before anything is attempted — which is itself the contract.
        let refused = start(&host, &manifest).expect_err("a system service needs root");
        assert!(refused.to_string().contains("sudo"), "{refused}");
    }

    #[test]
    fn a_user_service_is_started_by_bootstrapping_its_definition() {
        let dir = Path::new("/opt/edge-1");
        let manifest = manifest_for(dir, ServiceScope::User);
        let target = service_target(&manifest.name, ServiceScope::User);
        let running = report(&[("state", "running"), ("runs", "1"), ("pid", "100")]);
        let host =
            ScriptedHost::new().absent_then(&format!("{LAUNCHCTL} print {target}"), 1, &running);
        start(&host, &manifest).expect("a booted-out agent is started by bootstrapping it");
        assert!(
            host.ran(&format!(
                "{LAUNCHCTL} bootstrap {} {}",
                gui_domain(),
                manifest.definition_path.display()
            )),
            "{:?}",
            host.calls()
        );
    }

    #[test]
    fn a_running_user_service_is_started_idempotently() {
        let dir = Path::new("/opt/edge-1");
        let manifest = manifest_for(dir, ServiceScope::User);
        let target = service_target(&manifest.name, ServiceScope::User);
        let host = ScriptedHost::new().says(
            &format!("{LAUNCHCTL} print {target}"),
            &report(&[("state", "running"), ("runs", "1"), ("pid", "100")]),
        );
        start(&host, &manifest).expect("already running is the state a start asks for");
        assert!(host.ran(&format!("{LAUNCHCTL} kickstart {target}")));
        assert!(
            !host.ran(&format!("{LAUNCHCTL} kickstart -k {target}")),
            "a start must not kill the run that is already serving"
        );
    }

    #[test]
    fn a_restart_kills_the_run_that_is_up_rather_than_asking_spiced_to_exit() {
        let dir = Path::new("/opt/edge-1");
        let manifest = manifest_for(dir, ServiceScope::User);
        let target = service_target(&manifest.name, ServiceScope::User);
        let host = ScriptedHost::new().says(
            &format!("{LAUNCHCTL} print {target}"),
            &report(&[("state", "running"), ("runs", "2"), ("pid", "101")]),
        );
        restart(&host, &manifest).expect("restart");
        assert!(host.ran(&format!("{LAUNCHCTL} kickstart -k {target}")));
    }

    #[test]
    fn a_stop_boots_the_job_out_and_leaves_the_definition_installed() {
        let dir = Path::new("/opt/edge-1");
        let manifest = manifest_for(dir, ServiceScope::User);
        let target = service_target(&manifest.name, ServiceScope::User);
        let host = ScriptedHost::new().says(
            &format!("{LAUNCHCTL} print {target}"),
            &report(&[("state", "running"), ("runs", "1"), ("pid", "100")]),
        );

        stop(&host, &manifest).expect("stop");
        assert!(host.ran(&format!("{LAUNCHCTL} bootout {target}")));
        // Never `disable`: that survives a reboot, and a stopped service still
        // has to come back at the next one.
        assert!(
            !host.calls().iter().any(|call| call.contains("disable")),
            "{:?}",
            host.calls()
        );
    }

    #[test]
    fn a_stop_of_a_service_that_is_already_down_succeeds() {
        let dir = Path::new("/opt/edge-1");
        let manifest = manifest_for(dir, ServiceScope::User);
        let target = service_target(&manifest.name, ServiceScope::User);
        let host = ScriptedHost::new().has_no_job(&format!("{LAUNCHCTL} print {target}"));
        stop(&host, &manifest).expect("idempotent");
    }

    #[test]
    fn a_lifecycle_action_that_does_not_reach_its_state_says_what_launchd_reports() {
        let dir = Path::new("/opt/edge-1");
        let manifest = manifest_for(dir, ServiceScope::User);
        let target = service_target(&manifest.name, ServiceScope::User);
        let host = ScriptedHost::new().says(
            &format!("{LAUNCHCTL} print {target}"),
            &report(&[("state", "not running"), ("last exit code", "7")]),
        );
        let failed = restart(&host, &manifest).expect_err("a job that never comes back must fail");
        let message = failed.to_string();
        assert!(message.contains("failed rather than running"), "{message}");
        assert!(message.contains("spice cloud logs"), "{message}");
    }

    #[test]
    fn a_system_service_is_never_driven_without_root_and_a_user_one_never_through_sudo() {
        let dir = Path::new("/opt/edge-1");
        let system = manifest_for(dir, ServiceScope::System);
        // Tests do not run as root, so this is the real refusal.
        let refused = ensure_authorized(&system, "restart").expect_err("root is required");
        let message = refused.to_string();
        assert!(
            message.contains("sudo spice cloud service restart"),
            "{message}"
        );
        assert!(message.contains("Nothing was changed"), "{message}");

        let mut other_account = manifest_for(dir, ServiceScope::User);
        other_account.owner.uid = nix::unistd::Uid::effective().as_raw() + 1;
        let refused = ensure_authorized(&other_account, "stop")
            .expect_err("another account's agent is not ours to drive");
        let message = refused.to_string();
        assert!(message.contains("root's launchd domain"), "{message}");

        // The account's own agent is exactly what it may drive.
        ensure_authorized(&manifest_for(dir, ServiceScope::User), "stop")
            .expect("an account manages its own agent");
    }

    #[test]
    fn the_log_source_is_the_one_bounded_file_the_runtime_writes() {
        let label = job_label_for_dir(Path::new("/opt/edge-1"));
        let source = LaunchdBackend
            .log_source(&label, ServiceScope::System)
            .expect("a launchd service always has a log source");
        let LogSource::Files { stdout, stderr } = source else {
            panic!("launchd keeps its output in files, not in a journal");
        };
        // One stream, not launchd's two unbounded ones.
        assert_eq!(stdout, stderr);
        assert!(
            stdout.starts_with("/Library/Logs/Spice/"),
            "{}",
            stdout.display()
        );
        assert_eq!(
            stdout.file_name().and_then(|name| name.to_str()),
            Some(service_log::LIVE_FILE_NAME)
        );

        let user = LaunchdBackend
            .log_source(&label, ServiceScope::User)
            .expect("an agent writes logs too");
        assert_ne!(
            LaunchdBackend.log_source(&label, ServiceScope::System),
            Some(user),
            "a daemon and an agent must never share a log directory"
        );
    }

    #[test]
    fn two_instances_never_share_a_log_directory() {
        let one = job_label_for_dir(Path::new("/opt/edge-1"));
        let two = job_label_for_dir(Path::new("/opt/edge-2"));
        let one = log_dir_for_label(&one, ServiceScope::System).expect("a log directory");
        let two = log_dir_for_label(&two, ServiceScope::System).expect("a log directory");
        assert_ne!(one, two);
        assert!(!one.starts_with(&two) && !two.starts_with(&one));
    }

    #[test]
    fn a_definition_that_names_another_directory_is_not_this_directory_s_service() {
        let root = tempfile::tempdir().expect("create tempdir");
        let instance = root.path().join("edge-1");
        std::fs::create_dir_all(&instance).expect("create the instance directory");
        let label = job_label_for_dir(&instance);
        let definitions = root.path().join("LaunchAgents");
        std::fs::create_dir_all(&definitions).expect("create the definition directory");
        let path = definitions.join(format!("{label}{PLIST_SUFFIX}"));

        // A definition left behind by a directory that has since moved carries
        // the same derived label; taking it over would control an instance
        // nobody asked about.
        let foreign = render_plist(
            &label,
            Path::new("/somewhere/else"),
            Path::new("/somewhere/else/.spice"),
            Path::new("/usr/local/lib/spice/x/spiced"),
            Path::new("/var/log/x"),
            None,
        )
        .expect("render");
        std::fs::write(&path, foreign).expect("plant a definition");
        let plist = std::fs::read_to_string(&path).expect("read it back");
        assert_ne!(
            parse_working_dir(&plist).as_deref(),
            Some(instance.as_path())
        );

        let own = render_plist(
            &label,
            &instance,
            &instance.join(".spice"),
            Path::new("/usr/local/lib/spice/x/spiced"),
            Path::new("/var/log/x"),
            None,
        )
        .expect("render");
        std::fs::write(&path, own).expect("write our own definition");
        let plist = std::fs::read_to_string(&path).expect("read it back");
        assert_eq!(
            parse_working_dir(&plist).as_deref(),
            Some(instance.as_path())
        );
        assert_eq!(
            parse_config_dir(&plist),
            Some(instance.join(".spice")),
            "the identity binding must round-trip so an adoption can be verified"
        );
    }

    #[test]
    fn a_definition_is_written_atomically_and_never_as_a_file_launchd_would_load_early() {
        use std::os::unix::fs::PermissionsExt as _;

        let dir = tempfile::tempdir().expect("create tempdir");
        let path = owner_only_dir(dir.path()).join("ai.spice.cloud-connect.x.plist");
        write_plist(&path, b"<plist/>\n", ServiceScope::User).expect("write");
        assert_eq!(
            std::fs::read(&path).expect("read the definition"),
            b"<plist/>\n"
        );
        // The staging sibling is a dotfile, which launchd skips when it walks
        // the directory at boot, and it is gone once the rename succeeded.
        let leftovers: Vec<String> = std::fs::read_dir(dir.path())
            .expect("read the directory")
            .filter_map(std::result::Result::ok)
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with('.'))
            .collect();
        assert!(leftovers.is_empty(), "{leftovers:?}");

        let mode = std::fs::metadata(&path).expect("stat").permissions().mode() & 0o7777;
        assert_eq!(
            mode, PLIST_MODE,
            "launchd refuses a group-writable definition"
        );
    }

    /// Create a directory the service layer will accept, whatever the developer's
    /// umask.
    ///
    /// `tempfile` and `create_dir_all` both derive their mode from the process
    /// umask, so on a host that defaults to `umask 002` they hand the installer a
    /// group-writable directory — which `ensure_account_only_dir` is right to
    /// refuse, because another account in the group could replace the binary the
    /// service executes. Production never hits that: it creates the directory
    /// itself with an explicit mode. Only a test that pre-creates one does, and
    /// then it fails on the reviewer's machine and not the author's.
    fn owner_only_dir(path: &Path) -> &Path {
        use std::os::unix::fs::PermissionsExt as _;

        std::fs::create_dir_all(path).expect("create the directory");
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .expect("restrict the directory to its owner");
        path
    }

    /// A `spiced` stand-in that runs, so the install path's execution check
    /// passes without a real runtime.
    fn fake_runtime(path: &Path) {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::write(path, "#!/bin/sh\nexit 0\n").expect("write a fake runtime");
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755))
            .expect("make it executable");
    }

    fn install_paths_under(root: &Path) -> InstallPaths {
        InstallPaths {
            plist: root.join("LaunchAgents/ai.spice.cloud-connect.x.plist"),
            runtime_dir: root.join("runtime"),
            runtime: root.join("runtime/spiced"),
            log_dir: root.join("logs"),
        }
    }

    #[test]
    fn an_install_that_comes_up_healthy_keeps_the_new_runtime() {
        let root = tempfile::tempdir().expect("create tempdir");
        let instance = root.path().join("edge-1");
        std::fs::create_dir_all(instance.join(".spice")).expect("create the instance");
        let source = root.path().join("spiced");
        fake_runtime(&source);
        let paths = install_paths_under(root.path());
        let label = "ai.spice.cloud-connect.x";
        let target = service_target(label, ServiceScope::User);

        let host = ScriptedHost::new()
            .absent_then(
                &format!("{LAUNCHCTL} print {target}"),
                1,
                &report(&[("state", "running"), ("runs", "1"), ("pid", "100")]),
            )
            .health(&[HealthProbe::Healthy]);
        let request = InstallRequest {
            instance_dir: &instance,
            config_dir: &instance.join(".spice"),
            spiced_path: &source,
            scope: ServiceScope::User,
            health_url: HEALTH_URL,
        };
        let installed = install_at(&host, &request, label, &paths, None).expect("install");

        assert_eq!(installed.name, label);
        assert_eq!(installed.runtime, paths.runtime);
        assert_eq!(installed.working_dir, instance);
        assert!(paths.plist.is_file(), "the definition must be on disk");
        assert!(paths.runtime.is_file(), "the runtime must be staged");
        assert!(paths.log_dir.is_dir(), "the log directory must exist");
        // The previous runtime is only kept while the new one is unproven.
        assert!(!Rollback::backup_name(&paths.runtime).exists());
        // Booted out before bootstrapping: launchd keeps serving the definition
        // a job was bootstrapped with.
        let calls = host.calls();
        let bootstrapped = calls
            .iter()
            .position(|call| call.starts_with(&format!("{LAUNCHCTL} bootstrap")))
            .expect("the job must be bootstrapped");
        assert!(
            calls[..bootstrapped]
                .iter()
                .any(|call| call.starts_with(&format!("{LAUNCHCTL} print {target}"))),
            "{calls:?}"
        );
    }

    #[test]
    fn an_upgrade_that_never_becomes_healthy_puts_the_previous_service_back() {
        let root = tempfile::tempdir().expect("create tempdir");
        let instance = root.path().join("edge-1");
        std::fs::create_dir_all(instance.join(".spice")).expect("create the instance");
        let paths = install_paths_under(root.path());
        owner_only_dir(&paths.runtime_dir);
        owner_only_dir(&root.path().join("LaunchAgents"));

        // What was serving before the upgrade.
        std::fs::write(&paths.plist, b"<plist>previous</plist>\n").expect("previous definition");
        fake_runtime(&paths.runtime);
        std::fs::write(paths.runtime.with_extension("stamp"), "previous-digest")
            .expect("previous stamp");
        let source = root.path().join("spiced-new");
        fake_runtime(&source);

        let label = "ai.spice.cloud-connect.x";
        let target = service_target(label, ServiceScope::User);
        // Loaded before, and never healthy after: the job keeps being
        // relaunched.
        let relaunching: Vec<String> = (0..400)
            .map(|i| {
                report(&[
                    ("state", "running"),
                    ("runs", &i.to_string()),
                    ("pid", "100"),
                ])
            })
            .collect();
        let relaunching: Vec<&str> = relaunching.iter().map(String::as_str).collect();
        let host = ScriptedHost::new()
            .sequence(&format!("{LAUNCHCTL} print {target}"), &relaunching)
            .health(&[HealthProbe::Healthy]);

        let request = InstallRequest {
            instance_dir: &instance,
            config_dir: &instance.join(".spice"),
            spiced_path: &source,
            scope: ServiceScope::User,
            health_url: HEALTH_URL,
        };
        let failed = install_at(&host, &request, label, &paths, None)
            .expect_err("an upgrade that never becomes healthy must not be reported as installed");
        let message = failed.to_string();
        assert!(message.contains("put back"), "{message}");

        assert_eq!(
            std::fs::read(&paths.plist).expect("the previous definition is back"),
            b"<plist>previous</plist>\n"
        );
        assert_eq!(
            std::fs::read_to_string(&paths.runtime).expect("the previous runtime is back"),
            "#!/bin/sh\nexit 0\n"
        );
        // The stamp described the source the replaced runtime came from, and
        // the restored binary did not come from it.
        assert!(!paths.runtime.with_extension("stamp").exists());
        assert!(!Rollback::backup_name(&paths.runtime).exists());
        // The job the failed install bootstrapped is gone, and the one that was
        // there before is loaded again.
        assert!(
            host.ran(&format!("{LAUNCHCTL} bootout {target}")),
            "{:?}",
            host.calls()
        );
    }

    #[test]
    fn a_first_install_that_never_becomes_healthy_leaves_nothing_installed() {
        let root = tempfile::tempdir().expect("create tempdir");
        let instance = root.path().join("edge-1");
        std::fs::create_dir_all(instance.join(".spice")).expect("create the instance");
        let paths = install_paths_under(root.path());
        let source = root.path().join("spiced");
        fake_runtime(&source);

        let label = "ai.spice.cloud-connect.x";
        let target = service_target(label, ServiceScope::User);
        let host = ScriptedHost::new().says(
            &format!("{LAUNCHCTL} print {target}"),
            &report(&[("state", "not running"), ("last exit code", "1")]),
        );
        let request = InstallRequest {
            instance_dir: &instance,
            config_dir: &instance.join(".spice"),
            spiced_path: &source,
            scope: ServiceScope::User,
            health_url: HEALTH_URL,
        };
        let failed = install_at(&host, &request, label, &paths, None)
            .expect_err("a service that never comes up is not installed");
        assert!(
            failed.to_string().contains("Nothing was left installed"),
            "{failed}"
        );
        assert!(!paths.plist.exists(), "the definition must be gone");
        assert!(!paths.runtime.exists(), "the staged runtime must be gone");
    }

    #[test]
    fn an_install_refuses_a_runtime_this_kernel_will_not_execute() {
        use std::os::unix::fs::PermissionsExt as _;

        let root = tempfile::tempdir().expect("create tempdir");
        let source = root.path().join("spiced");
        // A file that is not an executable image: what a truncated download, a
        // broken code signature, or the wrong architecture looks like to the
        // kernel.
        std::fs::write(&source, "not a program").expect("write");
        let staged = root.path().join("staged");
        std::fs::copy(&source, &staged).expect("stage");
        std::fs::set_permissions(&staged, std::fs::Permissions::from_mode(0o755))
            .expect("make it executable");

        let refused = verify_staged_runtime_executes(&staged, &source, None)
            .expect_err("launchd would load the job and never start it");
        let message = refused.to_string();
        assert!(message.contains("does not run"), "{message}");
        assert!(message.contains("nothing was installed"), "{message}");
    }

    #[test]
    fn logs_read_the_bounded_files_the_runtime_wrote() {
        use std::io::Write as _;

        let root = tempfile::tempdir().expect("create tempdir");
        let directory = root.path().join("logs");
        let mut log =
            runtime_cloud_connect::service_log::RotatingLog::open(&directory).expect("open");
        for line in ["first", "second", "third"] {
            log.write_all(format!("{line}\n").as_bytes())
                .expect("write");
        }
        log.flush().expect("flush");

        let mut manifest = manifest_for(Path::new("/opt/edge-1"), ServiceScope::User);
        manifest.log_source = Some(LogSource::Files {
            stdout: service_log::live_path(&directory),
            stderr: service_log::live_path(&directory),
        });
        assert_eq!(
            log_dir_for_manifest(&manifest).expect("a log directory"),
            directory
        );

        let reader = ServiceLogReader::new(&directory);
        let (history, _) = reader.read_history(2).expect("read history");
        assert_eq!(history, vec!["second", "third"]);

        let captured = logs(
            &manifest,
            LogRequest {
                number: 2,
                follow: false,
                capture: true,
            },
        )
        .expect("capture the history");
        assert_eq!(
            captured,
            Some(vec!["second".to_string(), "third".to_string()])
        );
    }

    #[test]
    fn logs_of_a_service_that_has_written_nothing_are_not_an_error() {
        let root = tempfile::tempdir().expect("create tempdir");
        let directory = root.path().join("logs");
        std::fs::create_dir_all(&directory).expect("create the log directory");
        let mut manifest = manifest_for(Path::new("/opt/edge-1"), ServiceScope::User);
        manifest.log_source = Some(LogSource::Files {
            stdout: service_log::live_path(&directory),
            stderr: service_log::live_path(&directory),
        });
        logs(
            &manifest,
            LogRequest {
                number: 100,
                follow: false,
                capture: false,
            },
        )
        .expect("a service with no output yet is a fact, not a failure");
    }

    #[test]
    fn recovery_hints_name_the_job_and_where_its_output_is() {
        let manifest = manifest_for(Path::new("/opt/edge-1"), ServiceScope::System);
        let hints = recovery_hints(&manifest);
        assert!(
            hints
                .iter()
                .any(|hint| hint.contains("launchctl print system/")),
            "{hints:?}"
        );
        assert!(
            hints.iter().any(|hint| hint.contains("spiced.log")),
            "{hints:?}"
        );
    }

    #[test]
    fn an_operator_facing_command_says_sudo_only_where_it_is_needed() {
        assert_eq!(
            launchctl_command(ServiceScope::System, &["print", "system/x"]),
            "sudo launchctl print system/x",
            "tests do not run as root, so a system command needs elevation"
        );
        assert_eq!(
            launchctl_command(ServiceScope::User, &["print", "gui/501/x"]),
            "launchctl print gui/501/x"
        );
    }

    #[test]
    fn only_launchctl_is_ever_executed_and_never_through_the_path() {
        // A bare name would let `PATH` choose what a privileged install runs,
        // and any other absolute path is not the tool this back end drives.
        for candidate in ["launchctl", "/tmp/evil/launchctl"] {
            let refused = trusted_supervisor_program(candidate)
                .expect_err("only the one absolute launchctl is executable");
            assert_eq!(refused.kind(), std::io::ErrorKind::InvalidInput);
        }
        // The real one resolves on macOS and nowhere else, which is what
        // `preflight` reports.
        let resolved = trusted_supervisor_program(LAUNCHCTL);
        assert_eq!(resolved.is_ok(), Path::new(LAUNCHCTL).is_file());
    }
}

/// The lifecycle driven against the launchd this host is actually running.
///
/// Ignored by default and never run by the gate: it bootstraps and boots out
/// real jobs in the invoking account's GUI domain, which is not something a
/// shared CI runner should have done to it, and it takes about a minute
/// because every install waits out a real [`SETTLE_WINDOW`]. Run it by hand on
/// a macOS desktop session:
///
/// ```text
/// cargo test -p spice --lib real_launchd -- --ignored --nocapture
/// ```
///
/// The runtime it installs is a shell script rather than `spiced`, because what
/// is under test is launchd's contract — `RunAtLoad`, `KeepAlive
/// {SuccessfulExit=false}`, `bootout`, `kickstart -k` — and a script is the
/// only way to ask for a crash, a clean exit, and a binary that will not run on
/// demand.
#[cfg(test)]
#[cfg(target_os = "macos")]
mod real_launchd {
    use std::cell::RefCell;
    use std::io::{Read as _, Write as _};
    use std::net::TcpListener;
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    use super::*;

    /// How long a state a real launchd reaches asynchronously is waited for.
    const WAIT: Duration = Duration::from_secs(45);
    const POLL: Duration = Duration::from_millis(200);

    /// A health endpoint the installer can actually reach, so the probe is
    /// exercised against a real socket rather than stubbed out.
    struct HealthEndpoint {
        url: String,
        healthy: Arc<AtomicBool>,
        running: Arc<AtomicBool>,
    }

    impl HealthEndpoint {
        fn start() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind a health endpoint");
            let port = listener.local_addr().expect("read the bound port").port();
            let healthy = Arc::new(AtomicBool::new(true));
            let running = Arc::new(AtomicBool::new(true));
            let answers = Arc::clone(&healthy);
            let alive = Arc::clone(&running);
            std::thread::spawn(move || {
                for stream in listener.incoming() {
                    if !alive.load(Ordering::SeqCst) {
                        return;
                    }
                    let Ok(mut stream) = stream else { continue };
                    let mut request = [0_u8; 512];
                    let _ = stream.read(&mut request);
                    let status = if answers.load(Ordering::SeqCst) {
                        "HTTP/1.1 200 OK"
                    } else {
                        "HTTP/1.1 503 Service Unavailable"
                    };
                    let _ = stream.write_all(
                        format!("{status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                            .as_bytes(),
                    );
                }
            });
            Self {
                url: format!("http://127.0.0.1:{port}/health"),
                healthy,
                running,
            }
        }
    }

    impl Drop for HealthEndpoint {
        fn drop(&mut self) {
            self.running.store(false, Ordering::SeqCst);
        }
    }

    /// Boots out every label this test loaded and removes everything it wrote
    /// under the account's own tree, whatever happens to the test.
    ///
    /// The assets go where a real user install puts them — `~/Library/LaunchAgents`,
    /// `~/Library/Application Support/spice/services`, `~/Library/Logs/Spice` —
    /// because that is the layout `uninstall` reasons about; a synthetic one
    /// would have it decline to remove a staged runtime it could not prove was
    /// this service's own. The labels are derived from a temporary instance
    /// directory, so they cannot collide with anything already installed.
    struct InstallGuard {
        labels: Vec<String>,
        paths: Vec<InstallPaths>,
    }

    impl Drop for InstallGuard {
        fn drop(&mut self) {
            for label in &self.labels {
                let _ = std::process::Command::new(LAUNCHCTL)
                    .args(["bootout", &service_target(label, ServiceScope::User)])
                    .output();
            }
            for paths in &self.paths {
                let _ = std::fs::remove_file(&paths.plist);
                let _ = std::fs::remove_dir_all(&paths.runtime_dir);
                let _ = std::fs::remove_dir_all(&paths.log_dir);
            }
        }
    }

    /// Write the stand-in runtime. `mode` is read at every start from a file
    /// the test rewrites, so one installed service can be made to serve, to
    /// exit cleanly, or to fail.
    fn write_fake_runtime(path: &Path, control: &Path) {
        use std::os::unix::fs::PermissionsExt as _;
        let script = format!(
            r#"#!/bin/sh
log_dir=""
while [ $# -gt 0 ]; do
  case "$1" in
    --service-log-dir) log_dir="$2"; shift 2;;
    --version) echo "spiced 0.0.0-fake"; exit 0;;
    *) shift;;
  esac
done
mkdir -p "$log_dir"
echo "started pid $$ mode $(cat {control} 2>/dev/null)" >> "$log_dir/spiced.log"
case "$(cat {control} 2>/dev/null)" in
  clean-exit) echo "exiting cleanly" >> "$log_dir/spiced.log"; exit 0;;
  fail) echo "failing" >> "$log_dir/spiced.log"; exit 3;;
  *) while true; do echo "tick $(date +%s) pid $$" >> "$log_dir/spiced.log"; sleep 1; done;;
esac
"#,
            control = control.display()
        );
        std::fs::write(path, script).expect("write the stand-in runtime");
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755))
            .expect("make the stand-in runtime executable");
    }

    fn set_mode(control: &Path, mode: &str) {
        std::fs::write(control, mode).expect("set the stand-in runtime's mode");
    }

    /// Where a real user install of `dir` puts its assets.
    fn paths_for(dir: &Path, label: &str) -> InstallPaths {
        install_paths(label, dir, ServiceScope::User).expect("resolve the install paths")
    }

    fn manifest_for(dir: &Path, label: &str, paths: &InstallPaths) -> ServiceManifest {
        ServiceManifest {
            schema_version: super::super::manifest::MANIFEST_SCHEMA_VERSION,
            directory: dir.to_path_buf(),
            name: label.to_string(),
            scope: ServiceScope::User,
            supervisor: Supervisor::Launchd,
            owner: super::super::ServiceOwner {
                uid: nix::unistd::Uid::effective().as_raw(),
                gid: nix::unistd::Gid::effective().as_raw(),
                name: super::super::account_name(nix::unistd::Uid::effective().as_raw()),
            },
            definition_path: paths.plist.clone(),
            runtime_path: paths.runtime.clone(),
            log_source: Some(LogSource::Files {
                stdout: service_log::live_path(&paths.log_dir),
                stderr: service_log::live_path(&paths.log_dir),
            }),
            runtime_digest: String::new(),
            runtime_version: "0.0.0-fake".to_string(),
            health_url: String::new(),
        }
    }

    /// Wait for launchd to report `wanted` for a user agent.
    fn await_reported(label: &str, wanted: ServiceState, what: &str) -> String {
        await_reported_in(label, ServiceScope::User, wanted, what)
    }

    /// Wait for launchd to report `wanted`, or say what it reported instead.
    fn await_reported_in(
        label: &str,
        scope: ServiceScope,
        wanted: ServiceState,
        what: &str,
    ) -> String {
        let host = ProcessHost;
        let deadline = Instant::now() + WAIT;
        let mut last = String::new();
        while Instant::now() < deadline {
            match print_job(&host, label, scope) {
                JobReport::Held(printed) => {
                    if normalize_launchd_state(&printed) == wanted {
                        return printed;
                    }
                    last = printed;
                }
                JobReport::Absent => {
                    if wanted == ServiceState::Stopped {
                        return String::new();
                    }
                    last = "launchd has no such job".to_string();
                }
                JobReport::Unanswerable(why) => last = why,
            }
            std::thread::sleep(POLL);
        }
        panic!("{what}: launchd never reported {wanted}. Last report: {last}");
    }

    fn pid_of(printed: &str) -> Option<String> {
        top_level_field(printed, "pid")
    }

    fn wait_for_log_line(log_dir: &Path, needle: &str, what: &str) {
        let reader = ServiceLogReader::new(log_dir);
        let deadline = Instant::now() + WAIT;
        while Instant::now() < deadline {
            if let Ok((lines, _)) = reader.read_history(500)
                && lines.iter().any(|line| line.contains(needle))
            {
                return;
            }
            std::thread::sleep(POLL);
        }
        panic!(
            "{what}: no log line containing {needle:?} in {}",
            log_dir.display()
        );
    }

    #[test]
    #[ignore = "drives the real launchd in this account's GUI domain; run by hand on a macOS desktop session"]
    fn the_whole_lifecycle_against_the_launchd_on_this_host() {
        assert!(
            gui_domain_is_available(&ProcessHost),
            "this test needs a GUI login session; run it from a desktop session, not over SSH"
        );

        let root = tempfile::tempdir().expect("create tempdir");
        let host = ProcessHost;
        let health = HealthEndpoint::start();

        // Two instance directories, so what one service does can be shown not
        // to touch the other.
        let one_dir = root.path().join("edge-1");
        let two_dir = root.path().join("edge-2");
        for dir in [&one_dir, &two_dir] {
            std::fs::create_dir_all(dir.join(".spice")).expect("create the instance directory");
        }
        let one_label = job_label_for_dir(&one_dir);
        let two_label = job_label_for_dir(&two_dir);
        let one = paths_for(&one_dir, &one_label);
        let two = paths_for(&two_dir, &two_label);
        let _guard = InstallGuard {
            labels: vec![one_label.clone(), two_label.clone()],
            paths: vec![one.clone(), two.clone()],
        };

        let control = root.path().join("mode");
        set_mode(&control, "serve");
        let source = root.path().join("spiced");
        write_fake_runtime(&source, &control);

        // ---- install ---------------------------------------------------
        let one_config = one_dir.join(".spice");
        let installed = install_at(
            &host,
            &InstallRequest {
                instance_dir: &one_dir,
                config_dir: &one_config,
                spiced_path: &source,
                scope: ServiceScope::User,
                health_url: &health.url,
            },
            &one_label,
            &one,
            None,
        )
        .expect("install the first service");
        assert_eq!(installed.name, one_label);
        let first_run = await_reported(&one_label, ServiceState::Running, "after installing");
        let first_pid = pid_of(&first_run).expect("a running job has a pid");
        wait_for_log_line(&one.log_dir, "started pid", "after installing");

        // The definition on disk is what `status` and `uninstall` resolve from,
        // and it has to name this directory and this identity to be adopted.
        let found = find_for_dir(&one_dir, ServiceScope::User)
            .expect("the installed definition must be discoverable from its directory alone");
        assert_eq!(found.name, one_label);
        assert_eq!(found.path, one.plist);
        assert_eq!(found.working_dir, one_dir);
        assert_eq!(found.config_dir.as_deref(), Some(one_config.as_path()));
        assert_eq!(found.runtime, one.runtime);

        let manifest = manifest_for(&one_dir, &one_label, &one);
        let observed = observe(&host, &manifest);
        assert_eq!(observed.state, ServiceState::Running);
        assert_eq!(
            observed.starts,
            ServiceStarts::LoginOnly,
            "a LaunchAgent starts with its owner's login session and not before"
        );

        // ---- a second instance, independent ----------------------------
        let second_health = HealthEndpoint::start();
        assert_ne!(
            health.url, second_health.url,
            "the two instances answer on different ports"
        );
        let two_config = two_dir.join(".spice");
        install_at(
            &host,
            &InstallRequest {
                instance_dir: &two_dir,
                config_dir: &two_config,
                spiced_path: &source,
                scope: ServiceScope::User,
                health_url: &second_health.url,
            },
            &two_label,
            &two,
            None,
        )
        .expect("install the second service");
        await_reported(
            &two_label,
            ServiceState::Running,
            "after installing the second",
        );
        wait_for_log_line(&two.log_dir, "started pid", "after installing the second");

        let (one_lines, _) = ServiceLogReader::new(&one.log_dir)
            .read_history(1000)
            .expect("read the first service's log");
        let two_pid = pid_of(&await_reported(
            &two_label,
            ServiceState::Running,
            "reading the second service's pid",
        ))
        .expect("a running job has a pid");
        assert!(
            !one_lines
                .iter()
                .any(|line| line.contains(&format!("pid {two_pid}"))),
            "one instance's log must carry no line from the other"
        );

        // ---- idempotent re-install -------------------------------------
        install_at(
            &host,
            &InstallRequest {
                instance_dir: &one_dir,
                config_dir: &one_config,
                spiced_path: &source,
                scope: ServiceScope::User,
                health_url: &health.url,
            },
            &one_label,
            &one,
            None,
        )
        .expect("re-installing is the upgrade path and must succeed");
        let reinstalled = await_reported(&one_label, ServiceState::Running, "after re-installing");
        assert_ne!(
            pid_of(&reinstalled).as_deref(),
            Some(first_pid.as_str()),
            "a re-install replaces the running process rather than leaving the old one"
        );

        // ---- stop, and stay stopped ------------------------------------
        stop(&host, &manifest).expect("stop");
        assert_eq!(
            print_job(&host, &one_label, ServiceScope::User),
            JobReport::Absent,
            "a stopped service is out of its domain"
        );
        assert!(
            one.plist.is_file(),
            "a stop leaves the service installed, so it comes back at the next login"
        );
        assert_eq!(observe(&host, &manifest).state, ServiceState::Stopped);
        // Idempotent.
        stop(&host, &manifest).expect("stopping a stopped service succeeds");

        // ---- start, and restart ----------------------------------------
        start(&host, &manifest).expect("start");
        let started = await_reported(&one_label, ServiceState::Running, "after starting");
        start(&host, &manifest).expect("starting a running service succeeds");
        assert_eq!(
            pid_of(&await_reported(
                &one_label,
                ServiceState::Running,
                "after a second start"
            )),
            pid_of(&started),
            "a start must not disturb the run that is already serving"
        );

        restart(&host, &manifest).expect("restart");
        let restarted = await_reported(&one_label, ServiceState::Running, "after restarting");
        assert_ne!(
            pid_of(&restarted),
            pid_of(&started),
            "a restart replaces the run"
        );

        // ---- a crash comes back ----------------------------------------
        let crashed_pid = pid_of(&restarted).expect("a running job has a pid");
        std::process::Command::new("/bin/kill")
            .args(["-9", &crashed_pid])
            .output()
            .expect("kill the running runtime");
        let deadline = Instant::now() + WAIT;
        let recovered = loop {
            assert!(
                Instant::now() < deadline,
                "launchd never brought the runtime back"
            );
            if let JobReport::Held(printed) = print_job(&host, &one_label, ServiceScope::User)
                && normalize_launchd_state(&printed) == ServiceState::Running
                && pid_of(&printed).as_deref() != Some(crashed_pid.as_str())
            {
                break printed;
            }
            std::thread::sleep(POLL);
        };
        assert!(pid_of(&recovered).is_some());

        // ---- a clean exit stays down -----------------------------------
        set_mode(&control, "clean-exit");
        // Whether the restart catches the process while it is briefly alive is
        // a race and not the contract; what is under test is what launchd does
        // *after* it ends. `KeepAlive {SuccessfulExit=false}` is the whole
        // point: a runtime that ended the way an operator's stop ends one must
        // not be relaunched.
        let _ = restart(&host, &manifest);
        std::thread::sleep(Duration::from_secs(
            u64::from(THROTTLE_INTERVAL_SECONDS) + 5,
        ));
        let after_clean_exit = print_job(&host, &one_label, ServiceScope::User);
        let JobReport::Held(printed) = &after_clean_exit else {
            panic!("the job should still be loaded: {after_clean_exit:?}");
        };
        assert_ne!(
            normalize_launchd_state(printed),
            ServiceState::Running,
            "a clean exit must not be relaunched: {printed}"
        );
        set_mode(&control, "serve");

        // ---- logs: history, and following across a rotation ------------
        restart(&host, &manifest).expect("bring the service back for the log checks");
        await_reported(&one_label, ServiceState::Running, "before the log checks");
        wait_for_log_line(&one.log_dir, "tick", "before the log checks");

        let reader = ServiceLogReader::new(&one.log_dir);
        let (_, cursor) = reader.read_history(0).expect("take a follow cursor");
        // Rotate the way the runtime's own appender does — by rename — while it
        // keeps writing, and confirm the follower ends up on the replacement.
        std::fs::rename(
            service_log::live_path(&one.log_dir),
            service_log::rotated_path(&one.log_dir, 1),
        )
        .expect("rotate the live log");
        let seen: RefCell<Vec<String>> = RefCell::new(Vec::new());
        let deadline = Instant::now() + WAIT;
        reader
            .follow(
                cursor,
                |line| seen.borrow_mut().push(line.to_string()),
                || {
                    Instant::now() < deadline
                        && !(service_log::live_path(&one.log_dir).exists()
                            && seen.borrow().iter().any(|line| line.contains("tick")))
                },
            )
            .expect("follow across the rotation");
        let seen = seen.into_inner();
        assert!(
            seen.iter().any(|line| line.contains("tick")),
            "the follower must keep printing after the file it held was renamed: {seen:?}"
        );

        // ---- an upgrade that never comes up is rolled back --------------
        let broken = root.path().join("spiced-broken");
        std::fs::write(&broken, "#!/bin/sh\nexit 3\n").expect("write a runtime that fails");
        std::fs::set_permissions(&broken, std::fs::Permissions::from_mode(0o755))
            .expect("make it executable");
        let good_runtime = std::fs::read(&one.runtime).expect("read the runtime that works");
        let failed = install_at(
            &host,
            &InstallRequest {
                instance_dir: &one_dir,
                config_dir: &one_config,
                spiced_path: &broken,
                scope: ServiceScope::User,
                health_url: &health.url,
            },
            &one_label,
            &one,
            None,
        )
        .expect_err("an upgrade to a runtime that will not stay up must not be reported installed");
        assert!(failed.to_string().contains("put back"), "{failed}");
        assert_eq!(
            std::fs::read(&one.runtime).expect("read the restored runtime"),
            good_runtime,
            "the runtime that was serving must be back on the path the service executes"
        );
        await_reported(&one_label, ServiceState::Running, "after the rollback");

        // The other instance was never touched by any of it.
        assert_eq!(
            observe(&host, &manifest_for(&two_dir, &two_label, &two)).state,
            ServiceState::Running,
            "one instance's failed upgrade must not disturb another's service"
        );

        // ---- uninstall keeps the logs and takes everything else ---------
        let log_lines_before = ServiceLogReader::new(&one.log_dir)
            .read_history(10_000)
            .expect("read the log before uninstalling")
            .0
            .len();
        uninstall(&host, &manifest).expect("uninstall");
        assert_eq!(
            print_job(&host, &one_label, ServiceScope::User),
            JobReport::Absent
        );
        assert!(!one.plist.exists(), "the definition must be gone");
        assert!(!one.runtime_dir.exists(), "the staged runtime must be gone");
        assert!(
            service_log::live_path(&one.log_dir).exists(),
            "the logs are what an operator reads after removing a misbehaving service"
        );

        // ---- reinstalling the same directory keeps its identity ---------
        let reinstalled = install_at(
            &host,
            &InstallRequest {
                instance_dir: &one_dir,
                config_dir: &one_config,
                spiced_path: &source,
                scope: ServiceScope::User,
                health_url: &health.url,
            },
            &one_label,
            &one,
            None,
        )
        .expect("reinstall");
        assert_eq!(
            reinstalled.name, one_label,
            "the same directory derives the same label, so it is the same service"
        );
        await_reported(&one_label, ServiceState::Running, "after reinstalling");
        let after = ServiceLogReader::new(&one.log_dir)
            .read_history(10_000)
            .expect("read the log after reinstalling")
            .0;
        assert!(
            after.len() > log_lines_before,
            "a reinstall continues the log it had rather than starting a new one"
        );

        // ---- an unhealthy instance fails the gate -----------------------
        health.healthy.store(false, Ordering::SeqCst);
        let unhealthy_dir = root.path().join("edge-3");
        std::fs::create_dir_all(unhealthy_dir.join(".spice")).expect("create the instance");
        let unhealthy_label = job_label_for_dir(&unhealthy_dir);
        let unhealthy = paths_for(&unhealthy_dir, &unhealthy_label);
        let _unhealthy_guard = InstallGuard {
            labels: vec![unhealthy_label.clone()],
            paths: vec![unhealthy.clone()],
        };
        let refused = install_at(
            &host,
            &InstallRequest {
                instance_dir: &unhealthy_dir,
                config_dir: &unhealthy_dir.join(".spice"),
                spiced_path: &source,
                scope: ServiceScope::User,
                health_url: &health.url,
            },
            &unhealthy_label,
            &unhealthy,
            None,
        )
        .expect_err("an instance that reports itself unhealthy must not install");
        assert!(refused.to_string().contains("unhealthy"), "{refused}");
        assert!(!unhealthy.plist.exists(), "nothing must be left installed");

        // ---- and the second instance is still serving throughout --------
        uninstall(&host, &manifest_for(&two_dir, &two_label, &two)).expect("uninstall the second");
        uninstall(&host, &manifest_for(&one_dir, &one_label, &one)).expect("clean up the first");
    }

    /// The system half of the lifecycle: a `LaunchDaemon` in the `system`
    /// domain, running as the operator who invoked `sudo` rather than as root.
    ///
    /// Needs root, and running `cargo` as root would leave root-owned artefacts
    /// in the target directory, so build the test binary as yourself and run
    /// just this test under `sudo`:
    ///
    /// ```text
    /// cargo test -p spice --lib --no-run
    /// sudo <the spice-*.rs test binary cargo printed> \
    ///     --ignored --exact --nocapture \
    ///     commands::connect::service::launchd::real_launchd::the_system_daemon_lifecycle_against_the_launchd_on_this_host
    /// ```
    #[test]
    #[ignore = "installs a real LaunchDaemon; run the test binary under sudo, not cargo"]
    fn the_system_daemon_lifecycle_against_the_launchd_on_this_host() {
        // Skipped rather than failed without root, so running every ignored
        // test as the operator exercises the agent half and says why it stopped
        // short of the daemon half.
        if !super::super::is_root() || std::env::var_os("SUDO_UID").is_none() {
            eprintln!(
                "skipping the system daemon lifecycle: run this test's binary under `sudo` from \
                 an operator account, so a LaunchDaemon is installed with root privileges and \
                 runs as that operator"
            );
            return;
        }
        let account =
            super::super::service_account(Path::new("/"), super::super::RootFallback::Allow)
                .expect("resolving the service account from a root-owned directory");

        let host = ProcessHost;
        let health = HealthEndpoint::start();
        // Not the default temporary directory: under `sudo` that is root's own
        // per-user cache under `/var/folders`, which is mode 0700 and root
        // owned — so the daemon, which runs as the operator, could not traverse
        // into its own working directory and launchd would never spawn it.
        // `/private/tmp` is traversable by every account on the host.
        let root = tempfile::Builder::new()
            .prefix("spice-launchd-system")
            .tempdir_in("/private/tmp")
            .expect("create a temporary directory the operator can reach");
        let dir = root.path().join("edge-system");
        let config = dir.join(".spice");
        std::fs::create_dir_all(&config).expect("create the instance directory");
        // Enrollment creates this tree as the operator, and a privileged
        // install verifies rather than changes that — so the test has to hand
        // it over the way enrollment would.
        for path in [root.path(), dir.as_path(), config.as_path()] {
            std::os::unix::fs::chown(path, Some(account.uid), Some(account.gid))
                .expect("give the instance directory to the operator");
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
                .expect("restrict the instance directory to its owner");
        }

        let control = root.path().join("mode");
        set_mode(&control, "serve");
        std::os::unix::fs::chown(&control, Some(account.uid), Some(account.gid))
            .expect("give the control file to the operator");
        let source = root.path().join("spiced");
        write_fake_runtime(&source, &control);

        let label = job_label_for_dir(&dir);
        let paths = install_paths(&label, &dir, ServiceScope::System)
            .expect("resolve the system install paths");
        let _guard = SystemInstallGuard {
            label: label.clone(),
            paths: paths.clone(),
        };

        let installed = install_at(
            &host,
            &InstallRequest {
                instance_dir: &dir,
                config_dir: &config,
                spiced_path: &source,
                scope: ServiceScope::System,
                health_url: &health.url,
            },
            &label,
            &paths,
            Some(account),
        )
        .expect("install a system daemon");
        assert_eq!(installed.path, paths.plist);
        assert!(
            paths.plist.starts_with("/Library/LaunchDaemons"),
            "{}",
            paths.plist.display()
        );

        let printed = await_reported_in(
            &label,
            ServiceScope::System,
            ServiceState::Running,
            "after installing the daemon",
        );
        assert!(pid_of(&printed).is_some());

        // The definition launchd loaded names the operator, not root: a
        // privileged installer must not run user configuration as root.
        let plist = std::fs::read_to_string(&paths.plist).expect("read the definition");
        let names = account_names(account).expect("resolve the operator's names");
        assert!(
            plist.contains(&format!(
                "<key>UserName</key>\n\t<string>{}</string>",
                names.0
            )),
            "{plist}"
        );
        assert_ne!(
            names.0, "root",
            "this test must be run under sudo, not as a root login"
        );

        // launchd refuses a daemon that is not root-owned and root-writable
        // only, and says so nowhere the operator will look.
        let meta = std::fs::metadata(&paths.plist).expect("stat the definition");
        assert_eq!(meta.uid(), 0);
        assert_eq!(meta.gid(), 0);
        assert_eq!(meta.permissions().mode() & 0o7777, PLIST_MODE);

        // The daemon writes its own log as the operator, under a root-only
        // tree the operator cannot substitute.
        let log_meta = std::fs::metadata(&paths.log_dir).expect("stat the log directory");
        assert_eq!(log_meta.uid(), account.uid);
        wait_for_log_line(&paths.log_dir, "started pid", "after installing the daemon");

        let manifest = ServiceManifest {
            scope: ServiceScope::System,
            definition_path: paths.plist.clone(),
            runtime_path: paths.runtime.clone(),
            log_source: Some(LogSource::Files {
                stdout: service_log::live_path(&paths.log_dir),
                stderr: service_log::live_path(&paths.log_dir),
            }),
            owner: super::super::ServiceOwner {
                uid: account.uid,
                gid: account.gid,
                name: super::super::account_name(account.uid),
            },
            ..manifest_for(&dir, &label, &paths)
        };
        let observed = observe(&host, &manifest);
        assert_eq!(observed.state, ServiceState::Running);
        assert_eq!(
            observed.starts,
            ServiceStarts::BootWithoutLogin,
            "a LaunchDaemon is the one that comes up at boot with nobody logged in"
        );
        assert_eq!(observed.starts_action, None);

        // The definition is discoverable from the directory alone.
        let found = find_for_dir(&dir, ServiceScope::System).expect("find the installed daemon");
        assert_eq!(found.name, label);
        assert_eq!(found.config_dir.as_deref(), Some(config.as_path()));

        stop(&host, &manifest).expect("stop the daemon");
        assert_eq!(
            print_job(&host, &label, ServiceScope::System),
            JobReport::Absent
        );
        assert!(paths.plist.is_file(), "a stop leaves the daemon installed");
        start(&host, &manifest).expect("start the daemon");
        await_reported_in(
            &label,
            ServiceScope::System,
            ServiceState::Running,
            "after starting the daemon",
        );

        uninstall(&host, &manifest).expect("uninstall the daemon");
        assert!(!paths.plist.exists());
        assert!(!paths.runtime_dir.exists());
        assert!(
            service_log::live_path(&paths.log_dir).exists(),
            "the logs outlive the service they describe"
        );
    }

    /// Removes the system daemon this test installed, whatever happens to it.
    struct SystemInstallGuard {
        label: String,
        paths: InstallPaths,
    }

    impl Drop for SystemInstallGuard {
        fn drop(&mut self) {
            let _ = std::process::Command::new(LAUNCHCTL)
                .args([
                    "bootout",
                    &service_target(&self.label, ServiceScope::System),
                ])
                .output();
            let _ = std::fs::remove_file(&self.paths.plist);
            let _ = std::fs::remove_dir_all(&self.paths.runtime_dir);
            let _ = std::fs::remove_dir_all(&self.paths.log_dir);
        }
    }
}
