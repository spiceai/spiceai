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

//! The macOS back end: one `LaunchDaemon` per instance directory.
//!
//! launchd fails quietly in three ways this module has to convert into errors.
//! It refuses a daemon definition whose ownership or permissions are wrong. It
//! will happily bootstrap a job whose program the kernel then declines to
//! execute — a binary still carrying `com.apple.quarantine`, or one whose code
//! signature is broken. And it keeps serving the definition a job was
//! bootstrapped with, so loading over a job that is still there silently leaves
//! the old one in force.
//!
//! So: the definition's ownership is set and read back, the previous job is
//! unloaded and its absence confirmed before the new one is loaded, the staged
//! runtime is executed once before the job is created, and the install does not
//! return until launchd has reported the job running and still running a moment
//! later.

use std::path::{Path, PathBuf};
use std::time::Duration;

use super::backend::{
    InstallRequest, LogRequest, ServiceBackend, ServiceObservation, lifecycle_pending,
};
use super::manifest::ServiceManifest;
use super::model::{LogSource, ServiceScope, ServiceStarts, ServiceState, Supervisor};
use super::{InstalledService, PreflightFailure};
use crate::error::{Error, Result};

/// Directory launchd reads administrator-provided daemon definitions from.
const LAUNCH_DAEMON_DIR: &str = "/Library/LaunchDaemons";

/// Directory launchd reads a user's own agent definitions from, relative to
/// the account's home directory.
const LAUNCH_AGENT_SUBDIR: &str = "Library/LaunchAgents";

/// Shared prefix of every job label this command installs. Also the prefix used
/// to discover installed instances.
const LABEL_PREFIX: &str = "ai.spice.cloud-connect";

/// Suffix of every daemon definition this command installs.
const PLIST_SUFFIX: &str = ".plist";

/// Mode a daemon definition must have: readable by all, writable only by root.
const PLIST_MODE: u32 = 0o644;

/// Absolute path to `launchctl`, so a root process is not steered by `PATH`.
const LAUNCHCTL: &str = "/bin/launchctl";

/// What `launchctl` says when the job is not loaded, as opposed to when it
/// could not answer at all.
const NO_SUCH_SERVICE: &str = "Could not find service";

/// The state reported for a job launchd does not have.
const NOT_LOADED: &str = "not loaded";

/// The state launchd reports for a job whose process exists.
const RUNNING: &str = "running";

/// How long to wait for a job to leave the domain after `bootout` was accepted.
/// `bootout` returns as soon as launchd has been told, and the runtime is given
/// `ExitTimeOut` to stop before it is killed, so this has to outlast that with
/// room to spare — waiting too long only costs a slow reinstall, while giving
/// up too early reports a failure for a service that was on its way out.
const UNLOAD_ATTEMPTS: u32 = 240;

/// How long to wait when `bootout` itself failed. Nothing is coming, so this is
/// only enough to catch a job that was on its way out already.
const UNLOAD_GRACE_ATTEMPTS: u32 = 4;
const UNLOAD_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// A short cushion for a `bootstrap` that lands while launchd is still settling.
const BOOTSTRAP_ATTEMPTS: u32 = 5;
const BOOTSTRAP_RETRY_INTERVAL: Duration = Duration::from_millis(200);

/// How long to wait for launchd to report the job running before treating the
/// install as failed.
const RUNNING_ATTEMPTS: u32 = 60;
const RUNNING_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// How long the job has to stay up on the same process before the install is
/// called a success.
const SETTLE_INTERVAL: Duration = Duration::from_secs(2);

/// Derive this instance directory's job label.
pub(super) fn job_label_for_dir(dir: &Path) -> String {
    format!(
        "{LABEL_PREFIX}.{stem}",
        stem = super::name_stem_for_dir(dir)
    )
}

/// The definition a label is installed as, in the domain `scope` selects.
fn plist_path(label: &str, scope: ServiceScope) -> PathBuf {
    let dir = match scope {
        ServiceScope::System => PathBuf::from(LAUNCH_DAEMON_DIR),
        ServiceScope::User => dirs::home_dir()
            .unwrap_or_default()
            .join(LAUNCH_AGENT_SUBDIR),
    };
    dir.join(format!("{label}{PLIST_SUFFIX}"))
}

/// How `launchctl` is asked about a job. Agents live in their owner's GUI
/// domain, daemons in the system domain, and naming the wrong one is how a
/// command reports on a job that is not the one installed.
fn service_target_in(label: &str, scope: ServiceScope) -> String {
    match scope {
        ServiceScope::System => format!("system/{label}"),
        ServiceScope::User => format!("gui/{}/{label}", nix::unistd::Uid::effective().as_raw()),
    }
}

/// How `launchctl` is asked about a system daemon — the only domain this
/// release installs into.
fn service_target(label: &str) -> String {
    service_target_in(label, ServiceScope::System)
}

/// Render the daemon definition for an instance.
///
/// `instance_dir` is baked in as `WorkingDirectory` so the daemon resolves its
/// spicepod from the directory the operator enrolled, not from wherever
/// launchd happens to start it. `config_dir` preserves the resolved Spice state
/// directory and `spiced_path` is the absolute binary path resolved at install
/// time.
pub(super) fn render_plist(
    label: &str,
    instance_dir: &Path,
    config_dir: &Path,
    spiced_path: &Path,
    user_name: &str,
    group_name: &str,
) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>Label</key>
	<string>{label}</string>
	<key>ProgramArguments</key>
	<array>
		<string>{spiced}</string>
	</array>
	<key>WorkingDirectory</key>
	<string>{instance_dir}</string>
	<key>UserName</key>
	<string>{user_name}</string>
	<key>GroupName</key>
	<string>{group_name}</string>
	<key>EnvironmentVariables</key>
	<dict>
		<key>SPICE_CONFIG_DIR</key>
		<string>{config_dir}</string>
	</dict>
	<key>RunAtLoad</key>
	<true/>
	<!-- A deployment applies to the running instance, so this is not what makes
	     one land: it is what brings the instance back from the things that do
	     end it — a reboot, an OOM kill, an unhandled failure. launchd never
	     gives up on a KeepAlive job, so a crash loop is throttled rather than
	     abandoned. -->
	<key>KeepAlive</key>
	<true/>
	<key>ThrottleInterval</key>
	<integer>5</integer>
	<key>ExitTimeOut</key>
	<integer>30</integer>
	<key>ProcessType</key>
	<string>Standard</string>
</dict>
</plist>
"#,
        label = xml_escape(label),
        spiced = xml_escape(&spiced_path.display().to_string()),
        instance_dir = xml_escape(&instance_dir.display().to_string()),
        config_dir = xml_escape(&config_dir.display().to_string()),
        user_name = xml_escape(user_name),
        group_name = xml_escape(group_name),
    )
}

/// launchd requires account names rather than the numeric ids systemd accepts.
fn account_names(account: super::ServiceAccount) -> Result<(String, String)> {
    let user = nix::unistd::User::from_uid(nix::unistd::Uid::from_raw(account.uid))
        .map_err(|e| Error::CloudConnectIo {
            message: format!("look up service user {}: {e}", account.uid),
        })?
        .ok_or_else(|| Error::InvalidArgument {
            message: format!(
                "Failed to install the Spice Cloud Connect service: uid {} has no local account name for launchd.",
                account.uid
            ),
        })?;
    let group = nix::unistd::Group::from_gid(nix::unistd::Gid::from_raw(account.gid))
        .map_err(|e| Error::CloudConnectIo {
            message: format!("look up service group {}: {e}", account.gid),
        })?
        .ok_or_else(|| Error::InvalidArgument {
            message: format!(
                "Failed to install the Spice Cloud Connect service: gid {} has no local group name for launchd.",
                account.gid
            ),
        })?;
    Ok((user.name, group.name))
}

/// Map a `launchctl print` `state` value onto the normalized vocabulary.
fn normalize_launchd_state(reported: &str) -> ServiceState {
    match reported.trim() {
        "running" => ServiceState::Running,
        "spawn scheduled" | "spawning" => ServiceState::Starting,
        "exiting" | "stopping" => ServiceState::Stopping,
        // launchd's word for a `KeepAlive` job it is holding but not running:
        // installed and down.
        "waiting" | "not running" => ServiceState::Stopped,
        "error" | "exited" => ServiceState::Failed,
        // Everything else, including launchd's `not loaded` for a job it does
        // not have. The definition file decides `not_installed`, so this is a
        // supervisor that disagrees with disk rather than an absence.
        _ => ServiceState::Unavailable,
    }
}

fn preflight(scope: ServiceScope) -> std::result::Result<(), PreflightFailure> {
    if scope == ServiceScope::User {
        return Err(PreflightFailure::UserScopePending);
    }
    Ok(())
}

fn install(request: &InstallRequest<'_>) -> Result<InstalledService> {
    let InstallRequest {
        instance_dir,
        config_dir,
        spiced_path,
        scope,
    } = *request;

    let account = super::service_account(instance_dir)?;
    let (user_name, group_name) = account_names(account)?;
    super::provision_config_ownership(config_dir, account)?;

    let staged_runtime = super::stage_runtime(spiced_path, |staged, source| {
        verify_staged_runtime_executes(staged, source, account)
    })?;

    let label = job_label_for_dir(instance_dir);
    let path = plist_path(&label, scope);
    write_daemon_plist(
        &path,
        &render_plist(
            &label,
            instance_dir,
            config_dir,
            &staged_runtime,
            &user_name,
            &group_name,
        ),
    )?;

    // Before loading, not after: launchd serves the definition a job was
    // bootstrapped with, so loading over a job that is still there would leave
    // the previous definition — and the previous binary — in force while this
    // reported the new one.
    unload(&label)?;
    load(&label, &path)?;
    confirm_running(&label)?;

    Ok(InstalledService {
        name: label,
        path,
        working_dir: instance_dir.to_path_buf(),
        runtime: staged_runtime,
    })
}

/// Unload the job the manifest describes and delete its definition, touching
/// nothing else.
fn uninstall(manifest: &ServiceManifest) -> Result<()> {
    let unloaded = unload(&manifest.name);

    // Deleting the definition is what must happen even when the job would not
    // stop: one left on disk starts the runtime again at the next boot, against
    // a released identity. Already gone is the retry of exactly that case.
    if let Err(e) = std::fs::remove_file(&manifest.definition_path)
        && e.kind() != std::io::ErrorKind::NotFound
    {
        // The unload failure is lost otherwise, and a job still running is the
        // more urgent half of the problem.
        let also = unloaded
            .err()
            .map_or_else(String::new, |err| format!(" {}", folded(err)));
        return Err(Error::CloudConnectIo {
            message: format!(
                "remove the launchd daemon definition {}: {e}. The daemon would start again at \
                 boot against a released identity — re-run \
                 `sudo spice connect service uninstall`.{also}",
                manifest.definition_path.display()
            ),
        });
    }

    // Surfaced after the deletion, not instead of it: a job launchd is still
    // holding keeps restarting against the released identity until the host
    // reboots, and the operator has to be told.
    unloaded
}

/// The service installed for `instance_dir` in `scope`, if something that knows
/// what the job actually runs states that its working directory is this one.
///
/// The label proves nothing on its own: it is derived from the directory, so a
/// job anyone else created under it carries the same label. Adoption therefore
/// needs the working directory *stated* by a source that knows — the definition
/// on disk, or launchd's own report of the job it is holding — and it has to
/// match exactly. Neither available means no service is resolved, which is the
/// safe answer: the alternative is uninstalling a foreign job under a label this
/// directory happens to derive.
///
/// launchd's report is consulted because a job it is still holding counts even
/// with no definition on disk: `uninstall` deletes the definition whether or not
/// the job could be stopped, so if this looked only at the file, the state that
/// leaves behind would be invisible to the removal that has to clean it up, and
/// the released instance would keep restarting until the host rebooted.
fn find_for_dir(instance_dir: &Path, scope: ServiceScope) -> Option<InstalledService> {
    let label = job_label_for_dir(instance_dir);
    let path = plist_path(&label, scope);
    let plist = std::fs::read_to_string(&path).ok();

    // The definition is asked first because it is what an uninstall deletes. A
    // definition that names another directory settles the question on its own —
    // launchd is not consulted to overrule it — and one that names no directory
    // at all leaves launchd as the only source that can still answer.
    let working_dir = plist
        .as_deref()
        .and_then(parse_working_dir)
        .or_else(|| launchd_working_dir(&label, scope))?;
    if working_dir != instance_dir {
        return None;
    }

    let runtime = plist
        .as_deref()
        .and_then(parse_program)
        .unwrap_or_else(super::staged_runtime_path);
    Some(InstalledService {
        name: label,
        path,
        working_dir,
        runtime,
    })
}

/// The working directory launchd reports for the job it is holding under
/// `label` in `scope`, or `None` when there is no such job, launchd could not be
/// asked, or it named no directory.
///
/// This is the independent statement that lets an orphaned job — one whose
/// definition is already deleted — still be adopted and removed, without
/// inferring anything from the label.
fn launchd_working_dir(label: &str, scope: ServiceScope) -> Option<PathBuf> {
    let printed = print_job(label, scope)?;
    // `launchctl print` has spelled this field both ways across macOS releases.
    ["working directory", "cwd"]
        .into_iter()
        .find_map(|key| top_level_field(&printed, key))
        .map(PathBuf::from)
}

/// The stdout/stderr files a definition names, when it names any.
///
/// `None` is the honest answer for a definition that configures no output
/// paths: launchd then sends the job's output to the unified system log, which
/// is not a source `spice connect service logs` reads. Configuring explicit
/// paths is the launchd lifecycle work.
fn plist_log_source(label: &str, scope: ServiceScope) -> Option<LogSource> {
    let plist = std::fs::read_to_string(plist_path(label, scope)).ok()?;
    let stdout = first_string_after_key(&plist, "StandardOutPath")?;
    let stderr =
        first_string_after_key(&plist, "StandardErrorPath").unwrap_or_else(|| stdout.clone());
    Some(LogSource::Files {
        stdout: PathBuf::from(stdout),
        stderr: PathBuf::from(stderr),
    })
}

/// The job's state as `launchctl print` reports it (`running`, `waiting`,
/// `not running`, …), [`NOT_LOADED`] when launchd has no such job, or `None`
/// when launchd could not be asked.
fn is_active(label: &str, scope: ServiceScope) -> Option<String> {
    let output = std::process::Command::new(LAUNCHCTL)
        .arg("print")
        .arg(service_target_in(label, scope))
        .output()
        .ok()?;

    if output.status.success() {
        return top_level_field(&String::from_utf8_lossy(&output.stdout), "state");
    }

    // launchd distinguishes "no such job" from "I could not answer"; reporting
    // the second as a state would tell the operator the instance is gone when
    // it may be running.
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    said.contains(NO_SUCH_SERVICE)
        .then(|| NOT_LOADED.to_string())
}

fn recovery_hints(manifest: &ServiceManifest) -> Vec<String> {
    let target = service_target_in(&manifest.name, manifest.scope);
    let sudo = if manifest.scope == ServiceScope::System && !super::is_root() {
        "sudo "
    } else {
        ""
    };
    vec![
        format!("{sudo}launchctl print {target}"),
        format!("{sudo}launchctl kickstart -k {target}"),
    ]
}

/// Observe the job: the state launchd reports plus whether it comes back on
/// its own.
fn observe(manifest: &ServiceManifest) -> ServiceObservation {
    let Some(reported) = is_active(&manifest.name, manifest.scope) else {
        return ServiceObservation::unavailable(format!(
            "launchctl could not be asked about {}. Check that this account may query the \
             {} domain (`{}`).",
            manifest.name,
            manifest.scope,
            recovery_hints(manifest)
                .first()
                .map_or("launchctl print", String::as_str),
        ));
    };
    let state = normalize_launchd_state(&reported);
    let (starts, starts_action) = observe_persistence(manifest);
    ServiceObservation {
        state,
        starts,
        // An `unavailable` state always says why. `not loaded` is the one that
        // matters most: launchd has no such job while its definition is on
        // disk, which is a different repair from a word this release does not
        // recognise.
        diagnostic: (state == ServiceState::Unavailable)
            .then(|| unrecognized_state_diagnostic(&reported, manifest)),
        starts_action,
    }
}

/// Why a state Spice does not recognise is reported as `unavailable`, naming
/// launchd's own answer and the command that produced it.
fn unrecognized_state_diagnostic(reported: &str, manifest: &ServiceManifest) -> String {
    format!(
        "`launchctl print {target}` reported state `{reported}`, which is not a state Spice can \
         act on. Run it to see what launchd holds for this job.",
        target = service_target_in(&manifest.name, manifest.scope),
        reported = reported.trim(),
    )
}

/// Whether the job will start on its own, and what to run when it will not.
///
/// The scope decides the ceiling: a daemon is bootstrapped into the system
/// domain and runs before anyone logs in, while an agent lives in its owner's
/// GUI domain and cannot run before that owner is there — macOS offers no
/// non-root way to make an agent start earlier, so there is nothing to advise.
///
/// The ceiling is not the answer on its own, though. launchd keeps a *separate*
/// disabled database, so a definition that is installed and correct can still be
/// held down by `launchctl disable`, and inferring persistence from the scope
/// alone would report `boot_without_login` for a daemon that will not start at
/// all.
fn observe_persistence(manifest: &ServiceManifest) -> (ServiceStarts, Option<String>) {
    match job_is_disabled(&manifest.name, manifest.scope) {
        Some(true) => (
            ServiceStarts::Disabled,
            Some(format!(
                "{sudo}launchctl enable {target}",
                sudo = if manifest.scope == ServiceScope::System && !super::is_root() {
                    "sudo "
                } else {
                    ""
                },
                target = service_target_in(&manifest.name, manifest.scope),
            )),
        ),
        Some(false) => match manifest.scope {
            ServiceScope::System => (ServiceStarts::BootWithoutLogin, None),
            ServiceScope::User => (ServiceStarts::LoginOnly, None),
        },
        // The database could not be read, so persistence is not knowable. A
        // guess here is the claim of boot persistence this check exists to
        // avoid making.
        None => (ServiceStarts::Unavailable, None),
    }
}

/// Whether launchd's disabled database holds this label down in `scope`.
///
/// `None` when the database could not be read at all — distinct from a label it
/// simply does not list, which is `Some(false)`.
fn job_is_disabled(label: &str, scope: ServiceScope) -> Option<bool> {
    let domain = match scope {
        ServiceScope::System => "system".to_string(),
        ServiceScope::User => format!("user/{}", nix::unistd::Uid::effective().as_raw()),
    };
    let output = std::process::Command::new(LAUNCHCTL)
        .arg("print-disabled")
        .arg(&domain)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(parse_disabled(
        &String::from_utf8_lossy(&output.stdout),
        label,
    ))
}

/// Whether `print-disabled` output lists `label` as disabled.
///
/// The output is a list of `"<label>" => <value>` lines, where the value has
/// been spelled `true`/`false` and `disabled`/`enabled` across macOS releases.
/// A label the list does not mention is not disabled.
fn parse_disabled(printed: &str, label: &str) -> bool {
    let quoted = format!("\"{label}\"");
    printed
        .lines()
        .map(str::trim)
        .filter_map(|line| line.split_once("=>"))
        .find(|(name, _)| name.trim() == quoted)
        .is_some_and(|(_, value)| {
            let value = value.trim().trim_end_matches(';').trim();
            value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("disabled")
        })
}

/// The macOS back end.
pub(super) struct LaunchdBackend;

impl ServiceBackend for LaunchdBackend {
    fn supervisor(&self) -> Supervisor {
        Supervisor::Launchd
    }

    fn preflight(&self, scope: ServiceScope) -> std::result::Result<(), PreflightFailure> {
        preflight(scope)
    }

    fn name_for_dir(&self, instance_dir: &Path) -> String {
        job_label_for_dir(instance_dir)
    }

    fn definition_path(&self, name: &str, scope: ServiceScope) -> PathBuf {
        plist_path(name, scope)
    }

    fn log_source(&self, name: &str, scope: ServiceScope) -> Option<LogSource> {
        plist_log_source(name, scope)
    }

    fn find_installed(&self, instance_dir: &Path, scope: ServiceScope) -> Option<InstalledService> {
        find_for_dir(instance_dir, scope)
    }

    fn install(&self, request: &InstallRequest<'_>) -> Result<InstalledService> {
        install(request)
    }

    fn uninstall(&self, manifest: &ServiceManifest) -> Result<()> {
        uninstall(manifest)
    }

    fn start(&self, manifest: &ServiceManifest) -> Result<()> {
        Err(lifecycle_pending(self, "start", manifest))
    }

    fn stop(&self, manifest: &ServiceManifest) -> Result<()> {
        Err(lifecycle_pending(self, "stop", manifest))
    }

    fn restart(&self, manifest: &ServiceManifest) -> Result<()> {
        Err(lifecycle_pending(self, "restart", manifest))
    }

    fn observe(&self, manifest: &ServiceManifest) -> ServiceObservation {
        observe(manifest)
    }

    fn logs(&self, manifest: &ServiceManifest, _request: LogRequest) -> Result<()> {
        Err(lifecycle_pending(self, "read the logs of", manifest))
    }

    fn recovery_hints(&self, manifest: &ServiceManifest) -> Vec<String> {
        recovery_hints(manifest)
    }
}

/// The text of an error being folded into another one, without the second
/// `Cloud Connect I/O error:` prefix that nesting the whole `Display` would add.
fn folded(err: Error) -> String {
    match err {
        Error::CloudConnectIo { message } => message,
        other => other.to_string(),
    }
}

/// Asks the system domain: installing and unloading is a `LaunchDaemon`
/// operation in this release, which [`preflight`] enforces.
fn job_is_absent(label: &str) -> bool {
    state_is_absent(is_active(label, ServiceScope::System).as_deref())
}

/// `true` only when launchd said, in as many words, that it has no such job.
///
/// Deliberately not "anything that is not a job launchd named": an unanswerable
/// `launchctl print` must never be read as proof the job is gone, because that
/// is exactly how a daemon survives its own removal. `None` is therefore false
/// here, and the caller fails closed on it — `unload` keeps waiting and then
/// errors rather than reporting a job it cannot see as stopped.
fn state_is_absent(state: Option<&str>) -> bool {
    state == Some(NOT_LOADED)
}

/// `launchctl print`'s report for this job in `scope`, or `None` when launchd has
/// no such job or could not be asked.
fn print_job(label: &str, scope: ServiceScope) -> Option<String> {
    let output = std::process::Command::new(LAUNCHCTL)
        .arg("print")
        .arg(service_target_in(label, scope))
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).into_owned())
}

/// Take the job out of the domain and confirm it is gone.
fn unload(label: &str) -> Result<()> {
    if job_is_absent(label) {
        return Ok(());
    }

    let target = service_target(label);
    let requested = launchctl(&["bootout", &target]);
    if let Err(ref err) = requested {
        tracing::debug!("launchctl bootout {target}: {err}");
    }

    // `bootout` returns once launchd has been told, not once the runtime has
    // stopped, and the runtime gets `ExitTimeOut` to do that. When `bootout`
    // itself failed there is nothing on the way, so only a job that was already
    // leaving is worth waiting for.
    let attempts = if requested.is_ok() {
        UNLOAD_ATTEMPTS
    } else {
        UNLOAD_GRACE_ATTEMPTS
    };
    for attempt in 0..attempts {
        if job_is_absent(label) {
            return Ok(());
        }
        if attempt + 1 < attempts {
            std::thread::sleep(UNLOAD_POLL_INTERVAL);
        }
    }

    let detail = requested
        .err()
        .map_or_else(String::new, |err| format!(": {}", folded(err)));
    Err(Error::CloudConnectIo {
        message: format!(
            "launchd is still holding the job {label} after being asked to unload it{detail}. \
             Unload it with `sudo launchctl bootout {target}`."
        ),
    })
}

/// Put the job into the domain.
fn load(label: &str, path: &Path) -> Result<()> {
    let target = service_target(label);
    // A label `launchctl disable` has touched stays disabled in launchd's own
    // database, where `bootstrap` succeeds and the job still never runs.
    if let Err(err) = launchctl(&["enable", &target]) {
        tracing::debug!("launchctl enable {target}: {err}");
    }

    let plist = path.to_string_lossy().into_owned();
    let mut last = None;
    for attempt in 0..BOOTSTRAP_ATTEMPTS {
        match launchctl(&["bootstrap", "system", &plist]) {
            Ok(()) => return Ok(()),
            Err(err) => {
                last = Some(err);
                if attempt + 1 < BOOTSTRAP_ATTEMPTS {
                    std::thread::sleep(BOOTSTRAP_RETRY_INTERVAL);
                }
            }
        }
    }

    let detail = last.map_or_else(String::new, |err| format!(": {}", folded(err)));
    Err(Error::CloudConnectIo {
        message: format!(
            "launchd would not load the Spice Cloud Connect daemon {label}{detail}. It refuses a \
             definition it cannot parse, or one that is not owned by root:wheel and writable only \
             by root — check it with `plutil -lint {plist}` and `ls -l {plist}`."
        ),
    })
}

/// Write the daemon definition as the root-owned, root-writable-only file
/// launchd requires.
///
/// launchd refuses a definition that is not owned by root or that anyone but
/// root can write, and reports the refusal only to the system log — so
/// ownership and mode are set explicitly and then read back, rather than left
/// to the process umask and to whatever the file happened to be before.
fn write_daemon_plist(path: &Path, plist: &str) -> Result<()> {
    use std::fs::OpenOptions;
    use std::io::Write as _;
    use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _};

    let io_error = |e: std::io::Error| Error::CloudConnectIo {
        message: format!(
            "write the launchd daemon definition {}: {e}",
            path.display()
        ),
    };

    super::ensure_root_only_dir(Path::new(LAUNCH_DAEMON_DIR))?;

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
                "write the launchd daemon definition {}: the path has no file name",
                path.display()
            ),
        })?;
    let staging = path.with_file_name(format!(".{file_name}.incoming"));
    let _ = std::fs::remove_file(&staging);

    // Anything short of the rename leaves the sibling behind, and a half-written
    // definition is not worth keeping.
    let written = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(PLIST_MODE)
            .open(&staging)
            .map_err(io_error)?;
        file.write_all(plist.as_bytes()).map_err(io_error)?;
        file.sync_all().map_err(io_error)?;
        drop(file);

        // `create_new` honours the process umask, which can clear bits the mode
        // asked for; set them back before launchd is pointed at the file.
        std::fs::set_permissions(&staging, std::fs::Permissions::from_mode(PLIST_MODE))
            .map_err(io_error)?;
        std::os::unix::fs::chown(&staging, Some(0), Some(0)).map_err(|e| Error::CloudConnectIo {
            message: format!(
                "give the launchd daemon definition {} to root: {e}",
                path.display()
            ),
        })
    })();
    if let Err(err) = written {
        let _ = std::fs::remove_file(&staging);
        return Err(err);
    }

    std::fs::rename(&staging, path).map_err(io_error)?;

    let meta = std::fs::metadata(path).map_err(io_error)?;
    let mode = meta.permissions().mode() & 0o7777;
    if meta.uid() != 0 || meta.gid() != 0 || mode & 0o022 != 0 {
        return Err(Error::InvalidArgument {
            message: format!(
                "Failed to install the Spice Cloud Connect service: {plist_path} is owned by \
                 uid {uid} gid {gid} with mode {mode:04o}. launchd silently refuses to load a \
                 daemon that is not owned by root:wheel and writable only by root. Fix it with \
                 `sudo chown root:wheel {plist_path}` and `sudo chmod 644 {plist_path}`, then \
                 re-run `sudo spice connect service install`.",
                plist_path = path.display(),
                uid = meta.uid(),
                gid = meta.gid(),
            ),
        });
    }

    Ok(())
}

/// Prove the staged runtime is a binary this kernel will actually execute.
///
/// launchd bootstraps a job whose program cannot be executed and then reports
/// nothing: the job exists, never runs, and the instance is silently offline.
/// Running the copy once as the same unprivileged account launchd will use
/// turns every cause of that — a quarantine attribute, a code signature the
/// kernel rejects, the wrong architecture — into an error before the job is
/// created, without executing operator-supplied code as root.
///
/// The copy has to be the subject rather than `source`, because copying is what
/// drops `com.apple.quarantine`: checking the source would condemn a runtime
/// that stages perfectly well.
fn verify_staged_runtime_executes(
    staged: &Path,
    source: &Path,
    account: super::ServiceAccount,
) -> Result<()> {
    use std::os::unix::process::CommandExt as _;

    let mut command = std::process::Command::new(staged);
    command
        .arg("--version")
        .uid(account.uid)
        .gid(account.gid)
        // The candidate is operator-supplied. Give it no installer secrets or
        // dynamic-loader overrides while probing it.
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .current_dir("/");
    let failure = match command.output() {
        Ok(output) if output.status.success() => return Ok(()),
        Ok(output) => {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            if stderr.is_empty() {
                format!("it exited with {}", output.status)
            } else {
                stderr
            }
        }
        Err(e) => e.to_string(),
    };

    // Named even though staging drops it, since it is the likeliest thing the
    // operator can act on.
    let quarantine = if has_quarantine(source) {
        format!(
            " It carries com.apple.quarantine — clear it with `sudo xattr -d com.apple.quarantine {source}`.",
            source = source.display()
        )
    } else {
        String::new()
    };

    Err(Error::InvalidArgument {
        message: format!(
            "Failed to install the Spice Cloud Connect service: a copy of the runtime at {source} \
             does not run: {failure}. launchd would load the daemon and never start it, so nothing \
             was installed and any service already on this host keeps the runtime it has.\
             {quarantine} Check the binary with `codesign --verify {source}` and \
             `spctl --assess --type execute {source}`, then re-run \
             `sudo spice connect service install`.",
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

/// Confirm launchd started the job and that it stayed up.
///
/// An accepted definition is not a started instance, and an instance that exits
/// the moment it starts is not one either — `KeepAlive` would relaunch it
/// forever while the install reported success. The first observation proves
/// launchd executed something; requiring the same process to still be there a
/// moment later proves it survived.
fn confirm_running(label: &str) -> Result<()> {
    let pid = wait_for_running(label)?;
    std::thread::sleep(SETTLE_INTERVAL);

    let settled =
        print_job(label, ServiceScope::System).and_then(|printed| top_level_field(&printed, "pid"));
    if settled.as_deref() == Some(pid.as_str()) {
        return Ok(());
    }

    Err(Error::CloudConnectIo {
        message: format!(
            "The Spice Cloud Connect daemon {label} started as pid {pid} and did not stay \
             up{report}. It is installed and launchd keeps relaunching it: find out why with \
             `sudo launchctl print {target}`, or run `spiced` in the instance \
             directory to watch it fail in the foreground. `sudo spice connect remove` stops it.",
            report = job_report(label),
            target = service_target(label),
        ),
    })
}

/// Wait for launchd to report the job running, and return the pid it is running
/// as. State and pid come from one report, so they always describe the same
/// moment.
fn wait_for_running(label: &str) -> Result<String> {
    for attempt in 0..RUNNING_ATTEMPTS {
        if let Some(printed) = print_job(label, ServiceScope::System)
            && top_level_field(&printed, "state").as_deref() == Some(RUNNING)
            && let Some(pid) = top_level_field(&printed, "pid")
        {
            return Ok(pid);
        }
        if attempt + 1 < RUNNING_ATTEMPTS {
            std::thread::sleep(RUNNING_POLL_INTERVAL);
        }
    }

    Err(Error::CloudConnectIo {
        message: format!(
            "The Spice Cloud Connect daemon {label} was installed but launchd has not started \
             it{report}. Find out why with `sudo launchctl print {target}`; a label that \
             `launchctl disable` has touched stays disabled until \
             `sudo launchctl enable {target}`. `sudo spice connect remove` undoes the install.",
            report = job_report(label),
            target = service_target(label),
        ),
    })
}

/// launchd's own account of a job that is not running as it should be.
fn job_report(label: &str) -> String {
    let Some(printed) = print_job(label, ServiceScope::System) else {
        return String::new();
    };

    let reported: Vec<String> = ["state", "runs", "last exit code"]
        .into_iter()
        .filter_map(|key| top_level_field(&printed, key).map(|value| format!("{key} = {value}")))
        .collect();

    if reported.is_empty() {
        String::new()
    } else {
        format!(" ({})", reported.join(", "))
    }
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

/// Run `launchctl <args>`, turning a non-zero exit into an error carrying
/// launchd's own words — which name the actual problem far better than an exit
/// code. `launchctl` reports some failures on stdout, so both streams are read.
fn launchctl(args: &[&str]) -> Result<()> {
    let output = std::process::Command::new(LAUNCHCTL)
        .args(args)
        .output()
        .map_err(|e| Error::CloudConnectIo {
            message: format!("run `launchctl {}`: {e}", args.join(" ")),
        })?;

    if output.status.success() {
        return Ok(());
    }

    let said = format!(
        "{} {}",
        String::from_utf8_lossy(&output.stderr).trim(),
        String::from_utf8_lossy(&output.stdout).trim()
    );
    let said = said.trim();
    Err(Error::CloudConnectIo {
        message: format!(
            "`launchctl {}` failed: {}",
            args.join(" "),
            if said.is_empty() {
                format!("exit status {}", output.status)
            } else {
                said.to_string()
            }
        ),
    })
}

/// Parse the `WorkingDirectory` value out of a rendered definition.
fn parse_working_dir(plist: &str) -> Option<PathBuf> {
    first_string_after_key(plist, "WorkingDirectory").map(PathBuf::from)
}

/// Parse the binary the job runs — the first entry of `ProgramArguments` —
/// dropping its arguments. Lets `status` report which runtime an installed
/// daemon is actually running, including one written before the runtime moved.
fn parse_program(plist: &str) -> Option<PathBuf> {
    first_string_after_key(plist, "ProgramArguments").map(PathBuf::from)
}

/// The first `<string>` following `<key>key</key>`, unescaped.
///
/// Enough for the two values read back: `WorkingDirectory` is a bare string and
/// the program is the first element of the `ProgramArguments` array. Every
/// value written is escaped, so no value can contain a key element and be
/// mistaken for one.
fn first_string_after_key(plist: &str, key: &str) -> Option<String> {
    let marker = format!("<key>{key}</key>");
    let rest = &plist[plist.find(&marker)? + marker.len()..];
    let open = rest.find("<string>")? + "<string>".len();
    let close = open + rest[open..].find("</string>")?;
    let value = xml_unescape(rest[open..close].trim());
    (!value.is_empty()).then_some(value)
}

/// Escape a value for XML element content. Paths are operator-supplied, so a
/// directory named `a&b` must not produce a definition launchd cannot parse.
fn xml_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            _ => out.push(ch),
        }
    }
    out
}

/// Reverse [`xml_escape`], plus the two entities a hand-edited definition may
/// carry. `&amp;` is undone last so `&amp;lt;` reads back as `&lt;`.
fn xml_unescape(value: &str) -> String {
    value
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_launchd_state_normalizes() {
        for (reported, expected) in [
            ("running", ServiceState::Running),
            ("spawn scheduled", ServiceState::Starting),
            ("exiting", ServiceState::Stopping),
            ("waiting", ServiceState::Stopped),
            ("not running", ServiceState::Stopped),
            ("error", ServiceState::Failed),
            ("not loaded", ServiceState::Unavailable),
            ("", ServiceState::Unavailable),
            ("something new", ServiceState::Unavailable),
        ] {
            assert_eq!(
                normalize_launchd_state(reported),
                expected,
                "launchd `{reported}`"
            );
        }
        // `launchctl print` values arrive with surrounding whitespace.
        assert_eq!(normalize_launchd_state(" running "), ServiceState::Running);
    }

    const TEST_USER: &str = "spice-operator";
    const TEST_GROUP: &str = "spice-users";

    fn rendered(dir: &str) -> (String, String) {
        let label = job_label_for_dir(Path::new(dir));
        let config_dir = Path::new(dir).join(".spice");
        let plist = render_plist(
            &label,
            Path::new(dir),
            &config_dir,
            &super::super::staged_runtime_path(),
            TEST_USER,
            TEST_GROUP,
        );
        (label, plist)
    }

    #[test]
    fn label_is_reverse_dns_and_legible() {
        let label = job_label_for_dir(Path::new("/opt/edge-1"));
        assert!(
            label.starts_with("ai.spice.cloud-connect.edge-1-"),
            "{label}"
        );
    }

    #[test]
    fn two_directories_yield_two_independent_jobs() {
        // The acceptance criterion: two instance directories install two jobs,
        // stored as two files, so removing one cannot disturb the other.
        let a = job_label_for_dir(Path::new("/opt/edge-1"));
        let b = job_label_for_dir(Path::new("/opt/edge-2"));
        assert_ne!(a, b);
        assert_ne!(
            plist_path(&a, ServiceScope::System),
            plist_path(&b, ServiceScope::System)
        );
    }

    #[test]
    fn same_basename_in_different_parents_does_not_collide() {
        let a = job_label_for_dir(Path::new("/srv/a/edge"));
        let b = job_label_for_dir(Path::new("/srv/b/edge"));
        assert_ne!(a, b);
        assert!(a.starts_with("ai.spice.cloud-connect.edge-"));
        assert!(b.starts_with("ai.spice.cloud-connect.edge-"));
    }

    #[test]
    fn label_is_deterministic_for_the_same_directory() {
        // Re-running `--install` must land on the same job rather than adding a
        // second one.
        let dir = Path::new("/opt/edge-1");
        assert_eq!(job_label_for_dir(dir), job_label_for_dir(dir));
    }

    #[test]
    fn plist_path_lives_in_the_daemon_directory() {
        let label = "ai.spice.cloud-connect.edge-1-1a2b3c4d";
        let path = plist_path(label, ServiceScope::System);
        assert_eq!(
            path,
            PathBuf::from("/Library/LaunchDaemons/ai.spice.cloud-connect.edge-1-1a2b3c4d.plist")
        );
        // The label carries dots of its own, so recovering it from a file name
        // means dropping the suffix rather than splitting on the last dot.
        assert_eq!(
            path.file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| name.strip_suffix(PLIST_SUFFIX)),
            Some(label)
        );
    }

    #[test]
    fn rendered_plist_is_a_daemon_that_always_restarts() {
        let (label, plist) = rendered("/opt/edge-1");
        assert!(plist.contains(&format!("<string>{label}</string>")));
        // KeepAlive is the Restart=always analogue: what brings an enrolled
        // instance back from a reboot or an unhandled failure.
        assert!(plist.contains("<key>KeepAlive</key>\n\t<true/>"), "{plist}");
        assert!(plist.contains("<key>RunAtLoad</key>\n\t<true/>"), "{plist}");
        // Background would have macOS throttle the runtime's I/O.
        assert!(plist.contains("<key>ProcessType</key>\n\t<string>Standard</string>"));
    }

    #[test]
    fn rendered_plist_bakes_the_absolute_working_directory() {
        let (_, plist) = rendered("/opt/edge-1");
        assert_eq!(
            parse_working_dir(&plist),
            Some(PathBuf::from("/opt/edge-1")),
            "the working directory must round-trip so status can discover it"
        );
    }

    #[test]
    fn rendered_plist_preserves_the_resolved_config_directory() {
        let label = job_label_for_dir(Path::new("/opt/edge-1"));
        let plist = render_plist(
            &label,
            Path::new("/opt/edge-1"),
            Path::new("/var/lib/spice/custom-config"),
            Path::new("/usr/bin/spiced"),
            TEST_USER,
            TEST_GROUP,
        );
        assert!(plist.contains(
            "<key>SPICE_CONFIG_DIR</key>\n\t\t<string>/var/lib/spice/custom-config</string>"
        ));
    }

    #[test]
    fn rendered_plist_runs_as_the_non_root_operator() {
        let (_, plist) = rendered("/opt/edge-1");
        assert!(plist.contains("<key>UserName</key>\n\t<string>spice-operator</string>"));
        assert!(plist.contains("<key>GroupName</key>\n\t<string>spice-users</string>"));
    }

    #[test]
    fn rendered_plist_runs_the_staged_runtime_from_the_enrolled_directory() {
        let (_, plist) = rendered("/opt/edge-1");
        // The daemon is unprivileged, but the program remains the staged
        // root-owned copy, never the operator's replaceable runtime.
        assert_eq!(
            parse_program(&plist),
            Some(super::super::staged_runtime_path())
        );
        // No flag: the enrolled identity under SPICE_CONFIG_DIR is what
        // activates Cloud Connect on every start.
        assert!(!plist.contains("--cloud-connect"));
    }

    #[test]
    fn rendered_plist_escapes_a_directory_that_would_break_the_xml() {
        let dir = Path::new("/opt/a&b<c>");
        let plist = render_plist(
            "ai.spice.cloud-connect.x",
            dir,
            &dir.join(".spice"),
            Path::new("/usr/bin/spiced"),
            TEST_USER,
            TEST_GROUP,
        );
        assert!(
            plist.contains("<string>/opt/a&amp;b&lt;c&gt;</string>"),
            "{plist}"
        );
        assert_eq!(parse_working_dir(&plist), Some(dir.to_path_buf()));
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
    }

    #[test]
    fn parsing_a_program_takes_the_binary_and_not_its_arguments() {
        let plist = "<key>ProgramArguments</key>\n<array>\n<string>/usr/bin/spiced</string>\n\
                     <string>--metrics</string>\n</array>";
        assert_eq!(parse_program(plist), Some(PathBuf::from("/usr/bin/spiced")));
    }

    #[test]
    fn xml_round_trips_the_characters_that_would_break_a_definition() {
        for value in ["/opt/a&b", "/opt/<x>", "/opt/plain", "/opt/a&amp;b"] {
            assert_eq!(xml_unescape(&xml_escape(value)), value, "{value}");
        }
    }

    #[test]
    fn top_level_field_reads_the_job_and_not_its_endpoints() {
        // `launchctl print` repeats field names for a job's endpoints at a
        // deeper indent; reading one of those as the job's state would report a
        // dead instance as active. The nested block leads here, so matching on
        // the indent rather than on which line comes first is what answers.
        let printed = "system/ai.spice.x = {\n\tendpoints = {\n\t\tstate = active\n\t}\n\
                       \tstate = not running\n\tlast exit code = 78\n}\n";
        assert_eq!(
            top_level_field(printed, "state"),
            Some("not running".to_string())
        );
        assert_eq!(
            top_level_field(printed, "last exit code"),
            Some("78".to_string())
        );
        assert_eq!(top_level_field(printed, "pid"), None);
    }

    /// A manifest for the job under test, without touching the filesystem.
    fn manifest(scope: ServiceScope) -> ServiceManifest {
        let name = job_label_for_dir(Path::new("/opt/edge-1"));
        ServiceManifest {
            schema_version: super::super::manifest::MANIFEST_SCHEMA_VERSION,
            directory: PathBuf::from("/opt/edge-1"),
            name: name.clone(),
            scope,
            supervisor: Supervisor::Launchd,
            owner: super::super::ServiceOwner {
                uid: 501,
                gid: 20,
                name: Some("alice".to_string()),
            },
            definition_path: plist_path(&name, scope),
            runtime_path: super::super::staged_runtime_path(),
            log_source: None,
            runtime_digest: String::new(),
            runtime_version: "v2.2.0".to_string(),
            health_url: "http://127.0.0.1:8090/health".to_string(),
        }
    }

    #[test]
    fn recovery_hints_name_the_job_in_its_own_domain() {
        let hints = recovery_hints(&manifest(ServiceScope::System));
        assert!(
            hints[0]
                .ends_with("launchctl print system/ai.spice.cloud-connect.edge-1-2962fca679ae993a"),
            "{hints:?}"
        );
        assert!(hints[1].contains("kickstart -k system/"), "{hints:?}");

        // An agent lives in its owner's GUI domain, and naming the system
        // domain would report on a job that is not the one installed.
        let user = recovery_hints(&manifest(ServiceScope::User));
        assert!(user[0].contains("launchctl print gui/"), "{user:?}");
        assert!(
            !user.iter().any(|hint| hint.starts_with("sudo")),
            "an agent is managed by its owner, never through sudo: {user:?}"
        );
    }

    #[test]
    fn an_agent_definition_lives_under_the_owners_home() {
        let label = job_label_for_dir(Path::new("/opt/edge-1"));
        let daemon = plist_path(&label, ServiceScope::System);
        let agent = plist_path(&label, ServiceScope::User);
        assert!(daemon.starts_with(LAUNCH_DAEMON_DIR), "{daemon:?}");
        assert_ne!(daemon, agent);
        assert!(
            agent.to_string_lossy().contains(LAUNCH_AGENT_SUBDIR),
            "{agent:?}"
        );
    }

    #[test]
    fn the_disabled_database_decides_boot_persistence() {
        // launchd keeps this list separately from the definition, so a daemon
        // that is installed and correct can still be held down by
        // `launchctl disable` — and must not be reported as starting at boot.
        let printed = "\
\"com.apple.something\" => false
\"ai.spice.cloud-connect.edge-1-2962fca679ae993a\" => true
\"com.apple.other\" => disabled
";
        assert!(parse_disabled(
            printed,
            "ai.spice.cloud-connect.edge-1-2962fca679ae993a"
        ));
        assert!(
            parse_disabled(printed, "com.apple.other"),
            "the `disabled` spelling counts too"
        );
        assert!(!parse_disabled(printed, "com.apple.something"));
        assert!(
            !parse_disabled(printed, "ai.spice.cloud-connect.absent"),
            "a label the list does not mention is not disabled"
        );
        // A substring of another label must not match.
        assert!(!parse_disabled(
            "\"ai.spice.cloud-connect.edge-1-2962fca679ae993a.extra\" => true\n",
            "ai.spice.cloud-connect.edge-1-2962fca679ae993a"
        ));
    }

    #[test]
    fn an_unrecognized_launchd_state_explains_itself() {
        // The model promises every `unavailable` state says why, and `not
        // loaded` is the answer that matters: launchd has no such job while the
        // definition is on disk.
        let manifest = manifest(ServiceScope::System);
        let diagnostic = unrecognized_state_diagnostic(NOT_LOADED, &manifest);
        assert!(diagnostic.contains(NOT_LOADED), "{diagnostic}");
        assert!(
            diagnostic.contains("launchctl print system/"),
            "{diagnostic}"
        );
        assert_eq!(
            normalize_launchd_state(NOT_LOADED),
            ServiceState::Unavailable,
            "the diagnostic only reaches the report for an unavailable state"
        );
    }

    #[test]
    fn adoption_needs_a_stated_working_directory_that_matches() {
        // The label is derived from the directory, so any job created under it
        // carries the same one. Only a source that knows what the job runs — the
        // definition, or launchd's own report — may settle the question, and
        // neither available must resolve nothing rather than adopt a foreign
        // job under a label this directory happens to derive.
        let dir = Path::new("/opt/edge-1");
        let plist = render_plist(
            &job_label_for_dir(dir),
            dir,
            &dir.join(".spice"),
            Path::new("/usr/local/lib/spice/spiced"),
            "alice",
            "staff",
        );
        assert_eq!(parse_working_dir(&plist), Some(dir.to_path_buf()));

        // A definition naming somewhere else settles it on its own; launchd is
        // never consulted to overrule what the definition says.
        let foreign = render_plist(
            &job_label_for_dir(dir),
            Path::new("/opt/somewhere-else"),
            Path::new("/opt/somewhere-else/.spice"),
            Path::new("/usr/local/lib/spice/spiced"),
            "alice",
            "staff",
        );
        assert_eq!(
            parse_working_dir(&foreign),
            Some(PathBuf::from("/opt/somewhere-else"))
        );

        // A definition that states no working directory leaves launchd as the
        // only source that can answer.
        assert_eq!(parse_working_dir("<plist><dict></dict></plist>"), None);
    }

    #[test]
    fn launchds_own_report_states_the_working_directory() {
        // The independent statement that lets an orphaned job — one whose
        // definition is already deleted — still be adopted and removed.
        let printed =
            "system/ai.spice.x = {\n\tstate = running\n\tworking directory = /opt/edge-1\n}\n";
        assert_eq!(
            top_level_field(printed, "working directory"),
            Some("/opt/edge-1".to_string())
        );
        // Older releases spell it `cwd`, and both are accepted.
        let older = "system/ai.spice.x = {\n\tcwd = /opt/edge-1\n}\n";
        assert_eq!(
            ["working directory", "cwd"]
                .into_iter()
                .find_map(|key| top_level_field(older, key)),
            Some("/opt/edge-1".to_string())
        );
        // A report that names no directory cannot verify anything.
        let silent = "system/ai.spice.x = {\n\tstate = running\n}\n";
        assert_eq!(
            ["working directory", "cwd"]
                .into_iter()
                .find_map(|key| top_level_field(silent, key)),
            None
        );
    }

    #[test]
    fn a_definition_naming_no_output_paths_reports_no_log_source() {
        // The plist this release writes configures none, so claiming a file
        // would name something nothing writes to.
        assert_eq!(
            plist_log_source("ai.spice.cloud-connect.absent", ServiceScope::System),
            None
        );
    }

    #[test]
    fn a_user_scope_install_is_refused_rather_than_half_performed() {
        assert!(matches!(
            preflight(ServiceScope::User),
            Err(PreflightFailure::UserScopePending)
        ));
        preflight(ServiceScope::System).expect("a system daemon is what this release installs");
    }

    #[test]
    fn every_lifecycle_action_reports_itself_as_pending_rather_than_panicking() {
        let manifest = manifest(ServiceScope::System);
        for result in [
            LaunchdBackend.start(&manifest),
            LaunchdBackend.stop(&manifest),
            LaunchdBackend.restart(&manifest),
            LaunchdBackend.logs(
                &manifest,
                super::super::LogRequest {
                    number: 100,
                    follow: false,
                },
            ),
        ] {
            let error = result.expect_err("the lifecycle is not implemented yet");
            assert!(matches!(error, Error::NotImplemented { .. }), "{error}");
            assert!(error.to_string().contains("launchctl"), "{error}");
        }
    }

    #[test]
    fn an_unanswerable_launchd_is_never_read_as_a_job_that_is_gone() {
        // This decides whether `unload` may stop waiting, so `None` — launchd
        // could not be asked — has to be false. Reading it as "gone" is how a
        // daemon survives its own removal.
        assert!(!state_is_absent(None));
        assert!(state_is_absent(Some(NOT_LOADED)));
        for state in [RUNNING, "waiting", "not running"] {
            assert!(!state_is_absent(Some(state)), "{state}");
        }
    }
    #[test]
    fn a_job_launchd_does_not_have_is_reported_as_such() {
        // The one end of the mapping a real launchd can answer here.
        let answer = is_active(
            "ai.spice.cloud-connect.definitely-not-installed-0000",
            ServiceScope::System,
        );
        assert!(
            answer.is_none() || answer.as_deref() == Some(NOT_LOADED),
            "unexpected state for a job that does not exist: {answer:?}"
        );
    }
}
