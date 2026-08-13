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

use super::backend::{
    InstallRequest, LogRequest, ServiceBackend, ServiceObservation, lifecycle_pending,
};
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
/// spicepod from the directory the operator enrolled, not from wherever
/// systemd happens to start it. `config_dir` preserves the resolved Spice state
/// directory and `spiced_path` is the absolute binary path resolved at install
/// time.
fn render_unit(
    instance_dir: &Path,
    config_dir: &Path,
    spiced_path: &Path,
    account: ServiceAccount,
) -> Result<String> {
    let instance_dir = escape_systemd_path(instance_dir)?;
    let config_dir = escape_systemd_path(config_dir)?;
    let spiced = escape_systemd_path(spiced_path)?;
    Ok(format!(
        "[Unit]\n\
         Description=Spice runtime connected to Spice Cloud\n\
         Documentation=https://spiceai.org/docs\n\
         After=network-online.target\n\
         Wants=network-online.target\n\
         \n\
         [Service]\n\
         Type=simple\n\
         User={uid}\n\
         Group={gid}\n\
         WorkingDirectory=\"{instance_dir}\"\n\
         Environment=\"SPICE_CONFIG_DIR={config_dir}\"\n\
         ExecStart=\"{spiced}\"\n\
         # A deployment applies to the running instance, so this is not what\n\
         # makes one land: it is what brings the instance back from the things\n\
         # that do end it — a reboot, an OOM kill, an unhandled failure.\n\
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
        uid = account.uid,
        gid = account.gid,
    ))
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

fn preflight(scope: ServiceScope) -> std::result::Result<(), PreflightFailure> {
    if !Path::new(SYSTEMD_RUNTIME_MARKER).is_dir() {
        return Err(PreflightFailure::SystemdUnavailable);
    }
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
    super::provision_config_ownership(config_dir, account)?;

    // systemd reports a unit whose `ExecStart` will not run, so the staged copy
    // needs no separate check.
    let staged_runtime = super::stage_runtime(spiced_path, |_, _| Ok(()))?;

    let name = unit_name_for_dir(instance_dir);
    let dir = unit_dir(scope).ok_or_else(|| Error::CloudConnectIo {
        message: "locate the systemd unit directory for this account".to_string(),
    })?;
    let path = dir.join(&name);
    let unit = render_unit(instance_dir, config_dir, &staged_runtime, account)?;

    std::fs::write(&path, unit).map_err(|e| Error::CloudConnectIo {
        message: format!(
            "write systemd unit {}: {e}. The identity is staged at {} — fix the problem \
             and re-run `spice connect service install` to finish.",
            path.display(),
            config_dir.display()
        ),
    })?;

    systemctl(scope, &["daemon-reload"])?;
    // `enable --now` starts the service and persists the boot-time link in one
    // step. On a reinstall the unit is already enabled and already running, so
    // follow with an explicit restart to pick up the rewritten unit and the
    // upgraded binary — `enable --now` alone would leave the old process up.
    systemctl(scope, &["enable", "--now", &name])?;
    systemctl(scope, &["restart", &name])?;

    Ok(InstalledService {
        name,
        path,
        working_dir: instance_dir.to_path_buf(),
        runtime: staged_runtime,
    })
}

/// Stop, disable, and delete the unit the manifest describes.
///
/// Stop/disable failures are tolerated — a unit file left on disk would restart
/// a service against a released identity forever, so the deletion is what must
/// happen.
fn uninstall(manifest: &ServiceManifest) -> Result<()> {
    // Best-effort: a unit that is already stopped, already disabled, or whose
    // systemd is not running must not block removing the file.
    if let Err(err) = systemctl(manifest.scope, &["disable", "--now", &manifest.name]) {
        tracing::debug!("systemctl disable --now {}: {err}", manifest.name);
    }

    match std::fs::remove_file(&manifest.definition_path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(Error::CloudConnectIo {
                message: format!(
                    "remove systemd unit {}: {e}. The service would keep restarting against a \
                     released identity — delete the file and run `sudo systemctl daemon-reload`.",
                    manifest.definition_path.display()
                ),
            });
        }
    }

    if let Err(err) = systemctl(manifest.scope, &["daemon-reload"]) {
        tracing::debug!("systemctl daemon-reload: {err}");
    }

    Ok(())
}

/// The unit installed for `instance_dir` in `scope`, if the definition under
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
    let runtime = parse_exec_runtime(&unit).unwrap_or_else(super::staged_runtime_path);
    Some(InstalledService {
        name,
        path,
        working_dir: instance_dir.to_path_buf(),
        runtime,
    })
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
fn is_active(unit_name: &str, scope: ServiceScope) -> Option<String> {
    // `is-active` exits non-zero for anything but `active`, and prints the
    // state either way — so read stdout regardless of the exit status.
    let state = systemctl_query(scope, &["is-active", unit_name])?;
    (!state.is_empty()).then_some(state)
}

/// Observe the unit: the state systemd reports plus whether it will come back
/// on its own.
fn observe(manifest: &ServiceManifest) -> ServiceObservation {
    let Some(reported) = is_active(&manifest.name, manifest.scope) else {
        return ServiceObservation::unavailable(format!(
            "systemctl could not be asked about {}. Check that systemd is running and that this \
             account may query it ({}).",
            manifest.name,
            systemctl_command(manifest.scope, &["is-active", &manifest.name])
        ));
    };
    let (starts, starts_action) = observe_persistence(manifest);
    ServiceObservation {
        state: normalize_systemd_state(&reported),
        starts,
        diagnostic: None,
        starts_action,
    }
}

/// Whether the unit is enabled, translated into the operator outcome.
///
/// A system unit that is enabled comes up at boot with nobody logged in. A
/// *user* unit that is enabled comes up when its owner logs in and no earlier
/// unless that account lingers, so the conservative answer is `login_only`
/// plus the command that changes it — claiming boot persistence that is not
/// there is the failure this avoids.
fn observe_persistence(manifest: &ServiceManifest) -> (ServiceStarts, Option<String>) {
    let Some(enabled) = systemctl_query(manifest.scope, &["is-enabled", &manifest.name]) else {
        return (ServiceStarts::Unavailable, None);
    };
    let enabled = matches!(
        enabled.as_str(),
        "enabled" | "enabled-runtime" | "static" | "alias" | "indirect" | "generated"
    );
    if !enabled {
        return (
            ServiceStarts::Disabled,
            Some(systemctl_command(
                manifest.scope,
                &["enable", &manifest.name],
            )),
        );
    }
    match manifest.scope {
        ServiceScope::System => (ServiceStarts::BootWithoutLogin, None),
        ServiceScope::User => (
            ServiceStarts::LoginOnly,
            Some(format!(
                "loginctl enable-linger {}",
                manifest.owner.describe()
            )),
        ),
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
    parts.push("systemctl");
    parts.extend(scope_args(scope));
    parts.extend(args);
    parts.join(" ")
}

/// Ask `systemctl` a question and return its trimmed stdout.
///
/// The exit status is ignored on purpose: the query commands report their
/// answer on stdout and use the exit status to encode that answer. `None`
/// means `systemctl` could not be run at all.
fn systemctl_query(scope: ServiceScope, args: &[&str]) -> Option<String> {
    let output = std::process::Command::new("systemctl")
        .args(scope_args(scope))
        .args(args)
        .output()
        .ok()?;
    Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Run `systemctl <args>`, turning a non-zero exit into an error carrying
/// systemd's own stderr — which names the actual problem far better than an
/// exit code.
fn systemctl(scope: ServiceScope, args: &[&str]) -> Result<()> {
    let output = std::process::Command::new("systemctl")
        .args(scope_args(scope))
        .args(args)
        .output()
        .map_err(|e| Error::CloudConnectIo {
            message: format!("run `{}`: {e}", systemctl_command(scope, args)),
        })?;

    if output.status.success() {
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    Err(Error::CloudConnectIo {
        message: format!(
            "`{}` failed: {}",
            systemctl_command(scope, args),
            if stderr.is_empty() {
                format!("exit status {}", output.status)
            } else {
                stderr
            }
        ),
    })
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

#[cfg(test)]
mod tests {
    use super::*;

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

    const TEST_ACCOUNT: ServiceAccount = ServiceAccount {
        uid: 1000,
        gid: 1001,
    };

    fn rendered(instance_dir: &str, config_dir: &str, spiced_path: &str) -> String {
        render_unit(
            Path::new(instance_dir),
            Path::new(config_dir),
            Path::new(spiced_path),
            TEST_ACCOUNT,
        )
        .expect("test paths are safe systemd values")
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
            "/root/.spice/bin/spiced",
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
            "/root/.spice/bin/spiced",
        );
        // No flag: the enrolled identity under SPICE_CONFIG_DIR is what
        // activates Cloud Connect on every start.
        assert!(unit.contains("ExecStart=\"/root/.spice/bin/spiced\"\n"));
        assert!(!unit.contains("--cloud-connect"));
    }

    #[test]
    fn rendered_unit_always_restarts() {
        // `Restart=always` is what keeps an enrolled instance reachable: a
        // deployment applies without ending the process, but a reboot or an
        // unhandled failure does end it, and the instance has to come back.
        let unit = rendered("/opt/edge-1", "/opt/edge-1/.spice", "/usr/bin/spiced");
        assert!(unit.contains("\nRestart=always\n"));
        // A rate limit would let a crash loop give up permanently.
        assert!(unit.contains("StartLimitIntervalSec=0"));
        assert!(unit.contains("WantedBy=multi-user.target"));
    }

    #[test]
    fn rendered_unit_runs_the_root_owned_staged_runtime() {
        // ExecStart stays the staged root-owned copy — never the operator's
        // replaceable `~/.spice/bin/spiced`.
        let staged = super::super::staged_runtime_path();
        let unit = render_unit(
            Path::new("/opt/edge-1"),
            Path::new("/opt/edge-1/.spice"),
            &staged,
            TEST_ACCOUNT,
        )
        .expect("render unit");
        assert_eq!(parse_exec_runtime(&unit), Some(staged));
    }

    #[test]
    fn rendered_unit_runs_as_the_non_root_operator() {
        let unit = rendered("/opt/edge-1", "/opt/edge-1/.spice", "/usr/bin/spiced");
        assert!(unit.contains("User=1000\n"));
        assert!(unit.contains("Group=1001\n"));
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
            TEST_ACCOUNT,
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
            TEST_ACCOUNT,
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

    /// A manifest for the unit under test, without touching the filesystem.
    fn manifest(scope: ServiceScope) -> ServiceManifest {
        let name = unit_name_for_dir(Path::new("/opt/edge-1"));
        ServiceManifest {
            schema_version: super::super::manifest::MANIFEST_SCHEMA_VERSION,
            directory: PathBuf::from("/opt/edge-1"),
            name: name.clone(),
            scope,
            supervisor: Supervisor::Systemd,
            owner: super::super::ServiceOwner {
                uid: 1000,
                gid: 1000,
                name: Some("alice".to_string()),
            },
            definition_path: SystemdBackend.definition_path(&name, scope),
            runtime_path: super::super::staged_runtime_path(),
            log_source: SystemdBackend.log_source(&name, scope),
            runtime_digest: String::new(),
            runtime_version: "v2.2.0".to_string(),
            health_url: "http://127.0.0.1:8090/health".to_string(),
        }
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
    fn a_user_scope_install_is_refused_rather_than_half_performed() {
        // The systemd user lifecycle is separate work; asking for it must name
        // the path that does work instead of writing a unit nothing manages.
        assert!(matches!(
            preflight(ServiceScope::User),
            Err(PreflightFailure::UserScopePending | PreflightFailure::SystemdUnavailable)
        ));
    }

    #[test]
    fn every_lifecycle_action_reports_itself_as_pending_rather_than_panicking() {
        let manifest = manifest(ServiceScope::System);
        for result in [
            SystemdBackend.start(&manifest),
            SystemdBackend.stop(&manifest),
            SystemdBackend.restart(&manifest),
            SystemdBackend.logs(
                &manifest,
                super::super::LogRequest {
                    number: 100,
                    follow: false,
                },
            ),
        ] {
            let error = result.expect_err("the lifecycle is not implemented yet");
            assert!(matches!(error, Error::NotImplemented { .. }), "{error}");
            // The refusal has to leave the operator with something they can run.
            assert!(error.to_string().contains("systemctl"), "{error}");
        }
    }
}
