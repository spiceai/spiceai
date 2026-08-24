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

//! The normalized service vocabulary.
//!
//! One state vocabulary is shared by every backend and `spice cloud status`:
//! the backends translate their supervisor's words into these types once, and
//! everything above them — human rendering, JSON, exit codes — reads only
//! these types.
//!
//! The per-supervisor translation tables live with the back ends that own
//! those words (`normalize_systemd_state`, `normalize_launchd_state`), so a
//! supervisor's vocabulary never leaks into this one.
//!
//! There is deliberately no `unknown` state. A supervisor that cannot be
//! asked is [`ServiceState::Unavailable`] with a diagnostic, which is a
//! statement about the *query* rather than a second name for "I don't know"
//! that automation would have to reconcile with the first.

use std::fmt;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// The normalized lifecycle state of the service for one instance directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ServiceState {
    /// No service is installed for this instance directory. A foreground
    /// runtime is also `not_installed`: nothing here supervises it.
    NotInstalled,
    /// The supervisor has been asked to bring the service up and it has not
    /// finished doing so.
    Starting,
    /// The service is up.
    Running,
    /// The supervisor has been asked to take the service down and it has not
    /// finished doing so.
    Stopping,
    /// The service is installed and down.
    Stopped,
    /// The service is installed and the supervisor reports it as failed.
    Failed,
    /// The supervisor could not be asked, so the state is not knowable right
    /// now. Always accompanied by a diagnostic.
    Unavailable,
}

impl ServiceState {
    /// The stable machine-readable spelling, identical to the JSON encoding.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::NotInstalled => "not_installed",
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
            Self::Failed => "failed",
            Self::Unavailable => "unavailable",
        }
    }

    /// Whether this state means the command that reported it should exit
    /// non-zero. A service that is merely down is a fact, not a failure; a
    /// state that could not be determined, or one the supervisor calls
    /// failed, is.
    pub(crate) fn is_degraded(self) -> bool {
        matches!(self, Self::Failed | Self::Unavailable)
    }
}

impl fmt::Display for ServiceState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Which service domain the service is installed into.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ServiceScope {
    /// Installed for one operator account and managed by that account.
    User,
    /// Installed host-wide and managed with elevated privileges.
    System,
}

impl ServiceScope {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::System => "system",
        }
    }
}

impl fmt::Display for ServiceScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The supervisor that owns the service definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Supervisor {
    Systemd,
    Launchd,
}

impl Supervisor {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Systemd => "systemd",
            Self::Launchd => "launchd",
        }
    }
}

impl fmt::Display for Supervisor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Boot persistence as an operator outcome rather than as supervisor jargon.
///
/// The question an operator is actually asking is "will this instance be
/// serving after a reboot", so that is what the vocabulary answers. The word
/// "lingering" is never the answer; it appears only inside the remediation
/// command a Linux user service needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ServiceStarts {
    /// Comes up when the host boots, with nobody logged in.
    BootWithoutLogin,
    /// Comes up when its owner logs in, and not before.
    LoginOnly,
    /// Installed, but the supervisor will not start it on its own.
    Disabled,
    /// The supervisor could not be asked.
    Unavailable,
}

impl ServiceStarts {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::BootWithoutLogin => "boot_without_login",
            Self::LoginOnly => "login_only",
            Self::Disabled => "disabled",
            Self::Unavailable => "unavailable",
        }
    }

    /// The plain-language answer for human output.
    pub(crate) fn describe(self) -> &'static str {
        match self {
            Self::BootWithoutLogin => "at boot, without login",
            Self::LoginOnly => "at login only",
            Self::Disabled => "not started automatically",
            Self::Unavailable => "could not be determined",
        }
    }
}

impl fmt::Display for ServiceStarts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Where the supervisor keeps this service's output.
///
/// Modelled rather than flattened to a string because the two supervisors
/// answer differently in kind: systemd owns the journal and is queried by
/// unit, while launchd writes to files the definition names. The local
/// fallback for `spice cloud logs` needs to know which of those it is looking
/// at.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum LogSource {
    /// The systemd journal, selected by unit.
    Journal { unit: String, scope: ServiceScope },
    /// Explicit stdout/stderr files named by a launchd definition.
    Files { stdout: PathBuf, stderr: PathBuf },
}

impl LogSource {
    /// The single line human status prints for this source.
    pub(crate) fn describe(&self) -> String {
        match self {
            Self::Journal { unit, scope } => match scope {
                ServiceScope::User => format!("systemd journal (user), unit {unit}"),
                ServiceScope::System => format!("systemd journal, unit {unit}"),
            },
            Self::Files { stdout, stderr } => {
                if stdout == stderr {
                    format!("{}", stdout.display())
                } else {
                    format!("{}, {}", stdout.display(), stderr.display())
                }
            }
        }
    }
}

/// The service half of [`super::super::status::ConnectStatus`].
///
/// `spice cloud status` embeds this exact value. Field order here *is* the JSON
/// field order — keep additions at the end so a golden fixture diff stays
/// readable.
///
/// This is a public automation surface, versioned by
/// [`super::super::status::STATUS_SCHEMA_VERSION`]: adding a field is
/// compatible, but renaming one, removing one, or adding an enum variant that
/// consumers must branch on is not.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ServiceStatus {
    /// Whether a service definition for this instance directory exists.
    pub(crate) installed: bool,
    /// The normalized lifecycle state.
    pub(crate) state: ServiceState,
    /// `null` when nothing is installed.
    pub(crate) scope: Option<ServiceScope>,
    /// `null` when nothing is installed.
    pub(crate) supervisor: Option<Supervisor>,
    /// Boot persistence as an operator outcome.
    pub(crate) starts: ServiceStarts,
    /// The account the runtime runs as.
    pub(crate) owner: Option<String>,
    /// The name the supervisor knows the service by.
    pub(crate) name: Option<String>,
    /// The instance directory baked into the definition.
    pub(crate) working_dir: Option<PathBuf>,
    /// The unit file or plist.
    pub(crate) definition_path: Option<PathBuf>,
    /// The `spiced` binary the service runs.
    pub(crate) runtime_path: Option<PathBuf>,
    /// Where to read this service's output.
    pub(crate) log_source: Option<LogSource>,
    /// Why the state is `unavailable`, or what is wrong with an installation
    /// that could be read but not trusted. `null` when there is nothing to
    /// report.
    pub(crate) diagnostic: Option<String>,
    /// The command that would give this service the boot persistence it does
    /// not have. `null` when the persistence is already what it should be, or
    /// when nothing is installed.
    pub(crate) starts_action: Option<String>,
}

impl ServiceStatus {
    /// The status of an instance directory with no service.
    pub(crate) fn not_installed() -> Self {
        Self {
            installed: false,
            state: ServiceState::NotInstalled,
            scope: None,
            supervisor: None,
            starts: ServiceStarts::Disabled,
            owner: None,
            name: None,
            working_dir: None,
            definition_path: None,
            runtime_path: None,
            log_source: None,
            diagnostic: None,
            starts_action: None,
        }
    }

    /// The status of an instance directory whose service could be found but
    /// not trusted — a manifest that fails validation, for instance. Reported
    /// rather than swallowed: acting on it could control the wrong process.
    pub(crate) fn unavailable(diagnostic: String) -> Self {
        Self {
            installed: true,
            state: ServiceState::Unavailable,
            starts: ServiceStarts::Unavailable,
            diagnostic: Some(diagnostic),
            ..Self::not_installed()
        }
    }

    /// Status collection itself failed before it could establish whether a
    /// service definition exists. This is deliberately distinct from
    /// [`Self::unavailable`], which has already found service state and must
    /// not tell automation to install over it.
    pub(crate) fn inspection_unavailable(diagnostic: String) -> Self {
        Self {
            state: ServiceState::Unavailable,
            starts: ServiceStarts::Unavailable,
            diagnostic: Some(diagnostic),
            ..Self::not_installed()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn there_is_no_unknown_state_spelling() {
        // The contract has one vocabulary for "cannot be determined". A second
        // one would force automation to reconcile two names for one condition.
        for state in [
            ServiceState::NotInstalled,
            ServiceState::Starting,
            ServiceState::Running,
            ServiceState::Stopping,
            ServiceState::Stopped,
            ServiceState::Failed,
            ServiceState::Unavailable,
        ] {
            assert_ne!(state.as_str(), "unknown");
        }
    }

    #[test]
    fn found_but_untrusted_state_remains_installed() {
        let found = ServiceStatus::unavailable("manifest is untrusted".to_string());
        assert!(found.installed);
        assert_eq!(found.state, ServiceState::Unavailable);

        let uninspected =
            ServiceStatus::inspection_unavailable("inspection task failed".to_string());
        assert!(!uninspected.installed);
        assert_eq!(uninspected.state, ServiceState::Unavailable);
    }

    #[test]
    fn only_failed_and_unavailable_are_degraded() {
        assert!(ServiceState::Failed.is_degraded());
        assert!(ServiceState::Unavailable.is_degraded());
        for state in [
            ServiceState::NotInstalled,
            ServiceState::Starting,
            ServiceState::Running,
            ServiceState::Stopping,
            ServiceState::Stopped,
        ] {
            assert!(!state.is_degraded(), "{state}");
        }
    }

    #[test]
    fn boot_persistence_never_says_lingering() {
        for starts in [
            ServiceStarts::BootWithoutLogin,
            ServiceStarts::LoginOnly,
            ServiceStarts::Disabled,
            ServiceStarts::Unavailable,
        ] {
            assert!(!starts.describe().contains("linger"), "{starts}");
        }
    }

    #[test]
    fn json_spellings_match_the_machine_readable_strings() {
        // The two are used interchangeably — `as_str` in human output and
        // errors, serde in JSON — so a divergence would publish two names.
        for state in [
            ServiceState::NotInstalled,
            ServiceState::Starting,
            ServiceState::Running,
            ServiceState::Stopping,
            ServiceState::Stopped,
            ServiceState::Failed,
            ServiceState::Unavailable,
        ] {
            let json = serde_json::to_string(&state).expect("serialize state");
            assert_eq!(json, format!("\"{}\"", state.as_str()));
        }
        for starts in [
            ServiceStarts::BootWithoutLogin,
            ServiceStarts::LoginOnly,
            ServiceStarts::Disabled,
            ServiceStarts::Unavailable,
        ] {
            let json = serde_json::to_string(&starts).expect("serialize starts");
            assert_eq!(json, format!("\"{}\"", starts.as_str()));
        }
        for scope in [ServiceScope::User, ServiceScope::System] {
            let json = serde_json::to_string(&scope).expect("serialize scope");
            assert_eq!(json, format!("\"{}\"", scope.as_str()));
        }
        for supervisor in [Supervisor::Systemd, Supervisor::Launchd] {
            let json = serde_json::to_string(&supervisor).expect("serialize supervisor");
            assert_eq!(json, format!("\"{}\"", supervisor.as_str()));
        }
    }

    #[test]
    fn log_source_describes_both_kinds() {
        let journal = LogSource::Journal {
            unit: "spiced-cloud-connect-edge-1.service".to_string(),
            scope: ServiceScope::User,
        };
        assert!(
            journal.describe().contains("user"),
            "{}",
            journal.describe()
        );
        let files = LogSource::Files {
            stdout: PathBuf::from("/var/log/spice/out.log"),
            stderr: PathBuf::from("/var/log/spice/err.log"),
        };
        assert_eq!(
            files.describe(),
            "/var/log/spice/out.log, /var/log/spice/err.log"
        );
    }
}
