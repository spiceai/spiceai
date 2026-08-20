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

#![cfg_attr(not(any(target_os = "linux", target_os = "macos")), expect(dead_code))]

//! The interface every service back end implements.
//!
//! One trait for both supported back ends, with no defaulted methods: a back end
//! that forgets an operation fails to compile rather than silently inheriting
//! a no-op, which is the failure mode a defaulted lifecycle method has.
//!
//! The trait speaks only the normalized vocabulary in [`super::model`]. Every
//! supervisor word — `activating`, `waiting`, `not loaded` — is translated
//! inside the back end, so nothing above this line has to know which
//! supervisor answered.

use std::path::{Path, PathBuf};

use super::model::{LogSource, ServiceScope, ServiceStarts, ServiceState, Supervisor};
use super::{InstalledService, PreflightFailure, manifest::ServiceManifest};
use crate::error::Result;

/// What an install needs to know, resolved by the caller so the back end
/// never derives a path from the environment.
#[derive(Debug, Clone, Copy)]
pub(crate) struct InstallRequest<'a> {
    /// The canonical instance directory: the spicepod root the service runs
    /// from, and the only input the service name is derived from.
    pub(crate) instance_dir: &'a Path,
    /// The resolved Spice config directory holding the enrolled identity.
    pub(crate) config_dir: &'a Path,
    /// The `spiced` the CLI found, to be staged for the service.
    pub(crate) spiced_path: &'a Path,
    /// The service domain to install into.
    pub(crate) scope: ServiceScope,
    /// Where this instance answers a health probe, so an install can prove the
    /// service it just started is actually serving before it reports success.
    pub(crate) health_url: &'a str,
}

/// One fresh reading of what the supervisor says about a service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ServiceObservation {
    pub(crate) state: ServiceState,
    pub(crate) starts: ServiceStarts,
    /// Why the reading is [`ServiceState::Unavailable`], when it is.
    pub(crate) diagnostic: Option<String>,
    /// The command that would give the service the boot persistence it lacks.
    pub(crate) starts_action: Option<String>,
}

/// Another Spice-managed service whose default listeners would collide with
/// a new installation for this directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ServiceConflict {
    pub(crate) name: String,
    pub(crate) path: PathBuf,
    pub(crate) working_dir: PathBuf,
}

impl ServiceObservation {
    /// A reading the supervisor could not answer.
    pub(crate) fn unavailable(diagnostic: String) -> Self {
        Self {
            state: ServiceState::Unavailable,
            starts: ServiceStarts::Unavailable,
            diagnostic: Some(diagnostic),
            starts_action: None,
        }
    }
}

/// How much log history to print, and whether to keep printing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LogRequest {
    /// Lines of existing history to print before anything else. `0` with
    /// `follow` prints only new output.
    pub(crate) number: u32,
    /// Keep printing new output until interrupted.
    pub(crate) follow: bool,
    /// Return the bounded history to the caller instead of writing it to
    /// stdout. Capture requests never follow.
    pub(crate) capture: bool,
}

/// The operations a supervisor back end provides.
///
/// Deliberately without default implementations. Adding one later would let a
/// back end inherit a no-op for an operation it must actually perform — the
/// exact regression a defaulted trait method produces in a wrapper.
pub(crate) trait ServiceBackend {
    /// Which supervisor this back end drives.
    fn supervisor(&self) -> Supervisor;

    /// Whether this host can have a service installed in `scope`, checked
    /// before anything irreversible happens.
    ///
    /// # Errors
    ///
    /// Returns the reason installation is impossible on this host.
    fn preflight(&self, scope: ServiceScope) -> std::result::Result<(), PreflightFailure>;

    /// The deterministic service name this back end gives `instance_dir`.
    fn name_for_dir(&self, instance_dir: &Path) -> String;

    /// Where the definition for `name` lives in `scope`.
    fn definition_path(&self, name: &str, scope: ServiceScope) -> PathBuf;

    /// Where the supervisor keeps the output of `name` in `scope`, or `None`
    /// when the definition names no source this CLI can read.
    fn log_source(&self, name: &str, scope: ServiceScope) -> Option<LogSource>;

    /// The service installed for `instance_dir`, as the supervisor's own
    /// definition describes it, or `None` when there is no definition under
    /// this directory's derived name that names this directory.
    ///
    /// This is not a search: the name is derived from `instance_dir` and the
    /// definition is accepted only when it agrees. It exists so a directory
    /// that lost its manifest — deleted by hand, or restored from a backup that
    /// skipped it — can still be reported on and removed, and so an install can
    /// tell "my own service" from "someone else's file under the name I
    /// derive".
    fn find_installed(&self, instance_dir: &Path, scope: ServiceScope) -> Option<InstalledService>;

    /// Find a service for another directory that would share this runtime's
    /// fixed HTTP and Flight listeners.
    ///
    /// # Errors
    ///
    /// Returns an error when an installation directory exists but cannot be
    /// inspected safely enough to rule out a collision.
    fn find_conflicting_installation(&self, instance_dir: &Path)
    -> Result<Option<ServiceConflict>>;

    /// Install (or reinstall) and start the service.
    ///
    /// # Errors
    ///
    /// Returns an error when the definition cannot be written or the
    /// supervisor rejects it.
    fn install(&self, request: &InstallRequest<'_>) -> Result<InstalledService>;

    /// Stop and remove exactly the service the manifest describes.
    ///
    /// # Errors
    ///
    /// Returns an error when the definition cannot be removed, or when the
    /// supervisor is left holding a job the removed definition can no longer
    /// account for.
    fn uninstall(&self, manifest: &ServiceManifest) -> Result<()>;

    /// Start an installed, stopped service. Idempotent.
    ///
    /// # Errors
    ///
    /// Returns an error when the supervisor refuses or the service does not
    /// come up.
    fn start(&self, manifest: &ServiceManifest) -> Result<()>;

    /// Stop a running service. Idempotent, and leaves it installed.
    ///
    /// # Errors
    ///
    /// Returns an error when the supervisor refuses or the service does not
    /// go down.
    fn stop(&self, manifest: &ServiceManifest) -> Result<()>;

    /// Stop and start the service through the supervisor, waiting for it to
    /// reach [`ServiceState::Running`] or fail. Never asks `spiced` to exit
    /// itself.
    ///
    /// # Errors
    ///
    /// Returns an error when the service does not come back.
    fn restart(&self, manifest: &ServiceManifest) -> Result<()>;

    /// Ask the supervisor for the service's current state and persistence.
    ///
    /// Never fails: a supervisor that cannot be asked is reported as
    /// [`ServiceState::Unavailable`] with a diagnostic, because status has to
    /// render something either way.
    fn observe(&self, manifest: &ServiceManifest) -> ServiceObservation;

    /// Read the service's output as `request` asks. A capture request returns
    /// the bounded history; a terminal request writes it directly and returns
    /// `None`.
    ///
    /// # Errors
    ///
    /// Returns an error when the log source cannot be read.
    fn logs(&self, manifest: &ServiceManifest, request: LogRequest) -> Result<Option<Vec<String>>>;

    /// The native supervisor commands an operator falls back to when a Spice
    /// command cannot complete. Recovery detail, never the primary interface.
    fn recovery_hints(&self, manifest: &ServiceManifest) -> Vec<String>;
}

/// The back end for this host.
///
/// Selected at compile time: a host has one supervisor, and choosing it at
/// runtime would mean shipping a code path that can never run there.
#[cfg(target_os = "linux")]
pub(crate) fn for_host() -> &'static dyn ServiceBackend {
    &super::systemd::SystemdBackend
}

/// The back end for macOS.
#[cfg(target_os = "macos")]
pub(crate) fn for_host() -> &'static dyn ServiceBackend {
    &super::launchd::LaunchdBackend
}

/// The back end for a host with no supported supervisor.
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub(crate) fn for_host() -> &'static dyn ServiceBackend {
    &unsupported::UnsupportedBackend
}

/// A back end for a host that has no supervisor Spice can install into.
///
/// [`ServiceBackend::preflight`] rejects such a host before any state changes,
/// so the remaining operations exist to keep the CLI building and to give the
/// same refusal if one is somehow reached.
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod unsupported {
    use super::{
        InstallRequest, InstalledService, LogRequest, LogSource, Path, PathBuf, PreflightFailure,
        Result, ServiceBackend, ServiceConflict, ServiceManifest, ServiceObservation, ServiceScope,
        Supervisor,
    };

    pub(super) struct UnsupportedBackend;

    impl ServiceBackend for UnsupportedBackend {
        fn supervisor(&self) -> Supervisor {
            // Nothing installs here, so the value is only ever a placeholder in
            // an error path.
            Supervisor::Systemd
        }

        fn preflight(&self, _scope: ServiceScope) -> std::result::Result<(), PreflightFailure> {
            Err(PreflightFailure::UnsupportedPlatform)
        }

        fn name_for_dir(&self, instance_dir: &Path) -> String {
            super::super::name_stem_for_dir(instance_dir)
        }

        fn definition_path(&self, name: &str, _scope: ServiceScope) -> PathBuf {
            PathBuf::from(name)
        }

        fn log_source(&self, _name: &str, _scope: ServiceScope) -> Option<LogSource> {
            None
        }

        fn find_installed(
            &self,
            _instance_dir: &Path,
            _scope: ServiceScope,
        ) -> Option<InstalledService> {
            None
        }

        fn find_conflicting_installation(
            &self,
            _instance_dir: &Path,
        ) -> Result<Option<ServiceConflict>> {
            Ok(None)
        }

        fn install(&self, _request: &InstallRequest<'_>) -> Result<InstalledService> {
            Err(PreflightFailure::UnsupportedPlatform.into())
        }

        fn uninstall(&self, _manifest: &ServiceManifest) -> Result<()> {
            Err(PreflightFailure::UnsupportedPlatform.into())
        }

        fn start(&self, _manifest: &ServiceManifest) -> Result<()> {
            Err(PreflightFailure::UnsupportedPlatform.into())
        }

        fn stop(&self, _manifest: &ServiceManifest) -> Result<()> {
            Err(PreflightFailure::UnsupportedPlatform.into())
        }

        fn restart(&self, _manifest: &ServiceManifest) -> Result<()> {
            Err(PreflightFailure::UnsupportedPlatform.into())
        }

        fn observe(&self, _manifest: &ServiceManifest) -> ServiceObservation {
            ServiceObservation::unavailable(format!(
                "{} has no service supervisor Spice can manage.",
                std::env::consts::OS
            ))
        }

        fn logs(
            &self,
            _manifest: &ServiceManifest,
            _request: LogRequest,
        ) -> Result<Option<Vec<String>>> {
            Err(PreflightFailure::UnsupportedPlatform.into())
        }

        fn recovery_hints(&self, _manifest: &ServiceManifest) -> Vec<String> {
            Vec::new()
        }
    }
}

/// A back end whose every answer is scripted, so the layers above it can be
/// tested against states no test host can be put into on demand.
#[cfg(test)]
pub(crate) mod fake {
    use std::cell::RefCell;

    use super::{
        InstallRequest, InstalledService, LogRequest, LogSource, Path, PathBuf, PreflightFailure,
        Result, ServiceBackend, ServiceConflict, ServiceManifest, ServiceObservation, ServiceScope,
        ServiceStarts, ServiceState, Supervisor,
    };
    use crate::error::Error;

    /// The service name the fake gives every directory, so tests can assert
    /// against a fixed prefix.
    pub(crate) const FAKE_NAME_PREFIX: &str = "fake-cloud-connect";

    /// The runtime path the fake reports as installed.
    pub(crate) const FAKE_RUNTIME: &str = "/usr/local/lib/spice/spiced";

    pub(crate) struct FakeBackend {
        pub(crate) supervisor: Supervisor,
        pub(crate) observation: ServiceObservation,
        /// Directory the fake writes its definitions into, so the shared
        /// resolution and pre-existing-name checks can be exercised against
        /// real files.
        pub(crate) definitions: PathBuf,
        /// Operations that fail, and what they say when they do. Lets a test
        /// drive a partial failure — an uninstall whose supervisor refused
        /// after the definition was already gone, for instance.
        pub(crate) failures: Vec<(&'static str, String)>,
        /// Every back-end call made, in order.
        pub(crate) calls: RefCell<Vec<String>>,
        /// Test hook that makes the manifest destination unusable only after
        /// installation, exercising the caller's rollback boundary.
        pub(crate) block_manifest_write: bool,
    }

    impl FakeBackend {
        /// A fake whose definitions live under `root`, reporting a running
        /// service.
        pub(crate) fn new(root: &Path) -> Self {
            Self {
                supervisor: Supervisor::Systemd,
                observation: ServiceObservation {
                    state: ServiceState::Running,
                    starts: ServiceStarts::BootWithoutLogin,
                    diagnostic: None,
                    starts_action: None,
                },
                definitions: root.join("definitions"),
                failures: Vec::new(),
                calls: RefCell::new(Vec::new()),
                block_manifest_write: false,
            }
        }

        pub(crate) fn in_state(root: &Path, state: ServiceState) -> Self {
            let mut fake = Self::new(root);
            fake.observation.state = state;
            fake
        }

        pub(crate) fn failing(root: &Path, operation: &'static str, message: &str) -> Self {
            let mut fake = Self::new(root);
            fake.failures = vec![(operation, message.to_string())];
            fake
        }

        pub(crate) fn calls(&self) -> Vec<String> {
            self.calls.borrow().clone()
        }

        /// Put a definition on disk that this back end did not write — what a
        /// hand-made unit, or one left behind by a directory that has since
        /// moved, looks like to the installer.
        pub(crate) fn plant_foreign_definition(&self, instance_dir: &Path) {
            let name = self.name_for_dir(instance_dir);
            let path = self.definition_path(&name, ServiceScope::System);
            std::fs::create_dir_all(&self.definitions).expect("create definition dir");
            std::fs::write(path, "/somewhere/else").expect("plant a definition");
        }

        fn record(&self, operation: &'static str) -> Result<()> {
            self.calls.borrow_mut().push(operation.to_string());
            match self
                .failures
                .iter()
                .find(|(name, _)| *name == operation)
                .map(|(_, message)| message.clone())
            {
                Some(message) => Err(Error::CloudConnectIo { message }),
                None => Ok(()),
            }
        }
    }

    impl ServiceBackend for FakeBackend {
        fn supervisor(&self) -> Supervisor {
            self.supervisor
        }

        fn preflight(&self, _scope: ServiceScope) -> std::result::Result<(), PreflightFailure> {
            Ok(())
        }

        fn name_for_dir(&self, instance_dir: &Path) -> String {
            format!(
                "{FAKE_NAME_PREFIX}-{}",
                super::super::name_stem_for_dir(instance_dir)
            )
        }

        fn definition_path(&self, name: &str, _scope: ServiceScope) -> PathBuf {
            self.definitions.join(name)
        }

        fn log_source(&self, name: &str, scope: ServiceScope) -> Option<LogSource> {
            Some(LogSource::Journal {
                unit: name.to_string(),
                scope,
            })
        }

        fn find_installed(
            &self,
            instance_dir: &Path,
            scope: ServiceScope,
        ) -> Option<InstalledService> {
            let name = self.name_for_dir(instance_dir);
            let path = self.definition_path(&name, scope);
            // The definition records the directory it was written for, the way
            // a real one bakes in its working directory. A definition that
            // names somewhere else is not this directory's service.
            let recorded = std::fs::read_to_string(&path).ok()?;
            (Path::new(recorded.trim()) == instance_dir).then(|| InstalledService {
                name,
                path,
                working_dir: instance_dir.to_path_buf(),
                config_dir: Some(instance_dir.join(".spice")),
                runtime: PathBuf::from(FAKE_RUNTIME),
            })
        }

        fn find_conflicting_installation(
            &self,
            instance_dir: &Path,
        ) -> Result<Option<ServiceConflict>> {
            let entries = match std::fs::read_dir(&self.definitions) {
                Ok(entries) => entries,
                Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(source) => {
                    return Err(Error::CloudConnectIo {
                        message: format!("inspect fake service definitions: {source}"),
                    });
                }
            };
            for entry in entries {
                let entry = entry.map_err(|source| Error::CloudConnectIo {
                    message: format!("inspect a fake service definition: {source}"),
                })?;
                let name = entry.file_name().to_string_lossy().into_owned();
                if !name.starts_with(FAKE_NAME_PREFIX) {
                    continue;
                }
                let working_dir =
                    PathBuf::from(std::fs::read_to_string(entry.path()).map_err(|source| {
                        Error::CloudConnectIo {
                            message: format!("read fake service definition {name}: {source}"),
                        }
                    })?);
                if working_dir != instance_dir {
                    return Ok(Some(ServiceConflict {
                        name,
                        path: entry.path(),
                        working_dir,
                    }));
                }
            }
            Ok(None)
        }

        fn install(&self, request: &InstallRequest<'_>) -> Result<InstalledService> {
            self.record("install")?;
            let name = self.name_for_dir(request.instance_dir);
            let path = self.definition_path(&name, request.scope);
            std::fs::create_dir_all(&self.definitions).map_err(|e| Error::CloudConnectIo {
                message: format!("create the fake definition directory: {e}"),
            })?;
            std::fs::write(&path, request.instance_dir.display().to_string()).map_err(|e| {
                Error::CloudConnectIo {
                    message: format!("write the fake definition: {e}"),
                }
            })?;
            if self.block_manifest_write {
                std::fs::create_dir_all(request.config_dir).map_err(|e| Error::CloudConnectIo {
                    message: format!("create the fake config directory: {e}"),
                })?;
                std::fs::create_dir(
                    request
                        .config_dir
                        .join(super::super::manifest::SERVICE_MANIFEST_FILE),
                )
                .map_err(|e| Error::CloudConnectIo {
                    message: format!("block the fake manifest destination: {e}"),
                })?;
            }
            Ok(InstalledService {
                name,
                path,
                working_dir: request.instance_dir.to_path_buf(),
                config_dir: Some(request.config_dir.to_path_buf()),
                runtime: PathBuf::from(FAKE_RUNTIME),
            })
        }

        fn uninstall(&self, manifest: &ServiceManifest) -> Result<()> {
            self.record("uninstall")?;
            let _ = std::fs::remove_file(&manifest.definition_path);
            Ok(())
        }

        fn start(&self, _manifest: &ServiceManifest) -> Result<()> {
            self.record("start")
        }

        fn stop(&self, _manifest: &ServiceManifest) -> Result<()> {
            self.record("stop")
        }

        fn restart(&self, _manifest: &ServiceManifest) -> Result<()> {
            self.record("restart")
        }

        fn observe(&self, _manifest: &ServiceManifest) -> ServiceObservation {
            self.observation.clone()
        }

        fn logs(
            &self,
            _manifest: &ServiceManifest,
            request: LogRequest,
        ) -> Result<Option<Vec<String>>> {
            self.record("logs")?;
            Ok(request.capture.then(Vec::new))
        }

        fn recovery_hints(&self, manifest: &ServiceManifest) -> Vec<String> {
            vec![format!("fakectl status {}", manifest.name)]
        }
    }
}
