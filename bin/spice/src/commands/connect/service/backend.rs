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

//! Manifest-scoped backend contract implemented independently by systemd and
//! launchd. Every operation receives a validated manifest or an install request
//! tied to one canonical directory; arbitrary service names are never inputs.

use std::path::PathBuf;

use super::manifest::ServiceManifest;
use super::model::{Diagnostic, ServiceStarts, ServiceState};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BackendFailure {
    pub(crate) code: String,
    pub(crate) message: String,
    pub(crate) remediation: String,
}

impl BackendFailure {
    #[must_use]
    pub(crate) fn new(
        code: impl Into<String>,
        message: impl Into<String>,
        remediation: impl Into<String>,
    ) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            remediation: remediation.into(),
        }
    }

    #[must_use]
    pub(crate) fn diagnostic(self) -> Diagnostic {
        Diagnostic::new(self.code, self.message, self.remediation)
    }
}

impl std::fmt::Display for BackendFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for BackendFailure {}

pub(crate) type Result<T, E = BackendFailure> = std::result::Result<T, E>;

/// Supervisor-normalized observation. Backends translate native state once;
/// collectors and renderers never branch on systemctl/launchctl strings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BackendStatus {
    pub(crate) state: ServiceState,
    pub(crate) starts: ServiceStarts,
    pub(crate) start_remediation: Option<String>,
}

/// Inputs whose persistence is owned by service backends. Enrollment is kept
/// outside the backend so credentials can never cross the privilege seam.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InstallRequest {
    pub(crate) directory: PathBuf,
    pub(crate) config_directory: PathBuf,
    pub(crate) source_runtime: PathBuf,
}

/// Bounded log selection shared by journald and rotated-file implementations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LogRequest {
    pub(crate) number: u32,
    pub(crate) follow: bool,
}

/// Exact-directory service lifecycle contract.
pub(crate) trait ServiceBackend {
    fn install(&self, request: &InstallRequest) -> Result<ServiceManifest>;
    fn uninstall(&self, manifest: &ServiceManifest) -> Result<()>;
    fn start(&self, manifest: &ServiceManifest) -> Result<()>;
    fn stop(&self, manifest: &ServiceManifest) -> Result<()>;
    fn restart(&self, manifest: &ServiceManifest) -> Result<()>;
    fn status(&self, manifest: &ServiceManifest) -> Result<BackendStatus>;
    fn logs(&self, manifest: &ServiceManifest, request: LogRequest) -> Result<()>;
}

/// Placeholder selected until the platform-specific lifecycle branches stack
/// on this contract. Status without a manifest remains a normal not-installed
/// state and never calls this backend.
pub(crate) struct PlatformBackend;

impl PlatformBackend {
    fn unavailable(action: &str) -> BackendFailure {
        BackendFailure::new(
            "service_backend_unavailable",
            format!(
                "Failed to {action} the Spice Cloud Connect service: the {} lifecycle backend is unavailable in this build.",
                std::env::consts::OS
            ),
            "Install a build with the native service backend, or run `spice run` in the foreground.",
        )
    }
}

impl ServiceBackend for PlatformBackend {
    fn install(&self, request: &InstallRequest) -> Result<ServiceManifest> {
        let _ = (
            &request.directory,
            &request.config_directory,
            &request.source_runtime,
        );
        Err(Self::unavailable("install"))
    }

    fn uninstall(&self, _manifest: &ServiceManifest) -> Result<()> {
        Err(Self::unavailable("uninstall"))
    }

    fn start(&self, _manifest: &ServiceManifest) -> Result<()> {
        Err(Self::unavailable("start"))
    }

    fn stop(&self, _manifest: &ServiceManifest) -> Result<()> {
        Err(Self::unavailable("stop"))
    }

    fn restart(&self, _manifest: &ServiceManifest) -> Result<()> {
        Err(Self::unavailable("restart"))
    }

    fn status(&self, _manifest: &ServiceManifest) -> Result<BackendStatus> {
        Err(Self::unavailable("inspect"))
    }

    fn logs(&self, _manifest: &ServiceManifest, request: LogRequest) -> Result<()> {
        let _ = (request.number, request.follow);
        Err(Self::unavailable("read logs for"))
    }
}
