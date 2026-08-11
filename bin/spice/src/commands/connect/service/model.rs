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

//! Stable, versioned status types shared by `connect status` and
//! `connect service status`.

use std::path::PathBuf;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Version of the public Cloud Connect status and service-manifest schemas.
pub(crate) const SCHEMA_VERSION: u32 = 1;

/// One fresh status snapshot for a Cloud Connect instance directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ConnectStatus {
    pub(crate) schema_version: u32,
    pub(crate) observed_at: DateTime<Utc>,
    pub(crate) directory: PathBuf,
    pub(crate) connection: ConnectionStatus,
    pub(crate) service: ServiceStatus,
    pub(crate) deployment: DeploymentStatus,
}

impl ConnectStatus {
    /// The filtered document emitted by `connect service status`.
    #[must_use]
    pub(crate) fn service_document(&self) -> ServiceStatusDocument {
        ServiceStatusDocument {
            schema_version: self.schema_version,
            observed_at: self.observed_at,
            directory: self.directory.clone(),
            service: self.service.clone(),
        }
    }

    /// An unavailable probe is an operational failure even though a complete
    /// JSON snapshot is still emitted.
    #[must_use]
    pub(crate) fn has_unavailable_section(&self) -> bool {
        self.connection.state == ConnectionState::Unavailable
            || self.service.state == ServiceState::Unavailable
            || self.connection.diagnostic.is_some()
            || self.service.diagnostic.is_some()
            || self.deployment.diagnostic.is_some()
    }
}

/// The service-only view. Its nested `service` value uses the same Rust value
/// as [`ConnectStatus::service`], preventing a filtered schema from drifting.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ServiceStatusDocument {
    pub(crate) schema_version: u32,
    pub(crate) observed_at: DateTime<Utc>,
    pub(crate) directory: PathBuf,
    pub(crate) service: ServiceStatus,
}

/// Local Cloud connection observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ConnectionStatus {
    pub(crate) state: ConnectionState,
    pub(crate) instance_id: Option<String>,
    pub(crate) organization: Option<String>,
    pub(crate) project: Option<String>,
    pub(crate) identity_path: PathBuf,
    pub(crate) identity_expires_at: Option<DateTime<Utc>>,
    pub(crate) gateway: Option<String>,
    pub(crate) new_project_url: Option<String>,
    pub(crate) monitor_url: Option<String>,
    pub(crate) diagnostic: Option<Diagnostic>,
}

/// Normalized Cloud connection states exposed to automation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ConnectionState {
    NotEnrolled,
    EnrolledOffline,
    Connected,
    Unavailable,
}

/// Local supervisor observation for this exact instance directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ServiceStatus {
    pub(crate) installed: bool,
    pub(crate) state: ServiceState,
    pub(crate) scope: Option<ServiceScope>,
    pub(crate) supervisor: Option<ServiceSupervisor>,
    pub(crate) starts: ServiceStarts,
    pub(crate) start_remediation: Option<String>,
    pub(crate) owner: Option<ServiceOwner>,
    pub(crate) name: Option<String>,
    pub(crate) working_directory: PathBuf,
    pub(crate) definition_path: Option<PathBuf>,
    pub(crate) runtime_path: Option<PathBuf>,
    pub(crate) log_source: Option<String>,
    pub(crate) diagnostic: Option<Diagnostic>,
}

impl ServiceStatus {
    #[must_use]
    pub(crate) fn not_installed(working_directory: PathBuf) -> Self {
        Self {
            installed: false,
            state: ServiceState::NotInstalled,
            scope: None,
            supervisor: None,
            starts: ServiceStarts::Disabled,
            start_remediation: None,
            owner: None,
            name: None,
            working_directory,
            definition_path: None,
            runtime_path: None,
            log_source: None,
            diagnostic: None,
        }
    }

    #[must_use]
    pub(crate) fn unavailable(working_directory: PathBuf, diagnostic: Diagnostic) -> Self {
        Self {
            state: ServiceState::Unavailable,
            starts: ServiceStarts::Unavailable,
            diagnostic: Some(diagnostic),
            ..Self::not_installed(working_directory)
        }
    }
}

/// Normalized service states common to systemd and launchd.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ServiceState {
    NotInstalled,
    Starting,
    Running,
    Stopping,
    Stopped,
    Failed,
    Unavailable,
}

/// Whether the service is installed in the current user's supervisor domain or
/// the machine-wide supervisor domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ServiceScope {
    User,
    System,
}

/// Supervisor that owns the exact service named by the manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ServiceSupervisor {
    Systemd,
    Launchd,
}

/// Persistence guarantee independently observed from the supervisor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ServiceStarts {
    Disabled,
    LoginOnly,
    BootWithoutLogin,
    Unavailable,
}

/// Account the managed runtime executes as.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ServiceOwner {
    pub(crate) name: String,
    pub(crate) uid: u32,
}

/// Desired deployment and delivered-secret metadata local to this directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DeploymentStatus {
    pub(crate) desired_path: PathBuf,
    pub(crate) delivered_secrets: Vec<String>,
    pub(crate) restart_required: Vec<String>,
    pub(crate) diagnostic: Option<Diagnostic>,
}

/// A stable machine-readable failure carried inside the affected status
/// section, rather than replacing the whole snapshot with free-form text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Diagnostic {
    pub(crate) code: String,
    pub(crate) message: String,
    pub(crate) remediation: String,
}

impl Diagnostic {
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
}
