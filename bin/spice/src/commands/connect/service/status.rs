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

//! Fresh per-directory status collection and rendering.

use std::path::{Path, PathBuf};

use chrono::{TimeZone as _, Utc};
use fs2::FileExt as _;

use crate::context::RuntimeContext;

use super::backend::ServiceBackend;
use super::manifest::{ManifestState, ServiceManifest, load_validated};
use super::model::{
    ConnectStatus, ConnectionState, ConnectionStatus, DeploymentStatus, Diagnostic, SCHEMA_VERSION,
    ServiceStarts, ServiceState, ServiceStatus,
};

const RUNTIME_LOCK_FILE: &str = "runtime.lock";

pub(crate) async fn collect(
    context: &RuntimeContext,
    config_dir: &Path,
    directory: &Path,
    backend: &dyn ServiceBackend,
) -> ConnectStatus {
    let canonical_directory = std::fs::canonicalize(directory).unwrap_or_else(|_| directory.into());
    let identity_path = config_dir.join(runtime_cloud_connect::config::IDENTITY_FILE);
    let (service, manifest) = collect_service(config_dir, &canonical_directory, backend);
    let connection = collect_connection(
        context,
        config_dir,
        identity_path,
        &service,
        manifest.as_ref(),
    )
    .await;
    let deployment = collect_deployment(config_dir);

    // Stamp after every sequential probe so observed_at never predates an
    // observation included in this snapshot.
    ConnectStatus {
        schema_version: SCHEMA_VERSION,
        observed_at: Utc::now(),
        directory: canonical_directory,
        connection,
        service,
        deployment,
    }
}

fn collect_service(
    config_dir: &Path,
    directory: &Path,
    backend: &dyn ServiceBackend,
) -> (ServiceStatus, Option<ServiceManifest>) {
    let manifest = match load_validated(config_dir, directory) {
        Ok(Some(manifest)) => manifest,
        Ok(None) => return (ServiceStatus::not_installed(directory.to_path_buf()), None),
        Err(error) => {
            return (
                ServiceStatus::unavailable(
                    directory.to_path_buf(),
                    Diagnostic::new(
                        "service_manifest_invalid",
                        error.to_string(),
                        "Repair or remove this directory's service.json, then run `spice connect service status` again.",
                    ),
                ),
                None,
            );
        }
    };

    if manifest.state == ManifestState::Uninstalled {
        return (
            ServiceStatus::not_installed(directory.to_path_buf()),
            Some(manifest),
        );
    }

    let status = status_from_manifest(&manifest, backend);
    (status, Some(manifest))
}

fn status_from_manifest(manifest: &ServiceManifest, backend: &dyn ServiceBackend) -> ServiceStatus {
    let base = |state, starts, start_remediation, diagnostic| ServiceStatus {
        installed: true,
        state,
        scope: Some(manifest.scope),
        supervisor: Some(manifest.backend),
        starts,
        start_remediation,
        owner: Some(manifest.owner.clone()),
        name: Some(manifest.name.clone()),
        working_directory: manifest.directory.clone(),
        definition_path: Some(manifest.definition_path.clone()),
        runtime_path: Some(manifest.runtime_path.clone()),
        log_source: Some(manifest.log_source.clone()),
        diagnostic,
    };

    match backend.status(manifest) {
        Ok(observation) => base(
            observation.state,
            observation.starts,
            observation.start_remediation,
            None,
        ),
        Err(error) => base(
            ServiceState::Unavailable,
            ServiceStarts::Unavailable,
            None,
            Some(error.diagnostic()),
        ),
    }
}

async fn collect_connection(
    context: &RuntimeContext,
    config_dir: &Path,
    identity_path: PathBuf,
    service: &ServiceStatus,
    manifest: Option<&ServiceManifest>,
) -> ConnectionStatus {
    let identity = match runtime_cloud_connect::identity::IdentityStore::load_optional(
        &identity_path,
    ) {
        Ok(identity) => identity,
        Err(error) => {
            return ConnectionStatus {
                state: ConnectionState::Unavailable,
                instance_id: None,
                organization: None,
                project: None,
                identity_path,
                identity_expires_at: None,
                gateway: None,
                new_project_url: None,
                monitor_url: None,
                diagnostic: Some(Diagnostic::new(
                    "identity_unavailable",
                    format!("Failed to read the Cloud Connect identity: {error}"),
                    "Restore an owner-only identity.json or run `spice connect remove --yes` before enrolling again.",
                )),
            };
        }
    };

    let Some(identity) = identity else {
        return ConnectionStatus {
            state: ConnectionState::NotEnrolled,
            instance_id: None,
            organization: None,
            project: None,
            identity_path,
            identity_expires_at: None,
            gateway: None,
            new_project_url: None,
            monitor_url: None,
            diagnostic: None,
        };
    };

    let expires_at = identity.not_after_unix.and_then(|seconds| {
        i64::try_from(seconds)
            .ok()
            .and_then(|seconds| Utc.timestamp_opt(seconds, 0).single())
    });
    let mut status = ConnectionStatus {
        state: ConnectionState::EnrolledOffline,
        instance_id: Some(identity.identifier),
        organization: None,
        project: None,
        identity_path,
        identity_expires_at: expires_at,
        gateway: (!identity.gateway_addr.is_empty()).then_some(identity.gateway_addr),
        new_project_url: None,
        monitor_url: None,
        diagnostic: None,
    };

    let service_expected_running = service.state == ServiceState::Running;
    let foreground_owned =
        runtime_lock_is_held(&config_dir.join(RUNTIME_LOCK_FILE)).unwrap_or(false);
    let status_url = if service_expected_running {
        manifest.and_then(|manifest| manifest.health_url.as_deref())
    } else if foreground_owned {
        Some(context.http_endpoint())
    } else {
        None
    };

    if let Some(url) = status_url {
        match runtime_cloud_stream_is_connected(context, url).await {
            Ok(true) => status.state = ConnectionState::Connected,
            Ok(false) => {}
            Err(message) if service_expected_running => {
                status.state = ConnectionState::Unavailable;
                status.diagnostic = Some(Diagnostic::new(
                    "runtime_status_unavailable",
                    message,
                    "Check `spice connect service logs` and retry `spice connect status`.",
                ));
            }
            Err(_) => {}
        }
    }
    status
}

fn runtime_lock_is_held(path: &Path) -> std::io::Result<bool> {
    let file = match std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    match file.try_lock_exclusive() {
        Ok(()) => {
            file.unlock()?;
            Ok(false)
        }
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(true),
        Err(error) => Err(error),
    }
}

async fn runtime_cloud_stream_is_connected(
    context: &RuntimeContext,
    endpoint: &str,
) -> std::result::Result<bool, String> {
    let status_url = status_url(endpoint)?;
    let response = context
        .http_client()
        .get(status_url.clone())
        .send()
        .await
        .map_err(|error| format!("Failed to query runtime status at {status_url}: {error}"))?;
    if !response.status().is_success() {
        return Err(format!(
            "Failed to query runtime status at {status_url}: HTTP {}",
            response.status()
        ));
    }
    let document = response
        .json::<serde_json::Value>()
        .await
        .map_err(|error| format!("Failed to parse runtime status at {status_url}: {error}"))?;
    let Some(rows) = document.as_array() else {
        return Err(format!(
            "Failed to parse runtime status at {status_url}: expected an array"
        ));
    };
    Ok(rows.iter().any(|row| {
        let name = row.get("name").and_then(serde_json::Value::as_str);
        let state = row.get("status").and_then(serde_json::Value::as_str);
        matches!(name, Some("cloud_connect" | "cloud-connect"))
            && matches!(state, Some("Ready" | "ready" | "Connected" | "connected"))
    }))
}

fn status_url(endpoint: &str) -> std::result::Result<reqwest::Url, String> {
    let mut url = reqwest::Url::parse(endpoint)
        .map_err(|error| format!("Invalid local runtime health URL {endpoint}: {error}"))?;
    url.set_path("/v1/status");
    url.set_query(None);
    url.set_fragment(None);
    Ok(url)
}

fn collect_deployment(config_dir: &Path) -> DeploymentStatus {
    let desired_path = config_dir.join(runtime_cloud_connect::config::CLOUD_MANAGED_SPICEPOD_FILE);
    let secret_path = config_dir.join(runtime_cloud_connect::secret_cache::SECRET_CACHE_FILE);
    let (delivered_secrets, diagnostic) =
        match runtime_cloud_connect::secret_cache::read_header(&secret_path) {
            Some(header) => (header.names, None),
            None if secret_path.exists() => (
                Vec::new(),
                Some(Diagnostic::new(
                    "delivered_secrets_unavailable",
                    format!(
                        "Failed to read delivered-secret metadata at {}.",
                        secret_path.display()
                    ),
                    "Deploy the project again to replace the local delivered-secret cache.",
                )),
            ),
            None => (Vec::new(), None),
        };
    DeploymentStatus {
        desired_path,
        delivered_secrets,
        restart_required: Vec::new(),
        diagnostic,
    }
}

pub(crate) fn render_human(status: &ConnectStatus) {
    println!("Spice Cloud Connect");
    println!("  directory:   {}", status.directory.display());
    println!(
        "  connection:  {}",
        connection_label(status.connection.state)
    );
    println!(
        "  instance:    {}",
        status
            .connection
            .instance_id
            .as_deref()
            .unwrap_or("not enrolled")
    );
    println!("Service");
    println!("  installed:   {}", status.service.installed);
    println!("  state:       {}", service_label(status.service.state));
    println!(
        "  definition:  {}",
        display_optional_path(status.service.definition_path.as_deref())
    );
    println!(
        "  runtime:     {}",
        display_optional_path(status.service.runtime_path.as_deref())
    );
    println!(
        "  logs:        {}",
        status
            .service
            .log_source
            .as_deref()
            .unwrap_or("not installed")
    );
    println!("Deployment");
    println!(
        "  desired:     {}",
        status.deployment.desired_path.display()
    );
    println!(
        "  secrets:     {}",
        if status.deployment.delivered_secrets.is_empty() {
            "none delivered yet".to_string()
        } else {
            status.deployment.delivered_secrets.join(", ")
        }
    );
    for diagnostic in [
        status.connection.diagnostic.as_ref(),
        status.service.diagnostic.as_ref(),
        status.deployment.diagnostic.as_ref(),
    ]
    .into_iter()
    .flatten()
    {
        eprintln!(
            "{}: {} {}",
            diagnostic.code, diagnostic.message, diagnostic.remediation
        );
    }
}

pub(crate) fn render_service_human(status: &ConnectStatus) {
    println!("Spice Cloud Connect service");
    println!("  directory:   {}", status.directory.display());
    println!("  installed:   {}", status.service.installed);
    println!("  state:       {}", service_label(status.service.state));
    println!(
        "  definition:  {}",
        display_optional_path(status.service.definition_path.as_deref())
    );
    println!(
        "  runtime:     {}",
        display_optional_path(status.service.runtime_path.as_deref())
    );
    println!(
        "  logs:        {}",
        status
            .service
            .log_source
            .as_deref()
            .unwrap_or("not installed")
    );
    if let Some(diagnostic) = &status.service.diagnostic {
        eprintln!(
            "{}: {} {}",
            diagnostic.code, diagnostic.message, diagnostic.remediation
        );
    }
}

fn display_optional_path(path: Option<&Path>) -> String {
    path.map_or_else(
        || "not installed".to_string(),
        |path| path.display().to_string(),
    )
}

fn connection_label(state: ConnectionState) -> &'static str {
    match state {
        ConnectionState::NotEnrolled => "not enrolled",
        ConnectionState::EnrolledOffline => "enrolled, offline",
        ConnectionState::Connected => "connected",
        ConnectionState::Unavailable => "unavailable",
    }
}

fn service_label(state: ServiceState) -> &'static str {
    match state {
        ServiceState::NotInstalled => "not installed",
        ServiceState::Starting => "starting",
        ServiceState::Running => "running",
        ServiceState::Stopping => "stopping",
        ServiceState::Stopped => "stopped",
        ServiceState::Failed => "failed",
        ServiceState::Unavailable => "unavailable",
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone as _;

    use super::*;
    use crate::commands::connect::service::backend::{
        BackendFailure, BackendStatus, InstallRequest, LogRequest, Result as BackendResult,
    };
    use crate::commands::connect::service::manifest::ManifestState;
    use crate::commands::connect::service::model::{ServiceOwner, ServiceScope, ServiceSupervisor};

    struct FakeBackend {
        result: BackendResult<BackendStatus>,
    }

    impl ServiceBackend for FakeBackend {
        fn install(&self, _request: &InstallRequest) -> BackendResult<ServiceManifest> {
            Err(BackendFailure::new("unused", "unused", "unused"))
        }

        fn uninstall(&self, _manifest: &ServiceManifest) -> BackendResult<()> {
            Ok(())
        }

        fn start(&self, _manifest: &ServiceManifest) -> BackendResult<()> {
            Ok(())
        }

        fn stop(&self, _manifest: &ServiceManifest) -> BackendResult<()> {
            Ok(())
        }

        fn restart(&self, _manifest: &ServiceManifest) -> BackendResult<()> {
            Ok(())
        }

        fn status(&self, _manifest: &ServiceManifest) -> BackendResult<BackendStatus> {
            self.result.clone()
        }

        fn logs(&self, _manifest: &ServiceManifest, _request: LogRequest) -> BackendResult<()> {
            Ok(())
        }
    }

    fn manifest() -> ServiceManifest {
        ServiceManifest {
            schema_version: SCHEMA_VERSION,
            directory: PathBuf::from("/srv/edge-analytics"),
            name: "spiced-cloud-connect-edge-analytics-59e8c853e76c15ba.service".to_string(),
            scope: ServiceScope::User,
            backend: ServiceSupervisor::Systemd,
            owner: ServiceOwner {
                name: "edge".to_string(),
                uid: 1000,
            },
            definition_path: PathBuf::from("/home/edge/.config/systemd/user/edge.service"),
            runtime_path: PathBuf::from("/home/edge/.local/share/spice/edge/spiced"),
            log_source: "journald:spiced-cloud-connect-edge.service".to_string(),
            runtime_sha256: "a".repeat(64),
            runtime_version: "v2.2.0".to_string(),
            health_url: Some("http://127.0.0.1:8090/health".to_string()),
            state: ManifestState::Installed,
        }
    }

    #[test]
    fn every_normalized_supervisor_state_reaches_the_shared_model() {
        for state in [
            ServiceState::Starting,
            ServiceState::Running,
            ServiceState::Stopping,
            ServiceState::Stopped,
            ServiceState::Failed,
        ] {
            for starts in [
                ServiceStarts::Disabled,
                ServiceStarts::LoginOnly,
                ServiceStarts::BootWithoutLogin,
            ] {
                let backend = FakeBackend {
                    result: Ok(BackendStatus {
                        state,
                        starts,
                        start_remediation: None,
                    }),
                };
                let status = status_from_manifest(&manifest(), &backend);
                assert_eq!(status.state, state);
                assert_eq!(status.starts, starts);
                assert_eq!(status.supervisor, Some(ServiceSupervisor::Systemd));
            }
        }

        let mut launchd = manifest();
        launchd.backend = ServiceSupervisor::Launchd;
        let backend = FakeBackend {
            result: Ok(BackendStatus {
                state: ServiceState::Running,
                starts: ServiceStarts::LoginOnly,
                start_remediation: None,
            }),
        };
        assert_eq!(
            status_from_manifest(&launchd, &backend).supervisor,
            Some(ServiceSupervisor::Launchd)
        );
    }

    #[test]
    fn a_backend_failure_is_localized_to_the_service_section() {
        let backend = FakeBackend {
            result: Err(BackendFailure::new(
                "supervisor_query_failed",
                "systemctl could not query the unit",
                "Check the user service bus.",
            )),
        };
        let status = status_from_manifest(&manifest(), &backend);
        assert_eq!(status.state, ServiceState::Unavailable);
        assert_eq!(
            status
                .diagnostic
                .as_ref()
                .map(|diagnostic| diagnostic.code.as_str()),
            Some("supervisor_query_failed")
        );
        assert!(status.installed);
    }

    #[test]
    fn full_and_filtered_json_use_byte_equivalent_service_values() {
        let snapshot = fixture_status();
        let full = serde_json::to_value(&snapshot).expect("serialize full status");
        let filtered = serde_json::to_value(snapshot.service_document())
            .expect("serialize filtered service status");
        assert_eq!(
            serde_json::to_vec(&full["service"]).expect("encode full service"),
            serde_json::to_vec(&filtered["service"]).expect("encode filtered service")
        );
    }

    #[test]
    fn schema_v1_fixture_is_exact() {
        let actual = serde_json::to_string_pretty(&fixture_status()).expect("serialize fixture");
        assert_eq!(
            actual,
            include_str!("../../../../tests/fixtures/connect_status_v1.json").trim()
        );
    }

    fn fixture_status() -> ConnectStatus {
        ConnectStatus {
            schema_version: SCHEMA_VERSION,
            observed_at: Utc
                .with_ymd_and_hms(2026, 8, 11, 12, 0, 0)
                .single()
                .expect("valid fixture timestamp"),
            directory: PathBuf::from("/srv/edge-analytics"),
            connection: ConnectionStatus {
                state: ConnectionState::EnrolledOffline,
                instance_id: Some("inst_edge".to_string()),
                organization: Some("acme".to_string()),
                project: Some("edge-analytics".to_string()),
                identity_path: PathBuf::from("/srv/edge-analytics/.spice/identity.json"),
                identity_expires_at: None,
                gateway: Some("gateway.example:443".to_string()),
                new_project_url: None,
                monitor_url: Some("https://spice.ai/acme/edge-analytics/monitor".to_string()),
                diagnostic: None,
            },
            service: ServiceStatus {
                installed: true,
                state: ServiceState::Running,
                scope: Some(ServiceScope::User),
                supervisor: Some(ServiceSupervisor::Systemd),
                starts: ServiceStarts::BootWithoutLogin,
                start_remediation: None,
                owner: Some(ServiceOwner {
                    name: "edge".to_string(),
                    uid: 1000,
                }),
                name: Some(
                    "spiced-cloud-connect-edge-analytics-59e8c853e76c15ba.service".to_string(),
                ),
                working_directory: PathBuf::from("/srv/edge-analytics"),
                definition_path: Some(PathBuf::from(
                    "/home/edge/.config/systemd/user/spiced-cloud-connect-edge-analytics-59e8c853e76c15ba.service",
                )),
                runtime_path: Some(PathBuf::from(
                    "/home/edge/.local/share/spice/cloud-connect/edge-analytics-59e8c853e76c15ba/spiced",
                )),
                log_source: Some(
                    "journald:spiced-cloud-connect-edge-analytics-59e8c853e76c15ba.service"
                        .to_string(),
                ),
                diagnostic: None,
            },
            deployment: DeploymentStatus {
                desired_path: PathBuf::from(
                    "/srv/edge-analytics/.spice/cloud-managed-spicepod.yaml",
                ),
                delivered_secrets: vec!["database_password".to_string()],
                restart_required: Vec::new(),
                diagnostic: None,
            },
        }
    }
}
