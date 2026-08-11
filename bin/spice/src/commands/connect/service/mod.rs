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

//! Public `spice connect service` grammar and the manifest-scoped lifecycle
//! seam. Native systemd and launchd implementations stack behind
//! [`backend::ServiceBackend`]; this module owns argument and persistence
//! semantics shared by both.

mod backend;
mod manifest;
mod model;
mod status;

use std::path::{Path, PathBuf};

use clap::{Args, Subcommand};

use crate::context::RuntimeContext;
use crate::error::{Error, Result};
use crate::output::{OutputFormat, write_json};

use backend::{InstallRequest, LogRequest};
pub(crate) use backend::{PlatformBackend, ServiceBackend};
use manifest::{ManifestState, ServiceManifest, load_validated};

/// Arguments below the public `service` group.
#[derive(Args)]
pub struct ServiceArgs {
    #[command(subcommand)]
    pub(crate) command: Option<ServiceCommand>,
}

/// Lifecycle actions shared by systemd and launchd.
#[derive(Subcommand)]
pub enum ServiceCommand {
    /// Install, enable, and start a service for this directory.
    Install(ServiceInstallArgs),
    /// Stop and remove the service while retaining Cloud identity.
    Uninstall,
    /// Start an installed service without changing persistence.
    Start,
    /// Stop an installed service without changing persistence.
    Stop,
    /// Restart an installed service and wait for health.
    Restart,
    /// Show the normalized service state.
    Status(ServiceStatusArgs),
    /// Read service logs.
    Logs(ServiceLogsArgs),
}

/// Enrollment inputs accepted by service installation. Authentication and
/// project preparation happen outside privileged backend implementations.
#[derive(Args)]
pub struct ServiceInstallArgs {
    #[arg(long, value_name = "SLUG")]
    pub(crate) org: Option<String>,

    #[arg(long, value_name = "SLUG")]
    pub(crate) project: Option<String>,

    #[arg(long, value_name = "SECRET")]
    pub(crate) token: Option<EnrollmentToken>,

    #[arg(long, value_name = "LABEL")]
    pub(crate) region: Option<String>,
}

/// Secret input whose debug representation can never expose the enrollment
/// key through clap argument logging.
#[derive(Clone)]
pub struct EnrollmentToken(String);

impl std::str::FromStr for EnrollmentToken {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        if value.trim().is_empty() {
            return Err("the enrollment key cannot be empty".to_string());
        }
        Ok(Self(value.to_string()))
    }
}

impl std::fmt::Debug for EnrollmentToken {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("<redacted>")
    }
}

impl EnrollmentToken {
    #[must_use]
    pub(crate) fn expose(&self) -> &str {
        &self.0
    }
}

#[derive(Args)]
pub struct ServiceStatusArgs {
    /// Output format.
    #[arg(long, short = 'o', default_value = "table")]
    pub(crate) output: OutputFormat,
}

#[derive(Args)]
pub struct ServiceLogsArgs {
    /// Number of existing log records to print before returning or following.
    #[arg(
        long,
        short = 'n',
        value_name = "LINES",
        default_value_t = 100,
        value_parser = clap::value_parser!(u32).range(0..=100_000)
    )]
    pub(crate) number: u32,

    /// Keep following new records until interrupted.
    #[arg(long, short = 'f')]
    pub(crate) follow: bool,
}

impl ServiceArgs {
    pub(crate) fn output_mut(&mut self) -> Option<&mut OutputFormat> {
        match &mut self.command {
            Some(ServiceCommand::Status(args)) => Some(&mut args.output),
            _ => None,
        }
    }

    #[must_use]
    pub(crate) fn produces_json(&self) -> bool {
        matches!(
            &self.command,
            Some(ServiceCommand::Status(ServiceStatusArgs {
                output: OutputFormat::Json
            }))
        )
    }
}

pub(crate) async fn execute(
    context: &RuntimeContext,
    args: ServiceArgs,
    config_dir: &Path,
    directory: &Path,
    backend: &dyn ServiceBackend,
) -> Result<()> {
    let Some(command) = args.command else {
        print_action_help();
        return Ok(());
    };

    match command {
        ServiceCommand::Install(args) => {
            // The enrollment lane consumes these outside the privileged
            // backend. Reading them here keeps this seam explicit without
            // allowing a backend implementation to gain access to a token.
            let _enrollment = (
                args.org.as_deref(),
                args.project.as_deref(),
                args.token.as_ref().map(EnrollmentToken::expose),
                args.region.as_deref(),
            );
            let source_runtime = context.resolve_spiced_path().ok_or_else(|| {
                Error::InvalidArgument {
                    message: "Failed to install the Spice Cloud Connect service: no Spice runtime was found. Run `spice install` and retry `spice connect service install`.".to_string(),
                }
            })?;
            let request = InstallRequest {
                directory: canonical_directory(directory)?,
                config_directory: config_dir.to_path_buf(),
                source_runtime,
            };
            install_exact(config_dir, &request, backend)?;
            println!("Spice Cloud Connect service: installed and running");
            Ok(())
        }
        ServiceCommand::Uninstall => {
            let removed = uninstall_exact(config_dir, directory, backend)?;
            if removed.is_some() {
                println!(
                    "Service removed. Cloud identity and any project attachment were retained."
                );
                println!("  spice connect service install");
                println!("  spice run");
            } else {
                println!("Spice Cloud Connect service: not installed.");
            }
            Ok(())
        }
        ServiceCommand::Start => with_manifest(config_dir, directory, |manifest| {
            backend.start(manifest).map_err(Error::from)
        }),
        ServiceCommand::Stop => with_manifest(config_dir, directory, |manifest| {
            backend.stop(manifest).map_err(Error::from)
        }),
        ServiceCommand::Restart => with_manifest(config_dir, directory, |manifest| {
            backend.restart(manifest).map_err(Error::from)
        }),
        ServiceCommand::Status(args) => {
            let snapshot = status::collect(context, config_dir, directory, backend).await;
            match args.output {
                OutputFormat::Table => status::render_service_human(&snapshot),
                OutputFormat::Json => write_json(&snapshot.service_document())?,
            }
            if snapshot.service.state == model::ServiceState::Unavailable
                || snapshot.service.diagnostic.is_some()
            {
                return Err(Error::ReportedStatusFailure);
            }
            Ok(())
        }
        ServiceCommand::Logs(args) => with_manifest(config_dir, directory, |manifest| {
            backend
                .logs(
                    manifest,
                    LogRequest {
                        number: args.number,
                        follow: args.follow,
                    },
                )
                .map_err(Error::from)
        }),
    }
}

fn install_exact(
    config_dir: &Path,
    request: &InstallRequest,
    backend: &dyn ServiceBackend,
) -> Result<ServiceManifest> {
    let installed = backend.plan_install(request).map_err(Error::from)?;
    manifest::validate_install_plan(config_dir, &request.directory, &installed)
        .map_err(Error::from)?;
    if let Some(existing) = load_validated(config_dir, &request.directory).map_err(Error::from)? {
        existing
            .ensure_same_service(&installed)
            .map_err(Error::from)?;
    }
    backend.install(request, &installed).map_err(Error::from)?;
    manifest::write(config_dir, &installed).map_err(Error::from)?;
    Ok(installed)
}

/// The shared uninstall primitive used both by `service uninstall` and the
/// broader confirmed-release transaction in `connect remove`.
pub(crate) fn uninstall_exact(
    config_dir: &Path,
    directory: &Path,
    backend: &dyn ServiceBackend,
) -> Result<Option<ServiceManifest>> {
    let canonical = canonical_directory(directory)?;
    let Some(manifest) = load_validated(config_dir, &canonical).map_err(Error::from)? else {
        return Ok(None);
    };
    if manifest.state == ManifestState::Uninstalled {
        return Ok(None);
    }
    backend.uninstall(&manifest).map_err(Error::from)?;
    manifest::remove(config_dir).map_err(Error::from)?;
    Ok(Some(manifest))
}

pub(crate) async fn collect_status(
    context: &RuntimeContext,
    config_dir: &Path,
    directory: &Path,
    backend: &dyn ServiceBackend,
) -> model::ConnectStatus {
    status::collect(context, config_dir, directory, backend).await
}

pub(crate) fn render_status_human(status: &model::ConnectStatus) {
    status::render_human(status);
}

fn with_manifest(
    config_dir: &Path,
    directory: &Path,
    operation: impl FnOnce(&ServiceManifest) -> Result<()>,
) -> Result<()> {
    let canonical = canonical_directory(directory)?;
    let manifest = load_validated(config_dir, &canonical)
        .map_err(Error::from)?
        .ok_or_else(|| Error::InvalidArgument {
            message: format!(
                "No Spice Cloud Connect service is installed for {}. Run `spice connect service install` first.",
                canonical.display()
            ),
        })?;
    if manifest.state == ManifestState::Uninstalled {
        return Err(Error::InvalidArgument {
            message: format!(
                "No Spice Cloud Connect service is installed for {}. Run `spice connect service install` first.",
                canonical.display()
            ),
        });
    }
    operation(&manifest)
}

fn canonical_directory(directory: &Path) -> Result<PathBuf> {
    std::fs::canonicalize(directory).map_err(|source| Error::CloudConnectIo {
        message: format!(
            "canonicalize instance directory {} before a service operation: {source}",
            directory.display()
        ),
    })
}

impl From<backend::BackendFailure> for Error {
    fn from(error: backend::BackendFailure) -> Self {
        Self::CloudConnectIo {
            message: format!("{} {}", error.message, error.remediation),
        }
    }
}

impl From<manifest::Error> for Error {
    fn from(error: manifest::Error) -> Self {
        Self::CloudConnectIo {
            message: error.to_string(),
        }
    }
}

fn print_action_help() {
    println!("Manage the Spice Cloud Connect service for one instance directory.");
    println!();
    println!("Usage: spice connect service <ACTION> [OPTIONS]");
    println!();
    println!("Actions:");
    println!("  install    Install, enable, and start the service");
    println!("  uninstall  Remove the service and retain Cloud identity");
    println!("  start      Start an installed service");
    println!("  stop       Stop an installed service");
    println!("  restart    Restart an installed service");
    println!("  status     Show normalized service status");
    println!("  logs       Read or follow service logs");
}

#[cfg(all(test, unix))]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::backend::{BackendFailure, BackendStatus, Result as BackendResult};
    use super::manifest::{ManifestState, service_name_for_dir};
    use super::model::{SCHEMA_VERSION, ServiceOwner, ServiceScope, ServiceSupervisor};
    use super::*;

    struct MutationSentinelBackend {
        plan: ServiceManifest,
        mutated: AtomicBool,
        uninstalled: AtomicBool,
    }

    impl ServiceBackend for MutationSentinelBackend {
        fn plan_install(&self, _request: &InstallRequest) -> BackendResult<ServiceManifest> {
            Ok(self.plan.clone())
        }

        fn install(
            &self,
            _request: &InstallRequest,
            _manifest: &ServiceManifest,
        ) -> BackendResult<()> {
            self.mutated.store(true, Ordering::SeqCst);
            Ok(())
        }

        fn uninstall(&self, _manifest: &ServiceManifest) -> BackendResult<()> {
            self.uninstalled.store(true, Ordering::SeqCst);
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
            Err(BackendFailure::new("unused", "unused", "unused"))
        }

        fn logs(&self, _manifest: &ServiceManifest, _request: LogRequest) -> BackendResult<()> {
            Ok(())
        }
    }

    fn test_manifest(directory: &Path, scope: ServiceScope) -> ServiceManifest {
        let name = match service_name_for_dir(directory, ServiceSupervisor::Systemd) {
            Ok(name) => name,
            Err(error) => panic!("derive service name: {error}"),
        };
        ServiceManifest {
            schema_version: SCHEMA_VERSION,
            directory: directory.to_path_buf(),
            name,
            scope,
            backend: ServiceSupervisor::Systemd,
            owner: ServiceOwner {
                name: "test-owner".to_string(),
                uid: nix::unistd::Uid::effective().as_raw(),
            },
            definition_path: directory.join("definition.service"),
            runtime_path: directory.join("spiced"),
            log_source: "journald:test".to_string(),
            runtime_sha256: "a".repeat(64),
            runtime_version: "v2.2.0".to_string(),
            health_url: Some("http://127.0.0.1:8090/health".to_string()),
            state: ManifestState::Installed,
        }
    }

    #[test]
    fn manifest_collision_is_rejected_before_backend_mutation() {
        let instance = tempfile::TempDir::new().expect("create instance directory");
        let directory = std::fs::canonicalize(instance.path()).expect("canonical directory");
        let config_dir = directory.join(".spice");
        let existing = test_manifest(&directory, ServiceScope::User);
        manifest::write(&config_dir, &existing).expect("write existing manifest");

        let backend = MutationSentinelBackend {
            plan: test_manifest(&directory, ServiceScope::System),
            mutated: AtomicBool::new(false),
            uninstalled: AtomicBool::new(false),
        };
        let request = InstallRequest {
            directory,
            config_directory: config_dir.clone(),
            source_runtime: instance.path().join("source-spiced"),
        };

        let error = install_exact(&config_dir, &request, &backend)
            .expect_err("different service ownership must collide");
        assert!(
            error.to_string().contains("service_name_collision"),
            "{error}"
        );
        assert!(
            !backend.mutated.load(Ordering::SeqCst),
            "collision must be detected before backend mutation"
        );
    }

    #[test]
    fn shared_uninstall_removes_only_service_state() {
        let instance = tempfile::TempDir::new().expect("create instance directory");
        let directory = std::fs::canonicalize(instance.path()).expect("canonical directory");
        let config_dir = directory.join(".spice");
        let existing = test_manifest(&directory, ServiceScope::User);
        manifest::write(&config_dir, &existing).expect("write existing manifest");
        let identity_path = config_dir.join(runtime_cloud_connect::config::IDENTITY_FILE);
        std::fs::write(&identity_path, b"identity-owned-by-connect-remove")
            .expect("write identity sentinel");
        let backend = MutationSentinelBackend {
            plan: existing,
            mutated: AtomicBool::new(false),
            uninstalled: AtomicBool::new(false),
        };

        let removed = uninstall_exact(&config_dir, &directory, &backend)
            .expect("uninstall exact service")
            .expect("installed service was removed");
        assert!(backend.uninstalled.load(Ordering::SeqCst));
        assert_eq!(removed.directory, directory);
        assert!(!config_dir.join(manifest::MANIFEST_FILE).exists());
        assert_eq!(
            std::fs::read(identity_path).expect("read retained identity sentinel"),
            b"identity-owned-by-connect-remove"
        );
    }
}
