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
use manifest::{ServiceManifest, load_validated};

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
            let installed = backend.install(&request).map_err(backend_error)?;
            if let Some(existing) =
                load_validated(config_dir, &request.directory).map_err(manifest_error)?
            {
                existing
                    .ensure_same_service(&installed)
                    .map_err(manifest_error)?;
            }
            manifest::write(config_dir, &installed).map_err(manifest_error)?;
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
            backend.start(manifest).map_err(backend_error)
        }),
        ServiceCommand::Stop => with_manifest(config_dir, directory, |manifest| {
            backend.stop(manifest).map_err(backend_error)
        }),
        ServiceCommand::Restart => with_manifest(config_dir, directory, |manifest| {
            backend.restart(manifest).map_err(backend_error)
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
                .map_err(backend_error)
        }),
    }
}

/// The shared uninstall primitive used both by `service uninstall` and the
/// broader confirmed-release transaction in `connect remove`.
pub(crate) fn uninstall_exact(
    config_dir: &Path,
    directory: &Path,
    backend: &dyn ServiceBackend,
) -> Result<Option<ServiceManifest>> {
    let canonical = canonical_directory(directory)?;
    let Some(manifest) = load_validated(config_dir, &canonical).map_err(manifest_error)? else {
        return Ok(None);
    };
    backend.uninstall(&manifest).map_err(backend_error)?;
    manifest::remove(config_dir).map_err(manifest_error)?;
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
        .map_err(manifest_error)?
        .ok_or_else(|| Error::InvalidArgument {
            message: format!(
                "No Spice Cloud Connect service is installed for {}. Run `spice connect service install` first.",
                canonical.display()
            ),
        })?;
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

fn backend_error(error: backend::BackendFailure) -> Error {
    Error::CloudConnectIo {
        message: format!("{} {}", error.message, error.remediation),
    }
}

fn manifest_error(error: manifest::Error) -> Error {
    Error::CloudConnectIo {
        message: error.to_string(),
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
