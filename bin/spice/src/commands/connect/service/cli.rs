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

//! The `spice connect service` command group.
//!
//! `service` is the canonical spelling everywhere — help, completions, errors,
//! portal copy, documentation. `svc` is a hidden clap alias for interactive
//! typing only; it is deliberately absent from generated help and from every
//! message this CLI prints, so it never becomes a second public spelling.
//!
//! Every action resolves its target from the instance directory (`--dir`, or
//! the current directory) and that directory's manifest. There is no way to
//! name a systemd unit or a launchd label, because a lifecycle command aimed at
//! a service belonging to another instance is worse than one that refuses.

use std::path::Path;

use clap::{Args, Subcommand};

use super::super::status::{self, ConnectStatus};
use super::{ServiceBackend, ServiceManifest, backend};
use crate::context::RuntimeContext;
use crate::error::{Error, Result};
use crate::output::OutputFormat;

/// Default number of log lines printed before following.
const DEFAULT_LOG_LINES: u32 = 100;

/// Ceiling on the initial log line count. A request for more history than a
/// supervisor keeps is a mistake worth naming rather than a request to page
/// through everything on the host.
const MAX_LOG_LINES: u32 = 100_000;

/// The concise help printed by `spice connect service` with no action.
///
/// Hand-written rather than clap's generated help so the group's summary line
/// stays one screen. [`tests::the_no_action_help_lists_every_action`] pins it
/// against the actual set of subcommands.
const SERVICE_HELP: &str = "\
Manage the persistent Spice Cloud Connect service for this instance directory.

  spice connect service install      Install and start the service.
  spice connect service uninstall    Stop and remove it, keeping the Cloud identity.
  spice connect service start        Start an installed, stopped service.
  spice connect service stop         Stop a running service, leaving it installed.
  spice connect service restart      Restart it through the supervisor and wait.
  spice connect service status       Report its state, persistence, and paths.
  spice connect service logs         Print its output (-n <lines>, -f to follow).

Use --dir <path> to act on an instance rooted at another directory.
Docs: https://spiceai.org/docs";

/// Arguments for `spice connect service`.
#[derive(Args, Debug)]
pub struct ServiceArgs {
    /// The action to perform. With none, concise help is printed and nothing
    /// is done.
    #[command(subcommand)]
    pub command: Option<ServiceCommand>,
}

/// The service lifecycle actions.
#[derive(Subcommand, Debug)]
pub enum ServiceCommand {
    /// Install and start the service for this instance directory.
    ///
    /// Idempotent: re-running restages the current runtime, rewrites the
    /// definition, and restarts the service without touching the enrolled
    /// identity.
    Install,

    /// Stop and remove this directory's service, keeping the Cloud identity.
    ///
    /// `spice connect remove` remains the command that releases the identity.
    Uninstall,

    /// Start an installed, stopped service. Succeeds if it is already running.
    Start,

    /// Stop a running service, leaving it installed and enabled.
    Stop,

    /// Restart the service through its supervisor and wait for the result.
    ///
    /// Never asks a running `spiced` to exit itself, and never signals a
    /// foreground runtime.
    Restart,

    /// Report this directory's service state, boot persistence, and paths.
    Status(ServiceStatusArgs),

    /// Print this service's output.
    Logs(LogsArgs),
}

impl ServiceCommand {
    /// The action's name as it appears in help and in error messages.
    fn as_str(&self) -> &'static str {
        match self {
            Self::Install => "install",
            Self::Uninstall => "uninstall",
            Self::Start => "start",
            Self::Stop => "stop",
            Self::Restart => "restart",
            Self::Status(_) => "status",
            Self::Logs(_) => "logs",
        }
    }
}

/// Arguments for `spice connect service status`.
#[derive(Args, Debug)]
pub struct ServiceStatusArgs {
    /// Output format. `json` writes the same service object that
    /// `spice connect status --output json` nests, and writes nothing else to
    /// stdout.
    #[arg(long, short = 'o', value_enum, default_value_t = OutputFormat::Table)]
    pub output: OutputFormat,
}

/// Arguments for `spice connect service logs`.
#[derive(Args, Debug)]
pub struct LogsArgs {
    /// Lines of existing history to print first. `0` with `--follow` prints
    /// only new output.
    #[arg(
        long = "number",
        short = 'n',
        value_name = "LINES",
        default_value_t = DEFAULT_LOG_LINES,
        value_parser = clap::value_parser!(u32).range(0..=i64::from(MAX_LOG_LINES)),
    )]
    pub number: u32,

    /// Keep printing new output until interrupted.
    ///
    /// Spelled `-f, --follow` to match `docker logs` and `kubectl logs`.
    /// `--tail` is intentionally not accepted.
    #[arg(long, short = 'f')]
    pub follow: bool,
}

/// Execute `spice connect service <action>`.
///
/// # Errors
///
/// Returns an error when the action fails, when it needs a service that is not
/// installed, or when the reported state is one automation must not read as
/// healthy.
pub async fn execute(
    ctx: &RuntimeContext,
    args: ServiceArgs,
    instance_dir: &Path,
    config_dir: &Path,
    endpoint: &str,
) -> Result<()> {
    let Some(command) = args.command else {
        println!("{SERVICE_HELP}");
        return Ok(());
    };

    let backend = backend();
    let action = command.as_str();
    match command {
        ServiceCommand::Install => install(ctx, backend, instance_dir, config_dir, endpoint).await,
        ServiceCommand::Uninstall => uninstall(backend, instance_dir, config_dir),
        ServiceCommand::Status(args) => {
            let status = ConnectStatus::collect(backend, instance_dir, config_dir, endpoint).await;
            status::render_service(&status, args.output)?;
            degraded_error(&status)
        }
        ServiceCommand::Start => backend.start(&require_installed(
            backend,
            instance_dir,
            config_dir,
            action,
        )?),
        ServiceCommand::Stop => match resolve_or_report(backend, instance_dir, config_dir, action)?
        {
            Some(manifest) => backend.stop(&manifest),
            None => Ok(()),
        },
        ServiceCommand::Restart => backend.restart(&require_installed(
            backend,
            instance_dir,
            config_dir,
            action,
        )?),
        ServiceCommand::Logs(args) => {
            match resolve_or_report(backend, instance_dir, config_dir, action)? {
                Some(manifest) => backend.logs(
                    &manifest,
                    super::LogRequest {
                        number: args.number,
                        follow: args.follow,
                    },
                ),
                None => Ok(()),
            }
        }
    }
}

/// Install and report what was installed.
async fn install(
    ctx: &RuntimeContext,
    backend: &dyn ServiceBackend,
    instance_dir: &Path,
    config_dir: &Path,
    endpoint: &str,
) -> Result<()> {
    // Resolved, not derived from `$HOME`: `sudo` rewrites `HOME` to `/root`, and
    // the runtime the operator installed is normally under their own home.
    let spiced_path = ctx.resolve_spiced_path().ok_or_else(|| Error::InvalidArgument {
        message: format!(
            "Failed to install the Spice Cloud Connect service: no Spice runtime was found at {}. \
             Install it with `spice install` and re-run `spice connect service install`. \
             See: https://spiceai.org/docs",
            ctx.spiced_path().display()
        ),
    })?;
    let runtime_version = ctx.runtime_version().unwrap_or_default();
    let health_url = format!("{}/health", ctx.http_endpoint().trim_end_matches('/'));

    let manifest = super::install(
        backend,
        instance_dir,
        config_dir,
        &spiced_path,
        &runtime_version,
        &health_url,
    )?;

    // The headline is the state, the scope, and the persistence — the three
    // facts an operator checks — followed by the commands they will actually
    // use. Paths belong in status, which the first of those commands prints.
    let status = ConnectStatus::collect(backend, instance_dir, config_dir, endpoint).await;
    println!(
        "Installed the Spice Cloud Connect service: {} ({} service, {}).",
        status.service.state, manifest.scope, manifest.supervisor
    );
    println!("  starts:      {}", status.service.starts.describe());
    if let Some(action) = &status.service.starts_action {
        println!("               run `{action}` to change that");
    }
    if !manifest.runtime_version.is_empty() {
        println!("  version:     {}", manifest.runtime_version);
    }
    if let Some(monitor) = &status.connection.monitor_url {
        println!("  monitor:     {monitor}");
    }
    println!();
    println!("Manage it with:");
    println!("  spice connect status");
    println!("  spice connect service restart");
    println!("  spice connect service logs -f");
    println!("  spice connect service uninstall");
    Ok(())
}

/// Uninstall and say exactly what was and was not removed.
fn uninstall(backend: &dyn ServiceBackend, instance_dir: &Path, config_dir: &Path) -> Result<()> {
    let Some(manifest) = super::uninstall(backend, instance_dir, config_dir)? else {
        println!(
            "No Spice Cloud Connect service is installed for {}. The Cloud identity and project \
             attachment are untouched.",
            instance_dir.display()
        );
        return Ok(());
    };

    println!("Removed the Spice Cloud Connect service {}.", manifest.name);
    println!(
        "The Cloud identity, project attachment, delivered secrets, and instance files were \
         retained — `spice connect service install` resumes the same enrollment, and `spice run` \
         starts the instance in the foreground. `spice connect remove` is the command that \
         releases the Cloud identity."
    );
    Ok(())
}

/// Resolve the service an action needs, or fail with the action's own
/// remediation.
///
/// Used by the actions that cannot do anything useful without a service:
/// pointing at `install` is more helpful than a bare "not installed".
fn require_installed(
    backend: &dyn ServiceBackend,
    instance_dir: &Path,
    config_dir: &Path,
    action: &str,
) -> Result<ServiceManifest> {
    super::resolve(backend, instance_dir, config_dir)?.ok_or_else(|| Error::ServiceNotInstalled {
        message: format!(
            "Failed to {action} the Spice Cloud Connect service for {}: no supervisor-managed \
             service is installed for this directory. Install one with \
             `spice connect service install`. A `spiced` running in the foreground is not \
             supervisor-managed and is left alone. See: https://spiceai.org/docs",
            instance_dir.display()
        ),
    })
}

/// Resolve the service an action can report `not_installed` for, printing that
/// report when there is none.
///
/// `stop` and `logs` are meaningful answers on a directory with no service —
/// there is nothing running and nothing to read — so they say so and succeed
/// rather than failing.
fn resolve_or_report(
    backend: &dyn ServiceBackend,
    instance_dir: &Path,
    config_dir: &Path,
    action: &str,
) -> Result<Option<ServiceManifest>> {
    let resolved = super::resolve(backend, instance_dir, config_dir)?;
    if resolved.is_none() {
        println!(
            "Spice Cloud Connect service: not_installed ({}). Nothing to {action}; the Cloud \
             identity is untouched.",
            instance_dir.display()
        );
    }
    Ok(resolved)
}

/// Turn a degraded snapshot into the non-zero exit automation needs.
///
/// The report is already on stdout by the time this runs: the diagnosis travels
/// as the process's error on stderr, so a `--output json` run stays parseable.
fn degraded_error(status: &ConnectStatus) -> Result<()> {
    if !status.is_degraded() {
        return Ok(());
    }
    Err(Error::ServiceUnavailable {
        message: format!(
            "The Spice Cloud Connect service for {} is {}{}",
            status.connection.directory.display(),
            status.service.state,
            match &status.service.diagnostic {
                Some(diagnostic) => format!(": {diagnostic}"),
                None => ".".to_string(),
            }
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_no_action_help_lists_every_action() {
        // The help is hand-written, so it has to be pinned against the real
        // set of actions or it will silently go stale.
        for action in [
            "install",
            "uninstall",
            "start",
            "stop",
            "restart",
            "status",
            "logs",
        ] {
            assert!(
                SERVICE_HELP.contains(&format!("spice connect service {action}")),
                "the no-action help must name `{action}`"
            );
        }
        // `svc` is a typing alias, never documented copy.
        assert!(!SERVICE_HELP.contains("svc"), "{SERVICE_HELP}");
    }

    #[test]
    fn action_names_match_the_documented_grammar() {
        assert_eq!(ServiceCommand::Install.as_str(), "install");
        assert_eq!(ServiceCommand::Uninstall.as_str(), "uninstall");
        assert_eq!(ServiceCommand::Start.as_str(), "start");
        assert_eq!(ServiceCommand::Stop.as_str(), "stop");
        assert_eq!(ServiceCommand::Restart.as_str(), "restart");
        assert_eq!(
            ServiceCommand::Status(ServiceStatusArgs {
                output: OutputFormat::Table
            })
            .as_str(),
            "status"
        );
        assert_eq!(
            ServiceCommand::Logs(LogsArgs {
                number: DEFAULT_LOG_LINES,
                follow: false
            })
            .as_str(),
            "logs"
        );
    }
}
