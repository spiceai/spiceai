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

//! The `spice cloud service` command group.
//!
//! `service` is the canonical spelling everywhere — help, completions, errors,
//! portal copy, documentation. `svc` is a hidden clap alias for interactive
//! typing only; it is deliberately absent from generated help and from every
//! message this CLI prints, so it never becomes a second public spelling.
//!
//! Every action resolves its target from the current instance directory and
//! that directory's manifest. There is no way to
//! name a systemd unit or a launchd label, because a lifecycle command aimed at
//! a service belonging to another instance is worse than one that refuses.

use std::path::Path;

use clap::{Args, Subcommand};

use super::{ServiceBackend, ServiceManifest, backend};
use crate::context::RuntimeContext;
use crate::error::{Error, Result};

/// The concise help printed by `spice cloud service` with no action.
///
/// Hand-written rather than clap's generated help so the group's summary line
/// stays one screen. [`tests::the_no_action_help_lists_every_action`] pins it
/// against the actual set of subcommands.
const SERVICE_HELP: &str = "\
Manage the persistent Spice Cloud Connect service for this instance directory.

  spice cloud service install      Install and start the service.
  spice cloud service uninstall    Stop and remove it, keeping the Cloud identity.
  spice cloud service start        Start an installed, stopped service.
  spice cloud service stop         Stop a running service, leaving it installed.
  spice cloud service restart      Restart it through the supervisor and wait.

Docs: https://spiceai.org/docs";

/// Arguments for `spice cloud service`.
#[derive(Args, Debug, Clone)]
pub struct ServiceArgs {
    /// The action to perform. With none, concise help is printed and nothing
    /// is done.
    #[command(subcommand)]
    pub command: Option<ServiceCommand>,
}

/// The service lifecycle actions.
#[derive(Subcommand, Debug, Clone)]
pub enum ServiceCommand {
    /// Install and start the service for this instance directory.
    ///
    /// Idempotent: re-running restages the current runtime, rewrites the
    /// definition, and restarts the service without touching the enrolled
    /// identity.
    Install,

    /// Stop and remove this directory's service, keeping the Cloud identity.
    ///
    /// `spice cloud unlink` remains the command that releases the identity.
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
        }
    }
}

/// Execute `spice cloud service <action>`.
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
) -> Result<()> {
    let Some(command) = args.command else {
        println!("{SERVICE_HELP}");
        return Ok(());
    };
    ensure_service_lifecycle_supported()?;

    let backend = backend();
    let action = command.as_str();
    let mutation_lock = Some(
        runtime_cloud_connect::MutationLock::acquire(config_dir, action)
            .await
            .map_err(|source| Error::CloudConnectIo {
                message: format!("acquire Cloud Connect state for service {action}: {source}"),
            })?,
    );
    let locked_dirs = if let Some(lock) = mutation_lock.as_ref() {
        let service_config_dir = tokio::fs::canonicalize(config_dir)
            .await
            .map_err(|source| Error::CloudConnectIo {
                message: format!(
                    "resolve the locked Cloud Connect config directory {}: {source}",
                    config_dir.display()
                ),
            })?;
        lock.ensure_directory_stable()
            .map_err(|source| Error::CloudConnectIo {
                message: format!(
                    "validate locked Cloud Connect state for service {action}: {source}"
                ),
            })?;
        let identity_config_dir =
            lock.descriptor_relative_config_dir()
                .map_err(|source| Error::CloudConnectIo {
                    message: format!(
                        "pin locked Cloud Connect state for service {action}: {source}"
                    ),
                })?;
        Some((service_config_dir, identity_config_dir))
    } else {
        None
    };
    let (service_config_dir, identity_config_dir) = locked_dirs.as_ref().map_or(
        (config_dir, config_dir),
        |(service_config_dir, identity_config_dir)| {
            (service_config_dir.as_path(), identity_config_dir.as_path())
        },
    );
    // Manifest state resolves through the descriptor the lock retains, not
    // through a name (see `PinnedConfigDir`).
    //
    // `identity_config_dir` above is still a pathname on a platform without
    // Linux's descriptor-rooted traversal, so identity validation and the
    // manifest can resolve to different directories in one install. Closing
    // that needs descriptor-relative reads through the whole state layer: #13291.
    let state_config_dir = match mutation_lock.as_ref() {
        Some(lock) => super::PinnedConfigDir::for_lock(service_config_dir, lock)?,
        None => super::PinnedConfigDir::unlocked(config_dir),
    };
    match command {
        ServiceCommand::Install => {
            install(
                ctx,
                backend,
                instance_dir,
                service_config_dir,
                identity_config_dir,
                state_config_dir,
            )
            .await
        }
        ServiceCommand::Uninstall => {
            uninstall(backend, instance_dir, service_config_dir, &state_config_dir)
        }
        ServiceCommand::Start => {
            let manifest = require_installed(
                backend,
                instance_dir,
                service_config_dir,
                &state_config_dir,
                action,
            )?;
            with_recovery_detail(backend, &manifest, backend.start(&manifest))
        }
        ServiceCommand::Stop => match resolve_or_report(
            backend,
            instance_dir,
            service_config_dir,
            &state_config_dir,
            action,
        )? {
            Some(manifest) => with_recovery_detail(backend, &manifest, backend.stop(&manifest)),
            None => Ok(()),
        },
        ServiceCommand::Restart => {
            let manifest = require_installed(
                backend,
                instance_dir,
                service_config_dir,
                &state_config_dir,
                action,
            )?;
            with_recovery_detail(backend, &manifest, backend.restart(&manifest))
        }
    }
}

/// Print logs from the installed local service, returning whether a service
/// was present for this directory.
///
/// # Errors
///
/// Returns an error when service state or its configured log source cannot be
/// read.
pub(crate) fn print_local_logs(
    instance_dir: &Path,
    config_dir: &Path,
    number: u32,
    follow: bool,
) -> Result<bool> {
    let backend = backend();
    let state = super::PinnedConfigDir::unlocked(config_dir);
    let Some(manifest) = super::resolve_with_state(backend, instance_dir, &state, config_dir)?
    else {
        return Ok(false);
    };
    let request = super::LogRequest {
        number,
        follow,
        capture: false,
    };
    with_recovery_detail(backend, &manifest, backend.logs(&manifest, request))?;
    Ok(true)
}

/// Read bounded logs from the installed local service, returning `None` when
/// this directory has no service.
///
/// # Errors
///
/// Returns an error when service state or its configured log source cannot be
/// read.
pub(crate) fn read_local_logs(
    instance_dir: &Path,
    config_dir: &Path,
    number: u32,
) -> Result<Option<Vec<String>>> {
    let backend = backend();
    let state = super::PinnedConfigDir::unlocked(config_dir);
    let Some(manifest) = super::resolve_with_state(backend, instance_dir, &state, config_dir)?
    else {
        return Ok(None);
    };
    let request = super::LogRequest {
        number,
        follow: false,
        capture: true,
    };
    with_recovery_detail(backend, &manifest, backend.logs(&manifest, request))
}

/// Refuse a lifecycle action on a host with no supervisor this release drives.
///
/// Status and logs are exposed by their Cloud commands and do not pass through
/// this lifecycle-only boundary.
#[cfg_attr(
    any(target_os = "linux", target_os = "macos"),
    expect(
        clippy::unnecessary_wraps,
        reason = "the cross-platform command boundary returns Result so unsupported targets can reject the lifecycle"
    )
)]
fn ensure_service_lifecycle_supported() -> Result<()> {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        Ok(())
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        Err(Error::InvalidArgument {
            message: format!(
                "`spice cloud service` supports Linux with systemd and macOS with launchd (this host is {}). Run `spiced` from the enrolled directory under your own supervisor instead. See: https://spiceai.org/docs",
                std::env::consts::OS
            ),
        })
    }
}

/// Install and report what was installed.
async fn install(
    ctx: &RuntimeContext,
    backend: &dyn ServiceBackend,
    instance_dir: &Path,
    service_config_dir: &Path,
    identity_config_dir: &Path,
    state_config_dir: super::PinnedConfigDir,
) -> Result<()> {
    let identity = validate_service_identity(identity_config_dir).await?;

    // Resolved, not derived from `$HOME`: `sudo` rewrites `HOME` to `/root`, and
    // the runtime the operator installed is normally under their own home.
    let spiced_path = ctx
        .resolve_spiced()?
        .ok_or_else(|| Error::InvalidArgument {
            message: crate::context::runtime_not_selected_message(
                "install the Spice Cloud Connect service",
                &ctx.spiced_path(),
            ),
        })?
        .into_path();
    // Propagated, not defaulted: the manifest records the version installed, so
    // a lookup that failed has to fail the install rather than publish a
    // manifest that claims no version at all.
    //
    // Resolved and probed as the account the service will run as, not as this
    // process: the runtime the operator installed is normally under their own
    // home, so under `sudo` reading its version here as root would execute a
    // user-writable binary with full privilege and the caller's environment.
    let service_owner = super::install_owner(
        instance_dir,
        super::root_fallback_for(super::backend().supervisor()),
    )?;
    let runtime_version = super::probe_runtime_version(&spiced_path, &service_owner)?;
    let health_url = format!("{}/health", ctx.http_endpoint().trim_end_matches('/'));

    // Staging files, invoking systemd, and polling the blocking health gate are
    // deliberately synchronous inside the service layer. Keep that work off a
    // Tokio worker so the CLI's other async tasks remain responsive while an
    // install waits for the runtime to settle.
    let install_instance_dir = instance_dir.to_path_buf();
    let install_service_config_dir = service_config_dir.to_path_buf();
    let manifest = tokio::task::spawn_blocking(move || {
        super::install_with_state(
            super::backend(),
            &install_instance_dir,
            &install_service_config_dir,
            &state_config_dir,
            &spiced_path,
            &runtime_version,
            &health_url,
        )
    })
    .await
    .map_err(|source| Error::CloudConnectIo {
        message: format!("wait for the service installer task: {source}"),
    })??;

    // The headline is the state, the scope, and the persistence — the three
    // facts an operator checks — followed by the commands they will actually
    // use. Paths belong in status, which the first of those commands prints.
    let observation = backend.observe(&manifest);
    println!(
        "Installed the Spice Cloud Connect service: {} ({} service, {}).",
        observation.state, manifest.scope, manifest.supervisor
    );
    println!("  starts:      {}", observation.starts.describe());
    if let Some(action) = &observation.starts_action {
        println!("               run `{action}` to change that");
    }
    if !manifest.runtime_version.is_empty() {
        println!("  version:     {}", manifest.runtime_version);
    }
    if let Some(monitor) = identity.monitor_url() {
        println!("  monitor:     {monitor}");
    }
    println!();
    println!("Manage it with:");
    println!("  spice cloud status");
    println!("  spice cloud service restart");
    println!("  spice cloud logs -f");
    println!("  spice cloud service uninstall");
    Ok(())
}

/// Require the exact durable identity the installed service can use.
///
/// The unit persists only `SPICE_CONFIG_DIR`, so an override inherited from
/// the installing shell cannot make an otherwise unusable identity safe to
/// install. The runtime will make the same decision after the first restart.
async fn validate_service_identity(
    config_dir: &Path,
) -> Result<runtime_cloud_connect::ReconnectableIdentity> {
    let mut config = runtime_cloud_connect::CloudConnectConfig::from_env_at(
        env!("CARGO_PKG_VERSION"),
        config_dir.to_path_buf(),
    );
    config.gateway_endpoint = None;

    match runtime_cloud_connect::load_reconnectable_identity_async(&config).await {
        Ok(Some(identity)) => Ok(identity),
        Ok(None) => Err(Error::InvalidArgument {
            message: format!(
                "Failed to install the Spice Cloud Connect service: {} has no enrolled identity. \
                 Mint an enrollment key in the Spice Cloud portal and start the runtime with \
                 `spiced --token <enrollment-key>` before installing the service. \
                 See: https://spiceai.org/docs",
                config.identity_path.display()
            ),
        }),
        Err(error) => Err(Error::CloudConnectIo {
            message: format!(
                "validate the durable Cloud Connect identity before installing the service: {error}"
            ),
        }),
    }
}

/// Uninstall and say exactly what was and was not removed.
fn uninstall(
    backend: &dyn ServiceBackend,
    instance_dir: &Path,
    service_config_dir: &Path,
    state_config_dir: &super::PinnedConfigDir,
) -> Result<()> {
    let Some(manifest) =
        super::uninstall_with_state(backend, instance_dir, state_config_dir, service_config_dir)?
    else {
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
         retained — `spice cloud service install` resumes the same enrollment, and \
         `spice run` starts the instance in the foreground. `spice cloud unlink` is the \
         command that releases the Cloud identity."
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
    service_config_dir: &Path,
    state_config_dir: &super::PinnedConfigDir,
    action: &str,
) -> Result<ServiceManifest> {
    super::resolve_with_state(backend, instance_dir, state_config_dir, service_config_dir)?
        .ok_or_else(|| Error::ServiceNotInstalled {
            message: format!(
                "Failed to {action} the Spice Cloud Connect service for {}: no supervisor-managed \
             service is installed for this directory. Install one with \
             `spice cloud service install`. A `spiced` running in the foreground is not \
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
    service_config_dir: &Path,
    state_config_dir: &super::PinnedConfigDir,
    action: &str,
) -> Result<Option<ServiceManifest>> {
    let resolved =
        super::resolve_with_state(backend, instance_dir, state_config_dir, service_config_dir)?;
    if resolved.is_none() {
        println!(
            "Spice Cloud Connect service: not_installed ({}). Nothing to {action}; the Cloud \
             identity is untouched.",
            instance_dir.display()
        );
    }
    Ok(resolved)
}

/// Name the supervisor's own commands for this service when a Spice command
/// could not complete.
///
/// Recovery detail, never the primary interface: the hints are printed only on
/// a failure, on stderr so a `--output json` run stays parseable, and ahead of
/// the diagnosis the process exits with. An interruption is not a failure —
/// the viewer stopped and the service is unchanged — so it gets none.
fn with_recovery_detail<T>(
    backend: &dyn ServiceBackend,
    manifest: &ServiceManifest,
    result: Result<T>,
) -> Result<T> {
    if matches!(result, Err(ref err) if !matches!(err, Error::Interrupted)) {
        let hints = backend.recovery_hints(manifest);
        if !hints.is_empty() {
            eprintln!(
                "If you need to drive {} directly, its supervisor's own commands are:",
                manifest.name
            );
            for hint in hints {
                eprintln!("  {hint}");
            }
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_no_action_help_lists_every_action() {
        // The help is hand-written, so it has to be pinned against the real
        // set of actions or it will silently go stale.
        for action in ["install", "uninstall", "start", "stop", "restart"] {
            assert!(
                SERVICE_HELP.contains(&format!("spice cloud service {action}")),
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
    }
}
