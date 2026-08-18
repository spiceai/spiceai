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

//! `spice connect` — enroll this directory with Spice Cloud, create its
//! project transactionally, and manage the resulting instance.
//!
//! Two distinct use cases share this command:
//!
//! 1. **Cloud Connect instance state** (remote management of `spiced` from
//!    Spice Cloud). Enrollment itself is performed by the runtime: mint an
//!    enrollment key in the Spice Cloud portal and start the runtime with
//!    it (`spiced --token <enrollment-key>`); a directory with an enrolled
//!    identity reconnects automatically on every later start. Bare
//!    `spice connect` starts that instance — in the foreground, or through
//!    the installed service when there is one — and the subcommands manage
//!    it: `status` reports it, `service` installs and manages the
//!    persistent service that keeps it running, and `remove` releases the
//!    instance and clears it.
//!
//! 2. **Deprecated pod-add behavior**: when the argument is a Spicepod
//!    path on Spice.ai Cloud (e.g. `spiceai/quickstart`), this prints a
//!    deprecation notice and behaves like `spice add <pod>` with Spice.ai
//!    Cloud authentication headers.

mod naming;
mod project;
mod service;
mod state;
mod status;
mod transaction;

use std::{
    io::IsTerminal as _,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::commands::add::{AddArgs, execute_add_or_connect};
use crate::commands::cloud::org as cloud_org;
use crate::commands::cloud::{CloudClient, ProjectTarget};
use crate::context::RuntimeContext;
use crate::error::{CloudErrorCode, Error, Result};
use crate::output::OutputFormat;
use clap::{Args, Subcommand};
use runtime_cloud_connect::config::{CloudConnectConfig, IDENTITY_FILE};
use secrecy::{ExposeSecret as _, SecretString};

use service::ServiceState;
use status::ConnectStatus;

/// File (relative to the config dir) holding a `--endpoint` override so later
/// `spiced` starts reach the same control plane the enroll did.
///
/// Shared with the runtime, which removes this override on release: defining the
/// name on each side independently would let a rename here leave a persisted
/// override behind for the next enrollment to pick up.
const CLOUD_ENDPOINT_FILE: &str = CloudConnectConfig::ENDPOINT_OVERRIDE_FILE;

/// Arguments for the `spice connect` command.
#[derive(Args, Debug)]
#[command(
    about = "Connect this directory to Spice Cloud and start its instance",
    long_about = r#"`spice connect` enrolls this directory with Spice Cloud and manages its instance.

This is an interactive setup flow. It authenticates a user, resolves one
owner/admin organization, enrolls the local instance, and atomically creates
and attaches a new project. Without a login, it offers inline login
(recommended) or secure enrollment-key entry.

The transaction is retry-safe: an interrupted enrollment reuses its durable
operation and key material, while project creation uses the enrolled instance's
single attachment as its exact replay key. Existing identities always win and
are never duplicated. A re-run continues the pending enrollment in the mode that
started it — it never asks which authentication to use again, and an enrollment
key is asked for again only because keys are never stored.

For unattended enrollment, run `spiced --token <enrollment-key>`. The runtime
enrolls into the organization authorized by the key; project creation and
attachment happen separately in Spice Cloud.

  spice connect status                    Show this directory's Cloud
                                          connection, service, and deployment
                                          state. `--output json` prints the
                                          same report for automation.
  spice connect service install           Install and start `spiced` as a
                                          persistent service so the instance
                                          survives reboots and closed
                                          terminals. Re-run to upgrade in
                                          place: latest binary, rewritten
                                          service definition, service
                                          restarted, identity untouched.
  spice connect service ...               The rest of the service lifecycle:
                                          uninstall, start, stop, restart,
                                          status, logs.
  spice connect remove                    Delete this instance's project using
                                          the logged-in user session, uninstall
                                          its service, and clear local Cloud
                                          state. Stop any foreground or managed
                                          `spiced` instance first; removal
                                          refuses while it is running. Use
                                          `--force` only to recover local state
                                          when Cloud cleanup cannot complete.

Use `--dir <path>` to manage an instance rooted at a different directory:
per-instance state lives under `<dir>/.spice`, so multiple instances on one
host enroll independently. Until managed services accept per-instance HTTP
and Flight endpoints, only one Spice-managed service can be installed on a
host; run additional instances in the foreground or under your own supervisor
with unique endpoints. `SPICE_CONFIG_DIR` overrides the derived location
entirely and wins over `--dir`.

The managed service lifecycle initially supports Linux with systemd. Containers
pass the enrollment key directly to the runtime (`spiced --token`) under the
container runtime's restart policy. Other platforms run under an
operator-managed foreground process or supervisor.

DEPRECATED POD-ADD BEHAVIOR:
  spice connect <org>/<pod>               Deprecated; use `spice add <org>/<pod>`.

EXAMPLES
  spice connect
  spice connect --dir /srv/edge --region us-west-2
  spice connect status
  spice connect status --output json
  sudo spice connect service install
  sudo spice connect remove

Docs: https://spiceai.org/docs"#
)]
pub struct ConnectArgs {
    /// Optional explicit subcommand. If absent, the first positional
    /// argument (`target`) selects the deprecated pod-add behavior.
    #[command(subcommand)]
    pub command: Option<ConnectCommand>,

    /// A Spicepod path (`<org>/<pod>`) for the deprecated pod-add behavior.
    /// Secrets are never accepted positionally: enrollment keys go to the
    /// runtime as `spiced --token <enrollment-key>`.
    #[arg(value_name = "TARGET")]
    pub target: Option<ConnectTarget>,

    /// Declared location label for the enrolled instance.
    #[arg(long, value_name = "LABEL", global = true)]
    pub region: Option<String>,

    /// Override the Spice Cloud endpoint used when inspecting state or
    /// reporting a release. Defaults to `https://api.spice.ai`. Also
    /// configurable via `SPICE_CLOUD_ENDPOINT`.
    #[arg(long, value_name = "URL", global = true)]
    pub endpoint: Option<String>,

    /// The instance directory: per-instance Cloud Connect state (the
    /// enrolled identity) lives under `<dir>/.spice`. Defaults to the
    /// current directory. `SPICE_CONFIG_DIR` overrides the derived `.spice`
    /// location entirely. Applies to `status`, `remove`, and `service`.
    #[arg(long, value_name = "PATH", global = true)]
    pub dir: Option<PathBuf>,

    /// Skip the confirmation prompt. Applies to `remove`, which otherwise
    /// asks before releasing the instance and stopping its service.
    #[arg(long, short = 'y', global = true)]
    pub yes: bool,

    /// Clear this directory's local state even when Spice Cloud could not
    /// confirm the release. Applies to `remove`, which otherwise keeps the
    /// identity so a retry can finish. Use it when the instance is already
    /// deleted in the portal, or when the control plane that issued it is gone:
    /// the portal-side delete is authoritative either way.
    #[arg(long, global = true)]
    pub force: bool,

    /// The global `-v` count, forwarded by the dispatcher for the same reason
    /// as [`ConnectArgs::cloud_region`] below: the flag is global and clap
    /// would reject a second definition of it here.
    ///
    /// A foreground runtime this command starts is the command's own output,
    /// so `spice -v connect` has to reach it exactly as `spice run -v` does.
    #[arg(skip)]
    pub verbosity: u8,

    /// The global `--cloud-region`, forwarded by the dispatcher rather than
    /// declared here (the flag is global; clap would reject a second definition
    /// of the same name).
    ///
    /// Carried so state-management commands can reject it explicitly rather
    /// than silently implying that it changes the enrolled instance. It stays
    /// honoured on the deprecated pod-add fallthrough, where it always meant
    /// the Spice.ai Cloud data region.
    #[arg(skip)]
    pub cloud_region: Option<String>,
}

/// Cloud-connect subcommands.
#[derive(Subcommand, Debug)]
pub enum ConnectCommand {
    /// Show this directory's Spice Cloud Connect state: connection, service,
    /// and deployment, from one snapshot.
    Status(StatusArgs),

    /// Delete this instance's project using the current user session, uninstall
    /// its service, and clear local Cloud identity and staged state.
    Remove,

    /// Install and manage the persistent service for this instance directory.
    ///
    /// `svc` is a hidden alias for interactive typing; `service` is the only
    /// documented spelling.
    #[command(alias = "svc")]
    Service(service::cli::ServiceArgs),
}

/// A positional value may be a deprecated Spicepod path, but operators can
/// accidentally paste an enrollment authority here. Keep it zeroizing and
/// permanently redacted from derived CLI diagnostics either way.
#[derive(Clone)]
pub struct ConnectTarget(SecretString);

impl ConnectTarget {
    fn expose(&self) -> &str {
        self.0.expose_secret()
    }
}

fn is_deprecated_spicepod_target(target: &str) -> bool {
    let Some((org, pod)) = target.split_once('/') else {
        return false;
    };
    !org.is_empty() && !pod.is_empty() && !pod.contains('/')
}

impl std::str::FromStr for ConnectTarget {
    type Err = std::convert::Infallible;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        Ok(Self(SecretString::from(value.to_string())))
    }
}

impl std::fmt::Debug for ConnectTarget {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ConnectTarget([REDACTED])")
    }
}

/// Arguments for `spice connect status`.
#[derive(Args, Debug)]
pub struct StatusArgs {
    /// Output format. `json` writes one report and nothing else to stdout.
    #[arg(long, short = 'o', value_enum, default_value_t = OutputFormat::Table)]
    pub output: OutputFormat,
}

impl ConnectArgs {
    /// Whether this invocation writes JSON to stdout, so the dispatcher can
    /// suppress the version banner that would otherwise foul it.
    #[must_use]
    pub fn produces_json(&self) -> bool {
        match &self.command {
            Some(ConnectCommand::Status(args)) => args.output == OutputFormat::Json,
            Some(ConnectCommand::Service(args)) => matches!(
                &args.command,
                Some(service::cli::ServiceCommand::Status(args))
                    if args.output == OutputFormat::Json
            ),
            _ => false,
        }
    }

    /// Select JSON output wherever this command has a structured form.
    pub fn apply_machine_mode(&mut self) {
        match &mut self.command {
            Some(ConnectCommand::Status(args)) => args.output = OutputFormat::Json,
            Some(ConnectCommand::Service(args)) => {
                if let Some(service::cli::ServiceCommand::Status(args)) = &mut args.command {
                    args.output = OutputFormat::Json;
                }
            }
            // `remove` and the service lifecycle actions report progress rather
            // than structured data, and the deprecated pod-add fallthrough has
            // no JSON form.
            _ => {}
        }
    }
}

/// Execute the `spice connect` command.
///
/// # Errors
///
/// Returns an error if I/O fails, this directory holds no Cloud Connect
/// state to act on, or the deprecated pod-add path errors.
pub async fn execute(ctx: &RuntimeContext, args: ConnectArgs) -> Result<()> {
    let config_dir = CloudConnectConfig::resolve_config_dir(args.dir.as_deref());

    if let Some(cmd) = args.command {
        reject_cloud_region(args.cloud_region.as_deref())?;
        if args.region.is_some() {
            return Err(Error::InvalidUsage {
                message: "--region applies to enrollment, not to a `connect` subcommand."
                    .to_string(),
            });
        }
        reject_remove_only_flags(&cmd, args.force, args.yes)?;
        let instance_dir = requested_instance_dir(args.dir.as_deref())?;
        return match cmd {
            ConnectCommand::Status(status_args) => {
                print_status(
                    &instance_dir,
                    &config_dir,
                    args.endpoint.as_deref(),
                    status_args.output,
                )
                .await
            }
            ConnectCommand::Remove => {
                remove_identity(
                    &instance_dir,
                    &config_dir,
                    args.endpoint.as_deref(),
                    args.yes,
                    args.force,
                )
                .await
            }
            ConnectCommand::Service(service_args) => {
                let endpoint = endpoint_for_local_reporting(&config_dir, args.endpoint.as_deref())?;
                service::cli::execute(ctx, service_args, &instance_dir, &config_dir, &endpoint)
                    .await
            }
        };
    }

    if args.force || args.yes {
        return Err(Error::InvalidUsage {
            message: "--force and --yes apply only to `spice connect remove`; interactive setup and the deprecated pod-add form do not consume them."
                .to_string(),
        });
    }

    // The deprecated pod-add fallthrough is a Spice.ai Cloud fetch, where
    // `--cloud-region` has always been meaningful, so it is not rejected here.
    if let Some(target) = args.target.as_ref().map(ConnectTarget::expose) {
        if args.region.is_some() {
            return Err(Error::InvalidUsage {
                message:
                    "--region cannot be combined with the deprecated `connect <org>/<pod>` form."
                        .to_string(),
            });
        }

        // A secret must never ride a positional argument. Reject canonical keys
        // and close near misses without echoing either.
        if runtime_cloud_connect::enrollment_key::looks_like_enrollment_key(target) {
            return Err(Error::InvalidUsage {
                message: "An enrollment key is not accepted as a positional argument. For unattended enrollment, run `spiced --token <enrollment-key>` from the instance directory. See: https://spiceai.org/docs".to_string(),
            });
        }
        if !is_deprecated_spicepod_target(target) {
            return Err(Error::InvalidUsage {
                message: "`spice connect` accepts only interactive setup, a documented lifecycle subcommand, or the deprecated `<org>/<pod>` Spicepod form."
                    .to_string(),
            });
        }
        if args.endpoint.is_some() {
            return Err(Error::InvalidUsage {
                message: "--endpoint applies to Cloud Connect setup, not to the deprecated `connect <org>/<pod>` form; use `spice add <org>/<pod>`."
                    .to_string(),
            });
        }

        eprintln!(
            "warning: `spice connect <org>/<pod>` is deprecated and will be removed in a future release; use `spice add {target}` instead."
        );
        return execute_add_or_connect(
            ctx,
            AddArgs {
                pod_path: target.to_string(),
            },
            true,
        )
        .await;
    }

    reject_cloud_region(args.cloud_region.as_deref())?;
    if !std::io::stdin().is_terminal() || !std::io::stderr().is_terminal() {
        return Err(Error::InvalidUsage {
            message: "`spice connect` is an interactive setup flow and requires a terminal. For unattended enrollment, run `spiced --token <enrollment-key>`; create and attach the project separately in Spice Cloud.".to_string(),
        });
    }
    // The command promises to finish with either a foreground runtime or the
    // supported Linux service. Refuse native Windows before enrollment or
    // project creation commits remote state this CLI cannot run.
    ctx.ensure_local_runtime_supported()?;

    let Some(directory) = transaction::execute(
        ctx,
        transaction::ConnectRequest {
            org: None,
            project: None,
            token: None,
            region: args.region,
            dir: args.dir,
            endpoint: args.endpoint,
        },
    )
    .await?
    else {
        return Ok(());
    };

    // The transaction has committed this directory's durable state, so the
    // command ends where the operator asked it to: at a running instance.
    start_instance(ctx, Some(&directory), args.verbosity).await
}

/// Reject `--cloud-region` on Cloud Connect state-management commands.
///
/// Neither half of an enrollment is chosen by a region code on this side:
///
/// - The **control plane** used to inspect and release state comes from
///   `--endpoint`, then `SPICE_CLOUD_ENDPOINT`, then the `cloud-endpoint` file,
///   then [`runtime_cloud_connect::config::DEFAULT_ENDPOINT`].
/// - The **gateway** the control stream dials comes back in the enroll response
///   as `gateway_addr`. Spice Cloud resolves it from `--region` — the instance's
///   declared location — by ranking the stamps it actually runs a gateway in and
///   picking the nearest, falling back to the deployment's home stamp for a
///   location it cannot rank. Deriving a gateway host from a region code
///   CLI-side is the conflation that hands out hostnames with nothing behind
///   them, so `spiced` sends the location and never interprets it.
///
/// Erroring beats accepting-and-ignoring: `--cloud-region` silently doing
/// nothing would look like a working region selection right up until someone
/// checked which gateway the instance dialled.
fn reject_cloud_region(cloud_region: Option<&str>) -> Result<()> {
    let Some(region) = cloud_region.filter(|r| !r.is_empty()) else {
        return Ok(());
    };
    Err(Error::InvalidArgument {
        message: format!(
            "--cloud-region {region} does not apply to `spice connect`. Spice Cloud selects this \
             instance's gateway from the location supplied to `spiced --region` during \
             `spiced --token` enrollment and returns it in the enroll response, falling back \
             to the home stamp for a location it does not recognise. Use --endpoint <url> \
             here only to inspect or release state through another Spice Cloud control plane. \
             See: https://spiceai.org/docs"
        ),
    })
}

/// Leave this directory's instance running, once its Cloud Connect state is
/// durable.
///
/// The command ends at a running instance rather than at a report, because that
/// is what the operator asked for — `spice connect status` is the command that
/// reports. Which process runs it is decided by what is already installed: a
/// supervised instance is started through its supervisor and this command
/// returns, an unsupervised one runs in the foreground and this command stays
/// attached to it until it exits.
async fn start_instance(ctx: &RuntimeContext, dir: Option<&Path>, verbosity: u8) -> Result<()> {
    let instance_dir = requested_instance_dir(dir)?;
    let state_config_dir = CloudConnectConfig::resolve_config_dir(dir);
    let reconnect_config = runtime_cloud_connect::CloudConnectConfig::from_env_at(
        env!("CARGO_PKG_VERSION"),
        state_config_dir.clone(),
    );
    match runtime_cloud_connect::load_reconnectable_identity_async(&reconnect_config).await {
        Ok(Some(_)) => {}
        Ok(None) => return Ok(()),
        Err(source) => {
            return Err(Error::CloudConnectIo {
                message: format!(
                    "validate the enrolled identity before starting this instance: {source}"
                ),
            });
        }
    }
    let service_config_dir =
        tokio::fs::canonicalize(&state_config_dir)
            .await
            .map_err(|source| Error::CloudConnectIo {
                message: format!(
                    "resolve the Cloud Connect config directory {}: {source}",
                    state_config_dir.display()
                ),
            })?;
    // The same preflight runs before the transaction; retain the error here as
    // a defensive boundary for callers that invoke this helper directly.
    ctx.ensure_local_runtime_supported()?;

    // A host with no supervisor this release drives has nothing that could own
    // the process, so the runtime runs in the foreground and this command stays
    // attached to it.
    if !cfg!(any(target_os = "linux", target_os = "macos")) {
        println!("Starting the Spice runtime. Press Ctrl-C to stop it.");
        return crate::runtime_launcher::run_runtime(
            ctx,
            &crate::runtime_launcher::RunConfig {
                working_dir: Some(instance_dir.clone()),
                verbosity,
                connection_report: crate::runtime_launcher::ConnectionReport::Runtime,
                ..crate::runtime_launcher::RunConfig::default()
            },
        )
        .await;
    }

    // What owns the process, and so what this command starts. An instance a
    // supervisor already owns must not also be started in the terminal: the
    // host would hold two runtimes for one identity, which is what the second
    // one's directory lock refuses — after the operator was told their instance
    // was starting.
    let backend = service::backend();
    let Some(manifest) = service::resolve_with_state(
        backend,
        &instance_dir,
        &state_config_dir,
        &service_config_dir,
    )?
    else {
        println!("Starting the Spice runtime. Press Ctrl-C to stop it.");
        return crate::runtime_launcher::run_runtime(
            ctx,
            &crate::runtime_launcher::RunConfig {
                // The instance directory resolves both the spicepod and the
                // `.spice` state this command just inspected, so the runtime
                // has to start there rather than wherever the CLI was invoked
                // from.
                working_dir: Some(instance_dir.clone()),
                verbosity,
                // Enrollment and attachment are complete, but only the runtime
                // can truthfully report that it is both serving and
                // acknowledged by Spice Cloud.
                connection_report: crate::runtime_launcher::ConnectionReport::Runtime,
                ..crate::runtime_launcher::RunConfig::default()
            },
        )
        .await;
    };

    // A service that is already up is the state this command asks for, so it is
    // reported without being touched: interrupting a serving instance to prove
    // it can be started is the one thing this must not do. Anything else is
    // asked to start.
    if backend.observe(&manifest).state != ServiceState::Running {
        backend.start(&manifest)?;
    }
    println!(
        "Spice Cloud Connect: {} is running as the {} service {} ({}).",
        instance_dir.display(),
        manifest.scope,
        manifest.name,
        manifest.supervisor
    );
    println!();
    println!("Manage it with:");
    println!("  spice connect status");
    println!("  spice connect service logs -f");
    Ok(())
}

/// Collect the one status snapshot used by the bare and explicit status forms.
async fn collect_status(
    instance_dir: &Path,
    config_dir: &Path,
    endpoint: Option<&str>,
) -> Result<ConnectStatus> {
    let endpoint = endpoint_for_local_reporting(config_dir, endpoint)?;
    Ok(ConnectStatus::collect(instance_dir, config_dir, &endpoint).await)
}

/// Render an already-collected snapshot and preserve its degraded exit status.
fn render_status(status: &ConnectStatus, output: OutputFormat) -> Result<()> {
    status::render(status, output)?;
    // The report is already on stdout; the diagnosis travels as the command's
    // error so a `--output json` run stays parseable.
    match status.degradation() {
        Some(message) => Err(Error::ServiceUnavailable { message }),
        None => Ok(()),
    }
}

/// Resolve the user-selected instance independently of the Cloud state path.
/// `SPICE_CONFIG_DIR` overrides only `config_dir`; it must never move the
/// Spicepod working directory or change the deterministic service identity.
fn requested_instance_dir(explicit: Option<&Path>) -> Result<PathBuf> {
    let cwd = std::env::current_dir().map_err(|source| Error::CloudConnectIo {
        message: format!("resolve the current instance directory: {source}"),
    })?;
    let requested = match explicit {
        Some(path) if path.is_absolute() => path.to_path_buf(),
        Some(path) => cwd.join(path),
        None => cwd,
    };
    canonicalize_instance_dir(&requested).map_err(|source| Error::CloudConnectIo {
        message: format!(
            "resolve the instance directory {} through its existing filesystem prefix: {source}",
            requested.display()
        ),
    })
}

fn reject_remove_only_flags(command: &ConnectCommand, force: bool, yes: bool) -> Result<()> {
    if !matches!(command, ConnectCommand::Remove) && (force || yes) {
        return Err(Error::InvalidUsage {
            message: "--force and --yes apply only to `spice connect remove`; status and service commands do not consume them."
                .to_string(),
        });
    }
    Ok(())
}

/// Reduce an instance directory to the one spelling every command must derive
/// its service name from.
///
/// `fs::canonicalize` is the authoritative answer because it resolves
/// symlinks. A not-yet-created leaf still needs a stable service identity, so
/// canonicalize the longest existing prefix and normalize only the unresolved
/// suffix. Textually collapsing `..` before that point can cross a symlink and
/// select a different instance directory.
fn canonicalize_instance_dir(dir: &Path) -> std::io::Result<PathBuf> {
    enum UnresolvedComponent {
        Parent,
        Normal(std::ffi::OsString),
    }

    // Callers make the path absolute first. Keep the helper's relative-path
    // behavior explicit for tests and for defensive reuse: there is no stable
    // filesystem root from which to resolve a partial relative path.
    if !dir.is_absolute() {
        return Ok(dir.to_path_buf());
    }

    let mut prefix = dir.to_path_buf();
    let mut unresolved = Vec::new();
    loop {
        match std::fs::canonicalize(&prefix) {
            Ok(mut canonical) => {
                for component in unresolved.into_iter().rev() {
                    match component {
                        UnresolvedComponent::Parent => {
                            canonical.pop();
                        }
                        UnresolvedComponent::Normal(name) => canonical.push(name),
                    }
                }
                return Ok(canonical);
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                let Some(component) = prefix.components().next_back() else {
                    return Err(source);
                };
                match component {
                    std::path::Component::ParentDir => {
                        unresolved.push(UnresolvedComponent::Parent);
                    }
                    std::path::Component::Normal(name) => {
                        unresolved.push(UnresolvedComponent::Normal(name.to_owned()));
                    }
                    std::path::Component::CurDir => {}
                    std::path::Component::Prefix(_) | std::path::Component::RootDir => {
                        return Err(source);
                    }
                }
                if !prefix.pop() {
                    return Err(source);
                }
            }
            Err(source) => return Err(source),
        }
    }
}

/// Collect and render one status snapshot.
async fn print_status(
    instance_dir: &Path,
    config_dir: &Path,
    endpoint: Option<&str>,
    output: OutputFormat,
) -> Result<()> {
    let status = collect_status(instance_dir, config_dir, endpoint).await?;
    render_status(&status, output)
}

/// Remove this instance: delete its attached project with an authenticated
/// user session, uninstall an installed service, and clear local Cloud state.
///
/// Project deletion is attempted before local credentials are destroyed so a
/// failed Cloud operation remains retryable. `force` is explicitly local-state
/// recovery: it permits cleanup after a failed Cloud operation while clearly
/// leaving the operator responsible for Cloud-side cleanup.
async fn remove_identity(
    instance_dir: &Path,
    config_dir: &Path,
    endpoint: Option<&str>,
    assume_yes: bool,
    force: bool,
) -> Result<()> {
    let mutation_lock = runtime_cloud_connect::MutationLock::acquire(config_dir, "remove")
        .await
        .map_err(|source| Error::CloudConnectIo {
            message: format!("acquire Cloud Connect state for removal: {source}"),
        })?;
    let display_config_dir = tokio::fs::canonicalize(config_dir)
        .await
        .map_err(|source| Error::CloudConnectIo {
            message: format!(
                "resolve the locked Cloud Connect config directory {}: {source}",
                config_dir.display()
            ),
        })?;
    mutation_lock
        .ensure_directory_stable()
        .map_err(|source| Error::CloudConnectIo {
            message: format!("validate locked Cloud Connect state for removal: {source}"),
        })?;
    // Every state path below resolves through the directory descriptor held by
    // `mutation_lock`. A system service's unprivileged account may rename its
    // config directory while this root command is running; looking up the old
    // pathname again would let a replacement symlink redirect the deletion.
    let config_dir = mutation_lock
        .descriptor_relative_config_dir()
        .map_err(|source| Error::CloudConnectIo {
            message: format!("pin locked Cloud Connect state for removal: {source}"),
        })?;
    let _instance_lock =
        runtime_cloud_connect::RuntimeLock::acquire(&config_dir).map_err(|source| {
            Error::CloudConnectIo {
                message: format!(
                    "{source} Stop the running instance before using `spice connect remove`."
                ),
            }
        })?;
    // Own the complete state transition before inspecting any file. Without
    // this boundary, removal can clear old state while enrollment is still
    // promoting a replacement identity under the same directory lock.
    let enrollment_transaction = Arc::new(
        runtime_cloud_connect::EnrollmentTransactionLock::try_acquire_async(&config_dir)
            .await
            .map_err(|e| Error::CloudConnectIo {
                message: format!("acquire the enrollment transaction before removal: {e}"),
            })?,
    );

    let identity_path = config_dir.join(IDENTITY_FILE);
    let draft_path = runtime_cloud_connect::EnrollmentDraft::path_in(&config_dir);
    let journal_path = config_dir.join(state::CONNECT_OPERATION_FILE);
    let project_journal_path = config_dir.join(state::PROJECT_OPERATION_FILE);
    let endpoint_path = config_dir.join(CLOUD_ENDPOINT_FILE);
    let cache_path = config_dir.join(runtime_cloud_connect::secret_cache::SECRET_CACHE_FILE);
    let backend = service::backend();

    let (identity, identity_load_failure) =
        match runtime_cloud_connect::identity::IdentityStore::load_optional_async(
            identity_path.clone(),
        )
        .await
        {
            Ok(identity) => (identity, None),
            Err(source) if force => (None, Some(source.to_string())),
            Err(source) => {
                return Err(Error::CloudConnectIo {
                    message: format!("load identity: {source}"),
                });
            }
        };
    let installed =
        service::resolve_with_state(backend, instance_dir, &config_dir, &display_config_dir)?;

    // `symlink_metadata` observes the directory entry itself. A dangling link
    // is state that `--force` must remove, not an absent file that can be left
    // behind to block or redirect the next enrollment.
    let had_identity = identity.is_some()
        || identity_load_failure.is_some()
        || state_entry_exists(&identity_path)?;
    let had_draft = state_entry_exists(&draft_path)?;
    let had_journal = state_entry_exists(&journal_path)?;
    let had_project_journal = state_entry_exists(&project_journal_path)?;
    let had_endpoint = state_entry_exists(&endpoint_path)?;
    let had_cache = state_entry_exists(&cache_path)?;

    let release_artifacts = [
        (
            "delivered-secrets cache",
            cache_path.as_path(),
            runtime_cloud_connect::identity::ArtifactKinds::Runtime,
        ),
        (
            "enrollment draft",
            draft_path.as_path(),
            runtime_cloud_connect::identity::ArtifactKinds::Runtime,
        ),
        (
            "enrollment journal",
            journal_path.as_path(),
            runtime_cloud_connect::identity::ArtifactKinds::Connect,
        ),
        (
            "project journal",
            project_journal_path.as_path(),
            runtime_cloud_connect::identity::ArtifactKinds::Connect,
        ),
        (
            "endpoint binding",
            endpoint_path.as_path(),
            runtime_cloud_connect::identity::ArtifactKinds::Connect,
        ),
        (
            "Cloud identity",
            identity_path.as_path(),
            runtime_cloud_connect::identity::ArtifactKinds::Runtime,
        ),
    ];
    let mut had_release_artifacts = false;
    for (label, path, kinds) in &release_artifacts {
        let present = runtime_cloud_connect::identity::release_artifacts_present(path, *kinds)
            .map_err(|source| Error::CloudConnectIo {
                message: format!(
                    "inspect interrupted writes for the {label} at {} before removal: {source}",
                    path.display()
                ),
            })?;
        had_release_artifacts |= present;
    }

    if !had_identity
        && !had_draft
        && !had_journal
        && !had_project_journal
        && !had_endpoint
        && !had_cache
        && !had_release_artifacts
        && installed.is_none()
    {
        // This may be a retry after every unlink succeeded but one directory
        // synchronization failed. Prove every canonical file durably absent
        // before reporting the directory already clear.
        clear_local_connect_state(
            &config_dir,
            &identity_path,
            &cache_path,
            &endpoint_path,
            Arc::clone(&enrollment_transaction),
        )
        .await?;
        println!("Spice Cloud Connect: nothing to remove.");
        return Ok(());
    }

    // Without a usable identity there is no authoritative project/org target
    // to compare with the authenticated user session. Never turn that into a
    // local-only success implicitly: `--force` is the explicit recovery path
    // for a host whose Cloud cleanup was completed separately.
    if identity.is_none() && !force {
        return Err(Error::CloudConnectIo {
            message: "No usable Spice Cloud identity is available to identify and delete this instance's project. Complete the Cloud cleanup separately, then use `spice connect remove --force` to abandon only this host's local state."
                .to_string(),
        });
    }

    // Confirm before touching a running service: stopping it takes the
    // instance offline, and an operator who ran this in the wrong directory
    // needs the chance to say no. Name what will be affected.
    if !assume_yes {
        println!(
            "This will delete this instance's project in Spice Cloud and remove the instance from this host:"
        );
        if let Some(ref installed) = installed {
            println!("  service:   {} (stopped and uninstalled)", installed.name);
        }
        println!("  directory: {}", instance_dir.display());
        if had_identity {
            println!("  identity:  {} (deleted)", identity_path.display());
        }
        if had_cache {
            println!(
                "  secrets:   {} (deleted — the app's delivered secrets)",
                cache_path.display()
            );
        }
        if had_release_artifacts {
            println!("  recovery:  interrupted Cloud state writes (deleted)");
        }
        if !confirm("Continue?")? {
            println!(
                "Nothing was removed — the service is still running and the local state is intact."
            );
            return Ok(());
        }
    }

    // Authorization is read-only and must be known before deleting the Cloud
    // project. Otherwise a non-root removal of a system service can succeed in
    // Cloud and only then discover that it cannot stop the enrolled runtime.
    if let Some(ref manifest) = installed {
        backend.authorize_uninstall(manifest)?;
    }

    // Confirmation is the mutation boundary. Reclaim every crash artifact
    // only after it succeeds, while all three exclusion layers are still held
    // and before any Cloud or canonical-file deletion makes the operation
    // irreversible. These siblings can contain complete credentials, cache
    // plaintext/ciphertext, or a candidate journal that would resurrect state
    // after removal.
    for (label, path, kinds) in release_artifacts {
        runtime_cloud_connect::identity::reclaim_all_release_artifacts(path, kinds).map_err(
            |source| Error::CloudConnectIo {
                message: format!(
                    "reclaim interrupted writes for the {label} at {} before removal: {source}",
                    path.display()
                ),
            },
        )?;
    }

    // Project deletion is a user-authorized Cloud lifecycle action. The
    // instance identity cannot authorize deleting its own project, and a
    // service-account token is not a user session, so removal fails closed
    // unless the operator is logged in to the identity's organization.
    if let Some(ref identity) = identity {
        match delete_attached_project(&config_dir, endpoint, identity).await {
            Ok(target) => {
                println!("Deleted project {target} in Spice Cloud.");
            }
            Err(reason) if !force => {
                return Err(Error::CloudConnectIo {
                    message: format!("delete this instance's project in Spice Cloud: {reason}"),
                });
            }
            Err(reason) => {
                println!(
                    "Could not delete the project in Spice Cloud: {reason} Clearing this \
                     directory's local state only because --force was given; cloud cleanup \
                     remains required."
                );
            }
        }
    } else if let Some(reason) = identity_load_failure {
        println!(
            "Could not read the Cloud identity needed to delete its project: {reason} Clearing \
             this directory's local state only because --force was given; cloud cleanup remains \
             required."
        );
    } else {
        println!(
            "No Cloud identity is present to identify and delete this instance's project. \
             Clearing this directory's local state only because --force was given; cloud \
             cleanup remains required."
        );
    }

    // A service left running after project deletion would reconnect with an
    // identity whose renewal has been revoked, so uninstall it before clearing
    // the local credential. A failure must not abort before local Cloud state is
    // cleared; report it at the end. The uninstall primitive is shared with
    // `spice connect service uninstall`, while identity cleanup belongs only to
    // this command.
    let uninstall_failure = match installed.as_ref() {
        Some(manifest) => match service::uninstall_resolved(backend, manifest, &config_dir) {
            Ok(()) => {
                println!("Stopped and uninstalled {}.", manifest.name);
                None
            }
            Err(err) => Some(err),
        },
        None => None,
    };

    clear_local_connect_state(
        &config_dir,
        &identity_path,
        &cache_path,
        &endpoint_path,
        Arc::clone(&enrollment_transaction),
    )
    .await?;

    println!(
        "Spice Cloud Connect identity cleared. To re-enroll this directory, mint a new \
         enrollment key in the Spice Cloud portal and start the runtime with \
         `spiced --token <enrollment-key>`."
    );
    // Surfaced last so the exit status still reports it: the local state is
    // gone, but a service left behind would keep restarting a runtime with no
    // identity until someone removes it.
    match uninstall_failure {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

/// Clear every canonical local Cloud Connect file in crash-safe order.
///
/// Each delete runs even when its file is already missing: a retry after an
/// unlink whose parent-directory sync failed must synchronize that absence
/// before it can report success. The secret cache precedes the identity because
/// the identity contains the cache's only key, and the identity is removed last
/// so every retry still has authoritative ownership until all auxiliary state
/// has been durably cleared.
async fn clear_local_connect_state(
    config_dir: &Path,
    identity_path: &Path,
    cache_path: &Path,
    endpoint_path: &Path,
    enrollment_transaction: Arc<runtime_cloud_connect::EnrollmentTransactionLock>,
) -> Result<()> {
    let blocking_config_dir = config_dir.to_path_buf();
    let blocking_cache_path = cache_path.to_path_buf();
    let blocking_endpoint_path = endpoint_path.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<()> {
        let remove_deployment_file = |label: &str, file: &str| {
            let path = blocking_config_dir.join(file);
            state::remove_durable_file(&path).map_err(|e| Error::CloudConnectIo {
                message: format!("remove {label} at {}: {e}", path.display()),
            })
        };

        // Keep a write-ahead marker in place until both canonical halves are
        // gone. If this process exits first, startup restores the prior complete
        // pair; once the marker is deleted, no old canonical or staging half is
        // available to be mixed into a later enrollment.
        state::remove_durable_file(&blocking_cache_path).map_err(|e| Error::CloudConnectIo {
            message: format!("remove the delivered-secrets cache: {e}"),
        })?;
        for (label, file) in [
            (
                "cloud-managed Spicepod",
                runtime_cloud_connect::config::CLOUD_MANAGED_SPICEPOD_FILE,
            ),
            (
                "staged cloud-managed Spicepod",
                "spicepod-cloud-managed.incoming.yml",
            ),
            (
                "cloud-managed Spicepod replacement backup",
                "spicepod-cloud-managed.bak",
            ),
            (
                "deployment transaction marker",
                runtime_cloud_connect::config::DEPLOYMENT_TRANSACTION_FILE,
            ),
            (
                "deployment transaction staging marker",
                runtime_cloud_connect::config::DEPLOYMENT_TRANSACTION_INCOMING_FILE,
            ),
            (
                "staged delivered-secret cache",
                runtime_cloud_connect::config::INCOMING_SECRET_CACHE_FILE,
            ),
            (
                "previous delivered-secret cache",
                runtime_cloud_connect::config::PREVIOUS_SECRET_CACHE_FILE,
            ),
            (
                "previous cloud-managed Spicepod",
                runtime_cloud_connect::config::PREVIOUS_CLOUD_MANAGED_SPICEPOD_FILE,
            ),
        ] {
            remove_deployment_file(label, file)?;
        }
        state::ConnectOperation::delete(&blocking_config_dir).map_err(|e| {
            Error::CloudConnectIo {
                message: format!("remove enrollment journal: {e}"),
            }
        })?;
        state::ProjectOperation::delete(&blocking_config_dir).map_err(|e| {
            Error::CloudConnectIo {
                message: format!("remove project assignment journal: {e}"),
            }
        })?;
        state::remove_durable_file(&blocking_endpoint_path).map_err(|e| Error::CloudConnectIo {
            message: format!("remove endpoint override: {e}"),
        })?;
        remove_local_credential_debris(&blocking_config_dir)?;
        Ok(())
    })
    .await
    .map_err(|source| Error::CloudConnectIo {
        message: format!("local Cloud Connect cleanup task failed: {source}"),
    })??;

    enrollment_transaction
        .delete_draft_async()
        .await
        .map_err(|e| Error::CloudConnectIo {
            message: format!("remove enrollment draft: {e}"),
        })?;
    runtime_cloud_connect::identity::IdentityStore::clear_with_transaction_async(
        identity_path.to_path_buf(),
        Arc::clone(&enrollment_transaction),
    )
    .await
    .map_err(|e| Error::CloudConnectIo {
        message: format!("clear identity: {e}"),
    })?;
    Ok(())
}

/// Remove only the secret-bearing temp, backup, and quarantined draft name
/// families created by Cloud Connect writers. The caller owns all three local
/// state locks, so every matching writer artifact is abandoned and removable.
fn remove_local_credential_debris(config_dir: &Path) -> Result<()> {
    let draft_path = runtime_cloud_connect::EnrollmentDraft::path_in(config_dir);
    let identity_path = config_dir.join(IDENTITY_FILE);
    let entries = match std::fs::read_dir(config_dir) {
        Ok(entries) => entries,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => {
            return Err(Error::CloudConnectIo {
                message: format!(
                    "inspect local Cloud credential debris in {}: {source}",
                    config_dir.display()
                ),
            });
        }
    };
    for entry in entries {
        let entry = entry.map_err(|source| Error::CloudConnectIo {
            message: format!(
                "inspect local Cloud credential debris in {}: {source}",
                config_dir.display()
            ),
        })?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let generated_draft_quarantine = is_generated_draft_quarantine(&name);
        let generated_draft_temp =
            runtime_cloud_connect::identity::is_runtime_atomic_write_artifact(&draft_path, &name);
        let generated_identity_temp =
            runtime_cloud_connect::identity::is_runtime_atomic_write_artifact(
                &identity_path,
                &name,
            );
        if generated_draft_quarantine || generated_draft_temp || generated_identity_temp {
            state::remove_durable_file(&entry.path()).map_err(|source| Error::CloudConnectIo {
                message: format!(
                    "remove local Cloud credential debris at {}: {source}",
                    entry.path().display()
                ),
            })?;
        }
    }
    Ok(())
}

/// Exact spelling emitted by `state::quarantine`: decimal epoch milliseconds
/// and process ID. Prefix/suffix matching alone would classify an
/// operator-created backup as Cloud state.
fn is_generated_draft_quarantine(name: &str) -> bool {
    let Some(parts) = name
        .strip_prefix("enrollment-draft.quarantine.")
        .and_then(|name| name.strip_suffix(".json"))
    else {
        return false;
    };
    let mut parts = parts.split('.');
    let (Some(epoch), Some(pid), None) = (parts.next(), parts.next(), parts.next()) else {
        return false;
    };
    epoch
        .parse::<u128>()
        .is_ok_and(|value| value.to_string() == epoch)
        && pid
            .parse::<u32>()
            .is_ok_and(|value| value.to_string() == pid)
}

fn state_entry_exists(path: &Path) -> Result<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(Error::CloudConnectIo {
            message: format!(
                "inspect Cloud Connect state at {}: {source}",
                path.display()
            ),
        }),
    }
}

async fn delete_attached_project(
    config_dir: &Path,
    endpoint: Option<&str>,
    identity: &runtime_cloud_connect::Identity,
) -> std::result::Result<String, String> {
    let org = identity.org_name.as_deref().ok_or_else(|| {
        "the stored identity does not name its organization; log in and repair the attachment before removing it"
            .to_string()
    })?;
    let app_id = identity.app_id.as_deref().ok_or_else(|| {
        "the stored identity is not attached to a project ID; remove it from Spice Cloud first, then use --force only to recover local state"
            .to_string()
    })?;
    let project_id = app_id
        .parse::<i64>()
        .ok()
        .filter(|id| *id > 0)
        .ok_or_else(|| {
            format!(
                "the stored project ID {app_id:?} is invalid; refusing to resolve a mutable project name"
            )
        })?;
    let token = cloud_org::token_for_org(org)
        .or_else(cloud_org::default_token)
        .ok_or_else(|| format!("no authenticated user session is stored for organization {org}"))?;
    let endpoint = resolved_endpoint(config_dir, endpoint).map_err(|error| error.to_string())?;
    let client = CloudClient::with_token_for_org_at(token, Some(org), &endpoint)
        .map_err(|error| error.to_string())?;
    let user = client
        .optional_user_auth_context()
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| {
            "project deletion requires an authenticated user session; service-account credentials are not accepted"
                .to_string()
        })?;
    if !user.org_name.eq_ignore_ascii_case(org) {
        return Err(format!(
            "the authenticated user session belongs to organization {}, but this instance belongs to {org}",
            user.org_name
        ));
    }

    if let Err(error) = client.delete_project_by_id(project_id).await
        && error.cloud_code() != Some(CloudErrorCode::NotFound)
    {
        return Err(error.to_string());
    }
    Ok(match identity.app_name.as_deref() {
        Some(project) => ProjectTarget::new(Some(org.to_string()), project.to_string()).to_string(),
        None => format!("project ID {project_id} in {org}"),
    })
}

/// Ask the operator to confirm a destructive step.
///
/// A non-interactive stdin (a script, a pipe) cannot answer, so rather than
/// assuming yes — which would let an unattended run stop a service nobody asked
/// it to — this errors and names `--yes` as the way scripts opt in.
fn confirm(prompt: &str) -> Result<bool> {
    use std::io::{BufRead as _, IsTerminal as _, Write as _};

    if !std::io::stdin().is_terminal() {
        return Err(Error::InvalidArgument {
            message: format!(
                "{prompt} — stdin is not a terminal, so the confirmation cannot be asked. \
                 Pass --yes to confirm non-interactively. Nothing was removed."
            ),
        });
    }

    print!("{prompt} [y/N] ");
    std::io::stdout()
        .flush()
        .map_err(|e| Error::CloudConnectIo {
            message: format!("write prompt: {e}"),
        })?;

    let mut answer = String::new();
    std::io::stdin()
        .lock()
        .read_line(&mut answer)
        .map_err(|e| Error::CloudConnectIo {
            message: format!("read confirmation: {e}"),
        })?;
    Ok(matches!(
        answer.trim().to_ascii_lowercase().as_str(),
        "y" | "yes"
    ))
}

/// Resolve the control-plane endpoint from explicit process configuration,
/// durable enrollment state, or the legacy instance-local endpoint file.
fn resolved_endpoint(config_dir: &Path, explicit: Option<&str>) -> Result<String> {
    let requested = explicit
        .filter(|endpoint| !endpoint.is_empty())
        .map(str::to_string)
        .or_else(|| {
            std::env::var("SPICE_CLOUD_ENDPOINT")
                .ok()
                .filter(|endpoint| !endpoint.is_empty())
        })
        .map(|endpoint| {
            runtime_cloud_connect::config::normalize_control_plane_endpoint(&endpoint).map_err(
                |source| Error::InvalidUsage {
                    message: format!("invalid Cloud Connect endpoint: {source}"),
                },
            )
        })
        .transpose()?;

    let identity =
        runtime_cloud_connect::IdentityStore::load_optional(&config_dir.join(IDENTITY_FILE))
            .map_err(|source| Error::CloudConnectIo {
                message: format!("load enrolled Cloud Connect endpoint: {source}"),
            })?;
    let bound = match identity.and_then(|identity| identity.control_plane_endpoint) {
        Some(endpoint) => Some(endpoint),
        None => runtime_cloud_connect::EnrollmentDraft::load_optional(config_dir)
            .map_err(|source| Error::CloudConnectIo {
                message: format!("load pending Cloud Connect endpoint: {source}"),
            })?
            .map(|draft| draft.binding.endpoint),
    }
    .map(|endpoint| {
        runtime_cloud_connect::config::normalize_control_plane_endpoint(&endpoint).map_err(
            |source| Error::CloudConnectIo {
                message: format!("stored Cloud Connect endpoint is invalid: {source}"),
            },
        )
    })
    .transpose()?;

    if let (Some(requested), Some(bound)) = (requested.as_deref(), bound.as_deref())
        && requested != bound
    {
        return Err(Error::InvalidUsage {
            message: format!(
                "endpoint {requested} does not match this instance's enrolled control plane {bound}"
            ),
        });
    }

    if let Some(endpoint) = requested.or(bound) {
        return Ok(endpoint);
    }

    runtime_cloud_connect::CloudConnectConfig::read_normalized_enroll_endpoint_override(config_dir)
        .map_err(|source| Error::CloudConnectIo {
            message: source.to_string(),
        })?
        .map_or_else(
            || Ok(runtime_cloud_connect::config::DEFAULT_ENDPOINT.to_string()),
            Ok,
        )
}

/// Resolve the endpoint for operations whose primary purpose is local state
/// inspection or service management. A damaged endpoint binding must not hide
/// an unreadable identity report or block stop, uninstall, and log access.
fn endpoint_for_local_reporting(config_dir: &Path, explicit: Option<&str>) -> Result<String> {
    match resolved_endpoint(config_dir, explicit) {
        Ok(endpoint) => Ok(endpoint),
        Err(error) if explicit.is_some() => Err(error),
        Err(error) => {
            eprintln!(
                "Cloud Connect endpoint could not be resolved for this local operation: {error}"
            );
            Ok(runtime_cloud_connect::config::DEFAULT_ENDPOINT.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{CommandFactory as _, Parser as _};

    /// A parser standing in for the real CLI, so the grammar under test is
    /// exactly the one `spice connect ...` presents.
    #[derive(clap::Parser, Debug)]
    #[command(name = "spice")]
    struct Harness {
        #[command(subcommand)]
        command: HarnessCommand,
    }

    #[derive(Subcommand, Debug)]
    enum HarnessCommand {
        Connect(ConnectArgs),
    }

    fn parse(args: &[&str]) -> Result<ConnectArgs, clap::Error> {
        let harness = Harness::try_parse_from(args)?;
        let HarnessCommand::Connect(connect) = harness.command;
        Ok(connect)
    }

    #[test]
    fn positional_target_debug_never_exposes_an_enrollment_authority() {
        let authority = "spice-enroll-abcdefghijklmnopqrstuvwxyz012345";
        let args = parse(&["spice", "connect", authority]).expect("target parses");
        let debug = format!("{args:?}");
        assert!(!debug.contains(authority));
        assert!(debug.contains("[REDACTED]"));
    }

    #[test]
    fn only_an_org_and_pod_pair_can_use_deprecated_fallthrough() {
        assert!(is_deprecated_spicepod_target("spiceai/quickstart"));
        for target in [
            "spiceai/quickstart/extra",
            "/quickstart",
            "spiceai/",
            "unknown-lifecycle-word",
        ] {
            assert!(
                !is_deprecated_spicepod_target(target),
                "{target} must not reach the deprecated pod-add path"
            );
        }
    }

    fn service_command(args: &[&str]) -> service::cli::ServiceCommand {
        let connect = parse(args).expect("the grammar must accept this");
        match connect.command {
            Some(ConnectCommand::Service(service_args)) => {
                service_args.command.expect("an action was given")
            }
            other => panic!("expected a service action, got {other:?}"),
        }
    }

    #[test]
    fn every_service_action_parses() {
        for action in [
            "install",
            "uninstall",
            "start",
            "stop",
            "restart",
            "status",
            "logs",
        ] {
            let parsed = service_command(&["spice", "connect", "service", action]);
            assert!(
                format!("{parsed:?}").to_lowercase().starts_with(action),
                "`spice connect service {action}` parsed as {parsed:?}"
            );
        }
    }

    #[test]
    fn service_without_an_action_parses_and_selects_nothing() {
        let connect = parse(&["spice", "connect", "service"]).expect("no action is valid");
        match connect.command {
            Some(ConnectCommand::Service(args)) => assert!(args.command.is_none()),
            other => panic!("expected the service group, got {other:?}"),
        }
    }

    #[test]
    fn remove_only_flags_are_rejected_by_status_and_service_commands() {
        for argv in [
            &["spice", "connect", "status", "--force"][..],
            &["spice", "connect", "status", "--yes"][..],
            &["spice", "connect", "service", "restart", "--force"][..],
            &["spice", "connect", "service", "status", "--yes"][..],
        ] {
            let parsed = parse(argv).expect("global remove-only flag parses for dispatch");
            let command = parsed.command.as_ref().expect("subcommand");
            let error = reject_remove_only_flags(command, parsed.force, parsed.yes)
                .expect_err("a non-remove command must reject remove-only flags");
            assert!(error.to_string().contains("apply only"), "{error}");
        }

        let parsed = parse(&["spice", "connect", "remove", "--force", "--yes"])
            .expect("remove accepts its flags");
        reject_remove_only_flags(
            parsed.command.as_ref().expect("remove command"),
            parsed.force,
            parsed.yes,
        )
        .expect("remove consumes both flags");
    }

    #[test]
    fn svc_is_a_hidden_alias_for_service() {
        // The alias exists for interactive typing. It is asserted only here, so
        // nothing else in the CLI can start documenting it as a second
        // spelling.
        let parsed = service_command(&["spice", "connect", "svc", "status"]);
        assert!(matches!(parsed, service::cli::ServiceCommand::Status(_)));

        let group = Harness::command();
        let connect = group
            .get_subcommands()
            .find(|c| c.get_name() == "connect")
            .expect("connect exists");
        let service = connect
            .get_subcommands()
            .find(|c| c.get_name() == "service")
            .expect("service exists");
        assert!(
            service.get_visible_aliases().next().is_none(),
            "`svc` must not appear in generated help"
        );
    }

    #[test]
    fn install_is_a_subcommand_and_never_a_flag() {
        // `--install` was the previous spelling; it must be gone so the
        // documented grammar is the only one.
        parse(&["spice", "connect", "--install"]).expect_err("--install must not parse");
        assert!(matches!(
            service_command(&["spice", "connect", "service", "install"]),
            service::cli::ServiceCommand::Install
        ));
    }

    #[test]
    fn logs_defaults_to_one_hundred_lines() {
        let service::cli::ServiceCommand::Logs(args) =
            service_command(&["spice", "connect", "service", "logs"])
        else {
            panic!("expected logs");
        };
        assert_eq!(args.number, 100);
        assert!(!args.follow);
    }

    #[test]
    fn logs_accepts_a_count_and_a_follow_flag() {
        for args in [
            ["spice", "connect", "service", "logs", "-n", "500"],
            ["spice", "connect", "service", "logs", "--number", "500"],
        ] {
            let service::cli::ServiceCommand::Logs(parsed) = service_command(&args) else {
                panic!("expected logs");
            };
            assert_eq!(parsed.number, 500);
        }

        for args in [
            ["spice", "connect", "service", "logs", "-f"],
            ["spice", "connect", "service", "logs", "--follow"],
        ] {
            let service::cli::ServiceCommand::Logs(parsed) = service_command(&args) else {
                panic!("expected logs");
            };
            assert!(parsed.follow);
            assert_eq!(parsed.number, 100);
        }

        // `-n 0 -f` follows only new lines.
        let service::cli::ServiceCommand::Logs(parsed) =
            service_command(&["spice", "connect", "service", "logs", "-n", "0", "-f"])
        else {
            panic!("expected logs");
        };
        assert_eq!(parsed.number, 0);
        assert!(parsed.follow);
    }

    #[test]
    fn logs_rejects_tail_and_an_out_of_range_count() {
        parse(&["spice", "connect", "service", "logs", "--tail"])
            .expect_err("--tail is intentionally absent");
        parse(&["spice", "connect", "service", "logs", "--tail", "50"])
            .expect_err("--tail is intentionally absent");
        parse(&["spice", "connect", "service", "logs", "-n", "100001"])
            .expect_err("a count beyond the ceiling must be refused");
        parse(&["spice", "connect", "service", "logs", "-n", "-1"])
            .expect_err("a negative count must be refused");
    }

    #[test]
    fn both_status_commands_accept_the_same_output_flag() {
        for args in [
            ["spice", "connect", "status", "--output", "json"],
            ["spice", "connect", "status", "-o", "json"],
        ] {
            let connect = parse(&args).expect("status accepts --output");
            assert!(connect.produces_json());
        }

        for args in [
            ["spice", "connect", "service", "status", "--output", "json"],
            ["spice", "connect", "service", "status", "-o", "json"],
        ] {
            let connect = parse(&args).expect("service status accepts --output");
            assert!(connect.produces_json());
        }

        // Table is the default, and only `status` produces JSON.
        assert!(
            !parse(&["spice", "connect", "status"])
                .expect("parse")
                .produces_json()
        );
        assert!(
            !parse(&["spice", "connect", "service", "logs"])
                .expect("parse")
                .produces_json()
        );
    }

    #[test]
    fn machine_mode_selects_json_for_both_status_commands() {
        let mut connect = parse(&["spice", "connect", "status"]).expect("parse");
        connect.apply_machine_mode();
        assert!(connect.produces_json());

        let mut connect = parse(&["spice", "connect", "service", "status"]).expect("parse");
        connect.apply_machine_mode();
        assert!(connect.produces_json());

        // A lifecycle action has no structured form, so machine mode leaves it
        // alone rather than inventing one.
        let mut connect = parse(&["spice", "connect", "service", "restart"]).expect("parse");
        connect.apply_machine_mode();
        assert!(!connect.produces_json());
    }

    #[test]
    fn dir_is_global_across_every_service_action() {
        for action in [
            "install",
            "uninstall",
            "start",
            "stop",
            "restart",
            "status",
            "logs",
        ] {
            let connect = parse(&["spice", "connect", "service", action, "--dir", "/srv/edge"])
                .unwrap_or_else(|e| panic!("`--dir` must apply to `{action}`: {e}"));
            assert_eq!(connect.dir.as_deref(), Some(Path::new("/srv/edge")));
        }
    }

    #[test]
    fn a_service_action_never_accepts_a_supervisor_name() {
        // Resolution is by instance directory. Accepting a unit or label would
        // let a command control a service belonging to another instance.
        for args in [
            ["spice", "connect", "service", "restart", "some.service"],
            [
                "spice",
                "connect",
                "service",
                "logs",
                "ai.spice.cloud-connect.x",
            ],
        ] {
            parse(&args).expect_err("a service name must not be accepted");
        }
    }

    #[tokio::test]
    async fn a_directory_the_transaction_left_unconnected_is_not_started() {
        // The transaction that runs before this has already said why nothing
        // was enrolled — a cancelled login, a declined prompt — so starting an
        // unmanaged runtime here would contradict it.
        let dir = tempfile::tempdir().expect("create tempdir");
        let instance_dir = dir.path().canonicalize().expect("canonical instance");
        let ctx = crate::context::RuntimeContext::new().expect("build a runtime context");
        start_instance(&ctx, Some(&instance_dir), 0)
            .await
            .expect("an unconnected directory is not an error here");
    }

    #[test]
    fn an_explicit_instance_is_independent_of_a_custom_config_directory() {
        assert_eq!(
            requested_instance_dir(Some(Path::new("/opt/edge-1")))
                .expect("resolve explicit instance"),
            PathBuf::from("/opt/edge-1")
        );
        assert_ne!(
            requested_instance_dir(Some(Path::new("/opt/edge-1")))
                .expect("resolve explicit instance"),
            PathBuf::from("/var/lib/spice-state"),
            "an arbitrary SPICE_CONFIG_DIR must not become the working directory"
        );
    }

    #[test]
    fn equivalent_spellings_reduce_to_one_instance_directory() {
        // The service name is a digest of this path, so two spellings of one
        // directory must not become two services.
        for spelling in [
            "/srv/edge",
            "/srv/./edge",
            "/srv/other/../edge",
            "/srv//edge",
        ] {
            assert_eq!(
                requested_instance_dir(Some(Path::new(spelling)))
                    .expect("resolve instance spelling"),
                PathBuf::from("/srv/edge"),
                "{spelling}"
            );
        }
    }

    #[test]
    fn a_symlinked_instance_directory_resolves_to_its_target() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let real = dir.path().join("real");
        std::fs::create_dir_all(&real).expect("create the instance directory");
        let link = dir.path().join("link");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&real, &link).expect("symlink the instance directory");
        #[cfg(not(unix))]
        std::fs::create_dir_all(&link).expect("stand in for a symlink");

        // Both spellings name one directory, so both must derive one service.
        let through_link = requested_instance_dir(Some(&link)).expect("resolve link");
        let direct = requested_instance_dir(Some(&real)).expect("resolve target");
        #[cfg(unix)]
        assert_eq!(through_link, direct);
        #[cfg(not(unix))]
        assert_ne!(through_link, direct);
    }

    #[cfg(unix)]
    #[test]
    fn an_unresolved_leaf_after_a_symlink_uses_the_filesystem_parent() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let real_parent = dir.path().join("real");
        let real_target = real_parent.join("nested");
        std::fs::create_dir_all(&real_target).expect("create symlink target");
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&real_target, &link).expect("create symlink");

        let unresolved = link.join("..").join("new-instance");
        assert!(!unresolved.exists(), "the leaf must exercise the fallback");
        let resolved =
            canonicalize_instance_dir(&unresolved).expect("resolve existing symlink prefix");
        let filesystem_parent = canonicalize_instance_dir(&real_parent.join("new-instance"))
            .expect("resolve the expected filesystem parent");
        let textually_normalized = canonicalize_instance_dir(&dir.path().join("new-instance"))
            .expect("resolve the textually normalized path");
        assert_eq!(
            resolved, filesystem_parent,
            "the unresolved leaf must follow the symlink before applying its parent component"
        );
        assert_ne!(
            resolved, textually_normalized,
            "textual normalization would select the wrong instance"
        );
    }

    #[test]
    fn a_leading_parent_component_is_not_silently_dropped() {
        // Nothing to pop, and rewriting the path would name a different
        // directory than the caller asked for.
        assert_eq!(
            canonicalize_instance_dir(Path::new("../unresolvable/edge"))
                .expect("keep a relative path unchanged"),
            PathBuf::from("../unresolvable/edge")
        );
    }

    #[test]
    fn resolved_endpoint_prefers_the_explicit_flag() {
        let dir = tempfile::tempdir().expect("create tempdir");
        assert_eq!(
            resolved_endpoint(dir.path(), Some("https://explicit.example"))
                .expect("resolve explicit endpoint"),
            "https://explicit.example"
        );
    }

    #[test]
    fn resolved_endpoint_uses_an_unbound_legacy_override() {
        let dir = tempfile::tempdir().expect("create tempdir");

        // Nothing on disk: the built-in default.
        assert_eq!(
            resolved_endpoint(dir.path(), None).expect("resolve default endpoint"),
            runtime_cloud_connect::config::DEFAULT_ENDPOINT
        );

        // Before a durable binding exists, the legacy operator-authored file
        // is the only record of a private control plane.
        std::fs::write(
            dir.path().join(CLOUD_ENDPOINT_FILE),
            "https://override.example\n",
        )
        .expect("write override");
        assert_eq!(
            resolved_endpoint(dir.path(), None).expect("resolve endpoint with unbound file"),
            "https://override.example"
        );

        // A blank override is not an endpoint.
        std::fs::write(dir.path().join(CLOUD_ENDPOINT_FILE), "  \n").expect("write blank override");
        assert_eq!(
            resolved_endpoint(dir.path(), None).expect("resolve endpoint with blank file"),
            runtime_cloud_connect::config::DEFAULT_ENDPOINT
        );
    }

    #[cfg(unix)]
    #[test]
    fn resolved_endpoint_fails_closed_on_an_unreadable_override() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().expect("create tempdir");
        let target = dir.path().join("redirected-endpoint");
        std::fs::write(&target, "https://wrong-control-plane.example")
            .expect("write target endpoint");
        symlink(&target, dir.path().join(CLOUD_ENDPOINT_FILE)).expect("create endpoint symlink");

        let error = resolved_endpoint(dir.path(), None)
            .expect_err("an unsafe override must not fall back to the production endpoint");

        assert!(
            error.to_string().contains("could not be read safely")
                || error
                    .to_string()
                    .contains("read the Cloud Connect endpoint override"),
            "{error}"
        );
        assert!(!error.to_string().contains("wrong-control-plane"));
    }

    #[tokio::test]
    async fn local_status_reports_an_unreadable_identity_when_endpoint_resolution_fails() {
        let dir = tempfile::tempdir().expect("create tempdir");
        std::fs::write(dir.path().join(IDENTITY_FILE), "not valid identity JSON")
            .expect("write malformed identity");

        let status = collect_status(dir.path(), dir.path(), None)
            .await
            .expect("local status collection remains available");

        assert_eq!(status.connection.state, status::ConnectionState::Unreadable);
        assert_eq!(
            status.connection.endpoint,
            runtime_cloud_connect::config::DEFAULT_ENDPOINT
        );
    }

    #[tokio::test]
    async fn removal_does_not_touch_state_without_enrollment_transaction_ownership() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let config_dir = dir
            .path()
            .canonicalize()
            .expect("canonical tempdir")
            .join(".spice");
        let active =
            runtime_cloud_connect::EnrollmentTransactionLock::try_acquire_async(&config_dir)
                .await
                .expect("hold enrollment transaction");

        let draft_path = runtime_cloud_connect::EnrollmentDraft::path_in(&config_dir);
        let cache_path = config_dir.join(runtime_cloud_connect::secret_cache::SECRET_CACHE_FILE);
        let endpoint_path = config_dir.join(CLOUD_ENDPOINT_FILE);
        let state = [
            (&draft_path, b"active-draft".as_slice()),
            (&cache_path, b"active-cache".as_slice()),
            (&endpoint_path, b"https://active.example".as_slice()),
        ];
        for (path, contents) in state {
            std::fs::write(path, contents).expect("write active state");
        }

        let error = remove_identity(dir.path(), &config_dir, None, true, false)
            .await
            .expect_err("active enrollment owns removal transaction");
        assert!(
            error.to_string().contains("Another live process"),
            "{error}"
        );
        assert_eq!(
            std::fs::read(&draft_path).expect("draft remains"),
            b"active-draft"
        );
        assert_eq!(
            std::fs::read(&cache_path).expect("cache remains"),
            b"active-cache"
        );
        assert_eq!(
            std::fs::read(&endpoint_path).expect("endpoint remains"),
            b"https://active.example"
        );

        drop(active);
    }

    #[tokio::test]
    async fn removal_without_an_identity_requires_explicit_local_recovery() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let config_dir = dir
            .path()
            .canonicalize()
            .expect("canonical tempdir")
            .join(".spice");
        std::fs::create_dir_all(&config_dir).expect("create config dir");
        let endpoint_path = config_dir.join(CLOUD_ENDPOINT_FILE);
        std::fs::write(&endpoint_path, "https://cloud.example")
            .expect("write Cloud-bearing local state");

        let error = remove_identity(dir.path(), &config_dir, None, true, false)
            .await
            .expect_err("missing identity must not imply Cloud cleanup succeeded");

        assert!(error.to_string().contains("No usable Spice Cloud identity"));
        assert_eq!(
            std::fs::read_to_string(&endpoint_path).expect("endpoint state remains"),
            "https://cloud.example"
        );
    }

    #[tokio::test]
    async fn forced_local_recovery_removes_the_previous_cloud_generation() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let config_dir = dir
            .path()
            .canonicalize()
            .expect("canonical tempdir")
            .join(".spice");
        std::fs::create_dir_all(&config_dir).expect("create config dir");
        let artifacts = [
            IDENTITY_FILE,
            "enrollment-draft.json",
            "enrollment-draft.quarantine.1.2.json",
            ".enrollment-draft.json.93f1f89a-e2b7-4597-b01d-6e955efb8de8.tmp",
            ".identity.json.ae2284bb-7147-4a82-8816-37fe41f401d8.tmp",
            runtime_cloud_connect::secret_cache::SECRET_CACHE_FILE,
            runtime_cloud_connect::config::CLOUD_MANAGED_SPICEPOD_FILE,
            "spicepod-cloud-managed.incoming.yml",
            "spicepod-cloud-managed.bak",
            runtime_cloud_connect::config::DEPLOYMENT_TRANSACTION_FILE,
            runtime_cloud_connect::config::DEPLOYMENT_TRANSACTION_INCOMING_FILE,
            runtime_cloud_connect::config::PREVIOUS_CLOUD_MANAGED_SPICEPOD_FILE,
            runtime_cloud_connect::config::PREVIOUS_SECRET_CACHE_FILE,
            runtime_cloud_connect::config::INCOMING_SECRET_CACHE_FILE,
        ]
        .map(|file| config_dir.join(file));
        for artifact in &artifacts {
            std::fs::write(artifact, b"previous Cloud generation")
                .expect("write previous Cloud artifact");
        }
        let operator_owned = [
            ".identity.json.manual.bak",
            ".enrollment-draft.json.notes.bak",
            "enrollment-draft.quarantine.notes.2.json",
        ]
        .map(|file| config_dir.join(file));
        for path in &operator_owned {
            std::fs::write(path, b"operator backup").expect("write operator-owned backup");
        }

        remove_identity(dir.path(), &config_dir, None, true, true)
            .await
            .expect("forced local recovery succeeds");

        for artifact in artifacts {
            assert!(
                !artifact.exists(),
                "successful recovery must remove {}",
                artifact.display()
            );
        }

        for path in operator_owned {
            assert!(
                path.exists(),
                "local recovery must preserve operator-owned {}",
                path.display()
            );
        }
    }
}
