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

//! `spice connect` — Spice Cloud Connect instance management.
//!
//! Two distinct use cases share this command:
//!
//! 1. **Cloud Connect instance state** (remote management of `spiced` from
//!    Spice Cloud). Enrollment itself is performed by the runtime: mint an
//!    enrollment key in the Spice Cloud portal and start the runtime with
//!    it (`spiced --token <enrollment-key>`); a directory with an enrolled
//!    identity reconnects automatically on every later start. This command
//!    inspects and manages that per-directory state: `status` reports it,
//!    `service` installs and manages the persistent service that keeps it
//!    running, and `remove` releases the instance and clears it.
//!
//! 2. **Deprecated pod-add behavior**: when the argument is a Spicepod
//!    path on Spice.ai Cloud (e.g. `spiceai/quickstart`), this prints a
//!    deprecation notice and behaves like `spice add <pod>` with Spice.ai
//!    Cloud authentication headers.

mod service;
mod status;

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::commands::add::{AddArgs, execute_add_or_connect};
use crate::context::RuntimeContext;
use crate::error::{Error, Result};
use crate::output::OutputFormat;
use clap::{Args, Subcommand};
use runtime_cloud_connect::config::{CloudConnectConfig, IDENTITY_FILE};

use status::ConnectStatus;

/// File (relative to the config dir) holding a `--endpoint` override so later
/// `spiced` starts reach the same control plane the enroll did.
const CLOUD_ENDPOINT_FILE: &str = "cloud-endpoint";

/// Arguments for the `spice connect` command.
#[derive(Args, Debug)]
#[command(
    about = "Manage this host's Spice Cloud Connect state (or add a cloud-hosted Spicepod)",
    long_about = r#"`spice connect` manages this directory's Spice Cloud Connect state.

Enrollment is performed by the runtime, not this command: mint an enrollment
key in the Spice Cloud portal and start the runtime with it —

  spiced --token <enrollment-key>

The runtime enrolls before it serves traffic, stores the issued identity under
`.spice/`, and every later `spiced` or `spice run` start in that directory
reconnects automatically from the identity alone.

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
  spice connect remove                    Release this instance: report the
                                          release to Spice Cloud, uninstall the
                                          service when one was installed, and
                                          clear the local identity on disk.
                                          A running `spiced` keeps its
                                          in-memory identity until it is
                                          restarted or the cloud sends a Remove
                                          command (a mere stream drop just
                                          reconnects with the same identity),
                                          so restart spiced to stop remote
                                          management immediately.

Use `--dir <path>` to manage an instance rooted at a different directory:
per-instance state lives under `<dir>/.spice`, so multiple instances on one
host enroll independently. `SPICE_CONFIG_DIR` overrides the derived location
entirely and wins over `--dir`.

A service needs either Linux with systemd or macOS with launchd. Containers
pass the enrollment key directly to the runtime (`spiced --token`) under the
container runtime's restart policy; Windows enrolls and runs under the user's
own supervisor.

DEPRECATED POD-ADD BEHAVIOR:
  spice connect <org>/<pod>               Deprecated; use `spice add <org>/<pod>`.

EXAMPLES
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
    pub target: Option<String>,

    /// Override the Spice Cloud endpoint used when inspecting state or
    /// reporting a release. Defaults to `https://api.spice.ai`. Also
    /// configurable via `SPICE_CLOUD_ENDPOINT`.
    #[arg(long, value_name = "URL")]
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

    /// Release this instance: report the release to Spice Cloud, uninstall an
    /// installed service, and clear the local identity. spiced will continue
    /// running unmanaged after the next restart.
    Remove,

    /// Install and manage the persistent service for this instance directory.
    ///
    /// `svc` is a hidden alias for interactive typing; `service` is the only
    /// documented spelling.
    #[command(alias = "svc")]
    Service(service::cli::ServiceArgs),
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
        return match cmd {
            ConnectCommand::Status(status_args) => {
                print_status(&config_dir, args.endpoint.as_deref(), status_args.output).await
            }
            ConnectCommand::Remove => {
                remove_identity(&config_dir, args.endpoint.as_deref(), args.yes).await
            }
            ConnectCommand::Service(service_args) => {
                service::cli::execute(
                    ctx,
                    service_args,
                    &instance_dir_for(&config_dir),
                    &config_dir,
                    &resolved_endpoint(&config_dir, args.endpoint.as_deref()),
                )
                .await
            }
        };
    }

    // Rejected on the Cloud Connect branches only. The deprecated pod-add
    // fallthrough below is a Spice.ai Cloud fetch, where `--cloud-region` has
    // always been meaningful, so refusing it there would be a regression.
    let Some(target) = args.target.as_deref() else {
        reject_cloud_region(args.cloud_region.as_deref())?;
        return connect_existing(&config_dir, args.endpoint.as_deref()).await;
    };

    // A secret must never ride a positional argument. Reject canonical keys
    // and close near misses — and never echo either — instead of falling
    // through to the pod-add path, which would treat the key as a pod name and
    // reproduce it in errors and requests.
    if runtime_cloud_connect::enrollment_key::looks_like_enrollment_key(target) {
        return Err(Error::InvalidArgument {
            message: "An enrollment key is not accepted as a positional argument. \
                      Enrollment is performed by the runtime: start it with \
                      `spiced --token <enrollment-key>` from the instance directory. \
                      See: https://spiceai.org/docs"
                .to_string(),
        });
    }

    // Deprecated pod-add behavior, forwarded to `spice add`.
    eprintln!(
        "warning: `spice connect <org>/<pod>` is deprecated and will be removed in a future release; use `spice add {target}` instead."
    );
    let add_args = AddArgs {
        pod_path: target.to_string(),
    };
    execute_add_or_connect(ctx, add_args, true).await
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

/// Bare `spice connect` (no subcommand, no pod path): report the existing
/// per-directory state.
///
/// A directory with no identity has nothing this command can act on —
/// enrollment belongs to the runtime — so it errors with the exact command that
/// does enroll.
async fn connect_existing(config_dir: &Path, endpoint: Option<&str>) -> Result<()> {
    if has_enrolled_identity(config_dir)? {
        println!("This host is already enrolled with Spice Cloud.");
        println!();
        return print_status(config_dir, endpoint, OutputFormat::Table).await;
    }

    Err(Error::InvalidArgument {
        message: format!(
            "This directory ({}) is not connected to Spice Cloud. Mint an enrollment key in \
             the Spice Cloud portal and start the runtime with it: \
             `spiced --token <enrollment-key>`. Later starts reconnect automatically from the \
             stored identity. See: https://spiceai.org/docs",
            instance_dir_for(config_dir).display()
        ),
    })
}

/// Whether this directory holds a usable enrolled identity.
///
/// Existence alone is not enough: an unreadable identity would be reported as
/// enrolled, but every `spiced` restart would reject it and run without Cloud
/// Connect.
fn has_enrolled_identity(config_dir: &Path) -> Result<bool> {
    runtime_cloud_connect::identity::IdentityStore::load_optional(&config_dir.join(IDENTITY_FILE))
        .map(|identity| identity.is_some())
        .map_err(|e| Error::CloudConnectIo {
            message: format!("load identity: {e}"),
        })
}

/// The instance directory a config dir belongs to: `<dir>/.spice` → `<dir>`.
///
/// `SPICE_CONFIG_DIR` can point anywhere, so a config dir that is not named
/// `.spice` has no instance directory above it — in that case the config dir
/// itself is the best available answer for the service's working directory.
fn instance_dir_for(config_dir: &Path) -> PathBuf {
    if config_dir.file_name() == Some(std::ffi::OsStr::new(".spice"))
        && let Some(parent) = config_dir.parent().filter(|p| !p.as_os_str().is_empty())
    {
        return parent.to_path_buf();
    }
    config_dir.to_path_buf()
}

/// Collect and render one status snapshot.
async fn print_status(
    config_dir: &Path,
    endpoint: Option<&str>,
    output: OutputFormat,
) -> Result<()> {
    let status = ConnectStatus::collect(
        service::backend(),
        &instance_dir_for(config_dir),
        config_dir,
        &resolved_endpoint(config_dir, endpoint),
    )
    .await;
    status::render(&status, output)?;
    if status.is_degraded() {
        return Err(Error::ServiceUnavailable {
            message: format!(
                "The Spice Cloud Connect service for {} is {}{}",
                status.connection.directory.display(),
                status.service.state,
                match &status.service.diagnostic {
                    Some(diagnostic) => format!(": {diagnostic}"),
                    None => ".".to_string(),
                }
            ),
        });
    }
    Ok(())
}

/// What Spice Cloud said about the release, reduced to the only question that
/// decides whether local state may be cleared.
#[derive(Debug)]
enum ReleaseVerdict {
    /// The cloud confirmed the release, or confirmed this instance is not
    /// there. Either way the credential on disk is no longer usable and can go.
    Confirmed {
        outcome: runtime_cloud_connect::release::ReleaseOutcome,
    },
    /// The cloud could not be reached, or refused in a way that leaves the
    /// instance registered. Local state must survive so a retry can finish the
    /// removal.
    Unconfirmed { reason: String },
}

/// Report this instance's release to Spice Cloud and classify the answer.
///
/// The classification is the whole point: clearing the identity is what makes a
/// removal unrecoverable, so it happens only once the cloud has said the
/// instance is released or already gone. A network blip must leave a directory
/// a retry can finish from — the alternative silently orphans a registry row
/// that nobody local can release any more.
async fn release_instance(
    config_dir: &Path,
    endpoint: Option<&str>,
    identity: &runtime_cloud_connect::Identity,
) -> ReleaseVerdict {
    let endpoint = resolved_endpoint(config_dir, endpoint);
    let ca = (!identity.ca_bundle_pem.is_empty()).then_some(identity.ca_bundle_pem.as_str());

    match runtime_cloud_connect::release::release(&endpoint, identity, ca).await {
        Ok(outcome) => ReleaseVerdict::Confirmed { outcome },
        // The cloud has no such instance: the same end state the release was
        // asking for, including the case where it was already deleted in the
        // portal.
        Err(runtime_cloud_connect::release::Error::Rejected { status, .. })
            if status == 404 || status == 410 =>
        {
            ReleaseVerdict::Confirmed {
                outcome: runtime_cloud_connect::release::ReleaseOutcome {
                    status: "removed".to_string(),
                    app_name: None,
                },
            }
        }
        Err(err) => ReleaseVerdict::Unconfirmed {
            reason: format!(
                "{err} Nothing was removed locally: the identity, delivered secrets, and any \
                 installed service are intact so `spice connect remove` can finish the removal. \
                 If Spice Cloud cannot accept the release at all, delete the instance in the \
                 Spice Cloud portal and re-run this command — a released instance is confirmed \
                 absent and its local state is then cleared. \
                 See: https://spiceai.org/docs"
            ),
        },
    }
}

/// Release this instance: report the release to Spice Cloud, uninstall an
/// installed service, and clear the local identity and staged state.
///
/// Ordered so that no step is taken on the strength of a guess. The release is
/// reported first and its answer decides everything after it: a confirmed
/// release (or a confirmed absence) is what makes the local credential dead and
/// safe to delete, while an unconfirmed one leaves the directory exactly as it
/// was for a retry to finish.
async fn remove_identity(
    config_dir: &Path,
    endpoint: Option<&str>,
    assume_yes: bool,
) -> Result<()> {
    // Own the complete state transition before inspecting any file. Without
    // this boundary, removal can clear old state while enrollment is still
    // promoting a replacement identity under the same directory lock.
    let enrollment_transaction = Arc::new(
        runtime_cloud_connect::EnrollmentTransactionLock::try_acquire_async(config_dir)
            .await
            .map_err(|e| Error::CloudConnectIo {
                message: format!("acquire the enrollment transaction before removal: {e}"),
            })?,
    );

    let identity_path = config_dir.join(IDENTITY_FILE);
    let draft_path = runtime_cloud_connect::EnrollmentDraft::path_in(config_dir);
    let endpoint_path = config_dir.join(CLOUD_ENDPOINT_FILE);
    let cache_path = config_dir.join(runtime_cloud_connect::secret_cache::SECRET_CACHE_FILE);
    let instance_dir = instance_dir_for(config_dir);
    let backend = service::backend();

    let identity = runtime_cloud_connect::identity::IdentityStore::load_optional(&identity_path)
        .map_err(|e| Error::CloudConnectIo {
            message: format!("load identity: {e}"),
        })?;
    let installed = service::resolve(backend, &instance_dir, config_dir)?;

    let had_identity = identity.is_some();
    let had_draft = draft_path.exists();
    let had_endpoint = endpoint_path.exists();
    let had_cache = cache_path.exists();

    if !had_identity && !had_draft && !had_endpoint && !had_cache && installed.is_none() {
        println!("Spice Cloud Connect: nothing to remove.");
        return Ok(());
    }

    // Confirm before touching a running service: stopping it takes the
    // instance offline, and an operator who ran this in the wrong directory
    // needs the chance to say no. Name what will be affected.
    if !assume_yes {
        println!("This will release this instance from Spice Cloud and remove it from this host:");
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
        if !confirm("Continue?")? {
            println!(
                "Nothing was removed — the service is still running and the local state is intact."
            );
            return Ok(());
        }
    }

    // Reported before anything is cleared: the identity leaf is the credential
    // that authorises the release, so it has to still exist.
    if let Some(ref identity) = identity {
        match release_instance(config_dir, endpoint, identity).await {
            ReleaseVerdict::Confirmed { outcome } => {
                println!("Released this instance in Spice Cloud.");
                if !outcome.status.is_empty() {
                    println!("  registry status: {}", outcome.status);
                }
                if let Some(app) = outcome.app_name {
                    println!(
                        "  app {app} is paused — its deploy target was removed. Move it to \
                         another instance, or delete it, in the Spice Cloud portal."
                    );
                }
            }
            ReleaseVerdict::Unconfirmed { reason } => {
                return Err(Error::CloudConnectIo {
                    message: format!("release this instance in Spice Cloud: {reason}"),
                });
            }
        }
    }

    // A service left running against a released identity restarts forever, so
    // this is the step that most needs to happen — but a failure must not abort
    // the command before the identity is cleared. The cloud has already
    // released the instance by this point, so keeping a dead credential on disk
    // is strictly worse than reporting the uninstall failure at the end. The
    // uninstall primitive is shared with `spice connect service uninstall`;
    // what is not shared is the identity, which only this command releases.
    let uninstall_failure = match service::uninstall(backend, &instance_dir, config_dir) {
        Ok(Some(removed)) => {
            println!("Stopped and uninstalled {}.", removed.name);
            None
        }
        Ok(None) => None,
        Err(err) => Some(err),
    };

    // Before the identity: the cache holds the app's secrets and the identity
    // holds the only key that opens them, so deleting the key first would leave
    // an unopenable file that still has to be removed. Deleting the secrets
    // first means an interrupted `remove` never leaves them behind.
    if had_cache {
        runtime_cloud_connect::secret_cache::remove(&cache_path).map_err(|e| {
            Error::CloudConnectIo {
                message: format!("remove the delivered-secrets cache: {e}"),
            }
        })?;
    }

    if had_identity {
        // Clearing this also destroys the cache key, which is only in this file.
        runtime_cloud_connect::identity::IdentityStore::clear(&identity_path).map_err(|e| {
            Error::CloudConnectIo {
                message: format!("clear identity: {e}"),
            }
        })?;
    }

    // An unfinished enrollment's draft holds only this directory's
    // provisional key material and operation ID — released along with
    // everything else so the next enrollment starts clean.
    if had_draft {
        enrollment_transaction
            .delete_draft_async()
            .await
            .map_err(|e| Error::CloudConnectIo {
                message: format!("remove enrollment draft: {e}"),
            })?;
    }
    // Also clear any `cloud-endpoint` override so a later enrollment
    // without `SPICE_CLOUD_ENDPOINT` doesn't silently keep using the stale
    // endpoint.
    if had_endpoint
        && let Err(e) = std::fs::remove_file(&endpoint_path)
        && e.kind() != std::io::ErrorKind::NotFound
    {
        return Err(Error::CloudConnectIo {
            message: format!("remove endpoint override: {e}"),
        });
    }

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

/// Resolve the endpoint `spiced` will actually contact, mirroring the
/// precedence used at runtime: an explicit `--endpoint` first, then the
/// `SPICE_CLOUD_ENDPOINT` env var, then the on-disk `cloud-endpoint` override
/// in the config dir, then the built-in default.
fn resolved_endpoint(config_dir: &Path, explicit: Option<&str>) -> String {
    if let Some(endpoint) = explicit.filter(|e| !e.is_empty()) {
        return endpoint.to_string();
    }
    if let Ok(env) = std::env::var("SPICE_CLOUD_ENDPOINT")
        && !env.is_empty()
    {
        return env;
    }
    if let Ok(s) = std::fs::read_to_string(config_dir.join(CLOUD_ENDPOINT_FILE)) {
        let trimmed = s.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    runtime_cloud_connect::config::DEFAULT_ENDPOINT.to_string()
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

    #[test]
    fn instance_dir_is_the_parent_of_a_dot_spice_config_dir() {
        assert_eq!(
            instance_dir_for(Path::new("/opt/edge-1/.spice")),
            PathBuf::from("/opt/edge-1")
        );
    }

    #[test]
    fn instance_dir_falls_back_to_a_custom_config_dir() {
        // SPICE_CONFIG_DIR can point anywhere; a directory not named `.spice`
        // has no instance directory above it to infer.
        assert_eq!(
            instance_dir_for(Path::new("/var/lib/spice-state")),
            PathBuf::from("/var/lib/spice-state")
        );
    }

    #[test]
    fn a_malformed_identity_is_not_reported_as_enrolled() {
        let dir = tempfile::tempdir().expect("create tempdir");
        std::fs::write(dir.path().join(IDENTITY_FILE), "not valid JSON")
            .expect("write malformed identity");

        let error = has_enrolled_identity(dir.path())
            .expect_err("a malformed identity must not read as enrolled");
        assert!(error.to_string().contains("load identity"), "{error}");
    }

    #[test]
    fn resolved_endpoint_prefers_the_explicit_flag() {
        let dir = tempfile::tempdir().expect("create tempdir");
        assert_eq!(
            resolved_endpoint(dir.path(), Some("https://explicit.example")),
            "https://explicit.example"
        );
    }

    #[test]
    fn resolved_endpoint_reads_the_on_disk_override_then_the_default() {
        let dir = tempfile::tempdir().expect("create tempdir");

        // Nothing on disk: the built-in default.
        assert_eq!(
            resolved_endpoint(dir.path(), None),
            runtime_cloud_connect::config::DEFAULT_ENDPOINT
        );

        // The override file wins over the default.
        std::fs::write(
            dir.path().join(CLOUD_ENDPOINT_FILE),
            "https://override.example\n",
        )
        .expect("write override");
        assert_eq!(
            resolved_endpoint(dir.path(), None),
            "https://override.example"
        );

        // A blank override is not an endpoint.
        std::fs::write(dir.path().join(CLOUD_ENDPOINT_FILE), "  \n").expect("write blank override");
        assert_eq!(
            resolved_endpoint(dir.path(), None),
            runtime_cloud_connect::config::DEFAULT_ENDPOINT
        );
    }

    #[tokio::test]
    async fn removal_does_not_touch_state_without_enrollment_transaction_ownership() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let config_dir = dir.path().join(".spice");
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

        let error = remove_identity(&config_dir, None, true)
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
    async fn an_unconfirmed_release_keeps_every_piece_of_local_state() {
        // The acceptance criterion: a transient Cloud failure must leave a
        // directory a retry can finish the removal from. Clearing the identity
        // first would orphan a registry row nobody local can release any more.
        let dir = tempfile::tempdir().expect("create tempdir");
        let config_dir = dir.path().join(".spice");
        std::fs::create_dir_all(&config_dir).expect("create config dir");

        let identity_path = config_dir.join(IDENTITY_FILE);
        let identity = runtime_cloud_connect::Identity {
            identifier: "inst_test".to_string(),
            ..test_identity()
        };
        runtime_cloud_connect::identity::IdentityStore::store(&identity_path, &identity)
            .expect("store identity");
        let cache_path = config_dir.join(runtime_cloud_connect::secret_cache::SECRET_CACHE_FILE);
        std::fs::write(&cache_path, b"delivered-secrets").expect("write cache");

        // A port nothing listens on: the release cannot be reported, which is
        // exactly the transient failure under test.
        let error = remove_identity(&config_dir, Some("http://127.0.0.1:1"), true)
            .await
            .expect_err("an unconfirmed release must fail the removal");
        assert!(
            error.to_string().contains("Nothing was removed locally"),
            "{error}"
        );
        assert!(identity_path.exists(), "the identity must survive");
        assert!(cache_path.exists(), "the delivered secrets must survive");
    }

    /// A syntactically complete identity whose key material is never used: the
    /// removal path under test fails before it can sign anything.
    fn test_identity() -> runtime_cloud_connect::Identity {
        runtime_cloud_connect::Identity {
            identifier: "inst_test".to_string(),
            identity_cert_pem: "-----BEGIN CERTIFICATE-----\nAA==\n-----END CERTIFICATE-----\n"
                .to_string(),
            private_key_pem: "-----BEGIN PRIVATE KEY-----\nAA==\n-----END PRIVATE KEY-----\n"
                .to_string(),
            public_key_pem: "-----BEGIN PUBLIC KEY-----\nAA==\n-----END PUBLIC KEY-----\n"
                .to_string(),
            ca_bundle_pem: String::new(),
            gateway_addr: "gateway.example:443".to_string(),
            not_after_unix: None,
            enc_private_key_pem: String::new(),
            enc_public_key_pem: String::new(),
            enc_previous_private_key_pem: String::new(),
            cache_key_b64: String::new(),
            app_id: None,
            org_name: None,
            app_name: None,
            monitor_url: None,
        }
    }
}
