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
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::commands::add::{AddArgs, execute_add_or_connect};
use crate::context::RuntimeContext;
use crate::error::{Error, Result};
use crate::output::OutputFormat;
use clap::{Args, Subcommand};
use runtime_cloud_connect::config::{CloudConnectConfig, IDENTITY_FILE};
use runtime_cloud_connect::enrollment_key::EnrollmentKey;
use zeroize::Zeroizing;

use service::ServiceState;
use status::{ConnectStatus, ConnectionState};

/// File (relative to the config dir) holding a `--endpoint` override so later
/// `spiced` starts reach the same control plane the enroll did.
const CLOUD_ENDPOINT_FILE: &str = "cloud-endpoint";

/// Arguments for the `spice connect` command.
#[derive(Args, Debug)]
#[command(
    about = "Connect this directory to Spice Cloud and start its instance",
    long_about = r#"`spice connect` enrolls this directory with Spice Cloud and manages its instance.

With a logged-in user session, the command resolves one owner/admin
organization, enrolls the local instance, and atomically creates and attaches a
new project. Without a login, an interactive terminal offers inline login
(recommended) or secure enrollment-key entry. Enrollment-key mode always
leaves the instance unattached and prints the Cloud-provided recovery link.

The transaction is retry-safe: an interrupted enrollment reuses its durable
operation and key material, while project creation uses the enrolled instance's
single attachment as its exact replay key. Existing identities always win and
are never duplicated. A re-run continues the pending enrollment in the mode and
organization that started it — it never asks which authentication to use again,
and an enrollment key is asked for again only because keys are never stored.

NON-INTERACTIVE
  Login mode requires both --org <org> and --project <name>.
  Key mode requires --token <enrollment-key> and rejects --project.
  A pending enrollment needs only what it cannot recover: --token
  <enrollment-key> for a key operation, --project <name> for a login one.

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
  spice connect
  spice connect --org acme --project retail-analytics
  spice connect --token <enrollment-key> --org acme
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
    pub target: Option<String>,

    /// Organization to enroll into, or the expected organization for an
    /// enrollment key. Login mode validates owner/admin membership.
    #[arg(long, value_name = "SLUG")]
    pub org: Option<String>,

    /// Project to create and attach. Required for non-interactive login mode;
    /// rejected in enrollment-key mode.
    #[arg(long, value_name = "SLUG")]
    pub project: Option<String>,

    /// One-time enrollment key. The value is redacted from Debug/errors and is
    /// never persisted. This mode always remains unattached.
    #[arg(long, value_name = "SECRET")]
    pub token: Option<EnrollmentKeyArgument>,

    /// Declared location label for the enrolled instance.
    #[arg(long, value_name = "LABEL")]
    pub region: Option<String>,

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

/// A raw enrollment-key argument whose command-line parsing cannot echo an
/// invalid secret. Validation happens after `clap` has rendered its own
/// diagnostics, and both `Debug` and destruction keep the raw value out of
/// logs and reusable memory.
#[derive(Clone)]
pub struct EnrollmentKeyArgument(Zeroizing<String>);

impl std::str::FromStr for EnrollmentKeyArgument {
    type Err = std::convert::Infallible;

    fn from_str(raw: &str) -> std::result::Result<Self, Self::Err> {
        Ok(Self(Zeroizing::new(raw.to_string())))
    }
}

impl std::fmt::Debug for EnrollmentKeyArgument {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("EnrollmentKeyArgument([REDACTED])")
    }
}

impl EnrollmentKeyArgument {
    fn into_enrollment_key(self) -> Result<EnrollmentKey> {
        EnrollmentKey::parse(self.0.as_str()).map_err(|source| Error::InvalidUsage {
            message: source.to_string(),
        })
    }
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
        if args.org.is_some()
            || args.project.is_some()
            || args.token.is_some()
            || args.region.is_some()
        {
            return Err(Error::InvalidUsage {
                message: "--org, --project, --token, and --region apply to enrollment, not to a `connect` subcommand.".to_string(),
            });
        }
        return match cmd {
            ConnectCommand::Status(status_args) => {
                print_status(&config_dir, args.endpoint.as_deref(), status_args.output).await
            }
            ConnectCommand::Remove => {
                remove_identity(&config_dir, args.endpoint.as_deref(), args.yes, args.force).await
            }
            ConnectCommand::Service(service_args) => {
                let endpoint = endpoint_for_local_reporting(&config_dir, args.endpoint.as_deref());
                service::cli::execute(
                    ctx,
                    service_args,
                    &instance_dir_for(&config_dir),
                    &config_dir,
                    &endpoint,
                )
                .await
            }
        };
    }

    // The deprecated pod-add fallthrough is a Spice.ai Cloud fetch, where
    // `--cloud-region` has always been meaningful, so it is not rejected here.
    if let Some(target) = args.target.as_deref() {
        if args.org.is_some()
            || args.project.is_some()
            || args.token.is_some()
            || args.region.is_some()
        {
            return Err(Error::InvalidUsage {
                message: "--org, --project, --token, and --region cannot be combined with the deprecated `connect <org>/<pod>` form.".to_string(),
            });
        }

        // A secret must never ride a positional argument. Reject canonical keys
        // and close near misses without echoing either.
        if runtime_cloud_connect::enrollment_key::looks_like_enrollment_key(target) {
            return Err(Error::InvalidArgument {
                message: "An enrollment key is not accepted as a positional argument. Pass it with `spice connect --token <enrollment-key>`. See: https://spiceai.org/docs".to_string(),
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
    let token = args
        .token
        .map(EnrollmentKeyArgument::into_enrollment_key)
        .transpose()?;
    let endpoint = args.endpoint.clone();
    let dir = args.dir.clone();
    transaction::execute(
        ctx,
        transaction::ConnectRequest {
            org: args.org,
            project: args.project,
            token,
            region: args.region,
            dir: args.dir,
            endpoint: args.endpoint,
        },
    )
    .await?;

    // The transaction has committed this directory's durable state, so the
    // command ends where the operator asked it to: at a running instance.
    start_instance(
        ctx,
        &config_dir,
        endpoint.as_deref(),
        dir.as_deref(),
        args.verbosity,
    )
    .await
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
async fn start_instance(
    ctx: &RuntimeContext,
    config_dir: &Path,
    endpoint: Option<&str>,
    dir: Option<&Path>,
    verbosity: u8,
) -> Result<()> {
    let instance_dir = instance_dir_for(config_dir);
    let status = collect_status(config_dir, endpoint).await?;

    // There is nothing to start. The transaction that ran before this has
    // already said why — an enrollment the operator cancelled, or a project
    // prompt they declined — and starting an unmanaged runtime on top of that
    // would contradict it.
    if matches!(
        status.connection.state,
        ConnectionState::NotConnected | ConnectionState::EnrollmentIncomplete
    ) {
        return Ok(());
    }

    // An identity that cannot activate Cloud Connect would start a runtime that
    // serves locally and reaches no control plane. That is reported as the
    // failure it is — `render_status` prints the diagnosis and returns it as
    // this command's error — rather than started as if it were a connection.
    if matches!(
        status.connection.state,
        ConnectionState::Unreadable | ConnectionState::Unusable
    ) {
        return render_status(&status, OutputFormat::Table);
    }

    // A host where this CLI does not manage a local runtime — native Windows,
    // outside WSL — is enrolled and reconnects like any other; it just starts
    // its runtime under the operator's own supervisor. Naming that beats
    // failing a transaction that has already committed.
    if ctx.ensure_local_runtime_supported().is_err() {
        println!(
            "This directory is connected. Start its runtime with `spiced` from {} \
             (or under your own supervisor); it reconnects from the stored identity.",
            instance_dir.display()
        );
        return Ok(());
    }

    // What owns the process, and so what this command starts. An instance a
    // supervisor already owns must not also be started in the terminal: the
    // host would hold two runtimes for one identity, which is what the second
    // one's directory lock refuses — after the operator was told their instance
    // was starting.
    let backend = service::backend();
    let Some(manifest) = service::resolve(backend, &instance_dir, config_dir)? else {
        println!("Starting the Spice runtime. Press Ctrl-C to stop it.");
        return crate::runtime_launcher::run_runtime(
            ctx,
            &crate::runtime_launcher::RunConfig {
                // The instance directory resolves both the spicepod and the
                // `.spice` state this command just inspected, so the runtime
                // has to start there rather than wherever the CLI was invoked
                // from.
                working_dir: dir.map(Path::to_path_buf),
                verbosity,
                // The transaction above has already printed this instance's
                // connection block, so the runtime must not print it a second
                // time a few seconds later. A `spice run` or `spiced` start,
                // which no transaction precedes, still gets it from the
                // runtime.
                connection_report: crate::runtime_launcher::ConnectionReport::AlreadyReported,
                ..crate::runtime_launcher::RunConfig::default()
            },
        )
        .await;
    };

    // A service that is already up is the state this command asks for, so it is
    // reported without being touched. That is not just an optimization: it is
    // what makes `spice connect` answer on every host whose supervisor this
    // release cannot yet drive (launchd's lifecycle actions are still
    // unimplemented), and it is what keeps the command from interrupting a
    // serving instance. Anything else is asked to start.
    if status.service.state != ServiceState::Running {
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
async fn collect_status(config_dir: &Path, endpoint: Option<&str>) -> Result<ConnectStatus> {
    let endpoint = endpoint_for_local_reporting(config_dir, endpoint);
    Ok(ConnectStatus::collect(
        service::backend(),
        &instance_dir_for(config_dir),
        config_dir,
        &endpoint,
    )
    .await)
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

/// The canonical instance directory a config dir belongs to: `<dir>/.spice` →
/// `<dir>`.
///
/// `SPICE_CONFIG_DIR` can point anywhere, so a config dir that is not named
/// `.spice` has no instance directory above it — in that case the config dir
/// itself is the best available answer for the service's working directory.
///
/// The result is canonicalized because it *is* the service's identity: the
/// service name is a digest of this path, so `/srv/edge`, `/srv/./edge`,
/// `/srv/other/../edge`, and a symlink to any of them have to reduce to one
/// answer. Without that, one instance directory can install a second service
/// under a second name, or reject the valid manifest of the one it already has.
fn instance_dir_for(config_dir: &Path) -> PathBuf {
    let dir = if config_dir.file_name() == Some(std::ffi::OsStr::new(".spice"))
        && let Some(parent) = config_dir.parent().filter(|p| !p.as_os_str().is_empty())
    {
        parent.to_path_buf()
    } else {
        config_dir.to_path_buf()
    };
    canonicalize_instance_dir(&dir)
}

/// Reduce an instance directory to the one spelling every command must derive
/// its service name from.
///
/// `fs::canonicalize` is the authoritative answer — it resolves symlinks, which
/// a purely textual normalization cannot — but it needs the directory to exist.
/// A directory that is not there yet still has to produce a stable name (that
/// is how `status` reports on a directory before anything is created in it), so
/// the fallback normalizes `.` and `..` textually. Resolving `..` without the
/// filesystem is only safe because the input is already absolute.
fn canonicalize_instance_dir(dir: &Path) -> PathBuf {
    if let Ok(canonical) = std::fs::canonicalize(dir) {
        return canonical;
    }
    let mut normalized = PathBuf::new();
    for component in dir.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                // A leading `..` has nothing to pop, so it is kept: dropping it
                // would silently rewrite the path to a different directory.
                if !normalized.pop() {
                    normalized.push(component.as_os_str());
                }
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized
}

/// Collect and render one status snapshot.
async fn print_status(
    config_dir: &Path,
    endpoint: Option<&str>,
    output: OutputFormat,
) -> Result<()> {
    let status = collect_status(config_dir, endpoint).await?;
    render_status(&status, output)
}

/// What Spice Cloud said about the release, reduced to the only question that
/// decides whether local state may be cleared.
#[derive(Debug)]
enum ReleaseVerdict {
    /// The cloud confirmed the release, or stated that this instance is
    /// permanently gone. Either way the credential on disk is no longer usable
    /// and can go.
    Confirmed {
        outcome: runtime_cloud_connect::release::ReleaseOutcome,
    },
    /// The cloud could not be reached, or refused in a way that does not
    /// establish what happened to the instance. Local state must survive so a
    /// retry can finish the removal.
    Unconfirmed { reason: String },
}

/// The status the control plane uses to say an instance existed and is
/// permanently gone. Unlike a bare not-found, it cannot also mean "you asked
/// the wrong control plane" or "you may not see this instance".
const RELEASE_GONE_STATUS: u16 = 410;

/// Report this instance's release to Spice Cloud and classify the answer.
///
/// The classification is the whole point: clearing the identity is what makes a
/// removal unrecoverable, so it happens only once the cloud has said the
/// instance is released or permanently gone. A network blip, or an answer that
/// does not establish what happened, must leave a directory a retry can finish
/// from — the alternative silently orphans a registry row that nobody local can
/// release any more.
///
/// A `404` deliberately does **not** confirm anything. The release endpoint
/// answers not-found for an instance that belongs to another organization, and
/// for a request aimed at a control plane that never issued this identity, so
/// reading it as absence lets a mistyped `--endpoint` clear the only credential
/// for an instance that is alive in its real registry.
async fn release_instance(
    config_dir: &Path,
    endpoint: Option<&str>,
    identity: &runtime_cloud_connect::Identity,
) -> ReleaseVerdict {
    let endpoint = match resolved_endpoint(config_dir, endpoint) {
        Ok(endpoint) => endpoint,
        Err(error) => {
            return ReleaseVerdict::Unconfirmed {
                reason: format!("the persisted control-plane binding is unusable: {error}"),
            };
        }
    };
    let ca = (!identity.ca_bundle_pem.is_empty()).then_some(identity.ca_bundle_pem.as_str());

    classify_release(
        runtime_cloud_connect::release::release(&endpoint, identity, ca).await,
        &endpoint,
    )
}

/// [`release_instance`] without the request: the classification of what the
/// control plane answered.
///
/// Separated so every branch — including the `404` that must *not* confirm
/// anything — is tested without key material or a control plane.
fn classify_release(
    result: runtime_cloud_connect::release::Result<runtime_cloud_connect::release::ReleaseOutcome>,
    endpoint: &str,
) -> ReleaseVerdict {
    match result {
        Ok(outcome) => ReleaseVerdict::Confirmed { outcome },
        Err(runtime_cloud_connect::release::Error::Rejected { status, .. })
            if status == RELEASE_GONE_STATUS =>
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
                 installed service are intact, so `spice connect remove` can finish the removal \
                 once {endpoint} can be reached. If this instance is already deleted in the \
                 Spice Cloud portal — or that control plane is the wrong one for it — pass \
                 --force to clear this directory's local state without a confirmed release; the \
                 portal-side delete stays authoritative. See: https://spiceai.org/docs"
            ),
        },
    }
}

/// Release this instance: report the release to Spice Cloud, uninstall an
/// installed service, and clear the local identity and staged state.
///
/// Ordered so that no step is taken on the strength of a guess. The release is
/// reported first and its answer decides everything after it: a confirmed
/// release, or a stated permanent absence, is what makes the local credential
/// dead and safe to delete, while an unconfirmed one leaves the directory
/// exactly as it was for a retry to finish. `force` is the operator saying they
/// have decided for themselves — it is honoured only after the unconfirmed
/// reason has been printed.
async fn remove_identity(
    config_dir: &Path,
    endpoint: Option<&str>,
    assume_yes: bool,
    force: bool,
) -> Result<()> {
    let _connect_lock = runtime_cloud_connect::MutationLock::acquire(config_dir, "remove")
        .await
        .map_err(|error| Error::CloudConnectIo {
            message: error.to_string(),
        })?;
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
    let journal_path = config_dir.join(state::CONNECT_OPERATION_FILE);
    let project_journal_path = config_dir.join(state::PROJECT_OPERATION_FILE);
    let endpoint_path = config_dir.join(CLOUD_ENDPOINT_FILE);
    let cache_path = config_dir.join(runtime_cloud_connect::secret_cache::SECRET_CACHE_FILE);
    let instance_dir = instance_dir_for(config_dir);
    let backend = service::backend();

    let identity =
        runtime_cloud_connect::identity::IdentityStore::load_optional_async(identity_path.clone())
            .await
            .map_err(|e| Error::CloudConnectIo {
                message: format!("load identity: {e}"),
            })?;
    let installed = service::resolve(backend, &instance_dir, config_dir)?;

    let had_identity = identity.is_some();
    let had_draft = draft_path.exists();
    let had_journal = journal_path.exists();
    let had_project_journal = project_journal_path.exists();
    let had_endpoint = endpoint_path.exists();
    let had_cache = cache_path.exists();

    if !had_identity
        && !had_draft
        && !had_journal
        && !had_project_journal
        && !had_endpoint
        && !had_cache
        && installed.is_none()
    {
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
            ReleaseVerdict::Unconfirmed { reason } if !force => {
                return Err(Error::CloudConnectIo {
                    message: format!("release this instance in Spice Cloud: {reason}"),
                });
            }
            // Asked for explicitly, and only ever after the operator has been
            // told what could not be confirmed: the local state goes and the
            // portal-side delete is what actually releases the instance.
            ReleaseVerdict::Unconfirmed { reason } => {
                println!(
                    "Could not confirm the release with Spice Cloud: {reason} Clearing this \
                     directory's local state anyway because --force was given."
                );
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
        runtime_cloud_connect::identity::IdentityStore::clear_with_transaction_async(
            identity_path.clone(),
            Arc::clone(&enrollment_transaction),
        )
        .await
        .map_err(|e| Error::CloudConnectIo {
            message: format!("clear identity: {e}"),
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
    if had_journal {
        state::ConnectOperation::delete(config_dir).map_err(|e| Error::CloudConnectIo {
            message: format!("remove enrollment journal: {e}"),
        })?;
    }
    if had_project_journal {
        state::ProjectOperation::delete(config_dir).map_err(|e| Error::CloudConnectIo {
            message: format!("remove project assignment journal: {e}"),
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
fn endpoint_for_local_reporting(config_dir: &Path, explicit: Option<&str>) -> String {
    match resolved_endpoint(config_dir, explicit) {
        Ok(endpoint) => endpoint,
        Err(error) => {
            eprintln!(
                "Cloud Connect endpoint could not be resolved for this local operation: {error}"
            );
            runtime_cloud_connect::config::DEFAULT_ENDPOINT.to_string()
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

    #[tokio::test]
    async fn a_directory_the_transaction_left_unconnected_is_not_started() {
        // The transaction that runs before this has already said why nothing
        // was enrolled — a cancelled login, a declined prompt — so starting an
        // unmanaged runtime here would contradict it.
        let dir = tempfile::tempdir().expect("create tempdir");
        let ctx = crate::context::RuntimeContext::new().expect("build a runtime context");
        start_instance(&ctx, &dir.path().join(".spice"), None, Some(dir.path()), 0)
            .await
            .expect("an unconnected directory is not an error here");
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
    fn equivalent_spellings_reduce_to_one_instance_directory() {
        // The service name is a digest of this path, so two spellings of one
        // directory must not become two services.
        for spelling in [
            "/srv/edge/.spice",
            "/srv/./edge/.spice",
            "/srv/other/../edge/.spice",
            "/srv//edge/.spice",
        ] {
            assert_eq!(
                instance_dir_for(Path::new(spelling)),
                PathBuf::from("/srv/edge"),
                "{spelling}"
            );
        }
    }

    #[test]
    fn a_symlinked_instance_directory_resolves_to_its_target() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let real = dir.path().join("real");
        std::fs::create_dir_all(real.join(".spice")).expect("create the instance directory");
        let link = dir.path().join("link");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&real, &link).expect("symlink the instance directory");
        #[cfg(not(unix))]
        std::fs::create_dir_all(&link).expect("stand in for a symlink");

        // Both spellings name one directory, so both must derive one service.
        let through_link = instance_dir_for(&link.join(".spice"));
        let direct = instance_dir_for(&real.join(".spice"));
        #[cfg(unix)]
        assert_eq!(through_link, direct);
        #[cfg(not(unix))]
        assert_ne!(through_link, direct);
    }

    #[test]
    fn a_leading_parent_component_is_not_silently_dropped() {
        // Nothing to pop, and rewriting the path would name a different
        // directory than the caller asked for.
        assert_eq!(
            canonicalize_instance_dir(Path::new("../unresolvable/edge")),
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

        let status = collect_status(dir.path(), None)
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

        let error = remove_identity(&config_dir, None, true, false)
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

    #[test]
    fn a_not_found_release_never_confirms_an_absence() {
        // The release endpoint answers not-found for an instance in another
        // organization, and for a request aimed at a control plane that never
        // issued this identity. Reading either as absence lets a mistyped
        // --endpoint clear the only credential for a live instance.
        for status in [400, 401, 403, 404, 409, 500, 503] {
            let verdict = classify_release(
                Err(runtime_cloud_connect::release::Error::Rejected {
                    status,
                    message: "not found".to_string(),
                }),
                "https://api.example",
            );
            assert!(
                matches!(verdict, ReleaseVerdict::Unconfirmed { .. }),
                "HTTP {status} must not confirm anything: {verdict:?}"
            );
        }
    }

    #[test]
    fn only_a_stated_permanent_absence_confirms_without_a_release() {
        // `410 Gone` says the instance existed and is permanently gone, which a
        // cross-organization or wrong-control-plane request does not produce.
        let verdict = classify_release(
            Err(runtime_cloud_connect::release::Error::Rejected {
                status: RELEASE_GONE_STATUS,
                message: "gone".to_string(),
            }),
            "https://api.example",
        );
        match verdict {
            ReleaseVerdict::Confirmed { outcome } => assert_eq!(outcome.status, "removed"),
            other @ ReleaseVerdict::Unconfirmed { .. } => {
                panic!("410 must confirm the end state: {other:?}")
            }
        }

        // And an accepted release confirms, which is the ordinary path.
        let verdict = classify_release(
            Ok(runtime_cloud_connect::release::ReleaseOutcome {
                status: "removed".to_string(),
                app_name: Some("edge-analytics".to_string()),
            }),
            "https://api.example",
        );
        assert!(
            matches!(verdict, ReleaseVerdict::Confirmed { .. }),
            "{verdict:?}"
        );
    }

    #[test]
    fn an_unconfirmed_release_names_the_way_to_finish_it() {
        let verdict = classify_release(
            Err(runtime_cloud_connect::release::Error::Rejected {
                status: 404,
                message: "no such instance".to_string(),
            }),
            "https://api.example",
        );
        let ReleaseVerdict::Unconfirmed { reason } = verdict else {
            panic!("404 must not confirm");
        };
        assert!(reason.contains("Nothing was removed locally"), "{reason}");
        assert!(reason.contains("--force"), "{reason}");
        assert!(reason.contains("https://api.example"), "{reason}");
    }

    #[tokio::test]
    async fn an_unconfirmed_release_keeps_every_piece_of_local_state() {
        // The acceptance criterion: a transient Cloud failure must leave a
        // directory a retry can finish the removal from. Clearing the identity
        // first would orphan a registry row nobody local can release any more.
        let dir = tempfile::tempdir().expect("create tempdir");
        let config_dir = dir
            .path()
            .canonicalize()
            .expect("canonical tempdir")
            .join(".spice");
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
        let error = remove_identity(&config_dir, Some("http://127.0.0.1:1"), true, false)
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
            new_project_url: None,
            control_plane_endpoint: None,
        }
    }
}
