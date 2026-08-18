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

//! `spice connect` — Spice Cloud Connect enrollment flow.
//!
//! Two distinct use cases share this command:
//!
//! 1. **Cloud Connect enrollment** (remote management of `spiced` from
//!    Spice Cloud). This foundation accepts a one-shot key created in the
//!    Spice Cloud portal:
//!
//!    ```text
//!    spice connect spice-enroll-abcdefghijklmnopqrstuvwxyz012345
//!    ```
//!
//!    The command **enrolls and exits**: it installs the runtime if missing,
//!    completes the HTTPS enroll (identity issued and
//!    persisted, registry row created), prints the next steps, and returns.
//!    It starts `spiced` only when `--install` asks for a persistent
//!    service. `status`/`remove` inspect and clear the local state.
//!
//! 2. **Deprecated pod-add behavior**: when the argument is a Spicepod
//!    path on Spice.ai Cloud (e.g. `spiceai/quickstart`), this prints a
//!    deprecation notice and behaves like `spice add <pod>` with Spice.ai
//!    Cloud authentication headers.

mod service;

use std::path::{Path, PathBuf};

use crate::commands::add::{AddArgs, execute_add_or_connect};
use crate::context::RuntimeContext;
use crate::error::{Error, Result};
use clap::{Args, Subcommand};
use runtime_cloud_connect::config::{CloudConnectConfig, IDENTITY_FILE};
use runtime_cloud_connect::enrollment_key::looks_like_enrollment_key;
use secrecy::{ExposeSecret as _, SecretString};

/// Legacy file (relative to the config dir) holding an endpoint override.
/// Fresh enrollment stores its canonical control-plane binding in the identity;
/// this path remains only for compatibility and cleanup.
const CLOUD_ENDPOINT_FILE: &str = "cloud-endpoint";

/// Arguments for the `spice connect` command.
#[derive(Args, Debug)]
#[command(
    about = "Enroll this host with Spice Cloud (or add a cloud-hosted Spicepod)",
    long_about = r#"`spice connect` enrolls this host with Spice Cloud and exits.

  spice connect spice-enroll-<32 base64url characters>
                                          Enroll with a one-shot key from the
                                          Spice Cloud portal, for hosts with no
                                          login: the identity is issued and
                                          stored locally, the instance appears
                                          in the portal registry, and the
                                          command exits.
  sudo spice connect --install            For a previously enrolled directory,
                                          install
                                          `spiced --cloud-connect` as a
                                          persistent systemd service on Linux.
                                          Re-run to upgrade in place: latest
                                          binary, rewritten service definition,
                                          service restarted, staged identity
                                          untouched.
  spice connect status                    Show the current enrollment state.
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

Nothing is left running unless `--install` is passed — otherwise start the
runtime with `spiced --cloud-connect` to bring the instance online.

Use `--dir <path>` to enroll an instance rooted at a different directory:
per-instance state lives under `<dir>/.spice`, so multiple instances on one
host enroll independently. `SPICE_CONFIG_DIR` overrides the derived location
entirely and wins over `--dir`.

`--install` requires root and Linux with systemd. Containers use
`spiced --token <enrollment-key>` for unattended enrollment under the
container runtime's restart policy. This layer does not provide service
lifecycle management on macOS or Windows.

DEPRECATED POD-ADD BEHAVIOR:
  spice connect <org>/<pod>               Deprecated; use `spice add <org>/<pod>`.

EXAMPLES
  sudo spice connect --install
  spice connect spice-enroll-abcdefghijklmnopqrstuvwxyz012345
  spice connect spice-enroll-abcdefghijklmnopqrstuvwxyz012345 --dir /opt/edge-1
  spice connect status
  sudo spice connect remove

Docs: https://spiceai.org/docs"#
)]
pub struct ConnectArgs {
    /// Optional explicit subcommand. If absent, the first positional
    /// argument (`target`) is inspected to decide between enrollment flow
    /// and the deprecated pod-add behavior.
    #[command(subcommand)]
    pub command: Option<ConnectCommand>,

    /// First positional argument: either a Spice Cloud enrollment key
    /// (`spice-enroll-...`) or a Spicepod path (`<org>/<pod>`). Omit it only
    /// to inspect/install an already-enrolled directory in this layer.
    #[arg(value_name = "TARGET")]
    pub target: Option<ConnectTarget>,

    /// Override the Spice Cloud enroll endpoint the enrollment authority is
    /// presented to. Defaults to `https://api.spice.ai`. Also
    /// configurable via `SPICE_CLOUD_ENDPOINT`. The gateway (stream)
    /// address is issued by the enroll response, not configured here.
    #[arg(long, value_name = "URL")]
    pub endpoint: Option<String>,

    /// The instance directory: per-instance Cloud Connect state (the
    /// identity and retry-safe enrollment draft) lives under `<dir>/.spice`.
    /// Defaults to the current directory; resolved to an absolute path at
    /// enroll time. `SPICE_CONFIG_DIR` overrides the derived `.spice`
    /// location entirely. Applies to enrollment, `status`, and `remove`.
    #[arg(long, value_name = "PATH", global = true)]
    pub dir: Option<PathBuf>,

    /// Where this instance runs, e.g. `us-west-2` or `on-prem-syd`. A
    /// customer-declared label, not a probed fact.
    ///
    /// Spice Cloud displays it on the registry row and resolves this instance's
    /// gateway from it, by ranking the stamps it actually runs a gateway in and
    /// returning the nearest as `gateway_addr` in the enroll response — the same
    /// nearest-stamp mapping a BYOC cluster's region gets. A label it cannot
    /// rank falls back to the deployment's home stamp, so every enrollment gets
    /// a gateway that resolves.
    ///
    /// Any syntactically valid label is therefore accepted, including one no
    /// region catalog knows: a standalone host need not be in a cloud region at
    /// all. Also configurable via `SPICE_CONNECT_ADOPT_REGION`. Omitted on a
    /// re-enroll leaves an existing region untouched.
    #[arg(long, value_name = "REGION")]
    pub region: Option<String>,

    /// Install and start `spiced --cloud-connect` as a persistent system
    /// service running from the instance directory, so the instance survives
    /// reboots and closed terminals. Requires Linux with systemd and root.
    /// Combinable with a code, or run on its own after a prior enroll.
    /// Re-running is the idempotent in-place upgrade path.
    #[arg(long, global = true)]
    pub install: bool,

    /// Skip the confirmation prompt. Applies to `remove`, which otherwise
    /// asks before stopping and uninstalling a service.
    #[arg(long, short = 'y', global = true)]
    pub yes: bool,

    /// The global `--cloud-region`, forwarded by the dispatcher rather than
    /// declared here (the flag is global; clap would reject a second definition
    /// of the same name).
    ///
    /// Carried only so the adoption path can *refuse* it with a message naming
    /// `--region` and `--endpoint` — see [`reject_cloud_region`]. It stays
    /// honoured on the deprecated pod-add fallthrough, where it always meant
    /// the Spice.ai Cloud data region.
    #[arg(skip)]
    pub cloud_region: Option<String>,
}

/// A positional connect target. It may be a deprecated Spicepod path, but it
/// may also be a one-shot enrollment authority, so its storage is zeroizing
/// and its `Debug` representation never exposes the value.
#[derive(Clone)]
pub struct ConnectTarget(SecretString);

impl ConnectTarget {
    fn expose(&self) -> &str {
        self.0.expose_secret()
    }
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

/// Cloud-connect subcommands.
#[derive(Subcommand, Debug)]
pub enum ConnectCommand {
    /// Show the current Spice Cloud Connect adoption state.
    Status,

    /// Release this instance: report the release to Spice Cloud, uninstall an
    /// installed service, and clear the local identity. spiced will continue
    /// running unmanaged after the next restart.
    Remove,
}

/// Execute the `spice connect` command.
///
/// # Errors
///
/// Returns an error if I/O fails, the enrollment is rejected, or the
/// deprecated pod-add path errors.
pub async fn execute(ctx: &RuntimeContext, args: ConnectArgs) -> Result<()> {
    let config_dir = CloudConnectConfig::resolve_config_dir(args.dir.as_deref());

    if let Some(cmd) = args.command {
        reject_cloud_region(args.cloud_region.as_deref())?;
        return match cmd {
            ConnectCommand::Status => print_status(&config_dir, args.endpoint.as_deref()).await,
            ConnectCommand::Remove => {
                remove_identity(&config_dir, args.endpoint.as_deref(), args.yes).await
            }
        };
    }

    // A region typo must be caught before any authority is presented: the
    // cloud rejects it too, but failing here keeps the diagnosis local and a
    // one-shot key unspent.
    let region = validate_region(args.region.as_deref())?;

    let enroll = EnrollOptions {
        region,
        install: args.install,
        endpoint: args.endpoint,
    };

    // Rejected on the adoption branches only. The deprecated pod-add
    // fallthrough below is a Spice.ai Cloud fetch, where `--cloud-region` has
    // always been meaningful, so refusing it there would be a regression.
    let Some(target) = args.target.as_ref().map(ConnectTarget::expose) else {
        reject_cloud_region(args.cloud_region.as_deref())?;
        return connect_without_code(ctx, &config_dir, enroll).await;
    };

    if let Ok(key) = runtime_cloud_connect::EnrollmentKey::parse(target) {
        reject_cloud_region(args.cloud_region.as_deref())?;
        return enroll_instance(ctx, key, &config_dir, enroll).await;
    }

    // An input that clearly looks like an adoption code but fails validation
    // is a malformed code, not a Spicepod path. Treating it as a pod path
    // produces a misleading cloud-Spicepod error and may fire a cloud pod-add
    // request for what was plainly meant to be an adoption code, so reject it
    // explicitly instead of falling through to the pod-add path.
    if looks_like_enrollment_key(target) {
        return Err(Error::InvalidArgument {
            message:
                "The supplied value looks like a Spice Cloud enrollment key but is malformed. \
                 Expected spice-enroll- followed by exactly 32 base64url characters. \
                 Copy a fresh key from your Spice Cloud portal and retry."
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

/// Reject `--cloud-region` on the adoption path, naming the two flags that do
/// what the caller was reaching for.
///
/// Neither half of an enrollment is chosen by a region code on this side:
///
/// - The **control plane** the enroll and renew requests go to comes from
///   `--endpoint`, then `SPICE_CLOUD_ENDPOINT`, then the `cloud-endpoint` file
///   staged by an earlier connect, then
///   [`runtime_cloud_connect::config::DEFAULT_ENDPOINT`].
/// - The **gateway** the control stream dials comes back in the enroll response
///   as `gateway_addr`. Spice Cloud resolves it from `--region` — the instance's
///   declared location — by ranking the stamps it actually runs a gateway in and
///   picking the nearest, falling back to the deployment's home stamp for a
///   location it cannot rank. Deriving a gateway host from a region code
///   CLI-side is the conflation that hands out hostnames with nothing behind
///   them, so the CLI sends the location and never interprets it.
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
             instance's gateway from --region (where the instance runs) and returns it in the \
             enroll response, falling back to the home stamp for a location it does not \
             recognise. Use --region <region> to record the location, or --endpoint <url> to \
             enroll against a different Spice Cloud control plane. \
             See: https://spiceai.org/docs"
        ),
    })
}

/// Validate an explicit `--region` label, mirroring the cloud's own rule.
fn validate_region(region: Option<&str>) -> Result<Option<String>> {
    let Some(region) = region.map(str::trim).filter(|r| !r.is_empty()) else {
        return Ok(None);
    };
    if !runtime_cloud_connect::is_valid_instance_region(region) {
        return Err(Error::InvalidArgument {
            message: format!(
                "Invalid --region '{region}': expected 2-64 lowercase letters, digits, and \
                 hyphens, starting and ending with a letter or digit (for example 'us-west-2' or \
                 'on-prem-syd'). Any such label is accepted — it need not be a cloud region. \
                 See: https://spiceai.org/docs"
            ),
        });
    }
    Ok(Some(region.to_string()))
}

/// The enrollment options gathered from flags, distinct from the credential.
struct EnrollOptions {
    region: Option<String>,
    install: bool,
    endpoint: Option<String>,
}

/// `spice connect` with no enrollment key.
///
/// An already-enrolled directory is never re-enrolled: that could create a second
/// registry row for a host that already has one. The authenticated interactive
/// setup flow is deliberately owned by the later interactive layer in this
/// stack; this foundation accepts only the one-shot positional authority.
async fn connect_without_code(
    ctx: &RuntimeContext,
    config_dir: &Path,
    enroll: EnrollOptions,
) -> Result<()> {
    if config_dir.join(IDENTITY_FILE).exists() {
        if enroll.install {
            // The `--install`-after-a-prior-enroll path, and the in-place
            // upgrade path when a service is already installed.
            return install_service(ctx, config_dir);
        }
        println!("This host is already enrolled with Spice Cloud.");
        println!();
        return print_status(config_dir, enroll.endpoint.as_deref()).await;
    }

    Err(Error::InvalidArgument {
        message: "Interactive `spice connect` setup is introduced by the interactive workflow layer. In this foundation layer, supply a one-shot `spice-enroll-…` key or use `spiced --token <enrollment-key>` for unattended enrollment."
            .to_string(),
    })
}

/// Enroll this host with Spice Cloud and exit.
///
/// Sequence: preflight the service install (so a host that cannot be installed
/// on fails with the code unspent), stage the code when it is stageable,
/// install the runtime if missing, complete the HTTPS enroll (identity issued
/// and persisted, registry row created cloud-side, and region recorded when
/// requested), optionally install the service, and print the next steps.
async fn enroll_instance(
    ctx: &RuntimeContext,
    token: runtime_cloud_connect::EnrollmentKey,
    config_dir: &Path,
    enroll: EnrollOptions,
) -> Result<()> {
    // Preflight BEFORE the enroll: a host with no supervisor or without root
    // must fail with nothing installed and the adoption code still redeemable.
    if enroll.install {
        service::preflight()?;
    }

    std::fs::create_dir_all(config_dir).map_err(|e| Error::CloudConnectIo {
        message: format!("create config dir {}: {e}", config_dir.display()),
    })?;

    println!("Enrolling this host with Spice Cloud...");

    ctx.ensure_local_runtime_supported()?;

    // Auto-install runtime if not present, so the printed next step
    // (`spiced --cloud-connect`) works immediately after this command.
    if !ctx.is_runtime_installed() {
        tracing::info!("Spice.ai runtime is not installed. Installing now...");
        crate::commands::install::execute(ctx, &crate::commands::install::InstallArgs::default())
            .await?;
    }

    // Complete the HTTPS enroll right here — `spiced` is never started.
    //
    // Report the version of the runtime that will actually run, probed from the
    // binary itself, so the registry row never shows a version the host is not
    // running. The CLI's own version is only a fallback for when `spiced` cannot
    // be executed; the two agree in shipped builds, where the CLI and the
    // runtime move in lockstep.
    let runtime_version = ctx
        .runtime_version()
        .unwrap_or_else(|_| env!("CARGO_PKG_VERSION").to_string());
    let mut config = CloudConnectConfig::from_env_at(runtime_version, config_dir.to_path_buf());
    if let Some(ref ep) = enroll.endpoint {
        // The explicit flag wins over `SPICE_CLOUD_ENDPOINT` for this
        // process. A successful fresh enrollment persists the canonical
        // control-plane binding in the identity used by later starts.
        config.enroll_endpoint = ep.clone();
    }
    if enroll.region.is_some() {
        config.instance_region = enroll.region.clone();
    }

    let retry_hint = "re-run `spice connect <spice-enroll-key>`";
    let outcome = runtime_cloud_connect::enroll_now_with_token(
        &config,
        token.expose_secret(),
        None,
        runtime_cloud_connect::RetryPolicy::INTERACTIVE,
    )
    .await;
    let outcome = match outcome {
        Ok(outcome) => outcome,
        Err(err) if err.is_credential_rejection() => {
            return Err(Error::CloudConnectEnroll {
                message: format!(
                    "{err}. Obtain a fresh enrollment key in the Spice Cloud portal and re-run `spice connect <spice-enroll-key>`."
                ),
            });
        }
        Err(err) if err.is_authoritative_rejection() => {
            return Err(Error::CloudConnectEnroll {
                message: format!("{err}. Fix the reported problem and {retry_hint}."),
            });
        }
        Err(err @ runtime_cloud_connect::enroll::EnrollNowError::Persist { .. }) => {
            // The identity was issued but could not be written; the message
            // carries the recovery steps (the code is already consumed).
            return Err(Error::CloudConnectEnroll {
                message: err.to_string(),
            });
        }
        Err(err) => {
            return Err(Error::CloudConnectEnroll {
                message: format!("{err}. {retry_hint} to retry."),
            });
        }
    };

    let (identity, metadata) = match outcome {
        runtime_cloud_connect::EnrollNowOutcome::AlreadyEnrolled { identity } => (identity, None),
        runtime_cloud_connect::EnrollNowOutcome::Enrolled { identity, metadata } => {
            (identity, Some(metadata))
        }
    };
    // The canonical identity now carries the control-plane binding. A legacy
    // endpoint file is removed only after a fresh enrollment has committed;
    // an existing identity short-circuit leaves every byte of its local state
    // untouched, including a self-hosted binding used by older runtimes.
    if metadata.is_some() {
        let endpoint_path = config_dir.join(CLOUD_ENDPOINT_FILE);
        if let Err(error) = std::fs::remove_file(&endpoint_path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(
                "The enrolled identity is durable, but the legacy endpoint override at {} could not be removed: {error}",
                endpoint_path.display()
            );
        }
    }
    println!("Enrolled with Spice Cloud.");
    // Display only the organization reported by the issuing control plane.
    if let Some(org) = metadata
        .as_ref()
        .map(|metadata| &metadata.organization.name)
        .filter(|org| !org.is_empty())
    {
        println!("  org:         {org}");
    }
    println!("  instance id: {}", identity.identifier);
    println!("  identity:    {}", config.identity_path.display());
    if !identity.gateway_addr.is_empty() {
        println!("  gateway:     {}", identity.gateway_addr);
    }
    if let Some(region) = metadata
        .as_ref()
        .and_then(|metadata| metadata.region.as_ref())
        .or(enroll.region.as_ref())
    {
        println!("  region:      {region}");
    }
    println!("  app:         unattached — attach to an app in the Spice Cloud portal");
    println!();

    if enroll.install {
        // Any failure here is post-enroll: the identity is staged and the
        // install resumes with `sudo spice connect --install`, so say so
        // rather than leaving the operator to guess whether to re-enroll.
        return install_service(ctx, config_dir).map_err(|err| Error::CloudConnectEnroll {
            message: format!(
                "The host is enrolled and its identity is staged at {}, but the service could not \
                 be installed: {err} Fix the problem and run `sudo spice connect --install` to \
                 finish — do not re-enroll.",
                config_dir.display()
            ),
        });
    }

    println!("Nothing is running yet. Choose how this instance runs:");
    println!("  sudo spice connect --install   Install a persistent service (Linux/systemd)");
    println!("  spiced --cloud-connect         Run it in the foreground from this directory");
    println!("The instance shows as connected in the Spice Cloud portal once the runtime is up.");

    Ok(())
}

/// Install (or reinstall) the service for this instance directory and report
/// its name and how to manage it.
fn install_service(ctx: &RuntimeContext, config_dir: &Path) -> Result<()> {
    service::preflight()?;

    // The service runs from the *instance* directory, not the `.spice` config
    // dir beneath it: that directory is the spicepod root the runtime loads
    // from.
    let instance_dir = instance_dir_for(config_dir);
    // Resolved, not derived from `$HOME`: `sudo` rewrites `HOME` to `/root`, and
    // the runtime the operator installed is normally under their own home.
    let spiced_path = ctx.resolve_spiced_path().ok_or_else(|| Error::InvalidArgument {
        message: format!(
            "Failed to install the Spice Cloud Connect service: no Spice runtime was found at {}. \
             Install it with `spice install` and re-run `sudo spice connect --install`. \
             See: https://spiceai.org/docs",
            ctx.spiced_path().display()
        ),
    })?;

    let installed = service::install(&instance_dir, config_dir, &spiced_path)?;

    println!("Installed the Spice Cloud Connect service.");
    println!("  service:   {}", installed.name);
    println!("  file:      {}", installed.path.display());
    println!("  directory: {}", instance_dir.display());
    // Name both paths: the operator needs to know which build was installed and
    // that the service runs a root-owned copy of it, not the original.
    println!("  runtime:   {}", installed.runtime.display());
    if installed.runtime != spiced_path {
        println!("             staged from {}", spiced_path.display());
    }
    if let Ok(version) = ctx.runtime_version() {
        println!("  version:   {version}");
    }
    if let Some(state) = service::is_active(&installed.name) {
        println!("  state:     {state}");
    }
    println!();
    println!("Manage it with:");
    for hint in service::manage_hints(&installed.name) {
        println!("  {hint}");
    }
    println!(
        "Re-run `sudo spice connect --install` to upgrade the runtime in place; \
         `sudo spice connect remove` to release this instance and uninstall the service."
    );
    Ok(())
}

/// The instance directory a config dir belongs to: `<dir>/.spice` → `<dir>`.
///
/// `SPICE_CONFIG_DIR` can point anywhere, so a config dir that is not named
/// `.spice` has no instance directory above it — in that case the config dir
/// itself is the best available answer for `WorkingDirectory`.
fn instance_dir_for(config_dir: &Path) -> PathBuf {
    if config_dir.file_name() == Some(std::ffi::OsStr::new(".spice"))
        && let Some(parent) = config_dir.parent().filter(|p| !p.as_os_str().is_empty())
    {
        return parent.to_path_buf();
    }
    config_dir.to_path_buf()
}

async fn print_status(config_dir: &Path, endpoint: Option<&str>) -> Result<()> {
    let identity_path = config_dir.join(IDENTITY_FILE);
    let draft_path = runtime_cloud_connect::EnrollmentDraft::path_in(config_dir);

    let identity = runtime_cloud_connect::identity::IdentityStore::load_optional(&identity_path)
        .map_err(|e| Error::CloudConnectIo {
            message: format!("load identity: {e}"),
        })?;

    if let Some(id) = identity {
        let expiry = id.not_after_unix.map_or_else(
            || "unbounded".to_string(),
            |secs| format!("unix={secs} (expired={})", id.is_expired()),
        );
        println!("Spice Cloud Connect: adopted");
        println!("  identifier:  {}", id.identifier);
        println!("  identity:    {}", identity_path.display());
        if !id.gateway_addr.is_empty() {
            println!("  gateway:     {}", id.gateway_addr);
        }
        println!("  expiry:      {expiry}");
        print_deployed_spicepod(config_dir);
        print_delivered_secrets(config_dir);
        print_service_for_dir(config_dir);
        // An identity that reads as expired on a host whose clock is wrong is
        // not actually expired — measure before the operator goes chasing a
        // renewal problem that does not exist. A live identity needs no probe,
        // which keeps the common `status` offline and instant.
        if id.is_expired() {
            report_clock_skew(&resolved_endpoint(config_dir, endpoint)).await;
        }
        return Ok(());
    }

    if draft_path.exists() {
        println!("Spice Cloud Connect: pending enrollment");
        println!("  enrollment draft: {}", draft_path.display());
        println!(
            "  present a fresh enrollment key to resume this retry-safe operation, or run `spice connect remove --yes` to abandon it"
        );
        print_service_for_dir(config_dir);
        report_clock_skew(&resolved_endpoint(config_dir, endpoint)).await;
        return Ok(());
    }

    // No state in this directory. A host can still be connected from another
    // instance directory, so report the installed services rather than a
    // misleading "not connected".
    let installed = service::discover_all();
    if !installed.is_empty() {
        println!(
            "Spice Cloud Connect: no instance in this directory ({}).",
            instance_dir_for(config_dir).display()
        );
        println!("Installed services on this host:");
        for service in &installed {
            let state = service::is_active(&service.name).unwrap_or_else(|| "unknown".to_string());
            println!(
                "  {} (dir {}) — {state}",
                service.name,
                service.working_dir.display()
            );
        }
        println!();
        println!(
            "Run `spice connect status --dir <directory>` to inspect one of them, or \
             present a fresh enrollment key with `spice connect <spice-enroll-key>` here."
        );
        return Ok(());
    }

    println!("Spice Cloud Connect: not connected");
    println!(
        "Run `spice connect <spice-enroll-key>` with a fresh key from your Spice Cloud portal, \
         or use `spiced --token <enrollment-key>` for unattended enrollment."
    );
    Ok(())
}

/// Report whether a deployed spicepod is what this instance comes up on.
///
/// Reads the config dir only, so it answers on a host with no network.
fn print_deployed_spicepod(config_dir: &Path) {
    let spicepod = config_dir.join(runtime_cloud_connect::config::CLOUD_MANAGED_SPICEPOD_FILE);
    if spicepod.exists() {
        println!("  deployment:  {}", spicepod.display());
    } else {
        println!(
            "  deployment:  none yet — this instance runs its local spicepod until an app is deployed to it"
        );
    }
}

/// Report the delivered secrets held in the local cache.
///
/// Reads only the cache's plaintext header, so this works without the key and
/// **cannot** print a value. Names are what diagnose the common failure: a
/// component referencing a secret the last deployment did not deliver.
fn print_delivered_secrets(config_dir: &Path) {
    let path = config_dir.join(runtime_cloud_connect::secret_cache::SECRET_CACHE_FILE);
    let Some(header) = runtime_cloud_connect::secret_cache::read_header(&path) else {
        if path.exists() {
            println!(
                "  secrets:     cache present but unreadable — deploy the app to re-deliver them"
            );
        } else {
            println!("  secrets:     none delivered yet — deploy the app to deliver them");
        }
        return;
    };
    if header.names.is_empty() {
        println!("  secrets:     none (the last deployment delivered no secrets)");
        return;
    }
    println!(
        "  secrets:     {} delivered: {}",
        header.names.len(),
        header.names.join(", ")
    );
}

/// Report the service installed for this instance directory, when there is one.
fn print_service_for_dir(config_dir: &Path) {
    let instance_dir = instance_dir_for(config_dir);
    let Some(installed) = service::find_for_dir(&instance_dir) else {
        // Not an error: containers and foreground runs are supported ways to
        // run, and `--install` needs a supported supervisor.
        return;
    };
    let state = service::is_active(&installed.name).unwrap_or_else(|| "unknown".to_string());
    println!("  service:     {} — {state}", installed.name);
}

/// Measure the host clock against Spice Cloud and report a significant skew.
///
/// Called only from the states a wrong clock explains — an identity that reads
/// as expired, or an enrollment stuck pending — so a healthy `status` makes no
/// network request at all. Best-effort and silent on failure: `status` must
/// stay usable on a host with no network.
async fn report_clock_skew(endpoint: &str) {
    if let Some(skew) = runtime_cloud_connect::clock_skew::diagnose(endpoint, None).await
        && skew.is_significant()
    {
        println!("  clock:       {}", skew.advice());
    }
}

/// Release this instance: report the release to Spice Cloud, uninstall an
/// installed service, and clear the local identity and staged state.
///
/// Local state is cleared whether or not the cloud could be reached — the host
/// is being decommissioned, and a `remove` that failed because the network was
/// down would leave a credential behind. Unreachable, the registry row reads
/// `disconnected` until it is deleted in the portal; the portal-side delete is
/// authoritative either way.
async fn remove_identity(
    config_dir: &Path,
    endpoint: Option<&str>,
    assume_yes: bool,
) -> Result<()> {
    // Inventory and cleanup are one enrollment transaction. An enrollment may
    // already be promoting its identity when removal starts; acquiring this
    // boundary first makes removal observe and clear that winner instead of
    // sampling an old draft-only state and returning with a new identity.
    let enrollment_transaction = std::sync::Arc::new(
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

    let identity =
        runtime_cloud_connect::identity::IdentityStore::load_optional_async(identity_path.clone())
            .await
            .map_err(|e| Error::CloudConnectIo {
                message: format!("load identity: {e}"),
            })?;
    let installed = service::find_for_dir(&instance_dir);

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
    if let Some(ref installed) = installed
        && !assume_yes
    {
        println!("This will release this instance from Spice Cloud and remove it from this host:");
        println!("  service:   {} (stopped and uninstalled)", installed.name);
        println!("  directory: {}", instance_dir.display());
        if had_identity {
            println!("  identity:  {} (deleted)", identity_path.display());
        }
        if had_draft {
            println!("  draft:     {} (deleted)", draft_path.display());
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

    // Report the release before clearing anything: the identity leaf is the
    // credential that authorises it, so it has to still exist.
    if let Some(ref identity) = identity {
        report_release(config_dir, endpoint, identity).await;
    }

    // A service left running against a released identity restarts forever, so
    // this is the step that most needs to happen — but a failure must not abort
    // the command before the identity is cleared. The cloud has already
    // released the instance by this point, so keeping a dead credential on disk
    // is strictly worse than reporting the uninstall failure at the end.
    let uninstall_failure = match installed {
        Some(ref installed) => match service::uninstall(&instance_dir) {
            Ok(_) => {
                println!("Stopped and uninstalled {}.", installed.name);
                None
            }
            Err(err) => Some(err),
        },
        None => None,
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
        runtime_cloud_connect::identity::IdentityStore::clear_async(&identity_path)
            .await
            .map_err(|e| Error::CloudConnectIo {
                message: format!("clear identity: {e}"),
            })?;
    }

    if had_draft {
        enrollment_transaction
            .delete_draft_async()
            .await
            .map_err(|e| Error::CloudConnectIo {
                message: format!("remove enrollment draft: {e}"),
            })?;
    }

    // Also clear any `cloud-endpoint` override so a later
    // `spice connect <code>` without `--endpoint` doesn't silently keep
    // using the stale endpoint.
    if had_endpoint
        && let Err(e) = std::fs::remove_file(&endpoint_path)
        && e.kind() != std::io::ErrorKind::NotFound
    {
        return Err(Error::CloudConnectIo {
            message: format!("remove endpoint override: {e}"),
        });
    }

    println!(
        "Spice Cloud Connect identity cleared. Present a fresh enrollment key with `spice connect <spice-enroll-key>` to enroll this directory again."
    );

    // Surfaced last so the exit status still reports it: the local state is
    // gone, but a service left behind would keep restarting a runtime with no
    // identity until someone removes it.
    match uninstall_failure {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

/// Tell Spice Cloud this instance is released, and report what happened.
///
/// Never fails the command: the release moves the registry row to `removed`
/// immediately when it lands. Unreachable, the row reads `disconnected` until it
/// is deleted in the portal, and the operator is told so.
async fn report_release(
    config_dir: &Path,
    endpoint: Option<&str>,
    identity: &runtime_cloud_connect::Identity,
) {
    let endpoint = resolved_endpoint(config_dir, endpoint);
    let ca = (!identity.ca_bundle_pem.is_empty()).then_some(identity.ca_bundle_pem.as_str());

    match runtime_cloud_connect::release::release(&endpoint, identity, ca).await {
        Ok(outcome) => {
            println!("Released this instance in Spice Cloud.");
            if !outcome.status.is_empty() {
                println!("  registry status: {}", outcome.status);
            }
            if let Some(app) = outcome.app_name {
                println!(
                    "  app {app} is paused — its deploy target was removed. Move it to another \
                     instance, or delete it, in the Spice Cloud portal."
                );
            }
        }
        Err(err) => {
            println!(
                "Could not report the release to Spice Cloud at {endpoint}: {err} \
                 Clearing local state anyway. The instance reads as disconnected in the portal \
                 until you delete it there."
            );
        }
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

    #[test]
    fn connect_target_debug_never_exposes_an_enrollment_authority() {
        let authority = "spice-enroll-abcdefghijklmnopqrstuvwxyz012345";
        let target: ConnectTarget = authority.parse().expect("infallible parse");
        let debug = format!("{target:?}");
        assert_eq!(debug, "ConnectTarget([REDACTED])");
        assert!(!debug.contains(authority));
    }

    #[test]
    fn enrollment_key_near_misses_stay_on_the_redacted_path() {
        assert!(looks_like_enrollment_key(
            "spice-enroll-abcdefghijklmnopqrstuvwxyz012345"
        ));
        let secret = "A".repeat(32);
        assert!(looks_like_enrollment_key(&format!("SPICE-ENROLL-{secret}")));
        assert!(looks_like_enrollment_key(&format!("spcie-enroll-{secret}")));
        assert!(looks_like_enrollment_key(&format!(
            "prefix/spice-enroll-{secret}"
        )));
    }

    #[test]
    fn looks_like_enrollment_key_rejects_pod_paths() {
        assert!(!looks_like_enrollment_key("spiceai/quickstart"));
        assert!(!looks_like_enrollment_key("org/pod"));
        assert!(!looks_like_enrollment_key("ordinary-local-path"));
    }

    #[test]
    fn validate_region_accepts_labels_no_catalog_knows() {
        // A standalone host need not be in a cloud region at all, and a brand
        // new AWS region must not need a CLI release to be usable.
        for region in ["us-west-2", "on-prem-syd", "ap-southeast-7"] {
            assert_eq!(
                validate_region(Some(region)).expect("valid").as_deref(),
                Some(region)
            );
        }
    }

    #[test]
    fn validate_region_trims_and_treats_blank_as_absent() {
        assert_eq!(
            validate_region(Some("  us-west-2 "))
                .expect("valid")
                .as_deref(),
            Some("us-west-2")
        );
        assert_eq!(validate_region(Some("   ")).expect("blank is absent"), None);
        assert_eq!(validate_region(None).expect("absent"), None);
    }

    #[test]
    fn validate_region_rejects_malformed_labels_with_an_actionable_message() {
        let err = validate_region(Some("US_WEST_2")).expect_err("must reject");
        let message = err.to_string();
        assert!(message.contains("--region"), "{message}");
        assert!(
            message.contains("us-west-2") && message.contains("on-prem-syd"),
            "the message must show both an AWS and a non-AWS example: {message}"
        );
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
    fn resolved_endpoint_prefers_the_explicit_flag() {
        let dir = std::env::temp_dir().join(format!("spice-connect-ep-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        assert_eq!(
            resolved_endpoint(&dir, Some("https://explicit.example")),
            "https://explicit.example"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn resolved_endpoint_reads_the_on_disk_override_then_the_default() {
        let dir = std::env::temp_dir().join(format!("spice-connect-ep2-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create scratch dir");

        // Nothing on disk: the built-in default.
        assert_eq!(
            resolved_endpoint(&dir, None),
            runtime_cloud_connect::config::DEFAULT_ENDPOINT
        );

        // The override file wins over the default.
        std::fs::write(dir.join(CLOUD_ENDPOINT_FILE), "https://override.example\n")
            .expect("write override");
        assert_eq!(resolved_endpoint(&dir, None), "https://override.example");

        // A blank override is not an endpoint.
        std::fs::write(dir.join(CLOUD_ENDPOINT_FILE), "  \n").expect("write blank override");
        assert_eq!(
            resolved_endpoint(&dir, None),
            runtime_cloud_connect::config::DEFAULT_ENDPOINT
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn cloud_region_and_region_are_distinct_flags() {
        // The pair a customer is most likely to transpose: `--region` records
        // where the instance runs and is validated as a label, while
        // `--cloud-region` names a Spice Cloud and is not a label on the row.
        // Guard that the instance-region validator does not silently accept a
        // Spice Cloud *data* region suffix as an instance location.
        assert_eq!(
            validate_region(Some("us-west-2-prod-aws-data"))
                .expect("charset-valid")
                .as_deref(),
            Some("us-west-2-prod-aws-data"),
            "the CLI validates charset only — the cloud stores the label verbatim"
        );
    }
}
