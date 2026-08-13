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
//!    `remove` releases the instance and clears it, and `--install` wraps
//!    an already-enrolled directory in a persistent system service.
//!
//! 2. **Deprecated pod-add behavior**: when the argument is a Spicepod
//!    path on Spice.ai Cloud (e.g. `spiceai/quickstart`), this prints a
//!    deprecation notice and behaves like `spice add <pod>` with Spice.ai
//!    Cloud authentication headers.

mod service;

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::commands::add::{AddArgs, execute_add_or_connect};
use crate::context::RuntimeContext;
use crate::error::{Error, Result};
use clap::{Args, Subcommand};
use runtime_cloud_connect::config::{CloudConnectConfig, IDENTITY_FILE};

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

  spice connect status                    Show the current enrollment state.
  sudo spice connect --install            Install `spiced` as a persistent
                                          system service for an already
                                          enrolled directory — systemd on
                                          Linux, a launchd daemon on macOS.
                                          Re-run to upgrade in place: latest
                                          binary, rewritten service definition,
                                          service restarted, identity
                                          untouched.
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

`--install` requires root, and either Linux with systemd or macOS with
launchd. Containers pass the enrollment key directly to the runtime
(`spiced --token`) under the container runtime's restart policy; Windows
enrolls and runs under the user's own supervisor.

DEPRECATED POD-ADD BEHAVIOR:
  spice connect <org>/<pod>               Deprecated; use `spice add <org>/<pod>`.

EXAMPLES
  spice connect status
  sudo spice connect --install
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
    /// location entirely. Applies to `status`, `remove`, and `--install`.
    #[arg(long, value_name = "PATH", global = true)]
    pub dir: Option<PathBuf>,

    /// Install and start `spiced` as a persistent system service running
    /// from an already-enrolled instance directory, so the instance
    /// survives reboots and closed terminals. Requires root, and either
    /// Linux with systemd or macOS with launchd. Re-running is the
    /// idempotent in-place upgrade path.
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
    /// Show the current Spice Cloud Connect enrollment state.
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
/// Returns an error if I/O fails, this directory holds no Cloud Connect
/// state to act on, or the deprecated pod-add path errors.
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

    // Rejected on the Cloud Connect branches only. The deprecated pod-add
    // fallthrough below is a Spice.ai Cloud fetch, where `--cloud-region` has
    // always been meaningful, so refusing it there would be a regression.
    let Some(target) = args.target.as_deref() else {
        reject_cloud_region(args.cloud_region.as_deref())?;
        return connect_existing(ctx, &config_dir, args.install, args.endpoint.as_deref()).await;
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
             to the home stamp for a location it does not recognize. Use --endpoint <url> \
             here only to inspect or release state through another Spice Cloud control plane. \
             See: https://spiceai.org/docs"
        ),
    })
}

/// Bare `spice connect` (no subcommand, no pod path): act on the existing
/// per-directory state.
///
/// An enrolled directory reports its status, or installs the persistent
/// service when `--install` asks for one. A directory with no identity has
/// nothing this command can act on — enrollment belongs to the runtime — so
/// it errors with the exact command that does enroll.
async fn connect_existing(
    ctx: &RuntimeContext,
    config_dir: &Path,
    install: bool,
    endpoint: Option<&str>,
) -> Result<()> {
    if has_enrolled_identity(config_dir)? {
        if install {
            // The `--install`-after-a-prior-enroll path, and the in-place
            // upgrade path when a service is already installed.
            return install_service(ctx, config_dir);
        }
        println!("This host is already enrolled with Spice Cloud.");
        println!();
        return print_status(config_dir, endpoint).await;
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
/// Existence alone is not enough: installing a service over a malformed or
/// unreadable identity would report success, but every `spiced` restart would
/// reject that identity and run without Cloud Connect.
fn has_enrolled_identity(config_dir: &Path) -> Result<bool> {
    let mut config = runtime_cloud_connect::CloudConnectConfig::from_env_at(
        env!("CARGO_PKG_VERSION"),
        config_dir.to_path_buf(),
    );
    // An installed service carries only SPICE_CONFIG_DIR. A gateway override
    // inherited from the invoking shell is transient and cannot make an
    // otherwise unusable durable identity safe to install.
    config.gateway_endpoint = None;
    runtime_cloud_connect::load_reconnectable_identity(&config)
        .map(|identity| identity.is_some())
        .map_err(|e| Error::CloudConnectIo {
            message: format!("load identity: {e}"),
        })
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

    let mut config = runtime_cloud_connect::CloudConnectConfig::from_env_at(
        env!("CARGO_PKG_VERSION"),
        config_dir.to_path_buf(),
    );
    config.gateway_endpoint = None;
    let identity = runtime_cloud_connect::load_reconnectable_identity(&config).map_err(|e| {
        Error::CloudConnectIo {
            message: format!("load identity: {e}"),
        }
    })?;

    if let Some(id) = identity {
        let expiry = id.not_after_unix.map_or_else(
            || "unbounded".to_string(),
            |secs| format!("unix={secs} (expired={})", id.is_expired()),
        );
        println!("Spice Cloud Connect: enrolled");
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

    // An enrollment that started but never completed leaves its retry-safe
    // draft behind. The draft is non-secret apart from the provisional keys
    // (never printed); the operation ID names what a retried enrollment
    // resumes.
    let draft_path = runtime_cloud_connect::EnrollmentDraft::path_in(config_dir);
    if draft_path.exists() {
        println!("Spice Cloud Connect: enrollment incomplete");
        println!("  draft:       {}", draft_path.display());
        println!(
            "  a previous enrollment did not finish. Mint a new enrollment key in the Spice \
             Cloud portal and start the runtime with it (`spiced --token <enrollment-key>`); \
             the retried enrollment resumes the same pending operation instead of creating a \
             duplicate instance."
        );
        print_service_for_dir(config_dir);
        // A stuck enrollment is a state a wrong clock explains: the enroll
        // keeps failing on certificate validity.
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
            "Run `spice connect status --dir <directory>` to inspect one of them, or enroll \
             this directory as a new instance with `spiced --token <enrollment-key>`."
        );
        return Ok(());
    }

    println!("Spice Cloud Connect: not connected");
    println!(
        "Mint an enrollment key in the Spice Cloud portal and start the runtime with it: \
         `spiced --token <enrollment-key>`."
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

    let identity = runtime_cloud_connect::identity::IdentityStore::load_optional(&identity_path)
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
        runtime_cloud_connect::identity::IdentityStore::clear_with_transaction(
            &identity_path,
            &enrollment_transaction,
        )
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
    fn a_malformed_identity_is_not_accepted_as_enrolled_state() {
        let dir = tempfile::tempdir().expect("create tempdir");
        std::fs::write(dir.path().join(IDENTITY_FILE), "not valid JSON")
            .expect("write malformed identity");

        let error = has_enrolled_identity(dir.path())
            .expect_err("a malformed identity must not enable service installation");
        assert!(error.to_string().contains("load identity"), "{error}");
    }

    #[test]
    fn a_parseable_but_unusable_identity_is_not_accepted_as_enrolled_state() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join(IDENTITY_FILE);
        std::fs::write(
            path,
            serde_json::json!({
                "identifier": "",
                "identity_cert_pem": "credential-that-must-not-be-printed",
                "private_key_pem": "private-key-that-must-not-be-printed",
                "public_key_pem": "public-key",
                "gateway_addr": "gateway.example:443"
            })
            .to_string(),
        )
        .expect("write unusable identity");

        let error = has_enrolled_identity(dir.path())
            .expect_err("an unusable identity must not enable service installation");
        let rendered = error.to_string();
        assert!(rendered.contains("cannot be used"), "{rendered}");
        assert!(rendered.contains("identifier is empty"), "{rendered}");
        assert!(!rendered.contains("credential-that-must-not-be-printed"));
        assert!(!rendered.contains("private-key-that-must-not-be-printed"));
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
}
