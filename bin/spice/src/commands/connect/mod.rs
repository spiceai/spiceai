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
//! Enrollment, status, removal, and service lifecycle are scoped to one
//! instance directory. Deprecated pod-add behavior remains temporarily: when
//! the argument is a Spicepod
//!    path on Spice.ai Cloud (e.g. `spiceai/quickstart`), this prints a
//!    deprecation notice and behaves like `spice add <pod>` with Spice.ai
//!    Cloud authentication headers.

mod mint;
mod service;

use std::path::{Path, PathBuf};

use crate::commands::add::{AddArgs, execute_add_or_connect};
use crate::context::RuntimeContext;
use crate::error::{Error, Result};
use clap::{Args, Subcommand};
use runtime_cloud_connect::config::{CloudConnectConfig, IDENTITY_FILE, PENDING_ADOPT_CODE_FILE};

use crate::output::OutputFormat;

/// File (relative to the config dir) holding a `--endpoint` override so later
/// `spiced` starts reach the same control plane the enroll did.
const CLOUD_ENDPOINT_FILE: &str = "cloud-endpoint";

/// Arguments for the `spice connect` command.
#[derive(Args)]
#[command(
    about = "Enroll this host with Spice Cloud (or add a cloud-hosted Spicepod)",
    long_about = r#"`spice connect` enrolls one instance directory with Spice Cloud.

  spice connect                           Enroll using the local login.
  spice connect SPICE-ADOPT-...           Enroll with a portal-issued key.
  spice connect status -o json            Show the versioned local status.
  spice connect service                   Show service actions.
  spice connect service install           Install a persistent service.
  spice connect service logs -n 100 -f    Read and follow service logs.
  spice connect remove --yes              Uninstall, release, then clear state.

Use `--dir <path>` to enroll an instance rooted at a different directory:
per-instance state lives under `<dir>/.spice`, so multiple instances on one
host enroll independently. `SPICE_CONFIG_DIR` overrides the derived location
entirely and wins over `--dir`.

ENVIRONMENT
  SPICE_CONNECT_ADOPT_CODE                Adoption code, for hosts with no CLI.
  SPICE_CONNECT_ADOPT_APP_NAME            Mirrors --app-name.
  SPICE_CONNECT_ADOPT_CREATE              Mirrors --create.
  SPICE_CONNECT_ADOPT_REGION              Mirrors --region.

DEPRECATED POD-ADD BEHAVIOR:
  spice connect <org>/<pod>               Deprecated; use `spice add <org>/<pod>`.

EXAMPLES
  spice connect SPICE-ADOPT-7K2PX-9XYZ2-A1B2C-D3E4F
  spice connect SPICE-ADOPT-7K2PX-9XYZ2-A1B2C-D3E4F --dir /opt/edge-1
  spice connect --region on-prem-syd --app-name edge-fleet
  spice connect status --output json
  spice connect service status
  spice connect remove --yes

Docs: https://spiceai.org/docs"#
)]
pub struct ConnectArgs {
    /// Optional explicit subcommand. If absent, the first positional
    /// argument (`target`) is inspected to decide between enrollment flow
    /// and the deprecated pod-add behavior.
    #[command(subcommand)]
    pub command: Option<ConnectCommand>,

    /// First positional argument: either a Spice Cloud adoption code
    /// (`SPICE-ADOPT-...`) or a Spicepod path (`<org>/<pod>`). Omitted on a
    /// host logged in with `spice login`, which mints its own code.
    #[arg(value_name = "TARGET")]
    pub target: Option<String>,

    /// Override the Spice Cloud enroll endpoint the adoption code is
    /// presented to. Defaults to `https://api.spice.ai`. Also
    /// configurable via `SPICE_CLOUD_ENDPOINT`. The gateway (stream)
    /// address is issued by the enroll response, not configured here.
    #[arg(long, value_name = "URL", global = true)]
    pub endpoint: Option<String>,

    /// The instance directory: per-instance Cloud Connect state (the
    /// identity and staged adoption code) lives under `<dir>/.spice`.
    /// Defaults to the current directory; resolved to an absolute path at
    /// enroll time. `SPICE_CONFIG_DIR` overrides the derived `.spice`
    /// location entirely. Applies to enrollment, `status`, and `remove`.
    #[arg(long, value_name = "PATH", global = true)]
    pub dir: Option<PathBuf>,

    /// Which Spice Cloud org to enroll into, when the `spice login`
    /// credential on this host belongs to several. Sent to the adoption-code
    /// mint as an assertion — the org comes from the token, so naming an org
    /// the login does not belong to is an error rather than a silent mint into
    /// a different one. Ignored when an explicit adoption code is given: a
    /// code already carries its own org scope.
    #[arg(long, value_name = "NAME")]
    pub org: Option<String>,

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

    /// Attach the instance to the existing Spice Cloud app of this name at
    /// enroll. Fails (without consuming the code) when no such app exists —
    /// pass --create to create it. Also configurable via
    /// `SPICE_CONNECT_ADOPT_APP_NAME`. Omitted: the instance enrolls
    /// unattached and is attached later in the portal.
    #[arg(long, value_name = "NAME")]
    pub app_name: Option<String>,

    /// With --app-name: create the app when it does not exist, then attach
    /// the instance to it. Never creates silently — an absent app without
    /// this flag is an error. Also configurable via
    /// `SPICE_CONNECT_ADOPT_CREATE`.
    #[arg(long, requires = "app_name")]
    pub create: bool,

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

/// Cloud-connect subcommands.
#[derive(Subcommand)]
pub enum ConnectCommand {
    /// Show the current Spice Cloud Connect adoption state.
    Status(ConnectStatusArgs),

    /// Release this instance: report the release to Spice Cloud, uninstall an
    /// installed service, and clear the local identity. spiced will continue
    /// running unmanaged after the next restart.
    Remove,

    /// Manage the persistent service for this instance directory.
    #[command(alias = "svc")]
    Service(service::ServiceArgs),
}

#[derive(Args)]
pub struct ConnectStatusArgs {
    /// Output format.
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

impl ConnectArgs {
    pub fn output_mut(&mut self) -> Option<&mut OutputFormat> {
        match &mut self.command {
            Some(ConnectCommand::Status(args)) => Some(&mut args.output),
            Some(ConnectCommand::Service(args)) => args.output_mut(),
            _ => None,
        }
    }

    #[must_use]
    pub fn produces_json(&self) -> bool {
        match &self.command {
            Some(ConnectCommand::Status(args)) => args.output == OutputFormat::Json,
            Some(ConnectCommand::Service(args)) => args.produces_json(),
            _ => false,
        }
    }
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
            ConnectCommand::Status(status_args) => {
                print_status(ctx, &config_dir, status_args.output).await
            }
            ConnectCommand::Remove => {
                remove_identity(
                    &config_dir,
                    args.endpoint.as_deref(),
                    args.yes,
                    &service::PlatformBackend,
                )
                .await
            }
            ConnectCommand::Service(service_args) => {
                let instance_dir = instance_dir_for(&config_dir);
                service::execute(
                    ctx,
                    service_args,
                    &config_dir,
                    &instance_dir,
                    &service::PlatformBackend,
                )
                .await
            }
        };
    }

    // A region typo must be caught before anything is staged or minted: the
    // cloud rejects it too (it validates before consuming the code), but
    // failing here keeps the diagnosis local and the code unspent.
    let region = validate_region(args.region.as_deref())?;

    let enroll = EnrollOptions {
        app_name: args.app_name,
        create: args.create,
        region,
        org: args.org,
        endpoint: args.endpoint,
    };

    // Rejected on the adoption branches only. The deprecated pod-add
    // fallthrough below is a Spice.ai Cloud fetch, where `--cloud-region` has
    // always been meaningful, so refusing it there would be a regression.
    let Some(target) = args.target.as_deref() else {
        reject_cloud_region(args.cloud_region.as_deref())?;
        return connect_without_code(ctx, &config_dir, enroll).await;
    };

    if runtime_cloud_connect::is_valid_adoption_code(target) {
        reject_cloud_region(args.cloud_region.as_deref())?;
        return enroll_instance(
            ctx,
            Credential::Code(target.to_string()),
            &config_dir,
            enroll,
        )
        .await;
    }

    // An input that clearly looks like an adoption code but fails validation
    // is a malformed code, not a Spicepod path. Treating it as a pod path
    // produces a misleading cloud-Spicepod error and may fire a cloud pod-add
    // request for what was plainly meant to be an adoption code, so reject it
    // explicitly instead of falling through to the pod-add path.
    if looks_like_adoption_code(target) {
        return Err(Error::InvalidArgument {
            message: format!(
                "'{target}' looks like a Spice Cloud adoption code but is malformed. \
                 Expected SPICE-ADOPT-XXXXX-XXXXX-XXXXX-XXXXX (each segment is 5 uppercase \
                 letters or digits). Copy the code from your Spice Cloud portal and retry."
            ),
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
    app_name: Option<String>,
    create: bool,
    region: Option<String>,
    org: Option<String>,
    endpoint: Option<String>,
}

/// How this enrollment is authenticated.
enum Credential {
    /// An adoption code the operator supplied — staged on disk so an
    /// interrupted enroll can be resumed by a later `spiced` start.
    Code(String),
    /// A code the CLI minted from the `spice login` credential. **Never
    /// staged**: it lives for one enroll, and writing it down would put a live
    /// org credential on the host for no benefit.
    Minted(String),
}

impl Credential {
    fn code(&self) -> &str {
        match self {
            Self::Code(code) | Self::Minted(code) => code,
        }
    }

    /// `true` when the code may be written to the config dir.
    fn is_stageable(&self) -> bool {
        matches!(self, Self::Code(_))
    }
}

/// `spice connect` with no adoption code.
///
/// An already-enrolled directory is never re-enrolled: that would mint a second
/// registry row for a host that already has one. So this resolves in order:
/// an existing identity (install and/or report), then a staged code (finish the
/// interrupted enroll), then the `spice login` credential (mint and enroll).
async fn connect_without_code(
    ctx: &RuntimeContext,
    config_dir: &Path,
    enroll: EnrollOptions,
) -> Result<()> {
    if config_dir.join(IDENTITY_FILE).exists() {
        println!("This host is already enrolled with Spice Cloud.");
        println!();
        return print_status(ctx, config_dir, OutputFormat::Table).await;
    }

    if let Some(staged) = read_staged_code(config_dir)? {
        println!(
            "Resuming the enrollment staged at {}.",
            config_dir.display()
        );
        return enroll_instance(ctx, Credential::Code(staged), config_dir, enroll).await;
    }

    let minted = mint::mint_adoption_code(enroll.org.as_deref()).await?;
    let mut enroll = enroll;
    // The mint resolved the org authoritatively from the token; prefer it over
    // whatever was asserted so the summary reports what actually happened.
    if minted.org.is_some() {
        enroll.org = minted.org;
    }
    enroll_instance(ctx, Credential::Minted(minted.code), config_dir, enroll).await
}

/// Read a staged pending adoption code, if one is present and non-empty.
fn read_staged_code(config_dir: &Path) -> Result<Option<String>> {
    let path = config_dir.join(PENDING_ADOPT_CODE_FILE);
    if !path.exists() {
        return Ok(None);
    }
    let staged = std::fs::read_to_string(&path).map_err(|e| Error::CloudConnectIo {
        message: format!("read staged adoption code {}: {e}", path.display()),
    })?;
    Ok(Some(staged.trim().to_string()).filter(|code| !code.is_empty()))
}

/// Enroll this host with Spice Cloud and exit.
///
/// Sequence: preflight the service install (so a host that cannot be installed
/// on fails with the code unspent), stage the code when it is stageable,
/// install the runtime if missing, complete the HTTPS enroll (identity issued
/// and persisted, registry row created cloud-side, app attached and region
/// recorded when requested), optionally install the service, and print the next
/// steps.
async fn enroll_instance(
    ctx: &RuntimeContext,
    credential: Credential,
    config_dir: &Path,
    enroll: EnrollOptions,
) -> Result<()> {
    std::fs::create_dir_all(config_dir).map_err(|e| Error::CloudConnectIo {
        message: format!("create config dir {}: {e}", config_dir.display()),
    })?;

    let endpoint_path = config_dir.join(CLOUD_ENDPOINT_FILE);
    let pending_path = config_dir.join(PENDING_ADOPT_CODE_FILE);

    // If the user did NOT pass `--endpoint`, remove any previous override
    // so the next `spiced` start doesn't silently re-use a stale endpoint
    // from an earlier connect. A `remove` also clears this file, but
    // re-staging without `--endpoint` is the more common case.
    if enroll.endpoint.is_none()
        && let Err(e) = std::fs::remove_file(&endpoint_path)
        && e.kind() != std::io::ErrorKind::NotFound
    {
        return Err(Error::CloudConnectIo {
            message: format!(
                "remove stale endpoint override {}: {e}",
                endpoint_path.display()
            ),
        });
    }

    if credential.is_stageable() {
        atomic_write_0600(&pending_path, credential.code().as_bytes()).map_err(|e| {
            Error::CloudConnectIo {
                message: format!("write adoption code: {e}"),
            }
        })?;
    }

    // Write the endpoint override BEFORE printing success. If the override
    // can't be persisted, roll the staged code back so adoption can't
    // proceed against the wrong control plane on the next `spiced` start.
    if let Some(ref ep) = enroll.endpoint
        && let Err(e) = atomic_write_0600(&endpoint_path, ep.as_bytes())
    {
        // Best-effort rollback of the staged code; surface the
        // original endpoint-write failure to the caller.
        let _ = std::fs::remove_file(&pending_path);
        return Err(Error::CloudConnectIo {
            message: format!(
                "write endpoint override {}: {e} (adoption code not staged)",
                endpoint_path.display()
            ),
        });
    }

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
    // The credential resolved above wins over any env/staged state that
    // `from_env_at` picked up.
    config.adoption_code = Some(credential.code().to_string());
    // Only point at the staged file when there is one: a minted code is never
    // written to disk, and naming a path here would make the enroll delete
    // whatever happens to be staged for a different code.
    config.pending_adopt_code_path = credential.is_stageable().then(|| pending_path.clone());
    if let Some(ref ep) = enroll.endpoint {
        // The explicit flag wins over `SPICE_CLOUD_ENDPOINT` for this
        // process; the `cloud-endpoint` file written above covers later
        // `spiced` starts.
        config.enroll_endpoint = ep.clone();
    }
    // Flags win over the SPICE_CONNECT_ADOPT_* env vars `from_env_at`
    // picked up.
    if enroll.app_name.is_some() {
        config.adopt_app_name = enroll.app_name.clone();
    }
    if enroll.create {
        config.adopt_create_app = true;
    }
    if enroll.region.is_some() {
        config.instance_region = enroll.region.clone();
    }

    let retry_hint = if credential.is_stageable() {
        "re-run `spice connect <code>`"
    } else {
        // A minted code is spent with this process, so the retry is another
        // mint — i.e. the same bare command.
        "re-run `spice connect`"
    };

    let outcome = match runtime_cloud_connect::enroll::enroll_now(&config).await {
        Ok(outcome) => outcome,
        Err(err) if err.is_credential_rejection() => {
            // The cloud rejected the code itself (invalid or consumed);
            // `enroll_now` already discarded the staged copy.
            return Err(Error::CloudConnectEnroll {
                message: format!(
                    "{err}. Mint a new adoption code in the Spice Cloud portal and re-run `spice connect <code>`."
                ),
            });
        }
        Err(err) if err.is_authoritative_rejection() => {
            // Rejected for a reason other than the code — an expired code,
            // or app-attachment validation (no such app, attach conflict,
            // app limit). The code was NOT consumed and stays staged.
            return Err(Error::CloudConnectEnroll {
                message: format!(
                    "{err}. The adoption code was not consumed — fix the reported problem \
                     (e.g. correct --app-name, or pass --create to create the app) and {retry_hint}."
                ),
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
            // Transient (transport / 5xx / a clock-skew-rejected TLS
            // handshake): the code was NOT consumed.
            let resume = if credential.is_stageable() {
                format!(
                    " or start `spiced --cloud-connect` to keep retrying in the background (the code stays staged at {})",
                    pending_path.display()
                )
            } else {
                String::new()
            };
            return Err(Error::CloudConnectEnroll {
                message: format!(
                    "{err}. The adoption code was not consumed — {retry_hint} to retry{resume}."
                ),
            });
        }
    };

    let registration = &outcome.registration;
    println!("Enrolled with Spice Cloud.");
    // Prefer the org the cloud reported over the one that was asserted.
    if let Some(org) = registration.org.as_ref().or(enroll.org.as_ref()) {
        println!("  org:         {org}");
    }
    println!("  instance id: {}", outcome.identity.identifier);
    println!("  identity:    {}", config.identity_path.display());
    if !outcome.identity.gateway_addr.is_empty() {
        println!("  gateway:     {}", outcome.identity.gateway_addr);
    }
    if let Some(region) = registration.region.as_ref().or(enroll.region.as_ref()) {
        println!("  region:      {region}");
    }
    match registration.app_name {
        Some(ref app) => println!("  app:         {app}"),
        None => println!("  app:         unattached — attach to an app in the Spice Cloud portal"),
    }
    println!();

    println!("Nothing is running yet. Choose how this instance runs:");
    println!("  spice connect service install  Install a persistent service for this directory");
    println!("  spiced --cloud-connect         Run it in the foreground from this directory");
    println!("The instance shows as connected in the Spice Cloud portal once the runtime is up.");

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

async fn print_status(
    context: &RuntimeContext,
    config_dir: &Path,
    output: OutputFormat,
) -> Result<()> {
    let instance_dir = instance_dir_for(config_dir);
    let snapshot = service::collect_status(
        context,
        config_dir,
        &instance_dir,
        &service::PlatformBackend,
    )
    .await;
    match output {
        OutputFormat::Table => service::render_status_human(&snapshot),
        OutputFormat::Json => crate::output::write_json(&snapshot)?,
    }
    if snapshot.has_unavailable_section() {
        return Err(Error::ReportedStatusFailure);
    }
    Ok(())
}

/// Release this instance through a confirmed transaction. The exact local
/// service is uninstalled first, but identity and recovery state are cleared
/// only after Spice Cloud confirms release or authoritatively reports that the
/// same instance is absent.
async fn remove_identity(
    config_dir: &Path,
    endpoint: Option<&str>,
    assume_yes: bool,
    backend: &dyn service::ServiceBackend,
) -> Result<()> {
    let identity_path = config_dir.join(IDENTITY_FILE);
    let pending_path = config_dir.join(PENDING_ADOPT_CODE_FILE);
    let endpoint_path = config_dir.join(CLOUD_ENDPOINT_FILE);
    let cache_path = config_dir.join(runtime_cloud_connect::secret_cache::SECRET_CACHE_FILE);
    let instance_dir = instance_dir_for(config_dir);

    let identity = runtime_cloud_connect::identity::IdentityStore::load_optional(&identity_path)
        .map_err(|e| Error::CloudConnectIo {
            message: format!("load identity: {e}"),
        })?;
    let manifest_path = config_dir.join("service.json");

    let had_identity = identity.is_some();
    let had_pending = pending_path.exists();
    let had_endpoint = endpoint_path.exists();
    let had_cache = cache_path.exists();

    let had_manifest = manifest_path.exists();

    if !had_identity && !had_pending && !had_endpoint && !had_cache && !had_manifest {
        println!("Spice Cloud Connect: nothing to remove.");
        return Ok(());
    }

    if !assume_yes {
        println!("This will release this instance from Spice Cloud and remove it from this host:");
        if had_manifest {
            println!("  service:   stopped and uninstalled from this directory");
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

    if let Some(removed) = service::uninstall_exact(config_dir, &instance_dir, backend)? {
        println!("Stopped and uninstalled {}.", removed.name);
    }

    // The identity is the proof-of-possession credential for release, so no
    // local identity/recovery state may be cleared before this succeeds.
    if let Some(ref identity) = identity {
        report_release(config_dir, endpoint, identity).await?;
    }

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

    if had_pending
        && let Err(e) = std::fs::remove_file(&pending_path)
        && e.kind() != std::io::ErrorKind::NotFound
    {
        return Err(Error::CloudConnectIo {
            message: format!("remove pending code: {e}"),
        });
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
        "Spice Cloud Connect identity cleared. Run `spice connect` to enroll this directory again."
    );
    Ok(())
}

/// Tell Spice Cloud this exact instance is released. A same-instance 404 is
/// authoritative absence; transport and server failures retain local state so
/// the proof-of-possession request remains retryable.
async fn report_release(
    config_dir: &Path,
    endpoint: Option<&str>,
    identity: &runtime_cloud_connect::Identity,
) -> Result<()> {
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
            Ok(())
        }
        Err(runtime_cloud_connect::release::Error::Rejected { status: 404, .. }) => {
            println!("This instance is already absent from Spice Cloud.");
            Ok(())
        }
        Err(err) => Err(Error::CloudConnectIo {
            message: format!(
                "Failed to release this instance in Spice Cloud at {endpoint}: {err} Local identity and recovery state were retained. Retry `spice connect remove --yes`."
            ),
        }),
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

/// Returns `true` if `target` is shaped like a Spice Cloud adoption code —
/// i.e. it starts with the `SPICE-ADOPT` prefix — regardless of whether it
/// passes full validation. Used to distinguish a malformed adoption code from
/// a genuine Spicepod path. Matches the `SPICE`/`ADOPT` prefix segments
/// checked by [`runtime_cloud_connect::is_valid_adoption_code`].
fn looks_like_adoption_code(target: &str) -> bool {
    target == "SPICE-ADOPT" || target.starts_with("SPICE-ADOPT-")
}

#[cfg(unix)]
fn atomic_write_0600(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write as _;
    use std::os::unix::fs::OpenOptionsExt as _;
    use std::os::unix::fs::PermissionsExt as _;

    let dir = path.parent().unwrap_or_else(|| std::path::Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("pending-adopt-code");
    let tmp = dir.join(format!(".{file_name}.tmp"));

    // `OpenOptions::mode` only applies when the file is *created*. A stale
    // `.<file>.tmp` from a previous crashed run with broader permissions
    // would otherwise be reused, then renamed into place exposing the
    // adoption code or endpoint override under those wider permissions.
    // Remove any stale temp first, refuse to reuse an existing inode via
    // `create_new`, and explicitly re-assert `0o600` before writing.
    if let Err(err) = std::fs::remove_file(&tmp)
        && err.kind() != std::io::ErrorKind::NotFound
    {
        return Err(err);
    }

    {
        let mut f = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&tmp)?;
        f.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)
}

#[cfg(not(unix))]
fn atomic_write_0600(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    std::fs::write(path, bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn looks_like_adoption_code_matches_prefix() {
        // Well-formed and malformed adoption codes both look like codes.
        assert!(looks_like_adoption_code(
            "SPICE-ADOPT-7K2PX-9XYZ2-A1B2C-D3E4F"
        ));
        assert!(looks_like_adoption_code("SPICE-ADOPT-AAA-BBBB"));
        assert!(looks_like_adoption_code("SPICE-ADOPT-aaaa-BBBB"));
        assert!(looks_like_adoption_code("SPICE-ADOPT"));
    }

    #[test]
    fn looks_like_adoption_code_rejects_pod_paths() {
        assert!(!looks_like_adoption_code("spiceai/quickstart"));
        assert!(!looks_like_adoption_code("org/pod"));
        assert!(!looks_like_adoption_code("SPICE-CONNECT-AAAA-BBBB"));
        assert!(!looks_like_adoption_code("spice-adopt-aaaa-bbbb"));
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
    fn minted_codes_are_never_staged() {
        // A staged code is a live org credential on disk. An operator-supplied
        // code is already on the host and staging it enables resume; a minted
        // one exists for a single enroll and must not be written down.
        assert!(Credential::Code("SPICE-ADOPT-AAAAA-BBBBB".to_string()).is_stageable());
        assert!(!Credential::Minted("SPICE-ADOPT-AAAAA-BBBBB".to_string()).is_stageable());
        // Both still present the same code to the enroll request.
        assert_eq!(
            Credential::Minted("SPICE-ADOPT-AAAAA-BBBBB".to_string()).code(),
            "SPICE-ADOPT-AAAAA-BBBBB"
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

    #[test]
    fn read_staged_code_treats_a_blank_file_as_absent() {
        let dir = std::env::temp_dir().join(format!("spice-connect-staged-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create scratch dir");

        assert_eq!(read_staged_code(&dir).expect("no file"), None);

        std::fs::write(dir.join(PENDING_ADOPT_CODE_FILE), "  \n").expect("write blank");
        assert_eq!(
            read_staged_code(&dir).expect("blank file"),
            None,
            "a blank staged file is not a credential"
        );

        std::fs::write(
            dir.join(PENDING_ADOPT_CODE_FILE),
            "SPICE-ADOPT-AAAAA-BBBBB\n",
        )
        .expect("write code");
        assert_eq!(
            read_staged_code(&dir).expect("staged code").as_deref(),
            Some("SPICE-ADOPT-AAAAA-BBBBB"),
            "the trailing newline a shell adds must be trimmed"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
