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
//! 1. **Cloud Connect interactive setup** (remote management of `spiced` from
//!    Spice Cloud). Bare `spice connect` authenticates the user, enrolls this
//!    directory, creates and attaches a project, then starts the instance in
//!    the foreground. Unattended enrollment belongs to the runtime itself via
//!    `spiced --token <enrollment-key>` and does not create a project.
//!
//! 2. **Deprecated pod-add behavior**: when the argument is a Spicepod
//!    path on Spice.ai Cloud (e.g. `spiceai/quickstart`), this prints a
//!    deprecation notice and behaves like `spice add <pod>` with Spice.ai
//!    Cloud authentication headers.

mod naming;
mod project;
#[cfg(test)]
#[expect(
    dead_code,
    reason = "the stacked Linux service PR wires these lifecycle entry points"
)]
mod service;
mod state;
mod transaction;

use std::io::IsTerminal as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::commands::add::{AddArgs, execute_add_or_connect};
use crate::context::RuntimeContext;
use crate::error::{Error, Result};
use clap::{Args, Subcommand};
use runtime_cloud_connect::config::{
    CLOUD_MANAGED_SPICEPOD_FILE, CloudConnectConfig, DEPLOYMENT_TRANSACTION_FILE,
    DEPLOYMENT_TRANSACTION_INCOMING_FILE, IDENTITY_FILE, INCOMING_SECRET_CACHE_FILE,
    PREVIOUS_CLOUD_MANAGED_SPICEPOD_FILE, PREVIOUS_SECRET_CACHE_FILE,
};
use secrecy::{ExposeSecret as _, SecretString};

/// Arguments for the `spice connect` command.
#[derive(Args, Debug)]
#[command(
    about = "Connect this directory to Spice Cloud and start its instance",
    long_about = r#"`spice connect` enrolls this directory with Spice Cloud and manages its instance.

This is an interactive setup flow. It authenticates a user, resolves an
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

`spice connect remove --force --yes` is the explicit recovery path for an
unusable identity or an enrollment transaction that must be abandoned. It
clears only this directory's local Cloud state; delete any Cloud project
separately.

Use `--dir <path>` to manage an instance rooted at a different directory:
per-instance state lives under `<dir>/.spice`, so multiple instances on one
host enroll independently. `SPICE_CONFIG_DIR` overrides the derived location
entirely and wins over `--dir`.

DEPRECATED POD-ADD BEHAVIOR:
  spice connect <org>/<pod>               Deprecated; use `spice add <org>/<pod>`.

EXAMPLES
  spice connect
  spice connect --dir /srv/edge --region us-west-2
  spice connect remove --force --yes

Docs: https://spiceai.org/docs"#
)]
pub struct ConnectArgs {
    /// Explicit state-recovery command. Setup remains the default when absent.
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

    /// Override the Spice Cloud endpoint used for enrollment. Defaults to
    /// `https://api.spice.ai`. Also
    /// configurable via `SPICE_CLOUD_ENDPOINT`.
    #[arg(long, value_name = "URL", global = true)]
    pub endpoint: Option<String>,

    /// The instance directory: per-instance Cloud Connect state (the
    /// enrolled identity) lives under `<dir>/.spice`. Defaults to the
    /// current directory. `SPICE_CONFIG_DIR` overrides the derived `.spice`
    /// location entirely.
    #[arg(long, value_name = "PATH", global = true)]
    pub dir: Option<PathBuf>,

    /// Confirm local state abandonment without another prompt.
    #[arg(long, short = 'y', global = true)]
    pub yes: bool,

    /// Abandon only this directory's local Cloud state. This does not delete a
    /// Cloud project; clean up that project separately.
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

/// Cloud Connect state-recovery commands available with interactive setup.
#[derive(Subcommand, Debug)]
pub enum ConnectCommand {
    /// Abandon this directory's local Cloud identity and retry state. Requires
    /// `--force --yes`; Cloud project cleanup remains a separate operation.
    Remove,
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

impl ConnectArgs {
    /// Whether this invocation writes JSON to stdout, so the dispatcher can
    /// suppress the version banner that would otherwise foul it.
    #[must_use]
    pub fn produces_json(&self) -> bool {
        false
    }

    /// Select JSON output wherever this command has a structured form.
    pub fn apply_machine_mode(&mut self) {
        // Interactive setup has no structured output mode.
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

    if matches!(args.command, Some(ConnectCommand::Remove)) {
        reject_cloud_region(args.cloud_region.as_deref())?;
        if args.endpoint.is_some() {
            return Err(Error::InvalidUsage {
                message: "--endpoint does not apply to `connect remove`; --force abandons local state only, and Cloud project cleanup is separate."
                    .to_string(),
            });
        }
        if args.region.is_some() {
            return Err(Error::InvalidUsage {
                message: "--region applies to enrollment, not to `connect remove`.".to_string(),
            });
        }
        return abandon_local_state(&config_dir, args.yes, args.force).await;
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
                message: "`spice connect` accepts only interactive setup, `connect remove`, or the deprecated `<org>/<pod>` Spicepod form. Lifecycle words such as `status`, `start`, and `stop` are not remote Spicepod targets."
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
    // The command promises to finish by running the instance in this terminal.
    // Refuse a platform that cannot launch the local runtime before enrollment
    // or project creation commits any remote state.
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
    start_foreground_instance(ctx, Some(&directory), args.verbosity).await
}

/// Explicitly abandon local transaction and credential state.
///
/// This is the recovery half of removal: it is intentionally force-only and
/// makes no claim about Cloud-side project cleanup. The shared mutation lock
/// is outer to the runtime lease, matching enrollment's lock order, so removal
/// cannot win a gap and have enrollment recreate the state it just cleared.
async fn abandon_local_state(config_dir: &Path, yes: bool, force: bool) -> Result<()> {
    if !force || !yes {
        return Err(Error::InvalidUsage {
            message: "Local Cloud Connect recovery requires `spice connect remove --force --yes`. This abandons only local identity and retry state; delete any Cloud project separately.".to_string(),
        });
    }

    let mutation_lock = runtime_cloud_connect::MutationLock::acquire(config_dir, "remove")
        .await
        .map_err(|source| Error::CloudConnectIo {
            message: format!("acquire Cloud Connect state for local recovery: {source}"),
        })?;
    let display_config_dir = mutation_lock
        .config_dir()
        .map_err(|source| Error::CloudConnectIo {
            message: format!("validate locked Cloud Connect state: {source}"),
        })?
        .to_path_buf();
    abandon_local_state_locked(&mutation_lock, &display_config_dir).await
}

async fn abandon_local_state_locked(
    mutation_lock: &runtime_cloud_connect::MutationLock,
    display_config_dir: &Path,
) -> Result<()> {
    let config_dir = mutation_lock
        .descriptor_relative_config_dir()
        .map_err(|source| Error::CloudConnectIo {
            message: format!("pin locked Cloud Connect state for local recovery: {source}"),
        })?;
    let _runtime_lock =
        runtime_cloud_connect::RuntimeLock::acquire(&config_dir).map_err(|source| {
            Error::CloudConnectIo {
                message: format!(
                    "{source} Stop the running instance before using `spice connect remove`."
                ),
            }
        })?;

    // One enrollment transaction spans the entire cleanup. Taking it once is
    // what prevents `spiced --token` from publishing a new draft or identity
    // between two individually locked deletes while this command still reports
    // that the directory was cleared.
    let enrollment_transaction = Arc::new(
        runtime_cloud_connect::EnrollmentTransactionLock::try_acquire_async(&config_dir)
            .await
            .map_err(|source| Error::CloudConnectIo {
                message: format!("acquire the enrollment transaction for local recovery: {source}"),
            })?,
    );
    let identity_path = config_dir.join(IDENTITY_FILE);
    let blocking_transaction = Arc::clone(&enrollment_transaction);
    tokio::task::spawn_blocking(move || {
        // Carry a guard into the uncancellable blocking task. If the async
        // caller is dropped, cleanup cannot continue after releasing the
        // enrollment exclusion.
        let _enrollment_transaction = blocking_transaction;
        clear_local_cloud_state(&config_dir)
    })
    .await
    .map_err(|source| Error::CloudConnectIo {
        message: format!("local Cloud Connect cleanup task failed: {source}"),
    })??;
    enrollment_transaction
        .delete_draft_async()
        .await
        .map_err(|source| Error::CloudConnectIo {
            message: format!("remove enrollment draft: {source}"),
        })?;
    runtime_cloud_connect::identity::IdentityStore::clear_with_transaction_async(
        identity_path,
        Arc::clone(&enrollment_transaction),
    )
    .await
    .map_err(|source| Error::CloudConnectIo {
        message: format!("remove Cloud Connect identity: {source}"),
    })?;

    println!(
        "Removed local Spice Cloud Connect state from {}. Cloud project cleanup, if any, must be completed separately.",
        display_config_dir.display()
    );
    Ok(())
}

/// Remove the auxiliary files of one local Cloud generation on the blocking
/// pool. The caller retains the mutation, runtime, and enrollment leases, and
/// removes the draft and identity through that same enrollment transaction
/// after this returns. `config_dir` is rooted at the locked directory
/// descriptor, so no pathname replacement can redirect these deletes.
fn clear_local_cloud_state(config_dir: &Path) -> Result<()> {
    state::remove_durable_file(
        &config_dir.join(runtime_cloud_connect::secret_cache::SECRET_CACHE_FILE),
    )
    .map_err(|source| Error::CloudConnectIo {
        message: format!("remove delivered-secret cache: {source}"),
    })?;

    for file in [
        CLOUD_MANAGED_SPICEPOD_FILE,
        "spicepod-cloud-managed.incoming.yml",
        "spicepod-cloud-managed.bak",
        DEPLOYMENT_TRANSACTION_FILE,
        DEPLOYMENT_TRANSACTION_INCOMING_FILE,
        PREVIOUS_CLOUD_MANAGED_SPICEPOD_FILE,
        PREVIOUS_SECRET_CACHE_FILE,
        INCOMING_SECRET_CACHE_FILE,
    ] {
        state::remove_durable_file(&config_dir.join(file)).map_err(|source| {
            Error::CloudConnectIo {
                message: format!("remove Cloud Connect deployment state {file}: {source}"),
            }
        })?;
    }

    remove_local_credential_debris(config_dir)?;
    state::ConnectOperation::delete(config_dir).map_err(|source| Error::CloudConnectIo {
        message: format!("remove enrollment journal: {source}"),
    })?;
    state::ProjectOperation::delete(config_dir).map_err(|source| Error::CloudConnectIo {
        message: format!("remove project journal: {source}"),
    })?;
    state::remove_durable_file(&config_dir.join(CloudConnectConfig::ENDPOINT_OVERRIDE_FILE))
        .map_err(|source| Error::CloudConnectIo {
            message: format!("remove endpoint override: {source}"),
        })?;
    Ok(())
}

/// Remove secret-bearing crash/recovery files whose names are created by the
/// enrollment and identity writers. The caller owns the mutation, runtime, and
/// enrollment exclusion needed to treat every matching temp/backup as dead.
/// Match only exact generated name families; never recurse or follow links.
fn remove_local_credential_debris(config_dir: &Path) -> Result<()> {
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
        let extension = Path::new(&name)
            .extension()
            .and_then(std::ffi::OsStr::to_str);
        let generated_draft_quarantine =
            name.starts_with("enrollment-draft.quarantine.") && extension == Some("json");
        let generated_draft_temp =
            name.starts_with(".enrollment-draft.json.") && matches!(extension, Some("tmp" | "bak"));
        let generated_identity_temp =
            name.starts_with(".identity.json.") && matches!(extension, Some("tmp" | "bak"));
        let fixed_identity_backup = name == "identity.bak" || name == "identity.json.bak";
        if generated_draft_quarantine
            || generated_draft_temp
            || generated_identity_temp
            || fixed_identity_backup
        {
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

/// Reject the legacy global Cloud data-region flag on Cloud Connect setup.
/// Interactive setup has its own instance-location and control-plane options;
/// local recovery has neither.
fn reject_cloud_region(cloud_region: Option<&str>) -> Result<()> {
    let Some(region) = cloud_region.filter(|r| !r.is_empty()) else {
        return Ok(());
    };
    Err(Error::InvalidUsage {
        message: format!(
            "--cloud-region {region} selects a Spice.ai Cloud data region and does not apply to \
             `spice connect`. For interactive setup, use --region <location> for the instance \
             label and --endpoint <url> for the enrollment control plane. `connect remove` is \
             local-only and accepts neither option. See: https://spiceai.org/docs"
        ),
    })
}

async fn start_foreground_instance(
    ctx: &RuntimeContext,
    dir: Option<&Path>,
    verbosity: u8,
) -> Result<()> {
    // The same preflight runs before the transaction; retain the error here as
    // a defensive boundary for callers that invoke this helper directly.
    ctx.ensure_local_runtime_supported()?;

    println!("Starting the Spice runtime. Press Ctrl-C to stop it.");
    // The transaction's block reports durable enrollment/attachment only. It
    // deliberately does not claim the process is connected or serving; the
    // runtime prints that distinct completion after both its listener is bound
    // and Cloud acknowledges this session. `AlreadyReported` is reserved for a
    // launcher that already emitted that final runtime claim.
    crate::runtime_launcher::run_runtime(
        ctx,
        &crate::runtime_launcher::RunConfig {
            working_dir: dir.map(Path::to_path_buf),
            verbosity,
            connection_report: crate::runtime_launcher::ConnectionReport::Runtime,
            ..crate::runtime_launcher::RunConfig::default()
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser as _;

    #[derive(clap::Parser, Debug)]
    #[command(name = "spice")]
    struct Harness {
        #[command(subcommand)]
        command: HarnessCommand,
    }

    #[derive(clap::Subcommand, Debug)]
    enum HarnessCommand {
        Connect(ConnectArgs),
    }

    fn parse(args: &[&str]) -> std::result::Result<ConnectArgs, clap::Error> {
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
            "status",
            "start",
            "stop",
            "restart",
            "logs",
            "spiceai/quickstart/extra",
            "/quickstart",
            "spiceai/",
        ] {
            assert!(
                !is_deprecated_spicepod_target(target),
                "{target} must not reach the deprecated pod-add path"
            );
        }
    }

    #[test]
    fn connect_exposes_only_interactive_setup_options() {
        let args = parse(&[
            "spice",
            "connect",
            "--dir",
            "/srv/edge",
            "--region",
            "us-west-2",
        ])
        .expect("interactive setup options parse");
        assert_eq!(args.dir.as_deref(), Some(Path::new("/srv/edge")));
        assert_eq!(args.region.as_deref(), Some("us-west-2"));

        for flag in ["--org", "--project", "--token"] {
            assert!(
                parse(&["spice", "connect", flag, "value"]).is_err(),
                "{flag} must not be accepted by spice connect"
            );
        }
    }

    #[test]
    fn force_remove_is_an_explicit_local_recovery_command() {
        let args = parse(&[
            "spice",
            "connect",
            "remove",
            "--force",
            "--yes",
            "--dir",
            "/srv/edge",
        ])
        .expect("local recovery parses");
        assert!(matches!(args.command, Some(ConnectCommand::Remove)));
        assert!(args.force);
        assert!(args.yes);
        assert_eq!(args.dir.as_deref(), Some(Path::new("/srv/edge")));
    }

    #[test]
    fn connect_has_no_machine_output_mode() {
        let mut args = parse(&["spice", "connect"]).expect("parse bare connect");
        args.apply_machine_mode();
        assert!(!args.produces_json());
    }

    #[tokio::test]
    async fn local_recovery_durably_removes_the_previous_cloud_generation() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let config_dir = dir.path().join(".spice");
        std::fs::create_dir_all(&config_dir).expect("create config directory");
        let artifacts = [
            IDENTITY_FILE,
            "enrollment-draft.json",
            "enrollment-draft.quarantine.1.2.json",
            ".enrollment-draft.json.abandoned.tmp",
            ".identity.json.interrupted.bak",
            "identity.bak",
            runtime_cloud_connect::secret_cache::SECRET_CACHE_FILE,
            CLOUD_MANAGED_SPICEPOD_FILE,
            "spicepod-cloud-managed.incoming.yml",
            "spicepod-cloud-managed.bak",
            DEPLOYMENT_TRANSACTION_FILE,
            DEPLOYMENT_TRANSACTION_INCOMING_FILE,
            PREVIOUS_CLOUD_MANAGED_SPICEPOD_FILE,
            PREVIOUS_SECRET_CACHE_FILE,
            INCOMING_SECRET_CACHE_FILE,
        ]
        .map(|file| config_dir.join(file));
        for artifact in &artifacts {
            std::fs::write(artifact, b"previous Cloud generation")
                .expect("write previous Cloud artifact");
        }

        abandon_local_state(&config_dir, true, true)
            .await
            .expect("local recovery succeeds");

        for artifact in artifacts {
            assert!(
                !artifact.exists(),
                "local recovery must remove {}",
                artifact.display()
            );
        }
    }

    #[tokio::test]
    async fn local_recovery_deletes_nothing_without_enrollment_transaction_ownership() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let config_dir = dir.path().join(".spice");
        let active =
            runtime_cloud_connect::EnrollmentTransactionLock::try_acquire_async(&config_dir)
                .await
                .expect("hold the enrollment transaction");
        let identity = config_dir.join(IDENTITY_FILE);
        let draft = runtime_cloud_connect::EnrollmentDraft::path_in(&config_dir);
        let cache = config_dir.join(runtime_cloud_connect::secret_cache::SECRET_CACHE_FILE);
        for (path, contents) in [
            (&identity, b"active identity".as_slice()),
            (&draft, b"active draft".as_slice()),
            (&cache, b"active cache".as_slice()),
        ] {
            std::fs::write(path, contents).expect("write active Cloud state");
        }

        let error = abandon_local_state(&config_dir, true, true)
            .await
            .expect_err("another enrollment transaction must exclude local recovery");

        assert!(
            error.to_string().contains("Another live process"),
            "the refusal must explain that enrollment still owns the state: {error}"
        );
        for path in [&identity, &draft, &cache] {
            assert!(
                path.exists(),
                "recovery must delete nothing before it owns the enrollment transaction: {}",
                path.display()
            );
        }
        drop(active);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_recovery_clears_the_locked_directory_after_path_replacement() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let config_dir = dir.path().join(".spice");
        std::fs::create_dir_all(&config_dir).expect("create config directory");
        std::fs::write(config_dir.join(IDENTITY_FILE), b"locked identity")
            .expect("write locked identity");

        let mutation_lock = runtime_cloud_connect::MutationLock::acquire(
            &config_dir,
            "local-recovery-path-replacement-test",
        )
        .await
        .expect("acquire mutation lock");
        let moved_config_dir = dir.path().join("locked-spice");
        std::fs::rename(&config_dir, &moved_config_dir).expect("rename locked directory");
        std::fs::create_dir_all(&config_dir).expect("create replacement directory");
        let replacement_identity = config_dir.join(IDENTITY_FILE);
        std::fs::write(&replacement_identity, b"replacement identity")
            .expect("write replacement identity");

        abandon_local_state_locked(&mutation_lock, &config_dir)
            .await
            .expect("descriptor-rooted recovery succeeds");

        assert!(
            !moved_config_dir.join(IDENTITY_FILE).exists(),
            "the identity in the locked directory must be removed"
        );
        assert_eq!(
            std::fs::read(&replacement_identity).expect("read replacement identity"),
            b"replacement identity",
            "path replacement must not redirect destructive cleanup"
        );
    }
}
