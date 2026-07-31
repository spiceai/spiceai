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
//!    Spice Cloud). The user passes an adoption code obtained in the
//!    Spice Cloud portal:
//!
//!    ```text
//!    spice connect SPICE-ADOPT-7K2PX-9XYZ2-A1B2C-D3E4F
//!    ```
//!
//!    The command **enrolls and exits**: it stages the code, installs the
//!    runtime if missing, completes the HTTPS enroll against the cloud
//!    (identity issued and persisted, registry row created), prints the
//!    next steps, and returns — it does not start `spiced`. Start the
//!    runtime with `spiced --cloud-connect` (or install it as a service)
//!    to bring the instance online. `status`/`forget` inspect and clear
//!    the local state.
//!
//! 2. **Deprecated pod-add behavior**: when the argument is a Spicepod
//!    path on Spice.ai Cloud (e.g. `spiceai/quickstart`), this prints a
//!    deprecation notice and behaves like `spice add <pod>` with Spice.ai
//!    Cloud authentication headers.

use std::path::{Path, PathBuf};

use crate::commands::add::{AddArgs, execute_add_or_connect};
use crate::context::RuntimeContext;
use crate::error::Result;
use clap::{Args, Subcommand};
use runtime_cloud_connect::config::{CloudConnectConfig, IDENTITY_FILE, PENDING_ADOPT_CODE_FILE};

/// Arguments for the `spice connect` command.
#[derive(Args, Debug)]
#[command(
    about = "Enroll this host with Spice Cloud (or add a cloud-hosted Spicepod)",
    long_about = r#"`spice connect` enrolls this host with Spice Cloud and exits.

  spice connect SPICE-ADOPT-XXXXX-XXXXX-XXXXX-XXXXX
                                          Enroll with an adoption code from the
                                          Spice Cloud portal: the identity is
                                          issued and stored locally, the
                                          instance appears in the portal
                                          registry, and the command exits.
                                          Nothing is left running — start the
                                          runtime with `spiced --cloud-connect`
                                          to bring the instance online.
  spice connect status                    Show the current enrollment state.
  spice connect forget                    Clear the local identity on disk.
                                          A running `spiced` keeps its
                                          in-memory identity until it is
                                          restarted or the cloud sends a Forget
                                          command (a mere stream drop just
                                          reconnects with the same identity),
                                          so restart spiced to stop remote
                                          management immediately.

Use `--dir <path>` to enroll an instance rooted at a different directory:
per-instance state lives under `<dir>/.spice`, so multiple instances on one
host enroll independently.

DEPRECATED POD-ADD BEHAVIOR:
  spice connect <org>/<pod>               Deprecated; use `spice add <org>/<pod>`.

EXAMPLES
  spice connect SPICE-ADOPT-7K2PX-9XYZ2-A1B2C-D3E4F
  spice connect SPICE-ADOPT-7K2PX-9XYZ2-A1B2C-D3E4F --dir /opt/edge-1
  spice connect status
  spice connect forget

Docs: https://spiceai.org/docs"#
)]
pub struct ConnectArgs {
    /// Optional explicit subcommand. If absent, the first positional
    /// argument (`target`) is inspected to decide between enrollment flow
    /// and the deprecated pod-add behavior.
    #[command(subcommand)]
    pub command: Option<ConnectCommand>,

    /// First positional argument: either a Spice Cloud adoption code
    /// (`SPICE-ADOPT-...`) or a Spicepod path (`<org>/<pod>`).
    #[arg(value_name = "TARGET")]
    pub target: Option<String>,

    /// Override the Spice Cloud enroll endpoint the adoption code is
    /// presented to. Defaults to `https://cloud.spice.ai`. Also
    /// configurable via `SPICE_CLOUD_ENDPOINT`. The gateway (stream)
    /// address is issued by the enroll response, not configured here.
    #[arg(long, value_name = "URL")]
    pub endpoint: Option<String>,

    /// The instance directory: per-instance Cloud Connect state (the
    /// identity and staged adoption code) lives under `<dir>/.spice`.
    /// Defaults to the current directory; resolved to an absolute path at
    /// enroll time. `SPICE_CONFIG_DIR` overrides the derived `.spice`
    /// location entirely. Applies to enrollment, `status`, and `forget`.
    #[arg(long, value_name = "PATH", global = true)]
    pub dir: Option<PathBuf>,

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
}

/// Cloud-connect subcommands.
#[derive(Subcommand, Debug)]
pub enum ConnectCommand {
    /// Show the current Spice Cloud Connect adoption state.
    Status,

    /// Clear the local Spice Cloud Connect identity. spiced will
    /// continue running unmanaged after the next restart.
    Forget,
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
        return execute_subcommand(&cmd, &config_dir);
    }

    let Some(target) = args.target.as_deref() else {
        // No positional argument and no subcommand — default to status
        // so that `spice connect` with no args is informative.
        return execute_subcommand(&ConnectCommand::Status, &config_dir);
    };

    if runtime_cloud_connect::is_valid_adoption_code(target) {
        let attach = AppAttachArgs {
            app_name: args.app_name,
            create: args.create,
        };
        return enroll_instance(ctx, target, args.endpoint.as_deref(), &config_dir, attach).await;
    }

    // An input that clearly looks like an adoption code but fails validation
    // is a malformed code, not a Spicepod path. Treating it as a pod path
    // produces a misleading cloud-Spicepod error and may fire a cloud pod-add
    // request for what was plainly meant to be an adoption code, so reject it
    // explicitly instead of falling through to the pod-add path.
    if looks_like_adoption_code(target) {
        return Err(crate::error::Error::InvalidArgument {
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

fn execute_subcommand(cmd: &ConnectCommand, config_dir: &Path) -> Result<()> {
    match cmd {
        ConnectCommand::Status => print_status(config_dir),
        ConnectCommand::Forget => forget_identity(config_dir),
    }
}

/// App-attachment intent from the `--app-name` / `--create` flags.
struct AppAttachArgs {
    app_name: Option<String>,
    create: bool,
}

/// Enroll this host with Spice Cloud and exit.
///
/// Sequence: stage the code under the config dir (so a failed or
/// interrupted enroll can be resumed by a later `spiced` start), install
/// the runtime if missing, complete the HTTPS enroll (identity issued and
/// persisted, registry row created cloud-side, app attached when
/// requested), print the next steps, and return — `spiced` is never
/// started.
async fn enroll_instance(
    ctx: &RuntimeContext,
    code: &str,
    endpoint: Option<&str>,
    config_dir: &Path,
    attach: AppAttachArgs,
) -> Result<()> {
    let pending_path = config_dir.join(PENDING_ADOPT_CODE_FILE);
    std::fs::create_dir_all(config_dir).map_err(|e| crate::error::Error::CloudConnectIo {
        message: format!("create config dir {}: {e}", config_dir.display()),
    })?;

    let endpoint_path = config_dir.join("cloud-endpoint");

    // If the user did NOT pass `--endpoint`, remove any previous override
    // so the next `spiced` start doesn't silently re-use a stale endpoint
    // from an earlier connect. A `forget` also clears this file, but
    // re-staging without `--endpoint` is the more common case.
    if endpoint.is_none()
        && let Err(e) = std::fs::remove_file(&endpoint_path)
        && e.kind() != std::io::ErrorKind::NotFound
    {
        return Err(crate::error::Error::CloudConnectIo {
            message: format!(
                "remove stale endpoint override {}: {e}",
                endpoint_path.display()
            ),
        });
    }

    atomic_write_0600(&pending_path, code.as_bytes()).map_err(|e| {
        crate::error::Error::CloudConnectIo {
            message: format!("write adoption code: {e}"),
        }
    })?;

    // Write the endpoint override BEFORE printing success. If the override
    // can't be persisted, roll the staged code back so adoption can't
    // proceed against the wrong control plane on the next `spiced` start.
    if let Some(ep) = endpoint
        && let Err(e) = atomic_write_0600(&endpoint_path, ep.as_bytes())
    {
        // Best-effort rollback of the staged code; surface the
        // original endpoint-write failure to the caller.
        let _ = std::fs::remove_file(&pending_path);
        return Err(crate::error::Error::CloudConnectIo {
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
    // The CLI and the runtime ship in lockstep, so the CLI's own version
    // stands in for the runtime version in the reported host facts.
    let mut config =
        CloudConnectConfig::from_env_at(env!("CARGO_PKG_VERSION"), config_dir.to_path_buf());
    // The explicit positional code wins over any env/staged state that
    // `from_env_at` picked up, and points at the file staged above so a
    // consumed or dead code is discarded with it.
    config.adoption_code = Some(code.to_string());
    config.pending_adopt_code_path = Some(pending_path.clone());
    if let Some(ep) = endpoint {
        // The explicit flag wins over `SPICE_CLOUD_ENDPOINT` for this
        // process; the `cloud-endpoint` file written above covers later
        // `spiced` starts.
        config.enroll_endpoint = ep.to_string();
    }
    // Flags win over the SPICE_CONNECT_ADOPT_* env vars `from_env_at`
    // picked up.
    if attach.app_name.is_some() {
        config.adopt_app_name = attach.app_name;
    }
    if attach.create {
        config.adopt_create_app = true;
    }

    let outcome = match runtime_cloud_connect::enroll::enroll_now(&config).await {
        Ok(outcome) => outcome,
        Err(err) if err.is_credential_rejection() => {
            // The cloud rejected the code itself (invalid or consumed);
            // `enroll_now` already discarded the staged copy.
            return Err(crate::error::Error::CloudConnectEnroll {
                message: format!(
                    "{err}. Mint a new adoption code in the Spice Cloud portal and re-run `spice connect <code>`."
                ),
            });
        }
        Err(err) if err.is_authoritative_rejection() => {
            // Rejected for a reason other than the code — an expired code,
            // or app-attachment validation (no such app, attach conflict,
            // app limit). The code was NOT consumed and stays staged.
            return Err(crate::error::Error::CloudConnectEnroll {
                message: format!(
                    "{err}. The adoption code was not consumed — fix the reported problem \
                     (e.g. correct --app-name, or pass --create to create the app) and re-run `spice connect <code>`."
                ),
            });
        }
        Err(err @ runtime_cloud_connect::enroll::EnrollNowError::Persist { .. }) => {
            // The identity was issued but could not be written; the message
            // carries the recovery steps (the code is already consumed).
            return Err(crate::error::Error::CloudConnectEnroll {
                message: err.to_string(),
            });
        }
        Err(err) => {
            // Transient (transport / 5xx): the code was NOT consumed and the
            // staged copy is kept, so both retry paths work.
            return Err(crate::error::Error::CloudConnectEnroll {
                message: format!(
                    "{err}. The adoption code was not consumed — re-run `spice connect <code>` to retry, \
                     or start `spiced --cloud-connect` to keep retrying in the background (the code stays staged at {}).",
                    pending_path.display()
                ),
            });
        }
    };

    println!("Enrolled with Spice Cloud.");
    println!("  instance id: {}", outcome.identity.identifier);
    println!("  identity:    {}", config.identity_path.display());
    if !outcome.identity.gateway_addr.is_empty() {
        println!("  gateway:     {}", outcome.identity.gateway_addr);
    }
    match outcome.app_name {
        Some(ref app) => println!("  app:         {app}"),
        None => println!("  app:         unattached — attach to an app in the Spice Cloud portal"),
    }
    println!();
    println!("Nothing is running yet. Start the runtime from the instance directory to connect:");
    println!("  spiced --cloud-connect");
    println!("The instance shows as connected in the Spice Cloud portal once the runtime is up.");

    Ok(())
}

fn print_status(config_dir: &Path) -> Result<()> {
    let identity_path = config_dir.join(IDENTITY_FILE);
    let pending_path = config_dir.join(PENDING_ADOPT_CODE_FILE);

    let identity = runtime_cloud_connect::identity::IdentityStore::load_optional(&identity_path)
        .map_err(|e| crate::error::Error::CloudConnectIo {
            message: format!("load identity: {e}"),
        })?;

    if let Some(id) = identity {
        let expiry = if id.not_after_unix == 0 {
            "unbounded".to_string()
        } else {
            format!("unix={} (expired={})", id.not_after_unix, id.is_expired())
        };
        println!("Spice Cloud Connect: adopted");
        println!("  identifier:  {}", id.identifier);
        println!("  identity:    {}", identity_path.display());
        if !id.gateway_addr.is_empty() {
            println!("  gateway:     {}", id.gateway_addr);
        }
        println!("  expiry:      {expiry}");
        return Ok(());
    }

    if pending_path.exists() {
        let preview = std::fs::read_to_string(&pending_path).map_err(|e| {
            crate::error::Error::CloudConnectIo {
                message: format!("read pending code: {e}"),
            }
        })?;
        let preview = preview.trim();
        println!("Spice Cloud Connect: pending enrollment");
        println!("  pending code at: {}", pending_path.display());
        let mask = mask_code(preview);
        println!("  code (masked):   {mask}");
        println!(
            "  re-run `spice connect <code>` to enroll with {}, or start `spiced --cloud-connect` to enroll in the background.",
            resolved_endpoint(&pending_path)
        );
        return Ok(());
    }

    println!("Spice Cloud Connect: not connected");
    println!("Run `spice connect <SPICE-ADOPT-...>` with a code from your Spice Cloud portal.");
    Ok(())
}

fn forget_identity(config_dir: &Path) -> Result<()> {
    let identity_path = config_dir.join(IDENTITY_FILE);
    let pending_path = config_dir.join(PENDING_ADOPT_CODE_FILE);
    let endpoint_path = config_dir.join("cloud-endpoint");

    let had_identity = identity_path.exists();
    let had_pending = pending_path.exists();
    let had_endpoint = endpoint_path.exists();

    if had_identity {
        runtime_cloud_connect::identity::IdentityStore::clear(&identity_path).map_err(|e| {
            crate::error::Error::CloudConnectIo {
                message: format!("clear identity: {e}"),
            }
        })?;
    }
    if had_pending
        && let Err(e) = std::fs::remove_file(&pending_path)
        && e.kind() != std::io::ErrorKind::NotFound
    {
        return Err(crate::error::Error::CloudConnectIo {
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
        return Err(crate::error::Error::CloudConnectIo {
            message: format!("remove endpoint override: {e}"),
        });
    }

    if had_identity || had_pending || had_endpoint {
        println!(
            "Spice Cloud Connect identity cleared. Run `spice connect <SPICE-ADOPT-...>` to re-adopt."
        );
    } else {
        println!("Spice Cloud Connect: nothing to forget.");
    }
    Ok(())
}

/// Resolve the endpoint `spiced` will actually contact, mirroring the
/// precedence used at runtime: `SPICE_CLOUD_ENDPOINT` env var first, then
/// the on-disk `cloud-endpoint` override (sibling of the pending code
/// file), then the built-in default.
fn resolved_endpoint(pending_path: &std::path::Path) -> String {
    if let Ok(env) = std::env::var("SPICE_CLOUD_ENDPOINT")
        && !env.is_empty()
    {
        return env;
    }
    if let Some(parent) = pending_path.parent() {
        let override_path = parent.join("cloud-endpoint");
        if let Ok(s) = std::fs::read_to_string(&override_path) {
            let trimmed = s.trim();
            if !trimmed.is_empty() {
                return trimmed.to_string();
            }
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

fn mask_code(code: &str) -> String {
    // Grouped form (`SPICE-ADOPT-XXXXX-XXXXX-...`): mask the interior segments,
    // keeping the `SPICE-ADOPT` prefix and the final segment for recognition.
    if code.contains('-') {
        let mut parts = code.split('-').collect::<Vec<_>>();
        let len = parts.len();
        if len < 3 {
            return code.to_string();
        }
        for part in parts.iter_mut().take(len - 1).skip(2) {
            *part = "****";
        }
        return parts.join("-");
    }
    // Dash-less token (the raw 64-hex code the portal mints today): a single-use
    // secret, so mask the middle by characters rather than printing it whole.
    // Short strings have nothing meaningful to hide and are left as-is.
    mask_opaque_token(code)
}

/// Mask an opaque single-use token, keeping a short prefix and suffix for
/// recognition and replacing the middle with `****`. Tokens short enough that
/// masking would reveal most of them are returned unchanged.
fn mask_opaque_token(token: &str) -> String {
    const KEEP: usize = 4;
    let chars: Vec<char> = token.chars().collect();
    if chars.len() <= KEEP * 2 {
        return token.to_string();
    }
    let prefix: String = chars[..KEEP].iter().collect();
    let suffix: String = chars[chars.len() - KEEP..].iter().collect();
    format!("{prefix}****{suffix}")
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
    fn mask_code_keeps_prefix_and_suffix() {
        assert_eq!(
            mask_code("SPICE-ADOPT-AAAA-BBBB-CCCC"),
            "SPICE-ADOPT-****-****-CCCC"
        );
    }

    #[test]
    fn mask_code_short_codes_unchanged() {
        assert_eq!(mask_code("SHORT"), "SHORT");
        assert_eq!(mask_code("FOO-BAR"), "FOO-BAR");
    }

    #[test]
    fn mask_code_masks_raw_hex_token() {
        // The cloud portal mints randomBytes(32).toString('hex') — a 64-char
        // dash-less single-use secret. `spice connect status` must NOT print it
        // whole: mask the middle, keeping only a short prefix/suffix.
        let code = "9f500bdec2f2dcf06e50f255d6d8291603e9b10f5abf500a5de5ad6d2069837d";
        let masked = mask_code(code);
        assert_eq!(masked, "9f50****837d");
        assert!(
            !masked.contains("bdec2f2"),
            "the interior of the adoption code must not be shown: {masked}"
        );
    }

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
}
