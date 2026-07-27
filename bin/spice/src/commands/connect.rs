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

//! `spice connect` — Spice Cloud Connect adoption flow.
//!
//! Two distinct use cases share this command:
//!
//! 1. **Cloud Connect adoption** (remote management of `spiced` from
//!    Spice Cloud). The user passes an adoption code obtained in the
//!    Spice Cloud portal:
//!
//!    ```text
//!    spice connect SPICE-ADOPT-7K2PX-9XYZ2-A1B2C-D3E4F
//!    ```
//!
//!    or one of the explicit subcommands `status`/`forget`.
//!
//! 2. **Legacy pod-add behavior** (kept for back-compat): when the
//!    argument is a Spicepod path on Spice.ai Cloud (e.g.
//!    `spiceai/quickstart`), this behaves like `spice add <pod>` with
//!    Spice.ai Cloud authentication headers.

use std::path::PathBuf;
use std::process::Stdio;

use crate::commands::add::{AddArgs, execute_add_or_connect};
use crate::context::RuntimeContext;
use crate::error::Result;
use clap::{Args, Subcommand};

/// Arguments for the `spice connect` command.
#[derive(Args, Debug)]
#[command(
    about = "Connect this instance to Spice Cloud (or add a cloud-hosted Spicepod)",
    long_about = r#"`spice connect` has two modes:

CLOUD CONNECT ADOPTION:
  spice connect SPICE-ADOPT-XXXXX-XXXXX-XXXXX-XXXXX   Stage an adoption code so the next
                                          `spiced` start connects to Spice Cloud
                                          and is shown as "Pending Adoption" in
                                          the portal.
  spice connect status                    Show the current adoption state.
  spice connect forget                    Clear the local identity on disk.
                                          A running `spiced` keeps its
                                          in-memory identity until it is
                                          restarted or the cloud sends a Forget
                                          command (a mere stream drop just
                                          reconnects with the same identity),
                                          so restart spiced to stop remote
                                          management immediately.

LEGACY POD-ADD BEHAVIOR:
  spice connect <org>/<pod>               Equivalent to `spice add <org>/<pod>`
                                          but attaches Spice.ai Cloud auth
                                          headers so private Spicepods can be
                                          fetched.

EXAMPLES
  spice connect SPICE-ADOPT-7K2PX-9XYZ2-A1B2C-D3E4F
  spice connect status
  spice connect forget
  spice connect spiceai/quickstart

Docs: https://spiceai.org/docs"#
)]
pub struct ConnectArgs {
    /// Optional explicit subcommand. If absent, the first positional
    /// argument (`target`) is inspected to decide between adoption flow
    /// and legacy pod-add behavior.
    #[command(subcommand)]
    command: Option<ConnectCommand>,

    /// First positional argument: either a Spice Cloud adoption code
    /// (`SPICE-ADOPT-...`) or a Spicepod path (`<org>/<pod>`).
    #[arg(value_name = "TARGET")]
    target: Option<String>,

    /// Override the Spice Cloud enroll endpoint the runtime presents its
    /// adoption code to. Defaults to `https://cloud.spice.ai`. Also
    /// configurable via `SPICE_CLOUD_ENDPOINT`. The gateway (stream)
    /// address is issued by the enroll response, not configured here.
    #[arg(long, value_name = "URL")]
    endpoint: Option<String>,
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
/// Returns an error if I/O fails or the legacy pod-add path errors.
pub async fn execute(ctx: &RuntimeContext, args: ConnectArgs) -> Result<()> {
    if let Some(cmd) = args.command {
        return execute_subcommand(&cmd);
    }

    let Some(target) = args.target.as_deref() else {
        // No positional argument and no subcommand — default to status
        // so that `spice connect` with no args is informative.
        return execute_subcommand(&ConnectCommand::Status);
    };

    if runtime_cloud_connect::is_valid_adoption_code(target) {
        return stage_adoption_code(ctx, target, args.endpoint.as_deref()).await;
    }

    // An input that clearly looks like an adoption code but fails validation
    // is a malformed code, not a Spicepod path. Treating it as a pod path
    // produces a misleading cloud-Spicepod error and may fire a cloud pod-add
    // request for what was plainly meant to be an adoption code, so reject it
    // explicitly instead of falling through to the legacy pod-add path.
    if looks_like_adoption_code(target) {
        return Err(crate::error::Error::InvalidArgument {
            message: format!(
                "'{target}' looks like a Spice Cloud adoption code but is malformed. \
                 Expected SPICE-ADOPT-XXXXX-XXXXX-XXXXX-XXXXX (each segment is 5 uppercase \
                 letters or digits). Copy the code from your Spice Cloud portal and retry."
            ),
        });
    }

    // Fall back to legacy pod-add behavior.
    let add_args = AddArgs {
        pod_path: target.to_string(),
    };
    execute_add_or_connect(ctx, add_args, true).await
}

fn execute_subcommand(cmd: &ConnectCommand) -> Result<()> {
    match cmd {
        ConnectCommand::Status => print_status(),
        ConnectCommand::Forget => forget_identity(),
    }
}

async fn stage_adoption_code(
    ctx: &RuntimeContext,
    code: &str,
    endpoint: Option<&str>,
) -> Result<()> {
    let pending_path =
        runtime_cloud_connect::config::CloudConnectConfig::default_pending_adopt_code_path();
    if let Some(parent) = pending_path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| crate::error::Error::CloudConnectIo {
            message: format!("create config dir {}: {e}", parent.display()),
        })?;
    }

    let endpoint_path = pending_path.parent().map_or_else(
        || PathBuf::from("cloud-endpoint"),
        |p| p.join("cloud-endpoint"),
    );

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

    println!("Attaching this Spice Runtime to Spice Cloud Connect...");

    ctx.ensure_local_runtime_supported()?;

    // Auto-install runtime if not present
    if !ctx.is_runtime_installed() {
        tracing::info!("Spice.ai runtime is not installed. Installing now...");
        crate::commands::install::execute(ctx, &crate::commands::install::InstallArgs::default())
            .await?;
    }

    // Start spiced in the foreground — inheriting stdio and forwarding
    // signals — exactly as `spice run` does, so the user sees the runtime
    // logs and adoption progress and can Ctrl-C to stop it. The staged
    // adoption code drives the connection on startup.
    let mut cmd = tokio::process::Command::from(ctx.get_run_cmd(&[], None)?);
    cmd.stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());

    let mut child = cmd
        .spawn()
        .map_err(|e| crate::error::Error::CloudConnectIo {
            message: format!("Failed to start spiced: {e}"),
        })?;

    let status = crate::commands::run::run_with_signal_forwarding(&mut child).await?;

    if !status.success() {
        std::process::exit(status.code().unwrap_or(1));
    }

    Ok(())
}

fn print_status() -> Result<()> {
    let identity_path = runtime_cloud_connect::config::CloudConnectConfig::default_identity_path();
    let pending_path =
        runtime_cloud_connect::config::CloudConnectConfig::default_pending_adopt_code_path();

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
        println!("Spice Cloud Connect: pending adoption");
        println!("  pending code at: {}", pending_path.display());
        let mask = mask_code(preview);
        println!("  code (masked):   {mask}");
        println!(
            "  start `spiced` to enroll with {} and finish adoption.",
            resolved_endpoint(&pending_path)
        );
        return Ok(());
    }

    println!("Spice Cloud Connect: not connected");
    println!("Run `spice connect <SPICE-ADOPT-...>` with a code from your Spice Cloud portal.");
    Ok(())
}

fn forget_identity() -> Result<()> {
    let identity_path = runtime_cloud_connect::config::CloudConnectConfig::default_identity_path();
    let pending_path =
        runtime_cloud_connect::config::CloudConnectConfig::default_pending_adopt_code_path();
    let endpoint_path = pending_path.parent().map_or_else(
        || PathBuf::from("cloud-endpoint"),
        |p| p.join("cloud-endpoint"),
    );

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
