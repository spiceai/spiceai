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
//!    spice connect SPICE-ADOPT-7K2P-9XYZ-A1B2
//!    ```
//!
//!    or one of the explicit subcommands `status`/`forget`.
//!
//! 2. **Legacy pod-add behavior** (kept for back-compat): when the
//!    argument is a Spicepod path on Spice.ai Cloud (e.g.
//!    `spiceai/quickstart`), this behaves like `spice add <pod>` with
//!    Spice.ai Cloud authentication headers.

use std::path::PathBuf;

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
  spice connect SPICE-ADOPT-XXXX-XXXX     Stage an adoption code so the next
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
  spice connect SPICE-ADOPT-7K2P-9XYZ-A1B2
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
    pub command: Option<ConnectCommand>,

    /// First positional argument: either a Spice Cloud adoption code
    /// (`SPICE-ADOPT-...`) or a Spicepod path (`<org>/<pod>`).
    #[arg(value_name = "TARGET")]
    pub target: Option<String>,

    /// Override the Spice Cloud Connect endpoint. Defaults to
    /// `https://cloud.spice.ai`. Also configurable via
    /// `SPICE_CLOUD_ENDPOINT`.
    #[arg(long, value_name = "URL")]
    pub endpoint: Option<String>,
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
        return stage_adoption_code(target, args.endpoint.as_deref());
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

fn stage_adoption_code(code: &str, endpoint: Option<&str>) -> Result<()> {
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
    if let Some(ep) = endpoint {
        if let Err(e) = atomic_write_0600(&endpoint_path, ep.as_bytes()) {
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
        println!(
            "Adoption code stored at {}.\nStart `spiced` (or restart if already running) to begin adoption.",
            pending_path.display()
        );
        println!("Endpoint override stored at {}.", endpoint_path.display());
    } else {
        println!(
            "Adoption code stored at {}.\nStart `spiced` (or restart if already running) to begin adoption.",
            pending_path.display()
        );
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
            "  start `spiced` to send the code to {} and finish adoption.",
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
    if had_pending && let Err(e) = std::fs::remove_file(&pending_path) {
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

fn mask_code(code: &str) -> String {
    let mut parts = code.split('-').collect::<Vec<_>>();
    let len = parts.len();
    if len < 3 {
        return code.to_string();
    }
    for part in parts.iter_mut().take(len - 1).skip(2) {
        *part = "****";
    }
    parts.join("-")
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
}
