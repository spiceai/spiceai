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

//! Deprecated `spice connect <org>/<pod>` compatibility surface.
//!
//! Cloud instance enrollment and lifecycle commands live under `spice cloud`.

pub(crate) mod project;
pub(crate) mod service;
mod state;
pub(crate) mod status;
pub(crate) mod transaction;

use std::path::{Path, PathBuf};

use crate::commands::add::{AddArgs, execute_add_or_connect};
use crate::context::RuntimeContext;
use crate::error::{Error, Result};
use clap::Args;
use secrecy::{ExposeSecret as _, SecretString};

/// The Cloud Connect state directory inside an instance's config directory,
/// once any state an earlier version kept in the config directory has moved
/// into it and the directory has passed the state-directory rule.
///
/// Nothing moves while a runtime holds the instance directory: an older one
/// still uses the old location, and a current one moved its state when it
/// started.
///
/// # Errors
///
/// Returns an error when the state directory, or a config directory that
/// still holds state to move, cannot be trusted or used.
pub(crate) async fn local_state_dir(config_dir: &Path) -> Result<PathBuf> {
    runtime_cloud_connect::state_dir::migrate_legacy_state_when_idle(config_dir)
        .await
        .map_err(|source| Error::CloudConnectIo {
            message: source.to_string(),
        })?;
    let state_dir = runtime_cloud_connect::state_dir::state_dir(config_dir);
    let checked = state_dir.clone();
    tokio::task::spawn_blocking(move || runtime_cloud_connect::state_dir::check(&checked, false))
        .await
        .map_err(|source| Error::CloudConnectIo {
            message: format!("state directory check task panicked: {source}"),
        })?
        .map_err(|source| Error::CloudConnectIo {
            message: source.to_string(),
        })?;
    Ok(state_dir)
}

/// Arguments for the deprecated `spice connect <org>/<pod>` command.
#[derive(Args, Debug)]
#[command(
    about = "Deprecated alias for adding a Spicepod dependency",
    long_about = r#"`spice connect <org>/<pod>` is deprecated.

Use `spice add <org>/<pod>` to add a Spicepod dependency. Use `spice cloud link`
to enroll and attach this directory, `spice cloud status` and `spice cloud logs`
to inspect it, `spice cloud service` for lifecycle commands, and
`spice cloud unlink` to detach it.

EXAMPLE
  spice connect spiceai/quickstart

Docs: https://spiceai.org/docs"#
)]
pub struct ConnectArgs {
    /// Spicepod path in `<org>/<pod>` form.
    #[arg(value_name = "ORG/POD")]
    pub target: Option<ConnectTarget>,
}

/// A positional value may contain credential-like input, so derived CLI
/// diagnostics always redact it.
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

fn is_deprecated_spicepod_target(target: &str) -> bool {
    let Some((org, pod)) = target.split_once('/') else {
        return false;
    };
    !org.is_empty() && !pod.is_empty() && !pod.contains('/')
}

/// Execute the deprecated pod-add compatibility path.
///
/// # Errors
///
/// Returns an error for every removed lifecycle spelling, credential-shaped
/// input, or an invalid Spicepod path.
pub async fn execute(ctx: &RuntimeContext, args: ConnectArgs) -> Result<()> {
    let Some(target) = args.target.as_ref().map(ConnectTarget::expose) else {
        return Err(Error::InvalidUsage {
            message: "`spice connect` only retains the deprecated `<org>/<pod>` form. Use `spice cloud link`, `spice cloud status`, `spice cloud service`, or `spice cloud unlink` for Cloud instance lifecycle.".to_string(),
        });
    };

    if runtime_cloud_connect::enrollment_key::looks_like_enrollment_key(target) {
        return Err(Error::InvalidUsage {
            message: "An enrollment key is not accepted as a positional argument. Start `spiced --token <enrollment-key>` from the instance directory. See: https://spiceai.org/docs".to_string(),
        });
    }
    if !is_deprecated_spicepod_target(target) {
        return Err(Error::InvalidUsage {
            message: "`spice connect` only accepts the deprecated `<org>/<pod>` Spicepod form; use `spice cloud` for instance lifecycle.".to_string(),
        });
    }

    eprintln!(
        "warning: `spice connect <org>/<pod>` is deprecated and will be removed in a future release; use `spice add {target}` instead."
    );
    execute_add_or_connect(
        ctx,
        AddArgs {
            pod_path: target.to_string(),
        },
        true,
    )
    .await
}
