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

//! Bootstrap resolution for secret-store `params:`.
//!
//! Secret stores are configured via the `params:` field on a Spicepod
//! `secrets:` entry. Each store's accepted parameters are declared as a
//! `#[derive(TypedParams)]` struct (see `crate::stores`), which validates
//! names, defaults, and `one_of`, and — via `#[params(deny_unknown)]` — fails
//! fast on unknown keys (silently dropping a misspelled `regoin` is exactly the
//! failure mode this design prevents).
//!
//! This module holds the two pieces of that pipeline that run *before* the
//! typed parse:
//!
//! - [`expand_bootstrap_refs`] resolves `${ env:KEY }` / `${ secrets:KEY }`
//!   references inside the params. Secret-store params are **not** resolved
//!   against the full [`crate::Secrets`] registry: secret stores must be
//!   initialized before any `${ store:KEY }` resolution is possible, so that
//!   would be a chicken-and-egg cycle. Only the bootstrap `env` store is
//!   available at this point.
//! - `non_empty` / `non_empty_secret` / `non_empty_path` normalize blank
//!   values to `None`, used by the stores' `into_config` conversions. Each is
//!   compiled only for the store features that call it, so a build without
//!   those stores does not carry them as dead code.

use std::collections::HashMap;
#[cfg(feature = "hashicorp_vault")]
use std::path::PathBuf;

use secrecy::ExposeSecret;
#[cfg(any(feature = "azure-keyvault", feature = "hashicorp_vault"))]
use secrecy::SecretString;
use snafu::prelude::*;

use crate::SecretStore;
use crate::lexer::SecretReplacementMatcher;

/// Trims a param value and maps an empty result to `None`.
///
/// Secret stores treat a defined-but-blank param (e.g. `client_secret: ""`, or
/// a whitespace-only value from a templated config) as *absent* — otherwise an
/// empty string would slip past validation and surface later as an opaque SDK
/// error instead of a clean "missing parameter".
#[cfg(any(feature = "azure-keyvault", feature = "hashicorp_vault"))]
#[must_use]
pub(crate) fn non_empty(value: Option<String>) -> Option<String> {
    let trimmed = value.map(|v| v.trim().to_string())?;
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

/// [`non_empty`] for secret values: trims and drops blanks without leaving the
/// trimmed bytes in a non-zeroizing intermediate `String`.
#[cfg(any(feature = "azure-keyvault", feature = "hashicorp_vault"))]
#[must_use]
pub(crate) fn non_empty_secret(value: Option<SecretString>) -> Option<SecretString> {
    let value = value?;
    let trimmed = value.expose_secret().trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(SecretString::from(trimmed.to_string()))
    }
}

/// [`non_empty`] for filesystem-path values.
#[cfg(feature = "hashicorp_vault")]
#[must_use]
pub(crate) fn non_empty_path(value: Option<PathBuf>) -> Option<PathBuf> {
    let value = value?;
    let trimmed = value.to_string_lossy();
    let trimmed = trimmed.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(PathBuf::from(trimmed))
    }
}

#[derive(Debug, Snafu)]
pub enum ParamError {
    #[snafu(display(
        "Secret-store params for '{store}' may only reference `env` or `secrets` (which resolves only env at bootstrap) (got `${{ {actual}:{key} }}` in '{param}'). Other stores are not yet initialized at this point."
    ))]
    UnsupportedBootstrapStore {
        store: String,
        param: String,
        actual: String,
        key: String,
    },

    #[snafu(display(
        "Environment variable '{key}' referenced by '{param}' on secret store '{store}' is not set."
    ))]
    MissingEnvVar {
        store: String,
        param: String,
        key: String,
    },

    #[snafu(display(
        "Failed to read environment variable '{key}' referenced by '{param}' on secret store '{store}': {source}"
    ))]
    EnvLookupFailed {
        store: String,
        param: String,
        key: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// Expands `${ env:KEY }` and `${ secrets:KEY }` references inside
/// secret-store params using the supplied bootstrap [`SecretStore`]
/// (typically a default [`crate::stores::env::EnvSecretStore`]).
///
/// Why this is restricted to env at bootstrap
/// ------------------------------------------
/// Secret-store params are resolved *while* the runtime is still building
/// the secret-store registry. The only store guaranteed to be available at
/// that point is `env` (which the runtime always loads as a default).
/// Allowing references to other stores here would be a bootstrap cycle:
/// e.g. an `aws_secrets_manager` store would need an `aws_secrets_manager`
/// store to be already loaded in order to resolve its own `region`.
///
/// `secrets:KEY` is accepted as well (matching the syntax used elsewhere in
/// the spicepod for cross-store lookup); during bootstrap it resolves
/// against env-only because env is the only store loaded.
///
/// Missing env vars produce a hard error rather than expanding to an empty
/// string — silent expansion is exactly the failure mode this whole feature
/// is meant to fix.
///
/// # Errors
///
/// Returns [`ParamError::UnsupportedBootstrapStore`] when a param references
/// a store other than `env` / `secrets`, [`ParamError::MissingEnvVar`] when a
/// referenced env var is unset, or [`ParamError::EnvLookupFailed`] when the
/// env-store lookup itself fails.
pub async fn expand_bootstrap_refs<S: ::std::hash::BuildHasher>(
    store: &str,
    params: &mut HashMap<String, String, S>,
    env_store: &dyn SecretStore,
) -> Result<(), ParamError> {
    for (param_name, value) in params.iter_mut() {
        let Some(expanded) = expand_one(store, param_name, value, env_store).await? else {
            continue;
        };
        *value = expanded;
    }
    Ok(())
}

async fn expand_one(
    store: &str,
    param_name: &str,
    value: &str,
    env_store: &dyn SecretStore,
) -> Result<Option<String>, ParamError> {
    let mut out = String::new();
    let mut last_end = 0;
    let mut any = false;

    for r in SecretReplacementMatcher::new(value) {
        any = true;

        // Only `env` and `secrets` are valid here — see the function
        // doc-comment for the bootstrap-cycle rationale.
        if r.store_name != "env" && r.store_name != crate::SECRETS {
            return Err(ParamError::UnsupportedBootstrapStore {
                store: store.to_string(),
                param: param_name.to_string(),
                actual: r.store_name.clone(),
                key: r.key.clone(),
            });
        }

        out.push_str(&value[last_end..r.span.start]);

        let resolved = env_store
            .get_secret(&r.key)
            .await
            .map_err(|source| ParamError::EnvLookupFailed {
                store: store.to_string(),
                param: param_name.to_string(),
                key: r.key.clone(),
                source,
            })?
            .ok_or_else(|| ParamError::MissingEnvVar {
                store: store.to_string(),
                param: param_name.to_string(),
                key: r.key.clone(),
            })?;

        out.push_str(resolved.expose_secret());
        last_end = r.span.end;
    }

    if !any {
        return Ok(None);
    }

    out.push_str(&value[last_end..]);
    Ok(Some(out))
}
