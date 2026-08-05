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

//! Typed spicepod component parameters.
//!
//! Support runtime for [`runtime_parameters_derive::TypedParams`]: components
//! declare a plain Rust struct describing their parameters and derive
//! `TypedParams` to deserialize the secret-injected string map produced by
//! `runtime_secrets::get_params_with_secrets` into concrete Rust types,
//! preserving the runtime's parameter semantics — per-variant key prefixing,
//! unknown-key warnings with typo suggestions, deprecation warnings, string
//! defaults, and secret autoload from configured secret stores.
//!
//! Compared to `ParameterSpec` lists, the struct is the single source of
//! truth: `required` is a non-`Option` field, `default` is an attribute,
//! `one_of` is a real enum implementing [`std::str::FromStr`], descriptions
//! are doc comments, and deprecation uses `#[deprecated]`.
//!
//! # Layering
//!
//! This crate is a foundation leaf with no dependents above it in the graph,
//! so any crate — regardless of its own tier — can derive `TypedParams`
//! without risking a cycle. Secret autoload is abstracted behind the
//! [`SecretAutoload`] trait rather than naming a concrete secrets registry, so
//! that a lower-tier crate owning its own resolver can implement the trait and
//! build on the typed-params machinery directly. `runtime-secrets` is simply
//! the first crate that needed this — the pattern generalizes to any
//! component's parameters, wherever in the graph it lives.

use std::collections::HashMap;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;

use secrecy::{ExposeSecret, SecretString};
use snafu::Snafu;
use tokio::sync::RwLock;

/// Types re-exported for use by `#[derive(TypedParams)]`-generated code only;
/// not part of the public API.
#[doc(hidden)]
pub mod __private {
    pub use secrecy::SecretString;
    pub use std::collections::HashMap;
    pub use std::sync::Arc;
    pub use tokio::sync::RwLock;
}

pub type Result<T, E = ParamsError> = std::result::Result<T, E>;

/// Errors produced while deserializing component parameters into a typed
/// struct. Messages carry the user-facing (prefixed) parameter key; callers
/// wrap them with the component name and connector.
#[derive(Debug, Snafu)]
pub enum ParamsError {
    #[snafu(display("Missing required parameter: {user_key}.{hint}"))]
    MissingRequired { user_key: String, hint: String },

    #[snafu(display("Invalid value for parameter '{user_key}': {reason}"))]
    InvalidValue { user_key: String, reason: String },

    #[snafu(display("Unknown parameter '{user_key}'. Supported parameters: {supported}."))]
    UnknownParameter { user_key: String, supported: String },
}

/// A secret-store resolver consulted for `#[param(autoload_secret)]` fields.
///
/// Implemented by `runtime_secrets::Secrets` (the real registry). Abstracting
/// the lookup keeps this crate a foundation leaf: the typed-params machinery
/// never names the `Secrets` type directly, so `runtime-secrets` can depend on
/// this crate without a cycle. Used only as a generic bound (never as `dyn`),
/// so native async-fn-in-trait is fine here.
///
/// Requires `Clone` so [`autoload_secret`] can take a point-in-time snapshot
/// before the (potentially network-bound) lookup, rather than holding the
/// registry's read guard across the `.await` — see `Secrets::snapshot`.
pub trait SecretAutoload: Send + Sync + Clone {
    /// Looks up an absent parameter by its (already-prefixed) key. Misses and
    /// store errors both resolve to `None` — autoload is best-effort.
    fn autoload_get(&self, key: &str) -> impl Future<Output = Option<SecretString>> + Send;
}

/// A no-op [`SecretAutoload`] for call sites that never autoload (e.g. secret
/// stores, which must be initialized *before* any secret resolution is
/// possible). Passing this makes the absence of autoload explicit and costs
/// nothing.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoSecretResolver;

impl SecretAutoload for NoSecretResolver {
    async fn autoload_get(&self, _key: &str) -> Option<SecretString> {
        None
    }
}

/// A component parameter struct deserializable from spicepod `params`.
///
/// Implemented via `#[derive(TypedParams)]`; see `runtime-parameters-derive`
/// for the attribute grammar. Not dyn-compatible (associated const + RPITIT):
/// use it as a generic bound, never as `dyn TypedParams`.
pub trait TypedParams: Sized {
    /// The component-variant prefix applied to component-scoped keys
    /// (e.g. `"openai"` → `openai_api_key`).
    const PREFIX: &'static str;

    /// Deserializes the secret-injected parameter map (the output of
    /// `runtime_secrets::get_params_with_secrets`) into `Self`.
    ///
    /// `component_name` is the user-facing resource label used in warnings
    /// (e.g. `"embedding my_embed"`). `secrets` is consulted only for fields
    /// marked `#[param(autoload_secret)]` that are absent from the map.
    fn try_from_params<R: SecretAutoload>(
        component_name: &str,
        params: HashMap<String, SecretString>,
        secrets: &Arc<RwLock<R>>,
    ) -> impl Future<Output = Result<Self>> + Send;
}

/// Parses a single parameter value via [`FromStr`], wrapping failures with the
/// user-facing (prefixed) key.
///
/// # Errors
///
/// Returns [`ParamsError::InvalidValue`] when parsing fails.
pub fn parse_param<T: FromStr>(user_key: &str, value: &SecretString) -> Result<T>
where
    T::Err: Display,
{
    value
        .expose_secret()
        .parse()
        .map_err(|e: T::Err| ParamsError::InvalidValue {
            user_key: user_key.to_string(),
            reason: e.to_string(),
        })
}

/// [`parse_param`] with a custom parser (`#[param(parse_with = ...)]`).
///
/// # Errors
///
/// Returns [`ParamsError::InvalidValue`] when the parser fails.
pub fn parse_param_with<T, E: Display>(
    user_key: &str,
    value: &SecretString,
    parser: impl FnOnce(&str) -> std::result::Result<T, E>,
) -> Result<T> {
    parser(value.expose_secret()).map_err(|e| ParamsError::InvalidValue {
        user_key: user_key.to_string(),
        reason: e.to_string(),
    })
}

/// Looks up an absent `#[param(autoload_secret)]` parameter in the configured
/// secret stores by its user-facing (prefixed) key. Store errors and misses
/// resolve to `None`.
///
/// Takes a point-in-time clone of `*secrets` under a brief read lock and
/// performs the lookup against it, rather than holding the read guard across
/// the (potentially network-bound) lookup — awaiting under the guard would
/// stall any writer that swaps the registry.
pub async fn autoload_secret<R: SecretAutoload>(
    secrets: &Arc<RwLock<R>>,
    component_name: &str,
    lookup_key: &str,
) -> Option<SecretString> {
    tracing::debug!("Attempting to autoload secret for {component_name}: {lookup_key}");
    let snapshot = secrets.read().await.clone();
    let secret = snapshot.autoload_get(lookup_key).await;
    if secret.is_some() {
        tracing::debug!("Autoloading secret for {component_name}: {lookup_key}");
    }
    secret
}

/// Emits the deprecation warning for a present deprecated parameter.
pub fn warn_deprecated(component_name: &str, user_key: &str, note: Option<&str>) {
    if let Some(note) = note {
        tracing::warn!("Parameter '{user_key}' is deprecated for {component_name}: {note}");
    } else {
        tracing::warn!("Parameter '{user_key}' is deprecated for {component_name}.");
    }
}

/// Warns about parameters left over after every declared field consumed its
/// keys: a wrong-prefix hint when adding/removing the prefix would match a
/// declared key, otherwise an unknown-parameter warning with a typo
/// suggestion.
pub fn warn_leftover_keys<S: std::hash::BuildHasher>(
    component_name: &str,
    leftover: &HashMap<String, SecretString, S>,
    known_user_keys: &[&str],
    prefix: &str,
) {
    let full_prefix = format!("{prefix}_");
    for key in leftover.keys() {
        // The user prefixed a key that is declared unprefixed, or vice versa.
        let other_form = key.strip_prefix(&full_prefix).map_or_else(
            || format!("{full_prefix}{key}"),
            std::string::ToString::to_string,
        );
        if known_user_keys.contains(&other_form.as_str()) {
            if key.starts_with(&full_prefix) {
                tracing::warn!(
                    "Ignoring parameter {key}: must not be prefixed with `{full_prefix}` for {component_name}."
                );
            } else {
                tracing::warn!(
                    "Ignoring parameter {key}: must be prefixed with `{full_prefix}` for {component_name}."
                );
            }
            continue;
        }

        if let Some(candidate) = util::levenshtein::closest_match(key, known_user_keys) {
            tracing::warn!(
                "Ignoring parameter `{key}`: not supported for {component_name}. Did you mean `{candidate}`?"
            );
        } else {
            tracing::warn!("Ignoring parameter `{key}`: not supported for {component_name}.");
        }
    }
}

/// Fail-fast counterpart of [`warn_leftover_keys`] for `#[params(deny_unknown)]`:
/// returns [`ParamsError::UnknownParameter`] for the first leftover key rather
/// than logging a warning. Silently dropping a misspelled parameter is the
/// exact failure mode fail-fast validation exists to prevent.
///
/// # Errors
///
/// Returns [`ParamsError::UnknownParameter`] naming the offending key and the
/// supported keys when any key remains unconsumed.
pub fn deny_leftover_keys<S: std::hash::BuildHasher>(
    leftover: &HashMap<String, SecretString, S>,
    known_user_keys: &[&str],
) -> Result<()> {
    // Iterate the known keys (deterministic order) to build the supported list;
    // pick any leftover key to name in the error.
    let Some(user_key) = leftover.keys().next() else {
        return Ok(());
    };
    let mut supported: Vec<&str> = known_user_keys.to_vec();
    supported.sort_unstable();
    let supported = if supported.is_empty() {
        "<none>".to_string()
    } else {
        supported.join(", ")
    };
    Err(ParamsError::UnknownParameter {
        user_key: user_key.clone(),
        supported,
    })
}
