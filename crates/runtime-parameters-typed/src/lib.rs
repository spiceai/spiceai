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

/// A parameter that a [`TypedParams`] struct accepts but does not bind to a
/// field: it is consumed (so it never trips [`warn_leftover_keys`]), folded
/// into the typo-suggestion candidate set, and — when `deprecated` — warned on
/// if present. Used via `#[params(passthrough = <PATH>)]` for large shared
/// parameter groups (e.g. the OpenAI-compatible chat-completion overrides all
/// model providers accept) whose values are read elsewhere from the raw params
/// map rather than off the typed struct.
#[derive(Debug, Clone, Copy)]
pub struct PassthroughParam {
    /// The unprefixed parameter name.
    pub name: &'static str,
    /// `true` when the key is component-scoped (prefixed with the container
    /// prefix, like `ParameterSpec::component`); `false` for runtime
    /// (unprefixed) keys.
    pub prefixed: bool,
    /// `Some(note)` when this key is deprecated; a warning fires when present.
    pub deprecated: Option<&'static str>,
    /// Description surfaced by schema generation (empty when undocumented).
    pub description: &'static str,
}

impl PassthroughParam {
    /// A runtime (unprefixed) passthrough key.
    #[must_use]
    pub const fn runtime(name: &'static str) -> Self {
        Self {
            name,
            prefixed: false,
            deprecated: None,
            description: "",
        }
    }

    /// A component (prefixed) passthrough key.
    #[must_use]
    pub const fn component(name: &'static str) -> Self {
        Self {
            name,
            prefixed: true,
            deprecated: None,
            description: "",
        }
    }

    /// Marks this passthrough key deprecated with `note`.
    #[must_use]
    pub const fn deprecated(mut self, note: &'static str) -> Self {
        self.deprecated = Some(note);
        self
    }

    /// Attaches a schema description.
    #[must_use]
    pub const fn description(mut self, description: &'static str) -> Self {
        self.description = description;
        self
    }

    /// The user-facing key for this passthrough param under `prefix`, mirroring
    /// the derive's prefixing rules: runtime keys are bare; component keys are
    /// `{prefix}_{name}` unless `name` already carries the prefix.
    #[must_use]
    pub fn user_key(&self, prefix: &str) -> String {
        if self.prefixed && !self.name.starts_with(&format!("{prefix}_")) {
            format!("{prefix}_{}", self.name)
        } else {
            self.name.to_string()
        }
    }
}

/// Consumes every `#[params(passthrough = ...)]` key from `params` so it never
/// trips [`warn_leftover_keys`], emits a deprecation warning for each present
/// deprecated key, and returns the user-facing keys so callers can fold them
/// into the typo-suggestion candidate set. Mirrors the deprecation/known-key
/// handling of `Parameters::try_new`.
pub fn consume_passthrough<S: std::hash::BuildHasher>(
    component_name: &str,
    params: &mut HashMap<String, SecretString, S>,
    prefix: &str,
    table: &[PassthroughParam],
) -> Vec<String> {
    let mut keys = Vec::with_capacity(table.len());
    for param in table {
        let user_key = param.user_key(prefix);
        if params.remove(&user_key).is_some()
            && let Some(note) = param.deprecated
        {
            warn_deprecated(component_name, &user_key, Some(note));
        }
        keys.push(user_key);
    }
    keys
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
