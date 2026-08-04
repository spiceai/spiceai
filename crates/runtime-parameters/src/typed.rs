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
//! [`TypedParams`] to deserialize the secret-injected string map produced by
//! [`runtime_secrets::get_params_with_secrets`], preserving the semantics of
//! the [`crate::Parameters`] machinery — per-variant key prefixing,
//! unknown-key warnings with typo suggestions, deprecation warnings, string
//! defaults, and secret autoload from configured secret stores.
//!
//! Compared to [`crate::ParameterSpec`] lists, the struct is the single source
//! of truth: `required` is a non-`Option` field, `default` is an attribute,
//! `one_of` is a real enum implementing [`std::str::FromStr`], descriptions
//! are doc comments, and deprecation uses `#[deprecated]`.

use std::collections::HashMap;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;

use runtime_secrets::Secrets;
use secrecy::{ExposeSecret, SecretString};
use snafu::Snafu;
use tokio::sync::RwLock;

/// Types re-exported for use by `#[derive(TypedParams)]`-generated code only;
/// not part of the public API.
#[doc(hidden)]
pub mod __private {
    pub use runtime_secrets::Secrets;
    pub use secrecy::SecretString;
    pub use std::collections::HashMap;
    pub use std::sync::Arc;
    pub use tokio::sync::RwLock;
}

pub type Result<T, E = ParamsError> = std::result::Result<T, E>;

/// Errors produced while deserializing component parameters into a typed
/// struct. Messages carry the user-facing (prefixed) parameter key; callers
/// wrap them with the component name and connector, matching how
/// [`crate::Parameters::try_new`] errors are surfaced today.
#[derive(Debug, Snafu)]
pub enum ParamsError {
    #[snafu(display("Missing required parameter: {user_key}.{hint}"))]
    MissingRequired { user_key: String, hint: String },

    #[snafu(display("Invalid value for parameter '{user_key}': {reason}"))]
    InvalidValue { user_key: String, reason: String },
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
    /// [`runtime_secrets::get_params_with_secrets`]) into `Self`.
    ///
    /// `component_name` is the user-facing resource label used in warnings
    /// (e.g. `"embedding my_embed"`). `secrets` is consulted only for fields
    /// marked `#[param(autoload_secret)]` that are absent from the map.
    fn try_from_params(
        component_name: &str,
        params: HashMap<String, SecretString>,
        secrets: &Arc<RwLock<Secrets>>,
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

/// Looks up an absent `#[param(autoload_secret)]` parameter in the configured secret
/// stores by its user-facing (prefixed) key. Mirrors the autoload pass of
/// [`crate::Parameters::try_new`]: store errors and misses resolve to `None`.
pub async fn autoload_secret(
    secrets: &Arc<RwLock<Secrets>>,
    component_name: &str,
    lookup_key: &str,
) -> Option<SecretString> {
    tracing::debug!("Attempting to autoload secret for {component_name}: {lookup_key}");
    let secrets = Secrets::snapshot(secrets).await;
    match secrets.get_secret(lookup_key).await {
        Ok(Some(secret)) => {
            tracing::debug!("Autoloading secret for {component_name}: {lookup_key}");
            Some(secret)
        }
        _ => None,
    }
}

/// Emits the deprecation warning for a present deprecated parameter, matching
/// the wording of [`crate::Parameters::try_new`].
pub fn warn_deprecated(component_name: &str, user_key: &str, note: Option<&str>) {
    if let Some(note) = note {
        tracing::warn!("Parameter '{user_key}' is deprecated for {component_name}: {note}");
    } else {
        tracing::warn!("Parameter '{user_key}' is deprecated for {component_name}.");
    }
}

/// Warns about parameters left over after every declared field consumed its
/// keys, matching the wording of [`crate::Parameters::validate_and_format_key`]:
/// a wrong-prefix hint when adding/removing the prefix would match a declared
/// key, otherwise an unknown-parameter warning with a typo suggestion.
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
