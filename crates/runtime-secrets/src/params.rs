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

//! Parameter validation for secret-store configuration.
//!
//! Secret stores are configured via the `params:` field on a Spicepod
//! `secrets:` entry. They reuse [`runtime_parameter_spec::ParameterSpec`]
//! (the same type used by data connectors and accelerators) to declare
//! their accepted parameters, and run a lightweight validator at load time.
//!
//! Differences from `runtime_parameters::Parameters::try_new`:
//!
//! - Secret-store params are **not** themselves resolved against a
//!   [`crate::Secrets`] registry. Secret stores must be initialized before
//!   any `${ store:KEY }` resolution is possible, so resolving secret-store
//!   params from secrets would be a chicken-and-egg cycle. Use
//!   environment-variable expansion (handled at the spicepod-loading layer)
//!   if you need to inject runtime values.
//! - There is no `prefix`. Secret-store param names are written as-is
//!   (`region`, `namespace`, `file_path`, ...).
//! - Validation is fail-fast: unknown parameters return an error rather
//!   than logging a warning, because silently dropping (for example) a
//!   misspelled `regoin` parameter is exactly the failure mode this
//!   feature is meant to fix.

use std::collections::HashMap;

use runtime_parameter_spec::ParameterSpec;
use secrecy::ExposeSecret;
use snafu::prelude::*;

use crate::SecretStore;
use crate::lexer::SecretReplacementMatcher;

#[derive(Debug, Snafu)]
pub enum ParamError {
    #[snafu(display(
        "Unknown parameter '{param}' for secret store '{store}'. Supported parameters: {supported}."
    ))]
    UnknownParam {
        store: String,
        param: String,
        supported: String,
    },

    #[snafu(display("Missing required parameter '{param}' for secret store '{store}'."))]
    MissingRequired { store: String, param: String },

    #[snafu(display(
        "Invalid value for parameter '{param}' on secret store '{store}': must be one of [{allowed}]; got '{value}'."
    ))]
    InvalidOneOf {
        store: String,
        param: String,
        value: String,
        allowed: String,
    },

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

/// Validates a set of user-provided params against a static [`ParameterSpec`]
/// list, returning a normalized `HashMap`.
///
/// Steps performed (in order):
/// 1. Reject any user-provided key that is not present in `spec`.
/// 2. Emit a tracing warning for each provided key that is marked deprecated.
/// 3. Fill in any unset parameters that have a `default`.
/// 4. Verify all `required` parameters are now present.
/// 5. Verify any `one_of`-constrained parameters hold an allowed value.
///
/// Secret-store params are never themselves expanded against the secrets
/// registry (see module-level docs for the rationale), so this function does
/// not take a `Secrets` handle.
///
/// # Errors
///
/// Returns [`ParamError::UnknownParam`] for params not in `spec`,
/// [`ParamError::MissingRequired`] when a required param is absent, or
/// [`ParamError::InvalidOneOf`] when a `one_of`-constrained param holds
/// an unsupported value.
pub fn validate_params<S: ::std::hash::BuildHasher>(
    store: &str,
    user_params: HashMap<String, String, S>,
    spec: &'static [ParameterSpec],
) -> Result<HashMap<String, String, S>, ParamError> {
    // 1. Reject unknown keys up-front. We do not silently drop them because
    //    the most common failure for secret-store config is a misspelled
    //    parameter name, and silent drops are exactly what makes the bug
    //    invisible.
    for key in user_params.keys() {
        if !spec.iter().any(|p| p.name == key) {
            return Err(ParamError::UnknownParam {
                store: store.to_string(),
                param: key.clone(),
                supported: supported_list(spec),
            });
        }
    }

    // 2. Warn about deprecated params the user did supply.
    for p in spec {
        if let Some(msg) = p.deprecation_message
            && user_params.contains_key(p.name)
        {
            tracing::warn!(
                "Parameter '{}' is deprecated for secret store '{}': {}",
                p.name,
                store,
                msg
            );
        }
    }

    // 3. Apply defaults for any unset, defaulted params.
    let mut params = user_params;
    for p in spec {
        if let Some(default) = p.default
            && !params.contains_key(p.name)
        {
            params.insert(p.name.to_string(), default.to_string());
        }
    }

    // 4. Required-param check (after defaults, so a defaulted param can be
    //    declared `required` for documentation without breaking the load).
    for p in spec {
        if p.required && !params.contains_key(p.name) {
            return Err(ParamError::MissingRequired {
                store: store.to_string(),
                param: p.name.to_string(),
            });
        }
    }

    // 5. one_of validation.
    for p in spec {
        let Some(allowed) = p.one_of else {
            continue;
        };
        if let Some(value) = params.get(p.name)
            && !allowed.contains(&value.as_str())
        {
            return Err(ParamError::InvalidOneOf {
                store: store.to_string(),
                param: p.name.to_string(),
                value: value.clone(),
                allowed: allowed.join(", "),
            });
        }
    }

    Ok(params)
}

fn supported_list(spec: &'static [ParameterSpec]) -> String {
    let mut names: Vec<&str> = spec
        .iter()
        .filter(|p| p.deprecation_message.is_none())
        .map(|p| p.name)
        .collect();
    names.sort_unstable();
    if names.is_empty() {
        "<none>".to_string()
    } else {
        names.join(", ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_SPEC: &[ParameterSpec] = &[
        ParameterSpec::runtime("region")
            .description("AWS region")
            .examples(&["us-east-1"]),
        ParameterSpec::runtime("mode")
            .default("auto")
            .one_of(&["auto", "manual"]),
        ParameterSpec::runtime("legacy").deprecated("use `region` instead"),
    ];

    fn map(items: &[(&str, &str)]) -> HashMap<String, String> {
        items
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn rejects_unknown_param() {
        let err = validate_params("test", map(&[("regoin", "us-east-1")]), TEST_SPEC)
            .expect_err("unknown param must be rejected");
        assert!(
            matches!(err, ParamError::UnknownParam { ref param, .. } if param == "regoin"),
            "got {err:?}"
        );
    }

    #[test]
    fn applies_defaults() {
        let out = validate_params("test", HashMap::new(), TEST_SPEC).expect("validates");
        assert_eq!(out.get("mode").map(String::as_str), Some("auto"));
        assert!(!out.contains_key("region"));
    }

    #[test]
    fn enforces_one_of() {
        let err = validate_params("test", map(&[("mode", "wat")]), TEST_SPEC)
            .expect_err("invalid one_of must be rejected");
        assert!(
            matches!(err, ParamError::InvalidOneOf { ref value, .. } if value == "wat"),
            "got {err:?}"
        );
    }

    #[test]
    fn allows_known_param() {
        let out =
            validate_params("test", map(&[("region", "eu-west-2")]), TEST_SPEC).expect("validates");
        assert_eq!(out.get("region").map(String::as_str), Some("eu-west-2"));
    }

    #[test]
    fn warns_on_deprecated_but_accepts() {
        // Deprecated params remain accepted; the warning is observed via
        // `tracing` and is not asserted here.
        let out = validate_params("test", map(&[("legacy", "x")]), TEST_SPEC).expect("validates");
        assert_eq!(out.get("legacy").map(String::as_str), Some("x"));
    }
}
