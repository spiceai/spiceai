/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

//! LLM Provider trait and utilities for querying provider capabilities.
//!
//! This module defines a common interface for LLM providers to expose
//! metadata about available models, which helps with error messages
//! and configuration validation.

use async_openai::error::{ApiError, OpenAIError};
use async_trait::async_trait;
use reqwest::StatusCode;
use secrecy::SecretString;
use snafu::Snafu;
use std::collections::HashMap;
use std::time::Duration;

/// Timeout for model list API calls
pub const API_TIMEOUT: Duration = Duration::from_secs(10);

/// Maximum number of models to display in hints
pub const MAX_MODELS_TO_DISPLAY: usize = 10;

/// Errors that can occur when listing models from a provider.
#[derive(Debug, Snafu)]
pub enum ListModelsError {
    #[snafu(display("Rate limited by {provider}"))]
    RateLimited { provider: String },

    #[snafu(display("Quota or payment required for {provider}"))]
    QuotaExceeded { provider: String },

    #[snafu(display("Invalid credentials for {provider}"))]
    InvalidCredentials { provider: String },

    #[snafu(display("Failed to authenticate with {provider}: {message}"))]
    AuthenticationFailed { provider: String, message: String },

    #[snafu(display("Network error connecting to {provider}: {message}"))]
    NetworkError { provider: String, message: String },

    /// The provider answered and did not serve the request, for a reason this crate does not
    /// recognise. Distinct from [`ListModelsError::NetworkError`], which says no answer came back
    /// at all.
    ///
    /// It says nothing about whether retrying would help, and must not be read that way: a
    /// persistent 5xx lands here too, because `async-openai` does not parse a server-error body
    /// and hands it over carrying neither `type` nor `code`. The variant's claim is only that the
    /// provider responded and the reason was not one this crate can name.
    #[snafu(display("{provider} refused the request: {message}"))]
    ProviderRefused { provider: String, message: String },

    #[snafu(display(
        "Missing required parameter '{param}' for listing models. Verify the model configuration."
    ))]
    MissingParameter { param: String },

    #[snafu(display("Provider {provider} does not support listing models"))]
    NotSupported { provider: String },
}

pub type ListModelsResult<T> = std::result::Result<T, ListModelsError>;

/// Trait for LLM providers that can list available models.
///
/// Each provider module should implement this trait to expose
/// its model discovery capabilities.
#[async_trait]
pub trait ListModels: Send + Sync {
    /// Returns the provider name for error messages.
    fn provider_name(&self) -> &'static str;

    /// Lists available models from this provider.
    ///
    /// Returns a list of model identifiers, or an error if the
    /// provider cannot be queried (rate limits, auth issues, etc).
    async fn list_models(&self) -> ListModelsResult<Vec<String>>;

    /// Returns a formatted hint string for error messages.
    async fn get_models_hint(&self) -> Option<String> {
        match self.list_models().await {
            Ok(models) if !models.is_empty() => {
                Some(format_models_hint(&models, self.provider_name()))
            }
            Ok(_) => None,
            Err(e) => {
                tracing::debug!("Failed to list models from {}: {}", self.provider_name(), e);
                None
            }
        }
    }
}

/// Formats a list of models into a user-friendly hint string.
#[must_use]
pub fn format_models_hint(models: &[String], provider_name: &str) -> String {
    if models.is_empty() {
        return String::new();
    }

    let display_models: Vec<&str> = models
        .iter()
        .take(MAX_MODELS_TO_DISPLAY)
        .map(String::as_str)
        .collect();
    let remaining = models.len().saturating_sub(MAX_MODELS_TO_DISPLAY);

    let mut hint = format!(
        "\nAvailable {} models include: {}",
        provider_name,
        display_models.join(", ")
    );
    if remaining > 0 {
        use std::fmt::Write;
        let _ = write!(hint, " (and {remaining} more)");
    }
    hint
}

/// Creates an HTTP client with standard timeout and TLS settings.
#[must_use]
pub fn create_http_client() -> Option<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent(util::spiceai_user_agent())
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(API_TIMEOUT)
        .use_rustls_tls()
        .build()
        .ok()
}

/// Maps HTTP status codes to appropriate `ListModelsError` variants.
#[must_use]
pub fn map_status_to_error(status: StatusCode, provider: &str) -> ListModelsError {
    match status {
        StatusCode::TOO_MANY_REQUESTS => ListModelsError::RateLimited {
            provider: provider.to_string(),
        },
        StatusCode::PAYMENT_REQUIRED | StatusCode::FORBIDDEN => ListModelsError::QuotaExceeded {
            provider: provider.to_string(),
        },
        StatusCode::UNAUTHORIZED => ListModelsError::InvalidCredentials {
            provider: provider.to_string(),
        },
        _ => ListModelsError::NetworkError {
            provider: provider.to_string(),
            message: format!("HTTP {status}"),
        },
    }
}

/// Classifies a failure from an `OpenAI`-compatible endpoint reached through `async-openai`.
///
/// The listers built on that client cannot use [`map_status_to_error`]: `async-openai` discards
/// the HTTP status, mapping any unsuccessful response to
/// [`OpenAIError::ApiError`] built from the response body alone. What survives is the body's own
/// `code` and `type`, so those are what this reads.
///
/// **`code` is consulted before `type`, because `type` alone cannot separate the cases.** Measured
/// against `OpenAI`'s own error bodies: a rejected key and a missing model are both
/// `"type":"invalid_request_error"`, and only `"code":"invalid_api_key"` versus
/// `"code":"model_not_found"` tells them apart.
///
/// **The provider's rendered message is never read while either field is present.** It carries
/// caller data the provider echoes back — a model id, a project id — so a substring test over it
/// classifies on the caller's own strings: `gpt-4o-mini-2024-0401` contains `401` and
/// `proj_429` contains `429` (issue #13747).
///
/// A refusal this does not recognise becomes [`ListModelsError::ProviderRefused`] rather than a
/// guess. The `OpenAI` taxonomy below is the one the client speaks; xAI and Spice endpoints are
/// `OpenAI`-compatible and reach the same client, and anything they spell differently lands in
/// that unrecognised arm instead of a wrong variant.
///
/// **A refusal that is not JSON at all is still classified.** A Spice runtime denies an
/// unauthenticated request with a 401 whose body is the bare string `Unauthorized`, which the
/// client cannot parse into an `ApiError` — it surfaces as
/// [`OpenAIError::JSONDeserialize`]. That is the credential failure a Spice user actually hits,
/// so the message tests are applied to the unparsed body too; see the arm for why an
/// unrecognised one stays a network error there rather than becoming a refusal.
#[must_use]
pub fn classify_openai_compatible_error(error: &OpenAIError, provider: &str) -> ListModelsError {
    // The same value for both arms below: the request did not come back as a refusal this can
    // read anything out of.
    let network_error = || ListModelsError::NetworkError {
        provider: provider.to_string(),
        message: error.to_string(),
    };

    let api_error = match error {
        OpenAIError::ApiError(api_error) => api_error,
        // The provider answered with a body `async-openai` could not parse into an `ApiError`.
        // A Spice runtime's own denial is exactly this shape: `AuthLayer` answers a rejected
        // request with a 401 whose entire body is the string `Unauthorized`
        // (`crates/runtime-auth/src/layer/http.rs`), which is not the JSON envelope the client
        // expects. The same variant also carries a *successful* response whose JSON did not
        // match, and the error cannot tell the two apart — so a body with no recognisable signal
        // stays a network error rather than being reported as a refusal that may never have
        // happened.
        OpenAIError::JSONDeserialize(_, content) => {
            return signal_in(content, provider).unwrap_or_else(network_error);
        }
        // Reqwest and client-side argument errors: no answer came back at all.
        _ => return network_error(),
    };

    if let Some(kind) = classify_by_code(api_error, provider) {
        return kind;
    }

    if let Some(kind) = classify_by_type(api_error, provider) {
        return kind;
    }

    // The message tests, reached only when the body carried *neither* typed field. A body that
    // named a `code` or a `type` has already had its say — falling through to the message there
    // would let caller data the provider echoed back decide the variant after all, which is the
    // whole defect (issue #13747).
    if api_error.code.is_none()
        && api_error.r#type.is_none()
        && let Some(kind) = signal_in(&api_error.message, provider)
    {
        return kind;
    }

    ListModelsError::ProviderRefused {
        provider: provider.to_string(),
        message: api_error.to_string(),
    }
}

/// The documented `OpenAI` error codes, which are machine-readable identifiers rather than prose.
/// Returns `None` for an absent or unrecognised code so the caller can fall through to `type`.
fn classify_by_code(api_error: &ApiError, provider: &str) -> Option<ListModelsError> {
    match api_error.code.as_deref()? {
        "invalid_api_key" | "invalid_organization" => Some(ListModelsError::InvalidCredentials {
            provider: provider.to_string(),
        }),
        "insufficient_quota" | "billing_hard_limit_reached" => {
            Some(ListModelsError::QuotaExceeded {
                provider: provider.to_string(),
            })
        }
        "rate_limit_exceeded" => Some(ListModelsError::RateLimited {
            provider: provider.to_string(),
        }),
        _ => None,
    }
}

/// The documented `OpenAI` error types. `None` for an absent or unrecognised type.
fn classify_by_type(api_error: &ApiError, provider: &str) -> Option<ListModelsError> {
    match api_error.r#type.as_deref()? {
        "insufficient_quota" => Some(ListModelsError::QuotaExceeded {
            provider: provider.to_string(),
        }),
        "rate_limit_error" => Some(ListModelsError::RateLimited {
            provider: provider.to_string(),
        }),
        "authentication_error" => Some(ListModelsError::InvalidCredentials {
            provider: provider.to_string(),
        }),
        _ => None,
    }
}

/// The message tests. `None` when the text carries no signal this can name, so each caller decides
/// what an unrecognised body means for it. Kept deliberately narrow: it runs only where no typed
/// field exists, and matches whole phrases rather than bare status digits, which is what let a
/// model id (`gpt-4o-mini-2024-0401`) or a project id (`proj_429`) classify a refusal before.
fn signal_in(message: &str, provider: &str) -> Option<ListModelsError> {
    let lowered = message.to_lowercase();

    if lowered.contains("too many requests") || lowered.contains("rate limit") {
        return Some(ListModelsError::RateLimited {
            provider: provider.to_string(),
        });
    }

    if lowered.contains("unauthorized") || lowered.contains("invalid api key") {
        return Some(ListModelsError::InvalidCredentials {
            provider: provider.to_string(),
        });
    }

    None
}

/// Helper to get a required parameter from a params map.
///
/// # Errors
///
/// Returns `ListModelsError::MissingParameter` if the key is not found.
#[expect(clippy::implicit_hasher)]
pub fn get_required_param<'a>(
    params: &'a HashMap<String, SecretString>,
    key: &str,
) -> ListModelsResult<&'a SecretString> {
    params
        .get(key)
        .ok_or_else(|| ListModelsError::MissingParameter {
            param: key.to_string(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_models_hint_empty() {
        let result = format_models_hint(&[], "test");
        assert!(result.is_empty());
    }

    #[test]
    fn test_format_models_hint_few_models() {
        let models = vec!["model-1".to_string(), "model-2".to_string()];
        let result = format_models_hint(&models, "test");
        assert!(result.contains("model-1"));
        assert!(result.contains("model-2"));
        assert!(!result.contains("more"));
    }

    #[test]
    fn test_format_models_hint_many_models() {
        let models: Vec<String> = (0..15).map(|i| format!("model-{i}")).collect();
        let result = format_models_hint(&models, "test");
        assert!(result.contains("model-0"));
        assert!(result.contains("model-9"));
        assert!(result.contains("5 more"));
    }

    #[test]
    fn test_map_status_rate_limited() {
        let err = map_status_to_error(StatusCode::TOO_MANY_REQUESTS, "test");
        assert!(matches!(err, ListModelsError::RateLimited { .. }));
    }

    #[test]
    fn test_map_status_unauthorized() {
        let err = map_status_to_error(StatusCode::UNAUTHORIZED, "test");
        assert!(matches!(err, ListModelsError::InvalidCredentials { .. }));
    }

    /// Builds the `ApiError` `async-openai` hands over for a refusal carrying these fields.
    fn refusal(message: &str, r#type: Option<&str>, code: Option<&str>) -> OpenAIError {
        OpenAIError::ApiError(ApiError {
            message: message.to_string(),
            r#type: r#type.map(ToString::to_string),
            param: None,
            code: code.map(ToString::to_string),
        })
    }

    /// `type` is the same for both, so only `code` separates a rejected key from a missing model.
    /// Reading `type` first, or the message at all, collapses them (issue #13747).
    #[test]
    fn code_outranks_type_where_type_cannot_tell_them_apart() {
        let rejected_key = refusal(
            "Incorrect API key provided: sk-xxx.",
            Some("invalid_request_error"),
            Some("invalid_api_key"),
        );
        assert!(matches!(
            classify_openai_compatible_error(&rejected_key, "test"),
            ListModelsError::InvalidCredentials { .. }
        ));

        let missing_model = refusal(
            "The model `gpt-4o-mini-2024-0401` does not exist.",
            Some("invalid_request_error"),
            Some("model_not_found"),
        );
        assert!(matches!(
            classify_openai_compatible_error(&missing_model, "test"),
            ListModelsError::ProviderRefused { .. }
        ));
    }

    /// The rate-limit arm. `async-openai` retries a 429 that is not out of quota under its own
    /// backoff, so the local-server test cannot reach this — it is asserted here instead.
    #[test]
    fn a_typed_rate_limit_is_reported_as_one() {
        for error in [
            refusal("Rate limit reached.", Some("rate_limit_error"), None),
            refusal("Rate limit reached.", None, Some("rate_limit_exceeded")),
        ] {
            assert!(matches!(
                classify_openai_compatible_error(&error, "test"),
                ListModelsError::RateLimited { .. }
            ));
        }
    }

    /// A model id or project id echoed back into the message must not decide the variant while a
    /// typed field is present — neither the false positive nor, below, the false negative.
    #[test]
    fn caller_data_in_the_message_does_not_decide_a_typed_refusal() {
        let ids = refusal(
            "Project `proj_429` cannot use model `gpt-4o-mini-2024-0401`; quota is unaffected.",
            Some("invalid_request_error"),
            Some("model_not_found"),
        );
        let classified = classify_openai_compatible_error(&ids, "test");
        assert!(
            matches!(classified, ListModelsError::ProviderRefused { .. }),
            "caller data decided the variant: {classified}"
        );
    }

    /// A `code` this crate does not recognise still counts as the body having had its say: the
    /// message must not decide the variant behind it, even with `type` absent.
    #[test]
    fn an_unrecognised_code_does_not_fall_through_to_the_message() {
        let classified = classify_openai_compatible_error(
            &refusal(
                "Unauthorized project `proj_429`.",
                None,
                Some("model_not_found"),
            ),
            "test",
        );
        assert!(
            matches!(classified, ListModelsError::ProviderRefused { .. }),
            "an unrecognised code fell through to the message tests: {classified}"
        );
    }

    /// A Spice runtime's `AuthLayer` denies with a 401 whose body is the bare string
    /// `Unauthorized`, so the client cannot build an `ApiError` at all. That is a credential
    /// failure and must read as one.
    #[test]
    fn an_unparsable_denial_body_is_still_classified() {
        let error = OpenAIError::JSONDeserialize(
            serde_json::from_str::<serde_json::Value>("Unauthorized").expect_err("not json"),
            "Unauthorized".to_string(),
        );
        assert!(matches!(
            classify_openai_compatible_error(&error, "test"),
            ListModelsError::InvalidCredentials { .. }
        ));
    }

    /// The same variant also carries a *successful* response whose JSON did not match. With no
    /// signal in the body there is no way to tell that from a refusal, so it stays a network
    /// error rather than claiming a refusal that may never have happened.
    #[test]
    fn an_unparsable_body_with_no_signal_stays_a_network_error() {
        let error = OpenAIError::JSONDeserialize(
            serde_json::from_str::<serde_json::Value>("<html>").expect_err("not json"),
            "<html>oops</html>".to_string(),
        );
        assert!(matches!(
            classify_openai_compatible_error(&error, "test"),
            ListModelsError::NetworkError { .. }
        ));
    }

    /// `async-openai` builds an all-`None` `ApiError` for a 5xx, whose body it does not parse. The
    /// message is then the only signal there is, and it carries no caller data.
    #[test]
    fn an_untyped_refusal_falls_back_to_the_message() {
        assert!(matches!(
            classify_openai_compatible_error(&refusal("429 Too Many Requests", None, None), "test"),
            ListModelsError::RateLimited { .. }
        ));
        assert!(matches!(
            classify_openai_compatible_error(&refusal("401 Unauthorized", None, None), "test"),
            ListModelsError::InvalidCredentials { .. }
        ));
        assert!(matches!(
            classify_openai_compatible_error(&refusal("502 Bad Gateway", None, None), "test"),
            ListModelsError::ProviderRefused { .. }
        ));
    }

    /// A failure that is not a provider refusal at all stays a network error, so a caller can
    /// still tell "no answer came back" from "the provider answered and said no".
    #[test]
    fn a_non_refusal_stays_a_network_error() {
        let error = OpenAIError::InvalidArgument("bad request builder".to_string());
        assert!(matches!(
            classify_openai_compatible_error(&error, "test"),
            ListModelsError::NetworkError { .. }
        ));
    }
}
