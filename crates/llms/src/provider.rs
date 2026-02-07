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

    #[snafu(display("Network error connecting to {provider}: {message}"))]
    NetworkError { provider: String, message: String },

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
}
