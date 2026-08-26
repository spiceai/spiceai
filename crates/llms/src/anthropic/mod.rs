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
#![allow(clippy::missing_errors_doc)]
use async_openai::{Client, error::OpenAIError};
use reqwest::header::HeaderValue;
use types::validate_model_variant;

mod chat;
mod list_models;
mod types;
mod types_stream;

pub use list_models::AnthropicModelLister;
pub use types::AnthropicModelVariant;

use crate::config::{GenericAuthMechanism, HostedModelConfig};

pub struct Anthropic {
    client: Client<HostedModelConfig>,
    model: AnthropicModelVariant,
    /// Whether [`Anthropic::model`] came from [`DEFAULT_ANTHROPIC_MODEL`] because the
    /// configuration named no model. Anthropic answers a request for a model it no longer serves
    /// with a `not_found_error` whose entire message is `model: <id>`, which is unactionable for
    /// an id the user never chose — see [`explain_model_not_found`].
    model_from_default: bool,
}

static ANTHROPIC_API_BASE: &str = "https://api.anthropic.com/v1";
/// The model used when no model id is configured. Anthropic retires model ids, so this must name
/// a model Anthropic still serves: a retired default fails every request from a configuration
/// that names no model.
pub static DEFAULT_ANTHROPIC_MODEL: &str = "claude-sonnet-5";
static ANTHROPIC_API_VERSION: &str = "2023-06-01";

impl Anthropic {
    pub fn new(
        auth: GenericAuthMechanism,
        model: Option<&str>,
        api_base: Option<&str>,
        version: Option<&str>,
    ) -> Result<Self, OpenAIError> {
        let value = HeaderValue::from_str(version.unwrap_or(ANTHROPIC_API_VERSION))
            .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?;
        let variant = validate_model_variant(model.unwrap_or(DEFAULT_ANTHROPIC_MODEL))?;
        let model_from_default = model.is_none();
        let cfg = HostedModelConfig::from_url(api_base.unwrap_or(ANTHROPIC_API_BASE))
            .with_auth(auth)
            .with_header_value("anthropic-version", value)
            .with_header_value(
                "anthropic-beta",
                HeaderValue::from_static("structured-outputs-2025-11-13"),
            );

        Ok(Self {
            client: Client::<HostedModelConfig>::with_config(cfg),
            model: variant,
            model_from_default,
        })
    }
}

const ANTHROPIC_DOCS: &str = "https://spiceai.org/docs/components/models/anthropic";

/// Replaces the message of an Anthropic `not_found_error` with one that names the model and the
/// parameter to change. Anthropic sends `model: <id>` and nothing else, so the raw error says
/// neither what went wrong nor what to do — and when `model_from_default` is set it names a model
/// the user has never seen.
///
/// Every other error is returned untouched, and the `ApiError` variant is preserved so callers
/// classifying the failure still see the same error kind.
fn explain_model_not_found(model: &str, model_from_default: bool, err: OpenAIError) -> OpenAIError {
    let mut api_error = match err {
        OpenAIError::ApiError(api_error)
            if api_error.r#type.as_deref() == Some("not_found_error") =>
        {
            api_error
        }
        other => return other,
    };

    api_error.message = if model_from_default {
        format!(
            "Failed to run a chat completion with Anthropic model '{model}': Anthropic does not \
             serve that model id, and it is the built-in default used when no model id is \
             configured. Set an explicit id as `from: anthropic:<model_id>`, naming a model your \
             Anthropic API key can reach. See: {ANTHROPIC_DOCS}"
        )
    } else {
        format!(
            "Failed to run a chat completion with Anthropic model '{model}': Anthropic does not \
             serve that model id. Anthropic retires model ids over time — check \
             `from: anthropic:<model_id>` against the models your Anthropic API key can reach. \
             See: {ANTHROPIC_DOCS}"
        )
    };

    OpenAIError::ApiError(api_error)
}

#[cfg(test)]
mod tests {
    use super::{
        ANTHROPIC_DOCS, Anthropic, DEFAULT_ANTHROPIC_MODEL, explain_model_not_found,
        types::validate_model_variant,
    };
    use crate::config::GenericAuthMechanism;
    use async_openai::error::{ApiError, OpenAIError};

    /// The error Anthropic answers with for a model it no longer serves: `model: <id>` and nothing
    /// else. Captured from
    /// <https://github.com/spiceai/spiceai/actions/runs/32968145550>, where the retired default
    /// failed every Anthropic test in the suite.
    fn model_not_found(model: &str) -> OpenAIError {
        OpenAIError::ApiError(ApiError {
            message: format!("model: {model}"),
            r#type: Some("not_found_error".to_string()),
            param: None,
            code: Some("404".to_string()),
        })
    }

    fn message_of(err: &OpenAIError) -> String {
        match err {
            OpenAIError::ApiError(api_error) => api_error.message.clone(),
            other => panic!("expected an ApiError, got {other:?}"),
        }
    }

    #[test]
    fn default_model_not_found_names_the_parameter_to_set() {
        let err = explain_model_not_found(
            DEFAULT_ANTHROPIC_MODEL,
            true,
            model_not_found(DEFAULT_ANTHROPIC_MODEL),
        );
        let message = message_of(&err);

        assert!(
            message.contains(&format!("'{DEFAULT_ANTHROPIC_MODEL}'")),
            "the message must name the model that was not found: {message}"
        );
        assert!(
            message.contains("built-in default"),
            "the user never chose this id, so the message must say where it came from: {message}"
        );
        assert!(
            message.contains("`from: anthropic:<model_id>`"),
            "the message must name the parameter to set: {message}"
        );
        assert!(
            message.contains(ANTHROPIC_DOCS),
            "the message must link the docs: {message}"
        );
        assert!(
            !message.contains('\n'),
            "a user-facing message stays on one line: {message}"
        );
    }

    #[test]
    fn configured_model_not_found_does_not_blame_the_default() {
        let err = explain_model_not_found(
            "claude-3-5-sonnet-latest",
            false,
            model_not_found("claude-3-5-sonnet-latest"),
        );
        let message = message_of(&err);

        assert!(
            message.contains("'claude-3-5-sonnet-latest'"),
            "the message must name the model that was not found: {message}"
        );
        assert!(
            !message.contains("built-in default"),
            "this id came from the configuration, so the message must not call it the default: \
             {message}"
        );
        assert!(
            message.contains("`from: anthropic:<model_id>`") && message.contains(ANTHROPIC_DOCS),
            "the message must name the parameter to check and link the docs: {message}"
        );
        assert!(
            !message.contains('\n'),
            "a user-facing message stays on one line: {message}"
        );
    }

    #[test]
    fn not_found_error_keeps_its_error_kind() {
        let err = explain_model_not_found(
            DEFAULT_ANTHROPIC_MODEL,
            true,
            model_not_found(DEFAULT_ANTHROPIC_MODEL),
        );

        let OpenAIError::ApiError(api_error) = err else {
            panic!("rewriting the message must not change the error variant");
        };
        assert_eq!(
            api_error.r#type.as_deref(),
            Some("not_found_error"),
            "a caller classifying the failure must still see the same error type"
        );
        assert_eq!(api_error.code.as_deref(), Some("404"));
    }

    #[test]
    fn other_errors_are_left_alone() {
        // A different `type` on the same variant: an authentication failure has nothing to do with
        // the model id, and rewriting it would send the reader to the wrong parameter.
        let auth_error = OpenAIError::ApiError(ApiError {
            message: "invalid x-api-key".to_string(),
            r#type: Some("authentication_error".to_string()),
            param: None,
            code: None,
        });
        assert_eq!(
            message_of(&explain_model_not_found(
                DEFAULT_ANTHROPIC_MODEL,
                true,
                auth_error
            )),
            "invalid x-api-key"
        );

        // An `ApiError` with no `type` at all, which is what a proxy in front of Anthropic may
        // return.
        let untyped = OpenAIError::ApiError(ApiError {
            message: "Not Found".to_string(),
            r#type: None,
            param: None,
            code: None,
        });
        assert_eq!(
            message_of(&explain_model_not_found(
                DEFAULT_ANTHROPIC_MODEL,
                true,
                untyped
            )),
            "Not Found"
        );

        // A variant that carries no `type` to test.
        let invalid_argument = OpenAIError::InvalidArgument("Image URL not supported".to_string());
        match explain_model_not_found(DEFAULT_ANTHROPIC_MODEL, true, invalid_argument) {
            OpenAIError::InvalidArgument(message) => {
                assert_eq!(message, "Image URL not supported");
            }
            other => panic!("expected the error to pass through untouched, got {other:?}"),
        }
    }

    /// The explanation above is only reachable if the flag it keys on is wired to the argument, and
    /// nothing else in the crate reads that flag.
    #[test]
    fn model_from_default_tracks_whether_a_model_was_named() {
        let defaulted = Anthropic::new(GenericAuthMechanism::from_api_key("key"), None, None, None)
            .expect("the default model must be constructible");
        assert!(defaulted.model_from_default);
        assert_eq!(defaulted.model, DEFAULT_ANTHROPIC_MODEL);

        let configured = Anthropic::new(
            GenericAuthMechanism::from_api_key("key"),
            Some("claude-haiku-4-5"),
            None,
            None,
        )
        .expect("a named model must be constructible");
        assert!(!configured.model_from_default);
        assert_eq!(configured.model, "claude-haiku-4-5");
    }

    /// A retired default can only be caught against the live API — the integration suite in
    /// `crates/llms/tests` does that. What is checkable here is that the id is one the runtime
    /// will accept at all, since `Anthropic::new` validates it and would otherwise fail every
    /// configuration that names no model.
    #[test]
    fn the_default_model_is_a_valid_model_id() {
        assert!(
            validate_model_variant(DEFAULT_ANTHROPIC_MODEL).is_ok(),
            "the default model must pass the model-id validation in `Anthropic::new`"
        );
    }
}
