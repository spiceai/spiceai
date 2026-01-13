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

//! Utilities for fetching available models from various LLM providers.
//!
//! This module delegates to the `llms` crate's provider implementations,
//! which contain the model listing logic for each provider.

use llms::provider::ListModels;
use secrecy::SecretString;
use spicepod::component::model::ModelSource;
use std::collections::HashMap;

/// Fetches a list of available models from a provider to help users debug configuration issues.
///
/// Returns a formatted string of available models, or None if:
/// - The provider doesn't support model listing
/// - The API call fails (rate limits, auth issues, network errors)
/// - Required credentials are missing
///
/// This function gracefully handles all error cases without propagating errors.
pub async fn get_available_models_hint(
    source: &ModelSource,
    params: &HashMap<String, SecretString>,
) -> Option<String> {
    let lister: Box<dyn ListModels> = match source {
        ModelSource::OpenAi => match llms::openai::OpenAiModelLister::from_params(params) {
            Ok(lister) => Box::new(lister),
            Err(e) => {
                tracing::debug!("Cannot create OpenAI model lister: {e}");
                return None;
            }
        },
        ModelSource::Anthropic => {
            match llms::anthropic::AnthropicModelLister::from_params(params) {
                Ok(lister) => Box::new(lister),
                Err(e) => {
                    tracing::debug!("Cannot create Anthropic model lister: {e}");
                    return None;
                }
            }
        }
        ModelSource::Xai => match llms::xai::XaiModelLister::from_params(params) {
            Ok(lister) => Box::new(lister),
            Err(e) => {
                tracing::debug!("Cannot create xAI model lister: {e}");
                return None;
            }
        },
        ModelSource::Google => match llms::google::GoogleModelLister::from_params(params) {
            Ok(lister) => Box::new(lister),
            Err(e) => {
                tracing::debug!("Cannot create Google model lister: {e}");
                return None;
            }
        },
        ModelSource::Bedrock => Box::new(llms::bedrock::BedrockModelLister::from_params(params)),
        ModelSource::Azure => match llms::azure::AzureModelLister::from_params(params) {
            Ok(lister) => Box::new(lister),
            Err(e) => {
                tracing::debug!("Cannot create Azure model lister: {e}");
                return None;
            }
        },
        ModelSource::Databricks => {
            match llms::databricks::DatabricksModelLister::from_params(params) {
                Ok(lister) => Box::new(lister),
                Err(e) => {
                    tracing::debug!("Cannot create Databricks model lister: {e}");
                    return None;
                }
            }
        }
        ModelSource::Perplexity => Box::new(llms::perplexity::PerplexityModelLister::new()),
        ModelSource::SpiceAI => match llms::spiceai::SpiceAiModelLister::from_params(params) {
            Ok(lister) => Box::new(lister),
            Err(e) => {
                tracing::debug!("Cannot create Spice Cloud model lister: {e}");
                return None;
            }
        },
        _ => {
            tracing::debug!("Model source {:?} does not support model listing", source);
            return None;
        }
    };

    lister.get_models_hint().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_missing_credentials_returns_none() {
        let params = HashMap::new();
        let result = get_available_models_hint(&ModelSource::OpenAi, &params).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_perplexity_returns_known_models() {
        let params = HashMap::new();
        let result = get_available_models_hint(&ModelSource::Perplexity, &params).await;
        assert!(result.is_some());
        let hint = result.expect("test: perplexity should return models");
        assert!(hint.contains("sonar"));
    }

    #[tokio::test]
    async fn test_bedrock_returns_known_models() {
        let params = HashMap::new();
        let result = get_available_models_hint(&ModelSource::Bedrock, &params).await;
        assert!(result.is_some());
        let hint = result.expect("test: bedrock should return models");
        assert!(hint.contains("claude"));
    }

    #[tokio::test]
    async fn test_unsupported_source_returns_none() {
        let params = HashMap::new();
        let result = get_available_models_hint(&ModelSource::File, &params).await;
        assert!(result.is_none());
    }
}
