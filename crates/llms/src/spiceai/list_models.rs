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

//! Model listing functionality for Spice Cloud provider.

use async_openai::Client;
use async_trait::async_trait;
use secrecy::{ExposeSecret, SecretString};
use std::collections::HashMap;

use crate::config::HostedModelConfig;
use crate::provider::{ListModels, ListModelsError, ListModelsResult, get_required_param};

const PROVIDER_NAME: &str = "Spice Cloud";
const DEFAULT_ENDPOINT: &str = "https://data.spiceai.io";

/// Spice Cloud model lister that fetches available models using the SDK.
pub struct SpiceAiModelLister {
    client: Client<HostedModelConfig>,
}

impl SpiceAiModelLister {
    /// Creates a new model lister from parameters.
    ///
    /// Required parameter: `spiceai_api_key`
    /// Optional parameter: `spiceai_endpoint` (defaults to <https://data.spiceai.io>)
    pub fn from_params(params: &HashMap<String, SecretString>) -> ListModelsResult<Self> {
        let api_key = get_required_param(params, "spiceai_api_key")?;
        let endpoint = params.get("spiceai_endpoint").map_or_else(
            || format!("{DEFAULT_ENDPOINT}/v1"),
            |s| format!("{}/v1", s.expose_secret().trim_end_matches('/')),
        );

        let config =
            HostedModelConfig::from_url(&endpoint).with_api_key(Some(api_key.expose_secret()));

        Ok(Self {
            client: Client::with_config(config),
        })
    }

    /// Creates a new model lister with explicit credentials.
    #[must_use]
    pub fn new(api_key: &SecretString, endpoint: Option<&str>) -> Self {
        let base_url = endpoint.map_or_else(
            || format!("{DEFAULT_ENDPOINT}/v1"),
            |e| format!("{}/v1", e.trim_end_matches('/')),
        );

        let config =
            HostedModelConfig::from_url(&base_url).with_api_key(Some(api_key.expose_secret()));

        Self {
            client: Client::with_config(config),
        }
    }

    /// Returns common Spice Cloud model names as a fallback.
    #[must_use]
    pub fn common_models() -> Vec<String> {
        vec![
            "openai/gpt-4o".to_string(),
            "openai/gpt-4o-mini".to_string(),
            "anthropic/claude-3-5-sonnet".to_string(),
            "google/gemini-pro".to_string(),
        ]
    }
}

#[async_trait]
impl ListModels for SpiceAiModelLister {
    fn provider_name(&self) -> &'static str {
        PROVIDER_NAME
    }

    async fn list_models(&self) -> ListModelsResult<Vec<String>> {
        let response = self.client.models().list().await.map_err(|e| {
            let message = e.to_string();
            if message.contains("401") || message.contains("Unauthorized") {
                ListModelsError::InvalidCredentials {
                    provider: PROVIDER_NAME.to_string(),
                }
            } else if message.contains("429") || message.contains("rate") {
                ListModelsError::RateLimited {
                    provider: PROVIDER_NAME.to_string(),
                }
            } else {
                ListModelsError::NetworkError {
                    provider: PROVIDER_NAME.to_string(),
                    message,
                }
            }
        })?;

        let models: Vec<String> = response.data.into_iter().map(|m| m.id).collect();

        if models.is_empty() {
            Ok(Self::common_models())
        } else {
            Ok(models)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_params_missing_key() {
        let params = HashMap::new();
        let result = SpiceAiModelLister::from_params(&params);
        assert!(matches!(
            result,
            Err(ListModelsError::MissingParameter { .. })
        ));
    }

    #[test]
    fn test_from_params_with_key() {
        let mut params = HashMap::new();
        params.insert(
            "spiceai_api_key".to_string(),
            SecretString::from("test-key"),
        );
        let result = SpiceAiModelLister::from_params(&params);
        result.expect("should succeed");
    }

    #[test]
    fn test_common_models_not_empty() {
        let models = SpiceAiModelLister::common_models();
        assert!(!models.is_empty());
    }
}
