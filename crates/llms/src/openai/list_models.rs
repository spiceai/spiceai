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

//! Model listing functionality for `OpenAI` provider.

use async_openai::{Client, config::OpenAIConfig};
use async_trait::async_trait;
use secrecy::{ExposeSecret, SecretString};
use std::collections::HashMap;

use crate::provider::{ListModels, ListModelsError, ListModelsResult, get_required_param};

const PROVIDER_NAME: &str = "OpenAI";
const API_BASE: &str = "https://api.openai.com/v1";

/// `OpenAI` model lister that fetches available models using the SDK.
pub struct OpenAiModelLister {
    client: Client<OpenAIConfig>,
}

impl OpenAiModelLister {
    /// Creates a new model lister from parameters.
    ///
    /// Required parameter: `openai_api_key`
    /// Optional parameter: `openai_api_base` (defaults to `OpenAI` API)
    pub fn from_params(params: &HashMap<String, SecretString>) -> ListModelsResult<Self> {
        let api_key = get_required_param(params, "openai_api_key")?;
        let api_base = params
            .get("openai_api_base")
            .map_or_else(|| API_BASE.to_string(), |s| s.expose_secret().to_string());

        let config = OpenAIConfig::default()
            .with_api_key(api_key.expose_secret())
            .with_api_base(&api_base);

        Ok(Self {
            client: Client::with_config(config),
        })
    }

    /// Creates a new model lister with explicit credentials.
    #[must_use]
    pub fn new(api_key: &SecretString, api_base: Option<&str>) -> Self {
        let config = OpenAIConfig::default()
            .with_api_key(api_key.expose_secret())
            .with_api_base(api_base.unwrap_or(API_BASE));

        Self {
            client: Client::with_config(config),
        }
    }
}

#[async_trait]
impl ListModels for OpenAiModelLister {
    fn provider_name(&self) -> &'static str {
        PROVIDER_NAME
    }

    async fn list_models(&self) -> ListModelsResult<Vec<String>> {
        let response = self.client.models().list().await.map_err(|e| {
            // Map OpenAI errors to our error types
            let message = e.to_string();
            if message.contains("401") || message.contains("Unauthorized") {
                ListModelsError::InvalidCredentials {
                    provider: PROVIDER_NAME.to_string(),
                }
            } else if message.contains("429") || message.contains("rate") {
                ListModelsError::RateLimited {
                    provider: PROVIDER_NAME.to_string(),
                }
            } else if message.contains("402") || message.contains("quota") {
                ListModelsError::QuotaExceeded {
                    provider: PROVIDER_NAME.to_string(),
                }
            } else {
                ListModelsError::NetworkError {
                    provider: PROVIDER_NAME.to_string(),
                    message,
                }
            }
        })?;

        // Filter to commonly used chat models
        let chat_models: Vec<String> = response
            .data
            .into_iter()
            .map(|m| m.id)
            .filter(|id| {
                id.starts_with("gpt-")
                    || id.starts_with("o1")
                    || id.starts_with("o3")
                    || id.starts_with("o4")
            })
            .collect();

        Ok(chat_models)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_params_missing_key() {
        let params = HashMap::new();
        let result = OpenAiModelLister::from_params(&params);
        assert!(matches!(
            result,
            Err(ListModelsError::MissingParameter { .. })
        ));
    }

    #[test]
    fn test_from_params_with_key() {
        let mut params = HashMap::new();
        params.insert("openai_api_key".to_string(), SecretString::from("test-key"));
        let result = OpenAiModelLister::from_params(&params);
        result.expect("should succeed");
    }

    #[test]
    fn test_from_params_with_custom_base() {
        let mut params = HashMap::new();
        params.insert("openai_api_key".to_string(), SecretString::from("test-key"));
        params.insert(
            "openai_api_base".to_string(),
            SecretString::from("https://custom.api.com"),
        );
        // Verify that from_params succeeds with custom base URL
        let result = OpenAiModelLister::from_params(&params);
        result.expect("should succeed with custom base URL");
    }
}
