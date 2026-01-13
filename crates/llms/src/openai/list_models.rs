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

//! Model listing functionality for OpenAI provider.

use async_trait::async_trait;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use std::collections::HashMap;

use crate::provider::{
    ListModels, ListModelsError, ListModelsResult, create_http_client, get_required_param,
    map_status_to_error,
};

const PROVIDER_NAME: &str = "OpenAI";
const API_BASE: &str = "https://api.openai.com/v1";

#[derive(Debug, Deserialize)]
struct ModelsResponse {
    data: Vec<Model>,
}

#[derive(Debug, Deserialize)]
struct Model {
    id: String,
}

/// OpenAI model lister that fetches available models from the API.
pub struct OpenAiModelLister {
    api_key: SecretString,
    api_base: String,
}

impl OpenAiModelLister {
    /// Creates a new model lister from parameters.
    ///
    /// Required parameter: `openai_api_key`
    /// Optional parameter: `openai_api_base` (defaults to OpenAI API)
    pub fn from_params(params: &HashMap<String, SecretString>) -> ListModelsResult<Self> {
        let api_key = get_required_param(params, "openai_api_key")?.clone();
        let api_base = params
            .get("openai_api_base")
            .map(|s| s.expose_secret().to_string())
            .unwrap_or_else(|| API_BASE.to_string());

        Ok(Self { api_key, api_base })
    }

    /// Creates a new model lister with explicit credentials.
    #[must_use]
    pub fn new(api_key: SecretString, api_base: Option<String>) -> Self {
        Self {
            api_key,
            api_base: api_base.unwrap_or_else(|| API_BASE.to_string()),
        }
    }
}

#[async_trait]
impl ListModels for OpenAiModelLister {
    fn provider_name(&self) -> &'static str {
        PROVIDER_NAME
    }

    async fn list_models(&self) -> ListModelsResult<Vec<String>> {
        let client = create_http_client().ok_or_else(|| ListModelsError::NetworkError {
            provider: PROVIDER_NAME.to_string(),
            message: "Failed to create HTTP client".to_string(),
        })?;

        let url = format!("{}/models", self.api_base.trim_end_matches('/'));

        let response = client
            .get(&url)
            .header(
                AUTHORIZATION,
                format!("Bearer {}", self.api_key.expose_secret()),
            )
            .header(CONTENT_TYPE, "application/json")
            .send()
            .await
            .map_err(|e| ListModelsError::NetworkError {
                provider: PROVIDER_NAME.to_string(),
                message: e.to_string(),
            })?;

        if !response.status().is_success() {
            return Err(map_status_to_error(response.status(), PROVIDER_NAME));
        }

        let body = response.text().await.map_err(|e| ListModelsError::NetworkError {
            provider: PROVIDER_NAME.to_string(),
            message: e.to_string(),
        })?;

        let models: ModelsResponse =
            serde_json::from_str(&body).map_err(|e| ListModelsError::NetworkError {
                provider: PROVIDER_NAME.to_string(),
                message: format!("Failed to parse response: {e}"),
            })?;

        // Filter to commonly used chat models
        let chat_models: Vec<String> = models
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
        assert!(matches!(result, Err(ListModelsError::MissingParameter { .. })));
    }

    #[test]
    fn test_from_params_with_key() {
        let mut params = HashMap::new();
        params.insert(
            "openai_api_key".to_string(),
            SecretString::new("test-key".to_string()),
        );
        let result = OpenAiModelLister::from_params(&params);
        assert!(result.is_ok());
    }

    #[test]
    fn test_from_params_with_custom_base() {
        let mut params = HashMap::new();
        params.insert(
            "openai_api_key".to_string(),
            SecretString::new("test-key".to_string()),
        );
        params.insert(
            "openai_api_base".to_string(),
            SecretString::new("https://custom.api.com".to_string()),
        );
        let lister = OpenAiModelLister::from_params(&params).expect("should succeed");
        assert_eq!(lister.api_base, "https://custom.api.com");
    }
}
