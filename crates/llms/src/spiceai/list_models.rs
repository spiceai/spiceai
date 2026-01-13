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

use async_trait::async_trait;
use reqwest::header::CONTENT_TYPE;
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use std::collections::HashMap;

use crate::provider::{
    ListModels, ListModelsError, ListModelsResult, create_http_client, get_required_param,
    map_status_to_error,
};

const PROVIDER_NAME: &str = "Spice Cloud";
const DEFAULT_ENDPOINT: &str = "https://data.spiceai.io";

#[derive(Debug, Deserialize)]
struct ModelsResponse {
    data: Vec<Model>,
}

#[derive(Debug, Deserialize)]
struct Model {
    id: String,
}

/// Spice Cloud model lister that fetches available models from the API.
pub struct SpiceAiModelLister {
    api_key: SecretString,
    endpoint: String,
}

impl SpiceAiModelLister {
    /// Creates a new model lister from parameters.
    ///
    /// Required parameter: `spiceai_api_key`
    /// Optional parameter: `spiceai_endpoint` (defaults to https://data.spiceai.io)
    pub fn from_params(params: &HashMap<String, SecretString>) -> ListModelsResult<Self> {
        let api_key = get_required_param(params, "spiceai_api_key")?.clone();
        let endpoint = params
            .get("spiceai_endpoint")
            .map(|s| s.expose_secret().to_string())
            .unwrap_or_else(|| DEFAULT_ENDPOINT.to_string());

        Ok(Self { api_key, endpoint })
    }

    /// Creates a new model lister with explicit credentials.
    #[must_use]
    pub fn new(api_key: SecretString, endpoint: Option<String>) -> Self {
        Self {
            api_key,
            endpoint: endpoint.unwrap_or_else(|| DEFAULT_ENDPOINT.to_string()),
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
        let client = create_http_client().ok_or_else(|| ListModelsError::NetworkError {
            provider: PROVIDER_NAME.to_string(),
            message: "Failed to create HTTP client".to_string(),
        })?;

        let url = format!("{}/v1/models", self.endpoint.trim_end_matches('/'));

        let response = client
            .get(&url)
            .header("X-API-Key", self.api_key.expose_secret())
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

        if models.data.is_empty() {
            Ok(Self::common_models())
        } else {
            Ok(models.data.into_iter().map(|m| m.id).collect())
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
        assert!(matches!(result, Err(ListModelsError::MissingParameter { .. })));
    }

    #[test]
    fn test_from_params_with_key() {
        let mut params = HashMap::new();
        params.insert(
            "spiceai_api_key".to_string(),
            SecretString::new("test-key".to_string()),
        );
        let result = SpiceAiModelLister::from_params(&params);
        assert!(result.is_ok());
    }

    #[test]
    fn test_from_params_with_custom_endpoint() {
        let mut params = HashMap::new();
        params.insert(
            "spiceai_api_key".to_string(),
            SecretString::new("test-key".to_string()),
        );
        params.insert(
            "spiceai_endpoint".to_string(),
            SecretString::new("https://custom.spiceai.io".to_string()),
        );
        let lister = SpiceAiModelLister::from_params(&params).expect("should succeed");
        assert_eq!(lister.endpoint, "https://custom.spiceai.io");
    }

    #[test]
    fn test_common_models_not_empty() {
        let models = SpiceAiModelLister::common_models();
        assert!(!models.is_empty());
    }
}
