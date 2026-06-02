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

//! Model listing functionality for Anthropic provider.

use async_trait::async_trait;
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use std::collections::HashMap;

use crate::provider::{
    ListModels, ListModelsError, ListModelsResult, create_http_client, get_required_param,
    map_status_to_error,
};

const PROVIDER_NAME: &str = "Anthropic";
const API_BASE: &str = "https://api.anthropic.com/v1";
const API_VERSION: &str = "2023-06-01";

#[derive(Debug, Deserialize)]
struct ModelsResponse {
    data: Vec<Model>,
}

#[derive(Debug, Deserialize)]
struct Model {
    id: String,
}

/// Anthropic model lister that fetches available models from the API.
#[expect(clippy::struct_field_names)]
pub struct AnthropicModelLister {
    api_key: SecretString,
    api_base: String,
    api_version: String,
}

impl AnthropicModelLister {
    /// Creates a new model lister from parameters.
    ///
    /// Required parameter: `anthropic_api_key`
    /// Optional parameters: `anthropic_api_base`, `anthropic_api_version`
    pub fn from_params(params: &HashMap<String, SecretString>) -> ListModelsResult<Self> {
        let api_key = get_required_param(params, "anthropic_api_key")?.clone();
        let api_base = params
            .get("anthropic_api_base")
            .map_or_else(|| API_BASE.to_string(), |s| s.expose_secret().to_string());
        let api_version = params.get("anthropic_api_version").map_or_else(
            || API_VERSION.to_string(),
            |s| s.expose_secret().to_string(),
        );

        Ok(Self {
            api_key,
            api_base,
            api_version,
        })
    }

    /// Creates a new model lister with explicit credentials.
    #[must_use]
    pub fn new(
        api_key: SecretString,
        api_base: Option<String>,
        api_version: Option<String>,
    ) -> Self {
        Self {
            api_key,
            api_base: api_base.unwrap_or_else(|| API_BASE.to_string()),
            api_version: api_version.unwrap_or_else(|| API_VERSION.to_string()),
        }
    }
}

#[async_trait]
impl ListModels for AnthropicModelLister {
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
            .header("x-api-key", self.api_key.expose_secret())
            .header("anthropic-version", &self.api_version)
            .send()
            .await
            .map_err(|e| ListModelsError::NetworkError {
                provider: PROVIDER_NAME.to_string(),
                message: e.to_string(),
            })?;

        if !response.status().is_success() {
            return Err(map_status_to_error(response.status(), PROVIDER_NAME));
        }

        let body = response
            .text()
            .await
            .map_err(|e| ListModelsError::NetworkError {
                provider: PROVIDER_NAME.to_string(),
                message: e.to_string(),
            })?;

        let models: ModelsResponse =
            serde_json::from_str(&body).map_err(|e| ListModelsError::NetworkError {
                provider: PROVIDER_NAME.to_string(),
                message: format!("Failed to parse response: {e}"),
            })?;

        Ok(models.data.into_iter().map(|m| m.id).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_params_missing_key() {
        let params = HashMap::new();
        let result = AnthropicModelLister::from_params(&params);
        assert!(matches!(
            result,
            Err(ListModelsError::MissingParameter { .. })
        ));
    }

    #[test]
    fn test_from_params_with_key() {
        let mut params = HashMap::new();
        params.insert(
            "anthropic_api_key".to_string(),
            SecretString::from("test-key"),
        );
        let result = AnthropicModelLister::from_params(&params);
        result.expect("should succeed");
    }
}
