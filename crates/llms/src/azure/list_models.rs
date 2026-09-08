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

//! Model listing functionality for Azure `OpenAI` provider.

use async_trait::async_trait;
use reqwest::header::CONTENT_TYPE;
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use std::collections::HashMap;

use crate::provider::{
    ListModels, ListModelsError, ListModelsResult, create_http_client, get_required_param,
    map_status_to_error,
};

const PROVIDER_NAME: &str = "Azure OpenAI";
const DEFAULT_API_VERSION: &str = "2024-10-21";

#[derive(Debug, Deserialize)]
struct ModelsResponse {
    data: Vec<Model>,
}

#[derive(Debug, Deserialize)]
struct Model {
    id: String,
}

/// Azure `OpenAI` model lister that fetches available models/deployments from the API.
pub struct AzureModelLister {
    endpoint: String,
    api_key: Option<SecretString>,
    entra_token: Option<SecretString>,
    api_version: String,
}

impl AzureModelLister {
    /// Creates a new model lister from parameters.
    ///
    /// Required parameter: `azure_endpoint`
    /// One of: `azure_api_key` or `azure_entra_token`
    /// Optional: `azure_api_version` (defaults to 2024-10-21)
    pub fn from_params(params: &HashMap<String, SecretString>) -> ListModelsResult<Self> {
        let endpoint = get_required_param(params, "azure_endpoint")?
            .expose_secret()
            .to_string();

        let api_key = params.get("azure_api_key").cloned();
        let entra_token = params.get("azure_entra_token").cloned();

        if api_key.is_none() && entra_token.is_none() {
            return Err(ListModelsError::MissingParameter {
                param: "azure_api_key or azure_entra_token".to_string(),
            });
        }

        let api_version = params.get("azure_api_version").map_or_else(
            || DEFAULT_API_VERSION.to_string(),
            |s| s.expose_secret().to_string(),
        );

        Ok(Self {
            endpoint,
            api_key,
            entra_token,
            api_version,
        })
    }

    /// Creates a new model lister with explicit credentials.
    #[must_use]
    pub fn new(
        endpoint: String,
        api_key: Option<SecretString>,
        entra_token: Option<SecretString>,
        api_version: Option<String>,
    ) -> Self {
        Self {
            endpoint,
            api_key,
            entra_token,
            api_version: api_version.unwrap_or_else(|| DEFAULT_API_VERSION.to_string()),
        }
    }
}

#[async_trait]
impl ListModels for AzureModelLister {
    fn provider_name(&self) -> &'static str {
        PROVIDER_NAME
    }

    async fn list_models(&self) -> ListModelsResult<Vec<String>> {
        let client = create_http_client().ok_or_else(|| ListModelsError::NetworkError {
            provider: PROVIDER_NAME.to_string(),
            message: "Failed to create HTTP client".to_string(),
        })?;

        let url = format!(
            "{}/openai/models?api-version={}",
            self.endpoint.trim_end_matches('/'),
            self.api_version
        );

        let mut request = client.get(&url).header(CONTENT_TYPE, "application/json");

        // Use api-key header for API key auth, or Authorization Bearer for Entra token
        if let Some(ref api_key) = self.api_key {
            request = request.header("api-key", api_key.expose_secret());
        } else if let Some(ref entra_token) = self.entra_token {
            request = request.header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {}", entra_token.expose_secret()),
            );
        }

        let response = request
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
    fn test_from_params_missing_endpoint() {
        let params = HashMap::new();
        let result = AzureModelLister::from_params(&params);
        assert!(matches!(
            result,
            Err(ListModelsError::MissingParameter { .. })
        ));
    }

    #[test]
    fn test_from_params_missing_auth() {
        let mut params = HashMap::new();
        params.insert(
            "azure_endpoint".to_string(),
            SecretString::from("https://test.openai.azure.com"),
        );
        let result = AzureModelLister::from_params(&params);
        assert!(matches!(
            result,
            Err(ListModelsError::MissingParameter { .. })
        ));
    }

    #[test]
    fn test_from_params_with_api_key() {
        let mut params = HashMap::new();
        params.insert(
            "azure_endpoint".to_string(),
            SecretString::from("https://test.openai.azure.com"),
        );
        params.insert("azure_api_key".to_string(), SecretString::from("test-key"));
        let result = AzureModelLister::from_params(&params);
        result.expect("should succeed with api_key");
    }

    #[test]
    fn test_from_params_with_entra_token() {
        let mut params = HashMap::new();
        params.insert(
            "azure_endpoint".to_string(),
            SecretString::from("https://test.openai.azure.com"),
        );
        params.insert(
            "azure_entra_token".to_string(),
            SecretString::from("test-token"),
        );
        let result = AzureModelLister::from_params(&params);
        result.expect("should succeed with entra_token");
    }
}
