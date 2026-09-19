/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Model listing for TypeSafe (`GET /v1/models`).

use async_trait::async_trait;
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use std::collections::HashMap;

use crate::provider::{
    ListModels, ListModelsError, ListModelsResult, create_http_client, get_required_param,
};

use super::DEFAULT_BASE_URL;

const PROVIDER_NAME: &str = "TypeSafe";

#[derive(Debug, Deserialize)]
struct ModelsResponse {
    #[serde(default)]
    models: Vec<ModelCard>,
    /// Some gateways return OpenAI-style `{ data: [...] }`.
    #[serde(default)]
    data: Vec<ModelCard>,
}

#[derive(Debug, Deserialize)]
struct ModelCard {
    #[serde(alias = "id")]
    name: String,
}

/// TypeSafe model lister (`GET /v1/models`).
pub struct TypeSafeModelLister {
    api_key: String,
    base_url: String,
}

impl TypeSafeModelLister {
    /// Required parameter: `typesafe_api_key`. Optional: `typesafe_endpoint`.
    pub fn from_params(params: &HashMap<String, SecretString>) -> ListModelsResult<Self> {
        let api_key = match get_required_param(params, "typesafe_api_key") {
            Ok(k) => k,
            Err(_) => get_required_param(params, "typesafe_ai_api_key")?,
        };
        let base_url = params
            .get("typesafe_endpoint")
            .map_or_else(|| DEFAULT_BASE_URL.to_string(), |s| s.expose_secret().to_string());

        Ok(Self {
            api_key: api_key.expose_secret().to_string(),
            base_url: base_url.trim_end_matches('/').to_string(),
        })
    }

    #[must_use]
    pub fn new(api_key: &SecretString, base_url: Option<&str>) -> Self {
        Self {
            api_key: api_key.expose_secret().to_string(),
            base_url: base_url.unwrap_or(DEFAULT_BASE_URL).trim_end_matches('/').to_string(),
        }
    }
}

#[async_trait]
impl ListModels for TypeSafeModelLister {
    fn provider_name(&self) -> &'static str {
        PROVIDER_NAME
    }

    async fn list_models(&self) -> ListModelsResult<Vec<String>> {
        let client = create_http_client().ok_or_else(|| ListModelsError::NetworkError {
            provider: PROVIDER_NAME.to_string(),
            message: "failed to build HTTP client".to_string(),
        })?;

        let response = client
            .get(format!("{}/v1/models", self.base_url))
            .bearer_auth(&self.api_key)
            .send()
            .await
            .map_err(|e| ListModelsError::NetworkError {
                provider: PROVIDER_NAME.to_string(),
                message: e.to_string(),
            })?;

        let status = response.status();
        if status == reqwest::StatusCode::UNAUTHORIZED {
            return Err(ListModelsError::InvalidCredentials {
                provider: PROVIDER_NAME.to_string(),
            });
        }
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(ListModelsError::RateLimited {
                provider: PROVIDER_NAME.to_string(),
            });
        }
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(ListModelsError::ProviderRefused {
                provider: PROVIDER_NAME.to_string(),
                message: format!("HTTP {status}: {body}"),
            });
        }

        let parsed: ModelsResponse = response.json().await.map_err(|e| {
            ListModelsError::NetworkError {
                provider: PROVIDER_NAME.to_string(),
                message: e.to_string(),
            }
        })?;

        let mut names: Vec<String> = parsed
            .models
            .into_iter()
            .chain(parsed.data)
            .map(|m| m.name)
            .collect();
        names.sort();
        names.dedup();
        Ok(names)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_params_missing_key() {
        let params = HashMap::new();
        assert!(matches!(
            TypeSafeModelLister::from_params(&params),
            Err(ListModelsError::MissingParameter { .. })
        ));
    }

    #[test]
    fn from_params_accepts_ai_api_key_alias() {
        let mut params = HashMap::new();
        params.insert(
            "typesafe_ai_api_key".to_string(),
            SecretString::from("sk-test"),
        );
        TypeSafeModelLister::from_params(&params).expect("alias key");
    }
}
