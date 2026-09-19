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

//! Model listing for `TypeSafe` (`GET /v1/models`).

use async_trait::async_trait;
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use std::collections::HashMap;

use crate::provider::{
    ListModels, ListModelsError, ListModelsResult, create_http_client, get_required_param,
    map_status_to_error,
};

use super::DEFAULT_BASE_URL;

const PROVIDER_NAME: &str = "TypeSafe";

#[derive(Debug, Deserialize)]
struct ModelsResponse {
    /// `alias` accepts the OpenAI-style `{ "data": [...] }` envelope as well.
    #[serde(default, alias = "data")]
    models: Vec<ModelCard>,
}

#[derive(Debug, Deserialize)]
struct ModelCard {
    #[serde(alias = "id")]
    name: String,
}

/// `TypeSafe` model lister (`GET /v1/models`).
pub struct TypeSafeModelLister {
    api_key: SecretString,
    base_url: String,
}

impl TypeSafeModelLister {
    /// Required parameter: `typesafe_api_key`. Optional: `typesafe_endpoint`.
    ///
    /// # Errors
    ///
    /// Returns [`ListModelsError::MissingParameter`] when neither `typesafe_api_key`
    /// nor `typesafe_ai_api_key` is present in `params`.
    pub fn from_params(params: &HashMap<String, SecretString>) -> ListModelsResult<Self> {
        let api_key = match get_required_param(params, "typesafe_api_key") {
            Ok(k) => k,
            Err(_) => get_required_param(params, "typesafe_ai_api_key")?,
        };
        let base_url = params.get("typesafe_endpoint").map_or_else(
            || DEFAULT_BASE_URL.to_string(),
            |s| s.expose_secret().to_string(),
        );

        Ok(Self {
            api_key: api_key.clone(),
            base_url: base_url.trim_end_matches('/').to_string(),
        })
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
            .bearer_auth(self.api_key.expose_secret())
            .send()
            .await
            .map_err(|e| ListModelsError::NetworkError {
                provider: PROVIDER_NAME.to_string(),
                message: e.to_string(),
            })?;

        if !response.status().is_success() {
            return Err(map_status_to_error(response.status(), PROVIDER_NAME));
        }

        let parsed: ModelsResponse =
            response
                .json()
                .await
                .map_err(|e| ListModelsError::NetworkError {
                    provider: PROVIDER_NAME.to_string(),
                    message: e.to_string(),
                })?;

        Ok(parsed.models.into_iter().map(|m| m.name).collect())
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

    /// Both the TypeSafe shape and the OpenAI-style envelope land in one field.
    #[test]
    fn models_response_accepts_either_envelope() {
        let native: ModelsResponse =
            serde_json::from_str(r#"{"models":[{"name":"jev-latest"}]}"#).expect("native");
        assert_eq!(native.models[0].name, "jev-latest");

        let openai_style: ModelsResponse =
            serde_json::from_str(r#"{"data":[{"id":"jev-1.13.0"}]}"#).expect("openai style");
        assert_eq!(openai_style.models[0].name, "jev-1.13.0");
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
