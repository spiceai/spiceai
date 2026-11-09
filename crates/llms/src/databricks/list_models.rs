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

//! Model listing functionality for Databricks provider.

use async_trait::async_trait;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use std::collections::HashMap;

use crate::provider::{
    ListModels, ListModelsError, ListModelsResult, create_http_client, get_required_param,
    map_status_to_error,
};

const PROVIDER_NAME: &str = "Databricks";

#[derive(Debug, Deserialize)]
struct EndpointsResponse {
    endpoints: Option<Vec<Endpoint>>,
}

#[derive(Debug, Deserialize)]
struct Endpoint {
    name: String,
}

/// Databricks model lister that fetches available serving endpoints from the API.
pub struct DatabricksModelLister {
    endpoint: String,
    token: SecretString,
}

impl DatabricksModelLister {
    /// Creates a new model lister from parameters.
    ///
    /// Required parameters: `databricks_endpoint`, `databricks_token`
    pub fn from_params(params: &HashMap<String, SecretString>) -> ListModelsResult<Self> {
        let endpoint = get_required_param(params, "databricks_endpoint")?
            .expose_secret()
            .to_string();
        let token = get_required_param(params, "databricks_token")?.clone();

        Ok(Self { endpoint, token })
    }

    /// Creates a new model lister with explicit credentials.
    #[must_use]
    pub fn new(endpoint: String, token: SecretString) -> Self {
        Self { endpoint, token }
    }

    /// Returns common Databricks Foundation Model API model names as a fallback.
    #[must_use]
    pub fn common_models() -> Vec<String> {
        vec![
            "databricks-meta-llama-3-3-70b-instruct".to_string(),
            "databricks-meta-llama-3-1-405b-instruct".to_string(),
            "databricks-claude-sonnet-4".to_string(),
            "databricks-claude-3-7-sonnet".to_string(),
            "databricks-gemini-2-5-flash".to_string(),
            "databricks-gte-large-en".to_string(),
            "databricks-bge-large-en".to_string(),
        ]
    }
}

#[async_trait]
impl ListModels for DatabricksModelLister {
    fn provider_name(&self) -> &'static str {
        PROVIDER_NAME
    }

    async fn list_models(&self) -> ListModelsResult<Vec<String>> {
        let client = create_http_client().ok_or_else(|| ListModelsError::NetworkError {
            provider: PROVIDER_NAME.to_string(),
            message: "Failed to create HTTP client".to_string(),
        })?;

        let url = format!(
            "{}/api/2.0/serving-endpoints",
            self.endpoint.trim_end_matches('/')
        );

        let response = client
            .get(&url)
            .header(
                AUTHORIZATION,
                format!("Bearer {}", self.token.expose_secret()),
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

        let body = response
            .text()
            .await
            .map_err(|e| ListModelsError::NetworkError {
                provider: PROVIDER_NAME.to_string(),
                message: e.to_string(),
            })?;

        let endpoints: EndpointsResponse =
            serde_json::from_str(&body).map_err(|e| ListModelsError::NetworkError {
                provider: PROVIDER_NAME.to_string(),
                message: format!("Failed to parse response: {e}"),
            })?;

        let models: Vec<String> = endpoints
            .endpoints
            .unwrap_or_default()
            .into_iter()
            .map(|e| e.name)
            .collect();

        // If no custom endpoints found, return common Foundation Model API models
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
    use serde_json::json;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn test_from_params_missing_endpoint() {
        let mut params = HashMap::new();
        params.insert(
            "databricks_token".to_string(),
            SecretString::from("test-token"),
        );
        let result = DatabricksModelLister::from_params(&params);
        assert!(matches!(
            result,
            Err(ListModelsError::MissingParameter { .. })
        ));
    }

    #[test]
    fn test_from_params_missing_token() {
        let mut params = HashMap::new();
        params.insert(
            "databricks_endpoint".to_string(),
            SecretString::from("https://test.databricks.com"),
        );
        let result = DatabricksModelLister::from_params(&params);
        assert!(matches!(
            result,
            Err(ListModelsError::MissingParameter { .. })
        ));
    }

    #[tokio::test]
    async fn test_from_params_success() {
        let mut params = HashMap::new();
        params.insert(
            "databricks_endpoint".to_string(),
            SecretString::from("https://test.databricks.com"),
        );
        params.insert(
            "databricks_token".to_string(),
            SecretString::from("test-token"),
        );
        let lister = DatabricksModelLister::from_params(&params).expect("should succeed");
        assert_eq!(lister.endpoint, "https://test.databricks.com");
        assert_eq!(lister.token.expose_secret(), "test-token");

        // The configured endpoint and token are the ones the listing uses, and
        // each serving endpoint is listed by name, in order.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/2.0/serving-endpoints"))
            .and(header("authorization", "Bearer test-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "endpoints": [{"name": "my-llm"}, {"name": "my-embedder"}]
            })))
            .expect(1)
            .mount(&server)
            .await;
        params.insert(
            "databricks_endpoint".to_string(),
            SecretString::from(server.uri()),
        );
        let models = DatabricksModelLister::from_params(&params)
            .expect("should succeed")
            .list_models()
            .await
            .expect("the mock serves the endpoint list");
        assert_eq!(
            models,
            vec!["my-llm".to_string(), "my-embedder".to_string()]
        );
    }

    /// A workspace with no custom serving endpoints still offers the Foundation
    /// Model API, so an empty or absent endpoint list falls back to it.
    #[tokio::test]
    async fn list_models_falls_back_to_common_models_without_serving_endpoints() {
        for body in [json!({"endpoints": []}), json!({})] {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/api/2.0/serving-endpoints"))
                .respond_with(ResponseTemplate::new(200).set_body_json(&body))
                .expect(1)
                .mount(&server)
                .await;
            let lister = DatabricksModelLister::new(server.uri(), SecretString::from("test-token"));
            let models = lister
                .list_models()
                .await
                .unwrap_or_else(|e| panic!("listing {body} must succeed: {e}"));
            assert_eq!(
                models,
                DatabricksModelLister::common_models(),
                "response body: {body}"
            );
        }
    }
}
