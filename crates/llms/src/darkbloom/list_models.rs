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

//! Lists the models Darkbloom serves, for the hint a model that fails to load prints.

use async_openai::Client;
use async_trait::async_trait;
use secrecy::{ExposeSecret, SecretString};
use std::collections::HashMap;

use super::api_base;
use crate::config::HostedModelConfig;
use crate::provider::{
    ListModels, ListModelsResult, classify_openai_compatible_error, get_required_param,
};

const PROVIDER_NAME: &str = "Darkbloom";

/// Lists the models of the Darkbloom network.
pub struct DarkbloomModelLister {
    client: Client<HostedModelConfig>,
}

impl DarkbloomModelLister {
    /// A lister from a model's params: `darkbloom_api_key` (required) and
    /// `darkbloom_endpoint` (optional).
    pub fn from_params(params: &HashMap<String, SecretString>) -> ListModelsResult<Self> {
        let api_key = get_required_param(params, "darkbloom_api_key")?;
        let endpoint = params
            .get("darkbloom_endpoint")
            .map(ExposeSecret::expose_secret);
        Ok(Self::new(api_key, endpoint))
    }

    /// A lister that authenticates with `api_key`, at `endpoint`, or at Darkbloom's API when it
    /// is `None`.
    #[must_use]
    pub fn new(api_key: &SecretString, endpoint: Option<&str>) -> Self {
        let config = HostedModelConfig::from_url(&api_base(endpoint))
            .with_bearer_token(Some(api_key.expose_secret()));
        Self {
            client: crate::provider::client_without_retry(config),
        }
    }
}

#[async_trait]
impl ListModels for DarkbloomModelLister {
    fn provider_name(&self) -> &'static str {
        PROVIDER_NAME
    }

    async fn list_models(&self) -> ListModelsResult<Vec<String>> {
        let response = self
            .client
            .models()
            .list()
            .await
            .map_err(|e| classify_openai_compatible_error(&e, PROVIDER_NAME))?;

        Ok(response.data.into_iter().map(|m| m.id).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::ListModelsError;
    use serde_json::json;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn params(entries: &[(&str, &str)]) -> HashMap<String, SecretString> {
        entries
            .iter()
            .map(|(k, v)| ((*k).to_string(), SecretString::from(*v)))
            .collect()
    }

    #[test]
    fn a_lister_needs_the_api_key() {
        let Err(ListModelsError::MissingParameter { param }) =
            DarkbloomModelLister::from_params(&HashMap::new())
        else {
            panic!("a lister without `darkbloom_api_key` must be refused");
        };
        assert_eq!(param, "darkbloom_api_key");
    }

    #[test]
    fn a_lister_defaults_to_darkbloom() {
        let lister = DarkbloomModelLister::from_params(&params(&[(
            "darkbloom_api_key",
            "eigeninference-test",
        )]))
        .expect("the key is given");
        assert_eq!(
            lister.client.config().base_url,
            "https://api.darkbloom.dev/v1"
        );
    }

    /// The listing reads `darkbloom_endpoint`, the param the model itself reads, with the key
    /// as a bearer token.
    #[tokio::test]
    async fn the_listing_reads_the_models_at_the_models_endpoint() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .and(header("authorization", "Bearer eigeninference-test"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "object": "list",
                "data": [
                    {"id": "gpt-oss-20b", "object": "model", "created": 0, "owned_by": "darkbloom"},
                    {"id": "EigenLabs/Qwen3.8-27B-4bit-mtp", "object": "model", "created": 0, "owned_by": "darkbloom"}
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let endpoint = format!("{}/v1", server.uri());
        let lister = DarkbloomModelLister::from_params(&params(&[
            ("darkbloom_api_key", "eigeninference-test"),
            ("darkbloom_endpoint", &endpoint),
        ]))
        .expect("the key is given");
        assert_eq!(
            lister.list_models().await.expect("the mock lists models"),
            ["gpt-oss-20b", "EigenLabs/Qwen3.8-27B-4bit-mtp"]
        );
    }

    /// A key Darkbloom refuses, with the body Darkbloom returned for one on 2026-11-08.
    #[tokio::test]
    async fn a_refused_key_is_invalid_credentials() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .respond_with(ResponseTemplate::new(401).set_body_json(json!({
                "error": {"code": "authentication_error", "message": "invalid API key", "type": "authentication_error"}
            })))
            .mount(&server)
            .await;

        let endpoint = format!("{}/v1", server.uri());
        let lister = DarkbloomModelLister::new(
            &SecretString::from("eigeninference-bad"),
            Some(endpoint.as_str()),
        );
        let error = lister.list_models().await.expect_err("the key is refused");
        assert!(
            matches!(&error, ListModelsError::InvalidCredentials { provider } if provider == "Darkbloom"),
            "{error:?}"
        );
    }
}
