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

//! Model listing functionality for the OrcaRouter model provider.

use async_openai::Client;
use async_trait::async_trait;
use secrecy::{ExposeSecret, SecretString};
use std::collections::HashMap;

use crate::config::HostedModelConfig;
use crate::orcarouter::api_base;
use crate::provider::{ListModels, ListModelsError, ListModelsResult, get_required_param};

// Names the provider in credential/network errors.
const PROVIDER_NAME: &str = "OrcaRouter";

/// Model lister for the OrcaRouter AI gateway.
pub struct OrcaRouterModelLister {
    client: Client<HostedModelConfig>,
}

impl OrcaRouterModelLister {
    /// Creates a new model lister from parameters.
    ///
    /// Optional parameter: `orcarouter_endpoint` (defaults to <https://api.orcarouter.ai>)
    /// Parameter `orcarouter_api_key`: required to authenticate with the gateway.
    pub fn from_params(params: &HashMap<String, SecretString>) -> ListModelsResult<Self> {
        let endpoint = params
            .get("orcarouter_endpoint")
            .map(ExposeSecret::expose_secret);

        let api_key = Some(get_required_param(params, "orcarouter_api_key")?);

        Ok(Self::new(api_key, endpoint))
    }

    /// Creates a new model lister with explicit credentials.
    #[must_use]
    pub fn new(api_key: Option<&SecretString>, endpoint: Option<&str>) -> Self {
        let config = HostedModelConfig::from_url(&api_base(endpoint))
            .with_bearer_token(api_key.map(ExposeSecret::expose_secret));

        Self {
            client: Client::with_config(config),
        }
    }

    /// Returns common OrcaRouter model names as a fallback.
    #[must_use]
    pub fn common_models() -> Vec<String> {
        vec![
            "orcarouter/auto".to_string(),
            "orcarouter/free".to_string(),
            "orcarouter/fusion".to_string(),
        ]
    }
}

#[async_trait]
impl ListModels for OrcaRouterModelLister {
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
        let result = OrcaRouterModelLister::from_params(&params);
        assert!(matches!(
            result,
            Err(ListModelsError::MissingParameter { .. })
        ));
    }

    #[test]
    fn test_from_params_with_key() {
        let mut params = HashMap::new();
        params.insert(
            "orcarouter_api_key".to_string(),
            SecretString::from("sk-orca-test"),
        );
        let result = OrcaRouterModelLister::from_params(&params);
        result.expect("should succeed");
    }

    #[test]
    fn test_common_models_not_empty() {
        let models = OrcaRouterModelLister::common_models();
        assert!(!models.is_empty());
    }
}
