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

//! Model listing functionality for AWS Bedrock provider.
//!
//! AWS Bedrock does not have a simple models list API available through the runtime SDK.
//! The management API requires the `aws-sdk-bedrock` crate which is separate from
//! `aws-sdk-bedrockruntime`. For simplicity, this implementation returns known
//! foundation models available in Bedrock.

use async_trait::async_trait;
use secrecy::{ExposeSecret, SecretString};
use std::collections::HashMap;

use crate::provider::{ListModels, ListModelsResult};

const PROVIDER_NAME: &str = "Bedrock";
const DEFAULT_REGION: &str = "us-east-1";

/// AWS Bedrock model lister.
///
/// Since the Bedrock Runtime SDK doesn't include model listing capabilities,
/// this returns a curated list of known foundation models available in Bedrock.
pub struct BedrockModelLister {
    region: String,
}

impl BedrockModelLister {
    /// Creates a new model lister from parameters.
    ///
    /// Optional parameter: `aws_region` (defaults to us-east-1)
    #[must_use]
    pub fn from_params(params: &HashMap<String, SecretString>) -> Self {
        let region = params.get("aws_region").map_or_else(
            || DEFAULT_REGION.to_string(),
            |s| s.expose_secret().to_string(),
        );

        Self { region }
    }

    /// Creates a new model lister with explicit configuration.
    #[must_use]
    pub fn new(region: Option<String>) -> Self {
        Self {
            region: region.unwrap_or_else(|| DEFAULT_REGION.to_string()),
        }
    }

    /// Returns the configured AWS region.
    #[must_use]
    pub fn region(&self) -> &str {
        &self.region
    }

    /// Returns known Bedrock foundation model IDs.
    ///
    /// This list includes commonly used models available through AWS Bedrock.
    /// Model availability varies by region.
    #[must_use]
    pub fn known_models() -> Vec<String> {
        vec![
            // Anthropic Claude models
            "anthropic.claude-3-5-sonnet-20240620-v1:0".to_string(),
            "anthropic.claude-3-5-haiku-20241022-v1:0".to_string(),
            "anthropic.claude-3-opus-20240229-v1:0".to_string(),
            "anthropic.claude-3-sonnet-20240229-v1:0".to_string(),
            "anthropic.claude-3-haiku-20240307-v1:0".to_string(),
            // Amazon Titan models
            "amazon.titan-text-premier-v1:0".to_string(),
            "amazon.titan-text-express-v1".to_string(),
            "amazon.titan-text-lite-v1".to_string(),
            "amazon.titan-embed-text-v2:0".to_string(),
            // Meta Llama models
            "meta.llama3-2-90b-instruct-v1:0".to_string(),
            "meta.llama3-2-11b-instruct-v1:0".to_string(),
            "meta.llama3-1-405b-instruct-v1:0".to_string(),
            "meta.llama3-1-70b-instruct-v1:0".to_string(),
            "meta.llama3-70b-instruct-v1:0".to_string(),
            // Mistral models
            "mistral.mistral-large-2407-v1:0".to_string(),
            "mistral.mixtral-8x7b-instruct-v0:1".to_string(),
            // Cohere models
            "cohere.command-r-plus-v1:0".to_string(),
            "cohere.command-r-v1:0".to_string(),
        ]
    }
}

#[async_trait]
impl ListModels for BedrockModelLister {
    fn provider_name(&self) -> &'static str {
        PROVIDER_NAME
    }

    async fn list_models(&self) -> ListModelsResult<Vec<String>> {
        // Bedrock Runtime SDK doesn't include model listing.
        // Return known foundation models.
        Ok(Self::known_models())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_params_defaults() {
        let params = HashMap::new();
        let lister = BedrockModelLister::from_params(&params);
        assert_eq!(lister.region(), DEFAULT_REGION);
    }

    #[test]
    fn test_from_params_with_region() {
        let mut params = HashMap::new();
        params.insert("aws_region".to_string(), SecretString::from("eu-west-1"));
        let lister = BedrockModelLister::from_params(&params);
        assert_eq!(lister.region(), "eu-west-1");
    }

    #[test]
    fn test_known_models_not_empty() {
        let models = BedrockModelLister::known_models();
        assert!(!models.is_empty());
        assert!(models.iter().any(|m| m.contains("claude")));
        assert!(models.iter().any(|m| m.contains("titan")));
        assert!(models.iter().any(|m| m.contains("llama")));
    }

    #[tokio::test]
    async fn test_list_models() {
        let lister = BedrockModelLister::new(None);
        let models = lister.list_models().await.expect("list should succeed");
        assert!(!models.is_empty());
    }
}
