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

use llms::bedrock::embed::{cohere::CohereEmbeddingInputType, nova::NovaEmbeddingPurpose};
use runtime_parameters::TypedParams;
use secrecy::SecretString;
use std::collections::HashMap;

/// Parameters for `from: bedrock` embedding models.
///
/// `truncate_mode` stays a string here because it targets a different enum per
/// model family (`CohereEmbeddingTruncate` vs `NovaTruncationMode`); it is
/// parsed once the model id is known.
#[derive(TypedParams)]
#[params(prefix = "bedrock")]
pub struct BedrockEmbeddingParams {
    /// The AWS access key ID.
    #[param(runtime, autoload_secret)]
    pub aws_access_key_id: Option<SecretString>,
    /// The AWS secret access key.
    #[param(runtime, autoload_secret)]
    pub aws_secret_access_key: Option<SecretString>,
    /// The AWS session token.
    #[param(runtime, autoload_secret)]
    pub aws_session_token: Option<SecretString>,
    /// The AWS region to use for Bedrock embeddings.
    #[param(runtime)]
    pub aws_region: Option<String>,
    /// IAM role credential source.
    #[param(runtime)]
    pub aws_iam_role_source: Option<String>,
    /// The AWS profile name to use for credential resolution.
    #[param(runtime)]
    pub aws_profile: Option<String>,
    /// Maximum number of Bedrock API requests per minute.
    #[param(runtime, default = "1500")]
    pub requests_per_min_limit: u32,
    /// Maximum number of concurrent Bedrock API invocations.
    #[param(runtime, default = "40")]
    pub max_concurrent_invocations: usize,
    /// The number of dimensions for the embedding output.
    #[param(runtime)]
    pub dimensions: Option<u32>,
    /// Whether to normalize the embedding output.
    #[param(runtime)]
    pub normalize: Option<bool>,
    /// Truncation mode for input text that exceeds the model's token limit.
    #[param(runtime, alias = "truncate")]
    pub truncate_mode: Option<String>,
    /// The input type for Cohere embedding models.
    #[param(runtime)]
    pub input_type: Option<CohereEmbeddingInputType>,
    /// The embedding purpose for Nova multimodal embedding models.
    #[param(runtime)]
    pub embedding_purpose: Option<NovaEmbeddingPurpose>,
}

impl BedrockEmbeddingParams {
    #[must_use]
    pub fn runtime_params(&self) -> HashMap<String, SecretString> {
        let mut params = HashMap::from([
            (
                "requests_per_min_limit".to_string(),
                SecretString::from(self.requests_per_min_limit.to_string()),
            ),
            (
                "max_concurrent_invocations".to_string(),
                SecretString::from(self.max_concurrent_invocations.to_string()),
            ),
        ]);

        if let Some(value) = &self.aws_access_key_id {
            params.insert("aws_access_key_id".to_string(), value.clone());
        }
        if let Some(value) = &self.aws_secret_access_key {
            params.insert("aws_secret_access_key".to_string(), value.clone());
        }
        if let Some(value) = &self.aws_session_token {
            params.insert("aws_session_token".to_string(), value.clone());
        }
        if let Some(value) = &self.aws_region {
            params.insert("aws_region".to_string(), SecretString::from(value.clone()));
        }
        if let Some(value) = &self.aws_iam_role_source {
            params.insert(
                "aws_iam_role_source".to_string(),
                SecretString::from(value.clone()),
            );
        }
        if let Some(value) = &self.aws_profile {
            params.insert("aws_profile".to_string(), SecretString::from(value.clone()));
        }

        params
    }
}
