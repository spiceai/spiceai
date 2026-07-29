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

use std::collections::HashMap;

use runtime_parameters::TypedParams;
use secrecy::SecretString;

/// Parameters for `from: bedrock` chat models.
#[derive(TypedParams)]
#[params(
    prefix = "bedrock",
    passthrough = crate::model::params::common::PREFIXED_COMMON,
    emit_specs
)]
pub struct BedrockModelParams {
    /// The AWS access key ID to use for Bedrock models.
    #[param(runtime, autoload_secret)]
    pub aws_access_key_id: Option<SecretString>,
    /// The AWS secret access key to use for Bedrock models.
    #[param(runtime, autoload_secret)]
    pub aws_secret_access_key: Option<SecretString>,
    /// The AWS session token to use for Bedrock models.
    #[param(runtime, autoload_secret)]
    pub aws_session_token: Option<SecretString>,
    /// The AWS region to use for Bedrock models.
    #[param(runtime)]
    pub aws_region: Option<String>,
    /// IAM role credential source. 'auto' uses the default AWS credential chain, 'metadata' uses only instance/container metadata (IMDS, ECS, EKS/IRSA), 'env' uses only environment variables.
    #[param(runtime, one_of = ["auto", "metadata", "env"])]
    pub aws_iam_role_source: Option<String>,
    /// Identifier for the guardrail.
    pub guardrail_identifier: Option<String>,
    /// Guardrail version.
    pub guardrail_version: Option<String>,
    /// Trace behavior for the guardrail. Valid values: `enabled`, `disabled`, `enabled_full`.
    #[param(one_of = ["enabled", "disabled", "enabled_full"])]
    pub trace: Option<String>,
}

impl BedrockModelParams {
    /// Builds the AWS-credential map consumed by
    /// [`crate::model::util::create_bedrock_client`]. Keys mirror the historical
    /// runtime params so credential resolution is unchanged.
    #[must_use]
    pub fn runtime_params(&self) -> HashMap<String, SecretString> {
        let mut params = HashMap::new();
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
        params
    }
}
