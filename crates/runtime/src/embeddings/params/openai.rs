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

use llms::openai::UsageTier;
use runtime_parameters::TypedParams;
use secrecy::SecretString;

/// Parameters for `from: openai` embedding models.
#[derive(TypedParams)]
#[params(prefix = "openai")]
pub struct OpenAiEmbeddingParams {
    /// The `OpenAI` API base endpoint.
    #[param(runtime, default = "https://api.openai.com/v1")]
    pub endpoint: String,
    /// The `OpenAI` API key.
    #[param(autoload_secret)]
    pub api_key: Option<SecretString>,
    /// The `OpenAI` organization ID.
    pub org_id: Option<String>,
    /// The `OpenAI` project ID.
    pub project_id: Option<String>,
    /// The `OpenAI` workload identity provider (`idp_...`) that verifies this workload's identity
    /// token. With `openai_service_account_id` and `openai_identity_token_audience`, the model
    /// authenticates with workload identity federation instead of `openai_api_key`.
    pub identity_provider_id: Option<String>,
    /// The `OpenAI` service account (`user-...`) that the workload identity provider's mapping
    /// resolves this workload to.
    pub service_account_id: Option<String>,
    /// The audience the workload's identity token is issued for. It must match the workload
    /// identity provider's Audience.
    pub identity_token_audience: Option<String>,
    /// A file that holds the workload's identity token, read again for every exchange, such as a
    /// Kubernetes projected service account token. When unset, the token is requested from GitHub
    /// Actions.
    pub identity_token_file: Option<String>,
    /// The current usage tier for the `OpenAI` account: 'free', 'tier1'-'tier5'.
    #[param(default = "tier1")]
    pub usage_tier: UsageTier,
}

impl OpenAiEmbeddingParams {
    /// The workload identity federation params, and the client settings, borrowed.
    pub(crate) fn federation(&self) -> llms::workload_identity::OpenAiFederation<'_> {
        llms::workload_identity::OpenAiFederation {
            identity_provider_id: self.identity_provider_id.as_deref(),
            service_account_id: self.service_account_id.as_deref(),
            identity_token_audience: self.identity_token_audience.as_deref(),
            identity_token_file: self.identity_token_file.as_deref(),
            endpoint: self.endpoint.as_str(),
            org_id: self.org_id.as_deref(),
            project_id: self.project_id.as_deref(),
            usage_tier: self.usage_tier,
        }
    }
}
