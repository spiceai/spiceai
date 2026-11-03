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

use llms::openai::{ChatBackend, UsageTier};
use runtime_parameters::TypedParams;
use secrecy::SecretString;

/// Parameters for `from: openai` chat/responses models.
#[derive(Debug, TypedParams)]
#[params(
    prefix = "openai",
    passthrough = crate::model::params::common::OPENAI_COMMON,
    emit_specs
)]
pub struct OpenAiModelParams {
    /// The `OpenAI` API base endpoint. Can be overridden to use a compatible provider (i.e. Nvidia NIM).
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
    /// The current usage tier for the `OpenAI` account associated with the API key: 'free', 'tier1', 'tier2', 'tier3', 'tier4', or 'tier5'.
    #[param(default = "tier1")]
    pub usage_tier: UsageTier,
    /// Whether to use the Responses API backend when serving `/v1/chat/completions` for this model. `disabled` proxies to backend `/v1/chat/completions`; `enabled` proxies to backend `/v1/responses`.
    #[param(runtime, default = "disabled")]
    pub responses_api: ChatBackend,
    /// The `OpenAI` Responses tools to use when calling the model from the Responses API.
    #[param(default = "")]
    pub responses_tools: String,
}

impl OpenAiModelParams {
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
