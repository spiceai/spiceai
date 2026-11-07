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

use runtime_parameters::TypedParams;
use secrecy::SecretString;

/// Parameters for `from: anthropic` chat models.
#[derive(TypedParams)]
#[params(
    prefix = "anthropic",
    passthrough = crate::model::params::common::PREFIXED_COMMON,
    emit_specs
)]
pub struct AnthropicModelParams {
    /// The Anthropic API base endpoint.
    #[param(runtime)]
    pub endpoint: Option<String>,
    /// The Anthropic API key.
    pub api_key: Option<SecretString>,
    /// The Anthropic Auth Token.
    pub auth_token: Option<SecretString>,
    /// The Anthropic workspace ID, sent as the `anthropic-workspace-id` header. Required when the API key is not scoped to a workspace.
    pub workspace_id: Option<String>,
    /// The Anthropic federation rule (`fdrl_...`) that accepts this workload's identity token. With
    /// `anthropic_organization_id` and `anthropic_service_account_id`, the model authenticates with
    /// workload identity federation instead of `anthropic_api_key`.
    pub federation_rule_id: Option<String>,
    /// The Anthropic organization ID (a UUID), for workload identity federation.
    pub organization_id: Option<String>,
    /// The Anthropic service account (`svac_...`) the federated access token acts as.
    pub service_account_id: Option<String>,
    /// The audience the workload's identity token is issued for. Defaults to
    /// `https://api.anthropic.com`.
    pub identity_token_audience: Option<String>,
    /// A file that holds the workload's identity token, read again for every exchange, such as a
    /// Kubernetes projected service account token. When unset, the token is requested from GitHub
    /// Actions.
    pub identity_token_file: Option<String>,
    /// Anthropic usage tier (1-4). Used for rate limit defaults.
    pub usage_tier: Option<String>,
}

impl AnthropicModelParams {
    /// The workload identity federation params, borrowed.
    pub(crate) fn federation(&self) -> llms::workload_identity::AnthropicFederation<'_> {
        llms::workload_identity::AnthropicFederation {
            federation_rule_id: self.federation_rule_id.as_deref(),
            organization_id: self.organization_id.as_deref(),
            service_account_id: self.service_account_id.as_deref(),
            workspace_id: self.workspace_id.as_deref(),
            identity_token_audience: self.identity_token_audience.as_deref(),
            identity_token_file: self.identity_token_file.as_deref(),
        }
    }
}
