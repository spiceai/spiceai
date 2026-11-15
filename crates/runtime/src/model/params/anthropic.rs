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
use secrecy::{ExposeSecret, SecretString};

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
    /// workload identity federation instead of `anthropic_api_key`. Defaults to
    /// `ANTHROPIC_FEDERATION_RULE_ID`, which the Anthropic SDKs read.
    #[param(autoload_secret)]
    pub federation_rule_id: Option<SecretString>,
    /// The Anthropic organization ID (a UUID), for workload identity federation. Defaults to
    /// `ANTHROPIC_ORGANIZATION_ID`.
    #[param(autoload_secret)]
    pub organization_id: Option<SecretString>,
    /// The Anthropic service account (`svac_...`) the federated access token acts as. Defaults to
    /// `ANTHROPIC_SERVICE_ACCOUNT_ID`.
    #[param(autoload_secret)]
    pub service_account_id: Option<SecretString>,
    /// The audience the workload's identity token is issued for. Defaults to
    /// `https://api.anthropic.com`.
    pub identity_token_audience: Option<String>,
    /// A file that holds the workload's identity token, read again for every exchange, such as a
    /// Kubernetes projected service account token. Defaults to `ANTHROPIC_IDENTITY_TOKEN_FILE`;
    /// when neither is set, the token is requested from GitHub Actions or the workload's cloud
    /// identity.
    #[param(autoload_secret)]
    pub identity_token_file: Option<SecretString>,
    /// Anthropic usage tier (1-4). Used for rate limit defaults.
    pub usage_tier: Option<String>,
}

impl AnthropicModelParams {
    /// The workload identity federation params, borrowed. `raw_params` are the ones the Spicepod
    /// sets: when none of them is a federation param and the model has a key (`key_set`), the key
    /// wins and the federation settings from the environment are left out. `workspace_from_env`
    /// is `ANTHROPIC_WORKSPACE_ID`, which applies only to federation.
    pub(crate) fn federation<'a, S: std::hash::BuildHasher>(
        &'a self,
        raw_params: &std::collections::HashMap<String, SecretString, S>,
        key_set: bool,
        workspace_from_env: Option<&'a str>,
    ) -> llms::workload_identity::AnthropicFederation<'a> {
        let autoloaded = [
            ("anthropic_federation_rule_id", &self.federation_rule_id),
            ("anthropic_organization_id", &self.organization_id),
            ("anthropic_service_account_id", &self.service_account_id),
            ("anthropic_identity_token_file", &self.identity_token_file),
        ];
        // A blank value, such as an empty variable, counts as unset.
        let set = |value: &Option<SecretString>| {
            value
                .as_ref()
                .is_some_and(|value| !value.expose_secret().trim().is_empty())
        };
        let in_spicepod = autoloaded
            .iter()
            .any(|(name, value)| set(value) && raw_params.contains_key(*name))
            || (self.identity_token_audience.is_some()
                && raw_params.contains_key("anthropic_identity_token_audience"));
        let from_environment = autoloaded
            .iter()
            .any(|(name, value)| set(value) && !raw_params.contains_key(*name));
        let expose = |value: &'a Option<SecretString>| {
            value
                .as_ref()
                .map(ExposeSecret::expose_secret)
                .filter(|value| !value.trim().is_empty())
                .filter(|_| in_spicepod || !key_set)
        };
        llms::workload_identity::AnthropicFederation {
            federation_rule_id: expose(&self.federation_rule_id),
            organization_id: expose(&self.organization_id),
            service_account_id: expose(&self.service_account_id),
            workspace_id: self.workspace_id.as_deref().or(workspace_from_env),
            identity_token_audience: self.identity_token_audience.as_deref(),
            identity_token_file: expose(&self.identity_token_file),
            settings_from: llms::workload_identity::anthropic_settings_from(
                in_spicepod,
                from_environment,
            ),
        }
    }
}
