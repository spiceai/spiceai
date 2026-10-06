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

use llms::openai::ChatBackend;
use runtime_parameters::TypedParams;
use secrecy::SecretString;

/// Parameters for `from: azure` chat/responses models.
#[derive(TypedParams)]
#[params(
    prefix = "azure",
    passthrough = crate::model::params::common::PREFIXED_COMMON,
    emit_specs
)]
pub struct AzureModelParams {
    /// The Azure `OpenAI` resource endpoint, e.g., <https://resource-name.openai.azure.com>.
    #[param(runtime)]
    pub endpoint: Option<String>,
    /// The API version used for the Azure `OpenAI` service.
    pub api_version: Option<String>,
    /// The name of the model deployment.
    pub deployment_name: Option<String>,
    /// The Azure `OpenAI` API key from the models deployment page.
    pub api_key: Option<SecretString>,
    /// The Azure Entra token for authentication.
    pub entra_token: Option<SecretString>,
    /// Comma-separated list of `OpenAI`-hosted tools exposed via the Responses API for this model.
    // Read as the literal `openai_responses_tools` key on the Responses path
    // (`responses.rs`), so it is unprefixed (`runtime`) rather than azure-scoped.
    #[param(runtime, default = "")]
    pub openai_responses_tools: String,
    /// Whether to use the Responses API backend when serving `/v1/chat/completions` for this model. `disabled` proxies to backend `/v1/chat/completions`; `enabled` proxies to backend `/v1/responses`.
    #[param(runtime, default = "disabled")]
    pub responses_api: ChatBackend,
}
