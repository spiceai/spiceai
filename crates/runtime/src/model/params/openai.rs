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
use std::str::FromStr;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum OpenAiAuthMode {
    #[default]
    ApiKey,
    Codex,
}

impl OpenAiAuthMode {
    const VALUES: &[&str] = &["api_key", "codex"];
}

impl FromStr for OpenAiAuthMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "api_key" => Ok(Self::ApiKey),
            "codex" => Ok(Self::Codex),
            other => Err(format!(
                "must be one of: {}. Found {other}",
                Self::VALUES.join(", ")
            )),
        }
    }
}

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
    /// Authentication mode for the OpenAI-compatible endpoint. `api_key` uses `openai_api_key`; `codex` forwards the authenticated Codex request headers to the configured Codex endpoint.
    #[param(default = "api_key")]
    pub auth_mode: OpenAiAuthMode,
    /// The `OpenAI` organization ID.
    pub org_id: Option<String>,
    /// The `OpenAI` project ID.
    pub project_id: Option<String>,
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
