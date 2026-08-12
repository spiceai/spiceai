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
use runtime_parameters::{ParameterSpec, TypedParams};
use runtime_parameters_typed::{ParamsError, SecretAutoload, autoload_secret, parse_param};
use secrecy::SecretString;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;

pub const CODEX_API_BASE: &str = "https://chatgpt.com/backend-api/codex";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum OpenAiAuthMode {
    #[default]
    ApiKey,
    Codex,
    CodexPlan,
}

impl OpenAiAuthMode {
    const VALUES: &[&str] = &["api_key", "codex", "codex_plan"];
}

impl FromStr for OpenAiAuthMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "api_key" => Ok(Self::ApiKey),
            "codex" => Ok(Self::Codex),
            "codex_plan" => Ok(Self::CodexPlan),
            other => Err(format!(
                "must be one of: {}. Found {other}",
                Self::VALUES.join(", ")
            )),
        }
    }
}

/// Authentication for an `OpenAI` model.
#[derive(Debug)]
pub enum OpenAiAuth {
    ApiKey { api_key: Option<SecretString> },
    Codex,
    CodexPlan,
}

impl OpenAiAuth {
    #[must_use]
    pub const fn is_codex(&self) -> bool {
        matches!(self, Self::Codex | Self::CodexPlan)
    }

    #[must_use]
    pub const fn is_codex_plan(&self) -> bool {
        matches!(self, Self::CodexPlan)
    }

    #[must_use]
    pub fn api_key(&self) -> Option<&SecretString> {
        match self {
            Self::ApiKey { api_key } => api_key.as_ref(),
            Self::Codex | Self::CodexPlan => None,
        }
    }
}

/// Parameters shared by both `OpenAI` authentication variants.
#[derive(Debug, TypedParams)]
#[params(
    prefix = "openai",
    passthrough = crate::model::params::common::OPENAI_COMMON,
    emit_specs
)]
struct OpenAiModelParamsFields {
    /// The `OpenAI` API base endpoint. Can be overridden to use a compatible provider (i.e. Nvidia NIM).
    #[param(runtime, default = "https://api.openai.com/v1")]
    endpoint: String,
    /// The `OpenAI` organization ID.
    org_id: Option<String>,
    /// The `OpenAI` project ID.
    project_id: Option<String>,
    /// The current usage tier for the `OpenAI` account associated with the API key: 'free', 'tier1', 'tier2', 'tier3', 'tier4', or 'tier5'.
    #[param(default = "tier1")]
    usage_tier: UsageTier,
    /// Whether to use the Responses API backend when serving `/v1/chat/completions` for this model. `disabled` proxies to backend `/v1/chat/completions`; `enabled` proxies to backend `/v1/responses`. Defaults to `enabled` with Codex authentication.
    #[param(runtime, default = "disabled")]
    responses_api: ChatBackend,
    /// The `OpenAI` Responses tools to use when calling the model from the Responses API.
    #[param(default = "")]
    responses_tools: String,
}

/// Parameters for `from: openai` chat/responses models.
///
/// Authentication is flattened into the Spicepod parameter map, but stored as
/// one enum so a constructed model cannot carry both API-key and Codex auth.
#[derive(Debug)]
pub struct OpenAiModelParams {
    pub endpoint: String,
    pub auth: OpenAiAuth,
    pub org_id: Option<String>,
    pub project_id: Option<String>,
    pub usage_tier: UsageTier,
    pub responses_api: ChatBackend,
    pub responses_tools: String,
}

impl TypedParams for OpenAiModelParams {
    const PREFIX: &'static str = "openai";

    async fn try_from_params<R: SecretAutoload>(
        component_name: &str,
        mut params: HashMap<String, SecretString>,
        secrets: &Arc<RwLock<R>>,
    ) -> Result<Self, ParamsError> {
        let responses_api_is_supplied = params.contains_key("openai_responses_api");
        let auth_mode = params
            .remove("openai_auth_mode")
            .map(|value| parse_param("openai_auth_mode", &value))
            .transpose()?
            .unwrap_or_default();
        let supplied_api_key = params.remove("openai_api_key");

        let auth = match auth_mode {
            OpenAiAuthMode::ApiKey => OpenAiAuth::ApiKey {
                api_key: match supplied_api_key {
                    Some(api_key) => Some(api_key),
                    None => autoload_secret(secrets, component_name, "openai_api_key").await,
                },
            },
            OpenAiAuthMode::Codex => {
                if supplied_api_key.is_some() {
                    return Err(ParamsError::InvalidValue {
                        user_key: "openai_auth_mode".to_string(),
                        reason: "`codex` and `codex_plan` cannot be combined with `openai_api_key`"
                            .to_string(),
                    });
                }
                OpenAiAuth::Codex
            }
            OpenAiAuthMode::CodexPlan => {
                if supplied_api_key.is_some() {
                    return Err(ParamsError::InvalidValue {
                        user_key: "openai_auth_mode".to_string(),
                        reason: "`codex` and `codex_plan` cannot be combined with `openai_api_key`"
                            .to_string(),
                    });
                }
                OpenAiAuth::CodexPlan
            }
        };

        let fields =
            OpenAiModelParamsFields::try_from_params(component_name, params, secrets).await?;
        let endpoint = if auth.is_codex_plan() && fields.endpoint == "https://api.openai.com/v1" {
            CODEX_API_BASE.to_string()
        } else {
            fields.endpoint
        };
        let responses_api = if auth.is_codex() && !responses_api_is_supplied {
            ChatBackend::Responses
        } else {
            fields.responses_api
        };

        Ok(Self {
            endpoint,
            auth,
            org_id: fields.org_id,
            project_id: fields.project_id,
            usage_tier: fields.usage_tier,
            responses_api,
            responses_tools: fields.responses_tools,
        })
    }
}

impl OpenAiModelParams {
    #[must_use]
    pub fn parameter_specs() -> Vec<ParameterSpec> {
        let mut specs = OpenAiModelParamsFields::parameter_specs();
        specs.push(
            ParameterSpec::component("auth_mode")
                .description("Authentication mode for the OpenAI-compatible endpoint. `api_key` uses `openai_api_key`; `codex` forwards a Codex API-key request to the OpenAI endpoint; `codex_plan` forwards a signed-in Codex request to the ChatGPT Codex endpoint.")
                .default("api_key"),
        );
        specs.push(
            ParameterSpec::component("api_key")
                .description("The OpenAI API key. Cannot be used with `openai_auth_mode: codex` or `codex_plan`.")
                .secret(),
        );
        specs
    }
}
