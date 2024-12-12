/*
Copyright 2024 The Spice.ai OSS Authors

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
#![allow(clippy::missing_errors_doc)]

use async_openai::config::{AzureConfig, Config, OPENAI_API_BASE};
use async_openai::{config::OpenAIConfig, Client};

pub mod chat;
pub mod embed;

pub const MAX_COMPLETION_TOKENS: u16 = 1024_u16; // Avoid accidentally using infinite tokens. Should think about this more.

pub(crate) const GPT3_5_TURBO_INSTRUCT: &str = "gpt-3.5-turbo";
pub(crate) const TEXT_EMBED_3_SMALL: &str = "text-embedding-3-small";

pub const DEFAULT_LLM_MODEL: &str = GPT3_5_TURBO_INSTRUCT;
pub const DEFAULT_EMBEDDING_MODEL: &str = TEXT_EMBED_3_SMALL;

pub struct Openai<C: Config> {
    client: Client<C>,
    model: String,
}

#[must_use]
pub fn new_azure_client(
    model: String,
    api_base: Option<&str>,
    api_version: Option<&str>,
    deployment_name: Option<&str>,
    entra_token: Option<&str>,
    api_key: Option<&str>,
) -> Openai<AzureConfig> {
    let mut cfg = AzureConfig::new().with_deployment_id(deployment_name.unwrap_or(model.as_str()));

    if let Some(api_base) = api_base {
        cfg = cfg.with_api_base(api_base);
    }

    if let Some(api_version) = api_version {
        cfg = cfg.with_api_version(api_version);
    }

    if let Some(api_key) = api_key {
        cfg = cfg.with_api_key(api_key);
    }

    if let Some(entra_token) = entra_token {
        cfg = cfg.with_entra_token(entra_token);
    }

    Openai {
        client: Client::with_config(cfg),
        model,
    }
}

#[must_use]
pub fn new_openai_client(
    model: String,
    api_base: Option<&str>,
    api_key: Option<&str>,
    org_id: Option<&str>,
    project_id: Option<&str>,
) -> Openai<OpenAIConfig> {
    let mut cfg = OpenAIConfig::new();

    if let Some(org_id) = org_id {
        cfg = cfg.with_org_id(org_id);
    }

    if let Some(project_id) = project_id {
        cfg = cfg.with_project_id(project_id);
    }

    // If an API key is provided, use it. Otherwise use default from env variables.
    if let Some(api_key) = api_key {
        cfg = cfg.with_api_key(api_key);
    }
    if let Some(api_base) = api_base {
        cfg = cfg.with_api_base(api_base);
    }

    Openai {
        client: Client::with_config(cfg),
        model,
    }
}

impl<C: Config> Openai<C> {
    /// Returns true if the `OpenAI` compatible model supports [structured outputs](https://platform.openai.com/docs/guides/structured-outputs/).
    /// This is only supported for GPT-4o models from `OpenAI` (i.e not any other compatible servers).
    fn supports_structured_output(&self) -> bool {
        self.client.config().api_base() == OPENAI_API_BASE && self.model.starts_with("gpt-4o")
    }
}
