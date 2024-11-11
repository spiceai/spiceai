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

use async_openai::config::{Config, OPENAI_API_BASE};
use async_openai::{config::OpenAIConfig, Client};

pub mod chat;
pub mod embed;

pub const MAX_COMPLETION_TOKENS: u16 = 1024_u16; // Avoid accidentally using infinite tokens. Should think about this more.

pub(crate) const GPT3_5_TURBO_INSTRUCT: &str = "gpt-3.5-turbo";
pub(crate) const TEXT_EMBED_3_SMALL: &str = "text-embedding-3-small";

pub const DEFAULT_LLM_MODEL: &str = GPT3_5_TURBO_INSTRUCT;
pub const DEFAULT_EMBEDDING_MODEL: &str = TEXT_EMBED_3_SMALL;

pub struct Openai {
    client: Client<OpenAIConfig>,
    model: String,
}

impl Default for Openai {
    fn default() -> Self {
        Self::new(DEFAULT_LLM_MODEL.to_string(), None, None, None, None)
    }
}

impl Openai {
    #[must_use]
    pub fn new(
        model: String,
        api_base: Option<String>,
        api_key: Option<String>,
        org_id: Option<String>,
        project_id: Option<String>,
    ) -> Self {
        let mut cfg = OpenAIConfig::new()
            .with_org_id(org_id.unwrap_or_default())
            .with_project_id(project_id.unwrap_or_default());

        // If an API key is provided, use it. Otherwise use default from env variables.
        if let Some(api_key) = api_key {
            cfg = cfg.with_api_key(api_key);
        }
        if let Some(api_base) = api_base {
            cfg = cfg.with_api_base(api_base);
        }

        Self {
            client: Client::with_config(cfg),
            model,
        }
    }

    /// Returns true if the `OpenAI` compatible model supports [structured outputs](https://platform.openai.com/docs/guides/structured-outputs/).
    /// This is only supported for GPT-4o models from `OpenAI` (i.e not any other compatible servers).
    fn supports_structured_output(&self) -> bool {
        self.client.config().api_base() == OPENAI_API_BASE && self.model.starts_with("gpt-4o")
    }
}
