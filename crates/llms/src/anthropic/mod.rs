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
#![allow(clippy::missing_errors_doc)]
use async_openai::{error::OpenAIError, Client};
use types::validate_model_variant;

mod chat;
mod types;
mod types_stream;

pub use types::AnthropicModelVariant;

use crate::config::{GenericAuthMechanism, HostedModelConfig};

pub struct Anthropic {
    client: Client<HostedModelConfig>,
    model: AnthropicModelVariant,
}

static ANTHROPIC_API_BASE: &str = "https://api.anthropic.com/v1";
pub static DEFAULT_ANTHROPIC_MODEL: &str = "claude-3-5-sonnet-latest";
static ANTHROPIC_API_VERSION: &str = "2023-06-01";

impl Anthropic {
    pub fn new(
        auth: GenericAuthMechanism,
        model: Option<&str>,
        api_base: Option<&str>,
        version: Option<&str>,
    ) -> Result<Self, OpenAIError> {
        let variant = validate_model_variant(model.unwrap_or(DEFAULT_ANTHROPIC_MODEL))?;
        let cfg = HostedModelConfig::default()
            .with_auth(auth)
            .with_base_url(api_base.unwrap_or(ANTHROPIC_API_BASE))
            .with_header(
                "anthropic-version",
                version.unwrap_or(ANTHROPIC_API_VERSION),
            )
            .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?;

        Ok(Self {
            client: Client::<HostedModelConfig>::with_config(cfg),
            model: variant,
        })
    }
}
