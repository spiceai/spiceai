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

use std::num::NonZeroU32;
use std::str::FromStr;
use std::sync::Arc;

use async_openai::config::{AzureConfig, Config, OPENAI_API_BASE};
use async_openai::{Client, config::OpenAIConfig};
use governor::Quota;
use runtime_rate_control::{JitterConfig, RateController};

pub mod chat;
pub mod embed;

pub const MAX_COMPLETION_TOKENS: u16 = 1024_u16; // Avoid accidentally using infinite tokens. Should think about this more.

pub(crate) const GPT_4O_MINI: &str = "gpt-4o-mini";
pub(crate) const TEXT_EMBED_3_SMALL: &str = "text-embedding-3-small";

pub const DEFAULT_LLM_MODEL: &str = GPT_4O_MINI;
pub const DEFAULT_EMBEDDING_MODEL: &str = TEXT_EMBED_3_SMALL;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum UsageTier {
    Free,
    #[default]
    Tier1,
    Tier2,
    Tier3,
    Tier4,
    Tier5,
}

impl FromStr for UsageTier {
    type Err = crate::embeddings::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "free" => Ok(UsageTier::Free),
            "tier1" => Ok(UsageTier::Tier1),
            "tier2" => Ok(UsageTier::Tier2),
            "tier3" => Ok(UsageTier::Tier3),
            "tier4" => Ok(UsageTier::Tier4),
            "tier5" => Ok(UsageTier::Tier5),
            _ => Err(crate::embeddings::Error::InvalidOpenAITier {
                tier: s.to_string(),
            }),
        }
    }
}

impl From<UsageTier> for Arc<RateController> {
    fn from(val: UsageTier) -> Self {
        let max_concurrent_requests = match &val {
            &UsageTier::Free => 1,
            &UsageTier::Tier1 => 35,
            &UsageTier::Tier2 | &UsageTier::Tier3 => 60,
            &UsageTier::Tier4 | &UsageTier::Tier5 => 125,
        };

        let per_minute_quota = match &val {
            &UsageTier::Free => 100,
            &UsageTier::Tier1 => 3000,
            &UsageTier::Tier2 | &UsageTier::Tier3 => 5000,
            &UsageTier::Tier4 | &UsageTier::Tier5 => 10000,
        };

        let Some(per_minute_quota) = NonZeroU32::new(per_minute_quota) else {
            unreachable!("per_minute_quota for usage tiers are non-zero");
        };

        Arc::new(
            RateController::builder()
                .with_max_concurrent_requests(max_concurrent_requests)
                .add_quota(Quota::per_minute(per_minute_quota))
                .build(),
        )
    }
}

#[derive(Debug)]
pub struct Openai<C: Config> {
    client: Client<C>,
    model: String,

    rate_controller: Arc<RateController>,
}

pub(crate) fn default_rate_controller() -> Arc<RateController> {
    let Some(default_per_minute_quota) = NonZeroU32::new(500) else {
        unreachable!("Default quota should always be non-zero");
    };

    Arc::new(
        RateController::builder()
            .with_jitter(JitterConfig::zero())
            .with_max_concurrent_requests(4)
            .add_quota(Quota::per_minute(default_per_minute_quota))
            .build(),
    )
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
        rate_controller: default_rate_controller(),
    }
}

#[must_use]
pub fn new_openai_client(
    model: String,
    api_base: Option<&str>,
    api_key: Option<&str>,
    org_id: Option<&str>,
    project_id: Option<&str>,
    usage_tier: Option<UsageTier>,
) -> Openai<OpenAIConfig> {
    // Default to empty API key to avoid picking up ENV variable in downstream library.
    let mut cfg = OpenAIConfig::new().with_api_key("");

    if let Some(org_id) = org_id {
        cfg = cfg.with_org_id(org_id);
    }

    if let Some(project_id) = project_id {
        cfg = cfg.with_project_id(project_id);
    }

    if let Some(api_key) = api_key {
        cfg = cfg.with_api_key(api_key);
    }
    if let Some(api_base) = api_base {
        cfg = cfg.with_api_base(api_base);
    }

    Openai {
        client: Client::with_config(cfg),
        model,
        rate_controller: usage_tier.map_or_else(default_rate_controller, Into::into),
    }
}

#[must_use]
pub fn new_openai_client_with_config<C: async_openai::config::Config>(
    model: String,
    cfg: C,
) -> Openai<C> {
    Openai {
        client: Client::with_config(cfg),
        model,
        rate_controller: default_rate_controller(),
    }
}

impl<C: Config> Openai<C> {
    /// Returns true if the `OpenAI` compatible model supports [structured outputs](https://platform.openai.com/docs/guides/structured-outputs/).
    /// This is only supported for GPT-4o models from `OpenAI` (i.e not any other compatible servers).
    fn supports_structured_output(&self) -> bool {
        self.client.config().api_base() == OPENAI_API_BASE && self.model.starts_with("gpt-4o")
    }

    /// Returns true if the `OpenAI` compatible model supports `max_completion_tokens` in [`CreateChatCompletionRequest`].
    ///
    /// This is useful for limiting the number of tokens used in health checks.
    fn supports_max_completion_tokens(&self) -> bool {
        self.client.config().api_base() == OPENAI_API_BASE
    }
}
