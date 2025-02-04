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

use async_openai::Client;
use secrecy::Secret;

use crate::config::{GenericAuthMechanism, GenericHandlerConfig};

pub mod chat;
pub mod types;

pub struct PerplexitySonar {
    client: Client<GenericHandlerConfig>,
    model: String,
}

static PERPLEXITY_SONAR_API_BASE: &str = "https://api.perplexity.ai";
static PERPLEXITY_SONAR_DEFAULT_MODEL: &str = "sonar";

// TODO: Add `PerplexityRequestParameters`
impl PerplexitySonar {
    #[must_use]
    pub fn new(auth_token: &Secret<String>, model: Option<&str>) -> Self {
        let cfg = GenericHandlerConfig::default()
            .with_auth(GenericAuthMechanism::BearerToken(auth_token.clone()))
            .with_base_url(PERPLEXITY_SONAR_API_BASE);

        Self {
            client: Client::<GenericHandlerConfig>::with_config(cfg),
            model: model.unwrap_or(PERPLEXITY_SONAR_DEFAULT_MODEL).to_string(),
        }
    }
}
