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

use async_openai::{config::Config, Client};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use secrecy::{ExposeSecret, Secret};
use std::sync::LazyLock;

mod chat;
mod types;
mod types_stream;

pub use types::AnthropicModelVariant;

pub struct Anthropic {
    client: Client<AnthropicConfig>,
    model: AnthropicModelVariant,

    // The name of the model as known in the spice runtime (not the anthropic model).
    name: String,
}

static ANTHROPIC_API_BASE: &str = "https://api.anthropic.com/v1";
pub static DEFAULT_ANTHROPIC_MODEL: &str = "claude-3-5-sonnet-20240620";
static ANTHROPIC_API_VERSION: &str = "2023-06-01";
static DUMMY_API_KEY: LazyLock<Secret<String>> = LazyLock::new(|| Secret::new(String::new()));

impl Anthropic {
    pub fn new<S: Into<AnthropicModelVariant>>(
        config: AnthropicConfig,
        model: S,
        name: &str,
    ) -> Self {
        Self {
            client: Client::<AnthropicConfig>::with_config(config),
            model: model.into(),
            name: name.to_string(),
        }
    }
}

#[derive(Clone)]
pub struct AnthropicConfig {
    pub auth: Option<AnthropicAuthMechanism>,
    pub base_url: String,
    pub version: String,
    pub beta: Option<Vec<String>>,
}

impl Default for AnthropicConfig {
    fn default() -> Self {
        Self {
            auth: None,
            base_url: ANTHROPIC_API_BASE.to_string(),
            version: ANTHROPIC_API_VERSION.to_string(),
            beta: None,
        }
    }
}

impl AnthropicConfig {
    #[must_use]
    pub fn new() -> Self {
        AnthropicConfig::default()
    }

    #[must_use]
    pub fn with_api_key<S: Into<String>>(mut self, api_key: Option<S>) -> Self {
        if let Some(api_key) = api_key {
            self.auth = Some(AnthropicAuthMechanism::ApiKey(Secret::new(api_key.into())));
        }
        self
    }

    #[must_use]
    pub fn with_auth_token<S: Into<String>>(mut self, auth_token: Option<S>) -> Self {
        if let Some(auth_token) = auth_token {
            self.auth = Some(AnthropicAuthMechanism::AuthToken(Secret::new(
                auth_token.into(),
            )));
        }
        self
    }

    #[must_use]
    pub fn with_base_url<S: Into<String>>(mut self, base_url: Option<S>) -> Self {
        if let Some(base_url) = base_url {
            self.base_url = base_url.into();
        }
        self
    }

    #[must_use]
    pub fn with_version<S: Into<String>>(mut self, version: Option<S>) -> Self {
        if let Some(version) = version {
            self.version = version.into();
        }
        self
    }

    #[must_use]
    pub fn with_beta(mut self, beta: Vec<String>) -> Self {
        self.beta = Some(beta);
        self
    }
}

#[derive(Clone)]
pub enum AnthropicAuthMechanism {
    ApiKey(Secret<String>),
    AuthToken(Secret<String>),
}

// Requires `.expect(` to maintain `Config` trait's method signature.
// Expectation is that values in `AnthropicAuthMechanism` are valid headers, see:
// `<https://github.com/hyperium/http/blob/761d36acb069ed335d2f9dfd7a568b8735ec7fec/src/header/value.rs#L605>`
#[allow(clippy::expect_used)]
impl Config for AnthropicConfig {
    fn headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        match &self.auth {
            Some(AnthropicAuthMechanism::ApiKey(api_key)) => {
                headers.insert(
                    "x-api-key",
                    HeaderValue::from_str(api_key.expose_secret()).expect("Invalid API key"),
                );
            }
            Some(AnthropicAuthMechanism::AuthToken(auth_token)) => {
                headers.insert(
                    AUTHORIZATION,
                    HeaderValue::from_str(
                        format!("Bearer {}", auth_token.expose_secret()).as_str(),
                    )
                    .expect("Invalid auth token"),
                );
            }
            None => {}
        }

        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(
            "anthropic-version",
            HeaderValue::from_str(self.version.as_str()).expect("Invalid version"),
        );
        if let Some(beta) = &self.beta {
            headers.insert(
                "anthropic-beta",
                HeaderValue::from_str(beta.join(",").as_str()).expect("Invalid anthropic-beta"),
            );
        }
        headers
    }

    fn url(&self, path: &str) -> String {
        format!("{}/{}", self.api_base(), path)
    }

    fn query(&self) -> Vec<(&str, &str)> {
        vec![]
    }

    fn api_base(&self) -> &str {
        &self.base_url
    }

    fn api_key(&self) -> &Secret<String> {
        // This is a bit of a hack, but this method is not used anywhere.
        match &self.auth {
            Some(AnthropicAuthMechanism::ApiKey(api_key)) => api_key,
            Some(AnthropicAuthMechanism::AuthToken(auth_token)) => auth_token,
            None => &DUMMY_API_KEY,
        }
    }
}
