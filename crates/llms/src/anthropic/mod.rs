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

pub struct Anthropic {
    client: Client<AnthropicConfig>,
    model: String,
}

static ANTHROPIC_API_BASE: &str = "https://api.anthropic.com";
static ANTHROPIC_API_VERSION: &str = "2023-06-01";
static DUMMY_API_KEY: LazyLock<Secret<String>> = LazyLock::new(|| Secret::new(String::new()));

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
    pub fn new() -> Self {
        Default::default()
    }

    pub fn with_api_key<S: Into<String>>(mut self, api_key: S) -> Self {
        self.auth = Some(AnthropicAuthMechanism::ApiKey(Secret::new(api_key.into())));
        self
    }

    pub fn with_auth_token<S: Into<String>>(mut self, auth_token: S) -> Self {
        self.auth = Some(AnthropicAuthMechanism::AuthToken(Secret::new(
            auth_token.into(),
        )));
        self
    }

    pub fn with_base_url<S: Into<String>>(mut self, base_url: S) -> Self {
        self.base_url = base_url.into();
        self
    }

    pub fn with_version<S: Into<String>>(mut self, version: S) -> Self {
        self.version = version.into();
        self
    }

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

impl Config for AnthropicConfig {
    fn headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        match &self.auth {
            Some(AnthropicAuthMechanism::ApiKey(api_key)) => {
                headers.insert(
                    "x-api-key",
                    HeaderValue::from_str(api_key.expose_secret()).unwrap(),
                );
            }
            Some(AnthropicAuthMechanism::AuthToken(auth_token)) => {
                headers.insert(
                    AUTHORIZATION,
                    HeaderValue::from_str(
                        format!("Bearer {}", auth_token.expose_secret()).as_str(),
                    )
                    .unwrap(),
                );
            }
            None => {}
        }

        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(
            "anthropic-version",
            HeaderValue::from_str(self.version.as_str()).unwrap(),
        );
        if let Some(beta) = &self.beta {
            headers.insert(
                "anthropic-beta",
                HeaderValue::from_str(beta.join(",").as_str()).unwrap(),
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
