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

use async_openai::config::Config;
use reqwest::header::{
    HeaderMap, HeaderName, HeaderValue, InvalidHeaderValue, AUTHORIZATION, CONTENT_TYPE,
};
use secrecy::{ExposeSecret, Secret};
use std::sync::LazyLock;

static DUMMY_API_KEY: LazyLock<Secret<String>> = LazyLock::new(|| Secret::new(String::new()));

/// A generic configuration for any hosted `OpenAI` API client.
///
/// This configuration supports two authentication mechanisms (API key or Bearer token)
/// and allows you to set the base URL and add arbitrary default headers.
#[derive(Clone, Debug)]
pub struct HostedModelConfig {
    pub auth: Option<GenericAuthMechanism>,
    pub base_url: String,
    pub default_headers: HeaderMap,
}

impl Default for HostedModelConfig {
    fn default() -> Self {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        Self {
            auth: None,
            base_url: "http://localhost:8090/v1".to_string(),
            default_headers: headers,
        }
    }
}

impl HostedModelConfig {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the API key for authentication.
    #[must_use]
    pub fn with_api_key<S: Into<String>>(mut self, api_key: Option<S>) -> Self {
        if let Some(key) = api_key {
            self.auth = Some(GenericAuthMechanism::ApiKey(Secret::new(key.into())));
        }
        self
    }

    /// Set the bearer token for authentication.
    #[must_use]
    pub fn with_bearer_token<S: Into<String>>(mut self, token: Option<S>) -> Self {
        if let Some(token) = token {
            self.auth = Some(GenericAuthMechanism::BearerToken(Secret::new(token.into())));
        }
        self
    }

    /// Override the base URL for API calls.
    #[must_use]
    pub fn with_base_url<S: Into<String>>(mut self, base_url: S) -> Self {
        self.base_url = base_url.into();
        self
    }

    #[must_use]
    pub fn with_auth(mut self, auth: GenericAuthMechanism) -> Self {
        self.auth = Some(auth);
        self
    }

    /// Add (or override) a default header.
    pub fn with_header<V>(mut self, key: &'static str, value: V) -> Result<Self, InvalidHeaderValue>
    where
        V: Into<String>,
    {
        self.default_headers.insert(
            HeaderName::from_static(key),
            HeaderValue::from_str(&value.into())?,
        );
        Ok(self)
    }
}

/// A generic authentication mechanism that supports either an API key or a Bearer token.
#[derive(Clone, Debug)]
pub enum GenericAuthMechanism {
    ApiKey(Secret<String>),
    BearerToken(Secret<String>),
}

impl GenericAuthMechanism {
    pub fn from_api_key<S: Into<String>>(api_key: S) -> Self {
        Self::ApiKey(Secret::new(api_key.into()))
    }
    pub fn from_bearer_token<S: Into<String>>(bearer_token: S) -> Self {
        Self::BearerToken(Secret::new(bearer_token.into()))
    }
}

impl Config for HostedModelConfig {
    fn headers(&self) -> HeaderMap {
        let mut headers = self.default_headers.clone();

        // Insert authentication header if available.
        if let Some(auth) = &self.auth {
            match auth {
                GenericAuthMechanism::ApiKey(key) => {
                    match HeaderValue::from_str(key.expose_secret()) {
                        Ok(value) => {
                            headers.insert("x-api-key", value);
                        }
                        Err(_) => {
                            tracing::warn!(
                                "Invalid API key given for 'x-api-key' header. Will not use"
                            );
                        }
                    }
                }
                GenericAuthMechanism::BearerToken(token) => {
                    match HeaderValue::from_str(&format!("Bearer {}", token.expose_secret())) {
                        Ok(value) => {
                            headers.insert(AUTHORIZATION, value);
                        }
                        Err(_) => {
                            tracing::warn!(
                                "Invalid bearer token given for 'Authorization' header. Will not use"
                            );
                        }
                    };
                }
            }
        }

        headers
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    fn query(&self) -> Vec<(&str, &str)> {
        Vec::new()
    }

    fn api_base(&self) -> &str {
        &self.base_url
    }

    fn api_key(&self) -> &Secret<String> {
        // This is a bit of a hack, will result in valid, understandable auth errors.
        match &self.auth {
            Some(GenericAuthMechanism::ApiKey(key)) => key,
            Some(GenericAuthMechanism::BearerToken(token)) => token,
            None => &DUMMY_API_KEY,
        }
    }
}
