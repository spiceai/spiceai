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
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use secrecy::SecretString;
use std::sync::Arc;
use token_provider::{StaticTokenProvider, TokenProvider};

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

impl HostedModelConfig {
    pub fn from_url(url: &str) -> Self {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        Self {
            auth: None,
            base_url: url.to_string(),
            default_headers: headers,
        }
    }

    /// Set the API key for authentication.
    #[must_use]
    pub fn with_api_key<S: Into<String>>(mut self, api_key: Option<S>) -> Self {
        if let Some(key) = api_key {
            self.auth = Some(GenericAuthMechanism::from_api_key(key));
        }
        self
    }

    /// Set the bearer token for authentication.
    #[must_use]
    pub fn with_bearer_token<S: Into<String>>(mut self, token: Option<S>) -> Self {
        if let Some(token) = token {
            self.auth = Some(GenericAuthMechanism::from_bearer_token(token));
        }
        self
    }

    #[must_use]
    pub fn with_auth(mut self, auth: GenericAuthMechanism) -> Self {
        self.auth = Some(auth);
        self
    }

    /// Add (or override) a default header.
    #[must_use]
    pub fn with_header(mut self, key: &'static str, value: &'static str) -> Self {
        self = self.with_header_value(key, HeaderValue::from_static(value));
        self
    }

    #[must_use]
    pub fn with_header_value(mut self, key: &'static str, value: HeaderValue) -> Self {
        self.default_headers
            .insert(HeaderName::from_static(key), value);
        self
    }
}

/// A generic authentication mechanism that supports either an API key or a Bearer token.
#[derive(Clone, Debug)]
pub enum GenericAuthMechanism {
    ApiKey(Arc<dyn TokenProvider>),
    BearerToken(Arc<dyn TokenProvider>),
}

impl GenericAuthMechanism {
    pub fn from_api_key<S: Into<String>>(api_key: S) -> Self {
        GenericAuthMechanism::from_api_key_provider(Arc::new(StaticTokenProvider::new(
            SecretString::new(api_key.into().into()),
        )))
    }
    pub fn from_api_key_provider(provider: Arc<dyn TokenProvider>) -> Self {
        Self::ApiKey(provider)
    }

    pub fn from_bearer_token<S: Into<String>>(bearer_token: S) -> Self {
        GenericAuthMechanism::from_bearer_token_provider(Arc::new(StaticTokenProvider::new(
            SecretString::from(bearer_token.into()),
        )))
    }
    pub fn from_bearer_token_provider(provider: Arc<dyn TokenProvider>) -> Self {
        Self::BearerToken(provider)
    }
}

impl Config for HostedModelConfig {
    fn headers(&self) -> HeaderMap {
        let mut headers = self.default_headers.clone();

        // Insert authentication header if available.
        if let Some(auth) = &self.auth {
            match auth {
                GenericAuthMechanism::ApiKey(prov) => {
                    match HeaderValue::from_str(prov.get_token().as_str()) {
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
                GenericAuthMechanism::BearerToken(prov) => {
                    match HeaderValue::from_str(&format!("Bearer {}", prov.get_token())) {
                        Ok(value) => {
                            headers.insert(AUTHORIZATION, value);
                        }
                        Err(_) => {
                            tracing::warn!(
                                "Invalid bearer token given for 'Authorization' header. Will not use"
                            );
                        }
                    }
                }
            }
        }

        headers
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url.as_str())
    }

    fn query(&self) -> Vec<(&str, &str)> {
        Vec::new()
    }

    fn api_base(&self) -> &str {
        self.base_url.as_str()
    }

    fn api_key(&self) -> Arc<SecretString> {
        match &self.auth {
            Some(GenericAuthMechanism::BearerToken(prov) | GenericAuthMechanism::ApiKey(prov)) => {
                Arc::new(SecretString::from(prov.get_token()))
            }
            _ => Arc::new(SecretString::from(String::new())),
        }
    }
}
