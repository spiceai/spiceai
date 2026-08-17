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

//! Google Generative AI API client
#![allow(clippy::missing_errors_doc)]

use crate::error::{Error, HttpSnafu, Result};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use snafu::ResultExt;
use std::sync::Arc;
use token_provider::TokenProvider;

const BASE_URL: &str = "https://generativelanguage.googleapis.com/v1beta";
const API_KEY_HEADER: &str = "x-goog-api-key";

/// How the client authenticates outgoing requests.
#[derive(Clone)]
enum AuthMode {
    /// Google AI Studio: an API key sent as the `x-goog-api-key` header.
    ApiKey(String),
    /// Vertex AI: an `OAuth2` access token sent as `Authorization: Bearer <token>`,
    /// sourced from a [`TokenProvider`] (e.g. a GCP service account JWT-bearer exchange).
    Bearer(Arc<dyn TokenProvider>),
}

#[expect(clippy::struct_field_names)]
#[derive(Clone)]
pub struct Client {
    http_client: reqwest::Client,
    auth: AuthMode,
    base_url: String,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("http_client", &self.http_client)
            .field(
                "auth",
                match &self.auth {
                    AuthMode::ApiKey(_) => &"ApiKey([REDACTED])",
                    AuthMode::Bearer(_) => &"Bearer([REDACTED])",
                },
            )
            .field("base_url", &self.base_url)
            .finish()
    }
}

impl Client {
    pub fn new(api_key: impl Into<String>) -> Result<Self> {
        Self::with_base_url(api_key, BASE_URL)
    }

    pub fn with_base_url(api_key: impl Into<String>, base_url: impl Into<String>) -> Result<Self> {
        let api_key = api_key.into();
        if api_key.is_empty() {
            return Err(Error::InvalidApiKey);
        }

        Self::build(AuthMode::ApiKey(api_key), base_url)
    }

    /// Builds a client that authenticates via an `OAuth2` Bearer token sourced from
    /// `token_provider` (Vertex AI mode). `base_url` should be the project/location-scoped
    /// Vertex AI base, e.g.
    /// `https://{location}-aiplatform.googleapis.com/v1/projects/{project}/locations/{location}/publishers/google`.
    pub fn with_bearer_token(
        token_provider: Arc<dyn TokenProvider>,
        base_url: impl Into<String>,
    ) -> Result<Self> {
        Self::build(AuthMode::Bearer(token_provider), base_url)
    }

    fn build(auth: AuthMode, base_url: impl Into<String>) -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        let http_client = reqwest::Client::builder()
            .default_headers(headers)
            .connect_timeout(std::time::Duration::from_secs(10))
            .timeout(std::time::Duration::from_mins(5))
            .build()
            .context(HttpSnafu)?;

        Ok(Self {
            http_client,
            auth,
            base_url: base_url.into(),
        })
    }

    pub(crate) fn http_client(&self) -> &reqwest::Client {
        &self.http_client
    }

    pub(crate) fn build_url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    /// Whether this client talks to Vertex AI (Bearer-token auth) rather than Google AI
    /// Studio. Vertex AI's embedding models use a different request/response contract
    /// (`:predict`) than the Gemini Developer API's `:embedContent`/`:batchEmbedContents` —
    /// see `embeddings.rs`.
    pub(crate) fn is_vertex(&self) -> bool {
        matches!(self.auth, AuthMode::Bearer(_))
    }

    /// Attaches this client's authentication to outgoing request headers: an
    /// `x-goog-api-key` header for Google AI Studio, or an `Authorization: Bearer`
    /// header for Vertex AI.
    pub(crate) fn auth_headers(&self, mut headers: HeaderMap) -> HeaderMap {
        match &self.auth {
            AuthMode::ApiKey(key) => {
                if let Ok(value) = HeaderValue::from_str(key) {
                    headers.insert(API_KEY_HEADER, value);
                }
            }
            AuthMode::Bearer(token_provider) => {
                if let Ok(mut value) =
                    HeaderValue::from_str(&format!("Bearer {}", token_provider.get_token()))
                {
                    value.set_sensitive(true);
                    headers.insert(AUTHORIZATION, value);
                }
            }
        }
        headers
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::SecretString;
    use token_provider::StaticTokenProvider;

    #[test]
    fn api_key_mode_sets_goog_api_key_header_only() {
        let client = Client::new("test-api-key").expect("client should build");
        let headers = client.auth_headers(HeaderMap::new());

        assert_eq!(
            headers.get(API_KEY_HEADER).and_then(|v| v.to_str().ok()),
            Some("test-api-key")
        );
        assert!(headers.get(AUTHORIZATION).is_none());
    }

    #[test]
    fn bearer_mode_sets_authorization_header_only() {
        let token_provider: Arc<dyn TokenProvider> =
            Arc::new(StaticTokenProvider::new(SecretString::from("vertex-token")));
        let client = Client::with_bearer_token(token_provider, "https://example.invalid/v1")
            .expect("client should build");
        let headers = client.auth_headers(HeaderMap::new());

        assert_eq!(
            headers.get(AUTHORIZATION).and_then(|v| v.to_str().ok()),
            Some("Bearer vertex-token")
        );
        assert!(headers.get(API_KEY_HEADER).is_none());
    }

    #[test]
    fn empty_api_key_is_rejected() {
        assert!(matches!(Client::new(""), Err(Error::InvalidApiKey)));
    }
}
