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

use crate::error::{HttpSnafu, Result};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use snafu::ResultExt;
use std::sync::Arc;
use token_provider::TokenProvider;

#[expect(clippy::struct_field_names)]
#[derive(Clone)]
pub struct Client {
    http_client: reqwest::Client,
    /// Sources the `OAuth2` access token sent as `Authorization: Bearer <token>` — for Vertex
    /// AI, a GCP service account JWT-bearer exchange.
    token_provider: Arc<dyn TokenProvider>,
    base_url: String,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("http_client", &self.http_client)
            .field("token_provider", &"[REDACTED]")
            .field("base_url", &self.base_url)
            .finish()
    }
}

impl Client {
    /// Builds a client that authenticates via an `OAuth2` Bearer token sourced from
    /// `token_provider`. `base_url` should be the project/location-scoped Vertex AI base, e.g.
    /// `https://{location}-aiplatform.googleapis.com/v1/projects/{project}/locations/{location}/publishers/google`.
    pub fn with_bearer_token(
        token_provider: Arc<dyn TokenProvider>,
        base_url: impl Into<String>,
    ) -> Result<Self> {
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
            token_provider,
            base_url: base_url.into(),
        })
    }

    pub(crate) fn http_client(&self) -> &reqwest::Client {
        &self.http_client
    }

    pub(crate) fn build_url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    /// Attaches this client's `Authorization: Bearer` header to outgoing request headers.
    pub(crate) fn auth_headers(&self, mut headers: HeaderMap) -> HeaderMap {
        if let Ok(mut value) =
            HeaderValue::from_str(&format!("Bearer {}", self.token_provider.get_token()))
        {
            value.set_sensitive(true);
            headers.insert(AUTHORIZATION, value);
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
    fn the_access_token_is_sent_as_a_bearer_header() {
        let token_provider: Arc<dyn TokenProvider> =
            Arc::new(StaticTokenProvider::new(SecretString::from("vertex-token")));
        let client = Client::with_bearer_token(token_provider, "https://example.invalid/v1")
            .expect("client should build");
        let headers = client.auth_headers(HeaderMap::new());

        assert_eq!(
            headers.get(AUTHORIZATION).and_then(|v| v.to_str().ok()),
            Some("Bearer vertex-token")
        );
    }

    #[test]
    fn the_bearer_header_is_marked_sensitive_so_it_stays_out_of_logs() {
        let token_provider: Arc<dyn TokenProvider> =
            Arc::new(StaticTokenProvider::new(SecretString::from("vertex-token")));
        let client = Client::with_bearer_token(token_provider, "https://example.invalid/v1")
            .expect("client should build");
        let headers = client.auth_headers(HeaderMap::new());

        assert!(
            headers
                .get(AUTHORIZATION)
                .expect("the Authorization header should be set")
                .is_sensitive()
        );
    }
}
