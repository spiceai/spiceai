/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! The Google Cloud metadata server as a source of identity tokens. Every Google Cloud compute
//! environment with an attached service account (Compute Engine, Cloud Run, Cloud Functions, App
//! Engine, and GKE with Workload Identity) can request a Google-signed identity token for an
//! audience from it, issued by `https://accounts.google.com`, which a provider's federation rule
//! can trust.

use std::time::Duration;

use async_trait::async_trait;
use secrecy::SecretString;

use crate::workload_identity::IdentityTokenSource;

/// The metadata server's host, which `GCE_METADATA_HOST` replaces, as it does for Google's
/// client libraries.
const DEFAULT_HOST: &str = "metadata.google.internal";

/// How long the metadata server has to accept a connection. Outside Google Cloud there is none,
/// and the wait is the cost of looking for one.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(1);

/// How long a request for a token may take in all.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Bounds the reason quoted from the metadata server's error response.
const MAX_REASON_LEN: usize = 300;

/// An identity token for `audience` from the Google Cloud metadata server, as the workload's
/// attached service account.
pub struct GoogleMetadataIdentity {
    http: reqwest::Client,
    base_url: String,
    audience: String,
}

impl GoogleMetadataIdentity {
    /// The metadata server of this process's environment: `GCE_METADATA_HOST`, or
    /// `metadata.google.internal`.
    ///
    /// # Errors
    ///
    /// When the HTTP client cannot be built.
    pub fn from_env(audience: impl Into<String>) -> Result<Self, String> {
        let host = std::env::var("GCE_METADATA_HOST")
            .ok()
            .filter(|host| !host.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_HOST.to_string());
        Self::new(format!("http://{}", host.trim()), audience)
    }

    /// The metadata server at `base_url`, such as `http://metadata.google.internal`.
    ///
    /// # Errors
    ///
    /// When the HTTP client cannot be built.
    pub fn new(base_url: impl Into<String>, audience: impl Into<String>) -> Result<Self, String> {
        let http = reqwest::Client::builder()
            .user_agent(util::spiceai_user_agent())
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            // The metadata server is link-local: a proxy cannot reach it, and a redirect has
            // nowhere legitimate to go.
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| {
                format!("Failed to create the HTTP client for the metadata server: {error}")
            })?;
        Ok(Self {
            http,
            base_url: base_url.into().trim_end_matches('/').to_string(),
            audience: audience.into(),
        })
    }
}

#[async_trait]
impl IdentityTokenSource for GoogleMetadataIdentity {
    async fn fetch(&self) -> Result<SecretString, String> {
        let response = self
            .http
            .get(format!(
                "{}/computeMetadata/v1/instance/service-accounts/default/identity",
                self.base_url
            ))
            // `format=full` adds the service account's `email` claim, which federation rules
            // match beside its numeric `sub`.
            .query(&[("audience", self.audience.as_str()), ("format", "full")])
            .header("Metadata-Flavor", "Google")
            .send()
            .await
            .map_err(|error| {
                if error.is_connect() || error.is_timeout() {
                    format!(
                        "no metadata server answered within {}s",
                        CONNECT_TIMEOUT.as_secs()
                    )
                } else {
                    format!("the metadata server could not be reached: {error}")
                }
            })?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|error| format!("failed to read the metadata server's answer: {error}"))?;
        if !status.is_success() {
            return Err(format!(
                "the metadata server refused (HTTP {status}): {}",
                reason(&body)
            ));
        }
        Ok(SecretString::from(body.trim().to_string()))
    }

    fn describe(&self) -> String {
        "the Google Cloud service account (metadata server)".to_string()
    }

    fn hash_key(&self) -> String {
        format!("google_metadata_{}_{}", self.base_url, self.audience)
    }
}

/// The metadata server's error body as a bounded clause on one line.
fn reason(body: &str) -> String {
    let text = util::single_line(body);
    let text = text.trim().trim_end_matches('.');
    match text.char_indices().nth(MAX_REASON_LEN) {
        Some((end, _)) => format!("{}…", &text[..end]),
        None => text.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use axum::{
        Router,
        extract::{Query, State},
        http::{HeaderMap, StatusCode},
        routing::get,
    };
    use secrecy::ExposeSecret;

    use super::*;

    type Requests = Arc<Mutex<Vec<(HashMap<String, String>, Option<String>)>>>;

    async fn identity(
        State(requests): State<Requests>,
        Query(query): Query<HashMap<String, String>>,
        headers: HeaderMap,
    ) -> (StatusCode, String) {
        let flavor = headers
            .get("metadata-flavor")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        requests
            .lock()
            .expect("lock")
            .push((query.clone(), flavor.clone()));
        if flavor.as_deref() != Some("Google") {
            return (
                StatusCode::FORBIDDEN,
                "Missing Metadata-Flavor:Google header.".to_string(),
            );
        }
        if query.get("audience").map(String::as_str) == Some("refuse") {
            return (
                StatusCode::NOT_FOUND,
                "No service account is attached to this instance.\n".to_string(),
            );
        }
        (StatusCode::OK, "google-jwt\n".to_string())
    }

    async fn start() -> (String, Requests) {
        let requests = Requests::default();
        let app = Router::new()
            .route(
                "/computeMetadata/v1/instance/service-accounts/default/identity",
                get(identity),
            )
            .with_state(Arc::clone(&requests));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the mock metadata server");
        let address = listener.local_addr().expect("its address");
        tokio::spawn(async move { axum::serve(listener, app).await });
        (format!("http://{address}"), requests)
    }

    #[tokio::test]
    async fn the_metadata_server_issues_a_full_token_for_the_audience() {
        let (base, requests) = start().await;
        let source =
            GoogleMetadataIdentity::new(base, "https://api.anthropic.com").expect("a source");
        let token = source.fetch().await.expect("a token");
        assert_eq!(token.expose_secret(), "google-jwt");
        let requests = requests.lock().expect("lock");
        let (query, flavor) = &requests[0];
        assert_eq!(
            (
                query.get("audience").map(String::as_str),
                query.get("format").map(String::as_str),
                flavor.as_deref()
            ),
            (
                Some("https://api.anthropic.com"),
                Some("full"),
                Some("Google")
            )
        );
    }

    #[tokio::test]
    async fn a_refusal_names_the_metadata_servers_reason() {
        let (base, _requests) = start().await;
        let source = GoogleMetadataIdentity::new(base, "refuse").expect("a source");
        assert_eq!(
            source.fetch().await.map(|_| ()),
            Err("the metadata server refused (HTTP 404 Not Found): No service account is attached to this instance".to_string())
        );
    }

    #[tokio::test]
    async fn no_metadata_server_is_reported_as_none_answering() {
        // Nothing listens on this port: the connection is refused at once.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let address = listener.local_addr().expect("its address");
        drop(listener);
        let source =
            GoogleMetadataIdentity::new(format!("http://{address}"), "aud").expect("a source");
        assert_eq!(
            source.fetch().await.map(|_| ()),
            Err("no metadata server answered within 1s".to_string())
        );
    }
}
