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

//! SAML 2.0 Bearer Assertion Grant (RFC 7522) for Microsoft Entra / Azure AD.
//!
//! Exchanges a pre-acquired SAML assertion (from a federated IdP such as
//! Okta, PingFederate, or ADFS) for an OAuth2 access token at:
//!
//!     POST https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token
//!     grant_type=urn:ietf:params:oauth:grant-type:saml2-bearer
//!     assertion={base64url SAML}
//!     client_id={app-registration-id}
//!     scope={scope}
//!
//! The SAML assertion must be base64url-encoded per RFC 7522 §3. The Azure AD
//! app registration must be configured for SAML bearer grant and have a trust
//! relationship with the IdP that issued the assertion.

#![expect(
    clippy::doc_markdown,
    reason = "prose-frequent identifiers (IdP, OAuth2, PingFederate) are clearer without backticks"
)]

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use base64::{Engine as _, engine::general_purpose};
use reqwest::Client;
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use snafu::{ResultExt, Snafu};
use tokio::sync::{Mutex, RwLock};

/// Default grace period before expiry when we consider the token stale and
/// eagerly re-exchange.
const EXPIRY_GRACE: Duration = Duration::from_mins(1);

/// RFC 7522 grant type URI.
const GRANT_TYPE: &str = "urn:ietf:params:oauth:grant-type:saml2-bearer";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("HTTP error contacting Azure AD token endpoint '{url}': {source}"))]
    Http { url: String, source: reqwest::Error },

    #[snafu(display(
        "Azure AD rejected the SAML assertion ({code}): {description}. See https://learn.microsoft.com/en-us/azure/active-directory/develop/reference-aadsts-error-codes"
    ))]
    TokenRejected { code: String, description: String },

    #[snafu(display("Azure AD returned an unexpected response: {source}"))]
    ResponseParse { source: reqwest::Error },

    #[snafu(display(
        "SAML assertion is not valid base64url. RFC 7522 requires base64url-encoded assertions: {source}"
    ))]
    AssertionEncoding { source: base64::DecodeError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone)]
pub struct SamlBearerConfig {
    /// Azure AD tenant ID or alias (e.g. `contoso.onmicrosoft.com` or a GUID).
    pub tenant_id: String,
    /// App registration client ID with SAML assertion grant enabled and an IdP
    /// federation configured.
    pub client_id: String,
    /// SAML assertion, base64url-encoded per RFC 7522 §3.
    pub assertion: SecretString,
    /// OAuth2 scope to request. Defaults to Microsoft Graph's `.default`.
    pub scope: Option<String>,
    /// Override the Azure AD base URL (`https://login.microsoftonline.com`).
    /// Test-only hook — production callers leave this `None` to use Microsoft's
    /// real endpoint. Non-`None` values include the scheme and host only;
    /// the `/{tenant}/oauth2/v2.0/token` path is appended by the flow.
    pub authority_host_override: Option<String>,
}

/// A SAML bearer flow, plus a cached access token.
pub struct SamlBearerFlow {
    config: SamlBearerConfig,
    http: Client,
    cache: Arc<RwLock<Option<AcquiredToken>>>,
    /// Serializes concurrent token refreshes so only one caller hits Azure
    /// AD when the cache expires. The cache `RwLock` itself is never held
    /// across the HTTP exchange, so it doesn't block readers during refresh.
    refresh_lock: Arc<Mutex<()>>,
}

#[derive(Debug, Clone)]
pub struct AcquiredToken {
    pub access_token: SecretString,
    pub expires_at: Instant,
}

impl AcquiredToken {
    #[must_use]
    pub fn is_fresh(&self) -> bool {
        Instant::now() + EXPIRY_GRACE < self.expires_at
    }
}

#[derive(Debug, Deserialize)]
struct TokenSuccess {
    access_token: String,
    expires_in: u64,
}

#[derive(Debug, Default, Deserialize)]
struct TokenError {
    error: String,
    #[serde(default)]
    error_description: String,
}

impl SamlBearerFlow {
    #[must_use]
    pub fn new(config: SamlBearerConfig) -> Self {
        Self {
            config,
            http: Client::new(),
            cache: Arc::new(RwLock::new(None)),
            refresh_lock: Arc::new(Mutex::new(())),
        }
    }

    /// Return a valid access token, re-exchanging the assertion if the cached
    /// one is missing or stale. Callers that just want a one-shot exchange
    /// without caching can call [`Self::exchange_once`].
    pub async fn acquire_token(&self) -> Result<AcquiredToken> {
        // Fast path: read lock only — most calls hit a fresh cached token.
        {
            let cached = self.cache.read().await;
            if let Some(tok) = &*cached
                && tok.is_fresh()
            {
                return Ok(tok.clone());
            }
        }
        // Contended / refresh path: serialize on `refresh_lock` so only one
        // caller hits Azure AD even if many callers see a stale cache at
        // once (no thundering herd). The cache `RwLock` is dropped before
        // `exchange_once()` and reacquired briefly to write, so concurrent
        // readers never block on the network round-trip.
        let _refresh_guard = self.refresh_lock.lock().await;
        // Re-check: another caller may have refreshed while we were
        // waiting on `refresh_lock`.
        {
            let cached = self.cache.read().await;
            if let Some(tok) = &*cached
                && tok.is_fresh()
            {
                return Ok(tok.clone());
            }
        }
        let fresh = self.exchange_once().await?;
        *self.cache.write().await = Some(fresh.clone());
        Ok(fresh)
    }

    /// Perform the SAML → OAuth2 token exchange once, bypassing the cache.
    pub async fn exchange_once(&self) -> Result<AcquiredToken> {
        validate_assertion_encoding(self.config.assertion.expose_secret())?;

        let base = self
            .config
            .authority_host_override
            .as_deref()
            .unwrap_or("https://login.microsoftonline.com");
        let url = format!(
            "{}/{}/oauth2/v2.0/token",
            base.trim_end_matches('/'),
            self.config.tenant_id
        );
        let scope = self
            .config
            .scope
            .clone()
            .unwrap_or_else(|| super::DEFAULT_SCOPE.to_string());

        let params: [(&str, &str); 4] = [
            ("grant_type", GRANT_TYPE),
            ("assertion", self.config.assertion.expose_secret()),
            ("client_id", &self.config.client_id),
            ("scope", &scope),
        ];

        let resp = self
            .http
            .post(&url)
            .form(&params)
            .send()
            .await
            .context(HttpSnafu { url: url.clone() })?;

        if !resp.status().is_success() {
            let err: TokenError = resp.json().await.unwrap_or_else(|_| TokenError {
                error: "unknown".into(),
                error_description: "Azure AD returned a non-JSON error body".into(),
            });
            return Err(Error::TokenRejected {
                code: err.error,
                description: err.error_description,
            });
        }

        let body: TokenSuccess = resp.json().await.context(ResponseParseSnafu)?;
        Ok(AcquiredToken {
            access_token: SecretString::new(body.access_token.into()),
            expires_at: Instant::now() + Duration::from_secs(body.expires_in),
        })
    }
}

/// Verify that the assertion string is valid base64url. This catches the most
/// common misconfiguration (plain base64 vs base64url) early, before it hits
/// Azure AD.
fn validate_assertion_encoding(assertion: &str) -> Result<()> {
    general_purpose::URL_SAFE_NO_PAD
        .decode(assertion.trim_end_matches('='))
        .map(|_| ())
        .or_else(|_| general_purpose::URL_SAFE.decode(assertion).map(|_| ()))
        .context(AssertionEncodingSnafu)
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[test]
    fn base64url_assertion_validates() {
        // "hello world" in base64url
        let s = general_purpose::URL_SAFE_NO_PAD.encode("hello world");
        validate_assertion_encoding(&s).expect("base64url assertion should validate");
    }

    #[test]
    fn padded_base64url_also_validates() {
        let s = general_purpose::URL_SAFE.encode("hi");
        validate_assertion_encoding(&s).expect("padded base64url assertion should validate");
    }

    #[test]
    fn rejects_bogus_assertion() {
        let err = validate_assertion_encoding("!!not-base64!!")
            .expect_err("invalid base64 assertion should be rejected");
        assert!(matches!(err, Error::AssertionEncoding { .. }));
    }

    /// Minimal canned-response HTTP mock for the Azure AD token endpoint.
    /// Mirrors the pattern used by `crates/data_components/src/http/auth.rs`.
    struct MockResponse {
        status: &'static str,
        body: String,
    }

    async fn start_mock_ad(
        responses: Vec<MockResponse>,
    ) -> (
        String,
        Arc<AtomicUsize>,
        Arc<tokio::sync::Mutex<Vec<String>>>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock listener");
        let addr = listener.local_addr().expect("local_addr");
        let responses = Arc::new(tokio::sync::Mutex::new(VecDeque::from(responses)));
        let count = Arc::new(AtomicUsize::new(0));
        let captured = Arc::new(tokio::sync::Mutex::new(Vec::new()));

        let count_srv = Arc::clone(&count);
        let captured_srv = Arc::clone(&captured);
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let responses = Arc::clone(&responses);
                let count = Arc::clone(&count_srv);
                let captured = Arc::clone(&captured_srv);
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let mut buf = [0u8; 4096];
                    let mut raw = Vec::new();
                    let mut header_end: Option<usize> = None;
                    let mut content_length = 0usize;
                    loop {
                        let n = match stream.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => n,
                        };
                        raw.extend_from_slice(&buf[..n]);
                        if header_end.is_none()
                            && let Some(i) =
                                raw.windows(4).position(|w| w == b"\r\n\r\n").map(|j| j + 4)
                        {
                            header_end = Some(i);
                            let header_text = String::from_utf8_lossy(&raw[..i]);
                            content_length = header_text
                                .lines()
                                .find_map(|line| {
                                    let (k, v) = line.split_once(':')?;
                                    k.trim()
                                        .eq_ignore_ascii_case("Content-Length")
                                        .then(|| v.trim().parse::<usize>().ok())
                                        .flatten()
                                })
                                .unwrap_or(0);
                        }
                        if let Some(end) = header_end
                            && raw.len() >= end + content_length
                        {
                            break;
                        }
                    }
                    count.fetch_add(1, Ordering::SeqCst);
                    captured
                        .lock()
                        .await
                        .push(String::from_utf8_lossy(&raw).into_owned());
                    let resp = responses.lock().await.pop_front().unwrap_or(MockResponse {
                        status: "500 Internal Server Error",
                        body: "{}".into(),
                    });
                    let body = resp.body;
                    let http = format!(
                        "HTTP/1.1 {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                        resp.status,
                        body.len(),
                        body,
                    );
                    let _ = stream.write_all(http.as_bytes()).await;
                });
            }
        });
        (format!("http://{addr}"), count, captured)
    }

    fn assertion() -> SecretString {
        SecretString::new(
            general_purpose::URL_SAFE_NO_PAD
                .encode(b"<saml:Assertion/>")
                .into(),
        )
    }

    #[tokio::test]
    async fn saml_bearer_exchange_success() {
        let (base, count, captured) = start_mock_ad(vec![MockResponse {
            status: "200 OK",
            body: r#"{"access_token":"eyJ.TEST","expires_in":3599,"token_type":"Bearer"}"#
                .to_string(),
        }])
        .await;
        let flow = SamlBearerFlow::new(SamlBearerConfig {
            tenant_id: "tenant-guid".into(),
            client_id: "client-guid".into(),
            assertion: assertion(),
            scope: None,
            authority_host_override: Some(base.clone()),
        });
        let token = flow.exchange_once().await.expect("exchange_once");
        assert_eq!(token.access_token.expose_secret(), "eyJ.TEST");
        assert!(token.is_fresh());
        assert_eq!(count.load(Ordering::SeqCst), 1);

        let req = &captured.lock().await[0];
        assert!(req.starts_with("POST /tenant-guid/oauth2/v2.0/token"));
        assert!(req.contains("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Asaml2-bearer"));
        assert!(req.contains("client_id=client-guid"));
        assert!(req.contains("scope=https%3A%2F%2Fgraph.microsoft.com%2F.default"));
    }

    #[tokio::test]
    async fn saml_bearer_cache_prevents_second_exchange() {
        let (base, count, _captured) = start_mock_ad(vec![MockResponse {
            status: "200 OK",
            body: r#"{"access_token":"T1","expires_in":3599,"token_type":"Bearer"}"#.to_string(),
        }])
        .await;
        let flow = SamlBearerFlow::new(SamlBearerConfig {
            tenant_id: "t".into(),
            client_id: "c".into(),
            assertion: assertion(),
            scope: None,
            authority_host_override: Some(base),
        });
        let t1 = flow
            .acquire_token()
            .await
            .expect("first acquire_token should succeed");
        let t2 = flow
            .acquire_token()
            .await
            .expect("second acquire_token should hit cache and succeed");
        assert_eq!(t1.access_token.expose_secret(), "T1");
        assert_eq!(t2.access_token.expose_secret(), "T1");
        // Cache hit on the second call — only one HTTP request total.
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn saml_bearer_surfaces_aad_error_code() {
        let (base, _count, _captured) = start_mock_ad(vec![MockResponse {
            status: "400 Bad Request",
            body: r#"{"error":"invalid_grant","error_description":"AADSTS700024: SAML assertion expired"}"#.to_string(),
        }])
        .await;
        let flow = SamlBearerFlow::new(SamlBearerConfig {
            tenant_id: "t".into(),
            client_id: "c".into(),
            assertion: assertion(),
            scope: None,
            authority_host_override: Some(base),
        });
        let err = flow
            .exchange_once()
            .await
            .expect_err("token exchange should fail on rejection");
        match err {
            Error::TokenRejected { code, description } => {
                assert_eq!(code, "invalid_grant");
                assert!(description.contains("AADSTS700024"));
            }
            other => panic!("expected TokenRejected, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn saml_bearer_honors_custom_scope() {
        let (base, _count, captured) = start_mock_ad(vec![MockResponse {
            status: "200 OK",
            body: r#"{"access_token":"X","expires_in":3599,"token_type":"Bearer"}"#.into(),
        }])
        .await;
        let flow = SamlBearerFlow::new(SamlBearerConfig {
            tenant_id: "t".into(),
            client_id: "c".into(),
            assertion: assertion(),
            scope: Some("https://custom.example/.default offline_access".into()),
            authority_host_override: Some(base),
        });
        let _ = flow
            .exchange_once()
            .await
            .expect("exchange_once with custom scope should succeed");
        let req = &captured.lock().await[0];
        assert!(
            req.contains("scope=https%3A%2F%2Fcustom.example%2F.default"),
            "request body should contain custom scope; got: {req}"
        );
    }
}
