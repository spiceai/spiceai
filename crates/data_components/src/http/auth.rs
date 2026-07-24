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

//! HTTP request authenticators for the HTTP(S) data connector.
//!
//! The module exposes an [`HttpAuthenticator`] trait that [`crate::http::provider::HttpTableProvider`]
//! uses to decorate outgoing requests. The main implementation is
//! [`OAuth2Auth`], which acquires short-lived access tokens from a configured
//! token endpoint and refreshes them in the background before they expire. Two
//! `OAuth2` grants are supported (see [`OAuthGrant`]):
//!
//! - the refresh-token grant (RFC 6749 §6): exchange a long-lived refresh token
//!   for access tokens; the endpoint may rotate the refresh token;
//! - the client-credentials grant (RFC 6749 §4.4): authenticate as the client
//!   with `client_id`/`client_secret`; no refresh token is issued, so each
//!   refresh simply re-runs the same exchange (e.g. Shopify Admin API).
//!
//! By default the access token is attached as `Authorization: Bearer <token>`,
//! but the header name is configurable via [`TokenHeader`]; a non-`Authorization`
//! header carries the bare token, so non-standard schemes work too (e.g.
//! Shopify's `X-Shopify-Access-Token: <token>`).

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use reqwest::{
    Client, ClientBuilder, RequestBuilder,
    header::{AUTHORIZATION, HeaderName, HeaderValue},
};
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use snafu::prelude::*;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use url::Url;
use util::fibonacci_backoff::{Backoff as _, FibonacciBackoffBuilder};

use crate::resilient_http::{
    configure_client_builder, read_bounded_error_body, send_request_with_retry,
};

/// Refresh the access token this many seconds before it is reported to expire.
const TOKEN_REFRESH_BUFFER_SECS: u64 = 60;

/// Minimum wait before the next refresh attempt. Prevents a tight loop when the
/// token endpoint reports short or clock-skewed lifetimes.
const MIN_TOKEN_REFRESH_WAIT_SECS: u64 = 1;

/// Fallback access-token lifetime when the token endpoint omits `expires_in`.
const DEFAULT_TOKEN_LIFETIME_SECS: u64 = 3600;

/// Upper bound on background-refresh backoff after an error.
const MAX_REFRESH_BACKOFF_SECS: u64 = 300;

/// Cap on how much of the token endpoint's error body is echoed into logs and
/// `Error::TokenEndpointStatus`. Prevents accidentally surfacing large or
/// multi-line payloads (which may embed sensitive details) into callers.
const MAX_ERROR_BODY_BYTES: usize = 512;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid OAuth2 token URL '{url}': {source}"))]
    InvalidTokenUrl {
        url: String,
        source: url::ParseError,
    },

    #[snafu(display(
        "OAuth2 token URL must use HTTPS (or http://localhost or a loopback address for testing). Got: {url}"
    ))]
    InsecureTokenUrl { url: String },

    #[snafu(display("Failed to build OAuth2 HTTP client: {source}"))]
    BuildClient { source: reqwest::Error },

    #[snafu(display("OAuth2 token request to {url} failed: {source}"))]
    TokenRequest { url: String, source: reqwest::Error },

    #[snafu(display("OAuth2 token endpoint {url} returned HTTP {status}: {body}"))]
    TokenEndpointStatus {
        url: String,
        status: u16,
        body: String,
    },

    #[snafu(display("Failed to parse OAuth2 token response: {source}"))]
    InvalidTokenResponse { source: reqwest::Error },

    #[snafu(display(
        "OAuth2 token endpoint returned unsupported token_type '{token_type}'; only 'Bearer' is supported."
    ))]
    UnsupportedTokenType { token_type: String },

    #[snafu(display("OAuth2 access token is not a valid HTTP header value: {source}"))]
    InvalidAccessToken {
        source: reqwest::header::InvalidHeaderValue,
    },

    #[snafu(display("Invalid OAuth2 auth header name '{name}': {source}"))]
    InvalidHeaderName {
        name: String,
        source: reqwest::header::InvalidHeaderName,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Decorates outgoing HTTP requests with authentication.
///
/// Implementations must be cheap and synchronous — any asynchronous work
/// (e.g. token refresh) should happen on a background task so callers do not
/// pay latency on every request.
pub trait HttpAuthenticator: Send + Sync + fmt::Debug {
    /// Attach the authentication header to an outgoing request.
    fn apply(&self, builder: RequestBuilder) -> RequestBuilder;

    /// The HTTP header name this authenticator writes. Callers use it to guard
    /// against a conflicting static (`http_headers`) or dynamic
    /// (`request_headers`) header of the same name, which would otherwise be
    /// sent alongside the authenticator's value.
    fn header_name(&self) -> &HeaderName;
}

/// How client credentials are conveyed to the `OAuth2` token endpoint
/// (RFC 6749 §2.3.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ClientAuthMethod {
    /// HTTP Basic `Authorization` header. Preferred by the RFC.
    #[default]
    Basic,
    /// `client_id`/`client_secret` form fields in the request body.
    Body,
}

impl ClientAuthMethod {
    /// Parse from the user-facing `auth_client_auth` parameter value.
    ///
    /// # Errors
    /// Returns the unrecognized value unchanged when it is neither `basic` nor `body`.
    pub fn parse(value: &str) -> std::result::Result<Self, String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "basic" => Ok(Self::Basic),
            "body" => Ok(Self::Body),
            other => Err(other.to_string()),
        }
    }
}

/// The `OAuth2` grant used to obtain (and periodically refresh) access tokens.
#[derive(Clone, Debug)]
pub enum OAuthGrant {
    /// RFC 6749 §6 refresh-token grant. The connector trades a long-lived
    /// refresh token for short-lived access tokens; the token endpoint may
    /// rotate the refresh token on each exchange.
    RefreshToken(SecretString),
    /// RFC 6749 §4.4 client-credentials grant. The connector authenticates as
    /// itself with `client_id`/`client_secret`; no refresh token is issued, so
    /// each refresh re-runs the same exchange.
    ClientCredentials,
}

impl OAuthGrant {
    /// The `grant_type` form field value sent to the token endpoint.
    fn grant_type(&self) -> &'static str {
        match self {
            Self::RefreshToken(_) => "refresh_token",
            Self::ClientCredentials => "client_credentials",
        }
    }
}

/// Where the obtained access token is attached to outgoing data requests.
///
/// Only the header *name* is user-configurable; the value scheme is derived
/// from it. The `Authorization` header (the default) carries `Bearer <token>`
/// per RFC 6750; any other header carries the bare `<token>` — matching APIs
/// with non-standard schemes, e.g. the Shopify Admin API's
/// `X-Shopify-Access-Token: <token>`.
#[derive(Clone, Debug)]
pub struct TokenHeader {
    name: HeaderName,
}

impl Default for TokenHeader {
    fn default() -> Self {
        Self {
            name: AUTHORIZATION,
        }
    }
}

impl TokenHeader {
    /// Build a token header from the user-facing header name.
    ///
    /// `name` is the HTTP header name (e.g. `Authorization`,
    /// `X-Shopify-Access-Token`). The value scheme is derived: `Authorization`
    /// yields `Bearer <token>`, any other header yields the bare `<token>`.
    ///
    /// # Errors
    /// Returns [`Error::InvalidHeaderName`] if `name` is not a valid HTTP header
    /// name.
    pub fn new(name: &str) -> Result<Self> {
        let trimmed = name.trim();
        let header_name = HeaderName::from_bytes(trimmed.as_bytes()).map_err(|source| {
            Error::InvalidHeaderName {
                name: trimmed.to_string(),
                source,
            }
        })?;
        Ok(Self { name: header_name })
    }

    /// The configured header name.
    #[must_use]
    pub fn name(&self) -> &HeaderName {
        &self.name
    }

    /// Render the sensitive header value for `access_token`. The scheme depends
    /// on the header name: `Bearer <token>` for `Authorization`, bare `<token>`
    /// otherwise.
    fn header_value(&self, access_token: &SecretString) -> Result<HeaderValue> {
        let raw = if self.name == AUTHORIZATION {
            format!("Bearer {}", access_token.expose_secret())
        } else {
            access_token.expose_secret().to_string()
        };
        let mut header = HeaderValue::from_str(&raw).context(InvalidAccessTokenSnafu)?;
        header.set_sensitive(true);
        Ok(header)
    }
}

/// Static configuration for `OAuth2` token acquisition.
#[derive(Clone, Debug)]
pub struct OAuth2Config {
    pub token_url: String,
    pub client_id: Option<String>,
    pub client_secret: Option<SecretString>,
    pub scopes: Option<String>,
    pub client_auth: ClientAuthMethod,
    pub header: TokenHeader,
}

/// Authenticator that applies an `OAuth2` access token to every outgoing
/// request, refreshing the token in the background before it expires.
pub struct OAuth2Auth {
    token_url: String,
    header_name: HeaderName,
    rx: watch::Receiver<HeaderValue>,
    _handle: Arc<JoinHandle<()>>,
}

impl fmt::Debug for OAuth2Auth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OAuth2Auth")
            .field("token_url", &self.token_url)
            .field("header_name", &self.header_name)
            .field("access_token", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

impl OAuth2Auth {
    /// Perform the initial token exchange and spawn a background task that
    /// refreshes the access token before it expires.
    ///
    /// # Errors
    /// Returns an error if `config.token_url` is invalid, if the HTTP client
    /// cannot be constructed, or if the initial token exchange fails.
    pub async fn try_new(config: OAuth2Config, grant: OAuthGrant) -> Result<Self> {
        validate_token_url(&config.token_url)?;

        let client = build_token_client()?;

        let initial = exchange_token(&client, &config, &grant).await?;

        let header = config.header.header_value(&initial.access_token)?;
        let (tx, rx) = watch::channel(header);
        let expires_in = initial.expires_in.unwrap_or(DEFAULT_TOKEN_LIFETIME_SECS);

        // For the refresh-token grant, honor a rotated refresh token from the
        // initial response; the client-credentials grant carries no refresh
        // token to rotate.
        let grant = match (grant, initial.refresh_token) {
            (OAuthGrant::RefreshToken(_), Some(rotated)) => OAuthGrant::RefreshToken(rotated),
            (grant, _) => grant,
        };

        let header_name = config.header.name().clone();
        let token_url = config.token_url.clone();

        let handle = tokio::spawn(refresh_loop(client, config, grant, expires_in, tx));

        Ok(Self {
            token_url,
            header_name,
            rx,
            _handle: Arc::new(handle),
        })
    }

    /// Current auth header value. Test-only helper so downstream callers can't
    /// accidentally log or otherwise exfiltrate the access token via this API.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn current_header_value(&self) -> HeaderValue {
        self.rx.borrow().clone()
    }
}

impl HttpAuthenticator for OAuth2Auth {
    fn apply(&self, builder: RequestBuilder) -> RequestBuilder {
        builder.header(self.header_name.clone(), self.rx.borrow().clone())
    }

    fn header_name(&self) -> &HeaderName {
        &self.header_name
    }
}

async fn refresh_loop(
    client: Client,
    config: OAuth2Config,
    mut grant: OAuthGrant,
    initial_expires_in: u64,
    tx: watch::Sender<HeaderValue>,
) {
    let mut next_wait = next_refresh_wait(initial_expires_in);
    let mut backoff = FibonacciBackoffBuilder::new()
        .max_duration(Some(Duration::from_secs(MAX_REFRESH_BACKOFF_SECS)))
        .build();

    loop {
        tokio::select! {
            () = sleep(next_wait) => {}
            () = tx.closed() => break,
        }

        match exchange_token(&client, &config, &grant).await {
            Ok(resp) => {
                backoff.reset();
                let expires_in = resp.expires_in.unwrap_or(DEFAULT_TOKEN_LIFETIME_SECS);
                let header = match config.header.header_value(&resp.access_token) {
                    Ok(h) => h,
                    Err(e) => {
                        tracing::error!(
                            "OAuth2 refresh from {} returned an access token that is not a valid HTTP header value: {e}. Keeping the previous token and retrying.",
                            config.token_url
                        );
                        next_wait = backoff
                            .next_duration()
                            .unwrap_or(Duration::from_secs(MAX_REFRESH_BACKOFF_SECS));
                        continue;
                    }
                };
                tracing::debug!(
                    "OAuth2 access token refreshed from {}; expires in {expires_in}s",
                    config.token_url
                );
                if tx.send(header).is_err() {
                    break;
                }
                // Rotate the refresh token if the endpoint issued a new one.
                // Only meaningful for the refresh-token grant.
                if let (OAuthGrant::RefreshToken(current), Some(new_refresh)) =
                    (&mut grant, resp.refresh_token)
                {
                    *current = new_refresh;
                }
                next_wait = next_refresh_wait(expires_in);
            }
            Err(e) => {
                let backoff_duration = backoff
                    .next_duration()
                    .unwrap_or(Duration::from_secs(MAX_REFRESH_BACKOFF_SECS));
                tracing::warn!(
                    "OAuth2 token refresh against {} failed: {e}. Retrying in {backoff_duration:.2?}",
                    config.token_url
                );
                next_wait = backoff_duration;
            }
        }
    }
}

fn next_refresh_wait(expires_in: u64) -> Duration {
    Duration::from_secs(
        expires_in
            .saturating_sub(TOKEN_REFRESH_BUFFER_SECS)
            .max(MIN_TOKEN_REFRESH_WAIT_SECS),
    )
}

fn validate_token_url(url: &str) -> Result<()> {
    let parsed = Url::parse(url).map_err(|e| Error::InvalidTokenUrl {
        url: url.to_string(),
        source: e,
    })?;

    let is_localhost = match parsed.host() {
        Some(url::Host::Domain(d)) => d.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        None => false,
    };

    if parsed.scheme() == "https" || (parsed.scheme() == "http" && is_localhost) {
        Ok(())
    } else {
        Err(Error::InsecureTokenUrl {
            url: url.to_string(),
        })
    }
}

fn build_token_client() -> Result<Client> {
    configure_client_builder(ClientBuilder::new())
        .user_agent(util::spiceai_user_agent())
        .build()
        .context(BuildClientSnafu)
}

#[derive(Debug)]
struct TokenResponse {
    access_token: SecretString,
    refresh_token: Option<SecretString>,
    expires_in: Option<u64>,
}

#[derive(Deserialize)]
struct RawTokenResponse {
    access_token: String,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    expires_in: Option<u64>,
    #[serde(default)]
    token_type: Option<String>,
}

async fn exchange_token(
    client: &Client,
    config: &OAuth2Config,
    grant: &OAuthGrant,
) -> Result<TokenResponse> {
    let response = send_request_with_retry("HTTP connector OAuth2", "acquire access token", || {
        build_token_request(client, config, grant)
    })
    .await
    .map_err(|e| Error::TokenRequest {
        url: config.token_url.clone(),
        source: e,
    })?;

    if !response.status().is_success() {
        let status = response.status().as_u16();
        let body = read_bounded_error_body(response, MAX_ERROR_BODY_BYTES).await;
        return Err(Error::TokenEndpointStatus {
            url: config.token_url.clone(),
            status,
            body,
        });
    }

    let raw: RawTokenResponse = response.json().await.context(InvalidTokenResponseSnafu)?;

    if let Some(token_type) = raw.token_type.as_deref()
        && !token_type.eq_ignore_ascii_case("bearer")
    {
        return Err(Error::UnsupportedTokenType {
            token_type: token_type.to_string(),
        });
    }

    Ok(TokenResponse {
        access_token: SecretString::from(raw.access_token),
        refresh_token: raw.refresh_token.map(SecretString::from),
        expires_in: raw.expires_in,
    })
}

fn build_token_request(
    client: &Client,
    config: &OAuth2Config,
    grant: &OAuthGrant,
) -> RequestBuilder {
    let mut form: Vec<(String, String)> =
        vec![("grant_type".to_string(), grant.grant_type().to_string())];

    if let OAuthGrant::RefreshToken(refresh_token) = grant {
        form.push((
            "refresh_token".to_string(),
            refresh_token.expose_secret().to_string(),
        ));
    }

    if let Some(scopes) = &config.scopes {
        let trimmed = scopes.trim();
        if !trimmed.is_empty() {
            form.push(("scope".to_string(), trimmed.to_string()));
        }
    }

    let mut req = client.post(&config.token_url);

    match (
        config.client_id.as_deref(),
        config.client_secret.as_ref(),
        config.client_auth,
    ) {
        (Some(id), Some(secret), ClientAuthMethod::Basic) => {
            req = req.basic_auth(id, Some(secret.expose_secret()));
        }
        (Some(id), Some(secret), ClientAuthMethod::Body) => {
            form.push(("client_id".to_string(), id.to_string()));
            form.push((
                "client_secret".to_string(),
                secret.expose_secret().to_string(),
            ));
        }
        (Some(id), None, _) => {
            // Public client (RFC 6749 §2.1). Forward client_id for correlation.
            form.push(("client_id".to_string(), id.to_string()));
        }
        (None, _, _) => {}
    }

    req.form(&form)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::{
        collections::VecDeque,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    struct MockResponse {
        status: &'static str,
        body: String,
    }

    impl MockResponse {
        fn ok(body: &serde_json::Value) -> Self {
            Self {
                status: "200 OK",
                body: body.to_string(),
            }
        }

        fn status(status: &'static str, body: &serde_json::Value) -> Self {
            Self {
                status,
                body: body.to_string(),
            }
        }
    }

    async fn start_mock_server(
        responses: Vec<MockResponse>,
    ) -> (
        String,
        Arc<AtomicUsize>,
        Arc<tokio::sync::Mutex<Vec<String>>>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock listener");
        let addr = listener.local_addr().expect("mock local_addr");
        let responses = Arc::new(tokio::sync::Mutex::new(VecDeque::from(responses)));
        let request_count = Arc::new(AtomicUsize::new(0));
        let captured = Arc::new(tokio::sync::Mutex::new(Vec::new()));

        let request_count_srv = Arc::clone(&request_count);
        let captured_srv = Arc::clone(&captured);
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };

                let responses = Arc::clone(&responses);
                let count = Arc::clone(&request_count_srv);
                let captured = Arc::clone(&captured_srv);

                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};

                    let mut buf = [0u8; 4096];
                    let mut raw = Vec::new();
                    let mut header_end = None;
                    let mut content_length = 0usize;
                    loop {
                        let n = match stream.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => n,
                        };
                        raw.extend_from_slice(&buf[..n]);

                        if header_end.is_none()
                            && let Some(idx) =
                                raw.windows(4).position(|w| w == b"\r\n\r\n").map(|i| i + 4)
                        {
                            header_end = Some(idx);
                            let header_text = String::from_utf8_lossy(&raw[..idx]);
                            content_length = header_text
                                .lines()
                                .find_map(|line| {
                                    let (k, v) = line.split_once(':')?;
                                    if k.trim().eq_ignore_ascii_case("Content-Length") {
                                        v.trim().parse::<usize>().ok()
                                    } else {
                                        None
                                    }
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
                        body: "{}".to_string(),
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

        (format!("http://{addr}"), request_count, captured)
    }

    /// Convenience config with the default `Authorization: Bearer` header.
    fn test_config(token_url: String, client_auth: ClientAuthMethod) -> OAuth2Config {
        OAuth2Config {
            token_url,
            client_id: None,
            client_secret: None,
            scopes: None,
            client_auth,
            header: TokenHeader::default(),
        }
    }

    #[test]
    fn parse_client_auth_method() {
        assert_eq!(
            ClientAuthMethod::parse("basic"),
            Ok(ClientAuthMethod::Basic)
        );
        assert_eq!(ClientAuthMethod::parse("BODY"), Ok(ClientAuthMethod::Body));
        ClientAuthMethod::parse("none").expect_err("expected unsupported auth method to fail");
    }

    #[test]
    fn token_header_validation() {
        // Default: Authorization -> Bearer scheme.
        let default = TokenHeader::default();
        assert_eq!(default.name(), &AUTHORIZATION);
        assert_eq!(
            default
                .header_value(&SecretString::from("abc"))
                .expect("value")
                .to_str()
                .unwrap_or(""),
            "Bearer abc"
        );

        // Explicit Authorization (any case) also gets the Bearer scheme.
        let explicit = TokenHeader::new("authorization").expect("valid header");
        assert_eq!(
            explicit
                .header_value(&SecretString::from("abc"))
                .expect("value")
                .to_str()
                .unwrap_or(""),
            "Bearer abc"
        );

        // Any other header carries the bare token.
        let shopify =
            TokenHeader::new("X-Shopify-Access-Token").expect("valid custom header should parse");
        assert_eq!(shopify.name().as_str(), "x-shopify-access-token");
        let value = shopify
            .header_value(&SecretString::from("shpat_abc"))
            .expect("header value should build");
        assert_eq!(value.to_str().unwrap_or(""), "shpat_abc");
        assert!(value.is_sensitive(), "token header must be sensitive");

        assert!(matches!(
            TokenHeader::new("Bad Header Name"),
            Err(Error::InvalidHeaderName { .. })
        ));
    }

    #[test]
    fn validates_token_url() {
        validate_token_url("https://example.com/oauth/token")
            .expect("expected https token URL to be accepted");
        validate_token_url("http://localhost:1234/oauth/token")
            .expect("expected localhost token URL to be accepted");
        validate_token_url("http://127.0.0.1/oauth/token")
            .expect("expected loopback token URL to be accepted");
        assert!(matches!(
            validate_token_url("http://example.com/oauth/token"),
            Err(Error::InsecureTokenUrl { .. })
        ));
        assert!(matches!(
            validate_token_url("not a url"),
            Err(Error::InvalidTokenUrl { .. })
        ));
    }

    #[test]
    fn refresh_wait_clamps_to_minimum() {
        assert_eq!(next_refresh_wait(0), Duration::from_secs(1));
        assert_eq!(
            next_refresh_wait(TOKEN_REFRESH_BUFFER_SECS),
            Duration::from_secs(1)
        );
        assert_eq!(
            next_refresh_wait(TOKEN_REFRESH_BUFFER_SECS + 30),
            Duration::from_secs(30)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn exchange_refresh_token_sends_expected_form_and_basic_auth() {
        let (url, count, captured) = start_mock_server(vec![MockResponse::ok(&json!({
            "access_token": "new-access",
            "refresh_token": "rotated-refresh",
            "expires_in": 600,
            "token_type": "Bearer"
        }))])
        .await;

        let client = build_token_client().expect("build token client");
        let config = OAuth2Config {
            token_url: format!("{url}/oauth/token"),
            client_id: Some("my-client".to_string()),
            client_secret: Some(SecretString::from("super-secret")),
            scopes: Some("read:data".to_string()),
            client_auth: ClientAuthMethod::Basic,
            header: TokenHeader::default(),
        };

        let resp = exchange_token(
            &client,
            &config,
            &OAuthGrant::RefreshToken(SecretString::from("rt-original")),
        )
        .await
        .expect("token exchange should succeed");

        assert_eq!(resp.expires_in, Some(600));
        assert_eq!(resp.access_token.expose_secret(), "new-access");
        assert_eq!(
            resp.refresh_token.as_ref().map(ExposeSecret::expose_secret),
            Some("rotated-refresh")
        );
        assert_eq!(count.load(Ordering::SeqCst), 1);

        let request = captured.lock().await.remove(0);
        let lower = request.to_ascii_lowercase();
        // Basic auth: base64("my-client:super-secret")
        assert!(
            lower.contains("authorization: basic "),
            "expected Basic auth header, got: {request}"
        );
        assert!(
            request.contains("grant_type=refresh_token"),
            "missing grant_type in form: {request}"
        );
        assert!(
            request.contains("refresh_token=rt-original"),
            "missing refresh_token in form: {request}"
        );
        assert!(
            request.contains("scope=read%3Adata") || request.contains("scope=read:data"),
            "missing scope in form: {request}"
        );
        assert!(
            !request.contains("client_secret="),
            "client_secret should not appear in form body with basic auth: {request}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn exchange_refresh_token_sends_client_credentials_in_body() {
        let (url, _count, captured) = start_mock_server(vec![MockResponse::ok(&json!({
            "access_token": "at",
            "expires_in": 3600
        }))])
        .await;

        let client = build_token_client().expect("build token client");
        let config = OAuth2Config {
            token_url: format!("{url}/oauth/token"),
            client_id: Some("cid".to_string()),
            client_secret: Some(SecretString::from("csec")),
            scopes: None,
            client_auth: ClientAuthMethod::Body,
            header: TokenHeader::default(),
        };

        exchange_token(
            &client,
            &config,
            &OAuthGrant::RefreshToken(SecretString::from("rt")),
        )
        .await
        .expect("token exchange should succeed");

        let request = captured.lock().await.remove(0);
        let lower = request.to_ascii_lowercase();
        assert!(
            !lower.contains("authorization: basic"),
            "Basic auth should be absent in body mode: {request}"
        );
        assert!(
            request.contains("client_id=cid"),
            "client_id missing from body: {request}"
        );
        assert!(
            request.contains("client_secret=csec"),
            "client_secret missing from body: {request}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn exchange_client_credentials_grant_omits_refresh_token() {
        let (url, _count, captured) = start_mock_server(vec![MockResponse::ok(&json!({
            "access_token": "shpat_generated",
            "expires_in": 86399
        }))])
        .await;

        let client = build_token_client().expect("build token client");
        let config = OAuth2Config {
            token_url: format!("{url}/admin/oauth/access_token"),
            client_id: Some("shopify-client".to_string()),
            client_secret: Some(SecretString::from("shopify-secret")),
            scopes: None,
            client_auth: ClientAuthMethod::Body,
            header: TokenHeader::new("X-Shopify-Access-Token").expect("valid header config"),
        };

        let resp = exchange_token(&client, &config, &OAuthGrant::ClientCredentials)
            .await
            .expect("client-credentials exchange should succeed");
        assert_eq!(resp.access_token.expose_secret(), "shpat_generated");
        assert_eq!(resp.expires_in, Some(86399));

        let request = captured.lock().await.remove(0);
        assert!(
            request.contains("grant_type=client_credentials"),
            "missing client_credentials grant_type: {request}"
        );
        assert!(
            !request.contains("refresh_token="),
            "client-credentials grant must not send a refresh_token: {request}"
        );
        assert!(
            request.contains("client_id=shopify-client")
                && request.contains("client_secret=shopify-secret"),
            "client credentials should be in the body: {request}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn exchange_refresh_token_surfaces_non_success_body() {
        let (url, _count, _captured) = start_mock_server(vec![MockResponse::status(
            "400 Bad Request",
            &json!({"error": "invalid_grant"}),
        )])
        .await;

        let client = build_token_client().expect("build token client");
        let config = test_config(format!("{url}/oauth/token"), ClientAuthMethod::Basic);

        let err = exchange_token(
            &client,
            &config,
            &OAuthGrant::RefreshToken(SecretString::from("rt")),
        )
        .await
        .expect_err("400 should propagate as TokenEndpointStatus");

        match err {
            Error::TokenEndpointStatus { status, body, .. } => {
                assert_eq!(status, 400);
                assert!(body.contains("invalid_grant"), "body: {body}");
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn oauth2_auth_applies_bearer_header() {
        let (url, _count, _captured) = start_mock_server(vec![MockResponse::ok(&json!({
            "access_token": "initial-access",
            "expires_in": 3600
        }))])
        .await;

        let auth = OAuth2Auth::try_new(
            test_config(format!("{url}/oauth/token"), ClientAuthMethod::Basic),
            OAuthGrant::RefreshToken(SecretString::from("rt-seed")),
        )
        .await
        .expect("OAuth2Auth::try_new should succeed");

        assert_eq!(auth.header_name(), &AUTHORIZATION);
        let current = auth.current_header_value();
        assert_eq!(current.to_str().unwrap_or(""), "Bearer initial-access");
        assert!(
            current.is_sensitive(),
            "bearer header must be marked sensitive"
        );

        let client = Client::new();
        // Never actually sent — we just inspect the built request's headers.
        let builder = client.get("https://example.invalid/data");
        let built = auth.apply(builder).build().expect("request should build");
        let header = built
            .headers()
            .get("Authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert_eq!(header, "Bearer initial-access");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn oauth2_auth_applies_custom_header_for_client_credentials() {
        let (url, _count, _captured) = start_mock_server(vec![MockResponse::ok(&json!({
            "access_token": "shpat_live",
            "expires_in": 86399
        }))])
        .await;

        let config = OAuth2Config {
            token_url: format!("{url}/admin/oauth/access_token"),
            client_id: Some("cid".to_string()),
            client_secret: Some(SecretString::from("csec")),
            scopes: None,
            client_auth: ClientAuthMethod::Body,
            header: TokenHeader::new("X-Shopify-Access-Token").expect("valid header config"),
        };

        let auth = OAuth2Auth::try_new(config, OAuthGrant::ClientCredentials)
            .await
            .expect("client-credentials auth should initialise");

        assert_eq!(auth.header_name().as_str(), "x-shopify-access-token");

        let client = Client::new();
        let builder = client.get("https://example.invalid/admin/api/shop.json");
        let built = auth.apply(builder).build().expect("request should build");
        assert_eq!(
            built
                .headers()
                .get("X-Shopify-Access-Token")
                .and_then(|v| v.to_str().ok())
                .unwrap_or(""),
            "shpat_live"
        );
        assert!(
            built.headers().get("Authorization").is_none(),
            "custom-header auth must not also set Authorization"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn exchange_rejects_non_bearer_token_type() {
        let (url, _count, _captured) = start_mock_server(vec![MockResponse::ok(&json!({
            "access_token": "at",
            "token_type": "MAC",
            "expires_in": 3600
        }))])
        .await;

        let client = build_token_client().expect("build token client");
        let config = test_config(format!("{url}/oauth/token"), ClientAuthMethod::Basic);

        let err = exchange_token(
            &client,
            &config,
            &OAuthGrant::RefreshToken(SecretString::from("rt")),
        )
        .await
        .expect_err("non-Bearer token_type should be rejected");

        match err {
            Error::UnsupportedTokenType { token_type } => {
                assert_eq!(token_type, "MAC");
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    /// Drives the background refresh loop long enough to verify that after
    /// `expires_in` elapses, the connector re-hits the token endpoint *using
    /// the refresh token that was rotated in on the previous response*, not
    /// the seed token.
    ///
    /// Note: we deliberately do not use `#[tokio::test(start_paused = true)]`
    /// here. The test depends on real TCP I/O against a local mock server, and
    /// reqwest's `connect_timeout` is driven by the tokio timer — paused time
    /// races with the (real) TCP connect and fires the connect timeout before
    /// the three-way handshake completes. Instead we use a small `expires_in`
    /// so the refresh loop sleeps just ~1s.
    #[tokio::test(flavor = "current_thread")]
    async fn refresh_loop_uses_rotated_refresh_token() {
        // Server responses: first exchange rotates seed->refresh-v1, second
        // exchange rotates refresh-v1->refresh-v2. Both issue successive
        // access tokens so we can observe the rotation took effect.
        //
        // `expires_in = TOKEN_REFRESH_BUFFER_SECS + 1` so the background loop
        // wakes up ~1s after the initial exchange.
        let expires_in = TOKEN_REFRESH_BUFFER_SECS + 1;
        let (url, count, captured) = start_mock_server(vec![
            MockResponse::ok(&json!({
                "access_token": "access-v1",
                "refresh_token": "refresh-v1",
                "expires_in": expires_in,
            })),
            MockResponse::ok(&json!({
                "access_token": "access-v2",
                "refresh_token": "refresh-v2",
                "expires_in": expires_in,
            })),
        ])
        .await;

        let auth = OAuth2Auth::try_new(
            test_config(format!("{url}/oauth/token"), ClientAuthMethod::Basic),
            OAuthGrant::RefreshToken(SecretString::from("seed-refresh")),
        )
        .await
        .expect("initial exchange should succeed");

        assert_eq!(
            auth.current_header_value().to_str().unwrap_or(""),
            "Bearer access-v1"
        );

        // Poll for the second exchange, bounded by a 5s deadline. Use
        // `tokio::time::Instant` (not `std::time::Instant`) so this cooperates
        // with any future move to simulated time without code churn.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < deadline {
            if count.load(Ordering::SeqCst) >= 2
                && auth.current_header_value().to_str().unwrap_or("") == "Bearer access-v2"
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        assert_eq!(
            count.load(Ordering::SeqCst),
            2,
            "background refresh did not fire a second exchange within 5s",
        );
        assert_eq!(
            auth.current_header_value().to_str().unwrap_or(""),
            "Bearer access-v2",
            "watch channel did not propagate the rotated access token",
        );

        // Second request should carry the rotated refresh token from the first
        // response, not the seed.
        let requests = captured.lock().await;
        assert_eq!(requests.len(), 2, "expected two token requests");
        let second = &requests[1];
        assert!(
            second.contains("refresh_token=refresh-v1"),
            "second refresh did not use the rotated token: {second}"
        );
        assert!(
            !second.contains("refresh_token=seed-refresh"),
            "second refresh incorrectly reused the seed token: {second}"
        );
    }
}
