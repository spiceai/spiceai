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

use data_components::resilient_http::{configure_client_builder, send_request_with_retry};
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use snafu::prelude::*;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::Duration;
use std::{fmt, sync::Arc};
use token_provider::{Result, TokenProvider};
use tokio::{sync::watch, task::JoinHandle, time::sleep};
use util::fibonacci_backoff::{Backoff as _, FibonacciBackoffBuilder};

use crate::request::DatabricksAuthExtension;
use runtime_request_context::RequestContext;

const TOKEN_REFRESH_BUFFER_SECS: u64 = 300;
const MIN_TOKEN_REFRESH_WAIT_SECS: u64 = 1;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to obtain Databricks service principal token for machine-to-machine authentication. {source}"
    ))]
    UnableToGetToken {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

#[derive(Clone)]
pub struct DatabricksM2MTokenProvider {
    endpoint: String,
    client_id: String,

    rx: watch::Receiver<SecretString>,

    _handle: Arc<JoinHandle<()>>,
}

impl Hash for DatabricksM2MTokenProvider {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.endpoint.hash(state);
        self.client_id.hash(state);
    }
}

impl fmt::Debug for DatabricksM2MTokenProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DatabricksM2MTokenProvider")
            .field("endpoint", &self.endpoint)
            .field("client_id", &self.client_id)
            .field("tx", &"<watch::Sender>")
            .field("rx", &"<watch::Receiver>")
            .field("_handle", &"<JoinHandle>")
            .finish()
    }
}

impl DatabricksM2MTokenProvider {
    pub async fn try_new(
        endpoint: String,
        client_id: String,
        client_secret: SecretString,
    ) -> Result<Self, Error> {
        let client = build_m2m_token_client().map_err(|e| Error::UnableToGetToken {
            source: Box::new(e),
        })?;

        // initial fetch
        let TokenResponse {
            access_token,
            expires_in,
            ..
        } = get_m2m_access_token(&client, &endpoint, &client_id, &client_secret)
            .await
            .map_err(|e| Error::UnableToGetToken { source: e })?;

        // create watch channel
        let (tx, rx) = watch::channel(access_token);

        // spawn background refresh loop
        let cloned_client_id = client_id.clone();
        let cloned_endpoint = endpoint.clone();
        let cloned_tx = tx;
        let refresh_client = client.clone();

        let secret = client_secret.clone();

        let handle = tokio::spawn(async move {
            // Databricks M2M access token lifespan is one hour. Schedule a refresh five minutes before expiration
            let mut next_wait = next_token_refresh_wait(expires_in);

            let mut backoff = FibonacciBackoffBuilder::new()
                .max_duration(Some(Duration::from_secs(300))) // Cap at 5 minutes
                .build();

            loop {
                sleep(next_wait).await;

                match get_m2m_access_token(
                    &refresh_client,
                    &cloned_endpoint,
                    &cloned_client_id,
                    &secret,
                )
                .await
                {
                    Ok(TokenResponse {
                        access_token,
                        expires_in,
                        ..
                    }) => {
                        backoff.reset();
                        tracing::debug!("M2M token refreshed; expires in {}", expires_in);
                        let _ = cloned_tx.send(access_token.clone());
                        next_wait = next_token_refresh_wait(expires_in);
                    }
                    Err(e) => {
                        let backoff_duration =
                            backoff.next_duration().unwrap_or(Duration::from_secs(300));
                        tracing::error!(
                            "Databricks M2M token refresh failed: {}. Retrying in {:.2?}",
                            e,
                            backoff_duration
                        );
                        next_wait = backoff_duration;
                    }
                }
            }
        });

        Ok(Self {
            endpoint,
            client_id,
            rx,
            _handle: Arc::new(handle),
        })
    }
}

impl TokenProvider for DatabricksM2MTokenProvider {
    fn get_token(&self) -> String {
        self.rx.borrow().expose_secret().to_string()
    }

    fn dyn_hash(&self) -> String {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish().to_string()
    }

    fn subscribe(&self) -> Option<watch::Receiver<String>> {
        let mut secret_rx = self.rx.clone();
        let (tx, rx) = watch::channel(secret_rx.borrow().expose_secret().to_string());
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = tx.closed() => {
                        break;
                    }
                    changed = secret_rx.changed() => {
                        if changed.is_err() {
                            break;
                        }
                        let exposed = secret_rx.borrow().expose_secret().to_string();
                        if tx.send(exposed).is_err() {
                            break;
                        }
                    }
                }
            }
        });
        Some(rx)
    }
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: SecretString,
    token_type: String,
    expires_in: u64,
    scope: String,
}

impl fmt::Debug for TokenResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenResponse")
            .field("access_token", &"[REDACTED]")
            .field("token_type", &self.token_type)
            .field("expires_in", &self.expires_in)
            .field("scope", &self.scope)
            .finish()
    }
}

async fn get_m2m_access_token(
    client: &reqwest::Client,
    databricks_endpoint: &str,
    client_id: &str,
    client_secret: &SecretString,
) -> Result<TokenResponse, Box<dyn std::error::Error + Send + Sync>> {
    let token_endpoint_url = databricks_token_endpoint_url(databricks_endpoint)?;

    let response = send_request_with_retry("Databricks", "request M2M access token", || {
        client
            .post(&token_endpoint_url)
            .basic_auth(client_id, Some(client_secret.expose_secret()))
            .header("Content-Type", "application/x-www-form-urlencoded")
            .form(&[("grant_type", "client_credentials"), ("scope", "all-apis")])
    })
    .await?;

    if !response.status().is_success() {
        let status = response.status();
        let error_text = response.text().await?;
        return Err(format!("Failed to get access token: HTTP {status}, {error_text}",).into());
    }

    let token_response = response.json::<TokenResponse>().await?;

    tracing::debug!(
        "Got access token, expires in {} seconds",
        token_response.expires_in
    );

    Ok(token_response)
}

fn build_m2m_token_client() -> Result<reqwest::Client, reqwest::Error> {
    configure_client_builder(reqwest::Client::builder())
        .user_agent(util::spiceai_user_agent())
        .build()
}

fn databricks_token_endpoint_url(
    databricks_endpoint: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let endpoint = databricks_endpoint.trim_end_matches('/');
    let base_url = if endpoint.starts_with("https://") {
        endpoint.to_string()
    } else if endpoint.starts_with("http://") {
        let parsed = url::Url::parse(endpoint)?;
        let is_localhost = match parsed.host() {
            Some(url::Host::Domain("localhost")) => true,
            Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
            Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
            _ => false,
        };
        if is_localhost {
            endpoint.to_string()
        } else {
            return Err(format!(
                "Databricks token endpoint must use HTTPS for non-localhost hosts, got: {endpoint}"
            )
            .into());
        }
    } else {
        format!("https://{endpoint}")
    };

    Ok(format!("{base_url}/oidc/v1/token"))
}

fn next_token_refresh_wait(expires_in: u64) -> Duration {
    Duration::from_secs(
        expires_in
            .saturating_sub(TOKEN_REFRESH_BUFFER_SECS)
            .max(MIN_TOKEN_REFRESH_WAIT_SECS),
    )
}

#[derive(Debug)]
#[cfg(feature = "databricks")]
pub enum AuthCredentials<'a> {
    Token(&'a SecretString),
    ServicePrincipal(&'a str, &'a SecretString),
    U2M(&'a str),
}

//
// U2M
//

#[derive(Clone)]
pub struct DatabricksU2MTokenProvider {
    endpoint: String,
    client_id: String,
}

impl Hash for DatabricksU2MTokenProvider {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.endpoint.hash(state);
        self.client_id.hash(state);
    }
}

impl fmt::Debug for DatabricksU2MTokenProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DatabricksU2MTokenProvider")
            .field("endpoint", &self.endpoint)
            .field("client_id", &self.client_id)
            .finish()
    }
}

impl TokenProvider for DatabricksU2MTokenProvider {
    /// Retrieves the corresponding access token from the current request context by matching the `client_id`.
    /// If no token is found, it returns an empty string, and the dependent component is expected to handle this as an error.
    ///
    /// # Safety
    /// This function uses `RequestContext::current_sync()`, which is marked unsafe because it accesses thread-local or global state
    /// that may not be valid outside of a request context. In this usage, we are always calling `get_token` from within a valid
    /// async request context, so it is safe to call this function here.
    fn get_token(&self) -> String {
        let context = unsafe { RequestContext::current_sync() };
        if let Some(extension) = context.extension::<DatabricksAuthExtension>() {
            if let Some(token) = extension.get_token(&self.client_id) {
                tracing::debug!(
                    "using access_token for {} from the request context",
                    &self.client_id,
                );
                return token.expose_secret().to_string();
            }
            tracing::debug!("no token found for client_id {}", &self.client_id);
        } else {
            tracing::debug!("not in the scope of request context");
        }

        String::new()
    }

    fn dyn_hash(&self) -> String {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish().to_string()
    }
}

impl DatabricksU2MTokenProvider {
    #[must_use]
    pub fn new(endpoint: String, client_id: String) -> Self {
        Self {
            endpoint,
            client_id,
        }
    }
}

// ============================================================================
// Token Provider Helper Functions
// ============================================================================

#[cfg(feature = "databricks")]
use crate::parameters::Parameters;
#[cfg(feature = "databricks")]
use token_provider::StaticTokenProvider;
#[cfg(feature = "databricks")]
use token_provider::registry::TokenProviderRegistry;

/// Build auth credentials from parameters.
#[cfg(feature = "databricks")]
pub fn build_auth_credentials(params: &Parameters) -> Result<AuthCredentials<'_>, AuthConfigError> {
    let token = params.get("token").ok();
    let client_id = params.get("client_id").expose().ok();
    let client_secret = params.get("client_secret").ok();

    match (token, client_id, client_secret) {
        (Some(token), None, None) => Ok(AuthCredentials::Token(token)),
        (None, Some(client_id), None) => Ok(AuthCredentials::U2M(client_id)),
        (None, Some(client_id), Some(client_secret)) => {
            Ok(AuthCredentials::ServicePrincipal(client_id, client_secret))
        }
        (None, None, None) => Err(AuthConfigError::InvalidConfiguration {
            message: "Missing `databricks_token` or `databricks_client_id` and `databricks_client_secret` parameters".to_string(),
        }),
        (None, None, Some(_)) => Err(AuthConfigError::MissingParameter {
            parameter: "databricks_client_id".to_string(),
        }),
        (Some(_), Some(_), Some(_) | None) => Err(AuthConfigError::InvalidConfiguration {
            message: "Choose either `databricks_token` or `databricks_client_id` and `databricks_client_secret`".to_string(),
        }),
        _ => Err(AuthConfigError::InvalidConfiguration {
            message: "Invalid authentication configuration. Choose either `databricks_token` or `databricks_client_id` and `databricks_client_secret`".to_string(),
        }),
    }
}

/// Error type for auth configuration.
#[derive(Debug, Snafu)]
#[cfg(feature = "databricks")]
pub enum AuthConfigError {
    #[snafu(display("Missing required parameter: {parameter}"))]
    MissingParameter { parameter: String },

    #[snafu(display("Invalid configuration: {message}"))]
    InvalidConfiguration { message: String },
}

/// Get a token provider based on auth credentials.
#[cfg(feature = "databricks")]
pub async fn get_token_provider(
    endpoint: &str,
    auth_credentials: AuthCredentials<'_>,
    token_provider_registry: Arc<TokenProviderRegistry>,
) -> Result<Arc<dyn TokenProvider>, Error> {
    Ok(match auth_credentials {
        AuthCredentials::Token(token) => Arc::new(StaticTokenProvider::new(token.clone())),
        AuthCredentials::ServicePrincipal(client_id, client_secret) => {
            get_m2m_token_provider(endpoint, client_id, client_secret, &token_provider_registry)
                .await?
        }
        AuthCredentials::U2M(client_id) => {
            get_u2m_token_provider(endpoint, client_id, &token_provider_registry).await?
        }
    })
}

/// Get or create an M2M token provider.
#[cfg(feature = "databricks")]
pub async fn get_m2m_token_provider(
    endpoint: &str,
    client_id: &str,
    client_secret: &SecretString,
    token_provider_registry: &Arc<TokenProviderRegistry>,
) -> Result<Arc<dyn TokenProvider>, Error> {
    token_provider_registry
        .get_or_create_provider(format!("databricks_m2m_{endpoint}_{client_id}"), || async {
            DatabricksM2MTokenProvider::try_new(
                endpoint.to_string(),
                client_id.to_string(),
                client_secret.clone(),
            )
            .await
        })
        .await
        .map_err(|e| Error::UnableToGetToken {
            source: Box::new(e),
        })
}

/// Get or create a U2M token provider.
#[cfg(feature = "databricks")]
pub async fn get_u2m_token_provider(
    endpoint: &str,
    client_id: &str,
    token_provider_registry: &Arc<TokenProviderRegistry>,
) -> Result<Arc<dyn TokenProvider>, Error> {
    token_provider_registry
        .get_or_create_provider::<DatabricksU2MTokenProvider, std::convert::Infallible, _, _>(
            format!("databricks_u2m_{endpoint}_{client_id}"),
            || async {
                Ok(DatabricksU2MTokenProvider::new(
                    endpoint.to_string(),
                    client_id.to_string(),
                ))
            },
        )
        .await
        .map_err(|err| Error::UnableToGetToken {
            source: Box::new(err),
        })
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

    #[derive(Clone)]
    struct MockHttpResponse {
        status_line: &'static str,
        headers: Vec<(String, String)>,
        body: String,
    }

    impl MockHttpResponse {
        fn json(status_line: &'static str, body: &serde_json::Value) -> Self {
            Self {
                status_line,
                headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                body: serde_json::to_string(&body).expect("mock JSON should serialize"),
            }
        }
    }

    async fn start_mock_server(
        responses: Vec<MockHttpResponse>,
    ) -> (
        String,
        Arc<AtomicUsize>,
        Arc<tokio::sync::Mutex<Vec<String>>>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("should bind to a port");
        let addr = listener
            .local_addr()
            .expect("should have a listener address");
        let responses = Arc::new(tokio::sync::Mutex::new(VecDeque::from(responses)));
        let requests = Arc::new(AtomicUsize::new(0));
        let captured_requests = Arc::new(tokio::sync::Mutex::new(Vec::new()));

        let requests_for_server = Arc::clone(&requests);
        let captured_requests_for_server = Arc::clone(&captured_requests);
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };

                let responses = Arc::clone(&responses);
                let requests = Arc::clone(&requests_for_server);
                let captured_requests = Arc::clone(&captured_requests_for_server);
                tokio::spawn(async move {
                    use tokio::io::AsyncWriteExt;

                    let captured_request = read_http_request(&mut stream).await;
                    requests.fetch_add(1, Ordering::SeqCst);
                    captured_requests
                        .lock()
                        .await
                        .push(String::from_utf8_lossy(&captured_request).into_owned());

                    let response =
                        responses.lock().await.pop_front().unwrap_or_else(|| {
                            MockHttpResponse::json("200 OK", &json!({"ok": true}))
                        });

                    let mut http_response = format!(
                        "HTTP/1.1 {}\r\nContent-Length: {}\r\n",
                        response.status_line,
                        response.body.len()
                    );
                    for (header_name, header_value) in response.headers {
                        let _ = std::fmt::Write::write_fmt(
                            &mut http_response,
                            format_args!("{header_name}: {header_value}\r\n"),
                        );
                    }
                    http_response.push_str("\r\n");
                    http_response.push_str(&response.body);

                    let _ = stream.write_all(http_response.as_bytes()).await;
                });
            }
        });

        (format!("http://{addr}"), requests, captured_requests)
    }

    async fn read_http_request(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
        use tokio::io::AsyncReadExt;

        let mut captured_request = Vec::with_capacity(4096);
        let mut buf = [0u8; 1024];
        let mut expected_total_len = None;

        loop {
            let bytes_read = match stream.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(bytes_read) => bytes_read,
            };

            captured_request.extend_from_slice(&buf[..bytes_read]);

            if expected_total_len.is_none() {
                expected_total_len = expected_http_request_len(&captured_request);
            }

            if let Some(expected_total_len) = expected_total_len
                && captured_request.len() >= expected_total_len
            {
                break;
            }
        }

        captured_request
    }

    fn expected_http_request_len(request: &[u8]) -> Option<usize> {
        let headers_end = request
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .map(|position| position + 4)?;

        let content_length = String::from_utf8_lossy(&request[..headers_end])
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                if name.trim().eq_ignore_ascii_case("Content-Length") {
                    value.trim().parse::<usize>().ok()
                } else {
                    None
                }
            })
            .unwrap_or(0);

        Some(headers_end.saturating_add(content_length))
    }

    fn token_response_body() -> serde_json::Value {
        json!({
            "access_token": "test-access-token",
            "token_type": "Bearer",
            "expires_in": 3600,
            "scope": "all-apis"
        })
    }

    #[test]
    fn test_databricks_token_endpoint_url_normalizes_host() {
        assert_eq!(
            databricks_token_endpoint_url("dbc.example.databricks.com")
                .expect("plain hostname should be accepted"),
            "https://dbc.example.databricks.com/oidc/v1/token"
        );
        assert_eq!(
            databricks_token_endpoint_url("http://127.0.0.1:1234/")
                .expect("http://localhost should be allowed"),
            "http://127.0.0.1:1234/oidc/v1/token"
        );
        assert!(
            databricks_token_endpoint_url("http://dbc.example.databricks.com").is_err(),
            "http:// to non-localhost should be rejected"
        );
        assert_eq!(
            databricks_token_endpoint_url("http://[::1]:1234/")
                .expect("http://[::1] should be allowed"),
            "http://[::1]:1234/oidc/v1/token"
        );
    }

    #[test]
    fn test_next_token_refresh_wait_clamps_short_lived_tokens() {
        assert_eq!(next_token_refresh_wait(0), Duration::from_secs(1));
        assert_eq!(
            next_token_refresh_wait(TOKEN_REFRESH_BUFFER_SECS),
            Duration::from_secs(1)
        );
        assert_eq!(
            next_token_refresh_wait(TOKEN_REFRESH_BUFFER_SECS + 15),
            Duration::from_secs(15)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_get_m2m_access_token_retries_rate_limited_response() {
        let (endpoint, requests, _) = start_mock_server(vec![
            MockHttpResponse {
                status_line: "429 Too Many Requests",
                headers: vec![
                    ("Content-Type".to_string(), "application/json".to_string()),
                    ("Retry-After".to_string(), "0".to_string()),
                ],
                body: json!({"error": "rate limited"}).to_string(),
            },
            MockHttpResponse::json("200 OK", &token_response_body()),
        ])
        .await;

        let client = build_m2m_token_client().expect("should build token client");

        let response = get_m2m_access_token(
            &client,
            &endpoint,
            "client-id",
            &SecretString::from("client-secret"),
        )
        .await
        .expect("token request should succeed after retrying the rate-limited response");

        assert_eq!(response.expires_in, 3600);
        assert_eq!(requests.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_get_m2m_access_token_retries_server_error_response() {
        let (endpoint, requests, _) = start_mock_server(vec![
            MockHttpResponse::json("503 Service Unavailable", &json!({"error": "busy"})),
            MockHttpResponse::json("200 OK", &token_response_body()),
        ])
        .await;

        let client = build_m2m_token_client().expect("should build token client");

        let response = get_m2m_access_token(
            &client,
            &endpoint,
            "client-id",
            &SecretString::from("client-secret"),
        )
        .await
        .expect("token request should succeed after retrying the transient server error");

        assert_eq!(response.expires_in, 3600);
        assert_eq!(requests.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_get_m2m_access_token_requests_supported_encodings() {
        let (endpoint, requests, captured_requests) =
            start_mock_server(vec![MockHttpResponse::json(
                "200 OK",
                &token_response_body(),
            )])
            .await;

        let client = build_m2m_token_client().expect("should build token client");

        let response = get_m2m_access_token(
            &client,
            &endpoint,
            "client-id",
            &SecretString::from("client-secret"),
        )
        .await
        .expect("token request should succeed");

        let request = captured_requests
            .lock()
            .await
            .remove(0)
            .to_ascii_lowercase();

        assert_eq!(response.expires_in, 3600);
        assert_eq!(requests.load(Ordering::SeqCst), 1);
        assert!(
            request.contains("accept-encoding: zstd, br, gzip, deflate"),
            "request should advertise all supported encodings: {request}"
        );
    }
}
