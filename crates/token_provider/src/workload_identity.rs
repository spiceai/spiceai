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

//! Workload identity federation: a [`TokenProvider`] that exchanges an identity token issued by
//! the platform a workload runs on for a short-lived model provider access token, and exchanges
//! again before that access token expires.
//!
//! The access tokens are short-lived by design. An `OpenAI` token never outlives the identity
//! token it was exchanged for, so one exchanged for a GitHub Actions OIDC token (which lives five
//! minutes) lasts at most five minutes. A client that holds one token for its whole life loses the
//! provider after that, which is why the exchange repeats in the background and every request
//! reads the current token through [`TokenProvider::get_token`].

use std::{
    fmt,
    hash::{DefaultHasher, Hash, Hasher},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use serde_json::{Value, json};
use snafu::prelude::*;
use tokio::sync::watch;
use tokio::time::sleep;
use util::fibonacci_backoff::{Backoff as _, FibonacciBackoffBuilder};

use crate::TokenProvider;

/// Anthropic's token endpoint.
const ANTHROPIC_TOKEN_URL: &str = "https://api.anthropic.com/v1/oauth/token";

/// The audience GitHub Actions is asked to issue an identity token for when exchanging it with
/// Anthropic.
pub const ANTHROPIC_AUDIENCE: &str = "https://api.anthropic.com";

/// `OpenAI`'s token endpoint.
const OPENAI_TOKEN_URL: &str = "https://auth.openai.com/oauth/token";

const ANTHROPIC_AUTHENTICATION_HISTORY: &str =
    "https://platform.claude.com/settings/workload-identity-federation?tab=history";
const OPENAI_WORKLOAD_IDENTITY_SETTINGS: &str =
    "https://platform.openai.com/settings/organization/security/workload-identity-provider";

/// A token is exchanged again this long before it expires, or after three quarters of its life
/// when that is shorter.
const MAX_REFRESH_LEAD: Duration = Duration::from_mins(1);

/// The longest wait between retries of a failed exchange. It is short because a token exchanged
/// for a GitHub Actions identity token lives five minutes: retries must fit in the time the
/// current token has left.
const MAX_RETRY_WAIT: Duration = Duration::from_secs(15);

/// The shortest wait before exchanging again, so a token reported as already expiring cannot make
/// the refresh loop exchange back to back.
const MIN_REFRESH_WAIT: Duration = Duration::from_secs(1);

/// Bounds the reason quoted from a provider's error response.
const MAX_REASON_LEN: usize = 300;

/// How much of an error response is read for its reason.
const MAX_ERROR_BODY_BYTES: usize = 4096;

#[derive(Debug, Snafu)]
pub enum WorkloadIdentityError {
    #[snafu(display(
        "Failed to get a workload identity token from GitHub Actions: `ACTIONS_ID_TOKEN_REQUEST_URL` and `ACTIONS_ID_TOKEN_REQUEST_TOKEN` are not set. Give the job `permissions: id-token: write`."
    ))]
    GitHubActionsEnvironmentMissing,

    #[snafu(display("Failed to request a workload identity token from GitHub Actions: {source}"))]
    SubjectTokenRequest { source: reqwest::Error },

    #[snafu(display(
        "GitHub Actions refused the workload identity token request (HTTP {status}): {reason}. Give the job `permissions: id-token: write`."
    ))]
    SubjectTokenRefused {
        status: reqwest::StatusCode,
        reason: String,
    },

    #[snafu(display(
        "Failed to read the workload identity token GitHub Actions returned: {source}"
    ))]
    SubjectTokenResponse { source: reqwest::Error },

    #[snafu(display(
        "Failed to read the workload identity token GitHub Actions returned: {source}"
    ))]
    SubjectTokenBody { source: http_body_limit::Error },

    #[snafu(display(
        "Failed to read the workload identity token file '{}': {source}. Verify that the platform mounts the token at that path.",
        path.display()
    ))]
    SubjectTokenFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("The workload identity token from {origin} is empty."))]
    EmptySubjectToken { origin: String },

    #[snafu(display("Failed to create the HTTP client for workload identity: {source}"))]
    HttpClient { source: reqwest::Error },

    #[snafu(display(
        "Failed to reach {provider}'s token endpoint {token_url} to exchange the workload identity token: {source}"
    ))]
    ExchangeRequest {
        provider: &'static str,
        token_url: String,
        source: reqwest::Error,
    },

    #[snafu(display(
        "{provider} refused to exchange the workload identity token for an access token (HTTP {status}): {reason}. {hint}"
    ))]
    ExchangeRefused {
        provider: &'static str,
        status: reqwest::StatusCode,
        reason: String,
        hint: String,
    },

    #[snafu(display("Failed to read {provider}'s token exchange response: {source}"))]
    ExchangeResponse {
        provider: &'static str,
        source: reqwest::Error,
    },

    #[snafu(display("Failed to read {provider}'s token exchange response: {source}"))]
    ExchangeBody {
        provider: &'static str,
        source: http_body_limit::Error,
    },

    #[snafu(display(
        "{provider} issued a '{token_type}' token, which cannot be sent as a bearer token."
    ))]
    UnsupportedTokenType {
        provider: &'static str,
        token_type: String,
    },
}

pub type Result<T, E = WorkloadIdentityError> = std::result::Result<T, E>;

/// Where the identity token that is exchanged comes from. A source is read again for every
/// exchange: identity tokens are short-lived, and one that carries a `jti` claim can be exchanged
/// only once.
#[derive(Clone)]
pub enum SubjectTokenSource {
    /// The OIDC token endpoint of a GitHub Actions job, requested for `audience`. The job needs
    /// `permissions: id-token: write`.
    GitHubActions {
        request_url: String,
        request_token: SecretString,
        audience: String,
    },
    /// A file that holds the token, such as one Kubernetes projects into a pod and rotates.
    File(PathBuf),
}

impl fmt::Debug for SubjectTokenSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GitHubActions { audience, .. } => f
                .debug_struct("GitHubActions")
                .field("audience", audience)
                .finish_non_exhaustive(),
            Self::File(path) => f.debug_tuple("File").field(path).finish(),
        }
    }
}

impl SubjectTokenSource {
    /// The OIDC token endpoint of the GitHub Actions job this process runs in, from the
    /// `ACTIONS_ID_TOKEN_REQUEST_URL` and `ACTIONS_ID_TOKEN_REQUEST_TOKEN` variables GitHub sets
    /// for a job with `permissions: id-token: write`.
    ///
    /// # Errors
    ///
    /// [`WorkloadIdentityError::GitHubActionsEnvironmentMissing`] when either variable is unset
    /// or empty.
    pub fn github_actions_from_env(audience: impl Into<String>) -> Result<Self> {
        Self::github_actions(
            std::env::var("ACTIONS_ID_TOKEN_REQUEST_URL").ok(),
            std::env::var("ACTIONS_ID_TOKEN_REQUEST_TOKEN").ok(),
            audience,
        )
    }

    fn github_actions(
        request_url: Option<String>,
        request_token: Option<String>,
        audience: impl Into<String>,
    ) -> Result<Self> {
        match (request_url, request_token) {
            (Some(request_url), Some(request_token))
                if !request_url.is_empty() && !request_token.is_empty() =>
            {
                Ok(Self::GitHubActions {
                    request_url,
                    request_token: SecretString::from(request_token),
                    audience: audience.into(),
                })
            }
            _ => GitHubActionsEnvironmentMissingSnafu.fail(),
        }
    }

    fn describe(&self) -> String {
        match self {
            Self::GitHubActions { .. } => "GitHub Actions".to_string(),
            Self::File(path) => format!("the file '{}'", path.display()),
        }
    }

    async fn fetch(&self, http: &reqwest::Client) -> Result<SecretString> {
        let token = match self {
            Self::GitHubActions {
                request_url,
                request_token,
                audience,
            } => {
                let response = http
                    .get(request_url)
                    .query(&[("audience", audience.as_str())])
                    .bearer_auth(request_token.expose_secret())
                    .send()
                    .await
                    .context(SubjectTokenRequestSnafu)?;
                let status = response.status();
                if !status.is_success() {
                    let body = http_body_limit::read_prefix(response, MAX_ERROR_BODY_BYTES).await;
                    return SubjectTokenRefusedSnafu {
                        status,
                        reason: truncate(&String::from_utf8_lossy(&body)),
                    }
                    .fail();
                }

                http_body_limit::buffer(response, http_body_limit::Limit::METADATA)
                    .await
                    .context(SubjectTokenBodySnafu)?
                    .json::<IdTokenResponse>()
                    .await
                    .context(SubjectTokenResponseSnafu)?
                    .value
            }
            Self::File(path) => {
                let contents = tokio::fs::read_to_string(path)
                    .await
                    .context(SubjectTokenFileSnafu { path: path.clone() })?;
                SecretString::from(contents.trim().to_string())
            }
        };
        ensure!(
            !token.expose_secret().is_empty(),
            EmptySubjectTokenSnafu {
                origin: self.describe()
            }
        );
        Ok(token)
    }
}

/// The provider an identity token is exchanged with, and the identifiers the exchange names. None
/// of them is secret, so they also identify the provider in [`TokenProvider::dyn_hash`].
#[derive(Clone, Debug, Hash)]
pub enum Exchange {
    /// Anthropic's RFC 7523 `jwt-bearer` grant. `workspace_id` is required when the federation
    /// rule is enabled for more than one workspace.
    Anthropic {
        token_url: String,
        federation_rule_id: String,
        organization_id: String,
        service_account_id: String,
        workspace_id: Option<String>,
    },
    /// `OpenAI`'s RFC 8693 token exchange. The project the token acts in is the one of the
    /// service account the provider's mapping resolves.
    OpenAi {
        token_url: String,
        identity_provider_id: String,
        service_account_id: String,
    },
}

impl Exchange {
    #[must_use]
    pub fn anthropic(
        federation_rule_id: impl Into<String>,
        organization_id: impl Into<String>,
        service_account_id: impl Into<String>,
        workspace_id: Option<String>,
    ) -> Self {
        Self::Anthropic {
            token_url: ANTHROPIC_TOKEN_URL.to_string(),
            federation_rule_id: federation_rule_id.into(),
            organization_id: organization_id.into(),
            service_account_id: service_account_id.into(),
            workspace_id,
        }
    }

    #[must_use]
    pub fn openai(
        identity_provider_id: impl Into<String>,
        service_account_id: impl Into<String>,
    ) -> Self {
        Self::OpenAi {
            token_url: OPENAI_TOKEN_URL.to_string(),
            identity_provider_id: identity_provider_id.into(),
            service_account_id: service_account_id.into(),
        }
    }

    fn provider(&self) -> &'static str {
        match self {
            Self::Anthropic { .. } => "Anthropic",
            Self::OpenAi { .. } => "OpenAI",
        }
    }

    fn token_url(&self) -> &str {
        match self {
            Self::Anthropic { token_url, .. } | Self::OpenAi { token_url, .. } => token_url,
        }
    }

    fn body(&self, subject_token: &SecretString) -> Value {
        match self {
            Self::Anthropic {
                federation_rule_id,
                organization_id,
                service_account_id,
                workspace_id,
                ..
            } => {
                let mut body = json!({
                    "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                    "assertion": subject_token.expose_secret(),
                    "federation_rule_id": federation_rule_id,
                    "organization_id": organization_id,
                    "service_account_id": service_account_id,
                });
                if let Some(workspace_id) = workspace_id {
                    body["workspace_id"] = Value::String(workspace_id.clone());
                }
                body
            }
            Self::OpenAi {
                identity_provider_id,
                service_account_id,
                ..
            } => json!({
                "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
                "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
                "subject_token": subject_token.expose_secret(),
                "identity_provider_id": identity_provider_id,
                "service_account_id": service_account_id,
            }),
        }
    }

    /// What to check when the provider refuses an exchange.
    fn refusal_hint(&self) -> String {
        match self {
            Self::Anthropic { .. } => format!(
                "Anthropic records why it refused an exchange only in its authentication history: {ANTHROPIC_AUTHENTICATION_HISTORY}"
            ),
            Self::OpenAi { .. } => format!(
                "Verify the identity provider's issuer and audience, and that a service account mapping matches the token's claims: {OPENAI_WORKLOAD_IDENTITY_SETTINGS}"
            ),
        }
    }
}

struct AccessToken {
    token: SecretString,
    expires_in: Duration,
}

/// GitHub Actions' answer to an OIDC token request.
#[derive(Deserialize)]
struct IdTokenResponse {
    value: SecretString,
}

/// A provider's answer to a token exchange (RFC 6749 section 5.1).
#[derive(Deserialize)]
struct TokenResponse {
    access_token: SecretString,
    #[serde(default)]
    token_type: Option<String>,
    #[serde(default)]
    expires_in: Option<u64>,
}

/// How long to keep a token that expires in `expires_in` before exchanging again.
fn refresh_after(expires_in: Duration) -> Duration {
    let lead = MAX_REFRESH_LEAD.min(expires_in / 4);
    expires_in.saturating_sub(lead).max(MIN_REFRESH_WAIT)
}

/// `text` as a clause of a longer message: trimmed, without a trailing period, and bounded.
fn truncate(text: &str) -> String {
    let text = util::single_line(text);
    let text = text.trim().trim_end_matches('.');
    match text.char_indices().nth(MAX_REASON_LEN) {
        Some((end, _)) => format!("{}…", &text[..end]),
        None => text.to_string(),
    }
}

/// The reason in a provider's error response: Anthropic's `error.message`, an `OAuth`
/// `error` with its `error_description`, or the body itself.
fn refusal_reason(body: &str) -> String {
    let Ok(value) = serde_json::from_str::<Value>(body) else {
        return truncate(body);
    };
    match &value["error"] {
        Value::Object(error) => match error.get("message").and_then(Value::as_str) {
            Some(message) => truncate(message),
            None => truncate(body),
        },
        Value::String(code) => match value["error_description"].as_str() {
            Some(description) => truncate(&format!("{code}: {description}")),
            None => truncate(code),
        },
        _ => truncate(body),
    }
}

async fn exchange(
    http: &reqwest::Client,
    source: &SubjectTokenSource,
    exchange: &Exchange,
) -> Result<AccessToken> {
    let subject_token = source.fetch(http).await?;
    let provider = exchange.provider();
    let response = http
        .post(exchange.token_url())
        .json(&exchange.body(&subject_token))
        .send()
        .await
        .context(ExchangeRequestSnafu {
            provider,
            token_url: exchange.token_url().to_string(),
        })?;
    let status = response.status();
    if !status.is_success() {
        let body = http_body_limit::read_prefix(response, MAX_ERROR_BODY_BYTES).await;
        return ExchangeRefusedSnafu {
            provider,
            status,
            reason: refusal_reason(&String::from_utf8_lossy(&body)),
            hint: exchange.refusal_hint(),
        }
        .fail();
    }

    let token: TokenResponse = http_body_limit::buffer(response, http_body_limit::Limit::METADATA)
        .await
        .context(ExchangeBodySnafu { provider })?
        .json()
        .await
        .context(ExchangeResponseSnafu { provider })?;
    if let Some(token_type) = token.token_type
        && !token_type.eq_ignore_ascii_case("bearer")
    {
        return UnsupportedTokenTypeSnafu {
            provider,
            token_type,
        }
        .fail();
    }
    Ok(AccessToken {
        token: token.access_token,
        // Both providers report the lifetime. A response without one is treated as the shortest
        // lifetime either issues, so the token is exchanged again in good time.
        expires_in: Duration::from_secs(token.expires_in.unwrap_or(300)),
    })
}

/// A [`TokenProvider`] backed by workload identity federation. Construction performs the first
/// exchange, so a misconfigured identity fails there; a background task then exchanges again
/// before each token expires, and stops when the provider is dropped.
pub struct WorkloadIdentityTokenProvider {
    source: Arc<SubjectTokenSource>,
    exchange: Arc<Exchange>,
    rx: watch::Receiver<SecretString>,
}

impl fmt::Debug for WorkloadIdentityTokenProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkloadIdentityTokenProvider")
            .field("source", &self.source)
            .field("exchange", &self.exchange)
            .finish_non_exhaustive()
    }
}

impl TokenProvider for WorkloadIdentityTokenProvider {
    fn get_token(&self) -> String {
        self.rx.borrow().expose_secret().to_string()
    }

    fn dyn_hash(&self) -> String {
        // Identifiers only: never the identity token or the access token.
        let mut hasher = DefaultHasher::new();
        self.exchange.hash(&mut hasher);
        match self.source.as_ref() {
            SubjectTokenSource::GitHubActions { audience, .. } => {
                "github_actions".hash(&mut hasher);
                audience.hash(&mut hasher);
            }
            SubjectTokenSource::File(path) => {
                "file".hash(&mut hasher);
                path.hash(&mut hasher);
            }
        }
        hasher.finish().to_string()
    }

    fn subscribe(&self) -> Option<watch::Receiver<String>> {
        let mut secret_rx = self.rx.clone();
        let (tx, rx) = watch::channel(secret_rx.borrow().expose_secret().to_string());
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = tx.closed() => break,
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

impl WorkloadIdentityTokenProvider {
    /// Exchanges an identity token from `source` with `exchange`'s provider, and keeps
    /// exchanging before each access token expires.
    ///
    /// # Errors
    ///
    /// When the first identity token cannot be read, or the provider refuses or cannot be
    /// reached for the first exchange.
    pub async fn try_new(source: SubjectTokenSource, exchange: Exchange) -> Result<Self> {
        let http = reqwest::Client::builder()
            .user_agent(util::spiceai_user_agent())
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            // An exchange never needs a redirect, and following a 307 or 308 would send the
            // identity token in the request body to wherever it points.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .context(HttpClientSnafu)?;
        let source = Arc::new(source);
        let exchange = Arc::new(exchange);

        let first = self::exchange(&http, &source, &exchange).await?;
        tracing::debug!(
            "Exchanged a workload identity token from {} for an {} access token that expires in {}s",
            source.describe(),
            exchange.provider(),
            first.expires_in.as_secs()
        );
        let first_expires_in = first.expires_in;
        let (tx, rx) = watch::channel(first.token);

        let task_source = Arc::clone(&source);
        let task_exchange = Arc::clone(&exchange);
        tokio::spawn(async move {
            let mut backoff = FibonacciBackoffBuilder::new()
                .max_duration(Some(MAX_RETRY_WAIT))
                .build();
            let mut next_wait = refresh_after(first_expires_in);
            loop {
                // A dropped provider closes the channel: stop exchanging tokens nothing reads.
                tokio::select! {
                    () = sleep(next_wait) => {}
                    () = tx.closed() => break,
                }
                match self::exchange(&http, &task_source, &task_exchange).await {
                    Ok(token) => {
                        backoff.reset();
                        tracing::debug!(
                            "Refreshed the {} access token; it expires in {}s",
                            task_exchange.provider(),
                            token.expires_in.as_secs()
                        );
                        next_wait = refresh_after(token.expires_in);
                        if tx.send(token.token).is_err() {
                            break;
                        }
                    }
                    Err(error) => {
                        next_wait = backoff.next_duration().unwrap_or(MAX_RETRY_WAIT);
                        tracing::warn!(
                            "Failed to refresh the {} access token, so requests keep the current one until it expires. Retrying in {}s. Cause: {error}",
                            task_exchange.provider(),
                            next_wait.as_secs()
                        );
                    }
                }
            }
        });

        Ok(Self {
            source,
            exchange,
            rx,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::{
        Json, Router,
        extract::{Query, State},
        http::{HeaderMap, StatusCode},
        response::{IntoResponse, Response},
        routing::{get, post},
    };

    use super::*;

    /// The query and `Authorization` header of a request for an identity token.
    type IdTokenRequest = (HashMap<String, String>, Option<String>);

    /// What the mock GitHub Actions and token endpoints received, and how the token endpoint
    /// answers.
    #[derive(Default)]
    struct Mock {
        id_token_requests: Mutex<Vec<IdTokenRequest>>,
        exchanges: Mutex<Vec<Value>>,
        id_tokens_issued: AtomicUsize,
        access_tokens_issued: AtomicUsize,
        expires_in: AtomicUsize,
        /// The exchange (1-based) to refuse once with a 503, or 0 for none.
        refuse_exchange: AtomicUsize,
    }

    async fn id_token(
        State(mock): State<Arc<Mock>>,
        Query(query): Query<HashMap<String, String>>,
        headers: HeaderMap,
    ) -> Json<Value> {
        let authorization = headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        mock.id_token_requests
            .lock()
            .expect("lock")
            .push((query, authorization));
        let n = mock.id_tokens_issued.fetch_add(1, Ordering::SeqCst) + 1;
        Json(json!({"value": format!("jwt-{n}")}))
    }

    async fn token(State(mock): State<Arc<Mock>>, Json(body): Json<Value>) -> Response {
        let attempt = {
            let mut exchanges = mock.exchanges.lock().expect("lock");
            exchanges.push(body);
            exchanges.len()
        };
        if mock.refuse_exchange.load(Ordering::SeqCst) == attempt {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "temporarily_unavailable", "error_description": "try again"})),
            )
                .into_response();
        }
        let n = mock.access_tokens_issued.fetch_add(1, Ordering::SeqCst) + 1;
        Json(json!({
            "access_token": format!("tok-{n}"),
            "token_type": "Bearer",
            "expires_in": mock.expires_in.load(Ordering::SeqCst),
        }))
        .into_response()
    }

    async fn refuse_anthropic() -> Response {
        (
            StatusCode::UNAUTHORIZED,
            Json(json!({"type": "error", "error": {"type": "authentication_error", "message": "Authentication failed"}, "request_id": "req_1"})),
        )
            .into_response()
    }

    async fn redirect_to_token() -> Response {
        (
            StatusCode::TEMPORARY_REDIRECT,
            [(axum::http::header::LOCATION, "/token")],
        )
            .into_response()
    }

    async fn refuse_openai() -> Response {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid_grant", "error_description": "No service account mapping matched the token."})),
        )
            .into_response()
    }

    /// Starts the mock endpoints; access tokens it issues expire in `expires_in` seconds.
    async fn start(expires_in: usize) -> (String, Arc<Mock>) {
        let mock = Arc::new(Mock::default());
        mock.expires_in.store(expires_in, Ordering::SeqCst);
        let app = Router::new()
            .route("/id-token", get(id_token))
            .route("/token", post(token))
            .route("/refuse/anthropic", post(refuse_anthropic))
            .route("/refuse/openai", post(refuse_openai))
            .route("/redirect", post(redirect_to_token))
            .with_state(Arc::clone(&mock));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the mock endpoints");
        let address = listener.local_addr().expect("mock address");
        tokio::spawn(async move { axum::serve(listener, app).await });
        (format!("http://{address}"), mock)
    }

    fn github_actions(base: &str, audience: &str) -> SubjectTokenSource {
        SubjectTokenSource::github_actions(
            Some(format!("{base}/id-token?api-version=2.0")),
            Some("request-token".to_string()),
            audience,
        )
        .expect("a GitHub Actions source")
    }

    fn openai_at(token_url: String) -> Exchange {
        Exchange::OpenAi {
            token_url,
            identity_provider_id: "idp_1".to_string(),
            service_account_id: "user-1".to_string(),
        }
    }

    fn anthropic_at(token_url: String, workspace_id: Option<&str>) -> Exchange {
        Exchange::Anthropic {
            token_url,
            federation_rule_id: "fdrl_1".to_string(),
            organization_id: "org-1".to_string(),
            service_account_id: "svac_1".to_string(),
            workspace_id: workspace_id.map(str::to_string),
        }
    }

    /// Polls `provider` until it serves `expected`, failing with the last token it served.
    async fn wait_for_token(provider: &WorkloadIdentityTokenProvider, expected: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let current = provider.get_token();
            if current == expected {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the provider still serves '{current}', not '{expected}'"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    #[tokio::test]
    async fn an_openai_token_is_exchanged_again_with_a_new_identity_token_before_it_expires() {
        let (base, mock) = start(4).await;
        let provider = WorkloadIdentityTokenProvider::try_new(
            github_actions(&base, "https://api.openai.com"),
            openai_at(format!("{base}/token")),
        )
        .await
        .expect("the first exchange");
        assert_eq!(provider.get_token(), "tok-1");

        // A 4 s token is exchanged again after 3 s, well before it expires.
        wait_for_token(&provider, "tok-2").await;

        let exchanges = mock.exchanges.lock().expect("lock").clone();
        assert_eq!(
            exchanges,
            vec![
                json!({
                    "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
                    "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
                    "subject_token": "jwt-1",
                    "identity_provider_id": "idp_1",
                    "service_account_id": "user-1",
                }),
                json!({
                    "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
                    "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
                    "subject_token": "jwt-2",
                    "identity_provider_id": "idp_1",
                    "service_account_id": "user-1",
                }),
            ],
            "each exchange sends a new identity token, since one with a `jti` is accepted once"
        );

        let id_token_requests = mock.id_token_requests.lock().expect("lock").clone();
        assert_eq!(id_token_requests.len(), 2);
        for (query, authorization) in id_token_requests {
            assert_eq!(
                (
                    query.get("api-version").map(String::as_str),
                    query.get("audience").map(String::as_str),
                    authorization.as_deref(),
                ),
                (
                    Some("2.0"),
                    Some("https://api.openai.com"),
                    Some("Bearer request-token")
                ),
            );
        }
    }

    #[tokio::test]
    async fn anthropic_receives_the_jwt_bearer_grant_with_its_identifiers() {
        let (base, mock) = start(3600).await;
        let with_workspace = WorkloadIdentityTokenProvider::try_new(
            github_actions(&base, ANTHROPIC_AUDIENCE),
            anthropic_at(format!("{base}/token"), Some("wrkspc_1")),
        )
        .await
        .expect("an exchange naming the workspace");
        let without_workspace = WorkloadIdentityTokenProvider::try_new(
            github_actions(&base, ANTHROPIC_AUDIENCE),
            anthropic_at(format!("{base}/token"), None),
        )
        .await
        .expect("an exchange naming no workspace");
        assert_eq!(
            (with_workspace.get_token(), without_workspace.get_token()),
            ("tok-1".to_string(), "tok-2".to_string())
        );

        assert_eq!(
            mock.exchanges.lock().expect("lock").clone(),
            vec![
                json!({
                    "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                    "assertion": "jwt-1",
                    "federation_rule_id": "fdrl_1",
                    "organization_id": "org-1",
                    "service_account_id": "svac_1",
                    "workspace_id": "wrkspc_1",
                }),
                json!({
                    "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                    "assertion": "jwt-2",
                    "federation_rule_id": "fdrl_1",
                    "organization_id": "org-1",
                    "service_account_id": "svac_1",
                }),
            ]
        );
    }

    #[tokio::test]
    async fn a_token_file_is_read_again_for_every_exchange() {
        let (base, mock) = start(4).await;
        let dir = tempfile::tempdir().expect("a temporary directory");
        let path = dir.path().join("token");
        tokio::fs::write(&path, "file-jwt-1\n")
            .await
            .expect("write the token file");

        let provider = WorkloadIdentityTokenProvider::try_new(
            SubjectTokenSource::File(path.clone()),
            openai_at(format!("{base}/token")),
        )
        .await
        .expect("the first exchange");
        // The platform rotates the token on disk before the next exchange.
        tokio::fs::write(&path, "file-jwt-2")
            .await
            .expect("rotate the token file");
        wait_for_token(&provider, "tok-2").await;

        let subject_tokens: Vec<Value> = mock
            .exchanges
            .lock()
            .expect("lock")
            .iter()
            .map(|body| body["subject_token"].clone())
            .collect();
        assert_eq!(
            subject_tokens,
            vec![json!("file-jwt-1"), json!("file-jwt-2")]
        );
    }

    #[tokio::test]
    async fn a_failed_refresh_keeps_the_current_token_and_is_retried() {
        let (base, mock) = start(4).await;
        mock.refuse_exchange.store(2, Ordering::SeqCst);
        let provider = WorkloadIdentityTokenProvider::try_new(
            github_actions(&base, "https://api.openai.com"),
            openai_at(format!("{base}/token")),
        )
        .await
        .expect("the first exchange");

        // The second exchange is refused; the third, about a second later, succeeds.
        wait_for_token(&provider, "tok-2").await;
        assert_eq!(mock.exchanges.lock().expect("lock").len(), 3);
    }

    #[tokio::test]
    async fn a_refused_exchange_names_the_provider_its_reason_and_where_to_look() {
        let (base, _mock) = start(3600).await;
        let anthropic = WorkloadIdentityTokenProvider::try_new(
            github_actions(&base, ANTHROPIC_AUDIENCE),
            anthropic_at(format!("{base}/refuse/anthropic"), None),
        )
        .await
        .expect_err("Anthropic refuses the exchange");
        assert_eq!(
            anthropic.to_string(),
            "Anthropic refused to exchange the workload identity token for an access token (HTTP 401 Unauthorized): Authentication failed. Anthropic records why it refused an exchange only in its authentication history: https://platform.claude.com/settings/workload-identity-federation?tab=history"
        );

        let openai = WorkloadIdentityTokenProvider::try_new(
            github_actions(&base, "https://api.openai.com"),
            openai_at(format!("{base}/refuse/openai")),
        )
        .await
        .expect_err("OpenAI refuses the exchange");
        assert_eq!(
            openai.to_string(),
            "OpenAI refused to exchange the workload identity token for an access token (HTTP 400 Bad Request): invalid_grant: No service account mapping matched the token. Verify the identity provider's issuer and audience, and that a service account mapping matches the token's claims: https://platform.openai.com/settings/organization/security/workload-identity-provider"
        );
    }

    #[tokio::test]
    async fn a_redirect_from_the_token_endpoint_is_not_followed() {
        let (base, mock) = start(3600).await;
        let error = WorkloadIdentityTokenProvider::try_new(
            github_actions(&base, "https://api.openai.com"),
            openai_at(format!("{base}/redirect")),
        )
        .await
        .expect_err("a redirected exchange fails");
        assert!(
            error
                .to_string()
                .starts_with("OpenAI refused to exchange the workload identity token for an access token (HTTP 307 Temporary Redirect)"),
            "{error}"
        );
        assert!(
            mock.exchanges.lock().expect("lock").is_empty(),
            "the identity token must not be re-sent to the redirect's target"
        );
    }

    #[test]
    fn github_actions_needs_the_request_url_and_token() {
        for (url, token) in [
            (None, Some("t")),
            (Some("https://example.com"), None),
            (Some(""), Some("t")),
            (None, None),
        ] {
            let error = SubjectTokenSource::github_actions(
                url.map(str::to_string),
                token.map(str::to_string),
                ANTHROPIC_AUDIENCE,
            )
            .expect_err("no GitHub Actions source");
            assert_eq!(
                error.to_string(),
                "Failed to get a workload identity token from GitHub Actions: `ACTIONS_ID_TOKEN_REQUEST_URL` and `ACTIONS_ID_TOKEN_REQUEST_TOKEN` are not set. Give the job `permissions: id-token: write`."
            );
        }
    }

    #[test]
    fn a_token_is_refreshed_a_minute_early_or_after_three_quarters_of_its_life_and_never_at_once() {
        assert_eq!(
            [3600, 600, 299, 4, 0].map(|secs| refresh_after(Duration::from_secs(secs))),
            [
                Duration::from_mins(59),
                Duration::from_mins(9),
                Duration::from_secs(239),
                Duration::from_secs(3),
                MIN_REFRESH_WAIT,
            ]
        );
    }
}
