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
//! the platform a workload runs on for a short-lived provider access token, and exchanges again
//! before that access token expires.
//!
//! The providers are Anthropic, `OpenAI`, Microsoft Entra ID, whose tokens authenticate Azure
//! `OpenAI`, and Databricks.
//!
//! The access tokens are short-lived by design. An `OpenAI` token never outlives the identity
//! token it was exchanged for, so one exchanged for a GitHub Actions OIDC token (which lives five
//! minutes) lasts at most five minutes. A client that holds one token for its whole life loses the
//! provider after that, which is why the exchange repeats in the background and every request
//! reads the current token through [`TokenProvider::get_token`].

use std::{
    fmt,
    hash::{DefaultHasher, Hash, Hasher},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{DateTime, SecondsFormat, Utc};
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use snafu::prelude::*;
use tokio::sync::watch;
use tokio::time::{Instant, sleep};
use util::fibonacci_backoff::{Backoff as _, FibonacciBackoffBuilder};

use crate::TokenProvider;

/// Anthropic's token endpoint.
const ANTHROPIC_TOKEN_URL: &str = "https://api.anthropic.com/v1/oauth/token";

/// The audience GitHub Actions is asked to issue an identity token for when exchanging it with
/// Anthropic.
pub const ANTHROPIC_AUDIENCE: &str = "https://api.anthropic.com";

/// The audience `OpenAI`'s Kubernetes guide recommends for a workload's identity token, which
/// GitHub Actions or a cloud identity service is asked to issue a token for.
pub const OPENAI_AUDIENCE: &str = "https://api.openai.com/v1";

/// `OpenAI`'s token endpoint.
const OPENAI_TOKEN_URL: &str = "https://auth.openai.com/oauth/token";

/// The audience Microsoft Entra ID suggests for the federated credential of an application or
/// managed identity, which GitHub Actions is asked to issue an identity token for.
pub const ENTRA_AUDIENCE: &str = "api://AzureADTokenExchange";

/// The Microsoft Entra ID authority of the Azure public cloud.
const ENTRA_AUTHORITY_HOST: &str = "https://login.microsoftonline.com";

const ENTRA_WORKLOAD_IDENTITY_DOCS: &str =
    "https://learn.microsoft.com/entra/workload-id/workload-identity-federation";

const DATABRICKS_FEDERATION_POLICY_DOCS: &str =
    "https://docs.databricks.com/aws/en/dev-tools/auth/oauth-federation-policy";

const ANTHROPIC_AUTHENTICATION_HISTORY: &str =
    "https://platform.claude.com/settings/workload-identity-federation?tab=history";
const OPENAI_WORKLOAD_IDENTITY_SETTINGS: &str =
    "https://platform.openai.com/settings/organization/security/workload-identity-provider";

/// A token exchanged for a token file is exchanged again at the latest this long before it
/// expires, or after three quarters of its life when that is shorter, when the file has not
/// changed by then. A token exchanged for an identity token Spice requests is exchanged again
/// halfway through its life, leaving the other half for retries.
const MAX_REFRESH_LEAD: Duration = Duration::from_mins(1);

/// The longest wait between retries of a failed exchange. It is short because a token exchanged
/// for a GitHub Actions identity token lives five minutes: retries must fit in the time the
/// current token has left.
const MAX_RETRY_WAIT: Duration = Duration::from_secs(15);

/// The shortest wait before exchanging again, so a token reported as already expiring cannot make
/// the refresh loop exchange back to back.
const MIN_REFRESH_WAIT: Duration = Duration::from_secs(1);

/// How often a token file is checked for a new token: an eighth of the access token's life,
/// within these bounds, so a token the platform rotates is exchanged soon after it is written.
const MAX_FILE_POLL: Duration = Duration::from_secs(10);
const MIN_FILE_POLL: Duration = Duration::from_millis(100);

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

    #[snafu(display("Failed to get a workload identity token from {origin}: {reason}"))]
    SubjectTokenRequested { origin: String, reason: String },

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
        "{provider} refused to exchange the workload identity token for an access token (HTTP {status}): {reason}.{claims} {hint}"
    ))]
    ExchangeRefused {
        provider: &'static str,
        status: reqwest::StatusCode,
        reason: String,
        /// The sentence naming the identity token's claims, with a leading space, or empty.
        claims: String,
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

    #[snafu(display(
        "The Microsoft Entra tenant must be a directory (tenant) ID, which is a GUID, or a domain name such as 'contoso.onmicrosoft.com'."
    ))]
    InvalidEntraTenant,

    #[snafu(display(
        "The Microsoft Entra authority must be an https:// URL, such as https://login.microsoftonline.us, because the workload identity token is sent to it."
    ))]
    InsecureEntraAuthority,
}

pub type Result<T, E = WorkloadIdentityError> = std::result::Result<T, E>;

/// A source Spice requests a fresh identity token from for every exchange, such as a cloud's
/// identity service: the AWS STS `GetWebIdentityToken` API, the Google Cloud metadata server, or
/// a Microsoft Entra managed identity.
#[async_trait]
pub trait IdentityTokenSource: Send + Sync {
    /// A fresh identity token, or why there is none, worded as the cause clause of a message.
    async fn fetch(&self) -> std::result::Result<SecretString, String>;

    /// Where the token comes from, as a message names it, such as `the AWS IAM role (STS
    /// GetWebIdentityToken)`.
    fn describe(&self) -> String;

    /// Identifiers that tell sources apart, never a token, for [`TokenProvider::dyn_hash`].
    fn hash_key(&self) -> String;
}

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
    /// A source Spice requests a token from for every exchange, such as a cloud's identity
    /// service.
    Requested(Arc<dyn IdentityTokenSource>),
}

impl fmt::Debug for SubjectTokenSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GitHubActions { audience, .. } => f
                .debug_struct("GitHubActions")
                .field("audience", audience)
                .finish_non_exhaustive(),
            Self::File(path) => f.debug_tuple("File").field(path).finish(),
            Self::Requested(source) => f
                .debug_tuple("Requested")
                .field(&source.describe())
                .finish(),
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

    /// The OIDC token endpoint of a GitHub Actions job, from the values of its
    /// `ACTIONS_ID_TOKEN_REQUEST_URL` and `ACTIONS_ID_TOKEN_REQUEST_TOKEN` variables.
    ///
    /// # Errors
    ///
    /// [`WorkloadIdentityError::GitHubActionsEnvironmentMissing`] when either is absent or empty.
    pub fn github_actions(
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

    /// Where the token comes from, as a message names it, such as `the file '/var/run/token'`.
    #[must_use]
    pub fn describe(&self) -> String {
        match self {
            Self::GitHubActions { .. } => "GitHub Actions".to_string(),
            Self::File(path) => format!("the file '{}'", path.display()),
            Self::Requested(source) => source.describe(),
        }
    }

    /// A fresh identity token from this source, for a caller that exchanges it itself.
    ///
    /// # Errors
    ///
    /// When the token cannot be read or requested, or is empty.
    pub async fn fetch_identity_token(&self) -> Result<SecretString> {
        self.fetch(&exchange_client()?).await
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
            Self::Requested(source) => source.fetch().await.map_err(|reason| {
                WorkloadIdentityError::SubjectTokenRequested {
                    origin: source.describe(),
                    reason,
                }
            })?,
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
    /// Microsoft Entra ID's client credentials grant with a federated client assertion: the
    /// identity token stands in for the secret of the application or managed identity
    /// `client_id`, whose federated credentials must match its claims.
    Entra {
        token_url: String,
        client_id: String,
        scope: String,
    },
    /// Databricks' RFC 8693 token exchange at a workspace's token endpoint. A federation policy
    /// of the service principal `client_id` must match the identity token's claims.
    Databricks {
        token_url: String,
        client_id: String,
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

    /// An exchange with the Microsoft Entra ID tenant `tenant_id`, for a token of `scope`, such
    /// as `https://cognitiveservices.azure.com/.default`. `authority_host` replaces the public
    /// cloud's `https://login.microsoftonline.com`, as `AZURE_AUTHORITY_HOST` does for the Azure
    /// SDKs.
    ///
    /// # Errors
    ///
    /// [`WorkloadIdentityError::InvalidEntraTenant`] when `tenant_id` is neither a GUID nor a
    /// domain name, so that it cannot change the path of the token endpoint, and
    /// [`WorkloadIdentityError::InsecureEntraAuthority`] when `authority_host` names a scheme
    /// other than `https`, which would send the identity token unencrypted.
    pub fn entra(
        authority_host: Option<&str>,
        tenant_id: &str,
        client_id: impl Into<String>,
        scope: impl Into<String>,
    ) -> Result<Self> {
        ensure!(is_entra_tenant(tenant_id), InvalidEntraTenantSnafu);
        let authority = authority_host
            .map(str::trim)
            .filter(|host| !host.is_empty())
            .unwrap_or(ENTRA_AUTHORITY_HOST)
            .trim_end_matches('/');
        let authority = match authority.split_once("://") {
            Some((scheme, _)) => {
                ensure!(
                    scheme.eq_ignore_ascii_case("https"),
                    InsecureEntraAuthoritySnafu
                );
                authority.to_string()
            }
            None => format!("https://{authority}"),
        };
        Ok(Self::Entra {
            token_url: format!("{authority}/{tenant_id}/oauth2/v2.0/token"),
            client_id: client_id.into(),
            scope: scope.into(),
        })
    }

    /// An exchange at the Databricks workspace token endpoint `token_url`, for a token of the
    /// service principal `client_id`.
    #[must_use]
    pub fn databricks(token_url: impl Into<String>, client_id: impl Into<String>) -> Self {
        Self::Databricks {
            token_url: token_url.into(),
            client_id: client_id.into(),
        }
    }

    fn provider(&self) -> &'static str {
        match self {
            Self::Anthropic { .. } => "Anthropic",
            Self::OpenAi { .. } => "OpenAI",
            Self::Entra { .. } => "Microsoft Entra ID",
            Self::Databricks { .. } => "Databricks",
        }
    }

    fn token_url(&self) -> &str {
        match self {
            Self::Anthropic { token_url, .. }
            | Self::OpenAi { token_url, .. }
            | Self::Entra { token_url, .. }
            | Self::Databricks { token_url, .. } => token_url,
        }
    }

    /// The exchange request: JSON for Anthropic and `OpenAI`, a form for Microsoft Entra ID and
    /// Databricks.
    fn request(
        &self,
        http: &reqwest::Client,
        subject_token: &SecretString,
    ) -> reqwest::RequestBuilder {
        let request = http.post(self.token_url());
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
                request.json(&body)
            }
            Self::OpenAi {
                identity_provider_id,
                service_account_id,
                ..
            } => request.json(&json!({
                "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
                "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
                "subject_token": subject_token.expose_secret(),
                "identity_provider_id": identity_provider_id,
                "service_account_id": service_account_id,
            })),
            Self::Entra {
                client_id, scope, ..
            } => request.form(&[
                ("grant_type", "client_credentials"),
                ("client_id", client_id.as_str()),
                ("scope", scope.as_str()),
                (
                    "client_assertion_type",
                    "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
                ),
                ("client_assertion", subject_token.expose_secret()),
            ]),
            Self::Databricks { client_id, .. } => request.form(&[
                (
                    "grant_type",
                    "urn:ietf:params:oauth:grant-type:token-exchange",
                ),
                ("subject_token_type", "urn:ietf:params:oauth:token-type:jwt"),
                ("subject_token", subject_token.expose_secret()),
                ("client_id", client_id.as_str()),
                ("scope", "all-apis"),
            ]),
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
            Self::Entra { client_id, .. } => format!(
                "Verify that the application or managed identity '{client_id}' has a federated credential whose issuer, subject and audience match the workload identity token: {ENTRA_WORKLOAD_IDENTITY_DOCS}"
            ),
            Self::Databricks { client_id, .. } => format!(
                "Verify that the service principal '{client_id}' has a federation policy whose issuer, subject and audience match the workload identity token: {DATABRICKS_FEDERATION_POLICY_DOCS}"
            ),
        }
    }
}

/// Whether `tenant` is a Microsoft Entra tenant as a token endpoint path names it: a directory ID
/// (a GUID) or a domain name. Nothing else can appear in the path, so a tenant cannot point the
/// exchange at another endpoint.
fn is_entra_tenant(tenant: &str) -> bool {
    !tenant.is_empty()
        && tenant.len() <= 253
        && tenant.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
        })
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

/// How long to keep a token exchanged for a token file that expires in `expires_in` before
/// exchanging again when the file has not changed by then.
fn refresh_after(expires_in: Duration) -> Duration {
    let lead = MAX_REFRESH_LEAD.min(expires_in / 4);
    expires_in.saturating_sub(lead).max(MIN_REFRESH_WAIT)
}

/// How long to keep a token exchanged for an identity token Spice requests (from GitHub Actions
/// or a cloud identity service) before exchanging again: half its life, so a provider or identity
/// service that is briefly unreachable has the other half to come back before requests fail.
fn refresh_after_requested(expires_in: Duration) -> Duration {
    (expires_in / 2).max(MIN_REFRESH_WAIT)
}

/// When to exchange again after a token that expires in `expires_in`: a requested identity token
/// is new every time, while a token file is exchanged as soon as it changes (see
/// [`wait_for_rotation`]) and at the latest by [`refresh_after`].
fn next_refresh(source: &SubjectTokenSource, expires_in: Duration) -> Duration {
    match source {
        SubjectTokenSource::File(_) => refresh_after(expires_in),
        SubjectTokenSource::GitHubActions { .. } | SubjectTokenSource::Requested(_) => {
            refresh_after_requested(expires_in)
        }
    }
}

/// How often a token file is checked for a new token while its access token lives `life`.
fn file_poll(life: Duration) -> Duration {
    (life / 8).clamp(MIN_FILE_POLL, MAX_FILE_POLL)
}

/// The digest of an identity token, to tell whether a token file still holds the token last
/// exchanged.
fn token_digest(token: &str) -> [u8; 32] {
    Sha256::digest(token.trim().as_bytes()).into()
}

/// How a wait for a token file to change ended.
#[derive(Debug, PartialEq, Eq)]
enum Rotation {
    /// The file holds a token other than the one last exchanged.
    Rotated,
    /// `max_wait` passed with the file unchanged.
    Unchanged,
    /// The provider was dropped.
    Closed,
}

/// Waits up to `max_wait` for the token file `path` to hold a token whose digest is not
/// `exchanged`, checking every `poll`. Exchanging a token as soon as the platform rotates it gives
/// the new access token the most time before it expires, and never sends a provider a token it
/// has already exchanged, which one that accepts each `jti` once refuses.
async fn wait_for_rotation<T>(
    path: &Path,
    exchanged: &[u8; 32],
    max_wait: Duration,
    poll: Duration,
    tx: &watch::Sender<T>,
) -> Rotation {
    let deadline = Instant::now() + max_wait;
    loop {
        let now = Instant::now();
        if now >= deadline {
            return Rotation::Unchanged;
        }
        tokio::select! {
            () = sleep(poll.min(deadline - now)) => {}
            () = tx.closed() => return Rotation::Closed,
        }
        if let Ok(contents) = tokio::fs::read_to_string(path).await
            && !contents.trim().is_empty()
            && token_digest(&contents) != *exchanged
        {
            return Rotation::Rotated;
        }
    }
}

/// The claims of an identity token that a provider's federation rule matches: its issuer,
/// subject, audience and expiry, read from the payload without verifying it. They identify the
/// workload but grant nothing, unlike the token, which is never shown.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityTokenClaims {
    pub issuer: Option<String>,
    pub subject: Option<String>,
    pub audience: Vec<String>,
    pub expires_at: Option<DateTime<Utc>>,
}

impl IdentityTokenClaims {
    /// The claims of `token`, or `None` when it is not a JWT whose payload is a JSON object.
    #[must_use]
    pub fn read(token: &str) -> Option<Self> {
        let mut parts = token.trim().split('.');
        let (_header, payload, _signature) = (parts.next()?, parts.next()?, parts.next()?);
        if parts.next().is_some() {
            return None;
        }
        let payload = URL_SAFE_NO_PAD.decode(payload.trim_end_matches('=')).ok()?;
        let Value::Object(claims) = serde_json::from_slice::<Value>(&payload).ok()? else {
            return None;
        };
        let text = |name: &str| claims.get(name).and_then(Value::as_str).map(claim_value);
        let audience = match claims.get("aud") {
            Some(Value::String(audience)) => vec![claim_value(audience)],
            Some(Value::Array(audiences)) => audiences
                .iter()
                .filter_map(Value::as_str)
                .map(claim_value)
                .collect(),
            _ => Vec::new(),
        };
        let expires_at = claims
            .get("exp")
            .and_then(Value::as_i64)
            .and_then(|exp| DateTime::from_timestamp(exp, 0));
        Some(Self {
            issuer: text("iss"),
            subject: text("sub"),
            audience,
            expires_at,
        })
    }

    /// The sentence a refusal adds so the user can compare the token with the provider's rule,
    /// with a leading space, such as ` The identity token from the file '/t' has issuer
    /// 'https://issuer', subject 'system:serviceaccount:spice:spiced', audience
    /// 'https://api.anthropic.com' and expiry 2026-11-07T14:05:00Z.` Empty when the token
    /// carries none of these claims.
    #[must_use]
    pub fn sentence(&self, origin: &str) -> String {
        let mut parts = Vec::new();
        if let Some(issuer) = &self.issuer {
            parts.push(format!("issuer '{issuer}'"));
        }
        if let Some(subject) = &self.subject {
            parts.push(format!("subject '{subject}'"));
        }
        match self.audience.as_slice() {
            [] => {}
            [audience] => parts.push(format!("audience '{audience}'")),
            audiences => parts.push(format!(
                "audiences {}",
                audiences
                    .iter()
                    .map(|audience| format!("'{audience}'"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
        }
        if let Some(expires_at) = self.expires_at {
            parts.push(format!(
                "expiry {}",
                expires_at.to_rfc3339_opts(SecondsFormat::Secs, true)
            ));
        }
        let list = match parts.as_slice() {
            [] => return String::new(),
            [only] => only.clone(),
            [rest @ .., last] => format!("{} and {last}", rest.join(", ")),
        };
        format!(" The identity token from {origin} has {list}.")
    }
}

/// A claim's value as a message quotes it: on one line and bounded.
fn claim_value(text: &str) -> String {
    let text = util::single_line(text);
    match text.char_indices().nth(MAX_REASON_LEN) {
        Some((end, _)) => format!("{}…", &text[..end]),
        None => text.into_owned(),
    }
}

/// The HTTP client exchanges use. It follows no redirects: an exchange never needs one, and
/// following a 307 or 308 would send the identity token in the request body to wherever it
/// points.
fn exchange_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent(util::spiceai_user_agent())
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context(HttpClientSnafu)
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
/// `error` with its `error_description`, or the body itself. Microsoft Entra ID ends every
/// description with request identifiers (`Trace ID: … Correlation ID: … Timestamp: …`), which
/// are left out so the bounded reason keeps the part that names the cause.
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
            Some(description) => {
                let description = description
                    .split_once(" Trace ID:")
                    .map_or(description, |(cause, _)| cause);
                truncate(&format!("{code}: {description}"))
            }
            None => truncate(code),
        },
        _ => truncate(body),
    }
}

/// Exchanges an identity token from `source`, returning the access token and the digest of the
/// identity token it was exchanged for.
async fn exchange(
    http: &reqwest::Client,
    source: &SubjectTokenSource,
    exchange: &Exchange,
) -> Result<(AccessToken, [u8; 32])> {
    let subject_token = source.fetch(http).await?;
    let digest = token_digest(subject_token.expose_secret());
    let provider = exchange.provider();
    let response = exchange
        .request(http, &subject_token)
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
            claims: IdentityTokenClaims::read(subject_token.expose_secret())
                .map(|claims| claims.sentence(&source.describe()))
                .unwrap_or_default(),
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
    Ok((
        AccessToken {
            token: token.access_token,
            // Every provider reports the lifetime. A response without one is treated as the
            // shortest lifetime any of them issues, so the token is exchanged again in good time.
            expires_in: Duration::from_secs(token.expires_in.unwrap_or(300)),
        },
        digest,
    ))
}

/// A [`TokenProvider`] backed by workload identity federation. Construction performs the first
/// exchange, so a misconfigured identity fails there; a background task then exchanges again
/// before each token expires, and stops when the provider is dropped.
pub struct WorkloadIdentityTokenProvider {
    component: Arc<str>,
    source: Arc<SubjectTokenSource>,
    exchange: Arc<Exchange>,
    rx: watch::Receiver<SecretString>,
}

/// The warning logged when a refresh of `component`'s access token fails. It names the
/// component, because several models can authenticate with the same provider, and says until
/// when requests still work (`expires_at`, compared with `now`). `unchanged_file` names a token
/// file that still holds the token last exchanged, which is exchanged again as soon as the
/// platform rotates it.
#[must_use]
pub fn refresh_failure_message(
    component: &str,
    expires_at: DateTime<Utc>,
    now: DateTime<Utc>,
    retry_in: Duration,
    error: &dyn fmt::Display,
    unchanged_file: Option<&Path>,
) -> String {
    let expiry = expires_at.to_rfc3339_opts(SecondsFormat::Secs, true);
    let impact = if now < expires_at {
        format!("so requests keep the current one until it expires at {expiry}")
    } else {
        format!(
            "and the current one expired at {expiry}, so requests fail until a refresh succeeds"
        )
    };
    let retry = match unchanged_file {
        Some(path) => format!(
            "Retrying in {}s, or as soon as the platform rotates the identity token in '{}', which has not changed since the last exchange",
            retry_in.as_secs(),
            path.display()
        ),
        None => format!("Retrying in {}s", retry_in.as_secs()),
    };
    format!("Failed to refresh the access token of {component}, {impact}. {retry}. Cause: {error}")
}

/// When a token that expires `expires_in` from now expires.
fn expiry_after(expires_in: Duration) -> DateTime<Utc> {
    Utc::now() + chrono::Duration::from_std(expires_in).unwrap_or(chrono::Duration::days(365))
}

impl fmt::Debug for WorkloadIdentityTokenProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkloadIdentityTokenProvider")
            .field("component", &self.component)
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
            SubjectTokenSource::Requested(source) => {
                "requested".hash(&mut hasher);
                source.hash_key().hash(&mut hasher);
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
    /// exchanging before each access token expires. `component` names what the token
    /// authenticates, as the logs show it, such as `model 'gpt' (openai)`.
    ///
    /// # Errors
    ///
    /// When the first identity token cannot be read, or the provider refuses or cannot be
    /// reached for the first exchange.
    pub async fn try_new(
        component: impl Into<Arc<str>>,
        source: SubjectTokenSource,
        exchange: Exchange,
    ) -> Result<Self> {
        let component: Arc<str> = component.into();
        let http = exchange_client()?;
        let source = Arc::new(source);
        let exchange = Arc::new(exchange);

        let (first, first_digest) = self::exchange(&http, &source, &exchange).await?;
        tracing::debug!(
            "Exchanged a workload identity token from {} for the {} access token of {component}; it expires in {}s",
            source.describe(),
            exchange.provider(),
            first.expires_in.as_secs()
        );
        let first_expires_in = first.expires_in;
        let (tx, rx) = watch::channel(first.token);

        let task_component = Arc::clone(&component);
        let task_source = Arc::clone(&source);
        let task_exchange = Arc::clone(&exchange);
        tokio::spawn(async move {
            let mut backoff = FibonacciBackoffBuilder::new()
                .max_duration(Some(MAX_RETRY_WAIT))
                .build();
            let mut life = first_expires_in;
            let mut expires_at = expiry_after(first_expires_in);
            let mut exchanged = first_digest;
            let mut next_wait = next_refresh(&task_source, first_expires_in);
            loop {
                // A dropped provider closes the channel: stop exchanging tokens nothing reads.
                let rotated = match task_source.as_ref() {
                    SubjectTokenSource::File(path) => {
                        match wait_for_rotation(path, &exchanged, next_wait, file_poll(life), &tx)
                            .await
                        {
                            Rotation::Rotated => true,
                            Rotation::Unchanged => false,
                            Rotation::Closed => break,
                        }
                    }
                    SubjectTokenSource::GitHubActions { .. } | SubjectTokenSource::Requested(_) => {
                        tokio::select! {
                            () = sleep(next_wait) => {}
                            () = tx.closed() => break,
                        }
                        true
                    }
                };
                match self::exchange(&http, &task_source, &task_exchange).await {
                    Ok((token, digest)) => {
                        backoff.reset();
                        tracing::debug!(
                            "Refreshed the {} access token of {task_component}; it expires in {}s",
                            task_exchange.provider(),
                            token.expires_in.as_secs()
                        );
                        life = token.expires_in;
                        expires_at = expiry_after(token.expires_in);
                        exchanged = digest;
                        next_wait = next_refresh(&task_source, token.expires_in);
                        if tx.send(token.token).is_err() {
                            break;
                        }
                    }
                    Err(error) => {
                        next_wait = backoff.next_duration().unwrap_or(MAX_RETRY_WAIT);
                        let unchanged_file = match task_source.as_ref() {
                            SubjectTokenSource::File(path) if !rotated => Some(path.as_path()),
                            _ => None,
                        };
                        tracing::warn!(
                            "{}",
                            refresh_failure_message(
                                &task_component,
                                expires_at,
                                Utc::now(),
                                next_wait,
                                &error,
                                unchanged_file,
                            )
                        );
                    }
                }
            }
        });

        Ok(Self {
            component,
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
        Form, Json, Router,
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
        issue(&mock, body)
    }

    /// A token endpoint that reads a form rather than JSON, as Microsoft Entra ID's and
    /// Databricks' do.
    async fn form_token(
        State(mock): State<Arc<Mock>>,
        Form(form): Form<HashMap<String, String>>,
    ) -> Response {
        let body = form
            .into_iter()
            .map(|(name, value)| (name, Value::String(value)))
            .collect();
        issue(&mock, Value::Object(body))
    }

    /// Records an exchange request and answers it: with an access token, or with the refusal
    /// `refuse_exchange` asks for.
    fn issue(mock: &Mock, body: Value) -> Response {
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

    async fn refuse_entra() -> Response {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "invalid_client",
                "error_description": "AADSTS700213: No matching federated identity record found for presented assertion subject 'repo:org/repo:environment:prod'. Check your federated identity credential Subject, Audience and Issuer against the presented assertion. Trace ID: 0b1c Correlation ID: 5e6f Timestamp: 2026-11-03 10:00:00Z",
                "error_codes": [700_213],
            })),
        )
            .into_response()
    }

    async fn refuse_databricks() -> Response {
        (
            StatusCode::UNAUTHORIZED,
            Json(json!({
                "error": "invalid_client",
                "error_description": "Client authentication failed",
            })),
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
            .route("/entra/token", post(form_token))
            .route("/refuse/entra", post(refuse_entra))
            .route("/databricks/token", post(form_token))
            .route("/refuse/databricks", post(refuse_databricks))
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

    fn entra_at(token_url: String) -> Exchange {
        Exchange::Entra {
            token_url,
            client_id: "client-1".to_string(),
            scope: "https://cognitiveservices.azure.com/.default".to_string(),
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
            "model 'test' (openai)",
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
            "model 'test' (openai)",
            github_actions(&base, ANTHROPIC_AUDIENCE),
            anthropic_at(format!("{base}/token"), Some("wrkspc_1")),
        )
        .await
        .expect("an exchange naming the workspace");
        let without_workspace = WorkloadIdentityTokenProvider::try_new(
            "model 'test' (openai)",
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
    async fn entra_receives_the_client_credentials_grant_with_a_new_identity_token_as_its_assertion()
     {
        let (base, mock) = start(4).await;
        let provider = WorkloadIdentityTokenProvider::try_new(
            "model 'gpt' (azure)",
            github_actions(&base, ENTRA_AUDIENCE),
            entra_at(format!("{base}/entra/token")),
        )
        .await
        .expect("the first exchange");
        assert_eq!(provider.get_token(), "tok-1");
        wait_for_token(&provider, "tok-2").await;

        let grant = |assertion: &str| {
            json!({
                "grant_type": "client_credentials",
                "client_id": "client-1",
                "scope": "https://cognitiveservices.azure.com/.default",
                "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
                "client_assertion": assertion,
            })
        };
        assert_eq!(
            mock.exchanges.lock().expect("lock").clone(),
            vec![grant("jwt-1"), grant("jwt-2")],
            "each exchange is a form carrying a new identity token"
        );
        let audiences: Vec<Option<String>> = mock
            .id_token_requests
            .lock()
            .expect("lock")
            .iter()
            .map(|(query, _)| query.get("audience").cloned())
            .collect();
        assert_eq!(
            audiences,
            vec![
                Some("api://AzureADTokenExchange".to_string()),
                Some("api://AzureADTokenExchange".to_string())
            ]
        );
    }

    #[tokio::test]
    async fn databricks_receives_the_token_exchange_grant_with_a_new_identity_token_for_its_service_principal()
     {
        let (base, mock) = start(4).await;
        let provider = WorkloadIdentityTokenProvider::try_new(
            "Databricks service principal 'client-1'",
            github_actions(&base, "account-1"),
            Exchange::databricks(format!("{base}/databricks/token"), "client-1"),
        )
        .await
        .expect("the first exchange");
        assert_eq!(provider.get_token(), "tok-1");
        wait_for_token(&provider, "tok-2").await;

        let grant = |subject_token: &str| {
            json!({
                "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
                "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
                "subject_token": subject_token,
                "client_id": "client-1",
                "scope": "all-apis",
            })
        };
        assert_eq!(
            mock.exchanges.lock().expect("lock").clone(),
            vec![grant("jwt-1"), grant("jwt-2")],
            "each exchange is a form carrying a new identity token"
        );
    }

    #[test]
    fn an_entra_exchange_goes_to_the_tenant_of_the_authority() {
        let url = |authority: Option<&str>, tenant: &str| {
            Exchange::entra(authority, tenant, "client-1", "scope")
                .map(|exchange| exchange.token_url().to_string())
                .map_err(|error| error.to_string())
        };
        assert_eq!(
            [
                url(None, "72f988bf-86f1-41af-91ab-2d7cd011db47"),
                url(None, "contoso.onmicrosoft.com"),
                url(Some("https://login.microsoftonline.us/"), "contoso.onmicrosoft.us"),
                url(Some("login.chinacloudapi.cn"), "contoso"),
                url(Some(""), "contoso"),
            ],
            [
                Ok("https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/v2.0/token".to_string()),
                Ok("https://login.microsoftonline.com/contoso.onmicrosoft.com/oauth2/v2.0/token".to_string()),
                Ok("https://login.microsoftonline.us/contoso.onmicrosoft.us/oauth2/v2.0/token".to_string()),
                Ok("https://login.chinacloudapi.cn/contoso/oauth2/v2.0/token".to_string()),
                Ok("https://login.microsoftonline.com/contoso/oauth2/v2.0/token".to_string()),
            ]
        );
    }

    #[test]
    fn an_entra_authority_must_be_https() {
        for authority in [
            "http://login.microsoftonline.com",
            "HTTP://login.microsoftonline.us/",
            "ftp://login.example.test",
        ] {
            assert_eq!(
                Exchange::entra(Some(authority), "contoso", "client-1", "scope")
                    .map(|exchange| exchange.token_url().to_string())
                    .map_err(|error| error.to_string()),
                Err("The Microsoft Entra authority must be an https:// URL, such as https://login.microsoftonline.us, because the workload identity token is sent to it.".to_string()),
                "{authority:?}"
            );
        }
        assert_eq!(
            Exchange::entra(
                Some("HTTPS://login.microsoftonline.us"),
                "contoso",
                "c",
                "s"
            )
            .map(|exchange| exchange.token_url().to_string())
            .map_err(|error| error.to_string()),
            Ok("HTTPS://login.microsoftonline.us/contoso/oauth2/v2.0/token".to_string())
        );
    }

    #[test]
    fn an_entra_tenant_cannot_change_the_token_endpoint() {
        for tenant in [
            "",
            "contoso/oauth2",
            "../common",
            "contoso?x=1",
            "contoso#x",
            "contoso..com",
            "-contoso",
            "contoso.com.",
            "con toso",
            "contoso@evil.com",
        ] {
            assert_eq!(
                Exchange::entra(None, tenant, "client-1", "scope")
                    .map(|exchange| exchange.token_url().to_string())
                    .map_err(|error| error.to_string()),
                Err("The Microsoft Entra tenant must be a directory (tenant) ID, which is a GUID, or a domain name such as 'contoso.onmicrosoft.com'.".to_string()),
                "{tenant:?}"
            );
        }
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
            "model 'test' (openai)",
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
            "model 'test' (openai)",
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
            "model 'test' (openai)",
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
            "model 'test' (openai)",
            github_actions(&base, "https://api.openai.com"),
            openai_at(format!("{base}/refuse/openai")),
        )
        .await
        .expect_err("OpenAI refuses the exchange");
        assert_eq!(
            openai.to_string(),
            "OpenAI refused to exchange the workload identity token for an access token (HTTP 400 Bad Request): invalid_grant: No service account mapping matched the token. Verify the identity provider's issuer and audience, and that a service account mapping matches the token's claims: https://platform.openai.com/settings/organization/security/workload-identity-provider"
        );

        let entra = WorkloadIdentityTokenProvider::try_new(
            "model 'gpt' (azure)",
            github_actions(&base, ENTRA_AUDIENCE),
            entra_at(format!("{base}/refuse/entra")),
        )
        .await
        .expect_err("Microsoft Entra ID refuses the exchange");
        assert_eq!(
            entra.to_string(),
            "Microsoft Entra ID refused to exchange the workload identity token for an access token (HTTP 400 Bad Request): invalid_client: AADSTS700213: No matching federated identity record found for presented assertion subject 'repo:org/repo:environment:prod'. Check your federated identity credential Subject, Audience and Issuer against the presented assertion. Verify that the application or managed identity 'client-1' has a federated credential whose issuer, subject and audience match the workload identity token: https://learn.microsoft.com/entra/workload-id/workload-identity-federation"
        );

        let databricks = WorkloadIdentityTokenProvider::try_new(
            "Databricks service principal 'client-1'",
            github_actions(&base, "account-1"),
            Exchange::databricks(format!("{base}/refuse/databricks"), "client-1"),
        )
        .await
        .expect_err("Databricks refuses the exchange");
        assert_eq!(
            databricks.to_string(),
            "Databricks refused to exchange the workload identity token for an access token (HTTP 401 Unauthorized): invalid_client: Client authentication failed. Verify that the service principal 'client-1' has a federation policy whose issuer, subject and audience match the workload identity token: https://docs.databricks.com/aws/en/dev-tools/auth/oauth-federation-policy"
        );
    }

    #[tokio::test]
    async fn a_redirect_from_the_token_endpoint_is_not_followed() {
        let (base, mock) = start(3600).await;
        let error = WorkloadIdentityTokenProvider::try_new(
            "model 'test' (openai)",
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

    fn at(rfc3339: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(rfc3339)
            .expect("an RFC 3339 time")
            .with_timezone(&Utc)
    }

    #[test]
    fn a_refresh_failure_names_the_component_and_until_when_its_token_works() {
        let error = WorkloadIdentityError::EmptySubjectToken {
            origin: "GitHub Actions".to_string(),
        };
        let expires_at = at("2026-11-07T14:05:00Z");
        let before = at("2026-11-07T14:00:00Z");
        let after = at("2026-11-07T14:06:00Z");
        let two_seconds = Duration::from_secs(2);
        assert_eq!(
            [
                refresh_failure_message("model 'gpt-a' (openai)", expires_at, before, two_seconds, &error, None),
                refresh_failure_message("embedding 'embed' (openai)", expires_at, after, two_seconds, &error, None),
                refresh_failure_message(
                    "model 'claude' (anthropic)",
                    expires_at,
                    before,
                    two_seconds,
                    &error,
                    Some(Path::new("/var/run/secrets/tokens/anthropic")),
                ),
            ],
            [
                "Failed to refresh the access token of model 'gpt-a' (openai), so requests keep the current one until it expires at 2026-11-07T14:05:00Z. Retrying in 2s. Cause: The workload identity token from GitHub Actions is empty.".to_string(),
                "Failed to refresh the access token of embedding 'embed' (openai), and the current one expired at 2026-11-07T14:05:00Z, so requests fail until a refresh succeeds. Retrying in 2s. Cause: The workload identity token from GitHub Actions is empty.".to_string(),
                "Failed to refresh the access token of model 'claude' (anthropic), so requests keep the current one until it expires at 2026-11-07T14:05:00Z. Retrying in 2s, or as soon as the platform rotates the identity token in '/var/run/secrets/tokens/anthropic', which has not changed since the last exchange. Cause: The workload identity token from GitHub Actions is empty.".to_string(),
            ]
        );
    }

    /// A JWT whose payload is `claims`, with a header and signature no exchange here checks.
    fn jwt(claims: &Value) -> String {
        format!(
            "{}.{}.c2lnbmF0dXJl",
            URL_SAFE_NO_PAD.encode(br#"{"alg":"RS256"}"#),
            URL_SAFE_NO_PAD.encode(claims.to_string())
        )
    }

    #[test]
    fn an_identity_tokens_claims_are_named_for_the_user_to_compare_with_the_rule() {
        let claims = IdentityTokenClaims::read(&jwt(&json!({
            "iss": "https://oidc.eks.us-east-1.amazonaws.com/id/EXAMPLE",
            "sub": "system:serviceaccount:spice:spiced",
            "aud": "https://api.anthropic.com",
            "exp": 1_794_060_300,
            "kubernetes.io": {"namespace": "spice"},
        })))
        .expect("the claims of a JWT");
        assert_eq!(
            claims.sentence("the file '/var/run/secrets/tokens/anthropic'"),
            " The identity token from the file '/var/run/secrets/tokens/anthropic' has issuer 'https://oidc.eks.us-east-1.amazonaws.com/id/EXAMPLE', subject 'system:serviceaccount:spice:spiced', audience 'https://api.anthropic.com' and expiry 2026-11-07T14:05:00Z."
        );

        let audiences = IdentityTokenClaims::read(&jwt(&json!({"aud": ["a", "b"]})))
            .expect("the claims of a JWT");
        assert_eq!(
            audiences.sentence("GitHub Actions"),
            " The identity token from GitHub Actions has audiences 'a', 'b'."
        );
        let none = IdentityTokenClaims::read(&jwt(&json!({"name": "x"}))).expect("a JWT");
        assert_eq!(none.sentence("GitHub Actions"), "");

        for not_a_jwt in [
            "jwt-1",
            "a.b",
            "a.b.c.d",
            "a.!!!.c",
            &jwt(&json!(["array"])),
        ] {
            assert_eq!(IdentityTokenClaims::read(not_a_jwt), None, "{not_a_jwt}");
        }
    }

    #[tokio::test]
    async fn a_refused_exchange_names_the_identity_tokens_claims() {
        let (base, _mock) = start(3600).await;
        let dir = tempfile::tempdir().expect("a temporary directory");
        let path = dir.path().join("token");
        let token = jwt(&json!({
            "iss": "https://token.actions.githubusercontent.com",
            "sub": "repo:org/repo:environment:prod",
            "aud": "https://api.anthropic.com",
            "exp": 1_794_060_300,
        }));
        tokio::fs::write(&path, &token)
            .await
            .expect("write the token file");
        let error = WorkloadIdentityTokenProvider::try_new(
            "model 'claude' (anthropic)",
            SubjectTokenSource::File(path.clone()),
            anthropic_at(format!("{base}/refuse/anthropic"), None),
        )
        .await
        .expect_err("Anthropic refuses the exchange");
        assert_eq!(
            error.to_string(),
            format!(
                "Anthropic refused to exchange the workload identity token for an access token (HTTP 401 Unauthorized): Authentication failed. The identity token from the file '{}' has issuer 'https://token.actions.githubusercontent.com', subject 'repo:org/repo:environment:prod', audience 'https://api.anthropic.com' and expiry 2026-11-07T14:05:00Z. Anthropic records why it refused an exchange only in its authentication history: https://platform.claude.com/settings/workload-identity-federation?tab=history",
                path.display()
            )
        );
        assert!(
            !error.to_string().contains(&token),
            "a refusal must never quote the token itself"
        );
    }

    /// A requested identity token source that issues `requested-n` and counts its requests.
    struct Counting(AtomicUsize);

    #[async_trait]
    impl IdentityTokenSource for Counting {
        async fn fetch(&self) -> std::result::Result<SecretString, String> {
            let n = self.0.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(SecretString::from(format!("requested-{n}")))
        }

        fn describe(&self) -> String {
            "the test identity service".to_string()
        }

        fn hash_key(&self) -> String {
            "counting".to_string()
        }
    }

    /// A requested identity token source that cannot issue one.
    struct Unavailable;

    #[async_trait]
    impl IdentityTokenSource for Unavailable {
        async fn fetch(&self) -> std::result::Result<SecretString, String> {
            Err("no metadata server answered within 1s".to_string())
        }

        fn describe(&self) -> String {
            "the Google Cloud service account (metadata server)".to_string()
        }

        fn hash_key(&self) -> String {
            "unavailable".to_string()
        }
    }

    #[tokio::test]
    async fn a_requested_identity_token_is_new_for_every_exchange() {
        let (base, mock) = start(4).await;
        let source = Arc::new(Counting(AtomicUsize::new(0)));
        let provider = WorkloadIdentityTokenProvider::try_new(
            "model 'test' (openai)",
            SubjectTokenSource::Requested(Arc::clone(&source) as Arc<dyn IdentityTokenSource>),
            openai_at(format!("{base}/token")),
        )
        .await
        .expect("the first exchange");
        // Refreshed halfway through the token's four-second life.
        wait_for_token(&provider, "tok-2").await;
        let subject_tokens: Vec<Value> = mock
            .exchanges
            .lock()
            .expect("lock")
            .iter()
            .map(|body| body["subject_token"].clone())
            .collect();
        assert_eq!(
            subject_tokens[..2],
            [json!("requested-1"), json!("requested-2")]
        );
    }

    #[tokio::test]
    async fn a_requested_identity_token_that_is_unavailable_names_its_source() {
        let (base, _mock) = start(3600).await;
        let error = WorkloadIdentityTokenProvider::try_new(
            "model 'test' (anthropic)",
            SubjectTokenSource::Requested(Arc::new(Unavailable)),
            anthropic_at(format!("{base}/token"), None),
        )
        .await
        .expect_err("no identity token");
        assert_eq!(
            error.to_string(),
            "Failed to get a workload identity token from the Google Cloud service account (metadata server): no metadata server answered within 1s"
        );
    }

    #[tokio::test]
    async fn a_token_file_is_exchanged_again_when_it_changes_and_not_before() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let path = dir.path().join("token");
        tokio::fs::write(&path, "jwt-1\n")
            .await
            .expect("write the token file");
        let exchanged = token_digest("jwt-1");
        let (tx, _rx) = watch::channel(());

        assert_eq!(
            wait_for_rotation(
                &path,
                &exchanged,
                Duration::from_millis(300),
                MIN_FILE_POLL,
                &tx
            )
            .await,
            Rotation::Unchanged
        );
        let writer = {
            let path = path.clone();
            tokio::spawn(async move {
                sleep(Duration::from_millis(200)).await;
                tokio::fs::write(&path, "jwt-2")
                    .await
                    .expect("rotate the token file");
            })
        };
        assert_eq!(
            wait_for_rotation(
                &path,
                &exchanged,
                Duration::from_secs(10),
                MIN_FILE_POLL,
                &tx
            )
            .await,
            Rotation::Rotated
        );
        writer.await.expect("the writer");
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
    fn a_token_file_is_exchanged_at_the_latest_a_minute_early_and_a_requested_token_halfway() {
        let file = SubjectTokenSource::File(PathBuf::from("/token"));
        let requested = SubjectTokenSource::Requested(Arc::new(Unavailable));
        assert_eq!(
            [3600, 600, 299, 4, 0].map(|secs| next_refresh(&file, Duration::from_secs(secs))),
            [
                Duration::from_mins(59),
                Duration::from_mins(9),
                Duration::from_secs(239),
                Duration::from_secs(3),
                MIN_REFRESH_WAIT,
            ]
        );
        assert_eq!(
            [3600, 600, 299, 4, 0].map(|secs| next_refresh(&requested, Duration::from_secs(secs))),
            [
                Duration::from_mins(30),
                Duration::from_mins(5),
                Duration::from_millis(149_500),
                Duration::from_secs(2),
                MIN_REFRESH_WAIT,
            ]
        );
        assert_eq!(
            [3600, 40, 0].map(|secs| file_poll(Duration::from_secs(secs))),
            [MAX_FILE_POLL, Duration::from_secs(5), MIN_FILE_POLL]
        );
    }
}
