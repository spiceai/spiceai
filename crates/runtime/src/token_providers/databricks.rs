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
use std::path::PathBuf;
use std::time::Duration;
use std::{fmt, sync::Arc};
use token_provider::registry::TokenProviderRegistry;
use token_provider::workload_identity::{
    Exchange, SubjectTokenSource, WorkloadIdentityError, WorkloadIdentityTokenProvider,
};
use token_provider::{Result, TokenProvider};
use tokio::{sync::watch, task::JoinHandle, time::sleep};
use util::fibonacci_backoff::{Backoff as _, FibonacciBackoffBuilder};

use crate::request::DatabricksAuthExtension;
use runtime_request_context::RequestContext;

const TOKEN_REFRESH_BUFFER_SECS: u64 = 300;
const MIN_TOKEN_REFRESH_WAIT_SECS: u64 = 1;

const MODEL_DOCS: &str = "https://spiceai.org/docs/components/models/databricks";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to obtain Databricks service principal token for machine-to-machine authentication: {source}. Machine-to-machine is used whenever `databricks_client_secret` is set, including when it is auto-loaded from the secret stores or the environment (`DATABRICKS_CLIENT_SECRET`, `SPICE_DATABRICKS_CLIENT_SECRET`); set `databricks_auth_mode: u2m` for user-to-machine OAuth instead."
    ))]
    UnableToGetToken {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// A mistake in the parameters of workload identity federation: `parameter` is the one to
    /// change.
    #[snafu(display("{message}"))]
    InvalidWorkloadIdentity {
        parameter: &'static str,
        message: String,
    },

    #[snafu(display("{source}"))]
    WorkloadIdentityExchange { source: WorkloadIdentityError },

    /// No source answered with an identity token: no file, no GitHub Actions job, and no cloud
    /// identity that issues one. Not a mistake in the parameters: a cloud identity can answer
    /// later, as when a metadata server is not ready at startup.
    #[snafu(display("{message}"))]
    NoIdentityToken { message: String },
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
                .max_duration(Some(Duration::from_mins(5))) // Cap at 5 minutes
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
                            backoff.next_duration().unwrap_or(Duration::from_mins(5));
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
    let response = http_body_limit::buffer(response, http_body_limit::Limit::METADATA).await?;

    if !response.status().is_success() {
        let status = response.status();
        let error_text = response.text().await?;
        return Err(format!("Failed to get access token: HTTP {status}, {error_text}").into());
    }

    let token_response = response.json::<TokenResponse>().await?;

    tracing::debug!(
        "Got access token, expires in {} seconds",
        token_response.expires_in
    );

    Ok(token_response)
}

fn build_m2m_token_client() -> Result<reqwest::Client, reqwest::Error> {
    // Token responses are read through `http_body_limit`, which decodes them while
    // refusing decompression amplification.
    http_body_limit::decode_manually(configure_client_builder(reqwest::Client::builder()))
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
    WorkloadIdentity(WorkloadIdentity<'a>),
}

/// A service principal that authenticates with workload identity federation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkloadIdentity<'a> {
    pub client_id: &'a str,
    pub identity_token: IdentityToken<'a>,
}

/// Where the identity token of a [`WorkloadIdentity`] comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityToken<'a> {
    /// A file the platform keeps current.
    File(&'a str),
    /// The OIDC token of the GitHub Actions job, requested for `audience`, or for the workspace's
    /// token endpoint, which Databricks accepts by default, when it is `None`.
    GitHubActions { audience: Option<&'a str> },
    /// An identity token from the workload's cloud identity (AWS, Google Cloud or Azure),
    /// requested for `audience`.
    Cloud { audience: &'a str },
}

/// A Databricks parameter's value, and whether the Spicepod sets it, as opposed to an
/// environment variable or secret store supplying it unasked (`DATABRICKS_CLIENT_ID`, ...): a
/// value the Spicepod sets always wins.
#[derive(Debug, Clone, Copy, Default)]
pub struct Setting<'a> {
    pub value: Option<&'a str>,
    pub in_spicepod: bool,
}

impl<'a> Setting<'a> {
    /// A value the Spicepod sets.
    #[must_use]
    pub fn spicepod(value: Option<&'a str>) -> Self {
        Self {
            value,
            in_spicepod: true,
        }
    }

    fn get(self) -> Option<&'a str> {
        self.value.filter(|value| !value.trim().is_empty())
    }

    fn spicepod_value(self) -> Option<&'a str> {
        self.get().filter(|_| self.in_spicepod)
    }
}

/// Whether this process runs in a GitHub Actions job that can request an OIDC token: one with
/// `permissions: id-token: write`, for which GitHub sets `ACTIONS_ID_TOKEN_REQUEST_URL` and
/// `ACTIONS_ID_TOKEN_REQUEST_TOKEN`.
#[must_use]
pub fn in_github_actions() -> bool {
    SubjectTokenSource::github_actions_from_env("").is_ok()
}

/// The federation parameters of a Databricks model or embedding: its `databricks_client_id`,
/// `databricks_oidc_token_filepath` and `databricks_token_audience`, autoloaded when the Spicepod
/// does not set them, and `raw_params`, the ones the Spicepod sets.
#[must_use]
pub fn component_federation<'a, S: std::hash::BuildHasher>(
    raw_params: &std::collections::HashMap<String, SecretString, S>,
    [client_id, oidc_token_filepath, token_audience]: [Option<&'a SecretString>; 3],
    github_actions: bool,
) -> FederationParams<'a> {
    let setting = |name: &str, value: Option<&'a SecretString>| Setting {
        value: value.map(ExposeSecret::expose_secret),
        in_spicepod: raw_params.contains_key(name),
    };
    FederationParams {
        client_id: setting("databricks_client_id", client_id),
        oidc_token_filepath: setting("databricks_oidc_token_filepath", oidc_token_filepath),
        token_audience: setting("databricks_token_audience", token_audience),
        github_actions,
    }
}

/// The client ID a Databricks model or embedding uses outside workload identity federation: one
/// from the environment counts only beside a client secret, so an ambient `DATABRICKS_CLIENT_ID`
/// neither makes a token configuration ambiguous nor selects user-to-machine OAuth.
#[must_use]
pub fn component_client_id<'a, S: std::hash::BuildHasher>(
    raw_params: &std::collections::HashMap<String, SecretString, S>,
    client_id: Option<&'a SecretString>,
    client_secret: Option<&SecretString>,
) -> Option<&'a str> {
    client_id
        .filter(|_| raw_params.contains_key("databricks_client_id") || client_secret.is_some())
        .map(ExposeSecret::expose_secret)
        .filter(|client_id| !client_id.trim().is_empty())
}

/// The workload identity federation parameters of a Databricks component.
#[derive(Debug, Clone, Copy, Default)]
pub struct FederationParams<'a> {
    pub client_id: Setting<'a>,
    pub oidc_token_filepath: Setting<'a>,
    pub token_audience: Setting<'a>,
    /// Whether this process runs in a GitHub Actions job that can request an OIDC token.
    pub github_actions: bool,
}

impl<'a> FederationParams<'a> {
    /// The identity these parameters configure, or `None` when they configure none.
    ///
    /// - A token file or an audience selects federation. So does a client ID on its own, with no
    ///   token or client secret, in a GitHub Actions job, where user-to-machine OAuth cannot run.
    /// - `key_param` names a token or client secret the Spicepod also sets. Federation the
    ///   Spicepod sets cannot be combined with it, and federation from the environment yields to
    ///   it. `has_key` counts a token or secret from anywhere.
    /// - The identity token comes from the file, else the GitHub Actions job, else the
    ///   workload's cloud identity, for the audience.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidWorkloadIdentity`] for parameters that conflict or are missing.
    pub fn workload_identity(
        self,
        key_param: Option<&str>,
        has_key: bool,
    ) -> Result<Option<WorkloadIdentity<'a>>, Error> {
        let file = self.oidc_token_filepath.get();
        let audience = self.token_audience.get();
        let client_id = self.client_id.get();
        if file.is_none() && audience.is_none() {
            return Ok(match client_id {
                Some(client_id) if self.github_actions && !has_key => Some(WorkloadIdentity {
                    client_id,
                    identity_token: IdentityToken::GitHubActions { audience: None },
                }),
                _ => None,
            });
        }
        if self.oidc_token_filepath.spicepod_value().is_some()
            && self.token_audience.spicepod_value().is_some()
        {
            return InvalidWorkloadIdentitySnafu {
                parameter: "databricks_token_audience",
                message: "`databricks_token_audience` applies only to an identity token Spice requests, from GitHub Actions or the workload's cloud identity, but the identity token comes from `databricks_oidc_token_filepath`, whose audience is set where it is issued. Remove one of them",
            }
            .fail();
        }
        // A file and an audience, not both from the Spicepod: the one the Spicepod sets wins, and
        // between two from the environment, the file does.
        let use_file = match (file, audience) {
            (Some(_), Some(_)) => {
                self.oidc_token_filepath.in_spicepod || !self.token_audience.in_spicepod
            }
            (file, _) => file.is_some(),
        };
        let (federation_param, in_spicepod) = if use_file {
            (
                "databricks_oidc_token_filepath",
                self.oidc_token_filepath.in_spicepod,
            )
        } else {
            ("databricks_token_audience", self.token_audience.in_spicepod)
        };
        if let Some(key_param) = key_param {
            if !in_spicepod {
                return Ok(None);
            }
            return InvalidWorkloadIdentitySnafu {
                parameter: federation_param,
                message: format!(
                    "A Databricks component authenticates with either `{key_param}` or workload identity federation (`{federation_param}`), and both are set. Remove one of them"
                ),
            }
            .fail();
        }
        let Some(client_id) = client_id else {
            return InvalidWorkloadIdentitySnafu {
                parameter: "databricks_client_id",
                message: "Workload identity federation needs `databricks_client_id`, the application ID of the service principal, and `databricks_client_id` is not set",
            }
            .fail();
        };
        let identity_token = match (use_file, file, audience) {
            (true, Some(file), _) | (_, Some(file), None) => IdentityToken::File(file),
            (_, _, Some(audience)) if self.github_actions => IdentityToken::GitHubActions {
                audience: Some(audience),
            },
            (_, _, Some(audience)) => IdentityToken::Cloud { audience },
            (_, None, None) => return Ok(None),
        };
        Ok(Some(WorkloadIdentity {
            client_id,
            identity_token,
        }))
    }
}

/// The model loading error `error` is: a configuration error for a mistake in the parameters.
#[must_use]
pub fn model_error(error: Error) -> llms::chat::Error {
    match error {
        Error::InvalidWorkloadIdentity { parameter, message } => {
            llms::chat::Error::InvalidParamValueError {
                param: parameter.to_string(),
                message: format!("{message}. See: {MODEL_DOCS}"),
            }
        }
        Error::NoIdentityToken { message } => llms::chat::Error::FailedToAuthenticate {
            source: format!("{message}. See: {MODEL_DOCS}").into(),
        },
        other => llms::chat::Error::FailedToAuthenticate {
            source: Box::new(other),
        },
    }
}

/// Get or create the refreshing token provider of a service principal that authenticates with
/// workload identity federation. Components that name the same service principal and identity
/// source share one provider, so its logs name the workspace. `component` names what the token
/// authenticates in the load line, such as `model 'llm' (databricks)`.
///
/// # Errors
///
/// [`Error::InvalidWorkloadIdentity`] when there is no identity token to request or `endpoint`
/// cannot receive one, and [`Error::WorkloadIdentityExchange`] when the first exchange fails.
pub async fn get_workload_identity_token_provider(
    component: &str,
    endpoint: &str,
    identity: WorkloadIdentity<'_>,
    token_provider_registry: &Arc<TokenProviderRegistry>,
) -> Result<Arc<dyn TokenProvider>, Error> {
    let WorkloadIdentity {
        client_id,
        identity_token,
    } = identity;
    let token_url = databricks_token_endpoint_url(endpoint).map_err(|error| {
        Error::InvalidWorkloadIdentity {
            parameter: "databricks_endpoint",
            message: error.to_string(),
        }
    })?;

    let (source, source_key) = match identity_token {
        IdentityToken::File(path) => (
            SubjectTokenSource::File(PathBuf::from(path)),
            format!("file_{path}"),
        ),
        IdentityToken::GitHubActions { audience } => {
            // Databricks accepts the workspace's token endpoint as the audience by default.
            let audience = audience.unwrap_or(token_url.as_str());
            (
                SubjectTokenSource::github_actions_from_env(audience).map_err(|_| {
                    Error::InvalidWorkloadIdentity {
                        parameter: "databricks_oidc_token_filepath",
                        message: "Workload identity federation has no identity token: set `databricks_oidc_token_filepath` to the file the platform keeps it in, such as a Kubernetes projected service account token, or run in a GitHub Actions job with `permissions: id-token: write`".to_string(),
                    }
                })?,
                format!("audience_{audience}"),
            )
        }
        IdentityToken::Cloud { audience } => (
            llms::workload_identity::cloud_identity(component, audience)
                .await
                .map_err(|tried| Error::NoIdentityToken {
                    message: llms::workload_identity::no_identity_token_message(
                        "databricks_oidc_token_filepath",
                        &tried,
                    ),
                })?,
            format!("cloud_{audience}"),
        ),
    };
    let origin = source.describe();

    let provider = token_provider_registry
        .get_or_create_provider(
            format!("databricks_oidc_{endpoint}_{client_id}_{source_key}"),
            || {
                WorkloadIdentityTokenProvider::try_new(
                    format!("Databricks workspace '{endpoint}'"),
                    source,
                    Exchange::databricks(token_url, client_id),
                )
            },
        )
        .await
        .map_err(|source| Error::WorkloadIdentityExchange { source })?;
    tracing::info!(
        "Authenticated {component} as a Databricks service principal, with the identity token from {origin}."
    );
    Ok(provider)
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

/// The accepted values of the `databricks_auth_mode` parameter, in the order they are documented.
///
/// The connector and catalog connector parameter specs share this list so that every accepted value
/// is also a value [`build_auth_credentials`] dispatches on.
#[cfg(feature = "databricks")]
pub const AUTH_MODES: &[&str] = &["auto", "token", "m2m", "u2m"];

/// The description both parameter specs use for `databricks_auth_mode`.
#[cfg(feature = "databricks")]
pub const AUTH_MODE_DESCRIPTION: &str = "The authentication mode to use: 'token' for a personal access token, 'm2m' for machine-to-machine service principal credentials, 'u2m' for user-to-machine OAuth, or 'auto' (default) to select from the credentials that are set. Set this explicitly to keep a credential auto-loaded from the environment from selecting another mode.";

/// Which Databricks credential flow to use.
///
/// Credentials do not only come from the Spicepod: a `secret` parameter is auto-loaded from the
/// secret stores and the environment when the Spicepod omits it, so [`AuthMode::Auto`] can infer a
/// flow the Spicepod author did not ask for — an ambient `DATABRICKS_CLIENT_SECRET` on its own is
/// enough to select machine-to-machine. An explicit mode pins the flow and ignores the credentials
/// that flow does not use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg(feature = "databricks")]
pub enum AuthMode {
    /// Infer the flow from whichever credentials are set (default).
    Auto,
    /// Personal access token: `databricks_token`.
    Token,
    /// Machine-to-machine: `databricks_client_id` + `databricks_client_secret`.
    M2M,
    /// User-to-machine OAuth: `databricks_client_id`, authorized in the browser.
    U2M,
}

#[cfg(feature = "databricks")]
impl AuthMode {
    /// The mode as `databricks_auth_mode` spells it.
    fn name(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Token => "token",
            Self::M2M => "m2m",
            Self::U2M => "u2m",
        }
    }

    /// Parses the `databricks_auth_mode` parameter, ASCII-case-insensitively so that the `M2M` and
    /// `U2M` spellings used in the documentation are accepted alongside `m2m` and `u2m`.
    fn from_params(params: &Parameters) -> Result<Self, AuthConfigError> {
        let Some(value) = params.get("auth_mode").expose().ok() else {
            return Ok(Self::Auto);
        };

        match value.to_ascii_lowercase().as_str() {
            "auto" => Ok(Self::Auto),
            "token" => Ok(Self::Token),
            "m2m" => Ok(Self::M2M),
            "u2m" => Ok(Self::U2M),
            _ => Err(AuthConfigError::InvalidConfiguration {
                message: format!(
                    "Invalid `databricks_auth_mode` value: '{value}'. Use one of: {}.",
                    AUTH_MODES.join(", ")
                ),
            }),
        }
    }
}

#[cfg(feature = "databricks")]
fn missing_for_mode(mode: &str, parameter: &str) -> AuthConfigError {
    AuthConfigError::InvalidConfiguration {
        message: format!(
            "`databricks_auth_mode: {mode}` requires `{parameter}`. Set `{parameter}`, or remove `databricks_auth_mode` to select the authentication mode from the credentials that are set"
        ),
    }
}

/// Build auth credentials from parameters, for a process that may run in a GitHub Actions job.
#[cfg(feature = "databricks")]
pub fn build_auth_credentials(params: &Parameters) -> Result<AuthCredentials<'_>, AuthConfigError> {
    build_auth_credentials_in(params, in_github_actions())
}

/// Build auth credentials from parameters. `github_actions` says whether the process runs in a
/// GitHub Actions job that can request an OIDC token.
#[cfg(feature = "databricks")]
pub fn build_auth_credentials_in(
    params: &Parameters,
    github_actions: bool,
) -> Result<AuthCredentials<'_>, AuthConfigError> {
    let setting = |name: &str| Setting {
        value: params.get(name).expose().ok(),
        in_spicepod: params.get(name).ok().is_some() && !params.is_autoloaded(name),
    };
    let token = params.get("token").ok();
    let client_id_setting = setting("client_id");
    let client_secret = params.get("client_secret").ok();
    let mode = AuthMode::from_params(params)?;

    if mode == AuthMode::Auto {
        let key_param = [
            ("token", "databricks_token"),
            ("client_secret", "databricks_client_secret"),
        ]
        .into_iter()
        .find(|(name, _)| params.get(name).ok().is_some() && !params.is_autoloaded(name))
        .map(|(_, param)| param);
        let federation = FederationParams {
            client_id: client_id_setting,
            oidc_token_filepath: setting("oidc_token_filepath"),
            token_audience: setting("token_audience"),
            github_actions,
        };
        if let Some(identity) = federation
            .workload_identity(key_param, token.is_some() || client_secret.is_some())
            .map_err(|error| AuthConfigError::InvalidConfiguration {
                message: error.to_string(),
            })?
        {
            return Ok(AuthCredentials::WorkloadIdentity(identity));
        }
    } else if let Some(param) = [
        ("oidc_token_filepath", "databricks_oidc_token_filepath"),
        ("token_audience", "databricks_token_audience"),
    ]
    .into_iter()
    .find(|(name, _)| setting(name).spicepod_value().is_some())
    .map(|(_, param)| param)
    {
        return Err(AuthConfigError::InvalidConfiguration {
            message: format!(
                "`databricks_auth_mode: {}` cannot be combined with workload identity federation (`{param}`), which it would ignore. Remove `{param}`, or remove `databricks_auth_mode` to authenticate with workload identity federation",
                mode.name()
            ),
        });
    }

    // A client ID from the environment counts in `auto` only beside a client secret, so an
    // ambient `DATABRICKS_CLIENT_ID` neither makes a token configuration ambiguous nor selects
    // user-to-machine OAuth.
    let client_id = match mode {
        AuthMode::Auto if !client_id_setting.in_spicepod && client_secret.is_none() => None,
        _ => client_id_setting.get(),
    };

    match mode {
        AuthMode::Token => token
            .map(AuthCredentials::Token)
            .ok_or_else(|| missing_for_mode("token", "databricks_token")),
        AuthMode::M2M => match (client_id, client_secret) {
            (Some(client_id), Some(client_secret)) => {
                Ok(AuthCredentials::ServicePrincipal(client_id, client_secret))
            }
            (None, _) => Err(missing_for_mode("m2m", "databricks_client_id")),
            (Some(_), None) => Err(missing_for_mode("m2m", "databricks_client_secret")),
        },
        // U2M deliberately ignores `databricks_client_secret` and `databricks_token`: pinning the
        // mode is how a Spicepod opts out of an ambient service-principal secret or token
        // redirecting it to another flow.
        AuthMode::U2M => client_id
            .map(AuthCredentials::U2M)
            .ok_or_else(|| missing_for_mode("u2m", "databricks_client_id")),
        AuthMode::Auto => match (token, client_id, client_secret) {
            (Some(token), None, None) => Ok(AuthCredentials::Token(token)),
            (None, Some(client_id), None) => Ok(AuthCredentials::U2M(client_id)),
            (None, Some(client_id), Some(client_secret)) => {
                tracing::debug!(
                    "Databricks authentication: machine-to-machine, because `databricks_client_secret` is set. Set `databricks_auth_mode: u2m` for user-to-machine OAuth."
                );
                Ok(AuthCredentials::ServicePrincipal(client_id, client_secret))
            }
            (None, None, None) => Err(AuthConfigError::InvalidConfiguration {
                message: NO_CREDENTIAL_MESSAGE.to_string(),
            }),
            (None, None, Some(_)) => Err(AuthConfigError::MissingParameter {
                parameter: "databricks_client_id".to_string(),
            }),
            (Some(_), Some(_), _) | (Some(_), None, Some(_)) => {
                Err(AuthConfigError::InvalidConfiguration {
                    message: AMBIGUOUS_CREDENTIALS_MESSAGE.to_string(),
                })
            }
        },
    }
}

/// No credential is set.
///
/// Not gated on the `databricks` feature: the Databricks model and embedding loaders in
/// `model/chat.rs` and `model/embed.rs` use it too, and they build under `models` without
/// `databricks` (as `tools/spiceschema` does).
pub const NO_CREDENTIAL_MESSAGE: &str = "Missing credentials: set `databricks_token`, `databricks_client_id` with `databricks_client_secret`, or workload identity federation (`databricks_client_id` with `databricks_oidc_token_filepath`, or `databricks_client_id` alone in a GitHub Actions job with `permissions: id-token: write`)";

/// Both a token and service-principal credentials are set. Parameters absent from the Spicepod are
/// auto-loaded from the secret stores and the environment, so this is reachable without the
/// Spicepod setting both — say so, and point at the parameter that resolves it.
#[cfg(feature = "databricks")]
pub const AMBIGUOUS_CREDENTIALS_MESSAGE: &str = "Both `databricks_token` and service principal credentials (`databricks_client_id`/`databricks_client_secret`) are set, which select different authentication modes. Parameters left out of the Spicepod are auto-loaded from the secret stores and the environment (`DATABRICKS_TOKEN`, `DATABRICKS_CLIENT_SECRET`, `SPICE_DATABRICKS_*`), so one of them may not come from the Spicepod. Unset the credential you do not want, or set `databricks_auth_mode` to `token`, `m2m` or `u2m` to choose the mode explicitly";

/// Error type for auth configuration.
#[derive(Debug, Snafu)]
#[cfg(feature = "databricks")]
pub enum AuthConfigError {
    #[snafu(display("Missing required parameter: {parameter}"))]
    MissingParameter { parameter: String },

    #[snafu(display("Invalid configuration: {message}"))]
    InvalidConfiguration { message: String },
}

/// Get a token provider based on auth credentials. `component` names what the token authenticates
/// in the logs, such as `dataset 'orders' (databricks)`.
#[cfg(feature = "databricks")]
pub async fn get_token_provider(
    component: &str,
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
        AuthCredentials::WorkloadIdentity(identity) => {
            get_workload_identity_token_provider(
                component,
                endpoint,
                identity,
                &token_provider_registry,
            )
            .await?
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

#[cfg(all(test, feature = "databricks"))]
mod auth_mode_tests {
    use super::*;
    use crate::catalogconnector::databricks::PARAMETERS;

    /// `build_auth_credentials` outside a GitHub Actions job, whatever the job running the test.
    fn build(params: &Parameters) -> Result<AuthCredentials<'_>, AuthConfigError> {
        build_auth_credentials_in(params, false)
    }

    fn parameters(params: &[(&str, &str)]) -> Parameters {
        Parameters::new(
            params
                .iter()
                .map(|(key, value)| ((*key).to_string(), SecretString::from(*value)))
                .collect(),
            "databricks",
            PARAMETERS,
        )
    }

    /// Every value the parameter spec accepts must be a value `build_auth_credentials` dispatches
    /// on, or the runtime would accept a mode it then ignores.
    #[test]
    fn every_accepted_auth_mode_is_dispatched() {
        for mode in AUTH_MODES {
            let params = parameters(&[("auth_mode", mode)]);
            assert!(
                AuthMode::from_params(&params).is_ok(),
                "`{mode}` is in AUTH_MODES but is not parsed by AuthMode::from_params"
            );
        }

        let spec = PARAMETERS
            .iter()
            .find(|spec| spec.name == "auth_mode")
            .expect("the Databricks catalog connector declares an `auth_mode` parameter");
        assert_eq!(spec.one_of, Some(AUTH_MODES));
    }

    #[test]
    fn auth_mode_parses_documented_uppercase_spellings() {
        for (value, expected) in [
            ("U2M", AuthMode::U2M),
            ("M2M", AuthMode::M2M),
            ("Token", AuthMode::Token),
            ("AUTO", AuthMode::Auto),
        ] {
            let params = parameters(&[("auth_mode", value)]);
            assert_eq!(
                AuthMode::from_params(&params).expect("mode should parse"),
                expected,
                "`auth_mode: {value}` should parse as {expected:?}"
            );
        }
    }

    #[test]
    fn auth_mode_rejects_unknown_value() {
        let params = parameters(&[("auth_mode", "oauth")]);
        let error = AuthMode::from_params(&params).expect_err("unknown mode should be rejected");
        assert!(
            error.to_string().contains("auto, token, m2m, u2m"),
            "error should list the accepted modes, got: {error}"
        );
    }

    #[test]
    fn missing_auth_mode_defaults_to_auto() {
        let params = parameters(&[("client_id", "id")]);
        assert_eq!(
            AuthMode::from_params(&params).expect("absent mode should default"),
            AuthMode::Auto
        );
    }

    /// Regression test for #11508: a `databricks_client_secret` the Spicepod never set — it is
    /// auto-loaded from the environment — must not redirect a U2M dataset to machine-to-machine.
    #[test]
    fn u2m_mode_ignores_autoloaded_client_secret() {
        let params = parameters(&[
            ("auth_mode", "u2m"),
            ("client_id", "client-id"),
            ("client_secret", "ambient-secret"),
        ]);

        match build(&params).expect("u2m should be selected") {
            AuthCredentials::U2M(client_id) => assert_eq!(client_id, "client-id"),
            other => panic!("expected U2M, got {other:?}"),
        }
    }

    /// The same shape one step earlier: with an ambient token as well, `auto` cannot decide at all
    /// (and errors), while an explicit mode still resolves.
    #[test]
    fn u2m_mode_ignores_autoloaded_token() {
        let credentials = [("client_id", "client-id"), ("token", "ambient-token")];

        let auto = build(&parameters(&credentials))
            .expect_err("auto cannot choose between a token and a client id");
        assert!(
            auto.to_string().contains("databricks_auth_mode"),
            "the ambiguous-credentials error should point at `databricks_auth_mode`, got: {auto}"
        );

        let mut with_mode = credentials.to_vec();
        with_mode.push(("auth_mode", "u2m"));
        match build(&parameters(&with_mode)).expect("u2m should be selected") {
            AuthCredentials::U2M(client_id) => assert_eq!(client_id, "client-id"),
            other => panic!("expected U2M, got {other:?}"),
        }
    }

    #[test]
    fn token_mode_ignores_service_principal_credentials() {
        let params = parameters(&[
            ("auth_mode", "token"),
            ("token", "pat"),
            ("client_id", "client-id"),
            ("client_secret", "ambient-secret"),
        ]);

        match build(&params).expect("token should be selected") {
            AuthCredentials::Token(token) => assert_eq!(token.expose_secret(), "pat"),
            other => panic!("expected Token, got {other:?}"),
        }
    }

    #[test]
    fn m2m_mode_ignores_token() {
        let params = parameters(&[
            ("auth_mode", "m2m"),
            ("token", "ambient-token"),
            ("client_id", "client-id"),
            ("client_secret", "secret"),
        ]);

        match build(&params).expect("m2m should be selected") {
            AuthCredentials::ServicePrincipal(client_id, secret) => {
                assert_eq!(client_id, "client-id");
                assert_eq!(secret.expose_secret(), "secret");
            }
            other => panic!("expected ServicePrincipal, got {other:?}"),
        }
    }

    #[test]
    fn explicit_modes_name_the_credential_they_need() {
        for (mode, credentials, expected_parameter) in [
            ("token", vec![("client_id", "id")], "databricks_token"),
            ("m2m", vec![("token", "pat")], "databricks_client_id"),
            ("m2m", vec![("client_id", "id")], "databricks_client_secret"),
            (
                "u2m",
                vec![("client_secret", "secret")],
                "databricks_client_id",
            ),
        ] {
            let mut params = credentials;
            params.push(("auth_mode", mode));
            let error =
                build(&parameters(&params)).expect_err("the credential the mode needs is missing");
            let message = error.to_string();
            assert!(
                message.contains(expected_parameter) && message.contains(mode),
                "`auth_mode: {mode}` should name `{expected_parameter}`, got: {message}"
            );
        }
    }

    fn workload_identity(params: &[(&str, &str)]) -> Result<String, String> {
        match build(&parameters(params)) {
            Ok(AuthCredentials::WorkloadIdentity(identity)) => Ok(format!("{identity:?}")),
            Ok(other) => panic!("expected workload identity federation, got {other:?}"),
            Err(error) => Err(error.to_string()),
        }
    }

    #[test]
    fn a_federation_param_selects_federation() {
        let expected = |identity_token| {
            Ok(format!(
                "{:?}",
                WorkloadIdentity {
                    client_id: "id",
                    identity_token,
                }
            ))
        };
        assert_eq!(
            workload_identity(&[("client_id", "id"), ("oidc_token_filepath", "/token")]),
            expected(IdentityToken::File("/token"))
        );
        // Outside GitHub Actions, the audience is requested from the workload's cloud identity.
        assert_eq!(
            workload_identity(&[("client_id", "id"), ("token_audience", "account")]),
            expected(IdentityToken::Cloud {
                audience: "account"
            })
        );
        match build_auth_credentials_in(
            &parameters(&[("client_id", "id"), ("token_audience", "account")]),
            true,
        ) {
            Ok(AuthCredentials::WorkloadIdentity(identity)) => assert_eq!(
                identity.identity_token,
                IdentityToken::GitHubActions {
                    audience: Some("account")
                }
            ),
            other => panic!("expected workload identity federation, got {other:?}"),
        }
    }

    /// A store holding `databricks_token`, as the environment store holds an ambient token.
    struct AmbientToken;

    #[async_trait::async_trait]
    impl runtime_secrets::SecretStore for AmbientToken {
        async fn get_secret(
            &self,
            key: &str,
        ) -> std::result::Result<Option<SecretString>, Box<dyn std::error::Error + Send + Sync>>
        {
            Ok((key == "databricks_token").then(|| SecretString::from("ambient-token")))
        }
    }

    /// A token the Spicepod never set must not stop a component that names federation params.
    #[tokio::test]
    async fn federation_wins_over_a_token_autoloaded_from_a_secret_store() {
        let mut registry = runtime_secrets::Secrets::new();
        registry.register_store("env", Arc::new(AmbientToken));
        let params = Parameters::try_new(
            "test component",
            vec![
                (
                    "databricks_endpoint".to_string(),
                    SecretString::from("dbc-example.cloud.databricks.com"),
                ),
                ("databricks_client_id".to_string(), SecretString::from("id")),
                (
                    "databricks_token_audience".to_string(),
                    SecretString::from("account"),
                ),
            ],
            "databricks",
            Arc::new(tokio::sync::RwLock::new(registry)),
            PARAMETERS,
        )
        .await
        .expect("parameters should build");

        assert!(params.is_autoloaded("token"));
        match build_auth_credentials_in(&params, true).expect("federation should be selected") {
            AuthCredentials::WorkloadIdentity(identity) => assert_eq!(
                identity,
                WorkloadIdentity {
                    client_id: "id",
                    identity_token: IdentityToken::GitHubActions {
                        audience: Some("account")
                    },
                }
            ),
            other => panic!("expected workload identity federation, got {other:?}"),
        }
    }

    /// A store holding `databricks_*` values, as the environment store holds
    /// `DATABRICKS_CLIENT_ID` and the like.
    struct Ambient(&'static [(&'static str, &'static str)]);

    #[async_trait::async_trait]
    impl runtime_secrets::SecretStore for Ambient {
        async fn get_secret(
            &self,
            key: &str,
        ) -> std::result::Result<Option<SecretString>, Box<dyn std::error::Error + Send + Sync>>
        {
            Ok(self
                .0
                .iter()
                .find(|(name, _)| *name == key)
                .map(|(_, value)| SecretString::from(*value)))
        }
    }

    /// Parameters the Spicepod sets (`spicepod`, unprefixed), with `ambient` autoloaded.
    async fn with_ambient(
        spicepod: &[(&str, &str)],
        ambient: &'static [(&'static str, &'static str)],
    ) -> Parameters {
        let mut registry = runtime_secrets::Secrets::new();
        registry.register_store("env", Arc::new(Ambient(ambient)));
        match Parameters::try_new(
            "test component",
            spicepod
                .iter()
                .map(|(key, value)| (format!("databricks_{key}"), SecretString::from(*value)))
                .collect(),
            "databricks",
            Arc::new(tokio::sync::RwLock::new(registry)),
            PARAMETERS,
        )
        .await
        {
            Ok(params) => params,
            Err(error) => panic!("parameters should build: {error}"),
        }
    }

    #[tokio::test]
    async fn an_ambient_client_id_counts_only_for_federation_or_beside_a_secret() {
        let params = with_ambient(
            &[
                ("endpoint", "dbc-example.cloud.databricks.com"),
                ("token", "pat"),
            ],
            &[("databricks_client_id", "env-id")],
        )
        .await;
        assert!(params.is_autoloaded("client_id"));
        assert!(
            matches!(build(&params), Ok(AuthCredentials::Token(_))),
            "a token with `DATABRICKS_CLIENT_ID` in the environment is not ambiguous"
        );

        let params = with_ambient(
            &[("endpoint", "dbc-example.cloud.databricks.com")],
            &[("databricks_client_id", "env-id")],
        )
        .await;
        assert_eq!(
            build(&params)
                .map(|_| ())
                .map_err(|error| error.to_string()),
            Err(format!("Invalid configuration: {NO_CREDENTIAL_MESSAGE}")),
            "`DATABRICKS_CLIENT_ID` alone does not select user-to-machine OAuth"
        );

        let params = with_ambient(
            &[("endpoint", "dbc-example.cloud.databricks.com")],
            &[
                ("databricks_client_id", "env-id"),
                ("databricks_oidc_token_filepath", "/env-token"),
            ],
        )
        .await;
        match build(&params) {
            Ok(AuthCredentials::WorkloadIdentity(identity)) => assert_eq!(
                identity,
                WorkloadIdentity {
                    client_id: "env-id",
                    identity_token: IdentityToken::File("/env-token"),
                }
            ),
            other => panic!("expected workload identity federation, got {other:?}"),
        }
    }

    /// A client ID on its own stays user-to-machine outside GitHub Actions; in a job, where
    /// user-to-machine OAuth cannot run, it is federation with the workspace's default audience.
    #[test]
    fn a_client_id_alone_is_federation_only_in_github_actions() {
        match build(&parameters(&[("client_id", "id")])).expect("a client id alone selects U2M") {
            AuthCredentials::U2M(client_id) => assert_eq!(client_id, "id"),
            other => panic!("expected U2M, got {other:?}"),
        }
        match build_auth_credentials_in(&parameters(&[("client_id", "id")]), true) {
            Ok(AuthCredentials::WorkloadIdentity(identity)) => assert_eq!(
                identity,
                WorkloadIdentity {
                    client_id: "id",
                    identity_token: IdentityToken::GitHubActions { audience: None },
                }
            ),
            other => panic!("expected workload identity federation, got {other:?}"),
        }
        // A client secret beside it is machine-to-machine, in a job or not.
        match build_auth_credentials_in(
            &parameters(&[("client_id", "id"), ("client_secret", "secret")]),
            true,
        ) {
            Ok(AuthCredentials::ServicePrincipal(client_id, _)) => assert_eq!(client_id, "id"),
            other => panic!("expected M2M, got {other:?}"),
        }
    }

    #[test]
    fn an_explicit_mode_beside_a_federation_param_is_an_error() {
        assert_eq!(
            build(&parameters(&[
                ("auth_mode", "token"),
                ("token", "pat"),
                ("oidc_token_filepath", "/token"),
            ]))
            .map(|_| ())
            .map_err(|error| error.to_string()),
            Err("Invalid configuration: `databricks_auth_mode: token` cannot be combined with workload identity federation (`databricks_oidc_token_filepath`), which it would ignore. Remove `databricks_oidc_token_filepath`, or remove `databricks_auth_mode` to authenticate with workload identity federation".to_string())
        );
    }

    #[test]
    fn a_federation_mistake_names_the_param_to_fix() {
        assert_eq!(
            workload_identity(&[("oidc_token_filepath", "/token")]),
            Err("Invalid configuration: Workload identity federation needs `databricks_client_id`, the application ID of the service principal, and `databricks_client_id` is not set".to_string())
        );
        assert_eq!(
            workload_identity(&[
                ("client_id", "id"),
                ("oidc_token_filepath", "/token"),
                ("token", "pat"),
            ]),
            Err("Invalid configuration: A Databricks component authenticates with either `databricks_token` or workload identity federation (`databricks_oidc_token_filepath`), and both are set. Remove one of them".to_string())
        );
        assert_eq!(
            workload_identity(&[
                ("client_id", "id"),
                ("token_audience", "account"),
                ("client_secret", "secret"),
            ]),
            Err("Invalid configuration: A Databricks component authenticates with either `databricks_client_secret` or workload identity federation (`databricks_token_audience`), and both are set. Remove one of them".to_string())
        );
        assert_eq!(
            workload_identity(&[
                ("client_id", "id"),
                ("oidc_token_filepath", "/token"),
                ("token_audience", "account"),
            ]),
            Err("Invalid configuration: `databricks_token_audience` applies only to an identity token Spice requests, from GitHub Actions or the workload's cloud identity, but the identity token comes from `databricks_oidc_token_filepath`, whose audience is set where it is issued. Remove one of them".to_string())
        );
    }

    #[test]
    fn auto_mode_selection_is_unchanged() {
        match build(&parameters(&[("token", "pat")]))
            .expect("a token alone selects token authentication")
        {
            AuthCredentials::Token(token) => assert_eq!(token.expose_secret(), "pat"),
            other => panic!("expected Token, got {other:?}"),
        }

        match build(&parameters(&[("client_id", "id")])).expect("a client id alone selects U2M") {
            AuthCredentials::U2M(client_id) => assert_eq!(client_id, "id"),
            other => panic!("expected U2M, got {other:?}"),
        }

        match build(&parameters(&[
            ("client_id", "id"),
            ("client_secret", "secret"),
        ]))
        .expect("a client id and secret select M2M")
        {
            AuthCredentials::ServicePrincipal(client_id, secret) => {
                assert_eq!(client_id, "id");
                assert_eq!(secret.expose_secret(), "secret");
            }
            other => panic!("expected ServicePrincipal, got {other:?}"),
        }

        let missing = build(&parameters(&[])).expect_err("no credentials at all is an error");
        assert_eq!(
            missing.to_string(),
            format!("Invalid configuration: {NO_CREDENTIAL_MESSAGE}")
        );

        let no_client_id = build(&parameters(&[("client_secret", "secret")]))
            .expect_err("a client secret without a client id is an error");
        assert!(
            no_client_id.to_string().contains("databricks_client_id"),
            "got: {no_client_id}"
        );

        // A token beside a client secret is still ambiguous even though no client id makes M2M
        // possible, so the mode stays a deliberate choice rather than an inferred one.
        for ambiguous in [
            vec![("token", "pat"), ("client_id", "id")],
            vec![("token", "pat"), ("client_secret", "secret")],
            vec![
                ("token", "pat"),
                ("client_id", "id"),
                ("client_secret", "secret"),
            ],
        ] {
            let error = build(&parameters(&ambiguous))
                .expect_err("a token beside service principal credentials is ambiguous");
            assert!(
                error.to_string().contains("databricks_auth_mode"),
                "got: {error}"
            );
        }
    }
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
    fn federation_wins_over_a_key_only_the_spicepod_can_conflict_with() {
        let federation = FederationParams {
            client_id: Setting::spicepod(Some("id")),
            oidc_token_filepath: Setting::spicepod(Some("/token")),
            ..FederationParams::default()
        };
        assert_eq!(
            federation
                .workload_identity(None, true)
                .expect("no conflict"),
            Some(WorkloadIdentity {
                client_id: "id",
                identity_token: IdentityToken::File("/token"),
            })
        );
        let error = federation
            .workload_identity(Some("databricks_token"), true)
            .expect_err("a key the Spicepod sets conflicts");
        assert_eq!(
            model_error(error).to_string(),
            "Invalid value for parameter databricks_oidc_token_filepath. A Databricks component authenticates with either `databricks_token` or workload identity federation (`databricks_oidc_token_filepath`), and both are set. Remove one of them. See: https://spiceai.org/docs/components/models/databricks"
        );
    }

    #[test]
    fn no_identity_token_is_not_a_configuration_error() {
        // A cloud identity can answer later, so the model is loaded again.
        let error = model_error(Error::NoIdentityToken {
            message: "Workload identity federation has no identity token".to_string(),
        });
        assert_eq!(
            (error.is_configuration_error(), error.to_string()),
            (
                false,
                "Workload identity federation has no identity token. See: https://spiceai.org/docs/components/models/databricks".to_string()
            )
        );
    }

    #[test]
    fn no_federation_param_leaves_the_other_modes_to_decide() {
        assert_eq!(
            FederationParams {
                client_id: Setting::spicepod(Some("id")),
                oidc_token_filepath: Setting::spicepod(Some("")),
                ..FederationParams::default()
            }
            .workload_identity(Some("databricks_token"), true)
            .expect("no federation param is set"),
            None
        );
    }

    /// Federation settings from the environment (`DATABRICKS_OIDC_TOKEN_FILEPATH`, ...) yield to a
    /// key the Spicepod sets, and a value the Spicepod sets wins over one from the environment.
    #[test]
    fn a_spicepod_value_wins_over_one_from_the_environment() {
        let from_env = |value| Setting {
            value: Some(value),
            in_spicepod: false,
        };
        let env_file = FederationParams {
            client_id: from_env("env-id"),
            oidc_token_filepath: from_env("/env-token"),
            ..FederationParams::default()
        };
        assert_eq!(
            env_file
                .workload_identity(Some("databricks_token"), true)
                .expect("the Spicepod's token wins"),
            None
        );
        assert_eq!(
            env_file
                .workload_identity(None, true)
                .expect("federation from the environment")
                .map(|identity| identity.identity_token),
            Some(IdentityToken::File("/env-token"))
        );
        // An audience the Spicepod sets wins over a token file from the environment.
        let spicepod_audience = FederationParams {
            client_id: Setting::spicepod(Some("id")),
            oidc_token_filepath: from_env("/env-token"),
            token_audience: Setting::spicepod(Some("account")),
            github_actions: false,
        };
        assert_eq!(
            spicepod_audience
                .workload_identity(None, false)
                .expect("federation")
                .map(|identity| identity.identity_token),
            Some(IdentityToken::Cloud {
                audience: "account"
            })
        );
    }

    #[tokio::test]
    async fn components_naming_the_same_identity_share_one_token_provider() {
        let (endpoint, requests, captured_requests) = start_mock_server(vec![
            MockHttpResponse::json("200 OK", &token_response_body()),
            MockHttpResponse::json("200 OK", &token_response_body()),
        ])
        .await;
        let dir = tempfile::tempdir().expect("a temporary directory");
        let file = |name: &str| {
            let path = dir.path().join(name);
            std::fs::write(&path, format!("{name}-jwt\n")).expect("write the token file");
            path.to_string_lossy().into_owned()
        };
        let (first, second) = (file("first"), file("second"));
        let registry = Arc::new(TokenProviderRegistry::new());

        for path in [&first, &first, &second] {
            let provider = get_workload_identity_token_provider(
                "dataset 'orders' (databricks)",
                &endpoint,
                WorkloadIdentity {
                    client_id: "client-1",
                    identity_token: IdentityToken::File(path),
                },
                &registry,
            )
            .await
            .expect("the first exchange");
            assert_eq!(provider.get_token(), "test-access-token");
        }

        assert_eq!(
            requests.load(Ordering::SeqCst),
            2,
            "one exchange per identity source, not per component"
        );
        let request = captured_requests.lock().await.remove(0);
        assert!(
            request.starts_with("POST /oidc/v1/token "),
            "the exchange goes to the workspace's token endpoint: {request}"
        );
        assert!(
            request.contains("subject_token=first-jwt") && request.contains("client_id=client-1"),
            "{request}"
        );
    }

    #[tokio::test]
    async fn a_token_endpoint_that_is_not_https_receives_no_identity_token() {
        let error = get_workload_identity_token_provider(
            "dataset 'orders' (databricks)",
            "http://dbc.example.databricks.com",
            WorkloadIdentity {
                client_id: "client-1",
                identity_token: IdentityToken::File("/nonexistent"),
            },
            &Arc::new(TokenProviderRegistry::new()),
        )
        .await
        .expect_err("an insecure endpoint is refused");
        assert_eq!(
            error.to_string(),
            "Databricks token endpoint must use HTTPS for non-localhost hosts, got: http://dbc.example.databricks.com"
        );
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
