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
        "Failed to obtain Databricks service principal token for machine-to-machine authentication: {source}. Machine-to-machine is used whenever `databricks_client_secret` is set, including when it is auto-loaded from the secret stores or the environment (`DATABRICKS_CLIENT_SECRET`, `SPICE_DATABRICKS_CLIENT_SECRET`); set `databricks_auth_mode: u2m` for user-to-machine OAuth instead."
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

/// Build auth credentials from parameters.
#[cfg(feature = "databricks")]
pub fn build_auth_credentials(params: &Parameters) -> Result<AuthCredentials<'_>, AuthConfigError> {
    let token = params.get("token").ok();
    let client_id = params.get("client_id").expose().ok();
    let client_secret = params.get("client_secret").ok();

    match AuthMode::from_params(params)? {
        AuthMode::Token => token.map(AuthCredentials::Token).ok_or_else(|| {
            missing_for_mode("token", "databricks_token")
        }),
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
                message: "Missing `databricks_token` or `databricks_client_id` and `databricks_client_secret` parameters".to_string(),
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

#[cfg(all(test, feature = "databricks"))]
mod auth_mode_tests {
    use super::*;
    use crate::catalogconnector::databricks::PARAMETERS;

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

        match build_auth_credentials(&params).expect("u2m should be selected") {
            AuthCredentials::U2M(client_id) => assert_eq!(client_id, "client-id"),
            other => panic!("expected U2M, got {other:?}"),
        }
    }

    /// The same shape one step earlier: with an ambient token as well, `auto` cannot decide at all
    /// (and errors), while an explicit mode still resolves.
    #[test]
    fn u2m_mode_ignores_autoloaded_token() {
        let credentials = [("client_id", "client-id"), ("token", "ambient-token")];

        let auto = build_auth_credentials(&parameters(&credentials))
            .expect_err("auto cannot choose between a token and a client id");
        assert!(
            auto.to_string().contains("databricks_auth_mode"),
            "the ambiguous-credentials error should point at `databricks_auth_mode`, got: {auto}"
        );

        let mut with_mode = credentials.to_vec();
        with_mode.push(("auth_mode", "u2m"));
        match build_auth_credentials(&parameters(&with_mode)).expect("u2m should be selected") {
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

        match build_auth_credentials(&params).expect("token should be selected") {
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

        match build_auth_credentials(&params).expect("m2m should be selected") {
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
            let error = build_auth_credentials(&parameters(&params))
                .expect_err("the credential the mode needs is missing");
            let message = error.to_string();
            assert!(
                message.contains(expected_parameter) && message.contains(mode),
                "`auth_mode: {mode}` should name `{expected_parameter}`, got: {message}"
            );
        }
    }

    #[test]
    fn auto_mode_selection_is_unchanged() {
        match build_auth_credentials(&parameters(&[("token", "pat")]))
            .expect("a token alone selects token authentication")
        {
            AuthCredentials::Token(token) => assert_eq!(token.expose_secret(), "pat"),
            other => panic!("expected Token, got {other:?}"),
        }

        match build_auth_credentials(&parameters(&[("client_id", "id")]))
            .expect("a client id alone selects U2M")
        {
            AuthCredentials::U2M(client_id) => assert_eq!(client_id, "id"),
            other => panic!("expected U2M, got {other:?}"),
        }

        match build_auth_credentials(&parameters(&[
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

        let missing = build_auth_credentials(&parameters(&[]))
            .expect_err("no credentials at all is an error");
        assert!(
            missing.to_string().contains(
                "Missing `databricks_token` or `databricks_client_id` and `databricks_client_secret` parameters"
            ),
            "got: {missing}"
        );

        let no_client_id = build_auth_credentials(&parameters(&[("client_secret", "secret")]))
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
            let error = build_auth_credentials(&parameters(&ambiguous))
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
