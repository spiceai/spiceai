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

use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use snafu::prelude::*;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::Duration;
use std::{fmt, sync::Arc};
use token_provider::{Result, TokenProvider};
use tokio::{sync::watch, task::JoinHandle, time::sleep};
use util::fibonacci_backoff::FibonacciBackoffBuilder;

use crate::request::DatabricksAuthExtension;
use runtime_request_context::RequestContext;

const TOKEN_REFRESH_BUFFER_SECS: u64 = 300;

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
        // initial fetch
        let TokenResponse {
            access_token,
            expires_in,
            ..
        } = get_m2m_access_token(endpoint.clone(), client_id.clone(), client_secret.clone())
            .await
            .map_err(|e| Error::UnableToGetToken { source: e })?;

        // create watch channel
        let (tx, rx) = watch::channel(access_token);

        // spawn background refresh loop
        let cloned_client_id = client_id.clone();
        let cloned_endpoint = endpoint.clone();
        let cloned_tx = tx;

        let secret = client_secret.clone();

        let handle = tokio::spawn(async move {
            // Databricks M2M access token lifespan is one hour. Schedule a refresh five minutes before expiration
            let mut next_wait = Duration::from_secs(expires_in - TOKEN_REFRESH_BUFFER_SECS);

            let mut backoff = FibonacciBackoffBuilder::new()
                .max_duration(Some(Duration::from_secs(300))) // Cap at 5 minutes
                .build();

            loop {
                sleep(next_wait).await;

                match get_m2m_access_token(
                    cloned_endpoint.clone(),
                    cloned_client_id.clone(),
                    secret.clone(),
                )
                .await
                {
                    Ok(TokenResponse {
                        access_token,
                        expires_in,
                        ..
                    }) => {
                        tracing::debug!("M2M token refreshed; expires in {}", expires_in);
                        let _ = cloned_tx.send(access_token.clone());
                        next_wait = Duration::from_secs(expires_in - TOKEN_REFRESH_BUFFER_SECS);
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
    databricks_endpoint: String,
    client_id: String,
    client_secret: SecretString,
) -> Result<TokenResponse, Box<dyn std::error::Error + Send + Sync>> {
    let token_endpoint_url = format!("https://{databricks_endpoint}/oidc/v1/token");

    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .build()?;

    let response = client
        .post(&token_endpoint_url)
        .basic_auth(client_id, Some(client_secret.expose_secret()))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .form(&[("grant_type", "client_credentials"), ("scope", "all-apis")])
        .send()
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

use crate::parameters::Parameters;
use token_provider::StaticTokenProvider;
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
        .get_or_create_provider(format!("databricks_m2m_{client_id}"), || async {
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
            format!("databricks_u2m_{client_id}"),
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
