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
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
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

    tx: watch::Sender<String>,
    rx: watch::Receiver<String>,

    _handle: Arc<JoinHandle<()>>,
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
        let (tx, rx) = watch::channel(access_token.clone());

        // spawn background refresh loop
        let cloned_client_id = client_id.clone();
        let cloned_endpoint = endpoint.clone();
        let cloned_tx = tx.clone();

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
            tx,
            rx,
            _handle: Arc::new(handle),
        })
    }
}

impl TokenProvider for DatabricksM2MTokenProvider {
    fn get_token(&self) -> String {
        self.rx.borrow().clone()
    }

    fn subscribe(&self) -> Option<watch::Receiver<String>> {
        Some(self.tx.subscribe())
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct TokenResponse {
    access_token: String,
    token_type: String,
    expires_in: u64,
    scope: String,
}

async fn get_m2m_access_token(
    databricks_endpoint: String,
    client_id: String,
    client_secret: SecretString,
) -> Result<TokenResponse, Box<dyn std::error::Error + Send + Sync>> {
    let token_endpoint_url = format!("https://{databricks_endpoint}/oidc/v1/token");

    let client = reqwest::Client::new();

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
}

impl DatabricksU2MTokenProvider {
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(endpoint: String, client_id: String) -> Self {
        Self {
            endpoint,
            client_id,
        }
    }
}
