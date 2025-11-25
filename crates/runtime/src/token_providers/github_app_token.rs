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

use std::{
    fmt,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use chrono::{DateTime, Utc};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use token_provider::{Result, TokenProvider};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use util::fibonacci_backoff::FibonacciBackoffBuilder;

#[derive(Debug, Snafu)]
pub enum GitHubAppError {
    #[snafu(display("Invalid private key. Verify the GitHub private key parameter."))]
    InvalidPrivateKey { source: jsonwebtoken::errors::Error },

    #[snafu(display("Failed to get system time. Verify your system time."))]
    UnableToGetSystemTime { source: std::time::SystemTimeError },

    #[snafu(display("Invalid system time. Verify your system time."))]
    InvalidSystemTime { source: std::num::TryFromIntError },

    #[snafu(display(
        "Failed to generate JWT Verify the GitHub Connector configuration and try again. For details, visit: https://spiceai.org/docs/components/data-connectors/github#common-configuration"
    ))]
    UnableToGenerateJWT { source: jsonwebtoken::errors::Error },

    #[snafu(display(
        "Failed to get GitHub installation access token Verify the GitHub Connector configuration and try again. For details, visit: https://spiceai.org/docs/components/data-connectors/github#common-configuration"
    ))]
    UnableToGetGitHubInstallationAccessToken { source: reqwest::Error },

    #[snafu(display(
        "Failed to get GitHub installation access token body. Verify the GitHub Connector configuration and try again. For details, visit: https://spiceai.org/docs/components/data-connectors/github#common-configuration"
    ))]
    UnableToGetGitHubInstallationAccessTokenBody { source: reqwest::Error },

    #[snafu(display("Unable to parse GitHub token expiration page"))]
    UnableToParseTokenExpiration {},
}

// A constant refresh buffer: refresh 60 seconds before expiration.
const TOKEN_REFRESH_BUFFER_SECS: u64 = 60;

pub struct GitHubAppTokenProvider {
    app_client_id: Arc<str>,
    private_key: Arc<str>,
    installation_id: Arc<str>,
    rx: watch::Receiver<SecretString>,
    _handle: Arc<JoinHandle<()>>,
}

impl std::fmt::Debug for GitHubAppTokenProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GitHubAppTokenProvider")
            .field("app_client_id", &self.app_client_id)
            .field("installation_id", &self.installation_id)
            .field("private_key.len()", &self.private_key.len())
            .finish_non_exhaustive()
    }
}

impl TokenProvider for GitHubAppTokenProvider {
    fn get_token(&self) -> String {
        self.rx.borrow().expose_secret().to_string()
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

impl GitHubAppTokenProvider {
    /// Creates a new `GitHubAppTokenProvider` and attempts to spawn a background token refresher.
    pub async fn try_new(
        app_client_id: Arc<str>,
        private_key: Arc<str>,
        installation_id: Arc<str>,
    ) -> Result<Self, GitHubAppError> {
        let init_token = generate_token(
            Arc::clone(&app_client_id),
            Arc::clone(&private_key),
            Arc::clone(&installation_id),
        )
        .await?;

        let (tx, rx) = watch::channel(init_token.token.clone());

        // variables for tokio thread.
        let cloned_app_client_id = Arc::clone(&app_client_id);
        let cloned_private_key = Arc::clone(&private_key);
        let cloned_installation_id = Arc::clone(&installation_id);
        let cloned_tx = tx.clone();

        let handle = tokio::spawn(async move {
            let mut backoff = FibonacciBackoffBuilder::new()
                .max_duration(Some(Duration::from_secs(300))) // Cap at 5 minutes
                .build();

            let mut next_wait = init_token.next_wait();

            loop {
                sleep(next_wait).await;

                match generate_token(
                    Arc::clone(&cloned_app_client_id),
                    Arc::clone(&cloned_private_key),
                    Arc::clone(&cloned_installation_id),
                )
                .await
                {
                    Ok(new_token) => {
                        tracing::debug!(
                            "GitHub token refreshed; expires at {}",
                            new_token.expires_at
                        );
                        next_wait = new_token.next_wait();
                        let _ = cloned_tx.send(new_token.token.clone());
                    }
                    Err(e) => {
                        next_wait = backoff.next_duration().unwrap_or(Duration::from_secs(300));
                        tracing::error!(
                            "GitHub token refresh failed: {}. Retrying in {:?}",
                            e,
                            next_wait
                        );
                    }
                }
            }
        });

        Ok(Self {
            app_client_id,
            private_key,
            installation_id,
            rx,
            _handle: Arc::new(handle),
        })
    }
}

#[derive(Clone)]
pub struct GitHubToken {
    pub token: SecretString,
    pub expires_at: DateTime<Utc>,
}

impl fmt::Debug for GitHubToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GitHubToken")
            .field("token", &"[REDACTED]")
            .field("expires_at", &self.expires_at)
            .finish()
    }
}
impl GitHubToken {
    #[allow(clippy::cast_sign_loss)]
    #[must_use]
    pub fn next_wait(&self) -> Duration {
        Duration::from_secs(
            ((self.expires_at - Utc::now()).num_seconds() as u64) - TOKEN_REFRESH_BUFFER_SECS,
        )
    }
}

#[derive(Serialize)]
struct Claims {
    iat: usize,
    exp: usize,
    iss: String,
}

async fn generate_token(
    app_client_id: Arc<str>,
    private_key: Arc<str>,
    installation_id: Arc<str>,
) -> Result<GitHubToken, GitHubAppError> {
    let iat = usize::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context(UnableToGetSystemTimeSnafu {})?
            .as_secs(),
    )
    .context(InvalidSystemTimeSnafu {})?;

    let exp = iat + 600;
    let claims = Claims {
        iat,
        exp,
        iss: app_client_id.to_string(),
    };
    let private_key = private_key.as_ref();
    let encoding_key =
        EncodingKey::from_rsa_pem(private_key.as_bytes()).context(InvalidPrivateKeySnafu {})?;

    let jwt_token = encode(&Header::new(Algorithm::RS256), &claims, &encoding_key)
        .context(UnableToGenerateJWTSnafu {})?;

    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .build()
        .context(UnableToGetGitHubInstallationAccessTokenSnafu {})?;

    let response = client
        .post(format!(
            "https://api.github.com/app/installations/{installation_id}/access_tokens",
        ))
        .header("Accept", "application/vnd.github+json")
        .header("Authorization", format!("Bearer {jwt_token}"))
        .header("X-GitHub-Api-Version", "2022-11-28")
        .header("User-Agent", "spice")
        .send()
        .await
        .context(UnableToGetGitHubInstallationAccessTokenSnafu {})?;

    #[allow(clippy::items_after_statements)]
    #[derive(Deserialize, Debug)]
    struct TokenResponse {
        token: SecretString,
        expires_at: String,
    }
    let resp: TokenResponse = response
        .json()
        .await
        .context(UnableToGetGitHubInstallationAccessTokenBodySnafu {})?;

    Ok(GitHubToken {
        token: resp.token,
        expires_at: DateTime::parse_from_rfc3339(&resp.expires_at)
            .map_err(|_| GitHubAppError::UnableToParseTokenExpiration {})?
            .with_timezone(&Utc),
    })
}
