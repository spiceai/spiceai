use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use data_components::token_wrapper::{Error, Result, TokenWrapper};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use tokio::sync::RwLock;

#[derive(Debug, Snafu)]
pub enum GitHubAppError {
    #[snafu(display("Invalid private key"))]
    InvalidPrivateKey { source: jsonwebtoken::errors::Error },

    #[snafu(display("Unable to get system time"))]
    UnableToGetSystemTime { source: std::time::SystemTimeError },

    #[snafu(display("Invalid system time"))]
    InvalidSystemTime { source: std::num::TryFromIntError },

    #[snafu(display("Unable to generate JWT"))]
    UnableToGenerateJWT { source: jsonwebtoken::errors::Error },

    #[snafu(display("Unable to get GitHub installation access token"))]
    UnableToGetGitHubInstallationAccessToken { source: reqwest::Error },

    #[snafu(display("Unable to get GitHub installation access token body"))]
    UnableToGetGitHubInstallationAccessTokenBody { source: reqwest::Error },
}

#[derive(Serialize)]
struct Claims {
    iat: usize,
    exp: usize,
    iss: String,
}

#[derive(Deserialize, Debug)]
struct InstallationTokenResponse {
    token: String,
    expires_at: String,
}

pub struct GitHubAppTokenWrapper {
    token: Arc<RwLock<String>>,
    expires_at: Arc<RwLock<String>>,
    app_client_id: Arc<str>,
    private_key: Arc<str>,
    installation_id: Arc<str>,
}

impl GitHubAppTokenWrapper {
    #[must_use]
    pub fn new(app_client_id: Arc<str>, private_key: Arc<str>, installation_id: Arc<str>) -> Self {
        Self {
            token: Arc::new(RwLock::new(String::new())),
            expires_at: Arc::new(RwLock::new(String::new())),
            app_client_id,
            private_key,
            installation_id,
        }
    }

    async fn generate_token(&self) -> Result<String, GitHubAppError> {
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
            iss: self.app_client_id.to_string(),
        };
        let private_key = self.private_key.as_ref();
        let encoding_key =
            EncodingKey::from_rsa_pem(private_key.as_bytes()).context(InvalidPrivateKeySnafu {})?;

        let jwt_token = encode(&Header::new(Algorithm::RS256), &claims, &encoding_key)
            .context(UnableToGenerateJWTSnafu {})?;

        let client = reqwest::Client::new();

        let response = client
            .post(format!(
                "https://api.github.com/app/installations/{}/access_tokens",
                self.installation_id
            ))
            .header("Accept", "application/vnd.github+json")
            .header("Authorization", format!("Bearer {jwt_token}"))
            .header("X-GitHub-Api-Version", "2022-11-28")
            .header("User-Agent", "spice")
            .send()
            .await
            .context(UnableToGetGitHubInstallationAccessTokenSnafu {})?;

        let token_response: InstallationTokenResponse = response
            .json()
            .await
            .context(UnableToGetGitHubInstallationAccessTokenBodySnafu {})?;

        self.expires_at
            .write()
            .await
            .clone_from(&token_response.expires_at);

        Ok(token_response.token)
    }
}

#[async_trait]
impl TokenWrapper for GitHubAppTokenWrapper {
    fn is_refreshable(&self) -> bool {
        true
    }

    async fn get_token(&self) -> Result<String> {
        let token = {
            let read_guard = self.token.read().await;
            read_guard.clone()
        };

        let expires_at = {
            let read_guard = self.expires_at.read().await;
            DateTime::parse_from_rfc3339(read_guard.as_str())
                .ok()
                .map(|dt| dt.with_timezone(&Utc))
        };

        // If the token is not empty and not expired, return it
        if let Some(expires_at) = expires_at {
            if !token.is_empty() && Utc::now() < expires_at {
                return Ok(token);
            }
        }

        // Otherwise, refresh the token
        let new_token = self
            .generate_token()
            .await
            .map_err(|e| Error::UnableToRefreshToken {
                source: Box::new(e),
            })?;

        {
            let mut write_guard = self.token.write().await;
            write_guard.clone_from(&new_token);
        }

        Ok(new_token)
    }

    async fn refresh_token(&self) -> Result<()> {
        let token = self
            .generate_token()
            .await
            .map_err(|e| Error::UnableToRefreshToken {
                source: Box::new(e),
            })?;

        let mut lock = self.token.write().await;
        lock.clone_from(&token);

        Ok(())
    }
}
