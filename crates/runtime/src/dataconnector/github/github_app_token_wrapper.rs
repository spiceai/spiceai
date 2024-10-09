use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use data_components::token_wrapper::{Error, Result, TokenWrapper};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde::Serialize;
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

pub struct GitHubAppTokenWrapper {
    token: Arc<RwLock<String>>,
    app_client_id: Arc<str>,
    private_key: Arc<str>,
    installation_id: Arc<str>,
}

impl GitHubAppTokenWrapper {
    #[must_use]
    pub fn new(app_client_id: Arc<str>, private_key: Arc<str>, installation_id: Arc<str>) -> Self {
        Self {
            token: Arc::new(RwLock::new(String::new())),
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

        let token = client
            .post(format!(
                "https://api.github.com/app/installations/{}/access_tokens",
                self.installation_id
            ))
            .header("Accept", "application/vnd.github+json")
            .header("Authorization", format!("Bearer {jwt_token}"))
            .header("X-GitHub-Api-Version", "2022-11-28")
            .send()
            .await
            .context(UnableToGetGitHubInstallationAccessTokenSnafu {})?
            .text()
            .await
            .context(UnableToGetGitHubInstallationAccessTokenBodySnafu {})?;

        Ok(token)
    }
}

#[async_trait]
impl TokenWrapper for GitHubAppTokenWrapper {
    fn is_refreshable(&self) -> bool {
        true
    }

    async fn get_token(&self) -> Result<String> {
        match self.token.read().await.clone() {
            token if !token.is_empty() => Ok(token),
            _ => {
                let token =
                    self.generate_token()
                        .await
                        .map_err(|e| Error::UnableToRefreshToken {
                            source: Box::new(e),
                        })?;

                let mut lock = self.token.write().await;
                lock.clone_from(&token);

                Ok(token)
            }
        }
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
