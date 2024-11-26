use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use data_components::token_provider::{Error, Result, TokenProvider};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use tokio::sync::RwLock;

#[derive(Debug, Snafu)]
pub enum GitHubAppError {
    #[snafu(display("Invalid private key. Verify the GitHub private key parameter."))]
    InvalidPrivateKey { source: jsonwebtoken::errors::Error },

    #[snafu(display("Failed to get system time.\nVerify your system time."))]
    UnableToGetSystemTime { source: std::time::SystemTimeError },

    #[snafu(display("Invalid system time.\nVerify your system time."))]
    InvalidSystemTime { source: std::num::TryFromIntError },

    #[snafu(display(
        "Failed to generate JWT\nVerify the GitHub Connector configuration and try again. For details, visit: https://docs.spiceai.org/components/data-connectors/github#common-configuration"
    ))]
    UnableToGenerateJWT { source: jsonwebtoken::errors::Error },

    #[snafu(display(
        "Failed to get GitHub installation access token\nVerify the GitHub Connector configuration and try again. For details, visit: https://docs.spiceai.org/components/data-connectors/github#common-configuration"
    ))]
    UnableToGetGitHubInstallationAccessToken { source: reqwest::Error },

    #[snafu(display("Failed to get GitHub installation access token body.\nVerify the GitHub Connector configuration and try again. For details, visit: https://docs.spiceai.org/components/data-connectors/github#common-configuration"))]
    UnableToGetGitHubInstallationAccessTokenBody { source: reqwest::Error },
}

#[derive(Serialize)]
struct Claims {
    iat: usize,
    exp: usize,
    iss: String,
}

#[derive(Deserialize, Debug)]
struct TokenResponse {
    token: String,
    expires_at: String,
}

#[async_trait]
trait TokenGenerator: Send + Sync {
    async fn generate_token(
        &self,
        app_client_id: Arc<str>,
        private_key: Arc<str>,
        installation_id: Arc<str>,
    ) -> Result<TokenResponse, GitHubAppError>;
}

struct GitHubTokenGenerator {}

#[async_trait]
impl TokenGenerator for GitHubTokenGenerator {
    async fn generate_token(
        &self,
        app_client_id: Arc<str>,
        private_key: Arc<str>,
        installation_id: Arc<str>,
    ) -> Result<TokenResponse, GitHubAppError> {
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

        let client = reqwest::Client::new();

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

        let token_response: TokenResponse = response
            .json()
            .await
            .context(UnableToGetGitHubInstallationAccessTokenBodySnafu {})?;

        Ok(token_response)
    }
}

pub struct GitHubAppTokenProvider {
    token: Arc<RwLock<String>>,
    expires_at: Arc<RwLock<String>>,
    app_client_id: Arc<str>,
    private_key: Arc<str>,
    installation_id: Arc<str>,
    token_generator: Arc<dyn TokenGenerator>,
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

impl GitHubAppTokenProvider {
    #[must_use]
    pub fn new(app_client_id: Arc<str>, private_key: Arc<str>, installation_id: Arc<str>) -> Self {
        Self {
            token: Arc::new(RwLock::new(String::new())),
            expires_at: Arc::new(RwLock::new(String::new())),
            app_client_id,
            private_key,
            installation_id,
            token_generator: Arc::new(GitHubTokenGenerator {}),
        }
    }
}

#[async_trait]
impl TokenProvider for GitHubAppTokenProvider {
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

        let mut write_guard = self.token.write().await;

        // Otherwise, refresh the token
        let token_response = self
            .token_generator
            .generate_token(
                Arc::clone(&self.app_client_id),
                Arc::clone(&self.private_key),
                Arc::clone(&self.installation_id),
            )
            .await
            .map_err(|e| Error::UnableToGetToken {
                source: Box::new(e),
            })?;

        write_guard.clone_from(&token_response.token);

        self.expires_at
            .write()
            .await
            .clone_from(&token_response.expires_at);

        Ok(token_response.token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockTokenGenerator {
        counter: Arc<RwLock<usize>>,
    }

    impl MockTokenGenerator {
        fn new() -> Self {
            Self {
                counter: Arc::new(RwLock::new(0)),
            }
        }
    }

    #[async_trait]
    impl TokenGenerator for MockTokenGenerator {
        async fn generate_token(
            &self,
            _app_client_id: Arc<str>,
            _private_key: Arc<str>,
            _installation_id: Arc<str>,
        ) -> Result<TokenResponse, GitHubAppError> {
            let mut counter = self.counter.write().await;
            *counter += 1;
            let token = format!("token_{}", *counter);

            tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

            Ok(TokenResponse {
                token,
                expires_at: (Utc::now() + chrono::Duration::seconds(2)).to_rfc3339(),
            })
        }
    }

    #[tokio::test]
    async fn test_get_token_refresh() {
        let app_client_id = Arc::from("app_client_id".to_string());
        let private_key = Arc::from("private_key".to_string());
        let installation_id = Arc::from("installation_id".to_string());
        let token_generator = Arc::new(MockTokenGenerator::new());

        let token_provider = GitHubAppTokenProvider {
            token: Arc::new(RwLock::new(String::new())),
            expires_at: Arc::new(RwLock::new(String::new())),
            app_client_id,
            private_key,
            installation_id,
            token_generator,
        };

        // First call to get_token should generate a new token
        let token = token_provider
            .get_token()
            .await
            .expect("Failed to get token");
        assert_eq!(token, "token_1");

        // Second call to get_token should return the same token
        let token = token_provider
            .get_token()
            .await
            .expect("Failed to get token");
        assert_eq!(token, "token_1");

        // sleep 3 seconds to expire the token
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        // Third call to get_token should generate a new token
        let token = token_provider
            .get_token()
            .await
            .expect("Failed to get token");
        assert_eq!(token, "token_2");
    }

    /*
    0ms     1s      2s      3s      4s      5s      6s      7s      8s      9s      10s
    |-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|
    |======>| (get_token 0) = "token_1" (generated by first call)
    |->     | (get_token 1) = "token_1"
        |-> | (get_token 2) = "token_1"
            |->| (get_token 3) = "token_1"
            |   |->| (get_token 4) = "token_1"
            |       |->| (get_token 5) = "token_1"
            |           |->| (get_token 6) = "token_1"
            |               |-> |======>| (get_token 7) = "token_2" (new token generated)
            |               |   |->     | (get_token 8) = "token_2"
            |               |        |->| (get_token 9) = "token_2"
            |               |           |->|| (get_token 10) = "token_2"
            |               |           |
            |=== token_1 ===|           |=== token_2 ===|
    |-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|
    0ms     1s      2s      3s      4s      5s      6s      7s      8s      9s      10s

    Log:
    #0 (2ms) - get_token
    #1 (503ms) - get_token
    #2 (1004ms) - get_token
    #0 (1004ms) - token: token_1
    #1 (1004ms) - token: token_1
    #2 (1004ms) - token: token_1
    #3 (1503ms) - get_token
    #3 (1503ms) - token: token_1
    #4 (2003ms) - get_token
    #4 (2003ms) - token: token_1
    #5 (2503ms) - get_token
    #5 (2503ms) - token: token_1
    #6 (3003ms) - get_token
    #6 (3003ms) - token: token_1
    #7 (3503ms) - get_token
    #8 (4003ms) - get_token
    #9 (4503ms) - get_token
    #7 (4505ms) - token: token_2
    #8 (4505ms) - token: token_2
    #9 (4505ms) - token: token_2

     */
    #[tokio::test]
    async fn test_get_token_concurrent() -> Result<()> {
        let app_client_id = Arc::from("app_client_id".to_string());
        let private_key = Arc::from("private_key".to_string());
        let installation_id = Arc::from("installation_id".to_string());
        let token_generator = Arc::new(MockTokenGenerator::new());

        let token_provider = Arc::new(GitHubAppTokenProvider {
            token: Arc::new(RwLock::new(String::new())),
            expires_at: Arc::new(RwLock::new(String::new())),
            app_client_id,
            private_key,
            installation_id,
            token_generator,
        });

        let mut handles = vec![];

        // Spawn 10 concurrent tasks to get the token
        for i in 0..10 {
            let token_provider_clone = Arc::clone(&token_provider);
            let handle = tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(i * 500)).await;
                token_provider_clone
                    .get_token()
                    .await
                    .expect("Failed to get token")
            });
            handles.push(handle);
        }

        let mut tokens = vec![];
        for handle in handles {
            let token = handle.await.expect("Failed to get token");
            tokens.push(token);
        }

        for (i, token) in tokens.iter().enumerate().take(10) {
            if i < 7 {
                assert_eq!(token, "token_1", "{i}");
            } else {
                assert_eq!(token, "token_2", "{i}");
            }
        }

        Ok(())
    }
}
