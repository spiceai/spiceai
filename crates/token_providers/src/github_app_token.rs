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

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{Result, TokenProvider};

use chrono::{DateTime, Utc};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use util::fibonacci_backoff::FibonacciBackoffBuilder;

#[derive(Debug, Snafu)]
pub enum GitHubAppError {
    #[snafu(display("Invalid private key. Verify the GitHub private key parameter."))]
    InvalidPrivateKey { source: jsonwebtoken::errors::Error },

    #[snafu(display("Failed to get system time.\nVerify your system time."))]
    UnableToGetSystemTime { source: std::time::SystemTimeError },

    #[snafu(display("Invalid system time.\nVerify your system time."))]
    InvalidSystemTime { source: std::num::TryFromIntError },

    #[snafu(display(
        "Failed to generate JWT\nVerify the GitHub Connector configuration and try again. For details, visit: https://spiceai.org/docs/components/data-connectors/github#common-configuration"
    ))]
    UnableToGenerateJWT { source: jsonwebtoken::errors::Error },

    #[snafu(display(
        "Failed to get GitHub installation access token\nVerify the GitHub Connector configuration and try again. For details, visit: https://spiceai.org/docs/components/data-connectors/github#common-configuration"
    ))]
    UnableToGetGitHubInstallationAccessToken { source: reqwest::Error },

    #[snafu(display(
        "Failed to get GitHub installation access token body.\nVerify the GitHub Connector configuration and try again. For details, visit: https://spiceai.org/docs/components/data-connectors/github#common-configuration"
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
    tx: watch::Sender<String>,
    rx: watch::Receiver<String>,
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
        self.rx.borrow().clone()
    }

    fn subscribe(&self) -> Option<watch::Receiver<String>> {
        Some(self.tx.subscribe())
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
            tx,
            rx,
            _handle: Arc::new(handle),
        })
    }
}

#[derive(Clone, Debug)]
pub struct GitHubToken {
    pub token: String,
    pub expires_at: DateTime<Utc>,
}
impl GitHubToken {
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

    #[derive(Deserialize, Debug)]
    struct TokenResponse {
        token: String,
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

// #[cfg(test)]
// mod tests {
//     use super::*;

//     struct MockTokenGenerator {
//         counter: Arc<RwLock<usize>>,
//     }

//     impl MockTokenGenerator {
//         fn new() -> Self {
//             Self {
//                 counter: Arc::new(RwLock::new(0)),
//             }
//         }
//     }

//     #[async_trait]
//     impl TokenGenerator for MockTokenGenerator {
//         async fn generate_token(
//             &self,
//             _app_client_id: Arc<str>,
//             _private_key: Arc<str>,
//             _installation_id: Arc<str>,
//         ) -> Result<TokenResponse, GitHubAppError> {
//             let mut counter = self.counter.write().await;
//             *counter += 1;
//             let token = format!("token_{}", *counter);

//             tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

//             Ok(TokenResponse {
//                 token,
//                 expires_at: (Utc::now() + chrono::Duration::seconds(2)).to_rfc3339(),
//             })
//         }
//     }

//     #[tokio::test]
//     async fn test_get_token_refresh() {
//         let app_client_id = Arc::from("app_client_id".to_string());
//         let private_key = Arc::from("private_key".to_string());
//         let installation_id = Arc::from("installation_id".to_string());
//         let token_generator = Arc::new(MockTokenGenerator::new());

//         let token_provider = GitHubAppTokenProvider {
//             token: Arc::new(RwLock::new(String::new())),
//             expires_at: Arc::new(RwLock::new(String::new())),
//             app_client_id,
//             private_key,
//             installation_id,
//             token_generator,
//         };

//         // First call to get_token should generate a new token
//         let token = token_provider
//             .get_token()
//             .await
//             .expect("Failed to get token");
//         assert_eq!(token, "token_1");

//         // Second call to get_token should return the same token
//         let token = token_provider
//             .get_token()
//             .await
//             .expect("Failed to get token");
//         assert_eq!(token, "token_1");

//         // sleep 3 seconds to expire the token
//         tokio::time::sleep(std::time::Duration::from_secs(3)).await;

//         // Third call to get_token should generate a new token
//         let token = token_provider
//             .get_token()
//             .await
//             .expect("Failed to get token");
//         assert_eq!(token, "token_2");
//     }
// }
