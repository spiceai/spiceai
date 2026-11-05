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
#![allow(clippy::missing_errors_doc)]

//! GitHub App installation tokens: a JWT signed with the App's private key is exchanged for an
//! installation token, and exchanged again before that token expires.
//!
//! The private key is either held in memory ([`AppKey::Pem`]) or held by a key service that
//! signs with it and never reveals it ([`AppKey::Signer`]), such as Azure Key Vault or AWS KMS.

use std::{
    fmt,
    hash::DefaultHasher,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::jwt_signer::{JwtSigner, SignError};
use crate::{Result, TokenProvider, ensure_jwt_crypto_provider};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{DateTime, Utc};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use snafu::prelude::*;
use std::hash::Hash;
use std::hash::Hasher;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use util::fibonacci_backoff::{Backoff as _, FibonacciBackoffBuilder};

const GITHUB_APP_DOCS: &str =
    "https://spiceai.org/docs/components/data-connectors/github#github-app-installation";

/// Bounds the reason quoted from GitHub's error response.
const MAX_REASON_LEN: usize = 300;

/// How much of an error response is read for its reason.
const MAX_ERROR_BODY_BYTES: usize = 4096;

/// Where a GitHub App's private key is.
#[derive(Clone)]
pub enum AppKey {
    /// The PEM-encoded private key, in memory.
    Pem(Arc<str>),
    /// A key service that holds the private key and signs with it.
    Signer(Arc<dyn JwtSigner>),
}

impl fmt::Debug for AppKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pem(pem) => f
                .debug_tuple("Pem")
                .field(&format_args!("[REDACTED; {} bytes]", pem.len()))
                .finish(),
            Self::Signer(signer) => f.debug_tuple("Signer").field(&signer.key_id()).finish(),
        }
    }
}

#[derive(Debug, Snafu)]
pub enum GitHubAppError {
    #[snafu(display("Invalid private key. Verify the GitHub private key parameter."))]
    InvalidPrivateKey { source: jsonwebtoken::errors::Error },

    #[snafu(display("Failed to get system time. Verify your system time."))]
    UnableToGetSystemTime { source: std::time::SystemTimeError },

    #[snafu(display("Invalid system time. Verify your system time."))]
    InvalidSystemTime { source: std::num::TryFromIntError },

    #[snafu(display(
        "Failed to generate JWT. Verify the GitHub Connector configuration and try again. For details, visit: https://spiceai.org/docs/components/data-connectors/github#common-configuration"
    ))]
    UnableToGenerateJWT { source: jsonwebtoken::errors::Error },

    #[snafu(display("Failed to build the GitHub App JWT: {source}"))]
    UnableToSerializeJwt { source: serde_json::Error },

    #[snafu(display("Failed to sign the GitHub App JWT with the key '{key_id}': {source}"))]
    UnableToSignJwt { key_id: String, source: SignError },

    #[snafu(display(
        "Failed to get GitHub installation access token. Verify the GitHub Connector configuration and try again. For details, visit: https://spiceai.org/docs/components/data-connectors/github#common-configuration"
    ))]
    UnableToGetGitHubInstallationAccessToken { source: reqwest::Error },

    #[snafu(display(
        "GitHub refused the GitHub App JWT for installation '{installation_id}' (HTTP {status}): {reason}. Verify that `github_client_id` is the App's client ID, that the App is installed as `github_installation_id`, and that the private key is one of the App's. For details, visit: {GITHUB_APP_DOCS}"
    ))]
    GitHubRefusedAppJwt {
        installation_id: String,
        status: reqwest::StatusCode,
        reason: String,
    },

    #[snafu(display(
        "Failed to get GitHub installation access token body. Verify the GitHub Connector configuration and try again. For details, visit: https://spiceai.org/docs/components/data-connectors/github#common-configuration"
    ))]
    UnableToGetGitHubInstallationAccessTokenBody { source: reqwest::Error },

    #[snafu(display("Unable to parse GitHub token expiration timestamp"))]
    UnableToParseTokenExpiration {},
}

// A constant refresh buffer: refresh 60 seconds before expiration.
const TOKEN_REFRESH_BUFFER_SECS: u64 = 60;

/// A GitHub App installation, and where its tokens come from.
struct Installation {
    app_client_id: Arc<str>,
    key: AppKey,
    id: Arc<str>,
    /// The GitHub API the installation token is requested from: `https://api.github.com`, or a
    /// GitHub Enterprise Server's `https://<host>/api/v3`.
    api_base: Arc<str>,
}

pub struct GitHubAppTokenProvider {
    app_client_id: Arc<str>,
    key: AppKey,
    installation_id: Arc<str>,
    rx: watch::Receiver<SecretString>,
    _handle: Arc<JoinHandle<()>>,
}

impl Hash for GitHubAppTokenProvider {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Only hash non-sensitive identifiers; do not include the private key.
        self.app_client_id.hash(state);
        self.installation_id.hash(state);
    }
}

impl std::fmt::Debug for GitHubAppTokenProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GitHubAppTokenProvider")
            .field("app_client_id", &self.app_client_id)
            .field("installation_id", &self.installation_id)
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

impl TokenProvider for GitHubAppTokenProvider {
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

impl GitHubAppTokenProvider {
    /// Creates a new `GitHubAppTokenProvider` and attempts to spawn a background token refresher.
    /// `component` names what the token authenticates, as the logs show it, such as
    /// `dataset 'issues' (github)`. `api_base` is the GitHub API the installation token is
    /// requested from, such as `https://api.github.com`.
    pub async fn try_new(
        component: impl Into<Arc<str>>,
        app_client_id: Arc<str>,
        key: AppKey,
        installation_id: Arc<str>,
        api_base: Arc<str>,
    ) -> Result<Self, GitHubAppError> {
        let installation = Arc::new(Installation {
            app_client_id: Arc::clone(&app_client_id),
            key: key.clone(),
            id: Arc::clone(&installation_id),
            api_base,
        });
        let init_token = generate_token(&installation).await?;
        let component: Arc<str> = component.into();

        let (tx, rx) = watch::channel(init_token.token.clone());

        let handle = tokio::spawn(async move {
            let mut backoff = FibonacciBackoffBuilder::new()
                .max_duration(Some(Duration::from_mins(5))) // Cap at 5 minutes
                .build();

            let mut next_wait = init_token.next_wait();
            let mut expires_at = init_token.expires_at;

            loop {
                sleep(next_wait).await;

                match generate_token(&installation).await {
                    Ok(new_token) => {
                        tracing::debug!(
                            "GitHub token refreshed; expires at {}",
                            new_token.expires_at
                        );
                        next_wait = new_token.next_wait();
                        expires_at = new_token.expires_at;
                        backoff.reset();
                        let _ = tx.send(new_token.token.clone());
                    }
                    Err(e) => {
                        next_wait = backoff.next_duration().unwrap_or(Duration::from_mins(5));
                        tracing::warn!(
                            "{}",
                            refresh_failure_message(&component, expires_at, next_wait, &e)
                        );
                    }
                }
            }
        });

        Ok(Self {
            app_client_id,
            key,
            installation_id,
            rx,
            _handle: Arc::new(handle),
        })
    }
}

/// The warning logged when a refresh of `component`'s token fails: what still works, until when,
/// and why the refresh failed.
fn refresh_failure_message(
    component: &str,
    expires_at: DateTime<Utc>,
    retry_in: Duration,
    error: &GitHubAppError,
) -> String {
    format!(
        "Failed to refresh the GitHub App token of {component}, so requests use the current token until it expires at {} and then fail. Retrying in {}s. Cause: {error}",
        expires_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        retry_in.as_secs()
    )
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
    #[expect(clippy::cast_sign_loss)]
    #[must_use]
    pub fn next_wait(&self) -> Duration {
        // Clamp seconds-until-expiry to a non-negative value before casting to u64;
        // an already-expired (or near-expired) token yields a negative value that would
        // otherwise wrap to a huge sleep. Subtract the refresh buffer with saturating
        // subtraction so it floors at 0 rather than underflowing.
        let seconds_until_expiry = (self.expires_at - Utc::now()).num_seconds().max(0) as u64;
        Duration::from_secs(seconds_until_expiry.saturating_sub(TOKEN_REFRESH_BUFFER_SECS))
    }
}

#[derive(Serialize)]
struct Claims {
    iat: usize,
    exp: usize,
    iss: String,
}

/// The App JWT that authenticates as the App `app_client_id`: valid for ten minutes from now,
/// the longest GitHub accepts.
fn claims(app_client_id: &str) -> Result<Claims, GitHubAppError> {
    let iat = usize::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context(UnableToGetSystemTimeSnafu {})?
            .as_secs(),
    )
    .context(InvalidSystemTimeSnafu {})?;
    Ok(Claims {
        iat,
        exp: iat + 600,
        iss: app_client_id.to_string(),
    })
}

/// `claims` as an RS256 JWT signed with `key`. A signer receives the SHA-256 digest of the
/// JWT's signing input; the token it makes is byte for byte the one the PEM path makes, since
/// RSASSA-PKCS1-v1_5 signatures are deterministic.
async fn app_jwt(claims: &Claims, key: &AppKey) -> Result<String, GitHubAppError> {
    let header = Header::new(Algorithm::RS256);
    match key {
        AppKey::Pem(private_key) => {
            ensure_jwt_crypto_provider();
            let encoding_key = EncodingKey::from_rsa_pem(private_key.as_bytes())
                .context(InvalidPrivateKeySnafu {})?;
            encode(&header, claims, &encoding_key).context(UnableToGenerateJWTSnafu {})
        }
        AppKey::Signer(signer) => {
            let header = serde_json::to_vec(&header).context(UnableToSerializeJwtSnafu)?;
            let claims = serde_json::to_vec(claims).context(UnableToSerializeJwtSnafu)?;
            let signing_input = format!(
                "{}.{}",
                URL_SAFE_NO_PAD.encode(header),
                URL_SAFE_NO_PAD.encode(claims)
            );
            let digest: [u8; 32] = Sha256::digest(signing_input.as_bytes()).into();
            let signature =
                signer
                    .sign_sha256_digest(&digest)
                    .await
                    .context(UnableToSignJwtSnafu {
                        key_id: signer.key_id().to_string(),
                    })?;
            Ok(format!(
                "{signing_input}.{}",
                URL_SAFE_NO_PAD.encode(signature)
            ))
        }
    }
}

/// The reason in GitHub's error response: its `message`, or the body itself, on one line and
/// bounded.
fn refusal_reason(body: &[u8]) -> String {
    let body = String::from_utf8_lossy(body);
    let reason = serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|value| value["message"].as_str().map(str::to_string))
        .unwrap_or_else(|| body.to_string());
    let reason = util::single_line(&reason);
    let reason = reason.trim().trim_end_matches('.');
    match reason.char_indices().nth(MAX_REASON_LEN) {
        Some((end, _)) => format!("{}…", &reason[..end]),
        None => reason.to_string(),
    }
}

async fn generate_token(installation: &Installation) -> Result<GitHubToken, GitHubAppError> {
    let claims = claims(&installation.app_client_id)?;
    let jwt_token = app_jwt(&claims, &installation.key).await?;

    let client = reqwest::Client::builder()
        .user_agent(util::spiceai_user_agent())
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .build()
        .context(UnableToGetGitHubInstallationAccessTokenSnafu {})?;

    let response = client
        .post(format!(
            "{}/app/installations/{}/access_tokens",
            installation.api_base.trim_end_matches('/'),
            installation.id,
        ))
        .header("Accept", "application/vnd.github+json")
        .header("Authorization", format!("Bearer {jwt_token}"))
        .header("X-GitHub-Api-Version", "2022-11-28")
        .send()
        .await
        .context(UnableToGetGitHubInstallationAccessTokenSnafu {})?;
    let status = response.status();
    if !status.is_success() {
        let body = http_body_limit::read_prefix(response, MAX_ERROR_BODY_BYTES).await;
        return GitHubRefusedAppJwtSnafu {
            installation_id: installation.id.to_string(),
            status,
            reason: refusal_reason(&body),
        }
        .fail();
    }

    #[expect(clippy::items_after_statements)]
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

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;

    use aws_lc_rs::digest::{Digest as LcDigest, SHA256};
    use aws_lc_rs::encoding::AsDer;
    use aws_lc_rs::rsa::{KeyPair, KeySize};
    use aws_lc_rs::signature::{KeyPair as _, RSA_PKCS1_SHA256};
    use axum::{
        Json, Router,
        extract::{Path, State},
        http::{HeaderMap, StatusCode},
        response::{IntoResponse, Response},
        routing::post,
    };
    use base64::engine::general_purpose::STANDARD;
    use jsonwebtoken::{DecodingKey, Validation, decode};
    use serde_json::json;

    use super::*;

    /// An RSA key generated for one test, as GitHub hands an App's key out (PEM) and as a key
    /// service holds it.
    pub(crate) struct TestKey {
        pub key_pair: Arc<KeyPair>,
        pub pem: String,
        pub public_pem: String,
    }

    fn pem(label: &str, der: &[u8]) -> String {
        let body = STANDARD.encode(der);
        let lines: Vec<&str> = body
            .as_bytes()
            .chunks(64)
            .map(|line| std::str::from_utf8(line).expect("base64 is ASCII"))
            .collect();
        format!(
            "-----BEGIN {label}-----\n{}\n-----END {label}-----\n",
            lines.join("\n")
        )
    }

    pub(crate) fn test_key() -> TestKey {
        let key_pair = KeyPair::generate(KeySize::Rsa2048).expect("generate an RSA key");
        let private = key_pair.as_der().expect("PKCS#8 DER");
        let public = key_pair.public_key().as_der().expect("X.509 DER");
        TestKey {
            pem: pem("PRIVATE KEY", private.as_ref()),
            public_pem: pem("PUBLIC KEY", public.as_ref()),
            key_pair: Arc::new(key_pair),
        }
    }

    /// The RS256 signature of a message whose SHA-256 digest is `digest`, as a key service
    /// holding `key_pair` makes it.
    pub(crate) fn sign_digest(key_pair: &KeyPair, digest: &[u8]) -> Vec<u8> {
        let digest = LcDigest::import_less_safe(digest, &SHA256).expect("a SHA-256 digest");
        let mut signature = vec![0; key_pair.public_modulus_len()];
        key_pair
            .sign_digest(&RSA_PKCS1_SHA256, &digest, &mut signature)
            .expect("sign the digest");
        signature
    }

    /// A key service that holds `key_pair`, and records what it was asked to sign.
    #[derive(Debug)]
    struct LocalSigner {
        key_pair: Arc<KeyPair>,
        digests: Mutex<Vec<[u8; 32]>>,
    }

    #[async_trait]
    impl JwtSigner for LocalSigner {
        async fn sign_sha256_digest(
            &self,
            digest: &[u8; 32],
        ) -> std::result::Result<Vec<u8>, SignError> {
            self.digests.lock().expect("lock").push(*digest);
            Ok(sign_digest(&self.key_pair, digest))
        }

        fn key_id(&self) -> &'static str {
            "local-test-key"
        }
    }

    #[derive(Debug)]
    struct RefusingSigner;

    #[async_trait]
    impl JwtSigner for RefusingSigner {
        async fn sign_sha256_digest(
            &self,
            _digest: &[u8; 32],
        ) -> std::result::Result<Vec<u8>, SignError> {
            Err("AccessDeniedException: not authorized to perform kms:Sign".into())
        }

        fn key_id(&self) -> &'static str {
            "arn:aws:kms:us-east-1:111122223333:alias/github-app"
        }
    }

    #[derive(Deserialize)]
    struct VerifiedClaims {
        iss: String,
        iat: usize,
        exp: usize,
    }

    /// The claims of `jwt` if its RS256 signature verifies with `public_pem`.
    pub(crate) fn verify(jwt: &str, public_pem: &str, app_client_id: &str) -> (String, usize) {
        ensure_jwt_crypto_provider();
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&[app_client_id]);
        validation.set_required_spec_claims(&["exp", "iss"]);
        let claims = decode::<VerifiedClaims>(
            jwt,
            &DecodingKey::from_rsa_pem(public_pem.as_bytes()).expect("public key"),
            &validation,
        )
        .expect("the JWT's signature verifies with the App's public key")
        .claims;
        (claims.iss, claims.exp - claims.iat)
    }

    #[tokio::test]
    async fn a_key_service_signs_the_same_jwt_as_the_pem_key() {
        let key = test_key();
        let signer = Arc::new(LocalSigner {
            key_pair: Arc::clone(&key.key_pair),
            digests: Mutex::new(Vec::new()),
        });
        let claims = Claims {
            iat: 1_762_214_400,
            exp: 1_762_215_000,
            iss: "Iv23liTest".to_string(),
        };
        let from_pem = app_jwt(&claims, &AppKey::Pem(key.pem.as_str().into()))
            .await
            .expect("the PEM path signs");
        let from_signer = app_jwt(&claims, &AppKey::Signer(Arc::clone(&signer) as _))
            .await
            .expect("the signer path signs");
        assert_eq!(from_signer, from_pem);

        let (signing_input, _) = from_signer.rsplit_once('.').expect("a JWT has three parts");
        let expected: [u8; 32] = Sha256::digest(signing_input.as_bytes()).into();
        assert_eq!(*signer.digests.lock().expect("lock"), vec![expected]);
    }

    /// What the mock GitHub received: the installation in the path and the bearer JWT.
    #[derive(Default)]
    struct GitHub {
        requests: Mutex<Vec<(String, String)>>,
    }

    async fn access_tokens(
        State(github): State<Arc<GitHub>>,
        Path(installation): Path<String>,
        headers: HeaderMap,
    ) -> Response {
        let jwt = headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .unwrap_or_default()
            .to_string();
        github
            .requests
            .lock()
            .expect("lock")
            .push((installation.clone(), jwt));
        if installation == "404" {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"message": "Not Found", "documentation_url": "https://docs.github.com/rest/apps/apps#create-an-installation-access-token-for-an-app", "status": "404"})),
            )
                .into_response();
        }
        Json(json!({
            "token": format!("ghs_{installation}"),
            "expires_at": (Utc::now() + chrono::Duration::hours(1)).to_rfc3339(),
        }))
        .into_response()
    }

    /// Starts a mock GitHub API under `/api/v3`, as GitHub Enterprise Server serves it.
    async fn start_github() -> (String, Arc<GitHub>) {
        let github = Arc::new(GitHub::default());
        let app = Router::new()
            .route(
                "/api/v3/app/installations/{installation}/access_tokens",
                post(access_tokens),
            )
            .with_state(Arc::clone(&github));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the mock GitHub");
        let address = listener.local_addr().expect("mock address");
        tokio::spawn(async move { axum::serve(listener, app).await });
        (format!("http://{address}/api/v3"), github)
    }

    #[tokio::test]
    async fn an_installation_token_is_requested_from_the_api_with_a_jwt_the_key_service_signed() {
        let key = test_key();
        let (api_base, github) = start_github().await;
        let signer = Arc::new(LocalSigner {
            key_pair: Arc::clone(&key.key_pair),
            digests: Mutex::new(Vec::new()),
        });
        let provider = GitHubAppTokenProvider::try_new(
            "dataset 'stars' (github)",
            "Iv23liTest".into(),
            AppKey::Signer(signer),
            "42".into(),
            format!("{api_base}/").into(),
        )
        .await
        .expect("an installation token");
        assert_eq!(provider.get_token(), "ghs_42");

        let requests = github.requests.lock().expect("lock").clone();
        assert_eq!(requests.len(), 1);
        let (installation, jwt) = &requests[0];
        assert_eq!(installation, "42");
        assert_eq!(
            verify(jwt, &key.public_pem, "Iv23liTest"),
            ("Iv23liTest".to_string(), 600)
        );
    }

    #[tokio::test]
    async fn a_refusal_names_githubs_reason_and_a_signing_failure_names_the_key() {
        let key = test_key();
        let (api_base, _github) = start_github().await;
        let refused = GitHubAppTokenProvider::try_new(
            "dataset 'stars' (github)",
            "Iv23liTest".into(),
            AppKey::Pem(key.pem.as_str().into()),
            "404".into(),
            api_base.as_str().into(),
        )
        .await
        .expect_err("GitHub refuses the installation");
        assert_eq!(
            refused.to_string(),
            "GitHub refused the GitHub App JWT for installation '404' (HTTP 404 Not Found): Not Found. Verify that `github_client_id` is the App's client ID, that the App is installed as `github_installation_id`, and that the private key is one of the App's. For details, visit: https://spiceai.org/docs/components/data-connectors/github#github-app-installation"
        );

        let unsigned = GitHubAppTokenProvider::try_new(
            "dataset 'stars' (github)",
            "Iv23liTest".into(),
            AppKey::Signer(Arc::new(RefusingSigner)),
            "42".into(),
            api_base.as_str().into(),
        )
        .await
        .expect_err("the key service refuses to sign");
        assert_eq!(
            unsigned.to_string(),
            "Failed to sign the GitHub App JWT with the key 'arn:aws:kms:us-east-1:111122223333:alias/github-app': AccessDeniedException: not authorized to perform kms:Sign"
        );
    }

    #[test]
    fn a_refresh_failure_names_the_component_until_when_its_token_works_and_why() {
        let expires_at = DateTime::parse_from_rfc3339("2026-11-04T06:05:00Z")
            .expect("a timestamp")
            .with_timezone(&Utc);
        let error = GitHubAppError::UnableToSignJwt {
            key_id: "arn:aws:kms:us-east-1:111122223333:alias/github-app".to_string(),
            source: "AWS KMS: AccessDeniedException: not authorized to perform kms:Sign".into(),
        };
        assert_eq!(
            refresh_failure_message(
                "dataset 'issues' (github)",
                expires_at,
                Duration::from_secs(2),
                &error
            ),
            "Failed to refresh the GitHub App token of dataset 'issues' (github), so requests use the current token until it expires at 2026-11-04T06:05:00Z and then fail. Retrying in 2s. Cause: Failed to sign the GitHub App JWT with the key 'arn:aws:kms:us-east-1:111122223333:alias/github-app': AWS KMS: AccessDeniedException: not authorized to perform kms:Sign"
        );
    }

    #[test]
    fn a_refusal_reason_is_githubs_message_on_one_bounded_line() {
        assert_eq!(
            [
                refusal_reason(br#"{"message": "A JSON web token could not be decoded", "documentation_url": "https://docs.github.com"}"#),
                refusal_reason(b"upstream\nconnect error."),
                refusal_reason("x".repeat(400).as_bytes()),
            ],
            [
                "A JSON web token could not be decoded".to_string(),
                "upstream connect error".to_string(),
                format!("{}…", "x".repeat(300)),
            ]
        );
    }
}
