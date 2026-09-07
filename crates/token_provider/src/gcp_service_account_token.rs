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
#![expect(clippy::missing_errors_doc)]

use std::{
    fmt,
    hash::{DefaultHasher, Hash, Hasher},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::{Result, TokenProvider, ensure_jwt_crypto_provider};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use util::fibonacci_backoff::{Backoff as _, FibonacciBackoffBuilder};

const DEFAULT_TOKEN_URI: &str = "https://oauth2.googleapis.com/token";

// Refresh this many seconds before the access token expires.
const TOKEN_REFRESH_BUFFER_SECS: u64 = 60;

// Google honors a JWT-bearer assertion lifetime of at most 1 hour.
const ASSERTION_LIFETIME_SECS: usize = 3600;

#[derive(Debug, Snafu)]
pub enum GcpAuthError {
    #[snafu(display(
        "Invalid GCP service account key: {source}. Verify the JSON key file or string is well-formed."
    ))]
    InvalidServiceAccountJson { source: serde_json::Error },

    #[snafu(display("Invalid GCP service account private key: {source}"))]
    InvalidPrivateKey { source: jsonwebtoken::errors::Error },

    #[snafu(display("Failed to get system time. Verify your system time."))]
    UnableToGetSystemTime { source: std::time::SystemTimeError },

    #[snafu(display("Invalid system time. Verify your system time."))]
    InvalidSystemTime { source: std::num::TryFromIntError },

    #[snafu(display("Failed to sign GCP service account JWT: {source}"))]
    UnableToGenerateJWT { source: jsonwebtoken::errors::Error },

    #[snafu(display(
        "Failed to exchange the GCP service account JWT for an access token at {token_uri}: {source}"
    ))]
    UnableToExchangeToken {
        token_uri: String,
        source: reqwest::Error,
    },

    #[snafu(display("GCP token endpoint {token_uri} returned an error response: {source}"))]
    TokenEndpointStatus {
        token_uri: String,
        source: reqwest::Error,
    },

    #[snafu(display("Failed to parse the GCP token endpoint response: {source}"))]
    InvalidTokenResponse { source: reqwest::Error },
}

/// The fields Spice reads from a GCP service-account JSON key. Other fields present in the key
/// (`project_id`, `client_id`, `private_key_id`, ...) are ignored.
#[derive(Debug, Deserialize)]
struct ServiceAccountKey {
    client_email: String,
    private_key: String,
    #[serde(default)]
    token_uri: Option<String>,
}

#[derive(Serialize)]
struct Claims {
    iss: String,
    scope: String,
    aud: String,
    iat: usize,
    exp: usize,
}

struct AccessToken {
    token: SecretString,
    expires_in_secs: u64,
}

impl AccessToken {
    fn next_wait(&self) -> Duration {
        Duration::from_secs(
            self.expires_in_secs
                .saturating_sub(TOKEN_REFRESH_BUFFER_SECS),
        )
    }
}

/// A [`TokenProvider`] that authenticates as a GCP service account using the OAuth 2.0
/// JWT-bearer grant (RFC 7523): a JWT is self-signed with the service account's private key and
/// exchanged for a short-lived `OAuth2` access token, which is refreshed in the background before
/// it expires.
///
/// Used by the Google connector's Vertex AI mode (`google_api: vertex_ai`) — Vertex AI requires
/// an IAM-scoped `Authorization: Bearer` token, unlike the API-key auth used by the public
/// Google AI Studio API.
pub struct GcpServiceAccountTokenProvider {
    client_email: Arc<str>,
    scope: Arc<str>,
    rx: watch::Receiver<SecretString>,
    _handle: Arc<JoinHandle<()>>,
}

impl Hash for GcpServiceAccountTokenProvider {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Only hash non-sensitive identifiers; do not include the private key or token.
        self.client_email.hash(state);
        self.scope.hash(state);
    }
}

impl fmt::Debug for GcpServiceAccountTokenProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GcpServiceAccountTokenProvider")
            .field("client_email", &self.client_email)
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

impl TokenProvider for GcpServiceAccountTokenProvider {
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
                    () = tx.closed() => break,
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

impl GcpServiceAccountTokenProvider {
    /// Creates a new `GcpServiceAccountTokenProvider` from a GCP service-account JSON key and
    /// spawns a background task that refreshes the access token before it expires.
    ///
    /// `scope` is a space-delimited list of `OAuth2` scopes, e.g.
    /// `"https://www.googleapis.com/auth/cloud-platform"`.
    pub async fn try_new(
        service_account_json: &SecretString,
        scope: impl Into<Arc<str>>,
    ) -> Result<Self, GcpAuthError> {
        let key: ServiceAccountKey = serde_json::from_str(service_account_json.expose_secret())
            .context(InvalidServiceAccountJsonSnafu)?;
        let client_email: Arc<str> = Arc::from(key.client_email);
        let private_key: Arc<str> = Arc::from(key.private_key);
        let token_uri: Arc<str> = Arc::from(
            key.token_uri
                .unwrap_or_else(|| DEFAULT_TOKEN_URI.to_string()),
        );
        let scope: Arc<str> = scope.into();

        let init_token = exchange_token(
            Arc::clone(&client_email),
            Arc::clone(&private_key),
            Arc::clone(&token_uri),
            Arc::clone(&scope),
        )
        .await?;

        let (tx, rx) = watch::channel(init_token.token.clone());

        let cloned_client_email = Arc::clone(&client_email);
        let cloned_private_key = private_key;
        let cloned_token_uri = Arc::clone(&token_uri);
        let cloned_scope = Arc::clone(&scope);

        let handle = tokio::spawn(async move {
            let mut backoff = FibonacciBackoffBuilder::new()
                .max_duration(Some(Duration::from_mins(5)))
                .build();
            let mut next_wait = init_token.next_wait();

            loop {
                // Stop the background task once the provider (and its `rx`) is dropped —
                // otherwise this loop runs forever, holding the private key in memory and
                // making live token-exchange requests to Google for a provider nothing
                // references anymore (e.g. after a model config reload).
                tokio::select! {
                    () = sleep(next_wait) => {}
                    () = tx.closed() => break,
                }

                match exchange_token(
                    Arc::clone(&cloned_client_email),
                    Arc::clone(&cloned_private_key),
                    Arc::clone(&cloned_token_uri),
                    Arc::clone(&cloned_scope),
                )
                .await
                {
                    Ok(new_token) => {
                        backoff.reset();
                        tracing::debug!(
                            "GCP service account access token refreshed for {}; expires in {}s",
                            cloned_client_email,
                            new_token.expires_in_secs
                        );
                        next_wait = new_token.next_wait();
                        if tx.send(new_token.token.clone()).is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        next_wait = backoff.next_duration().unwrap_or(Duration::from_mins(5));
                        tracing::error!(
                            "GCP service account token refresh failed: {e}. Retrying in {next_wait:?}"
                        );
                    }
                }
            }
        });

        Ok(Self {
            client_email,
            scope,
            rx,
            _handle: Arc::new(handle),
        })
    }
}

async fn exchange_token(
    client_email: Arc<str>,
    private_key: Arc<str>,
    token_uri: Arc<str>,
    scope: Arc<str>,
) -> Result<AccessToken, GcpAuthError> {
    ensure_jwt_crypto_provider();

    let iat = usize::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context(UnableToGetSystemTimeSnafu)?
            .as_secs(),
    )
    .context(InvalidSystemTimeSnafu)?;
    let exp = iat + ASSERTION_LIFETIME_SECS;

    let claims = Claims {
        iss: client_email.to_string(),
        scope: scope.to_string(),
        aud: token_uri.to_string(),
        iat,
        exp,
    };

    let encoding_key =
        EncodingKey::from_rsa_pem(private_key.as_bytes()).context(InvalidPrivateKeySnafu)?;
    let jwt = encode(&Header::new(Algorithm::RS256), &claims, &encoding_key)
        .context(UnableToGenerateJWTSnafu)?;

    let client = reqwest::Client::builder()
        .user_agent(util::spiceai_user_agent())
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .build()
        .context(UnableToExchangeTokenSnafu {
            token_uri: token_uri.to_string(),
        })?;

    let response = client
        .post(token_uri.as_ref())
        .form(&[
            ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
            ("assertion", jwt.as_str()),
        ])
        .send()
        .await
        .context(UnableToExchangeTokenSnafu {
            token_uri: token_uri.to_string(),
        })?
        .error_for_status()
        .context(TokenEndpointStatusSnafu {
            token_uri: token_uri.to_string(),
        })?;

    #[expect(clippy::items_after_statements)]
    #[derive(Deserialize)]
    struct TokenResponse {
        access_token: SecretString,
        #[serde(default)]
        expires_in: Option<u64>,
    }

    let resp: TokenResponse = response.json().await.context(InvalidTokenResponseSnafu)?;

    Ok(AccessToken {
        token: resp.access_token,
        expires_in_secs: resp.expires_in.unwrap_or(3600),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_wait_clamps_to_zero_when_already_near_expiry() {
        let token = AccessToken {
            token: SecretString::from("t"),
            expires_in_secs: TOKEN_REFRESH_BUFFER_SECS - 1,
        };
        assert_eq!(token.next_wait(), Duration::from_secs(0));
    }

    #[test]
    fn next_wait_subtracts_refresh_buffer() {
        let token = AccessToken {
            token: SecretString::from("t"),
            expires_in_secs: TOKEN_REFRESH_BUFFER_SECS + 30,
        };
        assert_eq!(token.next_wait(), Duration::from_secs(30));
    }

    #[test]
    fn rejects_malformed_service_account_json() {
        let err = serde_json::from_str::<ServiceAccountKey>("not json").expect_err("should fail");
        // Just verifying our struct's Deserialize surfaces a serde_json error, which
        // `try_new` wraps as `GcpAuthError::InvalidServiceAccountJson`.
        assert!(err.to_string().contains("expected"));
    }

    // A throwaway 2048-bit RSA key generated solely for this test (`openssl genpkey -algorithm
    // RSA -pkeyopt rsa_keygen_bits:2048`). It signs nothing outside this test process and is not
    // associated with any real account.
    const TEST_RSA_PRIVATE_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDLod/dmzVBXYoi
/DqI2Gs/FU8nR2qPtaUWn7Z9+B4xFTVtXUvK16EaGWxBC1GdyUTJbDf6fweA3xQx
XqcD6vuAV9A2gF+VnW4iJJYpAo5HtQEnyE2Dkm3dq4eJqB9TTPGr0q3xwZnV26PP
DX4EhkoAfRvHYvG49KU8mL4lHvvMuuuMt15j6CQXFFieCw26tUvWNM+oEIcS+qtU
C1S1ZIqctuinWrvyXIrQPE4mzInD9zhHOmm7mm6ORMGeG4RdxU7aCDwmTjSXQNrB
CUaOv9DN89H/eGhbkxMRRFuG6yqu/p23IBPq9xVzRujR/JahGAv1AD8ekRNwY0nH
AGVDNE2VAgMBAAECggEABhnR9rY9OLmgN18eM9ZkFC5DwlYB689R5GUhlxGVefv8
L0whxrCCK6GCXEqGBfevP2emH7OpmmNUnAwNbfEe2uAGpXNgKb/fOlelRhJFCAH0
yGfYZq4+62rFs0qdsaW3AeJcgsc9oCol9MCfO2x4kB4vdGBTBKsTvkaHP953RdFw
I6G9WyTNkD9jVCTlYcMD5Xpe2PS3/tL8RThEApD0jz+YSdR2p6UaUrrafRtjlnI7
tPwtIFi62Ywc/mA6E+jYzu4PdEeuLqd0DK+JbYFCajfwY7gFjR7C0EBCEDquctz2
aq5PFOdykNtBcEiClrZ6IVQO7C+JnszFR4cVN1Dp8QKBgQDyTZtoaUHFf+QaTlxc
YYnhasE4fcgMH7szZFEEhShslqQGaJ9uiQtDrs1ZPQu6H1Cb4eUwW40ldVImmhYH
IQ6bBAPaxtVigVdM67qqoXLYezrCESgTImV113oYebVWNA6bIlTr7Iy9XHfr+WrC
SjlV3ZiCj9zcAFbrA6vKu9DVvQKBgQDXJKhyPvKt/Cxz0+xLLOtQR4/4hBsdVQj1
I501athLoUU0luaGcDGqPaM12OKN0rK3Wx0EHLrLgYRMmAnc6vnBGT6s0XihR7MY
FR3VF8eq4XE1hlkIegBQ4xQUtBBjThk9GlZ/YqxinDXA3cqgWqRzwPBoGt32mFmL
YQrXn/K4uQKBgQCAyrWHSyN1qiQBw5MeW44hblAkCd+Sai9kArd5spmpFm07JtNP
urJtGHX4MBWSqeB76xeam58rwO+YqyB4S57q0LiHylZT1LIlpcsDtWtJHD9ANkRZ
31eUyqMQbWjw2BzHYvlGqeapJIRtvpZ7jV5qgK33ACcCHYAVzU68JRa89QKBgH5V
lGSh+jeYoTHS/CqY0gVuQGiaiqGK0ZvBsgswFpsytETRZ8UCFOf3EeQI2+CUHUX2
Ru+nPzlJcLrZfNExWj+950PsLX3ncI9vtvju4dzCTYw0vAtyOMVG0v/lTMpDjiaH
VMuw0bGGMTp0AwxurasbXRCE1lp70/k4nvlHf2xhAoGAJHrFZb651bLr1Lap0Ged
jK2Fwy/3ROiX5ohV6/7nNgRkEyVozi9lg0qfSskLLCkJAvJFxHUMgP3a6zBcyq0G
ZzPq/jR3RXoVFrRypUNKYjSp+RrM8VEMOEbNTcqgJFnSu0HR3ZpeUpuSmtBe5gH/
DnjLo+9WUFjVlKHwAG/JjgU=
-----END PRIVATE KEY-----";

    /// Starts a mock token endpoint that replies once with `access_token`, capturing the raw
    /// request. Returns the endpoint URL and a handle to read the captured request afterward.
    async fn start_mock_token_endpoint(
        access_token: &'static str,
    ) -> (String, Arc<tokio::sync::Mutex<Option<String>>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock listener");
        let addr = listener.local_addr().expect("mock local_addr");
        let captured = Arc::new(tokio::sync::Mutex::new(None));
        let captured_srv = Arc::clone(&captured);

        tokio::spawn(async move {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };

            let mut raw = Vec::new();
            let mut buf = [0u8; 4096];
            let mut header_end = None;
            let mut content_length = 0usize;
            loop {
                let n = match stream.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => n,
                };
                raw.extend_from_slice(&buf[..n]);

                if header_end.is_none()
                    && let Some(idx) = raw.windows(4).position(|w| w == b"\r\n\r\n").map(|i| i + 4)
                {
                    header_end = Some(idx);
                    let header_text = String::from_utf8_lossy(&raw[..idx]);
                    content_length = header_text
                        .lines()
                        .find_map(|line| {
                            let (k, v) = line.split_once(':')?;
                            k.trim()
                                .eq_ignore_ascii_case("Content-Length")
                                .then(|| v.trim().parse::<usize>().ok())
                                .flatten()
                        })
                        .unwrap_or(0);
                }

                if let Some(end) = header_end
                    && raw.len() >= end + content_length
                {
                    break;
                }
            }
            *captured_srv.lock().await = Some(String::from_utf8_lossy(&raw).into_owned());

            let body = format!(
                r#"{{"access_token":"{access_token}","expires_in":3600,"token_type":"Bearer"}}"#
            );
            let http = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(http.as_bytes()).await;
        });

        (format!("http://{addr}/token"), captured)
    }

    /// Base64url-decodes (no padding) a JWT segment into its raw JSON claims.
    fn decode_jwt_segment(segment: &str) -> serde_json::Value {
        use base64::Engine;
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(segment)
            .expect("JWT segment should be valid base64url");
        serde_json::from_slice(&bytes).expect("JWT segment should be valid JSON")
    }

    #[tokio::test]
    async fn try_new_signs_a_jwt_and_exchanges_it_for_an_access_token() {
        let (token_uri, captured) = start_mock_token_endpoint("test-access-token").await;

        let service_account_json = SecretString::from(
            serde_json::json!({
                "client_email": "test@example.iam.gserviceaccount.com",
                "private_key": TEST_RSA_PRIVATE_KEY_PEM,
                "token_uri": token_uri,
            })
            .to_string(),
        );

        let provider = GcpServiceAccountTokenProvider::try_new(
            &service_account_json,
            "https://www.googleapis.com/auth/cloud-platform",
        )
        .await
        .expect("token exchange against the mock endpoint should succeed");

        assert_eq!(provider.get_token(), "test-access-token");

        let request = captured
            .lock()
            .await
            .take()
            .expect("mock endpoint should have received a request");
        assert!(
            request.contains("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer"),
            "expected the JWT-bearer grant_type in the form body: {request}"
        );

        let assertion = request
            .split("assertion=")
            .nth(1)
            .expect("form body should contain the assertion")
            .split(['&', '\r', '\n'])
            .next()
            .expect("assertion value should be present");
        let jwt = urlencoding_decode(assertion);
        let mut parts = jwt.split('.');
        let _header = parts.next().expect("JWT should have a header segment");
        let claims = decode_jwt_segment(parts.next().expect("JWT should have a claims segment"));

        assert_eq!(claims["iss"], "test@example.iam.gserviceaccount.com");
        assert_eq!(
            claims["scope"],
            "https://www.googleapis.com/auth/cloud-platform"
        );
        // `aud` must be the configured `token_uri` (the mock endpoint here, since the service
        // account JSON overrides it) — not a hardcoded default — so a custom `token_uri` in a
        // real service-account key is honored.
        assert_eq!(claims["aud"], token_uri);
    }

    /// Minimal `application/x-www-form-urlencoded` value decoder — just enough to recover a
    /// base64url JWT (`+`/`.`/`-`/`_` survive unescaped; only `%XX` sequences need decoding).
    fn urlencoding_decode(s: &str) -> String {
        let mut out = String::with_capacity(s.len());
        let mut chars = s.chars();
        while let Some(c) = chars.next() {
            if c == '%' {
                let hex: String = chars.by_ref().take(2).collect();
                if let Ok(byte) = u8::from_str_radix(&hex, 16) {
                    out.push(byte as char);
                }
            } else {
                out.push(c);
            }
        }
        out
    }
}
