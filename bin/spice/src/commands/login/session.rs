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

//! Spice Cloud browser sign-in shared by `spice login` and `spice cloud login`:
//! the OAuth 2.0 authorization code grant with PKCE (S256) and a loopback
//! redirect (RFC 8252), plus the refresh grant that renews its access token.

use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use sha2::{Digest as _, Sha256};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

use crate::error::{Error, Result};

/// Every scope the CLI's Spice Cloud calls need, and nothing more.
pub(crate) const SCOPES: &str = "apps:read apps:write apps:delete deployments:read deployments:write secrets:read secrets:write instances:write";

const CLIENT_NAME: &str = "Spice CLI";
const CALLBACK_PATH: &str = "/callback";

/// The authorization server accepts any port on a registered loopback redirect
/// URI, so the CLI registers this one and adds its listener's port per sign-in.
const REGISTERED_REDIRECT_URI: &str = "http://127.0.0.1/callback";

const SIGN_IN_TIMEOUT: Duration = Duration::from_mins(5);
const CALLBACK_READ_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_CALLBACK_REQUEST_BYTES: usize = 16 * 1024;

const CALLBACK_PAGE: &str = "Spice CLI received the sign-in response. Return to your terminal.";

/// The tokens one grant returned.
pub(crate) struct Grant {
    pub(crate) access_token: String,
    pub(crate) refresh_token: String,
}

/// Redacts both tokens; this type carries live credentials.
impl std::fmt::Debug for Grant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Grant")
            .field("access_token", &"<redacted>")
            .field("refresh_token", &"<redacted>")
            .finish()
    }
}

/// A completed browser sign-in and the client it was granted to.
#[derive(Debug)]
pub(crate) struct SignedIn {
    pub(crate) client_id: String,
    pub(crate) grant: Grant,
}

/// Sign in through the browser and return the granted tokens.
///
/// Reuses the client ID stored by an earlier sign-in, and registers a new
/// public client when there is none. Without a browser to open, or when stdin
/// is a terminal, the person can also paste the address the browser landed on,
/// for a browser on another machine.
///
/// # Errors
///
/// Returns an error if registration fails, the browser returns an error or a
/// response for another sign-in, no response arrives in time, or the code
/// exchange fails.
pub(crate) async fn sign_in(
    portal: &str,
    stored_client_id: Option<String>,
    open_browser: bool,
    requested_org: Option<&str>,
) -> Result<SignedIn> {
    let http = super::credentialed_client()?;
    let client_id = match stored_client_id {
        Some(client_id) => client_id,
        None => register_client(&http, portal).await?,
    };

    let flow = Flow::start(portal, client_id.clone()).await?;
    let authorize_url = flow.authorize_url()?;
    let accept_paste = !open_browser || std::io::IsTerminal::is_terminal(&std::io::stdin());
    if open_browser {
        eprintln!(
            "Opening the Spice Cloud sign-in page in your browser. If it does not open, visit:\n\n  {authorize_url}\n"
        );
        tokio::task::spawn_blocking(move || {
            let _ = system_open::that(authorize_url);
        });
    } else {
        eprintln!("Open this URL in a browser to sign in to Spice Cloud:\n\n  {authorize_url}\n");
    }
    if accept_paste {
        eprintln!(
            "If the browser is on another machine, it shows an error page after you approve. Copy the address of that page and paste it here.\n"
        );
    }
    if let Some(org) = requested_org {
        eprintln!("Choose organization '{org}' when Spice Cloud asks.");
    }
    eprintln!("Waiting for the sign-in to finish...");

    let pasted = accept_paste.then(read_pasted_line);
    let grant = flow.finish(&http, pasted, SIGN_IN_TIMEOUT).await?;
    Ok(SignedIn { client_id, grant })
}

/// Exchange `refresh_token` for a new access token and refresh token.
///
/// # Errors
///
/// Returns an error if the token endpoint cannot be reached or refuses the
/// refresh token.
pub(crate) async fn refresh(portal: &str, client_id: &str, refresh_token: &str) -> Result<Grant> {
    let http = super::credentialed_client()?;
    token_request(
        &http,
        portal,
        &serde_json::json!({
            "grant_type": "refresh_token",
            "client_id": client_id,
            "refresh_token": refresh_token,
        }),
    )
    .await
}

/// What a browser sign-in's access token says about its own renewal.
pub(crate) struct AccessClaims {
    /// Seconds until the token expires, negative once it has passed.
    pub(crate) seconds_left: i64,
    /// The OAuth client the token was granted to.
    pub(crate) client_id: String,
}

/// The `exp` and `client_id` claims of a JWT access token, or `None` when the
/// token is not a JWT with both.
///
/// Reads the claims for renewal only; the server checks the signature.
pub(crate) fn access_claims(token: &str) -> Option<AccessClaims> {
    let mut parts = token.split('.');
    let (Some(_), Some(payload), Some(_), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return None;
    };
    let claims: serde_json::Value =
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload).ok()?).ok()?;
    Some(AccessClaims {
        seconds_left: claims.get("exp")?.as_i64()? - chrono::Utc::now().timestamp(),
        client_id: claims.get("client_id")?.as_str()?.to_string(),
    })
}

/// One authorization request: its PKCE verifier, its `state`, and the loopback
/// listener its redirect comes back to.
struct Flow {
    portal: String,
    client_id: String,
    redirect_uri: String,
    state: String,
    verifier: String,
    listener: TcpListener,
}

impl Flow {
    /// Bind the loopback listener and make a new verifier and `state`.
    async fn start(portal: &str, client_id: String) -> Result<Self> {
        let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .await
            .map_err(|e| Error::InvalidArgument {
                message: format!("Could not listen on 127.0.0.1 for the sign-in redirect: {e}"),
            })?;
        let port = listener
            .local_addr()
            .map_err(|e| Error::InvalidArgument {
                message: format!("Could not read the sign-in listener address: {e}"),
            })?
            .port();

        Ok(Self {
            portal: portal.trim_end_matches('/').to_string(),
            client_id,
            redirect_uri: format!("http://127.0.0.1:{port}{CALLBACK_PATH}"),
            state: random_token(),
            verifier: random_token(),
            listener,
        })
    }

    fn authorize_url(&self) -> Result<String> {
        let mut url =
            reqwest::Url::parse(&format!("{}/oauth/authorize", self.portal)).map_err(|e| {
                Error::InvalidArgument {
                    message: format!("Invalid Spice Cloud URL '{}': {e}", self.portal),
                }
            })?;
        url.query_pairs_mut()
            .append_pair("response_type", "code")
            .append_pair("client_id", &self.client_id)
            .append_pair("redirect_uri", &self.redirect_uri)
            .append_pair("scope", SCOPES)
            .append_pair("state", &self.state)
            .append_pair("code_challenge", &code_challenge(&self.verifier))
            .append_pair("code_challenge_method", "S256");
        Ok(url.into())
    }

    /// Wait for the redirect, check it, and exchange its code.
    async fn finish(
        self,
        http: &reqwest::Client,
        pasted: Option<oneshot::Receiver<String>>,
        timeout: Duration,
    ) -> Result<Grant> {
        let Self {
            portal,
            client_id,
            redirect_uri,
            state,
            verifier,
            listener,
        } = self;

        let query = wait_for_redirect(listener, pasted, timeout).await?;
        let code = authorization_code(&query, &state)?;
        token_request(
            http,
            &portal,
            &serde_json::json!({
                "grant_type": "authorization_code",
                "client_id": client_id,
                "code": code,
                "redirect_uri": redirect_uri,
                "code_verifier": verifier,
            }),
        )
        .await
    }
}

/// 32 random bytes, base64url without padding: 43 characters, valid as a PKCE
/// verifier and as `state`.
fn random_token() -> String {
    use rand::RngExt as _;
    let mut bytes = [0u8; 32];
    rand::rng().fill(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

fn code_challenge(verifier: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
}

/// Read one line from stdin on a plain thread. A blocking stdin read cannot be
/// cancelled, and a Tokio blocking task would hold the runtime open at exit.
fn read_pasted_line() -> oneshot::Receiver<String> {
    let (sender, receiver) = oneshot::channel();
    std::thread::spawn(move || {
        let mut line = String::new();
        while matches!(std::io::stdin().read_line(&mut line), Ok(n) if n > 0) {
            if !line.trim().is_empty() {
                let _ = sender.send(line);
                return;
            }
            line.clear();
        }
    });
    receiver
}

/// The query of the first redirect to arrive, from the loopback listener or
/// pasted by the person. The listener closes when this returns.
async fn wait_for_redirect(
    listener: TcpListener,
    mut pasted: Option<oneshot::Receiver<String>>,
    timeout: Duration,
) -> Result<Vec<(String, String)>> {
    let deadline = tokio::time::sleep(timeout);
    tokio::pin!(deadline);

    loop {
        tokio::select! {
            () = &mut deadline => {
                return Err(Error::InvalidArgument {
                    message: "Timed out waiting for the browser sign-in. If the browser showed an error, run 'spice cloud logout', then 'spice login' again.".to_string(),
                });
            }
            accepted = listener.accept() => {
                let Ok((stream, _)) = accepted else { continue };
                if let Some(query) = serve_callback(stream).await {
                    return Ok(query);
                }
            }
            line = next_pasted_line(&mut pasted) => {
                match line {
                    Some(line) => return pasted_query(line.trim()),
                    // stdin closed without a line; keep waiting for the browser.
                    None => pasted = None,
                }
            }
        }
    }
}

/// The pasted line, `None` when stdin closed without one, or never when
/// nothing is read from stdin.
async fn next_pasted_line(pasted: &mut Option<oneshot::Receiver<String>>) -> Option<String> {
    match pasted {
        Some(receiver) => receiver.await.ok(),
        None => std::future::pending().await,
    }
}

/// Answer one loopback request. Returns the query of a request to the callback
/// path; any other request gets a 404 and the wait continues.
async fn serve_callback(mut stream: TcpStream) -> Option<Vec<(String, String)>> {
    let target = tokio::time::timeout(CALLBACK_READ_TIMEOUT, read_request_target(&mut stream))
        .await
        .ok()
        .flatten();
    let url =
        target.and_then(|target| reqwest::Url::parse(&format!("http://127.0.0.1{target}")).ok());

    let Some(url) = url.filter(|url| url.path() == CALLBACK_PATH) else {
        let _ = stream
            .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await;
        return None;
    };

    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\nCache-Control: no-store\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{CALLBACK_PAGE}",
        CALLBACK_PAGE.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.shutdown().await;
    Some(url.query_pairs().into_owned().collect())
}

/// The request target of a `GET` request. Reads the whole request head, so
/// closing the socket after the reply does not reset the connection.
async fn read_request_target(stream: &mut TcpStream) -> Option<String> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        if buffer.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
        if buffer.len() >= MAX_CALLBACK_REQUEST_BYTES {
            return None;
        }
        let read = stream.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        buffer.extend_from_slice(&chunk[..read]);
    }

    let line_end = buffer.windows(2).position(|pair| pair == b"\r\n")?;
    let line = std::str::from_utf8(&buffer[..line_end]).ok()?;
    let mut parts = line.split(' ');
    match (parts.next(), parts.next()) {
        (Some("GET"), Some(target)) if target.starts_with('/') => Some(target.to_string()),
        _ => None,
    }
}

fn pasted_query(pasted: &str) -> Result<Vec<(String, String)>> {
    let url = reqwest::Url::parse(pasted).map_err(|_| Error::InvalidArgument {
        message:
            "The pasted text is not the address the browser landed on. Run 'spice login' again."
                .to_string(),
    })?;
    Ok(url.query_pairs().into_owned().collect())
}

/// The authorization code from a redirect that belongs to this sign-in.
///
/// `state` is checked first, so an error or a code from another authorization
/// request is never acted on.
fn authorization_code(query: &[(String, String)], state: &str) -> Result<String> {
    let param = |name: &str| {
        query
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    };

    if param("state") != Some(state) {
        return Err(Error::InvalidArgument {
            message: "The sign-in response does not belong to this sign-in, so it was refused. Run 'spice login' again.".to_string(),
        });
    }

    if let Some(error) = param("error") {
        if error == "access_denied" {
            return Err(Error::DeviceAuthorizationDenied);
        }
        let detail = param("error_description").unwrap_or(error);
        return Err(Error::InvalidArgument {
            message: format!("Spice Cloud refused the sign-in: {detail}"),
        });
    }

    match param("code") {
        Some(code) if !code.is_empty() => Ok(code.to_string()),
        _ => Err(Error::InvalidArgument {
            message: "The sign-in response has no authorization code. Run 'spice login' again."
                .to_string(),
        }),
    }
}

/// Register the CLI as a public client: no secret, PKCE required.
async fn register_client(http: &reqwest::Client, portal: &str) -> Result<String> {
    #[derive(serde::Deserialize)]
    struct Registered {
        client_id: String,
    }

    let url = format!("{}/api/oauth/register", portal.trim_end_matches('/'));
    let response = http
        .post(&url)
        .headers(crate::commands::cloud::client::portal_headers()?)
        .json(&serde_json::json!({
            "client_name": CLIENT_NAME,
            "redirect_uris": [REGISTERED_REDIRECT_URI],
            "grant_types": ["authorization_code", "refresh_token"],
            "response_types": ["code"],
            "token_endpoint_auth_method": "none",
            "scope": SCOPES,
        }))
        .send()
        .await
        .map_err(|e| Error::InvalidResponse {
            message: format!("Could not reach Spice Cloud to register the CLI: {e}"),
        })?;

    let status = response.status();
    if !status.is_success() {
        return Err(Error::InvalidResponse {
            message: format!(
                "Spice Cloud did not register the CLI: {}",
                oauth_error_detail(response).await
            ),
        });
    }

    let registered: Registered = response.json().await.map_err(|e| Error::InvalidResponse {
        message: format!("Could not read the Spice Cloud client registration: {e}"),
    })?;
    Ok(registered.client_id)
}

async fn token_request(
    http: &reqwest::Client,
    portal: &str,
    body: &serde_json::Value,
) -> Result<Grant> {
    #[derive(serde::Deserialize)]
    struct TokenResponse {
        access_token: String,
        token_type: String,
        #[serde(default)]
        refresh_token: Option<String>,
    }

    let url = format!("{}/api/oauth/token", portal.trim_end_matches('/'));
    let response = http
        .post(&url)
        .headers(crate::commands::cloud::client::portal_headers()?)
        .json(body)
        .send()
        .await
        .map_err(|e| Error::InvalidResponse {
            message: format!("Could not reach the Spice Cloud token endpoint: {e}"),
        })?;

    if !response.status().is_success() {
        return Err(Error::InvalidResponse {
            message: format!(
                "Spice Cloud refused the token request: {}",
                oauth_error_detail(response).await
            ),
        });
    }

    let token: TokenResponse = response.json().await.map_err(|e| Error::InvalidResponse {
        message: format!("Could not read the Spice Cloud token response: {e}"),
    })?;
    if !token.token_type.eq_ignore_ascii_case("bearer") || token.access_token.is_empty() {
        return Err(Error::InvalidResponse {
            message: "Spice Cloud returned a token the CLI cannot use.".to_string(),
        });
    }
    let Some(refresh_token) = token.refresh_token.filter(|token| !token.is_empty()) else {
        return Err(Error::InvalidResponse {
            message: "Spice Cloud returned no refresh token.".to_string(),
        });
    };

    Ok(Grant {
        access_token: token.access_token,
        refresh_token,
    })
}

/// The `error_description` (or `error`) of an OAuth error response, or its
/// status when the body has neither.
async fn oauth_error_detail(response: reqwest::Response) -> String {
    #[derive(serde::Deserialize)]
    struct OAuthError {
        error: Option<String>,
        error_description: Option<String>,
    }

    let status = response.status();
    response
        .json::<OAuthError>()
        .await
        .ok()
        .and_then(|body| body.error_description.or(body.error))
        .unwrap_or_else(|| status.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, Request, ResponseTemplate};

    const TEST_TIMEOUT: Duration = Duration::from_secs(5);

    fn query_param(url: &str, name: &str) -> String {
        reqwest::Url::parse(url)
            .expect("authorize URL parses")
            .query_pairs()
            .find(|(key, _)| key == name)
            .map_or_else(
                || panic!("authorize URL has no {name}"),
                |(_, value)| value.into_owned(),
            )
    }

    /// Stand in for the browser: follow the redirect to the CLI's listener.
    async fn browser_redirect(authorize_url: &str, query: &str) -> reqwest::StatusCode {
        let redirect_uri = query_param(authorize_url, "redirect_uri");
        reqwest::get(format!("{redirect_uri}?{query}"))
            .await
            .expect("the listener answers")
            .status()
    }

    /// A token endpoint that records each request body and grants tokens.
    async fn token_endpoint(server: &MockServer) -> Arc<Mutex<Vec<serde_json::Value>>> {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&seen);
        Mock::given(method("POST"))
            .and(path("/api/oauth/token"))
            .respond_with(move |request: &Request| {
                let body: serde_json::Value =
                    serde_json::from_slice(&request.body).expect("token request is JSON");
                recorded.lock().expect("lock").push(body);
                ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "access_token": "access-1",
                    "token_type": "Bearer",
                    "expires_in": 3600,
                    "refresh_token": "refresh-1",
                }))
            })
            .mount(server)
            .await;
        seen
    }

    #[tokio::test]
    async fn a_completed_sign_in_sends_the_code_with_the_verifier_that_matches_its_challenge() {
        let server = MockServer::start().await;
        let seen = token_endpoint(&server).await;
        let flow = Flow::start(&server.uri(), "client-1".to_string())
            .await
            .expect("flow starts");
        let authorize_url = flow.authorize_url().expect("authorize URL");
        let state = query_param(&authorize_url, "state");
        let challenge = query_param(&authorize_url, "code_challenge");
        assert_eq!(query_param(&authorize_url, "code_challenge_method"), "S256");
        assert_eq!(query_param(&authorize_url, "scope"), SCOPES);

        let browser = tokio::spawn(async move {
            // A browser also asks for a favicon; that must not end the sign-in.
            let favicon = reqwest::get(format!(
                "{}/favicon.ico",
                query_param(&authorize_url, "redirect_uri").trim_end_matches(CALLBACK_PATH)
            ))
            .await
            .expect("the listener answers")
            .status();
            assert_eq!(favicon, reqwest::StatusCode::NOT_FOUND);
            browser_redirect(&authorize_url, &format!("code=code-1&state={state}")).await
        });

        let http = reqwest::Client::new();
        let grant = flow
            .finish(&http, None, TEST_TIMEOUT)
            .await
            .expect("sign-in completes");
        assert_eq!(
            browser.await.expect("browser task"),
            reqwest::StatusCode::OK
        );
        assert_eq!(grant.access_token, "access-1");
        assert_eq!(grant.refresh_token, "refresh-1");

        let requests = seen.lock().expect("lock");
        let [request] = requests.as_slice() else {
            panic!("expected one token request, got {}", requests.len());
        };
        assert_eq!(request["grant_type"], "authorization_code");
        assert_eq!(request["client_id"], "client-1");
        assert_eq!(request["code"], "code-1");
        let verifier = request["code_verifier"].as_str().expect("verifier");
        assert_eq!(code_challenge(verifier), challenge);
    }

    #[tokio::test]
    async fn each_sign_in_uses_a_new_verifier_and_state() {
        let first = Flow::start("https://spice.test", "client".to_string())
            .await
            .expect("flow starts");
        let second = Flow::start("https://spice.test", "client".to_string())
            .await
            .expect("flow starts");

        assert_eq!(first.verifier.len(), 43);
        assert_ne!(first.verifier, second.verifier);
        assert_ne!(first.state, second.state);
        assert_ne!(
            query_param(&first.authorize_url().expect("url"), "code_challenge"),
            query_param(&second.authorize_url().expect("url"), "code_challenge"),
        );
    }

    #[tokio::test]
    async fn a_redirect_with_another_state_is_refused_without_a_token_request() {
        let server = MockServer::start().await;
        let seen = token_endpoint(&server).await;
        let flow = Flow::start(&server.uri(), "client-1".to_string())
            .await
            .expect("flow starts");
        let authorize_url = flow.authorize_url().expect("authorize URL");
        let browser = tokio::spawn(async move {
            browser_redirect(&authorize_url, "code=stolen&state=someone-else").await
        });

        let err = flow
            .finish(&reqwest::Client::new(), None, TEST_TIMEOUT)
            .await
            .expect_err("a foreign state must be refused");
        browser.await.expect("browser task");
        assert!(
            err.to_string().contains("does not belong to this sign-in"),
            "{err}"
        );
        assert!(
            seen.lock().expect("lock").is_empty(),
            "no code may be exchanged"
        );
    }

    #[tokio::test]
    async fn an_error_redirect_is_refused() {
        for (query, expected) in [
            ("error=access_denied", "denied"),
            (
                "error=server_error&error_description=try+later",
                "try later",
            ),
        ] {
            let server = MockServer::start().await;
            let seen = token_endpoint(&server).await;
            let flow = Flow::start(&server.uri(), "client-1".to_string())
                .await
                .expect("flow starts");
            let authorize_url = flow.authorize_url().expect("authorize URL");
            let state = query_param(&authorize_url, "state");
            let browser = tokio::spawn(async move {
                browser_redirect(&authorize_url, &format!("{query}&state={state}")).await
            });

            let err = flow
                .finish(&reqwest::Client::new(), None, TEST_TIMEOUT)
                .await
                .expect_err("an error redirect must fail the sign-in");
            browser.await.expect("browser task");
            assert!(err.to_string().contains(expected), "{query}: {err}");
            assert!(
                seen.lock().expect("lock").is_empty(),
                "{query}: no token request"
            );
        }
    }

    #[tokio::test]
    async fn the_listener_is_loopback_only_and_closes_after_one_callback() {
        let server = MockServer::start().await;
        token_endpoint(&server).await;
        let flow = Flow::start(&server.uri(), "client-1".to_string())
            .await
            .expect("flow starts");
        let bound = flow.listener.local_addr().expect("listener address");
        assert_eq!(bound.ip(), std::net::IpAddr::from(Ipv4Addr::LOCALHOST));

        let authorize_url = flow.authorize_url().expect("authorize URL");
        let state = query_param(&authorize_url, "state");
        let callback = authorize_url.clone();
        tokio::spawn(async move {
            browser_redirect(&callback, &format!("code=code-1&state={state}")).await
        });
        flow.finish(&reqwest::Client::new(), None, TEST_TIMEOUT)
            .await
            .expect("sign-in completes");

        assert!(
            TcpStream::connect(bound).await.is_err(),
            "the listener must be closed after its callback"
        );
    }

    #[tokio::test]
    async fn no_redirect_times_out() {
        let flow = Flow::start("https://spice.test", "client-1".to_string())
            .await
            .expect("flow starts");
        let err = flow
            .finish(&reqwest::Client::new(), None, Duration::from_millis(100))
            .await
            .expect_err("no redirect must time out");
        assert!(err.to_string().contains("Timed out"), "{err}");
    }

    #[tokio::test]
    async fn a_pasted_redirect_address_completes_the_sign_in() {
        let server = MockServer::start().await;
        let seen = token_endpoint(&server).await;
        let flow = Flow::start(&server.uri(), "client-1".to_string())
            .await
            .expect("flow starts");
        let authorize_url = flow.authorize_url().expect("authorize URL");
        let state = query_param(&authorize_url, "state");
        let redirect_uri = query_param(&authorize_url, "redirect_uri");

        let (sender, receiver) = oneshot::channel();
        sender
            .send(format!("{redirect_uri}?code=code-2&state={state}\n"))
            .expect("send pasted line");
        flow.finish(&reqwest::Client::new(), Some(receiver), TEST_TIMEOUT)
            .await
            .expect("sign-in completes");

        assert_eq!(seen.lock().expect("lock")[0]["code"], "code-2");
    }

    #[tokio::test]
    async fn registration_asks_for_a_public_client_with_the_cli_scopes() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/oauth/register"))
            .and(wiremock::matchers::body_partial_json(serde_json::json!({
                "redirect_uris": [REGISTERED_REDIRECT_URI],
                "token_endpoint_auth_method": "none",
                "scope": SCOPES,
            })))
            .respond_with(
                ResponseTemplate::new(201).set_body_json(serde_json::json!({ "client_id": "c-1" })),
            )
            .mount(&server)
            .await;

        let client_id = register_client(&reqwest::Client::new(), &server.uri())
            .await
            .expect("registration succeeds");
        assert_eq!(client_id, "c-1");
    }

    #[tokio::test]
    async fn a_refused_refresh_reports_the_server_reason() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/oauth/token"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": "invalid_grant",
                "error_description": "The refresh token is not valid",
            })))
            .mount(&server)
            .await;

        let err = refresh(&server.uri(), "client-1", "old")
            .await
            .expect_err("a refused refresh fails");
        assert!(
            err.to_string().contains("The refresh token is not valid"),
            "{err}"
        );
    }

    #[test]
    fn claims_are_read_from_a_jwt_and_absent_otherwise() {
        let in_ten_minutes = chrono::Utc::now().timestamp() + 600;
        let payload = URL_SAFE_NO_PAD.encode(format!(
            r#"{{"exp":{in_ten_minutes},"client_id":"client-1"}}"#
        ));
        let claims = access_claims(&format!("e30.{payload}.c2ln")).expect("a JWT has claims");
        assert!(
            (590..=600).contains(&claims.seconds_left),
            "{}",
            claims.seconds_left
        );
        assert_eq!(claims.client_id, "client-1");

        let no_client = URL_SAFE_NO_PAD.encode(format!(r#"{{"exp":{in_ten_minutes}}}"#));
        assert!(access_claims(&format!("e30.{no_client}.c2ln")).is_none());
        assert!(access_claims("pat_123").is_none());
    }

    #[test]
    fn grant_debug_redacts_the_tokens() {
        let rendered = format!(
            "{:?}",
            Grant {
                access_token: "secret-access".to_string(),
                refresh_token: "secret-refresh".to_string(),
            }
        );
        assert!(!rendered.contains("secret-"), "{rendered}");
    }
}
