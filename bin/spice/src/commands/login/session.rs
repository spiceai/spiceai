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

//! Authenticated Spice.ai login sessions.
//!
//! `spice login` completes a browser authorization and persists the credential
//! for later commands. Commands that need the credential *in the same process*
//! — `spice connect` running an inline login and then enrolling with it —
//! consume the result as an [`AuthenticatedSession`] instead of shelling out to
//! `spice login` and re-reading the credential store afterwards: the session
//! carries the live access token, the identity it belongs to, and a
//! [`CredentialStore`] handle naming where the credential was persisted.
//!
//! The access token and app API key are unreachable through `Debug`: a session
//! travels through command code whose errors and traces format their context,
//! and no formatting path may put a live credential in a log line.

use crate::commands::cloud::CloudClient;
use crate::error::{Error, Result};

use super::{LoginOutput, SpiceAuthContext, save_credentials};

/// Where a login credential was persisted, without holding the credential.
///
/// Carried by [`AuthenticatedSession`] so a caller that obtains more
/// credentials mid-command can file them where the user's login already lives,
/// rather than guessing a backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialStore {
    /// Credentials are merged into the local `.env`/`.env.local` file.
    EnvFile,
    /// Credentials are stored in the platform keychain.
    Keychain,
    /// Nothing was persisted — the credential was printed (JSON mode) or
    /// supplied externally. Saving through this handle fails rather than
    /// silently dropping a credential.
    Unpersisted,
}

impl CredentialStore {
    /// The store the given login output backend persists to.
    pub(super) fn from_output(output: &LoginOutput) -> Self {
        match output {
            LoginOutput::Env => Self::EnvFile,
            LoginOutput::Keychain => Self::Keychain,
            LoginOutput::Json => Self::Unpersisted,
        }
    }

    /// Save credentials to this store, using the same writers as `spice login`.
    ///
    /// # Errors
    ///
    /// Returns an error if the store cannot be written, or if this handle is
    /// [`CredentialStore::Unpersisted`] — a caller holding no persistent store
    /// must decide what to do with the credential itself.
    pub fn save(&self, auth_type: &str, params: &[(&str, &str)]) -> Result<()> {
        match self {
            Self::EnvFile => save_credentials(&LoginOutput::Env, auth_type, params),
            Self::Keychain => save_credentials(&LoginOutput::Keychain, auth_type, params),
            Self::Unpersisted => Err(Error::InvalidArgument {
                message: "Failed to save credentials: this login was not persisted (JSON output \
                          mode), so there is no credential store to write to."
                    .to_string(),
            }),
        }
    }
}

/// A live Spice.ai login: the access token, the identity it authenticates, and
/// where the credential was persisted.
///
/// Internal to the CLI. The token is reachable only through
/// [`Self::access_token`] and [`Self::management_client`], never through
/// `Debug`.
pub struct AuthenticatedSession {
    access_token: String,
    username: String,
    email: String,
    org_name: String,
    app_name: Option<String>,
    credential_store: CredentialStore,
}

/// Redacts the token. Sessions travel through command code whose errors and
/// traces format their context, and this type must never be the reason a live
/// credential reaches a log line. The app API key is deliberately not retained
/// on the session at all — it is persisted by [`establish_session`] and read
/// back from the credential store by whatever needs it.
impl std::fmt::Debug for AuthenticatedSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthenticatedSession")
            .field("access_token", &"<redacted>")
            .field("username", &self.username)
            .field("email", &self.email)
            .field("org_name", &self.org_name)
            .field("app_name", &self.app_name)
            .field("credential_store", &self.credential_store)
            .finish()
    }
}

impl AuthenticatedSession {
    /// The login's username.
    #[must_use]
    pub fn username(&self) -> &str {
        &self.username
    }

    /// The login's email address.
    #[must_use]
    pub fn email(&self) -> &str {
        &self.email
    }

    /// The organization the login itself belongs to.
    ///
    /// This is the login's *default* org — evidence of identity, not a choice
    /// of enrollment target. Callers that act on an organization resolve one
    /// explicitly with
    /// [`super::connect_org::resolve_connect_organization`].
    #[must_use]
    pub fn org_name(&self) -> &str {
        &self.org_name
    }

    /// The app the login resolved, when the auth context reported one.
    #[must_use]
    pub fn app_name(&self) -> Option<&str> {
        self.app_name.as_deref()
    }

    /// Where the login credential was persisted.
    #[must_use]
    pub fn credential_store(&self) -> CredentialStore {
        self.credential_store
    }

    /// The live access token. Deliberately a method, not a field: reaching for
    /// the credential is explicit at the call site, and `Debug` never sees it.
    #[must_use]
    pub(crate) fn access_token(&self) -> &str {
        &self.access_token
    }

    /// A Spice Cloud management client authenticated as this session, bound to
    /// no organization.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be built.
    pub fn management_client(&self) -> Result<CloudClient> {
        CloudClient::with_token_for_org(self.access_token().to_string(), None)
    }
}

/// How the browser authorization ended: with a token, or with the user
/// refusing it. Refusal is a decision, not a failure — inline callers treat it
/// as a clean cancellation.
pub(super) enum BrowserLoginOutcome {
    /// The user authorized the login.
    Granted {
        access_token: String,
        context: SpiceAuthContext,
    },
    /// The user refused the authorization in the browser.
    Declined,
}

/// Redacts the token; see [`AuthenticatedSession`]'s `Debug` for why.
impl std::fmt::Debug for BrowserLoginOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Granted { context, .. } => f
                .debug_struct("Granted")
                .field("access_token", &"<redacted>")
                .field("context", context)
                .finish(),
            Self::Declined => f.write_str("Declined"),
        }
    }
}

/// One Spice.ai browser login flow: a locally minted auth code, the URL that
/// authorizes it, and the poll that waits for the authorization.
pub(super) struct BrowserLogin {
    base_url: String,
    auth_code: String,
}

impl BrowserLogin {
    pub(super) fn new() -> Self {
        Self::at(super::spice_base_url())
    }

    /// A flow against an explicit base URL. Tests point this at a mock server;
    /// production goes through [`Self::new`].
    pub(super) fn at(base_url: String) -> Self {
        Self {
            base_url,
            auth_code: super::generate_auth_code(),
        }
    }

    /// The authorization page for this flow's auth code.
    fn auth_url(&self) -> String {
        format!("{}/auth/token?code={}", self.base_url, self.auth_code)
    }

    /// Print the preamble `spice login` has always shown: the auth code to
    /// compare in the browser, and the URL to visit if the browser does not
    /// open.
    pub(super) fn announce(&self) {
        println!("Attempting to open Spice.ai authorization page in your default browser");
        println!("\nYour auth code:\n");
        println!("{}-{}", &self.auth_code[..4], &self.auth_code[4..]);
        println!("\nIf the browser does not open, visit the following URL manually:");
        println!("\n{}\n", self.auth_url());
    }

    /// Open the authorization page in the default browser, fire-and-forget.
    pub(super) fn open_browser(&self) {
        // `Command::status` in spawn_blocking so the opener does not block a
        // Tokio worker or delay the poll loop.
        let auth_url = self.auth_url();
        tokio::task::spawn_blocking(move || {
            let _ = system_open::that(auth_url);
        });
    }

    /// Wait for the browser authorization, then fetch who it authenticated.
    ///
    /// # Errors
    ///
    /// Returns an error if the exchange fails in a way waiting cannot clear,
    /// if the wait times out, or if the auth context cannot be fetched.
    pub(super) async fn authenticate(&self) -> Result<BrowserLoginOutcome> {
        self.authenticate_within(super::LOGIN_POLL_TIMEOUT, super::LOGIN_POLL_INTERVAL)
            .await
    }

    /// [`Self::authenticate`] with explicit bounds, so tests do not wait out
    /// the production timeout.
    async fn authenticate_within(
        &self,
        timeout: std::time::Duration,
        interval: std::time::Duration,
    ) -> Result<BrowserLoginOutcome> {
        let client = super::credentialed_client()?;
        let exchange_url = format!("{}/auth/token/exchange", self.base_url);

        let access_token = match super::poll_for_access_token(
            &client,
            &exchange_url,
            &self.auth_code,
            timeout,
            interval,
        )
        .await?
        {
            super::AccessTokenPoll::Granted(token) => token,
            super::AccessTokenPoll::Denied => return Ok(BrowserLoginOutcome::Declined),
        };

        // The spicepod manifest in the working directory may name a preferred
        // org/app for the auth context to resolve against.
        let (org_name, app_name) = super::read_spicepod_metadata();

        let context = super::get_spice_auth_context(
            &self.base_url,
            &access_token,
            org_name.as_deref(),
            app_name.as_deref(),
        )
        .await?;

        Ok(BrowserLoginOutcome::Granted {
            access_token,
            context,
        })
    }
}

/// Persist the credential the browser flow produced and build the session.
///
/// This is the persistence `spice login` has always performed —
/// `SPICE_SPICEAI_TOKEN` and `SPICE_SPICEAI_API_KEY` written to the chosen
/// store — so an inline login leaves the machine in the same state a
/// standalone `spice login` would. A missing API key is persisted as an empty
/// value so an older key cannot remain active for the new login. Taking a
/// [`CredentialStore`] (not a
/// [`LoginOutput`]) makes the persistence policy explicit at every call site
/// and leaves no path that could print the secrets instead of storing them.
///
/// # Errors
///
/// Returns an error if the credential store cannot be written, or if `store`
/// is [`CredentialStore::Unpersisted`] — a session must not claim a login
/// completed while its credential went nowhere.
pub(super) fn establish_session(
    access_token: String,
    context: SpiceAuthContext,
    store: CredentialStore,
) -> Result<AuthenticatedSession> {
    let api_key = context.app_api_key.clone().unwrap_or_default();
    store.save(
        "SPICEAI",
        &[("TOKEN", &access_token), ("API_KEY", &api_key)],
    )?;

    Ok(AuthenticatedSession {
        access_token,
        username: context.username,
        email: context.email,
        org_name: context.org_name,
        app_name: context.app_name,
        credential_store: store,
    })
}

/// Print the success lines `spice login` has always printed.
pub(super) fn print_login_success(session: &AuthenticatedSession) {
    println!(
        "\x1b[32mSuccessfully logged in to Spice.ai as {} ({})\x1b[0m",
        session.username(),
        session.email()
    );
    println!(
        "\x1b[32mUsing app {}/{}\x1b[0m",
        session.org_name(),
        session.app_name().unwrap_or_default()
    );
}

/// How an inline login ended, for a command that continues afterwards.
#[derive(Debug)]
pub enum LoginContinuation {
    /// The login completed; the session is live and the credential persisted.
    Authenticated(Box<AuthenticatedSession>),
    /// The user declined the authorization. A normal exit for the caller to
    /// return to its own flow, not an error.
    Cancelled,
}

/// Run the Spice.ai browser login inside another command and return the live
/// session, so the command continues without the user re-running anything.
///
/// Behaves like a bare `spice login`: same preamble, same browser flow, same
/// success output — the machine ends up logged in either way. The caller
/// chooses `store` explicitly (a bare `spice login` defaults to
/// [`CredentialStore::EnvFile`]); passing [`CredentialStore::Unpersisted`]
/// fails rather than completing a login whose credential went nowhere. The
/// user declining the authorization is a clean
/// [`LoginContinuation::Cancelled`], not an error.
///
/// # Errors
///
/// Returns an error if the flow fails or times out, or if the credential
/// cannot be persisted.
pub async fn login_inline(store: CredentialStore) -> Result<LoginContinuation> {
    let flow = BrowserLogin::new();
    flow.announce();
    flow.open_browser();
    tracing::info!("Waiting for authentication...");

    let outcome = flow.authenticate().await?;
    continue_login(outcome, store)
}

/// Map a finished browser flow to its continuation: persist-and-hand-back for
/// a grant, a clean cancellation for a decline.
fn continue_login(
    outcome: BrowserLoginOutcome,
    store: CredentialStore,
) -> Result<LoginContinuation> {
    match outcome {
        BrowserLoginOutcome::Declined => Ok(LoginContinuation::Cancelled),
        BrowserLoginOutcome::Granted {
            access_token,
            context,
        } => {
            let session = establish_session(access_token, context, store)?;
            print_login_success(&session);
            Ok(LoginContinuation::Authenticated(Box::new(session)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    const TEST_TIMEOUT: Duration = Duration::from_millis(600);
    const TEST_INTERVAL: Duration = Duration::from_millis(20);

    fn test_session(store: CredentialStore) -> AuthenticatedSession {
        AuthenticatedSession {
            access_token: "tok_super_secret".to_string(),
            username: "jane".to_string(),
            email: "jane@example.com".to_string(),
            org_name: "acme".to_string(),
            app_name: Some("retail-analytics".to_string()),
            credential_store: store,
        }
    }

    /// The session is the type command code passes around, so its `Debug` is
    /// the boundary that keeps the token out of logs and assertion messages.
    #[test]
    fn session_debug_redacts_the_token_and_api_key() {
        let rendered = format!("{:?}", test_session(CredentialStore::EnvFile));

        assert!(
            !rendered.contains("tok_super_secret"),
            "a live credential leaked into Debug output: {rendered}"
        );
        assert!(
            rendered.contains("<redacted>"),
            "the redaction should be visible so a reader knows a value exists: {rendered}"
        );
        assert!(
            rendered.contains("jane") && rendered.contains("acme"),
            "the identity fields are not secrets and should stay diagnosable: {rendered}"
        );
    }

    #[test]
    fn browser_outcome_debug_redacts_the_token() {
        let outcome = BrowserLoginOutcome::Granted {
            access_token: "tok_super_secret".to_string(),
            context: SpiceAuthContext {
                username: "jane".to_string(),
                email: "jane@example.com".to_string(),
                org_name: "acme".to_string(),
                app_name: None,
                app_api_key: Some("key_super_secret".to_string()),
            },
        };

        let rendered = format!("{outcome:?}");
        assert!(
            !rendered.contains("tok_super_secret") && !rendered.contains("key_super_secret"),
            "a live credential leaked into Debug output: {rendered}"
        );
    }

    #[test]
    fn the_session_reports_where_the_credential_lives() {
        assert_eq!(
            test_session(CredentialStore::EnvFile).credential_store(),
            CredentialStore::EnvFile
        );
        assert_eq!(
            CredentialStore::from_output(&LoginOutput::Env),
            CredentialStore::EnvFile
        );
        assert_eq!(
            CredentialStore::from_output(&LoginOutput::Keychain),
            CredentialStore::Keychain
        );
        assert_eq!(
            CredentialStore::from_output(&LoginOutput::Json),
            CredentialStore::Unpersisted
        );
    }

    /// An unpersisted store must refuse writes rather than silently dropping a
    /// credential the caller believes is saved.
    #[test]
    fn an_unpersisted_store_refuses_to_save() {
        let err = CredentialStore::Unpersisted
            .save("SPICEAI", &[("TOKEN", "tok")])
            .expect_err("an unpersisted store has nowhere to write");
        assert!(
            err.to_string().contains("not persisted"),
            "the error should say why: {err}"
        );
    }

    /// Marks the child half of [`establishing_a_session_persists_the_credential_pair`].
    const ESTABLISH_SESSION_CHILD_FLAG: &str = "SPICE_TEST_ESTABLISH_SESSION_IN_CHILD_CWD";

    /// The session must leave the machine in the same state `spice login`
    /// always has: token and API key merged into the env file, and the handle
    /// reporting where they went.
    ///
    /// The env-file backend writes to the cwd-relative `.env`, and the process
    /// working directory is shared by every test in this binary — so instead
    /// of mutating it, the test re-runs itself as a child process whose cwd
    /// *is* a scratch directory, and the parent asserts what landed on disk.
    #[test]
    fn establishing_a_session_persists_the_credential_pair() {
        if std::env::var_os(ESTABLISH_SESSION_CHILD_FLAG).is_some() {
            // Child: the parent spawned us with a scratch working directory.
            let context = SpiceAuthContext {
                username: "jane".to_string(),
                email: "jane@example.com".to_string(),
                org_name: "acme".to_string(),
                app_name: Some("retail-analytics".to_string()),
                app_api_key: Some("key_live_456".to_string()),
            };

            let session = establish_session(
                "tok_live_123".to_string(),
                context,
                CredentialStore::EnvFile,
            )
            .expect("the env backend should persist");

            assert_eq!(session.credential_store(), CredentialStore::EnvFile);
            assert_eq!(session.username(), "jane");
            assert_eq!(session.org_name(), "acme");
            assert_eq!(session.access_token(), "tok_live_123");

            let context_without_api_key = SpiceAuthContext {
                username: "jane".to_string(),
                email: "jane@example.com".to_string(),
                org_name: "acme".to_string(),
                app_name: Some("retail-analytics".to_string()),
                app_api_key: None,
            };
            establish_session(
                "tok_refreshed_789".to_string(),
                context_without_api_key,
                CredentialStore::EnvFile,
            )
            .expect("a missing API key should not prevent token persistence");
            return;
        }

        let scratch = tempfile::TempDir::new().expect("scratch dir should be creatable");
        let exe = std::env::current_exe().expect("test binary path should resolve");
        let output = std::process::Command::new(exe)
            .args([
                "commands::login::session::tests::establishing_a_session_persists_the_credential_pair",
                "--exact",
                "--test-threads=1",
            ])
            .env(ESTABLISH_SESSION_CHILD_FLAG, "1")
            .current_dir(scratch.path())
            .output()
            .expect("child test process should run");
        assert!(
            output.status.success(),
            "the child assertions failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        let env = std::fs::read_to_string(scratch.path().join(".env"))
            .expect(".env should be written in the child's working directory");
        assert!(
            env.contains("SPICE_SPICEAI_TOKEN=tok_refreshed_789")
                && env.contains("SPICE_SPICEAI_API_KEY="),
            "a login without an API key must clear the stored key: {env}"
        );
        assert!(
            !env.contains("SPICE_SPICEAI_API_KEY=key_live_456"),
            "an API key from the previous login must not remain active: {env}"
        );
    }

    /// The user declining in the browser ends an inline login cleanly: the
    /// caller gets `Cancelled` back and returns to its own flow.
    #[test]
    fn a_declined_authorization_continues_as_cancelled() {
        let continuation = continue_login(BrowserLoginOutcome::Declined, CredentialStore::EnvFile)
            .expect("a decline is not an error");
        assert!(
            matches!(continuation, LoginContinuation::Cancelled),
            "a decline must map to Cancelled, got: {continuation:?}"
        );
    }

    /// A session must not come into existence with its credential unpersisted:
    /// establishing against the unpersisted store is an error, not a print.
    #[test]
    fn establishing_a_session_refuses_an_unpersisted_store() {
        let context = SpiceAuthContext {
            username: "jane".to_string(),
            email: "jane@example.com".to_string(),
            org_name: "acme".to_string(),
            app_name: None,
            app_api_key: None,
        };

        let err = establish_session(
            "tok_live_123".to_string(),
            context,
            CredentialStore::Unpersisted,
        )
        .expect_err("an unpersisted store cannot back a session");
        assert!(
            err.to_string().contains("not persisted"),
            "the error should say why: {err}"
        );
    }

    async fn flow_against(
        server: &wiremock::MockServer,
    ) -> crate::error::Result<BrowserLoginOutcome> {
        BrowserLogin::at(server.uri())
            .authenticate_within(TEST_TIMEOUT, TEST_INTERVAL)
            .await
    }

    async fn mount_exchange(server: &wiremock::MockServer, body: serde_json::Value) {
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/auth/token/exchange"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(body))
            .mount(server)
            .await;
    }

    /// The full mock-server flow: the exchange grants a token, the auth
    /// context answers who it belongs to, and the outcome carries both.
    #[tokio::test]
    async fn a_granted_authorization_yields_the_token_and_identity() {
        let server = wiremock::MockServer::start().await;
        mount_exchange(
            &server,
            serde_json::json!({ "access_token": "tok_live_123" }),
        )
        .await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/api/spice-cli/auth"))
            .and(wiremock::matchers::header(
                "Authorization",
                "Bearer tok_live_123",
            ))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "username": "jane",
                    "email": "jane@example.com",
                    "org": { "name": "acme" },
                    "app": { "name": "retail-analytics", "api_key": "key_123" },
                })),
            )
            .mount(&server)
            .await;

        let outcome = flow_against(&server)
            .await
            .expect("a granted flow should succeed");

        let BrowserLoginOutcome::Granted {
            access_token,
            context,
        } = outcome
        else {
            panic!("expected a granted outcome");
        };
        assert_eq!(access_token, "tok_live_123");
        assert_eq!(context.username, "jane");
        assert_eq!(context.org_name, "acme");
        assert_eq!(context.app_name.as_deref(), Some("retail-analytics"));
        assert_eq!(context.app_api_key.as_deref(), Some("key_123"));
    }

    /// A refusal in the browser is an outcome, not an error — the standalone
    /// command turns it into "Access denied", the inline one into `Cancelled`.
    #[tokio::test]
    async fn a_denied_authorization_is_a_declined_outcome() {
        let server = wiremock::MockServer::start().await;
        mount_exchange(&server, serde_json::json!({ "access_denied": true })).await;

        let outcome = flow_against(&server)
            .await
            .expect("a denial is a decided outcome, not an error");
        assert!(matches!(outcome, BrowserLoginOutcome::Declined));
    }

    /// The token is only as good as the identity behind it: a granted exchange
    /// whose auth context cannot be fetched is a failed login.
    #[tokio::test]
    async fn a_failed_auth_context_fails_the_flow() {
        let server = wiremock::MockServer::start().await;
        mount_exchange(&server, serde_json::json!({ "access_token": "tok_live" })).await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/api/spice-cli/auth"))
            .respond_with(wiremock::ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let err = flow_against(&server)
            .await
            .expect_err("an unfetchable auth context should fail the login");
        assert!(
            err.to_string().contains("Auth context request failed"),
            "unexpected error: {err}"
        );
    }
}
