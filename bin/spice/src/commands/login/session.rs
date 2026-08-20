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

//! Bounded Spice Cloud browser authorization shared by `spice login` and
//! `spice cloud login`.

use crate::error::Result;

use super::SpiceAuthContext;

/// How the browser authorization ended: with a token, or with the user
/// refusing it.
pub(crate) enum BrowserLoginOutcome {
    /// The user authorized the login.
    Granted {
        access_token: String,
        context: SpiceAuthContext,
    },
    /// The user refused the authorization in the browser.
    Declined,
}

/// Redacts the token so diagnostics never expose a live credential.
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
pub(crate) struct BrowserLogin {
    base_url: String,
    auth_code: String,
}

impl BrowserLogin {
    /// A flow against an explicit base URL. Tests point this at a mock server;
    /// production uses the resolved portal base URL.
    pub(crate) fn at(base_url: String) -> Self {
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
    pub(crate) fn announce(&self) {
        println!("Attempting to open Spice.ai authorization page in your default browser");
        println!("\nYour auth code:\n");
        println!("{}-{}", &self.auth_code[..4], &self.auth_code[4..]);
        println!("\nIf the browser does not open, visit the following URL manually:");
        println!("\n{}\n", self.auth_url());
    }

    /// Open the authorization page in the default browser, fire-and-forget.
    pub(crate) fn open_browser(&self) {
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
    pub(crate) async fn authenticate(&self) -> Result<BrowserLoginOutcome> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    const TEST_TIMEOUT: Duration = Duration::from_millis(600);
    const TEST_INTERVAL: Duration = Duration::from_millis(20);

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

    /// A refusal in the browser is a decided outcome rather than a retryable
    /// exchange failure.
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
