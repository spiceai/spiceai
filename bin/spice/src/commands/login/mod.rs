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

//! Login command and subcommands for authenticating with various data sources.

mod auth_config;
mod providers;

use crate::context::RuntimeContext;
use crate::error::Result;
use crate::manifest;
use clap::{Args, Subcommand};

pub use auth_config::{merge_auth_config, store_keychain};

/// Credential storage backend for `spice login`.
#[derive(Debug, Clone, Default, clap::ValueEnum)]
pub enum LoginOutput {
    /// Write credentials to .env file (default)
    #[default]
    Env,
    /// Print credentials as JSON to stdout
    Json,
    /// Store credentials in the platform keychain
    Keychain,
}

impl PartialEq for LoginOutput {
    fn eq(&self, other: &Self) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(other)
    }
}

/// Arguments for the login command.
#[derive(Args, Debug)]
#[command(
    about = "Authenticate with Spice.ai or configure data-source credentials",
    long_about = r#"Authenticate with Spice.ai or store credentials for a specific data source.

With no subcommand, performs Spice.ai login (browser flow unless `--key` is
provided). With a provider subcommand, walks through the credentials needed by
that connector and stores them in the configured backend.

OUTPUT BACKENDS (via `--output`)
  env       Append to a local `.env` file (default)
  json      Print credentials as JSON to stdout
  keychain  Store credentials in the platform keychain (macOS Keychain, etc.)

PROVIDERS
  dremio, s3, postgres, snowflake, databricks, delta-lake, spark, sharepoint, abfs

EXAMPLES
  spice login                              # Spice.ai browser login
  spice login --key sk_live_...            # Spice.ai login with an existing API key
  spice login s3                           # Configure S3 credentials
  spice login postgres -o keychain         # Store Postgres creds in the keychain
  spice login databricks -o json | jq      # Print Databricks creds as JSON

Docs: https://spiceai.org/docs"#
)]
pub struct LoginArgs {
    /// API key for direct authentication (skips the OAuth/browser flow).
    #[arg(short = 'k', long)]
    pub key: Option<String>,

    /// Where to store the resulting credentials.
    #[arg(long, short = 'o', default_value = "env")]
    pub output: LoginOutput,

    #[command(subcommand)]
    pub command: Option<LoginCommands>,
}

/// Login subcommands for different providers.
#[derive(Subcommand, Debug)]
pub enum LoginCommands {
    /// Login to a Dremio instance
    Dremio(providers::DremioArgs),

    /// Login to S3 storage
    S3(providers::S3Args),

    /// Login to a Postgres instance
    Postgres(providers::PostgresArgs),

    /// Login to a Snowflake warehouse
    Snowflake(providers::SnowflakeArgs),

    /// Login to a Databricks instance
    Databricks(providers::DatabricksArgs),

    /// Configure credentials to access a Delta Lake table
    DeltaLake(providers::DeltaLakeArgs),

    /// Login to a Spark Connect remote
    Spark(providers::SparkArgs),

    /// Login to a Microsoft 365 `SharePoint` account
    Sharepoint(providers::SharePointArgs),

    /// Login to an Azure Blob Storage (ABFS) account
    Abfs(providers::AbfsArgs),
}

/// Execute the login command.
///
/// # Errors
///
/// Returns an error if authentication fails.
pub async fn execute(ctx: &RuntimeContext, args: LoginArgs) -> Result<()> {
    match args.command {
        Some(LoginCommands::Dremio(provider_args)) => {
            providers::login_dremio(ctx, provider_args).await
        }
        Some(LoginCommands::S3(provider_args)) => providers::login_s3(ctx, provider_args).await,
        Some(LoginCommands::Postgres(provider_args)) => {
            providers::login_postgres(ctx, provider_args).await
        }
        Some(LoginCommands::Snowflake(provider_args)) => {
            providers::login_snowflake(ctx, provider_args).await
        }
        Some(LoginCommands::Databricks(provider_args)) => {
            providers::login_databricks(ctx, provider_args).await
        }
        Some(LoginCommands::DeltaLake(provider_args)) => {
            providers::login_delta_lake(ctx, provider_args).await
        }
        Some(LoginCommands::Spark(provider_args)) => {
            providers::login_spark(ctx, provider_args).await
        }
        Some(LoginCommands::Sharepoint(provider_args)) => {
            providers::login_sharepoint(ctx, provider_args).await
        }
        Some(LoginCommands::Abfs(provider_args)) => providers::login_abfs(ctx, provider_args).await,
        None => {
            // Main Spice.ai login with OAuth flow
            login_spiceai(ctx, args.key, args.output).await
        }
    }
}

/// Save credentials using the specified output backend.
fn save_credentials(output: &LoginOutput, auth_type: &str, params: &[(&str, &str)]) -> Result<()> {
    match output {
        LoginOutput::Env => merge_auth_config(auth_type, params),
        LoginOutput::Json => {
            let mut map = serde_json::Map::new();
            for (key, value) in params {
                let secret_key = format!("SPICE_{auth_type}_{key}");
                map.insert(secret_key, serde_json::Value::String((*value).to_string()));
            }
            println!(
                "{}",
                serde_json::to_string(&serde_json::Value::Object(map)).unwrap_or_default()
            );
            Ok(())
        }
        LoginOutput::Keychain => store_keychain(auth_type, params),
    }
}

/// Login to Spice.ai using OAuth flow or direct API key.
async fn login_spiceai(
    _ctx: &RuntimeContext,
    api_key: Option<String>,
    output: LoginOutput,
) -> Result<()> {
    let is_json = output == LoginOutput::Json;

    if let Some(key) = api_key {
        // Direct API key authentication
        save_credentials(&output, "SPICEAI", &[("API_KEY", key.as_str())])?;
        if !is_json {
            println!("\x1b[32mSuccessfully logged in to Spice.ai with API key\x1b[0m");
        }
        return Ok(());
    }

    // Spice.ai OAuth flow
    let base_url = get_spice_base_url();
    let auth_code = generate_auth_code();

    let auth_url = format!("{base_url}/auth/token?code={auth_code}");

    if !is_json {
        println!("Attempting to open Spice.ai authorization page in your default browser");
        println!("\nYour auth code:\n");
        println!("{}-{}", &auth_code[..4], &auth_code[4..]);
        println!("\nIf the browser does not open, visit the following URL manually:");
        println!("\n{auth_url}\n");
    }

    // Fire-and-forget: open in spawn_blocking so Command::status does not
    // block a Tokio worker or delay the OAuth poll loop.
    let auth_url_for_open = auth_url.clone();
    tokio::task::spawn_blocking(move || {
        let _ = system_open::that(auth_url_for_open);
    });

    tracing::info!("Waiting for authentication...");

    // Poll for auth status
    let client = reqwest::Client::builder()
        .user_agent(format!(
            "spice/{} ({}; {})",
            env!("CARGO_PKG_VERSION"),
            std::env::consts::OS,
            std::env::consts::ARCH
        ))
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .unwrap_or_default();
    let exchange_url = format!("{base_url}/auth/token/exchange");

    let access_token = poll_for_access_token(
        &client,
        &exchange_url,
        &auth_code,
        LOGIN_POLL_TIMEOUT,
        LOGIN_POLL_INTERVAL,
    )
    .await?;

    // Try to read the Spicepod manifest for preferred org/app.
    let (org_name, app_name) = read_spicepod_metadata();

    // Get auth context
    let auth_context = get_spice_auth_context(
        &base_url,
        &access_token,
        org_name.as_deref(),
        app_name.as_deref(),
    )
    .await?;

    let api_key_value = auth_context.app_api_key.unwrap_or_default();

    // Save credentials
    if is_json {
        // In JSON mode, include auth context fields alongside credentials
        let json = serde_json::json!({
            "SPICE_SPICEAI_TOKEN": &access_token,
            "SPICE_SPICEAI_API_KEY": &api_key_value,
            "username": &auth_context.username,
            "org": &auth_context.org_name,
            "app": auth_context.app_name.as_deref().unwrap_or_default(),
        });
        println!("{}", serde_json::to_string(&json).unwrap_or_default());
    } else {
        save_credentials(
            &output,
            "SPICEAI",
            &[("TOKEN", &access_token), ("API_KEY", &api_key_value)],
        )?;

        println!(
            "\x1b[32mSuccessfully logged in to Spice.ai as {} ({})\x1b[0m",
            auth_context.username, auth_context.email
        );
        println!(
            "\x1b[32mUsing app {}/{}\x1b[0m",
            auth_context.org_name,
            auth_context.app_name.unwrap_or_default()
        );
    }

    Ok(())
}

/// How long the Spice.ai browser flow waits for the user before giving up.
///
/// Matches the Spice Cloud device flow in [`crate::commands::cloud`]; the
/// Microsoft device-code flow in [`providers`] bounds itself the same way, off
/// the `expires_in` its authorization server returns.
const LOGIN_POLL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

/// How long to wait between token-exchange attempts.
const LOGIN_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

/// What one token-exchange attempt tells the poll loop to do next.
#[derive(PartialEq, Eq)]
enum ExchangeOutcome {
    /// The exchange carried a usable access token.
    Token(String),
    /// The authorization was refused. Waiting cannot change that.
    Denied,
    /// The user has not finished authorizing in the browser yet.
    Pending,
    /// Re-sending this identical request cannot succeed.
    Permanent(String),
    /// A blip that repeating may clear, until the deadline runs out.
    Transient(String),
}

/// Redacts the token. This type carries a live credential, so it must not be
/// the reason one reaches a log line or an assertion message.
impl std::fmt::Debug for ExchangeOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Token(_) => f.write_str("Token(<redacted>)"),
            Self::Denied => f.write_str("Denied"),
            Self::Pending => f.write_str("Pending"),
            Self::Permanent(message) => write!(f, "Permanent({message})"),
            Self::Transient(message) => write!(f, "Transient({message})"),
        }
    }
}

/// Classify the HTTP status of a token-exchange attempt.
///
/// Returns `None` for a success status, meaning the body decides the outcome.
fn classify_exchange_status(status: reqwest::StatusCode) -> Option<ExchangeOutcome> {
    if status.is_success() {
        return None;
    }

    // A server that is briefly unhappy, or is asking us to back off, can still
    // become happy while the user is authorizing.
    if status.is_server_error()
        || status == reqwest::StatusCode::REQUEST_TIMEOUT
        || status == reqwest::StatusCode::TOO_MANY_REQUESTS
    {
        return Some(ExchangeOutcome::Transient(format!(
            "the exchange endpoint returned {status}"
        )));
    }

    // Anything else is the endpoint rejecting this exact request: a 4xx, or a
    // redirect the client declines to follow because it leaves the origin the
    // credential was minted for. Neither is cleared by sending it again.
    Some(ExchangeOutcome::Permanent(format!(
        "the exchange endpoint returned {status}"
    )))
}

/// Classify the body of a successful token-exchange response.
fn classify_exchange_body(body: &serde_json::Value) -> ExchangeOutcome {
    if body["access_denied"].as_bool().unwrap_or(false) {
        return ExchangeOutcome::Denied;
    }

    match body["access_token"].as_str() {
        Some(token) if !token.is_empty() => ExchangeOutcome::Token(token.to_string()),
        // No token yet: the user has not finished in the browser.
        _ => ExchangeOutcome::Pending,
    }
}

/// Poll the Spice.ai token exchange until it yields a token, refuses, or
/// `timeout` elapses.
///
/// # Errors
///
/// Returns an error if the authorization is refused, if the endpoint reports a
/// failure that repeating cannot clear, or if `timeout` elapses first.
async fn poll_for_access_token(
    client: &reqwest::Client,
    exchange_url: &str,
    auth_code: &str,
    timeout: std::time::Duration,
    interval: std::time::Duration,
) -> Result<String> {
    let start = std::time::Instant::now();
    // Kept so an endpoint that only ever fails reports *why* it timed out,
    // rather than logging the same line once a second for the whole deadline.
    let mut last_transient: Option<String> = None;

    loop {
        if start.elapsed() > timeout {
            let detail = last_transient
                .take()
                .map(|message| format!(" Last error: {message}."))
                .unwrap_or_default();
            return Err(crate::error::Error::InvalidArgument {
                message: format!("Authentication timed out. Please try again.{detail}"),
            });
        }

        tokio::time::sleep(interval).await;

        let response = client
            .post(exchange_url)
            .header("Content-Type", "application/json")
            .json(&serde_json::json!({ "code": auth_code }))
            .send()
            .await;

        let outcome = match response {
            Ok(response) => {
                // Read the status out before the body: `json()` consumes the
                // response, so it cannot borrow it in a match scrutinee.
                let status = response.status();
                match classify_exchange_status(status) {
                    Some(outcome) => outcome,
                    None => match response.json::<serde_json::Value>().await {
                        Ok(body) => classify_exchange_body(&body),
                        // A success status carrying a body we cannot read is
                        // more likely an interposed proxy than a decided
                        // answer, so it stays retriable and the deadline
                        // bounds it.
                        Err(e) => ExchangeOutcome::Transient(format!(
                            "could not parse the exchange response: {e}"
                        )),
                    },
                }
            }
            Err(e) => ExchangeOutcome::Transient(format!("could not reach the exchange: {e}")),
        };

        match outcome {
            ExchangeOutcome::Token(token) => return Ok(token),
            ExchangeOutcome::Denied => {
                return Err(crate::error::Error::InvalidArgument {
                    message: "Access denied".to_string(),
                });
            }
            ExchangeOutcome::Permanent(message) => {
                return Err(crate::error::Error::InvalidArgument {
                    message: format!("Authentication failed: {message}."),
                });
            }
            ExchangeOutcome::Transient(message) => {
                tracing::debug!("Retrying the spice.ai token exchange: {message}");
                last_transient = Some(message);
            }
            // A healthy "not yet" supersedes an earlier blip: if the deadline
            // runs out from here, it is the user who did not finish.
            ExchangeOutcome::Pending => last_transient = None,
        }
    }
}

/// Get the Spice.ai base URL.
fn get_spice_base_url() -> String {
    if let Ok(url) = std::env::var("SPICE_BASE_URL") {
        return url;
    }

    let version = env!("CARGO_PKG_VERSION");
    if version.ends_with("-dev") {
        "https://dev.spice.ai".to_string()
    } else {
        "https://spice.ai".to_string()
    }
}

/// Generate a random 8-character auth code.
fn generate_auth_code() -> String {
    use rand::RngExt;
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = rand::rng();

    (0..8)
        .map(|_| {
            let idx = rng.random_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}

/// Read org and app name from spicepod.yaml or spicepod.yml if it exists.
fn read_spicepod_metadata() -> (Option<String>, Option<String>) {
    let Some(spicepod_path) = manifest::existing_spicepod_path(std::path::Path::new(".")) else {
        return (None, None);
    };

    let Ok(contents) = std::fs::read_to_string(spicepod_path) else {
        return (None, None);
    };

    let Ok(yaml) = yaml::from_str::<yaml::Value>(&contents) else {
        return (None, None);
    };

    let org_name = yaml
        .get("metadata")
        .and_then(|m| m.get("org"))
        .and_then(|o| o.as_str())
        .map(String::from);

    let app_name = yaml.get("name").and_then(|n| n.as_str()).map(String::from);

    (org_name, app_name)
}

/// Auth context from Spice.ai API.
#[derive(Debug, serde::Deserialize)]
struct SpiceAuthContext {
    username: String,
    email: String,
    org_name: String,
    app_name: Option<String>,
    app_api_key: Option<String>,
}

/// Get auth context from Spice.ai API.
async fn get_spice_auth_context(
    base_url: &str,
    access_token: &str,
    org_name: Option<&str>,
    app_name: Option<&str>,
) -> Result<SpiceAuthContext> {
    let mut url = format!("{base_url}/api/spice-cli/auth");

    let mut params = Vec::new();
    if let Some(org) = org_name {
        params.push(format!("org_name={}", urlencoding::encode(org)));
    }
    if let Some(app) = app_name {
        params.push(format!("app_name={}", urlencoding::encode(app)));
    }
    if !params.is_empty() {
        url = format!("{url}?{}", params.join("&"));
    }

    let client = reqwest::Client::builder()
        .user_agent(format!(
            "spice/{} ({}; {})",
            env!("CARGO_PKG_VERSION"),
            std::env::consts::OS,
            std::env::consts::ARCH
        ))
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .unwrap_or_default();
    let response = client
        .get(&url)
        .header("Authorization", format!("Bearer {access_token}"))
        .send()
        .await
        .map_err(|e| crate::error::Error::InvalidResponse {
            message: format!("Failed to get auth context: {e}"),
        })?;

    if !response.status().is_success() {
        let text = response.text().await.unwrap_or_default();
        return Err(crate::error::Error::InvalidResponse {
            message: format!("Auth context request failed: {text}"),
        });
    }

    // Parse the response - API returns nested org/app objects
    let body: serde_json::Value =
        response
            .json()
            .await
            .map_err(|e| crate::error::Error::InvalidResponse {
                message: format!("Failed to parse auth context: {e}"),
            })?;

    Ok(SpiceAuthContext {
        username: body["username"].as_str().unwrap_or_default().to_string(),
        email: body["email"].as_str().unwrap_or_default().to_string(),
        org_name: body["org"]["name"].as_str().unwrap_or_default().to_string(),
        app_name: body["app"]["name"].as_str().map(String::from),
        app_api_key: body["app"]["api_key"].as_str().map(String::from),
    })
}

#[cfg(test)]
mod tests {
    use super::{
        ExchangeOutcome, classify_exchange_body, classify_exchange_status, poll_for_access_token,
    };
    use std::time::Duration;

    /// Long enough that a bounded loop still polls several times, short enough
    /// that an unbounded one is unmistakable.
    const TEST_TIMEOUT_MS: u64 = 600;
    const TEST_TIMEOUT: Duration = Duration::from_millis(TEST_TIMEOUT_MS);
    const TEST_INTERVAL: Duration = Duration::from_millis(20);

    fn status(code: u16) -> reqwest::StatusCode {
        reqwest::StatusCode::from_u16(code).expect("code should be a valid HTTP status")
    }

    async fn exchange_against(
        template: wiremock::ResponseTemplate,
    ) -> (crate::error::Result<String>, u64) {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .respond_with(template)
            .mount(&server)
            .await;

        let url = format!("{}/auth/token/exchange", server.uri());
        let client = reqwest::Client::new();
        let started = std::time::Instant::now();
        let result =
            poll_for_access_token(&client, &url, "ABCD1234", TEST_TIMEOUT, TEST_INTERVAL).await;

        let elapsed = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        (result, elapsed)
    }

    #[test]
    fn a_success_status_defers_to_the_body() {
        assert_eq!(classify_exchange_status(status(200)), None);
        assert_eq!(classify_exchange_status(status(204)), None);
    }

    #[test]
    fn a_server_error_or_backoff_status_is_retriable() {
        for code in [408, 429, 500, 502, 503, 504] {
            assert!(
                matches!(
                    classify_exchange_status(status(code)),
                    Some(ExchangeOutcome::Transient(_))
                ),
                "{code} should be retriable"
            );
        }
    }

    #[test]
    fn a_rejecting_status_is_permanent() {
        // 302 is the cross-origin redirect the client declines to follow; the
        // 4xx codes are the endpoint rejecting this exact request.
        for code in [301, 302, 307, 400, 401, 403, 404, 410] {
            assert!(
                matches!(
                    classify_exchange_status(status(code)),
                    Some(ExchangeOutcome::Permanent(_))
                ),
                "{code} should be permanent"
            );
        }
    }

    #[test]
    fn a_body_carrying_a_token_succeeds() {
        let body = serde_json::json!({ "access_token": "tok_123" });
        assert_eq!(
            classify_exchange_body(&body),
            ExchangeOutcome::Token("tok_123".to_string())
        );
    }

    #[test]
    fn an_access_denied_body_is_denied() {
        let body = serde_json::json!({ "access_denied": true });
        assert_eq!(classify_exchange_body(&body), ExchangeOutcome::Denied);
    }

    #[test]
    fn a_body_without_a_usable_token_is_still_pending() {
        for body in [
            serde_json::json!({}),
            serde_json::json!({ "access_token": "" }),
            serde_json::json!({ "access_denied": false }),
            serde_json::json!({ "access_token": null }),
        ] {
            assert_eq!(
                classify_exchange_body(&body),
                ExchangeOutcome::Pending,
                "{body} should keep polling"
            );
        }
    }

    /// Regression test for #12506: before the deadline this spun at one request
    /// per second forever, because a body with no token is indistinguishable
    /// from "the user has not finished yet".
    #[tokio::test]
    async fn a_perpetually_pending_exchange_gives_up_at_the_deadline() {
        let template = wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({}));
        let (result, elapsed) = exchange_against(template).await;

        let Err(err) = result else {
            panic!("a never-completing exchange should not succeed");
        };
        assert!(
            err.to_string().contains("Authentication timed out"),
            "expected a timeout error, got: {err}"
        );
        assert!(
            elapsed < TEST_TIMEOUT_MS * 10,
            "the loop should stop near its deadline, took {elapsed}ms"
        );
    }

    /// Regression test for #12506: a wrong `SPICE_BASE_URL` answers 404 to every
    /// attempt, which the old loop retried forever.
    #[tokio::test]
    async fn a_rejecting_endpoint_fails_without_waiting_out_the_deadline() {
        let (result, elapsed) = exchange_against(wiremock::ResponseTemplate::new(404)).await;

        let Err(err) = result else {
            panic!("a 404 exchange endpoint should not succeed");
        };
        assert!(
            err.to_string().contains("404"),
            "the error should name the status, got: {err}"
        );
        assert!(
            elapsed < TEST_TIMEOUT_MS,
            "a permanent failure should not wait out the deadline, took {elapsed}ms"
        );
    }

    /// A 5xx stays retriable, so it is the deadline — not the first bad
    /// response — that ends the loop, and the reason survives into the error.
    #[tokio::test]
    async fn a_failing_but_retriable_endpoint_reports_why_it_timed_out() {
        let (result, _) = exchange_against(wiremock::ResponseTemplate::new(503)).await;

        let Err(err) = result else {
            panic!("a 503 exchange endpoint should not succeed");
        };
        let message = err.to_string();
        assert!(
            message.contains("Authentication timed out"),
            "expected a timeout error, got: {message}"
        );
        assert!(
            message.contains("503"),
            "the timeout should carry the last error, got: {message}"
        );
    }

    #[tokio::test]
    async fn a_token_response_completes_the_flow() {
        let template = wiremock::ResponseTemplate::new(200)
            .set_body_json(serde_json::json!({ "access_token": "tok_abc" }));
        let (result, _) = exchange_against(template).await;

        assert_eq!(
            result.expect("a token response should succeed"),
            "tok_abc".to_string()
        );
    }

    #[tokio::test]
    async fn an_access_denied_response_stops_immediately() {
        let template = wiremock::ResponseTemplate::new(200)
            .set_body_json(serde_json::json!({ "access_denied": true }));
        let (result, elapsed) = exchange_against(template).await;

        let Err(err) = result else {
            panic!("an access_denied response should not succeed");
        };
        assert!(
            err.to_string().contains("Access denied"),
            "expected an access-denied error, got: {err}"
        );
        assert!(
            elapsed < TEST_TIMEOUT_MS,
            "a denial should not wait out the deadline, took {elapsed}ms"
        );
    }
}
