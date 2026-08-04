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
use spice_cloud_client::redirect::same_origin_redirect_policy;

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

    // Poll for auth status. The exchange below posts the auth code in the request body, so
    // this client must not follow a redirect off the origin.
    let client = credentialed_client()?;
    let exchange_url = format!("{base_url}/auth/token/exchange");

    let access_token = loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let response = client
            .post(&exchange_url)
            .header("Content-Type", "application/json")
            .json(&serde_json::json!({ "code": auth_code }))
            .send()
            .await;

        let response = match response {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("Error exchanging auth code with spice.ai: {e}");
                continue;
            }
        };

        // A 3xx here means the redirect policy refused to follow it, so this is the
        // redirecting origin's own response, not the target's. Fail instead of parsing it:
        // retrying cannot make a redirect succeed, and an unchecked body could otherwise be
        // read as a token or as "still pending".
        let status = response.status();
        if status.is_redirection() {
            return Err(crate::error::Error::InvalidResponse {
                message: format!(
                    "The Spice.ai token exchange answered {status}. The redirect was not \
                     followed because it leaves the origin the auth code was issued for."
                ),
            });
        }

        let body: serde_json::Value = match response.json().await {
            Ok(v) => v,
            Err(e) => {
                tracing::error!("Error parsing exchange response: {e}");
                continue;
            }
        };

        if body["access_denied"].as_bool().unwrap_or(false) {
            return Err(crate::error::Error::InvalidArgument {
                message: "Access denied".to_string(),
            });
        }

        if let Some(token) = body["access_token"].as_str()
            && !token.is_empty()
        {
            break token.to_string();
        }
    };

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

/// Build the HTTP client for the credential-bearing calls `spice login` makes.
///
/// The redirect policy is the reason this is shared rather than built per call site. These
/// requests carry an auth code, a device code or a bearer token, and `reqwest`'s default
/// policy follows a `Location` up to ten hops. Stripping headers is not enough on its own:
/// `Authorization` is dropped on a cross-origin hop, but a 307 or 308 replays the request
/// *body* — which is exactly where the Spice.ai token exchange and the Microsoft
/// device-code flow carry their credential — so the hop itself has to be refused.
///
/// # Errors
///
/// Returns an error if the client cannot be built. This is deliberately not defaulted
/// past: a default client would silently drop the same-origin policy.
fn credentialed_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent(format!(
            "spice/{} ({}; {})",
            env!("CARGO_PKG_VERSION"),
            std::env::consts::OS,
            std::env::consts::ARCH
        ))
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(30))
        .redirect(same_origin_redirect_policy())
        .build()
        .map_err(|e| crate::error::Error::InvalidResponse {
            message: format!("Could not build the login HTTP client: {e}"),
        })
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

    let client = credentialed_client()?;
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
    use super::credentialed_client;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::{TcpListener, TcpStream};

    /// How long a request that must not hang is given before the test fails it. Well under
    /// the client's own 30-second timeout, so a regression fails fast instead of stalling.
    const TEST_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

    /// Read the whole request — head *and* body — so the client's write completes before we
    /// reply. Closing a socket with unread request data still buffered can surface as a
    /// reset rather than the response under test, which on Windows is packetisation
    /// dependent and so intermittent.
    fn drain_request(stream: &mut TcpStream) {
        let mut reader = BufReader::new(stream);
        let mut content_length = 0usize;
        let mut line = String::new();

        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
            if line == "\r\n" || line == "\n" {
                break;
            }
            let lowered = line.to_ascii_lowercase();
            if let Some(value) = lowered.strip_prefix("content-length:") {
                content_length = value.trim().parse().unwrap_or(0);
            }
        }

        if content_length > 0 {
            let mut body = vec![0u8; content_length];
            let _ = reader.read_exact(&mut body);
        }
    }

    fn serve_once(listener: &TcpListener, response: &str) {
        let Ok((mut stream, _)) = listener.accept() else {
            return;
        };
        drain_request(&mut stream);
        let _ = stream.write_all(response.as_bytes());
        let _ = stream.flush();
    }

    fn localhost_listener() -> TcpListener {
        TcpListener::bind("127.0.0.1:0")
            .expect("a test listener should bind to an ephemeral port")
    }

    fn local_port(listener: &TcpListener) -> u16 {
        listener
            .local_addr()
            .expect("listener should have a local address")
            .port()
    }

    /// A cross-origin redirect must not be followed, because the request body carries the
    /// auth code and a 307 replays it to the new origin.
    #[tokio::test]
    async fn test_cross_origin_redirect_is_not_followed() {
        let redirector = localhost_listener();
        let elsewhere = localhost_listener();
        let elsewhere_port = local_port(&elsewhere);
        let redirector_port = local_port(&redirector);
        let url = format!("http://127.0.0.1:{redirector_port}/auth/token/exchange");

        // Nothing should ever connect here; poll without blocking after the call returns.
        elsewhere
            .set_nonblocking(true)
            .expect("listener should go non-blocking");

        let response = format!(
            "HTTP/1.1 307 Temporary Redirect\r\n\
             Location: http://127.0.0.1:{elsewhere_port}/collect\r\n\
             Content-Length: 0\r\n\
             Connection: close\r\n\r\n"
        );
        let server = std::thread::spawn(move || serve_once(&redirector, &response));

        let client = credentialed_client().expect("client should build");
        let request = client
            .post(&url)
            .json(&serde_json::json!({ "code": "SECRET12" }))
            .send();
        // On the old default policy the client follows the hop and then waits on a listener
        // that never answers, so without this bound the regression would surface only as a
        // 30-second stall.
        let got = tokio::time::timeout(TEST_REQUEST_TIMEOUT, request)
            .await
            .expect("a refused redirect must return promptly, not hang")
            .expect("the 307 should come back as a response");

        // Stopped at the redirect rather than followed, and the 3xx is still diagnosable.
        assert_eq!(got.status().as_u16(), 307);

        // `WouldBlock` specifically: any other error would mean the listener itself failed,
        // which is not evidence that nothing ever connected to it.
        let contacted = elsewhere.accept();
        let refused_kind = contacted.as_ref().err().map(std::io::Error::kind);
        assert_eq!(
            refused_kind,
            Some(std::io::ErrorKind::WouldBlock),
            "the off-origin listener must never be contacted"
        );

        server.join().expect("server thread should not panic");
    }

    /// The policy must not break a legitimate same-origin redirect.
    #[tokio::test]
    async fn test_same_origin_redirect_is_followed() {
        let listener = localhost_listener();
        let port = local_port(&listener);
        let url = format!("http://127.0.0.1:{port}/auth/token/exchange");

        let redirect = format!(
            "HTTP/1.1 307 Temporary Redirect\r\n\
             Location: http://127.0.0.1:{port}/auth/token/exchange/retry\r\n\
             Content-Length: 0\r\n\
             Connection: close\r\n\r\n"
        );
        let ok = "HTTP/1.1 200 OK\r\n\
                  Content-Type: application/json\r\n\
                  Content-Length: 11\r\n\
                  Connection: close\r\n\r\n\
                  {\"ok\":true}";
        let server = std::thread::spawn(move || {
            serve_once(&listener, &redirect);
            serve_once(&listener, ok);
        });

        let client = credentialed_client().expect("client should build");
        let request = client
            .post(&url)
            .json(&serde_json::json!({ "code": "SECRET12" }))
            .send();
        let got = tokio::time::timeout(TEST_REQUEST_TIMEOUT, request)
            .await
            .expect("the same-origin redirect chain must not hang")
            .expect("a same-origin redirect should be followed");

        assert_eq!(got.status().as_u16(), 200);

        server.join().expect("server thread should not panic");
    }
}
