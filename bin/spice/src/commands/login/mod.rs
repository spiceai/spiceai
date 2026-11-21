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
pub(crate) mod keychain;
mod providers;
pub mod session;

use crate::context::RuntimeContext;
use crate::error::Result;
use clap::{Args, Subcommand};
use spice_cloud_client::redirect::same_origin_redirect_policy;

pub use auth_config::{
    env_file_path, env_file_vars, merge_auth_config, read_env_var, store_keychain,
};

#[cfg(test)]
pub(crate) use auth_config::test_env_file;

/// Credential storage backend for `spice login`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum LoginOutput {
    /// Write credentials to .env file (default)
    #[default]
    Env,
    /// Print credentials as JSON to stdout
    Json,
    /// Store credentials in the platform keychain
    Keychain,
}

/// Arguments for the login command.
#[derive(Args, Debug)]
#[command(
    about = "Authenticate with Spice.ai or configure data-source credentials",
    long_about = r#"Authenticate with Spice.ai or store credentials for a specific data source.

With no subcommand, chooses browser or access-token login for Spice Cloud
unless `--key` supplies a Spice.ai data-plane API key. With a provider
subcommand, walks through the credentials needed by that connector and stores
them in the configured backend.

OUTPUT BACKENDS (via `--output`)
  env       Append to a local `.env` file (default)
  json      Print credentials as JSON to stdout
  keychain  Store credentials in the platform keychain (macOS Keychain, etc.)

PROVIDERS
  dremio, s3, postgres, snowflake, databricks, delta-lake, spark, sharepoint, abfs

EXAMPLES
  spice login                              # Choose browser or access-token login
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

    /// Spice Cloud organization to authenticate for.
    #[arg(long, value_name = "ORG")]
    pub org: Option<String>,

    #[command(subcommand)]
    pub command: Option<LoginCommands>,
}

/// Login subcommands for different providers.
#[derive(Subcommand, Debug)]
pub enum LoginCommands {
    /// Log in to Spice Cloud with a subscription in a browser
    Subscription(crate::commands::cloud::SubscriptionLoginArgs),

    /// Log in to Spice Cloud with an access token
    #[command(alias = "pat")]
    Token(crate::commands::cloud::TokenLoginArgs),

    /// Log in to Spice Cloud with OAuth client credentials
    Api(crate::commands::cloud::ApiLoginArgs),

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
        Some(LoginCommands::Subscription(method)) => {
            crate::commands::cloud::execute_login(
                &crate::commands::cloud::LoginArgs {
                    output: args.output,
                    method: Some(crate::commands::cloud::LoginMethod::Subscription(method)),
                },
                args.org.as_deref(),
            )
            .await
        }
        Some(LoginCommands::Token(method)) => {
            crate::commands::cloud::execute_login(
                &crate::commands::cloud::LoginArgs {
                    output: args.output,
                    method: Some(crate::commands::cloud::LoginMethod::Token(method)),
                },
                args.org.as_deref(),
            )
            .await
        }
        Some(LoginCommands::Api(method)) => {
            crate::commands::cloud::execute_login(
                &crate::commands::cloud::LoginArgs {
                    output: args.output,
                    method: Some(crate::commands::cloud::LoginMethod::Api(method)),
                },
                args.org.as_deref(),
            )
            .await
        }
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
        None if args.key.is_some() => login_spiceai(ctx, args.key, args.output),
        None => {
            crate::commands::cloud::execute_login(
                &crate::commands::cloud::LoginArgs {
                    output: args.output,
                    method: None,
                },
                args.org.as_deref(),
            )
            .await
        }
    }
}

/// Save credentials using the specified output backend.
pub(crate) fn save_credentials(
    output: LoginOutput,
    auth_type: &str,
    params: &[(&str, &str)],
) -> Result<()> {
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

/// Store the data-plane API key used by `spice --cloud`.
fn login_spiceai(
    _ctx: &RuntimeContext,
    api_key: Option<String>,
    output: LoginOutput,
) -> Result<()> {
    let is_json = output == LoginOutput::Json;
    let key = api_key.ok_or_else(|| crate::error::Error::InvalidArgument {
        message: "The data-plane API key was not provided.".to_string(),
    })?;
    save_credentials(output, "SPICEAI", &[("API_KEY", key.as_str())])?;
    if !is_json {
        println!("\x1b[32mSuccessfully logged in to Spice.ai with API key\x1b[0m");
    }
    Ok(())
}

/// Build the HTTP client for the credential-bearing calls `spice login` makes.
///
/// The redirect policy is the reason this is shared rather than built per call site. These
/// requests carry an authorization code, a refresh token, a device code or a bearer token,
/// and `reqwest`'s default policy follows a `Location` up to ten hops. Stripping headers is
/// not enough on its own: `Authorization` is dropped on a cross-origin hop, but a 307 or 308
/// replays the request *body* — which is exactly where the Spice Cloud token endpoint and
/// the Microsoft device-code flow carry their credential — so the hop itself has to be
/// refused.
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
pub(crate) fn spice_base_url() -> String {
    crate::commands::cloud::client::portal_base_url()
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
        TcpListener::bind("127.0.0.1:0").expect("test listener should bind")
    }

    fn local_port(listener: &TcpListener) -> u16 {
        listener
            .local_addr()
            .expect("listener should have a local address")
            .port()
    }

    /// Provider regression guard: the SharePoint/ABFS OAuth device-code flows
    /// are dispatched to the provider module untouched by the session refactor.
    /// A grammar change that rerouted them through the Spice.ai browser flow
    /// would change their vocabulary and credential handling.
    #[test]
    fn provider_subcommands_still_route_to_the_device_code_flows() {
        use clap::Parser;

        #[derive(clap::Parser)]
        struct TestCli {
            #[command(subcommand)]
            command: TestCommand,
        }

        #[derive(clap::Subcommand)]
        enum TestCommand {
            Login(super::LoginArgs),
        }

        for provider in ["sharepoint", "abfs"] {
            let cli = TestCli::try_parse_from([
                "spice",
                "login",
                provider,
                "-t",
                "tenant123",
                "-c",
                "client456",
            ])
            .unwrap_or_else(|e| panic!("`spice login {provider}` should still parse: {e}"));

            let TestCommand::Login(args) = cli.command;
            match (provider, args.command) {
                ("sharepoint", Some(super::LoginCommands::Sharepoint(provider_args))) => {
                    assert_eq!(provider_args.tenant_id, "tenant123");
                    assert_eq!(provider_args.client_id, "client456");
                }
                ("abfs", Some(super::LoginCommands::Abfs(provider_args))) => {
                    assert_eq!(provider_args.tenant_id, "tenant123");
                    assert_eq!(provider_args.client_id, "client456");
                }
                (provider, other) => panic!(
                    "`spice login {provider}` no longer routes to its provider flow: {other:?}"
                ),
            }
        }
    }

    /// A cross-origin redirect must not be followed, because the request body carries the
    /// authorization code and a 307 replays it to the new origin.
    #[tokio::test]
    async fn test_cross_origin_redirect_is_not_followed() {
        let redirector = localhost_listener();
        let elsewhere = localhost_listener();
        let elsewhere_port = local_port(&elsewhere);
        let redirector_port = local_port(&redirector);
        let url = format!("http://127.0.0.1:{redirector_port}/api/oauth/token");

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
        let url = format!("http://127.0.0.1:{port}/api/oauth/token");

        let redirect = format!(
            "HTTP/1.1 307 Temporary Redirect\r\n\
             Location: http://127.0.0.1:{port}/api/oauth/token/retry\r\n\
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
