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
use clap::{Args, Subcommand};

pub use auth_config::merge_auth_config;

/// Arguments for the login command.
#[derive(Args, Debug)]
pub struct LoginArgs {
    /// API key for direct authentication (bypasses OAuth flow)
    #[arg(short = 'k', long)]
    pub key: Option<String>,

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
            login_spiceai(ctx, args.key).await
        }
    }
}

/// Login to Spice.ai using OAuth flow or direct API key.
async fn login_spiceai(_ctx: &RuntimeContext, api_key: Option<String>) -> Result<()> {
    if let Some(key) = api_key {
        // Direct API key authentication
        merge_auth_config("SPICEAI", &[("API_KEY", key.as_str())])?;
        println!("\x1b[32mSuccessfully logged in to Spice.ai with API key\x1b[0m");
        return Ok(());
    }

    // Spice.ai OAuth flow
    let base_url = get_spice_base_url();
    let auth_code = generate_auth_code();

    let auth_url = format!("{base_url}/auth/token?code={auth_code}");

    println!("Attempting to open Spice.ai authorization page in your default browser");
    println!("\nYour auth code:\n");
    println!("{}-{}", &auth_code[..4], &auth_code[4..]);
    println!("\nIf the browser does not open, visit the following URL manually:");
    println!("\n{auth_url}\n");

    // Try to open browser automatically
    let _ = open::that(&auth_url);

    tracing::info!("Waiting for authentication...");

    // Poll for auth status
    let client = reqwest::Client::new();
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

    // Try to read spicepod.yaml for preferred org/app
    let (org_name, app_name) = read_spicepod_metadata();

    // Get auth context
    let auth_context = get_spice_auth_context(
        &base_url,
        &access_token,
        org_name.as_deref(),
        app_name.as_deref(),
    )
    .await?;

    // Save credentials
    merge_auth_config(
        "SPICEAI",
        &[
            ("TOKEN", &access_token),
            ("API_KEY", &auth_context.app_api_key.unwrap_or_default()),
        ],
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

    Ok(())
}

/// Get the Spice.ai base URL.
fn get_spice_base_url() -> String {
    if let Ok(url) = std::env::var("SPICE_BASE_URL") {
        return url;
    }

    let version = env!("CARGO_PKG_VERSION");
    if version.ends_with("-dev") || version.ends_with("-unstable") {
        "https://dev.spice.ai".to_string()
    } else {
        "https://spice.ai".to_string()
    }
}

/// Generate a random 8-character auth code.
fn generate_auth_code() -> String {
    use rand::Rng;
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = rand::rng();

    (0..8)
        .map(|_| {
            let idx = rng.random_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}

/// Read org and app name from spicepod.yaml if it exists.
fn read_spicepod_metadata() -> (Option<String>, Option<String>) {
    let Ok(contents) = std::fs::read_to_string("spicepod.yaml") else {
        return (None, None);
    };

    let Ok(yaml) = serde_yaml::from_str::<serde_yaml::Value>(&contents) else {
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

    let client = reqwest::Client::new();
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
