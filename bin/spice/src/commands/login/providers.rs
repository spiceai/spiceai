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

//! Login provider implementations.

// Async functions match the interface used by the main login command dispatcher,
// even though most don't actually need to await anything.
#![allow(clippy::unused_async)]

use crate::context::RuntimeContext;
use crate::error::Result;
use clap::Args;

use super::merge_auth_config;

// Auth type constants (match Go CLI)
const AUTH_TYPE_DREMIO: &str = "DREMIO";
const AUTH_TYPE_S3: &str = "S3";
const AUTH_TYPE_PG: &str = "PG";
const AUTH_TYPE_SNOWFLAKE: &str = "SNOWFLAKE";
const AUTH_TYPE_DATABRICKS: &str = "DATABRICKS";
const AUTH_TYPE_DELTA_LAKE: &str = "DELTA_LAKE";
const AUTH_TYPE_SPARK: &str = "SPARK";
const AUTH_TYPE_SHAREPOINT: &str = "SHAREPOINT";
const AUTH_TYPE_ABFS: &str = "ABFS";

// Auth param constants (match Go CLI)
const AUTH_PARAM_USERNAME: &str = "USERNAME";
const AUTH_PARAM_PASSWORD: &str = "PASSWORD";
const AUTH_PARAM_PASS: &str = "PASS";
const AUTH_PARAM_KEY: &str = "KEY";
const AUTH_PARAM_SECRET: &str = "SECRET";
const AUTH_PARAM_ACCOUNT: &str = "ACCOUNT";
const AUTH_PARAM_TOKEN: &str = "TOKEN";
const AUTH_PARAM_REMOTE: &str = "REMOTE";
const AUTH_PARAM_PRIVATE_KEY_PATH: &str = "PRIVATE_KEY_PATH";
const AUTH_PARAM_PRIVATE_KEY_PASSPHRASE: &str = "PRIVATE_KEY_PASSPHRASE";
const AUTH_PARAM_CLIENT_ID: &str = "CLIENT_ID";
const AUTH_PARAM_CLIENT_SECRET: &str = "CLIENT_SECRET";
const AUTH_PARAM_AWS_DEFAULT_REGION: &str = "AWS_DEFAULT_REGION";
const AUTH_PARAM_AWS_ACCESS_KEY_ID: &str = "AWS_ACCESS_KEY_ID";
const AUTH_PARAM_AWS_SECRET_ACCESS_KEY: &str = "AWS_SECRET_ACCESS_KEY";
const AUTH_PARAM_AZURE_ACCOUNT_NAME: &str = "AZURE_STORAGE_ACCOUNT_NAME";
const AUTH_PARAM_AZURE_ACCESS_KEY: &str = "AZURE_STORAGE_ACCESS_KEY";
const AUTH_PARAM_GCP_SERVICE_ACCOUNT_KEY_PATH: &str = "GOOGLE_SERVICE_ACCOUNT_PATH";
const AUTH_PARAM_BEARER_TOKEN: &str = "BEARER_TOKEN";
const AUTH_PARAM_TENANT_ID: &str = "TENANT_ID";

/// Arguments for Dremio login.
#[derive(Args, Debug)]
pub struct DremioArgs {
    /// Username
    #[arg(short, long)]
    pub username: String,

    /// Password
    #[arg(short, long)]
    pub password: String,
}

/// Login to Dremio.
pub async fn login_dremio(_ctx: &RuntimeContext, args: DremioArgs) -> Result<()> {
    merge_auth_config(
        AUTH_TYPE_DREMIO,
        &[
            (AUTH_PARAM_USERNAME, &args.username),
            (AUTH_PARAM_PASSWORD, &args.password),
        ],
    )?;

    println!("\x1b[32mSuccessfully logged in to Dremio\x1b[0m");
    Ok(())
}

/// Arguments for S3 login.
#[derive(Args, Debug)]
pub struct S3Args {
    /// Access key
    #[arg(short = 'k', long = "access-key")]
    pub access_key: String,

    /// Access secret
    #[arg(short = 's', long = "access-secret")]
    pub access_secret: String,
}

/// Login to S3.
pub async fn login_s3(_ctx: &RuntimeContext, args: S3Args) -> Result<()> {
    merge_auth_config(
        AUTH_TYPE_S3,
        &[
            (AUTH_PARAM_KEY, &args.access_key),
            (AUTH_PARAM_SECRET, &args.access_secret),
        ],
    )?;

    println!("\x1b[32mSuccessfully logged in to S3\x1b[0m");
    Ok(())
}

/// Arguments for Postgres login.
#[derive(Args, Debug)]
pub struct PostgresArgs {
    /// Password
    #[arg(short, long)]
    pub password: String,
}

/// Login to Postgres.
pub async fn login_postgres(_ctx: &RuntimeContext, args: PostgresArgs) -> Result<()> {
    merge_auth_config(AUTH_TYPE_PG, &[(AUTH_PARAM_PASS, &args.password)])?;

    println!("\x1b[32mSuccessfully logged in to Postgres\x1b[0m");
    Ok(())
}

/// Arguments for Snowflake login.
#[derive(Args, Debug)]
pub struct SnowflakeArgs {
    /// Account identifier
    #[arg(short, long)]
    pub account: String,

    /// Username
    #[arg(short, long)]
    pub username: String,

    /// Password (for username/password auth)
    #[arg(short, long)]
    pub password: Option<String>,

    /// Private key path (for key-pair auth)
    #[arg(short = 'k', long = "private-key-path")]
    pub private_key_path: Option<String>,

    /// Passphrase for private key
    #[arg(short = 's', long)]
    pub passphrase: Option<String>,
}

/// Login to Snowflake.
pub async fn login_snowflake(_ctx: &RuntimeContext, args: SnowflakeArgs) -> Result<()> {
    let mut params: Vec<(&str, &str)> = vec![
        (AUTH_PARAM_ACCOUNT, &args.account),
        (AUTH_PARAM_USERNAME, &args.username),
    ];

    if let Some(ref private_key_path) = args.private_key_path {
        // Key-pair authentication
        params.push((AUTH_PARAM_PRIVATE_KEY_PATH, private_key_path));
        if let Some(ref passphrase) = args.passphrase {
            params.push((AUTH_PARAM_PRIVATE_KEY_PASSPHRASE, passphrase));
        }
    } else if let Some(ref password) = args.password {
        // Username/password authentication
        params.push((AUTH_PARAM_PASSWORD, password));
    } else {
        return Err(crate::error::Error::InvalidArgument {
            message: "Either --password or --private-key-path must be provided".to_string(),
        });
    }

    merge_auth_config(AUTH_TYPE_SNOWFLAKE, &params)?;

    println!("\x1b[32mSuccessfully logged in to Snowflake\x1b[0m");
    Ok(())
}

/// Arguments for Databricks login.
#[derive(Args, Debug)]
pub struct DatabricksArgs {
    /// Access token (for PAT auth)
    #[arg(short = 'p', long)]
    pub token: Option<String>,

    /// Client ID (for service principal auth)
    #[arg(long = "client-id")]
    pub client_id: Option<String>,

    /// Client secret (for service principal auth)
    #[arg(long = "client-secret")]
    pub client_secret: Option<String>,

    /// AWS region (for Delta Lake on S3)
    #[arg(long = "aws-region")]
    pub aws_region: Option<String>,

    /// AWS access key ID
    #[arg(long = "aws-access-key-id")]
    pub aws_access_key_id: Option<String>,

    /// AWS secret access key
    #[arg(long = "aws-secret-access-key")]
    pub aws_secret_access_key: Option<String>,

    /// Azure storage account name
    #[arg(long = "azure-storage-account-name")]
    pub azure_storage_account_name: Option<String>,

    /// Azure storage access key
    #[arg(long = "azure-storage-access-key")]
    pub azure_storage_access_key: Option<String>,

    /// Google service account path
    #[arg(long = "google-service-account-path")]
    pub google_service_account_path: Option<String>,
}

/// Login to Databricks.
pub async fn login_databricks(_ctx: &RuntimeContext, args: DatabricksArgs) -> Result<()> {
    // Validate: either token OR both client-id and client-secret
    let has_token = args.token.is_some();
    let has_service_principal = args.client_id.is_some() && args.client_secret.is_some();

    if has_token == has_service_principal {
        return Err(crate::error::Error::InvalidArgument {
            message: "You must provide either --token OR both --client-id and --client-secret for Databricks authentication.".to_string(),
        });
    }

    let mut params: Vec<(&str, &str)> = Vec::new();

    if let Some(ref token) = args.token {
        params.push((AUTH_PARAM_TOKEN, token));
    }
    if let Some(ref client_id) = args.client_id {
        params.push((AUTH_PARAM_CLIENT_ID, client_id));
    }
    if let Some(ref client_secret) = args.client_secret {
        params.push((AUTH_PARAM_CLIENT_SECRET, client_secret));
    }

    // Optional cloud storage credentials
    if let Some(ref region) = args.aws_region {
        params.push((AUTH_PARAM_AWS_DEFAULT_REGION, region));
    }
    if let Some(ref key_id) = args.aws_access_key_id {
        params.push((AUTH_PARAM_AWS_ACCESS_KEY_ID, key_id));
    }
    if let Some(ref secret) = args.aws_secret_access_key {
        params.push((AUTH_PARAM_AWS_SECRET_ACCESS_KEY, secret));
    }
    if let Some(ref account_name) = args.azure_storage_account_name {
        params.push((AUTH_PARAM_AZURE_ACCOUNT_NAME, account_name));
    }
    if let Some(ref access_key) = args.azure_storage_access_key {
        params.push((AUTH_PARAM_AZURE_ACCESS_KEY, access_key));
    }
    if let Some(ref path) = args.google_service_account_path {
        params.push((AUTH_PARAM_GCP_SERVICE_ACCOUNT_KEY_PATH, path));
    }

    merge_auth_config(AUTH_TYPE_DATABRICKS, &params)?;

    println!("\x1b[32mSuccessfully configured credentials for Databricks\x1b[0m");
    Ok(())
}

/// Arguments for Delta Lake login.
#[derive(Args, Debug)]
pub struct DeltaLakeArgs {
    /// AWS region
    #[arg(long = "aws-region")]
    pub aws_region: Option<String>,

    /// AWS access key ID
    #[arg(long = "aws-access-key-id")]
    pub aws_access_key_id: Option<String>,

    /// AWS secret access key
    #[arg(long = "aws-secret-access-key")]
    pub aws_secret_access_key: Option<String>,

    /// Azure storage account name
    #[arg(long = "azure-storage-account-name")]
    pub azure_storage_account_name: Option<String>,

    /// Azure storage access key
    #[arg(long = "azure-storage-access-key")]
    pub azure_storage_access_key: Option<String>,

    /// Google service account path
    #[arg(long = "google-service-account-path")]
    pub google_service_account_path: Option<String>,
}

/// Configure credentials for Delta Lake.
pub async fn login_delta_lake(_ctx: &RuntimeContext, args: DeltaLakeArgs) -> Result<()> {
    let mut params: Vec<(&str, &str)> = Vec::new();

    if let Some(ref region) = args.aws_region {
        params.push((AUTH_PARAM_AWS_DEFAULT_REGION, region));
    }
    if let Some(ref key_id) = args.aws_access_key_id {
        params.push((AUTH_PARAM_AWS_ACCESS_KEY_ID, key_id));
    }
    if let Some(ref secret) = args.aws_secret_access_key {
        params.push((AUTH_PARAM_AWS_SECRET_ACCESS_KEY, secret));
    }
    if let Some(ref account_name) = args.azure_storage_account_name {
        params.push((AUTH_PARAM_AZURE_ACCOUNT_NAME, account_name));
    }
    if let Some(ref access_key) = args.azure_storage_access_key {
        params.push((AUTH_PARAM_AZURE_ACCESS_KEY, access_key));
    }
    if let Some(ref path) = args.google_service_account_path {
        params.push((AUTH_PARAM_GCP_SERVICE_ACCOUNT_KEY_PATH, path));
    }

    if params.is_empty() {
        return Err(crate::error::Error::InvalidArgument {
            message: "At least one credential must be provided".to_string(),
        });
    }

    merge_auth_config(AUTH_TYPE_DELTA_LAKE, &params)?;

    println!("\x1b[32mSuccessfully configured credentials for Delta Lake\x1b[0m");
    Ok(())
}

/// Arguments for Spark login.
#[derive(Args, Debug)]
pub struct SparkArgs {
    /// Spark remote connection string
    #[arg(long = "spark_remote")]
    pub spark_remote: String,
}

/// Login to Spark Connect remote.
pub async fn login_spark(_ctx: &RuntimeContext, args: SparkArgs) -> Result<()> {
    merge_auth_config(AUTH_TYPE_SPARK, &[(AUTH_PARAM_REMOTE, &args.spark_remote)])?;

    println!("\x1b[32mSuccessfully logged in to Spark\x1b[0m");
    Ok(())
}

/// Arguments for `SharePoint` login.
#[derive(Args, Debug)]
pub struct SharePointArgs {
    /// Microsoft organization tenant ID
    #[arg(short = 't', long = "tenant-id")]
    pub tenant_id: String,

    /// Microsoft Azure AD application client ID
    #[arg(short = 'c', long = "client-id")]
    pub client_id: String,
}

/// Login to Microsoft 365 `SharePoint`.
///
/// This performs an interactive `OAuth2` device code flow to authenticate with
/// Microsoft Azure AD and obtain an access token for `SharePoint` access.
pub async fn login_sharepoint(_ctx: &RuntimeContext, args: SharePointArgs) -> Result<()> {
    // Microsoft Graph scopes for SharePoint access
    let scopes = [
        "User.Read",
        "Files.Read.All",
        "Sites.Read.All",
        "GroupMember.Read.All",
    ];

    let access_token = msal_device_code_flow(&args.tenant_id, &args.client_id, &scopes).await?;

    merge_auth_config(
        AUTH_TYPE_SHAREPOINT,
        &[
            (AUTH_PARAM_BEARER_TOKEN, &access_token),
            (AUTH_PARAM_TENANT_ID, &args.tenant_id),
            (AUTH_PARAM_CLIENT_ID, &args.client_id),
        ],
    )?;

    println!(
        "\x1b[32mSuccessfully logged into Microsoft 365 SharePoint with client ID: {}\x1b[0m",
        args.client_id
    );
    Ok(())
}

/// Arguments for Azure Blob Storage (ABFS) login.
#[derive(Args, Debug)]
pub struct AbfsArgs {
    /// Microsoft organization tenant ID
    #[arg(short = 't', long = "tenant-id")]
    pub tenant_id: String,

    /// Microsoft Azure AD application client ID
    #[arg(short = 'c', long = "client-id")]
    pub client_id: String,
}

/// Login to Azure Blob Storage (ABFS).
///
/// This performs an interactive `OAuth2` device code flow to authenticate with
/// Microsoft Azure AD and obtain an access token for Azure Storage access.
pub async fn login_abfs(_ctx: &RuntimeContext, args: AbfsArgs) -> Result<()> {
    // Azure Storage scope
    let scopes = ["https://storage.azure.com/.default"];

    let access_token = msal_device_code_flow(&args.tenant_id, &args.client_id, &scopes).await?;

    merge_auth_config(
        AUTH_TYPE_ABFS,
        &[
            (AUTH_PARAM_BEARER_TOKEN, &access_token),
            (AUTH_PARAM_TENANT_ID, &args.tenant_id),
            (AUTH_PARAM_CLIENT_ID, &args.client_id),
        ],
    )?;

    println!(
        "\x1b[32mSuccessfully logged into Azure Storage Account with client ID: {}\x1b[0m",
        args.client_id
    );
    Ok(())
}

/// Perform Microsoft `OAuth2` device code flow authentication.
///
/// This is the CLI-friendly alternative to browser-based interactive auth.
/// The user is given a device code and URL to visit to complete authentication.
async fn msal_device_code_flow(
    tenant_id: &str,
    client_id: &str,
    scopes: &[&str],
) -> Result<String> {
    let authority_url = format!("https://login.microsoftonline.com/{tenant_id}");
    let device_code_url = format!("{authority_url}/oauth2/v2.0/devicecode");
    let token_url = format!("{authority_url}/oauth2/v2.0/token");

    let scope_string = scopes.join(" ");

    // Step 1: Request device code
    let client = reqwest::Client::new();
    let device_response = client
        .post(&device_code_url)
        .form(&[("client_id", client_id), ("scope", &scope_string)])
        .send()
        .await
        .map_err(|e| crate::error::Error::InvalidResponse {
            message: format!("Failed to request device code: {e}"),
        })?;

    if !device_response.status().is_success() {
        let error_text = device_response.text().await.unwrap_or_default();
        return Err(crate::error::Error::InvalidResponse {
            message: format!("Device code request failed: {error_text}"),
        });
    }

    let device_code_data: serde_json::Value =
        device_response
            .json()
            .await
            .map_err(|e| crate::error::Error::InvalidResponse {
                message: format!("Failed to parse device code response: {e}"),
            })?;

    let device_code = device_code_data["device_code"].as_str().ok_or_else(|| {
        crate::error::Error::InvalidResponse {
            message: "No device_code in response".to_string(),
        }
    })?;

    let user_code = device_code_data["user_code"].as_str().ok_or_else(|| {
        crate::error::Error::InvalidResponse {
            message: "No user_code in response".to_string(),
        }
    })?;

    let verification_uri = device_code_data["verification_uri"]
        .as_str()
        .ok_or_else(|| crate::error::Error::InvalidResponse {
            message: "No verification_uri in response".to_string(),
        })?;

    let interval = device_code_data["interval"].as_u64().unwrap_or(5);
    let expires_in = device_code_data["expires_in"].as_u64().unwrap_or(900);

    // Step 2: Display instructions to user
    println!("\nTo sign in, use a web browser to open the page:");
    println!("  {verification_uri}");
    println!("\nAnd enter the code: {user_code}\n");

    // Try to open browser automatically
    let _ = open::that(verification_uri);

    println!("Waiting for authentication...");

    // Step 3: Poll for token
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(expires_in);
    let poll_interval = std::time::Duration::from_secs(interval);

    loop {
        if start.elapsed() > timeout {
            return Err(crate::error::Error::InvalidResponse {
                message: "Authentication timed out. Please try again.".to_string(),
            });
        }

        tokio::time::sleep(poll_interval).await;

        let token_response = client
            .post(&token_url)
            .form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
                ("client_id", client_id),
                ("device_code", device_code),
            ])
            .send()
            .await
            .map_err(|e| crate::error::Error::InvalidResponse {
                message: format!("Token request failed: {e}"),
            })?;

        let token_data: serde_json::Value =
            token_response
                .json()
                .await
                .map_err(|e| crate::error::Error::InvalidResponse {
                    message: format!("Failed to parse token response: {e}"),
                })?;

        // Check for errors
        if let Some(error) = token_data["error"].as_str() {
            match error {
                "authorization_pending" => {
                    // User hasn't completed auth yet, keep polling
                    continue;
                }
                "slow_down" => {
                    // We need to slow down polling
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
                "expired_token" => {
                    return Err(crate::error::Error::InvalidResponse {
                        message: "Device code expired. Please try again.".to_string(),
                    });
                }
                "access_denied" => {
                    return Err(crate::error::Error::InvalidResponse {
                        message: "Access denied by user.".to_string(),
                    });
                }
                _ => {
                    let description = token_data["error_description"].as_str().unwrap_or(error);
                    return Err(crate::error::Error::InvalidResponse {
                        message: format!("Authentication error: {description}"),
                    });
                }
            }
        }

        // Success - extract access token
        if let Some(access_token) = token_data["access_token"].as_str() {
            return Ok(access_token.to_string());
        }

        return Err(crate::error::Error::InvalidResponse {
            message: "No access_token in token response".to_string(),
        });
    }
}
