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

//! Cloud commands for managing Spice Cloud resources.

pub mod bytes;
mod client;
mod config;

use crate::context::RuntimeContext;
use crate::error::{InvalidArgumentSnafu, Result};
use crate::output::{OutputFormat, TableOutput, write_json};
use clap::{Args, Subcommand};
use dialoguer::{Input, Password, Select, theme::ColorfulTheme};
use snafu::ResultExt;
use std::{fmt, io::IsTerminal};

pub use client::CloudClient;
pub use config::{CloudLink, get_linked_app, load_cloud_link, remove_cloud_link, save_cloud_link};
use spice_cloud_client::types::{AppKind, IngestionMetrics, UpdateChannel};

/// Arguments for the cloud command.
#[derive(Args, Debug)]
#[command(
    about = "Manage Spice Cloud resources (apps, deployments, secrets, ...)",
    long_about = r#"Manage resources on Spice Cloud: authenticate, list and inspect
apps and deployments, manage secrets, view logs and metrics, and deploy.

Most subcommands require an active Spice Cloud session. Sign in with one of:
  spice cloud login subscription      # Browser-based subscription login
  spice cloud login pat               # Personal access token
  spice cloud login api               # OAuth client credentials (automation)

EXAMPLES
  spice cloud whoami                  # Show the active Spice Cloud identity
  spice cloud apps                    # List apps
  spice cloud link <app>              # Link the current directory to an app
  spice cloud deploy                  # Deploy the linked app
  spice cloud logs --tail             # Stream logs for the linked deployment
  spice cloud secrets set MY_KEY=...  # Manage app secrets

Docs: https://spiceai.org/docs/spice-cloud"#
)]
pub struct CloudArgs {
    #[command(subcommand)]
    pub command: CloudCommands,
}

/// Cloud subcommands.
#[derive(Subcommand, Debug)]
pub enum CloudCommands {
    /// Login to Spice Cloud
    Login(LoginArgs),

    /// Logout from Spice Cloud
    Logout,

    /// Show current authenticated user
    Whoami(WhoamiArgs),

    /// Link current directory to a Spice Cloud app
    Link(LinkArgs),

    /// Unlink current directory from Spice Cloud app
    Unlink,

    /// List all apps
    Apps(AppsArgs),

    /// List deployments for an app
    Deployments(DeploymentsArgs),

    /// List available regions
    Regions(RegionsArgs),

    /// List available container images
    Images(ImagesArgs),

    /// Manage secrets for an app
    #[command(subcommand)]
    Secrets(SecretsCommands),

    /// View deployment logs
    Logs(LogsArgs),

    /// Create a new resource
    #[command(subcommand)]
    Create(CreateCommands),

    /// Get details of a resource
    #[command(subcommand)]
    Get(GetCommands),

    /// Update a resource
    #[command(subcommand)]
    Update(UpdateCommands),

    /// Delete a resource
    #[command(subcommand)]
    Delete(DeleteCommands),

    /// Deploy the app
    Deploy(DeployArgs),

    /// Inspect current deployment status
    Inspect(InspectArgs),

    /// Rollback to a previous deployment
    Rollback(RollbackArgs),

    /// Show API keys for an app
    #[command(name = "api-keys")]
    ApiKeys(ApiKeysArgs),

    /// Show metrics for an app's pods
    Metrics(MetricsArgs),
}

// ============================================================================
// Subcommand argument structs
// ============================================================================

#[derive(Args, Debug)]
pub struct WhoamiArgs {
    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

#[derive(Args, Debug)]
pub struct AppsArgs {
    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

#[derive(Args, Debug)]
pub struct RegionsArgs {
    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

#[derive(Args)]
pub struct LoginArgs {
    #[command(subcommand)]
    pub method: Option<LoginMethod>,
}

impl fmt::Debug for LoginArgs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LoginArgs")
            .field("method", &self.method)
            .finish()
    }
}

#[derive(Subcommand)]
pub enum LoginMethod {
    /// Log in with your Spice Cloud subscription in a browser
    Subscription(SubscriptionLoginArgs),

    /// Log in with a Spice Cloud personal access token
    #[command(name = "pat")]
    Pat(PatLoginArgs),

    /// Log in with OAuth client credentials for automation
    Api(ApiLoginArgs),
}

impl fmt::Debug for LoginMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Subscription(args) => f.debug_tuple("Subscription").field(args).finish(),
            Self::Pat(args) => f.debug_tuple("Pat").field(args).finish(),
            Self::Api(args) => f.debug_tuple("Api").field(args).finish(),
        }
    }
}

#[derive(Args, Debug)]
pub struct SubscriptionLoginArgs {
    /// Don't open a browser; print the URL and a one-time code to enter on
    /// another device. Useful over SSH or in headless shells.
    #[arg(long)]
    pub device: bool,
}

#[derive(Args)]
pub struct PatLoginArgs {
    /// Personal access token. Omit to enter it securely.
    #[arg(
        long,
        env = "SPICE_CLOUD_PAT",
        value_name = "TOKEN",
        help_heading = "PAT Login Options"
    )]
    pub token: Option<String>,
}

impl fmt::Debug for PatLoginArgs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PatLoginArgs")
            .field("token", &self.token.as_deref().map(|_| "[REDACTED]"))
            .finish()
    }
}

#[derive(Args)]
pub struct ApiLoginArgs {
    /// OAuth client ID. Omit to enter it interactively.
    #[arg(
        long,
        env = "SPICE_CLOUD_CLIENT_ID",
        value_name = "CLIENT_ID",
        help_heading = "API Login Options"
    )]
    pub client_id: Option<String>,

    /// OAuth client secret. Omit to enter it securely.
    #[arg(
        long,
        env = "SPICE_CLOUD_CLIENT_SECRET",
        value_name = "CLIENT_SECRET",
        help_heading = "API Login Options"
    )]
    pub client_secret: Option<String>,
}

impl fmt::Debug for ApiLoginArgs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApiLoginArgs")
            .field("client_id", &self.client_id)
            .field(
                "client_secret",
                &self.client_secret.as_deref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

#[derive(Args, Debug)]
pub struct LinkArgs {
    /// App name in org/app format
    pub app: String,
}

#[derive(Args, Debug)]
pub struct DeploymentsArgs {
    /// App name in org/app format (uses linked app if not specified)
    #[arg(long)]
    pub app: Option<String>,

    /// Maximum number of deployments to show
    #[arg(long, default_value = "10")]
    pub limit: usize,

    /// Filter by deployment status
    #[arg(long)]
    pub status: Option<String>,

    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

#[derive(Args, Debug)]
pub struct ImagesArgs {
    /// Filter by channel (stable, beta, etc.)
    #[arg(long)]
    pub channel: Option<String>,

    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

#[derive(Args, Debug)]
pub struct LogsArgs {
    /// App name in org/app format (uses linked app if not specified)
    #[arg(long)]
    pub app: Option<String>,

    /// Deployment ID (uses latest if not specified)
    #[arg(long)]
    pub deployment: Option<i64>,

    /// Maximum number of log entries to show
    #[arg(long, default_value = "100")]
    pub limit: usize,

    /// Follow logs in real-time
    #[arg(short, long)]
    pub follow: bool,

    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

#[derive(Args, Debug)]
pub struct DeployArgs {
    /// App name in org/app format (uses linked app if not specified)
    #[arg(long)]
    pub app: Option<String>,

    /// Container image tag to deploy
    #[arg(long)]
    pub image: Option<String>,

    /// Number of replicas
    #[arg(long)]
    pub replicas: Option<i32>,

    /// Enable debug mode
    #[arg(long)]
    pub debug: bool,

    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

#[derive(Args, Debug)]
pub struct InspectArgs {
    /// App name in org/app format (uses linked app if not specified)
    #[arg(long)]
    pub app: Option<String>,

    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

#[derive(Args, Debug)]
pub struct RollbackArgs {
    /// App name in org/app format (uses linked app if not specified)
    #[arg(long)]
    pub app: Option<String>,

    /// Target deployment ID to rollback to
    #[arg(long)]
    pub target: Option<i64>,

    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

#[derive(Args, Debug)]
pub struct ApiKeysArgs {
    /// App name in org/app format (uses linked app if not specified)
    #[arg(long)]
    pub app: Option<String>,

    /// Regenerate API key (1 or 2)
    #[arg(long)]
    pub regenerate: Option<u8>,

    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

#[derive(Args, Debug)]
pub struct MetricsArgs {
    /// App name in org/app format (uses linked app if not specified)
    #[arg(long)]
    pub app: Option<String>,

    /// Window for counter metrics (e.g. 1m, 5m, 1h). Parsed as a duration.
    #[arg(long, value_parser = parse_window)]
    pub window: Option<String>,

    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

// ============================================================================
// Secrets subcommands
// ============================================================================

#[derive(Subcommand, Debug)]
pub enum SecretsCommands {
    /// List all secrets
    List(SecretsListArgs),

    /// Set a secret
    Set(SecretsSetArgs),

    /// Get a secret value
    Get(SecretsGetArgs),

    /// Delete a secret
    #[command(alias = "rm")]
    Delete(SecretsDeleteArgs),
}

#[derive(Args, Debug)]
pub struct SecretsListArgs {
    /// App name in org/app format (uses linked app if not specified)
    #[arg(long)]
    pub app: Option<String>,

    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

#[derive(Args, Debug)]
pub struct SecretsSetArgs {
    /// App name in org/app format (uses linked app if not specified)
    #[arg(long)]
    pub app: Option<String>,

    /// Secret name
    pub name: String,

    /// Secret value
    pub value: String,

    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

#[derive(Args, Debug)]
pub struct SecretsGetArgs {
    /// App name in org/app format (uses linked app if not specified)
    #[arg(long)]
    pub app: Option<String>,

    /// Secret name
    pub name: String,

    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

#[derive(Args, Debug)]
pub struct SecretsDeleteArgs {
    /// App name in org/app format (uses linked app if not specified)
    #[arg(long)]
    pub app: Option<String>,

    /// Secret name
    pub name: String,

    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

// ============================================================================
// Create subcommands
// ============================================================================

#[derive(Subcommand, Debug)]
pub enum CreateCommands {
    /// Create a new app
    App(CreateAppArgs),

    /// Create a new deployment
    Deployment(CreateDeploymentArgs),
}

#[derive(Args, Debug)]
pub struct CreateAppArgs {
    /// App name
    pub name: String,

    /// Deployment region (e.g. us-east-1-prod-aws-data)
    #[arg(long)]
    pub region: String,

    /// App kind (set or cluster)
    #[arg(long, value_parser = clap::value_parser!(AppKind), default_value = "set")]
    pub kind: AppKind,

    /// App description
    #[arg(long)]
    pub description: Option<String>,

    /// App visibility (public or private)
    #[arg(long, default_value = "private")]
    pub visibility: String,

    /// Number of scheduler replicas
    #[arg(long)]
    pub replicas: Option<i32>,

    /// Scheduler CPU limit in vCPUs (e.g. 4)
    #[arg(long)]
    pub cpu: Option<i32>,

    /// Scheduler memory limit (e.g. 16Gi, 16GiB)
    #[arg(long)]
    pub memory: Option<bytes::NumBytes>,

    /// Block storage size in GB
    #[arg(long)]
    pub storage_size_gb: Option<f64>,

    /// Number of executor replicas
    #[arg(long)]
    pub executor_replicas: Option<i32>,

    /// Executor CPU limit in vCPUs (e.g. 8)
    #[arg(long)]
    pub executor_cpu: Option<i32>,

    /// Executor memory limit (e.g. 32Gi, 32GiB)
    #[arg(long)]
    pub executor_memory: Option<bytes::NumBytes>,

    /// Path to a spicepod.yaml file
    #[arg(long)]
    pub spicepod: Option<String>,

    /// Update channel (stable, preview, nightly, internal)
    #[arg(long, value_parser = clap::value_parser!(UpdateChannel))]
    pub channel: Option<UpdateChannel>,

    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

#[derive(Args, Debug)]
pub struct CreateDeploymentArgs {
    /// App name in org/app format (uses linked app if not specified)
    #[arg(long)]
    pub app: Option<String>,

    /// Container image tag
    #[arg(long)]
    pub image: Option<String>,

    /// Number of replicas
    #[arg(long)]
    pub replicas: Option<i32>,

    /// Enable debug mode
    #[arg(long)]
    pub debug: bool,

    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

// ============================================================================
// Get subcommands
// ============================================================================

#[derive(Subcommand, Debug)]
pub enum GetCommands {
    /// Get app details
    App(GetAppArgs),
}

#[derive(Args, Debug)]
pub struct GetAppArgs {
    /// App name in org/app format
    pub app: String,

    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

// ============================================================================
// Update subcommands
// ============================================================================

#[derive(Subcommand, Debug)]
pub enum UpdateCommands {
    /// Update an app
    App(UpdateAppArgs),
}

#[derive(Args, Debug)]
pub struct UpdateAppArgs {
    /// App name in org/app format (uses linked app if not specified)
    #[arg(long)]
    pub app: Option<String>,

    /// New description
    #[arg(long)]
    pub description: Option<String>,

    /// New visibility (public or private)
    #[arg(long)]
    pub visibility: Option<String>,

    /// Number of scheduler replicas
    #[arg(long)]
    pub replicas: Option<i32>,

    /// Container image tag
    #[arg(long)]
    pub image: Option<String>,

    /// Deployment region
    #[arg(long)]
    pub region: Option<String>,

    /// Scheduler CPU limit in vCPUs (e.g. 4)
    #[arg(long)]
    pub cpu: Option<i32>,

    /// Scheduler memory limit (e.g. 16Gi, 16GiB)
    #[arg(long)]
    pub memory: Option<bytes::NumBytes>,

    /// Block storage size in GB
    #[arg(long)]
    pub storage_size_gb: Option<f64>,

    /// Number of executor replicas
    #[arg(long)]
    pub executor_replicas: Option<i32>,

    /// Executor CPU limit in vCPUs (e.g. 8)
    #[arg(long)]
    pub executor_cpu: Option<i32>,

    /// Executor memory limit (e.g. 32Gi, 32GiB)
    #[arg(long)]
    pub executor_memory: Option<bytes::NumBytes>,

    /// Path to a spicepod.yaml file
    #[arg(long)]
    pub spicepod: Option<String>,

    /// Update channel (stable, preview, nightly, internal)
    #[arg(long, value_parser = clap::value_parser!(UpdateChannel))]
    pub channel: Option<UpdateChannel>,

    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

// ============================================================================
// Delete subcommands
// ============================================================================

#[derive(Subcommand, Debug)]
pub enum DeleteCommands {
    /// Delete an app
    App(DeleteAppArgs),
}

#[derive(Args, Debug)]
pub struct DeleteAppArgs {
    /// App name in org/app format
    pub app: String,

    /// Skip confirmation prompt
    #[arg(long, short)]
    pub yes: bool,

    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

// ============================================================================
// Execute function
// ============================================================================

/// Execute the cloud command.
///
/// # Errors
///
/// Returns an error if the cloud operation fails.
pub async fn execute(_ctx: &RuntimeContext, args: &CloudArgs) -> Result<()> {
    match &args.command {
        CloudCommands::Login(login_args) => execute_login(login_args).await,
        CloudCommands::Logout => execute_logout(),
        CloudCommands::Whoami(whoami_args) => execute_whoami(whoami_args).await,
        CloudCommands::Link(link_args) => execute_link(link_args).await,
        CloudCommands::Unlink => execute_unlink(),
        CloudCommands::Apps(apps_args) => execute_apps(apps_args).await,
        CloudCommands::Deployments(deploy_args) => execute_deployments(deploy_args).await,
        CloudCommands::Regions(regions_args) => execute_regions(regions_args).await,
        CloudCommands::Images(images_args) => execute_images(images_args).await,
        CloudCommands::Secrets(secrets_cmd) => execute_secrets(secrets_cmd).await,
        CloudCommands::Logs(logs_args) => execute_logs(logs_args).await,
        CloudCommands::Create(create_cmd) => execute_create(create_cmd).await,
        CloudCommands::Get(get_cmd) => execute_get(get_cmd).await,
        CloudCommands::Update(update_cmd) => execute_update(update_cmd).await,
        CloudCommands::Delete(delete_cmd) => execute_delete(delete_cmd).await,
        CloudCommands::Deploy(deploy_args) => execute_deploy(deploy_args).await,
        CloudCommands::Inspect(inspect_args) => execute_inspect(inspect_args).await,
        CloudCommands::Rollback(rollback_args) => execute_rollback(rollback_args).await,
        CloudCommands::ApiKeys(api_keys_args) => execute_api_keys(api_keys_args).await,
        CloudCommands::Metrics(metrics_args) => execute_metrics(metrics_args).await,
    }
}

// ============================================================================
// Command implementations
// ============================================================================

async fn execute_login(args: &LoginArgs) -> Result<()> {
    match &args.method {
        Some(LoginMethod::Subscription(args)) => execute_login_device_flow(!args.device).await,
        Some(LoginMethod::Pat(args)) => execute_login_pat(args).await,
        Some(LoginMethod::Api(args)) => execute_login_api(args).await,
        None => execute_login_with_chooser().await,
    }
}

async fn execute_login_with_chooser() -> Result<()> {
    ensure_login_chooser_tty(std::io::stdin().is_terminal())?;

    let items = [
        "Subscription Login (browser)",
        "Subscription Login (device code, no browser)",
        "Personal Access Token (PAT)",
        "API Login (OAuth client)",
    ];
    let selection = Select::with_theme(&ColorfulTheme::default())
        .with_prompt("How would you like to log in to Spice Cloud?")
        .items(items)
        .default(0)
        .interact()
        .map_err(|err| crate::error::Error::InvalidArgument {
            message: format!("Failed to read login selection: {err}"),
        })?;

    match selection {
        0 => execute_login_device_flow(true).await,
        1 => execute_login_device_flow(false).await,
        2 => execute_login_pat(&PatLoginArgs { token: None }).await,
        3 => {
            execute_login_api(&ApiLoginArgs {
                client_id: None,
                client_secret: None,
            })
            .await
        }
        _ => InvalidArgumentSnafu {
            message: "Invalid login selection".to_string(),
        }
        .fail(),
    }
}

fn ensure_login_chooser_tty(is_terminal: bool) -> Result<()> {
    if !is_terminal {
        return InvalidArgumentSnafu {
            message: "Choose a login type explicitly when running non-interactively: 'spice cloud login subscription', 'spice cloud login subscription --device', 'spice cloud login pat', or 'spice cloud login api'",
        }
        .fail();
    }

    Ok(())
}

async fn execute_login_pat(args: &PatLoginArgs) -> Result<()> {
    let token = resolve_string_or_prompt(
        args.token.as_deref(),
        "PAT",
        "--token",
        "SPICE_CLOUD_PAT",
        "Spice Cloud personal access token",
        true,
    )?;

    save_token_and_print_login_result(&token).await
}

async fn execute_login_api(args: &ApiLoginArgs) -> Result<()> {
    let client_id = resolve_string_or_prompt(
        args.client_id.as_deref(),
        "OAuth client ID",
        "--client-id",
        "SPICE_CLOUD_CLIENT_ID",
        "OAuth client ID",
        false,
    )?;
    let client_secret = resolve_string_or_prompt(
        args.client_secret.as_deref(),
        "OAuth client secret",
        "--client-secret",
        "SPICE_CLOUD_CLIENT_SECRET",
        "OAuth client secret",
        true,
    )?;

    let client = CloudClient::new_unauthenticated()?;
    let token = client
        .exchange_client_credentials(&client_id, &client_secret)
        .await?;

    save_token_and_print_login_result(&token).await
}

fn resolve_string_or_prompt(
    value: Option<&str>,
    label: &str,
    flag: &str,
    env_var: &str,
    prompt: &str,
    secret: bool,
) -> Result<String> {
    resolve_string_or_prompt_with_terminal(
        value,
        label,
        flag,
        env_var,
        prompt,
        secret,
        std::io::stdin().is_terminal(),
    )
}

fn resolve_string_or_prompt_with_terminal(
    value: Option<&str>,
    label: &str,
    flag: &str,
    env_var: &str,
    prompt: &str,
    secret: bool,
    is_terminal: bool,
) -> Result<String> {
    if let Some(value) = value {
        if value.is_empty() {
            return InvalidArgumentSnafu {
                message: format!("{label} cannot be empty."),
            }
            .fail();
        }

        return Ok(value.to_string());
    }

    // The chooser path constructs args structs with all fields set to None,
    // bypassing Clap's env-var resolution. Re-resolve here so chooser-based
    // PAT/API logins respect the configured env vars.
    if let Ok(env_value) = std::env::var(env_var)
        && !env_value.is_empty()
    {
        return Ok(env_value);
    }

    if !is_terminal {
        return InvalidArgumentSnafu {
            message: format!("{label} is required. Provide {flag} or set {env_var}."),
        }
        .fail();
    }

    let value = if secret {
        Password::with_theme(&ColorfulTheme::default())
            .with_prompt(prompt)
            .interact()
            .map_err(|err| crate::error::Error::InvalidArgument {
                message: format!("Failed to read {label}: {err}"),
            })?
    } else {
        Input::<String>::with_theme(&ColorfulTheme::default())
            .with_prompt(prompt)
            .interact_text()
            .map_err(|err| crate::error::Error::InvalidArgument {
                message: format!("Failed to read {label}: {err}"),
            })?
    };

    if value.is_empty() {
        return InvalidArgumentSnafu {
            message: format!("{label} cannot be empty."),
        }
        .fail();
    }

    Ok(value)
}

async fn save_token_and_print_login_result(token: &str) -> Result<()> {
    use crate::commands::login::merge_auth_config;

    let authed_client = CloudClient::with_token(token)?;
    let auth_context_result = authed_client.get_auth_context().await;

    merge_auth_config("SPICEAI", &[("TOKEN", token)])?;

    match auth_context_result {
        Ok(context) => {
            if let Some(api_key) = context.app_api_key {
                merge_auth_config("SPICEAI", &[("API_KEY", &api_key)])?;
            }

            println!();
            println!(
                "\x1b[32m✓ Successfully logged in to Spice Cloud as {} ({})\x1b[0m",
                context.username, context.email
            );
        }
        Err(err) => {
            println!();
            println!(
                "\x1b[33m! Login token saved, but Spice Cloud could not verify the authenticated user context: {err}\x1b[0m"
            );
            println!(
                "\x1b[33m! Subsequent cloud commands may fail if the token is invalid or unauthorized.\x1b[0m"
            );
        }
    }

    print_post_login_help();
    Ok(())
}

async fn execute_login_device_flow(open_browser: bool) -> Result<()> {
    use rand::RngExt;

    // Generate auth code
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = rand::rng();
    let auth_code: String = (0..8)
        .map(|_| {
            let idx = rng.random_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect();

    let client = CloudClient::new_unauthenticated()?;
    let auth_url = client.get_auth_url(&auth_code);

    if open_browser {
        println!("Opening Spice Cloud authorization page in your default browser...");
    } else {
        println!("Complete Spice Cloud device login in a browser.");
    }
    println!(
        "\nYour auth code:\n\n  {}-{}\n",
        &auth_code[..4],
        &auth_code[4..]
    );
    if open_browser {
        println!("If the browser does not open, visit the following URL manually:");
    } else {
        println!("Open this URL in a browser:");
    }
    println!("\n  {auth_url}\n");

    if open_browser {
        let _ = open::that(&auth_url);
    }

    println!("Waiting for authentication...");

    // Poll for auth status
    let timeout = std::time::Duration::from_secs(300); // 5 minutes
    let start = std::time::Instant::now();

    loop {
        if start.elapsed() > timeout {
            return InvalidArgumentSnafu {
                message: "Authentication timed out. Please try again.",
            }
            .fail();
        }

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        if let Ok(Some(response)) = client.exchange_code(&auth_code).await {
            if response.access_denied {
                return InvalidArgumentSnafu {
                    message: "Access denied",
                }
                .fail();
            }

            if let Some(token) = response.access_token {
                return save_token_and_print_login_result(&token).await;
            }
        }
    }
}

fn print_post_login_help() {
    println!();
    println!("You can now use 'spice cloud' commands to manage your apps and deployments.");
    println!();
    println!("Quick start:");
    println!("  spice cloud apps              - List your apps");
    println!("  spice cloud create app <name> - Create a new app");
    println!("  spice cloud deploy --app <org/app> - Deploy your app");
    println!();
}

fn execute_logout() -> Result<()> {
    // Remove Spice.ai auth tokens
    let env_file = if std::path::Path::new(".env.local").exists() {
        ".env.local"
    } else {
        ".env"
    };

    let path = std::path::Path::new(env_file);
    if !path.exists() {
        println!("\x1b[32m✓ Already logged out\x1b[0m");
        return Ok(());
    }

    let content = std::fs::read_to_string(path).unwrap_or_default();
    let lines: Vec<&str> = content
        .lines()
        .filter(|line| {
            !line.starts_with("SPICE_SPICEAI_TOKEN=") && !line.starts_with("SPICE_SPICEAI_API_KEY=")
        })
        .collect();

    if lines.is_empty()
        || lines
            .iter()
            .all(|l| l.trim().is_empty() || l.starts_with('#'))
    {
        let _ = std::fs::remove_file(path);
    } else {
        let new_content = lines.join("\n");
        std::fs::write(path, new_content).map_err(|e| crate::error::Error::ConfigIo {
            operation: "write",
            path: path.to_path_buf(),
            source: e,
        })?;
    }

    println!("\x1b[32m✓ Successfully logged out from Spice Cloud\x1b[0m");
    Ok(())
}

async fn execute_whoami(args: &WhoamiArgs) -> Result<()> {
    let client = CloudClient::new()?;
    let context = client.get_auth_context().await?;

    if args.output == OutputFormat::Json {
        return write_json(&context);
    }

    println!("Logged in as: {} ({})", context.username, context.email);
    println!("Organization: {}", context.org_name);
    if let Some(app_name) = context.app_name {
        println!("Default App:  {}/{}", context.org_name, app_name);
    }

    Ok(())
}

async fn execute_link(args: &LinkArgs) -> Result<()> {
    let client = CloudClient::new()?;

    // Verify the app exists
    let app = client.get_app(&args.app).await?;

    // Save the link
    let link = CloudLink {
        org: app.org.clone(),
        app: app.name.clone(),
        app_id: Some(app.id),
        region: app.region,
        linked_at: Some(chrono::Utc::now().to_rfc3339()),
    };
    save_cloud_link(&link)?;

    println!("\x1b[32m✓ Linked to app {}/{}\x1b[0m", link.org, link.app);
    println!();
    println!("You can now use commands without specifying --app:");
    println!("  spice cloud deploy");
    println!("  spice cloud logs");
    println!("  spice cloud secrets list");

    Ok(())
}

fn execute_unlink() -> Result<()> {
    remove_cloud_link()?;
    println!("\x1b[32m✓ Unlinked from Spice Cloud app\x1b[0m");
    Ok(())
}

async fn execute_apps(args: &AppsArgs) -> Result<()> {
    let client = CloudClient::new()?;
    let context = client.get_auth_context().await?;
    let apps = client.list_apps().await?;

    if apps.is_empty() {
        println!("No apps found. Create one with: spice cloud create app <name>");
        return Ok(());
    }

    if args.output == OutputFormat::Json {
        return write_json(&apps);
    }

    let mut table = TableOutput::new(vec![
        "NAME",
        "DESCRIPTION",
        "REGION",
        "VISIBILITY",
        "CREATED",
    ]);
    for app in &apps {
        let display_name = display_app_name(app, &context.org_name);
        table.add_row(vec![
            display_name,
            app.description.clone().unwrap_or_default(),
            app.region.clone().unwrap_or_else(|| "-".to_string()),
            app.visibility
                .clone()
                .unwrap_or_else(|| "private".to_string()),
            app.created_at.clone().unwrap_or_else(|| "-".to_string()),
        ]);
    }
    table.print();

    Ok(())
}

/// Format an app's display name as `org/name`, falling back to the auth
/// context org when the app payload does not include one. The Spice Cloud
/// `/v1/apps` endpoint does not currently populate `org` on each app, so the
/// auth context provides the only source of truth for the user's org.
fn display_app_name(app: &spice_cloud_client::types::App, context_org: &str) -> String {
    let org = if app.org.is_empty() {
        context_org
    } else {
        app.org.as_str()
    };
    if org.is_empty() {
        app.name.clone()
    } else {
        format!("{org}/{}", app.name)
    }
}

async fn execute_deployments(args: &DeploymentsArgs) -> Result<()> {
    let client = CloudClient::new()?;
    let app_name = require_app(args.app.as_deref())?;

    let deployments = client
        .list_deployments(&app_name, args.limit, args.status.as_deref())
        .await?;

    if deployments.is_empty() {
        println!("No deployments found for {app_name}");
        return Ok(());
    }

    if args.output == OutputFormat::Json {
        return write_json(&deployments);
    }

    let mut table = TableOutput::new(vec!["ID", "STATUS", "IMAGE", "REPLICAS", "CREATED"]);
    for dep in deployments {
        table.add_row(vec![
            dep.id.to_string(),
            dep.status,
            dep.image_tag.unwrap_or_else(|| "-".to_string()),
            dep.replicas
                .map_or_else(|| "-".to_string(), |r| r.to_string()),
            dep.created_at.unwrap_or_else(|| "-".to_string()),
        ]);
    }
    table.print();

    Ok(())
}

async fn execute_regions(args: &RegionsArgs) -> Result<()> {
    let client = CloudClient::new()?;
    let regions_resp = client.list_regions(None).await?;

    if args.output == OutputFormat::Json {
        return write_json(&regions_resp.regions);
    }

    let mut table = TableOutput::new(vec!["NAME", "REGION", "PROVIDER", "DEFAULT"]);
    for region in regions_resp.regions {
        table.add_row(vec![
            region.name,
            region.region,
            region.provider_name.unwrap_or(region.provider),
            if region.is_default { "✓" } else { "" }.to_string(),
        ]);
    }
    table.print();

    Ok(())
}

async fn execute_images(args: &ImagesArgs) -> Result<()> {
    let client = CloudClient::new()?;
    let images_resp = client
        .list_container_images(args.channel.as_deref())
        .await?;

    if args.output == OutputFormat::Json {
        return write_json(&images_resp);
    }

    let mut table = TableOutput::new(vec!["TAG", "CHANNEL", "DEFAULT"]);
    for image in images_resp.images {
        let is_default = Some(&image.tag) == images_resp.default.as_ref();
        table.add_row(vec![
            image.tag,
            image.channel.unwrap_or_else(|| "-".to_string()),
            if is_default { "✓" } else { "" }.to_string(),
        ]);
    }
    table.print();

    Ok(())
}

async fn execute_secrets(cmd: &SecretsCommands) -> Result<()> {
    match cmd {
        SecretsCommands::List(args) => {
            let client = CloudClient::new()?;
            let app_name = require_app(args.app.as_deref())?;
            let secrets = client.list_secrets(&app_name).await?;

            if secrets.is_empty() {
                println!("No secrets found for {app_name}");
                return Ok(());
            }

            if args.output == OutputFormat::Json {
                return write_json(&secrets);
            }

            let mut table = TableOutput::new(vec!["NAME", "UPDATED"]);
            for secret in secrets {
                table.add_row(vec![
                    secret.name,
                    secret.updated_at.unwrap_or_else(|| "-".to_string()),
                ]);
            }
            table.print();
        }
        SecretsCommands::Set(args) => {
            let client = CloudClient::new()?;
            let app_name = require_app(args.app.as_deref())?;
            client
                .set_secret(&app_name, &args.name, &args.value)
                .await?;
            if args.output == OutputFormat::Json {
                return write_json(&serde_json::json!({"name": args.name, "status": "set"}));
            }
            println!("\x1b[32m✓ Secret '{}' set successfully\x1b[0m", args.name);
        }
        SecretsCommands::Get(args) => {
            let client = CloudClient::new()?;
            let app_name = require_app(args.app.as_deref())?;
            let secret = client.get_secret(&app_name, &args.name).await?;
            if args.output == OutputFormat::Json {
                return write_json(&secret);
            }
            println!("{}", secret.value.unwrap_or_default());
        }
        SecretsCommands::Delete(args) => {
            let client = CloudClient::new()?;
            let app_name = require_app(args.app.as_deref())?;
            client.delete_secret(&app_name, &args.name).await?;
            if args.output == OutputFormat::Json {
                return write_json(&serde_json::json!({"name": args.name, "status": "deleted"}));
            }
            println!("\x1b[32m✓ Secret '{}' deleted\x1b[0m", args.name);
        }
    }
    Ok(())
}

async fn execute_logs(args: &LogsArgs) -> Result<()> {
    let client = CloudClient::new()?;
    let app_name = require_app(args.app.as_deref())?;

    let deployment_id = if let Some(id) = args.deployment {
        id
    } else {
        let latest = client.get_latest_deployment(&app_name).await?;
        latest.id
    };

    let logs = client
        .get_deployment_logs(&app_name, deployment_id, args.limit, None)
        .await?;

    if args.output == OutputFormat::Json {
        return write_json(&logs);
    }

    for entry in logs.logs {
        let level_color = match entry.level.as_deref() {
            Some("error") => "\x1b[31m",
            Some("warn") => "\x1b[33m",
            Some("info") => "\x1b[32m",
            Some("debug") => "\x1b[34m",
            _ => "\x1b[0m",
        };
        println!(
            "{} {}{}\x1b[0m {}",
            entry.timestamp.unwrap_or_default(),
            level_color,
            entry.level.unwrap_or_default(),
            entry.message
        );
    }

    // TODO: Implement follow mode with streaming

    Ok(())
}

async fn execute_create(cmd: &CreateCommands) -> Result<()> {
    match cmd {
        CreateCommands::App(args) => {
            if args.kind == AppKind::Cluster && args.replicas.is_some_and(|r| r != 1) {
                return Err(crate::error::Error::InvalidArgument {
                    message: "SpicepodCluster requires --replicas 1".to_string(),
                });
            }

            let client = CloudClient::new()?;
            let spicepod_content = if let Some(path) = args.spicepod.as_deref() {
                Some(read_spicepod_file(path).await?)
            } else {
                None
            };

            let app = client
                .create_app(
                    &args.name,
                    &args.region,
                    args.kind,
                    args.description.as_deref(),
                    &args.visibility,
                    args.replicas,
                    args.cpu,
                    args.memory,
                    args.storage_size_gb,
                    args.executor_replicas,
                    args.executor_cpu,
                    args.executor_memory,
                )
                .await?;

            let org_app = app.full_name();

            let app = if spicepod_content.is_some() || args.channel.is_some() {
                match client
                    .update_app(
                        &org_app,
                        client::UpdateAppParams {
                            spicepod: spicepod_content,
                            channel: args.channel,
                            ..client::UpdateAppParams::default()
                        },
                    )
                    .await
                {
                    Ok(updated_app) => updated_app,
                    Err(error) => {
                        return Err(crate::error::Error::InvalidResponse {
                            message: format!(
                                "Created app {org_app}, but failed to update spicepod/channel: {error}. The app still exists; run `spice cloud update app --app {org_app}` to apply those settings or delete the app manually."
                            ),
                        });
                    }
                }
            } else {
                app
            };

            if args.output == OutputFormat::Json {
                return write_json(&app);
            }
            println!("\x1b[32m✓ Created app {org_app}\x1b[0m");
            if let Ok(api_keys) = client.get_api_keys(&org_app).await
                && let Some(api_key) = api_keys.api_key
            {
                println!("\nAPI Key: {api_key}");
                println!("\nSave this key - it won't be shown again.");
            }
        }
        CreateCommands::Deployment(args) => {
            let client = CloudClient::new()?;
            let app_name = require_app(args.app.as_deref())?;
            let deployment = client
                .create_deployment(&app_name, args.image.as_deref(), args.replicas, args.debug)
                .await?;
            if args.output == OutputFormat::Json {
                return write_json(&deployment);
            }
            println!(
                "\x1b[32m✓ Created deployment {} (status: {})\x1b[0m",
                deployment.id, deployment.status
            );
        }
    }
    Ok(())
}

async fn execute_get(cmd: &GetCommands) -> Result<()> {
    match cmd {
        GetCommands::App(args) => {
            let client = CloudClient::new()?;
            let app = client.get_app(&args.app).await?;

            if args.output == OutputFormat::Json {
                return write_json(&app);
            }

            println!("Name:        {}", app.full_name());
            if let Some(desc) = app.description {
                println!("Description: {desc}");
            }
            if let Some(visibility) = app.visibility {
                println!("Visibility:  {visibility}");
            }
            if let Some(region) = app.region {
                println!("Region:      {region}");
            }
            if let Some(created) = app.created_at {
                println!("Created:     {created}");
            }
        }
    }
    Ok(())
}

async fn execute_update(cmd: &UpdateCommands) -> Result<()> {
    match cmd {
        UpdateCommands::App(args) => {
            let client = CloudClient::new()?;
            let app_name = require_app(args.app.as_deref())?;
            let spicepod_content = if let Some(path) = args.spicepod.as_deref() {
                Some(read_spicepod_file(path).await?)
            } else {
                None
            };

            let app = client
                .update_app(
                    &app_name,
                    client::UpdateAppParams {
                        description: args.description.as_deref(),
                        visibility: args.visibility.as_deref(),
                        replicas: args.replicas,
                        image_tag: args.image.as_deref(),
                        region: args.region.as_deref(),
                        cpu: args.cpu,
                        memory: args.memory,
                        storage_size_gb: args.storage_size_gb,
                        executor_replicas: args.executor_replicas,
                        executor_cpu: args.executor_cpu,
                        executor_memory: args.executor_memory,
                        spicepod: spicepod_content,
                        channel: args.channel,
                    },
                )
                .await?;

            if args.output == OutputFormat::Json {
                return write_json(&app);
            }
            println!("\x1b[32m✓ Updated app {}\x1b[0m", app.full_name());
        }
    }
    Ok(())
}

async fn execute_delete(cmd: &DeleteCommands) -> Result<()> {
    use std::io::Write;

    match cmd {
        DeleteCommands::App(args) => {
            if !args.yes {
                print!("Are you sure you want to delete '{}'? [y/N] ", args.app);
                std::io::stdout()
                    .flush()
                    .context(crate::error::ConfigIoSnafu {
                        operation: "write",
                        path: std::path::PathBuf::from("stdout"),
                    })?;

                let mut input = String::new();
                std::io::stdin()
                    .read_line(&mut input)
                    .context(crate::error::ConfigIoSnafu {
                        operation: "read",
                        path: std::path::PathBuf::from("stdin"),
                    })?;

                if input.trim().to_lowercase() != "y" {
                    println!("Cancelled.");
                    return Ok(());
                }
            }

            let client = CloudClient::new()?;
            client.delete_app(&args.app).await?;
            if args.output == OutputFormat::Json {
                return write_json(&serde_json::json!({"app": args.app, "status": "deleted"}));
            }
            println!("\x1b[32m✓ Deleted app {}\x1b[0m", args.app);
        }
    }
    Ok(())
}

async fn execute_deploy(args: &DeployArgs) -> Result<()> {
    let client = CloudClient::new()?;
    let app_name = require_app(args.app.as_deref())?;

    let deployment = client
        .create_deployment(&app_name, args.image.as_deref(), args.replicas, args.debug)
        .await?;

    if args.output == OutputFormat::Json {
        return write_json(&deployment);
    }

    println!("Deploying to {app_name}...");
    println!(
        "\x1b[32m✓ Deployment {} started (status: {})\x1b[0m",
        deployment.id, deployment.status
    );

    Ok(())
}

async fn execute_inspect(args: &InspectArgs) -> Result<()> {
    let client = CloudClient::new()?;
    let app_name = require_app(args.app.as_deref())?;

    let app = client.get_app(&app_name).await?;
    let deployments = client.list_deployments(&app_name, 1, None).await?;

    if args.output == OutputFormat::Json {
        return write_json(&serde_json::json!({
            "app": app,
            "latest_deployment": deployments.first(),
        }));
    }

    println!("App: {}", app.full_name());
    if let Some(region) = app.region {
        println!("Region: {region}");
    }

    if let Some(deployment) = deployments.first() {
        println!();
        println!("Latest Deployment:");
        println!("  ID:      {}", deployment.id);
        println!("  Status:  {}", deployment.status);
        if let Some(image) = &deployment.image_tag {
            println!("  Image:   {image}");
        }
        if let Some(replicas) = deployment.replicas {
            println!("  Replicas: {replicas}");
        }
        if let Some(created) = &deployment.created_at {
            println!("  Created: {created}");
        }
        if let Some(error) = &deployment.error_message {
            println!("  Error:   {error}");
        }
    } else {
        println!("\nNo deployments found.");
    }

    Ok(())
}

async fn execute_rollback(args: &RollbackArgs) -> Result<()> {
    let client = CloudClient::new()?;
    let app_name = require_app(args.app.as_deref())?;

    let target_id = if let Some(id) = args.target {
        id
    } else {
        // Get the second-to-last deployment
        let deployments = client.list_deployments(&app_name, 2, None).await?;
        if deployments.len() < 2 {
            return InvalidArgumentSnafu {
                message: "No previous deployment to rollback to",
            }
            .fail();
        }
        deployments[1].id
    };

    let deployment = client.rollback(&app_name, target_id).await?;

    if args.output == OutputFormat::Json {
        return write_json(&deployment);
    }

    println!(
        "\x1b[32m✓ Rollback to deployment {} initiated (new deployment: {})\x1b[0m",
        target_id, deployment.id
    );

    Ok(())
}

async fn execute_api_keys(args: &ApiKeysArgs) -> Result<()> {
    let client = CloudClient::new()?;
    let app_name = require_app(args.app.as_deref())?;

    if let Some(key_num) = args.regenerate {
        if key_num != 1 && key_num != 2 {
            return InvalidArgumentSnafu {
                message: "Key number must be 1 or 2",
            }
            .fail();
        }
        let response = client.regenerate_api_key(&app_name, key_num).await?;
        if args.output == OutputFormat::Json {
            return write_json(&response);
        }
        println!("\x1b[32m✓ Regenerated API key {key_num}\x1b[0m");
        if let Some(key) = response.api_key {
            println!("\nAPI Key 1: {key}");
        }
        if let Some(key2) = response.api_key_2 {
            println!("API Key 2: {key2}");
        }
    } else {
        let keys = client.get_api_keys(&app_name).await?;
        if args.output == OutputFormat::Json {
            return write_json(&keys);
        }
        if let Some(key) = keys.api_key {
            println!("API Key 1: {key}");
        }
        if let Some(key2) = keys.api_key_2 {
            println!("API Key 2: {key2}");
        }
    }

    Ok(())
}

async fn execute_metrics(args: &MetricsArgs) -> Result<()> {
    let client = CloudClient::new()?;
    let app_name = require_app(args.app.as_deref())?;
    let app = client.get_app(&app_name).await?;

    let response = client
        .get_app_metrics(app.id, args.window.as_deref())
        .await?;

    if args.output == OutputFormat::Json {
        return write_json(&response);
    }

    if response.metrics.is_empty() {
        println!("No metrics available for {app_name}");
        return Ok(());
    }
    let mut table = TableOutput::new(vec![
        "POD",
        "CPU %",
        "MEMORY",
        "DISK USED",
        "DISK AVAIL",
        "DISK CAP",
    ]);
    for (pod, m) in &response.metrics {
        table.add_row(vec![
            pod.clone(),
            m.cpu_usage_percent
                .map_or_else(|| "-".to_string(), |v| format!("{v:.1}")),
            m.memory_usage_bytes.map_or_else(
                || "-".to_string(),
                |v| bytes::NumBytes::from_bytes(v).to_string(),
            ),
            m.disk_read_bytes
                .map_or_else(|| "-".to_string(), bytes::format_bytes_f64),
            m.disk_read_operations
                .map_or_else(|| "-".to_string(), |v| format!("{v:.1}")),
            m.disk_write_bytes
                .map_or_else(|| "-".to_string(), bytes::format_bytes_f64),
            m.disk_write_operations
                .map_or_else(|| "-".to_string(), |v| format!("{v:.1}")),
        ]);
    }
    table.print();

    println!();
    match &response.ingestion {
        Some(IngestionMetrics {
            rows_ingested: Some(rows),
            bytes_ingested: Some(bytes),
        }) => {
            println!(
                "Ingestion: {rows} rows, {}",
                bytes::NumBytes::from_bytes(*bytes)
            );
        }
        Some(IngestionMetrics {
            rows_ingested: Some(rows),
            bytes_ingested: None,
        }) => {
            println!("Ingestion: {rows} rows");
        }
        Some(IngestionMetrics {
            rows_ingested: None,
            bytes_ingested: Some(bytes),
        }) => {
            println!("Ingestion: {}", bytes::NumBytes::from_bytes(*bytes));
        }
        Some(IngestionMetrics {
            rows_ingested: None,
            bytes_ingested: None,
        })
        | None => {}
    }

    Ok(())
}

// ============================================================================
// Helper functions
// ============================================================================

/// Read a spicepod YAML file from disk and return its contents as a string.
async fn read_spicepod_file(path: &str) -> Result<String> {
    tokio::fs::read_to_string(path)
        .await
        .map_err(|e| crate::error::Error::InvalidArgument {
            message: format!("Failed to read spicepod file '{path}': {e}"),
        })
}

/// Validate that `--window` parses as a duration via `fundu`.
fn parse_window(s: &str) -> std::result::Result<String, String> {
    fundu::parse_duration(s)
        .map(|_| s.to_string())
        .map_err(|e| format!("invalid duration '{s}': {e}"))
}

/// Get the app name from the flag or the linked app.
fn require_app(flag_value: Option<&str>) -> Result<String> {
    if let Some(app) = flag_value {
        return Ok(app.to_string());
    }

    if let Some(app) = get_linked_app()? {
        return Ok(app);
    }

    InvalidArgumentSnafu {
        message: "App name is required. Use --app <org/app> or run 'spice cloud link' to link an app",
    }
    .fail()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn login_chooser_requires_tty() {
        let err = ensure_login_chooser_tty(false).expect_err("non-TTY chooser should fail");

        assert!(
            err.to_string()
                .contains("Choose a login type explicitly when running non-interactively"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn resolve_string_or_prompt_uses_non_empty_value() {
        let value = resolve_string_or_prompt_with_terminal(
            Some("client-id"),
            "OAuth client ID",
            "--client-id",
            "SPICE_CLOUD_CLIENT_ID",
            "OAuth client ID",
            false,
            false,
        )
        .expect("provided value should be accepted");

        assert_eq!(value, "client-id");
    }

    #[test]
    fn resolve_string_or_prompt_rejects_empty_value() {
        let err = resolve_string_or_prompt_with_terminal(
            Some(""),
            "OAuth client ID",
            "--client-id",
            "SPICE_CLOUD_CLIENT_ID",
            "OAuth client ID",
            false,
            false,
        )
        .expect_err("empty value should fail");

        assert!(
            err.to_string().contains("OAuth client ID cannot be empty."),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn resolve_string_or_prompt_requires_value_when_non_interactive() {
        let err = resolve_string_or_prompt_with_terminal(
            None,
            "OAuth client ID",
            "--client-id",
            "SPICE_CLOUD_CLIENT_ID",
            "OAuth client ID",
            false,
            false,
        )
        .expect_err("missing value should fail without a TTY");

        assert!(
            err.to_string().contains(
                "OAuth client ID is required. Provide --client-id or set SPICE_CLOUD_CLIENT_ID."
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn resolve_string_or_prompt_falls_back_to_env_var() {
        // Use a unique env var name so the test does not depend on host state.
        let env_var = "SPICE_CLOUD_TEST_RESOLVE_FALLBACK";
        // SAFETY: Setting environment variable for test purposes only.
        unsafe { std::env::set_var(env_var, "from-env") };

        let value = resolve_string_or_prompt_with_terminal(
            None,
            "test value",
            "--test",
            env_var,
            "test value",
            false,
            false,
        )
        .expect("env var should be used when value is None");

        // SAFETY: Removing environment variable for test purposes only.
        unsafe { std::env::remove_var(env_var) };

        assert_eq!(value, "from-env");
    }

    fn test_app(org: &str, name: &str) -> spice_cloud_client::types::App {
        spice_cloud_client::types::App {
            id: 1,
            name: name.to_string(),
            org: org.to_string(),
            description: None,
            visibility: None,
            created_at: None,
            region: None,
            production_branch: None,
            config: None,
        }
    }

    #[test]
    fn display_app_name_uses_app_org_when_present() {
        let app = test_app("analytics", "dashboard");
        assert_eq!(display_app_name(&app, "fallback"), "analytics/dashboard");
    }

    #[test]
    fn display_app_name_falls_back_to_context_org_when_app_org_is_empty() {
        let app = test_app("", "dashboard");
        assert_eq!(display_app_name(&app, "analytics"), "analytics/dashboard");
    }

    #[test]
    fn display_app_name_omits_leading_slash_when_org_unavailable() {
        let app = test_app("", "dashboard");
        assert_eq!(display_app_name(&app, ""), "dashboard");
    }
}
