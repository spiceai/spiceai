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

mod client;
mod config;

use crate::context::RuntimeContext;
use crate::error::{InvalidArgumentSnafu, Result};
use crate::output::TableOutput;
use clap::{Args, Subcommand};
use snafu::ResultExt;

pub use client::CloudClient;
pub use config::{CloudLink, get_linked_app, load_cloud_link, remove_cloud_link, save_cloud_link};

/// Arguments for the cloud command.
#[derive(Args, Debug)]
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
    Whoami,

    /// Link current directory to a Spice Cloud app
    Link(LinkArgs),

    /// Unlink current directory from Spice Cloud app
    Unlink,

    /// List all apps
    Apps,

    /// List deployments for an app
    Deployments(DeploymentsArgs),

    /// List available regions
    Regions,

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
}

// ============================================================================
// Subcommand argument structs
// ============================================================================

#[derive(Args, Debug)]
pub struct LoginArgs {
    /// Skip opening the browser and print the auth URL instead
    #[arg(long)]
    pub no_browser: bool,
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
}

#[derive(Args, Debug)]
pub struct ImagesArgs {
    /// Filter by channel (stable, beta, etc.)
    #[arg(long)]
    pub channel: Option<String>,
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
}

#[derive(Args, Debug)]
pub struct InspectArgs {
    /// App name in org/app format (uses linked app if not specified)
    #[arg(long)]
    pub app: Option<String>,
}

#[derive(Args, Debug)]
pub struct RollbackArgs {
    /// App name in org/app format (uses linked app if not specified)
    #[arg(long)]
    pub app: Option<String>,

    /// Target deployment ID to rollback to
    #[arg(long)]
    pub target: Option<i64>,
}

#[derive(Args, Debug)]
pub struct ApiKeysArgs {
    /// App name in org/app format (uses linked app if not specified)
    #[arg(long)]
    pub app: Option<String>,

    /// Regenerate API key (1 or 2)
    #[arg(long)]
    pub regenerate: Option<u8>,
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
}

#[derive(Args, Debug)]
pub struct SecretsGetArgs {
    /// App name in org/app format (uses linked app if not specified)
    #[arg(long)]
    pub app: Option<String>,

    /// Secret name
    pub name: String,
}

#[derive(Args, Debug)]
pub struct SecretsDeleteArgs {
    /// App name in org/app format (uses linked app if not specified)
    #[arg(long)]
    pub app: Option<String>,

    /// Secret name
    pub name: String,
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

    /// App description
    #[arg(long)]
    pub description: Option<String>,

    /// App visibility (public or private)
    #[arg(long, default_value = "private")]
    pub visibility: String,
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

    /// Number of replicas
    #[arg(long)]
    pub replicas: Option<i32>,

    /// Container image tag
    #[arg(long)]
    pub image: Option<String>,

    /// Deployment region
    #[arg(long)]
    pub region: Option<String>,
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
        CloudCommands::Whoami => execute_whoami().await,
        CloudCommands::Link(link_args) => execute_link(link_args).await,
        CloudCommands::Unlink => execute_unlink(),
        CloudCommands::Apps => execute_apps().await,
        CloudCommands::Deployments(deploy_args) => execute_deployments(deploy_args).await,
        CloudCommands::Regions => execute_regions().await,
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
    }
}

// ============================================================================
// Command implementations
// ============================================================================

async fn execute_login(args: &LoginArgs) -> Result<()> {
    use crate::commands::login::merge_auth_config;
    use rand::Rng;

    // Generate auth code
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = rand::rng();
    let auth_code: String = (0..8)
        .map(|_| {
            let idx = rng.random_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect();

    let client = CloudClient::new_unauthenticated();
    let auth_url = client.get_auth_url(&auth_code);

    println!("Opening Spice Cloud authorization page in your default browser...");
    println!(
        "\nYour auth code:\n\n  {}-{}\n",
        &auth_code[..4],
        &auth_code[4..]
    );
    println!("If the browser does not open, visit the following URL manually:");
    println!("\n  {auth_url}\n");

    if !args.no_browser {
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
                // Save the token
                merge_auth_config("SPICEAI", &[("TOKEN", &token)])?;

                // Get user info
                let authed_client = CloudClient::new()?;
                if let Ok(context) = authed_client.get_auth_context().await {
                    if let Some(api_key) = context.app_api_key {
                        merge_auth_config("SPICEAI", &[("API_KEY", &api_key)])?;
                    }
                    println!();
                    println!(
                        "\x1b[32m✓ Successfully logged in to Spice Cloud as {} ({})\x1b[0m",
                        context.username, context.email
                    );
                } else {
                    println!("\n\x1b[32m✓ Successfully logged in to Spice Cloud\x1b[0m");
                }

                println!();
                println!(
                    "You can now use 'spice cloud' commands to manage your apps and deployments."
                );
                println!();
                println!("Quick start:");
                println!("  spice cloud apps              - List your apps");
                println!("  spice cloud create app <name> - Create a new app");
                println!("  spice cloud deploy --app <org/app> - Deploy your app");

                return Ok(());
            }
        }
    }
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

async fn execute_whoami() -> Result<()> {
    let client = CloudClient::new()?;
    let context = client.get_auth_context().await?;

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

async fn execute_apps() -> Result<()> {
    let client = CloudClient::new()?;
    let apps = client.list_apps().await?;

    if apps.is_empty() {
        println!("No apps found. Create one with: spice cloud create app <name>");
        return Ok(());
    }

    let mut table = TableOutput::new(vec![
        "NAME",
        "DESCRIPTION",
        "REGION",
        "VISIBILITY",
        "CREATED",
    ]);
    for app in apps {
        table.add_row(vec![
            app.full_name(),
            app.description.unwrap_or_default(),
            app.region.unwrap_or_else(|| "-".to_string()),
            app.visibility.unwrap_or_else(|| "private".to_string()),
            app.created_at.unwrap_or_else(|| "-".to_string()),
        ]);
    }
    table.print();

    Ok(())
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

async fn execute_regions() -> Result<()> {
    let client = CloudClient::new()?;
    let regions_resp = client.list_regions(None).await?;

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
            println!("\x1b[32m✓ Secret '{}' set successfully\x1b[0m", args.name);
        }
        SecretsCommands::Get(args) => {
            let client = CloudClient::new()?;
            let app_name = require_app(args.app.as_deref())?;
            let secret = client.get_secret(&app_name, &args.name).await?;
            println!("{}", secret.value.unwrap_or_default());
        }
        SecretsCommands::Delete(args) => {
            let client = CloudClient::new()?;
            let app_name = require_app(args.app.as_deref())?;
            client.delete_secret(&app_name, &args.name).await?;
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
            let client = CloudClient::new()?;
            let app = client
                .create_app(&args.name, args.description.as_deref(), &args.visibility)
                .await?;
            println!("\x1b[32m✓ Created app {}\x1b[0m", app.full_name());
            if let Some(api_key) = app.api_key {
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

            let app = client
                .update_app(
                    &app_name,
                    args.description.as_deref(),
                    args.visibility.as_deref(),
                    args.replicas,
                    args.image.as_deref(),
                    args.region.as_deref(),
                )
                .await?;

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
            println!("\x1b[32m✓ Deleted app {}\x1b[0m", args.app);
        }
    }
    Ok(())
}

async fn execute_deploy(args: &DeployArgs) -> Result<()> {
    let client = CloudClient::new()?;
    let app_name = require_app(args.app.as_deref())?;

    println!("Deploying to {app_name}...");

    let deployment = client
        .create_deployment(&app_name, args.image.as_deref(), args.replicas, args.debug)
        .await?;

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
        println!("\x1b[32m✓ Regenerated API key {key_num}\x1b[0m");
        if let Some(key) = response.api_key {
            println!("\nAPI Key 1: {key}");
        }
        if let Some(key2) = response.api_key_2 {
            println!("API Key 2: {key2}");
        }
    } else {
        let keys = client.get_api_keys(&app_name).await?;
        if let Some(key) = keys.api_key {
            println!("API Key 1: {key}");
        }
        if let Some(key2) = keys.api_key_2 {
            println!("API Key 2: {key2}");
        }
    }

    Ok(())
}

// ============================================================================
// Helper functions
// ============================================================================

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
