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
        None => {
            // Main Spice.ai login with OAuth flow
            login_spiceai(ctx, args.key).await
        }
    }
}

/// Login to Spice.ai using OAuth flow or direct API key.
#[expect(
    clippy::unused_async,
    reason = "Async for consistency with other login providers"
)]
async fn login_spiceai(_ctx: &RuntimeContext, api_key: Option<String>) -> Result<()> {
    if let Some(key) = api_key {
        // Direct API key authentication
        merge_auth_config("SPICEAI", &[("API_KEY", key.as_str())])?;
        println!("\x1b[32mSuccessfully logged in to Spice.ai with API key\x1b[0m");
        return Ok(());
    }

    // OAuth flow - this would normally open a browser and poll for auth
    // For now, we'll provide instructions for manual authentication
    println!("Spice.ai OAuth login is not yet implemented in the Rust CLI.");
    println!("Please use one of the following alternatives:");
    println!("  1. Use --key to provide an API key directly: spice login --key <your-api-key>");
    println!("  2. Set the SPICE_API_KEY environment variable");
    println!("  3. Add SPICE_SPICEAI_API_KEY=<your-api-key> to your .env file");

    Ok(())
}
