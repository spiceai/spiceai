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

//! Spice.ai CLI - Main entry point.

use clap::{Parser, Subcommand};
use spice::commands::{
    acceleration, add, catalogs, chat, cloud, cluster, connect, dataset, datasets, eval, init,
    install, login, models, nsql, pods, query, refresh, run, search, sql, status, trace, upgrade,
    version, workers,
};
use spice::{Result, RuntimeContext};
use tracing_subscriber::EnvFilter;

/// Spice.ai CLI - Interact with the Spice.ai runtime
#[derive(Parser)]
#[command(name = "spice", version, about = "Spice.ai CLI")]
#[command(propagate_version = true)]
struct Cli {
    /// Verbose logging (-v for debug, -vv for trace)
    #[arg(short, long, action = clap::ArgAction::Count, global = true)]
    verbose: u8,

    /// The API key to use for authentication
    #[arg(long, global = true, env = "SPICE_API_KEY")]
    api_key: Option<String>,

    /// Use cloud instance of Spice. Requires --api-key
    #[arg(long, global = true)]
    cloud: bool,

    /// HTTP endpoint of Spice
    #[arg(long, global = true, default_value = "http://127.0.0.1:8090")]
    http_endpoint: String,

    /// The path to the root certificate file used to verify the Spice.ai runtime server certificate
    #[arg(long, global = true)]
    tls_root_certificate_file: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Show version information
    Version(version::VersionArgs),

    /// Show the status of the Spice runtime
    Status(status::StatusArgs),

    /// Run Spice.ai - starts the Spice.ai runtime
    Run(run::RunArgs),

    /// Start an interactive SQL query session
    Sql(sql::SqlArgs),

    /// Initialize Spice app - creates a new spicepod.yaml
    Init(init::InitArgs),

    /// Install or reinstall the Spice.ai runtime
    #[command(alias = "i")]
    Install(install::InstallArgs),

    /// Add Spicepod - adds a Spicepod to the project
    Add(add::AddArgs),

    /// Connect to a Spice.ai Cloud Platform app Spicepod
    Connect(connect::ConnectArgs),

    /// Login to Spice.ai or configure credentials for data sources
    Login(login::LoginArgs),

    /// Lists datasets loaded by the Spice runtime
    Datasets(datasets::DatasetsArgs),

    /// Lists catalogs configured by the Spice runtime
    Catalogs(catalogs::CatalogsArgs),

    /// Lists models loaded by the Spice runtime
    Models(models::ModelsArgs),

    /// Lists Spicepods loaded by the Spice runtime
    Pods(pods::PodsArgs),

    /// Refresh a dataset
    Refresh(refresh::RefreshArgs),

    /// Upgrades the Spice CLI and runtime to the latest or specified version
    Upgrade(upgrade::UpgradeArgs),

    /// Lists workers loaded by the Spice runtime
    Workers(workers::WorkersArgs),

    /// Manage dataset acceleration features
    Acceleration(acceleration::AccelerationArgs),

    /// Dataset operations (configure datasets)
    Dataset(dataset::DatasetArgs),

    /// Manage Spice Cloud resources
    Cloud(cloud::CloudArgs),

    /// Run model evaluation
    Eval(eval::EvalArgs),

    /// Return traces for operations that occurred in Spice
    Trace(trace::TraceArgs),

    /// Cluster operations for Spice runtime
    Cluster(cluster::ClusterArgs),

    /// Text-to-SQL REPL - translate natural language to SQL
    Nsql(nsql::NsqlArgs),

    /// Submit an async query or start an interactive async query REPL
    Query(query::QueryArgs),

    /// Search datasets with embeddings
    Search(search::SearchArgs),

    /// Chat with an LLM
    Chat(chat::ChatArgs),
}

fn main() {
    let cli = Cli::parse();

    // Initialize logging based on verbosity
    let filter = match cli.verbose {
        0 => EnvFilter::new("info"),
        1 => EnvFilter::new("debug"),
        _ => EnvFilter::new("trace"),
    };

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .without_time()
        .init();

    // Print version header (matching Go CLI behavior)
    if !matches!(cli.command, Commands::Version(_)) {
        println!("Spice.ai OSS CLI {}", version::cli_version());
    }

    // Run the CLI
    if let Err(e) = run_cli(cli) {
        tracing::error!("{e}");
        std::process::exit(1);
    }
}

fn run_cli(cli: Cli) -> Result<()> {
    // Create runtime context from CLI args
    let ctx = RuntimeContext::with_args(
        Some(cli.http_endpoint),
        cli.api_key,
        cli.cloud,
        cli.tls_root_certificate_file,
    )?;

    // Execute the command
    match cli.command {
        Commands::Version(args) => {
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| spice::error::Error::RuntimeExecution { source: e })?;
            rt.block_on(version::execute(&ctx, &args))?;
        }
        Commands::Status(args) => {
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| spice::error::Error::RuntimeExecution { source: e })?;
            rt.block_on(status::execute(&ctx, &args))?;
        }
        Commands::Run(args) => {
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| spice::error::Error::RuntimeExecution { source: e })?;
            rt.block_on(run::execute(&ctx, &args, cli.verbose))?;
        }
        Commands::Sql(args) => {
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| spice::error::Error::RuntimeExecution { source: e })?;
            rt.block_on(sql::execute(&ctx, &args))?;
        }
        Commands::Init(args) => {
            init::execute(&args)?;
        }
        Commands::Install(args) => {
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| spice::error::Error::RuntimeExecution { source: e })?;
            rt.block_on(install::execute(&ctx, &args))?;
        }
        Commands::Add(args) => {
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| spice::error::Error::RuntimeExecution { source: e })?;
            rt.block_on(add::execute(&ctx, args))?;
        }
        Commands::Connect(args) => {
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| spice::error::Error::RuntimeExecution { source: e })?;
            rt.block_on(connect::execute(&ctx, args))?;
        }
        Commands::Login(args) => {
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| spice::error::Error::RuntimeExecution { source: e })?;
            rt.block_on(login::execute(&ctx, args))?;
        }
        Commands::Datasets(args) => {
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| spice::error::Error::RuntimeExecution { source: e })?;
            rt.block_on(datasets::execute(&ctx, &args))?;
        }
        Commands::Catalogs(args) => {
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| spice::error::Error::RuntimeExecution { source: e })?;
            rt.block_on(catalogs::execute(&ctx, &args))?;
        }
        Commands::Models(args) => {
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| spice::error::Error::RuntimeExecution { source: e })?;
            rt.block_on(models::execute(&ctx, &args))?;
        }
        Commands::Pods(args) => {
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| spice::error::Error::RuntimeExecution { source: e })?;
            rt.block_on(pods::execute(&ctx, &args))?;
        }
        Commands::Refresh(args) => {
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| spice::error::Error::RuntimeExecution { source: e })?;
            rt.block_on(refresh::execute(&ctx, &args))?;
        }
        Commands::Upgrade(args) => {
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| spice::error::Error::RuntimeExecution { source: e })?;
            rt.block_on(upgrade::execute(&ctx, &args))?;
        }
        Commands::Workers(args) => {
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| spice::error::Error::RuntimeExecution { source: e })?;
            rt.block_on(workers::execute(&ctx, &args))?;
        }
        Commands::Acceleration(args) => {
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| spice::error::Error::RuntimeExecution { source: e })?;
            rt.block_on(acceleration::execute(&ctx, &args))?;
        }
        Commands::Dataset(args) => {
            dataset::execute(&args)?;
        }
        Commands::Cloud(args) => {
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| spice::error::Error::RuntimeExecution { source: e })?;
            rt.block_on(cloud::execute(&ctx, &args))?;
        }
        Commands::Eval(args) => {
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| spice::error::Error::RuntimeExecution { source: e })?;
            rt.block_on(eval::execute(&ctx, &args))?;
        }
        Commands::Trace(args) => {
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| spice::error::Error::RuntimeExecution { source: e })?;
            rt.block_on(trace::execute(&ctx, &args))?;
        }
        Commands::Cluster(args) => {
            cluster::execute(&args)?;
        }
        Commands::Nsql(args) => {
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| spice::error::Error::RuntimeExecution { source: e })?;
            rt.block_on(nsql::execute(&ctx, &args))?;
        }
        Commands::Query(args) => {
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| spice::error::Error::RuntimeExecution { source: e })?;
            rt.block_on(query::execute(&ctx, &args))?;
        }
        Commands::Search(args) => {
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| spice::error::Error::RuntimeExecution { source: e })?;
            rt.block_on(search::execute(&ctx, &args))?;
        }
        Commands::Chat(args) => {
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| spice::error::Error::RuntimeExecution { source: e })?;
            rt.block_on(chat::execute(&ctx, &args))?;
        }
    }

    Ok(())
}
