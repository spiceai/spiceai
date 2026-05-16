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

use clap::{CommandFactory, Parser, Subcommand};
use spice::commands::acceleration::{AccelerationArgs, SnapshotArgs, SnapshotsArgs};
use spice::commands::{
    acceleration, add, catalogs, chat, cloud, cluster, completions, connect, dataset, datasets,
    feedback, init, install, login, models, nsql, pods, query, refresh, run, search, sql, status,
    trace, upgrade, validate, version, workers,
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

    /// Use cloud instance of Spice in the specified region. Requires --api-key
    #[arg(long, global = true, value_parser = ["us-east-1", "us-west-2"])]
    cloud: Option<String>,

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

    /// Generate shell completions
    Completions(completions::CompletionsArgs),

    /// Validate a spicepod.yaml without starting the runtime
    Validate(validate::ValidateArgs),

    /// Open the Spice.ai community Slack to share feedback
    Feedback(feedback::FeedbackArgs),
}

fn main() {
    use std::io::IsTerminal;

    let cli = Cli::parse();

    // Verbosity flag wins; otherwise honour RUST_LOG; otherwise default to info.
    let filter = if cli.verbose > 0 {
        if cli.verbose == 1 {
            EnvFilter::new("debug")
        } else {
            EnvFilter::new("trace")
        }
    } else {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
    };

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .without_time()
        .init();

    // Version banner: stderr-only so it doesn't foul pipes, and only for interactive stderr.
    // Suppressed for commands that produce JSON (scripting) or where it's just noise.
    if std::io::stderr().is_terminal()
        && !matches!(cli.command, Commands::Version(_) | Commands::Completions(_))
        && !is_json_output(&cli.command)
    {
        eprintln!("Spice.ai OSS CLI {}", version::cli_version());
    }

    // Run the CLI
    if let Err(e) = run_cli(cli) {
        tracing::error!("{e}");
        std::process::exit(1);
    }
}

/// Returns true if the command will output JSON, so the banner should be suppressed.
fn is_json_output(cmd: &Commands) -> bool {
    use spice::output::OutputFormat;
    match cmd {
        Commands::Status(a) => a.output == OutputFormat::Json,
        Commands::Datasets(a) => a.output == OutputFormat::Json,
        Commands::Catalogs(a) => a.output == OutputFormat::Json,
        Commands::Models(a) => a.output == OutputFormat::Json,
        Commands::Pods(a) => a.output == OutputFormat::Json,
        Commands::Workers(a) => a.output == OutputFormat::Json,
        Commands::Trace(a) => matches!(a.output, trace::OutputFormat::Json),
        Commands::Search(a) => a.output == OutputFormat::Json,
        Commands::Query(a) => a.output == OutputFormat::Json,
        Commands::Acceleration(AccelerationArgs {
            command:
                acceleration::AccelerationCommand::Snapshots(SnapshotsArgs { output, .. })
                | acceleration::AccelerationCommand::Snapshot(SnapshotArgs { output, .. }),
        }) => *output == OutputFormat::Json,
        Commands::Cloud(a) => match &a.command {
            cloud::CloudCommands::Whoami(x) => x.output == OutputFormat::Json,
            cloud::CloudCommands::Apps(x) => x.output == OutputFormat::Json,
            cloud::CloudCommands::Regions(x) => x.output == OutputFormat::Json,
            cloud::CloudCommands::Images(x) => x.output == OutputFormat::Json,
            cloud::CloudCommands::Deployments(x) => x.output == OutputFormat::Json,
            cloud::CloudCommands::Inspect(x) => x.output == OutputFormat::Json,
            cloud::CloudCommands::ApiKeys(x) => x.output == OutputFormat::Json,
            cloud::CloudCommands::Metrics(x) => x.output == OutputFormat::Json,
            cloud::CloudCommands::Logs(x) => x.output == OutputFormat::Json,
            cloud::CloudCommands::Deploy(x) => x.output == OutputFormat::Json,
            cloud::CloudCommands::Rollback(x) => x.output == OutputFormat::Json,
            cloud::CloudCommands::Secrets(cloud::SecretsCommands::List(x)) => {
                x.output == OutputFormat::Json
            }
            cloud::CloudCommands::Secrets(cloud::SecretsCommands::Set(x)) => {
                x.output == OutputFormat::Json
            }
            cloud::CloudCommands::Secrets(cloud::SecretsCommands::Get(x)) => {
                x.output == OutputFormat::Json
            }
            cloud::CloudCommands::Secrets(cloud::SecretsCommands::Delete(x)) => {
                x.output == OutputFormat::Json
            }
            cloud::CloudCommands::Create(cloud::CreateCommands::App(x)) => {
                x.output == OutputFormat::Json
            }
            cloud::CloudCommands::Create(cloud::CreateCommands::Deployment(x)) => {
                x.output == OutputFormat::Json
            }
            cloud::CloudCommands::Get(cloud::GetCommands::App(x)) => x.output == OutputFormat::Json,
            cloud::CloudCommands::Update(cloud::UpdateCommands::App(x)) => {
                x.output == OutputFormat::Json
            }
            cloud::CloudCommands::Delete(cloud::DeleteCommands::App(x)) => {
                x.output == OutputFormat::Json
            }
            cloud::CloudCommands::Login(_)
            | cloud::CloudCommands::Logout
            | cloud::CloudCommands::Link(_)
            | cloud::CloudCommands::Unlink => false,
        },
        _ => false,
    }
}

fn run_cli(cli: Cli) -> Result<()> {
    // Create runtime context from CLI args
    let ctx = RuntimeContext::with_args(
        Some(cli.http_endpoint),
        cli.api_key,
        cli.cloud.as_deref(),
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
        Commands::Completions(args) => {
            completions::execute(&args, &mut Cli::command());
        }
        Commands::Validate(args) => {
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| spice::error::Error::RuntimeExecution { source: e })?;
            rt.block_on(validate::execute(&args))?;
        }
        Commands::Feedback(args) => {
            feedback::execute(&args)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(args).expect("failed to parse CLI args")
    }

    fn is_json(args: &[&str]) -> bool {
        is_json_output(&parse(args).command)
    }

    #[test]
    fn cloud_login_subscription_device_flag_parses() {
        let cli = parse(&["spice", "cloud", "login", "subscription", "--device"]);

        let Commands::Cloud(cloud::CloudArgs {
            command: cloud::CloudCommands::Login(login_args),
        }) = cli.command
        else {
            panic!("expected cloud login command");
        };
        let Some(cloud::LoginMethod::Subscription(args)) = login_args.method else {
            panic!("expected subscription login method");
        };

        assert!(args.device);
    }

    #[test]
    fn cloud_login_api_flags_parse_under_api_subcommand() {
        let cli = parse(&[
            "spice",
            "cloud",
            "login",
            "api",
            "--client-id",
            "client-id",
            "--client-secret",
            "client-secret",
        ]);

        let Commands::Cloud(cloud::CloudArgs {
            command: cloud::CloudCommands::Login(login_args),
        }) = cli.command
        else {
            panic!("expected cloud login command");
        };
        let Some(cloud::LoginMethod::Api(args)) = login_args.method else {
            panic!("expected api login method");
        };

        assert_eq!(args.client_id.as_deref(), Some("client-id"));
        assert_eq!(args.client_secret.as_deref(), Some("client-secret"));
    }

    #[test]
    fn cloud_metrics_json_output_suppresses_banner() {
        assert!(is_json(&[
            "spice", "cloud", "metrics", "--app", "org/app", "--output", "json",
        ]));
    }

    #[test]
    fn cloud_metrics_table_output_keeps_banner() {
        assert!(!is_json(&["spice", "cloud", "metrics", "--app", "org/app"]));
        assert!(!is_json(&[
            "spice", "cloud", "metrics", "--app", "org/app", "--output", "table",
        ]));
    }

    #[test]
    fn cloud_all_json_producing_subcommands_suppress_banner() {
        // Every cloud subcommand whose execute_* fn writes JSON when --output=json
        // must cause the banner to be suppressed, otherwise piping to `jq` breaks.
        let json_producing: &[&[&str]] = &[
            &["spice", "cloud", "whoami", "--output", "json"],
            &["spice", "cloud", "apps", "--output", "json"],
            &["spice", "cloud", "regions", "--output", "json"],
            &["spice", "cloud", "images", "--output", "json"],
            &["spice", "cloud", "deployments", "--output", "json"],
            &["spice", "cloud", "inspect", "--output", "json"],
            &["spice", "cloud", "api-keys", "--output", "json"],
            &["spice", "cloud", "metrics", "--output", "json"],
            &["spice", "cloud", "logs", "--output", "json"],
            &["spice", "cloud", "deploy", "--output", "json"],
            &["spice", "cloud", "rollback", "--output", "json"],
            &["spice", "cloud", "secrets", "list", "--output", "json"],
            &[
                "spice", "cloud", "secrets", "set", "name", "value", "--output", "json",
            ],
            &[
                "spice", "cloud", "secrets", "get", "name", "--output", "json",
            ],
            &[
                "spice", "cloud", "secrets", "delete", "name", "--output", "json",
            ],
            &[
                "spice", "cloud", "create", "app", "name", "--output", "json",
            ],
            &["spice", "cloud", "create", "deployment", "--output", "json"],
            &[
                "spice", "cloud", "get", "app", "org/app", "--output", "json",
            ],
            &["spice", "cloud", "update", "app", "--output", "json"],
            &[
                "spice", "cloud", "delete", "app", "org/app", "--yes", "--output", "json",
            ],
        ];
        for argv in json_producing {
            assert!(
                is_json(argv),
                "expected --output=json to suppress banner for: {argv:?}",
            );
        }
    }

    #[test]
    fn non_json_commands_keep_banner() {
        // Commands without --output=json should not suppress the banner.
        assert!(!is_json(&["spice", "cloud", "login"]));
        assert!(!is_json(&["spice", "cloud", "logout"]));
        assert!(!is_json(&["spice", "cloud", "unlink"]));
        assert!(!is_json(&["spice", "datasets"]));
        assert!(!is_json(&["spice", "pods"]));
    }

    #[test]
    fn non_cloud_json_output_suppresses_banner() {
        assert!(is_json(&["spice", "datasets", "--output", "json"]));
        assert!(is_json(&["spice", "pods", "--output", "json"]));
        assert!(is_json(&["spice", "status", "--output", "json"]));
    }
}
