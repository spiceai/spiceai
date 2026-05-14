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
use spice::commands::component::{ComponentSection, SingletonSection};
use spice::commands::{
    acceleration, add, catalogs, chat, cloud, cluster, completions, component, connect, dataset,
    datasets, init, install, login, models, nsql, pods, query, refresh, run, search, sql, status,
    trace, upgrade, validate, version, workers,
};
use spice::output::OutputFormat;
use spice::{Result, RuntimeContext};
use tracing_subscriber::EnvFilter;

/// Spice.ai CLI - Interact with the Spice.ai runtime, edit Spicepod manifests, and manage Spice Cloud.
#[derive(Parser)]
#[command(
    name = "spice",
    version,
    about = "Spice.ai CLI - SQL, search, and AI inference for data apps and agents",
    long_about = "\
Spice.ai CLI - SQL, search, and AI inference for data apps and agents.

The `spice` command lets you install and run the Spice runtime locally, query \
federated data sources with SQL, search and chat with your data, edit Spicepod \
manifests, and manage Spice Cloud deployments.

Quick start:
  spice init my_app          # Scaffold a new Spicepod in ./my_app/
  cd my_app
  spice run                  # Install (if needed) and start the runtime
  spice sql                  # Open an interactive SQL REPL

Common workflows:
  Manage data:    spice dataset add ...  |  spice catalog add ...  |  spice refresh ...
  Models & AI:    spice model add ...    |  spice chat   |  spice search  |  spice nsql
  Inspect:        spice status  |  spice datasets  |  spice models  |  spice pods
  Deploy:         spice cloud login  |  spice cloud deploy

Run `spice <command> --help` for details on any command.
Docs: https://spiceai.org/docs",
    after_help = "Docs: https://spiceai.org/docs  |  Cookbook: https://github.com/spiceai/cookbook"
)]
#[command(propagate_version = true)]
struct Cli {
    /// Increase log verbosity (-v for debug, -vv for trace).
    #[arg(short, long, action = clap::ArgAction::Count, global = true)]
    verbose: u8,

    /// Programmatic mode for LLMs and automation: prefer JSON output and structured JSON errors.
    #[arg(short = 'p', long)]
    programmatic: bool,

    /// API key used to authenticate with the runtime or Spice.ai Cloud.
    #[arg(long, global = true, env = "SPICE_API_KEY")]
    api_key: Option<String>,

    /// Target Spice.ai Cloud in the given region instead of a local runtime (requires --api-key).
    #[arg(long, global = true, value_parser = ["us-east-1", "us-west-2"])]
    cloud: Option<String>,

    /// HTTP endpoint of the Spice runtime to talk to.
    #[arg(long, global = true, default_value = "http://127.0.0.1:8090")]
    http_endpoint: String,

    /// Path to a PEM root certificate used to verify the runtime's TLS server certificate.
    #[arg(long, global = true)]
    tls_root_certificate_file: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    // ── Lifecycle ────────────────────────────────────────────────────────────
    Version(version::VersionArgs),

    #[command(alias = "i")]
    Install(install::InstallArgs),

    Upgrade(upgrade::UpgradeArgs),

    Run(run::RunArgs),

    Status(status::StatusArgs),

    // ── Spicepod scaffolding & dependencies ──────────────────────────────────
    Init(init::InitArgs),

    Add(add::AddArgs),

    Connect(connect::ConnectArgs),

    Validate(validate::ValidateArgs),

    // ── Spicepod manifest editing ────────────────────────────────────────────
    Dataset(dataset::DatasetArgs),

    /// Add or configure catalog entries in `spicepod.yaml`.
    #[command(long_about = component::COMPONENT_LONG_ABOUT)]
    Catalog(component::ComponentArgs),

    /// Add or configure model entries in `spicepod.yaml`.
    #[command(long_about = component::COMPONENT_LONG_ABOUT)]
    Model(component::ComponentArgs),

    /// Add or configure view entries in `spicepod.yaml`.
    #[command(long_about = component::COMPONENT_LONG_ABOUT)]
    View(component::ComponentArgs),

    /// Add or configure embedding entries in `spicepod.yaml`.
    #[command(long_about = component::COMPONENT_LONG_ABOUT)]
    Embedding(component::ComponentArgs),

    /// Add or configure reranker entries in `spicepod.yaml`.
    #[command(long_about = component::COMPONENT_LONG_ABOUT)]
    Reranker(component::ComponentArgs),

    /// Add or configure tool entries in `spicepod.yaml`.
    #[command(long_about = component::COMPONENT_LONG_ABOUT)]
    Tool(component::ComponentArgs),

    /// Add or configure worker entries in `spicepod.yaml`.
    #[command(long_about = component::COMPONENT_LONG_ABOUT)]
    Worker(component::ComponentArgs),

    /// Add or configure function entries in `spicepod.yaml`.
    #[command(long_about = component::COMPONENT_LONG_ABOUT)]
    Function(component::ComponentArgs),

    /// Add or configure secret entries in `spicepod.yaml`.
    #[command(long_about = component::COMPONENT_LONG_ABOUT)]
    Secret(component::ComponentArgs),

    /// Configure the `runtime:` section of `spicepod.yaml`.
    #[command(long_about = component::SINGLETON_LONG_ABOUT)]
    Runtime(component::SingletonArgs),

    /// Configure the `management:` section of `spicepod.yaml`.
    #[command(long_about = component::SINGLETON_LONG_ABOUT)]
    Management(component::SingletonArgs),

    /// Configure the `snapshots:` section of `spicepod.yaml`.
    #[command(long_about = component::SINGLETON_LONG_ABOUT)]
    Snapshots(component::SingletonArgs),

    Extension(component::ExtensionArgs),

    Metadata(component::MetadataArgs),

    // ── Listing & inspection (talk to a running runtime) ─────────────────────
    Pods(pods::PodsArgs),

    Datasets(datasets::DatasetsArgs),

    Catalogs(catalogs::CatalogsArgs),

    Models(models::ModelsArgs),

    Workers(workers::WorkersArgs),

    Trace(trace::TraceArgs),

    // ── Querying & AI ────────────────────────────────────────────────────────
    Sql(sql::SqlArgs),

    Query(query::QueryArgs),

    Nsql(nsql::NsqlArgs),

    Search(search::SearchArgs),

    Chat(chat::ChatArgs),

    Refresh(refresh::RefreshArgs),

    Acceleration(acceleration::AccelerationArgs),

    // ── Auth, cluster, and Spice Cloud ───────────────────────────────────────
    Login(login::LoginArgs),

    Cloud(cloud::CloudArgs),

    Cluster(cluster::ClusterArgs),

    // ── Tooling ──────────────────────────────────────────────────────────────
    Completions(completions::CompletionsArgs),
}

fn main() {
    use std::io::IsTerminal;

    let mut cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => {
            if raw_args_enable_programmatic_mode() {
                let exit_code = error.exit_code();
                write_programmatic_clap_error(&error);
                std::process::exit(exit_code);
            }
            error.exit();
        }
    };

    if cli.programmatic {
        apply_programmatic_mode(&mut cli.command);
    }

    // Verbosity flag wins; otherwise honour RUST_LOG; otherwise default to info.
    let filter = if cli.verbose > 0 {
        if cli.verbose == 1 {
            EnvFilter::new("debug")
        } else {
            EnvFilter::new("trace")
        }
    } else if cli.programmatic {
        EnvFilter::new("off")
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
        && !cli.programmatic
        && !matches!(cli.command, Commands::Version(_) | Commands::Completions(_))
        && !is_json_output(&cli.command)
    {
        eprintln!("Spice.ai OSS CLI {}", version::cli_version());
    }

    // Run the CLI
    let programmatic = cli.programmatic;
    if let Err(e) = run_cli(cli) {
        if programmatic {
            write_programmatic_error(&e);
        } else {
            tracing::error!("{e}");
        }
        std::process::exit(1);
    }
}

fn raw_args_enable_programmatic_mode() -> bool {
    let mut args = std::env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-p" | "--programmatic" => return true,
            "--" => return false,
            "--api-key" | "--cloud" | "--http-endpoint" | "--tls-root-certificate-file" => {
                let _ = args.next();
            }
            value if value.starts_with("--api-key=")
                || value.starts_with("--cloud=")
                || value.starts_with("--http-endpoint=")
                || value.starts_with("--tls-root-certificate-file=") => {}
            value if value.starts_with("--") => {}
            value if value.starts_with('-') => {
                if value.chars().skip(1).any(|flag| flag == 'p') {
                    return true;
                }
            }
            _ => return false,
        }
    }

    false
}

fn apply_programmatic_mode(command: &mut Commands) {
    match command {
        Commands::Version(args) => args.output = OutputFormat::Json,
        Commands::Status(args) => args.output = OutputFormat::Json,
        Commands::Datasets(args) => args.output = OutputFormat::Json,
        Commands::Catalogs(args) => args.output = OutputFormat::Json,
        Commands::Models(args) => args.output = OutputFormat::Json,
        Commands::Pods(args) => args.output = OutputFormat::Json,
        Commands::Workers(args) => args.output = OutputFormat::Json,
        Commands::Trace(args) => args.output = trace::OutputFormat::Json,
        Commands::Search(args) => args.output = OutputFormat::Json,
        Commands::Query(args) => apply_programmatic_query_mode(args),
        Commands::Acceleration(args) => apply_programmatic_acceleration_mode(args),
        Commands::Cloud(args) => apply_programmatic_cloud_mode(&mut args.command),
        Commands::Init(_)
        | Commands::Install(_)
        | Commands::Upgrade(_)
        | Commands::Run(_)
        | Commands::Add(_)
        | Commands::Connect(_)
        | Commands::Validate(_)
        | Commands::Dataset(_)
        | Commands::Catalog(_)
        | Commands::Model(_)
        | Commands::View(_)
        | Commands::Embedding(_)
        | Commands::Reranker(_)
        | Commands::Tool(_)
        | Commands::Worker(_)
        | Commands::Function(_)
        | Commands::Secret(_)
        | Commands::Runtime(_)
        | Commands::Management(_)
        | Commands::Snapshots(_)
        | Commands::Extension(_)
        | Commands::Metadata(_)
        | Commands::Sql(_)
        | Commands::Nsql(_)
        | Commands::Chat(_)
        | Commands::Refresh(_)
        | Commands::Login(_)
        | Commands::Cluster(_)
        | Commands::Completions(_) => {}
    }
}

fn apply_programmatic_query_mode(args: &mut query::QueryArgs) {
    args.output = OutputFormat::Json;

    let Some(command) = &mut args.command else {
        return;
    };

    match command {
        query::QuerySubcommand::List { output, .. }
        | query::QuerySubcommand::Status { output, .. }
        | query::QuerySubcommand::Results { output, .. } => {
            *output = OutputFormat::Json;
        }
        query::QuerySubcommand::Cancel { .. } => {}
    }
}

fn apply_programmatic_acceleration_mode(args: &mut AccelerationArgs) {
    match &mut args.command {
        acceleration::AccelerationCommand::Snapshots(args) => args.output = OutputFormat::Json,
        acceleration::AccelerationCommand::Snapshot(args) => args.output = OutputFormat::Json,
        acceleration::AccelerationCommand::SetSnapshot(_) => {}
    }
}

fn apply_programmatic_cloud_mode(command: &mut cloud::CloudCommands) {
    match command {
        cloud::CloudCommands::Whoami(args) => args.output = OutputFormat::Json,
        cloud::CloudCommands::Apps(args) => args.output = OutputFormat::Json,
        cloud::CloudCommands::Deployments(args) => args.output = OutputFormat::Json,
        cloud::CloudCommands::Regions(args) => args.output = OutputFormat::Json,
        cloud::CloudCommands::Images(args) => args.output = OutputFormat::Json,
        cloud::CloudCommands::Logs(args) => args.output = OutputFormat::Json,
        cloud::CloudCommands::Deploy(args) => args.output = OutputFormat::Json,
        cloud::CloudCommands::Inspect(args) => args.output = OutputFormat::Json,
        cloud::CloudCommands::Rollback(args) => args.output = OutputFormat::Json,
        cloud::CloudCommands::ApiKeys(args) => args.output = OutputFormat::Json,
        cloud::CloudCommands::Metrics(args) => args.output = OutputFormat::Json,
        cloud::CloudCommands::Secrets(command) => match command {
            cloud::SecretsCommands::List(args) => args.output = OutputFormat::Json,
            cloud::SecretsCommands::Set(args) => args.output = OutputFormat::Json,
            cloud::SecretsCommands::Get(args) => args.output = OutputFormat::Json,
            cloud::SecretsCommands::Delete(args) => args.output = OutputFormat::Json,
        },
        cloud::CloudCommands::Create(command) => match command {
            cloud::CreateCommands::App(args) => args.output = OutputFormat::Json,
            cloud::CreateCommands::Deployment(args) => args.output = OutputFormat::Json,
        },
        cloud::CloudCommands::Get(cloud::GetCommands::App(args)) => {
            args.output = OutputFormat::Json;
        }
        cloud::CloudCommands::Update(cloud::UpdateCommands::App(args)) => {
            args.output = OutputFormat::Json;
        }
        cloud::CloudCommands::Delete(cloud::DeleteCommands::App(args)) => {
            args.output = OutputFormat::Json;
        }
        cloud::CloudCommands::Login(_)
        | cloud::CloudCommands::Logout
        | cloud::CloudCommands::Link(_)
        | cloud::CloudCommands::Unlink => {}
    }
}

fn write_programmatic_clap_error(error: &clap::Error) {
    let body = serde_json::json!({
        "status": "error",
        "error": {
            "code": "cli_parse_error",
            "kind": format!("{:?}", error.kind()),
            "message": error.to_string(),
        }
    });

    match serde_json::to_string(&body) {
        Ok(body) => eprintln!("{body}"),
        Err(_) => eprintln!(
            "{}",
            r#"{"status":"error","error":{"code":"error_serialization_failed","message":"Failed to serialize CLI error"}}"#
        ),
    }
}

fn write_programmatic_error(error: &spice::error::Error) {
    let body = serde_json::json!({
        "status": "error",
        "error": {
            "code": programmatic_error_code(error),
            "message": error.to_string(),
        }
    });

    match serde_json::to_string(&body) {
        Ok(body) => eprintln!("{body}"),
        Err(_) => eprintln!(
            "{}",
            r#"{"status":"error","error":{"code":"error_serialization_failed","message":"Failed to serialize CLI error"}}"#
        ),
    }
}

fn programmatic_error_code(error: &spice::error::Error) -> &'static str {
    match error {
        spice::error::Error::RuntimeNotInstalled => "runtime_not_installed",
        spice::error::Error::WindowsNativeRuntimeUnsupported => {
            "windows_native_runtime_unsupported"
        }
        spice::error::Error::RuntimeUnavailable { .. } => "runtime_unavailable",
        spice::error::Error::Unauthorized => "unauthorized",
        spice::error::Error::PermissionDenied => "permission_denied",
        spice::error::Error::RuntimeHttp { .. } => "runtime_http_error",
        spice::error::Error::ConnectionFailed { .. } => "connection_failed",
        spice::error::Error::HttpRequestFailed { .. } => "http_request_failed",
        spice::error::Error::InvalidResponse { .. } => "invalid_response",
        spice::error::Error::ConfigIo { .. } => "config_io",
        spice::error::Error::ConfigParse { .. } => "config_parse",
        spice::error::Error::CreateDirectory { .. } => "create_directory",
        spice::error::Error::RuntimeExecution { .. } => "runtime_execution",
        spice::error::Error::RuntimeVersion { .. } => "runtime_version",
        spice::error::Error::Environment { .. } => "environment",
        spice::error::Error::InvalidArgument { .. } => "invalid_argument",
        spice::error::Error::HomeDirectoryNotFound => "home_directory_not_found",
        spice::error::Error::Repl { .. } => "repl",
        spice::error::Error::ChildProcessId => "child_process_id",
        spice::error::Error::SignalHandler { .. } => "signal_handler",
        spice::error::Error::ModelNotFound { .. } => "model_not_found",
        spice::error::Error::NoModelsConfigured => "no_models_configured",
    }
}

/// Returns true if the command will output JSON, so the banner should be suppressed.
fn is_json_output(cmd: &Commands) -> bool {
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
        Commands::Catalog(args) => {
            component::execute_component(ComponentSection::Catalog, &args)?;
        }
        Commands::Models(args) => {
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| spice::error::Error::RuntimeExecution { source: e })?;
            rt.block_on(models::execute(&ctx, &args))?;
        }
        Commands::Model(args) => {
            component::execute_component(ComponentSection::Model, &args)?;
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
        Commands::Worker(args) => {
            component::execute_component(ComponentSection::Worker, &args)?;
        }
        Commands::Acceleration(args) => {
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| spice::error::Error::RuntimeExecution { source: e })?;
            rt.block_on(acceleration::execute(&ctx, &args))?;
        }
        Commands::Dataset(args) => {
            dataset::execute(&args)?;
        }
        Commands::View(args) => {
            component::execute_component(ComponentSection::View, &args)?;
        }
        Commands::Embedding(args) => {
            component::execute_component(ComponentSection::Embedding, &args)?;
        }
        Commands::Reranker(args) => {
            component::execute_component(ComponentSection::Reranker, &args)?;
        }
        Commands::Tool(args) => {
            component::execute_component(ComponentSection::Tool, &args)?;
        }
        Commands::Function(args) => {
            component::execute_component(ComponentSection::Function, &args)?;
        }
        Commands::Secret(args) => {
            component::execute_component(ComponentSection::Secret, &args)?;
        }
        Commands::Runtime(args) => {
            component::execute_singleton(SingletonSection::Runtime, &args)?;
        }
        Commands::Management(args) => {
            component::execute_singleton(SingletonSection::Management, &args)?;
        }
        Commands::Snapshots(args) => {
            component::execute_singleton(SingletonSection::Snapshots, &args)?;
        }
        Commands::Extension(args) => {
            component::execute_extension(&args)?;
        }
        Commands::Metadata(args) => {
            component::execute_metadata(&args)?;
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

    fn parse_with_programmatic_mode(args: &[&str]) -> Cli {
        let mut cli = parse(args);
        if cli.programmatic {
            apply_programmatic_mode(&mut cli.command);
        }
        cli
    }

    #[test]
    fn programmatic_flag_defaults_version_to_json() {
        let cli = parse_with_programmatic_mode(&["spice", "-p", "version"]);
        assert!(cli.programmatic);

        let Commands::Version(args) = cli.command else {
            panic!("expected version command");
        };
        assert_eq!(args.output, OutputFormat::Json);
    }

    #[test]
    fn programmatic_flag_defaults_nested_outputs_to_json() {
        let cli = parse_with_programmatic_mode(&["spice", "-p", "query", "list"]);
        let Commands::Query(args) = cli.command else {
            panic!("expected query command");
        };
        let Some(query::QuerySubcommand::List { output, .. }) = args.command else {
            panic!("expected query list command");
        };
        assert_eq!(output, OutputFormat::Json);

        let cli = parse_with_programmatic_mode(&["spice", "-p", "cloud", "secrets", "list"]);
        let Commands::Cloud(cloud::CloudArgs {
            command: cloud::CloudCommands::Secrets(cloud::SecretsCommands::List(args)),
        }) = cli.command
        else {
            panic!("expected cloud secrets list command");
        };
        assert_eq!(args.output, OutputFormat::Json);
    }

    #[test]
    fn programmatic_error_codes_are_stable() {
        let error = spice::error::Error::InvalidArgument {
            message: "bad input".to_string(),
        };

        assert_eq!(programmatic_error_code(&error), "invalid_argument");
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
