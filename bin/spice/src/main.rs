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
    datasets, feedback, init, install, login, models, nsql, pods, query, refresh, run, search, sql,
    status, trace, upgrade, validate, version, workers,
};
use spice::output::OutputFormat;
use spice::{Result, RuntimeContext};
use spice_cloud_client::endpoints::{
    is_valid_region as is_valid_cloud_region, normalize_data_region,
};
use std::ffi::{OsStr, OsString};
use std::sync::LazyLock;
use tracing_subscriber::EnvFilter;

const DEFAULT_CLOUD_REGION: &str = "us-east-1";
const MACHINE_ERROR_SERIALIZATION_FAILED: &str = r#"{"status":"error","error":{"code":"error_serialization_failed","message":"Failed to serialize CLI error"}}"#;
const SQL_DIRECT_SHORTCUT_VALUE_FLAGS: &[&str] = &[
    "--endpoint",
    "--flight-endpoint",
    "--cache-control",
    "--client-tls-certificate-file",
    "--client-tls-key-file",
    "--headers",
    "--output",
    "-o",
];
const CHAT_DIRECT_SHORTCUT_VALUE_FLAGS: &[&str] = &[
    "--endpoint",
    "--headers",
    "--model",
    "-m",
    "--temperature",
    "--output",
    "-o",
];
static GLOBAL_VALUE_FLAGS: LazyLock<Vec<String>> = LazyLock::new(collect_global_value_flags);
static TOP_LEVEL_COMMANDS: LazyLock<Vec<String>> = LazyLock::new(|| {
    let mut commands = Vec::new();
    for command in Cli::command().get_subcommands() {
        commands.push(command.get_name().to_string());
        commands.extend(command.get_all_aliases().map(ToString::to_string));
    }
    commands
});

fn collect_global_value_flags() -> Vec<String> {
    Cli::command()
        .get_arguments()
        .filter(|arg| arg.is_global_set())
        .filter(|arg| {
            matches!(
                arg.get_action(),
                clap::ArgAction::Set | clap::ArgAction::Append
            )
        })
        .filter_map(|arg| arg.get_long().map(|long| format!("--{long}")))
        .collect()
}

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
    spice -sql \"show tables\"   # Run a single SQL query and exit
    spice -chat \"Summarize loaded datasets\" # Prompt the configured LLM and exit
  spice sql                  # Open an interactive SQL REPL

Direct shortcuts (`-sql`, `-p`, `-chat`) are root-level forms. Quote multi-word
queries and prompts so the shell passes them as one argument.

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

    /// Machine-readable mode for LLMs and automation: prefer JSON output where supported and always emit structured JSON errors.
    #[arg(long, alias = "programmatic", global = true)]
    machine: bool,

    /// API key used to authenticate with the runtime or Spice.ai Cloud.
    #[arg(long, global = true, env = "SPICE_API_KEY")]
    api_key: Option<String>,

    /// Target Spice.ai Cloud instead of a local runtime (requires --api-key).
    #[arg(long, global = true, action = clap::ArgAction::SetTrue)]
    cloud: bool,

    /// Spice.ai Cloud runtime endpoint region used with --cloud.
    #[arg(long, global = true, value_parser = parse_cloud_region, default_value = DEFAULT_CLOUD_REGION, requires = "cloud")]
    cloud_region: String,

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

    /// Open the Spice.ai community Slack to share feedback
    Feedback(feedback::FeedbackArgs),
}

fn main() {
    use std::io::IsTerminal;

    let args = normalize_direct_command_args(std::env::args_os());
    let mut cli = match Cli::try_parse_from(args) {
        Ok(cli) => cli,
        Err(error) => {
            if raw_args_enable_machine_mode() && should_write_machine_clap_error(&error) {
                let exit_code = error.exit_code();
                write_machine_clap_error(&error);
                std::process::exit(exit_code);
            }
            error.exit();
        }
    };

    if cli.machine {
        apply_machine_mode(&mut cli.command);
    }

    // Verbosity flag wins; otherwise honour RUST_LOG; otherwise default to info.
    let filter = if cli.verbose > 0 {
        if cli.verbose == 1 {
            EnvFilter::new("debug")
        } else {
            EnvFilter::new("trace")
        }
    } else if cli.machine {
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
        && !cli.machine
        && !matches!(cli.command, Commands::Version(_) | Commands::Completions(_))
        && !is_json_output(&cli.command)
    {
        eprintln!("Spice.ai OSS CLI {}", version::cli_version());
    }

    // Run the CLI
    let machine = cli.machine;
    if let Err(e) = run_cli(cli) {
        if machine {
            write_machine_error(&e);
        } else {
            tracing::error!("{e}");
        }
        std::process::exit(1);
    }
}

fn normalize_direct_command_args(args: impl IntoIterator<Item = OsString>) -> Vec<OsString> {
    let mut normalized = Vec::new();
    let mut args = args.into_iter().peekable();

    if let Some(program_name) = args.next() {
        normalized.push(program_name);
    }

    while let Some(arg) = args.next() {
        if arg == OsStr::new("-sql") {
            normalized.extend(normalize_direct_shortcut_args(
                args,
                "sql",
                "--query",
                SQL_DIRECT_SHORTCUT_VALUE_FLAGS,
            ));
            break;
        }

        if arg == OsStr::new("-p") || arg == OsStr::new("-chat") {
            normalized.extend(normalize_direct_shortcut_args(
                args,
                "chat",
                "--direct-prompt",
                CHAT_DIRECT_SHORTCUT_VALUE_FLAGS,
            ));
            break;
        }

        if arg == OsStr::new("--") {
            normalized.push(arg);
            normalized.extend(args);
            break;
        }

        if let Some(region) = cloud_region_from_equals(&arg) {
            normalized.push(OsString::from("--cloud"));
            normalized.push(OsString::from("--cloud-region"));
            normalized.push(OsString::from(region));
            continue;
        }

        if arg == OsStr::new("--cloud") {
            normalized.push(arg);
            if args
                .peek()
                .is_some_and(|value| should_treat_as_cloud_region(value.as_os_str()))
            {
                normalized.push(OsString::from("--cloud-region"));
                if let Some(region) = args.next() {
                    normalized.push(region);
                }
            }
            continue;
        }

        let arg_text = arg.to_string_lossy();
        let consumes_value = global_value_flag_consumes_value(&arg_text);
        let is_subcommand = !arg_text.starts_with('-');
        normalized.push(arg);

        if consumes_value {
            if let Some(value) = args.next() {
                normalized.push(value);
            }
            continue;
        }

        if is_subcommand {
            normalized.extend(normalize_cloud_region_flags(args));
            break;
        }
    }

    normalized
}

fn normalize_direct_shortcut_args(
    args: impl IntoIterator<Item = OsString>,
    command: &str,
    value_flag: &str,
    value_flags: &[&str],
) -> Vec<OsString> {
    let mut passthrough = Vec::new();
    let mut direct_value = None;
    let mut args = args.into_iter().peekable();

    while let Some(arg) = args.next() {
        if arg == OsStr::new("--") {
            if direct_value.is_none() {
                direct_value = args.next();
            } else {
                passthrough.push(arg);
            }
            passthrough.extend(args);
            break;
        }

        if let Some(region) = cloud_region_from_equals(&arg) {
            passthrough.push(OsString::from("--cloud"));
            passthrough.push(OsString::from("--cloud-region"));
            passthrough.push(OsString::from(region));
            continue;
        }

        if arg == OsStr::new("--cloud") {
            passthrough.push(arg);
            if args
                .peek()
                .is_some_and(|value| should_treat_as_cloud_region(value.as_os_str()))
            {
                passthrough.push(OsString::from("--cloud-region"));
                if let Some(region) = args.next() {
                    passthrough.push(region);
                }
            }
            continue;
        }

        let arg_text = arg.to_string_lossy();
        if direct_value.is_none() && !arg_text.starts_with('-') {
            direct_value = Some(arg);
            continue;
        }

        let consumes_value = global_value_flag_consumes_value(&arg_text)
            || direct_shortcut_flag_consumes_value(&arg_text, value_flags);
        passthrough.push(arg);

        if consumes_value && let Some(value) = args.next() {
            passthrough.push(value);
        }
    }

    let mut normalized = vec![OsString::from(command)];
    normalized.extend(passthrough);
    normalized.push(OsString::from(value_flag));
    if let Some(value) = direct_value {
        normalized.push(value);
    }
    normalized
}

fn normalize_cloud_region_flags(args: impl IntoIterator<Item = OsString>) -> Vec<OsString> {
    let mut normalized = Vec::new();
    let mut args = args.into_iter().peekable();

    while let Some(arg) = args.next() {
        if arg == OsStr::new("--") {
            normalized.push(arg);
            normalized.extend(args);
            break;
        }

        if let Some(region) = cloud_region_from_equals(&arg) {
            normalized.push(OsString::from("--cloud"));
            normalized.push(OsString::from("--cloud-region"));
            normalized.push(OsString::from(region));
            continue;
        }

        if arg == OsStr::new("--cloud") {
            normalized.push(arg);
            if args
                .peek()
                .is_some_and(|value| should_treat_as_cloud_region(value.as_os_str()))
            {
                normalized.push(OsString::from("--cloud-region"));
                if let Some(region) = args.next() {
                    normalized.push(region);
                }
            }
            continue;
        }

        normalized.push(arg);
    }

    normalized
}

fn parse_cloud_region(value: &str) -> std::result::Result<String, String> {
    if let Some(region) = normalize_data_region(value) {
        return Ok(region);
    }

    Err(format!(
        "invalid cloud region '{value}': expected lowercase letters, digits, and hyphens, starting and ending with a letter or digit"
    ))
}

// Legacy `--cloud <region>` syntax has no value marker, so only consume values
// that look like supported region shortcuts and never current command names.
fn should_treat_as_cloud_region(value: &OsStr) -> bool {
    value.to_str().is_some_and(|value| {
        looks_like_cloud_region_shortcut_value(value) && !is_top_level_command(value)
    })
}

fn looks_like_cloud_region_shortcut_value(value: &str) -> bool {
    looks_like_legacy_cloud_region(value) || looks_like_data_region_name(value)
}

fn looks_like_data_region_name(value: &str) -> bool {
    normalize_data_region(value).is_some_and(|region| region != value)
}

fn looks_like_legacy_cloud_region(value: &str) -> bool {
    if !is_valid_cloud_region(value) {
        return false;
    }

    let mut parts = value.split('-');
    let (Some(first), Some(second), Some(third)) = (parts.next(), parts.next(), parts.next())
    else {
        return false;
    };

    match parts.next() {
        None => legacy_region_parts_match(first, second, third),
        Some(fourth) if first == "us" && second == "gov" && parts.next().is_none() => {
            legacy_region_parts_match(first, third, fourth)
        }
        _ => false,
    }
}

fn legacy_region_parts_match(prefix: &str, area: &str, zone: &str) -> bool {
    prefix.len() == 2
        && prefix.chars().all(|c| c.is_ascii_lowercase())
        && !area.is_empty()
        && area.chars().all(|c| c.is_ascii_lowercase())
        && !zone.is_empty()
        && zone.chars().all(|c| c.is_ascii_digit())
}

fn direct_shortcut_flag_consumes_value(value: &str, value_flags: &[&str]) -> bool {
    !value.contains('=') && value_flags.contains(&value)
}

fn global_value_flag_consumes_value(value: &str) -> bool {
    !value.contains('=') && GLOBAL_VALUE_FLAGS.iter().any(|flag| flag == value)
}

fn cloud_region_from_equals(value: &OsStr) -> Option<&str> {
    let value = value.to_str()?;
    value
        .strip_prefix("--cloud=")
        .filter(|region| looks_like_cloud_region_shortcut_value(region))
}

fn is_top_level_command(value: &str) -> bool {
    TOP_LEVEL_COMMANDS.iter().any(|command| command == value)
}

fn raw_args_enable_machine_mode() -> bool {
    args_enable_machine_mode(std::env::args_os().skip(1))
}

fn args_enable_machine_mode(args: impl IntoIterator<Item = OsString>) -> bool {
    for arg in args {
        if arg == OsStr::new("--") {
            return false;
        }
        if arg == OsStr::new("--machine") || arg == OsStr::new("--programmatic") {
            return true;
        }
    }

    false
}

fn apply_machine_mode(command: &mut Commands) {
    // Keep this match explicit so adding a top-level command forces a machine-mode audit.
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
        Commands::Sql(args) => args.output = OutputFormat::Json,
        Commands::Query(args) => apply_machine_query_mode(args),
        Commands::Acceleration(args) => apply_machine_acceleration_mode(args),
        Commands::Chat(args) => args.output = OutputFormat::Json,
        Commands::Refresh(args) => args.output = OutputFormat::Json,
        Commands::Cloud(args) => apply_machine_cloud_mode(&mut args.command),
        // `Nsql` is intentionally excluded: it is always an interactive REPL with
        // no one-shot/non-interactive mode, so there is no JSON output format to apply.
        // The remaining commands are lifecycle/manifest-editing commands with no
        // structured output.
        Commands::Nsql(_)
        | Commands::Init(_)
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
        | Commands::Login(_)
        | Commands::Cluster(_)
        | Commands::Completions(_)
        | Commands::Feedback(_) => {}
    }
}

fn apply_machine_query_mode(args: &mut query::QueryArgs) {
    args.output = OutputFormat::Json;

    let Some(command) = &mut args.command else {
        return;
    };

    match command {
        query::QuerySubcommand::List { output, .. }
        | query::QuerySubcommand::Status { output, .. }
        | query::QuerySubcommand::Results { output, .. }
        | query::QuerySubcommand::Cancel { output, .. } => {
            *output = OutputFormat::Json;
        }
    }
}

fn apply_machine_acceleration_mode(args: &mut AccelerationArgs) {
    match &mut args.command {
        acceleration::AccelerationCommand::Snapshots(args) => args.output = OutputFormat::Json,
        acceleration::AccelerationCommand::Snapshot(args) => args.output = OutputFormat::Json,
        acceleration::AccelerationCommand::SetSnapshot(args) => args.output = OutputFormat::Json,
    }
}

fn apply_machine_cloud_mode(command: &mut cloud::CloudCommands) {
    match command {
        cloud::CloudCommands::Whoami(args) => args.output = OutputFormat::Json,
        cloud::CloudCommands::Apps(args) => args.output = OutputFormat::Json,
        cloud::CloudCommands::Deployments(args) => args.output = OutputFormat::Json,
        cloud::CloudCommands::Regions(args) => args.output = OutputFormat::Json,
        cloud::CloudCommands::Images(args) => args.output = OutputFormat::Json,
        cloud::CloudCommands::Logs(args) => args.output = OutputFormat::Json,
        cloud::CloudCommands::Deploy(args) => args.output = OutputFormat::Json,
        cloud::CloudCommands::Inspect(args) => args.output = OutputFormat::Json,
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

fn write_machine_clap_error(error: &clap::Error) {
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
        Err(_) => eprintln!("{MACHINE_ERROR_SERIALIZATION_FAILED}"),
    }
}

fn should_write_machine_clap_error(error: &clap::Error) -> bool {
    !matches!(
        error.kind(),
        clap::error::ErrorKind::DisplayHelp
            | clap::error::ErrorKind::DisplayVersion
            | clap::error::ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
    )
}

fn write_machine_error(error: &spice::error::Error) {
    let body = serde_json::json!({
        "status": "error",
        "error": {
            "code": machine_error_code(error),
            "message": error.to_string(),
        }
    });

    match serde_json::to_string(&body) {
        Ok(body) => eprintln!("{body}"),
        Err(_) => eprintln!("{MACHINE_ERROR_SERIALIZATION_FAILED}"),
    }
}

fn machine_error_code(error: &spice::error::Error) -> &'static str {
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
        spice::error::Error::DeviceAuthorizationDenied => "device_authorization_denied",
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
        Commands::Sql(a) => a.output == OutputFormat::Json,
        Commands::Query(a) => a.output == OutputFormat::Json,
        Commands::Chat(a) => a.output == OutputFormat::Json,
        Commands::Refresh(a) => a.output == OutputFormat::Json,
        Commands::Acceleration(AccelerationArgs {
            command:
                acceleration::AccelerationCommand::Snapshots(SnapshotsArgs { output, .. })
                | acceleration::AccelerationCommand::Snapshot(SnapshotArgs { output, .. })
                | acceleration::AccelerationCommand::SetSnapshot(acceleration::SetSnapshotArgs {
                    output,
                    ..
                }),
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
    let cloud_region = cli.cloud.then_some(cli.cloud_region.as_str());
    let ctx = RuntimeContext::with_args(
        Some(cli.http_endpoint),
        cli.api_key,
        cloud_region,
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

    fn parse_normalized(args: &[&str]) -> Cli {
        let args = normalize_direct_command_args(args.iter().map(OsString::from));
        Cli::try_parse_from(args).expect("failed to parse CLI args")
    }

    fn try_parse_normalized(args: &[&str]) -> std::result::Result<Cli, clap::Error> {
        let args = normalize_direct_command_args(args.iter().map(OsString::from));
        Cli::try_parse_from(args)
    }

    fn is_json(args: &[&str]) -> bool {
        is_json_output(&parse(args).command)
    }

    fn parse_with_machine_mode(args: &[&str]) -> Cli {
        let mut cli = parse(args);
        if cli.machine {
            apply_machine_mode(&mut cli.command);
        }
        cli
    }

    #[test]
    fn machine_flag_defaults_version_to_json() {
        let cli = parse_with_machine_mode(&["spice", "--machine", "version"]);
        assert!(cli.machine);

        let Commands::Version(args) = cli.command else {
            panic!("expected version command");
        };
        assert_eq!(args.output, OutputFormat::Json);
    }

    #[test]
    fn machine_flag_defaults_nested_outputs_to_json() {
        let cli = parse_with_machine_mode(&["spice", "--machine", "query", "list"]);
        let Commands::Query(args) = cli.command else {
            panic!("expected query command");
        };
        let Some(query::QuerySubcommand::List { output, .. }) = args.command else {
            panic!("expected query list command");
        };
        assert_eq!(output, OutputFormat::Json);

        let cli = parse_with_machine_mode(&["spice", "--machine", "cloud", "secrets", "list"]);
        let Commands::Cloud(cloud::CloudArgs {
            command: cloud::CloudCommands::Secrets(cloud::SecretsCommands::List(args)),
        }) = cli.command
        else {
            panic!("expected cloud secrets list command");
        };
        assert_eq!(args.output, OutputFormat::Json);
    }

    #[test]
    fn machine_flag_defaults_mutating_subcommands_to_json_when_supported() {
        let cli = parse_with_machine_mode(&["spice", "--machine", "query", "cancel", "query-1"]);
        let Commands::Query(args) = cli.command else {
            panic!("expected query command");
        };
        let Some(query::QuerySubcommand::Cancel { output, .. }) = args.command else {
            panic!("expected query cancel command");
        };
        assert_eq!(output, OutputFormat::Json);

        let cli = parse_with_machine_mode(&[
            "spice",
            "--machine",
            "acceleration",
            "set-snapshot",
            "taxi_trips",
            "42",
        ]);
        let Commands::Acceleration(AccelerationArgs {
            command: acceleration::AccelerationCommand::SetSnapshot(args),
        }) = cli.command
        else {
            panic!("expected acceleration set-snapshot command");
        };
        assert_eq!(args.output, OutputFormat::Json);
    }

    #[test]
    fn machine_flag_is_global_after_subcommands() {
        let cli = parse_with_machine_mode(&["spice", "status", "--machine"]);
        assert!(cli.machine);

        let Commands::Status(args) = cli.command else {
            panic!("expected status command");
        };
        assert_eq!(args.output, OutputFormat::Json);
    }

    #[test]
    fn machine_flag_defaults_direct_automation_outputs_to_json() {
        let cli = parse_with_machine_mode(&["spice", "--machine", "sql", "--query", "select 1"]);
        let Commands::Sql(args) = cli.command else {
            panic!("expected sql command");
        };
        assert_eq!(args.output, OutputFormat::Json);

        let cli = parse_with_machine_mode(&["spice", "--machine", "chat", "hello"]);
        let Commands::Chat(args) = cli.command else {
            panic!("expected chat command");
        };
        assert_eq!(args.output, OutputFormat::Json);

        let cli = parse_with_machine_mode(&["spice", "--machine", "refresh", "taxi_trips"]);
        let Commands::Refresh(args) = cli.command else {
            panic!("expected refresh command");
        };
        assert_eq!(args.output, OutputFormat::Json);
    }

    #[test]
    fn machine_flag_after_direct_shortcut_sets_json_output() {
        let mut cli = parse_normalized(&["spice", "-sql", "select 1", "--machine"]);
        assert!(cli.machine);
        apply_machine_mode(&mut cli.command);
        let Commands::Sql(args) = cli.command else {
            panic!("expected sql command");
        };
        assert_eq!(args.output, OutputFormat::Json);
    }

    #[test]
    fn machine_flag_before_direct_sql_text_is_not_consumed_as_query() {
        let mut cli = parse_normalized(&["spice", "-sql", "--machine", "select 1"]);
        assert!(cli.machine);
        apply_machine_mode(&mut cli.command);
        let Commands::Sql(args) = cli.command else {
            panic!("expected sql command");
        };
        assert_eq!(args.query.as_deref(), Some("select 1"));
        assert_eq!(args.output, OutputFormat::Json);
    }

    #[test]
    fn machine_error_codes_are_stable() {
        let error = spice::error::Error::InvalidArgument {
            message: "bad input".to_string(),
        };

        assert_eq!(machine_error_code(&error), "invalid_argument");
    }

    #[test]
    fn machine_mode_keeps_help_and_version_as_clap_display() {
        for args in [
            ["spice", "--machine", "--help"],
            ["spice", "--machine", "--version"],
        ] {
            let Err(error) = Cli::try_parse_from(args) else {
                panic!("help/version should be clap display");
            };
            assert!(
                !should_write_machine_clap_error(&error),
                "machine mode should not convert clap display output to JSON"
            );
        }
    }

    #[test]
    fn machine_mode_writes_json_for_real_parse_errors() {
        let Err(error) = Cli::try_parse_from(["spice", "--machine", "unknown-command"]) else {
            panic!("unknown command should be a parse error");
        };

        assert!(should_write_machine_clap_error(&error));
    }

    #[test]
    fn machine_mode_error_detection_is_lexical() {
        assert!(args_enable_machine_mode(
            ["--machine", "unknown-command"]
                .into_iter()
                .map(OsString::from)
        ));
        assert!(args_enable_machine_mode(
            ["unknown-command", "--programmatic"]
                .into_iter()
                .map(OsString::from)
        ));
        assert!(!args_enable_machine_mode(
            ["--", "--machine"].into_iter().map(OsString::from)
        ));
    }

    #[test]
    fn global_value_flags_match_cli_definition() {
        let actual = GLOBAL_VALUE_FLAGS.as_slice();
        let expected = [
            "--api-key",
            "--cloud-region",
            "--http-endpoint",
            "--tls-root-certificate-file",
        ]
        .map(ToString::to_string)
        .to_vec();

        assert_eq!(actual, expected.as_slice());
    }

    #[test]
    fn direct_sql_flag_normalizes_to_sql_query() {
        let cli = parse_normalized(&["spice", "-sql", "select 1"]);
        let Commands::Sql(args) = cli.command else {
            panic!("expected sql command");
        };
        assert_eq!(args.query.as_deref(), Some("select 1"));
    }

    #[test]
    fn direct_sql_flag_normalizes_after_global_options() {
        let cli = parse_normalized(&[
            "spice",
            "--http-endpoint",
            "http://127.0.0.1:8090",
            "-sql",
            "show tables",
        ]);
        assert_eq!(cli.http_endpoint, "http://127.0.0.1:8090");
        let Commands::Sql(args) = cli.command else {
            panic!("expected sql command");
        };
        assert_eq!(args.query.as_deref(), Some("show tables"));
    }

    #[test]
    fn cloud_flag_defaults_region_without_consuming_command() {
        let cli = parse_normalized(&["spice", "--cloud", "status"]);
        assert!(cli.cloud);
        assert_eq!(cli.cloud_region, DEFAULT_CLOUD_REGION);

        let Commands::Status(_) = cli.command else {
            panic!("expected status command");
        };
    }

    #[test]
    fn cloud_region_without_cloud_is_rejected() {
        let Err(error) = try_parse_normalized(&["spice", "--cloud-region", "us-west-2", "status"])
        else {
            panic!("cloud-region without cloud should fail parsing");
        };

        let message = error.to_string();
        assert!(message.contains("--cloud"));
        assert!(message.contains("--cloud-region"));
    }

    #[test]
    fn cloud_flag_accepts_legacy_region_value() {
        let cli = parse_normalized(&["spice", "--cloud", "us-west-2", "status"]);
        assert!(cli.cloud);
        assert_eq!(cli.cloud_region, "us-west-2");

        let Commands::Status(_) = cli.command else {
            panic!("expected status command");
        };
    }

    #[test]
    fn cloud_flag_accepts_legacy_unlisted_region_value() {
        let cli = parse_normalized(&["spice", "--cloud", "eu-central-1", "status"]);
        assert!(cli.cloud);
        assert_eq!(cli.cloud_region, "eu-central-1");

        let Commands::Status(_) = cli.command else {
            panic!("expected status command");
        };
    }

    #[test]
    fn cloud_flag_accepts_full_data_region_value() {
        let cli = parse_normalized(&["spice", "--cloud", "us-west-2-prod-aws-data", "status"]);
        assert!(cli.cloud);
        assert_eq!(cli.cloud_region, "us-west-2");

        let Commands::Status(_) = cli.command else {
            panic!("expected status command");
        };
    }

    #[test]
    fn cloud_flag_does_not_consume_arbitrary_value_as_region() {
        let Err(error) = try_parse_normalized(&["spice", "--cloud", "foo", "status"]) else {
            panic!("arbitrary value after --cloud should not be consumed as a region");
        };

        assert!(error.to_string().contains("unrecognized subcommand 'foo'"));
    }

    #[test]
    fn cloud_flag_does_not_consume_arbitrary_region_shaped_value() {
        let Err(error) = try_parse_normalized(&["spice", "--cloud", "foo-1", "status"]) else {
            panic!("arbitrary region-like value after --cloud should not be consumed");
        };

        assert!(
            error
                .to_string()
                .contains("unrecognized subcommand 'foo-1'")
        );
    }

    #[test]
    fn cloud_equals_rejects_arbitrary_region_value() {
        let Err(error) = try_parse_normalized(&["spice", "--cloud=foo", "status"]) else {
            panic!("arbitrary value in --cloud= should not be normalized as a region");
        };

        let message = error.to_string();
        assert!(message.contains("--cloud"));
        assert!(message.contains("foo"));
    }

    #[test]
    fn cloud_flag_accepts_equals_region_value() {
        let cli = parse_normalized(&["spice", "--cloud=us-west-2", "status"]);
        assert!(cli.cloud);
        assert_eq!(cli.cloud_region, "us-west-2");

        let Commands::Status(_) = cli.command else {
            panic!("expected status command");
        };
    }

    #[test]
    fn cloud_flag_accepts_equals_unlisted_region_value() {
        let cli = parse_normalized(&["spice", "--cloud=eu-central-1", "status"]);
        assert!(cli.cloud);
        assert_eq!(cli.cloud_region, "eu-central-1");

        let Commands::Status(_) = cli.command else {
            panic!("expected status command");
        };
    }

    #[test]
    fn cloud_flag_accepts_equals_full_data_region_value() {
        let cli = parse_normalized(&["spice", "--cloud=us-west-2-prod-aws-data", "status"]);
        assert!(cli.cloud);
        assert_eq!(cli.cloud_region, "us-west-2");

        let Commands::Status(_) = cli.command else {
            panic!("expected status command");
        };
    }

    #[test]
    fn cloud_flag_does_not_consume_top_level_command_as_region() {
        let cli = parse_normalized(&["spice", "--cloud", "models"]);
        assert!(cli.cloud);
        assert_eq!(cli.cloud_region, DEFAULT_CLOUD_REGION);

        let Commands::Models(_) = cli.command else {
            panic!("expected models command");
        };
    }

    #[test]
    fn top_level_commands_do_not_match_cloud_region_shortcut_shape() {
        for command in TOP_LEVEL_COMMANDS.iter() {
            assert!(
                !looks_like_cloud_region_shortcut_value(command),
                "top-level command '{command}' would be ambiguous after --cloud"
            );
        }
    }

    #[test]
    fn cloud_equals_rejects_invalid_region_syntax() {
        let Err(error) = try_parse_normalized(&["spice", "--cloud=bad_region", "status"]) else {
            panic!("invalid cloud region should fail parsing");
        };

        let message = error.to_string();
        assert!(message.contains("--cloud"));
        assert!(message.contains("bad_region"));
    }

    #[test]
    fn direct_sql_flag_keeps_trailing_global_options() {
        let cli = parse_normalized(&[
            "spice",
            "-sql",
            "show tables",
            "--http-endpoint",
            "http://127.0.0.1:8090",
        ]);
        assert_eq!(cli.http_endpoint, "http://127.0.0.1:8090");
        let Commands::Sql(args) = cli.command else {
            panic!("expected sql command");
        };
        assert_eq!(args.query.as_deref(), Some("show tables"));
    }

    #[test]
    fn direct_sql_flag_normalizes_trailing_cloud_region() {
        let cli = parse_normalized(&["spice", "-sql", "show tables", "--cloud", "us-west-2"]);
        assert!(cli.cloud);
        assert_eq!(cli.cloud_region, "us-west-2");

        let Commands::Sql(args) = cli.command else {
            panic!("expected sql command");
        };
        assert_eq!(args.query.as_deref(), Some("show tables"));
    }

    #[test]
    fn direct_shortcut_value_flags_are_scoped_to_target_command() {
        assert!(direct_shortcut_flag_consumes_value(
            "--cache-control",
            SQL_DIRECT_SHORTCUT_VALUE_FLAGS
        ));
        assert!(!direct_shortcut_flag_consumes_value(
            "--cache-control",
            CHAT_DIRECT_SHORTCUT_VALUE_FLAGS
        ));
        assert!(direct_shortcut_flag_consumes_value(
            "--temperature",
            CHAT_DIRECT_SHORTCUT_VALUE_FLAGS
        ));
        assert!(!direct_shortcut_flag_consumes_value(
            "--temperature",
            SQL_DIRECT_SHORTCUT_VALUE_FLAGS
        ));
    }

    #[test]
    fn direct_sql_flag_is_not_normalized_inside_subcommands() {
        let args = normalize_direct_command_args(
            ["spice", "sql", "-sql", "select 1"]
                .into_iter()
                .map(OsString::from),
        );

        assert_eq!(
            args,
            ["spice", "sql", "-sql", "select 1"]
                .into_iter()
                .map(OsString::from)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn direct_prompt_flag_normalizes_to_chat_message() {
        let cli = parse_normalized(&["spice", "-p", "Summarize loaded datasets"]);
        let Commands::Chat(args) = cli.command else {
            panic!("expected chat command");
        };
        assert!(args.direct_prompt);
        assert_eq!(args.message.as_deref(), Some("Summarize loaded datasets"));
    }

    #[test]
    fn direct_prompt_flag_keeps_model_option() {
        let cli = parse_normalized(&["spice", "-p", "--model", "llm", "Summarize loaded datasets"]);
        let Commands::Chat(args) = cli.command else {
            panic!("expected chat command");
        };
        assert!(args.direct_prompt);
        assert_eq!(args.model.as_deref(), Some("llm"));
        assert_eq!(args.message.as_deref(), Some("Summarize loaded datasets"));
    }

    #[test]
    fn direct_prompt_flag_keeps_trailing_global_options() {
        let cli = parse_normalized(&[
            "spice",
            "-p",
            "--model",
            "llm",
            "Summarize loaded datasets",
            "--api-key",
            "secret",
        ]);
        assert_eq!(cli.api_key.as_deref(), Some("secret"));
        let Commands::Chat(args) = cli.command else {
            panic!("expected chat command");
        };
        assert!(args.direct_prompt);
        assert_eq!(args.model.as_deref(), Some("llm"));
        assert_eq!(args.message.as_deref(), Some("Summarize loaded datasets"));
    }

    #[test]
    fn direct_chat_flag_normalizes_to_chat_message() {
        let cli = parse_normalized(&["spice", "-chat", "Summarize loaded datasets"]);
        let Commands::Chat(args) = cli.command else {
            panic!("expected chat command");
        };
        assert!(args.direct_prompt);
        assert_eq!(args.message.as_deref(), Some("Summarize loaded datasets"));
    }

    #[test]
    fn direct_chat_flag_keeps_model_option() {
        let cli = parse_normalized(&[
            "spice",
            "-chat",
            "--model",
            "llm",
            "Summarize loaded datasets",
        ]);
        let Commands::Chat(args) = cli.command else {
            panic!("expected chat command");
        };
        assert!(args.direct_prompt);
        assert_eq!(args.model.as_deref(), Some("llm"));
        assert_eq!(args.message.as_deref(), Some("Summarize loaded datasets"));
    }

    #[test]
    fn direct_prompt_flag_is_not_normalized_inside_subcommands() {
        let args = normalize_direct_command_args(
            ["spice", "chat", "-p", "Summarize loaded datasets"]
                .into_iter()
                .map(OsString::from),
        );

        assert_eq!(
            args,
            ["spice", "chat", "-p", "Summarize loaded datasets"]
                .into_iter()
                .map(OsString::from)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn direct_chat_flag_is_not_normalized_inside_subcommands() {
        let args = normalize_direct_command_args(
            ["spice", "chat", "-chat", "Summarize loaded datasets"]
                .into_iter()
                .map(OsString::from),
        );

        assert_eq!(
            args,
            ["spice", "chat", "-chat", "Summarize loaded datasets"]
                .into_iter()
                .map(OsString::from)
                .collect::<Vec<_>>()
        );
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
                "spice",
                "cloud",
                "create",
                "app",
                "name",
                "--region",
                "us-east-1",
                "--output",
                "json",
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
