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
pub(crate) mod client;
pub mod org;

use crate::commands::login::LoginOutput;
use crate::context::RuntimeContext;
use crate::error::{CloudErrorCode, Error, InvalidArgumentSnafu, Result};
use crate::output::{OutputFormat, TableOutput, write_json};
use clap::{Args, Subcommand};
use dialoguer::{Input, Password, Select, theme::ColorfulTheme};
use snafu::ResultExt;
use std::{collections::BTreeSet, fmt, io::IsTerminal};

pub use client::{
    CloudClient, ProjectTarget, is_device_authorization_denied_error, parse_org_project,
};
use runtime_cloud_connect::{CloudConnectConfig, identity::IdentityStore};
use spice_cloud_client::{
    endpoints::{data_region_name, normalize_data_region},
    types::{Deployment, IngestionMetrics, PodMetrics, ProjectKind, UpdateChannel},
};

/// Arguments for the cloud command.
#[derive(Args, Debug)]
#[command(
    about = "Manage Spice Cloud resources (apps, deployments, secrets, ...)",
    long_about = r#"Manage resources on Spice Cloud: authenticate, list and inspect
apps and deployments, manage secrets, view logs and metrics, and deploy.

Most subcommands require an active Spice Cloud session. Sign in with one of:
  spice cloud login subscription      # Browser-based subscription login
  spice cloud login token             # Access token (alias: pat)
  spice cloud login api               # OAuth client credentials (automation)

ORGANIZATIONS
Commands act on one organization at a time. Name it inline as `<org>/<app>`,
select it for the invocation with `--org`, or set it for good with
`spice cloud org use <org>`. `spice cloud whoami` shows which one is in effect.

EXAMPLES
  spice cloud whoami                        # Identity and active organization
  spice cloud orgs                          # Organizations you can act on
  spice cloud org use spicehq               # Make spicehq the active org
  spice cloud projects --org spicehq        # List projects in one org
  spice cloud deploy --project spicehq/team-app --wait
  spice cloud deployments --project spicehq/team-app
  spice cloud logs --project spicehq/team-app --level error
  spice cloud status --project spicehq/team-app        # Health in one command
  spice cloud link spicehq/team-app         # Default this directory to an app

Docs: https://spiceai.org/docs/spice-cloud"#
)]
pub struct CloudArgs {
    /// Organization to act on, overriding `SPICE_CLOUD_ORG` and the active org
    // Deliberately not bound to the env var via Clap's `env`: the CLI reads
    // `SPICE_CLOUD_ORG` itself so it can report which source chose the org, and
    // so the flag and the env var can be ranked rather than merged.
    #[arg(long, global = true, value_name = "ORG")]
    pub org: Option<String>,

    #[command(subcommand)]
    pub command: CloudCommands,
}

/// Cloud subcommands.
#[derive(Subcommand, Debug)]
pub enum CloudCommands {
    /// Login to Spice Cloud
    Login(LoginArgs),

    /// Logout from Spice Cloud
    Logout(LogoutArgs),

    /// Show current authenticated user
    Whoami(WhoamiArgs),

    /// List organizations this identity can act on
    Orgs(OrgsArgs),

    /// Show or change the active organization
    #[command(subcommand)]
    Org(OrgCommands),

    /// Link the current directory to a Spice Cloud project
    Link(LinkArgs),

    /// Unlink the current directory from its Spice Cloud project
    Unlink,

    /// List all projects
    #[command(alias = "apps")]
    Projects(ProjectsArgs),

    /// List deployments for a project
    Deployments(DeploymentsArgs),

    /// List available regions
    Regions(RegionsArgs),

    /// List available container images
    Images(ImagesArgs),

    /// Create, inspect, and change one project
    #[command(subcommand)]
    Project(ProjectCommands),

    /// Manage secrets for a project
    #[command(subcommand)]
    Secrets(SecretsCommands),

    /// View runtime logs
    Logs(LogsArgs),

    // `about`/`long_about` live on `DeployArgs`; a doc comment here would
    // shadow the long help that documents where the spicepod comes from.
    Deploy(DeployArgs),

    // `about`/`long_about` live on `StatusArgs`.
    Status(StatusArgs),

    /// Install and manage the persistent service for this enrolled directory
    Service(crate::commands::connect::service::cli::ServiceArgs),

    /// Show dataset load state for a project
    Datasets(DatasetsArgs),

    /// Show API keys for a project
    #[command(name = "api-keys")]
    ApiKeys(ApiKeysArgs),

    /// Show resource usage for a project's instances
    Metrics(MetricsArgs),

    // ── Superseded spellings ────────────────────────────────────────────────
    // Hidden from help but still parsed, so scripts written against the old
    // surface keep working for one release. Each warns and delegates to its
    // replacement rather than duplicating behavior — two implementations of one
    // command is how they drift apart.
    #[command(subcommand, hide = true)]
    Create(CreateCommands),

    #[command(subcommand, hide = true)]
    Get(GetCommands),

    #[command(subcommand, hide = true)]
    Update(UpdateCommands),

    #[command(subcommand, hide = true)]
    Delete(DeleteCommands),

    #[command(hide = true)]
    Inspect(InspectArgs),

    #[command(subcommand, hide = true)]
    Instance(InstanceCommands),
}

impl CloudCommands {
    /// The command's `--output` setting, when it produces structured output.
    ///
    /// Lives beside the enum so adding a subcommand requires one exhaustive
    /// match. Omitting structured output here would print the startup banner
    /// into `--machine` stdout and break `jq`.
    pub fn output_mut(&mut self) -> Option<&mut OutputFormat> {
        match self {
            Self::Whoami(a) => Some(&mut a.output),
            Self::Orgs(a) => Some(&mut a.output),
            Self::Projects(a) => Some(&mut a.output),
            Self::Deployments(a) => Some(&mut a.output),
            Self::Regions(a) => Some(&mut a.output),
            Self::Images(a) => Some(&mut a.output),
            Self::Logs(a) => Some(&mut a.output),
            Self::Deploy(a) => Some(&mut a.output),
            Self::Status(a) => Some(&mut a.output),
            Self::Datasets(a) => Some(&mut a.output),
            Self::ApiKeys(a) => Some(&mut a.output),
            Self::Metrics(a) => Some(&mut a.output),
            Self::Inspect(a) => Some(&mut a.output),
            Self::Org(OrgCommands::Use(a)) => Some(&mut a.output),
            Self::Org(OrgCommands::Current(a)) => Some(&mut a.output),
            Self::Secrets(
                SecretsCommands::List(SecretsListArgs { output, .. })
                | SecretsCommands::Get(SecretsGetArgs { output, .. })
                | SecretsCommands::Delete(SecretsDeleteArgs { output, .. }),
            ) => Some(output),
            Self::Secrets(SecretsCommands::Set(a)) => Some(&mut a.output),
            Self::Project(ProjectCommands::Create(a))
            | Self::Create(CreateCommands::Project(a)) => Some(&mut a.output),
            Self::Project(ProjectCommands::Get(a)) | Self::Get(GetCommands::Project(a)) => {
                Some(&mut a.output)
            }
            Self::Project(ProjectCommands::Update(a))
            | Self::Update(UpdateCommands::Project(a)) => Some(&mut a.output),
            Self::Project(ProjectCommands::Delete(a))
            | Self::Delete(DeleteCommands::Project(a)) => Some(&mut a.output),
            Self::Create(CreateCommands::Deployment(a)) => Some(&mut a.output),
            Self::Instance(InstanceCommands::List(a)) => Some(&mut a.output),
            Self::Instance(InstanceCommands::Status(a)) => Some(&mut a.output),
            Self::Instance(InstanceCommands::Datasets(a)) => Some(&mut a.output),
            Self::Login(_)
            | Self::Logout(_)
            | Self::Link(_)
            | Self::Unlink
            | Self::Service(_)
            | Self::Org(OrgCommands::Clear) => None,
        }
    }

    /// Whether this command will emit JSON, so the banner must stay off stdout.
    ///
    /// Takes `&mut self` so the single match above stays the only place the
    /// command tree is enumerated; callers already hold the parsed `Cli`
    /// mutably at this point.
    pub fn produces_json(&mut self) -> bool {
        matches!(self, Self::Login(args) if args.output == LoginOutput::Json)
            || self.output_mut().is_some_and(|o| *o == OutputFormat::Json)
    }
}

/// Operations on a single project.
#[derive(Subcommand, Debug)]
pub enum ProjectCommands {
    /// Create a new project
    Create(CreateProjectArgs),

    /// Show a project's configuration
    Get(GetProjectArgs),

    /// Change a project's configuration
    Update(UpdateProjectArgs),

    /// Delete a project
    Delete(DeleteProjectArgs),
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
pub struct ProjectsArgs {
    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

#[derive(Args, Debug)]
#[command(
    about = "Show whether a project is healthy",
    long_about = r#"Show whether a project is healthy.

Answers the whole "why is my project red?" question in one place: the project,
its latest deployment, every instance serving it with per-instance health, and
any dataset that is not loading. Without --instance the report comes from the
project's general endpoint and describes the deployment as a whole; with
--instance it is pinned to one instance.

EXAMPLES
  spice cloud status
  spice cloud status --project spicehq/team-app
  spice cloud status --project spicehq/team-app --instance spicepod-team-app-abc-0-0"#
)]
pub struct StatusArgs {
    /// Project name in org/project format (uses the enrolled instance's project if omitted)
    #[arg(long, alias = "app", value_name = "ORG/PROJECT")]
    pub project: Option<String>,

    /// Pin the report to one instance (default: the project's general endpoint)
    #[arg(long, value_name = "NAME")]
    pub instance: Option<String>,

    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

#[derive(Args, Debug)]
pub struct DatasetsArgs {
    /// Project name in org/project format (uses the enrolled instance's project if omitted)
    #[arg(long, alias = "app", value_name = "ORG/PROJECT")]
    pub project: Option<String>,

    /// Pin the report to one instance (default: the project's general endpoint)
    #[arg(long, value_name = "NAME")]
    pub instance: Option<String>,

    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

#[derive(Args, Debug)]
pub struct OrgsArgs {
    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

/// Which stored sessions `spice cloud logout` discards.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
#[clap(rename_all = "snake_case")]
pub enum LogoutScope {
    /// Forget the credential for the organization in effect (default).
    #[default]
    Active,
    /// Forget every stored organization credential on this machine.
    All,
}

#[derive(Args, Debug)]
pub struct LogoutArgs {
    /// Which stored sessions to discard.
    #[arg(long, value_enum, default_value = "active")]
    pub scope: LogoutScope,
}

#[derive(Subcommand, Debug)]
pub enum OrgCommands {
    /// Set the organization subsequent commands act on
    #[command(alias = "switch")]
    Use(OrgUseArgs),

    /// Show the organization in effect
    Current(OrgCurrentArgs),

    /// Return to the organization the credential was issued for
    Clear,
}

#[derive(Args, Debug)]
pub struct OrgUseArgs {
    /// Organization name
    pub org: String,

    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

#[derive(Args, Debug)]
pub struct OrgCurrentArgs {
    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

#[derive(Subcommand, Debug)]
#[command(
    about = "Superseded by 'spice cloud status' and 'spice cloud datasets'",
    long_about = r#"Superseded. Use 'spice cloud status' and 'spice cloud datasets'.

These spellings still work but will be removed in a future release.

  spice cloud instance list      →  spice cloud status
  spice cloud instance status    →  spice cloud status
  spice cloud instance datasets  →  spice cloud datasets

Both replacements take --instance to pin the report to one instance."#
)]
pub enum InstanceCommands {
    /// List the instances serving a project
    #[command(visible_alias = "ls")]
    List(InstanceListArgs),

    /// Show component readiness for a project
    Status(InstanceStatusArgs),

    /// Show dataset load state for a project
    Datasets(InstanceDatasetsArgs),
}

#[derive(Args, Debug)]
pub struct InstanceListArgs {
    /// Project name in org/project format (uses the enrolled instance's project if omitted)
    #[arg(long, alias = "app", value_name = "ORG/PROJECT")]
    pub project: Option<String>,

    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

#[derive(Args, Debug)]
pub struct InstanceStatusArgs {
    /// Project name in org/project format (uses the enrolled instance's project if omitted)
    #[arg(long, alias = "app", value_name = "ORG/PROJECT")]
    pub project: Option<String>,

    /// Pin the request to one instance (default: the app's general endpoint)
    #[arg(long, value_name = "NAME")]
    pub instance: Option<String>,

    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

#[derive(Args, Debug)]
pub struct InstanceDatasetsArgs {
    /// Project name in org/project format (uses the enrolled instance's project if omitted)
    #[arg(long, alias = "app", value_name = "ORG/PROJECT")]
    pub project: Option<String>,

    /// Pin the request to one instance (default: the app's general endpoint)
    #[arg(long, value_name = "NAME")]
    pub instance: Option<String>,

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
    /// Where to store or print the resulting credentials.
    #[arg(long, short = 'o', default_value = "env")]
    pub output: LoginOutput,

    #[command(subcommand)]
    pub method: Option<LoginMethod>,
}

impl fmt::Debug for LoginArgs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LoginArgs")
            .field("output", &self.output)
            .field("method", &self.method)
            .finish()
    }
}

#[derive(Subcommand)]
pub enum LoginMethod {
    /// Log in with your Spice Cloud subscription in a browser
    Subscription(SubscriptionLoginArgs),

    /// Log in with a Spice Cloud access token
    #[command(alias = "pat")]
    Token(TokenLoginArgs),

    /// Log in with OAuth client credentials for automation
    Api(ApiLoginArgs),
}

impl fmt::Debug for LoginMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Subscription(args) => f.debug_tuple("Subscription").field(args).finish(),
            Self::Token(args) => f.debug_tuple("Token").field(args).finish(),
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
pub struct TokenLoginArgs {
    /// Access token. Omit to enter it securely.
    #[arg(
        long,
        env = "SPICE_CLOUD_PAT",
        value_name = "TOKEN",
        help_heading = "Access Token Login Options"
    )]
    pub token: Option<String>,
}

impl fmt::Debug for TokenLoginArgs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenLoginArgs")
            .field("token", &self.token.as_deref().map(|_| "[REDACTED]"))
            .finish()
    }
}

/// Where a credential is filed and which organization it must serve.
///
/// User access tokens may serve every organization the user belongs to;
/// machine credentials remain organization-bound. The server verifies the
/// requested organization before the credential is stored under that name.
struct LoginTarget<'a> {
    requested_org: Option<&'a str>,
    output: LoginOutput,
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
    /// Project name in org/project format. Omit to choose interactively.
    pub project: Option<String>,
}

#[derive(Args, Debug)]
pub struct DeploymentsArgs {
    /// Project name in org/project format (uses the enrolled instance's project if omitted)
    #[arg(long, alias = "app", value_name = "ORG/PROJECT")]
    pub project: Option<String>,

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

/// Minimum severity of log entries to show.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
#[clap(rename_all = "snake_case")]
pub enum LogLevelFilter {
    /// Every entry the runtime emitted (default).
    #[default]
    All,
    /// Warnings and errors.
    Warn,
    /// Errors only.
    Error,
}

impl LogLevelFilter {
    /// Whether an entry's level passes this filter. Entries with no level are
    /// always shown: dropping them could hide the failure being investigated.
    fn admits(self, level: Option<&str>) -> bool {
        let Some(level) = level else {
            return true;
        };
        match self {
            Self::All => true,
            Self::Warn => level.eq_ignore_ascii_case("warn") || level.eq_ignore_ascii_case("error"),
            Self::Error => level.eq_ignore_ascii_case("error"),
        }
    }
}

#[derive(Args, Debug)]
pub struct LogsArgs {
    /// Project name in org/project format (uses the enrolled instance's project if omitted)
    #[arg(long, alias = "app", value_name = "ORG/PROJECT")]
    pub project: Option<String>,

    /// Maximum number of log entries to request per runtime instance
    #[arg(long, visible_alias = "tail", default_value = "100")]
    pub limit: usize,

    /// Only show entries at or above this severity
    #[arg(long, value_enum, default_value = "all")]
    pub level: LogLevelFilter,

    /// Only show entries after this RFC 3339 timestamp
    #[arg(long)]
    pub since: Option<String>,

    /// Follow logs in real-time
    #[arg(short, long)]
    pub follow: bool,

    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

#[derive(Args, Debug)]
#[command(
    about = "Deploy a project",
    long_about = r#"Deploy an app on Spice Cloud.

Spice Cloud deploys the project's stored Spicepod. `spice cloud link` seeds it
from the local Spicepod only when the project has none; later local edits are
not synchronized. Use `spice cloud project update --spicepod <path>` to replace
the stored Spicepod explicitly, or `--branch` / `--commit` to deploy a
different repository revision.

EXAMPLES
  spice cloud deploy --project spicehq/team-app
  spice cloud deploy --project spicehq/team-app --wait --timeout 15m
  spice cloud deploy --project spicehq/team-app --branch release --replicas 2"#
)]
pub struct DeployArgs {
    /// Project name in org/project format (uses the enrolled instance's project if omitted)
    #[arg(long, alias = "app", value_name = "ORG/PROJECT")]
    pub project: Option<String>,

    /// Container image tag to deploy
    #[arg(long)]
    pub image: Option<String>,

    /// Git branch to deploy the spicepod from (defaults to the app's production branch)
    #[arg(long)]
    pub branch: Option<String>,

    /// Git commit SHA to deploy the spicepod from
    #[arg(long, value_name = "SHA")]
    pub commit: Option<String>,

    /// Number of replicas
    #[arg(long)]
    pub replicas: Option<i32>,

    /// Enable debug mode
    #[arg(long)]
    pub debug: bool,

    /// Wait for the deployment to reach a terminal status before returning
    #[arg(long)]
    pub wait: bool,

    /// How long to wait with --wait (e.g. 5m, 90s)
    #[arg(long, value_parser = parse_window, default_value = "10m")]
    pub timeout: String,

    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

#[derive(Args, Debug)]
pub struct InspectArgs {
    /// Project name in org/project format (uses the enrolled instance's project if omitted)
    #[arg(long, alias = "app", value_name = "ORG/PROJECT")]
    pub project: Option<String>,

    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

#[derive(Args, Debug)]
pub struct ApiKeysArgs {
    /// Project name in org/project format (uses the enrolled instance's project if omitted)
    #[arg(long, alias = "app", value_name = "ORG/PROJECT")]
    pub project: Option<String>,

    /// Regenerate API key (1 or 2)
    #[arg(long)]
    pub regenerate: Option<u8>,

    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

#[derive(Args, Debug)]
pub struct MetricsArgs {
    /// Project name in org/project format (uses the enrolled instance's project if omitted)
    #[arg(long, alias = "app", value_name = "ORG/PROJECT")]
    pub project: Option<String>,

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
    /// Project name in org/project format (uses the enrolled instance's project if omitted)
    #[arg(long, alias = "app", value_name = "ORG/PROJECT")]
    pub project: Option<String>,

    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

#[derive(Args, Debug)]
pub struct SecretsSetArgs {
    /// Project name in org/project format (uses the enrolled instance's project if omitted)
    #[arg(long, alias = "app", value_name = "ORG/PROJECT")]
    pub project: Option<String>,

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
    /// Project name in org/project format (uses the enrolled instance's project if omitted)
    #[arg(long, alias = "app", value_name = "ORG/PROJECT")]
    pub project: Option<String>,

    /// Secret name
    pub name: String,

    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

#[derive(Args, Debug)]
pub struct SecretsDeleteArgs {
    /// Project name in org/project format (uses the enrolled instance's project if omitted)
    #[arg(long, alias = "app", value_name = "ORG/PROJECT")]
    pub project: Option<String>,

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
    /// Create a new project
    Project(CreateProjectArgs),

    /// Create a new deployment
    Deployment(CreateDeploymentArgs),
}

#[derive(Args, Debug)]
pub struct CreateProjectArgs {
    /// Project name
    pub name: String,

    /// Deployment region (e.g. us-east-1-prod-aws-data)
    #[arg(long, value_parser = parse_create_project_region)]
    pub region: String,

    /// Project kind (set or cluster)
    #[arg(long, value_parser = clap::value_parser!(ProjectKind), default_value = "set")]
    pub kind: ProjectKind,

    /// Project description
    #[arg(long)]
    pub description: Option<String>,

    /// Project visibility (public or private)
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
    /// Project name in org/project format (uses the enrolled instance's project if omitted)
    #[arg(long, alias = "app", value_name = "ORG/PROJECT")]
    pub project: Option<String>,

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
    /// Get project details
    Project(GetProjectArgs),
}

#[derive(Args, Debug)]
pub struct GetProjectArgs {
    /// Project name in org/project format
    pub project: String,

    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

// ============================================================================
// Update subcommands
// ============================================================================

#[derive(Subcommand, Debug)]
pub enum UpdateCommands {
    /// Update a project
    Project(UpdateProjectArgs),
}

#[derive(Args, Debug)]
pub struct UpdateProjectArgs {
    /// Project name in org/project format (uses the enrolled instance's project if omitted)
    #[arg(long, alias = "app", value_name = "ORG/PROJECT")]
    pub project: Option<String>,

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
    /// Delete a project
    Project(DeleteProjectArgs),
}

#[derive(Args, Debug)]
pub struct DeleteProjectArgs {
    /// Project name in org/project format
    pub project: String,

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
pub async fn execute(ctx: &RuntimeContext, args: &CloudArgs) -> Result<()> {
    if let Some(org) = args.org.as_deref() {
        org::validate_org_name(org)?;
    }
    let org = args.org.as_deref();

    match &args.command {
        CloudCommands::Login(login_args) => execute_login(login_args, org).await,
        CloudCommands::Logout(logout_args) => execute_logout(logout_args, org),
        CloudCommands::Whoami(whoami_args) => execute_whoami(whoami_args, org).await,
        CloudCommands::Orgs(orgs_args) => execute_orgs(orgs_args, org).await,
        CloudCommands::Org(org_cmd) => execute_org(org_cmd, org).await,
        CloudCommands::Link(link_args) => execute_link(ctx, link_args, org).await,
        CloudCommands::Unlink => execute_unlink().await,
        CloudCommands::Projects(apps_args) => execute_projects(apps_args, org).await,
        CloudCommands::Deployments(deploy_args) => execute_deployments(deploy_args, org).await,
        CloudCommands::Regions(regions_args) => execute_regions(regions_args, org).await,
        CloudCommands::Images(images_args) => execute_images(images_args, org).await,
        CloudCommands::Project(project_cmd) => execute_project(project_cmd, org).await,
        CloudCommands::Secrets(secrets_cmd) => execute_secrets(secrets_cmd, org).await,
        CloudCommands::Logs(logs_args) => execute_logs(ctx, logs_args, org).await,
        CloudCommands::Deploy(deploy_args) => execute_deploy(deploy_args, org).await,
        CloudCommands::Status(status_args) => execute_status(ctx, status_args, org).await,
        CloudCommands::Service(service_args) => execute_service(ctx, service_args).await,
        CloudCommands::Datasets(datasets_args) => execute_datasets(ctx, datasets_args, org).await,
        CloudCommands::ApiKeys(api_keys_args) => execute_api_keys(api_keys_args, org).await,
        CloudCommands::Metrics(metrics_args) => execute_metrics(metrics_args, org).await,

        // Superseded spellings: warn, then delegate to the replacement.
        CloudCommands::Create(create_cmd) => match create_cmd {
            CreateCommands::Project(args) => {
                warn_superseded("create project", "project create");
                execute_project_create(args, org).await
            }
            CreateCommands::Deployment(args) => {
                warn_superseded("create deployment", "deploy");
                execute_create_deployment(args, org).await
            }
        },
        CloudCommands::Get(GetCommands::Project(args)) => {
            warn_superseded("get project", "project get");
            execute_project_get(args, org).await
        }
        CloudCommands::Update(UpdateCommands::Project(args)) => {
            warn_superseded("update project", "project update");
            execute_project_update(args, org).await
        }
        CloudCommands::Delete(DeleteCommands::Project(args)) => {
            warn_superseded("delete project", "project delete");
            execute_project_delete(args, org).await
        }
        CloudCommands::Inspect(inspect_args) => {
            warn_superseded("inspect", "status");
            execute_status(
                ctx,
                &StatusArgs {
                    project: inspect_args.project.clone(),
                    instance: None,
                    output: inspect_args.output,
                },
                org,
            )
            .await
        }
        CloudCommands::Instance(instance_cmd) => {
            let replacement = match instance_cmd {
                InstanceCommands::List(_) | InstanceCommands::Status(_) => "status",
                InstanceCommands::Datasets(_) => "datasets",
            };
            warn_superseded("instance", replacement);
            match instance_cmd {
                InstanceCommands::List(args) => {
                    execute_status(
                        ctx,
                        &StatusArgs {
                            project: args.project.clone(),
                            instance: None,
                            output: args.output,
                        },
                        org,
                    )
                    .await
                }
                InstanceCommands::Status(args) => {
                    execute_status(
                        ctx,
                        &StatusArgs {
                            project: args.project.clone(),
                            instance: args.instance.clone(),
                            output: args.output,
                        },
                        org,
                    )
                    .await
                }
                InstanceCommands::Datasets(args) => {
                    execute_datasets(
                        ctx,
                        &DatasetsArgs {
                            project: args.project.clone(),
                            instance: args.instance.clone(),
                            output: args.output,
                        },
                        org,
                    )
                    .await
                }
            }
        }
    }
}

async fn execute_project(cmd: &ProjectCommands, flag_org: Option<&str>) -> Result<()> {
    match cmd {
        ProjectCommands::Create(args) => execute_project_create(args, flag_org).await,
        ProjectCommands::Get(args) => execute_project_get(args, flag_org).await,
        ProjectCommands::Update(args) => execute_project_update(args, flag_org).await,
        ProjectCommands::Delete(args) => execute_project_delete(args, flag_org).await,
    }
}

/// One report answering "is this project healthy?".
///
/// Aggregates project metadata, the latest deployment, every instance with its
/// health, and any dataset that is not loading. Aggregation is the default and
/// narrowing is the flag, matching
/// `kubectl describe --show-events` and `fly status`; an operator diagnosing an
/// outage should not have to know which of five commands holds the answer.
async fn execute_status(
    ctx: &RuntimeContext,
    args: &StatusArgs,
    flag_org: Option<&str>,
) -> Result<()> {
    let current = std::env::current_dir().map_err(|source| Error::CloudConnectIo {
        message: format!("resolve the current instance directory for Cloud status: {source}"),
    })?;
    let instance_dir =
        tokio::fs::canonicalize(&current)
            .await
            .map_err(|source| Error::CloudConnectIo {
                message: format!(
                    "canonicalize the current instance directory {}: {source}",
                    current.display()
                ),
            })?;
    let config_dir = CloudConnectConfig::resolve_config_dir(Some(&instance_dir));
    let local = crate::commands::connect::status::ConnectStatus::collect(
        &instance_dir,
        &config_dir,
        &client::get_base_url(),
    )
    .await;
    let local_degradation = local.degradation();
    let (target, org_source) =
        resolve_project_target_with_source(args.project.as_deref(), flag_org).await?;
    let client = connect_for_target(&target).await?;

    let project = client.get_project(&target).await?;
    let deployments = client.list_deployments(&target, 1, None).await?;
    let latest = deployments.first();

    // Instance and dataset health live on the data plane. A project that has
    // never deployed has no runtime to ask, and that legitimately reads as an
    // empty fleet. Every other failure — unreachable, unauthorized, malformed
    // — is reported: rendering it as "no instances are running" would make the
    // diagnosis command lie about the thing it exists to diagnose.
    let never_deployed = latest.is_none();
    let mut runtime_error: Option<String> = None;

    let (instances, datasets) =
        match project_runtime_context(ctx, &client, &target, args.instance.as_deref()).await {
            Ok(runtime_ctx) => {
                let instances = fetch_instance_json::<SpicepodStatusResponse>(
                    &runtime_ctx,
                    "/v1/spice_runtime",
                    &target,
                )
                .await;
                let datasets = fetch_instance_json::<Vec<InstanceDatasetInfo>>(
                    &runtime_ctx,
                    "/v1/datasets?status=true",
                    &target,
                )
                .await;

                for failure in [instances.as_ref().err(), datasets.as_ref().err()] {
                    if let Some(err) = failure
                        && runtime_error.is_none()
                    {
                        runtime_error = Some(err.to_string());
                    }
                }

                (
                    instances
                        .map(|response| response.status.pod_statuses)
                        .unwrap_or_default(),
                    datasets.unwrap_or_default(),
                )
            }
            Err(err) => {
                if !never_deployed {
                    runtime_error = Some(err.to_string());
                }
                (Vec::new(), Vec::new())
            }
        };

    let unhealthy: Vec<&InstanceDatasetInfo> = datasets
        .iter()
        .filter(|dataset| dataset_needs_attention(dataset.status.as_deref()))
        .collect();

    if args.output == OutputFormat::Json {
        write_json(&serde_json::json!({
            "schema_version": local.schema_version,
            "project": project,
            "org": target.org,
            "instance": args.instance,
            "latest_deployment": latest,
            "instances": instances,
            "datasets_total": datasets.len(),
            "datasets_unhealthy": unhealthy,
            "runtime_error": runtime_error,
            "link": {
                "connection": local.connection,
                "service": local.service,
                "deployment": local.deployment,
            },
        }))?;
        return match local_degradation {
            Some(message) => Err(Error::ServiceUnavailable { message }),
            None => Ok(()),
        };
    }

    println!("{}", describe_scope(&target, args.instance.as_deref()));
    if target.org.is_some() && org_source != OrgSource::ProjectArgument {
        println!("  organization from {}", org_source.label());
    }
    if let Some(region) = &project.region {
        println!("  region {region}");
    }

    match latest {
        Some(deployment) => {
            let age = deployment
                .created_at
                .as_deref()
                .map_or(String::new(), |created| format!(" ({created})"));
            println!("  deployment {} {}{age}", deployment.id, deployment.status);
            if let Some(error) = &deployment.error_message {
                println!("  error: {error}");
            }
        }
        None => println!("  no deployments yet"),
    }

    println!();
    if let Some(err) = &runtime_error {
        // Say the fleet is unknown rather than empty.
        println!("Could not read instance or dataset health: {err}");
        println!("  Instance and dataset state below may be incomplete.");
        println!();
    }
    if instances.is_empty() {
        if runtime_error.is_some() {
            println!("Instances: unknown.");
        } else {
            println!("No instances are running.");
        }
    } else {
        let mut table = TableOutput::new(vec!["INSTANCE", "STATUS", "DEPLOYMENT", "STARTED"]);
        for instance in &instances {
            table.add_row(vec![
                instance.name.clone(),
                instance.status().to_string(),
                instance.deployment_id().unwrap_or("-").to_string(),
                instance
                    .start_time
                    .clone()
                    .unwrap_or_else(|| "-".to_string()),
            ]);
        }
        table.print();
        let serving = instances.iter().filter(|i| i.is_serving()).count();
        println!("{serving}/{} instances serving.", instances.len());
    }

    if !datasets.is_empty() {
        println!();
        if unhealthy.is_empty() {
            println!("{} datasets, all loaded.", datasets.len());
        } else {
            // Only the datasets that need attention: a full listing here would
            // bury the one that is broken. `spice cloud datasets` shows them all.
            println!(
                "{}/{} datasets need attention:",
                unhealthy.len(),
                datasets.len()
            );
            for dataset in &unhealthy {
                let detail = dataset
                    .error_message
                    .as_deref()
                    .map_or(String::new(), |error| {
                        format!(" — {}", truncate_for_table(error, 60))
                    });
                println!(
                    "  {} {}{detail}",
                    dataset.name,
                    dataset.status.as_deref().unwrap_or("unknown")
                );
            }
            println!();
            println!("  Logs: spice cloud logs --project {target} --level error");
        }
    }

    println!();
    println!("Local enrolled-instance state:");
    crate::commands::connect::status::render(&local, OutputFormat::Table)?;
    match local_degradation {
        Some(message) => Err(Error::ServiceUnavailable { message }),
        None => Ok(()),
    }
}

async fn execute_datasets(
    ctx: &RuntimeContext,
    args: &DatasetsArgs,
    flag_org: Option<&str>,
) -> Result<()> {
    let target = resolve_project_target(args.project.as_deref(), flag_org).await?;
    let client = connect_for_target(&target).await?;
    let runtime_ctx =
        project_runtime_context(ctx, &client, &target, args.instance.as_deref()).await?;

    let datasets = fetch_instance_json::<Vec<InstanceDatasetInfo>>(
        &runtime_ctx,
        "/v1/datasets?status=true",
        &target,
    )
    .await?;

    print_instance_datasets(&datasets, &target, args.instance.as_deref(), args.output)
}

/// Tell the user a spelling has moved, once, before the command runs.
///
/// Written through `tracing` so `--machine` mode (which sets the filter to
/// `off`) keeps stdout a single clean JSON document.
fn warn_superseded(old: &str, new: &str) {
    // Deliberately stderr, not `tracing`: this CLI's subscriber writes to
    // stdout, so a superseded command run with `-o json` would emit this line
    // before its JSON and break `jq` — the exact compatibility the superseded
    // spellings exist to preserve.
    eprintln!(
        "warning: 'spice cloud {old}' is now 'spice cloud {new}'. The old spelling still works but will be removed in a future release."
    );
}

// ============================================================================
// Organization context
// ============================================================================

/// Where the organization a command acts on was chosen.
///
/// Ordered most authoritative first, matching the standard CLI configuration
/// ladder (flags → environment → project config → user config); see
/// <https://clig.dev/#configuration>.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrgSource {
    /// The `<org>/<app>` argument named it outright.
    ProjectArgument,
    /// The `--org` flag.
    Flag,
    /// The `SPICE_CLOUD_ORG` environment variable.
    Environment,
    /// The attachment stored with this directory's enrolled identity.
    LinkedApp,
    /// The persisted active org, set by `spice cloud org use`.
    ActiveOrg,
    /// Nothing named one, so the credential's own org applies.
    Credential,
}

impl OrgSource {
    /// A short label naming the source in user-facing output.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::ProjectArgument => "project argument",
            Self::Flag => "--org flag",
            Self::Environment => org::ACTIVE_ORG_VAR,
            Self::LinkedApp => "enrolled instance",
            Self::ActiveOrg => "active org",
            Self::Credential => "credential",
        }
    }
}

/// The organization in effect, and where it came from, ignoring any `org/app`
/// argument (which [`resolve_project_target`] layers on top).
///
/// Precedence: `--org` flag, then `SPICE_CLOUD_ORG`, then the persisted active
/// org. When none of those name an org the credential's own org is used and
/// this returns `None`.
fn resolve_org_with_source(flag_org: Option<&str>) -> Result<(Option<String>, OrgSource)> {
    if let Some(org) = flag_org {
        org::validate_org_name(org)?;
        return Ok((Some(org.to_string()), OrgSource::Flag));
    }

    if let Ok(org) = std::env::var(org::ACTIVE_ORG_VAR)
        && !org.is_empty()
    {
        org::validate_org_name(&org)?;
        return Ok((Some(org), OrgSource::Environment));
    }

    match org::active_org()? {
        Some(org) => Ok((Some(org), OrgSource::ActiveOrg)),
        None => Ok((None, OrgSource::Credential)),
    }
}

/// The organization in effect for commands that do not name an app.
fn resolve_org(flag_org: Option<&str>) -> Result<Option<String>> {
    Ok(resolve_org_with_source(flag_org)?.0)
}

/// Resolve which app a command acts on, and where its org came from.
///
/// Follows the standard configuration ladder — flags beat the environment,
/// which beats project config, which beats user config
/// (<https://clig.dev/#configuration>). Two signals that a user stated
/// explicitly are never silently ranked against each other: if `--app
/// <org>/<app>` and `--org` name different orgs, or a linked directory
/// disagrees with an explicit `--org`, the command fails and names both. A
/// wrong-organization deploy is not recoverable by re-reading the scrollback.
async fn resolve_project_target_with_source(
    app_flag: Option<&str>,
    flag_org: Option<&str>,
) -> Result<(ProjectTarget, OrgSource)> {
    let (explicit_org, explicit_source) = if let Some(org) = flag_org {
        org::validate_org_name(org)?;
        (Some(org.to_string()), OrgSource::Flag)
    } else if let Ok(org) = std::env::var(org::ACTIVE_ORG_VAR)
        && !org.is_empty()
    {
        org::validate_org_name(&org)?;
        (Some(org), OrgSource::Environment)
    } else {
        (None, OrgSource::Credential)
    };

    let config_dir = CloudConnectConfig::resolve_config_dir(None);
    let identity_path = config_dir.join(runtime_cloud_connect::config::IDENTITY_FILE);
    let identity = IdentityStore::load_optional_async(identity_path.clone())
        .await
        .map_err(|source| Error::CloudConnectIo {
            message: format!(
                "read the enrolled identity at {} while resolving the Cloud project: {source}",
                identity_path.display()
            ),
        })?;
    let attached_org = identity
        .as_ref()
        .and_then(|identity| identity.org_name.as_deref())
        .filter(|org| !org.is_empty());
    let (default_org, default_source) = if let Some(org) = explicit_org.clone() {
        (Some(org), explicit_source)
    } else if let Some(org) = attached_org {
        (Some(org.to_string()), OrgSource::LinkedApp)
    } else {
        resolve_org_with_source(None)?
    };

    // An `<org>/<app>` argument names the app completely and outranks everything.
    if let Some(app_flag) = app_flag {
        let (path_org, app) = parse_org_project(app_flag);
        if app.is_empty() {
            return Err(Error::cloud_with_hint(
                CloudErrorCode::InvalidRequest,
                format!("Invalid app name '{app_flag}': expected <app> or <org>/<app>."),
                "Run 'spice cloud projects' to list the apps you can reach.",
            ));
        }

        let Some(path_org) = path_org else {
            // A bare app name inherits whatever org is in effect.
            return Ok((ProjectTarget::new(default_org, app), default_source));
        };

        org::validate_org_name(&path_org)?;
        ensure_orgs_agree(
            &path_org,
            "the app argument",
            explicit_org.as_deref(),
            explicit_source,
        )?;
        return Ok((
            ProjectTarget::new(Some(path_org), app),
            OrgSource::ProjectArgument,
        ));
    }

    // No app named: the enrolled identity is the directory's single source of
    // attachment truth. `.spice/cloud.json` is not a resolution source.
    let Some(identity) = identity else {
        return Err(Error::cloud_with_hint(
            CloudErrorCode::InvalidRequest,
            "No app specified.",
            "Pass --project <org>/<project>, or enroll this directory and run 'spice cloud link <org>/<project>'.",
        ));
    };
    let Some(project) = identity.app_name.filter(|project| !project.is_empty()) else {
        return Err(Error::cloud_with_hint(
            CloudErrorCode::InvalidRequest,
            "No app specified: this directory's enrolled instance is not attached to a project.",
            "Pass --project <org>/<project> to select one, or run 'spice cloud link' interactively.",
        ));
    };
    let linked_org = identity.org_name.filter(|org| !org.is_empty());
    if let Some(linked_org) = linked_org.as_deref() {
        org::validate_org_name(linked_org)?;
        ensure_orgs_agree(
            linked_org,
            "the enrolled instance",
            explicit_org.as_deref(),
            explicit_source,
        )?;
    }
    Ok((
        ProjectTarget::new(linked_org.or(default_org), project),
        OrgSource::LinkedApp,
    ))
}

/// Refuse to guess when two explicit signals name different organizations.
///
/// `gh` adopted this rule after implicit selection confused users
/// (<https://github.com/cli/cli/discussions/6777>); the tools that silently pick
/// instead have documented wrong-target incidents.
fn ensure_orgs_agree(
    stated: &str,
    stated_source: &str,
    other: Option<&str>,
    other_source: OrgSource,
) -> Result<()> {
    let Some(other) = other else {
        return Ok(());
    };
    if stated.eq_ignore_ascii_case(other) {
        return Ok(());
    }
    // Only flags and the environment are explicit enough to conflict; the
    // active org is a standing default that any explicit signal may override.
    if !matches!(other_source, OrgSource::Flag | OrgSource::Environment) {
        return Ok(());
    }

    Err(Error::cloud_with_hint(
        CloudErrorCode::OrgConflict,
        format!(
            "Conflicting organizations: {stated_source} says '{stated}', but {} says '{other}'.",
            other_source.label()
        ),
        format!(
            "Name one organization: pass --project {stated}/<project>, or drop --org and let {stated_source} decide."
        ),
    ))
}

/// Resolve which app a command acts on.
async fn resolve_project_target(
    app_flag: Option<&str>,
    flag_org: Option<&str>,
) -> Result<ProjectTarget> {
    Ok(resolve_project_target_with_source(app_flag, flag_org)
        .await?
        .0)
}

/// Build a client for the org a command acts on.
async fn connect(flag_org: Option<&str>) -> Result<CloudClient> {
    CloudClient::connect(resolve_org(flag_org)?.as_deref()).await
}

/// Build a client for the org that owns `target`.
async fn connect_for_target(target: &ProjectTarget) -> Result<CloudClient> {
    CloudClient::connect(target.org.as_deref()).await
}

/// Print the fully-qualified target and where its org came from, before a
/// command changes anything.
///
/// A wrong-organization deploy or delete cannot be undone by reading the
/// scrollback afterwards, and a persisted org is invisible at the call site.
/// Suppressed in machine mode, where the same facts belong in the JSON result.
fn announce_target(action: &str, target: &ProjectTarget, source: OrgSource, output: OutputFormat) {
    if output == OutputFormat::Json {
        return;
    }

    println!("{action} {target}");
    if target.org.is_some() && source != OrgSource::ProjectArgument {
        println!("  organization from {}", source.label());
    }
}

// ============================================================================
// Command implementations
// ============================================================================

pub(crate) async fn execute_login(args: &LoginArgs, org: Option<&str>) -> Result<()> {
    let target = LoginTarget {
        requested_org: org,
        output: args.output,
    };
    match &args.method {
        Some(LoginMethod::Subscription(args)) => {
            execute_login_device_flow(!args.device, &target).await
        }
        Some(LoginMethod::Token(args)) => execute_login_token(args, &target).await,
        Some(LoginMethod::Api(args)) => execute_login_api(args, &target).await,
        None => execute_login_with_chooser(&target).await,
    }
}

async fn execute_login_with_chooser(target: &LoginTarget<'_>) -> Result<()> {
    ensure_login_chooser_tty(std::io::stdin().is_terminal())?;

    let items = ["Login with a web browser", "Paste an access token"];
    let selection = Select::with_theme(&ColorfulTheme::default())
        .with_prompt("How would you like to authenticate to Spice Cloud?")
        .items(items)
        .default(0)
        .interact()
        .map_err(|err| crate::error::Error::InvalidArgument {
            message: format!("Failed to read login selection: {err}"),
        })?;

    match selection {
        0 => execute_login_device_flow(true, target).await,
        1 => execute_login_token(&TokenLoginArgs { token: None }, target).await,
        _ => InvalidArgumentSnafu {
            message: "Invalid login selection".to_string(),
        }
        .fail(),
    }
}

fn ensure_login_chooser_tty(is_terminal: bool) -> Result<()> {
    if !is_terminal {
        return InvalidArgumentSnafu {
            message: "Choose a login type explicitly when running non-interactively: 'spice login subscription', 'spice login token', or 'spice login api'",
        }
        .fail();
    }

    Ok(())
}

async fn execute_login_token(args: &TokenLoginArgs, target: &LoginTarget<'_>) -> Result<()> {
    if target.output != LoginOutput::Json && args.token.is_none() {
        println!(
            "Tip: you can generate an access token here {}/account/tokens",
            client::portal_base_url()
        );
    }
    let token = resolve_string_or_prompt(
        args.token.as_deref(),
        "Access token",
        "--token",
        "SPICE_CLOUD_PAT",
        "Paste your access token",
        true,
    )?;

    save_token_and_print_login_result(&token, target).await
}

async fn execute_login_api(args: &ApiLoginArgs, target: &LoginTarget<'_>) -> Result<()> {
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

    // Save the token and client credentials to the env file. Service-account
    // tokens do not have a user context, so skip the auth-context check used
    // for subscription/PAT logins.
    save_api_credentials_and_print_login_result(&client_id, &client_secret, &token, target).await
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

/// The `merge_auth_config` key a credential is filed under.
///
/// `merge_auth_config("SPICEAI", &[(key, _)])` writes `SPICE_SPICEAI_{key}`, so
/// this must stay the inverse of [`org::org_token_var`] — a mismatch would store
/// a credential the reader never finds.
fn credential_key(org: Option<&str>) -> String {
    match org {
        Some(org) if !org.is_empty() => org::org_token_var(org)
            .strip_prefix("SPICE_SPICEAI_")
            .unwrap_or("TOKEN")
            .to_string(),
        _ => "TOKEN".to_string(),
    }
}

/// The `merge_auth_config` key the app API key is filed under.
///
/// Inverse of [`org::org_api_key_var`], for the same reason as
/// [`credential_key`].
fn api_key_credential_key(org: Option<&str>) -> String {
    match org {
        Some(org) if !org.is_empty() => org::org_api_key_var(org)
            .strip_prefix("SPICE_SPICEAI_")
            .unwrap_or("API_KEY")
            .to_string(),
        _ => "API_KEY".to_string(),
    }
}

/// Build the credential writes for a user login.
///
/// User management tokens are membership-wide, so the default token and every
/// verified organization receive a copy. A data-plane API key belongs to the
/// organization returned with the authenticated user context; an explicit
/// login for another member organization must not relabel that key or replace
/// the default data-plane key.
fn user_login_values<'a>(
    token: &'a str,
    api_key: Option<&'a str>,
    token_org: Option<&str>,
    store_org: Option<&str>,
) -> Vec<(String, &'a str)> {
    let mut credential_orgs = BTreeSet::new();
    if let Some(org) = token_org {
        credential_orgs.insert(org.to_string());
    }
    if let Some(org) = store_org {
        credential_orgs.insert(org.to_string());
    }

    let mut values = vec![(credential_key(None), token)];
    values.extend(
        credential_orgs
            .iter()
            .map(|org| (credential_key(Some(org)), token)),
    );

    if let Some(api_key) = api_key {
        if store_org.is_none() {
            values.push((api_key_credential_key(None), api_key));
        }
        if let Some(org) = token_org.or(store_org) {
            values.push((api_key_credential_key(Some(org)), api_key));
        }
    }

    values
}

/// Machine credentials are confined to one organization when one is named.
/// A named API login must not replace the membership-wide default user token.
fn machine_login_values<'a>(token: &'a str, store_org: Option<&str>) -> Vec<(String, &'a str)> {
    vec![(credential_key(store_org), token)]
}

fn keychain_login_orgs(spiceai: &[(&str, &str)]) -> BTreeSet<String> {
    spiceai
        .iter()
        .filter_map(|(key, _)| org::org_from_credential_key(key))
        .collect()
}

/// Check that a freshly minted credential really serves the requested org.
///
/// User credentials can act across every organization the user belongs to;
/// machine credentials remain bound to the organization that issued them.
async fn verify_login_org(
    client: &CloudClient,
    requested_org: Option<&str>,
    token_org: Option<&str>,
) -> Result<Option<String>> {
    let Some(requested) = requested_org else {
        return Ok(None);
    };

    // The server is authoritative on user membership and machine-token scope.
    // Do not compare a user's default org with the requested org: doing so
    // rejects valid member access before the membership endpoint can decide.
    let _ = token_org;
    client.get_auth_context_for_org(requested).await?;
    Ok(Some(requested.to_string()))
}

fn persist_login_values(
    output: LoginOutput,
    spiceai: &[(&str, &str)],
    cloud: &[(&str, &str)],
    context: Option<&spice_cloud_client::types::AuthContext>,
) -> Result<()> {
    use crate::commands::login::save_credentials;

    if output == LoginOutput::Json {
        let mut result = serde_json::Map::new();
        for (key, value) in spiceai {
            result.insert(
                format!("SPICE_SPICEAI_{key}"),
                serde_json::Value::String((*value).to_string()),
            );
        }
        for (key, value) in cloud {
            result.insert(
                format!("SPICE_CLOUD_{key}"),
                serde_json::Value::String((*value).to_string()),
            );
        }
        if let Some(context) = context {
            result.insert("username".to_string(), context.username.clone().into());
            result.insert("org".to_string(), context.org_name.clone().into());
            result.insert(
                "app".to_string(),
                context.app_name.clone().unwrap_or_default().into(),
            );
        }
        return write_json(&serde_json::Value::Object(result));
    }

    if output == LoginOutput::Keychain {
        org::remember_keychain_orgs(&keychain_login_orgs(spiceai))?;
    }

    if !spiceai.is_empty() {
        save_credentials(output, "SPICEAI", spiceai)?;
    }
    if !cloud.is_empty() {
        save_credentials(output, "CLOUD", cloud)?;
    }
    Ok(())
}

async fn save_token_and_print_login_result(token: &str, target: &LoginTarget<'_>) -> Result<()> {
    let authed_client = CloudClient::with_token_for_org(token, target.requested_org)?;
    let auth_context_result = authed_client.get_auth_context().await;

    let token_org = auth_context_result
        .as_ref()
        .ok()
        .map(|context| context.org_name.clone())
        .filter(|org| !org.is_empty());

    // Verify before storing: a credential filed under an org it cannot act on
    // would make every later command fail with a confusing server error.
    let store_org =
        verify_login_org(&authed_client, target.requested_org, token_org.as_deref()).await?;

    let api_key = auth_context_result
        .as_ref()
        .ok()
        .and_then(|context| context.app_api_key.as_deref());
    let values = user_login_values(token, api_key, token_org.as_deref(), store_org.as_deref());
    let value_refs: Vec<(&str, &str)> = values
        .iter()
        .map(|(key, value)| (key.as_str(), *value))
        .collect();
    persist_login_values(
        target.output,
        &value_refs,
        &[],
        auth_context_result.as_ref().ok(),
    )?;

    if target.output == LoginOutput::Json {
        return Ok(());
    }

    match auth_context_result {
        Ok(context) => {
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

    print_post_login_org_context(&authed_client, token_org.as_deref(), store_org.as_deref()).await;
    print_post_login_help();
    Ok(())
}

async fn save_api_credentials_and_print_login_result(
    client_id: &str,
    client_secret: &str,
    token: &str,
    target: &LoginTarget<'_>,
) -> Result<()> {
    let authed_client = CloudClient::with_token_for_org(token, target.requested_org)?;
    let store_org = verify_login_org(&authed_client, target.requested_org, None).await?;

    let values = machine_login_values(token, store_org.as_deref());
    let value_refs: Vec<(&str, &str)> = values
        .iter()
        .map(|(key, value)| (key.as_str(), *value))
        .collect();
    persist_login_values(
        target.output,
        &value_refs,
        &[("CLIENT_ID", client_id), ("CLIENT_SECRET", client_secret)],
        None,
    )?;
    if target.output == LoginOutput::Json {
        return Ok(());
    }

    println!();
    println!("\x1b[32m✓ Successfully logged in to Spice Cloud with API credentials\x1b[0m");
    println!("  Client ID: {client_id}");
    println!();
    match target.output {
        LoginOutput::Env => println!("Credentials saved to the local `.env` file."),
        LoginOutput::Keychain => println!("Credentials saved to the platform keychain."),
        LoginOutput::Json => {}
    }

    print_post_login_org_context(&authed_client, None, store_org.as_deref()).await;
    print_post_login_help();
    Ok(())
}

/// Tell a multi-org user which org is in effect and how to change it.
///
/// Best-effort: an org listing the deployment does not serve must not turn a
/// successful login into a failure.
async fn print_post_login_org_context(
    client: &CloudClient,
    token_org: Option<&str>,
    stored_org: Option<&str>,
) {
    if let Some(org) = stored_org {
        if let Err(err) = org::set_active_org(org) {
            tracing::warn!(
                "Logged in, but could not record '{org}' as the active organization: {err}"
            );
            return;
        }
        println!("Active org: {org}");
        return;
    }

    let active = match org::active_org() {
        Ok(active) => active,
        Err(err) => {
            tracing::debug!("Could not read the active organization: {err}");
            None
        }
    };
    let effective = active.as_deref().or(token_org);
    let Some(effective) = effective else {
        return;
    };

    println!("Active org: {effective}");

    if let Ok(Some(orgs)) = client.list_orgs().await
        && orgs.len() > 1
    {
        println!(
            "  You belong to {} organizations — run 'spice cloud orgs' to list them, or 'spice cloud org use <org>' to switch.",
            orgs.len()
        );
    }
}

async fn execute_login_device_flow(open_browser: bool, target: &LoginTarget<'_>) -> Result<()> {
    use crate::commands::login::session::{BrowserLogin, BrowserLoginOutcome};

    let flow = BrowserLogin::at(client::portal_base_url());
    if target.output != LoginOutput::Json {
        flow.announce();
    }
    if open_browser {
        flow.open_browser();
    }
    let BrowserLoginOutcome::Granted { access_token, .. } = flow.authenticate().await? else {
        return Err(Error::DeviceAuthorizationDenied);
    };
    save_token_and_print_login_result(&access_token, target).await
}

fn print_post_login_help() {
    println!();
    println!("You can now use 'spice cloud' commands to manage your apps and deployments.");
    println!();
    println!("Quick start:");
    println!("  spice cloud orgs                   - List your organizations");
    println!("  spice cloud projects               - List your projects");
    println!("  spice cloud project create <name>  - Create a new project");
    println!("  spice cloud deploy --project <org/project> - Deploy it");
    println!();
}

fn execute_logout(args: &LogoutArgs, flag_org: Option<&str>) -> Result<()> {
    let mut cleared = Vec::new();
    let mut already_logged_out: Option<String> = None;

    match args.scope {
        LogoutScope::All => {
            for org in org::orgs_with_stored_tokens()? {
                if remove_env_keys(&[org::org_token_var(&org), org::org_api_key_var(&org)])? {
                    cleared.push(org);
                }
            }
            remove_env_keys(&default_credential_keys())?;
            org::clear_keychain_orgs()?;
            org::clear_active_org()?;
        }
        LogoutScope::Active => match resolve_org(flag_org)? {
            // An org with its own credential loses only that credential; the
            // personal-org session in the same directory survives.
            Some(org) if org::has_org_token(&org) => {
                if remove_env_keys(&[org::org_token_var(&org), org::org_api_key_var(&org)])? {
                    cleared.push(org.clone());
                }
                org::forget_keychain_org(&org)?;
                if resolve_org(None)?.is_some_and(|active| active.eq_ignore_ascii_case(&org)) {
                    org::clear_active_org()?;
                }
            }
            // An org was named but holds no credential of its own. It is
            // already logged out. Falling through to the default credentials
            // here would destroy a *different* organization's session — the
            // default credential belongs to whichever org minted it, and named
            // orgs deliberately never fall back to it.
            Some(org) => {
                already_logged_out = Some(org);
            }
            // No org named: clear the default session.
            None => {
                if remove_env_keys(&default_credential_keys())? {
                    cleared.push("default".to_string());
                }
                org::clear_active_org()?;
            }
        },
    }

    // Say exactly what was discarded. "Logged out" over a no-op would leave a
    // user believing a credential is gone when it is still on disk.
    match (args.scope, cleared.first()) {
        (LogoutScope::All, _) => {
            println!(
                "\x1b[32m✓ Logged out of all Spice Cloud organizations on this machine\x1b[0m"
            );
        }
        (LogoutScope::Active, Some(org)) if org != "default" => {
            println!("\x1b[32m✓ Logged out of organization {org}\x1b[0m");
            println!("  Other stored organizations are untouched — use --scope all to clear them.");
        }
        (LogoutScope::Active, Some(_)) => {
            println!("\x1b[32m✓ Successfully logged out from Spice Cloud\x1b[0m");
        }
        (LogoutScope::Active, None) => match &already_logged_out {
            Some(org) => println!("\x1b[32m✓ No stored credential for organization {org}\x1b[0m"),
            None => println!("\x1b[32m✓ Already logged out\x1b[0m"),
        },
    }

    Ok(())
}

/// Credential variables that belong to no particular org.
fn default_credential_keys() -> Vec<String> {
    vec![
        org::DEFAULT_TOKEN_VAR.to_string(),
        org::DEFAULT_API_KEY_VAR.to_string(),
        "SPICE_CLOUD_CLIENT_ID".to_string(),
        "SPICE_CLOUD_CLIENT_SECRET".to_string(),
    ]
}

/// Drop `keys` from the env file, and from the platform keychain.
///
/// Returns whether anything was removed. Uses the writer's own parser rather
/// than prefix-matching lines, so a credential the reader can find is one this
/// can remove — matching raw text left `KEY = value` readable but unremovable,
/// so logout reported success while the credential stayed live.
///
/// The keychain is cleared too: `read_credential` consults it before the env
/// file, so clearing only the file leaves a working credential behind.
fn remove_env_keys(keys: &[String]) -> Result<bool> {
    use crate::commands::login::env_file_path;

    let mut removed = false;

    // The keychain is consulted before the env file when reading a credential,
    // so clearing only the file would leave a working credential behind.
    //
    // A failure here must not abort: aborting would skip the env file too and
    // leave *more* credentials live than before. Instead keep going and report
    // what could not be cleared, so the user knows to remove it by hand rather
    // than believing a clean logout. `NoEntry` and an unavailable backend both
    // mean there is nothing stored to clear — headless and containerized hosts
    // legitimately have no keychain at all.
    let mut keychain_failures = Vec::new();
    for key in keys {
        match keyring::Entry::new(key, "spice").map(|entry| entry.delete_credential()) {
            Ok(Ok(())) => removed = true,
            // Nothing stored to clear: either no entry, or no usable keychain
            // at all — headless and containerized hosts legitimately have none.
            Ok(Err(
                keyring::Error::NoEntry
                | keyring::Error::NoStorageAccess(_)
                | keyring::Error::PlatformFailure(_),
            ))
            | Err(_) => {}
            Ok(Err(err)) => keychain_failures.push(format!("{key} ({err})")),
        }
    }

    let path = std::path::Path::new(env_file_path());
    if !path.exists() {
        ensure_keychain_credentials_removed(&keychain_failures)?;
        return Ok(removed);
    }

    // Never rewrite or delete the file on a failed read. Treating an unreadable
    // or non-UTF-8 file as empty would delete it wholesale, taking every
    // unrelated setting with it.
    let content = std::fs::read_to_string(path).map_err(|e| crate::error::Error::ConfigIo {
        operation: "read",
        path: path.to_path_buf(),
        source: e,
    })?;

    // Keep every line that does not assign a removed key, byte-for-byte.
    // Rebuilding the file from a parsed map instead would drop comments and
    // blank lines, collapse duplicate keys, and truncate a multi-line value
    // such as a PEM block to its first line.
    let mut kept: Vec<&str> = Vec::new();
    for line in content.lines() {
        if assigns_any(line, keys) {
            removed = true;
        } else {
            kept.push(line);
        }
    }

    // Only discard the file once nothing but comments and blank lines remain.
    if kept
        .iter()
        .all(|line| line.trim().is_empty() || line.trim_start().starts_with('#'))
    {
        std::fs::remove_file(path).map_err(|e| crate::error::Error::ConfigIo {
            operation: "delete",
            path: path.to_path_buf(),
            source: e,
        })?;
        ensure_keychain_credentials_removed(&keychain_failures)?;
        return Ok(removed);
    }

    let mut updated = kept.join("\n");
    if content.ends_with('\n') {
        updated.push('\n');
    }
    std::fs::write(path, updated).map_err(|e| crate::error::Error::ConfigIo {
        operation: "write",
        path: path.to_path_buf(),
        source: e,
    })?;

    ensure_keychain_credentials_removed(&keychain_failures)?;

    Ok(removed)
}

fn ensure_keychain_credentials_removed(failures: &[String]) -> Result<()> {
    if failures.is_empty() {
        return Ok(());
    }

    Err(Error::InvalidArgument {
        message: format!(
            "Failed to remove {} from the keychain. Local env-file entries were cleared, but these credentials may still be usable. Remove them manually before leaving this machine.",
            failures.join(", ")
        ),
    })
}

/// Whether an env-file line assigns one of `keys`.
///
/// Splits on the first `=` and trims the left side, matching how the reader
/// parses keys. Comparing raw text instead missed the spaced `KEY = value`
/// form the reader accepts, leaving such a credential readable but not
/// removable — so logout reported success while it stayed live.
fn assigns_any(line: &str, keys: &[String]) -> bool {
    let Some((lhs, _)) = line.split_once('=') else {
        return false;
    };
    let name = lhs.trim().trim_start_matches("export ").trim();
    keys.iter().any(|key| key == name)
}

async fn execute_whoami(args: &WhoamiArgs, flag_org: Option<&str>) -> Result<()> {
    let (effective_org, source) = resolve_org_with_source(flag_org)?;
    let client = CloudClient::connect(effective_org.as_deref()).await?;

    let context = match client.get_auth_context().await {
        Ok(ctx) => ctx,
        Err(err) if client::is_unauthorized_auth_context_error(&err) => {
            // The auth-context endpoint requires a user token (subscription
            // or PAT). Service-account tokens (OAuth client credentials) are
            // valid for API calls but do not have a user identity.
            if client.list_projects().await.is_ok() {
                return Err(Error::cloud_with_hint(
                    CloudErrorCode::Forbidden,
                    "User identity is not available for this authentication method. The current credential is a valid service-account token and can be used for API calls, but has no user identity.",
                    "Run 'spice cloud login subscription' or 'spice cloud login token' to obtain a user token.",
                ));
            }
            return Err(err);
        }
        Err(err) => return Err(err),
    };

    // Report the org commands will actually use, which is the selected one when
    // set and the credential's own org otherwise.
    let active_org = effective_org
        .clone()
        .unwrap_or_else(|| context.org_name.clone());

    let available_orgs = client.list_orgs().await.unwrap_or_default();

    if args.output == OutputFormat::Json {
        return write_json(&serde_json::json!({
            "username": context.username,
            "email": context.email,
            "org_name": context.org_name,
            "active_org": active_org,
            "active_org_source": source.label(),
            "app_name": context.app_name,
            "available_orgs": available_orgs
                .as_ref()
                .map(|orgs| orgs.iter().map(|org| org.name.clone()).collect::<Vec<_>>()),
        }));
    }

    println!("Logged in as: {} ({})", context.username, context.email);
    println!("Active org:   {active_org} (from {})", source.label());
    if !context.org_name.is_empty() && !context.org_name.eq_ignore_ascii_case(&active_org) {
        println!("Credential org: {}", context.org_name);
    }
    if let Some(app_name) = context.app_name {
        println!("Default Project:  {active_org}/{app_name}");
    }
    match available_orgs {
        Some(orgs) if orgs.len() > 1 => {
            println!("Organizations: {} (run 'spice cloud orgs')", orgs.len());
        }
        Some(_) | None => {}
    }

    Ok(())
}

async fn execute_orgs(args: &OrgsArgs, flag_org: Option<&str>) -> Result<()> {
    let active = resolve_org(flag_org)?;
    let client = CloudClient::connect(active.as_deref()).await?;

    let listed = client.list_orgs().await?;
    let context_org = client
        .optional_user_auth_context()
        .await
        .ok()
        .flatten()
        .map(|context| context.org_name)
        .filter(|org| !org.is_empty());

    let stored = org::orgs_with_stored_tokens()?;
    let rows = build_org_rows(
        listed.as_deref(),
        context_org.as_deref(),
        active.as_deref(),
        &stored,
    );

    if args.output == OutputFormat::Json {
        return write_json(&rows);
    }

    if rows.is_empty() {
        println!("No organizations found for this credential.");
        return Ok(());
    }

    let mut table = TableOutput::new(vec!["NAME", "ID", "ROLE", "ACTIVE", "CREDENTIAL"]);
    for row in &rows {
        table.add_row(vec![
            row.name.clone(),
            row.id.map_or_else(|| "-".to_string(), |id| id.to_string()),
            row.role.clone().unwrap_or_else(|| "-".to_string()),
            if row.active { "✓" } else { "" }.to_string(),
            if row.has_credential { "stored" } else { "-" }.to_string(),
        ]);
    }
    table.print();

    if listed.is_none() {
        println!();
        println!(
            "Note: this Spice Cloud deployment does not expose an organization listing, so only organizations this CLI already knows about are shown."
        );
    }

    Ok(())
}

/// One row of `spice cloud orgs`.
#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
pub struct OrgRow {
    pub name: String,
    pub id: Option<i64>,
    pub role: Option<String>,
    /// Whether commands currently act on this org.
    pub active: bool,
    /// Whether a credential is stored specifically for this org.
    pub has_credential: bool,
}

/// Merge what the API reports with what the CLI knows locally.
///
/// When the API cannot enumerate orgs, the credential's own org plus any org
/// with a stored credential is still a useful, truthful answer — better than
/// reporting none.
fn build_org_rows(
    listed: Option<&[spice_cloud_client::types::Org]>,
    context_org: Option<&str>,
    active_org: Option<&str>,
    stored_credentials: &std::collections::BTreeSet<String>,
) -> Vec<OrgRow> {
    let mut rows: Vec<OrgRow> = Vec::new();
    let mut push = |name: String, id: Option<i64>, role: Option<String>| {
        if rows
            .iter()
            .any(|row: &OrgRow| row.name.eq_ignore_ascii_case(&name))
        {
            return;
        }
        let active = active_org.is_some_and(|active| active.eq_ignore_ascii_case(&name))
            || (active_org.is_none()
                && context_org.is_some_and(|org| org.eq_ignore_ascii_case(&name)));
        let has_credential = stored_credentials
            .iter()
            .any(|stored| stored.eq_ignore_ascii_case(&name));
        rows.push(OrgRow {
            name,
            id,
            role,
            active,
            has_credential,
        });
    };

    if let Some(listed) = listed {
        for org in listed {
            push(org.name.clone(), org.id, org.role.clone());
        }
    }

    if let Some(context_org) = context_org {
        push(context_org.to_string(), None, None);
    }
    for org in stored_credentials {
        push(org.clone(), None, None);
    }
    if let Some(active) = active_org {
        push(active.to_string(), None, None);
    }

    rows.sort_by_key(|row| row.name.to_lowercase());
    rows
}

async fn execute_org(cmd: &OrgCommands, flag_org: Option<&str>) -> Result<()> {
    match cmd {
        OrgCommands::Use(args) => {
            org::validate_org_name(&args.org)?;

            // Check membership before switching, so a typo or a revoked
            // membership fails here rather than as an empty app list later.
            // Only a listing that definitively excludes the org blocks the
            // switch — the server re-checks membership on every request anyway,
            // so an unreachable API must not strand the user in the wrong org.
            let verified = match CloudClient::connect(Some(&args.org)).await {
                Ok(client) => match client.list_orgs().await {
                    Ok(Some(orgs)) => {
                        if !orgs
                            .iter()
                            .any(|org| org.name.eq_ignore_ascii_case(&args.org))
                        {
                            return Err(Error::cloud_with_hint(
                                CloudErrorCode::OrgForbidden,
                                format!("You are not a member of organization '{}'.", args.org),
                                "Run 'spice cloud orgs' to list the organizations you can act on.",
                            ));
                        }
                        true
                    }
                    Ok(None) => false,
                    Err(err) => {
                        tracing::debug!("Could not verify organization membership: {err}");
                        false
                    }
                },
                Err(err) => {
                    tracing::debug!("Not authenticated, so membership was not verified: {err}");
                    false
                }
            };

            org::set_active_org(&args.org)?;

            if args.output == OutputFormat::Json {
                return write_json(&serde_json::json!({
                    "active_org": args.org,
                    "membership_verified": verified,
                    "status": "set",
                }));
            }
            println!("\x1b[32m✓ Active organization set to {}\x1b[0m", args.org);
            if !verified {
                println!(
                    "  Could not confirm membership from here; Spice Cloud checks it on every request."
                );
            }
            if !org::has_org_token(&args.org) {
                println!(
                    "  No credential is stored for this organization yet — run 'spice cloud login token --org {}'.",
                    args.org
                );
            }
            println!(
                "  For scripts and CI, prefer {}=<org> — it is scoped to the shell instead of the machine.",
                org::ACTIVE_ORG_VAR
            );
            Ok(())
        }
        OrgCommands::Current(args) => {
            let active = resolve_org(flag_org)?;
            if args.output == OutputFormat::Json {
                return write_json(&serde_json::json!({ "active_org": active }));
            }
            match active {
                Some(org) => println!("{org}"),
                None => println!(
                    "No active organization set; commands use the organization the credential was issued for."
                ),
            }
            Ok(())
        }
        OrgCommands::Clear => {
            org::clear_active_org()?;
            println!(
                "\x1b[32m✓ Cleared the active organization; commands now use the credential's own organization\x1b[0m"
            );
            Ok(())
        }
    }
}

async fn execute_service(
    ctx: &RuntimeContext,
    args: &crate::commands::connect::service::cli::ServiceArgs,
) -> Result<()> {
    let current = std::env::current_dir().map_err(|source| Error::CloudConnectIo {
        message: format!("resolve the current instance directory: {source}"),
    })?;
    let instance_dir =
        tokio::fs::canonicalize(&current)
            .await
            .map_err(|source| Error::CloudConnectIo {
                message: format!(
                    "canonicalize the current instance directory {}: {source}",
                    current.display()
                ),
            })?;
    let config_dir = CloudConnectConfig::resolve_config_dir(Some(&instance_dir));
    crate::commands::connect::service::cli::execute(
        ctx,
        crate::commands::connect::service::cli::ServiceArgs {
            command: args.command.clone(),
        },
        &instance_dir,
        &config_dir,
    )
    .await
}

async fn execute_link(ctx: &RuntimeContext, args: &LinkArgs, flag_org: Option<&str>) -> Result<()> {
    ensure_link_chooser_tty(std::io::stdin().is_terminal())?;
    let current = std::env::current_dir().map_err(|source| Error::CloudConnectIo {
        message: format!("resolve the current instance directory: {source}"),
    })?;
    let instance_dir =
        tokio::fs::canonicalize(&current)
            .await
            .map_err(|source| Error::CloudConnectIo {
                message: format!(
                    "canonicalize the current instance directory {}: {source}",
                    current.display()
                ),
            })?;
    let config_dir = CloudConnectConfig::resolve_config_dir(Some(&instance_dir));
    let identity_path = config_dir.join(runtime_cloud_connect::config::IDENTITY_FILE);
    let identity = IdentityStore::load_optional_async(identity_path)
        .await
        .map_err(|source| Error::CloudConnectIo {
            message: format!("read the enrolled identity before linking: {source}"),
        })?;
    let endpoint = identity
        .as_ref()
        .and_then(|identity| identity.control_plane_endpoint.clone())
        .unwrap_or_else(client::get_base_url);

    let target = if let Some(project) = args.project.as_deref() {
        resolve_project_target(Some(project), flag_org).await?
    } else {
        let token = user_token_for_cloud_connect(&endpoint, None, "Linking", "link").await?;
        let attach_client = crate::commands::connect::project::ProjectClient::new(&endpoint)
            .map_err(|error| Error::CloudConnectIo {
                message: error.to_string(),
            })?;
        let projects = attach_client
            .list_attachable(&runtime_cloud_connect::enroll::SessionToken::new(token))
            .await
            .map_err(|error| Error::CloudConnectIo {
                message: error.to_string(),
            })?;
        choose_attachable_project(projects).await?
    };
    let org = target.org.as_deref().ok_or_else(|| {
        Error::cloud_with_hint(
            CloudErrorCode::InvalidRequest,
            format!("Project '{}' has no organization metadata.", target.project),
            "Pass an explicit <org>/<project> target.",
        )
    })?;
    let token = user_token_for_cloud_connect(&endpoint, Some(org), "Linking", "link").await?;
    let management_client = CloudClient::with_token_for_org_at(token, Some(org), &endpoint)?;
    let project = management_client.get_project(&target).await?;

    let result = crate::commands::connect::transaction::execute(
        ctx,
        crate::commands::connect::transaction::ConnectRequest {
            org: Some(org.to_string()),
            project: Some(project.name.clone()),
            token: None,
            region: None,
            dir: None,
            endpoint: Some(endpoint),
        },
    )
    .await?;
    if result.is_none() {
        return Ok(());
    }

    upload_local_spicepod_if_absent(&management_client, &target, &project).await?;
    if let Err(err) = ignore_cloud_link_dir().await {
        tracing::warn!(
            "Linked the enrolled instance to project '{}', but failed to add `.spice` to `.gitignore`, so its mTLS private key could be committed. Add `.spice/` to `.gitignore` before committing. Cause: {err}. See: https://spiceai.org/docs",
            target
        );
    }

    println!("\x1b[32m✓ Linked the enrolled instance to project {target}\x1b[0m");
    println!();
    println!("Start this instance in the current directory with:");
    println!("  spice run");
    println!();
    println!("You can now use commands without specifying --project:");
    println!("  spice cloud deploy");
    println!("  spice cloud logs");
    println!("  spice cloud secrets list");

    Ok(())
}

async fn user_token_for_cloud_connect(
    endpoint: &str,
    requested_org: Option<&str>,
    action: &str,
    command: &str,
) -> Result<String> {
    let mut candidates = Vec::new();
    for token in [
        requested_org.and_then(org::token_for_org),
        org::default_token(),
        org::active_org()
            .ok()
            .flatten()
            .and_then(|org| org::token_for_org(&org)),
    ]
    .into_iter()
    .flatten()
    {
        if !candidates.contains(&token) {
            candidates.push(token);
        }
    }
    if candidates.is_empty() {
        return Err(Error::cloud_with_hint(
            CloudErrorCode::NotAuthenticated,
            format!("{action} an enrolled instance requires a Spice Cloud user login."),
            format!("Run `spice login`, then retry `spice cloud {command}`."),
        ));
    }

    for token in candidates {
        let client = CloudClient::with_token_for_org_at(token.clone(), None, endpoint)?;
        if client.optional_user_auth_context().await?.is_none() {
            continue;
        }
        if let Some(org) = requested_org {
            client.get_auth_context_for_org(org).await?;
        }
        return Ok(token);
    }

    Err(Error::cloud_with_hint(
        CloudErrorCode::NotAuthenticated,
        format!("{action} an enrolled instance requires a Spice Cloud user login."),
        "Run `spice login` with a user account that can access the target organization.",
    ))
}

fn ensure_link_chooser_tty(is_terminal: bool) -> Result<()> {
    if is_terminal {
        return Ok(());
    }
    Err(Error::cloud_with_hint(
        CloudErrorCode::InvalidRequest,
        "`spice cloud link` requires an interactive terminal.",
        "Run the command from an interactive terminal; use `spiced --token <enrollment-key>` for unattended enrollment.",
    ))
}

async fn choose_attachable_project(
    projects: Vec<crate::commands::connect::project::AttachableProject>,
) -> Result<ProjectTarget> {
    if projects.is_empty() {
        return Err(Error::cloud_with_hint(
            CloudErrorCode::ProjectNotFound,
            "Spice Cloud returned no projects that can be attached.",
            "Create a project in Spice Cloud, then retry `spice cloud link`.",
        ));
    }
    let items: Vec<String> = projects
        .iter()
        .map(|project| {
            let instances = if project.instances.is_empty() {
                "no enrolled instances".to_string()
            } else {
                project
                    .instances
                    .iter()
                    .map(|instance| match instance.location.as_deref() {
                        Some(location) => format!("{} ({location})", instance.id),
                        None => instance.id.clone(),
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            format!("{}/{} — {instances}", project.org, project.name)
        })
        .collect();
    let selection = tokio::task::spawn_blocking(move || {
        Select::with_theme(&ColorfulTheme::default())
            .with_prompt("Choose a Spice Cloud project")
            .items(&items)
            .default(0)
            .interact()
    })
    .await
    .map_err(|source| Error::CloudConnectIo {
        message: format!("project chooser task failed: {source}"),
    })?
    .map_err(|source| Error::InvalidArgument {
        message: format!("Failed to read the project selection: {source}"),
    })?;
    let project = projects
        .get(selection)
        .ok_or_else(|| Error::InvalidArgument {
            message: "The selected project was not present in the server response.".to_string(),
        })?;
    Ok(ProjectTarget::new(
        Some(project.org.clone()),
        project.name.clone(),
    ))
}

async fn upload_local_spicepod_if_absent(
    client: &CloudClient,
    target: &ProjectTarget,
    project: &spice_cloud_client::types::Project,
) -> Result<()> {
    let remote_has_spicepod = project
        .config
        .as_ref()
        .and_then(|config| config.spicepod.as_ref())
        .is_some_and(|spicepod| !spicepod.is_null());
    if remote_has_spicepod {
        println!(
            "Project {target} already has a Spicepod configuration; the local Spicepod was not uploaded. Reconcile changes explicitly with `spice cloud project update --spicepod <path> {target}`."
        );
        return Ok(());
    }
    let Some(path) = crate::manifest::existing_spicepod_path(std::path::Path::new(".")) else {
        return Ok(());
    };
    let spicepod = tokio::fs::read_to_string(&path)
        .await
        .map_err(|source| Error::ConfigIo {
            operation: "read",
            path: path.clone(),
            source,
        })?;
    client
        .update_project(
            target,
            client::UpdateProjectParams {
                spicepod: Some(spicepod.clone()),
                ..client::UpdateProjectParams::default()
            },
        )
        .await?;
    println!("Uploaded {} to project {target}.", path.display());
    let secret_names = secret_references(&spicepod);
    if !secret_names.is_empty() {
        println!("Set the referenced project secrets before deploying:");
        for name in secret_names {
            println!("  spice cloud secrets set {name} <value> --project {target}");
        }
    }
    Ok(())
}

fn secret_references(spicepod: &str) -> Vec<String> {
    runtime_secrets::iter_secret_references(spicepod)
        .filter(|reference| reference.store == runtime_secrets::SECRETS)
        .map(|reference| reference.key)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// Add `.spice` to the working directory's `.gitignore`, if it is a git
/// repository and the entry is not already present.
///
/// Best-effort: never fails a `link` that otherwise succeeded.
async fn ignore_cloud_link_dir() -> Result<()> {
    if !tokio::fs::try_exists(".git")
        .await
        .map_err(|source| Error::CloudConnectIo {
            message: format!("inspect the repository before ignoring `.spice`: {source}"),
        })?
    {
        return Ok(());
    }

    let path = std::path::Path::new(".gitignore");
    let existing = match tokio::fs::read_to_string(path).await {
        Ok(existing) => existing,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(source) => {
            return Err(crate::error::Error::ConfigIo {
                operation: "read",
                path: path.to_path_buf(),
                source,
            });
        }
    };
    if existing
        .lines()
        .any(|line| matches!(line.trim(), ".spice" | ".spice/" | "/.spice" | "/.spice/"))
    {
        return Ok(());
    }

    let mut updated = existing;
    if !updated.is_empty() && !updated.ends_with('\n') {
        updated.push('\n');
    }
    updated.push_str(".spice\n");

    tokio::fs::write(path, updated)
        .await
        .map_err(|e| crate::error::Error::ConfigIo {
            operation: "write",
            path: path.to_path_buf(),
            source: e,
        })
}

async fn execute_unlink() -> Result<()> {
    let current = std::env::current_dir().map_err(|source| Error::CloudConnectIo {
        message: format!("resolve the current instance directory: {source}"),
    })?;
    let instance_dir =
        tokio::fs::canonicalize(&current)
            .await
            .map_err(|source| Error::CloudConnectIo {
                message: format!(
                    "canonicalize the current instance directory {}: {source}",
                    current.display()
                ),
            })?;
    let requested_config_dir = CloudConnectConfig::resolve_config_dir(Some(&instance_dir));
    let mutation_lock =
        runtime_cloud_connect::MutationLock::acquire(&requested_config_dir, "unlink")
            .await
            .map_err(|source| Error::CloudConnectIo {
                message: format!("acquire Cloud Connect state for unlink: {source}"),
            })?;
    let display_config_dir = tokio::fs::canonicalize(&requested_config_dir)
        .await
        .map_err(|source| Error::CloudConnectIo {
            message: format!(
                "resolve the locked Cloud Connect config directory {}: {source}",
                requested_config_dir.display()
            ),
        })?;
    mutation_lock
        .ensure_directory_stable()
        .map_err(|source| Error::CloudConnectIo {
            message: format!("validate locked Cloud Connect state for unlink: {source}"),
        })?;
    let config_dir = mutation_lock
        .descriptor_relative_config_dir()
        .map_err(|source| Error::CloudConnectIo {
            message: format!("pin locked Cloud Connect state for unlink: {source}"),
        })?;
    let identity_path = config_dir.join(runtime_cloud_connect::config::IDENTITY_FILE);
    let pinned_config = crate::commands::connect::service::PinnedConfigDir::for_lock(
        display_config_dir.clone(),
        &mutation_lock,
    )?;
    let _runtime_lock =
        runtime_cloud_connect::RuntimeLock::acquire(&config_dir).map_err(|source| {
            Error::CloudConnectIo {
                message: format!(
                    "{source} Stop the running instance before using `spice cloud unlink`."
                ),
            }
        })?;
    let service_backend = crate::commands::connect::service::backend();
    let installed_service = crate::commands::connect::service::resolve_with_state(
        service_backend,
        &instance_dir,
        &pinned_config,
        &display_config_dir,
    )?;
    let identity = IdentityStore::load_optional_async(identity_path.clone())
        .await
        .map_err(|source| Error::CloudConnectIo {
            message: format!("read the enrolled identity before unlinking: {source}"),
        })?
        .ok_or_else(|| {
            Error::cloud_with_hint(
                CloudErrorCode::InvalidRequest,
                "This directory has no enrolled Spice Cloud instance to unlink.",
                "Run `spice cloud status` to inspect the current directory.",
            )
        })?;

    let configured_endpoint = client::get_base_url();
    let endpoint = release_endpoint(
        identity.control_plane_endpoint.as_deref(),
        &configured_endpoint,
    );
    let requested_org = identity.org_name.as_deref().filter(|org| !org.is_empty());
    let token =
        user_token_for_cloud_connect(endpoint, requested_org, "Unlinking", "unlink").await?;
    match runtime_cloud_connect::release::release(endpoint, &identity, &token, None).await {
        Ok(_) => {}
        Err(error) if error.is_not_found() => {}
        Err(error) => {
            return Err(Error::CloudConnectIo {
                message: format!(
                    "Spice Cloud did not confirm release of instance '{}', so its local identity was kept for retry: {error}",
                    identity.identifier
                ),
            });
        }
    }

    if let Some(manifest) = installed_service.as_ref() {
        crate::commands::connect::service::uninstall_resolved(
            service_backend,
            manifest,
            &pinned_config,
        )
        .map_err(|error| Error::ServiceUnavailable {
            message: format!(
                "Spice Cloud released instance '{}', but its local service could not be removed, so the local identity was kept for a safe retry: {error}",
                identity.identifier
            ),
        })?;
    }
    crate::commands::connect::transaction::clear_local_state(&config_dir, &identity_path).await?;
    println!("\x1b[32m✓ Unlinked and released the enrolled Spice Cloud instance\x1b[0m");
    Ok(())
}

fn release_endpoint<'a>(
    identity_endpoint: Option<&'a str>,
    configured_endpoint: &'a str,
) -> &'a str {
    identity_endpoint.unwrap_or(configured_endpoint)
}

async fn execute_projects(args: &ProjectsArgs, flag_org: Option<&str>) -> Result<()> {
    let active_org = resolve_org(flag_org)?;
    let client = CloudClient::connect(active_org.as_deref()).await?;
    let context = client.optional_user_auth_context().await?;
    let mut projects = client.list_projects().await?;

    if projects.is_empty() {
        match &active_org {
            Some(org) => println!(
                "No projects found in organization {org}. Create one with: spice cloud project create <name> --org {org}"
            ),
            None => {
                println!("No projects found. Create one with: spice cloud project create <name>");
            }
        }
        return Ok(());
    }

    // Label with the org the *credential* reports, never the one that was
    // requested. Stamping the requested org onto a listing the server actually
    // produced for another org would assert an attribution the CLI has not
    // verified — and `--output json` would carry it into scripts.
    let context_org = context
        .as_ref()
        .map(|c| c.org_name.as_str())
        .filter(|org| !org.is_empty())
        .or(active_org.as_deref())
        .unwrap_or("");
    // The Spice Cloud `/v1/apps` endpoint does not populate `org` per project, so
    // backfill it from the auth-context org — the same fallback the table
    // rendering applies via `display_project_name`. Without this, `--output json`
    // emitted `"org": ""` while the table showed `<org>/<name>`, breaking
    // format parity and machine-readable scripting (see #11041).
    backfill_project_orgs(&mut projects, context_org);

    if args.output == OutputFormat::Json {
        return write_json(&projects);
    }

    let mut table = TableOutput::new(vec![
        "NAME",
        "DESCRIPTION",
        "REGION",
        "VISIBILITY",
        "CREATED",
    ]);
    for project in &projects {
        let display_name = display_project_name(project, context_org);
        table.add_row(vec![
            display_name,
            project.description.clone().unwrap_or_default(),
            project.region.clone().unwrap_or_else(|| "-".to_string()),
            project
                .visibility
                .clone()
                .unwrap_or_else(|| "private".to_string()),
            project
                .created_at
                .clone()
                .unwrap_or_else(|| "-".to_string()),
        ]);
    }
    table.print();

    Ok(())
}

/// Backfill each project's empty `org` from the auth-context org so machine-readable
/// (`--output json`) output matches the human-readable table, which already
/// applies this fallback when rendering via [`display_project_name`]. The Spice
/// Cloud `/v1/apps` endpoint does not populate `org` on each project, so the auth
/// context is the only source of truth for the user's org. A no-op when
/// `context_org` is empty (nothing to fall back to) or the project already carries
/// an org.
fn backfill_project_orgs(projects: &mut [spice_cloud_client::types::Project], context_org: &str) {
    if context_org.is_empty() {
        return;
    }
    for project in projects.iter_mut() {
        if project.org.is_empty() {
            project.org = context_org.to_string();
        }
    }
}

/// Format a project's display name as `org/name`, falling back to the auth
/// context org when the project payload does not include one.
fn display_project_name(project: &spice_cloud_client::types::Project, context_org: &str) -> String {
    let org = if project.org.is_empty() {
        context_org
    } else {
        project.org.as_str()
    };
    if org.is_empty() {
        project.name.clone()
    } else {
        format!("{org}/{}", project.name)
    }
}

async fn execute_deployments(args: &DeploymentsArgs, flag_org: Option<&str>) -> Result<()> {
    let target = resolve_project_target(args.project.as_deref(), flag_org).await?;
    let client = connect_for_target(&target).await?;

    let deployments = client
        .list_deployments(&target, args.limit, args.status.as_deref())
        .await?;

    if deployments.is_empty() {
        println!("No deployments found for {target}");
        return Ok(());
    }

    if args.output == OutputFormat::Json {
        return write_json(&deployments);
    }

    // Commit and error are what an operator diagnosing a failed deploy needs
    // first, so they get columns rather than requiring a second command.
    let mut table = TableOutput::new(vec![
        "ID", "STATUS", "IMAGE", "REPLICAS", "COMMIT", "CREATED", "ERROR",
    ]);
    for dep in deployments {
        table.add_row(vec![
            dep.id.to_string(),
            dep.status,
            dep.image_tag.unwrap_or_else(|| "-".to_string()),
            dep.replicas
                .map_or_else(|| "-".to_string(), |r| r.to_string()),
            short_commit(dep.commit_sha.as_deref()),
            dep.created_at.unwrap_or_else(|| "-".to_string()),
            truncate_for_table(dep.error_message.as_deref().unwrap_or("-"), 60),
        ]);
    }
    table.print();

    Ok(())
}

/// Abbreviate a commit SHA to the 7 characters operators actually compare.
fn short_commit(sha: Option<&str>) -> String {
    match sha {
        // Char-wise, not byte-wise: `commit_sha` comes from the API, and
        // slicing bytes would abort the command on a multi-byte boundary.
        Some(sha) if !sha.is_empty() => sha.chars().take(7).collect(),
        _ => "-".to_string(),
    }
}

/// Clip a cell so one long error message does not destroy the table layout.
/// The untruncated value is always available in `--output json`.
fn truncate_for_table(value: &str, max: usize) -> String {
    let single_line = value.replace(['\n', '\r'], " ");
    if single_line.chars().count() <= max {
        return single_line;
    }
    let clipped: String = single_line.chars().take(max.saturating_sub(1)).collect();
    format!("{clipped}…")
}

async fn execute_regions(args: &RegionsArgs, flag_org: Option<&str>) -> Result<()> {
    let client = connect(flag_org).await?;
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

async fn execute_images(args: &ImagesArgs, flag_org: Option<&str>) -> Result<()> {
    let client = connect(flag_org).await?;
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

async fn execute_secrets(cmd: &SecretsCommands, flag_org: Option<&str>) -> Result<()> {
    match cmd {
        SecretsCommands::List(args) => {
            let target = resolve_project_target(args.project.as_deref(), flag_org).await?;
            let client = connect_for_target(&target).await?;
            let secrets = client.list_secrets(&target).await?;

            if secrets.is_empty() {
                println!("No secrets found for {target}");
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
            let (target, org_source) =
                resolve_project_target_with_source(args.project.as_deref(), flag_org).await?;
            announce_target("Setting secret on", &target, org_source, args.output);
            let client = connect_for_target(&target).await?;
            client.set_secret(&target, &args.name, &args.value).await?;
            if args.output == OutputFormat::Json {
                return write_json(&serde_json::json!({"name": args.name, "status": "set"}));
            }
            println!("\x1b[32m✓ Secret '{}' set successfully\x1b[0m", args.name);
        }
        SecretsCommands::Get(args) => {
            let target = resolve_project_target(args.project.as_deref(), flag_org).await?;
            let client = connect_for_target(&target).await?;
            let secret = client.get_secret(&target, &args.name).await?;
            if args.output == OutputFormat::Json {
                return write_json(&secret);
            }
            println!("{}", secret.value.unwrap_or_default());
        }
        SecretsCommands::Delete(args) => {
            let target = resolve_project_target(args.project.as_deref(), flag_org).await?;
            let client = connect_for_target(&target).await?;
            client.delete_secret(&target, &args.name).await?;
            if args.output == OutputFormat::Json {
                return write_json(&serde_json::json!({"name": args.name, "status": "deleted"}));
            }
            println!("\x1b[32m✓ Secret '{}' deleted\x1b[0m", args.name);
        }
    }
    Ok(())
}

async fn execute_logs(ctx: &RuntimeContext, args: &LogsArgs, flag_org: Option<&str>) -> Result<()> {
    let tail_lines = runtime_log_tail_lines(args.limit)?;
    let since = args
        .since
        .as_deref()
        .map(chrono::DateTime::parse_from_rfc3339)
        .transpose()
        .map_err(|source| Error::InvalidArgument {
            message: format!("Invalid --since timestamp: {source}. Expected RFC 3339."),
        })?;
    let target = resolve_project_target(args.project.as_deref(), flag_org).await?;
    let client = connect_for_target(&target).await?;
    let cloud_result = fetch_runtime_logs(ctx, &client, &target, tail_lines).await;

    let mut cloud_error = None;
    let mut empty_cloud_response = None;
    match cloud_result {
        Ok(logs) => {
            if logs.logs.is_empty() {
                empty_cloud_response = Some(logs);
            } else {
                ensure_cloud_logs_follow_supported(args.follow)?;
                let logs = filter_cloud_logs_since(logs, args.level, since.as_ref());
                if args.output == OutputFormat::Json {
                    return write_json(&logs);
                }
                render_cloud_logs(logs);
                return Ok(());
            }
        }
        Err(error) => cloud_error = Some(error),
    }

    let current = std::env::current_dir().map_err(|source| Error::CloudConnectIo {
        message: format!("resolve the current instance directory for local logs: {source}"),
    })?;
    let instance_dir =
        tokio::fs::canonicalize(&current)
            .await
            .map_err(|source| Error::CloudConnectIo {
                message: format!(
                    "canonicalize the current instance directory {} for local logs: {source}",
                    current.display()
                ),
            })?;
    let config_dir = CloudConnectConfig::resolve_config_dir(Some(&instance_dir));
    let number = u32::try_from(args.limit).unwrap_or(u32::MAX);
    if args.follow {
        if args.output == OutputFormat::Json {
            return Err(Error::InvalidArgument {
                message: "`--follow` cannot produce one bounded JSON document. Omit `--follow` or use table output for local service logs.".to_string(),
            });
        }
        if !matches!(args.level, LogLevelFilter::All) || since.is_some() {
            return Err(Error::InvalidArgument {
                message: "Local service log streaming cannot apply `--level` or `--since`. Omit `--follow` to use these filters.".to_string(),
            });
        }

        let local_instance_dir = instance_dir.clone();
        let local_config_dir = config_dir.clone();
        let local = tokio::task::spawn_blocking(move || {
            crate::commands::connect::service::cli::print_local_logs(
                &local_instance_dir,
                &local_config_dir,
                number,
                true,
            )
        })
        .await
        .map_err(|source| Error::CloudConnectIo {
            message: format!("local service log task failed: {source}"),
        })??;
        if local {
            if let Some(error) = cloud_error {
                eprintln!(
                    "warning: Spice Cloud logs for project '{target}' were unavailable, so logs from this directory's local service are shown instead. Cause: {error}"
                );
            }
            return Ok(());
        }
    } else {
        let local_instance_dir = instance_dir.clone();
        let local_config_dir = config_dir.clone();
        let local = tokio::task::spawn_blocking(move || {
            crate::commands::connect::service::cli::read_local_logs(
                &local_instance_dir,
                &local_config_dir,
                number,
            )
        })
        .await
        .map_err(|source| Error::CloudConnectIo {
            message: format!("local service log task failed: {source}"),
        })??;
        if let Some(lines) = local {
            let logs =
                filter_cloud_logs_since(parse_local_log_lines(lines), args.level, since.as_ref());
            if args.output == OutputFormat::Json {
                return write_json(&logs);
            }
            if let Some(error) = cloud_error {
                eprintln!(
                    "warning: Spice Cloud logs for project '{target}' were unavailable, so logs from this directory's local service are shown instead. Cause: {error}"
                );
            }
            render_cloud_logs(logs);
            return Ok(());
        }
    }
    if let Some(error) = cloud_error {
        return Err(error);
    }
    if args.output == OutputFormat::Json {
        return write_json(
            &empty_cloud_response
                .unwrap_or(spice_cloud_client::types::LogsResponse { logs: Vec::new() }),
        );
    }
    Ok(())
}

const STANDALONE_LOG_POD_PLACEHOLDER: &str = "standalone";

fn runtime_log_tail_lines(limit: usize) -> Result<u32> {
    if limit == 0 {
        return InvalidArgumentSnafu {
            message: "--limit must be greater than zero".to_string(),
        }
        .fail();
    }
    Ok(u32::try_from(limit).unwrap_or(u32::MAX))
}

async fn fetch_runtime_logs(
    ctx: &RuntimeContext,
    client: &CloudClient,
    target: &ProjectTarget,
    tail_lines: u32,
) -> Result<spice_cloud_client::types::LogsResponse> {
    let runtime_ctx = project_runtime_context(ctx, client, target, None).await?;
    let status: serde_json::Value =
        fetch_instance_json(&runtime_ctx, "/v1/spice_runtime", target).await?;
    let pods = runtime_log_pods(&status, target)?;
    let mut logs = Vec::new();

    for pod in pods {
        let path = runtime_logs_path(&pod, tail_lines);
        let response = runtime_ctx.get(&path).await.map_err(|source| {
            Error::cloud_with_hint(
                CloudErrorCode::NotFound,
                format!("Could not reach runtime logs for project {target}: {source}"),
                format!("Check it with 'spice cloud status --project {target}'."),
            )
        })?;
        let response = crate::error::check_response(response, runtime_ctx.http_endpoint()).await?;
        let text =
            response
                .text()
                .await
                .map_err(|source| crate::error::Error::InvalidResponse {
                    message: format!(
                        "Failed to read runtime logs for project {target} from {path}: {source}"
                    ),
                })?;
        let source = if pod == STANDALONE_LOG_POD_PLACEHOLDER {
            "standalone"
        } else {
            pod.as_str()
        };
        logs.extend(parse_runtime_log_text(&text, source));
    }

    Ok(spice_cloud_client::types::LogsResponse { logs })
}

fn runtime_log_pods(status: &serde_json::Value, target: &ProjectTarget) -> Result<Vec<String>> {
    let Some(hosted_status) = status.get("status") else {
        return Ok(vec![STANDALONE_LOG_POD_PLACEHOLDER.to_string()]);
    };
    let hosted: SpicepodStatus =
        serde_json::from_value(hosted_status.clone()).map_err(|source| {
            crate::error::Error::InvalidResponse {
                message: format!("Failed to read runtime instances for project {target}: {source}"),
            }
        })?;
    let mut pods: Vec<String> = hosted
        .pod_statuses
        .into_iter()
        .map(|pod| pod.name)
        .filter(|name| !name.is_empty())
        .collect();
    pods.sort();
    pods.dedup();
    if pods.is_empty() {
        return Err(Error::cloud_with_hint(
            CloudErrorCode::NotFound,
            format!("Project {target} has no running instances whose logs can be read."),
            format!("Check it with 'spice cloud status --project {target}'."),
        ));
    }
    Ok(pods)
}

fn runtime_logs_path(pod: &str, tail_lines: u32) -> String {
    format!(
        "/v1/spice_runtime/pods/{}/logs?tailLines={tail_lines}",
        urlencoding::encode(pod)
    )
}

fn parse_runtime_log_text(text: &str, source: &str) -> Vec<spice_cloud_client::types::LogEntry> {
    text.lines()
        .map(|line| parse_runtime_log_line(line.trim_end_matches('\r'), source))
        .collect()
}

fn parse_runtime_log_line(line: &str, source: &str) -> spice_cloud_client::types::LogEntry {
    let (first, after_first) = split_log_field(line);
    let (timestamp, remainder) = if chrono::DateTime::parse_from_rfc3339(first).is_ok() {
        (Some(first.to_string()), after_first)
    } else {
        (None, line)
    };
    let (candidate_level, after_level) = split_log_field(remainder);
    let normalized_level = candidate_level.to_ascii_lowercase();
    let (level, message) = if matches!(
        normalized_level.as_str(),
        "trace" | "debug" | "info" | "warn" | "error"
    ) {
        (Some(normalized_level), after_level)
    } else {
        (None, remainder)
    };

    spice_cloud_client::types::LogEntry {
        timestamp,
        level,
        message: message.to_string(),
        source: Some(source.to_string()),
    }
}

fn split_log_field(line: &str) -> (&str, &str) {
    let trimmed = line.trim_start();
    match trimmed.find(char::is_whitespace) {
        Some(index) => (&trimmed[..index], trimmed[index..].trim_start()),
        None => (trimmed, ""),
    }
}

fn filter_cloud_logs_since(
    mut logs: spice_cloud_client::types::LogsResponse,
    level: LogLevelFilter,
    since: Option<&chrono::DateTime<chrono::FixedOffset>>,
) -> spice_cloud_client::types::LogsResponse {
    logs.logs.retain(|entry| {
        level.admits(entry.level.as_deref())
            && since.is_none_or(|since| {
                entry.timestamp.as_deref().is_none_or(|timestamp| {
                    match chrono::DateTime::parse_from_rfc3339(timestamp) {
                        Ok(timestamp) => timestamp >= *since,
                        Err(_) => true,
                    }
                })
            })
    });
    logs
}

fn parse_local_log_lines(lines: Vec<String>) -> spice_cloud_client::types::LogsResponse {
    spice_cloud_client::types::LogsResponse {
        logs: lines
            .into_iter()
            .map(|line| parse_runtime_log_line(&line, "local"))
            .collect(),
    }
}

fn ensure_cloud_logs_follow_supported(follow: bool) -> Result<()> {
    if follow {
        return Err(Error::InvalidArgument {
            message: "`--follow` is not supported when logs are served by Spice Cloud. Omit `--follow` and rerun the command to fetch newer entries.".to_string(),
        });
    }
    Ok(())
}

fn render_cloud_logs(logs: spice_cloud_client::types::LogsResponse) {
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
}

async fn execute_project_create(args: &CreateProjectArgs, flag_org: Option<&str>) -> Result<()> {
    let create_region = validate_create_project_args(args)?;

    let client = connect(flag_org).await?;
    let spicepod_content = if let Some(path) = args.spicepod.as_deref() {
        Some(read_spicepod_file(path).await?)
    } else {
        None
    };

    let app = client
        .create_project(
            &args.name,
            &create_region,
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

    // The create response may omit `org`, so fall back to the org this
    // command acted on — the same org the new app was created in.
    let created = ProjectTarget::new(
        if app.org.is_empty() {
            resolve_org(flag_org)?
        } else {
            Some(app.org.clone())
        },
        app.name.clone(),
    );

    let app = if spicepod_content.is_some() || args.channel.is_some() {
        match client
            .update_project(
                &created,
                client::UpdateProjectParams {
                    spicepod: spicepod_content,
                    channel: args.channel,
                    ..client::UpdateProjectParams::default()
                },
            )
            .await
        {
            Ok(updated_app) => updated_app,
            Err(error) => {
                let update_error = error.to_string();
                let cleanup_result = client.delete_project(&created).await;
                let cleanup_message = match cleanup_result {
                    Ok(()) => "The app was deleted to roll back the failed create.".to_string(),
                    Err(cleanup_error) => format!(
                        "The app still exists, and an automatic delete attempt failed: {cleanup_error}. Run 'spice cloud api-keys --project {created}' if you need to inspect its provisioned API keys, or delete the app manually."
                    ),
                };
                return Err(crate::error::Error::InvalidResponse {
                    message: format!(
                        "Created app {created}, but failed to update spicepod/channel: {update_error}. {cleanup_message}"
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
    println!("\x1b[32m✓ Created app {created}\x1b[0m");
    if let Ok(api_keys) = client.get_api_keys(&created).await
        && let Some(api_key) = api_keys.api_key
    {
        println!("\nAPI Key: {api_key}");
        println!("\nSave this key - it won't be shown again.");
    }

    Ok(())
}

async fn execute_create_deployment(
    args: &CreateDeploymentArgs,
    flag_org: Option<&str>,
) -> Result<()> {
    let target = resolve_project_target(args.project.as_deref(), flag_org).await?;
    let client = connect_for_target(&target).await?;
    let deployment = client
        .create_deployment(
            &target,
            client::CreateDeploymentParams {
                image_tag: args.image.as_deref(),
                replicas: args.replicas,
                debug: args.debug,
                ..client::CreateDeploymentParams::default()
            },
        )
        .await?;
    if args.output == OutputFormat::Json {
        return write_json(&deployment);
    }
    println!(
        "\x1b[32m✓ Created deployment {} (status: {})\x1b[0m",
        deployment.id, deployment.status
    );

    Ok(())
}

fn validate_create_project_args(args: &CreateProjectArgs) -> Result<String> {
    let region = normalize_create_project_region(&args.region)?;

    if args.kind == ProjectKind::Cluster {
        if args.replicas != Some(1) {
            return Err(crate::error::Error::InvalidArgument {
                message: "Cluster apps require --replicas 1".to_string(),
            });
        }

        let mut missing = Vec::new();
        if args.executor_replicas.is_none() {
            missing.push("--executor-replicas");
        }
        if args.executor_cpu.is_none() {
            missing.push("--executor-cpu");
        }
        if args.executor_memory.is_none() {
            missing.push("--executor-memory");
        }

        if !missing.is_empty() {
            return Err(crate::error::Error::InvalidArgument {
                message: format!(
                    "Cluster apps require explicit executor configuration: {}",
                    missing.join(", ")
                ),
            });
        }
    }

    Ok(region)
}

async fn execute_project_get(args: &GetProjectArgs, flag_org: Option<&str>) -> Result<()> {
    let target = resolve_project_target(Some(&args.project), flag_org).await?;
    let client = connect_for_target(&target).await?;
    let app = client.get_project(&target).await?;

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

    Ok(())
}

async fn execute_project_update(args: &UpdateProjectArgs, flag_org: Option<&str>) -> Result<()> {
    let target = resolve_project_target(args.project.as_deref(), flag_org).await?;
    let client = connect_for_target(&target).await?;
    let spicepod_content = if let Some(path) = args.spicepod.as_deref() {
        Some(read_spicepod_file(path).await?)
    } else {
        None
    };

    let app = client
        .update_project(
            &target,
            client::UpdateProjectParams {
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

    Ok(())
}

async fn execute_project_delete(args: &DeleteProjectArgs, flag_org: Option<&str>) -> Result<()> {
    use std::io::Write;

    let (target, org_source) =
        resolve_project_target_with_source(Some(&args.project), flag_org).await?;

    if !args.yes {
        // Confirm against the fully-qualified name and say where the org
        // came from: with several orgs in play, the bare app name is not
        // enough to know what is about to be destroyed.
        announce_target("About to delete", &target, org_source, args.output);
        print!("Continue? [y/N] ");
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

    let client = connect_for_target(&target).await?;
    client.delete_project(&target).await?;
    if args.output == OutputFormat::Json {
        return write_json(&serde_json::json!({"app": target.display(), "status": "deleted"}));
    }
    println!("\x1b[32m✓ Deleted app {target}\x1b[0m");

    Ok(())
}

/// Deployment statuses that will not change without another deploy.
const DEPLOYMENT_TERMINAL_SUCCESS: [&str; 3] = ["succeeded", "success", "completed"];
const DEPLOYMENT_TERMINAL_FAILURE: [&str; 4] = ["failed", "error", "cancelled", "canceled"];

/// Whether a status is terminal, and if so whether it succeeded.
///
/// Unknown statuses are treated as still running: waiting a little longer is
/// recoverable, whereas declaring an in-flight deploy finished is not.
fn deployment_outcome(status: &str) -> Option<bool> {
    let status = status.trim().to_ascii_lowercase();
    if DEPLOYMENT_TERMINAL_SUCCESS.contains(&status.as_str()) {
        return Some(true);
    }
    if DEPLOYMENT_TERMINAL_FAILURE.contains(&status.as_str()) {
        return Some(false);
    }
    None
}

async fn execute_deploy(args: &DeployArgs, flag_org: Option<&str>) -> Result<()> {
    let (target, org_source) =
        resolve_project_target_with_source(args.project.as_deref(), flag_org).await?;
    announce_target("Deploying to", &target, org_source, args.output);
    let client = connect_for_target(&target).await?;

    // Resolve once: create_deployment and the wait loop both need the id.
    let project_id = client.resolve_id(&target).await?;
    let deployment = client
        .create_deployment_for_id(
            project_id,
            client::CreateDeploymentParams {
                image_tag: args.image.as_deref(),
                branch: args.branch.as_deref(),
                commit_sha: args.commit.as_deref(),
                replicas: args.replicas,
                debug: args.debug,
            },
            &target,
        )
        .await?;

    let is_json = args.output == OutputFormat::Json;
    if !is_json {
        println!(
            "\x1b[32m✓ Deployment {} started (status: {})\x1b[0m",
            deployment.id, deployment.status
        );
    }

    if !args.wait {
        if is_json {
            return write_json(&deployment);
        }
        println!(
            "  Track it with 'spice cloud deployments --project {target}', or re-run with --wait."
        );
        return Ok(());
    }

    let final_deployment = wait_for_deployment(
        &client,
        &target,
        project_id,
        deployment,
        &args.timeout,
        is_json,
    )
    .await?;

    if is_json {
        return write_json(&final_deployment);
    }

    Ok(())
}

/// Poll a deployment until it reaches a terminal status or the timeout elapses.
///
/// Polls the real status rather than sleeping a fixed interval, so a fast deploy
/// returns fast and a slow one is not cut short. A failed deployment is an error
/// so `spice cloud deploy --wait` can gate a script.
async fn wait_for_deployment(
    client: &CloudClient,
    target: &ProjectTarget,
    project_id: i64,
    mut deployment: Deployment,
    timeout: &str,
    quiet: bool,
) -> Result<Deployment> {
    let timeout = fundu::parse_duration(timeout).map_err(|e| {
        Error::cloud(
            CloudErrorCode::InvalidRequest,
            format!("Invalid --timeout value '{timeout}': {e}"),
        )
    })?;

    let start = std::time::Instant::now();
    let mut last_status = deployment.status.clone();
    let mut interval = std::time::Duration::from_secs(2);
    let max_interval = std::time::Duration::from_secs(15);

    loop {
        if let Some(succeeded) = deployment_outcome(&deployment.status) {
            if succeeded {
                if !quiet {
                    println!(
                        "\x1b[32m✓ Deployment {} {} after {}s\x1b[0m",
                        deployment.id,
                        deployment.status,
                        start.elapsed().as_secs()
                    );
                }
                return Ok(deployment);
            }

            let detail = deployment
                .error_message
                .as_deref()
                .map_or(String::new(), |error| format!(": {error}"));
            return Err(Error::cloud_with_hint(
                CloudErrorCode::DeployFailed,
                format!(
                    "Deployment {} for app {target} finished with status '{}'{detail}",
                    deployment.id, deployment.status
                ),
                format!("Inspect it with 'spice cloud logs --project {target} --level error'."),
            ));
        }

        if start.elapsed() >= timeout {
            // Distinct from `deploy_failed`: the deployment may still succeed,
            // so a script must be able to tell "it broke" from "I stopped
            // watching" without parsing the message.
            return Err(Error::cloud_with_hint(
                CloudErrorCode::DeployTimeout,
                format!(
                    "Timed out after {}s waiting for deployment {} (last status: {}). The deployment is still running.",
                    timeout.as_secs(),
                    deployment.id,
                    deployment.status
                ),
                format!(
                    "Wait longer with --timeout, or check 'spice cloud deployments --project {target}'."
                ),
            ));
        }

        tokio::time::sleep(interval).await;
        interval = (interval * 2).min(max_interval);

        // Poll by id: the project was resolved before the loop and its id
        // cannot change, so re-resolving by name each tick would repeat the
        // whole name lookup on every poll for the life of the wait.
        let deployments = client.list_deployments_for_id(project_id, 20, None).await?;
        let Some(refreshed) = deployments
            .into_iter()
            .find(|candidate| candidate.id == deployment.id)
        else {
            return Err(Error::cloud(
                CloudErrorCode::NotFound,
                format!(
                    "Deployment {} is no longer listed for app {target}.",
                    deployment.id
                ),
            ));
        };

        if !quiet && refreshed.status != last_status {
            println!("  status: {}", refreshed.status);
            last_status.clone_from(&refreshed.status);
        }
        deployment = refreshed;
    }
}

async fn execute_api_keys(args: &ApiKeysArgs, flag_org: Option<&str>) -> Result<()> {
    let target = resolve_project_target(args.project.as_deref(), flag_org).await?;
    let client = connect_for_target(&target).await?;

    if let Some(key_num) = args.regenerate {
        if key_num != 1 && key_num != 2 {
            return InvalidArgumentSnafu {
                message: "Key number must be 1 or 2",
            }
            .fail();
        }
        let response = client.regenerate_api_key(&target, key_num).await?;
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
        let keys = client.get_api_keys(&target).await?;
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

async fn execute_metrics(args: &MetricsArgs, flag_org: Option<&str>) -> Result<()> {
    let target = resolve_project_target(args.project.as_deref(), flag_org).await?;
    let client = connect_for_target(&target).await?;
    let app = client.get_project(&target).await?;

    let response = client
        .get_project_metrics(app.id, args.window.as_deref())
        .await?;

    if args.output == OutputFormat::Json {
        return write_json(&response);
    }

    if response.metrics.is_empty() {
        println!("No metrics available for {target}");
        return Ok(());
    }
    let mut table = TableOutput::new(metrics_table_headers());
    for (pod, m) in &response.metrics {
        table.add_row(metrics_table_row(pod, m));
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

fn metrics_table_headers() -> Vec<&'static str> {
    vec![
        "POD",
        "CPU %",
        "MEMORY",
        "DISK READ",
        "READ OPS",
        "DISK WRITE",
        "WRITE OPS",
    ]
}

fn metrics_table_row(pod: &str, m: &PodMetrics) -> Vec<String> {
    vec![
        pod.to_string(),
        m.cpu_usage_percent
            .map_or_else(|| "-".to_string(), |v| format!("{v:.1}")),
        m.memory_usage_bytes
            .map_or_else(|| "-".to_string(), format_bytes),
        m.disk_read_bytes
            .map_or_else(|| "-".to_string(), bytes::format_bytes_f64),
        m.disk_read_operations
            .map_or_else(|| "-".to_string(), |v| format!("{v:.1}")),
        m.disk_write_bytes
            .map_or_else(|| "-".to_string(), bytes::format_bytes_f64),
        m.disk_write_operations
            .map_or_else(|| "-".to_string(), |v| format!("{v:.1}")),
    ]
}

fn format_bytes(bytes: u64) -> String {
    bytes::NumBytes::from_bytes(bytes).to_string()
}

fn normalize_create_project_region(region: &str) -> Result<String> {
    let Some(endpoint_region) = normalize_data_region(region) else {
        return Err(crate::error::Error::InvalidArgument {
            message: format!(
                "Invalid region '{region}': expected lowercase letters, digits, and hyphens, starting and ending with a letter or digit"
            ),
        });
    };

    data_region_name(&endpoint_region).ok_or_else(|| crate::error::Error::InvalidArgument {
        message: format!("Invalid region '{region}': expected a Spice Cloud data region"),
    })
}

fn parse_create_project_region(region: &str) -> std::result::Result<String, String> {
    normalize_create_project_region(region).map_err(|error| match error {
        crate::error::Error::InvalidArgument { message } => message,
        error => error.to_string(),
    })
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

// ============================================================================
// Runtime inspection (data plane)
// ============================================================================

/// Header that pins a data-plane request to one instance.
///
/// Absent, the request goes to the app's general endpoint and is answered by
/// the deployment as a whole.
const TARGET_INSTANCE_HEADER: &str = "SCP-Target-Instance";

/// Annotation carrying the deployment an instance belongs to.
const DEPLOYMENT_ID_ANNOTATION: &str = "spice.ai/deployment-id";

/// `GET /v1/spice_runtime` — every instance serving the app.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct SpicepodStatusResponse {
    #[serde(default)]
    status: SpicepodStatus,
}

#[derive(Debug, Default, serde::Deserialize, serde::Serialize)]
struct SpicepodStatus {
    #[serde(rename = "createTime", default)]
    create_time: Option<String>,
    #[serde(rename = "podStatuses", default)]
    pod_statuses: Vec<InstanceStatus>,
}

/// One instance (pod) serving the app.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct InstanceStatus {
    #[serde(default)]
    name: String,
    #[serde(default)]
    phase: String,
    #[serde(rename = "startTime", default)]
    start_time: Option<String>,
    #[serde(rename = "spicedStatus", default)]
    spiced_status: Option<SpicedStatus>,
    #[serde(default)]
    annotations: Option<std::collections::HashMap<String, String>>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct SpicedStatus {
    #[serde(default)]
    health: Option<String>,
    #[serde(default)]
    ready: Option<bool>,
    #[serde(default)]
    terminating: Option<bool>,
}

impl InstanceStatus {
    /// The deployment this instance belongs to.
    fn deployment_id(&self) -> Option<&str> {
        self.annotations
            .as_ref()?
            .get(DEPLOYMENT_ID_ANNOTATION)
            .map(String::as_str)
    }

    /// The instance's lifecycle status.
    ///
    /// Mirrors the Spice Cloud portal's canonical derivation so the CLI reports
    /// the same word for the same state. Resolution order — first match wins:
    /// terminating, failed, deploying (spiced not reporting yet), then
    /// ready/unhealthy, then loading. `loading` is a real phase in Spice: the
    /// runtime loads its initial datasets before it becomes ready, and calling
    /// that "not ready" would read as a fault.
    fn status(&self) -> &'static str {
        let Some(spiced) = &self.spiced_status else {
            // A pod Kubernetes already reports as Failed is not merely starting.
            return if self.phase == "Failed" {
                "failed"
            } else {
                "deploying"
            };
        };

        if spiced.terminating == Some(true) {
            return "terminating";
        }
        if self.phase == "Failed" {
            return "failed";
        }
        if spiced.ready == Some(true) {
            return if spiced.health.as_deref() == Some("unhealthy") {
                "unhealthy"
            } else {
                "ready"
            };
        }
        "loading"
    }

    /// Whether this instance counts as a serving replica — ready, or ready but
    /// reporting a degraded health check.
    fn is_serving(&self) -> bool {
        matches!(self.status(), "ready" | "unhealthy")
    }
}

/// A dataset reported by an instance's `/v1/datasets?status=true` endpoint.
///
/// Mirrors the fields of `runtime_api_types::v1::datasets::DatasetInfo` that
/// matter for diagnosis. Declared locally rather than depending on the runtime
/// crate: the CLI must not pull in the runtime's dependency tree, and unknown
/// fields are ignored so a runtime newer than the CLI still parses.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct InstanceDatasetInfo {
    name: String,
    from: String,
    #[serde(default)]
    acceleration_enabled: bool,
    #[serde(default)]
    replication_enabled: bool,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    error_message: Option<String>,
}

/// Build a client for one project's running instances.
///
/// The management API does not expose runtime state, so this resolves the
/// project's region and API key through it and then targets the regional data
/// endpoint. `instance` pins the request to a single instance; unset, the
/// request reaches the project's general endpoint and describes the deployment
/// as a whole.
async fn project_runtime_context(
    ctx: &RuntimeContext,
    client: &CloudClient,
    target: &ProjectTarget,
    instance: Option<&str>,
) -> Result<RuntimeContext> {
    let project = client.get_project(target).await?;

    let region = project.region.clone().ok_or_else(|| {
        Error::cloud_with_hint(
            CloudErrorCode::NotFound,
            format!(
                "Project {target} does not report a region, so its instance endpoint is unknown."
            ),
            format!("Check it with 'spice cloud status --project {target}'."),
        )
    })?;
    let region =
        spice_cloud_client::endpoints::normalize_data_region(&region).ok_or_else(|| {
            Error::cloud(
                CloudErrorCode::InvalidRequest,
                format!("Project {target} reports an unrecognized region '{region}'."),
            )
        })?;

    // Prefer the key the management API reports for this project; fall back to
    // a key stored for the same org only if the API withholds one. Never reach
    // for another org's key.
    let api_key = match client.get_api_keys(target).await?.api_key {
        Some(api_key) => Some(api_key),
        None => org::api_key_for_org(target.org.as_deref()),
    };
    let api_key = api_key.ok_or_else(|| {
        Error::cloud_with_hint(
            CloudErrorCode::Forbidden,
            format!(
                "No API key is available for project {target}, so its instances cannot be queried."
            ),
            format!("Generate one with 'spice cloud api-keys --project {target} --regenerate 1'."),
        )
    })?;

    let mut runtime_ctx = RuntimeContext::with_args(
        None,
        Some(api_key),
        Some(&region),
        ctx.tls_root_certificate_file().map(ToString::to_string),
    )?;

    if let Some(instance) = instance {
        runtime_ctx.add_headers(std::collections::HashMap::from([(
            TARGET_INSTANCE_HEADER.to_string(),
            instance.to_string(),
        )]));
    }

    Ok(runtime_ctx)
}

/// Name what a report describes: one pinned instance, or the project as a whole.
///
/// Without this an operator cannot tell whether "ready" describes their whole
/// deployment or one instance of several.
fn describe_scope(target: &ProjectTarget, pinned_instance: Option<&str>) -> String {
    match pinned_instance {
        Some(instance) => format!("Instance {instance} of project {target}"),
        None => format!("Project {target}"),
    }
}

/// Read a JSON document from a running app instance.
async fn fetch_instance_json<T: serde::de::DeserializeOwned>(
    ctx: &RuntimeContext,
    path: &str,
    target: &ProjectTarget,
) -> Result<T> {
    let response = ctx.get(path).await.map_err(|err| {
        Error::cloud_with_hint(
            CloudErrorCode::NotFound,
            format!("Could not reach the instance for app {target}: {err}"),
            format!(
                "The app may not be running yet — check 'spice cloud status --project {target}'."
            ),
        )
    })?;

    // Check the status before trusting the body: an unauthorized or errored
    // response would otherwise deserialize to an empty list and read as "this
    // app has no datasets", hiding the very failure being investigated.
    let response = crate::error::check_response(response, ctx.http_endpoint()).await?;

    response
        .json::<T>()
        .await
        .map_err(|err| crate::error::Error::InvalidResponse {
            message: format!("Failed to parse {path} for app {target}: {err}"),
        })
}

fn print_instance_datasets(
    datasets: &[InstanceDatasetInfo],
    target: &ProjectTarget,
    pinned_instance: Option<&str>,
    output: OutputFormat,
) -> Result<()> {
    if output == OutputFormat::Json {
        return write_json(&datasets);
    }

    if datasets.is_empty() {
        println!(
            "{} has no datasets configured.",
            describe_scope(target, pinned_instance)
        );
        return Ok(());
    }

    println!("{}", describe_scope(target, pinned_instance));
    let mut table = TableOutput::new(vec!["DATASET", "FROM", "STATUS", "ACCELERATED", "ERROR"]);
    for dataset in datasets {
        table.add_row(vec![
            dataset.name.clone(),
            dataset.from.clone(),
            dataset.status.clone().unwrap_or_else(|| "-".to_string()),
            if dataset.acceleration_enabled {
                "✓"
            } else {
                ""
            }
            .to_string(),
            truncate_for_table(dataset.error_message.as_deref().unwrap_or("-"), 50),
        ]);
    }
    table.print();

    let unhealthy: Vec<&InstanceDatasetInfo> = datasets
        .iter()
        .filter(|dataset| dataset_needs_attention(dataset.status.as_deref()))
        .collect();
    if !unhealthy.is_empty() {
        println!();
        println!(
            "{} of {} datasets are not Ready: {}.",
            unhealthy.len(),
            datasets.len(),
            unhealthy
                .iter()
                .map(|dataset| dataset.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
        println!("  Logs: spice cloud logs --project {target} --level error");
    }

    Ok(())
}

/// Whether a dataset status warrants an operator's attention.
///
/// `Refreshing` and `Initializing` are healthy in-progress states, so only a
/// genuinely stuck or failed dataset is called out. An unreported status is
/// treated as fine: the runtime omits it unless `status=true` was honored.
fn dataset_needs_attention(status: Option<&str>) -> bool {
    status.is_some_and(|status| {
        !matches!(
            status.to_ascii_lowercase().as_str(),
            "ready" | "refreshing" | "initializing" | "disabled"
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn metrics_table_row_matches_header_count() {
        // Regression for #9989: row was emitting one more cell than the header
        // had columns, so the `disk_write_operations` value rendered without a
        // label and the disk columns were captioned with unrelated names.
        let headers = metrics_table_headers();

        let none_metrics = PodMetrics::default();
        let none_row = metrics_table_row("pod-none", &none_metrics);
        assert_eq!(
            none_row.len(),
            headers.len(),
            "row count must match header count when fields are None"
        );

        let full_metrics = PodMetrics {
            cpu_usage_percent: Some(123.4),
            memory_usage_bytes: Some(1024 * 1024 * 1024),
            disk_read_bytes: Some(2048.0),
            disk_read_operations: Some(11.0),
            disk_write_bytes: Some(4096.0),
            disk_write_operations: Some(22.0),
        };
        let full_row = metrics_table_row("pod-full", &full_metrics);
        assert_eq!(
            full_row.len(),
            headers.len(),
            "row count must match header count when fields are populated"
        );
    }

    #[test]
    fn metrics_table_headers_label_every_disk_column() {
        // Regression for #9989: the original labels "DISK USED / DISK AVAIL /
        // DISK CAP" were both wrong (they described capacity, not I/O) and
        // omitted `disk_write_operations` entirely. Lock the labels in.
        let headers = metrics_table_headers();
        assert_eq!(
            headers,
            vec![
                "POD",
                "CPU %",
                "MEMORY",
                "DISK READ",
                "READ OPS",
                "DISK WRITE",
                "WRITE OPS",
            ]
        );
    }

    #[test]
    fn metrics_table_row_renders_dash_for_missing_values() {
        let m = PodMetrics::default();
        let row = metrics_table_row("p", &m);
        // Pod name is always present; the six metric cells should be "-".
        assert_eq!(row[0], "p");
        assert!(
            row[1..].iter().all(|cell| cell == "-"),
            "missing metric cells should render as '-', got: {row:?}"
        );
    }

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
    fn link_requires_a_terminal_even_with_an_explicit_target() {
        let error = ensure_link_chooser_tty(false).expect_err("link is interactive-only");
        assert!(error.to_string().contains("interactive terminal"));
        ensure_link_chooser_tty(true).expect("an interactive terminal is accepted");
    }

    #[test]
    fn spicepod_secret_references_are_sorted_and_deduplicated() {
        assert_eq!(
            secret_references(
                "password: ${ secrets: ZETA }\nuser: ${secrets : ALPHA}\nagain: ${secrets:ZETA}\nenv: ${ env:IGNORED }\nempty: ${secrets:}\nbroken: ${secrets:OPEN"
            ),
            vec!["ALPHA".to_string(), "ZETA".to_string()]
        );
    }

    #[test]
    fn is_cloud_unauthorized_error_matches_a_rejected_credential() {
        let err = Error::cloud(CloudErrorCode::TokenExpired, "Unauthorized: token expired");

        assert!(client::is_unauthorized_auth_context_error(&err));
    }

    #[test]
    fn is_cloud_unauthorized_error_rejects_unrelated_errors() {
        // A forbidden response means the credential is valid but the action is
        // not allowed — that must not be mistaken for a missing user identity.
        let err = Error::cloud(CloudErrorCode::Forbidden, "Forbidden: missing scope");

        assert!(!client::is_unauthorized_auth_context_error(&err));
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

    fn create_project_args(kind: ProjectKind, replicas: Option<i32>) -> CreateProjectArgs {
        CreateProjectArgs {
            name: "app".to_string(),
            region: "us-east-1-prod-aws-data".to_string(),
            kind,
            description: None,
            visibility: "private".to_string(),
            replicas,
            cpu: None,
            memory: None,
            storage_size_gb: None,
            executor_replicas: None,
            executor_cpu: None,
            executor_memory: None,
            spicepod: None,
            channel: None,
            output: OutputFormat::Table,
        }
    }

    fn cluster_app_args(replicas: Option<i32>) -> CreateProjectArgs {
        let mut args = create_project_args(ProjectKind::Cluster, replicas);
        args.executor_replicas = Some(1);
        args.executor_cpu = Some(1);
        args.executor_memory = Some(bytes::NumBytes::from_bytes(1024));
        args
    }

    #[test]
    fn create_cluster_requires_explicit_single_replica() {
        let err = validate_create_project_args(&create_project_args(ProjectKind::Cluster, None))
            .expect_err("cluster without replicas should fail");

        assert_eq!(
            err.to_string(),
            "Invalid argument: Cluster apps require --replicas 1"
        );
    }

    #[test]
    fn create_cluster_requires_executor_configuration() {
        let err = validate_create_project_args(&create_project_args(ProjectKind::Cluster, Some(1)))
            .expect_err("cluster without executor configuration should fail");

        assert_eq!(
            err.to_string(),
            "Invalid argument: Cluster apps require explicit executor configuration: --executor-replicas, --executor-cpu, --executor-memory"
        );
    }

    #[test]
    fn create_cluster_accepts_one_replica() {
        validate_create_project_args(&cluster_app_args(Some(1)))
            .expect("cluster with one scheduler replica should pass");
    }

    #[test]
    fn create_project_rejects_invalid_region_syntax() {
        let mut args = create_project_args(ProjectKind::Set, None);
        args.region = "bad_region".to_string();

        let err = validate_create_project_args(&args).expect_err("invalid region should fail");

        assert!(err.to_string().contains("Invalid region 'bad_region'"));
    }

    #[test]
    fn create_project_region_accepts_short_and_data_region_names() {
        assert_eq!(
            normalize_create_project_region("us-east-1").expect("short region should normalize"),
            "us-east-1-prod-aws-data"
        );
        assert_eq!(
            normalize_create_project_region("us-east-1-prod-aws-data")
                .expect("data region should normalize"),
            "us-east-1-prod-aws-data"
        );
    }

    fn test_project(org: &str, name: &str) -> spice_cloud_client::types::Project {
        spice_cloud_client::types::Project {
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
    fn display_project_name_uses_project_org_when_present() {
        let project = test_project("analytics", "dashboard");
        assert_eq!(
            display_project_name(&project, "fallback"),
            "analytics/dashboard"
        );
    }

    #[test]
    fn display_project_name_falls_back_to_context_org_when_project_org_is_empty() {
        let project = test_project("", "dashboard");
        assert_eq!(
            display_project_name(&project, "analytics"),
            "analytics/dashboard"
        );
    }

    #[test]
    fn display_project_name_omits_leading_slash_when_org_unavailable() {
        let project = test_project("", "dashboard");
        assert_eq!(display_project_name(&project, ""), "dashboard");
    }

    #[test]
    fn backfill_project_orgs_fills_empty_org_from_context() {
        let mut projects = vec![
            test_project("", "ltd-mint"),
            test_project("", "zippy-cayenne"),
        ];
        backfill_project_orgs(&mut projects, "Jeadie");
        assert_eq!(projects[0].org, "Jeadie");
        assert_eq!(projects[1].org, "Jeadie");
    }

    #[test]
    fn backfill_project_orgs_preserves_existing_org() {
        let mut projects = vec![
            test_project("analytics", "dashboard"),
            test_project("", "ltd-mint"),
        ];
        backfill_project_orgs(&mut projects, "Jeadie");
        assert_eq!(projects[0].org, "analytics");
        assert_eq!(projects[1].org, "Jeadie");
    }

    #[test]
    fn backfill_project_orgs_noop_when_context_empty() {
        let mut projects = vec![test_project("", "ltd-mint")];
        backfill_project_orgs(&mut projects, "");
        assert_eq!(projects[0].org, "");
    }

    // ========================================================================
    // Organization context
    // ========================================================================

    #[test]
    fn conflicting_explicit_orgs_are_refused_not_ranked() {
        // Regression guard for the precedence inversion this replaced: an
        // explicit `--org` used to lose silently to the app argument, with only
        // a warning that `--machine` output never shows. A wrong-organization
        // deploy is not recoverable, so two explicit signals must not be ranked.
        let err = ensure_orgs_agree(
            "spicehq",
            "the app argument",
            Some("lukekim"),
            OrgSource::Flag,
        )
        .expect_err("conflicting explicit orgs must fail");

        assert_eq!(err.cloud_code(), Some(CloudErrorCode::OrgConflict));
        let rendered = err.to_string();
        assert!(
            rendered.contains("'spicehq'") && rendered.contains("'lukekim'"),
            "the error must name both organizations: {rendered}"
        );
    }

    #[test]
    fn the_environment_is_explicit_enough_to_conflict() {
        let err = ensure_orgs_agree(
            "spicehq",
            "the enrolled instance",
            Some("lukekim"),
            OrgSource::Environment,
        )
        .expect_err("SPICE_CLOUD_ORG is an explicit statement of intent");

        assert_eq!(err.cloud_code(), Some(CloudErrorCode::OrgConflict));
    }

    #[test]
    fn a_standing_active_org_yields_to_any_explicit_signal() {
        // The active org is a default, not a statement about this command, so
        // overriding it must not be an error — otherwise `org use` would make
        // every qualified command fail.
        ensure_orgs_agree(
            "spicehq",
            "the app argument",
            Some("lukekim"),
            OrgSource::ActiveOrg,
        )
        .expect("an explicit org may override the active org");
        ensure_orgs_agree(
            "spicehq",
            "the app argument",
            Some("lukekim"),
            OrgSource::LinkedApp,
        )
        .expect("an explicit org may override an enrolled instance");
    }

    #[test]
    fn agreeing_orgs_are_never_a_conflict() {
        ensure_orgs_agree(
            "spicehq",
            "the app argument",
            Some("SpiceHQ"),
            OrgSource::Flag,
        )
        .expect("org comparison is case-insensitive");
        ensure_orgs_agree("spicehq", "the app argument", None, OrgSource::Credential)
            .expect("no other org means nothing to conflict with");
    }

    #[test]
    fn org_source_labels_name_something_the_user_can_act_on() {
        assert_eq!(OrgSource::Flag.label(), "--org flag");
        assert_eq!(OrgSource::Environment.label(), "SPICE_CLOUD_ORG");
        assert_eq!(OrgSource::LinkedApp.label(), "enrolled instance");
        assert_eq!(OrgSource::ActiveOrg.label(), "active org");
    }

    #[test]
    fn org_rows_mark_the_active_org_and_stored_credentials() {
        let listed = vec![
            spice_cloud_client::types::Org {
                id: Some(1),
                name: "spicehq".to_string(),
                display_name: None,
                role: Some("member".to_string()),
            },
            spice_cloud_client::types::Org {
                id: Some(2),
                name: "lukekim".to_string(),
                display_name: None,
                role: Some("owner".to_string()),
            },
        ];
        let stored = BTreeSet::from(["spicehq".to_string()]);

        let rows = build_org_rows(Some(&listed), Some("lukekim"), Some("spicehq"), &stored);

        assert_eq!(rows.len(), 2);
        // Sorted by name, so lukekim comes first.
        assert_eq!(rows[0].name, "lukekim");
        assert!(
            !rows[0].active,
            "the active org overrides the credential org"
        );
        assert!(!rows[0].has_credential);
        assert_eq!(rows[1].name, "spicehq");
        assert!(rows[1].active);
        assert!(rows[1].has_credential);
        assert_eq!(rows[1].role.as_deref(), Some("member"));
    }

    #[test]
    fn org_rows_fall_back_to_local_knowledge_when_the_api_cannot_list() {
        // A deployment without an org listing must still report the orgs the
        // CLI can prove it knows about, rather than claiming the user has none.
        let stored = BTreeSet::from(["spicehq".to_string()]);

        let rows = build_org_rows(None, Some("lukekim"), None, &stored);

        let names: Vec<&str> = rows.iter().map(|row| row.name.as_str()).collect();
        assert_eq!(names, vec!["lukekim", "spicehq"]);
        assert!(
            rows[0].active,
            "with no active org selected, the credential's org is the one in effect"
        );
    }

    #[test]
    fn org_rows_do_not_duplicate_an_org_reported_by_several_sources() {
        let listed = vec![spice_cloud_client::types::Org {
            id: Some(1),
            name: "spicehq".to_string(),
            display_name: None,
            role: None,
        }];
        let stored = BTreeSet::from(["spicehq".to_string()]);

        let rows = build_org_rows(Some(&listed), Some("spicehq"), Some("spicehq"), &stored);

        assert_eq!(rows.len(), 1, "one org should produce one row: {rows:?}");
        assert_eq!(rows[0].id, Some(1), "the API's richer record should win");
    }

    #[test]
    fn org_credentials_are_written_where_the_reader_looks_for_them() {
        // `merge_auth_config("SPICEAI", &[(key, _)])` writes `SPICE_SPICEAI_{key}`.
        // If that name and `org_token_var` ever drift apart, a login would
        // succeed and every later command would fail as unauthenticated.
        for org in ["spicehq", "spice-hq", "acme.co"] {
            assert_eq!(
                format!("SPICE_SPICEAI_{}", credential_key(Some(org))),
                org::org_token_var(org),
                "the write and read paths must agree for org '{org}'"
            );
        }
    }

    #[test]
    fn a_credential_with_no_org_stays_the_default_one() {
        // Preserves single-org behavior: `spice cloud login` with no --org keeps
        // writing SPICE_SPICEAI_TOKEN, so nothing changes for existing users.
        assert_eq!(credential_key(None), "TOKEN");
        assert_eq!(
            format!("SPICE_SPICEAI_{}", credential_key(None)),
            org::DEFAULT_TOKEN_VAR
        );
    }

    #[test]
    fn an_org_credential_never_overwrites_the_default_one() {
        assert_ne!(credential_key(Some("spicehq")), credential_key(None));
    }

    #[test]
    fn a_named_machine_login_does_not_replace_the_default_user_token() {
        let values = machine_login_values("machine-token", Some("acme"));
        let keys: Vec<String> = values.iter().map(|(key, _)| key.clone()).collect();

        assert_eq!(keys, vec![credential_key(Some("acme"))]);
        assert!(!keys.contains(&credential_key(None)));
    }

    #[test]
    fn keychain_login_indexes_every_per_org_token_and_no_other_credential() {
        let values = user_login_values(
            "user-token",
            Some("personal-api-key"),
            Some("personal"),
            Some("acme"),
        );
        let value_refs: Vec<(&str, &str)> = values
            .iter()
            .map(|(key, value)| (key.as_str(), *value))
            .collect();

        assert_eq!(
            keychain_login_orgs(&value_refs),
            BTreeSet::from(["acme".to_string(), "personal".to_string()])
        );
    }

    #[test]
    fn a_user_login_without_an_app_key_does_not_clear_data_plane_credentials() {
        let values = user_login_values("user-token", None, Some("personal"), Some("acme"));

        assert!(
            values.iter().all(|(key, _)| !key.contains("API_KEY")),
            "an absent API key must produce no API-key write: {values:?}"
        );
    }

    #[test]
    fn a_cross_org_user_login_does_not_relabel_or_replace_the_data_plane_key() {
        let values = user_login_values(
            "user-token",
            Some("personal-api-key"),
            Some("personal"),
            Some("acme"),
        );
        let keys: Vec<String> = values.iter().map(|(key, _)| key.clone()).collect();

        assert!(keys.contains(&credential_key(None)));
        assert!(keys.contains(&credential_key(Some("personal"))));
        assert!(keys.contains(&credential_key(Some("acme"))));
        assert!(keys.contains(&api_key_credential_key(Some("personal"))));
        assert!(!keys.contains(&api_key_credential_key(None)));
        assert!(!keys.contains(&api_key_credential_key(Some("acme"))));
    }

    #[test]
    fn unlink_uses_the_configured_cloud_api_when_identity_has_no_endpoint() {
        let configured = "https://staging.api.spice.ai";

        assert_eq!(release_endpoint(None, configured), configured);
        assert_eq!(
            release_endpoint(Some("https://identity.example"), configured),
            "https://identity.example"
        );
    }

    #[test]
    fn org_api_keys_are_written_where_the_reader_looks_for_them() {
        // The app API key must be filed per-org too. Writing it to the shared
        // default let a second org's login replace the first org's data-plane
        // key, and left it behind when that org logged out.
        for org in ["spicehq", "spice-hq"] {
            assert_eq!(
                format!("SPICE_SPICEAI_{}", api_key_credential_key(Some(org))),
                org::org_api_key_var(org),
                "the API-key write and read paths must agree for org '{org}'"
            );
        }
        assert_eq!(
            format!("SPICE_SPICEAI_{}", api_key_credential_key(None)),
            org::DEFAULT_API_KEY_VAR
        );
    }

    #[test]
    fn logout_clears_both_credentials_for_an_org() {
        // A token without its API key (or vice versa) is a half-logged-out
        // state that later commands can still authenticate with.
        let keys = default_credential_keys();
        assert!(keys.contains(&org::DEFAULT_TOKEN_VAR.to_string()));
        assert!(keys.contains(&org::DEFAULT_API_KEY_VAR.to_string()));
    }

    fn instance(phase: &str, spiced: Option<SpicedStatus>) -> InstanceStatus {
        InstanceStatus {
            name: "spicepod-app-abc-0-0".to_string(),
            phase: phase.to_string(),
            start_time: None,
            spiced_status: spiced,
            annotations: None,
        }
    }

    fn spiced(
        ready: Option<bool>,
        health: Option<&str>,
        terminating: Option<bool>,
    ) -> SpicedStatus {
        SpicedStatus {
            health: health.map(ToString::to_string),
            ready,
            terminating,
        }
    }

    #[test]
    fn instance_status_matches_the_portal_resolution_order() {
        // The portal documents this derivation as the single source of truth
        // and warns against re-deriving it inline, because ad-hoc versions
        // drifted and labelled the same pod state differently per surface.
        // Terminating wins over everything, including a Failed phase.
        assert_eq!(
            instance("Failed", Some(spiced(Some(true), None, Some(true)))).status(),
            "terminating"
        );
        assert_eq!(
            instance("Failed", Some(spiced(Some(false), None, None))).status(),
            "failed"
        );
        // spiced not reporting yet — the container is still coming up.
        assert_eq!(instance("Pending", None).status(), "deploying");
        // ...but a Failed pod that never reported is failed, not deploying.
        assert_eq!(instance("Failed", None).status(), "failed");
        assert_eq!(
            instance("Running", Some(spiced(Some(true), Some("healthy"), None))).status(),
            "ready"
        );
        assert_eq!(
            instance("Running", Some(spiced(Some(true), Some("unhealthy"), None))).status(),
            "unhealthy"
        );
        // Up but not ready is `loading`, a real phase in Spice — the runtime
        // loads its initial datasets before becoming ready.
        assert_eq!(
            instance("Running", Some(spiced(Some(false), None, None))).status(),
            "loading"
        );
    }

    #[test]
    fn serving_replicas_include_degraded_but_ready_instances() {
        // A degraded instance is still taking traffic, so it counts toward the
        // serving total; counting it as down would misreport capacity.
        assert!(
            instance("Running", Some(spiced(Some(true), Some("unhealthy"), None))).is_serving()
        );
        assert!(instance("Running", Some(spiced(Some(true), None, None))).is_serving());
        assert!(!instance("Running", Some(spiced(Some(false), None, None))).is_serving());
        assert!(!instance("Pending", None).is_serving());
    }

    #[test]
    fn the_deployment_annotation_ties_an_instance_to_its_deployment() {
        let mut with_annotation = instance("Running", None);
        with_annotation.annotations = Some(std::collections::HashMap::from([(
            "spice.ai/deployment-id".to_string(),
            "4821".to_string(),
        )]));
        assert_eq!(with_annotation.deployment_id(), Some("4821"));
        assert_eq!(instance("Running", None).deployment_id(), None);
    }

    #[test]
    fn output_names_what_actually_answered() {
        // "Ready" means something different for one replica than for a whole
        // deployment, so the scope has to be stated either way.
        let target = ProjectTarget::new(Some("spicehq".to_string()), "team-app");
        assert_eq!(describe_scope(&target, None), "Project spicehq/team-app");
        assert_eq!(
            describe_scope(&target, Some("spicepod-team-app-abc-0-0")),
            "Instance spicepod-team-app-abc-0-0 of project spicehq/team-app"
        );
    }

    #[test]
    fn instance_listing_parses_the_runtime_payload() {
        let body = r#"{
            "status": {
                "createTime": "2026-08-04T21:00:00Z",
                "podStatuses": [
                    {"uid":"u1","name":"spicepod-app-abc-0-0","phase":"Running","ip":"10.0.0.1","port":8090,
                     "startTime":"2026-08-04T21:01:00Z",
                     "spicedStatus":{"health":"healthy","ready":true},
                     "annotations":{"spice.ai/deployment-id":"4821"}},
                    {"uid":"u2","name":"spicepod-app-abc-0-1","phase":"Pending","ip":"","port":8090}
                ]
            }
        }"#;

        let parsed: SpicepodStatusResponse =
            serde_json::from_str(body).expect("runtime payload should deserialize");
        let instances = parsed.status.pod_statuses;

        assert_eq!(instances.len(), 2);
        assert_eq!(instances[0].status(), "ready");
        assert_eq!(instances[0].deployment_id(), Some("4821"));
        // The second pod omits spicedStatus entirely — it must not fail to parse.
        assert_eq!(instances[1].status(), "deploying");
        assert_eq!(instances.iter().filter(|i| i.is_serving()).count(), 1);
    }

    #[test]
    fn datasets_needing_attention_exclude_healthy_progress_states() {
        // Refreshing and Initializing are healthy; flagging them would train
        // operators to ignore the warning.
        assert!(!dataset_needs_attention(Some("Ready")));
        assert!(!dataset_needs_attention(Some("Refreshing")));
        assert!(!dataset_needs_attention(Some("Initializing")));
        assert!(!dataset_needs_attention(Some("Disabled")));
        assert!(dataset_needs_attention(Some("Error")));
        // An absent status means the runtime did not report one, not a fault.
        assert!(!dataset_needs_attention(None));
    }

    // ========================================================================
    // Deploy and logs
    // ========================================================================

    #[test]
    fn deployment_outcome_classifies_terminal_statuses() {
        assert_eq!(deployment_outcome("succeeded"), Some(true));
        assert_eq!(deployment_outcome("Completed"), Some(true));
        assert_eq!(deployment_outcome("failed"), Some(false));
        assert_eq!(deployment_outcome("CANCELLED"), Some(false));
    }

    #[test]
    fn deploy_timeout_is_distinguishable_from_deploy_failure() {
        // A script gating on exit code must be able to tell "the deploy broke"
        // from "I stopped watching" without parsing message text.
        assert_ne!(
            CloudErrorCode::DeployTimeout.as_str(),
            CloudErrorCode::DeployFailed.as_str()
        );
        assert_eq!(CloudErrorCode::DeployTimeout.as_str(), "deploy_timeout");
    }

    #[test]
    fn deployment_outcome_treats_unknown_statuses_as_still_running() {
        // Declaring an in-flight deploy finished is unrecoverable; waiting a
        // little longer is not. Unknown statuses must keep the poll going.
        for status in ["pending", "queued", "in_progress", "rolling_out", ""] {
            assert_eq!(
                deployment_outcome(status),
                None,
                "'{status}' must not be treated as terminal"
            );
        }
    }

    #[test]
    fn log_level_filter_keeps_entries_at_or_above_the_threshold() {
        assert!(LogLevelFilter::All.admits(Some("debug")));
        assert!(LogLevelFilter::Warn.admits(Some("warn")));
        assert!(LogLevelFilter::Warn.admits(Some("ERROR")));
        assert!(!LogLevelFilter::Warn.admits(Some("info")));
        assert!(LogLevelFilter::Error.admits(Some("error")));
        assert!(!LogLevelFilter::Error.admits(Some("warn")));
    }

    #[test]
    fn log_level_filter_keeps_entries_with_no_level() {
        // An unlabelled line is often the panic or stack trace being hunted;
        // dropping it would hide the failure the filter was meant to surface.
        assert!(LogLevelFilter::Error.admits(None));
    }

    #[test]
    fn a_nonempty_cloud_log_response_never_falls_back_after_filtering() {
        let logs = spice_cloud_client::types::LogsResponse {
            logs: vec![spice_cloud_client::types::LogEntry {
                timestamp: None,
                level: Some("info".to_string()),
                message: "healthy".to_string(),
                source: None,
            }],
        };

        let filtered = filter_cloud_logs_since(logs, LogLevelFilter::Error, None);
        assert!(filtered.logs.is_empty());
    }

    #[test]
    fn runtime_log_route_maps_limit_and_escapes_the_pod_segment() {
        assert_eq!(
            runtime_logs_path("pod/name", 250),
            "/v1/spice_runtime/pods/pod%2Fname/logs?tailLines=250"
        );
        runtime_log_tail_lines(0).expect_err("zero limit must be rejected");
        assert_eq!(runtime_log_tail_lines(42).expect("valid limit"), 42);
    }

    #[test]
    fn runtime_log_pods_use_a_placeholder_only_for_standalone_status() {
        let target = ProjectTarget::new(Some("acme".to_string()), "analytics");
        let standalone = serde_json::json!({
            "phase": "Ready",
            "reason": "runtime is ready"
        });
        assert_eq!(
            runtime_log_pods(&standalone, &target).expect("standalone status"),
            vec![STANDALONE_LOG_POD_PLACEHOLDER.to_string()]
        );

        let hosted = serde_json::json!({
            "status": {
                "podStatuses": [
                    {"name": "analytics-b", "phase": "Running"},
                    {"name": "analytics-a", "phase": "Running"},
                    {"name": "analytics-a", "phase": "Running"}
                ]
            }
        });
        assert_eq!(
            runtime_log_pods(&hosted, &target).expect("hosted status"),
            vec!["analytics-a".to_string(), "analytics-b".to_string()]
        );

        let hosted_without_pods = serde_json::json!({"status": {"podStatuses": []}});
        runtime_log_pods(&hosted_without_pods, &target)
            .expect_err("hosted status without pods must be rejected");
    }

    #[test]
    fn raw_runtime_logs_are_parsed_and_filtered_since_client_side() {
        let raw = concat!(
            "2026-08-20T00:00:00Z  INFO runtime: old\n",
            "2026-08-20T00:00:02+00:00 ERROR runtime: new\n",
            "panic continuation without a timestamp\n"
        );
        let logs = spice_cloud_client::types::LogsResponse {
            logs: parse_runtime_log_text(raw, "analytics-a"),
        };
        assert_eq!(logs.logs.len(), 3);
        assert_eq!(logs.logs[0].level.as_deref(), Some("info"));
        assert_eq!(logs.logs[0].message, "runtime: old");
        assert_eq!(logs.logs[0].source.as_deref(), Some("analytics-a"));

        let since =
            chrono::DateTime::parse_from_rfc3339("2026-08-20T00:00:01Z").expect("valid timestamp");
        let filtered = filter_cloud_logs_since(logs, LogLevelFilter::All, Some(&since));
        let messages: Vec<&str> = filtered
            .logs
            .iter()
            .map(|entry| entry.message.as_str())
            .collect();
        assert_eq!(
            messages,
            vec!["runtime: new", "panic continuation without a timestamp"]
        );
    }

    #[test]
    fn local_fallback_logs_apply_the_same_level_and_since_filters() {
        let logs = parse_local_log_lines(vec![
            "2026-08-20T00:00:00Z INFO runtime: old".to_string(),
            "2026-08-20T00:00:02Z ERROR runtime: failed".to_string(),
        ]);
        let since =
            chrono::DateTime::parse_from_rfc3339("2026-08-20T00:00:01Z").expect("valid timestamp");
        let filtered = filter_cloud_logs_since(logs, LogLevelFilter::Error, Some(&since));

        assert_eq!(filtered.logs.len(), 1);
        assert_eq!(filtered.logs[0].message, "runtime: failed");
        assert_eq!(filtered.logs[0].level.as_deref(), Some("error"));
        assert_eq!(filtered.logs[0].source.as_deref(), Some("local"));
    }

    #[test]
    fn cloud_logs_reject_follow_instead_of_returning_a_one_shot_response() {
        let error = ensure_cloud_logs_follow_supported(true)
            .expect_err("a successful Cloud response cannot honor follow");
        assert!(error.to_string().contains("--follow"));
        ensure_cloud_logs_follow_supported(false).expect("a one-shot request is supported");
    }

    #[test]
    fn logout_does_not_claim_success_after_a_keychain_removal_failure() {
        let error = ensure_keychain_credentials_removed(&["SPICE_SPICEAI_TOKEN_ACME".to_string()])
            .expect_err("a usable keychain credential must fail logout");

        assert!(error.to_string().contains("may still be usable"));
    }

    #[test]
    fn short_commit_abbreviates_to_seven_characters() {
        assert_eq!(short_commit(Some("24cb0e71fd0123456789")), "24cb0e7");
        assert_eq!(short_commit(Some("abc")), "abc");
        assert_eq!(short_commit(None), "-");
        assert_eq!(short_commit(Some("")), "-");
    }

    #[test]
    fn logout_matches_every_assignment_form_the_reader_accepts() {
        // The reader trims around `=`, so all of these are readable
        // credentials. Matching raw text missed the spaced forms, leaving them
        // live while logout reported success — twice, since the first fix
        // handled indentation but not spacing around `=`.
        let keys = vec!["SPICE_SPICEAI_TOKEN".to_string()];
        for line in [
            "SPICE_SPICEAI_TOKEN=abc",
            "  SPICE_SPICEAI_TOKEN=abc",
            "SPICE_SPICEAI_TOKEN =abc",
            "SPICE_SPICEAI_TOKEN = abc",
            "\texport SPICE_SPICEAI_TOKEN=abc",
        ] {
            assert!(assigns_any(line, &keys), "should match: {line:?}");
        }
    }

    #[test]
    fn logout_does_not_match_an_unrelated_or_prefixed_key() {
        let keys = vec!["SPICE_SPICEAI_TOKEN".to_string()];
        for line in [
            "DATABASE_URL=postgres://localhost/db",
            "# SPICE_SPICEAI_TOKEN=commented-out",
            "SPICE_SPICEAI_TOKEN_SPICEHQ=other-org",
            "MY_SPICE_SPICEAI_TOKEN=different",
            "no equals sign here",
        ] {
            assert!(!assigns_any(line, &keys), "should not match: {line:?}");
        }
    }

    #[test]
    fn short_commit_does_not_split_a_multi_byte_character() {
        // `commit_sha` is server-supplied. Byte-slicing it aborted the whole
        // command when a multi-byte character straddled byte 7.
        assert_eq!(short_commit(Some("abcdéfghij")), "abcdéfg");
        assert_eq!(
            short_commit(Some("日本語のコミット")),
            "日本語のコミット".chars().take(7).collect::<String>()
        );
    }

    #[test]
    fn table_cells_stay_on_one_line_and_within_width() {
        let value = "connection refused\nwhile loading dataset taxi_trips from postgres";
        let cell = truncate_for_table(value, 30);
        assert!(!cell.contains('\n'), "a table cell must not wrap: {cell}");
        assert_eq!(cell.chars().count(), 30);
        assert!(cell.ends_with('…'));
    }

    #[test]
    fn table_cells_shorter_than_the_limit_are_untouched() {
        assert_eq!(truncate_for_table("ok", 30), "ok");
    }

    #[test]
    fn json_output_populates_org_after_backfill() {
        // Regression for #11041: the `/v1/apps` payload omits `org`, so
        // `--output json` serialized `"org": ""`. After the backfill the
        // serialized JSON must carry the context org, matching the table's
        // `<org>/<name>` display.
        let mut projects = vec![test_project("", "ltd-mint")];
        backfill_project_orgs(&mut projects, "Jeadie");
        let value = serde_json::to_value(&projects[0]).expect("project should serialize to JSON");
        assert_eq!(
            value.get("org").and_then(serde_json::Value::as_str),
            Some("Jeadie"),
            "JSON output must populate `org` to match the table output"
        );
        assert_eq!(
            value.get("name").and_then(serde_json::Value::as_str),
            Some("ltd-mint")
        );
    }
}
