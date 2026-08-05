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
pub mod org;

use crate::context::RuntimeContext;
use crate::error::{CloudErrorCode, Error, InvalidArgumentSnafu, Result};
use crate::output::{OutputFormat, TableOutput, write_json};
use clap::{Args, Subcommand};
use dialoguer::{Input, Password, Select, theme::ColorfulTheme};
use snafu::ResultExt;
use std::{fmt, io::IsTerminal};

pub use client::{AppTarget, CloudClient, is_device_authorization_denied_error, parse_org_app};
pub use config::{CloudLink, get_linked_app, load_cloud_link, remove_cloud_link, save_cloud_link};
use spice_cloud_client::{
    endpoints::{data_region_name, normalize_data_region},
    types::{AppKind, Deployment, IngestionMetrics, PodMetrics, UpdateChannel},
};

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

ORGANIZATIONS
Commands act on one organization at a time. Name it inline as `<org>/<app>`,
select it for the invocation with `--org`, or set it for good with
`spice cloud org use <org>`. `spice cloud whoami` shows which one is in effect.

EXAMPLES
  spice cloud whoami                        # Identity and active organization
  spice cloud orgs                          # Organizations you can act on
  spice cloud org use spicehq               # Make spicehq the active org
  spice cloud apps --org spicehq            # List apps in one org
  spice cloud deploy --app spicehq/team-app --wait
  spice cloud deployments --app spicehq/team-app
  spice cloud logs --app spicehq/team-app --level error
  spice cloud instance status --app spicehq/team-app   # Component readiness
  spice cloud link spicehq/team-app         # Default this directory to an app

Docs: https://spiceai.org/docs/spice-cloud"#
)]
pub struct CloudArgs {
    /// Organization to act on for this invocation (overrides `SPICE_CLOUD_ORG`
    /// and the active org).
    ///
    /// Deliberately not bound to the env var via Clap: the CLI reads
    /// `SPICE_CLOUD_ORG` itself so it can tell the user which source chose the
    /// org, and so a flag and the env var can be ranked rather than merged.
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

    // `about`/`long_about` live on `DeployArgs`; a doc comment here would
    // shadow the long help that documents where the spicepod comes from.
    Deploy(DeployArgs),

    /// Inspect current deployment status
    Inspect(InspectArgs),

    /// Show API keys for an app
    #[command(name = "api-keys")]
    ApiKeys(ApiKeysArgs),

    /// Show metrics for an app's pods
    Metrics(MetricsArgs),

    /// Inspect a running app instance
    #[command(subcommand)]
    Instance(InstanceCommands),
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
pub enum InstanceCommands {
    /// Show component readiness for a running app instance
    Status(InstanceStatusArgs),

    /// Show dataset load state for a running app instance
    Datasets(InstanceDatasetsArgs),
}

#[derive(Args, Debug)]
pub struct InstanceStatusArgs {
    /// App name in org/app format (uses linked app if not specified)
    #[arg(long)]
    pub app: Option<String>,

    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

#[derive(Args, Debug)]
pub struct InstanceDatasetsArgs {
    /// App name in org/app format (uses linked app if not specified)
    #[arg(long)]
    pub app: Option<String>,

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

/// Where a freshly minted credential is filed, and which org it claims to be for.
///
/// Spice Cloud fixes a token's organization when it is minted, so `--org` states
/// which org the caller believes the credential serves. The CLI verifies that
/// claim against the server before storing it under that org, so a mismatch
/// fails at login rather than silently acting on the wrong org later.
struct LoginTarget<'a> {
    requested_org: Option<&'a str>,
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

/// Minimum severity of log entries to show.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
#[clap(rename_all = "snake_case")]
pub enum LogLevelFilter {
    /// Every entry the deployment emitted (default).
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
    /// App name in org/app format (uses linked app if not specified)
    #[arg(long)]
    pub app: Option<String>,

    /// Deployment ID (uses latest if not specified)
    #[arg(long)]
    pub deployment: Option<i64>,

    /// Maximum number of log entries to show
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
    about = "Deploy the app",
    long_about = r#"Deploy an app on Spice Cloud.

Spice Cloud pulls the spicepod from the app's connected git repository — by
default the app's production branch. A local spicepod is NOT uploaded; use
`spice cloud update app --spicepod <path>` to change the stored spicepod first,
or `--branch` / `--commit` to deploy a different revision.

EXAMPLES
  spice cloud deploy --app spicehq/team-app
  spice cloud deploy --app spicehq/team-app --wait --timeout 15m
  spice cloud deploy --app spicehq/team-app --branch release --replicas 2"#
)]
pub struct DeployArgs {
    /// App name in org/app format (uses linked app if not specified)
    #[arg(long)]
    pub app: Option<String>,

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
    /// App name in org/app format (uses linked app if not specified)
    #[arg(long)]
    pub app: Option<String>,

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
    #[arg(long, value_parser = parse_create_app_region)]
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
        CloudCommands::Link(link_args) => execute_link(link_args, org).await,
        CloudCommands::Unlink => execute_unlink(),
        CloudCommands::Apps(apps_args) => execute_apps(apps_args, org).await,
        CloudCommands::Deployments(deploy_args) => execute_deployments(deploy_args, org).await,
        CloudCommands::Regions(regions_args) => execute_regions(regions_args, org).await,
        CloudCommands::Images(images_args) => execute_images(images_args, org).await,
        CloudCommands::Secrets(secrets_cmd) => execute_secrets(secrets_cmd, org).await,
        CloudCommands::Logs(logs_args) => execute_logs(logs_args, org).await,
        CloudCommands::Create(create_cmd) => execute_create(create_cmd, org).await,
        CloudCommands::Get(get_cmd) => execute_get(get_cmd, org).await,
        CloudCommands::Update(update_cmd) => execute_update(update_cmd, org).await,
        CloudCommands::Delete(delete_cmd) => execute_delete(delete_cmd, org).await,
        CloudCommands::Deploy(deploy_args) => execute_deploy(deploy_args, org).await,
        CloudCommands::Inspect(inspect_args) => execute_inspect(inspect_args, org).await,
        CloudCommands::ApiKeys(api_keys_args) => execute_api_keys(api_keys_args, org).await,
        CloudCommands::Metrics(metrics_args) => execute_metrics(metrics_args, org).await,
        CloudCommands::Instance(instance_cmd) => execute_instance(ctx, instance_cmd, org).await,
    }
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
    AppArgument,
    /// The `--org` flag.
    Flag,
    /// The `SPICE_CLOUD_ORG` environment variable.
    Environment,
    /// `.spice/cloud.json`, written by `spice cloud link`.
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
            Self::AppArgument => "app argument",
            Self::Flag => "--org flag",
            Self::Environment => org::ACTIVE_ORG_VAR,
            Self::LinkedApp => "linked app",
            Self::ActiveOrg => "active org",
            Self::Credential => "credential",
        }
    }
}

/// The organization in effect, and where it came from, ignoring any `org/app`
/// argument (which [`resolve_app_target`] layers on top).
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
fn resolve_app_target_with_source(
    app_flag: Option<&str>,
    flag_org: Option<&str>,
) -> Result<(AppTarget, OrgSource)> {
    let (context_org, context_source) = resolve_org_with_source(flag_org)?;

    // An `<org>/<app>` argument names the app completely and outranks everything.
    if let Some(app_flag) = app_flag {
        let (path_org, app) = parse_org_app(app_flag);
        if app.is_empty() {
            return Err(Error::cloud_with_hint(
                CloudErrorCode::InvalidRequest,
                format!("Invalid app name '{app_flag}': expected <app> or <org>/<app>."),
                "Run 'spice cloud apps' to list the apps you can reach.",
            ));
        }

        let Some(path_org) = path_org else {
            // A bare app name inherits whatever org is in effect.
            return Ok((AppTarget::new(context_org, app), context_source));
        };

        org::validate_org_name(&path_org)?;
        ensure_orgs_agree(
            &path_org,
            "the app argument",
            context_org.as_deref(),
            context_source,
        )?;
        return Ok((AppTarget::new(Some(path_org), app), OrgSource::AppArgument));
    }

    // No app named: fall back to the directory's linked app.
    let Some(link) = load_cloud_link()? else {
        return Err(Error::cloud_with_hint(
            CloudErrorCode::InvalidRequest,
            "No app specified.",
            "Pass --app <org>/<app>, or run 'spice cloud link <org>/<app>' to set a default for this directory.",
        ));
    };

    let Some(link_org) = (!link.org.is_empty()).then(|| link.org.clone()) else {
        return Ok((AppTarget::new(context_org, link.app), context_source));
    };

    org::validate_org_name(&link_org)?;

    // The link is project-level config, so it loses to a flag or the
    // environment — but only after saying so, never silently.
    match context_source {
        OrgSource::Flag | OrgSource::Environment => {
            ensure_orgs_agree(
                &link_org,
                "the linked app",
                context_org.as_deref(),
                context_source,
            )?;
            Ok((
                AppTarget::new(Some(link_org), link.app),
                OrgSource::LinkedApp,
            ))
        }
        _ => Ok((
            AppTarget::new(Some(link_org), link.app),
            OrgSource::LinkedApp,
        )),
    }
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
            "Name one organization: pass --app {stated}/<app>, or drop --org and let {stated_source} decide."
        ),
    ))
}

/// Resolve which app a command acts on.
fn resolve_app_target(app_flag: Option<&str>, flag_org: Option<&str>) -> Result<AppTarget> {
    Ok(resolve_app_target_with_source(app_flag, flag_org)?.0)
}

/// Build a client for the org a command acts on.
fn connect(flag_org: Option<&str>) -> Result<CloudClient> {
    CloudClient::connect(resolve_org(flag_org)?.as_deref())
}

/// Build a client for the org that owns `target`.
fn connect_for_target(target: &AppTarget) -> Result<CloudClient> {
    CloudClient::connect(target.org.as_deref())
}

/// Print the fully-qualified target and where its org came from, before a
/// command changes anything.
///
/// A wrong-organization deploy or delete cannot be undone by reading the
/// scrollback afterwards, and a persisted org is invisible at the call site.
/// Suppressed in machine mode, where the same facts belong in the JSON result.
fn announce_target(action: &str, target: &AppTarget, source: OrgSource, output: OutputFormat) {
    if output == OutputFormat::Json {
        return;
    }

    println!("{action} {target}");
    if target.org.is_some() && source != OrgSource::AppArgument {
        println!("  organization from {}", source.label());
    }
}

// ============================================================================
// Command implementations
// ============================================================================

async fn execute_login(args: &LoginArgs, org: Option<&str>) -> Result<()> {
    let target = LoginTarget { requested_org: org };
    match &args.method {
        Some(LoginMethod::Subscription(args)) => {
            execute_login_device_flow(!args.device, &target).await
        }
        Some(LoginMethod::Pat(args)) => execute_login_pat(args, &target).await,
        Some(LoginMethod::Api(args)) => execute_login_api(args, &target).await,
        None => execute_login_with_chooser(&target).await,
    }
}

async fn execute_login_with_chooser(target: &LoginTarget<'_>) -> Result<()> {
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
        0 => execute_login_device_flow(true, target).await,
        1 => execute_login_device_flow(false, target).await,
        2 => execute_login_pat(&PatLoginArgs { token: None }, target).await,
        3 => {
            execute_login_api(
                &ApiLoginArgs {
                    client_id: None,
                    client_secret: None,
                },
                target,
            )
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

async fn execute_login_pat(args: &PatLoginArgs, target: &LoginTarget<'_>) -> Result<()> {
    let token = resolve_string_or_prompt(
        args.token.as_deref(),
        "PAT",
        "--token",
        "SPICE_CLOUD_PAT",
        "Spice Cloud personal access token",
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

/// Store a credential under the org it actually serves.
///
/// With no `--org`, the credential becomes the default one, preserving today's
/// single-org behavior. With `--org`, the credential is filed under a per-org
/// variable so it cannot displace the personal-org credential.
fn store_credential(token: &str, org: Option<&str>) -> Result<()> {
    use crate::commands::login::merge_auth_config;

    merge_auth_config("SPICEAI", &[(&credential_key(org), token)])
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

/// Check that a freshly minted credential really serves the requested org.
///
/// Spice Cloud binds a token to one org at mint time, so `--org` is a claim to
/// verify, not a setting to apply. Returns the org the credential should be
/// filed under, which is `None` when the caller did not name one.
async fn verify_login_org(
    client: &CloudClient,
    requested_org: Option<&str>,
    token_org: Option<&str>,
) -> Result<Option<String>> {
    let Some(requested) = requested_org else {
        return Ok(None);
    };

    if let Some(token_org) = token_org
        && token_org.eq_ignore_ascii_case(requested)
    {
        return Ok(Some(requested.to_string()));
    }

    // The identity endpoint reports the token's own org. A different org means
    // this credential cannot act on the requested one, however the CLI files it.
    if let Some(token_org) = token_org {
        return Err(Error::cloud_with_hint(
            CloudErrorCode::WrongOrg,
            format!("This credential is issued for organization '{token_org}', not '{requested}'."),
            format!(
                "Mint a credential for '{requested}' in the Spice Cloud portal (a personal access token or an OAuth client owned by that organization), then re-run with --org {requested}."
            ),
        ));
    }

    // A service-account credential has no user identity, so probe the org
    // directly; the server rejects a credential that cannot act on it.
    client.get_auth_context_for_org(requested).await?;
    Ok(Some(requested.to_string()))
}

async fn save_token_and_print_login_result(token: &str, target: &LoginTarget<'_>) -> Result<()> {
    use crate::commands::login::merge_auth_config;

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

    store_credential(token, store_org.as_deref())?;

    match auth_context_result {
        Ok(context) => {
            if let Some(api_key) = context.app_api_key {
                // File the data-plane key beside the management token for the
                // same org. Writing it to the shared default would let a second
                // org's login replace the first org's key, and leave it behind
                // when that org logs out.
                merge_auth_config(
                    "SPICEAI",
                    &[(&api_key_credential_key(store_org.as_deref()), &api_key)],
                )?;
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
    use crate::commands::login::merge_auth_config;

    let authed_client = CloudClient::with_token_for_org(token, target.requested_org)?;
    let store_org = verify_login_org(&authed_client, target.requested_org, None).await?;

    store_credential(token, store_org.as_deref())?;
    merge_auth_config(
        "CLOUD",
        &[("CLIENT_ID", client_id), ("CLIENT_SECRET", client_secret)],
    )?;

    println!();
    println!("\x1b[32m✓ Successfully logged in to Spice Cloud with API credentials\x1b[0m");
    println!("  Client ID: {client_id}");
    println!();
    println!("Credentials saved to env.");

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
        // Fire-and-forget: open in spawn_blocking so Command::status does not
        // block a Tokio worker or delay the device-flow poll loop.
        let auth_url_for_open = auth_url.clone();
        tokio::task::spawn_blocking(move || {
            let _ = system_open::that(auth_url_for_open);
        });
    }

    println!("Waiting for authentication...");

    // Poll for auth status
    let timeout = std::time::Duration::from_mins(5); // 5 minutes
    let start = std::time::Instant::now();

    loop {
        if start.elapsed() > timeout {
            return InvalidArgumentSnafu {
                message: "Authentication timed out. Please try again.",
            }
            .fail();
        }

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let response = match client.exchange_code(&auth_code).await {
            Ok(response) => response,
            Err(error) if is_device_authorization_denied_error(&error) => return Err(error),
            Err(error) => {
                tracing::debug!("Failed to poll device login status; retrying: {error}");
                continue;
            }
        };

        if let Some(response) = response {
            if response.access_denied {
                return InvalidArgumentSnafu {
                    message: "Access denied",
                }
                .fail();
            }

            if let Some(token) = response.access_token {
                return save_token_and_print_login_result(&token, target).await;
            }
        }
    }
}

fn print_post_login_help() {
    println!();
    println!("You can now use 'spice cloud' commands to manage your apps and deployments.");
    println!();
    println!("Quick start:");
    println!("  spice cloud orgs                   - List your organizations");
    println!("  spice cloud apps                   - List your apps");
    println!("  spice cloud create app <name>      - Create a new app");
    println!("  spice cloud deploy --app <org/app> - Deploy your app");
    println!();
}

fn execute_logout(args: &LogoutArgs, flag_org: Option<&str>) -> Result<()> {
    let mut cleared = Vec::new();

    match args.scope {
        LogoutScope::All => {
            for org in org::orgs_with_stored_tokens() {
                if remove_env_keys(&[org::org_token_var(&org), org::org_api_key_var(&org)])? {
                    cleared.push(org);
                }
            }
            remove_env_keys(&default_credential_keys())?;
            org::clear_active_org()?;
        }
        LogoutScope::Active => {
            match resolve_org(flag_org)? {
                // An org with its own credential loses only that credential;
                // the personal-org session in the same directory survives.
                Some(org) if org::has_org_token(&org) => {
                    if remove_env_keys(&[org::org_token_var(&org), org::org_api_key_var(&org)])? {
                        cleared.push(org.clone());
                    }
                    if resolve_org(None)?.is_some_and(|active| active.eq_ignore_ascii_case(&org)) {
                        org::clear_active_org()?;
                    }
                }
                _ => {
                    if remove_env_keys(&default_credential_keys())? {
                        cleared.push("default".to_string());
                    }
                    org::clear_active_org()?;
                }
            }
        }
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
        (LogoutScope::Active, None) => {
            println!("\x1b[32m✓ Already logged out\x1b[0m");
        }
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

/// Drop `keys` from the working directory's env file, returning whether any were
/// present. Removes the file once nothing but comments and blank lines remain.
fn remove_env_keys(keys: &[String]) -> Result<bool> {
    let env_file = if std::path::Path::new(".env.local").exists() {
        ".env.local"
    } else {
        ".env"
    };

    let path = std::path::Path::new(env_file);
    if !path.exists() {
        return Ok(false);
    }

    let content = std::fs::read_to_string(path).unwrap_or_default();
    let prefixes: Vec<String> = keys.iter().map(|key| format!("{key}=")).collect();
    let mut removed = false;
    let lines: Vec<&str> = content
        .lines()
        .filter(|line| {
            // Trim before matching, mirroring the reader. Matching untrimmed
            // would leave an indented credential readable but unremovable, so
            // logout would silently leave it behind.
            let matched = prefixes
                .iter()
                .any(|prefix| line.trim_start().starts_with(prefix.as_str()));
            removed |= matched;
            !matched
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

    Ok(removed)
}

async fn execute_whoami(args: &WhoamiArgs, flag_org: Option<&str>) -> Result<()> {
    let effective_org = resolve_org(flag_org)?;
    let client = CloudClient::connect(effective_org.as_deref())?;

    let context = match client.get_auth_context().await {
        Ok(ctx) => ctx,
        Err(err) if is_cloud_unauthorized_error(&err) => {
            // The auth-context endpoint requires a user token (subscription
            // or PAT). Service-account tokens (OAuth client credentials) are
            // valid for API calls but do not have a user identity.
            if client.list_apps().await.is_ok() {
                return Err(Error::cloud_with_hint(
                    CloudErrorCode::Forbidden,
                    "User identity is not available for this authentication method. The current credential is a valid service-account token and can be used for API calls, but has no user identity.",
                    "Run 'spice cloud login subscription' or 'spice cloud login pat' to obtain a user token.",
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
    let org_source = org_source_label(flag_org, effective_org.as_deref());
    let available_orgs = client.list_orgs().await.unwrap_or_default();

    if args.output == OutputFormat::Json {
        return write_json(&serde_json::json!({
            "username": context.username,
            "email": context.email,
            "org_name": context.org_name,
            "active_org": active_org,
            "active_org_source": org_source,
            "app_name": context.app_name,
            "available_orgs": available_orgs
                .as_ref()
                .map(|orgs| orgs.iter().map(|org| org.name.clone()).collect::<Vec<_>>()),
        }));
    }

    println!("Logged in as: {} ({})", context.username, context.email);
    println!("Active org:   {active_org} (from {org_source})");
    if !context.org_name.is_empty() && !context.org_name.eq_ignore_ascii_case(&active_org) {
        println!("Credential org: {}", context.org_name);
    }
    if let Some(app_name) = context.app_name {
        println!("Default App:  {active_org}/{app_name}");
    }
    match available_orgs {
        Some(orgs) if orgs.len() > 1 => {
            println!("Organizations: {} (run 'spice cloud orgs')", orgs.len());
        }
        Some(_) | None => {}
    }

    Ok(())
}

/// Where the org in effect came from, so `whoami` can answer "why this org?".
///
/// Clap fills `--org` from `SPICE_CLOUD_ORG` too, so the two are distinguished
/// by checking whether the env var supplied the value.
fn org_source_label(flag_org: Option<&str>, effective_org: Option<&str>) -> &'static str {
    match (effective_org, flag_org) {
        (None, _) => "credential",
        (Some(_), Some(flag)) => {
            if std::env::var(org::ACTIVE_ORG_VAR).is_ok_and(|env| env == flag) {
                org::ACTIVE_ORG_VAR
            } else {
                "--org"
            }
        }
        (Some(_), None) => "active org",
    }
}

async fn execute_orgs(args: &OrgsArgs, flag_org: Option<&str>) -> Result<()> {
    let active = resolve_org(flag_org)?;
    let client = CloudClient::connect(active.as_deref())?;

    let listed = client.list_orgs().await?;
    let context_org = client
        .optional_user_auth_context()
        .await
        .ok()
        .flatten()
        .map(|context| context.org_name)
        .filter(|org| !org.is_empty());

    let stored = org::orgs_with_stored_tokens();
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
            let verified = match CloudClient::connect(Some(&args.org)) {
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
                    "  No credential is stored for this organization yet — run 'spice cloud login pat --org {}'.",
                    args.org
                );
            }
            // A directory linked to another org's app will not follow this
            // switch; say so now rather than failing confusingly later.
            if let Ok(Some(link)) = load_cloud_link()
                && !link.org.is_empty()
                && !link.org.eq_ignore_ascii_case(&args.org)
            {
                println!(
                    "  Note: this directory is linked to {}, which takes precedence here. Re-link with 'spice cloud link {}/<app>' to follow the switch.",
                    link.full_name(),
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

async fn execute_link(args: &LinkArgs, flag_org: Option<&str>) -> Result<()> {
    let target = resolve_app_target(Some(&args.app), flag_org)?;
    let client = connect_for_target(&target)?;

    // Verify the app exists
    let app = client.get_app(&target).await?;

    // The API does not return `org` on app payloads, so fall back to the org the
    // command resolved, which is what later commands will use for this link.
    let org = if app.org.is_empty() {
        target.org.clone().unwrap_or_default()
    } else {
        app.org
    };

    // Save the link
    let link = CloudLink {
        org,
        app: app.name,
        app_id: Some(app.id),
        region: app.region,
        linked_at: Some(chrono::Utc::now().to_rfc3339()),
    };
    save_cloud_link(&link)?;
    // The link names one org's app, so committing it would retarget a teammate
    // working in another org. Keep it out of version control by default.
    if let Err(err) = ignore_cloud_link_dir() {
        tracing::debug!("Could not add .spice to .gitignore: {err}");
    }

    println!("\x1b[32m✓ Linked to app {}\x1b[0m", link.full_name());
    println!();
    println!("You can now use commands without specifying --app:");
    println!("  spice cloud deploy");
    println!("  spice cloud logs");
    println!("  spice cloud secrets list");

    Ok(())
}

/// Add `.spice` to the working directory's `.gitignore`, if it is a git
/// repository and the entry is not already present.
///
/// Best-effort: never fails a `link` that otherwise succeeded.
fn ignore_cloud_link_dir() -> Result<()> {
    if !std::path::Path::new(".git").exists() {
        return Ok(());
    }

    let path = std::path::Path::new(".gitignore");
    let existing = std::fs::read_to_string(path).unwrap_or_default();
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

    std::fs::write(path, updated).map_err(|e| crate::error::Error::ConfigIo {
        operation: "write",
        path: path.to_path_buf(),
        source: e,
    })
}

fn execute_unlink() -> Result<()> {
    remove_cloud_link()?;
    println!("\x1b[32m✓ Unlinked from Spice Cloud app\x1b[0m");
    Ok(())
}

async fn execute_apps(args: &AppsArgs, flag_org: Option<&str>) -> Result<()> {
    let active_org = resolve_org(flag_org)?;
    let client = CloudClient::connect(active_org.as_deref())?;
    let context = client.optional_user_auth_context().await?;
    let mut apps = client.list_apps().await?;

    if apps.is_empty() {
        match &active_org {
            Some(org) => println!(
                "No apps found in organization {org}. Create one with: spice cloud create app <name> --org {org}"
            ),
            None => println!("No apps found. Create one with: spice cloud create app <name>"),
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
    // The Spice Cloud `/v1/apps` endpoint does not populate `org` per app, so
    // backfill it from the auth-context org — the same fallback the table
    // rendering applies via `display_app_name`. Without this, `--output json`
    // emitted `"org": ""` while the table showed `<org>/<name>`, breaking
    // format parity and machine-readable scripting (see #11041).
    backfill_app_orgs(&mut apps, context_org);

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
        let display_name = display_app_name(app, context_org);
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

fn is_cloud_unauthorized_error(err: &crate::error::Error) -> bool {
    client::is_unauthorized_auth_context_error(err)
}

/// Backfill each app's empty `org` from the auth-context org so machine-readable
/// (`--output json`) output matches the human-readable table, which already
/// applies this fallback when rendering via [`display_app_name`]. The Spice
/// Cloud `/v1/apps` endpoint does not populate `org` on each app, so the auth
/// context is the only source of truth for the user's org. A no-op when
/// `context_org` is empty (nothing to fall back to) or the app already carries
/// an org.
fn backfill_app_orgs(apps: &mut [spice_cloud_client::types::App], context_org: &str) {
    if context_org.is_empty() {
        return;
    }
    for app in apps.iter_mut() {
        if app.org.is_empty() {
            app.org = context_org.to_string();
        }
    }
}

/// Format an app's display name as `org/name`, falling back to the auth
/// context org when the app payload does not include one.
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

async fn execute_deployments(args: &DeploymentsArgs, flag_org: Option<&str>) -> Result<()> {
    let target = resolve_app_target(args.app.as_deref(), flag_org)?;
    let client = connect_for_target(&target)?;

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
        Some(sha) if sha.len() > 7 => sha[..7].to_string(),
        Some(sha) if !sha.is_empty() => sha.to_string(),
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
    let client = connect(flag_org)?;
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
    let client = connect(flag_org)?;
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
            let target = resolve_app_target(args.app.as_deref(), flag_org)?;
            let client = connect_for_target(&target)?;
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
                resolve_app_target_with_source(args.app.as_deref(), flag_org)?;
            announce_target("Setting secret on", &target, org_source, args.output);
            let client = connect_for_target(&target)?;
            client.set_secret(&target, &args.name, &args.value).await?;
            if args.output == OutputFormat::Json {
                return write_json(&serde_json::json!({"name": args.name, "status": "set"}));
            }
            println!("\x1b[32m✓ Secret '{}' set successfully\x1b[0m", args.name);
        }
        SecretsCommands::Get(args) => {
            let target = resolve_app_target(args.app.as_deref(), flag_org)?;
            let client = connect_for_target(&target)?;
            let secret = client.get_secret(&target, &args.name).await?;
            if args.output == OutputFormat::Json {
                return write_json(&secret);
            }
            println!("{}", secret.value.unwrap_or_default());
        }
        SecretsCommands::Delete(args) => {
            let target = resolve_app_target(args.app.as_deref(), flag_org)?;
            let client = connect_for_target(&target)?;
            client.delete_secret(&target, &args.name).await?;
            if args.output == OutputFormat::Json {
                return write_json(&serde_json::json!({"name": args.name, "status": "deleted"}));
            }
            println!("\x1b[32m✓ Secret '{}' deleted\x1b[0m", args.name);
        }
    }
    Ok(())
}

async fn execute_logs(args: &LogsArgs, flag_org: Option<&str>) -> Result<()> {
    let target = resolve_app_target(args.app.as_deref(), flag_org)?;
    let client = connect_for_target(&target)?;

    let deployment_id = if let Some(id) = args.deployment {
        id
    } else {
        let latest = client.get_latest_deployment(&target).await?;
        latest.id
    };

    let mut logs = client
        .get_deployment_logs(&target, deployment_id, args.limit, args.since.as_deref())
        .await?;

    logs.logs
        .retain(|entry| args.level.admits(entry.level.as_deref()));

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

    if args.follow {
        println!();
        println!(
            "Note: --follow is not yet supported by the Spice Cloud logs API; re-run the command to fetch newer entries."
        );
    }

    Ok(())
}

async fn execute_create(cmd: &CreateCommands, flag_org: Option<&str>) -> Result<()> {
    match cmd {
        CreateCommands::App(args) => {
            let create_region = validate_create_app_args(args)?;

            let client = connect(flag_org)?;
            let spicepod_content = if let Some(path) = args.spicepod.as_deref() {
                Some(read_spicepod_file(path).await?)
            } else {
                None
            };

            let app = client
                .create_app(
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
            let created = AppTarget::new(
                if app.org.is_empty() {
                    resolve_org(flag_org)?
                } else {
                    Some(app.org.clone())
                },
                app.name.clone(),
            );

            let app = if spicepod_content.is_some() || args.channel.is_some() {
                match client
                    .update_app(
                        &created,
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
                        let update_error = error.to_string();
                        let cleanup_result = client.delete_app(&created).await;
                        let cleanup_message = match cleanup_result {
                            Ok(()) => {
                                "The app was deleted to roll back the failed create.".to_string()
                            }
                            Err(cleanup_error) => format!(
                                "The app still exists, and an automatic delete attempt failed: {cleanup_error}. Run 'spice cloud api-keys --app {created}' if you need to inspect its provisioned API keys, or delete the app manually."
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
        }
        CreateCommands::Deployment(args) => {
            let target = resolve_app_target(args.app.as_deref(), flag_org)?;
            let client = connect_for_target(&target)?;
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
        }
    }
    Ok(())
}

fn validate_create_app_args(args: &CreateAppArgs) -> Result<String> {
    let region = normalize_create_app_region(&args.region)?;

    if args.kind == AppKind::Cluster {
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

async fn execute_get(cmd: &GetCommands, flag_org: Option<&str>) -> Result<()> {
    match cmd {
        GetCommands::App(args) => {
            let target = resolve_app_target(Some(&args.app), flag_org)?;
            let client = connect_for_target(&target)?;
            let app = client.get_app(&target).await?;

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

async fn execute_update(cmd: &UpdateCommands, flag_org: Option<&str>) -> Result<()> {
    match cmd {
        UpdateCommands::App(args) => {
            let target = resolve_app_target(args.app.as_deref(), flag_org)?;
            let client = connect_for_target(&target)?;
            let spicepod_content = if let Some(path) = args.spicepod.as_deref() {
                Some(read_spicepod_file(path).await?)
            } else {
                None
            };

            let app = client
                .update_app(
                    &target,
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

async fn execute_delete(cmd: &DeleteCommands, flag_org: Option<&str>) -> Result<()> {
    use std::io::Write;

    match cmd {
        DeleteCommands::App(args) => {
            let (target, org_source) = resolve_app_target_with_source(Some(&args.app), flag_org)?;

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

            let client = connect_for_target(&target)?;
            client.delete_app(&target).await?;
            if args.output == OutputFormat::Json {
                return write_json(
                    &serde_json::json!({"app": target.display(), "status": "deleted"}),
                );
            }
            println!("\x1b[32m✓ Deleted app {target}\x1b[0m");
        }
    }
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
    let (target, org_source) = resolve_app_target_with_source(args.app.as_deref(), flag_org)?;
    announce_target("Deploying to", &target, org_source, args.output);
    let client = connect_for_target(&target)?;

    let deployment = client
        .create_deployment(
            &target,
            client::CreateDeploymentParams {
                image_tag: args.image.as_deref(),
                branch: args.branch.as_deref(),
                commit_sha: args.commit.as_deref(),
                replicas: args.replicas,
                debug: args.debug,
            },
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
            "  Track it with 'spice cloud deployments --app {target}', or re-run with --wait."
        );
        return Ok(());
    }

    let final_deployment =
        wait_for_deployment(&client, &target, deployment, &args.timeout, is_json).await?;

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
    target: &AppTarget,
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
                format!(
                    "Inspect it with 'spice cloud logs --app {target} --deployment {} --level error'.",
                    deployment.id
                ),
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
                    "Wait longer with --timeout, or check 'spice cloud deployments --app {target}'."
                ),
            ));
        }

        tokio::time::sleep(interval).await;
        interval = (interval * 2).min(max_interval);

        let deployments = client.list_deployments(target, 20, None).await?;
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

async fn execute_inspect(args: &InspectArgs, flag_org: Option<&str>) -> Result<()> {
    let target = resolve_app_target(args.app.as_deref(), flag_org)?;
    let client = connect_for_target(&target)?;

    let app = client.get_app(&target).await?;
    let deployments = client.list_deployments(&target, 1, None).await?;
    // Metrics report one row per pod, which is the closest the management API
    // gets to "how many replicas are actually up".
    let pods = client
        .get_app_metrics(app.id, None)
        .await
        .map(|metrics| metrics.metrics.keys().cloned().collect::<Vec<_>>())
        .unwrap_or_default();

    if args.output == OutputFormat::Json {
        return write_json(&serde_json::json!({
            "app": app,
            "org": target.org,
            "latest_deployment": deployments.first(),
            "pods": pods,
        }));
    }

    println!("App:    {target}");
    if let Some(region) = app.region {
        println!("Region: {region}");
    }
    if let Some(branch) = app.production_branch {
        println!("Branch: {branch}");
    }

    if let Some(deployment) = deployments.first() {
        println!();
        println!("Latest Deployment:");
        println!("  ID:       {}", deployment.id);
        println!("  Status:   {}", deployment.status);
        if let Some(image) = &deployment.image_tag {
            println!("  Image:    {image}");
        }
        if let Some(replicas) = deployment.replicas {
            println!("  Replicas: {replicas}");
        }
        if let Some(commit) = &deployment.commit_sha {
            println!("  Commit:   {}", short_commit(Some(commit)));
        }
        if let Some(created) = &deployment.created_at {
            println!("  Created:  {created}");
        }
        if let Some(error) = &deployment.error_message {
            println!("  Error:    {error}");
        }
    } else {
        println!("\nNo deployments found.");
    }

    println!();
    if pods.is_empty() {
        println!("Pods: none reporting metrics");
    } else {
        println!("Pods ({}):", pods.len());
        for pod in &pods {
            println!("  {pod}");
        }
        println!("  Component readiness: spice cloud instance status --app {target}");
    }

    Ok(())
}

async fn execute_api_keys(args: &ApiKeysArgs, flag_org: Option<&str>) -> Result<()> {
    let target = resolve_app_target(args.app.as_deref(), flag_org)?;
    let client = connect_for_target(&target)?;

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
    let target = resolve_app_target(args.app.as_deref(), flag_org)?;
    let client = connect_for_target(&target)?;
    let app = client.get_app(&target).await?;

    let response = client
        .get_app_metrics(app.id, args.window.as_deref())
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

fn normalize_create_app_region(region: &str) -> Result<String> {
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

fn parse_create_app_region(region: &str) -> std::result::Result<String, String> {
    normalize_create_app_region(region).map_err(|error| match error {
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

/// A component reported by an app instance's `/v1/status` endpoint.
///
/// This endpoint reports connection endpoints only — `http`, `flight`,
/// `metrics`, `opentelemetry`. Dataset state comes from `/v1/datasets`.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct InstanceComponentStatus {
    name: String,
    status: String,
    #[serde(default)]
    endpoint: Option<String>,
}

/// A dataset reported by an app instance's `/v1/datasets?status=true` endpoint.
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

async fn execute_instance(
    ctx: &RuntimeContext,
    cmd: &InstanceCommands,
    flag_org: Option<&str>,
) -> Result<()> {
    let (app_flag, output) = match cmd {
        InstanceCommands::Status(args) => (args.app.as_deref(), args.output),
        InstanceCommands::Datasets(args) => (args.app.as_deref(), args.output),
    };

    let target = resolve_app_target(app_flag, flag_org)?;
    let client = connect_for_target(&target)?;
    let app = client.get_app(&target).await?;

    // The management API does not expose runtime state, so ask the app's own
    // runtime, authenticating with the app API key the management API holds.
    let region = app.region.clone().ok_or_else(|| {
        Error::cloud_with_hint(
            CloudErrorCode::NotFound,
            format!("App {target} does not report a region, so its instance endpoint is unknown."),
            format!("Check the app with 'spice cloud inspect --app {target}'."),
        )
    })?;
    // Prefer the key the management API reports for this app; fall back to a
    // key stored for the same org only if the API withholds one. Never reach
    // for another org's key.
    let api_key = match client.get_api_keys(&target).await?.api_key {
        Some(api_key) => Some(api_key),
        None => org::api_key_for_org(target.org.as_deref()),
    };
    let api_key = api_key.ok_or_else(|| {
        Error::cloud_with_hint(
            CloudErrorCode::Forbidden,
            format!("No API key is available for app {target}, so its instance cannot be queried."),
            format!("Generate one with 'spice cloud api-keys --app {target} --regenerate 1'."),
        )
    })?;

    let region =
        spice_cloud_client::endpoints::normalize_data_region(&region).ok_or_else(|| {
            Error::cloud(
                CloudErrorCode::InvalidRequest,
                format!("App {target} reports an unrecognized region '{region}'."),
            )
        })?;

    let runtime_ctx = RuntimeContext::with_args(
        None,
        Some(api_key),
        Some(&region),
        ctx.tls_root_certificate_file().map(ToString::to_string),
    )?;

    match cmd {
        InstanceCommands::Status(_) => {
            let components = fetch_instance_json::<Vec<InstanceComponentStatus>>(
                &runtime_ctx,
                "/v1/status",
                &target,
            )
            .await?;
            print_instance_status(&components, &target, output)
        }
        InstanceCommands::Datasets(_) => {
            // `/v1/status` reports only connection endpoints (http, flight,
            // metrics, opentelemetry) — never datasets. Dataset state lives on
            // the datasets route, and only when `status=true` is requested.
            let datasets = fetch_instance_json::<Vec<InstanceDatasetInfo>>(
                &runtime_ctx,
                "/v1/datasets?status=true",
                &target,
            )
            .await?;
            print_instance_datasets(&datasets, &target, output)
        }
    }
}

/// Read a JSON document from a running app instance.
async fn fetch_instance_json<T: serde::de::DeserializeOwned>(
    ctx: &RuntimeContext,
    path: &str,
    target: &AppTarget,
) -> Result<T> {
    let response = ctx.get(path).await.map_err(|err| {
        Error::cloud_with_hint(
            CloudErrorCode::NotFound,
            format!("Could not reach the instance for app {target}: {err}"),
            format!("The app may not be running yet — check 'spice cloud inspect --app {target}'."),
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

fn print_instance_status(
    components: &[InstanceComponentStatus],
    target: &AppTarget,
    output: OutputFormat,
) -> Result<()> {
    if output == OutputFormat::Json {
        return write_json(&components);
    }

    if components.is_empty() {
        println!("App instance {target} reported no components.");
        return Ok(());
    }

    let mut table = TableOutput::new(vec!["COMPONENT", "STATUS", "ENDPOINT"]);
    for component in components {
        table.add_row(vec![
            component.name.clone(),
            component.status.clone(),
            component
                .endpoint
                .clone()
                .unwrap_or_else(|| "-".to_string()),
        ]);
    }
    table.print();

    let unhealthy: Vec<&InstanceComponentStatus> = components
        .iter()
        .filter(|component| !component.status.eq_ignore_ascii_case("Ready"))
        .collect();
    if !unhealthy.is_empty() {
        println!();
        println!(
            "{} of {} components are not Ready. Check 'spice cloud logs --app {target} --level error'.",
            unhealthy.len(),
            components.len()
        );
    }

    Ok(())
}

fn print_instance_datasets(
    datasets: &[InstanceDatasetInfo],
    target: &AppTarget,
    output: OutputFormat,
) -> Result<()> {
    if output == OutputFormat::Json {
        return write_json(&datasets);
    }

    if datasets.is_empty() {
        println!("App instance {target} has no datasets configured.");
        return Ok(());
    }

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
        println!("  Logs: spice cloud logs --app {target} --level error");
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
    fn is_cloud_unauthorized_error_matches_a_rejected_credential() {
        let err = Error::cloud(CloudErrorCode::TokenExpired, "Unauthorized: token expired");

        assert!(is_cloud_unauthorized_error(&err));
    }

    #[test]
    fn is_cloud_unauthorized_error_rejects_unrelated_errors() {
        // A forbidden response means the credential is valid but the action is
        // not allowed — that must not be mistaken for a missing user identity.
        let err = Error::cloud(CloudErrorCode::Forbidden, "Forbidden: missing scope");

        assert!(!is_cloud_unauthorized_error(&err));
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

    fn create_app_args(kind: AppKind, replicas: Option<i32>) -> CreateAppArgs {
        CreateAppArgs {
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

    fn cluster_app_args(replicas: Option<i32>) -> CreateAppArgs {
        let mut args = create_app_args(AppKind::Cluster, replicas);
        args.executor_replicas = Some(1);
        args.executor_cpu = Some(1);
        args.executor_memory = Some(bytes::NumBytes::from_bytes(1024));
        args
    }

    #[test]
    fn create_cluster_requires_explicit_single_replica() {
        let err = validate_create_app_args(&create_app_args(AppKind::Cluster, None))
            .expect_err("cluster without replicas should fail");

        assert_eq!(
            err.to_string(),
            "Invalid argument: Cluster apps require --replicas 1"
        );
    }

    #[test]
    fn create_cluster_requires_executor_configuration() {
        let err = validate_create_app_args(&create_app_args(AppKind::Cluster, Some(1)))
            .expect_err("cluster without executor configuration should fail");

        assert_eq!(
            err.to_string(),
            "Invalid argument: Cluster apps require explicit executor configuration: --executor-replicas, --executor-cpu, --executor-memory"
        );
    }

    #[test]
    fn create_cluster_accepts_one_replica() {
        validate_create_app_args(&cluster_app_args(Some(1)))
            .expect("cluster with one scheduler replica should pass");
    }

    #[test]
    fn create_app_rejects_invalid_region_syntax() {
        let mut args = create_app_args(AppKind::Set, None);
        args.region = "bad_region".to_string();

        let err = validate_create_app_args(&args).expect_err("invalid region should fail");

        assert!(err.to_string().contains("Invalid region 'bad_region'"));
    }

    #[test]
    fn create_app_region_accepts_short_and_data_region_names() {
        assert_eq!(
            normalize_create_app_region("us-east-1").expect("short region should normalize"),
            "us-east-1-prod-aws-data"
        );
        assert_eq!(
            normalize_create_app_region("us-east-1-prod-aws-data")
                .expect("data region should normalize"),
            "us-east-1-prod-aws-data"
        );
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

    #[test]
    fn backfill_app_orgs_fills_empty_org_from_context() {
        let mut apps = vec![test_app("", "ltd-mint"), test_app("", "zippy-cayenne")];
        backfill_app_orgs(&mut apps, "Jeadie");
        assert_eq!(apps[0].org, "Jeadie");
        assert_eq!(apps[1].org, "Jeadie");
    }

    #[test]
    fn backfill_app_orgs_preserves_existing_org() {
        let mut apps = vec![test_app("analytics", "dashboard"), test_app("", "ltd-mint")];
        backfill_app_orgs(&mut apps, "Jeadie");
        // An app that already carries an org keeps it; only empty orgs are filled.
        assert_eq!(apps[0].org, "analytics");
        assert_eq!(apps[1].org, "Jeadie");
    }

    #[test]
    fn backfill_app_orgs_noop_when_context_empty() {
        let mut apps = vec![test_app("", "ltd-mint")];
        backfill_app_orgs(&mut apps, "");
        assert_eq!(apps[0].org, "");
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
            "the linked app",
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
        .expect("an explicit org may override a linked app");
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
        assert_eq!(OrgSource::LinkedApp.label(), "linked app");
        assert_eq!(OrgSource::ActiveOrg.label(), "active org");
    }

    #[test]
    fn whoami_names_where_the_org_came_from() {
        assert_eq!(org_source_label(None, None), "credential");
        assert_eq!(org_source_label(None, Some("spicehq")), "active org");
        assert_eq!(org_source_label(Some("spicehq"), Some("spicehq")), "--org");
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
    fn short_commit_abbreviates_to_seven_characters() {
        assert_eq!(short_commit(Some("24cb0e71fd0123456789")), "24cb0e7");
        assert_eq!(short_commit(Some("abc")), "abc");
        assert_eq!(short_commit(None), "-");
        assert_eq!(short_commit(Some("")), "-");
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
        let mut apps = vec![test_app("", "ltd-mint")];
        backfill_app_orgs(&mut apps, "Jeadie");
        let value = serde_json::to_value(&apps[0]).expect("app should serialize to JSON");
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
