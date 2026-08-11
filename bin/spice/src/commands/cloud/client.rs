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

//! Cloud API client wrapper for the Spice CLI.
//!
//! Thin wrapper around [`spice_cloud_client::CloudClient`] that adds CLI-specific
//! constructor logic (base URL selection, token resolution) and converts errors
//! into the CLI error type.

use std::collections::{BTreeMap, BTreeSet};

use crate::error::{CloudErrorCode, Error, InvalidResponseSnafu, Result};

pub use spice_cloud_client::CloudClient as InnerCloudClient;
use spice_cloud_client::types::{
    ApiKeysResponse, AuthContext, AuthExchangeResponse, ContainerImagesResponse,
    CreateDeploymentRequest, CreateProjectRequest, Deployment, LogsResponse, MetricsResponse, Org,
    Project, ProjectExecutor, ProjectKind, ProjectResourceLimits, ProjectResources,
    RegenerateApiKeyResponse, RegionsResponse, Secret, UpdateChannel, UpdateProjectRequest,
};

use super::org;

const DEV_CLOUD_API_BASE_URL: &str = "https://dev-api.spice.ai";
const CLOUD_API_BASE_URL: &str = "https://api.spice.ai";

/// The project a command acts on, after `--project`, `--org`, the linked
/// project, and the active org have been reconciled.
///
/// `org` is `None` only when nothing in the invocation named one, in which case
/// the credential's own org is used.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectTarget {
    pub org: Option<String>,
    pub project: String,
}

impl ProjectTarget {
    /// Build a target from an explicit org and project name.
    pub fn new(org: Option<String>, project: impl Into<String>) -> Self {
        Self {
            org,
            project: project.into(),
        }
    }

    /// `org/project` when the org is known, otherwise the bare project name.
    #[must_use]
    pub fn display(&self) -> String {
        match &self.org {
            Some(org) => format!("{org}/{}", self.project),
            None => self.project.clone(),
        }
    }
}

impl std::fmt::Display for ProjectTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.display())
    }
}

/// CLI wrapper around [`spice_cloud_client::CloudClient`].
///
/// Adds convenience constructors that automatically resolve the base URL and
/// authentication token from the CLI environment.
pub struct CloudClient {
    inner: InnerCloudClient,
    org: Option<String>,
}

/// What to deploy. Spice Cloud pulls the spicepod from the app's connected
/// repository, so an unset `branch`/`commit_sha` means the app's production
/// branch — the CLI never uploads a local spicepod as part of a deploy.
#[derive(Debug, Default, Clone, Copy)]
pub struct CreateDeploymentParams<'a> {
    pub image_tag: Option<&'a str>,
    pub branch: Option<&'a str>,
    pub commit_sha: Option<&'a str>,
    pub replicas: Option<i32>,
    pub debug: bool,
}

#[derive(Default)]
pub struct UpdateProjectParams<'a> {
    pub description: Option<&'a str>,
    pub visibility: Option<&'a str>,
    pub replicas: Option<i32>,
    pub image_tag: Option<&'a str>,
    pub region: Option<&'a str>,
    pub cpu: Option<i32>,
    pub memory: Option<NumBytes>,
    pub storage_size_gb: Option<f64>,
    pub executor_replicas: Option<i32>,
    pub executor_cpu: Option<i32>,
    pub executor_memory: Option<NumBytes>,
    pub spicepod: Option<String>,
    pub channel: Option<UpdateChannel>,
}

impl CloudClient {
    /// Create a new authenticated cloud client that acts on `org`.
    ///
    /// A credential stored for that org wins. Otherwise the default credential
    /// is used **only when it can be shown to belong to that same org** — the
    /// invariant is "never use one organization's token for another", not
    /// "never use the default token", and rejecting a user's own credential for
    /// their own org would break the single-credential path most people have.
    ///
    /// A service-account credential has no user identity to check against. Its
    /// organization is fixed by the OAuth client that issued it and the server
    /// authorizes every request, so it is allowed through rather than blocked
    /// on a check that cannot be performed.
    pub async fn connect(org: Option<&str>) -> Result<Self> {
        let Some(org) = org else {
            let token = org::default_token().ok_or_else(not_authenticated)?;
            return Self::with_token_for_org(token, None);
        };

        org::validate_org_name(org)?;

        if let Some(token) = org::token_for_org(org) {
            return Self::with_token_for_org(token, Some(org));
        }

        let default = org::default_token().ok_or_else(|| org_credential_missing(org))?;

        // Probe the token's own identity before granting it the requested org.
        // `get_auth_context` sends no org context precisely so the answer names
        // the org the token is *bound to*, rather than echoing back the org
        // being asked about — the latter would accept any credential.
        let probe = Self::with_token_for_org(default.clone(), None)?;
        match probe.optional_user_auth_context().await? {
            Some(context) if context.org_name.eq_ignore_ascii_case(org) => {
                Self::with_token_for_org(default, Some(org))
            }
            Some(context) => Err(default_credential_wrong_org(org, &context.org_name)),
            // A service-account credential has no user identity to check.
            None => Self::with_token_for_org(default, Some(org)),
        }
    }

    /// Create a new authenticated cloud client with an explicit bearer token,
    /// acting on `org`.
    pub fn with_token_for_org(token: impl Into<String>, org: Option<&str>) -> Result<Self> {
        let mut inner = InnerCloudClient::new(&get_base_url())
            .map_err(map_cloud_error(None))?
            .with_token(token);
        if let Some(org) = org {
            org::validate_org_name(org)?;
            inner = inner.with_org(org);
        }

        Ok(Self {
            inner,
            org: org.map(ToString::to_string),
        })
    }

    /// Create a new unauthenticated cloud client (for the login flow).
    pub fn new_unauthenticated() -> Result<Self> {
        Ok(Self {
            inner: InnerCloudClient::new(&get_base_url()).map_err(map_cloud_error(None))?,
            org: None,
        })
    }

    /// Convert an API error into a CLI error, attributing 403s to the org this
    /// client requested.
    fn err(&self, error: spice_cloud_client::error::Error) -> Error {
        map_cloud_error(self.org.as_deref())(error)
    }

    /// Get the auth URL for the login flow.
    pub fn get_auth_url(&self, auth_code: &str) -> String {
        self.inner.get_auth_url(auth_code)
    }

    /// Exchange an auth code for an access token.
    pub async fn exchange_code(&self, auth_code: &str) -> Result<Option<AuthExchangeResponse>> {
        self.inner
            .exchange_code(auth_code)
            .await
            .map_err(|error| self.err(error))
    }

    /// Exchange `OAuth2` client credentials for an access token.
    pub async fn exchange_client_credentials(
        &self,
        client_id: &str,
        client_secret: &str,
    ) -> Result<String> {
        let response = self
            .inner
            .exchange_client_credentials(client_id, client_secret)
            .await
            .map_err(|error| self.err(error))?;

        if response.token_type.eq_ignore_ascii_case("bearer") {
            Ok(response.access_token)
        } else {
            InvalidResponseSnafu {
                message: format!(
                    "Failed to exchange client credentials: unsupported OAuth token type '{}'; expected 'Bearer'",
                    response.token_type
                ),
            }
            .fail()
        }
    }

    /// Get the auth context for the current user.
    pub async fn get_auth_context(&self) -> Result<AuthContext> {
        self.inner
            .get_auth_context()
            .await
            .map_err(|error| self.err(error))
    }

    /// Returns user auth context when the token supports it.
    ///
    /// Service-account tokens cannot access the auth-context endpoint; those
    /// `Unauthorized` failures are treated as absent user context.
    pub async fn optional_user_auth_context(&self) -> Result<Option<AuthContext>> {
        match self.get_auth_context().await {
            Ok(ctx) => Ok(Some(ctx)),
            Err(err) if is_unauthorized_auth_context_error(&err) => Ok(None),
            Err(err) => Err(err),
        }
    }

    // ========================================================================
    // Apps
    // ========================================================================

    pub async fn get_project_metrics(
        &self,
        project_id: i64,
        window: Option<&str>,
    ) -> Result<MetricsResponse> {
        self.inner
            .get_project_metrics(project_id, window)
            .await
            .map_err(|error| self.err(error))
    }

    pub async fn list_projects(&self) -> Result<Vec<Project>> {
        self.inner
            .list_projects()
            .await
            .map_err(|error| self.err(error))
    }

    /// Resolve `target` to a single app the credential can see.
    ///
    /// The listing is the only place org membership is visible to the CLI, so it
    /// is also where a "wrong org" mistake is caught: an app that exists under a
    /// different org produces a switch hint rather than a bare "not found".
    pub async fn get_project(&self, target: &ProjectTarget) -> Result<Project> {
        let project_id = self.resolve_id(target).await?;
        self.get_project_by_id(project_id).await
    }

    /// Resolve a target to its numeric id without fetching the full project.
    ///
    /// Most callers only need the id to address a sub-resource. Fetching the
    /// whole project for that costs a round trip per call, and the id cannot
    /// change for the life of a command.
    pub async fn resolve_id(&self, target: &ProjectTarget) -> Result<i64> {
        // The listing and the identity are independent; overlap them.
        let (context, projects) =
            tokio::try_join!(self.optional_user_auth_context(), self.list_projects())?;
        let context_org = context.as_ref().map(|c| c.org_name.as_str());
        resolve_project_id(&projects, target, context_org)
    }

    /// List deployments for an already-resolved project.
    pub async fn list_deployments_for_id(
        &self,
        project_id: i64,
        limit: usize,
        status: Option<&str>,
    ) -> Result<Vec<Deployment>> {
        self.inner
            .list_deployments(project_id, limit, status)
            .await
            .map_err(|error| self.err(error))
    }

    /// Get API keys for an already-resolved project.
    pub async fn get_api_keys_for_id(&self, project_id: i64) -> Result<ApiKeysResponse> {
        self.inner
            .get_api_keys(project_id)
            .await
            .map_err(|error| self.err(error))
    }

    // ========================================================================
    // Organizations
    // ========================================================================

    /// List the organizations the credential can act on.
    ///
    /// Returns `Ok(None)` when the API does not serve an org listing, so callers
    /// can fall back to what the CLI knows locally instead of claiming the user
    /// belongs to no orgs.
    pub async fn list_orgs(&self) -> Result<Option<Vec<Org>>> {
        match self.inner.list_orgs().await {
            Ok(orgs) => Ok(Some(orgs)),
            Err(spice_cloud_client::error::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(self.err(error)),
        }
    }

    /// Fetch the auth context for `org` rather than the credential's own org.
    ///
    /// Doubles as a membership probe: a non-member is rejected by the server.
    pub async fn get_auth_context_for_org(&self, org: &str) -> Result<AuthContext> {
        org::validate_org_name(org)?;
        self.inner
            .get_auth_context_for_org(Some(org))
            .await
            .map_err(|error| match error {
                spice_cloud_client::error::Error::NotFound { .. } => Error::cloud_with_hint(
                    CloudErrorCode::OrgNotFound,
                    format!("Organization '{org}' was not found."),
                    "Run 'spice cloud orgs' to list the organizations you can access.",
                ),
                spice_cloud_client::error::Error::Forbidden { .. } => Error::cloud_with_hint(
                    CloudErrorCode::OrgForbidden,
                    format!("You are not a member of organization '{org}'."),
                    "Ask an owner of that organization to add you, then run 'spice cloud orgs'.",
                ),
                error => self.err(error),
            })
    }

    pub async fn get_project_by_id(&self, project_id: i64) -> Result<Project> {
        self.inner
            .get_project_by_id(project_id)
            .await
            .map_err(|error| self.err(error))
    }

    #[expect(clippy::too_many_arguments)]
    pub async fn create_project(
        &self,
        name: &str,
        region: &str,
        kind: ProjectKind,
        description: Option<&str>,
        visibility: &str,
        replicas: Option<i32>,
        cpu: Option<i32>,
        memory: Option<NumBytes>,
        storage_size_gb: Option<f64>,
        executor_replicas: Option<i32>,
        executor_cpu: Option<i32>,
        executor_memory: Option<NumBytes>,
    ) -> Result<Project> {
        let request = build_create_project_request(
            name,
            region,
            kind,
            description,
            visibility,
            replicas,
            cpu,
            memory,
            storage_size_gb,
            executor_replicas,
            executor_cpu,
            executor_memory,
        );
        self.inner
            .create_project(&request)
            .await
            .map_err(|error| self.err(error))
    }

    pub async fn update_project(
        &self,
        target: &ProjectTarget,
        params: UpdateProjectParams<'_>,
    ) -> Result<Project> {
        let app = self.get_project(target).await?;
        let resources = build_resources(params.cpu, params.memory);
        // Create and update both send storage size at the app level. The executor field remains
        // in the wire type for API compatibility, but the CLI does not set it.
        let executor = build_executor(
            params.executor_replicas,
            params.executor_cpu,
            params.executor_memory,
        );

        let request = UpdateProjectRequest {
            description: params.description.map(String::from),
            visibility: params.visibility.map(String::from),
            replicas: params.replicas,
            image_tag: params.image_tag.map(String::from),
            update_channel: params.channel.map(|channel| channel.to_string()),
            region: params.region.map(String::from),
            resources,
            executor,
            storage_size_gb: params.storage_size_gb,
            spicepod: params.spicepod,
        };
        self.inner
            .update_project(app.id, &request)
            .await
            .map_err(|error| self.err(error))
    }

    pub async fn delete_project(&self, target: &ProjectTarget) -> Result<()> {
        let project_id = self.resolve_id(target).await?;
        self.inner
            .delete_project(project_id)
            .await
            .map_err(|error| self.err(error))
    }

    // ========================================================================
    // Deployments
    // ========================================================================

    pub async fn list_deployments(
        &self,
        target: &ProjectTarget,
        limit: usize,
        status: Option<&str>,
    ) -> Result<Vec<Deployment>> {
        let project_id = self.resolve_id(target).await?;
        self.inner
            .list_deployments(project_id, limit, status)
            .await
            .map_err(|error| self.err(error))
    }

    pub async fn get_latest_deployment(&self, target: &ProjectTarget) -> Result<Deployment> {
        let deployments = self.list_deployments(target, 1, None).await?;
        deployments.into_iter().next().ok_or_else(|| {
            Error::cloud_with_hint(
                CloudErrorCode::NotFound,
                format!("No deployments found for app {target}."),
                format!("Deploy it first with 'spice cloud deploy --app {target}'."),
            )
        })
    }

    pub async fn create_deployment(
        &self,
        target: &ProjectTarget,
        params: CreateDeploymentParams<'_>,
    ) -> Result<Deployment> {
        let project_id = self.resolve_id(target).await?;
        self.create_deployment_for_id(project_id, params, target)
            .await
    }

    /// Create a deployment for an already-resolved project.
    ///
    /// `target` is carried only for error messages.
    pub async fn create_deployment_for_id(
        &self,
        project_id: i64,
        params: CreateDeploymentParams<'_>,
        target: &ProjectTarget,
    ) -> Result<Deployment> {
        let request = CreateDeploymentRequest {
            image: None,
            image_tag: params.image_tag.map(String::from),
            replicas: params.replicas,
            branch: params.branch.map(String::from),
            commit_sha: params.commit_sha.map(String::from),
            commit_message: None,
            channel: None,
            debug: params.debug,
        };
        self.inner
            .create_deployment(project_id, &request)
            .await
            .map_err(|error| match error {
                // The Cloud API rejects a second deployment while one is in
                // flight; give that its own code so automation can wait and
                // retry rather than treating it as a hard failure.
                spice_cloud_client::error::Error::Conflict { message } => Error::cloud_with_hint(
                    CloudErrorCode::DeployConflict,
                    format!("A deployment is already in progress for project {target}: {message}"),
                    format!(
                        "Check it with 'spice cloud deployments --project {target}', or wait for it to finish."
                    ),
                ),
                error => self.err(error),
            })
    }

    pub async fn get_deployment_logs(
        &self,
        target: &ProjectTarget,
        deployment_id: i64,
        limit: usize,
        since: Option<&str>,
    ) -> Result<LogsResponse> {
        let project_id = self.resolve_id(target).await?;
        self.inner
            .get_deployment_logs(project_id, deployment_id, limit, since)
            .await
            .map_err(|error| self.err(error))
    }

    // ========================================================================
    // Regions & Images
    // ========================================================================

    pub async fn list_regions(&self, env: Option<&str>) -> Result<RegionsResponse> {
        self.inner
            .list_regions(env)
            .await
            .map_err(|error| self.err(error))
    }

    pub async fn list_container_images(
        &self,
        channel: Option<&str>,
    ) -> Result<ContainerImagesResponse> {
        self.inner
            .list_container_images(channel)
            .await
            .map_err(|error| self.err(error))
    }

    // ========================================================================
    // Secrets
    // ========================================================================

    pub async fn list_secrets(&self, target: &ProjectTarget) -> Result<Vec<Secret>> {
        let project_id = self.resolve_id(target).await?;
        self.inner
            .list_secrets(project_id)
            .await
            .map_err(|error| self.err(error))
    }

    pub async fn get_secret(&self, target: &ProjectTarget, name: &str) -> Result<Secret> {
        let project_id = self.resolve_id(target).await?;
        self.inner
            .get_secret(project_id, name)
            .await
            .map_err(|error| self.err(error))
    }

    pub async fn set_secret(
        &self,
        target: &ProjectTarget,
        name: &str,
        value: &str,
    ) -> Result<Secret> {
        let project_id = self.resolve_id(target).await?;
        self.inner
            .set_secret(project_id, name, value)
            .await
            .map_err(|error| self.err(error))
    }

    pub async fn delete_secret(&self, target: &ProjectTarget, name: &str) -> Result<()> {
        let project_id = self.resolve_id(target).await?;
        self.inner
            .delete_secret(project_id, name)
            .await
            .map_err(|error| self.err(error))
    }

    // ========================================================================
    // API Keys
    // ========================================================================

    pub async fn get_api_keys(&self, target: &ProjectTarget) -> Result<ApiKeysResponse> {
        let project_id = self.resolve_id(target).await?;
        self.inner
            .get_api_keys(project_id)
            .await
            .map_err(|error| self.err(error))
    }

    pub async fn regenerate_api_key(
        &self,
        target: &ProjectTarget,
        key_number: u8,
    ) -> Result<RegenerateApiKeyResponse> {
        let project_id = self.resolve_id(target).await?;
        self.inner
            .regenerate_api_key(project_id, key_number)
            .await
            .map_err(|error| self.err(error))
    }
}

// ============================================================================
// Helper functions
// ============================================================================

fn get_base_url() -> String {
    if let Ok(url) = std::env::var("SPICE_CLOUD_API_URL") {
        return url;
    }

    // Use dev API for dev versions
    let version = crate::commands::version::cli_version();
    if version.ends_with("-dev") {
        return DEV_CLOUD_API_BASE_URL.to_string();
    }

    CLOUD_API_BASE_URL.to_string()
}

fn not_authenticated() -> Error {
    Error::cloud_with_hint(
        CloudErrorCode::NotAuthenticated,
        "Not authenticated with Spice Cloud.",
        "Run 'spice cloud login' (or set SPICE_SPICEAI_TOKEN) to authenticate.",
    )
}

/// The default credential belongs to a different organization than the one
/// named, so using it would run the command somewhere the caller did not ask
/// for while reporting the org they did ask for.
fn default_credential_wrong_org(requested: &str, actual: &str) -> Error {
    Error::cloud_with_hint(
        CloudErrorCode::OrgCredentialMissing,
        format!(
            "No Spice Cloud credential is stored for organization '{requested}'; your default credential belongs to '{actual}'."
        ),
        format!(
            "Authenticate for it with 'spice cloud login pat --org {requested}' (or 'spice cloud login api --org {requested}' for automation)."
        ),
    )
}

/// An organization was named but no credential is bound to it.
///
/// Names the credential's own org when it is known, because "you are logged in
/// as someone else" is the actual problem and the user cannot see it otherwise.
fn org_credential_missing(org: &str) -> Error {
    let current = if org::default_token().is_some() {
        " Your default credential belongs to a different organization."
    } else {
        ""
    };

    Error::cloud_with_hint(
        CloudErrorCode::OrgCredentialMissing,
        format!("No Spice Cloud credential is stored for organization '{org}'.{current}"),
        format!(
            "Authenticate for it with 'spice cloud login pat --org {org}' (or 'spice cloud login api --org {org}' for automation), or set {}.",
            org::org_token_var(org)
        ),
    )
}

/// Split a user-supplied `org/app` (or bare `app`) into its parts.
///
/// The org is `None` for a bare name, which the caller resolves from `--org`,
/// the linked app, or the active org.
#[must_use]
pub fn parse_org_project(org_app: &str) -> (Option<String>, String) {
    match org_app.split_once('/') {
        Some((org, app)) if !org.is_empty() => (Some(org.to_string()), app.to_string()),
        Some((_, app)) => (None, app.to_string()),
        None => (None, org_app.to_string()),
    }
}

/// The org an app belongs to: its own when the payload carries one, otherwise
/// the credential's org, which is the only org `/v1/apps` can be listing.
fn effective_project_org<'a>(app: &'a Project, context_org: Option<&'a str>) -> Option<&'a str> {
    if app.org.is_empty() {
        context_org.filter(|org| !org.is_empty())
    } else {
        Some(app.org.as_str())
    }
}

/// Resolve an app ID from a listing.
///
/// When `target.org` is set it must match; an app of the same name under another
/// visible org is reported as [`CloudErrorCode::WrongOrg`] with the switch to
/// make, because "not found" sends the operator looking for a typo that is not
/// there. When no org is known — the credential is a service account and the
/// listing omits orgs — a uniquely named app still resolves, and an ambiguous
/// one is refused rather than guessed.
fn resolve_project_id(
    apps: &[Project],
    target: &ProjectTarget,
    context_org: Option<&str>,
) -> Result<i64> {
    let wanted_org = target
        .org
        .as_deref()
        .or(context_org)
        .filter(|o| !o.is_empty());

    let mut org_unknown_matches = Vec::new();
    let mut other_org_matches = BTreeSet::new();

    for app in apps {
        if app.name != target.project {
            continue;
        }

        match (effective_project_org(app, context_org), wanted_org) {
            (Some(app_org), Some(wanted)) if app_org.eq_ignore_ascii_case(wanted) => {
                return Ok(app.id);
            }
            (Some(app_org), Some(_)) => {
                other_org_matches.insert(app_org.to_string());
            }
            (Some(_) | None, None) | (None, Some(_)) => org_unknown_matches.push(app.id),
        }
    }

    if let [id] = org_unknown_matches.as_slice() {
        return Ok(*id);
    }

    if org_unknown_matches.len() > 1 {
        return Err(Error::cloud_with_hint(
            CloudErrorCode::ProjectNotFound,
            format!(
                "Multiple apps named '{}' are visible and none reports an organization.",
                target.project
            ),
            "Pass --app <org>/<app>, or authenticate with a user token so org context is available.",
        ));
    }

    if !other_org_matches.is_empty() {
        let others: Vec<String> = other_org_matches.iter().cloned().collect();
        let requested = wanted_org.unwrap_or("the active organization");
        return Err(Error::cloud_with_hint(
            CloudErrorCode::WrongOrg,
            format!(
                "Project '{}' was not found in organization '{requested}', but exists in {}.",
                target.project,
                format_org_list(&others)
            ),
            format!(
                "Run 'spice cloud org use {0}', or pass --app {0}/{1}.",
                others.first().map_or("<org>", String::as_str),
                target.project
            ),
        ));
    }

    let visible_orgs: Vec<String> = apps
        .iter()
        .filter_map(|app| effective_project_org(app, context_org).map(ToString::to_string))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    let hint = match wanted_org {
        Some(org)
            if !visible_orgs
                .iter()
                .any(|seen| seen.eq_ignore_ascii_case(org)) =>
        {
            format!(
                "This credential can see {}. Run 'spice cloud orgs' to list your organizations, then 'spice cloud org use <org>'.",
                if visible_orgs.is_empty() {
                    "no organizations".to_string()
                } else {
                    format_org_list(&visible_orgs)
                }
            )
        }
        _ => format!(
            "Run 'spice cloud apps{}' to list the apps you can reach.",
            wanted_org.map_or(String::new(), |org| format!(" --org {org}"))
        ),
    };

    Err(Error::cloud_with_hint(
        CloudErrorCode::ProjectNotFound,
        format!("Project '{target}' was not found."),
        hint,
    ))
}

/// Render org names as `'a'`, `'a' and 'b'`, or `'a', 'b', and 'c'`.
fn format_org_list(orgs: &[String]) -> String {
    let quoted: Vec<String> = orgs.iter().map(|org| format!("'{org}'")).collect();
    match quoted.split_last() {
        None => "no organizations".to_string(),
        Some((last, [])) => last.clone(),
        Some((last, [first])) => format!("{first} and {last}"),
        Some((last, rest)) => format!("{}, and {last}", rest.join(", ")),
    }
}

/// Convert an API error into a CLI error carrying a stable code.
///
/// `org` is the organization the request asked for, so a 403 can say which
/// membership check failed instead of a bare "forbidden".
fn map_cloud_error(org: Option<&str>) -> impl Fn(spice_cloud_client::error::Error) -> Error {
    let org = org.map(ToString::to_string);
    move |error| {
        use spice_cloud_client::error::Error as CloudError;
        match error {
            CloudError::Unauthorized { message } => Error::cloud_with_hint(
                CloudErrorCode::TokenExpired,
                format!("Spice Cloud rejected the credential: {message}"),
                "Run 'spice cloud login' to re-authenticate.",
            ),
            // A 403 on a management route means "this action is not allowed",
            // which is usually a missing role or scope rather than a missing
            // membership. Only the dedicated membership probe can tell the
            // difference, so it — not this mapping — emits `org_forbidden`.
            CloudError::Forbidden { message } => Error::cloud_with_hint(
                CloudErrorCode::Forbidden,
                match &org {
                    Some(org) => format!("Not permitted in organization '{org}': {message}"),
                    None => format!("Not permitted: {message}"),
                },
                "Check the credential's role and scopes for this action, or contact your organization admin.",
            ),
            CloudError::AuthorizationDenied => Error::DeviceAuthorizationDenied,
            CloudError::InvalidResponse { message } => Error::InvalidResponse { message },
            CloudError::NotFound { message } => {
                Error::cloud(CloudErrorCode::NotFound, format!("Not found: {message}"))
            }
            CloudError::Conflict { message } => {
                Error::cloud(CloudErrorCode::Conflict, format!("Conflict: {message}"))
            }
            CloudError::Api { status, message } => Error::cloud(
                CloudErrorCode::ApiError,
                format!("Spice Cloud request failed with status {status}: {message}"),
            ),
            CloudError::HttpRequest { source } => Error::HttpRequestFailed { source },
            CloudError::JsonParse { source } => Error::InvalidResponse {
                message: format!("Failed to parse response: {source}"),
            },
        }
    }
}

use super::bytes::NumBytes;

#[expect(clippy::too_many_arguments)]
fn build_create_project_request(
    name: &str,
    region: &str,
    kind: ProjectKind,
    description: Option<&str>,
    visibility: &str,
    replicas: Option<i32>,
    cpu: Option<i32>,
    memory: Option<NumBytes>,
    storage_size_gb: Option<f64>,
    executor_replicas: Option<i32>,
    executor_cpu: Option<i32>,
    executor_memory: Option<NumBytes>,
) -> CreateProjectRequest {
    let resources = build_resources(cpu, memory);
    let executor = build_executor(executor_replicas, executor_cpu, executor_memory);

    let (tags, replicas) = match kind {
        ProjectKind::Cluster => {
            let mut tags = BTreeMap::new();
            tags.insert("kind".to_string(), "cluster".to_string());
            (Some(tags), Some(1))
        }
        ProjectKind::Set => (None, replicas),
    };

    CreateProjectRequest {
        name: name.to_string(),
        description: description.map(String::from),
        visibility: visibility.to_string(),
        // The Cloud create-app endpoint currently accepts the target deployment region
        // in the legacy `cname` request field; update-app uses the newer `region` field.
        cname: Some(region.to_string()),
        cluster_name: None,
        tags,
        replicas,
        resources,
        executor,
        storage_size_gb,
    }
}

/// Build an [`ProjectResources`] from optional CPU (vCPUs) and a parsed [`NumBytes`] memory value.
///
/// Returns `None` if neither is provided.
fn build_resources(cpu: Option<i32>, memory: Option<NumBytes>) -> Option<ProjectResources> {
    if cpu.is_none() && memory.is_none() {
        return None;
    }
    Some(ProjectResources {
        limits: ProjectResourceLimits {
            cpu: cpu.map(|v| v.to_string()),
            memory: memory.map(NumBytes::to_resource_string),
            ephemeral_storage: None,
        },
        requests: None,
    })
}

/// Build an [`ProjectExecutor`] from optional executor params.
///
/// Returns `None` if no executor-related fields are provided.
fn build_executor(
    replicas: Option<i32>,
    cpu: Option<i32>,
    memory: Option<NumBytes>,
) -> Option<ProjectExecutor> {
    if replicas.is_none() && cpu.is_none() && memory.is_none() {
        return None;
    }
    Some(ProjectExecutor {
        replicas,
        resources: build_resources(cpu, memory),
        storage_size_gb: None,
    })
}

/// A rejected credential on the auth-context endpoint.
///
/// Service-account tokens are valid for the management API but have no user
/// identity, so callers that only want the identity treat this as "absent".
pub fn is_unauthorized_auth_context_error(err: &crate::error::Error) -> bool {
    err.cloud_code() == Some(CloudErrorCode::TokenExpired)
}

pub fn is_device_authorization_denied_error(error: &crate::error::Error) -> bool {
    matches!(error, crate::error::Error::DeviceAuthorizationDenied)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_resources_does_not_default_memory() {
        let resources = build_resources(Some(4), None).expect("cpu should create resources");

        assert_eq!(resources.limits.cpu.as_deref(), Some("4"));
        assert!(resources.limits.memory.is_none());
    }

    #[test]
    fn build_resources_preserves_memory_unit() {
        let memory = NumBytes::parse("3500Mi").expect("memory should parse");

        let resources =
            build_resources(None, Some(memory)).expect("memory should create resources");

        assert_eq!(resources.limits.memory.as_deref(), Some("3500Mi"));
    }

    #[test]
    fn build_executor_does_not_default_executor_memory() {
        let executor =
            build_executor(None, Some(2), None).expect("executor cpu should create executor");

        let resources = executor.resources.expect("executor resources should exist");
        assert_eq!(resources.limits.cpu.as_deref(), Some("2"));
        assert!(resources.limits.memory.is_none());
    }

    #[test]
    fn create_project_request_sends_region_as_cname() {
        let request = build_create_project_request(
            "app",
            "us-east-1-prod-aws-data",
            ProjectKind::Set,
            None,
            "private",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );

        let value = serde_json::to_value(request).expect("create app request should serialize");

        assert_eq!(
            value,
            serde_json::json!({
                "name": "app",
                "visibility": "private",
                "cname": "us-east-1-prod-aws-data"
            })
        );
    }

    fn test_app(id: i64, name: &str, org: &str) -> Project {
        Project {
            id,
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

    fn target(org: Option<&str>, app: &str) -> ProjectTarget {
        ProjectTarget::new(org.map(ToString::to_string), app)
    }

    #[test]
    fn parse_org_project_splits_qualified_and_bare_names() {
        assert_eq!(
            parse_org_project("spicehq/team-app"),
            (Some("spicehq".to_string()), "team-app".to_string())
        );
        assert_eq!(
            parse_org_project("team-app"),
            (None, "team-app".to_string())
        );
        // A leading slash names no org, so the caller's org context still applies.
        assert_eq!(
            parse_org_project("/team-app"),
            (None, "team-app".to_string())
        );
    }

    #[test]
    fn resolve_project_id_matches_the_requested_org() {
        let apps = vec![
            test_app(1, "dashboard", "analytics"),
            test_app(2, "dashboard", "other"),
        ];

        let id = resolve_project_id(&apps, &target(Some("analytics"), "dashboard"), None)
            .expect("should match org from app payload");

        assert_eq!(id, 1);
    }

    #[test]
    fn resolve_project_id_uses_context_org_when_the_listing_omits_it() {
        // `/v1/apps` does not populate `org`, so the credential's own org is
        // the only evidence of which org the listing describes.
        let apps = vec![test_app(7, "dashboard", "")];

        let id = resolve_project_id(
            &apps,
            &target(Some("analytics"), "dashboard"),
            Some("analytics"),
        )
        .expect("should match via auth context org");

        assert_eq!(id, 7);
    }

    #[test]
    fn resolve_project_id_reports_wrong_org_rather_than_not_found() {
        // Regression guard for the multi-org failure this replaced: asking for
        // an app that lives in another visible org used to say only "not found",
        // sending the operator hunting for a typo that was not there.
        let apps = vec![test_app(1, "team-app", "spicehq")];

        let err = resolve_project_id(&apps, &target(Some("lukekim"), "team-app"), None)
            .expect_err("org mismatch should not resolve");

        assert_eq!(err.cloud_code(), Some(CloudErrorCode::WrongOrg));
        let rendered = err.to_string();
        assert!(
            rendered.contains("not found in organization 'lukekim'")
                && rendered.contains("'spicehq'"),
            "error should name both orgs: {rendered}"
        );
        assert!(
            rendered.contains("spice cloud org use spicehq"),
            "error should offer the switch: {rendered}"
        );
    }

    #[test]
    fn resolve_project_id_suggests_listing_orgs_when_the_org_is_not_visible() {
        let apps = vec![test_app(1, "other-app", "lukekim")];

        let err = resolve_project_id(&apps, &target(Some("spicehq"), "team-app"), Some("lukekim"))
            .expect_err("unknown app in an unseen org should fail");

        assert_eq!(err.cloud_code(), Some(CloudErrorCode::ProjectNotFound));
        assert!(
            err.to_string().contains("spice cloud orgs"),
            "error should point at org discovery: {err}"
        );
    }

    #[test]
    fn resolve_project_id_allows_a_unique_match_when_no_org_is_known() {
        // A service-account credential has no user auth context, so neither the
        // listing nor the identity reports an org; a single match is unambiguous.
        let apps = vec![test_app(9, "dashboard", "")];

        let id = resolve_project_id(&apps, &target(None, "dashboard"), None)
            .expect("single org-less app should match");

        assert_eq!(id, 9);
    }

    #[test]
    fn resolve_project_id_refuses_to_guess_between_ambiguous_matches() {
        let apps = vec![test_app(1, "dashboard", ""), test_app(2, "dashboard", "")];

        let err = resolve_project_id(&apps, &target(None, "dashboard"), None)
            .expect_err("ambiguous apps should fail");

        assert_eq!(err.cloud_code(), Some(CloudErrorCode::ProjectNotFound));
        assert!(
            err.to_string().contains("Multiple apps named 'dashboard'"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn app_target_displays_qualified_and_bare_names() {
        assert_eq!(
            target(Some("spicehq"), "team-app").display(),
            "spicehq/team-app"
        );
        assert_eq!(target(None, "team-app").display(), "team-app");
    }

    #[test]
    fn format_org_list_reads_as_prose() {
        assert_eq!(format_org_list(&["a".to_string()]), "'a'");
        assert_eq!(
            format_org_list(&["a".to_string(), "b".to_string()]),
            "'a' and 'b'"
        );
        assert_eq!(
            format_org_list(&["a".to_string(), "b".to_string(), "c".to_string()]),
            "'a', 'b', and 'c'"
        );
    }

    #[test]
    fn a_forbidden_action_is_not_reported_as_a_membership_failure() {
        // A 403 on a management route usually means a missing role or scope,
        // not a missing membership. Reporting `org_forbidden` for both sent
        // automation down the wrong remediation path and told a legitimate
        // member to ask for an invitation they already have.
        let err = map_cloud_error(Some("spicehq"))(spice_cloud_client::error::Error::Forbidden {
            message: "requires admin role".to_string(),
        });

        assert_eq!(err.cloud_code(), Some(CloudErrorCode::Forbidden));
        assert!(
            err.to_string().contains("organization 'spicehq'"),
            "the org is still worth naming for context: {err}"
        );
    }

    #[test]
    fn forbidden_without_org_context_stays_generic() {
        let err = map_cloud_error(None)(spice_cloud_client::error::Error::Forbidden {
            message: "missing scope".to_string(),
        });

        assert_eq!(err.cloud_code(), Some(CloudErrorCode::Forbidden));
    }

    #[test]
    fn a_default_credential_from_another_org_is_refused_by_name() {
        // The invariant is "never use one org's token for another", not "never
        // use the default token". When the default credential demonstrably
        // belongs elsewhere, say so — and name both orgs, because "no
        // credential" alone sends the user looking for the wrong problem.
        let err = default_credential_wrong_org("spicehq", "lukekim");

        assert_eq!(err.cloud_code(), Some(CloudErrorCode::OrgCredentialMissing));
        let rendered = err.to_string();
        assert!(
            rendered.contains("'spicehq'") && rendered.contains("'lukekim'"),
            "the error must name the requested and the actual org: {rendered}"
        );
        assert!(
            rendered.contains("login pat --org spicehq"),
            "the error must say how to authenticate for the requested org: {rendered}"
        );
    }

    #[test]
    fn a_named_org_without_a_credential_fails_closed() {
        // The credential fallback used to run the command against the default
        // token's org while reporting the requested one.
        let err = org_credential_missing("spicehq");

        assert_eq!(err.cloud_code(), Some(CloudErrorCode::OrgCredentialMissing));
        let rendered = err.to_string();
        assert!(
            rendered.contains("'spicehq'") && rendered.contains("login pat --org spicehq"),
            "the error must name the org and how to authenticate for it: {rendered}"
        );
    }

    #[test]
    fn unauthorized_maps_to_token_expired() {
        let err = map_cloud_error(None)(spice_cloud_client::error::Error::Unauthorized {
            message: "invalid or expired token".to_string(),
        });

        assert_eq!(err.cloud_code(), Some(CloudErrorCode::TokenExpired));
        assert!(is_unauthorized_auth_context_error(&err));
    }
}
