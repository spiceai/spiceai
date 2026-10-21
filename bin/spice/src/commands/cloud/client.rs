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

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    pin::Pin,
};

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

/// The project a command acts on, after `--project`, `--org`, the enrolled
/// instance attachment, and the active org have been reconciled.
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
    /// A credential stored for that org wins. Otherwise a default *user*
    /// credential may be used for any organization whose membership endpoint
    /// accepts it. Machine credentials have no membership identity and remain
    /// organization-bound, so they require an explicitly stored per-org token.
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

        let probe = Self::with_token_for_org(default.clone(), None)?;
        // Ask for the identity directly rather than through
        // [`Self::optional_user_auth_context`]: that helper folds "rejected"
        // and "cannot describe" into one absent answer, and they mean opposite
        // things here.
        match probe.get_auth_context().await {
            Ok(_) => {
                confirm_org_access(&probe, org).await?;
                Self::with_token_for_org(default, Some(org))
            }
            Err(err) if err.cloud_code() == Some(CloudErrorCode::TokenExpired) => {
                Err(super::rejected_user_credential_error(None, Some(org)))
            }
            // No identity came back, so whether this is a user token — usable
            // for every member org — is unknown. Declining would refuse a
            // member their own organization on the strength of a lookup that
            // failed, so send the request and let the server, which is
            // authoritative on membership, answer.
            Err(err) if is_absent_user_identity_error(&err) => {
                tracing::debug!(
                    "Spice Cloud did not describe the identity behind the default credential ({err}); acting on organization '{org}' with it and letting the server decide"
                );
                Self::with_token_for_org(default, Some(org))
            }
            Err(err) => Err(err),
        }
    }

    /// Create a new authenticated cloud client with an explicit bearer token,
    /// acting on `org`.
    pub fn with_token_for_org(token: impl Into<String>, org: Option<&str>) -> Result<Self> {
        Self::with_token_for_org_at(token, org, &get_base_url())
    }

    /// Create an authenticated client against an explicit Cloud API base URL.
    ///
    /// Cloud Connect uses this for `--endpoint` and local HTTP fixtures; every
    /// other Cloud command continues to use [`Self::with_token_for_org`].
    pub fn with_token_for_org_at(
        token: impl Into<String>,
        org: Option<&str>,
        base_url: &str,
    ) -> Result<Self> {
        let mut inner = InnerCloudClient::new(base_url)
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

    /// Returns user auth context when Spice Cloud describes one.
    ///
    /// `None` means the identity endpoint did not describe a user. A 401 may
    /// identify a valid machine credential, but may instead mean the
    /// credential expired or was revoked; a 404 proves only that no user was
    /// described. A caller that must tell those cases apart should call
    /// [`Self::get_auth_context`] and match the error itself.
    pub async fn optional_user_auth_context(&self) -> Result<Option<AuthContext>> {
        match self.get_auth_context().await {
            Ok(ctx) => Ok(Some(ctx)),
            Err(err) if is_absent_user_identity_error(&err) => Ok(None),
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
        let (project_id, listing_org) = self.resolve_id_and_org(target).await?;
        let project = self.get_project_by_id(project_id).await?;
        Ok(self.attribute(project, listing_org.as_deref()))
    }

    /// Stamp the org a project belongs to onto a payload that omits it.
    ///
    /// Spice Cloud returns no `org` on a project, so a command would otherwise
    /// print a bare name and serialize `"org": ""` for the very project whose
    /// org it just used to find it — while `spice cloud projects`, run against
    /// the same credential, named the org for that same project.
    fn attribute(&self, mut project: Project, org: Option<&str>) -> Project {
        if project.org.is_empty()
            && let Some(org) = org.or(self.org.as_deref()).filter(|org| !org.is_empty())
        {
            project.org = org.to_string();
        }
        project
    }

    /// The org whose projects this credential sees when no command named one.
    ///
    /// `None` for a service-account token, which has no user identity to ask.
    async fn credential_org(&self) -> Result<Option<String>> {
        Ok(self
            .optional_user_auth_context()
            .await?
            .map(|context| context.org_name)
            .filter(|org| !org.is_empty()))
    }

    /// Resolve a target to its numeric id without fetching the full project.
    ///
    /// Most callers only need the id to address a sub-resource. Fetching the
    /// whole project for that costs a round trip per call, and the id cannot
    /// change for the life of a command.
    pub async fn resolve_id(&self, target: &ProjectTarget) -> Result<i64> {
        Ok(self.resolve_id_and_org(target).await?.0)
    }

    /// Resolve a target to its id and to the org of the listing that resolved
    /// it, so a caller can attribute a payload Spice Cloud returns without one.
    ///
    /// The org is a by-product of the resolution rather than a second lookup:
    /// discarding it here is what left the default path unable to name an org
    /// it had already been told.
    async fn resolve_id_and_org(&self, target: &ProjectTarget) -> Result<(i64, Option<String>)> {
        // A listing this client asked an organization for describes that
        // organization, so the identity is not worth a round trip: it would
        // answer with the org the credential is bound to, and every project
        // would be attributed to it.
        if let Some(org) = self.org.as_deref() {
            let projects = self.list_projects().await?;
            let listing_org = resolve_listing_org(Some(org), None);
            let id = resolve_project_id(&projects, target, listing_org)?;
            return Ok((id, listing_org.map(ToString::to_string)));
        }

        // Nothing named an organization, so the listing is the credential's
        // own. The listing and the identity are independent; overlap them.
        let (context, projects) =
            tokio::try_join!(self.optional_user_auth_context(), self.list_projects())?;
        let credential_org = context.as_ref().map(|c| c.org_name.as_str());
        let listing_org = resolve_listing_org(None, credential_org);
        let id = resolve_project_id(&projects, target, listing_org)?;
        Ok((id, listing_org.map(ToString::to_string)))
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
            .map_err(|error| self.map_org_probe_error(org, error))
    }

    /// This client, carrying `org` on every request it makes.
    fn scoped_to_org(&self, org: &str) -> Self {
        Self {
            inner: self.inner.clone().with_org(org),
            org: Some(org.to_string()),
        }
    }

    /// Render a failed organization probe.
    ///
    /// Named rather than inline because [`org_probe_is_inconclusive`] has to
    /// agree with the codes produced here: a rule that tolerates a code this
    /// never emits reads as working and silently does nothing.
    fn map_org_probe_error(&self, org: &str, error: spice_cloud_client::error::Error) -> Error {
        match error {
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
        }
    }

    pub async fn get_project_by_id(&self, project_id: i64) -> Result<Project> {
        self.inner
            .get_project_by_id(project_id)
            .await
            .map_err(|error| self.err(error))
    }

    pub async fn create_project(
        &self,
        name: &str,
        description: Option<&str>,
        visibility: &str,
        placement: CreateProjectPlacement,
    ) -> Result<Project> {
        // Spice Cloud creates the project in the org this client acts on, so
        // that org is its org. There is no listing here to learn it from, so
        // when the command named none, ask the identity for the credential's
        // own — and ask *before* the create, so that a failed lookup stays
        // side-effect free instead of reporting a project that now exists as a
        // failure and sending the caller into a duplicate retry. The org is a
        // label on the answer rather than part of it, so a lookup that fails
        // anyway leaves the project unattributed rather than uncreated.
        let created_in = match self.org {
            Some(_) => None,
            None => self.credential_org().await.ok().flatten(),
        };

        let request = build_create_project_request(name, description, visibility, placement);
        let project = self
            .inner
            .create_project(&request)
            .await
            .map_err(|error| self.err(error))?;
        Ok(self.attribute(project, created_in.as_deref()))
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
        let project = self
            .inner
            .update_project(app.id, &request)
            .await
            .map_err(|error| self.err(error))?;
        // The project this update just read is the same one, already attributed
        // by the resolution above, so the update response costs no extra lookup
        // to name.
        Ok(self.attribute(project, Some(app.org.as_str())))
    }

    pub async fn delete_project(&self, target: &ProjectTarget) -> Result<()> {
        let project_id = self.resolve_id(target).await?;
        self.delete_project_by_id(project_id).await
    }

    /// Delete the project with an already-resolved immutable Cloud ID.
    pub async fn delete_project_by_id(&self, project_id: i64) -> Result<()> {
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

pub(crate) fn get_base_url() -> String {
    if let Ok(url) = std::env::var("SPICE_CLOUD_API_URL") {
        return url;
    }

    // Compatibility for one release. `SPICE_CLOUD_API_URL` is authoritative;
    // the old portal-origin variable is converted only for the known hosted
    // origins, while arbitrary self-hosted origins remain unchanged.
    if let Ok(url) = std::env::var("SPICE_BASE_URL") {
        return api_base_url_from_legacy_portal(&url);
    }

    // Use dev API for dev versions
    let version = crate::commands::version::cli_version();
    if version.ends_with("-dev") {
        return DEV_CLOUD_API_BASE_URL.to_string();
    }

    CLOUD_API_BASE_URL.to_string()
}

fn api_base_url_from_legacy_portal(base_url: &str) -> String {
    match base_url.trim_end_matches('/') {
        "https://spice.ai" => "https://api.spice.ai".to_string(),
        "https://dev.spice.ai" => "https://dev-api.spice.ai".to_string(),
        other => other.to_string(),
    }
}

/// Portal origin paired with the authoritative Cloud API origin.
#[must_use]
pub(crate) fn portal_base_url() -> String {
    portal_base_url_from_api(&get_base_url())
}

fn portal_base_url_from_api(base: &str) -> String {
    let Ok(mut url) = reqwest::Url::parse(base) else {
        return base.trim_end_matches('/').to_string();
    };
    let Some(host) = url.host_str() else {
        return base.trim_end_matches('/').to_string();
    };
    let portal_host = if let Some(rest) = host.strip_prefix("api.") {
        Some(rest.to_string())
    } else if let Some(rest) = host.strip_suffix("-api.spice.ai") {
        Some(format!("{rest}.spice.ai"))
    } else if let Some((prefix, rest)) = host.split_once(".api.") {
        Some(format!("{prefix}.{rest}"))
    } else {
        None
    };
    if let Some(portal_host) = portal_host
        && url.set_host(Some(&portal_host)).is_ok()
    {
        return url.as_str().trim_end_matches('/').to_string();
    }
    base.trim_end_matches('/').to_string()
}

fn not_authenticated() -> Error {
    Error::cloud_with_hint(
        CloudErrorCode::NotAuthenticated,
        "Not authenticated with Spice Cloud.",
        "Run 'spice cloud login' (or set SPICE_SPICEAI_TOKEN) to authenticate.",
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
            "Authenticate for it with 'spice cloud login token --org {org}' (or 'spice cloud login api --org {org}' for automation), or set {}.",
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

/// The organization a project listing describes.
///
/// Spice Cloud scopes `/v1/apps` to the organization named in the request
/// header, so the org a client acts on is the org its listing belongs to. The
/// credential's own org answers only when nothing named one: the identity
/// endpoint reports the org the *token* is bound to, which is a different
/// organization whenever a command names one, and labelling one org's projects
/// with another's name is wrong in the table, in `--output json`, and in every
/// name resolved from it.
pub(super) fn resolve_listing_org<'a>(
    client_org: Option<&'a str>,
    credential_org: Option<&'a str>,
) -> Option<&'a str> {
    client_org.or(credential_org).filter(|org| !org.is_empty())
}

/// The org an app belongs to: its own when the payload carries one, otherwise
/// the org whose listing it arrived in.
fn effective_project_org<'a>(app: &'a Project, listing_org: Option<&'a str>) -> Option<&'a str> {
    if app.org.is_empty() {
        listing_org.filter(|org| !org.is_empty())
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
    listing_org: Option<&str>,
) -> Result<i64> {
    let wanted_org = target
        .org
        .as_deref()
        .or(listing_org)
        .filter(|o| !o.is_empty());

    let mut org_unknown_matches = Vec::new();
    let mut other_org_matches = BTreeSet::new();

    for app in apps {
        if app.name != target.project {
            continue;
        }

        match (effective_project_org(app, listing_org), wanted_org) {
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
        .filter_map(|app| effective_project_org(app, listing_org).map(ToString::to_string))
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
pub(crate) fn format_org_list(orgs: &[String]) -> String {
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
            CloudError::ResponseTooLarge { limit } => Error::InvalidResponse {
                message: format!("Spice Cloud response exceeded the {limit} byte limit"),
            },
            CloudError::JsonParse { source } => Error::InvalidResponse {
                message: format!("Failed to parse response: {source}"),
            },
        }
    }
}

use super::bytes::NumBytes;

/// The placement `POST /v1/projects` is asked for, and everything that only
/// means something once a placement exists.
///
/// Spice Cloud resolves the project's kind from whether the request names a
/// region source at all, and refuses a client-supplied `kind`. Modelling the
/// two answers as separate variants is what makes the standalone request
/// unable to carry a region, replica count, resource limit, or executor: those
/// fields exist only on [`ManagedProjectPlacement`], so there is no path that
/// sets one and still omits the region source.
#[derive(Debug)]
pub enum CreateProjectPlacement {
    /// A Spice-managed project: a region, and the hosted runtime that region
    /// provisions.
    Managed(ManagedProjectPlacement),
    /// A Cloud Connect project. The request names no region source, so Spice
    /// Cloud creates the project with no instance attached; the operator's own
    /// `spiced` becomes its runtime when the instance is attached, and the
    /// region follows from the stamp that instance's control stream terminates
    /// on.
    Standalone,
}

/// Region and hosted-runtime configuration for a Spice-managed project.
#[derive(Debug)]
pub struct ManagedProjectPlacement {
    /// Data region name, already normalized (e.g. `us-east-1-prod-aws-data`).
    pub region: String,
    pub kind: ProjectKind,
    pub replicas: Option<i32>,
    pub cpu: Option<i32>,
    pub memory: Option<NumBytes>,
    pub storage_size_gb: Option<f64>,
    pub executor_replicas: Option<i32>,
    pub executor_cpu: Option<i32>,
    pub executor_memory: Option<NumBytes>,
}

fn build_create_project_request(
    name: &str,
    description: Option<&str>,
    visibility: &str,
    placement: CreateProjectPlacement,
) -> CreateProjectRequest {
    let base = CreateProjectRequest {
        name: name.to_string(),
        description: description.map(String::from),
        visibility: visibility.to_string(),
        cname: None,
        cluster_name: None,
        tags: None,
        replicas: None,
        resources: None,
        executor: None,
        storage_size_gb: None,
    };

    // Every field left unset is skipped on the wire, so this is the request
    // with no region source in it — which is what Spice Cloud reads as a
    // request for a Cloud Connect project.
    let CreateProjectPlacement::Managed(managed) = placement else {
        return base;
    };

    let (tags, replicas) = match managed.kind {
        ProjectKind::Cluster => {
            let mut tags = BTreeMap::new();
            tags.insert("kind".to_string(), "cluster".to_string());
            (Some(tags), Some(1))
        }
        ProjectKind::Set => (None, managed.replicas),
    };

    CreateProjectRequest {
        // The Cloud create-app endpoint currently accepts the target deployment region
        // in the legacy `cname` request field; update-app uses the newer `region` field.
        cname: Some(managed.region),
        tags,
        replicas,
        resources: build_resources(managed.cpu, managed.memory),
        executor: build_executor(
            managed.executor_replicas,
            managed.executor_cpu,
            managed.executor_memory,
        ),
        storage_size_gb: managed.storage_size_gb,
        ..base
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

/// What an identity-endpoint failure proves about a credential.
#[derive(Debug, PartialEq, Eq)]
enum IdentityFailure {
    /// Spice Cloud rejected it: expired, revoked, or not a user credential.
    Rejected,
    /// Spice Cloud had no user to describe. The credential may still be a
    /// perfectly good user token — this says nothing either way.
    Undescribed,
    /// Something the caller has to see, such as a refusal or a server error.
    Fatal,
}

fn classify_identity_failure(err: &crate::error::Error) -> IdentityFailure {
    match err.cloud_code() {
        Some(CloudErrorCode::TokenExpired) => IdentityFailure::Rejected,
        Some(CloudErrorCode::NotFound) => IdentityFailure::Undescribed,
        _ => IdentityFailure::Fatal,
    }
}

/// The stored credentials that could act as a user, most specific first.
///
/// Both link stages build this list, and they must build the same one: if the
/// preflight considers a credential the enrollment transaction does not, a
/// link passes its checks and then reports no credential at all.
pub fn user_credential_candidates(requested: Option<&str>) -> Vec<String> {
    let mut candidates = Vec::new();
    for token in [
        requested.and_then(org::token_for_org),
        org::default_token(),
        org::active_org()
            .ok()
            .flatten()
            .and_then(|active| org::token_for_org(&active)),
    ]
    .into_iter()
    .flatten()
    {
        if !candidates.contains(&token) {
            candidates.push(token);
        }
    }
    candidates
}

/// Whether an organization check refused this particular credential.
///
/// A refusal reaches us under two codes, because two different questions can
/// ask it: the identity probe renders one as `OrgForbidden`, and the scoped
/// project listing renders its own as `Forbidden`. Both say the same thing —
/// *this* credential may not act there — which is a fact about the credential,
/// not about the request, so another credential is still worth trying.
pub(crate) fn is_org_refusal(err: &crate::error::Error) -> bool {
    matches!(
        err.cloud_code(),
        Some(CloudErrorCode::OrgForbidden | CloudErrorCode::Forbidden)
    )
}

/// Whether a failed organization probe leaves access undecided.
///
/// The identity endpoint answers 404 for several conditions — including an
/// organization that exists but has no app — and the probe renders all of them
/// as [`CloudErrorCode::OrgNotFound`]. None of them prove the credential may
/// not act on the organization, so none of them should decide it here. A
/// refusal arrives as `OrgForbidden`, which is conclusive and is not tolerated.
fn org_probe_is_inconclusive(err: &crate::error::Error) -> bool {
    matches!(
        err.cloud_code(),
        Some(CloudErrorCode::OrgNotFound | CloudErrorCode::NotFound)
    )
}

/// Confirm this credential may act on `org`, or say why not.
///
/// The identity endpoint answers 404 for conditions that say nothing about
/// access — an organization with no app reads the same as one that does not
/// exist — so that answer decides nothing here. It is also not proof of
/// access: callers store a credential under the organization this confirms,
/// and filing one under an organization it cannot act on makes every later
/// command fail obscurely.
///
/// So an inconclusive answer is followed by a question whose answer cannot be
/// ambiguous: list the organization's projects. That is a read the server
/// refuses for a non-member, so success requires membership and nothing else
/// is inferred.
pub async fn confirm_org_access(client: &CloudClient, org: &str) -> Result<()> {
    match client.get_auth_context_for_org(org).await {
        Ok(_) => Ok(()),
        Err(err) if org_probe_is_inconclusive(&err) => {
            tracing::debug!(
                "Spice Cloud did not describe this credential's access to organization '{org}' ({err}); confirming membership by listing that organization's projects instead"
            );
            client.scoped_to_org(org).list_projects().await?;
            Ok(())
        }
        Err(err) => Err(err),
    }
}

/// What searching the stored credentials for a user credential found.
#[derive(PartialEq, Eq)]
pub enum UserCredentialSearch {
    /// A credential Spice Cloud accepts as a user.
    Found(String),
    /// Credentials were stored, and Spice Cloud rejected every one.
    AllRejected,
    /// No credential was stored to try.
    NoneStored,
}

/// Redact the live credential carried by [`UserCredentialSearch::Found`].
impl std::fmt::Debug for UserCredentialSearch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Found(_) => f.write_str("Found(<redacted>)"),
            Self::AllRejected => f.write_str("AllRejected"),
            Self::NoneStored => f.write_str("NoneStored"),
        }
    }
}

/// The first credential in `candidates` that can act as a user.
///
/// Both Cloud Connect entry points — the `spice cloud link` preflight and the
/// enrollment transaction — choose a credential this way, and they must agree:
/// one accepting a credential the other rejects strands a link half-done.
///
/// A credential the identity endpoint *rejects* cannot provide a user identity,
/// so it is skipped. One the endpoint merely cannot *describe* is unknown
/// rather than unusable, and is kept as a fallback behind any credential that
/// does describe a user.
pub async fn first_user_credential(
    candidates: &[String],
    endpoint: &str,
    org: Option<&str>,
) -> Result<UserCredentialSearch> {
    first_user_credential_with_probe(candidates, endpoint, org, identity_probe).await
}

type IdentityProbeFuture<'a> = Pin<Box<dyn Future<Output = Result<AuthContext>> + Send + 'a>>;
type IdentityProbe = for<'a> fn(&'a CloudClient) -> IdentityProbeFuture<'a>;

fn identity_probe(client: &CloudClient) -> IdentityProbeFuture<'_> {
    Box::pin(client.get_auth_context())
}

async fn first_user_credential_with_probe(
    candidates: &[String],
    endpoint: &str,
    org: Option<&str>,
    probe: IdentityProbe,
) -> Result<UserCredentialSearch> {
    // A credential Spice Cloud describes wins, but only among those that may
    // actually act on the organization: a refusal is a fact about one
    // credential, so it disqualifies that candidate rather than the search.
    let mut fallback: Option<&String> = None;
    let mut refusal: Option<crate::error::Error> = None;
    let mut rejected = 0;

    for token in candidates {
        let client = CloudClient::with_token_for_org_at(token.clone(), None, endpoint)?;

        let described = match probe(&client).await {
            Ok(_) => true,
            Err(err) => match classify_identity_failure(&err) {
                IdentityFailure::Rejected => {
                    rejected += 1;
                    continue;
                }
                IdentityFailure::Fatal => return Err(err),
                IdentityFailure::Undescribed => {
                    tracing::debug!(
                        "Spice Cloud did not describe the identity behind a stored credential ({err}); considering it a fallback"
                    );
                    false
                }
            },
        };

        if let Some(org) = org
            && let Err(err) = confirm_org_access(&client, org).await
        {
            if !is_org_refusal(&err) {
                return Err(err);
            }
            tracing::debug!(
                "A stored credential may not act on organization '{org}' ({err}); trying the next one"
            );
            refusal = Some(err);
            continue;
        }

        if described {
            return Ok(UserCredentialSearch::Found(token.clone()));
        }
        fallback.get_or_insert(token);
    }

    if let Some(token) = fallback {
        return Ok(UserCredentialSearch::Found(token.clone()));
    }

    // Nothing was usable. A refusal explains that far better than the caller's
    // "no user login" fallback message, so surface it.
    if let Some(err) = refusal {
        return Err(err);
    }

    Ok(exhausted_user_credential_search(rejected))
}

fn exhausted_user_credential_search(rejected: usize) -> UserCredentialSearch {
    if rejected == 0 {
        UserCredentialSearch::NoneStored
    } else {
        UserCredentialSearch::AllRejected
    }
}

/// The auth-context endpoint did not describe a user for this credential.
///
/// Two answers mean the same thing to a caller that only wants the identity:
/// the endpoint rejected the credential (401), or it has no user record to
/// return for it (404). Service-account tokens are valid for the management
/// API but have no user identity, so both are "absent" rather than fatal —
/// a caller that needs the identity says so with its own error.
pub fn is_absent_user_identity_error(err: &crate::error::Error) -> bool {
    matches!(
        err.cloud_code(),
        Some(CloudErrorCode::TokenExpired | CloudErrorCode::NotFound)
    )
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

    fn managed_placement(region: &str) -> ManagedProjectPlacement {
        ManagedProjectPlacement {
            region: region.to_string(),
            kind: ProjectKind::Set,
            replicas: None,
            cpu: None,
            memory: None,
            storage_size_gb: None,
            executor_replicas: None,
            executor_cpu: None,
            executor_memory: None,
        }
    }

    #[test]
    fn create_project_request_sends_region_as_cname() {
        let request = build_create_project_request(
            "app",
            None,
            "private",
            CreateProjectPlacement::Managed(managed_placement("us-east-1-prod-aws-data")),
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

    /// Spice Cloud reads "no region source at all" as the request for a Cloud
    /// Connect project, so the absence of every one of `cname`, `cluster_name`
    /// and `region` is the wire contract — not an incidental empty field.
    #[test]
    fn standalone_create_project_request_names_no_region_source() {
        let request = build_create_project_request(
            "app",
            Some("analytics"),
            "private",
            CreateProjectPlacement::Standalone,
        );

        let value = serde_json::to_value(request).expect("create app request should serialize");

        assert_eq!(
            value,
            serde_json::json!({
                "name": "app",
                "description": "analytics",
                "visibility": "private"
            })
        );
    }

    fn test_app(id: i64, name: &str, org: &str) -> Project {
        Project {
            id,
            name: name.to_string(),
            org: org.to_string(),
            kind: None,
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
    fn legacy_portal_origins_map_to_their_cloud_api_origins() {
        for (portal, api) in [
            ("https://spice.ai", "https://api.spice.ai"),
            ("https://dev.spice.ai/", "https://dev-api.spice.ai"),
            (
                "https://cloud.internal.example/base/",
                "https://cloud.internal.example/base",
            ),
        ] {
            assert_eq!(api_base_url_from_legacy_portal(portal), api);
        }
    }

    #[test]
    fn portal_origin_is_derived_from_the_authoritative_api_origin() {
        for (api, portal) in [
            ("https://api.spice.ai", "https://spice.ai"),
            ("https://dev-api.spice.ai", "https://dev.spice.ai"),
            (
                "https://cloud.internal.example",
                "https://cloud.internal.example",
            ),
            (
                "https://cloud.internal.example/base/",
                "https://cloud.internal.example/base",
            ),
        ] {
            assert_eq!(portal_base_url_from_api(api), portal);
        }
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
    fn resolve_project_id_uses_the_listing_org_when_the_payload_omits_it() {
        // `/v1/apps` does not populate `org`, so the org the listing was
        // requested for is the only evidence of which org it describes.
        let apps = vec![test_app(7, "dashboard", "")];

        let id = resolve_project_id(
            &apps,
            &target(Some("analytics"), "dashboard"),
            Some("analytics"),
        )
        .expect("should match via the listing org");

        assert_eq!(id, 7);
    }

    #[test]
    fn the_listing_org_is_the_org_the_client_acted_on() {
        // Regression for the cross-org mis-attribution this replaced: Spice
        // Cloud scopes `/v1/apps` to the requested org but returns no `org` on
        // any row, so preferring the credential's org labelled another
        // organization's projects with the caller's own — and every name
        // resolved from that listing then failed, or hit a same-named project
        // in the wrong org.
        assert_eq!(
            resolve_listing_org(Some("spiceai"), Some("lukekim")),
            Some("spiceai")
        );
    }

    #[test]
    fn the_listing_org_falls_back_to_the_credential_when_no_org_was_named() {
        assert_eq!(resolve_listing_org(None, Some("lukekim")), Some("lukekim"));
        assert_eq!(resolve_listing_org(None, None), None);
        // A service-account credential reports no org rather than an empty one.
        assert_eq!(resolve_listing_org(None, Some("")), None);
    }

    #[test]
    fn a_project_in_a_named_org_resolves_from_that_orgs_listing() {
        // End of the same regression, one layer up: the listing Spice Cloud
        // returned for 'spiceai' carries no org, and resolving 'spiceai/docs'
        // against it must find the project rather than report it as living
        // somewhere else.
        let apps = vec![test_app(680, "docs", "")];
        let listing_org = resolve_listing_org(Some("spiceai"), Some("lukekim"));

        let id = resolve_project_id(&apps, &target(Some("spiceai"), "docs"), listing_org)
            .expect("a project in the org the listing was fetched for should resolve");

        assert_eq!(id, 680);
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
    fn a_named_org_without_a_credential_fails_closed() {
        let err = org_credential_missing("spicehq");

        assert_eq!(err.cloud_code(), Some(CloudErrorCode::OrgCredentialMissing));
        let rendered = err.to_string();
        assert!(
            rendered.contains("'spicehq'") && rendered.contains("login token --org spicehq"),
            "the error must name the org and how to authenticate for it: {rendered}"
        );
    }

    /// A client that names no org, as `spice cloud project get <name>` builds.
    fn client_without_an_org() -> CloudClient {
        CloudClient::with_token_for_org_at("token", None, "https://api.spice.ai")
            .expect("cloud client should build")
    }

    #[test]
    fn a_payload_is_attributed_to_the_org_that_resolved_it() {
        // Regression for the default path: with no `--org` and no active org,
        // both the target and the client name no org, and the org the
        // resolution had already learned from the identity was discarded — so
        // `project get <name>` printed a bare name and serialized `"org": ""`
        // for a project `spice cloud projects`, on the same credential, had
        // just listed as `<org>/<name>`.
        let attributed =
            client_without_an_org().attribute(test_app(812, "docs", ""), Some("lukekim"));

        assert_eq!(attributed.org, "lukekim");
        assert_eq!(attributed.full_name(), "lukekim/docs");
    }

    #[test]
    fn attribution_never_overrides_an_org_the_payload_carries() {
        let attributed =
            client_without_an_org().attribute(test_app(1, "docs", "spiceai"), Some("lukekim"));

        assert_eq!(attributed.org, "spiceai");
    }

    #[test]
    fn attribution_is_absent_rather_than_guessed_when_no_org_is_known() {
        // A service-account credential has no user identity, so nothing can
        // name an org — a bare name is the honest answer, not a made-up one.
        let attributed = client_without_an_org().attribute(test_app(1, "docs", ""), None);

        assert_eq!(attributed.org, "");
        assert_eq!(attributed.full_name(), "docs");
    }

    #[test]
    fn unauthorized_maps_to_token_expired() {
        let err = map_cloud_error(None)(spice_cloud_client::error::Error::Unauthorized {
            message: "invalid or expired token".to_string(),
        });

        assert_eq!(err.cloud_code(), Some(CloudErrorCode::TokenExpired));
        assert!(is_absent_user_identity_error(&err));
    }

    /// The organization probe's own rendering decides what
    /// [`org_probe_is_inconclusive`] must accept. Asserting the rule against a
    /// hand-built error would pass while the two disagreed, which is exactly
    /// how tolerating `NotFound` alone came to be a no-op: the probe renders
    /// that response as `OrgNotFound`.
    #[test]
    fn the_org_probe_renders_a_missing_answer_as_something_the_rule_tolerates() {
        let client = CloudClient::new_unauthenticated().expect("client should build");

        let undescribed = client.map_org_probe_error(
            "acme",
            spice_cloud_client::error::Error::NotFound {
                message: "{}".to_string(),
            },
        );
        assert_eq!(undescribed.cloud_code(), Some(CloudErrorCode::OrgNotFound));
        assert!(
            org_probe_is_inconclusive(&undescribed),
            "an undescribed organization must not decide access: {undescribed}"
        );

        let refused = client.map_org_probe_error(
            "acme",
            spice_cloud_client::error::Error::Forbidden {
                message: "not a member".to_string(),
            },
        );
        assert_eq!(refused.cloud_code(), Some(CloudErrorCode::OrgForbidden));
        assert!(
            !org_probe_is_inconclusive(&refused),
            "a refusal is conclusive and must be surfaced: {refused}"
        );
    }

    /// A refusal disqualifies one credential, not the search — and it arrives
    /// under two codes, since the identity probe and the scoped listing render
    /// theirs differently. Missing either would abort the search on a
    /// credential that was merely the wrong one to try.
    #[test]
    fn both_renderings_of_a_refusal_disqualify_only_that_credential() {
        let from_identity_probe = Error::cloud(CloudErrorCode::OrgForbidden, "not a member");
        let from_scoped_listing = Error::cloud(CloudErrorCode::Forbidden, "not permitted");
        let not_a_refusal = Error::cloud(CloudErrorCode::ApiError, "boom");

        assert!(is_org_refusal(&from_identity_probe));
        assert!(is_org_refusal(&from_scoped_listing));
        assert!(!is_org_refusal(&not_a_refusal));
    }

    /// 401 is the one answer that disqualifies a credential; 404 leaves it
    /// unknown, and anything else is a failure the caller must see.
    #[test]
    fn identity_failures_are_classified_by_what_they_prove() {
        let rejected = map_cloud_error(None)(spice_cloud_client::error::Error::Unauthorized {
            message: "invalid or expired token".to_string(),
        });
        let undescribed = map_cloud_error(None)(spice_cloud_client::error::Error::NotFound {
            message: "{}".to_string(),
        });
        let forbidden = map_cloud_error(None)(spice_cloud_client::error::Error::Forbidden {
            message: "missing scope".to_string(),
        });
        let server_error = map_cloud_error(None)(spice_cloud_client::error::Error::Api {
            status: 500,
            message: "boom".to_string(),
        });

        assert_eq!(
            classify_identity_failure(&rejected),
            IdentityFailure::Rejected
        );
        assert_eq!(
            classify_identity_failure(&undescribed),
            IdentityFailure::Undescribed,
            "a credential Spice Cloud cannot describe must stay usable"
        );
        assert_eq!(
            classify_identity_failure(&forbidden),
            IdentityFailure::Fatal
        );
        assert_eq!(
            classify_identity_failure(&server_error),
            IdentityFailure::Fatal
        );
    }

    /// Spice Cloud answers the identity endpoint with 404 for credentials it
    /// cannot describe. Treating that as fatal blocks `spice cloud link` and
    /// `spice cloud whoami` behind an identity neither of them requires.
    #[test]
    fn not_found_is_an_absent_identity_rather_than_a_failure() {
        let err = map_cloud_error(None)(spice_cloud_client::error::Error::NotFound {
            message: "{}".to_string(),
        });

        assert_eq!(err.cloud_code(), Some(CloudErrorCode::NotFound));
        assert!(is_absent_user_identity_error(&err));
    }

    /// Regression test for #13487: no candidates and candidates rejected with
    /// 401 are distinct terminal states, so callers cannot render both as a
    /// missing login.
    #[test]
    fn rejected_candidates_are_not_reported_as_none_stored() {
        let rejected = map_cloud_error(None)(spice_cloud_client::error::Error::Unauthorized {
            message: "invalid or expired token".to_string(),
        });
        assert_eq!(
            classify_identity_failure(&rejected),
            IdentityFailure::Rejected
        );

        let no_candidates = exhausted_user_credential_search(0);
        let one_rejected = exhausted_user_credential_search(1);
        assert_eq!(no_candidates, UserCredentialSearch::NoneStored);
        assert_eq!(one_rejected, UserCredentialSearch::AllRejected);
    }

    #[tokio::test]
    async fn an_empty_candidate_list_reports_none_stored_without_a_request() {
        let outcome = first_user_credential(&[], "https://cloud.invalid", None)
            .await
            .expect("an empty candidate list needs no request");

        assert_eq!(outcome, UserCredentialSearch::NoneStored);
    }

    /// Regression test for #13487: a stored credential rejected with 401 must
    /// reach callers as rejected, not as an absent login.
    #[tokio::test]
    async fn a_rejected_stored_credential_reports_all_rejected() {
        fn rejected_identity_probe(_: &CloudClient) -> IdentityProbeFuture<'_> {
            Box::pin(async {
                Err(map_cloud_error(None)(
                    spice_cloud_client::error::Error::Unauthorized {
                        message: "invalid or expired token".to_string(),
                    },
                ))
            })
        }

        let outcome = first_user_credential_with_probe(
            &["rejected-token".to_string()],
            "https://cloud.invalid",
            None,
            rejected_identity_probe,
        )
        .await
        .expect("a rejected credential is a terminal search outcome");

        assert_eq!(outcome, UserCredentialSearch::AllRejected);
    }
}
