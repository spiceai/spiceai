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

//! Request and response types for the Spice Cloud API.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use snafu::Snafu;

// ============================================================================
// Common enums
// ============================================================================

/// Runtime update channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UpdateChannel {
    Stable,
    Preview,
    Nightly,
    Internal,
}

#[derive(Debug, Snafu, PartialEq, Eq)]
pub enum ParseCloudEnumError {
    #[snafu(display(
        "invalid channel '{input}'. Expected one of: stable, preview, nightly, internal"
    ))]
    InvalidUpdateChannel { input: String },
    #[snafu(display("invalid kind '{input}'. Expected one of: set, cluster"))]
    InvalidProjectKind { input: String },
}

impl std::fmt::Display for UpdateChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stable => write!(f, "stable"),
            Self::Preview => write!(f, "preview"),
            Self::Nightly => write!(f, "nightly"),
            Self::Internal => write!(f, "internal"),
        }
    }
}

impl std::str::FromStr for UpdateChannel {
    type Err = ParseCloudEnumError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "stable" => Ok(Self::Stable),
            "preview" => Ok(Self::Preview),
            "nightly" => Ok(Self::Nightly),
            "internal" => Ok(Self::Internal),
            _ => Err(ParseCloudEnumError::InvalidUpdateChannel {
                input: s.to_string(),
            }),
        }
    }
}

/// Project kind — determines whether the project is a `SpicepodSet` or `SpicepodCluster`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProjectKind {
    /// Standard scheduler-only deployment.
    Set,
    /// Distributed deployment with separate scheduler and executor pods.
    Cluster,
}

impl std::fmt::Display for ProjectKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Set => write!(f, "set"),
            Self::Cluster => write!(f, "cluster"),
        }
    }
}

impl std::str::FromStr for ProjectKind {
    type Err = ParseCloudEnumError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "set" | "spicepodset" => Ok(Self::Set),
            "cluster" | "spicepodcluster" => Ok(Self::Cluster),
            _ => Err(ParseCloudEnumError::InvalidProjectKind {
                input: s.to_string(),
            }),
        }
    }
}

// ============================================================================
// Organizations
// ============================================================================

/// An organization the authenticated identity can act on.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Org {
    #[serde(default)]
    pub id: Option<i64>,
    pub name: String,
    #[serde(default, alias = "displayName", alias = "display_name")]
    pub display_name: Option<String>,
    /// Membership role (`owner`, `admin`, `member`, ...) when the API reports one.
    #[serde(default)]
    pub role: Option<String>,
}

/// Wire format for org listings. The endpoint may return either a bare array or
/// an `{"orgs": [...]}` envelope, so accept both.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum OrgsResponse {
    Wrapped { orgs: Vec<Org> },
    Bare(Vec<Org>),
}

impl OrgsResponse {
    #[must_use]
    pub fn into_orgs(self) -> Vec<Org> {
        match self {
            Self::Wrapped { orgs } | Self::Bare(orgs) => orgs,
        }
    }
}

// ============================================================================
// Projects
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Project {
    pub id: i64,
    pub name: String,
    #[serde(default)]
    pub org: String,
    pub description: Option<String>,
    pub visibility: Option<String>,
    pub created_at: Option<String>,
    pub region: Option<String>,
    pub production_branch: Option<String>,
    #[serde(default)]
    pub config: Option<ProjectConfig>,
}

impl Project {
    #[must_use]
    pub fn full_name(&self) -> String {
        if self.org.is_empty() {
            self.name.clone()
        } else {
            format!("{}/{}", self.org, self.name)
        }
    }
}

/// Wire format for a project listing.
///
/// Spice Cloud renamed apps to projects and serves both spellings: the
/// canonical `/v1/projects` answers with a `projects` envelope, while the
/// preserved `/v1/apps` alias keeps its original `apps` envelope. Accept
/// either so the CLI reads the same on both paths and across the rename.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum ProjectsResponse {
    Projects { projects: Vec<Project> },
    Apps { apps: Vec<Project> },
}

impl ProjectsResponse {
    #[must_use]
    pub fn into_projects(self) -> Vec<Project> {
        match self {
            Self::Projects { projects } | Self::Apps { apps: projects } => projects,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProjectExecutor {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replicas: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<ProjectResources>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_size_gb: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProjectConfig {
    pub spicepod: Option<serde_json::Value>,
    pub registry: Option<String>,
    pub image: Option<String>,
    pub image_tag: Option<String>,
    pub update_channel: Option<String>,
    pub replicas: Option<i32>,
    pub resources: Option<ProjectResources>,
    pub executor: Option<ProjectExecutor>,
    pub region: Option<String>,
    pub node_group: Option<String>,
    pub storage_size_gb: Option<f64>,
    /// Deprecated: Use `storage_size_gb` instead.
    pub storage_claim_size_gb: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProjectResources {
    pub limits: ProjectResourceLimits,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requests: Option<ProjectResourceRequests>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProjectResourceLimits {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu: Option<String>,
    /// Omitted when the caller leaves memory unspecified; Cloud API treats the missing field as
    /// unset/default rather than as a request for a synthetic client-side default.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory: Option<String>,
    #[serde(rename = "ephemeral-storage", skip_serializing_if = "Option::is_none")]
    pub ephemeral_storage: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProjectResourceRequests {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct CreateProjectRequest {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub visibility: String,
    /// Deprecated region source. Mutually exclusive with [`Self::cluster_name`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cname: Option<String>,
    /// Dedicated-cluster / nodegroup assignment (from `GET /v1/clusters`).
    /// Mutually exclusive with [`Self::cname`]; when set, cloud injects the
    /// `organization` / `_cluster` scheduling tags from the nodegroup row.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<BTreeMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replicas: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<ProjectResources>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub executor: Option<ProjectExecutor>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_size_gb: Option<f64>,
}

#[derive(Debug, Serialize, Default)]
pub struct UpdateProjectRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub visibility: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replicas: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_tag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update_channel: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spicepod: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<ProjectResources>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub executor: Option<ProjectExecutor>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_size_gb: Option<f64>,
}

// ============================================================================
// Deployments
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Deployment {
    pub id: i64,
    #[serde(default)]
    pub status: String,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub image: Option<String>,
    pub image_tag: Option<String>,
    pub replicas: Option<i32>,
    pub branch: Option<String>,
    pub commit_sha: Option<String>,
    pub commit_message: Option<String>,
    pub error_message: Option<String>,
    pub creation_source: Option<String>,
    pub created_by: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct DeploymentsResponse {
    pub deployments: Vec<Deployment>,
}

#[derive(Debug, Serialize)]
pub struct CreateDeploymentRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_tag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replicas: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit_sha: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    pub debug: bool,
}

// ============================================================================
// Regions
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Region {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub region: String,
    #[serde(default)]
    pub provider: String,
    #[serde(rename = "providerName")]
    pub provider_name: Option<String>,
    #[serde(default)]
    pub cname: Option<String>,
    #[serde(rename = "isDefault", default)]
    pub is_default: bool,
    #[serde(default)]
    pub disabled: bool,
}

#[derive(Debug, Deserialize)]
pub struct RegionsResponse {
    pub regions: Vec<Region>,
    pub default: Option<String>,
}

// ============================================================================
// Container images
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerImage {
    pub name: Option<String>,
    pub tag: String,
    pub channel: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ContainerImagesResponse {
    pub images: Vec<ContainerImage>,
    pub default: Option<String>,
}

// ============================================================================
// Secrets
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Secret {
    pub id: Option<i64>,
    pub name: String,
    pub value: Option<String>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SecretsResponse {
    pub secrets: Vec<Secret>,
}

#[derive(Debug, Serialize)]
pub struct SetSecretRequest {
    pub name: String,
    pub value: String,
}

// ============================================================================
// Logs
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub timestamp: Option<String>,
    pub level: Option<String>,
    pub message: String,
    pub source: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LogsResponse {
    pub logs: Vec<LogEntry>,
}

// ============================================================================
// API keys
// ============================================================================

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiKeysResponse {
    pub api_key: Option<String>,
    pub api_key_2: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RegenerateApiKeyResponse {
    pub api_key: Option<String>,
    pub api_key_2: Option<String>,
    pub regenerated_key: Option<u8>,
}

#[derive(Debug, Serialize)]
pub struct RegenerateApiKeyRequest {
    pub key_number: u8,
}

// ============================================================================
// Standalone instance adoption codes
// ============================================================================

/// Request body for `POST /v1/instance-adoption-codes`, the management-API
/// mint a logged-in `spice connect` uses instead of making the customer copy a
/// code out of the portal.
#[derive(Debug, Default, Serialize)]
pub struct MintAdoptionCodeRequest {
    /// Display label for the adoption-codes screen — who minted this and why.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// Lifetime in seconds. Omitted to take the endpoint's short default,
    /// which is what a mint-and-redeem-immediately caller wants: a code that
    /// outlives its own enroll is a live org credential nobody is holding.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl_seconds: Option<u32>,
    /// The org the caller believes it is minting into. An **assertion, not a
    /// selection** — the org always comes from the token, and a mismatch is a
    /// not-found. Without it, a token bound to org A plus `--org B` would mint
    /// quietly into A.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub org: Option<String>,
}

/// Response from `POST /v1/instance-adoption-codes`. The plaintext code is
/// returned exactly once, at mint.
#[derive(Debug, Deserialize)]
pub struct MintAdoptionCodeResponse {
    /// The plaintext adoption code. Never logged, never written to disk.
    pub code: String,
    /// The org the code is scoped to, as the cloud resolved it from the token.
    #[serde(default)]
    pub org: Option<String>,
    /// RFC 3339 expiry, for an error message that can say how long the code
    /// was good for.
    #[serde(default)]
    pub expires_at: Option<String>,
}

// ============================================================================
// Metrics
// ============================================================================

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PodMetrics {
    pub cpu_usage_percent: Option<f64>,
    pub memory_usage_bytes: Option<u64>,
    pub disk_read_bytes: Option<f64>,
    pub disk_read_operations: Option<f64>,
    pub disk_write_bytes: Option<f64>,
    pub disk_write_operations: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IngestionMetrics {
    pub rows_ingested: Option<u64>,
    pub bytes_ingested: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClusterMetrics {
    pub active_executors_count: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MetricsResponse {
    pub metrics: BTreeMap<String, PodMetrics>,
    #[serde(default)]
    pub ingestion: Option<IngestionMetrics>,
    #[serde(default)]
    pub cluster: Option<ClusterMetrics>,
}

// ============================================================================
// Auth
// ============================================================================

// Debug is intentionally not derived: access_token must not appear in logs or error output.
#[derive(Deserialize)]
pub struct AuthExchangeResponse {
    pub access_token: Option<String>,
    #[serde(default)]
    pub access_denied: bool,
}

// Debug is intentionally not derived: the device auth `code` is short-lived but
// exchangeable for an access token, so treat it like a secret.
#[derive(Serialize)]
pub struct AuthExchangeRequest<'a> {
    pub code: &'a str,
}

// Debug is intentionally not derived: client_secret must not appear in logs or error output.
#[derive(Serialize)]
pub struct OAuthTokenRequest<'a> {
    pub client_id: &'a str,
    pub client_secret: &'a str,
    pub grant_type: &'static str,
}

// Debug is intentionally not derived: access_token must not appear in logs or error output.
#[derive(Deserialize)]
pub struct OAuthTokenResponse {
    pub access_token: String,
    pub token_type: String,
}

// Debug is intentionally not derived: app_api_key must not appear in logs or error output.
#[derive(Serialize, Deserialize)]
pub struct AuthContext {
    pub username: String,
    pub email: String,
    pub org_name: String,
    pub app_name: Option<String>,
    pub app_api_key: Option<String>,
}

/// Wire format for the Spice Cloud auth context endpoint, which returns
/// `org` and `app` as nested objects. Flattened into [`AuthContext`] for the
/// rest of the CLI.
#[derive(Deserialize)]
pub struct AuthContextRaw {
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub email: String,
    #[serde(default)]
    pub org: Option<AuthContextOrg>,
    #[serde(default)]
    pub app: Option<AuthContextApp>,
}

#[derive(Deserialize)]
pub struct AuthContextOrg {
    pub name: Option<String>,
}

#[derive(Deserialize)]
pub struct AuthContextApp {
    pub name: Option<String>,
    pub api_key: Option<String>,
}

impl From<AuthContextRaw> for AuthContext {
    fn from(raw: AuthContextRaw) -> Self {
        let org_name = raw.org.and_then(|o| o.name).unwrap_or_default();
        let (app_name, app_api_key) = match raw.app {
            Some(app) => (app.name, app.api_key),
            None => (None, None),
        };
        Self {
            username: raw.username,
            email: raw.email,
            org_name,
            app_name,
            app_api_key,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_channel_parse_error_is_typed() {
        let error = "beta"
            .parse::<UpdateChannel>()
            .expect_err("invalid channel should fail");

        assert!(matches!(
            error,
            ParseCloudEnumError::InvalidUpdateChannel { .. }
        ));
    }

    #[test]
    fn app_kind_parse_error_is_typed() {
        let error = "worker"
            .parse::<ProjectKind>()
            .expect_err("invalid app kind should fail");

        assert!(matches!(
            error,
            ParseCloudEnumError::InvalidProjectKind { .. }
        ));
    }

    #[test]
    fn resource_limits_omit_unspecified_memory() {
        let limits = ProjectResourceLimits {
            cpu: Some("2".to_string()),
            memory: None,
            ephemeral_storage: None,
        };

        let value = serde_json::to_value(limits).expect("limits should serialize");
        assert_eq!(value, serde_json::json!({ "cpu": "2" }));
    }

    #[test]
    fn project_listings_parse_on_both_sides_of_the_rename() {
        // Spice Cloud serves `/v1/projects` with a `projects` envelope and
        // keeps `/v1/apps` with its original `apps` envelope for existing
        // clients. The CLI must read the same from either, so switching paths
        // later is a one-line change and never a parse failure in the field.
        let projects: ProjectsResponse =
            serde_json::from_str(r#"{"projects":[{"id":1,"name":"team-app","org":"spicehq"}]}"#)
                .expect("projects envelope should deserialize");
        assert_eq!(projects.into_projects()[0].name, "team-app");

        let apps: ProjectsResponse =
            serde_json::from_str(r#"{"apps":[{"id":1,"name":"team-app","org":"spicehq"}]}"#)
                .expect("legacy apps envelope should deserialize");
        assert_eq!(apps.into_projects()[0].name, "team-app");
    }

    #[test]
    fn orgs_response_accepts_wrapped_and_bare_payloads() {
        let wrapped: OrgsResponse = serde_json::from_str(
            r#"{"orgs":[{"id":1,"name":"spicehq","displayName":"Spice HQ","role":"owner"}]}"#,
        )
        .expect("wrapped org listing should deserialize");
        let orgs = wrapped.into_orgs();
        assert_eq!(orgs.len(), 1);
        assert_eq!(orgs[0].name, "spicehq");
        assert_eq!(orgs[0].display_name.as_deref(), Some("Spice HQ"));
        assert_eq!(orgs[0].role.as_deref(), Some("owner"));

        let bare: OrgsResponse = serde_json::from_str(r#"[{"name":"lukekim"}]"#)
            .expect("bare org listing should deserialize");
        let orgs = bare.into_orgs();
        assert_eq!(orgs.len(), 1);
        assert_eq!(orgs[0].name, "lukekim");
        assert!(orgs[0].id.is_none());
    }

    #[test]
    fn app_config_preserves_unknown_update_channel() {
        let config = serde_json::from_value::<ProjectConfig>(serde_json::json!({
            "update_channel": "next"
        }))
        .expect("unknown update channels should deserialize");

        assert_eq!(config.update_channel.as_deref(), Some("next"));
    }
}
