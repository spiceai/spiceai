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
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "stable" => Ok(Self::Stable),
            "preview" => Ok(Self::Preview),
            "nightly" => Ok(Self::Nightly),
            "internal" => Ok(Self::Internal),
            _ => Err(format!(
                "invalid channel '{s}'. Expected one of: stable, preview, nightly, internal"
            )),
        }
    }
}

/// App kind — determines whether the app is a `SpicepodSet` or `SpicepodCluster`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AppKind {
    /// Standard scheduler-only deployment.
    Set,
    /// Distributed deployment with separate scheduler and executor pods.
    Cluster,
}

impl std::fmt::Display for AppKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Set => write!(f, "set"),
            Self::Cluster => write!(f, "cluster"),
        }
    }
}

impl std::str::FromStr for AppKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "set" | "spicepodset" => Ok(Self::Set),
            "cluster" | "spicepodcluster" => Ok(Self::Cluster),
            _ => Err(format!("invalid kind '{s}'. Expected one of: set, cluster")),
        }
    }
}

// ============================================================================
// Apps
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct App {
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
    pub config: Option<AppConfig>,
}

impl App {
    #[must_use]
    pub fn full_name(&self) -> String {
        format!("{}/{}", self.org, self.name)
    }
}

#[derive(Debug, Deserialize)]
pub struct AppsResponse {
    pub apps: Vec<App>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AppExecutor {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replicas: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<AppResources>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_size_gb: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AppConfig {
    pub spicepod: Option<serde_json::Value>,
    pub registry: Option<String>,
    pub image: Option<String>,
    pub image_tag: Option<String>,
    pub update_channel: Option<UpdateChannel>,
    pub replicas: Option<i32>,
    pub resources: Option<AppResources>,
    pub executor: Option<AppExecutor>,
    pub region: Option<String>,
    pub node_group: Option<String>,
    pub storage_size_gb: Option<f64>,
    /// Deprecated: Use `storage_size_gb` instead.
    pub storage_claim_size_gb: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AppResources {
    pub limits: AppResourceLimits,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requests: Option<AppResourceRequests>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AppResourceLimits {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory: Option<String>,
    #[serde(rename = "ephemeral-storage", skip_serializing_if = "Option::is_none")]
    pub ephemeral_storage: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AppResourceRequests {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct CreateAppRequest {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub visibility: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cname: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<BTreeMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replicas: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<AppResources>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub executor: Option<AppExecutor>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_size_gb: Option<f64>,
}

#[derive(Debug, Serialize, Default)]
pub struct UpdateAppRequest {
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
    pub resources: Option<AppResources>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub executor: Option<AppExecutor>,
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
    pub created_by: Option<String>,
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

#[derive(Debug, Serialize, Deserialize)]
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
#[derive(Debug, Deserialize)]
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

#[derive(Debug, Deserialize)]
pub struct AuthContextOrg {
    pub name: Option<String>,
}

#[derive(Debug, Deserialize)]
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
