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
    pub api_key: Option<String>,
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
}

#[derive(Debug, Serialize)]
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
    pub region: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spicepod: Option<String>,
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

#[derive(Debug, Deserialize)]
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

#[derive(Debug, Deserialize)]
pub struct LogsResponse {
    pub logs: Vec<LogEntry>,
}

// ============================================================================
// API keys
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct ApiKeysResponse {
    pub api_key: Option<String>,
    pub api_key_2: Option<String>,
}

#[derive(Debug, Deserialize)]
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
// Rollback
// ============================================================================

#[derive(Debug, Serialize)]
pub struct RollbackRequest {
    pub target_deployment_id: i64,
}

// ============================================================================
// Auth
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct AuthExchangeResponse {
    pub access_token: Option<String>,
    pub access_denied: bool,
}

#[derive(Debug, Deserialize)]
pub struct AuthContext {
    pub username: String,
    pub email: String,
    pub org_name: String,
    pub app_name: Option<String>,
    pub app_api_key: Option<String>,
}
