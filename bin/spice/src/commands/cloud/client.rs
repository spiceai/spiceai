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

//! Cloud API client for Spice Cloud.

use crate::error::{HttpRequestFailedSnafu, InvalidArgumentSnafu, InvalidResponseSnafu, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::time::Duration;

const CLOUD_API_BASE_URL: &str = "https://api.spice.ai";
const DEV_CLOUD_API_BASE_URL: &str = "https://dev-api.spice.ai";

/// Cloud API client.
pub struct CloudClient {
    base_url: String,
    client: Client,
    token: Option<String>,
}

// ============================================================================
// API types
// ============================================================================

#[derive(Debug, Serialize, Deserialize)]
pub struct App {
    pub id: i64,
    pub name: String,
    pub org: String,
    pub description: Option<String>,
    pub visibility: Option<String>,
    pub created_at: Option<String>,
    pub region: Option<String>,
    pub production_branch: Option<String>,
    pub api_key: Option<String>,
}

impl App {
    pub fn full_name(&self) -> String {
        format!("{}/{}", self.org, self.name)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AppsResponse {
    pub apps: Vec<App>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Deployment {
    pub id: i64,
    pub status: String,
    pub created_at: Option<String>,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub image_tag: Option<String>,
    pub replicas: Option<i32>,
    pub commit_sha: Option<String>,
    pub commit_message: Option<String>,
    pub error_message: Option<String>,
    pub creation_source: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeploymentsResponse {
    pub deployments: Vec<Deployment>,
}

#[derive(Debug, Serialize, Deserialize)]
#[expect(
    clippy::struct_field_names,
    reason = "API contract requires 'region' field"
)]
pub struct Region {
    pub name: String,
    pub region: String,
    pub provider: String,
    #[serde(rename = "providerName")]
    pub provider_name: Option<String>,
    #[serde(rename = "isDefault")]
    pub is_default: bool,
    pub disabled: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RegionsResponse {
    pub regions: Vec<Region>,
    pub default: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Debug, Serialize, Deserialize)]
pub struct Secret {
    pub id: Option<i64>,
    pub name: String,
    pub value: Option<String>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SecretsResponse {
    pub secrets: Vec<Secret>,
}

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Debug, Serialize, Deserialize)]
pub struct AuthExchangeResponse {
    pub access_token: Option<String>,
    pub access_denied: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AuthContext {
    pub username: String,
    pub email: String,
    pub org_name: String,
    pub app_name: Option<String>,
    pub app_api_key: Option<String>,
}

// ============================================================================
// Request types
// ============================================================================

#[derive(Debug, Serialize)]
struct CreateAppRequest {
    name: String,
    description: Option<String>,
    visibility: String,
}

#[derive(Debug, Serialize)]
struct UpdateAppRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    visibility: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    replicas: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    image_tag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    region: Option<String>,
}

#[derive(Debug, Serialize)]
struct CreateDeploymentRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    image_tag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    replicas: Option<i32>,
    debug: bool,
}

#[derive(Debug, Serialize)]
struct SetSecretRequest {
    name: String,
    value: String,
}

#[derive(Debug, Serialize)]
struct RegenerateApiKeyRequest {
    key_number: u8,
}

#[derive(Debug, Serialize)]
struct RollbackRequest {
    target_deployment_id: i64,
}

// ============================================================================
// Client implementation
// ============================================================================

impl CloudClient {
    /// Create a new authenticated cloud client.
    pub fn new() -> Result<Self> {
        let token = get_auth_token()?;
        Ok(Self {
            base_url: get_base_url(),
            client: Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .context(HttpRequestFailedSnafu)?,
            token: Some(token),
        })
    }

    /// Create a new unauthenticated cloud client (for login flow).
    ///
    /// # Panics
    /// Panics if the HTTP client cannot be created (should never happen with default config).
    pub fn new_unauthenticated() -> Self {
        Self {
            base_url: get_base_url(),
            client: Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap_or_else(|_| Client::new()),
            token: None,
        }
    }

    /// Get the auth URL for the login flow.
    pub fn get_auth_url(&self, auth_code: &str) -> String {
        format!(
            "{}/v1/auth/device?code={}",
            self.base_url.replace("api.", ""),
            auth_code
        )
    }

    /// Exchange an auth code for an access token.
    pub async fn exchange_code(&self, auth_code: &str) -> Result<Option<AuthExchangeResponse>> {
        let url = format!(
            "{}/v1/auth/device/exchange?code={}",
            self.base_url, auth_code
        );
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .context(HttpRequestFailedSnafu)?;

        if response.status() == reqwest::StatusCode::ACCEPTED {
            // Still waiting for auth
            return Ok(None);
        }

        if !response.status().is_success() {
            return Ok(None);
        }

        let body: AuthExchangeResponse = response.json().await.context(HttpRequestFailedSnafu)?;
        Ok(Some(body))
    }

    /// Get the auth context for the current user.
    pub async fn get_auth_context(&self) -> Result<AuthContext> {
        let url = format!("{}/v1/auth/context", self.base_url);
        let response = self
            .client
            .get(&url)
            .bearer_auth(self.token.as_deref().unwrap_or(""))
            .send()
            .await
            .context(HttpRequestFailedSnafu)?;

        self.handle_response(response).await
    }

    // ========================================================================
    // Apps
    // ========================================================================

    pub async fn list_apps(&self) -> Result<Vec<App>> {
        let url = format!("{}/v1/apps", self.base_url);
        let response = self
            .client
            .get(&url)
            .bearer_auth(self.token.as_deref().unwrap_or(""))
            .send()
            .await
            .context(HttpRequestFailedSnafu)?;

        let apps: AppsResponse = self.handle_response(response).await?;
        Ok(apps.apps)
    }

    pub async fn get_app(&self, org_app: &str) -> Result<App> {
        let apps = self.list_apps().await?;
        let (org, name) = parse_org_app(org_app);

        for app in apps {
            if app.name == name && (org.is_empty() || app.org == org) {
                return self.get_app_by_id(app.id).await;
            }
        }

        InvalidResponseSnafu {
            message: format!("App '{org_app}' not found"),
        }
        .fail()
    }

    pub async fn get_app_by_id(&self, app_id: i64) -> Result<App> {
        let url = format!("{}/v1/apps/{}", self.base_url, app_id);
        let response = self
            .client
            .get(&url)
            .bearer_auth(self.token.as_deref().unwrap_or(""))
            .send()
            .await
            .context(HttpRequestFailedSnafu)?;

        self.handle_response(response).await
    }

    pub async fn create_app(
        &self,
        name: &str,
        description: Option<&str>,
        visibility: &str,
    ) -> Result<App> {
        let url = format!("{}/v1/apps", self.base_url);
        let request = CreateAppRequest {
            name: name.to_string(),
            description: description.map(String::from),
            visibility: visibility.to_string(),
        };

        let response = self
            .client
            .post(&url)
            .bearer_auth(self.token.as_deref().unwrap_or(""))
            .json(&request)
            .send()
            .await
            .context(HttpRequestFailedSnafu)?;

        self.handle_response(response).await
    }

    pub async fn update_app(
        &self,
        org_app: &str,
        description: Option<&str>,
        visibility: Option<&str>,
        replicas: Option<i32>,
        image_tag: Option<&str>,
        region: Option<&str>,
    ) -> Result<App> {
        let app = self.get_app(org_app).await?;
        let url = format!("{}/v1/apps/{}", self.base_url, app.id);

        let request = UpdateAppRequest {
            description: description.map(String::from),
            visibility: visibility.map(String::from),
            replicas,
            image_tag: image_tag.map(String::from),
            region: region.map(String::from),
        };

        let response = self
            .client
            .put(&url)
            .bearer_auth(self.token.as_deref().unwrap_or(""))
            .json(&request)
            .send()
            .await
            .context(HttpRequestFailedSnafu)?;

        self.handle_response(response).await
    }

    pub async fn delete_app(&self, org_app: &str) -> Result<()> {
        let app = self.get_app(org_app).await?;
        let url = format!("{}/v1/apps/{}", self.base_url, app.id);

        let response = self
            .client
            .delete(&url)
            .bearer_auth(self.token.as_deref().unwrap_or(""))
            .send()
            .await
            .context(HttpRequestFailedSnafu)?;

        self.handle_empty_response(response).await
    }

    // ========================================================================
    // Deployments
    // ========================================================================

    pub async fn list_deployments(
        &self,
        org_app: &str,
        limit: usize,
        status: Option<&str>,
    ) -> Result<Vec<Deployment>> {
        use std::fmt::Write;

        let app = self.get_app(org_app).await?;
        let mut url = format!(
            "{}/v1/apps/{}/deployments?limit={}",
            self.base_url, app.id, limit
        );
        if let Some(s) = status {
            let _ = write!(url, "&status={s}");
        }

        let response = self
            .client
            .get(&url)
            .bearer_auth(self.token.as_deref().unwrap_or(""))
            .send()
            .await
            .context(HttpRequestFailedSnafu)?;

        let deployments: DeploymentsResponse = self.handle_response(response).await?;
        Ok(deployments.deployments)
    }

    pub async fn get_latest_deployment(&self, org_app: &str) -> Result<Deployment> {
        let deployments = self.list_deployments(org_app, 1, None).await?;
        deployments
            .into_iter()
            .next()
            .ok_or_else(|| crate::error::Error::InvalidResponse {
                message: format!("No deployments found for '{org_app}'"),
            })
    }

    pub async fn create_deployment(
        &self,
        org_app: &str,
        image_tag: Option<&str>,
        replicas: Option<i32>,
        debug: bool,
    ) -> Result<Deployment> {
        let app = self.get_app(org_app).await?;
        let url = format!("{}/v1/apps/{}/deployments", self.base_url, app.id);

        let request = CreateDeploymentRequest {
            image_tag: image_tag.map(String::from),
            replicas,
            debug,
        };

        let response = self
            .client
            .post(&url)
            .bearer_auth(self.token.as_deref().unwrap_or(""))
            .json(&request)
            .send()
            .await
            .context(HttpRequestFailedSnafu)?;

        self.handle_response(response).await
    }

    pub async fn get_deployment_logs(
        &self,
        org_app: &str,
        deployment_id: i64,
        limit: usize,
        since: Option<&str>,
    ) -> Result<LogsResponse> {
        use std::fmt::Write;

        let app = self.get_app(org_app).await?;
        let mut url = format!(
            "{}/v1/apps/{}/deployments/{}/logs?limit={}",
            self.base_url, app.id, deployment_id, limit
        );
        if let Some(s) = since {
            let _ = write!(url, "&since={s}");
        }

        let response = self
            .client
            .get(&url)
            .bearer_auth(self.token.as_deref().unwrap_or(""))
            .send()
            .await
            .context(HttpRequestFailedSnafu)?;

        self.handle_response(response).await
    }

    pub async fn rollback(&self, org_app: &str, target_deployment_id: i64) -> Result<Deployment> {
        let app = self.get_app(org_app).await?;
        let url = format!("{}/v1/apps/{}/rollback", self.base_url, app.id);

        let request = RollbackRequest {
            target_deployment_id,
        };

        let response = self
            .client
            .post(&url)
            .bearer_auth(self.token.as_deref().unwrap_or(""))
            .json(&request)
            .send()
            .await
            .context(HttpRequestFailedSnafu)?;

        self.handle_response(response).await
    }

    // ========================================================================
    // Regions & Images
    // ========================================================================

    pub async fn list_regions(&self, env: Option<&str>) -> Result<RegionsResponse> {
        use std::fmt::Write;

        let mut url = format!("{}/v1/regions", self.base_url);
        if let Some(e) = env {
            let _ = write!(url, "?env={e}");
        }

        let response = self
            .client
            .get(&url)
            .bearer_auth(self.token.as_deref().unwrap_or(""))
            .send()
            .await
            .context(HttpRequestFailedSnafu)?;

        self.handle_response(response).await
    }

    pub async fn list_container_images(
        &self,
        channel: Option<&str>,
    ) -> Result<ContainerImagesResponse> {
        use std::fmt::Write;

        let mut url = format!("{}/v1/container-images", self.base_url);
        if let Some(c) = channel {
            let _ = write!(url, "?channel={c}");
        }

        let response = self
            .client
            .get(&url)
            .bearer_auth(self.token.as_deref().unwrap_or(""))
            .send()
            .await
            .context(HttpRequestFailedSnafu)?;

        self.handle_response(response).await
    }

    // ========================================================================
    // Secrets
    // ========================================================================

    pub async fn list_secrets(&self, org_app: &str) -> Result<Vec<Secret>> {
        let app = self.get_app(org_app).await?;
        let url = format!("{}/v1/apps/{}/secrets", self.base_url, app.id);

        let response = self
            .client
            .get(&url)
            .bearer_auth(self.token.as_deref().unwrap_or(""))
            .send()
            .await
            .context(HttpRequestFailedSnafu)?;

        let secrets: SecretsResponse = self.handle_response(response).await?;
        Ok(secrets.secrets)
    }

    pub async fn get_secret(&self, org_app: &str, name: &str) -> Result<Secret> {
        let app = self.get_app(org_app).await?;
        let url = format!("{}/v1/apps/{}/secrets/{}", self.base_url, app.id, name);

        let response = self
            .client
            .get(&url)
            .bearer_auth(self.token.as_deref().unwrap_or(""))
            .send()
            .await
            .context(HttpRequestFailedSnafu)?;

        self.handle_response(response).await
    }

    pub async fn set_secret(&self, org_app: &str, name: &str, value: &str) -> Result<Secret> {
        let app = self.get_app(org_app).await?;
        let url = format!("{}/v1/apps/{}/secrets", self.base_url, app.id);

        let request = SetSecretRequest {
            name: name.to_string(),
            value: value.to_string(),
        };

        let response = self
            .client
            .post(&url)
            .bearer_auth(self.token.as_deref().unwrap_or(""))
            .json(&request)
            .send()
            .await
            .context(HttpRequestFailedSnafu)?;

        self.handle_response(response).await
    }

    pub async fn delete_secret(&self, org_app: &str, name: &str) -> Result<()> {
        let app = self.get_app(org_app).await?;
        let url = format!("{}/v1/apps/{}/secrets/{}", self.base_url, app.id, name);

        let response = self
            .client
            .delete(&url)
            .bearer_auth(self.token.as_deref().unwrap_or(""))
            .send()
            .await
            .context(HttpRequestFailedSnafu)?;

        self.handle_empty_response(response).await
    }

    // ========================================================================
    // API Keys
    // ========================================================================

    pub async fn get_api_keys(&self, org_app: &str) -> Result<ApiKeysResponse> {
        let app = self.get_app(org_app).await?;
        let url = format!("{}/v1/apps/{}/api-keys", self.base_url, app.id);

        let response = self
            .client
            .get(&url)
            .bearer_auth(self.token.as_deref().unwrap_or(""))
            .send()
            .await
            .context(HttpRequestFailedSnafu)?;

        self.handle_response(response).await
    }

    pub async fn regenerate_api_key(
        &self,
        org_app: &str,
        key_number: u8,
    ) -> Result<RegenerateApiKeyResponse> {
        let app = self.get_app(org_app).await?;
        let url = format!("{}/v1/apps/{}/api-keys", self.base_url, app.id);

        let request = RegenerateApiKeyRequest { key_number };

        let response = self
            .client
            .post(&url)
            .bearer_auth(self.token.as_deref().unwrap_or(""))
            .json(&request)
            .send()
            .await
            .context(HttpRequestFailedSnafu)?;

        self.handle_response(response).await
    }

    // ========================================================================
    // Response handling
    // ========================================================================

    async fn handle_response<T: serde::de::DeserializeOwned>(
        &self,
        response: reqwest::Response,
    ) -> Result<T> {
        let status = response.status();
        let body = response.text().await.context(HttpRequestFailedSnafu)?;

        match status.as_u16() {
            200..=202 => {
                serde_json::from_str(&body).map_err(|e| crate::error::Error::InvalidResponse {
                    message: format!("Failed to parse response: {e}"),
                })
            }
            401 => InvalidArgumentSnafu {
                message: "Unauthorized: invalid or expired token. Run 'spice cloud login' to re-authenticate",
            }
            .fail(),
            403 => InvalidArgumentSnafu {
                message: "Forbidden: insufficient permissions for this operation",
            }
            .fail(),
            404 => InvalidResponseSnafu {
                message: "Not found: the requested resource does not exist",
            }
            .fail(),
            _ => InvalidResponseSnafu {
                message: format!("Request failed with status {status}: {body}"),
            }
            .fail(),
        }
    }

    async fn handle_empty_response(&self, response: reqwest::Response) -> Result<()> {
        let status = response.status();
        let body = response.text().await.context(HttpRequestFailedSnafu)?;

        match status.as_u16() {
            200..=204 => Ok(()),
            401 => InvalidArgumentSnafu {
                message: "Unauthorized: invalid or expired token. Run 'spice cloud login' to re-authenticate",
            }
            .fail(),
            403 => InvalidArgumentSnafu {
                message: "Forbidden: insufficient permissions for this operation",
            }
            .fail(),
            404 => InvalidResponseSnafu {
                message: "Not found: the requested resource does not exist",
            }
            .fail(),
            _ => InvalidResponseSnafu {
                message: format!("Request failed with status {status}: {body}"),
            }
            .fail(),
        }
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

fn get_auth_token() -> Result<String> {
    // First check environment variable
    if let Ok(token) = std::env::var("SPICE_SPICEAI_TOKEN")
        && !token.is_empty()
    {
        return Ok(token);
    }

    // Try .env.local first, then .env
    let env_file = if std::path::Path::new(".env.local").exists() {
        ".env.local"
    } else {
        ".env"
    };

    if let Ok(content) = std::fs::read_to_string(env_file) {
        for line in content.lines() {
            if let Some(value) = line.strip_prefix("SPICE_SPICEAI_TOKEN=") {
                let token = value.trim_matches('"').trim_matches('\'').to_string();
                if !token.is_empty() {
                    return Ok(token);
                }
            }
        }
    }

    InvalidArgumentSnafu {
        message: "Not authenticated. Run 'spice cloud login' to authenticate with Spice Cloud",
    }
    .fail()
}

fn parse_org_app(org_app: &str) -> (String, String) {
    if let Some((org, app)) = org_app.split_once('/') {
        (org.to_string(), app.to_string())
    } else {
        (String::new(), org_app.to_string())
    }
}
