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

use std::collections::BTreeMap;

use crate::error::{InvalidArgumentSnafu, InvalidResponseSnafu, Result};

pub use spice_cloud_client::CloudClient as InnerCloudClient;
use spice_cloud_client::types::{
    ApiKeysResponse, App, AppExecutor, AppKind, AppResourceLimits, AppResources, AuthContext,
    AuthExchangeResponse, ContainerImagesResponse, CreateAppRequest, CreateDeploymentRequest,
    Deployment, LogsResponse, MetricsResponse, RegenerateApiKeyResponse, RegionsResponse, Secret,
    UpdateAppRequest, UpdateChannel,
};

const DEV_CLOUD_API_BASE_URL: &str = "https://dev-api.spice.ai";
const CLOUD_API_BASE_URL: &str = "https://api.spice.ai";

/// CLI wrapper around [`spice_cloud_client::CloudClient`].
///
/// Adds convenience constructors that automatically resolve the base URL and
/// authentication token from the CLI environment.
pub struct CloudClient {
    inner: InnerCloudClient,
}

impl CloudClient {
    /// Create a new authenticated cloud client.
    pub fn new() -> Result<Self> {
        let token = get_auth_token()?;
        Self::with_token(token)
    }

    /// Create a new authenticated cloud client with an explicit bearer token.
    pub fn with_token(token: impl Into<String>) -> Result<Self> {
        Ok(Self {
            inner: InnerCloudClient::new(&get_base_url())
                .map_err(into_cli)?
                .with_token(token),
        })
    }

    /// Create a new unauthenticated cloud client (for the login flow).
    pub fn new_unauthenticated() -> Result<Self> {
        Ok(Self {
            inner: InnerCloudClient::new(&get_base_url()).map_err(into_cli)?,
        })
    }

    /// Get the auth URL for the login flow.
    pub fn get_auth_url(&self, auth_code: &str) -> String {
        self.inner.get_auth_url(auth_code)
    }

    /// Exchange an auth code for an access token.
    pub async fn exchange_code(&self, auth_code: &str) -> Result<Option<AuthExchangeResponse>> {
        self.inner.exchange_code(auth_code).await.map_err(into_cli)
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
            .map_err(into_cli)?;

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
        self.inner.get_auth_context().await.map_err(into_cli)
    }

    // ========================================================================
    // Apps
    // ========================================================================

    pub async fn get_app_metrics(
        &self,
        app_id: i64,
        window: Option<&str>,
    ) -> Result<MetricsResponse> {
        self.inner
            .get_app_metrics(app_id, window)
            .await
            .map_err(into_cli)
    }

    pub async fn list_apps(&self) -> Result<Vec<App>> {
        self.inner.list_apps().await.map_err(into_cli)
    }

    pub async fn get_app(&self, org_app: &str) -> Result<App> {
        let (org, name) = parse_org_app(org_app);

        if org.is_empty() {
            return InvalidArgumentSnafu {
                message: format!("App name must be in org/app format, got '{org_app}'"),
            }
            .fail();
        }

        let context = self.get_auth_context().await?;
        let apps = self.list_apps().await?;

        for app in apps {
            let app_org = if app.org.is_empty() {
                &context.org_name
            } else {
                &app.org
            };
            if app.name == name && app_org.eq_ignore_ascii_case(&org) {
                return self.get_app_by_id(app.id).await;
            }
        }

        InvalidResponseSnafu {
            message: format!("App '{org_app}' not found"),
        }
        .fail()
    }

    pub async fn get_app_by_id(&self, app_id: i64) -> Result<App> {
        self.inner.get_app_by_id(app_id).await.map_err(into_cli)
    }

    #[expect(clippy::too_many_arguments)]
    pub async fn create_app(
        &self,
        name: &str,
        region: &str,
        kind: AppKind,
        description: Option<&str>,
        visibility: &str,
        replicas: Option<i32>,
        cpu: Option<i32>,
        memory: Option<NumBytes>,
        storage_size_gb: Option<f64>,
        executor_replicas: Option<i32>,
        executor_cpu: Option<i32>,
        executor_memory: Option<NumBytes>,
    ) -> Result<App> {
        let resources = build_resources(cpu, memory);
        let executor = build_executor(
            executor_replicas,
            executor_cpu,
            executor_memory,
            storage_size_gb,
        );

        let (tags, replicas) = match kind {
            AppKind::Cluster => {
                let mut t = BTreeMap::new();
                t.insert("kind".to_string(), "cluster".to_string());
                (Some(t), Some(1))
            }
            AppKind::Set => (None, replicas),
        };

        let request = CreateAppRequest {
            name: name.to_string(),
            description: description.map(String::from),
            visibility: visibility.to_string(),
            cname: Some(region.to_string()),
            tags,
            replicas,
            resources,
            executor,
        };
        self.inner.create_app(&request).await.map_err(into_cli)
    }

    #[expect(clippy::too_many_arguments)]
    pub async fn update_app(
        &self,
        org_app: &str,
        description: Option<&str>,
        visibility: Option<&str>,
        replicas: Option<i32>,
        image_tag: Option<&str>,
        region: Option<&str>,
        cpu: Option<i32>,
        memory: Option<NumBytes>,
        storage_size_gb: Option<f64>,
        executor_replicas: Option<i32>,
        executor_cpu: Option<i32>,
        executor_memory: Option<NumBytes>,
        spicepod: Option<String>,
        channel: Option<UpdateChannel>,
    ) -> Result<App> {
        let app = self.get_app(org_app).await?;
        let resources = build_resources(cpu, memory);
        // The update endpoint accepts storage size at the app level; create app nests it under executor.
        let executor = build_executor(executor_replicas, executor_cpu, executor_memory, None);

        let request = UpdateAppRequest {
            description: description.map(String::from),
            visibility: visibility.map(String::from),
            replicas,
            image_tag: image_tag.map(String::from),
            update_channel: channel.map(|channel| channel.to_string()),
            region: region.map(String::from),
            resources,
            executor,
            storage_size_gb,
            spicepod,
        };
        self.inner
            .update_app(app.id, &request)
            .await
            .map_err(into_cli)
    }

    pub async fn delete_app(&self, org_app: &str) -> Result<()> {
        let app = self.get_app(org_app).await?;
        self.inner.delete_app(app.id).await.map_err(into_cli)
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
        let app = self.get_app(org_app).await?;
        self.inner
            .list_deployments(app.id, limit, status)
            .await
            .map_err(into_cli)
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
        let request = CreateDeploymentRequest {
            image: None,
            image_tag: image_tag.map(String::from),
            replicas,
            branch: None,
            commit_sha: None,
            commit_message: None,
            channel: None,
            debug,
        };
        self.inner
            .create_deployment(app.id, &request)
            .await
            .map_err(into_cli)
    }

    pub async fn get_deployment_logs(
        &self,
        org_app: &str,
        deployment_id: i64,
        limit: usize,
        since: Option<&str>,
    ) -> Result<LogsResponse> {
        let app = self.get_app(org_app).await?;
        self.inner
            .get_deployment_logs(app.id, deployment_id, limit, since)
            .await
            .map_err(into_cli)
    }

    pub async fn rollback(&self, org_app: &str, target_deployment_id: i64) -> Result<Deployment> {
        let app = self.get_app(org_app).await?;
        self.inner
            .rollback(app.id, target_deployment_id)
            .await
            .map_err(into_cli)
    }

    // ========================================================================
    // Regions & Images
    // ========================================================================

    pub async fn list_regions(&self, env: Option<&str>) -> Result<RegionsResponse> {
        self.inner.list_regions(env).await.map_err(into_cli)
    }

    pub async fn list_container_images(
        &self,
        channel: Option<&str>,
    ) -> Result<ContainerImagesResponse> {
        self.inner
            .list_container_images(channel)
            .await
            .map_err(into_cli)
    }

    // ========================================================================
    // Secrets
    // ========================================================================

    pub async fn list_secrets(&self, org_app: &str) -> Result<Vec<Secret>> {
        let app = self.get_app(org_app).await?;
        self.inner.list_secrets(app.id).await.map_err(into_cli)
    }

    pub async fn get_secret(&self, org_app: &str, name: &str) -> Result<Secret> {
        let app = self.get_app(org_app).await?;
        self.inner.get_secret(app.id, name).await.map_err(into_cli)
    }

    pub async fn set_secret(&self, org_app: &str, name: &str, value: &str) -> Result<Secret> {
        let app = self.get_app(org_app).await?;
        self.inner
            .set_secret(app.id, name, value)
            .await
            .map_err(into_cli)
    }

    pub async fn delete_secret(&self, org_app: &str, name: &str) -> Result<()> {
        let app = self.get_app(org_app).await?;
        self.inner
            .delete_secret(app.id, name)
            .await
            .map_err(into_cli)
    }

    // ========================================================================
    // API Keys
    // ========================================================================

    pub async fn get_api_keys(&self, org_app: &str) -> Result<ApiKeysResponse> {
        let app = self.get_app(org_app).await?;
        self.inner.get_api_keys(app.id).await.map_err(into_cli)
    }

    pub async fn regenerate_api_key(
        &self,
        org_app: &str,
        key_number: u8,
    ) -> Result<RegenerateApiKeyResponse> {
        let app = self.get_app(org_app).await?;
        self.inner
            .regenerate_api_key(app.id, key_number)
            .await
            .map_err(into_cli)
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
    // 1. Check environment variable
    if let Ok(token) = std::env::var("SPICE_SPICEAI_TOKEN")
        && !token.is_empty()
    {
        return Ok(token);
    }

    // 2. Try platform keychain
    if let Ok(entry) = keyring::Entry::new("SPICE_SPICEAI_TOKEN", "spice")
        && let Ok(token) = entry.get_password()
        && !token.is_empty()
    {
        return Ok(token);
    }

    // 3. Try .env.local first, then .env
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

pub fn parse_org_app(org_app: &str) -> (String, String) {
    if let Some((org, app)) = org_app.split_once('/') {
        (org.to_string(), app.to_string())
    } else {
        (String::new(), org_app.to_string())
    }
}

use super::bytes::NumBytes;

/// Build an [`AppResources`] from optional CPU (vCPUs) and a parsed [`NumBytes`] memory value.
///
/// Returns `None` if neither is provided.
fn build_resources(cpu: Option<i32>, memory: Option<NumBytes>) -> Option<AppResources> {
    if cpu.is_none() && memory.is_none() {
        return None;
    }
    Some(AppResources {
        limits: AppResourceLimits {
            cpu: cpu.map(|v| v.to_string()),
            memory: memory.map(NumBytes::to_resource_string).unwrap_or_default(),
            ephemeral_storage: None,
        },
        requests: None,
    })
}

/// Build an [`AppExecutor`] from optional executor params.
///
/// Returns `None` if no executor-related fields are provided.
fn build_executor(
    replicas: Option<i32>,
    cpu: Option<i32>,
    memory: Option<NumBytes>,
    storage_size_gb: Option<f64>,
) -> Option<AppExecutor> {
    if replicas.is_none() && cpu.is_none() && memory.is_none() && storage_size_gb.is_none() {
        return None;
    }
    Some(AppExecutor {
        replicas,
        resources: build_resources(cpu, memory),
        storage_size_gb,
    })
}

/// Convert a [`spice_cloud_client::error::Error`] into the CLI error type.
fn into_cli(e: spice_cloud_client::error::Error) -> crate::error::Error {
    use spice_cloud_client::error::Error as CloudError;
    match e {
        CloudError::Unauthorized { message } => crate::error::Error::InvalidArgument {
            message: format!("Unauthorized: {message}. Run 'spice cloud login' to re-authenticate"),
        },
        CloudError::Forbidden { message } => crate::error::Error::InvalidArgument {
            message: format!("Forbidden: {message}"),
        },
        CloudError::NotFound { message } => crate::error::Error::InvalidResponse {
            message: format!("Not found: {message}"),
        },
        CloudError::Conflict { message } => crate::error::Error::InvalidResponse {
            message: format!("Conflict: {message}"),
        },
        CloudError::Api { status, message } => crate::error::Error::InvalidResponse {
            message: format!("Request failed with status {status}: {message}"),
        },
        CloudError::HttpRequest { source } => crate::error::Error::HttpRequestFailed { source },
        CloudError::JsonParse { source } => crate::error::Error::InvalidResponse {
            message: format!("Failed to parse response: {source}"),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_resources_does_not_default_memory() {
        let resources = build_resources(Some(4), None).expect("cpu should create resources");

        assert_eq!(resources.limits.cpu.as_deref(), Some("4"));
        assert!(resources.limits.memory.is_empty());
    }

    #[test]
    fn build_resources_preserves_memory_unit() {
        let memory = NumBytes::parse("3500Mi").expect("memory should parse");

        let resources =
            build_resources(None, Some(memory)).expect("memory should create resources");

        assert_eq!(resources.limits.memory, "3500Mi");
    }

    #[test]
    fn build_executor_does_not_default_executor_memory() {
        let executor =
            build_executor(None, Some(2), None, None).expect("executor cpu should create executor");

        let resources = executor.resources.expect("executor resources should exist");
        assert_eq!(resources.limits.cpu.as_deref(), Some("2"));
        assert!(resources.limits.memory.is_empty());
    }
}
