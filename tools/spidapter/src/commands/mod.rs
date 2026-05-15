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

use std::{collections::BTreeMap, time::Duration};

use reqwest::Client;
use spice_cloud_client::{
    CloudClient,
    types::{
        AppExecutor, AppResourceLimits, AppResourceRequests, AppResources, CreateAppRequest,
        CreateDeploymentRequest, UpdateAppRequest, UpdateChannel,
    },
};

pub(crate) mod secrets;

/// Resource and replica configuration for creating a Spice Cloud app.
pub(crate) struct AppCreateConfig {
    pub app_memory_limit: Option<String>,
    pub app_cpu_limit: Option<String>,
    pub app_cpu_request: Option<String>,
    pub app_memory_request: Option<String>,
    pub app_replicas: Option<i32>,
    pub app_storage_size_gb: Option<f64>,
    pub executor_replicas: i32,
    pub executor_memory_limit: Option<String>,
    pub executor_cpu_limit: Option<String>,
    pub executor_cpu_request: Option<String>,
    pub executor_memory_request: Option<String>,
    pub executor_storage_size_gb: Option<f64>,
    pub ephemeral_storage_limit_gb: Option<String>,
    pub organization_tag: Option<String>,
}

pub(crate) fn spice_cloud_base_url(api_url_override: Option<&str>) -> String {
    api_url_override
        .map_or_else(|| "https://api.spice.ai".to_string(), ToString::to_string)
        .trim_end_matches('/')
        .to_string()
}

/// Resolve the Spice Cloud API token.
///
/// Uses `api_key_override` when provided; otherwise falls back to the
/// `SPICEAI_API_KEY`, `SPICE_API_KEY`, `SPICE_SPICEAI_API_KEY`, and
/// `SPICE_SPICEAI_TOKEN` environment variables (in that order).
pub(crate) fn spice_cloud_token(api_key_override: Option<&str>) -> anyhow::Result<String> {
    if let Some(key) = api_key_override {
        return Ok(key.to_string());
    }
    std::env::var("SPICEAI_API_KEY")
        .or_else(|_| std::env::var("SPICE_API_KEY"))
        .or_else(|_| std::env::var("SPICE_SPICEAI_API_KEY"))
        .or_else(|_| std::env::var("SPICE_SPICEAI_TOKEN"))
        .map_err(|_| {
            anyhow::anyhow!(
                "No Spice Cloud token found. Set one of SPICEAI_API_KEY, SPICE_API_KEY, SPICE_SPICEAI_API_KEY, or SPICE_SPICEAI_TOKEN"
            )
        })
}

/// Build a [`CloudClient`] from an optional API URL override, optional API key
/// override, and environment token fallback.
pub(crate) fn build_cloud_client(
    api_url_override: Option<&str>,
    api_key_override: Option<&str>,
) -> anyhow::Result<CloudClient> {
    let base_url = spice_cloud_base_url(api_url_override);
    let token = spice_cloud_token(api_key_override)?;
    Ok(CloudClient::new(&base_url)?
        .with_token(token)
        .with_timeout(Duration::from_secs(600))?)
}

/// Default resource allocation shared by scheduler and executor when no overrides are provided.
fn default_resources() -> AppResources {
    AppResources {
        limits: AppResourceLimits {
            cpu: None,
            memory: Some("16Gi".to_string()),
            ephemeral_storage: None,
        },
        requests: Some(AppResourceRequests {
            cpu: Some("0.1".to_string()),
            memory: Some("256Mi".to_string()),
        }),
    }
}

/// Build an [`AppResources`] by merging explicit overrides on top of a set of
/// base (default) resources.
///
/// Each field is overridden independently: only the values that are `Some`
/// replace the corresponding field in `base`.
fn resources_over(
    base: AppResources,
    memory_limit: Option<&str>,
    cpu_limit: Option<&str>,
    cpu_request: Option<&str>,
    memory_request: Option<&str>,
    ephemeral_storage_limit: Option<&str>,
) -> AppResources {
    let memory_limit_val = memory_limit.map(ToString::to_string).or(base.limits.memory);
    let cpu_limit_val = cpu_limit.map(ToString::to_string).or(base.limits.cpu);
    let cpu_request_val = cpu_request
        .map(ToString::to_string)
        .or(base.requests.as_ref().and_then(|r| r.cpu.clone()));
    let memory_request_val = memory_request
        .map(ToString::to_string)
        .or(base.requests.as_ref().and_then(|r| r.memory.clone()));

    AppResources {
        limits: AppResourceLimits {
            cpu: cpu_limit_val,
            memory: memory_limit_val,
            ephemeral_storage: ephemeral_storage_limit.map(ToString::to_string),
        },
        requests: if cpu_request_val.is_some() || memory_request_val.is_some() {
            Some(AppResourceRequests {
                cpu: cpu_request_val,
                memory: memory_request_val,
            })
        } else {
            None
        },
    }
}

pub(crate) async fn ensure_spice_cloud_app(
    cloud: &CloudClient,
    app_name: &str,
    config: &AppCreateConfig,
) -> anyhow::Result<i64> {
    let apps = cloud.list_apps().await?;
    if let Some(app) = apps.into_iter().find(|a| a.name == app_name) {
        return Ok(app.id);
    }

    let cname = resolve_default_cname(cloud).await?;

    // App (scheduler) resources — start from defaults, then apply any overrides.
    let resources = resources_over(
        default_resources(),
        config.app_memory_limit.as_deref(),
        config.app_cpu_limit.as_deref(),
        config.app_cpu_request.as_deref(),
        config.app_memory_request.as_deref(),
        config.ephemeral_storage_limit_gb.as_deref(),
    );

    // Executor — same resource defaults as scheduler; each field overridable independently.
    let executor = Some(AppExecutor {
        replicas: Some(config.executor_replicas),
        resources: Some(resources_over(
            default_resources(),
            config.executor_memory_limit.as_deref(),
            config.executor_cpu_limit.as_deref(),
            config.executor_cpu_request.as_deref(),
            config.executor_memory_request.as_deref(),
            config.ephemeral_storage_limit_gb.as_deref(),
        )),
        storage_size_gb: config.executor_storage_size_gb,
    });

    let create_result = cloud
        .create_app(&CreateAppRequest {
            name: app_name.to_string(),
            description: None,
            visibility: "private".to_string(),
            cname: Some(cname),
            tags: {
                let mut tags = BTreeMap::from([("kind".to_string(), "cluster".to_string())]);
                if let Some(org) = &config.organization_tag {
                    tags.insert("organization".to_string(), org.clone());
                }
                Some(tags)
            },
            replicas: config.app_replicas,
            resources: Some(resources),
            executor,
            storage_size_gb: None,
        })
        .await;

    match create_result {
        Ok(app) => {
            apply_storage_config(cloud, app.id, config).await?;
            Ok(app.id)
        }
        Err(spice_cloud_client::error::Error::Conflict { .. }) => {
            // Race condition — another caller created it; re-fetch
            let apps = cloud.list_apps().await?;
            if let Some(app) = apps.into_iter().find(|a| a.name == app_name) {
                apply_storage_config(cloud, app.id, config).await?;
                return Ok(app.id);
            }
            Err(anyhow::anyhow!(
                "App '{app_name}' not found after conflict on create"
            ))
        }
        Err(e) => Err(anyhow::anyhow!(
            "Failed to create Spice Cloud app '{app_name}': {e}"
        )),
    }
}

/// Apply storage configuration to an app via the update API if any storage
/// sizes are configured.
async fn apply_storage_config(
    cloud: &CloudClient,
    app_id: i64,
    config: &AppCreateConfig,
) -> anyhow::Result<()> {
    let has_storage =
        config.app_storage_size_gb.is_some() || config.executor_storage_size_gb.is_some();

    if !has_storage {
        return Ok(());
    }

    let executor = config.executor_storage_size_gb.map(|size| AppExecutor {
        replicas: None,
        resources: None,
        storage_size_gb: Some(size),
    });

    cloud
        .update_app(
            app_id,
            &UpdateAppRequest {
                executor,
                storage_size_gb: config.app_storage_size_gb,
                ..UpdateAppRequest::default()
            },
        )
        .await?;

    Ok(())
}

pub(crate) async fn resolve_default_cname(cloud: &CloudClient) -> anyhow::Result<String> {
    let regions = cloud.list_regions(None).await?;

    // Find the region matching the `default` field, then return its cname
    if let Some(default_region) = &regions.default
        && !default_region.is_empty()
    {
        if let Some(region) = regions.regions.iter().find(|r| r.region == *default_region)
            && let Some(cname) = &region.cname
        {
            return Ok(cname.clone());
        }
        // Fall back to the default value itself if no matching region found
        return Ok(default_region.clone());
    }

    if let Some(region) = regions
        .regions
        .iter()
        .find(|region| region.is_default && !region.disabled)
    {
        return Ok(region
            .cname
            .clone()
            .unwrap_or_else(|| region.region.clone()));
    }

    if let Some(region) = regions.regions.iter().find(|region| !region.disabled) {
        return Ok(region
            .cname
            .clone()
            .unwrap_or_else(|| region.region.clone()));
    }

    Err(anyhow::anyhow!(
        "Unable to determine a default Spice Cloud region (cname) for app creation"
    ))
}

pub(crate) async fn apply_spicepod_to_app(
    cloud: &CloudClient,
    app_id: i64,
    spicepod_yaml: &str,
) -> anyhow::Result<()> {
    cloud
        .update_app(
            app_id,
            &UpdateAppRequest {
                spicepod: Some(spicepod_yaml.to_string()),
                ..UpdateAppRequest::default()
            },
        )
        .await?;
    Ok(())
}

pub(crate) async fn create_deployment(
    cloud: &CloudClient,
    app_id: i64,
    channel: Option<&UpdateChannel>,
) -> anyhow::Result<()> {
    let created = cloud
        .create_deployment(
            app_id,
            &CreateDeploymentRequest {
                image: None,
                image_tag: None,
                replicas: Some(1),
                branch: None,
                commit_sha: None,
                commit_message: None,
                channel: channel.map(ToString::to_string),
                debug: false,
            },
        )
        .await?;
    eprintln!("Deployment {} created", created.id);
    Ok(())
}

/// Delete (soft-delete) a Spice Cloud app.
pub(crate) async fn delete_app(cloud: &CloudClient, app_id: i64) -> anyhow::Result<()> {
    cloud.delete_app(app_id).await?;
    Ok(())
}

/// Wait for a Spice Cloud deployment to become ready by polling the SQL endpoint.
///
/// Sends `SELECT 1` to `https://{cname}.spiceai.io/v1/sql` until it returns a successful response.
pub(crate) async fn wait_for_deployment_ready(
    client: &Client,
    cname: &str,
    api_key: &str,
    timeout: Duration,
) -> anyhow::Result<()> {
    let sql_url = format!("https://{cname}.spiceai.io/v1/sql");
    eprintln!("Waiting for deployment to become ready at {sql_url}...");

    let started = tokio::time::Instant::now();
    loop {
        if started.elapsed() > timeout {
            return Err(anyhow::anyhow!(
                "Timed out after {}s waiting for deployment to become ready at {sql_url}",
                timeout.as_secs(),
            ));
        }

        let elapsed = started.elapsed().as_secs();

        match client
            .post(&sql_url)
            .header("X-API-Key", api_key)
            .body("SELECT 1")
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => {
                eprintln!("  Deployment ready ({elapsed}s elapsed)");
                return Ok(());
            }
            Ok(response) => {
                eprintln!("  Not ready: {} ({elapsed}s elapsed)", response.status());
            }
            Err(e) => {
                eprintln!("  Not ready: {e} ({elapsed}s elapsed)");
            }
        }

        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

/// Derive the Flight endpoint URL from a Spice Cloud cname.
///
/// Replaces the `-data` suffix with `-flight` and constructs `https://{flight_cname}.spiceai.io`.
/// For example, `us-east-1-dev-aws-data` becomes `https://us-east-1-dev-aws-flight.spiceai.io`.
pub(crate) fn flight_url_from_cname(cname: &str) -> String {
    let flight_cname = if let Some(prefix) = cname.strip_suffix("-data") {
        format!("{prefix}-flight")
    } else {
        cname.to_string()
    };
    format!("https://{flight_cname}.spiceai.io")
}

/// Sanitize a spicepod name for use as a Spice Cloud app name.
///
/// App names can only contain letters, numbers, and hyphens.
/// Truncated to 42 characters to leave room for Kubernetes name prefixes
/// and suffixes (e.g. `spicepod-{name}-scheduler-0` must be ≤63 chars).
pub(crate) fn sanitize_app_name(name: &str) -> String {
    let sanitized: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '-'
            }
        })
        .take(42)
        .collect();
    sanitized.trim_end_matches('-').to_string()
}
