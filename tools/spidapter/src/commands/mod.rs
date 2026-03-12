/*
Copyright 2024-2025 The Spice.ai OSS Authors

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
        AppResourceLimits, AppResourceRequests, AppResources, CreateAppRequest,
        CreateDeploymentRequest, UpdateAppRequest,
    },
};

pub(crate) mod secrets;

pub(crate) fn spice_cloud_base_url(api_url_override: Option<&str>) -> String {
    api_url_override
        .map(ToString::to_string)
        .or_else(|| std::env::var("SPICE_CLOUD_API_URL").ok())
        .unwrap_or_else(|| "https://api.spice.ai".to_string())
        .trim_end_matches('/')
        .to_string()
}

pub(crate) fn spice_cloud_token() -> anyhow::Result<String> {
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

/// Build a [`CloudClient`] from an optional API URL override and environment token.
pub(crate) fn build_cloud_client(api_url_override: Option<&str>) -> anyhow::Result<CloudClient> {
    let base_url = spice_cloud_base_url(api_url_override);
    let token = spice_cloud_token()?;
    Ok(CloudClient::new(&base_url)?
        .with_token(token)
        .with_timeout(Duration::from_secs(600))?)
}

/// Parse an optional non-negative integer from an environment variable.
///
/// Returns `Ok(None)` when the variable is unset, and an error when it is set
/// but cannot be parsed as a non-negative integer.
fn env_var_replicas(name: &str) -> anyhow::Result<Option<i32>> {
    match std::env::var(name) {
        Err(_) => Ok(None),
        Ok(val) => val
            .trim()
            .parse::<i32>()
            .map(Some)
            .map_err(|_| anyhow::anyhow!("{name} must be a non-negative integer, got '{val}'")),
    }
}

/// Default resource allocation shared by scheduler and executor when no env vars override them.
fn default_resources() -> AppResources {
    AppResources {
        limits: AppResourceLimits {
            cpu: None,
            memory: "16Gi".to_string(),
            ephemeral_storage: None,
        },
        requests: Some(AppResourceRequests {
            cpu: Some("0.1".to_string()),
            memory: Some("256Mi".to_string()),
        }),
    }
}

/// Build an [`AppResources`] from environment variables.
///
/// The memory limit variable is the anchor: if it is unset, `None` is returned.
/// CPU limit/request and memory request are all optional on top of that.
fn env_var_resources(
    memory_limit_var: &str,
    cpu_limit_var: &str,
    cpu_request_var: &str,
    memory_request_var: &str,
) -> Option<AppResources> {
    let memory_limit = std::env::var(memory_limit_var).ok()?;
    let cpu_request = std::env::var(cpu_request_var).ok();
    let memory_request = std::env::var(memory_request_var).ok();
    Some(AppResources {
        limits: AppResourceLimits {
            cpu: std::env::var(cpu_limit_var).ok(),
            memory: memory_limit,
            ephemeral_storage: None,
        },
        requests: if cpu_request.is_some() || memory_request.is_some() {
            Some(AppResourceRequests {
                cpu: cpu_request,
                memory: memory_request,
            })
        } else {
            None
        },
    })
}

pub(crate) async fn ensure_spice_cloud_app(
    cloud: &CloudClient,
    app_name: &str,
) -> anyhow::Result<(i64, Option<String>)> {
    let apps = cloud.list_apps().await?;
    if let Some(app) = apps.into_iter().find(|a| a.name == app_name) {
        return Ok((app.id, app.api_key));
    }

    let cname = resolve_default_cname(cloud).await?;

    // App (scheduler) resources — default to 16Gi memory / 0.1 cpu request when unset.
    let resources = env_var_resources(
        "SPIDAPTER_APP_MEMORY_LIMIT",
        "SPIDAPTER_APP_CPU_LIMIT",
        "SPIDAPTER_APP_CPU_REQUEST",
        "SPIDAPTER_APP_MEMORY_REQUEST",
    )
    .unwrap_or_else(default_resources);

    let replicas = env_var_replicas("SPIDAPTER_APP_REPLICAS")?;

    // Executor resources — same defaults as scheduler; override via env vars.
    let executor_resources = env_var_resources(
        "SPIDAPTER_EXECUTOR_MEMORY_LIMIT",
        "SPIDAPTER_EXECUTOR_CPU_LIMIT",
        "SPIDAPTER_EXECUTOR_CPU_REQUEST",
        "SPIDAPTER_EXECUTOR_MEMORY_REQUEST",
    )
    .unwrap_or_else(default_resources);
    let executor_replicas = env_var_replicas("SPIDAPTER_EXECUTOR_REPLICAS")?.unwrap_or(1);

    let create_result = cloud
        .create_app(&CreateAppRequest {
            name: app_name.to_string(),
            description: None,
            visibility: "private".to_string(),
            cname: Some(cname),
            tags: Some(BTreeMap::from([(
                "kind".to_string(),
                "cluster".to_string(),
            )])),
            replicas,
            resources: Some(resources),
            executor_replicas: Some(executor_replicas),
            executor_resources: Some(executor_resources),
        })
        .await;

    match create_result {
        Ok(app) => Ok((app.id, app.api_key)),
        Err(spice_cloud_client::error::Error::Conflict { .. }) => {
            // Race condition — another caller created it; re-fetch
            let apps = cloud.list_apps().await?;
            if let Some(app) = apps.into_iter().find(|a| a.name == app_name) {
                return Ok((app.id, app.api_key));
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
                description: None,
                visibility: None,
                replicas: None,
                image_tag: None,
                region: None,
                spicepod: Some(spicepod_yaml.to_string()),
                resources: None,
                executor_replicas: None,
                executor_resources: None,
            },
        )
        .await?;
    Ok(())
}

pub(crate) async fn create_deployment(
    cloud: &CloudClient,
    app_id: i64,
    channel: Option<&str>,
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
                channel: channel.map(String::from),
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
