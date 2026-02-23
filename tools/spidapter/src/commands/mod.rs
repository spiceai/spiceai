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

use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use test_framework::anyhow;

pub(crate) mod secrets;

#[derive(Debug, Deserialize)]
struct CloudAppsResponse {
    apps: Vec<CloudApp>,
}

#[derive(Debug, Deserialize)]
struct CloudApp {
    id: i64,
    name: String,
    api_key: Option<String>,
}

#[derive(Debug, Serialize)]
struct CloudCreateAppRequest {
    name: String,
    cname: String,
    visibility: String,
    tags: BTreeMap<String, String>,
}

#[derive(Debug, Serialize)]
struct CloudUpdateAppRequest {
    spicepod: String,
}

#[derive(Debug, Deserialize)]
struct CloudRegionsResponse {
    regions: Vec<CloudRegion>,
    default: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CloudRegion {
    region: String,
    #[serde(default)]
    cname: Option<String>,
    #[serde(rename = "isDefault", default)]
    is_default: bool,
    #[serde(default)]
    disabled: bool,
}

#[derive(Debug, Deserialize)]
struct CloudDeployment {
    id: i64,
}

#[derive(Debug, Serialize)]
struct CloudCreateDeploymentRequest {
    debug: bool,
}

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

pub(crate) async fn ensure_spice_cloud_app(
    client: &Client,
    base_url: &str,
    token: &str,
    app_name: &str,
) -> anyhow::Result<(i64, Option<String>)> {
    let apps_url = format!("{base_url}/v1/apps");
    let response = client.get(&apps_url).bearer_auth(token).send().await?;

    if response.status() == StatusCode::UNAUTHORIZED {
        return Err(anyhow::anyhow!(
            "Spice Cloud authentication failed (401). Verify your token scopes for apps:read/apps:write"
        ));
    }

    let apps: CloudAppsResponse = response.error_for_status()?.json().await?;
    if let Some(app) = apps.apps.into_iter().find(|a| a.name == app_name) {
        return Ok((app.id, app.api_key));
    }

    let cname = resolve_default_cname(client, base_url, token).await?;

    let create_response = client
        .post(&apps_url)
        .bearer_auth(token)
        .json(&CloudCreateAppRequest {
            name: app_name.to_string(),
            cname,
            visibility: "private".to_string(),
            tags: BTreeMap::from([("kind".to_string(), "cluster".to_string())]),
        })
        .send()
        .await?;

    if create_response.status() == StatusCode::CONFLICT {
        let retry = client.get(&apps_url).bearer_auth(token).send().await?;
        let apps: CloudAppsResponse = retry.error_for_status()?.json().await?;
        if let Some(app) = apps.apps.into_iter().find(|a| a.name == app_name) {
            return Ok((app.id, app.api_key));
        }
    }

    let status = create_response.status();
    if status.is_client_error() || status.is_server_error() {
        let body = create_response
            .text()
            .await
            .unwrap_or_else(|_| "<failed to read body>".to_string());
        return Err(anyhow::anyhow!(
            "Failed to create Spice Cloud app '{app_name}' ({status}): {body}"
        ));
    }

    let app: CloudApp = create_response.json().await?;
    Ok((app.id, app.api_key))
}

pub(crate) async fn resolve_default_cname(
    client: &Client,
    base_url: &str,
    token: &str,
) -> anyhow::Result<String> {
    let regions_url = format!("{base_url}/v1/regions");
    let response = client.get(&regions_url).bearer_auth(token).send().await?;
    let regions: CloudRegionsResponse = response.error_for_status()?.json().await?;

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
    client: &Client,
    base_url: &str,
    token: &str,
    app_id: i64,
    spicepod_yaml: &str,
) -> anyhow::Result<()> {
    let app_url = format!("{base_url}/v1/apps/{app_id}");

    client
        .put(app_url)
        .bearer_auth(token)
        .json(&CloudUpdateAppRequest {
            spicepod: spicepod_yaml.to_string(),
        })
        .send()
        .await?
        .error_for_status()?;

    Ok(())
}

pub(crate) async fn create_deployment(
    client: &Client,
    base_url: &str,
    token: &str,
    app_id: i64,
) -> anyhow::Result<()> {
    let deployments_url = format!("{base_url}/v1/apps/{app_id}/deployments");

    let response = client
        .post(&deployments_url)
        .bearer_auth(token)
        .json(&CloudCreateDeploymentRequest { debug: false })
        .send()
        .await?;

    let created: CloudDeployment = response.error_for_status()?.json().await?;
    eprintln!("Deployment {} created", created.id);
    Ok(())
}

/// Delete (soft-delete) a Spice Cloud app.
///
/// Calls `DELETE /v1/apps/{appId}` which sets `deleted_at`, stops the app,
/// and releases its resources.
pub(crate) async fn delete_app(
    client: &Client,
    base_url: &str,
    token: &str,
    app_id: i64,
) -> anyhow::Result<()> {
    let app_url = format!("{base_url}/v1/apps/{app_id}");

    client
        .delete(&app_url)
        .bearer_auth(token)
        .send()
        .await?
        .error_for_status()?;

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
