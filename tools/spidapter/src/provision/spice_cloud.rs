// Copyright 2026 Spice AI, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::time::Duration;

use spice_cloud_client::CloudClient;
use spice_cloud_client::types::UpdateAppRequest;
use system_adapter_protocol::DatasetConfig;
use uuid::Uuid;

use super::super::{
    FederatedStorageConfig, RunState, ScpRunState, SetupConfig, generate_initial_spicepod,
    serialize_spicepod,
};
use crate::args::{DeploymentMode, StdioArgs};
use crate::commands;
use crate::scenario::{CayenneConfig, ScpConfig};

#[expect(clippy::too_many_arguments)]
pub(crate) async fn provision_scp_app(
    run_id: Uuid,
    args: &StdioArgs,
    scp: &ScpConfig,
    setup_config: &SetupConfig,
    datasets: &HashMap<String, DatasetConfig>,
    deployment_mode: &DeploymentMode,
    wait_for_ready: bool,
    cayenne: Option<&CayenneConfig>,
) -> anyhow::Result<RunState> {
    let api_url = args.spice_cloud_api_url.trim_end_matches('/');
    let cloud = commands::build_cloud_client(Some(api_url), args.api_key.as_deref())?;

    let cname = commands::resolve_default_cname(&cloud).await?;
    let flight_url = scp
        .flight_url
        .clone()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| commands::flight_url_from_cname(&cname));
    let run_id_str = run_id.to_string();
    let short_id = run_id_str.split('-').next().unwrap_or_default();
    let app_name = commands::sanitize_app_name(&format!("spidapter-{short_id}"));

    eprintln!("[stdio] Spice Cloud API: {api_url}");
    eprintln!("[stdio] Region cname: {cname}");
    eprintln!("[stdio] Flight endpoint: {flight_url}");
    eprintln!("[stdio] App name: {app_name}");

    let res = &scp.resources;
    let app_create_config = commands::AppCreateConfig {
        app_memory_limit: res.app_memory.clone(),
        app_cpu_limit: res.app_cpu.clone(),
        app_cpu_request: res.app_cpu_request.clone(),
        app_memory_request: res.app_memory_request.clone(),
        app_replicas: res.app_replicas,
        app_storage_size_gb: res.app_storage_size_gb,
        executor_replicas: res.executor_replicas.unwrap_or(1),
        executor_memory_limit: res.executor_memory.clone(),
        executor_cpu_limit: res.executor_cpu.clone(),
        executor_cpu_request: res.executor_cpu_request.clone(),
        executor_memory_request: res.executor_memory_request.clone(),
        executor_storage_size_gb: res.executor_storage_size_gb,
        ephemeral_storage_limit_gb: res.ephemeral_storage_gb.clone(),
        organization_tag: scp.organization_tag.clone(),
    };
    eprintln!(
        "[stdio] App resource config: \
         app_memory_limit={:?}, app_memory_request={:?}, app_cpu_limit={:?}, app_cpu_request={:?}, \
         app_replicas={:?}, app_storage_size_gb={:?}, \
         executor_replicas={}, executor_memory_limit={:?}, executor_memory_request={:?}, \
         executor_cpu_limit={:?}, executor_cpu_request={:?}, executor_storage_size_gb={:?}, \
         ephemeral_storage_limit_gb={:?}",
        app_create_config.app_memory_limit,
        app_create_config.app_memory_request,
        app_create_config.app_cpu_limit,
        app_create_config.app_cpu_request,
        app_create_config.app_replicas,
        app_create_config.app_storage_size_gb,
        app_create_config.executor_replicas,
        app_create_config.executor_memory_limit,
        app_create_config.executor_memory_request,
        app_create_config.executor_cpu_limit,
        app_create_config.executor_cpu_request,
        app_create_config.executor_storage_size_gb,
        app_create_config.ephemeral_storage_limit_gb,
    );
    let app_id =
        commands::ensure_spice_cloud_app(&cloud, &app_name, &app_create_config, deployment_mode)
            .await?;

    // Fetch API key from the dedicated api-keys endpoint
    let api_keys = cloud
        .get_api_keys(app_id)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to fetch API keys for app '{app_name}': {e}"))?;

    let api_key = api_keys.api_key.ok_or_else(|| {
        anyhow::anyhow!("Spice Cloud did not return an API key for app '{app_name}'")
    })?;

    eprintln!("[stdio] App ID: {app_id}");
    eprintln!("[stdio] Deployment mode: {deployment_mode:?}");

    let spicepod =
        generate_initial_spicepod(&run_id, setup_config, datasets, None, args, scp, cayenne)
            .await?;
    let spicepod_yaml = serialize_spicepod(&spicepod)?;
    eprintln!(
        "[stdio] Generated spicepod for app '{app_name}' ({} bytes):\n{spicepod_yaml}",
        spicepod_yaml.len()
    );

    eprintln!("[stdio] Uploading spicepod to app...");
    commands::apply_spicepod_to_app(&cloud, app_id, &spicepod_yaml).await?;
    eprintln!("[stdio] Spicepod uploaded");

    // Set secrets from environment for any secret references in the spicepod
    eprintln!("[stdio] Setting secrets from spicepod...");
    commands::secrets::set_spicepod_secrets(&cloud, app_id, &spicepod_yaml).await?;
    eprintln!("[stdio] Spicepod secrets set");

    eprintln!("[stdio] Setting RUNNER secret...");
    match commands::secrets::set_secret(&cloud, app_id, "RUNNER", "spidapter").await {
        Ok(()) => eprintln!("[stdio] RUNNER secret set"),
        Err(e) => eprintln!("[stdio] warning: failed to set RUNNER secret (non-fatal): {e}"),
    }

    // Apply custom image configuration if any image-related overrides are provided.
    let has_custom_image = scp.image_tag.is_some() || scp.channel.is_some();

    if has_custom_image {
        eprintln!(
            "[stdio] Applying custom image config: tag={:?}, channel={:?}",
            scp.image_tag, scp.channel
        );
        cloud
            .update_app(
                app_id,
                &UpdateAppRequest {
                    image_tag: scp.image_tag.clone(),
                    update_channel: scp.channel.as_ref().map(ToString::to_string),
                    ..UpdateAppRequest::default()
                },
            )
            .await
            .map_err(|e| {
                anyhow::anyhow!("Failed to apply custom image config to app '{app_name}': {e}")
            })?;
        eprintln!("[stdio] Custom image config applied");
    }

    eprintln!("[stdio] Creating deployment...");
    commands::create_deployment(&cloud, app_id, scp.channel.as_ref(), args.spice_debug).await?;

    // Always wait for spiced to accept SQL queries before returning.
    let poll_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(600))
        .build()?;
    commands::wait_for_deployment_ready(
        &poll_client,
        &cname,
        &api_key,
        Duration::from_secs(args.ready_wait),
    )
    .await?;

    if wait_for_ready && deployment_mode == &DeploymentMode::Cluster {
        let executor_wait_timeout = std::env::var("SPIDAPTER_DEPLOYMENT_READY_WAIT")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(120);

        eprintln!(
            "[stdio] Deployment is ready, waiting an additional {executor_wait_timeout}s for executors to connect..."
        );
        tokio::time::sleep(Duration::from_secs(executor_wait_timeout)).await;
    }

    eprintln!("[stdio] Spice Cloud deployment ready for app '{app_name}' at {flight_url}");

    let sql_url = format!("https://{cname}.spiceai.io/v1/sql");

    Ok(RunState::Scp(Box::new(ScpRunState {
        app_id,
        api_key,
        flight_url,
        sql_url,
        cloud,
        storage: FederatedStorageConfig::Direct, // will be replaced by setup() caller
        ec2_guards: vec![],
        dynamodb_guard: None,
    })))
}

#[expect(dead_code)]
pub(crate) async fn wait_for_scp_executor_count(
    cloud: &CloudClient,
    app_id: i64,
    expected_count: u64,
    timeout: Duration,
) {
    eprintln!(
        "[stdio] Deployment is ready, waiting up to {}s for {expected_count} executor(s) to connect...",
        timeout.as_secs(),
    );

    let started = tokio::time::Instant::now();

    loop {
        if started.elapsed() > timeout {
            eprintln!(
                "[stdio] Timed out after {}s waiting for {expected_count} executor(s); proceeding anyway",
                timeout.as_secs(),
            );
            return;
        }

        match cloud.get_app_metrics(app_id, None).await {
            Ok(metrics) => {
                if let Some(cluster) = &metrics.cluster
                    && let Some(count) = cluster.active_executors_count
                    && count >= expected_count
                {
                    eprintln!("[stdio] {count}/{expected_count} executor(s) connected");
                    return;
                }
            }
            Err(e) => {
                eprintln!("[stdio] Metrics poll error (retrying): {e}");
            }
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
