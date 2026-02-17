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

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use crate::args::{CommonArgs, DatasetTestArgs, SpicedStartMode};
use async_trait::async_trait;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use test_framework::{
    anyhow,
    app::{App, AppBuilder},
    opentelemetry_sdk::Resource,
    queries::QuerySet,
    spiced::{SpicedInstance, StartRequest},
    spicepod::Spicepod,
    spicepod_utils::from_app,
    spicetest::datasets::NotStarted,
    telemetry::{OtlpExporterConfig, Telemetry},
};

#[cfg(feature = "append")]
pub(crate) mod append;
pub(crate) mod bench;
pub(crate) mod data_consistency;
pub(crate) mod dispatch;
pub(crate) mod evals;
pub(crate) mod load;
pub(crate) mod query;
pub(crate) mod search;
pub(crate) mod streaming;
pub(crate) mod text_to_sql;
pub(crate) mod throughput;
pub(crate) type RowCounts = BTreeMap<Arc<str>, usize>;

#[async_trait]
trait SpicedStarter {
    async fn start(
        &self,
        args: &CommonArgs,
        start_request: StartRequest,
    ) -> anyhow::Result<SpicedInstance>;
}

struct LocalSpicedStarter;

#[async_trait]
impl SpicedStarter for LocalSpicedStarter {
    async fn start(
        &self,
        _args: &CommonArgs,
        start_request: StartRequest,
    ) -> anyhow::Result<SpicedInstance> {
        SpicedInstance::start(start_request).await
    }
}

struct SpiceCloudSpicedStarter;

#[derive(Debug, Deserialize)]
struct CloudAppsResponse {
    apps: Vec<CloudApp>,
}

#[derive(Debug, Deserialize)]
struct CloudApp {
    id: i64,
    name: String,
}

#[derive(Debug, Serialize)]
struct CloudCreateAppRequest {
    name: String,
    cname: String,
    visibility: String,
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
    #[serde(rename = "isDefault")]
    is_default: bool,
    disabled: bool,
}

#[derive(Debug, Deserialize)]
struct CloudDeploymentsResponse {
    deployments: Vec<CloudDeployment>,
}

#[derive(Debug, Deserialize)]
struct CloudDeployment {
    id: i64,
    status: String,
    error_message: Option<String>,
}

#[derive(Debug, Serialize)]
struct CloudCreateDeploymentRequest {
    debug: bool,
}

fn spice_cloud_base_url(args: &CommonArgs) -> String {
    args.spiced_start_api_url
        .clone()
        .or_else(|| std::env::var("SPICE_CLOUD_API_URL").ok())
        .unwrap_or_else(|| "https://api.spice.ai".to_string())
        .trim_end_matches('/')
        .to_string()
}

fn spice_cloud_token() -> anyhow::Result<String> {
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

async fn ensure_spice_cloud_app(
    client: &Client,
    base_url: &str,
    token: &str,
    app_name: &str,
) -> anyhow::Result<i64> {
    let apps_url = format!("{base_url}/v1/apps");
    let response = client.get(&apps_url).bearer_auth(token).send().await?;

    if response.status() == StatusCode::UNAUTHORIZED {
        return Err(anyhow::anyhow!(
            "Spice Cloud authentication failed (401). Verify your token scopes for apps:read/apps:write"
        ));
    }

    let apps: CloudAppsResponse = response.error_for_status()?.json().await?;
    if let Some(app) = apps.apps.into_iter().find(|a| a.name == app_name) {
        return Ok(app.id);
    }

    let cname = resolve_default_cname(client, base_url, token).await?;

    let create_response = client
        .post(&apps_url)
        .bearer_auth(token)
        .json(&CloudCreateAppRequest {
            name: app_name.to_string(),
            cname,
            visibility: "private".to_string(),
        })
        .send()
        .await?;

    if create_response.status() == StatusCode::CONFLICT {
        let retry = client.get(&apps_url).bearer_auth(token).send().await?;
        let apps: CloudAppsResponse = retry.error_for_status()?.json().await?;
        if let Some(app) = apps.apps.into_iter().find(|a| a.name == app_name) {
            return Ok(app.id);
        }
    }

    let app: CloudApp = create_response.error_for_status()?.json().await?;
    Ok(app.id)
}

async fn resolve_default_cname(
    client: &Client,
    base_url: &str,
    token: &str,
) -> anyhow::Result<String> {
    let regions_url = format!("{base_url}/v1/regions");
    let response = client.get(&regions_url).bearer_auth(token).send().await?;
    let regions: CloudRegionsResponse = response.error_for_status()?.json().await?;

    if let Some(default_region) = regions.default
        && !default_region.is_empty()
    {
        return Ok(default_region);
    }

    if let Some(region) = regions
        .regions
        .iter()
        .find(|region| region.is_default && !region.disabled)
    {
        return Ok(region.region.clone());
    }

    if let Some(region) = regions.regions.iter().find(|region| !region.disabled) {
        return Ok(region.region.clone());
    }

    Err(anyhow::anyhow!(
        "Unable to determine a default Spice Cloud region (cname) for app creation"
    ))
}

async fn apply_spicepod_to_app(
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

async fn create_and_wait_for_deployment(
    client: &Client,
    base_url: &str,
    token: &str,
    app_id: i64,
    timeout: Duration,
) -> anyhow::Result<()> {
    let deployments_url = format!("{base_url}/v1/apps/{app_id}/deployments");

    let response = client
        .post(&deployments_url)
        .bearer_auth(token)
        .json(&CloudCreateDeploymentRequest { debug: false })
        .send()
        .await?;

    let created: CloudDeployment = response.error_for_status()?.json().await?;

    let started = tokio::time::Instant::now();
    loop {
        if started.elapsed() > timeout {
            return Err(anyhow::anyhow!(
                "Timed out waiting for Spice Cloud deployment {} to become ready",
                created.id
            ));
        }

        let status_response = client
            .get(format!("{deployments_url}?limit=1"))
            .bearer_auth(token)
            .send()
            .await?;

        let deployments: CloudDeploymentsResponse =
            status_response.error_for_status()?.json().await?;
        let Some(latest) = deployments.deployments.first() else {
            tokio::time::sleep(Duration::from_secs(2)).await;
            continue;
        };

        let normalized = latest.status.to_ascii_lowercase();
        if matches!(
            normalized.as_str(),
            "running" | "ready" | "active" | "completed" | "success" | "succeeded"
        ) {
            return Ok(());
        }

        if matches!(
            normalized.as_str(),
            "failed" | "error" | "cancelled" | "canceled"
        ) {
            return Err(anyhow::anyhow!(
                "Spice Cloud deployment {} failed: {}",
                latest.id,
                latest
                    .error_message
                    .clone()
                    .unwrap_or_else(|| latest.status.clone())
            ));
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

#[async_trait]
impl SpicedStarter for SpiceCloudSpicedStarter {
    async fn start(
        &self,
        args: &CommonArgs,
        _start_request: StartRequest,
    ) -> anyhow::Result<SpicedInstance> {
        if !args.is_external_instance() {
            return Err(anyhow::anyhow!(
                "--spiced-path must be a Flight endpoint URL when --spiced-start-mode=spice-cloud"
            ));
        }

        let base_url = spice_cloud_base_url(args);
        let token = spice_cloud_token()?;
        let spicepod = Spicepod::load_exact(args.spicepod_path.clone()).await?;
        let app_name = spicepod.name.clone();
        let spicepod_yaml = std::fs::read_to_string(&args.spicepod_path).map_err(|source| {
            anyhow::anyhow!(
                "Failed to read spicepod file at {}: {source}",
                args.spicepod_path.display()
            )
        })?;

        let client = Client::builder().timeout(Duration::from_secs(30)).build()?;

        let app_id = ensure_spice_cloud_app(&client, &base_url, &token, &app_name).await?;

        apply_spicepod_to_app(&client, &base_url, &token, app_id, &spicepod_yaml).await?;

        create_and_wait_for_deployment(
            &client,
            &base_url,
            &token,
            app_id,
            Duration::from_secs(args.ready_wait),
        )
        .await?;

        println!(
            "Spice Cloud deployment ready for app '{app_name}'. Connecting to Flight endpoint: {}",
            args.spiced_path,
        );

        Ok(SpicedInstance::external(&args.spiced_path))
    }
}

fn spiced_starter(mode: SpicedStartMode) -> Box<dyn SpicedStarter + Send + Sync> {
    match mode {
        SpicedStartMode::Local => Box::new(LocalSpicedStarter),
        SpicedStartMode::SpiceCloud => Box::new(SpiceCloudSpicedStarter),
    }
}

/// Create telemetry with resource attributes known upfront.
///
/// This ensures the `SdkMeterProvider` is created with the correct resource,
/// so metrics recorded after this call will have the proper resource attributes.
#[must_use]
pub(crate) fn create_telemetry_with_resource(common: &CommonArgs, resource: Resource) -> Telemetry {
    if let Some(endpoint) = &common.otlp_endpoint {
        return Telemetry::with_otlp_resource(
            OtlpExporterConfig {
                endpoint: endpoint.clone().into(),
                headers: common.otlp_header.clone(),
                timeout: Duration::from_secs(10),
            },
            resource,
        );
    }

    Telemetry::new_with_resource(&resource, "SPICEAI_BENCHMARK_METRICS_KEY")
}

/// Build a test configuration with validation data if applicable
///
/// This is a common helper for bench, throughput, and load tests that:
/// 1. Loads the query set from args
/// 2. Applies query overrides if specified
/// 3. Adds validation data for scenario queries when validation is enabled
/// 4. Adds reference schema for validation against known good tables
///
/// # Returns
/// Tuple of (`QuerySet`, `NotStarted` builder)
pub(crate) async fn build_test_with_validation(
    args: &DatasetTestArgs,
    test_builder: NotStarted,
) -> anyhow::Result<(QuerySet, NotStarted)> {
    let query_set = args.load_query_set()?;
    let query_overrides = args
        .query_overrides
        .clone()
        .map(test_framework::queries::QueryOverrides::from);
    let queries = query_set.get_queries(query_overrides, None, None).await?;

    let mut test_builder = test_builder
        .with_query_set(queries)
        .with_query_set_type(query_set.clone())
        .with_query_overrides(query_overrides);

    // Add validation data if this is a scenario query set with validation enabled
    if args.validate
        && let Some(validation_data) =
            query_set.get_validation_data(args.scenario_query_file.as_deref())?
    {
        test_builder = test_builder.with_validation_data(validation_data);
    }

    // Add reference schema if provided for validation against known good tables
    if let Some(ref_schema) = &args.reference_schema {
        test_builder = test_builder.with_reference_schema(Some(ref_schema.clone()));
    }

    Ok((query_set, test_builder))
}

pub(crate) async fn run_or_connect_spiced(
    args: &CommonArgs,
) -> anyhow::Result<(App, SpicedInstance)> {
    let (app, mut instance) = match args.spiced_start_mode {
        SpicedStartMode::Local if args.is_external_instance() => {
            println!(
                "Connecting to external spiced instance at: {}",
                args.spiced_path
            );
            let spicepod = Spicepod::load_exact(args.spicepod_path.clone()).await?;
            let app = AppBuilder::new(spicepod.name.clone())
                .with_spicepod(spicepod)
                .build();
            let instance = SpicedInstance::external(&args.spiced_path);
            (app, instance)
        }
        _ => {
            let (app, start_request) = get_app_and_start_request(args).await?;
            let instance = start_spiced_instance(args, start_request).await?;
            (app, instance)
        }
    };
    instance
        .wait_for_ready(std::time::Duration::from_secs(args.ready_wait))
        .await?;

    Ok((app, instance))
}

pub(crate) async fn start_spiced_instance(
    args: &CommonArgs,
    start_request: StartRequest,
) -> anyhow::Result<SpicedInstance> {
    spiced_starter(args.spiced_start_mode)
        .start(args, start_request)
        .await
}

pub(crate) async fn get_app_and_start_request(
    args: &CommonArgs,
) -> anyhow::Result<(App, StartRequest)> {
    // When metrics are disabled, no Telemetry is created, so METER_PROVIDER_ONCE
    // remains unset and all metric operations are no-ops.

    let mut spicepod = Spicepod::load_exact(args.spicepod_path.clone()).await?;
    let mut app_builder = AppBuilder::new(spicepod.name.clone()).with_spicepod(spicepod.clone());

    if let Some(dependencies_root) = &args.spicepod_dependencies {
        for dependency in &spicepod.dependencies {
            let dependent_spicepod = Spicepod::load(&dependencies_root.join(dependency)).await?;
            app_builder = app_builder.with_spicepod_dependency(dependent_spicepod);
        }
    }
    // After we've loaded dependencies, remove.
    spicepod.dependencies = vec![];
    let app = app_builder.build();

    let mut start_request = StartRequest::new(args.spiced_path_buf(), from_app(app.clone()))?;

    if let Some(ref data_dir) = args.data_dir {
        start_request = start_request.with_data_dir(data_dir.clone());
    }

    // If scrape_spiced_metrics is enabled, add --metrics flag to spiced
    if args.scrape_spiced_metrics {
        start_request = start_request
            .with_additional_args(vec!["--metrics".to_string(), "0.0.0.0:9090".to_string()]);
    }

    Ok((app, start_request))
}

pub(crate) async fn env_export(args: &CommonArgs) -> anyhow::Result<()> {
    let (_, mut start_request) = get_app_and_start_request(args).await?;

    start_request.prepare()?;
    let tempdir_path = start_request.get_tempdir_path();

    println!(
        "Exported spicepod environment to: {}",
        tempdir_path.to_string_lossy()
    );

    // Wait for input before exiting
    println!("Press Enter to exit...");
    std::io::stdin().read_line(&mut String::new())?;

    Ok(())
}

#[macro_export]
macro_rules! wait_test_and_memory {
    ($test:expr, $memory_token:expr, $memory_readings:expr) => {
        match $test.wait().await {
            Ok(test) => test,
            Err(e) => {
                observe_memory($memory_token, $memory_readings).await?;
                return Err(e);
            }
        }
    };
}

/// Process and display metrics from the spiced metrics scraper
///
/// # Arguments
/// * `scraper` - Optional metrics scraper to stop and process
/// * `emit_to_telemetry` - Whether to emit metrics to OpenTelemetry
/// * `attributes` - Optional attributes to attach to emitted metrics (e.g., test name)
///
/// # Returns
/// The collected `SpicedMetrics` if scraper was present, None otherwise
pub(crate) async fn process_spiced_metrics(
    scraper: Option<crate::spiced_metrics::MetricsScraper>,
    emit_to_telemetry: bool,
    attributes: &[test_framework::opentelemetry::KeyValue],
) -> Option<crate::spiced_metrics::SpicedMetrics> {
    let scraper = scraper?;

    match scraper.stop().await {
        Ok(metrics) => {
            println!("\n{}", vec!["="; 30].join(""));
            println!("Spiced Runtime Metrics:");
            println!("{}", vec!["="; 30].join(""));

            // Display and optionally emit key metrics
            // Note: Prometheus exporter appends _total to counter metrics
            if let Some(query_count) = metrics.get_counter_value("query_executions_total") {
                println!("Total Queries Executed: {query_count}");

                if emit_to_telemetry {
                    crate::metrics::SPICED_QUERY_COUNT.record(query_count, attributes);
                }
            }

            if let Some(cache_hits) = metrics.get_counter_value("results_cache_hits_total")
                && let Some(cache_requests) =
                    metrics.get_counter_value("results_cache_requests_total")
                && cache_requests > 0.0
            {
                let hit_rate = cache_hits / cache_requests;
                println!("Cache Hit Rate: {:.2}%", hit_rate * 100.0);

                if emit_to_telemetry {
                    crate::metrics::SPICED_CACHE_HIT_RATE.record(hit_rate, attributes);
                }
            }

            if let Some(active_conns) = metrics.get_gauge_max("query_active_count") {
                println!("Peak Active Connections: {active_conns}");

                if emit_to_telemetry {
                    crate::metrics::SPICED_ACTIVE_CONNECTIONS.record(active_conns, attributes);
                }
            }

            println!("{}", vec!["="; 30].join(""));
            Some(metrics)
        }
        Err(e) => {
            println!("Warning: Failed to collect spiced metrics: {e}");
            None
        }
    }
}
