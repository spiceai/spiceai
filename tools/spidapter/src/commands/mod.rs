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
pub(crate) mod secrets;
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
    ) -> anyhow::Result<(SpicedInstance, Option<String>)>;
}

struct LocalSpicedStarter;

#[async_trait]
impl SpicedStarter for LocalSpicedStarter {
    async fn start(
        &self,
        _args: &CommonArgs,
        start_request: StartRequest,
    ) -> anyhow::Result<(SpicedInstance, Option<String>)> {
        Ok((SpicedInstance::start(start_request).await?, None))
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
    println!("Deployment {} created", created.id);
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
    println!("Waiting for deployment to become ready at {sql_url}...");

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
                println!("  Deployment ready ({elapsed}s elapsed)");
                return Ok(());
            }
            Ok(response) => {
                println!("  Not ready: {} ({elapsed}s elapsed)", response.status());
            }
            Err(e) => {
                println!("  Not ready: {e} ({elapsed}s elapsed)");
            }
        }

        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

#[async_trait]
impl SpicedStarter for SpiceCloudSpicedStarter {
    async fn start(
        &self,
        args: &CommonArgs,
        _start_request: StartRequest,
    ) -> anyhow::Result<(SpicedInstance, Option<String>)> {
        let base_url = spice_cloud_base_url(args.spiced_start_api_url.as_deref());
        let token = spice_cloud_token()?;
        let spicepod = Spicepod::load_exact(args.spicepod_path.clone()).await?;
        let app_name = sanitize_app_name(&spicepod.name);
        let spicepod_yaml = std::fs::read_to_string(&args.spicepod_path).map_err(|source| {
            anyhow::anyhow!(
                "Failed to read spicepod file at {}: {source}",
                args.spicepod_path.display()
            )
        })?;

        let client = Client::builder()
            .timeout(Duration::from_secs(600))
            .build()?;

        let cname = resolve_default_cname(&client, &base_url, &token).await?;
        let flight_url = flight_url_from_cname(&cname);

        println!("Spice Cloud API: {base_url}");
        println!("Region cname: {cname}");
        println!("Flight endpoint: {flight_url}");
        println!("App name: {app_name}");

        let (app_id, app_api_key) =
            ensure_spice_cloud_app(&client, &base_url, &token, &app_name).await?;

        println!("App ID: {app_id}");
        println!(
            "App API key: {}",
            if app_api_key.is_some() {
                "<present>"
            } else {
                "<not returned>"
            }
        );

        println!("Uploading spicepod to app...");
        apply_spicepod_to_app(&client, &base_url, &token, app_id, &spicepod_yaml).await?;
        println!("Spicepod uploaded");

        println!("Setting secrets from spicepod...");
        secrets::set_spicepod_secrets(&client, &base_url, &token, app_id, &spicepod_yaml).await?;
        println!("Spicepod secrets set");

        println!("Setting RUNNER secret...");
        secrets::set_secret(&client, &base_url, &token, app_id, "RUNNER", "spidapter").await?;
        println!("RUNNER secret set");

        println!("Creating deployment...");
        create_deployment(&client, &base_url, &token, app_id).await?;

        let api_key_for_poll = app_api_key.clone().ok_or_else(|| {
            anyhow::anyhow!("App API key is required to poll deployment readiness")
        })?;

        wait_for_deployment_ready(
            &client,
            &cname,
            &api_key_for_poll,
            Duration::from_secs(args.ready_wait),
        )
        .await?;

        let http_base_url = format!("https://{cname}.spiceai.io");

        println!(
            "Spice Cloud deployment ready for app '{app_name}'. Connecting to Flight endpoint: {flight_url}",
        );

        Ok((
            SpicedInstance::external_with_http(&flight_url, &http_base_url),
            app_api_key,
        ))
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
pub(crate) fn sanitize_app_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect()
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

/// Create a Flight-based query executor for the given spiced instance.
pub(crate) async fn create_query_executor(
    spiced_instance: &test_framework::spiced::SpicedInstance,
    api_key: Option<String>,
) -> anyhow::Result<Box<dyn test_framework::execution::QueryExecutor>> {
    let spice_client = spiced_instance.spice_client(api_key, false).await?;
    Ok(Box::new(test_framework::execution::FlightExecutor::new(
        std::sync::Arc::new(spice_client),
    )))
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
) -> anyhow::Result<(App, SpicedInstance, Option<String>)> {
    let (app, mut instance, api_key) = match args.spiced_start_mode {
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
            (app, instance, None)
        }
        _ => {
            let (app, start_request) = get_app_and_start_request(args).await?;
            let (instance, api_key) = start_spiced_instance(args, start_request).await?;
            (app, instance, api_key)
        }
    };

    // Skip wait_for_ready for Spice Cloud — readiness was already verified
    // by polling the SQL endpoint during deployment.
    if !matches!(args.spiced_start_mode, SpicedStartMode::SpiceCloud) {
        instance
            .wait_for_ready(std::time::Duration::from_secs(args.ready_wait))
            .await?;
    }

    Ok((app, instance, api_key))
}

pub(crate) async fn start_spiced_instance(
    args: &CommonArgs,
    start_request: StartRequest,
) -> anyhow::Result<(SpicedInstance, Option<String>)> {
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
