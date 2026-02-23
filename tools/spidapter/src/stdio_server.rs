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

use async_trait::async_trait;
use spice_cloud_client::CloudClient;
use spicepod::component::ComponentOrReference;
use spicepod::component::catalog::Catalog as SpicepodCatalog;
use spicepod::component::dataset::Dataset;
use spicepod::component::runtime::{Runtime, TelemetryConfig};
use spicepod::param::Params;
use spicepod::spec::SpicepodDefinition;
use system_adapter_protocol::{
    AdbcDriver, DatasetConfig, Handler, IngestionMetrics, MetricsResponse, ResourceMetrics, Server,
    SetupResponse, TeardownResponse,
};
use test_framework::anyhow;
use uuid::Uuid;

use crate::args::StdioArgs;
use crate::commands;

/// State for an active benchmark run provisioned via `setup`.
struct RunState {
    /// Spice Cloud app ID.
    app_id: i64,
    /// API key for the app (used for Flight SQL authentication).
    api_key: String,
    /// Flight SQL endpoint URL derived from the cname.
    flight_url: String,
    /// Cloud client used during provisioning (reused for teardown).
    cloud: CloudClient,
}

#[derive(Debug, Clone)]
struct SetupConfig {
    /// Per-dataset `from` URIs, keyed by dataset name.
    etl_region: Option<String>,
    etl_endpoint: Option<String>,
}

impl SetupConfig {
    fn from_metadata(metadata: &HashMap<String, serde_json::Value>) -> Self {
        Self {
            etl_region: metadata_string(metadata, "etl_region"),
            etl_endpoint: metadata_string(metadata, "etl_endpoint"),
        }
    }
}

fn metadata_string(metadata: &HashMap<String, serde_json::Value>, key: &str) -> Option<String> {
    metadata
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToString::to_string)
}

/// System adapter handler that provisions Spice Cloud apps.
struct SpidapterHandler {
    /// Active runs keyed by run ID.
    runs: HashMap<Uuid, RunState>,
    /// Spice Cloud API URL override (from CLI args).
    api_url_override: Option<String>,
    /// Timeout in seconds for deployment readiness.
    ready_wait: u64,
    /// Release channel for the spice.ai runtime image.
    channel: Option<String>,
}

impl SpidapterHandler {
    fn new(args: &StdioArgs) -> Self {
        Self {
            runs: HashMap::new(),
            api_url_override: args.spice_cloud_api_url.clone(),
            ready_wait: args.ready_wait,
            channel: args.channel.clone(),
        }
    }
}

#[async_trait]
impl Handler for SpidapterHandler {
    async fn setup(
        &mut self,
        run_id: Uuid,
        metadata: HashMap<String, serde_json::Value>,
        datasets: HashMap<String, DatasetConfig>,
    ) -> Result<SetupResponse, String> {
        eprintln!(
            "[stdio] setup: run_id={run_id}, metadata_keys={:?}",
            metadata.keys().collect::<Vec<_>>()
        );

        let setup_config = SetupConfig::from_metadata(&metadata);

        let state = provision_spice_cloud_app(
            run_id,
            self.api_url_override.as_deref(),
            self.ready_wait,
            self.channel.as_deref(),
            &setup_config,
            &datasets,
        )
        .await
        .map_err(|e| format!("Setup failed: {e}"))?;

        let response = SetupResponse {
            driver: AdbcDriver::Flightsql,
            db_kwargs: HashMap::from([
                (
                    "uri".to_string(),
                    serde_json::Value::String(state.flight_url.clone()),
                ),
                (
                    "username".to_string(),
                    serde_json::Value::String(String::new()),
                ),
                (
                    "password".to_string(),
                    serde_json::Value::String(state.api_key.clone()),
                ),
            ]),
        };

        self.runs.insert(run_id, state);
        Ok(response)
    }

    async fn metrics(&mut self, run_id: Uuid) -> std::result::Result<MetricsResponse, String> {
        let state = self
            .runs
            .get(&run_id)
            .ok_or_else(|| format!("No active run found for {run_id}"))?;

        let cloud_metrics = state
            .cloud
            .get_app_metrics(state.app_id)
            .await
            .map_err(|e| format!("Failed to fetch metrics: {e}"))?;

        let pods = cloud_metrics.metrics.values().collect::<Vec<_>>();

        let resource = if pods.is_empty() {
            ResourceMetrics::default()
        } else {
            let avg_cpu = match pods
                .iter()
                .filter_map(|p| p.cpu_usage_percent.is_some().then_some(1.0))
                .sum::<f64>()
            {
                0.0 => None,
                n => Some(pods.iter().filter_map(|p| p.cpu_usage_percent).sum::<f64>() / n),
            };

            let total_memory = match pods
                .iter()
                .filter_map(|p| p.memory_usage_bytes.is_some().then_some(1_u64))
                .sum::<u64>()
            {
                0 => None,
                n => Some(
                    pods.iter()
                        .filter_map(|p| p.memory_usage_bytes)
                        .sum::<u64>()
                        / n,
                ),
            };
            ResourceMetrics {
                cpu_usage_percent: avg_cpu,
                memory_usage_bytes: total_memory,
                disk_read_bytes: None,
                disk_write_bytes: None,
                disk_read_iops: None,
                disk_write_iops: None,
            }
        };

        let ingestion = cloud_metrics
            .ingestion
            .map(|i| IngestionMetrics {
                rows_ingested: i.rows_ingested,
                bytes_ingested: i.bytes_ingested,
                ..IngestionMetrics::default()
            })
            .unwrap_or_default();

        Ok(MetricsResponse {
            resource,
            ingestion,
        })
    }

    async fn teardown(&mut self, run_id: Uuid) -> Result<TeardownResponse, String> {
        eprintln!("[stdio] teardown: run_id={run_id}");

        let Some(state) = self.runs.remove(&run_id) else {
            eprintln!("[stdio] teardown: run_id={run_id} not found (already torn down?)");
            return Ok(TeardownResponse { ok: true });
        };

        eprintln!(
            "[stdio] teardown: deleting app {} at {}",
            state.app_id,
            state.cloud.base_url()
        );

        commands::delete_app(&state.cloud, state.app_id)
            .await
            .map_err(|e| format!("Failed to delete app {}: {e}", state.app_id))?;

        eprintln!("[stdio] teardown: app {} deleted", state.app_id);
        Ok(TeardownResponse { ok: true })
    }
}

pub async fn run_stdio_server(args: &StdioArgs) -> anyhow::Result<()> {
    let handler = SpidapterHandler::new(args);
    let mut server = Server::new(handler);
    server
        .run_stdio()
        .await
        .map_err(|e| anyhow::anyhow!("Stdio server error: {e}"))
}

// ── Spice Cloud provisioning ─────────────────────────────────────────

/// Provision a Spice Cloud app from the setup request.
///
/// Follows the same flow as `SpiceCloudSpicedStarter::start()`:
/// 1. Resolve the default cname / region
/// 2. Create or find the SCP app
/// 3. Generate and upload the spicepod from the dataset configs
/// 4. Set secrets (RUNNER + any env-based secrets)
/// 5. Create a deployment
/// 6. Wait for the deployment to become ready
async fn provision_spice_cloud_app(
    run_id: Uuid,
    api_url_override: Option<&str>,
    ready_wait: u64,
    channel: Option<&str>,
    setup_config: &SetupConfig,
    datasets: &HashMap<String, DatasetConfig>,
) -> anyhow::Result<RunState> {
    let cloud = commands::build_cloud_client(api_url_override)?;

    let cname = commands::resolve_default_cname(&cloud).await?;
    let flight_url = commands::flight_url_from_cname(&cname);
    let run_id_str = run_id.to_string();
    let short_id = run_id_str.split('-').next().unwrap_or_default();
    let app_name = commands::sanitize_app_name(&format!("spidapter-{short_id}"));

    eprintln!("[stdio] Spice Cloud API: {}", cloud.base_url());
    eprintln!("[stdio] Region cname: {cname}");
    eprintln!("[stdio] Flight endpoint: {flight_url}");
    eprintln!("[stdio] App name: {app_name}");

    let (app_id, app_api_key) = commands::ensure_spice_cloud_app(&cloud, &app_name).await?;

    let api_key = app_api_key.ok_or_else(|| {
        anyhow::anyhow!("Spice Cloud did not return an API key for app '{app_name}'")
    })?;

    eprintln!("[stdio] App ID: {app_id}");

    let spicepod_yaml = generate_initial_spicepod(&run_id, setup_config, datasets)?;
    eprintln!("[stdio] Generated spicepod:\n{spicepod_yaml}");

    eprintln!("[stdio] Uploading spicepod to app...");
    commands::apply_spicepod_to_app(&cloud, app_id, &spicepod_yaml).await?;
    eprintln!("[stdio] Spicepod uploaded");

    // Set secrets from environment for any secret references in the spicepod
    eprintln!("[stdio] Setting secrets from spicepod...");
    commands::secrets::set_spicepod_secrets(&cloud, app_id, &spicepod_yaml).await?;
    eprintln!("[stdio] Spicepod secrets set");

    eprintln!("[stdio] Setting RUNNER secret...");
    commands::secrets::set_secret(&cloud, app_id, "RUNNER", "spidapter").await?;
    eprintln!("[stdio] RUNNER secret set");

    eprintln!("[stdio] Creating deployment...");
    commands::create_deployment(&cloud, app_id, channel).await?;

    let poll_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(600))
        .build()?;
    commands::wait_for_deployment_ready(
        &poll_client,
        &cname,
        &api_key,
        Duration::from_secs(ready_wait),
    )
    .await?;

    eprintln!("[stdio] Spice Cloud deployment ready for app '{app_name}' at {flight_url}");

    // Execute CREATE TABLE statements to create cayenne-backed tables from S3 sources.
    let sql_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3600))
        .build()?;
    for (dataset_name, _config) in datasets {
        let sql = generate_create_table_sql(CAYENNE_CATALOG_NAME, dataset_name);
        eprintln!("[stdio] Executing: {sql}");
        execute_sql(&sql_client, &cname, &api_key, &sql).await?;
        eprintln!("[stdio] Created cayenne table '{dataset_name}'");
    }

    Ok(RunState {
        app_id,
        api_key,
        flight_url,
        cloud,
    })
}

/// Name of the cayenne catalog registered in the spicepod.
const CAYENNE_CATALOG_NAME: &str = "cayenne";

/// Generate the spicepod YAML with a cayenne catalog and S3-sourced federated datasets.
///
/// The datasets are registered as plain S3 sources (no acceleration). After deployment,
/// `CREATE TABLE cayenne.{table} AS SELECT * FROM {table}` DDL statements are executed
/// to create cayenne-backed tables that ingest data from these S3 sources.
fn generate_initial_spicepod(
    run_id: &Uuid,
    setup_config: &SetupConfig,
    datasets: &HashMap<String, DatasetConfig>,
) -> anyhow::Result<String> {
    let run_id_str = run_id.to_string();
    let short_id = run_id_str.split('-').next().unwrap_or_default();
    let region = setup_config
        .etl_region
        .clone()
        .or_else(|| std::env::var("AWS_REGION").ok())
        .or_else(|| std::env::var("AWS_DEFAULT_REGION").ok())
        .unwrap_or_else(|| "us-east-1".to_string());

    let mut spicepod = SpicepodDefinition::new(format!("spidapter-{short_id}"));
    spicepod.runtime = Runtime {
        telemetry: TelemetryConfig {
            enabled: false,
            ..TelemetryConfig::default()
        },
        ..Runtime::default()
    };

    // Add a cayenne catalog for local acceleration via DDL.
    let cayenne_catalog =
        SpicepodCatalog::new("cayenne".to_string(), CAYENNE_CATALOG_NAME.to_string());
    spicepod
        .catalogs
        .push(ComponentOrReference::Component(cayenne_catalog));

    for (dataset_name, config) in datasets {
        let from = config.location.as_deref().ok_or_else(|| {
            anyhow::anyhow!("Dataset '{dataset_name}' is missing a 'from' URI in its config")
        })?;

        let mut param_map = HashMap::from([
            ("file_format".to_string(), "parquet".to_string()),
            ("s3_auth".to_string(), "public".to_string()),
            ("s3_region".to_string(), region.clone()),
            ("hive_partitioning_enabled".to_string(), "true".to_string()),
        ]);
        if let Some(endpoint) = &setup_config.etl_endpoint {
            param_map.insert("s3_endpoint".to_string(), endpoint.clone());
        }

        let mut dataset = Dataset::new(from, dataset_name.as_str());
        dataset.params = Some(Params::from_string_map(param_map));

        spicepod
            .datasets
            .push(ComponentOrReference::Component(dataset));
    }

    yaml::to_string(&spicepod).map_err(|e| anyhow::anyhow!("Failed to serialize spicepod: {e}"))
}

/// Generate a `CREATE TABLE` DDL statement for a cayenne catalog table.
///
/// Produces `CREATE TABLE {catalog}.{table} AS SELECT * FROM {table}` which creates
/// a cayenne-backed table and ingests data from the identically-named S3 source dataset.
fn generate_create_table_sql(catalog_name: &str, table_name: &str) -> String {
    format!("CREATE TABLE {catalog_name}.{table_name} AS SELECT * FROM {table_name}")
}

/// Execute a SQL statement against the deployed Spice Cloud instance.
async fn execute_sql(
    client: &reqwest::Client,
    cname: &str,
    api_key: &str,
    sql: &str,
) -> anyhow::Result<()> {
    let sql_url = format!("https://{cname}.spiceai.io/v1/sql");
    let response = client
        .post(&sql_url)
        .header("X-API-Key", api_key)
        .body(sql.to_string())
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!(
            "SQL execution failed (HTTP {status}): {body}"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn setup_config_parses_metadata() {
        let metadata = HashMap::from([(
            "etl_region".to_string(),
            serde_json::Value::String("us-west-2".to_string()),
        )]);

        let config = SetupConfig::from_metadata(&metadata);
        assert_eq!(config.etl_region.as_deref(), Some("us-west-2"));
    }

    #[test]
    fn generate_spicepod_includes_cayenne_catalog_and_datasets() {
        let setup_config = SetupConfig {
            etl_region: Some("us-west-2".to_string()),
            etl_endpoint: Some("http://localhost:9000".to_string()),
        };

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let datasets = HashMap::from([(
            "my_table".to_string(),
            DatasetConfig {
                schema: schema.clone(),
                location: Some("s3://bucket/path/my_table/".to_string()),
                primary_key_columns: Vec::new(),
                time_column: None,
                partition_columns: Vec::new(),
            },
        )]);

        let spicepod = generate_initial_spicepod(&Uuid::nil(), &setup_config, &datasets)
            .expect("spicepod should generate");

        // Cayenne catalog should be present
        assert!(
            spicepod.contains("from: cayenne"),
            "missing cayenne catalog from"
        );
        assert!(
            spicepod.contains("name: cayenne"),
            "missing cayenne catalog name"
        );

        // S3 dataset should be present without acceleration
        assert!(spicepod.contains("from: \"s3://bucket/path/my_table/\""));
        assert!(spicepod.contains("name: my_table"));
        assert!(spicepod.contains("file_format: parquet"));
        assert!(spicepod.contains("s3_region: us-west-2"));
        assert!(spicepod.contains("s3_endpoint: \"http://localhost:9000\""));
        assert!(
            !spicepod.contains("engine: cayenne"),
            "should not have per-dataset acceleration"
        );
        assert!(
            !spicepod.contains("mode: file"),
            "should not have acceleration mode"
        );
        assert!(
            !spicepod.contains("refresh_mode:"),
            "should not have refresh_mode"
        );

        // Telemetry should be disabled
        assert!(spicepod.contains("telemetry:"));
        assert!(spicepod.contains("enabled: false"));
    }

    #[test]
    fn generate_spicepod_errors_on_missing_dataset_source() {
        let setup_config = SetupConfig {
            etl_region: None,
            etl_endpoint: None,
        };

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let datasets = HashMap::from([(
            "missing_table".to_string(),
            DatasetConfig {
                schema: schema.clone(),
                location: None,
                primary_key_columns: Vec::new(),
                time_column: None,
                partition_columns: Vec::new(),
            },
        )]);

        let err = generate_initial_spicepod(&Uuid::nil(), &setup_config, &datasets)
            .expect_err("missing source should fail");
        assert!(
            err.to_string().contains("missing_table"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn generate_create_table_produces_correct_sql() {
        let sql = generate_create_table_sql("cayenne", "orders");
        assert_eq!(sql, "CREATE TABLE cayenne.orders AS SELECT * FROM orders");
    }
}
