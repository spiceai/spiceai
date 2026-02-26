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

use arrow::datatypes::DataType;
use async_trait::async_trait;
use spice_cloud_client::CloudClient;
use spicepod::acceleration::{Acceleration, Mode, RefreshMode};
use spicepod::component::ComponentOrReference;
use spicepod::component::catalog::Catalog;
use spicepod::component::dataset::Dataset;
use spicepod::component::runtime::{Runtime, TelemetryConfig};
use spicepod::param::Params;
use spicepod::spec::SpicepodDefinition;
use system_adapter_protocol::{
    AdbcDriver, DatasetConfig, EtlSinkType, Handler, IngestionMetrics, MetricsResponse,
    ResourceMetrics, Server, SetupResponse, TeardownResponse,
};
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
    region: Option<String>,
    endpoint: Option<String>,
    sink_type: Option<EtlSinkType>,
}

impl SetupConfig {
    fn from_metadata(metadata: &HashMap<String, serde_json::Value>) -> Self {
        Self {
            region: metadata_string(metadata, "etl_region"),
            endpoint: metadata_string(metadata, "etl_endpoint"),
            sink_type: None,
        }
    }

    fn set_etl_sink_type(mut self, sink_type: Option<EtlSinkType>) -> Self {
        self.sink_type = sink_type;
        self
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
        etl_sink_type: Option<EtlSinkType>,
    ) -> Result<SetupResponse, String> {
        eprintln!(
            "[stdio] setup: run_id={run_id}, metadata_keys={:?}",
            metadata.keys().collect::<Vec<_>>()
        );

        let setup_config = SetupConfig::from_metadata(&metadata).set_etl_sink_type(etl_sink_type);

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
            catalog_namespace: etl_sink_type
                .as_ref()
                .filter(|t| matches!(t, EtlSinkType::Adbc))
                .map(|_| "spicebench.bench".to_string()),
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

async fn post_setup_sink_action(
    setup_config: &SetupConfig,
    datasets: &HashMap<String, DatasetConfig>,
    cname: &str,
    api_key: &str,
) -> anyhow::Result<()> {
    if setup_config.sink_type == Some(EtlSinkType::Adbc) {
        eprintln!("[stdio] Executing post-setup actions for ADBC sink...");

        let create_table_statements = generate_adbc_create_table_statements(datasets)?;
        if create_table_statements.is_empty() {
            eprintln!("[stdio] No datasets configured for ADBC sink, skipping table creation");
            return Ok(());
        }

        let sql_url = format!("https://{cname}.spiceai.io/v1/sql");
        let sql_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()?;

        for statement in create_table_statements {
            eprintln!("[stdio] Running post-setup SQL: {statement}");
            let response = sql_client
                .post(&sql_url)
                .header("X-API-Key", api_key)
                .body(statement.clone())
                .send()
                .await?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "<failed to read error body>".to_string());
                return Err(anyhow::anyhow!(
                    "Failed to execute post-setup SQL against {sql_url}: status={status}, sql={statement}, body={body}"
                ));
            }
        }

        eprintln!("[stdio] ADBC post-setup table creation complete");
    } else {
        eprintln!(
            "[stdio] No ETL sink type specified or ETL sink requires no additional steps, skipping post-setup actions"
        );
    }
    Ok(())
}

fn generate_adbc_create_table_statements(
    datasets: &HashMap<String, DatasetConfig>,
) -> anyhow::Result<Vec<String>> {
    let mut dataset_names = datasets.keys().cloned().collect::<Vec<_>>();
    dataset_names.sort_unstable();

    dataset_names
        .into_iter()
        .map(|dataset_name| {
            let dataset = datasets
                .get(&dataset_name)
                .ok_or_else(|| anyhow::anyhow!("Dataset '{dataset_name}' was not found"))?;
            generate_adbc_create_table_statement(&dataset_name, dataset)
        })
        .collect()
}

fn generate_adbc_create_table_statement(
    dataset_name: &str,
    dataset: &DatasetConfig,
) -> anyhow::Result<String> {
    let quoted_dataset_name = quote_identifier(dataset_name);

    let column_definitions = dataset
        .schema
        .fields()
        .iter()
        .map(|field| {
            let column_name = quote_identifier(field.name());
            let data_type = adbc_sql_type_for_arrow(field.data_type())?;
            let nullable = if field.is_nullable() { "" } else { " NOT NULL" };
            Ok(format!("{column_name} {data_type}{nullable}"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    if column_definitions.is_empty() {
        return Err(anyhow::anyhow!(
            "Dataset '{dataset_name}' has no columns; cannot generate CREATE TABLE statement"
        ));
    }

    Ok(format!(
        "CREATE TABLE spicebench.bench.{quoted_dataset_name} ({})",
        column_definitions.join(", ")
    ))
}

fn adbc_sql_type_for_arrow(data_type: &DataType) -> anyhow::Result<String> {
    let sql_type = match data_type {
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 => "TINYINT".to_string(),
        DataType::Int16 => "SMALLINT".to_string(),
        DataType::Int32 => "INT".to_string(),
        DataType::Int64 => "BIGINT".to_string(),
        DataType::UInt8 => "TINYINT UNSIGNED".to_string(),
        DataType::UInt16 => "SMALLINT UNSIGNED".to_string(),
        DataType::UInt32 => "INT UNSIGNED".to_string(),
        DataType::UInt64 => "BIGINT UNSIGNED".to_string(),
        DataType::Float16 | DataType::Float32 => "FLOAT".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => format!("DECIMAL({precision}, {scale})"),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "TEXT".to_string(),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "BLOB".to_string(),
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Time32(_) | DataType::Time64(_) => "TIME".to_string(),
        DataType::Timestamp(_, _) => "TIMESTAMP".to_string(),
        _ => {
            return Err(anyhow::anyhow!(
                "Unsupported Arrow type for ADBC sink table creation: {data_type:?}"
            ));
        }
    };

    Ok(sql_type)
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
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

    post_setup_sink_action(setup_config, datasets, &cname, &api_key).await?;

    Ok(RunState {
        app_id,
        api_key,
        flight_url,
        cloud,
    })
}

fn generate_hive_spicepod(
    run_id: &Uuid,
    setup_config: &SetupConfig,
    datasets: &HashMap<String, DatasetConfig>,
) -> anyhow::Result<String> {
    let run_id_str = run_id.to_string();
    let short_id = run_id_str.split('-').next().unwrap_or_default();
    let region = setup_config
        .region
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
        if let Some(endpoint) = &setup_config.endpoint {
            param_map.insert("s3_endpoint".to_string(), endpoint.clone());
        }

        let mut dataset = Dataset::new(from, dataset_name.as_str());
        dataset.params = Some(Params::from_string_map(param_map));
        dataset.acceleration = Some(Acceleration {
            enabled: true,
            engine: Some("cayenne".to_string()),
            mode: Mode::File,
            refresh_mode: Some(RefreshMode::Full),
            refresh_check_interval: Some("1s".to_string()),
            ..Acceleration::default()
        });

        spicepod
            .datasets
            .push(ComponentOrReference::Component(dataset));
    }

    yaml::to_string(&spicepod).map_err(|e| anyhow::anyhow!("Failed to serialize spicepod: {e}"))
}

fn generate_adbc_spicepod(run_id: &Uuid) -> anyhow::Result<String> {
    let run_id_str = run_id.to_string();
    let short_id = run_id_str.split('-').next().unwrap_or_default();

    let mut spicepod = SpicepodDefinition::new(format!("spidapter-{short_id}"));
    spicepod.runtime = Runtime {
        telemetry: TelemetryConfig {
            enabled: false,
            ..TelemetryConfig::default()
        },
        ..Runtime::default()
    };

    spicepod
        .catalogs
        .push(ComponentOrReference::Component(Catalog::new(
            "cayenne".to_string(),
            "spicebench".to_string(),
        )));
    yaml::to_string(&spicepod).map_err(|e| anyhow::anyhow!("Failed to serialize spicepod: {e}"))
}

/// Generate the spicepod YAML with individual dataset entries sourced from S3 parquet files.
fn generate_initial_spicepod(
    run_id: &Uuid,
    setup_config: &SetupConfig,
    datasets: &HashMap<String, DatasetConfig>,
) -> anyhow::Result<String> {
    match setup_config.sink_type {
        Some(EtlSinkType::Adbc) => generate_adbc_spicepod(run_id),
        _ => generate_hive_spicepod(run_id, setup_config, datasets),
    }
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
        assert_eq!(config.region.as_deref(), Some("us-west-2"));
    }

    #[test]
    fn generate_spicepod_includes_dataset_entries() {
        let setup_config = SetupConfig {
            region: Some("us-west-2".to_string()),
            endpoint: Some("http://localhost:9000".to_string()),
            sink_type: None,
        };

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let datasets = HashMap::from([(
            "my_table".to_string(),
            DatasetConfig {
                schema,
                location: Some("s3://bucket/path/my_table/".to_string()),
                primary_key_columns: Vec::new(),
                time_column: None,
                partition_columns: Vec::new(),
            },
        )]);

        let spicepod = generate_initial_spicepod(&Uuid::nil(), &setup_config, &datasets)
            .expect("spicepod should generate");

        assert!(spicepod.contains("from: \"s3://bucket/path/my_table/\""));
        assert!(spicepod.contains("name: my_table"));
        assert!(spicepod.contains("file_format: parquet"));
        assert!(spicepod.contains("s3_region: us-west-2"));
        assert!(spicepod.contains("s3_endpoint: \"http://localhost:9000\""));
        assert!(spicepod.contains("engine: cayenne"));
        assert!(spicepod.contains("mode: file"));
        assert!(spicepod.contains("refresh_mode: full"));
        assert!(spicepod.contains("refresh_check_interval: 1s"));
        assert!(spicepod.contains("telemetry:"));
        assert!(spicepod.contains("enabled: false"));
    }

    #[test]
    fn generate_spicepod_errors_on_missing_dataset_source() {
        let setup_config = SetupConfig {
            region: None,
            endpoint: None,
            sink_type: None,
        };

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let datasets = HashMap::from([(
            "missing_table".to_string(),
            DatasetConfig {
                schema,
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
    fn adbc_create_table_statement_uses_namespace_and_types() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("price", DataType::Decimal128(10, 2), true),
            Field::new(
                "created_at",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                true,
            ),
        ]));

        let statement = generate_adbc_create_table_statement(
            "orders",
            &DatasetConfig {
                schema,
                location: Some("s3://bucket/path/orders/".to_string()),
                primary_key_columns: vec!["id".to_string()],
                time_column: None,
                partition_columns: Vec::new(),
            },
        )
        .expect("statement should generate");

        assert!(statement.contains("CREATE TABLE IF NOT EXISTS spicebench.bench.\"orders\""));
        assert!(statement.contains("\"id\" BIGINT NOT NULL"));
        assert!(statement.contains("\"name\" TEXT"));
        assert!(statement.contains("\"price\" DECIMAL(10, 2)"));
        assert!(statement.contains("\"created_at\" TIMESTAMP"));
        assert!(statement.contains("PRIMARY KEY (\"id\")"));
    }

    #[test]
    fn adbc_create_table_statement_errors_for_unsupported_type() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "metadata",
            DataType::Struct(vec![Field::new("k", DataType::Utf8, true)].into()),
            true,
        )]));

        let err = generate_adbc_create_table_statement(
            "events",
            &DatasetConfig {
                schema,
                location: Some("s3://bucket/path/events/".to_string()),
                primary_key_columns: Vec::new(),
                time_column: None,
                partition_columns: Vec::new(),
            },
        )
        .expect_err("unsupported Arrow type should fail");

        assert!(
            err.to_string().contains("Unsupported Arrow type"),
            "unexpected error: {err}"
        );
    }
}
