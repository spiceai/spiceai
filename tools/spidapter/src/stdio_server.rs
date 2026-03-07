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

use std::collections::{HashMap, HashSet};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command as StdCommand, Stdio};
use std::time::Duration;

use tokio::process::{Child, Command as TokioCommand};

use arrow::datatypes::DataType;
use async_trait::async_trait;
use spice_cloud_client::CloudClient;
use spicepod::acceleration::{Acceleration, Mode, RefreshMode};
use spicepod::component::ComponentOrReference;
use spicepod::component::access::AccessMode;
use spicepod::component::catalog::Catalog;
use spicepod::component::dataset::Dataset;
use spicepod::component::runtime::{
    ApiKey, ApiKeyAuth, Auth, Flight, Runtime, Scheduler, TelemetryConfig,
};
use spicepod::param::Params;
use spicepod::spec::SpicepodDefinition;
use system_adapter_protocol::{
    AdbcDriver, DatasetConfig, EtlSinkType, Handler, IngestionMetrics, MetricsResponse,
    ResourceMetrics, Server, SetupResponse, TeardownResponse,
};
use tokio::time::sleep;
use uuid::Uuid;

use crate::args::StdioArgs;
use crate::commands;

const SPIDAPTER_BACKEND_ENV: &str = "SPIDAPTER_BACKEND";
const LOCAL_BIND_HOST: &str = "0.0.0.0";
const LOCAL_CONNECT_HOST: &str = "127.0.0.1";
const LOCAL_SPICED_BINARY: &str = "spiced";
const LOCAL_SPICE_BINARY: &str = "spice";

/// State for an active benchmark run provisioned via `setup`.
enum RunState {
    Scp {
        /// Spice Cloud app ID.
        app_id: i64,
        /// API key for the app (used for Flight SQL authentication).
        api_key: String,
        /// Flight SQL endpoint URL derived from the cname.
        flight_url: String,
        /// Cloud client used during provisioning (reused for teardown).
        cloud: CloudClient,
    },
    Local(Box<LocalRunState>),
}

impl RunState {
    fn flight_url(&self) -> &str {
        match self {
            Self::Scp { flight_url, .. } => flight_url.as_str(),
            Self::Local(state) => state.flight_url.as_str(),
        }
    }

    fn password(&self) -> &str {
        match self {
            Self::Scp { api_key, .. } => api_key.as_str(),
            Self::Local(_) => "",
        }
    }
}

struct LocalRunState {
    scheduler_child: Child,
    executor_child: Child,
    flight_url: String,
    flight_api_key: Option<String>,
    sql_url: String,
    working_dir: PathBuf,
}

impl Drop for LocalRunState {
    fn drop(&mut self) {
        let _ = self.executor_child.start_kill();
        let _ = self.scheduler_child.start_kill();
    }
}

#[derive(Debug, Clone, Copy)]
enum BackendMode {
    Scp,
    Local,
}

impl BackendMode {
    fn from_env() -> Result<Self, String> {
        let raw = match std::env::var(SPIDAPTER_BACKEND_ENV) {
            Ok(value) => value,
            Err(std::env::VarError::NotPresent) => return Ok(Self::Scp),
            Err(err) => {
                return Err(format!("Failed to read {SPIDAPTER_BACKEND_ENV}: {err}"));
            }
        };

        parse_backend_mode(&raw)
    }
}

fn parse_backend_mode(raw_value: &str) -> Result<BackendMode, String> {
    match raw_value.trim().to_ascii_lowercase().as_str() {
        "" | "scp" => Ok(BackendMode::Scp),
        "local" => Ok(BackendMode::Local),
        value => Err(format!(
            "Invalid {SPIDAPTER_BACKEND_ENV} value '{value}'. Supported values: scp, local"
        )),
    }
}

#[derive(Debug, Clone)]
struct SetupConfig {
    /// Per-dataset `from` URIs, keyed by dataset name.
    region: Option<String>,
    endpoint: Option<String>,
    sink_type: Option<EtlSinkType>,
    state_location: Option<String>,
}

impl SetupConfig {
    fn from_metadata(metadata: &HashMap<String, serde_json::Value>) -> Self {
        Self {
            region: metadata_string(metadata, "etl_region"),
            endpoint: metadata_string(metadata, "etl_endpoint"),
            sink_type: None,
            state_location: metadata_string(metadata, "scheduler_state_location"),
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

/// Average non-`None` `f64` values extracted from pods.
#[expect(clippy::cast_precision_loss)]
fn avg_opt(
    pods: &[&spice_cloud_client::types::PodMetrics],
    f: fn(&spice_cloud_client::types::PodMetrics) -> Option<f64>,
) -> Option<f64> {
    let (sum, count) = pods.iter().fold((0.0, 0_u64), |(s, c), p| {
        if let Some(v) = f(p) {
            (s + v, c + 1)
        } else {
            (s, c)
        }
    });
    (count > 0).then_some(sum / (count as f64))
}

/// Sum non-`None` `u64` values extracted from pods.
fn sum_opt_u64(
    pods: &[&spice_cloud_client::types::PodMetrics],
    f: fn(&spice_cloud_client::types::PodMetrics) -> Option<u64>,
) -> Option<u64> {
    let (sum, any) = pods.iter().fold((0_u64, false), |(s, any), p| {
        if let Some(v) = f(p) {
            (s.saturating_add(v), true)
        } else {
            (s, any)
        }
    });
    any.then_some(sum)
}

/// Sum non-`None` `f64` values and convert to `u64`.
#[expect(clippy::cast_sign_loss)]
#[expect(clippy::cast_possible_truncation)]
fn sum_opt_f64_as_u64(
    pods: &[&spice_cloud_client::types::PodMetrics],
    f: fn(&spice_cloud_client::types::PodMetrics) -> Option<f64>,
) -> Option<u64> {
    let (sum, any) = pods.iter().fold((0.0_f64, false), |(s, any), p| {
        if let Some(v) = f(p) {
            (s + v, true)
        } else {
            (s, any)
        }
    });
    any.then_some(sum.round() as u64)
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
        let backend = BackendMode::from_env()?;

        let state = match backend {
            BackendMode::Scp => {
                provision_spice_cloud_app(
                    run_id,
                    self.api_url_override.as_deref(),
                    self.ready_wait,
                    self.channel.as_deref(),
                    &setup_config,
                    &datasets,
                )
                .await
            }
            BackendMode::Local => {
                provision_local_spiced_cluster(
                    run_id,
                    Duration::from_secs(self.ready_wait),
                    &setup_config,
                    &datasets,
                )
                .await
            }
        }
        .map_err(|e| format!("Setup failed: {e}"))?;

        let mut db_kwargs = HashMap::from([
            (
                "uri".to_string(),
                serde_json::Value::String(state.flight_url().to_string()),
            ),
            (
                "username".to_string(),
                serde_json::Value::String(String::new()),
            ),
            (
                "password".to_string(),
                serde_json::Value::String(state.password().to_string()),
            ),
        ]);

        if let RunState::Local(local_state) = &state
            && let Some(api_key) = &local_state.flight_api_key
        {
            db_kwargs.insert(
                "adbc.flight.sql.rpc.call_header.authorization".to_string(),
                serde_json::Value::String(format!("Bearer {api_key}")),
            );
        }

        let response = SetupResponse {
            driver: AdbcDriver::Flightsql,
            db_kwargs,
            catalog_namespace: etl_sink_type
                .as_ref()
                .filter(|sink_type| matches!(sink_type, EtlSinkType::Adbc))
                .map(|_| "spicebench.bench".to_string()),
            read_driver: None,
        };

        self.runs.insert(run_id, state);
        Ok(response)
    }

    async fn metrics(&mut self, run_id: Uuid) -> std::result::Result<MetricsResponse, String> {
        let state = self
            .runs
            .get(&run_id)
            .ok_or_else(|| format!("No active run found for {run_id}"))?;

        match state {
            RunState::Scp { app_id, cloud, .. } => {
                let cloud_metrics = cloud
                    .get_app_metrics(*app_id, None)
                    .await
                    .map_err(|e| format!("Failed to fetch metrics: {e}"))?;

                let pods = cloud_metrics.metrics.values().collect::<Vec<_>>();

                let resource = if pods.is_empty() {
                    ResourceMetrics::default()
                } else {
                    ResourceMetrics {
                        cpu_usage_percent: avg_opt(&pods, |p| p.cpu_usage_percent),
                        memory_usage_bytes: sum_opt_u64(&pods, |p| p.memory_usage_bytes),
                        disk_read_bytes: sum_opt_f64_as_u64(&pods, |p| p.disk_read_bytes),
                        disk_write_bytes: sum_opt_f64_as_u64(&pods, |p| p.disk_write_bytes),
                        disk_read_iops: sum_opt_f64_as_u64(&pods, |p| p.disk_read_operations),
                        disk_write_iops: sum_opt_f64_as_u64(&pods, |p| p.disk_write_operations),
                    }
                };

                let ingestion = cloud_metrics
                    .ingestion
                    .map(|ingestion| IngestionMetrics {
                        rows_ingested: ingestion.rows_ingested,
                        bytes_ingested: ingestion.bytes_ingested,
                        ..IngestionMetrics::default()
                    })
                    .unwrap_or_default();

                Ok(MetricsResponse {
                    resource,
                    ingestion,
                })
            }
            RunState::Local(_) => Ok(MetricsResponse {
                resource: ResourceMetrics::default(),
                ingestion: IngestionMetrics::default(),
            }),
        }
    }

    async fn teardown(&mut self, run_id: Uuid) -> Result<TeardownResponse, String> {
        eprintln!("[stdio] teardown: run_id={run_id}");

        let Some(state) = self.runs.remove(&run_id) else {
            eprintln!("[stdio] teardown: run_id={run_id} not found (already torn down?)");
            return Ok(TeardownResponse { ok: true });
        };

        match state {
            RunState::Scp { app_id, cloud, .. } => {
                eprintln!(
                    "[stdio] teardown: deleting app {app_id} at {}",
                    cloud.base_url()
                );
                commands::delete_app(&cloud, app_id)
                    .await
                    .map_err(|e| format!("Failed to delete app {app_id}: {e}"))?;
                eprintln!("[stdio] teardown: app {app_id} deleted");
            }
            RunState::Local(mut local_state) => {
                teardown_local_run(&mut local_state)
                    .await
                    .map_err(|e| format!("Failed to teardown local run {run_id}: {e}"))?;
            }
        }

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
    sql_url: &str,
    api_key: Option<&str>,
) -> anyhow::Result<()> {
    if setup_config.sink_type == Some(EtlSinkType::Adbc) {
        eprintln!("[stdio] Executing post-setup actions for ADBC sink...");

        let create_table_statements = generate_adbc_create_table_statements(datasets)?;
        if create_table_statements.is_empty() {
            eprintln!("[stdio] No datasets configured for ADBC sink, skipping table creation");
            return Ok(());
        }

        let sql_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()?;

        for statement in create_table_statements {
            eprintln!("[stdio] Running post-setup SQL: {statement}");

            let mut attempts = 0;

            loop {
                let mut request = sql_client.post(sql_url).body(statement.clone());
                if let Some(key) = api_key {
                    request = request.header("X-API-Key", key);
                }
                let response = request.send().await?;

                if response.status().is_success() {
                    break;
                }

                attempts += 1;

                if attempts >= 3 {
                    let status = response.status();
                    let body = response
                        .text()
                        .await
                        .unwrap_or_else(|e| format!("<failed to read error response body: {e}>"));
                    return Err(anyhow::anyhow!(
                        "Failed to execute post-setup SQL against {sql_url}: status={status}, sql={statement}, body={body}"
                    ));
                }

                let backoff_seconds = attempts * 2;
                eprintln!(
                    "[stdio] Post-setup SQL failed, retrying in {backoff_seconds}s (attempt {attempts}/3)"
                );
                sleep(Duration::from_secs(backoff_seconds)).await;
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

    let mut table_elements = column_definitions;
    if !dataset.primary_key_columns.is_empty() {
        let schema_columns = dataset
            .schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<HashSet<_>>();

        for primary_key_column in &dataset.primary_key_columns {
            if !schema_columns.contains(primary_key_column) {
                return Err(anyhow::anyhow!(
                    "Dataset '{dataset_name}' has primary key column '{primary_key_column}' that is not present in the schema"
                ));
            }
        }

        let primary_keys = dataset
            .primary_key_columns
            .iter()
            .map(|column| quote_identifier(column))
            .collect::<Vec<_>>()
            .join(", ");
        table_elements.push(format!("PRIMARY KEY ({primary_keys})"));
    }

    Ok(format!(
        "CREATE TABLE spicebench.bench.{quoted_dataset_name} ({})",
        table_elements.join(", ")
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

    let spicepod = generate_initial_spicepod(&run_id, setup_config, datasets, None)?;
    let spicepod_yaml = serialize_spicepod(&spicepod)?;
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

    let sql_url = format!("https://{cname}.spiceai.io/v1/sql");
    post_setup_sink_action(setup_config, datasets, &sql_url, Some(&api_key)).await?;

    Ok(RunState::Scp {
        app_id,
        api_key,
        flight_url,
        cloud,
    })
}

#[derive(Debug, Clone, Copy)]
struct LocalPorts {
    scheduler_http: u16,
    scheduler_flight: u16,
    scheduler_node: u16,
    executor_http: u16,
    executor_flight: u16,
    executor_node: u16,
}

#[derive(Debug, Clone)]
struct LocalPkiPaths {
    ca_cert: PathBuf,
    scheduler_cert: PathBuf,
    scheduler_key: PathBuf,
    executor_cert: PathBuf,
    executor_key: PathBuf,
}

async fn provision_local_spiced_cluster(
    run_id: Uuid,
    ready_wait: Duration,
    setup_config: &SetupConfig,
    datasets: &HashMap<String, DatasetConfig>,
) -> anyhow::Result<RunState> {
    let ports = allocate_local_ports(LOCAL_BIND_HOST)?;

    let working_dir = create_local_working_dir(run_id).await?;
    let local_flight_api_key = (setup_config.sink_type == Some(EtlSinkType::Adbc))
        .then(|| format!("spidapter-local-{run_id}"));

    let setup_result = async {
        let scheduler_dir = working_dir.join("scheduler");
        let executor_dir = working_dir.join("executor-0");
        tokio::fs::create_dir_all(&scheduler_dir).await?;
        tokio::fs::create_dir_all(&executor_dir).await?;

        let spicepod = generate_initial_spicepod(
            &run_id,
            setup_config,
            datasets,
            local_flight_api_key.as_deref(),
        )?;
        let spicepod_path = write_local_spicepod(&spicepod, &working_dir).await?;

        let run_id_str = run_id.to_string();
        let short_run_id = run_id_str.split('-').next().unwrap_or_default();
        let process_id = std::process::id();
        let scheduler_cert_name = format!("spidapter-scheduler-{short_run_id}-{process_id}");
        let executor_cert_name = format!("spidapter-executor-{short_run_id}-{process_id}");

        let pki_paths = ensure_local_cluster_pki(
            LOCAL_SPICE_BINARY,
            LOCAL_CONNECT_HOST,
            &scheduler_cert_name,
            &executor_cert_name,
        )
        .await?;

        Ok::<(PathBuf, PathBuf, PathBuf, LocalPkiPaths), anyhow::Error>((
            scheduler_dir,
            executor_dir,
            spicepod_path,
            pki_paths,
        ))
    }
    .await;

    let (scheduler_dir, executor_dir, spicepod_path, pki_paths) = match setup_result {
        Ok(result) => result,
        Err(error) => {
            let _ = cleanup_local_artifacts(&working_dir).await;
            return Err(error);
        }
    };

    let scheduler_args = scheduler_spiced_args(
        LOCAL_BIND_HOST,
        LOCAL_CONNECT_HOST,
        ports,
        &pki_paths,
        spicepod_path.as_path(),
    );
    let mut scheduler_child = match spawn_local_spiced(
        LOCAL_SPICED_BINARY,
        &scheduler_dir,
        &scheduler_args,
        "scheduler",
    ) {
        Ok(child) => child,
        Err(error) => {
            let _ = cleanup_local_artifacts(&working_dir).await;
            return Err(error);
        }
    };

    let scheduler_http_url = format!("http://{}:{}", LOCAL_CONNECT_HOST, ports.scheduler_http);
    let scheduler_sql_url = format!("{scheduler_http_url}/v1/sql");

    if let Err(error) = wait_for_local_http_ready(
        &scheduler_http_url,
        &mut scheduler_child,
        ready_wait,
        "scheduler",
    )
    .await
    {
        let _ = stop_child_process(&mut scheduler_child, "scheduler").await;
        let _ = cleanup_local_artifacts(&working_dir).await;
        return Err(error);
    }

    let executor_args =
        executor_spiced_args(LOCAL_BIND_HOST, LOCAL_CONNECT_HOST, ports, &pki_paths);
    let mut executor_child = match spawn_local_spiced(
        LOCAL_SPICED_BINARY,
        &executor_dir,
        &executor_args,
        "executor",
    ) {
        Ok(child) => child,
        Err(error) => {
            let _ = stop_child_process(&mut scheduler_child, "scheduler").await;
            let _ = cleanup_local_artifacts(&working_dir).await;
            return Err(error);
        }
    };

    if let Err(error) = wait_for_local_sql_ready(
        &scheduler_sql_url,
        &mut scheduler_child,
        &mut executor_child,
        ready_wait,
        local_flight_api_key.as_deref(),
    )
    .await
    {
        let _ = stop_child_process(&mut executor_child, "executor").await;
        let _ = stop_child_process(&mut scheduler_child, "scheduler").await;
        let _ = cleanup_local_artifacts(&working_dir).await;
        return Err(error);
    }

    if let Err(error) = post_setup_sink_action(
        setup_config,
        datasets,
        &scheduler_sql_url,
        local_flight_api_key.as_deref(),
    )
    .await
    {
        let _ = stop_child_process(&mut executor_child, "executor").await;
        let _ = stop_child_process(&mut scheduler_child, "scheduler").await;
        let _ = cleanup_local_artifacts(&working_dir).await;
        return Err(error);
    }

    Ok(RunState::Local(Box::new(LocalRunState {
        scheduler_child,
        executor_child,
        flight_url: format!("grpc://{}:{}", LOCAL_CONNECT_HOST, ports.scheduler_flight),
        flight_api_key: local_flight_api_key,
        sql_url: scheduler_sql_url,
        working_dir,
    })))
}

async fn create_local_working_dir(run_id: Uuid) -> anyhow::Result<PathBuf> {
    let run_dir = std::env::temp_dir().join(format!("spidapter-local-{run_id}"));
    if tokio::fs::metadata(&run_dir).await.is_ok() {
        tokio::fs::remove_dir_all(&run_dir).await?;
    }
    tokio::fs::create_dir_all(&run_dir).await?;
    Ok(run_dir)
}

async fn write_local_spicepod(
    spicepod: &SpicepodDefinition,
    working_dir: &Path,
) -> anyhow::Result<PathBuf> {
    let spicepod_yaml = serialize_spicepod(spicepod)?;
    let spicepod_path = working_dir.join("spicepod.yaml");
    tokio::fs::write(&spicepod_path, spicepod_yaml).await?;
    Ok(spicepod_path)
}

fn serialize_spicepod(spicepod: &SpicepodDefinition) -> anyhow::Result<String> {
    yaml::to_string(spicepod).map_err(|e| anyhow::anyhow!("Failed to serialize spicepod: {e}"))
}

fn allocate_local_ports(host: &str) -> anyhow::Result<LocalPorts> {
    Ok(LocalPorts {
        scheduler_http: reserve_local_port(host)?,
        scheduler_flight: reserve_local_port(host)?,
        scheduler_node: reserve_local_port(host)?,
        executor_http: reserve_local_port(host)?,
        executor_flight: reserve_local_port(host)?,
        executor_node: reserve_local_port(host)?,
    })
}

fn reserve_local_port(host: &str) -> anyhow::Result<u16> {
    let listener = TcpListener::bind((host, 0))?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

fn pki_dir() -> anyhow::Result<PathBuf> {
    let home_dir = dirs::home_dir()
        .ok_or_else(|| anyhow::anyhow!("Home directory not found; cannot resolve ~/.spice/pki"))?;
    Ok(home_dir.join(".spice").join("pki"))
}

async fn ensure_local_cluster_pki(
    spice_cli_path: &str,
    host: &str,
    scheduler_cert_name: &str,
    executor_cert_name: &str,
) -> anyhow::Result<LocalPkiPaths> {
    let pki_dir = pki_dir()?;
    let ca_cert = pki_dir.join("ca.crt");
    let ca_key = pki_dir.join("ca.key");

    if !ca_cert.exists() || !ca_key.exists() {
        eprintln!("[stdio] local backend: generating cluster CA with spice cluster tls init");
        run_spice_cli_command(
            spice_cli_path,
            vec!["cluster".to_string(), "tls".to_string(), "init".to_string()],
        )
        .await?;
    }

    add_tls_certificate(spice_cli_path, scheduler_cert_name, host).await?;
    add_tls_certificate(spice_cli_path, executor_cert_name, host).await?;

    Ok(LocalPkiPaths {
        ca_cert,
        scheduler_cert: pki_dir.join(format!("{scheduler_cert_name}.crt")),
        scheduler_key: pki_dir.join(format!("{scheduler_cert_name}.key")),
        executor_cert: pki_dir.join(format!("{executor_cert_name}.crt")),
        executor_key: pki_dir.join(format!("{executor_cert_name}.key")),
    })
}

async fn add_tls_certificate(
    spice_cli_path: &str,
    cert_name: &str,
    host: &str,
) -> anyhow::Result<()> {
    let mut args = vec![
        "cluster".to_string(),
        "tls".to_string(),
        "add".to_string(),
        cert_name.to_string(),
    ];

    if !host.is_empty() {
        args.push("--host".to_string());
        args.push(host.to_string());
    }

    run_spice_cli_command(spice_cli_path, args).await
}

async fn run_spice_cli_command(binary_path: &str, args: Vec<String>) -> anyhow::Result<()> {
    let binary_path = binary_path.to_string();
    let command_display = format!("{binary_path} {}", args.join(" "));
    let command_display_for_spawn = command_display.clone();
    let args_for_spawn = args;

    let status = tokio::task::spawn_blocking(move || {
        StdCommand::new(&binary_path)
            .args(&args_for_spawn)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .status()
            .map_err(|error| {
                anyhow::anyhow!("Failed to execute '{command_display_for_spawn}': {error}")
            })
    })
    .await
    .map_err(|error| anyhow::anyhow!("Failed to join command '{command_display}': {error}"))??;

    if !status.success() {
        return Err(anyhow::anyhow!(
            "Command '{command_display}' failed with status {status}"
        ));
    }

    Ok(())
}

fn scheduler_spiced_args(
    bind_host: &str,
    advertise_host: &str,
    ports: LocalPorts,
    pki_paths: &LocalPkiPaths,
    spicepod_path: &Path,
) -> Vec<String> {
    vec![
        "--role".to_string(),
        "scheduler".to_string(),
        "--http".to_string(),
        format!("{bind_host}:{}", ports.scheduler_http),
        "--flight".to_string(),
        format!("{bind_host}:{}", ports.scheduler_flight),
        "--node-bind-address".to_string(),
        format!("{bind_host}:{}", ports.scheduler_node),
        "--node-advertise-address".to_string(),
        advertise_host.to_string(),
        "--node-mtls-ca-certificate-file".to_string(),
        pki_paths.ca_cert.display().to_string(),
        "--node-mtls-certificate-file".to_string(),
        pki_paths.scheduler_cert.display().to_string(),
        "--node-mtls-key-file".to_string(),
        pki_paths.scheduler_key.display().to_string(),
        spicepod_path.display().to_string(),
    ]
}

fn executor_spiced_args(
    bind_host: &str,
    scheduler_host: &str,
    ports: LocalPorts,
    pki_paths: &LocalPkiPaths,
) -> Vec<String> {
    vec![
        "--role".to_string(),
        "executor".to_string(),
        "--scheduler-address".to_string(),
        format!("https://{scheduler_host}:{}", ports.scheduler_node),
        "--http".to_string(),
        format!("{bind_host}:{}", ports.executor_http),
        "--flight".to_string(),
        format!("{bind_host}:{}", ports.executor_flight),
        "--node-bind-address".to_string(),
        format!("{bind_host}:{}", ports.executor_node),
        "--node-advertise-address".to_string(),
        scheduler_host.to_string(),
        "--node-mtls-ca-certificate-file".to_string(),
        pki_paths.ca_cert.display().to_string(),
        "--node-mtls-certificate-file".to_string(),
        pki_paths.executor_cert.display().to_string(),
        "--node-mtls-key-file".to_string(),
        pki_paths.executor_key.display().to_string(),
    ]
}

fn spawn_local_spiced(
    spiced_path: &str,
    current_dir: &Path,
    args: &[String],
    process_name: &str,
) -> anyhow::Result<Child> {
    eprintln!(
        "[stdio] local backend: launching {process_name} process: {spiced_path} {}",
        args.join(" ")
    );

    TokioCommand::new(spiced_path)
        .kill_on_drop(true)
        .args(args)
        .current_dir(current_dir)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|error| anyhow::anyhow!("Failed to start local {process_name} process: {error}"))
}

async fn wait_for_local_http_ready(
    http_url: &str,
    child: &mut Child,
    timeout: Duration,
    process_name: &str,
) -> anyhow::Result<()> {
    let ready_url = format!("{http_url}/health");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()?;

    let started = tokio::time::Instant::now();
    loop {
        ensure_process_is_running(child, process_name)?;

        if started.elapsed() > timeout {
            return Err(anyhow::anyhow!(
                "Timed out after {}s waiting for local {process_name} readiness at {ready_url}",
                timeout.as_secs()
            ));
        }

        match client.get(&ready_url).send().await {
            Ok(response) if response.status().is_success() => return Ok(()),
            Ok(_) | Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    }
}

async fn wait_for_local_sql_ready(
    sql_url: &str,
    scheduler_child: &mut Child,
    executor_child: &mut Child,
    timeout: Duration,
    api_key: Option<&str>,
) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()?;

    let started = tokio::time::Instant::now();
    loop {
        ensure_process_is_running(scheduler_child, "scheduler")?;
        ensure_process_is_running(executor_child, "executor")?;

        if started.elapsed() > timeout {
            return Err(anyhow::anyhow!(
                "Timed out after {}s waiting for local SQL readiness at {sql_url}",
                timeout.as_secs()
            ));
        }

        let mut request = client.post(sql_url).body("SELECT 1");
        if let Some(key) = api_key {
            request = request.header("X-API-Key", key);
        }

        match request.send().await {
            Ok(response) if response.status().is_success() => return Ok(()),
            Ok(_) | Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    }
}

fn ensure_process_is_running(child: &mut Child, process_name: &str) -> anyhow::Result<()> {
    if let Some(status) = child.try_wait()? {
        return Err(anyhow::anyhow!(
            "Local {process_name} process exited early with status {status}"
        ));
    }
    Ok(())
}

async fn teardown_local_run(local_state: &mut LocalRunState) -> anyhow::Result<()> {
    eprintln!(
        "[stdio] teardown: stopping local executor process (sql endpoint: {})",
        local_state.sql_url
    );
    stop_child_process(&mut local_state.executor_child, "executor").await?;

    eprintln!("[stdio] teardown: stopping local scheduler process");
    stop_child_process(&mut local_state.scheduler_child, "scheduler").await?;

    cleanup_local_artifacts(&local_state.working_dir).await
}

async fn stop_child_process(child: &mut Child, process_name: &str) -> anyhow::Result<()> {
    if let Some(status) = child.try_wait()? {
        eprintln!("[stdio] local backend: {process_name} already stopped with status {status}");
        return Ok(());
    }

    child.kill().await.map_err(|error| {
        anyhow::anyhow!("Failed to terminate local {process_name} process: {error}")
    })?;

    let status = child.wait().await.map_err(|error| {
        anyhow::anyhow!("Failed to wait for local {process_name} process: {error}")
    })?;

    eprintln!("[stdio] local backend: {process_name} stopped with status {status}");
    Ok(())
}

async fn cleanup_local_artifacts(working_dir: &Path) -> anyhow::Result<()> {
    if tokio::fs::metadata(working_dir).await.is_ok() {
        tokio::fs::remove_dir_all(working_dir).await?;
        eprintln!(
            "[stdio] local backend: removed artifacts in {}",
            working_dir.display()
        );
    }

    Ok(())
}

fn generate_hive_spicepod(
    run_id: &Uuid,
    setup_config: &SetupConfig,
    datasets: &HashMap<String, DatasetConfig>,
) -> anyhow::Result<SpicepodDefinition> {
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
            ..Acceleration::default()
        });

        spicepod
            .datasets
            .push(ComponentOrReference::Component(dataset));
    }

    Ok(spicepod)
}

fn generate_adbc_spicepod(run_id: &Uuid, flight_api_key: Option<&str>) -> SpicepodDefinition {
    let run_id_str = run_id.to_string();
    let short_id = run_id_str.split('-').next().unwrap_or_default();

    let mut spicepod = SpicepodDefinition::new(format!("spidapter-{short_id}"));
    spicepod.runtime = Runtime {
        telemetry: TelemetryConfig {
            enabled: false,
            ..TelemetryConfig::default()
        },
        auth: flight_api_key.map(|key| Auth {
            api_key: Some(ApiKeyAuth {
                enabled: true,
                keys: vec![ApiKey::ReadWrite {
                    key: key.to_string(),
                }],
            }),
        }),
        flight: Some(Flight {
            do_put_rate_limit_enabled: false,
            ..Flight::default()
        }),
        ..Runtime::default()
    };

    spicepod.catalogs.push(ComponentOrReference::Component(
        Catalog::new("cayenne".to_string(), "spicebench".to_string())
            .with_access(AccessMode::ReadWriteCreate),
    ));
    spicepod
}

/// Generate the initial [`SpicepodDefinition`] for the benchmark run.
fn generate_initial_spicepod(
    run_id: &Uuid,
    setup_config: &SetupConfig,
    datasets: &HashMap<String, DatasetConfig>,
    flight_api_key: Option<&str>,
) -> anyhow::Result<SpicepodDefinition> {
    let mut spicepod = match setup_config.sink_type {
        Some(EtlSinkType::Adbc) => Ok(generate_adbc_spicepod(run_id, flight_api_key)),
        _ => generate_hive_spicepod(run_id, setup_config, datasets),
    }?;

    if let Some(ref loc) = setup_config.state_location {
        spicepod.runtime.scheduler = Some(Scheduler {
            state_location: loc.clone(),
            params: Some(Params::from_string_map(HashMap::from([(
                "s3_auth".to_string(),
                "key".to_string(),
            )]))),
            partition_management: None,
        });
    }

    Ok(spicepod)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn setup_config_parses_metadata() {
        let metadata = HashMap::from([
            (
                "etl_region".to_string(),
                serde_json::Value::String("us-west-2".to_string()),
            ),
            (
                "scheduler_state_location".to_string(),
                serde_json::Value::String("s3://my-bucket/state".to_string()),
            ),
        ]);

        let config = SetupConfig::from_metadata(&metadata);
        assert_eq!(config.region.as_deref(), Some("us-west-2"));
        assert_eq!(
            config.state_location.as_deref(),
            Some("s3://my-bucket/state")
        );
    }

    #[test]
    fn backend_mode_parser_defaults_to_scp() {
        assert!(matches!(parse_backend_mode(""), Ok(BackendMode::Scp)));
        assert!(matches!(parse_backend_mode("scp"), Ok(BackendMode::Scp)));
    }

    #[test]
    fn backend_mode_parser_supports_local() {
        assert!(matches!(
            parse_backend_mode("LOCAL"),
            Ok(BackendMode::Local)
        ));
    }

    #[test]
    fn backend_mode_parser_rejects_unknown_values() {
        let error = parse_backend_mode("unexpected").expect_err("invalid backend should fail");
        assert!(
            error.contains("Invalid SPIDAPTER_BACKEND value"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn generate_spicepod_includes_dataset_entries() {
        let setup_config = SetupConfig {
            region: Some("us-west-2".to_string()),
            endpoint: Some("http://localhost:9000".to_string()),
            sink_type: None,
            state_location: Some("s3://bucket/state".to_string()),
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

        let spicepod = generate_initial_spicepod(&Uuid::nil(), &setup_config, &datasets, None)
            .expect("spicepod should generate");
        let spicepod_yaml =
            serialize_spicepod(&spicepod).expect("spicepod should serialize to YAML");

        assert!(spicepod_yaml.contains("from: \"s3://bucket/path/my_table/\""));
        assert!(spicepod_yaml.contains("name: my_table"));
        assert!(spicepod_yaml.contains("file_format: parquet"));
        assert!(spicepod_yaml.contains("s3_region: us-west-2"));
        assert!(spicepod_yaml.contains("s3_endpoint: \"http://localhost:9000\""));
        assert!(spicepod_yaml.contains("engine: cayenne"));
        assert!(spicepod_yaml.contains("mode: file"));
        assert!(spicepod_yaml.contains("refresh_mode: full"));
        assert!(spicepod_yaml.contains("telemetry:"));
        assert!(spicepod_yaml.contains("enabled: false"));
        assert!(spicepod_yaml.contains("state_location: \"s3://bucket/state\""));
        assert!(spicepod_yaml.contains("s3_auth: key"));
    }

    #[test]
    fn generate_spicepod_errors_on_missing_dataset_source() {
        let setup_config = SetupConfig {
            region: None,
            endpoint: None,
            sink_type: None,
            state_location: None,
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

        let err = generate_initial_spicepod(&Uuid::nil(), &setup_config, &datasets, None)
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

        assert!(statement.contains("CREATE TABLE spicebench.bench.\"orders\""));
        assert!(statement.contains("\"id\" BIGINT NOT NULL"));
        assert!(statement.contains("\"name\" TEXT"));
        assert!(statement.contains("\"price\" DECIMAL(10, 2)"));
        assert!(statement.contains("\"created_at\" TIMESTAMP"));
        assert!(statement.contains("PRIMARY KEY (\"id\")"));
    }

    #[test]
    fn adbc_create_table_statement_errors_when_primary_key_missing_in_schema() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let err = generate_adbc_create_table_statement(
            "events",
            &DatasetConfig {
                schema,
                location: Some("s3://bucket/path/events/".to_string()),
                primary_key_columns: vec!["missing_column".to_string()],
                time_column: None,
                partition_columns: Vec::new(),
            },
        )
        .expect_err("primary key not in schema should fail");

        assert!(
            err.to_string().contains("is not present in the schema"),
            "unexpected error: {err}"
        );
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
