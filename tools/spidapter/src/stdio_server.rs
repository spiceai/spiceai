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
use spice_cloud_client::{CloudClient, types::UpdateAppRequest};
use spicepod::acceleration::{Acceleration, Mode, RefreshMode};
use spicepod::component::ComponentOrReference;
use spicepod::component::access::AccessMode;
use spicepod::component::catalog::Catalog;
use spicepod::component::dataset::Dataset;
use spicepod::component::runtime::{
    ApiKey, ApiKeyAuth, Auth, Flight, Query, Runtime, Scheduler, TelemetryConfig,
    default_max_partition_assignments_per_interval, default_max_partitions_per_executor,
    default_partition_assignment_interval, default_partition_discovery_timeout,
};
use spicepod::param::{ParamValue, Params};
use spicepod::spec::SpicepodDefinition;
use system_adapter_protocol::{
    AdbcDriver, DatasetConfig, EtlSinkType, Handler, IngestionMetrics, MetricsResponse,
    ResourceMetrics, Server, SetupResponse, TeardownResponse,
};
use tokio::time::sleep;
use uuid::Uuid;

use crate::args::StdioArgs;
use crate::commands;

const LOCAL_BIND_HOST: &str = "0.0.0.0";
const LOCAL_CONNECT_HOST: &str = "127.0.0.1";
const LOCAL_SPICED_BINARY: &str = "spiced";
const LOCAL_SPICE_BINARY: &str = "spice";
const POST_SETUP_SQL_MAX_RETRIES: u64 = 5;
const SPIDAPTER_NUM_EXECUTORS_ENV: &str = "SPIDAPTER_NUM_EXECUTORS";
const MAX_LOCAL_EXECUTORS: usize = 16;
const LOCAL_EXECUTOR_REGISTRATION_METRIC_GRACE: Duration = Duration::from_secs(15);

/// State for an active benchmark run provisioned via `setup`.
enum RunState {
    Scp {
        /// Spice Cloud app ID.
        app_id: i64,
        /// API key for the app (used for Flight SQL authentication).
        api_key: String,
        /// Flight SQL endpoint URL derived from the cname.
        flight_url: String,
        /// Normalized API base URL (stored separately from `cloud` to avoid tainted-struct logging).
        api_url: String,
        /// SQL endpoint URL for DDL execution.
        sql_url: String,
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

    fn sql_url(&self) -> &str {
        match self {
            Self::Scp { sql_url, .. } => sql_url.as_str(),
            Self::Local(state) => state.sql_url.as_str(),
        }
    }

    fn api_key(&self) -> Option<&str> {
        match self {
            Self::Scp { api_key, .. } => Some(api_key.as_str()),
            Self::Local(state) => state.flight_api_key.as_deref(),
        }
    }
}

struct LocalRunState {
    scheduler_child: Child,
    executor_children: Vec<Child>,
    flight_url: String,
    flight_api_key: Option<String>,
    sql_url: String,
    working_dir: PathBuf,
}

impl Drop for LocalRunState {
    fn drop(&mut self) {
        for child in &mut self.executor_children {
            let _ = child.start_kill();
        }
        let _ = self.scheduler_child.start_kill();
    }
}

use crate::args::BackendMode;

#[derive(Debug, Clone)]
struct SetupConfig {
    /// Per-dataset `from` URIs, keyed by dataset name.
    region: Option<String>,
    endpoint: Option<String>,
    sink_type: Option<EtlSinkType>,
    /// Absolute path to a spicepod the client wants deployed verbatim
    /// (testoperator's cluster-bench path passes this; spicebench leaves it
    /// empty and relies on the `datasets` JSON-RPC parameter instead).
    spicepod_path: Option<String>,
}

impl SetupConfig {
    fn from_metadata(metadata: &HashMap<String, serde_json::Value>) -> Self {
        Self {
            region: metadata_string(metadata, "etl_region"),
            endpoint: metadata_string(metadata, "etl_endpoint"),
            sink_type: None,
            spicepod_path: metadata_string(metadata, "spicepod_path"),
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
    /// Datasets from setup, keyed by run ID → dataset name → config.
    run_datasets: HashMap<Uuid, HashMap<String, DatasetConfig>>,
    /// Full CLI args (includes all flags and env-var-backed configuration).
    args: StdioArgs,
}

impl SpidapterHandler {
    fn new(args: &StdioArgs) -> Self {
        Self {
            runs: HashMap::new(),
            run_datasets: HashMap::new(),
            args: args.clone(),
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
        let backend = self.args.backend;

        let state = match backend {
            BackendMode::Scp => {
                provision_spice_cloud_app(run_id, &self.args, &setup_config, &datasets).await
            }
            BackendMode::Local => {
                provision_local_spiced_cluster(
                    run_id,
                    Duration::from_secs(self.args.ready_wait),
                    &setup_config,
                    &datasets,
                    &self.args,
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

        // Advertise the cluster's HTTP query endpoint so testoperator (or any
        // other client) can drive the Ballista `/v1/queries` path and the
        // `/v1/ready` probe at the correct region URL. Without this the client
        // would have to guess the URL from the Flight host, which fails for
        // region-prefixed Spice Cloud deployments.
        let mut endpoints: std::collections::HashMap<
            String,
            std::collections::HashMap<String, serde_json::Value>,
        > = std::collections::HashMap::new();
        let http_base = state.sql_url().trim_end_matches("/v1/sql").to_string();
        let mut queries_kwargs: std::collections::HashMap<String, serde_json::Value> =
            std::collections::HashMap::new();
        queries_kwargs.insert(
            "url".to_string(),
            serde_json::Value::String(format!("{http_base}/v1/queries")),
        );
        if let Some(api_key) = state.api_key() {
            queries_kwargs.insert(
                "authorization_header".to_string(),
                serde_json::Value::String(format!("Bearer {api_key}")),
            );
        }
        endpoints.insert("spice.http.v1.queries".to_string(), queries_kwargs);

        let response = SetupResponse {
            driver: AdbcDriver::Flightsql,
            db_kwargs,
            catalog_namespace: etl_sink_type
                .as_ref()
                .filter(|sink_type| matches!(sink_type, EtlSinkType::Adbc))
                .map(|_| "spicebench.bench".to_string()),
            read_driver: None,
            endpoints,
        };

        self.runs.insert(run_id, state);
        self.run_datasets.insert(run_id, datasets);

        Ok(response)
    }

    async fn metrics(
        &mut self,
        run_id: Uuid,
        _final_scrape: bool,
    ) -> std::result::Result<MetricsResponse, String> {
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
                        num_compute_nodes: None,
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
        self.run_datasets.remove(&run_id);

        match state {
            RunState::Scp {
                app_id,
                api_url,
                cloud,
                ..
            } => {
                eprintln!("[stdio] teardown: deleting app {app_id} at {api_url}");
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

    async fn create_staging_table(
        &mut self,
        run_id: Uuid,
        source_dataset: &str,
        staging_table_name: &str,
    ) -> std::result::Result<system_adapter_protocol::CreateStagingTableResponse, String> {
        let state = self
            .runs
            .get(&run_id)
            .ok_or_else(|| format!("No active run found for {run_id}"))?;
        if !self
            .run_datasets
            .get(&run_id)
            .map_or(false, |ds| ds.contains_key(source_dataset))
        {
            return Err(format!(
                "Source dataset '{source_dataset}' not found in run {run_id}"
            ));
        }

        // Use CREATE TABLE ... LIKE ... to copy schema, partition expression,
        // AND partition-to-executor assignments from the source table.
        let quoted_staging = quote_identifier(staging_table_name);
        let quoted_source = quote_identifier(source_dataset);
        let ddl = format!(
            "CREATE TABLE IF NOT EXISTS spicebench.bench.{quoted_staging} LIKE spicebench.bench.{quoted_source}"
        );

        eprintln!(
            "[stdio] create_staging_table: source={source_dataset}, staging={staging_table_name}, sql={ddl}"
        );

        execute_sql_statement(state.sql_url(), state.api_key(), &ddl)
            .await
            .map_err(|e| format!("Failed to execute staging table DDL: {e}"))?;

        Ok(system_adapter_protocol::CreateStagingTableResponse { ok: true })
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

        for statement in create_table_statements {
            eprintln!("[stdio] Running post-setup SQL: {statement}");
            execute_sql_statement(sql_url, api_key, &statement).await?;
        }

        eprintln!("[stdio] ADBC post-setup table creation complete");
    } else {
        eprintln!(
            "[stdio] No ETL sink type specified or ETL sink requires no additional steps, skipping post-setup actions"
        );
    }
    Ok(())
}

/// Execute a single SQL statement against the system's HTTP SQL endpoint.
async fn execute_sql_statement(
    sql_url: &str,
    api_key: Option<&str>,
    statement: &str,
) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()?;

    let mut attempts = 0;
    loop {
        let mut request = client.post(sql_url).body(statement.to_string());
        request = request.header("X-Accept-Rows", "999");
        if let Some(key) = api_key {
            request = request.header("X-API-Key", key);
        }
        let response = request.send().await?;

        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|e| format!("<failed to read response body: {e}>"));

        if status.is_success() {
            return Ok(());
        }

        attempts += 1;
        if attempts >= POST_SETUP_SQL_MAX_RETRIES {
            return Err(anyhow::anyhow!(
                "Failed to execute SQL against {sql_url} after {POST_SETUP_SQL_MAX_RETRIES} attempts: status={status}, sql={statement}, body={body}"
            ));
        }

        let backoff_seconds = attempts * 2;
        eprintln!(
            "[stdio] SQL execution failed (status={status}, body={body}), retrying in {backoff_seconds}s (attempt {attempts}/{POST_SETUP_SQL_MAX_RETRIES})"
        );
        sleep(Duration::from_secs(backoff_seconds)).await;
    }
}

pub(crate) fn generate_adbc_create_table_statements(
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

pub(crate) fn generate_adbc_create_table_statement(
    dataset_name: &str,
    dataset: &DatasetConfig,
) -> anyhow::Result<String> {
    let DatasetConfig {
        schema,
        primary_key_columns,
        partition_columns,
        ..
    } = dataset;
    let quoted_dataset_name = quote_identifier(dataset_name);

    let column_definitions = schema
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
    if !primary_key_columns.is_empty() {
        let schema_columns = schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<HashSet<_>>();

        for primary_key_column in primary_key_columns {
            if !schema_columns.contains(primary_key_column) {
                return Err(anyhow::anyhow!(
                    "Dataset '{dataset_name}' has primary key column '{primary_key_column}' that is not present in the schema"
                ));
            }
        }

        let primary_keys = primary_key_columns
            .iter()
            .map(|column| quote_identifier(column))
            .collect::<Vec<_>>()
            .join(", ");
        table_elements.push(format!("PRIMARY KEY ({primary_keys})"));
    }

    if partition_columns.len() > 1 {
        return Err(anyhow::anyhow!(
            "Dataset '{dataset_name}' specifies {} partition columns, but only a single partition column is supported",
            partition_columns.len()
        ));
    }

    let partition_clause = if partition_columns.is_empty() {
        String::default()
    } else {
        format!("PARTITION BY ({})", partition_columns.join(", "))
    };

    Ok(format!(
        "CREATE TABLE IF NOT EXISTS spicebench.bench.{quoted_dataset_name} ({}) {partition_clause}",
        table_elements.join(", ")
    ))
}

pub(crate) fn adbc_sql_type_for_arrow(data_type: &DataType) -> anyhow::Result<String> {
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

pub(crate) fn quote_identifier(identifier: &str) -> String {
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
    args: &StdioArgs,
    setup_config: &SetupConfig,
    datasets: &HashMap<String, DatasetConfig>,
) -> anyhow::Result<RunState> {
    let api_url = args.spice_cloud_api_url.trim_end_matches('/');
    let cloud = commands::build_cloud_client(Some(api_url), args.api_key.as_deref())?;

    let cname = commands::resolve_default_cname(&cloud).await?;
    let flight_url = args
        .flight_url
        .clone()
        .unwrap_or_else(|| commands::flight_url_from_cname(&cname));
    let run_id_str = run_id.to_string();
    let short_id = run_id_str.split('-').next().unwrap_or_default();
    let app_name = commands::sanitize_app_name(&format!("spidapter-{short_id}"));

    eprintln!("[stdio] Spice Cloud API: {api_url}");
    eprintln!("[stdio] Region cname: {cname}");
    eprintln!("[stdio] Flight endpoint: {flight_url}");
    eprintln!("[stdio] App name: {app_name}");

    let app_create_config = commands::AppCreateConfig {
        app_memory_limit: args.app_memory_limit.clone(),
        app_cpu_limit: args.app_cpu_limit.clone(),
        app_cpu_request: args.app_cpu_request.clone(),
        app_memory_request: args.app_memory_request.clone(),
        app_replicas: args.app_replicas,
        app_storage_size_gb: args.app_storage_size_gb,
        executor_replicas: args.executor_replicas,
        executor_memory_limit: args.executor_memory_limit.clone(),
        executor_cpu_limit: args.executor_cpu_limit.clone(),
        executor_cpu_request: args.executor_cpu_request.clone(),
        executor_memory_request: args.executor_memory_request.clone(),
        executor_storage_size_gb: args.executor_storage_size_gb,
        ephemeral_storage_limit_gb: args.ephemeral_storage_limit_gb.clone(),
        organization_tag: args.organization_tag.clone(),
    };
    let app_id = commands::ensure_spice_cloud_app(&cloud, &app_name, &app_create_config).await?;

    // Fetch API key from the dedicated api-keys endpoint
    let api_keys = cloud
        .get_api_keys(app_id)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to fetch API keys for app '{app_name}': {e}"))?;

    let api_key = api_keys.api_key.ok_or_else(|| {
        anyhow::anyhow!("Spice Cloud did not return an API key for app '{app_name}'")
    })?;

    eprintln!("[stdio] App ID: {app_id}");

    let spicepod = generate_initial_spicepod(&run_id, setup_config, datasets, None, args).await?;
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

    // Apply custom image configuration if any image-related overrides are provided.
    // This updates the app's image_tag/update_channel before creating the deployment,
    // so the deployment picks up the requested image version instead of the default.
    let has_custom_image = args.image_tag.is_some() || args.channel.is_some();

    if has_custom_image {
        eprintln!(
            "[stdio] Applying custom image config: tag={:?}, channel={:?}",
            args.image_tag, args.channel
        );
        cloud
            .update_app(
                app_id,
                &UpdateAppRequest {
                    image_tag: args.image_tag.clone(),
                    update_channel: args.channel.as_ref().map(ToString::to_string),
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
    commands::create_deployment(&cloud, app_id, args.channel.as_ref()).await?;

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

    // Wait for executors to connect before declaring the deployment ready.
    // Executors should know to create missing tables when they join: https://github.com/spiceai/spiceai/issues/9848
    let _expected_executors: u64 = std::env::var("SPIDAPTER_NUM_EXECUTORS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    let executor_wait_timeout = std::env::var("SPIDAPTER_DEPLOYMENT_READY_WAIT")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(120);

    // after the deployment is reported "ready", wait for another 10 seconds (or SPIDAPTER_DEPLOYMENT_READY_WAIT seconds if set)
    // not all executors may be connected yet. executors should know to create missing tables when they join: https://github.com/spiceai/spiceai/issues/9848
    eprintln!(
        "[stdio] Deployment is ready, waiting an additional {executor_wait_timeout}s for executors to connect..."
    );
    tokio::time::sleep(Duration::from_secs(executor_wait_timeout)).await;
    // wait_for_scp_executor_count(&cloud, app_id, expected_executors, Duration::from_secs(executor_wait_timeout)).await;

    eprintln!("[stdio] Spice Cloud deployment ready for app '{app_name}' at {flight_url}");

    let sql_url = format!("https://{cname}.spiceai.io/v1/sql");
    post_setup_sink_action(setup_config, datasets, &sql_url, Some(&api_key)).await?;

    Ok(RunState::Scp {
        app_id,
        api_key,
        flight_url,
        api_url: api_url.to_owned(),
        sql_url,
        cloud,
    })
}

#[derive(Debug, Clone)]
struct LocalPorts {
    scheduler_http: u16,
    scheduler_flight: u16,
    scheduler_node: u16,
    /// Per-executor ports: (http, flight, node)
    executor_ports: Vec<(u16, u16, u16)>,
}

#[derive(Debug, Clone)]
struct LocalPkiPaths {
    ca_cert: PathBuf,
    scheduler_cert: PathBuf,
    scheduler_key: PathBuf,
    /// Per-executor PKI: (cert, key)
    executor_pki: Vec<(PathBuf, PathBuf)>,
}

async fn provision_local_spiced_cluster(
    run_id: Uuid,
    ready_wait: Duration,
    setup_config: &SetupConfig,
    datasets: &HashMap<String, DatasetConfig>,
    args: &StdioArgs,
) -> anyhow::Result<RunState> {
    let num_exec = num_executors()?;
    eprintln!("[stdio] local backend: provisioning cluster with {num_exec} executor(s)");
    let ports = allocate_local_ports(LOCAL_BIND_HOST, num_exec)?;

    let working_dir = create_local_working_dir(run_id).await?;
    let local_flight_api_key = (setup_config.sink_type == Some(EtlSinkType::Adbc))
        .then(|| format!("spidapter-local-{run_id}"));

    let setup_result = async {
        let scheduler_dir = working_dir.join("scheduler");
        tokio::fs::create_dir_all(&scheduler_dir).await?;

        let mut executor_dirs = Vec::with_capacity(num_exec);
        for i in 0..num_exec {
            let dir = working_dir.join(format!("executor-{i}"));
            tokio::fs::create_dir_all(&dir).await?;
            executor_dirs.push(dir);
        }

        let spicepod = generate_initial_spicepod(
            &run_id,
            setup_config,
            datasets,
            local_flight_api_key.as_deref(),
            args,
        )
        .await?;
        let spicepod_path = write_local_spicepod(&spicepod, &working_dir, "spicepod.yaml").await?;

        let run_id_str = run_id.to_string();
        let short_run_id = run_id_str.split('-').next().unwrap_or_default();
        let process_id = std::process::id();
        let scheduler_cert_name = format!("spidapter-scheduler-{short_run_id}-{process_id}");
        let executor_cert_names: Vec<String> = (0..num_exec)
            .map(|i| format!("spidapter-executor{i}-{short_run_id}-{process_id}"))
            .collect();

        let pki_paths = ensure_local_cluster_pki(
            LOCAL_SPICE_BINARY,
            LOCAL_CONNECT_HOST,
            &scheduler_cert_name,
            &executor_cert_names,
        )
        .await?;

        Ok::<(PathBuf, Vec<PathBuf>, PathBuf, LocalPkiPaths), anyhow::Error>((
            scheduler_dir,
            executor_dirs,
            spicepod_path,
            pki_paths,
        ))
    }
    .await;

    let (scheduler_dir, executor_dirs, spicepod_path, pki_paths) = match setup_result {
        Ok(result) => result,
        Err(error) => {
            let _ = cleanup_local_artifacts(&working_dir).await;
            return Err(error);
        }
    };

    let scheduler_args = scheduler_spiced_args(
        LOCAL_BIND_HOST,
        LOCAL_CONNECT_HOST,
        &ports,
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

    let executor_http_urls = ports
        .executor_ports
        .iter()
        .take(num_exec)
        .map(|ports| format!("http://{}:{}", LOCAL_CONNECT_HOST, ports.0))
        .collect::<Vec<_>>();

    let mut executor_children = Vec::with_capacity(num_exec);
    for (i, executor_dir) in executor_dirs.iter().enumerate().take(num_exec) {
        let (executor_cert, executor_key) = &pki_paths.executor_pki[i];
        let executor_args = executor_spiced_args(
            LOCAL_BIND_HOST,
            LOCAL_CONNECT_HOST,
            ports.scheduler_node,
            ports.executor_ports[i],
            &pki_paths.ca_cert,
            executor_cert,
            executor_key,
        );
        let label = format!("executor-{i}");
        match spawn_local_spiced(LOCAL_SPICED_BINARY, executor_dir, &executor_args, &label) {
            Ok(child) => executor_children.push(child),
            Err(error) => {
                for child in &mut executor_children {
                    let _ = stop_child_process(child, "executor").await;
                }
                let _ = stop_child_process(&mut scheduler_child, "scheduler").await;
                let _ = cleanup_local_artifacts(&working_dir).await;
                return Err(error);
            }
        }
    }

    // Wait for at least the first executor to be SQL-reachable
    if let Err(error) = wait_for_local_sql_ready(
        &scheduler_sql_url,
        &mut scheduler_child,
        &mut executor_children[0],
        ready_wait,
        local_flight_api_key.as_deref(),
    )
    .await
    {
        for child in &mut executor_children {
            let _ = stop_child_process(child, "executor").await;
        }
        let _ = stop_child_process(&mut scheduler_child, "scheduler").await;
        let _ = cleanup_local_artifacts(&working_dir).await;
        return Err(error);
    }

    if num_exec > 1 {
        let remaining = num_exec.saturating_sub(1);
        eprintln!(
            "[stdio] local backend: waiting for remaining {remaining} executor(s) to register with the scheduler..."
        );
        if let Err(error) = wait_for_local_executor_count(
            &scheduler_http_url,
            &executor_http_urls,
            &mut scheduler_child,
            &mut executor_children,
            num_exec,
            ready_wait,
        )
        .await
        {
            for child in &mut executor_children {
                let _ = stop_child_process(child, "executor").await;
            }
            let _ = stop_child_process(&mut scheduler_child, "scheduler").await;
            let _ = cleanup_local_artifacts(&working_dir).await;
            return Err(error);
        }
    }

    if let Err(error) = post_setup_sink_action(
        setup_config,
        datasets,
        &scheduler_sql_url,
        local_flight_api_key.as_deref(),
    )
    .await
    {
        for child in &mut executor_children {
            let _ = stop_child_process(child, "executor").await;
        }
        let _ = stop_child_process(&mut scheduler_child, "scheduler").await;
        let _ = cleanup_local_artifacts(&working_dir).await;
        return Err(error);
    }

    Ok(RunState::Local(Box::new(LocalRunState {
        scheduler_child,
        executor_children,
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
    file_name: &str,
) -> anyhow::Result<PathBuf> {
    let spicepod_yaml = serialize_spicepod(spicepod)?;
    let spicepod_path = working_dir.join(file_name);
    tokio::fs::write(&spicepod_path, spicepod_yaml).await?;
    Ok(spicepod_path)
}

fn serialize_spicepod(spicepod: &SpicepodDefinition) -> anyhow::Result<String> {
    yaml::to_string(spicepod).map_err(|e| anyhow::anyhow!("Failed to serialize spicepod: {e}"))
}

fn num_executors() -> anyhow::Result<usize> {
    match std::env::var(SPIDAPTER_NUM_EXECUTORS_ENV) {
        Ok(raw) => {
            let parsed = raw.trim().parse::<usize>().map_err(|error| {
                anyhow::anyhow!(
                    "Invalid {SPIDAPTER_NUM_EXECUTORS_ENV} value '{raw}': {error}. Expected an integer in the range 1..={MAX_LOCAL_EXECUTORS}."
                )
            })?;

            if !(1..=MAX_LOCAL_EXECUTORS).contains(&parsed) {
                anyhow::bail!(
                    "Invalid {SPIDAPTER_NUM_EXECUTORS_ENV} value '{parsed}'. Supported range for the local backend is 1..={MAX_LOCAL_EXECUTORS}."
                );
            }

            Ok(parsed)
        }
        Err(std::env::VarError::NotPresent) => Ok(1),
        Err(std::env::VarError::NotUnicode(_)) => anyhow::bail!(
            "Invalid {SPIDAPTER_NUM_EXECUTORS_ENV} value: expected valid UTF-8 in the range 1..={MAX_LOCAL_EXECUTORS}."
        ),
    }
}

fn allocate_local_ports(host: &str, num_executors: usize) -> anyhow::Result<LocalPorts> {
    let mut executor_ports = Vec::with_capacity(num_executors);
    for _ in 0..num_executors {
        executor_ports.push((
            reserve_local_port(host)?,
            reserve_local_port(host)?,
            reserve_local_port(host)?,
        ));
    }
    Ok(LocalPorts {
        scheduler_http: reserve_local_port(host)?,
        scheduler_flight: reserve_local_port(host)?,
        scheduler_node: reserve_local_port(host)?,
        executor_ports,
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
    executor_cert_names: &[String],
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
    let mut executor_pki = Vec::with_capacity(executor_cert_names.len());
    for executor_cert_name in executor_cert_names {
        add_tls_certificate(spice_cli_path, executor_cert_name, host).await?;
        executor_pki.push((
            pki_dir.join(format!("{executor_cert_name}.crt")),
            pki_dir.join(format!("{executor_cert_name}.key")),
        ));
    }

    Ok(LocalPkiPaths {
        ca_cert,
        scheduler_cert: pki_dir.join(format!("{scheduler_cert_name}.crt")),
        scheduler_key: pki_dir.join(format!("{scheduler_cert_name}.key")),
        executor_pki,
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
    ports: &LocalPorts,
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
    scheduler_node_port: u16,
    executor_ports: (u16, u16, u16),
    ca_cert: &Path,
    executor_cert: &Path,
    executor_key: &Path,
) -> Vec<String> {
    vec![
        "--role".to_string(),
        "executor".to_string(),
        "--scheduler-address".to_string(),
        format!("https://{scheduler_host}:{scheduler_node_port}"),
        "--http".to_string(),
        format!("{bind_host}:{}", executor_ports.0),
        "--flight".to_string(),
        format!("{bind_host}:{}", executor_ports.1),
        "--node-bind-address".to_string(),
        format!("{bind_host}:{}", executor_ports.2),
        "--node-advertise-address".to_string(),
        scheduler_host.to_string(),
        "--node-mtls-ca-certificate-file".to_string(),
        ca_cert.display().to_string(),
        "--node-mtls-certificate-file".to_string(),
        executor_cert.display().to_string(),
        "--node-mtls-key-file".to_string(),
        executor_key.display().to_string(),
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

    let current_stderr = std::io::stderr();

    TokioCommand::new(spiced_path)
        .kill_on_drop(true)
        .args(args)
        .current_dir(current_dir)
        .stdout(Stdio::from(current_stderr))
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

/// Polls the Spice Cloud metrics API until the expected number of executors have connected,
/// or the timeout expires. On timeout, logs a warning and returns (non-fatal).
#[expect(dead_code)]
async fn wait_for_scp_executor_count(
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

async fn wait_for_local_executor_count(
    scheduler_http_url: &str,
    executor_http_urls: &[String],
    scheduler_child: &mut Child,
    executor_children: &mut [Child],
    expected_count: usize,
    timeout: Duration,
) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()?;
    let metrics_url = format!("{scheduler_http_url}/metrics");
    let started = tokio::time::Instant::now();

    loop {
        ensure_process_is_running(scheduler_child, "scheduler")?;
        for (idx, child) in executor_children.iter_mut().enumerate() {
            ensure_process_is_running(child, &format!("executor-{idx}"))?;
        }

        if started.elapsed() > timeout {
            return Err(anyhow::anyhow!(
                "Timed out after {}s waiting for {expected_count} local executors to register via {metrics_url}",
                timeout.as_secs()
            ));
        }

        if let Ok(response) = client.get(&metrics_url).send().await
            && response.status().is_success()
            && let Ok(body) = response.text().await
            && scheduler_active_executor_count(&body)
                .is_some_and(|active_count| active_count >= expected_count)
        {
            return Ok(());
        }

        if started.elapsed() >= LOCAL_EXECUTOR_REGISTRATION_METRIC_GRACE
            && all_local_executor_http_ready(&client, executor_http_urls).await
        {
            eprintln!(
                "[stdio] local backend: scheduler executor-count metric unavailable; falling back to per-executor health checks"
            );
            return Ok(());
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn all_local_executor_http_ready(
    client: &reqwest::Client,
    executor_http_urls: &[String],
) -> bool {
    for executor_http_url in executor_http_urls {
        let ready_url = format!("{executor_http_url}/health");
        match client.get(&ready_url).send().await {
            Ok(response) if response.status().is_success() => {}
            Ok(_) | Err(_) => return false,
        }
    }

    true
}

fn scheduler_active_executor_count(metrics_body: &str) -> Option<usize> {
    metrics_body
        .lines()
        .filter(|line| line.starts_with("scheduler_active_executors_count"))
        .filter_map(|line| line.split_whitespace().last())
        .filter_map(|value| value.parse::<usize>().ok())
        .max()
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
        "[stdio] teardown: stopping {} local executor process(es) (sql endpoint: {})",
        local_state.executor_children.len(),
        local_state.sql_url
    );
    for (i, child) in local_state.executor_children.iter_mut().enumerate() {
        stop_child_process(child, &format!("executor-{i}")).await?;
    }

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

/// Read a spicepod YAML file from disk and rename it to the per-run app name
/// before deploying. Used by testoperator's cluster-bench path: the client
/// passes a `spicepod_path` in setup metadata; we deploy whatever's there
/// (datasets, runtime config, etc.) verbatim, only overriding the `name`.
async fn load_spicepod_from_path(path: &str, run_id: &Uuid) -> anyhow::Result<SpicepodDefinition> {
    let contents = tokio::fs::read_to_string(path)
        .await
        .map_err(|e| anyhow::anyhow!("failed to read spicepod from `{path}`: {e}"))?;
    parse_and_rename_spicepod(&contents, run_id)
        .map_err(|e| anyhow::anyhow!("failed to parse spicepod YAML at `{path}`: {e}"))
}

fn parse_and_rename_spicepod(yaml_str: &str, run_id: &Uuid) -> anyhow::Result<SpicepodDefinition> {
    let mut spicepod: SpicepodDefinition = yaml::from_str(yaml_str)?;
    let run_id_str = run_id.to_string();
    let short_id = run_id_str.split('-').next().unwrap_or_default();
    spicepod.name = format!("spidapter-{short_id}");
    Ok(spicepod)
}

fn generate_hive_spicepod(
    run_id: &Uuid,
    setup_config: &SetupConfig,
    datasets: &HashMap<String, DatasetConfig>,
    aws_region: Option<&str>,
) -> anyhow::Result<SpicepodDefinition> {
    let run_id_str = run_id.to_string();
    let short_id = run_id_str.split('-').next().unwrap_or_default();
    let region = setup_config
        .region
        .clone()
        .or_else(|| aws_region.map(ToString::to_string))
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
            if endpoint.starts_with("http://") {
                param_map.insert("allow_http".to_string(), "true".to_string());
            }
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

fn generate_adbc_spicepod(
    run_id: &Uuid,
    flight_api_key: Option<&str>,
    args: &StdioArgs,
) -> SpicepodDefinition {
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
            oidc: None,
        }),
        flight: Some(Flight {
            do_put_rate_limit_enabled: false,
            ..Flight::default()
        }),
        query: Some(Query {
            memory_limit: args
                .query_memory_limit
                .clone()
                .or(Some("150Gi".to_string())),
            ..Query::default()
        }),
        ..Runtime::default()
    };

    let mut cayenne_catalog = Catalog::new("cayenne".to_string(), "spicebench".to_string())
        .with_access(AccessMode::ReadWriteCreate);

    let mut params_map = HashMap::new();

    if let Some(cayenne_data_dir) = &args
        .cayenne_data_dir
        .clone()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
    {
        params_map.insert("cayenne_data_dir".to_string(), cayenne_data_dir.clone());
    }

    if let Some(cayenne_metadata_dir) = &args
        .cayenne_metadata_dir
        .clone()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
    {
        params_map.insert(
            "cayenne_metadata_dir".to_string(),
            cayenne_metadata_dir.clone(),
        );
    }

    if !params_map.is_empty() {
        cayenne_catalog.params = Some(Params::from_string_map(params_map));
    }

    spicepod
        .catalogs
        .push(ComponentOrReference::Component(cayenne_catalog));
    spicepod
}

/// Generate the initial [`SpicepodDefinition`] for the benchmark run.
async fn generate_initial_spicepod(
    run_id: &Uuid,
    setup_config: &SetupConfig,
    datasets: &HashMap<String, DatasetConfig>,
    flight_api_key: Option<&str>,
    args: &StdioArgs,
) -> anyhow::Result<SpicepodDefinition> {
    let scheduler_state_location = args.scheduler_state_location.as_deref();
    let aws_region = args.aws_region.as_deref();

    // If the client supplied a spicepod path in setup metadata
    // (testoperator's cluster-bench path), use the contents of that file
    // verbatim as the deployable spicepod and only layer scheduler config on
    // top. The spicebench `datasets` HashMap path is only consulted when no
    // spicepod_path is provided.
    let mut spicepod = if let Some(path) = setup_config.spicepod_path.as_deref() {
        load_spicepod_from_path(path, run_id).await?
    } else {
        match setup_config.sink_type {
            Some(EtlSinkType::Adbc) => Ok(generate_adbc_spicepod(run_id, flight_api_key, args)),
            _ => generate_hive_spicepod(run_id, setup_config, datasets, aws_region),
        }?
    };

    if let Some(loc) = scheduler_state_location {
        let mut path = loc.trim();
        if let Some(p) = path.strip_suffix('/') {
            path = p;
        }
        let state_location = format!("{path}/{run_id}");
        if !path.is_empty() {
            // Wire `s3_key`/`s3_secret` through `${secrets:...}` references
            // so the runtime's cluster-secret lookup resolves them against
            // the AWS_*-named secrets `set_spicepod_secrets` already uploads.
            // Without these references, the scheduler's S3 client asks the
            // cluster secret store for literal keys `s3_key`/`s3_secret` and
            // fails ("Secret not found").
            let mut sched = Scheduler {
                state_location,
                params: Some(Params::from_string_map(HashMap::from([
                    ("s3_auth".to_string(), "key".to_string()),
                    (
                        "s3_key".to_string(),
                        "${secrets:AWS_ACCESS_KEY_ID}".to_string(),
                    ),
                    (
                        "s3_secret".to_string(),
                        "${secrets:AWS_SECRET_ACCESS_KEY}".to_string(),
                    ),
                ]))),
                partition_assignment_interval: default_partition_assignment_interval(),
                max_partition_assignments_per_interval:
                    default_max_partition_assignments_per_interval(),
                max_partitions_per_executor: default_max_partitions_per_executor(),
                partition_discovery_timeout: default_partition_discovery_timeout(),
            };

            if let Some(region) = aws_region {
                sched.params.as_mut().map(|p| {
                    p.data.insert(
                        "s3_region".to_string(),
                        ParamValue::String(region.to_string()),
                    )
                });
            }
            spicepod.runtime.scheduler = Some(sched);
        }
    }

    Ok(spicepod)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use clap::ValueEnum;
    use std::sync::Arc;

    fn test_stdio_args() -> StdioArgs {
        StdioArgs {
            verbose: false,
            spice_cloud_api_url: "https://api.spice.ai".to_string(),
            ready_wait: 600,
            channel: None,
            image_tag: None,
            api_key: None,
            backend: BackendMode::Scp,
            flight_url: None,
            app_memory_limit: None,
            app_cpu_limit: None,
            app_cpu_request: None,
            app_memory_request: None,
            app_replicas: None,
            executor_replicas: 1,
            executor_memory_limit: None,
            executor_cpu_limit: None,
            executor_cpu_request: None,
            executor_memory_request: None,
            app_storage_size_gb: None,
            executor_storage_size_gb: None,
            scheduler_state_location: Some("s3://bucket/state".to_string()),
            aws_region: None,
            cayenne_data_dir: None,
            cayenne_metadata_dir: None,
            ephemeral_storage_limit_gb: None,
            organization_tag: None,
            query_memory_limit: None,
        }
    }

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
    fn backend_mode_parser_defaults_to_scp() {
        assert!(matches!(
            BackendMode::from_str("scp", true),
            Ok(BackendMode::Scp)
        ));
    }

    #[test]
    fn backend_mode_parser_supports_local() {
        assert!(matches!(
            BackendMode::from_str("local", true),
            Ok(BackendMode::Local)
        ));
    }

    #[test]
    fn backend_mode_parser_rejects_unknown_values() {
        BackendMode::from_str("unexpected", true)
            .expect_err("unknown backend mode should be rejected");
    }

    #[tokio::test]
    async fn generate_spicepod_includes_dataset_entries() {
        let setup_config = SetupConfig {
            region: Some("us-west-2".to_string()),
            endpoint: Some("http://localhost:9000".to_string()),
            sink_type: None,
            spicepod_path: None,
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

        let args = test_stdio_args();
        let spicepod =
            generate_initial_spicepod(&Uuid::nil(), &setup_config, &datasets, None, &args)
                .await
                .expect("spicepod should generate");
        let spicepod_yaml =
            serialize_spicepod(&spicepod).expect("spicepod should serialize to YAML");

        assert!(spicepod_yaml.contains("from: \"s3://bucket/path/my_table/\""));
        assert!(spicepod_yaml.contains("name: my_table"));
        assert!(spicepod_yaml.contains("file_format: parquet"));
        assert!(spicepod_yaml.contains("s3_region: us-west-2"));
        assert!(spicepod_yaml.contains("s3_endpoint: \"http://localhost:9000\""));
        assert!(spicepod_yaml.contains("allow_http: \"true\""));
        assert!(spicepod_yaml.contains("engine: cayenne"));
        assert!(spicepod_yaml.contains("mode: file"));
        assert!(spicepod_yaml.contains("refresh_mode: full"));
        assert!(spicepod_yaml.contains("telemetry:"));
        assert!(spicepod_yaml.contains("enabled: false"));
        assert!(spicepod_yaml.contains(
            "state_location: \"s3://bucket/state/00000000-0000-0000-0000-000000000000\""
        ));
        assert!(spicepod_yaml.contains("s3_auth: key"));
    }

    #[tokio::test]
    async fn generate_spicepod_errors_on_missing_dataset_source() {
        let setup_config = SetupConfig {
            region: None,
            endpoint: None,
            sink_type: None,
            spicepod_path: None,
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

        let args = test_stdio_args();
        let err = generate_initial_spicepod(&Uuid::nil(), &setup_config, &datasets, None, &args)
            .await
            .expect_err("missing source should fail");
        assert!(
            err.to_string().contains("missing_table"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn generate_initial_spicepod_uses_loaded_spicepod_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("input.yaml");
        // Original name is intentionally different from the per-run name so we
        // can assert the override below.
        tokio::fs::write(
            &path,
            "version: v1\nkind: Spicepod\nname: user-supplied\ndatasets:\n  - from: s3://public-bucket/customer.parquet\n    name: customer\n    params:\n      file_format: parquet\n      s3_auth: public\n",
        )
        .await
        .expect("write yaml");

        let setup_config = SetupConfig {
            region: None,
            endpoint: None,
            sink_type: None,
            spicepod_path: Some(path.to_string_lossy().into_owned()),
        };
        let datasets: HashMap<String, DatasetConfig> = HashMap::new();
        let args = test_stdio_args();
        let run_id = Uuid::parse_str("01234567-89ab-cdef-0123-456789abcdef").expect("parse uuid");

        let spicepod = generate_initial_spicepod(&run_id, &setup_config, &datasets, None, &args)
            .await
            .expect("spicepod loads from disk");

        assert_eq!(spicepod.name, "spidapter-01234567");
        let yaml = serialize_spicepod(&spicepod).expect("serialize");
        assert!(
            yaml.contains("customer.parquet"),
            "dataset from file is missing in deployed spicepod: {yaml}"
        );
        assert!(
            yaml.contains("s3_auth: public"),
            "public auth from file is missing: {yaml}"
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

    #[test]
    fn adbc_create_table_statement_includes_partition_by() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, true),
        ]));

        let statement = generate_adbc_create_table_statement(
            "events",
            &DatasetConfig {
                schema,
                location: Some("s3://bucket/path/events/".to_string()),
                primary_key_columns: Vec::new(),
                time_column: None,
                partition_columns: vec!["region".to_string()],
            },
        )
        .expect("statement should generate");

        assert!(
            statement.contains("PARTITION BY (region)"),
            "expected PARTITION BY clause in: {statement}"
        );
    }

    #[test]
    fn adbc_create_table_statement_allows_partition_column_not_in_schema() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let statement = generate_adbc_create_table_statement(
            "events",
            &DatasetConfig {
                schema,
                location: Some("s3://bucket/path/events/".to_string()),
                primary_key_columns: Vec::new(),
                time_column: None,
                partition_columns: vec!["missing_col".to_string()],
            },
        )
        .expect("partition column not in schema should still succeed");

        assert!(
            statement.contains("PARTITION BY (missing_col)"),
            "expected PARTITION BY clause in: {statement}"
        );
    }

    #[test]
    fn adbc_create_table_statement_errors_for_multiple_partition_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, true),
            Field::new("country", DataType::Utf8, true),
        ]));

        let err = generate_adbc_create_table_statement(
            "events",
            &DatasetConfig {
                schema,
                location: Some("s3://bucket/path/events/".to_string()),
                primary_key_columns: Vec::new(),
                time_column: None,
                partition_columns: vec!["region".to_string(), "country".to_string()],
            },
        )
        .expect_err("multiple partition columns should fail");

        assert!(
            err.to_string()
                .contains("only a single partition column is supported"),
            "unexpected error: {err}"
        );
    }
}
