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
use std::path::{Path, PathBuf};
use std::time::Duration;

use arrow::datatypes::DataType;
use async_trait::async_trait;
use spice_cloud_client::CloudClient;
use spicepod::component::runtime::{
    Scheduler, default_max_partition_assignments_per_interval, default_max_partitions_per_executor,
    default_partition_assignment_interval, default_partition_discovery_timeout,
};
use spicepod::param::{ParamValue, Params};
use spicepod::spec::SpicepodDefinition;
use system_adapter_protocol::{
    AdbcDriver, DatasetConfig, EtlSinkType, Handler, IngestionMetrics, MetricsResponse,
    ResourceMetrics, Server, SetupResponse, TeardownResponse,
};
use tokio::process::Child;
use tokio::time::sleep;
use uuid::Uuid;

use crate::args::{PgAccelerationEngine, StdioArgs};
use crate::commands;

#[path = "ingestion_targets/mod.rs"]
mod ingestion_targets;

#[path = "provision_scp.rs"]
mod provision_scp;

#[path = "provision_local.rs"]
mod provision_local;

use ingestion_targets::cayenne::{
    build_cayenne_setup_response, generate_cayenne_sink_spicepod, generate_hive_spicepod,
};
use ingestion_targets::postgres_cdc::{
    PgConfig, generate_postgres_wal_spicepod, setup_postgres_for_wal, teardown_postgres,
};
use provision_local::{provision_local_cluster, provision_local_single_node, teardown_local_run};
use provision_scp::provision_scp_app;

const POST_SETUP_SQL_MAX_RETRIES: u64 = 5;

/// State for an active benchmark run provisioned via `setup`.
enum RunState {
    Scp {
        /// Spice Cloud app ID.
        app_id: i64,
        /// API key for the app (used for Flight SQL authentication).
        api_key: String,
        /// Flight SQL endpoint URL derived from the cname.
        flight_url: String,
        /// SQL endpoint URL for DDL execution.
        sql_url: String,
        /// Cloud client used during provisioning (reused for teardown).
        cloud: CloudClient,
        /// If the run used `PostgreSQL` WAL CDC, connection details needed for teardown.
        pg_config: Box<Option<PgConfig>>,
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
    processes: LocalProcesses,
    flight_url: String,
    flight_api_key: Option<String>,
    sql_url: String,
    working_dir: PathBuf,
    pg_config: Option<PgConfig>,
}

enum LocalProcesses {
    SingleNode {
        child: Child,
    },
    Cluster {
        scheduler_child: Child,
        executor_children: Vec<Child>,
    },
}

impl Drop for LocalRunState {
    fn drop(&mut self) {
        match &mut self.processes {
            LocalProcesses::SingleNode { child } => {
                let _ = child.start_kill();
            }
            LocalProcesses::Cluster {
                scheduler_child,
                executor_children,
            } => {
                for child in executor_children {
                    let _ = child.start_kill();
                }
                let _ = scheduler_child.start_kill();
            }
        }
    }
}

use crate::args::{BackendMode, DeploymentMode};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum IngestionTarget {
    #[default]
    Cayenne,
    PostgresCdc,
}

#[derive(Debug, Clone)]
struct SetupConfig {
    region: Option<String>,
    endpoint: Option<String>,
    sink_type: Option<EtlSinkType>,
    /// Absolute path to a spicepod the client wants deployed verbatim
    /// (testoperator's cluster-bench path passes this; spicebench leaves it
    /// empty and relies on the `datasets` JSON-RPC parameter instead).
    spicepod_path: Option<String>,
    ingestion_target: IngestionTarget,
    /// Present when `ingestion_target == PostgresCdc`.
    pg_config: Option<PgConfig>,
}

impl SetupConfig {
    fn from_metadata(metadata: &HashMap<String, serde_json::Value>) -> Self {
        Self {
            region: metadata_string(metadata, "etl_region"),
            endpoint: metadata_string(metadata, "etl_endpoint"),
            sink_type: None,
            spicepod_path: metadata_string(metadata, "spicepod_path"),
            ingestion_target: IngestionTarget::default(),
            pg_config: None,
        }
    }

    fn set_etl_sink_type(mut self, sink_type: Option<EtlSinkType>) -> Self {
        self.sink_type = sink_type;
        self
    }

    fn set_pg_config(mut self, pg_config: Option<PgConfig>) -> Self {
        self.ingestion_target = if pg_config.is_some() {
            IngestionTarget::PostgresCdc
        } else {
            IngestionTarget::Cayenne
        };
        self.pg_config = pg_config;
        self
    }
}

fn resolve_aws_region(setup_config: &SetupConfig) -> String {
    setup_config
        .region
        .clone()
        .or_else(|| std::env::var("AWS_REGION").ok())
        .or_else(|| std::env::var("AWS_DEFAULT_REGION").ok())
        .unwrap_or_else(|| "us-east-1".to_string())
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

        let pg_config = PgConfig::from_args(&self.args);

        // WAL CDC mode: set up PostgreSQL tables, replication slot, and publication
        // before provisioning Spice so the spicepod can reference them immediately.
        if let Some(ref pg) = pg_config {
            setup_postgres_for_wal(pg, &datasets)
                .await
                .map_err(|e| format!("Failed to set up PostgreSQL for WAL CDC: {e}"))?;
        }

        let setup_config = SetupConfig::from_metadata(&metadata)
            .set_etl_sink_type(etl_sink_type)
            .set_pg_config(pg_config);
        let backend = self.args.backend;

        let state = match (backend, self.args.deployment_mode) {
            (BackendMode::Scp, _) => {
                provision_scp_app(run_id, &self.args, &setup_config, &datasets).await
            }
            (BackendMode::Local, DeploymentMode::SingleNode) => {
                provision_local_single_node(
                    run_id,
                    Duration::from_secs(self.args.ready_wait),
                    &setup_config,
                    &datasets,
                    &self.args,
                )
                .await
            }
            (BackendMode::Local, DeploymentMode::Distributed) => {
                provision_local_cluster(
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

        let response = match setup_config.ingestion_target {
            IngestionTarget::PostgresCdc => {
                let pg = setup_config
                    .pg_config
                    .as_ref()
                    .ok_or_else(|| "pg_config missing for PostgresCdc target".to_string())?;
                let flight_kwargs = HashMap::from([
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
                SetupResponse {
                    driver: AdbcDriver::Postgresql,
                    db_kwargs: pg.adbc_kwargs(),
                    catalog_namespace: Some(pg.schema.clone()),
                    read_driver: Some((AdbcDriver::Flightsql, flight_kwargs)),
                    endpoints: HashMap::new(),
                }
            }
            IngestionTarget::Cayenne => build_cayenne_setup_response(etl_sink_type, &state),
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
        let run_datasets = self.run_datasets.remove(&run_id).unwrap_or_default();

        let pg_config = match &state {
            RunState::Scp { pg_config, .. } => *pg_config.clone(),
            RunState::Local(local) => local.pg_config.clone(),
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

        if let Some(pg) = pg_config {
            teardown_postgres(&pg, &run_datasets)
                .await
                .map_err(|e| format!("Failed to teardown PostgreSQL: {e}"))?;
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
    if setup_config.ingestion_target == IngestionTarget::PostgresCdc {
        eprintln!("[stdio] CDC ingestion target: skipping post-setup SQL actions");
        return Ok(());
    }

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

async fn write_local_spicepod(
    spicepod: &SpicepodDefinition,
    working_dir: &Path,
) -> anyhow::Result<PathBuf> {
    let spicepod_yaml = serialize_spicepod(spicepod)?;
    eprintln!("[stdio] Generated local spicepod:\n{spicepod_yaml}");
    let spicepod_path = working_dir.join("spicepod.yaml");
    tokio::fs::write(&spicepod_path, spicepod_yaml).await?;
    Ok(spicepod_path)
}

fn serialize_spicepod(spicepod: &SpicepodDefinition) -> anyhow::Result<String> {
    yaml::to_string(spicepod).map_err(|e| anyhow::anyhow!("Failed to serialize spicepod: {e}"))
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

/// Generate the initial [`SpicepodDefinition`] for the benchmark run.
async fn generate_initial_spicepod(
    run_id: &Uuid,
    setup_config: &SetupConfig,
    datasets: &HashMap<String, DatasetConfig>,
    flight_api_key: Option<&str>,
    args: &StdioArgs,
) -> anyhow::Result<SpicepodDefinition> {
    let scheduler_state_location = args.scheduler_state_location.as_deref();

    let mut spicepod = if let Some(path) = setup_config.spicepod_path.as_deref() {
        load_spicepod_from_path(path, run_id).await?
    } else {
        match setup_config.ingestion_target {
            IngestionTarget::PostgresCdc => {
                let pg = setup_config
                    .pg_config
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("pg_config missing for PostgresCdc target"))?;
                let engine = match args.pg_acceleration {
                    PgAccelerationEngine::Cayenne => "cayenne",
                    PgAccelerationEngine::Duckdb => "duckdb",
                };
                generate_postgres_wal_spicepod(run_id, pg, datasets, engine)
            }
            IngestionTarget::Cayenne => match setup_config.sink_type {
                Some(EtlSinkType::Adbc) => {
                    generate_cayenne_sink_spicepod(run_id, flight_api_key, args)
                }
                _ => generate_hive_spicepod(run_id, setup_config, datasets)?,
            },
        }
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

            let region = resolve_aws_region(setup_config);
            sched.params.as_mut().map(|p| {
                p.data
                    .insert("s3_region".to_string(), ParamValue::String(region))
            });
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
            deployment_mode: DeploymentMode::Distributed,
            pg_host: None,
            pg_port: 5432,
            pg_user: None,
            pg_password: String::new(),
            pg_database: None,
            pg_schema: "public".to_string(),
            pg_acceleration: PgAccelerationEngine::Cayenne,
            spiced_binary: "spiced".to_string(),
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
            ingestion_target: IngestionTarget::Cayenne,
            pg_config: None,
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
            ingestion_target: IngestionTarget::Cayenne,
            pg_config: None,
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
    async fn generate_spicepod_uses_loaded_spicepod_path() {
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
            ingestion_target: IngestionTarget::Cayenne,
            pg_config: None,
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
