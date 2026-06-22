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
    AdbcDriver, CdcReplicationMetrics, CdcTableMetrics, DatasetConfig, Handler, IngestionMetrics,
    MetricsResponse, ResourceMetrics, Server, SetupResponse, SinkConfig, TeardownResponse,
};
use tokio::process::Child;
use tokio::time::sleep;
use uuid::Uuid;

use crate::args::{DeploymentMode, StdioArgs};
use crate::commands;
use crate::scenario::{
    AccelerationEngine, CayenneConfig, ComputeConfig, DirectConfig, DynamoDbConfig, MongoEndpoint,
    PgEndpoint, ScenarioConfig, ScpConfig, SourceConfig, SpiceCompute, load_scenario,
};

#[path = "sources/mod.rs"]
mod sources;

#[path = "provision/mod.rs"]
mod provision;

use provision::{
    launch_ec2_debezium, launch_mongodb_ec2, launch_postgres_ec2, provision_local_single_node,
    provision_local_spiced_cluster, provision_scp_app, teardown_local_run, terminate_ec2_instance,
};
use sources::cayenne::generate_cayenne_sink_spicepod;
use sources::dynamodb::{
    DynamoDbTeardownInfo, create_dynamodb_tables, delete_dynamodb_tables,
    generate_dynamodb_spicepod,
};
use sources::mongodb::generate_mongodb_spicepod;
use sources::postgres_debezium::{
    generate_postgres_debezium_spicepod, register_debezium_postgres_connector,
    setup_postgres_for_debezium,
};
use sources::postgres_wal::{
    PgConfig, generate_postgres_wal_spicepod, pg_create_table_ddl, pg_error_message,
    setup_postgres_for_wal, teardown_postgres, tpch_schema_name,
};

const POST_SETUP_SQL_MAX_RETRIES: u64 = 5;

struct ScpRunState {
    app_id: i64,
    api_key: String,
    flight_url: String,
    sql_url: String,
    cloud: CloudClient,
    storage: FederatedStorageConfig,
    ec2_guards: Vec<Ec2Guard>,
    dynamodb_guard: Option<DynamoDbGuard>,
    mongodb_guard: Option<MongoDbGuard>,
}

/// State for an active benchmark run provisioned via `setup`.
enum RunState {
    Scp(Box<ScpRunState>),
    Local(Box<LocalRunState>),
}

impl RunState {
    fn flight_url(&self) -> &str {
        match self {
            Self::Scp(scp) => scp.flight_url.as_str(),
            Self::Local(state) => state.flight_url.as_str(),
        }
    }

    fn password(&self) -> &str {
        match self {
            Self::Scp(scp) => scp.api_key.as_str(),
            Self::Local(_) => "",
        }
    }

    fn sql_url(&self) -> &str {
        match self {
            Self::Scp(scp) => scp.sql_url.as_str(),
            Self::Local(state) => state.sql_url.as_str(),
        }
    }

    fn api_key(&self) -> Option<&str> {
        match self {
            Self::Scp(scp) => Some(scp.api_key.as_str()),
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
    storage: FederatedStorageConfig,
    ec2_guards: Vec<Ec2Guard>,
    dynamodb_guard: Option<DynamoDbGuard>,
    mongodb_guard: Option<MongoDbGuard>,
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

/// RAII guard that terminates an EC2 instance when dropped.
struct Ec2Guard {
    instance_id: Option<String>,
    region: String,
}

impl Ec2Guard {
    fn new(instance_id: String, region: String) -> Self {
        Self {
            instance_id: Some(instance_id),
            region,
        }
    }

    fn disarm(&mut self) -> Option<(String, String)> {
        self.instance_id.take().map(|id| (id, self.region.clone()))
    }
}

impl Drop for Ec2Guard {
    fn drop(&mut self) {
        let Some(instance_id) = self.instance_id.take() else {
            return;
        };
        let region = self.region.clone();
        eprintln!("[stdio] Ec2Guard: terminating instance {instance_id} (region={region})");
        std::thread::spawn(move || {
            let Ok(rt) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                eprintln!("[stdio] Ec2Guard: failed to build tokio runtime for cleanup");
                return;
            };
            if let Err(e) = rt.block_on(terminate_ec2_instance(&region, &instance_id)) {
                eprintln!("[stdio] Ec2Guard: failed to terminate instance {instance_id}: {e}");
            }
        })
        .join()
        .ok();
    }
}

/// RAII guard that deletes `DynamoDB` tables when dropped.
struct DynamoDbGuard {
    info: Option<DynamoDbTeardownInfo>,
}

impl DynamoDbGuard {
    fn new(info: DynamoDbTeardownInfo) -> Self {
        Self { info: Some(info) }
    }

    fn disarm(&mut self) -> Option<DynamoDbTeardownInfo> {
        self.info.take()
    }
}

impl Drop for DynamoDbGuard {
    fn drop(&mut self) {
        let Some(info) = self.info.take() else {
            return;
        };
        eprintln!(
            "[stdio] DynamoDbGuard: deleting {} table(s)",
            info.table_names.len()
        );
        std::thread::spawn(move || {
            let Ok(rt) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                eprintln!("[stdio] DynamoDbGuard: failed to build tokio runtime for cleanup");
                return;
            };
            if let Err(e) = rt.block_on(delete_dynamodb_tables(&info)) {
                eprintln!("[stdio] DynamoDbGuard: failed to delete DynamoDB tables: {e}");
            }
        })
        .join()
        .ok();
    }
}

/// Returns `uri` with its database path component replaced by `database`,
/// preserving the scheme, authority (userinfo/host), and query string.
///
/// `MongoDB` reads the default database from the URI path, so rewriting it routes
/// both the spicebench sink and the spiced connector to the same per-run db.
fn with_mongodb_database(uri: &str, database: &str) -> String {
    let (base, query) = match uri.split_once('?') {
        Some((b, q)) => (b, Some(q)),
        None => (uri, None),
    };
    // Authority begins after the "scheme://" prefix; the path is the first '/'
    // after that. Anything from that '/' onward is the old database name.
    let scheme_end = base.find("://").map_or(0, |i| i + 3);
    let authority_and_path = &base[scheme_end..];
    let new_base = match authority_and_path.find('/') {
        Some(slash) => format!(
            "{}{}/{database}",
            &base[..scheme_end],
            &authority_and_path[..slash]
        ),
        None => format!("{base}/{database}"),
    };
    match query {
        Some(q) => format!("{new_base}?{q}"),
        None => new_base,
    }
}

/// Drops a `MongoDB` database on an existing (connect-mode) instance.
async fn drop_mongodb_database(uri: &str, database: &str) -> anyhow::Result<()> {
    let client = mongodb::Client::with_uri_str(uri)
        .await
        .map_err(|e| anyhow::anyhow!("connect to MongoDB for cleanup: {e}"))?;
    client
        .database(database)
        .drop()
        .await
        .map_err(|e| anyhow::anyhow!("drop database '{database}': {e}"))?;
    Ok(())
}

/// Holds the per-run `MongoDB` database to drop during explicit teardown.
///
/// Used in connect mode (e.g. Atlas), where the instance is shared and outlives
/// the run, so the throwaway database must be cleaned up. (In provision mode the
/// whole EC2 instance is terminated, so no per-database cleanup is needed.)
///
/// IMPORTANT: this is NOT an auto-deleting RAII guard. The database is dropped
/// *only* by the explicit teardown path when `preserve_resources == false`
/// (see `teardown`). Dropping this value never deletes anything — see the `Drop`
/// impl — so `--no-teardown`, errors, panics, and process exit all leave the
/// database intact (fail-safe against destroying data the caller asked to keep).
struct MongoDbGuard {
    /// `(uri, database)`; `None` once taken by teardown.
    target: Option<(String, String)>,
}

impl MongoDbGuard {
    fn new(uri: String, database: String) -> Self {
        Self {
            target: Some((uri, database)),
        }
    }

    fn disarm(&mut self) -> Option<(String, String)> {
        self.target.take()
    }
}

impl Drop for MongoDbGuard {
    fn drop(&mut self) {
        // Fail-safe: NEVER delete the database implicitly on drop. The per-run
        // database is dropped only by the explicit teardown path when
        // `preserve_resources == false`. Any other path that drops this value —
        // `--no-teardown` (preserve), an error/early return, a panic, or process
        // exit — must leave the database intact so a caller who asked to keep it
        // (or a crashed run) never loses data they may want to inspect.
        //
        // The cost is that an abnormal exit leaks a throwaway `spidapter_<id>`
        // database on the shared instance; those are safe to GC by name later.
        if let Some((_, database)) = self.target.take() {
            eprintln!(
                "[stdio] MongoDbGuard: dropped without explicit teardown; \
                 leaving database '{database}' in place (not deleting)"
            );
        }
    }
}

/// Configuration for the federated storage backend for a benchmark run.
#[derive(Debug, Clone)]
enum FederatedStorageConfig {
    Direct,
    Postgres {
        pg: PgConfig,
        acceleration: AccelerationEngine,
    },
    PostgresDebezium {
        pg: PgConfig,
        kafka_brokers: String,
        debezium_connect_url: String,
        acceleration: AccelerationEngine,
    },
    DynamoDB {
        prefix: String,
        region: String,
        acceleration: AccelerationEngine,
    },
    MongoDB {
        uri: String,
        acceleration: AccelerationEngine,
    },
}

impl FederatedStorageConfig {
    fn deployment_mode(&self) -> DeploymentMode {
        match self {
            Self::Direct => DeploymentMode::Cluster,
            _ => DeploymentMode::SingleNode,
        }
    }
}

fn acceleration_engine_str(engine: AccelerationEngine) -> &'static str {
    match engine {
        AccelerationEngine::Cayenne => "cayenne",
        AccelerationEngine::Duckdb => "duckdb",
    }
}

#[derive(Debug, Clone)]
struct SetupConfig {
    region: Option<String>,
    /// Absolute path to a spicepod the client wants deployed verbatim.
    spicepod_path: Option<String>,
    storage: FederatedStorageConfig,
    /// Explicit AWS region override for the `DynamoDB` write path.
    aws_region_override: Option<String>,
}

impl SetupConfig {
    fn from_metadata(metadata: &HashMap<String, serde_json::Value>) -> Self {
        Self {
            region: metadata_string(metadata, "etl_region"),
            spicepod_path: metadata_string(metadata, "spicepod_path"),
            storage: FederatedStorageConfig::Direct,
            aws_region_override: None,
        }
    }

    fn set_storage(mut self, storage: FederatedStorageConfig) -> Self {
        self.storage = storage;
        self
    }
}

fn resolve_aws_region(setup_config: &SetupConfig) -> String {
    setup_config
        .aws_region_override
        .clone()
        .or_else(|| std::env::var("AWS_REGION").ok())
        .or_else(|| std::env::var("AWS_DEFAULT_REGION").ok())
        .or_else(|| setup_config.region.clone())
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

/// Fetch raw Prometheus metrics text from a spiced instance.
async fn fetch_prometheus_metrics(url: &str, api_key: Option<&str>) -> Result<String, String> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| format!("HTTP client error: {e}"))?;
    let mut req = client.get(url);
    if let Some(key) = api_key {
        req = req.bearer_auth(key);
    }
    let resp = req.send().await.map_err(|e| format!("GET {url}: {e}"))?;
    resp.text().await.map_err(|e| format!("read body: {e}"))
}

/// Parse a labeled Prometheus metric line `<name>{...dataset="X"...} <value>`,
/// summing values per `dataset` label. `name` must include any suffix
/// (`_sum`, `_count`, `_total`, …).
fn parse_labeled(body: &str, name: &str) -> std::collections::HashMap<String, f64> {
    let mut out = std::collections::HashMap::new();
    let prefix = format!("{name}{{");
    for line in body.lines() {
        if !line.starts_with(&prefix) {
            continue;
        }
        let dataset = line
            .split("dataset=\"")
            .nth(1)
            .and_then(|s| s.split('"').next())
            .unwrap_or("unknown")
            .to_string();
        let value: f64 = line
            .split_whitespace()
            .last()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0.0);
        *out.entry(dataset).or_insert(0.0) += value;
    }
    out
}

/// Build the vendor-neutral `CdcReplicationMetrics` from spiced's Prometheus
/// text, mapping spiced's MongoDB/cayenne metric names onto the generic
/// per-table contract. Returns `None` when no CDC metrics are present (e.g. a
/// non-CDC run), so the field stays absent rather than empty.
#[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn build_cdc_replication_metrics(body: &str) -> Option<CdcReplicationMetrics> {
    let recv = parse_labeled(body, "dataset_acceleration_cdc_source_recv_wait_ms_sum");
    let apply = parse_labeled(body, "dataset_acceleration_cdc_apply_burst_duration_ms_sum");
    let apply_count = parse_labeled(
        body,
        "dataset_acceleration_cdc_apply_burst_duration_ms_count",
    );
    let linger = parse_labeled(body, "dataset_acceleration_cdc_linger_wait_ms_sum");
    let bytes = parse_labeled(body, "dataset_acceleration_cdc_apply_burst_bytes_sum");
    // Per-burst row counter emitted by spiced. Match the exact name spiced
    // exposes (`..._cdc_apply_burst_rows_total`); fall back to legacy/suffixed
    // variants in case the metric name or OTel→Prometheus suffixing differs.
    let mut rows = parse_labeled(body, "dataset_acceleration_cdc_apply_burst_rows_total");
    for alt in [
        "dataset_acceleration_cdc_apply_burst_rows_total_total",
        "dataset_acceleration_cdc_apply_rows_total",
        "dataset_acceleration_cdc_apply_rows_total_total",
    ] {
        if rows.is_empty() {
            rows = parse_labeled(body, alt);
        }
    }

    let mut tables: Vec<String> = recv
        .keys()
        .chain(apply.keys())
        .chain(rows.keys())
        .chain(bytes.keys())
        .cloned()
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    tables.sort();
    if tables.is_empty() {
        return None;
    }

    let per_table = tables
        .into_iter()
        .map(|t| CdcTableMetrics {
            source_wait_ms: recv.get(&t).copied(),
            apply_ms: apply.get(&t).copied(),
            apply_count: apply_count.get(&t).map(|v| v.round() as u64),
            linger_ms: linger.get(&t).copied(),
            rows_applied: rows.get(&t).map(|v| v.round() as u64),
            bytes_applied: bytes.get(&t).map(|v| v.round() as u64),
            table: t,
        })
        .collect();

    Some(CdcReplicationMetrics { per_table })
}

/// System adapter handler that provisions Spice Cloud apps.
struct SpidapterHandler {
    /// Active runs keyed by run ID.
    runs: HashMap<Uuid, RunState>,
    /// Datasets from setup, keyed by run ID → dataset name → config.
    run_datasets: HashMap<Uuid, HashMap<String, DatasetConfig>>,
    /// Full CLI args (includes all flags and env-var-backed configuration).
    args: StdioArgs,
    /// Loaded scenario configuration (source type, compute, channel, acceleration).
    scenario: ScenarioConfig,
}

impl SpidapterHandler {
    fn new(args: &StdioArgs, mut scenario: ScenarioConfig) -> Self {
        // Apply env var overrides for SCP image tag and channel so the workflow
        // can pass SPIDAPTER_IMAGE_TAG / SPIDAPTER_CHANNEL without modifying the
        // scenario YAML.
        if let Some(ComputeConfig::Scp(ref mut scp)) = scenario.compute {
            if let Ok(tag) = std::env::var("SPIDAPTER_IMAGE_TAG")
                && !tag.is_empty()
            {
                scp.image_tag = Some(tag);
            }
            if let Ok(channel) = std::env::var("SPIDAPTER_CHANNEL")
                && !channel.is_empty()
            {
                use spice_cloud_client::types::UpdateChannel;
                scp.channel = channel.parse::<UpdateChannel>().ok();
            }
        }
        Self {
            runs: HashMap::new(),
            run_datasets: HashMap::new(),
            args: args.clone(),
            scenario,
        }
    }

    /// Return the SCP config from the scenario, if present.
    fn scp_config(&self) -> Option<&ScpConfig> {
        match &self.scenario.compute {
            Some(ComputeConfig::Scp(scp)) => Some(scp),
            _ => None,
        }
    }

    /// Resolve the acceleration engine from the scenario.
    fn acceleration(&self) -> AccelerationEngine {
        self.scenario
            .acceleration
            .unwrap_or(AccelerationEngine::Cayenne)
    }

    /// Resolve the AWS region for EC2/DynamoDB provisioning.
    fn aws_region(&self) -> String {
        // For direct-ingest with a CayenneConfig, use the cayenne aws_region if set.
        if let SourceConfig::Direct(DirectConfig {
            cayenne: Some(ref c),
        }) = self.scenario.source
            && let Some(ref r) = c.aws_region
            && !r.is_empty()
        {
            return r.clone();
        }
        std::env::var("AWS_REGION")
            .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
            .unwrap_or_else(|_| "us-east-1".to_string())
    }

    /// Resolve the compute target from the scenario.
    fn compute(&self) -> SpiceCompute {
        match &self.scenario.compute {
            Some(ComputeConfig::Scp(_)) => SpiceCompute::Scp,
            Some(ComputeConfig::Local) | None => SpiceCompute::Local,
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

        let mut ec2_guards: Vec<Ec2Guard> = Vec::new();
        let mut dynamodb_guard: Option<DynamoDbGuard> = None;
        let mut mongodb_guard: Option<MongoDbGuard> = None;

        let acceleration = self.acceleration();
        let region = self.aws_region();
        let run_id_str = run_id.to_string();
        let short_id = run_id_str.split('-').next().unwrap_or_default();

        // Extract CayenneConfig from the scenario source for use in spicepod generation.
        let cayenne_cfg: Option<&CayenneConfig> = match &self.scenario.source {
            SourceConfig::Direct(DirectConfig { cayenne }) => cayenne.as_ref(),
            _ => None,
        };

        let storage = match &self.scenario.source {
            SourceConfig::Direct(_) => FederatedStorageConfig::Direct,

            SourceConfig::PostgresWal(PgEndpoint::Provision(prov)) => {
                let ec2 = launch_postgres_ec2(&prov.ec2, &region, short_id)
                    .await
                    .map_err(|e| format!("Failed to provision EC2 PostgreSQL instance: {e}"))?;
                ec2_guards.push(Ec2Guard::new(ec2.instance_id.clone(), ec2.region.clone()));
                let pg = PgConfig {
                    host: ec2.host,
                    port: ec2.pg_port,
                    user: ec2.pg_user,
                    password: ec2.pg_password,
                    database: ec2.pg_database,
                    schema: tpch_schema_name(&run_id),
                };
                setup_postgres_for_wal(&pg, &datasets)
                    .await
                    .map_err(|e| format!("Failed to set up PostgreSQL for WAL CDC: {e}"))?;
                FederatedStorageConfig::Postgres { pg, acceleration }
            }

            SourceConfig::PostgresWal(PgEndpoint::Connect(pg_conf)) => {
                let pg = PgConfig {
                    host: pg_conf.host.clone(),
                    port: pg_conf.port,
                    user: pg_conf.user.clone(),
                    password: pg_conf.password.clone(),
                    database: pg_conf.database.clone(),
                    schema: tpch_schema_name(&run_id),
                };
                setup_postgres_for_wal(&pg, &datasets)
                    .await
                    .map_err(|e| format!("Failed to set up PostgreSQL for WAL CDC: {e}"))?;
                FederatedStorageConfig::Postgres { pg, acceleration }
            }

            SourceConfig::PostgresDebezium(PgEndpoint::Provision(prov)) => {
                let ec2_spec = &prov.ec2;
                let (pg_res, deb_res) = tokio::join!(
                    launch_postgres_ec2(ec2_spec, &region, short_id),
                    launch_ec2_debezium(ec2_spec, &region, short_id)
                );

                let ec2_pg = match pg_res {
                    Ok(inst) => {
                        ec2_guards
                            .push(Ec2Guard::new(inst.instance_id.clone(), inst.region.clone()));
                        inst
                    }
                    Err(e) => {
                        return Err(format!("Failed to provision EC2 PostgreSQL instance: {e}"));
                    }
                };

                let ec2_deb = match deb_res {
                    Ok(inst) => {
                        ec2_guards
                            .push(Ec2Guard::new(inst.instance_id.clone(), inst.region.clone()));
                        inst
                    }
                    Err(e) => {
                        return Err(format!("Failed to provision EC2 Debezium instance: {e}"));
                    }
                };

                let pg = PgConfig {
                    host: ec2_pg.host,
                    port: ec2_pg.pg_port,
                    user: ec2_pg.pg_user,
                    password: ec2_pg.pg_password,
                    database: ec2_pg.pg_database,
                    schema: tpch_schema_name(&run_id),
                };

                setup_postgres_for_debezium(&pg, &datasets)
                    .await
                    .map_err(|e| format!("Failed to set up PostgreSQL for Debezium CDC: {e}"))?;

                FederatedStorageConfig::PostgresDebezium {
                    pg,
                    kafka_brokers: ec2_deb.kafka_brokers,
                    debezium_connect_url: ec2_deb.connect_url,
                    acceleration,
                }
            }

            SourceConfig::PostgresDebezium(PgEndpoint::Connect(pg_conf)) => {
                let kafka_brokers = std::env::var("KAFKA_BROKERS").map_err(|_| {
                    "KAFKA_BROKERS env var is required for local postgres-debezium mode".to_string()
                })?;
                let debezium_connect_url = std::env::var("DEBEZIUM_CONNECT_URL").map_err(|_| {
                    "DEBEZIUM_CONNECT_URL env var is required for local postgres-debezium mode"
                        .to_string()
                })?;
                let pg = PgConfig {
                    host: pg_conf.host.clone(),
                    port: pg_conf.port,
                    user: pg_conf.user.clone(),
                    password: pg_conf.password.clone(),
                    database: pg_conf.database.clone(),
                    schema: tpch_schema_name(&run_id),
                };
                setup_postgres_for_debezium(&pg, &datasets)
                    .await
                    .map_err(|e| format!("Failed to set up PostgreSQL for Debezium CDC: {e}"))?;
                FederatedStorageConfig::PostgresDebezium {
                    pg,
                    kafka_brokers,
                    debezium_connect_url,
                    acceleration,
                }
            }

            SourceConfig::DynamoDbStreams(DynamoDbConfig {
                region: dynamo_region,
            }) => {
                let effective_region = if dynamo_region.is_empty() {
                    std::env::var("AWS_REGION")
                        .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
                        .or_else(|_| metadata_string(&metadata, "etl_region").ok_or(()))
                        .unwrap_or_else(|()| "us-east-1".to_string())
                } else {
                    dynamo_region.clone()
                };
                FederatedStorageConfig::DynamoDB {
                    prefix: String::new(),
                    region: effective_region,
                    acceleration,
                }
            }

            SourceConfig::MongodbStreams(MongoEndpoint::Provision(prov)) => {
                let instance = launch_mongodb_ec2(&prov.ec2, &region, short_id)
                    .await
                    .map_err(|e| format!("Failed to provision EC2 MongoDB instance: {e}"))?;
                ec2_guards.push(Ec2Guard::new(
                    instance.instance_id.clone(),
                    instance.region.clone(),
                ));
                eprintln!(
                    "[stdio] EC2 MongoDB: instance {} ready at {}",
                    instance.instance_id, instance.host
                );
                FederatedStorageConfig::MongoDB {
                    uri: instance.uri,
                    acceleration,
                }
            }

            SourceConfig::MongodbStreams(MongoEndpoint::Connect(mongo_conf)) => {
                // Connect mode targets an existing, shared instance (e.g. Atlas).
                // Route this run to a fresh per-run database so concurrent runs are
                // isolated and cleanup is a single drop. The database is created
                // lazily on first write (by the spicebench sink) and dropped at
                // teardown via the MongoDbGuard below.
                let database = format!("spidapter_{short_id}");
                let uri = with_mongodb_database(&mongo_conf.uri, &database);
                eprintln!(
                    "[stdio] MongoDB connect: using per-run database '{database}' \
                     (created on first write, dropped at teardown)"
                );
                mongodb_guard = Some(MongoDbGuard::new(uri.clone(), database));
                FederatedStorageConfig::MongoDB { uri, acceleration }
            }
        };

        let mut setup_config = SetupConfig::from_metadata(&metadata).set_storage(storage);
        // For non-direct sources, still honour the env AWS_REGION override.
        setup_config.aws_region_override = std::env::var("AWS_REGION").ok();

        // DynamoDB: create tables now so spicebench can write via the native sink.
        if let FederatedStorageConfig::DynamoDB { ref region, .. } = setup_config.storage {
            let region = region.clone();
            let prefix = create_dynamodb_tables(&setup_config, &datasets)
                .await
                .map_err(|e| format!("Failed to create DynamoDB tables: {e}"))?;
            let table_names = datasets
                .keys()
                .map(|name| format!("{prefix}.{name}"))
                .collect();
            dynamodb_guard = Some(DynamoDbGuard::new(DynamoDbTeardownInfo {
                table_names,
                region,
            }));
            if let FederatedStorageConfig::DynamoDB {
                prefix: ref mut p, ..
            } = setup_config.storage
            {
                *p = prefix;
            }
        }

        // Debezium/PostgreSQL: register the connector.
        if let FederatedStorageConfig::PostgresDebezium {
            ref pg,
            ref debezium_connect_url,
            ..
        } = setup_config.storage
        {
            let table_names: Vec<&str> = datasets.keys().map(String::as_str).collect();
            let debezium_pg_host =
                std::env::var("PG_DEBEZIUM_HOST").unwrap_or_else(|_| pg.host.clone());
            register_debezium_postgres_connector(
                debezium_connect_url,
                pg,
                &debezium_pg_host,
                &table_names,
            )
            .await
            .map_err(|e| format!("Failed to register Debezium PostgreSQL connector: {e}"))?;
        }

        // For Cayenne (DirectIngest): provision spiced first (Flight URL needed to build SinkConfig).
        // For all other backends: build SinkConfig first, then provision spiced.
        let (sink, mut state) = if matches!(&setup_config.storage, FederatedStorageConfig::Direct) {
            let deployment_mode = setup_config.storage.deployment_mode();
            let provision_result = match self.compute() {
                SpiceCompute::Scp => {
                    let scp = self.scp_config().ok_or_else(|| {
                        "direct-ingest with SCP compute requires a scenario with `compute: scp:`"
                            .to_string()
                    })?;
                    provision_scp_app(
                        run_id,
                        &self.args,
                        scp,
                        &setup_config,
                        &datasets,
                        &deployment_mode,
                        true,
                        cayenne_cfg,
                    )
                    .await
                }
                SpiceCompute::Local => {
                    provision_local_spiced_cluster(
                        run_id,
                        Duration::from_secs(self.args.ready_wait),
                        &setup_config,
                        &datasets,
                        &self.args,
                        self.scp_config(),
                    )
                    .await
                }
            };
            let mut state = match provision_result {
                Ok(s) => s,
                Err(e) => return Err(format!("direct-ingest setup: provisioning failed:{e}")),
            };
            match &mut state {
                RunState::Scp(scp) => scp.storage = setup_config.storage.clone(),
                RunState::Local(local) => local.storage = setup_config.storage.clone(),
            }

            let sql_url = state.sql_url().to_string();
            let api_key = state.api_key().map(str::to_string);
            post_setup_sink_action(&datasets, &sql_url, api_key.as_deref())
                .await
                .map_err(|e| format!("direct-ingest post-setup SQL failed:{e}"))?;

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
                (
                    "spicebench.write_schema".to_string(),
                    serde_json::Value::String("spicebench.bench".to_string()),
                ),
            ]);
            if let RunState::Local(local_state) = &state
                && let Some(ak) = &local_state.flight_api_key
            {
                db_kwargs.insert(
                    "adbc.flight.sql.rpc.call_header.authorization".to_string(),
                    serde_json::Value::String(format!("Bearer {ak}")),
                );
            }
            (
                SinkConfig::Adbc {
                    driver: AdbcDriver::Flightsql,
                    db_kwargs,
                },
                state,
            )
        } else {
            let sink = match &setup_config.storage {
                FederatedStorageConfig::Postgres { pg, .. }
                | FederatedStorageConfig::PostgresDebezium { pg, .. } => {
                    let mut write_db_kwargs = pg.adbc_kwargs();
                    write_db_kwargs.insert(
                        "spicebench.write_schema".to_string(),
                        serde_json::Value::String(pg.schema.clone()),
                    );
                    SinkConfig::Adbc {
                        driver: AdbcDriver::Postgresql,
                        db_kwargs: write_db_kwargs,
                    }
                }
                FederatedStorageConfig::DynamoDB { .. } => {
                    let region = resolve_aws_region(&setup_config);
                    let access_key_id = std::env::var("AWS_ACCESS_KEY_ID").ok();
                    let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY").ok();
                    let session_token = std::env::var("AWS_SESSION_TOKEN").ok();
                    SinkConfig::DynamoDb {
                        region,
                        access_key_id,
                        secret_access_key,
                        session_token,
                    }
                }
                FederatedStorageConfig::MongoDB { uri, .. } => {
                    SinkConfig::MongoDb { uri: uri.clone() }
                }
                FederatedStorageConfig::Direct => unreachable!(),
            };

            let deployment_mode = setup_config.storage.deployment_mode();
            let provision_result = match self.compute() {
                SpiceCompute::Scp => {
                    let scp = self.scp_config().ok_or_else(|| {
                        "SCP compute requires a scenario with `compute: scp:`".to_string()
                    })?;
                    provision_scp_app(
                        run_id,
                        &self.args,
                        scp,
                        &setup_config,
                        &datasets,
                        &deployment_mode,
                        false,
                        cayenne_cfg,
                    )
                    .await
                }
                SpiceCompute::Local => match deployment_mode {
                    DeploymentMode::SingleNode => {
                        provision_local_single_node(
                            run_id,
                            Duration::from_secs(self.args.ready_wait),
                            &setup_config,
                            &datasets,
                            &self.args,
                            self.scp_config(),
                            cayenne_cfg,
                        )
                        .await
                    }
                    DeploymentMode::Cluster => {
                        provision_local_spiced_cluster(
                            run_id,
                            Duration::from_secs(self.args.ready_wait),
                            &setup_config,
                            &datasets,
                            &self.args,
                            self.scp_config(),
                        )
                        .await
                    }
                },
            };

            let state = match provision_result {
                Ok(mut s) => {
                    match &mut s {
                        RunState::Scp(scp) => scp.storage = setup_config.storage.clone(),
                        RunState::Local(local) => {
                            local.storage = setup_config.storage.clone();
                        }
                    }
                    s
                }
                Err(e) => {
                    return Err(format!("Setup failed: provisioning failed: {e}"));
                }
            };

            (sink, state)
        };

        let mut read_db_kwargs = HashMap::from([
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
            && let Some(ak) = &local_state.flight_api_key
        {
            read_db_kwargs.insert(
                "adbc.flight.sql.rpc.call_header.authorization".to_string(),
                serde_json::Value::String(format!("Bearer {ak}")),
            );
        }

        let catalog_namespace = match &setup_config.storage {
            FederatedStorageConfig::Direct => Some("spicebench.bench".to_string()),
            FederatedStorageConfig::DynamoDB { prefix, .. } if !prefix.is_empty() => {
                Some(prefix.clone())
            }
            _ => None,
        };

        match &mut state {
            RunState::Scp(scp) => {
                scp.ec2_guards = ec2_guards;
                scp.dynamodb_guard = dynamodb_guard;
                scp.mongodb_guard = mongodb_guard;
            }
            RunState::Local(local) => {
                local.ec2_guards = ec2_guards;
                local.dynamodb_guard = dynamodb_guard;
                local.mongodb_guard = mongodb_guard;
            }
        }

        self.runs.insert(run_id, state);
        self.run_datasets.insert(run_id, datasets);

        Ok(SetupResponse {
            sink,
            read_driver: AdbcDriver::Flightsql,
            read_db_kwargs,
            catalog_namespace,
            endpoints: HashMap::new(),
        })
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

        // Derive the Prometheus metrics URL.
        // - SCP: the gateway serves Prometheus at `/v1/metrics` (same host as
        //   `/v1/sql`), so swap only the trailing path segment and keep `/v1`.
        // - Local: spiced serves Prometheus on a dedicated `--metrics` port
        //   (SPIDAPTER_METRICS_PORT) at `/metrics`, distinct from the HTTP/SQL
        //   port — the path swap alone would hit the SQL port and return nothing.
        let prometheus_url = match state {
            RunState::Local(_) => match std::env::var("SPIDAPTER_METRICS_PORT") {
                Ok(port) if !port.trim().is_empty() => {
                    format!("http://127.0.0.1:{}/metrics", port.trim())
                }
                _ => state.sql_url().replace("/v1/sql", "/metrics"),
            },
            RunState::Scp(_) => state.sql_url().replace("/v1/sql", "/v1/metrics"),
        };
        let api_key = state.api_key().map(std::string::ToString::to_string);

        // Scrape Prometheus metrics for the CDC replication payload.
        let prom_body = fetch_prometheus_metrics(&prometheus_url, api_key.as_deref())
            .await
            .ok();
        let cdc_replication = prom_body.as_deref().and_then(build_cdc_replication_metrics);

        match state {
            RunState::Scp(scp) => {
                let cloud_metrics = scp
                    .cloud
                    .get_app_metrics(scp.app_id, None)
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
                    cdc_replication,
                })
            }
            RunState::Local(_) => Ok(MetricsResponse {
                resource: ResourceMetrics::default(),
                ingestion: IngestionMetrics::default(),
                cdc_replication,
            }),
        }
    }

    async fn teardown(
        &mut self,
        run_id: Uuid,
        preserve_resources: bool,
    ) -> Result<TeardownResponse, String> {
        eprintln!("[stdio] teardown: run_id={run_id} preserve_resources={preserve_resources}");

        let Some(mut state) = self.runs.remove(&run_id) else {
            eprintln!("[stdio] teardown: run_id={run_id} not found (already torn down?)");
            return Ok(TeardownResponse { ok: true });
        };
        let run_datasets = self.run_datasets.remove(&run_id).unwrap_or_default();

        let storage = match &state {
            RunState::Scp(scp) => scp.storage.clone(),
            RunState::Local(local) => local.storage.clone(),
        };

        let (ec2_guards, dynamodb_guard, mongodb_guard) = match &mut state {
            RunState::Scp(scp) => (
                std::mem::take(&mut scp.ec2_guards),
                scp.dynamodb_guard.take(),
                scp.mongodb_guard.take(),
            ),
            RunState::Local(local) => (
                std::mem::take(&mut local.ec2_guards),
                local.dynamodb_guard.take(),
                local.mongodb_guard.take(),
            ),
        };

        if preserve_resources {
            // Disarm all guards so their Drop impls don't clean up anything.
            // The run state is removed from the map (no further RPC calls possible)
            // but all provisioned resources stay alive for post-run inspection.
            for mut guard in ec2_guards {
                if let Some((instance_id, region)) = guard.disarm() {
                    eprintln!(
                        "[stdio] teardown(preserve): keeping EC2 instance {instance_id} \
                         (region={region}) alive"
                    );
                }
            }
            if let Some(mut guard) = dynamodb_guard {
                guard.disarm();
                eprintln!("[stdio] teardown(preserve): keeping DynamoDB tables alive");
            }
            if let Some(mut guard) = mongodb_guard
                && let Some((_, database)) = guard.disarm()
            {
                eprintln!(
                    "[stdio] teardown(preserve): keeping MongoDB database '{database}' alive"
                );
            }
            // For SCP: skip app deletion so the deployed spiced stays running.
            eprintln!("[stdio] teardown(preserve): skipping resource deletion");
            return Ok(TeardownResponse { ok: true });
        }

        match state {
            RunState::Scp(scp) => {
                eprintln!(
                    "[stdio] teardown: deleting app {} at {}",
                    scp.app_id,
                    scp.cloud.base_url()
                );
                commands::delete_app(&scp.cloud, scp.app_id)
                    .await
                    .map_err(|e| format!("Failed to delete app {}: {e}", scp.app_id))?;
                eprintln!("[stdio] teardown: app {} deleted", scp.app_id);
            }
            RunState::Local(mut local_state) => {
                teardown_local_run(&mut local_state)
                    .await
                    .map_err(|e| format!("Failed to teardown local run {run_id}: {e}"))?;
            }
        }

        match &storage {
            FederatedStorageConfig::Postgres { pg, .. }
            | FederatedStorageConfig::PostgresDebezium { pg, .. } => {
                teardown_postgres(pg, &run_datasets)
                    .await
                    .map_err(|e| format!("Failed to teardown PostgreSQL: {e}"))?;
            }
            FederatedStorageConfig::DynamoDB { .. }
            | FederatedStorageConfig::MongoDB { .. }
            | FederatedStorageConfig::Direct => {}
        }

        let mut cleanup: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
        for mut guard in ec2_guards {
            if let Some((instance_id, region)) = guard.disarm() {
                cleanup.spawn(async move {
                    eprintln!(
                        "[stdio] teardown: terminating EC2 instance {instance_id} (region={region})"
                    );
                    if let Err(e) = terminate_ec2_instance(&region, &instance_id).await {
                        eprintln!(
                            "[stdio] teardown: warning: failed to terminate EC2 instance {instance_id}: {e}"
                        );
                    }
                });
            }
        }
        if let Some(mut guard) = dynamodb_guard
            && let Some(info) = guard.disarm()
        {
            cleanup.spawn(async move {
                eprintln!(
                    "[stdio] teardown: deleting {} DynamoDB table(s)",
                    info.table_names.len()
                );
                if let Err(e) = delete_dynamodb_tables(&info).await {
                    eprintln!("[stdio] teardown: warning: failed to delete DynamoDB tables: {e}");
                }
            });
        }
        if let Some(mut guard) = mongodb_guard
            && let Some((uri, database)) = guard.disarm()
        {
            cleanup.spawn(async move {
                eprintln!("[stdio] teardown: dropping MongoDB database '{database}'");
                if let Err(e) = drop_mongodb_database(&uri, &database).await {
                    eprintln!(
                        "[stdio] teardown: warning: failed to drop MongoDB database '{database}': {e}"
                    );
                }
            });
        }
        while cleanup.join_next().await.is_some() {}

        Ok(TeardownResponse { ok: true })
    }

    async fn create_staging_table(
        &mut self,
        run_id: Uuid,
        source_dataset: &str,
        staging_table_name: &str,
    ) -> std::result::Result<system_adapter_protocol::CreateStagingTableResponse, String> {
        let (storage, sql_url, api_key) = {
            let state = self
                .runs
                .get(&run_id)
                .ok_or_else(|| format!("No active run found for {run_id}"))?;
            let storage = match state {
                RunState::Scp(scp) => scp.storage.clone(),
                RunState::Local(local) => local.storage.clone(),
            };
            (
                storage,
                state.sql_url().to_string(),
                state.api_key().map(std::string::ToString::to_string),
            )
        };

        if !self
            .run_datasets
            .get(&run_id)
            .map_or(false, |ds| ds.contains_key(source_dataset))
        {
            return Err(format!(
                "Source dataset '{source_dataset}' not found in run {run_id}"
            ));
        }

        if let FederatedStorageConfig::Postgres { pg, .. }
        | FederatedStorageConfig::PostgresDebezium { pg, .. } = &storage
        {
            let source_config = self
                .run_datasets
                .get(&run_id)
                .and_then(|ds| ds.get(source_dataset))
                .ok_or_else(|| {
                    format!("Source dataset '{source_dataset}' not found in run {run_id}")
                })?;
            let ddl = pg_create_table_ddl(&pg.schema, staging_table_name, source_config).map_err(
                |e| format!("Failed to generate staging table DDL for '{staging_table_name}': {e}"),
            )?;
            let client = pg
                .connect()
                .await
                .map_err(|e| format!("Failed to connect to PostgreSQL: {e}"))?;
            client
                .execute(
                    ddl.as_str(),
                    &[] as &[&(dyn tokio_postgres::types::ToSql + Sync)],
                )
                .await
                .map_err(|e| {
                    format!(
                        "Failed to create staging table '{staging_table_name}': {}",
                        pg_error_message(&e)
                    )
                })?;
        } else {
            let quoted_staging = quote_identifier(staging_table_name);
            let quoted_source = quote_identifier(source_dataset);
            let ddl = format!(
                "CREATE TABLE IF NOT EXISTS spicebench.bench.{quoted_staging} LIKE spicebench.bench.{quoted_source}"
            );
            eprintln!(
                "[stdio] create_staging_table: source={source_dataset}, staging={staging_table_name}, sql={ddl}"
            );
            execute_sql_statement(&sql_url, api_key.as_deref(), &ddl)
                .await
                .map_err(|e| format!("Failed to execute staging table DDL: {e}"))?;
        }

        Ok(system_adapter_protocol::CreateStagingTableResponse { ok: true })
    }
}

pub async fn run_stdio_server(args: &StdioArgs) -> anyhow::Result<()> {
    let scenario = if let Some(scenario_name) = &args.scenario {
        let s = load_scenario(scenario_name, args.scenario_base_path.as_deref())
            .map_err(|e| anyhow::anyhow!("Failed to load scenario '{scenario_name}': {e}"))?;
        eprintln!(
            "[stdio] Loaded scenario '{scenario_name}': source={:?}",
            s.source
        );
        s
    } else {
        ScenarioConfig {
            compute: None,
            acceleration: None,
            source: SourceConfig::Direct(DirectConfig::default()),
        }
    };

    let handler = SpidapterHandler::new(args, scenario);
    let mut server = Server::new(handler);
    tokio::select! {
        r = server.run_stdio() => {
            r.map_err(|e| anyhow::anyhow!("Stdio server error: {e}"))
        }
        _ = tokio::signal::ctrl_c() => {
            eprintln!("[stdio] Received interrupt, cleaning up resources...");
            Ok(())
        }
    }
}

async fn post_setup_sink_action(
    datasets: &HashMap<String, DatasetConfig>,
    sql_url: &str,
    api_key: Option<&str>,
) -> anyhow::Result<()> {
    eprintln!("[stdio] Executing post-setup actions for direct-ingest sink...");

    let create_table_statements = generate_adbc_create_table_statements(datasets)?;
    if create_table_statements.is_empty() {
        eprintln!("[stdio] No datasets configured, skipping table creation");
        return Ok(());
    }

    for statement in create_table_statements {
        eprintln!("[stdio] Running post-setup SQL: {statement}");
        execute_sql_statement(sql_url, api_key, &statement).await?;
    }

    eprintln!("[stdio] direct-ingest post-setup table creation complete");
    Ok(())
}

async fn execute_sql_statement(
    sql_url: &str,
    api_key: Option<&str>,
    statement: &str,
) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_mins(1))
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
    let spicepod_path = working_dir.join("spicepod.yaml");
    eprintln!(
        "[stdio] Generated local spicepod ({} bytes) at {}:\n{spicepod_yaml}",
        spicepod_yaml.len(),
        spicepod_path.display()
    );
    tokio::fs::write(&spicepod_path, &spicepod_yaml).await?;
    Ok(spicepod_path)
}

fn serialize_spicepod(spicepod: &SpicepodDefinition) -> anyhow::Result<String> {
    yaml::to_string(spicepod).map_err(|e| anyhow::anyhow!("Failed to serialize spicepod: {e}"))
}

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
///
/// `cayenne` is only used when `setup_config.storage` is `DirectIngest`.
/// `scp` provides the scheduler state location and query memory limit.
async fn generate_initial_spicepod(
    run_id: &Uuid,
    setup_config: &SetupConfig,
    datasets: &HashMap<String, DatasetConfig>,
    flight_api_key: Option<&str>,
    _args: &StdioArgs,
    scp: &ScpConfig,
    cayenne: Option<&CayenneConfig>,
) -> anyhow::Result<SpicepodDefinition> {
    // Local-bench fallback: when no scenario `compute.scp` block provides a
    // scheduler state location, honor the SCHEDULER_STATE_LOCATION env var
    // (passed by `spicebench run` via --system-adapter-env). spiced refuses to
    // start in scheduler mode without `runtime.scheduler.state_location`, and
    // the local backend otherwise leaves it unset.
    let scheduler_state_location_env = std::env::var("SCHEDULER_STATE_LOCATION")
        .ok()
        .filter(|s| !s.trim().is_empty());
    let scheduler_state_location = scp
        .scheduler_state_location
        .as_deref()
        .filter(|s| !s.trim().is_empty())
        .or(scheduler_state_location_env.as_deref());

    let mut spicepod = if let Some(path) = setup_config.spicepod_path.as_deref() {
        load_spicepod_from_path(path, run_id).await?
    } else {
        match &setup_config.storage {
            FederatedStorageConfig::Postgres {
                pg, acceleration, ..
            } => generate_postgres_wal_spicepod(
                run_id,
                pg,
                datasets,
                acceleration_engine_str(*acceleration),
            ),
            FederatedStorageConfig::Direct => generate_cayenne_sink_spicepod(
                run_id,
                flight_api_key,
                cayenne,
                scp.query_memory_limit.as_deref(),
            ),
            FederatedStorageConfig::DynamoDB {
                prefix,
                acceleration,
                ..
            } => generate_dynamodb_spicepod(
                run_id,
                setup_config,
                datasets,
                prefix,
                acceleration_engine_str(*acceleration),
            ),
            FederatedStorageConfig::PostgresDebezium {
                pg,
                kafka_brokers,
                acceleration,
                ..
            } => generate_postgres_debezium_spicepod(
                run_id,
                kafka_brokers,
                pg,
                acceleration_engine_str(*acceleration),
                datasets,
            ),
            FederatedStorageConfig::MongoDB { uri, acceleration } => generate_mongodb_spicepod(
                run_id,
                uri,
                datasets,
                acceleration_engine_str(*acceleration),
            ),
        }
    };

    if let Some(loc) = scheduler_state_location {
        let mut path = loc.trim();
        if let Some(p) = path.strip_suffix('/') {
            path = p;
        }
        let state_location = format!("{path}/{run_id}");
        if !path.is_empty() {
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
    use std::sync::Arc;

    fn test_stdio_args() -> StdioArgs {
        StdioArgs {
            verbose: false,
            scenario: None,
            scenario_base_path: None,
            ready_wait: 600,
            api_key: None,
            spice_cloud_api_url: "https://api.spice.ai".to_string(),
            spiced_binary: "spiced".to_string(),
            spice_debug: false,
        }
    }

    fn test_scp_config() -> ScpConfig {
        ScpConfig {
            scheduler_state_location: Some("s3://bucket/state".to_string()),
            ..ScpConfig::default()
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
    fn with_mongodb_database_replaces_path() {
        // Existing database in the path is replaced.
        assert_eq!(
            with_mongodb_database("mongodb+srv://u:p@host/old?tls=true", "spidapter_abc"),
            "mongodb+srv://u:p@host/spidapter_abc?tls=true"
        );
        // No database in the path: one is appended.
        assert_eq!(
            with_mongodb_database("mongodb+srv://u:p@host/?retryWrites=true", "db1"),
            "mongodb+srv://u:p@host/db1?retryWrites=true"
        );
        // No path and no query at all.
        assert_eq!(
            with_mongodb_database("mongodb://localhost:27017", "db1"),
            "mongodb://localhost:27017/db1"
        );
        // No query string, existing db path.
        assert_eq!(
            with_mongodb_database("mongodb://localhost:27017/test", "db1"),
            "mongodb://localhost:27017/db1"
        );
    }

    #[test]
    fn compute_mode_scp_parses() {
        use clap::ValueEnum;
        assert!(matches!(
            SpiceCompute::from_str("scp", true),
            Ok(SpiceCompute::Scp)
        ));
    }

    #[test]
    fn compute_mode_supports_local() {
        use clap::ValueEnum;
        assert!(matches!(
            SpiceCompute::from_str("local", true),
            Ok(SpiceCompute::Local)
        ));
    }

    #[test]
    fn compute_mode_rejects_unknown_values() {
        use clap::ValueEnum;
        SpiceCompute::from_str("unexpected", true)
            .expect_err("unknown compute mode should be rejected");
    }

    #[tokio::test]
    async fn generate_spicepod_includes_cayenne_catalog() {
        let setup_config = SetupConfig {
            region: None,
            spicepod_path: None,
            storage: FederatedStorageConfig::Direct,
            aws_region_override: None,
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
        let scp = test_scp_config();
        let spicepod = generate_initial_spicepod(
            &Uuid::nil(),
            &setup_config,
            &datasets,
            None,
            &args,
            &scp,
            None,
        )
        .await
        .expect("spicepod should generate");
        let spicepod_yaml =
            serialize_spicepod(&spicepod).expect("spicepod should serialize to YAML");

        assert!(
            spicepod_yaml.contains("from: cayenne"),
            "expected cayenne provider: {spicepod_yaml}"
        );
        assert!(
            spicepod_yaml.contains("name: spicebench"),
            "expected spicebench catalog name: {spicepod_yaml}"
        );
        assert!(
            spicepod_yaml.contains("telemetry:"),
            "expected telemetry config: {spicepod_yaml}"
        );
        assert!(
            spicepod_yaml.contains("enabled: false"),
            "expected telemetry disabled: {spicepod_yaml}"
        );
    }

    #[tokio::test]
    async fn generate_spicepod_uses_loaded_spicepod_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("input.yaml");
        tokio::fs::write(
            &path,
            "version: v1\nkind: Spicepod\nname: user-supplied\ndatasets:\n  - from: s3://public-bucket/customer.parquet\n    name: customer\n    params:\n      file_format: parquet\n      s3_auth: public\n",
        )
        .await
        .expect("write yaml");

        let setup_config = SetupConfig {
            region: None,
            spicepod_path: Some(path.to_string_lossy().into_owned()),
            storage: FederatedStorageConfig::Direct,
            aws_region_override: None,
        };
        let datasets: HashMap<String, DatasetConfig> = HashMap::new();
        let args = test_stdio_args();
        let scp = test_scp_config();
        let run_id = Uuid::parse_str("01234567-89ab-cdef-0123-456789abcdef").expect("parse uuid");

        let spicepod =
            generate_initial_spicepod(&run_id, &setup_config, &datasets, None, &args, &scp, None)
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
