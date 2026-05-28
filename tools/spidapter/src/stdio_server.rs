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
    AdbcDriver, DatasetConfig, Handler, IngestionMetrics, MetricsResponse, ResourceMetrics, Server,
    SetupResponse, SinkConfig, TeardownResponse,
};
use tokio::process::Child;
use tokio::time::sleep;
use uuid::Uuid;

use crate::args::{AccelerationEngine, DeploymentMode, FederatedStorage, SpiceCompute, StdioArgs};
use crate::commands;

#[path = "sources/mod.rs"]
mod sources;

#[path = "compute/scp.rs"]
mod compute_scp;

#[path = "compute/local.rs"]
mod compute_local;

use compute_local::{
    provision_local_single_node, provision_local_spiced_cluster, teardown_local_run,
};
use compute_scp::provision_scp_app;
use sources::cayenne::generate_cayenne_sink_spicepod;
use sources::dynamodb::{
    DynamoDbTeardownInfo, create_dynamodb_tables, delete_dynamodb_tables,
    generate_dynamodb_spicepod,
};
use sources::ec2_debezium::launch_ec2_debezium;
use sources::ec2_postgres::{
    Ec2PostgresInstance, is_ec2_mode, launch_postgres_ec2, terminate_ec2_instance,
};
use sources::postgres_cdc::{
    PgConfig, generate_postgres_wal_spicepod, pg_create_table_ddl, pg_error_message,
    setup_postgres_for_wal, teardown_postgres, tpch_schema_name,
};
use sources::postgres_debezium::{
    generate_postgres_debezium_spicepod, register_debezium_postgres_connector,
    setup_postgres_for_debezium,
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

/// EC2 instance provisioned for a benchmark run (used for teardown).
#[derive(Debug, Clone)]
struct Ec2InstanceInfo {
    instance_id: String,
    region: String,
}

/// RAII guard that terminates an EC2 instance when dropped.
struct Ec2Guard {
    instance_id: String,
    region: String,
}

impl Drop for Ec2Guard {
    fn drop(&mut self) {
        let instance_id = self.instance_id.clone();
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
                eprintln!(
                    "[stdio] Ec2Guard: failed to terminate instance {instance_id}: {e}"
                );
            }
        })
        .join()
        .ok();
    }
}

/// RAII guard that deletes DynamoDB tables when dropped.
struct DynamoDbGuard {
    info: DynamoDbTeardownInfo,
}

impl Drop for DynamoDbGuard {
    fn drop(&mut self) {
        eprintln!(
            "[stdio] DynamoDbGuard: deleting {} table(s)",
            self.info.table_names.len()
        );
        let info = self.info.clone();
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

/// Configuration for the federated storage backend for a benchmark run.
#[derive(Debug, Clone)]
enum FederatedStorageConfig {
    Cayenne,
    Postgres {
        pg: PgConfig,
        acceleration: AccelerationEngine,
        ec2: Option<Ec2InstanceInfo>,
    },
    PostgresDebezium {
        pg: PgConfig,
        kafka_brokers: String,
        debezium_connect_url: String,
        acceleration: AccelerationEngine,
        ec2: Option<Ec2InstanceInfo>,
        ec2_debezium: Option<Ec2InstanceInfo>,
    },
    DynamoDB {
        prefix: String,
        region: String,
        acceleration: AccelerationEngine,
    },
}

impl FederatedStorageConfig {
    fn deployment_mode(&self) -> DeploymentMode {
        match self {
            Self::Cayenne => DeploymentMode::Cluster,
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
    /// Absolute path to a spicepod the client wants deployed verbatim
    /// (testoperator's cluster-bench path passes this; spicebench leaves it
    /// empty and relies on the `datasets` JSON-RPC parameter instead).
    spicepod_path: Option<String>,
    storage: FederatedStorageConfig,
    /// Explicit AWS region from CLI args — overrides `region` (from `etl_region` metadata)
    /// for the `DynamoDB` write path so the benchmark source region doesn't bleed through.
    aws_region_override: Option<String>,
}

impl SetupConfig {
    fn from_metadata(metadata: &HashMap<String, serde_json::Value>) -> Self {
        Self {
            region: metadata_string(metadata, "etl_region"),
            spicepod_path: metadata_string(metadata, "spicepod_path"),
            storage: FederatedStorageConfig::Cayenne,
            aws_region_override: None,
        }
    }

    fn set_storage(mut self, storage: FederatedStorageConfig) -> Self {
        self.storage = storage;
        self
    }
}

fn resolve_aws_region(setup_config: &SetupConfig) -> String {
    // CLI --aws-region wins over etl_region metadata so the benchmark source
    // region (e.g. S3 us-east-1) doesn't route DynamoDB traffic to localhost
    // via a /etc/hosts redirect meant for a different service.
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
    ) -> Result<SetupResponse, String> {
        eprintln!(
            "[stdio] setup: run_id={run_id}, metadata_keys={:?}",
            metadata.keys().collect::<Vec<_>>()
        );

        // Guards accumulate as resources are provisioned. If setup returns Err at any
        // point before they are moved into RunState, their Drop impls clean up.
        let mut ec2_guards: Vec<Ec2Guard> = Vec::new();
        let mut dynamodb_guard: Option<DynamoDbGuard> = None;

        // Build the FederatedStorageConfig (same logic as setup()).
        let storage = match self.args.storage {
            FederatedStorage::Cayenne => FederatedStorageConfig::Cayenne,
            FederatedStorage::Postgres => {
                let run_id_str = run_id.to_string();
                let short_id = run_id_str.split('-').next().unwrap_or_default();

                let ec2_instance: Option<Ec2PostgresInstance> = if is_ec2_mode(&self.args) {
                    let instance = launch_postgres_ec2(&self.args, short_id)
                        .await
                        .map_err(|e| format!("Failed to provision EC2 PostgreSQL instance: {e}"))?;
                    ec2_guards.push(Ec2Guard {
                        instance_id: instance.instance_id.clone(),
                        region: instance.region.clone(),
                    });
                    Some(instance)
                } else {
                    None
                };

                let pg = if let Some(ref ec2) = ec2_instance {
                    Some(PgConfig {
                        host: ec2.host.clone(),
                        port: ec2.pg_port,
                        user: ec2.pg_user.clone(),
                        password: ec2.pg_password.clone(),
                        database: ec2.pg_database.clone(),
                        schema: tpch_schema_name(&run_id),
                    })
                } else {
                    PgConfig::from_args(&self.args, &run_id)
                };

                let pg = pg.ok_or_else(|| {
                    "FederatedStorage::Postgres requires PG_HOST or EC2 provisioning".to_string()
                })?;

                setup_postgres_for_wal(&pg, &datasets)
                    .await
                    .map_err(|e| format!("Failed to set up PostgreSQL for WAL CDC: {e}"))?;

                let ec2 = ec2_instance.map(|e| Ec2InstanceInfo {
                    instance_id: e.instance_id,
                    region: e.region,
                });

                FederatedStorageConfig::Postgres {
                    pg,
                    acceleration: self.args.acceleration,
                    ec2,
                }
            }
            FederatedStorage::PostgresDebezium => {
                let run_id_str = run_id.to_string();
                let short_id = run_id_str.split('-').next().unwrap_or_default();

                // Launch Postgres EC2 (if EC2 mode) and Debezium EC2 concurrently.
                let (ec2_pg_result, ec2_deb_result) = if is_ec2_mode(&self.args) {
                    let (pg_res, deb_res) = tokio::join!(
                        launch_postgres_ec2(&self.args, short_id),
                        launch_ec2_debezium(&self.args, short_id)
                    );
                    (Some(pg_res), deb_res)
                } else {
                    (None, launch_ec2_debezium(&self.args, short_id).await)
                };

                // Push guards for successful launches before unwrapping errors — this
                // ensures the successfully-provisioned instance is terminated if its
                // counterpart failed.
                let ec2_pg_instance: Option<Ec2PostgresInstance> = match ec2_pg_result {
                    None => None,
                    Some(Ok(inst)) => {
                        ec2_guards.push(Ec2Guard {
                            instance_id: inst.instance_id.clone(),
                            region: inst.region.clone(),
                        });
                        Some(inst)
                    }
                    Some(Err(e)) => {
                        return Err(format!("Failed to provision EC2 PostgreSQL instance: {e}"));
                    }
                };

                let ec2_deb = match ec2_deb_result {
                    Ok(inst) => {
                        ec2_guards.push(Ec2Guard {
                            instance_id: inst.instance_id.clone(),
                            region: inst.region.clone(),
                        });
                        inst
                    }
                    Err(e) => {
                        return Err(format!("Failed to provision EC2 Debezium instance: {e}"));
                    }
                };

                let pg = if let Some(ref ec2) = ec2_pg_instance {
                    Some(PgConfig {
                        host: ec2.host.clone(),
                        port: ec2.pg_port,
                        user: ec2.pg_user.clone(),
                        password: ec2.pg_password.clone(),
                        database: ec2.pg_database.clone(),
                        schema: tpch_schema_name(&run_id),
                    })
                } else {
                    PgConfig::from_args(&self.args, &run_id)
                };

                let pg = pg.ok_or_else(|| {
                    "FederatedStorage::PostgresDebezium requires PG_HOST or EC2 provisioning"
                        .to_string()
                })?;

                setup_postgres_for_debezium(&pg, &datasets)
                    .await
                    .map_err(|e| format!("Failed to set up PostgreSQL for Debezium CDC: {e}"))?;

                let kafka_brokers = ec2_deb.kafka_brokers.clone();
                let debezium_connect_url = ec2_deb.connect_url.clone();
                let ec2_debezium = Some(Ec2InstanceInfo {
                    instance_id: ec2_deb.instance_id.clone(),
                    region: ec2_deb.region.clone(),
                });
                let ec2 = ec2_pg_instance.map(|e| Ec2InstanceInfo {
                    instance_id: e.instance_id,
                    region: e.region,
                });

                FederatedStorageConfig::PostgresDebezium {
                    pg,
                    kafka_brokers,
                    debezium_connect_url,
                    acceleration: self.args.acceleration,
                    ec2,
                    ec2_debezium,
                }
            }
            FederatedStorage::DynamoDB => {
                let region = self
                    .args
                    .aws_region
                    .clone()
                    .or_else(|| std::env::var("AWS_REGION").ok())
                    .or_else(|| std::env::var("AWS_DEFAULT_REGION").ok())
                    .or_else(|| metadata_string(&metadata, "etl_region"))
                    .unwrap_or_else(|| "us-east-1".to_string());
                FederatedStorageConfig::DynamoDB {
                    prefix: String::new(),
                    region,
                    acceleration: self.args.acceleration,
                }
            }
        };

        let mut setup_config = SetupConfig::from_metadata(&metadata).set_storage(storage);
        setup_config.aws_region_override = self.args.aws_region.clone();

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
            dynamodb_guard = Some(DynamoDbGuard {
                info: DynamoDbTeardownInfo { table_names, region },
            });
            if let FederatedStorageConfig::DynamoDB {
                prefix: ref mut p, ..
            } = setup_config.storage
            {
                *p = prefix;
            }
        }

        // Debezium/PostgreSQL: register the connector so Kafka topics are created
        // before spicebench starts writing data.
        if let FederatedStorageConfig::PostgresDebezium {
            ref pg,
            ref debezium_connect_url,
            ..
        } = setup_config.storage
        {
            let table_names: Vec<&str> = datasets.keys().map(String::as_str).collect();
            register_debezium_postgres_connector(
                debezium_connect_url,
                pg,
                &pg.host,
                &table_names,
            )
            .await
            .map_err(|e| format!("Failed to register Debezium PostgreSQL connector: {e}"))?;
        }

        // For Cayenne: provision spiced first (Flight URL needed to build SinkConfig).
        // For all other backends: build SinkConfig first, then provision spiced.
        let (sink, mut state) = match &setup_config.storage {
            FederatedStorageConfig::Cayenne => {
                let deployment_mode = setup_config.storage.deployment_mode();
                let provision_result = match self.args.compute {
                    SpiceCompute::Cloud => {
                        provision_scp_app(
                            run_id,
                            &self.args,
                            &setup_config,
                            &datasets,
                            &deployment_mode,
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
                        )
                        .await
                    }
                };
                let mut state = match provision_result {
                    Ok(s) => s,
                    Err(e) => return Err(format!("Cayenne setup: provisioning failed: {e}")),
                };
                match &mut state {
                    RunState::Scp(scp) => scp.storage = setup_config.storage.clone(),
                    RunState::Local(local) => local.storage = setup_config.storage.clone(),
                }

                let sql_url = state.sql_url().to_string();
                let api_key = state.api_key().map(str::to_string);
                post_setup_sink_action(&datasets, &sql_url, api_key.as_deref())
                    .await
                    .map_err(|e| format!("Cayenne post-setup SQL failed: {e}"))?;

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
            }
            _ => {
                let sink = match &setup_config.storage {
                    FederatedStorageConfig::Postgres { pg, .. } => {
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
                    FederatedStorageConfig::PostgresDebezium { pg, .. } => {
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
                    FederatedStorageConfig::Cayenne => unreachable!(),
                };

                let deployment_mode = setup_config.storage.deployment_mode();
                let provision_result = match self.args.compute {
                    SpiceCompute::Cloud => {
                        provision_scp_app(
                            run_id,
                            &self.args,
                            &setup_config,
                            &datasets,
                            &deployment_mode,
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
                        // ec2_guards and dynamodb_guard drop here, cleaning up AWS resources.
                        return Err(format!("Setup failed: provisioning failed: {e}"));
                    }
                };

                (sink, state)
            }
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
            FederatedStorageConfig::Cayenne => Some("spicebench.bench".to_string()),
            FederatedStorageConfig::DynamoDB { prefix, .. } if !prefix.is_empty() => {
                Some(prefix.clone())
            }
            _ => None,
        };

        // Move guards into the run state so they live for the duration of the run.
        match &mut state {
            RunState::Scp(scp) => {
                scp.ec2_guards = ec2_guards;
                scp.dynamodb_guard = dynamodb_guard;
            }
            RunState::Local(local) => {
                local.ec2_guards = ec2_guards;
                local.dynamodb_guard = dynamodb_guard;
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

        let Some(mut state) = self.runs.remove(&run_id) else {
            eprintln!("[stdio] teardown: run_id={run_id} not found (already torn down?)");
            return Ok(TeardownResponse { ok: true });
        };
        let run_datasets = self.run_datasets.remove(&run_id).unwrap_or_default();

        let storage = match &state {
            RunState::Scp(scp) => scp.storage.clone(),
            RunState::Local(local) => local.storage.clone(),
        };

        // Extract guards now; they drop at end of function, AFTER Postgres cleanup below.
        // This ensures the EC2 instance (which hosts Postgres) is still alive during teardown.
        let (ec2_guards, dynamodb_guard) = match &mut state {
            RunState::Scp(scp) => (
                std::mem::take(&mut scp.ec2_guards),
                scp.dynamodb_guard.take(),
            ),
            RunState::Local(local) => (
                std::mem::take(&mut local.ec2_guards),
                local.dynamodb_guard.take(),
            ),
        };

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

        // Postgres cleanup must happen before the EC2 guards drop (EC2 hosts Postgres).
        match &storage {
            FederatedStorageConfig::Postgres { pg, .. }
            | FederatedStorageConfig::PostgresDebezium { pg, .. } => {
                teardown_postgres(pg, &run_datasets)
                    .await
                    .map_err(|e| format!("Failed to teardown PostgreSQL: {e}"))?;
            }
            FederatedStorageConfig::DynamoDB { .. } | FederatedStorageConfig::Cayenne => {}
        }

        // Guards drop here: EC2 instances are terminated, DynamoDB tables are deleted.
        drop(ec2_guards);
        drop(dynamodb_guard);

        Ok(TeardownResponse { ok: true })
    }

    async fn create_staging_table(
        &mut self,
        run_id: Uuid,
        source_dataset: &str,
        staging_table_name: &str,
    ) -> std::result::Result<system_adapter_protocol::CreateStagingTableResponse, String> {
        // Extract state fields upfront, cloning to release the borrow before async ops.
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
            // PostgresCdc/Debezium: create staging table directly in PostgreSQL.
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
            // eprintln!(
            //     "[stdio] create_staging_table (postgres): source={source_dataset}, staging={staging_table_name}, sql={ddl}"
            // );
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
            // Cayenne/other: copy schema from source via Spice SQL.
            // Use CREATE TABLE ... LIKE ... to copy schema, partition expression,
            // and partition-to-executor assignments from the source table.
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
    let handler = SpidapterHandler::new(args);
    let mut server = Server::new(handler);
    server
        .run_stdio()
        .await
        .map_err(|e| anyhow::anyhow!("Stdio server error: {e}"))
}

async fn post_setup_sink_action(
    datasets: &HashMap<String, DatasetConfig>,
    sql_url: &str,
    api_key: Option<&str>,
) -> anyhow::Result<()> {
    eprintln!("[stdio] Executing post-setup actions for Cayenne ADBC sink...");

    let create_table_statements = generate_adbc_create_table_statements(datasets)?;
    if create_table_statements.is_empty() {
        eprintln!("[stdio] No datasets configured, skipping table creation");
        return Ok(());
    }

    for statement in create_table_statements {
        eprintln!("[stdio] Running post-setup SQL: {statement}");
        execute_sql_statement(sql_url, api_key, &statement).await?;
    }

    eprintln!("[stdio] Cayenne post-setup table creation complete");
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
        match &setup_config.storage {
            FederatedStorageConfig::Postgres {
                pg, acceleration, ..
            } => generate_postgres_wal_spicepod(
                run_id,
                pg,
                datasets,
                acceleration_engine_str(*acceleration),
            ),
            FederatedStorageConfig::Cayenne => {
                generate_cayenne_sink_spicepod(run_id, flight_api_key, args)
            }
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
    use std::sync::Arc;

    fn test_stdio_args() -> StdioArgs {
        StdioArgs {
            verbose: false,
            spice_cloud_api_url: "https://api.spice.ai".to_string(),
            ready_wait: 600,
            channel: None,
            image_tag: None,
            api_key: None,
            compute: SpiceCompute::Cloud,
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
            storage: FederatedStorage::Cayenne,
            pg_host: None,
            pg_port: 5432,
            pg_user: None,
            pg_password: String::new(),
            pg_database: None,
            acceleration: AccelerationEngine::Cayenne,
            ec2_subnet_id: None,
            ec2_security_group_id: None,
            ec2_ami_id: None,
            ec2_instance_type: "m5.large".to_string(),
            ec2_associate_public_ip: false,
            ec2_iam_instance_profile: None,
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
    fn compute_mode_cloud_is_default() {
        use clap::ValueEnum;
        assert!(matches!(
            SpiceCompute::from_str("cloud", true),
            Ok(SpiceCompute::Cloud)
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
            storage: FederatedStorageConfig::Cayenne,
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
        let spicepod =
            generate_initial_spicepod(&Uuid::nil(), &setup_config, &datasets, None, &args)
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
            spicepod_path: Some(path.to_string_lossy().into_owned()),
            storage: FederatedStorageConfig::Cayenne,
            aws_region_override: None,
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
