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
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::client::FlightSqlServiceClient;
use async_trait::async_trait;
use system_adapter_protocol::{
    AdbcDriver, CreateStagingTableResponse, DatasetConfig, EtlSinkType, Handler, IngestionMetrics,
    MetricsResponse, ResourceMetrics, Server, SetupResponse, TeardownResponse,
};
use tokio::process::{Child, Command as TokioCommand};
use tokio::time::sleep;
use uuid::Uuid;

use crate::args::CayenneFlightsqlArgs;
use crate::stdio_server::{adbc_sql_type_for_arrow, quote_identifier};

const CONNECT_HOST: &str = "127.0.0.1";

/// State for a single active cayenne-flightsql run.
struct CayenneRunState {
    child: Child,
    /// grpc endpoint as an `http://` URL suitable for tonic (DDL execution).
    grpc_http_url: String,
    /// Catalog name registered in the `DataFusion` session.
    catalog: String,
    /// Default schema name.
    schema: String,
    /// Dataset configs from setup — used to generate staging table DDL.
    datasets: HashMap<String, DatasetConfig>,
    /// Per-run working directory — removed on teardown.
    working_dir: PathBuf,
}

impl Drop for CayenneRunState {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

struct CayenneFlightsqlHandler {
    runs: HashMap<Uuid, CayenneRunState>,
    args: CayenneFlightsqlArgs,
}

impl CayenneFlightsqlHandler {
    fn new(args: &CayenneFlightsqlArgs) -> Self {
        Self {
            runs: HashMap::new(),
            args: args.clone(),
        }
    }
}

#[async_trait]
impl Handler for CayenneFlightsqlHandler {
    async fn setup(
        &mut self,
        run_id: Uuid,
        _metadata: HashMap<String, serde_json::Value>,
        datasets: HashMap<String, DatasetConfig>,
        etl_sink_type: Option<EtlSinkType>,
    ) -> Result<SetupResponse, String> {
        eprintln!("[cayenne] setup: run_id={run_id}");

        let port = reserve_local_port(CONNECT_HOST)
            .map_err(|e| format!("Failed to allocate local port: {e}"))?;

        let addr = format!("{CONNECT_HOST}:{port}");
        let flight_url = format!("grpc://{addr}");
        let grpc_http_url = format!("http://{addr}");

        // Create a per-run working directory for Cayenne data/metadata.
        let working_dir = std::env::temp_dir().join(format!("spidapter-cayenne-{run_id}"));
        tokio::fs::create_dir_all(&working_dir)
            .await
            .map_err(|e| format!("Failed to create working dir: {e}"))?;

        let cmd_args = build_cayenne_args(&self.args, &addr, &working_dir);

        eprintln!(
            "[cayenne] launching {}: {}",
            self.args.cayenne_flightsql_binary,
            cmd_args.join(" ")
        );

        let current_stderr = std::io::stderr();
        let child = TokioCommand::new(&self.args.cayenne_flightsql_binary)
            .kill_on_drop(true)
            .args(&cmd_args)
            .stdout(Stdio::from(current_stderr))
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| {
                format!(
                    "Failed to spawn '{}': {e}",
                    self.args.cayenne_flightsql_binary
                )
            })?;

        let mut state = CayenneRunState {
            child,
            grpc_http_url,
            catalog: self.args.catalog.clone(),
            schema: self.args.default_schema.clone(),
            datasets: datasets.clone(),
            working_dir,
        };

        wait_for_grpc_ready(
            &addr,
            &mut state.child,
            Duration::from_secs(self.args.ready_wait),
        )
        .await
        .map_err(|e| format!("cayenne-flightsql did not become ready: {e}"))?;

        eprintln!("[cayenne] setup: cayenne-flightsql ready at {flight_url}");

        // When used as an ADBC ETL sink, pre-create all target tables so the
        // benchmark framework can bulk-ingest into them immediately.
        if etl_sink_type == Some(EtlSinkType::Adbc) {
            create_adbc_tables(
                &state.grpc_http_url,
                &state.catalog,
                &state.schema,
                &datasets,
            )
            .await
            .map_err(|e| format!("Failed to create ADBC tables: {e}"))?;
        }

        let db_kwargs = HashMap::from([
            ("uri".to_string(), serde_json::Value::String(flight_url)),
            (
                "username".to_string(),
                serde_json::Value::String(String::new()),
            ),
            (
                "password".to_string(),
                serde_json::Value::String(String::new()),
            ),
        ]);

        // When used as an ADBC ETL sink, tell the framework which
        // catalog.schema to target for table creation and ingestion.
        let catalog_namespace = etl_sink_type
            .as_ref()
            .filter(|t| matches!(t, EtlSinkType::Adbc))
            .map(|_| format!("{}.{}", state.catalog, state.schema));

        let response = SetupResponse {
            driver: AdbcDriver::Flightsql,
            db_kwargs,
            catalog_namespace,
            read_driver: None,
            endpoints: HashMap::new(),
        };

        self.runs.insert(run_id, state);
        Ok(response)
    }

    async fn metrics(
        &mut self,
        run_id: Uuid,
        _final_scrape: bool,
    ) -> Result<MetricsResponse, String> {
        if !self.runs.contains_key(&run_id) {
            return Err(format!("No active run found for {run_id}"));
        }
        Ok(MetricsResponse {
            resource: ResourceMetrics::default(),
            ingestion: IngestionMetrics::default(),
        })
    }

    async fn teardown(&mut self, run_id: Uuid) -> Result<TeardownResponse, String> {
        eprintln!("[cayenne] teardown: run_id={run_id}");

        let Some(mut state) = self.runs.remove(&run_id) else {
            eprintln!("[cayenne] teardown: run_id={run_id} not found (already torn down?)");
            return Ok(TeardownResponse { ok: true });
        };

        match state.child.try_wait() {
            Ok(Some(status)) => {
                eprintln!(
                    "[cayenne] teardown: cayenne-flightsql already stopped with status {status}"
                );
            }
            Ok(None) => {
                state
                    .child
                    .kill()
                    .await
                    .map_err(|e| format!("Failed to kill cayenne-flightsql: {e}"))?;
                let status = state
                    .child
                    .wait()
                    .await
                    .map_err(|e| format!("Failed to wait for cayenne-flightsql: {e}"))?;
                eprintln!("[cayenne] teardown: cayenne-flightsql stopped with status {status}");
            }
            Err(e) => {
                return Err(format!("Failed to check cayenne-flightsql status: {e}"));
            }
        }

        if tokio::fs::metadata(&state.working_dir).await.is_ok() {
            if let Err(e) = tokio::fs::remove_dir_all(&state.working_dir).await {
                eprintln!(
                    "[cayenne] teardown: failed to remove {}: {e}",
                    state.working_dir.display()
                );
            } else {
                eprintln!(
                    "[cayenne] teardown: removed {}",
                    state.working_dir.display()
                );
            }
        }

        Ok(TeardownResponse { ok: true })
    }

    async fn create_staging_table(
        &mut self,
        run_id: Uuid,
        source_dataset: &str,
        staging_table_name: &str,
    ) -> Result<CreateStagingTableResponse, String> {
        let state = self
            .runs
            .get(&run_id)
            .ok_or_else(|| format!("No active run found for {run_id}"))?;

        let source = state.datasets.get(source_dataset).ok_or_else(|| {
            format!("Source dataset '{source_dataset}' not found in run {run_id}")
        })?;

        // `CREATE TABLE ... LIKE` requires CayenneDdlHandler which is not
        // registered in the standalone cayenne-flightsql server. Generate
        // explicit column-by-column DDL from the stored schema instead.
        let ddl =
            generate_create_table_ddl(&state.catalog, &state.schema, staging_table_name, source)
                .map_err(|e| format!("Failed to generate staging table DDL: {e}"))?;

        eprintln!(
            "[cayenne] create_staging_table: source={source_dataset}, \
             staging={staging_table_name}, sql={ddl}"
        );

        execute_flight_sql_update(&state.grpc_http_url, &ddl)
            .await
            .map_err(|e| format!("Failed to execute staging table DDL: {e}"))?;

        Ok(CreateStagingTableResponse { ok: true })
    }
}

pub async fn run_cayenne_flightsql_server(args: &CayenneFlightsqlArgs) -> anyhow::Result<()> {
    let handler = CayenneFlightsqlHandler::new(args);
    let mut server = Server::new(handler);
    server
        .run_stdio()
        .await
        .map_err(|e| anyhow::anyhow!("Stdio server error: {e}"))
}

/// Bind to an ephemeral port and return it, dropping the listener immediately so the port
/// is available for the child process to bind.
fn reserve_local_port(host: &str) -> anyhow::Result<u16> {
    let listener = TcpListener::bind((host, 0))?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

/// Build the CLI argument list to pass to `cayenne-flightsql`.
fn build_cayenne_args(
    args: &CayenneFlightsqlArgs,
    addr: &str,
    working_dir: &std::path::Path,
) -> Vec<String> {
    let mut cmd_args = vec![
        "--addr".to_string(),
        addr.to_string(),
        "--catalog".to_string(),
        args.catalog.clone(),
        "--default-schema".to_string(),
        args.default_schema.clone(),
        "--spice-data-base-path".to_string(),
        working_dir.to_string_lossy().into_owned(),
    ];

    if let Some(data_dir) = &args.cayenne_data_dir {
        cmd_args.extend(["--cayenne-data-dir".to_string(), data_dir.clone()]);
    }
    if let Some(metadata_dir) = &args.cayenne_metadata_dir {
        cmd_args.extend(["--cayenne-metadata-dir".to_string(), metadata_dir.clone()]);
    }
    if let Some(footer_cache) = args.cayenne_footer_cache_mb {
        cmd_args.extend([
            "--cayenne-footer-cache-mb".to_string(),
            footer_cache.to_string(),
        ]);
    }
    if let Some(segment_cache) = args.cayenne_segment_cache_mb {
        cmd_args.extend([
            "--cayenne-segment-cache-mb".to_string(),
            segment_cache.to_string(),
        ]);
    }
    if let Some(file_size) = args.cayenne_target_file_size_mb {
        cmd_args.extend([
            "--cayenne-target-file-size-mb".to_string(),
            file_size.to_string(),
        ]);
    }
    if let Some(interval) = args.refresh_interval_secs {
        cmd_args.extend(["--refresh-interval-secs".to_string(), interval.to_string()]);
    }

    cmd_args
}

/// Poll the given TCP address every 200 ms until the child accepts a connection or exits.
async fn wait_for_grpc_ready(
    addr: &str,
    child: &mut Child,
    timeout: Duration,
) -> anyhow::Result<()> {
    let started = tokio::time::Instant::now();

    loop {
        if let Some(status) = child.try_wait()? {
            return Err(anyhow::anyhow!(
                "cayenne-flightsql process exited early with status {status}"
            ));
        }

        if started.elapsed() > timeout {
            return Err(anyhow::anyhow!(
                "Timed out after {}s waiting for cayenne-flightsql at {addr}",
                timeout.as_secs()
            ));
        }

        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return Ok(());
        }

        sleep(Duration::from_millis(200)).await;
    }
}

/// Generate and execute `CREATE TABLE IF NOT EXISTS` DDL for every dataset,
/// targeting `<catalog>.<schema>.<table>` instead of the hardcoded
/// `spicebench.bench` namespace used by the spiced ADBC path.
async fn create_adbc_tables(
    grpc_http_url: &str,
    catalog: &str,
    schema: &str,
    datasets: &HashMap<String, DatasetConfig>,
) -> anyhow::Result<()> {
    let mut dataset_names: Vec<&String> = datasets.keys().collect();
    dataset_names.sort_unstable();

    for dataset_name in dataset_names {
        let dataset = &datasets[dataset_name];
        let ddl = generate_create_table_ddl(catalog, schema, dataset_name, dataset)?;
        eprintln!("[cayenne] create table: {ddl}");
        execute_flight_sql_update(grpc_http_url, &ddl).await?;
    }

    Ok(())
}

/// Build a `CREATE TABLE IF NOT EXISTS` statement for one dataset.
fn generate_create_table_ddl(
    catalog: &str,
    schema: &str,
    dataset_name: &str,
    dataset: &DatasetConfig,
) -> anyhow::Result<String> {
    use std::collections::HashSet;

    let quoted_name = quote_identifier(dataset_name);

    let column_definitions = dataset
        .schema
        .fields()
        .iter()
        .map(|field| {
            let col = quote_identifier(field.name());
            let ty = adbc_sql_type_for_arrow(field.data_type())?;
            let nullable = if field.is_nullable() { "" } else { " NOT NULL" };
            Ok(format!("{col} {ty}{nullable}"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    if column_definitions.is_empty() {
        anyhow::bail!("Dataset '{dataset_name}' has no columns");
    }

    let mut elements = column_definitions;

    if !dataset.primary_key_columns.is_empty() {
        let schema_cols: HashSet<_> = dataset.schema.fields().iter().map(|f| f.name()).collect();
        for pk in &dataset.primary_key_columns {
            if !schema_cols.contains(pk) {
                anyhow::bail!("Dataset '{dataset_name}' primary key '{pk}' not present in schema");
            }
        }
        let pks = dataset
            .primary_key_columns
            .iter()
            .map(|c| quote_identifier(c))
            .collect::<Vec<_>>()
            .join(", ");
        elements.push(format!("PRIMARY KEY ({pks})"));
    }

    // Omit PARTITION BY — cayenne-flightsql runs as a single node and
    // DataFusion does not support bucket() partition expressions.
    Ok(format!(
        "CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{quoted_name} ({})",
        elements.join(", ")
    ))
}

/// Execute a DDL/DML statement against the cayenne-flightsql gRPC endpoint
/// using the Flight SQL `CommandStatementUpdate` / `DoPut` path.
async fn execute_flight_sql_update(grpc_http_url: &str, sql: &str) -> anyhow::Result<i64> {
    let channel = tonic::transport::Channel::from_shared(grpc_http_url.to_string())
        .map_err(|e| anyhow::anyhow!("Invalid Flight SQL URL '{grpc_http_url}': {e}"))?
        .connect()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect to Flight SQL at {grpc_http_url}: {e}"))?;

    let flight_client = FlightServiceClient::new(channel);
    let mut client = FlightSqlServiceClient::new_from_inner(flight_client);

    client
        .execute_update(sql.to_string(), None)
        .await
        .map_err(|e| anyhow::anyhow!("Flight SQL execute_update failed: {e}"))
}
