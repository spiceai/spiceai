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

use arrow::datatypes::{DataType, TimeUnit};
use async_trait::async_trait;
use spice_cloud_client::CloudClient;
use system_adapter_protocol::{
    AdbcDriver, CreateTablesResponse, DatasetConfig, Handler, Server, SetupResponse,
    TeardownResponse,
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
    /// Region cname (used to build the SQL endpoint URL).
    cname: String,
    /// Table names created for this run (for cleanup during teardown).
    created_tables: Vec<String>,
    /// ETL sink mode used by this run.
    etl_sink_mode: EtlSinkMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EtlSinkMode {
    Adbc,
    IcebergObjectStore,
}

impl EtlSinkMode {
    fn from_setup_metadata(metadata: &HashMap<String, serde_json::Value>) -> anyhow::Result<Self> {
        let value = metadata
            .get("etl_sink_mode")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("adbc");

        match value {
            "adbc" => Ok(Self::Adbc),
            "iceberg-object-store" => Ok(Self::IcebergObjectStore),
            other => Err(anyhow::anyhow!(
                "Unsupported setup metadata etl_sink_mode '{other}'. Allowed values: adbc, iceberg-object-store"
            )),
        }
    }
}

#[derive(Debug, Clone)]
struct SetupConfig {
    etl_sink_mode: EtlSinkMode,
    etl_region: Option<String>,
    etl_endpoint: Option<String>,
    etl_iceberg_warehouse_uri: Option<String>,
    etl_iceberg_namespace: Option<String>,
}

impl SetupConfig {
    fn from_metadata(metadata: &HashMap<String, serde_json::Value>) -> anyhow::Result<Self> {
        Ok(Self {
            etl_sink_mode: EtlSinkMode::from_setup_metadata(metadata)?,
            etl_region: metadata_string(metadata, "etl_region"),
            etl_endpoint: metadata_string(metadata, "etl_endpoint"),
            etl_iceberg_warehouse_uri: metadata_string(metadata, "etl_iceberg_warehouse_uri"),
            etl_iceberg_namespace: metadata_string(metadata, "etl_iceberg_namespace"),
        })
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
    ) -> Result<SetupResponse, String> {
        eprintln!(
            "[stdio] setup: run_id={run_id}, metadata_keys={:?}",
            metadata.keys().collect::<Vec<_>>()
        );

        let setup_config = SetupConfig::from_metadata(&metadata)
            .map_err(|e| format!("Invalid setup metadata: {e}"))?;

        let state = provision_spice_cloud_app(
            run_id,
            self.api_url_override.as_deref(),
            self.ready_wait,
            self.channel.as_deref(),
            &setup_config,
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

    async fn create_tables(
        &mut self,
        run_id: Uuid,
        datasets: HashMap<String, DatasetConfig>,
    ) -> Result<CreateTablesResponse, String> {
        eprintln!("[stdio] create_tables: run_id={run_id}");

        let state = self
            .runs
            .get_mut(&run_id)
            .ok_or_else(|| format!("Unknown run_id: {run_id}"))?;

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .map_err(|e| format!("Failed to create HTTP client: {e}"))?;

        let sql_url = sql_url_from_cname(&state.cname);

        if state.etl_sink_mode == EtlSinkMode::IcebergObjectStore {
            eprintln!(
                "[stdio] create_tables: skipping SQL DDL for run_id={run_id} (etl_sink_mode=iceberg-object-store)"
            );
            return Ok(CreateTablesResponse { ok: true });
        }

        for (table_name, config) in &datasets {
            let ddl = create_table_ddl(table_name, config);
            eprintln!("[stdio] create_tables: executing DDL: {ddl}");

            let response = client
                .post(&sql_url)
                .header("X-API-Key", &state.api_key)
                .body(ddl.clone())
                .send()
                .await
                .map_err(|e| format!("Failed to send DDL for table '{table_name}': {e}"))?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "<failed to read body>".to_string());
                return Err(format!(
                    "DDL for table '{table_name}' failed ({status}): {body}"
                ));
            }

            state.created_tables.push(table_name.clone());
            eprintln!("[stdio] create_tables: table '{table_name}' created");
        }
        Ok(CreateTablesResponse { ok: true })
    }

    async fn teardown(&mut self, run_id: Uuid) -> Result<TeardownResponse, String> {
        eprintln!("[stdio] teardown: run_id={run_id}");

        let Some(state) = self.runs.remove(&run_id) else {
            eprintln!("[stdio] teardown: run_id={run_id} not found (already torn down?)");
            return Ok(TeardownResponse { ok: true });
        };

        // Drop tables via DDL before deleting the app
        if !state.created_tables.is_empty() {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .map_err(|e| format!("Failed to create HTTP client: {e}"))?;

            let sql_url = sql_url_from_cname(&state.cname);

            for table_name in &state.created_tables {
                let ddl = format!("DROP TABLE IF EXISTS {}", quote_ident(table_name));
                eprintln!("[stdio] teardown: executing DDL: {ddl}");

                match client
                    .post(&sql_url)
                    .header("X-API-Key", &state.api_key)
                    .body(ddl)
                    .send()
                    .await
                {
                    Ok(response) if response.status().is_success() => {
                        eprintln!("[stdio] teardown: table '{table_name}' dropped");
                    }
                    Ok(response) => {
                        let status = response.status();
                        let body = response
                            .text()
                            .await
                            .unwrap_or_else(|_| "<failed to read body>".to_string());
                        eprintln!(
                            "[stdio] teardown: failed to drop table '{table_name}' ({status}): {body}"
                        );
                    }
                    Err(e) => {
                        eprintln!("[stdio] teardown: failed to drop table '{table_name}': {e}");
                    }
                }
            }
        }

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

    // Generate initial spicepod YAML (tables created later via create_tables)
    let spicepod_yaml = generate_initial_spicepod(&run_id, setup_config)?;
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

    Ok(RunState {
        app_id,
        api_key,
        flight_url,
        cloud,
        cname,
        created_tables: Vec::new(),
        etl_sink_mode: setup_config.etl_sink_mode,
    })
}

/// Build the SQL endpoint URL from a Spice Cloud cname.
fn sql_url_from_cname(cname: &str) -> String {
    format!("https://{cname}.spiceai.io/v1/sql")
}

/// Generate a `CREATE TABLE IF NOT EXISTS` DDL statement from a dataset's Arrow schema,
/// with acceleration configured for the cayenne engine with append refresh every 10 seconds.
///
/// If the schema contains a timestamp column, it is used as the acceleration time column
/// with `timestamptz` format.
fn create_table_ddl(table_name: &str, config: &DatasetConfig) -> String {
    let columns: Vec<String> = config
        .schema
        .fields()
        .iter()
        .map(|f| {
            let sql_type = sql_type_for_arrow(f.data_type());
            let nullable = if f.is_nullable() { "" } else { " NOT NULL" };
            format!("{} {sql_type}{nullable}", quote_ident(f.name()))
        })
        .collect();

    let with_opts = [
        "\"acceleration.engine\" = 'cayenne'".to_string(),
        "\"acceleration.mode\" = 'file'".to_string(),
        "\"acceleration.refresh_mode\" = 'full'".to_string(),
        "\"acceleration.refresh_check_interval\" = '1s'".to_string(),
    ];

    format!(
        "CREATE TABLE IF NOT EXISTS {} ({}) WITH ({})",
        quote_ident(table_name),
        columns.join(", "),
        with_opts.join(", ")
    )
}

/// Map Arrow data types to SQL type names for DDL.
fn sql_type_for_arrow(data_type: &DataType) -> String {
    match data_type {
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 => "TINYINT".to_string(),
        DataType::UInt8 | DataType::Int16 => "SMALLINT".to_string(),
        DataType::UInt16 | DataType::Int32 => "INT".to_string(),
        DataType::UInt32 | DataType::Int64 => "BIGINT".to_string(),
        DataType::UInt64 => "DECIMAL(20, 0)".to_string(),
        DataType::Float32 => "FLOAT".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Timestamp(
            TimeUnit::Second | TimeUnit::Millisecond | TimeUnit::Microsecond | TimeUnit::Nanosecond,
            _,
        ) => "TIMESTAMP".to_string(),
        DataType::Decimal128(p, s) => format!("DECIMAL({p}, {s})"),
        DataType::Binary | DataType::LargeBinary => "BINARY".to_string(),
        _ => "VARCHAR".to_string(),
    }
}

fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Generate the initial spicepod YAML with an Iceberg Glue catalog as the default catalog.
fn generate_initial_spicepod(run_id: &Uuid, setup_config: &SetupConfig) -> anyhow::Result<String> {
    let run_id_str = run_id.to_string();
    let short_id = run_id_str.split('-').next().unwrap_or_default();
    let region = setup_config
        .etl_region
        .clone()
        .or_else(|| std::env::var("SPIDAPTER_ICEBERG_REGION").ok())
        .or_else(|| std::env::var("AWS_REGION").ok())
        .or_else(|| std::env::var("AWS_DEFAULT_REGION").ok())
        .unwrap_or_else(|| "us-east-1".to_string());

    match setup_config.etl_sink_mode {
        EtlSinkMode::IcebergObjectStore => {
            let warehouse_uri = setup_config
                .etl_iceberg_warehouse_uri
                .as_deref()
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Missing setup metadata etl_iceberg_warehouse_uri for etl_sink_mode=iceberg-object-store"
                    )
                })?;

            let mut params = vec![
                "      iceberg_s3_access_key_id: ${secrets:AWS_ACCESS_KEY_ID}".to_string(),
                "      iceberg_s3_secret_access_key: ${secrets:AWS_SECRET_ACCESS_KEY}".to_string(),
                format!("      iceberg_s3_region: {region}"),
            ];

            if let Some(endpoint) = setup_config
                .etl_endpoint
                .clone()
                .or_else(|| std::env::var("SPIDAPTER_ICEBERG_S3_ENDPOINT").ok())
            {
                params.push(format!("      iceberg_s3_endpoint: {endpoint}"));
            }

            if let Some(namespace) = &setup_config.etl_iceberg_namespace {
                eprintln!(
                    "[stdio] setup: iceberg object-store namespace from metadata: {namespace}"
                );
            }

            Ok(format!(
                "version: v1beta1
kind: Spicepod
name: spidapter-{short_id}

catalogs:
  - from: iceberg:{warehouse_uri}
    name: spice
    access: read
    params:
{}
",
                params.join("\n")
            ))
        }
        EtlSinkMode::Adbc => {
            let catalog_from = if let Ok(from) = std::env::var("SPIDAPTER_ICEBERG_CATALOG_FROM") {
                from
            } else {
                let account_id = std::env::var("SPIDAPTER_ICEBERG_CATALOG_ACCOUNT_ID")
                    .or_else(|_| std::env::var("AWS_ICEBERG_ACCOUNT_ID"))
                    .map_err(|_| {
                        anyhow::anyhow!(
                            "Missing Iceberg catalog account id. Set SPIDAPTER_ICEBERG_CATALOG_FROM or SPIDAPTER_ICEBERG_CATALOG_ACCOUNT_ID (or AWS_ICEBERG_ACCOUNT_ID)."
                        )
                    })?;

                format!(
                    "iceberg:https://glue.{region}.amazonaws.com/iceberg/v1/catalogs/{account_id}/namespaces"
                )
            };

            Ok(format!(
                "version: v1beta1
kind: Spicepod
name: spidapter-{short_id}

catalogs:
  - from: {catalog_from}
    name: spice
    access: read_write_create
    params:
      iceberg_sigv4_enabled: true
      iceberg_s3_access_key_id: ${{secrets:AWS_ACCESS_KEY_ID}}
      iceberg_s3_secret_access_key: ${{secrets:AWS_SECRET_ACCESS_KEY}}
      iceberg_s3_region: {region}
"
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn setup_config_defaults_to_adbc_sink_mode() {
        let metadata = HashMap::new();
        let config = SetupConfig::from_metadata(&metadata).expect("metadata should parse");
        assert_eq!(config.etl_sink_mode, EtlSinkMode::Adbc);
    }

    #[test]
    fn setup_config_parses_iceberg_object_store_sink_mode() {
        let metadata = HashMap::from([(
            "etl_sink_mode".to_string(),
            serde_json::Value::String("iceberg-object-store".to_string()),
        )]);

        let config = SetupConfig::from_metadata(&metadata).expect("metadata should parse");
        assert_eq!(config.etl_sink_mode, EtlSinkMode::IcebergObjectStore);
    }

    #[test]
    fn setup_config_rejects_unknown_sink_mode() {
        let metadata = HashMap::from([(
            "etl_sink_mode".to_string(),
            serde_json::Value::String("unknown".to_string()),
        )]);

        let err = SetupConfig::from_metadata(&metadata).expect_err("invalid sink mode should fail");
        assert!(
            err.to_string().contains("Unsupported setup metadata etl_sink_mode"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn generate_spicepod_for_iceberg_object_store_requires_warehouse_uri() {
        let setup_config = SetupConfig {
            etl_sink_mode: EtlSinkMode::IcebergObjectStore,
            etl_region: Some("us-west-2".to_string()),
            etl_endpoint: None,
            etl_iceberg_warehouse_uri: None,
            etl_iceberg_namespace: None,
        };

        let err = generate_initial_spicepod(&Uuid::nil(), &setup_config)
            .expect_err("missing warehouse uri should fail");
        assert!(
            err.to_string().contains("etl_iceberg_warehouse_uri"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn generate_spicepod_for_iceberg_object_store_uses_metadata_values() {
        let setup_config = SetupConfig {
            etl_sink_mode: EtlSinkMode::IcebergObjectStore,
            etl_region: Some("us-west-2".to_string()),
            etl_endpoint: Some("http://localhost:9000".to_string()),
            etl_iceberg_warehouse_uri: Some("s3://bucket/prefix/run-id".to_string()),
            etl_iceberg_namespace: Some("spicebench.etl".to_string()),
        };

        let spicepod =
            generate_initial_spicepod(&Uuid::nil(), &setup_config).expect("spicepod should generate");

        assert!(spicepod.contains("from: iceberg:s3://bucket/prefix/run-id"));
        assert!(spicepod.contains("access: read"));
        assert!(spicepod.contains("iceberg_s3_region: us-west-2"));
        assert!(spicepod.contains("iceberg_s3_endpoint: http://localhost:9000"));
    }
}
