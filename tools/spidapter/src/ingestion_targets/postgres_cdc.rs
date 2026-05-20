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
use std::fmt::Write as _;

use arrow::datatypes::DataType;
use spicepod::acceleration::{Acceleration, Mode, OnConflictBehavior, RefreshMode};
use spicepod::component::ComponentOrReference;
use spicepod::component::dataset::Dataset;
use spicepod::component::runtime::{Runtime, TelemetryConfig};
use spicepod::param::Params;
use spicepod::spec::SpicepodDefinition;
use system_adapter_protocol::DatasetConfig;
use tokio_postgres::NoTls;
use uuid::Uuid;

use crate::args::StdioArgs;

#[expect(dead_code)]
pub(crate) const PG_REPLICATION_SLOT_NAME: &str = "spicebench_slot";

pub(crate) fn tpch_schema_name(run_id: &Uuid) -> String {
    format!("tpch_{}", crate::commands::run_id_short(run_id))
}
/// Legacy single-publication name kept for teardown cleanup only.
const PG_PUBLICATION_NAME: &str = "spicebench_pub";

fn pub_name_for_table(table_name: &str) -> String {
    format!("spicebench_pub_{table_name}")
}

/// `PostgreSQL` connection details for the WAL CDC write path.
#[derive(Debug, Clone)]
pub(crate) struct PgConfig {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) user: String,
    pub(crate) password: String,
    pub(crate) database: String,
    pub(crate) schema: String,
}

impl PgConfig {
    pub(crate) fn from_args(args: &StdioArgs, run_id: &Uuid) -> Option<Self> {
        let host = args.pg_host.clone()?;
        Some(Self {
            host,
            port: args.pg_port,
            user: args
                .pg_user
                .clone()
                .unwrap_or_else(|| "postgres".to_string()),
            password: args.pg_password.clone(),
            database: args
                .pg_database
                .clone()
                .unwrap_or_else(|| "spicebench".to_string()),
            schema: tpch_schema_name(run_id),
        })
    }

    pub(crate) fn adbc_uri(&self) -> String {
        // Embed search_path so ADBC UPDATE/INSERT statements can resolve
        // unqualified table names without requiring callers to schema-qualify.
        format!(
            "postgresql://{}:{}@{}:{}/{}?options=-c%20search_path%3D{}",
            urlencoding::encode(&self.user),
            urlencoding::encode(&self.password),
            self.host,
            self.port,
            self.database,
            urlencoding::encode(&self.schema),
        )
    }

    pub(crate) fn libpq_connection_string(&self) -> String {
        format!(
            "host={} port={} user={} password={} dbname={} sslmode=disable",
            self.host, self.port, self.user, self.password, self.database,
        )
    }

    pub(crate) fn adbc_kwargs(&self) -> HashMap<String, serde_json::Value> {
        HashMap::from([(
            "uri".to_string(),
            serde_json::Value::String(self.adbc_uri()),
        )])
    }

    pub(crate) async fn connect(&self) -> anyhow::Result<tokio_postgres::Client> {
        let config = format!(
            "host={} port={} user={} password={} dbname={}",
            self.host, self.port, self.user, self.password, self.database
        );
        let (client, conn) = tokio_postgres::connect(&config, NoTls).await.map_err(|e| {
            anyhow::anyhow!(
                "failed to connect to PostgreSQL ({}:{}): {}",
                self.host,
                self.port,
                pg_error_message(&e)
            )
        })?;
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                eprintln!("[stdio] pg connection error: {e}");
            }
        });
        Ok(client)
    }
}

pub(crate) fn generate_postgres_wal_spicepod(
    run_id: &Uuid,
    pg: &PgConfig,
    datasets: &HashMap<String, DatasetConfig>,
    acceleration_engine: &str,
) -> SpicepodDefinition {
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

    for (dataset_name, dataset_config) in datasets {
        let param_map = HashMap::from([
            (
                "pg_connection_string".to_string(),
                pg.libpq_connection_string(),
            ),
            ("pg_host".to_string(), pg.host.clone()),
            ("pg_port".to_string(), pg.port.to_string()),
            ("pg_user".to_string(), pg.user.clone()),
            ("pg_pass".to_string(), pg.password.clone()),
            ("pg_db".to_string(), pg.database.clone()),
            ("pg_sslmode".to_string(), "disable".to_string()),
            (
                "pg_publication".to_string(),
                pub_name_for_table(dataset_name),
            ),
        ]);

        let pks = &dataset_config.primary_key_columns;
        let primary_key = match pks.len() {
            0 => None,
            1 => Some(pks[0].clone()),
            _ => Some(format!("({})", pks.join(", "))),
        };
        let on_conflict = match &primary_key {
            None => HashMap::new(),
            Some(pk) => HashMap::from([(pk.clone(), OnConflictBehavior::Upsert)]),
        };

        let mut dataset = Dataset::new(
            format!("postgres:{}.{dataset_name}", pg.schema),
            dataset_name.as_str(),
        );
        dataset.params = Some(Params::from_string_map(param_map));
        dataset.acceleration = Some(Acceleration {
            enabled: true,
            engine: Some(acceleration_engine.to_string()),
            mode: Mode::File,
            refresh_mode: Some(RefreshMode::Changes),
            primary_key,
            on_conflict,
            ..Acceleration::default()
        });

        spicepod
            .datasets
            .push(ComponentOrReference::Component(dataset));
    }

    spicepod
}

pub(crate) fn pg_type_for_arrow(data_type: &DataType) -> anyhow::Result<String> {
    Ok(match data_type {
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 | DataType::UInt8 | DataType::Int16 | DataType::UInt16 => {
            "SMALLINT".to_string()
        }
        DataType::Int32 | DataType::UInt32 => "INTEGER".to_string(),
        DataType::Int64 | DataType::UInt64 => "BIGINT".to_string(),
        DataType::Float16 | DataType::Float32 => "REAL".to_string(),
        // Decimal128 is cast to Float64 on the write path by the ADBC client, so
        // the column must be DOUBLE PRECISION to match the binary COPY wire type.
        DataType::Float64 | DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
            "DOUBLE PRECISION".to_string()
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "TEXT".to_string(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "BYTEA".to_string(),
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Time32(_) | DataType::Time64(_) => "TIME".to_string(),
        DataType::Timestamp(_, _) => "TIMESTAMP".to_string(),
        other => anyhow::bail!("Unsupported Arrow type for PostgreSQL: {other:?}"),
    })
}

pub(crate) fn pg_create_table_ddl(
    schema_name: &str,
    table_name: &str,
    dataset: &DatasetConfig,
) -> anyhow::Result<String> {
    let mut cols: Vec<String> = dataset
        .schema
        .fields()
        .iter()
        .filter(|f| f.name() != "_op" && f.name() != "_op_index")
        .map(|f| {
            let nullable = if f.is_nullable() { "" } else { " NOT NULL" };
            Ok(format!(
                "{} {}{}",
                f.name(),
                pg_type_for_arrow(f.data_type())?,
                nullable
            ))
        })
        .collect::<anyhow::Result<_>>()?;

    if cols.is_empty() {
        anyhow::bail!("Dataset '{table_name}' has no columns");
    }

    let pk_cols = &dataset.primary_key_columns;
    if !pk_cols.is_empty() {
        let pk_list = pk_cols.join(", ");
        cols.push(format!("PRIMARY KEY ({pk_list})"));
    }

    Ok(format!(
        "CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ({})",
        cols.join(", ")
    ))
}

/// Extract a useful error message from a tokio-postgres error.
///
/// `tokio_postgres::Error::to_string()` returns just `"db error"` for server
/// errors; the actual message lives in the error source chain.
pub(crate) fn pg_error_message(e: &tokio_postgres::Error) -> String {
    if let Some(db_err) = e.as_db_error() {
        let mut msg = db_err.message().to_string();
        if let Some(detail) = db_err.detail() {
            let _ = write!(msg, ": {detail}");
        }
        msg
    } else {
        e.to_string()
    }
}

pub(crate) async fn setup_postgres_for_wal(
    pg: &PgConfig,
    datasets: &HashMap<String, DatasetConfig>,
) -> anyhow::Result<()> {
    let client = pg.connect().await?;

    // Verify WAL level
    let row = client
        .query_one("SHOW wal_level", &[])
        .await
        .map_err(|e| anyhow::anyhow!("failed to query wal_level: {}", pg_error_message(&e)))?;
    let wal_level: &str = row.get(0);
    if wal_level != "logical" {
        anyhow::bail!(
            "PostgreSQL wal_level is '{wal_level}', expected 'logical'. \
             Restart PostgreSQL with -c wal_level=logical."
        );
    }

    // Create schema
    let create_schema = format!("CREATE SCHEMA IF NOT EXISTS {}", pg.schema);
    client.execute(&create_schema, &[]).await.map_err(|e| {
        anyhow::anyhow!(
            "failed to create schema '{}': {}",
            pg.schema,
            pg_error_message(&e)
        )
    })?;

    // Drop and recreate tables to ensure clean state on each benchmark run.
    for (name, dataset) in datasets {
        let drop_ddl = format!("DROP TABLE IF EXISTS {}.{}", pg.schema, name);
        eprintln!("[stdio] pg WAL setup: {drop_ddl}");
        client.execute(&drop_ddl, &[]).await.map_err(|e| {
            anyhow::anyhow!("failed to drop table '{name}': {}", pg_error_message(&e))
        })?;
        let ddl = pg_create_table_ddl(&pg.schema, name, dataset)?;
        eprintln!("[stdio] pg WAL setup: {ddl}");
        client.execute(&ddl, &[]).await.map_err(|e| {
            anyhow::anyhow!("failed to create table '{name}': {}", pg_error_message(&e))
        })?;
    }

    // Create one publication per table so each dataset's replication slot only
    // receives WAL events from its own table, preventing schema-mismatch errors.
    for name in datasets.keys() {
        let pub_name = pub_name_for_table(name);
        let qualified_table = format!("{}.{}", pg.schema, name);
        // Drop first for idempotency across benchmark runs.
        client
            .execute(&format!("DROP PUBLICATION IF EXISTS {pub_name}"), &[])
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "failed to drop publication '{pub_name}': {}",
                    pg_error_message(&e)
                )
            })?;
        let create_pub = format!("CREATE PUBLICATION {pub_name} FOR TABLE {qualified_table}");
        eprintln!("[stdio] pg WAL setup: {create_pub}");
        client.execute(&create_pub, &[]).await.map_err(|e| {
            anyhow::anyhow!(
                "failed to create publication '{pub_name}': {}",
                pg_error_message(&e)
            )
        })?;
    }

    Ok(())
}

pub(crate) async fn teardown_postgres(
    pg: &PgConfig,
    datasets: &HashMap<String, DatasetConfig>,
) -> anyhow::Result<()> {
    let client = pg.connect().await?;

    for name in datasets.keys() {
        let drop_table = format!("DROP TABLE IF EXISTS {}.{}", pg.schema, name);
        eprintln!("[stdio] pg teardown: {drop_table}");
        client.execute(&drop_table, &[]).await.map_err(|e| {
            anyhow::anyhow!("failed to drop table '{name}': {}", pg_error_message(&e))
        })?;

        let slots: Vec<String> = client
            .query(
                "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE $1",
                &[&format!("spice_{name}_%")],
            )
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "failed to query replication slots for '{name}': {}",
                    pg_error_message(&e)
                )
            })?
            .into_iter()
            .map(|row| row.get::<_, String>(0))
            .collect();
        for slot in slots {
            drop_replication_slot(&client, &slot).await;
        }

        let pub_name = pub_name_for_table(name);
        let drop_pub = format!("DROP PUBLICATION IF EXISTS {pub_name}");
        eprintln!("[stdio] pg teardown: {drop_pub}");
        client.execute(&drop_pub, &[]).await.map_err(|e| {
            anyhow::anyhow!(
                "failed to drop publication '{pub_name}': {}",
                pg_error_message(&e)
            )
        })?;
    }

    // Drop the schema (and everything remaining in it) created for this run
    let drop_schema = format!("DROP SCHEMA IF EXISTS {} CASCADE", pg.schema);
    eprintln!("[stdio] pg teardown: {drop_schema}");
    client.execute(&drop_schema, &[]).await.map_err(|e| {
        anyhow::anyhow!(
            "failed to drop schema '{}': {}",
            pg.schema,
            pg_error_message(&e)
        )
    })?;

    // Drop legacy publication if it exists
    let drop_legacy_pub = format!("DROP PUBLICATION IF EXISTS {PG_PUBLICATION_NAME}");
    eprintln!("[stdio] pg teardown: {drop_legacy_pub}");
    client.execute(&drop_legacy_pub, &[]).await.map_err(|e| {
        anyhow::anyhow!(
            "failed to drop legacy publication: {}",
            pg_error_message(&e)
        )
    })?;

    // Final sweep: drop any remaining spice-related replication slots not caught
    // by the per-table loop above (e.g. legacy names, slots from a previous
    // interrupted run, or slots whose naming doesn't match the per-table pattern).
    let remaining_slots: Vec<String> = client
        .query(
            "SELECT slot_name FROM pg_replication_slots \
             WHERE slot_name LIKE 'spice%' OR slot_name LIKE 'spicebench%'",
            &[],
        )
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "failed to query remaining replication slots: {}",
                pg_error_message(&e)
            )
        })?
        .into_iter()
        .map(|row| row.get::<_, String>(0))
        .collect();
    for slot in remaining_slots {
        drop_replication_slot(&client, &slot).await;
    }

    Ok(())
}

/// Terminate any backend holding `slot`, then drop it.
///
/// Spiced is sometimes SIGKILL'd, leaving the backend connection alive in
/// postgres long enough for the slot to still appear active at teardown time.
/// We terminate it first so `pg_drop_replication_slot` doesn't fail with
/// "replication slot is active". Failures are logged as warnings rather than
/// propagated so that a stuck slot doesn't prevent the rest of teardown.
async fn drop_replication_slot(client: &tokio_postgres::Client, slot: &str) {
    // Terminate the walsender holding this slot (no-op if already idle).
    if let Err(e) = client
        .execute(
            "SELECT pg_terminate_backend(active_pid) \
             FROM pg_replication_slots \
             WHERE slot_name = $1 AND active_pid IS NOT NULL",
            &[&slot],
        )
        .await
    {
        eprintln!(
            "[stdio] pg teardown: warning: could not terminate backend for slot '{slot}': {}",
            pg_error_message(&e)
        );
    }

    eprintln!("[stdio] pg teardown: SELECT pg_drop_replication_slot('{slot}')");
    if let Err(e) = client
        .execute("SELECT pg_drop_replication_slot($1)", &[&slot])
        .await
    {
        eprintln!(
            "[stdio] pg teardown: warning: could not drop replication slot '{slot}': {}",
            pg_error_message(&e)
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema, TimeUnit};
    use std::sync::Arc;

    fn make_pg_config() -> PgConfig {
        PgConfig {
            host: "localhost".to_string(),
            port: 5432,
            user: "spice".to_string(),
            password: "s3cr3t".to_string(),
            database: "spicebench".to_string(),
            schema: "tpch_abc12345".to_string(),
        }
    }

    fn make_dataset(fields: Vec<Field>, pks: Vec<&str>) -> DatasetConfig {
        DatasetConfig {
            schema: Arc::new(Schema::new(fields)),
            primary_key_columns: pks.into_iter().map(str::to_string).collect(),
            location: None,
            time_column: None,
            partition_columns: Vec::new(),
        }
    }

    // ── pg_type_for_arrow ────────────────────────────────────────────────────

    #[test]
    fn pg_type_maps_integer_types() {
        assert_eq!(pg_type_for_arrow(&DataType::Int8).expect("Int8"), "SMALLINT");
        assert_eq!(pg_type_for_arrow(&DataType::UInt8).expect("UInt8"), "SMALLINT");
        assert_eq!(pg_type_for_arrow(&DataType::Int16).expect("Int16"), "SMALLINT");
        assert_eq!(pg_type_for_arrow(&DataType::UInt16).expect("UInt16"), "SMALLINT");
        assert_eq!(pg_type_for_arrow(&DataType::Int32).expect("Int32"), "INTEGER");
        assert_eq!(pg_type_for_arrow(&DataType::UInt32).expect("UInt32"), "INTEGER");
        assert_eq!(pg_type_for_arrow(&DataType::Int64).expect("Int64"), "BIGINT");
        assert_eq!(pg_type_for_arrow(&DataType::UInt64).expect("UInt64"), "BIGINT");
    }

    #[test]
    fn pg_type_maps_float_and_decimal_types() {
        assert_eq!(pg_type_for_arrow(&DataType::Float16).expect("Float16"), "REAL");
        assert_eq!(pg_type_for_arrow(&DataType::Float32).expect("Float32"), "REAL");
        assert_eq!(
            pg_type_for_arrow(&DataType::Float64).expect("Float64"),
            "DOUBLE PRECISION"
        );
        assert_eq!(
            pg_type_for_arrow(&DataType::Decimal128(10, 2)).expect("Decimal128"),
            "DOUBLE PRECISION"
        );
        assert_eq!(
            pg_type_for_arrow(&DataType::Decimal256(18, 6)).expect("Decimal256"),
            "DOUBLE PRECISION"
        );
    }

    #[test]
    fn pg_type_maps_text_and_binary_types() {
        assert_eq!(pg_type_for_arrow(&DataType::Utf8).expect("Utf8"), "TEXT");
        assert_eq!(pg_type_for_arrow(&DataType::LargeUtf8).expect("LargeUtf8"), "TEXT");
        assert_eq!(pg_type_for_arrow(&DataType::Utf8View).expect("Utf8View"), "TEXT");
        assert_eq!(pg_type_for_arrow(&DataType::Binary).expect("Binary"), "BYTEA");
        assert_eq!(pg_type_for_arrow(&DataType::LargeBinary).expect("LargeBinary"), "BYTEA");
        assert_eq!(pg_type_for_arrow(&DataType::BinaryView).expect("BinaryView"), "BYTEA");
    }

    #[test]
    fn pg_type_maps_temporal_types() {
        assert_eq!(pg_type_for_arrow(&DataType::Date32).expect("Date32"), "DATE");
        assert_eq!(pg_type_for_arrow(&DataType::Date64).expect("Date64"), "DATE");
        assert_eq!(
            pg_type_for_arrow(&DataType::Time32(TimeUnit::Second)).expect("Time32"),
            "TIME"
        );
        assert_eq!(
            pg_type_for_arrow(&DataType::Time64(TimeUnit::Nanosecond)).expect("Time64"),
            "TIME"
        );
        assert_eq!(
            pg_type_for_arrow(&DataType::Timestamp(TimeUnit::Microsecond, None)).expect("Timestamp"),
            "TIMESTAMP"
        );
    }

    #[test]
    fn pg_type_errors_for_unsupported_type() {
        let err = pg_type_for_arrow(&DataType::Struct(
            vec![Field::new("k", DataType::Utf8, true)].into(),
        ))
        .expect_err("struct type should be unsupported");
        assert!(
            err.to_string().contains("Unsupported Arrow type"),
            "unexpected error: {err}"
        );
    }

    // ── pg_create_table_ddl ──────────────────────────────────────────────────

    #[test]
    fn pg_ddl_generates_correct_create_table() {
        let dataset = make_dataset(
            vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, true),
                Field::new("price", DataType::Float64, true),
            ],
            vec!["id"],
        );
        let ddl = pg_create_table_ddl("tpch_abc12345", "orders", &dataset).expect("ddl should generate");

        assert!(ddl.starts_with("CREATE TABLE IF NOT EXISTS tpch_abc12345.orders ("));
        assert!(ddl.contains("id BIGINT NOT NULL"));
        assert!(ddl.contains("name TEXT"));
        assert!(ddl.contains("price DOUBLE PRECISION"));
        assert!(ddl.contains("PRIMARY KEY (id)"));
    }

    #[test]
    fn pg_ddl_handles_composite_primary_key() {
        let dataset = make_dataset(
            vec![
                Field::new("pk1", DataType::Int32, false),
                Field::new("pk2", DataType::Utf8, false),
            ],
            vec!["pk1", "pk2"],
        );
        let ddl = pg_create_table_ddl("myschema", "mytable", &dataset).expect("ddl should generate");
        assert!(ddl.contains("PRIMARY KEY (pk1, pk2)"));
    }

    #[test]
    fn pg_ddl_excludes_op_columns() {
        let dataset = make_dataset(
            vec![
                Field::new("id", DataType::Int64, false),
                Field::new("_op", DataType::Utf8, true),
                Field::new("_op_index", DataType::Int32, true),
            ],
            vec![],
        );
        let ddl = pg_create_table_ddl("s", "t", &dataset).expect("ddl should generate");
        assert!(
            !ddl.contains("_op"),
            "DDL should not include _op column: {ddl}"
        );
        assert!(
            !ddl.contains("_op_index"),
            "DDL should not include _op_index: {ddl}"
        );
    }

    #[test]
    fn pg_ddl_errors_when_no_data_columns() {
        let dataset = make_dataset(
            vec![
                Field::new("_op", DataType::Utf8, true),
                Field::new("_op_index", DataType::Int32, true),
            ],
            vec![],
        );
        let err = pg_create_table_ddl("s", "empty_table", &dataset)
            .expect_err("should fail when only _op columns remain");
        assert!(
            err.to_string().contains("no columns"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn pg_ddl_errors_for_unsupported_column_type() {
        let dataset = make_dataset(
            vec![Field::new(
                "data",
                DataType::Struct(vec![Field::new("k", DataType::Utf8, true)].into()),
                true,
            )],
            vec![],
        );
        let err =
            pg_create_table_ddl("s", "t", &dataset).expect_err("unsupported type should fail");
        assert!(
            err.to_string().contains("Unsupported Arrow type"),
            "unexpected error: {err}"
        );
    }

    // ── PgConfig helpers ─────────────────────────────────────────────────────

    #[test]
    fn pg_config_adbc_uri_formats_correctly() {
        let pg = make_pg_config();
        let uri = pg.adbc_uri();
        assert_eq!(
            uri,
            "postgresql://spice:s3cr3t@localhost:5432/spicebench?options=-c%20search_path%3Dtpch_abc12345"
        );
    }

    #[test]
    fn pg_config_adbc_uri_includes_search_path() {
        let pg = make_pg_config();
        let uri = pg.adbc_uri();
        assert!(
            uri.contains("search_path%3Dtpch_abc12345"),
            "URI should encode search_path for the run schema: {uri}"
        );
    }

    #[test]
    fn pg_config_adbc_uri_url_encodes_credentials() {
        let pg = PgConfig {
            user: "sp ice".to_string(),
            password: "p@ss/word".to_string(),
            ..make_pg_config()
        };
        let uri = pg.adbc_uri();
        assert!(uri.contains("sp%20ice"), "space should be encoded: {uri}");
        assert!(
            uri.contains("p%40ss%2Fword"),
            "@ and / should be encoded: {uri}"
        );
    }

    #[test]
    fn pg_config_libpq_connection_string_format() {
        let pg = make_pg_config();
        let cs = pg.libpq_connection_string();
        assert_eq!(
            cs,
            "host=localhost port=5432 user=spice password=s3cr3t dbname=spicebench sslmode=disable"
        );
    }

    #[test]
    fn pg_config_adbc_kwargs_contains_uri() {
        let pg = make_pg_config();
        let kwargs = pg.adbc_kwargs();
        let uri = kwargs.get("uri").expect("uri key should be present");
        assert!(
            uri.as_str().expect("uri should be a string").starts_with("postgresql://"),
            "uri should be a postgresql:// URL: {uri}"
        );
    }

    // ── generate_postgres_wal_spicepod ───────────────────────────────────────

    #[test]
    fn wal_spicepod_includes_schema_qualified_from_and_plain_name() {
        let pg = make_pg_config();
        let run_id = uuid::Uuid::nil();
        let dataset = make_dataset(
            vec![
                Field::new("l_orderkey", DataType::Int64, false),
                Field::new("l_quantity", DataType::Float64, true),
            ],
            vec!["l_orderkey"],
        );
        let datasets = HashMap::from([("lineitem".to_string(), dataset)]);

        let spicepod = generate_postgres_wal_spicepod(&run_id, &pg, &datasets, "duckdb");
        let yaml = yaml::to_string(&spicepod).expect("serialize");

        // `from` must include the schema so the WAL connector reads from the right schema.
        assert!(
            yaml.contains("from: \"postgres:tpch_abc12345.lineitem\"")
                || yaml.contains("from: 'postgres:tpch_abc12345.lineitem'"),
            "from path should be schema-qualified: {yaml}"
        );
        // `name` must be the plain table name so TPCH queries resolve correctly.
        assert!(
            yaml.contains("name: lineitem"),
            "dataset name should be unqualified: {yaml}"
        );
    }

    #[test]
    fn wal_spicepod_sets_per_table_publication() {
        let pg = make_pg_config();
        let run_id = uuid::Uuid::nil();
        let datasets = HashMap::from([(
            "orders".to_string(),
            make_dataset(
                vec![Field::new("o_orderkey", DataType::Int64, false)],
                vec!["o_orderkey"],
            ),
        )]);

        let spicepod = generate_postgres_wal_spicepod(&run_id, &pg, &datasets, "duckdb");
        let yaml = yaml::to_string(&spicepod).expect("serialize");

        assert!(
            yaml.contains("spicebench_pub_orders"),
            "publication name should include table name: {yaml}"
        );
    }

    #[test]
    fn wal_spicepod_sets_primary_key_and_upsert() {
        let pg = make_pg_config();
        let run_id = uuid::Uuid::nil();
        let datasets = HashMap::from([(
            "nation".to_string(),
            make_dataset(
                vec![
                    Field::new("n_nationkey", DataType::Int32, false),
                    Field::new("n_name", DataType::Utf8, false),
                ],
                vec!["n_nationkey"],
            ),
        )]);

        let spicepod = generate_postgres_wal_spicepod(&run_id, &pg, &datasets, "duckdb");
        let yaml = yaml::to_string(&spicepod).expect("serialize");

        assert!(
            yaml.contains("primary_key: n_nationkey"),
            "primary key missing: {yaml}"
        );
        assert!(
            yaml.contains("upsert"),
            "on_conflict upsert missing: {yaml}"
        );
    }

    #[test]
    fn wal_spicepod_disables_telemetry() {
        let pg = make_pg_config();
        let run_id = uuid::Uuid::nil();
        let spicepod = generate_postgres_wal_spicepod(&run_id, &pg, &HashMap::new(), "duckdb");
        let yaml = yaml::to_string(&spicepod).expect("serialize");

        assert!(
            yaml.contains("enabled: false"),
            "telemetry should be disabled: {yaml}"
        );
    }
}
