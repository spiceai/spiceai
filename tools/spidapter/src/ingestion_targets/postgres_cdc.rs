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

pub(crate) const PG_REPLICATION_SLOT_NAME: &str = "spicebench_slot";
/// Legacy single-publication name kept for teardown cleanup only.
const PG_PUBLICATION_NAME: &str = "spicebench_pub";

fn pub_name_for_table(table_name: &str) -> String {
    format!("spicebench_pub_{table_name}")
}

/// PostgreSQL connection details for the WAL CDC write path.
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
    pub(crate) fn from_args(args: &StdioArgs) -> Option<Self> {
        let host = args.pg_host.clone()?;
        Some(Self {
            host,
            port: args.pg_port,
            user: args.pg_user.clone().unwrap_or_else(|| "postgres".to_string()),
            password: args.pg_password.clone(),
            database: args.pg_database.clone().unwrap_or_else(|| "spicebench".to_string()),
            schema: args.pg_schema.clone(),
        })
    }

    pub(crate) fn adbc_uri(&self) -> String {
        format!(
            "postgresql://{}:{}@{}:{}/{}",
            urlencoding::encode(&self.user),
            urlencoding::encode(&self.password),
            self.host,
            self.port,
            self.database,
        )
    }

    pub(crate) fn libpq_connection_string(&self) -> String {
        format!(
            "host={} port={} user={} password={} dbname={} sslmode=disable",
            self.host, self.port, self.user, self.password, self.database,
        )
    }

    pub(crate) fn adbc_kwargs(&self) -> HashMap<String, serde_json::Value> {
        HashMap::from([("uri".to_string(), serde_json::Value::String(self.adbc_uri()))])
    }

    pub(crate) async fn connect(&self) -> anyhow::Result<tokio_postgres::Client> {
        let config = format!(
            "host={} port={} user={} password={} dbname={}",
            self.host, self.port, self.user, self.password, self.database
        );
        let (client, conn) = tokio_postgres::connect(&config, NoTls).await?;
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
) -> anyhow::Result<SpicepodDefinition> {
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
            ("pg_connection_string".to_string(), pg.libpq_connection_string()),
            ("pg_host".to_string(), pg.host.clone()),
            ("pg_port".to_string(), pg.port.to_string()),
            ("pg_user".to_string(), pg.user.clone()),
            ("pg_pass".to_string(), pg.password.clone()),
            ("pg_db".to_string(), pg.database.clone()),
            ("pg_sslmode".to_string(), "disable".to_string()),
            ("pg_publication".to_string(), pub_name_for_table(dataset_name)),
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
            format!("postgres:{dataset_name}"),
            dataset_name.as_str(),
        );
        dataset.params = Some(Params::from_string_map(param_map));
        dataset.acceleration = Some(Acceleration {
            enabled: true,
            engine: Some("duckdb".to_string()),
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

    Ok(spicepod)
}

pub(crate) fn pg_type_for_arrow(data_type: &DataType) -> anyhow::Result<String> {
    Ok(match data_type {
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 | DataType::UInt8 => "SMALLINT".to_string(),
        DataType::Int16 | DataType::UInt16 => "SMALLINT".to_string(),
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
            msg.push_str(&format!(": {detail}"));
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
    let row = client.query_one("SHOW wal_level", &[]).await?;
    let wal_level: &str = row.get(0);
    if wal_level != "logical" {
        anyhow::bail!(
            "PostgreSQL wal_level is '{wal_level}', expected 'logical'. \
             Restart PostgreSQL with -c wal_level=logical."
        );
    }

    // Create schema
    client
        .execute(
            &format!("CREATE SCHEMA IF NOT EXISTS {}", pg.schema),
            &[],
        )
        .await?;

    // Drop and recreate tables to ensure clean state on each benchmark run.
    for (name, dataset) in datasets {
        let drop_ddl = format!("DROP TABLE IF EXISTS {}.{}", pg.schema, name);
        eprintln!("[stdio] pg WAL setup: {drop_ddl}");
        client.execute(&drop_ddl, &[]).await?;
        let ddl = pg_create_table_ddl(&pg.schema, name, dataset)?;
        eprintln!("[stdio] pg WAL setup: {ddl}");
        client.execute(&ddl, &[]).await?;
    }

    // Create one publication per table so each dataset's replication slot only
    // receives WAL events from its own table, preventing schema-mismatch errors.
    for name in datasets.keys() {
        let pub_name = pub_name_for_table(name);
        let qualified_table = format!("{}.{}", pg.schema, name);
        // Drop first for idempotency across benchmark runs.
        client
            .execute(&format!("DROP PUBLICATION IF EXISTS {pub_name}"), &[])
            .await?;
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
        client.execute(&drop_table, &[]).await?;

        let slots: Vec<String> = client
            .query(
                "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE $1",
                &[&format!("spice_{name}_%")],
            )
            .await?
            .into_iter()
            .map(|row| row.get::<_, String>(0))
            .collect();
        for slot in slots {
            let drop_slot = format!("SELECT pg_drop_replication_slot('{slot}')");
            eprintln!("[stdio] pg teardown: {drop_slot}");
            client.execute(&drop_slot, &[]).await?;
        }

        let pub_name = pub_name_for_table(name);
        let drop_pub = format!("DROP PUBLICATION IF EXISTS {pub_name}");
        eprintln!("[stdio] pg teardown: {drop_pub}");
        client.execute(&drop_pub, &[]).await?;
    }

    // Drop legacy publication if it exists
    let drop_legacy_pub = format!("DROP PUBLICATION IF EXISTS {PG_PUBLICATION_NAME}");
    eprintln!("[stdio] pg teardown: {drop_legacy_pub}");
    client.execute(&drop_legacy_pub, &[]).await?;

    // Drop legacy named slot if it exists
    let legacy_rows = client
        .query(
            "SELECT slot_name FROM pg_replication_slots WHERE slot_name = $1",
            &[&PG_REPLICATION_SLOT_NAME],
        )
        .await?;
    if !legacy_rows.is_empty() {
        let drop_legacy =
            format!("SELECT pg_drop_replication_slot('{PG_REPLICATION_SLOT_NAME}')");
        eprintln!("[stdio] pg teardown: {drop_legacy}");
        client.execute(&drop_legacy, &[]).await?;
    }

    Ok(())
}
