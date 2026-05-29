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

use spicepod::acceleration::{Acceleration, Mode, OnConflictBehavior, RefreshMode};
use spicepod::component::ComponentOrReference;
use spicepod::component::dataset::Dataset;
use spicepod::component::runtime::{Runtime, TelemetryConfig};
use spicepod::param::Params;
use spicepod::semantic::Column;
use spicepod::spec::SpicepodDefinition;
use system_adapter_protocol::DatasetConfig;
use uuid::Uuid;

use super::postgres_cdc::{PgConfig, pg_create_table_ddl, pg_error_message};
use super::{arrow_type_to_spicepod_str, composite_key_str};

/// Set up `PostgreSQL` for Debezium CDC: verify `wal_level`, create schema, drop+create tables.
/// The replication slot and publication are managed by Debezium itself.
pub(crate) async fn setup_postgres_for_debezium(
    pg: &PgConfig,
    datasets: &HashMap<String, DatasetConfig>,
) -> anyhow::Result<()> {
    let client = pg.connect().await?;

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

    client
        .execute(&format!("CREATE SCHEMA IF NOT EXISTS {}", pg.schema), &[])
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "failed to create schema '{}': {}",
                pg.schema,
                pg_error_message(&e)
            )
        })?;

    for (name, dataset) in datasets {
        let drop_ddl = format!("DROP TABLE IF EXISTS {}.{}", pg.schema, name);
        eprintln!("[stdio] pg debezium setup: {drop_ddl}");
        client.execute(&drop_ddl, &[]).await.map_err(|e| {
            anyhow::anyhow!("failed to drop table '{name}': {}", pg_error_message(&e))
        })?;
        let ddl = pg_create_table_ddl(&pg.schema, name, dataset)?;
        eprintln!("[stdio] pg debezium setup: {ddl}");
        client.execute(&ddl, &[]).await.map_err(|e| {
            anyhow::anyhow!("failed to create table '{name}': {}", pg_error_message(&e))
        })?;
    }

    Ok(())
}

/// Register the Debezium `PostgreSQL` connector via the Kafka Connect REST API.
///
/// `debezium_host` — hostname the Debezium container uses to reach `PostgreSQL`.
/// Defaults to `pg.host` but can be overridden via `PG_DEBEZIUM_HOST` when the
/// Debezium container runs in Docker and needs a different hostname (e.g. `host.docker.internal`).
pub(crate) async fn register_debezium_postgres_connector(
    connect_url: &str,
    pg: &PgConfig,
    debezium_host: &str,
    table_names: &[&str],
) -> anyhow::Result<()> {
    let table_include_list = table_names
        .iter()
        .map(|t| format!("{}.{}", pg.schema, t))
        .collect::<Vec<_>>()
        .join(",");

    let body = serde_json::json!({
        "name": "spicebench-postgres",
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.hostname": debezium_host,
            "database.port": pg.port.to_string(),
            "database.user": pg.user,
            "database.password": pg.password,
            "database.dbname": pg.database,
            "topic.prefix": "spicebench",
            "table.include.list": table_include_list,
            "plugin.name": "pgoutput",
            "slot.name": "spicebench_debezium_slot",
            "publication.autocreate.mode": "filtered",
            "snapshot.mode": "initial"
        }
    });

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    let resp = client
        .post(format!("{connect_url}/connectors"))
        .json(&body)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to POST to Debezium Connect: {e}"))?;

    let status = resp.status();
    let response_body = resp
        .text()
        .await
        .unwrap_or_else(|e| format!("<failed to read body: {e}>"));

    if status.is_success() {
        eprintln!("[stdio] Debezium: PostgreSQL connector registered (status={status})");
        Ok(())
    } else if status.as_u16() == 409 {
        // Connector already exists from a previous run — update its config via PUT
        // so it points to the new schema/tables instead of the old ones.
        eprintln!("[stdio] Debezium: connector already exists, updating config via PUT...");
        let config = body["config"].clone();
        let put_resp = client
            .put(format!("{connect_url}/connectors/spicebench-postgres/config"))
            .json(&config)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to PUT Debezium connector config: {e}"))?;
        let put_status = put_resp.status();
        let put_body = put_resp
            .text()
            .await
            .unwrap_or_else(|e| format!("<failed to read body: {e}>"));
        if put_status.is_success() {
            eprintln!("[stdio] Debezium: connector config updated (status={put_status})");
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "Debezium connector config update failed: status={put_status}, body={put_body}"
            ))
        }
    } else {
        Err(anyhow::anyhow!(
            "Debezium Connect registration failed: status={status}, body={response_body}"
        ))
    }
}

/// Generate a [`SpicepodDefinition`] that reads from Kafka topics populated by the Debezium
/// `PostgreSQL` connector.
///
/// Topic naming: `spicebench.{schema}.{table_name}`.
pub(crate) fn generate_postgres_debezium_spicepod(
    run_id: &Uuid,
    kafka_brokers: &str,
    pg: &PgConfig,
    acceleration_engine: &str,
    datasets: &HashMap<String, DatasetConfig>,
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

    for (dataset_name, config) in datasets {
        let topic = format!("spicebench.{}.{dataset_name}", pg.schema);
        let mut dataset = Dataset::new(format!("debezium:{topic}"), dataset_name.as_str());
        dataset.params = Some(Params::from_string_map(HashMap::from([
            (
                "kafka_bootstrap_servers".to_string(),
                kafka_brokers.to_string(),
            ),
            (
                "kafka_security_protocol".to_string(),
                "PLAINTEXT".to_string(),
            ),
            (
                "batch_max_size".to_string(),
                "50000".to_string(),
            ),
            (
                "batch_max_duration".to_string(),
                "1s".to_string(),
            ),
        ])));

        dataset.columns = config
            .schema
            .fields()
            .iter()
            .map(|field| {
                Column::new(field.name())
                    .with_type(arrow_type_to_spicepod_str(field.data_type()))
                    .with_nullable(field.is_nullable())
            })
            .collect();

        let pk_cols = &config.primary_key_columns;
        let primary_key = composite_key_str(pk_cols);
        let on_conflict = primary_key
            .as_ref()
            .map(|k| HashMap::from([(k.clone(), OnConflictBehavior::Upsert)]))
            .unwrap_or_default();

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
