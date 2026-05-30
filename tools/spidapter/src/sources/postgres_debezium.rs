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
/// Register one Debezium connector per table so each gets its own replication
/// slot and WAL stream — allowing all tables to be captured in parallel rather
/// than being serialized behind the largest table (lineitem).
///
/// Connectors are named `spicebench-{table}` with slot `spicebench_{table}_slot`.
/// All connectors are registered concurrently, then we wait for all to reach
/// RUNNING before returning.
pub(crate) async fn register_debezium_postgres_connector(
    connect_url: &str,
    pg: &PgConfig,
    debezium_host: &str,
    table_names: &[&str],
) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    // Register all per-table connectors concurrently.
    let mut tasks = tokio::task::JoinSet::new();
    for &table in table_names {
        let connector_name = format!("spicebench-{table}");
        let slot_name = format!("spicebench_{table}_slot");
        let publication_name = format!("dbz_pub_{table}");
        let table_include = format!("{}.{table}", pg.schema);

        let body = serde_json::json!({
            "name": connector_name,
            "config": {
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "database.hostname": debezium_host,
                "database.port": pg.port.to_string(),
                "database.user": pg.user,
                "database.password": pg.password,
                "database.dbname": pg.database,
                "topic.prefix": "spicebench",
                "table.include.list": table_include,
                "plugin.name": "pgoutput",
                "slot.name": slot_name,
                "publication.name": publication_name,
                "publication.autocreate.mode": "filtered",
                "snapshot.mode": "initial",
                "heartbeat.interval.ms": "10000"
            }
        });

        let connect_url = connect_url.to_string();
        let client = client.clone();
        tasks.spawn(async move {
            register_single_connector(&connect_url, &client, &connector_name, &body).await
        });
    }

    while let Some(result) = tasks.join_next().await {
        result.map_err(|e| anyhow::anyhow!("Connector task panicked: {e}"))??;
    }

    // Wait for all connectors to reach RUNNING before returning.
    let mut wait_tasks = tokio::task::JoinSet::new();
    for &table in table_names {
        let connector_name = format!("spicebench-{table}");
        let connect_url = connect_url.to_string();
        let client = client.clone();
        wait_tasks.spawn(async move {
            wait_for_connector_running(&connect_url, &client, &connector_name).await
        });
    }
    while let Some(result) = wait_tasks.join_next().await {
        result.map_err(|e| anyhow::anyhow!("Connector wait task panicked: {e}"))??;
    }

    Ok(())
}

async fn register_single_connector(
    connect_url: &str,
    client: &reqwest::Client,
    connector_name: &str,
    body: &serde_json::Value,
) -> anyhow::Result<()> {
    let resp = client
        .post(format!("{connect_url}/connectors"))
        .json(body)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to POST connector '{connector_name}': {e}"))?;

    let status = resp.status();
    let response_body = resp.text().await.unwrap_or_else(|e| format!("<{e}>"));

    if status.is_success() {
        eprintln!("[stdio] Debezium: connector '{connector_name}' registered (status={status})");
    } else if status.as_u16() == 409 {
        // Connector already exists — delete and recreate for a fresh snapshot.
        eprintln!("[stdio] Debezium: connector '{connector_name}' exists — deleting and recreating...");
        let del = client
            .delete(format!("{connect_url}/connectors/{connector_name}"))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to DELETE '{connector_name}': {e}"))?;
        if !del.status().is_success() && del.status().as_u16() != 404 {
            let s = del.status();
            let b = del.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!("DELETE '{connector_name}' failed: {s} {b}"));
        }
        tokio::time::sleep(Duration::from_secs(3)).await;

        let recreate = client
            .post(format!("{connect_url}/connectors"))
            .json(body)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to recreate '{connector_name}': {e}"))?;
        let s = recreate.status();
        let b = recreate.text().await.unwrap_or_default();
        if !s.is_success() {
            return Err(anyhow::anyhow!("Recreation of '{connector_name}' failed: {s} {b}"));
        }
        eprintln!("[stdio] Debezium: connector '{connector_name}' recreated (status={s})");
    } else {
        return Err(anyhow::anyhow!(
            "Debezium connector '{connector_name}' registration failed: {status} {response_body}"
        ));
    }

    Ok(())
}

async fn wait_for_connector_running(
    connect_url: &str,
    client: &reqwest::Client,
    connector_name: &str,
) -> anyhow::Result<()> {
    let status_url = format!("{connect_url}/connectors/{connector_name}/status");
    let timeout = Duration::from_secs(120);
    let started = tokio::time::Instant::now();

    eprintln!("[stdio] Debezium: waiting for connector '{connector_name}' to reach RUNNING state...");

    loop {
        if started.elapsed() > timeout {
            anyhow::bail!(
                "Timed out after {}s waiting for Debezium connector to reach RUNNING state",
                timeout.as_secs()
            );
        }

        match client.get(&status_url).send().await {
            Ok(resp) if resp.status().is_success() => {
                let body: serde_json::Value =
                    resp.json().await.unwrap_or(serde_json::Value::Null);

                let connector_state = body
                    .get("connector")
                    .and_then(|c| c.get("state"))
                    .and_then(|s| s.as_str())
                    .unwrap_or("UNKNOWN");

                let task_states: Vec<&str> = body
                    .get("tasks")
                    .and_then(|t| t.as_array())
                    .map(|tasks| {
                        tasks
                            .iter()
                            .filter_map(|t| t.get("state").and_then(|s| s.as_str()))
                            .collect()
                    })
                    .unwrap_or_default();

                eprintln!(
                    "[stdio] Debezium: connector={connector_state} tasks={task_states:?}"
                );

                // Tasks RUNNING is sufficient — connector-level UNASSIGNED is a known
                // Kafka Connect quirk in single-node deployments where the coordinator
                // hasn't fully claimed the connector object but tasks are active.
                let tasks_running = !task_states.is_empty()
                    && task_states.iter().all(|&s| s == "RUNNING");

                if tasks_running {
                    eprintln!(
                        "[stdio] Debezium: connector '{connector_name}' tasks RUNNING, replication slot active"
                    );
                    return Ok(());
                }

                if connector_state == "FAILED" || task_states.iter().any(|&s| s == "FAILED") {
                    anyhow::bail!("Debezium connector '{connector_name}' entered FAILED state: {body}");
                }
            }
            Ok(resp) => {
                eprintln!(
                    "[stdio] Debezium: status check returned {}, retrying...",
                    resp.status()
                );
            }
            Err(e) => {
                eprintln!("[stdio] Debezium: status check failed ({e}), retrying...");
            }
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
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
    auto_load_complete: bool,
) -> SpicepodDefinition {
    let run_id_str = run_id.to_string();
    let short_id = run_id_str.split('-').next().unwrap_or_default();

    let mut spicepod = SpicepodDefinition::new(format!("spidapter-{short_id}"));
    spicepod.runtime = Runtime {
        telemetry: TelemetryConfig {
            enabled: false,
            ..TelemetryConfig::default()
        },
        params: std::collections::HashMap::from([
            // Match cdc_max_coalesced_envelopes to batch_max_size so the CDC
            // coalescing window is large enough to build full-size write batches.
            ("cdc_max_coalesced_envelopes".to_string(), "50000".to_string()),
        ]),
        ..Runtime::default()
    };

    for (dataset_name, config) in datasets {
        let topic = format!("spicebench.{}.{dataset_name}", pg.schema);
        let mut dataset = Dataset::new(format!("debezium:{topic}"), dataset_name.as_str());
        let mut param_map = HashMap::from([
            ("kafka_bootstrap_servers".to_string(), kafka_brokers.to_string()),
            ("kafka_security_protocol".to_string(), "PLAINTEXT".to_string()),
            ("batch_max_size".to_string(), "50000".to_string()),
            ("batch_max_duration".to_string(), "1s".to_string()),
            ("kafka_session_timeout_ms".to_string(), "300000".to_string()),
        ]);
        if auto_load_complete {
            param_map.insert("auto_load_complete".to_string(), "true".to_string());
        }
        dataset.params = Some(Params::from_string_map(param_map));

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

        // Metrics are commented out because the cloud spicepod schema (SpicepodSchema.parse)
        // expects metrics as {metrics: [...]} but the Rust Metrics struct serializes as a flat
        // array [...], causing deployment creation to fail with "Invalid spicepod configuration".
        // Uncomment once the cloud schema is updated to accept flat arrays.
        // dataset.metrics = Some(Metrics {
        //     metrics: vec![
        //         Metric { name: "records_lag".to_string(), enabled: true },
        //         Metric { name: "records_consumed_total".to_string(), enabled: true },
        //         Metric { name: "bytes_consumed_total".to_string(), enabled: true },
        //     ],
        // });

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
