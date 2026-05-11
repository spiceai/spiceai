/*
Copyright 2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Debezium CDC ingestion source for streaming benchmarks.
//!
//! Creates a fixed PostgreSQL table (`events_bench`), registers a long-lived
//! Debezium connector using the user-supplied topic prefix, and inserts data
//! via PostgreSQL (captured by Debezium into Kafka).
//!
//! The table and connector persist across runs; each benchmark run replays
//! from Kafka offset 0 using a unique consumer group.

use std::time::Duration;

use test_framework::anyhow::{self, Context, Result};
use tokio_postgres::{Client, NoTls};

use super::IngestionSource;

const PG_BATCH_SIZE: usize = 1000;
/// Fixed table name shared across all prepare invocations for a given topic prefix.
const TABLE_NAME: &str = "events_bench";
pub const MARKER_EVENT_TYPE: &str = "benchmark-ready";

/// Configuration for the Debezium ingestion source.
///
/// Environment variables:
/// - `KAFKA_BOOTSTRAP_SERVERS`: Kafka bootstrap servers (default: localhost:9092)
/// - `KAFKA_CONNECT_URL`: Kafka Connect REST URL (default: http://localhost:8083)
/// - `PG_HOST`: PostgreSQL host (default: localhost)
/// - `PG_PORT`: PostgreSQL port (default: 5432)
/// - `PG_DB`: PostgreSQL database (default: testdb)
/// - `PG_USER`: PostgreSQL user (default: postgres)
/// - `PG_PASSWORD`: PostgreSQL password (default: password)
#[derive(Debug, Clone)]
pub struct DebeziumConfig {
    pub kafka_bootstrap_servers: String,
    pub kafka_connect_url: String,
    pub pg_host: String,
    pub pg_port: u16,
    pub pg_db: String,
    pub pg_user: String,
    pub pg_password: String,
}

impl DebeziumConfig {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            kafka_bootstrap_servers: std::env::var("KAFKA_BOOTSTRAP_SERVERS")
                .unwrap_or_else(|_| "localhost:9092".to_string()),
            kafka_connect_url: std::env::var("KAFKA_CONNECT_URL")
                .unwrap_or_else(|_| "http://localhost:8083".to_string()),
            pg_host: std::env::var("PG_HOST").unwrap_or_else(|_| "localhost".to_string()),
            pg_port: std::env::var("PG_PORT")
                .unwrap_or_else(|_| "5432".to_string())
                .parse::<u16>()
                .context("Invalid PG_PORT")?,
            pg_db: std::env::var("PG_DB").unwrap_or_else(|_| "testdb".to_string()),
            pg_user: std::env::var("PG_USER").unwrap_or_else(|_| "postgres".to_string()),
            pg_password: std::env::var("PG_PASSWORD")
                .unwrap_or_else(|_| "password".to_string()),
        })
    }
}

pub struct DebeziumIngestionSource {
    config: DebeziumConfig,
    /// User-supplied topic prefix (e.g. `events-benchmark`).
    /// Debezium will publish to `{topic_prefix}.public.events_bench`.
    topic_prefix: String,
    connector_name: String,
    /// PostgreSQL replication slot name (hyphens replaced with underscores).
    slot_name: String,
    pg_client: Option<Client>,
    next_id: i64,
    http_client: reqwest::Client,
}

impl DebeziumIngestionSource {
    pub fn new(config: DebeziumConfig, topic_prefix: String) -> Self {
        let connector_name = format!("bench-{topic_prefix}");
        let slot_name = format!("bench_{}", topic_prefix.replace('-', "_"));
        Self {
            config,
            topic_prefix,
            connector_name,
            slot_name,
            pg_client: None,
            next_id: 1,
            http_client: reqwest::Client::new(),
        }
    }

    fn pg_connect_string(&self) -> String {
        format!(
            "host={} port={} user={} password={} dbname={}",
            self.config.pg_host,
            self.config.pg_port,
            self.config.pg_user,
            self.config.pg_password,
            self.config.pg_db,
        )
    }

    async fn register_connector(&self) -> Result<()> {
        let body = serde_json::json!({
            "name": self.connector_name,
            "config": {
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "database.hostname": self.config.pg_host,
                "database.port": self.config.pg_port.to_string(),
                "database.user": self.config.pg_user,
                "database.password": self.config.pg_password,
                "database.dbname": self.config.pg_db,
                "topic.prefix": self.topic_prefix,
                "table.include.list": format!("public.{TABLE_NAME}"),
                "plugin.name": "pgoutput",
                "slot.name": self.slot_name,
                "publication.name": self.slot_name,
                "snapshot.mode": "initial",
                "decimal.handling.mode": "double",
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "true",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "true"
            }
        });

        let url = format!("{}/connectors", self.config.kafka_connect_url);
        let resp = self
            .http_client
            .post(&url)
            .json(&body)
            .send()
            .await
            .context("Failed to register Debezium connector")?;

        let status = resp.status();
        // 409 Conflict means the connector already exists — treat as success.
        if status == reqwest::StatusCode::CONFLICT {
            println!("Connector '{}' already registered.", self.connector_name);
            return Ok(());
        }
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Failed to register connector (HTTP {status}): {text}"
            ));
        }

        println!("Registered Debezium connector '{}'", self.connector_name);
        Ok(())
    }

    async fn wait_for_connector_running(&self) -> Result<()> {
        let url = format!(
            "{}/connectors/{}/status",
            self.config.kafka_connect_url, self.connector_name
        );

        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            if std::time::Instant::now() > deadline {
                return Err(anyhow::anyhow!(
                    "Timeout waiting for Debezium connector to become RUNNING"
                ));
            }

            match self.http_client.get(&url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    if let Ok(json) = resp.json::<serde_json::Value>().await {
                        let state = json
                            .pointer("/connector/state")
                            .and_then(|v| v.as_str())
                            .unwrap_or("UNKNOWN");
                        let task_state = json
                            .pointer("/tasks/0/state")
                            .and_then(|v| v.as_str())
                            .unwrap_or("UNKNOWN");
                        if state == "RUNNING" && task_state == "RUNNING" {
                            println!("Connector '{}' is RUNNING", self.connector_name);
                            return Ok(());
                        }
                        println!("Connector state: {state}, task: {task_state}");
                    }
                }
                _ => {}
            }

            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    fn client(&self) -> Result<&Client> {
        self.pg_client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("PostgreSQL client not initialized — call prepare() first"))
    }
}

#[async_trait::async_trait]
impl IngestionSource for DebeziumIngestionSource {
    async fn prepare(&mut self) -> Result<()> {
        println!(
            "Preparing Debezium ingestion source (pg: {}:{}, topic_prefix: {})",
            self.config.pg_host, self.config.pg_port, self.topic_prefix
        );

        let (client, connection) = tokio_postgres::connect(&self.pg_connect_string(), NoTls)
            .await
            .context("Failed to connect to PostgreSQL")?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("PostgreSQL connection error: {e}");
            }
        });

        // Drop and recreate the table for a clean slate.
        client
            .execute(
                &format!("DROP TABLE IF EXISTS public.{TABLE_NAME}"),
                &[],
            )
            .await
            .context("Failed to drop existing events_bench table")?;

        client
            .execute(
                &format!(
                    "CREATE TABLE public.{TABLE_NAME} (
                        id         BIGINT PRIMARY KEY,
                        user_id    INTEGER       NOT NULL,
                        event_type VARCHAR(64)   NOT NULL,
                        payload    TEXT          NOT NULL DEFAULT '',
                        amount     FLOAT8        NOT NULL DEFAULT 0.0
                    )"
                ),
                &[],
            )
            .await
            .context("Failed to create events_bench table")?;

        client
            .execute(
                &format!("ALTER TABLE public.{TABLE_NAME} REPLICA IDENTITY FULL"),
                &[],
            )
            .await
            .context("Failed to set REPLICA IDENTITY")?;

        println!("Created table 'public.{TABLE_NAME}'");

        self.pg_client = Some(client);

        self.register_connector().await?;
        self.wait_for_connector_running().await?;

        Ok(())
    }

    async fn produce_rows(&mut self, count: u64) -> Result<()> {
        if count == 0 {
            return Ok(());
        }

        const EVENT_TYPES: &[&str] = &["click", "view", "purchase", "refund", "signup"];
        let mut produced = 0u64;

        for chunk_start in (0..count).step_by(PG_BATCH_SIZE) {
            let chunk_len = (count - chunk_start).min(PG_BATCH_SIZE as u64) as usize;
            let base_id = self.next_id;
            self.next_id += chunk_len as i64;

            let values_sql: String = (0..chunk_len)
                .map(|i| {
                    let id = base_id + i as i64;
                    let user_id = ((id % 10_000) + 1) as i32;
                    let event_type = EVENT_TYPES[(id as usize) % EVENT_TYPES.len()];
                    let amount = (id as f64 * 0.01) % 1000.0;
                    format!("({id}, {user_id}, '{event_type}', 'p{id}', {amount:.6})")
                })
                .collect::<Vec<_>>()
                .join(", ");

            let sql = format!(
                "INSERT INTO public.{TABLE_NAME} (id, user_id, event_type, payload, amount) VALUES {values_sql}"
            );

            self.client()?
                .execute(sql.as_str(), &[])
                .await
                .with_context(|| format!("Failed to insert batch starting at id={base_id}"))?;

            produced += chunk_len as u64;
            if produced % 10_000 == 0 || produced == count {
                println!("  Inserted {produced}/{count} rows into 'public.{TABLE_NAME}'");
            }
        }

        Ok(())
    }

    async fn produce_marker(&mut self, marker_event_type: &str) -> Result<()> {
        let id = self.next_id;
        self.next_id += 1;

        self.client()?
            .execute(
                &format!(
                    "INSERT INTO public.{TABLE_NAME} (id, user_id, event_type, payload, amount) VALUES ($1, $2, $3, $4, $5)"
                ),
                &[&id, &(-1_i32), &marker_event_type, &"marker", &0.0_f64],
            )
            .await
            .context("Failed to insert marker row")?;

        println!("Inserted marker into 'public.{TABLE_NAME}' (id={id}, event_type={marker_event_type})");
        Ok(())
    }

    async fn delete_marker(&mut self, _marker_event_type: &str) -> Result<()> {
        // Marker is permanent — no deletion.
        Ok(())
    }

    /// No-op: the table and connector persist so future benchmark runs can replay.
    async fn cleanup(&self) -> Result<()> {
        println!(
            "Debezium table 'public.{TABLE_NAME}' and connector '{}' retained for future runs.",
            self.connector_name
        );
        Ok(())
    }

    fn kafka_bootstrap_servers(&self) -> &str {
        &self.config.kafka_bootstrap_servers
    }

    fn source_from_field(&self) -> String {
        format!("debezium:{}.public.{TABLE_NAME}", self.topic_prefix)
    }
}
