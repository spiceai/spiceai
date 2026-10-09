/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::{collections::HashMap, time::Duration};

use bollard::secret::HealthConfig;
use datafusion_table_providers::{
    UnsupportedTypeAction, sql::db_connection_pool::postgrespool::PostgresConnectionPool,
};
use secrecy::SecretString;
use tokio_postgres::NoTls;
use tracing::instrument;

use test_framework::source_versions::{Source, source_image};

use crate::docker::{ContainerRunnerBuilder, RunningContainer, wait_for_tcp_port};

pub const PG_PASSWORD: &str = "runtime-integration-test-pw";

/// The container health check. Over TCP, because the image's entrypoint first
/// runs a temporary server for its init scripts that listens only on the Unix
/// socket and reports ready, then shuts it down and starts the real one: a
/// socket probe can pass in that gap, while a test connecting over the published
/// port reaches nothing yet and sees "connection closed".
const PG_READY_PROBE: &str = "pg_isready -h 127.0.0.1 -U postgres";
const PG_DOCKER_CONTAINER: &str = "runtime-integration-test-postgres";
const PG_CONTAINER_START_TIMEOUT: Duration = Duration::from_mins(3);
const PG_HOST_PORT_READY_TIMEOUT: Duration = Duration::from_mins(1);

pub fn get_pg_params(port: usize) -> HashMap<String, SecretString> {
    let mut params = HashMap::new();
    params.insert(
        "pg_host".to_string(),
        SecretString::from("localhost".to_string()),
    );
    params.insert("pg_port".to_string(), SecretString::from(port.to_string()));
    params.insert(
        "pg_user".to_string(),
        SecretString::from("postgres".to_string()),
    );
    params.insert(
        "pg_pass".to_string(),
        SecretString::from(PG_PASSWORD.to_string()),
    );
    params.insert(
        "pg_db".to_string(),
        SecretString::from("postgres".to_string()),
    );
    params.insert(
        "pg_sslmode".to_string(),
        SecretString::from("disable".to_string()),
    );
    params
}

pub async fn connect(port: u16) -> Result<tokio_postgres::Client, anyhow::Error> {
    let mut cfg = tokio_postgres::Config::new();
    cfg.host("localhost")
        .port(port)
        .user("postgres")
        .password(PG_PASSWORD)
        .dbname("postgres");
    let (client, connection) = cfg.connect(NoTls).await?;
    tokio::spawn(async move {
        let _: Result<(), tokio_postgres::Error> = connection.await;
    });
    Ok(client)
}

#[instrument]
pub async fn start_postgres_docker_container() -> Result<RunningContainer, anyhow::Error> {
    let running_container = ContainerRunnerBuilder::new(PG_DOCKER_CONTAINER)
        .image(source_image(Source::Postgres)?)
        .publish_port(5432)
        .add_env_var("POSTGRES_PASSWORD", PG_PASSWORD)
        .healthcheck(HealthConfig {
            test: Some(vec!["CMD-SHELL".to_string(), PG_READY_PROBE.to_string()]),
            interval: Some(1_000_000_000), // 1s
            timeout: Some(5_000_000_000),  // 5s
            retries: Some(60),
            start_period: Some(10_000_000_000), // 10s
            start_interval: None,
        })
        .build()?
        .run(Some(PG_CONTAINER_START_TIMEOUT))
        .await?;

    wait_for_tcp_port(
        "127.0.0.1",
        running_container.host_port(5432)?,
        PG_HOST_PORT_READY_TIMEOUT,
    )
    .await?;
    Ok(running_container)
}

/// Like [`start_postgres_docker_container`] but launches Postgres with
/// `wal_level=logical` and generous slot/sender limits so that the
/// postgres replication tests can create multiple replication slots.
#[instrument]
pub async fn start_postgres_docker_container_with_logical_wal()
-> Result<RunningContainer, anyhow::Error> {
    let running_container = ContainerRunnerBuilder::new(&format!("{PG_DOCKER_CONTAINER}-repl"))
        .image(source_image(Source::Postgres)?)
        .publish_port(5432)
        .add_env_var("POSTGRES_PASSWORD", PG_PASSWORD)
        .command([
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=10",
            "-c",
            "max_wal_senders=10",
        ])
        .healthcheck(HealthConfig {
            test: Some(vec!["CMD-SHELL".to_string(), PG_READY_PROBE.to_string()]),
            interval: Some(1_000_000_000),
            timeout: Some(5_000_000_000),
            retries: Some(60),
            start_period: Some(10_000_000_000),
            start_interval: None,
        })
        .build()?
        .run(Some(PG_CONTAINER_START_TIMEOUT))
        .await?;

    wait_for_tcp_port(
        "127.0.0.1",
        running_container.host_port(5432)?,
        PG_HOST_PORT_READY_TIMEOUT,
    )
    .await?;
    Ok(running_container)
}

#[instrument]
pub async fn get_postgres_connection_pool(
    port: usize,
    action: Option<UnsupportedTypeAction>,
) -> Result<PostgresConnectionPool, anyhow::Error> {
    let action = action.unwrap_or_default();
    let pool = PostgresConnectionPool::new(get_pg_params(port))
        .await?
        .with_unsupported_type_action(action);

    Ok(pool)
}

/// Use an explicitly supplied disposable replication database, or start a container.
/// The external fixture must use the test credentials and enable logical WAL.
pub async fn replication_test_database() -> Result<(usize, Option<RunningContainer>), anyhow::Error>
{
    if let Ok(port) = std::env::var("POSTGRES_REPLICATION_TEST_PORT") {
        return Ok((usize::from(port.parse::<u16>()?), None));
    }
    let container = start_postgres_docker_container_with_logical_wal().await?;
    let port = usize::from(container.host_port(5432)?);
    Ok((port, Some(container)))
}
