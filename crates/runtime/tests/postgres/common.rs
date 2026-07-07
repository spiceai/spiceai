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
use rand::RngExt;
use secrecy::SecretString;
use tracing::instrument;

use crate::docker::{ContainerRunnerBuilder, RunningContainer, wait_for_tcp_port};

pub const PG_PASSWORD: &str = "runtime-integration-test-pw";
const PG_IMAGE: &str = "docker.io/library/postgres:latest";
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

pub fn get_random_port() -> Result<usize, anyhow::Error> {
    let mut rng = rand::rng();

    for _ in 0..100 {
        let port: usize = rng.random_range(15432..65535);
        let addr = std::net::SocketAddr::from(([127, 0, 0, 1], u16::try_from(port)?));
        if std::net::TcpListener::bind(addr).is_ok() {
            return Ok(port);
        }
    }
    Err(anyhow::anyhow!("No available port found"))
}

#[instrument]
pub async fn start_postgres_docker_container(
    port: usize,
) -> Result<RunningContainer<'static>, anyhow::Error> {
    let container_name = format!("{PG_DOCKER_CONTAINER}-{port}");
    let container_name: &'static str = Box::leak(container_name.into_boxed_str());
    let port = port.try_into().unwrap_or(15432);

    let running_container = ContainerRunnerBuilder::new(container_name)
        .image(PG_IMAGE.to_string())
        .add_port_binding(5432, port)
        .add_env_var("POSTGRES_PASSWORD", PG_PASSWORD)
        .healthcheck(HealthConfig {
            test: Some(vec![
                "CMD-SHELL".to_string(),
                "pg_isready -U postgres".to_string(),
            ]),
            interval: Some(1_000_000_000), // 1s
            timeout: Some(5_000_000_000),  // 5s
            retries: Some(60),
            start_period: Some(10_000_000_000), // 10s
            start_interval: None,
        })
        .build()?
        .run(Some(PG_CONTAINER_START_TIMEOUT))
        .await?;

    wait_for_tcp_port("127.0.0.1", port, PG_HOST_PORT_READY_TIMEOUT).await?;
    Ok(running_container)
}

/// Like [`start_postgres_docker_container`] but launches Postgres with
/// `wal_level=logical` and generous slot/sender limits so that the
/// postgres replication tests can create multiple replication slots.
#[instrument]
pub async fn start_postgres_docker_container_with_logical_wal(
    port: usize,
) -> Result<RunningContainer<'static>, anyhow::Error> {
    let container_name = format!("{PG_DOCKER_CONTAINER}-repl-{port}");
    let container_name: &'static str = Box::leak(container_name.into_boxed_str());
    let port: u16 = port
        .try_into()
        .map_err(|e| anyhow::anyhow!("port {port} does not fit in u16: {e}"))?;

    let running_container = ContainerRunnerBuilder::new(container_name)
        .image(PG_IMAGE.to_string())
        .add_port_binding(5432, port)
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
            test: Some(vec![
                "CMD-SHELL".to_string(),
                "pg_isready -U postgres".to_string(),
            ]),
            interval: Some(1_000_000_000),
            timeout: Some(5_000_000_000),
            retries: Some(60),
            start_period: Some(10_000_000_000),
            start_interval: None,
        })
        .build()?
        .run(Some(PG_CONTAINER_START_TIMEOUT))
        .await?;

    wait_for_tcp_port("127.0.0.1", port, PG_HOST_PORT_READY_TIMEOUT).await?;
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
