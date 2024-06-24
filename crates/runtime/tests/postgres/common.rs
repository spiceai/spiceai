/*
Copyright 2024 The Spice.ai OSS Authors

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

use std::{collections::HashMap, sync::Arc};

use bollard::secret::HealthConfig;
use db_connection_pool::postgrespool::PostgresConnectionPool;
use secrecy::SecretString;
// use spicepod::component::{dataset::Dataset, params::Params as DatasetParams};
use tracing::instrument;

use crate::docker::{ContainerRunnerBuilder, RunningContainer};

const PG_PASSWORD: &str = "runtime-integration-test-pw";
const PG_DOCKER_CONTAINER: &str = "runtime-integration-test-postgres";

fn get_pg_params() -> HashMap<String, SecretString> {
    let mut params = HashMap::new();
    params.insert(
        "pg_host".to_string(),
        SecretString::from("localhost".to_string()),
    );
    params.insert(
        "pg_port".to_string(),
        SecretString::from("15432".to_string()),
    );
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

#[instrument]
pub(super) async fn start_postgres_docker_container(
) -> Result<RunningContainer<'static>, anyhow::Error> {
    let running_container = ContainerRunnerBuilder::new(PG_DOCKER_CONTAINER)
        .image("postgres:latest")
        .add_port_binding(5432, 15432)
        .add_env_var("POSTGRES_PASSWORD", PG_PASSWORD)
        .healthcheck(HealthConfig {
            test: Some(vec![
                "CMD-SHELL".to_string(),
                "pg_isready -U postgres".to_string(),
            ]),
            interval: Some(250_000_000), // 250ms
            timeout: Some(100_000_000),  // 100ms
            retries: Some(5),
            start_period: Some(500_000_000), // 100ms
            start_interval: None,
        })
        .build()?
        .run()
        .await?;

    tokio::time::sleep(std::time::Duration::from_millis(5000)).await;
    Ok(running_container)
}

#[instrument]
pub(super) async fn get_postgres_connection_pool() -> Result<PostgresConnectionPool, anyhow::Error>
{
    let pool = PostgresConnectionPool::new(Arc::new(get_pg_params())).await?;

    Ok(pool)
}
